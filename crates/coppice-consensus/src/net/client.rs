//! The Raft network factory and per-peer client (ADR 0002/0011/0016/0018).
//!
//! openraft asks the factory for one [`GrpcRaftNetwork`] per target node and
//! drives replication, elections, and snapshot installs through it. The factory
//! owns a per-peer [`Channel`] map so a peer's mTLS connection (ADR 0011) is
//! dialed once and reused; a membership address change drops and redials that
//! peer. Every request stamps the cluster identity (ADR 0016) and the error
//! taxonomy below is chosen to drive openraft's backoff correctly.

// openraft's `RaftNetwork` methods return `RPCError`, a large enum; the
// signatures (and the helpers that feed them) are dictated by the trait, so
// boxing is not an option.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Code;

use coppice_tls::TlsStore;

use openraft::error::{
    Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Snapshot, Vote};

use coppice_core::bytes::ByteSize;
use coppice_net::transport::Client;
use coppice_proto::pb::raft::v1 as pb;

use crate::adapter::TypeConfig;
use crate::storage::raftpb;
use crate::CoordinatorId;

use super::convert;

/// Wire chunk size for a streaming snapshot install (ADR 0018).
///
/// The ADR 0018 container streams in file order, read straight off the
/// sender's durable snapshot file (the `SnapshotData` binding is the
/// file-backed `SnapshotFile`, see `adapter.rs`), so this bounds both the
/// wire message size and the sender's memory: one chunk in flight, however
/// large the snapshot.
pub const SNAPSHOT_CHUNK: ByteSize = ByteSize::from_mib(1);

/// The path name fail-stop wire-decode errors are attributed to.
const WIRE: &str = "raft-rpc";

/// When each peer last *answered* a Raft RPC from this node — recorded at the
/// transport, per successful response (ADR 0037 §9).
///
/// This is the leader-side "have I actually heard from this voter" fact behind
/// the `?require=healthy` liveness test. It cannot be inferred from log
/// positions: openraft's metrics expose only matched indexes, and on an idle
/// log a dead follower's matched index stays current forever. A replication
/// response is real observation — a leader heartbeats every follower at
/// `heartbeat_interval` even when the log is idle, so on a leader this map
/// stays fresh for a live peer and goes stale within about one interval of a
/// dead one. On a non-leader nothing sends replication RPCs, so the map simply
/// freezes; callers gate on leadership before reading it.
#[derive(Clone, Default)]
pub struct PeerContact {
    peers: Arc<Mutex<HashMap<CoordinatorId, Instant>>>,
}

impl PeerContact {
    /// Record a successful RPC response from `target`, now.
    fn record(&self, target: CoordinatorId) {
        self.peers
            .lock()
            .expect("peer contact map poisoned")
            .insert(target, Instant::now());
    }

    /// Time since `target` last answered an RPC from this process; `None`
    /// when it never has.
    pub fn elapsed(&self, target: CoordinatorId) -> Option<Duration> {
        self.peers
            .lock()
            .expect("peer contact map poisoned")
            .get(&target)
            .map(Instant::elapsed)
    }
}

/// Creates one [`GrpcRaftNetwork`] per target, sharing a per-peer channel map.
///
/// Cheap to hold: it keeps the shared TLS store and rebuilds a
/// [`ClientTlsConfig`] from the *current* material each time it dials a peer, so
/// a rotated leaf is picked up on the next (re)dial without a restart (ADR 0037
/// §4). Channels are dialed lazily and reused per peer; an address change drops
/// and redials.
///
/// The factory and every [`GrpcRaftNetwork`] it hands out share one [`Shared`]
/// (an `Arc`), so a network's per-dial `channel_for` consults the *same*
/// generation-checked channel map. This is what makes a rotation reach a
/// long-lived replication worker: the worker holds a `GrpcRaftNetwork`, and each
/// of its RPCs re-resolves the channel through the shared map rather than
/// cloning a frozen `Channel` captured at `new_client` time (ADR 0037 §4).
pub struct GrpcNetworkFactory {
    shared: Arc<Shared>,
}

/// State shared by the factory and every network it creates: the cluster
/// identity, the hot-reload TLS store, the per-RPC timeout, and the per-peer
/// channel map consulted on every dial.
struct Shared {
    history_id: [u8; 16],
    tls: Arc<TlsStore>,
    rpc_timeout: Duration,
    /// Per-peer `(dialed address, TLS generation, channel)`. A membership change
    /// that moves a peer's address drops the stale channel and redials; a TLS
    /// rotation (the store's [`generation`] advancing) does the same, so a
    /// reconnect after a rotation presents the fresh leaf instead of the leaf
    /// captured in the cached channel's [`ClientTlsConfig`] (ADR 0037 §4).
    ///
    /// [`generation`]: coppice_tls::TlsStore::generation
    channels: Mutex<PeerChannels>,
    /// Per-peer last-successful-response instants (ADR 0037 §9).
    contact: PeerContact,
}

/// Per-peer cache value: the dialed address, the TLS material generation it was
/// built at, and the lazily-connected channel itself.
type PeerChannels = HashMap<CoordinatorId, (String, u64, Channel)>;

impl GrpcNetworkFactory {
    /// Build the factory from the shared hot-reload TLS store (ADR 0011/0037),
    /// the per-RPC timeout, and the cluster identity stamped into every request.
    pub fn new(history_id: [u8; 16], tls: Arc<TlsStore>, rpc_timeout: Duration) -> Self {
        GrpcNetworkFactory {
            shared: Arc::new(Shared {
                history_id,
                tls,
                rpc_timeout,
                channels: Mutex::new(HashMap::new()),
                contact: PeerContact::default(),
            }),
        }
    }

    /// The per-peer contact facts this factory's networks record. Taken once
    /// at assembly (before the factory moves into openraft) and read wherever
    /// the `?require=healthy` liveness test runs (ADR 0037 §9).
    pub fn contact(&self) -> PeerContact {
        self.shared.contact.clone()
    }
}

impl Shared {
    /// The mutual-TLS client config built from the material current *right now*
    /// (ADR 0037 §4): the cluster CA as the trust root, this node's leaf as the
    /// client identity. Rebuilt per dial so a reconnect after a rotation uses
    /// the fresh leaf.
    fn client_tls(&self) -> ClientTlsConfig {
        let material = self.tls.current();
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(material.ca_pem()))
            .identity(Identity::from_pem(material.cert_pem(), material.key_pem()))
    }

    /// Reuse the peer's channel, redialing if its address changed, the TLS
    /// material rotated since it was dialed, or it was never dialed. `Err`
    /// carries an operator-readable reason the RPC layer surfaces as
    /// [`Unreachable`].
    ///
    /// Called on **every** dial (each RPC), not just at `new_client`: it is a
    /// map lookup plus an atomic generation read under a short-lived mutex,
    /// released before any await, so replication is never serialized on it. The
    /// generation check is what closes the rotation gap: the cached `Channel`
    /// froze its `ClientTlsConfig` at creation, so after a rotation an internal
    /// tonic reconnect (or a retained network's next RPC) would keep presenting
    /// the old leaf. Evicting on a generation bump forces a re-dial that rebuilds
    /// the config from the current material.
    fn channel_for(&self, target: CoordinatorId, addr: &str) -> Result<Channel, String> {
        let generation = self.tls.generation();
        let mut map = self.channels.lock().expect("network channel map poisoned");
        if let Some((existing, gen, channel)) = map.get(&target) {
            if existing == addr && *gen == generation {
                return Ok(channel.clone());
            }
        }
        let channel = build_channel(&self.client_tls(), addr, self.rpc_timeout)
            .map_err(|e| format!("cannot dial coordinator {target} at {addr}: {e}"))?;
        map.insert(target, (addr.to_string(), generation, channel.clone()));
        Ok(channel)
    }
}

/// Construct a lazily-connecting mTLS channel to `addr` (`host:port` or
/// `[v6]:port`).
///
/// `connect_lazy` hands reconnection to tonic; per-peer reuse is the factory's
/// channel map. The SNI domain name is the unbracketed host part of the dial
/// address (an IPv6 SAN identity carries no brackets); the `https://{addr}`
/// authority keeps the original brackets. `Err` is an operator-readable string.
fn build_channel(tls: &ClientTlsConfig, addr: &str, timeout: Duration) -> Result<Channel, String> {
    let (host, _port) = coppice_tls::split_host_port(addr).map_err(|e| e.to_string())?;
    let endpoint = Endpoint::from_shared(format!("https://{addr}"))
        .map_err(|e| e.to_string())?
        .tls_config(tls.clone().domain_name(host))
        .map_err(|e| e.to_string())?
        .connect_timeout(timeout)
        .timeout(timeout);
    Ok(endpoint.connect_lazy())
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: CoordinatorId, node: &BasicNode) -> GrpcRaftNetwork {
        // No dial here: the network resolves its channel through the shared,
        // generation-checked map on every RPC, so a rotation reaches even a
        // long-lived replication worker that keeps this object (ADR 0037 §4).
        // A membership address change makes openraft build a new network with
        // the new node, so the captured `addr` need never mutate in place.
        GrpcRaftNetwork {
            shared: Arc::clone(&self.shared),
            target,
            addr: node.addr.clone(),
        }
    }
}

/// A single-target Raft client that resolves its mTLS channel through the shared
/// factory map on **every** RPC.
///
/// It holds no frozen `Channel`: [`dial`](Self::dial) calls
/// [`Shared::channel_for`] each time, so a TLS rotation (generation bump) or an
/// internal tonic reconnect always presents the current leaf, not the one
/// captured when openraft first created this network (ADR 0037 §4). openraft
/// keeps this object alive for the life of a replication stream, which is
/// exactly why the per-dial resolution — rather than a captured channel — is
/// required.
pub struct GrpcRaftNetwork {
    shared: Arc<Shared>,
    target: CoordinatorId,
    /// The peer's dial address, captured at `new_client`. A membership change
    /// that moves the peer spawns a fresh network, so this stays fixed for the
    /// object's life.
    addr: String,
}

impl GrpcRaftNetwork {
    /// Resolve the peer's channel through the shared generation-checked map, or
    /// an [`Unreachable`] RPC error if it cannot be dialed. Consulted per RPC so
    /// a rotation is never pinned to a stale channel.
    fn dial<E: std::error::Error>(&self) -> Result<Channel, RPCError<CoordinatorId, BasicNode, E>> {
        self.shared
            .channel_for(self.target, &self.addr)
            .map_err(|msg| RPCError::Unreachable(Unreachable::new(&io::Error::other(msg))))
    }
}

/// Map a non-OK gRPC status onto openraft's RPC error taxonomy (unary RPCs).
///
/// `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`CANCELLED` mean the peer is down or
/// partitioned → [`Unreachable`], which drives openraft's `backoff()`. Every
/// other status (including a history-id mismatch's `FAILED_PRECONDITION`) is
/// a [`NetworkError`]: retry soon but do not treat the peer as healthy. A
/// `FAILED_PRECONDITION` is logged at error level — it means cross-cluster
/// contact (ADR 0016).
fn status_to_rpc<E: std::error::Error>(
    target: CoordinatorId,
    status: tonic::Status,
) -> RPCError<CoordinatorId, BasicNode, E> {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                "coordinator {target} unreachable: {}",
                status.message()
            ))))
        }
        code => {
            if code == Code::FailedPrecondition {
                tracing::error!(
                    peer = target,
                    detail = %status.message(),
                    "raft RPC refused with FAILED_PRECONDITION — cross-cluster contamination (ADR 0016)"
                );
            }
            RPCError::Network(NetworkError::new(&io::Error::other(format!(
                "coordinator {target} rejected RPC ({code:?}): {}",
                status.message()
            ))))
        }
    }
}

/// The streaming-install counterpart of [`status_to_rpc`], mirroring the same
/// unreachable-vs-network split into [`StreamingError`] variants.
fn status_to_streaming(
    target: CoordinatorId,
    status: tonic::Status,
) -> StreamingError<TypeConfig, Fatal<CoordinatorId>> {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
            StreamingError::Unreachable(Unreachable::new(&io::Error::other(format!(
                "coordinator {target} unreachable: {}",
                status.message()
            ))))
        }
        code => {
            if code == Code::FailedPrecondition {
                tracing::error!(
                    peer = target,
                    detail = %status.message(),
                    "snapshot install refused with FAILED_PRECONDITION — cross-cluster contamination (ADR 0016)"
                );
            }
            StreamingError::Network(NetworkError::new(&io::Error::other(format!(
                "coordinator {target} rejected snapshot install ({code:?}): {}",
                status.message()
            ))))
        }
    }
}

impl RaftNetwork<TypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<CoordinatorId>,
        RPCError<CoordinatorId, BasicNode, RaftError<CoordinatorId>>,
    > {
        let channel = self.dial()?;
        let mut client = Client::new(channel);
        let req = convert::append_entries_to_pb(&rpc, self.shared.history_id);
        let resp = client
            .append_entries(req)
            .await
            .map_err(|status| status_to_rpc(self.target, status))?;
        // The peer answered a replication RPC (heartbeats ride this same
        // path), which is exactly what "the leader has heard from this voter"
        // means for the liveness test (ADR 0037 §9). A rejection body still
        // counts: the peer is alive and reachable either way.
        self.shared.contact.record(self.target);
        convert::append_response_from_pb(resp.into_inner())
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<CoordinatorId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<CoordinatorId>,
        RPCError<CoordinatorId, BasicNode, RaftError<CoordinatorId>>,
    > {
        let channel = self.dial()?;
        let mut client = Client::new(channel);
        let req = convert::vote_request_to_pb(&rpc, self.shared.history_id);
        let resp = client
            .vote(req)
            .await
            .map_err(|status| status_to_rpc(self.target, status))?;
        convert::vote_response_from_pb(resp.into_inner())
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    // The chunked `install_snapshot` RPC is not implemented: under openraft's
    // `generic-snapshot-data` feature it is a deprecated dead path, and
    // `full_snapshot` below is the one send path (ADR 0018).

    async fn full_snapshot(
        &mut self,
        vote: Vote<CoordinatorId>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<CoordinatorId>, StreamingError<TypeConfig, Fatal<CoordinatorId>>>
    {
        let channel = self
            .shared
            .channel_for(self.target, &self.addr)
            .map_err(|msg| StreamingError::Unreachable(Unreachable::new(&io::Error::other(msg))))?;
        let mut client = Client::new(channel);

        // Header first, then the container bytes in file order as `data`
        // chunks (ADR 0018), read straight off the durable snapshot file (the
        // `SnapshotData` binding is the file-backed `SnapshotFile`): one wire
        // chunk in memory at a time, however large the snapshot.
        let header = pb::InstallSnapshotHeader {
            history_id: self.shared.history_id.to_vec(),
            vote: Some(raftpb::vote_to_pb(&vote)),
            meta: Some(convert::snapshot_ident_to_pb(&snapshot.meta)),
        };
        let file = snapshot.snapshot;

        // The feeder runs on the blocking pool (file reads are sync seam IO).
        // On any local read error it drops `tx`, truncating the stream — the
        // receiver's container validation refuses the torn copy and openraft
        // retries. If the RPC ends first (cancel or response), `rx` drops and
        // the next `blocking_send` fails, so the thread exits promptly.
        let (tx, rx) = mpsc::channel::<pb::InstallSnapshotRequest>(2);
        let _feeder = tokio::task::spawn_blocking(move || {
            let header_msg = pb::InstallSnapshotRequest {
                chunk: Some(pb::install_snapshot_request::Chunk::Header(header)),
            };
            if tx.blocking_send(header_msg).is_err() {
                return;
            }
            let len = match file.len() {
                Ok(len) => len,
                Err(error) => {
                    tracing::warn!(%error, "snapshot send aborted: cannot size snapshot file");
                    return;
                }
            };
            // The chunk size becomes a buffer length here, and only here.
            let mut buf = vec![
                0u8;
                SNAPSHOT_CHUNK
                    .as_usize_saturating()
                    .min(len.max(1) as usize)
            ];
            let mut at = 0u64;
            while at < len {
                let n = ((len - at) as usize).min(buf.len());
                if let Err(error) = file.read_exact_at(at, &mut buf[..n]) {
                    tracing::warn!(%error, "snapshot send aborted: cannot read snapshot file");
                    return;
                }
                let msg = pb::InstallSnapshotRequest {
                    chunk: Some(pb::install_snapshot_request::Chunk::Data(buf[..n].to_vec())),
                };
                if tx.blocking_send(msg).is_err() {
                    return;
                }
                at += n as u64;
            }
        });

        let stream = ReceiverStream::new(rx);
        let call = client.install_snapshot(stream);
        tokio::pin!(call);
        tokio::pin!(cancel);

        tokio::select! {
            reason = &mut cancel => {
                // Returning drops the call (and with it the feeder's channel),
                // so the blocking feeder unblocks and exits.
                Err(StreamingError::Closed(reason))
            }
            result = &mut call => {
                let resp = result.map_err(|status| status_to_streaming(self.target, status))?;
                // A completed snapshot install is contact too (ADR 0037 §9):
                // a follower resyncing by snapshot answers no append RPCs for
                // the whole transfer, and must not read as dead meanwhile.
                self.shared.contact.record(self.target);
                let vote_pb = resp.into_inner().vote.ok_or_else(|| {
                    StreamingError::Network(NetworkError::new(&io::Error::new(
                        io::ErrorKind::InvalidData,
                        "InstallSnapshotResponse missing vote",
                    )))
                })?;
                let vote = raftpb::vote_from_pb(Path::new(WIRE), &vote_pb)
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
                Ok(SnapshotResponse::new(vote))
            }
        }
    }

    // `backoff()` keeps openraft's default (a constant 500 ms) — no config.
}
