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
use std::time::Duration;

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
use openraft::{Snapshot, Vote};

use coppice_core::bytes::ByteSize;
use coppice_net::transport::Client;
use coppice_proto::pb::raft::v1 as pb;

use crate::adapter::TypeConfig;
use crate::membership::CoordinatorNode;
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

/// Creates one [`GrpcRaftNetwork`] per target, sharing a per-peer channel map.
///
/// Cheap to hold: it keeps the shared TLS store and rebuilds a
/// [`ClientTlsConfig`] from the *current* material each time it dials a peer, so
/// a rotated leaf is picked up on the next (re)dial without a restart (ADR 0037
/// §6). Channels are dialed lazily and reused per peer; an address change drops
/// and redials.
pub struct GrpcNetworkFactory {
    cluster_uuid: [u8; 16],
    tls: Arc<TlsStore>,
    rpc_timeout: Duration,
    /// Per-peer `(dialed address, channel)`. A membership change that moves a
    /// peer's address drops the stale channel and redials.
    channels: Arc<Mutex<HashMap<CoordinatorId, (String, Channel)>>>,
}

impl GrpcNetworkFactory {
    /// Build the factory from the shared hot-reload TLS store (ADR 0011/0037),
    /// the per-RPC timeout, and the cluster identity stamped into every request.
    pub fn new(cluster_uuid: [u8; 16], tls: Arc<TlsStore>, rpc_timeout: Duration) -> Self {
        GrpcNetworkFactory {
            cluster_uuid,
            tls,
            rpc_timeout,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The mutual-TLS client config built from the material current *right now*
    /// (ADR 0037 §6): the cluster CA as the trust root, this node's leaf as the
    /// client identity. Rebuilt per dial so a reconnect after a rotation uses
    /// the fresh leaf.
    fn client_tls(&self) -> ClientTlsConfig {
        let material = self.tls.current();
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(material.ca_pem()))
            .identity(Identity::from_pem(material.cert_pem(), material.key_pem()))
    }

    /// Reuse the peer's channel, redialing if its address changed or it was
    /// never dialed. `Err` carries an operator-readable reason the RPC layer
    /// surfaces as [`Unreachable`].
    fn channel_for(&self, target: CoordinatorId, addr: &str) -> Result<Channel, String> {
        let mut map = self.channels.lock().expect("network channel map poisoned");
        if let Some((existing, channel)) = map.get(&target) {
            if existing == addr {
                return Ok(channel.clone());
            }
        }
        let channel = build_channel(&self.client_tls(), addr, self.rpc_timeout)
            .map_err(|e| format!("cannot dial coordinator {target} at {addr}: {e}"))?;
        map.insert(target, (addr.to_string(), channel.clone()));
        Ok(channel)
    }
}

/// Construct a lazily-connecting mTLS channel to `addr` (`host:port`).
///
/// `connect_lazy` hands reconnection to tonic; per-peer reuse is the factory's
/// channel map. The SNI domain name is the host part of the dial address.
fn build_channel(
    tls: &ClientTlsConfig,
    addr: &str,
    timeout: Duration,
) -> Result<Channel, tonic::transport::Error> {
    let host = addr
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(addr)
        .to_string();
    let endpoint = Endpoint::from_shared(format!("https://{addr}"))?
        .tls_config(tls.clone().domain_name(host))?
        .connect_timeout(timeout)
        .timeout(timeout);
    Ok(endpoint.connect_lazy())
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(
        &mut self,
        target: CoordinatorId,
        node: &CoordinatorNode,
    ) -> GrpcRaftNetwork {
        // Per the trait contract, this must not fail even for a bad address; a
        // dial error is deferred into the per-RPC path as `Unreachable`.
        let channel = self.channel_for(target, &node.addr);
        GrpcRaftNetwork {
            target,
            cluster_uuid: self.cluster_uuid,
            channel,
        }
    }
}

/// A single-target Raft client over a shared, lazily-connected mTLS channel.
pub struct GrpcRaftNetwork {
    target: CoordinatorId,
    cluster_uuid: [u8; 16],
    /// The dial result captured at `new_client`. `Err` means the address could
    /// not be turned into a channel — treated as `Unreachable` on every RPC.
    channel: Result<Channel, String>,
}

impl GrpcRaftNetwork {
    /// The channel, or an [`Unreachable`] RPC error if the peer never dialed.
    fn dial<E: std::error::Error>(
        &self,
    ) -> Result<Channel, RPCError<CoordinatorId, CoordinatorNode, E>> {
        self.channel
            .clone()
            .map_err(|msg| RPCError::Unreachable(Unreachable::new(&io::Error::other(msg))))
    }
}

/// Map a non-OK gRPC status onto openraft's RPC error taxonomy (unary RPCs).
///
/// `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`CANCELLED` mean the peer is down or
/// partitioned → [`Unreachable`], which drives openraft's `backoff()`. Every
/// other status (including a cluster-UUID mismatch's `FAILED_PRECONDITION`) is
/// a [`NetworkError`]: retry soon but do not treat the peer as healthy. A
/// `FAILED_PRECONDITION` is logged at error level — it means cross-cluster
/// contact (ADR 0016).
fn status_to_rpc<E: std::error::Error>(
    target: CoordinatorId,
    status: tonic::Status,
) -> RPCError<CoordinatorId, CoordinatorNode, E> {
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
        RPCError<CoordinatorId, CoordinatorNode, RaftError<CoordinatorId>>,
    > {
        let channel = self.dial()?;
        let mut client = Client::new(channel);
        let req = convert::append_entries_to_pb(&rpc, self.cluster_uuid);
        let resp = client
            .append_entries(req)
            .await
            .map_err(|status| status_to_rpc(self.target, status))?;
        convert::append_response_from_pb(resp.into_inner())
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<CoordinatorId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<CoordinatorId>,
        RPCError<CoordinatorId, CoordinatorNode, RaftError<CoordinatorId>>,
    > {
        let channel = self.dial()?;
        let mut client = Client::new(channel);
        let req = convert::vote_request_to_pb(&rpc, self.cluster_uuid);
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
            .channel
            .clone()
            .map_err(|msg| StreamingError::Unreachable(Unreachable::new(&io::Error::other(msg))))?;
        let mut client = Client::new(channel);

        // Header first, then the container bytes in file order as `data`
        // chunks (ADR 0018), read straight off the durable snapshot file (the
        // `SnapshotData` binding is the file-backed `SnapshotFile`): one wire
        // chunk in memory at a time, however large the snapshot.
        let header = pb::InstallSnapshotHeader {
            cluster_uuid: self.cluster_uuid.to_vec(),
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
