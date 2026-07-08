//! Node assembly: the openraft-free entry point the coordinator starts a
//! replica through (ADR 0002/0011/0016).
//!
//! [`start`] runs the ADR 0016 identity matrix (restart vs bootstrap vs join),
//! opens or stamps the segment store, spawns the publishing apply task, builds
//! the openraft node with the gRPC transport, and hands back a [`StartedNode`]:
//! the [`Consensus`](crate::Consensus) handle, the read views, the event tap,
//! an admin [`NodeHandle`], and the tonic transport service ready to mount on
//! the coordinator's mTLS server. No openraft type appears in this surface.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

use openraft::error::{InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft, SnapshotPolicy};

use coppice_raft_net::transport::Server;

use crate::adapter::{OpenraftConsensus, TypeConfig, APPLY_CHANNEL_CAPACITY};
use crate::events::{EventTap, EventTapReceiver};
use crate::fs::{Fs, RealFs};
use crate::net::{GrpcNetworkFactory, RaftTransportHandler};
use crate::storage::{self, StorageOptions};
use crate::view::{StateViews, ViewPublisher, ViewPublisherConfig};
use crate::{apply_loop, status, ConsensusError, ConsensusStatus, CoordinatorId};

/// PEM material for the mutual-TLS coordinator mesh (ADR 0011).
pub struct NodeTls {
    /// The cluster CA, used to verify peer certificates.
    pub ca_pem: Vec<u8>,
    /// This node's certificate chain.
    pub cert_pem: Vec<u8>,
    /// This node's private key.
    pub key_pem: Vec<u8>,
}

/// How this process intends to join the cluster (ADR 0016).
///
/// The intent is the operator's assertion about the data directory, checked
/// against what is actually on disk; a mismatch fail-stops rather than guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartIntent {
    /// Resume an existing instance from an initialized directory.
    Restart,
    /// Form a brand-new single-voter cluster from an empty directory.
    Bootstrap,
    /// Start a fresh replacement instance (learner-join) from an empty directory.
    Join,
}

/// Everything a coordinator supplies to bring up its consensus replica.
pub struct NodeOptions {
    /// This replica's allocate-once Raft identity (ADR 0016).
    pub node_id: CoordinatorId,
    /// The cluster this replica belongs to (ADR 0016).
    pub cluster_uuid: [u8; 16],
    /// The data directory; must already exist.
    pub data_dir: std::path::PathBuf,
    /// The `host:port` peers dial to reach this node (used at bootstrap).
    pub advertise_addr: String,
    /// openraft's election-timeout minimum; the maximum is twice this.
    pub election_timeout: Duration,
    /// Leader heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Per-RPC timeout for the transport.
    pub rpc_timeout: Duration,
    /// Snapshot cadence: build after this many log entries since the last
    /// snapshot (`SnapshotPolicy::LogsSinceLast`).
    pub snapshot_log_entries: u64,
    /// How many post-snapshot log entries openraft keeps before purge.
    pub snapshot_keep_log_entries: u64,
    /// Capacity of the derived event tap (ADR 0008).
    pub event_tap_capacity: usize,
    /// mTLS material (ADR 0011).
    pub tls: NodeTls,
}

/// A running consensus replica, assembled and ready to serve.
pub struct StartedNode {
    /// The openraft-free proposal/read/membership surface.
    pub consensus: OpenraftConsensus,
    /// Published read views of applied state.
    pub views: StateViews,
    /// The derived event stream (ADR 0008).
    pub event_tap: EventTapReceiver,
    /// Admin/shutdown handle.
    pub handle: NodeHandle,
    /// The Raft transport service, ready to mount on the coordinator's mTLS
    /// server.
    pub transport: Server<RaftTransportHandler>,
}

/// A cheap, cloneable admin handle to the running node.
#[derive(Clone)]
pub struct NodeHandle {
    raft: Raft<TypeConfig>,
    node_id: CoordinatorId,
    #[allow(dead_code)]
    cluster_uuid: [u8; 16],
    status: watch::Receiver<ConsensusStatus>,
}

impl NodeHandle {
    /// Shut the replica down (coordinator-runtime.md shutdown step 5): the apply
    /// task drains and exits when the adapter drops the request channel.
    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ConsensusError::Fatal(format!("raft shutdown join error: {e}")))
    }

    /// A point-in-time summary for the admin `ClusterStatus` RPC and tests.
    ///
    /// Reads the latest metrics without awaiting; the replication list is
    /// populated only when this node is leader.
    pub fn cluster_summary(&self) -> ClusterSummary {
        let known_committed = self.status.borrow().known_committed;

        let metrics = self.raft.metrics();
        let m = metrics.borrow();

        let mut members: Vec<MemberSummary> = Vec::new();
        for (id, node) in m.membership_config.nodes() {
            members.push(MemberSummary {
                id: *id,
                addr: node.addr.clone(),
                voter: false,
            });
        }
        for voter in m.membership_config.voter_ids() {
            if let Some(member) = members.iter_mut().find(|s| s.id == voter) {
                member.voter = true;
            }
        }

        let replication = m
            .replication
            .as_ref()
            .map(|repl| {
                repl.iter()
                    .map(|(id, matched)| (*id, matched.map(|l| l.index).unwrap_or(0)))
                    .collect()
            })
            .unwrap_or_default();

        ClusterSummary {
            local_id: self.node_id,
            leader: m.current_leader,
            term: m.current_term,
            last_applied: m.last_applied.map(|id| id.index).unwrap_or(0),
            known_committed,
            members,
            replication,
        }
    }
}

/// A snapshot of cluster state for the admin surface (ADR 0016).
#[derive(Debug, Clone)]
pub struct ClusterSummary {
    /// This node's Raft identity.
    pub local_id: CoordinatorId,
    /// The current leader, when known.
    pub leader: Option<CoordinatorId>,
    /// The current term.
    pub term: u64,
    /// Highest applied log index.
    pub last_applied: u64,
    /// Highest committed index this node knows of.
    pub known_committed: u64,
    /// Membership: id, dial address, and whether the node is a voter.
    pub members: Vec<MemberSummary>,
    /// Per-follower matched index; empty when this node is not leader.
    pub replication: Vec<(CoordinatorId, u64)>,
}

/// One membership entry in a [`ClusterSummary`].
#[derive(Debug, Clone)]
pub struct MemberSummary {
    /// The node's Raft identity.
    pub id: CoordinatorId,
    /// The address peers dial.
    pub addr: String,
    /// Whether the node is a voter (vs a learner).
    pub voter: bool,
}

/// A fail-stop during [`start`]: each message names the data directory, the
/// intent flag, and the identities so the first error an operator sees is
/// actionable (ADR 0016).
#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    /// The data directory is missing or not a directory.
    #[error("data directory {0} does not exist (the coordinator must create it before start)")]
    MissingDataDir(std::path::PathBuf),

    /// The intent flag disagrees with what is on disk (the ADR 0016 matrix).
    #[error("{0}")]
    RefusedStart(String),

    /// A storage-layer failure (including identity-stamp mismatch at open).
    #[error(transparent)]
    Storage(#[from] io::Error),

    /// openraft construction, initialization, or config validation failed.
    #[error("raft startup failed: {0}")]
    Raft(String),
}

/// Bring up a consensus replica per the intent (ADR 0016).
pub async fn start(
    options: NodeOptions,
    intent: StartIntent,
) -> Result<StartedNode, NodeStartError> {
    let NodeOptions {
        node_id,
        cluster_uuid,
        data_dir,
        advertise_addr,
        election_timeout,
        heartbeat_interval,
        rpc_timeout,
        snapshot_log_entries,
        snapshot_keep_log_entries,
        event_tap_capacity,
        tls,
    } = options;

    // Step 1: the directory must exist (the caller owns creating it).
    if !data_dir.is_dir() {
        return Err(NodeStartError::MissingDataDir(data_dir));
    }
    let fs = RealFs::new(data_dir.clone());

    // Step 2 + 3: the ADR 0016 identity matrix. Each arm either proceeds to
    // open, stamps a fresh directory, or fail-stops with an operator-actionable
    // message.
    let initialized = fs.exists(Path::new("manifest"))?;
    match (intent, initialized) {
        (StartIntent::Restart, true) => {}
        (StartIntent::Restart, false) => {
            return Err(NodeStartError::RefusedStart(format!(
                "data directory {} has no manifest: refusing to start on an unexpectedly empty \
                 directory — a failed mount is indistinguishable from a fresh disk; pass \
                 --bootstrap (first node of a new cluster) or --join (fresh replacement instance) \
                 if this is deliberate (ADR 0016)",
                data_dir.display()
            )));
        }
        (StartIntent::Bootstrap | StartIntent::Join, true) => {
            return Err(NodeStartError::RefusedStart(format!(
                "--bootstrap/--join was passed but {} is already initialized (manifest present); \
                 intent flags are only legal on an empty directory (ADR 0016)",
                data_dir.display()
            )));
        }
        (StartIntent::Bootstrap | StartIntent::Join, false) => {
            // Mints a fresh instance UUID (ADR 0016).
            storage::init(&fs, &StorageOptions::new(cluster_uuid, node_id))?;
        }
    }

    // Step 4: recovery. An identity-stamp mismatch fail-stops inside `open`
    // with context and rides out as `Storage`.
    let mut recovered = storage::open(fs, StorageOptions::new(cluster_uuid, node_id))?;
    let last_applied_index = recovered.last_applied.map(|id| id.index).unwrap_or(0);

    // Step 5: the publishing apply task. The recovered state moves into the
    // apply loop; the publisher is seeded with a clone at the same index.
    let state = std::mem::take(&mut recovered.state);
    let (publisher, views) = ViewPublisher::new(state.clone(), ViewPublisherConfig::default());
    let (tap, event_tap) = EventTap::channel(event_tap_capacity);
    let (apply_tx, apply_rx) = mpsc::channel(APPLY_CHANNEL_CAPACITY);
    tokio::spawn(apply_loop::run(
        state,
        last_applied_index,
        apply_rx,
        publisher,
        tap,
    ));

    // Step 6: split the stores; grab the committed watch before they move.
    let (log_store, sm_store) = recovered.into_stores(apply_tx);
    let committed_rx = log_store.committed_watch();

    // Step 7: openraft config (durations in ms; election max = 2× min).
    let election_min = duration_ms(election_timeout);
    let install_snapshot_timeout = duration_ms(rpc_timeout).max(20_000);
    let config = Config {
        cluster_name: hex(&cluster_uuid),
        election_timeout_min: election_min,
        election_timeout_max: election_min.saturating_mul(2),
        heartbeat_interval: duration_ms(heartbeat_interval),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_log_entries),
        max_in_snapshot_log_to_keep: snapshot_keep_log_entries,
        install_snapshot_timeout,
        ..Default::default()
    }
    .validate()
    .map_err(|e| NodeStartError::Raft(format!("invalid raft config: {e}")))?;

    // Step 8: the network factory and the openraft node.
    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&tls.ca_pem))
        .identity(Identity::from_pem(&tls.cert_pem, &tls.key_pem));
    let factory = GrpcNetworkFactory::new(cluster_uuid, client_tls, rpc_timeout);
    let raft = Raft::new(node_id, Arc::new(config), factory, log_store, sm_store)
        .await
        .map_err(|e| NodeStartError::Raft(format!("raft node construction failed: {e}")))?;

    // Step 9: single-voter cluster creation on bootstrap (ADR 0016).
    if intent == StartIntent::Bootstrap {
        let members = BTreeMap::from([(
            node_id,
            BasicNode {
                addr: advertise_addr,
            },
        )]);
        raft.initialize(members).await.map_err(|e| match e {
            RaftError::APIError(InitializeError::NotAllowed(_)) => NodeStartError::RefusedStart(
                format!("--bootstrap refused: this cluster is already initialized (ADR 0016): {e}"),
            ),
            other => NodeStartError::Raft(format!("raft initialize failed: {other}")),
        })?;
    }

    // Step 10 + 11: status watch, seam, transport, handle.
    let status = status::spawn(raft.metrics(), committed_rx);
    let consensus = OpenraftConsensus::new(raft.clone(), status.clone(), views.clone());
    let transport = Server::new(RaftTransportHandler::new(raft.clone(), cluster_uuid));
    let handle = NodeHandle {
        raft,
        node_id,
        cluster_uuid,
        status,
    };

    Ok(StartedNode {
        consensus,
        views,
        event_tap,
        handle,
        transport,
    })
}

/// Milliseconds of a duration, saturating into `u64` (openraft's config unit).
fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Lowercase hex of raw identity bytes — the openraft `cluster_name`.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}
