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

use openraft::error::{InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft, SnapshotPolicy};

use coppice_net::transport::Server;

use crate::adapter::{OpenraftConsensus, TypeConfig, APPLY_CHANNEL_CAPACITY};
use crate::contact::ContactTracker;
use crate::events::{EventTap, EventTapReceiver};
use crate::fs::{Fs, RealFs};
use crate::net::{GrpcNetworkFactory, PeerContact, RaftTransportHandler};
use crate::storage::{self, StorageOptions};
use crate::view::{StateViews, ViewPublisher, ViewPublisherConfig};
use crate::{apply_loop, status, ConsensusError, ConsensusStatus, CoordinatorId};

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
///
/// Deliberately no node id: the replica's allocate-once Raft identity is
/// minted at init and read back from the data directory's manifest stamp on
/// every restart (ADR 0025) — operators never choose one.
pub struct NodeOptions {
    /// The raft history this replica belongs to (ADR 0016, ADR 0037 §3).
    pub history_id: [u8; 16],
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
    /// The hot-reload mTLS store for the coordinator mesh (ADR 0011,
    /// ADR 0037 §4): outbound peer channels re-read the current material on
    /// every (re)dial, so a rotation reaches the raft mesh without a restart.
    pub tls: Arc<coppice_tls::TlsStore>,
    /// The expected voter-set size (ADR 0037 §7), node-local coordinator
    /// config. [`Consensus::promote_voter`](crate::Consensus::promote_voter)
    /// refuses a promotion that would push the voter count past this unless
    /// it is paired with a same-change removal that keeps the count level.
    /// `0` disables the ceiling (no configured expectation).
    pub cluster_size: usize,
    /// How long a voter may go without acknowledging leader contact before
    /// the evidence-gated removal path (ADR 0037 §7) may fold it out of the
    /// voter set. Node-local coordinator config
    /// (`[discovery].removal_grace`), read from the leader's own
    /// [`ContactTracker`] evidence — never from log-position progress, which
    /// stalls identically for a dead peer and an idle-but-live one.
    pub removal_grace: Duration,
    /// How long a learner may go without acknowledging leader contact before
    /// the periodic learner-GC task (ADR 0037 §7) retires its bound machine
    /// identity and removes its seat. Node-local coordinator config
    /// (`[discovery].learner_expiry`).
    pub learner_expiry: Duration,
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
    /// Stamps the ADR 0037 §3 `formation_complete` marker through this
    /// replica's open storage engine. Only the `init` path uses it.
    pub formation: Arc<dyn storage::FormationStamp>,
}

/// A cheap, cloneable admin handle to the running node.
#[derive(Clone)]
pub struct NodeHandle {
    raft: Raft<TypeConfig>,
    node_id: CoordinatorId,
    instance_uuid: [u8; 16],
    history_id: [u8; 16],
    status: watch::Receiver<ConsensusStatus>,
    /// Per-peer last-successful-RPC-response instants, recorded by this
    /// node's own transport (ADR 0037 §9): the observation behind
    /// [`replication_health`](NodeHandle::replication_health)'s `last_contact`.
    contact: PeerContact,
}

impl NodeHandle {
    /// This replica's allocate-once Raft identity, read from the data
    /// directory's manifest stamp at start (ADR 0025).
    pub fn node_id(&self) -> CoordinatorId {
        self.node_id
    }

    /// The instance UUID stamped when this data directory was initialized
    /// (ADR 0025), reported through `/readyz` (ADR 0037 §9).
    pub fn instance_uuid(&self) -> [u8; 16] {
        self.instance_uuid
    }

    /// The raft history this replica serves (ADR 0037 §3): minted at
    /// formation, stamped in the manifest, and read back from it on resume.
    /// The stamp is the authority — config carries only the logical
    /// `cluster_id`, which deliberately survives a wipe-and-re-form while
    /// this value does not.
    pub fn history_id(&self) -> [u8; 16] {
        self.history_id
    }

    /// Create the single-voter cluster this node is the sole member of
    /// (ADR 0037 §3 step 4).
    ///
    /// Separated from [`start`]'s [`StartIntent::Bootstrap`] arm so the `init` path can
    /// stamp its formation intent, mint the cluster CA, and open the store
    /// *before* the history exists — and so a crash on either side of this
    /// call is a distinguishable, testable point. Idempotent in the only way
    /// that matters: a second call against an already-initialized raft is
    /// refused by openraft and surfaces as [`NodeStartError::RefusedStart`].
    pub async fn initialize_single_voter(
        &self,
        advertise_addr: String,
    ) -> Result<(), NodeStartError> {
        let members = BTreeMap::from([(
            self.node_id,
            BasicNode {
                addr: advertise_addr,
            },
        )]);
        self.raft.initialize(members).await.map_err(|e| match e {
            RaftError::APIError(InitializeError::NotAllowed(_)) => NodeStartError::RefusedStart(
                format!("refusing to form: this raft history is already initialized: {e}"),
            ),
            other => NodeStartError::Raft(format!("raft initialize failed: {other}")),
        })
    }

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
            snapshot_last_index: m.snapshot.map(|id| id.index),
            members,
            replication,
            millis_since_quorum_ack: m.millis_since_quorum_ack,
        }
    }

    /// The per-voter facts behind the `?require=healthy` liveness test
    /// (ADR 0037 §9), read at the moment of the call.
    ///
    /// Meaningful only on a leader, and the fields say why: `matched` comes
    /// from openraft's leader-side replication metrics, and `last_contact`
    /// from this node's own transport, which only sends replication RPCs
    /// while leading. Callers gate on `leader == Some(local_id)` first.
    ///
    /// `last_log_index` is deliberately the frontier — the same one the
    /// promotion lag gate measures against (see `PROMOTION_LAG_MAX` in
    /// `adapter.rs`) — not `last_applied`: a leader with an apply backlog must
    /// not make its followers look closer than promotion would call them.
    pub fn replication_health(&self) -> ReplicationHealth {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        let voters = m
            .membership_config
            .voter_ids()
            .map(|id| VoterHealth {
                id,
                matched: m
                    .replication
                    .as_ref()
                    .and_then(|repl| repl.get(&id).copied())
                    .map(|logid| logid.map(|l| l.index).unwrap_or(0)),
                last_contact: self.contact.elapsed(id),
            })
            .collect();
        ReplicationHealth {
            local_id: self.node_id,
            leader: m.current_leader,
            term: m.current_term,
            last_log_index: m.last_log_index.unwrap_or(0),
            voters,
        }
    }
}

/// A leader's point-in-time view of each voter's replication liveness
/// (ADR 0037 §9), from [`NodeHandle::replication_health`].
#[derive(Debug, Clone)]
pub struct ReplicationHealth {
    /// This node's Raft identity.
    pub local_id: CoordinatorId,
    /// The current leader, when known.
    pub leader: Option<CoordinatorId>,
    /// The current term — a health window accumulated under one leadership
    /// must not survive into the next (ADR 0037 §9).
    pub term: u64,
    /// The leader's last log index: the canonical lag frontier, shared with
    /// the promotion gate.
    pub last_log_index: u64,
    /// One entry per current voter (the local node included).
    pub voters: Vec<VoterHealth>,
}

/// One voter in a [`ReplicationHealth`].
#[derive(Debug, Clone)]
pub struct VoterHealth {
    /// The voter's Raft identity.
    pub id: CoordinatorId,
    /// The log index the leader has confirmed replicated to this voter;
    /// `None` when no replication stream tracks it (non-leaders, and a voter
    /// the leader has not begun replicating to).
    pub matched: Option<u64>,
    /// Time since this voter last answered one of this node's RPCs; `None`
    /// when it never has in this process's life. Real transport observation,
    /// never inferred from log positions — on an idle log a dead voter's
    /// `matched` stays current forever, but its contact goes stale within a
    /// heartbeat interval.
    pub last_contact: Option<Duration>,
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
    /// Log index the last snapshot covers (openraft's snapshot metric);
    /// `None` when this node has taken no snapshot yet.
    pub snapshot_last_index: Option<u64>,
    /// Membership: id, dial address, and whether the node is a voter.
    pub members: Vec<MemberSummary>,
    /// Per-follower matched index; empty when this node is not leader.
    pub replication: Vec<(CoordinatorId, u64)>,
    /// For a leader, milliseconds since a quorum last acknowledged it
    /// (openraft's leader-lease metric); `None` on non-leaders and on a
    /// leader no quorum has acknowledged yet. The readiness gate uses it to
    /// notice a leader that has lost its cluster (ADR 0037 §9).
    pub millis_since_quorum_ack: Option<u64>,
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
        history_id,
        data_dir,
        advertise_addr,
        election_timeout,
        heartbeat_interval,
        rpc_timeout,
        snapshot_log_entries,
        snapshot_keep_log_entries,
        event_tap_capacity,
        tls,
        cluster_size,
        removal_grace,
        learner_expiry,
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
                 directory — a failed mount is indistinguishable from a fresh disk. A daemon \
                 that means to form or join a cluster does so on an EMPTY directory, and \
                 derives that intent itself (ADR 0016, ADR 0037 §1)",
                data_dir.display()
            )));
        }
        (StartIntent::Bootstrap | StartIntent::Join, true) => {
            return Err(NodeStartError::RefusedStart(format!(
                "{} is already initialized (manifest present), so it can only be resumed; \
                 forming or joining a cluster on it would discard the instance it already \
                 carries (ADR 0016)",
                data_dir.display()
            )));
        }
        (intent @ (StartIntent::Bootstrap | StartIntent::Join), false) => {
            // Mints this replica's allocate-once Raft identity and a fresh
            // instance UUID, both stamped into the manifest (ADR 0016 / 0025).
            //
            // A joiner's directory is additionally stamped as formed: its
            // history came from the cluster it probed rather than from its own
            // config, and the marker is what tells a later start not to apply
            // the config-vs-stamp wrong-volume check to it (ADR 0037 §3/§6).
            let options = StorageOptions::new(history_id);
            let minted = if intent == StartIntent::Join {
                storage::init_joined(
                    &fs,
                    &options,
                    coppice_core::time::Timestamp::now().as_micros(),
                )?
            } else {
                storage::init(&fs, &options)?
            };
            tracing::debug!(
                node_id = minted,
                ?intent,
                "minted coordinator raft identity (stamped in the data directory, ADR 0025)"
            );
        }
    }

    // Step 4: recovery. The replica's identity comes back from the manifest
    // stamp; a cluster-stamp mismatch fail-stops inside `open` with context
    // and rides out as `Storage`.
    let mut recovered = storage::open(fs, StorageOptions::new(history_id))?;
    let node_id = recovered.node_id;
    let instance_uuid = recovered.instance_uuid;
    // Taken before the stores move into openraft: formation's last step
    // (ADR 0037 §3 step 7) stamps its marker through this same engine.
    let formation = recovered.formation_stamp();
    let last_applied_index = recovered.last_applied.map(|id| id.index).unwrap_or(0);

    // Step 5: the publishing apply task. The recovered state moves into the
    // apply loop; the publisher is seeded with a clone at the same index, so
    // `views.latest()` is correct before the apply task is ever polled (the
    // coordinator runtime reads it to seed the fanout's replay floor, KOI-3).
    let state = std::mem::take(&mut recovered.state);
    let (publisher, views) = ViewPublisher::new(
        state.clone(),
        last_applied_index,
        ViewPublisherConfig::default(),
    );
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
        cluster_name: hex(&history_id),
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

    // Step 8: the network factory and the openraft node. The factory holds the
    // shared hot-reload store and rebuilds per-peer channels when the material's
    // generation advances (ADR 0037 §4).
    //
    // The evidence tracker is the shared seam between the network factory
    // (which notes every AppendEntries attempt/ack) and the evidence-gated
    // membership decisions in the consensus adapter (ADR 0037 §7).
    let evidence = Arc::new(ContactTracker::default());
    let factory = GrpcNetworkFactory::new(history_id, tls, rpc_timeout, evidence.clone());
    // Taken before the factory moves into openraft: the transport records
    // per-peer contact instants into this shared handle, and the admin handle
    // reads them for the `?require=healthy` liveness test (ADR 0037 §9).
    let contact = factory.contact();
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
                format!("refusing to form: this cluster is already initialized (ADR 0016): {e}"),
            ),
            other => NodeStartError::Raft(format!("raft initialize failed: {other}")),
        })?;
    }

    // Step 10 + 11: status watch, seam, transport, handle.
    let status = status::spawn(raft.metrics(), committed_rx);
    let consensus = OpenraftConsensus::new(
        raft.clone(),
        status.clone(),
        views.clone(),
        cluster_size,
        removal_grace,
        learner_expiry,
        evidence,
    );
    let transport = Server::new(RaftTransportHandler::new(raft.clone(), history_id));
    let handle = NodeHandle {
        raft,
        node_id,
        instance_uuid,
        history_id,
        status,
        contact,
    };

    Ok(StartedNode {
        consensus,
        views,
        event_tap,
        handle,
        transport,
        formation,
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
