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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use coppice_tls::TlsStore;
use openraft::error::{InitializeError, RaftError};
use openraft::{Config, Raft, SnapshotPolicy};

use coppice_net::transport::Server;

use crate::adapter::{OpenraftConsensus, TypeConfig, APPLY_CHANNEL_CAPACITY};
use crate::events::{EventTap, EventTapReceiver};
use crate::fs::{Fs, RealFs};
use crate::membership::{CoordinatorNode, LivenessAttestor, MembershipPolicy};
use crate::net::{GrpcNetworkFactory, RaftTransportHandler};
use crate::storage::{self, StorageCore, StorageOptions};
use crate::view::{StateViews, ViewPublisher, ViewPublisherConfig};
use crate::{apply_loop, status, ConsensusError, ConsensusStatus, CoordinatorId};

/// Everything a coordinator supplies to bring up its consensus replica.
///
/// Deliberately no node id: the replica's allocate-once Raft identity is
/// minted at init and read back from the data directory's manifest stamp on
/// every restart (ADR 0025) — operators never choose one.
pub struct NodeOptions {
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
    /// The shared hot-reload mTLS store (ADR 0011/0037 §6). The raft peer mesh
    /// reads the current client material from it at each (re)dial, so a rotated
    /// leaf is used on reconnect without a restart; in-flight peer connections
    /// finish on the old leaf.
    pub tls: Arc<TlsStore>,
    /// This replica's own machine identity — the CA-attested subject of its
    /// configured `[tls]` leaf (ADR 0037 §6). Bound into the initial voter's
    /// membership record at `raft.initialize` so no seat, including the first,
    /// is ever unbound. `None` until the bootstrap package extracts it from the
    /// leaf; the initial voter is then bound to the empty identity, which never
    /// collides with a real one.
    pub machine_identity: Option<String>,
    /// Node-local membership policy (cluster size, grace periods; ADR 0037 §5).
    /// Wired from config by a later package; [`MembershipPolicy::default`]
    /// carries the ADR defaults (3 / 60s / 60s) until then.
    pub membership_policy: MembershipPolicy,
    /// Optional discovery-liveness attestor (ADR 0037 §5). `Some` only for a
    /// discovery backend with liveness semantics (`ec2-asg`), which lets the
    /// leader's evidence-gated overflow removal require positive absence; `None`
    /// (the default for `static`/`dns`/`file`) contributes nothing, so a stale
    /// registration or unedited list can never block a legitimate removal.
    pub attestor: Option<Arc<dyn LivenessAttestor>>,
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
    /// The instance UUID stamped in this directory's manifest (ADR 0025),
    /// surfaced so the coordinator's `/readyz` can report it (ADR 0037 §7).
    pub instance_uuid: [u8; 16],
    /// The formation control surface (ADR 0037 §3): the durable, resumable
    /// state machine that turns a parked (uninitialized) replica into the
    /// founding single voter. Driven by `InitializeCluster` and by
    /// crash-resume-on-restart.
    pub formation: FormationControl,
}

/// The founding-formation control surface (ADR 0037 §3).
///
/// A replica always constructs its raft node at [`start`] (its identity is
/// minted eagerly on an empty directory), but a genuinely new instance leaves
/// raft **uninitialized** — it is *parked*. This handle drives the one
/// deliberate act that seeds the first voter: durably record the operator's
/// formation token into the manifest, then `raft.initialize` with this replica
/// as the single voter, binding its own machine identity into the seat (ADR
/// 0037 §6). Because the token is recorded before the irreversible
/// `raft.initialize`, a crash in that window is repaired by re-running
/// [`FormationControl::form`] with the same token on restart.
#[derive(Clone)]
pub struct FormationControl {
    raft: Raft<TypeConfig>,
    core: Arc<Mutex<StorageCore<RealFs>>>,
    node_id: CoordinatorId,
    advertise_addr: String,
    machine_identity: String,
    /// Serializes the whole read→record→initialize sequence (ADR 0037 §3;
    /// finding: concurrent InitializeCluster). Two `InitializeCluster` requests
    /// with different tokens arriving at one parked daemon must not both read
    /// "empty token, uninitialized", both record a token, and both treat the
    /// loser's `raft.initialize` `NotAllowed` as idempotent success. Shared
    /// across clones so every admin-handler clone contends on one lock.
    form_lock: Arc<tokio::sync::Mutex<()>>,
}

/// The result of a successful [`FormationControl::form`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormationOutcome {
    /// This call performed formation: token recorded, `raft.initialize` ran.
    Formed,
    /// Raft was already initialized under the same token — idempotent success.
    AlreadyFormed,
}

/// Why [`FormationControl::form`] could not proceed (ADR 0037 §3).
#[derive(Debug, thiserror::Error)]
pub enum FormationError {
    /// A different formation token is already recorded in the manifest stamp;
    /// recovery re-runs `cluster init` with `recorded`.
    #[error("a different formation token is already recorded ({recorded})")]
    ConflictingToken { recorded: String },
    /// Raft is already initialized here, but this replica holds no founding
    /// formation record (ADR 0037 §3): it is a normally-joined replica, so
    /// there is no recorded token to compare against and no bootstrap policy may
    /// be applied. Refused as a plain "already initialized" — the supplied token
    /// is neither confirmed nor allowed to seed anything. Only a replica holding
    /// the recorded token may report [`FormationOutcome::AlreadyFormed`], and
    /// only on an exact match.
    #[error("already initialized; this replica holds no formation record")]
    AlreadyInitializedNoRecord,
    /// Durably recording the token failed.
    #[error("recording the formation token: {0}")]
    Storage(String),
    /// `raft.initialize` failed for a reason other than already-initialized.
    #[error("raft initialize failed: {0}")]
    Raft(String),
}

impl FormationControl {
    /// This replica's minted raft identity (ADR 0025).
    pub fn node_id(&self) -> CoordinatorId {
        self.node_id
    }

    /// Whether raft membership already exists here (i.e. formation completed or
    /// this replica joined an existing cluster).
    pub async fn is_initialized(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(false)
    }

    /// The durable formation token recorded in the manifest, if any.
    pub fn recorded_token(&self) -> Option<String> {
        self.core
            .lock()
            .expect("storage core poisoned")
            .formation_token()
            .map(str::to_string)
    }

    /// Run the formation state machine for `token` (ADR 0037 §3, steps 1–3
    /// minus the discovery probe guard, which the coordinator's admin handler
    /// applies before calling this).
    ///
    /// - Already initialized with the recorded token matching: idempotent
    ///   success ([`FormationOutcome::AlreadyFormed`]).
    /// - Already initialized with a *different* recorded token: a conflict.
    /// - Already initialized but *no* recorded token (a normally-joined
    ///   replica): refused [`FormationError::AlreadyInitializedNoRecord`] — no
    ///   token comparison is possible and no policy is applied.
    /// - Uninitialized with a recorded token: the same token resumes formation;
    ///   a different one is a conflict.
    /// - Uninitialized, no recorded token: record it durably, then
    ///   `raft.initialize` with this replica as the single voter bound to its
    ///   own machine identity (ADR 0037 §6).
    pub async fn form(&self, token: &str) -> Result<FormationOutcome, FormationError> {
        // Serialize the whole read→record→initialize sequence (finding:
        // concurrent InitializeCluster). Held across the recorded-token read,
        // the durable record, and `raft.initialize` so two racing tokens cannot
        // both record and both claim success.
        let _guard = self.form_lock.lock().await;

        let recorded = self.recorded_token();
        let initialized = self.is_initialized().await;

        if initialized {
            return match &recorded {
                // Only the founding replica holds the recorded token; it alone
                // reports the idempotent success, and only on an exact match.
                Some(rec) if rec == token => Ok(FormationOutcome::AlreadyFormed),
                Some(rec) => Err(FormationError::ConflictingToken {
                    recorded: rec.clone(),
                }),
                // A normally-joined replica has no founding record: it can
                // neither compare tokens nor apply a bootstrap policy (ADR 0037
                // §3). Refuse as a plain "already initialized" — never
                // `AlreadyFormed`, so the caller's token is not silently
                // confirmed and the supplied policy is not applied.
                None => Err(FormationError::AlreadyInitializedNoRecord),
            };
        }

        // Uninitialized. A recorded token from an interrupted former must match
        // (a crash between stamp and initialize resumes with the same token).
        if let Some(rec) = &recorded {
            if rec != token {
                return Err(FormationError::ConflictingToken {
                    recorded: rec.clone(),
                });
            }
        }

        // Record the operator's intent durably BEFORE the irreversible
        // initialize, so a crash here completes formation on restart. The
        // storage layer refuses to overwrite a *different* existing token, so a
        // racing former that somehow slipped past the lock is still caught here;
        // on that refusal we surface the recorded winner rather than a raw IO
        // error.
        if let Err(e) = self
            .core
            .lock()
            .expect("storage core poisoned")
            .record_formation_token(token)
        {
            if let Some(rec) = self.recorded_token() {
                if rec != token {
                    return Err(FormationError::ConflictingToken { recorded: rec });
                }
            }
            return Err(FormationError::Storage(e.to_string()));
        }

        let members = BTreeMap::from([(
            self.node_id,
            CoordinatorNode::new(self.advertise_addr.clone(), self.machine_identity.clone()),
        )]);
        match self.raft.initialize(members).await {
            Ok(()) => Ok(FormationOutcome::Formed),
            // Lost the initialize race, or resumed after a crash: raft is now
            // initialized by someone. It is only an idempotent success if the
            // recorded token is ours; if a different token won, name it, and if
            // no record survives, refuse as a plain already-initialized.
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                match self.recorded_token() {
                    Some(rec) if rec == token => Ok(FormationOutcome::AlreadyFormed),
                    Some(rec) => Err(FormationError::ConflictingToken { recorded: rec }),
                    None => Err(FormationError::AlreadyInitializedNoRecord),
                }
            }
            Err(other) => Err(FormationError::Raft(other.to_string())),
        }
    }
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
    /// This replica's allocate-once Raft identity, read from the data
    /// directory's manifest stamp at start (ADR 0025).
    pub fn node_id(&self) -> CoordinatorId {
        self.node_id
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
                machine_identity: node.machine_identity.clone(),
                superseded: node.superseded,
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
    /// Log index the last snapshot covers (openraft's snapshot metric);
    /// `None` when this node has taken no snapshot yet.
    pub snapshot_last_index: Option<u64>,
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
    /// The machine identity bound to this seat (ADR 0037 §6); empty for a
    /// record written before identities were bound.
    pub machine_identity: String,
    /// Whether this voter has been superseded by a replacement learner
    /// (ADR 0037 §5).
    pub superseded: bool,
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

/// Bring up a consensus replica, deriving startup intent from the disk
/// (ADR 0037 §1).
///
/// There are no operator-supplied intent flags. Intent is *derived*:
///
/// - **Manifest present** → resume this instance from its stamp (ADR 0016/0025);
///   the cluster-UUID cross-check in `storage::open` still fail-stops a wrong
///   volume. Raft is constructed and openraft replays the log — an already-formed
///   replica comes back a member, an interrupted one comes back uninitialized.
/// - **Manifest absent** → a brand-new instance. Its allocate-once identity is
///   minted and stamped eagerly (ADR 0025), raft is constructed but left
///   **uninitialized** (parked). It never seeds itself as a voter: the only
///   paths to a first voter are the explicit [`FormationControl::form`] (ADR
///   0037 §3) and joining an existing cluster as a learner (§4). The amnesiac-
///   voter defense (ADR 0016) is therefore intact — an empty disk can only ever
///   become a learner or the deliberately-formed founder.
pub async fn start(options: NodeOptions) -> Result<StartedNode, NodeStartError> {
    let NodeOptions {
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
        machine_identity,
        membership_policy,
        attestor,
    } = options;

    // Step 1: the directory must exist (the caller owns creating it).
    if !data_dir.is_dir() {
        return Err(NodeStartError::MissingDataDir(data_dir));
    }
    let fs = RealFs::new(data_dir.clone());

    // Step 2 + 3: derive intent from the disk (ADR 0037 §1). A manifest present
    // means "resume"; absent means "new instance" — mint and stamp the identity
    // eagerly, but never seed a voter here (that is formation's or the join
    // path's job). The old empty-directory fail-stop is gone: an empty disk is a
    // new instance, and a failed mount is guarded at the unit/mount layer plus
    // the cluster-UUID stamp check in `storage::open` below.
    if !fs.exists(Path::new("manifest"))? {
        let minted = storage::init(&fs, &StorageOptions::new(cluster_uuid))?;
        tracing::info!(
            node_id = minted,
            "new instance: minted coordinator raft identity (stamped in the data \
             directory; the replica is parked until it forms or joins, ADR 0037)"
        );
    }

    // Step 4: recovery. The replica's identity comes back from the manifest
    // stamp; a cluster-stamp mismatch fail-stops inside `open` with context
    // and rides out as `Storage`.
    let mut recovered = storage::open(fs, StorageOptions::new(cluster_uuid))?;
    let node_id = recovered.node_id;
    let instance_uuid = recovered.instance_uuid;
    // A clone of the storage-core handle for the formation control surface
    // (ADR 0037 §3), captured before the stores consume `recovered`.
    let core_handle = recovered.core_handle();
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

    // Step 8: the network factory and the openraft node. The factory holds the
    // shared TLS store and rebuilds its client config from the current material
    // at each (re)dial, so a rotated leaf is picked up on reconnect (ADR 0037
    // §6).
    let factory = GrpcNetworkFactory::new(cluster_uuid, tls, rpc_timeout);
    let raft = Raft::new(node_id, Arc::new(config), factory, log_store, sm_store)
        .await
        .map_err(|e| NodeStartError::Raft(format!("raft node construction failed: {e}")))?;

    // Step 9: NO automatic single-voter creation (ADR 0037 §1). A new instance
    // is parked (raft uninitialized); the only paths to a first voter are the
    // explicit `FormationControl::form` (§3) and joining an existing cluster as
    // a learner (§4). The founding seat, when formation runs, binds this
    // replica's own machine identity (§6); an unwired identity binds the empty
    // string, which never collides with a real one.
    let machine_identity = machine_identity.unwrap_or_default();
    let formation = FormationControl {
        raft: raft.clone(),
        core: core_handle,
        node_id,
        advertise_addr,
        machine_identity,
        form_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    // Step 10 + 11: status watch, seam, transport, handle.
    let status = status::spawn(raft.metrics(), committed_rx);
    // The optional liveness attestor (ADR 0037 §5) is supplied by the caller
    // from the discovery backend: `Some` for backends with liveness semantics
    // (`ec2-asg`), `None` otherwise, in which case only replication evidence
    // gates the leader's overflow removal.
    let consensus = OpenraftConsensus::new(
        raft.clone(),
        status.clone(),
        views.clone(),
        membership_policy,
        attestor,
    );
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
        formation,
        instance_uuid,
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
