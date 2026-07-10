//! # coppice-consensus
//!
//! The async seam between the coordinator control plane and openraft.
//!
//! This crate is a **thin adapter over openraft 0.9** (ADR 0002), not an
//! abstraction meant to swap Raft libraries. openraft owns election,
//! replication, and membership-change correctness; the seam converts at that
//! boundary so openraft's types never appear in any other crate's signatures.
//! Everything downstream consumes the openraft-free surface defined here:
//! [`Consensus`] for proposals and reads, [`StateView`]/[`StateViews`] for
//! applied state, [`EventTap`] for the derived event stream, and
//! [`ConsensusError`] for failures.
//!
//! The runtime that wires these together — the single-writer apply task that
//! owns the mutable [`coppice_state::StateMachine`], the view publisher, the
//! event tap, and the openraft node — is documented in
//! `docs/architecture/coordinator-runtime.md`. Two coordinates run through the
//! whole design and must not be conflated: the Raft applied **log index** (the
//! read/event cursor of ADR 0007/0008) and [`coppice_state::StateMachine::version`]
//! (the applied-command count the scheduler uses for `expected_version`).
//! [`StateView`] exposes both.

use std::future::Future;

use coppice_state::Command;

mod adapter;
mod apply_loop;
mod error;
mod events;
pub mod fs;
mod net;
mod node;
mod status;
pub mod storage;
mod view;

pub use adapter::{
    ApplyRequest, ApplyResult, OpenraftConsensus, TypeConfig, APPLY_CHANNEL_CAPACITY,
    MAX_INFLIGHT_PROPOSALS, PROMOTION_LAG_MAX,
};
pub use error::{ConsensusError, ProposeError};
pub use events::{EventBatch, EventTap, EventTapReceiver, TapItem};
pub use node::{
    start, ClusterSummary, MemberSummary, NodeHandle, NodeOptions, NodeStartError, NodeTls,
    StartIntent, StartedNode,
};
pub use view::{StateView, StateViews, ViewPublisher, ViewPublisherConfig};

/// The Raft transport service type the coordinator mounts on its mTLS server.
///
/// Re-exported so the coordinator names the concrete tonic service without
/// depending on the `net` module's internals.
pub use coppice_net::transport::Server as RaftTransportServer;
/// The handler the transport server wraps (built by [`start`]).
pub use net::RaftTransportHandler;

/// Register descriptions for every metric this crate can emit, recursing into
/// each submodule that exposes metrics. Call once, after the process installs
/// its global metrics recorder.
pub fn describe_metrics() {
    apply_loop::describe_metrics();
    view::describe_metrics();
}

/// Run any point-in-time sampling behind this crate's metrics, recursing into
/// each submodule that exposes metrics. The future /metrics endpoint calls
/// this immediately before rendering a scrape.
pub fn gather_metrics() {
    apply_loop::gather_metrics();
    view::gather_metrics();
}

/// Raft identity of one coordinator replica — an instance identity, never a
/// reusable slot (ADR 0016). Distinct from [`coppice_core::id::NodeId`], which
/// identifies compute nodes.
pub type CoordinatorId = u64;

/// This replica's current view of cluster leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// This replica is the leader for `term`.
    Leader { term: u64 },
    /// This replica is a follower; `leader` is the current leader when known.
    Follower { leader: Option<CoordinatorId> },
    /// Election in progress, or metrics not yet observed.
    Unknown,
}

impl Role {
    /// Whether this replica currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }
}

/// A snapshot of this replica's leadership and progress, published through
/// [`Consensus::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusStatus {
    /// This replica's Raft identity.
    pub id: CoordinatorId,
    /// Current leadership role.
    pub role: Role,
    /// Log index of the last entry applied locally (the ADR 0007/0008 cursor).
    pub last_applied: u64,
    /// Highest committed index this replica knows of; `known_committed - last_applied`
    /// is the surfaced staleness bound for follower reads (ADR 0007).
    pub known_committed: u64,
}

/// The resolved result of a proposal: committed, applied, outcome observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Raft log index at which the command applied.
    pub log_index: u64,
    /// The deterministic apply outcome.
    ///
    /// `Err` is a *rejection* — committed as a no-op on every replica with
    /// the reason recorded (command-catalog.md); normal control flow for
    /// racing proposers, not a failure of `propose`.
    pub outcome: Result<coppice_state::Applied, coppice_state::RejectionReason>,
}

/// The proposal, read-barrier, membership, and observation surface of the
/// replicated control plane.
///
/// The leader accepts authoritative writes; followers redirect via
/// [`ConsensusError::NotLeader`]. Implementations are a thin adapter over
/// openraft (ADR 0002); see [`OpenraftConsensus`].
pub trait Consensus: Send + Sync + 'static {
    /// Propose a command.
    ///
    /// Resolves only once the command is committed AND applied, carrying the
    /// apply outcome. Backpressured by a bounded in-flight budget. On
    /// [`ConsensusError::Timeout`] the outcome is UNKNOWN — the command may
    /// still commit; proposers rely on the catalog's idempotency rules,
    /// never blind resubmission of non-idempotent intents.
    fn propose(
        &self,
        command: Command,
    ) -> impl Future<Output = Result<Applied, ConsensusError>> + Send;

    /// Linearizable read barrier (leader only): returns an index N such that
    /// reading any view with `applied_index >= N` is a strong read (ADR 0007).
    /// Pair with [`StateViews::at_least`].
    fn read_index(&self) -> impl Future<Output = Result<u64, ConsensusError>> + Send;

    /// Leadership + progress watch. Latest-value semantics; cheap to clone.
    fn status(&self) -> tokio::sync::watch::Receiver<ConsensusStatus>;

    /// Handle to published read views of applied state.
    fn views(&self) -> StateViews;

    /// Add a fresh node as a non-voting learner (ADR 0016 step 2).
    ///
    /// `addr` is the network endpoint the Raft transport dials.
    fn add_learner(
        &self,
        node: CoordinatorId,
        addr: String,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send;

    /// Promote a caught-up learner to voter, optionally removing a departed
    /// voter in the same joint-consensus change (ADR 0016 step 3).
    fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send;

    /// Remove a node from membership.
    fn remove_node(
        &self,
        node: CoordinatorId,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send;

    /// Ask consensus to build a snapshot now (housekeeping trigger; sealed
    /// segments become deletable once covered — ADR 0002/0017).
    fn trigger_snapshot(&self) -> impl Future<Output = Result<(), ConsensusError>> + Send;
}
