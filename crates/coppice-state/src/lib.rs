//! # coppice-state
//!
//! The deterministic replicated state machine that sits behind Raft.
//!
//! This crate defines the authoritative control-plane state and the set of
//! commands that mutate it. Application of a command **must be deterministic**:
//! given the same sequence of committed commands, every replica must reach the
//! same state. That rules out wall-clock reads, randomness, network calls,
//! expensive scheduling computation, and iteration over unordered maps during
//! apply. See `docs/architecture/high-availability.md` and the full catalog
//! and apply contract in `docs/architecture/command-catalog.md`.
//!
//! Commands commit *decisions, not computations*. Every timestamp rides in
//! the command, every id is minted by the proposer, and a command that fails
//! validation applies as a deterministic no-op recording a
//! [`RejectionReason`] — it was already committed to the log on every
//! replica, so refusing it must be just as reproducible as applying it.

use std::collections::BTreeMap;

use coppice_core::attempt::{Attempt, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, JobState};
use coppice_core::node::Node;
use coppice_core::quota::{
    ChargeRecord, CostUnits, CostWeights, DecayPolicy, PriorityMultiplier, UsageState,
    DEFAULT_PENALTY_EXPONENT_MILLI,
};
use coppice_core::allocation::Allocation;

mod apply;
pub mod command;

pub use command::Command;

/// The authoritative, replicated control-plane state.
///
/// Only durable semantic state required for correctness lives here. Derived
/// state (indexes, queue projections, UI aggregates) is rebuilt from this.
/// `BTreeMap` is used throughout to keep iteration deterministic, and every
/// field is `PartialEq` so the determinism harness can assert replica
/// equivalence structurally.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachine {
    pub jobs: BTreeMap<JobId, JobRecord>,
    pub attempts: BTreeMap<AttemptId, AttemptRecord>,
    pub allocations: BTreeMap<AllocationId, AllocationRecord>,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    /// The quota-entity tree (ADR 0005) with each entity's replicated usage
    /// accumulator.
    pub quota_entities: BTreeMap<QuotaEntityId, QuotaEntity>,
    /// Exactly the allocations in state `Accruing`, keyed `(node, seq)` so a
    /// range scan yields one node's accruing allocations in commit order —
    /// the funding order of ADR 0014. Never iterate accruals by
    /// `AllocationId`: UUID order is meaningless across histories.
    /// Derived from the allocation map, so it is not snapshotted: the proto
    /// snapshot path (`coppice_proto::convert`) rebuilds it from the
    /// Accruing `AllocationRecord`s at load.
    pub accrual_queue: BTreeMap<(NodeId, u64), AllocationId>,
    /// Commit-order sequence for allocations. Part of replicated state so it
    /// is a pure function of the command history.
    pub next_allocation_seq: u64,
    /// Replicated cluster policy (ADR 0020: never in node config files).
    pub policy: PolicyConfig,
    /// Semantic feature gate (ADR 0003), bumped only by `BumpClusterVersion`.
    pub cluster_version: u32,
    /// Count of applied log entries, accepted or rejected. Bumped on every
    /// applied command so it is a stable coordinate for `expected_version`
    /// and read-consistency cursors.
    pub version: u64,
}

/// A job's replicated record: the submitted spec plus lifecycle bookkeeping
/// owned by the apply loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub spec: Job,
    pub state: JobState,
    /// The Q32.32 priority multiplier resolved by the API at proposal time
    /// (ADR 0019: apply never sees the raw `i32` in arithmetic).
    pub multiplier: PriorityMultiplier,
    pub submitted_at_us: i64,
    /// Retries consumed. `Revoked` outcomes requeue without touching this.
    pub retries_used: u32,
    pub current_attempt: Option<AttemptId>,
    /// Every attempt this job has had, in creation order.
    pub attempts: Vec<AttemptId>,
}

/// An attempt's replicated record: the attempt itself plus the charge that
/// placement committed, kept for true-up at terminal resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub attempt: Attempt,
    /// The placement group sharing the `Ready` barrier. v1: the job's id.
    pub group: GroupId,
    pub charge: ChargeRecord,
    /// Rate and multiplier the charge used, so true-up never repriced by a
    /// later policy edit (ADR 0019).
    pub rate_ucu_per_second: u64,
    pub multiplier: PriorityMultiplier,
    /// Set when the attempt is observed `Running`. An attempt that never
    /// started has actual cost zero at true-up.
    pub started_at_us: Option<i64>,
}

/// An allocation's replicated record plus its commit-order sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationRecord {
    pub allocation: Allocation,
    /// Commit order: assigned from `next_allocation_seq` at creation.
    /// Funding iterates ascending `seq`, never id order.
    pub seq: u64,
}

/// A node's replicated record: descriptor plus its fencing epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub node: Node,
    /// Bumped on (re)registration and on loss declaration; invalidates all
    /// coordinator→agent commands issued under earlier epochs (ADR 0009).
    pub epoch: u64,
}

/// One node of the quota-entity tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEntity {
    pub parent: Option<QuotaEntityId>,
    pub name: String,
    /// The soft quota as a *stock* in µCU (ADR 0019); config tooling converts
    /// human rates.
    pub quota: CostUnits,
    pub usage: UsageState,
}

/// Maximum quota-tree depth. Bounds the ancestor walk during charging so no
/// command can turn apply into unbounded work.
pub const QUOTA_TREE_DEPTH_CAP: u32 = 32;

/// Replicated cluster policy (ADR 0020). Everything here would diverge
/// scheduling or accounting if replicas disagreed, so none of it may appear
/// in a node config file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConfig {
    pub cost_weights: CostWeights,
    pub decay: DecayPolicy,
    pub penalty_exponent_milli: u32,
    /// Maps the user-facing `priority: i32` to a Q32.32 cost multiplier. The
    /// API resolves through this table at proposal time.
    pub priority_multipliers: BTreeMap<i32, PriorityMultiplier>,
    /// K: at most this many jobs hold accruing allocations at once
    /// (ADR 0014, default 4).
    pub accrual_limit: u32,
    /// Charge-time runtime for jobs with no enforced `max_runtime`, seconds.
    pub default_charge_runtime_s: u64,
    /// Terminal jobs are eligible for `EvictTerminalJobs` this long after
    /// terminal state (ADR 0012). Consulted by the proposer, never by apply.
    pub terminal_retention_us: i64,
    /// Default SIGTERM→SIGKILL grace for aborts, microseconds.
    pub abort_grace_us: i64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        PolicyConfig {
            cost_weights: CostWeights::default(),
            decay: DecayPolicy::DEFAULT,
            penalty_exponent_milli: DEFAULT_PENALTY_EXPONENT_MILLI,
            priority_multipliers: BTreeMap::new(),
            accrual_limit: 4,
            default_charge_runtime_s: 86_400,
            terminal_retention_us: 72 * 3_600 * 1_000_000,
            abort_grace_us: 30 * 1_000_000,
        }
    }
}

/// Why a committed command was refused. The rejection is part of the
/// deterministic apply result: every replica computes the identical reason,
/// state changes only by the `version` bump, and the proposer observes it
/// through the leader's apply result. See the taxonomy table in
/// `docs/architecture/command-catalog.md`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RejectionReason {
    #[error("job {0} not found")]
    UnknownJob(JobId),
    #[error("node {0} not found")]
    UnknownNode(NodeId),
    #[error("attempt {0} not found")]
    UnknownAttempt(AttemptId),
    #[error("allocation {0} not found")]
    UnknownAllocation(AllocationId),
    #[error("quota entity {0} not found")]
    UnknownQuotaEntity(QuotaEntityId),
    #[error("job {0} already exists")]
    DuplicateJob(JobId),
    #[error("attempt {0} already exists")]
    DuplicateAttempt(AttemptId),
    #[error("allocation {0} already exists")]
    DuplicateAllocation(AllocationId),
    #[error("job {0} is terminal")]
    JobTerminal(JobId),
    #[error("job {0} is not queued")]
    JobNotQueued(JobId),
    #[error("job {0} is not terminal")]
    JobNotTerminal(JobId),
    #[error("attempt {0} already passed this transition")]
    StaleAttemptState(AttemptId),
    #[error("attempt {attempt} is not on node {node}")]
    AttemptNotOnNode { attempt: AttemptId, node: NodeId },
    #[error("allocation {0} is not accruing")]
    AllocationNotAccruing(AllocationId),
    #[error("node {0} is not schedulable")]
    NodeNotSchedulable(NodeId),
    #[error("observed set for node {node} carries epoch {got}, current is {current}")]
    StaleNodeEpoch { node: NodeId, current: u64, got: u64 },
    #[error("allocation {0} requests more than the node's total capacity")]
    RequestExceedsNodeCapacity(AllocationId),
    #[error("batch would leave more than {limit} jobs accruing")]
    AccrualLimitExceeded { limit: u32 },
    #[error("placement shape unsupported in v1 (one allocation, singleton group)")]
    UnsupportedPlacementShape,
    #[error("quota entity {0} parent chain would cycle or exceed the depth cap")]
    QuotaEntityCycle(QuotaEntityId),
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),
    #[error("cluster version {requested} is not above current {current}")]
    ClusterVersionNotMonotonic { current: u32, requested: u32 },
    #[error("command shape invalid: {0}")]
    InvalidCommand(String),
    #[error("batch rejected; per-item diagnostics attached")]
    InvalidBatch(Vec<(u32, RejectionReason)>),
}

/// Change events produced by an accepted command — derived output for the
/// event fanout (ADR 0008) and the coordinator runtime, never read back by
/// apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    JobSubmitted { job: JobId },
    JobStateChanged { job: JobId, from: JobState, to: JobState },
    AttemptStateChanged { attempt: AttemptId, state: AttemptState },
    AllocationFunded { allocation: AllocationId },
    /// An abort needs a `StopJob` sent to this node — apply does no I/O; the
    /// runtime acts on this.
    StopRequested { node: NodeId, allocation: AllocationId },
    NodeEpochBumped { node: NodeId, epoch: u64 },
    JobEvicted { job: JobId },
    QuotaEntityConfigured { entity: QuotaEntityId },
    PolicyUpdated,
    ClusterVersionBumped { to: u32 },
}

/// The successful result of applying one command.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    pub events: Vec<Event>,
}
