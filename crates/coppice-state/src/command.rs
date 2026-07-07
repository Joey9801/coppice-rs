//! Commands that mutate the replicated state machine.
//!
//! Commands are the only entries in the Raft log that change authoritative
//! state. Each carries everything needed for deterministic application:
//! proposer-minted ids, proposer-stamped timestamps (`*_at_us`, Unix µs), and
//! decisions rather than computations. The catalog — proposer, payload,
//! validation, apply effects, and rejections per command — lives in
//! `docs/architecture/command-catalog.md`; the protobuf schema (ADR 0003) is
//! frozen from that document, and these serde types mirror it field for
//! field.

use std::collections::BTreeMap;

use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::Job;
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use serde::{Deserialize, Serialize};

use crate::PolicyConfig;

/// A committed mutation to the authoritative state. One arm of the versioned
/// proto envelope; every command here is cluster-version 1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    // API-proposed.
    SubmitJob(SubmitJob),
    AbortJob(AbortJob),
    // Scheduler-proposed.
    CommitPlacements(CommitPlacements),
    DispatchAttempt(DispatchAttempt),
    // Agent ingestion: leader-normalized observed facts, never raw reports.
    RecordAttemptStarted(RecordAttemptStarted),
    RecordAttemptExited(RecordAttemptExited),
    RecordAttemptOutcome(RecordAttemptOutcome),
    ReconcileNode(ReconcileNode),
    // Node lifecycle.
    RegisterNode(RegisterNode),
    DeclareNodeLost(DeclareNodeLost),
    SetNodeSchedulable(SetNodeSchedulable),
    // Housekeeping.
    EvictTerminalJobs(EvictTerminalJobs),
    // Admin / policy.
    ConfigureQuotaEntity(ConfigureQuotaEntity),
    UpdatePolicy(UpdatePolicy),
    BumpClusterVersion(BumpClusterVersion),
}

/// Record a newly submitted job. Admission is synchronous in v1, so one
/// apply walks `Submitted → Accepted → Queued`. No quota charge here — cost
/// is charged at placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJob {
    pub job: Job,
    /// Resolved by the API from the replicated multiplier table; apply never
    /// sees the raw `priority: i32` in arithmetic (ADR 0019).
    pub multiplier: PriorityMultiplier,
    pub submitted_at_us: i64,
}

/// Request an abort: sets `abort_requested`, terminates immediately when no
/// agent interaction is needed, otherwise emits `StopRequested` and lets the
/// outcome arrive through ingestion. Legal in every non-terminal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortJob {
    pub job: JobId,
    pub reason: Option<String>,
    pub requested_at_us: i64,
}

/// One scheduler pass's atomic batch of placements and revocations.
/// All-or-nothing: any invalid item rejects the whole batch with per-item
/// diagnostics, and the scheduler recomputes — failed proposals are normal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlacements {
    /// The state version the scheduler's snapshot was taken at. Audit
    /// record: semantic re-validation of each item is what actually gates
    /// the batch (see the apply contract).
    pub expected_version: u64,
    /// Accruing allocations to revoke (attempt outcome `Revoked`, requeued
    /// free of retry budget). Applied before placements so freed capacity
    /// pledges onward in commit order first.
    pub revocations: Vec<AllocationId>,
    pub placements: Vec<Placement>,
    /// Charge timestamp for the batch's quota charges and refunds.
    pub proposed_at_us: i64,
}

/// One job seated on one or more nodes. `allocations` is plural for the
/// gang-scheduling seam; v1 writers emit exactly one and set `group` to the
/// job's id, and apply rejects other shapes (`UnsupportedPlacementShape`) —
/// a committed multi-allocation placement must be representable so every
/// replica computes the identical rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub job: JobId,
    pub attempt: AttemptId,
    pub group: GroupId,
    pub allocations: Vec<AllocationSpec>,
}

/// The allocation a placement creates. Whether it starts `Funded` or
/// `Accruing` is decided by apply from actual free capacity, not by the
/// proposer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationSpec {
    pub id: AllocationId,
    pub node: NodeId,
    pub requested: Resources,
}

/// Commit `Ready → Dispatching` **before** `StartJob` is sent: the
/// replicated dispatching/running set is the "intended" side of the
/// ObservedSet diff, so a crash between commit and send reconciles as lost,
/// never as an untracked container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchAttempt {
    pub attempt: AttemptId,
    pub dispatched_at_us: i64,
}

/// Container observed running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAttemptStarted {
    pub attempt: AttemptId,
    pub observed_at_us: i64,
}

/// Exit observed; agent-side finalization still in flight. Skippable — the
/// terminal edge exists from every non-terminal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAttemptExited {
    pub attempt: AttemptId,
    pub observed_at_us: i64,
}

/// Terminal outcome for an attempt. Runs the full terminal path: release +
/// funding cascade, quota true-up, and job resolution (retry policy,
/// abort-wins-over-retry, truth-wins-the-race) in one apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAttemptOutcome {
    pub attempt: AttemptId,
    /// Any outcome except `Revoked`, which only `CommitPlacements` produces.
    pub outcome: AttemptOutcome,
    /// Normalizer-computed; ignored for true-up when the attempt never
    /// reached `Running` (actual cost is zero by rule).
    pub actual_runtime_us: u64,
    pub observed_at_us: i64,
}

/// Verdicts from the leader's ObservedSet diff (ADR 0009). "Stop" verdicts
/// never appear: an unknown running container has nothing in state to
/// mutate; the leader sends `StopJob` directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileNode {
    pub node: NodeId,
    /// The epoch the set was observed under; must match the node's current
    /// epoch or the whole set predates a re-registration.
    pub node_epoch: u64,
    /// Intended and running: confirm `Running`.
    pub adopted: Vec<AttemptId>,
    /// Intended but absent: attempt failure, retry policy applies.
    pub lost: Vec<LostAttempt>,
    pub observed_at_us: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LostAttempt {
    pub attempt: AttemptId,
    /// Normalizer-picked, typically `AgentError`; never `Revoked`.
    pub outcome: AttemptOutcome,
    pub actual_runtime_us: u64,
}

/// Node (re)registration. Re-registration bumps the node epoch, fencing all
/// commands issued under earlier epochs; the drain flag survives (an agent
/// restart must not undo an admin's drain).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNode {
    pub node: NodeId,
    pub capacity: Resources,
    pub labels: BTreeMap<String, String>,
    pub registered_at_us: i64,
}

/// Node missed the replicated heartbeat deadline: epoch bump, unschedulable,
/// and every live attempt on it terminates `NodeLost` (platform outcome —
/// retry policy applies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclareNodeLost {
    pub node: NodeId,
    pub declared_at_us: i64,
}

/// Admin drain / undrain. Drain blocks new placements only: running work and
/// existing accrual funding continue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetNodeSchedulable {
    pub node: NodeId,
    pub schedulable: bool,
    pub updated_at_us: i64,
}

/// Remove terminal jobs from replicated state (ADR 0012). Proposed by leader
/// housekeeping **only after** the history-store write is durable; the apply
/// itself just deletes. Missing ids are skipped (duplicate proposals across
/// leader changes must be idempotent); a live listed job rejects the batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictTerminalJobs {
    pub jobs: Vec<JobId>,
    pub evicted_at_us: i64,
}

/// Create or update one quota entity. Updates preserve accumulated usage —
/// reconfiguration is not an amnesty. No delete in v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureQuotaEntity {
    pub entity: QuotaEntityId,
    pub parent: Option<QuotaEntityId>,
    pub name: String,
    /// A stock in µCU; the CLI converts human rates (ADR 0019).
    pub quota: CostUnits,
    pub updated_at_us: i64,
}

/// Full replacement of the replicated policy. Human-facing forms (half-life,
/// rates) are converted by tooling before proposal; in-flight charge records
/// keep their recorded rate and multiplier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdatePolicy {
    pub policy: PolicyConfig,
    pub updated_at_us: i64,
}

/// Bump the semantic feature gate (ADR 0003). The leader refuses to propose
/// past the minimum version supported by voting members; apply enforces
/// monotonicity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BumpClusterVersion {
    pub to: u32,
    pub bumped_at_us: i64,
}
