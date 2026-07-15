//! Handwritten JSON DTOs for the `/api/v1` surface — read models and
//! write bodies alike.
//!
//! The JSON contract is owned by these types, not by protobuf: they are
//! versioned with the route prefix (`/api/v1` ⇔ this module), serialized
//! with plain serde, and mirror `web/src/api/types.ts` by name and
//! semantics (the TS side spells keys in camelCase; the web client maps
//! casing at its wire boundary). Protobuf remains the canonical format
//! for internal RPC, storage, and replication — it never leaks its wire
//! idioms (wrapped ids, stringified u64, `SCREAMING_CASE` enum names,
//! omitted empties) into these bodies. Each endpoint adds its DTOs here
//! in the change that implements it (ADR 0031, "Wire format").
//!
//! Conventions, fixed for the v1 surface:
//! - `snake_case` keys (`"cpu_millis"`) and enum values (`"unknown"`,
//!   `"oom_killed"`);
//! - ids as their typed string form (`"node-<uuid>"`, ADR 0024);
//! - integers as JSON numbers (timestamps µs, costs µCU, cpu millicores);
//! - absent optionals as explicit `null`, empty lists as `[]`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coppice_core::attempt;
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};

/// Resource quantities (mirrors `coppice_core::resource::Resources`).
///
/// `deny_unknown_fields` because this nests inside write requests: a typo
/// (`"cpu_milis"`) must be `INVALID_ARGUMENT`, not a silent zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl From<&coppice_core::resource::Resources> for Resources {
    fn from(r: &coppice_core::resource::Resources) -> Self {
        Resources {
            cpu_millis: r.cpu_millis,
            memory_bytes: r.memory_bytes,
            disk_bytes: r.disk_bytes,
        }
    }
}

/// Liveness, eventually derived from agent heartbeats (epoch fencing per
/// ADR 0009). `Unknown` is the only value produced today: the replicated
/// state records no health input (`DeclareNodeLost` bumps the epoch and
/// clears `schedulable`, indistinguishable from an operator drain), and
/// heartbeat liveness is not wired yet — reporting `Healthy` would be a
/// lie for a definitively lost node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    Unknown,
    Healthy,
    Lost,
}

/// `coppice_core::attempt::AttemptState`, flattened for display (the
/// `Terminal` outcome payload travels separately as [`AttemptView::outcome`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Accruing,
    Ready,
    Dispatching,
    Running,
    Finalizing,
    Terminal,
}

impl From<&attempt::AttemptState> for AttemptState {
    fn from(s: &attempt::AttemptState) -> Self {
        match s {
            attempt::AttemptState::Accruing => AttemptState::Accruing,
            attempt::AttemptState::Ready => AttemptState::Ready,
            attempt::AttemptState::Dispatching => AttemptState::Dispatching,
            attempt::AttemptState::Running => AttemptState::Running,
            attempt::AttemptState::Finalizing => AttemptState::Finalizing,
            attempt::AttemptState::Terminal(_) => AttemptState::Terminal,
        }
    }
}

/// Why an attempt reached `Terminal` (mirrors
/// `coppice_core::attempt::AttemptOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcomeKind {
    Exited,
    OomKilled,
    MaxRuntimeExceeded,
    Aborted,
    Revoked,
    PullFailed,
    StartFailed,
    NodeLost,
    AgentError,
}

/// Who "owns" an outcome (drives retry policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeClass {
    Success,
    UserError,
    UserRequest,
    Platform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptOutcome {
    pub kind: AttemptOutcomeKind,
    /// Present when `kind` is `Exited`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exit_code: Option<i32>,
    pub class: OutcomeClass,
}

impl From<&attempt::AttemptOutcome> for AttemptOutcome {
    fn from(o: &attempt::AttemptOutcome) -> Self {
        use attempt::AttemptOutcome as O;
        let (kind, exit_code) = match o {
            O::Exited { code } => (AttemptOutcomeKind::Exited, Some(*code)),
            O::OomKilled => (AttemptOutcomeKind::OomKilled, None),
            O::MaxRuntimeExceeded => (AttemptOutcomeKind::MaxRuntimeExceeded, None),
            O::Aborted => (AttemptOutcomeKind::Aborted, None),
            O::Revoked => (AttemptOutcomeKind::Revoked, None),
            O::PullFailed { .. } => (AttemptOutcomeKind::PullFailed, None),
            O::StartFailed { .. } => (AttemptOutcomeKind::StartFailed, None),
            O::NodeLost => (AttemptOutcomeKind::NodeLost, None),
            O::AgentError => (AttemptOutcomeKind::AgentError, None),
        };
        let class = match o.class() {
            attempt::OutcomeClass::Success => OutcomeClass::Success,
            attempt::OutcomeClass::UserError => OutcomeClass::UserError,
            attempt::OutcomeClass::UserRequest => OutcomeClass::UserRequest,
            attempt::OutcomeClass::Platform => OutcomeClass::Platform,
        };
        AttemptOutcome {
            kind,
            exit_code,
            class,
        }
    }
}

/// `coppice_core::allocation::AllocationState` as its display union.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationState {
    Accruing,
    Funded,
    Active,
    Released,
}

impl From<coppice_core::allocation::AllocationState> for AllocationState {
    fn from(s: coppice_core::allocation::AllocationState) -> Self {
        use coppice_core::allocation::AllocationState as S;
        match s {
            S::Accruing => AllocationState::Accruing,
            S::Funded => AllocationState::Funded,
            S::Active => AllocationState::Active,
            S::Released => AllocationState::Released,
        }
    }
}

/// Read-model projection of an attempt with its charge metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptView {
    pub id: AttemptId,
    pub job: JobId,
    pub node: NodeId,
    pub allocation: AllocationId,
    pub state: AttemptState,
    /// Present iff `state` is `Terminal`.
    pub outcome: Option<AttemptOutcome>,
    pub started_at_us: Option<i64>,
    pub ended_at_us: Option<i64>,
    /// µCU per second while running (cost weights × requested resources).
    pub rate_ucu_per_second: u64,
    /// Upfront charge for this attempt (trued-up at finalization).
    pub charged_ucu: u64,
}

/// Read-model projection of an allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationView {
    pub id: AllocationId,
    pub job: JobId,
    pub attempt: AttemptId,
    pub node: NodeId,
    pub requested: Resources,
    pub funded: Resources,
    pub state: AllocationState,
    /// Commit order — drives funding priority within a node.
    pub seq: u64,
}

/// Per-dimension funding progress, 0..1.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FundedFraction {
    pub cpu: f64,
    pub memory: f64,
    pub disk: f64,
}

/// An accruing allocation with funding progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccrualView {
    pub allocation: AllocationView,
    pub funded_fraction: FundedFraction,
    /// Earliest guaranteed full-funding time; `null` means unbounded.
    pub projected_start_us: Option<i64>,
}

/// Summary of a compute node's current state for the list view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSummary {
    pub id: NodeId,
    pub capacity: Resources,
    /// Sum of funded resources across non-Released allocations.
    pub allocated: Resources,
    /// Actual measured consumption; zero until agent telemetry lands.
    pub used: Resources,
    pub labels: BTreeMap<String, String>,
    /// False = draining: no new placements, running work continues.
    pub schedulable: bool,
    pub health: NodeHealth,
    /// Bumps on (re)registration or loss; fences stale agent commands.
    pub epoch: u64,
    /// Last heartbeat from the agent; `null` until agents report.
    pub last_heartbeat_us: Option<i64>,
    /// Attempts currently `Running` on this node.
    pub running_count: u32,
    /// Allocations currently `Accruing` on this node.
    pub accruing_count: u32,
}

/// `GET /api/v1/nodes` — an envelope, never a bare array, so fields can
/// be added later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNodesResponse {
    pub nodes: Vec<NodeSummary>,
}

/// `GET /api/v1/nodes/{node}` (mirrors `NodeDetail` in `types.ts`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetNodeResponse {
    pub summary: NodeSummary,
    /// Attempts currently dispatching/running/finalizing on this node.
    pub active_attempts: Vec<AttemptView>,
    /// Accruing allocations queued against this node, in funding order.
    pub accrual_queue: Vec<AccrualView>,
}

// ---------------------------------------------------------------------------
// Cluster overview
// ---------------------------------------------------------------------------

/// A job's flat display **phase**: the read-time join of its `JobState` with
/// the state of the attempt it carries (ADR 0030). Never replicated — the
/// state machine stores `Attempting(attempt)` and the attempt's own state,
/// and every UI surface that shows a single job status renders this join.
///
/// `Accruing`/`Ready`/`Dispatching` all read as `Preparing`; a `Terminal`
/// attempt under a still-`Attempting` job means resolution is completing in
/// the same apply, which reads as `Finalizing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    Submitted,
    Accepted,
    Queued,
    Preparing,
    Running,
    Finalizing,
    Succeeded,
    Failed,
    Aborted,
}

impl JobPhase {
    /// Every phase, in lifecycle order. [`QueueStats::by_state`] reports a
    /// count for each one, so a caller never has to tell "zero" from
    /// "absent"; `Ord` follows this order, so the map iterates in it.
    pub const ALL: [JobPhase; 9] = [
        JobPhase::Submitted,
        JobPhase::Accepted,
        JobPhase::Queued,
        JobPhase::Preparing,
        JobPhase::Running,
        JobPhase::Finalizing,
        JobPhase::Succeeded,
        JobPhase::Failed,
        JobPhase::Aborted,
    ];
}

/// One point in the queue's recent history (for sparklines), oldest first:
/// one closed derived-stats bucket (ADR 0032, tier 3).
///
/// A missing instant is a missing *sample* — buckets that predate the
/// process or an event-stream gap are absent from the list (their `t_us`
/// simply never appears), never rendered as zeros.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct QueueSample {
    pub t_us: i64,
    pub depth: u32,
    pub drained_per_minute: f64,
    pub arrived_per_minute: f64,
}

/// Queue depth and composition. Point-in-time fields project from
/// replicated state; the rates and `history` are **derived** fields
/// (ADR 0032's per-field re-class of ADR 0031): served from this replica's
/// in-memory bucket window, coverage-annotated, replica-local.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    /// Jobs currently in `Queued` — the same number as
    /// `by_state[JobPhase::Queued]`.
    pub depth: u32,
    /// Jobs leaving / entering the queue per minute over the recent window
    /// (the newest derived buckets).
    ///
    /// `null` when the window has no coverage — a freshly (re)started
    /// replica, or one that just lost the event stream — which is a gap,
    /// not the claim `0.0` would make ("nothing is draining").
    pub drain_rate_per_minute: Option<f64>,
    pub arrival_rate_per_minute: Option<f64>,
    /// Age of the longest-waiting `Queued` job, measured at read time
    /// against the wall clock; `null` when nothing is queued.
    pub oldest_queued_age_us: Option<i64>,
    /// Job counts by displayed phase — every [`JobPhase`], zeros included.
    pub by_state: BTreeMap<JobPhase, u32>,
    /// Recent queue history, oldest first — the retained derived buckets.
    /// Empty exactly when the rates above are `null`.
    pub history: Vec<QueueSample>,
}

/// Node counts behind the cluster's capacity totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCounts {
    pub total: u32,
    /// Registered, not draining, and not lost.
    pub schedulable: u32,
    /// Nodes reported [`NodeHealth::Lost`] — always 0 until liveness has an
    /// input (see [`NodeHealth`]), never a fabricated count.
    pub lost: u32,
}

/// Cluster-wide capacity, summed over the nodes in [`NodeCounts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCapacity {
    pub nodes: NodeCounts,
    /// Registered capacity, excluding lost nodes.
    pub capacity: Resources,
    /// Sum of funded resources across non-Released allocations.
    pub allocated: Resources,
    /// Actual measured consumption; zero until agent telemetry lands, like
    /// [`NodeSummary::used`] it sums.
    pub used: Resources,
}

/// A job's lifecycle state as event payloads render it — the raw
/// `coppice_core::job::JobState` union with `Attempting`'s attempt id
/// flattened away (the id travels on attempt-scoped events, not on a job
/// transition's `from`/`to`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStateKind {
    Submitted,
    Accepted,
    Queued,
    Attempting,
    Succeeded,
    Failed,
    Aborted,
}

impl From<coppice_core::job::JobState> for JobStateKind {
    fn from(s: coppice_core::job::JobState) -> Self {
        use coppice_core::job::JobState as S;
        match s {
            S::Submitted => JobStateKind::Submitted,
            S::Accepted => JobStateKind::Accepted,
            S::Queued => JobStateKind::Queued,
            S::Attempting(_) => JobStateKind::Attempting,
            S::Succeeded => JobStateKind::Succeeded,
            S::Failed => JobStateKind::Failed,
            S::Aborted => JobStateKind::Aborted,
        }
    }
}

/// ADR 0032's one timeline-event wire shape, shared by the overview's
/// `recent_events`, `GetJobTimeline`, and the ADR 0008 subscription payload
/// — no endpoint invents its own.
///
/// `(index, ordinal)` is the event's identity: the ordering and
/// deduplication key everywhere. `at_us` is the advisory proposer stamp —
/// it may run backwards across proposers as the index advances, and no
/// consumer may reorder or "correct" by it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    /// The producing command's Raft log index.
    pub index: u64,
    /// The event's position within that command's full batch, assigned
    /// before any filtering — a scoped stream may show gaps, never renumber.
    pub ordinal: u32,
    /// When the proposer asserted this fact (ADR 0032's flattened
    /// semantics: sub-items inherit their command's stamp).
    pub at_us: i64,
    #[serde(flatten)]
    pub body: TimelineEventBody,
}

/// The event payload: kind plus the scope keys stamped during apply
/// (mirrors `coppice_state::Event` arm for arm).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelineEventBody {
    JobSubmitted {
        job: JobId,
    },
    JobStateChanged {
        job: JobId,
        from: JobStateKind,
        to: JobStateKind,
    },
    AttemptStateChanged {
        attempt: AttemptId,
        job: JobId,
        node: NodeId,
        state: AttemptState,
    },
    AllocationFunded {
        allocation: AllocationId,
        job: JobId,
        node: NodeId,
    },
    StopRequested {
        node: NodeId,
        allocation: AllocationId,
        job: JobId,
    },
    NodeEpochBumped {
        node: NodeId,
        epoch: u64,
    },
    JobEvicted {
        job: JobId,
    },
    QuotaEntityConfigured {
        entity: QuotaEntityId,
    },
    PolicyUpdated,
    ClusterVersionBumped {
        to: u32,
    },
}

impl From<&coppice_state::Event> for TimelineEventBody {
    fn from(e: &coppice_state::Event) -> Self {
        use coppice_state::Event as E;
        match e {
            E::JobSubmitted { job } => TimelineEventBody::JobSubmitted { job: *job },
            E::JobStateChanged { job, from, to } => TimelineEventBody::JobStateChanged {
                job: *job,
                from: (*from).into(),
                to: (*to).into(),
            },
            E::AttemptStateChanged {
                attempt,
                job,
                node,
                state,
            } => TimelineEventBody::AttemptStateChanged {
                attempt: *attempt,
                job: *job,
                node: *node,
                state: state.into(),
            },
            E::AllocationFunded {
                allocation,
                job,
                node,
            } => TimelineEventBody::AllocationFunded {
                allocation: *allocation,
                job: *job,
                node: *node,
            },
            E::StopRequested {
                node,
                allocation,
                job,
            } => TimelineEventBody::StopRequested {
                node: *node,
                allocation: *allocation,
                job: *job,
            },
            E::NodeEpochBumped { node, epoch } => TimelineEventBody::NodeEpochBumped {
                node: *node,
                epoch: *epoch,
            },
            E::JobEvicted { job } => TimelineEventBody::JobEvicted { job: *job },
            E::QuotaEntityConfigured { entity } => {
                TimelineEventBody::QuotaEntityConfigured { entity: *entity }
            }
            E::PolicyUpdated => TimelineEventBody::PolicyUpdated,
            E::ClusterVersionBumped { to } => TimelineEventBody::ClusterVersionBumped { to: *to },
        }
    }
}

/// The overview's window of recent cluster events (ADR 0032, tier 1),
/// newest first, served from this replica's fanout ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentEventsWindow {
    /// Exclusive coverage cursor: the window is complete for every applied
    /// index *strictly above* this, and claims nothing at or below it
    /// (ADR 0032's honest-absence vocabulary). Empty `events` with a high
    /// cursor is a freshly restarted coordinator, not a quiet cluster.
    pub floor_index: u64,
    pub events: Vec<TimelineEvent>,
}

/// `GET /api/v1/overview` (mirrors `ClusterOverview` in `types.ts`).
///
/// Consistency is per-field (ADR 0032 amending ADR 0031): `queue.depth`,
/// `by_state`, and `capacity` are bounded reads of replicated state, while
/// the queue rates/history (derived buckets) and `recent_events` (fanout
/// ring) are derived, replica-local, and coverage-annotated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterOverviewResponse {
    /// The cluster this replica belongs to (node config, ADR 0020).
    pub cluster_id: ClusterId,
    pub queue: QueueStats,
    pub capacity: ClusterCapacity,
    pub recent_events: RecentEventsWindow,
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// Per-job retry policy (mirrors `coppice_core::job::RetryPolicy`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_retries: u32,
    /// Opt-in to retrying user-error outcomes (nonzero exit, OOM). Never
    /// applies to `MaxRuntimeExceeded` or aborts.
    pub retry_user_errors: bool,
}

impl From<RetryPolicy> for coppice_core::job::RetryPolicy {
    fn from(r: RetryPolicy) -> Self {
        coppice_core::job::RetryPolicy {
            max_retries: r.max_retries,
            retry_user_errors: r.retry_user_errors,
        }
    }
}

impl From<Resources> for coppice_core::resource::Resources {
    fn from(r: Resources) -> Self {
        coppice_core::resource::Resources {
            cpu_millis: r.cpu_millis,
            memory_bytes: r.memory_bytes,
            disk_bytes: r.disk_bytes,
        }
    }
}

/// `POST /api/v1/jobs`.
///
/// The client-minted `job` id is the submission's idempotency identity
/// (ADR 0026): retrying after a timeout, connection loss, or leader change
/// re-sends the identical request, and a repeat whose first attempt already
/// committed resolves to the same job — success with the original `JobId`,
/// never a second job. Reusing an id with a *different* payload is
/// rejected.
///
/// `deny_unknown_fields`: on a write, an unrecognized key is a client bug
/// — a typo (`"max_runtme_us"`) or wrong casing (`"maxRuntimeUs"`) would
/// otherwise silently drop its field to the default, turning e.g. a
/// bounded job into a default-priced one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitJobRequest {
    /// Client-minted job identity (`job-<uuid>`, ADR 0024) — required.
    /// Mint a fresh id per logical submission; reuse it verbatim on every
    /// retry.
    pub job: JobId,
    pub image: String,
    /// The container command line, pre-tokenized (argv semantics, no shell
    /// parsing) — required and non-empty.
    pub command: Vec<String>,
    /// Entrypoint override; absent runs the image's own entrypoint. When
    /// present, must be non-empty.
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    /// Resources requested for scheduling and isolation.
    pub requests: Resources,
    /// Resolved through the replicated multiplier table; a priority with no
    /// configured multiplier is invalid.
    #[serde(default)]
    pub priority: i32,
    /// Enforced runtime bound; absent = charged the policy default runtime.
    #[serde(default)]
    pub max_runtime_us: Option<u64>,
    /// The quota-entity leaf to charge.
    pub quota_entity: QuotaEntityId,
    /// Absent = the platform default policy.
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    /// Echo of the client-minted id from the request.
    pub job: JobId,
    /// Raft log index at which this request's command applied. Pair it
    /// with `?min_index=` on a subsequent read for read-your-writes
    /// (ADR 0007). On an idempotent repeat this is the repeat's own apply
    /// index — ≥ the original commit, so still a valid cursor.
    pub log_index: u64,
}

/// `POST /api/v1/jobs/{job}/abort` — commits a desired-state transition;
/// it does not synchronously stop the container.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortJobRequest {
    /// The path segment is authoritative; the body's `job`, when present,
    /// must match it (`{}` aborts with no reason).
    #[serde(default)]
    pub job: Option<JobId>,
    /// Optional reason, recorded in job history and events.
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AbortJobResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_summary_serializes_to_the_contract_shape() {
        let id: NodeId = "node-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let summary = NodeSummary {
            id,
            capacity: Resources {
                cpu_millis: 4000,
                memory_bytes: 8_000_000_000,
                disk_bytes: 0,
            },
            allocated: Resources {
                cpu_millis: 1000,
                memory_bytes: 1_000_000,
                disk_bytes: 0,
            },
            used: Resources {
                cpu_millis: 0,
                memory_bytes: 0,
                disk_bytes: 0,
            },
            labels: BTreeMap::from([("zone".to_string(), "a".to_string())]),
            schedulable: true,
            health: NodeHealth::Unknown,
            epoch: 3,
            last_heartbeat_us: None,
            running_count: 2,
            accruing_count: 1,
        };

        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "node-00000000-0000-0000-0000-000000000001",
                "capacity": { "cpu_millis": 4000, "memory_bytes": 8_000_000_000u64, "disk_bytes": 0 },
                "allocated": { "cpu_millis": 1000, "memory_bytes": 1_000_000, "disk_bytes": 0 },
                "used": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                "labels": { "zone": "a" },
                "schedulable": true,
                "health": "unknown",
                "epoch": 3,
                "last_heartbeat_us": null,
                "running_count": 2,
                "accruing_count": 1,
            })
        );
    }

    #[test]
    fn empty_list_serializes_as_an_empty_array() {
        let json = serde_json::to_value(ListNodesResponse { nodes: vec![] }).unwrap();
        assert_eq!(json, serde_json::json!({ "nodes": [] }));
    }

    #[test]
    fn overview_serializes_to_the_contract_shape() {
        let cluster: ClusterId = "cluster-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let job: JobId = "job-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let response = GetClusterOverviewResponse {
            cluster_id: cluster,
            queue: QueueStats {
                depth: 1,
                drain_rate_per_minute: None,
                arrival_rate_per_minute: None,
                oldest_queued_age_us: Some(5_000_000),
                by_state: JobPhase::ALL
                    .iter()
                    .map(|phase| (*phase, u32::from(*phase == JobPhase::Queued)))
                    .collect(),
                history: vec![QueueSample {
                    t_us: 10,
                    depth: 1,
                    drained_per_minute: 2.0,
                    arrived_per_minute: 4.0,
                }],
            },
            recent_events: RecentEventsWindow {
                floor_index: 3,
                events: vec![TimelineEvent {
                    index: 7,
                    ordinal: 2,
                    at_us: 5_000_100,
                    body: TimelineEventBody::JobStateChanged {
                        job,
                        from: JobStateKind::Accepted,
                        to: JobStateKind::Queued,
                    },
                }],
            },
            capacity: ClusterCapacity {
                nodes: NodeCounts {
                    total: 1,
                    schedulable: 1,
                    lost: 0,
                },
                capacity: Resources {
                    cpu_millis: 4000,
                    memory_bytes: 0,
                    disk_bytes: 0,
                },
                allocated: Resources {
                    cpu_millis: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                },
                used: Resources {
                    cpu_millis: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                },
            },
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "cluster_id": "cluster-00000000-0000-0000-0000-000000000001",
                "queue": {
                    "depth": 1,
                    // A window with no coverage keeps `null` rates — never a
                    // fabricated 0.0 (see `QueueStats`).
                    "drain_rate_per_minute": null,
                    "arrival_rate_per_minute": null,
                    "oldest_queued_age_us": 5_000_000,
                    // Every phase is present, zeros included, in lifecycle order.
                    "by_state": {
                        "submitted": 0,
                        "accepted": 0,
                        "queued": 1,
                        "preparing": 0,
                        "running": 0,
                        "finalizing": 0,
                        "succeeded": 0,
                        "failed": 0,
                        "aborted": 0,
                    },
                    "history": [{
                        "t_us": 10,
                        "depth": 1,
                        "drained_per_minute": 2.0,
                        "arrived_per_minute": 4.0,
                    }],
                },
                "capacity": {
                    "nodes": { "total": 1, "schedulable": 1, "lost": 0 },
                    "capacity": { "cpu_millis": 4000, "memory_bytes": 0, "disk_bytes": 0 },
                    "allocated": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                    "used": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                },
                // ADR 0032's shared timeline shape: identity `(index,
                // ordinal)` + advisory `at_us`, kind and scope keys flat.
                "recent_events": {
                    "floor_index": 3,
                    "events": [{
                        "index": 7,
                        "ordinal": 2,
                        "at_us": 5_000_100,
                        "kind": "job_state_changed",
                        "job": "job-00000000-0000-0000-0000-000000000002",
                        "from": "accepted",
                        "to": "queued",
                    }],
                },
            })
        );
    }

    /// The flattened tag: a payload-free kind serializes to just the
    /// identity triple plus `kind`, and unit variants round-trip.
    #[test]
    fn timeline_event_kinds_flatten_without_payload_noise() {
        let event = TimelineEvent {
            index: 9,
            ordinal: 0,
            at_us: 42,
            body: TimelineEventBody::PolicyUpdated,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "index": 9,
                "ordinal": 0,
                "at_us": 42,
                "kind": "policy_updated",
            })
        );
        let back: TimelineEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back.body, TimelineEventBody::PolicyUpdated);
    }

    #[test]
    fn minimal_submit_request_deserializes_with_defaults() {
        let job = JobId::new();
        let entity = QuotaEntityId::new();
        let req: SubmitJobRequest = serde_json::from_value(serde_json::json!({
            "job": job.to_string(),
            "image": "busybox",
            "command": ["run"],
            "requests": { "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 },
            "quota_entity": entity.to_string(),
        }))
        .expect("minimal request");

        assert_eq!(req.job, job);
        assert_eq!(req.quota_entity, entity);
        assert_eq!(req.priority, 0);
        assert_eq!(req.max_runtime_us, None);
        assert!(req.entrypoint.is_none());
        assert!(req.retry.is_none());
    }

    #[test]
    fn submit_request_requires_its_core_fields() {
        // Omitting a required field (here `requests`) is a deserialization
        // error, not a silent default — the DTO owns required-ness, unlike
        // proto3 JSON where every field is optional on the wire.
        let result: Result<SubmitJobRequest, _> = serde_json::from_value(serde_json::json!({
            "job": JobId::new().to_string(),
            "image": "busybox",
            "command": ["run"],
            "quota_entity": QuotaEntityId::new().to_string(),
        }));
        assert!(result.is_err());
    }

    #[test]
    fn submit_request_rejects_unknown_fields_at_every_level() {
        let base = serde_json::json!({
            "job": JobId::new().to_string(),
            "image": "busybox",
            "command": ["run"],
            "requests": { "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 },
            "quota_entity": QuotaEntityId::new().to_string(),
        });

        // A top-level typo would otherwise silently default the real field.
        let mut typo = base.clone();
        typo["max_runtme_us"] = serde_json::json!(3_600_000_000u64);
        assert!(serde_json::from_value::<SubmitJobRequest>(typo).is_err());

        // Wrong casing is the same failure mode, not an alias.
        let mut cased = base.clone();
        cased["maxRuntimeUs"] = serde_json::json!(3_600_000_000u64);
        assert!(serde_json::from_value::<SubmitJobRequest>(cased).is_err());

        // Nested request objects are strict too.
        let mut nested = base;
        nested["requests"]["cpu_milis"] = serde_json::json!(1);
        assert!(serde_json::from_value::<SubmitJobRequest>(nested).is_err());
    }

    #[test]
    fn submit_response_serializes_ids_bare_and_ints_as_numbers() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let json = serde_json::to_value(SubmitJobResponse { job, log_index: 7 }).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "job": "job-00000000-0000-0000-0000-000000000001",
                "log_index": 7,
            })
        );
    }

    #[test]
    fn terminal_outcome_carries_kind_class_and_exit_code() {
        let outcome: AttemptOutcome = (&attempt::AttemptOutcome::Exited { code: 3 }).into();
        let json = serde_json::to_value(outcome).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "kind": "exited", "exit_code": 3, "class": "user_error" })
        );

        // `exit_code` is an optional property, omitted — not null — when
        // the kind has none.
        let json =
            serde_json::to_value(AttemptOutcome::from(&attempt::AttemptOutcome::NodeLost)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "kind": "node_lost", "class": "platform" })
        );
    }
}
