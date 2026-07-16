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
//! - **instants as ISO 8601 / RFC 3339 strings**
//!   (`"2026-07-16T09:30:00.000000Z"`, always UTC, µs precision) — a bare
//!   integer carries neither its epoch nor its unit, and the mistake it
//!   invites (reading µs as ms) is silent and off by a thousand;
//! - **durations as whole seconds**, in a `_seconds`-suffixed key
//!   (`"max_runtime_seconds"`). Named units, but a number: a duration has no
//!   epoch or timezone to lose, which is what motivates strings for instants,
//!   and every client can do arithmetic on it without a parser;
//! - other integers as JSON numbers (costs µCU, cpu millicores);
//! - absent optionals as explicit `null`, empty lists as `[]`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coppice_core::attempt;
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::time::Timestamp;

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
    pub started_at: Option<Timestamp>,
    pub ended_at: Option<Timestamp>,
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
    pub projected_start: Option<Timestamp>,
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
    pub last_heartbeat: Option<Timestamp>,
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
/// process or an event-stream gap are absent from the list (their `t`
/// simply never appears), never rendered as zeros.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct QueueSample {
    pub t: Timestamp,
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
    pub oldest_queued_age_seconds: Option<i64>,
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
/// deduplication key everywhere. `at` is the advisory proposer stamp —
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
    pub at: Timestamp,
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
// Job list (GET /api/v1/jobs)
// ---------------------------------------------------------------------------

/// A job's summary row for the list view (mirrors `JobSummary` in
/// `types.ts`), the read-time join of a job with the attempt it carries
/// (ADR 0030).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    pub state: JobStateKind,
    /// The attempt the job is pursuing — `Some` exactly while `state` is
    /// `attempting` (the id `JobStateKind` flattened away).
    pub attempt: Option<AttemptId>,
    pub image: String,
    pub quota_entity: QuotaEntityId,
    /// `""` if the entity is (impossibly) absent from the tree, never a
    /// fabricated name.
    pub quota_entity_name: String,
    pub priority: i32,
    pub submitted_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    /// Node of the current attempt, when one exists.
    pub node: Option<NodeId>,
    /// State of the attempt `state` points at — lets a row derive its phase
    /// without a second fetch; `null` when there is no live attempt.
    pub attempt_state: Option<AttemptState>,
    /// Min funded/requested fraction across dimensions; only while the
    /// current attempt is `accruing`, `null` otherwise.
    pub funding_fraction: Option<f64>,
    /// µCU charged across the job's attempts so far. See the projection note
    /// (`project::total_charged`) on why a terminal job reports its gross
    /// charge, not the trued-up net: the true-up settles only against entity
    /// usage and is not retained per attempt.
    pub cost_ucu: u64,
    /// Outcome of the last attempt; only when the job is terminal.
    pub outcome: Option<AttemptOutcome>,
}

/// `GET /api/v1/jobs` — an envelope with the keyset-pagination cursor
/// (ADR 0031 as amended: JSON filter AST, no `total`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListJobsResponse {
    pub jobs: Vec<JobSummary>,
    /// Opaque continuation token ([`JobCursor`]); `null` iff the scan
    /// reached the low end of the map. A short page with a non-null cursor
    /// means "more may exist, continue", never "done".
    pub next_cursor: Option<String>,
}

/// The keyset pagination cursor: the literal `v1:<job-id>`.
///
/// Opaque by contract (the `v1:` version tag lets the format change), so
/// its parse/format lives in exactly one place. Not base64 — the token is
/// already URL-safe and human-legible, and hiding a `job-<uuid>` behind an
/// encoding buys nothing.
pub struct JobCursor;

impl JobCursor {
    const PREFIX: &'static str = "v1:";

    /// The token for a job id.
    pub fn format(id: JobId) -> String {
        format!("{}{id}", Self::PREFIX)
    }

    /// Parse a cursor token back to its job id; anything that is not
    /// `v1:` + a valid [`JobId`] is a caller error.
    pub fn parse(token: &str) -> Result<JobId, String> {
        let rest = token
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| format!("cursor must begin with `{}`", Self::PREFIX))?;
        rest.parse::<JobId>()
            .map_err(|e| format!("invalid cursor: {e}"))
    }
}

/// Maximum nesting depth of a [`JobFilter`] tree (combinators deep).
pub const MAX_FILTER_DEPTH: usize = 8;
/// Maximum total nodes (combinators + leaves) in a [`JobFilter`] tree.
pub const MAX_FILTER_NODES: usize = 64;

/// The job-list filter AST (mirrors `JobFilter` in `types.ts`).
///
/// Externally tagged: every node is a JSON object with exactly one key, so
/// an unknown key (`label`, `submitted_by` — reserved, not implemented) or
/// a two-key object is a deserialization error, surfaced as
/// `INVALID_ARGUMENT`. The remaining shape rules that serde cannot express
/// — non-empty combinator/`in` lists, depth and node caps, at-least-one
/// bound, ordered bounds — are checked in [`JobFilter::validate`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobFilter {
    /// AND over a non-empty list.
    All(Vec<JobFilter>),
    /// OR over a non-empty list.
    Any(Vec<JobFilter>),
    Not(Box<JobFilter>),
    Phase(PhaseFilter),
    Entity(EntityFilter),
    /// Current attempt's node; an unknown node matches nothing.
    Node(NodeId),
    Image(ImageFilter),
    Id(IdFilter),
    /// Case-insensitive substring over the job id string OR the image.
    Search(String),
    Submitted(SubmittedFilter),
    Requests(RequestsFilter),
}

/// `{"phase": {"in": [...]}}` — matches the derived [`JobPhase`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseFilter {
    /// Non-empty (checked in [`JobFilter::validate`]).
    pub r#in: Vec<JobPhase>,
}

/// `{"entity": {"id": "quota-…", "scope": "subtree"}}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityFilter {
    pub id: QuotaEntityId,
    #[serde(default)]
    pub scope: EntityScope,
}

/// Entity match breadth. `Subtree` (the default) matches the entity and all
/// its descendants; `Exact` matches only the entity itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityScope {
    #[default]
    Subtree,
    Exact,
}

/// `{"image": {"contains": "…"}}` or `{"equals": "…"}` — exactly one op
/// (the single-key object enforces it).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ImageFilter {
    Contains(String),
    Equals(String),
}

/// `{"id": {"in": ["job-…", …]}}` — a malformed id fails to deserialize.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdFilter {
    /// Non-empty (checked in [`JobFilter::validate`]).
    pub r#in: Vec<JobId>,
}

/// `{"submitted": {"after": ISO8601, "before": ISO8601}}` — at least one
/// bound; `after` inclusive `≥`, `before` exclusive `<`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmittedFilter {
    #[serde(default)]
    pub after: Option<Timestamp>,
    #[serde(default)]
    pub before: Option<Timestamp>,
}

/// `{"requests": {"resource": …, "min": n, "max": n}}` — at least one
/// bound, both inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestsFilter {
    pub resource: RequestsResource,
    #[serde(default)]
    pub min: Option<u64>,
    #[serde(default)]
    pub max: Option<u64>,
}

/// The requested-resource dimension a [`RequestsFilter`] bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestsResource {
    CpuMillis,
    MemoryBytes,
    DiskBytes,
}

impl JobFilter {
    /// Enforce the shape rules serde cannot: non-empty combinator and `in`
    /// lists, the depth and node caps, and the at-least-one / ordered-bound
    /// rules on `submitted`/`requests`. Every violation names what was
    /// wrong so the handler can return it verbatim as `INVALID_ARGUMENT`.
    pub fn validate(&self) -> Result<(), String> {
        let mut nodes = 0usize;
        self.check(1, &mut nodes)
    }

    fn check(&self, depth: usize, nodes: &mut usize) -> Result<(), String> {
        if depth > MAX_FILTER_DEPTH {
            return Err(format!(
                "filter nesting exceeds the maximum depth of {MAX_FILTER_DEPTH}"
            ));
        }
        *nodes += 1;
        if *nodes > MAX_FILTER_NODES {
            return Err(format!(
                "filter exceeds the maximum of {MAX_FILTER_NODES} nodes"
            ));
        }
        match self {
            JobFilter::All(fs) => {
                if fs.is_empty() {
                    return Err("`all` filter list must be non-empty".to_string());
                }
                for f in fs {
                    f.check(depth + 1, nodes)?;
                }
            }
            JobFilter::Any(fs) => {
                if fs.is_empty() {
                    return Err("`any` filter list must be non-empty".to_string());
                }
                for f in fs {
                    f.check(depth + 1, nodes)?;
                }
            }
            JobFilter::Not(f) => f.check(depth + 1, nodes)?,
            JobFilter::Phase(p) => {
                if p.r#in.is_empty() {
                    return Err("`phase.in` must be non-empty".to_string());
                }
            }
            JobFilter::Id(i) => {
                if i.r#in.is_empty() {
                    return Err("`id.in` must be non-empty".to_string());
                }
            }
            JobFilter::Submitted(s) => {
                if s.after.is_none() && s.before.is_none() {
                    return Err("`submitted` requires at least one of `after`/`before`".to_string());
                }
                if let (Some(after), Some(before)) = (s.after, s.before) {
                    if after > before {
                        return Err(
                            "`submitted.after` must not be later than `submitted.before`"
                                .to_string(),
                        );
                    }
                }
            }
            JobFilter::Requests(r) => {
                if r.min.is_none() && r.max.is_none() {
                    return Err("`requests` requires at least one of `min`/`max`".to_string());
                }
                if let (Some(min), Some(max)) = (r.min, r.max) {
                    if min > max {
                        return Err("`requests.min` must not exceed `requests.max`".to_string());
                    }
                }
            }
            JobFilter::Entity(_)
            | JobFilter::Node(_)
            | JobFilter::Image(_)
            | JobFilter::Search(_) => {}
        }
        Ok(())
    }
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
/// — a typo (`"max_runtme_seconds"`) or wrong casing
/// (`"maxRuntimeSeconds"`) would otherwise silently drop its field to the
/// default, turning e.g. a bounded job into a default-priced one.
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
    /// Enforced runtime bound, in whole seconds; absent = charged the policy
    /// default runtime. Must be positive when present.
    #[serde(default)]
    pub max_runtime_seconds: Option<i64>,
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

// ---------------------------------------------------------------------------
// Coordinators (GET /api/v1/coordinators)
// ---------------------------------------------------------------------------

/// `GET /api/v1/coordinators` (ADR 0031, local read) — this replica's view of
/// the raft cluster: leader/term/indexes, replicated-state counts, and the
/// per-member roster. Mirrors `CoordinatorStatus` in `web/src/api/types.ts`.
///
/// Read locally off the consensus metrics and a replica-local state snapshot,
/// so every figure is "as this replica sees it" — a follower answers from its
/// own applied position, not the leader's.
#[derive(Debug, Clone, Serialize)]
pub struct GetCoordinatorStatusResponse {
    /// The cluster this replica belongs to (node config, ADR 0020).
    pub cluster_id: ClusterId,
    /// The current leader's raft id, when one is known.
    pub leader: Option<u64>,
    /// The current raft term.
    pub term: u64,
    /// Highest committed log index known to the serving replica.
    pub known_committed: u64,
    /// Highest applied log index on the serving replica.
    pub last_applied: u64,
    /// Applied-command count on the serving replica
    /// (`StateMachine::version`) — a state coordinate, distinct from the raft
    /// log index.
    pub state_version: u64,
    /// The last snapshot's coverage, or `null` when this replica has taken no
    /// snapshot yet. `size_bytes` and `taken_at` inside it are always null:
    /// `SnapshotMeta` (snapshot.proto) records neither.
    pub snapshot: Option<CoordinatorSnapshot>,
    /// Object counts in the replicated state machine.
    pub state_counts: CoordinatorStateCounts,
    /// One entry per configured cluster member.
    pub members: Vec<CoordinatorMember>,
    // `host` per-member and any cluster-wide health rollup are deliberately
    // omitted: no inter-coordinator reporting exists (openraft metrics carry
    // no host stats — the web mock invents them). Out of scope until an
    // inter-coordinator reporting channel exists.
}

/// The serving replica's last snapshot, as far as it is knowable from
/// openraft metrics. Only the covered log index is real today.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CoordinatorSnapshot {
    /// Snapshot size on disk. Always `null`: `SnapshotMeta` carries no size,
    /// and computing one would mean stat-ing snapshot files on a read path.
    pub size_bytes: Option<u64>,
    /// Log index the last snapshot covers (openraft's snapshot metric).
    pub last_included_index: u64,
    /// When the snapshot was taken. Always `null`: `SnapshotMeta` records no
    /// timestamp.
    pub taken_at: Option<Timestamp>,
    /// Applied entries since the snapshot: `last_applied − last_included_index`.
    pub entries_since_snapshot: u64,
}

/// Object counts in the replicated state machine (`StateMachine` map lengths).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CoordinatorStateCounts {
    pub jobs: u64,
    pub attempts: u64,
    pub allocations: u64,
    pub nodes: u64,
    pub quota_entities: u64,
}

/// A coordinator's role in the raft cluster, derived from the leader id and
/// its voter flag (ADR 0031): leader if it is the current leader, learner if
/// it is a non-voter, follower otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorRole {
    Leader,
    Follower,
    Learner,
}

/// One cluster member in a [`GetCoordinatorStatusResponse`].
#[derive(Debug, Clone, Serialize)]
pub struct CoordinatorMember {
    /// The member's raft id.
    pub id: u64,
    /// The address peers dial (host:port).
    pub addr: String,
    /// Derived role (see [`CoordinatorRole`]).
    pub role: CoordinatorRole,
    /// Whether the member is a voter (vs a learner).
    pub voter: bool,
    /// Highest applied index on this member: the serving replica reports its
    /// own exactly; peers are `null` (their apply progress is not tracked
    /// here — the leader observes only their *replicated* index, which
    /// feeds `replication_lag_entries` instead).
    pub last_applied: Option<u64>,
    /// Entries this member is behind the leader's committed index
    /// (`known_committed − matched`), leader-only; `null` on followers or for
    /// a member the leader has no replication entry for.
    pub replication_lag_entries: Option<u64>,
    // `host` (cpu/memory/disk fractions) and `last_seen` are omitted: neither
    // has any source (no inter-coordinator host reporting; coordinator
    // liveness is not tracked — liveness follows compute nodes). The web mock
    // invents both. Out of scope.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture instants are seconds from the epoch, so the range check
    /// cannot fire.
    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

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
            last_heartbeat: None,
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
                "last_heartbeat": null,
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
                oldest_queued_age_seconds: Some(5),
                by_state: JobPhase::ALL
                    .iter()
                    .map(|phase| (*phase, u32::from(*phase == JobPhase::Queued)))
                    .collect(),
                history: vec![QueueSample {
                    t: ts(10),
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
                    at: ts(5_000_100),
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
                    "oldest_queued_age_seconds": 5,
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
                        "t": "1970-01-01T00:00:00.000010Z",
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
                // ordinal)` + advisory `at`, kind and scope keys flat.
                "recent_events": {
                    "floor_index": 3,
                    "events": [{
                        "index": 7,
                        "ordinal": 2,
                        "at": "1970-01-01T00:00:05.000100Z",
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
            at: ts(42),
            body: TimelineEventBody::PolicyUpdated,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "index": 9,
                "ordinal": 0,
                "at": "1970-01-01T00:00:00.000042Z",
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
        assert_eq!(req.max_runtime_seconds, None);
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
        typo["max_runtme_seconds"] = serde_json::json!(3_600);
        assert!(serde_json::from_value::<SubmitJobRequest>(typo).is_err());

        // Wrong casing is the same failure mode, not an alias.
        let mut cased = base.clone();
        cased["maxRuntimeSeconds"] = serde_json::json!(3_600);
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

    // ---- job list ---------------------------------------------------------

    fn parse_filter(json: serde_json::Value) -> Result<JobFilter, serde_json::Error> {
        serde_json::from_value(json)
    }

    #[test]
    fn every_leaf_deserializes_from_the_contract_json() {
        let job = JobId::new().to_string();
        let entity = QuotaEntityId::new().to_string();
        let node = NodeId::new().to_string();
        let cases = serde_json::json!([
            {"all": [{"phase": {"in": ["queued"]}}]},
            {"any": [{"search": "x"}]},
            {"not": {"search": "x"}},
            {"phase": {"in": ["queued", "running"]}},
            {"entity": {"id": entity, "scope": "subtree"}},
            {"entity": {"id": entity}},
            {"node": node},
            {"image": {"contains": "alpine"}},
            {"image": {"equals": "alpine:3"}},
            {"id": {"in": [job]}},
            {"search": "needle"},
            {"submitted": {"after": "2026-07-16T00:00:00.000000Z"}},
            {"requests": {"resource": "cpu_millis", "min": 1000}},
        ]);
        for case in cases.as_array().unwrap() {
            let filter = parse_filter(case.clone()).expect("leaf parses");
            filter.validate().expect("leaf validates");
        }
    }

    #[test]
    fn entity_scope_defaults_to_subtree() {
        let entity = QuotaEntityId::new().to_string();
        let filter = parse_filter(serde_json::json!({"entity": {"id": entity}})).unwrap();
        match filter {
            JobFilter::Entity(e) => assert_eq!(e.scope, EntityScope::Subtree),
            other => panic!("expected entity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_keys_and_variants_are_rejected() {
        // An unknown top-level variant (reserved `label`, or a typo).
        assert!(parse_filter(serde_json::json!({"label": "x"})).is_err());
        // A two-key object is not a single externally-tagged variant.
        assert!(parse_filter(serde_json::json!({"any": [], "all": []})).is_err());
        // An unknown field inside a leaf struct.
        assert!(parse_filter(serde_json::json!({"phase": {"in": ["queued"], "x": 1}})).is_err());
        // An unknown phase value.
        assert!(parse_filter(serde_json::json!({"phase": {"in": ["nope"]}})).is_err());
        // A malformed id.
        assert!(parse_filter(serde_json::json!({"id": {"in": ["not-a-job"]}})).is_err());
    }

    #[test]
    fn image_requires_exactly_one_op() {
        // Two ops in one object → not a single-variant enum.
        assert!(
            parse_filter(serde_json::json!({"image": {"contains": "a", "equals": "b"}})).is_err()
        );
    }

    #[test]
    fn validate_rejects_empty_lists() {
        assert!(parse_filter(serde_json::json!({"all": []}))
            .unwrap()
            .validate()
            .is_err());
        assert!(parse_filter(serde_json::json!({"any": []}))
            .unwrap()
            .validate()
            .is_err());
        assert!(parse_filter(serde_json::json!({"phase": {"in": []}}))
            .unwrap()
            .validate()
            .is_err());
        assert!(parse_filter(serde_json::json!({"id": {"in": []}}))
            .unwrap()
            .validate()
            .is_err());
    }

    #[test]
    fn validate_enforces_the_depth_cap() {
        // 8 `not`s wrapping a leaf = depth 9 > 8.
        let mut inner = JobFilter::Search("x".to_string());
        for _ in 0..8 {
            inner = JobFilter::Not(Box::new(inner));
        }
        assert!(inner.validate().is_err());
        // One fewer is exactly at the cap.
        let mut ok = JobFilter::Search("x".to_string());
        for _ in 0..7 {
            ok = JobFilter::Not(Box::new(ok));
        }
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_enforces_the_node_cap() {
        // `all` (1 node) + 64 leaves = 65 > 64.
        let leaves = (0..64)
            .map(|_| JobFilter::Search("x".to_string()))
            .collect();
        assert!(JobFilter::All(leaves).validate().is_err());
        // 63 leaves = 64 nodes, exactly the cap.
        let leaves = (0..63)
            .map(|_| JobFilter::Search("x".to_string()))
            .collect();
        assert!(JobFilter::All(leaves).validate().is_ok());
    }

    #[test]
    fn validate_enforces_bound_rules() {
        // submitted needs at least one bound.
        assert!(parse_filter(serde_json::json!({"submitted": {}}))
            .unwrap()
            .validate()
            .is_err());
        // after must not be later than before.
        assert!(parse_filter(serde_json::json!({
            "submitted": {"after": "2026-07-16T02:00:00.000000Z", "before": "2026-07-16T01:00:00.000000Z"}
        }))
        .unwrap()
        .validate()
        .is_err());
        // requests needs at least one bound.
        assert!(
            parse_filter(serde_json::json!({"requests": {"resource": "disk_bytes"}}))
                .unwrap()
                .validate()
                .is_err()
        );
        // min must not exceed max.
        assert!(parse_filter(serde_json::json!({
            "requests": {"resource": "memory_bytes", "min": 10, "max": 5}
        }))
        .unwrap()
        .validate()
        .is_err());
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let id: JobId = "job-00000000-0000-0000-0000-000000000007".parse().unwrap();
        let token = JobCursor::format(id);
        assert_eq!(token, "v1:job-00000000-0000-0000-0000-000000000007");
        assert_eq!(JobCursor::parse(&token).unwrap(), id);
        // Missing version prefix.
        assert!(JobCursor::parse("job-00000000-0000-0000-0000-000000000007").is_err());
        // Right prefix, unparseable id.
        assert!(JobCursor::parse("v1:not-a-job").is_err());
    }

    #[test]
    fn job_summary_serializes_to_the_contract_shape() {
        let id: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let attempt: AttemptId = "attempt-00000000-0000-0000-0000-000000000002"
            .parse()
            .unwrap();
        let node: NodeId = "node-00000000-0000-0000-0000-000000000003".parse().unwrap();
        let entity: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000004"
            .parse()
            .unwrap();
        let summary = JobSummary {
            id,
            state: JobStateKind::Attempting,
            attempt: Some(attempt),
            image: "alpine:3".to_string(),
            quota_entity: entity,
            quota_entity_name: "team-a".to_string(),
            priority: 1,
            submitted_at: ts(9_500_000),
            terminal_at: None,
            node: Some(node),
            attempt_state: Some(AttemptState::Accruing),
            funding_fraction: Some(0.5),
            cost_ucu: 42,
            outcome: None,
        };
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "job-00000000-0000-0000-0000-000000000001",
                "state": "attempting",
                "attempt": "attempt-00000000-0000-0000-0000-000000000002",
                "image": "alpine:3",
                "quota_entity": "quota-00000000-0000-0000-0000-000000000004",
                "quota_entity_name": "team-a",
                "priority": 1,
                "submitted_at": "1970-01-01T00:00:09.500000Z",
                // Absent optionals are explicit null, never omitted.
                "terminal_at": null,
                "node": "node-00000000-0000-0000-0000-000000000003",
                "attempt_state": "accruing",
                "funding_fraction": 0.5,
                "cost_ucu": 42,
                "outcome": null,
            })
        );
    }

    #[test]
    fn list_jobs_response_carries_an_explicit_null_cursor() {
        let json = serde_json::to_value(ListJobsResponse {
            jobs: vec![],
            next_cursor: None,
        })
        .unwrap();
        assert_eq!(json, serde_json::json!({ "jobs": [], "next_cursor": null }));
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
