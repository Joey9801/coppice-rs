//! Job list, job detail, job writes, and the query parameters that go with
//! them (`/api/v1/jobs*`).

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

use crate::entity_ref::QuotaEntityRef;
use crate::env::JobEnv;
use crate::id::{AllocationId, AttemptId, JobId, NodeId, QuotaEntityId};
use crate::metadata::{JobMetadata, MetadataError};
use crate::pagination::{JobCursor, TimelineCursor};
use crate::time::Timestamp;

use super::{
    AccrualView, AttemptOutcome, AttemptState, AttemptView, JobFilter, JobStateKind,
    QuotaEntityView, Resources, RetryPolicy,
};

/// Deserialize a float that may arrive as `null`, mapping `null` to
/// [`f64::INFINITY`].
///
/// This is the exact inverse of the serialization side: JSON has no
/// infinity, so `serde_json` renders a non-finite float as `null`, and a
/// plain `f64` field then *fails* to read its own output back. The quota
/// figures this decorates are legitimately infinite (an entity with zero
/// quota and nonzero usage is infinitely over), so every such field opts
/// into this reader; without it a client that decodes a job carrying such a
/// figure errors on an otherwise valid cluster state.
///
/// Serialization is untouched — this only affects reading.
fn null_as_infinity<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<f64>::deserialize(deserializer)?.unwrap_or(f64::INFINITY))
}

// ---------------------------------------------------------------------------
// Job list (GET /api/v1/jobs)
// ---------------------------------------------------------------------------

/// A job's summary row for the list view: the read-time join of a job with
/// the attempt it carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobSummary {
    /// The job's id.
    pub id: JobId,
    /// Its raw lifecycle state.
    pub state: JobStateKind,
    /// The attempt the job is pursuing — `Some` exactly while `state` is
    /// `attempting`.
    pub attempt: Option<AttemptId>,
    /// The container image.
    pub image: String,
    /// The quota entity the job is charged to.
    pub quota_entity: QuotaEntityId,
    /// The entity's path (ADR 0045), `""` if the entity is (impossibly)
    /// absent from the tree, never a fabricated path.
    pub quota_entity_path: String,
    /// Scheduling priority.
    pub priority: i32,
    /// When the job was submitted.
    pub submitted_at: Timestamp,
    /// The principal that submitted the job; `null` for a job with no actor
    /// on its submit command.
    pub submitted_by: Option<String>,
    /// When the job reached a terminal state, if it has.
    pub terminal_at: Option<Timestamp>,
    /// Node of the current attempt, when one exists.
    pub node: Option<NodeId>,
    /// State of the attempt `attempt` points at — lets a row derive its
    /// phase without a second fetch; `null` when there is no live attempt.
    pub attempt_state: Option<AttemptState>,
    /// Min funded/requested fraction across dimensions; only while the
    /// current attempt is `accruing`, `null` otherwise.
    pub funding_fraction: Option<f64>,
    /// Gross µCU charged across the job's attempts so far (the upfront
    /// placement charges), never the trued-up net — that is the detail's
    /// `CostReport::actual_ucu`.
    pub cost_ucu: u64,
    /// Outcome of the last attempt; only when the job is terminal.
    pub outcome: Option<AttemptOutcome>,
    /// User-owned annotations (ADR 0042) — always present, `{}` when empty.
    /// Carried on the summary so a list row can be titled by
    /// `metadata.name` without a second fetch per row.
    pub metadata: JobMetadata,
}

/// `GET /api/v1/jobs` — an envelope with the keyset-pagination cursor.
///
/// A short page with a non-null `next_cursor` means "more may exist,
/// continue"; only `next_cursor == null` means the scan reached the low end
/// of the map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListJobsResponse {
    /// The page of rows.
    pub jobs: Vec<JobSummary>,
    /// Opaque continuation token; `null` iff the scan reached the low end
    /// of the map.
    pub next_cursor: Option<JobCursor>,
}

/// The immutable submitted spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobSpecView {
    /// The container image.
    pub image: String,
    /// The container command line, pre-tokenized.
    pub command: Vec<String>,
    /// Entrypoint override; `null` runs the image's own entrypoint.
    pub entrypoint: Option<Vec<String>>,
    /// Resources requested for scheduling and isolation.
    pub requests: Resources,
    /// Scheduling priority.
    pub priority: i32,
    /// Enforced runtime bound; `null` when unbounded.
    #[serde(rename = "max_runtime_seconds", with = "crate::time::seconds::option")]
    pub max_runtime: Option<Duration>,
    /// The quota-entity leaf charged.
    pub quota_entity: QuotaEntityId,
    /// The entity's path (ADR 0045), `""` if the entity is (impossibly)
    /// absent from the tree, never a fabricated path.
    pub quota_entity_path: String,
    /// The job's retry policy.
    pub retry: RetryPolicy,
    /// The principal that submitted the job, stamped from the command's
    /// verified actor — never client-supplied. `null` for a job whose
    /// submit command carried no actor.
    pub submitted_by: Option<String>,
    /// The environment overlay, `{}` when the job set none. Fixed at
    /// submission; there is no update route. Defaulted on decode so a
    /// coordinator that predates the field still reads.
    #[serde(default)]
    pub env: JobEnv,
}

/// A committed abort request on a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AbortRequestedView {
    /// The operator-supplied reason, if any.
    pub reason: Option<String>,
    /// When the abort was requested.
    pub requested_at: Timestamp,
}

/// One entity's contribution to a queued job's penalty product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PenaltyLink {
    /// The contributing entity.
    pub entity: QuotaEntityId,
    /// Its stored name.
    pub name: String,
    /// Its path (ADR 0045).
    pub path: String,
    /// Decayed usage as of read time.
    pub usage_ucu: u64,
    /// Its configured quota.
    pub quota_ucu: u64,
    /// `null` on the wire when infinite; see `null_as_infinity`.
    #[serde(deserialize_with = "null_as_infinity")]
    pub over_quota_ratio: f64,
    /// `null` on the wire when infinite; see `null_as_infinity`.
    #[serde(deserialize_with = "null_as_infinity")]
    pub penalty: f64,
}

/// The priority-term inputs for a `Queued` job.
///
/// `rank`, `queue_depth`, `score`, and the age-bonus inputs are absent,
/// never a fabricated value: ranking the whole queue per read would cost an
/// O(queue) scan, and composing the score would duplicate scheduler-owned
/// inputs that could drift from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueuePositionExplainer {
    /// The job's priority multiplier as a real number.
    pub multiplier: f64,
    /// One entry per ancestor entity, leaf → root.
    pub penalty_chain: Vec<PenaltyLink>,
    /// Product of the chain penalties. Infinite — hence `null` on the wire,
    /// see `null_as_infinity` — whenever any link in the chain is: each
    /// link's penalty is `>= 1`, so one infinite ancestor carries through.
    #[serde(deserialize_with = "null_as_infinity")]
    pub penalty_product: f64,
    /// How long the job has been queued. The server measures it against the
    /// wall clock at read time and clamps at zero, so there is no
    /// ran-backwards case for a caller to answer for.
    #[serde(rename = "age_seconds", with = "crate::time::seconds")]
    pub age: Duration,
}

/// Per-dimension split of the base cost rate (µCU/second), summing to
/// [`CostReport::rate_ucu_per_second`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RateBreakdown {
    /// CPU's share.
    pub cpu: u64,
    /// Memory's share.
    pub memory: u64,
    /// Disk's share.
    pub disk: u64,
}

/// Which direction a finalization adjustment moved.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum TrueUpKind {
    /// Money returned to the entity.
    Refund,
    /// Extra charged to the entity.
    Surcharge,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl TrueUpKind {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, TrueUpKind::Unknown(_))
    }
}

/// A true-up adjustment applied at finalization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TrueUpView {
    /// Which direction the adjustment moved.
    pub kind: TrueUpKind,
    /// Its magnitude.
    pub amount_ucu: u64,
}

/// A job's cost breakdown, computed from replicated policy and the job's
/// charge records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CostReport {
    /// Base µCU/second from cost weights × requested resources, before any
    /// multiplier. `rate_breakdown` sums to this.
    pub rate_ucu_per_second: u64,
    /// The per-dimension split.
    pub rate_breakdown: RateBreakdown,
    /// Priority-class multiplier (`>= 1`) mapped from the job's priority by
    /// policy.
    pub priority_multiplier: f64,
    /// Runtime penalty folded in for a job with no declared max runtime;
    /// `1.0` when the job is bounded.
    pub unbounded_multiplier: f64,
    /// The µCU/second actually priced: rate × priority × unbounded.
    pub effective_rate_ucu_per_second: u64,
    /// Duration the upfront placement charge covers: the job's declared max
    /// runtime, or the policy default charge runtime when unset.
    #[serde(rename = "charge_window_seconds", with = "crate::time::seconds")]
    pub charge_window: Duration,
    /// True iff `charge_window` is the policy default (job unbounded).
    pub charge_window_is_default: bool,
    /// Upfront charge for one placement: effective_rate × charge_window.
    pub estimated_ucu: u64,
    /// Gross µCU charged across attempts so far; 0 before the job is
    /// placed.
    pub charged_ucu: u64,
    /// Fraction (0..1) of the unused charge a true-up refunds.
    pub refund_fraction: f64,
    /// Final settled cost: `charged_ucu` less the net refund (or plus the
    /// net surcharge) across the job's attempts. `null` until the job is
    /// terminal, and `null` on a terminal job when any attempt's
    /// settlement was not retained: the settled cost is then unknown, not
    /// the gross.
    pub actual_ucu: Option<u64>,
    /// The net finalization refund/surcharge across the job's attempts.
    /// `null` whenever `actual_ucu` is, and `null` on a settled job whose
    /// adjustments net to nothing (it ran to its limit, was never placed,
    /// or its retries cancelled exactly).
    pub true_up: Option<TrueUpView>,
}

/// `GET /api/v1/jobs/{job}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobDetail {
    /// The job's id.
    pub id: JobId,
    /// Its raw lifecycle state.
    pub state: JobStateKind,
    /// The immutable submitted spec.
    pub spec: JobSpecView,
    /// When the job was submitted.
    pub submitted_at: Timestamp,
    /// When the job entered its current state. **Approximated, not
    /// event-derived**: `submitted_at` while queued, the current attempt's
    /// `started_at` (falling back to `submitted_at`) while running, and
    /// `terminal_at` once terminal. Drives "in this state for …" displays;
    /// do not treat it as an exact transition instant.
    pub state_since: Timestamp,
    /// When the job reached a terminal state, if it has.
    pub terminal_at: Option<Timestamp>,
    /// Retries consumed so far.
    pub retries_used: u32,
    /// The committed abort request, if the job has one.
    pub abort_requested: Option<AbortRequestedView>,
    /// Quota-entity ancestry, root first, the owning entity last.
    pub entity_chain: Vec<QuotaEntityView>,
    /// Every attempt the job has made.
    pub attempts: Vec<AttemptView>,
    /// Present iff the job is `Queued`.
    pub queue: Option<QueuePositionExplainer>,
    /// Present iff the current attempt is accruing.
    pub accrual: Option<AccrualView>,
    /// The job's cost breakdown.
    pub cost: CostReport,
    /// User-owned annotations (ADR 0042) — always present, `{}` when empty.
    /// A sibling of `spec` rather than a field of it: the map is mutable
    /// after submission, and `spec` is the immutable submission.
    pub metadata: JobMetadata,
}

// ---------------------------------------------------------------------------
// Job timeline (GET /api/v1/jobs/{job}/timeline)
// ---------------------------------------------------------------------------

/// One timeline event, shared by `GetJobTimeline` and the subscription feed
/// — no endpoint invents its own shape.
///
/// `(index, ordinal)` is the event's identity: the ordering and
/// deduplication key everywhere. `at` is the advisory proposer stamp — it
/// may run backwards across proposers as the index advances, and no
/// consumer may reorder or "correct" by it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TimelineEvent {
    /// The producing command's Raft log index.
    pub index: u64,
    /// The event's position within that command's full batch, assigned
    /// before any filtering — a scoped stream may show gaps, never
    /// renumber.
    pub ordinal: u32,
    /// When the proposer asserted this fact.
    pub at: Timestamp,
    /// The event's kind and its scope keys.
    #[serde(flatten)]
    pub body: TimelineEventBody,
}

/// The event payload: kind plus the scope keys stamped during apply.
///
/// Internally tagged on `"kind"`. The final `Unknown` variant is this
/// client's forward-compatibility escape hatch: `#[serde(tag)]` enums
/// otherwise fail outright on a tag they do not recognize, and `Unknown` —
/// legal only because it is a unit variant — accepts any kind this client
/// predates. Its payload (the other keys on the object) is discarded; there
/// is nowhere faithful to put an arbitrary future shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TimelineEventBody {
    /// A job was submitted.
    JobSubmitted {
        /// The job.
        job: JobId,
    },
    /// A job's lifecycle state changed.
    JobStateChanged {
        /// The job.
        job: JobId,
        /// The state it left.
        from: JobStateKind,
        /// The state it entered.
        to: JobStateKind,
    },
    /// An attempt's state changed.
    AttemptStateChanged {
        /// The attempt.
        attempt: AttemptId,
        /// Its job.
        job: JobId,
        /// Its node.
        node: NodeId,
        /// The state it entered.
        state: AttemptState,
    },
    /// An allocation reached full funding.
    AllocationFunded {
        /// The allocation.
        allocation: AllocationId,
        /// Its job.
        job: JobId,
        /// Its node.
        node: NodeId,
    },
    /// A running attempt was asked to stop.
    StopRequested {
        /// The node it was asked of.
        node: NodeId,
        /// The allocation being released.
        allocation: AllocationId,
        /// The job.
        job: JobId,
    },
    /// A node's epoch bumped (re-registration or loss).
    NodeEpochBumped {
        /// The node.
        node: NodeId,
        /// Its new epoch.
        epoch: u64,
    },
    /// A job was evicted from replicated state.
    JobEvicted {
        /// The job.
        job: JobId,
    },
    /// A job's metadata map changed.
    JobMetadataUpdated {
        /// The job.
        job: JobId,
    },
    /// A quota entity was created or updated.
    QuotaEntityConfigured {
        /// The entity.
        entity: QuotaEntityId,
    },
    /// The cluster policy was updated.
    PolicyUpdated,
    /// The authorization configuration was updated.
    AuthorizationUpdated,
    /// The replicated cluster version bumped.
    ClusterVersionBumped {
        /// The version it bumped to.
        to: u32,
    },
    /// An event kind this client does not know — a newer server's
    /// vocabulary. Its payload is discarded.
    #[serde(other)]
    Unknown,
}

impl TimelineEventBody {
    /// The job this event is about, for the kinds that are about a job.
    ///
    /// `None` for the cluster-scoped kinds — a node's epoch, a quota entity,
    /// the policy, the authorization config — and for [`Unknown`], whose
    /// payload (and therefore whose scope ids) this client discarded. A
    /// consumer keying events by job must treat `None` as "not attributable",
    /// never as "some job I know nothing about".
    ///
    /// [`Unknown`]: TimelineEventBody::Unknown
    pub fn job(&self) -> Option<JobId> {
        match self {
            TimelineEventBody::JobSubmitted { job }
            | TimelineEventBody::JobStateChanged { job, .. }
            | TimelineEventBody::AttemptStateChanged { job, .. }
            | TimelineEventBody::AllocationFunded { job, .. }
            | TimelineEventBody::StopRequested { job, .. }
            | TimelineEventBody::JobEvicted { job }
            | TimelineEventBody::JobMetadataUpdated { job } => Some(*job),
            TimelineEventBody::NodeEpochBumped { .. }
            | TimelineEventBody::QuotaEntityConfigured { .. }
            | TimelineEventBody::PolicyUpdated
            | TimelineEventBody::AuthorizationUpdated
            | TimelineEventBody::ClusterVersionBumped { .. }
            | TimelineEventBody::Unknown => None,
        }
    }
}

/// `GET /api/v1/jobs/{job}/timeline` — one job's transition timeline,
/// honestly partial.
///
/// `events` are ascending by `(index, ordinal)` — the same identity and
/// order as every other timeline surface, never re-sorted by the advisory
/// `at`. `floor_index` is the **exclusive** coverage floor: nothing at or
/// below it is claimed, so the timeline is complete-from-submission exactly
/// when it contains the job's `job_submitted` event; an empty window with a
/// high floor is a truncated (aged-out) job, not a nonexistent one.
/// `next_cursor` is the opaque continuation token: a short page with a
/// non-null cursor means "continue", never "done"; only `null` means this
/// replica has nothing further retained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetJobTimelineResponse {
    /// The page of events.
    pub events: Vec<TimelineEvent>,
    /// The exclusive coverage floor.
    pub floor_index: u64,
    /// Opaque continuation token; `null` iff the ring scan reached its
    /// newest retained event.
    pub next_cursor: Option<TimelineCursor>,
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// `POST /api/v1/jobs`.
///
/// The client-minted `job` id is the submission's idempotency identity
/// (ADR 0026): retrying after a timeout, connection loss, or leader change
/// re-sends the identical request, and a repeat whose first attempt already
/// committed resolves to the same job — success with the original
/// [`JobId`], never a second job. Reusing an id with a *different* payload
/// is rejected.
///
/// `#[non_exhaustive]` blocks a struct literal; build one with
/// [`SubmitJobRequest::new`] and the `with_*` builder methods.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SubmitJobRequest {
    /// Client-minted job identity — required. Mint a fresh id per logical
    /// submission; reuse it verbatim on every retry.
    pub job: JobId,
    /// The container image.
    pub image: String,
    /// The container command line, pre-tokenized (argv semantics, no shell
    /// parsing) — required and non-empty.
    pub command: Vec<String>,
    /// Entrypoint override; absent runs the image's own entrypoint. When
    /// present, must be non-empty.
    pub entrypoint: Option<Vec<String>>,
    /// Resources requested for scheduling and isolation.
    pub requests: Resources,
    /// Resolved through the replicated multiplier table; a priority with no
    /// configured multiplier is invalid.
    pub priority: i32,
    /// Enforced runtime bound; absent = charged the policy default
    /// runtime. Must be positive when present.
    #[serde(rename = "max_runtime_seconds", with = "crate::time::seconds::option")]
    pub max_runtime: Option<Duration>,
    /// The quota-entity leaf to charge, by id or path (ADR 0045). A path is
    /// resolved against the serving replica's read view before proposing;
    /// the proposed command carries the id it resolved to.
    pub quota_entity: QuotaEntityRef,
    /// Absent = the platform default policy.
    pub retry: Option<RetryPolicy>,
    /// User-owned annotations (ADR 0042); absent is the empty map. Checked
    /// against the ADR's limits at admission, and again at apply.
    pub metadata: JobMetadata,
    /// Environment overlay for the container; absent is the empty map.
    /// Immutable after submission and part of the submission's idempotency
    /// identity, so a retry must resend it verbatim. Not a secret channel —
    /// it is replicated and served back through the API like the rest of the
    /// spec.
    #[serde(default)]
    pub env: JobEnv,
}

impl SubmitJobRequest {
    /// A minimal request: no entrypoint override, priority 0, unbounded
    /// runtime, default retry policy, empty metadata, empty environment.
    pub fn new(
        job: JobId,
        image: impl Into<String>,
        command: impl IntoIterator<Item = impl Into<String>>,
        requests: Resources,
        quota_entity: impl Into<QuotaEntityRef>,
    ) -> SubmitJobRequest {
        SubmitJobRequest {
            job,
            image: image.into(),
            command: command.into_iter().map(Into::into).collect(),
            entrypoint: None,
            requests,
            priority: 0,
            max_runtime: None,
            quota_entity: quota_entity.into(),
            retry: None,
            metadata: JobMetadata::new(),
            env: JobEnv::new(),
        }
    }

    /// Override the image's own entrypoint.
    pub fn with_entrypoint(
        mut self,
        entrypoint: impl IntoIterator<Item = impl Into<String>>,
    ) -> SubmitJobRequest {
        self.entrypoint = Some(entrypoint.into_iter().map(Into::into).collect());
        self
    }

    /// Set the scheduling priority.
    pub fn with_priority(mut self, priority: i32) -> SubmitJobRequest {
        self.priority = priority;
        self
    }

    /// Bound the runtime, and the upfront charge window with it.
    pub fn with_max_runtime(mut self, max_runtime: Duration) -> SubmitJobRequest {
        self.max_runtime = Some(max_runtime);
        self
    }

    /// Set a non-default retry policy.
    pub fn with_retry(mut self, retry: RetryPolicy) -> SubmitJobRequest {
        self.retry = Some(retry);
        self
    }

    /// Set the initial metadata map.
    pub fn with_metadata(mut self, metadata: JobMetadata) -> SubmitJobRequest {
        self.metadata = metadata;
        self
    }

    /// Set the container's environment overlay.
    pub fn with_env(mut self, env: JobEnv) -> SubmitJobRequest {
        self.env = env;
        self
    }

    /// Check the shape rules the server enforces before spending a round
    /// trip on a request that cannot succeed: `command` non-empty,
    /// `entrypoint` non-empty when present, `max_runtime` positive when
    /// present, and the metadata and environment maps within their own limits.
    pub fn validate(&self) -> Result<(), String> {
        if self.command.is_empty() {
            return Err("`command` must be non-empty".to_string());
        }
        if let Some(entrypoint) = &self.entrypoint {
            if entrypoint.is_empty() {
                return Err("`entrypoint` must be non-empty when present".to_string());
            }
        }
        if let Some(max_runtime) = self.max_runtime {
            if max_runtime.is_zero() {
                return Err("`max_runtime` must be positive when present".to_string());
            }
        }
        self.metadata.validate().map_err(|e| e.to_string())?;
        self.env.validate().map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// The answer to a submission: the job id, and the index its command
/// applied at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SubmitJobResponse {
    /// Echo of the client-minted id from the request.
    pub job: JobId,
    /// Raft log index at which this request's command applied. Pair it
    /// with `?min_index=` on a subsequent read for read-your-writes. On an
    /// idempotent repeat this is the repeat's own apply index — `>=` the
    /// original commit, so still a valid cursor.
    pub log_index: u64,
}

/// `POST /api/v1/jobs/{job}/abort` — commits a desired-state transition; it
/// does not synchronously stop the container.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AbortJobRequest {
    /// The path segment is authoritative; when present, this must match it
    /// (an empty request aborts with no reason).
    pub job: Option<JobId>,
    /// Optional reason, recorded in job history and events.
    pub reason: Option<String>,
}

impl AbortJobRequest {
    /// An abort with no reason and no echoed job id.
    pub fn new() -> AbortJobRequest {
        AbortJobRequest {
            job: None,
            reason: None,
        }
    }

    /// Echo the job id in the body (must match the path segment).
    pub fn with_job(mut self, job: JobId) -> AbortJobRequest {
        self.job = Some(job);
        self
    }

    /// Record a reason for the abort.
    pub fn with_reason(mut self, reason: impl Into<String>) -> AbortJobRequest {
        self.reason = Some(reason.into());
        self
    }
}

impl Default for AbortJobRequest {
    fn default() -> AbortJobRequest {
        AbortJobRequest::new()
    }
}

/// The answer to an abort request: empty. A write that resolved is a write
/// that applied; the job's resulting state is read back from
/// `GET /api/v1/jobs/{job}` rather than echoed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AbortJobResponse {}

/// `PUT /api/v1/jobs/{job}/metadata` (ADR 0042) — the whole map, replacing
/// whatever is stored.
///
/// `metadata` is required, unlike the patch's two halves: a body that
/// forgot the field would otherwise clear the map silently. Sending an
/// empty map explicitly still clears it, which is the point of the verb.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplaceJobMetadataRequest {
    /// The map to store, replacing whatever is there.
    pub metadata: JobMetadata,
}

impl ReplaceJobMetadataRequest {
    /// Replace the stored map with `metadata`.
    pub fn new(metadata: JobMetadata) -> ReplaceJobMetadataRequest {
        ReplaceJobMetadataRequest { metadata }
    }
}

/// `POST /api/v1/jobs/{job}/metadata` (ADR 0042) — the small patch for a
/// caller that owns only some keys: `set` merged over the stored map, then
/// `unset` keys removed.
///
/// Both halves default to empty, so the default request is a legal no-op.
/// A key in both, or a duplicate in `unset`, is rejected by
/// [`UpdateJobMetadataRequest::validate`] — the request is ambiguous about
/// intent, and guessing an order would make the answer depend on a rule
/// nobody wrote down.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UpdateJobMetadataRequest {
    /// Entries to merge over the stored map.
    pub set: JobMetadata,
    /// Keys to remove, after `set` is merged.
    pub unset: Vec<String>,
}

impl UpdateJobMetadataRequest {
    /// An empty patch (a legal no-op).
    pub fn new() -> UpdateJobMetadataRequest {
        UpdateJobMetadataRequest::default()
    }

    /// Merge one entry into `set`, checking the key and value against the
    /// ADR 0042 limits.
    pub fn set(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<UpdateJobMetadataRequest, MetadataError> {
        self.set.insert(key, value)?;
        Ok(self)
    }

    /// Remove one key from `unset`.
    pub fn unset(mut self, key: impl Into<String>) -> UpdateJobMetadataRequest {
        self.unset.push(key.into());
        self
    }

    /// The shape rules `serde` cannot express, plus the per-entry limits on
    /// `set` (via [`JobMetadata::insert`] having already checked each one).
    ///
    /// Deliberately **not** checked here: the *resulting* map. A patch is
    /// merged over state this client may not hold the latest copy of, so
    /// the key count and whole-map size of the result are the server's to
    /// judge, at apply.
    pub fn validate(&self) -> Result<(), String> {
        for key in &self.unset {
            if key.is_empty() {
                return Err("`unset` must not contain an empty key".to_string());
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for key in &self.unset {
            if !seen.insert(key.as_str()) {
                return Err(format!("`unset` names {key:?} more than once"));
            }
            if self.set.contains_key(key) {
                return Err(format!(
                    "metadata key {key:?} appears in both `set` and `unset`"
                ));
            }
        }
        Ok(())
    }
}

/// The answer to either metadata write: the job, and the index its command
/// applied at, for a read-your-writes `?min_index=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplaceJobMetadataResponse {
    /// The job that was updated.
    pub job: JobId,
    /// The apply index, for read-your-writes.
    pub log_index: u64,
}

/// The patch's answer. Identical in shape to
/// [`ReplaceJobMetadataResponse`] and deliberately a separate type: the two
/// routes are two distinct message pairs, and a client generating from a
/// message table gets the name it expects for each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UpdateJobMetadataResponse {
    /// The job that was updated.
    pub job: JobId,
    /// The apply index, for read-your-writes.
    pub log_index: u64,
}

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// The largest page size `GET /api/v1/jobs` accepts — the top of the
/// server's `1..=1000` range, which it rejects outside of rather than
/// clamping.
///
/// Worth naming because a walk that intends to read a whole set wants the
/// fewest requests it can get away with: [`JobWatcher`](crate::JobWatcher)
/// asks for exactly this.
pub const MAX_LIST_JOBS_LIMIT: u32 = 1000;

/// Query parameters for `GET /api/v1/jobs`.
///
/// The server defaults `limit` to 100 when absent, and rejects anything
/// outside `1..=`[`MAX_LIST_JOBS_LIMIT`] with `INVALID_ARGUMENT` — it is
/// never silently clamped.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct ListJobsParams {
    /// The filter tree to scope the scan to; absent matches every job.
    pub filter: Option<JobFilter>,
    /// Continue a previous scan from this cursor.
    pub cursor: Option<JobCursor>,
    /// Page size; server default 100, valid range `1..=1000`.
    pub limit: Option<u32>,
}

impl ListJobsParams {
    /// No filter, no cursor, the server's default page size.
    pub fn new() -> ListJobsParams {
        ListJobsParams::default()
    }

    /// Scope the scan to `filter`.
    pub fn with_filter(mut self, filter: JobFilter) -> ListJobsParams {
        self.filter = Some(filter);
        self
    }

    /// Continue a previous scan from `cursor`.
    pub fn with_cursor(mut self, cursor: JobCursor) -> ListJobsParams {
        self.cursor = Some(cursor);
        self
    }

    /// Request a page of `limit` rows.
    pub fn with_limit(mut self, limit: u32) -> ListJobsParams {
        self.limit = Some(limit);
        self
    }

    /// The `filter`/`cursor`/`limit` query pairs to send, each present only
    /// when set.
    pub fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(filter) = &self.filter {
            pairs.push((
                "filter",
                serde_json::to_string(filter).expect("a JobFilter always serializes"),
            ));
        }
        if let Some(cursor) = &self.cursor {
            pairs.push(("cursor", cursor.as_str().to_string()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit", limit.to_string()));
        }
        pairs
    }
}

/// Query parameters for `GET /api/v1/jobs/{job}/timeline`.
///
/// Same limit contract as [`ListJobsParams`]: server default 100, valid
/// range `1..=1000`, out of range is a 400, never clamped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct TimelineParams {
    /// Continue a previous scan from this cursor.
    pub cursor: Option<TimelineCursor>,
    /// Page size; server default 100, valid range `1..=1000`.
    pub limit: Option<u32>,
}

impl TimelineParams {
    /// No cursor, the server's default page size.
    pub fn new() -> TimelineParams {
        TimelineParams::default()
    }

    /// Continue a previous scan from `cursor`.
    pub fn with_cursor(mut self, cursor: TimelineCursor) -> TimelineParams {
        self.cursor = Some(cursor);
        self
    }

    /// Request a page of `limit` events.
    pub fn with_limit(mut self, limit: u32) -> TimelineParams {
        self.limit = Some(limit);
        self
    }

    /// The `cursor`/`limit` query pairs to send, each present only when
    /// set.
    pub fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(cursor) = &self.cursor {
            pairs.push(("cursor", cursor.as_str().to_string()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit", limit.to_string()));
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(n: u128) -> JobId {
        format!("job-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
    }

    fn quota(n: u128) -> QuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
    }

    /// The queue age is a `Duration` here and an `age_seconds` number on the
    /// wire, like every other `_seconds` key in this crate.
    #[test]
    fn a_queued_explainers_age_is_whole_seconds_on_the_wire() {
        let explainer = QueuePositionExplainer {
            multiplier: 1.0,
            penalty_chain: Vec::new(),
            penalty_product: 1.0,
            age: Duration::from_secs(30),
        };
        let json = serde_json::to_value(&explainer).unwrap();
        assert_eq!(json["age_seconds"], serde_json::json!(30));
        assert!(json.get("age").is_none());
        let back: QueuePositionExplainer = serde_json::from_value(json).unwrap();
        assert_eq!(back.age, Duration::from_secs(30));
    }

    #[test]
    fn a_job_summary_matches_the_server_shape() {
        let id = job(1);
        let entity = quota(2);
        let summary = JobSummary {
            id,
            state: JobStateKind::Queued,
            attempt: None,
            image: "ubuntu:22.04".to_string(),
            quota_entity: entity,
            quota_entity_path: "acme/team-a".to_string(),
            priority: 0,
            submitted_at: Timestamp::UNIX_EPOCH,
            submitted_by: None,
            terminal_at: None,
            node: None,
            attempt_state: None,
            funding_fraction: None,
            cost_ucu: 0,
            outcome: None,
            metadata: JobMetadata::new(),
        };
        assert_eq!(
            serde_json::to_value(&summary).unwrap(),
            serde_json::json!({
                "id": id.to_string(),
                "state": "queued",
                "attempt": null,
                "image": "ubuntu:22.04",
                "quota_entity": entity.to_string(),
                "quota_entity_path": "acme/team-a",
                "priority": 0,
                "submitted_at": "1970-01-01T00:00:00.000000Z",
                "submitted_by": null,
                "terminal_at": null,
                "node": null,
                "attempt_state": null,
                "funding_fraction": null,
                "cost_ucu": 0,
                "outcome": null,
                "metadata": {},
            })
        );
    }

    #[test]
    fn list_jobs_response_carries_an_explicit_null_cursor() {
        let response = ListJobsResponse {
            jobs: vec![],
            next_cursor: None,
        };
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({ "jobs": [], "next_cursor": null })
        );
        let with_cursor = ListJobsResponse {
            jobs: vec![],
            next_cursor: Some(JobCursor::from("v1:job-x".to_string())),
        };
        assert_eq!(
            serde_json::to_value(&with_cursor).unwrap(),
            serde_json::json!({ "jobs": [], "next_cursor": "v1:job-x" })
        );
    }

    #[test]
    fn submit_job_request_serializes_optionals_as_explicit_null() {
        let request = SubmitJobRequest::new(
            job(3),
            "ubuntu",
            ["echo", "hi"],
            Resources::new(1000, 1024, 0),
            quota(4),
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "job": job(3).to_string(),
                "image": "ubuntu",
                "command": ["echo", "hi"],
                "entrypoint": null,
                "requests": { "cpu_millis": 1000, "memory_bytes": 1024, "disk_bytes": 0 },
                "priority": 0,
                "max_runtime_seconds": null,
                "quota_entity": quota(4).to_string(),
                "retry": null,
                "metadata": {},
                "env": {},
            })
        );
        let back: SubmitJobRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, request);
        assert!(request.validate().is_ok());
    }

    /// A path ref is as valid a `quota_entity` as an id (ADR 0045).
    #[test]
    fn submit_job_request_accepts_a_path_entity_ref() {
        let path: crate::entity_ref::QuotaEntityPath = "acme/eng".parse().unwrap();
        let request = SubmitJobRequest::new(
            job(20),
            "ubuntu",
            ["echo", "hi"],
            Resources::default(),
            path.clone(),
        );
        assert_eq!(
            request.quota_entity,
            crate::entity_ref::QuotaEntityRef::Path(path)
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["quota_entity"], serde_json::json!("acme/eng"));
    }

    #[test]
    fn submit_job_request_builders_set_the_optional_fields() {
        let request =
            SubmitJobRequest::new(job(5), "ubuntu", ["run"], Resources::default(), quota(6))
                .with_entrypoint(["/bin/sh"])
                .with_priority(2)
                .with_max_runtime(Duration::from_secs(60))
                .with_retry(RetryPolicy::new(3))
                .with_metadata(JobMetadata::from_iter([("name", "nightly")]));
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["entrypoint"], serde_json::json!(["/bin/sh"]));
        assert_eq!(json["priority"], serde_json::json!(2));
        assert_eq!(json["max_runtime_seconds"], serde_json::json!(60));
        assert_eq!(
            json["retry"],
            serde_json::json!({ "max_retries": 3, "retry_user_errors": false })
        );
        assert_eq!(json["metadata"], serde_json::json!({ "name": "nightly" }));
    }

    #[test]
    fn submit_job_request_validate_rejects_empty_command() {
        let request = SubmitJobRequest::new(
            job(7),
            "ubuntu",
            Vec::<String>::new(),
            Resources::default(),
            quota(8),
        );
        assert_eq!(
            request.validate(),
            Err("`command` must be non-empty".to_string())
        );
    }

    #[test]
    fn submit_job_request_validate_rejects_a_non_positive_max_runtime() {
        let request =
            SubmitJobRequest::new(job(9), "ubuntu", ["run"], Resources::default(), quota(10))
                .with_max_runtime(Duration::ZERO);
        assert_eq!(
            request.validate(),
            Err("`max_runtime` must be positive when present".to_string())
        );
    }

    #[test]
    fn abort_job_request_serializes_both_fields_as_explicit_null_by_default() {
        let request = AbortJobRequest::new();
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({ "job": null, "reason": null })
        );
        let request = AbortJobRequest::new().with_reason("operator request");
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({ "job": null, "reason": "operator request" })
        );
    }

    #[test]
    fn abort_job_response_is_an_empty_object() {
        assert_eq!(
            serde_json::to_value(AbortJobResponse {}).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn update_job_metadata_request_validate_matches_the_server_wording() {
        let request = UpdateJobMetadataRequest::new().unset("");
        assert_eq!(
            request.validate(),
            Err("`unset` must not contain an empty key".to_string())
        );

        let request = UpdateJobMetadataRequest::new().unset("a").unset("a");
        assert_eq!(
            request.validate(),
            Err("`unset` names \"a\" more than once".to_string())
        );

        let request = UpdateJobMetadataRequest::new()
            .set("a", "1")
            .unwrap()
            .unset("a");
        assert_eq!(
            request.validate(),
            Err("metadata key \"a\" appears in both `set` and `unset`".to_string())
        );

        let request = UpdateJobMetadataRequest::new()
            .set("a", "1")
            .unwrap()
            .unset("b");
        assert_eq!(request.validate(), Ok(()));
    }

    #[test]
    fn a_timeline_event_flattens_its_body() {
        let event = TimelineEvent {
            index: 42,
            ordinal: 0,
            at: Timestamp::UNIX_EPOCH,
            body: TimelineEventBody::JobStateChanged {
                job: job(11),
                from: JobStateKind::Queued,
                to: JobStateKind::Attempting,
            },
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({
                "index": 42,
                "ordinal": 0,
                "at": "1970-01-01T00:00:00.000000Z",
                "kind": "job_state_changed",
                "job": job(11).to_string(),
                "from": "queued",
                "to": "attempting",
            })
        );
    }

    /// Every job-scoped kind reports its job, and every kind that is not
    /// about a job reports nothing — including `Unknown`, whose scope ids
    /// were discarded at decode.
    #[test]
    fn an_events_job_is_reported_for_the_kinds_that_have_one() {
        let node: NodeId = "node-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let attempt = AttemptId::new();
        let allocation = AllocationId::new();
        let scoped = [
            TimelineEventBody::JobSubmitted { job: job(1) },
            TimelineEventBody::JobStateChanged {
                job: job(1),
                from: JobStateKind::Queued,
                to: JobStateKind::Attempting,
            },
            TimelineEventBody::AttemptStateChanged {
                attempt,
                job: job(1),
                node,
                state: AttemptState::Running,
            },
            TimelineEventBody::AllocationFunded {
                allocation,
                job: job(1),
                node,
            },
            TimelineEventBody::StopRequested {
                node,
                allocation,
                job: job(1),
            },
            TimelineEventBody::JobEvicted { job: job(1) },
            TimelineEventBody::JobMetadataUpdated { job: job(1) },
        ];
        for body in scoped {
            assert_eq!(body.job(), Some(job(1)), "{body:?}");
        }
        let unscoped = [
            TimelineEventBody::NodeEpochBumped { node, epoch: 3 },
            TimelineEventBody::QuotaEntityConfigured {
                entity: QuotaEntityId::new(),
            },
            TimelineEventBody::PolicyUpdated,
            TimelineEventBody::AuthorizationUpdated,
            TimelineEventBody::ClusterVersionBumped { to: 2 },
            TimelineEventBody::Unknown,
        ];
        for body in unscoped {
            assert_eq!(body.job(), None, "{body:?}");
        }
    }

    #[test]
    fn an_unrecognized_event_kind_becomes_unknown() {
        let json = serde_json::json!({
            "index": 1,
            "ordinal": 0,
            "at": "1970-01-01T00:00:00.000000Z",
            "kind": "something_new",
            "extra": "discarded",
        });
        let event: TimelineEvent = serde_json::from_value(json).unwrap();
        assert_eq!(event.body, TimelineEventBody::Unknown);
    }

    #[test]
    fn get_job_timeline_response_carries_an_explicit_null_cursor() {
        let response = GetJobTimelineResponse {
            events: vec![],
            floor_index: 0,
            next_cursor: None,
        };
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({ "events": [], "floor_index": 0, "next_cursor": null })
        );
    }

    #[test]
    fn a_cost_report_renders_infinite_figures_as_null_and_reads_them_back() {
        let json = serde_json::json!({
            "rate_ucu_per_second": 100,
            "rate_breakdown": { "cpu": 60, "memory": 40, "disk": 0 },
            "priority_multiplier": 1.0,
            "unbounded_multiplier": 1.0,
            "effective_rate_ucu_per_second": 100,
            "charge_window_seconds": 3600,
            "charge_window_is_default": false,
            "estimated_ucu": 360_000,
            "charged_ucu": 0,
            "refund_fraction": 0.5,
            "actual_ucu": null,
            "true_up": null,
        });
        let report: CostReport = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(report.charge_window, Duration::from_secs(3600));
        assert_eq!(serde_json::to_value(&report).unwrap(), json);
    }

    #[test]
    fn a_penalty_link_reads_null_as_infinity() {
        let json = serde_json::json!({
            "entity": quota(12).to_string(),
            "name": "team-a",
            "path": "acme/team-a",
            "usage_ucu": 10,
            "quota_ucu": 0,
            "over_quota_ratio": null,
            "penalty": null,
        });
        let link: PenaltyLink = serde_json::from_value(json).unwrap();
        assert_eq!(link.over_quota_ratio, f64::INFINITY);
        assert_eq!(link.penalty, f64::INFINITY);
        assert_eq!(
            serde_json::to_value(&link).unwrap()["over_quota_ratio"],
            serde_json::json!(null)
        );
    }

    #[test]
    fn list_jobs_params_query_pairs_are_present_only_when_set() {
        assert_eq!(ListJobsParams::new().query_pairs(), Vec::new());
        let params = ListJobsParams::new()
            .with_filter(JobFilter::search("x"))
            .with_cursor(JobCursor::from("v1:job-y".to_string()))
            .with_limit(50);
        assert_eq!(
            params.query_pairs(),
            vec![
                ("filter", "{\"search\":\"x\"}".to_string()),
                ("cursor", "v1:job-y".to_string()),
                ("limit", "50".to_string()),
            ]
        );
    }

    #[test]
    fn timeline_params_query_pairs_are_present_only_when_set() {
        assert_eq!(TimelineParams::new().query_pairs(), Vec::new());
        let params = TimelineParams::new()
            .with_cursor(TimelineCursor::from("v1:1:0".to_string()))
            .with_limit(20);
        assert_eq!(
            params.query_pairs(),
            vec![
                ("cursor", "v1:1:0".to_string()),
                ("limit", "20".to_string()),
            ]
        );
    }

    #[test]
    fn an_unknown_true_up_kind_keeps_its_spelling() {
        let view: TrueUpView = serde_json::from_value(serde_json::json!({
            "kind": "clawback",
            "amount_ucu": 5,
        }))
        .unwrap();
        assert_eq!(view.kind, TrueUpKind::Unknown("clawback".to_string()));
    }
}
