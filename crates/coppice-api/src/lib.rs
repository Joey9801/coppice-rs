//! # coppice-api
//!
//! The external API layer — the user-facing entry point for job submission,
//! abort, status queries, event subscriptions, and administrative
//! actions. The web UI and CLI are both built on this same surface.
//!
//! The API runs on every coordinator replica. Read-only requests may be served
//! by followers when suitably fresh reads are acceptable; mutating requests are
//! routed or forwarded to the current Raft leader. Endpoints are modelled as
//! durable state transitions, not imperative worker control. See
//! `docs/architecture/components.md` and `docs/operations/security.md`.

pub mod http;

use std::future::Future;

use coppice_core::time::Timestamp;
use http::dto::{
    AbortJobRequest, ConfigureQuotaEntityRequest, ConfigureQuotaEntityResponse, SubmitJobRequest,
    SubmitJobResponse, UpdateAuthorizationRequest, UpdateAuthorizationResponse,
};

/// Consistency class for read operations (ADR 0007).
///
/// Every read endpoint has a default class set by ADR 0031; the caller may
/// override it with `?consistency=`. `Deserialize` covers the query-parameter
/// path; the rename keeps the wire form lowercase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    Strong,
    Bounded,
    Eventual,
}

/// Transport-independent parameters for a read operation.
pub struct ReadOptions {
    pub consistency: Consistency,
    pub min_index: Option<u64>,
}

/// A consistent snapshot of the state machine with staleness metadata.
///
/// The `StateMachine` clone is O(1) (persistent maps, ADR 0028). Handlers
/// project the state into their response type and attach the indexes as
/// response headers.
pub struct ReadView {
    state: coppice_state::StateMachine,
    applied_index: u64,
    committed_index: u64,
}

impl ReadView {
    pub fn new(
        state: coppice_state::StateMachine,
        applied_index: u64,
        committed_index: u64,
    ) -> Self {
        ReadView {
            state,
            applied_index,
            committed_index,
        }
    }

    pub fn state(&self) -> &coppice_state::StateMachine {
        &self.state
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub fn committed_index(&self) -> u64 {
        self.committed_index
    }
}

/// One closed bucket of queue transitions (ADR 0032, tier 3), derived
/// replica-locally by counting the event stream — never replicated, never
/// snapshotted.
///
/// Counts are transitions observed *during* the bucket; `depth` is sampled
/// from the latest published view at bucket close. A bucket that was never
/// produced (the process wasn't running, or coverage was lost to an event
/// gap) is simply absent from the window — honest absence, never a zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueBucket {
    /// Bucket start (inclusive).
    pub start: Timestamp,
    /// Bucket close (exclusive). Buckets are nominally 30 s wide,
    /// but a stalled producer closes one *long* bucket rather than
    /// back-filling — so every rate over a bucket must scale by this actual
    /// span, never an assumed width.
    pub end: Timestamp,
    /// Jobs in `Queued` at bucket close.
    pub depth: u32,
    /// Transitions into `Queued` during the bucket (submissions + requeues).
    pub arrivals: u32,
    /// Transitions out of `Queued` during the bucket (placements, aborts).
    pub drains: u32,
}

/// The rolling window of closed queue buckets, oldest first — the source
/// for the overview's queue rates and `history` (ADR 0032, tier 3).
///
/// Contiguous by construction: the producing task drops the whole window
/// when it loses event-stream coverage, so a bucket's presence means its
/// counts are complete over its own `[start, end)` span.
#[derive(Debug, Clone, Default)]
pub struct QueueWindow {
    pub buckets: Vec<QueueBucket>,
}

/// One node's freshest usage reading, as the leader last heard it on a
/// heartbeat (ADR 0039). `used` is job-attributable on all three dimensions —
/// directly comparable to that node's `allocated`, never a host-level total.
///
/// A node with no entry is not reporting: absence means "not measured", never
/// zero (ADR 0039).
///
/// Two stamps, and only one of them decides anything. `received_at` is the
/// *coordinator's* clock when the leader peeled the reading off a heartbeat:
/// every freshness question — the sink's staleness cutoff, a bucket's
/// used-coverage, the scrape's sample-age — is answered from it, because it
/// is the only stamp a remote clock cannot move. `sampled_at` is the agent's
/// clock at its own fold, kept as advisory metadata (how far behind the
/// reading was on the reporter's side) and never used as a cutoff: an agent
/// an hour fast would otherwise keep a dead node's last reading "fresh" for
/// an hour after it went silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeUsageSample {
    pub used: coppice_core::resource::Resources,
    /// The reporting agent's clock at its fold. **Advisory only.**
    pub sampled_at: Timestamp,
    /// The coordinator's clock when this reading arrived. Every freshness
    /// and TTL decision uses this.
    pub received_at: Timestamp,
}

/// One closed bucket of a node's capacity/allocated/used accounting (ADR
/// 0039), sampled by the leader's usage-history task — best-effort,
/// in-memory, never replicated and never snapshotted.
///
/// `capacity` and `allocated` are read from the replicated state at bucket
/// close, so they are always present; `used` is `None` for a bucket in which
/// the node reported nothing fresh. A bucket that was never produced (the
/// process wasn't leading, or wasn't running) is simply absent from the
/// window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageBucket {
    /// Bucket start (inclusive).
    pub start: Timestamp,
    /// Bucket close (exclusive). Nominally 30 s, but a stalled producer
    /// closes one *long* bucket rather than back-filling, so a consumer that
    /// scales anything by the span must read it here.
    pub end: Timestamp,
    pub capacity: coppice_core::resource::Resources,
    pub allocated: coppice_core::resource::Resources,
    /// Measured job-attributable consumption at close; `None` when the node
    /// had no fresh sample (absence, never a zero).
    pub used: Option<coppice_core::resource::Resources>,
}

/// The rolling window of one node's closed usage buckets, oldest first.
#[derive(Debug, Clone, Default)]
pub struct UsageWindow {
    pub buckets: Vec<UsageBucket>,
}

/// A cluster-wide usage bucket: the per-node buckets summed, plus how many
/// nodes actually contributed a `used` reading.
///
/// `reporting_nodes` is what keeps the cluster total honest — a sum over 2 of
/// 5 nodes is not the cluster's consumption, and the ratio is the only thing
/// that says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterUsageBucket {
    pub bucket: UsageBucket,
    /// Nodes whose bucket carried a `used` reading.
    pub reporting_nodes: u32,
    /// Nodes present in the replicated state at bucket close.
    pub total_nodes: u32,
}

/// The leader's rolling usage history for every node it is tracking (ADR
/// 0039), published wholesale by the usage-history task and shared by
/// `Arc` — a read never copies it.
///
/// Departed nodes are dropped as they leave the replicated state, so a key
/// here is a node that existed at the last tick.
#[derive(Debug, Clone, Default)]
pub struct ClusterUsage {
    pub nodes: std::collections::BTreeMap<coppice_core::id::NodeId, UsageWindow>,
    pub cluster: Vec<ClusterUsageBucket>,
}

/// What a usage read sees: the freshest per-node samples *now*, plus the
/// shared rolling history behind them.
///
/// Two sources with different cadences on purpose. `current` is read live
/// from the leader's heartbeat sink at request time, so a node that has just
/// reported shows up immediately rather than at the next 30 s bucket close;
/// `history` is the task's last published window. On a follower — where no
/// agent sessions terminate — `current` is empty and `history` has no
/// buckets, and every `used` renders as honestly absent.
#[derive(Debug, Clone, Default)]
pub struct UsageSnapshot {
    /// Nodes with a reading fresher than the staleness cutoff. Absent =
    /// not measured.
    pub current: std::collections::BTreeMap<coppice_core::id::NodeId, NodeUsageSample>,
    pub history: std::sync::Arc<ClusterUsage>,
}

/// One event with the identity and stamp of ADR 0032's shared timeline
/// shape: ordered and deduplicated by `(index, ordinal)`, rendered at the
/// advisory `at`.
#[derive(Debug, Clone)]
pub struct StampedEvent {
    /// The producing command's log index.
    pub index: u64,
    /// The event's position in its full batch, assigned before any
    /// filtering — part of its identity.
    pub ordinal: u32,
    /// The command's proposer stamp; advisory, never an ordering key.
    pub at: Timestamp,
    pub event: coppice_state::Event,
}

/// One job's timeline window, ascending by `(index, ordinal)`, served from
/// this replica's fanout ring (ADR 0032, tier 1) — the honestly-partial
/// backstop behind `GetJobTimeline`.
///
/// `floor_index` is an exclusive coverage cursor: the timeline is complete
/// for every applied index
/// *strictly above* it and claims nothing at or below it, so a window is
/// complete-from-submission only when it actually contains the job's
/// `job_submitted` event. `next` is the `(index, ordinal)` content coordinate
/// to resume strictly after: `Some` means the ring held more past this page
/// (continue), `None` means the caller has everything this replica currently
/// retains.
#[derive(Debug, Clone)]
pub struct JobTimelineWindow {
    pub floor_index: u64,
    pub events: Vec<StampedEvent>,
    pub next: Option<(u64, u32)>,
}

/// This replica's view of the raft cluster for `GET /api/v1/coordinators`
/// (ADR 0031, local read) — the consensus/membership facts the raft layer
/// knows, with no replicated-state counts (those come from a `read_state`
/// snapshot at the handler and are projected purely).
///
/// Ids are plain `u64` raft identities: `coppice-api` speaks DTOs and does
/// not depend on `coppice-consensus`, so its `CoordinatorId` type never
/// crosses this boundary. The coordinator translates its `ClusterSummary`
/// into this shape before handing it over.
#[derive(Debug, Clone)]
pub struct CoordinatorSummary {
    /// The serving replica's own raft id — the one member whose applied
    /// index we can report exactly.
    pub local_id: u64,
    /// The current leader's raft id, when one is known.
    pub leader: Option<u64>,
    /// The current raft term.
    pub term: u64,
    /// Highest committed index this replica knows of.
    pub known_committed: u64,
    /// Highest applied index on this replica.
    pub last_applied: u64,
    /// Log index the last snapshot covers, from openraft metrics — `None`
    /// when this replica has taken no snapshot yet. The projection derives
    /// `entries_since_snapshot` from it; snapshot size and time have no
    /// source (`SnapshotMeta` carries neither) and are always null.
    pub snapshot_last_index: Option<u64>,
    /// One entry per configured cluster member.
    pub members: Vec<CoordinatorMemberSummary>,
}

/// One membership entry in a [`CoordinatorSummary`].
#[derive(Debug, Clone)]
pub struct CoordinatorMemberSummary {
    /// The member's raft id.
    pub id: u64,
    /// The address peers dial (host:port).
    pub addr: String,
    /// Whether the member is a voter (vs a learner) — drives the projected
    /// role.
    pub voter: bool,
    /// The leader's matched (replicated) index for this member, when this
    /// replica is leader and tracks it; `None` on followers or for a member
    /// with no replication entry. The projection turns it into
    /// `replication_lag_entries`.
    pub matched_index: Option<u64>,
}

/// Errors surfaced to API callers.
///
/// A `Rejected` outcome means the command committed and apply refused it
/// deterministically — normal control flow for a racing proposer, never a
/// server fault (`docs/architecture/coordinator-runtime.md`, "The consensus
/// seam"). Every other variant means the write never resolved to a
/// replicated decision at all.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// This replica is not the leader. `leader_hint`, when present, is the
    /// leader's advertised **client-API address** (dialable by the caller
    /// for a retry) — never an internal identifier like the raft
    /// CoordinatorId. No producer can supply it: raft membership records
    /// only the peer-plane address, and ADR 0038 chose internal forwarding
    /// over advertising client addresses through membership, so it stays
    /// `None` by design.
    ///
    /// Since ADR 0038 this is the *fallback*, not the ordinary answer to a
    /// write on a follower: a follower forwards the write to the leader and
    /// answers with the leader's own outcome. This reaches a client only
    /// when no leader is known at all (an election is in progress), when the
    /// known leader has no address to forward to, or when the coordinator
    /// forwarded to turns out not to lead either — all of which mean "ask
    /// again", which is exactly what an empty hint says.
    #[error("not the leader{}", .leader_hint.as_deref().map(|h| format!(" (leader is {h})")).unwrap_or_default())]
    NotLeader { leader_hint: Option<String> },

    /// The request failed synchronous validation before anything was
    /// proposed; the caller must fix the request, retrying as-is will not
    /// help.
    #[error("invalid request: {0}")]
    Invalid(String),

    /// The command committed and applied, but apply refused it
    /// deterministically — a normal outcome of a race between proposers
    /// (see `docs/architecture/command-catalog.md` for rejection semantics), not a server fault.
    #[error("rejected: {0}")]
    Rejected(#[source] coppice_state::RejectionReason),

    /// A [`Rejected`](Self::Rejected) relayed from the leader across the
    /// internal forwarding hop (ADR 0038), carrying the leader's *rendered*
    /// reason rather than the reason itself: `RejectionReason` has no proto
    /// encoding to reconstruct from, and the rendered text is all a client
    /// ever sees of it.
    ///
    /// `kind` is the one structured thing that does survive the hop. Rendered
    /// text alone was enough while every rejection was a 409; since ADR 0023
    /// the status a rejection deserves depends on *which* rejection it is —
    /// `PermissionDenied` is a 403 and three of the authorization rejections
    /// are 400s on one endpoint — and deciding that by matching on English
    /// would be a contract held together by a string. So the leader classifies
    /// its own rejection and the follower maps the classification.
    #[error("rejected: {reason}")]
    ForwardedRejection { kind: RejectionKind, reason: String },

    /// The write did not resolve to a replicated decision: a timeout,
    /// overload, or the seam shutting down. The caller may retry.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// The rejections whose *identity* changes the HTTP status a client sees, as
/// a closed classification that survives the ADR 0038 forwarding hop.
///
/// Deliberately not one arm per [`coppice_state::RejectionReason`]: this is
/// not a wire re-encoding of that enum (there is none — the rendered text is
/// what crosses), it is the small set of distinctions the status mapping
/// actually turns on. Everything else is [`Other`](Self::Other), the 409 that
/// every rejection was before ADR 0023.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RejectionKind {
    /// An ordinary committed-and-refused race outcome: `REJECTED` (409).
    #[default]
    Other,
    /// Apply's own ADR 0023 re-check refused the actor: `PERMISSION_DENIED`
    /// (403), everywhere, so a revocation landing between the API's
    /// pre-check and the command's log position never surfaces as a 500.
    PermissionDenied,
    /// A binding scoped to a quota entity that does not exist. A 400 on
    /// `PUT /api/v1/authorization` only — on submit and configure it stays
    /// the documented 409.
    UnknownQuotaEntity,
    /// A malformed bindings list (an empty subject).
    InvalidAuthorization,
    /// A bindings list retaining no unscoped admin (ADR 0023's loud
    /// accident guard).
    AuthorizationLockout,
}

impl RejectionKind {
    /// Classify a local rejection for the hop — the one place the mapping
    /// from `RejectionReason` to [`RejectionKind`] lives.
    pub fn of(reason: &coppice_state::RejectionReason) -> RejectionKind {
        use coppice_state::RejectionReason as R;
        match reason {
            R::PermissionDenied(_) => RejectionKind::PermissionDenied,
            R::UnknownQuotaEntity(_) => RejectionKind::UnknownQuotaEntity,
            R::InvalidAuthorization(_) => RejectionKind::InvalidAuthorization,
            R::AuthorizationLockout => RejectionKind::AuthorizationLockout,
            _ => RejectionKind::Other,
        }
    }
}

// ---------------------------------------------------------------------------
// Log-fetch seam (ADR 0034)
// ---------------------------------------------------------------------------
//
// The one coordinator→agent RPC behind `GET /api/v1/jobs/{job}/logs`. These
// types are deliberately proto-free plain structs: `coppice-api` does not
// depend on `coppice-proto` (ADR 0031 — the crate "speaks DTOs"), so the seam
// cannot name the generated `coppice.agent.v1.FetchLogs*` messages. The
// coordinator's `ControlPlane` impl converts these to/from the wire types at
// its boundary, exactly as the raft transport converts domain types to pb in
// `coppice-consensus`. Page orchestration (the multi-attempt walk, the cursor,
// the 4-RPC budget) lives in the HTTP handler; this seam is a single RPC.

/// Which of an attempt's two streams to fetch. `None` on the request means
/// both (the proto's `LOG_STREAM_UNSPECIFIED` filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStreamSelector {
    Stdout,
    Stderr,
}

/// An exclusive resume position within one attempt's chunks. The store orders
/// by `(at, insertion)`, so `at_us` alone cannot address a position when
/// several chunks share a microsecond: `skip` counts the chunks already
/// consumed at exactly `at_us`, and the walk resumes strictly after them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogResumePosition {
    pub at_us: i64,
    pub skip: u64,
}

/// One page request for a single attempt's logs (one `NodeService::FetchLogs`
/// RPC). The coordinator has already resolved `(job, attempt)` to the target
/// node from replicated state before dialing.
#[derive(Debug, Clone)]
pub struct LogFetchRequest {
    pub job: coppice_core::id::JobId,
    pub attempt: coppice_core::id::AttemptId,
    /// Half-open `[from_us, until_us)` window; either bound may be open.
    pub from_us: Option<i64>,
    pub until_us: Option<i64>,
    /// Stream filter; `None` returns both.
    pub stream: Option<LogStreamSelector>,
    /// Exclusive lower cursor; `None` starts from the window edge.
    pub resume: Option<LogResumePosition>,
    /// Direction: `false` walks newest-first (matches `order=desc`).
    pub ascending: bool,
    /// Hard caps on the page; the store stops at whichever trips first and
    /// reports `exhausted = false`.
    pub max_chunks: u32,
    pub max_bytes: u32,
}

/// One stored log chunk: a run of bytes captured at one microsecond on one
/// stream. Bytes are returned verbatim; UTF-8 decoding happens at the HTTP edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogChunk {
    pub at_us: i64,
    pub stream: LogStreamSelector,
    pub payload: Vec<u8>,
    /// True when the store cut `payload` down to the page's remaining byte
    /// budget because the chunk alone exceeded it: the chunk still counts as
    /// fully consumed, so the resume cursor advances past it whole and the
    /// dropped bytes are never re-served (only ever set on a page's first chunk).
    pub truncated: bool,
}

/// A page of chunks in the requested direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub chunks: Vec<LogChunk>,
    /// True when the walk reached the end of the requested range within this
    /// page; false when a cap cut it short and a further page exists.
    pub exhausted: bool,
    /// The store's oldest/newest retained `at_us` for this attempt, when
    /// known. A requested `from_us` earlier than `earliest_at_us` means older
    /// chunks existed and were pruned (the API's `truncated` verdict).
    pub earliest_at_us: Option<i64>,
    pub latest_at_us: Option<i64>,
}

/// The result of one `FetchLogs`: a page, or the store holding no data for the
/// attempt at all (segments pruned, or telemetry never written — the "gone"
/// signal behind the `expired` availability verdict).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFetchOutcome {
    Chunks(LogPage),
    UnknownAttempt,
}

/// Why a `fetch_logs` RPC could not be completed against the target node.
///
/// `UnknownAttempt` is *not* an error — it is a normal [`LogFetchOutcome`]. An
/// error here means the RPC itself did not land: a dial failure, a deadline,
/// or the absence of any reachable node service.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LogFetchError {
    /// The node could not be reached: no dialable channel, a connect/deadline
    /// failure, or the agent hosts no service. `reason` is operator-readable
    /// and surfaces verbatim in the `sources[].reason` of the response.
    #[error("node unreachable: {reason}")]
    Unreachable { reason: String },
}

// The metrics twin of the log-fetch seam above (`NodeService::FetchMetrics`).
// Same single-RPC shape: page orchestration (the multi-attempt walk, the
// cursor, the 4-RPC budget) lives in the HTTP handler for
// `GET /api/v1/jobs/{job}/usage`; this seam is one RPC. Samples are fixed-size
// rows, so there is no byte budget — only `max_samples` — the one structural
// divergence from the log seam. `LogResumePosition` (the same `(at, insertion)`
// coordinate the proto's shared `ResumePosition` carries) is reused rather than
// re-declared.

/// One page request for a single attempt's metric samples (one
/// `NodeService::FetchMetrics` RPC). The coordinator has already resolved
/// `(job, attempt)` to the target node from replicated state before dialing.
#[derive(Debug, Clone)]
pub struct MetricsFetchRequest {
    pub job: coppice_core::id::JobId,
    pub attempt: coppice_core::id::AttemptId,
    /// Half-open `[from_us, until_us)` window; either bound may be open.
    pub from_us: Option<i64>,
    pub until_us: Option<i64>,
    /// Exclusive lower cursor; `None` starts from the window edge.
    pub resume: Option<LogResumePosition>,
    /// Direction: `false` walks newest-first (matches `order=desc`).
    pub ascending: bool,
    /// Hard cap on the page; the store stops here and reports
    /// `exhausted = false` when more rows remain. No byte cap: samples are
    /// fixed-size rows.
    pub max_samples: u32,
}

/// One stored metric sample, mirroring the agent's periodic resource row
/// field-for-field (docker-executor.md §8.1). Counters are cumulative —
/// readers derive rates, so a missed sample loses resolution, never mass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSample {
    pub at_us: i64,
    /// Cumulative CPU time consumed, µs.
    pub cpu_usage_total_us: u64,
    /// Cumulative CPU time the container was throttled, µs.
    pub cpu_throttled_total_us: u64,
    /// Current resident memory.
    pub memory_used_bytes: u64,
    /// Peak resident memory over the attempt so far.
    pub memory_peak_bytes: u64,
    /// Writable-layer bytes from the disk poller's last reading.
    pub disk_writable_bytes: u64,
    /// Image bytes — constant per attempt; writable + image = usage.
    pub disk_image_bytes: u64,
    /// Cumulative bytes received on the container network.
    pub net_rx_bytes_total: u64,
    /// Cumulative bytes transmitted on the container network.
    pub net_tx_bytes_total: u64,
    /// Cumulative block-I/O bytes read.
    pub blkio_read_bytes_total: u64,
    /// Cumulative block-I/O bytes written.
    pub blkio_write_bytes_total: u64,
}

/// A page of samples in the requested direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsPage {
    pub samples: Vec<MetricSample>,
    /// True when the walk reached the end of the requested range within this
    /// page; false when `max_samples` cut it short and a further page exists.
    pub exhausted: bool,
    /// The store's oldest/newest retained sample `at_us` for this attempt, when
    /// known. A requested `from_us` earlier than `earliest_at_us` means older
    /// samples existed and were pruned (the API's `truncated` verdict).
    pub earliest_at_us: Option<i64>,
    pub latest_at_us: Option<i64>,
}

/// The result of one `FetchMetrics`: a page, or the store holding no data for
/// the attempt at all (segments pruned, or telemetry never written — the
/// "gone" signal behind the `expired` availability verdict).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsFetchOutcome {
    Samples(MetricsPage),
    UnknownAttempt,
}

/// Why a `fetch_metrics` RPC could not be completed against the target node.
/// The metrics twin of [`LogFetchError`]: `UnknownAttempt` is a normal
/// [`MetricsFetchOutcome`], not an error — an error here means the RPC itself
/// did not land.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MetricsFetchError {
    /// The node could not be reached: no dialable channel, a connect/deadline
    /// failure, or the agent hosts no service. `reason` is operator-readable
    /// and surfaces verbatim in the `sources[].reason` of the response.
    #[error("node unreachable: {reason}")]
    Unreachable { reason: String },
}

/// The set of operations the API layer exposes. Implemented by the
/// coordinator, which owns access to consensus and state.
///
/// Every method resolves only once the underlying command is committed AND
/// applied (`Consensus::propose`'s contract) — never merely queued or
/// merely committed. `SubmitJobResponse` echoes the client-minted job id —
/// the submission's idempotency identity (ADR 0026), so a caller may retry
/// an unknown outcome with the identical request and never create a second
/// job — and carries the apply's `log_index` so a write can be paired with
/// a strong read for read-your-writes (ADR 0007).
pub trait ControlPlane: Send + Sync + 'static {
    /// The cluster this replica belongs to (its node config, ADR 0020 — not
    /// replicated state, and constant for the process's lifetime).
    ///
    /// Reads that identify the cluster to a caller (`GET /api/v1/overview`)
    /// take it from here rather than from a view: no `StateMachine` field
    /// carries it, and a replica knows its own cluster before it has applied
    /// anything.
    fn cluster_id(&self) -> coppice_core::id::ClusterId;

    /// Propose the submission on behalf of `actor` (ADR 0023).
    ///
    /// `actor` is the identity the authentication layer resolved for the
    /// request, and it rides the proposed command: apply stamps it onto
    /// `Job.submitted_by` and re-evaluates the authorization decision against
    /// the bindings as of the command's own log position. The HTTP layer has
    /// already pre-checked the same decision against a read view and answered
    /// 403 if it failed — the pre-check is the fast, friendly half, and this
    /// re-check is the authoritative one, which is what makes a revocation
    /// racing an in-flight write resolve in log order rather than by luck.
    fn submit_job(
        &self,
        req: SubmitJobRequest,
        actor: coppice_state::Actor,
    ) -> impl Future<Output = Result<SubmitJobResponse, ApiError>> + Send;

    /// Propose the abort on behalf of `actor`; same two-check arrangement as
    /// [`submit_job`](ControlPlane::submit_job). Ownership rides along for
    /// free: apply compares the actor's principal to the job's `submitted_by`.
    fn abort_job(
        &self,
        req: AbortJobRequest,
        actor: coppice_state::Actor,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Propose the `ConfigureQuotaEntity` upsert (ADR 0031's write class) on
    /// behalf of `actor`. Resolves once committed and applied; a cycle or
    /// unknown-parent refusal surfaces as [`ApiError::Rejected`] (a normal 409
    /// race outcome), and an authorization refusal as
    /// `RejectionReason::PermissionDenied` (403).
    ///
    /// Authorization follows the same arrangement as
    /// [`submit_job`](ControlPlane::submit_job): the API layer pre-checks
    /// against a read view and apply re-checks at the log position (ADR 0023).
    fn configure_quota_entity(
        &self,
        req: ConfigureQuotaEntityRequest,
        actor: coppice_state::Actor,
    ) -> impl Future<Output = Result<ConfigureQuotaEntityResponse, ApiError>> + Send;

    /// Replace the replicated role bindings wholesale on behalf of `actor`
    /// (`PUT /api/v1/authorization`, ADR 0023) — an unscoped-admin verb.
    ///
    /// Exactly **one** command: `UpdateAuthorization` carries the bindings
    /// and, optionally, the `groups_claim` rename, and apply installs both at
    /// one log position. The two edits are only safe together — split across
    /// two commands, the second is authorized at *its own* position against
    /// the bindings the first just installed, while the actor's groups are
    /// still read under the old claim name, so a rename that also swaps the
    /// admin group denies its own second half. `groups_claim` still lives in
    /// `PolicyConfig`, and `UpdatePolicy` still replaces it wholesale;
    /// concurrent editors resolve last-writer-wins in log order.
    fn update_authorization(
        &self,
        req: UpdateAuthorizationRequest,
        actor: coppice_state::Actor,
    ) -> impl Future<Output = Result<UpdateAuthorizationResponse, ApiError>> + Send;

    /// Resolve a read at the requested consistency and return a snapshot of
    /// the replicated state with its staleness metadata.
    ///
    /// Strong reads call `Consensus::read_index` (leader only) then wait for
    /// the view to catch up; bounded/eventual reads serve the latest
    /// published view, optionally gated by `min_index` for read-your-writes.
    fn read_state(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<ReadView, ApiError>> + Send;

    /// This replica's rolling window of queue-transition buckets (ADR 0032,
    /// tier 3) — derived class, replica-local, coverage-annotated by
    /// absence. Cheap: a clone of the latest published window, no locks
    /// held, no consensus involvement.
    fn queue_window(&self) -> QueueWindow;

    /// This replica's best-effort node-usage view (ADR 0039): the freshest
    /// heartbeat samples plus the rolling per-node/cluster history behind
    /// them. Derived class, in-memory, never replicated — and **leader-only
    /// in practice**, since agent sessions terminate on the leader. A
    /// follower answers with an empty snapshot, which reads as `used: null`
    /// everywhere rather than as zero.
    ///
    /// Cheap: a live read of a small map plus an `Arc` clone of the published
    /// history, no locks held across an await and no consensus involvement.
    fn usage_window(&self) -> UsageSnapshot;

    /// One job's transition timeline (ADR 0032), ascending by `(index,
    /// ordinal)`, resuming strictly after `after` and bounded by `limit`
    /// matches — a derived, replica-local read with a coverage floor, served
    /// identically by every replica (leader and follower alike).
    ///
    /// Today this is served entirely from this replica's per-replica fanout
    /// ring (tier 1): the answer is honestly partial, truncated below
    /// `floor_index`, and a job whose events aged out of the ring returns an
    /// empty-but-floored window. When a durable history store (ADR 0032 tier
    /// 2) exists **and** an operator has configured one, this same method
    /// fuses the store's durable prefix below the ring's tail behind this
    /// signature — the cursor is a content coordinate precisely so it survives
    /// that fusion. The ring path stays permanently as the backstop for
    /// deployments without a durable store (e.g. `coppice dev`); it is never
    /// removed, only supplemented. That store, its writer, and any config
    /// plumbing are out of scope here — this is only the seam.
    fn job_timeline(
        &self,
        job: coppice_core::id::JobId,
        after: Option<(u64, u32)>,
        limit: usize,
    ) -> impl Future<Output = JobTimelineWindow> + Send;

    /// This replica's view of the raft cluster for `GET /api/v1/coordinators`
    /// (ADR 0031, local read): leader/term/indexes and per-member membership,
    /// read straight from the consensus metrics — no replicated state, no
    /// consensus round-trip.
    ///
    /// `Err(ApiError::Unavailable)` when the consensus handle is not attached
    /// to this API server (the same "no coverage" posture as a missing fanout
    /// ring): the replicated-state reads still work, but this raft-level view
    /// cannot be produced.
    fn coordinator_status(&self) -> Result<CoordinatorSummary, ApiError>;

    /// Fetch one bounded page of an attempt's stored logs from the agent that
    /// ran it (ADR 0034's single coordinator→agent RPC, `NodeService::FetchLogs`).
    ///
    /// The HTTP handler for `GET /api/v1/jobs/{job}/logs` has already resolved
    /// `(job, attempt)` to `node` and its advertised `addr` from replicated
    /// state, and owns the page orchestration — the multi-attempt walk, the
    /// cursor, and the at-most-4-RPCs-per-request budget. This method is one
    /// RPC: it dials the node (any replica may, no leader involvement), applies
    /// a 5 s deadline, and returns the store's answer or an [`LogFetchError`].
    /// A pruned/never-written attempt is the normal
    /// [`LogFetchOutcome::UnknownAttempt`], not an error.
    fn fetch_logs(
        &self,
        node: coppice_core::id::NodeId,
        addr: &str,
        req: LogFetchRequest,
    ) -> impl Future<Output = Result<LogFetchOutcome, LogFetchError>> + Send;

    /// Fetch one bounded page of an attempt's stored metric samples from the
    /// agent that ran it (`NodeService::FetchMetrics`), the metrics twin of
    /// [`fetch_logs`](ControlPlane::fetch_logs).
    ///
    /// The HTTP handler for `GET /api/v1/jobs/{job}/usage` has already resolved
    /// `(job, attempt)` to `node` and its advertised `addr` from replicated
    /// state, and owns the page orchestration — the multi-attempt walk, the
    /// cursor, and the at-most-4-RPCs-per-request budget. This method is one
    /// RPC: it dials the node (any replica may, no leader involvement), applies
    /// a 5 s deadline, and returns the store's answer or a [`MetricsFetchError`].
    /// A pruned/never-written attempt is the normal
    /// [`MetricsFetchOutcome::UnknownAttempt`], not an error.
    fn fetch_metrics(
        &self,
        node: coppice_core::id::NodeId,
        addr: &str,
        req: MetricsFetchRequest,
    ) -> impl Future<Output = Result<MetricsFetchOutcome, MetricsFetchError>> + Send;
}
