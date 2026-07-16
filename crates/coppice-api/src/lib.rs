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
use http::dto::{AbortJobRequest, SubmitJobRequest, SubmitJobResponse};

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

/// The most recent cluster events, newest first, served from the fanout
/// ring (ADR 0032, tier 1).
///
/// `floor_index` is an exclusive coverage cursor: the window is complete
/// for every applied index *strictly above* it and claims nothing at or
/// below it. An empty `events` with a high cursor is a freshly restarted
/// coordinator, not a quiet cluster.
#[derive(Debug, Clone)]
pub struct RecentClusterEvents {
    pub floor_index: u64,
    pub events: Vec<StampedEvent>,
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
    /// CoordinatorId. Today no producer can supply it (raft membership
    /// records only the peer-plane address), so it is `None` until client
    /// addresses are advertised through membership or writes are forwarded
    /// internally (ADR 0031).
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

    /// The write did not resolve to a replicated decision: a timeout,
    /// overload, or the seam shutting down. The caller may retry.
    #[error("unavailable: {0}")]
    Unavailable(String),
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

    fn submit_job(
        &self,
        req: SubmitJobRequest,
    ) -> impl Future<Output = Result<SubmitJobResponse, ApiError>> + Send;

    fn abort_job(&self, req: AbortJobRequest) -> impl Future<Output = Result<(), ApiError>> + Send;

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

    /// The most recent cluster events this replica's fanout ring retains
    /// (ADR 0032, tier 1), newest first, at most `limit` — derived class,
    /// replica-local, with the coverage floor.
    fn recent_events(&self, limit: usize) -> impl Future<Output = RecentClusterEvents> + Send;

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
}
