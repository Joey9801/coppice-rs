//! Bounded-channel capacities and cadence constants for the coordinator runtime.
//!
//! Every constant here is one row of the channel-inventory table in
//! `docs/architecture/coordinator-runtime.md#channel-inventory`; the doc
//! comment on each names the row and its "policy when full" so a capacity
//! change is never made without checking the doc (or vice versa).

use std::time::Duration;

/// Apply task -> event fanout ("event tap" row).
///
/// `try_send`; on full the batch is DROPPED and the receiver synthesizes a gap
/// (ADR 0008). Owned by `main::bootstrap`, which constructs the tap: the apply
/// task itself lives in `coppice-consensus`, not this crate.
/// See `docs/architecture/coordinator-runtime.md`.
pub const EVENT_TAP_CAPACITY: usize = 4096;

/// Fanout -> one queue per subscriber ("per-subscriber queue" row).
///
/// `try_send`; on full the subscriber is marked gapped and its backlog is dropped.
pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 1024;

/// Cadence for retrying a pending `Gap` to a subscriber whose queue overflowed
/// (or that overflowed during a cursor replay) and then saw no further events
/// ("per-subscriber queue" row, KOI-3).
///
/// A gap marker is delivered on the next batch, but a subscriber that overflows
/// and then idles would otherwise never learn it must resync. This bounds that
/// wedge: the fanout re-attempts pending gaps on every tick once the queue has
/// drained.
pub const FANOUT_GAP_RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// Session tasks -> ingestion, one shared channel ("agent inbound" row).
///
/// `send().await`; a full channel stalls the session's socket read, which is the
/// correct backpressure point (TCP, never apply).
pub const AGENT_INBOUND_CAPACITY: usize = 8192;

/// Router -> one outbound queue per session ("agent outbound" row).
///
/// `try_send`; on full the session is DISCONNECTED (idempotent commands plus
/// ADR 0009 reconciliation heal the reconnect).
pub const AGENT_OUTBOUND_CAPACITY: usize = 256;

/// Dispatch/ingestion -> session manager ("command router" row).
///
/// `send().await`; producers are leader-only loops that tolerate this backpressure.
pub const COMMAND_ROUTER_CAPACITY: usize = 1024;

/// Per-session pump tasks -> session manager (session open/close registrations).
///
/// Not a channel-inventory row: the manager's own small control inbox, sized
/// like the other control channels. `send().await`; the producers are the
/// per-session pump tasks, which tolerate this backpressure.
pub const SESSION_CONTROL_CAPACITY: usize = 64;

/// Client -> event fanout subscribe requests (not itself a channel-inventory row).
///
/// The fanout task's own inbox. `send().await`, sized like the other small control channels.
pub const SUBSCRIBE_REQUESTS_CAPACITY: usize = 64;

/// Ring events examined per `GetJobTimeline` window request before the scan
/// returns short with a resume cursor (ADR 0032, tier 1).
///
/// The window scan runs on the fanout task's own loop, so an unbounded
/// filtered walk over a full ring (up to [`FANOUT_RING_MAX_EVENTS`]) would
/// stall event delivery and every other pending read for one request. This
/// bounds that stall; the caller continues from the returned `next`.
pub const EVENT_WINDOW_SCAN_BUDGET: usize = 100_000;

/// Fanout reconnection ring: max events retained ("fanout ring" row, ADR 0008).
///
/// Evict-oldest when full — it is a reconnection buffer, not history.
pub const FANOUT_RING_MAX_EVENTS: usize = 1_000_000;

/// Fanout reconnection ring: max age retained ("fanout ring" row, ADR 0008).
///
/// Evict-oldest when full.
pub const FANOUT_RING_MAX_AGE: Duration = Duration::from_secs(3600);

/// Fanout reconnection ring: approximate max bytes retained (ADR 0043).
///
/// The third bound alongside count and age, and the one that binds first now
/// that a batch carries scope keys: an entity chain, a submitter and a
/// metadata map per job named, plus a second map on every eviction and
/// metadata update. A million *tiny* events fit in the count bound and a
/// million events carrying 4 KiB metadata maps do not, so the count bound
/// alone no longer describes a memory ceiling. Approximate because
/// [`approx_bytes`](crate::tasks::event_fanout) cannot see a `BTreeMap`
/// node's true overhead; it is a safety bound, not an accounting figure.
pub const FANOUT_RING_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Events examined per batch-aligned catch-up page before the scan returns
/// short with a resume index (ADR 0043).
///
/// Smaller than [`EVENT_WINDOW_SCAN_BUDGET`] on purpose: a timeline read is
/// one request and done, whereas a catch-up is a *loop* whose client is
/// already receiving live items into a bounded queue. Shorter pages keep the
/// fanout's own loop responsive and get the subscriber onto the live stream
/// sooner, at the cost of more round trips — which are cheap, since both ends
/// are in this process.
pub const EVENT_CATCH_UP_SCAN_BUDGET: usize = 20_000;

/// How often the fanout sends each subscriber the ADR 0043 progress bookmark
/// ("everything matching at or below N has been sent").
///
/// Also the stream's keepalive, which is why it is a timer and not purely
/// event-driven: an idle cluster must still produce bytes often enough that a
/// proxy or a load balancer does not reap an open SSE connection as dead.
/// Fifteen seconds sits comfortably inside the 30–60 s idle timeouts those
/// default to.
///
/// Part of the wire contract (ADR 0043): a healthy stream carries a frame at
/// least this often, and clients treat several missed intervals as a dead
/// connection rather than a quiet one — `coppice-client`'s
/// `DEFAULT_STREAM_IDLE_TIMEOUT` defaults to 60 s, four times this interval.
/// Raising this value narrows that margin for every client already deployed,
/// so it must not be done casually.
pub const EVENT_PROGRESS_INTERVAL: Duration = Duration::from_secs(15);

/// One connection task -> its HTTP handler (ADR 0043), the second hop of an
/// event subscription.
///
/// `send().await`; a full queue is the *client's* backpressure reaching the
/// connection task, which is exactly where it should stop — the task then
/// stops draining its fanout queue, and that one overflows into a gap by the
/// ordinary rule. Small, because it buys nothing: the fanout queue behind it
/// is the real buffer.
pub const EVENT_STREAM_QUEUE_CAPACITY: usize = 64;

/// Concurrent event subscriptions one replica will serve (ADR 0043).
///
/// Each one costs a [`SUBSCRIBER_QUEUE_CAPACITY`] queue plus a connection
/// task, so this is what bounds the memory a client population can make a
/// replica hold. Over it, subscribe is refused with `UNAVAILABLE` rather than
/// queued: a client told "not now" retries against another replica, which is
/// a better answer than a stream that exists but cannot keep up.
pub const MAX_EVENT_SUBSCRIPTIONS: usize = 1024;

/// Width of one derived queue-stats bucket (ADR 0032, tier 3).
///
/// The derived-stats task closes a bucket of queue arrival/drain counts at
/// this cadence; the overview's rates and `history` are projections over
/// the closed buckets.
pub const QUEUE_BUCKET_INTERVAL: Duration = Duration::from_secs(30);

/// Closed queue-stats buckets retained (ADR 0032: ≤ 1 h of 30 s buckets,
/// task-local, never on the `StateMachine`, never snapshotted).
pub const QUEUE_WINDOW_MAX_BUCKETS: usize = 120;

/// Width of one node-usage bucket (ADR 0039).
///
/// The usage-history task closes a bucket of per-node capacity/allocated/used
/// at this cadence; the node utilization panel and the overview's capacity
/// chart are projections over the closed buckets. Matched to
/// [`QUEUE_BUCKET_INTERVAL`] so the two charts share an x-axis granularity.
pub const USAGE_BUCKET_INTERVAL: Duration = Duration::from_secs(30);

/// Closed usage buckets retained per node (ADR 0039: ≤ 1 h of 30 s buckets,
/// task-local, never on the `StateMachine`, never snapshotted). Long-term
/// retention is Prometheus's job, off the `/metrics` gauges.
pub const USAGE_WINDOW_MAX_BUCKETS: usize = 120;

/// How old a node's usage reading may be before it reads as absent
/// (ADR 0039).
///
/// Matched to [`AGENT_LIVENESS_DEADLINE`]: a node whose last report is older
/// than this is a candidate for `DeclareNodeLost`, so a usage reading that
/// outlives the deadline has no claim to describe anything current.
pub const USAGE_SAMPLE_MAX_AGE: Duration = Duration::from_secs(90);

/// Housekeeping tick cadence (ADR 0012 / ADR 0017): how often a leader sweeps
/// for terminal jobs past the replicated retention TTL and for nodes past the
/// liveness deadline.
///
/// The default rather than the value, since `[pacing] housekeeping_interval`
/// can shorten it ([`crate::config::PacingConfig`]) — liveness only, so a
/// shorter tick notices a due job sooner and never makes one due.
pub const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60);

/// Agent-liveness deadline before the leader proposes `DeclareNodeLost`
/// (ADR 0009 health monitor).
///
/// A node whose last report is older than this and that is still schedulable
/// or holds live allocations is declared lost. This is documented as
/// replicated policy in `docs/operations/configuration.md` ("Agent-liveness /
/// allocation-lost deadlines") and will migrate into `PolicyConfig` later; a
/// node-local constant keeps `coppice-state` frozen for now.
pub const AGENT_LIVENESS_DEADLINE: Duration = Duration::from_secs(90);
