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

/// Fanout reconnection ring: max events retained ("fanout ring" row, ADR 0008).
///
/// Evict-oldest when full — it is a reconnection buffer, not history.
pub const FANOUT_RING_MAX_EVENTS: usize = 1_000_000;

/// Fanout reconnection ring: max age retained ("fanout ring" row, ADR 0008).
///
/// Evict-oldest when full.
pub const FANOUT_RING_MAX_AGE: Duration = Duration::from_secs(3600);

/// Housekeeping tick cadence (ADR 0012 / ADR 0017).
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
