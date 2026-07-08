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

/// Session tasks -> ingestion, one shared channel ("agent inbound" row).
///
/// `send().await`; a full channel stalls the session's socket read, which is the
/// correct backpressure point (TCP, never apply).
pub const AGENT_INBOUND_CAPACITY: usize = 8192;

/// Router -> one outbound queue per session ("agent outbound" row).
///
/// `try_send`; on full the session is DISCONNECTED (idempotent commands plus
/// ADR 0009 reconciliation heal the reconnect). No transport exists yet to
/// actually open a session (`tasks::agent_gateway::run_session`), so this
/// isn't wired into a live channel construction today.
#[allow(dead_code)]
pub const AGENT_OUTBOUND_CAPACITY: usize = 256;

/// Dispatch/ingestion -> session manager ("command router" row).
///
/// `send().await`; producers are leader-only loops that tolerate this backpressure.
pub const COMMAND_ROUTER_CAPACITY: usize = 1024;

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
