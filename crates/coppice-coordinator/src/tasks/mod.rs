//! Coordinator background tasks.
//!
//! One module per row of the "Task inventory" table in
//! `docs/architecture/coordinator-runtime.md`; `crate::runtime` wires them
//! together into the topology that document specifies. The consensus/apply
//! task itself lives in `coppice-consensus`, not here.

pub mod agent_gateway;
pub mod api_server;
pub mod derived_stats;
pub mod dispatch;
pub mod event_fanout;
// One client's `GET /api/v1/events` subscription (ADR 0043): the connection
// task that pulls its catch-up and relays the live queue. Private — the
// control plane opens one per request and nothing else has a reason to.
pub(crate) mod event_stream;
pub mod housekeeping;
pub mod ingestion;
// Stale-learner garbage collection (ADR 0037 §7): leader-only, evidence is
// failed replication contact past `learner_expiry` — never log position.
pub mod learner_gc;
pub mod node_client;
// This replica's own leaf renewal (ADR 0037 §4): re-issue at ~2/3 lifetime,
// locally while leader and over the admin channel otherwise.
pub mod renewal;
pub mod scheduler_driver;
// Rolling best-effort node-usage history (ADR 0039): samples the replicated
// state and the leader's heartbeat sink into 30 s buckets, and feeds both the
// utilization/capacity charts and the `coppice_node_*` gauges.
pub mod usage_history;
