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
pub mod housekeeping;
pub mod ingestion;
pub mod node_client;
pub mod scheduler_driver;
