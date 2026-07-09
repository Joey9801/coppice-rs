//! # coppice-agent
//!
//! The node agent: an eventually-consistent executor of coordinator intent
//! (ADR 0009, `docs/protocols/agent-coordinator.md`).
//!
//! The agent never trusts its own memory over its journal plus the container
//! runtime: commands are fenced by `(leader_term, node_epoch)` before being
//! acted on, intents are journaled durably before containers start, and on
//! restart the recovered journal is reconciled against the runtime to build
//! the full `ObservedSet` reported before any new work is accepted.

pub mod config;
pub mod executor;
pub mod journal;
pub mod observed;
pub mod session;
