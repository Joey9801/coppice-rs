//! # coppice-proto
//!
//! Wire-level message types shared between processes: the public API surface
//! and the agent-coordinator protocol.
//!
//! The protocol is designed around reconciliation and idempotency. Messages
//! carry enough identity (job, allocation, attempt, epoch, sequence) that
//! duplicate, delayed, and reordered delivery is safe. These types are the
//! serialized boundary, so schema evolution must stay backward compatible; see
//! `docs/architecture/versioning.md`.

pub mod agent;
pub mod api;
