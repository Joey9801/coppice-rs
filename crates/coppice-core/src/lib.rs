//! # coppice-core
//!
//! Shared domain model for the Coppice batch job scheduler.
//!
//! This crate holds the vocabulary that every other crate speaks: identifiers,
//! the resource model, the job lifecycle, and the distinction between desired,
//! observed, and derived state. It deliberately contains no I/O, no async, and
//! no scheduling policy — only types and pure functions that are safe to depend
//! on from anywhere in the workspace.
//!
//! See `docs/architecture/state-model.md` and `docs/lifecycle/job-lifecycle.md`.

pub mod allocation;
pub mod attempt;
pub mod id;
pub mod job;
pub mod node;
pub mod quota;
pub mod resource;

// The old single-value `Epoch` placeholder is gone: ADR 0009 settled the
// fencing token as the (leader_term, node_epoch) pair, defined on the wire
// as `coppice.agent.v1.FencingToken` and carried in every coordinator→agent
// command header.
