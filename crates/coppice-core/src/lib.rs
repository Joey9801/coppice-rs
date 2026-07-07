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

pub mod id;
pub mod job;
pub mod node;
pub mod resource;

/// Monotonic fencing token used to reject stale leaders and commands.
///
/// Epochs are attached to coordinator-issued commands so that agents can ignore
/// instructions from a leader that has since been superseded. See
/// `docs/architecture/high-availability.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct Epoch(pub u64);
