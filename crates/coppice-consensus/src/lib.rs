//! # coppice-consensus
//!
//! Raft integration for the coordinator control plane.
//!
//! This crate wraps a Raft implementation and drives the deterministic
//! [`coppice_state::StateMachine`]: it proposes commands, replicates the log,
//! applies committed entries, manages terms/epochs for fencing, and persists
//! snapshots so recovering coordinators need not replay an unbounded log.
//!
//! The concrete Raft library and persistence layer are not yet chosen; see
//! `docs/roadmap/open-decisions.md`. The traits here define the seam so that
//! decision can be deferred without blocking the rest of the workspace.

use coppice_state::Command;

/// Proposes commands to the replicated log and reports leadership.
///
/// Only the leader may accept authoritative writes; followers redirect, proxy,
/// or reject with leader information. See
/// `docs/architecture/high-availability.md`.
pub trait Consensus {
    type Error;

    /// Propose a command for replication. Resolves once the command is
    /// committed and applied, or fails if this node is not the leader or the
    /// proposal loses to a concurrent commit.
    fn propose(&self, command: Command) -> Result<(), Self::Error>;

    /// Whether this node currently believes it is the leader.
    fn is_leader(&self) -> bool;
}
