//! # coppice-scheduler
//!
//! The scheduler engine turns queued jobs into proposed placement decisions.
//!
//! It never mutates authoritative state directly. Instead it operates on a
//! consistent snapshot of cluster state, computes a batch of proposed
//! placements, accruing allocations, and revocations, and hands them back to
//! the coordinator to be validated and committed through Raft. Because it runs
//! against a snapshot, proposals may fail validation due to concurrent
//! changes — that is a normal path that triggers recomputation, not an error.
//!
//! On a full cluster, queued jobs stay **unpinned** and are seated in
//! effective-score order as capacity frees, wherever it frees. Accruing
//! allocations are created only as the license to backfill *past* a blocked
//! high-score job (at most K held cluster-wide), are re-planned every pass,
//! and are revocable at no cost to the job. See this crate's `README.md`,
//! `docs/scheduling/scheduling-model.md`, and
//! `docs/scheduling/quotas-and-priorities.md`.
//!
//! Scheduling is CPU-intensive and runs asynchronously so it never blocks Raft
//! application, API handling, or agent heartbeat processing.

use coppice_state::StateMachine;

/// A batch of proposed placements, accruals, and revocations, valid against a specific state version.
///
/// The coordinator commits or rejects it atomically.
#[derive(Debug, Default)]
pub struct PlacementProposal {
    /// The state version this proposal was computed against.
    pub against_version: u64,
    // Placeholder for the proposed placements, accruals, and revocations.
}

/// Computes placement proposals from a read-only view of cluster state.
pub trait Scheduler {
    /// Run one scheduling pass against a snapshot, returning proposed
    /// placements for the coordinator to validate.
    fn schedule(&self, snapshot: &StateMachine) -> PlacementProposal;
}
