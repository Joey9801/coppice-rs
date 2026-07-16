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
//! effective-score order (ADR 0021) as capacity frees, wherever it frees.
//! Accruing allocations are created only as the license to backfill *past* a
//! blocked high-score job (at most K held cluster-wide), are re-planned every
//! pass, and are revocable at no cost to the job — the only revocations the
//! scheduler ever proposes; funded allocations are never revoked in v1
//! (ADR 0014). See this crate's `README.md`,
//! `docs/scheduling/scheduling-model.md`, `docs/scheduling/scheduler-v1.md`,
//! and `docs/scheduling/quotas-and-priorities.md`.
//!
//! The pass itself is a pure function of `(snapshot, now)`: no I/O, no clock
//! reads, no randomness. Attempt and allocation ids are minted by the caller
//! when the proposal is converted to a command
//! ([`PlacementProposal::to_commit_placements`]), so determinism arguments
//! stay confined to the decisions. Scheduling is CPU-intensive and runs
//! asynchronously so it never blocks Raft application, API handling, or agent
//! heartbeat processing.

use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{AllocationSpec, CommitPlacements, Placement};
use coppice_state::StateMachine;

mod engine;
pub mod score;

pub use engine::HeuristicScheduler;

/// Scheduler-side tuning knobs.
///
/// Deliberately not replicated policy: these shape proposals, and every
/// proposal is re-validated against replicated state at commit
/// (`docs/architecture/command-catalog.md`), so replicas need not agree on
/// them (ADR 0020's node-config side of the line).
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Jobs considered for seating per pass — the top of the effective-score
    /// order. Bounds per-cycle work so a deep backlog cannot wedge a pass.
    pub max_candidates: usize,
    /// Placements emitted per pass. Revocations are bounded separately by the
    /// replicated accrual cap K and need no knob.
    pub max_placements_per_cycle: usize,
    /// Weight of the ADR 0021 age term, in effective-priority points per
    /// decay half-life waited.
    pub w_age: f64,
    /// Minimum earliness by which a move to another node must improve an
    /// existing accrual's finite `projected_ready` bound before the pass
    /// will revoke and reseat it there (ADR 0027). An indefinite bound is
    /// always worth trading for a finite one, whatever the threshold.
    pub replan_min_improvement: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            max_candidates: 4096,
            max_placements_per_cycle: 512,
            w_age: score::DEFAULT_AGE_WEIGHT,
            replan_min_improvement: Duration::from_secs(300),
        }
    }
}

/// One proposed seating of a job on a node.
///
/// Whether the resulting allocation starts `Funded` or `Accruing` is decided
/// by apply from actual free capacity (ADR 0014); `expect_funded` records the
/// scheduler's own simulation of that outcome for diagnostics and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedPlacement {
    pub job: JobId,
    pub node: NodeId,
    pub requested: Resources,
    pub expect_funded: bool,
}

/// A batch of proposed placements and accrual revocations, valid against a
/// specific state version.
///
/// The coordinator commits or rejects it atomically (all-or-nothing with
/// per-item diagnostics, `docs/architecture/command-catalog.md`). Placement
/// order is funding order and must be preserved through conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementProposal {
    /// The state version this proposal was computed against; becomes the
    /// command's `expected_version` (an audit record — semantic re-validation
    /// is what gates the batch).
    pub against_version: u64,
    /// The `now` the pass ran at; becomes the command's `proposed_at`.
    pub now: Timestamp,
    /// Accruing allocations to revoke, always paired with placements they
    /// enable in this same batch (a reseat elsewhere or a strict-backfill
    /// lend). Never a funded allocation (ADR 0014).
    pub revocations: Vec<AllocationId>,
    pub placements: Vec<ProposedPlacement>,
}

impl PlacementProposal {
    /// Whether the pass decided nothing. The driver backs off on empty
    /// passes, so a pass over a state with nothing actionable must be empty.
    pub fn is_empty(&self) -> bool {
        self.revocations.is_empty() && self.placements.is_empty()
    }

    /// Convert to the command, minting proposer ids via `mint`.
    ///
    /// Ids are random UUIDs in production (the driver) and deterministic
    /// counters in tests, which is why minting is the caller's: the pass
    /// itself stays a pure function of the snapshot. Emits the v1 shape apply
    /// demands: exactly one allocation per placement, `group` = the job's id
    /// (singleton groups).
    pub fn to_commit_placements(
        &self,
        mint: &mut dyn FnMut() -> (AttemptId, AllocationId),
    ) -> CommitPlacements {
        let placements = self
            .placements
            .iter()
            .map(|p| {
                let (attempt, allocation) = mint();
                Placement {
                    job: p.job,
                    attempt,
                    group: GroupId(p.job.0),
                    allocations: vec![AllocationSpec {
                        id: allocation,
                        node: p.node,
                        requested: p.requested,
                    }],
                }
            })
            .collect();
        CommitPlacements {
            expected_version: self.against_version,
            revocations: self.revocations.clone(),
            placements,
            proposed_at: self.now,
        }
    }
}

/// Computes placement proposals from a read-only view of cluster state.
pub trait Scheduler {
    /// Run one scheduling pass against a snapshot, returning proposed
    /// placements for the coordinator to validate.
    ///
    /// `now` is proposer-stamped wall time, passed in so the pass itself
    /// never reads a clock.
    fn schedule(&self, snapshot: &StateMachine, now: Timestamp) -> PlacementProposal;
}
