//! Scheduler driver (leader-only).
//!
//! A loop of: take `views.latest()`, run the CPU-heavy `Scheduler::schedule`
//! pass in `spawn_blocking` (the trait stays *sync* — it is pure CPU over an
//! immutable view, `docs/architecture/coordinator-runtime.md`, "Scheduler
//! driver"), propose `CommitPlacements`, await the outcome. At most one
//! proposal is in flight by construction: this loop's body is
//! straight-line sequential code, never spawned concurrently.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateViews};
use coppice_scheduler::{PlacementProposal, Scheduler};
use coppice_state::command::CommitPlacements;
use coppice_state::{Command, StateMachine};

use crate::leadership;

/// How long an empty pass waits before trying again, so the loop doesn't
/// spin when there is nothing to schedule. `StateViews` has no "wait for any
/// change" primitive (only `at_least(index)`, which needs a target index),
/// so a short poll is the available alternative to a true view-change wait.
const EMPTY_PASS_BACKOFF: Duration = Duration::from_millis(200);

/// Placeholder `Scheduler` until a real engine lands in `coppice-scheduler`,
/// which today defines only the trait and an empty `PlacementProposal`.
/// Keeps the task topology wired and exercised; every pass is a no-op.
pub struct NoopScheduler;

impl Scheduler for NoopScheduler {
    fn schedule(&self, snapshot: &StateMachine) -> PlacementProposal {
        PlacementProposal { against_version: snapshot.version }
    }
}

/// Run the scheduler driver loop until shutdown.
pub async fn run<C, S>(
    consensus: Arc<C>,
    views: StateViews,
    scheduler: Arc<S>,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) where
    C: Consensus,
    S: Scheduler + Send + Sync + 'static,
{
    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::info!(term, "scheduler driver: gained leadership");

        loop {
            let view = views.latest();
            let pass_scheduler = Arc::clone(&scheduler);
            let proposal = match tokio::task::spawn_blocking(move || pass_scheduler.schedule(view.state()))
                .await
            {
                Ok(proposal) => proposal,
                Err(join_error) => {
                    tracing::error!(error = %join_error, "scheduler driver: scheduling pass panicked");
                    return;
                }
            };

            if proposal_is_empty(&proposal) {
                tokio::select! {
                    biased;
                    _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                    _ = tokio::time::sleep(EMPTY_PASS_BACKOFF) => {}
                }
                continue;
            }

            let command = Command::CommitPlacements(commit_placements_from_proposal(proposal));
            match consensus.propose(command).await {
                Ok(Applied { outcome: Ok(_), .. }) => {
                    // Placed; loop straight back around for the next pass.
                }
                Ok(Applied { outcome: Err(reason), .. }) => {
                    tracing::debug!(
                        ?reason,
                        "scheduler driver: CommitPlacements rejected, recomputing (scheduling-model.md)"
                    );
                }
                Err(e) if e.is_retryable() => {
                    tracing::info!(
                        error = %e,
                        "scheduler driver: retryable propose error, re-gating on leadership"
                    );
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, "scheduler driver: fatal propose error");
                    return;
                }
            }
        }
    }
}

/// `PlacementProposal` doesn't yet carry placement/revocation data
/// (`coppice-scheduler` ships only the trait and an audit-only proposal
/// today) — until it does, every pass is trivially empty.
fn proposal_is_empty(_proposal: &PlacementProposal) -> bool {
    true
}

/// Deferred: `PlacementProposal` -> `CommitPlacements` once
/// `coppice-scheduler` carries real placement/revocation data.
fn commit_placements_from_proposal(proposal: PlacementProposal) -> CommitPlacements {
    todo!(
        "PlacementProposal -> CommitPlacements conversion once coppice-scheduler \
         carries placement data (against_version={})",
        proposal.against_version
    )
}
