//! Scheduler driver (leader-only).
//!
//! A loop of: take `views.latest()`, run the CPU-heavy `Scheduler::schedule`
//! pass in `spawn_blocking` (the trait stays *sync* — it is pure CPU over an
//! immutable view, `docs/architecture/coordinator-runtime.md`, "Scheduler
//! driver"), propose `CommitPlacements`, await the outcome. At most one
//! proposal is in flight by construction: this loop's body is
//! straight-line sequential code, never spawned concurrently.
//!
//! Every completed proposal — accepted or rejected — changes what the next
//! pass must see: an acceptance changes state directly, and a rejection
//! (`docs/architecture/command-catalog.md`'s all-or-nothing batch outcome)
//! still means *some* concurrent command landed first (that is what caused
//! the rejection), and only a fresh view can tell the difference from
//! re-proposing the same jobs into the same rejection. So after any
//! completed proposal the driver waits for `views` to catch up to the
//! commit's log index before scheduling again (see the `at_least` call
//! below) — otherwise the next pass reads a stale snapshot and hot-loops on
//! the same rejection.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use coppice_consensus::{Consensus, ConsensusStatus, StateViews};
use coppice_core::id::{AllocationId, AttemptId};
use coppice_core::time::Timestamp;
use coppice_scheduler::Scheduler;
use coppice_state::Command;

use crate::leadership;

/// How long an empty pass waits before trying again.
///
/// Prevents spinning when there is nothing to schedule. `StateViews` has no
/// "wait for any change" primitive (only `at_least(index)`, which needs a
/// target index), so a short poll is the available alternative to a true
/// view-change wait.
const EMPTY_PASS_BACKOFF: Duration = Duration::from_millis(200);

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
        tracing::info!(term, "coordinator gained scheduling leadership");

        loop {
            let view = views.latest();
            let now = Timestamp::now();
            let pass_scheduler = Arc::clone(&scheduler);
            let proposal = match tokio::task::spawn_blocking(move || {
                pass_scheduler.schedule(view.state(), now)
            })
            .await
            {
                Ok(proposal) => proposal,
                Err(join_error) => {
                    tracing::error!(error = %join_error, "scheduler driver: scheduling pass panicked");
                    return;
                }
            };

            if proposal.is_empty() {
                tokio::select! {
                    biased;
                    _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                    _ = tokio::time::sleep(EMPTY_PASS_BACKOFF) => {}
                }
                continue;
            }

            let command = Command::CommitPlacements(
                proposal.to_commit_placements(&mut || (AttemptId::new(), AllocationId::new())),
            );
            let applied = match consensus.propose(command).await {
                Ok(applied) => applied,
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
            };

            match &applied.outcome {
                Ok(_) => {
                    // Placed; the freshness gate below still applies before
                    // the next pass.
                }
                Err(reason) => {
                    tracing::debug!(
                        ?reason,
                        "scheduler driver: CommitPlacements rejected, recomputing (scheduling-model.md)"
                    );
                }
            }

            // Freshness gate: every completed proposal, accepted or
            // rejected, changed what the next pass must see (an acceptance
            // changes state; a rejection means something else did). Wait for
            // `views` to reflect this commit's log index before scheduling
            // again, or the next pass reads a stale snapshot and re-proposes
            // into the same rejection. Races against leadership loss the
            // same way the empty-pass sleep does.
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                result = views.at_least(applied.log_index) => {
                    if result.is_err() {
                        return;
                    }
                }
            }
        }
    }
}
