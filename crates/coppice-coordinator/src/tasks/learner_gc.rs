//! Stale-learner garbage collection (ADR 0037 §7, last paragraph).
//!
//! Installations that died permanently before promotion leave learner seats
//! behind. They never affect quorum — that is why the ADR is content to let
//! them linger for an unambiguous period — but under instance churn they
//! accumulate in membership, and each keeps a machine-identity binding alive
//! against a machine that is never coming back. This task, leader-only, is
//! what keeps membership records bounded.
//!
//! Three rules make it safe:
//!
//! - **The criterion is failed contact, never lack of log advancement.** A
//!   fully caught-up learner on an idle cluster — the `new_node_id` of a
//!   pending `ReplaceVoter`, say — may legitimately see no new entries for
//!   hours; it keeps acknowledging heartbeats, so the leader's contact
//!   evidence says it is alive and it survives indefinitely. The evidence
//!   lives in `coppice-consensus`'s contact tracker and is read through
//!   [`Consensus::expired_learners`].
//! - **Voters are never touched.** There is no background *voter* reaper
//!   (§7): voter membership shrinks only inside `ReplaceVoter`, an
//!   evidence-gated promotion, or an explicit `admin remove`.
//! - **Retire the binding, then release the seat.** Retirement is a mark, not
//!   a delete: the one-identity↔one-seat invariant extends past the seat's
//!   life, so a re-arriving installation carrying the retired identity is
//!   *refused* rather than silently re-admitted. Doing it in the other order
//!   would open exactly that window. Each step is idempotent, so a crash
//!   between them is repaired by the next tick.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use coppice_consensus::{Consensus, ConsensusStatus, StateViews};
use coppice_core::time::Timestamp;
use coppice_state::command::RetireMachineBinding;
use coppice_state::Command;

use crate::leadership;

/// The longest gap between GC passes, whatever `learner_expiry` says.
///
/// The tick period is `min(learner_expiry / 4, GC_INTERVAL_MAX)`: fine enough
/// relative to the expiry that a reap happens promptly after the bound is
/// crossed, and never so coarse on a long expiry (the 1h default) that an
/// operator watching status wonders whether the task is running at all.
const GC_INTERVAL_MAX: Duration = Duration::from_secs(60);

/// The shortest tick period, so a test-scale `learner_expiry` cannot turn this
/// into a spin loop.
const GC_INTERVAL_MIN: Duration = Duration::from_millis(100);

/// Run the learner-GC loop until shutdown, self-gating on leadership like
/// every other leader-only task (`coordinator-runtime.md`, "Leader
/// transitions").
pub async fn run<C: Consensus>(
    consensus: Arc<C>,
    views: StateViews,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let period = (consensus.learner_expiry() / 4).clamp(GC_INTERVAL_MIN, GC_INTERVAL_MAX);

    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::debug!(term, "learner-gc: gained leadership");

        let mut ticker = interval(period);
        // The first tick fires immediately; skip it so gaining leadership
        // does not itself trigger a sweep before any contact evidence has
        // accumulated under this term.
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                _ = ticker.tick() => run_pass(&consensus, &views).await,
            }
        }
    }
}

/// One sweep: retire and release every learner past `learner_expiry`.
async fn run_pass<C: Consensus>(consensus: &Arc<C>, views: &StateViews) {
    for learner in consensus.expired_learners() {
        // The binding first (see the module doc): a retired identity is
        // refused re-admission even if the seat removal below never lands.
        let machine = views
            .latest()
            .state()
            .machine_for_raft_node(learner)
            .copied();
        if let Some(machine) = machine {
            match consensus
                .propose(Command::RetireMachineBinding(RetireMachineBinding {
                    machine,
                    retired_at: Timestamp::now(),
                }))
                .await
            {
                // An already-retired binding applies as a no-op, so a repeated
                // pass (the removal below failed last time) is free.
                Ok(applied) => {
                    if let Err(reason) = applied.outcome {
                        tracing::warn!(
                            node_id = learner,
                            %machine,
                            %reason,
                            "learner-gc: retiring the machine binding was rejected; leaving the \
                             seat in place"
                        );
                        continue;
                    }
                }
                Err(e) => {
                    // Lost leadership, a timeout, a shutdown: the next tick
                    // (or the next leader) retries. Never remove the seat
                    // without the binding retired first.
                    tracing::debug!(
                        node_id = learner,
                        error = %e,
                        "learner-gc: could not retire the machine binding; retrying next tick"
                    );
                    continue;
                }
            }
        }

        match consensus.remove_node(learner).await {
            Ok(()) => tracing::info!(
                node_id = learner,
                machine = ?machine.map(|m| m.to_string()),
                "learner-gc: removed a learner with no successful replication contact for \
                 longer than learner_expiry (ADR 0037 §7)"
            ),
            Err(e) => tracing::debug!(
                node_id = learner,
                error = %e,
                "learner-gc: could not remove the expired learner; retrying next tick"
            ),
        }
    }
}
