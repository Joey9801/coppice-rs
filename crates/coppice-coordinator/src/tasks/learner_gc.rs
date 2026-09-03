//! Stale-learner garbage collection (ADR 0037 §7, last paragraph).
//!
//! Installations that died permanently before promotion leave learner seats
//! behind. They never affect quorum — that is why the ADR is content to let
//! them linger for an unambiguous period — but under instance churn they
//! accumulate in membership, and each keeps a machine-identity binding alive
//! against a machine that is never coming back. This task, leader-only, is
//! what keeps membership records coherent and bounded.
//!
//! Bounded is the stale-learner half. Coherent is the other way membership
//! goes wrong, and it has no operator waiting on it: openraft's
//! `change_membership` is two sequential proposals — a joint configuration,
//! then the uniform one — and a leader that loses leadership, crashes, or
//! merely abandons the call on its commit deadline between them leaves the
//! cluster in the joint config. Joint membership silently enforces the OLD
//! quorum requirement as well as the new one, so availability is reduced while
//! every status surface reports health, and — because the seat already appears
//! in the joint union — the joiner's convergence loop declares itself a voter
//! and stops retrying. Nothing else would ever repair it. So this loop calls
//! [`Consensus::finish_pending_membership_change`] on gaining leadership (which
//! also catches the case openraft documents: a predecessor that died between
//! the two phases) and at the start of every pass.
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
//!   evidence-gated promotion, or an explicit `admin remove`. Because a
//!   candidate's own convergence loop runs concurrently with this task, the
//!   whole destructive step is delegated to
//!   [`Consensus::reap_expired_learner`], which re-verifies
//!   still-a-learner-and-still-expired under the same lock promotion commits
//!   hold — a candidate that races to voter is skipped, retirement and all.
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

        // Immediately, before the first sweep and before any contact evidence
        // has had time to accumulate: a membership change stranded in the
        // joint configuration by the *previous* leader — the case openraft's
        // own documentation calls out — is repaired by whoever holds the term
        // next, and this is the first moment this node can do it. Failures are
        // debug-logged and retried on the next tick, like the reap errors
        // below.
        if let Err(e) = consensus.finish_pending_membership_change().await {
            tracing::debug!(
                term,
                error = %e,
                "learner-gc: could not finish a half-done membership change on gaining \
                 leadership; retrying next tick"
            );
        }

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

/// One sweep: finish any half-done membership change, then retire and release
/// every learner past `learner_expiry`.
///
/// [`Consensus::expired_learners`] is only a candidate list; the destructive
/// half runs entirely inside [`Consensus::reap_expired_learner`], which
/// re-checks — under the same lock every membership mutation holds — that the
/// candidate is *still* an expired non-voter at the moment it acts. A
/// candidate that raced to voter in the awaits between listing and reaping is
/// skipped with nothing proposed: its retirement never lands, its seat is
/// never touched, and this task can never become the background voter reaper
/// §7 forbids.
async fn run_pass<C: Consensus>(consensus: &Arc<C>, views: &StateViews) {
    // First, because it is the repair nobody else retries (see the module
    // doc), and because a joint membership's voter set is the union of two
    // configurations — so the "is this candidate still a learner" question the
    // reap below turns on cannot be answered coherently until the change
    // finishes. A uniform membership proposes nothing, so this costs a metrics
    // read on every ordinary pass.
    if let Err(e) = consensus.finish_pending_membership_change().await {
        tracing::debug!(
            error = %e,
            "learner-gc: could not finish a half-done membership change; retrying next tick"
        );
    }

    for learner in consensus.expired_learners() {
        let machine = views
            .latest()
            .state()
            .machine_for_raft_node(learner)
            .copied();
        // The reap commits the retirement strictly before releasing the seat
        // (see the module doc); an already-retired binding applies as a
        // no-op, so a repeated pass (the removal failed last time) is free.
        let retire = machine.map(|machine| {
            Command::RetireMachineBinding(RetireMachineBinding {
                machine,
                retired_at: Timestamp::now(),
            })
        });
        match consensus.reap_expired_learner(learner, retire).await {
            Ok(true) => tracing::info!(
                node_id = learner,
                machine = ?machine.map(|m| m.to_string()),
                "learner-gc: retired and removed a learner with no successful replication \
                 contact for longer than learner_expiry (ADR 0037 §7)"
            ),
            Ok(false) => tracing::debug!(
                node_id = learner,
                "learner-gc: candidate was no longer an expired learner at the destructive \
                 point (promoted, recovered, or its retirement was refused); skipped"
            ),
            Err(e) => tracing::debug!(
                node_id = learner,
                error = %e,
                "learner-gc: could not reap the expired learner; retrying next tick"
            ),
        }
    }
}
