//! ADR 0044 at apply: the attempt's termination record
//! (`RecordAttemptNotified`) and the `Preempted` outcome's accounting — a
//! free requeue, abort-wins, and the platform-class full refund.

mod common;

use common::*;
use coppice_core::attempt::{
    Acknowledged, AttemptOutcome, AttemptState, PreemptReason, Preemption,
};
use coppice_core::job::{JobState, RetryPolicy};
use coppice_core::quota::{Settlement, TrueUp};
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::RecordAttemptNotified;
use coppice_state::{Command, Event, RejectionReason, StateMachine};

const ATTEMPT: u128 = 11;

/// One bounded job (`max_runtime` 3600 s) placed on node 1 as attempt
/// `aid(11)`, left `Dispatching`.
fn dispatched(retry: RetryPolicy) -> StateMachine {
    let mut sm = setup();
    apply_ok(&mut sm, submit_cmd(jid(1), cpu(1_000), Some(3_600), retry));
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(ATTEMPT), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(ATTEMPT), base_ts()));
    sm
}

/// [`dispatched`], then observed running at the base instant.
fn running(retry: RetryPolicy) -> StateMachine {
    let mut sm = dispatched(retry);
    apply_ok(&mut sm, started_cmd(aid(ATTEMPT), base_ts()));
    sm
}

fn secs(s: i64) -> Timestamp {
    base_ts() + Duration::from_secs(s)
}

fn notified(at: Timestamp, remaining_s: i64, shortfall_s: i64) -> Command {
    notified_for(PreemptReason::Interruption, at, remaining_s, shortfall_s)
}

fn notified_for(
    reason: PreemptReason,
    at: Timestamp,
    remaining_s: i64,
    shortfall_s: i64,
) -> Command {
    Command::RecordAttemptNotified(RecordAttemptNotified {
        attempt: aid(ATTEMPT),
        reason,
        remaining: Duration::from_secs(remaining_s),
        shortfall: Duration::from_secs(shortfall_s),
        observed_at: at,
    })
}

fn preemption(sm: &StateMachine) -> Option<Preemption> {
    sm.attempts[&aid(ATTEMPT)].preemption
}

#[test]
fn a_first_notice_creates_the_record_acknowledged() {
    let mut sm = running(RetryPolicy::default());
    let applied = apply_ok(&mut sm, notified(secs(10), 120, 5));
    assert_eq!(
        preemption(&sm),
        Some(Preemption {
            reason: PreemptReason::Interruption,
            requested_at: secs(10),
            acknowledged: Some(Acknowledged {
                notified_at: secs(10),
                deadline: secs(130),
                notice_shortfall: Duration::from_secs(5),
            }),
        })
    );
    assert_eq!(
        applied.events,
        vec![Event::AttemptNotified {
            attempt: aid(ATTEMPT),
            job: jid(1),
            node: nid(1),
        }]
    );
    // The attempt stays Running and its allocation active: no new phase.
    assert_eq!(
        sm.attempts[&aid(ATTEMPT)].attempt.state,
        AttemptState::Running
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Attempting(aid(ATTEMPT)));
}

#[test]
fn a_notice_acknowledges_an_unacknowledged_request_keeping_its_reason() {
    let mut sm = running(RetryPolicy::default());
    let requested = Preemption {
        reason: PreemptReason::Priority { for_job: jid(9) },
        requested_at: secs(5),
        acknowledged: None,
    };
    sm.attempts.get_mut(&aid(ATTEMPT)).unwrap().preemption = Some(requested);

    apply_ok(&mut sm, notified(secs(10), 60, 0));
    assert_eq!(
        preemption(&sm),
        Some(Preemption {
            reason: PreemptReason::Priority { for_job: jid(9) },
            requested_at: secs(5),
            acknowledged: Some(Acknowledged {
                notified_at: secs(10),
                deadline: secs(70),
                notice_shortfall: Duration::ZERO,
            }),
        })
    );
}

#[test]
fn the_deadline_only_ever_moves_earlier() {
    let mut sm = running(RetryPolicy::default());
    apply_ok(&mut sm, notified(secs(10), 120, 0));
    let first = preemption(&sm);

    // A re-reported mark (same or later stop) changes nothing and says so
    // with no event.
    let later = apply_ok(&mut sm, notified(secs(20), 200, 0));
    assert!(later.events.is_empty());
    assert_eq!(preemption(&sm), first);
    let same = apply_ok(&mut sm, notified(secs(30), 100, 0));
    assert!(same.events.is_empty());
    assert_eq!(preemption(&sm), first);

    // An interruption shortening the window moves the deadline and the
    // shortfall; when it was first notified, and why, never change.
    let earlier = apply_ok(
        &mut sm,
        notified_for(PreemptReason::Drain, secs(40), 30, 50),
    );
    assert_eq!(earlier.events.len(), 1);
    assert_eq!(
        preemption(&sm),
        Some(Preemption {
            reason: PreemptReason::Interruption,
            requested_at: secs(10),
            acknowledged: Some(Acknowledged {
                notified_at: secs(10),
                deadline: secs(70),
                notice_shortfall: Duration::from_secs(50),
            }),
        })
    );
}

#[test]
fn a_notice_for_an_attempt_that_is_not_running_is_stale() {
    // Not yet observed running.
    let mut sm = dispatched(RetryPolicy::default());
    assert_eq!(
        sm.apply(&notified(secs(10), 60, 0)).unwrap_err(),
        RejectionReason::StaleAttemptState(aid(ATTEMPT))
    );
    assert_eq!(preemption(&sm), None);

    // Already ended.
    let mut sm = running(RetryPolicy::default());
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(ATTEMPT),
            AttemptOutcome::Exited { code: 0 },
            60,
            secs(60),
        ),
    );
    assert_eq!(
        sm.apply(&notified(secs(70), 60, 0)).unwrap_err(),
        RejectionReason::StaleAttemptState(aid(ATTEMPT))
    );
    assert_eq!(preemption(&sm), None);

    // Unknown.
    let unknown = Command::RecordAttemptNotified(RecordAttemptNotified {
        attempt: aid(99),
        reason: PreemptReason::Interruption,
        remaining: Duration::from_secs(1),
        shortfall: Duration::ZERO,
        observed_at: secs(1),
    });
    assert_eq!(
        sm.apply(&unknown).unwrap_err(),
        RejectionReason::UnknownAttempt(aid(99))
    );
}

#[test]
fn a_preempted_attempt_requeues_without_consuming_retry_budget() {
    // No retries left at all: a budget-consuming platform outcome would fail
    // the job, a preemption never does.
    let mut sm = running(RetryPolicy {
        max_retries: 0,
        retry_user_errors: false,
    });
    apply_ok(&mut sm, notified(secs(10), 60, 0));
    let outcome = AttemptOutcome::Preempted {
        reason: PreemptReason::Interruption,
    };
    apply_ok(
        &mut sm,
        outcome_cmd(aid(ATTEMPT), outcome.clone(), 75, secs(75)),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Queued);
    assert_eq!(sm.jobs[&jid(1)].retries_used, 0);
    assert_eq!(
        sm.attempts[&aid(ATTEMPT)].attempt.state,
        AttemptState::Terminal(outcome)
    );
    // The record stays on the ended attempt.
    assert!(preemption(&sm).is_some());
}

#[test]
fn a_pending_abort_wins_over_the_preemption_requeue() {
    let mut sm = running(RetryPolicy::default());
    apply_ok(&mut sm, notified(secs(10), 60, 0));
    apply_ok(&mut sm, abort_cmd(jid(1), secs(20)));
    let outcome = AttemptOutcome::Preempted {
        reason: PreemptReason::Interruption,
    };
    apply_ok(
        &mut sm,
        outcome_cmd(aid(ATTEMPT), outcome.clone(), 75, secs(75)),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Aborted);
    // The attempt keeps the outcome the agent reported.
    assert_eq!(
        sm.attempts[&aid(ATTEMPT)].attempt.state,
        AttemptState::Terminal(outcome)
    );
}

/// `Preempted` is platform class (ADR 0029, `f = 1000`): the unused charge is
/// refunded in full, with no retention, unlike the job's own non-zero exit.
#[test]
fn a_preempted_attempt_is_refunded_in_full() {
    let end = secs(600);
    let settle = |outcome: AttemptOutcome| {
        let mut sm = running(RetryPolicy::default());
        apply_ok(&mut sm, outcome_cmd(aid(ATTEMPT), outcome, 600, end));
        let record = &sm.attempts[&aid(ATTEMPT)];
        (record.charge.amount, record.settlement, sm.policy.decay)
    };

    let (charge, settlement, decay) = settle(AttemptOutcome::Preempted {
        reason: PreemptReason::Interruption,
    });
    let Some(Settlement {
        actual_cost,
        true_up: TrueUp::Refund(refund),
    }) = settlement
    else {
        panic!("a preemption settles to a refund: {settlement:?}");
    };
    assert_eq!(
        refund,
        decay.decay_between(charge.saturating_sub(actual_cost), base_ts(), end)
    );

    let (_, own_exit, _) = settle(AttemptOutcome::Exited { code: 1 });
    let Some(Settlement {
        true_up: TrueUp::Refund(retained),
        ..
    }) = own_exit
    else {
        panic!("a short exit settles to a refund: {own_exit:?}");
    };
    assert!(
        retained < refund,
        "the job's own exit keeps retention: {retained:?} !< {refund:?}"
    );
}

/// The provider's interruption deadline (ADR 0044 §5) is display-only node
/// state owned by the agent's draining announcement: `SetNodeDraining` sets
/// and clears it, and a registration rewrites it.
#[test]
fn the_interruption_deadline_follows_the_draining_announcement() {
    use coppice_state::command::{RegisterNode, SetNodeDraining};
    let mut sm = setup();
    let deadline = secs(120);
    apply_ok(
        &mut sm,
        Command::SetNodeDraining(SetNodeDraining {
            node: nid(1),
            draining: true,
            interruption_deadline: Some(deadline),
            at: secs(1),
        }),
    );
    assert_eq!(sm.nodes[&nid(1)].interruption_deadline, Some(deadline));
    assert!(sm.nodes[&nid(1)].draining);

    // A registration that does not carry it clears it, with `draining`.
    let Command::RegisterNode(reg) = register_node_cmd(nid(1), cpu(10_000), secs(2)) else {
        unreachable!("register_node_cmd builds a RegisterNode");
    };
    apply_ok(&mut sm, Command::RegisterNode(reg.clone()));
    assert_eq!(sm.nodes[&nid(1)].interruption_deadline, None);
    apply_ok(
        &mut sm,
        Command::RegisterNode(RegisterNode {
            draining: true,
            interruption_deadline: Some(deadline),
            ..reg
        }),
    );
    assert_eq!(sm.nodes[&nid(1)].interruption_deadline, Some(deadline));
}
