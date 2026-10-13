//! Admission on a draining node (ADR 0044 §5): the horizon `H`, bounded
//! backfill, and the conservative fit check, through `CommitPlacements`.
//!
//! Every test uses `nid(1)` (10 000 mCPU) and the default `abort_grace` of
//! 30 s, so a running anchor with `max_runtime` 3600 s started at `base_ts()`
//! bounds the node at `base + 3630 s`.

mod common;

use common::*;
use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, AttemptState, PreemptReason};
use coppice_core::job::RetryPolicy;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{CommitPlacements, RecordAttemptNotified};
use coppice_state::{Command, NodeHorizon, Rejection, RejectionReason, StateMachine};

const S: i64 = 1_000_000;

fn at(seconds: i64) -> Timestamp {
    ts(TS_US + seconds * S)
}

/// The anchor's bound: started at `base`, 3600 s runtime, 30 s grace.
fn anchor_horizon() -> Timestamp {
    at(3_630)
}

/// `setup()` plus a running anchor job on `nid(1)` — 4000 mCPU, the given
/// `max_runtime` — started at `base_ts()`, then the node drained.
fn drained_with_anchor(max_runtime_s: Option<i64>) -> StateMachine {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(4_000), max_runtime_s, RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, drain_cmd(nid(1)));
    sm
}

fn submit(sm: &mut StateMachine, job: u128, millis: u64, max_runtime_s: Option<i64>) {
    apply_ok(
        sm,
        submit_cmd(jid(job), cpu(millis), max_runtime_s, RetryPolicy::default()),
    );
}

/// Place job `n` (attempt `n*11`, allocation `n*111`) on `nid(1)` at `when`.
fn place(
    sm: &mut StateMachine,
    n: u128,
    millis: u64,
    when: Timestamp,
) -> Result<(), RejectionReason> {
    sm.apply(&place_cmd(
        placement(jid(n), aid(n * 11), alid(n * 111), nid(1), cpu(millis)),
        when,
    ))
    .map(|_| ())
}

fn draining_rejection(index: u32) -> RejectionReason {
    RejectionReason::InvalidBatch(vec![Rejection {
        item_index: index,
        reason: RejectionReason::NodeDraining(nid(1)),
    }])
}

#[test]
fn bounded_backfill_within_the_horizon_is_admitted_funded_with_its_cutoff() {
    let mut sm = drained_with_anchor(Some(3_600));
    assert_eq!(
        sm.node_horizon(nid(1)),
        NodeHorizon::Bounded(anchor_horizon())
    );
    // 60 + 1800 + 30 = 1890 s ≤ 3630 s.
    submit(&mut sm, 2, 2_000, Some(1_800));
    place(&mut sm, 2, 2_000, at(60)).expect("bounded backfill fits the horizon");

    let attempt = &sm.attempts[&aid(22)];
    assert_eq!(attempt.drain_cutoff, Some(anchor_horizon()));
    assert_eq!(attempt.attempt.state, AttemptState::Ready);
    assert_eq!(
        sm.allocations[&alid(222)].allocation.state,
        AllocationState::Funded,
        "backfill is fully funded at once"
    );
    // Exactly at the horizon is still within it: 1830 + 1770 + 30 = 3630.
    submit(&mut sm, 3, 1_000, Some(1_770));
    place(&mut sm, 3, 1_000, at(1_830)).expect("a bound at H is admitted");
}

#[test]
fn backfill_past_the_horizon_is_refused() {
    let mut sm = drained_with_anchor(Some(3_600));
    // 60 + 3600 + 30 = 3690 s > 3630 s.
    submit(&mut sm, 2, 2_000, Some(3_600));
    assert_eq!(place(&mut sm, 2, 2_000, at(60)), Err(draining_rejection(0)));
    // A job with no enforced bound is never backfill.
    submit(&mut sm, 3, 1_000, None);
    assert_eq!(place(&mut sm, 3, 1_000, at(60)), Err(draining_rejection(0)));
}

#[test]
fn unbounded_running_work_leaves_no_horizon_and_admits_nothing() {
    let mut sm = drained_with_anchor(None);
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Unbounded);
    submit(&mut sm, 2, 1_000, Some(1));
    assert_eq!(place(&mut sm, 2, 1_000, at(60)), Err(draining_rejection(0)));
}

/// The anchor's agent reports a notice delivered at `observed_s` with
/// `remaining_s` left (ADR 0044 §3), acknowledging its termination record.
fn notify_anchor(sm: &mut StateMachine, observed_s: i64, remaining_s: i64) {
    apply_ok(
        sm,
        Command::RecordAttemptNotified(RecordAttemptNotified {
            attempt: aid(11),
            reason: PreemptReason::Interruption,
            remaining: Duration::from_secs(remaining_s),
            shortfall: Duration::ZERO,
            observed_at: at(observed_s),
        }),
    );
}

#[test]
fn an_acknowledged_termination_bounds_running_work_without_max_runtime() {
    // No `max_runtime`, so only the notice deadline bounds the anchor:
    // observed at +10 s with 120 s left is a deadline of +130 s, and the
    // stop grace puts H at +160 s.
    let mut sm = drained_with_anchor(None);
    notify_anchor(&mut sm, 10, 120);
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Bounded(at(160)));
    submit(&mut sm, 2, 1_000, Some(60));
    place(&mut sm, 2, 1_000, at(60)).expect("60 s + 30 s grace lands by H");
    assert_eq!(sm.attempts[&aid(22)].drain_cutoff, Some(at(160)));
}

#[test]
fn an_acknowledged_termination_only_tightens_the_horizon() {
    // Earlier than the runtime bound: the deadline wins.
    let mut sm = drained_with_anchor(Some(3_600));
    notify_anchor(&mut sm, 10, 120);
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Bounded(at(160)));

    // Later than the runtime bound: the runtime bound stands.
    let mut sm = drained_with_anchor(Some(3_600));
    notify_anchor(&mut sm, 10, 7_200);
    assert_eq!(
        sm.node_horizon(nid(1)),
        NodeHorizon::Bounded(anchor_horizon())
    );
}

#[test]
fn work_not_yet_started_leaves_no_horizon() {
    let mut sm = setup();
    submit(&mut sm, 1, 4_000, Some(3_600));
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    // Ready, never started: nothing bounds when it launches.
    apply_ok(&mut sm, drain_cmd(nid(1)));
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Unbounded);
    submit(&mut sm, 2, 1_000, Some(1));
    assert_eq!(place(&mut sm, 2, 1_000, at(60)), Err(draining_rejection(0)));
}

#[test]
fn an_accrual_on_the_node_leaves_no_horizon() {
    let mut sm = setup();
    submit(&mut sm, 1, 8_000, Some(3_600));
    place(&mut sm, 1, 8_000, base_ts()).unwrap();
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    // 2000 free; a 6000 request accrues.
    submit(&mut sm, 2, 6_000, Some(3_600));
    place(&mut sm, 2, 6_000, base_ts()).unwrap();
    assert_eq!(
        sm.allocations[&alid(222)].allocation.state,
        AllocationState::Accruing
    );
    apply_ok(&mut sm, drain_cmd(nid(1)));
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Unbounded);
    submit(&mut sm, 3, 1_000, Some(1));
    assert_eq!(place(&mut sm, 3, 1_000, at(60)), Err(draining_rejection(0)));
}

#[test]
fn an_empty_drained_node_is_complete_and_admits_nothing() {
    let mut sm = setup();
    apply_ok(&mut sm, drain_cmd(nid(1)));
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Empty);
    submit(&mut sm, 2, 1_000, Some(1));
    assert_eq!(place(&mut sm, 2, 1_000, at(60)), Err(draining_rejection(0)));
}

/// Backfill never opens an accrual: a seat that does not fit the node's
/// free capacity now is refused, not left accruing.
#[test]
fn backfill_that_does_not_fit_now_is_refused_rather_than_accrued() {
    let mut sm = drained_with_anchor(Some(3_600));
    // 6000 free.
    submit(&mut sm, 2, 7_000, Some(60));
    assert_eq!(place(&mut sm, 2, 7_000, at(60)), Err(draining_rejection(0)));
    assert!(sm.accrual_queue.is_empty());
}

/// The fit is checked against the batch's earlier seats too, so two seats
/// that each fit alone but not together reject the second.
#[test]
fn backfill_fit_counts_the_batch_s_earlier_seats() {
    let mut sm = drained_with_anchor(Some(3_600));
    submit(&mut sm, 2, 4_000, Some(60));
    submit(&mut sm, 3, 4_000, Some(60));
    let batch = |third: u64| {
        Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![],
            placements: vec![
                placement(jid(2), aid(22), alid(222), nid(1), cpu(4_000)),
                placement(jid(3), aid(33), alid(333), nid(1), cpu(third)),
            ],
            proposed_at: at(60),
        })
    };
    assert_eq!(sm.apply(&batch(4_000)).unwrap_err(), draining_rejection(1));
    // 4000 + 2000 = the 6000 free: both seat, both funded.
    apply_ok(&mut sm, batch(2_000));
    assert_eq!(
        sm.allocations[&alid(333)].allocation.state,
        AllocationState::Funded
    );
}

/// A backfill's recorded cutoff bounds it for its whole life: started later
/// than its launch, its running formula would overshoot, but the cutoff keeps
/// the horizon where it was — even after the anchor that set it is gone.
#[test]
fn backfill_cannot_move_the_horizon_later() {
    let mut sm = drained_with_anchor(Some(3_600));
    // 1700 + 1800 + 30 = 3530 ≤ 3630.
    submit(&mut sm, 2, 2_000, Some(1_800));
    place(&mut sm, 2, 2_000, at(1_700)).unwrap();
    apply_ok(&mut sm, dispatch_cmd(aid(22), at(1_700)));
    // Observed late: 1850 + 1800 + 30 = 3680 > the 3630 cutoff.
    apply_ok(&mut sm, started_cmd(aid(22), at(1_850)));
    assert_eq!(
        sm.node_horizon(nid(1)),
        NodeHorizon::Bounded(anchor_horizon())
    );
    apply_ok(
        &mut sm,
        outcome_cmd(aid(11), AttemptOutcome::Exited { code: 0 }, 100, at(100)),
    );
    assert_eq!(
        sm.node_horizon(nid(1)),
        NodeHorizon::Bounded(anchor_horizon()),
        "the backfill's cutoff, not its later running bound"
    );
    apply_ok(
        &mut sm,
        outcome_cmd(aid(22), AttemptOutcome::Exited { code: 0 }, 100, at(1_950)),
    );
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Empty);
}

/// An agent that announced it is leaving admits nothing, bounded or not.
#[test]
fn an_announced_node_refuses_even_bounded_backfill() {
    let mut sm = drained_with_anchor(Some(3_600));
    apply_ok(&mut sm, set_draining_cmd(nid(1), true));
    submit(&mut sm, 2, 1_000, Some(60));
    assert_eq!(place(&mut sm, 2, 1_000, at(60)), Err(draining_rejection(0)));
}

/// Undrain reopens the node to ordinary placements: no cutoff is recorded.
#[test]
fn an_undrained_node_takes_ordinary_placements_without_a_cutoff() {
    let mut sm = drained_with_anchor(Some(3_600));
    apply_ok(&mut sm, undrain_cmd(nid(1)));
    submit(&mut sm, 2, 1_000, None);
    place(&mut sm, 2, 1_000, at(60)).unwrap();
    assert_eq!(sm.attempts[&aid(22)].drain_cutoff, None);
}

/// A backfill launch the agent refused at the door reports `Revoked`
/// (ADR 0044 §5): accepted only for a drain backfill that never started, and
/// resolved like any revocation — requeued free of retry budget.
#[test]
fn a_refused_backfill_launch_resolves_revoked_as_a_free_requeue() {
    let mut sm = drained_with_anchor(Some(3_600));
    submit(&mut sm, 2, 2_000, Some(1_800));
    place(&mut sm, 2, 2_000, at(60)).unwrap();
    apply_ok(&mut sm, dispatch_cmd(aid(22), at(60)));

    // The anchor is ordinary work: an agent-reported Revoked is refused.
    assert!(matches!(
        sm.apply(&outcome_cmd(aid(11), AttemptOutcome::Revoked, 0, at(61))),
        Err(RejectionReason::InvalidCommand(_))
    ));

    apply_ok(
        &mut sm,
        outcome_cmd(aid(22), AttemptOutcome::Revoked, 0, at(61)),
    );
    assert_eq!(
        sm.attempts[&aid(22)].attempt.state,
        AttemptState::Terminal(AttemptOutcome::Revoked)
    );
    let job = &sm.jobs[&jid(2)];
    assert_eq!(job.state, coppice_core::job::JobState::Queued);
    assert_eq!(job.retries_used, 0, "a revocation is free");
}

/// Once a backfill has started, Revoked is no longer a door refusal.
#[test]
fn a_started_backfill_cannot_report_revoked() {
    let mut sm = drained_with_anchor(Some(3_600));
    submit(&mut sm, 2, 2_000, Some(1_800));
    place(&mut sm, 2, 2_000, at(60)).unwrap();
    apply_ok(&mut sm, dispatch_cmd(aid(22), at(60)));
    apply_ok(&mut sm, started_cmd(aid(22), at(61)));
    assert!(matches!(
        sm.apply(&outcome_cmd(aid(22), AttemptOutcome::Revoked, 0, at(62))),
        Err(RejectionReason::InvalidCommand(_))
    ));
}

// ---- Deadline drains (ADR 0044 §5) ----
//
// Under `setup()`'s policy — notice cap 5 min, `abort_grace` 30 s, cleanup
// margin 30 s — a deadline drain needs 360 s of lead, and `T_notice = D −
// 360 s`.

const LEAD_S: i64 = 360;

fn deadline_drain(
    sm: &mut StateMachine,
    now_s: i64,
    deadline_s: i64,
) -> Result<(), RejectionReason> {
    sm.apply(&deadline_drain_cmd_at(nid(1), at(now_s), at(deadline_s)))
        .map(|_| ())
}

fn recorded_drain(sm: &StateMachine) -> Option<coppice_core::node::Drain> {
    sm.nodes[&nid(1)].node.drain
}

#[test]
fn the_deadline_drain_times_follow_the_policy() {
    let sm = setup();
    assert_eq!(
        sm.policy.deadline_drain_min_lead(),
        Duration::from_secs(LEAD_S)
    );
    let times = sm.policy.deadline_drain_times(at(1_000));
    assert_eq!(times.t_stop, at(1_000 - 60));
    assert_eq!(times.t_notice, at(1_000 - LEAD_S));
}

#[test]
fn a_deadline_needs_the_full_lead_from_receipt_exactly() {
    // Exactly the lead is enough.
    let mut sm = setup();
    deadline_drain(&mut sm, 0, LEAD_S).expect("a deadline exactly the lead away is accepted");
    assert_eq!(
        recorded_drain(&sm),
        Some(coppice_core::node::Drain {
            requested_at: at(0),
            deadline: Some(at(LEAD_S)),
        })
    );

    // One microsecond short is refused, and leaves no record behind.
    let mut sm = setup();
    let short = sm.apply(&deadline_drain_cmd_at(
        nid(1),
        at(0),
        ts(TS_US + LEAD_S * S - 1),
    ));
    assert_eq!(
        short.map(|_| ()),
        Err(RejectionReason::DrainDeadlineTooSoon {
            node: nid(1),
            min_lead: Duration::from_secs(LEAD_S),
        })
    );
    assert_eq!(recorded_drain(&sm), None);
}

#[test]
fn a_deadline_on_a_regular_drain_keeps_its_stamp() {
    let mut sm = setup();
    apply_ok(&mut sm, drain_cmd_at(nid(1), at(0)));
    deadline_drain(&mut sm, 100, 1_000).expect("a regular drain gains a deadline");
    assert_eq!(
        recorded_drain(&sm),
        Some(coppice_core::node::Drain {
            requested_at: at(0),
            deadline: Some(at(1_000)),
        })
    );
}

#[test]
fn a_replacement_is_accepted_only_before_the_recorded_notice_time() {
    let mut sm = setup();
    deadline_drain(&mut sm, 0, 2_000).expect("first deadline");
    // T_notice(2000) = 1640. Later and earlier replacements both pass before it.
    deadline_drain(&mut sm, 100, 3_000).expect("a later replacement before T_notice");
    // T_notice(3000) = 2640.
    deadline_drain(&mut sm, 2_000, 2_400).expect("an earlier replacement before T_notice");
    assert_eq!(
        recorded_drain(&sm),
        Some(coppice_core::node::Drain {
            requested_at: at(0),
            deadline: Some(at(2_400)),
        }),
        "a replacement keeps the original stamp"
    );
    // T_notice(2400) = 2040: from there on the deadline is fixed.
    assert_eq!(
        deadline_drain(&mut sm, 2_040, 5_000),
        Err(RejectionReason::DrainNoticeStarted {
            node: nid(1),
            notice_at: at(2_040),
        })
    );
    // One microsecond before it is still a replacement.
    let mut before = setup();
    deadline_drain(&mut before, 0, 2_400).unwrap();
    before
        .apply(&deadline_drain_cmd_at(
            nid(1),
            ts(TS_US + 2_040 * S - 1),
            at(5_000),
        ))
        .expect("just before T_notice");
    // The deadline on record is a no-op — before the lead runs out, and
    // after it (a retried request stays idempotent).
    let version = sm.version;
    deadline_drain(&mut sm, 1_000, 2_400).expect("the same deadline is a no-op");
    deadline_drain(&mut sm, 2_100, 2_400).expect("still a no-op past T_notice");
    assert_eq!(recorded_drain(&sm).unwrap().deadline, Some(at(2_400)));
    assert_eq!(sm.version, version + 2, "only the version moves");
}

#[test]
fn a_plain_re_drain_never_removes_a_deadline_and_undrain_clears_it() {
    let mut sm = setup();
    deadline_drain(&mut sm, 0, 2_000).unwrap();
    apply_ok(&mut sm, drain_cmd_at(nid(1), at(1_900)));
    assert_eq!(
        recorded_drain(&sm),
        Some(coppice_core::node::Drain {
            requested_at: at(0),
            deadline: Some(at(2_000)),
        }),
        "a plain drain on a deadline drain is a no-op, even past T_notice"
    );
    apply_ok(&mut sm, undrain_cmd(nid(1)));
    assert_eq!(
        recorded_drain(&sm),
        None,
        "undrain clears the record, deadline and all"
    );
    // Undrain then drain again starts over, without a deadline.
    apply_ok(&mut sm, drain_cmd_at(nid(1), at(1_950)));
    assert_eq!(recorded_drain(&sm).unwrap().deadline, None);
}

#[test]
fn a_deadline_drain_admits_backfill_up_to_its_notice_time() {
    // An unbounded anchor: a regular drain would admit nothing, but a deadline
    // drain stops it at T_stop, so its cutoff is T_notice regardless.
    let mut sm = drained_with_anchor(None);
    assert_eq!(sm.node_horizon(nid(1)), NodeHorizon::Unbounded);
    // D = 3600 → T_notice = 3240.
    deadline_drain(&mut sm, 0, 3_600).unwrap();

    // 30 + 3180 + 30 = 3240 ≤ T_notice: admitted, recording T_notice.
    submit(&mut sm, 2, 1_000, Some(3_180));
    place(&mut sm, 2, 1_000, at(30)).expect("bounded backfill by T_notice");
    assert_eq!(sm.attempts[&aid(22)].drain_cutoff, Some(at(3_240)));
    // One second longer overshoots T_notice.
    submit(&mut sm, 3, 1_000, Some(3_181));
    assert_eq!(place(&mut sm, 3, 1_000, at(30)), Err(draining_rejection(0)));
    // An unbounded job is never backfill.
    submit(&mut sm, 4, 1_000, None);
    assert_eq!(place(&mut sm, 4, 1_000, at(30)), Err(draining_rejection(0)));
    // At T_notice admission is closed by construction.
    submit(&mut sm, 5, 1_000, Some(1));
    assert_eq!(
        place(&mut sm, 5, 1_000, at(3_240)),
        Err(draining_rejection(0))
    );
}

#[test]
fn an_empty_node_under_a_deadline_drain_still_admits_backfill() {
    let mut sm = setup();
    deadline_drain(&mut sm, 0, 3_600).unwrap();
    submit(&mut sm, 2, 1_000, Some(60));
    place(&mut sm, 2, 1_000, at(30)).expect("an empty node is no reason to refuse before T_notice");
    assert_eq!(sm.attempts[&aid(22)].drain_cutoff, Some(at(3_240)));
}

/// A start the agent refuses under its drain plan — at the door after
/// `T_notice`, or mid-pull once the plan's `T_stop` passed — never ran, and
/// resolves `Preempted { Drain }` from `Dispatching` with the free retry.
#[test]
fn a_start_refused_under_a_drain_plan_resolves_preempted_as_a_free_retry() {
    let mut sm = setup();
    submit(&mut sm, 2, 1_000, None);
    place(&mut sm, 2, 1_000, at(0)).unwrap();
    apply_ok(&mut sm, dispatch_cmd(aid(22), at(1)));
    let outcome = AttemptOutcome::Preempted {
        reason: PreemptReason::Drain,
    };
    apply_ok(&mut sm, outcome_cmd(aid(22), outcome.clone(), 0, at(2)));
    assert_eq!(
        sm.attempts[&aid(22)].attempt.state,
        AttemptState::Terminal(outcome)
    );
    let job = &sm.jobs[&jid(2)];
    assert_eq!(job.state, coppice_core::job::JobState::Queued);
    assert_eq!(job.retries_used, 0, "a drain preemption is free");
}
