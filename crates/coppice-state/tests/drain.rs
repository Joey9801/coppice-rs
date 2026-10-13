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
