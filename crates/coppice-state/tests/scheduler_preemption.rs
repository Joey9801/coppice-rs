//! ADR 0044 §4 at apply: a preempting placement's eviction group. Every
//! victim is re-validated (a refusal is a proposer bug, rejecting the whole
//! batch), the beneficiary is committed accruing, and each victim's attempt
//! gets an unacknowledged `Priority` termination record and a
//! `PreemptRequested` event.

mod common;

use common::*;
use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptState, PreemptReason, Preemption};
use coppice_core::id::{AllocationId, JobId, NodeId};
use coppice_core::job::{JobState, RetryPolicy};
use coppice_core::preemption::PreemptionSpec;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{CommitPlacements, Placement, RecordAttemptNotified};
use coppice_state::{Command, Event, PreemptionRefusal, RejectionReason, StateMachine};

/// The beneficiary every test places.
const BENEFICIARY: u128 = 100;

fn secs(s: i64) -> Timestamp {
    base_ts() + Duration::from_secs(s)
}

fn submit(sm: &mut StateMachine, job: JobId, millis: u64, priority: i32, preemptible: bool) {
    submit_with_runtime(sm, job, millis, priority, preemptible, None);
}

fn submit_with_runtime(
    sm: &mut StateMachine,
    job: JobId,
    millis: u64,
    priority: i32,
    preemptible: bool,
    max_runtime_s: Option<i64>,
) {
    let mut cmd = submit_cmd_full(
        job,
        cpu(millis),
        max_runtime_s,
        RetryPolicy::default(),
        Default::default(),
        PreemptionSpec {
            preemptible,
            ..Default::default()
        },
    );
    if let Command::SubmitJob(c) = &mut cmd {
        c.job.priority = priority;
    }
    apply_ok(sm, cmd);
}

/// Place job `n` (attempt `aid(n)`, allocation `alid(n)`) on `node` and run
/// it.
fn run(sm: &mut StateMachine, n: u128, node: NodeId, millis: u64) {
    apply_ok(
        sm,
        place_cmd(
            placement(jid(n), aid(n), alid(n), node, cpu(millis)),
            base_ts(),
        ),
    );
    apply_ok(sm, dispatch_cmd(aid(n), base_ts()));
    apply_ok(sm, started_cmd(aid(n), base_ts()));
}

/// Node 1 (10 cores) fully used by two running, preemptible, class-0 jobs —
/// 6 cores (`jid(1)`) and 4 (`jid(2)`) — and an 8-core, class-5 beneficiary
/// queued with nowhere to go.
fn full_node() -> StateMachine {
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 0, true);
    submit(&mut sm, jid(2), 4_000, 0, true);
    run(&mut sm, 1, nid(1), 6_000);
    run(&mut sm, 2, nid(1), 4_000);
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    sm
}

fn preempting(victims: &[u128]) -> Placement {
    preempting_on(nid(1), victims)
}

fn preempting_on(node: NodeId, victims: &[u128]) -> Placement {
    let mut p = placement(
        jid(BENEFICIARY),
        aid(BENEFICIARY),
        alid(BENEFICIARY),
        node,
        cpu(8_000),
    );
    p.preemptions = victims.iter().map(|n| alid(*n)).collect();
    p
}

fn commit(sm: &mut StateMachine, p: Placement) -> Result<(), RejectionReason> {
    sm.apply(&place_cmd(p, secs(10))).map(|_| ())
}

/// The single per-item refusal a rejected batch carries.
fn refusal(err: RejectionReason) -> (AllocationId, PreemptionRefusal) {
    match batch_reasons(&err).as_slice() {
        [RejectionReason::InvalidPreemption {
            allocation,
            refusal,
        }] => (*allocation, *refusal),
        other => panic!("expected one InvalidPreemption, got {other:?}"),
    }
}

fn batch_reasons(reason: &RejectionReason) -> Vec<RejectionReason> {
    match reason {
        RejectionReason::InvalidBatch(items) => {
            items.iter().map(|item| item.reason.clone()).collect()
        }
        other => vec![other.clone()],
    }
}

#[test]
fn an_eviction_group_records_each_victim_and_commits_the_beneficiary_accruing() {
    let mut sm = full_node();
    let applied = apply_ok(&mut sm, place_cmd(preempting(&[1, 2]), secs(10)));

    let reason = PreemptReason::Priority {
        for_job: jid(BENEFICIARY),
    };
    for n in [1, 2] {
        assert_eq!(
            sm.attempts[&aid(n)].preemption,
            Some(Preemption {
                reason,
                requested_at: secs(10),
                acknowledged: None,
            }),
            "victim {n}"
        );
        // Still running and still holding its capacity: there is no
        // allocation phase for a preemption.
        assert_eq!(sm.attempts[&aid(n)].attempt.state, AttemptState::Running);
        assert_eq!(
            sm.allocations[&alid(n)].allocation.state,
            AllocationState::Active
        );
    }
    // Nothing was free, so the beneficiary accrues and waits for the release.
    assert_eq!(
        sm.allocations[&alid(BENEFICIARY)].allocation.state,
        AllocationState::Accruing
    );
    assert_eq!(
        sm.jobs[&jid(BENEFICIARY)].state,
        JobState::Attempting(aid(BENEFICIARY))
    );
    let requested: Vec<&Event> = applied
        .events
        .iter()
        .filter(|e| matches!(e, Event::PreemptRequested { .. }))
        .collect();
    assert_eq!(
        requested,
        vec![
            &Event::PreemptRequested {
                attempt: aid(1),
                allocation: alid(1),
                job: jid(1),
                node: nid(1),
                reason,
                notice: sm.policy.preemption_notice_floor,
            },
            &Event::PreemptRequested {
                attempt: aid(2),
                allocation: alid(2),
                job: jid(2),
                node: nid(1),
                reason,
                notice: sm.policy.preemption_notice_floor,
            },
        ]
    );
}

#[test]
fn the_victims_release_funds_the_beneficiary() {
    let mut sm = full_node();
    apply_ok(&mut sm, place_cmd(preempting(&[1, 2]), secs(10)));
    for n in [1, 2] {
        apply_ok(
            &mut sm,
            outcome_cmd(
                aid(n),
                coppice_core::attempt::AttemptOutcome::Preempted {
                    reason: PreemptReason::Priority {
                        for_job: jid(BENEFICIARY),
                    },
                },
                70,
                secs(80),
            ),
        );
    }
    assert_eq!(
        sm.allocations[&alid(BENEFICIARY)].allocation.state,
        AllocationState::Funded
    );
    // Each victim requeued free.
    for n in [1, 2] {
        assert_eq!(sm.jobs[&jid(n)].state, JobState::Queued);
        assert_eq!(sm.jobs[&jid(n)].retries_used, 0);
    }
}

#[test]
fn a_group_too_small_to_fit_the_beneficiary_is_refused() {
    let mut sm = full_node();
    let err = commit(&mut sm, preempting(&[2])).unwrap_err();
    assert_eq!(refusal(err), (alid(2), PreemptionRefusal::DoesNotFit));
    assert!(sm.attempts[&aid(2)].preemption.is_none(), "nothing applied");
}

#[test]
fn another_guaranteed_release_inside_the_notice_bound_counts_toward_the_fit() {
    // 4 cores of the node end at 30 s (a bounded job), inside the 60 s notice
    // + 30 s grace bound, so preempting the 6-core job alone suffices.
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 0, true);
    submit_with_runtime(&mut sm, jid(2), 4_000, 0, false, Some(30));
    run(&mut sm, 1, nid(1), 6_000);
    run(&mut sm, 2, nid(1), 4_000);
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    commit(&mut sm, preempting(&[1])).expect("the bounded job's release completes the fit");

    // The same release past the bound does not count.
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 0, true);
    submit_with_runtime(&mut sm, jid(2), 4_000, 0, false, Some(3_600));
    run(&mut sm, 1, nid(1), 6_000);
    run(&mut sm, 2, nid(1), 4_000);
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    let err = commit(&mut sm, preempting(&[1])).unwrap_err();
    assert_eq!(refusal(err), (alid(1), PreemptionRefusal::DoesNotFit));
}

#[test]
fn the_class_rule_is_re_checked() {
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 5, true);
    submit(&mut sm, jid(2), 4_000, 0, true);
    run(&mut sm, 1, nid(1), 6_000);
    run(&mut sm, 2, nid(1), 4_000);
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    let err = commit(&mut sm, preempting(&[1, 2])).unwrap_err();
    assert_eq!(refusal(err), (alid(1), PreemptionRefusal::NotOutranked));
}

#[test]
fn a_victim_must_have_been_placed_preemptible() {
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 0, false);
    submit(&mut sm, jid(2), 4_000, 0, true);
    run(&mut sm, 1, nid(1), 6_000);
    run(&mut sm, 2, nid(1), 4_000);
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    let err = commit(&mut sm, preempting(&[1, 2])).unwrap_err();
    assert_eq!(refusal(err), (alid(1), PreemptionRefusal::NotPreemptible));
}

#[test]
fn a_victim_must_be_running() {
    let mut sm = setup();
    submit(&mut sm, jid(1), 6_000, 0, true);
    submit(&mut sm, jid(2), 4_000, 0, true);
    run(&mut sm, 1, nid(1), 6_000);
    // Dispatched, not yet observed running: its allocation is funded, not
    // active.
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(2), alid(2), nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(2), base_ts()));
    submit(&mut sm, jid(BENEFICIARY), 8_000, 5, false);
    let err = commit(&mut sm, preempting(&[1, 2])).unwrap_err();
    assert_eq!(refusal(err), (alid(2), PreemptionRefusal::NotActive));
}

#[test]
fn a_victim_already_terminating_is_refused() {
    let mut sm = full_node();
    apply_ok(
        &mut sm,
        Command::RecordAttemptNotified(RecordAttemptNotified {
            attempt: aid(1),
            reason: PreemptReason::Interruption,
            remaining: Duration::from_secs(60),
            shortfall: Duration::ZERO,
            observed_at: secs(5),
        }),
    );
    let err = commit(&mut sm, preempting(&[1, 2])).unwrap_err();
    assert_eq!(
        refusal(err),
        (alid(1), PreemptionRefusal::AlreadyTerminating)
    );
}

#[test]
fn a_victim_on_another_node_is_refused() {
    let mut sm = full_node();
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(10_000), base_ts()));
    submit(&mut sm, jid(3), 4_000, 0, true);
    run(&mut sm, 3, nid(2), 4_000);
    let err = commit(&mut sm, preempting(&[1, 3])).unwrap_err();
    assert_eq!(refusal(err), (alid(3), PreemptionRefusal::WrongNode));
}

#[test]
fn a_victim_named_twice_is_refused() {
    let mut sm = full_node();
    let err = commit(&mut sm, preempting(&[1, 1, 2])).unwrap_err();
    assert_eq!(refusal(err), (alid(1), PreemptionRefusal::Duplicate));
}

#[test]
fn an_unknown_victim_is_refused() {
    let mut sm = full_node();
    let err = commit(&mut sm, preempting(&[1, 99])).unwrap_err();
    assert_eq!(
        batch_reasons(&err),
        vec![RejectionReason::UnknownAllocation(alid(99))]
    );
}

#[test]
fn no_victim_on_a_draining_node() {
    let mut sm = full_node();
    apply_ok(&mut sm, set_draining_cmd(nid(1), true));
    let err = commit(&mut sm, preempting(&[1, 2])).unwrap_err();
    // The ordinary admission gate answers first: a draining node takes no
    // ordinary placement at all.
    assert!(
        batch_reasons(&err)
            .iter()
            .any(|r| matches!(r, RejectionReason::NodeDraining(n) if *n == nid(1))),
        "{err}"
    );
}

#[test]
fn the_per_node_guard_holds_while_victims_are_outstanding() {
    let mut sm = full_node();
    submit(&mut sm, jid(3), 2_000, 0, true);
    apply_ok(&mut sm, place_cmd(preempting(&[1, 2]), secs(10)));

    // A second eviction group on the same node while the first is
    // outstanding: refused, even naming a fresh victim.
    submit(&mut sm, jid(200), 1_000, 9, false);
    let mut p = placement(jid(200), aid(200), alid(200), nid(1), cpu(1_000));
    p.preemptions = vec![alid(1)];
    let err = sm.apply(&place_cmd(p, secs(20))).unwrap_err();
    assert_eq!(refusal(err), (alid(1), PreemptionRefusal::Guarded));

    // Once the victims have ended, the guard lifts.
    for n in [1, 2] {
        apply_ok(
            &mut sm,
            outcome_cmd(
                aid(n),
                coppice_core::attempt::AttemptOutcome::Preempted {
                    reason: PreemptReason::Priority {
                        for_job: jid(BENEFICIARY),
                    },
                },
                70,
                secs(80),
            ),
        );
    }
    assert!(sm.preemption_ledger().guarded.is_empty());
    assert_eq!(
        sm.preemption_ledger().latest_request.get(&nid(1)),
        Some(&secs(10)),
        "the cooldown clock outlives the guard"
    );
}

#[test]
fn one_eviction_group_per_node_per_batch() {
    let mut sm = full_node();
    submit(&mut sm, jid(200), 1_000, 9, false);
    let mut other = placement(jid(200), aid(200), alid(200), nid(1), cpu(1_000));
    other.preemptions = vec![alid(2)];
    let err = sm
        .apply(&Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![],
            placements: vec![preempting(&[1]), other],
            proposed_at: secs(10),
        }))
        .unwrap_err();
    let reasons = batch_reasons(&err);
    assert!(
        reasons.contains(&RejectionReason::InvalidPreemption {
            allocation: alid(2),
            refusal: PreemptionRefusal::Guarded,
        }),
        "{reasons:?}"
    );
}

#[test]
fn a_beneficiary_already_accruing_may_be_reseated_with_an_eviction_group() {
    // The beneficiary already accrues on node 1 (revoke-and-reseat): its old
    // accrual is revoked in the same batch and does not count against the
    // fit.
    let mut sm = full_node();
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(BENEFICIARY), aid(50), alid(50), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    assert_eq!(
        sm.allocations[&alid(50)].allocation.state,
        AllocationState::Accruing
    );
    apply_ok(
        &mut sm,
        Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![alid(50)],
            placements: vec![preempting(&[1, 2])],
            proposed_at: secs(10),
        }),
    );
    assert!(sm.attempts[&aid(1)].preemption.is_some());
    assert_eq!(
        sm.jobs[&jid(BENEFICIARY)].state,
        JobState::Attempting(aid(BENEFICIARY))
    );
}
