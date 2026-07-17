//! Scripted lifecycle tests: every command in the catalog exercised through
//! realistic sequences, asserting the apply contract of
//! `docs/architecture/command-catalog.md` — resolution rules, funding order,
//! rejection-as-deterministic-no-op, and quota charging.

mod common;

use common::*;
use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::GroupId;
use coppice_core::job::{JobState, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier, FULL_REFUND_MILLI};
use coppice_core::time::Duration;
use coppice_state::command::{
    BumpClusterVersion, CommitPlacements, ConfigureQuotaEntity, DeclareNodeLost, EvictTerminalJobs,
    LostAttempt, ReconcileNode, SetNodeSchedulable,
};
use coppice_state::{Command, Event, Rejection, RejectionReason};

#[test]
fn happy_path_submit_to_eviction() {
    let mut sm = setup();
    let (job, attempt, alloc) = (jid(1), aid(11), alid(111));

    apply_ok(
        &mut sm,
        submit_cmd(job, cpu(4_000), Some(3_600), RetryPolicy::default()),
    );
    assert_eq!(sm.jobs[&job].state, JobState::Queued);

    let usage_before = sm.quota_entities[&ROOT].usage.usage;
    apply_ok(
        &mut sm,
        place_cmd(
            placement(job, attempt, alloc, nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    // Capacity was immediately available: accrual skipped, charge landed. The
    // job now carries the exact attempt id it is pursuing (ADR 0030).
    assert_eq!(sm.jobs[&job].state, JobState::Attempting(attempt));
    assert_eq!(sm.jobs[&job].current_attempt(), Some(attempt));
    assert_eq!(sm.attempts[&attempt].attempt.state, AttemptState::Ready);
    assert_eq!(
        sm.allocations[&alloc].allocation.state,
        AllocationState::Funded
    );
    let usage_charged = sm.quota_entities[&ROOT].usage.usage;
    assert!(
        usage_charged > usage_before,
        "placement must charge the quota entity"
    );

    apply_ok(&mut sm, dispatch_cmd(attempt, ts(TS_US + 1)));
    assert_eq!(
        sm.attempts[&attempt].attempt.state,
        AttemptState::Dispatching
    );

    apply_ok(&mut sm, started_cmd(attempt, ts(TS_US + 2)));
    // The job stays Attempting while the attempt runs — no Running mirror.
    assert_eq!(sm.jobs[&job].state, JobState::Attempting(attempt));
    assert_eq!(sm.attempts[&attempt].attempt.state, AttemptState::Running);
    assert_eq!(
        sm.allocations[&alloc].allocation.state,
        AllocationState::Active
    );

    apply_ok(&mut sm, exited_cmd(attempt, ts(TS_US + 60_000_000)));
    // Exit observed but outcome not yet recorded: the attempt is Finalizing
    // while the job stays Attempting (ADR 0030 — no job-level Finalizing).
    assert_eq!(sm.jobs[&job].state, JobState::Attempting(attempt));
    assert_eq!(
        sm.attempts[&attempt].attempt.state,
        AttemptState::Finalizing
    );

    apply_ok(
        &mut sm,
        outcome_cmd(
            attempt,
            AttemptOutcome::Exited { code: 0 },
            60,
            ts(TS_US + 60_000_000),
        ),
    );
    assert_eq!(sm.jobs[&job].state, JobState::Succeeded);
    assert_eq!(
        sm.allocations[&alloc].allocation.state,
        AllocationState::Released
    );
    // Ran 60 s of the declared 3600: true-up refunds the unused charge.
    assert!(sm.quota_entities[&ROOT].usage.usage < usage_charged);

    apply_ok(
        &mut sm,
        Command::EvictTerminalJobs(EvictTerminalJobs {
            jobs: vec![job],
            evicted_at: ts(TS_US + 1_000),
        }),
    );
    assert!(sm.jobs.is_empty() && sm.attempts.is_empty() && sm.allocations.is_empty());
}

#[test]
fn whale_accrues_then_funds_when_capacity_frees() {
    let mut sm = setup();
    // Filler occupies 8 of 10 cores and runs.
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));

    // The whale needs the whole node: partial funding, attempt accrues.
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Accruing);
    assert_eq!(sm.allocations[&alid(222)].allocation.funded, cpu(2_000));
    assert_eq!(sm.accrual_queue.len(), 1);
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Attempting(aid(22)));

    // Filler finishes: freed capacity pledges to the whale, Ready flips.
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            60,
            ts(TS_US + 1),
        ),
    );
    assert_eq!(
        sm.allocations[&alid(222)].allocation.state,
        AllocationState::Funded
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Ready);
    assert!(sm.accrual_queue.is_empty());
}

#[test]
fn funding_follows_commit_order_not_id_order() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));

    // First-committed whale gets a *larger* AllocationId than the second, so
    // id-ordered funding would fund the wrong one.
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(6_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(0xFF), nid(1), cpu(6_000)),
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        submit_cmd(jid(3), cpu(6_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(3), aid(33), alid(0x01), nid(1), cpu(6_000)),
            base_ts(),
        ),
    );

    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            60,
            ts(TS_US + 1),
        ),
    );
    // 10 cores freed: first-committed (seq order) fully funds, the later one
    // takes the remainder.
    assert_eq!(
        sm.allocations[&alid(0xFF)].allocation.state,
        AllocationState::Funded
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Ready);
    assert_eq!(
        sm.allocations[&alid(0x01)].allocation.state,
        AllocationState::Accruing
    );
    assert_eq!(sm.allocations[&alid(0x01)].allocation.funded, cpu(4_000));
}

#[test]
fn abort_with_no_live_attempt_is_immediate() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    apply_ok(&mut sm, abort_cmd(jid(1), base_ts()));
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Aborted);
    assert!(sm.jobs[&jid(1)].spec.abort_requested.is_some());
}

#[test]
fn abort_while_accruing_releases_and_refunds() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    let usage_before_whale = sm.quota_entities[&ROOT].usage.usage;

    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, abort_cmd(jid(2), base_ts()));

    assert_eq!(sm.jobs[&jid(2)].state, JobState::Aborted);
    assert_eq!(
        sm.attempts[&aid(22)].attempt.state,
        AttemptState::Terminal(AttemptOutcome::Aborted)
    );
    assert_eq!(
        sm.allocations[&alid(222)].allocation.state,
        AllocationState::Released
    );
    assert!(sm.accrual_queue.is_empty());
    // Same-tick charge and full refund: usage lands exactly where it was.
    assert_eq!(sm.quota_entities[&ROOT].usage.usage, usage_before_whale);
}

#[test]
fn abort_while_running_signals_stop_and_truth_wins() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));

    let applied = apply_ok(&mut sm, abort_cmd(jid(1), ts(TS_US + 1)));
    assert!(applied
        .events
        .iter()
        .any(|e| matches!(e, Event::StopRequested { allocation, .. } if *allocation == alid(111))));
    // The attempt is in the agent's hands; state is unchanged until the
    // outcome arrives — the job stays Attempting.
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Attempting(aid(11)));

    // The container exited naturally before the stop landed: truth wins.
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            30,
            ts(TS_US + 2),
        ),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Succeeded);
    assert!(sm.jobs[&jid(1)].spec.abort_requested.is_some());
}

#[test]
fn abort_wins_over_retry() {
    let mut sm = setup();
    // Platform failures normally retry; a pending abort must block that.
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, abort_cmd(jid(1), ts(TS_US + 1)));

    apply_ok(
        &mut sm,
        outcome_cmd(aid(11), AttemptOutcome::AgentError, 30, ts(TS_US + 2)),
    );
    // Not requeued (abort wins over retry) and not Aborted (the abort
    // mechanism didn't stop it — the agent failure did).
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Failed);
}

#[test]
fn abort_confirmed_by_agent_ends_aborted() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, abort_cmd(jid(1), ts(TS_US + 1)));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(11), AttemptOutcome::Aborted, 30, ts(TS_US + 2)),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Aborted);
}

#[test]
fn platform_failures_retry_until_budget_exhausted() {
    let mut sm = setup();
    let retry = RetryPolicy {
        max_retries: 1,
        retry_user_errors: false,
    };
    apply_ok(&mut sm, submit_cmd(jid(1), cpu(1_000), Some(3_600), retry));

    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::StartFailed { user_error: false },
            0,
            base_ts(),
        ),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Queued);
    assert_eq!(sm.jobs[&jid(1)].retries_used, 1);

    // Retry mints a fresh attempt via a new placement.
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(12), alid(112), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(12), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(12),
            AttemptOutcome::StartFailed { user_error: false },
            0,
            base_ts(),
        ),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Failed);
}

#[test]
fn user_errors_do_not_retry_unless_opted_in() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(11), AttemptOutcome::Exited { code: 1 }, 30, base_ts()),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Failed);

    let opt_in = RetryPolicy {
        max_retries: 3,
        retry_user_errors: true,
    };
    apply_ok(&mut sm, submit_cmd(jid(2), cpu(1_000), Some(3_600), opt_in));
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(22), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(22), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(22), AttemptOutcome::MemoryLimitExceeded, 30, base_ts()),
    );
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Queued);
}

#[test]
fn runtime_limit_exceeded_retries_like_the_other_limit_breaches() {
    let mut sm = setup();
    // Default policy: no user-error retries — the breach is terminal.
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(60), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(11), AttemptOutcome::RuntimeLimitExceeded, 90, base_ts()),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Failed);

    // Opted in, the runtime breach requeues exactly like a memory or disk
    // breach (ADR 0033: one classification for all three).
    let opt_in = RetryPolicy {
        max_retries: 3,
        retry_user_errors: true,
    };
    apply_ok(&mut sm, submit_cmd(jid(2), cpu(1_000), Some(60), opt_in));
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(22), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(22), base_ts()));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(22), AttemptOutcome::RuntimeLimitExceeded, 90, base_ts()),
    );
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Queued);
    assert_eq!(sm.jobs[&jid(2)].retries_used, 1);
}

#[test]
fn revocation_requeues_free_of_retry_budget() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    let usage_before = sm.quota_entities[&ROOT].usage.usage;

    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    // While the attempt is in flight the job carries its id.
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Attempting(aid(22)));
    assert_eq!(sm.jobs[&jid(2)].current_attempt(), Some(aid(22)));

    apply_ok(
        &mut sm,
        Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![alid(222)],
            placements: vec![],
            proposed_at: base_ts(),
        }),
    );
    // Requeue drops the id: the transition to Queued is the only bookkeeping.
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Queued);
    assert_eq!(sm.jobs[&jid(2)].current_attempt(), None);
    assert_eq!(
        sm.jobs[&jid(2)].retries_used,
        0,
        "revocation must not consume retry budget"
    );
    assert_eq!(
        sm.attempts[&aid(22)].attempt.state,
        AttemptState::Terminal(AttemptOutcome::Revoked)
    );
    // Same-tick full refund: requeue is free.
    assert_eq!(sm.quota_entities[&ROOT].usage.usage, usage_before);
}

#[test]
fn revoke_and_reseat_in_one_batch() {
    let mut sm = setup();
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(16_000), base_ts()));
    // Whale accrues on the full node 1.
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Accruing);

    // The re-plan: node 2 freed up first — revoke the accrual and seat the
    // whale there, atomically.
    apply_ok(
        &mut sm,
        Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![alid(222)],
            placements: vec![placement(jid(2), aid(23), alid(223), nid(2), cpu(10_000))],
            proposed_at: ts(TS_US + 1),
        }),
    );
    // The job passed through Queued (the revocation resolved it) and lands on
    // the fresh attempt — never an Attempting → Attempting edge (ADR 0030).
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Attempting(aid(23)));
    assert_eq!(sm.attempts[&aid(23)].attempt.state, AttemptState::Ready);
    assert!(sm.accrual_queue.is_empty());
}

#[test]
fn accrual_limit_bounds_concurrent_whales() {
    let mut sm = setup();
    apply_ok(&mut sm, update_policy_cmd(test_policy(1)));
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(9_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(9_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));

    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Accruing);

    apply_ok(
        &mut sm,
        submit_cmd(jid(3), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    let rejection = sm
        .apply(&place_cmd(
            placement(jid(3), aid(33), alid(333), nid(1), cpu(10_000)),
            base_ts(),
        ))
        .unwrap_err();
    assert_eq!(
        rejection,
        RejectionReason::AccrualLimitExceeded { limit: 1 }
    );
    assert_eq!(
        sm.jobs[&jid(3)].state,
        JobState::Queued,
        "rejected batch must have no effects"
    );
}

#[test]
fn reconcile_adopts_and_declares_lost() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(22), base_ts()));

    // A stale-epoch set is worthless and rejected whole.
    let stale = sm
        .apply(&Command::ReconcileNode(ReconcileNode {
            node: nid(1),
            node_epoch: 99,
            adopted: vec![aid(11)],
            lost: vec![],
            observed_at: ts(TS_US + 1),
        }))
        .unwrap_err();
    assert!(matches!(stale, RejectionReason::StaleNodeEpoch { .. }));

    let epoch = sm.nodes[&nid(1)].epoch;
    apply_ok(
        &mut sm,
        Command::ReconcileNode(ReconcileNode {
            node: nid(1),
            node_epoch: epoch,
            adopted: vec![aid(11)],
            lost: vec![LostAttempt {
                attempt: aid(22),
                outcome: AttemptOutcome::AgentError,
                actual_runtime: Duration::ZERO,
            }],
            observed_at: ts(TS_US + 1),
        }),
    );
    // Adopted: the missed started report is folded in. The job stays
    // Attempting; only the attempt advances to Running.
    assert_eq!(sm.attempts[&aid(11)].attempt.state, AttemptState::Running);
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Attempting(aid(11)));
    // Lost: platform failure, retry policy applies.
    assert_eq!(sm.jobs[&jid(2)].state, JobState::Queued);
    assert_eq!(sm.jobs[&jid(2)].retries_used, 1);
}

#[test]
fn node_lost_fails_attempts_and_fences() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    let epoch_before = sm.nodes[&nid(1)].epoch;

    apply_ok(
        &mut sm,
        Command::DeclareNodeLost(DeclareNodeLost {
            node: nid(1),
            declared_at: ts(TS_US + 60_000_000),
        }),
    );
    assert_eq!(sm.nodes[&nid(1)].epoch, epoch_before + 1);
    assert!(!sm.nodes[&nid(1)].node.schedulable);
    assert_eq!(
        sm.attempts[&aid(11)].attempt.state,
        AttemptState::Terminal(AttemptOutcome::NodeLost)
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Queued);
    assert!(sm.accrual_queue.is_empty());
}

#[test]
fn reregistration_bumps_epoch_and_preserves_drain() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        Command::SetNodeSchedulable(SetNodeSchedulable {
            node: nid(1),
            schedulable: false,
            updated_at: base_ts(),
        }),
    );
    let epoch_before = sm.nodes[&nid(1)].epoch;
    apply_ok(
        &mut sm,
        register_node_cmd(nid(1), cpu(20_000), ts(TS_US + 1)),
    );
    assert_eq!(sm.nodes[&nid(1)].epoch, epoch_before + 1);
    assert_eq!(sm.nodes[&nid(1)].node.capacity, cpu(20_000));
    assert!(
        !sm.nodes[&nid(1)].node.schedulable,
        "an agent restart must not undo a drain"
    );
}

#[test]
fn drained_node_rejects_placements_but_keeps_funding() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );

    apply_ok(
        &mut sm,
        Command::SetNodeSchedulable(SetNodeSchedulable {
            node: nid(1),
            schedulable: false,
            updated_at: base_ts(),
        }),
    );
    apply_ok(
        &mut sm,
        submit_cmd(jid(3), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    let rejection = sm
        .apply(&place_cmd(
            placement(jid(3), aid(33), alid(333), nid(1), cpu(1_000)),
            base_ts(),
        ))
        .unwrap_err();
    assert_eq!(
        rejection,
        RejectionReason::InvalidBatch(vec![Rejection {
            item_index: 0,
            reason: RejectionReason::NodeNotSchedulable(nid(1))
        }])
    );

    // Existing accrual keeps funding on the drained node.
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            30,
            ts(TS_US + 1),
        ),
    );
    assert_eq!(
        sm.allocations[&alid(222)].allocation.state,
        AllocationState::Funded
    );
}

#[test]
fn eviction_rejects_live_jobs_and_skips_missing() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    let rejection = sm
        .apply(&Command::EvictTerminalJobs(EvictTerminalJobs {
            jobs: vec![jid(1)],
            evicted_at: base_ts(),
        }))
        .unwrap_err();
    assert_eq!(
        rejection,
        RejectionReason::InvalidBatch(vec![Rejection {
            item_index: 0,
            reason: RejectionReason::JobNotTerminal(jid(1))
        }])
    );

    // Missing ids skip silently: duplicate eviction proposals are idempotent.
    apply_ok(&mut sm, abort_cmd(jid(1), base_ts()));
    let evict = Command::EvictTerminalJobs(EvictTerminalJobs {
        jobs: vec![jid(1), jid(999)],
        evicted_at: base_ts(),
    });
    apply_ok(&mut sm, evict.clone());
    apply_ok(&mut sm, evict);
    assert!(sm.jobs.is_empty());
}

#[test]
fn terminal_timestamp_is_stamped_by_the_resolving_command() {
    let mut sm = setup();

    // Immediate abort of a queued job: stamped from the abort's
    // `requested_at`.
    let queued = jid(1);
    apply_ok(
        &mut sm,
        submit_cmd(queued, cpu(1_000), None, RetryPolicy::default()),
    );
    assert_eq!(sm.jobs[&queued].terminal_at, None);
    apply_ok(&mut sm, abort_cmd(queued, ts(TS_US + 10)));
    assert_eq!(sm.jobs[&queued].state, JobState::Aborted);
    assert_eq!(sm.jobs[&queued].terminal_at, Some(ts(TS_US + 10)));

    // Normal outcome: stamped from the outcome report's `observed_at`.
    let ran = jid(2);
    apply_ok(
        &mut sm,
        submit_cmd(ran, cpu(1_000), Some(60), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(ran, aid(21), alid(210), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(21), ts(TS_US + 1)));
    apply_ok(&mut sm, started_cmd(aid(21), ts(TS_US + 2)));
    let done_at = ts(TS_US + 60_000_000);
    apply_ok(
        &mut sm,
        outcome_cmd(aid(21), AttemptOutcome::Exited { code: 0 }, 60, done_at),
    );
    assert_eq!(sm.jobs[&ran].state, JobState::Succeeded);
    assert_eq!(sm.jobs[&ran].terminal_at, Some(done_at));

    // A requeue is not terminal: the field stays `None` through the retry
    // and carries the *final* resolution's time, not the first failure's.
    let retried = jid(3);
    let one_retry = RetryPolicy {
        max_retries: 1,
        retry_user_errors: false,
    };
    apply_ok(
        &mut sm,
        submit_cmd(retried, cpu(1_000), Some(60), one_retry),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(retried, aid(31), alid(310), nid(1), cpu(1_000)),
            ts(TS_US + 20),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(31), ts(TS_US + 21)));
    apply_ok(&mut sm, started_cmd(aid(31), ts(TS_US + 22)));
    apply_ok(
        &mut sm,
        outcome_cmd(aid(31), AttemptOutcome::AgentError, 1, ts(TS_US + 30)),
    );
    assert_eq!(sm.jobs[&retried].state, JobState::Queued);
    assert_eq!(sm.jobs[&retried].terminal_at, None);
    apply_ok(
        &mut sm,
        place_cmd(
            placement(retried, aid(32), alid(320), nid(1), cpu(1_000)),
            ts(TS_US + 40),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(32), ts(TS_US + 41)));
    apply_ok(&mut sm, started_cmd(aid(32), ts(TS_US + 42)));
    let failed_at = ts(TS_US + 50);
    apply_ok(
        &mut sm,
        outcome_cmd(aid(32), AttemptOutcome::AgentError, 1, failed_at),
    );
    assert_eq!(sm.jobs[&retried].state, JobState::Failed);
    assert_eq!(sm.jobs[&retried].terminal_at, Some(failed_at));
}

#[test]
fn reconcile_and_node_loss_stamp_the_terminal_timestamp() {
    let mut sm = setup();
    let no_retry = RetryPolicy {
        max_retries: 0,
        retry_user_errors: false,
    };

    // Reconcile-lost: stamped from the reconcile report's `observed_at`.
    let reconciled = jid(1);
    apply_ok(
        &mut sm,
        submit_cmd(reconciled, cpu(1_000), Some(60), no_retry),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(reconciled, aid(11), alid(110), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), ts(TS_US + 1)));
    let observed_at = ts(TS_US + 90);
    apply_ok(
        &mut sm,
        Command::ReconcileNode(ReconcileNode {
            node: nid(1),
            node_epoch: 1,
            adopted: vec![],
            lost: vec![LostAttempt {
                attempt: aid(11),
                outcome: AttemptOutcome::AgentError,
                actual_runtime: Duration::ZERO,
            }],
            observed_at,
        }),
    );
    assert_eq!(sm.jobs[&reconciled].state, JobState::Failed);
    assert_eq!(sm.jobs[&reconciled].terminal_at, Some(observed_at));

    // Node loss: stamped from `declared_at`.
    let stranded = jid(2);
    apply_ok(
        &mut sm,
        submit_cmd(stranded, cpu(1_000), Some(60), no_retry),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(stranded, aid(21), alid(210), nid(1), cpu(1_000)),
            ts(TS_US + 100),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(21), ts(TS_US + 101)));
    apply_ok(&mut sm, started_cmd(aid(21), ts(TS_US + 102)));
    let declared_at = ts(TS_US + 200);
    apply_ok(
        &mut sm,
        Command::DeclareNodeLost(DeclareNodeLost {
            node: nid(1),
            declared_at,
        }),
    );
    assert_eq!(sm.jobs[&stranded].state, JobState::Failed);
    assert_eq!(sm.jobs[&stranded].terminal_at, Some(declared_at));
}

#[test]
fn quota_entity_updates_preserve_usage_and_reject_cycles() {
    let mut sm = setup();
    apply_ok(&mut sm, configure_entity_cmd(qid(2), Some(ROOT)));
    apply_ok(&mut sm, configure_entity_cmd(qid(3), Some(qid(2))));
    // Re-parenting the root under its grandchild would cycle.
    let rejection = sm
        .apply(&configure_entity_cmd(ROOT, Some(qid(3))))
        .unwrap_err();
    assert_eq!(rejection, RejectionReason::QuotaEntityCycle(ROOT));

    // Updates keep the accumulator: reconfiguration is not an amnesty.
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(4_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    let usage = sm.quota_entities[&ROOT].usage;
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    assert_eq!(sm.quota_entities[&ROOT].usage, usage);
}

#[test]
fn quota_entity_timestamps_stamp_on_create_and_advance_only_updated() {
    let mut sm = setup();
    let create_at = ts(1_000_000);
    let update_at = ts(9_000_000);
    let configure = |entity, parent, name: &str, at| {
        Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity,
            parent,
            name: name.into(),
            quota: CostUnits(1_000_000_000_000),
            updated_at: at,
        })
    };

    // On insert, created_at == updated_at == the command's stamp.
    apply_ok(&mut sm, configure(qid(7), Some(ROOT), "seven", create_at));
    let created = &sm.quota_entities[&qid(7)];
    assert_eq!(created.created_at, create_at);
    assert_eq!(created.updated_at, create_at);

    // On update, updated_at advances to the new stamp; created_at is preserved.
    apply_ok(&mut sm, configure(qid(7), Some(ROOT), "renamed", update_at));
    let updated = &sm.quota_entities[&qid(7)];
    assert_eq!(updated.name, "renamed");
    assert_eq!(updated.created_at, create_at);
    assert_eq!(updated.updated_at, update_at);
}

#[test]
fn charges_propagate_to_ancestors() {
    let mut sm = setup();
    apply_ok(&mut sm, configure_entity_cmd(qid(2), Some(ROOT)));
    let mut cmd = submit_cmd(jid(1), cpu(4_000), Some(3_600), RetryPolicy::default());
    if let Command::SubmitJob(ref mut s) = cmd {
        s.job.quota_entity = qid(2);
    }
    apply_ok(&mut sm, cmd);
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(4_000)),
            base_ts(),
        ),
    );
    let leaf = sm.quota_entities[&qid(2)].usage.usage;
    let root = sm.quota_entities[&ROOT].usage.usage;
    assert!(!leaf.is_zero());
    assert_eq!(
        leaf, root,
        "every ancestor on the path is charged the same cost"
    );
}

#[test]
fn cluster_version_bumps_are_monotonic() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        Command::BumpClusterVersion(BumpClusterVersion {
            to: 2,
            bumped_at: base_ts(),
        }),
    );
    assert_eq!(sm.cluster_version, 2);
    let rejection = sm
        .apply(&Command::BumpClusterVersion(BumpClusterVersion {
            to: 2,
            bumped_at: base_ts(),
        }))
        .unwrap_err();
    assert_eq!(
        rejection,
        RejectionReason::ClusterVersionNotMonotonic {
            current: 2,
            requested: 2
        }
    );
}

#[test]
fn rejection_is_a_deterministic_no_op_that_bumps_version() {
    let mut sm = setup();
    let mut expected = sm.clone();
    let rejection = sm.apply(&abort_cmd(jid(404), base_ts())).unwrap_err();
    assert_eq!(rejection, RejectionReason::UnknownJob(jid(404)));
    // A rejected command is an applied (no-op) log entry: only version moves.
    expected.version += 1;
    assert_eq!(sm, expected);
}

#[test]
fn stale_and_duplicate_reports_reject_monotonically() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );

    // Dispatch of a non-Ready attempt, started before dispatch, duplicate
    // started: all deterministic StaleAttemptState rejections.
    assert_eq!(
        sm.apply(&started_cmd(aid(11), base_ts())).unwrap_err(),
        RejectionReason::StaleAttemptState(aid(11))
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    assert_eq!(
        sm.apply(&dispatch_cmd(aid(11), base_ts())).unwrap_err(),
        RejectionReason::StaleAttemptState(aid(11))
    );
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    assert_eq!(
        sm.apply(&started_cmd(aid(11), base_ts())).unwrap_err(),
        RejectionReason::StaleAttemptState(aid(11))
    );
}

#[test]
fn submit_validates_identity_and_entity() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    // Same client-minted id, different spec: a distinct intent, rejected.
    assert_eq!(
        sm.apply(&submit_cmd(
            jid(1),
            cpu(2_000),
            None,
            RetryPolicy::default()
        ))
        .unwrap_err(),
        RejectionReason::SubmitSpecMismatch(jid(1))
    );
    let mut cmd = submit_cmd(jid(2), cpu(1_000), None, RetryPolicy::default());
    if let Command::SubmitJob(ref mut s) = cmd {
        s.job.quota_entity = qid(404);
    }
    assert_eq!(
        sm.apply(&cmd).unwrap_err(),
        RejectionReason::UnknownQuotaEntity(qid(404))
    );
}

/// The job id is the submission's idempotency identity (ADR 0026, KOI-2): an
/// identical resubmission — a client retry after an unknown outcome, or a
/// re-proposal across a leader change — is an accepted no-op with no events,
/// so the retrying client observes success and the original job.
#[test]
fn identical_resubmission_is_an_accepted_no_op() {
    let mut sm = setup();
    let cmd = submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default());
    apply_ok(&mut sm, cmd.clone());
    let mut expected = sm.clone();

    let applied = apply_ok(&mut sm, cmd.clone());
    assert_eq!(applied.events, vec![], "a repeat must not re-emit events");
    // Only the applied-entry counter moves; the job record is untouched.
    expected.version += 1;
    assert_eq!(sm, expected);

    // A different `submitted_at` (a retry re-stamps it) and multiplier are
    // not identity: the original commit's values stay authoritative.
    let mut restamped = cmd.clone();
    if let Command::SubmitJob(ref mut s) = restamped {
        s.submitted_at += Duration::from_secs(1);
    }
    apply_ok(&mut sm, restamped);
    expected.version += 1;
    assert_eq!(sm, expected);
}

/// The idempotent repeat holds for the job's whole residence in replicated
/// state — after an abort mutated the record and after the job went terminal
/// — because `abort_requested` and lifecycle state are apply-owned, not
/// submission identity.
#[test]
fn resubmission_stays_idempotent_after_abort_and_terminal_state() {
    let mut sm = setup();
    let cmd = submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default());
    apply_ok(&mut sm, cmd.clone());
    // Queued job: abort is immediate and terminal.
    apply_ok(&mut sm, abort_cmd(jid(1), base_ts()));
    assert!(sm.jobs[&jid(1)].state.is_terminal());

    let mut expected = sm.clone();
    let applied = apply_ok(&mut sm, cmd);
    assert_eq!(applied.events, vec![]);
    expected.version += 1;
    assert_eq!(sm, expected);
}

/// ADR 0029: a job placed with no declared `max_runtime` prices at the folded
/// (unbounded × base) multiplier over the synthetic `default_charge_runtime_s`,
/// and its charge record carries a full refund fraction — the synthetic
/// runtime is the platform's estimate, never a claim to retain against.
#[test]
fn unbounded_placement_charges_folded_rate_and_records_full_refund() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    // 1 core = 1_000_000 µCU/s; default_charge_runtime_s = 86_400; base 1× is
    // folded with the default 2.0× unbounded multiplier.
    let expected = CostUnits(1_000_000 * 86_400 * 2);
    assert_eq!(sm.attempts[&aid(11)].charge.amount, expected);
    assert_eq!(
        sm.attempts[&aid(11)].charge.refund_fraction_milli,
        FULL_REFUND_MILLI
    );
    assert_eq!(
        sm.attempts[&aid(11)].multiplier,
        PriorityMultiplier::from_integer(2),
        "the unbounded multiplier is folded into the recorded multiplier"
    );
    // Same tick as configure: no decay, so usage is exactly the charge.
    assert_eq!(sm.quota_entities[&ROOT].usage.usage, expected);
}

/// ADR 0029: a bounded job that ends job-attributably before its declared
/// bound refunds only `refund_fraction_milli` of the unused charge; the
/// remainder settles into usage. Charge and resolution share a tick, so the
/// retained µCU is exact.
#[test]
fn bounded_early_exit_retains_the_configured_fraction() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, exited_cmd(aid(11), ts(TS_US + 1)));
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            900,
            ts(TS_US + 1),
        ),
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Succeeded);
    // Charge 3600 s, ran 900 s: unused 2_700_000_000 µCU, of which the default
    // 750-milli fraction (2_025_000_000) refunds and 675_000_000 is retained
    // on top of the 900_000_000 actually consumed.
    let charge = 1_000_000 * 3_600;
    let actual = 1_000_000 * 900;
    let unused = charge - actual;
    let refunded = unused * 750 / 1000;
    assert_eq!(
        sm.quota_entities[&ROOT].usage.usage,
        CostUnits(charge - refunded)
    );
}

/// ADR 0029: platform-attributable resolutions never retain, even for a
/// bounded job with a sub-1000 refund fraction. An attempt that never ran has
/// zero actual cost, so the whole charge returns and usage lands back at the
/// pre-placement level.
#[test]
fn platform_faults_refund_bounded_jobs_in_full() {
    // Revoked while accruing: the whale never started.
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(8_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(8_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    let before_whale = sm.quota_entities[&ROOT].usage.usage;
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(10_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(2), aid(22), alid(222), nid(1), cpu(10_000)),
            base_ts(),
        ),
    );
    assert_eq!(sm.attempts[&aid(22)].attempt.state, AttemptState::Accruing);
    assert!(
        sm.quota_entities[&ROOT].usage.usage > before_whale,
        "placement charges up front"
    );
    apply_ok(
        &mut sm,
        Command::CommitPlacements(CommitPlacements {
            expected_version: 0,
            revocations: vec![alid(222)],
            placements: vec![],
            proposed_at: base_ts(),
        }),
    );
    assert_eq!(
        sm.quota_entities[&ROOT].usage.usage, before_whale,
        "revoke refunds the whole bounded charge despite the 750-milli fraction"
    );

    // NodeLost while dispatched: still never started, so the same full refund.
    let mut sm = setup();
    let before = sm.quota_entities[&ROOT].usage.usage;
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), Some(3_600), RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    assert!(sm.quota_entities[&ROOT].usage.usage > before);
    apply_ok(
        &mut sm,
        Command::DeclareNodeLost(DeclareNodeLost {
            node: nid(1),
            declared_at: base_ts(),
        }),
    );
    assert_eq!(
        sm.attempts[&aid(11)].attempt.state,
        AttemptState::Terminal(AttemptOutcome::NodeLost)
    );
    assert_eq!(
        sm.quota_entities[&ROOT].usage.usage, before,
        "node loss refunds the whole bounded charge"
    );
}

/// ADR 0029: an unbounded job that finishes early refunds its unused synthetic
/// charge *in full* (the record's fraction is 1000), and both charge and
/// true-up price at the folded rate — so usage settles at exactly the
/// synthetic-rate consumption of what ran.
#[test]
fn unbounded_early_finish_refunds_synthetic_charge_in_full() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(11), base_ts()));
    apply_ok(&mut sm, exited_cmd(aid(11), ts(TS_US + 1)));
    apply_ok(
        &mut sm,
        outcome_cmd(
            aid(11),
            AttemptOutcome::Exited { code: 0 },
            900,
            ts(TS_US + 1),
        ),
    );
    // Ran 900 s at the folded 2.0× rate: 1_000_000 × 900 × 2 µCU. The rest of
    // the synthetic 24 h charge comes back in full.
    assert_eq!(
        sm.quota_entities[&ROOT].usage.usage,
        CostUnits(1_000_000 * 900 * 2)
    );
}

/// ADR 0029: `UpdatePolicy` validates the two incentive knobs at commit, like
/// the decay policy — a multiplier below 1.0 or a refund fraction above 1000
/// rejects as `InvalidPolicy`, a deterministic no-op.
#[test]
fn update_policy_rejects_bad_incentive_knobs() {
    let mut sm = setup();
    let mut low = test_policy(4);
    low.unbounded_runtime_multiplier = PriorityMultiplier(PriorityMultiplier::ONE.0 - 1);
    let r = sm.apply(&update_policy_cmd(low)).unwrap_err();
    assert!(matches!(r, RejectionReason::InvalidPolicy(_)), "got {r:?}");

    let mut high = test_policy(4);
    high.refund_fraction_milli = FULL_REFUND_MILLI + 1;
    let r = sm.apply(&update_policy_cmd(high)).unwrap_err();
    assert!(matches!(r, RejectionReason::InvalidPolicy(_)), "got {r:?}");

    // Both rejections left the policy untouched; a valid edit still applies.
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
}

#[test]
fn v1_placement_shape_is_enforced() {
    let mut sm = setup();
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(1_000), None, RetryPolicy::default()),
    );
    let mut p = placement(jid(1), aid(11), alid(111), nid(1), cpu(1_000));
    p.group = GroupId(jid(999).0);
    assert_eq!(
        sm.apply(&place_cmd(p, base_ts())).unwrap_err(),
        RejectionReason::InvalidBatch(vec![Rejection {
            item_index: 0,
            reason: RejectionReason::UnsupportedPlacementShape
        }])
    );
}
