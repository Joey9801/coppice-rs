//! Behavioural tests for the `HeuristicScheduler` against real apply-built
//! states: resource-fit filtering, best-fit choice, the accrual cap K,
//! per-cycle work caps, emitted command shape, the strict-backfill boundary
//! (ADR 0014), and the finite projected-ready protection rules (ADR 0027).

mod common;

use common::*;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::AllocationId;
use coppice_core::job::JobState;
use coppice_core::quota::PriorityMultiplier;
use coppice_core::resource::Resources;
use coppice_scheduler::{HeuristicScheduler, PlacementProposal, Scheduler, SchedulerConfig};
use coppice_state::command::RecordAttemptOutcome;
use coppice_state::{Command, StateMachine};

fn schedule(sm: &StateMachine, now_us: i64) -> PlacementProposal {
    HeuristicScheduler::default().schedule(sm, now_us)
}

fn schedule_with(sm: &StateMachine, cfg: SchedulerConfig, now_us: i64) -> PlacementProposal {
    HeuristicScheduler::new(cfg).schedule(sm, now_us)
}

/// Submit a job whose enforced `max_runtime` is given in microseconds (the
/// second-granularity helper cannot express a sub-second boundary).
fn submit_runtime_us(
    sm: &mut StateMachine,
    job: coppice_core::id::JobId,
    requests: coppice_core::resource::Resources,
    max_runtime_us: u64,
) {
    let spec = coppice_core::job::Job {
        id: job,
        image: "img".into(),
        command: vec!["run".into()],
        entrypoint: None,
        requests,
        priority: 0,
        max_runtime_us: Some(max_runtime_us),
        quota_entity: ROOT,
        retry: coppice_core::job::RetryPolicy::default(),
        abort_requested: None,
    };
    apply_ok(
        sm,
        coppice_state::Command::SubmitJob(coppice_state::command::SubmitJob {
            job: spec,
            multiplier: PriorityMultiplier::ONE,
            submitted_at_us: TS,
        }),
    );
}

/// Apply a proposal through real commands and return the applied result, plus
/// the (attempt, allocation) ids that were minted for each placement (in
/// order).
fn commit(
    sm: &mut StateMachine,
    proposal: &PlacementProposal,
) -> Result<
    Vec<(coppice_core::id::AttemptId, coppice_core::id::AllocationId)>,
    coppice_state::RejectionReason,
> {
    let mut minted = Vec::new();
    let mut mint = minter();
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids);
        ids
    });
    sm.apply(&coppice_state::Command::CommitPlacements(cmd))?;
    Ok(minted)
}

#[test]
fn seats_a_fitting_job_and_emits_the_v1_shape() {
    let mut sm = setup(cpu(10_000), 4);
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(4_000), Some(600), PriorityMultiplier::ONE, TS),
    );

    let proposal = schedule(&sm, TS + 1);
    assert_eq!(proposal.placements.len(), 1);
    assert!(proposal.revocations.is_empty());
    let p = &proposal.placements[0];
    assert_eq!(p.job, jid(1));
    assert_eq!(p.node, nid(1));
    assert!(p.expect_funded, "a 4-core job on a free 10-core node funds");

    // The command carries the v1 shape apply demands: singleton group == job
    // id, exactly one allocation.
    let cmd = proposal.to_commit_placements(&mut minter());
    assert_eq!(cmd.placements.len(), 1);
    assert_eq!(cmd.placements[0].group.0, jid(1).0);
    assert_eq!(cmd.placements[0].allocations.len(), 1);
    assert_eq!(cmd.expected_version, proposal.against_version);
    assert_eq!(cmd.proposed_at_us, proposal.now_us);

    let minted = commit(&mut sm, &proposal).expect("batch applies");
    let (attempt, alloc) = minted[0];
    assert_eq!(
        sm.allocations[&alloc].allocation.state,
        AllocationState::Funded
    );
    assert_eq!(sm.jobs[&jid(1)].state, JobState::Attempting(attempt));
}

#[test]
fn rejects_a_job_that_exceeds_every_node_on_one_dimension() {
    // Node has plenty of CPU and disk but little memory; a memory-heavy job
    // fits no node and is never placed.
    let mut sm = setup(res(32_000, 8 << 30, 1 << 40), 4);
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            res(1_000, 64 << 30, 0),
            Some(600),
            PriorityMultiplier::ONE,
            TS,
        ),
    );
    let proposal = schedule(&sm, TS + 1);
    assert!(proposal.is_empty(), "an unplaceable job yields no proposal");
}

#[test]
fn best_fit_prefers_the_snugger_node() {
    // Two nodes: a roomy 64-core and a snug 8-core. A 6-core job packs onto the
    // snug node, leaving the roomy one for bigger work.
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(64_000), TS));
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(8_000), TS));
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(6_000), Some(600), PriorityMultiplier::ONE, TS),
    );

    let proposal = schedule(&sm, TS + 1);
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(
        proposal.placements[0].node,
        nid(2),
        "best-fit picks the snug node"
    );
}

#[test]
fn honours_the_effective_score_order_and_the_candidate_cap() {
    // Three queued jobs, one node with room for one. With max_candidates == 1
    // only the top-scored job is even considered.
    let mut sm = setup(cpu(10_000), 4);
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(9_000), Some(600), PriorityMultiplier::ONE, TS),
    );
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(9_000),
            Some(600),
            PriorityMultiplier::from_integer(5),
            TS,
        ),
    );
    apply_ok(
        &mut sm,
        submit_cmd(jid(3), cpu(9_000), Some(600), PriorityMultiplier::ONE, TS),
    );

    let cfg = SchedulerConfig {
        max_candidates: 1,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, TS + 1);
    assert_eq!(proposal.placements.len(), 1);
    // jid(2) has the 5× multiplier ⇒ top of the score order ⇒ the sole
    // candidate seated.
    assert_eq!(proposal.placements[0].job, jid(2));
}

#[test]
fn honours_the_placement_cap() {
    // Many fitting jobs, but the per-cycle placement cap bounds the batch.
    let mut sm = setup(res(1_000_000, 0, 0), 4);
    for i in 1..=10u128 {
        apply_ok(
            &mut sm,
            submit_cmd(jid(i), cpu(1_000), Some(600), PriorityMultiplier::ONE, TS),
        );
    }
    let cfg = SchedulerConfig {
        max_placements_per_cycle: 3,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, TS + 1);
    assert_eq!(
        proposal.placements.len(),
        3,
        "the placement cap bounds emissions"
    );
}

#[test]
fn accrual_opening_respects_the_cap_k() {
    // A node whose free capacity is already partly consumed, and three whales
    // that each need the whole node — they fit total capacity (so apply admits
    // them) but not free capacity, so each seating is an accrual open. With
    // K = 2 only two may hold accruing allocations at once.
    let mut sm = setup(cpu(8_000), 2);
    apply_ok(
        &mut sm,
        submit_cmd(jid(10), cpu(2_000), Some(600), PriorityMultiplier::ONE, TS),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(10), aid(10), alid(10), nid(1), cpu(2_000)),
            TS,
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(10), TS));
    apply_ok(&mut sm, started_cmd(aid(10), TS));
    for i in 1..=3u128 {
        apply_ok(
            &mut sm,
            submit_cmd(jid(i), cpu(8_000), None, PriorityMultiplier::ONE, TS),
        );
    }
    let proposal = schedule(&sm, TS + 1);
    // No whale free-fits (8 > 6 free) and none can backfill (no max_runtime),
    // so each seating is an accrual open — capped at K = 2.
    assert_eq!(
        proposal.placements.len(),
        2,
        "at most K accruing jobs opened"
    );
    assert!(proposal.placements.iter().all(|p| !p.expect_funded));

    let minted = commit(&mut sm, &proposal).expect("K-respecting batch applies");
    assert_eq!(minted.len(), 2);
    for (_, a) in &minted {
        assert_eq!(
            sm.allocations[a].allocation.state,
            AllocationState::Accruing
        );
    }
    // A second pass adds nothing: the cap is already reached.
    let again = schedule(&sm, TS + 2);
    assert!(again.is_empty(), "no third accrual past K");
}

#[test]
fn fixpoint_reached_after_placing_everything_placeable() {
    let mut sm = setup(cpu(10_000), 4);
    apply_ok(
        &mut sm,
        submit_cmd(jid(1), cpu(3_000), Some(600), PriorityMultiplier::ONE, TS),
    );
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(3_000), Some(600), PriorityMultiplier::ONE, TS),
    );

    let proposal = schedule(&sm, TS + 1);
    assert_eq!(proposal.placements.len(), 2);
    commit(&mut sm, &proposal).expect("applies");
    // Immediately re-running on the applied state must be empty (the driver's
    // backoff depends on it).
    assert!(schedule(&sm, TS + 2).is_empty());
}

/// Drive a running job onto the node so its capacity is consumed and it carries
/// a guaranteed release event, then queue a whale that can only accrue.
fn state_with_running_and_accruing_whale(
    runtime_r_s: u64,
) -> (StateMachine, coppice_core::id::AllocationId) {
    let mut sm = setup(cpu(32_000), 4);
    // Running job R: 16 cpu, enforced max_runtime, started at TS.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            cpu(16_000),
            Some(runtime_r_s),
            PriorityMultiplier::ONE,
            TS,
        ),
    );
    apply_ok(
        &mut sm,
        place_cmd(placement(jid(1), aid(1), alid(1), nid(1), cpu(16_000)), TS),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(1), TS));
    apply_ok(&mut sm, started_cmd(aid(1), TS));
    // Whale W: needs the whole 32 cpu, no max_runtime ⇒ it just accrues.
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(32_000), None, PriorityMultiplier::ONE, TS),
    );

    let proposal = schedule(&sm, TS + 50);
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].job, jid(2));
    assert!(!proposal.placements[0].expect_funded, "the whale accrues");
    let mut minted = Vec::new();
    // A low base disjoint from the test's own later minting into this state.
    let mut mint = minter_from(1000);
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids.1);
        ids
    });
    sm.apply(&coppice_state::Command::CommitPlacements(cmd))
        .expect("accrual opens");
    let whale_alloc = minted[0];
    assert_eq!(
        sm.allocations[&whale_alloc].allocation.state,
        AllocationState::Accruing
    );
    // The whale is funded to the 16 cpu currently free.
    assert_eq!(sm.allocations[&whale_alloc].allocation.funded, cpu(16_000));
    (sm, whale_alloc)
}

#[test]
fn strict_backfill_lends_exactly_at_the_boundary() {
    // R runs for 3600 s from TS ⇒ its 16 cpu is guaranteed free at
    // projected_ready = TS + 3_600_000_000, which is when the whale W would
    // become ready. A small job S that finishes exactly then may lend.
    let runtime_r_s = 3600u64;
    let projected_ready = TS + (runtime_r_s as i64) * 1_000_000;
    let (mut sm, whale_alloc) = state_with_running_and_accruing_whale(runtime_r_s);

    // S needs 8 cpu with an enforced runtime chosen so now + S.max_runtime
    // lands exactly on projected_ready (microsecond precision).
    let now = TS + 100;
    let s_runtime_us = (projected_ready - now) as u64;
    submit_runtime_us(&mut sm, jid(3), cpu(8_000), s_runtime_us);

    let proposal = schedule(&sm, now);
    // A lend: revoke the whale, seat S, reseat the whale after it.
    assert_eq!(
        proposal.revocations,
        vec![whale_alloc],
        "the lend revokes the whale"
    );
    assert!(proposal
        .placements
        .iter()
        .any(|p| p.job == jid(3) && p.expect_funded));
    assert!(
        proposal.placements.iter().any(|p| p.job == jid(2)),
        "the whale is reseated"
    );
    // Funding order: S is seated before the whale it borrows from.
    let s_idx = proposal
        .placements
        .iter()
        .position(|p| p.job == jid(3))
        .unwrap();
    let w_idx = proposal
        .placements
        .iter()
        .position(|p| p.job == jid(2))
        .unwrap();
    assert!(s_idx < w_idx, "the backfilled job funds before the reseat");

    // The whole batch applies with zero rejections.
    let mut mint = minter();
    let cmd = proposal.to_commit_placements(&mut mint);
    sm.apply(&coppice_state::Command::CommitPlacements(cmd))
        .expect("the lend applies cleanly");
}

#[test]
fn strict_backfill_declines_one_microsecond_past_the_boundary() {
    let runtime_r_s = 3600u64;
    let projected_ready = TS + (runtime_r_s as i64) * 1_000_000;
    let (mut sm, _whale_alloc) = state_with_running_and_accruing_whale(runtime_r_s);

    // S would finish one microsecond after the whale's projected_ready — the
    // strict rule forbids the lend.
    let now = TS + 100;
    let s_runtime_us = (projected_ready - now) as u64 + 1;
    submit_runtime_us(&mut sm, jid(3), cpu(8_000), s_runtime_us);

    let proposal = schedule(&sm, now);
    // No lend: the whale keeps its pledge. S opens its own accrual instead (the
    // node has no free capacity), so no revocation is proposed.
    assert!(proposal.revocations.is_empty(), "no lend past the boundary");
    let mut mint = minter();
    let cmd = proposal.to_commit_placements(&mut mint);
    sm.apply(&coppice_state::Command::CommitPlacements(cmd))
        .expect("applies without a lend");
    // The whale's accrual survives untouched.
    assert!(
        sm.attempts
            .values()
            .any(|a| a.attempt.state == AttemptState::Accruing),
        "the whale is still accruing"
    );
}

// ---- ADR 0027: finite projected-ready protection ----

/// Seed a running holder on a node: submit, place, dispatch, start (all at
/// `TS`). `max_runtime_s = None` makes the hold unbounded.
fn seed_running(
    sm: &mut StateMachine,
    n: u128,
    node: u128,
    requests: Resources,
    max_runtime_s: Option<u64>,
) {
    apply_ok(
        sm,
        submit_cmd(jid(n), requests, max_runtime_s, PriorityMultiplier::ONE, TS),
    );
    apply_ok(
        sm,
        place_cmd(placement(jid(n), aid(n), alid(n), nid(node), requests), TS),
    );
    apply_ok(sm, dispatch_cmd(aid(n), TS));
    apply_ok(sm, started_cmd(aid(n), TS));
}

/// One 32-cpu node where an *unbounded* runner `jid(1)` holds 16 cpu, and a
/// whale `jid(2)` (32 cpu, no `max_runtime`) accrues with 16 cpu funded. The
/// whale's remainder waits on the unbounded runner, so its `projected_ready`
/// is indefinite.
fn state_with_indefinite_whale() -> (StateMachine, AllocationId) {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), None);
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(32_000), None, PriorityMultiplier::ONE, TS),
    );

    // The fallback (ADR 0027): no node offers a finite bound, but the whale
    // still gets its accrual — protection does not depend on the bound.
    let proposal = schedule(&sm, TS + 50);
    assert_eq!(proposal.placements.len(), 1);
    assert!(!proposal.placements[0].expect_funded, "the whale accrues");
    let mut minted = Vec::new();
    let mut mint = minter_from(1000);
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids.1);
        ids
    });
    sm.apply(&Command::CommitPlacements(cmd))
        .expect("the accrual opens");
    let whale_alloc = minted[0];
    assert_eq!(sm.allocations[&whale_alloc].allocation.funded, cpu(16_000));
    (sm, whale_alloc)
}

#[test]
fn no_lend_when_the_accruals_bound_is_indefinite() {
    let (mut sm, whale_alloc) = state_with_indefinite_whale();
    // S is small and tightly bounded. Under ADR 0014's `None => true` it
    // would lend the whale's pledge; under ADR 0027 an indefinite bound
    // forbids the lend outright.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(3),
            cpu(8_000),
            Some(600),
            PriorityMultiplier::ONE,
            TS + 60,
        ),
    );
    let proposal = schedule(&sm, TS + 100);
    assert!(
        proposal.revocations.is_empty(),
        "no lend without a finite bound"
    );
    // S queues up behind the whale (its own accrual) instead of jumping it.
    assert!(proposal
        .placements
        .iter()
        .all(|p| p.job == jid(3) && !p.expect_funded));
    commit(&mut sm, &proposal).expect("applies without a lend");
    assert_eq!(
        sm.allocations[&whale_alloc].allocation.funded,
        cpu(16_000),
        "the whale keeps every unit it accrued"
    );
}

#[test]
fn an_indefinite_accrual_survives_an_adversarial_backfill_stream() {
    // The KOI-4 scenario: a succession of bounded jobs arrives while the
    // whale's bound is indefinite. P1: none of them may take anything back;
    // the whale's funding must be exactly what it would be with no stream.
    let (mut sm, whale_alloc) = state_with_indefinite_whale();
    let mut mint = minter();
    for i in 0..8u32 {
        let now = TS + 100 + i64::from(i) * 1_000_000;
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(100 + u128::from(i)),
                cpu(8_000),
                Some(600),
                PriorityMultiplier::ONE,
                now,
            ),
        );
        let proposal = schedule(&sm, now + 1);
        assert!(
            proposal.revocations.is_empty(),
            "pass {i}: backfill must not touch the indefinite accrual"
        );
        if proposal.is_empty() {
            continue;
        }
        let cmd = proposal.to_commit_placements(&mut mint);
        sm.apply(&Command::CommitPlacements(cmd))
            .unwrap_or_else(|e| panic!("pass {i}: applies: {e}"));
        assert_eq!(
            sm.allocations[&whale_alloc].allocation.funded,
            cpu(16_000),
            "pass {i}: funded capacity is monotone"
        );
        assert_eq!(
            sm.allocations[&whale_alloc].allocation.state,
            AllocationState::Accruing
        );
    }

    // The unbounded holder finally finishes. Its 16 cpu must fund the whale
    // at that very instant — funding is seq order and the adversarial
    // accruals all sit behind it — exactly as if the stream never arrived.
    let done = TS + 100 + 9_000_000;
    apply_ok(
        &mut sm,
        Command::RecordAttemptOutcome(RecordAttemptOutcome {
            attempt: aid(1),
            outcome: AttemptOutcome::Exited { code: 0 },
            actual_runtime_us: (done - TS) as u64,
            observed_at_us: done,
        }),
    );
    assert_eq!(
        sm.allocations[&whale_alloc].allocation.state,
        AllocationState::Funded,
        "the whale funds the moment its holder releases"
    );
    assert_eq!(sm.allocations[&whale_alloc].allocation.funded, cpu(32_000));
}

#[test]
fn accrual_opens_where_the_bound_is_finite() {
    // Two full nodes: node1 held by an unbounded job, node2 by a bounded one.
    // Both pledge nothing today, and the pledge-only ranking would take node1
    // (lowest NodeId). ADR 0027 never opens on an indefinite-bound node while
    // a finite-bound node is eligible.
    let mut sm = setup(cpu(32_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), TS));
    seed_running(&mut sm, 1, 1, cpu(32_000), None);
    seed_running(&mut sm, 2, 2, cpu(32_000), Some(3600));
    apply_ok(
        &mut sm,
        submit_cmd(jid(3), cpu(32_000), None, PriorityMultiplier::ONE, TS),
    );

    let proposal = schedule(&sm, TS + 1);
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].node, nid(2), "finite bound wins");
    assert!(!proposal.placements[0].expect_funded);
}

#[test]
fn an_accrual_moves_when_a_finite_bound_appears_elsewhere() {
    let (mut sm, whale_alloc) = state_with_indefinite_whale();
    // A second node appears, fully held by a bounded job: moving the whale
    // there trades an indefinite bound for a finite one — always worth it,
    // whatever the improvement threshold.
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), TS));
    seed_running(&mut sm, 3, 2, cpu(32_000), Some(3600));

    let proposal = schedule(&sm, TS + 200);
    assert_eq!(proposal.revocations, vec![whale_alloc], "the move revokes");
    assert_eq!(proposal.placements.len(), 1);
    let p = &proposal.placements[0];
    assert_eq!((p.job, p.node), (jid(2), nid(2)));
    assert!(!p.expect_funded, "the whale re-accrues on the target");

    commit(&mut sm, &proposal).expect("the move applies");
    // Fixpoint: re-running immediately on the applied state proposes nothing
    // (the driver's backoff depends on this).
    assert!(
        schedule(&sm, TS + 300).is_empty(),
        "no churn after the move"
    );
}

#[test]
fn a_finite_bound_moves_only_for_a_meaningful_improvement() {
    // The whale accrues on node1 with a finite bound (its holder releases at
    // TS + 7200 s) before node2 exists.
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(7200));
    apply_ok(
        &mut sm,
        submit_cmd(jid(2), cpu(32_000), None, PriorityMultiplier::ONE, TS),
    );
    let open = schedule(&sm, TS + 50);
    assert_eq!(open.placements.len(), 1);
    assert!(!open.placements[0].expect_funded);
    let mut mint = minter_from(1000);
    let cmd = open.to_commit_placements(&mut mint);
    sm.apply(&Command::CommitPlacements(cmd))
        .expect("the accrual opens");

    // node2's bound would be 60 s earlier — a real improvement, but under the
    // default 300 s threshold not a meaningful one.
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), TS));
    seed_running(&mut sm, 3, 2, cpu(32_000), Some(7140));
    assert!(
        schedule(&sm, TS + 100).is_empty(),
        "a 60 s gain does not justify a move at the default threshold"
    );

    // The same state under a 30 s threshold does move (the pass is a pure
    // function of the snapshot, so re-scheduling it is legal).
    let cfg = SchedulerConfig {
        replan_min_improvement_us: 30_000_000,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, TS + 100);
    assert_eq!(proposal.revocations.len(), 1, "the lower threshold moves");
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].node, nid(2));
}
