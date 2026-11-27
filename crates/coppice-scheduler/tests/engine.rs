//! Behavioural tests for the `HeuristicScheduler` against real apply-built
//! states: resource-fit filtering, best-fit choice, the accrual cap K,
//! per-cycle work caps, emitted command shape, the strict-backfill boundary
//! (ADR 0014), the finite projected-ready protection rules and per-node
//! accrual ceiling (ADR 0027), and per-class age weights (ADR 0047).

mod common;

use common::*;

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, JobId, NodeId};
use coppice_core::job::JobState;
use coppice_core::quota::{CostUnits, PriorityClass, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_scheduler::{score, HeuristicScheduler, PlacementProposal, Scheduler, SchedulerConfig};
use coppice_state::command::RecordAttemptOutcome;
use coppice_state::{Command, StateMachine};

fn schedule(sm: &StateMachine, now: Timestamp) -> PlacementProposal {
    HeuristicScheduler::default().schedule(sm, now)
}

fn schedule_with(sm: &StateMachine, cfg: SchedulerConfig, now: Timestamp) -> PlacementProposal {
    HeuristicScheduler::new(cfg).schedule(sm, now)
}

/// Submit a job with an exact enforced `max_runtime` (the second-granularity
/// helper cannot express a sub-second boundary).
fn submit_exact_runtime(
    sm: &mut StateMachine,
    job: coppice_core::id::JobId,
    requests: coppice_core::resource::Resources,
    max_runtime: Duration,
) {
    let spec = coppice_core::job::Job {
        id: job,
        image: "img".into(),
        command: vec!["run".into()],
        entrypoint: None,
        requests,
        priority: PriorityClass(0),
        max_runtime: Some(max_runtime),
        quota_entity: ROOT,
        retry: coppice_core::job::RetryPolicy::default(),
        abort_requested: None,
        submitted_by: None,
        metadata: Default::default(),
        env: Default::default(),
        preemption: Default::default(),
    };
    apply_ok(
        sm,
        coppice_state::Command::SubmitJob(coppice_state::command::SubmitJob {
            job: spec,
            multiplier: PriorityMultiplier::ONE,
            submitted_at: base_ts(),
            actor: None,
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
        submit_cmd(
            jid(1),
            cpu(4_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = schedule(&sm, ts(TS_US + 1));
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
    assert_eq!(cmd.proposed_at, proposal.now);

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
    let mut sm = setup(res(32_000, ByteSize::from_gib(8), ByteSize::from_tib(1)), 4);
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            res(1_000, ByteSize::from_gib(64), ByteSize::ZERO),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    let proposal = schedule(&sm, ts(TS_US + 1));
    assert!(proposal.is_empty(), "an unplaceable job yields no proposal");
}

#[test]
fn best_fit_prefers_the_snugger_node() {
    // Two nodes: a roomy 64-core and a snug 8-core. A 6-core job packs onto the
    // snug node, leaving the roomy one for bigger work.
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(64_000), base_ts()));
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(8_000), base_ts()));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            cpu(6_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = schedule(&sm, ts(TS_US + 1));
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
        submit_cmd(
            jid(1),
            cpu(9_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(9_000),
            Some(600),
            PriorityMultiplier::from_integer(5),
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(3),
            cpu(9_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let cfg = SchedulerConfig {
        max_candidates: 1,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, ts(TS_US + 1));
    assert_eq!(proposal.placements.len(), 1);
    // jid(2) has the 5× multiplier ⇒ top of the score order ⇒ the sole
    // candidate seated.
    assert_eq!(proposal.placements[0].job, jid(2));
}

/// ADR 0047: a zero-weight (backfill) class never ages, so a fresh
/// higher-class job outranks it however long it has queued — until that
/// job's entity penalty exceeds the class ratio (2× against 1× here).
#[test]
fn a_zero_weight_class_yields_until_the_penalty_exceeds_the_class_ratio() {
    const BACKFILL: u128 = 1;
    const URGENT: u128 = 2;
    const CHARGER: u128 = 9;
    let entity = qid(0xB0);
    let now = base_ts() + Duration::from_days(30);

    // nid(1) has room for exactly one of the two 9-core candidates; nid(2)
    // is filled by a running job that charges the urgent job's entity.
    let mut sm = setup(cpu(10_000), 1);
    apply_ok(&mut sm, configure_entity_cmd(entity, None));
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(10_000), base_ts()));
    // Class -1 at 1x, under-quota root entity, queued for 30 days.
    submit_full(&mut sm, BACKFILL, cpu(9_000), -1, false, 1, Some(600));
    // Class 0 at 2x, submitted just now; and the charging job, both on
    // `entity`.
    for (n, requests, multiplier) in [(URGENT, cpu(9_000), 2), (CHARGER, cpu(10_000), 1)] {
        let Command::SubmitJob(mut c) = submit_cmd(
            jid(n),
            requests,
            Some(600),
            PriorityMultiplier::from_integer(multiplier),
            now,
        ) else {
            unreachable!("submit_cmd builds a SubmitJob");
        };
        c.job.quota_entity = entity;
        apply_ok(&mut sm, Command::SubmitJob(c));
    }
    run_on(&mut sm, CHARGER, nid(2), now);
    let usage = sm.quota_entities[&entity].usage.usage.0;

    // Re-quota the entity so its usage reads `usage / quota` over, and
    // return the resulting penalty product at `now`.
    let set_quota = |sm: &mut StateMachine, quota: u64| -> f64 {
        let Command::ConfigureQuotaEntity(mut c) = configure_entity_cmd(entity, None) else {
            unreachable!("configure_entity_cmd builds a ConfigureQuotaEntity");
        };
        c.quota = CostUnits(quota);
        apply_ok(sm, Command::ConfigureQuotaEntity(c));
        score::penalty_product(&sm.quota_entities, entity, &sm.policy, now)
    };
    let funded = |cfg: SchedulerConfig, sm: &StateMachine| -> Vec<JobId> {
        schedule_with(sm, cfg, now)
            .placements
            .iter()
            .filter(|p| p.expect_funded)
            .map(|p| p.job)
            .collect()
    };
    let backfill_lane = SchedulerConfig {
        age_weights: BTreeMap::from([(PriorityClass(-1), 0.0)]),
        ..SchedulerConfig::default()
    };

    // 1.3x over quota: penalty 1.69, below the 2:1 class ratio.
    let p = set_quota(&mut sm, usage * 10 / 13);
    assert!(p > 1.0 && p < 2.0, "penalty {p}");
    assert_eq!(
        funded(backfill_lane.clone(), &sm),
        vec![jid(URGENT)],
        "30 days of age buy a zero-weight class nothing"
    );
    // Without the override the backfill job's age crosses the class gap.
    assert_eq!(funded(SchedulerConfig::default(), &sm), vec![jid(BACKFILL)]);

    // 1.5x over quota: penalty 2.25, past the class ratio.
    let p = set_quota(&mut sm, usage * 2 / 3);
    assert!(p > 2.0, "penalty {p}");
    assert_eq!(
        funded(backfill_lane, &sm),
        vec![jid(BACKFILL)],
        "the penalty is the way through the lane"
    );
}

#[test]
fn honours_the_placement_cap() {
    // Many fitting jobs, but the per-cycle placement cap bounds the batch.
    let mut sm = setup(res(1_000_000, ByteSize::ZERO, ByteSize::ZERO), 4);
    for i in 1..=10u128 {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(i),
                cpu(1_000),
                Some(600),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
    }
    let cfg = SchedulerConfig {
        max_placements_per_cycle: 3,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, ts(TS_US + 1));
    assert_eq!(
        proposal.placements.len(),
        3,
        "the placement cap bounds emissions"
    );
}

#[test]
fn accrual_opening_respects_the_cap_k() {
    // Three nodes whose free capacity is each already partly consumed, and
    // three whales that each need a whole node — they fit total capacity (so
    // apply admits them) but not free capacity, so each seating is an accrual
    // open. One node apiece keeps the per-node ceiling out of the way, leaving
    // K = 2 as the only thing capping the batch.
    let mut sm = setup(cpu(8_000), 2);
    for n in 2..=3u128 {
        apply_ok(&mut sm, register_node_cmd(nid(n), cpu(8_000), base_ts()));
    }
    for n in 1..=3u128 {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(10 + n),
                cpu(2_000),
                Some(600),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
        apply_ok(
            &mut sm,
            place_cmd(
                placement(jid(10 + n), aid(10 + n), alid(10 + n), nid(n), cpu(2_000)),
                base_ts(),
            ),
        );
        apply_ok(&mut sm, dispatch_cmd(aid(10 + n), base_ts()));
        apply_ok(&mut sm, started_cmd(aid(10 + n), base_ts()));
    }
    for i in 1..=3u128 {
        apply_ok(
            &mut sm,
            submit_cmd(jid(i), cpu(8_000), None, PriorityMultiplier::ONE, base_ts()),
        );
    }
    let proposal = schedule(&sm, ts(TS_US + 1));
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
    let again = schedule(&sm, ts(TS_US + 2));
    assert!(again.is_empty(), "no third accrual past K");
}

#[test]
fn at_most_one_accrual_opens_per_node() {
    // Two nodes, each already running a slice, and three whales that fit
    // neither node's free capacity and declare no `max_runtime` (so no lend is
    // available either). K = 4 leaves the cluster cap slack throughout: the
    // per-node ceiling of one accruing job (ADR 0027) is the only thing that
    // stops the third open.
    let mut sm = setup(cpu(8_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(8_000), base_ts()));
    for n in 1..=2u128 {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(10 + n),
                cpu(2_000),
                Some(600),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
        apply_ok(
            &mut sm,
            place_cmd(
                placement(jid(10 + n), aid(10 + n), alid(10 + n), nid(n), cpu(2_000)),
                base_ts(),
            ),
        );
        apply_ok(&mut sm, dispatch_cmd(aid(10 + n), base_ts()));
        apply_ok(&mut sm, started_cmd(aid(10 + n), base_ts()));
    }
    for i in 1..=3u128 {
        apply_ok(
            &mut sm,
            submit_cmd(jid(i), cpu(8_000), None, PriorityMultiplier::ONE, base_ts()),
        );
    }

    let proposal = schedule(&sm, ts(TS_US + 1));
    assert_eq!(
        proposal.placements.len(),
        2,
        "one accrual per node, not three on the best-scoring one"
    );
    let nodes: BTreeSet<NodeId> = proposal.placements.iter().map(|p| p.node).collect();
    assert_eq!(nodes.len(), 2, "the two opens land on different nodes");
    assert!(proposal.placements.iter().all(|p| !p.expect_funded));
    commit(&mut sm, &proposal).expect("one accrual per node applies");

    // Both nodes are occupied now, so the third whale has nowhere to accrue.
    let again = schedule(&sm, ts(TS_US + 2));
    assert!(again.is_empty(), "no second accrual on an occupied node");
}

#[test]
fn fixpoint_reached_after_placing_everything_placeable() {
    let mut sm = setup(cpu(10_000), 4);
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            cpu(3_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(3_000),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = schedule(&sm, ts(TS_US + 1));
    assert_eq!(proposal.placements.len(), 2);
    commit(&mut sm, &proposal).expect("applies");
    // Immediately re-running on the applied state must be empty (the driver's
    // backoff depends on it).
    assert!(schedule(&sm, ts(TS_US + 2)).is_empty());
}

/// Drive a running job onto the node so its capacity is consumed and it carries
/// a guaranteed release event, then queue a whale that can only accrue.
fn state_with_running_and_accruing_whale(
    runtime_r_s: i64,
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
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(1), alid(1), nid(1), cpu(16_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(1), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(1), base_ts()));
    // Whale W: needs the whole 32 cpu, no max_runtime ⇒ it just accrues.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(32_000),
            None,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = schedule(&sm, ts(TS_US + 50));
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
    // projected_ready = TS + 3600 s, which is when the whale W would become
    // ready. A small job S that finishes exactly then may lend.
    let runtime_r_s = 3600i64;
    let projected_ready = base_ts() + Duration::from_secs(runtime_r_s);
    let (mut sm, whale_alloc) = state_with_running_and_accruing_whale(runtime_r_s);

    // S needs 8 cpu with an enforced runtime chosen so now + S.max_runtime
    // lands exactly on projected_ready (microsecond precision).
    let now = ts(TS_US + 100);
    submit_exact_runtime(&mut sm, jid(3), cpu(8_000), projected_ready - now);

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
    let runtime_r_s = 3600i64;
    let projected_ready = base_ts() + Duration::from_secs(runtime_r_s);
    let (mut sm, _whale_alloc) = state_with_running_and_accruing_whale(runtime_r_s);

    // S would finish one microsecond after the whale's projected_ready — the
    // strict rule forbids the lend.
    let now = ts(TS_US + 100);
    let s_runtime = (projected_ready - now) + Duration::from_micros(1);
    submit_exact_runtime(&mut sm, jid(3), cpu(8_000), s_runtime);

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
    max_runtime_s: Option<i64>,
) {
    apply_ok(
        sm,
        submit_cmd(
            jid(n),
            requests,
            max_runtime_s,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        sm,
        place_cmd(
            placement(jid(n), aid(n), alid(n), nid(node), requests),
            base_ts(),
        ),
    );
    apply_ok(sm, dispatch_cmd(aid(n), base_ts()));
    apply_ok(sm, started_cmd(aid(n), base_ts()));
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
        submit_cmd(
            jid(2),
            cpu(32_000),
            None,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    // The fallback (ADR 0027): no node offers a finite bound, but the whale
    // still gets its accrual — protection does not depend on the bound.
    let proposal = schedule(&sm, ts(TS_US + 50));
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
            ts(TS_US + 60),
        ),
    );
    let proposal = schedule(&sm, ts(TS_US + 100));
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
        let now = ts(TS_US + 100) + Duration::from_secs(i64::from(i));
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
        let proposal = schedule(&sm, now + Duration::from_micros(1));
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
    let done = ts(TS_US + 100) + Duration::from_secs(9);
    apply_ok(
        &mut sm,
        Command::RecordAttemptOutcome(RecordAttemptOutcome {
            attempt: aid(1),
            outcome: AttemptOutcome::Exited { code: 0 },
            actual_runtime: done - base_ts(),
            observed_at: done,
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
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), base_ts()));
    seed_running(&mut sm, 1, 1, cpu(32_000), None);
    seed_running(&mut sm, 2, 2, cpu(32_000), Some(3600));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(3),
            cpu(32_000),
            None,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = schedule(&sm, ts(TS_US + 1));
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
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), base_ts()));
    seed_running(&mut sm, 3, 2, cpu(32_000), Some(3600));

    let proposal = schedule(&sm, ts(TS_US + 200));
    assert_eq!(proposal.revocations, vec![whale_alloc], "the move revokes");
    assert_eq!(proposal.placements.len(), 1);
    let p = &proposal.placements[0];
    assert_eq!((p.job, p.node), (jid(2), nid(2)));
    assert!(!p.expect_funded, "the whale re-accrues on the target");

    commit(&mut sm, &proposal).expect("the move applies");
    // Fixpoint: re-running immediately on the applied state proposes nothing
    // (the driver's backoff depends on this).
    assert!(
        schedule(&sm, ts(TS_US + 300)).is_empty(),
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
        submit_cmd(
            jid(2),
            cpu(32_000),
            None,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    let open = schedule(&sm, ts(TS_US + 50));
    assert_eq!(open.placements.len(), 1);
    assert!(!open.placements[0].expect_funded);
    let mut mint = minter_from(1000);
    let cmd = open.to_commit_placements(&mut mint);
    sm.apply(&Command::CommitPlacements(cmd))
        .expect("the accrual opens");

    // node2's bound would be 60 s earlier — a real improvement, but under the
    // default 300 s threshold not a meaningful one.
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(32_000), base_ts()));
    seed_running(&mut sm, 3, 2, cpu(32_000), Some(7140));
    assert!(
        schedule(&sm, ts(TS_US + 100)).is_empty(),
        "a 60 s gain does not justify a move at the default threshold"
    );

    // The same state under a 30 s threshold does move (the pass is a pure
    // function of the snapshot, so re-scheduling it is legal).
    let cfg = SchedulerConfig {
        replan_min_improvement: Duration::from_secs(30),
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, ts(TS_US + 100));
    assert_eq!(proposal.revocations.len(), 1, "the lower threshold moves");
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].node, nid(2));
}

// ---- drain backfill (ADR 0044 §5) ----

/// `setup` (one 32-cpu node), a running anchor `jid(1)` of 8 cpu with the
/// given `max_runtime`, started at `base_ts()`, then the node drained. With
/// the default 30 s `abort_grace` a 3600 s anchor bounds the node at
/// `base + 3630 s`.
fn drained_with_anchor(max_runtime_s: Option<i64>) -> StateMachine {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(8_000), max_runtime_s);
    apply_ok(&mut sm, drain_cmd(nid(1), base_ts()));
    sm
}

fn secs(n: i64) -> Timestamp {
    ts(TS_US + n * 1_000_000)
}

#[test]
fn a_drained_node_takes_bounded_backfill_within_its_horizon() {
    let mut sm = drained_with_anchor(Some(3_600));
    // Fits by the horizon: 60 + 1800 + 30 ≤ 3630.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(4_000),
            Some(1_800),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    // Overruns it: 60 + 3600 + 30 > 3630.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(3),
            cpu(4_000),
            Some(3_600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    // No bound at all.
    apply_ok(
        &mut sm,
        submit_cmd(jid(4), cpu(4_000), None, PriorityMultiplier::ONE, base_ts()),
    );
    let proposal = schedule(&sm, secs(60));
    let jobs: Vec<_> = proposal.placements.iter().map(|p| p.job).collect();
    assert_eq!(jobs, vec![jid(2)], "only the bounded fit is backfill");
    assert!(proposal.placements[0].expect_funded);
    let minted = commit(&mut sm, &proposal).expect("apply admits what the pass proposed");
    let attempt = &sm.attempts[&minted[0].0];
    assert_eq!(attempt.drain_cutoff, Some(secs(3_630)));
}

/// Under a deadline drain the cutoff is `T_notice`, whatever runs there
/// (ADR 0044 §5): backfill is proposed iff `now + max_runtime + abort_grace
/// <= T_notice`, even beside an unbounded anchor that a regular drain would
/// let block all backfill.
#[test]
fn a_deadline_drained_node_takes_backfill_by_its_notice_time() {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(8_000), None);
    let deadline = secs(4_000);
    apply_ok(&mut sm, deadline_drain_cmd(nid(1), base_ts(), deadline));
    let t_notice = sm.policy.deadline_drain_times(deadline).t_notice;
    let grace_s = sm.policy.abort_grace.as_micros() / 1_000_000;
    let now = secs(60);
    // The longest runtime that still lands exactly on T_notice, and one past.
    let fits = (t_notice.duration_since(now).as_micros() / 1_000_000) - grace_s;
    for (job, runtime) in [(2, fits + 1), (3, fits)] {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(job),
                cpu(1_000),
                Some(runtime),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
    }
    let proposal = schedule(&sm, now);
    let jobs: Vec<_> = proposal.placements.iter().map(|p| p.job).collect();
    assert_eq!(
        jobs,
        vec![jid(3)],
        "admitted iff now + max_runtime + grace <= T_notice"
    );
    let minted = commit(&mut sm, &proposal).expect("apply admits what the pass proposed");
    assert_eq!(sm.attempts[&minted[0].0].drain_cutoff, Some(t_notice));
}

#[test]
fn unbounded_running_work_leaves_a_drained_node_nothing_to_backfill() {
    let mut sm = drained_with_anchor(None);
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(1_000),
            Some(300),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    assert!(schedule(&sm, secs(60)).is_empty());
}

#[test]
fn an_empty_drained_node_takes_nothing() {
    let mut sm = setup(cpu(32_000), 4);
    apply_ok(&mut sm, drain_cmd(nid(1), base_ts()));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(1_000),
            Some(300),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    assert!(schedule(&sm, secs(60)).is_empty());
}

/// A bounded job too big for the drained node's free capacity now is not
/// left accruing there: backfill is fully funded or not placed at all.
#[test]
fn no_accrual_opens_on_a_drained_node() {
    let mut sm = drained_with_anchor(Some(3_600));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(30_000),
            Some(300),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    assert!(schedule(&sm, secs(60)).is_empty());
}

/// Backfill's cutoff is the horizon itself, so seating it leaves the horizon
/// exactly where it was, and later passes keep refusing work that would
/// overrun it — however many backfill seats have landed.
#[test]
fn backfill_cannot_move_the_horizon_later() {
    let mut sm = drained_with_anchor(Some(3_600));
    let before = sm.node_horizon(nid(1));
    assert_eq!(before, coppice_state::NodeHorizon::Bounded(secs(3_630)));
    for n in 2..=4 {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(n),
                cpu(4_000),
                Some(1_800),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
    }
    let proposal = schedule(&sm, secs(60));
    assert_eq!(proposal.placements.len(), 3);
    commit(&mut sm, &proposal).expect("applies");
    assert_eq!(sm.node_horizon(nid(1)), before);

    // At 1900 s a 1800 s job would end at 3730 > 3630: refused, though the
    // node has room.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(5),
            cpu(1_000),
            Some(1_800),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    assert!(schedule(&sm, secs(1_900)).is_empty());
}

/// An open node still wins a bounded job's free fit; the drained node takes
/// what the open one cannot.
#[test]
fn open_nodes_are_tried_before_drain_backfill() {
    let mut sm = drained_with_anchor(Some(3_600));
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(4_000), base_ts()));
    for n in 2..=3 {
        apply_ok(
            &mut sm,
            submit_cmd(
                jid(n),
                cpu(4_000),
                Some(1_800),
                PriorityMultiplier::ONE,
                base_ts(),
            ),
        );
    }
    let proposal = schedule(&sm, secs(60));
    let nodes: BTreeSet<NodeId> = proposal.placements.iter().map(|p| p.node).collect();
    assert_eq!(nodes, BTreeSet::from([nid(1), nid(2)]));
    commit(&mut sm, &proposal).expect("applies");
}
