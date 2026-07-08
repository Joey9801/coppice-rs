//! The scheduler↔apply contract: every proposal the scheduler emits against a
//! snapshot applies with zero rejections, its `expect_funded` predictions match
//! the resulting allocation states, and repeated passes reach a fixpoint. Plus
//! the staleness path (a stale proposal is rejected, a fresh one is accepted)
//! and the 1M-scale throughput budget.

mod common;

use common::*;

use coppice_core::allocation::AllocationState;
use coppice_core::id::AllocationId;
use coppice_core::job::Job;
use coppice_core::quota::PriorityMultiplier;
use coppice_core::resource::Resources;
use coppice_state::command::SubmitJob;
use coppice_state::{Command, RejectionReason, StateMachine};

use coppice_scheduler::{HeuristicScheduler, PlacementProposal, Scheduler, SchedulerConfig};
use coppice_testkit::synth::{synth_state, SynthConfig};

use proptest::prelude::*;

fn schedule(sm: &StateMachine, now_us: i64) -> PlacementProposal {
    HeuristicScheduler::default().schedule(sm, now_us)
}

/// Apply a proposal via a real command, returning the minted allocation ids in
/// placement order.
fn apply_proposal(
    sm: &mut StateMachine,
    proposal: &PlacementProposal,
    mint: &mut dyn FnMut() -> (coppice_core::id::AttemptId, AllocationId),
) -> Result<Vec<AllocationId>, RejectionReason> {
    let mut minted = Vec::new();
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids.1);
        ids
    });
    sm.apply(&Command::CommitPlacements(cmd))?;
    Ok(minted)
}

// ---- scenario generator ----

#[derive(Debug, Clone)]
struct JobGen {
    requests: Resources,
    max_runtime_s: Option<u64>,
    multiplier_n: u64,
    seed_running: bool,
}

#[derive(Debug, Clone)]
struct Scenario {
    nodes: Vec<Resources>,
    accrual_limit: u32,
    jobs: Vec<JobGen>,
}

fn resources_strategy() -> impl Strategy<Value = Resources> {
    // Every job request is bounded strictly below the smallest node capacity,
    // so a request always fits some node's total capacity (apply never rejects
    // for RequestExceedsNodeCapacity).
    (
        250u64..16_000,
        (256u64 << 20)..(16 << 30),
        0u64..(200 << 30),
    )
        .prop_map(|(cpu_millis, memory_bytes, disk_bytes)| Resources {
            cpu_millis,
            memory_bytes,
            disk_bytes,
        })
}

fn node_strategy() -> impl Strategy<Value = Resources> {
    (
        16_000u64..64_000,
        (16u64 << 30)..(64 << 30),
        (200u64 << 30)..(1000 << 30),
    )
        .prop_map(|(cpu_millis, memory_bytes, disk_bytes)| Resources {
            cpu_millis,
            memory_bytes,
            disk_bytes,
        })
}

fn job_strategy() -> impl Strategy<Value = JobGen> {
    (
        resources_strategy(),
        prop::option::of(300u64..86_400),
        1u64..6,
        any::<bool>(),
    )
        .prop_map(
            |(requests, max_runtime_s, multiplier_n, seed_running)| JobGen {
                requests,
                max_runtime_s,
                multiplier_n,
                seed_running,
            },
        )
}

fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (
        prop::collection::vec(node_strategy(), 1..4),
        1u32..5,
        prop::collection::vec(job_strategy(), 1..30),
    )
        .prop_map(|(nodes, accrual_limit, jobs)| Scenario {
            nodes,
            accrual_limit,
            jobs,
        })
}

/// Materialize a scenario into a legal `StateMachine` via real commands.
///
/// A subset of jobs is seeded as running (funded on the first node while free
/// capacity lasts) so later passes see consumed capacity and guaranteed
/// release events; the rest stay queued.
fn build_state(s: &Scenario) -> StateMachine {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(s.accrual_limit)));
    for (i, cap) in s.nodes.iter().enumerate() {
        apply_ok(&mut sm, register_node_cmd(nid((i + 1) as u128), *cap, TS));
    }

    let first_cap = s.nodes[0];
    let mut used_on_first = Resources::ZERO;
    for (i, job) in s.jobs.iter().enumerate() {
        let job_id = jid((i + 1) as u128);
        let multiplier = PriorityMultiplier(job.multiplier_n << 32);
        apply_ok(
            &mut sm,
            submit_cmd(job_id, job.requests, job.max_runtime_s, multiplier, TS),
        );
        if job.seed_running {
            let after = used_on_first.saturating_add(&job.requests);
            // Seed only when it funds fully on the first node — a partial seed
            // would be a fresh accrual, and stacking those past K would make
            // the seeding command itself illegal.
            if after.fits_within(&first_cap) {
                let attempt = aid(1_000 + i as u128);
                let alloc = alid(1_000 + i as u128);
                apply_ok(
                    &mut sm,
                    place_cmd(placement(job_id, attempt, alloc, nid(1), job.requests), TS),
                );
                if sm.allocations[&alloc].allocation.state == AllocationState::Funded {
                    apply_ok(&mut sm, dispatch_cmd(attempt, TS));
                    apply_ok(&mut sm, started_cmd(attempt, TS));
                    used_on_first = after;
                }
            }
        }
    }
    sm
}

/// Whether a freshly-placed allocation is fully funded (as opposed to accruing)
/// — the only two states a placement can produce.
fn is_funded(state: AllocationState) -> bool {
    match state {
        AllocationState::Funded => true,
        AllocationState::Accruing => false,
        // A placement never lands directly in Active/Released.
        other => panic!("unexpected fresh allocation state {other:?}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// The keystone: schedule → commit → apply is rejection-free, the funding
    /// predictions hold, and iterated passes converge to an empty proposal.
    #[test]
    fn schedule_then_apply_never_rejects_and_reaches_a_fixpoint(scenario in scenario_strategy()) {
        let mut sm = build_state(&scenario);
        let mut mint = minter();
        let mut now = TS + 10;
        let mut converged = false;
        for _ in 0..64 {
            let proposal = schedule(&sm, now);
            if proposal.is_empty() {
                converged = true;
                break;
            }
            let expect: Vec<bool> = proposal.placements.iter().map(|p| p.expect_funded).collect();
            let minted = apply_proposal(&mut sm, &proposal, &mut mint)
                .map_err(|e| TestCaseError::fail(format!("proposal rejected: {e}")))?;
            // Predictions match the states apply actually produced.
            for (alloc, funded) in minted.iter().zip(expect) {
                let state = sm.allocations[alloc].allocation.state;
                prop_assert_eq!(funded, is_funded(state), "expect_funded mismatch for {}", alloc);
            }
            now += 1_000_000;
        }
        prop_assert!(converged, "the pass never reached a fixpoint");
    }
}

/// A handful of fixed `synth_state` seeds at ~2k jobs: one schedule→apply cycle
/// applies with zero rejections and matching funding predictions.
///
/// synth's live allocations oversubscribe their nodes, so a fresh roomy node is
/// added to give queued jobs and re-plannable accruals somewhere to land — the
/// cycle then exercises real free-fit packing and re-planning, not just an
/// empty pass.
#[test]
fn synth_seeds_apply_without_rejection() {
    let roomy = Resources {
        cpu_millis: 100_000_000,
        memory_bytes: 64 << 40,
        disk_bytes: 1_000 << 40,
    };
    for seed in 0..4u64 {
        let mut cfg = SynthConfig::with_jobs(2_000);
        cfg.seed = seed;
        let mut sm = synth_state(&cfg);
        // synth timestamps predate its base instant; schedule just after it.
        let now = 1_700_000_000_000_000 + 1_000_000;
        apply_ok(&mut sm, register_node_cmd(nid(0xB16_00DE), roomy, now));

        let proposal = schedule(&sm, now);
        assert!(
            !proposal.is_empty(),
            "seed {seed}: the roomy node admits queued work"
        );
        // Zero rejections is the bar here. (Funding predictions are exact only
        // on apply-built states; synth's direct construction oversubscribes
        // nodes, so a placement can accrue where the funding check saw room —
        // apply's own accept/reject uses that same check, so the batch still
        // commits cleanly. The apply-built proptest above pins expect_funded.)
        let mut mint = minter();
        apply_proposal(&mut sm, &proposal, &mut mint)
            .unwrap_or_else(|e| panic!("seed {seed}: proposal rejected: {e}"));
    }
}

#[test]
fn stale_proposal_is_rejected_then_a_fresh_one_applies() {
    // One node, one fitting job ⇒ the scheduler seats it on that node.
    let mut sm = setup(cpu(10_000), 4);
    let job = Job {
        id: jid(1),
        image: "img".into(),
        requests: cpu(4_000),
        priority: 0,
        max_runtime_us: Some(600_000_000),
        quota_entity: ROOT,
        retry: coppice_core::job::RetryPolicy::default(),
        abort_requested: None,
    };
    apply_ok(
        &mut sm,
        Command::SubmitJob(SubmitJob {
            job,
            multiplier: PriorityMultiplier::ONE,
            submitted_at_us: TS,
        }),
    );

    let proposal = schedule(&sm, TS + 1);
    assert_eq!(proposal.placements.len(), 1);
    let target = proposal.placements[0].node;

    // Interfere: drain the very node the proposal targets. The proposal is now
    // stale.
    apply_ok(&mut sm, drain_cmd(target, TS + 2));

    let mut stale_mint = minter();
    let err = apply_proposal(&mut sm, &proposal, &mut stale_mint)
        .expect_err("a placement on a drained node must be rejected");
    let reasons = batch_reasons(&err);
    assert!(
        matches!(err, RejectionReason::InvalidBatch(_)),
        "expected an all-or-nothing batch rejection, got {err}"
    );
    assert!(
        reasons
            .iter()
            .any(|r| matches!(r, RejectionReason::NodeNotSchedulable(n) if *n == target)),
        "expected NodeNotSchedulable for the drained node, got {reasons:?}"
    );

    // Re-scheduling against the new snapshot yields a proposal that applies (the
    // only node is drained, so it is empty — which applies cleanly).
    let fresh = schedule(&sm, TS + 3);
    assert!(fresh.is_empty(), "no schedulable node remains");
    let mut fresh_mint = minter();
    apply_proposal(&mut sm, &fresh, &mut fresh_mint).expect("a fresh proposal applies");
}

#[test]
#[ignore = "1M-scale; run in release"]
fn throughput_one_million_jobs_under_budget() {
    let cfg = SynthConfig::with_jobs(1_000_000);
    let sm = synth_state(&cfg);
    let now = 1_700_000_000_000_000 + 1_000_000;

    let scheduler = HeuristicScheduler::default();
    let start = std::time::Instant::now();
    let proposal = scheduler.schedule(&sm, now);
    let elapsed = start.elapsed();
    eprintln!(
        "schedule(1_000_000 jobs) took {elapsed:?}: {} placements, {} revocations",
        proposal.placements.len(),
        proposal.revocations.len()
    );

    let defaults = SchedulerConfig::default();
    // The placement cap bounds the batch; a lend can append at most K reseats
    // past the last seated candidate.
    assert!(
        proposal.placements.len()
            <= defaults.max_placements_per_cycle + sm.policy.accrual_limit as usize,
        "placements {} exceed the work cap",
        proposal.placements.len()
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "schedule took {elapsed:?}, over the 5 s budget"
    );

    // The batch must still be legal against the state it was computed on.
    let mut mint = minter();
    let mut check = sm.clone();
    apply_proposal(&mut check, &proposal, &mut mint).expect("the 1M-scale proposal applies");
}
