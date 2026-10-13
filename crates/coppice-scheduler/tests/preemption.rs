//! Priority preemption in the scheduler pass (ADR 0044 §4): victim
//! eligibility (class and score rules), victim order, the smallest-prefix
//! rule, node choice, the per-cycle cap, the per-node guard, the cooldown,
//! the minimum-improvement threshold, spot-only and draining gates, the
//! revoke-and-reseat path and the accrual cap. Every proposal is committed
//! through real apply, which re-validates each eviction group.

mod common;

use common::*;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, PreemptReason};
use coppice_core::id::{AllocationId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_scheduler::{
    HeuristicScheduler, PlacementProposal, ProposedPlacement, Scheduler, SchedulerConfig,
};
use coppice_state::command::RecordAttemptOutcome;
use coppice_state::{Command, StateMachine};

/// The class of every beneficiary here; victims run at class 0 unless a test
/// says otherwise.
const HIGH: i32 = 5;

fn schedule_at(sm: &StateMachine, now: Timestamp) -> PlacementProposal {
    HeuristicScheduler::default().schedule(sm, now)
}

fn schedule_with(sm: &StateMachine, cfg: SchedulerConfig, now: Timestamp) -> PlacementProposal {
    HeuristicScheduler::new(cfg).schedule(sm, now)
}

fn schedule(sm: &StateMachine) -> PlacementProposal {
    schedule_at(sm, ts(TS_US + 1))
}

/// Commit a proposal through real apply; returns the minted allocation ids in
/// placement order.
fn commit(sm: &mut StateMachine, proposal: &PlacementProposal) -> Vec<AllocationId> {
    // A fresh id range per commit: one test commits several batches into
    // one state machine.
    static BATCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let batch = BATCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut minted = Vec::new();
    let mut mint = minter_from((1 << 100) + u128::from(batch) * 1_000);
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids.1);
        ids
    });
    apply_ok(sm, Command::CommitPlacements(cmd));
    minted
}

/// A running preemptible victim of class 0 and multiplier 1, unbounded.
fn victim(sm: &mut StateMachine, n: u128, node: NodeId, req: Resources, started_at: Timestamp) {
    submit_full(sm, n, req, 0, true, 1, None);
    run_on(sm, n, node, started_at);
}

/// A queued higher-class, higher-score job that cannot itself be preempted.
fn beneficiary(sm: &mut StateMachine, n: u128, req: Resources) {
    submit_full(sm, n, req, HIGH, false, 3, Some(3600));
}

fn placement_of(p: &PlacementProposal, n: u128) -> &ProposedPlacement {
    p.placements
        .iter()
        .find(|pp| pp.job == jid(n))
        .unwrap_or_else(|| panic!("no placement for job {n}: {p:?}"))
}

fn preempting(p: &PlacementProposal) -> Vec<&ProposedPlacement> {
    p.placements
        .iter()
        .filter(|pp| !pp.preemptions.is_empty())
        .collect()
}

/// The `for_job` of the attempt's termination record, if it has one.
fn priority_record(sm: &StateMachine, attempt_n: u128) -> Option<coppice_core::id::JobId> {
    sm.attempts[&aid(attempt_n)]
        .preemption
        .and_then(|p| match p.reason {
            PreemptReason::Priority { for_job } => Some(for_job),
            _ => None,
        })
}

/// ADR 0044 §4: a queued higher-class job with no seat takes a full node's
/// capacity by evicting lower-class preemptible work; the beneficiary is
/// proposed accruing, apply creates each victim's `Priority` record.
#[test]
fn a_higher_class_job_preempts_lower_class_work_on_a_full_node() {
    let mut sm = setup(cpu(4_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(1), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));

    let proposal = schedule(&sm);
    assert_eq!(proposal.placements.len(), 1, "{proposal:?}");
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(1));
    assert!(!p.expect_funded, "a preemption always accrues");
    // Equal class, score and work at risk: the AllocationId breaks the tie.
    assert_eq!(p.preemptions, vec![alid(1)]);

    let minted = commit(&mut sm, &proposal);
    assert_eq!(
        sm.allocations[&minted[0]].allocation.state,
        AllocationState::Accruing
    );
    assert_eq!(priority_record(&sm, 1), Some(jid(10)));
    assert_eq!(priority_record(&sm, 2), None, "only the group is evicted");
}

/// ADR 0044 §4 class rule: a victim must be of strictly lower class than the
/// beneficiary; an equal class is never preempted.
#[test]
fn an_equal_class_job_is_not_preempted() {
    let mut sm = setup(cpu(2_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    // Same class as the victim, much higher score.
    submit_full(&mut sm, 10, cpu(2_000), 0, false, 3, Some(3600));

    let proposal = schedule(&sm);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), None);
}

/// ADR 0044 §4 score rule: of lower class is not enough; a victim whose
/// effective score is not below the beneficiary's is passed over.
#[test]
fn a_victim_with_a_higher_score_is_not_chosen() {
    let mut sm = setup(cpu(4_000), 4);
    // Lower class but a 100x multiplier: score far above the beneficiary's 3x.
    submit_full(&mut sm, 1, cpu(2_000), 0, true, 100, None);
    run_on(&mut sm, 1, nid(1), base_ts());
    victim(&mut sm, 2, nid(1), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));

    let proposal = schedule(&sm);
    let p = placement_of(&proposal, 10);
    assert_eq!(p.preemptions, vec![alid(2)], "the high-score job is spared");
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), None);
    assert_eq!(priority_record(&sm, 2), Some(jid(10)));

    // With only high-score victims there is no group at all.
    let mut sm = setup(cpu(2_000), 4);
    submit_full(&mut sm, 1, cpu(2_000), 0, true, 100, None);
    run_on(&mut sm, 1, nid(1), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);
}

/// ADR 0044 §4 victim order: class ascending first, however much work the
/// lower-class victim has at risk.
#[test]
fn a_lower_class_victim_goes_first_whatever_its_work_at_risk() {
    let mut sm = setup(cpu(4_000), 4);
    let now = ts(TS_US + 1_000_000_000);
    // Class 1, started recently (little work at risk), lowest AllocationId.
    submit_full(&mut sm, 1, cpu(2_000), 1, true, 1, None);
    run_on(&mut sm, 1, nid(1), ts(TS_US + 990_000_000));
    // Class 0, running since the start of time (lots of work at risk).
    submit_full(&mut sm, 2, cpu(2_000), 0, true, 1, None);
    run_on(&mut sm, 2, nid(1), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));

    let proposal = schedule_at(&sm, now);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(2)]);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 2), Some(jid(10)));
    assert_eq!(priority_record(&sm, 1), None);
}

/// ADR 0044 §4 victim order: within a class and score, the victim with the
/// least work at risk (started most recently) goes first.
#[test]
fn among_equals_the_victim_with_less_work_at_risk_goes_first() {
    let mut sm = setup(cpu(4_000), 4);
    let now = ts(TS_US + 1_000_000_000);
    // The lower AllocationId has run longest.
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(1), cpu(2_000), ts(TS_US + 900_000_000));
    beneficiary(&mut sm, 10, cpu(2_000));

    let proposal = schedule_at(&sm, now);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(2)]);
    commit(&mut sm, &proposal);
}

/// ADR 0044 §4: the smallest prefix of the victim order that fits the
/// beneficiary is evicted; no more.
#[test]
fn only_the_smallest_fitting_prefix_of_victims_is_evicted() {
    let mut sm = setup(cpu(6_000), 4);
    let now = ts(TS_US + 1_000_000_000);
    // Later start = less work at risk = earlier in the order: 3, 2, 1.
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(1), cpu(2_000), ts(TS_US + 500_000_000));
    victim(&mut sm, 3, nid(1), cpu(2_000), ts(TS_US + 900_000_000));
    beneficiary(&mut sm, 10, cpu(4_000));

    let proposal = schedule_at(&sm, now);
    assert_eq!(
        placement_of(&proposal, 10).preemptions,
        vec![alid(3), alid(2)]
    );
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), None);
    assert_eq!(priority_record(&sm, 2), Some(jid(10)));
    assert_eq!(priority_record(&sm, 3), Some(jid(10)));
}

/// ADR 0044 §4: a node where the beneficiary does not fit even with every
/// eligible victim released is skipped, however attractive its ordering.
#[test]
fn a_node_that_cannot_fit_even_with_every_victim_is_skipped() {
    let mut sm = setup(cpu(4_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(4_000), base_ts()));
    // Node 1: 3000 held by non-preemptible work, 1000 by a victim.
    submit_full(&mut sm, 1, cpu(3_000), 0, false, 1, None);
    run_on(&mut sm, 1, nid(1), base_ts());
    victim(&mut sm, 2, nid(1), cpu(1_000), base_ts());
    // Node 2: all victims.
    victim(&mut sm, 3, nid(2), cpu(2_000), base_ts());
    victim(&mut sm, 4, nid(2), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));

    let proposal = schedule(&sm);
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(2));
    assert_eq!(p.preemptions, vec![alid(3)]);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 2), None);
}

/// Two full single-victim nodes and two beneficiaries.
fn two_node_two_beneficiaries() -> StateMachine {
    let mut sm = setup(cpu(2_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(2_000), base_ts()));
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(2), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    beneficiary(&mut sm, 11, cpu(2_000));
    sm
}

/// ADR 0044 §4: the per-cycle cap bounds the number of preemption-funded
/// placements (beneficiaries, not victims).
#[test]
fn the_per_cycle_cap_limits_preempting_placements() {
    let mut sm = two_node_two_beneficiaries();
    let cfg = SchedulerConfig {
        max_preempting_placements_per_cycle: 1,
        ..SchedulerConfig::default()
    };
    let proposal = schedule_with(&sm, cfg, ts(TS_US + 1));
    assert_eq!(preempting(&proposal).len(), 1, "{proposal:?}");
    commit(&mut sm, &proposal);

    let mut sm = two_node_two_beneficiaries();
    let proposal = schedule(&sm);
    assert_eq!(preempting(&proposal).len(), 2, "{proposal:?}");
    let nodes: std::collections::BTreeSet<_> = proposal.placements.iter().map(|p| p.node).collect();
    assert_eq!(nodes.len(), 2, "one eviction group per node");
    commit(&mut sm, &proposal);
    assert!(priority_record(&sm, 1).is_some());
    assert!(priority_record(&sm, 2).is_some());
}

/// ADR 0044 §4 per-node guard: while a node's victims are still running
/// under a `Priority` record, no second eviction group is proposed there.
#[test]
fn a_guarded_node_hosts_no_second_eviction_group() {
    let mut sm = setup(cpu(2_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), Some(jid(10)));

    beneficiary(&mut sm, 11, cpu(2_000));
    let proposal = schedule_at(&sm, ts(TS_US + 2));
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);

    // Another eligible node is still open to the second beneficiary.
    let mut sm = setup(cpu(2_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(2_000), base_ts()));
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(2), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    let first_node = placement_of(&proposal, 10).node;
    commit(&mut sm, &proposal);
    beneficiary(&mut sm, 11, cpu(2_000));
    let proposal = schedule_at(&sm, ts(TS_US + 2));
    let groups = preempting(&proposal);
    assert_eq!(groups.len(), 1, "{proposal:?}");
    assert_ne!(groups[0].node, first_node);
    commit(&mut sm, &proposal);
}

/// ADR 0044 §4 cooldown: a node whose latest scheduler-initiated request is
/// younger than `preemption_cooldown` hosts no new victims, even when the
/// earlier attempts have long ended.
#[test]
fn the_cooldown_holds_a_node_after_its_victims_have_ended() {
    let mut sm = setup(cpu(6_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 2, nid(1), cpu(2_000), base_ts());
    victim(&mut sm, 3, nid(1), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(1)]);
    commit(&mut sm, &proposal);
    apply_ok(
        &mut sm,
        Command::RecordAttemptOutcome(RecordAttemptOutcome {
            attempt: aid(1),
            outcome: AttemptOutcome::Preempted {
                reason: PreemptReason::Priority { for_job: jid(10) },
            },
            actual_runtime: Duration::from_secs(10),
            observed_at: ts(TS_US + 2),
        }),
    );
    let requested = sm.preemption_ledger().latest_request[&nid(1)];
    let cooldown = SchedulerConfig::default().preemption_cooldown;

    beneficiary(&mut sm, 11, cpu(2_000));
    let early = requested + cooldown - Duration::from_micros(1);
    let proposal = schedule_at(&sm, early);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);

    let proposal = schedule_at(&sm, requested + cooldown);
    assert_eq!(preempting(&proposal).len(), 1, "{proposal:?}");
    commit(&mut sm, &proposal);
}

/// A two-node cluster: node 1 holds non-preemptible work bounded by
/// `bound_s`, node 2 holds an unbounded victim. The beneficiary's natural
/// accrual target is node 1.
fn natural_vs_preemption(bound_s: i64) -> (StateMachine, PlacementProposal) {
    let mut sm = setup(cpu(4_000), 4);
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(4_000), base_ts()));
    submit_full(&mut sm, 1, cpu(4_000), 0, false, 1, Some(bound_s));
    run_on(&mut sm, 1, nid(1), base_ts());
    victim(&mut sm, 2, nid(2), cpu(4_000), base_ts());
    beneficiary(&mut sm, 10, cpu(4_000));
    let proposal = schedule(&sm);
    (sm, proposal)
}

/// ADR 0044 §4: preemption is only worth a victim's progress when the
/// ordinary rules offer nothing, or something later by at least
/// `preemption_min_improvement` (1 h default).
#[test]
fn a_soon_enough_natural_start_is_not_worth_a_preemption() {
    let (mut sm, proposal) = natural_vs_preemption(600);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    assert_eq!(placement_of(&proposal, 10).node, nid(1));
    commit(&mut sm, &proposal);
}

/// The other side of the threshold: a natural start hours away is.
#[test]
fn a_distant_natural_start_is_worth_a_preemption() {
    let (mut sm, proposal) = natural_vs_preemption(3 * 3600);
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(2));
    assert_eq!(p.preemptions, vec![alid(2)]);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 2), Some(jid(10)));
}

/// ADR 0044 §4 spot-only gate: a job that is not effectively preemptible
/// cannot take a `preemptible_only` node, not even by evicting its victims.
#[test]
fn a_non_preemptible_job_cannot_preempt_onto_a_spot_node() {
    let build = |preemptible: bool| {
        let mut sm = StateMachine::default();
        apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
        apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
        apply_ok(
            &mut sm,
            register_node_cmd_with(nid(2), cpu(2_000), base_ts(), true),
        );
        victim(&mut sm, 1, nid(2), cpu(2_000), base_ts());
        submit_full(&mut sm, 10, cpu(2_000), HIGH, preemptible, 3, Some(3600));
        sm
    };
    let mut sm = build(false);
    let proposal = schedule(&sm);
    assert!(proposal.placements.is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);

    let mut sm = build(true);
    let proposal = schedule(&sm);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(1)]);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), Some(jid(10)));
}

/// ADR 0044 §4/§5: work on a draining node is never a victim; an open node
/// with victims is used instead.
#[test]
fn victims_on_a_draining_node_are_never_chosen() {
    let mut sm = setup(cpu(2_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    apply_ok(&mut sm, drain_cmd(nid(1), base_ts()));
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);

    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(2_000), base_ts()));
    victim(&mut sm, 2, nid(2), cpu(2_000), base_ts());
    let proposal = schedule(&sm);
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(2));
    assert_eq!(p.preemptions, vec![alid(2)]);
    commit(&mut sm, &proposal);
    assert_eq!(priority_record(&sm, 1), None);
}

/// ADR 0044 §4: a job already accruing indefinitely is revoked and reseated
/// where an eviction group can fund it.
#[test]
fn an_indefinitely_accruing_job_is_reseated_onto_an_eviction_group() {
    let mut sm = setup(cpu(2_000), 4);
    // Node 1: same-class work it cannot preempt.
    submit_full(&mut sm, 1, cpu(2_000), HIGH, true, 1, None);
    run_on(&mut sm, 1, nid(1), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    let minted = commit(&mut sm, &proposal);
    let old = minted[0];
    assert_eq!(
        sm.allocations[&old].allocation.state,
        AllocationState::Accruing
    );

    // Lower-class work appears on a new node.
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(2_000), base_ts()));
    victim(&mut sm, 2, nid(2), cpu(2_000), base_ts());
    let proposal = schedule_at(&sm, ts(TS_US + 2));
    assert_eq!(proposal.revocations, vec![old], "{proposal:?}");
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(2));
    assert_eq!(p.preemptions, vec![alid(2)]);
    let minted = commit(&mut sm, &proposal);
    assert_ne!(
        sm.allocations[&old].allocation.state,
        AllocationState::Accruing
    );
    assert_eq!(
        sm.allocations[&minted[0]].allocation.state,
        AllocationState::Accruing
    );
    assert_eq!(priority_record(&sm, 2), Some(jid(10)));
}

/// ADR 0044 §4, "recovery keeps precedence": the per-node guard pins a
/// beneficiary's accrual to its victims' node only while that node is open.
/// Once it drains, the accrual is re-planned off it by an ordinary
/// improvement move — but never into a second eviction group, since its
/// victims are still outstanding.
#[test]
fn a_guarded_accrual_on_a_draining_node_is_re_planned_off_it() {
    let mut sm = setup(cpu(2_000), 4);
    victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
    beneficiary(&mut sm, 10, cpu(2_000));
    let proposal = schedule(&sm);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(1)]);
    let minted = commit(&mut sm, &proposal);
    let accrual = minted[0];

    // Another node, full of bounded work that frees in 10 minutes, plus
    // preemptible work the beneficiary could otherwise evict there.
    apply_ok(&mut sm, register_node_cmd(nid(2), cpu(4_000), base_ts()));
    submit_full(&mut sm, 3, cpu(2_000), 0, false, 1, Some(600));
    run_on(&mut sm, 3, nid(2), base_ts());
    victim(&mut sm, 4, nid(2), cpu(2_000), base_ts());

    // Open, the guarded node keeps its accrual.
    let proposal = schedule_at(&sm, ts(TS_US + 2));
    assert!(proposal.revocations.is_empty(), "{proposal:?}");

    // Drained, it lets the accrual go to the finite bound elsewhere, with no
    // eviction group, and apply takes the plain revoke-and-reseat.
    apply_ok(&mut sm, drain_cmd(nid(1), ts(TS_US + 3)));
    let proposal = schedule_at(&sm, ts(TS_US + 4));
    assert_eq!(proposal.revocations, vec![accrual], "{proposal:?}");
    let p = placement_of(&proposal, 10);
    assert_eq!(p.node, nid(2));
    assert!(p.preemptions.is_empty(), "{proposal:?}");
    let minted = commit(&mut sm, &proposal);
    assert_eq!(
        sm.allocations[&minted[0]].allocation.state,
        AllocationState::Accruing
    );
    assert!(sm.attempts[&aid(4)].preemption.is_none());
}

/// ADR 0044 §4: a new accrual past the cap K is refused at apply, so a
/// preemption (always an accrual) is not proposed once K is reached.
#[test]
fn no_preemption_is_proposed_past_the_accrual_cap() {
    let build = |limit: u32| {
        let mut sm = setup(cpu(2_000), limit);
        apply_ok(&mut sm, register_node_cmd(nid(2), cpu(2_000), base_ts()));
        victim(&mut sm, 1, nid(1), cpu(2_000), base_ts());
        // Node 2: held by same-class work, with one other job accruing.
        submit_full(&mut sm, 2, cpu(2_000), HIGH, false, 1, None);
        run_on(&mut sm, 2, nid(2), base_ts());
        submit_full(&mut sm, 3, cpu(2_000), 0, false, 1, None);
        apply_ok(
            &mut sm,
            place_cmd(
                placement(jid(3), aid(3), alid(3), nid(2), cpu(2_000)),
                base_ts(),
            ),
        );
        assert_eq!(
            sm.allocations[&alid(3)].allocation.state,
            AllocationState::Accruing
        );
        beneficiary(&mut sm, 10, cpu(2_000));
        sm
    };
    let mut sm = build(1);
    let proposal = schedule(&sm);
    assert!(preempting(&proposal).is_empty(), "{proposal:?}");
    commit(&mut sm, &proposal);

    let mut sm = build(2);
    let proposal = schedule(&sm);
    assert_eq!(placement_of(&proposal, 10).preemptions, vec![alid(1)]);
    commit(&mut sm, &proposal);
}
