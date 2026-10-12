//! Spot-only nodes (ADR 0044 §4): the candidate filter that keeps work that
//! is not effectively preemptible off `preemptible_only` nodes, and the
//! tie-break that steers effectively preemptible work onto them — with every
//! proposal committed through real apply, which enforces the same gate.

mod common;

use common::*;

use coppice_core::preemption::PreemptionSpec;
use coppice_core::quota::{CostUnits, PriorityMultiplier};

use coppice_scheduler::{HeuristicScheduler, PlacementProposal, Scheduler};
use coppice_state::command::ConfigureQuotaEntity;
use coppice_state::{Command, StateMachine};

fn schedule(sm: &StateMachine) -> PlacementProposal {
    HeuristicScheduler::default().schedule(sm, ts(TS_US + 1))
}

fn submit(sm: &mut StateMachine, n: u128, cpu_millis: u64, preemptible: bool) {
    apply_ok(
        sm,
        submit_cmd_with(
            jid(n),
            cpu(cpu_millis),
            Some(600),
            PriorityMultiplier::ONE,
            base_ts(),
            PreemptionSpec {
                preemptible,
                ..PreemptionSpec::default()
            },
        ),
    );
}

/// A cluster with no ordinary node: just a spot node `nid(2)`.
fn spot_only_cluster() -> StateMachine {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
    apply_ok(
        &mut sm,
        register_node_cmd_with(nid(2), cpu(10_000), base_ts(), true),
    );
    sm
}

/// Commit a proposal through apply; the gate there must agree.
fn commit(sm: &mut StateMachine, proposal: &PlacementProposal) {
    let cmd = proposal.to_commit_placements(&mut minter());
    apply_ok(sm, Command::CommitPlacements(cmd));
}

#[test]
fn work_that_is_not_preemptible_never_lands_on_a_spot_node() {
    let mut sm = spot_only_cluster();
    submit(&mut sm, 1, 1_000, false);
    let proposal = schedule(&sm);
    assert!(
        proposal.placements.is_empty(),
        "no seat and no accrual on a spot-only node: {proposal:?}"
    );

    // A persistent node appears: the job goes there.
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(10_000), base_ts()));
    let proposal = schedule(&sm);
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].node, nid(1));
    commit(&mut sm, &proposal);
}

#[test]
fn a_preemptible_job_takes_the_spot_node_on_a_tie() {
    // nid(1) ordinary, nid(2) spot, identical and empty: an exact tie on
    // every placement key, which would otherwise go to the lower NodeId.
    let mut sm = spot_only_cluster();
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(10_000), base_ts()));
    submit(&mut sm, 1, 2_000, true);
    submit(&mut sm, 2, 2_000, false);
    let proposal = schedule(&sm);
    let node_of = |n: u128| {
        proposal
            .placements
            .iter()
            .find(|p| p.job == jid(n))
            .map(|p| p.node)
    };
    assert_eq!(node_of(1), Some(nid(2)), "preemptible work prefers spot");
    assert_eq!(
        node_of(2),
        Some(nid(1)),
        "the rest keeps persistent capacity"
    );
    commit(&mut sm, &proposal);
    let attempt = sm.jobs[&jid(1)].current_attempt().unwrap();
    assert!(sm.attempts[&attempt].effective_preemptible);
}

#[test]
fn the_tie_break_is_not_a_bonus() {
    // The spot node is the looser fit: best fit still wins, so a
    // preemptible job that fits snugly on persistent capacity stays there.
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(4_000), base_ts()));
    apply_ok(
        &mut sm,
        register_node_cmd_with(nid(2), cpu(16_000), base_ts(), true),
    );
    submit(&mut sm, 1, 4_000, true);
    let proposal = schedule(&sm);
    assert_eq!(proposal.placements.len(), 1);
    assert_eq!(proposal.placements[0].node, nid(1));
}

#[test]
fn an_entity_switched_off_keeps_its_jobs_off_spot_nodes() {
    let mut sm = spot_only_cluster();
    apply_ok(
        &mut sm,
        Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity: ROOT,
            parent: None,
            name: format!("e{}", ROOT.0.simple()),
            quota: CostUnits(1_000_000_000_000),
            updated_at: base_ts(),
            actor: None,
            preemptible: Some(false),
        }),
    );
    submit(&mut sm, 1, 1_000, true);
    assert!(schedule(&sm).placements.is_empty());
}
