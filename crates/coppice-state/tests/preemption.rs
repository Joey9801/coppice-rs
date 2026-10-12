//! ADR 0044 slice 1 at apply: the `[preemption]` declarations, the reserved
//! `COPPICE_` env prefix, the quota entities' preemption switch, the policy
//! knobs, and effective preemptibility with its charge discount.

mod common;

use common::*;
use coppice_core::env::JobEnv;
use coppice_core::job::RetryPolicy;
use coppice_core::preemption::{NoticeSignal, PreemptionSpec};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Duration;
use coppice_state::command::ConfigureQuotaEntity;
use coppice_state::{Command, RejectionReason, StateMachine};

fn spec(preemptible: bool, notice_s: Option<i64>) -> PreemptionSpec {
    PreemptionSpec {
        preemptible,
        notice: notice_s.map(Duration::from_secs),
        notice_signal: NoticeSignal::Sigusr2,
    }
}

fn submit(job: u128, max_rt: Option<i64>, preemption: PreemptionSpec) -> Command {
    submit_cmd_full(
        jid(job),
        cpu(1_000),
        max_rt,
        RetryPolicy::default(),
        JobEnv::new(),
        preemption,
    )
}

fn set_switch(
    entity: coppice_core::id::QuotaEntityId,
    parent: Option<coppice_core::id::QuotaEntityId>,
    preemptible: Option<bool>,
) -> Command {
    Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
        entity,
        parent,
        name: entity_name(entity),
        quota: CostUnits(1_000_000_000_000),
        updated_at: base_ts(),
        actor: None,
        preemptible,
    })
}

/// Place job `n` (attempt `10n+1`, allocation `100n+11`) on node 1.
fn place(sm: &mut StateMachine, n: u128) {
    apply_ok(
        sm,
        place_cmd(
            placement(
                jid(n),
                aid(10 * n + 1),
                alid(100 * n + 11),
                nid(1),
                cpu(1_000),
            ),
            base_ts(),
        ),
    );
}

#[test]
fn an_absent_notice_resolves_to_the_policy_floor_at_apply() {
    let mut sm = setup();
    apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)));
    let stored = sm.jobs[&jid(1)].spec.preemption;
    assert_eq!(stored.notice, Some(Duration::from_secs(60)));
    assert!(stored.preemptible);
    assert_eq!(stored.notice_signal, NoticeSignal::Sigusr2);

    // A declared notice is stored as declared.
    apply_ok(&mut sm, submit(2, Some(3_600), spec(false, Some(120))));
    assert_eq!(
        sm.jobs[&jid(2)].spec.preemption.notice,
        Some(Duration::from_secs(120))
    );
}

#[test]
fn a_retry_leaving_the_notice_to_the_floor_matches_its_original() {
    let mut sm = setup();
    apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)));
    // The same submission again (ADR 0026): an accepted no-op, though the
    // stored spec now carries the resolved floor and the retry does not.
    assert!(apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)))
        .events
        .is_empty());
    // Spelling the floor out explicitly is the same submission too.
    assert!(
        apply_ok(&mut sm, submit(1, Some(3_600), spec(true, Some(60))))
            .events
            .is_empty()
    );
    // A different declaration under the same id is a mismatch.
    assert_eq!(
        sm.apply(&submit(1, Some(3_600), spec(false, None)))
            .unwrap_err(),
        RejectionReason::SubmitSpecMismatch(jid(1))
    );
}

#[test]
fn a_retry_after_the_floor_moves_still_matches_its_original() {
    let mut sm = setup();
    apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)));
    assert_eq!(
        sm.jobs[&jid(1)].spec.preemption.notice,
        Some(Duration::from_secs(60))
    );
    let mut raised = test_policy(4);
    raised.preemption_notice_floor = Duration::from_secs(90);
    apply_ok(&mut sm, update_policy_cmd(raised));

    // The unset notice means what it meant: the stored 60 s, not the new
    // floor. An accepted no-op.
    assert!(apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)))
        .events
        .is_empty());
    // An explicit, different notice is a different submission.
    assert_eq!(
        sm.apply(&submit(1, Some(3_600), spec(true, Some(90))))
            .unwrap_err(),
        RejectionReason::SubmitSpecMismatch(jid(1))
    );
    // A fresh job left to the floor gets the new one.
    apply_ok(&mut sm, submit(2, Some(3_600), spec(true, None)));
    assert_eq!(
        sm.jobs[&jid(2)].spec.preemption.notice,
        Some(Duration::from_secs(90))
    );
}

#[test]
fn a_notice_outside_the_policy_bounds_is_refused_at_apply() {
    let mut sm = setup();
    for notice_s in [59, 301] {
        let r = sm
            .apply(&submit(1, Some(3_600), spec(true, Some(notice_s))))
            .unwrap_err();
        assert!(
            matches!(r, RejectionReason::InvalidJobPreemption(_)),
            "notice {notice_s}: {r:?}"
        );
    }
    assert!(!sm.jobs.contains_key(&jid(1)));
    // The bounds are inclusive.
    apply_ok(&mut sm, submit(1, Some(3_600), spec(true, Some(300))));
}

#[test]
fn the_reserved_env_prefix_is_refused_at_apply() {
    let mut sm = setup();
    let env: JobEnv = [("COPPICE_ATTEMPT_ID".to_string(), "x".to_string())]
        .into_iter()
        .collect();
    let r = sm
        .apply(&submit_cmd_with_env(
            jid(1),
            cpu(1_000),
            None,
            RetryPolicy::default(),
            env,
        ))
        .unwrap_err();
    assert!(matches!(r, RejectionReason::InvalidJobEnv(_)), "{r:?}");
}

/// The discount folds into the recorded multiplier and the charge; a job
/// that is not effectively preemptible pays full price.
#[test]
fn an_effectively_preemptible_attempt_is_discounted_and_recorded() {
    let mut sm = setup();
    apply_ok(&mut sm, submit(1, Some(3_600), spec(true, None)));
    apply_ok(&mut sm, submit(2, Some(3_600), spec(false, None)));
    place(&mut sm, 1);
    place(&mut sm, 2);

    let discounted = &sm.attempts[&aid(11)];
    let full = &sm.attempts[&aid(21)];
    assert!(discounted.effective_preemptible);
    assert!(!full.effective_preemptible);
    // 1.0 × the default 0.75 discount, exactly.
    assert_eq!(discounted.multiplier, PriorityMultiplier(3 << 30));
    assert_eq!(full.multiplier, PriorityMultiplier::ONE);
    assert_eq!(discounted.charge.amount.0 * 4, full.charge.amount.0 * 3);
}

/// Both multipliers apply to an unbounded preemptible job, multiplying:
/// the default 2.0 surcharge and the default 0.75 discount make 1.5, exactly.
#[test]
fn the_discount_multiplies_with_the_unbounded_surcharge() {
    let mut sm = setup();
    apply_ok(&mut sm, submit(1, None, spec(true, None)));
    place(&mut sm, 1);
    let ar = &sm.attempts[&aid(11)];
    assert!(ar.effective_preemptible);
    assert_eq!(ar.multiplier, PriorityMultiplier(3 << 31));
}

/// An entity anywhere on the job's chain can switch preemption off, and the
/// verdict recorded at placement survives a later flip of the switch.
#[test]
fn an_entity_switch_governs_placement_and_never_reaches_a_placed_attempt() {
    let mut sm = setup();
    let leaf = qid(0x1EAF);
    apply_ok(&mut sm, set_switch(leaf, Some(ROOT), None));
    assert!(sm.quota_entities[&leaf].preemptible, "created switched on");

    // Root switched off: a preemptible job charging the leaf is not
    // effectively preemptible.
    apply_ok(&mut sm, set_switch(ROOT, None, Some(false)));
    let mut job = submit(1, Some(3_600), spec(true, None));
    if let Command::SubmitJob(s) = &mut job {
        s.job.quota_entity = leaf;
    }
    apply_ok(&mut sm, job);
    place(&mut sm, 1);
    assert!(!sm.attempts[&aid(11)].effective_preemptible);
    assert_eq!(sm.attempts[&aid(11)].multiplier, PriorityMultiplier::ONE);

    // Switching it back on reprices nothing already placed.
    apply_ok(&mut sm, set_switch(ROOT, None, Some(true)));
    assert!(!sm.attempts[&aid(11)].effective_preemptible);
    assert_eq!(sm.attempts[&aid(11)].multiplier, PriorityMultiplier::ONE);
    assert!(sm.effective_preemptible(&sm.jobs[&jid(1)].spec));
}

/// `preemptible: None` on a reconfigure leaves the switch as it is, so a
/// quota-only update never silently re-enables preemption.
#[test]
fn a_reconfigure_without_the_switch_leaves_it_alone() {
    let mut sm = setup();
    apply_ok(&mut sm, set_switch(ROOT, None, Some(false)));
    apply_ok(&mut sm, set_switch(ROOT, None, None));
    assert!(!sm.quota_entities[&ROOT].preemptible);
}

#[test]
fn update_policy_validates_the_preemption_knobs() {
    let mut sm = setup();
    let mut surcharge = test_policy(4);
    surcharge.preemptible_multiplier = PriorityMultiplier(PriorityMultiplier::ONE.0 + 1);
    let r = sm.apply(&update_policy_cmd(surcharge)).unwrap_err();
    assert!(matches!(r, RejectionReason::InvalidPolicy(_)), "{r:?}");

    let mut inverted = test_policy(4);
    inverted.preemption_notice_floor = Duration::from_mins(10);
    let r = sm.apply(&update_policy_cmd(inverted)).unwrap_err();
    assert!(matches!(r, RejectionReason::InvalidPolicy(_)), "{r:?}");

    // 1.0 (no discount) and a zero-width range are both legal.
    let mut edge = test_policy(4);
    edge.preemptible_multiplier = PriorityMultiplier::ONE;
    edge.preemption_notice_floor = Duration::from_secs(90);
    edge.preemption_notice_cap = Duration::from_secs(90);
    apply_ok(&mut sm, update_policy_cmd(edge));

    // A job left to the floor now gets the new floor.
    apply_ok(&mut sm, submit(1, Some(3_600), spec(false, None)));
    assert_eq!(
        sm.jobs[&jid(1)].spec.preemption.notice,
        Some(Duration::from_secs(90))
    );
}
