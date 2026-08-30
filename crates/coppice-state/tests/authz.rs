//! Apply-time authorization (ADR 0023), driven through the real apply path.
//!
//! `coppice_state::authz` has its own dense unit suite for the decision
//! table; these tests pin the *wiring*: that each actor-carrying command
//! consults the bindings in replicated state at its own log position, that
//! actor-less commands are untouched by any of it, that `UpdateAuthorization`
//! validates in its read-only phase before a byte moves, and that a
//! revocation resolves in log order.

mod common;

use common::*;
use coppice_core::job::RetryPolicy;
use coppice_state::authz::Role;
use coppice_state::{Applied, Command, Event, RejectionReason, StateMachine};

/// The canonical cluster plus a two-level entity tree under `ROOT`:
/// `team-a` with `squad` beneath it, and a sibling `team-b`.
const TEAM_A: u128 = 0xA1;
const SQUAD: u128 = 0xA2;
const TEAM_B: u128 = 0xB1;

fn tree_setup() -> StateMachine {
    let mut sm = setup();
    apply_ok(&mut sm, configure_entity_cmd(qid(TEAM_A), Some(ROOT)));
    apply_ok(&mut sm, configure_entity_cmd(qid(SQUAD), Some(qid(TEAM_A))));
    apply_ok(&mut sm, configure_entity_cmd(qid(TEAM_B), Some(ROOT)));
    sm
}

/// Install a bindings list with no authority check, the way a cluster is
/// bootstrapped: the first `UpdateAuthorization` is proposed with no actor.
fn install(sm: &mut StateMachine, bindings: Vec<coppice_state::authz::Binding>) {
    apply_ok(sm, update_authorization_cmd(bindings));
}

fn submit_to(job: u128, entity: coppice_core::id::QuotaEntityId) -> Command {
    let cmd = submit_cmd(jid(job), cpu(1_000), Some(60), RetryPolicy::default());
    match cmd {
        Command::SubmitJob(mut c) => {
            c.job.quota_entity = entity;
            Command::SubmitJob(c)
        }
        other => other,
    }
}

fn denied(reason: &RejectionReason) -> bool {
    matches!(reason, RejectionReason::PermissionDenied(_))
}

/// A command with no actor is an internal proposal: it applies exactly as it
/// did before authorization existed, even against a bindings list that would
/// deny everyone. This is the whole compatibility story for the scheduler,
/// ingestion, node lifecycle, and housekeeping proposers.
#[test]
fn actor_less_commands_are_unaffected_by_bindings() {
    let mut sm = tree_setup();
    // A list that grants nothing to anybody but the required unscoped admin.
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);

    apply_ok(&mut sm, submit_to(1, qid(SQUAD)));
    apply_ok(&mut sm, abort_cmd(jid(1), base_ts()));
    apply_ok(&mut sm, set_schedulable_cmd(nid(1), false));
    apply_ok(&mut sm, configure_entity_cmd(qid(0xC1), Some(ROOT)));
    apply_ok(&mut sm, update_policy_cmd(test_policy(5)));
    apply_ok(&mut sm, bump_version_cmd(2));
    assert!(sm.jobs[&jid(1)].state.is_terminal());
    assert_eq!(sm.cluster_version, 2);
}

/// Deny by default at apply: with an actor and no binding that matches it,
/// every actor-carrying command is refused `PermissionDenied` and leaves no
/// trace but the version bump.
#[test]
fn an_actor_with_no_binding_is_denied_every_verb() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let before = sm.clone();
    let ana = actor("ana");

    let commands = vec![
        with_actor(submit_to(1, qid(SQUAD)), ana.clone()),
        with_actor(set_schedulable_cmd(nid(1), false), ana.clone()),
        with_actor(
            configure_entity_cmd(qid(0xC1), Some(qid(TEAM_A))),
            ana.clone(),
        ),
        with_actor(update_policy_cmd(test_policy(5)), ana.clone()),
        with_actor(update_authorization_cmd(vec![]), ana.clone()),
        with_actor(bump_version_cmd(2), ana.clone()),
    ];
    for command in commands {
        let reason = sm.apply(&command).expect_err("deny by default");
        assert!(denied(&reason), "{command:?} gave {reason:?}");
    }
    // Version counts applied entries, accepted or rejected; nothing else moved.
    assert_eq!(sm.version, before.version + 6);
    assert_eq!(
        StateMachine {
            version: before.version,
            ..sm.clone()
        },
        before
    );
}

/// A scoped submitter may submit under its subtree and nowhere else — the
/// grant at `team-a` reaches the `squad` grandchild and stops at `team-b`.
#[test]
fn a_scoped_submitter_submits_only_inside_its_subtree() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            group_binding("batch-users", Role::Submitter, Some(qid(TEAM_A))),
        ],
    );
    let ana = actor_in("ana", &["batch-users"]);

    apply_ok(&mut sm, with_actor(submit_to(1, qid(SQUAD)), ana.clone()));
    let reason = sm
        .apply(&with_actor(submit_to(2, qid(TEAM_B)), ana.clone()))
        .expect_err("team-b is outside the subtree");
    assert!(denied(&reason), "{reason:?}");
    assert!(!sm.jobs.contains_key(&jid(2)));
}

/// `SubmitJob` stamps ownership from the *verified* actor, never from the
/// payload — and leaves it unset when the command carries no actor.
#[test]
fn submit_stamps_submitted_by_from_the_actor() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![group_binding("batch-users", Role::Admin, None)],
    );
    let ana = actor_in("ana", &["batch-users"]);

    // A client-supplied `submitted_by` is overwritten by the real principal.
    let mut spoofed = submit_to(1, ROOT);
    if let Command::SubmitJob(c) = &mut spoofed {
        c.job.submitted_by = Some("someone-else".into());
    }
    apply_ok(&mut sm, with_actor(spoofed, ana.clone()));
    assert_eq!(sm.jobs[&jid(1)].spec.submitted_by.as_deref(), Some("ana"));

    apply_ok(&mut sm, submit_to(2, ROOT));
    assert_eq!(sm.jobs[&jid(2)].spec.submitted_by, None);
}

/// An idempotent resubmission creates nothing and never rewrites
/// `submitted_by`, but it is still an actor-carrying command: authority is
/// re-checked at its own log position (ADR 0023), so a retry racing a
/// revocation rejects instead of silently succeeding.
#[test]
fn idempotent_resubmission_reauthorizes_at_its_own_log_position() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            group_binding("batch-users", Role::Submitter, None),
        ],
    );
    let ana = actor_in("ana", &["batch-users"]);
    apply_ok(&mut sm, with_actor(submit_to(1, ROOT), ana.clone()));

    // While the grant stands, the identical retry is an accepted no-op.
    let applied = sm
        .apply(&with_actor(submit_to(1, ROOT), ana.clone()))
        .expect("a matching resubmission under a live grant is an accepted no-op");
    assert_eq!(applied, Applied::default());

    // A principal holding nothing is refused even the no-op.
    let reason = sm
        .apply(&with_actor(submit_to(1, ROOT), actor("interloper")))
        .expect_err("the no-op still takes submit authority");
    assert!(denied(&reason), "{reason:?}");

    // Revoke the group's grant: ana's own identical retry now rejects at
    // apply — the deterministic revocation guarantee applies to no-ops too.
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let reason = sm
        .apply(&with_actor(submit_to(1, ROOT), ana))
        .expect_err("an identical retry after revocation is rejected");
    assert!(denied(&reason), "{reason:?}");

    // The original commit — and its owner — stand untouched throughout.
    assert_eq!(sm.jobs[&jid(1)].spec.submitted_by.as_deref(), Some("ana"));
}

/// Ownership is read from replicated state at apply: a principal aborts its
/// own job with no binding at all, and someone else's only as operator.
#[test]
fn abort_honors_ownership_then_falls_back_to_operator() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            principal_binding("ana", Role::Submitter, Some(ROOT)),
            principal_binding("bo", Role::Operator, Some(ROOT)),
        ],
    );
    let ana = actor("ana");
    apply_ok(&mut sm, with_actor(submit_to(1, ROOT), ana.clone()));
    apply_ok(&mut sm, with_actor(submit_to(2, ROOT), ana.clone()));

    // A submitter with no ownership cannot abort someone else's job...
    let reason = sm
        .apply(&with_actor(abort_cmd(jid(1), base_ts()), actor("carol")))
        .expect_err("carol holds nothing");
    assert!(denied(&reason), "{reason:?}");
    // ...its owner can, with only a submitter binding...
    apply_ok(&mut sm, with_actor(abort_cmd(jid(1), base_ts()), ana));
    // ...and an operator over the entity can abort anyone's.
    apply_ok(
        &mut sm,
        with_actor(abort_cmd(jid(2), base_ts()), actor("bo")),
    );
    assert!(sm.jobs[&jid(1)].state.is_terminal());
    assert!(sm.jobs[&jid(2)].state.is_terminal());
}

/// Drain is a cluster verb: an unscoped operator holds it, a subtree-scoped
/// admin does not, however wide the subtree.
#[test]
fn drain_takes_an_unscoped_binding() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            principal_binding("ops", Role::Operator, None),
            principal_binding("lead", Role::Admin, Some(ROOT)),
        ],
    );
    let reason = sm
        .apply(&with_actor(
            set_schedulable_cmd(nid(1), false),
            actor("lead"),
        ))
        .expect_err("a scoped admin holds no cluster verb");
    assert!(denied(&reason), "{reason:?}");
    assert!(sm.nodes[&nid(1)].node.schedulable);

    apply_ok(
        &mut sm,
        with_actor(set_schedulable_cmd(nid(1), false), actor("ops")),
    );
    assert!(!sm.nodes[&nid(1)].node.schedulable);
}

/// An unknown node still rejects `UnknownNode` before the authorization
/// check, so an actor-carrying command's rejections stay in the catalog's
/// documented order.
#[test]
fn drain_of_an_unknown_node_rejects_before_authorization() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("ops", Role::Admin, None)]);
    let reason = sm
        .apply(&with_actor(
            set_schedulable_cmd(nid(99), false),
            actor("nobody"),
        ))
        .expect_err("unknown node");
    assert!(
        matches!(reason, RejectionReason::UnknownNode(_)),
        "{reason:?}"
    );
}

/// A subtree-scoped admin reshapes their subtree and only their subtree:
/// creating under it works, creating under a sibling does not, and moving an
/// entity out of it does not either.
#[test]
fn scoped_admins_configure_only_inside_their_subtree() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            principal_binding("lead", Role::Admin, Some(qid(TEAM_A))),
        ],
    );
    let lead = actor("lead");

    apply_ok(
        &mut sm,
        with_actor(
            configure_entity_cmd(qid(0xC1), Some(qid(SQUAD))),
            lead.clone(),
        ),
    );
    let reason = sm
        .apply(&with_actor(
            configure_entity_cmd(qid(0xC2), Some(qid(TEAM_B))),
            lead.clone(),
        ))
        .expect_err("team-b is another subtree");
    assert!(denied(&reason), "{reason:?}");
    // Moving squad out from under team-a carries authority with it.
    let reason = sm
        .apply(&with_actor(
            configure_entity_cmd(qid(SQUAD), Some(qid(TEAM_B))),
            lead,
        ))
        .expect_err("a cross-subtree move takes unscoped admin");
    assert!(denied(&reason), "{reason:?}");
    assert_eq!(sm.quota_entities[&qid(SQUAD)].parent, Some(qid(TEAM_A)));
}

/// Operator certificates are an implicit unscoped admin outside the bindings
/// list — the break-glass that survives an empty or hostile list.
#[test]
fn an_operator_certificate_holds_every_verb() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let cert = operator_cert("ops-1");

    apply_ok(&mut sm, with_actor(submit_to(1, qid(SQUAD)), cert.clone()));
    apply_ok(
        &mut sm,
        with_actor(abort_cmd(jid(1), base_ts()), cert.clone()),
    );
    apply_ok(
        &mut sm,
        with_actor(set_schedulable_cmd(nid(1), false), cert.clone()),
    );
    apply_ok(
        &mut sm,
        with_actor(update_policy_cmd(test_policy(5)), cert.clone()),
    );
    apply_ok(&mut sm, with_actor(bump_version_cmd(2), cert.clone()));
    apply_ok(
        &mut sm,
        with_actor(
            update_authorization_cmd(vec![principal_binding("root", Role::Admin, None)]),
            cert,
        ),
    );
}

// ---- UpdateAuthorization ----

/// The happy path: an unscoped admin replaces the list wholesale, and the
/// accepted apply emits exactly one `AuthorizationUpdated`.
#[test]
fn update_authorization_replaces_the_list() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);

    let next = vec![
        principal_binding("root", Role::Admin, None),
        group_binding("batch-users", Role::Submitter, Some(qid(TEAM_A))),
    ];
    let applied = apply_ok(
        &mut sm,
        with_actor(update_authorization_cmd(next.clone()), actor("root")),
    );
    assert_eq!(applied.events, vec![Event::AuthorizationUpdated]);
    assert_eq!(sm.bindings, next);
}

/// Only an UNSCOPED admin may edit bindings: a scoped admin cannot, which is
/// exactly the delegated-binding-management case ADR 0023 defers.
#[test]
fn update_authorization_refuses_a_scoped_admin() {
    let mut sm = tree_setup();
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            principal_binding("lead", Role::Admin, Some(qid(TEAM_A))),
        ],
    );
    let before = sm.bindings.clone();
    let reason = sm
        .apply(&with_actor(
            update_authorization_cmd(vec![principal_binding("lead", Role::Admin, None)]),
            actor("lead"),
        ))
        .expect_err("a scoped admin may not touch bindings");
    assert!(denied(&reason), "{reason:?}");
    assert_eq!(sm.bindings, before);
}

/// An empty subject names nobody. It is refused `InvalidAuthorization`
/// before any mutation, for both subject kinds.
#[test]
fn update_authorization_refuses_an_empty_subject() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let before = sm.bindings.clone();

    for empty in [
        group_binding("", Role::Submitter, None),
        principal_binding("", Role::Operator, None),
    ] {
        let reason = sm
            .apply(&with_actor(
                update_authorization_cmd(vec![
                    principal_binding("root", Role::Admin, None),
                    empty.clone(),
                ]),
                actor("root"),
            ))
            .expect_err("an empty subject names nobody");
        assert!(
            matches!(reason, RejectionReason::InvalidAuthorization(_)),
            "{reason:?}"
        );
        assert_eq!(sm.bindings, before);
    }
}

/// Every scope must reference an existing quota entity — a binding scoped to
/// nothing would be silently inert.
#[test]
fn update_authorization_refuses_an_unknown_scope() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let before = sm.bindings.clone();
    let ghost = qid(0xDEAD);

    let reason = sm
        .apply(&with_actor(
            update_authorization_cmd(vec![
                principal_binding("root", Role::Admin, None),
                group_binding("batch-users", Role::Submitter, Some(ghost)),
            ]),
            actor("root"),
        ))
        .expect_err("scope must exist");
    assert!(
        matches!(reason, RejectionReason::UnknownQuotaEntity(e) if e == ghost),
        "{reason:?}"
    );
    assert_eq!(sm.bindings, before);
}

/// The lockout guard: the resulting list must retain at least one unscoped
/// admin. Operator certificates make lockout recoverable, but they are
/// outside the list by design and so do not count here.
#[test]
fn update_authorization_refuses_a_lockout() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let before = sm.bindings.clone();

    for attempt in [
        // Empty list.
        vec![],
        // Admins, but every one of them scoped.
        vec![principal_binding("lead", Role::Admin, Some(qid(TEAM_A)))],
        // Unscoped, but below admin.
        vec![principal_binding("ops", Role::Operator, None)],
    ] {
        let reason = sm
            .apply(&with_actor(
                update_authorization_cmd(attempt),
                actor("root"),
            ))
            .expect_err("an empty admin list is almost always an accident");
        assert!(
            matches!(reason, RejectionReason::AuthorizationLockout),
            "{reason:?}"
        );
        assert_eq!(sm.bindings, before);
    }

    // An operator certificate cannot buy its way past the guard either: the
    // check is on the resulting list, not on who proposed it.
    let reason = sm
        .apply(&with_actor(
            update_authorization_cmd(vec![]),
            operator_cert("ops-1"),
        ))
        .expect_err("certs are outside the list");
    assert!(
        matches!(reason, RejectionReason::AuthorizationLockout),
        "{reason:?}"
    );
}

/// The read-only phase runs in the catalog's order, so an unauthorized actor
/// is refused `PermissionDenied` even when the payload is also malformed —
/// authority is decided before the payload is inspected.
#[test]
fn update_authorization_checks_authority_before_shape() {
    let mut sm = tree_setup();
    install(&mut sm, vec![principal_binding("root", Role::Admin, None)]);
    let reason = sm
        .apply(&with_actor(
            update_authorization_cmd(vec![group_binding("", Role::Submitter, None)]),
            actor("nobody"),
        ))
        .expect_err("unauthorized");
    assert!(denied(&reason), "{reason:?}");
}

/// The deterministic half of the revocation race (ADR 0023): a command
/// authorized under the old bindings is re-evaluated against the list as of
/// its OWN log position. Grant, act, revoke, act again — the second act is
/// rejected on every replica, by log order alone.
#[test]
fn revocation_resolves_in_log_order() {
    let mut sm = tree_setup();
    let root = principal_binding("root", Role::Admin, None);
    let granted = vec![
        root.clone(),
        group_binding("batch-users", Role::Submitter, Some(qid(TEAM_A))),
    ];
    install(&mut sm, granted);
    let ana = actor_in("ana", &["batch-users"]);

    apply_ok(&mut sm, with_actor(submit_to(1, qid(SQUAD)), ana.clone()));

    // The revocation commits...
    apply_ok(
        &mut sm,
        with_actor(update_authorization_cmd(vec![root]), actor("root")),
    );

    // ...and the identical command, ordered after it, is now refused.
    let reason = sm
        .apply(&with_actor(submit_to(2, qid(SQUAD)), ana))
        .expect_err("the binding is gone as of this log position");
    assert!(denied(&reason), "{reason:?}");
    assert!(sm.jobs.contains_key(&jid(1)));
    assert!(!sm.jobs.contains_key(&jid(2)));
}

/// Bindings and the groups-claim policy survive a snapshot round-trip: a
/// replica restored from a snapshot must reach the same authorization
/// decisions as one that replayed the log.
#[test]
fn bindings_and_groups_claim_survive_a_snapshot_roundtrip() {
    use coppice_proto::convert::{state_from_records, state_to_records};

    let mut sm = tree_setup();
    let mut policy = test_policy(4);
    policy.groups_claim = "coppice_groups".into();
    apply_ok(&mut sm, update_policy_cmd(policy));
    install(
        &mut sm,
        vec![
            principal_binding("root", Role::Admin, None),
            group_binding("batch-users", Role::Submitter, Some(qid(TEAM_A))),
        ],
    );
    apply_ok(
        &mut sm,
        with_actor(submit_to(1, qid(SQUAD)), actor_in("ana", &["batch-users"])),
    );

    let rebuilt = state_from_records(state_to_records(&sm)).expect("records must rebuild");
    assert_eq!(rebuilt.bindings, sm.bindings);
    assert_eq!(rebuilt.policy.groups_claim, "coppice_groups");
    assert_eq!(
        rebuilt.jobs[&jid(1)].spec.submitted_by.as_deref(),
        Some("ana")
    );
    assert_eq!(rebuilt, sm);
}
