//! ADR 0045: quota-entity names are grammar-checked segments unique among
//! siblings (enforced at apply), name and parent are fixed at creation, and
//! paths are derived from the parent chain at read time and resolve back to
//! exactly one entity.

mod common;

use common::*;
use coppice_core::entity_ref::{QuotaEntityPath, QuotaEntityRef};
use coppice_core::id::QuotaEntityId;
use coppice_core::quota::{CostUnits, UsageState};
use coppice_state::command::ConfigureQuotaEntity;
use coppice_state::{Command, RejectionReason, StateMachine, QUOTA_TREE_DEPTH_CAP};

fn named(entity: QuotaEntityId, parent: Option<QuotaEntityId>, name: &str) -> Command {
    Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
        entity,
        parent,
        name: name.into(),
        quota: CostUnits(1_000_000),
        updated_at: base_ts(),
        actor: None,
    })
}

fn path(s: &str) -> QuotaEntityPath {
    s.parse().expect("fixture path")
}

/// `acme` → `eng` → {`platform`, `data`}, plus a second root `globex`.
fn tree() -> StateMachine {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, named(qid(1), None, "acme"));
    apply_ok(&mut sm, named(qid(2), Some(qid(1)), "eng"));
    apply_ok(&mut sm, named(qid(3), Some(qid(2)), "platform"));
    apply_ok(&mut sm, named(qid(4), Some(qid(2)), "data"));
    apply_ok(&mut sm, named(qid(5), None, "globex"));
    sm
}

#[test]
fn a_name_outside_the_segment_grammar_is_rejected_at_apply() {
    let mut sm = tree();
    let id_shaped = QuotaEntityId::new().to_string();
    for bad in ["", "a/b", "-x", "has space", id_shaped.as_str()] {
        let version = sm.version;
        let err = sm.apply(&named(qid(9), None, bad)).unwrap_err();
        assert!(
            matches!(err, RejectionReason::InvalidQuotaEntityName(_)),
            "{bad:?}: {err:?}"
        );
        // A rejection is a no-op beyond the version bump.
        assert!(!sm.quota_entities.contains_key(&qid(9)));
        assert_eq!(sm.version, version + 1);
    }
}

#[test]
fn a_sibling_clash_is_rejected_on_create() {
    let mut sm = tree();
    assert_eq!(
        sm.apply(&named(qid(9), Some(qid(2)), "platform"))
            .unwrap_err(),
        RejectionReason::QuotaEntityNameTaken {
            name: "platform".into(),
            holder: qid(3),
        }
    );
    assert!(!sm.quota_entities.contains_key(&qid(9)));
    // Names compare case-sensitively, and the same name under a different
    // parent is fine.
    apply_ok(&mut sm, named(qid(7), Some(qid(2)), "Platform"));
    apply_ok(&mut sm, named(qid(6), Some(qid(5)), "platform"));
    // Re-configuring an entity under its own name is not a clash with itself.
    apply_ok(&mut sm, named(qid(3), Some(qid(2)), "platform"));
}

#[test]
fn two_roots_may_not_share_a_name() {
    let mut sm = tree();
    assert_eq!(
        sm.apply(&named(qid(9), None, "acme")).unwrap_err(),
        RejectionReason::QuotaEntityNameTaken {
            name: "acme".into(),
            holder: qid(1),
        }
    );
}

#[test]
fn a_rename_is_rejected_and_changes_nothing() {
    let mut sm = tree();
    let before = sm.quota_entities[&qid(2)].clone();
    let version = sm.version;
    assert_eq!(
        sm.apply(&named(qid(2), Some(qid(1)), "engineering"))
            .unwrap_err(),
        RejectionReason::QuotaEntityImmutable(qid(2))
    );
    assert_eq!(sm.quota_entities[&qid(2)], before);
    assert_eq!(sm.version, version + 1);
    // Every path below it still names what it did.
    assert_eq!(
        sm.resolve_quota_entity_path(&path("acme/eng/platform")),
        Some(qid(3))
    );
    // Even into a name that would clash, or break the grammar: the entity
    // exists, so the only question is whether its name changes.
    for name in ["data", "plat/form"] {
        assert_eq!(
            sm.apply(&named(qid(3), Some(qid(2)), name)).unwrap_err(),
            RejectionReason::QuotaEntityImmutable(qid(3)),
            "{name}"
        );
    }
}

#[test]
fn a_move_is_rejected_and_changes_nothing() {
    let mut sm = tree();
    let before = sm.quota_entities[&qid(2)].clone();
    // Under another parent, to the root, and under its own descendant (the
    // move that was once a cycle) are all simply moves.
    for parent in [Some(qid(5)), None, Some(qid(3))] {
        assert_eq!(
            sm.apply(&named(qid(2), parent, "eng")).unwrap_err(),
            RejectionReason::QuotaEntityImmutable(qid(2)),
            "{parent:?}"
        );
        assert_eq!(sm.quota_entities[&qid(2)], before);
    }
    // A root cannot gain a parent either.
    assert_eq!(
        sm.apply(&named(qid(5), Some(qid(1)), "globex"))
            .unwrap_err(),
        RejectionReason::QuotaEntityImmutable(qid(5))
    );
    assert_eq!(
        sm.resolve_quota_entity_path(&path("acme/eng/data")),
        Some(qid(4))
    );
}

#[test]
fn a_quota_only_update_is_accepted_and_preserves_usage_and_created_at() {
    let mut sm = tree();
    // Give the entity a non-fresh accumulator and a creation instant the
    // update must not touch.
    let usage = UsageState::new(ts(1));
    sm.quota_entities.get_mut(&qid(3)).unwrap().usage = usage;
    let created_at = sm.quota_entities[&qid(3)].created_at;
    let later = ts(base_ts().as_micros() + 60_000_000);
    apply_ok(
        &mut sm,
        Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity: qid(3),
            parent: Some(qid(2)),
            name: "platform".into(),
            quota: CostUnits(42),
            updated_at: later,
            actor: None,
        }),
    );
    let e = &sm.quota_entities[&qid(3)];
    assert_eq!(e.quota, CostUnits(42));
    assert_eq!(e.usage, usage);
    assert_eq!(e.created_at, created_at);
    assert_eq!(e.updated_at, later);
    assert_eq!((e.name.as_str(), e.parent), ("platform", Some(qid(2))));
    assert_eq!(
        sm.quota_entity_path(qid(3)).as_deref(),
        Some("acme/eng/platform")
    );
}

#[test]
fn paths_are_derived_from_the_parent_chain() {
    let sm = tree();
    assert_eq!(sm.quota_entity_path(qid(1)).as_deref(), Some("acme"));
    assert_eq!(
        sm.quota_entity_path(qid(3)).as_deref(),
        Some("acme/eng/platform")
    );
    assert_eq!(sm.quota_entity_path(qid(99)), None);
}

#[test]
fn a_path_resolves_to_the_one_entity_it_names() {
    let sm = tree();
    assert_eq!(sm.resolve_quota_entity_path(&path("acme")), Some(qid(1)));
    assert_eq!(
        sm.resolve_quota_entity_path(&path("acme/eng/data")),
        Some(qid(4))
    );
    for missing in ["eng", "acme/platform", "acme/eng/platform/x", "ACME"] {
        assert_eq!(
            sm.resolve_quota_entity_path(&path(missing)),
            None,
            "{missing}"
        );
    }
    // Every entity's derived path resolves back to it.
    for id in sm.quota_entities.keys() {
        let p = path(&sm.quota_entity_path(*id).unwrap());
        assert_eq!(sm.resolve_quota_entity_path(&p), Some(*id));
    }
    // A ref: ids resolve to themselves unchecked, paths through the tree.
    let unknown = QuotaEntityId::new();
    assert_eq!(
        sm.resolve_quota_entity_ref(&QuotaEntityRef::Id(unknown)),
        Some(unknown)
    );
    assert_eq!(
        sm.resolve_quota_entity_ref(&QuotaEntityRef::Path(path("acme/eng"))),
        Some(qid(2))
    );
}

/// A chain of `n` entities `l1/l2/.../ln`, ids `base+1..=base+n`, under
/// `parent`.
fn chain(sm: &mut StateMachine, base: u128, parent: Option<QuotaEntityId>, n: u128) {
    let mut parent = parent;
    for k in 1..=n {
        apply_ok(sm, named(qid(base + k), parent, &format!("l{k}")));
        parent = Some(qid(base + k));
    }
}

#[test]
fn a_chain_exactly_at_the_cap_is_accepted_and_every_path_resolves_back() {
    let mut sm = StateMachine::default();
    let cap = QUOTA_TREE_DEPTH_CAP as u128;
    chain(&mut sm, 0, None, cap);
    for n in 1..=cap {
        let p = sm.quota_entity_path(qid(n)).unwrap();
        assert_eq!(p.split('/').count(), n as usize);
        assert_eq!(sm.resolve_quota_entity_path(&path(&p)), Some(qid(n)));
    }
    // A path longer than the cap can never name anything.
    let deepest = sm.quota_entity_path(qid(cap)).unwrap();
    assert_eq!(
        sm.resolve_quota_entity_path(&path(&format!("{deepest}/more"))),
        None
    );
}

#[test]
fn creating_one_level_past_the_cap_is_rejected() {
    let mut sm = StateMachine::default();
    let cap = QUOTA_TREE_DEPTH_CAP as u128;
    chain(&mut sm, 0, None, cap);
    let version = sm.version;
    assert_eq!(
        sm.apply(&named(qid(1000), Some(qid(cap)), "over"))
            .unwrap_err(),
        RejectionReason::QuotaEntityTooDeep(qid(1000))
    );
    assert!(!sm.quota_entities.contains_key(&qid(1000)));
    assert_eq!(sm.version, version + 1);
    // One level up there is room.
    apply_ok(&mut sm, named(qid(1000), Some(qid(cap - 1)), "sibling"));
}
