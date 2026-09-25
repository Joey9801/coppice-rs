//! ADR 0045: quota-entity names are grammar-checked segments unique among
//! siblings (enforced at apply), and paths are derived from the parent chain
//! at read time and resolve back to exactly one entity.

mod common;

use common::*;
use coppice_core::entity_ref::{QuotaEntityPath, QuotaEntityRef};
use coppice_core::id::QuotaEntityId;
use coppice_core::quota::CostUnits;
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
    // A rename into a bad name is refused the same way, and leaves the old one.
    let err = sm
        .apply(&named(qid(3), Some(qid(2)), "plat/form"))
        .unwrap_err();
    assert!(matches!(err, RejectionReason::InvalidQuotaEntityName(_)));
    assert_eq!(sm.quota_entities[&qid(3)].name, "platform");
}

#[test]
fn a_sibling_clash_is_rejected_on_create_rename_and_reparent() {
    let mut sm = tree();
    let taken = |holder| RejectionReason::QuotaEntityNameTaken {
        name: "platform".into(),
        holder,
    };
    // Create.
    assert_eq!(
        sm.apply(&named(qid(9), Some(qid(2)), "platform"))
            .unwrap_err(),
        taken(qid(3))
    );
    // Rename.
    assert_eq!(
        sm.apply(&named(qid(4), Some(qid(2)), "platform"))
            .unwrap_err(),
        taken(qid(3))
    );
    assert_eq!(sm.quota_entities[&qid(4)].name, "data");
    // Reparent: a `platform` elsewhere moving under `eng`.
    apply_ok(&mut sm, named(qid(6), Some(qid(5)), "platform"));
    assert_eq!(
        sm.apply(&named(qid(6), Some(qid(2)), "platform"))
            .unwrap_err(),
        taken(qid(3))
    );
    assert_eq!(sm.quota_entities[&qid(6)].parent, Some(qid(5)));
    // Names compare case-sensitively, and the same name under a different
    // parent is fine (that is what qid(6) under globex already showed).
    apply_ok(&mut sm, named(qid(7), Some(qid(2)), "Platform"));
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
    // Promoting a child to a root under a root's name clashes too.
    apply_ok(&mut sm, named(qid(8), Some(qid(1)), "globex"));
    assert_eq!(
        sm.apply(&named(qid(8), None, "globex")).unwrap_err(),
        RejectionReason::QuotaEntityNameTaken {
            name: "globex".into(),
            holder: qid(5),
        }
    );
}

#[test]
fn paths_are_derived_from_the_parent_chain_and_follow_renames_and_moves() {
    let mut sm = tree();
    assert_eq!(sm.quota_entity_path(qid(1)).as_deref(), Some("acme"));
    assert_eq!(
        sm.quota_entity_path(qid(3)).as_deref(),
        Some("acme/eng/platform")
    );
    assert_eq!(sm.quota_entity_path(qid(99)), None);

    // A rename changes the entity's path and every descendant's.
    apply_ok(&mut sm, named(qid(2), Some(qid(1)), "engineering"));
    assert_eq!(
        sm.quota_entity_path(qid(3)).as_deref(),
        Some("acme/engineering/platform")
    );
    // So does a move.
    apply_ok(&mut sm, named(qid(2), Some(qid(5)), "engineering"));
    assert_eq!(
        sm.quota_entity_path(qid(4)).as_deref(),
        Some("globex/engineering/data")
    );
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

#[test]
fn path_derivation_is_bounded_by_the_depth_cap() {
    let mut sm = StateMachine::default();
    let depth = QUOTA_TREE_DEPTH_CAP as u128;
    let mut parent = None;
    for n in 1..=depth {
        apply_ok(&mut sm, named(qid(n), parent, &format!("l{n}")));
        parent = Some(qid(n));
    }
    let deepest = sm.quota_entity_path(qid(depth)).unwrap();
    assert_eq!(deepest.split('/').count(), QUOTA_TREE_DEPTH_CAP as usize);
    assert_eq!(
        sm.resolve_quota_entity_path(&path(&deepest)),
        Some(qid(depth))
    );
    // A path longer than the cap can never name anything.
    let too_deep = format!("{deepest}/more");
    assert_eq!(sm.resolve_quota_entity_path(&path(&too_deep)), None);
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
    // One more level is refused, on create and on reparent alike.
    let version = sm.version;
    assert_eq!(
        sm.apply(&named(qid(1000), Some(qid(cap)), "over"))
            .unwrap_err(),
        RejectionReason::QuotaEntityCycle(qid(1000))
    );
    assert!(!sm.quota_entities.contains_key(&qid(1000)));
    assert_eq!(sm.version, version + 1);
    apply_ok(&mut sm, named(qid(1000), None, "loose"));
    assert_eq!(
        sm.apply(&named(qid(1000), Some(qid(cap)), "loose"))
            .unwrap_err(),
        RejectionReason::QuotaEntityCycle(qid(1000))
    );
    assert_eq!(sm.quota_entities[&qid(1000)].parent, None);
}

#[test]
fn reparenting_a_subtree_past_the_cap_is_rejected() {
    let mut sm = StateMachine::default();
    let cap = QUOTA_TREE_DEPTH_CAP as u128;
    // A trunk `l1/.../l{cap-2}` and a separate two-level subtree `mover/kid`.
    chain(&mut sm, 0, None, cap - 2);
    apply_ok(&mut sm, named(qid(500), None, "mover"));
    apply_ok(&mut sm, named(qid(501), Some(qid(500)), "kid"));
    apply_ok(&mut sm, named(qid(502), Some(qid(501)), "grandkid"));

    // Under the trunk's leaf `mover` itself would be at depth cap-1, but
    // `grandkid` would land at cap+1: refused, nothing moves.
    let err = sm
        .apply(&named(qid(500), Some(qid(cap - 2)), "mover"))
        .unwrap_err();
    assert_eq!(err, RejectionReason::QuotaEntityCycle(qid(500)));
    assert_eq!(sm.quota_entities[&qid(500)].parent, None);
    assert_eq!(
        sm.resolve_quota_entity_path(&path("mover/kid/grandkid")),
        Some(qid(502))
    );

    // One level higher it fits exactly: `grandkid` lands at the cap, and
    // every moved path still resolves back.
    apply_ok(&mut sm, named(qid(500), Some(qid(cap - 3)), "mover"));
    for id in [qid(500), qid(501), qid(502)] {
        let p = sm.quota_entity_path(id).unwrap();
        assert_eq!(sm.resolve_quota_entity_path(&path(&p)), Some(id), "{p}");
    }
    assert_eq!(
        sm.quota_entity_path(qid(502)).unwrap().split('/').count(),
        QUOTA_TREE_DEPTH_CAP as usize
    );
}
