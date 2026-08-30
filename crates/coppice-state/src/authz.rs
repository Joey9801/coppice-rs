//! Authorization: subtree-scoped role bindings and the one evaluation
//! function that decides every mutating verb (ADR 0023).
//!
//! [`evaluate`] is a **pure** function of (bindings, quota-entity tree,
//! actor, verb): no clock, no I/O, no floats, ordered-map lookups only. That
//! is what lets the API layer reject synchronously with a real 403 *and*
//! apply re-run the identical decision against the replicated bindings as of
//! the command's log position — a revocation in flight resolves in log
//! order, on every replica identically.
//!
//! The model is deliberately small: three roles composing upward, deny by
//! default, no negative grants, no custom roles. A binding's authority is
//! either cluster-wide (unscoped) or the subtree rooted at one quota entity
//! — the quota tree (ADR 0005) is the one hierarchy, serving authority as
//! well as accounting. A principal's effective authority is the union over
//! every binding its `sub` or its groups match.

use std::collections::BTreeMap;
use std::fmt;

use coppice_core::id::QuotaEntityId;

use crate::{QuotaEntity, QUOTA_TREE_DEPTH_CAP};

/// Who proposed an API-originated command, transcribed by the API layer from
/// a *verified* credential at proposal time (ADR 0022/0023).
///
/// Group membership is a token claim rather than replicated state, so it
/// rides the command: apply must stay a pure function of (state, command).
/// Only the seven actor-carrying commands have one; internal proposers act
/// with the system's own authority and carry none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Actor {
    /// The OIDC `sub`, an opaque string — or `cert:<CN>` for an operator
    /// certificate.
    pub principal: String,
    /// The token's groups claim (claim name per
    /// [`PolicyConfig::groups_claim`](crate::PolicyConfig::groups_claim)),
    /// matched by exact string.
    pub groups: Vec<String>,
    /// Authenticated with an operator certificate: an implicit unscoped
    /// admin, neither representable in nor removable through the bindings
    /// list (ADR 0022 break-glass).
    pub operator_cert: bool,
    /// The serving node runs the formally-supported open posture, where
    /// every request resolves to a static anonymous actor with implicit
    /// unscoped admin. Carried *in* the actor precisely because node config
    /// may never be consulted at apply time.
    pub auth_disabled: bool,
}

impl Actor {
    /// An actor holding implicit unscoped admin regardless of the bindings
    /// list — an operator certificate or the open posture.
    pub fn is_implicit_admin(&self) -> bool {
        self.operator_cert || self.auth_disabled
    }
}

/// The three built-in roles (ADR 0023).
///
/// Ordered, and that order is the model: verbs compose upward, so every
/// check is a `>=` against the role a binding grants. A closed set is what
/// keeps evaluation a total function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Submitter,
    Operator,
    Admin,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Role::Submitter => "submitter",
            Role::Operator => "operator",
            Role::Admin => "admin",
        })
    }
}

/// Who a binding names. Matched by exact string — an identity-provider
/// rename silently orphans a binding, which the operations doc says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    /// Everyone carrying this group in their token's groups claim.
    Group(String),
    /// One principal, matched against the token's `sub`.
    Principal(String),
}

impl Subject {
    /// The subject's name — the string matched against the actor. Empty is
    /// refused at apply (`InvalidAuthorization`), never here.
    pub fn name(&self) -> &str {
        match self {
            Subject::Group(g) => g,
            Subject::Principal(p) => p,
        }
    }
}

/// One role binding: a subject holds a role, cluster-wide or over one
/// quota-entity subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub subject: Subject,
    pub role: Role,
    /// The subtree this binding grants over — the entity itself and every
    /// descendant. `None` is unscoped: cluster-wide, and the only kind of
    /// binding that carries the cluster verbs.
    pub scope: Option<QuotaEntityId>,
}

/// A mutating action to authorize — one arm per actor-carrying command,
/// each carrying the scope its check needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb<'a> {
    /// Submit a job charging `entity`: `submitter` or higher over it.
    Submit { entity: &'a QuotaEntityId },
    /// Abort a job charging `entity`: `operator` or higher over it, or the
    /// ownership fast-path when the actor submitted the job.
    Abort {
        entity: &'a QuotaEntityId,
        submitted_by: Option<&'a str>,
    },
    /// Drain or undrain a node. A cluster verb: unscoped `operator` or
    /// higher.
    Drain,
    /// Create or reconfigure a quota entity: `admin` covering the entity's
    /// position, and — when the command actually reparents it — `admin`
    /// covering the new parent too. A move to the root, or out of every
    /// subtree the actor administers, therefore takes unscoped `admin`.
    ConfigureQuotaEntity {
        entity: &'a QuotaEntityId,
        new_parent: Option<&'a QuotaEntityId>,
    },
    /// Replace the replicated policy. A cluster verb: unscoped `admin`.
    UpdatePolicy,
    /// Replace the replicated bindings. A cluster verb: unscoped `admin`.
    /// Delegated binding management for scoped admins is deferred (ADR 0023).
    UpdateAuthorization,
    /// Bump the semantic feature gate. A cluster verb: unscoped `admin`.
    BumpClusterVersion,
}

impl fmt::Display for Verb<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verb::Submit { entity } => write!(f, "submit a job charging quota entity {entity}"),
            Verb::Abort { entity, .. } => write!(f, "abort a job charging quota entity {entity}"),
            Verb::Drain => f.write_str("change a node's schedulability"),
            Verb::ConfigureQuotaEntity { entity, new_parent } => match new_parent {
                Some(p) => write!(f, "configure quota entity {entity} under parent {p}"),
                None => write!(f, "configure quota entity {entity} at the tree root"),
            },
            Verb::UpdatePolicy => f.write_str("replace the replicated policy"),
            Verb::UpdateAuthorization => f.write_str("replace the replicated authorization"),
            Verb::BumpClusterVersion => f.write_str("bump the cluster version"),
        }
    }
}

/// A refused authorization: who asked, what for, and what would have been
/// enough.
///
/// Its [`Display`](fmt::Display) is the human-readable half of
/// [`RejectionReason::PermissionDenied`](crate::RejectionReason::PermissionDenied),
/// and the same text the API layer's 403 carries — one sentence, no state
/// beyond what the requester already knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    /// The actor's principal, verbatim.
    pub principal: String,
    /// What was attempted, rendered from the [`Verb`].
    pub attempted: String,
    /// What would have sufficed, e.g. "operator over that quota entity".
    pub required: &'static str,
}

impl fmt::Display for Denial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "principal {:?} may not {} (requires {})",
            self.principal, self.attempted, self.required
        )
    }
}

impl Denial {
    fn new(actor: &Actor, verb: &Verb<'_>, required: &'static str) -> Denial {
        Denial {
            principal: actor.principal.clone(),
            attempted: verb.to_string(),
            required,
        }
    }
}

/// Decide whether `actor` may perform `verb`, given the replicated bindings
/// and the quota-entity tree they are scoped against (ADR 0023).
///
/// Deny by default: an actor with no matching binding is refused every verb
/// but the two implicit grants — an operator certificate or the open posture
/// ([`Actor::is_implicit_admin`]), and aborting a job the actor submitted.
///
/// Pure and total: ordered-map lookups only, and the ancestor walk is capped
/// by [`QUOTA_TREE_DEPTH_CAP`] so a corrupted parent chain still terminates.
pub fn evaluate(
    bindings: &[Binding],
    entities: &BTreeMap<QuotaEntityId, QuotaEntity>,
    actor: &Actor,
    verb: Verb<'_>,
) -> Result<(), Denial> {
    // Operator certificates and the open posture are unscoped admin, outside
    // the bindings list and unrevokable through it.
    if actor.is_implicit_admin() {
        return Ok(());
    }

    let holds_over = |role: Role, target: &QuotaEntityId| {
        bindings.iter().any(|b| {
            b.role >= role && matches_subject(&b.subject, actor) && covers(b, target, entities)
        })
    };
    let holds_unscoped = |role: Role| {
        bindings
            .iter()
            .any(|b| b.role >= role && b.scope.is_none() && matches_subject(&b.subject, actor))
    };

    match verb {
        Verb::Submit { entity } => {
            if holds_over(Role::Submitter, entity) {
                return Ok(());
            }
            Err(Denial::new(
                actor,
                &verb,
                "submitter or higher over that quota entity",
            ))
        }
        Verb::Abort {
            entity,
            submitted_by,
        } => {
            // Ownership: a principal may always abort a job it submitted,
            // with no binding at all (ADR 0023's only implicit grant besides
            // the flags above). An empty principal never owns anything —
            // `submitted_by` is only ever stamped from a present actor.
            if !actor.principal.is_empty() && submitted_by == Some(actor.principal.as_str()) {
                return Ok(());
            }
            if holds_over(Role::Operator, entity) {
                return Ok(());
            }
            Err(Denial::new(
                actor,
                &verb,
                "job ownership, or operator or higher over that quota entity",
            ))
        }
        // Cluster verbs: an unscoped binding only. A subtree-scoped admin
        // reshapes their subtree, never the cluster.
        Verb::Drain => {
            if holds_unscoped(Role::Operator) {
                return Ok(());
            }
            Err(Denial::new(actor, &verb, "an unscoped operator binding"))
        }
        Verb::UpdatePolicy | Verb::UpdateAuthorization | Verb::BumpClusterVersion => {
            if holds_unscoped(Role::Admin) {
                return Ok(());
            }
            Err(Denial::new(actor, &verb, "an unscoped admin binding"))
        }
        Verb::ConfigureQuotaEntity { entity, new_parent } => {
            if holds_unscoped(Role::Admin) {
                return Ok(());
            }
            // Reparenting moves authority (ADR 0023), so a move must stay
            // inside a subtree the actor administers: ONE scoped binding has
            // to cover both the entity and its new parent. Two disjoint
            // scoped grants must not compose into a cross-subtree move —
            // that, like a move to the root (inside no subtree), is
            // cluster-shaped and takes unscoped admin.
            let holds_over_both = |a: &QuotaEntityId, b: &QuotaEntityId| {
                bindings.iter().any(|bind| {
                    bind.role >= Role::Admin
                        && matches_subject(&bind.subject, actor)
                        && covers(bind, a, entities)
                        && covers(bind, b, entities)
                })
            };
            match entities.get(entity) {
                Some(current) if current.parent.as_ref() == new_parent => {
                    // Not a move: the entity stays exactly where the actor
                    // already administers it.
                    if holds_over(Role::Admin, entity) {
                        return Ok(());
                    }
                    Err(Denial::new(
                        actor,
                        &verb,
                        "admin over that quota entity's current position",
                    ))
                }
                Some(_) => match new_parent {
                    Some(parent) if holds_over_both(entity, parent) => Ok(()),
                    _ => Err(Denial::new(
                        actor,
                        &verb,
                        "a single admin binding covering both the entity and its \
                         new parent (a cross-subtree move, or a move to the tree \
                         root, takes unscoped admin)",
                    )),
                },
                // Creation: no current position, so the entity's position IS
                // the new parent — one binding covering it covers both ends.
                None => match new_parent {
                    Some(parent) if holds_over(Role::Admin, parent) => Ok(()),
                    _ => Err(Denial::new(
                        actor,
                        &verb,
                        "admin over the new parent (a new root entity takes unscoped admin)",
                    )),
                },
            }
        }
    }
}

/// Whether a binding's subject names this actor — exact string against the
/// `sub` or against one of the token's groups.
fn matches_subject(subject: &Subject, actor: &Actor) -> bool {
    match subject {
        Subject::Principal(sub) => actor.principal == *sub,
        Subject::Group(group) => actor.groups.iter().any(|g| g == group),
    }
}

/// Whether a binding's scope covers `target`: unscoped covers everything, and
/// a scope at `S` covers `S` itself plus every descendant.
///
/// Resolved by walking the target's ancestor chain, capped at
/// [`QUOTA_TREE_DEPTH_CAP`] so the walk is bounded even if the chain is not.
/// A target absent from the tree is covered only by its own id (it has no
/// ancestors yet) — which is what a not-yet-created entity should be.
fn covers(
    binding: &Binding,
    target: &QuotaEntityId,
    entities: &BTreeMap<QuotaEntityId, QuotaEntity>,
) -> bool {
    let Some(scope) = binding.scope.as_ref() else {
        return true;
    };
    let mut cur = Some(*target);
    for _ in 0..QUOTA_TREE_DEPTH_CAP {
        let Some(id) = cur else { return false };
        if id == *scope {
            return true;
        }
        cur = entities.get(&id).and_then(|e| e.parent);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    use coppice_core::quota::{CostUnits, UsageState};
    use coppice_core::time::Timestamp;
    use uuid::Uuid;

    fn qid(n: u128) -> QuotaEntityId {
        QuotaEntityId(Uuid::from_u128(n))
    }

    const ORG: u128 = 1;
    const TEAM_A: u128 = 2;
    const TEAM_B: u128 = 3;
    const SQUAD: u128 = 4;

    /// `org` → {`team-a` → `squad`, `team-b`}: a parent, a sibling, and a
    /// grandchild, which is every containment case the subtree rule has.
    fn tree() -> BTreeMap<QuotaEntityId, QuotaEntity> {
        let at = Timestamp::from_micros(1_760_000_000_000_000).expect("in range");
        let entity = |parent: Option<u128>, name: &str| QuotaEntity {
            parent: parent.map(qid),
            name: name.to_string(),
            quota: CostUnits(1_000),
            usage: UsageState::new(at),
            created_at: at,
            updated_at: at,
        };
        BTreeMap::from([
            (qid(ORG), entity(None, "org")),
            (qid(TEAM_A), entity(Some(ORG), "team-a")),
            (qid(TEAM_B), entity(Some(ORG), "team-b")),
            (qid(SQUAD), entity(Some(TEAM_A), "squad")),
        ])
    }

    fn actor(principal: &str) -> Actor {
        Actor {
            principal: principal.to_string(),
            ..Actor::default()
        }
    }

    fn in_groups(principal: &str, groups: &[&str]) -> Actor {
        Actor {
            principal: principal.to_string(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            ..Actor::default()
        }
    }

    fn bound(subject: Subject, role: Role, scope: Option<u128>) -> Binding {
        Binding {
            subject,
            role,
            scope: scope.map(qid),
        }
    }

    fn principal(name: &str) -> Subject {
        Subject::Principal(name.to_string())
    }

    fn group(name: &str) -> Subject {
        Subject::Group(name.to_string())
    }

    /// Every verb, with the scope arm pointed at `team-a`.
    fn every_verb<'a>(entity: &'a QuotaEntityId, new_parent: &'a QuotaEntityId) -> Vec<Verb<'a>> {
        vec![
            Verb::Submit { entity },
            Verb::Abort {
                entity,
                submitted_by: None,
            },
            Verb::Drain,
            Verb::ConfigureQuotaEntity {
                entity,
                new_parent: Some(new_parent),
            },
            Verb::UpdatePolicy,
            Verb::UpdateAuthorization,
            Verb::BumpClusterVersion,
        ]
    }

    fn allowed(bindings: &[Binding], actor: &Actor, verb: Verb<'_>) -> bool {
        evaluate(bindings, &tree(), actor, verb).is_ok()
    }

    /// Deny by default: with no bindings at all, an ordinary principal holds
    /// nothing — not one verb, not even over an entity that does not exist.
    #[test]
    fn empty_bindings_deny_every_verb() {
        let (team_a, org) = (qid(TEAM_A), qid(ORG));
        let ghost = qid(0xDEAD);
        for verb in every_verb(&team_a, &org) {
            assert!(
                !allowed(&[], &actor("nobody"), verb),
                "deny by default: {verb:?}"
            );
        }
        assert!(!allowed(
            &[],
            &actor("nobody"),
            Verb::Submit { entity: &ghost }
        ));
    }

    /// The role × verb table of ADR 0023, checked exhaustively against an
    /// unscoped binding: submitter submits, operator adds abort and drain,
    /// admin adds everything.
    #[test]
    fn unscoped_roles_grant_exactly_their_row_of_the_table() {
        let (team_a, org) = (qid(TEAM_A), qid(ORG));
        // (role, expected outcome per every_verb's order)
        let table = [
            (
                Role::Submitter,
                [true, false, false, false, false, false, false],
            ),
            (
                Role::Operator,
                [true, true, true, false, false, false, false],
            ),
            (Role::Admin, [true, true, true, true, true, true, true]),
        ];
        for (role, expected) in table {
            let bindings = vec![bound(principal("ana"), role, None)];
            for (verb, want) in every_verb(&team_a, &org).into_iter().zip(expected) {
                assert_eq!(
                    allowed(&bindings, &actor("ana"), verb),
                    want,
                    "unscoped {role} on {verb:?}"
                );
            }
        }
    }

    /// The cluster verbs — drain, policy, authorization, cluster version —
    /// take an UNSCOPED binding. A scoped admin, however wide the subtree,
    /// reshapes their subtree and never the cluster.
    #[test]
    fn cluster_verbs_refuse_scoped_bindings_even_admin_ones() {
        // Scoped at the tree's own root, the widest scope expressible.
        let bindings = vec![bound(principal("ana"), Role::Admin, Some(ORG))];
        let ana = actor("ana");
        for verb in [
            Verb::Drain,
            Verb::UpdatePolicy,
            Verb::UpdateAuthorization,
            Verb::BumpClusterVersion,
        ] {
            assert!(
                !allowed(&bindings, &ana, verb),
                "cluster verb from a scoped binding: {verb:?}"
            );
        }
        // ...while the same admin's scoped verbs work.
        let squad = qid(SQUAD);
        assert!(allowed(&bindings, &ana, Verb::Submit { entity: &squad }));
    }

    /// A scope covers the entity itself and every descendant — a grant at
    /// the parent reaches the grandchild — and nothing outside it: a sibling
    /// subtree and the scope's own ancestor are both denied.
    #[test]
    fn a_scope_covers_its_subtree_and_nothing_else() {
        let bindings = vec![bound(principal("ana"), Role::Submitter, Some(TEAM_A))];
        let ana = actor("ana");
        for (entity, want) in [
            (qid(TEAM_A), true),  // the scope itself
            (qid(SQUAD), true),   // a descendant
            (qid(TEAM_B), false), // a sibling
            (qid(ORG), false),    // an ancestor
        ] {
            assert_eq!(
                allowed(&bindings, &ana, Verb::Submit { entity: &entity }),
                want,
                "subtree containment for {entity}"
            );
        }
    }

    /// Subjects match by exact string, against the `sub` for a principal
    /// binding and against the groups claim for a group one — and never
    /// across the two.
    #[test]
    fn principal_and_group_subjects_match_their_own_field() {
        let team_a = qid(TEAM_A);
        let submit = Verb::Submit { entity: &team_a };
        let by_group = vec![bound(group("batch-users"), Role::Submitter, None)];
        let by_principal = vec![bound(principal("ana"), Role::Submitter, None)];

        assert!(allowed(
            &by_group,
            &in_groups("ana", &["batch-users"]),
            submit
        ));
        assert!(!allowed(&by_group, &actor("ana"), submit));
        // A group binding is not satisfied by a principal of the same name,
        // nor a principal binding by a group of it.
        assert!(!allowed(&by_group, &actor("batch-users"), submit));
        assert!(!allowed(
            &by_principal,
            &in_groups("someone", &["ana"]),
            submit
        ));
        // Exact strings: no prefix, case, or substring matching.
        assert!(!allowed(
            &by_group,
            &in_groups("ana", &["batch-users-2", "Batch-Users"]),
            submit
        ));
    }

    /// Effective authority is the union over every matching binding: two
    /// narrow grants together cover what neither covers alone, and a
    /// group-derived grant composes with a principal-derived one.
    #[test]
    fn authority_is_the_union_over_matching_bindings() {
        let bindings = vec![
            bound(principal("ana"), Role::Submitter, Some(TEAM_A)),
            bound(group("oncall"), Role::Operator, Some(TEAM_B)),
        ];
        let ana = in_groups("ana", &["oncall"]);
        let (team_a, team_b) = (qid(TEAM_A), qid(TEAM_B));

        assert!(allowed(&bindings, &ana, Verb::Submit { entity: &team_a }));
        assert!(allowed(&bindings, &ana, Verb::Submit { entity: &team_b }));
        // The operator grant is confined to team-b; team-a stays submit-only.
        assert!(allowed(
            &bindings,
            &ana,
            Verb::Abort {
                entity: &team_b,
                submitted_by: None
            }
        ));
        assert!(!allowed(
            &bindings,
            &ana,
            Verb::Abort {
                entity: &team_a,
                submitted_by: None
            }
        ));
    }

    /// Operator certificates and the open posture are implicit unscoped
    /// admin: every verb, with an empty bindings list.
    #[test]
    fn operator_certs_and_open_mode_are_implicit_unscoped_admin() {
        let (team_a, org) = (qid(TEAM_A), qid(ORG));
        let cert = Actor {
            principal: "cert:ops-1".into(),
            operator_cert: true,
            ..Actor::default()
        };
        let open = Actor {
            principal: "anonymous".into(),
            auth_disabled: true,
            ..Actor::default()
        };
        for who in [&cert, &open] {
            assert!(who.is_implicit_admin());
            for verb in every_verb(&team_a, &org) {
                assert!(allowed(&[], who, verb), "{} on {verb:?}", who.principal);
            }
            // Including a move to the root, which no scoped admin may do.
            assert!(allowed(
                &[],
                who,
                Verb::ConfigureQuotaEntity {
                    entity: &team_a,
                    new_parent: None
                }
            ));
        }
    }

    /// Ownership: a principal may always abort the job it submitted, with no
    /// binding at all — and only that job. The grant is exact-string on the
    /// principal, and an actor with an empty principal owns nothing.
    #[test]
    fn ownership_allows_aborting_only_your_own_job() {
        let team_a = qid(TEAM_A);
        let mine = Verb::Abort {
            entity: &team_a,
            submitted_by: Some("ana"),
        };
        let theirs = Verb::Abort {
            entity: &team_a,
            submitted_by: Some("bo"),
        };
        let unowned = Verb::Abort {
            entity: &team_a,
            submitted_by: None,
        };
        assert!(allowed(&[], &actor("ana"), mine));
        assert!(!allowed(&[], &actor("ana"), theirs));
        assert!(!allowed(&[], &actor("ana"), unowned));
        // An unauthenticated-shaped actor must not own the unstamped jobs.
        assert!(!allowed(&[], &actor(""), unowned));
        // Someone else's job takes operator over its entity.
        let operator = vec![bound(principal("ana"), Role::Operator, Some(TEAM_A))];
        assert!(allowed(&operator, &actor("ana"), theirs));
    }

    /// Ownership does not leak into the other verbs: submitting a job does
    /// not let its owner submit more, or touch the entity.
    #[test]
    fn ownership_grants_nothing_but_the_abort() {
        let team_a = qid(TEAM_A);
        let ana = actor("ana");
        assert!(!allowed(&[], &ana, Verb::Submit { entity: &team_a }));
        assert!(!allowed(
            &[],
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &team_a,
                new_parent: Some(&qid(ORG))
            }
        ));
    }

    /// Reparenting moves authority, so a scoped admin must hold both ends.
    /// Within their subtree they may move an entity; a move that would carry
    /// it out of — or in from — another subtree is refused, and a move to
    /// the root takes unscoped admin.
    #[test]
    fn reparenting_needs_admin_over_both_ends() {
        let scoped = vec![bound(principal("ana"), Role::Admin, Some(TEAM_A))];
        let ana = actor("ana");
        let (team_a, team_b, squad, org) = (qid(TEAM_A), qid(TEAM_B), qid(SQUAD), qid(ORG));

        // Inside the subtree: squad (currently under team-a) → team-a.
        assert!(allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: Some(&team_a)
            }
        ));
        // Out of the subtree: squad → team-b, which ana does not administer.
        assert!(!allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: Some(&team_b)
            }
        ));
        // Into the subtree from outside: team-b → team-a. Ana administers
        // the destination but not team-b's current position.
        assert!(!allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &team_b,
                new_parent: Some(&team_a)
            }
        ));
        // To the root: inside no subtree, so unscoped admin only.
        assert!(!allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: None
            }
        ));
        let unscoped = vec![bound(principal("ana"), Role::Admin, None)];
        assert!(allowed(
            &unscoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: None
            }
        ));
        // Re-asserting an entity's existing parent is not a move, so the
        // new-parent check does not fire: a scoped admin may rename or
        // requota the very entity their scope is rooted at.
        assert!(allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &team_a,
                new_parent: Some(&org)
            }
        ));
    }

    /// Two disjoint scoped-admin grants must not compose into a
    /// cross-subtree move: ONE binding has to cover both ends. Admin over
    /// team-a plus admin over team-b still cannot move squad between them —
    /// that is unscoped-admin territory (ADR 0023) — while a single binding
    /// wide enough to contain both ends (at org) allows the same move.
    #[test]
    fn disjoint_scoped_admins_cannot_compose_a_cross_subtree_move() {
        let disjoint = vec![
            bound(principal("ana"), Role::Admin, Some(TEAM_A)),
            bound(principal("ana"), Role::Admin, Some(TEAM_B)),
        ];
        let ana = actor("ana");
        let (team_b, squad) = (qid(TEAM_B), qid(SQUAD));
        // Each end is individually administered, and non-move configuration
        // of either works...
        assert!(allowed(
            &disjoint,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &team_b,
                new_parent: Some(&qid(ORG))
            }
        ));
        // ...but the cross-subtree move squad (under team-a) → team-b is
        // refused: no single binding covers both ends.
        assert!(!allowed(
            &disjoint,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: Some(&team_b)
            }
        ));
        // One binding containing both ends allows the identical move.
        let org_admin = vec![bound(principal("ana"), Role::Admin, Some(ORG))];
        assert!(allowed(
            &org_admin,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &squad,
                new_parent: Some(&team_b)
            }
        ));
    }

    /// Creating an entity that does not exist yet has no current position to
    /// check, so it turns entirely on admin over the new parent.
    #[test]
    fn creating_an_entity_turns_on_the_new_parent() {
        let scoped = vec![bound(principal("ana"), Role::Admin, Some(TEAM_A))];
        let ana = actor("ana");
        let fresh = qid(0xF0);
        let (team_a, team_b) = (qid(TEAM_A), qid(TEAM_B));

        assert!(allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &fresh,
                new_parent: Some(&team_a)
            }
        ));
        assert!(!allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &fresh,
                new_parent: Some(&team_b)
            }
        ));
        // A new root entity is a cluster-shaped act: unscoped admin only.
        assert!(!allowed(
            &scoped,
            &ana,
            Verb::ConfigureQuotaEntity {
                entity: &fresh,
                new_parent: None
            }
        ));
    }

    /// Roles compose upward, so every check is a `>=`: a higher role
    /// satisfies a lower role's verb over the same scope.
    #[test]
    fn roles_compose_upward() {
        assert!(Role::Submitter < Role::Operator && Role::Operator < Role::Admin);
        let team_a = qid(TEAM_A);
        for role in [Role::Submitter, Role::Operator, Role::Admin] {
            let bindings = vec![bound(group("everyone"), role, Some(TEAM_A))];
            assert!(allowed(
                &bindings,
                &in_groups("ana", &["everyone"]),
                Verb::Submit { entity: &team_a }
            ));
        }
    }

    /// A parent chain that cycles must not hang the walk: the depth cap
    /// bounds it, and an unreachable scope is simply not covering.
    #[test]
    fn a_cyclic_parent_chain_terminates_and_denies() {
        let at = Timestamp::from_micros(1_760_000_000_000_000).expect("in range");
        let entity = |parent: u128| QuotaEntity {
            parent: Some(qid(parent)),
            name: "loop".to_string(),
            quota: CostUnits(0),
            usage: UsageState::new(at),
            created_at: at,
            updated_at: at,
        };
        let entities = BTreeMap::from([(qid(1), entity(2)), (qid(2), entity(1))]);
        let bindings = vec![bound(principal("ana"), Role::Admin, Some(0xBEEF))];
        let target = qid(1);
        assert!(evaluate(
            &bindings,
            &entities,
            &actor("ana"),
            Verb::Submit { entity: &target }
        )
        .is_err());
    }

    /// The denial carries the principal and what was attempted — it is the
    /// text of `PermissionDenied` and of the API's 403, so it must name both.
    #[test]
    fn denials_name_the_principal_and_the_attempt() {
        let team_a = qid(TEAM_A);
        let denial = evaluate(
            &[],
            &tree(),
            &actor("ana"),
            Verb::Submit { entity: &team_a },
        )
        .expect_err("deny by default");
        let text = denial.to_string();
        assert!(text.contains("ana"), "{text}");
        assert!(text.contains(&team_a.to_string()), "{text}");
        assert!(text.contains("submitter"), "{text}");
    }
}
