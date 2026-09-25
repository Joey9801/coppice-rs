//! The event-subscription seam (ADR 0043): the validated `jobs` selector and
//! the items a subscription delivers.
//!
//! These are the API-level types [`ControlPlane::subscribe_events`] speaks, so
//! the HTTP handler in [`http::events`](crate::http::events) and the
//! coordinator's fanout can agree without `coppice-api` depending on the
//! coordinator crate.
//!
//! The selector is **compiled once, at the edge**, from the same
//! [`dto::JobFilter`] AST `ListJobs` takes, restricted to the identity-like
//! leaves that apply-stamped scope keys can answer (ADR 0043). That
//! restriction is what makes the same filter always valid on `ListJobs`,
//! which is the resync query a client runs after a `gap`.

use std::collections::BTreeSet;

use coppice_core::id::{JobId, QuotaEntityId};
use coppice_core::time::Timestamp;
use coppice_state::ScopeView;

use crate::http::dto;

/// A `jobs` filter leaf this subscription surface cannot answer.
///
/// Named rather than generic: every refusal tells the caller *which* leaf was
/// the problem, because the fix is to move that leaf into the `ListJobs`
/// resync query instead (ADR 0043).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "the `{leaf}` filter leaf is not available on an event subscription: a subscription matches \
     on the identity keys stamped at apply time (metadata, entity, id, submitted_by), not on \
     state that changes underneath it"
)]
pub struct ForbiddenLeaf {
    /// The offending leaf's wire key, e.g. `phase`.
    pub leaf: &'static str,
}

/// A validated, compiled `jobs` selector.
///
/// Evaluated against a [`ScopeView`] — the scope keys the apply loop stamped
/// onto the event stream — and nothing else, which is precisely what makes a
/// subscription's verdict identical on every replica and stable across a
/// reconnect (ADR 0043, KOI-3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSelector {
    root: Node,
    /// See [`JobSelector::reads_entity_subtree`]. Computed once, at compile
    /// time, because it is consulted per batch on the delivery path.
    reads_entity_subtree: bool,
}

/// One node of a compiled selector. A private mirror of the allowed subset of
/// [`dto::JobFilter`], with the entity scope already reduced to a boolean and
/// the id list already a set.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    All(Vec<Node>),
    Any(Vec<Node>),
    Not(Box<Node>),
    /// Presence when `equals` is `None`, exact byte equality otherwise
    /// (ADR 0042's two metadata operands).
    Metadata {
        key: String,
        equals: Option<String>,
    },
    Entity {
        /// `None` for a path the handler never resolved against its view
        /// (ADR 0045) — it matches nothing. The subscribe handler resolves
        /// every path before compiling, so this is a guard, not a mode.
        id: Option<QuotaEntityId>,
        /// `true` matches only the job's own entity; `false` matches anywhere
        /// along its ancestry.
        exact: bool,
    },
    Id(BTreeSet<JobId>),
    SubmittedBy(String),
}

impl JobSelector {
    /// Compile a validated [`dto::JobFilter`] into a selector.
    ///
    /// The caller has already run [`dto::JobFilter::validate`] (depth, node
    /// and non-empty-list caps, shared with `ListJobs`); this adds the one
    /// rule that is specific to subscriptions — the restricted leaf set — and
    /// names the first leaf outside it.
    pub fn compile(filter: &dto::JobFilter) -> Result<JobSelector, ForbiddenLeaf> {
        let root = compile_node(filter)?;
        Ok(JobSelector {
            reads_entity_subtree: reads_entity_subtree(&root),
            root,
        })
    }

    /// Whether a job with these scope keys is in this selector's scope.
    pub fn matches(&self, job: JobId, scope: ScopeView<'_>) -> bool {
        eval(&self.root, job, scope)
    }

    /// Whether this selector's verdict can turn on a job's **ancestry** rather
    /// than only on its own keys — true iff a subtree-scoped `entity` leaf
    /// appears anywhere in the tree, under any combinator including `not`.
    ///
    /// Such a selector is the one kind that a `ConfigureQuotaEntity` reparent
    /// can move jobs into and out of without naming any of them, so the fanout
    /// owes it a gap when one goes by (ADR 0043). An exact-entity leaf reads
    /// the chain's head, which a reparent of some ancestor never changes.
    pub fn reads_entity_subtree(&self) -> bool {
        self.reads_entity_subtree
    }
}

/// Walk the compiled tree for a subtree-scoped `entity` leaf.
///
/// Negation is not special: `not(entity subtree = X)` admits precisely the
/// complement of a subtree, and a reparent moves jobs across that boundary in
/// the same way.
fn reads_entity_subtree(node: &Node) -> bool {
    match node {
        Node::All(ns) | Node::Any(ns) => ns.iter().any(reads_entity_subtree),
        Node::Not(n) => reads_entity_subtree(n),
        Node::Entity { exact, .. } => !exact,
        Node::Metadata { .. } | Node::Id(_) | Node::SubmittedBy(_) => false,
    }
}

fn compile_node(filter: &dto::JobFilter) -> Result<Node, ForbiddenLeaf> {
    use dto::JobFilter as F;
    Ok(match filter {
        F::All(fs) => Node::All(fs.iter().map(compile_node).collect::<Result<_, _>>()?),
        F::Any(fs) => Node::Any(fs.iter().map(compile_node).collect::<Result<_, _>>()?),
        F::Not(f) => Node::Not(Box::new(compile_node(f)?)),
        F::Metadata(m) => Node::Metadata {
            key: m.key.clone(),
            equals: m.equals.clone(),
        },
        F::Entity(e) => Node::Entity {
            id: e.id(),
            exact: e.scope == dto::EntityScope::Exact,
        },
        F::Id(i) => Node::Id(i.r#in.iter().copied().collect()),
        F::SubmittedBy(p) => Node::SubmittedBy(p.clone()),
        // Everything else reads state the scope keys deliberately do not
        // carry: a phase or a node changes under a live subscription, an
        // image or a free-text search would mean shipping the job spec on
        // every event, and `submitted` is a window a resync query bounds far
        // better than a stream can.
        F::Phase(_) => return Err(ForbiddenLeaf { leaf: "phase" }),
        F::Node(_) => return Err(ForbiddenLeaf { leaf: "node" }),
        F::Image(_) => return Err(ForbiddenLeaf { leaf: "image" }),
        F::Search(_) => return Err(ForbiddenLeaf { leaf: "search" }),
        F::Submitted(_) => return Err(ForbiddenLeaf { leaf: "submitted" }),
        F::Requests(_) => return Err(ForbiddenLeaf { leaf: "requests" }),
    })
}

fn eval(node: &Node, job: JobId, scope: ScopeView<'_>) -> bool {
    match node {
        Node::All(ns) => ns.iter().all(|n| eval(n, job, scope)),
        Node::Any(ns) => ns.iter().any(|n| eval(n, job, scope)),
        Node::Not(n) => !eval(n, job, scope),
        Node::Metadata { key, equals } => match (scope.metadata.get(key), equals) {
            (None, _) => false,
            // Byte for byte and case-sensitive, exactly as `ListJobs` reads
            // the same leaf (ADR 0042).
            (Some(stored), Some(operand)) => stored == operand,
            (Some(_), None) => true,
        },
        // The chain is entity-first, so `exact` is the head and `subtree` is
        // membership — the same two breadths `ListJobs` offers, decided here
        // without a tree walk because the walk already happened at apply.
        Node::Entity { id: None, .. } => false,
        Node::Entity {
            id: Some(id),
            exact,
        } => {
            if *exact {
                scope.entity_chain.first() == Some(id)
            } else {
                scope.entity_chain.contains(id)
            }
        }
        Node::Id(ids) => ids.contains(&job),
        // A job with no submitter (internal or pre-authz) matches nothing.
        Node::SubmittedBy(p) => scope.submitted_by == Some(p.as_str()),
    }
}

/// One item delivered on an event subscription (ADR 0043's three frames).
#[derive(Debug, Clone)]
pub enum EventStreamItem {
    /// One command's events that the selector admitted, never split across
    /// items: a `batch` frame.
    Batch(EventBatchItem),
    /// Everything matching at or below `index` has been delivered — the
    /// `progress` bookmark, which doubles as the stream's keepalive.
    Progress { index: u64 },
    /// Delivery was discontinuous: the client must re-query state with the
    /// same filter and resubscribe from the index that read reports (the
    /// ADR 0008 gap-and-resync contract). The stream continues live.
    Gap { earliest_available: u64 },
}

/// The events of one applied command that a subscription's filter admitted.
#[derive(Debug, Clone)]
pub struct EventBatchItem {
    /// The producing command's Raft log index — the ADR 0008 resume cursor,
    /// and the frame's SSE event id.
    pub index: u64,
    /// The command's advisory proposer stamp (ADR 0032); never an ordering
    /// key.
    pub at: Timestamp,
    /// Admitted events in batch order, each with the ordinal it was assigned
    /// in the *full* batch (ADR 0032: gaps are legitimate, renumbering is
    /// not).
    pub events: Vec<OrdinalEvent>,
}

/// One event paired with its batch-assigned ordinal (ADR 0032).
#[derive(Debug, Clone)]
pub struct OrdinalEvent {
    pub ordinal: u32,
    pub event: coppice_state::Event,
}

/// A live subscription, as the HTTP handler holds it.
///
/// Dropping it is how a disconnected client unsubscribes: the producer's next
/// send fails and it tears the whole subscription down. The channel closing is
/// the stream's clean end — a server drain or the fanout shutting down.
pub struct EventSubscription {
    pub items: tokio::sync::mpsc::Receiver<EventStreamItem>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope<'a>(
        chain: &'a [QuotaEntityId],
        submitted_by: Option<&'a str>,
        metadata: &'a coppice_core::metadata::JobMetadata,
    ) -> ScopeView<'a> {
        ScopeView {
            entity_chain: chain,
            submitted_by,
            metadata,
        }
    }

    fn metadata_leaf(key: &str, equals: Option<&str>) -> dto::JobFilter {
        dto::JobFilter::Metadata(dto::MetadataFilter {
            key: key.to_string(),
            equals: equals.map(str::to_string),
        })
    }

    fn entity_leaf(id: QuotaEntityId, scope: dto::EntityScope) -> dto::JobFilter {
        dto::JobFilter::Entity(dto::EntityFilter {
            entity: id.into(),
            scope,
        })
    }

    /// Every leaf outside the restricted set is refused by name, so the 400
    /// tells the caller which one to move into the resync query instead.
    #[test]
    fn forbidden_leaves_are_named_individually() {
        let cases = [
            (
                dto::JobFilter::Phase(dto::PhaseFilter {
                    r#in: vec![dto::JobPhase::Running],
                }),
                "phase",
            ),
            (
                dto::JobFilter::Node(coppice_core::id::NodeId::new()),
                "node",
            ),
            (
                dto::JobFilter::Image(dto::ImageFilter::Equals("registry/img".into())),
                "image",
            ),
            (dto::JobFilter::Search("abc".into()), "search"),
            (
                dto::JobFilter::Submitted(dto::SubmittedFilter {
                    after: Some(Timestamp::UNIX_EPOCH),
                    before: None,
                }),
                "submitted",
            ),
            (
                dto::JobFilter::Requests(dto::RequestsFilter {
                    resource: dto::RequestsResource::CpuMillis,
                    min: Some(1),
                    max: None,
                }),
                "requests",
            ),
        ];
        for (filter, leaf) in cases {
            let err = JobSelector::compile(&filter).expect_err("leaf is refused");
            assert_eq!(err.leaf, leaf);
            assert!(err.to_string().contains(leaf));
        }
    }

    /// A forbidden leaf nested under combinators is still named: the walk
    /// covers the whole tree, not just its root.
    #[test]
    fn a_forbidden_leaf_under_a_combinator_is_still_refused() {
        let filter = dto::JobFilter::All(vec![
            metadata_leaf("team", None),
            dto::JobFilter::Any(vec![dto::JobFilter::Node(coppice_core::id::NodeId::new())]),
        ]);
        assert_eq!(
            JobSelector::compile(&filter).expect_err("nested leaf is refused"),
            ForbiddenLeaf { leaf: "node" }
        );
    }

    #[test]
    fn entity_exact_matches_the_head_and_subtree_matches_the_chain() {
        let root = QuotaEntityId::new();
        let team = QuotaEntityId::new();
        let chain = [team, root];
        let empty = coppice_core::metadata::JobMetadata::new();
        let job = JobId::new();

        let compile = |f| JobSelector::compile(&f).expect("allowed leaf");
        let exact_team = compile(entity_leaf(team, dto::EntityScope::Exact));
        let exact_root = compile(entity_leaf(root, dto::EntityScope::Exact));
        let subtree_root = compile(entity_leaf(root, dto::EntityScope::Subtree));

        assert!(exact_team.matches(job, scope(&chain, None, &empty)));
        assert!(!exact_root.matches(job, scope(&chain, None, &empty)));
        assert!(subtree_root.matches(job, scope(&chain, None, &empty)));
    }

    #[test]
    fn metadata_presence_and_equality_and_submitted_by() {
        let mut metadata = coppice_core::metadata::JobMetadata::new();
        metadata.insert("team".into(), "platform".into());
        let job = JobId::new();
        let compile = |f| JobSelector::compile(&f).expect("allowed leaf");
        let present = compile(metadata_leaf("team", None));
        let equals = compile(metadata_leaf("team", Some("platform")));
        let other = compile(metadata_leaf("team", Some("storage")));
        let alice = compile(dto::JobFilter::SubmittedBy("alice".into()));

        assert!(present.matches(job, scope(&[], None, &metadata)));
        assert!(equals.matches(job, scope(&[], None, &metadata)));
        assert!(!other.matches(job, scope(&[], None, &metadata)));
        assert!(alice.matches(job, scope(&[], Some("alice"), &metadata)));
        // A job with no submitter matches no `submitted_by` filter.
        assert!(!alice.matches(job, scope(&[], None, &metadata)));
    }

    #[test]
    fn id_leaf_matches_the_job_id_itself() {
        let job = JobId::new();
        let empty = coppice_core::metadata::JobMetadata::new();
        let selector = JobSelector::compile(&dto::JobFilter::Id(dto::IdFilter { r#in: vec![job] }))
            .expect("allowed leaf");
        assert!(selector.matches(job, scope(&[], None, &empty)));
        assert!(!selector.matches(JobId::new(), scope(&[], None, &empty)));
    }

    /// The flag that decides whether a quota-entity reparent owes this
    /// subscriber a gap (ADR 0043). Only a *subtree* leaf reads a job's
    /// ancestry; an exact one reads the chain's head, which a reparent above
    /// the job never moves.
    #[test]
    fn reads_entity_subtree_is_true_only_for_a_subtree_leaf() {
        let entity = QuotaEntityId::new();
        let compile = |f| JobSelector::compile(&f).expect("allowed leaf");

        assert!(compile(entity_leaf(entity, dto::EntityScope::Subtree)).reads_entity_subtree());
        assert!(!compile(entity_leaf(entity, dto::EntityScope::Exact)).reads_entity_subtree());
        assert!(!compile(metadata_leaf("team", None)).reads_entity_subtree());
        assert!(!compile(dto::JobFilter::Id(dto::IdFilter {
            r#in: vec![JobId::new()]
        }))
        .reads_entity_subtree());
    }

    /// The walk covers the whole tree, under every combinator — a subtree leaf
    /// buried under `not` inside `any` still reads ancestry.
    #[test]
    fn reads_entity_subtree_finds_a_nested_leaf() {
        let entity = QuotaEntityId::new();
        let nested = dto::JobFilter::All(vec![
            metadata_leaf("team", None),
            dto::JobFilter::Any(vec![dto::JobFilter::Not(Box::new(entity_leaf(
                entity,
                dto::EntityScope::Subtree,
            )))]),
        ]);
        assert!(JobSelector::compile(&nested)
            .expect("allowed leaves")
            .reads_entity_subtree());

        // The same shape with an exact leaf reads no ancestry at all.
        let exact = dto::JobFilter::All(vec![
            metadata_leaf("team", None),
            dto::JobFilter::Any(vec![dto::JobFilter::Not(Box::new(entity_leaf(
                entity,
                dto::EntityScope::Exact,
            )))]),
        ]);
        assert!(!JobSelector::compile(&exact)
            .expect("allowed leaves")
            .reads_entity_subtree());
    }

    #[test]
    fn combinators_compose() {
        let mut metadata = coppice_core::metadata::JobMetadata::new();
        metadata.insert("team".into(), "platform".into());
        let job = JobId::new();
        let selector = JobSelector::compile(&dto::JobFilter::All(vec![
            metadata_leaf("team", Some("platform")),
            dto::JobFilter::Not(Box::new(metadata_leaf("archived", None))),
        ]))
        .expect("allowed leaves");
        assert!(selector.matches(job, scope(&[], None, &metadata)));

        metadata.insert("archived".into(), "true".into());
        assert!(!selector.matches(job, scope(&[], None, &metadata)));
    }
}
