//! The job-list filter AST (`?filter=` on `GET /api/v1/jobs`).
//!
//! Unlike almost everything else in [`crate::types`], this vocabulary is
//! **closed**: the server never echoes a `JobFilter` back on a response, so
//! there is no future-server-value case to tolerate, and none of the enums
//! here carry an `Unknown` catch-all.
//!
//! A filter tree is built with the constructors on [`JobFilter`] — its
//! variants exist so `serde` can (de)serialize the wire shape, but
//! `#[non_exhaustive]` blocks a caller from writing a struct literal, so the
//! constructors are the intended entry point. There is no
//! `to_query_value`/similar helper: send `serde_json::to_string(&filter)`
//! (or `serde_json::to_value`) as the `filter=` query parameter's value —
//! [`super::ListJobsParams::query_pairs`] does exactly that.

use serde::{Deserialize, Serialize};

use crate::id::{JobId, NodeId, QuotaEntityId};
use crate::metadata::validate_key;
use crate::time::Timestamp;

use super::JobPhase;

/// Maximum nesting depth of a [`JobFilter`] tree (combinators deep).
pub const MAX_FILTER_DEPTH: usize = 8;
/// Maximum total nodes (combinators + leaves) in a [`JobFilter`] tree.
pub const MAX_FILTER_NODES: usize = 64;

/// The job-list filter AST.
///
/// Externally tagged: every node is a JSON object with exactly one key
/// (`{"phase": {...}}`, `{"all": [...]}`), so an unknown key or a two-key
/// object is a deserialization error on the server. The remaining shape
/// rules `serde` cannot express — non-empty combinator/`in` lists, the depth
/// and node caps, at-least-one bounds, ordered bounds, the metadata leaf's
/// operand rules — are checked by [`JobFilter::validate`], which reproduces
/// the server's own rules (and error text) so a caller catches a malformed
/// filter before spending a round trip on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JobFilter {
    /// AND over a non-empty list.
    All(Vec<JobFilter>),
    /// OR over a non-empty list.
    Any(Vec<JobFilter>),
    /// Negation.
    Not(Box<JobFilter>),
    /// Matches the derived [`JobPhase`].
    Phase(PhaseFilter),
    /// Matches a quota entity, exactly or by subtree.
    Entity(EntityFilter),
    /// Current attempt's node; an unknown node matches nothing.
    Node(NodeId),
    /// Matches the job's image.
    Image(ImageFilter),
    /// Matches by job id.
    Id(IdFilter),
    /// Case-insensitive substring over the job id string OR the image.
    Search(String),
    /// Matches `submitted_at` against one or both bounds.
    Submitted(SubmittedFilter),
    /// Exact match on the submitting principal. A job with no submitter
    /// (internal or pre-authz) matches nothing.
    SubmittedBy(String),
    /// Matches a requested-resource dimension against one or both bounds.
    Requests(RequestsFilter),
    /// Matches a metadata key's presence, or its exact value (ADR 0042).
    Metadata(MetadataFilter),
}

/// `{"phase": {"in": [...]}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseFilter {
    /// Non-empty (checked by [`JobFilter::validate`]).
    pub r#in: Vec<JobPhase>,
}

/// `{"entity": {"id": "quota-…", "scope": "subtree"}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityFilter {
    /// The entity to match.
    pub id: QuotaEntityId,
    /// How far the match reaches.
    #[serde(default)]
    pub scope: EntityScope,
}

/// Entity match breadth. `Subtree` (the default) matches the entity and all
/// its descendants; `Exact` matches only the entity itself.
///
/// Client-authored only — this never travels on a response — so it is a
/// closed `snake_case` enum with no `Unknown` catch-all.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum EntityScope {
    /// The entity and all its descendants.
    #[default]
    Subtree,
    /// The entity itself only.
    Exact,
}

/// `{"image": {"contains": "…"}}` or `{"image": {"equals": "…"}}` — exactly
/// one op (the single-key object enforces it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ImageFilter {
    /// Case-insensitive substring match.
    Contains(String),
    /// Exact match.
    Equals(String),
}

/// `{"id": {"in": ["job-…", …]}}` — a malformed id fails to deserialize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdFilter {
    /// Non-empty (checked by [`JobFilter::validate`]).
    pub r#in: Vec<JobId>,
}

/// `{"submitted": {"after": ISO8601, "before": ISO8601}}` — at least one
/// bound; `after` inclusive `≥`, `before` exclusive `<`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmittedFilter {
    /// Inclusive lower bound.
    ///
    /// Omitted, not sent as `null`, when unset — unlike most request fields
    /// in this crate, so a hand-built filter tree stays readable and matches
    /// the server's own worked examples; an explicit `null` decodes
    /// identically to an absent key on both sides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Timestamp>,
    /// Exclusive upper bound. See [`Self::after`] on the omit-vs-null choice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Timestamp>,
}

/// `{"requests": {"resource": …, "min": n, "max": n}}` — at least one
/// bound, both inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestsFilter {
    /// The dimension to bound.
    pub resource: RequestsResource,
    /// Inclusive lower bound. See [`SubmittedFilter::after`] on the
    /// omit-vs-null choice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u64>,
    /// Inclusive upper bound. See [`SubmittedFilter::after`] on the
    /// omit-vs-null choice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,
}

/// The requested-resource dimension a [`RequestsFilter`] bounds.
///
/// Client-authored only — this never travels on a response — so it is a
/// closed `snake_case` enum with no `Unknown` catch-all.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum RequestsResource {
    /// CPU in thousandths of a core.
    CpuMillis,
    /// Memory in bytes.
    MemoryBytes,
    /// Disk in bytes.
    DiskBytes,
}

/// `{"metadata": {"key": "…"}}` — presence; plus `"equals"` for exact
/// string equality (ADR 0042). Nothing else: a pattern match is a further
/// leaf for a future ADR, not a shape this one carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// The metadata key, under the same charset and length rules as a stored
    /// key (ADR 0042): a key that could never be stored can never match, and
    /// refusing it names the typo.
    pub key: String,
    /// The exact value to compare against, byte for byte and
    /// case-sensitive. Absent is presence: the job has the key, whatever the
    /// value. `""` is an operand like any other — the empty string is a
    /// legal stored value.
    ///
    /// Omitted, not sent as `null`, when unset. See
    /// [`SubmittedFilter::after`] on the omit-vs-null choice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equals: Option<String>,
}

impl JobFilter {
    /// AND over a non-empty list of sub-filters.
    pub fn all(filters: impl IntoIterator<Item = JobFilter>) -> JobFilter {
        JobFilter::All(filters.into_iter().collect())
    }

    /// OR over a non-empty list of sub-filters.
    pub fn any(filters: impl IntoIterator<Item = JobFilter>) -> JobFilter {
        JobFilter::Any(filters.into_iter().collect())
    }

    /// Negation.
    ///
    /// Named for the wire key it builds (`{"not": …}`), which is what makes it
    /// findable next to [`all`](Self::all) and [`any`](Self::any) — worth more
    /// here than avoiding the name it shares with `std::ops::Not::not`. This
    /// is an associated function taking a filter, not a method on one, so the
    /// two can never actually be confused at a call site.
    #[allow(clippy::should_implement_trait)]
    pub fn not(filter: JobFilter) -> JobFilter {
        JobFilter::Not(Box::new(filter))
    }

    /// Matches any of the given displayed phases.
    pub fn phase_in(phases: impl IntoIterator<Item = JobPhase>) -> JobFilter {
        JobFilter::Phase(PhaseFilter {
            r#in: phases.into_iter().collect(),
        })
    }

    /// Matches an entity and its whole subtree.
    pub fn entity(id: QuotaEntityId) -> JobFilter {
        JobFilter::Entity(EntityFilter {
            id,
            scope: EntityScope::Subtree,
        })
    }

    /// Matches an entity exactly, not its descendants.
    pub fn entity_exact(id: QuotaEntityId) -> JobFilter {
        JobFilter::Entity(EntityFilter {
            id,
            scope: EntityScope::Exact,
        })
    }

    /// Matches the current attempt's node.
    pub fn node(id: NodeId) -> JobFilter {
        JobFilter::Node(id)
    }

    /// Case-insensitive substring match on the image.
    pub fn image_contains(substring: impl Into<String>) -> JobFilter {
        JobFilter::Image(ImageFilter::Contains(substring.into()))
    }

    /// Exact match on the image.
    pub fn image_equals(image: impl Into<String>) -> JobFilter {
        JobFilter::Image(ImageFilter::Equals(image.into()))
    }

    /// Matches any of the given job ids.
    pub fn id_in(ids: impl IntoIterator<Item = JobId>) -> JobFilter {
        JobFilter::Id(IdFilter {
            r#in: ids.into_iter().collect(),
        })
    }

    /// Case-insensitive substring match over the job id string OR the image.
    pub fn search(needle: impl Into<String>) -> JobFilter {
        JobFilter::Search(needle.into())
    }

    /// Matches jobs submitted at or after `after`.
    pub fn submitted_after(after: Timestamp) -> JobFilter {
        JobFilter::Submitted(SubmittedFilter {
            after: Some(after),
            before: None,
        })
    }

    /// Matches jobs submitted strictly before `before`.
    pub fn submitted_before(before: Timestamp) -> JobFilter {
        JobFilter::Submitted(SubmittedFilter {
            after: None,
            before: Some(before),
        })
    }

    /// Matches jobs submitted in `[after, before)`.
    pub fn submitted_between(after: Timestamp, before: Timestamp) -> JobFilter {
        JobFilter::Submitted(SubmittedFilter {
            after: Some(after),
            before: Some(before),
        })
    }

    /// Exact match on the submitting principal.
    pub fn submitted_by(principal: impl Into<String>) -> JobFilter {
        JobFilter::SubmittedBy(principal.into())
    }

    /// Matches a requested-resource dimension `>= min`.
    pub fn requests_min(resource: RequestsResource, min: u64) -> JobFilter {
        JobFilter::Requests(RequestsFilter {
            resource,
            min: Some(min),
            max: None,
        })
    }

    /// Matches a requested-resource dimension `<= max`.
    pub fn requests_max(resource: RequestsResource, max: u64) -> JobFilter {
        JobFilter::Requests(RequestsFilter {
            resource,
            min: None,
            max: Some(max),
        })
    }

    /// Matches a requested-resource dimension in `[min, max]`.
    pub fn requests_between(resource: RequestsResource, min: u64, max: u64) -> JobFilter {
        JobFilter::Requests(RequestsFilter {
            resource,
            min: Some(min),
            max: Some(max),
        })
    }

    /// Matches jobs that carry `key`, whatever its value.
    pub fn metadata_present(key: impl Into<String>) -> JobFilter {
        JobFilter::Metadata(MetadataFilter {
            key: key.into(),
            equals: None,
        })
    }

    /// Matches jobs where `key` is exactly `value`.
    pub fn metadata_equals(key: impl Into<String>, value: impl Into<String>) -> JobFilter {
        JobFilter::Metadata(MetadataFilter {
            key: key.into(),
            equals: Some(value.into()),
        })
    }

    /// Enforce the shape rules `serde` cannot: non-empty combinator and `in`
    /// lists, the depth and node caps, and the at-least-one / ordered-bound
    /// rules on `submitted`/`requests`. Every violation names what was wrong,
    /// with the identical wording the server uses for the same violation.
    pub fn validate(&self) -> Result<(), String> {
        let mut nodes = 0usize;
        self.check(1, &mut nodes)
    }

    fn check(&self, depth: usize, nodes: &mut usize) -> Result<(), String> {
        if depth > MAX_FILTER_DEPTH {
            return Err(format!(
                "filter nesting exceeds the maximum depth of {MAX_FILTER_DEPTH}"
            ));
        }
        *nodes += 1;
        if *nodes > MAX_FILTER_NODES {
            return Err(format!(
                "filter exceeds the maximum of {MAX_FILTER_NODES} nodes"
            ));
        }
        match self {
            JobFilter::All(fs) => {
                if fs.is_empty() {
                    return Err("`all` filter list must be non-empty".to_string());
                }
                for f in fs {
                    f.check(depth + 1, nodes)?;
                }
            }
            JobFilter::Any(fs) => {
                if fs.is_empty() {
                    return Err("`any` filter list must be non-empty".to_string());
                }
                for f in fs {
                    f.check(depth + 1, nodes)?;
                }
            }
            JobFilter::Not(f) => f.check(depth + 1, nodes)?,
            JobFilter::Phase(p) => {
                if p.r#in.is_empty() {
                    return Err("`phase.in` must be non-empty".to_string());
                }
            }
            JobFilter::Id(i) => {
                if i.r#in.is_empty() {
                    return Err("`id.in` must be non-empty".to_string());
                }
            }
            JobFilter::Submitted(s) => {
                if s.after.is_none() && s.before.is_none() {
                    return Err("`submitted` requires at least one of `after`/`before`".to_string());
                }
                if let (Some(after), Some(before)) = (s.after, s.before) {
                    if after > before {
                        return Err(
                            "`submitted.after` must not be later than `submitted.before`"
                                .to_string(),
                        );
                    }
                }
            }
            JobFilter::Requests(r) => {
                if r.min.is_none() && r.max.is_none() {
                    return Err("`requests` requires at least one of `min`/`max`".to_string());
                }
                if let (Some(min), Some(max)) = (r.min, r.max) {
                    if min > max {
                        return Err("`requests.min` must not exceed `requests.max`".to_string());
                    }
                }
            }
            // One node against the caps above, like every other leaf.
            JobFilter::Metadata(m) => check_metadata(m)?,
            JobFilter::Entity(_)
            | JobFilter::Node(_)
            | JobFilter::Image(_)
            | JobFilter::Search(_)
            | JobFilter::SubmittedBy(_) => {}
        }
        Ok(())
    }
}

/// The metadata leaf's own rule (ADR 0042): a storable key.
///
/// Read off the domain validator rather than restated here — one charset,
/// one length bound, one error text. The `equals` operand needs no check:
/// any UTF-8 string is a legal thing to compare against, and one too long to
/// be stored simply matches nothing.
fn check_metadata(filter: &MetadataFilter) -> Result<(), String> {
    validate_key(&filter.key).map_err(|e| format!("`metadata.key` is invalid: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_phase_leaf_is_a_single_key_object() {
        let filter = JobFilter::phase_in([JobPhase::Running, JobPhase::Finalizing]);
        assert_eq!(
            serde_json::to_value(&filter).unwrap(),
            serde_json::json!({ "phase": { "in": ["running", "finalizing"] } })
        );
    }

    #[test]
    fn an_entity_leaf_defaults_to_subtree_scope() {
        let id: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let filter = JobFilter::entity(id);
        assert_eq!(
            serde_json::to_value(&filter).unwrap(),
            serde_json::json!({ "entity": { "id": id.to_string(), "scope": "subtree" } })
        );
        let exact = JobFilter::entity_exact(id);
        assert_eq!(
            serde_json::to_value(&exact).unwrap(),
            serde_json::json!({ "entity": { "id": id.to_string(), "scope": "exact" } })
        );
    }

    #[test]
    fn a_node_leaf_is_a_bare_id() {
        let node: NodeId = "node-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let filter = JobFilter::node(node);
        assert_eq!(
            serde_json::to_value(&filter).unwrap(),
            serde_json::json!({ "node": node.to_string() })
        );
    }

    #[test]
    fn an_image_leaf_carries_exactly_one_op() {
        assert_eq!(
            serde_json::to_value(JobFilter::image_contains("ubuntu")).unwrap(),
            serde_json::json!({ "image": { "contains": "ubuntu" } })
        );
        assert_eq!(
            serde_json::to_value(JobFilter::image_equals("ubuntu:22.04")).unwrap(),
            serde_json::json!({ "image": { "equals": "ubuntu:22.04" } })
        );
    }

    #[test]
    fn an_id_leaf_is_the_in_list() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000003".parse().unwrap();
        let filter = JobFilter::id_in([job]);
        assert_eq!(
            serde_json::to_value(&filter).unwrap(),
            serde_json::json!({ "id": { "in": [job.to_string()] } })
        );
    }

    #[test]
    fn a_search_leaf_is_a_bare_string() {
        assert_eq!(
            serde_json::to_value(JobFilter::search("nightly")).unwrap(),
            serde_json::json!({ "search": "nightly" })
        );
    }

    #[test]
    fn a_submitted_leaf_omits_the_unset_bound() {
        let after = Timestamp::UNIX_EPOCH;
        assert_eq!(
            serde_json::to_value(JobFilter::submitted_after(after)).unwrap(),
            serde_json::json!({ "submitted": { "after": after.to_rfc3339() } })
        );
        let before = Timestamp::from_micros(1).unwrap();
        assert_eq!(
            serde_json::to_value(JobFilter::submitted_between(after, before)).unwrap(),
            serde_json::json!({
                "submitted": { "after": after.to_rfc3339(), "before": before.to_rfc3339() }
            })
        );
    }

    #[test]
    fn a_submitted_by_leaf_is_a_bare_string() {
        assert_eq!(
            serde_json::to_value(JobFilter::submitted_by("alice@example.com")).unwrap(),
            serde_json::json!({ "submitted_by": "alice@example.com" })
        );
    }

    #[test]
    fn a_requests_leaf_omits_the_unset_bound() {
        assert_eq!(
            serde_json::to_value(JobFilter::requests_min(RequestsResource::CpuMillis, 500))
                .unwrap(),
            serde_json::json!({ "requests": { "resource": "cpu_millis", "min": 500 } })
        );
        assert_eq!(
            serde_json::to_value(JobFilter::requests_between(
                RequestsResource::MemoryBytes,
                1,
                2
            ))
            .unwrap(),
            serde_json::json!({
                "requests": { "resource": "memory_bytes", "min": 1, "max": 2 }
            })
        );
    }

    #[test]
    fn a_metadata_leaf_omits_equals_when_absent() {
        assert_eq!(
            serde_json::to_value(JobFilter::metadata_present("name")).unwrap(),
            serde_json::json!({ "metadata": { "key": "name" } })
        );
        assert_eq!(
            serde_json::to_value(JobFilter::metadata_equals("name", "nightly")).unwrap(),
            serde_json::json!({ "metadata": { "key": "name", "equals": "nightly" } })
        );
    }

    #[test]
    fn combinators_nest_by_their_own_key() {
        let filter = JobFilter::all([
            JobFilter::phase_in([JobPhase::Running]),
            JobFilter::not(JobFilter::search("x")),
        ]);
        assert_eq!(
            serde_json::to_value(&filter).unwrap(),
            serde_json::json!({
                "all": [
                    { "phase": { "in": ["running"] } },
                    { "not": { "search": "x" } },
                ]
            })
        );
    }

    #[test]
    fn validate_rejects_an_empty_combinator_list() {
        let filter = JobFilter::All(vec![]);
        assert_eq!(
            filter.validate(),
            Err("`all` filter list must be non-empty".to_string())
        );
        let filter = JobFilter::Any(vec![]);
        assert_eq!(
            filter.validate(),
            Err("`any` filter list must be non-empty".to_string())
        );
    }

    #[test]
    fn validate_rejects_an_empty_in_list() {
        assert_eq!(
            JobFilter::Phase(PhaseFilter { r#in: vec![] }).validate(),
            Err("`phase.in` must be non-empty".to_string())
        );
        assert_eq!(
            JobFilter::Id(IdFilter { r#in: vec![] }).validate(),
            Err("`id.in` must be non-empty".to_string())
        );
    }

    #[test]
    fn validate_requires_at_least_one_bound() {
        let filter = JobFilter::Submitted(SubmittedFilter {
            after: None,
            before: None,
        });
        assert_eq!(
            filter.validate(),
            Err("`submitted` requires at least one of `after`/`before`".to_string())
        );
        let filter = JobFilter::Requests(RequestsFilter {
            resource: RequestsResource::CpuMillis,
            min: None,
            max: None,
        });
        assert_eq!(
            filter.validate(),
            Err("`requests` requires at least one of `min`/`max`".to_string())
        );
    }

    #[test]
    fn validate_orders_the_submitted_bounds() {
        let after = Timestamp::from_micros(2).unwrap();
        let before = Timestamp::from_micros(1).unwrap();
        let filter = JobFilter::submitted_between(after, before);
        assert_eq!(
            filter.validate(),
            Err("`submitted.after` must not be later than `submitted.before`".to_string())
        );
    }

    #[test]
    fn validate_orders_the_requests_bounds() {
        let filter = JobFilter::requests_between(RequestsResource::CpuMillis, 5, 1);
        assert_eq!(
            filter.validate(),
            Err("`requests.min` must not exceed `requests.max`".to_string())
        );
    }

    #[test]
    fn validate_checks_the_metadata_key() {
        let filter = JobFilter::metadata_present("has space");
        let err = filter.validate().unwrap_err();
        assert!(err.starts_with("`metadata.key` is invalid: "), "{err}");
    }

    #[test]
    fn validate_enforces_the_depth_cap() {
        let mut filter = JobFilter::search("leaf");
        for _ in 0..MAX_FILTER_DEPTH {
            filter = JobFilter::not(filter);
        }
        assert_eq!(
            filter.validate(),
            Err(format!(
                "filter nesting exceeds the maximum depth of {MAX_FILTER_DEPTH}"
            ))
        );
    }

    #[test]
    fn validate_enforces_the_node_cap() {
        let filter = JobFilter::all((0..MAX_FILTER_NODES).map(|_| JobFilter::search("x")));
        assert_eq!(
            filter.validate(),
            Err(format!(
                "filter exceeds the maximum of {MAX_FILTER_NODES} nodes"
            ))
        );
    }

    #[test]
    fn a_filter_round_trips_through_json() {
        let filter = JobFilter::all([
            JobFilter::phase_in([JobPhase::Queued]),
            JobFilter::any([JobFilter::submitted_by("alice"), JobFilter::search("x")]),
        ]);
        let json = serde_json::to_string(&filter).unwrap();
        let back: JobFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(back, filter);
    }
}
