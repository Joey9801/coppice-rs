//! Quota entities: the read models for the list/detail views, and the
//! `ConfigureQuotaEntity` upsert.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::entity_ref::QuotaEntityRef;
use crate::id::QuotaEntityId;
use crate::time::Timestamp;

use super::JobPhase;

/// Deserialize a float that may arrive as `null`, mapping `null` to
/// [`f64::INFINITY`].
///
/// This is the exact inverse of the server's serialization: JSON has no
/// infinity, so a non-finite float is rendered as `null`, and a plain `f64`
/// field then *fails* to read its own output back. The quota figures this
/// helper backs are legitimately infinite (an entity with zero quota and
/// nonzero usage is infinitely over quota), so every such field opts into
/// this reader; without it a client decoding these DTOs errors on a
/// perfectly valid cluster state.
///
/// Serialization is untouched — this only affects reading.
fn null_as_infinity<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<f64>::deserialize(deserializer)?.unwrap_or(f64::INFINITY))
}

/// How a quota entity came to exist.
///
/// `Sso` marks an entity auto-minted the first time the coordinator sees
/// an OIDC principal. No such minting path exists on the server yet, so
/// today every entity is `Configured`; the variant is declared so the
/// wire vocabulary is stable when it lands.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum QuotaEntityOrigin {
    /// Created or updated through `ConfigureQuotaEntity`.
    Configured,
    /// Auto-minted for an OIDC principal.
    Sso,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl QuotaEntityOrigin {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, QuotaEntityOrigin::Unknown(_))
    }
}

/// One node of the quota-entity tree, for the list/detail views.
///
/// `origin`/`principal` are the SSO provenance: replicated state records no
/// auto-minted entities yet, so `origin` is uniformly `configured` and
/// `principal` is `null` until that subsystem lands. `name` is the entity's
/// own stored segment, not a slash-joined path — no path is stored.
/// `usage_ucu`, `over_quota_ratio`, and `penalty` are decayed to read time,
/// so they are read-time figures, not the stored accumulator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuotaEntityNode {
    /// The entity's id.
    pub id: QuotaEntityId,
    /// The entity's own stored name segment.
    pub name: String,
    /// The entity's path: its ancestors' names, root first, joined by `/`
    /// (ADR 0045). Derived at read time, never stored.
    pub path: String,
    /// The parent entity; `null` roots the entity.
    pub parent: Option<QuotaEntityId>,
    /// How the entity came to exist.
    pub origin: QuotaEntityOrigin,
    /// OIDC `sub` the entity was auto-minted for; only on `sso` entities.
    pub principal: Option<String>,
    /// Soft quota as a stock in µCU.
    pub quota_ucu: u64,
    /// Decayed usage as of read time.
    pub usage_ucu: u64,
    /// How far over quota, as a ratio. Serializes as `null` when infinite
    /// (zero quota, nonzero usage) — JSON has no infinity, and the server
    /// renders a non-finite float as null; `null_as_infinity` reads that
    /// back.
    #[serde(deserialize_with = "null_as_infinity")]
    pub over_quota_ratio: f64,
    /// Multiplicative scheduling penalty ≥ 1 derived from the ratio (so also
    /// infinite, and `null` on the wire, when the ratio is).
    #[serde(deserialize_with = "null_as_infinity")]
    pub penalty: f64,
    /// When the entity was first configured.
    pub created_at: Timestamp,
    /// When the entity was last configured.
    pub updated_at: Timestamp,
    /// Live job counts over this entity's **subtree** (itself + descendants):
    /// a job under a descendant counts for every ancestor.
    pub queued_count: u32,
    /// Running job count over the subtree, on the same basis.
    pub running_count: u32,
}

/// A quota entity's core figures — no timestamps or counts — shared by
/// ancestry chains: a job's entity chain and an entity detail's own chain,
/// with usage decayed to read time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuotaEntityView {
    /// The entity's id.
    pub id: QuotaEntityId,
    /// The entity's own stored name segment.
    pub name: String,
    /// The entity's path: its ancestors' names, root first, joined by `/`
    /// (ADR 0045). Derived at read time, never stored.
    pub path: String,
    /// The parent entity; `null` roots the entity.
    pub parent: Option<QuotaEntityId>,
    /// Soft quota as a stock in µCU.
    pub quota_ucu: u64,
    /// Decayed usage as of the read's `now`.
    pub usage_ucu: u64,
    /// `null` on the wire when infinite; see `null_as_infinity`.
    #[serde(deserialize_with = "null_as_infinity")]
    pub over_quota_ratio: f64,
    /// Multiplicative scheduling penalty ≥ 1 derived from the ratio (`null`
    /// on the wire when infinite).
    #[serde(deserialize_with = "null_as_infinity")]
    pub penalty: f64,
}

/// `GET /api/v1/quota-entities` — an object envelope, never a bare array, so
/// fields can be added later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListQuotaEntitiesResponse {
    /// The tree's nodes.
    pub entities: Vec<QuotaEntityNode>,
}

/// One decayed-usage sample for an entity's sparkline. Unused today — no
/// usage-series sampler exists on the server — so
/// [`QuotaEntityStats::usage_history`] is always empty.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UsageSample {
    /// The sample's instant.
    pub t: Timestamp,
    /// Decayed usage at that instant.
    pub usage_ucu: u64,
}

/// Subtree-inclusive stats for one quota entity, tallied over the entity and
/// all its descendants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuotaEntityStats {
    /// Subtree job counts by displayed phase — every [`JobPhase`], zeros
    /// included.
    pub by_state: BTreeMap<JobPhase, u32>,
    /// Age of the longest-waiting queued job in the subtree, at read time;
    /// `null` when nothing is queued.
    #[serde(
        rename = "oldest_queued_age_seconds",
        with = "crate::time::seconds::option"
    )]
    pub oldest_queued_age: Option<std::time::Duration>,
    /// Σ µCU/s of the currently running attempts in the subtree (each
    /// attempt's recorded charge rate).
    pub burn_rate_ucu_per_second: u64,
    /// µCU charged to the subtree in the trailing 24h. **`null`**: no charge
    /// ledger exists on the server to measure it — a true-up settles against
    /// entity usage and retains no per-window total. A decision to serve
    /// null, never a fabricated 0.
    pub charged_ucu_24h: Option<u64>,
    /// Recent decayed-usage samples, oldest first. **Always empty**: no
    /// usage-series sampler exists on the server to produce them.
    pub usage_history: Vec<UsageSample>,
}

/// `GET /api/v1/quota-entities/{entity}`: the entity node, its ancestry, its
/// direct children, and its subtree stats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetQuotaEntityResponse {
    /// The entity itself.
    pub entity: QuotaEntityNode,
    /// Ancestry, root first, this entity last.
    pub chain: Vec<QuotaEntityView>,
    /// Direct children, in id order.
    pub children: Vec<QuotaEntityNode>,
    /// Subtree stats.
    pub stats: QuotaEntityStats,
}

/// `POST /api/v1/quota-entities` — the create-or-update upsert.
///
/// The caller mints `entity` and it is **required**, mirroring a job
/// submission's client-minted id: the id is the upsert's idempotency
/// identity, so a retry after an unknown outcome re-sends the same request
/// and lands on the same entity rather than creating a second one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConfigureQuotaEntityRequest {
    /// Client-minted entity id — required. The upsert target: an existing
    /// id updates, a fresh one creates.
    pub entity: QuotaEntityId,
    /// Parent in the quota tree, by id or path (ADR 0045); `null` roots the
    /// entity. A parent that does not exist, or one that would form a
    /// cycle, is rejected by the server. A path is resolved against the
    /// serving replica's read view before proposing.
    #[serde(default)]
    pub parent: Option<QuotaEntityRef>,
    /// The entity's name — one path segment (ADR 0045): 1–63 characters
    /// from `[A-Za-z0-9._-]`, the first alphanumeric, unique among its
    /// siblings. [`crate::validate_segment`] pre-checks the grammar; the
    /// sibling-uniqueness rule is enforced by the server at apply.
    pub name: String,
    /// Soft quota as a stock in µCU; the caller converts human rates.
    pub quota_ucu: u64,
}

impl ConfigureQuotaEntityRequest {
    /// A root-level (parentless) configure request. Chain [`with_parent`]
    /// to place it under another entity.
    ///
    /// [`with_parent`]: Self::with_parent
    pub fn new(entity: QuotaEntityId, name: impl Into<String>, quota_ucu: u64) -> Self {
        ConfigureQuotaEntityRequest {
            entity,
            parent: None,
            name: name.into(),
            quota_ucu,
        }
    }

    /// Place the entity under `parent` (by id or path) in the quota tree.
    pub fn with_parent(mut self, parent: impl Into<QuotaEntityRef>) -> Self {
        self.parent = Some(parent.into());
        self
    }
}

/// `POST /api/v1/quota-entities` response — the echoed entity id plus the
/// apply's `log_index`.
///
/// The write path (propose → committed decision) returns only the log
/// index, not a fresh projected view; pair the echoed id + `log_index` with
/// a strong `GET /api/v1/quota-entities/{entity}?min_index=…` for
/// read-your-writes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConfigureQuotaEntityResponse {
    /// Echo of the client-minted id from the request.
    pub entity: QuotaEntityId,
    /// The entity's path as of this write's apply (ADR 0045).
    pub path: String,
    /// Raft log index at which this upsert applied; pair with `?min_index=`
    /// on a subsequent read for read-your-writes.
    pub log_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_ref::QuotaEntityPath;

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

    #[test]
    fn quota_entity_node_serializes_to_the_contract_shape() {
        let id: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let parent: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000002"
            .parse()
            .unwrap();
        let node = QuotaEntityNode {
            id,
            name: "platform".to_string(),
            path: "acme/platform".to_string(),
            parent: Some(parent),
            origin: QuotaEntityOrigin::Configured,
            principal: None,
            quota_ucu: 1_000_000,
            usage_ucu: 500_000,
            over_quota_ratio: 0.5,
            penalty: 1.0,
            created_at: ts(1_000_000),
            updated_at: ts(9_000_000),
            queued_count: 3,
            running_count: 2,
        };
        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "quota-00000000-0000-0000-0000-000000000001",
                "name": "platform",
                "path": "acme/platform",
                "parent": "quota-00000000-0000-0000-0000-000000000002",
                "origin": "configured",
                "principal": null,
                "quota_ucu": 1_000_000,
                "usage_ucu": 500_000,
                "over_quota_ratio": 0.5,
                "penalty": 1.0,
                "created_at": "1970-01-01T00:00:01.000000Z",
                "updated_at": "1970-01-01T00:00:09.000000Z",
                "queued_count": 3,
                "running_count": 2,
            })
        );
    }

    #[test]
    fn infinite_over_quota_ratio_serializes_as_null() {
        let id: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let view = QuotaEntityView {
            id,
            name: "root".to_string(),
            path: "root".to_string(),
            parent: None,
            quota_ucu: 0,
            usage_ucu: 1,
            over_quota_ratio: f64::INFINITY,
            penalty: f64::INFINITY,
        };
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["over_quota_ratio"], serde_json::Value::Null);
        assert_eq!(json["penalty"], serde_json::Value::Null);
        assert_eq!(json["parent"], serde_json::Value::Null);
    }

    #[test]
    fn null_quota_floats_round_trip_back_to_infinity() {
        let id: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let node = QuotaEntityNode {
            id,
            name: "root".to_string(),
            path: "root".to_string(),
            parent: None,
            origin: QuotaEntityOrigin::Configured,
            principal: None,
            quota_ucu: 0,
            usage_ucu: 1,
            over_quota_ratio: f64::INFINITY,
            penalty: f64::INFINITY,
            created_at: ts(1_000_000),
            updated_at: ts(2_000_000),
            queued_count: 1,
            running_count: 0,
        };
        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["over_quota_ratio"], serde_json::Value::Null);
        assert_eq!(json["penalty"], serde_json::Value::Null);
        let back: QuotaEntityNode = serde_json::from_value(json).unwrap();
        assert_eq!(back.over_quota_ratio, f64::INFINITY);
        assert_eq!(back.penalty, f64::INFINITY);

        // A finite value is untouched by the same reader.
        let finite = QuotaEntityNode {
            over_quota_ratio: 2.5,
            penalty: 6.25,
            ..node
        };
        let json = serde_json::to_value(&finite).unwrap();
        assert_eq!(json["over_quota_ratio"], serde_json::json!(2.5));
        let back: QuotaEntityNode = serde_json::from_value(json).unwrap();
        assert_eq!(back.over_quota_ratio, 2.5);
        assert_eq!(back.penalty, 6.25);

        // The same on the chain view.
        let view: QuotaEntityView = serde_json::from_value(serde_json::json!({
            "id": id.to_string(),
            "name": "root",
            "path": "root",
            "parent": null,
            "quota_ucu": 0,
            "usage_ucu": 1,
            "over_quota_ratio": null,
            "penalty": null,
        }))
        .unwrap();
        assert_eq!(view.over_quota_ratio, f64::INFINITY);
        assert_eq!(view.penalty, f64::INFINITY);
    }

    #[test]
    fn list_quota_entities_is_an_object_envelope() {
        let json = serde_json::to_value(ListQuotaEntitiesResponse { entities: vec![] }).unwrap();
        assert_eq!(json, serde_json::json!({ "entities": [] }));
    }

    #[test]
    fn quota_entity_stats_serve_unbacked_fields_as_null_and_empty() {
        let stats = QuotaEntityStats {
            by_state: JobPhase::ALL.into_iter().map(|p| (p, 0)).collect(),
            oldest_queued_age: None,
            burn_rate_ucu_per_second: 0,
            charged_ucu_24h: None,
            usage_history: vec![],
        };
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["charged_ucu_24h"], serde_json::Value::Null);
        assert_eq!(json["usage_history"], serde_json::json!([]));
        assert_eq!(json["oldest_queued_age_seconds"], serde_json::Value::Null);
    }

    #[test]
    fn configure_request_requires_entity() {
        let entity = QuotaEntityId::new();
        // Minimal body: entity + name + quota_ucu; parent defaults to null.
        let req: ConfigureQuotaEntityRequest = serde_json::from_value(serde_json::json!({
            "entity": entity.to_string(),
            "name": "team",
            "quota_ucu": 1000,
        }))
        .expect("minimal configure request");
        assert_eq!(req.entity, entity);
        assert!(req.parent.is_none());
        assert_eq!(req.quota_ucu, 1000);

        // The client-minted id is required (idempotency identity).
        let missing_entity: Result<ConfigureQuotaEntityRequest, _> =
            serde_json::from_value(serde_json::json!({ "name": "team", "quota_ucu": 1000 }));
        assert!(missing_entity.is_err());
    }

    #[test]
    fn configure_response_serializes_id_bare_and_index_as_number() {
        let entity: QuotaEntityId = "quota-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let json = serde_json::to_value(ConfigureQuotaEntityResponse {
            entity,
            path: "acme/team".to_string(),
            log_index: 7,
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "entity": "quota-00000000-0000-0000-0000-000000000001",
                "path": "acme/team",
                "log_index": 7,
            })
        );
    }

    #[test]
    fn constructor_and_with_parent_build_the_expected_request() {
        let entity = QuotaEntityId::new();
        let parent = QuotaEntityId::new();
        let req = ConfigureQuotaEntityRequest::new(entity, "team", 1000).with_parent(parent);
        assert_eq!(req.entity, entity);
        assert_eq!(req.parent, Some(QuotaEntityRef::Id(parent)));
        assert_eq!(req.name, "team");
        assert_eq!(req.quota_ucu, 1000);
    }

    /// `with_parent` accepts a path as readily as an id (ADR 0045).
    #[test]
    fn with_parent_accepts_a_path_ref() {
        let entity = QuotaEntityId::new();
        let path: QuotaEntityPath = "acme/eng".parse().unwrap();
        let req =
            ConfigureQuotaEntityRequest::new(entity, "platform", 1000).with_parent(path.clone());
        assert_eq!(req.parent, Some(QuotaEntityRef::Path(path)));
    }
}
