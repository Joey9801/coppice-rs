//! Every endpoint's path, as a function of its typed ids.
//!
//! Paths here are **`/api/v1`-relative** — `"/jobs"`, not
//! `"https://host/api/v1/jobs"` — which is exactly what the untyped escape
//! hatch ([`Client::get_value`](crate::Client::get_value) and friends) takes.
//! That is the point of this module: a caller who needs a raw body from an
//! endpoint this crate types, or from a query parameter a newer server grew,
//! reaches the same URL the typed method does instead of re-deriving the
//! string and getting it subtly wrong.
//!
//! The typed methods are implemented on top of these functions, so the two
//! cannot drift.
//!
//! ```
//! use coppice_client::{paths, JobId};
//!
//! let job: JobId = "job-1683852a-993f-4497-a48b-6527b458fbd1".parse()?;
//! assert_eq!(paths::job_logs(job), "/jobs/job-1683852a-993f-4497-a48b-6527b458fbd1/logs");
//! # Ok::<(), coppice_client::ParseIdError>(())
//! ```
//!
//! [`HEALTHZ`] is the exception: liveness lives outside `/api/v1` and outside
//! its versioning, so it is an absolute path.

use crate::entity_ref::QuotaEntityRef;
use crate::id::{JobId, NodeId};

/// `GET /healthz` — the liveness probe. Absolute, **not** `/api/v1`-relative:
/// it is outside the JSON API and its versioning.
pub const HEALTHZ: &str = "/healthz";

/// `GET /session` — the calling credential's resolved identity and authority.
pub const SESSION: &str = "/session";

/// `GET /auth/config` — the deployment's public authentication posture.
/// Reachable without a credential.
pub const AUTH_CONFIG: &str = "/auth/config";

/// `GET`/`PUT /authorization` — the replicated role bindings.
pub const AUTHORIZATION: &str = "/authorization";

/// `GET /overview` — cluster queue and capacity headline.
pub const OVERVIEW: &str = "/overview";

/// `GET /queue/stats` — queue depth and composition on its own.
pub const QUEUE_STATS: &str = "/queue/stats";

/// `GET /jobs` (list) and `POST /jobs` (submit).
pub const JOBS: &str = "/jobs";

/// `GET /events` — the filtered job-event subscription (ADR 0043). The one
/// long-lived response on this surface: it answers `text/event-stream` and
/// stays open.
pub const EVENTS: &str = "/events";

/// `GET /nodes` — the node list.
pub const NODES: &str = "/nodes";

/// `GET /coordinators` — this replica's view of the raft cluster.
pub const COORDINATORS: &str = "/coordinators";

/// `GET /quota-entities` (list) and `POST /quota-entities` (upsert).
pub const QUOTA_ENTITIES: &str = "/quota-entities";

/// `GET /jobs/{job}` — one job's detail.
pub fn job(job: JobId) -> String {
    format!("/jobs/{job}")
}

/// `POST /jobs/{job}/abort`.
pub fn job_abort(job: JobId) -> String {
    format!("/jobs/{job}/abort")
}

/// `PUT` (replace) and `POST` (patch) `/jobs/{job}/metadata`.
pub fn job_metadata(job: JobId) -> String {
    format!("/jobs/{job}/metadata")
}

/// `GET /jobs/{job}/timeline`.
pub fn job_timeline(job: JobId) -> String {
    format!("/jobs/{job}/timeline")
}

/// `GET /jobs/{job}/logs`.
pub fn job_logs(job: JobId) -> String {
    format!("/jobs/{job}/logs")
}

/// `GET /jobs/{job}/usage`.
pub fn job_usage(job: JobId) -> String {
    format!("/jobs/{job}/usage")
}

/// `GET /nodes/{node}` — one node's detail.
pub fn node(node: NodeId) -> String {
    format!("/nodes/{node}")
}

/// `GET /nodes/{node}/utilization`.
pub fn node_utilization(node: NodeId) -> String {
    format!("/nodes/{node}/utilization")
}

/// `POST /nodes/{node}/drain` — cordon the node.
pub fn node_drain(node: NodeId) -> String {
    format!("/nodes/{node}/drain")
}

/// `POST /nodes/{node}/undrain` — lift the cordon.
pub fn node_undrain(node: NodeId) -> String {
    format!("/nodes/{node}/undrain")
}

/// `POST /nodes/{node}/remove` — evict the node's record.
pub fn node_remove(node: NodeId) -> String {
    format!("/nodes/{node}/remove")
}

/// `GET /quota-entities/{entity}` — one entity's detail.
///
/// `entity` is a ref (ADR 0045): an id or a path. Both are single URL path
/// segments once encoded: an id's alphabet (`quota-<uuid>`) is all
/// unreserved characters, and a path's grammar allows only
/// `[A-Za-z0-9._-]` and `/` — so the only character that needs escaping is
/// `/`, replaced here with its percent-encoding `%2F`. This is exact, not
/// an approximation: no other byte in either alphabet is reserved in a URL
/// path segment.
pub fn quota_entity(entity: impl Into<QuotaEntityRef>) -> String {
    let encoded = entity.into().to_string().replace('/', "%2F");
    format!("/quota-entities/{encoded}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_carry_the_typed_id_spelling() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        assert_eq!(
            super::job(job),
            "/jobs/job-00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            job_metadata(job),
            "/jobs/job-00000000-0000-0000-0000-000000000001/metadata"
        );
        let node: NodeId = "node-00000000-0000-0000-0000-000000000002".parse().unwrap();
        assert_eq!(
            node_drain(node),
            "/nodes/node-00000000-0000-0000-0000-000000000002/drain"
        );
    }

    /// An id ref needs no escaping; a path ref's `/` separators are
    /// percent-encoded so the whole ref stays one path segment (ADR 0045).
    #[test]
    fn quota_entity_percent_encodes_a_path_ref_but_not_an_id_ref() {
        let id: crate::id::QuotaEntityId = "quota-00000000-0000-0000-0000-000000000003"
            .parse()
            .unwrap();
        assert_eq!(
            quota_entity(id),
            "/quota-entities/quota-00000000-0000-0000-0000-000000000003"
        );

        let path: QuotaEntityRef = "acme/eng".parse().unwrap();
        assert_eq!(quota_entity(path), "/quota-entities/acme%2Feng");
    }
}
