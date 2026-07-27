//! The `GET /readyz` readiness endpoint (ADR 0037 §9).
//!
//! Log scraping is removed from every bringup workflow: a daemon's
//! convergence state is a JSON document on the client listener, beside
//! `/metrics`. Two things make that possible and both live here — the
//! [`ReadyzReport`] wire shape, and [`ReadyzEndpoint`], the same
//! captured-state seam [`MetricsEndpoint`](super::MetricsEndpoint) uses so
//! the route needs nothing from the [`ControlPlane`](crate::ControlPlane).
//!
//! That separation is not incidental. A **parked** daemon (ADR 0037 §1) has
//! no consensus replica at all, so it has no control plane to answer from —
//! yet it must still say what it is waiting for. The endpoint therefore
//! closes over two plain callbacks the daemon owns — the report and the
//! [`HealthVerdict`] — and the same route serves the closed pre-formation
//! surface ([`super::routes::closed_router`]) and the full one.
//!
//! # Scope
//!
//! The full ADR 0037 §9 surface: every [`ReadyzPhase`] a daemon can be in
//! (parked through voter), the plain node gate (`GET /readyz`), the
//! cluster-redundancy gate (`GET /readyz?require=healthy`, answered only by
//! the leader), the `formed` cardinality field (body-only, never a gate),
//! and last-admission-refusal surfacing (ADR 0037 §7).

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use serde::{Deserialize, Serialize};
use serde_json::json;

/// Where a daemon is in the ADR 0037 §1 lifecycle.
///
/// Serialized in the ADR's kebab-case spelling; automation matches on these
/// strings, so they are as much a contract as the HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadyzPhase {
    /// No manifest and no cluster found: serving the admin socket and this
    /// endpoint, waiting for an initialized cluster to appear or for a local
    /// `coppice coordinator init` (ADR 0037 §3).
    Waiting,
    /// This directory records a formation intent with no completion marker.
    /// A fail-stop with no resume path: wipe the data directory, restart,
    /// re-run `init` (ADR 0037 §3).
    FormationFailed,
    /// Admitted and catching up; reported once the convergence loop lands.
    Joining,
    /// A member of the cluster, but not a voter.
    Learner,
    /// A voter in the current membership.
    Voter,
}

/// One voter in a [`ReadyzReport`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyzVoter {
    pub node_id: u64,
    pub address: String,
}

/// The `/readyz` body (ADR 0037 §9).
///
/// Every field is present in every phase where it is knowable; the ones a
/// parked or failed daemon cannot know are omitted rather than zero-filled,
/// so automation can tell "no identity yet" from "identity 0".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyzReport {
    /// The operator-chosen logical cluster name from config (ADR 0020).
    pub cluster_id: String,
    /// The stamped raft history id (ADR 0037 §3), hex; absent before this
    /// directory is stamped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_id: Option<String>,
    /// This replica's allocate-once raft identity (ADR 0025); absent while
    /// parked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    /// The instance UUID of this data directory's current life (ADR 0025),
    /// hex; absent while parked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_uuid: Option<String>,
    pub phase: ReadyzPhase,
    /// The leader this replica last observed; absent when none is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<u64>,
    /// Whether this replica is the leader.
    pub is_leader: bool,
    /// Highest applied log index.
    pub applied_index: u64,
    /// Highest committed index this replica knows of.
    pub committed_index: u64,
    /// How far this replica's applied index trails what it knows is
    /// committed — the lag the readiness gate is taken against.
    pub replication_lag: u64,
    /// Whether this replica has lost contact with its cluster: a leader whose
    /// quorum acknowledgment has gone stale, or a follower that no longer
    /// knows a leader. Local lag alone cannot see this — a partitioned
    /// replica's applied and known-committed indexes freeze together, so its
    /// lag reads zero forever — which is why the gate consults it separately
    /// (ADR 0037 §9: readiness is distance from the *leader*, not from the
    /// replica's own frozen frontier).
    #[serde(default)]
    pub leader_contact_stale: bool,
    /// Current membership, ascending by node id; empty while parked.
    pub voters: Vec<ReadyzVoter>,
    /// The expected voter count from `[discovery] cluster_size` (ADR 0037
    /// §2). Node-local config, reported for automation that compares it with
    /// `voters`.
    pub cluster_size: usize,
    /// Why this replica is not ready, in the phases that have a reason to
    /// give — chiefly the `formation-failed` diagnostics, which are the
    /// operator's whole picture when no other surface is served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Membership cardinality has reached `cluster_size` (ADR 0037 §9) — the
    /// desired *shape*, saying nothing about health. Reported, never a gate.
    pub formed: bool,
    /// Voters the leader currently observes within the promotion-lag
    /// threshold. Only the leader can know this; absent elsewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_voters: Option<usize>,
    /// The last admission refusal this daemon received while converging — a
    /// duplicated-machine-identity refusal (ADR 0037 §7) surfaces here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_admission_refusal: Option<String>,
    /// A stable machine-readable code for why a gate failed, when one applies
    /// (currently only `health_unknown`); `reason` carries the human text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
}

impl ReadyzReport {
    /// A report for a daemon that has no consensus replica: the parked and
    /// failed phases, which differ only in phase and reason.
    pub fn unformed(
        cluster_id: String,
        phase: ReadyzPhase,
        cluster_size: usize,
        reason: Option<String>,
    ) -> ReadyzReport {
        ReadyzReport {
            cluster_id,
            history_id: None,
            node_id: None,
            instance_uuid: None,
            phase,
            leader: None,
            is_leader: false,
            applied_index: 0,
            committed_index: 0,
            replication_lag: 0,
            leader_contact_stale: false,
            voters: Vec::new(),
            cluster_size,
            reason,
            formed: false,
            live_voters: None,
            last_admission_refusal: None,
            reason_code: None,
        }
    }

    /// Node readiness (ADR 0037 §9, first gate): an initialized voter whose
    /// applied index is within the promotion threshold of the leader.
    ///
    /// "Of the leader" is approximated by two conditions that must both hold:
    /// the local lag against the known committed index is within
    /// `promotion_lag_max`, **and** the replica is in live contact with its
    /// cluster ([`leader_contact_stale`](ReadyzReport::leader_contact_stale)
    /// is false) — while contact holds, the known committed index tracks the
    /// leader's frontier with bounded staleness, and without contact the
    /// local lag is meaningless.
    ///
    /// `promotion_lag_max` is the same threshold membership uses to decide a
    /// learner has caught up, passed in so this crate does not depend on
    /// consensus. The *cluster-redundancy* gate (`?require=healthy`) is a
    /// separate question, answered by [`HealthVerdict`] and only by the
    /// leader.
    pub fn is_ready(&self, promotion_lag_max: u64) -> bool {
        self.phase == ReadyzPhase::Voter
            && self.replication_lag <= promotion_lag_max
            && !self.leader_contact_stale
    }
}

/// The cluster-redundancy answer behind `?require=healthy` (ADR 0037 §9).
/// Only the leader has replication metrics, so only the leader can answer;
/// a follower says so plainly rather than caching a stale snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthVerdict {
    /// Not the leader: this replica cannot answer. The leader hint travels
    /// in `ReadyzReport::leader`.
    Unknown,
    /// Leader: at least `cluster_size` voters within the promotion-lag
    /// threshold, sustained for the stability interval.
    Sustained { live_voters: usize },
    /// Leader: redundancy is not (yet) sustained.
    Degraded { live_voters: usize },
}

/// Machine-readable code for [`ReadyzEndpoint::handle`]'s `health_unknown`
/// refusal (ADR 0037 §9): `?require=healthy` on a replica that cannot
/// answer authoritatively.
const REASON_CODE_HEALTH_UNKNOWN: &str = "health_unknown";

/// The daemon-owned source of [`ReadyzReport`]s and [`HealthVerdict`]s.
///
/// Mirrors [`MetricsEndpoint`](super::MetricsEndpoint): the router captures
/// it directly rather than reaching it through router state, because the
/// phases that matter most here are exactly the ones with no control plane
/// behind them.
#[derive(Clone)]
pub struct ReadyzEndpoint {
    report: Arc<dyn Fn() -> ReadyzReport + Send + Sync>,
    health: Arc<dyn Fn() -> HealthVerdict + Send + Sync>,
    promotion_lag_max: u64,
}

impl ReadyzEndpoint {
    /// Build the endpoint over the daemon's own view of its phase and, on
    /// the leader, its replication health.
    ///
    /// Both callbacks are invoked per request and must not block: they read
    /// a watch/atomic the daemon publishes, never consensus directly.
    pub fn new(
        promotion_lag_max: u64,
        report: impl Fn() -> ReadyzReport + Send + Sync + 'static,
        health: impl Fn() -> HealthVerdict + Send + Sync + 'static,
    ) -> ReadyzEndpoint {
        ReadyzEndpoint {
            report: Arc::new(report),
            health: Arc::new(health),
            promotion_lag_max,
        }
    }

    /// An endpoint for tests and embedders with no formation state: always
    /// reports [`ReadyzPhase::Waiting`] and [`HealthVerdict::Unknown`], so
    /// `/readyz` answers 503 rather than panicking or 404ing.
    pub fn detached_for_tests() -> ReadyzEndpoint {
        ReadyzEndpoint::new(
            0,
            || {
                ReadyzReport::unformed(
                    "unknown".to_string(),
                    ReadyzPhase::Waiting,
                    0,
                    Some("detached test endpoint: no daemon state attached".to_string()),
                )
            },
            || HealthVerdict::Unknown,
        )
    }

    /// The current report, unmodified by any `require` gate.
    pub fn report(&self) -> ReadyzReport {
        (self.report)()
    }

    /// Handle one `GET /readyz`; `require` is the raw query value.
    ///
    /// - Absent or empty → the plain node gate: 200 iff
    ///   [`ReadyzReport::is_ready`], 503 otherwise. Body is the full report
    ///   either way.
    /// - `healthy` → additionally requires [`HealthVerdict::Sustained`] from
    ///   the leader. `Unknown` is 503 with `reason_code: "health_unknown"`;
    ///   `Degraded` is 503; `Sustained` still needs the node gate to answer
    ///   200.
    /// - Anything else → 400 with a JSON error body. There is deliberately
    ///   no `?require=formed`: `formed` is a body field, reported for
    ///   automation to read, never a gate (ADR 0037 §9).
    pub async fn handle(&self, require: Option<String>) -> Response {
        let require = require.as_deref().unwrap_or("");
        if !require.is_empty() && require != "healthy" {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!(
                        "unknown require value {require:?}; expected: healthy (ADR 0037 §9)"
                    )
                })),
            )
                .into_response();
        }

        let mut report = self.report();
        let node_ready = report.is_ready(self.promotion_lag_max);

        if require.is_empty() {
            let status = if node_ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            return (status, Json(report)).into_response();
        }

        // `require=healthy`: the cluster-redundancy gate (ADR 0037 §9),
        // answered authoritatively only by the leader.
        let status = match (self.health)() {
            HealthVerdict::Unknown => {
                report.reason_code = Some(REASON_CODE_HEALTH_UNKNOWN.to_string());
                report.reason = Some(
                    "this replica cannot answer the cluster-redundancy gate: only the leader \
                     has replication metrics; retry against the leader hinted in `leader`, or \
                     use `admin status --json`"
                        .to_string(),
                );
                StatusCode::SERVICE_UNAVAILABLE
            }
            HealthVerdict::Degraded { live_voters } => {
                report.live_voters = Some(live_voters);
                StatusCode::SERVICE_UNAVAILABLE
            }
            HealthVerdict::Sustained { live_voters } => {
                report.live_voters = Some(live_voters);
                if node_ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        };

        (status, Json(report)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voter_report(lag: u64) -> ReadyzReport {
        ReadyzReport {
            cluster_id: "prod".into(),
            history_id: Some("00".repeat(16)),
            node_id: Some(7),
            instance_uuid: Some("11".repeat(16)),
            phase: ReadyzPhase::Voter,
            leader: Some(7),
            is_leader: true,
            applied_index: 100,
            committed_index: 100 + lag,
            replication_lag: lag,
            leader_contact_stale: false,
            voters: vec![ReadyzVoter {
                node_id: 7,
                address: "c1:7071".into(),
            }],
            cluster_size: 1,
            reason: None,
            formed: true,
            live_voters: None,
            last_admission_refusal: None,
            reason_code: None,
        }
    }

    #[test]
    fn a_caught_up_voter_is_ready_and_a_lagging_one_is_not() {
        assert!(voter_report(0).is_ready(256));
        assert!(voter_report(256).is_ready(256));
        assert!(!voter_report(257).is_ready(256));
    }

    #[test]
    fn a_voter_out_of_contact_with_its_cluster_is_not_ready() {
        // The partitioned-replica case: applied and known-committed freeze
        // together, so lag reads zero — contact is the discriminating signal.
        let mut report = voter_report(0);
        report.leader_contact_stale = true;
        assert!(!report.is_ready(u64::MAX));
    }

    #[test]
    fn unformed_phases_are_never_ready_however_small_the_threshold() {
        for phase in [
            ReadyzPhase::Waiting,
            ReadyzPhase::FormationFailed,
            ReadyzPhase::Joining,
            ReadyzPhase::Learner,
        ] {
            let report = ReadyzReport::unformed("prod".into(), phase, 3, None);
            assert!(!report.is_ready(u64::MAX), "{phase:?} must not be ready");
        }
    }

    #[test]
    fn unknown_identity_is_omitted_not_zero_filled() {
        let report = ReadyzReport::unformed(
            "prod".into(),
            ReadyzPhase::Waiting,
            3,
            Some("no cluster found".into()),
        );
        let json = serde_json::to_value(&report).expect("serialize");
        assert_eq!(json["phase"], "waiting");
        assert_eq!(json["cluster_id"], "prod");
        assert_eq!(json["reason"], "no cluster found");
        assert!(json.get("node_id").is_none());
        assert!(json.get("history_id").is_none());
        assert!(json.get("instance_uuid").is_none());
        // `formed` is a plain bool, always present, never omitted.
        assert_eq!(json["formed"], false);
        // The leader-only / refusal / gate-diagnostic fields are omitted
        // when unknown, not zero/null-filled.
        assert!(json.get("live_voters").is_none());
        assert!(json.get("last_admission_refusal").is_none());
        assert!(json.get("reason_code").is_none());
    }

    #[test]
    fn phase_names_are_the_adr_spelling() {
        let failed = ReadyzReport::unformed("p".into(), ReadyzPhase::FormationFailed, 1, None);
        let json = serde_json::to_value(&failed).expect("serialize");
        assert_eq!(json["phase"], "formation-failed");
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn endpoint(report: ReadyzReport, health: HealthVerdict) -> ReadyzEndpoint {
        ReadyzEndpoint::new(256, move || report.clone(), move || health)
    }

    #[tokio::test]
    async fn plain_gate_is_unchanged_by_the_require_extension() {
        let ready = endpoint(voter_report(0), HealthVerdict::Unknown);
        let response = ready.handle(None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let mut lagging = voter_report(0);
        lagging.replication_lag = 999;
        let not_ready = endpoint(lagging, HealthVerdict::Unknown);
        let response = not_ready.handle(Some(String::new())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn require_healthy_on_a_non_leader_is_health_unknown() {
        let ep = endpoint(voter_report(0), HealthVerdict::Unknown);
        let response = ep.handle(Some("healthy".to_string())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(response).await;
        assert_eq!(body["reason_code"], "health_unknown");
        assert!(body["reason"].as_str().unwrap().contains("leader"));
    }

    #[tokio::test]
    async fn require_healthy_degraded_is_503_with_live_voters() {
        let ep = endpoint(voter_report(0), HealthVerdict::Degraded { live_voters: 1 });
        let response = ep.handle(Some("healthy".to_string())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(response).await;
        assert_eq!(body["live_voters"], 1);
    }

    #[tokio::test]
    async fn require_healthy_sustained_and_node_ready_is_200() {
        let ep = endpoint(voter_report(0), HealthVerdict::Sustained { live_voters: 3 });
        let response = ep.handle(Some("healthy".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["live_voters"], 3);
    }

    #[tokio::test]
    async fn require_healthy_sustained_but_node_not_ready_is_503() {
        let mut lagging = voter_report(0);
        lagging.replication_lag = 999;
        let ep = endpoint(lagging, HealthVerdict::Sustained { live_voters: 3 });
        let response = ep.handle(Some("healthy".to_string())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(response).await;
        // Still populated even though the overall gate failed.
        assert_eq!(body["live_voters"], 3);
    }

    #[tokio::test]
    async fn unknown_require_value_is_400() {
        let ep = endpoint(voter_report(0), HealthVerdict::Unknown);
        let response = ep.handle(Some("formed".to_string())).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_json(response).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("expected: healthy"));
    }

    #[tokio::test]
    async fn last_admission_refusal_is_omitted_when_absent_and_present_when_set() {
        let ep = endpoint(voter_report(0), HealthVerdict::Unknown);
        let body = body_json(ep.handle(None).await).await;
        assert!(body.get("last_admission_refusal").is_none());

        let mut refused = voter_report(0);
        refused.last_admission_refusal =
            Some("duplicated machine identity for node 7 (ADR 0037 §7)".to_string());
        let ep = endpoint(refused, HealthVerdict::Unknown);
        let body = body_json(ep.handle(None).await).await;
        assert_eq!(
            body["last_admission_refusal"],
            "duplicated machine identity for node 7 (ADR 0037 §7)"
        );
    }
}
