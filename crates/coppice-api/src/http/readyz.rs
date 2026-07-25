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
//! closes over a plain callback the daemon owns, and the same route serves
//! the closed pre-formation surface ([`super::routes::closed_router`]) and
//! the full one.
//!
//! # Scope
//!
//! This is the ADR 0037 chunk-03 subset: the phases a daemon can be in
//! before self-join exists — [`waiting`](ReadyzPhase::Waiting),
//! [`formation-failed`](ReadyzPhase::FormationFailed), and a formed replica
//! ([`voter`](ReadyzPhase::Voter) / [`learner`](ReadyzPhase::Learner)).
//! `joining`, `?require=healthy`, the `formed` cardinality field, and
//! admission-refusal surfacing arrive with the convergence loop; the shape
//! here is the one they slot into.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use serde::{Deserialize, Serialize};

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
    /// separate question answered only by the leader; it lands with the
    /// convergence loop.
    pub fn is_ready(&self, promotion_lag_max: u64) -> bool {
        self.phase == ReadyzPhase::Voter
            && self.replication_lag <= promotion_lag_max
            && !self.leader_contact_stale
    }
}

/// The daemon-owned source of [`ReadyzReport`]s.
///
/// Mirrors [`MetricsEndpoint`](super::MetricsEndpoint): the router captures
/// it directly rather than reaching it through router state, because the
/// phases that matter most here are exactly the ones with no control plane
/// behind them.
#[derive(Clone)]
pub struct ReadyzEndpoint {
    report: Arc<dyn Fn() -> ReadyzReport + Send + Sync>,
    promotion_lag_max: u64,
}

impl ReadyzEndpoint {
    /// Build the endpoint over the daemon's own view of its phase.
    ///
    /// The callback is invoked per request and must not block: it reads a
    /// watch/atomic the daemon publishes, never consensus.
    pub fn new(
        promotion_lag_max: u64,
        report: impl Fn() -> ReadyzReport + Send + Sync + 'static,
    ) -> ReadyzEndpoint {
        ReadyzEndpoint {
            report: Arc::new(report),
            promotion_lag_max,
        }
    }

    /// An endpoint for tests and embedders with no formation state: always
    /// reports [`ReadyzPhase::Waiting`], so `/readyz` answers 503 rather than
    /// panicking or 404ing.
    pub fn detached_for_tests() -> ReadyzEndpoint {
        ReadyzEndpoint::new(0, || {
            ReadyzReport::unformed(
                "unknown".to_string(),
                ReadyzPhase::Waiting,
                0,
                Some("detached test endpoint: no daemon state attached".to_string()),
            )
        })
    }

    /// The current report.
    pub fn report(&self) -> ReadyzReport {
        (self.report)()
    }

    /// Handle one `GET /readyz`: 200 iff this replica is node-ready, 503 with
    /// the same body otherwise (ADR 0037 §9 — the body is always the full
    /// state, the status is only the gate).
    pub async fn handle(&self) -> impl IntoResponse {
        let report = self.report();
        let status = if report.is_ready(self.promotion_lag_max) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        (status, Json(report))
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
    }

    #[test]
    fn phase_names_are_the_adr_spelling() {
        let failed = ReadyzReport::unformed("p".into(), ReadyzPhase::FormationFailed, 1, None);
        let json = serde_json::to_value(&failed).expect("serialize");
        assert_eq!(json["phase"], "formation-failed");
    }
}
