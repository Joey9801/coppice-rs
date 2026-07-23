//! The machine-readable readiness surface (ADR 0037 §7): `GET /readyz`.
//!
//! Every replica serves `/readyz` on the client listener beside `/metrics`. It
//! answers the convergence state as JSON and distinguishes three readiness
//! questions through the status code:
//!
//! - plain `GET /readyz` → 200 iff this replica is an initialized voter whose
//!   applied index is within the promotion threshold of the leader (the ASG
//!   lifecycle-hook / load-balancer node-readiness gate);
//! - `?require=formed` → additionally requires membership cardinality
//!   (`voters ≥ cluster_size`);
//! - `?require=healthy` → additionally requires `voters_live ≥ cluster_size`
//!   sustained for a stability interval — the cluster-redundancy gate.
//!
//! `voters_live` is a leader observation (openraft replication metrics exist
//! only there). The leader answers from its own summary; a **non-leader** must
//! not guess — for `?require=healthy` it fetches a freshness-bounded snapshot
//! from the leader over the existing admin `ClusterStatus` RPC (dialing the
//! leader's raft address from local membership with this daemon's machine
//! certificate) and caches it briefly. A stale snapshot or an unreachable
//! leader is reported as `health_unknown`, never as spurious health.
//!
//! The [`ReadyzState`] gathers the inputs (the only part that touches consensus
//! and the network); [`evaluate`] is a pure function of those inputs and the
//! requested gate, unit-tested across the waiting/learner/voter × formed ×
//! live/stale matrix without any transport.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use tokio::sync::watch;

use coppice_consensus::{ClusterSummary, CoordinatorId, NodeHandle, PROMOTION_LAG_MAX};
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::TlsStore;

use coppice_api::http::{ReadyzEndpoint, ReadyzFuture};

use crate::admin::{admin_channel_from_store, cluster_status};
use crate::convergence::{ConvergenceStatus, Phase};

/// Freshness bound on a follower's cached leader health snapshot (ADR 0037 §7:
/// "cached briefly, bound ≈ 2s"). Within it every replica gives the same
/// `voters_live`; past it the snapshot is stale and health is unknown.
const HEALTH_SNAPSHOT_TTL: Duration = Duration::from_secs(2);

/// How long `voters_live ≥ cluster_size` must hold continuously before
/// `?require=healthy` passes (ADR 0037 §7, default 10s).
const HEALTH_STABILITY_INTERVAL: Duration = Duration::from_secs(10);

/// Which readiness gate a `/readyz` request asks for (ADR 0037 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Require {
    /// Plain `GET /readyz`: node readiness (initialized, caught-up voter).
    Node,
    /// `?require=formed`: node readiness **and** membership cardinality.
    Formed,
    /// `?require=healthy`: node readiness **and** sustained live redundancy.
    Healthy,
}

impl Require {
    /// Parse the raw `?require=` value. Absent is [`Require::Node`]; an
    /// unrecognized value is `Err` (the handler answers 400).
    fn parse(raw: Option<&str>) -> Result<Require, String> {
        match raw {
            None | Some("") => Ok(Require::Node),
            Some("formed") => Ok(Require::Formed),
            Some("healthy") => Ok(Require::Healthy),
            Some(other) => Err(format!(
                "unknown require value {other:?}; expected one of: formed, healthy \
                 (ADR 0037 §7)"
            )),
        }
    }
}

/// The pure inputs [`evaluate`] gates on. Everything network- and
/// consensus-derived is resolved into this plain struct by
/// [`ReadyzState::inputs`], so the gate logic is transport-free and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyzInputs {
    /// The cluster this replica is stamped for.
    pub cluster_uuid: [u8; 16],
    /// This replica's raft identity, or `None` while parked (ADR 0037 §3).
    pub node_id: Option<CoordinatorId>,
    /// The instance UUID stamped in this directory's manifest (ADR 0025).
    pub instance_uuid: [u8; 16],
    /// The published convergence phase.
    pub phase: Phase,
    /// The current leader, when known.
    pub leader: Option<CoordinatorId>,
    /// Whether this replica is the leader.
    pub is_leader: bool,
    /// This replica's highest applied log index.
    pub applied_index: u64,
    /// This replica's replication lag behind the leader (follower/learner);
    /// `None` when this replica *is* the leader.
    pub replication_lag: Option<u64>,
    /// Voter count in membership (cardinality). `formed = voters ≥ cluster_size`.
    pub voters: usize,
    /// Voters the leader observes within the promotion-lag threshold. `None`
    /// when unknown — a non-leader that did not (or could not) source it.
    pub voters_live: Option<usize>,
    /// The node-local expected voter count (ADR 0037 §2).
    pub cluster_size: usize,
    /// Whether this replica is an initialized voter, with a known leader, whose
    /// applied index is within the promotion threshold of the leader.
    pub node_ready: bool,
    /// For `?require=healthy`: `Some(true)` when `voters_live ≥ cluster_size`
    /// has held for the stability interval, `Some(false)` when known but not
    /// (yet) sustained, `None` when health is **unknown** (stale/unreachable).
    /// Ignored by the other gates.
    pub healthy_sustained: Option<bool>,
}

/// The `/readyz` JSON body (ADR 0037 §7). Present on every response, whatever
/// the status code.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyzBody {
    pub cluster_uuid: String,
    pub node_id: Option<CoordinatorId>,
    pub instance_uuid: String,
    /// Kebab-case phase: `waiting` | `joining` | `learner` | `seat-conflict` |
    /// `voter`.
    pub phase: &'static str,
    pub leader: Option<CoordinatorId>,
    pub is_leader: bool,
    pub applied_index: u64,
    pub replication_lag: Option<u64>,
    pub voters: usize,
    pub voters_live: Option<usize>,
    pub cluster_size: usize,
    pub formed: bool,
    /// A machine-readable reason a gate failed, when one applies (currently
    /// `health_unknown`); omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

/// Evaluate the readiness gate (ADR 0037 §7). Pure: the status code and body
/// are a total function of the inputs and the requested gate.
///
/// - plain: 200 iff `node_ready`.
/// - `formed`: 200 iff `node_ready` **and** `voters ≥ cluster_size`.
/// - `healthy`: 200 iff `node_ready` **and** live redundancy is sustained;
///   `health_unknown` (503) when the follower could not source `voters_live`.
pub fn evaluate(inputs: &ReadyzInputs, require: Require) -> (StatusCode, ReadyzBody) {
    let formed = inputs.voters >= inputs.cluster_size;
    let mut reason: Option<&'static str> = None;

    let ok = match require {
        Require::Node => inputs.node_ready,
        Require::Formed => inputs.node_ready && formed,
        Require::Healthy => match inputs.healthy_sustained {
            // Unknown health is not health (ADR 0037 §7): a follower that could
            // not fetch a fresh leader snapshot answers `health_unknown`.
            None => {
                reason = Some("health_unknown");
                false
            }
            Some(sustained) => inputs.node_ready && sustained,
        },
    };

    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = ReadyzBody {
        cluster_uuid: uuid_string(inputs.cluster_uuid),
        node_id: inputs.node_id,
        instance_uuid: uuid_string(inputs.instance_uuid),
        phase: phase_kebab(inputs.phase),
        leader: inputs.leader,
        is_leader: inputs.is_leader,
        applied_index: inputs.applied_index,
        replication_lag: inputs.replication_lag,
        voters: inputs.voters,
        voters_live: inputs.voters_live,
        cluster_size: inputs.cluster_size,
        formed,
        reason,
    };
    (code, body)
}

/// Everything `/readyz` needs to answer, captured behind an `Arc` and threaded
/// from bootstrap into the router — the same "captured endpoint, not part of
/// the control plane" pattern as the metrics endpoint (ADR 0037 §7).
pub struct ReadyzState {
    /// The published convergence status (phase, cluster uuid, node id).
    convergence: watch::Receiver<ConvergenceStatus>,
    /// This replica's admin handle, for the raft-level cluster summary.
    handle: NodeHandle,
    /// The shared mTLS store; a non-leader dials the leader's admin surface
    /// through it, presenting this daemon's machine certificate (ADR 0037 §6).
    tls: Arc<TlsStore>,
    /// The node-local expected voter count (ADR 0037 §2).
    cluster_size: usize,
    /// This directory's instance UUID (ADR 0025).
    instance_uuid: [u8; 16],
    /// The cluster this replica is stamped for — the identity every admin RPC
    /// carries.
    cluster_uuid: [u8; 16],
    /// The follower health-snapshot cache and stability tracking.
    health: Mutex<HealthCache>,
}

/// A follower's cached leader health snapshot plus the stability-window
/// bookkeeping for `?require=healthy`.
#[derive(Debug, Default)]
struct HealthCache {
    /// The last `voters_live` observation and when it was taken; used only by
    /// followers (a leader reads its own summary each time).
    snapshot: Option<(usize, Instant)>,
    /// When `voters_live ≥ cluster_size` was first observed continuously; reset
    /// on any dip below, or on an unknown observation.
    healthy_since: Option<Instant>,
}

impl ReadyzState {
    /// Assemble the readiness state from the pieces bootstrap already holds.
    pub fn new(
        convergence: watch::Receiver<ConvergenceStatus>,
        handle: NodeHandle,
        tls: Arc<TlsStore>,
        cluster_size: usize,
        instance_uuid: [u8; 16],
        cluster_uuid: [u8; 16],
    ) -> ReadyzState {
        ReadyzState {
            convergence,
            handle,
            tls,
            cluster_size,
            instance_uuid,
            cluster_uuid,
            health: Mutex::new(HealthCache::default()),
        }
    }

    /// Build the transport-agnostic [`ReadyzEndpoint`] the API router mounts.
    pub fn into_endpoint(self: Arc<Self>) -> ReadyzEndpoint {
        ReadyzEndpoint::new(move |raw: Option<String>| -> ReadyzFuture {
            let state = Arc::clone(&self);
            Box::pin(async move { state.respond(raw.as_deref()).await })
        })
    }

    /// Handle one `/readyz` request end to end: parse `?require=`, gather the
    /// inputs, evaluate the gate, and render the response.
    async fn respond(&self, raw_require: Option<&str>) -> Response {
        let require = match Require::parse(raw_require) {
            Ok(r) => r,
            Err(message) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ReadyzError { error: message }),
                )
                    .into_response();
            }
        };
        let inputs = self.inputs(require).await;
        let (code, body) = evaluate(&inputs, require);
        (code, Json(body)).into_response()
    }

    /// Resolve the [`ReadyzInputs`] from consensus (and, for a non-leader
    /// `?require=healthy`, the leader over the admin channel). This is the only
    /// part of the surface that touches consensus or the network.
    async fn inputs(&self, require: Require) -> ReadyzInputs {
        let status = self.convergence.borrow().clone();
        let summary = self.handle.cluster_summary();

        let local_id = summary.local_id;
        let is_leader = summary.leader == Some(local_id);
        let applied_index = summary.last_applied;
        let known_committed = summary.known_committed;

        let voter_ids: Vec<CoordinatorId> = summary
            .members
            .iter()
            .filter(|m| m.voter)
            .map(|m| m.id)
            .collect();
        let voters = voter_ids.len();
        let is_voter = voter_ids.contains(&local_id);

        // This replica's lag behind the leader: how far its applied index
        // trails the highest committed index it has heard of. Zero/absent for
        // the leader, which is the position everyone else is measured against.
        let replication_lag = if is_leader {
            None
        } else {
            Some(known_committed.saturating_sub(applied_index))
        };
        // Node readiness (the plain-200 gate): an initialized voter, with a
        // known leader, caught up within the promotion threshold.
        // (`map_or(true, …)` rather than `is_none_or`, which is above the
        // workspace MSRV.)
        let within_threshold = replication_lag.map_or(true, |lag| lag <= PROMOTION_LAG_MAX);
        let node_ready = is_voter && summary.leader.is_some() && within_threshold;

        // `voters_live`: the leader answers from its own metrics for free; a
        // non-leader sources it from the leader only when `?require=healthy`
        // actually needs it (ADR 0037 §7 — never guess, never dial needlessly).
        let voters_live = if is_leader {
            Some(live_voters_from_summary(&summary, &voter_ids))
        } else if require == Require::Healthy {
            self.leader_health_snapshot(&summary).await
        } else {
            None
        };

        // Stability tracking is only meaningful — and only updated — for the
        // healthy gate, from whatever observation we just resolved.
        let healthy_sustained = if require == Require::Healthy {
            self.track_health(voters_live)
        } else {
            None
        };

        ReadyzInputs {
            cluster_uuid: status.cluster_uuid,
            node_id: status.node_id,
            instance_uuid: self.instance_uuid,
            phase: status.phase,
            leader: summary.leader,
            is_leader,
            applied_index,
            replication_lag,
            voters,
            voters_live,
            cluster_size: self.cluster_size,
            node_ready,
            healthy_sustained,
        }
    }

    /// Fetch (or reuse a fresh cache of) the leader's `voters_live` observation
    /// over the admin `ClusterStatus` RPC (ADR 0037 §7). `None` when the leader
    /// is unknown/unreachable or the response cannot be sourced — unknown
    /// health, never a guess.
    async fn leader_health_snapshot(&self, summary: &ClusterSummary) -> Option<usize> {
        // Serve a still-fresh cached snapshot without dialing, so every replica
        // gives the same answer within the freshness bound.
        if let Some((value, taken)) = self.health.lock().expect("readyz health poisoned").snapshot {
            if taken.elapsed() <= HEALTH_SNAPSHOT_TTL {
                return Some(value);
            }
        }

        // Otherwise dial the leader's raft address from local membership and
        // ask over the admin channel with this daemon's machine certificate.
        let leader_id = summary.leader?;
        let leader_addr = summary
            .members
            .iter()
            .find(|m| m.id == leader_id)
            .map(|m| m.addr.clone())?;

        let mut client = admin_channel_from_store(&leader_addr, &self.tls)
            .await
            .ok()?;
        let resp = cluster_status(&mut client, self.cluster_uuid).await.ok()?;
        let value = live_voters_from_status(&resp);

        self.health.lock().expect("readyz health poisoned").snapshot =
            Some((value, Instant::now()));
        Some(value)
    }

    /// Fold a fresh `voters_live` observation into the stability window and
    /// report whether `?require=healthy` is satisfied (ADR 0037 §7).
    ///
    /// `None` in means the observation is unknown (stale/unreachable): the
    /// window resets and the result is unknown. `Some(n)` folds in: at or above
    /// `cluster_size` starts (or continues) the window and passes once it has
    /// held for [`HEALTH_STABILITY_INTERVAL`]; below resets it.
    fn track_health(&self, voters_live: Option<usize>) -> Option<bool> {
        let mut cache = self.health.lock().expect("readyz health poisoned");
        match voters_live {
            None => {
                cache.healthy_since = None;
                None
            }
            Some(n) if n >= self.cluster_size => {
                let since = *cache.healthy_since.get_or_insert_with(Instant::now);
                Some(since.elapsed() >= HEALTH_STABILITY_INTERVAL)
            }
            Some(_) => {
                cache.healthy_since = None;
                Some(false)
            }
        }
    }
}

/// The `/readyz` 400 body for an unknown `?require=` value.
#[derive(Debug, Serialize)]
struct ReadyzError {
    error: String,
}

/// Count the voters the leader observes within the promotion-lag threshold,
/// from this replica's own summary (called only when this replica is leader,
/// where the replication metrics exist).
fn live_voters_from_summary(summary: &ClusterSummary, voter_ids: &[CoordinatorId]) -> usize {
    let matched: BTreeMap<CoordinatorId, u64> = summary.replication.iter().copied().collect();
    count_live_voters(voter_ids, summary.leader, summary.last_applied, &matched)
}

/// Count live voters from a leader's `ClusterStatus` response (a follower's
/// freshness-bounded snapshot of the same leader observation).
fn live_voters_from_status(resp: &pb::ClusterStatusResponse) -> usize {
    let voter_ids: Vec<CoordinatorId> = resp
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.clone())
        .unwrap_or_default();
    let matched: BTreeMap<CoordinatorId, u64> = resp
        .replication
        .iter()
        .map(|r| (r.node_id, r.matched_index))
        .collect();
    count_live_voters(
        &voter_ids,
        resp.leader_node_id,
        resp.last_applied_index,
        &matched,
    )
}

/// The shared liveness rule: a voter is live if it is the leader itself (which
/// applies its own log), or the leader's replication to it is within the
/// promotion-lag threshold of the leader's applied index. A voter with no
/// replication entry yet is conservatively not counted.
fn count_live_voters(
    voter_ids: &[CoordinatorId],
    leader: Option<CoordinatorId>,
    last_applied: u64,
    matched: &BTreeMap<CoordinatorId, u64>,
) -> usize {
    voter_ids
        .iter()
        .filter(|&&v| {
            if Some(v) == leader {
                return true;
            }
            matched
                .get(&v)
                .is_some_and(|&m| last_applied.saturating_sub(m) <= PROMOTION_LAG_MAX)
        })
        .count()
}

/// The kebab-case wire spelling of a convergence phase (ADR 0037 §7).
fn phase_kebab(phase: Phase) -> &'static str {
    match phase {
        Phase::Waiting => "waiting",
        Phase::Joining => "joining",
        Phase::Learner => "learner",
        Phase::SeatConflict => "seat-conflict",
        Phase::Voter => "voter",
    }
}

/// Format 16 raw bytes as a hyphenated UUID string.
fn uuid_string(bytes: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(bytes).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A baseline set of inputs for a caught-up voter in a formed, healthy
    /// three-node cluster; individual tests mutate one axis.
    fn voter_inputs() -> ReadyzInputs {
        ReadyzInputs {
            cluster_uuid: [1u8; 16],
            node_id: Some(7),
            instance_uuid: [2u8; 16],
            phase: Phase::Voter,
            leader: Some(7),
            is_leader: true,
            applied_index: 100,
            replication_lag: None,
            voters: 3,
            voters_live: Some(3),
            cluster_size: 3,
            node_ready: true,
            healthy_sustained: Some(true),
        }
    }

    #[test]
    fn plain_gate_is_200_only_for_a_ready_voter() {
        let inputs = voter_inputs();
        let (code, body) = evaluate(&inputs, Require::Node);
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body.phase, "voter");
        assert!(body.formed);
        assert!(body.reason.is_none());
    }

    #[test]
    fn plain_gate_is_503_for_a_parked_waiting_replica() {
        let inputs = ReadyzInputs {
            phase: Phase::Waiting,
            node_id: None,
            leader: None,
            is_leader: false,
            node_ready: false,
            voters: 1,
            voters_live: None,
            replication_lag: Some(0),
            ..voter_inputs()
        };
        let (code, body) = evaluate(&inputs, Require::Node);
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.phase, "waiting");
        assert_eq!(body.node_id, None);
    }

    #[test]
    fn plain_gate_ignores_unformed_membership() {
        // A lone caught-up voter is node-ready even before the cluster fills.
        let inputs = ReadyzInputs {
            voters: 1,
            cluster_size: 3,
            ..voter_inputs()
        };
        let (code, body) = evaluate(&inputs, Require::Node);
        assert_eq!(code, StatusCode::OK);
        assert!(!body.formed);
    }

    #[test]
    fn formed_gate_flips_with_membership_cardinality() {
        let unformed = ReadyzInputs {
            voters: 2,
            cluster_size: 3,
            ..voter_inputs()
        };
        assert_eq!(
            evaluate(&unformed, Require::Formed).0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let formed = ReadyzInputs {
            voters: 3,
            cluster_size: 3,
            ..voter_inputs()
        };
        assert_eq!(evaluate(&formed, Require::Formed).0, StatusCode::OK);
    }

    #[test]
    fn learner_is_never_node_ready() {
        // A learner (not a voter) is not node-ready even when caught up.
        let inputs = ReadyzInputs {
            phase: Phase::Learner,
            is_leader: false,
            leader: Some(1),
            replication_lag: Some(0),
            node_ready: false,
            ..voter_inputs()
        };
        assert_eq!(
            evaluate(&inputs, Require::Node).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(evaluate(&inputs, Require::Node).1.phase, "learner");
    }

    #[test]
    fn healthy_gate_requires_sustained_live_redundancy() {
        // Known-but-not-yet-sustained is a plain 503 (no reason).
        let ramping = ReadyzInputs {
            healthy_sustained: Some(false),
            ..voter_inputs()
        };
        let (code, body) = evaluate(&ramping, Require::Healthy);
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.reason.is_none());

        // Sustained → 200.
        let steady = voter_inputs();
        assert_eq!(evaluate(&steady, Require::Healthy).0, StatusCode::OK);
    }

    #[test]
    fn healthy_gate_reports_health_unknown_when_the_snapshot_is_missing() {
        // A follower that could not source `voters_live` answers 503 with the
        // machine-readable `health_unknown` reason and a null `voters_live`.
        let follower = ReadyzInputs {
            is_leader: false,
            leader: Some(1),
            replication_lag: Some(0),
            voters_live: None,
            healthy_sustained: None,
            ..voter_inputs()
        };
        let (code, body) = evaluate(&follower, Require::Healthy);
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.reason, Some("health_unknown"));
        assert_eq!(body.voters_live, None);
    }

    #[test]
    fn require_parses_gates_and_rejects_unknown_values() {
        assert_eq!(Require::parse(None).unwrap(), Require::Node);
        assert_eq!(Require::parse(Some("")).unwrap(), Require::Node);
        assert_eq!(Require::parse(Some("formed")).unwrap(), Require::Formed);
        assert_eq!(Require::parse(Some("healthy")).unwrap(), Require::Healthy);
        assert!(Require::parse(Some("bogus")).is_err());
    }

    #[test]
    fn body_serializes_kebab_phase_and_omits_absent_reason() {
        let (_code, body) = evaluate(&voter_inputs(), Require::Node);
        let json = serde_json::to_value(&body).expect("serialize body");
        assert_eq!(json["phase"], "voter");
        assert_eq!(json["formed"], true);
        assert_eq!(json["voters"], 3);
        assert_eq!(json["voters_live"], 3);
        // Absent reason is omitted, not null.
        assert!(json.get("reason").is_none());
        // UUIDs render as hyphenated strings.
        assert_eq!(
            json["instance_uuid"],
            "02020202-0202-0202-0202-020202020202"
        );
    }

    #[test]
    fn seat_conflict_phase_serializes_kebab_case() {
        let inputs = ReadyzInputs {
            phase: Phase::SeatConflict,
            node_ready: false,
            ..voter_inputs()
        };
        let (_code, body) = evaluate(&inputs, Require::Node);
        assert_eq!(body.phase, "seat-conflict");
    }

    #[test]
    fn live_voter_count_applies_the_promotion_threshold() {
        let voters = vec![1u64, 2, 3];
        let mut matched = BTreeMap::new();
        // Follower 2 is caught up; follower 3 is far behind.
        matched.insert(2, 1000u64);
        matched.insert(3, 1000u64 - PROMOTION_LAG_MAX - 1);
        // Leader (1) is always live; 2 within threshold; 3 not.
        let live = count_live_voters(&voters, Some(1), 1000, &matched);
        assert_eq!(live, 2);
    }
}
