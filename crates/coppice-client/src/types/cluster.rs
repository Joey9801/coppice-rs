//! Cluster-wide read models: the overview (queue + capacity), and the raft
//! coordinator status roster.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::id::ClusterId;
use crate::time::Timestamp;

use super::{JobPhase, Resources};

/// The `/healthz` body's `status` field. Always `ok` on the server today;
/// carries `Unknown` anyway since a probe endpoint outside `/api/v1`'s own
/// versioning is exactly the kind of thing a future server might repurpose,
/// and this client should degrade rather than fail to decode it.
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
pub enum HealthStatus {
    /// The process answered.
    Ok,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl HealthStatus {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, HealthStatus::Unknown(_))
    }
}

/// `GET /healthz` — the whole body.
///
/// Outside `/api/v1` and its versioning; carries no readiness, phase, or
/// cluster information on purpose. Reaching the endpoint at all — a 2xx
/// response — is the real answer; the body exists only for a probe that
/// insists on matching response content rather than just the status code.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HealthzResponse {
    /// Always `"ok"` on the server.
    pub status: HealthStatus,
}

/// One point in the queue's recent history (for sparklines), oldest first —
/// one closed derived-stats bucket.
///
/// A missing instant is a missing *sample* — buckets that predate the
/// process or an event-stream gap are absent from the list (their `t`
/// simply never appears), never rendered as zeros.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueSample {
    /// The bucket's instant.
    pub t: Timestamp,
    /// The depth sampled when the bucket closed, on the same `Queued` +
    /// accruing predicate as [`QueueStats::depth`] — the two are derived
    /// from one shared server-side helper so the series and the headline
    /// can never disagree.
    pub depth: u32,
    /// Jobs draining from the queue in this bucket, per minute.
    pub drained_per_minute: f64,
    /// Jobs arriving into the queue in this bucket, per minute.
    pub arrived_per_minute: f64,
}

/// Queue depth and composition. Point-in-time fields project from
/// replicated state; the rates and `history` are **derived** fields: served
/// from the answering replica's in-memory bucket window, coverage-annotated,
/// replica-local.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueStats {
    /// Jobs waiting for capacity: those queued plus those whose current
    /// attempt is still accruing. An accruing job has a node and a partial
    /// allocation but has not started, so counting only the strictly
    /// `Queued` phase would report a drained queue while work is still
    /// waiting.
    pub depth: u32,
    /// The accruing part of [`depth`](Self::depth): jobs whose current
    /// attempt is accruing. Exactly `by_state[JobPhase::Accruing]` — a
    /// separate field so the depth figure's composition is self-describing
    /// without a client having to know the phase breakdown.
    pub accruing: u32,
    /// Jobs leaving the queue per minute over the recent window (the newest
    /// derived buckets).
    ///
    /// `null` when the window has no coverage — a freshly (re)started
    /// replica, or one that just lost the event stream — which is a gap,
    /// not the claim `0.0` would make ("nothing is draining").
    pub drain_rate_per_minute: Option<f64>,
    /// Jobs entering the queue per minute over the recent window, on the
    /// same coverage rule as `drain_rate_per_minute`.
    pub arrival_rate_per_minute: Option<f64>,
    /// Age of the longest-waiting queued job, measured at read time against
    /// the wall clock; `null` when nothing is queued.
    #[serde(
        rename = "oldest_queued_age_seconds",
        with = "crate::time::seconds::option"
    )]
    pub oldest_queued_age: Option<Duration>,
    /// Job counts by displayed phase — every [`JobPhase`], zeros included.
    pub by_state: BTreeMap<JobPhase, u32>,
    /// Recent queue history, oldest first — the retained derived buckets.
    /// Empty exactly when the rates above are `null`.
    pub history: Vec<QueueSample>,
}

/// Node counts behind the cluster's capacity totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NodeCounts {
    /// Total registered nodes.
    pub total: u32,
    /// Nodes that accept placements: neither cordoned nor agent-draining,
    /// and not lost.
    pub schedulable: u32,
    /// Nodes reported lost. Leader-local like the health it counts: a
    /// follower — with no heartbeat marks to judge by — reports 0, never a
    /// fabricated count.
    pub lost: u32,
}

/// Cluster-wide capacity, summed over the nodes in [`NodeCounts`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClusterCapacity {
    /// Node counts.
    pub nodes: NodeCounts,
    /// Registered capacity, excluding lost nodes.
    pub capacity: Resources,
    /// Sum of funded resources across non-released allocations.
    pub allocated: Resources,
    /// Sum of the reporting nodes' `used` readings, `null` when no node is
    /// reporting at all. A partial sum is still reported — `reporting_nodes`
    /// / `total_nodes` are what say how much of the cluster it actually
    /// covers, and rendering `used` without them presents a partial sum as a
    /// total.
    pub used: Option<Resources>,
    /// Nodes contributing a reading to `used` right now.
    pub reporting_nodes: u32,
    /// Nodes in the replicated state — the denominator `reporting_nodes` is
    /// partial against. Equal to `nodes.total`, repeated here so the
    /// coverage pair reads the same way as a `history[]` sample's.
    pub total_nodes: u32,
    /// The rolling capacity/allocated/used history, oldest first — the
    /// leader's in-memory usage window. Empty on a follower and until the
    /// first bucket closes; missing coverage is a missing sample, never a
    /// zero.
    pub history: Vec<CapacitySample>,
}

/// One closed bucket of the cluster's capacity history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CapacitySample {
    /// Bucket start.
    pub t: Timestamp,
    /// Registered capacity at bucket close.
    pub capacity: Resources,
    /// Sum of funded resources across non-released allocations at bucket
    /// close.
    pub allocated: Resources,
    /// Summed measured consumption over `reporting_nodes`; `null` when none
    /// of them reported in this bucket.
    pub used: Option<Resources>,
    /// Nodes that contributed a `used` reading to this bucket.
    pub reporting_nodes: u32,
    /// Nodes registered at bucket close — `reporting_nodes` out of this many
    /// is the honesty annotation on `used`.
    pub total_nodes: u32,
}

/// `GET /api/v1/overview`.
///
/// Consistency is per-field: `queue.depth`, `by_state`, and `capacity` are
/// bounded reads of replicated state, while the queue rates/history (derived
/// buckets) are derived, replica-local, and coverage-annotated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetClusterOverviewResponse {
    /// The cluster this replica belongs to (node config).
    pub cluster_id: ClusterId,
    /// Queue depth and composition.
    pub queue: QueueStats,
    /// Cluster-wide capacity.
    pub capacity: ClusterCapacity,
}

/// The serving replica's last snapshot, as far as it is knowable from
/// openraft metrics. Only the covered log index is real today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CoordinatorSnapshot {
    /// Snapshot size on disk. Always `null` on the wire: the server's
    /// snapshot metadata carries no size, and computing one would mean
    /// stat-ing snapshot files on a read path.
    pub size_bytes: Option<u64>,
    /// Log index the last snapshot covers (openraft's snapshot metric).
    pub last_included_index: u64,
    /// When the snapshot was taken. Always `null` on the wire: the server's
    /// snapshot metadata records no timestamp.
    pub taken_at: Option<Timestamp>,
    /// Applied entries since the snapshot: `last_applied − last_included_index`.
    pub entries_since_snapshot: u64,
}

/// Object counts in the replicated state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CoordinatorStateCounts {
    /// Job count.
    pub jobs: u64,
    /// Attempt count.
    pub attempts: u64,
    /// Allocation count.
    pub allocations: u64,
    /// Node count.
    pub nodes: u64,
    /// Quota entity count.
    pub quota_entities: u64,
}

/// A coordinator's role in the raft cluster, derived from the leader id
/// and its voter flag: leader if it is the current leader, learner if it
/// is a non-voter, follower otherwise.
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
pub enum CoordinatorRole {
    /// Currently leading the term.
    Leader,
    /// A voting, non-leading member.
    Follower,
    /// A non-voting member.
    Learner,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl CoordinatorRole {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, CoordinatorRole::Unknown(_))
    }
}

/// A raft member id: a random 64-bit value carried on the wire as a decimal
/// string, deliberately — a JSON number would be silently corrupted by a
/// parser that reads it as `f64` (anything above `Number.MAX_SAFE_INTEGER`,
/// as a browser's JSON parser does).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftId(pub u64);

/// A `RaftId`'s wire string was not a valid decimal `u64`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid raft id {0:?}: expected a decimal u64")]
pub struct ParseRaftIdError(String);

impl std::fmt::Display for RaftId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for RaftId {
    type Err = ParseRaftIdError;

    fn from_str(s: &str) -> Result<RaftId, ParseRaftIdError> {
        s.parse()
            .map(RaftId)
            .map_err(|_| ParseRaftIdError(s.to_string()))
    }
}

impl Serialize for RaftId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RaftId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<RaftId, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One cluster member in a [`GetCoordinatorStatusResponse`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CoordinatorMember {
    /// The member's raft id.
    pub id: RaftId,
    /// The address peers dial (host:port).
    pub addr: String,
    /// Derived role.
    pub role: CoordinatorRole,
    /// Whether the member is a voter (vs a learner).
    pub voter: bool,
    /// Highest applied index on this member: the serving replica reports
    /// its own exactly; peers are `null` (their apply progress is not
    /// tracked here — the leader observes only their *replicated* index,
    /// which feeds `replication_lag_entries` instead).
    pub last_applied: Option<u64>,
    /// Entries this member is behind the leader's committed index,
    /// leader-only; `null` on followers or for a member the leader has no
    /// replication entry for.
    pub replication_lag_entries: Option<u64>,
}

/// `GET /api/v1/coordinators` — this replica's view of the raft cluster:
/// leader/term/indexes, replicated-state counts, and the per-member roster.
///
/// Read locally off the consensus metrics and a replica-local state
/// snapshot, so every figure is "as this replica sees it" — a follower
/// answers from its own applied position, not the leader's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetCoordinatorStatusResponse {
    /// The cluster this replica belongs to (node config).
    pub cluster_id: ClusterId,
    /// The current leader's raft id, when one is known.
    pub leader: Option<RaftId>,
    /// The current raft term.
    pub term: u64,
    /// Highest committed log index known to the serving replica.
    pub known_committed: u64,
    /// Highest applied log index on the serving replica.
    pub last_applied: u64,
    /// Applied-command count on the serving replica — a state coordinate,
    /// distinct from the raft log index.
    pub state_version: u64,
    /// The last snapshot's coverage, or `null` when this replica has taken
    /// no snapshot yet.
    pub snapshot: Option<CoordinatorSnapshot>,
    /// Object counts in the replicated state machine.
    pub state_counts: CoordinatorStateCounts,
    /// One entry per configured cluster member.
    pub members: Vec<CoordinatorMember>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

    #[test]
    fn overview_serializes_to_the_contract_shape() {
        let cluster: ClusterId = "cluster-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let response = GetClusterOverviewResponse {
            cluster_id: cluster,
            queue: QueueStats {
                depth: 2,
                accruing: 1,
                drain_rate_per_minute: None,
                arrival_rate_per_minute: None,
                oldest_queued_age: Some(Duration::from_secs(5)),
                by_state: JobPhase::ALL
                    .into_iter()
                    .map(|phase| {
                        let count = u32::from(matches!(
                            phase,
                            JobPhase::Queued | JobPhase::Accruing | JobPhase::Preparing
                        ));
                        (phase, count)
                    })
                    .collect(),
                history: vec![QueueSample {
                    t: ts(10),
                    depth: 1,
                    drained_per_minute: 2.0,
                    arrived_per_minute: 4.0,
                }],
            },
            capacity: ClusterCapacity {
                nodes: NodeCounts {
                    total: 1,
                    schedulable: 1,
                    lost: 0,
                },
                capacity: Resources {
                    cpu_millis: 4000,
                    memory_bytes: 0,
                    disk_bytes: 0,
                },
                allocated: Resources {
                    cpu_millis: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                },
                used: None,
                reporting_nodes: 0,
                total_nodes: 1,
                history: vec![CapacitySample {
                    t: ts(4_970_000),
                    capacity: Resources {
                        cpu_millis: 4000,
                        memory_bytes: 0,
                        disk_bytes: 0,
                    },
                    allocated: Resources {
                        cpu_millis: 0,
                        memory_bytes: 0,
                        disk_bytes: 0,
                    },
                    used: None,
                    reporting_nodes: 0,
                    total_nodes: 1,
                }],
            },
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "cluster_id": "cluster-00000000-0000-0000-0000-000000000001",
                "queue": {
                    "depth": 2,
                    "accruing": 1,
                    "drain_rate_per_minute": null,
                    "arrival_rate_per_minute": null,
                    "oldest_queued_age_seconds": 5,
                    "by_state": {
                        "submitted": 0,
                        "accepted": 0,
                        "queued": 1,
                        "accruing": 1,
                        "preparing": 1,
                        "running": 0,
                        "finalizing": 0,
                        "succeeded": 0,
                        "failed": 0,
                        "aborted": 0,
                    },
                    "history": [{
                        "t": "1970-01-01T00:00:00.000010Z",
                        "depth": 1,
                        "drained_per_minute": 2.0,
                        "arrived_per_minute": 4.0,
                    }],
                },
                "capacity": {
                    "nodes": { "total": 1, "schedulable": 1, "lost": 0 },
                    "capacity": { "cpu_millis": 4000, "memory_bytes": 0, "disk_bytes": 0 },
                    "allocated": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                    "used": null,
                    "reporting_nodes": 0,
                    "total_nodes": 1,
                    "history": [{
                        "t": "1970-01-01T00:00:04.970000Z",
                        "capacity": { "cpu_millis": 4000, "memory_bytes": 0, "disk_bytes": 0 },
                        "allocated": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                        "used": null,
                        "reporting_nodes": 0,
                        "total_nodes": 1,
                    }],
                },
            })
        );

        let back: GetClusterOverviewResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, response);
    }

    #[test]
    fn healthz_response_deserializes_the_status_field() {
        let response: HealthzResponse =
            serde_json::from_value(serde_json::json!({ "status": "ok" })).unwrap();
        assert_eq!(response.status, HealthStatus::Ok);
    }

    #[test]
    fn coordinator_ids_are_decimal_strings_that_parse_back_to_u64() {
        let member = CoordinatorMember {
            id: RaftId(u64::MAX),
            addr: "10.0.0.1:7070".to_string(),
            role: CoordinatorRole::Follower,
            voter: true,
            last_applied: None,
            replication_lag_entries: Some(3),
        };
        assert_eq!(member.id, RaftId(u64::MAX));

        let response = GetCoordinatorStatusResponse {
            cluster_id: "cluster-00000000-0000-0000-0000-000000000001"
                .parse()
                .unwrap(),
            leader: Some(RaftId(u64::MAX)),
            term: 1,
            known_committed: 0,
            last_applied: 0,
            state_version: 0,
            snapshot: None,
            state_counts: CoordinatorStateCounts {
                jobs: 0,
                attempts: 0,
                allocations: 0,
                nodes: 0,
                quota_entities: 0,
            },
            members: vec![member],
        };
        assert_eq!(response.leader, Some(RaftId(u64::MAX)));

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["leader"], serde_json::json!("18446744073709551615"));
        assert_eq!(
            json["members"][0]["id"],
            serde_json::json!("18446744073709551615")
        );
    }

    /// `RaftId`'s `Display`/`FromStr` round trip, independent of JSON.
    #[test]
    fn raft_id_display_and_from_str_round_trip() {
        use std::str::FromStr;
        let id = RaftId(18_446_744_073_709_551_615);
        assert_eq!(id.to_string(), "18446744073709551615");
        assert_eq!(
            RaftId::from_str("18446744073709551615").unwrap(),
            RaftId(u64::MAX)
        );
        assert!(RaftId::from_str("not-a-number").is_err());
    }

    #[test]
    fn a_missing_leader_is_none() {
        let response = GetCoordinatorStatusResponse {
            cluster_id: "cluster-00000000-0000-0000-0000-000000000001"
                .parse()
                .unwrap(),
            leader: None,
            term: 0,
            known_committed: 0,
            last_applied: 0,
            state_version: 0,
            snapshot: None,
            state_counts: CoordinatorStateCounts {
                jobs: 0,
                attempts: 0,
                allocations: 0,
                nodes: 0,
                quota_entities: 0,
            },
            members: vec![],
        };
        assert_eq!(response.leader, None);
    }
}
