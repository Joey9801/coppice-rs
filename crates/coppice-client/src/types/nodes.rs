//! Compute node read models: the list/detail views, host facts, and the
//! bodyless drain/undrain/remove admin verbs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::time::Timestamp;

use super::{AccrualView, AttemptView, Resources};

/// Liveness, derived at read time from the leader's heartbeat marks for
/// the term it currently leads: heard from within the liveness deadline
/// is `healthy`, tracked but silent for the deadline or longer is
/// `lost`.
///
/// **Naming note.** The server's own vocabulary literally has a variant
/// named `Unknown`, which collides with this crate's own `Unknown`
/// catch-all of the same name (see [`super`]). This client resolves the
/// clash by naming the known value `Unreported` instead: [`NodeHealth::Unreported`]
/// is the server's `"unknown"` verdict — the honest "no marks to judge by"
/// for a follower or stepped-down replica serving the read (the marks
/// are leader-local, like a node's `used` reading), a leader no agent
/// has reported to yet, or a node inside the grace window a new leader
/// granted it — never a fabricated `healthy`. [`NodeHealth::Unknown`] (this
/// crate's own catch-all) is a different thing entirely: a verdict this
/// client does not recognize at all, from a server vocabulary newer than
/// this crate.
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
pub enum NodeHealth {
    /// The server's `"unknown"` — no heartbeat marks to judge by.
    #[serde(rename = "unknown")]
    #[strum(serialize = "unknown")]
    Unreported,
    /// Heard from within the liveness deadline.
    Healthy,
    /// Tracked but silent for the liveness deadline or longer.
    Lost,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl NodeHealth {
    /// Whether this is the one verdict that means "heard from recently" —
    /// `Unreported` and `Lost` are both not that, and neither is a value
    /// this client does not recognize.
    pub fn is_healthy(&self) -> bool {
        matches!(self, NodeHealth::Healthy)
    }

    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, NodeHealth::Unknown(_))
    }
}

/// Summary of a compute node's current state, for the list view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NodeSummary {
    /// The node's id.
    pub id: NodeId,
    /// Registered capacity.
    pub capacity: Resources,
    /// Sum of funded resources across non-released allocations.
    pub allocated: Resources,
    /// Measured job-attributable consumption, as the node last reported it.
    /// `null` means the node is not reporting usage — a follower serving the
    /// read, an executor with no sampler, or a node whose last sample aged
    /// out. Never a zero standing in for absence.
    pub used: Option<Resources>,
    /// Operator-assigned labels.
    pub labels: BTreeMap<String, String>,
    /// The admin cordon: `false` means an operator has cordoned the node, or
    /// it has been declared lost. Survives agent restarts. A node takes new
    /// placements only when `schedulable && !draining`.
    pub schedulable: bool,
    /// The agent's own announcement that it is shutting down, cleared on
    /// re-registration. Distinct from the admin cordon: an operator did not
    /// necessarily ask for this. A node takes new placements only when
    /// `schedulable && !draining`.
    pub draining: bool,
    /// Liveness verdict.
    pub health: NodeHealth,
    /// Bumps on (re)registration or loss; fences stale agent commands.
    pub epoch: u64,
    /// Wall-clock stamp of the last report of any shape heard from this
    /// node's agent. Leader-local, like `used`: `null` on a follower, and on
    /// a leader that has not heard from the node in the term it leads —
    /// never a fabricated stamp. No staleness cutoff: the stamp stays as it
    /// ages. Display only — [`NodeHealth`] is derived from the
    /// coordinator's monotonic clock, so a wall-clock step never moves it.
    pub last_heartbeat: Option<Timestamp>,
    /// Attempts currently running on this node.
    pub running_count: u32,
    /// Allocations currently accruing on this node.
    pub accruing_count: u32,
}

/// `GET /api/v1/nodes` — an envelope, never a bare array, so fields can be
/// added later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListNodesResponse {
    /// The nodes.
    pub nodes: Vec<NodeSummary>,
}

/// `GET /api/v1/nodes/{node}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetNodeResponse {
    /// The list-view summary.
    pub summary: NodeSummary,
    /// What the node's machine is, as its agent reported at registration.
    /// Detail-only: [`NodeSummary`] stays lean because the list view renders
    /// dozens of them. `null` when the agent reported no facts.
    pub host: Option<HostFacts>,
    /// What capacity detection read on that host *before* the agent's
    /// capacity overrides. Differs from `summary.capacity` exactly when an
    /// operator overrode a dimension or the system reservation withheld
    /// room; `null` when the agent detected nothing.
    pub detected_capacity: Option<Resources>,
    /// Attempts currently dispatching/running/finalizing on this node.
    pub active_attempts: Vec<AttemptView>,
    /// Accruing allocations queued against this node, in funding order.
    pub accrual_queue: Vec<AccrualView>,
}

/// Static description of a node's machine.
///
/// Every field is best-effort: an empty string or a zero count is "the agent
/// could not read this", never a claim about the hardware. Nothing here is
/// authoritative — [`NodeSummary::capacity`] is what the node advertises,
/// and these facts only explain where that number came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HostFacts {
    /// Operating system family, e.g. `linux`, `macos`.
    pub os: String,
    /// Human-readable OS release.
    pub os_version: String,
    /// Human-readable kernel release.
    pub kernel_version: String,
    /// CPU architecture, e.g. `x86_64`, `aarch64`.
    pub arch: String,
    /// Marketing name of the CPU.
    pub cpu_model: String,
    /// Physical cores, ignoring SMT siblings. Zero = not determined.
    pub physical_cores: u32,
    /// Hardware threads the OS schedules on. Zero = not determined.
    pub logical_cores: u32,
    /// Total installed RAM in bytes. Zero = not determined.
    pub total_memory_bytes: u64,
    /// Total size of the filesystem holding the agent's data directory, in
    /// bytes. Zero = not determined.
    pub total_disk_bytes: u64,
    /// The version of the agent binary running on the node.
    pub agent_version: String,
}

/// `GET /api/v1/nodes/{node}/utilization` — one node's rolling
/// allocated-vs-used history against its capacity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetNodeUtilizationResponse {
    /// The node's registered capacity now (not per-sample: capacity only
    /// changes on re-registration).
    pub capacity: Resources,
    /// Closed buckets, oldest first. Empty on a follower and until the first
    /// bucket closes.
    pub samples: Vec<UtilizationSample>,
}

/// One closed bucket of a node's utilization history.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UtilizationSample {
    /// Bucket start.
    pub t: Timestamp,
    /// Funded resources across non-released allocations at bucket close.
    pub allocated: Resources,
    /// Measured job-attributable consumption; `null` when the node reported
    /// nothing fresh in this bucket (a gap in the chart, never a zero).
    pub used: Option<Resources>,
}

/// The answer to `POST /api/v1/nodes/{node}/drain` or `.../undrain` — always
/// empty. The node's resulting state is read back from
/// `GET /api/v1/nodes/{node}` rather than echoed here.
///
/// Both routes are bodyless (no `SetNodeSchedulableRequest` is sent on the
/// wire — that type is a server-internal seam argument, not a request
/// body), so this crate defines no request type for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DrainNodeResponse {}

/// The answer to `POST /api/v1/nodes/{node}/remove` — always empty.
///
/// Bodyless like drain/undrain (no `EvictNodeRequest` is sent on the wire —
/// that type is a server-internal seam argument, not a request body), so
/// this crate defines no request type for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RemoveNodeResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

    #[test]
    fn node_summary_serializes_to_the_contract_shape() {
        let id: NodeId = "node-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let summary = NodeSummary {
            id,
            capacity: Resources {
                cpu_millis: 4000,
                memory_bytes: 8_000_000_000,
                disk_bytes: 0,
            },
            allocated: Resources {
                cpu_millis: 1000,
                memory_bytes: 1_000_000,
                disk_bytes: 0,
            },
            used: Some(Resources {
                cpu_millis: 2_500,
                memory_bytes: 3_000_000_000,
                disk_bytes: 0,
            }),
            labels: BTreeMap::from([("zone".to_string(), "a".to_string())]),
            schedulable: true,
            draining: false,
            health: NodeHealth::Unreported,
            epoch: 3,
            last_heartbeat: None,
            running_count: 2,
            accruing_count: 1,
        };

        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "node-00000000-0000-0000-0000-000000000001",
                "capacity": { "cpu_millis": 4000, "memory_bytes": 8_000_000_000u64, "disk_bytes": 0 },
                "allocated": { "cpu_millis": 1000, "memory_bytes": 1_000_000, "disk_bytes": 0 },
                "used": { "cpu_millis": 2500, "memory_bytes": 3_000_000_000u64, "disk_bytes": 0 },
                "labels": { "zone": "a" },
                "schedulable": true,
                "draining": false,
                "health": "unknown",
                "epoch": 3,
                "last_heartbeat": null,
                "running_count": 2,
                "accruing_count": 1,
            })
        );

        let back: NodeSummary = serde_json::from_value(json).unwrap();
        assert_eq!(back, summary);
    }

    #[test]
    fn empty_list_serializes_as_an_empty_array() {
        let json = serde_json::to_value(ListNodesResponse { nodes: vec![] }).unwrap();
        assert_eq!(json, serde_json::json!({ "nodes": [] }));
    }

    #[test]
    fn the_servers_unknown_verdict_is_unreported_here() {
        let health: NodeHealth = serde_json::from_value(serde_json::json!("unknown")).unwrap();
        assert_eq!(health, NodeHealth::Unreported);
        assert!(!health.is_healthy());
        assert_eq!(
            serde_json::to_value(&health).unwrap(),
            serde_json::json!("unknown")
        );
    }

    #[test]
    fn a_verdict_this_client_does_not_know_falls_into_unknown() {
        let health: NodeHealth = serde_json::from_value(serde_json::json!("quarantined")).unwrap();
        assert_eq!(health, NodeHealth::Unknown("quarantined".to_string()));
        assert!(!health.is_healthy());
    }

    #[test]
    fn only_healthy_reports_healthy() {
        assert!(NodeHealth::Healthy.is_healthy());
        assert!(!NodeHealth::Lost.is_healthy());
        assert!(!NodeHealth::Unreported.is_healthy());
    }

    #[test]
    fn drain_and_remove_responses_are_empty_objects() {
        assert_eq!(
            serde_json::to_value(DrainNodeResponse {}).unwrap(),
            serde_json::json!({})
        );
        assert_eq!(
            serde_json::to_value(RemoveNodeResponse {}).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn utilization_sample_reports_a_gap_as_null_not_zero() {
        let sample = UtilizationSample {
            t: ts(10),
            allocated: Resources {
                cpu_millis: 1000,
                memory_bytes: 0,
                disk_bytes: 0,
            },
            used: None,
        };
        let json = serde_json::to_value(sample).unwrap();
        assert_eq!(json["used"], serde_json::Value::Null);
    }
}
