//! Handwritten JSON DTOs for the `/api/v1` read models.
//!
//! The read-model JSON contract is owned by these types, not by protobuf:
//! they are versioned with the route prefix (`/api/v1` ⇔ this module),
//! serialized with plain serde, and mirror `web/src/api/types.ts` by name
//! and semantics (the TS side spells keys in camelCase; the web client
//! maps casing at its wire boundary). Protobuf remains the canonical format for internal RPC,
//! storage, and replication — it never leaks its wire idioms (wrapped ids,
//! stringified u64, `SCREAMING_CASE` enum names, omitted empties) into
//! these responses. Each read endpoint adds its DTOs here in the change
//! that implements it (ADR 0031, "Wire format").
//!
//! Conventions, fixed for the v1 surface:
//! - `snake_case` keys (`"cpu_millis"`) and enum values (`"unknown"`,
//!   `"oom_killed"`);
//! - ids as their typed string form (`"node-<uuid>"`, ADR 0024);
//! - integers as JSON numbers (timestamps µs, costs µCU, cpu millicores);
//! - absent optionals as explicit `null`, empty lists as `[]`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coppice_core::attempt;
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};

/// Resource quantities (mirrors `coppice_core::resource::Resources`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl From<&coppice_core::resource::Resources> for Resources {
    fn from(r: &coppice_core::resource::Resources) -> Self {
        Resources {
            cpu_millis: r.cpu_millis,
            memory_bytes: r.memory_bytes,
            disk_bytes: r.disk_bytes,
        }
    }
}

/// Liveness, eventually derived from agent heartbeats (epoch fencing per
/// ADR 0009). `Unknown` is the only value produced today: the replicated
/// state records no health input (`DeclareNodeLost` bumps the epoch and
/// clears `schedulable`, indistinguishable from an operator drain), and
/// heartbeat liveness is not wired yet — reporting `Healthy` would be a
/// lie for a definitively lost node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    Unknown,
    Healthy,
    Lost,
}

/// `coppice_core::attempt::AttemptState`, flattened for display (the
/// `Terminal` outcome payload travels separately as [`AttemptView::outcome`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Accruing,
    Ready,
    Dispatching,
    Running,
    Finalizing,
    Terminal,
}

impl From<&attempt::AttemptState> for AttemptState {
    fn from(s: &attempt::AttemptState) -> Self {
        match s {
            attempt::AttemptState::Accruing => AttemptState::Accruing,
            attempt::AttemptState::Ready => AttemptState::Ready,
            attempt::AttemptState::Dispatching => AttemptState::Dispatching,
            attempt::AttemptState::Running => AttemptState::Running,
            attempt::AttemptState::Finalizing => AttemptState::Finalizing,
            attempt::AttemptState::Terminal(_) => AttemptState::Terminal,
        }
    }
}

/// Why an attempt reached `Terminal` (mirrors
/// `coppice_core::attempt::AttemptOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcomeKind {
    Exited,
    OomKilled,
    MaxRuntimeExceeded,
    Aborted,
    Revoked,
    PullFailed,
    StartFailed,
    NodeLost,
    AgentError,
}

/// Who "owns" an outcome (drives retry policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeClass {
    Success,
    UserError,
    UserRequest,
    Platform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptOutcome {
    pub kind: AttemptOutcomeKind,
    /// Present when `kind` is `Exited`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exit_code: Option<i32>,
    pub class: OutcomeClass,
}

impl From<&attempt::AttemptOutcome> for AttemptOutcome {
    fn from(o: &attempt::AttemptOutcome) -> Self {
        use attempt::AttemptOutcome as O;
        let (kind, exit_code) = match o {
            O::Exited { code } => (AttemptOutcomeKind::Exited, Some(*code)),
            O::OomKilled => (AttemptOutcomeKind::OomKilled, None),
            O::MaxRuntimeExceeded => (AttemptOutcomeKind::MaxRuntimeExceeded, None),
            O::Aborted => (AttemptOutcomeKind::Aborted, None),
            O::Revoked => (AttemptOutcomeKind::Revoked, None),
            O::PullFailed { .. } => (AttemptOutcomeKind::PullFailed, None),
            O::StartFailed { .. } => (AttemptOutcomeKind::StartFailed, None),
            O::NodeLost => (AttemptOutcomeKind::NodeLost, None),
            O::AgentError => (AttemptOutcomeKind::AgentError, None),
        };
        let class = match o.class() {
            attempt::OutcomeClass::Success => OutcomeClass::Success,
            attempt::OutcomeClass::UserError => OutcomeClass::UserError,
            attempt::OutcomeClass::UserRequest => OutcomeClass::UserRequest,
            attempt::OutcomeClass::Platform => OutcomeClass::Platform,
        };
        AttemptOutcome {
            kind,
            exit_code,
            class,
        }
    }
}

/// `coppice_core::allocation::AllocationState` as its display union.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationState {
    Accruing,
    Funded,
    Active,
    Released,
}

impl From<coppice_core::allocation::AllocationState> for AllocationState {
    fn from(s: coppice_core::allocation::AllocationState) -> Self {
        use coppice_core::allocation::AllocationState as S;
        match s {
            S::Accruing => AllocationState::Accruing,
            S::Funded => AllocationState::Funded,
            S::Active => AllocationState::Active,
            S::Released => AllocationState::Released,
        }
    }
}

/// Read-model projection of an attempt with its charge metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptView {
    pub id: AttemptId,
    pub job: JobId,
    pub node: NodeId,
    pub allocation: AllocationId,
    pub state: AttemptState,
    /// Present iff `state` is `Terminal`.
    pub outcome: Option<AttemptOutcome>,
    pub started_at_us: Option<i64>,
    pub ended_at_us: Option<i64>,
    /// µCU per second while running (cost weights × requested resources).
    pub rate_ucu_per_second: u64,
    /// Upfront charge for this attempt (trued-up at finalization).
    pub charged_ucu: u64,
}

/// Read-model projection of an allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationView {
    pub id: AllocationId,
    pub job: JobId,
    pub attempt: AttemptId,
    pub node: NodeId,
    pub requested: Resources,
    pub funded: Resources,
    pub state: AllocationState,
    /// Commit order — drives funding priority within a node.
    pub seq: u64,
}

/// Per-dimension funding progress, 0..1.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FundedFraction {
    pub cpu: f64,
    pub memory: f64,
    pub disk: f64,
}

/// An accruing allocation with funding progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccrualView {
    pub allocation: AllocationView,
    pub funded_fraction: FundedFraction,
    /// Earliest guaranteed full-funding time; `null` means unbounded.
    pub projected_start_us: Option<i64>,
}

/// Summary of a compute node's current state for the list view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSummary {
    pub id: NodeId,
    pub capacity: Resources,
    /// Sum of funded resources across non-Released allocations.
    pub allocated: Resources,
    /// Actual measured consumption; zero until agent telemetry lands.
    pub used: Resources,
    pub labels: BTreeMap<String, String>,
    /// False = draining: no new placements, running work continues.
    pub schedulable: bool,
    pub health: NodeHealth,
    /// Bumps on (re)registration or loss; fences stale agent commands.
    pub epoch: u64,
    /// Last heartbeat from the agent; `null` until agents report.
    pub last_heartbeat_us: Option<i64>,
    /// Attempts currently `Running` on this node.
    pub running_count: u32,
    /// Allocations currently `Accruing` on this node.
    pub accruing_count: u32,
}

/// `GET /api/v1/nodes` — an envelope, never a bare array, so fields can
/// be added later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNodesResponse {
    pub nodes: Vec<NodeSummary>,
}

/// `GET /api/v1/nodes/{node}` (mirrors `NodeDetail` in `types.ts`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetNodeResponse {
    pub summary: NodeSummary,
    /// Attempts currently dispatching/running/finalizing on this node.
    pub active_attempts: Vec<AttemptView>,
    /// Accruing allocations queued against this node, in funding order.
    pub accrual_queue: Vec<AccrualView>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
            used: Resources {
                cpu_millis: 0,
                memory_bytes: 0,
                disk_bytes: 0,
            },
            labels: BTreeMap::from([("zone".to_string(), "a".to_string())]),
            schedulable: true,
            health: NodeHealth::Unknown,
            epoch: 3,
            last_heartbeat_us: None,
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
                "used": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
                "labels": { "zone": "a" },
                "schedulable": true,
                "health": "unknown",
                "epoch": 3,
                "last_heartbeat_us": null,
                "running_count": 2,
                "accruing_count": 1,
            })
        );
    }

    #[test]
    fn empty_list_serializes_as_an_empty_array() {
        let json = serde_json::to_value(ListNodesResponse { nodes: vec![] }).unwrap();
        assert_eq!(json, serde_json::json!({ "nodes": [] }));
    }

    #[test]
    fn terminal_outcome_carries_kind_class_and_exit_code() {
        let outcome: AttemptOutcome = (&attempt::AttemptOutcome::Exited { code: 3 }).into();
        let json = serde_json::to_value(outcome).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "kind": "exited", "exit_code": 3, "class": "user_error" })
        );

        // `exit_code` is an optional property, omitted — not null — when
        // the kind has none.
        let json =
            serde_json::to_value(AttemptOutcome::from(&attempt::AttemptOutcome::NodeLost)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "kind": "node_lost", "class": "platform" })
        );
    }
}
