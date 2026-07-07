//! Agent ↔ coordinator protocol messages.
//!
//! Coordinator-to-agent messages express *desired* state; agent-to-coordinator
//! messages report *observed* state. Every message carries identifiers that
//! make retries safe and an [`Epoch`] so agents can fence stale leaders. The
//! full protocol is described in `docs/protocols/agent-coordinator.md`.

use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::Epoch;
use serde::{Deserialize, Serialize};

/// A message sent from the coordinator to a node agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinatorToAgent {
    /// Start the given job under an allocation.
    StartJob {
        epoch: Epoch,
        job: JobId,
        allocation: AllocationId,
        attempt: AttemptId,
    },
    /// Stop the work under an allocation (SIGTERM, grace period, SIGKILL).
    /// Idempotent, and valid even if the allocation is unknown: the agent
    /// journals a tombstone so a racing `StartJob` for it is refused.
    StopJob {
        epoch: Epoch,
        allocation: AllocationId,
    },
    /// Ask the node to stop accepting new work.
    Drain { epoch: Epoch },
}

/// A message sent from a node agent to the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentToCoordinator {
    /// Periodic liveness and capacity signal.
    Heartbeat { node: NodeId, epoch: Epoch },
    /// Report an observed attempt transition. Attempt-scoped and idempotent:
    /// the attempt state machine is monotonic, so the coordinator's apply
    /// naturally drops duplicate or stale reports (ADR 0009).
    AttemptStatus {
        node: NodeId,
        allocation: AllocationId,
        attempt: AttemptId,
        /// The observed attempt state, carrying the outcome when terminal
        /// (`coppice_core::attempt::AttemptState`).
        observed: coppice_core::attempt::AttemptState,
    },
}
