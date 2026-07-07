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
    /// Stop a running job.
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
    /// Report an observed job lifecycle transition.
    JobStatus {
        node: NodeId,
        allocation: AllocationId,
        // Placeholder: the observed status payload is defined alongside the
        // job lifecycle formalisation.
    },
}
