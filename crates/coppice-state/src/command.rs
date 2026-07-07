//! Commands that mutate the replicated state machine.
//!
//! Commands are the only entries in the Raft log that change authoritative
//! state. They must carry enough information to be applied deterministically by
//! any replica, including older binaries replaying old log entries — hence
//! explicit versioning is required as the schema evolves (see
//! `docs/architecture/versioning.md`).

use coppice_core::id::JobId;
use coppice_core::job::Job;
use serde::{Deserialize, Serialize};

/// A committed mutation to the authoritative state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Record a newly submitted job.
    SubmitJob { job: Job },
    /// Request cancellation of a job (a desired-state transition).
    CancelJob { job: JobId },
    /// Commit a batch of scheduler placement decisions, valid only against the
    /// expected state version.
    CommitPlacements {
        expected_version: u64,
        // Placeholder for the assignment batch produced by the scheduler.
    },
}
