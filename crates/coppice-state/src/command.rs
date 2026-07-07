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
    /// Request an abort (a desired-state transition: sets `abort_requested`).
    /// With no live attempt the job terminates as `Aborted` in the same
    /// apply; otherwise the attempt is stopped and resolution happens in
    /// `Finalizing`.
    AbortJob { job: JobId, reason: Option<String> },
    /// Commit a batch of scheduler placement decisions, valid only against the
    /// expected state version.
    CommitPlacements {
        expected_version: u64,
        // Placeholder for the placements, accruals, and revocations produced
        // by the scheduler.
    },
}
