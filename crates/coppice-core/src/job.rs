//! Job definition and lifecycle.
//!
//! The lifecycle enum below mirrors the states documented in
//! `docs/lifecycle/job-lifecycle.md`. Every transition has a clear owner
//! (user command, scheduler, coordinator, agent, or reconciler); the concrete
//! transition table is still being formalised.

use serde::{Deserialize, Serialize};

use crate::id::JobId;
use crate::resource::Resources;

/// A job as submitted by a user, plus the metadata needed to schedule it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    /// Container image reference to execute.
    pub image: String,
    /// Resources requested for scheduling and isolation.
    pub requests: Resources,
    /// Higher numbers schedule first, subject to fairness and quota.
    pub priority: i32,
}

/// The lifecycle state of a job.
///
/// Kept deliberately explicit so that ownership of each transition is
/// unambiguous. This is authoritative, Raft-replicated state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Submitted,
    Accepted,
    Queued,
    /// Future capacity is earmarked for this (typically large) job.
    Reserved,
    Assigned,
    Dispatching,
    Running,
    Completing,
    Succeeded,
    Failed,
    Retrying,
    Cancelled,
}
