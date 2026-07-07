//! Public API request/response types.
//!
//! The API is designed around durable state transitions rather than imperative
//! manipulation of workers: submitting or aborting a job commits a desired
//! state change that agents later observe and enforce. See
//! `docs/architecture/components.md` (External API Layer).

use coppice_core::id::JobId;
use coppice_core::resource::Resources;
use serde::{Deserialize, Serialize};

/// Request to submit a new job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobRequest {
    pub image: String,
    pub requests: Resources,
    pub priority: i32,
}

/// Response to a successful job submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    pub job: JobId,
}

/// Request to abort a job. Commits a desired-state transition; it does not
/// synchronously stop the container. Legal in any non-terminal state; the
/// job terminates as `Aborted` only if this mechanism actually stopped it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortJobRequest {
    pub job: JobId,
    /// Optional reason, recorded in job history and events.
    pub reason: Option<String>,
}
