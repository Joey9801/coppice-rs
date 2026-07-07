//! # coppice-api
//!
//! The external API layer — the user-facing entry point for job submission,
//! abort, status queries, event subscriptions, and administrative
//! actions. The web UI and CLI are both built on this same surface.
//!
//! The API runs on every coordinator replica. Read-only requests may be served
//! by followers when suitably fresh reads are acceptable; mutating requests are
//! routed or forwarded to the current Raft leader. Endpoints are modelled as
//! durable state transitions, not imperative worker control. See
//! `docs/architecture/components.md` and `docs/operations/security.md`.

use coppice_proto::api::{AbortJobRequest, SubmitJobRequest, SubmitJobResponse};

/// Errors surfaced to API callers.
#[derive(Debug)]
pub enum ApiError {
    /// This replica is not the leader; the caller should retry against the
    /// address carried here.
    NotLeader { leader_hint: Option<String> },
    /// The request failed validation.
    Invalid(String),
}

/// The set of operations the API layer exposes. Implemented by the coordinator,
/// which owns access to consensus and state.
pub trait ControlPlane {
    fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError>;
    fn abort_job(&self, req: AbortJobRequest) -> Result<(), ApiError>;
}
