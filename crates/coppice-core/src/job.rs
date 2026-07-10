//! Job definition and lifecycle.
//!
//! The job machine is deliberately coarse and user-visible; all execution
//! detail lives on the [`crate::attempt::Attempt`]. Retries mint a new
//! `AttemptId` and return the job to [`JobState::Queued`]; an abort is the
//! [`Job::abort_requested`] flag, not a distinct state, and every attempt end
//! funnels through [`JobState::Finalizing`] where outcome, retry, and abort
//! are resolved in one place. Decided in
//! `docs/decisions/0013-job-attempt-allocation-state-machines.md`; the
//! transition table lives in `docs/lifecycle/job-lifecycle.md`.

use crate::id::{JobId, QuotaEntityId};
use crate::resource::Resources;

/// A job as submitted by a user, plus the metadata needed to schedule it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    /// Container image reference to execute.
    pub image: String,
    /// Resources requested for scheduling and isolation.
    pub requests: Resources,
    /// User-chosen priority. A multiplier on the job's cost, not a free lane:
    /// burning budget faster pushes this job forward. See
    /// `docs/decisions/0005-cost-based-soft-quotas.md`.
    pub priority: i32,
    /// Enforced runtime bound in microseconds. Part of the job's price
    /// (ADR 0005) and the license to backfill (ADR 0014); jobs without one
    /// never touch pledged capacity and are charged a policy default runtime.
    pub max_runtime_us: Option<u64>,
    /// The quota-entity leaf this job charges (every ancestor on its path is
    /// charged too).
    pub quota_entity: QuotaEntityId,
    /// Retry policy resolved at finalization (ADR 0013): platform failures
    /// retry within budget, user errors only if opted in, `Revoked` requeues
    /// free, aborts and `MaxRuntimeExceeded` never retry.
    pub retry: RetryPolicy,
    /// Set when the user has requested an abort. Legal in every non-terminal
    /// state; once set, finalization never resolves to a retry. The job only
    /// terminates as [`JobState::Aborted`] if the abort mechanism actually
    /// stopped it — a natural exit that wins the race keeps its real outcome,
    /// with this flag still visible in history.
    pub abort_requested: Option<AbortRequest>,
}

/// Per-job retry policy.
///
/// Bounds attempts beyond the first; `Revoked` outcomes never consume this
/// budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    /// Opt-in to retrying user-error outcomes (nonzero exit, OOM). Never
    /// applies to `MaxRuntimeExceeded` (deterministic recurrence) or aborts.
    pub retry_user_errors: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 3,
            retry_user_errors: false,
        }
    }
}

/// A committed user request to abort a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortRequest {
    /// Optional user-supplied reason, surfaced in job history and events.
    pub reason: Option<String>,
    /// When the abort was committed (proposer-stamped, Unix µs).
    pub requested_at_us: i64,
    // Requester identity arrives with the identity ADR; its wire tag is
    // already earmarked in coppice.core.v1.AbortRequest.
}

/// The lifecycle state of a job.
///
/// Authoritative, Raft-replicated state. Coarse by design: it stays stable
/// while the attempt machine evolves (accrual now, gang barriers later). UIs
/// join this with the live attempt's state for detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Recorded durably; awaiting admission evaluation.
    Submitted,
    /// Passed admission (synchronous in v1; distinct for observability).
    Accepted,
    /// Waiting, unpinned. Ordered by effective score, not arrival.
    Queued,
    /// An attempt exists but is not yet running (accruing, ready, or
    /// dispatching).
    Preparing,
    /// The attempt's container is running.
    Running,
    /// The attempt ended or its exit was observed; the coordinator is
    /// resolving outcome, retry policy, and any abort request.
    Finalizing,
    /// Terminal: the final attempt exited successfully.
    Succeeded,
    /// Terminal: failed and retries are exhausted or inapplicable.
    Failed,
    /// Terminal: stopped by the abort mechanism (and only then — see
    /// [`Job::abort_requested`]).
    Aborted,
}

impl JobState {
    /// Whether this state is terminal.
    ///
    /// Terminal jobs never transition again and are eventually evicted to the
    /// history store (ADR 0012).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Aborted
        )
    }

    /// The legal transition table from `docs/lifecycle/job-lifecycle.md`.
    ///
    /// The state machine rejects any edge not listed here.
    pub fn may_transition_to(self, next: JobState) -> bool {
        use JobState::*;
        match (self, next) {
            // Forward path.
            (Submitted, Accepted) | (Accepted, Queued) | (Queued, Preparing) => true,
            (Preparing, Running) | (Running, Finalizing) => true,
            // Attempt ended before running (abort, revocation, pull/start
            // failure): still funnels through finalization.
            (Preparing, Finalizing) => true,
            // Resolution: terminal outcome, or retry with a fresh attempt.
            (Finalizing, Succeeded | Failed | Aborted | Queued) => true,
            // Abort with no live attempt is immediate.
            (Submitted | Accepted | Queued, Aborted) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobState::{self, *};

    const ALL: [JobState; 9] = [
        Submitted, Accepted, Queued, Preparing, Running, Finalizing, Succeeded, Failed, Aborted,
    ];

    #[test]
    fn terminal_states_have_no_exits() {
        for from in [Succeeded, Failed, Aborted] {
            for to in ALL {
                assert!(
                    !from.may_transition_to(to),
                    "{from:?} -> {to:?} must be illegal"
                );
            }
        }
    }

    #[test]
    fn live_attempts_abort_via_finalizing_only() {
        // With an attempt in flight, Aborted is reached through resolution,
        // never directly.
        assert!(!Preparing.may_transition_to(Aborted));
        assert!(!Running.may_transition_to(Aborted));
        assert!(Finalizing.may_transition_to(Aborted));
        // With no attempt, abort is immediate.
        for from in [Submitted, Accepted, Queued] {
            assert!(from.may_transition_to(Aborted));
        }
    }

    #[test]
    fn retry_is_finalizing_to_queued() {
        assert!(Finalizing.may_transition_to(Queued));
        assert!(!Running.may_transition_to(Queued));
    }
}
