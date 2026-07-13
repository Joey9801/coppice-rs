//! Job definition and lifecycle.
//!
//! The job machine is deliberately coarse and user-visible; all execution
//! detail lives on the [`crate::attempt::Attempt`]. Its one live-execution
//! state, [`JobState::Attempting`], *carries* the attempt it points at, so the
//! job↔attempt link is the state itself rather than a field beside it: there
//! is no second attempt slot to fill and no live state without an attempt, and
//! `Attempting(a) → Attempting(b)` is illegal — a fresh `AttemptId` only ever
//! arrives by way of [`JobState::Queued`]. Retries return the job to `Queued`;
//! an abort is the [`Job::abort_requested`] flag, not a distinct state. There
//! is no job-level `Finalizing`: the window between an attempt's exit and its
//! recorded outcome is honestly `Attempting(id)` with the attempt in
//! `Finalizing`, and once the attempt reaches `Terminal` resolution (outcome,
//! retry, abort) completes atomically in the same apply. Decided in
//! `docs/decisions/0029-structural-job-attempt-link.md`, amending
//! `docs/decisions/0013-job-attempt-allocation-state-machines.md`; the
//! transition table lives in `docs/lifecycle/job-lifecycle.md`.

use crate::id::{AttemptId, JobId, QuotaEntityId};
use crate::resource::Resources;

/// A job as submitted by a user, plus the metadata needed to schedule it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    /// Container image reference to execute.
    pub image: String,
    /// The container command line, pre-tokenized (argv semantics — no shell
    /// parsing anywhere in the platform). Required: never empty, enforced at
    /// the conversion boundary.
    pub command: Vec<String>,
    /// Entrypoint override; `None` runs the image's own entrypoint. When
    /// `Some`, the argv is non-empty (also enforced at conversion) so "no
    /// override" has exactly one representation.
    pub entrypoint: Option<Vec<String>>,
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
/// while the attempt machine evolves (accrual now, gang barriers later). The
/// single live-execution state, [`JobState::Attempting`], carries the id of
/// the attempt in flight; UIs join that attempt's own state for detail
/// (preparing/running/finalizing distinctions live on the attempt now, per
/// ADR 0029).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Recorded durably; awaiting admission evaluation.
    Submitted,
    /// Passed admission (synchronous in v1; distinct for observability).
    Accepted,
    /// Waiting, unpinned. Ordered by effective score, not arrival.
    Queued,
    /// An attempt is in flight — the id it carries is the single attempt this
    /// job is pursuing (accruing, ready, dispatching, running, or finalizing;
    /// the attempt's own state carries that detail). "At most one attempt in
    /// flight" is structural: there is no second slot (ADR 0029).
    Attempting(AttemptId),
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

    /// The attempt this job points at, if any — `Some` only while
    /// [`Attempting`](JobState::Attempting).
    ///
    /// A *derived* view of the state that cannot disagree with it (ADR 0029):
    /// there is no separate `current_attempt` field to fall out of sync.
    pub fn attempt(self) -> Option<AttemptId> {
        match self {
            JobState::Attempting(id) => Some(id),
            _ => None,
        }
    }

    /// The legal transition table from `docs/lifecycle/job-lifecycle.md`.
    ///
    /// The state machine rejects any edge not listed here. Equality is
    /// payload-aware, and the table is deliberately so: `Attempting(a) →
    /// Attempting(b)` is illegal even for `a == b`, because a new attempt id
    /// only ever arrives via `Queued` (ADR 0029).
    pub fn may_transition_to(self, next: JobState) -> bool {
        use JobState::*;
        match (self, next) {
            // Forward path.
            (Submitted, Accepted) | (Accepted, Queued) => true,
            (Queued, Attempting(_)) => true,
            // Resolution at the attempt's terminal: outcome, or retry — which
            // returns to Queued so the next attempt gets a fresh id. No
            // Attempting → Attempting edge.
            (Attempting(_), Succeeded | Failed | Aborted | Queued) => true,
            // Abort with no live attempt is immediate.
            (Submitted | Accepted | Queued, Aborted) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobState::{self, *};
    use crate::id::AttemptId;
    use uuid::Uuid;

    fn att(n: u128) -> JobState {
        Attempting(AttemptId(Uuid::from_u128(n)))
    }

    #[test]
    fn terminal_states_have_no_exits() {
        let all = [
            Submitted,
            Accepted,
            Queued,
            att(1),
            Succeeded,
            Failed,
            Aborted,
        ];
        for from in [Succeeded, Failed, Aborted] {
            for to in all {
                assert!(
                    !from.may_transition_to(to),
                    "{from:?} -> {to:?} must be illegal"
                );
            }
        }
    }

    #[test]
    fn attempting_to_attempting_is_illegal() {
        // A fresh attempt id only ever arrives via Queued — not even the same
        // id may re-arm Attempting directly.
        assert!(!att(1).may_transition_to(att(2)));
        assert!(!att(1).may_transition_to(att(1)));
        assert!(att(1).may_transition_to(Queued));
        assert!(Queued.may_transition_to(att(1)));
    }

    #[test]
    fn live_attempts_reach_aborted_through_resolution_only() {
        // With an attempt in flight, every terminal (Aborted included) is
        // reached from Attempting; there is no separate mid-resolution state.
        assert!(att(1).may_transition_to(Aborted));
        assert!(att(1).may_transition_to(Succeeded));
        assert!(att(1).may_transition_to(Failed));
        // With no attempt, abort is immediate.
        for from in [Submitted, Accepted, Queued] {
            assert!(from.may_transition_to(Aborted));
        }
    }

    #[test]
    fn retry_is_attempting_to_queued() {
        assert!(att(1).may_transition_to(Queued));
        // Queued does not loop to itself; the retry lands in Queued once.
        assert!(!Queued.may_transition_to(Queued));
    }

    #[test]
    fn attempt_accessor_is_some_only_while_attempting() {
        assert_eq!(att(7).attempt(), Some(AttemptId(Uuid::from_u128(7))));
        for s in [Submitted, Accepted, Queued, Succeeded, Failed, Aborted] {
            assert_eq!(s.attempt(), None);
        }
    }
}
