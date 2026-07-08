//! Attempts: one execution of a job.
//!
//! Retries mint a new attempt with a fresh [`AttemptId`]; the job keeps its
//! identity. All agent reports are attempt-scoped, which is what makes
//! duplicate and stale reports safe to drop (the attempt machine is
//! monotonic). Decided in
//! `docs/decisions/0013-job-attempt-allocation-state-machines.md`.

use crate::id::{AllocationId, AttemptId, JobId, NodeId};

/// One execution of a job. Authoritative, Raft-replicated state.
///
/// v1 attempts hold exactly one allocation; gang scheduling later spans a
/// placement group of attempts behind the same [`AttemptState::Ready`]
/// barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub id: AttemptId,
    pub job: JobId,
    pub allocation: AllocationId,
    pub node: NodeId,
    pub state: AttemptState,
}

/// The attempt state machine.
///
/// `Accruing → Ready → Dispatching → Running → Finalizing → Terminal`, plus a
/// direct edge from every non-terminal state to `Terminal` for early endings
/// (abort before start, revocation, pull/start failure, node lost).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptState {
    /// Allocations committed but not all funded. Skipped when capacity is
    /// immediately available (the common case).
    Accruing,
    /// The barrier: all allocations funded, dispatch may begin. Defined over
    /// a placement group; v1 groups are singletons.
    Ready,
    /// `StartJob` sent to the agent.
    Dispatching,
    /// Container observed running.
    Running,
    /// Exit observed; agent-side finalization (log flush, usage summary).
    Finalizing,
    /// Terminal, with the recorded outcome.
    Terminal(AttemptOutcome),
}

impl AttemptState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, AttemptState::Terminal(_))
    }

    /// The legal transition table: the forward chain, plus early termination
    /// from any non-terminal state.
    pub fn may_transition_to(&self, next: &AttemptState) -> bool {
        use AttemptState::*;
        if self.is_terminal() {
            return false;
        }
        if matches!(next, Terminal(_)) {
            return true;
        }
        matches!(
            (self, next),
            (Accruing, Ready) | (Ready, Dispatching) | (Dispatching, Running) | (Running, Finalizing)
        )
    }
}

/// Why an attempt ended.
///
/// Recorded on every terminal attempt so that "what stopped this job" is
/// always answerable from state, never inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The container exited on its own with this code.
    Exited { code: i32 },
    /// Killed by the kernel for exceeding its memory limit.
    OomKilled,
    /// Killed by the agent for exceeding its enforced `max_runtime`.
    MaxRuntimeExceeded,
    /// Terminated by the abort mechanism ([`crate::job::AbortRequest`]).
    Aborted,
    /// Revoked by the scheduler while accruing, to re-plan. Requeued without
    /// consuming retry budget.
    Revoked,
    /// The image could not be pulled. `user_error` distinguishes a bad image
    /// reference from registry/platform trouble.
    PullFailed { user_error: bool },
    /// The container could not be started.
    StartFailed { user_error: bool },
    /// The node was declared lost while the attempt was live.
    NodeLost,
    /// The agent failed in a way not attributable to the workload.
    AgentError,
}

/// Who or what an outcome is attributed to.
///
/// Drives default retry policy: platform failures retry, user errors don't
/// (job policy may opt in), user requests never do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeClass {
    Success,
    UserError,
    UserRequest,
    Platform,
}

impl AttemptOutcome {
    pub fn class(&self) -> OutcomeClass {
        use AttemptOutcome::*;
        match self {
            Exited { code: 0 } => OutcomeClass::Success,
            Exited { .. } | OomKilled | MaxRuntimeExceeded => OutcomeClass::UserError,
            Aborted => OutcomeClass::UserRequest,
            PullFailed { user_error } | StartFailed { user_error } => {
                if *user_error {
                    OutcomeClass::UserError
                } else {
                    OutcomeClass::Platform
                }
            }
            Revoked | NodeLost | AgentError => OutcomeClass::Platform,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_non_terminal_state_can_end_early() {
        use AttemptState::*;
        let terminal = Terminal(AttemptOutcome::Aborted);
        for state in [Accruing, Ready, Dispatching, Running, Finalizing] {
            assert!(state.may_transition_to(&terminal), "{state:?} must be able to end early");
        }
        assert!(!terminal.may_transition_to(&Terminal(AttemptOutcome::NodeLost)));
    }

    #[test]
    fn forward_chain_only_moves_forward() {
        use AttemptState::*;
        assert!(Accruing.may_transition_to(&Ready));
        assert!(!Ready.may_transition_to(&Accruing));
        assert!(!Accruing.may_transition_to(&Dispatching));
    }

    #[test]
    fn outcome_classes_match_the_adr_table() {
        use AttemptOutcome::*;
        assert_eq!(Exited { code: 0 }.class(), OutcomeClass::Success);
        assert_eq!(Exited { code: 137 }.class(), OutcomeClass::UserError);
        assert_eq!(Aborted.class(), OutcomeClass::UserRequest);
        assert_eq!(Revoked.class(), OutcomeClass::Platform);
        assert_eq!(PullFailed { user_error: true }.class(), OutcomeClass::UserError);
        assert_eq!(PullFailed { user_error: false }.class(), OutcomeClass::Platform);
    }
}
