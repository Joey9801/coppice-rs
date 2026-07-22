//! Errors surfaced across the consensus seam.
//!
//! Every failure a caller of [`Consensus`](crate::Consensus) can observe is one
//! of these variants. openraft's own error zoo (`RaftError`, `ClientWriteError`,
//! `ForwardToLeader`, `Fatal`, …) is converted at the adapter boundary and must
//! never leak past it — see `crates/coppice-consensus/src/adapter.rs` and
//! `docs/architecture/coordinator-runtime.md`.

use crate::membership::PromotionRefusal;
use crate::CoordinatorId;

/// A failure of a consensus operation.
///
/// The retryable/terminal split is what callers branch on; see
/// [`ConsensusError::is_retryable`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConsensusError {
    /// This replica is not the leader.
    ///
    /// `leader`, when known, is where the caller should redirect; `None`
    /// means an election is in progress.
    #[error("not the leader{}", .leader.map(|l| format!(" (leader is {l})")).unwrap_or_default())]
    NotLeader { leader: Option<CoordinatorId> },

    /// The operation did not resolve in time.
    ///
    /// For [`propose`](crate::Consensus::propose) the outcome is genuinely
    /// UNKNOWN — the command may yet commit — so proposers lean on the
    /// catalog's idempotency rules rather than blindly resubmitting
    /// non-idempotent intents (command-catalog.md).
    #[error("operation timed out; outcome unknown")]
    Timeout,

    /// A joint-consensus membership change is already in flight; only one may
    /// be outstanding at a time (ADR 0016).
    #[error("a membership change is already in progress")]
    MembershipInProgress,

    /// A learner is still behind the promotion threshold and cannot yet be
    /// made a voter (ADR 0016 step 3).
    #[error("learner is {lag} entries behind the promotion threshold")]
    LearnerNotCaughtUp { lag: u64 },

    /// `AddLearner` named an id already in membership at a *different* address
    /// (ADR 0037 §4). There is no silent repointing; a moved instance is a new
    /// instance. Terminal — retrying the same request cannot succeed.
    #[error("node is already in membership at a different address ({existing_addr})")]
    SameIdDifferentAddress { existing_addr: String },

    /// A different, still-live pending learner already holds this machine
    /// identity's single replacement slot (ADR 0037 §6). Non-retryable while
    /// `incumbent` stays reachable and makes progress: the loser watches
    /// status rather than resubmitting.
    #[error("machine seat is held by a pending learner (node {incumbent})")]
    MachineSeatPending { incumbent: CoordinatorId },

    /// A promotion could not fold in a removal that ADR 0037 §5 requires, and
    /// growing the cluster is not permitted. Terminal — needs operator cleanup.
    #[error("promotion refused: {0}")]
    PromotionRefused(PromotionRefusal),

    /// A membership verb named an id that is not in membership at all
    /// (ADR 0037 §4 — `PromoteVoter` on an unknown id). Terminal.
    #[error("node {id} is not in membership")]
    UnknownNode { id: CoordinatorId },

    /// This handle's consensus node is shutting down; the operation will not
    /// complete and retrying against this handle will not help.
    #[error("consensus is shutting down")]
    Shutdown,

    /// An unrecoverable consensus fault (storage failure, core panic). Terminal
    /// for this replica.
    #[error("consensus fault: {0}")]
    Fatal(String),
}

impl ConsensusError {
    /// Whether retrying can plausibly succeed.
    ///
    /// Retryable errors resolve by redirecting ([`NotLeader`](ConsensusError::NotLeader)),
    /// waiting ([`Timeout`](ConsensusError::Timeout),
    /// [`MembershipInProgress`](ConsensusError::MembershipInProgress),
    /// [`LearnerNotCaughtUp`](ConsensusError::LearnerNotCaughtUp)), or both.
    /// [`Shutdown`](ConsensusError::Shutdown) and [`Fatal`](ConsensusError::Fatal)
    /// are terminal for this handle.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ConsensusError::NotLeader { .. }
                | ConsensusError::Timeout
                | ConsensusError::MembershipInProgress
                | ConsensusError::LearnerNotCaughtUp { .. }
        )
    }
}

/// Alias kept so proposal-path signatures read naturally.
pub type ProposeError = ConsensusError;
