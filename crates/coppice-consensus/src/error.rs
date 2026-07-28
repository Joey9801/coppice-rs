//! Errors surfaced across the consensus seam.
//!
//! Every failure a caller of [`Consensus`](crate::Consensus) can observe is one
//! of these variants. openraft's own error zoo (`RaftError`, `ClientWriteError`,
//! `ForwardToLeader`, `Fatal`, …) is converted at the adapter boundary and must
//! never leak past it — see `crates/coppice-consensus/src/adapter.rs` and
//! `docs/architecture/coordinator-runtime.md`.

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

    /// The verb named a node id membership has never heard of (ADR 0037 §6).
    ///
    /// Distinct from [`LearnerNotCaughtUp`](ConsensusError::LearnerNotCaughtUp)
    /// on purpose: "promote a node that was never admitted" is an operator or
    /// caller error that no amount of waiting fixes, and the convergence loop
    /// must not poll on it.
    #[error("node {node} is not in membership")]
    UnknownNode { node: CoordinatorId },

    /// An `AddLearner` named a node id already in membership at a *different*
    /// address (ADR 0037 §6). There is no silent repointing: an instance whose
    /// address changed is a new instance, and a deliberate move goes through
    /// the operator-only `set-address` verb.
    #[error(
        "node {node} is already in membership at {current}; refusing to repoint it to \
         {requested} (ADR 0037 §6 — there is no silent address repair)"
    )]
    AddressConflict {
        node: CoordinatorId,
        current: String,
        requested: String,
    },

    /// The voter set is already at `cluster_size` and the candidate is not in
    /// it (ADR 0037 §7). The learner stays a caught-up learner and keeps
    /// polling; it is then either the `new_node_id` of a pending
    /// ReplaceVoter or waiting on evidence-gated removal — both chunk 06.
    #[error("voter set is full ({voters}/{cluster_size} voters); node {node} remains a learner")]
    VoterSetFull {
        node: CoordinatorId,
        voters: usize,
        cluster_size: usize,
    },

    /// A promotion would exceed `cluster_size` and no voter qualifies as
    /// evidence-dead — or removing the one that does would still leave the
    /// set too large (ADR 0037 §7 "the hands-off path"). Retryable exactly
    /// like [`VoterSetFull`](ConsensusError::VoterSetFull): the learner stays
    /// a caught-up learner and keeps polling, and the evidence may mature
    /// (or an operator may drive `ReplaceVoter`) at any tick.
    ///
    /// A *live* predecessor never qualifies, which is why a
    /// launch-before-terminate rollout has to name its pair explicitly.
    #[error(
        "no voter qualifies as evidence-dead for the {voters}/{cluster_size} voter set, so \
         promoting node {node} would overshoot; it remains a learner"
    )]
    NoRemovablePeer {
        node: CoordinatorId,
        voters: usize,
        cluster_size: usize,
    },

    /// The membership change would leave the continuing voter set with no
    /// confirmed CA-key holder (ADR 0037 §4). Terminal for the verb: it is a
    /// repair condition (a lost confirmation, a corrupt key file), not
    /// something a retry resolves.
    #[error(
        "refusing the change: no continuing voter holds a confirmed CA key (ADR 0037 §4) — \
         the change would leave the cluster unable to sign"
    )]
    NoKeyHolder,

    /// The membership change would leave a voter set the leader cannot see a
    /// live majority of (ADR 0037 §7's second postcondition). Retryable:
    /// contact may recover, and the caller (an operator driving
    /// `ReplaceVoter`, or a polling learner) re-offers.
    #[error(
        "refusing the change: only {live} of the {continuing} continuing voters have answered \
         this leader recently, which is not a live majority (ADR 0037 §7)"
    )]
    QuorumAtRisk { live: usize, continuing: usize },

    /// `ReplaceVoter` named an `old_node_id` that is not a voter (ADR 0037
    /// §7). Terminal: the verb replaces a voter, and naming a learner (or a
    /// stranger) is a caller error no waiting fixes. The idempotent
    /// already-replaced case is a success, not this.
    #[error("node {node} is not a voter, so it cannot be replaced (ADR 0037 §7)")]
    OldNotVoter { node: CoordinatorId },

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
    /// [`LearnerNotCaughtUp`](ConsensusError::LearnerNotCaughtUp),
    /// [`VoterSetFull`](ConsensusError::VoterSetFull)), or both.
    /// [`Shutdown`](ConsensusError::Shutdown) and [`Fatal`](ConsensusError::Fatal)
    /// are terminal for this handle, and
    /// [`UnknownNode`](ConsensusError::UnknownNode) /
    /// [`AddressConflict`](ConsensusError::AddressConflict) /
    /// [`NoKeyHolder`](ConsensusError::NoKeyHolder) /
    /// [`OldNotVoter`](ConsensusError::OldNotVoter) are terminal-not-fatal
    /// caller or repair conditions no amount of waiting fixes.
    /// [`NoRemovablePeer`](ConsensusError::NoRemovablePeer) joins the
    /// retryable set for the same reason `VoterSetFull` does (ADR 0037 §7).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ConsensusError::NotLeader { .. }
                | ConsensusError::Timeout
                | ConsensusError::MembershipInProgress
                | ConsensusError::LearnerNotCaughtUp { .. }
                | ConsensusError::VoterSetFull { .. }
                | ConsensusError::NoRemovablePeer { .. }
                | ConsensusError::QuorumAtRisk { .. }
        )
    }
}

/// Alias kept so proposal-path signatures read naturally.
pub type ProposeError = ConsensusError;
