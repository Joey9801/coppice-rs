//! The thin openraft adapter (ADR 0002).
//!
//! openraft's internals — election, log replication, membership joint
//! consensus, its own task and channel machinery — are a black box. This module
//! is the only place that names openraft types: it declares the
//! [`RaftTypeConfig`](openraft::RaftTypeConfig), holds the
//! [`openraft::Raft`] handle, and converts openraft's request/response/error
//! types into this crate's openraft-free surface ([`Applied`], [`ConsensusError`],
//! [`StateViews`]). No openraft type crosses this boundary into another crate's
//! signature.
//!
//! What is deliberately **not** here: the `RaftLogStorage` and
//! `RaftStateMachine` implementations. Those are the segment-storage task's job
//! (ADR 0002) — the `RaftStateMachine` adapter forwards committed entries to the
//! single-writer apply task over an [`ApplyRequest`] channel and awaits the
//! reply, so backpressure lands on openraft's replication rather than on a
//! lock. The apply task, the network factory, and the openraft node are wired
//! by the coordinator runtime (`docs/architecture/coordinator-runtime.md`),
//! which then assembles this adapter with [`OpenraftConsensus::new`].

use std::collections::BTreeSet;
// `declare_raft_types!` expands its default `SnapshotData = Cursor<Vec<u8>>`,
// which resolves against this import.
use std::io::Cursor;
use std::sync::Arc;

use tokio::sync::{oneshot, watch, Semaphore};

use openraft::error::{
    ChangeMembershipError, CheckIsLeaderError, ClientWriteError, Fatal, RaftError,
};
use openraft::{BasicNode, ChangeMembers, Raft};

use coppice_state::{Command, StateMachine};

use crate::error::ConsensusError;
use crate::view::StateViews;
use crate::{Applied, Consensus, ConsensusStatus, CoordinatorId};

/// The apply outcome carried back from the state machine: `Ok` for an accepted
/// command, `Err` for a committed-but-rejected one (command-catalog.md). This
/// is the Raft response type `R`.
pub type ApplyResult = Result<coppice_state::Applied, coppice_state::RejectionReason>;

openraft::declare_raft_types!(
    /// The openraft type binding for the coordinator.
    ///
    /// `D` is a control-plane [`Command`], `R` is its [`ApplyResult`]; the
    /// node id is [`CoordinatorId`] and nodes carry a dial address in a
    /// [`BasicNode`]. The remaining associated types take openraft's
    /// defaults (tokio runtime, oneshot responder, in-memory snapshot
    /// cursor). Neither `D` nor `R` implements serde, so openraft is built
    /// without its `serde` feature (ADR 0002).
    pub TypeConfig:
        D = Command,
        R = ApplyResult,
);

/// One unit of work for the apply task — the single writer of [`StateMachine`].
/// Sent over a bounded mpsc (capacity [`APPLY_CHANNEL_CAPACITY`]); the
/// `RaftStateMachine` adapter awaits the reply, so backpressure lands on
/// openraft's replication, never on a lock.
pub enum ApplyRequest {
    /// Apply committed entries in order; reply with one outcome per command.
    /// Each entry is `(log index, command)`, ascending by index.
    Apply {
        entries: Vec<(u64, Command)>,
        reply: oneshot::Sender<Vec<ApplyResult>>,
    },
    /// Hand out the current state for snapshot serialization: the apply task
    /// clones its `Arc<StateMachine>` and the applied index; serialization then
    /// happens off the apply task.
    Snapshot {
        reply: oneshot::Sender<(Arc<StateMachine>, u64)>,
    },
    /// Replace state wholesale from an installed snapshot, acknowledging once
    /// the swap is done. The state is boxed to keep this cold, large variant
    /// from inflating the size of every message on the channel.
    Install {
        state: Box<StateMachine>,
        applied_index: u64,
        reply: oneshot::Sender<()>,
    },
}

/// Capacity of the apply channel between the `RaftStateMachine` adapter and the
/// apply task. Small on purpose: it is a handoff, not a buffer, so a slow apply
/// throttles replication rather than growing an unbounded queue.
pub const APPLY_CHANNEL_CAPACITY: usize = 64;

/// The bounded in-flight proposal budget.
///
/// A proposer acquires one permit for the lifetime of a
/// [`Consensus::propose`] call, so no more than this many commands sit
/// un-applied in openraft at once; the excess waits on the semaphore
/// instead of piling into openraft's queues.
pub const MAX_INFLIGHT_PROPOSALS: usize = 4096;

/// The maximum log-index lag a learner may carry and still be promoted to
/// voter (ADR 0016 "caught up within a threshold").
///
/// Promotion adds the node to the quorum; a learner still far behind would
/// stall commit until it catches up, so [`Consensus::promote_voter`] refuses
/// the joint change while the learner's `leader_last_log − matched` exceeds
/// this, returning the retryable [`ConsensusError::LearnerNotCaughtUp`] so the
/// admin caller polls until it passes.
pub const PROMOTION_LAG_MAX: u64 = 256;

/// The openraft-backed [`Consensus`] implementation.
pub struct OpenraftConsensus {
    raft: Raft<TypeConfig>,
    status: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    proposal_permits: Arc<Semaphore>,
}

impl OpenraftConsensus {
    /// Assemble the seam from an already-constructed openraft handle plus the
    /// status and views the apply task publishes.
    ///
    /// Raft construction (which needs the segment [`RaftLogStorage`], the
    /// [`RaftStateMachine`] adapter, and the network factory — none of which
    /// live in this crate) stays with the coordinator runtime; keeping it out
    /// of here is what lets `coppice-consensus` avoid a dependency on the
    /// storage layer. The runtime builds those, spawns the apply task to obtain
    /// `status`/`views`, then calls this.
    ///
    /// [`RaftLogStorage`]: openraft::storage::RaftLogStorage
    /// [`RaftStateMachine`]: openraft::storage::RaftStateMachine
    pub fn new(
        raft: Raft<TypeConfig>,
        status: watch::Receiver<ConsensusStatus>,
        views: StateViews,
    ) -> Self {
        OpenraftConsensus {
            raft,
            status,
            views,
            proposal_permits: Arc::new(Semaphore::new(MAX_INFLIGHT_PROPOSALS)),
        }
    }
}

impl Consensus for OpenraftConsensus {
    async fn propose(&self, command: Command) -> Result<Applied, ConsensusError> {
        // Hold a permit for the whole round-trip: this is the bounded in-flight
        // budget. `acquire` errors only if the semaphore is closed, i.e. we are
        // shutting down.
        let _permit = self
            .proposal_permits
            .acquire()
            .await
            .map_err(|_| ConsensusError::Shutdown)?;

        match self.raft.client_write(command).await {
            Ok(response) => Ok(Applied {
                log_index: response.log_id.index,
                outcome: response.data,
            }),
            Err(error) => Err(map_client_write_error(error)),
        }
    }

    async fn read_index(&self) -> Result<u64, ConsensusError> {
        match self.raft.ensure_linearizable().await {
            // `None` means no log has been applied yet; index 0 is the correct
            // barrier for an empty state machine.
            Ok(read_log_id) => Ok(read_log_id.map(|id| id.index).unwrap_or(0)),
            Err(error) => Err(map_check_leader_error(error)),
        }
    }

    fn status(&self) -> watch::Receiver<ConsensusStatus> {
        self.status.clone()
    }

    fn views(&self) -> StateViews {
        self.views.clone()
    }

    async fn add_learner(&self, node: CoordinatorId, addr: String) -> Result<(), ConsensusError> {
        // Non-blocking: return once replication to the learner is set up. The
        // learner catches up via snapshot install plus log replay with no
        // quorum impact; the CLI polls health before promotion (ADR 0016).
        self.raft
            .add_learner(node, BasicNode { addr }, false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        // ADR 0016 catch-up gate: refuse to raise a learner into the quorum
        // until its replication lag is within the threshold. The check is
        // best-effort — it needs leader replication metrics; if this node is
        // not leader (no replication metrics) or the learner is not yet tracked
        // the promotion is refused as not-caught-up, and a racing step-down
        // still surfaces `NotLeader` from `change_membership` below.
        {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            let leader_last = metrics.last_log_index.unwrap_or(0);
            let matched = metrics
                .replication
                .as_ref()
                .and_then(|repl| repl.get(&promote).copied());
            let lag = match matched {
                Some(entry) => leader_last.saturating_sub(entry.map(|id| id.index).unwrap_or(0)),
                None => {
                    return Err(ConsensusError::LearnerNotCaughtUp { lag: leader_last });
                }
            };
            if lag > PROMOTION_LAG_MAX {
                return Err(ConsensusError::LearnerNotCaughtUp { lag });
            }
        }

        let changes = match remove {
            // Pure promotion: raise one learner to voter, leaving the rest of
            // the voter set untouched.
            None => ChangeMembers::AddVoterIds(BTreeSet::from([promote])),
            // Promotion plus removal in one joint change (ADR 0016 step 3):
            // compute the new voter set from current membership. `promote` must
            // already be a caught-up learner.
            Some(departed) => {
                let mut voters: BTreeSet<CoordinatorId> = self
                    .raft
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .collect();
                voters.insert(promote);
                voters.remove(&departed);
                ChangeMembers::ReplaceAllVoters(voters)
            }
        };
        // `retain = false`: a voter dropped by the change is removed outright,
        // not demoted to learner — the departed node id is never reused
        // (ADR 0016).
        self.raft
            .change_membership(changes, false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn remove_node(&self, node: CoordinatorId) -> Result<(), ConsensusError> {
        // Removes the node entirely. openraft requires it be a non-voter first;
        // a departed voter is dropped through `promote_voter`'s removal path.
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node])), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn trigger_snapshot(&self) -> Result<(), ConsensusError> {
        self.raft.trigger().snapshot().await.map_err(map_fatal)
    }
}

/// Map a client-write / membership error onto the seam's error surface. This is
/// requirement 5: a leadership loss (`ForwardToLeader`) becomes the retryable
/// [`ConsensusError::NotLeader`] so an in-flight proposal never hangs.
fn map_client_write_error(
    error: RaftError<CoordinatorId, ClientWriteError<CoordinatorId, BasicNode>>,
) -> ConsensusError {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ConsensusError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(change)) => {
            map_change_membership_error(change)
        }
        RaftError::Fatal(fatal) => map_fatal(fatal),
    }
}

/// Map a linearizable-read barrier error.
///
/// `QuorumNotEnough` means the leader could not confirm its lease within
/// the round — surfaced as a retryable [`ConsensusError::Timeout`].
fn map_check_leader_error(
    error: RaftError<CoordinatorId, CheckIsLeaderError<CoordinatorId, BasicNode>>,
) -> ConsensusError {
    match error {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)) => {
            ConsensusError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => ConsensusError::Timeout,
        RaftError::Fatal(fatal) => map_fatal(fatal),
    }
}

fn map_change_membership_error(error: ChangeMembershipError<CoordinatorId>) -> ConsensusError {
    match error {
        ChangeMembershipError::InProgress(_) => ConsensusError::MembershipInProgress,
        ChangeMembershipError::EmptyMembership(inner) => ConsensusError::Fatal(inner.to_string()),
        ChangeMembershipError::LearnerNotFound(inner) => ConsensusError::Fatal(inner.to_string()),
    }
}

fn map_fatal(fatal: Fatal<CoordinatorId>) -> ConsensusError {
    match fatal {
        Fatal::Stopped => ConsensusError::Shutdown,
        Fatal::Panicked => ConsensusError::Fatal("raft core panicked".to_string()),
        Fatal::StorageError(inner) => ConsensusError::Fatal(inner.to_string()),
    }
}
