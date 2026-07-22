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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{oneshot, watch, Semaphore};

use openraft::error::{
    ChangeMembershipError, CheckIsLeaderError, ClientWriteError, Fatal, RaftError,
};
use openraft::{ChangeMembers, Raft, RaftMetrics};

use coppice_state::{Command, StateMachine};

use crate::error::ConsensusError;
use crate::membership::{
    decide_add_learner, decide_promotion_voters, AddLearnerDecision, CoordinatorNode,
    LivenessAttestor, MembershipPolicy, NodeRecord, PromotionInputs,
};
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
    /// node id is [`CoordinatorId`] and nodes carry their dial address plus the
    /// machine-identity binding and superseded marker in a
    /// [`CoordinatorNode`](crate::membership::CoordinatorNode) (ADR 0037 §6,
    /// replacing openraft's `BasicNode`). Snapshots move as the file-backed
    /// [`SnapshotFile`](crate::storage::SnapshotFile) — never an in-memory
    /// buffer — so an ADR 0018 container streams disk-to-disk through
    /// install-snapshot (openraft's `generic-snapshot-data` feature). The
    /// remaining associated types take openraft's defaults (tokio runtime,
    /// oneshot responder). Neither `D` nor `R` implements serde, so openraft
    /// is built without its `serde` feature (ADR 0002).
    pub TypeConfig:
        D = Command,
        R = ApplyResult,
        Node = CoordinatorNode,
        SnapshotData = crate::storage::SnapshotFile,
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
    /// Advance the applied-index cursor to `applied_index` without touching
    /// state or the event stream. Blank (Raft no-op) and membership entries
    /// are applied entirely in the state-machine adapter — they never reach
    /// this task — but the published view's cursor must still move past them,
    /// or a strong read / event resync whose barrier lands on such an index
    /// (`read_index` returns the full Raft index) would wait forever. Reply
    /// acknowledges the advance so the adapter's `apply` keeps openraft's
    /// backpressure and ordering.
    Advance {
        applied_index: u64,
        reply: oneshot::Sender<()>,
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

/// Per-follower replication progress the leader tracks to reason about voter
/// liveness (ADR 0037 §5). Updated lazily from the openraft metrics watch on
/// each membership verb; `since` is when `matched` last advanced.
#[derive(Debug, Clone, Copy)]
struct FollowerProgress {
    matched: u64,
    since: Instant,
}

/// The openraft-backed [`Consensus`] implementation.
pub struct OpenraftConsensus {
    raft: Raft<TypeConfig>,
    status: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    proposal_permits: Arc<Semaphore>,
    /// Node-local membership policy (cluster size, grace periods; ADR 0037 §5).
    policy: MembershipPolicy,
    /// Optional discovery-liveness attestation hook (ADR 0037 §5). `None`
    /// contributes nothing to removal decisions.
    attestor: Option<Arc<dyn LivenessAttestor>>,
    /// Per-follower last-progress timestamps, updated lazily from the metrics
    /// watch each time a membership verb runs (ADR 0037 §5).
    progress: Arc<Mutex<HashMap<CoordinatorId, FollowerProgress>>>,
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
    /// `policy` is the node-local [`MembershipPolicy`] (ADR 0037 §5); `attestor`
    /// is the optional discovery-liveness hook.
    ///
    /// [`RaftLogStorage`]: openraft::storage::RaftLogStorage
    /// [`RaftStateMachine`]: openraft::storage::RaftStateMachine
    pub fn new(
        raft: Raft<TypeConfig>,
        status: watch::Receiver<ConsensusStatus>,
        views: StateViews,
        policy: MembershipPolicy,
        attestor: Option<Arc<dyn LivenessAttestor>>,
    ) -> Self {
        OpenraftConsensus {
            raft,
            status,
            views,
            proposal_permits: Arc::new(Semaphore::new(MAX_INFLIGHT_PROPOSALS)),
            policy,
            attestor,
            progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Flatten the committed membership config into the decision core's
    /// [`NodeRecord`] view.
    fn membership_records(
        metrics: &RaftMetrics<CoordinatorId, CoordinatorNode>,
    ) -> Vec<NodeRecord> {
        let voters: BTreeSet<CoordinatorId> = metrics.membership_config.voter_ids().collect();
        metrics
            .membership_config
            .nodes()
            .map(|(id, node)| NodeRecord {
                id: *id,
                addr: node.addr.clone(),
                machine_identity: node.machine_identity.clone(),
                superseded: node.superseded,
                voter: voters.contains(id),
            })
            .collect()
    }

    /// Refresh the per-follower progress map from a metrics sample and return a
    /// snapshot of each follower's last-progress instant (ADR 0037 §5).
    fn refresh_progress(
        &self,
        metrics: &RaftMetrics<CoordinatorId, CoordinatorNode>,
    ) -> HashMap<CoordinatorId, Instant> {
        let now = Instant::now();
        let mut map = self.progress.lock().expect("progress map poisoned");
        if let Some(repl) = metrics.replication.as_ref() {
            for (id, matched) in repl.iter() {
                let m = matched.map(|l| l.index).unwrap_or(0);
                let entry = map.entry(*id).or_insert(FollowerProgress {
                    matched: m,
                    since: now,
                });
                if m > entry.matched {
                    entry.matched = m;
                    entry.since = now;
                }
            }
        }
        map.iter().map(|(id, p)| (*id, p.since)).collect()
    }

    /// Admit `node` as a learner, binding `machine_identity` into its
    /// replicated record (ADR 0037 §6). Non-blocking: returns once replication
    /// to the learner is set up; it catches up via snapshot install plus log
    /// replay with no quorum impact.
    async fn admit_learner(
        &self,
        node: CoordinatorId,
        addr: String,
        machine_identity: String,
    ) -> Result<(), ConsensusError> {
        self.raft
            .add_learner(node, CoordinatorNode::new(addr, machine_identity), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    /// Mark `predecessor`'s node record superseded, as replicated state
    /// (ADR 0037 §5/§6). Keeps the address unchanged — only the flag flips, so
    /// this `SetNodes` cannot repoint a voter. Idempotent: a no-op if already
    /// superseded or absent.
    async fn mark_superseded(
        &self,
        predecessor: CoordinatorId,
        records: &[NodeRecord],
    ) -> Result<(), ConsensusError> {
        let Some(rec) = records.iter().find(|m| m.id == predecessor) else {
            return Ok(());
        };
        if rec.superseded {
            return Ok(());
        }
        let node = CoordinatorNode {
            addr: rec.addr.clone(),
            machine_identity: rec.machine_identity.clone(),
            superseded: true,
        };
        self.raft
            .change_membership(
                ChangeMembers::SetNodes(BTreeMap::from([(predecessor, node)])),
                true,
            )
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
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

    async fn add_learner(
        &self,
        node: CoordinatorId,
        addr: String,
        machine_identity: String,
    ) -> Result<(), ConsensusError> {
        // Decide against the *current* membership state before any other gate
        // (ADR 0037 §4): idempotent no-op / repoint refusal / seat rules (§6).
        let (records, staleness) = {
            let metrics = self.raft.metrics();
            let staleness = self.refresh_progress(&metrics.borrow());
            let records = Self::membership_records(&metrics.borrow());
            (records, staleness)
        };
        let grace = self.policy.replacement_grace;
        let now = Instant::now();
        let is_stale = |id: CoordinatorId| {
            // A pending learner is stale when the leader has seen no progress
            // from it (or never has) for `replacement_grace` (§6). An id with
            // no tracked progress at all is treated as reachable-until-proven,
            // so a just-admitted learner is never immediately evicted.
            staleness
                .get(&id)
                .map(|since| now.duration_since(*since) >= grace)
                .unwrap_or(false)
        };

        match decide_add_learner(&records, node, &addr, &machine_identity, is_stale) {
            AddLearnerDecision::Noop => Ok(()),
            AddLearnerDecision::RefuseSameIdDifferentAddress { existing_addr } => {
                Err(ConsensusError::SameIdDifferentAddress { existing_addr })
            }
            AddLearnerDecision::RefuseMachineSeatPending { incumbent } => {
                Err(ConsensusError::MachineSeatPending { incumbent })
            }
            AddLearnerDecision::AdmitFresh => {
                self.admit_learner(node, addr, machine_identity).await
            }
            AddLearnerDecision::AdmitReplacingVoter { predecessor } => {
                // Mark the predecessor superseded as replicated state before
                // admitting the replacement (§6): a separate committed
                // membership change (openraft has no single change that both
                // adds a learner and rewrites an existing node record). Both
                // steps are idempotent, so a crash between them is repaired by
                // re-running the convergence loop.
                self.mark_superseded(predecessor, &records).await?;
                self.admit_learner(node, addr, machine_identity).await
            }
            AddLearnerDecision::AdmitEvictingStaleLearner { stale } => {
                self.raft
                    .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([stale])), false)
                    .await
                    .map(|_| ())
                    .map_err(map_client_write_error)?;
                self.admit_learner(node, addr, machine_identity).await
            }
        }
    }

    async fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        // State short-circuit BEFORE the lag gate (ADR 0037 §4): an id that is
        // already a voter is a no-op success (it has no learner replication
        // entry to measure and must not be bounced with LearnerNotCaughtUp);
        // an id absent from membership is refused.
        {
            let records = Self::membership_records(&self.raft.metrics().borrow());
            match records.iter().find(|m| m.id == promote) {
                Some(m) if m.voter => return Ok(()),
                Some(_) => {}
                None => return Err(ConsensusError::UnknownNode { id: promote }),
            }
        }

        // ADR 0016 catch-up gate for an actual learner: refuse to raise it into
        // the quorum until its replication lag is within the threshold. Needs
        // leader replication metrics; if this node is not leader (no metrics)
        // or the learner is not tracked, refuse as not-caught-up, and a racing
        // step-down still surfaces `NotLeader` from `change_membership` below.
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

        // Compute the final voter set. Two paths (ADR 0037 §5):
        //   * `remove = Some(departed)` is the manual verb — remove exactly
        //     that id, unconditionally, plus any superseded predecessor.
        //   * `remove = None` is routine self-join — the leader folds in the
        //     replacement/overflow removals automatically.
        let final_voters = {
            let metrics = self.raft.metrics();
            let staleness_metrics = metrics.borrow();
            let staleness = self.refresh_progress(&staleness_metrics);
            let records = Self::membership_records(&staleness_metrics);
            let current_voters: BTreeSet<CoordinatorId> =
                staleness_metrics.membership_config.voter_ids().collect();
            let leader = staleness_metrics.id;
            let leader_last = staleness_metrics.last_log_index.unwrap_or(0);
            let now = Instant::now();

            // The promoting node's machine identity, and its superseded
            // predecessor still in membership (a bound voter marked superseded).
            let promoting_identity = records
                .iter()
                .find(|m| m.id == promote)
                .map(|m| m.machine_identity.clone())
                .unwrap_or_default();
            let superseded_predecessor = if promoting_identity.is_empty() {
                None
            } else {
                records
                    .iter()
                    .find(|m| {
                        m.voter
                            && m.superseded
                            && m.id != promote
                            && m.machine_identity == promoting_identity
                    })
                    .map(|m| m.id)
            };

            match remove {
                Some(departed) => {
                    let mut voters = current_voters.clone();
                    voters.insert(promote);
                    voters.remove(&departed);
                    if let Some(pred) = superseded_predecessor {
                        voters.remove(&pred);
                    }
                    voters
                }
                None => {
                    // Voters whose replication has been failing longer than
                    // `removal_grace` (§5), the leader's own observation; when a
                    // liveness attestor applies, also require it attests absence.
                    let dead_voters: BTreeSet<CoordinatorId> = current_voters
                        .iter()
                        .copied()
                        .filter(|v| *v != leader && *v != promote)
                        .filter(|v| {
                            staleness
                                .get(v)
                                .map(|since| {
                                    now.duration_since(*since) >= self.policy.removal_grace
                                })
                                .unwrap_or(false)
                        })
                        .filter(|v| {
                            self.attestor
                                .as_ref()
                                .map(|a| a.is_absent(*v))
                                .unwrap_or(true)
                        })
                        .collect();
                    // Voters the leader currently reaches within the lag
                    // threshold, plus the leader and the caught-up promoting
                    // node — the live-majority postcondition vantage.
                    let mut live_voters: BTreeSet<CoordinatorId> = current_voters
                        .iter()
                        .copied()
                        .filter(|v| {
                            *v == leader
                                || staleness_metrics
                                    .replication
                                    .as_ref()
                                    .and_then(|repl| repl.get(v).copied())
                                    .map(|entry| {
                                        leader_last
                                            .saturating_sub(entry.map(|l| l.index).unwrap_or(0))
                                            <= PROMOTION_LAG_MAX
                                    })
                                    .unwrap_or(false)
                        })
                        .collect();
                    live_voters.insert(promote);

                    decide_promotion_voters(PromotionInputs {
                        cluster_size: self.policy.cluster_size,
                        current_voters: &current_voters,
                        promoting: promote,
                        superseded_predecessor,
                        dead_voters: &dead_voters,
                        live_voters: &live_voters,
                    })
                    .map_err(ConsensusError::PromotionRefused)?
                }
            }
        };

        // `retain = false`: a voter dropped by the change is removed outright,
        // not demoted to learner — the departed node id is never reused
        // (ADR 0016).
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(final_voters), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn remove_node(&self, node: CoordinatorId) -> Result<(), ConsensusError> {
        // State short-circuit (ADR 0037 §4): an id already absent from
        // membership is a no-op success.
        {
            let records = Self::membership_records(&self.raft.metrics().borrow());
            if !records.iter().any(|m| m.id == node) {
                return Ok(());
            }
        }
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
    error: RaftError<CoordinatorId, ClientWriteError<CoordinatorId, CoordinatorNode>>,
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
    error: RaftError<CoordinatorId, CheckIsLeaderError<CoordinatorId, CoordinatorNode>>,
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
