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
use std::time::{Duration, Instant};

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
/// each membership verb.
///
/// `since` is the last instant the follower showed **affirmative liveness
/// evidence**: either its `matched` index advanced (it is acking new entries)
/// or it is fully caught up to the leader's last log index (matched ≥
/// leader_last, so the leader's heartbeats are being answered and there is
/// simply nothing left to replicate). A follower only *ages* toward "dead"
/// while it is BEHIND and not advancing — absence of workload in an idle
/// cluster never ages a live voter (ADR 0037 §5 requires affirmative
/// replication-failure evidence, not idleness).
#[derive(Debug, Clone, Copy)]
struct FollowerProgress {
    matched: u64,
    since: Instant,
}

/// Age one follower's progress record from a fresh metrics sample (ADR 0037
/// §5). Pure so the liveness rule is unit-testable without a live raft.
///
/// `since` resets to `now` on affirmative liveness evidence — the follower
/// advanced its matched index, or is caught up to `leader_last` (a follower
/// cannot exceed the leader, so `sampled ≥ leader_last` means exactly "caught
/// up"). Otherwise (behind and frozen) `since` is carried forward, so the
/// follower keeps aging toward the removal grace.
fn age_follower(
    prev: FollowerProgress,
    sampled: u64,
    leader_last: u64,
    now: Instant,
) -> FollowerProgress {
    let advanced = sampled > prev.matched;
    let caught_up = sampled >= leader_last;
    FollowerProgress {
        matched: prev.matched.max(sampled),
        since: if advanced || caught_up {
            now
        } else {
            prev.since
        },
    }
}

/// Fold the automatic promotion path's two dead-voter evidence sources into a
/// single candidate set (ADR 0037 §5), revalidated against the CURRENT voter
/// set. Pure so finding 3's union + revalidation is unit-testable without a
/// live raft.
///
/// The candidates are `progress_aged` ∪ `probe_dead`, each intersected with
/// `current_voters` — a candidate the membership re-read no longer shows as a
/// voter (removed by a racing change) is dropped, which is precisely why probe
/// evidence feeds this checked path rather than a separate unchecked manual
/// removal. The `leader` and the node being `promoted` are always excluded: the
/// leader is live by definition (finding 4's second exclusion layer), and the
/// promoting node just caught up. The attestor gate, which needs live records,
/// is applied by the caller on top of this set.
fn dead_voter_candidates(
    current_voters: &BTreeSet<CoordinatorId>,
    progress_aged: &BTreeSet<CoordinatorId>,
    probe_dead: &BTreeSet<CoordinatorId>,
    leader: CoordinatorId,
    promoting: CoordinatorId,
) -> BTreeSet<CoordinatorId> {
    progress_aged
        .iter()
        .copied()
        .chain(probe_dead.iter().copied())
        .filter(|v| current_voters.contains(v))
        .filter(|v| *v != leader && *v != promoting)
        .collect()
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
    /// Serializes the whole decide→commit→settle sequence for every membership
    /// mutation (ADR 0037 §4/§6). Held across snapshot-read → decide →
    /// `change_membership`/`add_learner` → metrics-settle so two concurrent
    /// requests can never both observe "no pending seat", both choose
    /// `AdmitFresh`, and both commit — which would let one credential obtain
    /// two votes (openraft does not re-run the seat predicate). The re-read of
    /// membership happens *after* this lock is taken; the lock is the fix and
    /// the re-read is what makes it correct.
    membership_lock: Arc<tokio::sync::Mutex<()>>,
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
            membership_lock: Arc::new(tokio::sync::Mutex::new(())),
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
    /// snapshot of each follower's last-liveness-evidence instant (ADR 0037 §5).
    ///
    /// A follower's clock resets whenever it advances or is caught up to the
    /// leader's last log index (see [`age_follower`]); it ages only while
    /// behind and frozen. So the returned `since` measures "how long since the
    /// leader last had affirmative evidence this follower is alive", which is
    /// what `removal_grace`/`replacement_grace` are compared against — not the
    /// absence of workload.
    fn refresh_progress(
        &self,
        metrics: &RaftMetrics<CoordinatorId, CoordinatorNode>,
    ) -> HashMap<CoordinatorId, Instant> {
        let now = Instant::now();
        let leader_last = metrics.last_log_index.unwrap_or(0);
        let mut map = self.progress.lock().expect("progress map poisoned");
        if let Some(repl) = metrics.replication.as_ref() {
            for (id, matched) in repl.iter() {
                let m = matched.map(|l| l.index).unwrap_or(0);
                let updated = match map.get(id) {
                    Some(prev) => age_follower(*prev, m, leader_last, now),
                    // First sight of this follower: seed its clock at `now` so a
                    // just-admitted learner is never immediately treated as
                    // stale, then let it age from here if it stays behind.
                    None => FollowerProgress {
                        matched: m,
                        since: now,
                    },
                };
                map.insert(*id, updated);
            }
        }
        map.iter().map(|(id, p)| (*id, p.since)).collect()
    }

    /// Block until this leader's metrics watch reflects the committed membership
    /// change at log index `index` (ADR 0037 §4/§6). `raft.metrics()` is a watch
    /// the core loop updates *slightly after* the client-write response
    /// resolves, so the next holder of [`Self::membership_lock`] could otherwise
    /// re-read a snapshot that still omits a just-committed learner and admit a
    /// second seat for one machine identity. Bounded so a stalled apply can
    /// never wedge the lock — the change is durably committed regardless of the
    /// wait's outcome.
    async fn settle_metrics(&self, index: u64) {
        let _ = self
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(index), "settle membership metrics after commit")
            .await;
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
        let resp = self
            .raft
            .add_learner(node, CoordinatorNode::new(addr, machine_identity), false)
            .await
            .map_err(map_client_write_error)?;
        // Settle before returning so the caller — still holding the membership
        // lock — leaves a metrics snapshot that already includes this learner.
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
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
        let resp = self
            .raft
            .change_membership(
                ChangeMembers::SetNodes(BTreeMap::from([(predecessor, node)])),
                true,
            )
            .await
            .map_err(map_client_write_error)?;
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
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
        // Serialize the whole decide→commit→settle sequence (finding: seat
        // TOCTOU). Two concurrent same-machine-identity admissions must not both
        // observe "no pending seat" and both admit; the lock forces the second
        // to re-read a snapshot that already includes the first's committed
        // learner and refuse it `MachineSeatPending` (ADR 0037 §4/§6).
        let _guard = self.membership_lock.lock().await;

        // Decide against the *current* membership state before any other gate
        // (ADR 0037 §4): idempotent no-op / repoint refusal / seat rules (§6).
        // Read AFTER acquiring the lock so the snapshot reflects every
        // membership mutation committed by a prior lock holder.
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
                let resp = self
                    .raft
                    .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([stale])), false)
                    .await
                    .map_err(map_client_write_error)?;
                self.settle_metrics(resp.log_id.index).await;
                self.admit_learner(node, addr, machine_identity).await
            }
        }
    }

    async fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
        probe_dead: BTreeSet<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (finding: seat TOCTOU):
        // the promotion's replacement/overflow removal is decided from a
        // membership + progress snapshot and then committed, so it must not
        // interleave with a concurrent add_learner/promote/remove (ADR 0037
        // §4/§5). All reads below happen under this lock.
        let _guard = self.membership_lock.lock().await;

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
                    // Dead-voter *candidates* are the union of two affirmative
                    // unreachability signals (§5), both revalidated against
                    // membership re-read under the lock (see
                    // [`dead_voter_candidates`]):
                    //   * the leader's own progress-aging — voters whose
                    //     replication has been failing longer than
                    //     `removal_grace`; and
                    //   * `probe_dead` — voters the admin surface has just found
                    //     continuously unreachable by direct probe.
                    // Both are intersected with the CURRENT voter set (a
                    // candidate no longer a voter is simply dropped — that is the
                    // atomic revalidation that replaces the old unchecked manual
                    // removal) and the leader/promote are excluded.
                    let progress_aged: BTreeSet<CoordinatorId> = current_voters
                        .iter()
                        .copied()
                        .filter(|v| {
                            staleness
                                .get(v)
                                .map(|since| {
                                    now.duration_since(*since) >= self.policy.removal_grace
                                })
                                .unwrap_or(false)
                        })
                        .collect();
                    // Where a liveness attestor applies it must ALSO attest
                    // absence; the pure candidate set stands alone otherwise.
                    let dead_voters: BTreeSet<CoordinatorId> = dead_voter_candidates(
                        &current_voters,
                        &progress_aged,
                        &probe_dead,
                        leader,
                        promote,
                    )
                    .into_iter()
                    .filter(|v| {
                        let Some(attestor) = self.attestor.as_ref() else {
                            return true; // no liveness semantics: evidence stands alone
                        };
                        records
                            .iter()
                            .find(|m| m.id == *v)
                            .is_some_and(|m| attestor.is_absent(*v, &m.addr))
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
        let resp = self
            .raft
            .change_membership(ChangeMembers::ReplaceAllVoters(final_voters), false)
            .await
            .map_err(map_client_write_error)?;
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
    }

    async fn remove_node(&self, node: CoordinatorId) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (finding: seat TOCTOU;
        // ADR 0037 §4). The re-read below happens under this lock.
        let _guard = self.membership_lock.lock().await;
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
        let resp = self
            .raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node])), false)
            .await
            .map_err(map_client_write_error)?;
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
    }

    async fn evict_stale_learner(
        &self,
        incumbent: CoordinatorId,
        machine_identity: &str,
    ) -> Result<(), ConsensusError> {
        // Serialized with every other membership mutation; revalidate under the
        // lock so the eviction evidence gathered outside cannot act on a stale
        // view (ADR 0037 §6): the incumbent must still be a pending LEARNER
        // bound to the contested machine identity. The voter set is never
        // touched here — a bound voter is retired only through promotion.
        let _guard = self.membership_lock.lock().await;
        let record = {
            let records = Self::membership_records(&self.raft.metrics().borrow());
            records.iter().find(|m| m.id == incumbent).cloned()
        };
        let Some(record) = record else {
            return Ok(()); // already gone: the slot is free
        };
        if record.voter {
            return Err(ConsensusError::NotALearner { node: incumbent });
        }
        if record.machine_identity != machine_identity {
            return Err(ConsensusError::MachineMismatch {
                node: incumbent,
                bound: record.machine_identity,
            });
        }
        let resp = self
            .raft
            .change_membership(
                ChangeMembers::RemoveNodes(BTreeSet::from([incumbent])),
                false,
            )
            .await
            .map_err(map_client_write_error)?;
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
    }

    async fn set_node_address(
        &self,
        node: CoordinatorId,
        new_addr: String,
    ) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (ADR 0037 §4/§6): the
        // decide→commit runs under the same lock as add-learner/promote so a
        // repoint can never interleave with a concurrent seat change and read a
        // membership snapshot the other request is mid-committing. The re-read
        // below happens after the lock is taken.
        let _guard = self.membership_lock.lock().await;
        // Decide against the *current* membership state (ADR 0037 §4): an id
        // absent from membership is refused with no silent creation; an id
        // already at `new_addr` is a no-op success. The endpoint verification
        // (§6) is the caller's, done before this seam is reached.
        let Some(rec) = Self::membership_records(&self.raft.metrics().borrow())
            .into_iter()
            .find(|m| m.id == node)
        else {
            return Err(ConsensusError::UnknownNode { id: node });
        };
        if rec.addr == new_addr {
            return Ok(());
        }
        // Repoint ONLY the address: machine_identity and the superseded marking
        // are carried through unchanged, and the voter set is not touched. This
        // narrow `SetNodes` is the whole of the break-glass repoint; openraft
        // warns a careless `SetNodes` address can split-brain, which is why the
        // caller verifies the endpoint owns both the subject and the node id
        // first (ADR 0037 §4/§6).
        let updated = CoordinatorNode {
            addr: new_addr,
            machine_identity: rec.machine_identity,
            superseded: rec.superseded,
        };
        let resp = self
            .raft
            .change_membership(
                ChangeMembers::SetNodes(BTreeMap::from([(node, updated)])),
                true,
            )
            .await
            .map_err(map_client_write_error)?;
        self.settle_metrics(resp.log_id.index).await;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::io;

    use openraft::error::{
        Fatal as OpenraftFatal, RPCError, ReplicationClosed, StreamingError, Unreachable,
    };
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::{Config, ServerState, Snapshot, Vote};

    use crate::fs::RealFs;
    use crate::storage;
    use crate::view::{ViewPublisher, ViewPublisherConfig};
    use crate::Role;

    // ---- Finding #3/#4: probe evidence feeds the checked automatic path ----

    use crate::membership::PromotionRefusal;

    fn ids(v: &[u64]) -> BTreeSet<CoordinatorId> {
        v.iter().copied().collect()
    }

    #[test]
    fn dead_candidates_union_intersects_current_voters() {
        // Progress-aging names {2}, the probe names {3,4}. Voter 4 is NOT a
        // current voter (a racing removal beat us here), so it drops out — the
        // atomic revalidation. The union is {2,3}.
        let out = dead_voter_candidates(
            &ids(&[1, 2, 3]), // current voters (leader 1)
            &ids(&[2]),       // progress-aged
            &ids(&[3, 4]),    // probe_dead (4 is stale/removed)
            1,                // leader
            5,                // promoting (a learner, not yet a voter)
        );
        assert_eq!(out, ids(&[2, 3]), "union ∩ current, stale probe id dropped");
    }

    #[test]
    fn dead_candidates_never_selects_leader_or_promoting() {
        // Finding 4: even if the probe names the leader (1) — e.g. it could not
        // hairpin-dial itself — the leader is never a candidate. The promoting
        // node just caught up, so it is excluded too.
        let out = dead_voter_candidates(
            &ids(&[1, 2, 3]),
            &ids(&[1]),    // progress-aging somehow named the leader
            &ids(&[1, 3]), // and so did the probe
            1,             // leader
            3,             // promoting is already a voter here
        );
        assert_eq!(
            out,
            ids(&[]),
            "leader and promoting node are never removal candidates"
        );
    }

    #[test]
    fn probe_evidence_flow_still_enforces_live_majority() {
        // Finding 3, check (i): probe evidence names a dead voter, and its
        // removal satisfies cardinality — but the resulting set would lack a
        // live majority, so the UNCHANGED automatic selection refuses rather
        // than commit a config that cannot make progress. cluster_size 3,
        // current {1,2,3}, promote 4 → the probe names 3 dead; removing it
        // leaves {1,2,4}, but only the promoting node (4) is live from the
        // leader's vantage (1 and 2 are also unreachable), so the live-majority
        // postcondition fails.
        let current = ids(&[1, 2, 3]);
        let dead = dead_voter_candidates(&current, &ids(&[]), &ids(&[3]), 1, 4);
        assert_eq!(dead, ids(&[3]), "the probed dead voter is a candidate");
        let err = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &current,
            promoting: 4,
            superseded_predecessor: None,
            dead_voters: &dead,
            // Only the caught-up promoting node is live — removing 3 still
            // leaves {1,2,4} without a live majority.
            live_voters: &ids(&[4]),
        })
        .unwrap_err();
        assert_eq!(
            err,
            PromotionRefusal::NoRemovablePeer,
            "evidence never bypasses the live-majority postcondition"
        );
    }

    #[test]
    fn probe_evidence_race_yields_no_unchecked_growth() {
        // Finding 3, check (ii): the probe named voter 3 dead, but by the time
        // the promotion commits, 3 is already gone (removed by a racing change),
        // so the revalidation drops it and no dead voter remains. The automatic
        // selection then refuses the overflow rather than growing the voter set.
        let current_after_race = ids(&[1, 2]); // 3 already removed
        let dead = dead_voter_candidates(&current_after_race, &ids(&[]), &ids(&[3]), 1, 4);
        assert_eq!(dead, ids(&[]), "the removed voter is not a candidate");
        // Promoting 4 into {1,2} is underfilled at cluster_size 3 — allowed with
        // no removal and, crucially, the set never exceeds cluster_size.
        let out = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &current_after_race,
            promoting: 4,
            superseded_predecessor: None,
            dead_voters: &dead,
            live_voters: &ids(&[1, 2, 4]),
        })
        .expect("underfilled promotion allowed");
        assert!(out.len() <= 3, "voter set never grows beyond cluster_size");
        assert_eq!(out, ids(&[1, 2, 4]));
    }

    // ---- Finding #2: dead-voter evidence is affirmative, not idleness ----

    #[test]
    fn age_follower_resets_when_caught_up() {
        // An idle follower that is fully caught up (matched == leader_last) but
        // whose matched has not advanced still shows liveness — its clock resets
        // so it never ages toward removal in an idle cluster.
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(30);
        let out = age_follower(
            FollowerProgress {
                matched: 5,
                since: t0,
            },
            5,
            5,
            later,
        );
        assert_eq!(out.since, later, "a caught-up follower resets its clock");
    }

    #[test]
    fn age_follower_ages_only_while_behind_and_frozen() {
        // Behind (leader_last 9 > matched 5) and not advancing → the clock is
        // carried forward, so the follower keeps aging toward removal_grace.
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(30);
        let out = age_follower(
            FollowerProgress {
                matched: 5,
                since: t0,
            },
            5,
            9,
            later,
        );
        assert_eq!(out.since, t0, "a behind, frozen follower keeps aging");
    }

    #[test]
    fn age_follower_resets_when_advancing_even_if_still_behind() {
        // Behind but advancing (matched 5 → 7): the leader is getting acks, so
        // it is alive — reset regardless of the remaining lag.
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(30);
        let out = age_follower(
            FollowerProgress {
                matched: 5,
                since: t0,
            },
            7,
            9,
            later,
        );
        assert_eq!(out.since, later, "an advancing follower resets");
        assert_eq!(out.matched, 7);
    }

    // ---- Finding #1: seat-decision TOCTOU is closed by the membership lock ----

    const TEST_CLUSTER: [u8; 16] = *b"adapter-seat-tst";

    /// A raft network that reaches no peer. A single-voter cluster commits every
    /// membership change on its own quorum, so a learner never needs to be
    /// reachable for `add_learner` to commit — which is exactly the seat race
    /// this exercises.
    struct DeadFactory;

    impl RaftNetworkFactory<TypeConfig> for DeadFactory {
        type Network = DeadNet;
        async fn new_client(&mut self, _target: CoordinatorId, _node: &CoordinatorNode) -> DeadNet {
            DeadNet
        }
    }

    struct DeadNet;

    impl RaftNetwork<TypeConfig> for DeadNet {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<CoordinatorId>,
            RPCError<CoordinatorId, CoordinatorNode, RaftError<CoordinatorId>>,
        > {
            Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "dead test network",
            ))))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<CoordinatorId>,
            _option: RPCOption,
        ) -> Result<
            VoteResponse<CoordinatorId>,
            RPCError<CoordinatorId, CoordinatorNode, RaftError<CoordinatorId>>,
        > {
            Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "dead test network",
            ))))
        }

        async fn full_snapshot(
            &mut self,
            _vote: Vote<CoordinatorId>,
            _snapshot: Snapshot<TypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
            _option: RPCOption,
        ) -> Result<
            SnapshotResponse<CoordinatorId>,
            StreamingError<TypeConfig, OpenraftFatal<CoordinatorId>>,
        > {
            Err(StreamingError::Unreachable(Unreachable::new(
                &io::Error::other("dead test network"),
            )))
        }
    }

    /// Build a single-voter leader whose founding seat is bound to
    /// `founder_identity`, ready to accept `add_learner` calls.
    async fn single_voter_leader(founder_identity: &str) -> (OpenraftConsensus, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let options = storage::StorageOptions::new(TEST_CLUSTER);
        let node_id = storage::init(&RealFs::new(dir.path()), &options).expect("init data dir");
        let recovered = storage::open(RealFs::new(dir.path()), options).expect("open recovery");
        let (log, sm) = recovered.into_stores_with_local_apply_task();

        let config = Config {
            cluster_name: "adapter-seat-tst".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config");

        let raft = Raft::new(node_id, Arc::new(config), DeadFactory, log, sm)
            .await
            .expect("raft node");
        raft.initialize(BTreeMap::from([(
            node_id,
            CoordinatorNode::new("127.0.0.1:65000", founder_identity),
        )]))
        .await
        .expect("initialize single voter");
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "become leader")
            .await
            .expect("single voter becomes leader");

        // The status sender and view publisher are unused by the membership
        // path under test; a dropped watch sender still lets `status()` borrow
        // the last value, and `views()` is never called here.
        let (_status_tx, status_rx) = watch::channel(ConsensusStatus {
            id: node_id,
            role: Role::Leader { term: 1 },
            last_applied: 0,
            known_committed: 0,
        });
        let (_publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());

        let consensus =
            OpenraftConsensus::new(raft, status_rx, views, MembershipPolicy::default(), None);
        (consensus, dir)
    }

    /// Two `add_learner` calls with the *same* machine identity but different
    /// node ids, raced. The membership lock forces them to serialize and the
    /// second to re-read the first's committed learner, so exactly one is
    /// admitted and the other is refused `MachineSeatPending`. Without the lock
    /// both would observe an empty seat and both admit — one credential holding
    /// two pending learners (the seat-decision TOCTOU).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_same_identity_add_learner_admits_exactly_one() {
        let (consensus, _dir) = single_voter_leader("founder").await;
        let consensus = Arc::new(consensus);

        let a = Arc::clone(&consensus);
        let b = Arc::clone(&consensus);
        let ha = tokio::spawn(async move {
            a.add_learner(100, "10.0.0.1:7071".to_string(), "m-shared".to_string())
                .await
        });
        let hb = tokio::spawn(async move {
            b.add_learner(200, "10.0.0.2:7071".to_string(), "m-shared".to_string())
                .await
        });
        let results = [ha.await.expect("join a"), hb.await.expect("join b")];

        let admitted = results.iter().filter(|r| r.is_ok()).count();
        let pending = results
            .iter()
            .filter(|r| matches!(r, Err(ConsensusError::MachineSeatPending { .. })))
            .count();
        assert_eq!(admitted, 1, "exactly one learner admitted: {results:?}");
        assert_eq!(
            pending, 1,
            "exactly one refused MachineSeatPending: {results:?}"
        );

        // And membership carries exactly one seat for the shared identity.
        let seats = consensus
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .filter(|(_, n)| n.machine_identity == "m-shared")
            .count();
        assert_eq!(
            seats, 1,
            "one committed seat for the shared machine identity"
        );
    }
}
