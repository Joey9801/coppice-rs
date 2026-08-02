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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{oneshot, watch, Semaphore};

use openraft::error::{
    ChangeMembershipError, CheckIsLeaderError, ClientWriteError, Fatal, RaftError,
};
use openraft::{BasicNode, ChangeMembers, Raft};

use coppice_state::{Command, StateMachine};

use crate::contact::ContactTracker;
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
    /// [`BasicNode`]. Snapshots move as the file-backed
    /// [`SnapshotFile`](crate::storage::SnapshotFile) — never an in-memory
    /// buffer — so an ADR 0018 container streams disk-to-disk through
    /// install-snapshot (openraft's `generic-snapshot-data` feature). The
    /// remaining associated types take openraft's defaults (tokio runtime,
    /// oneshot responder). Neither `D` nor `R` implements serde, so openraft
    /// is built without its `serde` feature (ADR 0002).
    pub TypeConfig:
        D = Command,
        R = ApplyResult,
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
/// stall commit until it catches up, so [`Consensus::plan_promotion`] (and
/// every other path that raises a learner — `commit_promotion`,
/// `replace_voter`) refuses while the learner's `leader_last_log − matched`
/// exceeds this, returning the retryable
/// [`ConsensusError::LearnerNotCaughtUp`] so the admin caller polls until it
/// passes.
pub const PROMOTION_LAG_MAX: u64 = 256;

/// How recently a voter must have acknowledged this leader's replication to
/// count toward the "live majority from the leader's vantage" postcondition
/// (ADR 0037 §7).
///
/// Three seconds: twice the default election-timeout minimum (1500ms), which
/// is openraft's election-timeout *maximum* and the same bound the coordinator
/// derives for `/readyz`'s contact-staleness test — past it, a healthy
/// connected peer would have called an election of its own. Deliberately far
/// shorter than `removal_grace` (default 120s): the grace period answers "has
/// this peer been dead long enough to evict", while this answers "is this peer
/// answering me right now", and conflating the two would let a change proceed
/// on a set the leader has not heard from in two minutes.
pub const LIVE_CONTACT_STALENESS: Duration = Duration::from_secs(3);

/// What [`Consensus::plan_promotion`] decided, before any key transfer or
/// membership change happens (ADR 0037 §4/§7).
///
/// The split exists because the confirmed-key-receipt precondition sits
/// *between* the gates and the joint change: the admin service plans, then
/// transfers the key and records the possession fact, then commits. Planning
/// is a pure read of local metrics and evidence, so a plan that is stale by
/// the time it commits is re-checked by [`Consensus::commit_promotion`]
/// rather than trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionPlan {
    /// The node is already a voter: the ADR 0037 §6 idempotency no-op. No key
    /// transfer, no membership change — a voter necessarily passed both gates
    /// when it was promoted.
    AlreadyVoter,
    /// The promotion may proceed once the candidate confirms key receipt.
    /// `evidence_removal` is the at-most-one evidence-dead voter the leader
    /// folds into the same joint change to stay within `cluster_size`
    /// (ADR 0037 §7 "the hands-off path"); `None` means the seat was free.
    Ready {
        evidence_removal: Option<CoordinatorId>,
    },
}

/// What [`Consensus::plan_replacement`] decided, before any key transfer or
/// membership change happens (ADR 0037 §4/§7).
///
/// The same split, for the same reason, as [`PromotionPlan`]: `ReplaceVoter`'s
/// key transfer grants root-equivalent custody (§4), so every gate — `old` a
/// sitting voter, `new` a caught-up learner, the postconditions — must pass
/// **before** the key leaves the leader's disk. A `new` that is refused here
/// is never keyed and never appears in the custody accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementPlan {
    /// `new` is already a voter and `old` is gone from membership entirely:
    /// the ADR 0037 §6 idempotent no-op. No key transfer, no change.
    Settled,
    /// Every gate passed; the replacement may proceed once `new` confirms
    /// durable key receipt.
    Ready,
}

/// The openraft-backed [`Consensus`] implementation.
pub struct OpenraftConsensus {
    raft: Raft<TypeConfig>,
    status: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    proposal_permits: Arc<Semaphore>,
    /// The expected voter-set size (ADR 0037 §7); `0` disables the ceiling.
    /// See [`crate::node::NodeOptions::cluster_size`].
    cluster_size: usize,
    /// How long a voter may go unanswered before the evidence-gated removal
    /// path may fold it out of the voter set (ADR 0037 §7). See
    /// [`crate::node::NodeOptions::removal_grace`].
    removal_grace: Duration,
    /// How long a learner may go unanswered before the periodic learner-GC
    /// task retires it (ADR 0037 §7). See
    /// [`crate::node::NodeOptions::learner_expiry`].
    learner_expiry: Duration,
    /// Per-peer contact evidence, written by the Raft network client on every
    /// AppendEntries round-trip (heartbeats included). The evidence source for
    /// evidence-gated voter removal and stale-learner GC (ADR 0037 §7): a
    /// live-but-idle peer keeps acknowledging heartbeats, so — unlike
    /// matched-index progress — it never goes stale on an idle cluster.
    contact: Arc<ContactTracker>,
    /// Serializes every check-then-act membership mutation on this leader —
    /// promotion commits, replacements, removals, address repoints, and the
    /// learner-GC reap. openraft serializes the `change_membership` calls
    /// themselves, but not the *decisions* in front of them: without this
    /// lock, learner GC could re-verify "still a learner" while a promotion
    /// is mid-commit and then remove a brand-new voter — precisely the
    /// background voter reaper ADR 0037 §7 forbids.
    membership_change: tokio::sync::Mutex<()>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft: Raft<TypeConfig>,
        status: watch::Receiver<ConsensusStatus>,
        views: StateViews,
        cluster_size: usize,
        removal_grace: Duration,
        learner_expiry: Duration,
        contact: Arc<ContactTracker>,
    ) -> Self {
        OpenraftConsensus {
            raft,
            status,
            views,
            proposal_permits: Arc::new(Semaphore::new(MAX_INFLIGHT_PROPOSALS)),
            cluster_size,
            removal_grace,
            learner_expiry,
            contact,
            membership_change: tokio::sync::Mutex::new(()),
        }
    }

    /// The dial address membership currently records for `node`, or `None`
    /// when it is not a member at all. The one read the ADR 0037 §6
    /// idempotency short-circuits are expressed in terms of.
    fn member_address(&self, node: CoordinatorId) -> Option<String> {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        let addr = m
            .membership_config
            .nodes()
            .find(|(id, _)| **id == node)
            .map(|(_, n)| n.addr.clone());
        addr
    }

    /// The current voter set, from local membership metrics.
    fn voter_ids(&self) -> BTreeSet<CoordinatorId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect()
    }

    /// Whether `node` appears in membership at all (voter or learner).
    fn is_member(&self, node: CoordinatorId) -> bool {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .any(|(id, _)| *id == node)
    }

    /// This replica's own Raft identity — the leader, on every path that
    /// reaches the gates below (a follower's `change_membership` returns
    /// `NotLeader` regardless).
    fn local_id(&self) -> CoordinatorId {
        self.raft.metrics().borrow().id
    }

    /// The ADR 0016 catch-up gate: refuse to raise a learner into the quorum
    /// until its replication lag is within [`PROMOTION_LAG_MAX`].
    ///
    /// Best-effort by construction — it needs leader-side replication
    /// metrics. On a non-leader (no replication map) or for a learner no
    /// stream tracks yet, the answer is "not caught up", and a racing
    /// step-down still surfaces `NotLeader` from `change_membership`.
    fn check_promotion_lag(&self, node: CoordinatorId) -> Result<(), ConsensusError> {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        let leader_last = metrics.last_log_index.unwrap_or(0);
        let matched = metrics
            .replication
            .as_ref()
            .and_then(|repl| repl.get(&node).copied());
        let lag = match matched {
            Some(entry) => leader_last.saturating_sub(entry.map(|id| id.index).unwrap_or(0)),
            None => return Err(ConsensusError::LearnerNotCaughtUp { lag: leader_last }),
        };
        if lag > PROMOTION_LAG_MAX {
            return Err(ConsensusError::LearnerNotCaughtUp { lag });
        }
        Ok(())
    }

    /// The at-most-one voter this leader may fold out of a promotion's joint
    /// change as *evidence-dead* (ADR 0037 §7).
    ///
    /// Evidence is the leader's own replication observation and nothing else:
    /// [`ContactTracker::failed_contact_for`] past `removal_grace`, which is
    /// unanswered-while-attempting time, not log-position staleness (an idle
    /// cluster advances no matched index, and a live-but-idle voter keeps
    /// acknowledging heartbeats). Two members are never candidates: the
    /// leader itself, which is trivially in contact with itself and has no
    /// tracker entry, and `promote`, which is the point of the change. The
    /// longest-dead voter wins, so repeated promotions drain the corpses in a
    /// stable order rather than picking arbitrarily among them.
    fn evidence_dead_voter(&self, promote: CoordinatorId) -> Option<CoordinatorId> {
        let now = std::time::Instant::now();
        let local = self.local_id();
        self.voter_ids()
            .into_iter()
            .filter(|voter| *voter != promote && *voter != local)
            .filter_map(|voter| {
                self.contact
                    .failed_contact_for(voter, now)
                    .filter(|failing| *failing > self.removal_grace)
                    .map(|failing| (failing, voter))
            })
            .max_by_key(|(failing, _)| *failing)
            .map(|(_, voter)| voter)
    }

    /// The explicit postconditions every membership change that touches the
    /// voter set must satisfy *before* it is proposed (ADR 0037 §4/§7).
    ///
    /// `continuing` is the voter set the change would leave behind, and
    /// `incoming` (when there is one) is the node being raised into it —
    /// used only to attribute a refusal, never as evidence: passing the
    /// replication-lag gate does **not** prove life on an idle log (a dead
    /// learner's matched index sits at zero lag forever), so the incoming
    /// node earns its liveness the same way every other voter does — a
    /// fresh acknowledgement.
    ///
    /// 1. **Size.** At most `cluster_size` voters, when one is configured.
    /// 2. **Live majority from the leader's vantage.** A strict majority of
    ///    `continuing` must have **acknowledged** this leader within
    ///    [`LIVE_CONTACT_STALENESS`] — attempts and matched indexes are not
    ///    evidence of life; only the leader itself is credited without an
    ///    ack (it cannot dial itself). This is what stops an evidence-gated
    ///    removal, or an operator's `ReplaceVoter`, from committing a set
    ///    that cannot elect.
    /// 3. **Key custody.** At least one continuing voter must hold a
    ///    confirmed CA key (§4): no change may leave the cluster unable to
    ///    sign. Read from replicated state, so it sees the confirmation the
    ///    key transfer just recorded.
    fn check_change_postconditions(
        &self,
        continuing: &BTreeSet<CoordinatorId>,
        incoming: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        if self.cluster_size > 0 && continuing.len() > self.cluster_size {
            return Err(ConsensusError::VoterSetFull {
                node: incoming.unwrap_or_else(|| self.local_id()),
                voters: continuing.len(),
                cluster_size: self.cluster_size,
            });
        }

        let local = self.local_id();
        let live = continuing
            .iter()
            .filter(|voter| {
                **voter == local || self.contact.is_live(**voter, LIVE_CONTACT_STALENESS)
            })
            .count();
        if live * 2 <= continuing.len() {
            return Err(ConsensusError::QuorumAtRisk {
                live,
                continuing: continuing.len(),
            });
        }

        let view = self.views.latest();
        let state = view.state();
        // Only a cluster that *owns* a CA has key custody to preserve. With no
        // replicated CA certificate the trust root was provisioned externally
        // (ADR 0022's pre-0037 model, which the older test fleets still use):
        // there is no cluster-held private key, no transfer, and nothing a
        // membership change could strand.
        if state.ca.is_some()
            && !continuing
                .iter()
                .any(|voter| state.has_key_confirmation(*voter))
        {
            return Err(ConsensusError::NoKeyHolder);
        }
        Ok(())
    }

    /// Whether `voter` qualifies as evidence-dead **right now**: failing
    /// contact for longer than `removal_grace` (ADR 0037 §7). Consulted both
    /// when a removal is planned and again at commit — a predecessor that
    /// recovers during the key transfer stops qualifying, and a live
    /// predecessor never qualifies.
    fn still_evidence_dead(&self, voter: CoordinatorId) -> bool {
        self.contact
            .failed_contact_for(voter, std::time::Instant::now())
            .is_some_and(|failing| failing > self.removal_grace)
    }

    /// The `ReplaceVoter` gate sequence (ADR 0037 §6/§7), shared verbatim by
    /// [`Consensus::plan_replacement`] — run *before* the key transfer, so a
    /// refused `new` is never keyed — and [`Consensus::replace_voter`],
    /// which re-runs it under the membership lock at commit because the
    /// transfer is a network round-trip and a replicated write.
    fn replacement_gates(
        &self,
        old: CoordinatorId,
        new: CoordinatorId,
    ) -> Result<ReplacementPlan, ConsensusError> {
        let voters = self.voter_ids();

        // The §6 idempotency short-circuit, first: "already the shape you
        // asked for" — `new` a voter and `old` gone from membership entirely
        // — is a plain success. (`old == new` can never satisfy it: one id
        // cannot be both a voter and absent.)
        if voters.contains(&new) && !self.is_member(old) {
            return Ok(ReplacementPlan::Settled);
        }
        // The verb replaces a *voter*: naming a learner or a stranger as
        // `old` is a caller error no waiting fixes. This also answers
        // `old == new` naming a learner.
        if !voters.contains(&old) {
            return Err(ConsensusError::OldNotVoter { node: old });
        }
        // The verb promotes a *learner*: outside the settled no-op above, a
        // sitting voter is never a valid `new` — accepting one would quietly
        // turn the call into a bare removal of `old`, a shrink path §7 does
        // not grant this verb. This also answers `old == new` naming a
        // voter.
        if voters.contains(&new) {
            return Err(ConsensusError::NewAlreadyVoter { node: new });
        }
        if !self.is_member(new) {
            return Err(ConsensusError::UnknownNode { node: new });
        }
        // The same catch-up gate promotion uses (ADR 0016).
        self.check_promotion_lag(new)?;

        let mut continuing = voters;
        continuing.insert(new);
        continuing.remove(&old);
        self.check_change_postconditions(&continuing, Some(new))?;
        Ok(ReplacementPlan::Ready)
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
        // The ADR 0037 §6 idempotency contract, checked BEFORE any other gate:
        // the convergence loop re-runs this verb on every tick and on every
        // restart, so "already admitted at this address" must be a plain
        // success rather than an openraft error the caller has to interpret.
        // The same id at a *different* address is the one refusal — no silent
        // repointing.
        if let Some(current) = self.member_address(node) {
            if current == addr {
                return Ok(());
            }
            return Err(ConsensusError::AddressConflict {
                node,
                current,
                requested: addr,
            });
        }

        // Non-blocking: return once replication to the learner is set up. The
        // learner catches up via snapshot install plus log replay with no
        // quorum impact; the CLI polls health before promotion (ADR 0016).
        self.raft
            .add_learner(node, BasicNode { addr }, false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    fn plan_promotion(&self, promote: CoordinatorId) -> Result<PromotionPlan, ConsensusError> {
        // ADR 0037 §6 idempotency, before the lag gate and deliberately so: a
        // voter has no learner replication entry to measure, so reaching the
        // gate below would bounce a settled voter with `LearnerNotCaughtUp`
        // forever. "Already the shape you asked for" is success.
        let voters = self.voter_ids();
        if voters.contains(&promote) {
            return Ok(PromotionPlan::AlreadyVoter);
        }
        // Promoting something membership has never heard of is terminal,
        // not "behind": nothing is replicating to it to catch up.
        if !self.is_member(promote) {
            return Err(ConsensusError::UnknownNode { node: promote });
        }

        self.check_promotion_lag(promote)?;

        // ADR 0037 §7 voter-count ceiling. `cluster_size == 0` means no
        // configured expectation, so nothing to overshoot. Otherwise the
        // leader may fold in the removal of AT MOST ONE evidence-dead voter
        // — the "hands-off path": a terminate-before-launch replacement needs
        // no operator to name the pair, because the corpse names itself.
        let mut evidence_removal = None;
        if self.cluster_size > 0 && voters.len() + 1 > self.cluster_size {
            let Some(dead) = self.evidence_dead_voter(promote) else {
                return Err(ConsensusError::NoRemovablePeer {
                    node: promote,
                    voters: voters.len(),
                    cluster_size: self.cluster_size,
                });
            };
            // One removal, never two (§7). If the set is over-full by more
            // than one seat, the promotion waits for an operator rather than
            // shrinking the quorum on its own.
            if voters.len() > self.cluster_size {
                return Err(ConsensusError::NoRemovablePeer {
                    node: promote,
                    voters: voters.len(),
                    cluster_size: self.cluster_size,
                });
            }
            evidence_removal = Some(dead);
        }

        // The continuing set is checked here too, so a promotion that could
        // never satisfy the postconditions is refused before the key is
        // transferred — the key reaches a disk only for a promotion that can
        // actually proceed (§4 root-equivalence is granted grudgingly).
        let mut continuing = voters.clone();
        continuing.insert(promote);
        if let Some(dead) = evidence_removal {
            continuing.remove(&dead);
        }
        match self.check_change_postconditions(&continuing, Some(promote)) {
            Ok(()) => {}
            // A removal that would break the live majority is a removal that
            // does not qualify, which is exactly "no removable peer" — the
            // learner keeps polling rather than reading a different verdict
            // from the same situation.
            Err(ConsensusError::QuorumAtRisk { .. }) if evidence_removal.is_some() => {
                return Err(ConsensusError::NoRemovablePeer {
                    node: promote,
                    voters: voters.len(),
                    cluster_size: self.cluster_size,
                })
            }
            Err(other) => return Err(other),
        }

        Ok(PromotionPlan::Ready { evidence_removal })
    }

    async fn commit_promotion(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (see
        // `membership_change`): the checks below and the change they guard
        // must be one indivisible decision.
        let _guard = self.membership_change.lock().await;

        // Re-run the cheap gates: `plan_promotion` ran before the key
        // transfer, which is a network round-trip and a replicated write, so
        // membership may have moved underneath it.
        let voters = self.voter_ids();
        let removal_settled = remove.map_or(true, |departed| !self.is_member(departed));
        if voters.contains(&promote) && removal_settled {
            return Ok(());
        }
        if !self.is_member(promote) {
            return Err(ConsensusError::UnknownNode { node: promote });
        }
        if !voters.contains(&promote) {
            self.check_promotion_lag(promote)?;
        }

        // Re-validate the evidence itself, not just membership (ADR 0037 §7:
        // "a *live* predecessor never qualifies"): the voter the plan chose
        // may have recovered during the key transfer, and removing it anyway
        // would evict a live peer on stale evidence. Refusing here sends the
        // learner back around the loop, which re-plans against fresh
        // evidence on its next tick.
        if let Some(departed) = remove {
            if !voters.contains(&departed) || !self.still_evidence_dead(departed) {
                return Err(ConsensusError::NoRemovablePeer {
                    node: promote,
                    voters: voters.len(),
                    cluster_size: self.cluster_size,
                });
            }
        }

        let mut continuing = voters.clone();
        continuing.insert(promote);
        if let Some(departed) = remove {
            continuing.remove(&departed);
        }
        self.check_change_postconditions(&continuing, Some(promote))?;

        let changes = match remove {
            // Pure promotion: raise one learner to voter, leaving the rest of
            // the voter set untouched.
            None => ChangeMembers::AddVoterIds(BTreeSet::from([promote])),
            // Promotion plus evidence-gated removal as ONE joint change
            // (ADR 0037 §7): the voter count never overshoots and quorum
            // among survivors holds throughout.
            Some(_) => ChangeMembers::ReplaceAllVoters(continuing),
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

    fn plan_replacement(
        &self,
        old: CoordinatorId,
        new: CoordinatorId,
    ) -> Result<ReplacementPlan, ConsensusError> {
        self.replacement_gates(old, new)
    }

    async fn replace_voter(
        &self,
        old: CoordinatorId,
        new: CoordinatorId,
    ) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (see
        // `membership_change`), and re-run the full gate sequence: the plan
        // the caller acted on predates the key transfer, so nothing from it
        // is trusted here.
        let _guard = self.membership_change.lock().await;
        if self.replacement_gates(old, new)? == ReplacementPlan::Settled {
            return Ok(());
        }

        let mut continuing = self.voter_ids();
        continuing.insert(new);
        continuing.remove(&old);

        // ONE joint change, per §7: promote and remove commit atomically, so
        // the voter count never overshoots and — because `new` confirmed
        // durable key receipt before this call — the continuing set holds the
        // signing key even when `old` was the last other holder (the
        // single-voter replacement case).
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(continuing), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn remove_node(&self, node: CoordinatorId) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (see
        // `membership_change`).
        let _guard = self.membership_change.lock().await;

        // ADR 0037 §6: a node that is already absent is the state the caller
        // asked for, so a retried removal succeeds instead of erroring.
        if self.member_address(node).is_none() {
            return Ok(());
        }

        let voters = self.voter_ids();
        if voters.contains(&node) {
            // The explicit operator removal of a *voter* (ADR 0037 §7's third
            // and last shrink path). Only the key-custody postcondition
            // applies: an operator naming a voter is asserting a judgement
            // the leader's contact evidence cannot make for it — but no
            // authority can waive §4, so a removal that would leave the
            // continuing voters unable to sign is refused for repair.
            let mut continuing = voters.clone();
            continuing.remove(&node);
            let view = self.views.latest();
            let state = view.state();
            if state.ca.is_some()
                && !continuing
                    .iter()
                    .any(|voter| state.has_key_confirmation(*voter))
            {
                return Err(ConsensusError::NoKeyHolder);
            }
            // `RemoveVoters` with `retain = false` drops the node from the
            // voter set *and* from `nodes` in one change (openraft 0.9's
            // `Membership::next_coherent` erases non-retained former voters
            // from the node map), so no follow-up `RemoveNodes` is needed —
            // and the seat is gone outright rather than demoted to learner,
            // which is what "the departed node id is never reused" requires.
            return self
                .raft
                .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([node])), false)
                .await
                .map(|_| ())
                .map_err(map_client_write_error);
        }

        // A learner: no voter-set change at all, so no key postcondition to
        // enforce. This is also the learner-GC task's removal path (§7).
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node])), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    fn learner_expiry(&self) -> Duration {
        self.learner_expiry
    }

    fn expired_learners(&self) -> Vec<CoordinatorId> {
        let now = std::time::Instant::now();
        let voters = self.voter_ids();
        let metrics = self.raft.metrics();
        let learners: Vec<CoordinatorId> = metrics
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, _)| *id)
            .filter(|id| !voters.contains(id))
            .collect();
        learners
            .into_iter()
            .filter(|learner| {
                self.contact
                    .failed_contact_for(*learner, now)
                    .is_some_and(|failing| failing > self.learner_expiry)
            })
            .collect()
    }

    async fn reap_expired_learner(
        &self,
        node: CoordinatorId,
        retire: Option<Command>,
    ) -> Result<bool, ConsensusError> {
        // Serialize with every membership mutation, and hold the lock across
        // the WHOLE sequence — eligibility re-check, retirement proposal, and
        // removal. [`Consensus::expired_learners`] chose this candidate an
        // await or two ago; if it has been promoted since (or is mid-commit
        // behind this lock), reaping it anyway would remove a voter — the
        // background voter reaper ADR 0037 §7 forbids — and retiring its
        // identity would orphan a live seat. Every check therefore re-runs
        // here, at the destructive point, under the same lock promotion
        // commits hold.
        let _guard = self.membership_change.lock().await;

        if self.member_address(node).is_none() {
            // Already gone (a crash between a prior reap's retirement and
            // removal re-arrives here: the retirement stands, and the
            // removal completes on this tick).
        } else {
            if self.voter_ids().contains(&node) {
                return Ok(false);
            }
            let expired = self
                .contact
                .failed_contact_for(node, std::time::Instant::now())
                .is_some_and(|failing| failing > self.learner_expiry);
            if !expired {
                return Ok(false);
            }
        }

        // Retirement lands BEFORE the seat vanishes (ADR 0037 §7 one-seat-
        // ever): a re-arriving installation with this identity must find the
        // binding already marked, never a window where the seat is gone but
        // the identity is re-admittable. The command is committed — ordered
        // ahead of the membership change in the same log — before the
        // removal is proposed, and a *rejected* retirement aborts the reap:
        // the seat is never released with its identity still re-admittable.
        if let Some(command) = retire {
            let applied = <Self as Consensus>::propose(self, command).await?;
            if let Err(reason) = applied.outcome {
                tracing::warn!(
                    node,
                    %reason,
                    "learner-gc reap: retiring the machine binding was rejected; leaving the \
                     seat in place"
                );
                return Ok(false);
            }
        }

        if self.member_address(node).is_some() {
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node])), false)
                .await
                .map(|_| ())
                .map_err(map_client_write_error)?;
        }
        Ok(true)
    }

    async fn set_node_address(
        &self,
        node: CoordinatorId,
        addr: String,
    ) -> Result<(), ConsensusError> {
        // Serialize with every other membership mutation (see
        // `membership_change`).
        let _guard = self.membership_change.lock().await;

        // The operator-only break-glass of ADR 0037 §6. `SetNodes` is the one
        // openraft change that can split-brain when misused, which is exactly
        // why no machine credential can reach this path and why the admin
        // service dial-back-verifies the *new* address before calling it.
        match self.member_address(node) {
            Some(current) if current == addr => return Ok(()),
            Some(_) => {}
            None => return Err(ConsensusError::UnknownNode { node }),
        }
        self.raft
            .change_membership(
                ChangeMembers::SetNodes(BTreeMap::from([(node, BasicNode { addr })])),
                false,
            )
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

/// The ADR 0037 §6 idempotency short-circuits and the §7 voter-count
/// ceiling, exercised against a real single-voter openraft cluster over the
/// segment storage engine. No real network is needed: every case here is
/// decided from local membership metrics before any RPC would be sent, so
/// the network factory below only has to exist, never actually connect.
#[cfg(test)]
mod idempotency_tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::error::{
        Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
    };
    use openraft::network::RPCOption;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::{BasicNode, Config, Raft, RaftNetwork, RaftNetworkFactory, Snapshot, Vote};

    use crate::contact::ContactTracker;
    use crate::fs::RealFs;
    use crate::storage::{self, StorageOptions};
    use crate::view::{ViewPublisher, ViewPublisherConfig};
    use crate::{status, Consensus, ConsensusError, CoordinatorId};

    use super::{OpenraftConsensus, PromotionPlan, TypeConfig};

    /// A network that never reaches a peer: `new_client` builds a lazy
    /// handle per openraft's contract (no dial on construction), and every
    /// RPC method fails `Unreachable`.
    #[derive(Clone)]
    struct NoopNetworkFactory;

    struct NoopNetwork;

    impl RaftNetworkFactory<TypeConfig> for NoopNetworkFactory {
        type Network = NoopNetwork;

        async fn new_client(&mut self, _target: CoordinatorId, _node: &BasicNode) -> NoopNetwork {
            NoopNetwork
        }
    }

    impl RaftNetwork<TypeConfig> for NoopNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<CoordinatorId>,
            RPCError<CoordinatorId, BasicNode, RaftError<CoordinatorId>>,
        > {
            Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "idempotency_tests: no real network",
            ))))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<CoordinatorId>,
            _option: RPCOption,
        ) -> Result<
            VoteResponse<CoordinatorId>,
            RPCError<CoordinatorId, BasicNode, RaftError<CoordinatorId>>,
        > {
            Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "idempotency_tests: no real network",
            ))))
        }

        async fn full_snapshot(
            &mut self,
            _vote: Vote<CoordinatorId>,
            _snapshot: Snapshot<TypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<CoordinatorId>, StreamingError<TypeConfig, Fatal<CoordinatorId>>>
        {
            Err(StreamingError::Unreachable(Unreachable::new(
                &io::Error::other("idempotency_tests: no real network"),
            )))
        }
    }

    /// One test cluster plus the two side-channels the ADR 0037 §4/§7 gates
    /// read: the published view (where key-possession confirmations live) and
    /// the leader's contact evidence.
    ///
    /// This harness's apply task does not publish views — it is
    /// `run_apply_task` with no publisher wired in — so a test that needs a
    /// replicated fact visible seeds it through [`Harness::confirm_key`]
    /// rather than proposing it and waiting.
    struct Harness {
        _dir: tempfile::TempDir,
        consensus: OpenraftConsensus,
        node_id: CoordinatorId,
        contact: Arc<ContactTracker>,
        published: std::sync::Mutex<(ViewPublisher, coppice_state::StateMachine, u64)>,
    }

    impl Harness {
        /// Give this cluster a CA of its own (ADR 0037 §4): the replicated
        /// certificate is what turns key custody into a membership invariant
        /// at all, so the postcondition only bites once it exists.
        fn record_ca(&self) {
            use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
            let key = KeyPair::generate().expect("ca key");
            let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(DnType::CommonName, "adapter-test-ca");
            let cert = params.self_signed(&key).expect("self-sign ca");

            let mut published = self.published.lock().expect("published state");
            let (publisher, state, index) = &mut *published;
            state.ca = Some(coppice_state::CaCertificate {
                bundle: coppice_state::CaCertBundle::parse(cert.pem())
                    .expect("a real CA cert PEM is a valid bundle"),
                recorded_at: coppice_core::time::Timestamp::now(),
            });
            *index += 1;
            publisher.publish_now(state, *index);
        }

        /// Record (in the published view) that `node` confirmed durable
        /// receipt of the CA key — the §4 fact the custody postcondition
        /// reads.
        fn confirm_key(&self, node: CoordinatorId) {
            let mut published = self.published.lock().expect("published state");
            let (publisher, state, index) = &mut *published;
            state
                .key_confirmations
                .insert(node, coppice_core::time::Timestamp::now());
            *index += 1;
            publisher.publish_now(state, *index);
        }
    }

    /// Bring up a real single-voter cluster over the segment storage engine
    /// with the no-op network above — enough to exercise membership
    /// short-circuits, which are decided from local metrics.
    async fn single_voter(cluster_size: usize) -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let history_id = *b"idempotency-test";
        let options = StorageOptions::new(history_id);
        let fs = RealFs::new(dir.path());
        storage::init(&fs, &options).expect("init data dir");
        let recovered = storage::open(RealFs::new(dir.path()), options).expect("open");
        let node_id = recovered.node_id;
        let state = recovered.state.clone();
        let last_applied_index = recovered.last_applied.map(|id| id.index).unwrap_or(0);
        let (log, sm) = recovered.into_stores_with_local_apply_task();
        let committed_rx = log.committed_watch();

        let (publisher, views) = ViewPublisher::new(
            state.clone(),
            last_applied_index,
            ViewPublisherConfig::default(),
        );

        let config = Config {
            cluster_name: "idempotency-test".to_string(),
            ..Default::default()
        }
        .validate()
        .expect("valid config");

        let raft = Raft::new(node_id, Arc::new(config), NoopNetworkFactory, log, sm)
            .await
            .expect("raft construction");

        raft.initialize(BTreeMap::from([(
            node_id,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        )]))
        .await
        .expect("initialize single-voter cluster");

        // A single-voter cluster becomes leader with no RPC round-trip; poll
        // metrics rather than sleeping a fixed guess.
        loop {
            if raft.metrics().borrow().current_leader == Some(node_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let status = status::spawn(raft.metrics(), committed_rx);
        let contact = Arc::new(ContactTracker::default());
        let consensus = OpenraftConsensus::new(
            raft,
            status,
            views,
            cluster_size,
            Duration::from_secs(120),
            // A zero learner-expiry: any *unanswered* attempt is instantly
            // past the bound, so the GC tests exercise the criterion itself
            // (failed acknowledgement vs. an answered heartbeat) rather than
            // waiting out a wall-clock hour.
            Duration::ZERO,
            Arc::clone(&contact),
        );
        Harness {
            _dir: dir,
            consensus,
            node_id,
            contact,
            published: std::sync::Mutex::new((publisher, state, last_applied_index)),
        }
    }

    #[tokio::test]
    async fn add_learner_same_address_is_noop() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        // The bootstrap voter is already a member at this address (ADR 0037
        // §6): a retried `AddLearner` must be a plain success, not an error
        // the caller has to interpret.
        let result = consensus
            .add_learner(node_id, "127.0.0.1:0".to_string())
            .await;
        assert!(
            result.is_ok(),
            "same-address add_learner must be a no-op success: {result:?}"
        );
    }

    #[tokio::test]
    async fn add_learner_different_address_is_address_conflict() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let result = consensus
            .add_learner(node_id, "127.0.0.1:9999".to_string())
            .await;
        match result {
            Err(ConsensusError::AddressConflict { node, .. }) => {
                assert_eq!(node, node_id);
            }
            other => panic!("expected AddressConflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn plan_promotion_already_voter_is_noop() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        // Checked before the replication-lag gate (ADR 0037 §6): a voter has
        // no learner replication entry to measure, so this must not bounce
        // with `LearnerNotCaughtUp`.
        let plan = consensus.plan_promotion(node_id);
        assert_eq!(
            plan.expect("already-voter promotion plans cleanly"),
            PromotionPlan::AlreadyVoter
        );
    }

    #[tokio::test]
    async fn plan_promotion_unknown_node_is_refused() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let _ = h.node_id;
        match consensus.plan_promotion(999) {
            Err(ConsensusError::UnknownNode { node }) => assert_eq!(node, 999),
            other => panic!("expected UnknownNode, got {other:?}"),
        }
        // "Promote a node that was never admitted" is a caller error no
        // amount of waiting fixes.
        assert!(!ConsensusError::UnknownNode { node: 999 }.is_retryable());
    }

    #[tokio::test]
    async fn remove_node_absent_is_noop() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let _ = h.node_id;
        let result = consensus.remove_node(999).await;
        assert!(
            result.is_ok(),
            "removing an absent node must be a no-op success: {result:?}"
        );
    }

    #[tokio::test]
    async fn set_node_address_unknown_node_is_refused() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let _ = h.node_id;
        let result = consensus
            .set_node_address(999, "127.0.0.1:1".to_string())
            .await;
        assert!(matches!(
            result,
            Err(ConsensusError::UnknownNode { node: 999 })
        ));
    }

    #[tokio::test]
    async fn set_node_address_same_address_is_noop() {
        let h = single_voter(0).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let result = consensus
            .set_node_address(node_id, "127.0.0.1:0".to_string())
            .await;
        assert!(
            result.is_ok(),
            "same-address set_node_address must be a no-op: {result:?}"
        );
    }

    #[tokio::test]
    async fn plan_promotion_with_no_evidence_dead_peer_is_retryable() {
        // cluster_size 1: the bootstrap voter already fills the one seat, and
        // the only other "voter" is this leader itself, which is never an
        // evidence-dead candidate (ADR 0037 §7). So there is nothing to fold
        // out and the learner keeps polling.
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner registers a fresh learner (no RPC needed to return)");

        let result = consensus.plan_promotion(learner_id);
        match &result {
            Err(ConsensusError::NoRemovablePeer {
                node,
                voters,
                cluster_size,
            }) => {
                assert_eq!(*node, learner_id);
                assert_eq!(*voters, 1);
                assert_eq!(*cluster_size, 1);
            }
            other => panic!("expected NoRemovablePeer, got {other:?}"),
        }
        assert!(
            result.unwrap_err().is_retryable(),
            "NoRemovablePeer must be retryable: the learner polls until evidence matures or an \
             operator drives ReplaceVoter (ADR 0037 §7)"
        );
    }

    #[tokio::test]
    async fn plan_promotion_into_a_free_seat_needs_no_removal() {
        // cluster_size 3 with one voter: the seat is free, so the plan folds
        // in no removal at all.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        // Life is proven only by an acknowledgement (ADR 0037 §7): the
        // incoming learner earns its place in the live-majority count the
        // same way any voter does, so the fixture stages one fresh ack.
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);

        match consensus.plan_promotion(learner_id) {
            Ok(PromotionPlan::Ready { evidence_removal }) => assert_eq!(evidence_removal, None),
            other => panic!("expected a removal-free promotion plan, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_never_acknowledging_candidate_cannot_be_promoted() {
        // The inverse of the fixture above, and the review finding it
        // encodes: a dead learner sits at zero lag forever on an idle log, so
        // passing the lag gate is NOT evidence of life. With no ack ever
        // recorded — an attempt alone is this node talking, not the peer
        // answering — the continuing set {leader, candidate} has one live
        // member of two, no strict majority, and the plan is refused before
        // any key could be transferred.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.contact.note_attempt(learner_id);

        let result = consensus.plan_promotion(learner_id);
        assert!(
            matches!(result, Err(ConsensusError::QuorumAtRisk { live: 1, .. })),
            "an attempted-but-never-acknowledging candidate must not count as live: {result:?}"
        );
    }

    #[tokio::test]
    async fn a_change_leaving_no_confirmed_key_holder_is_refused() {
        // ADR 0037 §4's postcondition, enforced on every path: this cluster
        // owns a CA, so *someone* in the continuing voter set must hold a
        // confirmed key. With no confirmation recorded anywhere, promoting a
        // learner is refused for operator repair — terminally, not something
        // polling fixes.
        let h = single_voter(3).await;
        h.record_ca();
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        // A live candidate (fresh ack staged), so the refusal below is
        // attributable to custody alone.
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);

        let result = consensus.plan_promotion(learner_id);
        assert!(
            matches!(result, Err(ConsensusError::NoKeyHolder)),
            "expected NoKeyHolder, got {result:?}"
        );
        assert!(!ConsensusError::NoKeyHolder.is_retryable());
    }

    #[tokio::test]
    async fn replace_voter_refuses_an_old_that_is_not_a_voter() {
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");

        // The learner is in membership but is not a voter, so there is
        // nothing to replace (ADR 0037 §7) — and no waiting changes that.
        let result = consensus.replace_voter(learner_id, node_id).await;
        match result {
            Err(ConsensusError::OldNotVoter { node }) => assert_eq!(node, learner_id),
            other => panic!("expected OldNotVoter, got {other:?}"),
        }
        assert!(!ConsensusError::OldNotVoter { node: learner_id }.is_retryable());
    }

    #[tokio::test]
    async fn replace_voter_refuses_an_unknown_new() {
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);
        let result = consensus.replace_voter(node_id, 999).await;
        match result {
            Err(ConsensusError::UnknownNode { node }) => assert_eq!(node, 999),
            other => panic!("expected UnknownNode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replace_voter_passes_the_local_gates_and_proceeds_to_the_joint_change() {
        // The single-voter replacement case (ADR 0037 §7): `old` is this very
        // leader, `new` is a caught-up learner that has confirmed key
        // receipt. Every local gate must pass — the call then proceeds into a
        // real openraft joint change, which this no-op-network harness can
        // never complete because the incoming voter cannot acknowledge the
        // entry. Bound it with a short timeout: what is under test is that no
        // local gate short-circuits.
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.record_ca();
        h.confirm_key(learner_id);
        // The continuing set is {new} alone: a strict majority of one needs
        // the incoming learner itself live, proven by a fresh ack.
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);

        let outcome = tokio::time::timeout(
            Duration::from_millis(200),
            consensus.replace_voter(node_id, learner_id),
        )
        .await;
        if let Ok(Err(e)) = outcome {
            panic!("no local gate may refuse this replacement: {e:?}");
        }
    }

    #[tokio::test]
    async fn replace_voter_refuses_a_sitting_voter_as_new() {
        // ADR 0037 §7: outside the exact idempotent no-op, `new` must be a
        // caught-up *learner* — a sitting voter as `new` would quietly turn
        // the call into a bare removal of `old`. `old == new` naming the one
        // voter is the sharpest form: it is a voter (so `OldNotVoter` does
        // not fire) and it is not the settled no-op (it is still a member),
        // so the new-is-a-voter refusal is the one that must answer.
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);

        let result = consensus.replace_voter(node_id, node_id).await;
        match result {
            Err(ConsensusError::NewAlreadyVoter { node }) => assert_eq!(node, node_id),
            other => panic!("expected NewAlreadyVoter, got {other:?}"),
        }
        assert!(!ConsensusError::NewAlreadyVoter { node: node_id }.is_retryable());

        // The plan-side gate answers identically, so the refusal lands
        // before any key transfer could (ADR 0037 §4).
        let planned = consensus.plan_replacement(node_id, node_id);
        assert!(
            matches!(planned, Err(ConsensusError::NewAlreadyVoter { .. })),
            "plan_replacement must refuse the same pair: {planned:?}"
        );
    }

    #[tokio::test]
    async fn commit_refuses_a_removal_whose_evidence_went_stale() {
        // ADR 0037 §7: "a *live* predecessor never qualifies" — and the
        // key transfer between planning and committing is exactly the window
        // a predecessor can recover in. A commit handed a removal target
        // that is not evidence-dead RIGHT NOW must refuse and send the
        // caller back around the loop, whatever some earlier plan said. Here
        // the "plan" is simulated by naming the leader's own (never
        // evidence-dead) seat as the removal — the strongest form of stale
        // evidence.
        let h = single_voter(1).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        h.record_ca();
        h.confirm_key(node_id);
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);

        let result = consensus.commit_promotion(learner_id, Some(node_id)).await;
        assert!(
            matches!(result, Err(ConsensusError::NoRemovablePeer { .. })),
            "a removal target that is not evidence-dead at commit must be refused: {result:?}"
        );
    }

    #[tokio::test]
    async fn expired_learners_needs_failed_contact_not_silence() {
        // A learner nobody has attempted to reach has no evidence against it
        // (ADR 0037 §7: the criterion is failed acknowledgement, never lack
        // of log advancement), so GC never sees it.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let node_id = h.node_id;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        assert!(consensus.expired_learners().is_empty());
    }

    #[tokio::test]
    async fn a_learner_the_leader_cannot_reach_expires() {
        // The GC criterion (ADR 0037 §7): attempted and unanswered for longer
        // than `learner_expiry` — which this harness sets to zero, so one
        // unanswered attempt suffices.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let learner_id = h.node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.contact.note_attempt(learner_id);
        assert_eq!(consensus.expired_learners(), vec![learner_id]);
    }

    #[tokio::test]
    async fn an_idle_learner_that_answers_heartbeats_never_expires() {
        // The converse the ADR calls out explicitly: a fully caught-up
        // learner on an idle cluster — the `new_node_id` of a pending
        // ReplaceVoter, say — sees no new log entries for hours but keeps
        // acknowledging heartbeats, and must survive indefinitely. Log
        // position is never the criterion.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let learner_id = h.node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);
        assert!(
            consensus.expired_learners().is_empty(),
            "an acknowledging learner must never be reaped, however idle the log"
        );
    }

    #[tokio::test]
    async fn a_voter_is_never_reaped_by_learner_gc() {
        // There is no background *voter* reaper (ADR 0037 §7): voter
        // membership shrinks only inside ReplaceVoter, an evidence-gated
        // promotion, or an explicit admin remove.
        let h = single_voter(3).await;
        h.contact.note_attempt(h.node_id);
        assert!(h.consensus.expired_learners().is_empty());
    }

    #[tokio::test]
    async fn reap_never_touches_a_voter() {
        // The review finding this guards, at the reap call itself rather
        // than at `expired_learners`' filtering: even handed the sitting
        // voter's own id directly — as if some caller had raced past the
        // membership snapshot `expired_learners` took — `reap_expired_learner`
        // re-checks "still a learner" under the membership lock and refuses,
        // exactly the background voter reaper ADR 0037 §7 forbids.
        let h = single_voter(3).await;
        let voters_before = h.consensus.voter_ids();

        let result = h.consensus.reap_expired_learner(h.node_id, None).await;
        assert_eq!(
            result.expect("reap must not error, only decline"),
            false,
            "a sitting voter must never be reaped by learner GC"
        );

        let voters_after = h.consensus.voter_ids();
        assert_eq!(
            voters_before, voters_after,
            "membership must be unchanged after refusing to reap a voter"
        );
        assert!(
            h.consensus.is_member(h.node_id),
            "the voter must still be a member after the reap declined"
        );
    }

    #[tokio::test]
    async fn reap_skips_a_learner_that_is_not_expired() {
        // The other half of the same race guard: a learner that has been
        // ANSWERING — an attempt followed by an ack, the same evidence
        // `expired_learners` itself requires — must not be reaped even if a
        // caller invokes the reap directly, because it re-runs the
        // eligibility check at the destructive point rather than trusting
        // whatever evidence justified an earlier candidacy.
        let h = single_voter(3).await;
        let consensus = &h.consensus;
        let learner_id = h.node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");
        h.contact.note_attempt(learner_id);
        h.contact.note_ack(learner_id);

        let result = consensus.reap_expired_learner(learner_id, None).await;
        assert_eq!(
            result.expect("reap must not error, only decline"),
            false,
            "a learner that is currently answering must not be reaped"
        );
        assert!(
            consensus.is_member(learner_id),
            "the learner must still be in membership after the reap declined"
        );
    }
}
