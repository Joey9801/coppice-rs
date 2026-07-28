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
    /// The expected voter-set size (ADR 0037 §7); `0` disables the ceiling.
    /// See [`crate::node::NodeOptions::cluster_size`].
    cluster_size: usize,
    /// How long a voter may go unanswered before the evidence-gated removal
    /// path may fold it out of the voter set (ADR 0037 §7). See
    /// [`crate::node::NodeOptions::removal_grace`].
    #[allow(dead_code)] // consumed by the evidence-gated removal path (chunk 06 continuation)
    removal_grace: Duration,
    /// How long a learner may go unanswered before the periodic learner-GC
    /// task retires it (ADR 0037 §7). See
    /// [`crate::node::NodeOptions::learner_expiry`].
    #[allow(dead_code)] // consumed by the learner-GC task (chunk 06 continuation)
    learner_expiry: Duration,
    /// Per-peer contact evidence, written by the Raft network client on every
    /// AppendEntries round-trip (heartbeats included). The evidence source for
    /// evidence-gated voter removal and stale-learner GC (ADR 0037 §7): a
    /// live-but-idle peer keeps acknowledging heartbeats, so — unlike
    /// matched-index progress — it never goes stale on an idle cluster.
    #[allow(dead_code)] // read by the evidence-gated removal path (chunk 06 continuation)
    contact: Arc<ContactTracker>,
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

    async fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        // ADR 0037 §6 idempotency, before the lag gate and deliberately so: a
        // voter has no learner replication entry to measure, so reaching the
        // gate below would bounce a settled voter with `LearnerNotCaughtUp`
        // forever. "Already the shape you asked for" is success.
        {
            let metrics = self.raft.metrics();
            let m = metrics.borrow();
            let voters: BTreeSet<CoordinatorId> = m.membership_config.voter_ids().collect();
            let known = m.membership_config.nodes().any(|(id, _)| *id == promote);
            let removal_settled = remove.map_or(true, |departed| {
                !m.membership_config.nodes().any(|(id, _)| *id == departed)
            });
            if voters.contains(&promote) && removal_settled {
                return Ok(());
            }
            // Promoting something membership has never heard of is terminal,
            // not "behind": nothing is replicating to it to catch up.
            if !known {
                return Err(ConsensusError::UnknownNode { node: promote });
            }

            // ADR 0037 §7 voter-count ceiling: refuse a promotion that would
            // push the voter set past `cluster_size`, unless a paired
            // same-change removal keeps the post-change count level (the
            // hands-off evidence-gated-removal path folds a departing voter
            // into this same joint change, so it must never be blocked
            // here). `cluster_size == 0` means no configured expectation —
            // the ceiling is not enforced.
            //
            // chunk 06 seam: the confirmed-key-receipt precondition (§4) and
            // the evidence-gated removal that picks `remove` when no caller
            // named a pair (§7 "the hands-off path") both hook in here —
            // this check only enforces the count, not who is admissible.
            if self.cluster_size > 0 {
                let mut post_change = voters.clone();
                post_change.insert(promote);
                if let Some(departed) = remove {
                    post_change.remove(&departed);
                }
                if post_change.len() > self.cluster_size {
                    return Err(ConsensusError::VoterSetFull {
                        node: promote,
                        voters: voters.len(),
                        cluster_size: self.cluster_size,
                    });
                }
            }
        }

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
        // ADR 0037 §6: a node that is already absent is the state the caller
        // asked for, so a retried removal succeeds instead of erroring.
        if self.member_address(node).is_none() {
            return Ok(());
        }
        // Removes the node entirely. openraft requires it be a non-voter first;
        // a departed voter is dropped through `promote_voter`'s removal path.
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node])), false)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn set_node_address(
        &self,
        node: CoordinatorId,
        addr: String,
    ) -> Result<(), ConsensusError> {
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

    use super::{OpenraftConsensus, TypeConfig};

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

    /// Bring up a real single-voter cluster over the segment storage engine
    /// with the no-op network above — enough to exercise membership
    /// short-circuits, which are decided from local metrics.
    async fn single_voter(
        cluster_size: usize,
    ) -> (tempfile::TempDir, OpenraftConsensus, CoordinatorId) {
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

        let (_publisher, views) =
            ViewPublisher::new(state, last_applied_index, ViewPublisherConfig::default());

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
        let consensus = OpenraftConsensus::new(
            raft,
            status,
            views,
            cluster_size,
            Duration::from_secs(120),
            Duration::from_secs(3600),
            Arc::new(ContactTracker::default()),
        );
        (dir, consensus, node_id)
    }

    #[tokio::test]
    async fn add_learner_same_address_is_noop() {
        let (_dir, consensus, node_id) = single_voter(0).await;
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
        let (_dir, consensus, node_id) = single_voter(0).await;
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
    async fn promote_voter_already_voter_is_noop() {
        let (_dir, consensus, node_id) = single_voter(0).await;
        // Checked before the replication-lag gate (ADR 0037 §6): a voter has
        // no learner replication entry to measure, so this must not bounce
        // with `LearnerNotCaughtUp`.
        let result = consensus.promote_voter(node_id, None).await;
        assert!(
            result.is_ok(),
            "already-voter promotion must be a no-op success: {result:?}"
        );
    }

    #[tokio::test]
    async fn promote_voter_unknown_node_is_refused() {
        let (_dir, consensus, _node_id) = single_voter(0).await;
        let result = consensus.promote_voter(999, None).await;
        match result {
            Err(ConsensusError::UnknownNode { node }) => assert_eq!(node, 999),
            other => panic!("expected UnknownNode, got {other:?}"),
        }
        // "Promote a node that was never admitted" is a caller error no
        // amount of waiting fixes.
        assert!(!ConsensusError::UnknownNode { node: 999 }.is_retryable());
    }

    #[tokio::test]
    async fn remove_node_absent_is_noop() {
        let (_dir, consensus, _node_id) = single_voter(0).await;
        let result = consensus.remove_node(999).await;
        assert!(
            result.is_ok(),
            "removing an absent node must be a no-op success: {result:?}"
        );
    }

    #[tokio::test]
    async fn set_node_address_unknown_node_is_refused() {
        let (_dir, consensus, _node_id) = single_voter(0).await;
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
        let (_dir, consensus, node_id) = single_voter(0).await;
        let result = consensus
            .set_node_address(node_id, "127.0.0.1:0".to_string())
            .await;
        assert!(
            result.is_ok(),
            "same-address set_node_address must be a no-op: {result:?}"
        );
    }

    #[tokio::test]
    async fn promote_voter_voter_set_full_is_retryable() {
        // cluster_size 1: the bootstrap voter already fills the one seat.
        let (_dir, consensus, node_id) = single_voter(1).await;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner registers a fresh learner (no RPC needed to return)");

        let result = consensus.promote_voter(learner_id, None).await;
        match &result {
            Err(ConsensusError::VoterSetFull {
                node,
                voters,
                cluster_size,
            }) => {
                assert_eq!(*node, learner_id);
                assert_eq!(*voters, 1);
                assert_eq!(*cluster_size, 1);
            }
            other => panic!("expected VoterSetFull, got {other:?}"),
        }
        assert!(
            result.unwrap_err().is_retryable(),
            "VoterSetFull must be retryable: the learner polls until a seat opens (ADR 0037 §7)"
        );
    }

    #[tokio::test]
    async fn promote_voter_paired_with_remove_is_not_blocked_by_ceiling() {
        // The same seat count (cluster_size 1, one existing voter) but this
        // time the promotion is paired with removing the existing voter in
        // the same joint change: the post-change count stays at 1, so the
        // ceiling must never refuse it with `VoterSetFull` (ADR 0037 §7 —
        // "an explicit paired promote/remove is not blocked").
        let (_dir, consensus, node_id) = single_voter(1).await;
        let learner_id = node_id + 1000;
        consensus
            .add_learner(learner_id, "127.0.0.1:1".to_string())
            .await
            .expect("add_learner");

        // With the local lag gate satisfied too (a fresh learner's zero
        // matched-index reads as zero lag, see the harness's replication
        // metrics), the call proceeds past both local gates into a real
        // openraft joint-consensus commit — which this no-op-network
        // harness can never complete, since the incoming voter can never
        // actually acknowledge the entry. Bound it with a short timeout:
        // the only thing under test is that neither local gate short-circuits
        // with `VoterSetFull`, not that the commit finishes.
        let outcome = tokio::time::timeout(
            Duration::from_millis(200),
            consensus.promote_voter(learner_id, Some(node_id)),
        )
        .await;
        if let Ok(Err(ConsensusError::VoterSetFull { .. })) = outcome {
            panic!("paired promote/remove must not be blocked by the voter-count ceiling");
        }
    }
}
