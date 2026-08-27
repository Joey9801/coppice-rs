//! Test-only fakes and record builders shared across coordinator unit tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::watch;

use coppice_consensus::{
    Applied, Consensus, ConsensusError, ConsensusStatus, CoordinatorId, Role, StateView,
    StateViews, ViewPublisher, ViewPublisherConfig,
};
use coppice_core::allocation::{Allocation, AllocationState};
use coppice_core::attempt::{Attempt, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, JobState, RetryPolicy};
use coppice_core::node::Node;
use coppice_core::quota::{ChargeRecord, CostUnits, PriorityMultiplier, FULL_REFUND_MILLI};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::{
    AllocationRecord, AttemptRecord, Command, JobRecord, NodeRecord, RejectionReason, StateMachine,
};

/// A read view over a hand-built state machine (published at index 1).
pub fn view_of(state: StateMachine) -> StateView {
    let (_publisher, views) = ViewPublisher::new(state, 1, ViewPublisherConfig::default());
    views.latest()
}

/// A node record at `epoch`, with the given schedulability and no labels.
pub fn node_record(id: NodeId, epoch: u64, schedulable: bool) -> NodeRecord {
    NodeRecord {
        node: Node {
            id,
            capacity: Resources::ZERO,
            labels: BTreeMap::new(),
            schedulable,
            service_addr: None,
        },
        epoch,
    }
}

/// An attempt record in `state` on `node`, with an optional `started_at`.
pub fn attempt_record(
    id: AttemptId,
    job: JobId,
    allocation: AllocationId,
    node: NodeId,
    state: AttemptState,
    started_at: Option<Timestamp>,
) -> AttemptRecord {
    AttemptRecord {
        attempt: Attempt {
            id,
            job,
            allocation,
            node,
            state,
        },
        group: GroupId(job.0),
        charge: ChargeRecord {
            amount: CostUnits(0),
            charged_at: Timestamp::UNIX_EPOCH,
            refund_fraction_milli: FULL_REFUND_MILLI,
        },
        rate_ucu_per_second: 0,
        multiplier: PriorityMultiplier(0),
        started_at,
    }
}

/// An allocation record in `state` on `node`, requesting `requested`.
pub fn allocation_record(
    id: AllocationId,
    job: JobId,
    attempt: AttemptId,
    node: NodeId,
    requested: Resources,
    state: AllocationState,
) -> AllocationRecord {
    AllocationRecord {
        allocation: Allocation {
            id,
            job,
            attempt,
            node,
            requested,
            funded: Resources::ZERO,
            state,
        },
        seq: 1,
    }
}

/// A `Queued` job record with the given spec fields.
///
/// `Queued` rather than a live `Attempting(id)` state: under the collapsed
/// job machine (ADR 0030) a live state carries the attempt it points at, and
/// callers of this fixture build their job and attempt records independently
/// (there is no `attempt_id` parameter here to keep them coherent). `Queued`
/// is the honest choice for a record with no attempt of its own; tests that
/// need a live job override `.state` with a real `Attempting(id)` alongside a
/// matching attempt record.
pub fn job_record(
    id: JobId,
    image: &str,
    requests: Resources,
    max_runtime: Option<Duration>,
) -> JobRecord {
    JobRecord {
        spec: Job {
            id,
            image: image.to_string(),
            command: vec!["run".into()],
            entrypoint: None,
            requests,
            priority: 0,
            max_runtime,
            quota_entity: QuotaEntityId::new(),
            retry: RetryPolicy::default(),
            abort_requested: None,
        },
        state: JobState::Queued,
        multiplier: PriorityMultiplier(0),
        submitted_at: Timestamp::UNIX_EPOCH,
        terminal_at: None,
        retries_used: 0,
        attempts: Vec::new(),
    }
}

/// The canned outcome [`FakeConsensus::propose`] returns.
pub enum ProposeOutcome {
    Accepted,
    Rejected(RejectionReason),
    NotLeader(Option<CoordinatorId>),
}

/// The canned outcome [`FakeConsensus::read_index`] returns.
///
/// Separate from [`ProposeOutcome`] and from the published status because the
/// ADR 0038 write path exists to survive them *disagreeing*: the barrier is
/// what tells a replica whose status watch still says "leader" that it is
/// not. A fake that derived all three from one knob could not express the
/// race the barrier was added for.
pub enum ReadIndexOutcome {
    /// A barrier at this index — the default, at index 0.
    At(u64),
    /// The barrier refused: this replica does not lead after all.
    NotLeader(Option<CoordinatorId>),
    /// The barrier could not be established (a stand-in for every non-leader
    /// failure: timeout, shutdown, …).
    Timeout,
}

/// A [`Consensus`] fake: `propose` returns a canned outcome instead of running real Raft.
///
/// `status`/`views` are backed by a real [`ViewPublisher`]/[`StateViews`] pair
/// so callers see the genuine seam behavior for reads.
pub struct FakeConsensus {
    outcome: Mutex<ProposeOutcome>,
    /// Every command `propose` was handed, in order — including the ones the
    /// canned outcome then rejected, since "what did this loop decide to
    /// propose" is a different question from "what did consensus accept".
    proposed: Mutex<Vec<Command>>,
    // Retained so the status watch stays open for the lifetime of the fake:
    // the leader-only loops (`leadership::until_leadership_lost`) treat a
    // closed status watch as "leadership lost", so a dropped sender would end
    // a drain loop before it processed anything.
    _status_tx: watch::Sender<ConsensusStatus>,
    status_rx: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    next_log_index: Mutex<u64>,
    read_index: Mutex<ReadIndexOutcome>,
}

impl FakeConsensus {
    /// Build a fake whose published role agrees with what `propose` will say.
    ///
    /// The two are not independent on a real replica, and code that reads
    /// leadership from the status watch *before* proposing — the ADR 0038
    /// write gate does — would be tested against an impossible replica if
    /// they disagreed here. So a fake that refuses proposals with `NotLeader`
    /// reports itself a follower of the same leader; anything else reports
    /// `Leader { term: 1 }`.
    ///
    /// Also returns the [`ViewPublisher`] half the test uses to seed/advance published state.
    pub fn new(outcome: ProposeOutcome) -> (Self, ViewPublisher) {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let role = match &outcome {
            ProposeOutcome::NotLeader(leader) => Role::Follower { leader: *leader },
            ProposeOutcome::Accepted | ProposeOutcome::Rejected(_) => Role::Leader { term: 1 },
        };
        let (status_tx, status_rx) = watch::channel(ConsensusStatus {
            id: 1,
            role,
            last_applied: 0,
            known_committed: 0,
        });
        let consensus = FakeConsensus {
            outcome: Mutex::new(outcome),
            proposed: Mutex::new(Vec::new()),
            _status_tx: status_tx,
            status_rx,
            views,
            next_log_index: Mutex::new(1),
            read_index: Mutex::new(ReadIndexOutcome::At(0)),
        };
        (consensus, publisher)
    }

    /// The commands proposed so far, oldest first.
    ///
    /// A snapshot rather than a guard: a test asserting on what a background
    /// loop proposed must not hold the lock the loop's next `propose` needs.
    pub fn proposed(&self) -> Vec<Command> {
        self.proposed.lock().unwrap().clone()
    }

    /// Pin the barrier [`Consensus::read_index`] returns (defaults to 0).
    ///
    /// Lets a test hold the linearizable read barrier *ahead* of what the
    /// publisher has published, to exercise strong-read gating.
    pub fn set_read_index(&self, index: u64) {
        self.set_read_index_outcome(ReadIndexOutcome::At(index));
    }

    /// Make the barrier answer something other than an index.
    ///
    /// The interesting case is [`ReadIndexOutcome::NotLeader`] while the
    /// status watch still reports `Leader`: the replica that *was* the leader
    /// when the watch was last published and is not one now.
    pub fn set_read_index_outcome(&self, outcome: ReadIndexOutcome) {
        *self.read_index.lock().unwrap() = outcome;
    }
}

impl Consensus for FakeConsensus {
    async fn propose(&self, command: Command) -> Result<Applied, ConsensusError> {
        self.proposed.lock().unwrap().push(command);
        let mut next_log_index = self.next_log_index.lock().unwrap();
        let log_index = *next_log_index;
        *next_log_index += 1;
        match &*self.outcome.lock().unwrap() {
            ProposeOutcome::Accepted => Ok(Applied {
                log_index,
                outcome: Ok(coppice_state::Applied::default()),
            }),
            ProposeOutcome::Rejected(reason) => Ok(Applied {
                log_index,
                outcome: Err(reason.clone()),
            }),
            ProposeOutcome::NotLeader(leader) => Err(ConsensusError::NotLeader { leader: *leader }),
        }
    }

    async fn read_index(&self) -> Result<u64, ConsensusError> {
        match &*self.read_index.lock().unwrap() {
            ReadIndexOutcome::At(index) => Ok(*index),
            ReadIndexOutcome::NotLeader(leader) => {
                Err(ConsensusError::NotLeader { leader: *leader })
            }
            ReadIndexOutcome::Timeout => Err(ConsensusError::Timeout),
        }
    }

    fn status(&self) -> watch::Receiver<ConsensusStatus> {
        self.status_rx.clone()
    }

    fn views(&self) -> StateViews {
        self.views.clone()
    }

    async fn add_learner(&self, _node: CoordinatorId, _addr: String) -> Result<(), ConsensusError> {
        Ok(())
    }

    /// Always "ready, nothing to fold out": this fake holds no membership,
    /// so there is no ceiling to hit and no contact evidence to consult.
    fn plan_promotion(
        &self,
        _promote: CoordinatorId,
    ) -> Result<coppice_consensus::PromotionPlan, ConsensusError> {
        Ok(coppice_consensus::PromotionPlan::Ready {
            evidence_removal: None,
        })
    }

    async fn commit_promotion(
        &self,
        _promote: CoordinatorId,
        _remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    /// Mirrors [`plan_promotion`](Self::plan_promotion): no membership, so
    /// every replacement plans clean.
    fn plan_replacement(
        &self,
        _old: CoordinatorId,
        _new: CoordinatorId,
    ) -> Result<coppice_consensus::ReplacementPlan, ConsensusError> {
        Ok(coppice_consensus::ReplacementPlan::Ready)
    }

    async fn replace_voter(
        &self,
        _old: CoordinatorId,
        _new: CoordinatorId,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn learner_expiry(&self) -> std::time::Duration {
        std::time::Duration::from_secs(3600)
    }

    /// Never anything to reap: contact evidence lives in the real seam, and a
    /// fake that invented some would be testing itself.
    fn expired_learners(&self) -> Vec<CoordinatorId> {
        Vec::new()
    }

    /// Nothing is ever eligible, for the same reason as
    /// [`expired_learners`](Self::expired_learners).
    async fn reap_expired_learner(
        &self,
        _node: CoordinatorId,
        _retire: Option<Command>,
    ) -> Result<bool, ConsensusError> {
        Ok(false)
    }

    async fn remove_node(&self, _node: CoordinatorId) -> Result<(), ConsensusError> {
        Ok(())
    }

    /// A no-op like the other membership verbs: no test in this crate drives
    /// the operator-only break-glass repoint (ADR 0037 §6), and a fake that
    /// held membership state would be a second implementation of it.
    async fn set_node_address(
        &self,
        _node: CoordinatorId,
        _addr: String,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    async fn trigger_snapshot(&self) -> Result<(), ConsensusError> {
        Ok(())
    }
}
