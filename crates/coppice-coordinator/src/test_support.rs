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
        },
        epoch,
    }
}

/// An attempt record in `state` on `node`, with an optional `started_at_us`.
pub fn attempt_record(
    id: AttemptId,
    job: JobId,
    allocation: AllocationId,
    node: NodeId,
    state: AttemptState,
    started_at_us: Option<i64>,
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
            charged_at_us: 0,
            refund_fraction_milli: FULL_REFUND_MILLI,
        },
        rate_ucu_per_second: 0,
        multiplier: PriorityMultiplier(0),
        started_at_us,
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
/// job machine (ADR 0029) a live state carries the attempt it points at, and
/// callers of this fixture build their job and attempt records independently
/// (there is no `attempt_id` parameter here to keep them coherent). `Queued`
/// is the honest choice for a record with no attempt of its own; tests that
/// need a live job override `.state` with a real `Attempting(id)` alongside a
/// matching attempt record.
pub fn job_record(
    id: JobId,
    image: &str,
    requests: Resources,
    max_runtime_us: Option<u64>,
) -> JobRecord {
    JobRecord {
        spec: Job {
            id,
            image: image.to_string(),
            command: vec!["run".into()],
            entrypoint: None,
            requests,
            priority: 0,
            max_runtime_us,
            quota_entity: QuotaEntityId::new(),
            retry: RetryPolicy::default(),
            abort_requested: None,
        },
        state: JobState::Queued,
        multiplier: PriorityMultiplier(0),
        submitted_at_us: 0,
        terminal_at_us: None,
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

/// A [`Consensus`] fake: `propose` returns a canned outcome instead of running real Raft.
///
/// `status`/`views` are backed by a real [`ViewPublisher`]/[`StateViews`] pair
/// so callers see the genuine seam behavior for reads.
pub struct FakeConsensus {
    outcome: Mutex<ProposeOutcome>,
    // Retained so the status watch stays open for the lifetime of the fake:
    // the leader-only loops (`leadership::until_leadership_lost`) treat a
    // closed status watch as "leadership lost", so a dropped sender would end
    // a drain loop before it processed anything.
    _status_tx: watch::Sender<ConsensusStatus>,
    status_rx: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    next_log_index: Mutex<u64>,
    read_index: Mutex<u64>,
}

impl FakeConsensus {
    /// Build a fake reporting `Leader { term: 1 }`.
    ///
    /// Also returns the [`ViewPublisher`] half the test uses to seed/advance published state.
    pub fn new(outcome: ProposeOutcome) -> (Self, ViewPublisher) {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (status_tx, status_rx) = watch::channel(ConsensusStatus {
            id: 1,
            role: Role::Leader { term: 1 },
            last_applied: 0,
            known_committed: 0,
        });
        let consensus = FakeConsensus {
            outcome: Mutex::new(outcome),
            _status_tx: status_tx,
            status_rx,
            views,
            next_log_index: Mutex::new(1),
            read_index: Mutex::new(0),
        };
        (consensus, publisher)
    }

    /// Pin the barrier [`Consensus::read_index`] returns (defaults to 0).
    ///
    /// Lets a test hold the linearizable read barrier *ahead* of what the
    /// publisher has published, to exercise strong-read gating.
    pub fn set_read_index(&self, index: u64) {
        *self.read_index.lock().unwrap() = index;
    }
}

impl Consensus for FakeConsensus {
    async fn propose(&self, _command: Command) -> Result<Applied, ConsensusError> {
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
        Ok(*self.read_index.lock().unwrap())
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

    async fn promote_voter(
        &self,
        _promote: CoordinatorId,
        _remove: Option<CoordinatorId>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    async fn remove_node(&self, _node: CoordinatorId) -> Result<(), ConsensusError> {
        Ok(())
    }

    async fn trigger_snapshot(&self) -> Result<(), ConsensusError> {
        Ok(())
    }
}
