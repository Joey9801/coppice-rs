//! Test-only fakes shared across coordinator unit tests.

use std::sync::Mutex;

use tokio::sync::watch;

use coppice_consensus::{
    Applied, Consensus, ConsensusError, ConsensusStatus, CoordinatorId, Role, StateViews,
    ViewPublisher, ViewPublisherConfig,
};
use coppice_state::{Command, RejectionReason, StateMachine};

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
    status_rx: watch::Receiver<ConsensusStatus>,
    views: StateViews,
    next_log_index: Mutex<u64>,
}

impl FakeConsensus {
    /// Build a fake reporting `Leader { term: 1 }`.
    ///
    /// Also returns the [`ViewPublisher`] half the test uses to seed/advance published state.
    pub fn new(outcome: ProposeOutcome) -> (Self, ViewPublisher) {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), ViewPublisherConfig::default());
        let (_status_tx, status_rx) = watch::channel(ConsensusStatus {
            id: 1,
            role: Role::Leader { term: 1 },
            last_applied: 0,
            known_committed: 0,
        });
        let consensus = FakeConsensus {
            outcome: Mutex::new(outcome),
            status_rx,
            views,
            next_log_index: Mutex::new(1),
        };
        (consensus, publisher)
    }
}

impl Consensus for FakeConsensus {
    async fn propose(&self, _command: Command) -> Result<Applied, ConsensusError> {
        let mut next_log_index = self.next_log_index.lock().unwrap();
        let log_index = *next_log_index;
        *next_log_index += 1;
        match &*self.outcome.lock().unwrap() {
            ProposeOutcome::Accepted => {
                Ok(Applied { log_index, outcome: Ok(coppice_state::Applied::default()) })
            }
            ProposeOutcome::Rejected(reason) => Ok(Applied { log_index, outcome: Err(reason.clone()) }),
            ProposeOutcome::NotLeader(leader) => Err(ConsensusError::NotLeader { leader: *leader }),
        }
    }

    async fn read_index(&self) -> Result<u64, ConsensusError> {
        Ok(0)
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
