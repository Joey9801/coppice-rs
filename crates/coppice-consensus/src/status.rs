//! Mapping openraft metrics into the seam's [`ConsensusStatus`] watch.
//!
//! The coordinator's leader-only tasks self-gate on this watch, and follower
//! reads surface their staleness from it (`docs/architecture/coordinator-runtime.md`,
//! ADR 0007). This module folds openraft's [`RaftMetrics`] plus the storage
//! layer's committed-index watch into the openraft-free [`ConsensusStatus`],
//! so no openraft type crosses the seam.

use tokio::sync::watch;

use crate::membership::CoordinatorNode;
use openraft::{RaftMetrics, ServerState};

use crate::{ConsensusStatus, CoordinatorId, Role};

/// Fold one metrics sample and committed index into a [`ConsensusStatus`].
fn compute(
    metrics: &RaftMetrics<CoordinatorId, CoordinatorNode>,
    committed: u64,
) -> ConsensusStatus {
    let role = match metrics.state {
        ServerState::Leader => Role::Leader {
            term: metrics.current_term,
        },
        // A candidate has no usable leader to report; surface it as Unknown so
        // callers wait out the election rather than trusting a stale leader.
        ServerState::Candidate => Role::Unknown,
        ServerState::Follower | ServerState::Learner => Role::Follower {
            leader: metrics.current_leader,
        },
        ServerState::Shutdown => Role::Unknown,
    };
    let last_applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);
    // `save_committed` can briefly trail applied during startup (openraft
    // applies from the snapshot index before persisting a committed marker),
    // so never report a committed frontier behind what is already applied.
    let known_committed = committed.max(last_applied);
    ConsensusStatus {
        id: metrics.id,
        role,
        last_applied,
        known_committed,
    }
}

/// Spawn the mapping task and return the resulting status watch.
///
/// The output watch is seeded with a correct initial value before returning,
/// so a reader that borrows immediately sees real state. The task recomputes on
/// every change to either input and publishes only when the value actually
/// changed (a term bump while staying leader *does* change it, so leadership
/// tasks that key off the term still wake). It exits when either input watch
/// closes — i.e. at openraft shutdown or when the log store is dropped.
pub(crate) fn spawn(
    mut metrics_rx: watch::Receiver<RaftMetrics<CoordinatorId, CoordinatorNode>>,
    mut committed_rx: watch::Receiver<u64>,
) -> watch::Receiver<ConsensusStatus> {
    let initial = compute(&metrics_rx.borrow(), *committed_rx.borrow());
    let (tx, rx) = watch::channel(initial);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = metrics_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = committed_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
            let next = compute(&metrics_rx.borrow(), *committed_rx.borrow());
            // `ConsensusStatus: Eq` — only publish (and wake readers) on a real
            // change, but recompute on every input tick so nothing is missed.
            tx.send_if_modified(|current| {
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            });
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership, Vote};

    use super::*;

    fn metrics(
        state: ServerState,
        term: u64,
        applied: Option<u64>,
    ) -> RaftMetrics<CoordinatorId, CoordinatorNode> {
        RaftMetrics {
            running_state: Ok(()),
            id: 1,
            current_term: term,
            vote: Vote::new(term, 1),
            last_log_index: applied,
            last_applied: applied.map(|index| LogId {
                leader_id: CommittedLeaderId::new(term, 1),
                index,
            }),
            snapshot: None,
            purged: None,
            state,
            current_leader: Some(1),
            millis_since_quorum_ack: None,
            membership_config: Arc::new(StoredMembership::new(
                None,
                Membership::new(vec![BTreeSet::from([1])], None),
            )),
            replication: None,
        }
    }

    #[test]
    fn leader_reports_term_and_committed_floor() {
        let m = metrics(ServerState::Leader, 4, Some(10));
        let status = compute(&m, 8);
        assert_eq!(status.role, Role::Leader { term: 4 });
        assert_eq!(status.last_applied, 10);
        // committed watch trailed applied; the floor is the applied index.
        assert_eq!(status.known_committed, 10);

        let status = compute(&m, 12);
        assert_eq!(status.known_committed, 12);
    }

    #[test]
    fn candidate_is_unknown_follower_reports_leader() {
        assert_eq!(
            compute(&metrics(ServerState::Candidate, 4, Some(1)), 1).role,
            Role::Unknown
        );
        assert_eq!(
            compute(&metrics(ServerState::Follower, 4, Some(1)), 1).role,
            Role::Follower { leader: Some(1) }
        );
    }

    #[tokio::test]
    async fn a_term_change_while_leader_publishes() {
        let (m_tx, m_rx) = watch::channel(metrics(ServerState::Leader, 4, Some(5)));
        let (_c_tx, c_rx) = watch::channel(5u64);
        let mut status = spawn(m_rx, c_rx);
        assert_eq!(status.borrow_and_update().role, Role::Leader { term: 4 });

        m_tx.send(metrics(ServerState::Leader, 5, Some(5))).unwrap();
        status.changed().await.unwrap();
        assert_eq!(status.borrow().role, Role::Leader { term: 5 });
    }
}
