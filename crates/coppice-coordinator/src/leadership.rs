//! Self-gating helper for leader-only tasks.
//!
//! Per `docs/architecture/coordinator-runtime.md` ("Leader transitions"),
//! leader-only tasks are never killed by a supervisor: each waits on the
//! status watch, runs its body, and stops the moment it observes leadership
//! lost or shutdown — always at an `.await` point it chose, never
//! mid-invariant. [`wait_for_leadership`] and [`until_leadership_lost`] are
//! the two arms of that pattern; every leader-only task in `tasks/` is built
//! as `loop { wait_for_leadership(...); select! { until_leadership_lost(...) ,
//! <task work> } }`.

use tokio::sync::watch;

use coppice_consensus::{ConsensusStatus, Role};

/// Wait until this replica becomes leader.
///
/// Returns the term once it does. Returns `None` if shutdown flips, or if
/// the status watch closes (the consensus seam is gone), before that
/// happens.
pub async fn wait_for_leadership(
    status: &mut watch::Receiver<ConsensusStatus>,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<u64> {
    loop {
        if *shutdown.borrow() {
            return None;
        }
        if let Role::Leader { term } = status.borrow_and_update().role {
            return Some(term);
        }
        tokio::select! {
            result = status.changed() => {
                if result.is_err() {
                    return None;
                }
            }
            result = shutdown.changed() => {
                if result.is_err() {
                    return None;
                }
            }
        }
    }
}

/// Resolve the moment this replica's status no longer reports
/// `Leader { term }` for the given `term` (a different term, or a
/// non-leader role), or shutdown flips.
///
/// A leader-only task selects its work against this future to know when to
/// stop draining and re-gate on [`wait_for_leadership`].
pub async fn until_leadership_lost(
    status: &mut watch::Receiver<ConsensusStatus>,
    term: u64,
    shutdown: &mut watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let still_leading = match status.borrow().role {
            Role::Leader { term: current } => current == term,
            _ => false,
        };
        if !still_leading {
            return;
        }
        tokio::select! {
            result = status.changed() => {
                if result.is_err() {
                    return;
                }
            }
            result = shutdown.changed() => {
                if result.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn status(role: Role) -> ConsensusStatus {
        ConsensusStatus { id: 1, role, last_applied: 0, known_committed: 0 }
    }

    #[tokio::test]
    async fn wait_for_leadership_wakes_on_role_change() {
        let (status_tx, mut status_rx) = watch::channel(status(Role::Follower { leader: None }));
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let waiter =
            tokio::spawn(async move { wait_for_leadership(&mut status_rx, &mut shutdown_rx).await });

        tokio::task::yield_now().await;
        status_tx.send(status(Role::Leader { term: 3 })).unwrap();

        let term = waiter.await.expect("join").expect("leadership");
        assert_eq!(term, 3);
    }

    #[tokio::test]
    async fn wait_for_leadership_stops_on_shutdown() {
        let (_status_tx, mut status_rx) = watch::channel(status(Role::Follower { leader: None }));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let waiter =
            tokio::spawn(async move { wait_for_leadership(&mut status_rx, &mut shutdown_rx).await });

        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();

        let result = waiter.await.expect("join");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn until_leadership_lost_resolves_on_role_change() {
        let (status_tx, mut status_rx) = watch::channel(status(Role::Leader { term: 5 }));
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let waiter = tokio::spawn(async move {
            until_leadership_lost(&mut status_rx, 5, &mut shutdown_rx).await;
        });

        tokio::task::yield_now().await;
        status_tx.send(status(Role::Follower { leader: Some(9) })).unwrap();

        tokio::time::timeout(Duration::from_secs(1), waiter).await.expect("timed out").expect("join");
    }

    #[tokio::test]
    async fn until_leadership_lost_ignores_a_different_term() {
        let (status_tx, mut status_rx) = watch::channel(status(Role::Leader { term: 5 }));
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let waiter = tokio::spawn(async move {
            until_leadership_lost(&mut status_rx, 5, &mut shutdown_rx).await;
        });

        tokio::task::yield_now().await;
        // Same replica, still leader, but a *newer* term than the one we're
        // watching for still counts as "lost" for the caller's purposes.
        status_tx.send(status(Role::Leader { term: 6 })).unwrap();

        tokio::time::timeout(Duration::from_secs(1), waiter).await.expect("timed out").expect("join");
    }
}
