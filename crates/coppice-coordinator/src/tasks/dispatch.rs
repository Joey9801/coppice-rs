//! Dispatch loop (leader-only).
//!
//! Consumes the event stream. On an attempt reaching `Ready` it proposes
//! `DispatchAttempt`, and only after that commits does it send `StartJob` —
//! the commit-before-send ordering of
//! `docs/architecture/command-catalog.md#dispatchattempt`. On a
//! `StopRequested` event it routes a `StopJob`. After any event gap (or on
//! gaining leadership) it resyncs by scanning the latest view for `Ready`
//! attempts and pending aborts — events emitted under a previous leader may
//! predate this task's subscription
//! (`docs/architecture/coordinator-runtime.md`, "Leader transitions").

use std::sync::Arc;

use tokio::sync::watch;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateViews};
use coppice_core::attempt::AttemptState;
use coppice_core::id::{AllocationId, AttemptId, NodeId};
use coppice_proto::pb::agent::v1::AgentCommand;
use coppice_state::command::DispatchAttempt;
use coppice_state::{AllocationRecord, AttemptRecord, Command, Event};

use crate::leadership;
use crate::tasks::agent_gateway::{RouteCommand, RouterHandle};
use crate::tasks::event_fanout::{EventFilter, FanoutHandle, SubscriptionItem};

/// Run the dispatch loop until shutdown.
pub async fn run<C: Consensus>(
    consensus: Arc<C>,
    views: StateViews,
    fanout: FanoutHandle,
    router: RouterHandle,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::info!(term, "dispatch: gained leadership");

        resync(&consensus, &views, &router).await;

        let Ok(mut subscription) = fanout.subscribe(EventFilter::All, None).await else {
            // Fanout is gone; nothing to dispatch from until this replica
            // re-gates (which will hit the same wall, so this is really a
            // shutdown in disguise).
            continue;
        };

        loop {
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => {
                    break;
                }
                item = subscription.items.recv() => {
                    let Some(item) = item else { break };
                    match item {
                        SubscriptionItem::Events(batch) => {
                            for event in &batch.events {
                                handle_event(&consensus, &views, &router, event).await;
                            }
                        }
                        SubscriptionItem::Gap { earliest_available } => {
                            tracing::info!(
                                earliest_available,
                                "dispatch: event stream gap, resyncing from the latest view"
                            );
                            resync(&consensus, &views, &router).await;
                        }
                    }
                }
            }
        }
    }
}

async fn handle_event<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    router: &RouterHandle,
    event: &Event,
) {
    match event {
        Event::AttemptStateChanged { attempt, state: AttemptState::Ready } => {
            dispatch_ready_attempt(consensus, views, router, *attempt).await;
        }
        Event::StopRequested { node, allocation } => {
            route_stop(router, *node, *allocation).await;
        }
        _ => {}
    }
}

/// On gaining leadership, or after any event-stream gap, scan the latest
/// view for `Ready` attempts and pending aborts. At-least-once delivery plus
/// idempotent commands make the rescan safe
/// (`docs/architecture/coordinator-runtime.md`, "Dispatch loop").
async fn resync<C: Consensus>(consensus: &Arc<C>, views: &StateViews, router: &RouterHandle) {
    let view = views.latest();

    let ready_attempts: Vec<AttemptId> = view
        .state()
        .attempts
        .iter()
        .filter(|(_, record)| record.attempt.state == AttemptState::Ready)
        .map(|(id, _)| *id)
        .collect();
    for attempt in ready_attempts {
        dispatch_ready_attempt(consensus, views, router, attempt).await;
    }

    let pending_aborts: Vec<(NodeId, AllocationId)> = view
        .state()
        .jobs
        .values()
        .filter(|job| job.spec.abort_requested.is_some())
        .filter_map(|job| job.current_attempt)
        .filter_map(|attempt_id| view.state().attempts.get(&attempt_id))
        .filter(|record| {
            matches!(record.attempt.state, AttemptState::Dispatching | AttemptState::Running)
        })
        .map(|record| (record.attempt.node, record.attempt.allocation))
        .collect();
    for (node, allocation) in pending_aborts {
        route_stop(router, node, allocation).await;
    }
}

/// Propose `DispatchAttempt`; only once that commits and is accepted does it
/// route `StartJob` to the attempt's node
/// (`docs/architecture/command-catalog.md#dispatchattempt`: commit before
/// send, so a crash in between reconciles as lost, never as an untracked
/// container).
async fn dispatch_ready_attempt<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    router: &RouterHandle,
    attempt: AttemptId,
) {
    let command = Command::DispatchAttempt(DispatchAttempt { attempt, dispatched_at_us: now_us() });
    match consensus.propose(command).await {
        Ok(Applied { outcome: Ok(_), .. }) => {
            let view = views.latest();
            let Some(record) = view.state().attempts.get(&attempt) else { return };
            let Some(allocation) = view.state().allocations.get(&record.attempt.allocation) else {
                return;
            };
            let node = record.attempt.node;
            let agent_command = start_job_command(record, allocation);
            if router.send(RouteCommand { node, command: agent_command }).await.is_err() {
                tracing::warn!(?attempt, "dispatch: router channel closed");
            }
        }
        Ok(Applied { outcome: Err(reason), .. }) => {
            tracing::debug!(?attempt, ?reason, "dispatch: DispatchAttempt rejected");
        }
        Err(e) if e.is_retryable() => {
            tracing::info!(?attempt, error = %e, "dispatch: retryable propose error");
        }
        Err(e) => {
            tracing::error!(?attempt, error = %e, "dispatch: fatal propose error");
        }
    }
}

async fn route_stop(router: &RouterHandle, node: NodeId, allocation: AllocationId) {
    let command = stop_job_command(allocation);
    if router.send(RouteCommand { node, command }).await.is_err() {
        tracing::warn!(?allocation, "dispatch: router channel closed");
    }
}

/// Build the `StartJob` proto command for a freshly-dispatched attempt.
/// Deferred: the payload (image, resource limits, `max_runtime_us`) needs
/// the job/allocation domain -> proto mapping
/// (`docs/architecture/command-catalog.md#dispatchattempt`); the ordering
/// and channel plumbing around this call are real.
fn start_job_command(_attempt: &AttemptRecord, _allocation: &AllocationRecord) -> AgentCommand {
    todo!("StartJob AgentCommand construction (command-catalog.md#dispatchattempt)")
}

/// Build the `StopJob` proto command for an allocation. Deferred: `grace_us`
/// comes from the replicated policy's `abort_grace_us`.
fn stop_job_command(_allocation: AllocationId) -> AgentCommand {
    todo!("StopJob AgentCommand construction (grace_us from replicated policy)")
}

fn now_us() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}
