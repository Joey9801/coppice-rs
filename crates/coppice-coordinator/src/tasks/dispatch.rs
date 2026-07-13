//! Dispatch loop (leader-only).
//!
//! Consumes the event stream. On an attempt reaching `Ready` it proposes
//! `DispatchAttempt`, and only after that commits does it send `StartJob` —
//! the commit-before-send ordering of
//! `docs/architecture/command-catalog.md#dispatchattempt`. On a
//! `StopRequested` event it routes a `StopJob`. On gaining leadership (after
//! subscribing) and after any event gap it resyncs by scanning a *strong*
//! view for `Ready` attempts and pending aborts — events emitted under a
//! previous leader may predate this task's subscription
//! (`docs/architecture/coordinator-runtime.md`, "Leader transitions").
//!
//! Subscribe-then-resync ordering is load-bearing: every `Ready` attempt is
//! either applied before the subscription registered — committed before
//! `resync`'s `read_index` barrier, so its strong view shows it — or applied
//! after, in which case the live subscription delivers it. Resyncing before
//! subscribing leaves a window (barrier taken, subscription not yet
//! registered) where an attempt appears in neither, and nothing re-emits
//! `Ready`: the job would wedge exactly like the stale-view bug in
//! `dispatch_ready_attempt`'s freshness gate below.

use std::sync::Arc;

use tokio::sync::watch;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateViews};
use coppice_core::attempt::AttemptState;
use coppice_core::id::{AllocationId, AttemptId, NodeId};
use coppice_proto::pb::agent::v1::{
    agent_command, AgentCommand, RegisterAccepted, StartJob, StopJob,
};
use coppice_state::command::DispatchAttempt;
use coppice_state::{AllocationRecord, AttemptRecord, Command, Event, JobRecord};

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

        // Subscribe BEFORE resyncing (module doc): the live subscription
        // covers everything applied after it registers, the strong resync
        // covers everything committed before its barrier, and this order is
        // what makes those two ranges overlap instead of leaving a gap.
        let Ok(mut subscription) = fanout.subscribe(EventFilter::All, None).await else {
            // Fanout is gone; nothing to dispatch from until this replica
            // re-gates (which will hit the same wall, so this is really a
            // shutdown in disguise).
            continue;
        };

        resync(&consensus, &views, &router).await;

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
                                "dispatch: event stream gap, resyncing from a strong view"
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
        Event::AttemptStateChanged {
            attempt,
            state: AttemptState::Ready,
            ..
        } => {
            dispatch_ready_attempt(consensus, views, router, *attempt).await;
        }
        Event::StopRequested {
            node, allocation, ..
        } => {
            route_stop(router, views, *node, *allocation).await;
        }
        _ => {}
    }
}

/// On gaining leadership or after any event-stream gap, rescan a strong view.
///
/// Scans for `Ready` attempts and pending aborts. At-least-once delivery plus
/// idempotent commands make the rescan safe. See
/// `docs/architecture/coordinator-runtime.md` ("Dispatch loop").
///
/// The scan must be a strong read, not `views.latest()`: the event tap emits
/// on every applied command while view publishing is cadence-gated, so the events
/// this resync stands in for (missed before subscribing, or dropped in a gap)
/// are routinely *ahead* of the latest published view. A stale scan misses a
/// `Ready` attempt that no future event re-emits — the same silent wedge as
/// the freshness gate in `dispatch_ready_attempt`. Every missed event was
/// applied before this function was called, so it is committed at or below
/// the `read_index` barrier and visible in the `at_least` view.
async fn resync<C: Consensus>(consensus: &Arc<C>, views: &StateViews, router: &RouterHandle) {
    let index = match consensus.read_index().await {
        Ok(index) => index,
        Err(e) => {
            // Leadership is moving or the replica is shutting down; the
            // leadership gate in `run` re-runs resync on regain.
            tracing::info!(error = %e, "dispatch: read_index failed during resync, skipping");
            return;
        }
    };
    let Ok(view) = views.at_least(index).await else {
        // The apply task is gone; this replica is shutting down.
        return;
    };

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
            matches!(
                record.attempt.state,
                AttemptState::Dispatching | AttemptState::Running
            )
        })
        .map(|record| (record.attempt.node, record.attempt.allocation))
        .collect();
    for (node, allocation) in pending_aborts {
        route_stop(router, views, node, allocation).await;
    }
}

/// Propose `DispatchAttempt`; only then route `StartJob` to the attempt's node.
///
/// Commit-before-send ordering (`docs/architecture/command-catalog.md#dispatchattempt`):
/// a crash in between reconciles as lost, never as an untracked container.
async fn dispatch_ready_attempt<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    router: &RouterHandle,
    attempt: AttemptId,
) {
    let command = Command::DispatchAttempt(DispatchAttempt {
        attempt,
        dispatched_at_us: now_us(),
    });
    match consensus.propose(command).await {
        Ok(Applied {
            outcome: Ok(_),
            log_index,
        }) => {
            // Freshness gate. `propose` returning means the command applied to
            // the state machine, but `views` publishes snapshots on its own
            // cadence, so `latest()` can still sit *behind* `log_index` — and
            // therefore behind the `CommitPlacements` that created this
            // attempt and its allocation. Reading a stale snapshot made the
            // two lookups below miss and silently drop the `StartJob`: the
            // attempt is already `Dispatching`, so it never re-emits `Ready`,
            // and `resync` only rescans `Ready` attempts, so nothing ever
            // heals it and the job hangs forever. Wait for the view to reflect
            // this commit, exactly as the scheduler driver does after its own
            // propose.
            let view = match views.at_least(log_index).await {
                Ok(view) => view,
                // The apply task is gone; this replica is shutting down.
                Err(_) => return,
            };
            let Some(record) = view.state().attempts.get(&attempt) else {
                // Unreachable once the view reflects `log_index` — the attempt
                // is precisely what `DispatchAttempt` just transitioned. Log
                // rather than vanish, so a regression surfaces as a warning
                // instead of a job wedged in `Dispatching`.
                tracing::warn!(
                    ?attempt,
                    "dispatch: attempt record missing, skipping StartJob"
                );
                return;
            };
            let Some(allocation) = view.state().allocations.get(&record.attempt.allocation) else {
                tracing::warn!(
                    ?attempt,
                    allocation = ?record.attempt.allocation,
                    "dispatch: allocation record missing, skipping StartJob"
                );
                return;
            };
            let Some(job) = view.state().jobs.get(&record.attempt.job) else {
                // Racing eviction: the job vanished before we could build its
                // StartJob. Reconciliation heals any container that slips out.
                tracing::warn!(?attempt, "dispatch: job record missing, skipping StartJob");
                return;
            };
            let node = record.attempt.node;
            let agent_command = start_job_command(job, record, allocation);
            if router
                .send(RouteCommand {
                    node,
                    command: agent_command,
                })
                .await
                .is_err()
            {
                tracing::warn!(?attempt, "dispatch: router channel closed");
            }
        }
        Ok(Applied {
            outcome: Err(reason),
            ..
        }) => {
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

async fn route_stop(
    router: &RouterHandle,
    views: &StateViews,
    node: NodeId,
    allocation: AllocationId,
) {
    let grace_us = views.latest().state().policy.abort_grace_us;
    let command = stop_job_command(allocation, grace_us);
    if router.send(RouteCommand { node, command }).await.is_err() {
        tracing::warn!(?allocation, "dispatch: router channel closed");
    }
}

/// Build the header-less `StartJob` command for a freshly-dispatched attempt.
///
/// `header: None` — the session manager stamps the fencing token as it routes
/// (`docs/architecture/command-catalog.md#dispatchattempt`). The `limits` are
/// the allocation's requested vector; `max_runtime_us` rides straight from the
/// job spec (absent = unbounded).
pub(crate) fn start_job_command(
    job: &JobRecord,
    attempt: &AttemptRecord,
    allocation: &AllocationRecord,
) -> AgentCommand {
    AgentCommand {
        header: None,
        body: Some(agent_command::Body::StartJob(StartJob {
            allocation: Some(allocation.allocation.id.into()),
            attempt: Some(attempt.attempt.id.into()),
            job: Some(job.spec.id.into()),
            image: job.spec.image.clone(),
            command: job.spec.command.clone(),
            entrypoint: job
                .spec
                .entrypoint
                .as_ref()
                .map(|argv| coppice_proto::pb::core::v1::Entrypoint { argv: argv.clone() }),
            limits: Some((&allocation.allocation.requested).into()),
            max_runtime_us: job.spec.max_runtime_us,
        })),
    }
}

/// Build the header-less `StopJob` command for an allocation.
///
/// `grace_us` is the replicated policy's `abort_grace_us` at the call site;
/// `header: None` — the manager stamps the token as it routes.
pub(crate) fn stop_job_command(allocation: AllocationId, grace_us: i64) -> AgentCommand {
    AgentCommand {
        header: None,
        body: Some(agent_command::Body::StopJob(StopJob {
            allocation: Some(allocation.into()),
            grace_us,
        })),
    }
}

/// Build the header-less `RegisterAccepted` command (ADR 0009 step 2).
///
/// The body is empty; the fresh fencing token rides in the header the manager
/// stamps, which is exactly how the agent adopts its new epoch before
/// reporting its ObservedSet.
pub(crate) fn register_accepted_command() -> AgentCommand {
    AgentCommand {
        header: None,
        body: Some(agent_command::Body::RegisterAccepted(RegisterAccepted {})),
    }
}

fn now_us() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::allocation::AllocationState;
    use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
    use coppice_core::resource::Resources;

    use crate::test_support::{allocation_record, attempt_record, job_record};

    fn requested() -> Resources {
        Resources {
            cpu_millis: 500,
            memory_bytes: 1 << 20,
            disk_bytes: 0,
        }
    }

    #[test]
    fn start_job_command_maps_every_field() {
        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();

        let job = job_record(job_id, "registry/img:1", requested(), Some(1_234));
        let attempt = attempt_record(
            attempt_id,
            job_id,
            alloc_id,
            node,
            AttemptState::Ready,
            None,
        );
        let allocation = allocation_record(
            alloc_id,
            job_id,
            attempt_id,
            node,
            requested(),
            AllocationState::Funded,
        );

        let command = start_job_command(&job, &attempt, &allocation);
        // The manager stamps the header on the way out.
        assert!(command.header.is_none());
        let agent_command::Body::StartJob(sj) = command.body.expect("body") else {
            panic!("expected StartJob");
        };
        assert_eq!(
            AllocationId::try_from(sj.allocation.unwrap()).unwrap(),
            alloc_id
        );
        assert_eq!(
            AttemptId::try_from(sj.attempt.unwrap()).unwrap(),
            attempt_id
        );
        assert_eq!(JobId::try_from(sj.job.unwrap()).unwrap(), job_id);
        assert_eq!(sj.image, "registry/img:1");
        assert_eq!(
            Resources::try_from(sj.limits.unwrap()).unwrap(),
            requested()
        );
        assert_eq!(sj.max_runtime_us, Some(1_234));
    }

    #[test]
    fn start_job_command_carries_absent_max_runtime() {
        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();
        let job = job_record(job_id, "img", requested(), None);
        let attempt = attempt_record(
            attempt_id,
            job_id,
            alloc_id,
            node,
            AttemptState::Ready,
            None,
        );
        let allocation = allocation_record(
            alloc_id,
            job_id,
            attempt_id,
            node,
            requested(),
            AllocationState::Funded,
        );

        let command = start_job_command(&job, &attempt, &allocation);
        let agent_command::Body::StartJob(sj) = command.body.expect("body") else {
            panic!("expected StartJob");
        };
        assert_eq!(sj.max_runtime_us, None);
    }

    #[test]
    fn stop_job_command_carries_allocation_and_grace() {
        let alloc_id = AllocationId::new();
        let command = stop_job_command(alloc_id, 30_000_000);
        assert!(command.header.is_none());
        let agent_command::Body::StopJob(sj) = command.body.expect("body") else {
            panic!("expected StopJob");
        };
        assert_eq!(
            AllocationId::try_from(sj.allocation.unwrap()).unwrap(),
            alloc_id
        );
        assert_eq!(sj.grace_us, 30_000_000);
    }

    #[test]
    fn register_accepted_command_is_an_empty_body() {
        let command = register_accepted_command();
        assert!(command.header.is_none());
        assert!(matches!(
            command.body,
            Some(agent_command::Body::RegisterAccepted(_))
        ));
    }

    /// The freshness gate, deterministically.
    ///
    /// `propose` commits at log index 1 while the publisher still sits at index
    /// 0, whose state holds none of the records `StartJob` is assembled from —
    /// the exact ordering that wedged a job in `Dispatching`. A `views.latest()`
    /// read would miss all three lookups and drop the `StartJob` silently; the
    /// `at_least(log_index)` gate must park until the publish lands, then route.
    #[tokio::test]
    async fn dispatch_waits_for_the_view_to_reach_the_committed_index() {
        use coppice_state::StateMachine;

        use crate::test_support::{FakeConsensus, ProposeOutcome};

        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();

        let (consensus, mut publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        let consensus = Arc::new(consensus);
        let views = consensus.views();
        let (router, mut router_rx) = RouterHandle::channel_for_test();

        let task = {
            let consensus = Arc::clone(&consensus);
            let views = views.clone();
            let router = router.clone();
            tokio::spawn(async move {
                dispatch_ready_attempt(&consensus, &views, &router, attempt_id).await;
            })
        };

        // One yield is enough for the single-threaded test runtime to drive the
        // task through `propose` (which never awaits) and park it on the gate.
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "dispatch must park until the view reaches the committed log index"
        );
        assert!(
            router_rx.try_recv().is_err(),
            "no StartJob may be routed off a view behind the commit"
        );

        // Publish the state that commit produced, at the committed index.
        let mut state = StateMachine::default();
        state.jobs.insert(
            job_id,
            job_record(job_id, "registry/img:1", requested(), None),
        );
        state.attempts.insert(
            attempt_id,
            attempt_record(
                attempt_id,
                job_id,
                alloc_id,
                node,
                AttemptState::Dispatching,
                None,
            ),
        );
        state.allocations.insert(
            alloc_id,
            allocation_record(
                alloc_id,
                job_id,
                attempt_id,
                node,
                requested(),
                AllocationState::Funded,
            ),
        );
        publisher.publish_now(&state, 1);

        task.await.expect("dispatch task");

        let routed = router_rx
            .try_recv()
            .expect("StartJob routed after the publish");
        assert_eq!(routed.node, node);
        let Some(agent_command::Body::StartJob(sj)) = routed.command.body else {
            panic!("expected StartJob");
        };
        assert_eq!(
            AttemptId::try_from(sj.attempt.unwrap()).unwrap(),
            attempt_id
        );
        assert_eq!(
            AllocationId::try_from(sj.allocation.unwrap()).unwrap(),
            alloc_id
        );
        assert_eq!(JobId::try_from(sj.job.unwrap()).unwrap(), job_id);
    }

    /// The resync strong read, deterministically.
    ///
    /// The `read_index` barrier sits at 2 while the publisher still sits at
    /// index 0, whose state holds no attempts — the shape of a `Ready` attempt
    /// applied ahead of the published view (the event tap emits per apply
    /// batch; publishing is cadence-gated). A `views.latest()` scan would see
    /// the empty state, find nothing to dispatch, and return — and since the
    /// attempt's `Ready` event predates the subscription, nothing would ever
    /// re-emit it. The strong read must park on `at_least(read_index)` until
    /// the publish lands, then find the attempt and route its `StartJob`.
    #[tokio::test]
    async fn resync_scans_a_strong_view_not_the_latest_published_one() {
        use coppice_state::StateMachine;

        use crate::test_support::{FakeConsensus, ProposeOutcome};

        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();

        let (consensus, mut publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        consensus.set_read_index(2);
        let consensus = Arc::new(consensus);
        let views = consensus.views();
        let (router, mut router_rx) = RouterHandle::channel_for_test();

        let task = {
            let consensus = Arc::clone(&consensus);
            let views = views.clone();
            let router = router.clone();
            tokio::spawn(async move {
                resync(&consensus, &views, &router).await;
            })
        };

        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "resync must park until the view reaches the read_index barrier"
        );
        assert!(
            router_rx.try_recv().is_err(),
            "nothing may be routed off a view behind the barrier"
        );

        // Publish the state the barrier promises: a Ready attempt with its
        // allocation and job, at the barrier index.
        let mut state = StateMachine::default();
        state.jobs.insert(
            job_id,
            job_record(job_id, "registry/img:1", requested(), None),
        );
        state.attempts.insert(
            attempt_id,
            attempt_record(
                attempt_id,
                job_id,
                alloc_id,
                node,
                AttemptState::Ready,
                None,
            ),
        );
        state.allocations.insert(
            alloc_id,
            allocation_record(
                alloc_id,
                job_id,
                attempt_id,
                node,
                requested(),
                AllocationState::Funded,
            ),
        );
        publisher.publish_now(&state, 2);

        task.await.expect("resync task");

        let routed = router_rx
            .try_recv()
            .expect("StartJob routed for the Ready attempt the strong view revealed");
        assert_eq!(routed.node, node);
        let Some(agent_command::Body::StartJob(sj)) = routed.command.body else {
            panic!("expected StartJob");
        };
        assert_eq!(
            AttemptId::try_from(sj.attempt.unwrap()).unwrap(),
            attempt_id
        );
    }
}
