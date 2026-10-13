//! Dispatch loop (leader-only).
//!
//! Consumes the event stream. On an attempt reaching `Ready` it proposes
//! `DispatchAttempt`, and only after that commits does it send `StartJob` —
//! the commit-before-send ordering of
//! `docs/architecture/command-catalog.md#dispatchattempt`. On a
//! `StopRequested` event it routes a `StopJob`, and on a `PreemptRequested`
//! event a `PreemptJob` (ADR 0044 §4). On gaining leadership (after
//! subscribing) and after any event gap it resyncs by scanning a *strong*
//! view for `Ready` attempts, pending aborts and pending preemptions — events emitted under a
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
use coppice_core::attempt::{AttemptOutcome, AttemptState, PreemptReason};
use coppice_core::id::{AllocationId, AttemptId, NodeId};
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1::{
    agent_command, AgentCommand, Drain, PreemptJob, RegisterAccepted, StartJob, StopJob,
};
use coppice_state::command::DispatchAttempt;
use coppice_state::{AllocationRecord, AttemptRecord, Command, Event, JobRecord, StateMachine};

use crate::leadership;
use crate::tasks::agent_gateway::{RouteCommand, RouterHandle};
use crate::tasks::event_fanout::{EventFilter, FanoutHandle, ProgressItems, SubscriptionItem};

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
        tracing::debug!(term, "dispatch: gained leadership");

        // Subscribe BEFORE resyncing (module doc): the live subscription
        // covers everything applied after it registers, the strong resync
        // covers everything committed before its barrier, and this order is
        // what makes those two ranges overlap instead of leaving a gap.
        //
        // No progress bookmarks: they exist so a client can render a resume
        // cursor (ADR 0043), and dispatch resyncs from a strong view rather
        // than from a cursor.
        let Ok(mut subscription) = fanout
            .subscribe(EventFilter::All, ProgressItems::Omit)
            .await
        else {
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
                            for ordinal_event in &batch.events {
                                handle_event(&consensus, &views, &router, &ordinal_event.event).await;
                            }
                        }
                        // Not subscribed with progress bookmarks, so this
                        // arm exists only because the item type carries them.
                        SubscriptionItem::Progress { .. } => {}
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
        Event::PreemptRequested {
            node,
            allocation,
            reason,
            notice,
            ..
        } => {
            // Built from the event alone: the published view routinely lags
            // the event stream, and the termination record this answers may
            // not be in it yet.
            let command = preempt_job_command(*allocation, *notice, *reason);
            route(router, *node, *allocation, command).await;
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
        .attempts
        .values()
        .filter(|record| attempt_has_pending_abort(view.state(), record))
        .map(|record| (record.attempt.node, record.attempt.allocation))
        .collect();
    for (node, allocation) in pending_aborts {
        route_stop(router, views, node, allocation).await;
    }

    let pending_preempts: Vec<(NodeId, AllocationId, AgentCommand)> = view
        .state()
        .attempts
        .values()
        .filter_map(|record| {
            pending_preempt_command(view.state(), record)
                .map(|command| (record.attempt.node, record.attempt.allocation, command))
        })
        .collect();
    for (node, allocation, command) in pending_preempts {
        route(router, node, allocation, command).await;
    }
}

/// Whether `record` is a live attempt whose agent must be told to stop it.
///
/// This is the "what should this agent have been told" rule for aborts,
/// derived purely from replicated state: the attempt is the job's current one,
/// is `Dispatching | Running` (a container exists or may exist), and the job
/// carries a pending `abort_requested`. `StopJob` is only routed live once
/// (`Event::StopRequested`), and the gateway drops commands for a node with no
/// session, so the rule is re-evaluated by [`resync`] (leadership gain, event
/// gap) and by the ingestion `ObservedSet` diff (every session open), beside
/// [`attempt_has_pending_preempt`]. The deadline drain's rule,
/// [`drain_command_for`], sits beside them.
pub(crate) fn attempt_has_pending_abort(
    state: &coppice_state::StateMachine,
    record: &AttemptRecord,
) -> bool {
    matches!(
        record.attempt.state,
        AttemptState::Dispatching | AttemptState::Running
    ) && state.jobs.get(&record.attempt.job).is_some_and(|job| {
        job.spec.abort_requested.is_some() && job.current_attempt() == Some(record.attempt.id)
    })
}

/// Whether `record` is a running attempt whose agent must still be told to
/// terminate it for a scheduler preemption (ADR 0044 §3, §4): it is its
/// job's current `Running` attempt and carries a `Priority` or `Relocation`
/// record that no reported mark has acknowledged yet. `PreemptJob` is routed
/// live once (`Event::PreemptRequested`) and dropped for a node with no
/// session, so this is re-evaluated wherever [`attempt_has_pending_abort`] is.
/// The agent is idempotent on its journaled notice intent, so a re-send to
/// an agent that did act only re-reports the mark.
pub(crate) fn attempt_has_pending_preempt(
    state: &coppice_state::StateMachine,
    record: &AttemptRecord,
) -> bool {
    record.attempt.state == AttemptState::Running
        && record
            .preemption
            .is_some_and(|p| p.acknowledged.is_none() && p.reason.for_job().is_some())
        && state
            .jobs
            .get(&record.attempt.job)
            .is_some_and(|job| job.current_attempt() == Some(record.attempt.id))
}

/// The `PreemptJob` a pending preemption still owes its agent, re-derived
/// from state: the victim job's declared notice.
pub(crate) fn pending_preempt_command(
    state: &StateMachine,
    record: &AttemptRecord,
) -> Option<AgentCommand> {
    if !attempt_has_pending_preempt(state, record) {
        return None;
    }
    let reason = record.preemption?.reason;
    let notice = state
        .jobs
        .get(&record.attempt.job)
        .map(|job| state.declared_notice(&job.spec))?;
    Some(preempt_job_command(
        record.attempt.allocation,
        notice,
        reason,
    ))
}

/// The `Drain` command the agent on `node` must be sent, given the operator
/// deadline it last echoed (`Heartbeat.drain_target_us`), or `None` when it
/// already holds what the record says (ADR 0044 §5, "Durability").
///
/// The "what should this agent have been told" rule for deadline drains, in
/// the same pure form as [`attempt_has_pending_abort`]: the node's drain
/// record is the source of intent. A deadline the echo does not match sends
/// `Drain { deadline_us }`; no deadline on the record (no drain, a regular
/// drain, or no node record at all) while the echo reports one sends the
/// withdrawal, `deadline_us` unset. A regular drain on its own sends nothing.
/// Re-sent on every mismatching heartbeat — the agent treats a target it
/// already holds as a no-op — so a command lost to a disconnect or a
/// leadership change is healed by the next leader to see the mismatch.
pub(crate) fn drain_command_for(
    state: &StateMachine,
    node: NodeId,
    echo: Option<Timestamp>,
) -> Option<AgentCommand> {
    let target = state
        .nodes
        .get(&node)
        .and_then(|record| record.node.drain)
        .and_then(|drain| drain.deadline);
    if target == echo {
        return None;
    }
    Some(AgentCommand {
        header: None,
        body: Some(agent_command::Body::Drain(Drain {
            deadline_us: target.map(Timestamp::as_micros),
            notice_window_us: state.policy.preemption_notice_cap.as_micros().max(0) as u64,
        })),
    })
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
        dispatched_at: Timestamp::now(),
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
            let agent_command = start_job_command(view.state(), job, record, allocation);
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

async fn route(
    router: &RouterHandle,
    node: NodeId,
    allocation: AllocationId,
    command: AgentCommand,
) {
    if router.send(RouteCommand { node, command }).await.is_err() {
        tracing::warn!(?allocation, "dispatch: router channel closed");
    }
}

async fn route_stop(
    router: &RouterHandle,
    views: &StateViews,
    node: NodeId,
    allocation: AllocationId,
) {
    let grace = views.latest().state().policy.abort_grace;
    let command = stop_job_command(allocation, grace);
    if router.send(RouteCommand { node, command }).await.is_err() {
        tracing::warn!(?allocation, "dispatch: router channel closed");
    }
}

/// Build the header-less `StartJob` command for a freshly-dispatched attempt.
///
/// `header: None` — the session manager stamps the fencing token as it routes
/// (`docs/architecture/command-catalog.md#dispatchattempt`). The `limits` are
/// the allocation's requested vector; `max_runtime` rides straight from the
/// job spec's `max_runtime` (absent = unbounded).
pub(crate) fn start_job_command(
    state: &StateMachine,
    job: &JobRecord,
    attempt: &AttemptRecord,
    allocation: &AllocationRecord,
) -> AgentCommand {
    let identity = resumption_identity(state, job, attempt.attempt.id);
    let preemption = &job.spec.preemption;
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
            max_runtime_us: job.spec.max_runtime.map(|d| d.as_micros() as u64),
            // Ascending name order for free (`JobEnv` is a `BTreeMap`); the
            // agent applies these over the image's own `ENV` on create.
            env: coppice_proto::convert::env_to_pb(&job.spec.env),
            attempt_index: identity.index,
            previous_attempt: identity.previous.as_ref().map(|(id, _)| (*id).into()),
            previous_attempt_outcome: identity
                .previous
                .as_ref()
                .map(|(_, outcome)| outcome.into()),
            preemptible: attempt.effective_preemptible,
            notice_signal: coppice_proto::convert::notice_signal_to_pb(preemption.notice_signal)
                as i32,
            // Resolved at submission, so always present on a stored job; the
            // floor is what an absent one would have resolved to.
            notice_us: preemption
                .notice
                .unwrap_or(state.policy.preemption_notice_floor)
                .as_micros()
                .max(0) as u64,
            // The policy's grace as of this dispatch (ADR 0044 §3): the agent
            // runs a platform termination's stop path without asking us.
            abort_grace_us: state.policy.abort_grace.as_micros().max(0) as u64,
            latest_start_us: latest_start(state, job, attempt).map(|t| t.as_micros()),
            // The policy's margin as of this dispatch (ADR 0044 §5): with
            // `abort_grace`, it fixes this attempt's T_stop under any drain
            // plan the agent runs.
            drain_cleanup_margin_us: state.policy.drain_cleanup_margin.as_micros().max(0) as u64,
        })),
    }
}

/// Drain backfill's launch bound (ADR 0044 §5): `cutoff − max_runtime −
/// abort_grace`, with the policy's current `abort_grace` at dispatch. `None`
/// for an ordinary placement. A backfill always has `max_runtime` (admission
/// requires it); one without would have no bound to protect, so it gets none.
fn latest_start(
    state: &StateMachine,
    job: &JobRecord,
    attempt: &AttemptRecord,
) -> Option<Timestamp> {
    let cutoff = attempt.drain_cutoff?;
    let runtime = job.spec.max_runtime?;
    Some(
        cutoff
            .saturating_sub(runtime)
            .saturating_sub(state.policy.abort_grace),
    )
}

/// The resumption identity of ADR 0044 §2: where an attempt sits in its
/// job's lineage, and the most recent earlier attempt that ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumptionIdentity {
    /// Position in `JobRecord.attempts`, counting attempts that never ran.
    pub index: u32,
    /// The most recent earlier attempt that reached `Running`, with its
    /// outcome.
    pub previous: Option<(AttemptId, AttemptOutcome)>,
}

/// Derive [`ResumptionIdentity`] from the job's durable lineage
/// (`JobRecord.attempts`, ADR 0030), so it is right even when the previous
/// attempt's end was never reported by its agent: a node loss is still a
/// recorded outcome.
///
/// "Reached `Running`" is a recorded `started_at`. Attempts that never ran —
/// revoked while accruing, refused at the door — are skipped for the
/// previous pair, since they can have touched nothing, but still count in
/// the index.
///
/// The pair is reported only for a candidate that is also `Terminal`, and
/// otherwise omitted *whole*, never half: the variables are both-or-neither.
/// Under ADR 0030 an earlier attempt is always terminal by the time a later
/// one exists — a job has one live attempt and only gets a new one through a
/// resolution — so the omission is a guard against a lineage that breaks
/// that rule, where inventing an outcome would be worse than saying nothing.
/// An attempt the lineage names but the state no longer holds is skipped
/// the same way.
pub(crate) fn resumption_identity(
    state: &StateMachine,
    job: &JobRecord,
    attempt: AttemptId,
) -> ResumptionIdentity {
    let position = job
        .attempts
        .iter()
        .position(|a| *a == attempt)
        .unwrap_or(job.attempts.len());
    let previous = job.attempts[..position]
        .iter()
        .rev()
        .filter_map(|id| state.attempts.get(id).map(|r| (*id, r)))
        .find(|(_, r)| r.started_at.is_some())
        .and_then(|(id, r)| match &r.attempt.state {
            AttemptState::Terminal(outcome) => Some((id, outcome.clone())),
            _ => None,
        });
    ResumptionIdentity {
        index: u32::try_from(position).unwrap_or(u32::MAX),
        previous,
    }
}

/// Build the header-less `StopJob` command for an allocation.
///
/// `grace` is the replicated policy's `abort_grace` at the call site;
/// `header: None` — the manager stamps the token as it routes.
pub(crate) fn stop_job_command(allocation: AllocationId, grace: Duration) -> AgentCommand {
    AgentCommand {
        header: None,
        body: Some(agent_command::Body::StopJob(StopJob {
            allocation: Some(allocation.into()),
            grace_us: grace.as_micros(),
        })),
    }
}

/// Build the header-less `PreemptJob` command for a victim allocation (ADR
/// 0044 §3): `notice` is the victim job's declared notice. The stop path's
/// grace is not carried: the agent uses the `abort_grace` journaled from the
/// attempt's `StartJob`, as it does for every platform stop.
pub(crate) fn preempt_job_command(
    allocation: AllocationId,
    notice: Duration,
    reason: PreemptReason,
) -> AgentCommand {
    AgentCommand {
        header: None,
        body: Some(agent_command::Body::PreemptJob(PreemptJob {
            allocation: Some(allocation.into()),
            notice_us: u64::try_from(notice.as_micros()).unwrap_or(0),
            reason: Some(reason.into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::allocation::AllocationState;
    use coppice_core::bytes::ByteSize;
    use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
    use coppice_core::resource::Resources;

    use crate::test_support::{allocation_record, attempt_record, job_record};

    fn requested() -> Resources {
        Resources {
            cpu_millis: 500,
            memory: ByteSize::from_mib(1),
            disk: ByteSize::ZERO,
        }
    }

    #[test]
    fn start_job_command_maps_every_field() {
        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();

        let mut job = job_record(
            job_id,
            "registry/img:1",
            requested(),
            Some(Duration::from_micros(1_234)),
        );
        job.spec.env = [
            ("RUST_LOG".to_string(), "info".to_string()),
            ("A_FIRST".to_string(), "1".to_string()),
        ]
        .into_iter()
        .collect();
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

        let command = start_job_command(&StateMachine::default(), &job, &attempt, &allocation);
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
        // Ascending name order (`JobEnv` is a `BTreeMap`), whatever order the
        // spec's own map happened to be built in.
        assert_eq!(
            sj.env,
            vec![
                coppice_proto::pb::core::v1::EnvVar {
                    name: "A_FIRST".to_string(),
                    value: "1".to_string(),
                },
                coppice_proto::pb::core::v1::EnvVar {
                    name: "RUST_LOG".to_string(),
                    value: "info".to_string(),
                },
            ]
        );
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

        let command = start_job_command(&StateMachine::default(), &job, &attempt, &allocation);
        let agent_command::Body::StartJob(sj) = command.body.expect("body") else {
            panic!("expected StartJob");
        };
        assert_eq!(sj.max_runtime_us, None);
        assert_eq!(sj.latest_start_us, None);
    }

    /// ADR 0044 §5: drain backfill carries `cutoff − max_runtime −
    /// abort_grace` as its launch bound; an ordinary placement carries none.
    #[test]
    fn start_job_command_carries_the_drain_backfill_launch_bound() {
        let job_id = JobId::new();
        let attempt_id = AttemptId::new();
        let alloc_id = AllocationId::new();
        let node = NodeId::new();
        let job = job_record(job_id, "img", requested(), Some(Duration::from_secs(600)));
        let mut attempt = attempt_record(
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
        let state = StateMachine::default();
        let start = |attempt: &AttemptRecord| {
            let command = start_job_command(&state, &job, attempt, &allocation);
            let agent_command::Body::StartJob(sj) = command.body.expect("body") else {
                panic!("expected StartJob");
            };
            sj.latest_start_us
        };
        assert_eq!(start(&attempt), None);

        let cutoff = Timestamp::from_micros(10_000_000_000).unwrap();
        attempt.drain_cutoff = Some(cutoff);
        let expected = cutoff
            .saturating_sub(Duration::from_secs(600))
            .saturating_sub(state.policy.abort_grace);
        assert_eq!(start(&attempt), Some(expected.as_micros()));
    }

    /// ADR 0044 §2: the index counts every attempt; the previous pair
    /// skips attempts that never ran and names the latest that did.
    #[test]
    fn resumption_identity_skips_attempts_that_never_ran() {
        let job_id = JobId::new();
        let node = NodeId::new();
        let mut state = StateMachine::default();
        let mut job = job_record(job_id, "img", requested(), None);
        let started = Some(Timestamp::UNIX_EPOCH);
        // 0 ran and was lost; 1 ran and exited; 2 was revoked while
        // accruing; 3 is the attempt being started.
        let lineage = [
            (AttemptState::Terminal(AttemptOutcome::NodeLost), started),
            (
                AttemptState::Terminal(AttemptOutcome::Exited { code: 3 }),
                started,
            ),
            (AttemptState::Terminal(AttemptOutcome::Revoked), None),
            (AttemptState::Ready, None),
        ];
        let ids: Vec<AttemptId> = lineage.iter().map(|_| AttemptId::new()).collect();
        for (id, (st, started_at)) in ids.iter().zip(lineage) {
            state.attempts.insert(
                *id,
                attempt_record(*id, job_id, AllocationId::new(), node, st, started_at),
            );
            job.attempts.push(*id);
        }

        let identity = resumption_identity(&state, &job, ids[3]);
        assert_eq!(identity.index, 3);
        assert_eq!(
            identity.previous,
            Some((ids[1], AttemptOutcome::Exited { code: 3 }))
        );
        // The first attempt has nobody before it.
        assert_eq!(
            resumption_identity(&state, &job, ids[0]),
            ResumptionIdentity {
                index: 0,
                previous: None
            }
        );

        // And it reaches the command, with the notice contract.
        job.spec.preemption.notice = Some(Duration::from_secs(90));
        let mut current = state.attempts.get(&ids[3]).unwrap().clone();
        current.effective_preemptible = true;
        let allocation = allocation_record(
            current.attempt.allocation,
            job_id,
            ids[3],
            node,
            requested(),
            AllocationState::Funded,
        );
        let agent_command::Body::StartJob(sj) =
            start_job_command(&state, &job, &current, &allocation)
                .body
                .expect("body")
        else {
            panic!("expected StartJob");
        };
        assert_eq!(sj.attempt_index, 3);
        assert_eq!(
            AttemptId::try_from(sj.previous_attempt.unwrap()).unwrap(),
            ids[1]
        );
        assert!(sj.previous_attempt_outcome.is_some());
        assert!(sj.preemptible);
        assert_eq!(sj.notice_us, 90_000_000);
        assert_eq!(
            sj.abort_grace_us,
            state.policy.abort_grace.as_micros() as u64
        );
        assert_eq!(
            sj.drain_cleanup_margin_us,
            state.policy.drain_cleanup_margin.as_micros() as u64,
            "the policy's margin rides every dispatch (ADR 0044 §5)"
        );
        assert_eq!(
            sj.notice_signal,
            coppice_proto::pb::core::v1::NoticeSignal::Sigusr1 as i32
        );
    }

    /// ADR 0044 §5, "Durability": the record is the intent and the echo is
    /// what the agent holds; only a mismatch is a command, and a regular
    /// drain is never one.
    #[test]
    fn drain_command_for_reconciles_the_record_against_the_echo() {
        let node = NodeId::new();
        let deadline = Timestamp::UNIX_EPOCH + Duration::from_hours(2);
        let other = deadline + Duration::from_mins(10);
        let mut state = StateMachine::default();
        state
            .nodes
            .insert(node, crate::test_support::node_record(node, 1, true));
        let drain_of = |cmd: Option<AgentCommand>| match cmd.and_then(|c| c.body) {
            Some(agent_command::Body::Drain(d)) => Some(d),
            None => None,
            other => panic!("expected a Drain, got {other:?}"),
        };

        // No drain, nothing echoed: nothing to say.
        assert_eq!(drain_command_for(&state, node, None), None);
        // A regular drain sends nothing either.
        state.nodes.get_mut(&node).unwrap().node.drain = Some(coppice_core::node::Drain {
            requested_at: Timestamp::UNIX_EPOCH,
            deadline: None,
        });
        assert_eq!(drain_command_for(&state, node, None), None);
        // ...but a stale echo of an earlier deadline is withdrawn.
        let withdrawal = drain_of(drain_command_for(&state, node, Some(deadline))).unwrap();
        assert_eq!(withdrawal.deadline_us, None);

        // A deadline drain: sent until echoed, with the policy's cap as the
        // common window.
        state.nodes.get_mut(&node).unwrap().node.drain = Some(coppice_core::node::Drain {
            requested_at: Timestamp::UNIX_EPOCH,
            deadline: Some(deadline),
        });
        for echo in [None, Some(other)] {
            let sent = drain_of(drain_command_for(&state, node, echo)).unwrap();
            assert_eq!(sent.deadline_us, Some(deadline.as_micros()));
            assert_eq!(
                sent.notice_window_us,
                state.policy.preemption_notice_cap.as_micros() as u64
            );
        }
        assert_eq!(drain_command_for(&state, node, Some(deadline)), None);

        // A node the record no longer holds is treated as undrained.
        let gone = NodeId::new();
        assert_eq!(drain_command_for(&state, gone, None), None);
        assert!(drain_of(drain_command_for(&state, gone, Some(deadline)))
            .unwrap()
            .deadline_us
            .is_none());
    }

    #[test]
    fn stop_job_command_carries_allocation_and_grace() {
        let alloc_id = AllocationId::new();
        let command = stop_job_command(alloc_id, Duration::from_secs(30));
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
