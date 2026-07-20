//! Ingestion / normalizer (leader-only).
//!
//! The boundary of `docs/architecture/command-catalog.md#the-agent-report-ingestion-boundary`:
//! fencing check, dedupe by `(AttemptId, attempt_state)`, timestamping, and
//! the ObservedSet diff, then `propose` (`RecordAttempt*`, `ReconcileNode`,
//! `RegisterNode`, `DeclareNodeLost` from the health monitor). Benign apply
//! rejections (`StaleAttemptState` and the like) are ignored rather than
//! treated as failures — see `docs/architecture/coordinator-runtime.md`,
//! "Ingestion / normalizer".
//!
//! Raw agent reports are never Raft commands (the ingestion boundary). This
//! layer turns each report into a [`Normalized`] verdict: the log commands to
//! propose, the `StopJob`s to route directly (a "stop" verdict has nothing in
//! state to mutate, so it never enters the log — command-catalog.md), whether
//! the report was a `Register` (so a `RegisterAccepted` follows once the epoch
//! bump is applied), and whether it counts as liveness for the node.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use coppice_consensus::{Consensus, ConsensusStatus, StateView, StateViews};
use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{
    LostAttempt, ReconcileNode, RecordAttemptExited, RecordAttemptOutcome, RecordAttemptStarted,
    RegisterNode,
};
use coppice_state::Command;

use crate::leadership;
use crate::liveness::NodeLiveness;
use crate::tasks::agent_gateway::{InboundReport, RouteCommand, RouterHandle};
use crate::tasks::dispatch::{register_accepted_command, stop_job_command};

use coppice_proto::pb::agent::v1::agent_report::Body;

/// The verdict of normalizing one agent report.
struct Normalized {
    /// Log commands to propose, in order.
    commands: Vec<Command>,
    /// `StopJob` targets to route directly to this node — never a log command
    /// (command-catalog.md: a running container with no replicated intent has
    /// nothing in state to mutate).
    stops: Vec<AllocationId>,
    /// The report was a `Register`: send `RegisterAccepted` once the
    /// `RegisterNode` apply bumps the epoch (ADR 0009 step 2).
    register: bool,
    /// Any report shape counts as liveness for this node.
    liveness: bool,
}

impl Normalized {
    fn empty() -> Self {
        Normalized {
            commands: Vec::new(),
            stops: Vec::new(),
            register: false,
            liveness: true,
        }
    }
}

/// Run the ingestion loop until shutdown.
pub async fn run<C: Consensus>(
    consensus: Arc<C>,
    views: StateViews,
    router: RouterHandle,
    liveness: NodeLiveness,
    mut inbound: mpsc::Receiver<InboundReport>,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::debug!(
            term,
            "ingestion: gained leadership, draining inbound reports"
        );

        let lost_leadership = drain(
            &consensus,
            &views,
            &router,
            &liveness,
            &mut inbound,
            &mut status,
            term,
            &mut shutdown,
        )
        .await;
        if !lost_leadership {
            // The inbound sender side is gone (agent gateway shut down)
            // rather than leadership having moved; nothing left to ingest.
            return;
        }
    }
}

/// Drain inbound reports until leadership is lost or shutdown.
///
/// Returns `true` when it stopped because leadership was lost (the caller should
/// re-gate), `false` when the inbound channel closed for good.
#[allow(clippy::too_many_arguments)]
async fn drain<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    router: &RouterHandle,
    liveness: &NodeLiveness,
    inbound: &mut mpsc::Receiver<InboundReport>,
    status: &mut watch::Receiver<ConsensusStatus>,
    term: u64,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        tokio::select! {
            biased;
            _ = leadership::until_leadership_lost(status, term, shutdown) => {
                return true;
            }
            report = inbound.recv() => {
                let Some(report) = report else { return false };
                if let Some(lost) = ingest(consensus, views, router, liveness, &report).await {
                    return lost;
                }
            }
        }
    }
}

/// Normalize and act on one report. Returns `Some(lost_leadership)` if the
/// drain loop should stop, `None` to keep draining.
async fn ingest<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    router: &RouterHandle,
    liveness: &NodeLiveness,
    report: &InboundReport,
) -> Option<bool> {
    let node = report.node;
    let now = Timestamp::now();
    let view = views.latest();
    let normalized = normalize(&view, report, now);

    if normalized.liveness {
        liveness.mark(node);
    }

    // Propose every log command; remember a successful RegisterNode's index so
    // we can read-our-writes before stamping RegisterAccepted.
    let mut register_index: Option<u64> = None;
    for command in normalized.commands {
        let is_register = matches!(command, Command::RegisterNode(_));
        match consensus.propose(command).await {
            Ok(applied) => match applied.outcome {
                Ok(_) => {
                    if is_register {
                        register_index = Some(applied.log_index);
                    }
                }
                Err(reason) => {
                    tracing::debug!(?reason, "ingestion: benign rejection");
                }
            },
            Err(e) if e.is_retryable() => {
                tracing::info!(
                    error = %e,
                    "ingestion: retryable propose error, re-gating on leadership"
                );
                return Some(true);
            }
            Err(e) => {
                tracing::error!(error = %e, "ingestion: fatal propose error");
                return Some(false);
            }
        }
    }

    // Route stops directly (never log commands, command-catalog.md).
    if !normalized.stops.is_empty() {
        let grace = view.state().policy.abort_grace;
        for allocation in normalized.stops {
            let command = stop_job_command(allocation, grace);
            if router.send(RouteCommand { node, command }).await.is_err() {
                tracing::warn!(?allocation, "ingestion: router closed routing StopJob");
            }
        }
    }

    // After a RegisterNode applies, wait until the manager's view carries the
    // bumped epoch (read-your-writes), then route RegisterAccepted so its
    // stamped token carries the fresh epoch (ADR 0009 step 2).
    if normalized.register {
        if let Some(index) = register_index {
            if views.at_least(index).await.is_ok() {
                let command = register_accepted_command();
                if router.send(RouteCommand { node, command }).await.is_err() {
                    tracing::warn!(%node, "ingestion: router closed routing RegisterAccepted");
                }
            }
        } else {
            // Unreachable today — apply never rejects RegisterNode — but a
            // rejection would otherwise strand the agent silently: no
            // RegisterAccepted means it retries registration forever.
            tracing::warn!(
                %node,
                "ingestion: RegisterNode was rejected, not routing RegisterAccepted"
            );
        }
    }

    None
}

/// Normalize one agent report into a [`Normalized`] verdict.
///
/// Fencing (ADR 0009), view-based dedupe, timestamping, and the ObservedSet /
/// heartbeat diffs. See
/// `docs/architecture/command-catalog.md#the-agent-report-ingestion-boundary`.
fn normalize(view: &StateView, report: &InboundReport, now: Timestamp) -> Normalized {
    let node = report.node;
    let report_epoch = report.report.node_epoch;
    let mut out = Normalized::empty();

    let Some(body) = report.report.body.as_ref() else {
        tracing::debug!(%node, "ingestion: empty report body");
        return out;
    };

    match body {
        // Registration is always legal — no fencing check (command-catalog.md
        // #registernode).
        Body::Register(reg) => {
            out.register = true;
            let capacity = match reg
                .capacity
                .clone()
                .and_then(|c| Resources::try_from(c).ok())
            {
                Some(c) => c,
                None => {
                    tracing::warn!(%node, "ingestion: Register with bad capacity, dropping");
                    out.register = false;
                    return out;
                }
            };
            // Labels: the agent sends canonical (ascending, unique) key/value
            // pairs; a duplicate key here is benign (last wins).
            let mut labels = BTreeMap::new();
            for label in &reg.labels {
                labels.insert(label.key.clone(), label.value.clone());
            }
            // Advertised NodeService address (ADR 0034); an empty string is a
            // second spelling of "no service", canonicalized to None.
            let service_addr = reg.service_addr.clone().filter(|addr| !addr.is_empty());
            out.commands.push(Command::RegisterNode(RegisterNode {
                node,
                capacity,
                labels,
                registered_at: now,
                service_addr,
            }));
        }

        // Periodic liveness + running-set invariant check (ADR 0009 last
        // paragraph). ImageCacheInventory is observed-only soft-scoring input
        // (ADR 0010) — never a command; ignored in v1.
        Body::Heartbeat(hb) => {
            let Some(node_record) = view.state().nodes.get(&node) else {
                tracing::debug!(%node, "ingestion: heartbeat for unknown node, dropping");
                return out;
            };
            if report_epoch != node_record.epoch {
                // Stale epoch: still counts for liveness, but produces no
                // commands or diff (ADR 0009).
                return out;
            }
            heartbeat_diff(view, node, report_epoch, hb, now, &mut out);
        }

        Body::AttemptStatus(status) => {
            let Some(node_record) = view.state().nodes.get(&node) else {
                tracing::debug!(%node, "ingestion: attempt status for unknown node, dropping");
                return out;
            };
            if report_epoch != node_record.epoch {
                // Stale epoch: AttemptStatus is dropped entirely (ADR 0009).
                tracing::debug!(%node, "ingestion: stale-epoch attempt status dropped");
                return out;
            }
            attempt_status_diff(view, status, now, &mut out);
        }

        Body::ObservedSet(set) => {
            let Some(node_record) = view.state().nodes.get(&node) else {
                tracing::debug!(%node, "ingestion: observed set for unknown node, dropping");
                return out;
            };
            let stale = report_epoch != node_record.epoch;
            observed_set_diff(view, node, report_epoch, set, now, stale, &mut out);
        }
    }

    out
}

/// Linear rank of an attempt phase, for the monotonic dedupe checks.
fn phase_rank(state: &AttemptState) -> u8 {
    match state {
        AttemptState::Accruing => 0,
        AttemptState::Ready => 1,
        AttemptState::Dispatching => 2,
        AttemptState::Running => 3,
        AttemptState::Finalizing => 4,
        AttemptState::Terminal(_) => 5,
    }
}

/// Runtime since the attempt reached `Running`, clamped at zero: the
/// reporting node's clock can legitimately be behind the stamp that recorded
/// the start.
fn runtime_since(started_at: Option<Timestamp>, now: Timestamp) -> Duration {
    match started_at {
        Some(started) => (now - started).max(Duration::ZERO),
        None => Duration::ZERO,
    }
}

/// An agent-reported runtime (`uint64` µs on the agent wire) as a domain
/// span, saturating: the domain type is signed, so a report past `i64::MAX`
/// µs — ~292 000 years, only reachable from a corrupt or hostile agent —
/// pins at the maximum rather than wrapping negative and pricing as zero.
fn reported_runtime(runtime_us: u64) -> Duration {
    Duration::from_micros(i64::try_from(runtime_us).unwrap_or(i64::MAX))
}

/// An `AttemptStatus` report (fresh epoch): dedupe by `(attempt, state)`
/// against the view and emit at most one attempt-progress command
/// (command-catalog.md).
fn attempt_status_diff(
    view: &StateView,
    status: &coppice_proto::pb::agent::v1::AttemptStatus,
    now: Timestamp,
    out: &mut Normalized,
) {
    let Some(attempt_id) = status
        .attempt
        .clone()
        .and_then(|a| AttemptId::try_from(a).ok())
    else {
        tracing::debug!("ingestion: attempt status with malformed attempt id, dropping");
        return;
    };
    let observed = match status.observed.and_then(|o| AttemptState::try_from(o).ok()) {
        Some(o) => o,
        None => {
            tracing::debug!(
                ?attempt_id,
                "ingestion: attempt status with bad observed state"
            );
            return;
        }
    };
    let Some(record) = view.state().attempts.get(&attempt_id) else {
        // Unknown attempt: stale report (apply would reject `UnknownAttempt`).
        tracing::debug!(?attempt_id, "ingestion: attempt status for unknown attempt");
        return;
    };
    let current_rank = phase_rank(&record.attempt.state);

    match observed {
        AttemptState::Running => {
            // Skip if already Running/Finalizing/Terminal.
            if current_rank < phase_rank(&AttemptState::Running) {
                out.commands
                    .push(Command::RecordAttemptStarted(RecordAttemptStarted {
                        attempt: attempt_id,
                        observed_at: now,
                    }));
            }
        }
        AttemptState::Finalizing => {
            // Skip if already >= Finalizing.
            if current_rank < phase_rank(&AttemptState::Finalizing) {
                out.commands
                    .push(Command::RecordAttemptExited(RecordAttemptExited {
                        attempt: attempt_id,
                        observed_at: now,
                    }));
            }
        }
        AttemptState::Terminal(outcome) => {
            if record.attempt.state.is_terminal() {
                return; // already terminal (dedupe)
            }
            if matches!(outcome, AttemptOutcome::Revoked) {
                // Only CommitPlacements may produce Revoked (command-catalog.md).
                tracing::warn!(?attempt_id, "ingestion: agent reported Revoked, dropping");
                return;
            }
            out.commands
                .push(Command::RecordAttemptOutcome(RecordAttemptOutcome {
                    attempt: attempt_id,
                    outcome,
                    actual_runtime: reported_runtime(status.runtime_us),
                    observed_at: now,
                }));
        }
        // Agents cannot observe pre-dispatch phases.
        AttemptState::Accruing | AttemptState::Ready | AttemptState::Dispatching => {
            tracing::warn!(
                ?attempt_id,
                "ingestion: agent reported an unobservable attempt state, dropping"
            );
        }
    }
}

/// The heartbeat running-set diff (fresh epoch): adopt Dispatching containers
/// the agent confirms running, stop containers with no live intent, and mark
/// `Running` attempts absent from the set as lost. A `Dispatching` attempt
/// absent from the set is NOT lost — the `StartJob` may still be in flight
/// (ADR 0009).
fn heartbeat_diff(
    view: &StateView,
    node: NodeId,
    report_epoch: u64,
    hb: &coppice_proto::pb::agent::v1::Heartbeat,
    now: Timestamp,
    out: &mut Normalized,
) {
    let running: BTreeSet<AllocationId> = hb
        .running
        .iter()
        .filter_map(|a| AllocationId::try_from(a.clone()).ok())
        .collect();

    let mut adopted: Vec<AttemptId> = Vec::new();
    let mut lost: Vec<LostAttempt> = Vec::new();

    // Forward: each reported-running allocation.
    for allocation in &running {
        match view.state().allocations.get(allocation) {
            None => out.stops.push(*allocation), // unknown allocation
            Some(alloc) if alloc.allocation.state == AllocationState::Released => {
                out.stops.push(*allocation);
            }
            Some(alloc) => {
                let attempt_id = alloc.allocation.attempt;
                match view.state().attempts.get(&attempt_id) {
                    None => out.stops.push(*allocation),
                    Some(att) if att.attempt.state.is_terminal() => out.stops.push(*allocation),
                    Some(att) if att.attempt.state == AttemptState::Dispatching => {
                        adopted.push(attempt_id);
                    }
                    Some(_) => {} // Running (normal) or otherwise: nothing
                }
            }
        }
    }

    // Reverse: `Running` attempts on this node absent from the running set are
    // lost. `Dispatching` (StartJob may be in flight) and `Finalizing`
    // (naturally absent from a running list) are NOT lost.
    for (attempt_id, att) in view.state().attempts.iter() {
        if att.attempt.node != node {
            continue;
        }
        if att.attempt.state == AttemptState::Running && !running.contains(&att.attempt.allocation)
        {
            lost.push(LostAttempt {
                attempt: *attempt_id,
                outcome: AttemptOutcome::AgentError,
                actual_runtime: runtime_since(att.started_at, now),
            });
        }
    }

    // Don't spam the log with empty reconciles (command-catalog.md).
    if !adopted.is_empty() || !lost.is_empty() {
        out.commands.push(Command::ReconcileNode(ReconcileNode {
            node,
            node_epoch: report_epoch,
            adopted,
            lost,
            observed_at: now,
        }));
    }
}

/// The full ObservedSet restart diff (ADR 0009 step 3). Under a stale epoch it
/// is demoted to reconciliation input: only the stop verdicts survive (they
/// depend on current state alone), never a `ReconcileNode` or attempt-outcome
/// command.
fn observed_set_diff(
    view: &StateView,
    node: NodeId,
    report_epoch: u64,
    set: &coppice_proto::pb::agent::v1::ObservedSet,
    now: Timestamp,
    stale: bool,
    out: &mut Normalized,
) {
    let mut adopted: Vec<AttemptId> = Vec::new();
    let mut mentioned_alloc: BTreeSet<AllocationId> = BTreeSet::new();
    let mut mentioned_attempt: BTreeSet<AttemptId> = BTreeSet::new();

    for obs in &set.allocations {
        let Some(alloc_id) = obs
            .allocation
            .clone()
            .and_then(|a| AllocationId::try_from(a).ok())
        else {
            tracing::debug!("ingestion: observed allocation with malformed id, skipping");
            continue;
        };
        mentioned_alloc.insert(alloc_id);
        if let Some(attempt_id) = obs
            .attempt
            .clone()
            .and_then(|a| AttemptId::try_from(a).ok())
        {
            mentioned_attempt.insert(attempt_id);
        }

        // Live intent = the allocation exists and is not Released, and its
        // attempt exists and is non-terminal.
        let live = view
            .state()
            .allocations
            .get(&alloc_id)
            .filter(|a| a.allocation.state != AllocationState::Released)
            .and_then(|a| view.state().attempts.get(&a.allocation.attempt))
            .filter(|att| !att.attempt.state.is_terminal())
            .map(|att| (att.attempt.id, att.attempt.state.clone()));

        let Some((attempt_id, attempt_state)) = live else {
            // No live intent: a running container is orphaned → stop; an
            // exited one is already settled → ignore.
            if obs.running {
                out.stops.push(alloc_id);
            }
            continue;
        };

        if stale {
            // Demoted: stops (handled above) are the only safe verdict.
            continue;
        }

        if obs.running {
            match attempt_state {
                AttemptState::Dispatching => adopted.push(attempt_id),
                AttemptState::Running => {} // already running, benign
                AttemptState::Finalizing => {
                    tracing::debug!(?attempt_id, "ingestion: observed running but finalizing");
                }
                _ => {} // Accruing/Ready with a live container: leave it
            }
        } else {
            // Observed exit: record the outcome (attempt is non-terminal here).
            let outcome = obs
                .outcome
                .and_then(|o| AttemptOutcome::try_from(o).ok())
                .filter(|o| !matches!(o, AttemptOutcome::Revoked))
                .unwrap_or_else(|| {
                    tracing::warn!(
                        ?attempt_id,
                        "ingestion: observed exit with missing/Revoked outcome, using AgentError"
                    );
                    AttemptOutcome::AgentError
                });
            out.commands
                .push(Command::RecordAttemptOutcome(RecordAttemptOutcome {
                    attempt: attempt_id,
                    outcome,
                    actual_runtime: reported_runtime(obs.runtime_us),
                    observed_at: now,
                }));
        }
    }

    if stale {
        return;
    }

    // Reverse: every Dispatching/Running/Finalizing attempt on this node not
    // mentioned anywhere in the set is lost. Dispatching-absent IS lost here
    // (unlike heartbeats) because the ObservedSet follows a re-registration
    // whose epoch bump already fenced any in-flight StartJob (ADR 0009).
    let mut lost: Vec<LostAttempt> = Vec::new();
    for (attempt_id, att) in view.state().attempts.iter() {
        if att.attempt.node != node {
            continue;
        }
        let live = matches!(
            att.attempt.state,
            AttemptState::Dispatching | AttemptState::Running | AttemptState::Finalizing
        );
        if live
            && !mentioned_attempt.contains(attempt_id)
            && !mentioned_alloc.contains(&att.attempt.allocation)
        {
            lost.push(LostAttempt {
                attempt: *attempt_id,
                outcome: AttemptOutcome::AgentError,
                actual_runtime: runtime_since(att.started_at, now),
            });
        }
    }

    if !adopted.is_empty() || !lost.is_empty() {
        out.commands.push(Command::ReconcileNode(ReconcileNode {
            node,
            node_epoch: report_epoch,
            adopted,
            lost,
            observed_at: now,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;
    use coppice_core::id::JobId;
    use coppice_state::StateMachine;

    use coppice_proto::pb::agent::v1::{
        AgentReport, AttemptStatus, Heartbeat, ObservedAllocation, ObservedSet, Register,
    };
    use coppice_proto::pb::core::v1::Label;

    use crate::test_support::{allocation_record, attempt_record, node_record, view_of};

    /// The fixture "now", one second past the epoch. A function rather than
    /// a const because `Timestamp`'s range check is a runtime one.
    fn now() -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(1)
    }

    fn requested() -> Resources {
        Resources {
            cpu_millis: 250,
            memory: ByteSize::from_mib(1),
            disk: ByteSize::ZERO,
        }
    }

    fn report(node: NodeId, epoch: u64, body: Body) -> InboundReport {
        InboundReport {
            node,
            report: AgentReport {
                node: Some(node.into()),
                node_epoch: epoch,
                body: Some(body),
            },
        }
    }

    fn heartbeat(running: &[AllocationId]) -> Body {
        Body::Heartbeat(Heartbeat {
            capacity: None,
            running: running.iter().map(|a| (*a).into()).collect(),
            image_cache: None,
        })
    }

    fn attempt_status(attempt: AttemptId, observed: &AttemptState, runtime_us: u64) -> Body {
        Body::AttemptStatus(AttemptStatus {
            allocation: Some(AllocationId::new().into()),
            attempt: Some(attempt.into()),
            job: Some(JobId::new().into()),
            observed: Some(observed.into()),
            runtime_us,
        })
    }

    fn observed(
        alloc: AllocationId,
        attempt: AttemptId,
        running: bool,
        outcome: Option<&AttemptOutcome>,
        runtime_us: u64,
    ) -> ObservedAllocation {
        ObservedAllocation {
            allocation: Some(alloc.into()),
            attempt: Some(attempt.into()),
            job: Some(JobId::new().into()),
            running,
            outcome: outcome.map(|o| o.into()),
            runtime_us,
        }
    }

    #[test]
    fn register_emits_register_node_no_fencing() {
        let node = NodeId::new();
        let view = view_of(StateMachine::default());
        let reg = Body::Register(Register {
            capacity: Some((&requested()).into()),
            labels: vec![Label {
                key: "zone".into(),
                value: "a".into(),
            }],
            service_addr: Some("10.0.0.7:9443".into()),
        });

        let out = normalize(&view, &report(node, 0, reg), now());

        assert!(out.register);
        assert!(out.liveness);
        assert_eq!(out.commands.len(), 1);
        match &out.commands[0] {
            Command::RegisterNode(rn) => {
                assert_eq!(rn.node, node);
                assert_eq!(rn.capacity, requested());
                assert_eq!(rn.labels.get("zone").map(String::as_str), Some("a"));
                assert_eq!(rn.registered_at, now());
                assert_eq!(rn.service_addr.as_deref(), Some("10.0.0.7:9443"));
            }
            other => panic!("expected RegisterNode, got {other:?}"),
        }
    }

    #[test]
    fn heartbeat_stale_epoch_is_liveness_only() {
        let node = NodeId::new();
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 2, true));
        let view = view_of(sm);

        // Report epoch 1 != current 2.
        let out = normalize(&view, &report(node, 1, heartbeat(&[])), now());
        assert!(out.commands.is_empty());
        assert!(out.stops.is_empty());
        assert!(out.liveness);
    }

    #[test]
    fn attempt_status_stale_epoch_is_dropped() {
        let node = NodeId::new();
        let attempt = AttemptId::new();
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 2, true));
        sm.attempts.insert(
            attempt,
            attempt_record(
                attempt,
                JobId::new(),
                AllocationId::new(),
                node,
                AttemptState::Dispatching,
                None,
            ),
        );
        let view = view_of(sm);

        let out = normalize(
            &view,
            &report(node, 1, attempt_status(attempt, &AttemptState::Running, 0)),
            now(),
        );
        assert!(out.commands.is_empty());
    }

    #[test]
    fn attempt_status_running_dedupes_on_view_state() {
        let node = NodeId::new();
        let attempt = AttemptId::new();
        let job = JobId::new();

        // Dispatching -> Running produces RecordAttemptStarted.
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        sm.attempts.insert(
            attempt,
            attempt_record(
                attempt,
                job,
                AllocationId::new(),
                node,
                AttemptState::Dispatching,
                None,
            ),
        );
        let out = normalize(
            &view_of(sm),
            &report(node, 5, attempt_status(attempt, &AttemptState::Running, 0)),
            now(),
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], Command::RecordAttemptStarted(_)));

        // Already Running -> the duplicate produces nothing (view-based dedupe).
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        sm.attempts.insert(
            attempt,
            attempt_record(
                attempt,
                job,
                AllocationId::new(),
                node,
                AttemptState::Running,
                Some(now()),
            ),
        );
        let out = normalize(
            &view_of(sm),
            &report(node, 5, attempt_status(attempt, &AttemptState::Running, 0)),
            now(),
        );
        assert!(out.commands.is_empty());
    }

    #[test]
    fn attempt_status_terminal_revoked_is_dropped() {
        let node = NodeId::new();
        let attempt = AttemptId::new();
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        sm.attempts.insert(
            attempt,
            attempt_record(
                attempt,
                JobId::new(),
                AllocationId::new(),
                node,
                AttemptState::Running,
                Some(now()),
            ),
        );
        let out = normalize(
            &view_of(sm),
            &report(
                node,
                5,
                attempt_status(attempt, &AttemptState::Terminal(AttemptOutcome::Revoked), 0),
            ),
            now(),
        );
        assert!(out.commands.is_empty());
    }

    #[test]
    fn heartbeat_diff_adopts_stops_and_loses() {
        let node = NodeId::new();
        let job = JobId::new();
        let (att_adopt, att_run, att_lost, att_disp) = (
            AttemptId::new(),
            AttemptId::new(),
            AttemptId::new(),
            AttemptId::new(),
        );
        let (a_adopt, a_run, a_lost, a_disp, a_ghost) = (
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
        );

        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        for (att, alloc, state, started) in [
            (att_adopt, a_adopt, AttemptState::Dispatching, None),
            (att_run, a_run, AttemptState::Running, Some(now())),
            (
                att_lost,
                a_lost,
                AttemptState::Running,
                Some(now() - Duration::from_micros(1_000)),
            ),
            (att_disp, a_disp, AttemptState::Dispatching, None),
        ] {
            sm.attempts
                .insert(att, attempt_record(att, job, alloc, node, state, started));
            sm.allocations.insert(
                alloc,
                allocation_record(alloc, job, att, node, requested(), AllocationState::Active),
            );
        }
        let view = view_of(sm);

        // Reported running: adopt (Dispatching), normal (Running), unknown (ghost -> stop).
        let out = normalize(
            &view,
            &report(node, 5, heartbeat(&[a_adopt, a_run, a_ghost])),
            now(),
        );

        assert_eq!(out.stops, vec![a_ghost]);
        assert_eq!(out.commands.len(), 1);
        let Command::ReconcileNode(rn) = &out.commands[0] else {
            panic!("expected ReconcileNode");
        };
        assert_eq!(rn.node, node);
        assert_eq!(rn.node_epoch, 5);
        assert_eq!(rn.adopted, vec![att_adopt]);
        assert_eq!(rn.lost.len(), 1);
        assert_eq!(rn.lost[0].attempt, att_lost);
        assert_eq!(rn.lost[0].outcome, AttemptOutcome::AgentError);
        assert_eq!(rn.lost[0].actual_runtime, Duration::from_micros(1_000));
        // att_disp (Dispatching, absent from running) is NOT lost.
        assert!(!rn.lost.iter().any(|l| l.attempt == att_disp));
    }

    /// Regression for the heartbeat-exit race: once an attempt's terminal
    /// report has been applied, a heartbeat that omits its allocation from
    /// `running` must not mark it lost. The agent reports the exit before
    /// reaping the container (session `pending_reaps`), and ingestion is
    /// serial — each report's commands are applied before the next report is
    /// normalized — so by the time the post-exit heartbeat arrives the attempt
    /// has left `Running` and its absence is benign, even while the exited
    /// container still awaits its reap behind the telemetry drain barrier.
    #[test]
    fn heartbeat_absence_after_terminal_report_is_not_lost() {
        let node = NodeId::new();
        let job = JobId::new();

        for state in [
            AttemptState::Finalizing,
            AttemptState::Terminal(AttemptOutcome::Exited { code: 0 }),
        ] {
            let attempt = AttemptId::new();
            let alloc = AllocationId::new();
            let mut sm = StateMachine::default();
            sm.nodes.insert(node, node_record(node, 5, true));
            sm.attempts.insert(
                attempt,
                attempt_record(attempt, job, alloc, node, state.clone(), Some(now())),
            );
            sm.allocations.insert(
                alloc,
                allocation_record(
                    alloc,
                    job,
                    attempt,
                    node,
                    requested(),
                    AllocationState::Active,
                ),
            );

            let out = normalize(&view_of(sm), &report(node, 5, heartbeat(&[])), now());
            assert!(
                out.commands.is_empty(),
                "an exited-but-unreaped allocation absent from the heartbeat \
                 must not be reconciled as lost (state {state:?})"
            );
            assert!(out.stops.is_empty());
        }
    }

    #[test]
    fn observed_set_diff_adopts_stops_loses_and_records_exits() {
        let node = NodeId::new();
        let job = JobId::new();
        let (att_adopt, att_disp_lost, att_exit, att_rev) = (
            AttemptId::new(),
            AttemptId::new(),
            AttemptId::new(),
            AttemptId::new(),
        );
        let (a_adopt, a_disp_lost, a_exit, a_rev, a_stop) = (
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
            AllocationId::new(),
        );

        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        for (att, alloc, state) in [
            (att_adopt, a_adopt, AttemptState::Dispatching),
            (att_disp_lost, a_disp_lost, AttemptState::Dispatching),
            (att_exit, a_exit, AttemptState::Running),
            (att_rev, a_rev, AttemptState::Running),
        ] {
            sm.attempts.insert(
                att,
                attempt_record(
                    att,
                    job,
                    alloc,
                    node,
                    state,
                    Some(now() - Duration::from_micros(500)),
                ),
            );
            sm.allocations.insert(
                alloc,
                allocation_record(alloc, job, att, node, requested(), AllocationState::Active),
            );
        }
        let view = view_of(sm);

        let set = Body::ObservedSet(ObservedSet {
            allocations: vec![
                observed(a_adopt, att_adopt, true, None, 0),
                observed(a_stop, AttemptId::new(), true, None, 0), // no live intent
                observed(
                    a_exit,
                    att_exit,
                    false,
                    Some(&AttemptOutcome::Exited { code: 0 }),
                    7,
                ),
                observed(a_rev, att_rev, false, Some(&AttemptOutcome::Revoked), 3),
            ],
        });

        let out = normalize(&view, &report(node, 5, set), now());

        // Orphaned running container -> stop.
        assert_eq!(out.stops, vec![a_stop]);

        // Observed exit for att_exit records its real outcome.
        assert!(out.commands.iter().any(|c| matches!(
            c,
            Command::RecordAttemptOutcome(o)
                if o.attempt == att_exit
                    && o.outcome == AttemptOutcome::Exited { code: 0 }
                    && o.actual_runtime == Duration::from_micros(7)
        )));
        // Revoked from an agent is substituted with AgentError.
        assert!(out.commands.iter().any(|c| matches!(
            c,
            Command::RecordAttemptOutcome(o)
                if o.attempt == att_rev && o.outcome == AttemptOutcome::AgentError
        )));
        // ReconcileNode: adopt att_adopt, lose the Dispatching attempt absent
        // from the set (Dispatching-absent IS lost for an ObservedSet).
        let rn = out
            .commands
            .iter()
            .find_map(|c| match c {
                Command::ReconcileNode(rn) => Some(rn),
                _ => None,
            })
            .expect("a ReconcileNode");
        assert_eq!(rn.adopted, vec![att_adopt]);
        assert_eq!(rn.lost.len(), 1);
        assert_eq!(rn.lost[0].attempt, att_disp_lost);
    }

    #[test]
    fn observed_set_stale_epoch_emits_stops_only() {
        let node = NodeId::new();
        let job = JobId::new();
        let att_disp = AttemptId::new();
        let (a_live, a_stop) = (AllocationId::new(), AllocationId::new());

        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        sm.attempts.insert(
            att_disp,
            attempt_record(att_disp, job, a_live, node, AttemptState::Dispatching, None),
        );
        sm.allocations.insert(
            a_live,
            allocation_record(
                a_live,
                job,
                att_disp,
                node,
                requested(),
                AllocationState::Active,
            ),
        );
        let view = view_of(sm);

        // Stale epoch (report 4 != current 5).
        let set = Body::ObservedSet(ObservedSet {
            allocations: vec![
                observed(a_live, att_disp, true, None, 0), // live intent -> ignored under stale
                observed(a_stop, AttemptId::new(), true, None, 0), // no intent -> stop
            ],
        });
        let out = normalize(&view, &report(node, 4, set), now());

        assert_eq!(out.stops, vec![a_stop]);
        assert!(
            out.commands.is_empty(),
            "stale ObservedSet emits no commands"
        );
    }

    #[test]
    fn unknown_node_heartbeat_is_dropped() {
        let node = NodeId::new();
        let view = view_of(StateMachine::default());
        let out = normalize(&view, &report(node, 1, heartbeat(&[])), now());
        assert!(out.commands.is_empty());
        assert!(out.stops.is_empty());
        assert!(out.liveness);
    }

    #[test]
    fn attempt_status_unknown_attempt_is_dropped() {
        let node = NodeId::new();
        let mut sm = StateMachine::default();
        sm.nodes.insert(node, node_record(node, 5, true));
        let view = view_of(sm);
        let out = normalize(
            &view,
            &report(
                node,
                5,
                attempt_status(AttemptId::new(), &AttemptState::Running, 0),
            ),
            now(),
        );
        assert!(out.commands.is_empty());
    }

    #[tokio::test]
    async fn register_routes_accepted_after_view_catches_up() {
        use coppice_consensus::Consensus;

        use crate::test_support::{FakeConsensus, ProposeOutcome};

        let node = NodeId::new();
        let (consensus, mut publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        let consensus = Arc::new(consensus);
        let views = consensus.views();
        let liveness = NodeLiveness::new();
        let (router, mut router_rx) = RouterHandle::channel_for_test();

        let reg = Body::Register(Register {
            capacity: Some((&requested()).into()),
            labels: vec![],
            service_addr: None,
        });
        let inbound = report(node, 0, reg);

        let task = {
            let consensus = Arc::clone(&consensus);
            let views = views.clone();
            let router = router.clone();
            let liveness = liveness.clone();
            tokio::spawn(
                async move { ingest(&consensus, &views, &router, &liveness, &inbound).await },
            )
        };

        // Let the task propose (FakeConsensus assigns the RegisterNode log
        // index 1), then publish a view at that index so read-your-writes
        // resolves and RegisterAccepted is routed.
        tokio::task::yield_now().await;
        publisher.publish_now(&StateMachine::default(), 1);

        assert_eq!(task.await.unwrap(), None);
        assert!(liveness.last_seen(node).is_some());
        let routed = router_rx.recv().await.expect("RegisterAccepted routed");
        assert_eq!(routed.node, node);
        assert!(matches!(
            routed.command.body,
            Some(coppice_proto::pb::agent::v1::agent_command::Body::RegisterAccepted(_))
        ));
    }
}
