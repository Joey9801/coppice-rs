//! Read-model projections: `StateMachine` → JSON DTOs ([`super::dto`]).
//!
//! These are pure functions of the replicated state, run at read time in
//! the handler (never in apply). Aggregations that scan the allocation or
//! attempt maps are handler-scoped throwaway memos, never stored on the
//! state machine.

use std::collections::BTreeMap;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::id::{ClusterId, NodeId};
use coppice_core::job::JobState;
use coppice_core::resource::Resources;
use coppice_state::{AttemptRecord, JobRecord, StateMachine};

use crate::{QueueWindow, RecentClusterEvents};

use super::dto;

/// How many of the newest closed buckets feed the headline queue rates:
/// 10 × 30 s = a 5-minute window (the full retained hour still ships in
/// `history` for sparklines).
const RATE_WINDOW_BUCKETS: usize = 10;

#[derive(Default)]
struct NodeMemo {
    allocated: Resources,
    running_count: u32,
    accruing_count: u32,
}

fn build_node_memos(state: &StateMachine) -> BTreeMap<NodeId, NodeMemo> {
    let mut memos: BTreeMap<NodeId, NodeMemo> = BTreeMap::new();

    for (_, alloc_record) in &state.allocations {
        let alloc = &alloc_record.allocation;
        if alloc.state.is_terminal() {
            continue;
        }
        let memo = memos.entry(alloc.node).or_default();
        memo.allocated = memo.allocated.saturating_add(&alloc.funded);
        if matches!(alloc.state, AllocationState::Accruing) {
            memo.accruing_count += 1;
        }
    }

    for (_, attempt_record) in &state.attempts {
        if matches!(attempt_record.attempt.state, AttemptState::Running) {
            memos
                .entry(attempt_record.attempt.node)
                .or_default()
                .running_count += 1;
        }
    }

    memos
}

/// Health has no reliable input yet: the replicated state records no loss
/// flag (`DeclareNodeLost` bumps the epoch and clears `schedulable`,
/// indistinguishable from an operator drain) and heartbeat liveness is not
/// wired, so every node reports `Unknown` rather than a fabricated `Healthy`.
///
/// One function so the node views and the cluster's `lost` count can never
/// disagree about what a node's health is.
fn node_health(_record: &coppice_state::NodeRecord) -> dto::NodeHealth {
    dto::NodeHealth::Unknown
}

fn node_summary(
    node_id: &NodeId,
    record: &coppice_state::NodeRecord,
    memo: &NodeMemo,
) -> dto::NodeSummary {
    dto::NodeSummary {
        id: *node_id,
        capacity: (&record.node.capacity).into(),
        allocated: (&memo.allocated).into(),
        used: (&Resources::ZERO).into(),
        labels: record.node.labels.clone(),
        schedulable: record.node.schedulable,
        health: node_health(record),
        epoch: record.epoch,
        last_heartbeat_us: None,
        running_count: memo.running_count,
        accruing_count: memo.accruing_count,
    }
}

pub fn list_nodes(state: &StateMachine) -> dto::ListNodesResponse {
    let memos = build_node_memos(state);
    let empty = NodeMemo::default();

    let nodes = state
        .nodes
        .iter()
        .map(|(id, record)| {
            let memo = memos.get(id).unwrap_or(&empty);
            node_summary(id, record, memo)
        })
        .collect();

    dto::ListNodesResponse { nodes }
}

pub fn get_node(state: &StateMachine, id: &NodeId) -> Option<dto::GetNodeResponse> {
    let record = state.nodes.get(id)?;
    let memos = build_node_memos(state);
    let empty = NodeMemo::default();
    let memo = memos.get(id).unwrap_or(&empty);

    let summary = node_summary(id, record, memo);

    let active_attempts = state
        .attempts
        .iter()
        .filter(|(_, ar)| {
            ar.attempt.node == *id
                && matches!(
                    ar.attempt.state,
                    AttemptState::Dispatching | AttemptState::Running | AttemptState::Finalizing
                )
        })
        .map(|(_, ar)| attempt_view(ar))
        .collect();

    let accrual_queue = state
        .accrual_queue
        .iter()
        .filter(|((node, _), _)| *node == *id)
        .filter_map(|((_, _), alloc_id)| {
            let alloc_record = state.allocations.get(alloc_id)?;
            let alloc = &alloc_record.allocation;
            Some(dto::AccrualView {
                allocation: dto::AllocationView {
                    id: alloc.id,
                    job: alloc.job,
                    attempt: alloc.attempt,
                    node: alloc.node,
                    requested: (&alloc.requested).into(),
                    funded: (&alloc.funded).into(),
                    state: alloc.state.into(),
                    seq: alloc_record.seq,
                },
                funded_fraction: funded_fraction(&alloc.funded, &alloc.requested),
                projected_start_us: None,
            })
        })
        .collect();

    Some(dto::GetNodeResponse {
        summary,
        active_attempts,
        accrual_queue,
    })
}

/// `GET /api/v1/overview`.
///
/// `now_us` is the reader's wall clock, used only for `oldest_queued_age_us`
/// — a *read-time* age, not replicated state (apply never reads a clock).
/// The caller passes it in so this stays a pure function of its inputs, as
/// are the two derived sources: `window` (this replica's queue buckets,
/// ADR 0032 tier 3) and `recent` (its fanout ring's newest events, tier 1).
pub fn cluster_overview(
    state: &StateMachine,
    cluster_id: ClusterId,
    now_us: i64,
    window: &QueueWindow,
    recent: &RecentClusterEvents,
) -> dto::GetClusterOverviewResponse {
    dto::GetClusterOverviewResponse {
        cluster_id,
        queue: queue_stats(state, now_us, window),
        capacity: cluster_capacity(state),
        recent_events: recent_events(recent),
    }
}

fn recent_events(recent: &RecentClusterEvents) -> dto::RecentEventsWindow {
    dto::RecentEventsWindow {
        floor_index: recent.floor_index,
        events: recent
            .events
            .iter()
            .map(|e| dto::TimelineEvent {
                index: e.index,
                ordinal: e.ordinal,
                at_us: e.at_us,
                body: (&e.event).into(),
            })
            .collect(),
    }
}

fn cluster_capacity(state: &StateMachine) -> dto::ClusterCapacity {
    let memos = build_node_memos(state);
    let empty = NodeMemo::default();

    let mut nodes = dto::NodeCounts {
        total: 0,
        schedulable: 0,
        lost: 0,
    };
    let mut capacity = Resources::ZERO;
    let mut allocated = Resources::ZERO;

    for (id, record) in &state.nodes {
        nodes.total += 1;
        // A lost node's capacity is not the cluster's to schedule against, so
        // it is excluded from the total rather than counted and discounted.
        if node_health(record) == dto::NodeHealth::Lost {
            nodes.lost += 1;
        } else {
            capacity = capacity.saturating_add(&record.node.capacity);
            if record.node.schedulable {
                nodes.schedulable += 1;
            }
        }
        // Allocations on a lost node still hold their funding until the loss
        // is reconciled, so they count wherever the node does.
        let memo = memos.get(id).unwrap_or(&empty);
        allocated = allocated.saturating_add(&memo.allocated);
    }

    dto::ClusterCapacity {
        nodes,
        capacity: (&capacity).into(),
        allocated: (&allocated).into(),
        // The sum of the per-node zeros of `NodeSummary::used`: no agent
        // telemetry exists to measure actual consumption yet.
        used: (&Resources::ZERO).into(),
    }
}

fn queue_stats(state: &StateMachine, now_us: i64, window: &QueueWindow) -> dto::QueueStats {
    // Seeded with every phase so the response reports a count for each one,
    // zeros included.
    let mut by_state: BTreeMap<dto::JobPhase, u32> =
        dto::JobPhase::ALL.iter().map(|phase| (*phase, 0)).collect();
    let mut oldest_queued_age_us: Option<i64> = None;

    for (_, record) in &state.jobs {
        *by_state.entry(job_phase(state, record)).or_default() += 1;

        if record.state == JobState::Queued {
            // A `submitted_at_us` in the future (proposer clock skew) is an
            // age of zero, never a negative one.
            let age = now_us.saturating_sub(record.submitted_at_us).max(0);
            oldest_queued_age_us = Some(oldest_queued_age_us.map_or(age, |old| old.max(age)));
        }
    }

    let (arrival_rate_per_minute, drain_rate_per_minute) = queue_rates(window);

    dto::QueueStats {
        depth: by_state[&dto::JobPhase::Queued],
        drain_rate_per_minute,
        arrival_rate_per_minute,
        oldest_queued_age_us,
        by_state,
        history: queue_history(window),
    }
}

/// Headline `(arrivals, drains)` per minute over the newest
/// [`RATE_WINDOW_BUCKETS`] closed buckets; `None` when the window has no
/// coverage at all (never a fabricated `0.0` — see `dto::QueueStats`).
fn queue_rates(window: &QueueWindow) -> (Option<f64>, Option<f64>) {
    if window.buckets.is_empty() || window.bucket_us <= 0 {
        return (None, None);
    }
    let newest = &window.buckets[window.buckets.len().saturating_sub(RATE_WINDOW_BUCKETS)..];
    let minutes = (newest.len() as f64 * window.bucket_us as f64) / 60_000_000.0;
    let arrivals: u64 = newest.iter().map(|b| u64::from(b.arrivals)).sum();
    let drains: u64 = newest.iter().map(|b| u64::from(b.drains)).sum();
    (
        Some(arrivals as f64 / minutes),
        Some(drains as f64 / minutes),
    )
}

/// Every retained bucket as a history sample, oldest first. Missing
/// coverage is a missing sample (its `t_us` never appears), never a zero.
fn queue_history(window: &QueueWindow) -> Vec<dto::QueueSample> {
    if window.bucket_us <= 0 {
        return Vec::new();
    }
    let per_minute = 60_000_000.0 / window.bucket_us as f64;
    window
        .buckets
        .iter()
        .map(|b| dto::QueueSample {
            t_us: b.start_us,
            depth: b.depth,
            drained_per_minute: f64::from(b.drains) * per_minute,
            arrived_per_minute: f64::from(b.arrivals) * per_minute,
        })
        .collect()
}

/// The read-time join of a job's state with its attempt's (ADR 0030).
///
/// An `Attempting` job whose attempt is missing from the map cannot happen
/// (apply mints the attempt in the same command), but it reads as
/// `Finalizing` — the same as a `Terminal` attempt — rather than panicking on
/// a state a future command shape might produce.
fn job_phase(state: &StateMachine, record: &JobRecord) -> dto::JobPhase {
    let attempt = match record.state {
        JobState::Submitted => return dto::JobPhase::Submitted,
        JobState::Accepted => return dto::JobPhase::Accepted,
        JobState::Queued => return dto::JobPhase::Queued,
        JobState::Succeeded => return dto::JobPhase::Succeeded,
        JobState::Failed => return dto::JobPhase::Failed,
        JobState::Aborted => return dto::JobPhase::Aborted,
        JobState::Attempting(attempt) => attempt,
    };

    match state.attempts.get(&attempt).map(|ar| &ar.attempt.state) {
        Some(AttemptState::Accruing | AttemptState::Ready | AttemptState::Dispatching) => {
            dto::JobPhase::Preparing
        }
        Some(AttemptState::Running) => dto::JobPhase::Running,
        Some(AttemptState::Finalizing | AttemptState::Terminal(_)) | None => {
            dto::JobPhase::Finalizing
        }
    }
}

fn attempt_view(ar: &AttemptRecord) -> dto::AttemptView {
    dto::AttemptView {
        id: ar.attempt.id,
        job: ar.attempt.job,
        node: ar.attempt.node,
        allocation: ar.attempt.allocation,
        state: (&ar.attempt.state).into(),
        outcome: match &ar.attempt.state {
            AttemptState::Terminal(outcome) => Some(outcome.into()),
            _ => None,
        },
        started_at_us: ar.started_at_us,
        ended_at_us: None,
        rate_ucu_per_second: ar.rate_ucu_per_second,
        charged_ucu: ar.charge.amount.0,
    }
}

fn funded_fraction(funded: &Resources, requested: &Resources) -> dto::FundedFraction {
    let frac = |funded: u64, requested: u64| -> f64 {
        if requested == 0 {
            1.0
        } else {
            funded as f64 / requested as f64
        }
    };
    dto::FundedFraction {
        cpu: frac(funded.cpu_millis, requested.cpu_millis),
        memory: frac(funded.memory_bytes, requested.memory_bytes),
        disk: frac(funded.disk_bytes, requested.disk_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::allocation::Allocation;
    use coppice_core::attempt::Attempt;
    use coppice_core::id::GroupId;
    use coppice_core::id::{AllocationId, AttemptId, JobId, QuotaEntityId};
    use coppice_core::node::Node;
    use coppice_core::quota::{ChargeRecord, CostUnits, PriorityMultiplier, FULL_REFUND_MILLI};
    use coppice_state::{AllocationRecord, AttemptRecord, NodeRecord};

    fn test_node(id: NodeId) -> NodeRecord {
        NodeRecord {
            node: Node {
                id,
                capacity: Resources {
                    cpu_millis: 4000,
                    memory_bytes: 8_000_000_000,
                    disk_bytes: 100_000_000_000,
                },
                labels: BTreeMap::new(),
                schedulable: true,
            },
            epoch: 1,
        }
    }

    fn test_attempt(id: AttemptId, job: JobId, node: NodeId, state: AttemptState) -> AttemptRecord {
        AttemptRecord {
            attempt: Attempt {
                id,
                job,
                allocation: AllocationId::new(),
                node,
                state,
            },
            group: GroupId(job.0),
            charge: ChargeRecord {
                amount: CostUnits(1000),
                charged_at_us: 0,
                refund_fraction_milli: FULL_REFUND_MILLI,
            },
            rate_ucu_per_second: 100,
            multiplier: PriorityMultiplier::ONE,
            started_at_us: Some(1000),
        }
    }

    fn test_allocation(
        id: AllocationId,
        job: JobId,
        attempt: AttemptId,
        node: NodeId,
        state: AllocationState,
    ) -> AllocationRecord {
        AllocationRecord {
            allocation: Allocation {
                id,
                job,
                attempt,
                node,
                requested: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 1_000_000,
                    disk_bytes: 0,
                },
                funded: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 1_000_000,
                    disk_bytes: 0,
                },
                state,
            },
            seq: 1,
        }
    }

    #[test]
    fn list_nodes_returns_empty_for_no_nodes() {
        let state = StateMachine::default();
        let response = list_nodes(&state);
        assert!(response.nodes.is_empty());
    }

    #[test]
    fn list_nodes_includes_all_registered_nodes() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let mut state = StateMachine::default();
        state.nodes.insert(n1, test_node(n1));
        state.nodes.insert(n2, test_node(n2));

        let response = list_nodes(&state);
        assert_eq!(response.nodes.len(), 2);
    }

    #[test]
    fn list_nodes_counts_running_and_accruing() {
        let node = NodeId::new();
        let job = JobId::new();
        let attempt_running = AttemptId::new();
        let attempt_accruing = AttemptId::new();
        let alloc_active = AllocationId::new();
        let alloc_accruing = AllocationId::new();

        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));

        state.attempts.insert(
            attempt_running,
            test_attempt(attempt_running, job, node, AttemptState::Running),
        );
        state.attempts.insert(
            attempt_accruing,
            test_attempt(attempt_accruing, job, node, AttemptState::Accruing),
        );
        state.allocations.insert(
            alloc_active,
            test_allocation(
                alloc_active,
                job,
                attempt_running,
                node,
                AllocationState::Active,
            ),
        );
        state.allocations.insert(
            alloc_accruing,
            test_allocation(
                alloc_accruing,
                job,
                attempt_accruing,
                node,
                AllocationState::Accruing,
            ),
        );

        let response = list_nodes(&state);
        assert_eq!(response.nodes.len(), 1);
        let summary = &response.nodes[0];
        assert_eq!(summary.running_count, 1);
        assert_eq!(summary.accruing_count, 1);
    }

    #[test]
    fn nodes_report_unknown_health_until_liveness_exists() {
        let node = NodeId::new();
        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));

        let response = list_nodes(&state);
        assert_eq!(response.nodes[0].health, dto::NodeHealth::Unknown);
    }

    #[test]
    fn get_node_returns_none_for_missing() {
        let state = StateMachine::default();
        assert!(get_node(&state, &NodeId::new()).is_none());
    }

    #[test]
    fn get_node_returns_active_attempts_and_accrual_queue() {
        let node = NodeId::new();
        let job = JobId::new();
        let attempt = AttemptId::new();
        let alloc = AllocationId::new();

        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));
        state.attempts.insert(
            attempt,
            test_attempt(attempt, job, node, AttemptState::Running),
        );
        state.allocations.insert(
            alloc,
            test_allocation(alloc, job, attempt, node, AllocationState::Active),
        );

        let response = get_node(&state, &node).unwrap();
        assert_eq!(response.active_attempts.len(), 1);
        assert_eq!(response.active_attempts[0].rate_ucu_per_second, 100);
        assert_eq!(response.active_attempts[0].outcome, None);
    }

    fn test_job(id: JobId, state: JobState, submitted_at_us: i64) -> coppice_state::JobRecord {
        coppice_state::JobRecord {
            spec: coppice_core::job::Job {
                id,
                image: "busybox".to_string(),
                command: vec!["run".to_string()],
                entrypoint: None,
                requests: Resources::ZERO,
                priority: 0,
                max_runtime_us: None,
                quota_entity: QuotaEntityId::new(),
                retry: Default::default(),
                abort_requested: None,
            },
            state,
            multiplier: PriorityMultiplier::ONE,
            submitted_at_us,
            terminal_at_us: None,
            retries_used: 0,
            attempts: Vec::new(),
        }
    }

    const CLUSTER: &str = "cluster-00000000-0000-0000-0000-000000000009";

    fn cluster() -> coppice_core::id::ClusterId {
        CLUSTER.parse().unwrap()
    }

    fn no_recent() -> RecentClusterEvents {
        RecentClusterEvents {
            floor_index: 0,
            events: Vec::new(),
        }
    }

    /// [`cluster_overview`] with empty derived sources — what a replica with
    /// no bucket or ring coverage serves.
    fn overview(state: &StateMachine, now_us: i64) -> dto::GetClusterOverviewResponse {
        cluster_overview(
            state,
            cluster(),
            now_us,
            &QueueWindow::default(),
            &no_recent(),
        )
    }

    #[test]
    fn overview_of_an_empty_cluster_counts_nothing() {
        let response = overview(&StateMachine::default(), 1_000);

        assert_eq!(response.cluster_id, cluster());
        assert_eq!(response.queue.depth, 0);
        assert_eq!(response.queue.oldest_queued_age_us, None);
        assert_eq!(response.capacity.nodes.total, 0);
        // Every phase is reported, at zero — never an absent key.
        assert_eq!(response.queue.by_state.len(), dto::JobPhase::ALL.len());
        assert!(response.queue.by_state.values().all(|count| *count == 0));
    }

    #[test]
    fn overview_sums_capacity_over_nodes_and_allocations() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let job = JobId::new();
        let attempt = AttemptId::new();
        let alloc = AllocationId::new();

        let mut state = StateMachine::default();
        state.nodes.insert(n1, test_node(n1));
        let mut draining = test_node(n2);
        draining.node.schedulable = false;
        state.nodes.insert(n2, draining);
        state.allocations.insert(
            alloc,
            test_allocation(alloc, job, attempt, n1, AllocationState::Active),
        );

        let capacity = overview(&state, 0).capacity;

        assert_eq!(capacity.nodes.total, 2);
        // A draining node is registered capacity but not schedulable.
        assert_eq!(capacity.nodes.schedulable, 1);
        assert_eq!(capacity.nodes.lost, 0);
        assert_eq!(capacity.capacity.cpu_millis, 8000);
        assert_eq!(capacity.allocated.cpu_millis, 1000);
        // No telemetry: measured use is zero, as on every node summary.
        assert_eq!(capacity.used.cpu_millis, 0);
    }

    #[test]
    fn overview_tallies_jobs_by_displayed_phase() {
        let node = NodeId::new();
        let running_job = JobId::new();
        let preparing_job = JobId::new();
        let running_attempt = AttemptId::new();
        let accruing_attempt = AttemptId::new();

        let queued_job = JobId::new();
        let mut state = StateMachine::default();
        state
            .jobs
            .insert(queued_job, test_job(queued_job, JobState::Queued, 0));
        state.jobs.insert(
            running_job,
            test_job(running_job, JobState::Attempting(running_attempt), 0),
        );
        state.jobs.insert(
            preparing_job,
            test_job(preparing_job, JobState::Attempting(accruing_attempt), 0),
        );
        state.attempts.insert(
            running_attempt,
            test_attempt(running_attempt, running_job, node, AttemptState::Running),
        );
        state.attempts.insert(
            accruing_attempt,
            test_attempt(
                accruing_attempt,
                preparing_job,
                node,
                AttemptState::Accruing,
            ),
        );

        let queue = overview(&state, 0).queue;

        // `Attempting` is never reported raw: an accruing attempt reads as
        // `Preparing`, a running one as `Running` (ADR 0030's read-time join).
        assert_eq!(queue.by_state[&dto::JobPhase::Queued], 1);
        assert_eq!(queue.by_state[&dto::JobPhase::Preparing], 1);
        assert_eq!(queue.by_state[&dto::JobPhase::Running], 1);
        assert_eq!(queue.depth, 1);
    }

    #[test]
    fn an_attempting_job_whose_attempt_is_terminal_reads_as_finalizing() {
        let node = NodeId::new();
        let job = JobId::new();
        let attempt = AttemptId::new();

        let mut state = StateMachine::default();
        state
            .jobs
            .insert(job, test_job(job, JobState::Attempting(attempt), 0));
        state.attempts.insert(
            attempt,
            test_attempt(
                attempt,
                job,
                node,
                AttemptState::Terminal(coppice_core::attempt::AttemptOutcome::Exited { code: 0 }),
            ),
        );

        let queue = overview(&state, 0).queue;
        assert_eq!(queue.by_state[&dto::JobPhase::Finalizing], 1);
    }

    #[test]
    fn oldest_queued_age_is_the_longest_wait_at_read_time() {
        let old = JobId::new();
        let recent = JobId::new();
        let running = JobId::new();

        let mut state = StateMachine::default();
        state
            .jobs
            .insert(old, test_job(old, JobState::Queued, 1_000));
        state
            .jobs
            .insert(recent, test_job(recent, JobState::Queued, 9_000));
        // A job that has left the queue no longer ages it, however old.
        state
            .jobs
            .insert(running, test_job(running, JobState::Succeeded, 0));

        let queue = overview(&state, 10_000).queue;
        assert_eq!(queue.oldest_queued_age_us, Some(9_000));
    }

    #[test]
    fn a_submission_timestamp_in_the_future_ages_to_zero_never_negative() {
        // Proposer clock skew: `submitted_at_us` rides in on the command, so a
        // reader's clock can legitimately be behind it.
        let job = JobId::new();
        let mut state = StateMachine::default();
        state
            .jobs
            .insert(job, test_job(job, JobState::Queued, 5_000));

        let queue = overview(&state, 1_000).queue;
        assert_eq!(queue.oldest_queued_age_us, Some(0));
    }

    fn window_of(buckets: Vec<crate::QueueBucket>) -> QueueWindow {
        QueueWindow {
            bucket_us: 30_000_000,
            buckets,
        }
    }

    fn bucket(start_us: i64, depth: u32, arrivals: u32, drains: u32) -> crate::QueueBucket {
        crate::QueueBucket {
            start_us,
            depth,
            arrivals,
            drains,
        }
    }

    #[test]
    fn queue_rates_are_per_minute_over_the_covered_window() {
        // Two closed 30 s buckets = one covered minute: 3 arrivals and 1
        // drain in it are exactly those per-minute rates.
        let window = window_of(vec![bucket(0, 5, 2, 1), bucket(30_000_000, 6, 1, 0)]);
        let (arrivals, drains) = queue_rates(&window);
        assert_eq!(arrivals, Some(3.0));
        assert_eq!(drains, Some(1.0));
    }

    #[test]
    fn queue_rates_use_only_the_newest_rate_window_buckets() {
        // An old burst outside the 10-bucket rate window must not inflate
        // the headline rate (it still ships in `history`).
        let mut buckets = vec![bucket(0, 0, 600, 600)];
        for i in 1..=(RATE_WINDOW_BUCKETS as i64) {
            buckets.push(bucket(i * 30_000_000, 0, 1, 0));
        }
        let (arrivals, drains) = queue_rates(&window_of(buckets));
        // 10 buckets × 30 s = 5 minutes, 10 arrivals → 2/min.
        assert_eq!(arrivals, Some(2.0));
        assert_eq!(drains, Some(0.0));
    }

    #[test]
    fn queue_history_carries_every_retained_bucket_scaled_per_minute() {
        let window = window_of(vec![bucket(0, 5, 2, 1), bucket(30_000_000, 6, 0, 3)]);
        let history = queue_history(&window);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].t_us, 0);
        assert_eq!(history[0].depth, 5);
        assert_eq!(history[0].arrived_per_minute, 4.0);
        assert_eq!(history[0].drained_per_minute, 2.0);
        assert_eq!(history[1].t_us, 30_000_000);
        assert_eq!(history[1].drained_per_minute, 6.0);
    }

    #[test]
    fn overview_serves_rates_and_history_from_the_window() {
        let window = window_of(vec![bucket(0, 5, 2, 1)]);
        let queue = cluster_overview(
            &StateMachine::default(),
            cluster(),
            0,
            &window,
            &no_recent(),
        )
        .queue;
        assert_eq!(queue.arrival_rate_per_minute, Some(4.0));
        assert_eq!(queue.drain_rate_per_minute, Some(2.0));
        assert_eq!(queue.history.len(), 1);
    }

    #[test]
    fn recent_events_keep_their_identity_and_stamp() {
        let job = JobId::new();
        let recent = RecentClusterEvents {
            floor_index: 4,
            events: vec![crate::StampedEvent {
                index: 9,
                ordinal: 3,
                at_us: 1_234,
                event: coppice_state::Event::JobSubmitted { job },
            }],
        };
        let rendered = cluster_overview(
            &StateMachine::default(),
            cluster(),
            0,
            &QueueWindow::default(),
            &recent,
        )
        .recent_events;
        assert_eq!(rendered.floor_index, 4);
        assert_eq!(rendered.events.len(), 1);
        let event = &rendered.events[0];
        assert_eq!((event.index, event.ordinal, event.at_us), (9, 3, 1_234));
        assert_eq!(event.body, dto::TimelineEventBody::JobSubmitted { job });
    }

    #[test]
    fn queue_rates_and_history_are_absent_not_zero() {
        // A window with no coverage (fresh replica, or one that just lost
        // the event stream); `0.0` would claim "nothing is draining" (see
        // `dto::QueueStats`).
        let queue = overview(&StateMachine::default(), 0).queue;
        assert_eq!(queue.drain_rate_per_minute, None);
        assert_eq!(queue.arrival_rate_per_minute, None);
        assert!(queue.history.is_empty());
    }

    #[test]
    fn funded_fraction_handles_zero_requested() {
        let ff = funded_fraction(&Resources::ZERO, &Resources::ZERO);
        assert_eq!(ff.cpu, 1.0);
        assert_eq!(ff.memory, 1.0);
        assert_eq!(ff.disk, 1.0);
    }
}
