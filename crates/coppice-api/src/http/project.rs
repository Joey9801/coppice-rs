//! Read-model projections: `StateMachine` → JSON DTOs ([`super::dto`]).
//!
//! These are pure functions of the replicated state, run at read time in
//! the handler (never in apply). Aggregations that scan the allocation or
//! attempt maps are handler-scoped throwaway memos, never stored on the
//! state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::bytes::ByteSize;
use coppice_core::id::{ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::JobState;
use coppice_core::quota::{self, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::{
    AttemptRecord, JobRecord, PolicyConfig, QuotaEntity, StateMachine, QUOTA_TREE_DEPTH_CAP,
};

use crate::{CoordinatorSummary, JobTimelineWindow, QueueWindow, RecentClusterEvents};

use super::dto;

/// How many of the newest closed buckets feed the headline queue rates:
/// nominally 10 × 30 s = a 5-minute window (buckets record their actual
/// span, so a stall-stretched bucket widens the window rather than skewing
/// the rate). The full retained hour still ships in `history`.
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
        last_heartbeat: None,
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
                projected_start: None,
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
/// `now` is the reader's wall clock, used only for `oldest_queued_age_seconds`
/// — a *read-time* age, not replicated state (apply never reads a clock).
/// The caller passes it in so this stays a pure function of its inputs, as
/// are the two derived sources: `window` (this replica's queue buckets,
/// ADR 0032 tier 3) and `recent` (its fanout ring's newest events, tier 1).
pub fn cluster_overview(
    state: &StateMachine,
    cluster_id: ClusterId,
    now: Timestamp,
    window: &QueueWindow,
    recent: &RecentClusterEvents,
) -> dto::GetClusterOverviewResponse {
    dto::GetClusterOverviewResponse {
        cluster_id,
        queue: queue_stats(state, now, window),
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
                at: e.at,
                body: (&e.event).into(),
            })
            .collect(),
    }
}

/// `GET /api/v1/jobs/{job}/timeline` — project the ring window (ADR 0032,
/// tier 1) onto the wire shape, ascending by `(index, ordinal)`. Pure over
/// the window: the 404-vs-empty verdict and cursor parsing stay in the
/// handler. Mirrors [`recent_events`], and the `next` content coordinate
/// becomes the opaque [`dto::TimelineCursor`].
pub fn job_timeline(window: &JobTimelineWindow) -> dto::GetJobTimelineResponse {
    dto::GetJobTimelineResponse {
        events: window
            .events
            .iter()
            .map(|e| dto::TimelineEvent {
                index: e.index,
                ordinal: e.ordinal,
                at: e.at,
                body: (&e.event).into(),
            })
            .collect(),
        floor_index: window.floor_index,
        next_cursor: window.next.map(dto::TimelineCursor::format),
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

/// `GET /api/v1/queue/stats`. The `queue` field of the overview, served on
/// its own (the UI's `QueueStats` is byte-for-byte that object). Same derived
/// inputs as [`cluster_overview`]'s queue block: `now` for the read-time
/// oldest-queued age, `window` for the replica-local rates/history.
pub(crate) fn queue_stats(
    state: &StateMachine,
    now: Timestamp,
    window: &QueueWindow,
) -> dto::QueueStats {
    // Seeded with every phase so the response reports a count for each one,
    // zeros included.
    let mut by_state: BTreeMap<dto::JobPhase, u32> =
        dto::JobPhase::ALL.iter().map(|phase| (*phase, 0)).collect();
    let mut oldest_queued_age: Option<Duration> = None;

    for (_, record) in &state.jobs {
        *by_state.entry(job_phase(state, record)).or_default() += 1;

        if record.state == JobState::Queued {
            // A `submitted_at` in the future (proposer clock skew) is an age
            // of zero, never a negative one.
            let age = (now - record.submitted_at).max(Duration::ZERO);
            oldest_queued_age = Some(oldest_queued_age.map_or(age, |old| old.max(age)));
        }
    }

    let rates = queue_rates(window);

    dto::QueueStats {
        depth: by_state[&dto::JobPhase::Queued],
        drain_rate_per_minute: rates.drains_per_minute,
        arrival_rate_per_minute: rates.arrivals_per_minute,
        oldest_queued_age_seconds: oldest_queued_age.map(Duration::as_secs),
        by_state,
        history: queue_history(window),
    }
}

/// Headline queue rates per minute over the newest closed buckets. Each field
/// is `None` when the window covers no time at all — never a fabricated `0.0`
/// (see `dto::QueueStats`).
struct QueueRates {
    /// Jobs enqueued per minute, scaled by the buckets' recorded spans.
    arrivals_per_minute: Option<f64>,
    /// Jobs drained per minute, scaled the same way.
    drains_per_minute: Option<f64>,
}

/// Headline [`QueueRates`] (`arrivals_per_minute`, `drains_per_minute`) over
/// the newest [`RATE_WINDOW_BUCKETS`] closed buckets, scaled by their
/// *recorded* spans — a stall-stretched bucket contributes its real coverage,
/// never an assumed 30 s. Each field is `None` when the window covers no time
/// at all (never a fabricated `0.0` — see `dto::QueueStats`).
fn queue_rates(window: &QueueWindow) -> QueueRates {
    let newest = &window.buckets[window.buckets.len().saturating_sub(RATE_WINDOW_BUCKETS)..];
    let covered: Duration = newest
        .iter()
        .map(|b| (b.end - b.start).max(Duration::ZERO))
        .sum();
    if !covered.is_positive() {
        return QueueRates {
            arrivals_per_minute: None,
            drains_per_minute: None,
        };
    }
    let minutes = covered.as_secs_f64() / 60.0;
    let arrivals: u64 = newest.iter().map(|b| u64::from(b.arrivals)).sum();
    let drains: u64 = newest.iter().map(|b| u64::from(b.drains)).sum();
    QueueRates {
        arrivals_per_minute: Some(arrivals as f64 / minutes),
        drains_per_minute: Some(drains as f64 / minutes),
    }
}

/// Every retained bucket as a history sample, oldest first, each scaled by
/// its own recorded span. Missing coverage is a missing sample (its `t`
/// never appears), never a zero; a degenerate zero-length bucket has no
/// honest rate and is skipped.
fn queue_history(window: &QueueWindow) -> Vec<dto::QueueSample> {
    window
        .buckets
        .iter()
        .filter(|b| b.end > b.start)
        .map(|b| {
            let per_minute = 60.0 / (b.end - b.start).as_secs_f64();
            dto::QueueSample {
                t: b.start,
                depth: b.depth,
                drained_per_minute: f64::from(b.drains) * per_minute,
                arrived_per_minute: f64::from(b.arrivals) * per_minute,
            }
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
        started_at: ar.started_at,
        ended_at: None,
        rate_ucu_per_second: ar.rate_ucu_per_second,
        charged_ucu: ar.charge.amount.0,
    }
}

fn funded_fraction(funded: &Resources, requested: &Resources) -> dto::FundedFraction {
    let frac = |funded: f64, requested: f64| -> f64 {
        if requested == 0.0 {
            1.0
        } else {
            funded / requested
        }
    };
    // Sizes widen through `u128` before the float: the ratio is fractional by
    // nature, and `u128` keeps a maxed-out size from wrapping on the way in.
    let size = |s: ByteSize| s.as_u128() as f64;
    dto::FundedFraction {
        cpu: frac(funded.cpu_millis as f64, requested.cpu_millis as f64),
        memory: frac(size(funded.memory), size(requested.memory)),
        disk: frac(size(funded.disk), size(requested.disk)),
    }
}

// ---------------------------------------------------------------------------
// Coordinators (GET /api/v1/coordinators)
// ---------------------------------------------------------------------------

/// `GET /api/v1/coordinators`.
///
/// Joins the consensus/membership `summary` (leader, term, indexes, roster)
/// with a replica-local `state` snapshot (version + object counts) and the
/// replica's own `cluster_id`. Pure: role, per-member lag, and the snapshot
/// derivation are computed here from the inputs, never stored.
pub fn coordinator_status(
    summary: &CoordinatorSummary,
    cluster_id: ClusterId,
    state: &StateMachine,
) -> dto::GetCoordinatorStatusResponse {
    let members = summary
        .members
        .iter()
        .map(|m| {
            let role = if summary.leader == Some(m.id) {
                dto::CoordinatorRole::Leader
            } else if !m.voter {
                dto::CoordinatorRole::Learner
            } else {
                dto::CoordinatorRole::Follower
            };
            dto::CoordinatorMember {
                id: m.id,
                addr: m.addr.clone(),
                role,
                voter: m.voter,
                // Exact for the serving replica; unknowable for peers (their
                // apply progress is not tracked — only their replicated index).
                last_applied: (m.id == summary.local_id).then_some(summary.last_applied),
                // Leader-only, from the matched (replicated) index.
                replication_lag_entries: m
                    .matched_index
                    .map(|matched| summary.known_committed.saturating_sub(matched)),
            }
        })
        .collect();

    // A snapshot section only when this replica has actually taken one; size
    // and time have no source, so they are null (see `dto::CoordinatorSnapshot`).
    let snapshot =
        summary
            .snapshot_last_index
            .map(|last_included_index| dto::CoordinatorSnapshot {
                size_bytes: None,
                last_included_index,
                taken_at: None,
                entries_since_snapshot: summary.last_applied.saturating_sub(last_included_index),
            });

    dto::GetCoordinatorStatusResponse {
        cluster_id,
        leader: summary.leader,
        term: summary.term,
        known_committed: summary.known_committed,
        last_applied: summary.last_applied,
        state_version: state.version,
        snapshot,
        state_counts: dto::CoordinatorStateCounts {
            jobs: state.jobs.len() as u64,
            attempts: state.attempts.len() as u64,
            allocations: state.allocations.len() as u64,
            nodes: state.nodes.len() as u64,
            quota_entities: state.quota_entities.len() as u64,
        },
        members,
    }
}

// ---------------------------------------------------------------------------
// Job list (GET /api/v1/jobs)
// ---------------------------------------------------------------------------

/// Records examined per request before a page is returned short with a
/// cursor (ADR 0031 as amended). The bound keeps a filter that matches
/// little against a huge job map from turning one read into an unbounded
/// scan; the client continues from `next_cursor`.
const JOB_SCAN_BUDGET: usize = 100_000;

/// `GET /api/v1/jobs`.
///
/// Descending scan of `state.jobs` (JobId order ≈ newest-submitted first,
/// UUIDv7) starting strictly below `cursor`, evaluating `filter` per record
/// and collecting matches until `limit`. See [`list_jobs_scan`] for the
/// cursor and budget semantics.
pub fn list_jobs(
    state: &StateMachine,
    filter: Option<&dto::JobFilter>,
    cursor: Option<JobId>,
    limit: usize,
) -> dto::ListJobsResponse {
    list_jobs_scan(state, filter, cursor, limit, JOB_SCAN_BUDGET)
}

/// [`list_jobs`] with the scan budget injected, so the short-page-with-cursor
/// path is testable without a 100 000-record fixture.
///
/// `next_cursor` is the token for the **last record examined** (matched or
/// not) and is `null` iff the scan reached the low end of the map — i.e. the
/// descending iterator was exhausted rather than cut off by `limit` or the
/// budget. Continuing from a non-null cursor resumes strictly below that id,
/// so a new head inserted between requests is never skipped and an already
/// returned id is never repeated.
fn list_jobs_scan(
    state: &StateMachine,
    filter: Option<&dto::JobFilter>,
    cursor: Option<JobId>,
    limit: usize,
    budget: usize,
) -> dto::ListJobsResponse {
    // Per-request memo: the descendant id set of every entity-subtree leaf,
    // computed once (BFS over the bounded `quota_entities` tree), never a
    // field on the state machine.
    let ctx = FilterContext::build(state, filter);

    // `imbl::OrdMap::range` is double-ended, so a strictly-below-cursor
    // descending walk is `range((Unbounded, Excluded(cursor))).rev()` — the
    // whole map is never collected.
    let upper = cursor.map_or(Bound::Unbounded, Bound::Excluded);
    let iter = state.jobs.range((Bound::Unbounded, upper));

    let mut jobs = Vec::new();
    let mut last_examined: Option<JobId> = None;
    // Stays true only if the iterator runs out on its own; any early break
    // (page full or budget spent) means more records may lie below.
    let mut exhausted = true;
    // `examined` is the count already inspected, so `examined >= budget`
    // stops the scan after exactly `budget` records.
    for (examined, (id, record)) in iter.rev().enumerate() {
        if jobs.len() >= limit || examined >= budget {
            exhausted = false;
            break;
        }
        last_examined = Some(*id);
        if filter.map_or(true, |f| ctx.matches(state, f, record)) {
            jobs.push(job_summary(state, record));
        }
    }

    dto::ListJobsResponse {
        next_cursor: if exhausted {
            None
        } else {
            last_examined.map(dto::JobCursor::format)
        },
        jobs,
    }
}

/// Per-request evaluation memo for a [`dto::JobFilter`].
///
/// Holds the precomputed descendant id set of each entity-subtree leaf so
/// filter evaluation is O(1) per job after this bounded setup. An unknown
/// entity id resolves to an empty set (matches nothing), never an error.
struct FilterContext {
    subtrees: BTreeMap<QuotaEntityId, BTreeSet<QuotaEntityId>>,
}

impl FilterContext {
    fn build(state: &StateMachine, filter: Option<&dto::JobFilter>) -> FilterContext {
        let mut ids = BTreeSet::new();
        if let Some(f) = filter {
            collect_subtree_ids(f, &mut ids);
        }
        let mut subtrees = BTreeMap::new();
        if !ids.is_empty() {
            let children = child_adjacency(state);
            for id in ids {
                subtrees.insert(id, descendant_set(&children, state, id));
            }
        }
        FilterContext { subtrees }
    }

    fn matches(&self, state: &StateMachine, filter: &dto::JobFilter, record: &JobRecord) -> bool {
        use dto::JobFilter as F;
        match filter {
            F::All(fs) => fs.iter().all(|f| self.matches(state, f, record)),
            F::Any(fs) => fs.iter().any(|f| self.matches(state, f, record)),
            F::Not(f) => !self.matches(state, f, record),
            F::Phase(p) => p.r#in.contains(&job_phase(state, record)),
            F::Entity(e) => match e.scope {
                dto::EntityScope::Subtree => self
                    .subtrees
                    .get(&e.id)
                    .is_some_and(|set| set.contains(&record.spec.quota_entity)),
                dto::EntityScope::Exact => record.spec.quota_entity == e.id,
            },
            F::Node(n) => current_attempt_node(state, record) == Some(*n),
            F::Image(dto::ImageFilter::Contains(s)) => record.spec.image.contains(s),
            F::Image(dto::ImageFilter::Equals(s)) => record.spec.image == *s,
            F::Id(i) => i.r#in.contains(&record.spec.id),
            F::Search(s) => {
                let needle = s.to_lowercase();
                record.spec.id.to_string().to_lowercase().contains(&needle)
                    || record.spec.image.to_lowercase().contains(&needle)
            }
            // `after` inclusive (≥), `before` exclusive (<) — the contract's
            // half-open window.
            F::Submitted(sf) => {
                sf.after.map_or(true, |a| record.submitted_at >= a)
                    && sf.before.map_or(true, |b| record.submitted_at < b)
            }
            // Both bounds inclusive.
            F::Requests(r) => {
                // The filter's bounds arrive as bare `uint64` from the JSON
                // filter AST, so the comparison happens in the wire's units.
                let value = match r.resource {
                    dto::RequestsResource::CpuMillis => record.spec.requests.cpu_millis,
                    dto::RequestsResource::MemoryBytes => record.spec.requests.memory.as_u64(),
                    dto::RequestsResource::DiskBytes => record.spec.requests.disk.as_u64(),
                };
                r.min.map_or(true, |m| value >= m) && r.max.map_or(true, |m| value <= m)
            }
        }
    }
}

/// Every entity id referenced by an entity-**subtree** leaf; exact-scope
/// leaves need no descendant set, so they are not collected.
fn collect_subtree_ids(filter: &dto::JobFilter, out: &mut BTreeSet<QuotaEntityId>) {
    use dto::JobFilter as F;
    match filter {
        F::All(fs) | F::Any(fs) => fs.iter().for_each(|f| collect_subtree_ids(f, out)),
        F::Not(f) => collect_subtree_ids(f, out),
        F::Entity(e) if e.scope == dto::EntityScope::Subtree => {
            out.insert(e.id);
        }
        _ => {}
    }
}

/// Parent → children adjacency over the (bounded) quota-entity tree, built
/// once per request that needs a subtree set.
fn child_adjacency(state: &StateMachine) -> BTreeMap<QuotaEntityId, Vec<QuotaEntityId>> {
    let mut children: BTreeMap<QuotaEntityId, Vec<QuotaEntityId>> = BTreeMap::new();
    for (id, entity) in &state.quota_entities {
        if let Some(parent) = entity.parent {
            children.entry(parent).or_default().push(*id);
        }
    }
    children
}

/// The entity and all its descendants (BFS). An id absent from the tree
/// yields the empty set — an unknown entity matches nothing, not itself.
fn descendant_set(
    children: &BTreeMap<QuotaEntityId, Vec<QuotaEntityId>>,
    state: &StateMachine,
    root: QuotaEntityId,
) -> BTreeSet<QuotaEntityId> {
    let mut set = BTreeSet::new();
    if !state.quota_entities.contains_key(&root) {
        return set;
    }
    let mut queue = vec![root];
    while let Some(id) = queue.pop() {
        if set.insert(id) {
            if let Some(kids) = children.get(&id) {
                queue.extend(kids.iter().copied());
            }
        }
    }
    set
}

fn current_attempt_node(state: &StateMachine, record: &JobRecord) -> Option<NodeId> {
    let attempt = record.state.attempt()?;
    state.attempts.get(&attempt).map(|ar| ar.attempt.node)
}

/// µCU charged across a job's attempts.
///
/// This is the gross placement charge summed over every attempt, both while
/// the job is live and once terminal. A terminal job's *net* cost — the
/// charge after the finalization true-up (ADR 0019/0029) — is not
/// recoverable here: `terminate_attempt` settles the refund/surcharge
/// against the quota entity's usage accumulator and retains neither the
/// adjustment nor the actual runtime on the attempt record, so replicated
/// state has no per-job net figure to project. The web mock reports its net
/// `actualUcu` when terminal; the server reports gross until the state
/// carries the settled amount.
fn total_charged(state: &StateMachine, record: &JobRecord) -> u64 {
    record
        .attempts
        .iter()
        .filter_map(|id| state.attempts.get(id))
        .map(|ar| ar.charge.amount.0)
        .fold(0u64, |acc, amount| acc.saturating_add(amount))
}

/// The last attempt's terminal outcome, if it has one.
fn last_attempt_outcome(state: &StateMachine, record: &JobRecord) -> Option<dto::AttemptOutcome> {
    let last = record.attempts.last()?;
    match &state.attempts.get(last)?.attempt.state {
        AttemptState::Terminal(outcome) => Some(outcome.into()),
        _ => None,
    }
}

fn job_summary(state: &StateMachine, record: &JobRecord) -> dto::JobSummary {
    let current = record.state.attempt();
    let attempt = current.and_then(|id| state.attempts.get(&id));

    // Funding progress is meaningful only while the attempt is accruing —
    // the min funded/requested across dimensions (the contract's scalar).
    let funding_fraction = attempt.and_then(|ar| {
        if matches!(ar.attempt.state, AttemptState::Accruing) {
            state.allocations.get(&ar.attempt.allocation).map(|alloc| {
                let ff = funded_fraction(&alloc.allocation.funded, &alloc.allocation.requested);
                ff.cpu.min(ff.memory).min(ff.disk)
            })
        } else {
            None
        }
    });

    dto::JobSummary {
        id: record.spec.id,
        state: record.state.into(),
        attempt: current,
        image: record.spec.image.clone(),
        quota_entity: record.spec.quota_entity,
        quota_entity_name: state
            .quota_entities
            .get(&record.spec.quota_entity)
            .map(|e| e.name.clone())
            .unwrap_or_default(),
        priority: record.spec.priority,
        submitted_at: record.submitted_at,
        terminal_at: record.terminal_at,
        node: attempt.map(|ar| ar.attempt.node),
        attempt_state: attempt.map(|ar| (&ar.attempt.state).into()),
        funding_fraction,
        cost_ucu: total_charged(state, record),
        outcome: if record.state.is_terminal() {
            last_attempt_outcome(state, record)
        } else {
            None
        },
    }
}

// ---------------------------------------------------------------------------
// Job detail (GET /api/v1/jobs/{job})
// ---------------------------------------------------------------------------

/// 2³², the Q32.32 scale factor — the same conversion the scheduler's
/// `score` module uses to render a fixed-point multiplier as a real number.
const Q32_SCALE: f64 = 4_294_967_296.0;

/// A quota entity's decayed metrics as of `now`, shared by the two ancestry
/// projections (`entity_chain` root-first, `penalty_chain` leaf-first).
struct EntityMetrics {
    id: QuotaEntityId,
    name: String,
    parent: Option<QuotaEntityId>,
    quota_ucu: u64,
    usage_ucu: u64,
    over_quota_ratio: f64,
    penalty: f64,
}

fn entity_metrics(
    state: &StateMachine,
    id: QuotaEntityId,
    now: Timestamp,
) -> Option<EntityMetrics> {
    let e = state.quota_entities.get(&id)?;
    let policy = &state.policy;
    let decayed = policy
        .decay
        .decay_between(e.usage.usage, e.usage.last_update, now);
    let over_quota_ratio = quota::over_quota_ratio(decayed, e.quota);
    Some(EntityMetrics {
        id,
        name: e.name.clone(),
        parent: e.parent,
        quota_ucu: e.quota.0,
        usage_ucu: decayed.0,
        over_quota_ratio,
        penalty: quota::penalty(over_quota_ratio, policy.penalty_exponent_milli),
    })
}

/// A job's quota-entity ancestry, leaf → root. Walks parents exactly as apply
/// and the scheduler do: depth-capped at [`QUOTA_TREE_DEPTH_CAP`], stopping at
/// a missing parent.
fn ancestor_ids(state: &StateMachine, leaf: QuotaEntityId) -> Vec<QuotaEntityId> {
    let mut ids = Vec::new();
    let mut cur = Some(leaf);
    for _ in 0..QUOTA_TREE_DEPTH_CAP {
        let Some(id) = cur else { break };
        let Some(e) = state.quota_entities.get(&id) else {
            break;
        };
        ids.push(id);
        cur = e.parent;
    }
    ids
}

/// The `entity_chain` field: ancestry root → leaf, usage decayed to `now`.
fn entity_chain(
    state: &StateMachine,
    leaf: QuotaEntityId,
    now: Timestamp,
) -> Vec<dto::QuotaEntityView> {
    let mut chain: Vec<dto::QuotaEntityView> = ancestor_ids(state, leaf)
        .into_iter()
        .filter_map(|id| entity_metrics(state, id, now))
        .map(|m| dto::QuotaEntityView {
            id: m.id,
            name: m.name,
            parent: m.parent,
            quota_ucu: m.quota_ucu,
            usage_ucu: m.usage_ucu,
            over_quota_ratio: m.over_quota_ratio,
            penalty: m.penalty,
        })
        .collect();
    chain.reverse();
    chain
}

/// The `penalty_chain` field: ancestry leaf → root, usage decayed to `now`.
fn penalty_chain(
    state: &StateMachine,
    leaf: QuotaEntityId,
    now: Timestamp,
) -> Vec<dto::PenaltyLink> {
    ancestor_ids(state, leaf)
        .into_iter()
        .filter_map(|id| entity_metrics(state, id, now))
        .map(|m| dto::PenaltyLink {
            entity: m.id,
            name: m.name,
            usage_ucu: m.usage_ucu,
            quota_ucu: m.quota_ucu,
            over_quota_ratio: m.over_quota_ratio,
            penalty: m.penalty,
        })
        .collect()
}

/// The `queue` explainer for a job — `None` unless the job is `Queued`.
///
/// Reports the ADR 0021 priority-term inputs that replicated state alone can
/// answer: the multiplier, the per-ancestor penalty chain and its product,
/// and the job's age. Rank, queue depth, and the composed score are
/// deliberately absent — ranking would mean either an O(queue) scan per read
/// or duplicating scheduler-owned scoring inputs (`w_age`, the age horizon)
/// that would drift from what the scheduler actually applies. If those fields
/// return, they should be read from a scheduler-published structure recording
/// how the last invocation's unplaced jobs fared against each other, never
/// recomputed per request.
fn queue_explainer(
    state: &StateMachine,
    record: &JobRecord,
    now: Timestamp,
) -> Option<dto::QueuePositionExplainer> {
    if record.state != JobState::Queued {
        return None;
    }
    let chain = penalty_chain(state, record.spec.quota_entity, now);
    // The product of the chain's links: the same per-link `quota::penalty`
    // values the scheduler's penalty product composes, over the same
    // depth-capped ancestor walk with the same read-time decay.
    let penalty_product = chain.iter().map(|l| l.penalty).product();
    let age = (now - record.submitted_at).max(Duration::ZERO);

    Some(dto::QueuePositionExplainer {
        multiplier: record.multiplier.0 as f64 / Q32_SCALE,
        penalty_chain: chain,
        penalty_product,
        age_seconds: age.as_secs(),
    })
}

/// The `accrual` field — `Some` only while the current attempt is `Accruing`,
/// built from the attempt's allocation exactly as [`get_node`]'s accrual queue.
fn job_accrual(state: &StateMachine, record: &JobRecord) -> Option<dto::AccrualView> {
    let attempt = state.attempts.get(&record.state.attempt()?)?;
    if !matches!(attempt.attempt.state, AttemptState::Accruing) {
        return None;
    }
    let alloc_record = state.allocations.get(&attempt.attempt.allocation)?;
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
        projected_start: None,
    })
}

/// The `state_since` approximation — see the doc on [`dto::JobDetail::state_since`].
fn state_since(state: &StateMachine, record: &JobRecord) -> Timestamp {
    if record.state.is_terminal() {
        return record.terminal_at.unwrap_or(record.submitted_at);
    }
    // A live attempt that has been observed running dates the current state
    // from its start; otherwise (queued, or an attempt still preparing) the
    // best proxy is submission time.
    record
        .state
        .attempt()
        .and_then(|id| state.attempts.get(&id))
        .and_then(|ar| ar.started_at)
        .unwrap_or(record.submitted_at)
}

/// A job's [`dto::CostReport`], computed from replicated policy and the job's
/// spec/charges with the ADR 0019 quota arithmetic (`coppice_core::quota`) —
/// the charge formulas are called, never re-derived here.
fn cost_report(state: &StateMachine, record: &JobRecord) -> dto::CostReport {
    let policy = &state.policy;
    let weights = &policy.cost_weights;
    let requests = &record.spec.requests;

    // Per-dimension rate via the canonical `resource_rate`, isolating one
    // dimension at a time (the other terms contribute zero) so the breakdown
    // uses exactly the same arithmetic as the total.
    let only = |cpu: u64, memory: ByteSize, disk: ByteSize| {
        quota::resource_rate(
            &Resources {
                cpu_millis: cpu,
                memory,
                disk,
            },
            weights,
        )
    };
    let rate_breakdown = dto::RateBreakdown {
        cpu: only(requests.cpu_millis, ByteSize::ZERO, ByteSize::ZERO),
        memory: only(0, requests.memory, ByteSize::ZERO),
        disk: only(0, ByteSize::ZERO, requests.disk),
    };
    let rate_ucu_per_second = quota::resource_rate(requests, weights);

    let priority_multiplier = record.multiplier;
    // The unbounded-runtime surcharge is folded in only for a job with no
    // declared `max_runtime` (ADR 0029).
    let (unbounded_multiplier, effective_multiplier) = if record.spec.max_runtime.is_none() {
        let u = policy.unbounded_runtime_multiplier;
        (u, priority_multiplier.saturating_mul(u))
    } else {
        (PriorityMultiplier::ONE, priority_multiplier)
    };

    // The charge window: the declared bound, or the policy default when unset.
    let (charge_window_seconds, charge_window_is_default) = match record.spec.max_runtime {
        Some(d) => (quota::runtime_seconds_ceil(d), false),
        None => (policy.default_charge_runtime_s, true),
    };

    let effective_rate_ucu_per_second =
        quota::cost_from_rate(rate_ucu_per_second, 1, effective_multiplier).0;
    let estimated_ucu = quota::cost_from_rate(
        rate_ucu_per_second,
        charge_window_seconds,
        effective_multiplier,
    )
    .0;

    // The refund fraction actually captured on the latest attempt's charge
    // (ADR 0029 freezes it at placement); before any placement, what a
    // placement now would capture — full for an unbounded job, else policy.
    let refund_fraction_milli = record
        .attempts
        .last()
        .and_then(|id| state.attempts.get(id))
        .map(|ar| ar.charge.refund_fraction_milli)
        .unwrap_or(if record.spec.max_runtime.is_none() {
            quota::FULL_REFUND_MILLI
        } else {
            policy.refund_fraction_milli
        });

    dto::CostReport {
        rate_ucu_per_second,
        rate_breakdown,
        priority_multiplier: priority_multiplier.0 as f64 / Q32_SCALE,
        unbounded_multiplier: unbounded_multiplier.0 as f64 / Q32_SCALE,
        effective_rate_ucu_per_second,
        charge_window_seconds,
        charge_window_is_default,
        estimated_ucu,
        charged_ucu: total_charged(state, record),
        refund_fraction: refund_fraction_milli as f64 / 1000.0,
        // No measured-usage pipeline exists, and the true-up is not retained
        // per job — both are honestly absent rather than fabricated.
        actual_ucu: None,
        true_up: None,
    }
}

/// `GET /api/v1/jobs/{job}`. `None` when the id is not in the view (a 404 at
/// the handler). `now` is the reader's wall clock, feeding the read-time
/// entity-usage decay, queue age, and penalty product — never replicated.
pub fn get_job(state: &StateMachine, id: &JobId, now: Timestamp) -> Option<dto::JobDetail> {
    let record = state.jobs.get(id)?;
    let spec = &record.spec;

    Some(dto::JobDetail {
        id: spec.id,
        state: record.state.into(),
        spec: dto::JobSpecView {
            image: spec.image.clone(),
            command: spec.command.clone(),
            entrypoint: spec.entrypoint.clone(),
            requests: (&spec.requests).into(),
            priority: spec.priority,
            max_runtime_seconds: spec.max_runtime.map(Duration::as_secs),
            quota_entity: spec.quota_entity,
            retry: dto::RetryPolicy {
                max_retries: spec.retry.max_retries,
                retry_user_errors: spec.retry.retry_user_errors,
            },
        },
        submitted_at: record.submitted_at,
        state_since: state_since(state, record),
        terminal_at: record.terminal_at,
        retries_used: record.retries_used,
        abort_requested: spec
            .abort_requested
            .as_ref()
            .map(|a| dto::AbortRequestedView {
                reason: a.reason.clone(),
                requested_at: a.requested_at,
            }),
        entity_chain: entity_chain(state, spec.quota_entity, now),
        attempts: record
            .attempts
            .iter()
            .filter_map(|aid| state.attempts.get(aid))
            .map(attempt_view)
            .collect(),
        queue: queue_explainer(state, record, now),
        accrual: job_accrual(state, record),
        cost: cost_report(state, record),
    })
}

// Quota entities (GET /api/v1/quota-entities[/{entity}])
// ---------------------------------------------------------------------------

/// Subtree-inclusive queued/running job counts for one entity.
#[derive(Default, Clone, Copy)]
struct QuotaCounts {
    queued: u32,
    running: u32,
}

/// Subtree-inclusive queued/running counts for **every** entity, in one pass
/// over the jobs. Each queued/running job increments the counter of its own
/// entity and every ancestor (bounded by [`QUOTA_TREE_DEPTH_CAP`], exactly as
/// apply walks the tree), so a node's count covers itself and all descendants
/// — the `types.ts` `QuotaEntityNode` semantics. Cheaper than a per-entity
/// subtree scan (`O(jobs × depth)`, not `O(entities × jobs)`), and a
/// handler-scoped memo, never state.
fn subtree_job_counts(state: &StateMachine) -> BTreeMap<QuotaEntityId, QuotaCounts> {
    let mut counts: BTreeMap<QuotaEntityId, QuotaCounts> = BTreeMap::new();
    for (_, record) in &state.jobs {
        let bump: fn(&mut QuotaCounts) = match job_phase(state, record) {
            dto::JobPhase::Queued => |c: &mut QuotaCounts| c.queued += 1,
            dto::JobPhase::Running => |c: &mut QuotaCounts| c.running += 1,
            _ => continue,
        };
        let mut cur = Some(record.spec.quota_entity);
        for _ in 0..QUOTA_TREE_DEPTH_CAP {
            let Some(id) = cur else { break };
            bump(counts.entry(id).or_default());
            cur = state.quota_entities.get(&id).and_then(|e| e.parent);
        }
    }
    counts
}

/// Decayed usage, over-quota ratio, and penalty for an entity at read time —
/// the shared derivation behind both the node and the chain view, matching
/// `score.rs`'s lazy decay so a listed figure never disagrees with the
/// scheduler's.
fn decayed_quota_figures(
    e: &QuotaEntity,
    now: Timestamp,
    policy: &PolicyConfig,
) -> (u64, f64, f64) {
    let usage = policy
        .decay
        .decay_between(e.usage.usage, e.usage.last_update, now);
    let ratio = quota::over_quota_ratio(usage, e.quota);
    let penalty = quota::penalty(ratio, policy.penalty_exponent_milli);
    (usage.0, ratio, penalty)
}

fn quota_entity_node(
    id: &QuotaEntityId,
    e: &QuotaEntity,
    now: Timestamp,
    policy: &PolicyConfig,
    counts: &BTreeMap<QuotaEntityId, QuotaCounts>,
) -> dto::QuotaEntityNode {
    let (usage_ucu, over_quota_ratio, penalty) = decayed_quota_figures(e, now, policy);
    let count = counts.get(id).copied().unwrap_or_default();
    dto::QuotaEntityNode {
        id: *id,
        name: e.name.clone(),
        parent: e.parent,
        quota_ucu: e.quota.0,
        usage_ucu,
        over_quota_ratio,
        penalty,
        created_at: e.created_at,
        updated_at: e.updated_at,
        queued_count: count.queued,
        running_count: count.running,
    }
}

fn quota_entity_view(
    id: &QuotaEntityId,
    e: &QuotaEntity,
    now: Timestamp,
    policy: &PolicyConfig,
) -> dto::QuotaEntityView {
    let (usage_ucu, over_quota_ratio, penalty) = decayed_quota_figures(e, now, policy);
    dto::QuotaEntityView {
        id: *id,
        name: e.name.clone(),
        parent: e.parent,
        quota_ucu: e.quota.0,
        usage_ucu,
        over_quota_ratio,
        penalty,
    }
}

/// `GET /api/v1/quota-entities`.
///
/// Every entity as a subtree-counted node, in id order (`quota_entities` is a
/// `BTreeMap`, so iteration is deterministic). `now` is the reader's wall
/// clock, used only to decay usage to read time — never replicated state.
pub fn list_quota_entities(state: &StateMachine, now: Timestamp) -> dto::ListQuotaEntitiesResponse {
    let counts = subtree_job_counts(state);
    let entities = state
        .quota_entities
        .iter()
        .map(|(id, e)| quota_entity_node(id, e, now, &state.policy, &counts))
        .collect();
    dto::ListQuotaEntitiesResponse { entities }
}

/// `GET /api/v1/quota-entities/{entity}`; `None` when the id is not in the
/// tree (the handler's 404). `now` decays usage to read time, as in the list.
pub fn get_quota_entity(
    state: &StateMachine,
    id: &QuotaEntityId,
    now: Timestamp,
) -> Option<dto::GetQuotaEntityResponse> {
    let entity = state.quota_entities.get(id)?;
    let counts = subtree_job_counts(state);
    let node = quota_entity_node(id, entity, now, &state.policy, &counts);

    // Ancestry, this entity first then up to the root, bounded by the depth
    // cap; reversed to root-first for the response.
    let mut chain_ids = Vec::new();
    let mut cur = Some(*id);
    for _ in 0..QUOTA_TREE_DEPTH_CAP {
        let Some(cid) = cur else { break };
        chain_ids.push(cid);
        cur = state.quota_entities.get(&cid).and_then(|e| e.parent);
    }
    chain_ids.reverse();
    let chain = chain_ids
        .iter()
        .filter_map(|cid| {
            state
                .quota_entities
                .get(cid)
                .map(|e| quota_entity_view(cid, e, now, &state.policy))
        })
        .collect();

    // Direct children only, in id order.
    let children = state
        .quota_entities
        .iter()
        .filter(|(_, e)| e.parent == Some(*id))
        .map(|(cid, e)| quota_entity_node(cid, e, now, &state.policy, &counts))
        .collect();

    let stats = quota_entity_stats(state, *id, now);

    Some(dto::GetQuotaEntityResponse {
        entity: node,
        chain,
        children,
        stats,
    })
}

/// Subtree-inclusive stats for one entity: job counts by phase, oldest
/// queued age, and the running burn rate over the entity and its descendants.
///
/// Reuses ListJobs' subtree machinery ([`child_adjacency`] +
/// [`descendant_set`]) to resolve the descendant id set once, then a single
/// pass over the jobs. `charged_ucu_24h` and `usage_history` are left
/// unbacked (null / empty): no charge ledger or usage-series sampler exists.
fn quota_entity_stats(
    state: &StateMachine,
    id: QuotaEntityId,
    now: Timestamp,
) -> dto::QuotaEntityStats {
    let children = child_adjacency(state);
    let subtree = descendant_set(&children, state, id);

    let mut by_state: BTreeMap<dto::JobPhase, u32> =
        dto::JobPhase::ALL.iter().map(|phase| (*phase, 0)).collect();
    let mut oldest_queued_age: Option<Duration> = None;
    let mut burn_rate: u64 = 0;

    for (_, record) in &state.jobs {
        if !subtree.contains(&record.spec.quota_entity) {
            continue;
        }
        *by_state.entry(job_phase(state, record)).or_default() += 1;

        if record.state == JobState::Queued {
            let age = (now - record.submitted_at).max(Duration::ZERO);
            oldest_queued_age = Some(oldest_queued_age.map_or(age, |old| old.max(age)));
        }
        // The running attempt's recorded charge rate is µCU/s already — the
        // same figure `AttemptView::rate_ucu_per_second` reports, no re-derive.
        if let Some(attempt_id) = record.state.attempt() {
            if let Some(ar) = state.attempts.get(&attempt_id) {
                if matches!(ar.attempt.state, AttemptState::Running) {
                    burn_rate = burn_rate.saturating_add(ar.rate_ucu_per_second);
                }
            }
        }
    }

    dto::QuotaEntityStats {
        by_state,
        oldest_queued_age_seconds: oldest_queued_age.map(Duration::as_secs),
        burn_rate_ucu_per_second: burn_rate,
        // No charge ledger or usage-series sampler exists — served unbacked
        // rather than fabricated (null / empty).
        charged_ucu_24h: None,
        usage_history: Vec::new(),
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
                    memory: ByteSize::from_bytes(8_000_000_000),
                    disk: ByteSize::from_bytes(100_000_000_000),
                },
                labels: BTreeMap::new(),
                schedulable: true,
                service_addr: None,
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
                charged_at: ts(0),
                refund_fraction_milli: FULL_REFUND_MILLI,
            },
            rate_ucu_per_second: 100,
            multiplier: PriorityMultiplier::ONE,
            started_at: Some(ts(1000)),
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
                    memory: ByteSize::from_bytes(1_000_000),
                    disk: ByteSize::ZERO,
                },
                funded: Resources {
                    cpu_millis: 1000,
                    memory: ByteSize::from_bytes(1_000_000),
                    disk: ByteSize::ZERO,
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

    fn test_job(id: JobId, state: JobState, submitted_at: Timestamp) -> coppice_state::JobRecord {
        coppice_state::JobRecord {
            spec: coppice_core::job::Job {
                id,
                image: "busybox".to_string(),
                command: vec!["run".to_string()],
                entrypoint: None,
                requests: Resources::ZERO,
                priority: 0,
                max_runtime: None,
                quota_entity: QuotaEntityId::new(),
                retry: Default::default(),
                abort_requested: None,
            },
            state,
            multiplier: PriorityMultiplier::ONE,
            submitted_at,
            terminal_at: None,
            retries_used: 0,
            attempts: Vec::new(),
        }
    }

    const CLUSTER: &str = "cluster-00000000-0000-0000-0000-000000000009";

    /// Fixture instants are seconds from the epoch, so the range check
    /// cannot fire.
    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

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
    fn overview(state: &StateMachine, now: Timestamp) -> dto::GetClusterOverviewResponse {
        cluster_overview(state, cluster(), now, &QueueWindow::default(), &no_recent())
    }

    #[test]
    fn overview_of_an_empty_cluster_counts_nothing() {
        let response = overview(&StateMachine::default(), ts(1_000));

        assert_eq!(response.cluster_id, cluster());
        assert_eq!(response.queue.depth, 0);
        assert_eq!(response.queue.oldest_queued_age_seconds, None);
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

        let capacity = overview(&state, ts(0)).capacity;

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
            .insert(queued_job, test_job(queued_job, JobState::Queued, ts(0)));
        state.jobs.insert(
            running_job,
            test_job(running_job, JobState::Attempting(running_attempt), ts(0)),
        );
        state.jobs.insert(
            preparing_job,
            test_job(preparing_job, JobState::Attempting(accruing_attempt), ts(0)),
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

        let queue = overview(&state, ts(0)).queue;

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
            .insert(job, test_job(job, JobState::Attempting(attempt), ts(0)));
        state.attempts.insert(
            attempt,
            test_attempt(
                attempt,
                job,
                node,
                AttemptState::Terminal(coppice_core::attempt::AttemptOutcome::Exited { code: 0 }),
            ),
        );

        let queue = overview(&state, ts(0)).queue;
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
            .insert(old, test_job(old, JobState::Queued, ts(1_000_000)));
        state
            .jobs
            .insert(recent, test_job(recent, JobState::Queued, ts(9_000_000)));
        // A job that has left the queue no longer ages it, however old.
        state
            .jobs
            .insert(running, test_job(running, JobState::Succeeded, ts(0)));

        // Read at t=10 s: the older job has waited 9 s, the recent one 1 s.
        let queue = overview(&state, ts(10_000_000)).queue;
        assert_eq!(queue.oldest_queued_age_seconds, Some(9));
    }

    #[test]
    fn a_submission_timestamp_in_the_future_ages_to_zero_never_negative() {
        // Proposer clock skew: `submitted_at` rides in on the command, so a
        // reader's clock can legitimately be behind it.
        let job = JobId::new();
        let mut state = StateMachine::default();
        state
            .jobs
            .insert(job, test_job(job, JobState::Queued, ts(5_000_000)));

        let queue = overview(&state, ts(1_000_000)).queue;
        assert_eq!(queue.oldest_queued_age_seconds, Some(0));
    }

    fn window_of(buckets: Vec<crate::QueueBucket>) -> QueueWindow {
        QueueWindow { buckets }
    }

    /// A nominal-width (30 s) bucket.
    fn bucket(start_us: i64, depth: u32, arrivals: u32, drains: u32) -> crate::QueueBucket {
        crate::QueueBucket {
            start: ts(start_us),
            end: ts(start_us) + Duration::from_secs(30),
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
        let rates = queue_rates(&window);
        assert_eq!(rates.arrivals_per_minute, Some(3.0));
        assert_eq!(rates.drains_per_minute, Some(1.0));
    }

    #[test]
    fn queue_rates_use_only_the_newest_rate_window_buckets() {
        // An old burst outside the 10-bucket rate window must not inflate
        // the headline rate (it still ships in `history`).
        let mut buckets = vec![bucket(0, 0, 600, 600)];
        for i in 1..=(RATE_WINDOW_BUCKETS as i64) {
            buckets.push(bucket(i * 30_000_000, 0, 1, 0));
        }
        let rates = queue_rates(&window_of(buckets));
        // 10 buckets × 30 s = 5 minutes, 10 arrivals → 2/min.
        assert_eq!(rates.arrivals_per_minute, Some(2.0));
        assert_eq!(rates.drains_per_minute, Some(0.0));
    }

    /// A missed-tick stall closes one honest long bucket; rates must scale
    /// by its recorded span, not the nominal width — a five-minute bucket
    /// with 10 arrivals is 2/min, not 20/min.
    #[test]
    fn a_stall_stretched_bucket_scales_by_its_recorded_span() {
        let long = crate::QueueBucket {
            start: ts(0),
            end: ts(0) + Duration::from_mins(5),
            depth: 0,
            arrivals: 10,
            drains: 5,
        };
        let rates = queue_rates(&window_of(vec![long]));
        assert_eq!(rates.arrivals_per_minute, Some(2.0));
        assert_eq!(rates.drains_per_minute, Some(1.0));

        let history = queue_history(&window_of(vec![long]));
        assert_eq!(history[0].arrived_per_minute, 2.0);
        assert_eq!(history[0].drained_per_minute, 1.0);
    }

    #[test]
    fn queue_history_carries_every_retained_bucket_scaled_per_minute() {
        let window = window_of(vec![bucket(0, 5, 2, 1), bucket(30_000_000, 6, 0, 3)]);
        let history = queue_history(&window);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].t, ts(0));
        assert_eq!(history[0].depth, 5);
        assert_eq!(history[0].arrived_per_minute, 4.0);
        assert_eq!(history[0].drained_per_minute, 2.0);
        assert_eq!(history[1].t, ts(30_000_000));
        assert_eq!(history[1].drained_per_minute, 6.0);
    }

    #[test]
    fn overview_serves_rates_and_history_from_the_window() {
        let window = window_of(vec![bucket(0, 5, 2, 1)]);
        let queue = cluster_overview(
            &StateMachine::default(),
            cluster(),
            ts(0),
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
                at: ts(1_234),
                event: coppice_state::Event::JobSubmitted { job },
            }],
        };
        let rendered = cluster_overview(
            &StateMachine::default(),
            cluster(),
            ts(0),
            &QueueWindow::default(),
            &recent,
        )
        .recent_events;
        assert_eq!(rendered.floor_index, 4);
        assert_eq!(rendered.events.len(), 1);
        let event = &rendered.events[0];
        assert_eq!((event.index, event.ordinal, event.at), (9, 3, ts(1_234)));
        assert_eq!(event.body, dto::TimelineEventBody::JobSubmitted { job });
    }

    #[test]
    fn queue_rates_and_history_are_absent_not_zero() {
        // A window with no coverage (fresh replica, or one that just lost
        // the event stream); `0.0` would claim "nothing is draining" (see
        // `dto::QueueStats`).
        let queue = overview(&StateMachine::default(), ts(0)).queue;
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

    // ---- job list ---------------------------------------------------------

    /// A JobId whose numeric order is `n`, so descending JobId order is
    /// descending `n` — deterministic, unlike freshly minted UUIDv7s.
    fn job_id(n: u16) -> JobId {
        format!("job-00000000-0000-0000-0000-{n:012x}")
            .parse()
            .unwrap()
    }

    fn quota_id(n: u16) -> QuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012x}")
            .parse()
            .unwrap()
    }

    /// A queued job with a controllable id and submission time; other spec
    /// fields default (image `busybox`, zero requests, a fresh entity).
    fn queued(id: JobId, submitted_us: i64) -> JobRecord {
        test_job(id, JobState::Queued, ts(submitted_us))
    }

    /// A job with the spec fields the leaf filters key on made explicit.
    fn spec_job(
        id: JobId,
        image: &str,
        entity: QuotaEntityId,
        requests: Resources,
        submitted_us: i64,
    ) -> JobRecord {
        let mut record = queued(id, submitted_us);
        record.spec.image = image.to_string();
        record.spec.quota_entity = entity;
        record.spec.requests = requests;
        record
    }

    fn ids(response: &dto::ListJobsResponse) -> Vec<JobId> {
        response.jobs.iter().map(|j| j.id).collect()
    }

    #[test]
    fn list_jobs_returns_newest_first_and_null_cursor_when_exhausted() {
        let mut state = StateMachine::default();
        for n in 1..=3 {
            let id = job_id(n);
            state.jobs.insert(id, queued(id, n as i64));
        }
        let response = list_jobs(&state, None, None, 100);
        assert_eq!(ids(&response), vec![job_id(3), job_id(2), job_id(1)]);
        // The scan ran off the low end: no more to fetch.
        assert_eq!(response.next_cursor, None);
    }

    #[test]
    fn a_full_page_carries_the_cursor_of_the_last_examined_record() {
        let mut state = StateMachine::default();
        for n in 1..=4 {
            let id = job_id(n);
            state.jobs.insert(id, queued(id, n as i64));
        }
        let page = list_jobs(&state, None, None, 2);
        assert_eq!(ids(&page), vec![job_id(4), job_id(3)]);
        assert_eq!(page.next_cursor, Some(dto::JobCursor::format(job_id(3))));
    }

    #[test]
    fn cursor_continuation_neither_skips_nor_duplicates_across_a_new_head() {
        let mut state = StateMachine::default();
        for n in 1..=4 {
            let id = job_id(n);
            state.jobs.insert(id, queued(id, n as i64));
        }
        let page1 = list_jobs(&state, None, None, 2);
        assert_eq!(ids(&page1), vec![job_id(4), job_id(3)]);
        let cursor = dto::JobCursor::parse(page1.next_cursor.as_deref().unwrap()).unwrap();

        // A newer job arrives between requests; keyset paging on immutable
        // ids ignores it (it sorts above the cursor) rather than shifting the
        // page and skipping job_id(2).
        let head = job_id(5);
        state.jobs.insert(head, queued(head, 5));

        let page2 = list_jobs(&state, None, Some(cursor), 2);
        assert_eq!(ids(&page2), vec![job_id(2), job_id(1)]);
        assert_eq!(page2.next_cursor, None);
    }

    #[test]
    fn budget_exhaustion_returns_a_short_page_with_a_cursor() {
        let mut state = StateMachine::default();
        for n in 1..=5 {
            let id = job_id(n);
            state.jobs.insert(id, queued(id, n as i64));
        }
        // No job is Running, so the page stays empty; a budget of 3 stops the
        // scan after the three newest, cursor at the last examined.
        let filter = dto::JobFilter::Phase(dto::PhaseFilter {
            r#in: vec![dto::JobPhase::Running],
        });
        let response = list_jobs_scan(&state, Some(&filter), None, 100, 3);
        assert!(response.jobs.is_empty());
        assert_eq!(
            response.next_cursor,
            Some(dto::JobCursor::format(job_id(3)))
        );
    }

    #[test]
    fn phase_leaf_matches_the_derived_phase() {
        let mut state = StateMachine::default();
        let q = job_id(1);
        let s = job_id(2);
        state.jobs.insert(q, test_job(q, JobState::Queued, ts(0)));
        state
            .jobs
            .insert(s, test_job(s, JobState::Succeeded, ts(0)));
        let filter = dto::JobFilter::Phase(dto::PhaseFilter {
            r#in: vec![dto::JobPhase::Queued],
        });
        assert_eq!(ids(&list_jobs(&state, Some(&filter), None, 100)), vec![q]);
    }

    #[test]
    fn entity_leaf_distinguishes_subtree_from_exact() {
        // root → child; a job under each.
        let root = quota_id(1);
        let child = quota_id(2);
        let mut state = StateMachine::default();
        state.quota_entities.insert(
            root,
            coppice_state::QuotaEntity {
                parent: None,
                name: "root".to_string(),
                quota: coppice_core::quota::CostUnits::ZERO,
                usage: coppice_core::quota::UsageState::new(ts(0)),
                created_at: ts(0),
                updated_at: ts(0),
            },
        );
        state.quota_entities.insert(
            child,
            coppice_state::QuotaEntity {
                parent: Some(root),
                name: "child".to_string(),
                quota: coppice_core::quota::CostUnits::ZERO,
                usage: coppice_core::quota::UsageState::new(ts(0)),
                created_at: ts(0),
                updated_at: ts(0),
            },
        );
        let root_job = job_id(1);
        let child_job = job_id(2);
        state.jobs.insert(
            root_job,
            spec_job(root_job, "img", root, Resources::ZERO, 0),
        );
        state.jobs.insert(
            child_job,
            spec_job(child_job, "img", child, Resources::ZERO, 0),
        );

        let subtree = dto::JobFilter::Entity(dto::EntityFilter {
            id: root,
            scope: dto::EntityScope::Subtree,
        });
        assert_eq!(
            ids(&list_jobs(&state, Some(&subtree), None, 100)),
            vec![child_job, root_job]
        );

        let exact = dto::JobFilter::Entity(dto::EntityFilter {
            id: root,
            scope: dto::EntityScope::Exact,
        });
        assert_eq!(
            ids(&list_jobs(&state, Some(&exact), None, 100)),
            vec![root_job]
        );

        // An unknown entity matches nothing, even under subtree scope.
        let unknown = dto::JobFilter::Entity(dto::EntityFilter {
            id: quota_id(99),
            scope: dto::EntityScope::Subtree,
        });
        assert!(list_jobs(&state, Some(&unknown), None, 100).jobs.is_empty());
    }

    #[test]
    fn node_leaf_matches_the_current_attempts_node() {
        let node = NodeId::new();
        let attempting = job_id(2);
        let queued_job = job_id(1);
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state
            .jobs
            .insert(queued_job, test_job(queued_job, JobState::Queued, ts(0)));
        state.jobs.insert(
            attempting,
            test_job(attempting, JobState::Attempting(attempt), ts(0)),
        );
        state.attempts.insert(
            attempt,
            test_attempt(attempt, attempting, node, AttemptState::Running),
        );

        let filter = dto::JobFilter::Node(node);
        assert_eq!(
            ids(&list_jobs(&state, Some(&filter), None, 100)),
            vec![attempting]
        );
        // An unknown node matches nothing.
        let other = dto::JobFilter::Node(NodeId::new());
        assert!(list_jobs(&state, Some(&other), None, 100).jobs.is_empty());
    }

    #[test]
    fn image_leaf_matches_contains_and_equals() {
        let mut state = StateMachine::default();
        let a = job_id(1);
        let b = job_id(2);
        state
            .jobs
            .insert(a, spec_job(a, "alpine:3", quota_id(1), Resources::ZERO, 0));
        state
            .jobs
            .insert(b, spec_job(b, "busybox:1", quota_id(1), Resources::ZERO, 0));

        let contains = dto::JobFilter::Image(dto::ImageFilter::Contains("alpine".to_string()));
        assert_eq!(ids(&list_jobs(&state, Some(&contains), None, 100)), vec![a]);

        let equals = dto::JobFilter::Image(dto::ImageFilter::Equals("busybox:1".to_string()));
        assert_eq!(ids(&list_jobs(&state, Some(&equals), None, 100)), vec![b]);
    }

    #[test]
    fn id_and_search_leaves() {
        let mut state = StateMachine::default();
        let a = job_id(1);
        let b = job_id(2);
        state
            .jobs
            .insert(a, spec_job(a, "alpine:3", quota_id(1), Resources::ZERO, 0));
        state
            .jobs
            .insert(b, spec_job(b, "busybox:1", quota_id(1), Resources::ZERO, 0));

        let id = dto::JobFilter::Id(dto::IdFilter { r#in: vec![b] });
        assert_eq!(ids(&list_jobs(&state, Some(&id), None, 100)), vec![b]);

        // Case-insensitive over the image string.
        let by_image = dto::JobFilter::Search("ALPINE".to_string());
        assert_eq!(ids(&list_jobs(&state, Some(&by_image), None, 100)), vec![a]);
        // …and over the job id string.
        let by_id = dto::JobFilter::Search(format!("{a}"));
        assert_eq!(ids(&list_jobs(&state, Some(&by_id), None, 100)), vec![a]);
    }

    #[test]
    fn submitted_window_is_after_inclusive_before_exclusive() {
        let mut state = StateMachine::default();
        for n in 1..=3u16 {
            let id = job_id(n);
            state.jobs.insert(id, queued(id, i64::from(n) * 1_000_000));
        }
        // after >= 1s, before < 3s → only the job at exactly 1s and 2s.
        let filter = dto::JobFilter::Submitted(dto::SubmittedFilter {
            after: Timestamp::from_micros(1_000_000),
            before: Timestamp::from_micros(3_000_000),
        });
        let matched = ids(&list_jobs(&state, Some(&filter), None, 100));
        assert_eq!(matched, vec![job_id(2), job_id(1)]);
    }

    #[test]
    fn requests_bounds_are_inclusive() {
        let mut state = StateMachine::default();
        let small = job_id(1);
        let big = job_id(2);
        state
            .jobs
            .insert(small, spec_job(small, "img", quota_id(1), cpu(1000), 0));
        state
            .jobs
            .insert(big, spec_job(big, "img", quota_id(1), cpu(4000), 0));

        let filter = dto::JobFilter::Requests(dto::RequestsFilter {
            resource: dto::RequestsResource::CpuMillis,
            min: Some(1000),
            max: Some(1000),
        });
        assert_eq!(
            ids(&list_jobs(&state, Some(&filter), None, 100)),
            vec![small]
        );
    }

    fn cpu(millis: u64) -> Resources {
        Resources {
            cpu_millis: millis,
            memory: ByteSize::ZERO,
            disk: ByteSize::ZERO,
        }
    }

    #[test]
    fn and_any_not_compose() {
        let mut state = StateMachine::default();
        let a = job_id(1); // alpine, queued
        let b = job_id(2); // busybox, queued
        let c = job_id(3); // alpine, succeeded
        state
            .jobs
            .insert(a, spec_job(a, "alpine", quota_id(1), Resources::ZERO, 0));
        state
            .jobs
            .insert(b, spec_job(b, "busybox", quota_id(1), Resources::ZERO, 0));
        let mut c_rec = spec_job(c, "alpine", quota_id(1), Resources::ZERO, 0);
        c_rec.state = JobState::Succeeded;
        state.jobs.insert(c, c_rec);

        // alpine AND (NOT succeeded) → just `a`.
        let filter = dto::JobFilter::All(vec![
            dto::JobFilter::Image(dto::ImageFilter::Contains("alpine".to_string())),
            dto::JobFilter::Not(Box::new(dto::JobFilter::Phase(dto::PhaseFilter {
                r#in: vec![dto::JobPhase::Succeeded],
            }))),
        ]);
        assert_eq!(ids(&list_jobs(&state, Some(&filter), None, 100)), vec![a]);

        // busybox OR succeeded → b and c.
        let either = dto::JobFilter::Any(vec![
            dto::JobFilter::Image(dto::ImageFilter::Equals("busybox".to_string())),
            dto::JobFilter::Phase(dto::PhaseFilter {
                r#in: vec![dto::JobPhase::Succeeded],
            }),
        ]);
        assert_eq!(
            ids(&list_jobs(&state, Some(&either), None, 100)),
            vec![c, b]
        );
    }

    #[test]
    fn summary_attempt_fields_track_the_current_attempt() {
        let node = NodeId::new();
        let job = job_id(1);
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        let mut record = test_job(job, JobState::Attempting(attempt), ts(0));
        record.attempts = vec![attempt];
        state.jobs.insert(job, record);
        // An accruing attempt pointing at a half-funded allocation.
        let alloc = AllocationId::new();
        let mut attempt_rec = test_attempt(attempt, job, node, AttemptState::Accruing);
        attempt_rec.attempt.allocation = alloc;
        state.attempts.insert(attempt, attempt_rec);
        let mut alloc_rec = test_allocation(alloc, job, attempt, node, AllocationState::Accruing);
        // requested cpu 1000 / mem 1_000_000 → min funded fraction 0.5.
        alloc_rec.allocation.funded = Resources {
            cpu_millis: 500,
            memory: ByteSize::from_bytes(500_000),
            disk: ByteSize::ZERO,
        };
        state.allocations.insert(alloc, alloc_rec);

        let summary = &list_jobs(&state, None, None, 100).jobs[0];
        assert_eq!(summary.state, dto::JobStateKind::Attempting);
        assert_eq!(summary.attempt, Some(attempt));
        assert_eq!(summary.node, Some(node));
        assert_eq!(summary.attempt_state, Some(dto::AttemptState::Accruing));
        assert_eq!(summary.funding_fraction, Some(0.5));
        assert_eq!(summary.outcome, None);
        // Gross charge from the one attempt (test fixture: 1000 µCU).
        assert_eq!(summary.cost_ucu, 1000);
    }

    #[test]
    fn summary_outcome_present_only_when_terminal() {
        let node = NodeId::new();
        let job = job_id(1);
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        let mut record = test_job(job, JobState::Failed, ts(0));
        record.attempts = vec![attempt];
        record.terminal_at = Some(ts(5));
        state.jobs.insert(job, record);
        state.attempts.insert(
            attempt,
            test_attempt(
                attempt,
                job,
                node,
                AttemptState::Terminal(coppice_core::attempt::AttemptOutcome::MemoryLimitExceeded),
            ),
        );

        let summary = &list_jobs(&state, None, None, 100).jobs[0];
        assert_eq!(summary.state, dto::JobStateKind::Failed);
        assert_eq!(summary.attempt, None);
        assert_eq!(summary.node, None);
        assert_eq!(summary.attempt_state, None);
        assert_eq!(summary.funding_fraction, None);
        assert_eq!(
            summary.outcome.as_ref().map(|o| o.kind),
            Some(dto::AttemptOutcomeKind::MemoryLimitExceeded)
        );
        assert_eq!(summary.terminal_at, Some(ts(5)));
    }

    // ---- job detail (GET /api/v1/jobs/{job}) ------------------------------

    /// The documented reference calibration (ADR 0019): 1 core-second = 1 CU
    /// = 1_000_000 µCU.
    const REFERENCE_WEIGHTS: coppice_core::quota::CostWeights = coppice_core::quota::CostWeights {
        per_cpu_milli_second: 1000 << 32,
        per_memory_byte_second: 1_000_000,
        per_disk_byte_second: 62_500,
    };

    fn put_entity(
        state: &mut StateMachine,
        id: QuotaEntityId,
        parent: Option<QuotaEntityId>,
        name: &str,
        quota: u64,
        usage: u64,
        at: Timestamp,
    ) {
        state.quota_entities.insert(
            id,
            coppice_state::QuotaEntity {
                parent,
                name: name.to_string(),
                quota: CostUnits(quota),
                usage: coppice_core::quota::UsageState {
                    usage: CostUnits(usage),
                    last_update: at,
                },
                created_at: at,
                updated_at: at,
            },
        );
    }

    #[test]
    fn get_job_returns_none_for_missing() {
        let state = StateMachine::default();
        assert!(get_job(&state, &job_id(1), ts(0)).is_none());
    }

    #[test]
    fn get_job_projects_spec_and_entity_chain_root_first() {
        let root = quota_id(1);
        let leaf = quota_id(2);
        let now = ts(1_000_000);
        let mut state = StateMachine::default();
        // No decay: usage stamped at `now`.
        put_entity(&mut state, root, None, "root", 1_000_000, 0, now);
        put_entity(&mut state, leaf, Some(root), "leaf", 1_000_000, 0, now);

        let id = job_id(1);
        let mut record = spec_job(id, "alpine:3", leaf, cpu(1000), 1_000_000);
        record.spec.command = vec!["sh".to_string(), "-c".to_string()];
        record.spec.entrypoint = Some(vec!["/bin/init".to_string()]);
        record.spec.max_runtime = Some(Duration::from_secs(3600));
        state.jobs.insert(id, record);

        let detail = get_job(&state, &id, now).unwrap();
        assert_eq!(detail.id, id);
        assert_eq!(detail.spec.image, "alpine:3");
        assert_eq!(detail.spec.command, vec!["sh", "-c"]);
        assert_eq!(detail.spec.entrypoint, Some(vec!["/bin/init".to_string()]));
        assert_eq!(detail.spec.max_runtime_seconds, Some(3600));
        assert_eq!(detail.spec.quota_entity, leaf);
        // Ancestry is root first, the owning (leaf) entity last.
        let names: Vec<&str> = detail
            .entity_chain
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(names, vec!["root", "leaf"]);
    }

    #[test]
    fn get_job_state_since_approximates_by_phase() {
        let now = ts(10_000_000);
        // Terminal: dates from terminal_at.
        let mut state = StateMachine::default();
        let term = job_id(1);
        let mut rec = test_job(term, JobState::Succeeded, ts(0));
        rec.terminal_at = Some(ts(5_000_000));
        state.jobs.insert(term, rec);
        let detail = get_job(&state, &term, now).unwrap();
        assert_eq!(detail.state_since, ts(5_000_000));
        // A terminal job is never queued and never accruing.
        assert!(detail.queue.is_none());
        assert!(detail.accrual.is_none());

        // Running: dates from the current attempt's started_at.
        let running = job_id(2);
        let attempt = AttemptId::new();
        let node = NodeId::new();
        let mut rrec = test_job(running, JobState::Attempting(attempt), ts(0));
        rrec.attempts = vec![attempt];
        state.jobs.insert(running, rrec);
        let mut arec = test_attempt(attempt, running, node, AttemptState::Running);
        arec.started_at = Some(ts(3_000_000));
        state.attempts.insert(attempt, arec);
        assert_eq!(
            get_job(&state, &running, now).unwrap().state_since,
            ts(3_000_000)
        );

        // Queued: dates from submission.
        let queued_j = job_id(3);
        state.jobs.insert(
            queued_j,
            test_job(queued_j, JobState::Queued, ts(2_000_000)),
        );
        assert_eq!(
            get_job(&state, &queued_j, now).unwrap().state_since,
            ts(2_000_000)
        );
    }

    #[test]
    fn get_job_cost_report_prices_a_bounded_job() {
        let now = ts(1_000_000);
        let mut state = StateMachine::default();
        state.policy.cost_weights = REFERENCE_WEIGHTS;
        let entity = quota_id(1);
        put_entity(&mut state, entity, None, "e", 1_000_000, 0, now);

        let id = job_id(1);
        // 1 core → 1 CU/s = 1_000_000 µCU/s; 2× priority; a declared 1h bound.
        let mut rec = spec_job(id, "img", entity, cpu(1000), 1_000_000);
        rec.spec.max_runtime = Some(Duration::from_secs(3600));
        rec.multiplier = PriorityMultiplier::from_integer(2);
        state.jobs.insert(id, rec);

        let cost = get_job(&state, &id, now).unwrap().cost;
        assert_eq!(cost.rate_ucu_per_second, 1_000_000);
        assert_eq!(cost.rate_breakdown.cpu, 1_000_000);
        assert_eq!(cost.rate_breakdown.memory, 0);
        assert_eq!(cost.rate_breakdown.disk, 0);
        assert_eq!(cost.priority_multiplier, 2.0);
        // Bounded: no unbounded surcharge.
        assert_eq!(cost.unbounded_multiplier, 1.0);
        assert_eq!(cost.effective_rate_ucu_per_second, 2_000_000);
        assert_eq!(cost.charge_window_seconds, 3600);
        assert!(!cost.charge_window_is_default);
        // 1_000_000 µCU/s × 3600 s × 2 = 7_200_000_000 µCU.
        assert_eq!(cost.estimated_ucu, 7_200_000_000);
        // Not placed: nothing charged yet.
        assert_eq!(cost.charged_ucu, 0);
        // Bounded default refund fraction (750 milli).
        assert_eq!(cost.refund_fraction, 0.75);
        // No measured-usage pipeline; no retained true-up.
        assert_eq!(cost.actual_ucu, None);
        assert!(cost.true_up.is_none());
    }

    #[test]
    fn get_job_cost_report_folds_the_unbounded_surcharge() {
        let now = ts(1_000_000);
        let mut state = StateMachine::default();
        state.policy.cost_weights = REFERENCE_WEIGHTS;
        let entity = quota_id(1);
        put_entity(&mut state, entity, None, "e", 1_000_000, 0, now);

        let id = job_id(1);
        // No max_runtime: the default 2× unbounded multiplier folds in and the
        // charge window is the policy default.
        let mut rec = spec_job(id, "img", entity, cpu(1000), 1_000_000);
        rec.spec.max_runtime = None;
        state.jobs.insert(id, rec);

        let cost = get_job(&state, &id, now).unwrap().cost;
        assert_eq!(cost.unbounded_multiplier, 2.0);
        assert_eq!(cost.priority_multiplier, 1.0);
        assert_eq!(cost.effective_rate_ucu_per_second, 2_000_000);
        assert_eq!(
            cost.charge_window_seconds,
            state.policy.default_charge_runtime_s
        );
        assert!(cost.charge_window_is_default);
        // Unbounded jobs get a full refund fraction.
        assert_eq!(cost.refund_fraction, 1.0);
    }

    #[test]
    fn get_job_queue_explainer_reports_the_penalty_chain() {
        let now = ts(1_000_000);
        let mut state = StateMachine::default();
        // A two-level ancestry, stamped at `now` so usage does not decay.
        let root = quota_id(1);
        let leaf = quota_id(2);
        // Root: 2× over quota → penalty 4 (quadratic default exponent).
        put_entity(&mut state, root, None, "team", 1_000_000, 2_000_000, now);
        // Leaf: within quota → penalty 1.
        put_entity(&mut state, leaf, Some(root), "user", 1_000_000, 0, now);

        let a = job_id(1);
        let mut ra = test_job(a, JobState::Queued, now);
        ra.spec.quota_entity = leaf;
        ra.multiplier = PriorityMultiplier::from_integer(2);
        state.jobs.insert(a, ra);

        let qa = get_job(&state, &a, now)
            .unwrap()
            .queue
            .expect("A is queued");
        assert_eq!(qa.multiplier, 2.0);
        // Chain is leaf → root.
        assert_eq!(qa.penalty_chain.len(), 2);
        assert_eq!(qa.penalty_chain[0].entity, leaf);
        assert_eq!(qa.penalty_chain[0].penalty, 1.0);
        let link = &qa.penalty_chain[1];
        assert_eq!(link.entity, root);
        assert_eq!(link.usage_ucu, 2_000_000);
        assert_eq!(link.quota_ucu, 1_000_000);
        assert_eq!(link.over_quota_ratio, 2.0);
        assert_eq!(link.penalty, 4.0);
        // The product composes the chain's links: 1 × 4.
        assert_eq!(qa.penalty_product, 4.0);
        assert_eq!(qa.age_seconds, 0);
    }

    #[test]
    fn get_job_accrual_present_only_while_accruing() {
        let now = ts(0);
        let node = NodeId::new();
        let job = job_id(1);
        let attempt = AttemptId::new();
        let alloc = AllocationId::new();
        let mut state = StateMachine::default();
        let mut rec = test_job(job, JobState::Attempting(attempt), ts(0));
        rec.attempts = vec![attempt];
        state.jobs.insert(job, rec);
        let mut arec = test_attempt(attempt, job, node, AttemptState::Accruing);
        arec.attempt.allocation = alloc;
        state.attempts.insert(attempt, arec);
        state.allocations.insert(
            alloc,
            test_allocation(alloc, job, attempt, node, AllocationState::Accruing),
        );

        let detail = get_job(&state, &job, now).unwrap();
        let accrual = detail.accrual.expect("accruing");
        assert_eq!(accrual.allocation.id, alloc);
        // The attempt is surfaced too.
        assert_eq!(detail.attempts.len(), 1);
        // Attempting, not queued: no queue explainer.
        assert!(detail.queue.is_none());
    }

    // ---- quota entities ---------------------------------------------------

    /// A quota entity with the fields the projections read made explicit.
    /// `usage`'s accumulator is stamped at `last_update`, and `created_at ==
    /// updated_at` (the freshly created case).
    fn entity(
        parent: Option<QuotaEntityId>,
        name: &str,
        quota: u64,
        usage: u64,
        last_update: Timestamp,
        created_at: Timestamp,
    ) -> QuotaEntity {
        QuotaEntity {
            parent,
            name: name.to_string(),
            quota: CostUnits(quota),
            usage: coppice_core::quota::UsageState {
                usage: CostUnits(usage),
                last_update,
            },
            created_at,
            updated_at: created_at,
        }
    }

    /// An `Attempting` job in the given phase-driving attempt state, charged
    /// to `entity`.
    fn attempting_job(
        state: &mut StateMachine,
        id: JobId,
        entity_id: QuotaEntityId,
        node: NodeId,
        attempt_state: AttemptState,
    ) {
        let attempt = AttemptId::new();
        let mut record = spec_job(id, "img", entity_id, Resources::ZERO, 0);
        record.state = JobState::Attempting(attempt);
        record.attempts = vec![attempt];
        state.jobs.insert(id, record);
        state
            .attempts
            .insert(attempt, test_attempt(attempt, id, node, attempt_state));
    }

    #[test]
    fn list_quota_entities_projects_figures_and_subtree_counts() {
        let root = quota_id(1);
        let child = quota_id(2);
        let now = ts(2_000_000);
        let mut state = StateMachine::default();
        // No decay at read time (last_update == now): usage is the stored
        // accumulator, so ratio/penalty are exact. Root is 2× over quota →
        // quadratic penalty 4.0 at the default exponent.
        state.quota_entities.insert(
            root,
            entity(None, "root", 1_000_000, 2_000_000, now, ts(1_000_000)),
        );
        state.quota_entities.insert(
            child,
            entity(Some(root), "child", 4_000_000, 0, now, ts(1_500_000)),
        );
        // A queued job and a running job, both charged to the child.
        let node = NodeId::new();
        state.jobs.insert(
            job_id(1),
            spec_job(job_id(1), "img", child, Resources::ZERO, 0),
        );
        attempting_job(&mut state, job_id(2), child, node, AttemptState::Running);

        let response = list_quota_entities(&state, now);
        // BTreeMap id order: root (id 1) then child (id 2).
        assert_eq!(response.entities.len(), 2);
        let root_node = &response.entities[0];
        assert_eq!(root_node.id, root);
        assert_eq!(root_node.usage_ucu, 2_000_000);
        assert_eq!(root_node.over_quota_ratio, 2.0);
        assert_eq!(root_node.penalty, 4.0);
        assert_eq!(root_node.created_at, ts(1_000_000));
        // Subtree counts: both the child's jobs roll up to the root.
        assert_eq!(root_node.queued_count, 1);
        assert_eq!(root_node.running_count, 1);
        // The child counts its own jobs too.
        let child_node = &response.entities[1];
        assert_eq!(child_node.queued_count, 1);
        assert_eq!(child_node.running_count, 1);
        // Within quota → penalty 1.0.
        assert_eq!(child_node.over_quota_ratio, 0.0);
        assert_eq!(child_node.penalty, 1.0);
    }

    #[test]
    fn list_quota_entities_decays_usage_to_read_time() {
        let root = quota_id(1);
        let mut state = StateMachine::default();
        // Accumulator stamped at the epoch; read one default half-life later
        // (1440 × 60 s ticks) must show decayed — strictly less — usage.
        state.quota_entities.insert(
            root,
            entity(None, "root", 10_000_000, 1_000_000, ts(0), ts(0)),
        );
        let one_half_life = ts(86_400_000_000);
        let node = &list_quota_entities(&state, one_half_life).entities[0];
        assert!(node.usage_ucu < 1_000_000, "usage must decay to read time");
        assert!(node.usage_ucu > 0);
    }

    #[test]
    fn get_quota_entity_returns_none_for_missing() {
        let state = StateMachine::default();
        assert!(get_quota_entity(&state, &quota_id(99), ts(0)).is_none());
    }

    #[test]
    fn get_quota_entity_returns_chain_children_and_subtree_stats() {
        let root = quota_id(1);
        let mid = quota_id(2);
        let leaf = quota_id(3);
        let now = ts(2_000_000);
        let mut state = StateMachine::default();
        state
            .quota_entities
            .insert(root, entity(None, "root", 1_000_000, 0, now, ts(0)));
        state
            .quota_entities
            .insert(mid, entity(Some(root), "mid", 1_000_000, 0, now, ts(0)));
        state
            .quota_entities
            .insert(leaf, entity(Some(mid), "leaf", 1_000_000, 0, now, ts(0)));

        // Under the subtree of `mid`: a queued job on leaf, a running job on
        // mid. A job on the root is *outside* mid's subtree.
        let node = NodeId::new();
        state.jobs.insert(
            job_id(1),
            spec_job(job_id(1), "img", leaf, Resources::ZERO, 0),
        );
        attempting_job(&mut state, job_id(2), mid, node, AttemptState::Running);
        state.jobs.insert(
            job_id(3),
            spec_job(job_id(3), "img", root, Resources::ZERO, 0),
        );

        let detail = get_quota_entity(&state, &mid, now).expect("mid exists");
        assert_eq!(detail.entity.id, mid);
        // Chain is root-first, this entity last.
        let chain_ids: Vec<_> = detail.chain.iter().map(|v| v.id).collect();
        assert_eq!(chain_ids, vec![root, mid]);
        // Only direct children.
        let child_ids: Vec<_> = detail.children.iter().map(|c| c.id).collect();
        assert_eq!(child_ids, vec![leaf]);
        // Subtree stats cover mid + leaf, not the root's own job.
        assert_eq!(detail.stats.by_state[&dto::JobPhase::Queued], 1);
        assert_eq!(detail.stats.by_state[&dto::JobPhase::Running], 1);
        // Oldest queued job submitted at 0, read at 2 s → age 2 s.
        assert_eq!(detail.stats.oldest_queued_age_seconds, Some(2));
        // The one running attempt's fixture rate (100 µCU/s).
        assert_eq!(detail.stats.burn_rate_ucu_per_second, 100);
        // Unbacked stats: null / empty, never fabricated.
        assert_eq!(detail.stats.charged_ucu_24h, None);
        assert!(detail.stats.usage_history.is_empty());
        // Every phase present at zero-or-more, none missing.
        assert_eq!(detail.stats.by_state.len(), dto::JobPhase::ALL.len());
    }

    #[test]
    fn get_quota_entity_chain_and_counts_are_depth_capped() {
        // A chain longer than the depth cap is walked at most
        // QUOTA_TREE_DEPTH_CAP hops, exactly as apply and the counts do.
        let mut state = StateMachine::default();
        let depth = QUOTA_TREE_DEPTH_CAP + 5;
        for i in 0..depth {
            let id = quota_id(i as u16 + 1);
            let parent = (i > 0).then(|| quota_id(i as u16));
            state
                .quota_entities
                .insert(id, entity(parent, "e", 1_000_000, 0, ts(0), ts(0)));
        }
        let leaf = quota_id(depth as u16);
        let detail = get_quota_entity(&state, &leaf, ts(0)).expect("leaf exists");
        // The chain is capped at the depth cap, not the full ancestry.
        assert_eq!(detail.chain.len(), QUOTA_TREE_DEPTH_CAP as usize);
    }
}
