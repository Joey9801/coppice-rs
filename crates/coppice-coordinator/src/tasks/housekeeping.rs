//! Housekeeping (leader-only; a 60 s tick in every deployment).
//!
//! Each tick runs three passes over the latest applied view, in order:
//!
//! 1. **Liveness** — nodes past [`AGENT_LIVENESS_DEADLINE`] are declared lost
//!    (ADR 0009's health monitor).
//! 2. **Node retention** — node records that stopped accepting placements,
//!    hold no live allocation, and have been silent for
//!    `policy.node_retention` are evicted (ADR 0041).
//! 3. **Terminal-job retention** — the pass described below.
//!
//! All three measure their own clock proposer-side and propose; apply stays
//! deterministic and idempotent under a leader change.
//!
//! Both eviction passes propose in **capped batches**: at most
//! [`MAX_EVICTIONS_PER_COMMAND`] ids per command and at most
//! [`MAX_EVICTION_BATCHES_PER_PASS`] commands per tick, oldest first, with
//! whatever is left over waiting for the next sweep (issue #155).
//!
//! Scans the view for terminal jobs past retention and removes them from
//! replicated state with an `EvictTerminalJobs` proposal. What gates that
//! proposal is the configured `[history]` mode (ADR 0012), which the daemon
//! threads in as a [`HistorySink`]:
//!
//! - a **durable store** (`clickhouse`, still future work) is written first —
//!   an external network call, therefore outside apply, with retries — and the
//!   eviction is proposed only once that write is durable, so a job leaving
//!   consensus state is always already queryable from history;
//! - **`none`** is the explicitly lossy mode: there is nothing to write to, so
//!   the replicated `terminal_retention` TTL is the whole gate and the
//!   evicted job's history is simply discarded. Nothing in this mode reports a
//!   history write, durable or otherwise (issue #43).
//!
//! See `docs/architecture/coordinator-runtime.md`, "Housekeeping".
//!
//! Snapshot cadence is **not** this task's job. openraft drives it from
//! `SnapshotPolicy::LogsSinceLast(snapshot_log_entries)` with
//! `max_in_snapshot_log_to_keep = snapshot_keep_log_entries`, both configured
//! in `[raft]` and passed through at node assembly
//! (`coppice-consensus::node`) — coalescing, retry, and the post-snapshot
//! purge (ADR 0017) all live there. `Consensus::trigger_snapshot` remains for
//! operators and tests that need a snapshot *now*.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::time::interval;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateView, StateViews};
use coppice_core::allocation::AllocationState;
use coppice_core::id::{JobId, NodeId};
use coppice_core::job::JobState;
use coppice_core::time::Timestamp;
use coppice_state::command::{DeclareNodeLost, EvictNodes, EvictTerminalJobs};
use coppice_state::Command;

use crate::leadership;
use crate::limits::{
    AGENT_LIVENESS_DEADLINE, MAX_EVICTIONS_PER_COMMAND, MAX_EVICTION_BATCHES_PER_PASS,
};
use crate::liveness::NodeLiveness;

/// Where this daemon's terminal-job history goes (ADR 0012) — the witness
/// that the `[history]` config section was read and stated a mode.
///
/// [`None`](HistorySink::None) is the explicitly lossy mode: the replicated
/// `policy.terminal_retention` TTL is the whole gate on `EvictTerminalJobs`,
/// standing in for the durable-receipt gate a real store provides, and the
/// evicted job's history is discarded. Nothing in this mode may ever claim a
/// history write happened, let alone a durable one — the mode exists so a
/// deployment can be lossy honestly rather than by accident (issue #43). A
/// `Durable(...)` variant arrives with the real history store, and the
/// write-then-evict ordering with it.
#[derive(Debug, Clone, Copy)]
pub enum HistorySink {
    None,
}

/// A terminal job as handed to the history store.
///
/// Built on every pass and, in the `none` mode, dropped again unread: this is
/// the seam the future durable store consumes, and keeping it assembled here
/// means the retention scan — the part that decides *which* jobs leave — does
/// not have to change when that store lands. Hence
/// `state`/`submitted_at`/`terminal_at` having no reader yet.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TerminalJobRecord {
    pub job: JobId,
    pub state: JobState,
    pub submitted_at: Timestamp,
    /// When the job reached its terminal state; the retention scan measured
    /// eligibility from this (never from `submitted_at` — KOI-1).
    pub terminal_at: Timestamp,
}

/// Run the housekeeping loop until shutdown.
///
/// `tick` is how often a leader sweeps: [`limits::HOUSEKEEPING_INTERVAL`] in
/// every deployment, and a short value only where a test or a dev cluster
/// would otherwise sit out a production tick to watch one retention TTL
/// expire ([`crate::config::PacingConfig`]'s `housekeeping_interval`). It is
/// pure liveness — the sweep's *verdict* is the replicated
/// `terminal_retention` measured against wall-clock `terminal_at`, so a
/// faster tick can only notice a due job sooner, never make one due.
///
/// [`limits::HOUSEKEEPING_INTERVAL`]: crate::limits::HOUSEKEEPING_INTERVAL
pub async fn run<C>(
    consensus: Arc<C>,
    views: StateViews,
    history: HistorySink,
    tick: Duration,
    liveness: NodeLiveness,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) where
    C: Consensus,
{
    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::debug!(term, "housekeeping: gained leadership");

        // Grant every known node a fresh liveness grace window so a node is
        // never declared lost on the first tick of a new leadership term.
        // Seeding the term also retires whatever a prior term left in the
        // map (`crate::liveness`).
        liveness.seed(
            term,
            views.latest().state().nodes.keys().copied(),
            Instant::now(),
        );

        let mut ticker = interval(tick);
        // The first tick fires immediately; skip it so gaining leadership
        // doesn't itself trigger an instant sweep.
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                _ = ticker.tick() => {
                    declare_lost_nodes(&consensus, &views, &liveness).await;
                    evict_silent_nodes(&consensus, &views, &liveness).await;
                    run_pass(&consensus, &views, history).await;
                }
            }
        }
    }
}

/// Declare every node that has missed the liveness deadline (ADR 0009 health
/// monitor).
async fn declare_lost_nodes<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    liveness: &NodeLiveness,
) {
    let view = views.latest();
    let stale = stale_nodes(&view, liveness, Instant::now());
    if stale.is_empty() {
        return;
    }
    // Proposer-side wall clock: housekeeping runs outside apply.
    let declared_at = Timestamp::now();
    for node in stale {
        let command = Command::DeclareNodeLost(DeclareNodeLost { node, declared_at });
        match consensus.propose(command).await {
            Ok(Applied { outcome: Ok(_), .. }) => {
                tracing::info!(%node, "housekeeping: node missed the liveness deadline, declared lost");
            }
            Ok(Applied {
                outcome: Err(reason),
                ..
            }) => {
                // `UnknownNode` is benign (the node was removed meanwhile).
                tracing::debug!(%node, ?reason, "housekeeping: DeclareNodeLost rejected");
            }
            Err(e) if e.is_retryable() => {
                tracing::info!(%node, error = %e, "housekeeping: retryable DeclareNodeLost error");
                return;
            }
            Err(e) => {
                tracing::error!(%node, error = %e, "housekeeping: fatal DeclareNodeLost error");
                return;
            }
        }
    }
}

/// The nodes whose last report is older than [`AGENT_LIVENESS_DEADLINE`] and
/// that still accept placements or hold a non-`Released` allocation.
///
/// This guard stops us re-declaring an already-lost node every tick:
/// `DeclareNodeLost` leaves it unschedulable with everything `Released`, and
/// a drained agent (ADR 0041) that went quiet is the same case — its record
/// is the retention GC's to collect. A node with no liveness mark at all is
/// left alone; real nodes are always seeded on leadership gain and marked on
/// every report.
fn stale_nodes(view: &StateView, liveness: &NodeLiveness, now: Instant) -> Vec<NodeId> {
    let mut out = Vec::new();
    for (node_id, node_record) in view.state().nodes.iter() {
        let overdue = match liveness.last_seen(*node_id) {
            Some(seen) => now.duration_since(seen) >= AGENT_LIVENESS_DEADLINE,
            None => false,
        };
        if !overdue {
            continue;
        }
        if node_record.accepts_placements() || has_live_allocation(view, *node_id) {
            out.push(*node_id);
        }
    }
    out
}

/// Whether `node` still holds an allocation that is not `Released` — the
/// "still has work on it" predicate shared by the liveness monitor, the
/// retention GC, and `EvictNodes`' own apply-side validation (ADR 0041).
fn has_live_allocation(view: &StateView, node: NodeId) -> bool {
    view.state()
        .allocations
        .values()
        .any(|a| a.allocation.node == node && a.allocation.state != AllocationState::Released)
}

/// Evict the node records whose full `node_retention` window of silence has
/// elapsed (ADR 0041).
///
/// Capped batches per tick, longest-silent first: apply skips ids that are
/// already gone, so a re-issued proposal is idempotent across a leader change,
/// and a backlog that does not fit this tick's budget simply waits for the
/// next one. This pass applies exactly the conditions apply re-checks.
async fn evict_silent_nodes<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    liveness: &NodeLiveness,
) {
    let view = views.latest();
    let due = due_for_node_eviction(
        &view,
        liveness,
        Instant::now(),
        MAX_EVICTIONS_PER_COMMAND * MAX_EVICTION_BATCHES_PER_PASS,
    );
    // An empty `due` is the only outcome of an inexpressible window, so the
    // fallback is never consulted.
    let retention = view
        .state()
        .policy
        .node_retention
        .to_std()
        .unwrap_or(Duration::MAX);
    let still_silent = |node: NodeId| match liveness.last_seen(node) {
        Some(seen) => Instant::now().saturating_duration_since(seen) >= retention,
        None => false,
    };
    propose_node_evictions(consensus, &due, MAX_EVICTIONS_PER_COMMAND, still_silent).await;
}

/// Propose `due` as `EvictNodes` commands of at most `per_command` ids each,
/// one at a time, stopping at the first rejection or error.
///
/// Sequential on purpose: the batches are disjoint, so nothing is lost by
/// leaving the tail to the next tick, and a single in-flight proposal keeps
/// this pass from monopolising the log. Chunking also narrows a rejection's
/// blast radius — apply rejects a whole `EvictNodes` batch if any one listed
/// node turns out ineligible, so a capped batch loses at most `per_command`
/// evictions to one bad id instead of the entire backlog.
///
/// `still_silent` is asked about every node immediately before its batch is
/// proposed. `due` was fixed before the first proposal, and the liveness map
/// keeps moving while earlier batches replicate: a node that resumed
/// reporting in that time must keep its record (and its cordon), so it is
/// dropped from its batch here rather than evicted on a stale verdict. The
/// race between this check and apply remains, as it always did for a single
/// batch — apply cannot see leader-local liveness — but it no longer grows
/// with the length of the pass.
async fn propose_node_evictions<C: Consensus>(
    consensus: &Arc<C>,
    due: &[NodeId],
    per_command: usize,
    still_silent: impl Fn(NodeId) -> bool,
) {
    let mut evicted = 0usize;
    for batch in due.chunks(per_command.max(1)) {
        let batch: Vec<NodeId> = batch.iter().copied().filter(|n| still_silent(*n)).collect();
        if batch.is_empty() {
            continue;
        }
        // Proposer-side wall clock: housekeeping runs outside apply.
        let command = Command::EvictNodes(EvictNodes {
            nodes: batch.clone(),
            // The retention GC is machine-proposed; only the admin API's
            // explicit `node remove` carries an actor.
            actor: None,
            evicted_at: Timestamp::now(),
        });
        match consensus.propose(command).await {
            // `EvictNodes` emits no events, so the applied batch's own length
            // is the best count available — an over-count only where apply
            // skipped an id that was already gone.
            Ok(Applied { outcome: Ok(_), .. }) => evicted += batch.len(),
            Ok(Applied {
                outcome: Err(reason),
                ..
            }) => {
                tracing::warn!(?reason, "housekeeping: EvictNodes rejected");
                break;
            }
            Err(e) if e.is_retryable() => {
                tracing::info!(error = %e, "housekeeping: retryable EvictNodes error");
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "housekeeping: fatal EvictNodes error");
                break;
            }
        }
    }
    if evicted > 0 {
        tracing::info!(
            count = evicted,
            "housekeeping: evicted node records silent past the retention window"
        );
    }
}

/// The nodes eligible for `EvictNodes`: they have stopped accepting
/// placements (an admin cordon, an agent's own drain announcement, or
/// `DeclareNodeLost`), hold no live allocation, and have been silent for at
/// least `policy.node_retention`.
///
/// Silence is measured exactly as [`stale_nodes`] measures it, against the
/// leader-local liveness marks of ADR 0040: a node with no mark at all is
/// not considered silent. A leader change therefore only ever delays an
/// eviction, never hastens one.
///
/// At most `limit` records come back, longest-silent first (oldest
/// `last_seen`, ties broken by [`NodeId`] so the order is deterministic): the
/// records that have been dead longest leave first, and the remainder waits
/// for the next sweep (issue #155).
fn due_for_node_eviction(
    view: &StateView,
    liveness: &NodeLiveness,
    now: Instant,
    limit: usize,
) -> Vec<NodeId> {
    let Some(retention) = view.state().policy.node_retention.to_std() else {
        // A negative window is not expressible as a monotonic span; treat it
        // as "never due" rather than evicting everything.
        tracing::warn!("housekeeping: node_retention is negative, skipping the node retention GC");
        return Vec::new();
    };
    let due: Vec<(Instant, NodeId)> = view
        .state()
        .nodes
        .iter()
        .filter(|(_, record)| !record.accepts_placements())
        .filter_map(|(node_id, _)| {
            let seen = liveness.last_seen(*node_id)?;
            (now.saturating_duration_since(seen) >= retention).then_some((seen, *node_id))
        })
        .filter(|(_, node_id)| !has_live_allocation(view, *node_id))
        .collect();
    oldest_first(due, limit, |(seen, node_id)| (*seen, *node_id))
        .into_iter()
        .map(|(_, node_id)| node_id)
        .collect()
}

/// The `limit` smallest items by `key`, sorted.
///
/// The partial selection matters at design scale: a backlog of a million due
/// records must not be fully sorted to find the few thousand that fit this
/// pass's budget, so anything over the limit is partitioned in linear time and
/// only the survivors are ordered.
fn oldest_first<T, K: Ord, F: Fn(&T) -> K>(mut items: Vec<T>, limit: usize, key: F) -> Vec<T> {
    if items.len() > limit {
        items.select_nth_unstable_by_key(limit, &key);
        items.truncate(limit);
    }
    items.sort_unstable_by_key(&key);
    items
}

async fn run_pass<C: Consensus>(consensus: &Arc<C>, views: &StateViews, history: HistorySink) {
    let view = views.latest();
    // Proposer-side wall clock: safe here because housekeeping runs outside
    // apply (`docs/architecture/coordinator-runtime.md`, "Housekeeping").
    let now = Timestamp::now();

    let budget = MAX_EVICTIONS_PER_COMMAND * MAX_EVICTION_BATCHES_PER_PASS;
    let due = due_for_eviction(&view, now, budget);

    if due.is_empty() {
        return;
    }
    if due.len() == budget {
        tracing::debug!(
            budget,
            "housekeeping: the eviction backlog filled this pass's budget, \
             the remainder waits for the next tick"
        );
    }

    propose_evictions(consensus, history, &due, now, MAX_EVICTIONS_PER_COMMAND).await;
}

/// Propose `due` as `EvictTerminalJobs` commands of at most `per_command` jobs
/// each, one at a time, stopping at the first rejection or error.
///
/// Sequential and capped for the reasons in [`MAX_EVICTIONS_PER_COMMAND`]: one
/// raft entry, one serial apply and one SSE frame per batch, with `due` fixed
/// from the single view snapshot the caller took. Nothing re-scans between
/// batches — the published view lags the applies, and the list is already
/// disjoint, so the next tick is the right place for whatever is left.
async fn propose_evictions<C: Consensus>(
    consensus: &Arc<C>,
    history: HistorySink,
    due: &[TerminalJobRecord],
    now: Timestamp,
    per_command: usize,
) {
    let batches = due.len().div_ceil(per_command.max(1));
    // The configured mode is the gate on what has to happen before the
    // proposal: a durable store is written here first, and `none` has nothing
    // to write to, so the TTL that made these jobs due is the whole of it
    // (ADR 0012, issue #43). Nothing is claimed about the *outcome* yet —
    // eligibility is not eviction, and a proposal can still be rejected or
    // lost to a leadership change, so the discard is reported below, from the
    // branch that knows it applied.
    match history {
        HistorySink::None => tracing::debug!(
            count = due.len(),
            batches,
            "housekeeping: terminal jobs past the TTL, proposing eviction (history = \"none\")"
        ),
    }

    let mut evicted = 0usize;
    for batch in due.chunks(per_command.max(1)) {
        let command = Command::EvictTerminalJobs(EvictTerminalJobs {
            jobs: batch.iter().map(|r| r.job).collect(),
            evicted_at: now,
        });
        match consensus.propose(command).await {
            // Applied: this is the first point at which anything may be said
            // about what happened to these jobs' history — and under `none`
            // what happened is that it went away. The count comes from the
            // apply outcome's `JobEvicted` events, not from `due` or the batch
            // length: apply skips ids that are already gone (that skip is what
            // makes duplicate proposals across a leadership change idempotent),
            // so the proposed count can overstate what was actually removed —
            // down to nothing at all, in which case no history was discarded
            // here and nothing says it was.
            Ok(Applied {
                outcome: Ok(applied),
                ..
            }) => {
                evicted += applied
                    .events
                    .iter()
                    .filter(|e| matches!(e, coppice_state::Event::JobEvicted { .. }))
                    .count();
            }
            // A rejection or an error ends the pass: the remaining batches are
            // still due next tick, and retrying them now would most likely hit
            // whatever stopped this one.
            Ok(Applied {
                outcome: Err(reason),
                ..
            }) => {
                tracing::debug!(?reason, "housekeeping: EvictTerminalJobs rejected");
                break;
            }
            Err(e) if e.is_retryable() => {
                tracing::info!(error = %e, "housekeeping: retryable propose error");
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "housekeeping: fatal propose error");
                break;
            }
        }
    }

    // One summary per pass, covering every batch that applied — including when
    // a later batch stopped the pass.
    if evicted > 0 {
        match history {
            HistorySink::None => tracing::info!(
                count = evicted,
                "housekeeping: history = \"none\": evicted terminal jobs past the TTL; \
                 their history is discarded (ADR 0012 lossy mode)"
            ),
        }
    }
}

/// The terminal jobs whose full post-terminal retention interval has
/// elapsed (ADR 0012).
///
/// The clock runs from `terminal_at`, never from submission: a
/// low-priority job may legitimately queue longer than the retention
/// interval before it ever runs, and must still get the full interval after
/// it finishes (KOI-1). A terminal job with no `terminal_at` — a record
/// that reached terminal state before the field existed — is never
/// considered due; retention leaks are recoverable, evictions are not.
///
/// The whole view is scanned, but at most `limit` records come back, oldest
/// `terminal_at` first with ties broken by [`JobId`] so the order is
/// deterministic. That is fair progress under a backlog: the longest-overdue
/// jobs leave first and the remainder waits for the next sweep, instead of an
/// arbitrary slice of the map being evicted forever ahead of older jobs
/// (issue #155).
fn due_for_eviction(view: &StateView, now: Timestamp, limit: usize) -> Vec<TerminalJobRecord> {
    let retention = view.state().policy.terminal_retention;
    let mut unstamped: u64 = 0;
    let due: Vec<TerminalJobRecord> = view
        .state()
        .jobs
        .iter()
        .filter(|(_, record)| record.state.is_terminal())
        .filter_map(|(id, record)| {
            let Some(terminal_at) = record.terminal_at else {
                unstamped += 1;
                return None;
            };
            (now - terminal_at >= retention).then_some(TerminalJobRecord {
                job: *id,
                state: record.state,
                submitted_at: record.submitted_at,
                terminal_at,
            })
        })
        .collect();
    if unstamped > 0 {
        tracing::warn!(
            count = unstamped,
            "housekeeping: terminal jobs without a terminal timestamp are exempt from eviction"
        );
    }
    oldest_first(due, limit, |r| (r.terminal_at, r.job))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    // The liveness deadline is an `Instant` span, so it stays std; the
    // retention fixtures below are domain spans.
    use std::time::Duration as StdDuration;

    use coppice_core::time::Duration;

    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use coppice_core::resource::Resources;
    use coppice_state::{PolicyConfig, StateMachine};

    use crate::limits::HOUSEKEEPING_INTERVAL;
    use crate::test_support::{
        allocation_record, job_record, node_record, view_of, FakeConsensus, ProposeOutcome,
    };

    #[test]
    fn stale_nodes_picks_schedulable_and_live_but_not_already_lost_or_fresh() {
        // Anchor arithmetic on `base` and add (never subtract) to avoid
        // underflowing the monotonic clock on a freshly started process.
        let base = Instant::now();
        let now = base + AGENT_LIVENESS_DEADLINE + StdDuration::from_secs(2);
        let overdue = base;

        let schedulable_stale = NodeId::new();
        let drained_live = NodeId::new();
        let drained_lost = NodeId::new();
        let fresh = NodeId::new();

        let mut sm = StateMachine::default();
        sm.nodes
            .insert(schedulable_stale, node_record(schedulable_stale, 1, true));
        sm.nodes
            .insert(drained_live, node_record(drained_live, 1, false));
        sm.nodes
            .insert(drained_lost, node_record(drained_lost, 1, false));
        sm.nodes.insert(fresh, node_record(fresh, 1, true));
        // A non-`Released` allocation keeps `drained_live` live.
        let alloc = AllocationId::new();
        sm.allocations.insert(
            alloc,
            allocation_record(
                alloc,
                JobId::new(),
                AttemptId::new(),
                drained_live,
                Resources::ZERO,
                AllocationState::Active,
            ),
        );
        let view = view_of(sm);

        let liveness = NodeLiveness::new();
        liveness.seed(1, [schedulable_stale, drained_live, drained_lost], overdue);
        liveness.seed(1, [fresh], now);

        let stale: BTreeSet<NodeId> = stale_nodes(&view, &liveness, now).into_iter().collect();
        assert!(stale.contains(&schedulable_stale));
        assert!(stale.contains(&drained_live));
        // Unschedulable with no live allocation: already lost, not re-declared.
        assert!(!stale.contains(&drained_lost));
        // Within its liveness grace window.
        assert!(!stale.contains(&fresh));
    }

    /// The retention GC's conditions, one node each (ADR 0041): silent past
    /// the window and holding nothing is due; anything still schedulable,
    /// still holding work, or still being heard from is not.
    #[test]
    fn nodes_are_evicted_only_once_drained_empty_and_silent() {
        let retention = PolicyConfig::default()
            .node_retention
            .to_std()
            .expect("the default window is positive");
        // Anchor on `base` and add, never subtract: the monotonic clock can be
        // younger than the retention window on a freshly started process.
        let base = Instant::now();
        let now = base + retention + StdDuration::from_secs(1);

        let drained_silent = NodeId::new();
        let schedulable_silent = NodeId::new();
        let drained_busy = NodeId::new();
        let drained_recent = NodeId::new();
        let lost_silent = NodeId::new();
        let untracked = NodeId::new();

        let mut sm = StateMachine::default();
        for (id, schedulable) in [
            (drained_silent, false),
            (schedulable_silent, true),
            (drained_busy, false),
            (drained_recent, false),
            (untracked, false),
        ] {
            sm.nodes.insert(id, node_record(id, 1, schedulable));
        }
        // A node the health monitor already declared lost: unschedulable with
        // every allocation released. Its record is precisely what this GC is
        // for — a churning ASG would otherwise accumulate them forever.
        sm.nodes
            .insert(lost_silent, node_record(lost_silent, 1, false));
        // `drained_busy` still holds a non-`Released` allocation.
        let alloc = AllocationId::new();
        sm.allocations.insert(
            alloc,
            allocation_record(
                alloc,
                JobId::new(),
                AttemptId::new(),
                drained_busy,
                Resources::ZERO,
                AllocationState::Active,
            ),
        );
        // A released allocation is not live and must not hold a record back.
        let released = AllocationId::new();
        sm.allocations.insert(
            released,
            allocation_record(
                released,
                JobId::new(),
                AttemptId::new(),
                drained_silent,
                Resources::ZERO,
                AllocationState::Released,
            ),
        );
        let view = view_of(sm);

        let liveness = NodeLiveness::new();
        liveness.seed(
            1,
            [
                drained_silent,
                schedulable_silent,
                drained_busy,
                lost_silent,
            ],
            base,
        );
        liveness.seed(1, [drained_recent], now);
        // `untracked` is deliberately left out of the map entirely.

        let due: BTreeSet<NodeId> = due_for_node_eviction(&view, &liveness, now, usize::MAX)
            .into_iter()
            .collect();
        assert!(due.contains(&drained_silent));
        assert!(due.contains(&lost_silent));
        // Still taking work: an operator undrained it, or it never drained.
        assert!(!due.contains(&schedulable_silent));
        // Still holding a live allocation: apply would reject the batch.
        assert!(!due.contains(&drained_busy));
        // Heard from inside the window: a node drained for maintenance whose
        // agent is still up keeps its record (and its cordon).
        assert!(!due.contains(&drained_recent));
        // No mark at all — the same rule `stale_nodes` follows.
        assert!(!due.contains(&untracked));

        // Nothing is due before the full window has elapsed.
        assert!(due_for_node_eviction(
            &view,
            &liveness,
            base + retention - StdDuration::from_secs(1),
            usize::MAX
        )
        .is_empty());
    }

    /// An agent that announced its own shutdown (ADR 0041) and then went away
    /// travels the same path: `draining` alone stops placements, so the record
    /// becomes due without any admin cordon or `DeclareNodeLost`.
    #[test]
    fn an_agent_announced_drain_is_enough_to_make_a_record_due() {
        let retention = PolicyConfig::default()
            .node_retention
            .to_std()
            .expect("the default window is positive");
        let base = Instant::now();
        let now = base + retention + StdDuration::from_secs(1);

        let node = NodeId::new();
        let mut sm = StateMachine::default();
        let mut record = node_record(node, 1, true);
        record.draining = true;
        sm.nodes.insert(node, record);
        let view = view_of(sm);

        let liveness = NodeLiveness::new();
        liveness.seed(1, [node], base);

        assert_eq!(
            due_for_node_eviction(&view, &liveness, now, usize::MAX),
            vec![node]
        );
    }

    /// A terminal job record with the given submission and terminal times.
    fn terminal_job(
        id: JobId,
        submitted_at: Timestamp,
        terminal_at: Option<Timestamp>,
    ) -> coppice_state::JobRecord {
        let mut r = job_record(id, "img", Resources::ZERO, None);
        r.state = JobState::Succeeded;
        r.submitted_at = submitted_at;
        r.terminal_at = terminal_at;
        r
    }

    #[test]
    fn eviction_runs_a_full_retention_from_the_terminal_transition() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);

        let done_long_ago = JobId::new();
        let long_queued_just_done = JobId::new();
        let ancient_but_live = JobId::new();
        let terminal_unstamped = JobId::new();

        let mut sm = StateMachine::default();
        // Finished a full retention interval ago: due.
        sm.jobs.insert(
            done_long_ago,
            terminal_job(
                done_long_ago,
                now - retention.saturating_mul(3),
                Some(now - retention),
            ),
        );
        // Queued for three retention intervals before running — the cheap
        // low-priority-job pattern — but finished only now: NOT due. The
        // clock runs from the terminal transition, never submission (KOI-1).
        sm.jobs.insert(
            long_queued_just_done,
            terminal_job(
                long_queued_just_done,
                now - retention.saturating_mul(3),
                Some(now - Duration::from_micros(10)),
            ),
        );
        // Still waiting on the queue after all that time: not terminal,
        // never a candidate no matter its age.
        let mut live = job_record(ancient_but_live, "img", Resources::ZERO, None);
        live.state = JobState::Queued;
        live.submitted_at = now - retention.saturating_mul(3);
        sm.jobs.insert(ancient_but_live, live);
        // Terminal but unstamped (reached terminal state before the field
        // existed): exempt — a retention leak beats an early eviction.
        sm.jobs.insert(
            terminal_unstamped,
            terminal_job(terminal_unstamped, now - retention.saturating_mul(3), None),
        );

        let view = view_of(sm);
        let due = due_for_eviction(&view, now, usize::MAX);
        assert_eq!(
            due.iter().map(|r| r.job).collect::<Vec<_>>(),
            vec![done_long_ago]
        );
        assert_eq!(due[0].terminal_at, now - retention);

        // The moment the post-terminal interval elapses, the long-queued job
        // becomes due too.
        let later = now + retention;
        let due_later: BTreeSet<JobId> = due_for_eviction(&view, later, usize::MAX)
            .into_iter()
            .map(|r| r.job)
            .collect();
        assert!(due_later.contains(&done_long_ago));
        assert!(due_later.contains(&long_queued_just_done));
        assert!(!due_later.contains(&ancient_but_live));
        assert!(!due_later.contains(&terminal_unstamped));
    }

    /// A state machine holding one due terminal job per entry of `ages`, each
    /// terminal `age * retention` before `now`. Returns the ids paired with
    /// their ages so a test can state the expected oldest-first order without
    /// depending on the (random) id order the map iterates in.
    fn due_jobs(now: Timestamp, ages: &[i64]) -> (StateMachine, Vec<(i64, JobId)>) {
        let retention = PolicyConfig::default().terminal_retention;
        let mut sm = StateMachine::default();
        let mut ids = Vec::new();
        for age in ages {
            let id = JobId::new();
            let terminal_at = now - retention.saturating_mul(*age);
            sm.jobs.insert(
                id,
                terminal_job(id, Timestamp::UNIX_EPOCH, Some(terminal_at)),
            );
            ids.push((*age, id));
        }
        (sm, ids)
    }

    /// Every id of the `EvictTerminalJobs` commands proposed, in order.
    fn proposed_evictions(consensus: &FakeConsensus) -> Vec<EvictTerminalJobs> {
        consensus
            .proposed()
            .into_iter()
            .filter_map(|c| match c {
                Command::EvictTerminalJobs(evict) => Some(evict),
                _ => None,
            })
            .collect()
    }

    /// The scan hands back the *longest-overdue* jobs, in order, and no more
    /// than the budget allows (issue #155): a backlog drains oldest first
    /// rather than by whatever slice of the map the iterator reached.
    #[test]
    fn the_due_list_is_oldest_first_and_bounded() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);
        // Ages in scrambled insertion order; a larger age is older.
        let (sm, mut ids) = due_jobs(now, &[3, 1, 5, 2, 4]);
        let view = view_of(sm);

        ids.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
        let oldest_three: Vec<JobId> = ids.iter().take(3).map(|(_, id)| *id).collect();

        let due = due_for_eviction(&view, now, 3);
        assert_eq!(due.iter().map(|r| r.job).collect::<Vec<_>>(), oldest_three);
    }

    /// Jobs that went terminal in the same microsecond are ordered by id, so
    /// two leaders scanning the same state propose the same batch.
    #[test]
    fn jobs_terminal_at_the_same_instant_are_ordered_by_id() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);
        let (sm, ids) = due_jobs(now, &[2, 2, 2]);
        let view = view_of(sm);

        let mut by_id: Vec<JobId> = ids.iter().map(|(_, id)| *id).collect();
        by_id.sort();
        let due = due_for_eviction(&view, now, 2);
        assert_eq!(due.iter().map(|r| r.job).collect::<Vec<_>>(), by_id[..2]);
    }

    /// A backlog over the per-command cap becomes several commands, proposed
    /// one after another, that between them name exactly the due list in
    /// order — and all carry the one clock reading the pass took.
    #[tokio::test]
    async fn a_backlog_is_proposed_in_capped_batches() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);
        let (sm, _) = due_jobs(now, &[1, 2, 3, 4, 5]);
        let view = view_of(sm);
        let due = due_for_eviction(&view, now, usize::MAX);
        assert_eq!(due.len(), 5);

        let (consensus, _publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        let consensus = Arc::new(consensus);
        propose_evictions(&consensus, HistorySink::None, &due, now, 2).await;

        let evictions = proposed_evictions(&consensus);
        assert_eq!(
            evictions.iter().map(|e| e.jobs.len()).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let proposed: Vec<JobId> = evictions.iter().flat_map(|e| e.jobs.clone()).collect();
        assert_eq!(proposed, due.iter().map(|r| r.job).collect::<Vec<_>>());
        assert!(evictions.iter().all(|e| e.evicted_at == now));
    }

    /// A rejected batch ends the pass: the rest of the backlog is still due
    /// next tick, and hammering the log with batches that are likely to fail
    /// the same way buys nothing.
    #[tokio::test]
    async fn a_rejected_batch_stops_the_pass() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);
        let (sm, _) = due_jobs(now, &[1, 2, 3, 4, 5]);
        let view = view_of(sm);
        let due = due_for_eviction(&view, now, usize::MAX);

        let (consensus, _publisher) = FakeConsensus::new(ProposeOutcome::Rejected(
            coppice_state::RejectionReason::JobNotTerminal(JobId::new()),
        ));
        let consensus = Arc::new(consensus);
        propose_evictions(&consensus, HistorySink::None, &due, now, 2).await;

        assert_eq!(proposed_evictions(&consensus).len(), 1);
    }

    /// The backlog the budget left behind is picked up by the next sweep,
    /// oldest first again — nothing is stranded by the bound.
    #[test]
    fn the_backlog_drains_across_passes() {
        let retention = PolicyConfig::default().terminal_retention;
        let now = Timestamp::UNIX_EPOCH + retention.saturating_mul(100);
        let (mut sm, mut ids) = due_jobs(now, &[1, 2, 3, 4, 5]);
        ids.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
        let oldest_first: Vec<JobId> = ids.iter().map(|(_, id)| *id).collect();

        let first = due_for_eviction(&view_of(sm.clone()), now, 2);
        assert_eq!(
            first.iter().map(|r| r.job).collect::<Vec<_>>(),
            oldest_first[..2]
        );

        // Apply the first pass's evictions and sweep again.
        for record in &first {
            sm.jobs.remove(&record.job);
        }
        let second = due_for_eviction(&view_of(sm), now, 2);
        assert_eq!(
            second.iter().map(|r| r.job).collect::<Vec<_>>(),
            oldest_first[2..4]
        );
    }

    /// Node records follow the same rule: the longest-silent leave first, and
    /// no more than the budget allows.
    #[test]
    fn due_node_records_are_longest_silent_first_and_bounded() {
        let retention = PolicyConfig::default()
            .node_retention
            .to_std()
            .expect("the default window is positive");
        let base = Instant::now();
        let now = base + retention.saturating_mul(10) + StdDuration::from_secs(1);

        let liveness = NodeLiveness::new();
        let mut sm = StateMachine::default();
        let mut ids = Vec::new();
        // Scrambled insertion order; a larger age is quieter for longer.
        for age in [3u32, 1, 5, 2, 4] {
            let id = NodeId::new();
            sm.nodes.insert(id, node_record(id, 1, false));
            liveness.seed(1, [id], now - retention.saturating_mul(age));
            ids.push((age, id));
        }
        let view = view_of(sm);

        ids.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
        let quietest_two: Vec<NodeId> = ids.iter().take(2).map(|(_, id)| *id).collect();
        assert_eq!(
            due_for_node_eviction(&view, &liveness, now, 2),
            quietest_two
        );
    }

    /// A node that resumes reporting while earlier batches replicate keeps
    /// its record: each batch is re-checked against the liveness map just
    /// before it is proposed, not against the verdict the scan reached.
    #[tokio::test]
    async fn a_node_heard_from_mid_pass_is_dropped_from_its_batch() {
        let due: Vec<NodeId> = (0..4).map(|_| NodeId::new()).collect();
        let resumed = due[2];

        let (consensus, _publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        let consensus = Arc::new(consensus);
        // The fake records a proposal before answering it, so "one batch has
        // been proposed" is exactly "the first batch is behind us".
        let seen = Arc::clone(&consensus);
        let still_silent = move |node: NodeId| !(node == resumed && !seen.proposed().is_empty());
        propose_node_evictions(&consensus, &due, 2, still_silent).await;

        let batches: Vec<Vec<NodeId>> = consensus
            .proposed()
            .into_iter()
            .filter_map(|c| match c {
                Command::EvictNodes(evict) => Some(evict.nodes),
                _ => None,
            })
            .collect();
        assert_eq!(batches, vec![vec![due[0], due[1]], vec![due[3]]]);
    }

    /// The loop end to end under `[history] mode = "none"`: with no store to
    /// write to, the replicated retention TTL is the whole gate, and what
    /// comes out of a tick is exactly one `EvictTerminalJobs` naming exactly
    /// the jobs past it (ADR 0012's lossy mode). A backlog under the cap is
    /// one proposal; only a bigger one is split (issue #155).
    #[tokio::test(start_paused = true)]
    async fn the_none_mode_evicts_on_the_ttl_alone() {
        let (consensus, mut publisher) = FakeConsensus::new(ProposeOutcome::Accepted);
        let consensus = Arc::new(consensus);
        let views = consensus.views();

        let evictable = JobId::new();
        let live = JobId::new();
        let mut sm = StateMachine::default();
        // Terminal at the epoch: `Timestamp::now()` is the wall clock, which
        // tokio's paused timer does not move, so this is a full retention
        // interval in the past on any machine that runs the test.
        sm.jobs.insert(
            evictable,
            terminal_job(
                evictable,
                Timestamp::UNIX_EPOCH,
                Some(Timestamp::UNIX_EPOCH),
            ),
        );
        sm.jobs
            .insert(live, job_record(live, "img", Resources::ZERO, None));
        assert!(
            Timestamp::now() - Timestamp::UNIX_EPOCH >= PolicyConfig::default().terminal_retention
        );
        publisher.publish_now(&sm, 1);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let status = consensus.status();
        let join = tokio::spawn(run(
            Arc::clone(&consensus),
            views,
            HistorySink::None,
            HOUSEKEEPING_INTERVAL,
            NodeLiveness::new(),
            status,
            shutdown_rx,
        ));

        // Let the loop take leadership and arm its ticker before moving the
        // clock: an `interval` created *after* the advance would measure from
        // the new time and never fire.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // The immediate first tick was consumed as the ticker was armed (the
        // loop skips it deliberately), so exactly one interval buys exactly
        // one sweep — a longer jump would burst several missed ticks at once.
        tokio::time::advance(HOUSEKEEPING_INTERVAL).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        shutdown_tx.send(true).expect("the loop is still running");
        join.await.expect("the loop joins on shutdown");

        let evictions: Vec<EvictTerminalJobs> = consensus
            .proposed()
            .into_iter()
            .filter_map(|c| match c {
                Command::EvictTerminalJobs(evict) => Some(evict),
                _ => None,
            })
            .collect();
        assert_eq!(
            evictions.len(),
            1,
            "one sweep, and a backlog under the cap is one proposal"
        );
        assert_eq!(evictions[0].jobs, vec![evictable]);
    }
}
