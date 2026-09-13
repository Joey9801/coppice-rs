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
use crate::limits::AGENT_LIVENESS_DEADLINE;
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
/// One batch per tick: apply skips ids that are already gone, so a
/// re-issued proposal is idempotent across a leader change. This pass
/// applies exactly the conditions apply re-checks.
async fn evict_silent_nodes<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
    liveness: &NodeLiveness,
) {
    let view = views.latest();
    let due = due_for_node_eviction(&view, liveness, Instant::now());
    if due.is_empty() {
        return;
    }
    // Proposer-side wall clock: housekeeping runs outside apply.
    let command = Command::EvictNodes(EvictNodes {
        nodes: due.clone(),
        // The retention GC is machine-proposed; only the admin API's explicit
        // `node remove` carries an actor.
        actor: None,
        evicted_at: Timestamp::now(),
    });
    match consensus.propose(command).await {
        Ok(Applied { outcome: Ok(_), .. }) => {
            tracing::info!(
                count = due.len(),
                "housekeeping: evicted node records silent past the retention window"
            );
        }
        Ok(Applied {
            outcome: Err(reason),
            ..
        }) => {
            tracing::warn!(?reason, "housekeeping: EvictNodes rejected");
        }
        Err(e) if e.is_retryable() => {
            tracing::info!(error = %e, "housekeeping: retryable EvictNodes error");
        }
        Err(e) => {
            tracing::error!(error = %e, "housekeeping: fatal EvictNodes error");
        }
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
fn due_for_node_eviction(view: &StateView, liveness: &NodeLiveness, now: Instant) -> Vec<NodeId> {
    let Some(retention) = view.state().policy.node_retention.to_std() else {
        // A negative window is not expressible as a monotonic span; treat it
        // as "never due" rather than evicting everything.
        tracing::warn!("housekeeping: node_retention is negative, skipping the node retention GC");
        return Vec::new();
    };
    view.state()
        .nodes
        .iter()
        .filter(|(_, record)| !record.accepts_placements())
        .map(|(node_id, _)| *node_id)
        .filter(|node_id| match liveness.last_seen(*node_id) {
            Some(seen) => now.saturating_duration_since(seen) >= retention,
            None => false,
        })
        .filter(|node_id| !has_live_allocation(view, *node_id))
        .collect()
}

async fn run_pass<C: Consensus>(consensus: &Arc<C>, views: &StateViews, history: HistorySink) {
    let view = views.latest();
    // Proposer-side wall clock: safe here because housekeeping runs outside
    // apply (`docs/architecture/coordinator-runtime.md`, "Housekeeping").
    let now = Timestamp::now();

    let due = due_for_eviction(&view, now);

    if due.is_empty() {
        return;
    }

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
            "housekeeping: terminal jobs past the TTL, proposing eviction (history = \"none\")"
        ),
    }

    let command = Command::EvictTerminalJobs(EvictTerminalJobs {
        jobs: due.iter().map(|r| r.job).collect(),
        evicted_at: now,
    });
    match consensus.propose(command).await {
        // Applied: this is the first point at which anything may be said about
        // what happened to the jobs' history — and under `none` what happened
        // is that it went away. The count comes from the apply outcome's
        // `JobEvicted` events, not from `due`: apply skips ids that are
        // already gone (that skip is what makes duplicate proposals across a
        // leadership change idempotent), so `due.len()` can overstate what
        // this proposal actually removed — down to nothing at all, in which
        // case no history was discarded here and nothing says it was.
        Ok(Applied {
            outcome: Ok(applied),
            ..
        }) => {
            let evicted = applied
                .events
                .iter()
                .filter(|e| matches!(e, coppice_state::Event::JobEvicted { .. }))
                .count();
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
        Ok(Applied {
            outcome: Err(reason),
            ..
        }) => {
            tracing::debug!(?reason, "housekeeping: EvictTerminalJobs rejected");
        }
        Err(e) if e.is_retryable() => {
            tracing::info!(error = %e, "housekeeping: retryable propose error");
        }
        Err(e) => {
            tracing::error!(error = %e, "housekeeping: fatal propose error");
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
fn due_for_eviction(view: &StateView, now: Timestamp) -> Vec<TerminalJobRecord> {
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
    due
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

        let due: BTreeSet<NodeId> = due_for_node_eviction(&view, &liveness, now)
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
            base + retention - StdDuration::from_secs(1)
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

        assert_eq!(due_for_node_eviction(&view, &liveness, now), vec![node]);
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
        let due = due_for_eviction(&view, now);
        assert_eq!(
            due.iter().map(|r| r.job).collect::<Vec<_>>(),
            vec![done_long_ago]
        );
        assert_eq!(due[0].terminal_at, now - retention);

        // The moment the post-terminal interval elapses, the long-queued job
        // becomes due too.
        let later = now + retention;
        let due_later: BTreeSet<JobId> = due_for_eviction(&view, later)
            .into_iter()
            .map(|r| r.job)
            .collect();
        assert!(due_later.contains(&done_long_ago));
        assert!(due_later.contains(&long_queued_just_done));
        assert!(!due_later.contains(&ancient_but_live));
        assert!(!due_later.contains(&terminal_unstamped));
    }

    /// The loop end to end under `[history] mode = "none"`: with no store to
    /// write to, the replicated retention TTL is the whole gate, and what
    /// comes out of a tick is exactly one `EvictTerminalJobs` naming exactly
    /// the jobs past it (ADR 0012's lossy mode).
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
        assert_eq!(evictions.len(), 1, "one sweep, one proposal");
        assert_eq!(evictions[0].jobs, vec![evictable]);
    }
}
