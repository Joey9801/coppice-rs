//! Housekeeping (leader-only, 60 s tick).
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
use std::time::Instant;

use tokio::sync::watch;
use tokio::time::interval;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateView, StateViews};
use coppice_core::allocation::AllocationState;
use coppice_core::id::{JobId, NodeId};
use coppice_core::job::JobState;
use coppice_core::time::Timestamp;
use coppice_state::command::{DeclareNodeLost, EvictTerminalJobs};
use coppice_state::Command;

use crate::leadership;
use crate::limits::{AGENT_LIVENESS_DEADLINE, HOUSEKEEPING_INTERVAL};
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
pub async fn run<C>(
    consensus: Arc<C>,
    views: StateViews,
    history: HistorySink,
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
        liveness.seed(views.latest().state().nodes.keys().copied(), Instant::now());

        let mut ticker = interval(HOUSEKEEPING_INTERVAL);
        // The first tick fires immediately; skip it so gaining leadership
        // doesn't itself trigger an instant sweep.
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                _ = ticker.tick() => {
                    declare_lost_nodes(&consensus, &views, &liveness).await;
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
/// that are still schedulable or hold a non-`Released` allocation.
///
/// The schedulable-or-live-allocations guard is what stops us re-declaring an
/// already-lost silent node every tick: `DeclareNodeLost` leaves the node
/// unschedulable with all its allocations `Released`, so a second declaration
/// is neither needed nor emitted. A node not yet tracked in the liveness map
/// (no report and no seed) is left alone — real nodes are always seeded on
/// leadership gain and marked on every report.
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
        let has_live_allocation = view.state().allocations.values().any(|a| {
            a.allocation.node == *node_id && a.allocation.state != AllocationState::Released
        });
        if node_record.node.schedulable || has_live_allocation {
            out.push(*node_id);
        }
    }
    out
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

    // The configured mode is the gate. `none` has nothing to write to, so the
    // TTL that made these jobs due is the whole of it — say so plainly rather
    // than logging a write that did not happen (ADR 0012, issue #43).
    match history {
        HistorySink::None => tracing::info!(
            count = due.len(),
            "housekeeping: history = \"none\", evicting terminal jobs past the TTL; \
             their history is discarded (ADR 0012 lossy mode)"
        ),
    }

    let command = Command::EvictTerminalJobs(EvictTerminalJobs {
        jobs: due.iter().map(|r| r.job).collect(),
        evicted_at: now,
    });
    match consensus.propose(command).await {
        Ok(Applied { outcome: Ok(_), .. }) => {}
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
        liveness.seed([schedulable_stale, drained_live, drained_lost], overdue);
        liveness.seed([fresh], now);

        let stale: BTreeSet<NodeId> = stale_nodes(&view, &liveness, now).into_iter().collect();
        assert!(stale.contains(&schedulable_stale));
        assert!(stale.contains(&drained_live));
        // Unschedulable with no live allocation: already lost, not re-declared.
        assert!(!stale.contains(&drained_lost));
        // Within its liveness grace window.
        assert!(!stale.contains(&fresh));
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

    /// The loop end to end under `history = "none"`: with no store to write
    /// to, the replicated retention TTL is the whole gate, and what comes out
    /// of a tick is exactly one `EvictTerminalJobs` naming exactly the jobs
    /// past it (ADR 0012's lossy mode).
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
