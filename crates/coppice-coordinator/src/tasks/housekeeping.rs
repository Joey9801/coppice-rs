//! Housekeeping (leader-only, 60 s tick).
//!
//! Scans the view for terminal jobs past retention and writes them to the
//! SQL job-history store first — an external network call, therefore
//! outside apply, with retries — and only after that write is durable
//! proposes `EvictTerminalJobs` (ADR 0012 ordering). The same task triggers
//! snapshots via `Consensus::trigger_snapshot` once applied-entries-since-
//! snapshot crosses a threshold (ADR 0002 / ADR 0017). See
//! `docs/architecture/coordinator-runtime.md`, "Housekeeping".

use std::future::Future;
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

/// The job-history sink (ADR 0012).
///
/// A sink, not a source: loss degrades history, never correctness.
pub trait HistoryStore: Send + Sync + 'static {
    /// Resolve only once the write is DURABLE.
    ///
    /// The evict proposal is sequenced after this.
    fn write_terminal_jobs(
        &self,
        jobs: Vec<TerminalJobRecord>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// A terminal job as handed to the history store.
///
/// `state`/`submitted_at`/`terminal_at` aren't read beyond
/// construction until a real history store consumes them
/// (`StubHistoryStore` only logs a count).
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

/// Placeholder history store until the SQL sink lands.
///
/// Logs and reports success.
pub struct StubHistoryStore;

impl HistoryStore for StubHistoryStore {
    async fn write_terminal_jobs(&self, jobs: Vec<TerminalJobRecord>) -> anyhow::Result<()> {
        tracing::info!(
            count = jobs.len(),
            "housekeeping: (stub) wrote terminal jobs to history"
        );
        Ok(())
    }
}

/// Run the housekeeping loop until shutdown.
pub async fn run<C, H>(
    consensus: Arc<C>,
    views: StateViews,
    history: Arc<H>,
    liveness: NodeLiveness,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) where
    C: Consensus,
    H: HistoryStore,
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
                    run_pass(&consensus, &views, &history).await;
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

async fn run_pass<C: Consensus, H: HistoryStore>(
    consensus: &Arc<C>,
    views: &StateViews,
    history: &Arc<H>,
) {
    let view = views.latest();
    // Proposer-side wall clock: safe here because housekeeping runs outside
    // apply (`docs/architecture/coordinator-runtime.md`, "Housekeeping").
    let now = Timestamp::now();

    let due = due_for_eviction(&view, now);

    if due.is_empty() {
        maybe_trigger_snapshot(consensus).await;
        return;
    }

    if let Err(e) = history.write_terminal_jobs(due.clone()).await {
        tracing::warn!(error = %e, "housekeeping: history write failed, will retry next tick");
        return;
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

    maybe_trigger_snapshot(consensus).await;
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

async fn maybe_trigger_snapshot<C: Consensus>(consensus: &Arc<C>) {
    if snapshot_due() {
        if let Err(e) = consensus.trigger_snapshot().await {
            tracing::warn!(error = %e, "housekeeping: trigger_snapshot failed");
        }
    }
}

/// Whether applied-entries-since-snapshot has crossed the ADR 0017 threshold.
///
/// Not yet wired to real metrics, so this never fires.
fn snapshot_due() -> bool {
    // TODO(ADR 0017): trigger once applied-entries-since-snapshot crosses
    // the configured threshold; sealed segments become deletable only once a
    // snapshot covers them.
    false
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

    use crate::test_support::{allocation_record, job_record, node_record, view_of};

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
}
