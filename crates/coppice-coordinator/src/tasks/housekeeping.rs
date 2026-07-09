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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::interval;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateView, StateViews};
use coppice_core::allocation::AllocationState;
use coppice_core::id::{JobId, NodeId};
use coppice_core::job::JobState;
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
/// Derived from what `JobRecord` exposes: it does not (yet) carry a
/// terminal-transition timestamp, so `submitted_at_us` is the only
/// timestamp available here. The retention scan below uses it as an age
/// proxy for the same reason — the honest limitation of not touching
/// `coppice-state` to add one. `state`/`submitted_at_us` aren't read beyond
/// construction until a real history store consumes them (`StubHistoryStore`
/// only logs a count).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TerminalJobRecord {
    pub job: JobId,
    pub state: JobState,
    pub submitted_at_us: i64,
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
        tracing::info!(term, "housekeeping: gained leadership");

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
    let declared_at_us = now_us();
    for node in stale {
        let command = Command::DeclareNodeLost(DeclareNodeLost {
            node,
            declared_at_us,
        });
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
    let retention_us = view.state().policy.terminal_retention_us;
    // Proposer-side wall clock: safe here because housekeeping runs outside
    // apply (`docs/architecture/coordinator-runtime.md`, "Housekeeping").
    let now_us = now_us();

    let due: Vec<TerminalJobRecord> = view
        .state()
        .jobs
        .iter()
        .filter(|(_, record)| record.state.is_terminal())
        .filter(|(_, record)| now_us.saturating_sub(record.submitted_at_us) >= retention_us)
        .map(|(id, record)| TerminalJobRecord {
            job: *id,
            state: record.state,
            submitted_at_us: record.submitted_at_us,
        })
        .collect();

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
        evicted_at_us: now_us,
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

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::time::Duration;

    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use coppice_core::resource::Resources;
    use coppice_state::StateMachine;

    use crate::test_support::{allocation_record, node_record, view_of};

    #[test]
    fn stale_nodes_picks_schedulable_and_live_but_not_already_lost_or_fresh() {
        // Anchor arithmetic on `base` and add (never subtract) to avoid
        // underflowing the monotonic clock on a freshly started process.
        let base = Instant::now();
        let now = base + AGENT_LIVENESS_DEADLINE + Duration::from_secs(2);
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
}
