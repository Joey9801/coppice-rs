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
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::interval;

use coppice_consensus::{Applied, Consensus, ConsensusStatus, StateViews};
use coppice_core::id::JobId;
use coppice_core::job::JobState;
use coppice_state::command::EvictTerminalJobs;
use coppice_state::Command;

use crate::leadership;
use crate::limits::HOUSEKEEPING_INTERVAL;

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

        let mut ticker = interval(HOUSEKEEPING_INTERVAL);
        // The first tick fires immediately; skip it so gaining leadership
        // doesn't itself trigger an instant sweep.
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = leadership::until_leadership_lost(&mut status, term, &mut shutdown) => break,
                _ = ticker.tick() => {
                    run_pass(&consensus, &views, &history).await;
                }
            }
        }
    }
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
