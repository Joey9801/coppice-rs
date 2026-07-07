//! Ingestion / normalizer (leader-only).
//!
//! The boundary of `docs/architecture/command-catalog.md#the-agent-report-ingestion-boundary`:
//! fencing check, dedupe by `(AttemptId, attempt_state)`, timestamping, and
//! the ObservedSet diff, then `propose` (`RecordAttempt*`, `ReconcileNode`,
//! `RegisterNode`, `DeclareNodeLost`). Benign apply rejections
//! (`StaleAttemptState` and the like) are ignored rather than treated as
//! failures — see `docs/architecture/coordinator-runtime.md`, "Ingestion /
//! normalizer".

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use coppice_consensus::{Consensus, ConsensusStatus, StateView, StateViews};
use coppice_state::Command;

use crate::leadership;
use crate::tasks::agent_gateway::InboundReport;

/// Run the ingestion loop until shutdown.
pub async fn run<C: Consensus>(
    consensus: Arc<C>,
    views: StateViews,
    mut inbound: mpsc::Receiver<InboundReport>,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let Some(term) = leadership::wait_for_leadership(&mut status, &mut shutdown).await else {
            return;
        };
        tracing::info!(term, "ingestion: gained leadership, draining inbound reports");

        let lost_leadership = drain(&consensus, &views, &mut inbound, &mut status, term, &mut shutdown).await;
        if !lost_leadership {
            // The inbound sender side is gone (agent gateway shut down)
            // rather than leadership having moved; nothing left to ingest.
            return;
        }
    }
}

/// Drain inbound reports until leadership is lost or shutdown. Returns
/// `true` when it stopped because leadership was lost (the caller should
/// re-gate), `false` when the inbound channel closed for good.
async fn drain<C: Consensus>(
    consensus: &Arc<C>,
    views: &StateViews,
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
                let view = views.latest();
                for command in normalize(&view, report) {
                    match consensus.propose(command).await {
                        Ok(applied) => {
                            if let Err(reason) = applied.outcome {
                                tracing::debug!(?reason, "ingestion: benign rejection");
                            }
                        }
                        Err(e) if e.is_retryable() => {
                            tracing::info!(
                                error = %e,
                                "ingestion: retryable propose error, re-gating on leadership"
                            );
                            return true;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "ingestion: fatal propose error");
                            return false;
                        }
                    }
                }
            }
        }
    }
}

/// Normalize one agent report into zero or more commands: fencing check
/// against `view`'s node epoch, dedupe, timestamping, and the ObservedSet
/// diff (`docs/architecture/command-catalog.md#the-agent-report-ingestion-boundary`).
/// Deferred: this is real domain logic, not wiring.
fn normalize(_view: &StateView, _report: InboundReport) -> Vec<Command> {
    todo!("agent-report normalization: fencing, dedupe, timestamping, ObservedSet diff")
}
