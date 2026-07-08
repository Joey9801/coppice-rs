//! The coordinator's task topology.
//!
//! Channel construction, task spawning, and shutdown — exactly the topology
//! of `docs/architecture/coordinator-runtime.md` ("Task inventory", "Task
//! and channel topology", "Leader transitions", "Shutdown order").

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use coppice_consensus::{Consensus, EventTapReceiver, StateViews};
use coppice_scheduler::HeuristicScheduler;

use crate::limits::AGENT_INBOUND_CAPACITY;
use crate::tasks::api_server::{self, CoordinatorControlPlane};
use crate::tasks::housekeeping::StubHistoryStore;
use crate::tasks::{
    agent_gateway, dispatch, event_fanout, housekeeping, ingestion, scheduler_driver,
};

/// Wire up and run every coordinator task.
///
/// Returns once shutdown has fully drained.
pub async fn run<C>(
    consensus: C,
    views: StateViews,
    event_tap: EventTapReceiver,
) -> anyhow::Result<()>
where
    C: Consensus,
{
    let consensus = Arc::new(consensus);
    let status = consensus.status();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // ---- Channels (capacities from `crate::limits`) ----
    let (inbound_tx, inbound_rx) = mpsc::channel(AGENT_INBOUND_CAPACITY);

    // ---- Every-replica tasks ----
    let (fanout, fanout_join) = event_fanout::spawn(event_tap, shutdown_rx.clone());
    tracing::info!("runtime: event fanout up");

    let (router, router_join) =
        agent_gateway::spawn(inbound_tx, status.clone(), shutdown_rx.clone());
    tracing::info!("runtime: agent gateway up");

    let control_plane = Arc::new(CoordinatorControlPlane::new(
        Arc::clone(&consensus),
        views.clone(),
    ));
    let api_join = tokio::spawn(api_server::run_placeholder(
        control_plane,
        shutdown_rx.clone(),
    ));
    tracing::info!("runtime: api server up");

    // ---- Leader-only tasks (every replica runs the loop; each self-gates
    // on the status watch per `crate::leadership`) ----
    let ingestion_join = tokio::spawn(ingestion::run(
        Arc::clone(&consensus),
        views.clone(),
        inbound_rx,
        status.clone(),
        shutdown_rx.clone(),
    ));

    let dispatch_join = tokio::spawn(dispatch::run(
        Arc::clone(&consensus),
        views.clone(),
        fanout.clone(),
        router.clone(),
        status.clone(),
        shutdown_rx.clone(),
    ));

    let scheduler_join = tokio::spawn(scheduler_driver::run(
        Arc::clone(&consensus),
        views.clone(),
        Arc::new(HeuristicScheduler::default()),
        status.clone(),
        shutdown_rx.clone(),
    ));

    let housekeeping_join = tokio::spawn(housekeeping::run(
        Arc::clone(&consensus),
        views.clone(),
        Arc::new(StubHistoryStore),
        status.clone(),
        shutdown_rx.clone(),
    ));
    tracing::info!("runtime: ingestion, dispatch, scheduler driver, housekeeping spawned");

    // ---- Shutdown trigger ----
    // Both interactive (ctrl-c / SIGINT) and orchestrated (SIGTERM, e.g. a
    // `kill` or a container stop) shutdowns flip the same watch; whichever
    // fires first wins the race and the other arm is dropped.
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "runtime: failed to install SIGTERM handler");
                    return;
                }
            };
            let reason = tokio::select! {
                res = tokio::signal::ctrl_c() => res.map(|()| "ctrl-c").ok(),
                _ = sigterm.recv() => Some("SIGTERM"),
            };
            if let Some(reason) = reason {
                tracing::info!(
                    signal = reason,
                    "runtime: shutdown signal received, shutting down"
                );
                let _ = shutdown_tx.send(true);
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("runtime: ctrl-c received, shutting down");
                let _ = shutdown_tx.send(true);
            }
        }
    });

    // Shutdown order (docs/architecture/coordinator-runtime.md, "Shutdown
    // order"): API/agent listeners stop accepting first, then the
    // leader-only loops drain and exit at their chosen await points, then
    // fanout closes its subscribers. Steps 5-6 of that order — openraft
    // shutdown draining the apply task's request queue, and the storage
    // layer flushing and closing — have no code here yet: they belong to
    // `bootstrap` once the segment storage layer and openraft node exist.
    let _ = api_join.await;
    let _ = router_join.await;
    tracing::info!("runtime: api server and agent gateway down");

    let _ = ingestion_join.await;
    let _ = dispatch_join.await;
    let _ = scheduler_join.await;
    let _ = housekeeping_join.await;
    tracing::info!("runtime: leader-only loops down");

    let _ = fanout_join.await;
    tracing::info!("runtime: event fanout down; shutdown complete");

    Ok(())
}
