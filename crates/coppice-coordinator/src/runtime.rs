//! The coordinator's task topology.
//!
//! Channel construction, task spawning, and shutdown — exactly the topology
//! of `docs/architecture/coordinator-runtime.md` ("Task inventory", "Task
//! and channel topology", "Leader transitions", "Shutdown order").

use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, watch};
use tonic::transport::Server;

use coppice_consensus::{Consensus, EventTapReceiver, StateViews};
use coppice_scheduler::HeuristicScheduler;

use crate::bootstrap::AgentListener;
use crate::limits::AGENT_INBOUND_CAPACITY;
use crate::liveness::NodeLiveness;
use crate::tasks::agent_gateway::{AgentSessionService, Gateway};
use crate::tasks::api_server::{self, CoordinatorControlPlane};
use crate::tasks::housekeeping::StubHistoryStore;
use crate::tasks::{
    agent_gateway, dispatch, event_fanout, housekeeping, ingestion, scheduler_driver,
};

/// Wire up and run every coordinator task.
///
/// Returns once shutdown has fully drained.
///
/// `external_shutdown` selects how the runtime is stopped. `None` is the
/// daemon path: the runtime owns its own shutdown watch and flips it from the
/// signal handler installed below (ctrl-c / SIGTERM). `Some(rx)` is the
/// integration-test path: the caller owns the trigger and flips it directly, so
/// no signal handler is installed and the test never has to raise a real
/// signal. Either way the same watch drives every task's drain, so the
/// documented shutdown join order is identical.
pub async fn run<C>(
    consensus: C,
    views: StateViews,
    event_tap: EventTapReceiver,
    agent_listener: AgentListener,
    external_shutdown: Option<watch::Receiver<bool>>,
) -> anyhow::Result<()>
where
    C: Consensus,
{
    let consensus = Arc::new(consensus);
    let status = consensus.status();
    // The daemon path owns the watch and drives it from signals; a test passes
    // its own receiver and keeps the sender, so `signal_tx` is `None` and no
    // signal handler is installed.
    let (shutdown_rx, signal_tx) = match external_shutdown {
        Some(rx) => (rx, None),
        None => {
            let (tx, rx) = watch::channel(false);
            (rx, Some(tx))
        }
    };

    // Leader-only health-monitor state, shared (not a channel) between
    // ingestion (marks) and housekeeping (seeds/reads) — see `crate::liveness`.
    let liveness = NodeLiveness::new();

    // ---- Channels (capacities from `crate::limits`) ----
    let (inbound_tx, inbound_rx) = mpsc::channel(AGENT_INBOUND_CAPACITY);

    // ---- Every-replica tasks ----
    // Seed the fanout's replay floor with the index the replica recovered at,
    // so a reconnect with a pre-restart cursor gaps instead of silently
    // replaying across the boundary (KOI-3).
    let recovery_index = views.latest().applied_index();
    let (fanout, fanout_join) =
        event_fanout::spawn(event_tap, recovery_index, shutdown_rx.clone());
    tracing::info!("runtime: event fanout up");

    let Gateway {
        router,
        authority,
        join: router_join,
    } = agent_gateway::spawn(
        inbound_tx,
        views.clone(),
        status.clone(),
        shutdown_rx.clone(),
    );
    tracing::info!("runtime: agent gateway up");

    // Agent session mTLS server. The listener is bound early in `bootstrap`;
    // here it starts accepting and stops on shutdown (listeners drain first,
    // `docs/architecture/coordinator-runtime.md`, "Shutdown order").
    let AgentListener { incoming, tls } = agent_listener;
    let agent_service = coppice_net::session::Server::new(AgentSessionService::new(authority));
    let agent_router = Server::builder()
        .tls_config(tls)
        .context("configuring the agent gateway server TLS")?
        .add_service(agent_service);
    let mut agent_shutdown = shutdown_rx.clone();
    let agent_server_join = tokio::spawn(async move {
        agent_router
            .serve_with_incoming_shutdown(incoming, async move {
                while !*agent_shutdown.borrow() {
                    if agent_shutdown.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
    });
    tracing::info!("runtime: agent session server up");

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
        router.clone(),
        liveness.clone(),
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
        liveness.clone(),
        status.clone(),
        shutdown_rx.clone(),
    ));
    tracing::info!("runtime: ingestion, dispatch, scheduler driver, housekeeping spawned");

    // ---- Shutdown trigger ----
    // The daemon path installs the signal handler; an integration test owns the
    // trigger itself (`signal_tx` is `None`) and never raises a real signal.
    // Both interactive (ctrl-c / SIGINT) and orchestrated (SIGTERM, e.g. a
    // `kill` or a container stop) shutdowns flip the same watch; whichever
    // fires first wins the race and the other arm is dropped.
    if let Some(shutdown_tx) = signal_tx {
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
    }

    // Shutdown order (docs/architecture/coordinator-runtime.md, "Shutdown
    // order"): API/agent listeners stop accepting first, then the
    // leader-only loops drain and exit at their chosen await points, then
    // fanout closes its subscribers. Steps 5-6 of that order — openraft
    // shutdown draining the apply task's request queue, and the storage
    // layer flushing and closing — have no code here yet: they belong to
    // `bootstrap` once the segment storage layer and openraft node exist.
    let _ = api_join.await;
    let _ = agent_server_join.await;
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
