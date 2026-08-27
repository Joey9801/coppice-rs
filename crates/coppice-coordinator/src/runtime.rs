//! The coordinator's task topology.
//!
//! Channel construction, task spawning, and shutdown — exactly the topology
//! of `docs/architecture/coordinator-runtime.md` ("Task inventory", "Task
//! and channel topology", "Leader transitions", "Shutdown order").

use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, watch};
use tonic::transport::Server;

use coppice_consensus::{Consensus, EventTapReceiver, NodeHandle, StateViews};
use coppice_core::id::ClusterId;
use coppice_scheduler::HeuristicScheduler;

use crate::bootstrap::{AgentListener, ClientListener};
use crate::limits::AGENT_INBOUND_CAPACITY;
use crate::liveness::NodeLiveness;
use crate::tasks::agent_gateway::{AgentSessionService, Gateway};
use crate::tasks::api_server::{self, CoordinatorControlPlane};
use crate::tasks::housekeeping::HistorySink;
use crate::tasks::node_client::NodeClient;
use crate::tasks::{
    agent_gateway, derived_stats, dispatch, event_fanout, housekeeping, ingestion, learner_gc,
    renewal, scheduler_driver,
};

/// How often the detached upkeep task drains the recorder's histogram buckets.
/// Matches the exporter's own default upkeep timeout in its `install` path, so
/// a scrape never sees buckets older than this regardless of scrape cadence.
const METRICS_UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Install the process-wide Prometheus recorder and return its scrape handle
/// (issue #46).
///
/// The embedder that owns the process lifecycle calls this once, before any
/// task emits a metric, and hands the returned handle to every `/metrics`
/// endpoint it serves. On success this describes every coordinator metric
/// ([`crate::describe_metrics`]) and spawns a detached task that runs
/// `run_upkeep` on a fixed [`METRICS_UPKEEP_INTERVAL`], so histogram buckets
/// drain on a timer rather than only when something scrapes — the recorder
/// records from the first apply whether or not any scraper is connected.
///
/// `set_global_recorder` is a once-per-process operation: installing a second
/// recorder in the same process is a startup bug (a real daemon and `coppice
/// dev` each install exactly one), so a lost race is a hard error, not a
/// warning. The integration tests never call this — they build detached
/// endpoints ([`MetricsEndpoint::detached_for_tests`](coppice_api::http::MetricsEndpoint::detached_for_tests))
/// and local recorders instead.
///
/// **Must be called from within a Tokio runtime**: it `tokio::spawn`s the
/// upkeep task. Both callers (`bootstrap::run` and `dev::run`) are `async`, so
/// this holds even though the function itself is synchronous.
///
/// The returned [`PrometheusHandle`](metrics_exporter_prometheus::PrometheusHandle)
/// is `Clone`, so one install can feed several scrape endpoints — as `coppice
/// dev` does, sharing this handle between its coordinator- and agent-side
/// `/metrics` routes.
pub fn install_metrics_recorder() -> anyhow::Result<metrics_exporter_prometheus::PrometheusHandle> {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("a metrics recorder was already installed in this process")?;
    crate::describe_metrics();
    spawn_upkeep(handle.clone());
    Ok(handle)
}

/// Spawn the detached task that drains the recorder's histogram buckets on a
/// fixed interval (issue #46), independent of scrapes. `tokio::time::interval`
/// ticks immediately on its first `tick`, which is harmless — an upkeep on a
/// fresh recorder is a no-op.
fn spawn_upkeep(handle: metrics_exporter_prometheus::PrometheusHandle) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(METRICS_UPKEEP_INTERVAL);
        loop {
            ticker.tick().await;
            handle.run_upkeep();
        }
    });
}

/// Wire up and run every coordinator task.
///
/// Returns once shutdown has fully drained.
///
/// `metrics` is the `/metrics` endpoint the API server hosts on the client
/// listener (issue #46); the caller builds it over a recorder it installed with
/// [`install_metrics_recorder`], so the runtime never touches the process-global
/// recorder slot itself (that lets `coppice dev` install one shared recorder for
/// its co-hosted coordinator and agent).
///
/// `external_shutdown` selects how the runtime is stopped. `None` is the
/// daemon path: the runtime owns its own shutdown watch and flips it from the
/// signal handler installed below (ctrl-c / SIGTERM). `Some(rx)` is the
/// integration-test path: the caller owns the trigger and flips it directly, so
/// no signal handler is installed and the test never has to raise a real
/// signal. Either way the same watch drives every task's drain, so the
/// documented shutdown join order is identical.
#[allow(clippy::too_many_arguments)] // wiring seam: each is a distinct runtime input
pub async fn run<C>(
    consensus: C,
    views: StateViews,
    event_tap: EventTapReceiver,
    node_handle: NodeHandle,
    agent_listener: AgentListener,
    client_listener: ClientListener,
    cluster_id: ClusterId,
    node_log_client: Arc<NodeClient>,
    // This daemon's data directory: the agent gateway's renewal RPC signs from
    // the CA key it holds (ADR 0037 §4).
    data_dir: std::path::PathBuf,
    metrics: coppice_api::http::MetricsEndpoint,
    readyz: coppice_api::http::ReadyzEndpoint,
    // The serving names this daemon's config declares (`formation::leaf_sans`);
    // renewal re-issues leaves against these rather than copying the old
    // leaf's SANs (ADR 0037 §4/§6). `None` falls back to copying.
    serving_sans: Option<Vec<String>>,
    // How often that task re-examines its conditions, and how hard a failed
    // renewal retries: the `[pacing]` renewal knobs (ADR 0037 §4).
    renewal_pacing: renewal::RenewalPacing,
    // Where terminal-job history goes (ADR 0012): the `[history]` mode the
    // daemon path resolved from config. No default — an embedder driving this
    // seam states it, because "lossy" is a deployment decision and not
    // something a missing argument gets to make (issue #43).
    history: HistorySink,
    // How often a leader sweeps terminal jobs past the retention TTL: the
    // `[pacing]` housekeeping knob (ADR 0012). Liveness only — it decides how
    // promptly a due job is noticed, never which jobs are due.
    housekeeping_interval: std::time::Duration,
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
    let (fanout, fanout_join) = event_fanout::spawn(event_tap, recovery_index, shutdown_rx.clone());
    tracing::debug!("runtime: event fanout up");

    // Derived queue stats (ADR 0032, tier 3): counts the event stream into
    // rolling buckets and publishes the window the overview's rates and
    // history are projected from.
    let (queue_window, derived_stats_join) =
        derived_stats::spawn(fanout.clone(), views.clone(), shutdown_rx.clone());
    tracing::debug!("runtime: derived stats up");

    let Gateway {
        router,
        authority,
        join: router_join,
    } = agent_gateway::spawn(
        inbound_tx,
        views.clone(),
        status.clone(),
        crate::enroll::Issuer::new(
            data_dir.clone(),
            views.clone(),
            Arc::clone(&consensus) as Arc<dyn crate::enroll::ReadBarrier>,
        ),
        shutdown_rx.clone(),
    );
    tracing::debug!("runtime: agent gateway up");

    // Agent session mTLS server. The listener is bound early in `bootstrap`;
    // here it starts accepting and stops on shutdown (listeners drain first,
    // `docs/architecture/coordinator-runtime.md`, "Shutdown order").
    let AgentListener { listener, tls } = agent_listener;
    // This replica's machine-plane identity, shared with two more consumers:
    // the `/enroll` proxy hop to the leader, and the renewal task that keeps
    // this very material from expiring (ADR 0037 §4).
    let machine_tls = Arc::clone(&tls);
    let listener = tokio::net::TcpListener::from_std(listener)
        .context("adopting the agent gateway listener into tokio")?;
    let incoming = coppice_tls::serve(listener, tls);
    let agent_service = coppice_net::session::Server::new(AgentSessionService::new(authority));
    let agent_router = Server::builder().add_service(agent_service);
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
    tracing::debug!("runtime: agent session server up");

    let control_plane = Arc::new(
        CoordinatorControlPlane::new(Arc::clone(&consensus), views.clone(), cluster_id)
            .with_derived(queue_window, fanout.clone())
            .with_node_handle(node_handle.clone())
            .with_log_client(node_log_client)
            // Writes that land here while this replica follows go to the
            // leader over the admin channel instead of coming back as a
            // redirect (ADR 0038) — the same hop, and the same machine
            // identity, the `/enroll` proxy below uses.
            .with_forwarder(crate::clientwrite::AdminForwarder::new(
                node_handle.clone(),
                Arc::clone(&machine_tls),
            )),
    );
    // `POST /api/v1/enroll` (ADR 0037 §4). Captured by the router directly,
    // not reached through the `ControlPlane`: issuing a certificate needs the
    // CA key on this disk, the leader's address, and this node's own identity
    // for the proxy hop — none of which belong in that trait.
    let enroll = crate::enroll::EnrollService::new(
        Arc::clone(&consensus),
        data_dir.clone(),
        node_handle.clone(),
        Arc::clone(&machine_tls),
    )
    .endpoint();
    let api_join = tokio::spawn(api_server::run(
        client_listener,
        control_plane,
        metrics,
        readyz,
        enroll,
        crate::clientedge::ClusterCa::from_views(views.clone()),
        shutdown_rx.clone(),
    ));
    tracing::debug!("runtime: API server up");

    // Keeps this replica's own machine leaf alive (ADR 0037 §4): renew at ~2/3
    // of its lifetime, signing locally while leader and over the admin channel
    // otherwise. Short leaf lifetimes are only free if this never stops.
    let renewal_join = tokio::spawn(renewal::run(
        machine_tls,
        data_dir,
        Arc::clone(&consensus),
        node_handle,
        serving_sans,
        renewal_pacing,
        shutdown_rx.clone(),
    ));

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

    // Bounds membership records under instance churn (ADR 0037 §7): retires
    // the machine binding of a learner the leader has failed to reach for
    // longer than `learner_expiry`, then releases its seat. Voters are never
    // touched — there is no background voter reaper.
    let learner_gc_join = tokio::spawn(learner_gc::run(
        Arc::clone(&consensus),
        views.clone(),
        status.clone(),
        shutdown_rx.clone(),
    ));

    let housekeeping_join = tokio::spawn(housekeeping::run(
        Arc::clone(&consensus),
        views.clone(),
        history,
        housekeeping_interval,
        liveness.clone(),
        status.clone(),
        shutdown_rx.clone(),
    ));
    tracing::info!(
        "coordinator runtime started (agent sessions, scheduling, dispatch, and housekeeping)"
    );
    // Listeners are serving: signal systemd `READY=1` (ADR 0037 §9). Unit
    // ordering keys off this; cluster/node readiness stays a later concern.
    // Silent no-op when `$NOTIFY_SOCKET` is unset (every non-systemd launch).
    crate::systemd::notify_ready();

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
                    // Tell systemd the exit is intentional (ADR 0037 §9).
                    crate::systemd::notify_stopping();
                    let _ = shutdown_tx.send(true);
                }
            }
            #[cfg(not(unix))]
            {
                if tokio::signal::ctrl_c().await.is_ok() {
                    tracing::info!("runtime: ctrl-c received, shutting down");
                    crate::systemd::notify_stopping();
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
    let _ = renewal_join.await;
    tracing::debug!("runtime: API control plane and agent gateway down");

    let _ = ingestion_join.await;
    let _ = dispatch_join.await;
    let _ = scheduler_join.await;
    let _ = housekeeping_join.await;
    let _ = learner_gc_join.await;
    tracing::debug!("runtime: leader-only loops down");

    // Derived stats subscribes to the fanout, so it drains before it.
    let _ = derived_stats_join.await;
    let _ = fanout_join.await;
    tracing::info!("coordinator runtime stopped");

    Ok(())
}
