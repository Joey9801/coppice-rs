//! # coppice-agent
//!
//! The node agent: an eventually-consistent executor of coordinator intent
//! (ADR 0009, `docs/protocols/agent-coordinator.md`).
//!
//! The agent never trusts its own memory over its journal plus the container
//! runtime: commands are fenced by `(leader_term, node_epoch)` before being
//! acted on, intents are journaled durably before containers start, and on
//! restart the recovered journal is reconciled against the runtime to build
//! the full `ObservedSet` reported before any new work is accepted.

pub mod config;
pub mod executor;
pub mod journal;
pub mod metrics_server;
pub mod node_service;
pub mod observed;
pub mod pressure;
pub mod session;
pub mod telemetry;

use anyhow::{Context, Result};
use coppice_consensus::fs::RealFs;
use coppice_proto::pb::core::v1 as pbcore;

/// Register descriptions for every metric the agent process can emit, recursing
/// into each module that exposes metrics (docker-executor.md §8.1). The
/// optional `/metrics` server (issue #46, [`metrics_server`]) calls this once
/// after [`run_daemon`] installs the Prometheus recorder, without knowing any
/// module's internals. [`run_daemon`] calls it at startup regardless — even
/// with no `metrics_addr` configured (no server), the descriptions are cheap
/// and harmless.
pub fn describe_metrics() {
    executor::docker::describe_metrics();
    telemetry::describe_metrics();
    node_service::describe_metrics();
}

/// Run any point-in-time sampling behind agent metrics, recursing the same
/// modules as [`describe_metrics`]. The `/metrics` server calls this
/// immediately before rendering each scrape.
pub fn gather_metrics() {
    executor::docker::gather_metrics();
    telemetry::gather_metrics();
    node_service::gather_metrics();
}

/// How often the detached upkeep task drains the recorder's histogram buckets.
/// Matches the exporter's own default upkeep timeout in its `install` path (and
/// the coordinator's `install_metrics_recorder`), so a scrape never sees buckets
/// older than this regardless of scrape cadence.
const METRICS_UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Install the process-wide Prometheus recorder and return its scrape handle
/// (issue #46).
///
/// [`run_daemon`] calls this once, before any counter is emitted. On success it
/// describes every agent metric ([`describe_metrics`]) and spawns a detached
/// task that runs `run_upkeep` on a fixed [`METRICS_UPKEEP_INTERVAL`], so the
/// histogram buckets drain on a timer whether or not anything scrapes — the
/// recorder records from startup even when no `metrics_addr` is configured.
///
/// `set_global_recorder` is a once-per-process operation: a lost race means a
/// recorder was already installed, which is a startup bug that must fail rather
/// than warn-and-continue. **Must be called from within a Tokio runtime** — it
/// `tokio::spawn`s the upkeep task ([`run_daemon`] is `async`). `coppice dev`
/// does not call this: it installs one shared recorder via the coordinator's
/// helper and describes the agent tree explicitly.
fn install_metrics_recorder() -> Result<metrics_exporter_prometheus::PrometheusHandle> {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("a metrics recorder was already installed in this process")?;
    describe_metrics();
    let upkeep_handle = handle.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(METRICS_UPKEEP_INTERVAL);
        loop {
            // `interval` ticks immediately on its first `tick`; upkeep on a
            // fresh recorder is a no-op, so that first tick is harmless.
            ticker.tick().await;
            upkeep_handle.run_upkeep();
        }
    });
    Ok(handle)
}

/// Run the agent daemon from its config file: recover the journal, build the
/// session over the production [`executor::DockerExecutor`], and enter the
/// dial/serve loop until the process is stopped.
///
/// This is the whole daemon minus argument parsing and tracing setup, which
/// belong to the `coppice` binary (`coppice agent --config <path>`).
pub async fn run_daemon(config_path: &std::path::Path) -> Result<()> {
    let config = config::load(config_path)?;
    config.log_effective();

    // Install the process-wide Prometheus recorder before any counter is
    // emitted, so the descriptions (§8.1) and every counter after land in the
    // recorder the `/metrics` server renders (issue #46). This also describes
    // every metric and spawns the periodic upkeep task. Keep its handle for the
    // server below. The server itself is bound later, only when `metrics_addr`
    // is configured, but the recorder is installed unconditionally so metrics
    // accrue (and upkeep drains histograms) from the start regardless.
    let metrics_handle = install_metrics_recorder()?;

    // The journal lives directly under the data directory; anchor RealFs there.
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = journal::Journal::open(fs).context("recovering the agent journal")?;

    let labels: Vec<pbcore::Label> = config
        .labels
        .iter()
        .map(|(key, value)| pbcore::Label {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();

    // Build the Docker executor: connect the daemon, learn its data-root (fail
    // fast if unreachable — an agent that cannot reach its daemon is useless),
    // spawn the shared disk-pressure monitor over data_dir + the data-root, then
    // construct the executor and its events task (docker-executor.md §9, §11).
    let docker_host = executor::docker::api::resolve_host(config.executor.docker_host.as_deref())
        .context("resolving the Docker daemon endpoint")?;
    let docker =
        executor::docker::api::connect(&docker_host).context("connecting to the Docker daemon")?;
    let data_root = executor::docker::api::data_root(&docker, &docker_host)
        .await
        .context("querying the Docker daemon for its data-root")?;
    let mut pressure_paths = vec![config.data_dir.clone()];
    if let Some(root) = data_root {
        pressure_paths.push(root);
    }
    // The image cache reads the same filesystems the pressure monitor watches
    // for its High-pressure eviction target (§7, §9); clone the vec before it
    // moves into `pressure::spawn`.
    let cache_options = executor::docker::cache::CacheOptions {
        config: config.image_cache.clone(),
        state_path: Some(config.data_dir.join("image-cache.json")),
        pressure_paths: pressure_paths.clone(),
        high_pct: config.pressure.high_pct,
    };
    let pressure_rx = pressure::spawn(pressure_paths.clone(), config.pressure);

    // Build the telemetry subsystem (§8): open the configured filesystem sinks,
    // spawn their retention janitors, and get the hub the collectors feed. The
    // returned `Telemetry` is kept alive for the daemon's lifetime — dropping it
    // would drop the janitor handles and stop them.
    let telemetry = telemetry::build(
        &config.telemetry,
        &config.data_dir,
        pressure_paths,
        config.pressure.high_pct,
        pressure_rx.clone(),
    )
    .await
    .context("building the telemetry subsystem")?;

    // `Some` whenever any sink is configured; per-kind suppression (§8.3) handles
    // partial configs (metrics-only or logs-only). With **zero** sinks there is
    // nothing to consume either stream, so pass `None` and collect nothing rather
    // than stream logs and poll stats only for the hub to discard every batch.
    let telemetry_wiring =
        (!config.telemetry.sinks.is_empty()).then(|| executor::docker::TelemetryWiring {
            hub: telemetry.hub.clone(),
            stores: telemetry.stores.clone(),
            log_store: telemetry.log_store.clone(),
            metrics_interval: config.telemetry.metrics_interval,
            drain_force_after: config.telemetry.drain_force_after,
        });
    let docker_executor = executor::DockerExecutor::new(
        docker,
        &config.executor,
        &docker_host,
        config.capacity.cpu_millis,
        config.reservation.cpu_millis,
        config.node(),
        pressure_rx,
        cache_options,
        telemetry_wiring,
    )
    .await
    .context("initializing the Docker executor")?;

    // Bind the NodeService listener eagerly (ADR 0034) before entering the
    // session loop, so a port conflict fails the daemon here rather than after
    // it has registered. Absent `[listen]` config = no listener, no
    // advertisement — a legitimate posture (the agent's logs are unreachable
    // off-node). The handler reads the first LOG-consuming store; with telemetry
    // disabled that is `None` and every fetch answers UnknownAttempt.
    if let Some(listen) = &config.listen {
        let listener = node_service::prepare_listener(listen.addr, &config.tls)
            .context("binding the NodeService listener")?;
        tracing::info!(
            service_addr = ?config.service_addr(),
            "NodeService listener bound; coordinators can dial for job logs (ADR 0034)"
        );
        node_service::serve(
            listener,
            telemetry.log_store.clone(),
            telemetry.metric_store.clone(),
        );
    }

    // Bind and serve the Prometheus `/metrics` server when configured (issue
    // #46), eagerly like the NodeService listener so a port conflict fails the
    // daemon here rather than after registration. Absent `metrics_addr` = no
    // server (the recorder still runs; nothing scrapes it) — a legitimate
    // posture for an agent whose metrics are pulled another way or not at all.
    if let Some(metrics_addr) = config.metrics_addr {
        let listener = metrics_server::prepare_listener(metrics_addr)
            .await
            .context("binding the metrics server listener")?;
        tracing::info!(%metrics_addr, "Prometheus metrics server bound at /metrics (issue #46)");
        metrics_server::serve(listener, metrics_handle, gather_metrics);
    }

    let session = session::Session::new(
        config.node(),
        config.advertised_resources(),
        labels,
        journal,
        state,
        docker_executor,
    )
    .with_service_addr(config.service_addr());

    tracing::info!("coppice agent started; entering the session loop");
    session::run(session, &config).await
}
