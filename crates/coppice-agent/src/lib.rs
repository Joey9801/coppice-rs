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
pub mod observed;
pub mod pressure;
pub mod session;

use anyhow::{Context, Result};
use coppice_consensus::fs::RealFs;
use coppice_proto::pb::core::v1 as pbcore;

/// Register descriptions for every metric the agent process can emit, recursing
/// into each module that exposes metrics (docker-executor.md §8.1). The future
/// /metrics endpoint calls this once after installing its recorder, without
/// knowing any module's internals; [`run_daemon`] also calls it at startup so
/// descriptions exist even before the endpoint lands.
pub fn describe_metrics() {
    executor::docker::describe_metrics();
}

/// Run any point-in-time sampling behind agent metrics, recursing the same
/// modules as [`describe_metrics`]. The /metrics endpoint calls this
/// immediately before rendering each scrape.
pub fn gather_metrics() {
    executor::docker::gather_metrics();
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

    // Register metric descriptions once at startup (§8.1). The agent has no
    // /metrics endpoint yet, so this is the sole call site for now; when the
    // endpoint lands it calls the same fan-out after installing its recorder.
    describe_metrics();

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
    let docker = executor::docker::api::connect(&config.executor.docker_host)
        .context("connecting to the Docker daemon")?;
    let data_root = executor::docker::api::data_root(&docker, &config.executor.docker_host)
        .await
        .context("querying the Docker daemon for its data-root")?;
    let mut pressure_paths = vec![config.data_dir.clone()];
    if let Some(root) = data_root {
        pressure_paths.push(root);
    }
    let pressure_rx = pressure::spawn(pressure_paths, config.pressure);
    let docker_executor =
        executor::DockerExecutor::new(docker, &config.executor, config.node(), pressure_rx);

    let session = session::Session::new(
        config.node(),
        config.capacity_resources(),
        labels,
        journal,
        state,
        docker_executor,
    );

    tracing::info!("coppice agent started; entering the session loop");
    session::run(session, &config).await
}
