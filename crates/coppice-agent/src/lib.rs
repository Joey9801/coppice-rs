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
pub mod session;

use anyhow::{Context, Result};
use coppice_consensus::fs::RealFs;
use coppice_proto::pb::core::v1 as pbcore;

/// Run the agent daemon from its config file: recover the journal, build the
/// session over the production [`executor::DockerExecutor`], and enter the
/// dial/serve loop until the process is stopped.
///
/// This is the whole daemon minus argument parsing and tracing setup, which
/// belong to the `coppice` binary (`coppice agent --config <path>`).
pub async fn run_daemon(config_path: &std::path::Path) -> Result<()> {
    let config = config::load(config_path)?;
    config.log_effective();

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

    let session = session::Session::new(
        config.node(),
        config.capacity_resources(),
        labels,
        journal,
        state,
        executor::DockerExecutor::new(),
    );

    tracing::info!("coppice agent started; entering the session loop");
    session::run(session, &config).await
}
