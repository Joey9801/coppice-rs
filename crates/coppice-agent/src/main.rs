//! Node agent daemon.
//!
//! The agent runs on every compute node and is an eventually-consistent
//! executor of coordinator intent. It registers with the coordinator,
//! advertises resources and labels, starts and stops containers, enforces local
//! limits, reports observed lifecycle transitions and usage, and reconciles
//! running containers against desired state after restart.
//!
//! It must be robust against duplicated commands, stale leaders, network
//! partitions, and process restarts: commands are validated against an epoch /
//! fencing token before being acted on. See
//! `docs/protocols/agent-coordinator.md` and `docs/operations/failure-handling.md`.

use anyhow::{bail, Context, Result};
use coppice_agent::config;
use coppice_agent::executor::DockerExecutor;
use coppice_agent::journal::Journal;
use coppice_agent::session::{run, Session};
use coppice_consensus::fs::RealFs;
use coppice_proto::pb::core::v1 as pbcore;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = parse_args()?;
    let config = config::load(&config_path)?;
    config.log_effective();

    // The journal lives directly under the data directory; anchor RealFs there.
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = Journal::open(fs).context("recovering the agent journal")?;

    let labels: Vec<pbcore::Label> = config
        .labels
        .iter()
        .map(|(key, value)| pbcore::Label {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();

    let session = Session::new(
        config.node(),
        config.capacity_resources(),
        labels,
        journal,
        state,
        DockerExecutor::new(),
    );

    tracing::info!("coppice-agent started; entering the session loop");
    run(session, &config).await
}

/// Parse the tiny CLI surface by hand (clap is not a dependency): `--config
/// <path>` is the only flag.
fn parse_args() -> Result<std::path::PathBuf> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .context("--config requires a path argument")?
                        .into(),
                );
            }
            other => bail!("unexpected argument {other:?}; usage: coppice-agent --config <path>"),
        }
    }
    config_path.context("missing required --config <path>")
}
