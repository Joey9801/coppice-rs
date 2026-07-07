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

use anyhow::Result;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("coppice-agent starting (skeleton)");

    // TODO: load config, register with the coordinator, recover local durable
    // state, reconcile running containers, and enter the heartbeat/execute loop.
    Ok(())
}
