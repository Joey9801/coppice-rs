//! Coordinator daemon.
//!
//! A coordinator replica forms part of the control plane. It participates in
//! Raft consensus, applies committed commands to the deterministic state
//! machine, serves (or forwards) API requests, receives agent heartbeats,
//! drives the asynchronous scheduler, and publishes state-change events. One
//! replica is the leader at any time. See `docs/architecture/components.md`
//! (Coordinator Replicas) for the intended internal structure.

use anyhow::Result;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("coppice-coordinator starting (skeleton)");

    // TODO: load config, join/bootstrap the Raft cluster, start the API server,
    // the agent RPC endpoint, the scheduler loop, and the event fanout.
    Ok(())
}
