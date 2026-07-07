//! Coordinator daemon.
//!
//! A coordinator replica forms part of the control plane. It participates in
//! Raft consensus, applies committed commands to the deterministic state
//! machine, serves (or forwards) API requests, receives agent heartbeats,
//! drives the asynchronous scheduler, and publishes state-change events. One
//! replica is the leader at any time. The concurrency architecture this
//! binary wires together — the task inventory, channel table, and leader-
//! transition rules — is specified in
//! `docs/architecture/coordinator-runtime.md`.

use anyhow::{bail, Result};

use coppice_consensus::{EventTap, EventTapReceiver, OpenraftConsensus, StateViews};

mod leadership;
mod limits;
mod runtime;
mod tasks;

#[cfg(test)]
mod test_support;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("coppice-coordinator starting");

    let (consensus, views, event_tap) = bootstrap().await?;
    runtime::run(consensus, views, event_tap).await
}

/// Construct the openraft node, segment storage, and the apply task, then
/// return the [`Consensus`](coppice_consensus::Consensus) seam plus its view
/// and event-tap handles for `runtime::run` to wire up.
///
/// Not implemented yet: the segment storage layer and openraft node
/// construction are not built yet; see
/// `docs/architecture/coordinator-runtime.md`.
async fn bootstrap() -> Result<(OpenraftConsensus, StateViews, EventTapReceiver)> {
    // Sizing the tap now — even though the rest of bootstrap isn't built —
    // keeps `limits::EVENT_TAP_CAPACITY` live here rather than dead, and
    // marks where the real construction plugs in once the apply task exists.
    let (_tap, _event_tap) = EventTap::channel(limits::EVENT_TAP_CAPACITY);

    bail!(
        "segment storage layer and openraft node construction are not built yet; \
         see docs/architecture/coordinator-runtime.md"
    );
}
