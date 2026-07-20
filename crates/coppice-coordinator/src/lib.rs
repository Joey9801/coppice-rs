//! Coordinator daemon library.
//!
//! A coordinator replica forms part of the control plane. It participates in
//! Raft consensus, applies committed commands to the deterministic state
//! machine, serves (or forwards) API requests, receives agent heartbeats,
//! drives the asynchronous scheduler, and publishes state-change events. One
//! replica is the leader at any time. The concurrency architecture this crate
//! wires together — the task inventory, channel table, and leader-transition
//! rules — is specified in `docs/architecture/coordinator-runtime.md`.
//!
//! The single `coppice` binary (the `coppice-cli` crate) mounts this crate's
//! CLI as `coppice coordinator …` and dispatches into [`run`]. Everything —
//! config loading, the boot sequence ([`bootstrap`]), the membership admin
//! surface ([`admin`]), and the task runtime (the private `runtime` module) —
//! lives here so integration tests can drive the same code paths the binary
//! does.

pub mod admin;
pub mod bootstrap;
pub mod cli;
pub mod config;
mod leadership;
mod limits;
mod liveness;
mod runtime;
mod tasks;

#[cfg(test)]
mod test_support;

use anyhow::Result;

// The `ControlPlane` impl, exported so integration tests exercise the same
// submit/abort path the (future) HTTP listener will host.
pub use tasks::api_server::CoordinatorControlPlane;

// The replica-local log-fetch client (ADR 0034). Exported so the end-to-end
// best-effort job-log test can attach a real client to a `CoordinatorControlPlane`
// and drive the full read path; the type already surfaces publicly through
// `bootstrap::BootedCoordinator::node_log_client`.
pub use tasks::node_client::NodeLogClient;

/// Register descriptions for every metric a coordinator process can emit,
/// recursing into each crate and module that exposes metrics. The future
/// /metrics endpoint (ADR 0020's `observability.metrics_addr`) calls this
/// once after installing its recorder, without knowing any module's internals.
pub fn describe_metrics() {
    coppice_consensus::describe_metrics();
    tasks::event_fanout::describe_metrics();
    tasks::node_client::describe_metrics();
}

/// Run any point-in-time sampling behind coordinator metrics, recursing the
/// same modules as [`describe_metrics`]. The /metrics endpoint calls this
/// immediately before rendering each scrape.
pub fn gather_metrics() {
    coppice_consensus::gather_metrics();
    tasks::event_fanout::gather_metrics();
    tasks::node_client::gather_metrics();
}

/// Parse-and-dispatch entry point the binary calls.
///
/// The default (no subcommand) invocation boots and runs a coordinator replica
/// through [`bootstrap::run`]; the hidden `admin` subcommand drives the
/// membership admin surface through [`admin::run_cli`].
pub async fn run(cli: cli::Cli) -> Result<()> {
    match cli.command {
        Some(cli::Command::Admin(admin)) => admin::run_cli(admin).await,
        None => bootstrap::run(cli.run_args()).await,
    }
}
