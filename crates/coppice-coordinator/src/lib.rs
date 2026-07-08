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
//! The binary ([`main`](../main.rs)) is a thin CLI shell: it parses arguments
//! and dispatches into [`run`]. Everything else — config loading, the boot
//! sequence ([`bootstrap`]), the membership admin surface ([`admin`]), and the
//! task runtime (the private `runtime` module) — lives here so integration
//! tests can drive the same code paths the binary does.

pub mod admin;
pub mod bootstrap;
pub mod cli;
pub mod config;
mod leadership;
mod limits;
mod runtime;
mod tasks;

#[cfg(test)]
mod test_support;

use anyhow::Result;

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
