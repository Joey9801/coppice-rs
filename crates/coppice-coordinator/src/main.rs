//! Coordinator daemon binary.
//!
//! A thin CLI shell: parse arguments (ADR 0020's deliberately tiny surface)
//! and hand off to the library ([`coppice_coordinator::run`]). All wiring —
//! config loading, the boot sequence, the runtime, and the membership admin
//! surface — lives in the library crate so integration tests exercise the same
//! paths this binary does.

use anyhow::Result;
use clap::Parser;

use coppice_coordinator::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    coppice_coordinator::run(cli).await
}
