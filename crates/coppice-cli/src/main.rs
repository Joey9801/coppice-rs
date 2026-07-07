//! `coppice` command-line client.
//!
//! A thin client over the public API used for operating the cluster: submitting
//! and aborting jobs, and querying job, node, and queue status. It talks to
//! the same API surface as the web UI (`coppice-api`).

use anyhow::Result;

fn main() -> Result<()> {
    // TODO: argument parsing and API client. Skeleton only.
    println!("coppice CLI (skeleton) — no commands implemented yet");
    Ok(())
}
