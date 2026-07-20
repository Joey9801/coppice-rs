//! The `coppice` binary: every component of the system behind one entry
//! point, selected by subcommand.
//!
//! - `coppice coordinator --config …` — run a coordinator replica (plus its
//!   hidden `admin` membership verbs);
//! - `coppice agent --config …` — run a node agent;
//! - `coppice dev …` — a self-contained single-node dev cluster;
//! - `coppice job …` — client commands against a cluster's API.
//!
//! Shipping one binary keeps deployment to a single artifact: the same build
//! runs as any component, so images and packaging never skew across roles.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod dev;
mod job;

#[derive(Debug, Parser)]
#[command(name = "coppice", version, about = "Coppice batch scheduler")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a coordinator replica (see `docs/operations/cluster-lifecycle.md`).
    Coordinator(coppice_coordinator::cli::Cli),

    /// Run a node agent.
    Agent(AgentArgs),

    /// Run a self-contained single-node dev cluster: one coordinator plus an
    /// in-process agent, throwaway per-run TLS (effectively no
    /// authentication), and a temp data directory unless --data-dir is set.
    /// For local development and integration tests only.
    Dev(dev::DevArgs),

    /// Job operations against a cluster's API.
    Job(job::JobArgs),
}

#[derive(Debug, clap::Args)]
struct AgentArgs {
    /// Path to the agent configuration file (ADR 0020).
    #[arg(long)]
    config: PathBuf,
}

/// Plain env-filter tracing for the roles that don't configure their own
/// (the coordinator installs a config-driven subscriber inside its own run
/// path, so this must not fire for it).
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new(
                        "warn,coppice=info,coppice_agent=info,coppice_consensus=info,coppice_coordinator=info",
                    )
                }),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Coordinator(args) => coppice_coordinator::run(args).await,
        Command::Agent(args) => {
            init_tracing();
            coppice_agent::run_daemon(&args.config).await
        }
        Command::Dev(args) => {
            init_tracing();
            dev::run(args).await
        }
        Command::Job(args) => {
            init_tracing();
            job::run(args).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_subcommand_parses_run_flags() {
        let cli = Cli::parse_from([
            "coppice",
            "coordinator",
            "--config",
            "/etc/c.toml",
            "--join",
        ]);
        match cli.command {
            Command::Coordinator(c) => {
                let run = c.run_args();
                assert_eq!(run.config, PathBuf::from("/etc/c.toml"));
                assert!(run.join);
                assert!(!run.bootstrap);
            }
            other => panic!("expected coordinator, got {other:?}"),
        }
    }

    #[test]
    fn coordinator_admin_verbs_still_parse_when_nested() {
        let cli = Cli::parse_from([
            "coppice",
            "coordinator",
            "admin",
            "--config",
            "c.toml",
            "--target",
            "coord-1:7071",
            "status",
        ]);
        match cli.command {
            Command::Coordinator(c) => assert!(c.command.is_some(), "admin subcommand expected"),
            other => panic!("expected coordinator, got {other:?}"),
        }
    }

    #[test]
    fn agent_subcommand_requires_config() {
        assert!(Cli::try_parse_from(["coppice", "agent"]).is_err());
        let cli = Cli::parse_from(["coppice", "agent", "--config", "/etc/a.toml"]);
        match cli.command {
            Command::Agent(a) => assert_eq!(a.config, PathBuf::from("/etc/a.toml")),
            other => panic!("expected agent, got {other:?}"),
        }
    }

    #[test]
    fn job_submit_parses() {
        let cli = Cli::parse_from(["coppice", "job", "submit", "job.toml"]);
        match cli.command {
            Command::Job(job::JobArgs {
                command: job::JobCommand::Submit { spec, job },
                ..
            }) => {
                assert_eq!(spec, PathBuf::from("job.toml"));
                assert!(job.is_none());
            }
            other => panic!("expected job submit, got {other:?}"),
        }
    }

    #[test]
    fn job_api_flag_is_global_before_or_after_the_verb() {
        // `--api` is a global arg: it parses on either side of the subcommand.
        let id = "job-00000000-0000-0000-0000-000000000001";
        for argv in [
            ["coppice", "job", "--api", "http://h:1", "status", id],
            ["coppice", "job", "status", id, "--api", "http://h:1"],
        ] {
            let cli = Cli::parse_from(argv);
            assert!(
                matches!(
                    cli.command,
                    Command::Job(job::JobArgs {
                        command: job::JobCommand::Status { .. },
                        ..
                    })
                ),
                "expected job status for {argv:?}"
            );
        }
    }
}
