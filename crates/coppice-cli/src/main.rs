//! The `coppice` binary: every component of the system behind one entry
//! point, selected by subcommand.
//!
//! - `coppice coordinator --config …` — run a coordinator replica (plus its
//!   hidden `admin` membership verbs);
//! - `coppice agent --config …` — run a node agent;
//! - `coppice dev …` — a self-contained single-node dev cluster;
//! - `coppice node …` — enrollment-token and identity administration
//!   (ADR 0037 §5);
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

    /// Enrollment-token and identity administration over a coordinator's
    /// admin channel (ADR 0037 §5).
    Node(coppice_coordinator::cli::NodeArgs),

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
        Command::Node(args) => {
            init_tracing();
            coppice_coordinator::node::run_cli(args).await
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

    /// The nested run path takes `--config` and nothing else: ADR 0037 §1
    /// removed `--bootstrap`/`--join`, so one command line covers scale-out
    /// join, instance replacement, and plain restart alike.
    #[test]
    fn coordinator_subcommand_parses_the_run_path() {
        let cli = Cli::parse_from(["coppice", "coordinator", "--config", "/etc/c.toml"]);
        match cli.command {
            Command::Coordinator(c) => {
                let run = c.run_args();
                assert_eq!(run.config, PathBuf::from("/etc/c.toml"));
            }
            other => panic!("expected coordinator, got {other:?}"),
        }
    }

    /// And a stale unit file carrying the removed flags fails loudly at parse
    /// rather than silently starting a daemon whose intent it cannot honor.
    #[test]
    fn the_removed_intent_flags_are_rejected() {
        for flag in ["--bootstrap", "--join"] {
            assert!(
                Cli::try_parse_from(["coppice", "coordinator", "--config", "/etc/c.toml", flag])
                    .is_err(),
                "{flag} must no longer parse (ADR 0037 §1)"
            );
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
    fn node_enroll_token_mint_parses() {
        use coppice_coordinator::cli::{EnrollTokenVerb, NodeVerb, RoleArg};
        let cli = Cli::parse_from([
            "coppice",
            "node",
            "--target",
            "coord-1:7071",
            "--ca",
            "ca.crt",
            "--cert",
            "op.crt",
            "--key",
            "op.key",
            "enroll-token",
            "mint",
            "--role",
            "agent",
            "--ttl",
            "15m",
            "--label",
            "fleet",
        ]);
        match cli.command {
            Command::Node(args) => {
                assert_eq!(args.target, "coord-1:7071");
                assert_eq!(args.ca, PathBuf::from("ca.crt"));
                let NodeVerb::EnrollToken {
                    verb: EnrollTokenVerb::Mint { role, ttl, label },
                } = args.verb
                else {
                    panic!("expected enroll-token mint");
                };
                assert_eq!(role, RoleArg::Agent);
                assert_eq!(ttl, Some(std::time::Duration::from_secs(900)));
                assert_eq!(label, "fleet");
            }
            other => panic!("expected node, got {other:?}"),
        }
    }

    #[test]
    fn node_enroll_token_list_and_revoke_parse() {
        use coppice_coordinator::cli::{EnrollTokenVerb, NodeVerb};
        let base = [
            "coppice",
            "node",
            "--target",
            "h:1",
            "--ca",
            "ca",
            "--cert",
            "c",
            "--key",
            "k",
            "enroll-token",
        ];
        let cli = Cli::parse_from(base.iter().copied().chain(["list"]));
        assert!(matches!(
            cli.command,
            Command::Node(args) if matches!(
                args.verb,
                NodeVerb::EnrollToken { verb: EnrollTokenVerb::List }
            )
        ));

        let id = "token-00000000-0000-0000-0000-000000000001";
        let cli = Cli::parse_from(base.iter().copied().chain(["revoke", "--id", id]));
        match cli.command {
            Command::Node(args) => {
                let NodeVerb::EnrollToken {
                    verb: EnrollTokenVerb::Revoke { id: parsed },
                } = args.verb
                else {
                    panic!("expected enroll-token revoke");
                };
                assert_eq!(parsed.to_string(), id);
            }
            other => panic!("expected node, got {other:?}"),
        }
    }

    #[test]
    fn node_revoke_identity_requires_exactly_one_target() {
        let base = [
            "coppice",
            "node",
            "--target",
            "h:1",
            "--ca",
            "ca",
            "--cert",
            "c",
            "--key",
            "k",
            "revoke-identity",
        ];
        // Neither, and both, are refused; a malformed id is refused at parse.
        assert!(Cli::try_parse_from(base).is_err());
        assert!(Cli::try_parse_from(base.iter().copied().chain([
            "--machine",
            "machine-00000000-0000-0000-0000-000000000001",
            "--node",
            "node-00000000-0000-0000-0000-000000000002",
        ]))
        .is_err());
        assert!(
            Cli::try_parse_from(base.iter().copied().chain(["--node", "not-a-node-id"])).is_err()
        );

        let cli = Cli::parse_from(
            base.iter()
                .copied()
                .chain(["--machine", "machine-00000000-0000-0000-0000-000000000001"]),
        );
        match cli.command {
            Command::Node(args) => {
                let coppice_coordinator::cli::NodeVerb::RevokeIdentity { machine, node } =
                    args.verb
                else {
                    panic!("expected revoke-identity");
                };
                assert!(node.is_none());
                assert_eq!(
                    machine.expect("machine parsed").to_string(),
                    "machine-00000000-0000-0000-0000-000000000001"
                );
            }
            other => panic!("expected node, got {other:?}"),
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
    fn job_usage_parses_with_attempt_and_order() {
        let cli = Cli::parse_from([
            "coppice",
            "job",
            "usage",
            "job-00000000-0000-0000-0000-000000000001",
            "--attempt",
            "attempt-00000000-0000-0000-0000-000000000002",
            "--order",
            "desc",
        ]);
        match cli.command {
            Command::Job(job::JobArgs {
                command:
                    job::JobCommand::Usage {
                        job,
                        attempt,
                        order,
                    },
                ..
            }) => {
                assert_eq!(job.to_string(), "job-00000000-0000-0000-0000-000000000001");
                assert_eq!(
                    attempt.map(|a| a.to_string()).as_deref(),
                    Some("attempt-00000000-0000-0000-0000-000000000002")
                );
                assert_eq!(order, Some(job::OrderArg::Desc));
            }
            other => panic!("expected job usage, got {other:?}"),
        }
    }

    #[test]
    fn job_usage_defaults_attempt_and_order_to_none() {
        let cli = Cli::parse_from([
            "coppice",
            "job",
            "usage",
            "job-00000000-0000-0000-0000-000000000001",
        ]);
        match cli.command {
            Command::Job(job::JobArgs {
                command: job::JobCommand::Usage { attempt, order, .. },
                ..
            }) => {
                assert!(attempt.is_none());
                assert!(order.is_none());
            }
            other => panic!("expected job usage, got {other:?}"),
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
