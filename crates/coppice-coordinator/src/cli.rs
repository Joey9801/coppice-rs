//! The coordinator command-line surface (ADR 0020, amended by ADR 0037 §1).
//!
//! Deliberately tiny: the default invocation takes just `--config`. Startup
//! intent is no longer declared — the daemon *derives* it from the data
//! directory (manifest present → resume; absent → new instance that parks and
//! converges, ADR 0037 §1), so the `--bootstrap` / `--join` flags are gone. A
//! single hidden `admin` subcommand carries the membership operations an
//! operator runs against a live cluster (ADR 0016) — hidden because it is
//! plumbing for runbooks/automation, retained but no longer part of any routine
//! procedure. The `coppice` binary mounts this surface as the `coordinator`
//! subcommand.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

/// Coordinator daemon.
///
/// With no subcommand, boots and runs a replica from `--config` — the same one
/// command in every situation (scale-out join, instance replacement, plain
/// restart), with intent derived from the disk (ADR 0037 §1). The hidden
/// `admin` subcommand drives the membership admin RPCs against a running node.
#[derive(Debug, Parser)]
#[command(
    name = "coordinator",
    version,
    // `--config` is only required on the default run path; a subcommand negates
    // that requirement, and the two surfaces never mix. The run args are inlined
    // (not flattened) because `subcommand_negates_reqs` only negates
    // requirements declared directly on this command.
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// The hidden admin subcommand, if any; `None` is the default run path.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Path to the node configuration file (ADR 0020). Required on the default
    /// run path; negated when a subcommand is present.
    #[arg(long, required = true)]
    pub config: Option<PathBuf>,
}

impl Cli {
    /// The default-run arguments, valid only when no subcommand is present.
    ///
    /// `--config` is guaranteed present here: clap requires it on the run path
    /// (`subcommand_negates_reqs` only drops it for a subcommand, which this
    /// call is never reached for).
    pub fn run_args(self) -> RunArgs {
        RunArgs {
            config: self.config.expect("--config is required on the run path"),
        }
    }
}

/// The resolved arguments for the default (run-a-replica) invocation.
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// Path to the node configuration file (ADR 0020).
    pub config: PathBuf,
}

/// The top-level subcommands. Only `admin` exists, and it is hidden.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Membership administration against a running cluster (ADR 0016).
    #[command(hide = true)]
    Admin(AdminArgs),
}

/// Common arguments plus the verb for an `admin` invocation.
#[derive(Debug, Args)]
pub struct AdminArgs {
    /// Path to the node configuration file — read for TLS material and the
    /// default `--target` (the first `[discovery.static]` seed address).
    #[arg(long)]
    pub config: PathBuf,

    /// The `host:port` of the coordinator to contact. Defaults to the first
    /// `[discovery.static] addrs` entry when the discovery backend is `static`;
    /// otherwise `--target` is required (ADR 0037 §2).
    #[arg(long)]
    pub target: Option<String>,

    /// The membership operation to perform.
    #[command(subcommand)]
    pub verb: AdminVerb,
}

/// The membership admin verbs (ADR 0016), each a thin wrapper over one
/// `RaftAdminService` RPC.
#[derive(Debug, Subcommand)]
pub enum AdminVerb {
    /// Add a fresh coordinator as a non-voting learner (ADR 0016 step 2).
    AddLearner {
        /// The learner's allocate-once Raft node id.
        #[arg(long)]
        node_id: u64,
        /// The `host:port` peers dial to reach it.
        #[arg(long)]
        addr: String,
    },

    /// Promote a caught-up learner to voter, optionally dropping a departed
    /// voter in the same joint change (ADR 0016 step 3).
    ///
    /// A learner still behind the promotion threshold yields a retryable
    /// "behind" response; this verb polls until it catches up or `--wait`
    /// elapses, which is what makes `coordinator replace` operable end to end.
    Promote {
        /// The learner to promote.
        #[arg(long)]
        node_id: u64,
        /// A departed voter to remove in the same joint change.
        #[arg(long)]
        remove: Option<u64>,
        /// How long to keep retrying while the learner is still catching up.
        #[arg(long, default_value = "60s", value_parser = parse_duration)]
        wait: Duration,
    },

    /// Remove a node from membership entirely.
    Remove {
        /// The node to remove.
        #[arg(long)]
        node_id: u64,
    },

    /// Operator break-glass: repoint an existing node's membership address
    /// (ADR 0037 §4).
    ///
    /// Operator-credential only. The leader commits the repoint only after
    /// dialing the *new* address and verifying by probe that its TLS subject
    /// matches the target's machine-identity binding and that `ProbeCluster`
    /// reports the target's stamped node id — a claimed node id without the
    /// matching CA-attested subject is refused. Unknown id is refused (no silent
    /// creation); a no-op when the address already matches. For the immutable
    /// model an instance whose address changed is normally just a new instance;
    /// this verb is the rare pet-deployment escape hatch.
    SetAddress {
        /// The node whose address to repoint.
        #[arg(long)]
        node_id: u64,
        /// The new `host:port` the node advertises.
        #[arg(long)]
        addr: String,
    },

    /// Print this coordinator's view of cluster state.
    Status {
        /// Emit the cluster-wide view as stable JSON for scripting (ADR 0037
        /// §7) instead of the default human-readable table.
        #[arg(long)]
        json: bool,
    },
}

/// Parse a humane duration string (`"60s"`, `"2m"`) for `--wait`.
///
/// Reuses `humantime`'s parser (the same grammar the config file's durations
/// use), so an unlabelled bare integer is rejected rather than silently
/// meaning some unit.
fn parse_duration(raw: &str) -> Result<Duration, String> {
    humantime_serde::re::humantime::parse_duration(raw).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an `admin` invocation from a bare argv (program name first).
    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("args should parse")
    }

    #[test]
    fn default_run_requires_config() {
        let cli = parse(&["coppice-coordinator", "--config", "/etc/c.toml"]);
        assert!(cli.command.is_none());
        assert_eq!(cli.config, Some(PathBuf::from("/etc/c.toml")));
        // The run-path extraction yields the same config.
        let run = cli.run_args();
        assert_eq!(run.config, PathBuf::from("/etc/c.toml"));
    }

    #[test]
    fn missing_config_on_run_path_is_an_error() {
        assert!(Cli::try_parse_from(["coppice-coordinator"]).is_err());
    }

    #[test]
    fn the_removed_intent_flags_are_rejected() {
        // ADR 0037 §1: `--bootstrap`/`--join` are gone; passing one is an error.
        assert!(Cli::try_parse_from([
            "coppice-coordinator",
            "--config",
            "/etc/c.toml",
            "--bootstrap",
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["coppice-coordinator", "--config", "/etc/c.toml", "--join",])
                .is_err()
        );
    }

    #[test]
    fn admin_add_learner_parses() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "add-learner",
            "--node-id",
            "7",
            "--addr",
            "coord-7:7071",
        ]);
        match cli.command {
            Some(Command::Admin(a)) => {
                assert_eq!(a.config, PathBuf::from("/etc/c.toml"));
                assert!(a.target.is_none());
                match a.verb {
                    AdminVerb::AddLearner { node_id, addr } => {
                        assert_eq!(node_id, 7);
                        assert_eq!(addr, "coord-7:7071");
                    }
                    other => panic!("wrong verb: {other:?}"),
                }
            }
            other => panic!("expected admin subcommand, got {other:?}"),
        }
    }

    #[test]
    fn admin_promote_parses_remove_and_wait() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "--target",
            "coord-1:7071",
            "promote",
            "--node-id",
            "4",
            "--remove",
            "2",
            "--wait",
            "90s",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert_eq!(a.target.as_deref(), Some("coord-1:7071"));
        match a.verb {
            AdminVerb::Promote {
                node_id,
                remove,
                wait,
            } => {
                assert_eq!(node_id, 4);
                assert_eq!(remove, Some(2));
                assert_eq!(wait, Duration::from_secs(90));
            }
            other => panic!("wrong verb: {other:?}"),
        }
    }

    #[test]
    fn admin_promote_defaults_wait_to_sixty_seconds() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "promote",
            "--node-id",
            "4",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        match a.verb {
            AdminVerb::Promote { remove, wait, .. } => {
                assert_eq!(remove, None);
                assert_eq!(wait, Duration::from_secs(60));
            }
            other => panic!("wrong verb: {other:?}"),
        }
    }

    #[test]
    fn admin_remove_parses() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "remove",
            "--node-id",
            "9",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        match a.verb {
            AdminVerb::Remove { node_id } => assert_eq!(node_id, 9),
            other => panic!("wrong verb: {other:?}"),
        }
    }

    #[test]
    fn admin_status_parses() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "status",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert!(matches!(a.verb, AdminVerb::Status { json: false }));
    }

    #[test]
    fn admin_status_json_flag_parses() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "status",
            "--json",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert!(matches!(a.verb, AdminVerb::Status { json: true }));
    }

    #[test]
    fn admin_rejects_a_bare_integer_wait() {
        assert!(Cli::try_parse_from([
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "promote",
            "--node-id",
            "4",
            "--wait",
            "60",
        ])
        .is_err());
    }
}
