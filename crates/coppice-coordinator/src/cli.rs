//! The coordinator command-line surface (ADR 0020).
//!
//! Deliberately tiny: the default invocation takes `--config` and nothing
//! else. There are no startup-intent flags — ADR 0037 §1 is "one command,
//! derived intent": what a daemon does at boot is read from what is on its
//! disk (a stamped manifest resumes; a formation intent without its completion
//! marker fail-stops; an empty directory converges or parks), never from an
//! operator remembering which flag this machine needs. Everything else
//! resolves file-over-default inside [`crate::config`].
//!
//! A single hidden `admin` subcommand carries the membership operations an
//! operator runs against a live cluster (ADR 0016/0037 §7) — hidden because it
//! is plumbing for runbooks and automation, not part of the daemon's
//! day-to-day surface. The `coppice` binary mounts this surface as the
//! `coordinator` subcommand.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

/// Coordinator daemon.
///
/// With no subcommand, boots and runs a replica from `--config`. The hidden
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
    ///
    /// The only run-path argument there is. Startup intent is derived from the
    /// data directory, not declared here (ADR 0037 §1), so every coordinator
    /// in a fleet is launched by a byte-identical command line — which is what
    /// lets one launch template serve founders, joiners and restarts alike.
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
///
/// One field, and that is the design: ADR 0037 §1 removed `--bootstrap` and
/// `--join` outright rather than defaulting them, because a startup intent
/// that config can express is a startup intent an operator can get wrong on
/// the one machine that matters.
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// Path to the node configuration file (ADR 0020).
    pub config: PathBuf,
}

/// The top-level subcommands: the one-per-cluster-lifetime `init` ceremony,
/// and the hidden `admin` plumbing.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Form this cluster (ADR 0037 §3).
    ///
    /// Run exactly once per cluster lifetime, against a parked daemon on the
    /// local machine. Formation is not a network verb: this talks to the
    /// daemon's Unix socket in its data directory, and local access to that
    /// socket is the authority. Re-running it against a formed cluster is
    /// harmless and reports `already-initialized`, so automation may retry.
    Init(InitArgs),

    /// Membership administration against a running cluster (ADR 0016).
    #[command(hide = true)]
    Admin(AdminArgs),
}

/// Arguments for `coppice coordinator init` (ADR 0037 §3).
#[derive(Debug, Args)]
pub struct InitArgs {
    /// Path to the node configuration file — read for the data directory,
    /// which is where the daemon's admin socket lives. No `--target` and no
    /// TLS flags: the socket is local, and being able to open it *is* the
    /// authorization.
    #[arg(long)]
    pub config: PathBuf,

    /// Bootstrap policy TOML to apply as part of formation (ADR 0020):
    /// priority multipliers and quota entities. Idempotent puts, so a re-run
    /// against an already-seeded cluster changes nothing.
    #[arg(long)]
    pub policy: Option<PathBuf>,

    /// A PEM certificate-signing request to sign into the first
    /// operator-profile certificate (ADR 0022's break-glass credential).
    /// Without one the cluster mints the keypair and prints both halves —
    /// collect them from this terminal, they are not stored.
    #[arg(long)]
    pub operator_csr: Option<PathBuf>,

    /// The common name the operator certificate carries.
    #[arg(long)]
    pub operator_cn: Option<String>,

    /// Write the issued material to files under this directory
    /// (`operator.crt`, `operator.key`, `ca.crt`) instead of printing the PEM
    /// blocks to stdout.
    #[arg(long)]
    pub out_dir: Option<PathBuf>,
}

/// Common arguments plus the verb for an `admin` invocation.
#[derive(Debug, Args)]
pub struct AdminArgs {
    /// Path to the node configuration file — read for TLS material and the
    /// default `--target` (the first candidate from the configured
    /// `[discovery]` backend).
    #[arg(long)]
    pub config: PathBuf,

    /// The `host:port` of the coordinator to contact. Defaults to the first
    /// candidate from the config's `[discovery]` backend; an error results if
    /// neither yields a target.
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

    /// Promote a caught-up learner to voter (ADR 0016 step 3, ADR 0037 §7).
    ///
    /// A learner still behind the promotion threshold yields a retryable
    /// "behind" response; this verb polls until it catches up or `--wait`
    /// elapses, which is what makes `coordinator replace` operable end to end.
    ///
    /// There is deliberately no `--remove`: a caller never names a pair. If
    /// the voter set is full the leader may fold out one voter it has itself
    /// observed unreachable past `removal_grace` (the hands-off,
    /// terminate-before-launch path), and a caller-named pair is the
    /// operator-only `replace-voter` below.
    Promote {
        /// The learner to promote.
        #[arg(long)]
        node_id: u64,
        /// How long to keep retrying while the learner is still catching up.
        #[arg(long, default_value = "60s", value_parser = parse_duration)]
        wait: Duration,
    },

    /// Replace one voter with another in a single joint change (ADR 0037 §7).
    ///
    /// The launch-before-terminate verb: the replacement installation enrolls,
    /// joins, and catches up as a learner on its own, and this names the pair
    /// the cluster cannot infer — a replacement carries a *new* machine
    /// identity by construction, so nothing links it to its predecessor.
    /// `--old` may be perfectly alive; a live predecessor never qualifies for
    /// the evidence-gated path, which is exactly why this verb exists.
    /// Operator credential only, and idempotent, so rollout automation may
    /// retry it.
    ReplaceVoter {
        /// The voter being replaced. Must currently be a voter.
        #[arg(long)]
        old: u64,
        /// The caught-up learner taking over the seat.
        #[arg(long)]
        new: u64,
    },

    /// Remove a node from membership entirely. Operator credential only
    /// (ADR 0037 §7): removal is the one membership change that can shrink a
    /// quorum, so no machine certificate may reach it.
    Remove {
        /// The node to remove.
        #[arg(long)]
        node_id: u64,
    },

    /// Repoint an existing member's dial address (ADR 0037 §6).
    ///
    /// Operator-credential break-glass for the pet deployment whose address
    /// moved. There is deliberately no self-service form: a wrong address can
    /// split-brain a raft, and under the immutable model an instance whose
    /// address changed is simply a new instance. The leader commits only after
    /// dialing the *new* address and confirming both that its serving
    /// certificate carries the machine identity already bound to this seat and
    /// that `ProbeCluster` there reports this node id.
    SetAddress {
        /// The member to repoint. Must already be in membership — this verb
        /// never creates a seat.
        #[arg(long)]
        node_id: u64,
        /// The `host:port` peers should dial instead.
        #[arg(long)]
        addr: String,
    },

    /// Print cluster-wide status (ADR 0037 §9).
    ///
    /// Always the *cluster's* view — membership with roles, machine-identity
    /// bindings, replication and health — never this daemon's local readiness
    /// document (that is `local-status`). Without `--target` the first
    /// candidate from the config's `[discovery]` backend is dialed, exactly
    /// as for the other network verbs; either way, if the answering replica
    /// is a follower the CLI re-dials the leader it names once, so the
    /// rendered document carries the leader-only fields (health, per-follower
    /// lag) whenever a leader is reachable.
    Status {
        /// Emit the stable JSON of ADR 0037 §9 instead of the human table:
        /// membership with roles and machine-identity bindings, per-follower
        /// replication lag and health (leader-only; `null`, never fabricated,
        /// when no leader answered), and leadership. This is the scripting
        /// surface; the table is for people.
        #[arg(long)]
        json: bool,
    },

    /// Print this daemon's own readiness document over the local admin
    /// socket (ADR 0037 §3/§9).
    ///
    /// The `GET /readyz` body, without needing the client listener — the only
    /// status a parked or formation-failed daemon can serve, which is exactly
    /// when an operator most needs one. Like `issue-operator-cert`, this
    /// rides the Unix socket in the daemon's data directory; `--target` does
    /// not apply.
    LocalStatus {
        /// Print the readiness document verbatim as JSON (it is already the
        /// documented stable shape of ADR 0037 §9) instead of the human table.
        #[arg(long)]
        json: bool,
    },

    /// Sign a new operator certificate on the local admin socket (ADR 0037
    /// §3).
    ///
    /// The documented day-0 recovery for "the `init` output was lost" and for
    /// "all operator certificates lost" — which is why it cannot be the
    /// network path: that path authorizes with an existing operator
    /// certificate, the very thing this recovers. It grants nothing local
    /// disk access did not already confer, since the CA key is on this disk.
    /// Unlike the other verbs here, `--target` does not apply.
    IssueOperatorCert {
        /// A PEM CSR to sign. Without one the cluster mints the keypair and
        /// prints both halves.
        #[arg(long)]
        operator_csr: Option<PathBuf>,
        /// The common name the certificate carries.
        #[arg(long)]
        operator_cn: Option<String>,
        /// Write the issued material to files under this directory instead of
        /// printing the PEM blocks to stdout.
        #[arg(long)]
        out_dir: Option<PathBuf>,
    },

    /// Re-root this cluster: replace the CA every machine credential chains
    /// to (ADR 0037 §4). See `docs/operations/re-rooting.md`.
    ///
    /// The compromise response for anything root-equivalent — any disk that
    /// has ever held the CA key, or a coordinator enrollment token. Like
    /// `init` and `issue-operator-cert`, and for the same reason, this rides
    /// the daemon's Unix socket rather than the network: re-rooting is
    /// root-equivalent authority, and authorizing it with a certificate
    /// issued by the CA being replaced would make every operator credential a
    /// re-rooting credential. `--target` does not apply, and it must be run
    /// on the **leader's** host — the new key is written to the disk it runs
    /// on, and that disk has to be the one that signs.
    RotateCa {
        #[command(subcommand)]
        verb: RotateCaVerb,
    },
}

/// The three moves of a re-root (ADR 0037 §4).
#[derive(Debug, Subcommand)]
pub enum RotateCaVerb {
    /// Open the dual-trust window: mint a new root, record it ahead of the
    /// outgoing one, sign under it from now on, and key the other voters.
    ///
    /// Nothing is refused at this point — both roots verify — so this is the
    /// safe half. Re-running it against a rotation already in progress mints
    /// nothing and only re-attempts key distribution, which is the recovery
    /// for a voter that was unreachable the first time.
    Begin,

    /// Report the recorded roots, this replica's turnover, and the custody
    /// accounting (ADR 0037 §4/§9). Read-only, and answerable on any replica
    /// — two of its three questions are about the replica it runs on.
    Status {
        /// Emit JSON instead of the human table.
        #[arg(long)]
        json: bool,
    },

    /// Close the dual-trust window: drop the outgoing root.
    ///
    /// The one irreversible step, and the only one that can strand a node.
    /// Refused until one full leaf lifetime has passed since `begin` — the
    /// point after which no leaf the outgoing root signed can still be
    /// unexpired — unless `--force` says the operator has verified turnover
    /// themselves.
    Complete {
        /// Complete before the leaf-lifetime bound has elapsed. Run it after
        /// verifying turnover, not instead of verifying it.
        #[arg(long)]
        force: bool,
    },
}

// ---------------------------------------------------------------------------
// `coppice node` — enrollment tokens and identity revocation (ADR 0037 §5)
// ---------------------------------------------------------------------------

/// Common arguments plus the verb for a `coppice node` invocation.
///
/// Unlike `admin`, the mTLS material is named explicitly: these verbs are run
/// with an operator certificate from a machine that has no coordinator config
/// file, which is exactly the audience ADR 0022's operator profile exists for.
#[derive(Debug, Args)]
pub struct NodeArgs {
    /// The `host:port` of a coordinator's admin surface.
    #[arg(long)]
    pub target: String,

    /// The cluster CA bundle (PEM) that verifies the target.
    #[arg(long)]
    pub ca: PathBuf,

    /// The operator certificate (PEM) to present.
    #[arg(long)]
    pub cert: PathBuf,

    /// The operator private key (PEM).
    #[arg(long)]
    pub key: PathBuf,

    /// The logical cluster id the target must serve. Optional: the target was
    /// named explicitly, so this is a guard, not a lookup.
    #[arg(long)]
    pub cluster_id: Option<String>,

    #[command(subcommand)]
    pub verb: NodeVerb,
}

#[derive(Debug, Subcommand)]
pub enum NodeVerb {
    /// Mint, list, and revoke enrollment tokens (ADR 0037 §5).
    EnrollToken {
        #[command(subcommand)]
        verb: EnrollTokenVerb,
    },

    /// Mark an issued identity revoked, so the leader refuses its renewals.
    ///
    /// This is the other half of evicting an illegitimate enrollment: revoking
    /// the *token* stops future enrollments but leaves already-issued leaves
    /// renewing (ADR 0037 §5).
    #[command(group = clap::ArgGroup::new("identity").required(true))]
    RevokeIdentity {
        /// A coordinator machine identity (`machine-<uuid>`).
        #[arg(long, value_parser = crate::node::parse_machine_id, group = "identity")]
        machine: Option<coppice_core::id::MachineId>,
        /// A compute node id (`node-<uuid>`).
        #[arg(long, value_parser = crate::node::parse_node_id, group = "identity")]
        node: Option<coppice_core::id::NodeId>,
    },
}

#[derive(Debug, Subcommand)]
pub enum EnrollTokenVerb {
    /// Mint a token for one role. The secret is printed to stdout exactly
    /// once — nothing stores it and no verb can recover it.
    Mint {
        /// The role the token grants; never both (ADR 0037 §5).
        #[arg(long, value_enum)]
        role: RoleArg,
        /// How long the token stays usable (`"15m"`, `"720h"`). Omit for a
        /// long-lived launch-template token, the supported v1 default.
        #[arg(long, value_parser = parse_duration)]
        ttl: Option<Duration>,
        /// A human label, unique among live tokens by convention.
        #[arg(long)]
        label: String,
    },

    /// List the enrollment tokens. Never prints hashes.
    List,

    /// Revoke a token: future enrollments stop, issued leaves are untouched.
    Revoke {
        #[arg(long, value_parser = crate::node::parse_enroll_token_id)]
        id: coppice_core::id::EnrollTokenId,
    },
}

/// The `--role` values, spelled as an operator types them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RoleArg {
    Agent,
    Coordinator,
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
    fn default_run_takes_only_config() {
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

    /// ADR 0037 §1: startup intent is derived from the data directory, so the
    /// flags that used to declare it are gone — and gone loudly, because a
    /// launch template still passing `--bootstrap` must fail to start rather
    /// than silently mean something new.
    #[test]
    fn the_removed_intent_flags_are_rejected() {
        for flag in ["--bootstrap", "--join"] {
            assert!(
                Cli::try_parse_from(["coppice-coordinator", "--config", "/etc/c.toml", flag])
                    .is_err(),
                "{flag} must no longer parse"
            );
        }
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
    fn admin_promote_parses_node_id_and_wait() {
        // No `--remove`: a caller never names a pair (ADR 0037 §7). The
        // removal a promotion may fold in is the leader's own
        // evidence-gated one, and a caller-named pair is `replace-voter`.
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
            "--wait",
            "90s",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert_eq!(a.target.as_deref(), Some("coord-1:7071"));
        match a.verb {
            AdminVerb::Promote { node_id, wait } => {
                assert_eq!(node_id, 4);
                assert_eq!(wait, Duration::from_secs(90));
            }
            other => panic!("wrong verb: {other:?}"),
        }
    }

    #[test]
    fn admin_replace_voter_parses_the_pair() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "replace-voter",
            "--old",
            "2",
            "--new",
            "5",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        match a.verb {
            AdminVerb::ReplaceVoter { old, new } => {
                assert_eq!(old, 2);
                assert_eq!(new, 5);
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
            AdminVerb::Promote { wait, .. } => assert_eq!(wait, Duration::from_secs(60)),
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
    fn admin_status_defaults_to_the_human_table() {
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
    fn admin_status_json_parses() {
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

    /// `local-status` is a distinct verb from `status`: one is the daemon's
    /// own readiness document over the Unix socket, the other the cluster-wide
    /// membership document — collapsing them is exactly the schema instability
    /// ADR 0037 §9 forbids.
    #[test]
    fn admin_local_status_parses_as_its_own_verb() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "local-status",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert!(matches!(a.verb, AdminVerb::LocalStatus { json: false }));

        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "local-status",
            "--json",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        assert!(matches!(a.verb, AdminVerb::LocalStatus { json: true }));
    }

    #[test]
    fn admin_set_address_parses() {
        let cli = parse(&[
            "coppice-coordinator",
            "admin",
            "--config",
            "/etc/c.toml",
            "set-address",
            "--node-id",
            "3",
            "--addr",
            "coord-3:7071",
        ]);
        let Some(Command::Admin(a)) = cli.command else {
            panic!("expected admin subcommand");
        };
        match a.verb {
            AdminVerb::SetAddress { node_id, addr } => {
                assert_eq!(node_id, 3);
                assert_eq!(addr, "coord-3:7071");
            }
            other => panic!("wrong verb: {other:?}"),
        }
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
