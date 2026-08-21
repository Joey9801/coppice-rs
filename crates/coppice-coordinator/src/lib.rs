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
// The public client listener's serving path (ADR 0037 §4 `[client_tls]`):
// plain HTTP, or a per-accept rustls acceptor that surfaces the peer
// certificate to handlers. Public because both serving surfaces —
// pre-formation and post-formation — and the integration suite drive it.
pub mod clientedge;
pub mod config;
// The self-converging membership loop (ADR 0037 §1/§6): the parked half that
// enrolls, discovers, probes, and joins, and the post-start half that carries
// a started replica from learner to voter. Private: it is driven entirely
// from `bootstrap`, and nothing outside this crate has a reason to hold one.
mod convergence;
// Coordinator discovery backends (ADR 0037 §2): the trait, the
// static/dns/file/ec2-asg backends, and the file-registration helper. Public
// because the trait and the run-scoped `FileRegistration` appear in
// `bootstrap` signatures.
pub mod discovery;

/// The leader-side enrollment and renewal core (ADR 0037 §4/§5), shared by the
/// `ForwardEnroll` admin RPC and the public `POST /api/v1/enroll` route.
mod enroll;

// The follower's proxy of `/enroll` to the leader, as a standalone endpoint
// (ADR 0037 §4). Public so the same production hop can be driven directly.
pub use enroll::proxying_enroll_endpoint;
// Explicit formation (ADR 0037 §3): the seven steps `coppice coordinator
// init` runs, the `formation_complete` marker semantics, and the phase every
// surface reads to decide what it may answer.
mod formation;
mod leadership;
mod limits;
mod liveness;
// The local admin socket (ADR 0037 §3): formation's authority, and the
// `issue-operator-cert` day-0 recovery verb. Both halves live here; the
// module is public for the client half ([`localadmin::call`] and its wire
// types), which the CLI verbs and the integration suite both speak.
pub mod localadmin;
// The bootstrap-policy TOML schema and its idempotent command proposals
// (ADR 0037 §3): a library for the formation handler and `coppice dev`'s
// seeding, so the two never drift. No CLI surface yet — `cluster init` wires
// it up in a later chunk.
pub mod policy;
// The client half of `ProbeCluster` (ADR 0037 §3): formation's double-init
// guard today, the convergence loop's search for the cluster later.
/// `coppice node` — the operator-facing enrollment-token and identity verbs
/// (ADR 0037 §5), over the same admin channel `coordinator admin` uses.
pub mod node;
mod probe;
mod runtime;
// Minimal systemd `Type=notify` client (ADR 0037 §9): READY=1 when listeners
// serve, STOPPING=1 at shutdown. Silent no-op off systemd.
mod systemd;
mod tasks;
// The real coordinator renewal attempt (leader-local branch included),
// exposed so integration tests can drive it without waiting out the timer.
#[doc(hidden)]
pub use tasks::renewal::renew_once as coordinator_renew_once;
// Named in `bootstrap::serve_runtime_with_serving_sans`'s signature, so it has
// to be reachable by embedders driving that seam; `[pacing]` itself stays
// crate-private (`config::PacingConfig::renewal` builds this).
pub use tasks::renewal::RenewalPacing;

#[cfg(test)]
mod test_support;

use anyhow::Result;

// The `ControlPlane` impl, exported so integration tests exercise the same
// submit/abort path the (future) HTTP listener will host.
pub use tasks::api_server::CoordinatorControlPlane;

// The replica-local node-fetch client (ADR 0034), backing both `fetch_logs`
// and `fetch_metrics`. Exported so the end-to-end best-effort telemetry tests
// can attach a real client to a `CoordinatorControlPlane` and drive the full
// read path; the type already surfaces publicly through
// `bootstrap::BootedCoordinator::node_log_client`.
pub use tasks::node_client::NodeClient;

// The process-wide Prometheus recorder install (issue #46). Re-exported from
// the otherwise-private `runtime` module so an embedder that owns the process
// lifecycle — the daemon `bootstrap::run`, and `coppice dev`, which runs a
// coordinator and an agent in ONE process off a single shared recorder — can
// install it once and hand the returned handle to every `/metrics` endpoint.
pub use runtime::install_metrics_recorder;

/// Register descriptions for every metric a coordinator process can emit,
/// recursing into each crate and module that exposes metrics. The `/metrics`
/// endpoint (issue #46) — served on the client API listener at `/metrics`, not
/// a dedicated port — calls this once as [`install_metrics_recorder`] installs
/// the Prometheus recorder, without knowing any module's internals.
///
/// There is deliberately no coordinator metrics-address config knob: the
/// endpoint rides the existing client listener rather than a separate address,
/// so of ADR 0020's `[observability]` fields only `otlp_endpoint` stays
/// parsed-but-unused.
pub fn describe_metrics() {
    coppice_consensus::describe_metrics();
    coppice_tls::describe_metrics();
    tasks::event_fanout::describe_metrics();
    tasks::node_client::describe_metrics();
}

/// Run any point-in-time sampling behind coordinator metrics, recursing the
/// same modules as [`describe_metrics`]. The `/metrics` endpoint calls this
/// immediately before rendering each scrape.
pub fn gather_metrics() {
    coppice_consensus::gather_metrics();
    coppice_tls::gather_metrics();
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
        Some(cli::Command::Init(args)) => localadmin::run_init(args).await,
        Some(cli::Command::Admin(admin)) => admin::run_cli(admin).await,
        None => bootstrap::run(cli.run_args()).await,
    }
}
