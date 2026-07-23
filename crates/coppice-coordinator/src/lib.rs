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
// The self-converging membership loop (ADR 0037 §4). Published so the later
// `/readyz` package can consume its `ConvergenceStatus`/`Phase`.
pub mod convergence;
// Coordinator discovery backends (ADR 0037 §2): the trait, the static/dns/file
// backends, and the file-registration helper. Consumed by `bootstrap`,
// `convergence`, and the `admin` formation probe guard; public because the
// trait and the run-scoped `FileRegistration` appear in `bootstrap` and
// `convergence` signatures.
pub mod discovery;
mod leadership;
mod limits;
// The bootstrap-policy TOML schema and its idempotent command proposals
// (ADR 0037 §3): parsed by the formation handler and reused by `coppice dev`'s
// seeding, so the two never drift.
mod liveness;
pub mod policy;
// The machine-readable readiness surface (ADR 0037 §7): `GET /readyz` and its
// pure gate. Public so the daemon builds a `ReadyzState` in `bootstrap` and the
// gate matrix can be exercised directly.
pub mod readyz;
mod runtime;
// Minimal systemd `Type=notify` client (ADR 0037 §7): READY=1 when listeners
// serve, STOPPING=1 at shutdown. Silent no-op off systemd.
mod systemd;
mod tasks;

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
        Some(cli::Command::Admin(admin)) => admin::run_cli(admin).await,
        None => bootstrap::run(cli.run_args()).await,
    }
}
