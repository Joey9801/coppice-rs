//! `coppice dev`: a self-contained single-node cluster for development and
//! integration testing.
//!
//! One process runs a coordinator daemon (consensus, scheduler, agent gateway
//! — the full task runtime) plus an in-process agent session dialing it over
//! localhost. Nothing about the security machinery is simulated: the
//! coordinator starts **certless** on the production park/converge path
//! ([`bootstrap::run_with`]), `dev` performs the single operator act the
//! lifecycle has — one local `init` over the daemon's Unix admin socket, the
//! same call `coppice coordinator init` makes — and formation mints the real
//! cluster CA and the coordinator's first leaf. The agent then **enrolls**
//! against the client listener with a token the `init` policy seeded, exactly
//! as a fleet machine does (ADR 0037 §3/§4/§5).
//!
//! What is dev-only is the *posture*, and it is declared rather than implied:
//! the client listener serves plain HTTP (`[client_tls] insecure = true`), so
//! the enrollment token crosses it in the clear and anything that can reach
//! the ports is effectively admin. Never expose a dev instance beyond
//! localhost.
//!
//! The data directory defaults to a temp dir deleted on exit; pass
//! `--data-dir` to keep state across runs. A second run against the same
//! directory **resumes**: the coordinator restarts from its manifest stamp,
//! `init` answers `AlreadyInitialized` (a success, ADR 0037 §3), the agent
//! finds its leaf already installed and makes no enrollment call at all, and
//! the ports, cluster id, and node id are the ones the directory remembers.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use coppice_agent::config::{
    CapacityConfig, Config as AgentConfig, ExecutorConfig, ListenConfig, TlsConfig as AgentTls,
};
use coppice_agent::executor::{DockerExecutor, Executor, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::session::{self, Session};
use coppice_agent::telemetry::FilesystemSink;
use coppice_api::http::ReadyzPhase;
use coppice_consensus::fs::RealFs;
use coppice_coordinator::bootstrap;
use coppice_coordinator::config as coord_config;
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::bytes::ByteSize;
use coppice_core::id::{ClusterId, NodeId, QuotaEntityId};
use coppice_enroll::{EnrollmentConfig, Secret};

#[derive(Debug, clap::Args)]
pub struct DevArgs {
    /// Data directory. Defaults to a fresh temp dir deleted on exit; pass a
    /// path to keep cluster and agent state across runs.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Client API port (0 reuses this data dir's remembered port, or picks a
    /// free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    client_port: u16,

    /// Agent-gateway port (0 reuses this data dir's remembered port, or picks
    /// a free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    agent_port: u16,

    /// Raft/admin port (0 reuses this data dir's remembered port, or picks a
    /// free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    raft_port: u16,

    /// Agent NodeService port for job-log retrieval (0 reuses this data dir's
    /// remembered port, or picks a free one; logged at startup). The
    /// in-process coordinator dials this over mTLS to serve
    /// `GET /api/v1/jobs/{job}/logs` (ADR 0034).
    #[arg(long, default_value_t = 0)]
    node_service_port: u16,

    /// Agent Prometheus `/metrics` port (0 reuses this data dir's remembered
    /// port, or picks a free one; logged at startup). Dev serves the agent
    /// scrape endpoint here (issue #46); the coordinator's `/metrics` rides
    /// the client API port instead.
    #[arg(long, default_value_t = 0)]
    metrics_port: u16,

    /// Executor backing the in-process agent. `fake` runs the lifecycle
    /// without containers (and captures no logs); `docker` is the production
    /// executor and needs a reachable Docker daemon.
    #[arg(long, value_enum, default_value_t = DevExecutor::Fake)]
    executor: DevExecutor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum DevExecutor {
    Fake,
    Docker,
}

impl std::fmt::Display for DevExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fake => f.write_str("fake"),
            Self::Docker => f.write_str("docker"),
        }
    }
}

/// The well-known quota entity dev jobs charge to. Fixed rather than minted
/// so submit examples keep working verbatim across dev clusters.
const DEV_QUOTA_ENTITY: &str = "quota-00000000-0000-0000-0000-000000000001";

/// The agent-role enrollment token `init` seeds and the in-process agent
/// presents (ADR 0037 §5).
///
/// A fixed literal rather than a generated secret, and that is not a
/// shortcut being papered over: the dev cluster's client listener is plain
/// HTTP, so the token crosses it in the clear regardless, and the banner says
/// so. What matters here is that the *mechanism* is the production one — a
/// role-scoped token seeded by the formation policy, a CSR, a cluster-signed
/// leaf — not that the secret is unguessable on a loopback interface no
/// remote party can reach.
const DEV_ENROLL_TOKEN: &str = "cpk_coppice-dev-local-agent-token";

/// The label carrying that token's idempotency (ADR 0037 §5): a re-`init`
/// mints nothing while a live token holds it.
const DEV_ENROLL_LABEL: &str = "dev-agent";

/// Read a persisted typed id from `path`, or mint one and persist it.
fn load_or_mint<T>(path: &Path, mint: impl FnOnce() -> T) -> Result<T>
where
    T: std::str::FromStr + std::fmt::Display,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    if path.exists() {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Ok(raw
            .trim()
            .parse()
            .with_context(|| format!("parsing {}", path.display()))?)
    } else {
        let id = mint();
        std::fs::write(path, format!("{id}\n"))
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(id)
    }
}

/// Resolve one listen port and remember it under the data directory.
///
/// An explicit `--*-port` always wins. Otherwise a persistent data dir reuses
/// the port it used last, which is what makes a restart a *resume*: the raft
/// manifest records this replica's advertised address, and the printed URLs
/// stay valid across runs. A fresh (or temp) directory picks a free ephemeral
/// port and writes it down.
fn resolve_port(root: &Path, name: &str, requested: u16) -> Result<u16> {
    let path = root.join(format!("port-{name}"));
    let remember = |port: u16| -> Result<u16> {
        std::fs::write(&path, format!("{port}\n"))
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(port)
    };

    if requested != 0 {
        return remember(requested);
    }
    if let Ok(raw) = std::fs::read_to_string(&path) {
        if let Ok(port) = raw.trim().parse::<u16>() {
            if port != 0 {
                return Ok(port);
            }
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    remember(listener.local_addr().context("local addr")?.port())
}

/// Sample every metric tree the dev cluster's single global recorder holds
/// (issue #46).
///
/// A one-process dev cluster has exactly one global Prometheus recorder, so
/// every scrape endpoint — the coordinator's on the client listener and the
/// agent's own — renders the union of both daemons' metrics. A scrape must
/// therefore sample BOTH trees before rendering, unlike production where each
/// daemon owns its own recorder and gathers only its own tree. This is the
/// shared `gather` behind both dev `/metrics` endpoints.
fn dev_gather() {
    coppice_coordinator::gather_metrics();
    coppice_agent::gather_metrics();
}

pub async fn run(args: DevArgs) -> Result<()> {
    // -- Layout: everything under one root. --------------------------------
    let (root, _tempdir) = match &args.data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating data dir {}", dir.display()))?;
            // Absolute, but **not** canonicalized: the config records this
            // path, the admin socket lives under it, and a Unix socket path
            // has ~100 bytes to spend. Resolving a short symlink an operator
            // deliberately pointed at a deep directory would spend them all.
            let dir = if dir.is_absolute() {
                dir.clone()
            } else {
                std::env::current_dir()
                    .context("resolving the working directory")?
                    .join(dir)
            };
            (dir, None)
        }
        None => {
            let dir = tempfile::tempdir().context("creating temp data dir")?;
            (dir.path().to_path_buf(), Some(dir))
        }
    };

    // Persistent identities: the cluster id must keep matching the manifest
    // stamp across restarts, and the agent node id must keep matching both its
    // journal history and the CN of the leaf it enrolled for. Both live as
    // typed-string files under the root.
    let cluster_id: ClusterId = load_or_mint(&root.join("cluster-id"), ClusterId::new)?;
    let agent_node: NodeId = load_or_mint(&root.join("agent-node-id"), NodeId::new)?;

    let raft_port = resolve_port(&root, "raft", args.raft_port)?;
    let agent_port = resolve_port(&root, "agent", args.agent_port)?;
    let client_port = resolve_port(&root, "client", args.client_port)?;
    let node_service_port = resolve_port(&root, "node-service", args.node_service_port)?;
    let metrics_port = resolve_port(&root, "metrics", args.metrics_port)?;

    // Install the one process-wide Prometheus recorder this dev cluster shares
    // (issue #46). `coppice dev` runs a coordinator AND an agent in one process,
    // so there is a single global recorder: install it here, describe both
    // daemons' metric trees into it (the coordinator helper describes the
    // coordinator tree; the agent's is described explicitly), and hand the one
    // handle to every `/metrics` endpoint below.
    let metrics_handle = coppice_coordinator::install_metrics_recorder()?;
    coppice_agent::describe_metrics();

    // -- Coordinator: certless, on the production park/converge path. ------
    //
    // Nothing is provisioned: the `[tls]` trio names paths that do not exist
    // yet, and formation writes into them (ADR 0037 §4's minimal deployment).
    let coord_data = root.join("coordinator");
    let coord_pki = root.join("coordinator-pki");
    std::fs::create_dir_all(&coord_pki).context("creating the coordinator PKI dir")?;
    let config_path = root.join("coordinator.toml");
    std::fs::write(
        &config_path,
        coordinator_toml(&CoordinatorLayout {
            cluster_id,
            data_dir: &coord_data,
            pki_dir: &coord_pki,
            raft_port,
            agent_port,
            client_port,
        }),
    )
    .context("writing dev coordinator config")?;

    let resolved = coord_config::load(&config_path).context("loading dev coordinator config")?;

    // The daemon lifecycle itself — park or resume, form on `init`, serve,
    // drain — over a shutdown watch this command owns. `run_with` binds every
    // listener, so there is no dev-specific transport wiring left.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let metrics = coppice_api::http::MetricsEndpoint::new(metrics_handle.clone(), dev_gather);
    let mut coordinator = tokio::spawn(bootstrap::run_with(resolved, metrics, Some(shutdown_rx)));

    // The single operator act in the whole lifecycle (ADR 0037 §3). The socket
    // is the documented default, `<data_dir>/admin.sock`, because the config
    // above leaves `[listen] admin_socket` unset.
    let admin_socket = coord_data.join("admin.sock");
    let formed = match init_cluster(&admin_socket, &coord_data, &mut coordinator).await {
        Ok(formed) => formed,
        Err(e) => {
            // A daemon that died on the way up (a taken port, unreadable
            // material) explains itself far better than the socket timeout
            // does; prefer its error when it has one.
            let _ = shutdown_tx.send(true);
            return Err(daemon_error(&mut coordinator).await.unwrap_or(e));
        }
    };
    if let Some(operator) = &formed.operator {
        write_operator_material(&root, operator)?;
    }

    // Formation is complete, but the client listener only starts serving once
    // the task runtime is up — and the agent's first act is to enroll against
    // it. Wait for it before starting the agent, so enrollment does not race
    // the route it posts to.
    let api = format!("http://127.0.0.1:{client_port}");
    wait_for_client_api(&api, &mut coordinator).await?;

    // -- Agent: in-process, enrolling, dialing the gateway over localhost. --
    let agent_pki = root.join("agent-pki");
    std::fs::create_dir_all(&agent_pki).context("creating the agent PKI dir")?;
    let agent_config = AgentConfig {
        node_id: agent_node,
        data_dir: root.join("agent"),
        coordinators: vec![format!("localhost:{agent_port}")],
        // Certless, exactly like the coordinator: these three paths are where
        // enrollment installs what it is handed.
        tls: AgentTls {
            cert_path: agent_pki.join("agent.crt"),
            key_path: agent_pki.join("agent.key"),
            ca_path: agent_pki.join("ca.crt"),
        },
        // The real thing (ADR 0037 §4): a token and an address. `insecure`
        // because the dev client listener is `[client_tls] insecure = true`,
        // and the posture must be declared on both ends or the endpoint is
        // refused at config validation.
        enrollment: Some(EnrollmentConfig {
            endpoint: api.clone(),
            token: Some(Secret::new(DEV_ENROLL_TOKEN)),
            token_path: None,
            insecure: true,
        }),
        // Generous static capacity: dev jobs should never be capacity-bound.
        capacity: CapacityConfig {
            cpu_millis: 16_000,
            memory: ByteSize::from_gib(16),
            disk: ByteSize::from_tib(1),
        },
        reservation: Default::default(),
        heartbeat_interval: Duration::from_secs(2),
        reconnect_backoff_min: Duration::from_millis(100),
        reconnect_backoff_max: Duration::from_secs(2),
        labels: Default::default(),
        // Docker Desktop on macOS exposes no Linux sysfs topology to this
        // process. Dev remains portable by retaining the S2 NanoCpus-only
        // behavior; production configs opt into affinity by default.
        executor: ExecutorConfig {
            whole_core_affinity: false,
            ..Default::default()
        },
        pressure: Default::default(),
        image_cache: Default::default(),
        telemetry: Default::default(),
        // The agent-hosted NodeService (ADR 0034): bind 127.0.0.1:<port> and
        // advertise 127.0.0.1, so the in-process coordinator can dial it for job
        // logs. The enrolled leaf carries the node id as a SAN — the cluster
        // adds that itself from the claimed identity — plus this advertised
        // host, which the cluster cannot know and the agent therefore declares.
        listen: Some(ListenConfig {
            addr: format!("127.0.0.1:{node_service_port}")
                .parse()
                .expect("node service socket addr"),
            advertise_host: "127.0.0.1".to_string(),
        }),
        // The agent's Prometheus `/metrics` endpoint (issue #46): bind
        // 127.0.0.1:<port> so dev mirrors production's two-endpoint shape (agent
        // scrape here, coordinator scrape on the client listener). Bound and
        // served below over the shared recorder, mirroring how `listen` is bound
        // by `serve_node_service`.
        metrics_addr: Some(
            format!("127.0.0.1:{metrics_port}")
                .parse()
                .expect("agent metrics socket addr"),
        ),
    };

    // Obtain the machine-plane leaf before anything tries to load it — the
    // same call, in the same place, that `coppice_agent::run_daemon` makes.
    // A no-op on every run after the first: a usable leaf on disk means no
    // network call and the token is never even read.
    coppice_agent::ensure_enrolled(&agent_config)
        .await
        .context("enrolling the dev agent")?;

    // Bind and serve the agent's `/metrics` endpoint before the executor match
    // moves `agent_config` into the session task (issue #46). Both dev scrape
    // URLs — this one and the coordinator's client-listener `/metrics` — share
    // the SAME recorder handle and `dev_gather`, so both render the identical
    // union of coordinator + agent metrics; the second endpoint exists only so
    // dev exercises the real agent scrape path, not because the views differ.
    if let Some(metrics_addr) = agent_config.metrics_addr {
        let listener = coppice_agent::metrics_server::prepare_listener(metrics_addr)
            .await
            .context("binding the dev agent metrics server")?;
        coppice_agent::metrics_server::serve(listener, metrics_handle.clone(), dev_gather);
    }
    // async-fn-in-trait futures carry no generic `Send` bound, so the spawn
    // happens per concrete executor type rather than in a generic helper. The
    // second tuple element holds the telemetry handle (Docker executor only)
    // alive for the dev cluster's lifetime, so its retention janitors are not
    // dropped early (§8.4).
    let (agent_join, _telemetry_guard) = match args.executor {
        DevExecutor::Fake => {
            let session = build_session(&agent_config, FakeExecutor::new())?;
            // The fake executor captures no container output, so the NodeService
            // serves no stores: every fetch honestly answers UnknownAttempt.
            serve_node_service(&agent_config, None, None)?;
            (tokio::spawn(run_agent(session, agent_config)), None)
        }
        DevExecutor::Docker => {
            // Mirror `run_daemon`'s wiring: connect the daemon, spawn the shared
            // disk-pressure monitor over data_dir + the data-root, then build the
            // executor (docker-executor.md §9, §11).
            let docker_host = coppice_agent::executor::docker::api::resolve_host(
                agent_config.executor.docker_host.as_deref(),
            )?;
            let docker = coppice_agent::executor::docker::api::connect(&docker_host)?;
            let data_root =
                coppice_agent::executor::docker::api::data_root(&docker, &docker_host).await?;
            let mut pressure_paths = vec![agent_config.data_dir.clone()];
            if let Some(root) = data_root {
                pressure_paths.push(root);
            }
            // The image cache reads the same filesystems the pressure monitor
            // watches for its High-pressure target (§7, §9); clone the paths
            // before they move into `pressure::spawn`.
            let cache_options = coppice_agent::executor::docker::cache::CacheOptions {
                config: agent_config.image_cache.clone(),
                state_path: Some(agent_config.data_dir.join("image-cache.json")),
                pressure_paths: pressure_paths.clone(),
                high_pct: agent_config.pressure.high_pct,
            };
            let pressure_rx =
                coppice_agent::pressure::spawn(pressure_paths.clone(), agent_config.pressure);
            // Mirror `run_daemon`'s telemetry wiring (§8): build the sinks + hub
            // and keep the returned handle alive for the agent task's lifetime.
            let telemetry = coppice_agent::telemetry::build(
                &agent_config.telemetry,
                &agent_config.data_dir,
                pressure_paths,
                agent_config.pressure.high_pct,
                pressure_rx.clone(),
            )
            .await?;
            // `Some` whenever any sink is configured; per-kind suppression (§8.3)
            // handles partial configs. Zero sinks ⇒ `None`: nothing consumes either
            // stream, so collect nothing rather than discard every batch.
            let telemetry_wiring = (!agent_config.telemetry.sinks.is_empty()).then(|| {
                coppice_agent::executor::docker::TelemetryWiring {
                    hub: telemetry.hub.clone(),
                    stores: telemetry.stores.clone(),
                    log_store: telemetry.log_store.clone(),
                    metrics_interval: agent_config.telemetry.metrics_interval,
                    drain_force_after: agent_config.telemetry.drain_force_after,
                }
            });
            let executor = DockerExecutor::new(
                docker,
                &agent_config.executor,
                &docker_host,
                agent_config.capacity.cpu_millis,
                agent_config.reservation.cpu_millis,
                agent_config.node(),
                pressure_rx,
                cache_options,
                telemetry_wiring,
            )
            .await?;
            let session = build_session(&agent_config, executor)?;
            // Serve the NodeService over the first LOG- and METRICS-consuming
            // telemetry stores (ADR 0034/0036), so the in-process coordinator
            // can dial for job logs and usage.
            serve_node_service(
                &agent_config,
                telemetry.log_store.clone(),
                telemetry.metric_store.clone(),
            )?;
            (
                tokio::spawn(run_agent(session, agent_config)),
                Some(telemetry),
            )
        }
    };

    // The cluster is only useful once the in-process agent's registration has
    // landed in applied state (epoch >= 1, ADR 0009). Treat that as the dev
    // command's readiness boundary rather than printing "up" while the loop
    // is still closing.
    let agent_epoch = wait_for_agent(&api, agent_node).await?;
    tracing::debug!(node = %agent_node, epoch = agent_epoch, "dev agent registered");

    eprintln!(
        "{}",
        ready_summary(&ReadySummary {
            root: &root,
            persistent: args.data_dir.is_some(),
            cluster_id,
            coordinator_raft_id: formed.raft_node_id,
            agent_node,
            agent_epoch,
            raft_port,
            agent_port,
            client_port,
            node_service_port,
            metrics_port,
            ui_available: coppice_api::http::ui_available(),
            quota_entity: DEV_QUOTA_ENTITY.parse().expect("dev quota entity id"),
            executor: args.executor,
        })
    );

    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;
    tracing::info!("shutting down the dev cluster");

    // Ordered teardown: stop the agent session first (its journal is crash-safe
    // by design, ADR 0009), then let the daemon drain itself through the shared
    // shutdown watch — `run_with` owns the whole coordinator-side order.
    agent_join.abort();
    let _ = agent_join.await;
    let _ = shutdown_tx.send(true);
    match coordinator.await {
        Ok(result) => result.context("the dev coordinator exited with an error")?,
        Err(e) if e.is_cancelled() => {}
        Err(e) => return Err(e).context("the dev coordinator task panicked"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Coordinator config
// ---------------------------------------------------------------------------

struct CoordinatorLayout<'a> {
    cluster_id: ClusterId,
    data_dir: &'a Path,
    pki_dir: &'a Path,
    raft_port: u16,
    agent_port: u16,
    client_port: u16,
}

/// The dev coordinator's config file: a production config with a single-voter
/// target, snappy raft timings, and a declared plain-HTTP client posture.
///
/// Regenerated on every run (ports and paths are derived), so edits do not
/// survive — the file exists to be read by `config::load`, not to be tuned.
fn coordinator_toml(layout: &CoordinatorLayout<'_>) -> String {
    format!(
        r#"# Generated by `coppice dev` on every run; edits are overwritten.
cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

# Single-node dev cluster: nothing to discover, but the section (and its
# matching backend table) is required — an explicit empty seed list is the
# `peers = []` successor (ADR 0037). `cluster_size = 1` is what makes one
# voter the *converged* state rather than a third of one.
[discovery]
backend = "static"
cluster_size = 1

[discovery.static]
addrs = []

[listen]
client_addr = "127.0.0.1:{client_port}"
raft_addr = "127.0.0.1:{raft_port}"
agent_addr = "127.0.0.1:{agent_port}"
advertise_host = "localhost"
# `admin_socket` is deliberately unset: the default is <data_dir>/admin.sock,
# which is the path `dev` drives `init` over.

[raft]
# Snappy dev timings: single node, localhost, no real elections to lose.
election_timeout = "300ms"
heartbeat_interval = "100ms"
rpc_timeout = "2s"

# Certless (ADR 0037 §4's minimal deployment): none of these three files
# exists at startup. Formation mints the cluster CA and this daemon's first
# leaf and writes them here.
[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[client_tls]
# Plain HTTP on the client listener (ADR 0037 §4: the posture is always
# explicit, never implied). The agent's enrollment token crosses this listener
# in the clear, which is why its `[enrollment] insecure` says so too.
insecure = true
"#,
        cluster_id = layout.cluster_id,
        data_dir = layout.data_dir.display(),
        client_port = layout.client_port,
        raft_port = layout.raft_port,
        agent_port = layout.agent_port,
        cert = layout.pki_dir.join("coordinator.crt").display(),
        key = layout.pki_dir.join("coordinator.key").display(),
        ca = layout.pki_dir.join("ca.crt").display(),
    )
}

/// The bootstrap policy `init` applies (ADR 0037 §3), seeding exactly the
/// replicated state a dev cluster needs before it can do anything.
///
/// A new cluster's policy has an **empty** priority-multiplier table, so every
/// `SubmitJob` fails synchronous validation until one commits, and there is no
/// quota entity to charge a job to. In production that is deliberate: policy
/// is replicated state an operator configures explicitly, and the node config
/// file never seeds it (ADR 0020). Dev has no operator, so it hands `init` the
/// same document an operator would: multipliers for priorities `-2..=2`
/// (doubling per step — monotone in priority, as ADR 0021's ranking expects),
/// the well-known "dev" quota entity, and the agent-role enrollment token the
/// in-process agent presents.
///
/// Every entry is idempotent by construction on the server side, so a re-run
/// against an existing cluster changes nothing — which is what lets `dev`
/// treat `AlreadyInitialized` as success.
fn dev_policy_toml() -> String {
    let mut out = String::new();
    for priority in -2i32..=2 {
        out.push_str(&format!(
            "[[priority_multiplier]]\nindex = {priority}\nmultiplier = {multiplier}\n\n",
            multiplier = 2f64.powi(priority),
        ));
    }
    // ~1e6 CU: deep enough that dev jobs never starve on quota, far enough
    // from u64::MAX to stay clear of saturation.
    out.push_str(&format!(
        "[[quota_entity]]\nid = \"{DEV_QUOTA_ENTITY}\"\nname = \"dev\"\nquota = 1000000000000\n\n"
    ));
    out.push_str(&format!(
        "[[enroll_token]]\nsecret = \"{DEV_ENROLL_TOKEN}\"\nrole = \"agent\"\n\
         label = \"{DEV_ENROLL_LABEL}\"\n"
    ));
    out
}

// ---------------------------------------------------------------------------
// Formation
// ---------------------------------------------------------------------------

/// What `dev` learned from its one `init` call.
struct FormedCluster {
    /// This replica's allocate-once raft identity (ADR 0025).
    raft_node_id: u64,
    /// The day-0 operator credential, on the run that actually formed. A
    /// resumed cluster answers `AlreadyInitialized` and issues nothing.
    operator: Option<OperatorPem>,
}

/// Wait for the daemon to be ready for `init`, then run it (ADR 0037 §3).
///
/// The wait is not cosmetic. The admin socket serves in *every* phase, but the
/// `init` verb is only answerable in two of them: a parked daemon, whose park
/// loop is the consumer of the formation channel, and a formed one, which
/// short-circuits to `AlreadyInitialized`. Between the two — a daemon resuming
/// an existing directory, which still reports `waiting` until it publishes
/// itself formed — nothing is consuming formation requests, so a call landing
/// there would block forever. The manifest is the thing that distinguishes
/// them: its presence is what makes the daemon take the resume branch, so
/// `waiting` is only an invitation to `init` while there is no manifest.
async fn init_cluster(
    socket: &Path,
    coord_data: &Path,
    coordinator: &mut tokio::task::JoinHandle<Result<()>>,
) -> Result<FormedCluster> {
    let manifest = coord_data.join("manifest");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        if coordinator.is_finished() {
            bail!("the dev coordinator exited before it could be initialized");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("the dev coordinator did not become ready for `init` within 60 seconds");
        }
        match coppice_coordinator::localadmin::call(socket, AdminCall::Status).await {
            Ok(AdminReply::Status { status }) => match status.phase {
                // Formed already: `init` is answerable and will say so.
                ReadyzPhase::Joining | ReadyzPhase::Learner | ReadyzPhase::Voter => break,
                // Parked *and* nothing on disk to resume: the park loop is
                // waiting on exactly this call.
                ReadyzPhase::Waiting if !manifest.exists() => break,
                // Resuming; the phase will settle. Or a fail-stop, which has no
                // resume path and must be reported rather than retried.
                ReadyzPhase::Waiting => {}
                ReadyzPhase::FormationFailed => {
                    bail!(
                        "the dev coordinator's data directory records a formation that never \
                         completed; there is no resume path — remove {} and start again \
                         (ADR 0037 §3)",
                        coord_data.display()
                    );
                }
                ReadyzPhase::HistorySuperseded => {
                    bail!(
                        "the dev coordinator's data directory holds a raft history that a \
                         later re-init superseded; there is no resume path — remove {} and \
                         start again (ADR 0037 §3)",
                        coord_data.display()
                    );
                }
            },
            Ok(other) => bail!("unexpected reply to the dev coordinator's status: {other:?}"),
            // Not bound yet: the socket appears early in startup, but not
            // instantly.
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let reply = coppice_coordinator::localadmin::call(
        socket,
        AdminCall::Init {
            policy: Some(dev_policy_toml()),
            operator_csr: None,
            operator_cn: Some("dev".to_string()),
        },
    )
    .await
    .context("running `init` on the dev coordinator")?;

    match reply {
        AdminReply::Formed {
            node_id, operator, ..
        } => {
            tracing::info!(raft_node = node_id, "dev cluster formed");
            Ok(FormedCluster {
                raft_node_id: node_id,
                operator: Some(operator),
            })
        }
        // A distinct outcome, not an error (ADR 0037 §3): this is what a second
        // `coppice dev` against the same data dir gets, and it means resume.
        AdminReply::AlreadyInitialized { status } => {
            tracing::info!("dev cluster already initialized; resuming");
            Ok(FormedCluster {
                raft_node_id: status.node_id.unwrap_or_default(),
                operator: None,
            })
        }
        AdminReply::FormationFailed { reason, .. } => bail!("{reason}"),
        AdminReply::Error { message } => bail!("{message}"),
        other => bail!("unexpected reply to init: {other:?}"),
    }
}

/// Write the day-0 operator credential beside the cluster (ADR 0037 §3 step 5).
///
/// Nothing in a dev cluster needs it — the client listener is plain HTTP with
/// no authentication — but it is what formation produced, and discarding it
/// would mean the one path that mints an operator identity is the one path dev
/// does not exercise end to end.
fn write_operator_material(root: &Path, operator: &OperatorPem) -> Result<()> {
    let dir = root.join("operator");
    std::fs::create_dir_all(&dir).context("creating the dev operator dir")?;
    std::fs::write(dir.join("operator.crt"), &operator.cert_pem)?;
    if let Some(key) = &operator.key_pem {
        std::fs::write(dir.join("operator.key"), key)?;
    }
    std::fs::write(dir.join("ca.crt"), &operator.ca_pem)?;
    tracing::info!(dir = %dir.display(), "wrote the dev cluster's day-0 operator credential");
    Ok(())
}

// ---------------------------------------------------------------------------
// Readiness polling over the client API
// ---------------------------------------------------------------------------

/// Wait until the client listener answers, i.e. the task runtime is serving.
///
/// Formation returns as soon as the replica is started; the API edge (and with
/// it `POST /api/v1/enroll`) comes up a moment later, as part of the runtime.
async fn wait_for_client_api(
    api: &str,
    coordinator: &mut tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{api}/api/v1/nodes");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if coordinator.is_finished() {
            return Err(daemon_error(coordinator)
                .await
                .unwrap_or_else(|| anyhow::anyhow!("the dev coordinator exited during startup")));
        }
        if client.get(&url).send().await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("the dev coordinator's client API did not come up within 60 seconds");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for the in-process agent's registration to land in applied state,
/// returning its epoch (ADR 0009).
async fn wait_for_agent(api: &str, agent_node: NodeId) -> Result<u64> {
    let client = reqwest::Client::new();
    let url = format!("{api}/api/v1/nodes/{agent_node}");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = client.get(&url).send().await {
                if response.status().is_success() {
                    if let Ok(node) = response
                        .json::<coppice_api::http::dto::GetNodeResponse>()
                        .await
                    {
                        return node.summary.epoch;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("dev agent did not register within 30 seconds")
}

/// The coordinator task's own error, when it has already finished.
async fn daemon_error(
    coordinator: &mut tokio::task::JoinHandle<Result<()>>,
) -> Option<anyhow::Error> {
    if !coordinator.is_finished() {
        return None;
    }
    match coordinator.await {
        Ok(Err(e)) => Some(e.context("the dev coordinator failed to start")),
        Ok(Ok(())) => Some(anyhow::anyhow!("the dev coordinator stopped unexpectedly")),
        Err(e) => Some(anyhow::Error::new(e).context("the dev coordinator task panicked")),
    }
}

// ---------------------------------------------------------------------------
// Agent wiring
// ---------------------------------------------------------------------------

/// Open the agent journal under the config's data dir (acquiring its `LOCK`)
/// and build the session over `executor`.
fn build_session<E: Executor + Clone>(
    config: &AgentConfig,
    executor: E,
) -> Result<Session<RealFs, E>> {
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating agent data dir {}", config.data_dir.display()))?;
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = Journal::open(fs).context("recovering the dev agent journal")?;
    Ok(Session::new(
        config.node(),
        config.advertised_resources(),
        Vec::new(),
        journal,
        state,
        executor,
    )
    // Advertise the NodeService endpoint at registration (ADR 0034) so the
    // coordinator learns where to dial for this node's job logs.
    .with_service_addr(config.service_addr()))
}

/// Bind and serve the agent-hosted NodeService (ADR 0034/0036) from the
/// config's `[listen]` + `[tls]`, so the in-process coordinator can dial it
/// for job logs and usage metrics.
///
/// `log_store`/`metric_store` are the first LOG- and METRICS-consuming
/// telemetry stores; `None` disables that stream (the fake executor captures
/// neither), in which case its fetches answer UnknownAttempt. Mirrors the
/// daemon `run` path's wiring in `lib.rs`.
fn serve_node_service(
    config: &AgentConfig,
    log_store: Option<FilesystemSink>,
    metric_store: Option<FilesystemSink>,
) -> Result<()> {
    let Some(listen) = &config.listen else {
        return Ok(());
    };
    let tls_store = coppice_agent::load_tls_store(&config.tls)?;
    let listener = coppice_agent::node_service::NodeServiceListener::bind(listen.addr, tls_store)
        .context("binding the dev NodeService listener")?;
    tracing::info!(
        service_addr = ?config.service_addr(),
        "dev NodeService listener bound; the coordinator can dial for job telemetry (ADR 0034/0036)"
    );
    coppice_agent::node_service::serve(listener, log_store, metric_store);
    Ok(())
}

/// The agent session loop as a task body (aborted at shutdown, like a
/// process kill — the journal is crash-safe by design, ADR 0009).
async fn run_agent<E: Executor + Clone>(session: Session<RealFs, E>, config: AgentConfig) {
    let tls_store = match coppice_agent::load_tls_store(&config.tls) {
        Ok(store) => store,
        Err(e) => {
            tracing::error!("dev agent session loop exited: {e:#}");
            return;
        }
    };
    if let Err(e) = session::run(session, &config, tls_store).await {
        tracing::error!("dev agent session loop exited: {e:#}");
    }
}

// ---------------------------------------------------------------------------
// The ready banner
// ---------------------------------------------------------------------------

struct ReadySummary<'a> {
    root: &'a Path,
    persistent: bool,
    cluster_id: ClusterId,
    coordinator_raft_id: u64,
    agent_node: NodeId,
    agent_epoch: u64,
    raft_port: u16,
    agent_port: u16,
    client_port: u16,
    node_service_port: u16,
    metrics_port: u16,
    ui_available: bool,
    quota_entity: QuotaEntityId,
    executor: DevExecutor,
}

fn ready_summary(summary: &ReadySummary<'_>) -> String {
    let data_lifetime = if summary.persistent {
        "persistent"
    } else {
        "temporary; deleted on exit"
    };

    format!(
        "\nCoppice dev is ready\n\
         \n\
         \x20 UI              {ui}\n\
         \x20 API             http://localhost:{client_port}/api/v1 (coppice job --api http://localhost:{client_port} …)\n\
         \x20 Raft/admin      https://localhost:{raft_port} (mTLS)\n\
         \x20 Agent gateway   https://localhost:{agent_port} (mTLS)\n\
         \x20 Node service    127.0.0.1:{node_service_port} (mTLS; agent job logs)\n\
         \x20 Metrics (coord) http://127.0.0.1:{client_port}/metrics\n\
         \x20 Metrics (agent) http://127.0.0.1:{metrics_port}/metrics\n\
         \x20 Data            {data_dir} ({data_lifetime})\n\
         \x20 Executor        {executor}\n\
         \x20 Cluster         {cluster_id} (Raft node {coordinator_raft_id})\n\
         \x20 Agent           {agent_node} (enrolled, epoch {agent_epoch})\n\
         \x20 Quota entity    {quota_entity} (\"dev\", seeded; priorities -2..=2)\n\
         \n\
         \x20 Local development only: authentication is effectively disabled.\n\
         \x20 Press Ctrl-C to stop.\n",
        ui = if summary.ui_available {
            format!("http://localhost:{}/", summary.client_port)
        } else {
            "not built (`npm --prefix web run build`, then restart)".to_string()
        },
        raft_port = summary.raft_port,
        agent_port = summary.agent_port,
        client_port = summary.client_port,
        node_service_port = summary.node_service_port,
        metrics_port = summary.metrics_port,
        data_dir = summary.root.display(),
        executor = summary.executor,
        cluster_id = summary.cluster_id,
        coordinator_raft_id = summary.coordinator_raft_id,
        agent_node = summary.agent_node,
        agent_epoch = summary.agent_epoch,
        quota_entity = summary.quota_entity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_summary_is_scannable_and_explicit_about_unavailable_surfaces() {
        let summary = ready_summary(&ReadySummary {
            root: Path::new("/tmp/coppice-dev"),
            persistent: false,
            cluster_id: "cluster-00000000-0000-0000-0000-000000000001"
                .parse()
                .expect("cluster id"),
            coordinator_raft_id: 42,
            agent_node: "node-00000000-0000-0000-0000-000000000002"
                .parse()
                .expect("node id"),
            agent_epoch: 1,
            raft_port: 7071,
            agent_port: 7072,
            client_port: 7070,
            node_service_port: 7073,
            metrics_port: 7074,
            ui_available: false,
            quota_entity: DEV_QUOTA_ENTITY.parse().expect("quota entity id"),
            executor: DevExecutor::Fake,
        });

        assert!(summary.starts_with("\nCoppice dev is ready\n\n"));
        assert!(summary.contains("UI              not built"));
        assert!(summary.contains("API             http://localhost:7070/api/v1"));
        assert!(summary.contains("Raft/admin      https://localhost:7071 (mTLS)"));
        assert!(summary.contains("Agent gateway   https://localhost:7072 (mTLS)"));
        assert!(summary.contains("Node service    127.0.0.1:7073 (mTLS; agent job logs)"));
        assert!(summary.contains("Metrics (coord) http://127.0.0.1:7070/metrics"));
        assert!(summary.contains("Metrics (agent) http://127.0.0.1:7074/metrics"));
        assert!(summary.contains("/tmp/coppice-dev (temporary; deleted on exit)"));
        assert!(summary.contains(
            "Agent           node-00000000-0000-0000-0000-000000000002 (enrolled, epoch 1)"
        ));
        assert!(summary.contains(&format!(
            "Quota entity    {DEV_QUOTA_ENTITY} (\"dev\", seeded; priorities -2..=2)"
        )));
    }

    /// The generated coordinator config must survive the daemon's own loader —
    /// it is written by `dev` and read back by `config::load`, so a drifted
    /// key or a missing required table is a startup failure, not a test one.
    #[test]
    fn the_generated_coordinator_config_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("coordinator.toml");
        std::fs::write(
            &path,
            coordinator_toml(&CoordinatorLayout {
                cluster_id: "cluster-00000000-0000-0000-0000-000000000001"
                    .parse()
                    .expect("cluster id"),
                data_dir: &dir.path().join("coordinator"),
                pki_dir: &dir.path().join("pki"),
                raft_port: 7071,
                agent_port: 7072,
                client_port: 7070,
            }),
        )
        .expect("write config");
        // Certless by construction: none of the `[tls]` files exists, and the
        // loader must accept that (formation mints them, ADR 0037 §4).
        coord_config::load(&path).expect("the dev coordinator config loads");
    }

    /// The seeding policy `dev` hands `init` must parse under the daemon's own
    /// schema, and must carry all three things a dev cluster needs.
    #[test]
    fn the_seeding_policy_parses_and_seeds_everything_dev_needs() {
        let toml = dev_policy_toml();
        let policy = coppice_coordinator::policy::FormationPolicy::parse_toml(toml.as_bytes())
            .expect("the dev policy parses");

        assert_eq!(policy.priority_multipliers.len(), 5);
        let zero = policy
            .priority_multipliers
            .iter()
            .find(|pm| pm.index == 0)
            .expect("priority 0");
        assert_eq!(zero.multiplier, 1.0);

        assert_eq!(policy.quota_entities.len(), 1);
        assert_eq!(
            policy.quota_entities[0].id.to_string(),
            DEV_QUOTA_ENTITY,
            "submit examples name this entity verbatim"
        );

        assert_eq!(policy.enroll_tokens.len(), 1);
        assert_eq!(policy.enroll_tokens[0].label, DEV_ENROLL_LABEL);
        assert_eq!(policy.enroll_tokens[0].secret, DEV_ENROLL_TOKEN);
    }

    /// An explicit port is honored and remembered; a zero port is minted once
    /// and then reused, which is what keeps a restarted dev cluster's URLs
    /// (and its advertised raft address) stable.
    #[test]
    fn ports_are_remembered_across_runs() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            resolve_port(dir.path(), "raft", 7071).expect("explicit port"),
            7071
        );
        assert_eq!(
            resolve_port(dir.path(), "raft", 0).expect("remembered port"),
            7071
        );

        let minted = resolve_port(dir.path(), "client", 0).expect("minted port");
        assert_ne!(minted, 0);
        assert_eq!(
            resolve_port(dir.path(), "client", 0).expect("remembered port"),
            minted
        );
    }
}
