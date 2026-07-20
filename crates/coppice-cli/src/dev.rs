//! `coppice dev`: a self-contained single-node cluster for development and
//! integration testing.
//!
//! One process runs a bootstrap-intent coordinator (consensus, scheduler,
//! agent gateway — the full task runtime) plus an in-process agent session
//! dialing it over localhost. mTLS stays structurally intact — the wire
//! paths are the production ones — but the CA and both leaves are minted
//! fresh in memory on every run and trusted only by this process, so there
//! is no key material to provision and **no authentication in any
//! meaningful sense: anything that can reach the ports is effectively
//! admin**. Never expose a dev instance beyond localhost.
//!
//! The data directory defaults to a temp dir deleted on exit; pass
//! `--data-dir` to keep state across runs (the coordinator restarts from its
//! manifest stamp and the agent keeps its journal and node identity).

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use coppice_agent::config::{
    CapacityConfig, Config as AgentConfig, ExecutorConfig, TlsConfig as AgentTls,
};
use coppice_agent::executor::{DockerExecutor, Executor, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::session::{self, Session};
use coppice_consensus::fs::RealFs;
use coppice_consensus::{Consensus, ConsensusError};
use coppice_coordinator::bootstrap::{self, AgentListener, BootedCoordinator, ClientListener};
use coppice_coordinator::config::{self as coord_config, CliOverrides};
use coppice_core::bytes::ByteSize;
use coppice_core::id::{ClusterId, NodeId, QuotaEntityId};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, UpdatePolicy};
use coppice_state::Command;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

#[derive(Debug, clap::Args)]
pub struct DevArgs {
    /// Data directory. Defaults to a fresh temp dir deleted on exit; pass a
    /// path to keep cluster and agent state across runs.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Client API port (0 picks a free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    client_port: u16,

    /// Agent-gateway port (0 picks a free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    agent_port: u16,

    /// Raft/admin port (0 picks a free one; logged at startup).
    #[arg(long, default_value_t = 0)]
    raft_port: u16,

    /// Executor backing the in-process agent. `fake` runs the lifecycle
    /// without containers; `docker` is the production executor (not yet
    /// implemented — every start fails).
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

/// A minted leaf credential: the certificate and its private key, as PEM bytes.
#[derive(Debug, Clone)]
struct CertKey {
    /// The X.509 leaf certificate, PEM-encoded.
    cert: Vec<u8>,
    /// The corresponding private key, PEM-encoded.
    key: Vec<u8>,
}

/// A throwaway in-memory CA and the two leaves a dev run needs. Minted fresh
/// every run: TLS material is deliberately not part of the persistent state.
struct DevPki {
    ca_pem: Vec<u8>,
    coordinator: CertKey,
    agent: CertKey,
}

fn mint_pki(agent_node: NodeId) -> Result<DevPki> {
    let ca_key = KeyPair::generate().context("generate dev CA key")?;
    let mut params = CertificateParams::new(Vec::<String>::new()).context("dev CA params")?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "coppice-dev-ca");
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = params.self_signed(&ca_key).context("self-sign dev CA")?;

    let leaf = |cn: &str| -> Result<CertKey> {
        let key = KeyPair::generate().context("generate dev leaf key")?;
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .context("dev leaf params")?;
        params.distinguished_name.push(DnType::CommonName, cn);
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        let cert = params
            .signed_by(&key, &ca_cert, &ca_key)
            .context("sign dev leaf")?;
        Ok(CertKey {
            cert: cert.pem().into_bytes(),
            key: key.serialize_pem().into_bytes(),
        })
    };

    Ok(DevPki {
        ca_pem: ca_cert.pem().into_bytes(),
        coordinator: leaf("coppice-dev-coordinator")?,
        // The gateway binds the client leaf's CN to the claimed NodeId
        // (ADR 0011), so the agent leaf carries the typed id string.
        agent: leaf(&agent_node.to_string())?,
    })
}

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

fn resolve_port(requested: u16) -> Result<u16> {
    if requested != 0 {
        return Ok(requested);
    }
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr().context("local addr")?.port())
}

pub async fn run(args: DevArgs) -> Result<()> {
    // -- Layout: everything under one root. --------------------------------
    let (root, _tempdir) = match &args.data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating data dir {}", dir.display()))?;
            (dir.clone(), None)
        }
        None => {
            let dir = tempfile::tempdir().context("creating temp data dir")?;
            (dir.path().to_path_buf(), Some(dir))
        }
    };

    // Persistent identities: the cluster id must keep matching the manifest
    // stamp across restarts, and the agent node id must keep matching its
    // journal history. Both live as typed-string files under the root.
    let cluster_id: ClusterId = load_or_mint(&root.join("cluster-id"), ClusterId::new)?;
    let agent_node: NodeId = load_or_mint(&root.join("agent-node-id"), NodeId::new)?;

    let pki = mint_pki(agent_node)?;
    let pki_dir = root.join("pki");
    std::fs::create_dir_all(&pki_dir).context("creating pki dir")?;
    let coord_cert = pki_dir.join("coordinator.crt");
    let coord_key = pki_dir.join("coordinator.key");
    let agent_cert = pki_dir.join("agent.crt");
    let agent_key = pki_dir.join("agent.key");
    let ca_path = pki_dir.join("ca.crt");
    std::fs::write(&coord_cert, &pki.coordinator.cert)?;
    std::fs::write(&coord_key, &pki.coordinator.key)?;
    std::fs::write(&agent_cert, &pki.agent.cert)?;
    std::fs::write(&agent_key, &pki.agent.key)?;
    std::fs::write(&ca_path, &pki.ca_pem)?;

    let raft_port = resolve_port(args.raft_port)?;
    let agent_port = resolve_port(args.agent_port)?;
    let client_port = resolve_port(args.client_port)?;

    // -- Coordinator: the production config + bootstrap path. --------------
    let coord_data = root.join("coordinator");
    let bootstrap_intent = !coord_data.join("manifest").exists();
    let config_path = root.join("coordinator.toml");
    let toml = format!(
        r#"# Generated by `coppice dev` on every run; edits are overwritten.
cluster_id = "{cluster_id}"
data_dir = "{data_dir}"
peers = []

[listen]
raft_addr = "127.0.0.1:{raft_port}"
advertise_host = "localhost"

[raft]
# Snappy dev timings: single node, localhost, no real elections to lose.
election_timeout = "300ms"
heartbeat_interval = "100ms"
rpc_timeout = "2s"

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"
"#,
        data_dir = coord_data.display(),
        cert = coord_cert.display(),
        key = coord_key.display(),
        ca = ca_path.display(),
    );
    std::fs::write(&config_path, toml).context("writing dev coordinator config")?;

    let resolved = coord_config::load(
        &config_path,
        CliOverrides {
            bootstrap: bootstrap_intent,
            join: false,
        },
    )
    .context("loading dev coordinator config")?;

    let BootedCoordinator {
        // The same id `dev` minted (or loaded) above and wrote into the config
        // bootstrap just read back.
        cluster_id: _,
        consensus,
        views,
        event_tap,
        handle,
        raft_server_shutdown,
        raft_server,
    } = bootstrap::bootstrap(resolved)
        .await
        .context("bootstrapping the dev coordinator")?;

    let agent_addr = format!("127.0.0.1:{agent_port}")
        .parse()
        .expect("agent socket addr");
    let listener = AgentListener::bind(
        agent_addr,
        &pki.coordinator.cert,
        &pki.coordinator.key,
        &pki.ca_pem,
    )
    .context("binding the dev agent listener")?;

    let client_addr = format!("127.0.0.1:{client_port}")
        .parse()
        .expect("client socket addr");
    let client_listener = ClientListener::bind(client_addr)
        .await
        .context("binding the dev client API listener")?;

    let (runtime_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_join = tokio::spawn(bootstrap::serve_runtime(
        Arc::clone(&consensus),
        views.clone(),
        event_tap,
        handle.clone(),
        listener,
        client_listener,
        cluster_id,
        Some(shutdown_rx),
    ));

    // Replicated state a dev cluster needs before it can accept a job: a
    // priority-multiplier table (empty on a fresh cluster by design) and a
    // quota entity to charge jobs to.
    let quota_entity = seed_dev_state(consensus.as_ref(), &views).await?;

    // -- Agent: in-process, dialing the gateway over localhost. ------------
    let agent_config = AgentConfig {
        node_id: agent_node,
        data_dir: root.join("agent"),
        coordinators: vec![format!("localhost:{agent_port}")],
        tls: AgentTls {
            cert_path: agent_cert,
            key_path: agent_key,
            ca_path,
        },
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
    };
    // async-fn-in-trait futures carry no generic `Send` bound, so the spawn
    // happens per concrete executor type rather than in a generic helper. The
    // second tuple element holds the telemetry handle (Docker executor only)
    // alive for the dev cluster's lifetime, so its retention janitors are not
    // dropped early (§8.4).
    let (agent_join, _telemetry_guard) = match args.executor {
        DevExecutor::Fake => {
            let session = build_session(&agent_config, FakeExecutor::new())?;
            (tokio::spawn(run_agent(session, agent_config)), None)
        }
        DevExecutor::Docker => {
            // Mirror `run_daemon`'s wiring: connect the daemon, spawn the shared
            // disk-pressure monitor over data_dir + the data-root, then build the
            // executor (docker-executor.md §9, §11).
            let docker =
                coppice_agent::executor::docker::api::connect(&agent_config.executor.docker_host)?;
            let data_root = coppice_agent::executor::docker::api::data_root(
                &docker,
                &agent_config.executor.docker_host,
            )
            .await?;
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
                agent_config.capacity.cpu_millis,
                agent_config.reservation.cpu_millis,
                agent_config.node(),
                pressure_rx,
                cache_options,
                telemetry_wiring,
            )
            .await?;
            let session = build_session(&agent_config, executor)?;
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
    let agent_epoch = wait_for_agent(&views, agent_node).await?;
    tracing::debug!(node = %agent_node, epoch = agent_epoch, "dev agent registered");

    eprintln!(
        "{}",
        ready_summary(&ReadySummary {
            root: &root,
            persistent: args.data_dir.is_some(),
            cluster_id,
            coordinator_raft_id: handle.node_id(),
            agent_node,
            agent_epoch,
            raft_port,
            agent_port,
            client_port,
            ui_available: coppice_api::http::ui_available(),
            quota_entity,
            executor: args.executor,
        })
    );

    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;
    tracing::info!("shutting down the dev cluster");

    // Ordered teardown mirroring the daemon shutdown tail: agent session,
    // task runtime, raft/admin transport, consensus.
    agent_join.abort();
    let _ = agent_join.await;
    let _ = runtime_shutdown.send(true);
    let _ = runtime_join.await;
    let _ = raft_server_shutdown.send(());
    let _ = raft_server.await;
    let _ = handle.shutdown().await;
    drop(consensus);

    Ok(())
}

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
    ))
}

/// The agent session loop as a task body (aborted at shutdown, like a
/// process kill — the journal is crash-safe by design, ADR 0009).
async fn run_agent<E: Executor + Clone>(session: Session<RealFs, E>, config: AgentConfig) {
    if let Err(e) = session::run(session, &config).await {
        tracing::error!("dev agent session loop exited: {e:#}");
    }
}

/// The well-known quota entity dev jobs charge to. Fixed rather than minted
/// so submit examples keep working verbatim across dev clusters.
const DEV_QUOTA_ENTITY: &str = "quota-00000000-0000-0000-0000-000000000001";

/// Dev priorities `-2..=2` mapped to cost multipliers 0.25×..4× (doubling
/// per step — monotone in priority, as ADR 0021's ranking expects).
fn dev_priority_table() -> BTreeMap<i32, PriorityMultiplier> {
    (-2i32..=2)
        .map(|p| (p, PriorityMultiplier(1u64 << (32 + p))))
        .collect()
}

/// Seed the replicated state a fresh dev cluster needs to accept a job.
///
/// A new cluster's policy has an **empty** priority-multiplier table, so
/// every `SubmitJob` fails synchronous validation until an `UpdatePolicy`
/// commits. In production that is deliberate: policy is replicated state an
/// operator configures explicitly through the admin tooling, and the node
/// config file never seeds it (ADR 0020). Dev has no operator, so propose
/// the same commands the tooling will use: multipliers for priorities
/// `-2..=2` and the well-known "dev" quota entity. Each seed is skipped
/// when already present, so policy or quota edits made against a persistent
/// `--data-dir` survive restarts.
async fn seed_dev_state<C: Consensus>(
    consensus: &C,
    views: &coppice_consensus::StateViews,
) -> Result<QuotaEntityId> {
    let entity: QuotaEntityId = DEV_QUOTA_ENTITY.parse().expect("dev quota entity id");
    let view = views.latest();
    let state = view.state();

    if state.policy.priority_multipliers.is_empty() {
        // UpdatePolicy is a full replacement: change only the table, keep
        // the booted defaults for everything else.
        let mut policy = state.policy.clone();
        policy.priority_multipliers = dev_priority_table();
        propose_seed(
            consensus,
            Command::UpdatePolicy(UpdatePolicy {
                policy,
                updated_at: Timestamp::now(),
            }),
            "seeding the dev priority-multiplier table",
        )
        .await?;
    }

    if !state.quota_entities.contains_key(&entity) {
        propose_seed(
            consensus,
            Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
                entity,
                parent: None,
                name: "dev".to_string(),
                // ~1e6 CU: deep enough that dev jobs never starve on quota,
                // far enough from u64::MAX to stay clear of saturation.
                quota: CostUnits(1_000_000_000_000),
                updated_at: Timestamp::now(),
            }),
            "seeding the dev quota entity",
        )
        .await?;
    }

    Ok(entity)
}

/// Propose one seed command, riding out the single node's initial election
/// (`NotLeader`/`Timeout` right after bootstrap) for up to 10 seconds.
async fn propose_seed<C: Consensus>(consensus: &C, command: Command, what: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match consensus.propose(command.clone()).await {
            Ok(applied) => {
                applied
                    .outcome
                    .with_context(|| format!("{what}: rejected at apply"))?;
                return Ok(());
            }
            Err(e @ (ConsensusError::NotLeader { .. } | ConsensusError::Timeout)) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e).context(what.to_string());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e).context(what.to_string()),
        }
    }
}

async fn wait_for_agent(views: &coppice_consensus::StateViews, agent_node: NodeId) -> Result<u64> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(epoch) = views
                .latest()
                .state()
                .nodes
                .get(&agent_node)
                .map(|node| node.epoch)
            {
                return epoch;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("dev agent did not register within 10 seconds")
}

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
         \x20 API             http://localhost:{client_port}/api/v1 (most reads still 501 UNIMPLEMENTED)\n\
         \x20 Raft/admin      https://localhost:{raft_port} (mTLS)\n\
         \x20 Agent gateway   https://localhost:{agent_port} (mTLS)\n\
         \x20 Data            {data_dir} ({data_lifetime})\n\
         \x20 Executor        {executor}\n\
         \x20 Cluster         {cluster_id} (Raft node {coordinator_raft_id})\n\
         \x20 Agent           {agent_node} (registered, epoch {agent_epoch})\n\
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
            ui_available: false,
            quota_entity: DEV_QUOTA_ENTITY.parse().expect("quota entity id"),
            executor: DevExecutor::Fake,
        });

        assert!(summary.starts_with("\nCoppice dev is ready\n\n"));
        assert!(summary.contains("UI              not built"));
        assert!(summary.contains("API             http://localhost:7070/api/v1"));
        assert!(summary.contains("Raft/admin      https://localhost:7071 (mTLS)"));
        assert!(summary.contains("Agent gateway   https://localhost:7072 (mTLS)"));
        assert!(summary.contains("/tmp/coppice-dev (temporary; deleted on exit)"));
        assert!(summary.contains(
            "Agent           node-00000000-0000-0000-0000-000000000002 (registered, epoch 1)"
        ));
        assert!(summary.contains(&format!(
            "Quota entity    {DEV_QUOTA_ENTITY} (\"dev\", seeded; priorities -2..=2)"
        )));
    }
}
