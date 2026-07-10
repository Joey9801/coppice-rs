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

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use coppice_agent::config::{CapacityConfig, Config as AgentConfig, TlsConfig as AgentTls};
use coppice_agent::executor::{DockerExecutor, Executor, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::session::{self, Session};
use coppice_consensus::fs::RealFs;
use coppice_coordinator::bootstrap::{self, AgentListener, BootedCoordinator};
use coppice_coordinator::config::{self as coord_config, CliOverrides};
use coppice_core::id::{ClusterId, NodeId};
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

/// A throwaway in-memory CA and the two leaves a dev run needs. Minted fresh
/// every run: TLS material is deliberately not part of the persistent state.
struct DevPki {
    ca_pem: Vec<u8>,
    coordinator: (Vec<u8>, Vec<u8>),
    agent: (Vec<u8>, Vec<u8>),
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

    let leaf = |cn: &str| -> Result<(Vec<u8>, Vec<u8>)> {
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
        Ok((cert.pem().into_bytes(), key.serialize_pem().into_bytes()))
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
    std::fs::write(&coord_cert, &pki.coordinator.0)?;
    std::fs::write(&coord_key, &pki.coordinator.1)?;
    std::fs::write(&agent_cert, &pki.agent.0)?;
    std::fs::write(&agent_key, &pki.agent.1)?;
    std::fs::write(&ca_path, &pki.ca_pem)?;

    let raft_port = resolve_port(args.raft_port)?;
    let agent_port = resolve_port(args.agent_port)?;

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
        &pki.coordinator.0,
        &pki.coordinator.1,
        &pki.ca_pem,
    )
    .context("binding the dev agent listener")?;

    let (runtime_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_join = tokio::spawn(bootstrap::serve_runtime(
        Arc::clone(&consensus),
        views.clone(),
        event_tap,
        listener,
        Some(shutdown_rx),
    ));

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
            memory_bytes: 1 << 34,
            disk_bytes: 1 << 40,
        },
        heartbeat_interval: Duration::from_secs(2),
        reconnect_backoff_min: Duration::from_millis(100),
        reconnect_backoff_max: Duration::from_secs(2),
        labels: Default::default(),
    };
    // async-fn-in-trait futures carry no generic `Send` bound, so the spawn
    // happens per concrete executor type rather than in a generic helper.
    let agent_join = match args.executor {
        DevExecutor::Fake => {
            let session = build_session(&agent_config, FakeExecutor::new())?;
            tokio::spawn(run_agent(session, agent_config))
        }
        DevExecutor::Docker => {
            let session = build_session(&agent_config, DockerExecutor::new())?;
            tokio::spawn(run_agent(session, agent_config))
        }
    };

    // Positive confirmation the loop is closed: report when the agent's
    // registration lands in applied state (epoch >= 1, ADR 0009).
    {
        let views = views.clone();
        tokio::spawn(async move {
            loop {
                match views
                    .latest()
                    .state()
                    .nodes
                    .get(&agent_node)
                    .map(|n| n.epoch)
                {
                    Some(epoch) => {
                        tracing::info!(node = %agent_node, epoch, "dev agent registered");
                        break;
                    }
                    None => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
    }

    tracing::info!(
        data_dir = %root.display(),
        persistent = args.data_dir.is_some(),
        cluster_id = %cluster_id,
        coordinator_raft_id = handle.node_id(),
        agent_node_id = %agent_node,
        raft_admin_endpoint = %format!("localhost:{raft_port}"),
        agent_gateway_endpoint = %format!("localhost:{agent_port}"),
        executor = ?args.executor,
        "coppice dev cluster is up (throwaway per-run TLS; no authentication — \
         localhost development only); Ctrl-C to stop"
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
        config.capacity_resources(),
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
