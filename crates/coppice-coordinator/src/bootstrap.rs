//! The coordinator boot sequence and process lifecycle.
//!
//! [`run`] is the default (run-a-replica) entry point the CLI dispatches to:
//! it loads config, initializes tracing, brings the node up through
//! [`bootstrap`], runs the task runtime until a shutdown signal drains it, then
//! executes the ordered shutdown tail (coordinator-runtime.md, "Shutdown
//! order", steps 5–6). [`bootstrap`] itself is the assembly half — it runs the
//! ADR 0016 identity matrix via `coppice_consensus::start`, stands up the mTLS
//! Raft + admin server, and hands back a [`BootedCoordinator`] the integration
//! test can also drive directly.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

use coppice_consensus::{
    Applied, Consensus, ConsensusError, ConsensusStatus, CoordinatorId, EventTapReceiver,
    NodeHandle, NodeOptions, NodeTls, OpenraftConsensus, StartIntent, StartedNode, StateViews,
};
use coppice_core::id::ClusterId;
use coppice_net::admin::Server as AdminServer;
use coppice_state::Command;

use crate::admin::AdminService;
use crate::cli::RunArgs;
use crate::{config, limits};

/// A fully-assembled, running coordinator replica.
///
/// Holds the consensus seam, its view/event handles, the admin/shutdown
/// [`NodeHandle`], and the running mTLS Raft + admin server (its shutdown
/// trigger and join handle). The consensus seam is shared behind an [`Arc`] so
/// the admin service and the task runtime can both reach it.
pub struct BootedCoordinator {
    /// The cluster this replica belongs to (its config's `cluster_id`, ADR
    /// 0020/0024). Carried out of bootstrap because the config itself is
    /// crate-private, and the task runtime's API edge reports it
    /// (`GET /api/v1/overview`).
    pub cluster_id: ClusterId,
    /// The consensus seam, shared with the mounted admin service.
    pub consensus: Arc<OpenraftConsensus>,
    /// Published read views of applied state.
    pub views: StateViews,
    /// The derived event stream (ADR 0008).
    pub event_tap: EventTapReceiver,
    /// Admin/shutdown handle (shutdown step 5).
    pub handle: NodeHandle,
    /// Fires the raft/admin server's graceful shutdown.
    pub raft_server_shutdown: oneshot::Sender<()>,
    /// The raft/admin server task; join it after triggering shutdown.
    pub raft_server: JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Run a coordinator replica end to end: load, boot, serve, shut down.
pub async fn run(args: RunArgs) -> Result<()> {
    // Config load happens before tracing init: a config error rides out as an
    // `anyhow` error and `main` prints it to stderr even though no subscriber
    // is installed yet.
    let resolved = config::load(
        &args.config,
        config::CliOverrides {
            bootstrap: args.bootstrap,
            join: args.join,
        },
    )
    .with_context(|| format!("loading coordinator config {}", args.config.display()))?;

    init_tracing(&resolved.config.observability)?;
    resolved.log_effective();

    tracing::info!("coppice-coordinator starting");

    // Bind the agent gateway listener early (fail-fast on a port conflict),
    // before consensus starts. Only the daemon path binds it — the integration
    // test drives `bootstrap` directly and runs several replicas in one
    // process, so binding a shared default agent port there would collide.
    let agent_listener = prepare_agent_listener(&resolved.config)?;
    let client_listener = ClientListener::bind(resolved.config.listen.client_addr).await?;

    let BootedCoordinator {
        cluster_id,
        consensus,
        views,
        event_tap,
        handle,
        raft_server_shutdown,
        raft_server,
    } = bootstrap(resolved).await?;

    // The task runtime owns steps 1–4 of the shutdown order and returns once
    // its own signal-driven shutdown has fully drained (`None`: the daemon path
    // lets the runtime install its own signal handler).
    serve_runtime(
        Arc::clone(&consensus),
        views,
        event_tap,
        handle.clone(),
        agent_listener,
        client_listener,
        cluster_id,
        None,
    )
    .await?;

    // Shutdown tail (coordinator-runtime.md steps 5–6), in dependency order.
    tracing::info!("shutdown: stopping raft/admin transport (no new peer traffic)");
    let _ = raft_server_shutdown.send(());
    match raft_server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "shutdown: raft/admin server ended with an error"),
        Err(e) => {
            tracing::warn!(error = %e, "shutdown: raft/admin server task did not join cleanly")
        }
    }

    tracing::info!("shutdown: transport down; shutting down consensus (apply task drains)");
    handle
        .shutdown()
        .await
        .context("shutting down the consensus node")?;

    tracing::info!(
        "shutdown: consensus down; releasing remaining handles (storage flushes on drop)"
    );
    drop(consensus);
    tracing::info!("shutdown complete");
    Ok(())
}

/// Run the coordinator's agent-facing task runtime over the shared consensus
/// seam.
///
/// `runtime::run` takes ownership of a `Consensus`; the wrapper delegates to
/// the shared [`Arc`] so the admin service keeps its own reference. `shutdown`
/// selects the stop mechanism: `None` lets the runtime install its own
/// signal handler (the daemon path); `Some(rx)` hands it a caller-owned trigger
/// so an integration test can drive [`bootstrap`] and this runtime directly and
/// shut them down without raising a real signal.
#[allow(clippy::too_many_arguments)] // thin wiring seam over `runtime::run`
pub async fn serve_runtime(
    consensus: Arc<OpenraftConsensus>,
    views: StateViews,
    event_tap: EventTapReceiver,
    node_handle: NodeHandle,
    agent_listener: AgentListener,
    client_listener: ClientListener,
    cluster_id: ClusterId,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<()> {
    crate::runtime::run(
        SharedConsensus(consensus),
        views,
        event_tap,
        node_handle,
        agent_listener,
        client_listener,
        cluster_id,
        shutdown,
    )
    .await
}

/// The bound public client-API listener (`listen.client_addr`, ADR 0031),
/// handed to `runtime::run` which serves `coppice_api::http` on it.
///
/// Bound eagerly (fail-fast on a port conflict) like [`AgentListener`].
/// Plain HTTP: unlike the fenced mTLS planes, this edge serves browsers
/// and CLIs — TLS termination here or in front of it is deployment
/// posture (the ADR's "config, not contract"), and authn is the bearer
/// token contract of ADR 0022, not transport identity.
pub struct ClientListener {
    listener: tokio::net::TcpListener,
}

impl ClientListener {
    /// Bind the client API listener on `addr`.
    pub async fn bind(addr: SocketAddr) -> Result<ClientListener> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow!("binding client API listener on {addr}: {e}"))?;
        tracing::info!(%addr, "client API listener bound");
        Ok(ClientListener { listener })
    }

    /// The actual bound address (which resolves a `:0` request).
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub(crate) fn into_inner(self) -> tokio::net::TcpListener {
        self.listener
    }
}

/// The bound agent gateway listener and its mTLS config, handed to
/// `runtime::run` which starts the tonic server after creating the session
/// channels.
///
/// Bound eagerly in [`run`] (fail-fast) but served inside the runtime so the
/// listener stops accepting first on shutdown, alongside the API server.
pub struct AgentListener {
    pub(crate) incoming: TcpIncoming,
    pub(crate) tls: ServerTlsConfig,
}

impl AgentListener {
    /// Bind the agent gateway's dedicated mTLS listener on `addr` from PEM
    /// material already in memory (ADR 0009/0011).
    ///
    /// The same cert/key/ca as the Raft/admin server; client certs are REQUIRED
    /// (`client_auth_optional(false)`) so the gateway can bind the agent's leaf
    /// CN to its NodeId at session accept. The daemon path reaches this through
    /// [`prepare_agent_listener`], which reads the PEM from the config's
    /// `[tls]` paths; the integration test calls it directly on a free port so
    /// several replicas can run in one process without colliding on the default
    /// agent port.
    pub fn bind(
        addr: SocketAddr,
        cert_pem: &[u8],
        key_pem: &[u8],
        ca_pem: &[u8],
    ) -> Result<AgentListener> {
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .client_ca_root(Certificate::from_pem(ca_pem))
            .client_auth_optional(false);

        let incoming = TcpIncoming::new(addr, true, None)
            .map_err(|e| anyhow!("binding agent gateway listener on {addr}: {e}"))?;
        tracing::info!(%addr, "agent gateway mTLS listener bound");

        Ok(AgentListener { incoming, tls })
    }
}

/// Bind the agent gateway's dedicated mTLS listener from the config's `[tls]`
/// paths (ADR 0009/0011).
///
/// Reads the same cert/key/ca the Raft/admin server uses and hands them to
/// [`AgentListener::bind`], naming each path on a read failure.
fn prepare_agent_listener(cfg: &config::Config) -> Result<AgentListener> {
    let cert = std::fs::read(&cfg.tls.cert_path)
        .with_context(|| format!("reading TLS certificate {}", cfg.tls.cert_path.display()))?;
    let key = std::fs::read(&cfg.tls.key_path)
        .with_context(|| format!("reading TLS private key {}", cfg.tls.key_path.display()))?;
    let ca = std::fs::read(&cfg.tls.ca_path)
        .with_context(|| format!("reading TLS CA certificate {}", cfg.tls.ca_path.display()))?;

    AgentListener::bind(cfg.listen.agent_addr, &cert, &key, &ca)
}

/// Assemble and start a coordinator replica (does not run the task runtime).
///
/// Each step fails with operator-actionable context. On success the returned
/// [`BootedCoordinator`] owns a live consensus replica and a running mTLS
/// server; the caller is responsible for the shutdown tail.
pub async fn bootstrap(resolved: config::ResolvedConfig) -> Result<BootedCoordinator> {
    let cfg = &resolved.config;

    // Step 2: the data directory. Creating an empty dir is safe — the ADR 0016
    // manifest check governs identity, and an unmounted volume surfaces here as
    // the empty-dir fail-stop inside `start`.
    std::fs::create_dir_all(&cfg.data_dir).with_context(|| {
        format!(
            "creating coordinator data directory {}",
            cfg.data_dir.display()
        )
    })?;

    // Step 3: the mTLS PEM material (ADR 0011), each path named on failure.
    let cert = std::fs::read(&cfg.tls.cert_path)
        .with_context(|| format!("reading TLS certificate {}", cfg.tls.cert_path.display()))?;
    let key = std::fs::read(&cfg.tls.key_path)
        .with_context(|| format!("reading TLS private key {}", cfg.tls.key_path.display()))?;
    let ca = std::fs::read(&cfg.tls.ca_path)
        .with_context(|| format!("reading TLS CA certificate {}", cfg.tls.ca_path.display()))?;

    let cluster_uuid = *cfg.cluster_id.0.as_bytes();
    let raft_addr = cfg.listen.raft_addr;

    // Step 4: node options from config. No node id: the replica's identity
    // is minted at init and read from the manifest stamp thereafter (ADR 0025).
    let options = NodeOptions {
        cluster_uuid,
        data_dir: cfg.data_dir.clone(),
        advertise_addr: cfg.listen.advertised_raft_addr(),
        election_timeout: cfg.raft.election_timeout,
        heartbeat_interval: cfg.raft.heartbeat_interval,
        rpc_timeout: cfg.raft.rpc_timeout,
        snapshot_log_entries: cfg.raft.snapshot_log_entries,
        snapshot_keep_log_entries: cfg.raft.snapshot_keep_log_entries,
        event_tap_capacity: limits::EVENT_TAP_CAPACITY,
        tls: NodeTls {
            ca_pem: ca.clone(),
            cert_pem: cert.clone(),
            key_pem: key.clone(),
        },
    };

    // Step 5: intent (the ADR 0016 matrix is enforced inside `start`).
    let intent = if resolved.bootstrap {
        StartIntent::Bootstrap
    } else if resolved.join {
        StartIntent::Join
    } else {
        StartIntent::Restart
    };

    let StartedNode {
        consensus,
        views,
        event_tap,
        handle,
        transport,
    } = coppice_consensus::start(options, intent)
        .await
        .context("starting consensus replica")?;

    // Surfaced on every start (not just at mint) so an operator can always
    // read the id off the newest log lines, e.g. for `admin add-learner`.
    tracing::info!(node_id = handle.node_id(), "coordinator raft identity");

    let consensus = Arc::new(consensus);

    // Step 6: the mTLS server carrying both the Raft transport and the admin
    // surface. Client certs are REQUIRED — `client_ca_root` sets the trust
    // root and `client_auth_optional(false)` makes presenting a client cert
    // mandatory (ADR 0011: no unauthenticated peer or admin traffic).
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(&cert, &key))
        .client_ca_root(Certificate::from_pem(&ca))
        .client_auth_optional(false);

    let admin = AdminServer::new(AdminService::new(
        Arc::clone(&consensus),
        handle.clone(),
        cluster_uuid,
    ));

    // Bind now so a failure names the address at bootstrap rather than surfacing
    // only when the server task is later awaited.
    let incoming = TcpIncoming::new(raft_addr, true, None)
        .map_err(|e| anyhow!("binding raft/admin listener on {raft_addr}: {e}"))?;

    let router = Server::builder()
        .tls_config(tls)
        .context("configuring the raft/admin server TLS")?
        .add_service(transport)
        .add_service(admin);

    let (raft_server_shutdown, shutdown_rx) = oneshot::channel::<()>();
    let raft_server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    tracing::info!(addr = %raft_addr, "raft/admin mTLS listener bound");

    Ok(BootedCoordinator {
        cluster_id: cfg.cluster_id,
        consensus,
        views,
        event_tap,
        handle,
        raft_server_shutdown,
        raft_server,
    })
}

/// Install the global tracing subscriber from the observability config.
///
/// `log_level` feeds an `EnvFilter`; `log_format` selects the text or JSON
/// event layout. Kept out of `bootstrap` so a config error can still reach
/// stderr before any subscriber exists.
fn init_tracing(obs: &config::ObservabilityConfig) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_new(&obs.log_level)
        .with_context(|| format!("invalid log_level {:?}", obs.log_level))?;

    match obs.log_format.as_str() {
        "text" => tracing_subscriber::fmt().with_env_filter(filter).init(),
        "json" => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init(),
        other => bail!("unknown log_format {other:?}; expected \"text\" or \"json\""),
    }
    Ok(())
}

/// Shares one [`OpenraftConsensus`] between the task runtime and the mounted
/// admin service.
///
/// `runtime::run` consumes a `Consensus` by value; this newtype lets it own a
/// handle to the same seam the admin service holds, delegating every trait
/// method to the shared [`Arc`].
struct SharedConsensus(Arc<OpenraftConsensus>);

impl Consensus for SharedConsensus {
    fn propose(
        &self,
        command: Command,
    ) -> impl Future<Output = Result<Applied, ConsensusError>> + Send {
        self.0.propose(command)
    }

    fn read_index(&self) -> impl Future<Output = Result<u64, ConsensusError>> + Send {
        self.0.read_index()
    }

    fn status(&self) -> watch::Receiver<ConsensusStatus> {
        self.0.status()
    }

    fn views(&self) -> StateViews {
        self.0.views()
    }

    fn add_learner(
        &self,
        node: CoordinatorId,
        addr: String,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.add_learner(node, addr)
    }

    fn promote_voter(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.promote_voter(promote, remove)
    }

    fn remove_node(
        &self,
        node: CoordinatorId,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.remove_node(node)
    }

    fn trigger_snapshot(&self) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.trigger_snapshot()
    }
}
