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
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tonic::transport::Server;

use coppice_consensus::{
    Applied, Consensus, ConsensusError, ConsensusStatus, CoordinatorId, EventTapReceiver,
    NodeHandle, NodeOptions, OpenraftConsensus, StartIntent, StartedNode, StateViews,
};
use coppice_core::id::ClusterId;
use coppice_net::admin::Server as AdminServer;
use coppice_state::Command;
use coppice_tls::{TlsPaths, TlsStore};

use crate::admin::AdminService;
use crate::cli::RunArgs;
use crate::discovery::FileRegistration;
use crate::tasks::node_client::NodeClient;
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
    /// Dials agents' `NodeService` listeners for job-log retrieval (ADR 0034),
    /// built from the same mTLS material as the raft mesh. Handed to the task
    /// runtime, which attaches it to the API control plane.
    pub node_log_client: Arc<NodeClient>,
    /// Fires the raft/admin server's graceful shutdown.
    pub raft_server_shutdown: oneshot::Sender<()>,
    /// The raft/admin server task; join it after triggering shutdown.
    pub raft_server: JoinHandle<Result<(), tonic::transport::Error>>,
    /// This process's registration in the `file` discovery directory
    /// (ADR 0037 §2), when that backend is configured. Removed explicitly at
    /// graceful shutdown; `Drop` is the best-effort backstop.
    pub file_registration: Option<FileRegistration>,
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

    // Install the process-wide Prometheus recorder here (issue #46), BEFORE
    // `bootstrap` starts consensus: consensus emits counters/gauges/histograms
    // from its first apply, so the recorder must already exist or those startup
    // metrics land in no recorder and are lost. This also builds the `/metrics`
    // endpoint the API server hosts on the client listener. The daemon owns this
    // process, so it owns the once-per-process recorder install (`runtime::run`
    // itself no longer touches the global slot); a lost race fails startup.
    let metrics = coppice_api::http::MetricsEndpoint::new(
        crate::install_metrics_recorder()?,
        crate::gather_metrics,
    );

    // Load the hot-reload mTLS store up front (fail-fast on a missing or
    // unparseable cert/key/CA — a coordinator with no valid material must not
    // start, ADR 0011) and drive reloads from an mtime poll plus SIGHUP. The
    // store is shared by both mTLS listeners, the raft peer mesh, and the
    // node-fetch client, so one rotation on disk re-arms them all (ADR 0037 §4).
    // Only the daemon path installs SIGHUP, mirroring how `runtime` gates its
    // own signal handler; the integration test drives `bootstrap` directly.
    let tls_store = load_tls_store(&resolved.config)?;
    let _tls_reload = coppice_tls::spawn_reload_task(
        Arc::clone(&tls_store),
        coppice_tls::ReloadOptions {
            sighup: true,
            ..Default::default()
        },
    );

    // Bind the agent gateway listener early (fail-fast on a port conflict),
    // before consensus starts. Only the daemon path binds it — the integration
    // test drives `bootstrap` directly and runs several replicas in one
    // process, so binding a shared default agent port there would collide.
    let agent_listener =
        AgentListener::bind(resolved.config.listen.agent_addr, Arc::clone(&tls_store))?;
    let client_listener = ClientListener::bind(resolved.config.listen.client_addr).await?;

    let BootedCoordinator {
        cluster_id,
        consensus,
        views,
        event_tap,
        handle,
        node_log_client,
        raft_server_shutdown,
        raft_server,
        file_registration,
    } = bootstrap(resolved, tls_store).await?;

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
        node_log_client,
        metrics,
        None,
    )
    .await?;

    // Shutdown tail (coordinator-runtime.md steps 5–6), in dependency order.
    // Remove this process's file-discovery registration first, while the
    // process can still do so gracefully (a leftover file is tolerated but
    // costs peers a failed dial, ADR 0037 §2).
    if let Some(registration) = file_registration {
        registration.remove().await;
    }

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
/// the shared [`Arc`] so the admin service keeps its own reference. `metrics`
/// is the client-listener `/metrics` endpoint the caller built over a recorder
/// it installed with [`crate::install_metrics_recorder`] (issue #46) — passed
/// through so `coppice dev` can hand its co-hosted coordinator and agent one
/// shared recorder. `shutdown` selects the stop mechanism: `None` lets the
/// runtime install its own signal handler (the daemon path); `Some(rx)` hands
/// it a caller-owned trigger so an integration test can drive [`bootstrap`] and
/// this runtime directly and shut them down without raising a real signal.
#[allow(clippy::too_many_arguments)] // thin wiring seam over `runtime::run`
pub async fn serve_runtime(
    consensus: Arc<OpenraftConsensus>,
    views: StateViews,
    event_tap: EventTapReceiver,
    node_handle: NodeHandle,
    agent_listener: AgentListener,
    client_listener: ClientListener,
    cluster_id: ClusterId,
    node_log_client: Arc<NodeClient>,
    metrics: coppice_api::http::MetricsEndpoint,
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
        node_log_client,
        metrics,
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

/// The bound agent gateway listener and its hot-reload TLS store, handed to
/// `runtime::run` which stands up the mTLS server after creating the session
/// channels.
///
/// Bound eagerly in [`run`] (fail-fast) but served inside the runtime so the
/// listener stops accepting first on shutdown, alongside the API server. Holds
/// the raw std listener (bound synchronously, so a port conflict fails fast) and
/// the reload store: the runtime converts the listener to tokio and runs the
/// connection-time acceptor from [`coppice_tls::serve`], so a rotated leaf is
/// served to new agent sessions without a restart (ADR 0037 §4).
pub struct AgentListener {
    pub(crate) listener: std::net::TcpListener,
    pub(crate) tls: Arc<TlsStore>,
}

impl AgentListener {
    /// Bind the agent gateway's dedicated mTLS listener on `addr`, resolving its
    /// server certificate from `tls` at each handshake (ADR 0009/0011/0037).
    ///
    /// Client certs stay REQUIRED (the store's server config is built with a
    /// mandatory client-cert verifier) so the gateway can bind the agent's leaf
    /// CN to its NodeId at session accept. The integration test and `coppice
    /// dev` call this directly with their own store on a free port so several
    /// listeners can coexist in one process.
    pub fn bind(addr: SocketAddr, tls: Arc<TlsStore>) -> Result<AgentListener> {
        let listener = std::net::TcpListener::bind(addr)
            .map_err(|e| anyhow!("binding agent gateway listener on {addr}: {e}"))?;
        // Non-blocking so `runtime` can adopt it as a tokio listener.
        listener
            .set_nonblocking(true)
            .map_err(|e| anyhow!("setting agent gateway listener non-blocking: {e}"))?;
        tracing::info!(%addr, "agent gateway mTLS listener bound");

        Ok(AgentListener { listener, tls })
    }
}

/// Load the shared hot-reload TLS store from the config's `[tls]` paths
/// (ADR 0011/0037 §4). Fails fast, naming the offending path, if any file is
/// missing or unparseable.
fn load_tls_store(cfg: &config::Config) -> Result<Arc<TlsStore>> {
    let paths = TlsPaths {
        cert: cfg.tls.cert_path.clone(),
        key: cfg.tls.key_path.clone(),
        ca: cfg.tls.ca_path.clone(),
    };
    TlsStore::load(paths).context("loading coordinator TLS material (config [tls])")
}

/// Assemble and start a coordinator replica (does not run the task runtime).
///
/// Each step fails with operator-actionable context. On success the returned
/// [`BootedCoordinator`] owns a live consensus replica and a running mTLS
/// server; the caller is responsible for the shutdown tail.
pub async fn bootstrap(
    resolved: config::ResolvedConfig,
    tls_store: Arc<TlsStore>,
) -> Result<BootedCoordinator> {
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

    let cluster_uuid = *cfg.cluster_id.0.as_bytes();
    let raft_addr = cfg.listen.raft_addr;

    // Step 3: bind the raft/admin listener FIRST, before anything publishes an
    // address (ADR 0037 §2). A `raft_addr` with port 0 (the multi-process dev
    // case) must resolve to its real bound port here so the *same* concrete
    // address reaches the advertised address, the `file`-discovery
    // registration, and NodeOptions — never `host:0`. Binding now also means a
    // port conflict fails at bootstrap, naming the address, rather than
    // surfacing only when the server task is later awaited.
    let listener = TcpListener::bind(raft_addr)
        .await
        .map_err(|e| anyhow!("binding raft/admin listener on {raft_addr}: {e}"))?;
    let bound_raft_port = listener
        .local_addr()
        .map_err(|e| anyhow!("reading the bound raft/admin listener address: {e}"))?
        .port();
    let advertise_addr = cfg.listen.advertised_raft_addr_on_port(bound_raft_port);
    tracing::info!(bind = %raft_addr, advertised = %advertise_addr, "raft/admin listener bound");

    // The `file` discovery backend needs this process registered so peers can
    // discover it (ADR 0037 §2); other backends need no registration.
    let file_registration = match &cfg.discovery.file {
        Some(file) => Some(
            FileRegistration::register(&file.dir, &advertise_addr)
                .context("registering in the file-discovery directory")?,
        ),
        None => None,
    };

    // Step 4: node options from config. No node id: the replica's identity is
    // minted at init and read from the manifest stamp thereafter (ADR 0025). The
    // consensus mesh shares the same hot-reload store, so a rotation reaches
    // outbound peer dials too (ADR 0037 §4).
    let options = NodeOptions {
        cluster_uuid,
        data_dir: cfg.data_dir.clone(),
        advertise_addr,
        election_timeout: cfg.raft.election_timeout,
        heartbeat_interval: cfg.raft.heartbeat_interval,
        rpc_timeout: cfg.raft.rpc_timeout,
        snapshot_log_entries: cfg.raft.snapshot_log_entries,
        snapshot_keep_log_entries: cfg.raft.snapshot_keep_log_entries,
        event_tap_capacity: limits::EVENT_TAP_CAPACITY,
        tls: Arc::clone(&tls_store),
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

    // The replica-local log-fetch client (ADR 0034): dials agents' NodeService
    // listeners with this node's leaf as the client identity and the cluster CA
    // as the trust root — the same hot-reload store the raft mesh and agent
    // gateway use, so a rotation reaches these dials too (ADR 0037 §4).
    let node_log_client = Arc::new(NodeClient::new(Arc::clone(&tls_store)));

    // Step 6: the mTLS server carrying both the Raft transport and the admin
    // surface. TLS is terminated by the connection-time acceptor
    // ([`coppice_tls::serve`]), which resolves this node's leaf from the shared
    // store at each handshake and enforces mandatory client auth against the
    // current CA — so the server config is NOT frozen on the tonic builder and
    // `.tls_config` is deliberately absent (ADR 0037 §4). Client certs stay
    // REQUIRED (ADR 0011: no unauthenticated peer or admin traffic).
    let admin = AdminServer::new(AdminService::new(
        Arc::clone(&consensus),
        handle.clone(),
        cluster_uuid,
    ));

    // Serve on the listener bound up front (before any address was published):
    // TLS is terminated by the connection-time acceptor over the shared store.
    let incoming = coppice_tls::serve(listener, Arc::clone(&tls_store));

    let router = Server::builder().add_service(transport).add_service(admin);

    let (raft_server_shutdown, shutdown_rx) = oneshot::channel::<()>();
    let raft_server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    tracing::info!(bind = %raft_addr, "raft/admin mTLS server serving");

    Ok(BootedCoordinator {
        cluster_id: cfg.cluster_id,
        consensus,
        views,
        event_tap,
        handle,
        node_log_client,
        raft_server_shutdown,
        raft_server,
        file_registration,
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
