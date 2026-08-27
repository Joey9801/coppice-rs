//! The coordinator boot sequence and process lifecycle.
//!
//! [`run`] is the only entry point the CLI dispatches to, and it takes no
//! startup intent — under ADR 0037 §1 there is one command for every
//! situation and intent is *derived*. It loads config, initializes tracing,
//! binds every listener, and then branches on what the data directory says:
//!
//! - **a manifest with a dangling formation intent** → serve the closed
//!   surface and nothing else, in phase `formation-failed`;
//! - **a manifest** → resume the instance on this disk under the history its
//!   stamp records, then run the full task runtime, as it always has;
//! - **no manifest** → this is a new instance, so **park**: serve the admin
//!   socket, `/readyz`, and `ProbeCluster` while the convergence loop enrolls,
//!   discovers, and probes. Park is left exactly two ways — an initialized
//!   cluster answers and this daemon joins it, or a local
//!   `coppice coordinator init` forms one here. It never bootstraps itself.
//!
//! The last branch is why the listeners are bound in [`run`] and handed to
//! whichever surface is serving: a parked daemon must be reachable on its
//! real ports (including a `:0` port a test asked for) *before* it has a
//! cluster, and must keep them across the transition.
//!
//! Every started replica — resumed, joined, or freshly formed — then runs the
//! post-start convergence loop ([`crate::convergence`]), which no-ops for a
//! caught-up voter and otherwise carries this identity to a voter seat.
//! [`bootstrap`] is the test/dev entry that skips park and formation entirely.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tonic::transport::Server;

use coppice_consensus::{
    Applied, Consensus, ConsensusError, ConsensusStatus, CoordinatorId, EventTapReceiver,
    NodeHandle, NodeOptions, OpenraftConsensus, StartIntent, StartedNode, StateViews,
    PROMOTION_LAG_MAX,
};
use coppice_core::id::ClusterId;
use coppice_net::admin::Server as AdminServer;
use coppice_state::Command;
use coppice_tls::{TlsPaths, TlsStore};

use coppice_api::http::{MetricsEndpoint, ReadyzEndpoint};

use crate::admin::AdminService;
use crate::cli::RunArgs;
use crate::formation::{self, Formation, PhaseState, StartupState};
use crate::localadmin::{AdminSocket, FormationCall, FormationDone, LocalAdmin};
use crate::tasks::housekeeping::HistorySink;
use crate::tasks::node_client::NodeClient;
use crate::tasks::renewal::RenewalPacing;
use crate::{config, limits};
use coppice_discovery::FileRegistration;

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
    /// This daemon's published phase (ADR 0037 §1/§9): what `/readyz`,
    /// `ProbeCluster`, and the admin socket answer from.
    phase: Arc<PhaseState>,
}

impl BootedCoordinator {
    /// This replica's readiness document, exactly as `GET /readyz` serves it
    /// (ADR 0037 §9). The integration harness asserts against it without
    /// standing up an HTTP client.
    pub fn readyz(&self) -> coppice_api::http::ReadyzReport {
        self.phase.readyz()
    }

    /// The `/readyz` endpoint to hand [`serve_runtime`], for embedders that
    /// assemble the runtime themselves (`coppice dev`, the integration
    /// harness).
    pub fn readyz_endpoint(&self) -> ReadyzEndpoint {
        readyz_endpoint(Arc::clone(&self.phase))
    }
}

/// Run a coordinator replica end to end: load, bind, form or resume, serve,
/// shut down.
pub async fn run(args: RunArgs) -> Result<()> {
    // Config load happens before tracing init: a config error rides out as an
    // `anyhow` error and `main` prints it to stderr even though no subscriber
    // is installed yet.
    let resolved = config::load(&args.config)
        .with_context(|| format!("loading coordinator config {}", args.config.display()))?;

    init_tracing(&resolved.config.observability)?;
    resolved.log_effective();

    tracing::info!("coppice-coordinator starting");

    // Install the process-wide Prometheus recorder here (issue #46), BEFORE
    // consensus starts: consensus emits counters/gauges/histograms from its
    // first apply, so the recorder must already exist or those startup metrics
    // land in no recorder and are lost. This also builds the `/metrics`
    // endpoint the API server hosts on the client listener. The daemon owns
    // this process, so it owns the once-per-process recorder install
    // (`runtime::run` itself no longer touches the global slot); a lost race
    // fails startup.
    let metrics = coppice_api::http::MetricsEndpoint::new(
        crate::install_metrics_recorder()?,
        crate::gather_metrics,
    );

    run_with(resolved, metrics, None).await
}

/// The daemon lifecycle, over an already-loaded config, TLS store, and
/// metrics endpoint.
///
/// Split out of [`run`] on exactly the seam `serve_runtime` already uses:
/// `shutdown` of `None` is the daemon path, where this owns the watch and
/// installs the signal handler; `Some(rx)` hands it a caller-owned trigger,
/// which is what lets the integration suite run whole daemons — parked,
/// forming, and fail-stopped — in one test process without a real signal, a
/// global recorder, or a `main`. Everything the ADR 0037 §1 branch decides
/// happens below this line, so the tested path and the shipped path are the
/// same code.
pub async fn run_with(
    resolved: config::ResolvedConfig,
    metrics: MetricsEndpoint,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<()> {
    // Resolve the TLS posture before anything binds. Material present on
    // disk loads the hot-reload store up front (fail-fast on unparseable
    // files, ADR 0011); material absent is legitimate exactly when the
    // daemon has no cluster to serve — the ADR 0037 §4 minimal deployment
    // provisions no certificates at all, and formation mints the first leaf.
    // The startup states that *serve consensus* still require material, so a
    // formed daemon whose certs went missing fails with the same clarity as
    // before.
    let tls_paths = tls_paths(&resolved.config);
    let mut tls: Option<Arc<TlsStore>> = if [&tls_paths.cert, &tls_paths.key, &tls_paths.ca]
        .iter()
        .all(|p| p.exists())
    {
        Some(load_tls_store(&resolved.config)?)
    } else {
        None
    };
    // Reload task (mtime poll; SIGHUP only in the daemon proper): spawned as
    // soon as a store exists — immediately, or after formation mints one.
    let daemon_owned = shutdown.is_none();
    let mut _tls_reload = tls
        .as_ref()
        .map(|store| spawn_tls_reload(store, daemon_owned));

    // Bind the agent gateway listener early (fail-fast on a port conflict),
    // raw: the TLS half is attached when the daemon is formed, which may be
    // later than now. Only the daemon path binds it — the integration test
    // drives `bootstrap` directly and runs several replicas in one process,
    // so binding a shared default agent port there would collide.
    let agent_raw = AgentListener::bind_raw(resolved.config.listen.agent_addr)?;
    let client_listener = bind_client_listener(
        &resolved.config,
        resolved.config.listen.client_addr,
        daemon_owned,
    )
    .await?;

    // Bind the raft/admin listener and the local admin socket next. Both are
    // needed *before* the daemon knows whether it has a cluster: a parked
    // daemon answers `ProbeCluster` on the first and receives `init` on the
    // second (ADR 0037 §3).
    let prepared = prepare(&resolved.config).await?;
    let admin_socket = AdminSocket::bind(
        &resolved.config.admin_socket_path(),
        &resolved.config.data_dir,
    )
    .await?;

    // One shutdown watch for the daemon's whole life, because the
    // pre-formation surfaces and the post-formation runtime both drain from
    // it. (`runtime::run` therefore always gets `Some(rx)`, the path that
    // skips its own signal handler; the handler is installed here instead,
    // because a parked daemon must answer a SIGTERM and never reaches the
    // runtime.)
    let external_shutdown = match shutdown {
        Some(rx) => rx,
        None => {
            let (tx, rx) = watch::channel(false);
            install_signal_handler(tx);
            rx
        }
    };
    // Everything below drains from an *internal* watch that the external one
    // feeds, so this daemon has a second way to stop itself: the convergence
    // loop's history-superseded fail-stop (ADR 0037 §3) flips it, and the tail
    // of this function turns that into a nonzero exit. One watch either way, so
    // the documented shutdown order is identical whichever trigger fired.
    let (internal_shutdown, shutdown_rx) = watch::channel(false);
    let failstop = crate::convergence::FailStop::new(internal_shutdown.clone());
    tokio::spawn({
        let mut external = external_shutdown;
        async move {
            let _ = external.wait_for(|stop| *stop).await;
            let _ = internal_shutdown.send(true);
        }
    });

    let (startup, marks) = formation::inspect(&resolved.config.data_dir)?;
    let phase = PhaseState::unformed(
        resolved.config.cluster_id,
        resolved.config.discovery.cluster_size,
        contact_staleness(&resolved.config),
        resolved.config.raft.health_stability_interval,
        marks,
    );
    let readyz = readyz_endpoint(Arc::clone(&phase));

    // The admin socket serves in every phase, this one included.
    let (form_tx, form_rx) = mpsc::channel::<FormationCall>(1);
    let local_admin = LocalAdmin::new(
        Arc::clone(&phase),
        resolved.config.data_dir.clone(),
        form_tx,
    );
    let admin_socket_join =
        tokio::spawn(admin_socket.serve(Arc::clone(&local_admin), shutdown_rx.clone()));

    let admin_service: AdminService<OpenraftConsensus> = AdminService::unformed(
        Arc::clone(&phase),
        resolved.config.data_dir.clone(),
        tls.clone(),
        resolved.config.token_kdf.kdf(),
    );

    let (started, tls_store) = match startup {
        // Fail-stop (ADR 0037 §3): serve the closed surface so the operator
        // can see *why*, and serve nothing else. There is no resume path.
        StartupState::FormationFailed { intent_at_us } => {
            tracing::error!("{}", formation::failed_diagnostic(intent_at_us));
            let closed = ClosedSurface::spawn(
                &prepared,
                &client_listener,
                tls.clone(),
                admin_service.clone(),
                metrics.clone(),
                readyz.clone(),
            )?;
            crate::systemd::notify_ready();
            let mut rx = shutdown_rx.clone();
            let _ = rx.wait_for(|s| *s).await;
            closed.shutdown().await;
            let _ = admin_socket_join.await;
            if let Some(registration) = prepared.file_registration {
                registration.remove().await;
            }
            bail!(
                "refusing to serve: {}",
                formation::failed_diagnostic(intent_at_us)
            );
        }

        // A resumable instance: the ADR 0016 matrix inside `start` governs,
        // and identity comes from the stamp. This state serves consensus, so
        // TLS material is required exactly as it always was — a formed daemon
        // whose certificates went missing must fail loudly, not park.
        StartupState::Resume { history_id } => {
            let store = tls
                .take()
                .ok_or_else(|| missing_tls_error(&resolved.config))?;
            let history = formation::resumed_history(&resolved.config, history_id, marks)?;
            let started = start_with_intent(
                &resolved.config,
                &prepared,
                history,
                Arc::clone(&store),
                StartIntent::Restart,
            )
            .await?;
            (started, store)
        }
        // Park (ADR 0037 §1): a new instance. It never bootstraps itself; it
        // converges toward whatever cluster exists, and waits for a local
        // `init` if none does.
        StartupState::Empty => {
            match park(
                &resolved,
                &prepared,
                tls.clone(),
                Arc::clone(&phase),
                admin_service.clone(),
                metrics.clone(),
                readyz.clone(),
                &client_listener,
                form_rx,
                shutdown_rx.clone(),
            )
            .await?
            {
                // Formed here, or joined a cluster the convergence loop found.
                // Both leave park with a running replica and a store; the
                // daemon may have started certless (formation minted the
                // material, or enrollment fetched it), so the reload task
                // starts here for either path.
                ParkOutcome::Started(started, store) => {
                    if _tls_reload.is_none() {
                        _tls_reload = Some(spawn_tls_reload(&store, daemon_owned));
                    }
                    (started, store)
                }
                outcome => {
                    // Either shut down while parked, or a formation attempt
                    // that died after stamping its intent. Nothing else was
                    // ever started, so the tail is short — but the failed case
                    // must still exit with the diagnostic, exactly as a
                    // restart into that state does.
                    let _ = admin_socket_join.await;
                    if let Some(registration) = prepared.file_registration {
                        registration.remove().await;
                    }
                    if let ParkOutcome::Failed { intent_at_us } = outcome {
                        bail!(
                            "refusing to serve: {}",
                            formation::failed_diagnostic(intent_at_us)
                        );
                    }
                    tracing::info!("shutdown complete (never formed)");
                    return Ok(());
                }
            }
        }
    };

    // The agent gateway's TLS half attaches now that material is certain.
    let agent_listener = AgentListener {
        listener: agent_raw,
        tls: Arc::clone(&tls_store),
    };

    // Captured before `assemble` consumes `prepared`: the convergence loop
    // asks to be admitted at exactly the address this process publishes
    // everywhere else, resolved `:0` port included.
    let advertise_addr = prepared.advertise_addr.clone();
    let convergence_tls = Arc::clone(&tls_store);
    // The local admin socket's share: `rotate-ca begin` keys the other voters
    // over the machine plane, which means dialing them with this daemon's own
    // serving material (ADR 0037 §4).
    let local_admin_tls = Arc::clone(&tls_store);

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
        phase: _,
    } = assemble(
        &resolved.config,
        tls_store,
        started,
        Arc::clone(&phase),
        admin_service,
        prepared,
    )?;

    local_admin.attach(Arc::clone(&consensus), handle.clone(), local_admin_tls);

    // The post-start convergence loop (ADR 0037 §6), for every replica this
    // process could have produced: a resumed voter (where it no-ops), a
    // replica the park loop just joined as an unadmitted new instance, and a
    // freshly formed first voter (which is already a voter, so it also
    // no-ops). One loop, no special cases — and the reason a scale-out needs
    // no operator at all.
    let convergence = crate::convergence::spawn(crate::convergence::Convergence {
        handle: handle.clone(),
        advertise_addr,
        cluster_id: cluster_id.to_string(),
        discovery: coppice_discovery::build(&resolved.config.discovery.seed_config())
            .context("building the discovery backend for the convergence loop")?,
        tls: convergence_tls,
        phase: Arc::clone(&phase),
        pacing: resolved.config.pacing.clone(),
        // The supersession channel that survives a re-init's new trust root
        // (ADR 0037 §3/§4); absent when this deployment configures no
        // enrollment endpoint.
        public_edge: crate::convergence::PublicEdge::from_config(
            resolved.config.enrollment.as_ref(),
        ),
        failstop: failstop.clone(),
        // Scoped to this daemon's config, so one process hosting a whole test
        // fleet can arm the joiner without arming its leader (ADR 0037 §6).
        // Always disarmed in a release build: the section cannot load there.
        failpoints: resolved.config.failpoints(),
        supersession: std::sync::Mutex::new(None),
    });

    // The task runtime owns steps 1–4 of the shutdown order and returns once
    // the shared shutdown watch has fully drained it.
    serve_runtime_with_serving_sans(
        Arc::clone(&consensus),
        views,
        event_tap,
        handle.clone(),
        agent_listener,
        client_listener,
        cluster_id,
        node_log_client,
        resolved.config.data_dir.clone(),
        metrics,
        readyz,
        // The config-declared serving names: renewal re-declares these on
        // every re-issued leaf, which is what lets an operator move this
        // daemon's advertised host by editing the config and restarting
        // (ADR 0037 §6 set-address choreography — see `tasks::renewal`).
        Some(crate::formation::leaf_sans(&resolved.config)),
        // The `[pacing]` renewal knobs, alongside the convergence pacing the
        // loop above got: same section, same node-local liveness-only
        // character (ADR 0020).
        resolved.config.pacing.renewal(),
        // The declared `[history]` mode (ADR 0012). `config::load` already
        // rejected a config that never declared one, so this cannot silently
        // become the lossy mode by default — it is what the file says.
        match resolved
            .config
            .history_mode()
            .context("resolving the [history] mode validated at load")?
        {
            crate::config::HistoryMode::None => HistorySink::None,
        },
        // The `[pacing]` housekeeping knob, alongside the renewal knobs
        // above: node-local, liveness-only, and 60 s unless the file says
        // otherwise.
        resolved.config.pacing.housekeeping_interval,
        Some(shutdown_rx),
    )
    .await?;

    // The convergence loop holds only client dials and drives idempotent
    // verbs, so aborting it at any await is indistinguishable from a tick
    // that never happened (ADR 0037 §6).
    convergence.abort();
    let _ = admin_socket_join.await;

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

    // A fail-stop drains exactly like a signal and then exits **nonzero**
    // (ADR 0037 §3): the difference between "the operator stopped this daemon"
    // and "this daemon refused to keep running" has to survive into the exit
    // status, because under `Restart=always` that status is the only thing
    // standing between a superseded volume and an endless quiet restart.
    if let Some(reason) = failstop.reason() {
        bail!("refusing to serve: {reason}");
    }

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
///
/// `history` is the `[history]` mode of ADR 0012 — where terminal-job history
/// goes once housekeeping evicts the job from replicated state. There is no
/// default: an embedder says which mode it is running, because a deployment
/// that never declared itself lossy must not be made lossy by an omitted
/// argument (issue #43).
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
    // This daemon's data directory: the agent gateway's renewal RPC signs from
    // the CA key it holds (ADR 0037 §4).
    data_dir: std::path::PathBuf,
    metrics: coppice_api::http::MetricsEndpoint,
    readyz: ReadyzEndpoint,
    history: HistorySink,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<()> {
    serve_runtime_with_serving_sans(
        consensus,
        views,
        event_tap,
        node_handle,
        agent_listener,
        client_listener,
        cluster_id,
        node_log_client,
        data_dir,
        metrics,
        readyz,
        None,
        // No config in hand on this seam, so production renewal pacing.
        RenewalPacing::default(),
        history,
        // Likewise the production sweep cadence: an embedder with nothing to
        // say about pacing gets what a deployment gets.
        crate::limits::HOUSEKEEPING_INTERVAL,
        shutdown,
    )
    .await
}

/// [`serve_runtime`] plus the daemon's configured serving names
/// (`formation::leaf_sans`), which the renewal task re-declares on every
/// re-issued leaf instead of copying the old leaf's SANs (ADR 0037 §4/§6 —
/// what lets an address move renew its way to a verifiable leaf). The daemon
/// path (`run_with`) always passes `Some`; [`serve_runtime`] is the
/// config-less embedder seam and falls back to copying.
///
/// `renewal_pacing` is that same task's `[pacing]` configuration, for the same
/// reason: the daemon path passes what the config says, the config-less seam
/// passes the production defaults. `housekeeping_interval` — how often a
/// leader sweeps terminal jobs past the retention TTL (ADR 0012) — is the same
/// bargain again, and exists as an argument at all so a test need not sit out
/// a production tick to watch a TTL expire. `history` is the ADR 0012
/// `[history]` mode, which has no such fallback — both seams state it, and the
/// daemon path maps it from the section `config::load` already validated.
#[allow(clippy::too_many_arguments)] // thin wiring seam over `runtime::run`
pub async fn serve_runtime_with_serving_sans(
    consensus: Arc<OpenraftConsensus>,
    views: StateViews,
    event_tap: EventTapReceiver,
    node_handle: NodeHandle,
    agent_listener: AgentListener,
    client_listener: ClientListener,
    cluster_id: ClusterId,
    node_log_client: Arc<NodeClient>,
    data_dir: std::path::PathBuf,
    metrics: coppice_api::http::MetricsEndpoint,
    readyz: ReadyzEndpoint,
    serving_sans: Option<Vec<String>>,
    renewal_pacing: RenewalPacing,
    history: HistorySink,
    housekeeping_interval: std::time::Duration,
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
        data_dir,
        metrics,
        readyz,
        serving_sans,
        renewal_pacing,
        history,
        housekeeping_interval,
        shutdown,
    )
    .await
}

/// A [`ReadyzEndpoint`] over a daemon's published phase (ADR 0037 §9).
///
/// The threshold is consensus's own promotion threshold, so "ready" means
/// exactly what "caught up enough to be a voter" means everywhere else.
///
/// Two callbacks because §9 asks two questions of the same daemon: the report
/// is *node* readiness, which every replica can answer about itself, and the
/// verdict is *cluster redundancy*, which only the leader can answer at all.
/// Both read the same [`PhaseState`], so a scrape of one can never disagree
/// with a scrape of the other about what this replica is.
pub(crate) fn readyz_endpoint(phase: Arc<PhaseState>) -> ReadyzEndpoint {
    let health = Arc::clone(&phase);
    ReadyzEndpoint::new(
        PROMOTION_LAG_MAX,
        move || phase.readyz(),
        move || health.health(),
    )
}

/// The bound public client-API listener (`listen.client_addr`, ADR 0031),
/// handed to `runtime::run` which serves `coppice_api::http` on it.
///
/// Bound eagerly (fail-fast on a port conflict) like [`AgentListener`]. Its
/// serving posture rides with it: `None` is the conspicuous
/// `[client_tls].insecure` plain-HTTP mode, and a store is an
/// externally-signed certificate served with per-accept resolution
/// (ADR 0037 §4). Unlike the fenced mTLS planes this edge serves browsers and
/// CLIs, so the cluster CA never serves here — it is only the trust anchor for
/// *client* certificates presented to it, and user authn remains the bearer
/// token contract of ADR 0022.
pub struct ClientListener {
    listener: tokio::net::TcpListener,
    tls: Option<Arc<coppice_tls::ClientTlsStore>>,
}

impl ClientListener {
    /// Bind the client API listener on `addr`, serving plain HTTP.
    pub async fn bind(addr: SocketAddr) -> Result<ClientListener> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow!("binding client API listener on {addr}: {e}"))?;
        tracing::info!(%addr, "client API listener bound");
        Ok(ClientListener {
            listener,
            tls: None,
        })
    }

    /// Serve this listener over TLS from `store`'s material.
    pub fn with_tls(mut self, store: Arc<coppice_tls::ClientTlsStore>) -> ClientListener {
        self.tls = Some(store);
        self
    }

    /// The actual bound address (which resolves a `:0` request).
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// The socket and the posture, for the serving path.
    pub(crate) fn into_parts(
        self,
    ) -> (
        tokio::net::TcpListener,
        Option<Arc<coppice_tls::ClientTlsStore>>,
    ) {
        (self.listener, self.tls)
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
        Ok(AgentListener {
            listener: AgentListener::bind_raw(addr)?,
            tls,
        })
    }

    /// Bind the raw TCP listener alone, for the daemon path where the TLS
    /// half arrives later than the bind must happen: a parked daemon claims
    /// its port before any material exists (ADR 0037 §3), and the store is
    /// attached once formed.
    pub(crate) fn bind_raw(addr: SocketAddr) -> Result<std::net::TcpListener> {
        let listener = std::net::TcpListener::bind(addr)
            .map_err(|e| anyhow!("binding agent gateway listener on {addr}: {e}"))?;
        // Non-blocking so `runtime` can adopt it as a tokio listener.
        listener
            .set_nonblocking(true)
            .map_err(|e| anyhow!("setting agent gateway listener non-blocking: {e}"))?;
        tracing::info!(%addr, "agent gateway mTLS listener bound");
        Ok(listener)
    }
}

/// Bind the client listener under the configured `[client_tls]` posture
/// (ADR 0037 §4).
///
/// The posture is resolved (and its "say which mode this is" error raised) at
/// config load; here it only decides whether a serving store is loaded — which
/// itself fails fast, naming the path, if the certificate or key is missing.
/// A daemon configured for TLS never silently serves plain HTTP.
async fn bind_client_listener(
    cfg: &config::Config,
    addr: SocketAddr,
    daemon_owned: bool,
) -> Result<ClientListener> {
    let listener = ClientListener::bind(addr).await?;
    match cfg.client_tls_posture()? {
        config::ClientTlsPosture::Insecure => {
            tracing::warn!(
                %addr,
                "client listener is serving PLAIN HTTP ([client_tls] insecure = true): \
                 enrollment tokens and bearer credentials cross this listener in the clear \
                 (ADR 0037 §4 — development/test only)"
            );
            Ok(listener)
        }
        config::ClientTlsPosture::Tls { cert, key } => {
            let store = coppice_tls::ClientTlsStore::load(coppice_tls::ClientTlsPaths {
                cert: cert.clone(),
                key: key.clone(),
            })
            .context("loading the client listener's serving certificate (config [client_tls])")?;
            // Rotations of the public serving certificate are picked up the
            // same way the machine plane's are: mtime poll, SIGHUP force.
            let _reload = coppice_tls::spawn_reload_task(
                Arc::clone(&store),
                coppice_tls::ReloadOptions {
                    sighup: daemon_owned,
                    ..Default::default()
                },
            );
            tracing::info!(%addr, cert = %cert.display(), "client listener serving TLS");
            Ok(listener.with_tls(store))
        }
    }
}

/// The config's `[tls]` paths as a [`TlsPaths`].
pub(crate) fn tls_paths(cfg: &config::Config) -> TlsPaths {
    TlsPaths {
        cert: cfg.tls.cert_path.clone(),
        key: cfg.tls.key_path.clone(),
        ca: cfg.tls.ca_path.clone(),
    }
}

/// Load the shared hot-reload TLS store from the config's `[tls]` paths
/// (ADR 0011/0037 §4). Fails fast, naming the offending path, if any file is
/// missing or unparseable.
fn load_tls_store(cfg: &config::Config) -> Result<Arc<TlsStore>> {
    TlsStore::load(tls_paths(cfg)).context("loading coordinator TLS material (config [tls])")
}

/// The error for a startup state that serves consensus without TLS material
/// on disk — the pre-ADR-0037 fail-fast, now scoped to the states it
/// belongs to (a parked daemon legitimately has no material yet).
fn missing_tls_error(cfg: &config::Config) -> anyhow::Error {
    anyhow!(
        "this data directory holds a cluster but the [tls] material is missing \
         (cert {}, key {}, ca {}); a coordinator serving consensus must have valid \
         machine credentials (ADR 0011)",
        cfg.tls.cert_path.display(),
        cfg.tls.key_path.display(),
        cfg.tls.ca_path.display(),
    )
}

/// Spawn the mtime-poll reload task over `store`; SIGHUP only in the daemon
/// proper (an in-process test rotates its store directly).
fn spawn_tls_reload(store: &Arc<TlsStore>, daemon_owned: bool) -> tokio::task::JoinHandle<()> {
    coppice_tls::spawn_reload_task(
        Arc::clone(store),
        coppice_tls::ReloadOptions {
            sighup: daemon_owned,
            ..Default::default()
        },
    )
}

/// The contact-staleness bound (ADR 0037 §9), used for both directions of
/// "have we actually heard from each other": how stale a leader's quorum
/// acknowledgment may be before this replica stops reporting itself ready,
/// and how long a voter may go without answering the leader's RPCs before
/// the health sampler stops counting it live. Twice the election-timeout
/// minimum — openraft's election-timeout maximum — past which a healthy,
/// connected node would have called an election of its own.
fn contact_staleness(cfg: &config::Config) -> std::time::Duration {
    cfg.raft.election_timeout.saturating_mul(2)
}

/// The data directory, the bound raft/admin listener, and the address this
/// replica will advertise — everything that must exist before the daemon
/// knows whether it has a cluster.
pub(crate) struct Prepared {
    listener: TcpListener,
    bind_addr: SocketAddr,
    advertise_addr: String,
    file_registration: Option<FileRegistration>,
}

/// Create the data directory and bind the raft/admin listener (ADR 0037 §2).
///
/// The listener is bound FIRST, before anything publishes an address. A
/// `raft_addr` with port 0 (the multi-process dev case) must resolve to its
/// real bound port here so the *same* concrete address reaches the advertised
/// address, the `file`-discovery registration, and `NodeOptions` — never
/// `host:0`. Binding now also means a port conflict fails at startup, naming
/// the address, rather than surfacing only when the server task is awaited.
pub(crate) async fn prepare(cfg: &config::Config) -> Result<Prepared> {
    // Creating an empty dir is safe — what governs identity is the manifest
    // check, and an unmounted volume surfaces as a parked daemon (ADR 0037
    // §1) whose `/readyz` says so, with the mount guard moved to the unit.
    std::fs::create_dir_all(&cfg.data_dir).with_context(|| {
        format!(
            "creating coordinator data directory {}",
            cfg.data_dir.display()
        )
    })?;

    let bind_addr = cfg.listen.raft_addr;
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| anyhow!("binding raft/admin listener on {bind_addr}: {e}"))?;
    let bound_raft_port = listener
        .local_addr()
        .map_err(|e| anyhow!("reading the bound raft/admin listener address: {e}"))?
        .port();
    let advertise_addr = cfg.listen.advertised_raft_addr_on_port(bound_raft_port);
    tracing::info!(bind = %bind_addr, advertised = %advertise_addr, "raft/admin listener bound");

    // The `file` discovery backend needs this process registered so peers can
    // discover it (ADR 0037 §2); other backends need no registration.
    let file_registration = match &cfg.discovery.file {
        Some(file) => Some(
            FileRegistration::register(&file.dir, &advertise_addr)
                .context("registering in the file-discovery directory")?,
        ),
        None => None,
    };

    Ok(Prepared {
        listener,
        bind_addr,
        advertise_addr,
        file_registration,
    })
}

/// Node options from config. No node id: the replica's identity is minted at
/// init and read from the manifest stamp thereafter (ADR 0025). The consensus
/// mesh shares the same hot-reload store, so a rotation reaches outbound peer
/// dials too (ADR 0037 §4).
pub(crate) fn node_options(
    cfg: &config::Config,
    history_id: [u8; 16],
    advertise_addr: String,
    tls_store: Arc<TlsStore>,
) -> NodeOptions {
    NodeOptions {
        history_id,
        data_dir: cfg.data_dir.clone(),
        advertise_addr,
        election_timeout: cfg.raft.election_timeout,
        heartbeat_interval: cfg.raft.heartbeat_interval,
        rpc_timeout: cfg.raft.rpc_timeout,
        snapshot_log_entries: cfg.raft.snapshot_log_entries,
        snapshot_keep_log_entries: cfg.raft.snapshot_keep_log_entries,
        event_tap_capacity: limits::EVENT_TAP_CAPACITY,
        tls: tls_store,
        // The expected voter count (ADR 0037 §2/§7): the ceiling the leader
        // applies to promotions. Node-local config, because convergence
        // consults it before replicated state is reachable.
        cluster_size: cfg.discovery.cluster_size,
        removal_grace: cfg.discovery.removal_grace,
        learner_expiry: cfg.discovery.learner_expiry,
    }
}

/// Bring up consensus under an explicit ADR 0016 intent (the matrix itself is
/// enforced inside `start`).
async fn start_with_intent(
    cfg: &config::Config,
    prepared: &Prepared,
    history_id: [u8; 16],
    tls_store: Arc<TlsStore>,
    intent: StartIntent,
) -> Result<StartedNode> {
    let options = node_options(cfg, history_id, prepared.advertise_addr.clone(), tls_store);
    coppice_consensus::start(options, intent)
        .await
        .context("starting consensus replica")
}

/// Assemble a booted coordinator around an already-started replica: publish
/// the formed phase, attach the admin service's consensus seam, and stand up
/// the mTLS raft/admin server.
fn assemble(
    cfg: &config::Config,
    tls_store: Arc<TlsStore>,
    started: StartedNode,
    phase: Arc<PhaseState>,
    admin_service: AdminService<OpenraftConsensus>,
    prepared: Prepared,
) -> Result<BootedCoordinator> {
    let StartedNode {
        consensus,
        views,
        event_tap,
        handle,
        transport,
        formation: _,
    } = started;

    // Surfaced on every start (not just at mint) so an operator can always
    // read the id off the newest log lines, e.g. for `admin add-learner`.
    tracing::info!(node_id = handle.node_id(), "coordinator raft identity");

    let consensus = Arc::new(consensus);
    phase.publish_formed(handle.clone(), views.clone());
    admin_service.attach(
        Arc::clone(&consensus),
        handle.clone(),
        Arc::clone(&tls_store),
    );

    // The replica-local log-fetch client (ADR 0034): dials agents' NodeService
    // listeners with this node's leaf as the client identity and the cluster CA
    // as the trust root — the same hot-reload store the raft mesh and agent
    // gateway use, so a rotation reaches these dials too (ADR 0037 §4).
    let node_log_client = Arc::new(NodeClient::new(Arc::clone(&tls_store)));

    // The mTLS server carrying the Raft transport and the admin surface. TLS
    // is terminated by the connection-time acceptor ([`coppice_tls::serve`]),
    // which resolves this node's leaf from the shared store at each handshake
    // and enforces mandatory client auth against the current CA — so the
    // server config is NOT frozen on the tonic builder and `.tls_config` is
    // deliberately absent (ADR 0037 §4). Client certs stay REQUIRED (ADR
    // 0011: no unauthenticated peer or admin traffic).
    let Prepared {
        listener,
        bind_addr,
        advertise_addr: _,
        file_registration,
    } = prepared;
    let incoming = coppice_tls::serve(listener, Arc::clone(&tls_store));
    let router = Server::builder()
        .add_service(transport)
        .add_service(AdminServer::new(admin_service));

    let (raft_server_shutdown, shutdown_rx) = oneshot::channel::<()>();
    let raft_server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    tracing::info!(bind = %bind_addr, "raft/admin mTLS server serving");

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
        phase,
    })
}

/// The **test and `coppice dev` entry**: resume this data directory, or form a
/// single-voter cluster on it if it is empty.
///
/// Deliberately not the production path. It skips park, skips formation's
/// seven steps, and runs no convergence loop — an embedder that wants those
/// calls [`run`] or [`run_with`], which is what a real daemon does. What this
/// is for is the caller that already knows there is exactly one replica and
/// wants it serving now: `coppice dev`, and the integration harness's
/// single-node fixtures.
///
/// The derived behavior mirrors the two states that matter to such a caller,
/// with no flag to get wrong: a directory with a manifest resumes under the
/// history its stamp records (a formed one carries the history `init` minted,
/// which config cannot know); a fresh directory becomes a single-voter cluster
/// stamped with the config-derived history, preserving the ADR 0016
/// config-vs-stamp cross-check for the directories it creates.
pub async fn bootstrap(
    resolved: config::ResolvedConfig,
    tls_store: Arc<TlsStore>,
) -> Result<BootedCoordinator> {
    start_directly(resolved, tls_store, StartIntent::Bootstrap).await
}

/// The **test entry for the joining half** of a multi-replica fixture: resume
/// this data directory, or start a fresh learner-join instance on it if it is
/// empty.
///
/// Identical to [`bootstrap`] but for what an *empty* directory means. A
/// production replica reaches [`StartIntent::Join`] through the convergence
/// loop ([`crate::convergence`]), which discovers a cluster, probes it, and
/// stamps the history the probe reported. A test that is building the cluster
/// by hand — one node bootstrapped in-process, the rest added through
/// `add_learner` on its consensus seam — has no discovery to do and already
/// knows the history, so it says so here rather than standing up the loop.
///
/// Nothing outside the integration suite should call this: a real daemon that
/// wants to join converges into it.
pub async fn bootstrap_joining(
    resolved: config::ResolvedConfig,
    tls_store: Arc<TlsStore>,
) -> Result<BootedCoordinator> {
    start_directly(resolved, tls_store, StartIntent::Join).await
}

/// The shared body of [`bootstrap`] and [`bootstrap_joining`]: derive the
/// intent from the directory, start, and assemble. `empty_intent` is what an
/// empty directory means to this caller — the only thing the two differ on.
async fn start_directly(
    resolved: config::ResolvedConfig,
    tls_store: Arc<TlsStore>,
    empty_intent: StartIntent,
) -> Result<BootedCoordinator> {
    let prepared = prepare(&resolved.config).await?;
    let (startup, marks) = formation::inspect(&resolved.config.data_dir)?;
    let (intent, history_id) = match startup {
        StartupState::Resume { history_id } => (
            StartIntent::Restart,
            formation::resumed_history(&resolved.config, history_id, marks)?,
        ),
        StartupState::Empty => (empty_intent, *resolved.config.cluster_id.0.as_bytes()),
        StartupState::FormationFailed { intent_at_us } => {
            bail!(
                "refusing to serve: {}",
                formation::failed_diagnostic(intent_at_us)
            )
        }
    };

    let options = node_options(
        &resolved.config,
        history_id,
        prepared.advertise_addr.clone(),
        Arc::clone(&tls_store),
    );
    let started = coppice_consensus::start(options, intent)
        .await
        .context("starting consensus replica")?;

    let phase = PhaseState::unformed(
        resolved.config.cluster_id,
        resolved.config.discovery.cluster_size,
        contact_staleness(&resolved.config),
        resolved.config.raft.health_stability_interval,
        marks,
    );
    let admin_service = AdminService::unformed(
        Arc::clone(&phase),
        resolved.config.data_dir.clone(),
        Some(Arc::clone(&tls_store)),
        resolved.config.token_kdf.kdf(),
    );

    assemble(
        &resolved.config,
        tls_store,
        started,
        phase,
        admin_service,
        prepared,
    )
}

// ---------------------------------------------------------------------------
// The pre-formation surface, and parking
// ---------------------------------------------------------------------------

/// What a daemon serves before it has a cluster (ADR 0037 §3).
///
/// Two servers on the two listeners the daemon has already bound: the client
/// listener carries `/readyz` and `/metrics` and *nothing else* — the client
/// API is not served until the `formation_complete` marker exists — and the
/// raft listener carries the admin service, whose only ungated verb is
/// `ProbeCluster`. A parked peer can therefore discover this daemon and learn
/// it is not a cluster, which is exactly the visibility the ADR wants and
/// exactly the joinability it forbids.
///
/// Both servers run on **duplicated** listener descriptors. The daemon keeps
/// the originals, so when formation completes and these servers drain, the
/// bound sockets — including the concrete port a `:0` config resolved to —
/// survive into the full runtime unchanged. Connections that arrive in the
/// handover window queue in the accept backlog rather than being refused.
///
/// The gRPC half exists only when the daemon holds TLS material. A genuinely
/// fresh installation has none (ADR 0037 §4: nothing is provisioned), so it
/// cannot terminate the mTLS handshake `ProbeCluster` rides on — peers that
/// dial it fail and skip it, exactly the "unreachable candidates are simply
/// skipped" posture, and the daemon remains visible through `/readyz` and
/// reachable through the admin socket.
struct ClosedSurface {
    stop: watch::Sender<bool>,
    http: JoinHandle<()>,
    grpc: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl ClosedSurface {
    fn spawn(
        prepared: &Prepared,
        client_listener: &ClientListener,
        tls_store: Option<Arc<TlsStore>>,
        admin_service: AdminService<OpenraftConsensus>,
        metrics: MetricsEndpoint,
        readyz: ReadyzEndpoint,
    ) -> Result<ClosedSurface> {
        let (stop, stop_rx) = watch::channel(false);

        let http_listener = dup_listener(&client_listener.listener)
            .context("duplicating the client listener for the pre-formation surface")?;
        let app = coppice_api::http::closed_router(metrics, readyz);
        // The same posture the formed daemon will serve under: an operator's
        // `curl https://…/readyz` must not have to change at formation. A
        // pre-formation daemon has no cluster CA, so it asks for no client
        // certificate ([`ClusterCa::none`]).
        let http_tls = client_listener
            .tls
            .clone()
            .map(|store| (store, crate::clientedge::ClusterCa::none()));
        let http_stop = stop_rx.clone();
        let http = tokio::spawn(async move {
            crate::clientedge::serve(http_listener, app, http_tls, http_stop).await;
        });

        let grpc = match tls_store {
            Some(store) => {
                let grpc_listener = dup_listener(&prepared.listener)
                    .context("duplicating the raft listener for the pre-formation surface")?;
                let incoming = coppice_tls::serve(grpc_listener, store);
                let router = Server::builder().add_service(AdminServer::new(admin_service));
                let mut grpc_stop = stop_rx;
                Some(tokio::spawn(async move {
                    router
                        .serve_with_incoming_shutdown(incoming, async move {
                            let _ = grpc_stop.wait_for(|s| *s).await;
                        })
                        .await
                }))
            }
            None => {
                tracing::info!(
                    "no TLS material yet: the pre-formation ProbeCluster surface is not \
                     served (peers skip an undialable candidate); formation will mint the \
                     first material"
                );
                None
            }
        };

        tracing::info!("serving the pre-formation surface (/readyz, /metrics, ProbeCluster)");
        Ok(ClosedSurface { stop, http, grpc })
    }

    /// Stop accepting, drain briefly, then release the duplicated descriptors.
    ///
    /// The drain is **bounded**, and that bound is load-bearing. Both servers
    /// stop accepting the instant `stop` fires, so the sockets are free for
    /// the full runtime immediately; what remains is graceful completion of
    /// requests already in flight. Waiting on that without a deadline would
    /// let a single client that opened a connection and never finished its
    /// request wedge the parked→formed handover — and with it the whole
    /// daemon, since nothing downstream of this call has started yet. A stalled
    /// health probe or a port scanner is enough. The pre-formation surface
    /// serves only `/readyz`, `/metrics`, and `ProbeCluster`, none of which
    /// take meaningful time, so anything still open at the deadline is by
    /// definition not making progress and is dropped.
    async fn shutdown(self) {
        let ClosedSurface { stop, http, grpc } = self;
        let _ = stop.send(true);
        let http_abort = http.abort_handle();
        let grpc_abort = grpc.as_ref().map(|g| g.abort_handle());
        let drain = async {
            let _ = http.await;
            if let Some(grpc) = grpc {
                let _ = grpc.await;
            }
        };
        if tokio::time::timeout(CLOSED_SURFACE_DRAIN, drain)
            .await
            .is_err()
        {
            tracing::warn!(
                timeout = ?CLOSED_SURFACE_DRAIN,
                "pre-formation surface still had in-flight connections at the deadline; \
                 dropping them so the runtime can take the listeners"
            );
            http_abort.abort();
            if let Some(grpc_abort) = grpc_abort {
                grpc_abort.abort();
            }
        }
        tracing::debug!("pre-formation surface down");
    }
}

/// How long in-flight pre-formation requests get to finish before the
/// handover proceeds without them. See [`ClosedSurface::shutdown`].
const CLOSED_SURFACE_DRAIN: std::time::Duration = std::time::Duration::from_secs(2);

/// Duplicate a bound listener's descriptor.
///
/// Both handles refer to the same socket, so closing one leaves the other —
/// and the bound port — intact. That is what lets the pre-formation surface
/// serve on the daemon's real ports and hand them back at formation without a
/// rebind (which a `:0` config could not survive).
fn dup_listener(listener: &TcpListener) -> Result<TcpListener> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        let owned = listener.as_fd().try_clone_to_owned()?;
        let std_listener = std::net::TcpListener::from(owned);
        std_listener.set_nonblocking(true)?;
        Ok(TcpListener::from_std(std_listener)?)
    }
    #[cfg(not(unix))]
    {
        let _ = listener;
        bail!("the coordinator's pre-formation surface requires a Unix platform")
    }
}

/// How a stint in the parked state ended.
///
/// The `Started` arm is much larger than the other two, which is the right
/// shape here: this value is constructed once per process, moved once, and
/// boxing the started replica would buy nothing but an allocation.
#[allow(clippy::large_enum_variant)]
enum ParkOutcome {
    /// A cluster now exists for this daemon to serve, by either of park's two
    /// exits (ADR 0037 §1): a local `init` formed one here, or the convergence
    /// loop found one in discovery and joined it. Deliberately one variant —
    /// nothing downstream of park behaves differently between the two, and a
    /// split would invite it to. The store is carried because it may have been
    /// created *during* park, by formation minting material or enrollment
    /// fetching it on a daemon that started certless.
    Started(StartedNode, Arc<TlsStore>),
    /// The daemon was asked to stop while still parked.
    Shutdown,
    /// A formation attempt died after stamping its intent. The directory is
    /// unrecoverable, so the daemon reports the fail-stop rather than
    /// returning to a park state that invites a retry it cannot satisfy.
    Failed { intent_at_us: i64 },
}

/// Park until this daemon has a cluster, or is asked to stop (ADR 0037 §1).
///
/// Two exits produce a cluster and they race in the `select!` below, because
/// a fleet's whole bringup is that race: **converge**, where this daemon
/// enrolls, discovers, probes, and joins an initialized cluster that already
/// exists; and **`init`**, where an operator (or their automation) forms one
/// here over the local socket. Whichever lands first wins, and the loser is
/// simply dropped — the convergence future holds nothing but client dials
/// until the instant it produces a replica, and `init`'s own probe guard
/// refuses if a cluster appeared meanwhile.
///
/// A parked daemon **never** bootstraps itself. That is the invariant this
/// function exists to hold: neither exit is reachable from discovery churn, a
/// partition, or a fleet that lost its volumes — all of those keep cycling
/// here, visibly, in phase `waiting`.
///
/// A formation attempt that fails leaves one of two states, distinguished by
/// re-reading the directory rather than by guessing: nothing durable happened
/// (the probe guard refused) and the daemon stays parked for another attempt;
/// or an intent was stamped and the directory is now unrecoverable, in which
/// case the *live* daemon moves to `formation-failed` immediately rather than
/// inviting a retry that cannot succeed.
#[allow(clippy::too_many_arguments)]
async fn park(
    resolved: &config::ResolvedConfig,
    prepared: &Prepared,
    tls: Option<Arc<TlsStore>>,
    phase: Arc<PhaseState>,
    admin_service: AdminService<OpenraftConsensus>,
    metrics: MetricsEndpoint,
    readyz: ReadyzEndpoint,
    client_listener: &ClientListener,
    mut form_rx: mpsc::Receiver<FormationCall>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<ParkOutcome> {
    let closed = ClosedSurface::spawn(
        prepared,
        client_listener,
        tls.clone(),
        admin_service,
        metrics,
        readyz,
    )?;

    tracing::info!(
        socket = %resolved.config.admin_socket_path().display(),
        "parked: this data directory holds no cluster. Converging — enrolling, discovering, \
         and probing for one. If this is a brand-new cluster, run \
         `coppice coordinator init` on one daemon to form it (ADR 0037 §1/§3)."
    );
    // Parked is a healthy, running daemon — `READY=1`, phase `waiting`, HTTP
    // 503 (ADR 0037 §9). Unit ordering keys off this, not off readiness.
    crate::systemd::notify_ready();

    // The converging half of park. Held across the whole stint rather than
    // rebuilt per iteration so its backoff survives a failed `init` attempt:
    // a daemon whose operator typo'd a policy file should not restart its
    // discovery cadence from scratch.
    let mut converge =
        crate::convergence::PreStart::new(&resolved.config, &prepared.advertise_addr, tls.clone());

    let outcome = loop {
        let call = tokio::select! {
            // Biased toward the local socket: an operator who ran `init` on
            // this host is waiting on a reply, and if a cluster genuinely
            // appeared in the same instant, `init`'s probe guard says so
            // rather than forming a second one.
            biased;
            call = form_rx.recv() => match call {
                Some(call) => call,
                // Nothing else holds the sender while parked.
                None => break ParkOutcome::Shutdown,
            },
            (started, store) = converge.run() => {
                phase.publish_formed(started.handle.clone(), started.views.clone());
                tracing::info!(
                    node_id = started.handle.node_id(),
                    "leaving park: joined the cluster discovery found (ADR 0037 §1)"
                );
                break ParkOutcome::Started(started, store);
            }
            _ = shutdown_rx.wait_for(|s| *s) => break ParkOutcome::Shutdown,
        };

        let ctx = formation::FormationContext {
            config: resolved.config.clone(),
            advertise_addr: prepared.advertise_addr.clone(),
            tls: tls.clone(),
            failpoint: None,
        };

        match formation::form(ctx, call.request).await {
            Ok(Formation {
                started,
                operator,
                machine,
                tls_store,
            }) => {
                let done = FormationDone {
                    history_id: formation::hex(&started.handle.history_id()),
                    node_id: started.handle.node_id(),
                    machine_id: machine.to_string(),
                    operator,
                };
                // Publish the phase before replying: the caller's very next
                // act is often to poll `/readyz` or `ProbeCluster`.
                phase.publish_formed(started.handle.clone(), started.views.clone());
                let _ = call.reply.send(Ok(done));
                break ParkOutcome::Started(started, tls_store);
            }
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "formation failed");
                let message = anyhow!("{e:#}");
                let _ = call.reply.send(Err(message));

                if let (StartupState::FormationFailed { intent_at_us }, _) =
                    formation::inspect(&resolved.config.data_dir)?
                {
                    phase.publish_failed(intent_at_us);
                    tracing::error!("{}", formation::failed_diagnostic(intent_at_us));
                    // Keep serving the closed surface so the operator can see
                    // the failed phase, exactly as a restart would — and keep
                    // answering the formation channel. A concurrent `init`
                    // that passed the phase check before this attempt failed
                    // is already queued behind it; simply parking on the
                    // shutdown watch would leave that caller waiting on a
                    // reply that never comes. New calls never reach the
                    // channel at all: the socket handler sees the failed phase
                    // and answers directly.
                    loop {
                        tokio::select! {
                            queued = form_rx.recv() => match queued {
                                Some(queued) => {
                                    let _ = queued.reply.send(Err(anyhow!(
                                        "{}",
                                        formation::failed_diagnostic(intent_at_us)
                                    )));
                                }
                                // Unreachable while the socket server holds
                                // the sender; stop draining and wait it out.
                                None => break,
                            },
                            _ = shutdown_rx.wait_for(|s| *s) => break,
                        }
                    }
                    let _ = shutdown_rx.wait_for(|s| *s).await;
                    break ParkOutcome::Failed { intent_at_us };
                }
                // Nothing was stamped: still parked, still available for a
                // corrected `init`.
            }
        }
    };

    closed.shutdown().await;
    Ok(outcome)
}

/// Install the daemon's shutdown signal handler.
///
/// Both interactive (ctrl-c / SIGINT) and orchestrated (SIGTERM, e.g. a
/// `kill` or a container stop) shutdowns flip the same watch; whichever fires
/// first wins and the other arm is dropped. Lives here rather than in
/// `runtime::run` because a parked daemon must also answer a SIGTERM, and it
/// never reaches the runtime.
fn install_signal_handler(shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGTERM handler");
                    return;
                }
            };
            let reason = tokio::select! {
                res = tokio::signal::ctrl_c() => res.map(|()| "ctrl-c").ok(),
                _ = sigterm.recv() => Some("SIGTERM"),
            };
            if let Some(reason) = reason {
                tracing::info!(signal = reason, "shutdown signal received, shutting down");
                // Tell systemd the exit is intentional (ADR 0037 §9).
                crate::systemd::notify_stopping();
                let _ = shutdown_tx.send(true);
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("ctrl-c received, shutting down");
                crate::systemd::notify_stopping();
                let _ = shutdown_tx.send(true);
            }
        }
    });
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

    fn plan_promotion(
        &self,
        promote: CoordinatorId,
    ) -> Result<coppice_consensus::PromotionPlan, ConsensusError> {
        self.0.plan_promotion(promote)
    }

    fn commit_promotion(
        &self,
        promote: CoordinatorId,
        remove: Option<CoordinatorId>,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.commit_promotion(promote, remove)
    }

    fn plan_replacement(
        &self,
        old: CoordinatorId,
        new: CoordinatorId,
    ) -> Result<coppice_consensus::ReplacementPlan, ConsensusError> {
        self.0.plan_replacement(old, new)
    }

    fn replace_voter(
        &self,
        old: CoordinatorId,
        new: CoordinatorId,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.replace_voter(old, new)
    }

    fn learner_expiry(&self) -> std::time::Duration {
        self.0.learner_expiry()
    }

    fn expired_learners(&self) -> Vec<CoordinatorId> {
        self.0.expired_learners()
    }

    fn reap_expired_learner(
        &self,
        node: CoordinatorId,
        retire: Option<Command>,
    ) -> impl Future<Output = Result<bool, ConsensusError>> + Send {
        self.0.reap_expired_learner(node, retire)
    }

    fn remove_node(
        &self,
        node: CoordinatorId,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.remove_node(node)
    }

    fn set_node_address(
        &self,
        node: CoordinatorId,
        addr: String,
    ) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.set_node_address(node, addr)
    }

    fn trigger_snapshot(&self) -> impl Future<Output = Result<(), ConsensusError>> + Send {
        self.0.trigger_snapshot()
    }
}
