//! Shared harness for the multi-node coordinator integration tests.
//!
//! Everything here is test-only scaffolding: a self-signed CA and per-node
//! leaf certificates (the mTLS material the Raft/admin transport requires,
//! ADR 0011), free-port allocation, a per-node config + data + cert tempdir,
//! and a [`Node`] wrapper that boots the real `bootstrap::bootstrap` path and
//! exposes the same lifecycle a running daemon has (graceful stop, abrupt
//! kill, restart-from-disk). No production code mints certificates or picks
//! ports — that all lives here.
//!
//! `dead_code` is allowed module-wide: `common` is shared across the test
//! binaries (`cluster`, `agent_protocol`), and each uses a different slice of
//! the harness, so items unused in one binary are not truly dead.
#![allow(dead_code)]

use std::future::Future;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coppice_core::id::ClusterId;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use coppice_consensus::{
    ClusterSummary, Consensus, ConsensusStatus, NodeHandle, OpenraftConsensus, StateViews,
};
use coppice_coordinator::bootstrap::{self, AgentListener, BootedCoordinator, ClientListener};
use coppice_coordinator::config::{self, CliOverrides};
use coppice_tls::{TlsPaths, TlsStore};

/// A hot-reload [`TlsStore`] over in-memory PEM material (ADR 0037 §4), the
/// store-based equivalent of handing raw PEM slices to the old bind/new
/// signatures. The paths are placeholders; these tests never trigger a disk
/// reload.
pub fn tls_store_from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Arc<TlsStore> {
    TlsStore::from_pem(
        TlsPaths {
            cert: "unused-cert".into(),
            key: "unused-key".into(),
            ca: "unused-ca".into(),
        },
        ca_pem.to_vec(),
        cert_pem.to_vec(),
        key_pem.to_vec(),
    )
    .expect("build tls store from pem")
}

/// A test CA plus one issued leaf's PEM material.
pub struct Leaf {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// A single self-signed CA that signs every node (and the admin client) leaf,
/// so one trust root spans the whole test mesh.
pub struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
    /// The CA certificate in PEM form — the trust root every leaf verifies
    /// against.
    pub pem: Vec<u8>,
}

impl Ca {
    pub fn new() -> Ca {
        let key = KeyPair::generate().expect("generate CA key pair");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "coppice-test-ca");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let cert = params.self_signed(&key).expect("self-sign CA");
        let pem = cert.pem().into_bytes();
        Ca { cert, key, pem }
    }

    /// Issue a leaf usable as BOTH a server and a client certificate: every
    /// node dials peers (client) and serves peers (server) with the same leaf,
    /// and the admin client presents one too, so each leaf carries both EKUs.
    /// SANs cover `localhost` and `127.0.0.1` so either dial form validates.
    pub fn leaf(&self) -> Leaf {
        self.leaf_with_cn("coppice-test-node")
    }

    /// Issue a leaf as [`Ca::leaf`] but with an explicit subject `cn`.
    ///
    /// The agent gateway parses the client leaf's CN and compares it against
    /// the claimed NodeId at session accept (ADR 0011), so the agent's client
    /// certificate must carry its node UUID string as its CN.
    pub fn leaf_with_cn(&self, cn: &str) -> Leaf {
        self.leaf_with_cn_and_sans(cn, &[])
    }

    /// Issue a leaf as [`Ca::leaf_with_cn`] but with additional dNSName SANs
    /// beyond `localhost`/`127.0.0.1`.
    ///
    /// The agent's `NodeService` server leaf must carry its typed node id as a
    /// SAN so a coordinator's id-pinned dial (TLS server-name = `node-<uuid>`)
    /// validates (ADR 0034).
    pub fn leaf_with_cn_and_sans(&self, cn: &str, extra_sans: &[String]) -> Leaf {
        let key = KeyPair::generate().expect("generate leaf key pair");
        let mut sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        sans.extend(extra_sans.iter().cloned());
        let mut params = CertificateParams::new(sans).expect("leaf params");
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
            .signed_by(&key, &self.cert, &self.key)
            .expect("sign leaf");
        Leaf {
            cert_pem: cert.pem().into_bytes(),
            key_pem: key.serialize_pem().into_bytes(),
        }
    }
}

/// Grab a currently-free localhost TCP port by binding `:0` and dropping the
/// listener. Racy in principle, fine in practice for a short-lived test.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// One coordinator replica's on-disk world (config + data dir + certs in a
/// tempdir) plus, once booted, its running [`BootedCoordinator`].
///
/// The tempdir is retained across a graceful stop / kill so the same replica
/// can be re-booted from its own disk (ADR 0016 Restart intent).
pub struct Node {
    /// Fixture label for panic messages — NOT the raft identity, which is
    /// minted at init and cached in [`Node::raft_id`] once booted (ADR 0025).
    pub id: u64,
    raft_id: Option<u64>,
    #[allow(dead_code)]
    pub port: u16,
    /// `localhost:PORT` — the address peers dial and admin tooling targets.
    pub advertise: String,
    pub cluster_id: ClusterId,
    #[allow(dead_code)]
    dir: TempDir,
    config_path: PathBuf,
    booted: Option<BootedCoordinator>,
}

impl Node {
    /// Lay down a fresh replica's tempdir (certs + config), without booting.
    pub fn new(id: u64, cluster_id: ClusterId, ca: &Ca) -> Node {
        let port = free_port();
        let dir = tempfile::tempdir().expect("create node tempdir");
        let root = dir.path();

        let leaf = ca.leaf();
        let cert_path = root.join("node.crt");
        let key_path = root.join("node.key");
        let ca_path = root.join("ca.crt");
        std::fs::write(&cert_path, &leaf.cert_pem).expect("write cert");
        std::fs::write(&key_path, &leaf.key_pem).expect("write key");
        std::fs::write(&ca_path, &ca.pem).expect("write ca");

        let data_dir = root.join("data");
        let config_path = root.join("coordinator.toml");
        let toml = format!(
            r#"cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

[discovery]
backend = "static"

[discovery.static]
addrs = []

[listen]
raft_addr = "127.0.0.1:{port}"
advertise_host = "localhost"

[raft]
election_timeout = "300ms"
heartbeat_interval = "100ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[observability]
log_level = "warn"
"#,
            data_dir = data_dir.display(),
            cert = cert_path.display(),
            key = key_path.display(),
            ca = ca_path.display(),
        );
        std::fs::write(&config_path, toml).expect("write config");

        Node {
            id,
            raft_id: None,
            port,
            advertise: format!("localhost:{port}"),
            cluster_id,
            dir,
            config_path,
            booted: None,
        }
    }

    /// The node's hot-reload TLS store, loaded from the cert/key/ca files laid
    /// down in [`Node::new`] — the same disk-based path the daemon takes.
    fn tls_store(&self) -> Arc<TlsStore> {
        let root = self.dir.path();
        TlsStore::load(TlsPaths {
            cert: root.join("node.crt"),
            key: root.join("node.key"),
            ca: root.join("ca.crt"),
        })
        .unwrap_or_else(|e| panic!("load tls store for node {}: {e:#}", self.id))
    }

    /// Boot (or re-boot) this replica through the real config + bootstrap path.
    pub async fn boot(&mut self, overrides: CliOverrides) {
        assert!(self.booted.is_none(), "node {} already booted", self.id);
        let resolved = config::load(&self.config_path, overrides)
            .unwrap_or_else(|e| panic!("load config for node {}: {e:#}", self.id));
        let booted = bootstrap::bootstrap(resolved, self.tls_store())
            .await
            .unwrap_or_else(|e| panic!("bootstrap node {}: {e:#}", self.id));
        // Cache the minted/stamped raft identity: it survives kill/stop so
        // membership surgery can still name a dead replica (ADR 0016 step 3).
        self.raft_id = Some(booted.handle.node_id());
        self.booted = Some(booted);
    }

    /// Boot expecting failure; returns the error for assertion (identity
    /// matrix). The success value is discarded so callers can `expect_err`
    /// without `BootedCoordinator: Debug`.
    pub async fn try_boot(&self, overrides: CliOverrides) -> anyhow::Result<()> {
        let resolved = config::load(&self.config_path, overrides)?;
        bootstrap::bootstrap(resolved, self.tls_store())
            .await
            .map(|_| ())
    }

    pub fn is_booted(&self) -> bool {
        self.booted.is_some()
    }

    /// The allocate-once raft identity this replica's data directory carries
    /// (ADR 0025). Available from first boot onward, including after a kill.
    pub fn raft_id(&self) -> u64 {
        self.raft_id
            .unwrap_or_else(|| panic!("node {} was never booted: no raft identity yet", self.id))
    }

    /// This replica's storage data directory (`<tempdir>/data`), for tests
    /// that assert on durable artifacts (e.g. the installed snapshot file).
    #[allow(dead_code)]
    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    fn booted(&self) -> &BootedCoordinator {
        self.booted
            .as_ref()
            .unwrap_or_else(|| panic!("node {} is not booted", self.id))
    }

    pub fn consensus(&self) -> Arc<OpenraftConsensus> {
        Arc::clone(&self.booted().consensus)
    }

    pub fn views(&self) -> StateViews {
        self.booted().views.clone()
    }

    pub fn status_rx(&self) -> watch::Receiver<ConsensusStatus> {
        self.booted().consensus.status()
    }

    pub fn is_leader(&self) -> bool {
        self.status_rx().borrow().role.is_leader()
    }

    /// This replica's `/readyz` document (ADR 0037 §9), straight from the
    /// booted coordinator's phase — no HTTP involved.
    pub fn readyz(&self) -> coppice_api::http::ReadyzReport {
        self.booted().readyz()
    }

    pub fn summary(&self) -> ClusterSummary {
        self.booted().handle.cluster_summary()
    }

    /// Overwrite this node's config file with a different cluster id (identity
    /// matrix): a Restart must then refuse the disk it was stamped against.
    pub fn rewrite_cluster_id(&mut self, new_cluster_id: ClusterId) {
        let raw = std::fs::read_to_string(&self.config_path).expect("read config");
        let replaced = raw.replace(
            &format!("cluster_id = \"{}\"", self.cluster_id),
            &format!("cluster_id = \"{new_cluster_id}\""),
        );
        assert_ne!(raw, replaced, "cluster_id line not found to rewrite");
        std::fs::write(&self.config_path, replaced).expect("rewrite config");
        self.cluster_id = new_cluster_id;
    }

    /// Ordered graceful shutdown mirroring the daemon's shutdown tail: stop the
    /// transport, then consensus, then release handles. The tempdir survives so
    /// the replica can re-boot from its own disk.
    pub async fn graceful_stop(&mut self) {
        let BootedCoordinator {
            cluster_id: _,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client: _,
            raft_server_shutdown,
            raft_server,
            ..
        } = self.booted.take().expect("node booted");
        let _ = raft_server_shutdown.send(());
        let _ = raft_server.await;
        let _ = handle.shutdown().await;
        drop(consensus);
        drop(views);
        drop(event_tap);
    }

    /// Abrupt death: abort the transport task so the listener dies without a
    /// graceful drain — peers simply see the node vanish. Local consensus is
    /// then shut down to release resources. The tempdir survives.
    pub async fn kill(&mut self) {
        let BootedCoordinator {
            cluster_id: _,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client: _,
            raft_server_shutdown,
            raft_server,
            ..
        } = self.booted.take().expect("node booted");
        raft_server.abort();
        drop(raft_server_shutdown);
        let _ = handle.shutdown().await;
        drop(consensus);
        drop(views);
        drop(event_tap);
    }
}

/// Poll `cond` until it returns true or `deadline` elapses, panicking with
/// `label` on expiry. The synchronization primitive for the whole suite: no
/// test blocks on a bare sleep.
pub async fn poll<F, Fut>(deadline: Duration, label: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if cond().await {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("timed out after {deadline:?} waiting for: {label}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A single bootstrapped coordinator replica **with its agent-facing task
/// runtime running** — what the agent↔coordinator protocol test drives.
///
/// [`Node`] boots `bootstrap::bootstrap` but stops there (the multi-node test
/// only needs consensus + the Raft/admin transport). This wrapper goes one step
/// further: it binds the agent gateway's mTLS listener on its own free port
/// (via [`AgentListener::bind`]) and runs `bootstrap::serve_runtime` — ingestion,
/// dispatch, the scheduler driver, and the agent session server — under a
/// caller-owned shutdown watch, so a test can boot it, drive a real agent
/// against `agent_endpoint`, and tear it down without raising a signal.
pub struct RunningCoordinator {
    /// Owns the tempdir (certs, config, data dir); kept alive for the run.
    _dir: TempDir,
    /// The shared consensus seam — propose commands here.
    pub consensus: Arc<OpenraftConsensus>,
    /// Published read views of applied state.
    pub views: StateViews,
    /// `localhost:PORT` the agent dials for its `AgentService` session.
    pub agent_endpoint: String,
    runtime_shutdown: watch::Sender<bool>,
    runtime_join: JoinHandle<anyhow::Result<()>>,
    handle: NodeHandle,
    raft_server_shutdown: Option<oneshot::Sender<()>>,
    raft_server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningCoordinator {
    /// Lay down a fresh single-node cluster (bootstrap intent) and start its
    /// full agent-facing runtime. The Raft/admin transport and the agent
    /// gateway each get their own free localhost port so several can run in one
    /// test process in parallel.
    pub async fn start(cluster_id: ClusterId, ca: &Ca) -> RunningCoordinator {
        let raft_port = free_port();
        let agent_port = free_port();
        let dir = tempfile::tempdir().expect("create coordinator tempdir");
        let root = dir.path();

        // One leaf serves the Raft/admin transport AND the agent gateway (both
        // reuse the node's identity, ADR 0011).
        let leaf = ca.leaf();
        let cert_path = root.join("node.crt");
        let key_path = root.join("node.key");
        let ca_path = root.join("ca.crt");
        std::fs::write(&cert_path, &leaf.cert_pem).expect("write cert");
        std::fs::write(&key_path, &leaf.key_pem).expect("write key");
        std::fs::write(&ca_path, &ca.pem).expect("write ca");

        let data_dir = root.join("data");
        let config_path = root.join("coordinator.toml");
        let toml = format!(
            r#"cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

[discovery]
backend = "static"

[discovery.static]
addrs = []

[listen]
raft_addr = "127.0.0.1:{raft_port}"
advertise_host = "localhost"

[raft]
election_timeout = "300ms"
heartbeat_interval = "100ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[observability]
log_level = "warn"
"#,
            data_dir = data_dir.display(),
            cert = cert_path.display(),
            key = key_path.display(),
            ca = ca_path.display(),
        );
        std::fs::write(&config_path, toml).expect("write config");

        let resolved = config::load(
            &config_path,
            CliOverrides {
                bootstrap: true,
                join: false,
            },
        )
        .expect("load coordinator config");

        // One shared hot-reload store for the raft/admin server and the agent
        // gateway (both reuse the node's identity, ADR 0011 / ADR 0037 §4).
        let tls_store = TlsStore::load(TlsPaths {
            cert: cert_path.clone(),
            key: key_path.clone(),
            ca: ca_path.clone(),
        })
        .expect("load coordinator tls store");

        let booted = bootstrap::bootstrap(resolved, Arc::clone(&tls_store))
            .await
            .expect("bootstrap coordinator");
        // The real readiness endpoint over this replica's published phase, so
        // `/readyz` in these tests answers what the daemon's would.
        let readyz = booted.readyz_endpoint();
        let BootedCoordinator {
            cluster_id,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client,
            raft_server_shutdown,
            raft_server,
            ..
        } = booted;

        // Bind the agent gateway listener on our own free port (bootstrap
        // itself never binds it — only the daemon `run` path does).
        let agent_addr = format!("127.0.0.1:{agent_port}")
            .parse()
            .expect("agent socket addr");
        let listener = AgentListener::bind(agent_addr, tls_store).expect("bind agent listener");
        // Client API listener on an ephemeral port so parallel tests never
        // collide on the default.
        let client_listener = ClientListener::bind("127.0.0.1:0".parse().expect("client addr"))
            .await
            .expect("bind client API listener");

        let (runtime_shutdown, shutdown_rx) = watch::channel(false);
        // A detached (non-installing) recorder, so several replicas in one test
        // process never race on the process-global recorder slot (issue #46).
        let metrics = coppice_api::http::MetricsEndpoint::detached_for_tests();
        let runtime_join = tokio::spawn(bootstrap::serve_runtime(
            Arc::clone(&consensus),
            views.clone(),
            event_tap,
            handle.clone(),
            listener,
            client_listener,
            cluster_id,
            node_log_client,
            metrics,
            readyz,
            Some(shutdown_rx),
        ));

        RunningCoordinator {
            _dir: dir,
            consensus,
            views,
            agent_endpoint: format!("localhost:{agent_port}"),
            runtime_shutdown,
            runtime_join,
            handle,
            raft_server_shutdown: Some(raft_server_shutdown),
            raft_server,
        }
    }

    pub fn consensus(&self) -> Arc<OpenraftConsensus> {
        Arc::clone(&self.consensus)
    }

    pub fn views(&self) -> StateViews {
        self.views.clone()
    }

    pub fn is_leader(&self) -> bool {
        self.consensus.status().borrow().role.is_leader()
    }

    /// Ordered teardown mirroring the daemon shutdown tail: drain the task
    /// runtime (agent + leader loops), then the Raft/admin transport, then
    /// consensus.
    pub async fn shutdown(mut self) {
        let _ = self.runtime_shutdown.send(true);
        let _ = self.runtime_join.await;
        if let Some(tx) = self.raft_server_shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.raft_server.await;
        let _ = self.handle.shutdown().await;
        drop(self.consensus);
    }
}

/// Wait for a replica's published views to reach `min_index` AND show
/// `cluster_version` — i.e. it has applied the same committed state.
pub async fn wait_converged(
    views: StateViews,
    min_index: u64,
    cluster_version: u32,
    deadline: Duration,
    label: &str,
) {
    poll(deadline, label, move || {
        let views = views.clone();
        async move {
            let view = views.latest();
            view.applied_index() >= min_index && view.state().cluster_version == cluster_version
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// A whole daemon, for the ADR 0037 §1 lifecycle
// ---------------------------------------------------------------------------

/// A coordinator daemon driven through [`bootstrap::run_with`] — the same code
/// the binary runs, minus the process-global recorder and the signal handler.
///
/// [`Node`] and [`RunningCoordinator`] hand-assemble a replica that already has
/// a cluster; this one does not assume there is one. It is what the formation
/// tests need: a daemon that can park, be `init`ed over its admin socket, and
/// keep serving on the same ports afterwards — plus one that can be stopped,
/// have its data directory tampered with, and restarted, which is how the
/// crash-mid-formation cases are staged.
pub struct Daemon {
    pub cluster_id: ClusterId,
    dir: TempDir,
    config_path: PathBuf,
    /// The CA whose leaf this daemon starts with. Formation replaces the
    /// `[tls]` material with cluster-minted material, so a client that dialed
    /// with this CA before `init` must re-dial with the cluster's afterwards.
    ca_pem: Vec<u8>,
    client_port: u16,
    raft_port: u16,
    running: Option<RunningDaemon>,
}

struct RunningDaemon {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<anyhow::Result<()>>,
}

impl Daemon {
    /// Lay down a fresh daemon's tempdir (certs + config) without starting it.
    ///
    /// Every port is allocated explicitly rather than with `:0`, because these
    /// tests dial the daemon from outside and must know where it is before it
    /// has told anyone anything.
    pub fn new(cluster_id: ClusterId, ca: &Ca) -> Daemon {
        Daemon::with_material(cluster_id, ca, true)
    }

    /// A daemon with **no TLS material on disk** — the ADR 0037 §4 minimal
    /// deployment, where nothing is provisioned and formation mints the first
    /// certificates. The config still names the `[tls]` paths; formation
    /// writes into them.
    pub fn new_certless(cluster_id: ClusterId, ca: &Ca) -> Daemon {
        Daemon::with_material(cluster_id, ca, false)
    }

    fn with_material(cluster_id: ClusterId, ca: &Ca, write_material: bool) -> Daemon {
        let dir = tempfile::tempdir().expect("create daemon tempdir");
        let root = dir.path();
        if write_material {
            let leaf = ca.leaf();
            std::fs::write(root.join("node.crt"), &leaf.cert_pem).expect("write cert");
            std::fs::write(root.join("node.key"), &leaf.key_pem).expect("write key");
            std::fs::write(root.join("ca.crt"), &ca.pem).expect("write ca");
        }

        let client_port = free_port();
        let raft_port = free_port();
        let agent_port = free_port();
        let config_path = root.join("coordinator.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

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

[raft]
election_timeout = "300ms"
heartbeat_interval = "100ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[observability]
log_level = "warn"
"#,
                data_dir = root.join("data").display(),
                cert = root.join("node.crt").display(),
                key = root.join("node.key").display(),
                ca = root.join("ca.crt").display(),
            ),
        )
        .expect("write config");

        Daemon {
            cluster_id,
            dir,
            config_path,
            ca_pem: ca.pem.clone(),
            client_port,
            raft_port,
            running: None,
        }
    }

    /// Point this daemon's `[discovery.static]` at `addrs`, so its
    /// pre-formation probe round has somewhere to look.
    pub fn set_static_discovery(&self, addrs: &[String]) {
        let quoted: Vec<String> = addrs.iter().map(|a| format!("\"{a}\"")).collect();
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        let updated = toml.replace("addrs = []", &format!("addrs = [{}]", quoted.join(", ")));
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Start (or restart) the daemon. Returns once `run_with` has been spawned;
    /// callers poll a surface to learn when it is serving.
    pub fn start(&mut self, overrides: CliOverrides) {
        assert!(self.running.is_none(), "daemon already running");
        let resolved = config::load(&self.config_path, overrides).expect("load daemon config");
        let (shutdown, shutdown_rx) = watch::channel(false);
        let join = tokio::spawn(bootstrap::run_with(
            resolved,
            coppice_api::http::MetricsEndpoint::detached_for_tests(),
            Some(shutdown_rx),
        ));
        self.running = Some(RunningDaemon { shutdown, join });
    }

    /// Stop the daemon and return what `run_with` returned — `Err` for the
    /// `formation-failed` fail-stop, `Ok` otherwise.
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        let running = self.running.take().expect("daemon is not running");
        let _ = running.shutdown.send(true);
        running.join.await.expect("daemon task joined")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    pub fn admin_socket(&self) -> PathBuf {
        self.data_dir().join("admin.sock")
    }

    pub fn config_path(&self) -> PathBuf {
        self.config_path.clone()
    }

    /// `localhost:PORT` for the raft/admin listener — what a peer would dial.
    pub fn raft_target(&self) -> String {
        format!("localhost:{}", self.raft_port)
    }

    /// The bound client-listener address, for tests that need a raw socket.
    pub fn client_addr(&self) -> String {
        format!("127.0.0.1:{}", self.client_port)
    }

    pub fn api(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.client_port)
    }

    /// Wipe the data directory, the documented recovery from a failed
    /// formation (ADR 0037 §3).
    pub fn wipe_data_dir(&self) {
        let dir = self.data_dir();
        if dir.exists() {
            std::fs::remove_dir_all(&dir).expect("wipe data dir");
        }
    }

    /// The CA the daemon trusted before formation.
    pub fn bootstrap_ca_pem(&self) -> Vec<u8> {
        self.ca_pem.clone()
    }

    /// Poll `/readyz` until it answers, returning `(status, body)`.
    pub async fn readyz(&self) -> (u16, serde_json::Value) {
        let client = reqwest::Client::new();
        let url = self.api("/readyz");
        let mut last = None;
        for _ in 0..200 {
            match client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body: serde_json::Value = resp.json().await.expect("readyz json");
                    return (status, body);
                }
                Err(e) => {
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
        panic!("/readyz never answered: {last:?}");
    }

    /// Wait until `/readyz` reports `phase`, returning the body.
    pub async fn await_phase(&self, phase: &str) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (_, body) = self.readyz().await;
            if body["phase"] == phase {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "daemon never reached phase {phase}; last body: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// One call on the daemon's admin socket, retrying while it binds.
    pub async fn admin(
        &self,
        call: coppice_coordinator::localadmin::AdminCall,
    ) -> coppice_coordinator::localadmin::AdminReply {
        let socket = self.admin_socket();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match coppice_coordinator::localadmin::call(&socket, call.clone()).await {
                Ok(reply) => return reply,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "admin socket {} never answered: {e:#}",
                        socket.display()
                    );
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }

    /// Probe this daemon over the mTLS admin plane, presenting `cert`/`key`
    /// against `ca`.
    pub async fn probe(
        &self,
        ca_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> anyhow::Result<coppice_proto::pb::raft::v1::ProbeClusterResponse> {
        // Bounded: a certless daemon's raft port is bound but unserved, so an
        // unbounded dial sits in the accept backlog forever waiting for a TLS
        // handshake no one performs — the same reason the real probe client
        // (`probe.rs`) carries a deadline.
        let attempt = async {
            let mut client = coppice_coordinator::admin::admin_channel(
                &self.raft_target(),
                ca_pem,
                cert_pem,
                key_pem,
            )
            .await?;
            let resp = client
                .probe_cluster(coppice_proto::pb::raft::v1::ProbeClusterRequest {
                    cluster_id: self.cluster_id.to_string(),
                })
                .await
                .map_err(|s| {
                    anyhow::anyhow!("ProbeCluster failed ({:?}): {}", s.code(), s.message())
                })?;
            anyhow::Ok(resp.into_inner())
        };
        tokio::time::timeout(Duration::from_secs(3), attempt)
            .await
            .map_err(|_| anyhow::anyhow!("probe timed out"))?
    }

    /// Attempt a membership verb, so a test can assert the ADR 0037 §3
    /// refusal directly rather than inferring it.
    pub async fn try_add_learner(
        &self,
        ca_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> anyhow::Result<()> {
        let mut client = coppice_coordinator::admin::admin_channel(
            &self.raft_target(),
            ca_pem,
            cert_pem,
            key_pem,
        )
        .await?;
        coppice_coordinator::admin::add_learner(
            &mut client,
            *self.cluster_id.0.as_bytes(),
            42,
            "localhost:1".to_string(),
        )
        .await
    }
}
