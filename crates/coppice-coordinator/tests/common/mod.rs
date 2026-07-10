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

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use coppice_core::id::ClusterId;

use coppice_consensus::{
    ClusterSummary, Consensus, ConsensusStatus, NodeHandle, OpenraftConsensus, StateViews,
};
use coppice_coordinator::bootstrap::{self, AgentListener, BootedCoordinator};
use coppice_coordinator::config::{self, CliOverrides};

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
        let key = KeyPair::generate().expect("generate leaf key pair");
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .expect("leaf params");
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
peers = []

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

    /// Boot (or re-boot) this replica through the real config + bootstrap path.
    pub async fn boot(&mut self, overrides: CliOverrides) {
        assert!(self.booted.is_none(), "node {} already booted", self.id);
        let resolved = config::load(&self.config_path, overrides)
            .unwrap_or_else(|e| panic!("load config for node {}: {e:#}", self.id));
        let booted = bootstrap::bootstrap(resolved)
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
        bootstrap::bootstrap(resolved).await.map(|_| ())
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
            consensus,
            views,
            event_tap,
            handle,
            raft_server_shutdown,
            raft_server,
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
            consensus,
            views,
            event_tap,
            handle,
            raft_server_shutdown,
            raft_server,
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
peers = []

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

        let BootedCoordinator {
            consensus,
            views,
            event_tap,
            handle,
            raft_server_shutdown,
            raft_server,
        } = bootstrap::bootstrap(resolved)
            .await
            .expect("bootstrap coordinator");

        // Bind the agent gateway listener on our own free port (bootstrap
        // itself never binds it — only the daemon `run` path does).
        let agent_addr = format!("127.0.0.1:{agent_port}")
            .parse()
            .expect("agent socket addr");
        let listener = AgentListener::bind(agent_addr, &leaf.cert_pem, &leaf.key_pem, &ca.pem)
            .expect("bind agent listener");

        let (runtime_shutdown, shutdown_rx) = watch::channel(false);
        let runtime_join = tokio::spawn(bootstrap::serve_runtime(
            Arc::clone(&consensus),
            views.clone(),
            event_tap,
            listener,
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
