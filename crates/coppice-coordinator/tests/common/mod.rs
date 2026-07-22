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
use coppice_coordinator::config;

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
        self.profiled_leaf(cn, None, extra_sans)
    }

    /// Issue a **coordinator machine** leaf (ADR 0037 §6):
    /// `OU=coppice-coordinator`, `CN` = the stable machine identity. The self-
    /// service grant keys on this profile, so a self-join must present one.
    pub fn machine_leaf(&self, machine_identity: &str) -> Leaf {
        self.profiled_leaf(machine_identity, Some("coppice-coordinator"), &[])
    }

    /// Issue an **operator-profile** leaf (ADR 0037 §6): `OU=coppice-operator`,
    /// authorized for every membership verb including `RemoveNode` and
    /// `InitializeCluster`.
    pub fn operator_leaf(&self) -> Leaf {
        self.profiled_leaf("coppice-test-operator", Some("coppice-operator"), &[])
    }

    /// Shared leaf mint: `cn`, an optional `OU` profile marker, extra SANs.
    fn profiled_leaf(&self, cn: &str, ou: Option<&str>, extra_sans: &[String]) -> Leaf {
        let key = KeyPair::generate().expect("generate leaf key pair");
        let mut sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        sans.extend(extra_sans.iter().cloned());
        let mut params = CertificateParams::new(sans).expect("leaf params");
        params.distinguished_name.push(DnType::CommonName, cn);
        if let Some(ou) = ou {
            params
                .distinguished_name
                .push(DnType::OrganizationalUnitName, ou);
        }
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

/// Build a hot-reload TLS store from a node's on-disk material
/// (`node.crt`/`node.key`/`ca.crt` under `root`), matching what
/// `bootstrap::run` builds from the config `[tls]` paths (ADR 0037 §6).
pub fn tls_store(root: &std::path::Path) -> Arc<coppice_tls::TlsStore> {
    coppice_tls::TlsStore::load(coppice_tls::TlsPaths {
        cert: root.join("node.crt"),
        key: root.join("node.key"),
        ca: root.join("ca.crt"),
    })
    .expect("load test tls store")
}

/// Build a hot-reload TLS store from in-memory PEM (the paths are recorded only
/// for error messages), for tests that mint a leaf and hand it straight to a
/// [`NodeClient`] without going through disk.
pub fn tls_store_from_pem(ca: &[u8], cert: &[u8], key: &[u8]) -> Arc<coppice_tls::TlsStore> {
    coppice_tls::TlsStore::from_pem(
        coppice_tls::TlsPaths {
            cert: "in-memory-cert".into(),
            key: "in-memory-key".into(),
            ca: "in-memory-ca".into(),
        },
        ca.to_vec(),
        cert.to_vec(),
        key.to_vec(),
    )
    .expect("build tls store from pem")
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
    /// This replica's machine identity (ADR 0037 §6): the CN of its serving
    /// leaf, `coord-<id>`. Endpoint verification checks a self-join's cert CN
    /// against this, so tests self-join with `ca.machine_leaf(&node.machine)`.
    pub machine: String,
    pub cluster_id: ClusterId,
    #[allow(dead_code)]
    dir: TempDir,
    config_path: PathBuf,
    booted: Option<BootedCoordinator>,
    /// A minimal `/readyz`-only HTTP server task, spawned on demand by
    /// [`Node::serve_readyz`] so a multi-node convergence test can assert the
    /// real HTTP readiness gate (ADR 0037 §7). Aborted at stop/kill.
    readyz_server: Option<JoinHandle<()>>,
}

/// Which discovery backend a harness [`Node`] is configured with (ADR 0037 §2).
pub enum DiscoverySpec {
    /// `[discovery.static]` with a literal seed list — the hand-driven default.
    Static(Vec<String>),
    /// `[discovery.file]` pointing at a shared registration directory: the
    /// dev/multi-process story, where each process registers itself and
    /// discovers the others through one shared directory with no other harness
    /// coordination.
    File(PathBuf),
}

/// A harness [`Node`]'s tunable knobs beyond identity: its discovery backend and
/// the two membership grace periods (ADR 0037 §5/§6). The grace periods default
/// to the production 60s; failure-mode tests shorten them so overflow removal
/// and stale-learner eviction are reachable inside a test deadline.
pub struct NodeSpec {
    pub id: u64,
    pub cluster_id: ClusterId,
    pub discovery: DiscoverySpec,
    pub removal_grace: Option<Duration>,
    pub replacement_grace: Option<Duration>,
}

impl NodeSpec {
    /// A spec with the given discovery backend and default (production) graces.
    pub fn new(id: u64, cluster_id: ClusterId, discovery: DiscoverySpec) -> NodeSpec {
        NodeSpec {
            id,
            cluster_id,
            discovery,
            removal_grace: None,
            replacement_grace: None,
        }
    }
}

impl Node {
    /// Lay down a fresh replica's tempdir (certs + config), without booting,
    /// with an empty `static` discovery view (the harness hand-drives
    /// membership).
    pub fn new(id: u64, cluster_id: ClusterId, ca: &Ca) -> Node {
        Node::new_with_seeds(id, cluster_id, ca, &[])
    }

    /// As [`Node::new`], but seed the `static` discovery backend with `seeds`
    /// (ADR 0037 §2) so this replica's convergence loop can find and self-join
    /// the cluster with no hand-driven admin calls — the park→join path.
    pub fn new_with_seeds(id: u64, cluster_id: ClusterId, ca: &Ca, seeds: &[String]) -> Node {
        Node::with_spec(
            NodeSpec::new(id, cluster_id, DiscoverySpec::Static(seeds.to_vec())),
            ca,
        )
    }

    /// As [`Node::new_with_seeds`], but with the `file` discovery backend
    /// pointing at the shared `dir` (ADR 0037 §2): the process registers itself
    /// there on boot and discovers its peers by enumerating the same directory —
    /// N identical-in-shape configs, the dev/multi-process convergence story.
    pub fn new_file_discovery(id: u64, cluster_id: ClusterId, ca: &Ca, dir: PathBuf) -> Node {
        Node::with_spec(NodeSpec::new(id, cluster_id, DiscoverySpec::File(dir)), ca)
    }

    /// Lay down a fresh replica's tempdir from a full [`NodeSpec`]: discovery
    /// backend plus optional short membership grace periods (ADR 0037 §5/§6).
    pub fn with_spec(spec: NodeSpec, ca: &Ca) -> Node {
        let NodeSpec {
            id,
            cluster_id,
            discovery,
            removal_grace,
            replacement_grace,
        } = spec;
        let port = free_port();
        let dir = tempfile::tempdir().expect("create node tempdir");
        let root = dir.path();

        // Serve a coordinator machine leaf whose CN is this replica's machine
        // identity (ADR 0037 §6), so endpoint verification of a self-join under
        // the same identity succeeds. Two nodes given the same `id` therefore
        // share a machine identity — exactly the same-installation replacement
        // shape the failure-mode tests need.
        let machine = format!("coord-{id}");
        let leaf = ca.machine_leaf(&machine);
        let cert_path = root.join("node.crt");
        let key_path = root.join("node.key");
        let ca_path = root.join("ca.crt");
        std::fs::write(&cert_path, &leaf.cert_pem).expect("write cert");
        std::fs::write(&key_path, &leaf.key_pem).expect("write key");
        std::fs::write(&ca_path, &ca.pem).expect("write ca");

        // `backend` and the grace periods are `[discovery]` fields, so they must
        // precede the `[discovery.<backend>]` sub-table (otherwise TOML nests
        // them under it and rejects the unknown key).
        let (backend_line, sub_table) = match &discovery {
            DiscoverySpec::Static(seeds) => (
                "backend = \"static\"".to_string(),
                format!(
                    "[discovery.static]\naddrs = [{}]\n",
                    seeds
                        .iter()
                        .map(|s| format!("\"{s}\""))
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            ),
            DiscoverySpec::File(dir) => (
                "backend = \"file\"".to_string(),
                format!("[discovery.file]\ndir = \"{}\"\n", dir.display()),
            ),
        };
        let grace_toml = {
            let mut s = String::new();
            if let Some(g) = removal_grace {
                s.push_str(&format!("removal_grace = \"{}ms\"\n", g.as_millis()));
            }
            if let Some(g) = replacement_grace {
                s.push_str(&format!("replacement_grace = \"{}ms\"\n", g.as_millis()));
            }
            s
        };

        let data_dir = root.join("data");
        let config_path = root.join("coordinator.toml");
        let toml = format!(
            r#"cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

[discovery]
{backend_line}
{grace_toml}{sub_table}
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
            machine,
            cluster_id,
            dir,
            config_path,
            booted: None,
            readyz_server: None,
        }
    }

    /// Boot (or re-boot) this replica through the real config + bootstrap path.
    /// Intent is derived from the disk (ADR 0037 §1): a fresh replica parks; a
    /// re-boot from disk resumes.
    pub async fn boot(&mut self) {
        assert!(self.booted.is_none(), "node {} already booted", self.id);
        let resolved = config::load(&self.config_path)
            .unwrap_or_else(|e| panic!("load config for node {}: {e:#}", self.id));
        let store = tls_store(self.dir.path());
        let booted = bootstrap::bootstrap(resolved, store)
            .await
            .unwrap_or_else(|e| panic!("bootstrap node {}: {e:#}", self.id));
        // Cache the minted/stamped raft identity: it survives kill/stop so
        // membership surgery can still name a dead replica (ADR 0016 step 3).
        self.raft_id = Some(booted.handle.node_id());
        self.booted = Some(booted);
    }

    /// Form this booted replica into the founding single-voter cluster
    /// (ADR 0037 §3), driving `FormationControl::form` in-process with `token`.
    pub async fn form(&self, token: &str) {
        self.booted()
            .formation
            .form(token)
            .await
            .unwrap_or_else(|e| panic!("form node {}: {e:#}", self.id));
    }

    /// Boot expecting failure; returns the error for assertion (identity
    /// matrix). The success value is discarded so callers can `expect_err`
    /// without `BootedCoordinator: Debug`.
    pub async fn try_boot(&self) -> anyhow::Result<()> {
        let resolved = config::load(&self.config_path)?;
        bootstrap::bootstrap(resolved, tls_store(self.dir.path()))
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

    pub fn summary(&self) -> ClusterSummary {
        self.booted().handle.cluster_summary()
    }

    /// The current convergence phase (ADR 0037 §4/§7) from the published watch.
    pub fn convergence_phase(&self) -> coppice_coordinator::convergence::Phase {
        self.booted().convergence_status.borrow().phase
    }

    /// Whether this replica is currently a voter in its own membership view.
    pub fn is_voter(&self) -> bool {
        self.summary()
            .members
            .iter()
            .any(|m| m.id == self.raft_id() && m.voter)
    }

    /// Start a minimal `/readyz`-only HTTP server on an ephemeral port over this
    /// replica's captured [`coppice_api::http::ReadyzEndpoint`] (ADR 0037 §7) and
    /// return its `http://127.0.0.1:PORT` base. This is the same `/readyz` route
    /// the daemon mounts on its client listener, served in isolation so a
    /// multi-node convergence test can assert the real HTTP readiness gate on a
    /// live cluster without standing up the whole control plane. Aborted on
    /// stop/kill.
    pub async fn serve_readyz(&mut self) -> String {
        assert!(
            self.readyz_server.is_none(),
            "node {} already serving /readyz",
            self.id
        );
        let endpoint = self.booted().readyz.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind readyz listener");
        let addr = listener.local_addr().expect("readyz local addr");
        let app = coppice_api::http::readyz_router(endpoint);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        self.readyz_server = Some(server);
        format!("http://{addr}")
    }

    /// Abort just the convergence loop, leaving consensus and the raft/admin
    /// transport serving. Models an instance that will not (re)join — a
    /// decommissioned predecessor whose process is going away — so a
    /// same-machine-identity replacement does not war with it over the seat
    /// after the leader retires it (ADR 0037 §5/§6).
    pub fn stop_convergence(&mut self) {
        self.booted().convergence.abort();
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
        if let Some(server) = self.readyz_server.take() {
            server.abort();
            let _ = server.await;
        }
        let BootedCoordinator {
            cluster_id: _,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client: _,
            raft_server_shutdown,
            raft_server,
            formation: _,
            convergence_status: _,
            convergence,
            readyz: _,
            file_registration: _,
        } = self.booted.take().expect("node booted");
        convergence.abort();
        let _ = convergence.await;
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
        if let Some(server) = self.readyz_server.take() {
            server.abort();
            let _ = server.await;
        }
        let BootedCoordinator {
            cluster_id: _,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client: _,
            raft_server_shutdown,
            raft_server,
            formation: _,
            convergence_status: _,
            convergence,
            readyz: _,
            file_registration: _,
        } = self.booted.take().expect("node booted");
        convergence.abort();
        let _ = convergence.await;
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
    /// `http://127.0.0.1:PORT` base of the client API listener (ADR 0031),
    /// where `/readyz` and `/metrics` are served (ADR 0037 §7).
    pub client_endpoint: String,
    runtime_shutdown: watch::Sender<bool>,
    runtime_join: JoinHandle<anyhow::Result<()>>,
    handle: NodeHandle,
    convergence: JoinHandle<()>,
    raft_server_shutdown: Option<oneshot::Sender<()>>,
    raft_server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningCoordinator {
    /// Lay down a fresh single-node cluster (bootstrap intent) and start its
    /// full agent-facing runtime, forming it into a single-voter cluster. The
    /// Raft/admin transport and the agent gateway each get their own free
    /// localhost port so several can run in one test process in parallel.
    pub async fn start(cluster_id: ClusterId, ca: &Ca) -> RunningCoordinator {
        RunningCoordinator::start_inner(cluster_id, ca, true).await
    }

    /// As [`RunningCoordinator::start`], but leave the replica **parked**
    /// (booted, its runtime and API listener serving, but never formed) so a
    /// test can observe the `waiting` phase / 503 `/readyz` of a fresh instance
    /// (ADR 0037 §1/§7).
    pub async fn start_parked(cluster_id: ClusterId, ca: &Ca) -> RunningCoordinator {
        RunningCoordinator::start_inner(cluster_id, ca, false).await
    }

    async fn start_inner(cluster_id: ClusterId, ca: &Ca, form: bool) -> RunningCoordinator {
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

        let resolved = config::load(&config_path).expect("load coordinator config");

        let BootedCoordinator {
            cluster_id,
            consensus,
            views,
            event_tap,
            handle,
            node_log_client,
            raft_server_shutdown,
            raft_server,
            formation,
            convergence_status: _,
            convergence,
            readyz,
            file_registration: _,
        } = bootstrap::bootstrap(resolved, tls_store(root))
            .await
            .expect("bootstrap coordinator");

        // Single-node cluster: form it in-process (ADR 0037 §3), the way
        // `coppice dev` does, so the runtime has a leader to serve against.
        // A parked variant skips this to exercise the `waiting` phase.
        if form {
            formation
                .form("running-coordinator-formation")
                .await
                .expect("form single-node cluster");
        }

        // Bind the agent gateway listener on our own free port (bootstrap
        // itself never binds it — only the daemon `run` path does).
        let agent_addr = format!("127.0.0.1:{agent_port}")
            .parse()
            .expect("agent socket addr");
        let listener =
            AgentListener::bind(agent_addr, tls_store(root)).expect("bind agent listener");
        // Client API listener on an ephemeral port so parallel tests never
        // collide on the default.
        let client_listener = ClientListener::bind("127.0.0.1:0".parse().expect("client addr"))
            .await
            .expect("bind client API listener");
        let client_addr = client_listener
            .local_addr()
            .expect("client API listener local addr");

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
            client_endpoint: format!("http://{client_addr}"),
            runtime_shutdown,
            runtime_join,
            handle,
            convergence,
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
        self.convergence.abort();
        let _ = self.convergence.await;
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
