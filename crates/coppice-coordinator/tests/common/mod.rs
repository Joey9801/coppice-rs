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

use coppice_core::id::{ClusterId, MachineId, NodeId};
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
use coppice_tls::{pki, TlsPaths, TlsStore};

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
        self.leaf_with_subject(cn, None, extra_sans)
    }

    /// Issue an **operator** leaf: `OU=coppice-operators` (ADR 0022), which is
    /// the profile the admin surface's membership verbs require (ADR 0037 §7).
    ///
    /// [`Ca::leaf`] carries no `OU` at all, so the classifier reads it as an
    /// agent leaf and then fails to parse its CN as a node id — correct, and
    /// the reason a fixture that administers membership must ask for this one.
    pub fn operator_leaf(&self) -> Leaf {
        self.leaf_with_subject("coppice-test-operator", Some(pki::OPERATOR_OU), &[])
    }

    /// Issue a **coordinator machine** leaf: `OU=coppice-coordinator` with a
    /// [`MachineId`] CN, which is what `verify_leaf` classifies as
    /// `Profile::Coordinator` (ADR 0037 §4) and therefore what the §7 machine
    /// self-service grant is keyed on. The refusal-matrix tests mint these to
    /// present a machine identity of their choosing — one the cluster has
    /// bound, or one it has never seen.
    pub fn coordinator_leaf(&self, machine: &MachineId) -> Leaf {
        self.leaf_with_subject(&machine.to_string(), Some(pki::COORDINATOR_OU), &[])
    }

    /// Issue an **agent** leaf: no `OU`, CN = a node id — classified as
    /// `Profile::Agent`. Agents hold none of the membership surface (ADR 0037
    /// §7), which is exactly what the refusal matrix asserts with one of these.
    pub fn agent_leaf(&self, node: &NodeId) -> Leaf {
        self.leaf_with_cn(&node.to_string())
    }

    /// This CA's own private key, PEM-encoded — the material `pki::write_ca_key`
    /// persists to a data directory that is meant to *own* this root (ADR 0037
    /// §4). Only a test that stages cluster custody by hand (rather than
    /// forming a cluster the normal way, which mints its own CA) needs this.
    pub fn key_pem(&self) -> Vec<u8> {
        self.key.serialize_pem().into_bytes()
    }

    fn leaf_with_subject(&self, cn: &str, ou: Option<&str>, extra_sans: &[String]) -> Leaf {
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

/// Grab a localhost TCP port that will still be free when a daemon binds it.
///
/// Binding `:0` and dropping the listener is not good enough: the kernel is
/// free to hand the released port to any other process on the host — another
/// test process's own `:0` bind, or the source port of an outbound connection
/// — in the window before the daemon binds it, and CI has observed exactly
/// that ("binding agent gateway listener: Address already in use"). Reallocating
/// on failure is no fix either, because a daemon's ports are cross-baked into
/// peer configs (static discovery, the fleet enrollment endpoint) and, once
/// formed, into replicated raft membership.
///
/// So the race is removed at allocation instead, with two defenses:
/// 1. ports come from a range *below* both the Linux (32768+) and macOS
///    (49152+) ephemeral defaults, so with the default ranges the kernel
///    never lands `:0` binds or outbound source ports there — only an
///    explicit bind can collide (a host whose ephemeral range was widened,
///    e.g. via `ip_local_port_range`, loses this defense but keeps the
///    others);
/// 2. each port is claimed via an exclusive advisory lock on a per-port file
///    in a host-shared directory, held for the life of this test process, so
///    concurrent test processes never pick the same port. The lock dies with
///    the process; crashed runs leak nothing.
///
/// A bind probe then filters ports an unrelated service already owns.
pub fn free_port() -> u16 {
    use nix::fcntl::{Flock, FlockArg};
    use std::hash::{BuildHasher as _, Hasher as _};
    use std::sync::Mutex;

    const RANGE_START: u32 = 20_000;
    const RANGE_LEN: u32 = 10_000;
    /// The claims this process holds, kept locked until process exit.
    static CLAIMED: Mutex<Vec<Flock<std::fs::File>>> = Mutex::new(Vec::new());

    let dir = std::env::temp_dir().join("coppice-test-ports");
    std::fs::create_dir_all(&dir).expect("create the port-claim directory");

    // A random starting point, so concurrent processes probe disjoint runs of
    // the range instead of all contending on the same first candidates.
    let mut seed = std::collections::hash_map::RandomState::new().build_hasher();
    seed.write_u32(std::process::id());
    let start = seed.finish() as u32 % RANGE_LEN;

    for i in 0..RANGE_LEN {
        let port = (RANGE_START + (start + i) % RANGE_LEN) as u16;
        // Unwritable claim file (another user's leftover in a shared /tmp):
        // not ours to take, move on.
        let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(format!("{port}.lock")))
        else {
            continue;
        };
        // Held by another live test process (or an earlier claim of our own).
        let Ok(lock) = Flock::lock(file, FlockArg::LockExclusiveNonblock) else {
            continue;
        };
        // Owned by an unrelated service; dropping `lock` releases the claim.
        if TcpListener::bind(("127.0.0.1", port)).is_err() {
            continue;
        }
        CLAIMED.lock().unwrap().push(lock);
        return port;
    }
    panic!("no free port in {RANGE_START}..{}", RANGE_START + RANGE_LEN);
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
    /// The machine identity this node's serving leaf presents, when it serves
    /// a coordinator-profile leaf (the [`Node::new`] default). Dial-back
    /// verification (ADR 0037 §6 step 3) classifies the serving certificate,
    /// so an admissible fixture node must present one; `None` only for a node
    /// built via [`Node::new_with_leaf`] around some other subject.
    pub machine: Option<MachineId>,
    #[allow(dead_code)]
    dir: TempDir,
    config_path: PathBuf,
    booted: Option<BootedCoordinator>,
}

impl Node {
    /// Lay down a fresh replica's tempdir (certs + config), without booting.
    ///
    /// The node serves a **coordinator-profile** leaf carrying a fresh machine
    /// identity (ADR 0037 §4): admission dial-back-verifies the serving
    /// certificate at a joiner's advertised address and binds the identity it
    /// presents (§6/§7), so the default fixture must be an admissible one.
    pub fn new(id: u64, cluster_id: ClusterId, ca: &Ca) -> Node {
        let machine = MachineId::new();
        Node::with_leaf(
            id,
            cluster_id,
            ca,
            ca.coordinator_leaf(&machine),
            Some(machine),
        )
    }

    /// As [`Node::new`], but serving an explicit leaf — e.g. the profile-less
    /// [`Ca::leaf`], for tests that need an endpoint whose serving certificate
    /// classifies as no coordinator identity at all.
    pub fn new_with_leaf(id: u64, cluster_id: ClusterId, ca: &Ca, leaf: Leaf) -> Node {
        Node::with_leaf(id, cluster_id, ca, leaf, None)
    }

    fn with_leaf(
        id: u64,
        cluster_id: ClusterId,
        ca: &Ca,
        leaf: Leaf,
        machine: Option<MachineId>,
    ) -> Node {
        let port = free_port();
        let dir = tempfile::tempdir().expect("create node tempdir");
        let root = dir.path();
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
# Generous for oversubscribed CI runners: 300ms elections flap for minutes on a
# 2-core host under load (observed: every enrollment forward bouncing between
# two voters each naming the other leader), and nothing in these suites needs
# fast failover — kills are real process deaths detected by contact evidence,
# not election timing.
election_timeout = "1s"
heartbeat_interval = "250ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[pacing]
probe_interval = "50ms"
settled_interval = "250ms"
refusal_backoff = "1s"
park_interval_min = "50ms"
park_interval_max = "250ms"
promote_poll_interval = "50ms"

# Minimal argon2 cost: these fleets mint throwaway tokens, and the production
# default costs ~300ms of KDF per hash in a debug build.
[token_kdf]
m_cost_kib = 8
t_cost = 1
p_cost = 1

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[client_tls]
# Plain HTTP on the client listener (ADR 0037 §4: the posture is always
# explicit, never implied).
insecure = true

[history]
# Explicitly lossy (ADR 0012): the integration daemons run no history store.
mode = "none"

[auth]
# Explicitly insecure: every request is an anonymous admin (issue #45).
insecure_open = true

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
            machine,
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
    ///
    /// Intent is derived from the data directory (ADR 0037 §1): the first call
    /// on a fresh `Node` forms a single-voter cluster, every later call after a
    /// stop or kill resumes the instance the first one stamped.
    pub async fn boot(&mut self) {
        self.boot_with(bootstrap::bootstrap).await
    }

    /// Boot as a *joining* replica: same as [`Node::boot`] except that an empty
    /// data directory mints a fresh learner-join instance instead of forming a
    /// cluster. The fixture that calls this then adds the node through the
    /// leader's `add_learner`, standing in for the convergence loop a real
    /// daemon would run.
    pub async fn boot_joining(&mut self) {
        self.boot_with(bootstrap::bootstrap_joining).await
    }

    async fn boot_with<F, Fut>(&mut self, entry: F)
    where
        F: FnOnce(config::ResolvedConfig, Arc<TlsStore>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<BootedCoordinator>>,
    {
        assert!(self.booted.is_none(), "node {} already booted", self.id);
        let resolved = config::load(&self.config_path)
            .unwrap_or_else(|e| panic!("load config for node {}: {e:#}", self.id));
        let booted = entry(resolved, self.tls_store())
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
    pub async fn try_boot(&self) -> anyhow::Result<()> {
        let resolved = config::load(&self.config_path)?;
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
    /// The resolved `127.0.0.1:PORT` of the client API listener (bound on `:0`),
    /// so a test can drive the daemon's *own* router — fanout ring, node handle
    /// and all — over real HTTP instead of hand-assembling a control plane.
    client_addr: std::net::SocketAddr,
    /// The coordinator's data directory (CA key + machine identity land here
    /// in tests that manufacture a cluster-owned PKI on this harness).
    pub data_dir: std::path::PathBuf,
    runtime_shutdown: watch::Sender<bool>,
    runtime_join: JoinHandle<anyhow::Result<()>>,
    /// The runtime's node handle (leadership + membership summaries).
    pub handle: NodeHandle,
    raft_server_shutdown: Option<oneshot::Sender<()>>,
    raft_server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningCoordinator {
    /// Lay down a fresh single-node cluster (bootstrap intent) and start its
    /// full agent-facing runtime. The Raft/admin transport and the agent
    /// gateway each get their own free localhost port so several can run in one
    /// test process in parallel.
    pub async fn start(cluster_id: ClusterId, ca: &Ca) -> RunningCoordinator {
        Self::start_with_housekeeping_interval(
            cluster_id,
            ca,
            coppice_coordinator::HOUSEKEEPING_INTERVAL,
        )
        .await
    }

    /// [`start`](Self::start) with the leader's housekeeping sweep on a short
    /// leash, for the one suite that has to watch a retention TTL actually
    /// expire (ADR 0012). Every other suite keeps the production 60 s cadence:
    /// a sweep that fires constantly is a background proposer competing with
    /// whatever the test under it is trying to observe, and none of them are
    /// asking about retention.
    pub async fn start_with_housekeeping_interval(
        cluster_id: ClusterId,
        ca: &Ca,
        housekeeping_interval: Duration,
    ) -> RunningCoordinator {
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
# Generous for oversubscribed CI runners: 300ms elections flap for minutes on a
# 2-core host under load (observed: every enrollment forward bouncing between
# two voters each naming the other leader), and nothing in these suites needs
# fast failover — kills are real process deaths detected by contact evidence,
# not election timing.
election_timeout = "1s"
heartbeat_interval = "250ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[pacing]
probe_interval = "50ms"
settled_interval = "250ms"
refusal_backoff = "1s"
park_interval_min = "50ms"
park_interval_max = "250ms"
promote_poll_interval = "50ms"

# Minimal argon2 cost: these fleets mint throwaway tokens, and the production
# default costs ~300ms of KDF per hash in a debug build.
[token_kdf]
m_cost_kib = 8
t_cost = 1
p_cost = 1

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[client_tls]
# Plain HTTP on the client listener (ADR 0037 §4: the posture is always
# explicit, never implied).
insecure = true

[history]
# Explicitly lossy (ADR 0012): the integration daemons run no history store.
mode = "none"

[auth]
# Explicitly insecure: every request is an anonymous admin (issue #45).
insecure_open = true

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
        let client_addr = client_listener
            .local_addr()
            .expect("resolved client API listener address");

        let (runtime_shutdown, shutdown_rx) = watch::channel(false);
        // A detached (non-installing) recorder, so several replicas in one test
        // process never race on the process-global recorder slot (issue #46).
        let metrics = coppice_api::http::MetricsEndpoint::detached_for_tests();
        // The wider seam rather than `serve_runtime`: it is the one that takes
        // the housekeeping cadence, and passing the production defaults for
        // the two arguments this harness has no opinion about (serving SANs,
        // renewal pacing) leaves every existing suite running exactly what it
        // ran before.
        let runtime_join = tokio::spawn(bootstrap::serve_runtime_with_serving_sans(
            Arc::clone(&consensus),
            views.clone(),
            event_tap,
            handle.clone(),
            listener,
            client_listener,
            cluster_id,
            node_log_client,
            data_dir.clone(),
            metrics,
            readyz,
            None,
            coppice_coordinator::RenewalPacing::default(),
            // Explicitly lossy (ADR 0012), matching the `[history]` section
            // these fixtures write: no history store runs behind the suites.
            coppice_coordinator::HistorySink::None,
            housekeeping_interval,
            // The test fleet's config harness runs the open posture (the same
            // `[auth] insecure_open = true` a dev cluster uses), so the fleet
            // tests drive the API without credentials exactly as before.
            coppice_authn::AuthMode::Open,
            // Nothing armed: `[test_failpoints]` is a per-daemon config
            // section, and this harness hand-assembles its replica rather
            // than going through `run_with`.
            coppice_coordinator::failpoints::Failpoints::default(),
            Some(shutdown_rx),
        ));

        RunningCoordinator {
            _dir: dir,
            data_dir: data_dir.clone(),
            consensus,
            views,
            agent_endpoint: format!("localhost:{agent_port}"),
            client_addr,
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

    /// A URL on this coordinator's client API listener — the plain-HTTP
    /// posture the fixture config sets (`[client_tls] insecure = true`).
    pub fn api(&self, path: &str) -> String {
        format!("http://{}{path}", self.client_addr)
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
    agent_port: u16,
    running: Option<RunningDaemon>,
}

struct RunningDaemon {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<anyhow::Result<()>>,
    /// The daemon's own runtime. `run_with` and everything it spawns —
    /// consensus core, listeners, the convergence loop — live here rather
    /// than on the test's runtime, so that [`Daemon::kill`] can drop the lot
    /// the way process death would: aborting one outer task would orphan the
    /// inner ones, and an orphaned consensus core holds the storage `LOCK`
    /// and the listen ports forever, wedging any restart.
    runtime: tokio::runtime::Runtime,
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
removal_grace = "120s"
learner_expiry = "1h"

[discovery.static]
addrs = []

[listen]
client_addr = "127.0.0.1:{client_port}"
raft_addr = "127.0.0.1:{raft_port}"
agent_addr = "127.0.0.1:{agent_port}"
advertise_host = "localhost"

[raft]
# Generous for oversubscribed CI runners: 300ms elections flap for minutes on a
# 2-core host under load (observed: every enrollment forward bouncing between
# two voters each naming the other leader), and nothing in these suites needs
# fast failover — kills are real process deaths detected by contact evidence,
# not election timing.
election_timeout = "1s"
heartbeat_interval = "250ms"
rpc_timeout = "2s"
snapshot_log_entries = 32
snapshot_keep_log_entries = 0

[pacing]
probe_interval = "50ms"
settled_interval = "250ms"
refusal_backoff = "1s"
park_interval_min = "50ms"
park_interval_max = "250ms"
promote_poll_interval = "50ms"
# Leaf renewal (ADR 0037 §4) — only `Daemon` runs the renewal task, since only
# it goes through `bootstrap::run_with`. Production re-evaluates its conditions
# every 15s, which is the floor on how long a re-rooted fleet takes to notice
# and carry itself onto the new root, and a failed attempt (routine while a
# fleet is still electing) then waits out a 30s-and-doubling backoff. Nothing
# here tests that tempo — the suites test that renewal happens at all — so pace
# it to this fleet's own timescale.
renewal_reevaluate_interval = "200ms"
renewal_retry_min = "300ms"
renewal_retry_max = "2s"

# Minimal argon2 cost: these fleets mint throwaway tokens, and the production
# default costs ~300ms of KDF per hash in a debug build.
[token_kdf]
m_cost_kib = 8
t_cost = 1
p_cost = 1

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[client_tls]
# Plain HTTP on the client listener (ADR 0037 §4: the posture is always
# explicit, never implied).
insecure = true

[history]
# Explicitly lossy (ADR 0012): the integration daemons run no history store.
mode = "none"

[auth]
# Explicitly insecure: every request is an anonymous admin (issue #45).
insecure_open = true

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
            agent_port,
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

    /// Switch this daemon to the `file` discovery backend (ADR 0037 §2),
    /// enumerating `dir`.
    ///
    /// The backend a real fleet uses when it has no service discovery worth
    /// the name: each daemon drops a run-scoped registration file naming its
    /// raft address, and every other daemon reads the directory. It is also
    /// the only backend under which N daemons can be given a *shape-identical*
    /// config — no per-node seed list — which is what [`Fleet`] needs.
    pub fn set_file_discovery(&self, dir: &std::path::Path) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        let updated = toml
            .replace("backend = \"static\"", "backend = \"file\"")
            .replace(
                "[discovery.static]\naddrs = []",
                &format!("[discovery.file]\ndir = \"{}\"", dir.display()),
            );
        assert_ne!(toml, updated, "no [discovery.static] block to rewrite");
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Set `[discovery] cluster_size`, the expected voter count (ADR 0037 §2).
    /// The fixture default is 1; anything that means to grow past a single
    /// voter must raise it, or the leader's §7 ceiling refuses the promotion.
    pub fn set_cluster_size(&self, size: usize) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        assert!(
            toml.contains("cluster_size = 1"),
            "no cluster_size line to rewrite"
        );
        let updated = toml.replace("cluster_size = 1", &format!("cluster_size = {size}"));
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Set `[discovery] removal_grace`, the evidence-gated-removal window
    /// (ADR 0037 §7). The fixture default is `120s`.
    pub fn set_removal_grace(&self, value: &str) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        assert!(
            toml.contains("removal_grace = \"120s\""),
            "no removal_grace line to rewrite"
        );
        let updated = toml.replace(
            "removal_grace = \"120s\"",
            &format!("removal_grace = \"{value}\""),
        );
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Set `[discovery] learner_expiry`, the learner-GC window (ADR 0037 §7).
    /// The fixture default is `1h`.
    pub fn set_learner_expiry(&self, value: &str) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        assert!(
            toml.contains("learner_expiry = \"1h\""),
            "no learner_expiry line to rewrite"
        );
        let updated = toml.replace(
            "learner_expiry = \"1h\"",
            &format!("learner_expiry = \"{value}\""),
        );
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Give this daemon an `[enrollment]` block (ADR 0037 §4/§5): the cluster's
    /// client-listener base URL and the token an operator minted. This is the
    /// entire fleet-wide artifact — the thing every node in a fleet ships with,
    /// identical — so a test that writes it is describing a real deployment.
    ///
    /// `insecure = true` because the fixture's client listener is plain HTTP.
    pub fn set_enrollment(&self, endpoint: &str, token: &str) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        std::fs::write(
            &self.config_path,
            format!(
                "{toml}\n[enrollment]\nendpoint = \"{endpoint}\"\ntoken = \"{token}\"\n\
                 insecure = true\n"
            ),
        )
        .expect("write config");
    }

    /// The `[auth] insecure_open = true` block every fixture config carries,
    /// verbatim — the thing [`Daemon::set_sso`] swaps out.
    const OPEN_AUTH_BLOCK: &'static str =
        "[auth]\n# Explicitly insecure: every request is an anonymous admin (issue #45).\n\
         insecure_open = true\n";

    /// Switch this daemon from the open posture to OIDC (issue #45): replace
    /// the `[auth] insecure_open = true` block with an `[sso]` table pointing
    /// at `issuer`. The two are mutually exclusive and one of them is
    /// required, so this is a replacement and never an addition.
    ///
    /// Call before [`Daemon::start`]: the posture is resolved at config load.
    pub fn set_sso(&self, issuer: &str, client_id: &str) {
        self.set_sso_block(&format!(
            "[sso]\nissuer = \"{issuer}\"\nclient_id = \"{client_id}\"\n"
        ));
    }

    /// [`set_sso`](Self::set_sso) with the `[sso]` table supplied verbatim —
    /// what the documented-example test needs, since the whole point there is
    /// that the TOML came out of `docs/operations/configuration.md` rather
    /// than out of this file.
    pub fn set_sso_block(&self, sso_toml: &str) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        let updated = toml.replace(Daemon::OPEN_AUTH_BLOCK, sso_toml);
        assert_ne!(
            toml, updated,
            "no [auth] block to replace — the fixture template must have changed"
        );
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Arm this daemon's write-path gates (see
    /// [`coppice_coordinator::failpoints`]), scoped to it alone exactly like
    /// [`Daemon::arm_failpoints`]. A gate parks the daemon at a named line
    /// until [`Daemon::release_gate`] lets it go, rather than parking it
    /// forever.
    pub fn arm_gates(&self, names: &[&str]) {
        let quoted: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
        let toml = self.config_without_failpoints();
        std::fs::write(
            &self.config_path,
            format!(
                "{toml}\n[test_failpoints]\ngate_at = [{}]\n",
                quoted.join(", ")
            ),
        )
        .expect("write config");
    }

    /// Wait until this daemon has parked at the gate `name`.
    ///
    /// Like [`Daemon::await_halted_at`] this waits on a durable file, so the
    /// observer cannot lose the race: once the marker exists, the gated line
    /// has been reached and nothing past it has run.
    pub async fn await_gate(&self, name: &str) {
        let marker = coppice_coordinator::failpoints::gate_reached_marker(&self.data_dir(), name);
        poll(
            Duration::from_secs(30),
            &format!("the daemon parks at the {name} gate"),
            || {
                let marker = marker.clone();
                async move { marker.exists() }
            },
        )
        .await;
    }

    /// Release a daemon parked at the gate `name`.
    pub fn release_gate(&self, name: &str) {
        let marker = coppice_coordinator::failpoints::gate_release_marker(&self.data_dir(), name);
        std::fs::write(&marker, name).expect("write the gate release marker");
    }

    /// Arm this daemon's join-pipeline failpoints (ADR 0037 §6), scoped to it
    /// alone: the `[test_failpoints]` section goes into *its* config file, so
    /// a fleet sharing this test process is untouched.
    ///
    /// Appended last and rewritten wholesale, so arming, re-arming and
    /// [`Daemon::clear_failpoints`] are all one operation on the tail of the
    /// file. Every other `set_*` here rewrites a line or appends its own
    /// section, so nothing else is disturbed — but call this *after* them.
    pub fn arm_failpoints(&self, names: &[&str]) {
        let quoted: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
        let toml = self.config_without_failpoints();
        std::fs::write(
            &self.config_path,
            format!(
                "{toml}\n[test_failpoints]\nhalt_at = [{}]\n",
                quoted.join(", ")
            ),
        )
        .expect("write config");
    }

    /// Disarm every failpoint: the config a restarted daemon gets, so a
    /// resume is a resume and not a second halt.
    pub fn clear_failpoints(&self) {
        std::fs::write(&self.config_path, self.config_without_failpoints()).expect("write config");
    }

    /// Where a daemon halted at `name` records that it got there — the
    /// harness's side of [`coppice_coordinator::failpoints::halt_marker`].
    pub fn halt_marker(&self, name: &str) -> PathBuf {
        coppice_coordinator::failpoints::halt_marker(&self.data_dir(), name)
    }

    /// Wait until this daemon has halted at `name`.
    ///
    /// The marker file is durable, so unlike a phase poll this cannot lose a
    /// race: the daemon is still serving (only its convergence loop is parked,
    /// permanently), so a caller that sees the marker can read `/readyz` for
    /// the state at the halt and then kill the process where it stands.
    pub async fn await_halted_at(&self, name: &str) {
        let marker = self.halt_marker(name);
        poll(
            Duration::from_secs(30),
            &format!("the daemon halts at the {name} failpoint"),
            || {
                let marker = marker.clone();
                async move { marker.exists() }
            },
        )
        .await;
    }

    /// This daemon's config with any `[test_failpoints]` section removed.
    /// The section is always the file's tail (see [`Daemon::arm_failpoints`]),
    /// so truncating at its header is exact.
    fn config_without_failpoints(&self) -> String {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        match toml.find("[test_failpoints]") {
            Some(at) => toml[..at].trim_end().to_string(),
            None => toml,
        }
    }

    /// Start (or restart) the daemon. Returns once `run_with` has been spawned;
    /// callers poll a surface to learn when it is serving.
    ///
    /// A boot failure is printed as it happens rather than only surfacing at
    /// [`Daemon::stop`]. Without it a daemon that refuses to start is
    /// indistinguishable from one that is merely slow — the caller is polling
    /// a surface that will never come up, and the error that explains why sits
    /// unread in a `JoinHandle` until the test has already failed on a
    /// timeout. (The fail-stop tests still read the value from `stop`.)
    pub fn start(&mut self) {
        assert!(self.running.is_none(), "daemon already running");
        let resolved = config::load(&self.config_path).expect("load daemon config");
        let (shutdown, shutdown_rx) = watch::channel(false);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build the daemon's runtime");
        let join = runtime.spawn(async move {
            let out = bootstrap::run_with(
                resolved,
                coppice_api::http::MetricsEndpoint::detached_for_tests(),
                Some(shutdown_rx),
            )
            .await;
            if let Err(e) = &out {
                eprintln!("daemon exited with an error: {e:#}");
            }
            out
        });
        self.running = Some(RunningDaemon {
            shutdown,
            join,
            runtime,
        });
    }

    /// Whether this daemon has been started and not yet stopped.
    pub fn is_running(&self) -> bool {
        self.running.is_some()
    }

    /// Stop the daemon and return what `run_with` returned — `Err` for the
    /// `formation-failed` fail-stop, `Ok` otherwise.
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        let running = self.running.take().expect("daemon is not running");
        let _ = running.shutdown.send(true);
        let out = running.join.await.expect("daemon task joined");
        // Wind the daemon's runtime down without blocking the test's: the
        // graceful path has already drained everything that matters.
        running.runtime.shutdown_background();
        out
    }

    /// Kill the daemon abruptly: no shutdown signal — the daemon's whole
    /// runtime is torn down, so every task it spawned (listeners, consensus
    /// core, the convergence loop) is dropped at whatever await it is parked
    /// on, exactly as process death would drop them. This is the
    /// crash-injection primitive: disk is left exactly as it was at the
    /// moment of death, and a subsequent [`Daemon::start`] must converge from
    /// it with no cleanup (ADR 0037 §6: `Restart=always` is the whole
    /// recovery story). Resource release (the storage `LOCK`, the ports) is
    /// asynchronous; a restart is preceded by [`Daemon::await_released`].
    pub async fn kill(&mut self) {
        let running = self.running.take().expect("daemon is not running");
        running.runtime.shutdown_background();
        let _ = running.join.await;
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    /// Wait until a killed daemon's resources are actually released.
    ///
    /// A real crash releases fds atomically with process death, but
    /// [`Daemon::kill`] is in-process: aborting the `run_with` task leaves the
    /// inner tasks it spawned (consensus core, listeners) to die as their
    /// channels and `Arc`s drop, which takes a few scheduler ticks. Until
    /// then the storage `LOCK` and the listen ports are still held, and an
    /// immediate [`Daemon::start`] fail-stops on `EAGAIN`. Call this between
    /// a kill and a restart; it changes nothing on disk.
    pub async fn await_released(&self) {
        use coppice_consensus::fs::Fs as _;
        // Sequential rather than combined, so a timeout names the resource
        // that is actually wedged.
        poll(
            Duration::from_secs(10),
            "killed daemon releases its storage LOCK",
            || async {
                // Acquiring and dropping the advisory lock is the probe: it
                // succeeds exactly when the orphaned storage core is gone.
                coppice_consensus::fs::RealFs::new(self.data_dir())
                    .lock(std::path::Path::new("LOCK"))
                    .is_ok()
            },
        )
        .await;
        for (port, name) in [
            (self.client_port, "client"),
            (self.raft_port, "raft"),
            (self.agent_port, "agent"),
        ] {
            poll(
                Duration::from_secs(10),
                &format!("killed daemon releases its {name} port"),
                || async { TcpListener::bind(("127.0.0.1", port)).is_ok() },
            )
            .await;
        }
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

    /// `localhost:PORT` for the agent session listener — what an agent dials.
    pub fn agent_target(&self) -> String {
        format!("localhost:{}", self.agent_port)
    }

    /// Write TLS material into this daemon's `[tls]` paths before it starts —
    /// the state a certless daemon reaches by enrolling, laid down directly.
    /// For tests whose subject is not enrollment: the daemon's first
    /// convergence round finds a usable leaf and goes straight to probing.
    pub fn install_tls_material(&self, ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) {
        let root = self.dir.path();
        std::fs::write(root.join("ca.crt"), ca_pem).expect("write ca");
        std::fs::write(root.join("node.crt"), cert_pem).expect("write cert");
        std::fs::write(root.join("node.key"), key_pem).expect("write key");
    }

    /// The `[tls]` paths this daemon serves from: a certless daemon's
    /// formation writes its own minted leaf here (ADR 0037 §3 step 3).
    pub fn tls_material(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let root = self.dir.path();
        (
            std::fs::read(root.join("ca.crt")).expect("read cluster ca"),
            std::fs::read(root.join("node.crt")).expect("read node cert"),
            std::fs::read(root.join("node.key")).expect("read node key"),
        )
    }

    /// The bound client-listener address, for tests that need a raw socket.
    pub fn client_addr(&self) -> String {
        format!("127.0.0.1:{}", self.client_port)
    }

    pub fn api(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.client_port)
    }

    /// Switch this daemon's client listener to TLS (ADR 0037 §4
    /// `[client_tls]`): write a serving leaf signed by `ca` and rewrite the
    /// config's posture from `insecure` to those paths. Returns the CA bundle a
    /// client must trust, and the `https://` base URL.
    ///
    /// Call before [`Daemon::start`]: the posture is resolved at config load.
    ///
    /// Rewrites the `[client_tls]` posture **line in its own section**, not
    /// every `insecure = true` in the file: [`Daemon::set_enrollment`] appends
    /// a section carrying the same line for an entirely different reason (the
    /// enrolling *client*'s opt-in to a plain-HTTP endpoint), and a blanket
    /// replace would turn that into two stray path keys under `[enrollment]`.
    pub fn set_client_tls(&self, ca: &Ca) -> (Vec<u8>, String) {
        let leaf = ca.leaf();
        let cert = self.dir.path().join("api.crt");
        let key = self.dir.path().join("api.key");
        std::fs::write(&cert, &leaf.cert_pem).expect("write client-tls cert");
        std::fs::write(&key, &leaf.key_pem).expect("write client-tls key");

        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        let updated = toml.replacen(
            "insecure = true",
            &format!(
                "cert_path = \"{}\"\nkey_path = \"{}\"",
                cert.display(),
                key.display()
            ),
            1,
        );
        assert_ne!(toml, updated, "no [client_tls] posture line to rewrite");
        std::fs::write(&self.config_path, updated).expect("write config");
        (
            ca.pem.clone(),
            format!("https://localhost:{}", self.client_port),
        )
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
    ///
    /// The budget is generous because "the listener is not up yet" is a real
    /// wait, not a failure: a replica restarting into a cluster that lost
    /// quorum while it was down must first see an election complete, which
    /// takes seconds, not milliseconds. Only a daemon that never serves at all
    /// should fail here.
    pub async fn readyz(&self) -> (u16, serde_json::Value) {
        let client = reqwest::Client::new();
        let url = self.api("/readyz");
        let mut last = None;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
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

    /// Set `[raft] health_stability_interval` (ADR 0037 §9), the window the
    /// leader's full-redundancy condition must hold before `?require=healthy`
    /// answers 200. The production default is 10s; a test that waits on the
    /// gate shrinks it. Call before [`Daemon::start`].
    pub fn set_health_stability(&self, interval: &str) {
        let toml = std::fs::read_to_string(&self.config_path).expect("read config");
        let updated = toml.replace(
            "[raft]\n",
            &format!("[raft]\nhealth_stability_interval = \"{interval}\"\n"),
        );
        assert_ne!(toml, updated, "no [raft] section to rewrite");
        std::fs::write(&self.config_path, updated).expect("write config");
    }

    /// Wait until `/readyz` reports any of `phases`, returning the body.
    ///
    /// The tight-poll counterpart of [`Daemon::await_phase`], for tests that
    /// must *catch* a transient convergence phase (`joining`, `learner`)
    /// rather than merely arrive at a terminal one: the poll interval is small
    /// against the convergence loop's tick (50ms under the fixture's
    /// `[pacing]`), so a phase that exists at all is observed.
    pub async fn await_phase_in(&self, phases: &[&str]) -> serde_json::Value {
        // Generous: a parked daemon under full-suite load can spend most of
        // its park backoff (up to 250ms a round under the fixture's
        // `[pacing]`) before its first join tick — and on an oversubscribed
        // host (CI, or concurrent builds) a starved fleet can churn elections
        // for a while first, during which every enrollment answers 503 and
        // each failed round costs a full backoff.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let (_, body) = self.readyz().await;
            if phases.iter().any(|p| body["phase"] == *p) {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "daemon never reached a phase in {phases:?}; last body: {body}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

// ---------------------------------------------------------------------------
// A control plane that answers nothing
// ---------------------------------------------------------------------------

/// A [`ControlPlane`](coppice_api::ControlPlane) with no cluster behind it.
///
/// The `/enroll` route (ADR 0037 §4) captures its own endpoint and never
/// touches the control plane, so a test that drives *only* that route can serve
/// the real router over this — which is exactly what proves the route's
/// independence from the trait.
pub struct NoopPlane;

impl coppice_api::ControlPlane for NoopPlane {
    fn cluster_id(&self) -> ClusterId {
        ClusterId::new()
    }

    async fn submit_job(
        &self,
        _req: coppice_api::http::dto::SubmitJobRequest,
        _actor: coppice_state::Actor,
    ) -> Result<coppice_api::http::dto::SubmitJobResponse, coppice_api::ApiError> {
        Err(unattached())
    }

    async fn abort_job(
        &self,
        _req: coppice_api::http::dto::AbortJobRequest,
        _actor: coppice_state::Actor,
    ) -> Result<(), coppice_api::ApiError> {
        Err(unattached())
    }

    async fn configure_quota_entity(
        &self,
        _req: coppice_api::http::dto::ConfigureQuotaEntityRequest,
        _actor: coppice_state::Actor,
    ) -> Result<coppice_api::http::dto::ConfigureQuotaEntityResponse, coppice_api::ApiError> {
        Err(unattached())
    }

    // This plane exists solely to serve the `/enroll` route independence
    // test; it has no cluster behind it, so authorization writes never land.
    async fn update_authorization(
        &self,
        _req: coppice_api::http::dto::UpdateAuthorizationRequest,
        _actor: coppice_state::Actor,
    ) -> Result<coppice_api::http::dto::UpdateAuthorizationResponse, coppice_api::ApiError> {
        Err(unattached())
    }

    async fn read_state(
        &self,
        _opts: coppice_api::ReadOptions,
    ) -> Result<coppice_api::ReadView, coppice_api::ApiError> {
        Err(unattached())
    }

    fn queue_window(&self) -> coppice_api::QueueWindow {
        coppice_api::QueueWindow::default()
    }

    fn usage_window(&self) -> coppice_api::UsageSnapshot {
        coppice_api::UsageSnapshot::default()
    }
    async fn job_timeline(
        &self,
        _job: coppice_core::id::JobId,
        _after: Option<(u64, u32)>,
        _limit: usize,
    ) -> coppice_api::JobTimelineWindow {
        coppice_api::JobTimelineWindow {
            floor_index: 0,
            events: Vec::new(),
            next: None,
        }
    }

    fn coordinator_status(&self) -> Result<coppice_api::CoordinatorSummary, coppice_api::ApiError> {
        Err(unattached())
    }

    async fn fetch_logs(
        &self,
        _node: coppice_core::id::NodeId,
        _addr: &str,
        _req: coppice_api::LogFetchRequest,
    ) -> Result<coppice_api::LogFetchOutcome, coppice_api::LogFetchError> {
        Err(coppice_api::LogFetchError::Unreachable {
            reason: "no control plane attached".to_string(),
        })
    }

    async fn fetch_metrics(
        &self,
        _node: coppice_core::id::NodeId,
        _addr: &str,
        _req: coppice_api::MetricsFetchRequest,
    ) -> Result<coppice_api::MetricsFetchOutcome, coppice_api::MetricsFetchError> {
        Err(coppice_api::MetricsFetchError::Unreachable {
            reason: "no control plane attached".to_string(),
        })
    }
}

fn unattached() -> coppice_api::ApiError {
    coppice_api::ApiError::Unavailable("no control plane attached to this listener".to_string())
}

// ---------------------------------------------------------------------------
// A fleet of shape-identical daemons (ADR 0037 §1)
// ---------------------------------------------------------------------------

/// The enrollment secret every fleet config carries, seeded into the forming
/// cluster by [`Fleet::seeding_policy`] (ADR 0037 §5).
///
/// Operator-chosen rather than cluster-minted, because that is what makes the
/// configs identical *before* the cluster exists: the value is already in the
/// launch template when node 1 forms. A cluster-minted token could only be
/// distributed after formation, which would put a per-node step back into the
/// flow this chunk exists to delete.
pub const FLEET_TOKEN: &str = "cpk_fleet-launch-template-secret";

/// N coordinator daemons whose configs differ only in the ports a single test
/// process forces them to differ in — the ADR 0037 §1 launch-template shape.
///
/// Every member gets the `file` discovery backend over one shared registration
/// directory, the same `cluster_size`, the same `[enrollment]` block naming the
/// same endpoint and the same pre-baked token, and no seed list, no node id, no
/// intent flag and no peer addresses. That is the whole point: if a test has to
/// tell any member anything about any other member, the property under test has
/// already been lost.
///
/// The intended shape is [`Fleet::start_all`] → [`Fleet::init`] on any one
/// member → [`Fleet::await_voters`]. Nothing in between.
pub struct Fleet {
    /// Holds the shared discovery registration directory. Dropped last.
    _dir: TempDir,
    pub cluster_id: ClusterId,
    /// The `[discovery] cluster_size` every member was given at construction
    /// (ADR 0037 §2/§7): fixed at the fleet's target voter count regardless of
    /// how many learners [`Fleet::add_member`] appends afterward.
    cluster_size: usize,
    pub members: Vec<Daemon>,
}

impl Fleet {
    /// Lay down `size` shape-identical daemon configs without starting any.
    ///
    /// Every member is **certless** (ADR 0037 §4's minimal deployment): the
    /// forming node mints its own material, and every other member enrolls for
    /// its leaf through the convergence loop. Nothing is provisioned by hand,
    /// which is the same reason the enrollment endpoint can be baked in before
    /// the cluster exists — it is an address, not a credential.
    pub fn new(size: usize, ca: &Ca) -> Fleet {
        assert!(size > 0, "a fleet needs at least one member");
        let dir = tempfile::tempdir().expect("create fleet tempdir");
        let registry = dir.path().join("discovery");
        std::fs::create_dir_all(&registry).expect("create the registration directory");

        let cluster_id = ClusterId::new();
        let members: Vec<Daemon> = (0..size)
            .map(|_| Daemon::new_certless(cluster_id, ca))
            .collect();

        // One well-known enrollment endpoint for the whole fleet, standing in
        // for the load-balanced name a real deployment bakes into its template.
        // A real name fronts every member and answers from whichever ones are
        // formed; a test process has no load balancer, so it names **member 0**
        // — which is why member 0 is the member a fleet test `init`s (see
        // [`Fleet::init`]). Member 0 pointing at itself is harmless: its own
        // loop finds it already holds the leaf formation minted.
        let endpoint = members[0].api("");
        for member in &members {
            member.set_file_discovery(&registry);
            member.set_cluster_size(size);
            member.set_enrollment(&endpoint, FLEET_TOKEN);
        }

        Fleet {
            _dir: dir,
            cluster_id,
            cluster_size: size,
            members,
        }
    }

    /// The shared `file`-discovery registration directory every member
    /// enumerates, for a test that hand-assembles one more same-shape member
    /// outside the initial batch (see [`Fleet::add_member`]).
    pub fn registry_dir(&self) -> PathBuf {
        self._dir.path().join("discovery")
    }

    /// The `[discovery] cluster_size` this fleet was built with.
    pub fn cluster_size(&self) -> usize {
        self.cluster_size
    }

    /// Lay down (without starting) one more shape-identical, certless member
    /// and append it to [`Fleet::members`] — a fresh installation joining a
    /// fleet that already exists (ADR 0037 §7: the hands-off replacement path
    /// is terminate-then-launch, and the evidence-gated-removal tests need a
    /// literal new installation, not a member `Fleet::new` already knew
    /// about). Returns the new member's index.
    ///
    /// The enrollment endpoint is re-derived from member 0 exactly as
    /// [`Fleet::new`] derives it, so the new member's config is identical in
    /// shape to every other member's, just later in time.
    pub fn add_member(&mut self, ca: &Ca) -> usize {
        let member = Daemon::new_certless(self.cluster_id, ca);
        member.set_file_discovery(&self.registry_dir());
        member.set_cluster_size(self.cluster_size);
        let endpoint = self.members[0].api("");
        member.set_enrollment(&endpoint, FLEET_TOKEN);
        self.members.push(member);
        self.members.len() - 1
    }

    /// The `init --policy` document that seeds [`FLEET_TOKEN`], so the token
    /// every config already carries becomes live the moment the cluster forms.
    pub fn seeding_policy() -> String {
        format!(
            "[[enroll_token]]\nsecret = \"{FLEET_TOKEN}\"\nrole = \"coordinator\"\n\
             label = \"coordinators\"\n"
        )
    }

    /// Start every member. They all park: none of them can form, and none of
    /// them has a cluster to find yet.
    pub fn start_all(&mut self) {
        for member in &mut self.members {
            member.start();
        }
    }

    /// Run `init` on member 0 — the single operator act in the whole lifecycle
    /// (ADR 0037 §3) — seeding the fleet's enrollment token with it.
    ///
    /// Member 0 and not an arbitrary member only because it is the address
    /// [`Fleet::new`] baked in as the enrollment endpoint, standing in for a
    /// load-balanced name. Which member forms is not otherwise significant, and
    /// nothing downstream of `init` treats it specially.
    pub async fn init(&mut self) -> coppice_coordinator::localadmin::OperatorPem {
        self.init_with_policy(Fleet::seeding_policy()).await
    }

    /// As [`Fleet::init`], but with a caller-supplied bootstrap-policy
    /// document instead of [`Fleet::seeding_policy`]'s bare token — e.g. a
    /// test that also needs a priority-multiplier table seeded before it can
    /// submit a job.
    pub async fn init_with_policy(
        &mut self,
        policy: String,
    ) -> coppice_coordinator::localadmin::OperatorPem {
        let index = 0;
        let member = &mut self.members[index];
        member.await_phase("waiting").await;
        let reply = member
            .admin(coppice_coordinator::localadmin::AdminCall::Init {
                policy: Some(policy),
                operator_csr: None,
                operator_cn: Some("day0".to_string()),
            })
            .await;
        match reply {
            coppice_coordinator::localadmin::AdminReply::Formed { operator, .. } => operator,
            other => panic!("expected member {index} to form the cluster, got {other:?}"),
        }
    }

    /// Wait until every member reports `voter` and agrees the voter set has
    /// `expected` members.
    ///
    /// Asserted from *every* member rather than the leader alone: a joiner that
    /// believes itself a voter while the leader disagrees is exactly the
    /// half-converged state this loop must not leave behind.
    /// Both conditions are polled together, because reaching `voter` is not
    /// reaching convergence: the member that formed the cluster is a voter one
    /// instant after `init`, in a voter set of one, and asserting there would
    /// pass before the fleet had done anything at all.
    pub async fn await_voters(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(60);
        for (i, member) in self.members.iter().enumerate() {
            loop {
                let (_, body) = member.readyz().await;
                let voters = body["voters"].as_array().map(|v| v.len()).unwrap_or(0);
                if body["phase"] == "voter" && voters == expected {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "member {i} never reached a {expected}-voter set: {body}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    /// Stop every member, ignoring the order — nothing here asserts on a
    /// clean shutdown, which the lifecycle tests cover directly.
    pub async fn stop_all(&mut self) {
        for member in &mut self.members {
            if member.is_running() {
                let _ = member.stop().await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Volume loss (ADR 0037 §1/§3)
// ---------------------------------------------------------------------------

/// A second `impl` block rather than an edit to the one above, so this file
/// stays append-only while several suites are being written against it.
impl Daemon {
    /// Destroy this installation the way losing its disk would: the data
    /// directory (manifest, raft log, machine identity, CA key) **and** the
    /// `[tls]` material go together.
    ///
    /// Distinct from [`Daemon::wipe_data_dir`], which models the ADR 0037 §3
    /// recovery from a failed formation — a deliberate operator act on a node
    /// whose certificates are still valid. This one models the volume being
    /// gone: what comes back is a fresh installation with no identity, no
    /// certificate and no history, which is the only state from which a daemon
    /// re-enrolls (`enroll_if_needed` returns early whenever a usable leaf is
    /// on disk, so a half-wipe would quietly keep the old identity's leaf).
    ///
    /// The config, the ports and the enrollment block survive, because in a
    /// real fleet those come from the launch template and not from the volume.
    pub fn wipe_installation(&self) {
        self.wipe_data_dir();
        for file in ["node.crt", "node.key", "ca.crt"] {
            let path = self.dir.path().join(file);
            if path.exists() {
                std::fs::remove_file(&path).expect("wipe tls material");
            }
        }
    }

    /// Wait for a daemon to exit **on its own**, and return what `run_with`
    /// returned.
    ///
    /// The counterpart of [`Daemon::stop`] for the fail-stop paths: nothing is
    /// signalled, because the point of the assertion is that the daemon
    /// decided to stop by itself (ADR 0037 §3). A daemon that is still serving
    /// when the budget runs out fails the test here rather than at whatever
    /// the caller was going to assert next, and names that it never exited.
    pub async fn await_exit(&mut self, budget: Duration) -> anyhow::Result<()> {
        let running = self.running.take().expect("daemon is not running");
        let out = tokio::time::timeout(budget, running.join)
            .await
            .expect("the daemon was still running when its exit budget ran out")
            .expect("daemon task joined");
        running.runtime.shutdown_background();
        out
    }
}
