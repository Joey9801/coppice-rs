//! The local admin socket: formation's authority (ADR 0037 §3).
//!
//! Every coordinator daemon serves a small admin surface on a Unix domain
//! socket in its data directory, in **every** state — parked, forming,
//! `formation-failed`, and formed. That is not an incidental convenience: it
//! is how `init` reaches a parked daemon, and how an operator sees a failed
//! formation on a node that is serving nothing else.
//!
//! # Authority
//!
//! Local socket access **is** the authority; there is no further
//! authentication on this surface. That is the honest posture (ADR 0037 §3):
//! whoever holds root on a coordinator host already holds everything the host
//! will ever store, including the CA key that lives in this same directory.
//! The daemon therefore enforces exactly one thing — that the socket and its
//! directory are reachable only by the daemon's user and root — and enforces
//! it on both, because several BSD-derived kernels ignore permission bits on
//! the socket file itself and honor only the containing directory.
//!
//! # Framing
//!
//! One JSON object per line, request and response, connection per call.
//! tonic-over-UDS was the alternative and would have been defensible, but
//! this surface argues for JSON: its largest verb (`status`) is *defined* as
//! returning the same document as `GET /readyz`, which is already a serde
//! type, and its payloads are PEM blobs and TOML text rather than anything
//! that wants a schema. Choosing JSON also keeps formation entirely out of
//! the proto corpus — appropriate for a verb the ADR is emphatic is not a
//! network RPC — and keeps the parked daemon's dependency surface to what it
//! already links.
//!
//! The protocol is deliberately not versioned: both ends ship in the same
//! binary, and the socket is local to one host.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};

use coppice_api::http::ReadyzReport;
use coppice_consensus::{NodeHandle, OpenraftConsensus};
use coppice_tls::TlsStore;

use crate::formation::{self, FormRequest, OperatorCredential, PhaseState};
use crate::rotate;

/// Cap on one request line. Generous for a policy TOML and a CSR, far below
/// anything that could exhaust the daemon: the surface is local, but "local"
/// includes a buggy script.
const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// A call on the admin socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "verb", rename_all = "kebab-case")]
pub enum AdminCall {
    /// Form the cluster (ADR 0037 §3). Idempotent against an already-formed
    /// daemon, which answers [`AdminReply::AlreadyInitialized`].
    Init {
        /// Bootstrap policy TOML, inlined by the client so the daemon never
        /// reads a path the caller controls.
        #[serde(default)]
        policy: Option<String>,
        #[serde(default)]
        operator_csr: Option<String>,
        #[serde(default)]
        operator_cn: Option<String>,
    },
    /// Sign a new operator certificate at any point post-formation — the
    /// documented recovery for a lost `init` output, and for "all operator
    /// certificates lost" (ADR 0037 §3).
    IssueOperatorCert {
        #[serde(default)]
        operator_csr: Option<String>,
        #[serde(default)]
        operator_cn: Option<String>,
    },
    /// This daemon's readiness document — byte-for-byte what `GET /readyz`
    /// serves, available without the client listener.
    Status,
    /// Open a re-root's dual-trust window (ADR 0037 §4). Leader-only, and on
    /// this plane for the same reason `init` is: re-rooting is
    /// root-equivalent authority.
    RotateCaBegin,
    /// The re-root's read-only surface; answerable on any replica.
    RotateCaStatus,
    /// Close a re-root's dual-trust window by dropping the outgoing root.
    RotateCaComplete {
        #[serde(default)]
        force: bool,
    },
}

/// The answer to an [`AdminCall`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum AdminReply {
    /// Formation ran and completed.
    Formed {
        cluster_id: String,
        history_id: String,
        node_id: u64,
        machine_id: String,
        operator: OperatorPem,
        status: ReadyzReport,
    },
    /// The distinct outcome a re-run gets against a formed cluster (ADR 0037
    /// §3): automation treats it as success.
    AlreadyInitialized { status: ReadyzReport },
    /// This daemon's directory records a formation that never completed.
    /// Reported rather than resumed — there is no resume path.
    FormationFailed {
        reason: String,
        status: ReadyzReport,
    },
    /// An operator certificate was issued.
    Issued { operator: OperatorPem },
    /// The `status` verb's answer.
    Status { status: ReadyzReport },
    /// A re-root's dual-trust window is open (ADR 0037 §4).
    RotationBegun { report: rotate::BeginReport },
    /// The `rotate-ca status` verb's answer.
    RotationStatus { status: rotate::RotationStatus },
    /// The outgoing root is no longer a trust anchor.
    RotationCompleted { report: rotate::CompleteReport },
    /// Anything that went wrong, in the operator's terms.
    Error { message: String },
}

/// The PEM material an operator collects from `init` (ADR 0037 §3 step 5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorPem {
    pub cert_pem: String,
    /// Present only when the cluster minted the keypair (no CSR supplied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_pem: Option<String>,
    pub ca_pem: String,
}

impl From<OperatorCredential> for OperatorPem {
    fn from(c: OperatorCredential) -> OperatorPem {
        OperatorPem {
            cert_pem: c.cert_pem,
            key_pem: c.key_pem,
            ca_pem: c.ca_pem,
        }
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// One `init` call handed to the parked daemon's own task.
///
/// Formation must run where the daemon's boot state lives — it produces the
/// running replica the daemon then serves — so the socket handler does not
/// execute it. Routing every attempt through a single-consumer channel also
/// makes concurrent `init` calls harmless by construction: the second one
/// finds a formed (or failed) daemon and is answered accordingly.
pub(crate) struct FormationCall {
    pub(crate) request: FormRequest,
    pub(crate) reply: oneshot::Sender<Result<FormationDone>>,
}

/// What the daemon reports back after running formation.
pub(crate) struct FormationDone {
    pub(crate) history_id: String,
    pub(crate) node_id: u64,
    pub(crate) machine_id: String,
    pub(crate) operator: OperatorCredential,
}

/// The state the socket handlers read.
pub(crate) struct LocalAdmin {
    phase: Arc<PhaseState>,
    data_dir: PathBuf,
    form_tx: mpsc::Sender<FormationCall>,
    /// Attached once the cluster is formed, so `issue-operator-cert` can read
    /// the CA certificate out of replicated state and the `rotate-ca` verbs
    /// can propose against it.
    formed: RwLock<Option<FormedSeam>>,
}

/// What the formed daemon lends this socket.
///
/// `issue-operator-cert` needed only consensus. Re-rooting needs two more
/// things, both of which exist only once the replica is assembled: the raft
/// [`NodeHandle`], to know whether this node leads and where its peers are,
/// and the serving [`TlsStore`], because keying the other voters means dialing
/// them on the machine plane with this daemon's own leaf.
struct FormedSeam {
    consensus: Arc<OpenraftConsensus>,
    handle: NodeHandle,
    tls: Arc<TlsStore>,
}

impl LocalAdmin {
    pub(crate) fn new(
        phase: Arc<PhaseState>,
        data_dir: PathBuf,
        form_tx: mpsc::Sender<FormationCall>,
    ) -> Arc<LocalAdmin> {
        Arc::new(LocalAdmin {
            phase,
            data_dir,
            form_tx,
            formed: RwLock::new(None),
        })
    }

    /// Attach the formed seam once the replica is assembled.
    pub(crate) fn attach(
        &self,
        consensus: Arc<OpenraftConsensus>,
        handle: NodeHandle,
        tls: Arc<TlsStore>,
    ) {
        *self.formed.write().expect("localadmin seam lock") = Some(FormedSeam {
            consensus,
            handle,
            tls,
        });
    }

    /// Clone the formed seam out, or report the pre-formation refusal every
    /// verb here shares: there is no cluster to act on yet.
    ///
    /// Cloned rather than borrowed because the rotation verbs await across
    /// their use of it, and a `std::sync::RwLock` guard must not be held over
    /// an await point.
    fn formed_seam(
        &self,
        verb: &str,
    ) -> Result<(Arc<OpenraftConsensus>, NodeHandle, Arc<TlsStore>)> {
        let guard = self.formed.read().expect("localadmin seam lock");
        let seam = guard.as_ref().ok_or_else(|| {
            anyhow!(
                "this coordinator has not formed a cluster, so `{verb}` has nothing to act on \
                 (ADR 0037 §3). Run `coppice coordinator init` first."
            )
        })?;
        Ok((
            Arc::clone(&seam.consensus),
            seam.handle.clone(),
            Arc::clone(&seam.tls),
        ))
    }

    /// The rotation context this daemon lends the `rotate-ca` verbs.
    fn rotation<'a>(
        &'a self,
        consensus: &'a OpenraftConsensus,
        handle: &'a NodeHandle,
        tls: &'a TlsStore,
    ) -> rotate::RotationContext<'a, OpenraftConsensus> {
        rotate::RotationContext {
            consensus,
            handle,
            tls,
            data_dir: &self.data_dir,
        }
    }

    async fn dispatch(&self, call: AdminCall) -> AdminReply {
        match call {
            AdminCall::Init {
                policy,
                operator_csr,
                operator_cn,
            } => {
                self.init(FormRequest {
                    policy: policy.map(String::into_bytes),
                    operator_csr: operator_csr.map(String::into_bytes),
                    operator_cn,
                })
                .await
            }
            AdminCall::IssueOperatorCert {
                operator_csr,
                operator_cn,
            } => match self.issue_operator_cert(FormRequest {
                policy: None,
                operator_csr: operator_csr.map(String::into_bytes),
                operator_cn,
            }) {
                Ok(operator) => AdminReply::Issued {
                    operator: operator.into(),
                },
                Err(e) => AdminReply::Error {
                    message: format!("{e:#}"),
                },
            },
            AdminCall::Status => AdminReply::Status {
                status: self.phase.readyz(),
            },
            AdminCall::RotateCaBegin => match self.rotate_ca_begin().await {
                Ok(report) => AdminReply::RotationBegun { report },
                Err(e) => AdminReply::Error {
                    message: format!("{e:#}"),
                },
            },
            AdminCall::RotateCaStatus => match self.rotate_ca_status() {
                Ok(status) => AdminReply::RotationStatus { status },
                Err(e) => AdminReply::Error {
                    message: format!("{e:#}"),
                },
            },
            AdminCall::RotateCaComplete { force } => match self.rotate_ca_complete(force).await {
                Ok(report) => AdminReply::RotationCompleted { report },
                Err(e) => AdminReply::Error {
                    message: format!("{e:#}"),
                },
            },
        }
    }

    async fn rotate_ca_begin(&self) -> Result<rotate::BeginReport> {
        let (consensus, handle, tls) = self.formed_seam("rotate-ca begin")?;
        rotate::begin(&self.rotation(&consensus, &handle, &tls)).await
    }

    fn rotate_ca_status(&self) -> Result<rotate::RotationStatus> {
        let (consensus, handle, tls) = self.formed_seam("rotate-ca status")?;
        rotate::status(&self.rotation(&consensus, &handle, &tls))
    }

    async fn rotate_ca_complete(&self, force: bool) -> Result<rotate::CompleteReport> {
        let (consensus, handle, tls) = self.formed_seam("rotate-ca complete")?;
        rotate::complete(&self.rotation(&consensus, &handle, &tls), force).await
    }

    async fn init(&self, request: FormRequest) -> AdminReply {
        // The outcome of `init` is derived from what this daemon *is*, not
        // from what its manifest happens to hold: a node formed by the legacy
        // `--bootstrap` flag carries no marker but is a real cluster, and
        // answering `AlreadyInitialized` there is the truthful answer.
        if let Some(reply) = self.settled_outcome() {
            return reply;
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .form_tx
            .send(FormationCall {
                request,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            // The parked task is gone: it either formed (via a concurrent
            // call) or fail-stopped. Re-read rather than guess.
            return self.settled_outcome().unwrap_or(AdminReply::Error {
                message: "this daemon is no longer accepting formation requests".to_string(),
            });
        }

        match reply_rx.await {
            Ok(Ok(done)) => AdminReply::Formed {
                cluster_id: self.phase.cluster_id().to_string(),
                history_id: done.history_id,
                node_id: done.node_id,
                machine_id: done.machine_id,
                operator: done.operator.into(),
                status: self.phase.readyz(),
            },
            Ok(Err(e)) => AdminReply::Error {
                message: format!("{e:#}"),
            },
            Err(_) => self.settled_outcome().unwrap_or(AdminReply::Error {
                message: "the daemon stopped while forming".to_string(),
            }),
        }
    }

    /// The reply for a daemon whose formation question is already answered,
    /// or `None` while it is genuinely parked.
    fn settled_outcome(&self) -> Option<AdminReply> {
        if self.phase.is_formed() {
            return Some(AdminReply::AlreadyInitialized {
                status: self.phase.readyz(),
            });
        }
        self.phase
            .failed_at()
            .map(|at| AdminReply::FormationFailed {
                reason: formation::failed_diagnostic(at),
                status: self.phase.readyz(),
            })
    }

    fn issue_operator_cert(&self, request: FormRequest) -> Result<OperatorCredential> {
        let (consensus, _, _) = self.formed_seam("issue-operator-cert")?;
        let (signer, ca_pem) = formation::load_cluster_ca(&self.data_dir, consensus.as_ref())?;
        formation::issue_operator_credential(&signer, &ca_pem, &request)
    }
}

/// The bound admin socket, ready to serve.
#[derive(Debug)]
pub(crate) struct AdminSocket {
    listener: UnixListener,
    path: PathBuf,
}

impl AdminSocket {
    /// Bind the socket owner-only, inside a directory this daemon owns.
    ///
    /// `owned_dir` is the daemon's data directory. When the socket lives
    /// inside it — the default — the directory is created and tightened to
    /// `0700`, which is what carries the access control on the BSD-derived
    /// kernels that ignore permission bits on the socket file itself. When an
    /// operator points `[listen] admin_socket` somewhere else (a systemd
    /// `RuntimeDirectory`, say), this **does not create or chmod that
    /// directory** — changing the mode of a path the daemon did not create
    /// would silently re-permission `/run`, or a directory shared by several
    /// replicas, and `set_permissions` follows symlinks — it **verifies** it:
    /// the directory must exist, be owned by the daemon's user, and carry no
    /// group/world bits, or the bind is refused. A warning would not do:
    /// socket access is the authority for formation and certificate signing,
    /// and on the kernels above the directory is the only thing enforcing
    /// it. (`RuntimeDirectoryMode=0700` produces a conforming directory.)
    ///
    /// A leftover socket file from a crashed process is removed, but only on
    /// the one error that proves nothing is listening. Any other connect
    /// failure — a live daemon running as another user (`EACCES`), a
    /// saturated backlog — is treated as "occupied", because unlinking there
    /// would take a running daemon's socket away, which is exactly what this
    /// check exists to prevent.
    pub(crate) async fn bind(path: &Path, owned_dir: &Path) -> Result<AdminSocket> {
        match path.parent() {
            Some(dir) if dir == owned_dir => {
                std::fs::create_dir_all(dir).with_context(|| {
                    format!("creating the admin socket directory {}", dir.display())
                })?;
                restrict_dir(dir)?;
            }
            Some(dir) => require_private_dir(dir)?,
            None => {}
        }

        if path.exists() {
            match UnixStream::connect(path).await {
                Ok(_) => bail!(
                    "another coordinator is already serving the admin socket {} — \
                     two daemons must not share a data directory",
                    path.display()
                ),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path).with_context(|| {
                        format!("removing the stale admin socket {}", path.display())
                    })?;
                }
                Err(e) => bail!(
                    "the admin socket {} exists and cannot be probed ({e}); refusing to \
                     replace it — if no coordinator is running, remove the file by hand",
                    path.display()
                ),
            }
        }

        let listener = UnixListener::bind(path)
            .with_context(|| format!("binding the admin socket {}", path.display()))?;
        restrict_socket(path)?;
        tracing::info!(socket = %path.display(), "local admin socket bound");
        Ok(AdminSocket {
            listener,
            path: path.to_path_buf(),
        })
    }

    /// Serve until `shutdown` flips, then unlink the socket.
    pub(crate) async fn serve(self, state: Arc<LocalAdmin>, mut shutdown: watch::Receiver<bool>) {
        loop {
            let stream = tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        tracing::warn!(error = %e, "admin socket: accept failed");
                        continue;
                    }
                },
                _ = shutdown.wait_for(|s| *s) => break,
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state).await {
                    tracing::warn!(error = %e, "admin socket: connection failed");
                }
            });
        }
        // Best effort: a leftover file is handled by the stale-socket check at
        // the next bind, so failing to unlink is not worth an error.
        let _ = std::fs::remove_file(&self.path);
        tracing::debug!(socket = %self.path.display(), "local admin socket closed");
    }
}

async fn handle_connection(stream: UnixStream, state: Arc<LocalAdmin>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read.take(MAX_REQUEST_BYTES))
        .read_line(&mut line)
        .await
        .context("reading the admin request")?;

    let reply = match serde_json::from_str::<AdminCall>(line.trim()) {
        Ok(call) => state.dispatch(call).await,
        Err(e) => AdminReply::Error {
            message: format!("malformed admin request: {e}"),
        },
    };

    let mut body = serde_json::to_vec(&reply).context("encoding the admin reply")?;
    body.push(b'\n');
    write
        .write_all(&body)
        .await
        .context("writing the admin reply")?;
    write.flush().await.context("flushing the admin reply")?;
    Ok(())
}

#[cfg(unix)]
fn restrict_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restricting {} to owner-only", dir.display()))
}

/// Refuse an operator-chosen socket directory that anyone but the daemon's
/// user can reach (ADR 0037 §3).
///
/// Access to the socket is the authority for formation and certificate
/// signing, and on BSD-derived kernels the containing directory's permission
/// bits are the *only* thing enforcing it (socket-file bits are ignored). So
/// the directory must be owned by this process's effective user and carry no
/// group or world bits at all — not merely no write bits, because search
/// permission is what lets another local user connect.
#[cfg(unix)]
fn require_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(dir).with_context(|| {
        format!(
            "inspecting the configured admin socket directory {}",
            dir.display()
        )
    })?;
    if !meta.is_dir() {
        bail!(
            "the configured admin socket parent {} is not a directory",
            dir.display()
        );
    }
    let euid = nix::unistd::geteuid().as_raw();
    if meta.uid() != euid {
        bail!(
            "the configured admin socket directory {} is owned by uid {} but this daemon \
             runs as uid {euid}; the socket's directory must be the daemon's own — local \
             access to the socket is the authority for formation (ADR 0037 §3)",
            dir.display(),
            meta.uid(),
        );
    }
    let mode = meta.mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "the configured admin socket directory {} has mode {mode:04o}; it must be \
             owner-only (0700) because directory permissions are what gate socket access \
             on several kernels, and socket access is the authority for formation \
             (ADR 0037 §3). For a systemd RuntimeDirectory, set RuntimeDirectoryMode=0700.",
            dir.display(),
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_private_dir(_dir: &Path) -> Result<()> {
    bail!("the coordinator admin socket requires a Unix platform")
}

#[cfg(unix)]
fn restrict_socket(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restricting {} to owner-only", path.display()))
}

#[cfg(not(unix))]
fn restrict_dir(_dir: &Path) -> Result<()> {
    bail!("the coordinator admin socket requires a Unix platform")
}

#[cfg(not(unix))]
fn restrict_socket(_path: &Path) -> Result<()> {
    bail!("the coordinator admin socket requires a Unix platform")
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Make one call on a daemon's admin socket.
///
/// No TLS, no target, no config beyond the socket path: the transport *is*
/// the authorization (ADR 0037 §3).
pub async fn call(socket: &Path, request: AdminCall) -> Result<AdminReply> {
    let stream = UnixStream::connect(socket).await.with_context(|| {
        format!(
            "connecting to the coordinator admin socket {} — is the daemon running, and does \
             this config name its data directory?",
            socket.display()
        )
    })?;
    let (read, mut write) = stream.into_split();

    let mut body = serde_json::to_vec(&request).context("encoding the admin request")?;
    body.push(b'\n');
    write
        .write_all(&body)
        .await
        .context("sending the admin request")?;
    write.flush().await.context("sending the admin request")?;
    drop(write);

    let mut line = String::new();
    BufReader::new(read.take(MAX_REQUEST_BYTES))
        .read_line(&mut line)
        .await
        .context("reading the admin reply")?;
    if line.trim().is_empty() {
        bail!("the coordinator closed the admin socket without replying");
    }
    serde_json::from_str(line.trim()).context("decoding the admin reply")
}

// ---------------------------------------------------------------------------
// CLI dispatch
// ---------------------------------------------------------------------------

/// `coppice coordinator init` (ADR 0037 §3).
pub(crate) async fn run_init(args: crate::cli::InitArgs) -> Result<()> {
    let socket = socket_path(&args.config)?;
    let reply = call(
        &socket,
        AdminCall::Init {
            policy: read_text(args.policy.as_deref(), "policy")?,
            operator_csr: read_text(args.operator_csr.as_deref(), "operator CSR")?,
            operator_cn: args.operator_cn,
        },
    )
    .await?;

    match reply {
        AdminReply::Formed {
            cluster_id,
            history_id,
            node_id,
            machine_id,
            operator,
            status,
        } => {
            println!("cluster {cluster_id} formed");
            println!("  history      {history_id}");
            println!("  node         {node_id}");
            println!("  machine      {machine_id}");
            println!("  phase        {}", phase_name(&status));
            println!();
            emit_operator(&operator, args.out_dir.as_deref())?;
            Ok(())
        }
        AdminReply::AlreadyInitialized { status } => {
            // A distinct outcome, not an error: automation retries `init` and
            // must be able to treat this as success (ADR 0037 §3).
            println!(
                "cluster {} is already initialized (phase {})",
                status.cluster_id,
                phase_name(&status)
            );
            println!(
                "  no changes made. To issue a fresh operator certificate, run \
                 `coppice coordinator admin issue-operator-cert`."
            );
            Ok(())
        }
        AdminReply::FormationFailed { reason, .. } => bail!("{reason}"),
        AdminReply::Error { message } => bail!("{message}"),
        other => bail!("unexpected reply to init: {other:?}"),
    }
}

/// `coppice coordinator admin issue-operator-cert` (ADR 0037 §3).
pub(crate) async fn run_issue_operator_cert(
    config: &Path,
    operator_csr: Option<&Path>,
    operator_cn: Option<String>,
    out_dir: Option<&Path>,
) -> Result<()> {
    let socket = socket_path(config)?;
    let reply = call(
        &socket,
        AdminCall::IssueOperatorCert {
            operator_csr: read_text(operator_csr, "operator CSR")?,
            operator_cn,
        },
    )
    .await?;

    match reply {
        AdminReply::Issued { operator } => emit_operator(&operator, out_dir),
        AdminReply::FormationFailed { reason, .. } => bail!("{reason}"),
        AdminReply::Error { message } => bail!("{message}"),
        other => bail!("unexpected reply to issue-operator-cert: {other:?}"),
    }
}

/// `coppice coordinator admin rotate-ca <verb>` (ADR 0037 §4).
///
/// On the local socket, like `init` and `issue-operator-cert`: re-rooting is
/// root-equivalent authority, and the runbook is
/// `docs/operations/re-rooting.md`.
pub(crate) async fn run_rotate_ca(config: &Path, verb: &crate::cli::RotateCaVerb) -> Result<()> {
    use crate::cli::RotateCaVerb;
    let socket = socket_path(config)?;

    match verb {
        RotateCaVerb::Begin => match call(&socket, AdminCall::RotateCaBegin).await? {
            AdminReply::RotationBegun { report } => {
                // Two independent facts, reported as two lines: what this call
                // did about *staging*, and whether it went on to *activate*.
                // Folding them into one sentence reads as a falsehood whenever
                // a single call does both — the staging line would say the
                // outgoing root still signs, moments after activation moved it.
                match (report.replaced_pending_root, report.resumed) {
                    (true, _) => println!(
                        "re-root resumed: the recorded pending root's key was not on this \
                         host's disk, so a REPLACEMENT pending root was minted and staged \
                         here. The superseded one left the bundle in the same commit, so it \
                         is no longer a trust anchor and any copy of its key is inert."
                    ),
                    (false, true) => println!(
                        "re-root resumed: the recorded rotation was picked up as it stood; \
                         nothing was re-minted."
                    ),
                    (false, false) => println!(
                        "re-root staged: the incoming root is recorded as PENDING, and both \
                         roots are now trust anchors."
                    ),
                }
                if report.activated {
                    println!(
                        "re-root ACTIVATED: every current voter durably holds the incoming \
                         key, so it is now the active signing root.\n"
                    );
                } else {
                    println!(
                        "the OUTGOING root still signs. Activation waits until every current \
                         voter durably holds the incoming key.\n"
                    );
                }
                if let Some(backup) = &report.key_backup {
                    println!("outgoing CA key preserved at {backup}");
                    println!(
                        "  that file is root-equivalent for as long as the outgoing root is \
                         trusted; treat it as such."
                    );
                    println!();
                }
                if report.distribution.is_empty() {
                    println!("no other voter to key (single-voter cluster)\n");
                } else {
                    println!("staged-key distribution to the other current voters:");
                    for peer in &report.distribution {
                        match (&peer.error, peer.already_held) {
                            (None, true) => println!(
                                "  node {:<6} {}  already held (nothing pushed)",
                                peer.node_id, peer.addr
                            ),
                            (None, false) => {
                                println!("  node {:<6} {}  installed", peer.node_id, peer.addr)
                            }
                            (Some(e), _) => {
                                println!("  node {:<6} {}  FAILED: {e}", peer.node_id, peer.addr)
                            }
                        }
                    }
                    println!();
                }
                print!("{}", render_rotation(&report.status));
                println!();

                // The refusal is a first-class outcome, not an error in the
                // rotation: staging succeeded, dual trust is in place, and the
                // outgoing root still signs — indefinitely and harmlessly. What
                // it is not is *done*, so this exits non-zero, with the marker
                // an operator can match on.
                if let Some(marker) = &report.refusal {
                    bail!(
                        "{marker}: the incoming root was NOT activated, because these current \
                         voters do not hold its key: {:?}.\n\n\
                         A root never becomes the active signing root until every current \
                         voter durably holds its private key — otherwise a failover could \
                         land the cluster on a voter that cannot sign. The rotation is parked \
                         in the staged phase, which is a working state: both roots are trust \
                         anchors and the outgoing root still signs, indefinitely if need be.\n\n\
                         Fix membership first — repair those hosts, or `admin replace-voter` / \
                         `admin remove-node` them — then re-run `rotate-ca begin`. It re-checks \
                         coverage against the CURRENT voter set and mints nothing it does not \
                         have to.",
                        report.missing_voters
                    );
                }
                println!(
                    "next: wait for every coordinator and agent to renew onto the new root \
                     (watch `rotate-ca status` on each), then run `rotate-ca complete`."
                );
                Ok(())
            }
            AdminReply::Error { message } => bail!("{message}"),
            other => bail!("unexpected reply to rotate-ca begin: {other:?}"),
        },

        RotateCaVerb::Status { json } => match call(&socket, AdminCall::RotateCaStatus).await? {
            AdminReply::RotationStatus { status } => {
                if *json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&status)
                            .context("encoding the rotation status")?
                    );
                } else {
                    print!("{}", render_rotation(&status));
                }
                Ok(())
            }
            AdminReply::Error { message } => bail!("{message}"),
            other => bail!("unexpected reply to rotate-ca status: {other:?}"),
        },

        RotateCaVerb::Complete { force } => {
            match call(&socket, AdminCall::RotateCaComplete { force: *force }).await? {
                AdminReply::RotationCompleted { report } => {
                    println!("re-root complete: the outgoing root is no longer trusted\n");
                    for root in &report.retired {
                        println!(
                            "retired  serial {}  ski {}",
                            root.serial,
                            root.subject_key_id.as_deref().unwrap_or("-")
                        );
                    }
                    println!();
                    print!("{}", render_rotation(&report.status));
                    Ok(())
                }
                AdminReply::Error { message } => bail!("{message}"),
                other => bail!("unexpected reply to rotate-ca complete: {other:?}"),
            }
        }
    }
}

/// The human table for `rotate-ca status`.
fn render_rotation(s: &rotate::RotationStatus) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "rotation      {}",
        match s.phase {
            rotate::RotationPhase::None => "none (single root)",
            rotate::RotationPhase::Staged =>
                "STAGED — incoming root pending; the OUTGOING root still signs",
            rotate::RotationPhase::Distributing =>
                "DISTRIBUTING — staged key going to the voters; the OUTGOING root still signs",
            rotate::RotationPhase::ActivePendingSwap =>
                "ACTIVE, PENDING LOCAL SWAP — the incoming root signs, but not on this host yet",
            rotate::RotationPhase::CompleteEligible =>
                "ACTIVE (dual trust) — ready for `rotate-ca complete` once its clock allows",
        }
    );
    let _ = writeln!(out, "recorded at   {} (unix us)", s.recorded_at_us);
    for root in &s.roots {
        let _ = writeln!(
            out,
            "  root {}      {}  cn={}  serial={}  ski={}",
            root.position,
            if root.active {
                "ACTIVE (signs)"
            } else if root.pending {
                "PENDING      "
            } else {
                "trusted only "
            },
            root.common_name.as_deref().unwrap_or("-"),
            root.serial,
            root.subject_key_id.as_deref().unwrap_or("-"),
        );
    }
    // The activation gate, per voter. This is the whole operator surface for
    // "why has my rotation not activated": a rotation parks in the staged
    // phase until every current voter holds the staged key, and these lines
    // name the ones that do not.
    if s.staged_root_serial.is_some() {
        for voter in &s.voters {
            let state = if s.staged_key_holders.contains(voter) {
                "holds the staged key"
            } else if s.staged_key_transfers.contains(voter) {
                "MAY hold it (unresolved transfer intent) — not counted by the gate"
            } else {
                "DOES NOT hold the staged key"
            };
            let _ = writeln!(out, "  voter {voter:<21} {state}");
        }
        if s.missing_voters.is_empty() {
            let _ = writeln!(
                out,
                "activation    every current voter holds the staged key; the next \
                 `rotate-ca begin` activates it"
            );
        } else {
            let _ = writeln!(
                out,
                "activation    BLOCKED on {:?} — `rotate-ca begin` will refuse to activate \
                 until every current voter holds the staged key. Repair or replace those \
                 voters, then re-run it.",
                s.missing_voters
            );
        }
    }
    let _ = writeln!(
        out,
        "this replica  anchor {} · leaf {} · signing key {}",
        if s.installed_matches_replicated {
            "current"
        } else {
            "STALE (does not trust the recorded bundle)"
        },
        if s.leaf_under_active_root {
            "renewed onto the active root"
        } else {
            "STALE (still under the outgoing root)"
        },
        if s.local_key_signs_active_root {
            "signs the active root"
        } else {
            "does NOT sign the active root"
        },
    );
    let _ = writeln!(out, "node          {} (leader {:?})", s.local_id, s.leader);
    if let Some(earliest) = s.earliest_complete_us {
        let _ = writeln!(
            out,
            "complete at   {earliest} (unix us) — one leaf lifetime ({}s) after begin; \
             --force overrides",
            s.leaf_lifetime_secs
        );
    }
    let _ = writeln!(
        out,
        "key holders   {:?}   pending transfers {:?}",
        s.key_holders, s.pending_key_transfers
    );
    // Staged holders are listed separately and are every bit as
    // root-equivalent: the pending root is a trust anchor from the moment it
    // is staged, so a leaf minted with the staged key already verifies
    // fleet-wide. They merge into `key holders` at activation.
    if s.staged_root_serial.is_some() {
        let _ = writeln!(
            out,
            "staged key    holders {:?}   pending transfers {:?}",
            s.staged_key_holders, s.staged_key_transfers
        );
    }
    out
}

/// `coppice coordinator admin local-status`: this daemon's own readiness
/// document over the local socket (ADR 0037 §3/§9).
///
/// A verb of its own, distinct from `admin status`, because the two answer
/// different questions with different stable JSON shapes: `status` is the
/// *cluster's* membership/bindings document over the network, and this is
/// the *daemon's* readiness document — the only status a parked or
/// formation-failed daemon can serve, since there is no formed cluster
/// behind the network verbs and possibly no TLS material to dial with. It
/// therefore never touches the network path at all. The document is the same
/// [`ReadyzReport`] `GET /readyz` serves; `--json` prints it verbatim (it is
/// already the documented stable JSON of ADR 0037 §9), and the default
/// renders the same fields as a human table.
pub(crate) async fn run_status(config: &Path, json: bool) -> Result<()> {
    let socket = socket_path(config)?;
    let status = match call(&socket, AdminCall::Status).await? {
        AdminReply::Status { status } => status,
        AdminReply::Error { message } => bail!("{message}"),
        other => bail!("unexpected reply to status: {other:?}"),
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&status).context("encoding the status document")?
        );
    } else {
        print!("{}", render_local_status(&status));
    }
    Ok(())
}

/// The human table for the local `status` verb: the readiness document's
/// fields, one per line, `-` for the ones this phase does not have.
fn render_local_status(s: &ReadyzReport) -> String {
    use std::fmt::Write as _;
    let opt = |v: Option<String>| v.unwrap_or_else(|| "-".to_string());

    let mut out = String::new();
    let _ = writeln!(out, "phase         {}", phase_name(s));
    let _ = writeln!(out, "cluster       {}", s.cluster_id);
    let _ = writeln!(out, "history       {}", opt(s.history_id.clone()));
    let _ = writeln!(
        out,
        "node          {}",
        opt(s.node_id.map(|n| n.to_string()))
    );
    let leader = match s.leader {
        Some(id) if s.is_leader => format!("{id} (this node)"),
        Some(id) => id.to_string(),
        None => "-".to_string(),
    };
    let _ = writeln!(out, "leader        {leader}");
    let _ = writeln!(out, "applied       {}", s.applied_index);
    let _ = writeln!(out, "committed     {}", s.committed_index);
    let _ = writeln!(out, "lag           {}", s.replication_lag);
    let _ = writeln!(
        out,
        "formed        {} ({}/{} voters)",
        s.formed,
        s.voters.len(),
        s.cluster_size
    );
    for voter in &s.voters {
        let _ = writeln!(out, "  voter {:<6} {}", voter.node_id, voter.address);
    }
    if let Some(refusal) = &s.last_admission_refusal {
        let _ = writeln!(out, "last admission refusal:");
        let _ = writeln!(out, "  {refusal}");
    }
    if let Some(reason) = &s.reason {
        let _ = writeln!(out, "note: {reason}");
    }
    out
}

/// The admin socket a config file's daemon serves on.
fn socket_path(config: &Path) -> Result<PathBuf> {
    let resolved = crate::config::load(config)
        .with_context(|| format!("reading config {}", config.display()))?;
    Ok(resolved.config.admin_socket_path())
}

fn read_text(path: Option<&Path>, what: &str) -> Result<Option<String>> {
    path.map(|p| {
        std::fs::read_to_string(p)
            .with_context(|| format!("reading the {what} file {}", p.display()))
    })
    .transpose()
}

fn phase_name(status: &ReadyzReport) -> String {
    serde_json::to_value(status.phase)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Print the issued material, or write it out.
///
/// Printing is the default because the ADR's picture is an operator's SSH
/// session collecting it: on the no-CSR path the private key exists nowhere
/// else, and writing it into a file the operator did not ask for would leave
/// it on the coordinator host — the one place it should not stay.
fn emit_operator(operator: &OperatorPem, out_dir: Option<&Path>) -> Result<()> {
    let Some(dir) = out_dir else {
        println!("{}", operator.cert_pem.trim_end());
        if let Some(key) = &operator.key_pem {
            println!("{}", key.trim_end());
        }
        println!("{}", operator.ca_pem.trim_end());
        if operator.key_pem.is_some() {
            eprintln!(
                "note: the operator private key above was minted for you and is stored \
                 nowhere else — save it now."
            );
        }
        return Ok(());
    };

    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating the output directory {}", dir.display()))?;
    write_pem(
        &dir.join("operator.crt"),
        operator.cert_pem.as_bytes(),
        false,
    )?;
    write_pem(&dir.join("ca.crt"), operator.ca_pem.as_bytes(), false)?;
    if let Some(key) = &operator.key_pem {
        write_pem(&dir.join("operator.key"), key.as_bytes(), true)?;
    }
    println!("wrote operator credentials to {}", dir.display());
    Ok(())
}

fn write_pem(path: &Path, bytes: &[u8], private: bool) -> Result<()> {
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restricting {} to owner-only", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = private;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calls_round_trip_through_the_wire_form() {
        let call = AdminCall::Init {
            policy: Some("[[quota_entity]]\n".into()),
            operator_csr: None,
            operator_cn: Some("alice".into()),
        };
        let encoded = serde_json::to_string(&call).expect("encode");
        assert!(encoded.contains(r#""verb":"init""#), "{encoded}");
        let decoded: AdminCall = serde_json::from_str(&encoded).expect("decode");
        assert!(matches!(
            decoded,
            AdminCall::Init {
                operator_cn: Some(cn),
                ..
            } if cn == "alice"
        ));
    }

    #[test]
    fn absent_optional_fields_decode() {
        let decoded: AdminCall = serde_json::from_str(r#"{"verb":"init"}"#).expect("decode");
        assert!(matches!(
            decoded,
            AdminCall::Init {
                policy: None,
                operator_csr: None,
                operator_cn: None
            }
        ));
    }

    #[test]
    fn replies_carry_their_outcome_tag() {
        let reply = AdminReply::Error {
            message: "nope".into(),
        };
        let encoded = serde_json::to_string(&reply).expect("encode");
        assert!(encoded.contains(r#""outcome":"error""#), "{encoded}");
    }

    #[tokio::test]
    async fn binding_twice_on_a_live_socket_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("admin.sock");
        let first = AdminSocket::bind(&path, dir.path())
            .await
            .expect("first bind");
        let err = AdminSocket::bind(&path, dir.path())
            .await
            .expect_err("second bind must be refused");
        assert!(
            err.to_string().contains("already serving"),
            "{}",
            err.to_string()
        );
        drop(first);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_explicit_socket_directory_must_be_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        // The socket outside the data directory: the daemon verifies rather
        // than chmods, because it did not create that directory.
        let data = tempfile::tempdir().expect("data dir");
        let outside = tempfile::tempdir().expect("socket dir");

        // Loose directory (world-searchable): refused outright — on several
        // kernels the directory bits are the only thing gating socket access,
        // and socket access is the authority for formation.
        std::fs::set_permissions(outside.path(), std::fs::Permissions::from_mode(0o755))
            .expect("loosen dir");
        let err = AdminSocket::bind(&outside.path().join("admin.sock"), data.path())
            .await
            .expect_err("a loose explicit directory must be refused");
        assert!(err.to_string().contains("owner-only"), "{err:#}");

        // Owner-only directory: accepted, and never chmodded by us.
        std::fs::set_permissions(outside.path(), std::fs::Permissions::from_mode(0o700))
            .expect("tighten dir");
        AdminSocket::bind(&outside.path().join("admin.sock"), data.path())
            .await
            .expect("an owner-only explicit directory binds");
    }

    #[tokio::test]
    async fn a_stale_socket_file_is_reclaimed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("admin.sock");
        // A crashed daemon's leftover: the file exists, nothing listens.
        drop(UnixListener::bind(&path).expect("bind"));
        assert!(path.exists());
        AdminSocket::bind(&path, dir.path())
            .await
            .expect("reclaim stale socket");
    }
}
