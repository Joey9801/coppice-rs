//! Node configuration file (ADR 0020).
//!
//! Every coordinator process reads exactly one TOML file at startup: listen
//! and advertise addresses, the data directory, TLS paths, Raft
//! liveness timing, SSO connection parameters, and observability settings.
//! Anything two replicas must agree on — quotas, decay policy, retention,
//! authorization mappings — is **cluster policy** instead, held in replicated
//! state and changed through `coppice-cli policy …`; it never appears here
//! (ADR 0020's litmus test). The cluster id is cross-checked against the
//! data directory's stamped identity at startup (ADR 0016) — this module
//! only parses it, the check itself lives in bootstrap. The replica's Raft
//! node id is deliberately *not* configuration: it is minted at init and
//! read back from the manifest stamp (ADR 0025).
//!
//! Unknown keys are startup errors (`deny_unknown_fields`): a typo'd knob
//! fail-stops naming the offending key rather than silently defaulting.
//! Durations are humane strings (`"1500ms"`, `"24h"`) via `humantime-serde`,
//! which rejects bare integers by construction — deliberately, so an
//! unlabelled `1500` cannot silently mean milliseconds, seconds, or a bug.
//!
//! The CLI surface is now exactly `--config` (ADR 0037 §1): startup intent is
//! *derived* from what the data directory says, not declared, so there is no
//! override layer left to merge and every knob resolves file-over-default via
//! `serde` defaults. [`load`] is the single place a config file becomes a
//! [`ResolvedConfig`], and the only values it computes rather than reads are
//! the ones a fleet must be able to leave blank in a byte-identical artifact
//! (chiefly `advertise_host`, ADR 0037 §2).

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use coppice_core::id::ClusterId;
use serde::Deserialize;

pub(crate) use client_tls::{ClientTlsConfig, ClientTlsPosture};
pub(crate) use discovery::{BackendKind, DiscoveryConfig};
// The `[enrollment]` table is the SHARED definition in `coppice-enroll` — the
// same type the agent parses, so the two daemons cannot disagree about what
// `insecure` means, and its `Secret` token field redacts itself from every
// `Debug` rendering (the whole config is logged at startup).
pub(crate) use coppice_enroll::EnrollmentConfig;

mod client_tls {
    //! The `[client_tls]` section (ADR 0037 §4): the **public** listener's own
    //! serving posture.
    //!
    //! Two honest modes and no third: an externally-signed certificate (or a
    //! TLS-terminating load balancer in front of one), or plain HTTP under a
    //! conspicuous opt-in. A cluster-issued leaf was considered and rejected —
    //! browsers will never trust a private root — so the cluster CA's only role
    //! on this listener is verifying *client* certificates. Nothing here
    //! defaults: a deployment states which of the two it is, because the
    //! difference is whether enrollment tokens cross the network in the clear.

    use std::path::PathBuf;

    use serde::Deserialize;

    /// The `[client_tls]` section as written.
    ///
    /// The invariant is a XOR, which serde cannot express, so the fields are
    /// individually optional and [`ClientTlsConfig::posture`] is the only way
    /// to read them — it either yields one of the two modes or an error naming
    /// both.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct ClientTlsConfig {
        #[serde(default)]
        pub(crate) cert_path: Option<PathBuf>,
        #[serde(default)]
        pub(crate) key_path: Option<PathBuf>,
        /// Serve plain HTTP. Development/test only: it exposes enrollment
        /// tokens on the wire (ADR 0037 §4).
        #[serde(default)]
        pub(crate) insecure: bool,
    }

    /// The resolved posture of the client listener.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum ClientTlsPosture {
        /// Serve HTTPS from these paths, hot-reloaded like `[tls]`.
        Tls { cert: PathBuf, key: PathBuf },
        /// Serve plain HTTP, explicitly and conspicuously.
        Insecure,
    }

    /// The one error message, shared by "neither configured" and every partial
    /// combination, because the fix is the same: say which of the two modes
    /// this deployment is in.
    fn ambiguous(detail: &str) -> anyhow::Error {
        anyhow::anyhow!(
            "[client_tls] {detail}. The public listener's posture is explicit, never implied: \
             set BOTH `cert_path` and `key_path` to an externally-signed serving certificate \
             (the production posture — or terminate TLS in a load balancer in front), OR set \
             `insecure = true` to serve plain HTTP, which is development/test only because it \
             exposes enrollment tokens on the wire (ADR 0037 §4)"
        )
    }

    impl ClientTlsConfig {
        /// Resolve the section into exactly one posture, or fail naming both
        /// options.
        pub(crate) fn posture(&self) -> anyhow::Result<ClientTlsPosture> {
            match (&self.cert_path, &self.key_path, self.insecure) {
                (Some(cert), Some(key), false) => Ok(ClientTlsPosture::Tls {
                    cert: cert.clone(),
                    key: key.clone(),
                }),
                (None, None, true) => Ok(ClientTlsPosture::Insecure),
                (Some(_), Some(_), true) => Err(ambiguous(
                    "names a serving certificate AND sets `insecure = true`",
                )),
                (Some(_), None, _) => Err(ambiguous("has `cert_path` but no `key_path`")),
                (None, Some(_), _) => Err(ambiguous("has `key_path` but no `cert_path`")),
                (None, None, false) => Err(ambiguous("configures neither mode")),
            }
        }

        /// The posture for a section that is absent entirely — the same
        /// "neither" error, so a missing table and an empty one read alike.
        pub(crate) fn absent() -> anyhow::Error {
            ambiguous("is missing")
        }
    }
}

mod discovery {
    //! The `[discovery]` config section (ADR 0037 §2).
    //!
    //! Discovery answers "whom might I dial first?", never "who are the
    //! voters?"; its output is advisory seed addresses only. The section names
    //! a `backend` and carries exactly one matching backend table. It also
    //! carries `cluster_size`, which convergence consults before replicated
    //! state is reachable — the same node-local justification as `cluster_id`.

    use std::path::PathBuf;
    use std::time::Duration;

    use serde::Deserialize;

    /// The `[discovery]` section: which backend seeds candidate raft addresses
    /// and the expected voter count.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct DiscoveryConfig {
        /// Which backend supplies candidate raft addresses. Exactly one
        /// matching backend table must be present (validated in
        /// [`DiscoveryConfig::validate`]).
        #[serde(default)]
        pub(crate) backend: BackendKind,

        /// Expected voter count. Node-local config (ADR 0037 §2): consulted by
        /// convergence, the leader's removal rule (§7), and the
        /// formation-complete signal (§9) before replicated state is
        /// reachable.
        #[serde(default = "default_cluster_size")]
        pub(crate) cluster_size: usize,

        /// How long a voter may go without acknowledging leader contact
        /// before the leader's evidence-gated removal path (ADR 0037 §7) may
        /// fold it out of the voter set — the leader's own
        /// heartbeat-acknowledgement evidence, never log-position progress
        /// (which stalls identically for a dead peer and an idle-but-live
        /// one). Node-local coordinator config, like `cluster_size` above.
        #[serde(default = "default_removal_grace", with = "humantime_serde")]
        pub(crate) removal_grace: Duration,

        /// How long a learner may go without acknowledging leader contact
        /// before the periodic learner-GC task (ADR 0037 §7) retires its
        /// bound machine identity and removes its seat. Node-local
        /// coordinator config, like `cluster_size` above.
        #[serde(default = "default_learner_expiry", with = "humantime_serde")]
        pub(crate) learner_expiry: Duration,

        /// `[discovery.static]` — present iff `backend = "static"`.
        #[serde(default, rename = "static")]
        pub(crate) static_backend: Option<StaticBackend>,

        /// `[discovery.dns]` — present iff `backend = "dns"`.
        #[serde(default)]
        pub(crate) dns: Option<DnsBackend>,

        /// `[discovery.file]` — present iff `backend = "file"`.
        #[serde(default)]
        pub(crate) file: Option<FileBackend>,

        /// `[discovery.ec2_asg]` — present iff `backend = "ec2-asg"`
        /// (ADR 0037 §2).
        #[serde(default)]
        pub(crate) ec2_asg: Option<Ec2AsgBackend>,
    }

    /// The discovery backend selector. TOML spelling matches ADR 0037 §2.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub(crate) enum BackendKind {
        /// The literal seed list — today's `peers`, under a new roof.
        #[default]
        Static,
        /// Resolve one DNS name per consultation (A/AAAA + SRV).
        Dns,
        /// Enumerate a well-known directory of run-scoped registration files.
        File,
        /// EC2 auto-scaling-group membership (ADR 0037 §2).
        Ec2Asg,
    }

    /// `[discovery.static]`: the literal list of dialable raft addresses.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct StaticBackend {
        #[serde(default)]
        pub(crate) addrs: Vec<String>,
    }

    /// `[discovery.dns]`: one name resolved per consultation. SRV records
    /// supply their own ports; A/AAAA records use `port`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct DnsBackend {
        pub(crate) name: String,
        /// Fallback port for A/AAAA records that carry none.
        pub(crate) port: u16,
    }

    /// `[discovery.file]`: a directory of run-scoped registration files, each
    /// naming one candidate on its first line.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct FileBackend {
        pub(crate) dir: PathBuf,
    }

    /// `[discovery.ec2_asg]`: the EC2 auto-scaling-group backend (ADR 0037 §2).
    ///
    /// The instance id and region are read from EC2 instance metadata (IMDSv2)
    /// at each consultation, so neither is configured here. `port` is required:
    /// discovery composes `private-ip:port` candidates and the raft listen port
    /// is not plumbed into the discovery builder, so the operator names it
    /// explicitly (the same shape as `[discovery.dns].port`).
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct Ec2AsgBackend {
        /// The raft port composed onto every discovered instance's private IP.
        pub(crate) port: u16,
        /// Explicit AWS region override. Optional: when unset the region is
        /// taken from this instance's IMDS document, which is the normal case
        /// for a coordinator running inside the group it discovers.
        #[serde(default)]
        pub(crate) region: Option<String>,
        /// Per-AWS-call timeout. Discovery must never hang startup (ADR 0037 §2
        /// contract), so each IMDS/ASG/EC2 call is bounded by this and a slow or
        /// unreachable control plane degrades to an empty candidate list with a
        /// warning rather than blocking convergence.
        #[serde(default = "default_ec2_asg_timeout", with = "humantime_serde")]
        pub(crate) timeout: Duration,
    }

    impl DiscoveryConfig {
        /// Reject a section whose backend tables do not match `backend`:
        /// exactly the one table matching `backend` must be present — no
        /// foreign backend table, and no absent table (`static` included: an
        /// operator with no seeds writes an explicit empty `addrs`, so the
        /// migration off the old top-level `peers` is always visible in the
        /// file, ADR 0037 §2).
        pub(crate) fn validate(&self) -> anyhow::Result<()> {
            // No foreign tables.
            let foreign = [
                (self.backend != BackendKind::Static && self.static_backend.is_some())
                    .then_some("static"),
                (self.backend != BackendKind::Dns && self.dns.is_some()).then_some("dns"),
                (self.backend != BackendKind::File && self.file.is_some()).then_some("file"),
                (self.backend != BackendKind::Ec2Asg && self.ec2_asg.is_some())
                    .then_some("ec2_asg"),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            if !foreign.is_empty() {
                anyhow::bail!(
                    "[discovery] backend = \"{}\" but unrelated table(s) present: {} — \
                     keep exactly the one matching table (ADR 0037 §2)",
                    self.backend.as_str(),
                    foreign.join(", "),
                );
            }

            // Required table present, for every backend including static.
            match self.backend {
                BackendKind::Static if self.static_backend.is_none() => {
                    anyhow::bail!(
                        "[discovery] backend = \"static\" requires a [discovery.static] table \
                         with `addrs` (an explicit empty list is valid, ADR 0037 §2)"
                    );
                }
                BackendKind::Static => {}
                BackendKind::Dns if self.dns.is_none() => {
                    anyhow::bail!(
                        "[discovery] backend = \"dns\" requires a [discovery.dns] table \
                         with `name` and `port` (ADR 0037 §2)"
                    );
                }
                BackendKind::File if self.file.is_none() => {
                    anyhow::bail!(
                        "[discovery] backend = \"file\" requires a [discovery.file] table \
                         with `dir` (ADR 0037 §2)"
                    );
                }
                BackendKind::Ec2Asg if self.ec2_asg.is_none() => {
                    anyhow::bail!(
                        "[discovery] backend = \"ec2-asg\" requires a [discovery.ec2_asg] \
                         table with `port` (ADR 0037 §2)"
                    );
                }
                _ => {}
            }
            Ok(())
        }

        /// The static seed list, if this config selects the `static` backend —
        /// the successor to the old top-level `peers`.
        pub(crate) fn static_addrs(&self) -> &[String] {
            match (self.backend, &self.static_backend) {
                (BackendKind::Static, Some(s)) => &s.addrs,
                _ => &[],
            }
        }
    }

    impl BackendKind {
        /// The TOML spelling, for operator-facing messages.
        pub(crate) fn as_str(self) -> &'static str {
            match self {
                BackendKind::Static => "static",
                BackendKind::Dns => "dns",
                BackendKind::File => "file",
                BackendKind::Ec2Asg => "ec2-asg",
            }
        }
    }

    fn default_cluster_size() -> usize {
        3
    }

    fn default_removal_grace() -> Duration {
        Duration::from_secs(120)
    }

    fn default_learner_expiry() -> Duration {
        Duration::from_secs(3600)
    }

    fn default_ec2_asg_timeout() -> Duration {
        Duration::from_secs(3)
    }
}

/// The coordinator's fully-parsed node configuration file.
///
/// Node-local only, per ADR 0020: everything here is either safe to vary per
/// replica (addresses, paths, Raft timing) or, for SSO, the *connection*
/// half of a split that keeps the authorization-shaped half in replicated
/// policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The operator-chosen **logical** cluster name every replica shares
    /// (ADR 0020/0037): the value a fresh daemon matches on when probing, and
    /// the one identity that survives a wipe-and-re-form. Deliberately
    /// distinct from the `history_id` formation mints and stamps into data
    /// directories (ADR 0037 §3) — that one names a single raft lifetime.
    /// For directories the legacy `--bootstrap`/`--join` flags created, the
    /// stamp is derived from this value and cross-checked at startup
    /// (ADR 0016). Parsed from the typed string form `cluster-<uuid>`
    /// (ADR 0024).
    pub(crate) cluster_id: ClusterId,

    /// Root of this replica's on-disk state (segment storage, manifest).
    pub(crate) data_dir: PathBuf,

    /// Coordinator discovery (ADR 0037 §2): which backend seeds candidate
    /// raft addresses. Subsumes the old top-level `peers` list (now
    /// `[discovery.static] addrs`). Required, with exactly one backend table
    /// matching `backend` — an old config still carrying `peers` fail-stops
    /// naming the key rather than silently discovering nothing. Seed-only,
    /// never authoritative: the addresses that matter for consensus live in
    /// replicated membership.
    pub(crate) discovery: DiscoveryConfig,

    /// Listen and advertise addresses.
    pub(crate) listen: ListenConfig,

    /// Raft liveness timing. Optional: the defaults suit ordinary
    /// datacenter networks.
    #[serde(default)]
    pub(crate) raft: RaftConfig,

    /// Convergence-loop pacing. Optional: the defaults suit ordinary
    /// deployments; the integration-test fixture shortens them so a fleet
    /// forms in milliseconds instead of production seconds.
    #[serde(default)]
    pub(crate) pacing: PacingConfig,

    /// Argon2id cost for hashing enrollment-token secrets. Optional; the
    /// defaults are the `argon2` crate's recommended production parameters.
    #[serde(default)]
    pub(crate) token_kdf: TokenKdfConfig,

    /// mTLS material for intra-cluster traffic (ADR 0011, day one). Required:
    /// there is no insecure fallback.
    pub(crate) tls: TlsConfig,

    /// The public client listener's own serving posture (ADR 0037 §4).
    /// **Required** — an absent section is the "neither mode configured"
    /// error, not a default. Optional to *serde* only so that error can name
    /// both options instead of reading "missing field `client_tls`"; every
    /// reader goes through [`Config::client_tls_posture`].
    #[serde(default)]
    client_tls: Option<ClientTlsConfig>,

    /// How this installation enrolls for its own machine leaf when it has none
    /// (ADR 0037 §4). Optional: a formed voter, or one whose material is
    /// supplied by an external PKI, never enrolls. Validated here, consumed by
    /// the convergence loop's enroll step ([`crate::convergence`]), which
    /// retries it every tick until a usable leaf exists.
    #[serde(default)]
    pub(crate) enrollment: Option<EnrollmentConfig>,

    /// SSO connection parameters, if this deployment uses SSO. `None` when
    /// the section is absent entirely. Only the *connection* shape lives
    /// here — role/group-to-admin mappings are policy (ADR 0020). Parsed now;
    /// the API server that consumes it is a later change.
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) sso: Option<SsoConfig>,

    /// Logging, tracing, and metrics endpoints. Optional: all fields default.
    #[serde(default)]
    pub(crate) observability: ObservabilityConfig,
}

/// Listen and advertise addresses for the coordinator's three server ports.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListenConfig {
    /// User/CLI API address: the JSON-over-HTTP client edge (ADR 0031).
    #[serde(default = "default_client_addr")]
    pub(crate) client_addr: SocketAddr,

    /// Coordinator peer (Raft) traffic address.
    #[serde(default = "default_raft_addr")]
    pub(crate) raft_addr: SocketAddr,

    /// Agent heartbeat and report address: the dedicated mTLS listener the
    /// agent gateway binds for `coppice.agent.v1.AgentService` sessions
    /// (ADR 0009/0011).
    #[serde(default = "default_agent_addr")]
    pub(crate) agent_addr: SocketAddr,

    /// The hostname peers and agents dial. Optional (ADR 0037 §2): when unset
    /// it is resolved via the fallback chain in [`resolve_advertise_host`]
    /// (explicit value ▸ resolvable system FQDN ▸ default-route local
    /// address), so a
    /// production fleet can ship one byte-identical config artifact. [`load`]
    /// resolves it once and stores the result back here; every reader after
    /// load sees the concrete value.
    #[serde(default)]
    pub(crate) advertise_host: Option<String>,

    /// The local admin socket (ADR 0037 §3): the Unix-domain-socket surface
    /// `coppice coordinator init` and `admin issue-operator-cert` speak to.
    ///
    /// Defaults to `<data_dir>/admin.sock`. The data directory is the
    /// honest default home for it: it is the one path an operator already
    /// configures per replica, it already holds owner-only key material, and
    /// it is created before any listener binds. A deployment that wants the
    /// socket in a systemd `RuntimeDirectory` sets this explicitly.
    ///
    /// Local access to this socket **is** the authority for the verbs it
    /// carries — there is no further authentication on it. The daemon
    /// tightens the socket to owner-only at bind, and the directory too when
    /// it is the daemon's own data directory (the default). An explicitly
    /// configured directory is **verified**, not chmodded — it must already
    /// be owned by the daemon's user with mode 0700, or the bind is refused
    /// (for a systemd `RuntimeDirectory`, set `RuntimeDirectoryMode=0700`).
    #[serde(default)]
    pub(crate) admin_socket: Option<PathBuf>,
}

impl ListenConfig {
    /// The Raft address this replica advertises to peers: the resolved
    /// `advertise_host` combined with the port half of
    /// [`raft_addr`](ListenConfig::raft_addr).
    ///
    /// Kept as a method rather than a stored field so the two can never
    /// silently drift apart when either is edited. [`load`] guarantees
    /// `advertise_host` is resolved to `Some` before any reader calls this.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn advertised_raft_addr(&self) -> String {
        self.advertised_raft_addr_on_port(self.raft_addr.port())
    }

    /// The Raft address this replica advertises on a *specific* bound port: the
    /// resolved `advertise_host` combined with `port`.
    ///
    /// Used when `raft_addr` requests port 0 (the multi-process dev case):
    /// bootstrap binds the listener first, learns the real port, and advertises
    /// *that* — so a `:0` config never publishes `host:0` to discovery or
    /// membership. An IPv6 `advertise_host` is bracketed into the valid
    /// `[v6]:port` authority form (the form `coppice_tls::split_host_port` and
    /// every dial seam expect).
    pub(crate) fn advertised_raft_addr_on_port(&self, port: u16) -> String {
        let host = self
            .advertise_host
            .as_deref()
            .expect("advertise_host resolved by config::load before use");
        if host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        }
    }
}

/// Resolve the address peers dial, per ADR 0037 §2's fallback chain:
/// explicit config value ▸ the system FQDN ▸ the local address of the
/// default route — through the production seams. See
/// [`resolve_advertise_host_with`] for the rules.
fn resolve_advertise_host(explicit: Option<&str>) -> Result<String> {
    resolve_advertise_host_with(
        explicit,
        system_hostname,
        host_resolves,
        default_route_local_addr,
    )
}

/// [`resolve_advertise_host`] with injectable hostname/resolution/route seams
/// (the fallback chain is behavior worth unit-testing; the seams are I/O).
///
/// The hostname step accepts only a **dialable FQDN**: the OS-reported name
/// must be FQDN-shaped (contain a dot — `gethostname` on a plainly-configured
/// host returns a bare short name, which peers on other hosts generally cannot
/// resolve) AND actually resolve on this host. Anything else falls through to
/// the default-route address, so a non-resolvable name is never published into
/// discovery or Raft membership. This is what keeps the byte-identical-config
/// story honest: a fleet whose hosts carry proper FQDNs advertises them; any
/// other fleet advertises a routable IP instead of a broken name.
fn resolve_advertise_host_with(
    explicit: Option<&str>,
    hostname: impl FnOnce() -> Option<String>,
    resolves: impl FnOnce(&str) -> bool,
    default_route: impl FnOnce() -> Option<String>,
) -> Result<String> {
    if let Some(host) = explicit {
        return Ok(host.to_string());
    }
    if let Some(name) = hostname() {
        if name.contains('.') && resolves(&name) {
            tracing::info!(advertise_host = %name, source = "system-fqdn", "resolved advertise_host");
            return Ok(name);
        }
        tracing::info!(
            hostname = %name,
            "system hostname is not a resolvable FQDN; falling back to the default route"
        );
    }
    if let Some(addr) = default_route() {
        tracing::info!(advertise_host = %addr, source = "default-route", "resolved advertise_host");
        return Ok(addr);
    }
    bail!(
        "advertise_host is unset and could not be resolved: the system hostname is not a \
         resolvable FQDN and no default route was found. Set `listen.advertise_host` \
         explicitly to the address peers and agents should dial (ADR 0037 §2)."
    );
}

/// The OS-reported hostname, or `None` if it is empty or not valid UTF-8.
/// Acceptance (FQDN shape + resolvability) is judged by the caller.
fn system_hostname() -> Option<String> {
    let name = gethostname::gethostname().into_string().ok()?;
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Whether `host` resolves to at least one address on this host (getaddrinfo
/// via `ToSocketAddrs`). Blocking, but `load` runs once at startup before any
/// runtime exists.
fn host_resolves(host: &str) -> bool {
    use std::net::ToSocketAddrs;
    (host, 0u16)
        .to_socket_addrs()
        .map(|mut addrs| addrs.next().is_some())
        .unwrap_or(false)
}

/// The local address of the default route: bind an unconnected UDP socket and
/// `connect` it toward a public address so the kernel selects the egress
/// interface, then read back the socket's local address. No traffic is sent
/// (UDP `connect` only records the peer). IPv4 is probed first; an
/// IPv6-only host falls back to the IPv6 probe (the composed authority is
/// bracketed by [`ListenConfig::advertised_raft_addr_on_port`]).
fn default_route_local_addr() -> Option<String> {
    for (bind, probe) in [
        ("0.0.0.0:0", "8.8.8.8:80"),
        ("[::]:0", "[2001:4860:4860::8888]:80"),
    ] {
        let Ok(socket) = std::net::UdpSocket::bind(bind) else {
            continue;
        };
        if socket.connect(probe).is_err() {
            continue;
        }
        if let Ok(local) = socket.local_addr() {
            if !local.ip().is_unspecified() {
                return Some(local.ip().to_string());
            }
        }
    }
    None
}

/// Raft liveness tuning.
///
/// Per ADR 0020, these affect only liveness (elections, heartbeats), never
/// safety, so they are node-local and safe to vary per replica — but the
/// defaults are right for ordinary datacenter networks and this section
/// should rarely need editing.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RaftConfig {
    /// Minimum election timeout; openraft randomizes the actual timeout in
    /// `[t, 2t]` per election to avoid split votes.
    #[serde(default = "default_election_timeout", with = "humantime_serde")]
    pub(crate) election_timeout: Duration,

    /// Interval between leader heartbeats to followers.
    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub(crate) heartbeat_interval: Duration,

    /// Per-request timeout for peer Raft RPCs.
    #[serde(default = "default_rpc_timeout", with = "humantime_serde")]
    pub(crate) rpc_timeout: Duration,

    /// Build a new snapshot every N applied log entries.
    #[serde(default = "default_snapshot_log_entries")]
    pub(crate) snapshot_log_entries: u64,

    /// How many post-snapshot log entries stay before purge (ADR 0017). A
    /// fresh learner that falls beyond this window can no longer catch up by
    /// log replay and resyncs via install-snapshot instead (ADR 0016).
    #[serde(default = "default_snapshot_keep_log_entries")]
    pub(crate) snapshot_keep_log_entries: u64,

    /// How long the leader must *continuously* observe a full, caught-up voter
    /// set before `GET /readyz?require=healthy` answers 200 (ADR 0037 §9).
    ///
    /// The gate is cluster redundancy, and redundancy that flickers is not
    /// redundancy: a voter that replicates for one poll and drops out for the
    /// next would otherwise let bringup automation proceed into a cluster that
    /// cannot survive losing a node. The interval is how long "sustained"
    /// means; any lapse restarts it. Tests and `coppice dev` shorten it, which
    /// is the only reason it is configurable at all.
    #[serde(
        default = "default_health_stability_interval",
        with = "humantime_serde"
    )]
    pub(crate) health_stability_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout: default_election_timeout(),
            heartbeat_interval: default_heartbeat_interval(),
            rpc_timeout: default_rpc_timeout(),
            snapshot_log_entries: default_snapshot_log_entries(),
            snapshot_keep_log_entries: default_snapshot_keep_log_entries(),
            health_stability_interval: default_health_stability_interval(),
        }
    }
}

/// Convergence-loop pacing (ADR 0037 §6).
///
/// Every value here is a *sleep between rounds*, never a timeout or a
/// deadline: the loop is tick-driven, nothing wakes it early, so these are
/// exactly the knobs that decide how long a join, a catch-up, or a promotion
/// takes to be noticed. Like `[raft]` they affect only liveness — a shorter
/// interval costs dials, a longer one costs latency, and neither can change
/// what the cluster agrees on — so they are node-local and safe to vary per
/// replica (ADR 0020). The defaults suit ordinary deployments; the
/// integration-test fixture shrinks them, which is the only reason they are
/// configurable at all — a fleet that forms in one process tree has no reason
/// to pay a production deployment's pacing.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PacingConfig {
    /// Base re-probe cadence while actively converging. Short: the cost of a
    /// tick is one dial to a peer that is expecting it, and convergence
    /// latency is a deployment's rolling-upgrade latency.
    #[serde(default = "default_probe_interval", with = "humantime_serde")]
    pub(crate) probe_interval: Duration,

    /// Slow cadence once converged to a voter, or once parked on an
    /// unavailable voter seat: nothing to do but keep observing, so a later
    /// membership change (a removal, a demotion) is still noticed.
    #[serde(default = "default_settled_interval", with = "humantime_serde")]
    pub(crate) settled_interval: Duration,

    /// How long to back off after a refusal that will not resolve by waiting —
    /// a duplicated machine identity, an address conflict (ADR 0037 §7).
    /// Hammering a leader that has already said "never" buys nothing; the
    /// operator sees the refusal in `/readyz` and fixes it, and the retry is
    /// only here at all because "never" can become "yes" once they do.
    #[serde(default = "default_refusal_backoff", with = "humantime_serde")]
    pub(crate) refusal_backoff: Duration,

    /// The shortest a parked daemon waits between pre-start convergence
    /// rounds: a fleet booting together should find each other in the first
    /// second or two.
    #[serde(default = "default_park_interval_min", with = "humantime_serde")]
    pub(crate) park_interval_min: Duration,

    /// The longest a parked daemon waits between rounds — a fleet parked for a
    /// week must not spin. The backoff doubles each failed round, so this also
    /// bounds the ramp, and reaching it is what escalates a parked daemon's
    /// failure reason to `warn` (a daemon still failing at maximum backoff is
    /// stuck, not booting).
    #[serde(default = "default_park_interval_max", with = "humantime_serde")]
    pub(crate) park_interval_max: Duration,

    /// How often the admin-side promotion wrapper retries while a learner is
    /// still catching up or the leader has not yet heard its first heartbeat
    /// acknowledgement. Bounded by the caller's own `wait` deadline, which is
    /// not configuration.
    #[serde(default = "default_promote_poll_interval", with = "humantime_serde")]
    pub(crate) promote_poll_interval: Duration,
}

/// `[token_kdf]`: argon2id cost for hashing enrollment-token secrets
/// (ADR 0037 §5).
///
/// Hashing happens on the node that seeds or mints a token; only the PHC
/// string — which records the cost it was hashed at — is replicated, and
/// verification reads its parameters from that string. So this is node-local
/// (ADR 0020) and mixed-cost fleets verify correctly.
///
/// **Lowering these weakens every hash minted under them.** The defaults are
/// the `argon2` crate's recommended production parameters; the one legitimate
/// reason to shrink them is a test or dev fleet minting throwaway tokens by
/// the dozen, where the default's deliberate ~hundreds-of-milliseconds of
/// work per hash is pure drag.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TokenKdfConfig {
    /// Memory cost in KiB.
    #[serde(default = "default_kdf_m_cost_kib")]
    pub(crate) m_cost_kib: u32,
    /// Iteration count.
    #[serde(default = "default_kdf_t_cost")]
    pub(crate) t_cost: u32,
    /// Parallelism lanes.
    #[serde(default = "default_kdf_p_cost")]
    pub(crate) p_cost: u32,
}

impl TokenKdfConfig {
    /// The cost as the `coppice-tls` hashing layer takes it.
    pub(crate) fn kdf(&self) -> coppice_tls::pki::TokenKdf {
        coppice_tls::pki::TokenKdf {
            m_cost_kib: self.m_cost_kib,
            t_cost: self.t_cost,
            p_cost: self.p_cost,
        }
    }

    /// Reject a cost argon2 itself would refuse (`t_cost = 0`, `p_cost = 0`,
    /// memory under `8 × p_cost` KiB). Checked at load because the first
    /// *use* can be arbitrarily far away: a daemon with no seeded tokens
    /// starts cleanly and would otherwise surface a bad `[token_kdf]` only
    /// as internal errors on every later mint request.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.kdf()
            .validate()
            .map_err(|e| anyhow::anyhow!("[token_kdf] {e}"))
    }
}

impl Default for TokenKdfConfig {
    fn default() -> Self {
        TokenKdfConfig {
            m_cost_kib: default_kdf_m_cost_kib(),
            t_cost: default_kdf_t_cost(),
            p_cost: default_kdf_p_cost(),
        }
    }
}

impl PacingConfig {
    /// Every `[pacing]` value is a sleep between rounds of some retry loop —
    /// zero turns that loop into a busy spin (the convergence tick, the
    /// pre-start park loop, the promote poll), so zero is a config error,
    /// not a speed setting. The park ramp's floor must not exceed its
    /// ceiling: the backoff doubles from `min` and clamps to `max`, and an
    /// inverted pair would "clamp" every wait up to the ceiling immediately.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("probe_interval", self.probe_interval),
            ("settled_interval", self.settled_interval),
            ("refusal_backoff", self.refusal_backoff),
            ("park_interval_min", self.park_interval_min),
            ("park_interval_max", self.park_interval_max),
            ("promote_poll_interval", self.promote_poll_interval),
        ] {
            if value.is_zero() {
                anyhow::bail!(
                    "[pacing] {name} must be non-zero — a zero interval turns its \
                     retry loop into a busy spin"
                );
            }
        }
        if self.park_interval_min > self.park_interval_max {
            anyhow::bail!(
                "[pacing] park_interval_min ({}) exceeds park_interval_max ({}) — \
                 the park backoff doubles from the min and clamps to the max",
                humantime_serde::re::humantime::format_duration(self.park_interval_min),
                humantime_serde::re::humantime::format_duration(self.park_interval_max),
            );
        }
        Ok(())
    }
}

impl Default for PacingConfig {
    fn default() -> Self {
        PacingConfig {
            probe_interval: default_probe_interval(),
            settled_interval: default_settled_interval(),
            refusal_backoff: default_refusal_backoff(),
            park_interval_min: default_park_interval_min(),
            park_interval_max: default_park_interval_max(),
            promote_poll_interval: default_promote_poll_interval(),
        }
    }
}

/// mTLS material for intra-cluster traffic (ADR 0011).
///
/// Secrets by path reference only: the config file itself never holds key
/// material, so it stays safe to commit, diff, and attach to support
/// bundles (ADR 0020).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TlsConfig {
    pub(crate) cert_path: PathBuf,
    pub(crate) key_path: PathBuf,
    pub(crate) ca_path: PathBuf,
}

/// SSO connection parameters.
///
/// Parsed but unused for now — the API server task owns SSO. Only the
/// connection shape lives here; anything authorization-shaped (role
/// mappings, admin groups) is replicated policy, because two coordinators
/// must never enforce different admin lists (ADR 0020).
// Parsed now; the API server that owns SSO consumes these in a later change.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub(crate) struct SsoConfig {
    pub(crate) issuer: String,
    pub(crate) client_id: String,
    pub(crate) client_secret_path: PathBuf,
}

/// Logging, tracing, and metrics settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub(crate) log_level: String,

    /// `"text"` or `"json"`.
    #[serde(default = "default_log_format")]
    pub(crate) log_format: String,

    // Parsed now; the OTLP exporter is wired in a later change. The Prometheus
    // `/metrics` endpoint is already live — it rides the client API listener at
    // `/metrics` (issue #46) rather than a separate address, so there is no
    // coordinator metrics-address knob here.
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) otlp_endpoint: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        ObservabilityConfig {
            log_level: default_log_level(),
            log_format: default_log_format(),
            otlp_endpoint: None,
        }
    }
}

fn default_client_addr() -> SocketAddr {
    "0.0.0.0:7070"
        .parse()
        .expect("valid default socket address")
}

fn default_raft_addr() -> SocketAddr {
    "0.0.0.0:7071"
        .parse()
        .expect("valid default socket address")
}

fn default_agent_addr() -> SocketAddr {
    "0.0.0.0:7072"
        .parse()
        .expect("valid default socket address")
}

fn default_election_timeout() -> Duration {
    Duration::from_millis(1500)
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(300)
}

fn default_rpc_timeout() -> Duration {
    Duration::from_secs(1)
}

fn default_snapshot_log_entries() -> u64 {
    50_000
}

fn default_snapshot_keep_log_entries() -> u64 {
    1000
}

/// ADR 0037 §9's stated default: long enough that a flapping follower cannot
/// slip through between two polls, short enough not to stall a bringup.
fn default_health_stability_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_probe_interval() -> Duration {
    Duration::from_millis(300)
}

fn default_settled_interval() -> Duration {
    Duration::from_secs(3)
}

fn default_refusal_backoff() -> Duration {
    Duration::from_secs(30)
}

fn default_park_interval_min() -> Duration {
    Duration::from_millis(500)
}

fn default_park_interval_max() -> Duration {
    Duration::from_secs(15)
}

fn default_promote_poll_interval() -> Duration {
    Duration::from_millis(500)
}

// The `argon2` crate's `Params::default()` values, restated as literals so a
// dependency bump that silently changes them fails the config unit test
// instead of silently re-costing every fleet's token hashes.
fn default_kdf_m_cost_kib() -> u32 {
    19456
}

fn default_kdf_t_cost() -> u32 {
    2
}

fn default_kdf_p_cost() -> u32 {
    1
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "text".to_string()
}

/// The fully-resolved configuration for this process.
///
/// A thin wrapper rather than a bare [`Config`] because "resolved" is a real
/// state: `load` has validated both TLS postures, checked the discovery
/// backend, and pinned `advertise_host` to a concrete value, so every reader
/// downstream may treat those as settled. It carries no startup intent —
/// under ADR 0037 §1 intent is derived from the data directory, and there is
/// no flag left for a config layer to override.
#[derive(Debug)]
pub struct ResolvedConfig {
    pub(crate) config: Config,
}

impl Config {
    /// The local admin socket path (ADR 0037 §3): the explicit
    /// `[listen] admin_socket`, or `<data_dir>/admin.sock`.
    ///
    /// Resolved from config alone so the `init` CLI, which never starts a
    /// daemon, reaches the same path the daemon binds.
    /// The client listener's posture (ADR 0037 §4), or the error naming both
    /// options — for an absent section as much as a half-configured one.
    pub(crate) fn client_tls_posture(&self) -> Result<ClientTlsPosture> {
        match &self.client_tls {
            Some(section) => section.posture(),
            None => Err(ClientTlsConfig::absent()),
        }
    }

    pub(crate) fn admin_socket_path(&self) -> PathBuf {
        self.listen
            .admin_socket
            .clone()
            .unwrap_or_else(|| self.data_dir.join("admin.sock"))
    }
}

impl ResolvedConfig {
    /// The parsed configuration itself, for callers that only want the file's
    /// contents (the in-crate formation tests, which never boot a daemon).
    #[cfg(test)]
    pub(crate) fn into_config(self) -> Config {
        self.config
    }

    /// Emit the fully-resolved effective configuration.
    ///
    /// Safe to log in full: secrets are held by path reference (ADR 0020) —
    /// with one exception, the dev-only inline `[enrollment].token`, whose
    /// `Secret` type redacts itself from every `Debug` rendering. A test
    /// below holds this line: the rendered config must never contain a token.
    pub(crate) fn log_effective(&self) {
        tracing::info!(
            cluster_id = %self.config.cluster_id,
            config = ?self.config,
            "effective coordinator configuration"
        );
    }
}

/// Load and validate the node configuration file.
///
/// One argument, because there is nothing left to layer on top of it: ADR 0037
/// §1 removed `--bootstrap`/`--join`, so every value resolves file-over-default
/// via `serde` field defaults. What this does beyond parsing is reject, at
/// startup, three things that would otherwise fail much later and much less
/// clearly — a discovery section that names a backend without its table, a
/// public listener whose TLS posture was never stated, and an `[enrollment]`
/// endpoint a token must never be sent to — and resolve `advertise_host` once
/// so no reader downstream has to.
pub fn load(path: &Path) -> Result<ResolvedConfig> {
    let mut config = read_config(path)?;
    config
        .discovery
        .validate()
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    // Both postures are resolved at load, not at first use: a deployment that
    // has not said whether its public listener is TLS or plainly insecure must
    // fail before it binds anything (ADR 0037 §4), and an enrollment section
    // pointed at an unverifiable endpoint must fail before it sends a token.
    config
        .client_tls_posture()
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    if let Some(enrollment) = &config.enrollment {
        enrollment
            .validate()
            .with_context(|| format!("reading coordinator config {}", path.display()))?;
    }
    // Both fail at load rather than at first use: a zero pacing interval
    // would busy-spin a background loop from the moment the daemon starts,
    // and an argon2-rejected `[token_kdf]` would otherwise surface only when
    // the first token is minted.
    config
        .pacing
        .validate()
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    config
        .token_kdf
        .validate()
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    let resolved_host = resolve_advertise_host(config.listen.advertise_host.as_deref())?;
    config.listen.advertise_host = Some(resolved_host);
    Ok(ResolvedConfig { config })
}

/// Read and parse the config file, wrapping any I/O or deserialization
/// failure with the file path so the error names both the file and (via
/// `serde`'s own message) the offending key.
fn read_config(path: &Path) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("reading coordinator config {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `contents` to a fresh temp file and return the guard plus its
    /// path (dropping the guard deletes the file).
    fn write_config(contents: &str) -> (tempfile::NamedTempFile, PathBuf) {
        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(contents.as_bytes())
            .expect("write temp file");
        let path = file.path().to_path_buf();
        (file, path)
    }

    /// The full documented example from `docs/operations/configuration.md`,
    /// extended with `cluster_id` and `[raft].rpc_timeout` (fields this
    /// module adds ahead of the doc pass).
    const FULL_EXAMPLE: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"

[discovery]
backend = "static"
cluster_size = 3
removal_grace = "90s"
learner_expiry = "30m"

[discovery.static]
addrs = ["coord-1.batch.example.com:7071", "coord-2.batch.example.com:7071"]

[listen]
client_addr = "0.0.0.0:7070"
raft_addr   = "0.0.0.0:7071"
agent_addr  = "0.0.0.0:7072"
advertise_host = "coord-3.batch.example.com"

[raft]
election_timeout   = "1500ms"
heartbeat_interval = "300ms"
rpc_timeout        = "1s"
snapshot_log_entries = 50_000
snapshot_keep_log_entries = 2000

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[client_tls]
cert_path = "/etc/coppice/pki/api.example.com.crt"
key_path  = "/etc/coppice/pki/api.example.com.key"

[enrollment]
endpoint = "https://coord.batch.example.com:7070"
token_path = "/etc/coppice/enroll-token"

[sso]
issuer = "https://sso.example.com/oidc"
client_id = "coppice"
client_secret_path = "/etc/coppice/oidc-secret"

[observability]
log_level  = "info"
log_format = "json"
otlp_endpoint = "https://otel-collector.example.com:4317"
"#;

    /// Everything but `[discovery]`, which the backend-specific tests append
    /// themselves (a document may carry only one `[discovery]` table).
    const BASE_WITHOUT_DISCOVERY: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"

[listen]
advertise_host = "coord-1.example.com"

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"
"#;

    const MINIMAL_EXAMPLE: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"

[listen]
advertise_host = "coord-1.example.com"

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[client_tls]
insecure = true

[discovery]
backend = "static"

[discovery.static]
addrs = []
"#;

    #[test]
    fn full_documented_example_parses() {
        let (_guard, path) = write_config(FULL_EXAMPLE);
        let config = read_config(&path).expect("full example should parse");

        assert_eq!(
            config.cluster_id,
            "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
                .parse()
                .unwrap()
        );
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/coppice"));
        assert_eq!(config.discovery.backend, BackendKind::Static);
        assert_eq!(config.discovery.cluster_size, 3);
        assert_eq!(config.discovery.removal_grace, Duration::from_secs(90));
        assert_eq!(
            config.discovery.learner_expiry,
            Duration::from_secs(30 * 60)
        );
        assert_eq!(
            config.discovery.static_addrs(),
            [
                "coord-1.batch.example.com:7071".to_string(),
                "coord-2.batch.example.com:7071".to_string(),
            ]
        );

        assert_eq!(config.listen.client_addr, default_client_addr());
        assert_eq!(config.listen.raft_addr, default_raft_addr());
        assert_eq!(config.listen.agent_addr, default_agent_addr());
        assert_eq!(
            config.listen.advertise_host.as_deref(),
            Some("coord-3.batch.example.com")
        );

        assert_eq!(config.raft.election_timeout, Duration::from_millis(1500));
        assert_eq!(config.raft.heartbeat_interval, Duration::from_millis(300));
        assert_eq!(config.raft.rpc_timeout, Duration::from_secs(1));
        assert_eq!(config.raft.snapshot_log_entries, 50_000);
        // File value overrides the built-in 1000 default.
        assert_eq!(config.raft.snapshot_keep_log_entries, 2000);

        assert_eq!(
            config.tls.cert_path,
            PathBuf::from("/etc/coppice/pki/node.crt")
        );
        assert_eq!(
            config.tls.key_path,
            PathBuf::from("/etc/coppice/pki/node.key")
        );
        assert_eq!(config.tls.ca_path, PathBuf::from("/etc/coppice/pki/ca.crt"));

        let sso = config.sso.expect("sso section present");
        assert_eq!(sso.issuer, "https://sso.example.com/oidc");
        assert_eq!(sso.client_id, "coppice");
        assert_eq!(
            sso.client_secret_path,
            PathBuf::from("/etc/coppice/oidc-secret")
        );

        assert_eq!(config.observability.log_level, "info");
        assert_eq!(config.observability.log_format, "json");
        assert_eq!(
            config.observability.otlp_endpoint.as_deref(),
            Some("https://otel-collector.example.com:4317")
        );
    }

    #[test]
    fn minimal_config_applies_documented_defaults() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("minimal example should parse");

        // Explicit empty static backend; cluster_size defaults.
        assert_eq!(config.discovery.backend, BackendKind::Static);
        assert!(config.discovery.static_addrs().is_empty());
        assert_eq!(config.discovery.cluster_size, 3);
        assert_eq!(config.discovery.removal_grace, Duration::from_secs(120));
        assert_eq!(config.discovery.learner_expiry, Duration::from_secs(3600));
        config.discovery.validate().expect("empty static is valid");

        assert_eq!(config.listen.client_addr, default_client_addr());
        assert_eq!(config.listen.raft_addr, default_raft_addr());
        assert_eq!(config.listen.agent_addr, default_agent_addr());

        assert_eq!(config.raft.election_timeout, Duration::from_millis(1500));
        assert_eq!(config.raft.heartbeat_interval, Duration::from_millis(300));
        assert_eq!(config.raft.rpc_timeout, Duration::from_secs(1));
        assert_eq!(config.raft.snapshot_log_entries, 50_000);
        // Absent key takes the built-in default.
        assert_eq!(config.raft.snapshot_keep_log_entries, 1000);

        assert!(config.sso.is_none());

        assert_eq!(config.observability.log_level, "info");
        assert_eq!(config.observability.log_format, "text");
        assert!(config.observability.otlp_endpoint.is_none());
    }

    #[test]
    fn missing_discovery_section_is_rejected() {
        // The section is required — an un-migrated config (or one still
        // carrying the old top-level `peers`) must fail-stop, not silently
        // discover nothing.
        let (_guard, path) = write_config(BASE_WITHOUT_DISCOVERY);
        let err = read_config(&path).expect_err("missing [discovery] must be rejected");
        assert!(format!("{err:#}").contains("discovery"), "{err:#}");
    }

    #[test]
    fn old_top_level_peers_key_is_rejected_by_name() {
        let bad = format!("{MINIMAL_EXAMPLE}\npeers = []\n");
        let (_guard, path) = write_config(&bad);
        let err = read_config(&path).expect_err("removed `peers` key must be rejected");
        assert!(format!("{err:#}").contains("peers"), "{err:#}");
    }

    #[test]
    fn static_backend_without_table_is_rejected() {
        let contents = format!("{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"static\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("parses; validation is separate");
        let err = config
            .discovery
            .validate()
            .expect_err("static without a [discovery.static] table must be rejected");
        assert!(format!("{err:#}").contains("[discovery.static]"), "{err:#}");
    }

    #[test]
    fn unknown_key_fails_naming_the_key() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[raft]\nelecton_timeout = \"1500ms\"\n");
        let (_guard, path) = write_config(&bad);
        let err = read_config(&path).expect_err("typo'd key should fail");
        let message = format!("{err:#}");
        assert!(
            message.contains("electon_timeout"),
            "error should name the offending key, got: {message}"
        );
        assert!(message.contains(&path.display().to_string()));
    }

    #[test]
    fn raw_integer_duration_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[raft]\nelection_timeout = 1500\n");
        let (_guard, path) = write_config(&bad);
        let err = read_config(&path).expect_err("unlabelled duration should fail");
        let message = format!("{err:#}");
        assert!(
            !message.is_empty(),
            "expected a parse error for a raw-integer duration"
        );
    }

    /// The health-stability interval is a `[raft]` duration like the others:
    /// humantime or nothing, defaulting to the ADR's 10s.
    #[test]
    fn health_stability_interval_defaults_and_parses() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("config should parse");
        assert_eq!(
            config.raft.health_stability_interval,
            Duration::from_secs(10)
        );

        let contents = format!("{MINIMAL_EXAMPLE}\n[raft]\nhealth_stability_interval = \"2s\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("config should parse");
        assert_eq!(
            config.raft.health_stability_interval,
            Duration::from_secs(2)
        );
    }

    /// `[token_kdf]` is entirely optional; its defaults must equal the
    /// `argon2` crate's own — the cost `hash_secret` used before the section
    /// existed — so an old config keeps minting identically-priced hashes,
    /// and a dependency bump that changes the crate's defaults is caught here
    /// rather than silently re-costing production fleets.
    #[test]
    fn token_kdf_defaults_and_parses() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("an absent [token_kdf] section is valid");
        assert_eq!(
            config.token_kdf.kdf(),
            coppice_tls::pki::TokenKdf::default()
        );
        assert_eq!(config.token_kdf.m_cost_kib, 19456);
        assert_eq!(config.token_kdf.t_cost, 2);
        assert_eq!(config.token_kdf.p_cost, 1);

        let contents =
            format!("{MINIMAL_EXAMPLE}\n[token_kdf]\nm_cost_kib = 8\nt_cost = 1\np_cost = 1\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("an explicit [token_kdf] section parses");
        assert_eq!(config.token_kdf.m_cost_kib, 8);
        assert_eq!(config.token_kdf.t_cost, 1);
        assert_eq!(config.token_kdf.p_cost, 1);
    }

    /// `[pacing]` is entirely optional and every field defaults to the value
    /// the convergence loop used when these were hardcoded constants — a
    /// config written before the section existed must keep production pacing.
    #[test]
    fn pacing_defaults_and_parses() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("an absent [pacing] section is valid");
        assert_eq!(config.pacing.probe_interval, Duration::from_millis(300));
        assert_eq!(config.pacing.settled_interval, Duration::from_secs(3));
        assert_eq!(config.pacing.refusal_backoff, Duration::from_secs(30));
        assert_eq!(config.pacing.park_interval_min, Duration::from_millis(500));
        assert_eq!(config.pacing.park_interval_max, Duration::from_secs(15));
        assert_eq!(
            config.pacing.promote_poll_interval,
            Duration::from_millis(500)
        );

        let contents = format!(
            "{MINIMAL_EXAMPLE}\n[pacing]\nprobe_interval = \"50ms\"\n\
             settled_interval = \"250ms\"\nrefusal_backoff = \"1s\"\n\
             park_interval_min = \"50ms\"\npark_interval_max = \"250ms\"\n\
             promote_poll_interval = \"50ms\"\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("an explicit [pacing] section parses");
        assert_eq!(config.pacing.probe_interval, Duration::from_millis(50));
        assert_eq!(config.pacing.settled_interval, Duration::from_millis(250));
        assert_eq!(config.pacing.refusal_backoff, Duration::from_secs(1));
        assert_eq!(config.pacing.park_interval_min, Duration::from_millis(50));
        assert_eq!(config.pacing.park_interval_max, Duration::from_millis(250));
        assert_eq!(
            config.pacing.promote_poll_interval,
            Duration::from_millis(50)
        );

        // A partial section keeps the defaults for everything it omits.
        let contents = format!("{MINIMAL_EXAMPLE}\n[pacing]\nsettled_interval = \"1s\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("a partial [pacing] section parses");
        assert_eq!(config.pacing.settled_interval, Duration::from_secs(1));
        assert_eq!(config.pacing.probe_interval, Duration::from_millis(300));
    }

    #[test]
    fn file_overrides_default_and_absent_value_takes_default() {
        let contents = format!("{MINIMAL_EXAMPLE}\n[observability]\nlog_level = \"debug\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("config should parse");

        // File value overrides the default.
        assert_eq!(config.observability.log_level, "debug");
        // Absent value in the same section takes the default.
        assert_eq!(config.observability.log_format, "text");
    }

    #[test]
    fn advertised_raft_addr_composes_host_and_raft_port() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("config should parse");
        assert_eq!(
            config.listen.advertised_raft_addr(),
            "coord-1.example.com:7071"
        );
    }

    #[test]
    fn dns_backend_parses_and_validates() {
        let contents = format!(
            "{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"dns\"\n\n\
             [discovery.dns]\nname = \"coord.batch.example.com\"\nport = 7071\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("dns discovery should parse");
        assert_eq!(config.discovery.backend, BackendKind::Dns);
        config.discovery.validate().expect("dns config is valid");
        let dns = config.discovery.dns.expect("dns table present");
        assert_eq!(dns.name, "coord.batch.example.com");
        assert_eq!(dns.port, 7071);
    }

    #[test]
    fn file_backend_parses_and_validates() {
        let contents = format!(
            "{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"file\"\n\n\
             [discovery.file]\ndir = \"/var/run/coppice/discovery\"\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("file discovery should parse");
        config.discovery.validate().expect("file config is valid");
        assert_eq!(
            config.discovery.file.expect("file table").dir,
            PathBuf::from("/var/run/coppice/discovery")
        );
    }

    #[test]
    fn ec2_asg_backend_parses() {
        let contents = format!(
            "{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"ec2-asg\"\n\n\
             [discovery.ec2_asg]\nport = 7071\nregion = \"us-east-1\"\ntimeout = \"5s\"\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("ec2-asg discovery should parse");
        assert_eq!(config.discovery.backend, BackendKind::Ec2Asg);
        let ec2 = config
            .discovery
            .ec2_asg
            .as_ref()
            .expect("ec2_asg table present");
        assert_eq!(ec2.port, 7071);
        assert_eq!(ec2.region.as_deref(), Some("us-east-1"));
        assert_eq!(ec2.timeout, Duration::from_secs(5));
        config
            .discovery
            .validate()
            .expect("ec2-asg config is valid");
    }

    #[test]
    fn ec2_asg_backend_defaults_region_and_timeout() {
        // `port` is the only required field; region defaults to the IMDS value
        // (None here) and timeout to 3s.
        let contents = format!(
            "{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"ec2-asg\"\n\n\
             [discovery.ec2_asg]\nport = 7071\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("ec2-asg with only a port should parse");
        let ec2 = config
            .discovery
            .ec2_asg
            .as_ref()
            .expect("ec2_asg table present");
        assert_eq!(ec2.port, 7071);
        assert_eq!(ec2.region, None);
        assert_eq!(ec2.timeout, Duration::from_secs(3));
    }

    #[test]
    fn ec2_asg_backend_without_table_is_rejected() {
        // Selecting the backend without its table (hence without `port`) is a
        // validation error, mirroring the dns/file required-table rule.
        let contents = format!("{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"ec2-asg\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("parses; validation is separate");
        let err = config
            .discovery
            .validate()
            .expect_err("ec2-asg without a [discovery.ec2_asg] table must be rejected");
        assert!(
            format!("{err:#}").contains("requires a [discovery.ec2_asg] table"),
            "{err:#}"
        );
    }

    #[test]
    fn backend_mismatch_with_foreign_table_is_rejected() {
        // backend = dns but a [discovery.static] table is present.
        let contents = format!(
            "{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"dns\"\n\n\
             [discovery.dns]\nname = \"coord.example.com\"\nport = 7071\n\n\
             [discovery.static]\naddrs = [\"a:1\"]\n"
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("parses; validation catches the mismatch");
        let err = config
            .discovery
            .validate()
            .expect_err("foreign table rejected");
        assert!(format!("{err:#}").contains("static"), "{err:#}");
    }

    #[test]
    fn missing_required_backend_table_is_rejected() {
        let contents = format!("{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"dns\"\n");
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("parses; validation catches the missing table");
        let err = config
            .discovery
            .validate()
            .expect_err("missing dns table rejected");
        assert!(format!("{err:#}").contains("[discovery.dns]"), "{err:#}");
    }

    #[test]
    fn load_resolves_and_validates() {
        // A minimal config with an explicit advertise_host: load() must resolve
        // it in place and pass discovery validation.
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let resolved = load(&path).expect("load succeeds");
        assert_eq!(
            resolved.config.listen.advertise_host.as_deref(),
            Some("coord-1.example.com")
        );
    }

    #[test]
    fn load_rejects_invalid_discovery_section() {
        let contents = format!("{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"dns\"\n");
        let (_guard, path) = write_config(&contents);
        let err = load(&path).expect_err("load must surface discovery validation errors");
        assert!(format!("{err:#}").contains("[discovery.dns]"), "{err:#}");
    }

    /// Every `[pacing]` value is a between-rounds sleep, so zero means a
    /// busy-spinning background loop — a startup error, not a speed setting.
    /// An inverted park ramp (min > max) is equally malformed.
    #[test]
    fn load_rejects_zero_and_inverted_pacing() {
        let contents = format!("{MINIMAL_EXAMPLE}\n[pacing]\nprobe_interval = \"0s\"\n");
        let (_guard, path) = write_config(&contents);
        let err = load(&path).expect_err("a zero pacing interval must fail at load");
        assert!(format!("{err:#}").contains("probe_interval"), "{err:#}");

        let contents = format!(
            "{MINIMAL_EXAMPLE}\n[pacing]\npark_interval_min = \"5s\"\n\
             park_interval_max = \"1s\"\n"
        );
        let (_guard, path) = write_config(&contents);
        let err = load(&path).expect_err("an inverted park ramp must fail at load");
        assert!(format!("{err:#}").contains("park_interval_min"), "{err:#}");
    }

    /// A `[token_kdf]` argon2 rejects must fail at load: the first mint can
    /// be arbitrarily far from startup, and until then the daemon looks
    /// healthy.
    #[test]
    fn load_rejects_argon2_refused_token_kdf() {
        for bad in [
            "t_cost = 0",
            "p_cost = 0",
            // Below argon2's floor of 8 KiB per lane.
            "m_cost_kib = 7",
        ] {
            let contents = format!("{MINIMAL_EXAMPLE}\n[token_kdf]\n{bad}\n");
            let (_guard, path) = write_config(&contents);
            let err = match load(&path) {
                Err(err) => err,
                Ok(_) => panic!("[token_kdf] {bad} must fail at load"),
            };
            assert!(format!("{err:#}").contains("[token_kdf]"), "{err:#}");
        }
    }

    #[test]
    fn resolve_advertise_host_prefers_explicit_value() {
        assert_eq!(
            resolve_advertise_host(Some("coord-7.example.com")).expect("explicit resolves"),
            "coord-7.example.com"
        );
    }

    // ---- the fallback chain, through injectable seams --------------------

    #[test]
    fn explicit_value_short_circuits_every_seam() {
        let got = resolve_advertise_host_with(
            Some("coord-7.example.com"),
            || panic!("hostname seam must not be consulted"),
            |_| panic!("resolution seam must not be consulted"),
            || panic!("route seam must not be consulted"),
        )
        .expect("explicit resolves");
        assert_eq!(got, "coord-7.example.com");
    }

    #[test]
    fn resolvable_fqdn_hostname_is_chosen() {
        let got = resolve_advertise_host_with(
            None,
            || Some("coord-1.batch.example.com".to_string()),
            |host| {
                assert_eq!(host, "coord-1.batch.example.com");
                true
            },
            || panic!("route seam must not be consulted when the FQDN resolves"),
        )
        .expect("fqdn resolves");
        assert_eq!(got, "coord-1.batch.example.com");
    }

    #[test]
    fn short_hostname_falls_through_to_the_default_route() {
        // A bare short name (no dot) is never published, even if it would
        // resolve locally — peers on other hosts generally cannot resolve it.
        let got = resolve_advertise_host_with(
            None,
            || Some("coord-1".to_string()),
            |_| panic!("a short name must not even be looked up"),
            || Some("10.0.0.7".to_string()),
        )
        .expect("route fallback");
        assert_eq!(got, "10.0.0.7");
    }

    #[test]
    fn unresolvable_fqdn_falls_through_to_the_default_route() {
        let got = resolve_advertise_host_with(
            None,
            || Some("ghost.internal.example".to_string()),
            |_| false,
            || Some("10.0.0.7".to_string()),
        )
        .expect("route fallback");
        assert_eq!(got, "10.0.0.7");
    }

    #[test]
    fn nothing_resolvable_is_an_error_naming_the_fix() {
        let err =
            resolve_advertise_host_with(None, || Some("coord-1".to_string()), |_| true, || None)
                .expect_err("no fallback left");
        assert!(
            format!("{err:#}").contains("listen.advertise_host"),
            "{err:#}"
        );
    }

    // ---- IPv6 advertised-address composition ------------------------------

    #[test]
    fn ipv6_advertise_host_is_bracketed_in_the_advertised_addr() {
        let contents = MINIMAL_EXAMPLE.replace(
            "advertise_host = \"coord-1.example.com\"",
            "advertise_host = \"2001:db8::1\"",
        );
        let (_guard, path) = write_config(&contents);
        let config = read_config(&path).expect("ipv6 advertise_host parses");
        assert_eq!(
            config.listen.advertised_raft_addr(),
            "[2001:db8::1]:7071",
            "IPv6 hosts must compose the bracketed authority form"
        );
        // Round-trips through the shared host:port parser.
        let (host, port) =
            coppice_tls::split_host_port(&config.listen.advertised_raft_addr()).expect("parses");
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, 7071);
    }
    // ---- [client_tls]: the public listener's posture (ADR 0037 §4) --------

    #[test]
    fn client_tls_paths_resolve_to_the_tls_posture() {
        let (_guard, path) = write_config(FULL_EXAMPLE);
        let config = read_config(&path).expect("full example parses");
        assert_eq!(
            config
                .client_tls_posture()
                .expect("cert + key is a posture"),
            ClientTlsPosture::Tls {
                cert: PathBuf::from("/etc/coppice/pki/api.example.com.crt"),
                key: PathBuf::from("/etc/coppice/pki/api.example.com.key"),
            }
        );
    }

    #[test]
    fn client_tls_insecure_resolves_to_the_plain_posture() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = read_config(&path).expect("minimal example parses");
        assert_eq!(
            config.client_tls_posture().expect("insecure is a posture"),
            ClientTlsPosture::Insecure
        );
    }

    /// Every way of not choosing — the table absent, empty, half-filled, or
    /// claiming both modes — fails with a message naming both options.
    #[test]
    fn an_unstated_client_tls_posture_fails_naming_both_options() {
        let without = MINIMAL_EXAMPLE.replace("[client_tls]\ninsecure = true\n", "");
        let cases = [
            ("absent", without.clone()),
            ("empty", format!("{without}\n[client_tls]\n")),
            (
                "cert only",
                format!("{without}\n[client_tls]\ncert_path = \"/x.crt\"\n"),
            ),
            (
                "key only",
                format!("{without}\n[client_tls]\nkey_path = \"/x.key\"\n"),
            ),
            (
                "both modes",
                format!(
                    "{without}\n[client_tls]\ncert_path = \"/x.crt\"\nkey_path = \"/x.key\"\n\
                     insecure = true\n"
                ),
            ),
        ];
        for (label, contents) in cases {
            let (_guard, path) = write_config(&contents);
            let err = match load(&path) {
                Err(e) => format!("{e:#}"),
                Ok(_) => panic!("{label}: an unstated posture must fail startup"),
            };
            assert!(err.contains("cert_path"), "{label}: {err}");
            assert!(err.contains("key_path"), "{label}: {err}");
            assert!(err.contains("insecure = true"), "{label}: {err}");
        }
    }

    #[test]
    fn enrollment_https_is_valid_and_http_needs_the_opt_in() {
        let base = MINIMAL_EXAMPLE;

        let https = format!(
            "{base}\n[enrollment]\nendpoint = \"https://coord.example.com:7070\"\n\
             token_path = \"/etc/coppice/enroll-token\"\n"
        );
        let (_guard, path) = write_config(&https);
        load(&path).expect("an https endpoint needs no opt-in");

        let http = format!(
            "{base}\n[enrollment]\nendpoint = \"http://coord.example.com:7070\"\n\
             token_path = \"/etc/coppice/enroll-token\"\n"
        );
        let (_guard, path) = write_config(&http);
        let err = load(&path).expect_err("plain http without the opt-in fails at startup");
        let message = format!("{err:#}");
        assert!(message.contains("insecure = true"), "{message}");

        let opted_in = format!("{http}insecure = true\n");
        let (_guard, path) = write_config(&opted_in);
        load(&path).expect("the opt-in accepts plain http");
    }

    #[test]
    fn enrollment_needs_exactly_one_token_form() {
        let both = format!(
            "{MINIMAL_EXAMPLE}\n[enrollment]\nendpoint = \"https://c:7070\"\n\
             token = \"cpk_x\"\ntoken_path = \"/t\"\n"
        );
        let (_guard, path) = write_config(&both);
        let err = load(&path).expect_err("both token forms is an error");
        assert!(format!("{err:#}").contains("exactly one"), "{err:#}");

        let neither = format!("{MINIMAL_EXAMPLE}\n[enrollment]\nendpoint = \"https://c:7070\"\n");
        let (_guard, path) = write_config(&neither);
        let err = load(&path).expect_err("no token is an error");
        assert!(format!("{err:#}").contains("token_path"), "{err:#}");
    }

    /// The startup log renders the whole resolved config; an inline
    /// enrollment token must never survive that rendering (ADR 0037 §4: the
    /// token never appears in logs or traces).
    #[test]
    fn an_inline_enrollment_token_never_reaches_the_startup_log() {
        let inline = format!(
            "{MINIMAL_EXAMPLE}\n[enrollment]\nendpoint = \"https://c:7070\"\n\
             token = \"cpk_inline_startup_secret\"\ninsecure = false\n"
        );
        let (_guard, path) = write_config(&inline);
        let resolved = load(&path).expect("load");

        let ((), rendered) = coppice_testkit::tracing_capture::capture(|| resolved.log_effective());
        assert!(
            rendered.contains("effective coordinator configuration"),
            "the startup line was captured: {rendered}"
        );
        coppice_testkit::tracing_capture::assert_no_secret(&rendered, "cpk_");

        // The same guarantee for ad-hoc Debug renderings of the config.
        let debugged = format!("{:?}", resolved.into_config());
        assert!(!debugged.contains("cpk_"), "{debugged}");
        assert!(debugged.contains("redacted"), "{debugged}");
    }

    #[test]
    fn an_absent_enrollment_section_is_fine() {
        // A formed voter, or one whose material comes from an external PKI,
        // never enrolls.
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let resolved = load(&path).expect("load");
        assert!(resolved.config.enrollment.is_none());
    }
}
