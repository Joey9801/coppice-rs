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
//! Precedence is `CLI > file > built-in defaults`. The CLI surface is
//! deliberately tiny — `--config` plus the ADR 0016 startup-intent flags,
//! [`CliOverrides::bootstrap`] and [`CliOverrides::join`] — so every other
//! knob resolves file-over-default via `serde` defaults, and [`load`] is the
//! single place the two layers merge.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use coppice_core::id::ClusterId;
use serde::Deserialize;

pub(crate) use discovery::{BackendKind, DiscoveryConfig};

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
        /// reachable. Parsed now; its consumers land with convergence.
        #[serde(default = "default_cluster_size")]
        #[allow(dead_code)]
        pub(crate) cluster_size: usize,

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
    /// The cluster identity every replica shares, generated once at
    /// `coppice-cli cluster init` and cross-checked against the data
    /// directory's stamp at startup (ADR 0016). Parsed from the typed
    /// string form `cluster-<uuid>` (ADR 0024).
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

    /// mTLS material for intra-cluster traffic (ADR 0011, day one). Required:
    /// there is no insecure fallback.
    pub(crate) tls: TlsConfig,

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
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout: default_election_timeout(),
            heartbeat_interval: default_heartbeat_interval(),
            rpc_timeout: default_rpc_timeout(),
            snapshot_log_entries: default_snapshot_log_entries(),
            snapshot_keep_log_entries: default_snapshot_keep_log_entries(),
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

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "text".to_string()
}

/// The entire CLI override surface (ADR 0020: the flag set stays
/// deliberately tiny). These are the ADR 0016 startup-intent flags; they
/// never appear in the config file, so the CLI layer is their sole
/// authority.
#[derive(Debug, Clone, Copy, Default)]
pub struct CliOverrides {
    /// `--bootstrap`: this is the first coordinator of a brand-new cluster.
    pub bootstrap: bool,
    /// `--join`: this is a fresh replica joining an existing cluster.
    pub join: bool,
}

/// The fully-resolved configuration for this process: the parsed file plus
/// the CLI startup-intent flags layered on top (ADR 0020 precedence,
/// `CLI > file > built-in defaults`).
#[derive(Debug)]
pub struct ResolvedConfig {
    pub(crate) config: Config,
    pub(crate) bootstrap: bool,
    pub(crate) join: bool,
}

impl ResolvedConfig {
    /// Emit the fully-resolved effective configuration.
    ///
    /// Safe to log in full: the file holds secrets by path reference only,
    /// never inline material (ADR 0020), so there is nothing to redact.
    pub(crate) fn log_effective(&self) {
        tracing::info!(
            cluster_id = %self.config.cluster_id,
            bootstrap = self.bootstrap,
            join = self.join,
            config = ?self.config,
            "effective coordinator configuration"
        );
    }
}

/// Load the node configuration file and merge it with CLI overrides.
///
/// Precedence is `CLI > file > built-in defaults` (ADR 0020): `cli` is
/// authoritative for the startup-intent flags, which never appear in the
/// file; every other value resolves file-over-default via `serde` field
/// defaults. `--bootstrap` and `--join` are mutually exclusive.
pub fn load(path: &Path, cli: CliOverrides) -> Result<ResolvedConfig> {
    if cli.bootstrap && cli.join {
        bail!("--bootstrap and --join are mutually exclusive; pass at most one");
    }
    let mut config = read_config(path)?;
    config
        .discovery
        .validate()
        .with_context(|| format!("reading coordinator config {}", path.display()))?;
    let resolved_host = resolve_advertise_host(config.listen.advertise_host.as_deref())?;
    config.listen.advertise_host = Some(resolved_host);
    Ok(ResolvedConfig {
        config,
        bootstrap: cli.bootstrap,
        join: cli.join,
    })
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

    #[test]
    fn bootstrap_and_join_together_fail() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let err = load(
            &path,
            CliOverrides {
                bootstrap: true,
                join: true,
            },
        )
        .expect_err("bootstrap and join together should be rejected");
        let message = format!("{err:#}");
        assert!(message.contains("--bootstrap"));
        assert!(message.contains("--join"));
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
        let resolved = load(&path, CliOverrides::default()).expect("load succeeds");
        assert_eq!(
            resolved.config.listen.advertise_host.as_deref(),
            Some("coord-1.example.com")
        );
    }

    #[test]
    fn load_rejects_invalid_discovery_section() {
        let contents = format!("{BASE_WITHOUT_DISCOVERY}\n[discovery]\nbackend = \"dns\"\n");
        let (_guard, path) = write_config(&contents);
        let err = load(&path, CliOverrides::default())
            .expect_err("load must surface discovery validation errors");
        assert!(format!("{err:#}").contains("[discovery.dns]"), "{err:#}");
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
}
