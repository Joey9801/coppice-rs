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
//! The CLI surface is now just `--config` (ADR 0037 §1: startup intent is
//! derived from the disk, not declared), so every knob resolves file-over-default
//! via `serde` defaults and [`load`] simply parses and validates.

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
    //! carries `cluster_size` and the two membership grace periods, which
    //! convergence and the leader's removal decisions consult before replicated
    //! state is reachable — the same node-local justification as `cluster_id`.

    use std::path::PathBuf;
    use std::time::Duration;

    use coppice_consensus::MembershipPolicy;
    use serde::Deserialize;

    /// The `[discovery]` section: which backend seeds candidate raft addresses,
    /// the expected voter count, and the two membership grace periods.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct DiscoveryConfig {
        /// Which backend supplies candidate raft addresses. Exactly one
        /// matching backend table must be present (validated in
        /// [`DiscoveryConfig::validate`]).
        #[serde(default)]
        pub(crate) backend: BackendKind,

        /// Expected voter count. Node-local config (ADR 0037 §2): consulted by
        /// convergence and the leader's removal rule (§5) and the
        /// formation-complete signal (§7) before replicated state is reachable.
        #[serde(default = "default_cluster_size")]
        pub(crate) cluster_size: usize,

        /// How long the leader's replication to a voter must have been failing
        /// before that voter qualifies for overflow removal (ADR 0037 §5).
        /// Consumed by the leader's removal rule (ADR 0037 §5).
        #[serde(default = "default_grace", with = "humantime_serde")]
        pub(crate) removal_grace: Duration,

        /// How long a pending learner's incumbent must be unreachable or make no
        /// progress before a losing joiner may retry its seat (ADR 0037 §6).
        #[serde(default = "default_grace", with = "humantime_serde")]
        pub(crate) replacement_grace: Duration,

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
        /// EC2 auto-scaling-group membership. Config variant reserved; the
        /// backend is not built in this PR (ADR 0037 §2).
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

    impl Default for DiscoveryConfig {
        fn default() -> Self {
            // The whole section absent → an empty static backend. Admin tooling
            // then simply requires an explicit `--target`.
            DiscoveryConfig {
                backend: BackendKind::Static,
                cluster_size: default_cluster_size(),
                removal_grace: default_grace(),
                replacement_grace: default_grace(),
                static_backend: None,
                dns: None,
                file: None,
                ec2_asg: None,
            }
        }
    }

    impl DiscoveryConfig {
        /// Reject a section whose backend tables do not match `backend`: the
        /// required table must be present (except `static`, which defaults to
        /// an empty list), and no foreign backend table may appear.
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

            // Required table present (static tolerates absence → empty list).
            match self.backend {
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

        /// The node-local [`MembershipPolicy`] (ADR 0037 §5) this config carries:
        /// the expected voter count and the two membership grace periods,
        /// consulted by convergence and the leader's removal rule before
        /// replicated state is reachable.
        pub(crate) fn membership_policy(&self) -> MembershipPolicy {
            MembershipPolicy {
                cluster_size: self.cluster_size,
                removal_grace: self.removal_grace,
                replacement_grace: self.replacement_grace,
            }
        }

        /// The static seed list, if this config selects the `static` backend —
        /// the successor to the old top-level `peers`, consulted by admin
        /// tooling for a default `--target`.
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

    fn default_grace() -> Duration {
        Duration::from_secs(60)
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

    /// Coordinator discovery (ADR 0037 §2): which backend seeds candidate raft
    /// addresses, the expected voter count, and the membership grace periods.
    /// Subsumes the old top-level `peers` list (now `[discovery.static]`).
    /// Optional: an absent section defaults to an empty `static` backend, so
    /// admin tooling then requires an explicit `--target`.
    #[serde(default)]
    pub(crate) discovery: DiscoveryConfig,

    /// Listen and advertise addresses. Required.
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
    /// (explicit value ▸ system hostname ▸ default-route local address), so a
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
    /// Used when `raft_addr` requests port 0 (the multi-process dev case, ADR
    /// 0037 §2): bootstrap binds the listener first, learns the real port, and
    /// advertises *that* — so a `:0` config never publishes `host:0` to
    /// discovery, membership, or the convergence loop.
    pub(crate) fn advertised_raft_addr_on_port(&self, port: u16) -> String {
        let host = self
            .advertise_host
            .as_deref()
            .expect("advertise_host resolved by config::load before use");
        format!("{host}:{port}")
    }
}

/// Resolve the address peers dial, per ADR 0037 §2's fallback chain:
/// explicit config value ▸ the OS-reported system hostname ▸ the local
/// address of the default route.
///
/// The default-route step opens a UDP socket and `connect`s it to a public
/// address to learn which local interface the kernel would route through; no
/// packets are sent (UDP `connect` only records the peer). It is the reliable
/// last resort — the hostname step depends on host DNS/`/etc/hosts` setup.
fn resolve_advertise_host(explicit: Option<&str>) -> Result<String> {
    if let Some(host) = explicit {
        return Ok(host.to_string());
    }
    if let Some(host) = system_hostname() {
        tracing::info!(advertise_host = %host, source = "system-hostname", "resolved advertise_host");
        return Ok(host);
    }
    if let Some(addr) = default_route_local_addr() {
        tracing::info!(advertise_host = %addr, source = "default-route", "resolved advertise_host");
        return Ok(addr);
    }
    bail!(
        "advertise_host is unset and could not be resolved: the system hostname was \
         unavailable and no default route was found. Set `listen.advertise_host` \
         explicitly to the address peers and agents should dial (ADR 0037 §2)."
    );
}

/// The OS-reported hostname, or `None` if it is empty or not valid UTF-8.
///
/// This is the "system FQDN" step of the fallback chain: on a correctly
/// configured host `gethostname` returns the FQDN, but a bare short name is
/// still returned as-is (and is often resolvable on the local network); full
/// FQDN canonicalization is deliberately left to host DNS configuration.
fn system_hostname() -> Option<String> {
    let name = gethostname::gethostname().into_string().ok()?;
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// The local address of the default route: bind an unconnected UDP socket and
/// `connect` it toward a public address so the kernel selects the egress
/// interface, then read back the socket's local address. No traffic is sent.
fn default_route_local_addr() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let local = socket.local_addr().ok()?;
    Some(local.ip().to_string())
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

/// The fully-resolved configuration for this process: the parsed file with
/// `advertise_host` resolved in place (ADR 0037 §1 removes the CLI intent flags,
/// so there is no override layer left to merge).
#[derive(Debug)]
pub struct ResolvedConfig {
    pub(crate) config: Config,
}

impl ResolvedConfig {
    /// Emit the fully-resolved effective configuration.
    ///
    /// Safe to log in full: the file holds secrets by path reference only,
    /// never inline material (ADR 0020), so there is nothing to redact.
    pub(crate) fn log_effective(&self) {
        tracing::info!(
            cluster_id = %self.config.cluster_id,
            config = ?self.config,
            "effective coordinator configuration"
        );
    }
}

/// Load and validate the node configuration file (ADR 0020/0037 §1).
///
/// Startup intent is no longer declared on the command line — it is derived
/// from the data directory at start (ADR 0037 §1) — so this just parses, checks
/// `[discovery]`, and resolves `advertise_host` in place so every later reader
/// (bootstrap, discovery registration, convergence) sees a concrete value.
pub fn load(path: &Path) -> Result<ResolvedConfig> {
    let mut config = read_config(path)?;
    config.discovery.validate().with_context(|| {
        format!(
            "validating [discovery] in coordinator config {}",
            path.display()
        )
    })?;
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
    /// extended with `cluster_id`, `[discovery]`, and `[raft].rpc_timeout`
    /// (fields this module adds ahead of the doc pass).
    const FULL_EXAMPLE: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"

[discovery]
backend = "static"
cluster_size = 3
removal_grace = "60s"
replacement_grace = "60s"

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

    const MINIMAL_EXAMPLE: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"

[listen]
advertise_host = "coord-1.example.com"

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"
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
        assert_eq!(config.discovery.removal_grace, Duration::from_secs(60));
        assert_eq!(config.discovery.replacement_grace, Duration::from_secs(60));
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

        // Absent [discovery] → empty static backend with defaults.
        assert_eq!(config.discovery.backend, BackendKind::Static);
        assert_eq!(config.discovery.cluster_size, 3);
        assert!(config.discovery.static_addrs().is_empty());

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
            "{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"dns\"\n\n\
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
            "{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"file\"\n\n\
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
            "{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"ec2-asg\"\n\n\
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
            "{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"ec2-asg\"\n\n\
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
        let contents = format!("{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"ec2-asg\"\n");
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
            "{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"dns\"\n\n\
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
        let contents = format!("{MINIMAL_EXAMPLE}\n[discovery]\nbackend = \"dns\"\n");
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
    fn resolve_advertise_host_prefers_explicit_value() {
        assert_eq!(
            resolve_advertise_host(Some("coord-7.example.com")).expect("explicit resolves"),
            "coord-7.example.com"
        );
    }
}
