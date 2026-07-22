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

    /// Seed list of peer Raft addresses (`"host:port"`), used by admin
    /// tooling to locate the cluster. Not authoritative: the addresses that
    /// matter for consensus live in replicated membership, not here.
    #[serde(default)]
    pub(crate) peers: Vec<String>,

    /// Listen and advertise addresses. Required: every deployment needs at
    /// least `advertise_host`, which has no sane default.
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

    /// The hostname peers and agents dial. No default: `0.0.0.0` binds are
    /// never dialable addresses, so this must be supplied explicitly.
    pub(crate) advertise_host: String,
}

impl ListenConfig {
    /// The Raft address this replica advertises to peers: `advertise_host`
    /// combined with the port half of [`raft_addr`](ListenConfig::raft_addr).
    ///
    /// Kept as a method rather than a stored field so the two can never
    /// silently drift apart when either is edited.
    pub(crate) fn advertised_raft_addr(&self) -> String {
        format!("{}:{}", self.advertise_host, self.raft_addr.port())
    }
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
    let config = read_config(path)?;
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
    /// extended with `cluster_id`, `peers`, and `[raft].rpc_timeout` (fields
    /// this module adds ahead of the doc pass).
    const FULL_EXAMPLE: &str = r#"
cluster_id = "cluster-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice"
peers = ["coord-1.batch.example.com:7071", "coord-2.batch.example.com:7071"]

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
        assert_eq!(
            config.peers,
            vec![
                "coord-1.batch.example.com:7071".to_string(),
                "coord-2.batch.example.com:7071".to_string(),
            ]
        );

        assert_eq!(config.listen.client_addr, default_client_addr());
        assert_eq!(config.listen.raft_addr, default_raft_addr());
        assert_eq!(config.listen.agent_addr, default_agent_addr());
        assert_eq!(config.listen.advertise_host, "coord-3.batch.example.com");

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

        assert!(config.peers.is_empty());

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
}
