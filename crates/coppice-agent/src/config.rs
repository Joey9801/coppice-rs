//! Node agent configuration file (`agent.toml`, ADR 0020 conventions).
//!
//! The agent reads exactly one TOML file at startup: its `node_id`, the data
//! directory the durable journal lives in, the coordinator endpoints to dial,
//! mTLS material (by path only), advertised capacity, and a handful of timing
//! knobs. Everything here is node-local; anything two replicas must agree on
//! is cluster policy and never appears in this file (ADR 0020's litmus test).
//!
//! The conventions mirror the coordinator's config module exactly:
//! `deny_unknown_fields` so a typo'd knob fail-stops naming the offending key;
//! durations are humane strings (`"10s"`, `"500ms"`) via `humantime_serde`,
//! which rejects bare integers by construction; secrets are referenced by
//! path so the file stays safe to commit, diff, and attach to support bundles.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use serde::Deserialize;

/// The agent's fully-parsed configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This node's identity (ADR 0009). The agent's mTLS client certificate
    /// carries this id's typed string form (`node-<uuid>`) as its subject CN;
    /// the coordinator authenticates that the CN matches the `node` in every
    /// report. Parsed from the same typed string form (ADR 0024).
    pub node_id: NodeId,

    /// Root of this node's on-disk state: the durable journal (`journal`) and
    /// its `LOCK` live directly under here.
    pub data_dir: PathBuf,

    /// Coordinator endpoints to dial (`"host:port"`), tried in order and
    /// rotated on connection failure or a not-leader refusal. Sessions
    /// terminate on the leader only; a follower refuses with a leader hint
    /// (agent-coordinator.md).
    pub coordinators: Vec<String>,

    /// mTLS material for the session transport (ADR 0011). Required: there is
    /// no insecure fallback.
    pub tls: TlsConfig,

    /// Advertised capacity. v1 advertises the configured vector verbatim;
    /// live detection lands later.
    pub capacity: CapacityConfig,

    /// Heartbeat cadence: `Heartbeat` (capacity, running set, image-cache
    /// inventory) is sent this often once registered.
    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub heartbeat_interval: Duration,

    /// Minimum reconnect backoff after a stream break (exponential up to
    /// [`Config::reconnect_backoff_max`]).
    #[serde(default = "default_backoff_min", with = "humantime_serde")]
    pub reconnect_backoff_min: Duration,

    /// Maximum reconnect backoff.
    #[serde(default = "default_backoff_max", with = "humantime_serde")]
    pub reconnect_backoff_max: Duration,

    /// Placement labels advertised at registration. `BTreeMap` keeps the
    /// canonical ascending-key ordering the wire form requires.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// mTLS material (ADR 0011). Secrets by path reference only.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
}

/// Advertised capacity vector (v1: configured, not detected).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    /// Milli-CPU units (1000 = one core).
    pub cpu_millis: u64,
    /// Memory in bytes.
    pub memory_bytes: u64,
    /// Disk in bytes.
    pub disk_bytes: u64,
}

impl Config {
    /// The advertised capacity as a domain [`Resources`] vector.
    pub fn capacity_resources(&self) -> Resources {
        Resources {
            cpu_millis: self.capacity.cpu_millis,
            memory_bytes: self.capacity.memory_bytes,
            disk_bytes: self.capacity.disk_bytes,
        }
    }

    /// The strongly-typed node identity.
    pub fn node(&self) -> NodeId {
        self.node_id
    }

    /// Emit the effective configuration at startup.
    ///
    /// Safe to log in full: TLS material is referenced by path, never inline
    /// (ADR 0020), so there is nothing to redact.
    pub fn log_effective(&self) {
        tracing::info!(
            node_id = %self.node_id,
            data_dir = %self.data_dir.display(),
            coordinators = ?self.coordinators,
            heartbeat_interval = ?self.heartbeat_interval,
            reconnect_backoff_min = ?self.reconnect_backoff_min,
            reconnect_backoff_max = ?self.reconnect_backoff_max,
            capacity = ?self.capacity,
            labels = ?self.labels,
            "effective agent configuration"
        );
    }
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_backoff_min() -> Duration {
    Duration::from_millis(500)
}

fn default_backoff_max() -> Duration {
    Duration::from_secs(15)
}

/// Read and parse the config file, wrapping any I/O or deserialization
/// failure with the file path so the error names both the file and (via
/// `serde`'s own message) the offending key.
pub fn load(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading agent config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("reading agent config {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_config(contents: &str) -> (tempfile::NamedTempFile, PathBuf) {
        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(contents.as_bytes())
            .expect("write temp file");
        let path = file.path().to_path_buf();
        (file, path)
    }

    const FULL_EXAMPLE: &str = r#"
node_id = "node-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice-agent"
coordinators = ["coord-1.example.com:7072", "coord-2.example.com:7072"]

heartbeat_interval = "5s"
reconnect_backoff_min = "250ms"
reconnect_backoff_max = "30s"

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[capacity]
cpu_millis   = 32000
memory_bytes = 137438953472
disk_bytes   = 1099511627776

[labels]
zone = "us-east-1a"
pool = "batch"
"#;

    const MINIMAL_EXAMPLE: &str = r#"
node_id = "node-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice-agent"
coordinators = ["coord-1.example.com:7072"]

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[capacity]
cpu_millis   = 8000
memory_bytes = 17179869184
disk_bytes   = 107374182400
"#;

    #[test]
    fn full_example_parses() {
        let (_guard, path) = write_config(FULL_EXAMPLE);
        let config = load(&path).expect("full example should parse");

        assert_eq!(
            config.node_id,
            "node-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10".parse().unwrap()
        );
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/coppice-agent"));
        assert_eq!(config.coordinators.len(), 2);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(config.reconnect_backoff_min, Duration::from_millis(250));
        assert_eq!(config.reconnect_backoff_max, Duration::from_secs(30));
        assert_eq!(config.capacity_resources().cpu_millis, 32000);
        assert_eq!(config.capacity_resources().memory_bytes, 137438953472);
        assert_eq!(
            config.labels.get("zone").map(String::as_str),
            Some("us-east-1a")
        );
    }

    #[test]
    fn minimal_example_applies_defaults() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = load(&path).expect("minimal example should parse");

        assert_eq!(config.heartbeat_interval, default_heartbeat_interval());
        assert_eq!(config.reconnect_backoff_min, default_backoff_min());
        assert_eq!(config.reconnect_backoff_max, default_backoff_max());
        assert!(config.labels.is_empty());
    }

    #[test]
    fn unknown_key_fails_naming_the_key() {
        let bad = format!("{MINIMAL_EXAMPLE}\nheatbeat_interval = \"10s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd key should fail");
        let message = format!("{err:#}");
        assert!(
            message.contains("heatbeat_interval"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn raw_integer_duration_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\nheartbeat_interval = 10\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("unlabelled duration should fail");
        assert!(!format!("{err:#}").is_empty());
    }

    #[test]
    fn labels_iterate_in_canonical_key_order() {
        let (_guard, path) = write_config(FULL_EXAMPLE);
        let config = load(&path).expect("parse");
        let keys: Vec<&String> = config.labels.keys().collect();
        assert_eq!(keys, vec!["pool", "zone"]);
    }
}
