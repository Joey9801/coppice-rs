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
use coppice_core::bytes::ByteSize;
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use serde::Deserialize;

use crate::telemetry::{FilesystemSinkConfig, SinkConfig, SinkKind};

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

    /// Full physical capacity. The system reservation is deducted before this
    /// vector is advertised upstream.
    pub capacity: CapacityConfig,

    /// Capacity withheld for the agent, daemon, kernel, and transient system
    /// work (§6.4). Defaults are fixed values, never capacity-scaled.
    #[serde(default)]
    pub reservation: ReservationConfig,

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

    /// Executor-side knobs. Defaulted whole, so a bare v1 config stays valid.
    #[serde(default)]
    pub executor: ExecutorConfig,

    /// Host disk-pressure thresholds (docker-executor.md §9). Defaulted whole,
    /// so a bare v1 config stays valid.
    #[serde(default)]
    pub pressure: PressureConfig,

    /// Image-cache policy (docker-executor.md §7). A top-level `[image_cache]`
    /// table per §10, not under `[executor]`. Defaulted whole, so a bare v1
    /// config stays valid.
    #[serde(default)]
    pub image_cache: ImageCacheConfig,

    /// Job-telemetry policy (docker-executor.md §8). A top-level `[telemetry]`
    /// table per §10, not under `[executor]`: telemetry is executor-agnostic
    /// (§2) — sampling cadence, drain backstop, segment roll bounds, live
    /// retention, and the configured sinks are the same regardless of which
    /// runtime executes a job, so they do not belong to `[executor]`. Defaulted
    /// whole, so a bare v1 config stays valid.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

/// Container-executor configuration (docker-executor.md §10). Node-local knobs
/// for the Docker runtime: the daemon endpoint, the fallback UID for images
/// that don't pin a non-root `USER`, the PID ceiling, and the reap-janitor
/// backstop.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorConfig {
    /// The Docker daemon endpoint the executor dials. A local Unix socket by
    /// default; a `tcp://host:port` form reaches a remote daemon over
    /// **plaintext** HTTP (`https://` is rejected until daemon TLS is wired),
    /// in which case the daemon's data-root is not a local filesystem and the
    /// §9 pressure monitor covers `data_dir` only.
    #[serde(default = "default_docker_host")]
    pub docker_host: String,

    /// The UID a container runs as when its image does not set a non-root
    /// `USER` (§6). `65534` (`nobody`) by default. UID 0 is rejected at load:
    /// the whole point of the rule is that workloads never run as root, so a
    /// root default would silently defeat it. A job may still request its own
    /// non-root UID; UID 0 from a job is rejected at start as user error.
    #[serde(default = "default_default_uid")]
    pub default_uid: u32,

    /// The `PidsLimit` applied to every container (§6): fork-bomb hygiene, not
    /// user-visible policy. Must be positive.
    #[serde(default = "default_pids_limit")]
    pub pids_limit: i64,

    /// Age past which the session janitor reaps an exited container whose exit
    /// is already journaled (`now − finished_at`, §5). A generous backstop —
    /// the exit-path reap normally removes containers promptly; this only
    /// catches ones whose reap was lost to a crash or a transient error.
    #[serde(default = "default_reap_janitor_after", with = "humantime_serde")]
    pub reap_janitor_after: Duration,

    /// Give whole-physical-core requests exclusive SMT sibling groups (§6.3).
    /// When disabled Docker receives only `NanoCpus`.
    #[serde(default = "default_whole_core_affinity")]
    pub whole_core_affinity: bool,

    /// Which disk-enforcement strategy the executor uses (docker-executor.md
    /// §6.2). `auto` (the default) probes the daemon at startup and picks native
    /// xfs project quotas when available, else the poll fallback; `quota` and
    /// `poll` pin one strategy. The choice is behind the `DiskEnforcer` seam —
    /// per-job creation and the poll loop are the only code that differs.
    #[serde(default = "default_disk_enforcement")]
    pub disk_enforcement: DiskEnforcement,

    /// How often the poll-fallback disk enforcer sweeps writable-layer usage
    /// (docker-executor.md §6.2). A floor, not a deadline — the sweep runs
    /// serially and never overlaps, so a slow daemon just lengthens the gap.
    /// Ignored under the native-quota strategy (the kernel enforces there).
    #[serde(default = "default_disk_poll_interval", with = "humantime_serde")]
    pub disk_poll_interval: Duration,
}

/// The disk-enforcement strategy selector (docker-executor.md §6.2, §10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskEnforcement {
    /// Probe the daemon at startup: native xfs project quotas when supported,
    /// else the poll fallback.
    Auto,
    /// Operator assertion of native xfs project quotas. Startup fails if a probe
    /// refutes support.
    Quota,
    /// Force the poll fallback regardless of daemon capabilities.
    Poll,
}

impl Default for ExecutorConfig {
    fn default() -> ExecutorConfig {
        ExecutorConfig {
            docker_host: default_docker_host(),
            default_uid: default_default_uid(),
            pids_limit: default_pids_limit(),
            reap_janitor_after: default_reap_janitor_after(),
            whole_core_affinity: default_whole_core_affinity(),
            disk_enforcement: default_disk_enforcement(),
            disk_poll_interval: default_disk_poll_interval(),
        }
    }
}

/// Host disk-pressure thresholds (docker-executor.md §9). Percent of a watched
/// filesystem's space used at which the shared pressure signal escalates to
/// `High` (telemetry retention and the image cache sweep early) and `Critical`
/// (both sweep to floor and the agent refuses new `StartJob`s). Node-local: a
/// smaller node may want to react sooner than a larger one.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureConfig {
    /// Percent used at or above which pressure is `High`. Must satisfy
    /// `0 < high_pct < critical_pct`.
    #[serde(default = "default_high_pct")]
    pub high_pct: u8,

    /// Percent used at or above which pressure is `Critical`. Must satisfy
    /// `high_pct < critical_pct <= 100`.
    #[serde(default = "default_critical_pct")]
    pub critical_pct: u8,
}

impl Default for PressureConfig {
    fn default() -> PressureConfig {
        PressureConfig {
            high_pct: default_high_pct(),
            critical_pct: default_critical_pct(),
        }
    }
}

/// Image-cache policy (docker-executor.md §7, §10). Node-local: the TTL a node
/// keeps idle images for, and how many pulls it runs at once. Both are
/// operational tuning, never cluster policy (ADR 0020), so they live here.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageCacheConfig {
    /// How long an unpinned image may sit idle before the janitor evicts it
    /// (docker-executor.md §7). `last_used_at` is the end of the last attempt
    /// that used it (or its pull time if never used). Must be nonzero — a zero
    /// TTL would evict every image the instant its last pin drops, defeating the
    /// cache. Default 30m.
    #[serde(default = "default_image_cache_ttl", with = "humantime_serde")]
    pub ttl: Duration,

    /// The global concurrent-pull limit (docker-executor.md §7): at most this
    /// many image pulls run at once across all in-flight starts, so a burst of
    /// cold starts cannot saturate the registry or the local disk. Must be at
    /// least 1. Default 2.
    #[serde(default = "default_max_concurrent_pulls")]
    pub max_concurrent_pulls: usize,
}

impl Default for ImageCacheConfig {
    fn default() -> ImageCacheConfig {
        ImageCacheConfig {
            ttl: default_image_cache_ttl(),
            max_concurrent_pulls: default_max_concurrent_pulls(),
        }
    }
}

/// Job-telemetry policy (docker-executor.md §8, §10). Node-local operational
/// tuning — sampling cadence, the forced-drain backstop, segment roll bounds,
/// the live-attempt retention cap, per-sink queue depth, and the configured
/// sinks. Executor-agnostic (§2), so it is a top-level `[telemetry]` table, not
/// nested under `[executor]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// How often the collectors sample per-container resource usage
    /// (docker-executor.md §8.1). Must be nonzero. Default 10s.
    #[serde(default = "default_metrics_interval", with = "humantime_serde")]
    pub metrics_interval: Duration,

    /// The forced-drain backstop (docker-executor.md §8.2): past this age a
    /// wedged log follower's reap proceeds without draining, metering the tail
    /// loss (`agent_log_drain_forced_total`) rather than blocking reap forever.
    /// Must be nonzero. Default 10m.
    #[serde(default = "default_drain_force_after", with = "humantime_serde")]
    pub drain_force_after: Duration,

    /// Segment size at which the filesystem sink rolls to a fresh segment
    /// (docker-executor.md §8.4) — retention deletes whole segments, never rows,
    /// so this bounds the delete granularity. Must be nonzero. Default 256 MiB.
    #[serde(default = "default_segment_max")]
    pub segment_max: ByteSize,

    /// Age at which the filesystem sink rolls to a fresh segment even below
    /// [`Self::segment_max`] (docker-executor.md §8.4), so a low-volume attempt
    /// still bounds how long a segment stays open. Must be nonzero. Default 6h.
    #[serde(default = "default_segment_max_age", with = "humantime_serde")]
    pub segment_max_age: Duration,

    /// The live-attempt cap (docker-executor.md §8.4): the maximum age of a
    /// *running* attempt's closed segments, measured from the successor
    /// segment's start — the open segment is never swept. Must be nonzero.
    /// Default 24h.
    #[serde(default = "default_live_retention", with = "humantime_serde")]
    pub live_retention: Duration,

    /// Depth of each sink instance's bounded queue, in batches
    /// (docker-executor.md §8.3). The hub gives every sink its own queue and
    /// drain task so a slow sink never backpressures container execution; a full
    /// queue drops oldest (a metered defect signal, never policy). Must be at
    /// least 1. Default 1024.
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,

    /// The configured sink instances (docker-executor.md §8.3). Each
    /// `[[telemetry.sinks]]` entry names its `type` and carries that variant's
    /// own fields. Defaults to a single filesystem sink consuming both streams
    /// with 60m retention under `<data_dir>/telemetry` — the documented default.
    /// An empty array is valid: an operator may deliberately disable job
    /// telemetry.
    #[serde(default = "default_sinks")]
    pub sinks: Vec<SinkConfig>,
}

impl Default for TelemetryConfig {
    fn default() -> TelemetryConfig {
        TelemetryConfig {
            metrics_interval: default_metrics_interval(),
            drain_force_after: default_drain_force_after(),
            segment_max: default_segment_max(),
            segment_max_age: default_segment_max_age(),
            live_retention: default_live_retention(),
            queue_depth: default_queue_depth(),
            sinks: default_sinks(),
        }
    }
}

impl TelemetryConfig {
    /// Reject semantically invalid values `serde` alone cannot catch — zero
    /// durations, a zero segment bound, a zero queue depth — and delegate each
    /// sink entry to its own [`SinkConfig::validate`]. Errors name the offending
    /// key, matching the rest of this module. An empty `sinks` array is valid
    /// (job telemetry deliberately disabled).
    fn validate(&self) -> Result<()> {
        if self.metrics_interval.is_zero() {
            anyhow::bail!("telemetry.metrics_interval must be greater than zero (§8.1)");
        }
        if self.drain_force_after.is_zero() {
            anyhow::bail!("telemetry.drain_force_after must be greater than zero (§8.2)");
        }
        if self.segment_max.is_zero() {
            anyhow::bail!("telemetry.segment_max must be greater than zero (§8.4)");
        }
        if self.segment_max_age.is_zero() {
            anyhow::bail!("telemetry.segment_max_age must be greater than zero (§8.4)");
        }
        if self.live_retention.is_zero() {
            anyhow::bail!("telemetry.live_retention must be greater than zero (§8.4)");
        }
        if self.queue_depth == 0 {
            anyhow::bail!("telemetry.queue_depth must be at least 1 (§8.3)");
        }
        for sink in &self.sinks {
            sink.validate()?;
        }
        Ok(())
    }
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
    /// Total RAM the node offers, before the system reservation.
    pub memory: ByteSize,
    /// Total scratch disk the node offers, before the system reservation.
    pub disk: ByteSize,
}

/// Fixed resources withheld from scheduling (§6.4).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationConfig {
    #[serde(default = "default_reservation_cpu_millis")]
    pub cpu_millis: u64,
    #[serde(default = "default_reservation_memory")]
    pub memory: ByteSize,
    #[serde(default = "default_reservation_disk")]
    pub disk: ByteSize,
}

impl Default for ReservationConfig {
    fn default() -> Self {
        Self {
            cpu_millis: default_reservation_cpu_millis(),
            memory: default_reservation_memory(),
            disk: default_reservation_disk(),
        }
    }
}

impl Config {
    /// Full capacity minus the fixed system reservation (§6.4).
    pub fn advertised_resources(&self) -> Resources {
        Resources {
            cpu_millis: self.capacity.cpu_millis - self.reservation.cpu_millis,
            memory: self.capacity.memory - self.reservation.memory,
            disk: self.capacity.disk - self.reservation.disk,
        }
    }

    /// Validate the configured full CPU capacity against discovered physical
    /// cores. Kept separate from file parsing so unit tests and non-Linux
    /// config tooling need not depend on the machine running them.
    pub fn validate_physical_cores(&self, physical_cores: usize) -> Result<()> {
        validate_cpu_capacity(self.capacity.cpu_millis, physical_cores).map_err(anyhow::Error::msg)
    }

    /// The strongly-typed node identity.
    pub fn node(&self) -> NodeId {
        self.node_id
    }

    /// Reject semantically invalid values that `serde` alone cannot catch:
    /// bounds and cross-field ordering. `serde`'s `deny_unknown_fields` and
    /// the humane-duration codec handle shape and typos; this handles meaning.
    /// Errors name the offending key so the operator can fix it directly.
    fn validate(&self) -> Result<()> {
        if self.executor.default_uid == 0 {
            anyhow::bail!("executor.default_uid must not be 0: workloads never run as root (§6)");
        }
        if self.executor.pids_limit <= 0 {
            anyhow::bail!(
                "executor.pids_limit must be positive, got {}",
                self.executor.pids_limit
            );
        }
        if self.executor.disk_poll_interval.is_zero() {
            anyhow::bail!("executor.disk_poll_interval must be greater than zero");
        }
        // The reservation must leave something to schedule on every dimension.
        // CPU and the two sizes are checked separately because they are no
        // longer the same type — the sizes report themselves in IEC units, so
        // the error names the same quantity the operator wrote in the file.
        if self.reservation.cpu_millis >= self.capacity.cpu_millis {
            anyhow::bail!(
                "reservation.cpu_millis ({}) must be less than capacity.cpu_millis ({})",
                self.reservation.cpu_millis,
                self.capacity.cpu_millis
            );
        }
        for (key, reservation, capacity) in [
            ("memory", self.reservation.memory, self.capacity.memory),
            ("disk", self.reservation.disk, self.capacity.disk),
        ] {
            if reservation >= capacity {
                anyhow::bail!(
                    "reservation.{key} ({reservation}) must be less than capacity.{key} ({capacity})"
                );
            }
        }
        if self.image_cache.ttl.is_zero() {
            anyhow::bail!("image_cache.ttl must be greater than zero (§7)");
        }
        if self.image_cache.max_concurrent_pulls < 1 {
            anyhow::bail!("image_cache.max_concurrent_pulls must be at least 1 (§7)");
        }
        let PressureConfig {
            high_pct,
            critical_pct,
        } = self.pressure;
        if !(0 < high_pct && high_pct < critical_pct && critical_pct <= 100) {
            anyhow::bail!(
                "pressure thresholds must satisfy 0 < high_pct < critical_pct <= 100, \
                 got high_pct = {high_pct}, critical_pct = {critical_pct}"
            );
        }
        self.telemetry.validate()?;
        Ok(())
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
            reservation = ?self.reservation,
            labels = ?self.labels,
            executor = ?self.executor,
            pressure = ?self.pressure,
            image_cache = ?self.image_cache,
            telemetry = ?self.telemetry,
            "effective agent configuration"
        );
    }
}

/// Shared startup arithmetic for the affinity-enabled Docker executor.
pub(crate) fn validate_cpu_capacity(
    capacity_cpu_millis: u64,
    physical_cores: usize,
) -> std::result::Result<(), String> {
    let physical_millis = u64::try_from(physical_cores)
        .unwrap_or(u64::MAX)
        .saturating_mul(1000);
    if capacity_cpu_millis > physical_millis {
        return Err(format!(
            "capacity.cpu_millis ({capacity_cpu_millis}) exceeds {physical_cores} physical cores ({physical_millis} mCPU)"
        ));
    }
    Ok(())
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

fn default_reap_janitor_after() -> Duration {
    Duration::from_secs(24 * 60 * 60)
}

fn default_docker_host() -> String {
    "unix:///var/run/docker.sock".to_string()
}

fn default_default_uid() -> u32 {
    65534
}

fn default_pids_limit() -> i64 {
    4096
}

fn default_whole_core_affinity() -> bool {
    true
}

fn default_disk_enforcement() -> DiskEnforcement {
    DiskEnforcement::Auto
}

fn default_disk_poll_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_reservation_cpu_millis() -> u64 {
    1000
}

fn default_reservation_memory() -> ByteSize {
    ByteSize::from_gib(2)
}

fn default_reservation_disk() -> ByteSize {
    ByteSize::from_gib(20)
}

fn default_image_cache_ttl() -> Duration {
    Duration::from_secs(30 * 60)
}

fn default_max_concurrent_pulls() -> usize {
    2
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_drain_force_after() -> Duration {
    Duration::from_secs(10 * 60)
}

fn default_segment_max() -> ByteSize {
    ByteSize::from_mib(256)
}

fn default_segment_max_age() -> Duration {
    Duration::from_secs(6 * 60 * 60)
}

fn default_live_retention() -> Duration {
    Duration::from_secs(24 * 60 * 60)
}

fn default_queue_depth() -> usize {
    1024
}

/// The documented default sink (docker-executor.md §8.3, §10): a single
/// filesystem sink consuming both streams with 60m retention under
/// `<data_dir>/telemetry` (`dir = None`). `FilesystemSinkConfig::default_retention`
/// is private to its module, so the 60m default is spelled explicitly here.
fn default_sinks() -> Vec<SinkConfig> {
    vec![SinkConfig::Filesystem(FilesystemSinkConfig {
        kinds: vec![SinkKind::Metrics, SinkKind::Logs],
        retention: Duration::from_secs(60 * 60),
        dir: None,
    })]
}

fn default_high_pct() -> u8 {
    85
}

fn default_critical_pct() -> u8 {
    95
}

/// Read and parse the config file, wrapping any I/O or deserialization
/// failure with the file path so the error names both the file and (via
/// `serde`'s own message) the offending key.
pub fn load(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading agent config {}", path.display()))?;
    let config: Config =
        toml::from_str(&raw).with_context(|| format!("reading agent config {}", path.display()))?;
    config
        .validate()
        .with_context(|| format!("validating agent config {}", path.display()))?;
    Ok(config)
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
memory     = "128GiB"
disk       = "1TiB"

[reservation]
cpu_millis = 2000
memory = "4GiB"
disk = "40GiB"

[labels]
zone = "us-east-1a"
pool = "batch"

[executor]
docker_host = "tcp://dockerd.internal:2375"
default_uid = 1000
pids_limit = 2048
reap_janitor_after = "1h"
whole_core_affinity = false
disk_enforcement = "poll"
disk_poll_interval = "5s"

[pressure]
high_pct = 80
critical_pct = 90

[image_cache]
ttl = "45m"
max_concurrent_pulls = 3

[telemetry]
metrics_interval = "5s"
drain_force_after = "15m"
segment_max = "512MiB"
segment_max_age = "3h"
live_retention = "12h"
queue_depth = 2048

[[telemetry.sinks]]
type = "filesystem"
kinds = ["metrics", "logs"]
retention = "90m"
dir = "/var/lib/coppice-agent/telemetry"

[[telemetry.sinks]]
type = "filesystem"
kinds = ["logs"]
retention = "30m"
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
memory     = "16GiB"
disk       = "100GiB"
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
        assert_eq!(config.advertised_resources().cpu_millis, 30000);
        assert_eq!(
            config.advertised_resources().memory,
            ByteSize::from_gib(124)
        );
        assert_eq!(
            config.labels.get("zone").map(String::as_str),
            Some("us-east-1a")
        );
        assert_eq!(
            config.executor.reap_janitor_after,
            Duration::from_secs(3600)
        );
        assert_eq!(config.executor.docker_host, "tcp://dockerd.internal:2375");
        assert_eq!(config.executor.default_uid, 1000);
        assert_eq!(config.executor.pids_limit, 2048);
        assert!(!config.executor.whole_core_affinity);
        assert_eq!(config.executor.disk_enforcement, DiskEnforcement::Poll);
        assert_eq!(config.executor.disk_poll_interval, Duration::from_secs(5));
        assert_eq!(config.pressure.high_pct, 80);
        assert_eq!(config.pressure.critical_pct, 90);
        assert_eq!(config.image_cache.ttl, Duration::from_secs(45 * 60));
        assert_eq!(config.image_cache.max_concurrent_pulls, 3);
        assert_eq!(config.telemetry.metrics_interval, Duration::from_secs(5));
        assert_eq!(
            config.telemetry.drain_force_after,
            Duration::from_secs(15 * 60)
        );
        assert_eq!(config.telemetry.segment_max, ByteSize::from_mib(512));
        assert_eq!(
            config.telemetry.segment_max_age,
            Duration::from_secs(3 * 60 * 60)
        );
        assert_eq!(
            config.telemetry.live_retention,
            Duration::from_secs(12 * 60 * 60)
        );
        assert_eq!(config.telemetry.queue_depth, 2048);
        assert_eq!(
            config.telemetry.sinks,
            vec![
                SinkConfig::Filesystem(FilesystemSinkConfig {
                    kinds: vec![SinkKind::Metrics, SinkKind::Logs],
                    retention: Duration::from_secs(90 * 60),
                    dir: Some(PathBuf::from("/var/lib/coppice-agent/telemetry")),
                }),
                SinkConfig::Filesystem(FilesystemSinkConfig {
                    kinds: vec![SinkKind::Logs],
                    retention: Duration::from_secs(30 * 60),
                    dir: None,
                }),
            ]
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
        assert_eq!(
            config.executor.reap_janitor_after,
            default_reap_janitor_after()
        );
        assert_eq!(
            config.executor.reap_janitor_after,
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(config.executor.docker_host, default_docker_host());
        assert_eq!(config.executor.default_uid, 65534);
        assert_eq!(config.executor.pids_limit, 4096);
        assert!(config.executor.whole_core_affinity);
        assert_eq!(config.executor.disk_enforcement, DiskEnforcement::Auto);
        assert_eq!(
            config.executor.disk_poll_interval,
            default_disk_poll_interval()
        );
        assert_eq!(config.executor.disk_poll_interval, Duration::from_secs(30));
        assert_eq!(config.reservation.cpu_millis, 1000);
        assert_eq!(config.reservation.memory, ByteSize::from_gib(2));
        assert_eq!(config.reservation.disk, ByteSize::from_gib(20));
        assert_eq!(config.advertised_resources().cpu_millis, 7000);
        assert_eq!(config.pressure.high_pct, 85);
        assert_eq!(config.pressure.critical_pct, 95);
        assert_eq!(config.image_cache.ttl, default_image_cache_ttl());
        assert_eq!(config.image_cache.ttl, Duration::from_secs(30 * 60));
        assert_eq!(
            config.image_cache.max_concurrent_pulls,
            default_max_concurrent_pulls()
        );
        assert_eq!(config.image_cache.max_concurrent_pulls, 2);
        // An omitted `[telemetry]` table yields every default, including the
        // single documented filesystem sink over both streams.
        assert_eq!(
            config.telemetry.metrics_interval,
            default_metrics_interval()
        );
        assert_eq!(config.telemetry.metrics_interval, Duration::from_secs(10));
        assert_eq!(
            config.telemetry.drain_force_after,
            default_drain_force_after()
        );
        assert_eq!(
            config.telemetry.drain_force_after,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(config.telemetry.segment_max, default_segment_max());
        assert_eq!(config.telemetry.segment_max, ByteSize::from_mib(256));
        assert_eq!(config.telemetry.segment_max_age, default_segment_max_age());
        assert_eq!(
            config.telemetry.segment_max_age,
            Duration::from_secs(6 * 60 * 60)
        );
        assert_eq!(config.telemetry.live_retention, default_live_retention());
        assert_eq!(
            config.telemetry.live_retention,
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(config.telemetry.queue_depth, default_queue_depth());
        assert_eq!(config.telemetry.queue_depth, 1024);
        assert_eq!(config.telemetry.sinks, default_sinks());
        assert_eq!(
            config.telemetry.sinks,
            vec![SinkConfig::Filesystem(FilesystemSinkConfig {
                kinds: vec![SinkKind::Metrics, SinkKind::Logs],
                retention: Duration::from_secs(60 * 60),
                dir: None,
            })]
        );
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
    fn reservation_must_be_strictly_less_than_capacity() {
        let bad = MINIMAL_EXAMPLE.replace("cpu_millis   = 8000", "cpu_millis   = 1000");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("reservation equal to capacity must fail");
        assert!(format!("{err:#}").contains("reservation.cpu_millis"));
    }

    #[test]
    fn full_capacity_must_fit_physical_cores() {
        let (_guard, path) = write_config(MINIMAL_EXAMPLE);
        let config = load(&path).unwrap();
        config.validate_physical_cores(8).unwrap();
        let err = config.validate_physical_cores(7).unwrap_err();
        assert!(format!("{err:#}").contains("capacity.cpu_millis"));
    }

    #[test]
    fn unknown_key_in_executor_table_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\nreap_janitor_afterr = \"1h\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd executor key should fail");
        let message = format!("{err:#}");
        assert!(
            message.contains("reap_janitor_afterr"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn labels_iterate_in_canonical_key_order() {
        let (_guard, path) = write_config(FULL_EXAMPLE);
        let config = load(&path).expect("parse");
        let keys: Vec<&String> = config.labels.keys().collect();
        assert_eq!(keys, vec!["pool", "zone"]);
    }

    #[test]
    fn root_default_uid_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\ndefault_uid = 0\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("UID 0 should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("default_uid"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn non_positive_pids_limit_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\npids_limit = 0\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("pids_limit 0 should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("pids_limit"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn pressure_high_at_least_critical_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[pressure]\nhigh_pct = 95\ncritical_pct = 95\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("high_pct == critical_pct should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("high_pct") && message.contains("critical_pct"),
            "error should name the pressure thresholds, got: {message}"
        );
    }

    #[test]
    fn pressure_zero_high_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[pressure]\nhigh_pct = 0\ncritical_pct = 95\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("high_pct 0 should be rejected");
        assert!(format!("{err:#}").contains("high_pct"));
    }

    #[test]
    fn pressure_critical_over_100_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[pressure]\nhigh_pct = 85\ncritical_pct = 101\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("critical_pct > 100 should be rejected");
        assert!(format!("{err:#}").contains("critical_pct"));
    }

    #[test]
    fn unknown_key_in_pressure_table_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[pressure]\nhigh_pctt = 85\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd pressure key should fail");
        let message = format!("{err:#}");
        assert!(
            message.contains("high_pctt"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn disk_enforcement_parses_all_three_variants() {
        for (raw, expected) in [
            ("auto", DiskEnforcement::Auto),
            ("quota", DiskEnforcement::Quota),
            ("poll", DiskEnforcement::Poll),
        ] {
            let toml = format!("{MINIMAL_EXAMPLE}\n[executor]\ndisk_enforcement = \"{raw}\"\n");
            let (_guard, path) = write_config(&toml);
            let config = load(&path).unwrap_or_else(|e| panic!("{raw:?} should parse: {e:#}"));
            assert_eq!(config.executor.disk_enforcement, expected);
        }
    }

    #[test]
    fn unknown_disk_enforcement_variant_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\ndisk_enforcement = \"native\"\n");
        let (_guard, path) = write_config(&bad);
        assert!(load(&path).is_err(), "an unknown strategy name must fail");
    }

    #[test]
    fn zero_disk_poll_interval_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\ndisk_poll_interval = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero poll interval should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("disk_poll_interval"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn zero_image_cache_ttl_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[image_cache]\nttl = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero cache TTL should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("image_cache.ttl"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn zero_max_concurrent_pulls_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[image_cache]\nmax_concurrent_pulls = 0\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero concurrent-pull limit should be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("image_cache.max_concurrent_pulls"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn unknown_key_in_image_cache_table_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[image_cache]\nttll = \"30m\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd image_cache key should fail");
        assert!(format!("{err:#}").contains("ttll"));
    }

    #[test]
    fn unknown_key_in_executor_table_still_rejected_with_new_fields() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[executor]\ndocker_hostt = \"x\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd executor key should fail");
        assert!(format!("{err:#}").contains("docker_hostt"));
    }

    #[test]
    fn unknown_key_in_telemetry_table_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nmetrics_intervall = \"10s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("typo'd telemetry key should fail");
        let message = format!("{err:#}");
        assert!(
            message.contains("metrics_intervall"),
            "error should name the offending key, got: {message}"
        );
    }

    #[test]
    fn raw_integer_telemetry_duration_is_rejected() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nmetrics_interval = 10\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a bare integer duration should fail");
        assert!(!format!("{err:#}").is_empty());
    }

    #[test]
    fn zero_metrics_interval_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nmetrics_interval = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero metrics interval should be rejected");
        assert!(format!("{err:#}").contains("telemetry.metrics_interval"));
    }

    #[test]
    fn zero_drain_force_after_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\ndrain_force_after = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero drain backstop should be rejected");
        assert!(format!("{err:#}").contains("telemetry.drain_force_after"));
    }

    #[test]
    fn zero_segment_max_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nsegment_max = \"0B\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero segment bound should be rejected");
        assert!(format!("{err:#}").contains("telemetry.segment_max"));
    }

    #[test]
    fn zero_segment_max_age_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nsegment_max_age = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero segment age should be rejected");
        assert!(format!("{err:#}").contains("telemetry.segment_max_age"));
    }

    #[test]
    fn zero_live_retention_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nlive_retention = \"0s\"\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero live retention should be rejected");
        assert!(format!("{err:#}").contains("telemetry.live_retention"));
    }

    #[test]
    fn zero_queue_depth_is_a_config_error() {
        let bad = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nqueue_depth = 0\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a zero queue depth should be rejected");
        assert!(format!("{err:#}").contains("telemetry.queue_depth"));
    }

    #[test]
    fn empty_sinks_array_is_accepted() {
        // An operator may deliberately disable job telemetry by configuring no
        // sinks: an explicit empty array is valid.
        let toml = format!("{MINIMAL_EXAMPLE}\n[telemetry]\nsinks = []\n");
        let (_guard, path) = write_config(&toml);
        let config = load(&path).expect("an empty sinks array should be accepted");
        assert!(config.telemetry.sinks.is_empty());
    }

    #[test]
    fn invalid_sink_entry_is_rejected_through_config_validate() {
        // An empty `kinds` list is shape-valid but meaning-invalid; the delegation
        // to `SinkConfig::validate` must surface it through `Config::validate`.
        let bad =
            format!("{MINIMAL_EXAMPLE}\n[[telemetry.sinks]]\ntype = \"filesystem\"\nkinds = []\n");
        let (_guard, path) = write_config(&bad);
        let err = load(&path).expect_err("a sink with empty kinds should be rejected");
        assert!(format!("{err:#}").contains("kinds"));
    }
}
