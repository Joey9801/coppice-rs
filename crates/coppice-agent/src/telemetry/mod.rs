//! Job telemetry: the sink framework and the filesystem segment store
//! (docker-executor.md §8).
//!
//! The agent collects two streams of per-container telemetry — periodic
//! resource [`MetricSample`]s (§8.1) and raw [`LogChunk`]s (§8.2) — and hands
//! them to a set of configured *sinks*. A sink is anything that can absorb a
//! batch: [`MetricsSink`] / [`LogSink`] (§8.3). The v1 sink is
//! [`FilesystemSink`] (§8.4), a segmented per-attempt SQLite store that is the
//! *local source of truth* and the backing store for coordinator-initiated
//! reads.
//!
//! This module is job telemetry only. Agent-*internal* metrics
//! (`agent_telemetry_*`) are a completely separate concern that rides the
//! repo's per-module [`describe_metrics`]/[`gather_metrics`] fan-out, wired into
//! the crate root in `lib.rs`.
//!
//! [`build`] assembles the whole subsystem from `[telemetry]` config: it opens
//! every configured [`FilesystemSink`], spawns each sink's retention janitor,
//! and hands the executor a [`TelemetryHub`] fanned out to them. The Docker
//! collectors (`executor::docker::stats`/`logs`) feed that hub, use the returned
//! [`Telemetry::log_store`] for the §8.2 resume boundary, and mark every
//! [`Telemetry::stores`] entry ended.

pub mod fs_sink;
pub mod hub;
pub mod sink;

use std::path::{Path, PathBuf};

use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::time::{Duration, Timestamp};
use tokio::sync::watch;

use crate::pressure::DiskPressure;

pub use fs_sink::{
    spawn_retention_janitor, AttemptTelemetry, FilesystemSink, FilesystemSinkOptions, LogQuery,
    StoreError, StoredLogChunk,
};
pub use hub::{HubSink, SinkInstance, TelemetryHub};
pub use sink::{FilesystemSinkConfig, LogSink, MetricsSink, SinkConfig, SinkKind};

// ---- construction from config (docker-executor.md §8.3, §8.4) -----------

/// Everything `run_daemon` constructs from `[telemetry]` config (§8.3/§8.4):
/// the fan-out hub the collectors feed, the filesystem stores behind it, and the
/// retention janitors that must stay alive for the process lifetime.
pub struct Telemetry {
    /// The fan-out hub the Docker collectors append batches to.
    pub hub: TelemetryHub,
    /// Every filesystem sink instance, in config order. The full set that all get
    /// the attempt-ended marker (§8.4); the §8.2 resume authority is
    /// [`log_store`](Self::log_store), not `stores[0]`.
    pub stores: Vec<FilesystemSink>,
    /// The §8.2 resume authority: the **first** filesystem sink whose `kinds`
    /// include [`SinkKind::Logs`], i.e. the first store that actually consumes the
    /// log stream and so advances its `MAX(at)` boundary. `None` when no sink
    /// consumes logs. A metrics-only `stores[0]` never sees a log chunk, so its
    /// boundary would stay `None` forever and every adoption/reconnect would
    /// replay the container's whole retained history into the real log sink —
    /// which is why the resume boundary is derived from *this* store, never
    /// `stores.first()`.
    pub log_store: Option<FilesystemSink>,
    /// Retention janitor handles, one per filesystem sink; the caller keeps them
    /// alive for the process lifetime (dropping a handle stops its janitor).
    pub janitors: Vec<tokio::task::JoinHandle<()>>,
}

/// Build the telemetry subsystem from `[telemetry]` config (docker-executor.md
/// §8.3, §8.4).
///
/// For each configured filesystem sink this opens a [`FilesystemSink`] (rooted
/// at the entry's `dir`, else `<data_dir>/telemetry`), spawns its retention
/// janitor over the shared pressure signal (§8.4/§9), and registers it with the
/// hub for the streams its `kinds` list. The segment-roll and live-retention
/// knobs are node-wide `[telemetry]` policy; only `retention` is per-sink. A
/// config with zero sinks yields a hub with no queues — appends become no-ops,
/// which is how an operator disables job telemetry (§8.3).
pub async fn build(
    config: &crate::config::TelemetryConfig,
    data_dir: &Path,
    pressure_paths: Vec<PathBuf>,
    high_pct: u8,
    pressure: watch::Receiver<DiskPressure>,
) -> anyhow::Result<Telemetry> {
    let mut stores = Vec::new();
    let mut janitors = Vec::new();
    let mut hub_sinks = Vec::new();
    let mut log_store: Option<FilesystemSink> = None;
    // Canonical roots seen so far, so two filesystem entries that resolve to the
    // same segment tree are rejected (§8.4): two independent writers + retention
    // janitors over one tree let a shorter-retention instance delete a
    // longer-retention one's data. Config-level validate cannot catch this — it
    // does not know `data_dir` — so the default-resolution collision (any two
    // entries omitting `dir`) is only visible here. The directory is created
    // first and then **canonicalized**, so textual aliases (`a/./b`, relative vs
    // absolute) and symlinked aliases of one tree all collide on the one real
    // path — raw `PathBuf` equality would wave them through and recreate the
    // corruption this guard exists to stop.
    let mut seen_roots: Vec<(usize, PathBuf)> = Vec::new();
    for (index, entry) in config.sinks.iter().enumerate() {
        match entry {
            SinkConfig::Filesystem(fs) => {
                let configured = fs.dir.clone().unwrap_or_else(|| data_dir.join("telemetry"));
                std::fs::create_dir_all(&configured).map_err(|err| {
                    anyhow::anyhow!(
                        "creating telemetry.sinks[{index}] root {}: {err}",
                        configured.display()
                    )
                })?;
                let root = std::fs::canonicalize(&configured).map_err(|err| {
                    anyhow::anyhow!(
                        "resolving telemetry.sinks[{index}] root {}: {err}",
                        configured.display()
                    )
                })?;
                if let Some((first, _)) = seen_roots.iter().find(|(_, seen)| *seen == root) {
                    anyhow::bail!(
                        "telemetry.sinks[{first}] and telemetry.sinks[{index}] resolve to the \
                         same root {}; give each instance a distinct `dir` (or consolidate their \
                         kinds into one entry) — two writers + retention janitors over one \
                         segment tree corrupt each other (§8.4)",
                        root.display()
                    );
                }
                seen_roots.push((index, root.clone()));
                let options = FilesystemSinkOptions {
                    // The canonical path, so the writer and janitor agree with the
                    // dedup guard about which tree they own (`FilesystemSink::new`'s
                    // own create_dir_all is then a no-op).
                    root,
                    // Segment roll + live-retention bounds are node-wide policy;
                    // ByteSize passes straight through, std durations cross into
                    // the workspace `Duration` the roll/sweep policy compares in.
                    segment_max: config.segment_max,
                    segment_max_age: Duration::from(config.segment_max_age),
                    retention: Duration::from(fs.retention),
                    live_retention: Duration::from(config.live_retention),
                    pressure_paths: pressure_paths.clone(),
                    high_pct,
                };
                let sink = FilesystemSink::new(options).await?;
                janitors.push(spawn_retention_janitor(sink.clone(), pressure.clone()));
                // The first log-consuming store is the §8.2 resume authority.
                if log_store.is_none() && fs.kinds.contains(&SinkKind::Logs) {
                    log_store = Some(sink.clone());
                }
                stores.push(sink.clone());
                hub_sinks.push(HubSink {
                    sink: SinkInstance::Filesystem(sink),
                    kinds: fs.kinds.clone(),
                });
            }
        }
    }
    let hub = TelemetryHub::new(hub_sinks, config.queue_depth);
    Ok(Telemetry {
        hub,
        stores,
        log_store,
        janitors,
    })
}

// ---- internal metrics (docker-executor.md §8.3) -------------------------

/// Batches a sink failed to persist and therefore dropped after accounting
/// (§8.3). An **error-level** counter: the hub→sink path is at-most-once in
/// process and any steady-state loss is a defect signal, never sanctioned
/// (sanctioned loss is only a crash, §8.4). The filesystem sink increments this
/// per failed flush batch.
const AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL: &str = "agent_telemetry_fs_write_errors_total";

/// Batches the [`TelemetryHub`] dropped because a sink's queue was full (§8.3).
/// Also an **error-level** counter: a full queue is a failure mode, not a policy
/// — expected volume sits orders of magnitude below what the queue holds, so any
/// steady-state drop is a defect signal (loss is sanctioned only in a crash,
/// §8.4). The hub increments this on **every** drop-oldest, and rate-limits the
/// accompanying warn to the first drop of a streak.
const AGENT_TELEMETRY_SINK_DROPPED_BATCHES: &str = "agent_telemetry_sink_dropped_batches";

/// Register this module's internal metric names (docker-executor.md §8.3). Part
/// of the crate-level [`crate::describe_metrics`] fan-out.
pub fn describe_metrics() {
    metrics::describe_counter!(
        AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL,
        metrics::Unit::Count,
        "Telemetry batches the filesystem sink failed to persist and dropped (§8.3)."
    );
    metrics::describe_counter!(
        AGENT_TELEMETRY_SINK_DROPPED_BATCHES,
        metrics::Unit::Count,
        "Telemetry batches the hub dropped because a sink's queue was full (§8.3)."
    );
}

/// Point-in-time sampling for this module (docker-executor.md §8.3). A no-op:
/// both metrics are *pushed* counters incremented at their failure event (the
/// push-style convention the cache/disk modules follow), so there is nothing to
/// sample here.
pub fn gather_metrics() {}

// ---- telemetry types (docker-executor.md §8.1, §8.2) --------------------

/// Which of a container's two output streams a [`LogChunk`] came from
/// (docker-executor.md §8.2). Encoded in the segment store as `0`/`1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

impl LogStream {
    /// The SQL encoding: `0` for [`LogStream::Stdout`], `1` for
    /// [`LogStream::Stderr`].
    pub(crate) fn to_i64(self) -> i64 {
        match self {
            LogStream::Stdout => 0,
            LogStream::Stderr => 1,
        }
    }

    /// Decode the SQL encoding. Any value other than `1` reads as
    /// [`LogStream::Stdout`] — the store only ever writes `0`/`1`, and a corrupt
    /// row's stream is not worth failing a read over (§8.4 lossy-tolerant reads).
    pub(crate) fn from_i64(raw: i64) -> LogStream {
        match raw {
            1 => LogStream::Stderr,
            _ => LogStream::Stdout,
        }
    }
}

/// One periodic resource sample for a running container (docker-executor.md
/// §8.1). Counters are cumulative — sinks and readers derive rates, so a missed
/// sample loses resolution, never mass. Extended additively when GPU and friends
/// land, the same evolution style as the protos.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSample {
    /// The placement this sample belongs to.
    pub allocation: AllocationId,
    /// The attempt this sample belongs to (the segment directory's identity).
    pub attempt: AttemptId,
    /// The job this sample belongs to (the segment directory's identity).
    pub job: JobId,
    /// When the sample was taken (µs-quantised; the storage encoding is int64
    /// µs).
    pub at: Timestamp,
    /// Cumulative CPU time consumed; readers derive utilisation.
    pub cpu_usage_total: Duration,
    /// Cumulative CPU time the container was throttled.
    pub cpu_throttled_total: Duration,
    /// Current resident memory.
    pub memory_used_bytes: u64,
    /// Peak resident memory over the attempt so far.
    pub memory_peak_bytes: u64,
    /// Writable-layer bytes, from the disk poller's last reading (§6.2).
    pub disk_writable_bytes: u64,
    /// Image bytes — constant per attempt; writable + image = usage (§6.2).
    pub disk_image_bytes: u64,
    /// Cumulative bytes received on the container network.
    pub net_rx_bytes_total: u64,
    /// Cumulative bytes transmitted on the container network.
    pub net_tx_bytes_total: u64,
    /// Cumulative block-I/O bytes read.
    pub blkio_read_bytes_total: u64,
    /// Cumulative block-I/O bytes written.
    pub blkio_write_bytes_total: u64,
}

/// One raw slice of a container's log output (docker-executor.md §8.2). The
/// `bytes` are the daemon's content verbatim — never re-framed, aligned, or
/// deduplicated — because identical user writes are semantically distinct
/// (§8.2 at-least-once contract).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogChunk {
    /// The placement this chunk belongs to.
    pub allocation: AllocationId,
    /// The attempt this chunk belongs to (the segment directory's identity).
    pub attempt: AttemptId,
    /// The job this chunk belongs to (the segment directory's identity).
    pub job: JobId,
    /// Docker's per-line timestamp, µs-quantised.
    pub at: Timestamp,
    /// Which output stream produced the chunk.
    pub stream: LogStream,
    /// The raw payload, no re-framing of user content.
    pub bytes: bytes::Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TelemetryConfig;
    use crate::pressure::DiskPressure;
    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use tempfile::TempDir;

    /// A filesystem sink config entry with the given kinds and optional dir.
    fn fs_entry(kinds: Vec<SinkKind>, dir: Option<PathBuf>) -> SinkConfig {
        SinkConfig::Filesystem(FilesystemSinkConfig {
            kinds,
            retention: std::time::Duration::from_secs(60),
            dir,
        })
    }

    /// Build the telemetry subsystem over `sinks`, rooted under `data_dir`, on a
    /// fresh pressure channel whose sender is returned so the janitors it spawns
    /// stay alive for the test.
    async fn build_with(
        data_dir: &Path,
        sinks: Vec<SinkConfig>,
    ) -> (anyhow::Result<Telemetry>, watch::Sender<DiskPressure>) {
        let config = TelemetryConfig {
            sinks,
            ..Default::default()
        };
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let telemetry = build(&config, data_dir, vec![data_dir.to_path_buf()], 90, rx).await;
        (telemetry, tx)
    }

    // ---- Fix 5: two entries resolving to one root are rejected (§8.4) --------

    #[tokio::test]
    async fn two_default_dir_entries_collide_on_the_resolved_root() {
        let data_dir = TempDir::new().unwrap();
        // Both omit `dir`, so both resolve to <data_dir>/telemetry.
        let (result, _tx) = build_with(
            data_dir.path(),
            vec![
                fs_entry(vec![SinkKind::Metrics], None),
                fs_entry(vec![SinkKind::Logs], None),
            ],
        )
        .await;
        let err = match result {
            Ok(_) => panic!("duplicate resolved roots must fail"),
            Err(err) => err,
        };
        let root = data_dir.path().join("telemetry");
        let message = format!("{err:#}");
        assert!(
            message.contains(&root.display().to_string()),
            "the error names the resolved root, got: {message}"
        );
    }

    #[tokio::test]
    async fn a_dot_alias_of_a_root_collides_after_canonicalization() {
        let data_dir = TempDir::new().unwrap();
        let plain = data_dir.path().join("tel");
        // The same tree spelled with a `.` component: raw `PathBuf` equality
        // would wave this through; canonicalization collapses it.
        let dotted = data_dir.path().join(".").join("tel");
        let (result, _tx) = build_with(
            data_dir.path(),
            vec![
                fs_entry(vec![SinkKind::Metrics], Some(plain)),
                fs_entry(vec![SinkKind::Logs], Some(dotted)),
            ],
        )
        .await;
        assert!(
            result.is_err(),
            "a textual alias of an earlier root must collide (§8.4)"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlinked_alias_of_a_root_collides_after_canonicalization() {
        let data_dir = TempDir::new().unwrap();
        let real = data_dir.path().join("real");
        std::fs::create_dir_all(&real).unwrap();
        let alias = data_dir.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let (result, _tx) = build_with(
            data_dir.path(),
            vec![
                fs_entry(vec![SinkKind::Metrics], Some(real)),
                fs_entry(vec![SinkKind::Logs], Some(alias)),
            ],
        )
        .await;
        assert!(
            result.is_err(),
            "a symlinked alias of an earlier root must collide (§8.4)"
        );
    }

    #[tokio::test]
    async fn distinct_dirs_build_stores_in_config_order_with_one_janitor_each() {
        let data_dir = TempDir::new().unwrap();
        let first = data_dir.path().join("a");
        let second = data_dir.path().join("b");
        let (result, _tx) = build_with(
            data_dir.path(),
            vec![
                fs_entry(vec![SinkKind::Metrics, SinkKind::Logs], Some(first.clone())),
                fs_entry(vec![SinkKind::Logs], Some(second.clone())),
            ],
        )
        .await;
        let telemetry = result.expect("distinct dirs build");
        assert_eq!(telemetry.stores.len(), 2, "one store per entry, in order");
        assert_eq!(
            telemetry.janitors.len(),
            2,
            "one retention janitor per filesystem sink"
        );
    }

    // ---- Fix 1: log_store is the first LOG-consuming sink, not stores[0] -----

    #[tokio::test]
    async fn log_store_is_the_first_log_consuming_sink_not_stores_first() {
        let data_dir = TempDir::new().unwrap();
        let metrics_dir = data_dir.path().join("metrics-only");
        let logs_dir = data_dir.path().join("logs");
        // A metrics-only entry FIRST, a logs entry SECOND: `stores[0]` never sees a
        // log chunk, so `log_store` must be the second sink.
        let (result, _tx) = build_with(
            data_dir.path(),
            vec![
                fs_entry(vec![SinkKind::Metrics], Some(metrics_dir)),
                fs_entry(vec![SinkKind::Logs], Some(logs_dir)),
            ],
        )
        .await;
        let telemetry = result.expect("build");
        let log_store = telemetry.log_store.expect("a logs sink exists");

        // Prove identity by writing a chunk through `log_store` and reading it back
        // from `stores[1]` (same root) while `stores[0]` stays empty.
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let chunk = LogChunk {
            allocation: alloc,
            attempt,
            job,
            at: Timestamp::UNIX_EPOCH + Duration::from_secs(1),
            stream: LogStream::Stdout,
            bytes: bytes::Bytes::from_static(b"hello"),
        };
        LogSink::append(&log_store, std::slice::from_ref(&chunk)).await;

        let via_second = telemetry.stores[1]
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 4 })
            .await
            .unwrap();
        assert_eq!(
            via_second.len(),
            1,
            "the chunk written through log_store is readable from stores[1] (same root)"
        );
        // The metrics-only stores[0] has no data for this attempt at all — an
        // `UnknownAttempt` error (its root has no such dir) or an empty read, either
        // way proving log_store is stores[1], not stores[0].
        let via_first = telemetry.stores[0]
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 4 })
            .await
            .unwrap_or_default();
        assert!(
            via_first.is_empty(),
            "the metrics-only stores[0] never received the log chunk — log_store is not stores[0]"
        );
    }

    // ---- Fix 4: empty sinks disable collection (§8.3) -----------------------

    #[tokio::test]
    async fn empty_sinks_yield_no_stores_no_log_store_and_no_consumers() {
        let data_dir = TempDir::new().unwrap();
        let (result, _tx) = build_with(data_dir.path(), vec![]).await;
        let telemetry = result.expect("an empty config builds");
        assert!(telemetry.stores.is_empty(), "no sinks ⇒ no stores");
        assert!(
            telemetry.log_store.is_none(),
            "no sinks ⇒ no resume authority"
        );
        assert!(
            !telemetry.hub.consumes(SinkKind::Metrics),
            "no sink consumes metrics"
        );
        assert!(
            !telemetry.hub.consumes(SinkKind::Logs),
            "no sink consumes logs"
        );
    }
}
