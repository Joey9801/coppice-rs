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
//! **Not yet here:** the `TelemetryHub` (§8.3 fan-out, bounded per-sink queues,
//! drop accounting) and the Docker collectors (`stats.rs`/`logs.rs`) land in a
//! later slice; the traits and the filesystem sink are the foundation they build
//! on.

pub mod fs_sink;
pub mod sink;

use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::time::{Duration, Timestamp};

pub use fs_sink::{
    spawn_retention_janitor, AttemptTelemetry, FilesystemSink, FilesystemSinkOptions, LogQuery,
    StoreError, StoredLogChunk,
};
pub use sink::{FilesystemSinkConfig, LogSink, MetricsSink, SinkConfig, SinkKind};

// ---- internal metrics (docker-executor.md §8.3) -------------------------

/// Batches a sink failed to persist and therefore dropped after accounting
/// (§8.3). An **error-level** counter: the hub→sink path is at-most-once in
/// process and any steady-state loss is a defect signal, never sanctioned
/// (sanctioned loss is only a crash, §8.4). The filesystem sink increments this
/// per failed flush batch.
const AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL: &str = "agent_telemetry_fs_write_errors_total";

/// Register this module's internal metric names (docker-executor.md §8.3). Part
/// of the crate-level [`crate::describe_metrics`] fan-out.
pub fn describe_metrics() {
    metrics::describe_counter!(
        AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL,
        metrics::Unit::Count,
        "Telemetry batches the filesystem sink failed to persist and dropped (§8.3)."
    );
}

/// Point-in-time sampling for this module (docker-executor.md §8.3). A no-op:
/// the sole metric is a *pushed* counter incremented at its failure event (the
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
