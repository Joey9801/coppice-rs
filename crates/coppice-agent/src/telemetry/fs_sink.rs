//! The filesystem sink: a segmented per-attempt SQLite store
//! (docker-executor.md §8.4).
//!
//! Each running attempt's telemetry is written to one or more SQLite *segment*
//! files under `<root>/<job-id>/<attempt-id>/seg-<start-µs>.db`. A segment rolls
//! at a size or age bound so no single file grows without limit, segments are
//! created in strictly increasing start order (a range read probes each one
//! through its `at` index and merges), and retention is pure whole-file
//! unlinks — never `DELETE`+vacuum. The
//! store is the *local source of truth* and the backing store for
//! coordinator-initiated reads (the [`FilesystemSink`] read API here; the RPC
//! surface is a later translation layer, §8.4).
//!
//! [`FilesystemSink`] is a cheap `Clone` handle over a shared `Inner`, the same
//! idiom as [`ImageCache`](crate::executor::docker::cache) — the retention
//! janitor captures only a clone plus a pressure receiver.
//!
//! **Durability** is WAL with `synchronous=NORMAL` (§8.4): an agent-process
//! crash loses at most the final uncommitted flush batch; an OS crash may
//! additionally roll back transactions since the last checkpoint. Those are the
//! *only* sanctioned telemetry losses — steady-state loss is a defect (§8.3),
//! which is why a failed flush increments an error-level counter rather than
//! disappearing.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::time::{Duration as CoreDuration, Timestamp};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, SqliteConnection, SqlitePool};
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use super::{LogChunk, LogStream, MetricSample, AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL};
use crate::pressure::DiskPressure;

/// The segment schema (docker-executor.md §8.4). Embedded at compile time from
/// `migrations/telemetry`, run on each freshly-created segment.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/telemetry");

/// How often the retention janitor sweeps absent a pressure transition (§8.4).
/// A fixed 60s, mirroring the image cache's `JANITOR_INTERVAL`: the cadence only
/// gates coarse, self-correcting deletion. A pressure transition wakes it at
/// once (see [`spawn_retention_janitor`]).
const JANITOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a new segment's SQLite connection waits on a locked database before
/// giving up (§8.4). Generous: the store's writes are batched and infrequent.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The ceiling for [`log_page`](FilesystemSink::log_page)'s `substr` payload
/// projection length. SQLite stores no blob longer than its compile-time
/// `SQLITE_MAX_LENGTH` (default 1e9 bytes), so a `substr(bytes, 1, N)` with any
/// `N` at or above this always returns the whole payload — and capping here
/// keeps the bound integer well clear of the large-value range where a
/// `substr` length near `i64::MAX` degenerates to an empty result.
const MAX_PAYLOAD_PROJECTION: u64 = 1_000_000_000;

// ---- construction (docker-executor.md §8.4) -----------------------------

/// Everything [`FilesystemSink::new`] needs. The retention/pressure knobs mirror
/// [`CacheOptions`](crate::executor::docker::cache); durations are the workspace
/// [`CoreDuration`] so the roll and sweep policy compare against `Timestamp`
/// arithmetic directly.
pub struct FilesystemSinkOptions {
    /// Segment root; `<data_dir>/telemetry` in production.
    pub root: PathBuf,
    /// Size bound a segment rolls at (default 256 MiB).
    pub segment_max: ByteSize,
    /// Age bound a segment rolls at (default 6h).
    pub segment_max_age: CoreDuration,
    /// How long after an attempt ends its segments are kept (default 60m).
    pub retention: CoreDuration,
    /// Live cap: the maximum age of a *running* attempt's closed segments
    /// (default 24h), measured from the successor's start.
    pub live_retention: CoreDuration,
    /// `statvfs` paths feeding the `High`/`Critical` pressure sweep target, the
    /// same paths the pressure monitor watches (§9). Empty = no local reading,
    /// which makes a pressure sweep drop every ended segment (disk safety wins).
    pub pressure_paths: Vec<PathBuf>,
    /// The high-water mark a pressure sweep deletes below (§9).
    pub high_pct: u8,
}

impl FilesystemSinkOptions {
    /// The documented defaults over `root` (docker-executor.md §8.4): 256 MiB /
    /// 6h segment roll, 60m post-end retention, 24h live cap, no pressure paths,
    /// an 85% high-water mark.
    pub fn new(root: PathBuf) -> FilesystemSinkOptions {
        FilesystemSinkOptions {
            root,
            segment_max: ByteSize::from_mib(256),
            segment_max_age: CoreDuration::from_hours(6),
            retention: CoreDuration::from_mins(60),
            live_retention: CoreDuration::from_hours(24),
            pressure_paths: Vec::new(),
            high_pct: 85,
        }
    }
}

/// One attempt's currently-open segment: its connection pool
/// (`max_connections = 1`), the file path, the creation [`Timestamp`] the roll
/// policy ages against, and a cached on-disk size estimate refreshed after each
/// flush.
struct OpenSegment {
    pool: SqlitePool,
    path: PathBuf,
    start: Timestamp,
    size: u64,
}

/// The shared guts behind every [`FilesystemSink`] clone.
struct Inner {
    root: PathBuf,
    segment_max: ByteSize,
    segment_max_age: CoreDuration,
    retention: CoreDuration,
    live_retention: CoreDuration,
    pressure_paths: Vec<PathBuf>,
    high_pct: u8,
    /// The open segment per attempt. A `tokio::sync::Mutex` because the write
    /// path holds it across the flush transaction (create/roll + insert); a
    /// single drain task per sink instance (§8.3) means no write contention, and
    /// the sweep only borrows it briefly to learn which files not to unlink.
    open: AsyncMutex<HashMap<(JobId, AttemptId), OpenSegment>>,
    /// Per-attempt, per-segment **data-time bounds** for `log_chunks`, keyed by
    /// segment start (ADR 0034 review — per-request work must not scale with
    /// the number of retained segments). Closed segments are immutable, so
    /// their bounds are probed **once per segment lifetime** (index-only
    /// `MIN`/`MAX` per stream) and cached; the open segment's entry is extended
    /// by the writer after every committed flush. [`log_page`] plans against
    /// these bounds and opens only the segments that can contribute to the
    /// page. The cache is derived state: a retention sweep that deletes
    /// anything clears it wholesale, and the next read rebuilds it from the
    /// survivors. A `std` mutex — never held across an await.
    ///
    /// [`log_page`]: FilesystemSink::log_page
    log_bounds: StdMutex<HashMap<(JobId, AttemptId), BTreeMap<Timestamp, StreamBounds>>>,
    /// Warn-once latch for the write-error path (pressure.rs style): the first
    /// failure of a streak logs at error, subsequent ones only bump the counter,
    /// and a success resets it — so a wedged disk is metered, not a log flood.
    write_error_logged: AtomicBool,
    /// Test-only instrumentation: how many `log_chunks` payload rows `log_page`
    /// has decoded on this sink — the boundary/beyond page reads that actually
    /// project `bytes`; the `MIN`/`MAX` bounds probes and the exact-µs `COUNT`
    /// probes decode no payload and are excluded. Instance-scoped so a parallel
    /// test cannot pollute it; the bounded-read and adversarial tests assert a
    /// tiny page over a many-thousand-row multi-segment attempt decodes only a
    /// handful, no matter what the cursor `skip` says — proof the caps bound the
    /// input scan, not merely the returned page.
    #[cfg(test)]
    rows_decoded: std::sync::atomic::AtomicU64,
    /// Test-only instrumentation companion to [`rows_decoded`](Inner::rows_decoded):
    /// the total payload BYTES `log_page` has materialized on this sink — the sum
    /// of every projected `substr(bytes, 1, ?)` prefix it read. Because the
    /// projection is bounded to the page byte budget, an oversized stored row
    /// never materializes beyond that budget; the oversized-row test asserts it.
    #[cfg(test)]
    bytes_materialized: std::sync::atomic::AtomicU64,
    /// Test-only instrumentation: how many segment files [`log_page`] has
    /// opened on this sink (bounds probes and page pulls alike). The
    /// segment-scaling tests assert a warm-cache page over many retained
    /// segments opens only the one or two that can contribute — proof that
    /// per-request IO is bounded by the page, not by retention.
    ///
    /// [`log_page`]: FilesystemSink::log_page
    #[cfg(test)]
    segments_opened: std::sync::atomic::AtomicU64,
}

/// The filesystem sink (docker-executor.md §8.4). A cheap `Clone` handle; clones
/// share one `Inner`.
#[derive(Clone)]
pub struct FilesystemSink {
    inner: Arc<Inner>,
}

impl FilesystemSink {
    /// Build the sink, creating `root` (docker-executor.md §8.4). No segment is
    /// opened here: the first append for an attempt creates its first segment,
    /// and after a restart that is always a *fresh* segment (nothing reopens an
    /// old open segment for writing — recovery appends only to the current one).
    pub async fn new(opts: FilesystemSinkOptions) -> anyhow::Result<FilesystemSink> {
        std::fs::create_dir_all(&opts.root).map_err(|err| {
            anyhow::anyhow!("creating telemetry root {}: {err}", opts.root.display())
        })?;
        Ok(FilesystemSink {
            inner: Arc::new(Inner {
                root: opts.root,
                segment_max: opts.segment_max,
                segment_max_age: opts.segment_max_age,
                retention: opts.retention,
                live_retention: opts.live_retention,
                pressure_paths: opts.pressure_paths,
                high_pct: opts.high_pct,
                open: AsyncMutex::new(HashMap::new()),
                log_bounds: StdMutex::new(HashMap::new()),
                write_error_logged: AtomicBool::new(false),
                #[cfg(test)]
                rows_decoded: std::sync::atomic::AtomicU64::new(0),
                #[cfg(test)]
                bytes_materialized: std::sync::atomic::AtomicU64::new(0),
                #[cfg(test)]
                segments_opened: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }
}

// ---- write path (docker-executor.md §8.4) -------------------------------

/// The write path's own error, kept internal because `append` is infallible at
/// the boundary (§8.3): a failed flush is logged, counted, and the batch
/// dropped — the error never escapes.
#[derive(Debug, thiserror::Error)]
enum WriteError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl FilesystemSink {
    /// The metrics write seam: groups the batch by attempt and flushes each group
    /// in one transaction, driving age-based rolls off the injected `now` (§8.4).
    /// Tests advance `now` synthetically instead of sleeping.
    pub(crate) async fn append_metrics_at(&self, batch: &[MetricSample], now: Timestamp) {
        if batch.is_empty() {
            return;
        }
        let groups = group_by_attempt(batch, |sample| (sample.job, sample.attempt));
        let mut map = self.inner.open.lock().await;
        for ((job, attempt), samples) in groups {
            match self
                .flush_metrics(&mut map, job, attempt, now, &samples)
                .await
            {
                Ok(()) => self
                    .inner
                    .write_error_logged
                    .store(false, Ordering::Relaxed),
                Err(err) => self.record_write_error(job, attempt, "metrics", &err),
            }
        }
    }

    /// The logs write seam (mirror of [`append_metrics_at`](Self::append_metrics_at)).
    pub(crate) async fn append_logs_at(&self, batch: &[LogChunk], now: Timestamp) {
        if batch.is_empty() {
            return;
        }
        let groups = group_by_attempt(batch, |chunk| (chunk.job, chunk.attempt));
        let mut map = self.inner.open.lock().await;
        for ((job, attempt), chunks) in groups {
            match self.flush_logs(&mut map, job, attempt, now, &chunks).await {
                Ok(()) => self
                    .inner
                    .write_error_logged
                    .store(false, Ordering::Relaxed),
                Err(err) => self.record_write_error(job, attempt, "logs", &err),
            }
        }
    }

    /// Flush one attempt's metric samples in a single transaction (§8.4's "one
    /// transaction per flush batch"), rolling the segment first if the pre-batch
    /// size/age check says so.
    async fn flush_metrics(
        &self,
        map: &mut HashMap<(JobId, AttemptId), OpenSegment>,
        job: JobId,
        attempt: AttemptId,
        now: Timestamp,
        samples: &[&MetricSample],
    ) -> Result<(), WriteError> {
        self.ensure_segment(map, job, attempt, now).await?;
        let key = (job, attempt);
        let pool = map.get(&key).expect("segment just ensured").pool.clone();
        let mut tx = pool.begin().await?;
        for sample in samples {
            let at = sample.at.as_micros();
            let allocation = sample.allocation.to_string();
            let cpu_usage = sample.cpu_usage_total.as_micros();
            let cpu_throttled = sample.cpu_throttled_total.as_micros();
            let memory_used = u64_to_i64_clamped(sample.memory_used_bytes, "memory_used_bytes");
            let memory_peak = u64_to_i64_clamped(sample.memory_peak_bytes, "memory_peak_bytes");
            let disk_writable =
                u64_to_i64_clamped(sample.disk_writable_bytes, "disk_writable_bytes");
            let disk_image = u64_to_i64_clamped(sample.disk_image_bytes, "disk_image_bytes");
            let net_rx = u64_to_i64_clamped(sample.net_rx_bytes_total, "net_rx_bytes_total");
            let net_tx = u64_to_i64_clamped(sample.net_tx_bytes_total, "net_tx_bytes_total");
            let blkio_read =
                u64_to_i64_clamped(sample.blkio_read_bytes_total, "blkio_read_bytes_total");
            let blkio_write =
                u64_to_i64_clamped(sample.blkio_write_bytes_total, "blkio_write_bytes_total");
            sqlx::query!(
                "INSERT INTO metrics (at, allocation_id, cpu_usage_total_us, \
                 cpu_throttled_total_us, memory_used_bytes, memory_peak_bytes, \
                 disk_writable_bytes, disk_image_bytes, net_rx_bytes_total, \
                 net_tx_bytes_total, blkio_read_bytes_total, blkio_write_bytes_total) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                at,
                allocation,
                cpu_usage,
                cpu_throttled,
                memory_used,
                memory_peak,
                disk_writable,
                disk_image,
                net_rx,
                net_tx,
                blkio_read,
                blkio_write,
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        if let Some(segment) = map.get_mut(&key) {
            segment.size = segment_size_on_disk(&segment.path);
        }
        Ok(())
    }

    /// Flush one attempt's log chunks in a single transaction (§8.4).
    async fn flush_logs(
        &self,
        map: &mut HashMap<(JobId, AttemptId), OpenSegment>,
        job: JobId,
        attempt: AttemptId,
        now: Timestamp,
        chunks: &[&LogChunk],
    ) -> Result<(), WriteError> {
        self.ensure_segment(map, job, attempt, now).await?;
        let key = (job, attempt);
        let pool = map.get(&key).expect("segment just ensured").pool.clone();
        let mut tx = pool.begin().await?;
        for chunk in chunks {
            let at = chunk.at.as_micros();
            let allocation = chunk.allocation.to_string();
            let stream = chunk.stream.to_i64();
            let bytes = chunk.bytes.as_ref();
            sqlx::query!(
                "INSERT INTO log_chunks (at, allocation_id, stream, bytes) VALUES (?, ?, ?, ?)",
                at,
                allocation,
                stream,
                bytes,
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        // Extend the open segment's cached data-time bounds only *after* the
        // commit: bounds must never claim rows that are not durable. A reader
        // interleaving between commit and this note merely sees bounds a
        // moment stale — the same read skew as querying an instant earlier.
        {
            let start = map.get(&key).expect("segment just ensured").start;
            let mut cache = self.inner.log_bounds.lock().expect("log_bounds poisoned");
            let bounds = cache.entry(key).or_default().entry(start).or_default();
            for chunk in chunks {
                bounds.note(chunk.stream, chunk.at.as_micros());
            }
        }
        if let Some(segment) = map.get_mut(&key) {
            segment.size = segment_size_on_disk(&segment.path);
        }
        Ok(())
    }

    /// Ensure `(job, attempt)` has an open segment fit to receive `now`'s batch,
    /// rolling first if the pre-batch check trips (§8.4 — never mid-batch). A
    /// roll cleanly closes the old pool (checkpointing WAL, removing `-wal`) then
    /// opens a fresh `seg-<start>.db`.
    async fn ensure_segment(
        &self,
        map: &mut HashMap<(JobId, AttemptId), OpenSegment>,
        job: JobId,
        attempt: AttemptId,
        now: Timestamp,
    ) -> Result<(), WriteError> {
        let key = (job, attempt);
        let roll = match map.get(&key) {
            Some(segment) => should_roll(
                segment.start,
                segment.size,
                now,
                self.inner.segment_max,
                self.inner.segment_max_age,
            ),
            None => true,
        };
        if roll {
            if let Some(old) = map.remove(&key) {
                old.pool.close().await;
            }
            let segment = self.create_segment(job, attempt, now).await?;
            map.insert(key, segment);
        }
        Ok(())
    }

    /// Create a fresh segment for `(job, attempt)` (§8.4): make the attempt
    /// directory, pick a start strictly after any existing segment's (so starts
    /// stay strictly increasing across rolls *and* restarts), open a WAL pool,
    /// run the schema migration, and write the `meta` rows.
    async fn create_segment(
        &self,
        job: JobId,
        attempt: AttemptId,
        now: Timestamp,
    ) -> Result<OpenSegment, WriteError> {
        let dir = self
            .inner
            .root
            .join(job.to_string())
            .join(attempt.to_string());
        std::fs::create_dir_all(&dir)?;
        let start = next_segment_start(&dir, now);
        let path = dir.join(segment_filename(start));
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        for (key, value) in [
            ("format_version", "1".to_string()),
            ("job_id", job.to_string()),
            ("attempt_id", attempt.to_string()),
            ("start_us", start.as_micros().to_string()),
        ] {
            sqlx::query!("INSERT INTO meta (key, value) VALUES (?, ?)", key, value)
                .execute(&pool)
                .await?;
        }
        let size = segment_size_on_disk(&path);
        Ok(OpenSegment {
            pool,
            path,
            start,
            size,
        })
    }

    /// Account for a failed flush (§8.3): bump the error-level counter and, on
    /// the first failure of a streak, log at error. The batch is dropped after
    /// this — steady-state loss is a defect signal, never silent.
    fn record_write_error(&self, job: JobId, attempt: AttemptId, kind: &str, err: &WriteError) {
        metrics::counter!(AGENT_TELEMETRY_FS_WRITE_ERRORS_TOTAL).increment(1);
        if !self.inner.write_error_logged.swap(true, Ordering::Relaxed) {
            tracing::error!(
                %job,
                %attempt,
                kind,
                error = %err,
                "telemetry filesystem sink write failed; dropping batch (§8.3)"
            );
        }
    }
}

/// The metrics stream lands through the [`MetricsSink`](super::MetricsSink)
/// trait; `append` is infallible at the boundary (§8.3).
impl super::MetricsSink for FilesystemSink {
    async fn append(&self, batch: &[MetricSample]) {
        self.append_metrics_at(batch, Timestamp::now()).await
    }
}

/// The logs stream lands through the [`LogSink`](super::LogSink) trait.
impl super::LogSink for FilesystemSink {
    async fn append(&self, batch: &[LogChunk]) {
        self.append_logs_at(batch, Timestamp::now()).await
    }
}

// ---- attempt end + retention (docker-executor.md §8.4, §9) ---------------

impl FilesystemSink {
    /// Mark an attempt ended (docker-executor.md §8.4): close its open segment
    /// (if any) so no writer holds the file, then durably write the `ended`
    /// marker (decimal µs). Idempotent — the first marker wins.
    ///
    /// An `Err` means the marker did **not** persist (e.g. `ENOSPC`) and the
    /// attempt still reads as live — which retention then protects rather than
    /// reclaims — so the caller must retry until this succeeds. The segment
    /// close is idempotent, so retrying is safe. The executor wiring that calls
    /// this lands in a later slice.
    pub async fn attempt_ended(
        &self,
        job: &JobId,
        attempt: &AttemptId,
        at: Timestamp,
    ) -> std::io::Result<()> {
        {
            let mut map = self.inner.open.lock().await;
            if let Some(segment) = map.remove(&(*job, *attempt)) {
                segment.pool.close().await;
            }
        }
        let dir = self
            .inner
            .root
            .join(job.to_string())
            .join(attempt.to_string());
        let marker = dir.join("ended");
        if marker.exists() {
            return Ok(()); // first marker wins (idempotent)
        }
        write_marker(&dir, &marker, at)
    }

    /// Run one retention sweep (docker-executor.md §8.4). Under `Ok` pressure
    /// only the normal-retention and live-cap deletions run; under `High`/
    /// `Critical` ended attempts' segments additionally go early, oldest-ended-
    /// first, until the watched filesystems read back below the high-water mark.
    /// Returns the number of segments deleted. The janitor calls this with
    /// `Timestamp::now()` and the current pressure.
    pub async fn sweep(&self, now: Timestamp, pressure: DiskPressure) -> usize {
        self.sweep_with(now, pressure, || self.bytes_to_free())
            .await
    }

    /// The sweep driver behind [`sweep`](Self::sweep), with the pressure
    /// stop-condition injected as a closure so tests drive the pressure tier
    /// with a synthetic bytes-to-free (the way `cache.rs` tests eviction purely).
    /// The pure [`sweep_plan`] selects and orders; this executes the file
    /// unlinks, re-sampling before each conditional deletion.
    async fn sweep_with(
        &self,
        now: Timestamp,
        pressure: DiskPressure,
        mut bytes_to_free: impl FnMut() -> Option<ByteSize>,
    ) -> usize {
        // Snapshot which files the writer currently holds open so the sweep
        // never unlinks one from under it (close-check via the map, not just
        // filenames — §8.4).
        let open_paths: HashSet<PathBuf> = {
            let map = self.inner.open.lock().await;
            map.values().map(|segment| segment.path.clone()).collect()
        };
        let views = scan_attempts(&self.inner.root, &open_paths);
        let plan = sweep_plan(
            &views,
            now,
            self.inner.retention,
            self.inner.live_retention,
            pressure,
        );

        let mut deleted = 0;
        for path in &plan.mandatory {
            if delete_segment_files(path) {
                deleted += 1;
            }
        }
        if pressure >= DiskPressure::High {
            for path in &plan.conditional {
                // Resample before each conditional deletion and stop once
                // genuinely below the mark. `None` (no local reading) keeps
                // going — disk safety wins, every ended segment goes.
                if let Some(over) = bytes_to_free() {
                    if over.is_zero() {
                        break;
                    }
                }
                if delete_segment_files(path) {
                    deleted += 1;
                }
            }
        }
        cleanup_empty_dirs(&self.inner.root);
        if deleted > 0 {
            // Coarse invalidation on any deletion: the bounds cache is derived
            // state, and the next read of an affected attempt rebuilds it from
            // the surviving segments (one probe per survivor). Sweeps that
            // delete are rare (60s tick, only when retention/pressure trips),
            // so wholesale clearing beats threading per-path attempt keys
            // through the sweep plan.
            self.inner
                .log_bounds
                .lock()
                .expect("log_bounds poisoned")
                .clear();
        }
        deleted
    }

    /// The pressure sweep's byte target: the max over [`pressure_paths`] of the
    /// bytes each filesystem must free to fall below `high_pct`
    /// ([`crate::pressure::bytes_over_pct`]). `None` = no local reading (empty
    /// paths or all-failed `statvfs`), read as "drop every ended segment".
    ///
    /// [`pressure_paths`]: FilesystemSinkOptions::pressure_paths
    fn bytes_to_free(&self) -> Option<ByteSize> {
        let mut target: Option<ByteSize> = None;
        for path in &self.inner.pressure_paths {
            if let Some(over) = crate::pressure::bytes_over_pct(path, self.inner.high_pct) {
                target = Some(target.map_or(over, |current| current.max(over)));
            }
        }
        target
    }
}

/// Spawn the retention janitor (docker-executor.md §8.4), returning its handle.
/// Captures only a [`FilesystemSink`] clone plus a pressure receiver — the same
/// no-cycle discipline as the image cache's `spawn_janitor` — so dropping the
/// handle is what stops it. A [`DiskPressure`] transition wakes it immediately;
/// otherwise it sweeps every [`JANITOR_INTERVAL`].
pub fn spawn_retention_janitor(
    sink: FilesystemSink,
    mut pressure: watch::Receiver<DiskPressure>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(JANITOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                changed = pressure.changed() => {
                    // The sender lives as long as the executor; an `Err` means it
                    // was dropped, so there is nothing left to serve.
                    if changed.is_err() {
                        return;
                    }
                }
            }
            let level = *pressure.borrow();
            sink.sweep(Timestamp::now(), level).await;
        }
    })
}

// ---- read API (docker-executor.md §8.4 read path) ------------------------

/// One attempt's telemetry presence, as [`FilesystemSink::list_attempts`]
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptTelemetry {
    /// The owning job.
    pub job: JobId,
    /// The attempt.
    pub attempt: AttemptId,
    /// When the attempt was marked ended, if a marker is present (`None` while
    /// live).
    pub ended_at: Option<Timestamp>,
    /// How many segment files the attempt currently has on disk.
    pub segments: usize,
}

/// A log read's shape (docker-executor.md §8.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogQuery {
    /// All chunks with `at` in the inclusive range.
    Range {
        /// Inclusive lower bound.
        from: Timestamp,
        /// Inclusive upper bound.
        to: Timestamp,
    },
    /// The newest `n` chunks, returned in ascending `(at, insertion order)`.
    Tail {
        /// How many chunks to return at most.
        n: usize,
    },
}

/// A log chunk as stored (the attempt/job identity is the caller's; §8.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLogChunk {
    /// Docker's per-line timestamp.
    pub at: Timestamp,
    /// Which output stream produced the chunk.
    pub stream: LogStream,
    /// The raw payload.
    pub bytes: bytes::Bytes,
    /// True when [`log_page`](FilesystemSink::log_page) cut this chunk's payload
    /// down to the remaining `max_bytes` budget because the chunk alone exceeded
    /// it (ADR 0034). The chunk still counts as fully consumed for resume
    /// arithmetic — the walk advances past it whole — so the truncated bytes are
    /// dropped, never re-served. Always `false` for chunks read through the
    /// unpaged [`log_chunks`](FilesystemSink::log_chunks) API.
    pub truncated: bool,
}

/// Direction of a [`log_page`](FilesystemSink::log_page) walk. `Descending`
/// (newest-first) is the exact reverse of the canonical `(at, insertion)`
/// ascending order, so a chunk's position in a descending page is fully
/// determined even when several chunks share a microsecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogOrder {
    /// Oldest chunk first.
    Ascending,
    /// Newest chunk first.
    Descending,
}

/// An exclusive resume position within one attempt's chunks (ADR 0034). Because
/// the store orders by `(at, insertion)`, a bare `at` cannot address a position
/// when several chunks share a microsecond: `skip` is the number of chunks
/// already consumed at exactly `at` *in the walk direction*, and the walk
/// resumes strictly after them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeAt {
    /// The microsecond the previous page stopped at.
    pub at: Timestamp,
    /// How many chunks at exactly `at` the previous page already returned.
    pub skip: u64,
}

/// A paged log query for the `FetchLogs` RPC (ADR 0034). A bounded, directional
/// walk over one attempt's chunks with an optional half-open time window, an
/// optional stream filter, an optional exclusive resume position, and hard caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPageQuery {
    /// Restrict to one stream; `None` returns both.
    pub stream: Option<LogStream>,
    /// Inclusive lower bound of the half-open `[from, until)` window; `None`
    /// opens the window on the old side.
    pub from: Option<Timestamp>,
    /// Exclusive upper bound of the half-open `[from, until)` window; `None`
    /// opens the window on the new side.
    pub until: Option<Timestamp>,
    /// Walk direction.
    pub order: LogOrder,
    /// Exclusive resume position; `None` starts from the window edge in the
    /// walk direction.
    pub resume: Option<ResumeAt>,
    /// Never return more than this many chunks in the page.
    pub max_chunks: usize,
    /// Stop before the page's cumulative payload bytes would exceed this — but
    /// always return at least the first chunk, so a single oversize chunk still
    /// makes progress.
    pub max_bytes: u64,
}

/// One page of a [`log_page`](FilesystemSink::log_page) walk (ADR 0034).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    /// The chunks in the requested direction, after range, resume, and caps.
    pub chunks: Vec<StoredLogChunk>,
    /// True when the walk reached the end of the requested window within this
    /// page; false when a cap (`max_chunks`/`max_bytes`) cut it short and a
    /// further page exists.
    pub exhausted: bool,
    /// The oldest retained `at` for this attempt over the stream-filtered data,
    /// independent of the query window — a requested `from` earlier than this
    /// means older chunks existed and were pruned (the API's `truncated`
    /// verdict). `None` when the attempt has no matching chunks.
    pub earliest_at: Option<Timestamp>,
    /// The newest retained `at`, the mirror of [`earliest_at`](LogPage::earliest_at).
    pub latest_at: Option<Timestamp>,
}

/// The telemetry read API's error (docker-executor.md §8.4). Typed rather than
/// `anyhow` so a caller can distinguish an unknown attempt from a storage
/// fault; `anyhow` stays at the wiring layer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No telemetry directory exists for the requested attempt.
    #[error("no telemetry for attempt {job}/{attempt}")]
    UnknownAttempt {
        /// The requested job.
        job: JobId,
        /// The requested attempt.
        attempt: AttemptId,
    },
    /// A filesystem error walking the segment set.
    #[error("telemetry store I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A SQLite error reading a segment.
    #[error("telemetry store SQL error: {0}")]
    Sql(#[from] sqlx::Error),
}

impl FilesystemSink {
    /// List attempts with telemetry on disk (docker-executor.md §8.4), optionally
    /// scoped to one job. Directory names that do not parse as ids are skipped
    /// and warned about; a missing root is an empty list.
    pub async fn list_attempts(
        &self,
        job: Option<&JobId>,
    ) -> Result<Vec<AttemptTelemetry>, StoreError> {
        let mut out = Vec::new();
        let job_dirs: Vec<(JobId, PathBuf)> = match job {
            Some(job) => vec![(*job, self.inner.root.join(job.to_string()))],
            None => list_id_dirs::<JobId>(&self.inner.root),
        };
        for (job, job_dir) in job_dirs {
            for (attempt, attempt_dir) in list_id_dirs::<AttemptId>(&job_dir) {
                out.push(AttemptTelemetry {
                    job,
                    attempt,
                    ended_at: read_ended_marker(&attempt_dir),
                    segments: list_segments(&attempt_dir).len(),
                });
            }
        }
        Ok(out)
    }

    /// Read an attempt's log chunks (docker-executor.md §8.4). Both shapes probe
    /// **every** segment through the `at` index and stable-merge: a segment's
    /// filename records its *creation* time, but its rows carry *event* time,
    /// which is always a little older than the flush that wrote it — and §8.2
    /// restart replay appends arbitrarily older chunks to the freshly created
    /// open segment — so pruning segments by filename would silently hide stored
    /// rows. Retention bounds the per-attempt segment count, keeping the probes
    /// cheap. Ordering is `(at, insertion order)` — rows are gathered in segment
    /// then rowid order and stable-sorted by `at`, so equal-`at` occurrences
    /// keep their write order — and duplicates are preserved: identical user
    /// writes are distinct (§8.2).
    pub async fn log_chunks(
        &self,
        job: &JobId,
        attempt: &AttemptId,
        stream: Option<LogStream>,
        query: LogQuery,
    ) -> Result<Vec<StoredLogChunk>, StoreError> {
        let dir = self.attempt_dir_existing(job, attempt)?;
        let segments = list_segments(&dir);
        let stream_filter = stream.map(LogStream::to_i64);
        match query {
            LogQuery::Range { from, to } => {
                let mut out = Vec::new();
                for (_, path) in &segments {
                    let Some(mut conn) = self.open_read(path).await? else {
                        continue;
                    };
                    let rows = log_range_rows(&mut conn, from, to, stream_filter).await?;
                    for (at, raw_stream, bytes) in rows {
                        if let Some(chunk) = stored_chunk(at, raw_stream, bytes) {
                            out.push(chunk);
                        }
                    }
                }
                out.sort_by_key(|chunk| chunk.at); // stable: ties keep write order
                Ok(out)
            }
            LogQuery::Tail { n } => {
                // Each segment contributes its own newest `n` — a superset of the
                // global newest `n` — then the merged, `(at, insertion order)`
                // sorted set keeps its last `n`.
                let mut collected: Vec<StoredLogChunk> = Vec::new();
                for (_, path) in &segments {
                    let Some(mut conn) = self.open_read(path).await? else {
                        continue;
                    };
                    let mut rows = log_tail_rows(&mut conn, stream_filter, n as i64).await?;
                    rows.reverse(); // (at DESC, id DESC) → this segment's write order
                    for (at, raw_stream, bytes) in rows {
                        if let Some(chunk) = stored_chunk(at, raw_stream, bytes) {
                            collected.push(chunk);
                        }
                    }
                }
                collected.sort_by_key(|chunk| chunk.at); // stable: ties keep write order
                if collected.len() > n {
                    collected.drain(..collected.len() - n);
                }
                Ok(collected)
            }
        }
    }

    /// Read one bounded page of an attempt's log chunks for the `FetchLogs` RPC
    /// (ADR 0034). A directional walk over the half-open `[from, until)` window
    /// with an optional stream filter, an optional exclusive resume position,
    /// and `max_chunks`/`max_bytes` caps.
    ///
    /// Like [`log_chunks`](Self::log_chunks) this probes **every** segment — a
    /// row's `at` can be older than its segment's filename start (§8.2 replay),
    /// so filename pruning would silently hide rows — but the reads are
    /// **bounded**, and bounded *independently of the untrusted cursor*: each
    /// segment's page slice is an indexed `BETWEEN` range scan **streamed** with a
    /// `LIMIT` of `max_chunks + 1` as the row backstop and a byte cutoff that
    /// stops the cursor once its projected bytes reach `max_bytes` (plus one
    /// look-ahead), the exclusive resume `skip` is applied through index-only
    /// `COUNT`/`OFFSET` at the boundary microsecond (decoding no payload for the
    /// skipped rows), each projected payload is capped to the byte budget via
    /// `substr`, and the whole-attempt bounds come from cheap `MIN`/`MAX`
    /// aggregates. A small page over a giant attempt therefore decodes only a
    /// handful of rows per segment and materializes only ~`max_bytes` per segment
    /// — never the full store, never `max_chunks × max_bytes` per segment, and
    /// never more work for a crafted or accumulated `skip` — the input scan and
    /// payload materialization are capped, not just the output page.
    ///
    /// `earliest_at`/`latest_at` span the whole stream-filtered attempt,
    /// **independent** of the window/resume, so the caller can detect head
    /// truncation. [`StoreError::UnknownAttempt`] when no directory exists for
    /// the attempt (its telemetry has fallen out of retention, or none was
    /// written) — the RPC maps that to its `UnknownAttempt` arm.
    pub async fn log_page(
        &self,
        job: &JobId,
        attempt: &AttemptId,
        query: &LogPageQuery,
    ) -> Result<LogPage, StoreError> {
        let dir = self.attempt_dir_existing(job, attempt)?;
        let segments = list_segments(&dir);
        let stream_filter = query.stream.map(LogStream::to_i64);

        // The per-segment data-time bounds cache (ADR 0034 review): closed
        // segments are probed once per lifetime, the open segment's entry is
        // writer-maintained, and everything below plans against these bounds so
        // per-request IO touches only the segments that can contribute to the
        // page — never all retained segments. §8.2 replay means a later segment
        // can hold older rows; the bounds are true data-time extents, so that
        // overlap is captured exactly.
        let bounds = self.ensure_log_bounds(job, attempt, &segments).await?;

        // Whole-attempt bounds for the head-truncation verdict, independent of
        // the query window/resume — straight from the cache, no per-request
        // segment probes.
        let mut earliest_at: Option<Timestamp> = None;
        let mut latest_at: Option<Timestamp> = None;
        for seg_bounds in bounds.values() {
            let Some((lo, hi)) = seg_bounds.for_filter(stream_filter) else {
                continue;
            };
            if let Some(at) = Timestamp::from_micros(lo) {
                earliest_at = Some(earliest_at.map_or(at, |cur| cur.min(at)));
            }
            if let Some(at) = Timestamp::from_micros(hi) {
                latest_at = Some(latest_at.map_or(at, |cur| cur.max(at)));
            }
        }

        // The half-open `[from, until)` window as an inclusive `[from_us,
        // until_incl_us]` micro range: `until` is exclusive and `at` is
        // µs-quantised, so `until - 1µs` closes it.
        let from_us = query.from.map_or(i64::MIN, Timestamp::as_micros);
        let until_incl_us = query
            .until
            .map_or(i64::MAX, |until| until.as_micros().saturating_sub(1));

        // The resume `skip` is unsigned and *client-controlled* (an opaque HTTP
        // cursor value that also legitimately accumulates across pages when many
        // chunks share one microsecond). It must never inflate the decode work:
        // a naive `LIMIT skip + max_chunks` would re-enable full-store payload
        // materialization for a crafted `skip` near `u64::MAX`. So the walk is
        // split into two disjoint reads whose *decoded* size is independent of
        // `skip`:
        //
        //   * the **boundary** run — the rows at exactly `resume.at` — is skipped
        //     with an index-only `COUNT` per segment (decoding nothing) to turn
        //     the cross-segment `skip` into a per-segment `OFFSET`, then only the
        //     surviving `max_chunks + 1` rows are projected;
        //   * the **beyond** run — the rows strictly past `resume.at` in the walk
        //     direction — needs no skip and reads a plain `LIMIT max_chunks + 1`.
        //
        // A `skip` at or beyond the total boundary count costs only the `COUNT`s.
        let want = (query.max_chunks as u64)
            .saturating_add(1)
            .min(i64::MAX as u64) as i64;

        // The payload projection bound: never materialize more than the page byte
        // budget (+1, so the truncated verdict is exact) from any single row, so
        // an oversized stored row cannot cause large input materialization even
        // though the response is truncated to the budget. `length(bytes)` still
        // gives the true size for the cumulative-budget arithmetic.
        let proj = query
            .max_bytes
            .saturating_add(1)
            .min(MAX_PAYLOAD_PROJECTION) as i64;

        // Which exact microsecond, if any, carries an in-window boundary run. A
        // `resume.at` outside `[from_us, until_incl_us]` has no boundary rows in
        // the window, so `skip` is inert and the whole window is a beyond read.
        let boundary_at = query.resume.and_then(|r| {
            let b = r.at.as_micros();
            (from_us <= b && b <= until_incl_us).then_some(b)
        });
        let skip = query.resume.map_or(0, |r| r.skip);

        // The beyond range excludes the boundary microsecond in the walk
        // direction. With no in-window boundary this is the whole window; the
        // `saturating_add/sub(1)` on `resume.at` also correctly empties the range
        // when `resume.at` sits on the already-consumed side of the window.
        let (beyond_lo, beyond_hi) = match query.order {
            LogOrder::Ascending => {
                let lo = query
                    .resume
                    .map_or(from_us, |r| from_us.max(r.at.as_micros().saturating_add(1)));
                (lo, until_incl_us)
            }
            LogOrder::Descending => {
                let hi = query.resume.map_or(until_incl_us, |r| {
                    until_incl_us.min(r.at.as_micros().saturating_sub(1))
                });
                (from_us, hi)
            }
        };

        // The boundary run, already in walk order: segments are visited in
        // cross-segment insertion order (ascending seg order, reversed for
        // descending), each `COUNT`ed at exactly `resume.at`, and `skip` is spent
        // against those counts to land on a per-segment `OFFSET`. Only once a
        // segment holds surviving rows does a projected `LIMIT ? OFFSET ?` read
        // decode anything — at most `want` rows total across the whole boundary.
        let mut boundary_walk: Vec<PageRow> = Vec::new();
        if let Some(b) = boundary_at {
            let mut remaining_skip = skip;
            let mut need = want;
            // Only segments whose bounds span the boundary microsecond can hold
            // boundary rows; the rest would `COUNT` to zero, so they are skipped
            // without being opened. Bounds are exact for committed data, so the
            // cross-segment `skip` arithmetic is unchanged.
            let mut walk_segments: Vec<&PathBuf> = segments
                .iter()
                .filter(|(start, _)| {
                    bounds
                        .get(start)
                        .and_then(|sb| sb.for_filter(stream_filter))
                        .is_some_and(|(lo, hi)| lo <= b && b <= hi)
                })
                .map(|(_, p)| p)
                .collect();
            if matches!(query.order, LogOrder::Descending) {
                walk_segments.reverse();
            }
            for path in walk_segments {
                if need <= 0 {
                    break;
                }
                let Some(mut conn) = self.open_read_counted(path).await? else {
                    continue;
                };
                let count = log_boundary_count(&mut conn, b, stream_filter).await? as u64;
                if remaining_skip >= count {
                    remaining_skip -= count;
                    continue;
                }
                let offset = remaining_skip as i64; // < count ≤ i64::MAX
                remaining_skip = 0;
                let rows = log_boundary_rows(
                    &mut conn,
                    b,
                    stream_filter,
                    query.order,
                    need,
                    offset,
                    proj,
                    query.max_bytes,
                )
                .await?;
                #[cfg(test)]
                {
                    self.inner
                        .rows_decoded
                        .fetch_add(rows.len() as u64, Ordering::Relaxed);
                    self.inner.bytes_materialized.fetch_add(
                        rows.iter().map(|r| r.prefix.len() as u64).sum(),
                        Ordering::Relaxed,
                    );
                }
                need -= rows.len() as i64;
                boundary_walk.extend(rows);
            }
        }

        // The beyond run, pruned by bounds: only candidates whose (filtered)
        // bounds intersect the window are considered at all, and they are
        // pulled in leading-edge order (descending: highest `max_at` first)
        // with a top-k stopping rule — once `want` rows are collected in walk
        // order, a candidate whose leading edge lies strictly beyond the
        // want-th row's `at` cannot contribute to this page, and neither can
        // any later candidate (the pull order sorts by that same edge). Ties
        // at the want-th microsecond are still pulled so cross-segment
        // insertion order stays exact. In the common case — segments rolling
        // forward in time — this opens exactly one segment no matter how many
        // are retained.
        //
        // `exhausted` stays exact under pruning: the rule only skips
        // candidates once `want = max_chunks + 1` rows are collected, and such
        // a page always ends on a cap with `exhausted = false`; with fewer
        // rows every candidate is pulled, and non-candidates provably hold
        // nothing in the window (bounds are exact).
        let mut candidates: Vec<(Timestamp, &PathBuf, (i64, i64))> = segments
            .iter()
            .filter_map(|(start, path)| {
                let sb = bounds.get(start)?.for_filter(stream_filter)?;
                (sb.0 <= beyond_hi && sb.1 >= beyond_lo).then_some((*start, path, sb))
            })
            .collect();
        match query.order {
            LogOrder::Ascending => candidates.sort_by_key(|(_, _, sb)| sb.0),
            LogOrder::Descending => candidates.sort_by_key(|(_, _, sb)| std::cmp::Reverse(sb.1)),
        }

        // Walk-order `at`s collected so far (boundary rows lead the walk),
        // kept sorted so the want-th row's `at` is a direct index.
        let sort_walk = |ats: &mut Vec<i64>| match query.order {
            LogOrder::Ascending => ats.sort_unstable(),
            LogOrder::Descending => ats.sort_unstable_by_key(|at| std::cmp::Reverse(*at)),
        };
        let mut collected: Vec<i64> = boundary_walk.iter().map(|r| r.at.as_micros()).collect();
        sort_walk(&mut collected);

        let mut pulled: BTreeMap<Timestamp, Vec<PageRow>> = BTreeMap::new();
        for (start, path, sb) in candidates {
            if collected.len() >= want as usize {
                let kth = collected[want as usize - 1];
                let beyond_kth = match query.order {
                    LogOrder::Ascending => sb.0 > kth,
                    LogOrder::Descending => sb.1 < kth,
                };
                if beyond_kth {
                    break;
                }
            }
            let Some(mut conn) = self.open_read_counted(path).await? else {
                continue;
            };
            let rows = log_beyond_rows(
                &mut conn,
                beyond_lo,
                beyond_hi,
                stream_filter,
                query.order,
                want,
                proj,
                query.max_bytes,
            )
            .await?;
            #[cfg(test)]
            {
                self.inner
                    .rows_decoded
                    .fetch_add(rows.len() as u64, Ordering::Relaxed);
                self.inner.bytes_materialized.fetch_add(
                    rows.iter().map(|r| r.prefix.len() as u64).sum(),
                    Ordering::Relaxed,
                );
            }
            collected.extend(rows.iter().map(|r| r.at.as_micros()));
            sort_walk(&mut collected);
            pulled.insert(start, rows);
        }

        // Assemble the pulled slices exactly as before: segment-start order
        // ascending (reversed for descending), stable-sorted by `at` so ties
        // keep segment-then-rowid order. The pull set is a subset of the full
        // segment list and every unpulled candidate was provably outside the
        // page, so the merge semantics are unchanged.
        let mut beyond_walk: Vec<PageRow> = match query.order {
            LogOrder::Ascending => pulled.into_values().flatten().collect(),
            LogOrder::Descending => pulled.into_values().rev().flatten().collect(),
        };
        match query.order {
            LogOrder::Ascending => beyond_walk.sort_by_key(|row| row.at),
            LogOrder::Descending => beyond_walk.sort_by_key(|row| std::cmp::Reverse(row.at)),
        }

        // The boundary rows all sit at exactly `resume.at`, the leading edge of
        // the walk, so they precede every beyond row (strictly past it) in walk
        // order — no cross-group sort is needed. `skip` was already consumed via
        // the boundary `OFFSET`, so there is nothing further to drop here.
        let walk: Vec<PageRow> = boundary_walk.into_iter().chain(beyond_walk).collect();

        // Apply the caps: fill until `max_chunks`, or until the next chunk would
        // push cumulative bytes past `max_bytes`. The first chunk always makes
        // progress — if it alone exceeds `max_bytes` its (already projection-
        // bounded) payload is cut to the budget and flagged, and it still counts
        // as fully consumed for the cursor (the walk advances past it whole).
        // `exhausted` is true only when the whole remaining walk fit.
        let mut chunks = Vec::new();
        let mut bytes = 0u64;
        let mut exhausted = true;
        for row in walk {
            if chunks.len() >= query.max_chunks {
                exhausted = false;
                break;
            }
            let size = row.total_len;
            if chunks.is_empty() {
                if size > query.max_bytes {
                    // Oversized first chunk: the projected prefix already holds at
                    // most `max_bytes + 1` bytes, so cutting to the budget never
                    // materialized the full stored row.
                    let cut = (query.max_bytes as usize).min(row.prefix.len());
                    chunks.push(StoredLogChunk {
                        at: row.at,
                        stream: row.stream,
                        bytes: row.prefix.slice(0..cut),
                        truncated: true,
                    });
                    bytes = cut as u64;
                } else {
                    // Fits whole: the row is within budget, so the projected
                    // prefix is its complete payload.
                    bytes += size;
                    chunks.push(row.into_stored(false));
                }
            } else if bytes.saturating_add(size) > query.max_bytes {
                exhausted = false;
                break;
            } else {
                bytes += size;
                chunks.push(row.into_stored(false));
            }
        }

        Ok(LogPage {
            chunks,
            exhausted,
            earliest_at,
            latest_at,
        })
    }

    /// Read an attempt's metric samples with `at` in the inclusive range
    /// (docker-executor.md §8.4), in `(at, insertion order)`. Probes every
    /// segment and stable-merges, for the reasons documented on
    /// [`log_chunks`](Self::log_chunks) — rows can be older than their segment's
    /// filename start. Ids come from the directory plus the row's stored
    /// allocation.
    pub async fn metric_samples(
        &self,
        job: &JobId,
        attempt: &AttemptId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<MetricSample>, StoreError> {
        let dir = self.attempt_dir_existing(job, attempt)?;
        let segments = list_segments(&dir);
        let mut out = Vec::new();
        for (_, path) in &segments {
            let Some(mut conn) = self.open_read(path).await? else {
                continue;
            };
            let rows = metric_range_rows(&mut conn, from, to).await?;
            for row in rows {
                if let Some(sample) = row.into_sample(*job, *attempt) {
                    out.push(sample);
                }
            }
        }
        out.sort_by_key(|sample| sample.at); // stable: ties keep write order
        Ok(out)
    }

    /// The newest stored log timestamp for an attempt (docker-executor.md §8.2
    /// resume input): `MAX(at)` over the attempt's log rows across **all**
    /// segments — not just the newest log-bearing one, because a later segment
    /// can hold only backdated (§8.2 replayed) rows while an earlier one holds
    /// the true maximum. Metrics-only segments contribute nothing; `None` if no
    /// segment has log rows. Whole-second flooring is the caller's job, not the
    /// store's.
    pub async fn max_log_timestamp(
        &self,
        job: &JobId,
        attempt: &AttemptId,
    ) -> Result<Option<Timestamp>, StoreError> {
        let dir = self.attempt_dir_existing(job, attempt)?;
        let segments = list_segments(&dir);
        let mut max: Option<Timestamp> = None;
        for (_, path) in &segments {
            let Some(mut conn) = self.open_read(path).await? else {
                continue;
            };
            let row = sqlx::query!(r#"SELECT MAX(at) AS "max_at: i64" FROM log_chunks"#)
                .fetch_one(&mut conn)
                .await?;
            if let Some(raw) = row.max_at {
                // A corrupt out-of-range micros value is treated as "no reading"
                // for this segment rather than failing the whole resume.
                if let Some(timestamp) = Timestamp::from_micros(raw) {
                    max = Some(max.map_or(timestamp, |current| current.max(timestamp)));
                }
            }
        }
        Ok(max)
    }

    /// The attempt's directory, or [`StoreError::UnknownAttempt`] if it does not
    /// exist. A directory that vanishes mid-read is tolerated per segment (§8.4).
    fn attempt_dir_existing(
        &self,
        job: &JobId,
        attempt: &AttemptId,
    ) -> Result<PathBuf, StoreError> {
        let dir = self
            .inner
            .root
            .join(job.to_string())
            .join(attempt.to_string());
        if dir.is_dir() {
            Ok(dir)
        } else {
            Err(StoreError::UnknownAttempt {
                job: *job,
                attempt: *attempt,
            })
        }
    }

    /// Open a segment read-write in WAL mode for a read (docker-executor.md
    /// §8.4). Read-write **deliberately**: `read_only(true)` cannot run the WAL
    /// recovery a crash-orphaned segment needs, so committed data behind a torn
    /// tail would be invisible. Only `SELECT`s are ever issued. A segment deleted
    /// by retention between listing and open yields `None` (skip + debug-log).
    async fn open_read(&self, path: &Path) -> Result<Option<SqliteConnection>, StoreError> {
        if !path.exists() {
            tracing::debug!(path = %path.display(), "telemetry segment vanished before read; skipping (§8.4)");
            return Ok(None);
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(BUSY_TIMEOUT);
        match options.connect().await {
            Ok(conn) => Ok(Some(conn)),
            Err(err) => {
                if !path.exists() {
                    tracing::debug!(path = %path.display(), "telemetry segment vanished during open; skipping (§8.4)");
                    Ok(None)
                } else {
                    Err(StoreError::Sql(err))
                }
            }
        }
    }

    /// [`open_read`](Self::open_read) plus the test-only opened-segments meter —
    /// every segment open on the [`log_page`](Self::log_page) path goes through
    /// here so the segment-scaling tests can count them.
    async fn open_read_counted(&self, path: &Path) -> Result<Option<SqliteConnection>, StoreError> {
        #[cfg(test)]
        self.inner.segments_opened.fetch_add(1, Ordering::Relaxed);
        self.open_read(path).await
    }

    /// Bring the attempt's cached per-segment bounds in sync with the on-disk
    /// segment list and return a snapshot. Closed segments are immutable, so
    /// any start already cached is trusted as-is; a start not yet cached (cold
    /// cache after restart or sweep invalidation) is probed **once** with an
    /// index-only `MIN`/`MAX` query. Entries for vanished segments are dropped.
    /// The publish merges rather than overwrites, so a concurrent writer note
    /// between probe and publish is never lost (bounds only widen).
    async fn ensure_log_bounds(
        &self,
        job: &JobId,
        attempt: &AttemptId,
        segments: &[(Timestamp, PathBuf)],
    ) -> Result<BTreeMap<Timestamp, StreamBounds>, StoreError> {
        let key = (*job, *attempt);
        let cached: BTreeMap<Timestamp, StreamBounds> = self
            .inner
            .log_bounds
            .lock()
            .expect("log_bounds poisoned")
            .get(&key)
            .cloned()
            .unwrap_or_default();

        let mut probed: Vec<(Timestamp, StreamBounds)> = Vec::new();
        for (start, path) in segments {
            if cached.contains_key(start) {
                continue;
            }
            let Some(mut conn) = self.open_read_counted(path).await? else {
                continue;
            };
            probed.push((*start, log_segment_bounds(&mut conn).await?));
        }

        let live: HashSet<Timestamp> = segments.iter().map(|(start, _)| *start).collect();
        let mut cache = self.inner.log_bounds.lock().expect("log_bounds poisoned");
        let entry = cache.entry(key).or_default();
        for (start, bounds) in probed {
            entry.entry(start).or_default().merge(bounds);
        }
        entry.retain(|start, _| live.contains(start));
        Ok(entry.clone())
    }
}

/// Fetch a `Range` query's log rows from one segment, as `(at, stream, bytes)`
/// tuples. Two static queries keep the optional stream filter compile-checked
/// without a runtime `IS NULL` param.
async fn log_range_rows(
    conn: &mut SqliteConnection,
    from: Timestamp,
    to: Timestamp,
    stream: Option<i64>,
) -> Result<Vec<(i64, i64, Vec<u8>)>, StoreError> {
    let from = from.as_micros();
    let to = to.as_micros();
    let rows = match stream {
        Some(stream) => sqlx::query!(
            r#"SELECT at AS "at!: i64", stream AS "stream!: i64", bytes AS "bytes!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? AND stream = ? ORDER BY at, id"#,
            from,
            to,
            stream,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| (row.at, row.stream, row.bytes))
        .collect(),
        None => sqlx::query!(
            r#"SELECT at AS "at!: i64", stream AS "stream!: i64", bytes AS "bytes!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? ORDER BY at, id"#,
            from,
            to,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| (row.at, row.stream, row.bytes))
        .collect(),
    };
    Ok(rows)
}

/// Fetch a `Tail` query's newest rows from one segment, in `(at DESC, id DESC)`
/// order, as `(at, stream, bytes)` tuples.
async fn log_tail_rows(
    conn: &mut SqliteConnection,
    stream: Option<i64>,
    limit: i64,
) -> Result<Vec<(i64, i64, Vec<u8>)>, StoreError> {
    let rows = match stream {
        Some(stream) => sqlx::query!(
            r#"SELECT at AS "at!: i64", stream AS "stream!: i64", bytes AS "bytes!: Vec<u8>"
               FROM log_chunks WHERE stream = ? ORDER BY at DESC, id DESC LIMIT ?"#,
            stream,
            limit,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| (row.at, row.stream, row.bytes))
        .collect(),
        None => sqlx::query!(
            r#"SELECT at AS "at!: i64", stream AS "stream!: i64", bytes AS "bytes!: Vec<u8>"
               FROM log_chunks ORDER BY at DESC, id DESC LIMIT ?"#,
            limit,
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| (row.at, row.stream, row.bytes))
        .collect(),
    };
    Ok(rows)
}

/// Per-stream data-time bounds `(min_at, max_at)` in µs for one segment's
/// `log_chunks` — `None` when the segment holds no rows for that stream. These
/// are **exact** (a bound is always some real row's `at`), which is what lets
/// [`log_page`] prune segments and settle `exhausted` without opening them:
/// a segment whose bounds miss the window provably holds nothing in it.
///
/// [`log_page`]: FilesystemSink::log_page
#[derive(Clone, Copy, Debug, Default)]
struct StreamBounds {
    stdout: Option<(i64, i64)>,
    stderr: Option<(i64, i64)>,
}

impl StreamBounds {
    /// Extend the bounds with one committed row (writer path).
    fn note(&mut self, stream: LogStream, at: i64) {
        let slot = match stream {
            LogStream::Stdout => &mut self.stdout,
            LogStream::Stderr => &mut self.stderr,
        };
        *slot = Some(match *slot {
            None => (at, at),
            Some((lo, hi)) => (lo.min(at), hi.max(at)),
        });
    }

    /// Union with another observation of the same segment. Bounds only ever
    /// widen, so merging a lazy probe with concurrent writer notes is safe in
    /// either order.
    fn merge(&mut self, other: StreamBounds) {
        for (slot, incoming) in [
            (&mut self.stdout, other.stdout),
            (&mut self.stderr, other.stderr),
        ] {
            if let Some((lo, hi)) = incoming {
                *slot = Some(match *slot {
                    None => (lo, hi),
                    Some((cur_lo, cur_hi)) => (cur_lo.min(lo), cur_hi.max(hi)),
                });
            }
        }
    }

    /// The bounds relevant to a query's stream filter: the one stream's, or the
    /// union when unfiltered. `None` = the segment holds nothing the query can
    /// see.
    fn for_filter(&self, filter: Option<i64>) -> Option<(i64, i64)> {
        match filter.map(LogStream::from_i64) {
            Some(LogStream::Stdout) => self.stdout,
            Some(LogStream::Stderr) => self.stderr,
            None => match (self.stdout, self.stderr) {
                (None, b) => b,
                (a, None) => a,
                (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.min(blo), ahi.max(bhi))),
            },
        }
    }
}

/// Probe one segment's per-stream `MIN`/`MAX` data-time bounds — index-only,
/// decodes no payload. Run **once per closed segment's lifetime** (they are
/// immutable); the open segment's bounds are thereafter extended in memory by
/// the writer.
async fn log_segment_bounds(conn: &mut SqliteConnection) -> Result<StreamBounds, StoreError> {
    let rows = sqlx::query!(
        r#"SELECT stream AS "stream!: i64", MIN(at) AS "min_at!: i64", MAX(at) AS "max_at!: i64"
           FROM log_chunks GROUP BY stream"#,
    )
    .fetch_all(conn)
    .await?;
    let mut bounds = StreamBounds::default();
    for row in rows {
        let stream = LogStream::from_i64(row.stream);
        bounds.note(stream, row.min_at);
        bounds.note(stream, row.max_at);
    }
    Ok(bounds)
}

/// One projected page row for [`log_page`]: its `at`/`stream`, the row's *true*
/// payload length (for the cumulative byte-budget arithmetic and the truncated
/// verdict), and a payload **prefix** bounded to the page byte budget. The full
/// stored `bytes` is never materialized — an oversized row yields only its
/// budget-sized prefix, so a crafted multi-MiB chunk cannot blow up input work.
struct PageRow {
    at: Timestamp,
    stream: LogStream,
    /// `length(bytes)` — the true stored size, possibly larger than `prefix`.
    total_len: u64,
    /// The first `min(total_len, max_bytes + 1)` bytes of the payload.
    prefix: bytes::Bytes,
}

impl PageRow {
    /// Rebuild a whole-payload [`StoredLogChunk`]. Only valid when the row fits
    /// the budget (`total_len ≤ max_bytes`), so the projected `prefix` already
    /// holds the complete payload; the oversized case cuts the prefix inline.
    fn into_stored(self, truncated: bool) -> StoredLogChunk {
        StoredLogChunk {
            at: self.at,
            stream: self.stream,
            bytes: self.prefix,
            truncated,
        }
    }
}

/// Rebuild a [`PageRow`] from a raw projected row, skipping one whose micros are
/// out of range (corruption tolerance, §8.4). `total_len` floors at zero.
fn page_row(at: i64, stream: i64, total_len: i64, prefix: Vec<u8>) -> Option<PageRow> {
    Some(PageRow {
        at: Timestamp::from_micros(at)?,
        stream: LogStream::from_i64(stream),
        total_len: total_len.max(0) as u64,
        prefix: bytes::Bytes::from(prefix),
    })
}

/// A raw projected page row as it streams out of SQLite: `(at, stream,
/// length(bytes), substr(bytes, 1, proj))`. The tuple shape is shared across the
/// eight direction × stream-filter queries so [`drain_projected`] can consume any
/// of them through one boxed stream.
type ProjectedRow = (i64, i64, i64, Vec<u8>);

/// A boxed, `Send` stream of [`ProjectedRow`]s — sqlx's `.fetch()` cursor mapped
/// to the shared tuple. Each static query has a distinct anonymous row type, so
/// mapping to this common item lets one drain loop serve every arm.
type ProjectedRowStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<ProjectedRow, sqlx::Error>> + Send + 'a>,
>;

/// Drain a projected-row cursor into `PageRow`s with a **byte-based early
/// cutoff** (ADR 0034 review): SQLite computes each row's `substr` projection
/// only as the cursor steps, so stopping the cursor stops the materialization.
/// The cursor is stepped until whichever comes first:
///
///   * the SQL `LIMIT` backstop (`max_chunks + 1` rows) is exhausted, or
///   * cumulative *projected* bytes pulled from this segment reach `budget`
///     (the page's `max_bytes`) — after which exactly one further payload-bearing
///     row is pulled as the merge's break/look-ahead, with any intervening
///     zero-length (empty) chunks pulled too (the merge can consume a run of
///     them after an exactly-fitting or oversized chunk before breaking).
///
/// This turns each segment's window from an eager O(`max_chunks` × `max_bytes`)
/// pull into an O(`max_bytes`) one: a segment materializes at most
/// `budget + 2·proj` projected bytes, independent of `max_chunks`. Correctness
/// rests on the merge drawing only a *prefix* of each segment's ordered window,
/// bounded to `max_bytes` total projected bytes — so no correct page can need
/// rows past the first ~budget bytes (+ the one look-ahead) of a segment's
/// window (see [`log_page`]).
async fn drain_projected(
    mut rows: ProjectedRowStream<'_>,
    budget: u64,
) -> Result<Vec<PageRow>, StoreError> {
    use tokio_stream::StreamExt as _;
    let mut out = Vec::new();
    let mut projected = 0u64;
    let mut over_budget = false;
    while let Some(item) = rows.next().await {
        let (at, stream, len, prefix) = item?;
        let plen = prefix.len() as u64;
        projected = projected.saturating_add(plen);
        if let Some(row) = page_row(at, stream, len, prefix) {
            out.push(row);
        }
        if over_budget {
            // Past the budget: the merge may still consume a run of empty
            // (zero-projection) chunks after an exactly-fitting or oversized
            // chunk, then break on the next payload-bearing one — so keep
            // stepping only while rows carry no payload, and stop once one does
            // (that row is the merge's break/look-ahead row).
            if plen > 0 {
                break;
            }
        } else if projected >= budget {
            // Cumulative projected bytes reached the page budget: a page bounded
            // to `budget` total bytes can draw nothing more from this segment
            // beyond one look-ahead row. The SQL `LIMIT` remains the row backstop.
            over_budget = true;
        }
    }
    Ok(out)
}

/// Stream one segment's **beyond** slice for [`log_page`] — the rows in the
/// inclusive `[lo, hi]` range, in the walk direction, projecting `length(bytes)`
/// plus a `substr(bytes, 1, proj)` prefix so payload materialization is bounded
/// to `proj` per row. `limit` (`max_chunks + 1`) is the SQL row backstop and
/// `budget` (`max_bytes`) the byte cutoff [`drain_projected`] applies as the
/// cursor steps; four static queries keep the direction and optional stream
/// filter compile-checked without runtime SQL assembly.
#[allow(clippy::too_many_arguments)]
async fn log_beyond_rows(
    conn: &mut SqliteConnection,
    lo: i64,
    hi: i64,
    stream: Option<i64>,
    order: LogOrder,
    limit: i64,
    proj: i64,
    budget: u64,
) -> Result<Vec<PageRow>, StoreError> {
    use tokio_stream::StreamExt as _;
    // Bind the filter value at function scope: the streamed query holds a
    // reference to its bound args for the cursor's whole lifetime, so a
    // `Some(stream)` arm-local (dropped at the arm's end) would not live long
    // enough. The unfiltered arms ignore it.
    let stream_val = stream.unwrap_or_default();
    let rows: ProjectedRowStream = match (order, stream) {
        (LogOrder::Ascending, Some(_)) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? AND stream = ? ORDER BY at, id LIMIT ?"#,
                proj,
                lo,
                hi,
                stream_val,
                limit,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Ascending, None) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? ORDER BY at, id LIMIT ?"#,
                proj,
                lo,
                hi,
                limit,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Descending, Some(_)) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? AND stream = ? ORDER BY at DESC, id DESC LIMIT ?"#,
                proj,
                lo,
                hi,
                stream_val,
                limit,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Descending, None) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at BETWEEN ? AND ? ORDER BY at DESC, id DESC LIMIT ?"#,
                proj,
                lo,
                hi,
                limit,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
    };
    drain_projected(rows, budget).await
}

/// Count one segment's rows at exactly `at` — an index-only probe over the `at`
/// index that decodes **no** payload, so an arbitrarily large cursor `skip` can
/// be spent against these counts for free. Two static queries keep the optional
/// stream filter compile-checked.
async fn log_boundary_count(
    conn: &mut SqliteConnection,
    at: i64,
    stream: Option<i64>,
) -> Result<i64, StoreError> {
    let n = match stream {
        Some(stream) => {
            sqlx::query!(
                r#"SELECT COUNT(*) AS "n!: i64" FROM log_chunks WHERE at = ? AND stream = ?"#,
                at,
                stream,
            )
            .fetch_one(conn)
            .await?
            .n
        }
        None => {
            sqlx::query!(
                r#"SELECT COUNT(*) AS "n!: i64" FROM log_chunks WHERE at = ?"#,
                at,
            )
            .fetch_one(conn)
            .await?
            .n
        }
    };
    Ok(n)
}

/// Stream one segment's surviving **boundary** rows — those at exactly `at`,
/// after `offset` (the per-segment share of the cursor `skip`), in the walk
/// direction (`id` ascending / descending), projecting the same bounded prefix
/// as [`log_beyond_rows`] and subject to the same byte cutoff via
/// [`drain_projected`]. SQLite applies `OFFSET` over the `(at, id)` index
/// without decoding payload for the skipped rows, so only the streamed rows
/// materialize any `bytes` — and the cursor stops at the byte budget, so an
/// enormous run of boundary rows costs at most `budget + 2·proj` per segment.
/// Four static queries keep direction and stream filter compile-checked.
#[allow(clippy::too_many_arguments)]
async fn log_boundary_rows(
    conn: &mut SqliteConnection,
    at: i64,
    stream: Option<i64>,
    order: LogOrder,
    limit: i64,
    offset: i64,
    proj: i64,
    budget: u64,
) -> Result<Vec<PageRow>, StoreError> {
    use tokio_stream::StreamExt as _;
    // Bind the filter value at function scope (see [`log_beyond_rows`]): the
    // streamed cursor holds a reference to its bound args for its whole lifetime.
    let stream_val = stream.unwrap_or_default();
    let rows: ProjectedRowStream = match (order, stream) {
        (LogOrder::Ascending, Some(_)) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at = ? AND stream = ? ORDER BY id LIMIT ? OFFSET ?"#,
                proj,
                at,
                stream_val,
                limit,
                offset,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Ascending, None) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at = ? ORDER BY id LIMIT ? OFFSET ?"#,
                proj,
                at,
                limit,
                offset,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Descending, Some(_)) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at = ? AND stream = ? ORDER BY id DESC LIMIT ? OFFSET ?"#,
                proj,
                at,
                stream_val,
                limit,
                offset,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
        (LogOrder::Descending, None) => Box::pin(
            sqlx::query!(
                r#"SELECT at AS "at!: i64", stream AS "stream!: i64",
                      length(bytes) AS "len!: i64", substr(bytes, 1, ?) AS "prefix!: Vec<u8>"
               FROM log_chunks WHERE at = ? ORDER BY id DESC LIMIT ? OFFSET ?"#,
                proj,
                at,
                limit,
                offset,
            )
            .fetch(conn)
            .map(|r| r.map(|row| (row.at, row.stream, row.len, row.prefix))),
        ),
    };
    drain_projected(rows, budget).await
}

/// One metrics row as read back; the stored integer columns are widened back to
/// the domain's `u64`/`Duration` at [`into_sample`](MetricRow::into_sample).
struct MetricRow {
    at: i64,
    allocation_id: String,
    cpu_usage_total_us: i64,
    cpu_throttled_total_us: i64,
    memory_used_bytes: i64,
    memory_peak_bytes: i64,
    disk_writable_bytes: i64,
    disk_image_bytes: i64,
    net_rx_bytes_total: i64,
    net_tx_bytes_total: i64,
    blkio_read_bytes_total: i64,
    blkio_write_bytes_total: i64,
}

impl MetricRow {
    /// Rebuild a [`MetricSample`], taking the ids from the directory (`job`,
    /// `attempt`) and the stored allocation. `None` — skipping the row — when the
    /// allocation or timestamp will not parse (corruption tolerance, §8.4).
    fn into_sample(self, job: JobId, attempt: AttemptId) -> Option<MetricSample> {
        Some(MetricSample {
            allocation: self.allocation_id.parse::<AllocationId>().ok()?,
            attempt,
            job,
            at: Timestamp::from_micros(self.at)?,
            cpu_usage_total: CoreDuration::from_micros(self.cpu_usage_total_us),
            cpu_throttled_total: CoreDuration::from_micros(self.cpu_throttled_total_us),
            memory_used_bytes: i64_to_u64(self.memory_used_bytes),
            memory_peak_bytes: i64_to_u64(self.memory_peak_bytes),
            disk_writable_bytes: i64_to_u64(self.disk_writable_bytes),
            disk_image_bytes: i64_to_u64(self.disk_image_bytes),
            net_rx_bytes_total: i64_to_u64(self.net_rx_bytes_total),
            net_tx_bytes_total: i64_to_u64(self.net_tx_bytes_total),
            blkio_read_bytes_total: i64_to_u64(self.blkio_read_bytes_total),
            blkio_write_bytes_total: i64_to_u64(self.blkio_write_bytes_total),
        })
    }
}

/// Fetch a metrics range's rows from one segment, in `(at, insertion order)`.
async fn metric_range_rows(
    conn: &mut SqliteConnection,
    from: Timestamp,
    to: Timestamp,
) -> Result<Vec<MetricRow>, StoreError> {
    let from = from.as_micros();
    let to = to.as_micros();
    let rows = sqlx::query!(
        r#"SELECT
             at AS "at!: i64",
             allocation_id AS "allocation_id!: String",
             cpu_usage_total_us AS "cpu_usage_total_us!: i64",
             cpu_throttled_total_us AS "cpu_throttled_total_us!: i64",
             memory_used_bytes AS "memory_used_bytes!: i64",
             memory_peak_bytes AS "memory_peak_bytes!: i64",
             disk_writable_bytes AS "disk_writable_bytes!: i64",
             disk_image_bytes AS "disk_image_bytes!: i64",
             net_rx_bytes_total AS "net_rx_bytes_total!: i64",
             net_tx_bytes_total AS "net_tx_bytes_total!: i64",
             blkio_read_bytes_total AS "blkio_read_bytes_total!: i64",
             blkio_write_bytes_total AS "blkio_write_bytes_total!: i64"
           FROM metrics WHERE at BETWEEN ? AND ? ORDER BY at, id"#,
        from,
        to,
    )
    .fetch_all(conn)
    .await?
    .into_iter()
    .map(|row| MetricRow {
        at: row.at,
        allocation_id: row.allocation_id,
        cpu_usage_total_us: row.cpu_usage_total_us,
        cpu_throttled_total_us: row.cpu_throttled_total_us,
        memory_used_bytes: row.memory_used_bytes,
        memory_peak_bytes: row.memory_peak_bytes,
        disk_writable_bytes: row.disk_writable_bytes,
        disk_image_bytes: row.disk_image_bytes,
        net_rx_bytes_total: row.net_rx_bytes_total,
        net_tx_bytes_total: row.net_tx_bytes_total,
        blkio_read_bytes_total: row.blkio_read_bytes_total,
        blkio_write_bytes_total: row.blkio_write_bytes_total,
    })
    .collect();
    Ok(rows)
}

/// Rebuild a [`StoredLogChunk`], skipping a row whose micros are out of range.
fn stored_chunk(at: i64, stream: i64, bytes: Vec<u8>) -> Option<StoredLogChunk> {
    Some(StoredLogChunk {
        at: Timestamp::from_micros(at)?,
        stream: LogStream::from_i64(stream),
        bytes: bytes::Bytes::from(bytes),
        truncated: false,
    })
}

// ---- pure policy + filesystem helpers (the unit-test surface) ------------

/// Whether a segment must roll before the next batch (docker-executor.md §8.4):
/// its on-disk size has reached `segment_max`, or its age has reached
/// `segment_max_age`. Pure, so the roll boundary is unit-testable.
fn should_roll(
    start: Timestamp,
    size: u64,
    now: Timestamp,
    segment_max: ByteSize,
    segment_max_age: CoreDuration,
) -> bool {
    size >= segment_max.as_u64() || now.duration_since(start) >= segment_max_age
}

/// A read-only view of one attempt's on-disk segments, the pure input to
/// [`sweep_plan`].
struct AttemptSweepView {
    ended_at: Option<Timestamp>,
    segments: Vec<SegmentView>,
}

/// One segment in an [`AttemptSweepView`], ordered ascending by `start`.
struct SegmentView {
    start: Timestamp,
    path: PathBuf,
    open: bool,
}

/// The ordered deletions one sweep will make (docker-executor.md §8.4).
/// `mandatory` runs unconditionally (normal retention + the live cap);
/// `conditional` runs only under pressure, in order, gated by the driver's
/// per-deletion resample.
struct SweepPlan {
    mandatory: Vec<PathBuf>,
    conditional: Vec<PathBuf>,
}

/// The pure sweep policy (docker-executor.md §8.4), so selection and ordering
/// are unit-testable without a filesystem or `statvfs`. Never selects a segment
/// the writer holds open, and never a live attempt's newest segment.
fn sweep_plan(
    views: &[AttemptSweepView],
    now: Timestamp,
    retention: CoreDuration,
    live_retention: CoreDuration,
    pressure: DiskPressure,
) -> SweepPlan {
    let mut mandatory = Vec::new();
    // Ended-but-not-yet-expired attempts, for the pressure tier, tagged with
    // their end time so the tier deletes oldest-ended-first.
    let mut pressure_attempts: Vec<(Timestamp, Vec<PathBuf>)> = Vec::new();

    for view in views {
        match view.ended_at {
            Some(ended) => {
                // Rule 1: past retention → delete every segment (an ended
                // attempt is closed, so its newest is deletable too).
                if now.duration_since(ended) > retention {
                    for segment in &view.segments {
                        if !segment.open {
                            mandatory.push(segment.path.clone());
                        }
                    }
                } else {
                    let paths: Vec<PathBuf> = view
                        .segments
                        .iter()
                        .filter(|segment| !segment.open)
                        .map(|segment| segment.path.clone())
                        .collect();
                    if !paths.is_empty() {
                        pressure_attempts.push((ended, paths));
                    }
                }
            }
            None => {
                // Rule 3 + 4: a live attempt's closed segment k (has a successor
                // k+1, so never the newest) goes once the successor has existed
                // longer than the live cap. Derived purely from filenames.
                let count = view.segments.len();
                for k in 0..count.saturating_sub(1) {
                    let successor_start = view.segments[k + 1].start;
                    let segment = &view.segments[k];
                    if !segment.open && now.duration_since(successor_start) > live_retention {
                        mandatory.push(segment.path.clone());
                    }
                }
            }
        }
    }

    // Rule 2: under pressure, the remaining ended attempts go oldest-ended-first
    // (and oldest segment first within each — the vecs are already ascending).
    let mut conditional = Vec::new();
    if pressure >= DiskPressure::High {
        pressure_attempts.sort_by_key(|(ended, _)| *ended);
        for (_, paths) in pressure_attempts {
            conditional.extend(paths);
        }
    }

    SweepPlan {
        mandatory,
        conditional,
    }
}

/// Scan `root` for every attempt's segments, tagging each segment with whether
/// the writer holds it open (docker-executor.md §8.4). Unparseable id
/// directories are skipped and warned about.
fn scan_attempts(root: &Path, open_paths: &HashSet<PathBuf>) -> Vec<AttemptSweepView> {
    let mut views = Vec::new();
    for (_, job_dir) in list_id_dirs::<JobId>(root) {
        for (_, attempt_dir) in list_id_dirs::<AttemptId>(&job_dir) {
            let segments = list_segments(&attempt_dir)
                .into_iter()
                .map(|(start, path)| SegmentView {
                    start,
                    open: open_paths.contains(&path),
                    path,
                })
                .collect();
            views.push(AttemptSweepView {
                ended_at: read_ended_marker(&attempt_dir),
                segments,
            });
        }
    }
    views
}

/// The `seg-<start-µs>.db` filename for a segment start, zero-padded to 20
/// digits so lexicographic order matches time order (docker-executor.md §8.4).
fn segment_filename(start: Timestamp) -> String {
    format!("seg-{:020}.db", start.as_micros())
}

/// Parse a segment start out of a `seg-<µs>.db` filename, or `None` for anything
/// else (a `-wal`/`-shm` sibling, the `ended` marker, junk).
fn parse_segment_start(name: &str) -> Option<Timestamp> {
    let digits = name.strip_prefix("seg-")?.strip_suffix(".db")?;
    Timestamp::from_micros(digits.parse::<i64>().ok()?)
}

/// The next segment start for `dir`: `now`, bumped strictly past any existing
/// segment's start so starts stay strictly increasing across rolls and restarts
/// even if the clock stalls or regresses (docker-executor.md §8.4).
fn next_segment_start(dir: &Path, now: Timestamp) -> Timestamp {
    let mut floor = now.as_micros();
    for (start, _) in list_segments(dir) {
        floor = floor.max(start.as_micros().saturating_add(1));
    }
    Timestamp::from_micros(floor).unwrap_or_else(Timestamp::max_value)
}

/// List a directory's segment files as `(start, path)`, ascending by start.
/// A missing directory is an empty list.
fn list_segments(dir: &Path) -> Vec<(Timestamp, PathBuf)> {
    let mut segments = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return segments;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(start) = parse_segment_start(name) {
            segments.push((start, path));
        }
    }
    segments.sort_by_key(|(start, _)| *start);
    segments
}

/// List the immediate child directories of `dir` whose names parse as id `T`,
/// as `(id, path)`. Unparseable names are skipped and warned about (§8.4).
fn list_id_dirs<T>(dir: &Path) -> Vec<(T, PathBuf)>
where
    T: std::str::FromStr,
{
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        match name.parse::<T>() {
            Ok(id) => out.push((id, path)),
            Err(_) => tracing::warn!(
                dir = %dir.display(),
                entry = name,
                "skipping telemetry directory whose name is not a valid id (§8.4)"
            ),
        }
    }
    out
}

/// The current on-disk size of a segment: its `.db` plus its `-wal`
/// (the `-shm` is negligible), each read leniently as zero on error.
fn segment_size_on_disk(path: &Path) -> u64 {
    let main = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let wal = std::fs::metadata(sibling(path, "-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    main + wal
}

/// Delete a segment's files — the `.db` and its `-wal`/`-shm` siblings
/// (docker-executor.md §8.4). Returns whether the main file was removed.
fn delete_segment_files(path: &Path) -> bool {
    let removed = std::fs::remove_file(path).is_ok();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(sibling(path, suffix));
    }
    removed
}

/// A sibling path formed by appending `suffix` to the file name — how SQLite
/// names the `-wal`/`-shm` companions of a database file.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name: OsString = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// Remove attempt directories left with no segment files after a sweep — drop
/// the `ended` marker and any orphaned `-wal`/`-shm` journal files, then the
/// now-empty attempt and job directories (docker-executor.md §8.4).
/// Whole-file/dir unlinks only; every error is tolerated (a non-empty dir
/// simply stays).
fn cleanup_empty_dirs(root: &Path) {
    let Ok(jobs) = std::fs::read_dir(root) else {
        return;
    };
    for job_entry in jobs.flatten() {
        let job_dir = job_entry.path();
        if !job_dir.is_dir() {
            continue;
        }
        if let Ok(attempts) = std::fs::read_dir(&job_dir) {
            for attempt_entry in attempts.flatten() {
                let attempt_dir = attempt_entry.path();
                if attempt_dir.is_dir() && list_segments(&attempt_dir).is_empty() {
                    let _ = std::fs::remove_file(attempt_dir.join("ended"));
                    remove_orphan_journal_files(&attempt_dir);
                    let _ = std::fs::remove_dir(&attempt_dir);
                }
            }
        }
        let _ = std::fs::remove_dir(&job_dir);
    }
}

/// Unlink `seg-*.db-wal`/`-shm` files whose `seg-*.db` no longer exists. A
/// reader that raced a sweep recreates them by path ([`open_read`] is
/// read-write WAL, so its first query re-creates the journals of an
/// already-unlinked segment), and a reader that dies without a clean close
/// leaves them behind — either way they hold no recoverable data once the main
/// file is gone, and they would block the attempt dir's removal on every
/// future sweep. The `.db` absence is re-checked per file so a segment being
/// created concurrently keeps its journal.
///
/// [`open_read`]: FilesystemSink::open_read
fn remove_orphan_journal_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(db_name) = name
            .strip_suffix("-wal")
            .or_else(|| name.strip_suffix("-shm"))
        else {
            continue;
        };
        if parse_segment_start(db_name).is_some() && !dir.join(db_name).exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Read an attempt's `ended` marker (decimal µs), or `None` if absent or
/// unparseable (treated as still-live).
fn read_ended_marker(dir: &Path) -> Option<Timestamp> {
    let raw = std::fs::read_to_string(dir.join("ended")).ok()?;
    Timestamp::from_micros(raw.trim().parse::<i64>().ok()?)
}

/// Durably write the `ended` marker (decimal µs) via a temp file + rename, so a
/// crash never leaves a half-written marker (docker-executor.md §8.4).
fn write_marker(dir: &Path, marker: &Path, at: Timestamp) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = sibling(marker, ".tmp");
    std::fs::write(&tmp, at.as_micros().to_string())?;
    std::fs::rename(&tmp, marker)
}

/// Group a batch by `(job, attempt)`, preserving each group's slice order (the
/// per-segment insertion order §8.2 mandates). Batches are small, so the linear
/// group lookup is ample.
fn group_by_attempt<T>(
    batch: &[T],
    key: impl Fn(&T) -> (JobId, AttemptId),
) -> Vec<((JobId, AttemptId), Vec<&T>)> {
    let mut groups: Vec<((JobId, AttemptId), Vec<&T>)> = Vec::new();
    for item in batch {
        let group_key = key(item);
        match groups
            .iter_mut()
            .find(|(existing, _)| *existing == group_key)
        {
            Some((_, items)) => items.push(item),
            None => groups.push((group_key, vec![item])),
        }
    }
    groups
}

/// Clamp a domain `u64` into the segment store's signed `INTEGER` column,
/// saturating at `i64::MAX` with a warning rather than dropping the row (§8.4).
fn u64_to_i64_clamped(value: u64, field: &'static str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| {
        tracing::warn!(
            field,
            value,
            "telemetry metric exceeds i64::MAX; clamping (row kept, §8.4)"
        );
        i64::MAX
    })
}

/// Widen a stored `INTEGER` back to the domain's `u64`. The store only ever
/// writes non-negative values (clamped from `u64`), so a negative reading is
/// corruption and floors at zero.
fn i64_to_u64(value: i64) -> u64 {
    value.max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{LogSink, MetricsSink};
    use std::io::Write;
    use tempfile::TempDir;

    // Hand-built timestamps the pressure.rs/cache.rs way: no clock, just
    // `UNIX_EPOCH + Duration` (§12).
    fn at(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + CoreDuration::from_secs(secs)
    }

    async fn sink_with(
        root: PathBuf,
        mutate: impl FnOnce(&mut FilesystemSinkOptions),
    ) -> FilesystemSink {
        let mut opts = FilesystemSinkOptions::new(root);
        // Tests default to "never roll" so a test opts into one roll axis at a
        // time; individual tests override.
        opts.segment_max = ByteSize::from_gib(1);
        opts.segment_max_age = CoreDuration::from_hours(24);
        mutate(&mut opts);
        FilesystemSink::new(opts).await.expect("build sink")
    }

    fn metric(job: JobId, attempt: AttemptId, alloc: AllocationId, at: Timestamp) -> MetricSample {
        MetricSample {
            allocation: alloc,
            attempt,
            job,
            at,
            cpu_usage_total: CoreDuration::from_secs(1),
            cpu_throttled_total: CoreDuration::ZERO,
            memory_used_bytes: 100,
            memory_peak_bytes: 200,
            disk_writable_bytes: 10,
            disk_image_bytes: 20,
            net_rx_bytes_total: 1,
            net_tx_bytes_total: 2,
            blkio_read_bytes_total: 3,
            blkio_write_bytes_total: 4,
        }
    }

    fn log(
        job: JobId,
        attempt: AttemptId,
        alloc: AllocationId,
        at: Timestamp,
        stream: LogStream,
        bytes: &[u8],
    ) -> LogChunk {
        LogChunk {
            allocation: alloc,
            attempt,
            job,
            at,
            stream,
            bytes: bytes::Bytes::copy_from_slice(bytes),
        }
    }

    fn attempt_dir(sink: &FilesystemSink, job: JobId, attempt: AttemptId) -> PathBuf {
        sink.inner
            .root
            .join(job.to_string())
            .join(attempt.to_string())
    }

    fn segment_files(dir: &Path) -> Vec<PathBuf> {
        list_segments(dir)
            .into_iter()
            .map(|(_, path)| path)
            .collect()
    }

    // ---- 1. segment roll (docker-executor.md §8.4) -------------------------

    #[tokio::test]
    async fn rolls_by_size_and_never_splits_a_batch() {
        let root = TempDir::new().unwrap();
        // A one-byte cap forces every append after the first to roll.
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        for i in 0..3 {
            let now = at(100 + i * 10);
            let batch = vec![
                metric(job, attempt, alloc, now),
                metric(job, attempt, alloc, now),
            ];
            sink.append_metrics_at(&batch, now).await;
        }
        let dir = attempt_dir(&sink, job, attempt);
        let segments = list_segments(&dir);
        assert_eq!(segments.len(), 3, "one segment per rolled append");
        // Strictly increasing, non-overlapping starts.
        for pair in segments.windows(2) {
            assert!(pair[0].0 < pair[1].0, "starts strictly increasing");
        }
        // Each segment holds exactly its whole 2-row batch (batch never split).
        for (_, path) in &segments {
            let mut conn = super::SqliteConnectOptions::new()
                .filename(path)
                .connect()
                .await
                .unwrap();
            let count = sqlx::query!(r#"SELECT COUNT(*) AS "n!: i64" FROM metrics"#)
                .fetch_one(&mut conn)
                .await
                .unwrap()
                .n;
            assert_eq!(count, 2, "a batch is never split across a roll");
        }
        // And all six rows are readable across the segment set.
        let all = sink
            .metric_samples(&job, &attempt, at(0), at(1000))
            .await
            .unwrap();
        assert_eq!(all.len(), 6);
    }

    #[tokio::test]
    async fn rolls_by_age() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(10);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        // 5s later: within the 10s age bound, same segment.
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(5))], at(5))
            .await;
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 1);
        // 15s from the segment start: past the bound, rolls.
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(15))], at(15))
            .await;
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 2);
    }

    // ---- 2. cross-segment range reads (docker-executor.md §8.4) -------------

    async fn three_log_segments(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
        alloc: AllocationId,
    ) {
        // A one-byte cap makes each append its own segment, with start == now.
        for t in [10, 20, 30] {
            sink.append_logs_at(
                &[log(job, attempt, alloc, at(t), LogStream::Stdout, b"x")],
                at(t),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn range_reads_span_segments_with_inclusive_boundaries() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        three_log_segments(&sink, job, attempt, alloc).await;
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 3);

        let ats = |chunks: Vec<StoredLogChunk>| -> Vec<i64> {
            chunks.into_iter().map(|c| c.at.as_micros()).collect()
        };

        // Whole span: every segment, concatenated in order.
        let all = sink
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(10),
                    to: at(30),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            ats(all),
            vec![at(10).as_micros(), at(20).as_micros(), at(30).as_micros()]
        );

        // Exact-boundary hit is inclusive.
        let exact = sink
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(20),
                    to: at(20),
                },
            )
            .await
            .unwrap();
        assert_eq!(ats(exact), vec![at(20).as_micros()]);

        // Empty range between two rows.
        let empty = sink
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(11),
                    to: at(19),
                },
            )
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn range_reads_preserve_duplicates_and_insertion_order() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // One batch, deliberately out-of-order `at` plus an identical pair.
        let batch = vec![
            log(job, attempt, alloc, at(51), LogStream::Stdout, b"b"),
            log(job, attempt, alloc, at(50), LogStream::Stdout, b"a"),
            log(job, attempt, alloc, at(40), LogStream::Stdout, b"dup"),
            log(job, attempt, alloc, at(40), LogStream::Stdout, b"dup"),
        ];
        sink.append_logs_at(&batch, at(50)).await;
        let rows = sink
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(0),
                    to: at(100),
                },
            )
            .await
            .unwrap();
        // Ordered by (at, insertion order); the identical pair is preserved.
        let shape: Vec<(i64, &[u8])> = rows
            .iter()
            .map(|c| (c.at.as_micros(), c.bytes.as_ref()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (at(40).as_micros(), b"dup".as_ref()),
                (at(40).as_micros(), b"dup".as_ref()),
                (at(50).as_micros(), b"a".as_ref()),
                (at(51).as_micros(), b"b".as_ref()),
            ]
        );
    }

    // A segment can hold rows *older* than its filename start: every first
    // flush batch is stamped before the segment was created, and §8.2 restart
    // replay appends arbitrarily older chunks to the freshly created open
    // segment. Range reads must find those rows (probing every segment), and
    // equal-`at` occurrences across segments must keep write order.
    #[tokio::test]
    async fn range_reads_find_rows_older_than_their_segment_start() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1); // roll each append
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Segment 1 (start ≈ 100) holds the original occurrence of at(15);
        // segment 2 (start ≈ 200) holds a §8.2 replayed copy of the same second
        // plus a chunk from just before the crash — all far older than its start.
        sink.append_logs_at(
            &[log(job, attempt, alloc, at(15), LogStream::Stdout, b"orig")],
            at(100),
        )
        .await;
        sink.append_logs_at(
            &[
                log(job, attempt, alloc, at(15), LogStream::Stdout, b"replay"),
                log(job, attempt, alloc, at(17), LogStream::Stdout, b"tail"),
            ],
            at(200),
        )
        .await;
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 2);

        let rows = sink
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(10),
                    to: at(20),
                },
            )
            .await
            .unwrap();
        let shape: Vec<(i64, &[u8])> = rows
            .iter()
            .map(|c| (c.at.as_micros(), c.bytes.as_ref()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (at(15).as_micros(), b"orig".as_ref()), // written first, sorts first
                (at(15).as_micros(), b"replay".as_ref()),
                (at(17).as_micros(), b"tail".as_ref()),
            ],
            "backdated rows in a later segment are found, in (at, write) order"
        );

        // Tail sees the true newest rows by `at`, not by segment position.
        let tail = sink
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 2 })
            .await
            .unwrap();
        let shape: Vec<(i64, &[u8])> = tail
            .iter()
            .map(|c| (c.at.as_micros(), c.bytes.as_ref()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (at(15).as_micros(), b"replay".as_ref()),
                (at(17).as_micros(), b"tail".as_ref()),
            ]
        );
    }

    #[tokio::test]
    async fn stream_filter_selects_one_stream() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_logs_at(
            &[
                log(job, attempt, alloc, at(10), LogStream::Stdout, b"out"),
                log(job, attempt, alloc, at(11), LogStream::Stderr, b"err"),
            ],
            at(10),
        )
        .await;
        let errs = sink
            .log_chunks(
                &job,
                &attempt,
                Some(LogStream::Stderr),
                LogQuery::Range {
                    from: at(0),
                    to: at(100),
                },
            )
            .await
            .unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].stream, LogStream::Stderr);
        assert_eq!(errs[0].bytes.as_ref(), b"err");
    }

    // ---- 3. tail reads (docker-executor.md §8.4) ---------------------------

    #[tokio::test]
    async fn tail_reads_span_segments() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        three_log_segments(&sink, job, attempt, alloc).await;

        let ats = |chunks: Vec<StoredLogChunk>| -> Vec<i64> {
            chunks.into_iter().map(|c| c.at.as_micros()).collect()
        };
        // Newest two, returned ascending, spanning >1 segment.
        let tail2 = sink
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 2 })
            .await
            .unwrap();
        assert_eq!(ats(tail2), vec![at(20).as_micros(), at(30).as_micros()]);
        // n larger than the total returns everything, ascending.
        let tail_all = sink
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 9 })
            .await
            .unwrap();
        assert_eq!(
            ats(tail_all),
            vec![at(10).as_micros(), at(20).as_micros(), at(30).as_micros()]
        );
    }

    // ---- 4. torn-WAL crash recovery (docker-executor.md §8.4) --------------

    #[tokio::test]
    async fn committed_data_survives_a_torn_wal_and_uncommitted_is_invisible() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, alloc) = (JobId::new(), AllocationId::new());

        // --- committed survives ---
        let attempt = AttemptId::new();
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(10))], at(10))
            .await;
        let path = segment_files(&attempt_dir(&sink, job, attempt))[0].clone();
        // Checkpoint A into the main db so it is safe regardless of the WAL, then
        // append B (which lands in the WAL).
        {
            let mut conn = super::SqliteConnectOptions::new()
                .filename(&path)
                .journal_mode(super::SqliteJournalMode::Wal)
                .connect()
                .await
                .unwrap();
            sqlx::query("PRAGMA wal_checkpoint(FULL)")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(20))], at(10))
            .await;

        // Copy the live .db + -wal into a fresh attempt dir and tear the WAL tail.
        let attempt2 = AttemptId::new();
        let dir2 = attempt_dir(&sink, job, attempt2);
        std::fs::create_dir_all(&dir2).unwrap();
        let name = path.file_name().unwrap();
        let path2 = dir2.join(name);
        std::fs::copy(&path, &path2).unwrap();
        let wal_src = sibling(&path, "-wal");
        if wal_src.exists() {
            let wal_dst = sibling(&path2, "-wal");
            std::fs::copy(&wal_src, &wal_dst).unwrap();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_dst)
                .unwrap();
            let len = file.metadata().unwrap().len();
            file.set_len(len.saturating_sub(8)).unwrap();
        }
        // The copy reads without error and A (committed + checkpointed) survives.
        let recovered = sink
            .metric_samples(&job, &attempt2, at(0), at(1000))
            .await
            .expect("torn-wal segment still reads");
        assert!(
            recovered.iter().any(|s| s.at == at(10)),
            "committed data survives a torn WAL tail"
        );

        // --- uncommitted is invisible ---
        let attempt3 = AttemptId::new();
        sink.append_metrics_at(&[metric(job, attempt3, alloc, at(30))], at(30))
            .await;
        let path3 = segment_files(&attempt_dir(&sink, job, attempt3))[0].clone();
        {
            let mut conn = super::SqliteConnectOptions::new()
                .filename(&path3)
                .journal_mode(super::SqliteJournalMode::Wal)
                .connect()
                .await
                .unwrap();
            let mut tx = sqlx::Connection::begin(&mut conn).await.unwrap();
            sqlx::query(
                "INSERT INTO metrics (at, allocation_id, cpu_usage_total_us, \
                 cpu_throttled_total_us, memory_used_bytes, memory_peak_bytes, \
                 disk_writable_bytes, disk_image_bytes, net_rx_bytes_total, \
                 net_tx_bytes_total, blkio_read_bytes_total, blkio_write_bytes_total) \
                 VALUES (999, 'alloc-x', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)",
            )
            .execute(&mut *tx)
            .await
            .unwrap();
            drop(tx); // rollback: never committed
        }
        let after = sink
            .metric_samples(&job, &attempt3, at(0), at(1000))
            .await
            .unwrap();
        assert_eq!(
            after.len(),
            1,
            "an uncommitted transaction is never visible"
        );
        assert_eq!(after[0].at, at(30));
    }

    // ---- 5. retention (docker-executor.md §8.4) ----------------------------

    #[tokio::test]
    async fn normal_sweep_honours_retention_from_the_marker() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_secs(60);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        sink.attempt_ended(&job, &attempt, at(100)).await.unwrap();
        // Before retention elapses: nothing deleted.
        let deleted = sink.sweep(at(150), DiskPressure::Ok).await;
        assert_eq!(deleted, 0);
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 1);
        // Past ended + retention: segments, marker, and dirs are gone.
        let deleted = sink.sweep(at(200), DiskPressure::Ok).await;
        assert_eq!(deleted, 1);
        assert!(!attempt_dir(&sink, job, attempt).exists());
    }

    #[tokio::test]
    async fn pressure_sweep_deletes_oldest_ended_first() {
        let root = TempDir::new().unwrap();
        // A long retention keeps both attempts out of the normal (mandatory) tier
        // so only the pressure tier can touch them.
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_hours(24);
        })
        .await;
        let (job, alloc) = (JobId::new(), AllocationId::new());
        let older = AttemptId::new();
        let newer = AttemptId::new();
        sink.append_metrics_at(&[metric(job, older, alloc, at(0))], at(0))
            .await;
        sink.attempt_ended(&job, &older, at(10)).await.unwrap();
        sink.append_metrics_at(&[metric(job, newer, alloc, at(0))], at(0))
            .await;
        sink.attempt_ended(&job, &newer, at(20)).await.unwrap();

        // Synthetic bytes-to-free: over the mark for the first deletion, below it
        // after — so exactly one (the oldest-ended) segment goes.
        let mut calls = 0;
        let deleted = sink
            .sweep_with(at(100), DiskPressure::High, || {
                calls += 1;
                if calls == 1 {
                    Some(ByteSize::from_mib(1))
                } else {
                    Some(ByteSize::ZERO)
                }
            })
            .await;
        assert_eq!(deleted, 1);
        assert!(
            !attempt_dir(&sink, job, older).exists(),
            "oldest-ended attempt is deleted first"
        );
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, newer)).len(),
            1,
            "the newer-ended attempt is retained once below the mark"
        );
    }

    #[tokio::test]
    async fn live_cap_deletes_closed_segments_past_the_cap() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1); // roll each append
            opts.live_retention = CoreDuration::from_secs(5);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Three segments with starts 0, 10, 20; the newest (20) stays open.
        for t in [0, 10, 20] {
            sink.append_metrics_at(&[metric(job, attempt, alloc, at(t))], at(t))
                .await;
        }
        let before = segment_files(&attempt_dir(&sink, job, attempt));
        assert_eq!(before.len(), 3);

        // now=100: seg0 (successor start 10) and seg1 (successor start 20) are
        // both past the 5s cap; the open newest (20) is never touched.
        let deleted = sink.sweep(at(100), DiskPressure::Ok).await;
        assert_eq!(deleted, 2);
        let after = segment_files(&attempt_dir(&sink, job, attempt));
        assert_eq!(after.len(), 1, "only the open newest segment remains");
    }

    #[tokio::test]
    async fn open_segment_survives_every_pressure_mode() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.live_retention = CoreDuration::from_hours(24); // no live-cap here
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        // Critical with a synthetic "always over the mark": a live attempt's open
        // segment must still survive (rule 4).
        let deleted = sink
            .sweep_with(at(1_000_000), DiskPressure::Critical, || {
                Some(ByteSize::from_mib(1))
            })
            .await;
        assert_eq!(deleted, 0);
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 1);
    }

    // ---- 7. max_log_timestamp (docker-executor.md §8.2) --------------------

    #[tokio::test]
    async fn max_log_timestamp_skips_metrics_only_and_picks_the_maximum() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1); // roll each append
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Older segment with logs, out-of-order so MAX != last inserted row.
        sink.append_logs_at(
            &[
                log(job, attempt, alloc, at(50), LogStream::Stdout, b"a"),
                log(job, attempt, alloc, at(70), LogStream::Stdout, b"b"),
                log(job, attempt, alloc, at(60), LogStream::Stdout, b"c"),
            ],
            at(50),
        )
        .await;
        // Newer, metrics-only segment (must be skipped).
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(90))], at(90))
            .await;
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            2,
            "distinct log and metrics segments"
        );
        let max = sink.max_log_timestamp(&job, &attempt).await.unwrap();
        assert_eq!(
            max,
            Some(at(70)),
            "MAX(at) across log rows, skipping metrics-only"
        );
    }

    // The resume boundary is MAX(at) across ALL segments (§8.2): a later
    // segment holding only backdated (replayed) rows must not shadow an
    // earlier segment's true maximum.
    #[tokio::test]
    async fn max_log_timestamp_is_global_across_backdated_segments() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1); // roll each append
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Segment 1 holds the true maximum (70); segment 2, created later, holds
        // only an older replayed row (60).
        sink.append_logs_at(
            &[log(job, attempt, alloc, at(70), LogStream::Stdout, b"a")],
            at(100),
        )
        .await;
        sink.append_logs_at(
            &[log(
                job,
                attempt,
                alloc,
                at(60),
                LogStream::Stdout,
                b"replay",
            )],
            at(200),
        )
        .await;
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 2);
        assert_eq!(
            sink.max_log_timestamp(&job, &attempt).await.unwrap(),
            Some(at(70)),
            "global MAX(at), not the newest log-bearing segment's"
        );
    }

    #[tokio::test]
    async fn max_log_timestamp_is_none_without_log_rows() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        assert_eq!(sink.max_log_timestamp(&job, &attempt).await.unwrap(), None);
    }

    #[tokio::test]
    async fn reading_an_unknown_attempt_is_a_typed_error() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt) = (JobId::new(), AttemptId::new());
        let err = sink
            .metric_samples(&job, &attempt, at(0), at(10))
            .await
            .expect_err("no such attempt");
        assert!(matches!(err, StoreError::UnknownAttempt { .. }));
    }

    // ---- 8. attempt_ended (docker-executor.md §8.4) ------------------------

    #[tokio::test]
    async fn attempt_ended_is_idempotent_and_records_micros() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        sink.attempt_ended(&job, &attempt, at(500)).await.unwrap();

        let marker = attempt_dir(&sink, job, attempt).join("ended");
        let content = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(content, at(500).as_micros().to_string());

        // A second end never overwrites the first marker.
        sink.attempt_ended(&job, &attempt, at(999)).await.unwrap();
        let content = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(
            content,
            at(500).as_micros().to_string(),
            "first marker wins"
        );

        // The end closed the open segment, and list_attempts reflects the marker.
        let attempts = sink.list_attempts(Some(&job)).await.unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].ended_at, Some(at(500)));
        assert_eq!(attempts[0].segments, 1);

        // After the end, a fresh append opens a *second* segment (never reopens
        // the closed one) and the new row is readable.
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(600))], at(600))
            .await;
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            2,
            "a post-end append opens a fresh segment"
        );
        let rows = sink
            .metric_samples(&job, &attempt, at(600), at(600))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "the post-end row is readable");
    }

    // A marker that cannot persist must surface as an `Err` (the attempt would
    // otherwise read as live forever), a retry after the obstruction clears
    // must succeed, and retention must then be able to reclaim the attempt.
    #[tokio::test]
    async fn attempt_ended_reports_marker_write_failure_and_retries() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_secs(60);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Obstruct the attempt *directory* path with a plain file so the marker
        // write cannot create it.
        let job_dir = sink.inner.root.join(job.to_string());
        std::fs::create_dir_all(&job_dir).unwrap();
        let obstruction = job_dir.join(attempt.to_string());
        std::fs::write(&obstruction, b"in the way").unwrap();

        sink.attempt_ended(&job, &attempt, at(500))
            .await
            .expect_err("an unpersisted marker must surface");
        // Still live: no marker was recorded.
        assert_eq!(read_ended_marker(&obstruction.clone()), None);

        // Obstruction clears; the attempt accrues data and the retry lands.
        std::fs::remove_file(&obstruction).unwrap();
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(400))], at(400))
            .await;
        sink.attempt_ended(&job, &attempt, at(500))
            .await
            .expect("retry succeeds once the obstruction clears");
        let attempts = sink.list_attempts(Some(&job)).await.unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].ended_at, Some(at(500)));

        // With the marker persisted, retention can now reclaim the attempt.
        let deleted = sink.sweep(at(561), DiskPressure::Ok).await;
        assert_eq!(deleted, 1);
        assert!(
            !attempt_dir(&sink, job, attempt).exists(),
            "the retried end unlocks retention reclaim"
        );
    }

    // ---- restart contract (docker-executor.md §8.4) ------------------------

    // A new sink over the same root (a process restart) must never reopen the
    // previous open segment for writing: the first append creates a fresh
    // segment with a strictly greater start — even under a regressed clock —
    // the old segment's rows stay untouched, and reads span both.
    #[tokio::test]
    async fn restart_opens_a_fresh_segment_and_reads_span_old_and_new() {
        let root = TempDir::new().unwrap();
        let tel_root = root.path().join("tel");
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        let sink1 = sink_with(tel_root.clone(), |_| {}).await;
        sink1
            .append_logs_at(
                &[log(
                    job,
                    attempt,
                    alloc,
                    at(70),
                    LogStream::Stdout,
                    b"before",
                )],
                at(1000),
            )
            .await;
        let dir = attempt_dir(&sink1, job, attempt);
        let seg_a = segment_files(&dir)[0].clone();
        drop(sink1); // the "crash": the writer goes away without attempt_ended

        // Restart with a clock that regressed to *before* segment A's start.
        let sink2 = sink_with(tel_root, |_| {}).await;
        sink2
            .append_logs_at(
                &[log(
                    job,
                    attempt,
                    alloc,
                    at(75),
                    LogStream::Stdout,
                    b"after",
                )],
                at(500),
            )
            .await;

        let segments = list_segments(&dir);
        assert_eq!(segments.len(), 2, "restart opens a fresh segment");
        assert_eq!(segments[0].1, seg_a, "the previous segment file remains");
        assert!(
            segments[0].0 < segments[1].0,
            "starts stay strictly increasing under a regressed clock"
        );

        // The old segment still holds exactly its original row — the new write
        // landed in the fresh segment, not in segment A.
        let mut conn = super::SqliteConnectOptions::new()
            .filename(&seg_a)
            .journal_mode(super::SqliteJournalMode::Wal)
            .connect()
            .await
            .unwrap();
        let count = sqlx::query!(r#"SELECT COUNT(*) AS "n!: i64" FROM log_chunks"#)
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .n;
        assert_eq!(count, 1, "previous segment untouched by the restart");

        // Reads merge both segments, and the resume boundary is the global max.
        let rows = sink2
            .log_chunks(
                &job,
                &attempt,
                None,
                LogQuery::Range {
                    from: at(0),
                    to: at(100),
                },
            )
            .await
            .unwrap();
        let bytes: Vec<&[u8]> = rows.iter().map(|c| c.bytes.as_ref()).collect();
        assert_eq!(bytes, vec![b"before".as_ref(), b"after".as_ref()]);
        assert_eq!(
            sink2.max_log_timestamp(&job, &attempt).await.unwrap(),
            Some(at(75))
        );
    }

    // ---- write-error isolation (docker-executor.md §8.3) -------------------

    // A failing attempt in a mixed batch is dropped and accounted; the other
    // attempt in the same batch persists — per-group flushes isolate faults.
    #[tokio::test]
    async fn write_errors_isolate_to_the_failing_attempt() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, alloc) = (JobId::new(), AllocationId::new());
        let good = AttemptId::new();
        let bad = AttemptId::new();
        // Obstruct `bad`'s attempt-directory path so its segment cannot be
        // created. `good` comes first in the batch so the error latch's final
        // state reflects the failing group.
        let job_dir = sink.inner.root.join(job.to_string());
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join(bad.to_string()), b"in the way").unwrap();

        let batch = vec![
            log(job, good, alloc, at(10), LogStream::Stdout, b"ok"),
            log(job, bad, alloc, at(10), LogStream::Stdout, b"doomed"),
        ];
        sink.append_logs_at(&batch, at(10)).await; // must not panic or bail early

        let rows = sink
            .log_chunks(
                &job,
                &good,
                None,
                LogQuery::Range {
                    from: at(0),
                    to: at(100),
                },
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "the healthy attempt's chunk persisted");
        assert_eq!(rows[0].bytes.as_ref(), b"ok");
        assert!(
            job_dir.join(bad.to_string()).is_file(),
            "the failing attempt wrote nothing"
        );
        assert!(
            sink.inner.write_error_logged.load(Ordering::Relaxed),
            "the dropped batch was accounted through the error path"
        );
    }

    // ---- retention boundary (docker-executor.md §8.4) ----------------------

    // The retention comparison is strict (`>`): an attempt is kept for its
    // full retention window and reclaimed only strictly after it — pinned here
    // so a change to `>=` shows up as a deliberate semantic choice.
    #[tokio::test]
    async fn retention_keeps_the_exact_boundary_and_reclaims_just_past_it() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_secs(60);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(0))], at(0))
            .await;
        sink.attempt_ended(&job, &attempt, at(100)).await.unwrap();

        // Exactly ended_at + retention: still kept.
        let deleted = sink.sweep(at(160), DiskPressure::Ok).await;
        assert_eq!(deleted, 0, "kept at exactly ended_at + retention");
        assert_eq!(segment_files(&attempt_dir(&sink, job, attempt)).len(), 1);

        // One second past the boundary: reclaimed.
        let deleted = sink.sweep(at(161), DiskPressure::Ok).await;
        assert_eq!(deleted, 1, "reclaimed strictly after the boundary");
        assert!(!attempt_dir(&sink, job, attempt).exists());
    }

    // ---- janitor (docker-executor.md §8.4, §9) -----------------------------

    // A pressure transition wakes the janitor immediately (the next interval
    // tick is 60s away, so a prompt deletion can only come from the watch),
    // and the task exits cleanly when the watch sender closes.
    #[tokio::test]
    async fn janitor_wakes_on_pressure_and_exits_when_the_sender_closes() {
        let root = TempDir::new().unwrap();
        // Retention far in the future: only the pressure tier can reclaim. No
        // pressure_paths means no local reading, so under High every ended
        // segment goes (disk safety wins).
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_hours(24);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // The janitor sweeps with the real clock, so anchor the attempt at
        // wall-clock now — an epoch-based synthetic time would sit 50+ years
        // past retention and be reclaimed by the very first Ok sweep.
        let now = Timestamp::now();
        sink.append_metrics_at(&[metric(job, attempt, alloc, now)], now)
            .await;
        sink.attempt_ended(&job, &attempt, now).await.unwrap();

        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let handle = spawn_retention_janitor(sink.clone(), rx);
        // Let the interval's immediate first tick sweep under Ok: no deletion.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            attempt_dir(&sink, job, attempt).exists(),
            "an Ok sweep leaves the within-retention attempt alone"
        );

        tx.send(DiskPressure::High).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while attempt_dir(&sink, job, attempt).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the pressure transition must wake the janitor well before the 60s tick"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("the janitor exits once the pressure sender closes")
            .unwrap();
    }

    // ---- read vs. sweep concurrency (docker-executor.md §8.4) --------------

    // Readers must tolerate segments vanishing under them mid-call: reads
    // racing a reclaiming sweep may see the full set, a partial set, or an
    // UnknownAttempt — never any other error. Multi-threaded so the sync
    // unlink loop genuinely overlaps the reads' awaits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_tolerate_a_concurrent_sweep() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max = ByteSize::from_bytes(1); // roll each append
            opts.retention = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        for t in 0..8 {
            sink.append_logs_at(
                &[log(job, attempt, alloc, at(t), LogStream::Stdout, b"x")],
                at(t),
            )
            .await;
        }
        sink.attempt_ended(&job, &attempt, at(10)).await.unwrap();

        let reader = {
            let sink = sink.clone();
            tokio::spawn(async move {
                for _ in 0..100 {
                    match sink
                        .log_chunks(
                            &job,
                            &attempt,
                            None,
                            LogQuery::Range {
                                from: at(0),
                                to: at(100),
                            },
                        )
                        .await
                    {
                        Ok(_) | Err(StoreError::UnknownAttempt { .. }) => {}
                        Err(err) => panic!("read failed during concurrent sweep: {err}"),
                    }
                    tokio::task::yield_now().await;
                }
            })
        };
        let sweeper = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.sweep(at(1000), DiskPressure::Ok).await })
        };
        reader.await.unwrap();
        let deleted = sweeper.await.unwrap();
        assert_eq!(deleted, 8);
        // The racing sweep's own cleanup pass may leave the attempt dir behind:
        // a reader that opened a segment just before its unlink recreates the
        // `-wal`/`-shm` by path on its first query (open_read is read-write
        // WAL). The contract is convergence, not a spotless racing pass — once
        // the readers are done, the next sweep reclaims the orphans and the
        // dir.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while attempt_dir(&sink, job, attempt).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the attempt dir must be reclaimed once the readers finish"
            );
            sink.sweep(at(1000), DiskPressure::Ok).await;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    // Journal files orphaned by a reader that died without a clean close (their
    // `seg-*.db` long gone) must not strand the attempt dir: they hold nothing
    // recoverable, so the sweep reclaims them along with the dir.
    #[tokio::test]
    async fn sweep_reclaims_orphaned_journal_files() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.retention = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        sink.append_logs_at(
            &[log(job, attempt, alloc, at(1), LogStream::Stdout, b"x")],
            at(1),
        )
        .await;
        sink.attempt_ended(&job, &attempt, at(2)).await.unwrap();
        let dir = attempt_dir(&sink, job, attempt);
        std::fs::write(dir.join("seg-00000000000000000000.db-wal"), b"stale").unwrap();
        std::fs::write(dir.join("seg-00000000000000000000.db-shm"), b"stale").unwrap();

        let deleted = sink.sweep(at(1000), DiskPressure::Ok).await;
        assert_eq!(deleted, 1);
        assert!(
            !dir.exists(),
            "orphaned journals must not strand the attempt dir"
        );
    }

    // The public `MetricsSink`/`LogSink` trait boundary (now-stamped) persists
    // and reads back — the sink absorbs its own errors, returning nothing.
    #[tokio::test]
    async fn public_sink_traits_persist_and_read_back() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let now = Timestamp::now();
        MetricsSink::append(&sink, &[metric(job, attempt, alloc, now)]).await;
        LogSink::append(
            &sink,
            &[log(job, attempt, alloc, now, LogStream::Stdout, b"hi")],
        )
        .await;
        let metrics = sink
            .metric_samples(
                &job,
                &attempt,
                Timestamp::UNIX_EPOCH,
                Timestamp::max_value(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.len(), 1);
        let logs = sink
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 8 })
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].bytes.as_ref(), b"hi");
    }

    // A stray non-id directory under the root is skipped, not fatal.
    #[tokio::test]
    async fn list_attempts_skips_unparseable_directories() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        std::fs::create_dir_all(sink.inner.root.join("not-a-job-id")).unwrap();
        let mut junk =
            std::fs::File::create(sink.inner.root.join("not-a-job-id").join("x")).unwrap();
        junk.write_all(b"junk").unwrap();
        let attempts = sink.list_attempts(None).await.unwrap();
        assert!(attempts.is_empty());
    }

    // ---- paged log query for FetchLogs (ADR 0034) --------------------------

    /// An unfiltered, uncapped query in one direction — the base most tests tweak.
    fn page_query(order: LogOrder) -> LogPageQuery {
        LogPageQuery {
            stream: None,
            from: None,
            until: None,
            order,
            resume: None,
            max_chunks: 1000,
            max_bytes: u64::MAX,
        }
    }

    /// The `at` seconds of a page's chunks, in returned order (the test fixtures
    /// stamp on whole-second boundaries via [`at`]).
    fn ats(page: &LogPage) -> Vec<i64> {
        page.chunks
            .iter()
            .map(|chunk| chunk.at.duration_since(Timestamp::UNIX_EPOCH).as_micros() / 1_000_000)
            .collect()
    }

    /// One one-byte-cap segment per second `1..=n`, each holding a single
    /// stdout chunk stamped at that second.
    async fn one_segment_per_second(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
        alloc: AllocationId,
        n: i64,
    ) {
        for t in 1..=n {
            sink.append_logs_at(
                &[log(job, attempt, alloc, at(t), LogStream::Stdout, b"x")],
                at(t),
            )
            .await;
        }
    }

    /// ADR 0034 review (segment scaling): with warm bounds — which the writer
    /// maintains for every segment it creates, so even a first read after the
    /// writes is warm — a page opens only the segment(s) that can contribute,
    /// no matter how many are retained. The one extra open is the top-k
    /// look-ahead that proves more rows exist (`want = max_chunks + 1`).
    #[tokio::test]
    async fn log_page_opens_contributing_segments_not_all_retained() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |o| {
            o.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        const SEGMENTS: i64 = 8;
        one_segment_per_second(&sink, job, attempt, alloc, SEGMENTS).await;

        // Newest-first single-row page: the newest segment plus at most one
        // look-ahead, out of eight retained.
        let before = sink.inner.segments_opened.load(Ordering::Relaxed);
        let mut query = page_query(LogOrder::Descending);
        query.max_chunks = 1;
        let page = sink.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![SEGMENTS]);
        assert!(!page.exhausted);
        let opened = sink.inner.segments_opened.load(Ordering::Relaxed) - before;
        assert!(opened <= 2, "opened {opened} of {SEGMENTS} segments");
        // Whole-attempt bounds come from the cache, not per-request probes.
        assert_eq!(page.earliest_at, Some(at(1)));
        assert_eq!(page.latest_at, Some(at(SEGMENTS)));

        // A time-window page over one old segment's range opens exactly that
        // segment: the window bounds every other candidate out.
        let before = sink.inner.segments_opened.load(Ordering::Relaxed);
        let mut query = page_query(LogOrder::Ascending);
        query.from = Some(at(3));
        query.until = Some(at(4)); // half-open [3s, 4s) → only t=3
        let page = sink.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![3]);
        assert!(page.exhausted);
        let opened = sink.inner.segments_opened.load(Ordering::Relaxed) - before;
        assert_eq!(opened, 1, "only the covering segment is opened");
    }

    /// A fresh sink handle over an existing tree (restart) probes each segment
    /// once to rebuild the bounds cache, then pages stay O(contributing).
    #[tokio::test]
    async fn log_page_cold_cache_probes_once_then_stays_bounded() {
        let root = TempDir::new().unwrap();
        let tel = root.path().join("tel");
        let writer = sink_with(tel.clone(), |o| {
            o.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        const SEGMENTS: i64 = 6;
        one_segment_per_second(&writer, job, attempt, alloc, SEGMENTS).await;

        // A separate handle shares nothing in memory with the writer.
        let reader = sink_with(tel, |_| {}).await;
        let mut query = page_query(LogOrder::Descending);
        query.max_chunks = 1;

        // Cold: every segment is probed once (index-only bounds), plus the
        // bounded page pull itself.
        let before = reader.inner.segments_opened.load(Ordering::Relaxed);
        let page = reader.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![SEGMENTS]);
        let cold = reader.inner.segments_opened.load(Ordering::Relaxed) - before;
        assert!(
            (SEGMENTS as u64..=SEGMENTS as u64 + 2).contains(&cold),
            "cold read probes each segment once (+bounded pull), opened {cold}"
        );

        // Warm: the cache carries the bounds; only the contributing tail opens.
        let before = reader.inner.segments_opened.load(Ordering::Relaxed);
        let page = reader.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![SEGMENTS]);
        let warm = reader.inner.segments_opened.load(Ordering::Relaxed) - before;
        assert!(warm <= 2, "warm read opened {warm} of {SEGMENTS}");
    }

    /// The writer's post-commit bounds notes keep the cache fresh: rows landed
    /// after the cache was built are served without re-probing the tree.
    #[tokio::test]
    async fn log_page_writer_notes_keep_warm_cache_fresh() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |o| {
            o.segment_max = ByteSize::from_bytes(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        one_segment_per_second(&sink, job, attempt, alloc, 4).await;

        let mut query = page_query(LogOrder::Descending);
        query.max_chunks = 1;
        let page = sink.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![4]);

        // A new row in a new segment after the cache is warm.
        sink.append_logs_at(
            &[log(job, attempt, alloc, at(5), LogStream::Stdout, b"x")],
            at(5),
        )
        .await;
        let before = sink.inner.segments_opened.load(Ordering::Relaxed);
        let page = sink.log_page(&job, &attempt, &query).await.unwrap();
        assert_eq!(ats(&page), vec![5], "the freshly written row is visible");
        let opened = sink.inner.segments_opened.load(Ordering::Relaxed) - before;
        assert!(opened <= 2, "no re-probe of old segments, opened {opened}");
    }

    #[tokio::test]
    async fn log_page_walks_both_directions_and_reports_bounds() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let batch: Vec<LogChunk> = (1..=3)
            .map(|s| log(job, attempt, alloc, at(s), LogStream::Stdout, b"x"))
            .collect();
        sink.append_logs_at(&batch, at(10)).await;

        let asc = sink
            .log_page(&job, &attempt, &page_query(LogOrder::Ascending))
            .await
            .unwrap();
        assert_eq!(ats(&asc), vec![1, 2, 3]);
        assert!(asc.exhausted, "the whole window fit");
        assert_eq!(asc.earliest_at, Some(at(1)));
        assert_eq!(asc.latest_at, Some(at(3)));

        let desc = sink
            .log_page(&job, &attempt, &page_query(LogOrder::Descending))
            .await
            .unwrap();
        assert_eq!(ats(&desc), vec![3, 2, 1], "descending is the reverse walk");
        // Bounds span the whole attempt regardless of direction.
        assert_eq!(desc.earliest_at, Some(at(1)));
        assert_eq!(desc.latest_at, Some(at(3)));
    }

    #[tokio::test]
    async fn log_page_resume_is_exclusive_across_chunks_at_one_microsecond() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Three chunks at exactly t=5 (a, b, c) plus one at t=6, one batch so the
        // insertion order at t=5 is a<b<c.
        let batch = vec![
            log(job, attempt, alloc, at(5), LogStream::Stdout, b"a"),
            log(job, attempt, alloc, at(5), LogStream::Stdout, b"b"),
            log(job, attempt, alloc, at(5), LogStream::Stdout, b"c"),
            log(job, attempt, alloc, at(6), LogStream::Stdout, b"d"),
        ];
        sink.append_logs_at(&batch, at(10)).await;

        let payloads = |page: &LogPage| -> Vec<Vec<u8>> {
            page.chunks.iter().map(|c| c.bytes.to_vec()).collect()
        };

        // Ascending resume at t=5 skipping 1 (consumed `a`) resumes strictly
        // after it: b, c, then d.
        let mut q = page_query(LogOrder::Ascending);
        q.resume = Some(ResumeAt { at: at(5), skip: 1 });
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(
            payloads(&page),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );

        // skip = 3 consumes the whole t=5 run; only d remains.
        q.resume = Some(ResumeAt { at: at(5), skip: 3 });
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(payloads(&page), vec![b"d".to_vec()]);
        assert!(page.exhausted);

        // Descending walk is c, b, a, then d is already past; resume at t=6
        // skip 1 consumes d, resuming at c, b, a.
        let mut qd = page_query(LogOrder::Descending);
        qd.resume = Some(ResumeAt { at: at(6), skip: 1 });
        let page = sink.log_page(&job, &attempt, &qd).await.unwrap();
        assert_eq!(
            payloads(&page),
            vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()],
            "descending resume drops the t=6 run, then walks t=5 newest-insertion-first reversed"
        );

        // Descending resume within the t=5 run: skip 1 (consumed `c`) → b, a.
        qd.resume = Some(ResumeAt { at: at(5), skip: 1 });
        let page = sink.log_page(&job, &attempt, &qd).await.unwrap();
        assert_eq!(payloads(&page), vec![b"b".to_vec(), b"a".to_vec()]);
    }

    #[tokio::test]
    async fn log_page_range_is_half_open() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let batch: Vec<LogChunk> = (1..=4)
            .map(|s| log(job, attempt, alloc, at(s), LogStream::Stdout, b"x"))
            .collect();
        sink.append_logs_at(&batch, at(10)).await;

        let mut q = page_query(LogOrder::Ascending);
        q.from = Some(at(2));
        q.until = Some(at(4));
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![2, 3], "from inclusive, until exclusive");
        // Bounds still reflect the full attempt, so a `from` past the earliest
        // signals head truncation to the caller.
        assert_eq!(page.earliest_at, Some(at(1)));
        assert_eq!(page.latest_at, Some(at(4)));
    }

    #[tokio::test]
    async fn log_page_byte_cap_short_page_and_always_progresses() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Three 4-byte chunks.
        let batch: Vec<LogChunk> = (1..=3)
            .map(|s| log(job, attempt, alloc, at(s), LogStream::Stdout, b"abcd"))
            .collect();
        sink.append_logs_at(&batch, at(10)).await;

        // A 6-byte cap fits one 4-byte chunk; a second (total 8) would exceed it.
        let mut q = page_query(LogOrder::Ascending);
        q.max_bytes = 6;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![1], "stops before exceeding max_bytes");
        assert!(!page.chunks[0].truncated, "a fitting first chunk is whole");
        assert!(!page.exhausted, "a cap cut the page short");

        // A cap below even one chunk still makes progress, but the oversized
        // first chunk is TRUNCATED to the budget and flagged — never returned
        // whole in violation of the hard cap (ADR 0034, the bypass fix).
        q.max_bytes = 1;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![1], "the first chunk still makes progress");
        assert_eq!(
            page.chunks[0].bytes.as_ref(),
            b"a",
            "payload cut to the 1-byte budget, not served whole"
        );
        assert!(page.chunks[0].truncated, "and flagged as truncated");
        assert!(!page.exhausted, "two whole chunks still remain");
    }

    #[tokio::test]
    async fn log_page_max_chunks_cap_and_exhausted_flag() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let batch: Vec<LogChunk> = (1..=3)
            .map(|s| log(job, attempt, alloc, at(s), LogStream::Stdout, b"x"))
            .collect();
        sink.append_logs_at(&batch, at(10)).await;

        let mut q = page_query(LogOrder::Ascending);
        q.max_chunks = 2;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![1, 2]);
        assert!(!page.exhausted, "a third chunk remains");

        // The follow-up page from the cursor position exhausts the walk.
        q.resume = Some(ResumeAt { at: at(2), skip: 1 });
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![3]);
        assert!(page.exhausted, "the walk reached the end");
    }

    #[tokio::test]
    async fn log_page_filters_by_stream() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let batch = vec![
            log(job, attempt, alloc, at(1), LogStream::Stdout, b"out"),
            log(job, attempt, alloc, at(2), LogStream::Stderr, b"err"),
            log(job, attempt, alloc, at(3), LogStream::Stdout, b"out2"),
        ];
        sink.append_logs_at(&batch, at(10)).await;

        let mut q = page_query(LogOrder::Ascending);
        q.stream = Some(LogStream::Stderr);
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(ats(&page), vec![2], "only the stderr chunk");
        // Bounds reflect the filtered stream: the only stderr chunk is its own
        // earliest and latest.
        assert_eq!(page.earliest_at, Some(at(2)));
        assert_eq!(page.latest_at, Some(at(2)));
    }

    #[tokio::test]
    async fn log_page_probes_every_segment_including_backdated_rows() {
        let root = TempDir::new().unwrap();
        // A 1s age bound forces the second append to roll to a fresh segment.
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // Segment 1 (created at now=100) holds a row at t=100.
        sink.append_logs_at(
            &[log(
                job,
                attempt,
                alloc,
                at(100),
                LogStream::Stdout,
                b"newer",
            )],
            at(100),
        )
        .await;
        // Segment 2 (created at now=200, so it rolls) holds a row at t=50 —
        // OLDER than segment 1's filename start (the §8.2 replay caveat).
        sink.append_logs_at(
            &[log(
                job,
                attempt,
                alloc,
                at(50),
                LogStream::Stdout,
                b"older",
            )],
            at(200),
        )
        .await;
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            2,
            "the second append rolled to a fresh segment"
        );

        let page = sink
            .log_page(&job, &attempt, &page_query(LogOrder::Ascending))
            .await
            .unwrap();
        assert_eq!(
            ats(&page),
            vec![50, 100],
            "the backdated row in the newer segment is found and ordered by at"
        );
        assert_eq!(page.earliest_at, Some(at(50)));
        assert_eq!(page.latest_at, Some(at(100)));
    }

    #[tokio::test]
    async fn log_page_unknown_attempt_when_no_directory() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt) = (JobId::new(), AttemptId::new());
        let err = sink
            .log_page(&job, &attempt, &page_query(LogOrder::Ascending))
            .await
            .expect_err("an attempt with no telemetry directory is unknown");
        assert!(matches!(err, StoreError::UnknownAttempt { .. }));
    }

    #[tokio::test]
    async fn log_page_empty_when_directory_exists_but_holds_no_logs() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        // A metrics-only attempt: the directory exists, but there are no log
        // rows — a valid empty page, NOT UnknownAttempt.
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(1))], at(10))
            .await;
        let page = sink
            .log_page(&job, &attempt, &page_query(LogOrder::Ascending))
            .await
            .unwrap();
        assert!(page.chunks.is_empty());
        assert!(page.exhausted, "an empty walk is exhausted");
        assert_eq!(page.earliest_at, None);
        assert_eq!(page.latest_at, None);
    }

    // An oversized first chunk is truncated to the byte budget and flagged, and
    // the resume cursor advances past it *whole* — the next page continues at the
    // following chunk, never re-serving the dropped tail (ADR 0034 bypass fix).
    #[tokio::test]
    async fn log_page_truncates_oversized_first_chunk_and_resume_continues_past_it() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let batch = vec![
            log(job, attempt, alloc, at(1), LogStream::Stdout, b"0123456789"),
            log(job, attempt, alloc, at(2), LogStream::Stdout, b"x"),
            log(job, attempt, alloc, at(3), LogStream::Stdout, b"y"),
        ];
        sink.append_logs_at(&batch, at(10)).await;

        // A 4-byte cap is smaller than the 10-byte first chunk: it comes back cut
        // to 4 bytes and flagged, the page ends there (more remains).
        let mut q = page_query(LogOrder::Ascending);
        q.max_bytes = 4;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        assert_eq!(page.chunks.len(), 1);
        assert_eq!(
            page.chunks[0].bytes.as_ref(),
            b"0123",
            "payload cut to the 4-byte budget"
        );
        assert!(page.chunks[0].truncated, "and flagged truncated");
        assert!(!page.exhausted, "the page stopped short with more to come");

        // The cursor advances past the whole chunk: resume at (at(1), skip=1) —
        // one chunk consumed at that microsecond — yields the *following* chunks,
        // never the dropped tail of the truncated one.
        q.resume = Some(ResumeAt { at: at(1), skip: 1 });
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        let payloads: Vec<&[u8]> = page.chunks.iter().map(|c| c.bytes.as_ref()).collect();
        assert_eq!(
            payloads,
            vec![b"x".as_ref(), b"y".as_ref()],
            "resume continues at the next chunk, tail of the truncated chunk is gone"
        );
        assert!(page.chunks.iter().all(|c| !c.truncated));
        assert!(page.exhausted);
    }

    // A small page over a many-thousand-row, multi-segment attempt must decode
    // only a bounded handful of rows — the caps bound the *input* scan, not just
    // the returned page (ADR 0034 review: a 256 MiB segment must never be fully
    // materialized to serve `limit=1`).
    #[tokio::test]
    async fn log_page_bounds_the_store_read_not_just_the_returned_page() {
        let root = TempDir::new().unwrap();
        // A 1s age bound rolls a fresh segment on each append batch.
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        // Three segments, ~2000 rows each (6000 total), disjoint ascending `at`
        // ranges matching creation order so the global newest lives in the last.
        const PER_SEGMENT: i64 = 2000;
        const SEGMENTS: i64 = 3;
        for seg in 0..SEGMENTS {
            let base = seg * PER_SEGMENT;
            let batch: Vec<LogChunk> = (0..PER_SEGMENT)
                .map(|i| log(job, attempt, alloc, at(base + i), LogStream::Stdout, b"x"))
                .collect();
            // A creation `now` far in the future of every event time and 10s past
            // the previous segment, so the age bound rolls a new segment each time.
            sink.append_logs_at(&batch, at(1_000_000 + seg * 10)).await;
        }
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            SEGMENTS as usize,
            "one segment per rolled batch"
        );

        // Measure only the decode work of a single `limit=1` newest-first page.
        sink.inner.rows_decoded.store(0, Ordering::Relaxed);
        let mut q = page_query(LogOrder::Descending);
        q.max_chunks = 1;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        let decoded = sink.inner.rows_decoded.load(Ordering::Relaxed);

        // Correctness: the one returned chunk is the global newest, and the
        // whole-attempt bounds are exact (from the cheap MIN/MAX probes).
        assert_eq!(page.chunks.len(), 1);
        assert_eq!(page.chunks[0].at, at(SEGMENTS * PER_SEGMENT - 1));
        assert_eq!(page.earliest_at, Some(at(0)));
        assert_eq!(page.latest_at, Some(at(SEGMENTS * PER_SEGMENT - 1)));

        // Boundedness: with `max_chunks = 1` and no resume, each segment reads at
        // most `1 + 1` rows (the page row plus the single exhaustion-lookahead
        // row), so the whole 6000-row attempt costs at most `2 * SEGMENTS` decoded
        // rows — a handful, not the full store.
        let total_rows = (SEGMENTS * PER_SEGMENT) as u64;
        let ceiling = 2 * SEGMENTS as u64;
        assert!(
            decoded <= ceiling,
            "decoded {decoded} rows for a limit=1 page; expected <= {ceiling} \
             (a bounded handful across {SEGMENTS} segments), far below the \
             {total_rows} stored"
        );
    }

    // A cursor `skip` near `u64::MAX` must never re-enable payload decoding: the
    // boundary microsecond is skipped through index-only `COUNT`s, so a populated
    // multi-segment attempt with thousands of chunks at that microsecond costs
    // *zero* payload decode there, and only the rows strictly beyond it are
    // served (ADR 0034 review: the input bound must be independent of the
    // untrusted, unsigned cursor).
    #[tokio::test]
    async fn log_page_adversarial_large_skip_decodes_nothing_at_the_boundary() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        // Three segments, each ~2000 chunks all at exactly t=5 (6000 at the one
        // boundary microsecond), plus a lone chunk strictly beyond at t=6.
        const PER_SEGMENT: i64 = 2000;
        const SEGMENTS: i64 = 3;
        for seg in 0..SEGMENTS {
            let mut batch: Vec<LogChunk> = (0..PER_SEGMENT)
                .map(|_| log(job, attempt, alloc, at(5), LogStream::Stdout, b"B"))
                .collect();
            if seg == SEGMENTS - 1 {
                batch.push(log(
                    job,
                    attempt,
                    alloc,
                    at(6),
                    LogStream::Stdout,
                    b"beyond",
                ));
            }
            sink.append_logs_at(&batch, at(1_000_000 + seg * 10)).await;
        }
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            SEGMENTS as usize
        );

        // Ascending, resume at the boundary with a skip near u64::MAX: the whole
        // t=5 run is consumed, so only the lone t=6 chunk survives.
        sink.inner.rows_decoded.store(0, Ordering::Relaxed);
        sink.inner.bytes_materialized.store(0, Ordering::Relaxed);
        let mut q = page_query(LogOrder::Ascending);
        q.max_chunks = 10;
        q.resume = Some(ResumeAt {
            at: at(5),
            skip: u64::MAX,
        });
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        let decoded = sink.inner.rows_decoded.load(Ordering::Relaxed);
        let bytes = sink.inner.bytes_materialized.load(Ordering::Relaxed);

        // Correct: the boundary is fully skipped, the beyond chunk is served whole.
        assert_eq!(
            page.chunks
                .iter()
                .map(|c| c.bytes.to_vec())
                .collect::<Vec<_>>(),
            vec![b"beyond".to_vec()]
        );
        assert!(page.exhausted);
        assert!(page.chunks.iter().all(|c| !c.truncated));

        // Bounded: the 6000 boundary rows decoded NOTHING (COUNTs only); the sole
        // decode is the single beyond chunk — exactly one row, six bytes — wholly
        // independent of the u64::MAX skip. The boundary's 6000 stored bytes never
        // materialized.
        assert_eq!(
            decoded, 1,
            "only the lone beyond row decoded; the 6000 boundary rows cost only COUNTs"
        );
        assert_eq!(bytes, 6, "only the 6-byte beyond payload materialized");
    }

    // Thousands of chunks sharing ONE microsecond across segments, paged through
    // with real cursors whose `skip` accumulates page by page. Each page's decode
    // and byte materialization stay bounded — independent of the growing skip —
    // while pagination is loss-free and correctly ordered (ADR 0034 review: a
    // legitimate cursor can accumulate a large skip within one microsecond).
    #[tokio::test]
    async fn log_page_accumulated_skip_stays_bounded_and_lossless() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        // Three segments, every chunk at exactly t=5. Payloads are the global walk
        // index (ascending walk = segment order then insertion order), so the
        // paged-out concatenation must be exactly 0..TOTAL.
        const PER_SEGMENT: u32 = 1500;
        const SEGMENTS: u32 = 3;
        const TOTAL: u32 = PER_SEGMENT * SEGMENTS;
        for seg in 0..SEGMENTS {
            let batch: Vec<LogChunk> = (0..PER_SEGMENT)
                .map(|i| {
                    let idx = seg * PER_SEGMENT + i;
                    log(
                        job,
                        attempt,
                        alloc,
                        at(5),
                        LogStream::Stdout,
                        &idx.to_be_bytes(),
                    )
                })
                .collect();
            sink.append_logs_at(&batch, at(1_000_000 + seg as i64 * 10))
                .await;
        }
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            SEGMENTS as usize
        );

        const PAGE: usize = 100;
        let want = PAGE as u64 + 1;
        // Per page: at most `want` boundary survivors (total, however many
        // segments they span) plus a `want`-capped beyond read per segment.
        let ceiling = (1 + SEGMENTS as u64) * want;
        let mut collected: Vec<u32> = Vec::new();
        let mut skip = 0u64;
        loop {
            sink.inner.rows_decoded.store(0, Ordering::Relaxed);
            sink.inner.bytes_materialized.store(0, Ordering::Relaxed);
            let mut q = page_query(LogOrder::Ascending);
            q.max_chunks = PAGE;
            q.resume = Some(ResumeAt { at: at(5), skip });
            let page = sink.log_page(&job, &attempt, &q).await.unwrap();
            let decoded = sink.inner.rows_decoded.load(Ordering::Relaxed);
            let bytes = sink.inner.bytes_materialized.load(Ordering::Relaxed);

            assert!(
                decoded <= ceiling,
                "page at skip={skip} decoded {decoded} rows; expected <= {ceiling} \
                 (bounded regardless of the accumulated skip)"
            );
            assert!(
                bytes <= ceiling * 4,
                "page at skip={skip} materialized {bytes} bytes; expected <= {} \
                 (each of the 4-byte rows projection-bounded)",
                ceiling * 4
            );

            for c in &page.chunks {
                collected.push(u32::from_be_bytes(c.bytes.as_ref().try_into().unwrap()));
            }
            skip += page.chunks.len() as u64;
            if page.exhausted {
                break;
            }
            assert!(
                !page.chunks.is_empty(),
                "a non-exhausted page must make progress"
            );
        }

        // Loss-free and correctly ordered: exactly 0..TOTAL, in order, no gaps or
        // duplicates.
        assert_eq!(
            collected.len(),
            TOTAL as usize,
            "every chunk returned exactly once across the paged walk"
        );
        assert!(
            collected.iter().copied().eq(0..TOTAL),
            "the paged walk reconstructs the in-order sequence with no gaps or dups"
        );
    }

    // A multi-MiB stored chunk paged with a small `max_bytes` must NOT materialize
    // the full row: the `substr` projection caps input bytes to the budget, and
    // the response is truncated to the budget and flagged, exactly as the byte-cap
    // contract requires (ADR 0034 review: bound payload projection before
    // materialization).
    #[tokio::test]
    async fn log_page_oversized_row_materializes_only_the_budget() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        const HUGE: usize = 4 * 1024 * 1024; // 4 MiB
        let huge = vec![b'Z'; HUGE];
        let batch = vec![
            log(job, attempt, alloc, at(1), LogStream::Stdout, &huge),
            log(job, attempt, alloc, at(2), LogStream::Stdout, b"tail"),
        ];
        sink.append_logs_at(&batch, at(10)).await;

        const BUDGET: u64 = 1024; // 1 KiB
        sink.inner.rows_decoded.store(0, Ordering::Relaxed);
        sink.inner.bytes_materialized.store(0, Ordering::Relaxed);
        let mut q = page_query(LogOrder::Ascending);
        q.max_bytes = BUDGET;
        q.max_chunks = 10;
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        let bytes = sink.inner.bytes_materialized.load(Ordering::Relaxed);

        // The response honors the truncation contract: the oversized chunk is cut
        // to the budget and flagged; the page stops there (more remains).
        assert_eq!(page.chunks.len(), 1);
        assert_eq!(
            page.chunks[0].bytes.len(),
            BUDGET as usize,
            "cut to the byte budget"
        );
        assert!(page.chunks[0].bytes.iter().all(|&b| b == b'Z'));
        assert!(page.chunks[0].truncated, "and flagged truncated");
        assert!(!page.exhausted, "the tail chunk still remains");

        // Bounded: the 4 MiB row never fully materialized. Every projected row is
        // capped at BUDGET+1 bytes, so the whole page materialized only a few KiB,
        // not 4 MiB.
        let ceiling = (BUDGET + 1) * 4;
        assert!(
            bytes <= ceiling,
            "materialized {bytes} bytes for a 4 MiB stored row; expected <= {ceiling} \
             (projection bounded to the byte budget, not the stored size)"
        );
        assert!(
            (bytes as usize) < HUGE,
            "materialized {bytes} bytes, far below the {HUGE}-byte stored payload"
        );
    }

    // The per-segment window is pulled with a byte-based early cutoff, not an
    // eager `LIMIT max_chunks+1` fetch: a small `max_bytes` over an attempt of
    // ~1000 medium rows across several segments must materialize only ~`max_bytes`
    // per segment — O(segments × max_bytes) — NOT `max_chunks × max_bytes` per
    // segment (ADR 0034 review: the LIMIT window would otherwise pull ~1001
    // budget-sized rows per segment). `max_chunks` is set far above the row count
    // so the byte budget is the sole binding cap.
    #[tokio::test]
    async fn log_page_window_pull_is_byte_bounded_per_segment_not_row_bounded() {
        let root = TempDir::new().unwrap();
        // A 1s age bound rolls a fresh segment on each append batch.
        let sink = sink_with(root.path().join("tel"), |opts| {
            opts.segment_max_age = CoreDuration::from_secs(1);
        })
        .await;
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());

        // Two segments, 500 rows each (1000 total), every row 64 KiB, disjoint
        // ascending `at` ranges matching creation order (~64 MiB stored).
        const ROW: usize = 64 * 1024;
        const PER_SEGMENT: i64 = 500;
        const SEGMENTS: i64 = 2;
        const BUDGET: u64 = 64 * 1024; // small page byte budget = one row
        for seg in 0..SEGMENTS {
            let base = seg * PER_SEGMENT;
            let payload = vec![b'x'; ROW];
            let batch: Vec<LogChunk> = (0..PER_SEGMENT)
                .map(|i| {
                    log(
                        job,
                        attempt,
                        alloc,
                        at(base + i),
                        LogStream::Stdout,
                        &payload,
                    )
                })
                .collect();
            sink.append_logs_at(&batch, at(1_000_000 + seg * 10)).await;
        }
        assert_eq!(
            segment_files(&attempt_dir(&sink, job, attempt)).len(),
            SEGMENTS as usize,
            "one segment per rolled batch"
        );

        sink.inner.rows_decoded.store(0, Ordering::Relaxed);
        sink.inner.bytes_materialized.store(0, Ordering::Relaxed);
        let mut q = page_query(LogOrder::Ascending);
        q.max_bytes = BUDGET;
        q.max_chunks = 10_000; // far above the 1000 stored rows: the byte cap binds
        let page = sink.log_page(&job, &attempt, &q).await.unwrap();
        let decoded = sink.inner.rows_decoded.load(Ordering::Relaxed);
        let bytes = sink.inner.bytes_materialized.load(Ordering::Relaxed);

        // Correct: the 64 KiB budget fits exactly one 64 KiB chunk (the global
        // oldest), it is returned whole and un-truncated, and the page reports a
        // further page exists. Whole-attempt bounds are exact.
        assert_eq!(
            page.chunks.len(),
            1,
            "one 64 KiB chunk fills the 64 KiB budget"
        );
        assert_eq!(page.chunks[0].at, at(0), "the global oldest, ascending");
        assert_eq!(
            page.chunks[0].bytes.len(),
            ROW,
            "returned whole, within budget"
        );
        assert!(
            !page.chunks[0].truncated,
            "a within-budget chunk is not truncated"
        );
        assert!(!page.exhausted, "999 rows remain");
        assert_eq!(page.earliest_at, Some(at(0)));
        assert_eq!(page.latest_at, Some(at(SEGMENTS * PER_SEGMENT - 1)));

        // The net property: bytes materialized is O(segments × max_bytes), NOT
        // O(segments × max_chunks × max_bytes). Each segment streams only until
        // its projected bytes reach the budget (one row) plus a single look-ahead
        // row, so it materializes ~2 × BUDGET; across the two segments that is
        // ~4 × BUDGET, a couple hundred KiB — while an eager `LIMIT max_chunks+1`
        // pull would have materialized all 1000 rows (~64 MiB).
        let ceiling = SEGMENTS as u64 * 2 * (BUDGET + 1);
        let stored = (SEGMENTS * PER_SEGMENT) as u64 * ROW as u64;
        assert!(
            bytes <= ceiling,
            "materialized {bytes} bytes for a {BUDGET}-byte page over {SEGMENTS} \
             segments; expected <= {ceiling} (~segments × 2 × max_bytes), NOT the \
             per-segment max_chunks × max_bytes an eager LIMIT window would pull"
        );
        assert!(
            bytes < stored / 100,
            "materialized {bytes} bytes, orders of magnitude below the {stored} \
             bytes stored across the 1000 rows"
        );
        // A bounded handful of rows decoded (two per segment), not the full store.
        assert!(
            decoded <= SEGMENTS as u64 * 2,
            "decoded {decoded} rows; expected <= {} (a look-ahead pair per segment)",
            SEGMENTS as u64 * 2
        );
    }
}
