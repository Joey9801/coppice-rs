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

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
    /// Warn-once latch for the write-error path (pressure.rs style): the first
    /// failure of a streak logs at error, subsequent ones only bump the counter,
    /// and a success resets it — so a wedged disk is metered, not a log flood.
    write_error_logged: AtomicBool,
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
                write_error_logged: AtomicBool::new(false),
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
/// the `ended` marker, then the now-empty attempt and job directories
/// (docker-executor.md §8.4). Whole-file/dir unlinks only; every error is
/// tolerated (a non-empty dir simply stays).
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
                    let _ = std::fs::remove_dir(&attempt_dir);
                }
            }
        }
        let _ = std::fs::remove_dir(&job_dir);
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

        // After the end, a fresh append opens a new segment (never reopens the
        // closed one).
        sink.append_metrics_at(&[metric(job, attempt, alloc, at(600))], at(600))
            .await;
        assert!(!segment_files(&attempt_dir(&sink, job, attempt)).is_empty());
    }

    // A marker that cannot persist must surface as an `Err` (the attempt would
    // otherwise read as live forever), and a retry after the obstruction clears
    // must succeed.
    #[tokio::test]
    async fn attempt_ended_reports_marker_write_failure_and_retries() {
        let root = TempDir::new().unwrap();
        let sink = sink_with(root.path().join("tel"), |_| {}).await;
        let (job, attempt) = (JobId::new(), AttemptId::new());
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

        std::fs::remove_file(&obstruction).unwrap();
        sink.attempt_ended(&job, &attempt, at(500))
            .await
            .expect("retry succeeds once the obstruction clears");
        let attempts = sink.list_attempts(Some(&job)).await.unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].ended_at, Some(at(500)));
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
}
