//! The image cache manager (docker-executor.md §7).
//!
//! All pulls funnel through here (per-reference singleflight + a global
//! concurrent-pull limit), the manager pins images in use, evicts idle ones,
//! and owns the `ImageCacheInventory` snapshot `cache_inventory()` reports for
//! heartbeats (ADR 0010: the agent owns eviction absolutely; cache state is
//! observed, never replicated).
//!
//! [`ImageCache`] is a cheap `Clone` handle over a shared [`CacheInner`], the
//! same idiom as [`super::DockerExecutor`]/[`super::Inner`]. The janitor task
//! captures only an [`ImageCache`] clone (plus a pressure receiver), never an
//! `Arc<super::Inner>` — the no-cycle rule mod.rs documents — so `Inner::drop`
//! aborting the janitor handle is what actually stops it.
//!
//! **Pins are in-memory only, never persisted, and restart recovery is
//! deliberately partial** (§7): running/exited containers re-pin from their
//! `coppice.image-digest` label, "but a pre-start pin has no container and the
//! journaled `StartIntent` carries no image identity — and it doesn't need to.
//! The epoch bump already fenced every pending intent … so the pin is simply
//! re-established when the re-delivered `StartJob` arrives with its image
//! reference." The between-registration-and-re-delivery window can at worst
//! evict an image that must be re-pulled — latency, never correctness, exactly
//! ADR 0010's contract. Do **not** try to "fix" that gap here.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bollard::models::ImageInspect;
use bollard::query_parameters::{
    CreateImageOptionsBuilder, ListContainersOptionsBuilder, ListImagesOptionsBuilder,
    RemoveImageOptionsBuilder,
};
use bollard::Docker;
use tokio::sync::{watch, Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;

use super::{api, classify, LABEL_ALLOCATION, LABEL_IMAGE_DIGEST};
use crate::config::ImageCacheConfig;
use crate::executor::StartError;
use crate::pressure::DiskPressure;

// ---- metrics (docker-executor.md §8.1) ----------------------------------

/// Distinct images the cache currently tracks. A gauge, *pushed* at every
/// mutation of the entries map (the `push_running_gauge` precedent in mod.rs)
/// rather than sampled, so the value never lags the map.
const AGENT_CACHED_IMAGES: &str = "agent_cached_images";
/// Summed on-disk size of the tracked images. Pushed alongside
/// [`AGENT_CACHED_IMAGES`] on the same entries-map transitions.
const AGENT_CACHED_IMAGE_BYTES: &str = "agent_cached_image_bytes";
/// Image pulls actually performed (a follower that finds the image present
/// never pulls, so this counts real network fetches, not `fetch` calls).
const AGENT_IMAGE_PULLS_TOTAL: &str = "agent_image_pulls_total";
/// Wall-clock duration of one pull's `create_image` drain (§7). A histogram so
/// an operator watches the distribution, not just a mean.
const AGENT_IMAGE_PULL_DURATION: &str = "agent_image_pull_duration_seconds";
/// Evictions, labelled by `reason` ([`REASON_TTL`]/[`REASON_PRESSURE`]/
/// [`REASON_HINT`]).
const AGENT_IMAGE_EVICTIONS_TOTAL: &str = "agent_image_evictions_total";

/// The `reason` label for an idle-past-TTL eviction (§7).
const REASON_TTL: &str = "ttl";
/// The `reason` label for an ahead-of-TTL eviction under disk pressure (§7/§9).
const REASON_PRESSURE: &str = "pressure";
/// The `reason` label for a coordinator `EvictImageHint` (§7, ADR 0010).
const REASON_HINT: &str = "hint";

/// Register this module's metric names (docker-executor.md §8.1). Part of the
/// docker module's `describe_metrics` fan-out.
pub(crate) fn describe_metrics() {
    metrics::describe_gauge!(
        AGENT_CACHED_IMAGES,
        metrics::Unit::Count,
        "Distinct images currently tracked by the agent's image cache (§7)."
    );
    metrics::describe_gauge!(
        AGENT_CACHED_IMAGE_BYTES,
        metrics::Unit::Bytes,
        "Summed on-disk size of the images the agent's cache tracks (§7)."
    );
    metrics::describe_counter!(
        AGENT_IMAGE_PULLS_TOTAL,
        metrics::Unit::Count,
        "Image pulls the cache manager actually performed (§7)."
    );
    metrics::describe_histogram!(
        AGENT_IMAGE_PULL_DURATION,
        metrics::Unit::Seconds,
        "Wall-clock duration of one image pull (§7)."
    );
    metrics::describe_counter!(
        AGENT_IMAGE_EVICTIONS_TOTAL,
        metrics::Unit::Count,
        "Images evicted by the cache manager, by reason (§7)."
    );
}

/// Point-in-time sampling for this module. A no-op: the two gauges are *pushed*
/// on every entries-map mutation (the mod.rs push-on-transition convention) and
/// the counters/histogram are pushed at their events, so there is nothing to
/// sample here. The same shape as `disk::gather_metrics`.
pub(crate) fn gather_metrics() {}

// ---- construction (docker-executor.md §7) -------------------------------

/// How often the janitor runs a full reconcile + eviction sweep even absent a
/// pressure transition (§11). A const 60s, mirroring `pressure.rs`'s
/// `SAMPLE_INTERVAL`: the cadence only gates coarse, self-correcting eviction,
/// so a fixed value keeps the surface small. A `High` pressure transition wakes
/// the janitor immediately (see [`spawn_janitor`]); tests drive one sweep
/// deterministically through the `cache_sweep_at` seam instead.
const JANITOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Everything [`ImageCache::new`] needs beyond the daemon client and the shared
/// pressure signal (docker-executor.md §7). Built by `run_daemon` and the test
/// harness and handed to [`super::DockerExecutor::new`].
pub struct CacheOptions {
    /// TTL and the global concurrent-pull limit (§7, §10).
    pub config: ImageCacheConfig,
    /// `data_dir/image-cache.json`; `None` = in-memory only (tests). The file is
    /// lossy-OK by design (§7), so it never rides the consensus `Fs` seam.
    pub state_path: Option<PathBuf>,
    /// Paths whose `statvfs` feeds the `High`-pressure byte target — the same
    /// paths `run_daemon` hands the pressure monitor (§9). Empty = no local
    /// reading (a remote daemon), which makes a `High` sweep drop every unpinned
    /// image (disk safety wins, ADR 0010).
    pub pressure_paths: Vec<PathBuf>,
    /// `pressure.high_pct` — the high-water mark a `High` sweep evicts below.
    pub high_pct: u8,
}

/// The result of resolving a job's image reference (§7). `inspect` is the fully
/// resolved image (bytes, config, size); `digest` is the cache key and the
/// `coppice.image-digest` label value, computed by the shared [`digest_of`] so
/// label and key can never diverge.
#[derive(Clone)]
pub(crate) struct FetchedImage {
    pub(crate) inspect: ImageInspect,
    pub(crate) digest: String,
}

/// One tracked image. Held in memory as typed [`ByteSize`]/[`Timestamp`]; the
/// on-disk JSON uses the plain `u64`/`i64` wire spellings ([`SerEntry`]).
#[derive(Clone)]
struct Entry {
    /// The image `id` — what eviction's `remove_image` and create pin to (the
    /// movable tag is deliberately never used, §7).
    id: String,
    /// The digest key (first `repo_digests` entry, else `id`; [`digest_of`]).
    digest: String,
    /// On-disk size, clamped at zero like `lifecycle.rs` does.
    size: ByteSize,
    /// End of the last attempt that used the image, or the pull/adopt time if
    /// never used (§7). The TTL clock's zero.
    last_used_at: Timestamp,
}

/// The state-file spelling of an [`Entry`] (docker-executor.md §7). Plain
/// integer fields — `u64` bytes, `i64` micros — because the file is a
/// lossy-OK local record, not a wire type: [`ByteSize`]/[`Timestamp`]'s own
/// serde forms (a humane string, an RFC 3339 string) would round-trip fine but
/// are heavier than this store needs.
#[derive(serde::Serialize, serde::Deserialize)]
struct SerEntry {
    id: String,
    digest: String,
    size_bytes: u64,
    last_used_at_us: i64,
}

impl From<&Entry> for SerEntry {
    fn from(entry: &Entry) -> SerEntry {
        SerEntry {
            id: entry.id.clone(),
            digest: entry.digest.clone(),
            size_bytes: entry.size.as_u64(),
            last_used_at_us: entry.last_used_at.as_micros(),
        }
    }
}

impl SerEntry {
    /// Rebuild an [`Entry`], dropping a row whose micros are out of
    /// [`Timestamp`]'s range (a corrupt file is lossy-OK, §7).
    fn into_entry(self) -> Option<Entry> {
        Some(Entry {
            id: self.id,
            digest: self.digest,
            size: ByteSize::from_bytes(self.size_bytes),
            last_used_at: Timestamp::from_micros(self.last_used_at_us)?,
        })
    }
}

/// The manager's mutable state, guarded by a plain `std::sync::Mutex` held only
/// across set mutations — never across an await (§11), so `persist` and every
/// daemon call happen after the guard is dropped.
struct CacheState {
    /// Tracked images by digest.
    entries: HashMap<String, Entry>,
    /// Digest → the allocations pinning it (§7). An image with a non-empty set
    /// is never evicted.
    pins: HashMap<String, HashSet<AllocationId>>,
    /// Reverse index for O(1) `release`, and to replace an allocation's pin when
    /// it re-pins to a new digest.
    by_alloc: HashMap<AllocationId, String>,
}

impl CacheState {
    /// Publish the inventory gauges. Call under the lock, at every mutation of
    /// `entries`, so the pushed values never lag the map (the mod.rs
    /// push-on-transition convention).
    fn push_gauges(&self) {
        metrics::gauge!(AGENT_CACHED_IMAGES).set(self.entries.len() as f64);
        let bytes: u128 = self
            .entries
            .values()
            .map(|entry| u128::from(entry.size.as_u64()))
            .sum();
        metrics::gauge!(AGENT_CACHED_IMAGE_BYTES).set(bytes as f64);
    }
}

/// A single in-flight resolution of one reference (§7 singleflight). The first
/// holder of the async mutex runs the puller and stores its result; followers
/// queued behind it find `done` populated and never pull, so n concurrent
/// starts of one image collapse to one pull.
#[derive(Default)]
struct Flight {
    done: Option<FetchedImage>,
}

/// The shared guts behind every [`ImageCache`] clone.
struct CacheInner {
    docker: Docker,
    config: ImageCacheConfig,
    /// `None` = in-memory only (tests); else `data_dir/image-cache.json`.
    state_path: Option<PathBuf>,
    /// `statvfs` paths for the `High`-pressure byte target (§9).
    pressure_paths: Vec<PathBuf>,
    /// The high-water mark a `High` sweep evicts below (§9).
    high_pct: u8,
    /// The shared host disk-pressure signal (§9): `prepare` drops hints under
    /// `High`, and a sweep's plan escalates with it.
    pressure: watch::Receiver<DiskPressure>,
    /// The tracked-image state (§7); never held across an await.
    state: Mutex<CacheState>,
    /// Per-reference singleflight map (§7). A `std::sync::Mutex` guards the map
    /// itself; each value's async mutex serializes that reference's
    /// inspect→pull→re-inspect. Entries whose `Arc::strong_count == 1` are swept
    /// after release (small map, linear sweep fine).
    inflight: Mutex<HashMap<String, Arc<AsyncMutex<Flight>>>>,
    /// Global concurrent-pull limit (§7, config default 2). Acquired around the
    /// actual pull only, never the inspect.
    pull_semaphore: Semaphore,
    /// Monotone count of pulls actually performed, for the `cache_pulls_started`
    /// integration-test seam (§7). Incremented inside the pull path.
    pulls_started: AtomicU64,
}

/// The image cache manager (docker-executor.md §7). A cheap `Clone` handle;
/// clones share one [`CacheInner`].
#[derive(Clone)]
pub(crate) struct ImageCache {
    inner: Arc<CacheInner>,
}

impl ImageCache {
    /// Build the manager, loading the state file lossily (an unreadable or
    /// corrupt file → empty, logged at info; §7). Reconciliation against the
    /// daemon's actual images and restart re-pinning happen in [`recover`], run
    /// from [`super::DockerExecutor::new`] before the janitor spawns.
    ///
    /// [`recover`]: ImageCache::recover
    pub(crate) fn new(
        docker: Docker,
        pressure: watch::Receiver<DiskPressure>,
        options: CacheOptions,
    ) -> ImageCache {
        let entries = load_state(options.state_path.as_deref());
        // `max(1)` guards a zero-permit semaphore (which would deadlock every
        // pull). Config validation already rejects `max_concurrent_pulls < 1`;
        // this is belt-and-suspenders against a future default regression.
        let permits = options.config.max_concurrent_pulls.max(1);
        let inner = CacheInner {
            docker,
            config: options.config,
            state_path: options.state_path,
            pressure_paths: options.pressure_paths,
            high_pct: options.high_pct,
            pressure,
            state: Mutex::new(CacheState {
                entries,
                pins: HashMap::new(),
                by_alloc: HashMap::new(),
            }),
            inflight: Mutex::new(HashMap::new()),
            pull_semaphore: Semaphore::new(permits),
            pulls_started: AtomicU64::new(0),
        };
        let cache = ImageCache {
            inner: Arc::new(inner),
        };
        // Publish the loaded inventory immediately; `recover` refreshes it.
        cache.lock_state().push_gauges();
        cache
    }

    fn lock_state(&self) -> MutexGuard<'_, CacheState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reconcile the tracked inventory against the daemon's actual images and
    /// re-pin from surviving containers (docker-executor.md §7). Run once from
    /// [`super::DockerExecutor::new`] before the janitor spawns; the reconcile
    /// half also runs on every janitor tick so the inventory tracks reality.
    ///
    /// Deliberately **best-effort, never fatal**: `run_daemon` already fails fast
    /// on an unreachable daemon (the data-root probe, §9), so a reconcile/re-pin
    /// error here is transient — the janitor's next reconcile catches up, and an
    /// unpinned surviving image is protected by the daemon's own in-use `409` on
    /// eviction. Making it fatal would also break the daemon-less
    /// `pressure_critical_refuses_start` construction path.
    pub(crate) async fn recover(&self) {
        if let Err(err) = self.reconcile(Timestamp::now()).await {
            tracing::info!(error = %err, "image-cache reconcile at startup failed; the janitor will retry (§7)");
        }
        if let Err(err) = self.repin_from_containers().await {
            tracing::warn!(error = %err, "image-cache re-pin at startup failed; surviving containers may be briefly unpinned (§7)");
        }
    }

    /// Adopt local images unknown to the state file (`last_used_at = now`, the
    /// doc's "rebuilt conservatively") and drop entries whose image no longer
    /// exists locally (§7): the daemon is the truth of existence.
    async fn reconcile(&self, now: Timestamp) -> Result<(), bollard::errors::Error> {
        let summaries = self
            .inner
            .docker
            .list_images(Some(ListImagesOptionsBuilder::new().build()))
            .await?;
        // digest → (id, size), first-writer-wins on a digest collision.
        let mut local: HashMap<String, (String, ByteSize)> = HashMap::new();
        for summary in &summaries {
            let digest = pick_digest(Some(&summary.repo_digests), Some(&summary.id));
            if digest.is_empty() {
                continue;
            }
            let size = ByteSize::from_bytes(summary.size.max(0) as u64);
            local
                .entry(digest)
                .or_insert_with(|| (summary.id.clone(), size));
        }

        let mut changed = false;
        {
            let mut st = self.lock_state();
            // Drop entries with no local image.
            let gone: Vec<String> = st
                .entries
                .keys()
                .filter(|digest| !local.contains_key(*digest))
                .cloned()
                .collect();
            for digest in gone {
                st.entries.remove(&digest);
                changed = true;
            }
            // Adopt unknown local images; refresh a known entry's id/size to
            // reality while keeping its `last_used_at` (its TTL clock).
            for (digest, (id, size)) in local {
                match st.entries.get_mut(&digest) {
                    Some(entry) => {
                        if entry.id != id || entry.size != size {
                            entry.id = id;
                            entry.size = size;
                            changed = true;
                        }
                    }
                    None => {
                        st.entries.insert(
                            digest.clone(),
                            Entry {
                                id,
                                digest,
                                size,
                                last_used_at: now,
                            },
                        );
                        changed = true;
                    }
                }
            }
            st.push_gauges();
        }
        if changed {
            self.persist();
        }
        Ok(())
    }

    /// Re-pin running/exited containers from their `coppice.image-digest` label
    /// (docker-executor.md §7, §5). The `all(true)` label-filtered list is the
    /// exact `recover_cpu_allocations` shape; the digest is read straight off the
    /// summary's labels (no inspect needed). Missing/unparseable labels → skip.
    async fn repin_from_containers(&self) -> Result<(), crate::executor::ExecutorError> {
        let mut filters = HashMap::new();
        filters.insert("label".to_string(), vec![LABEL_ALLOCATION.to_string()]);
        let options = ListContainersOptionsBuilder::new()
            .all(true)
            .filters(&filters)
            .build();
        let summaries = self
            .inner
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|err| {
                crate::executor::ExecutorError::Other(format!(
                    "listing containers for image-cache re-pin: {err}"
                ))
            })?;
        for summary in summaries {
            let labels = summary.labels.as_ref();
            let allocation = labels
                .and_then(|labels| labels.get(LABEL_ALLOCATION))
                .and_then(|raw| raw.parse::<AllocationId>().ok());
            let digest = labels
                .and_then(|labels| labels.get(LABEL_IMAGE_DIGEST))
                .filter(|digest| !digest.is_empty());
            if let (Some(allocation), Some(digest)) = (allocation, digest) {
                self.pin(allocation, digest);
            }
        }
        Ok(())
    }

    // ---- pulls (docker-executor.md §7) ----------------------------------

    /// Resolve `reference` to a [`FetchedImage`], pulling if absent (§7). Local
    /// wins: an inspect hit is done, no pull (digest refs are exact; tag refs
    /// accept the local tag — tag-drift re-resolution is future work). The pull
    /// itself is per-reference singleflight and bounded by the global
    /// concurrent-pull semaphore. On success the entry is recorded/refreshed
    /// (`last_used_at = now`) and the inventory gauges pushed.
    pub(crate) async fn fetch(&self, reference: &str) -> Result<FetchedImage, StartError> {
        let fetched = self
            .fetch_with(reference, || self.pull_and_inspect(reference))
            .await?;
        self.record(&fetched);
        Ok(fetched)
    }

    /// The singleflight orchestration around a puller closure (§7 planned
    /// upgrade 3). Generic over the closure so the singleflight is
    /// unit-testable without a daemon: n concurrent calls for one reference run
    /// the closure exactly once (followers reuse the stored result), and the map
    /// entry is swept once no caller holds it.
    async fn fetch_with<F, Fut>(
        &self,
        reference: &str,
        puller: F,
    ) -> Result<FetchedImage, StartError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<FetchedImage, StartError>>,
    {
        let flight = {
            let mut map = self
                .inner
                .inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.entry(reference.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(Flight::default())))
                .clone()
        };

        let result = {
            let mut slot = flight.lock().await;
            match &slot.done {
                // A concurrent leader already resolved this reference: reuse it,
                // never pull (the singleflight's whole point).
                Some(fetched) => Ok(fetched.clone()),
                None => {
                    let resolved = puller().await;
                    // Cache only success — a failed pull leaves the slot empty so
                    // the next caller retries rather than inheriting the error.
                    if let Ok(fetched) = &resolved {
                        slot.done = Some(fetched.clone());
                    }
                    resolved
                }
            }
        };

        // Sweep the map entry once we and every follower have let go. Dropping
        // our clone first makes `strong_count == 1` mean "only the map holds it".
        drop(flight);
        {
            let mut map = self
                .inner
                .inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = map.get(reference) {
                if Arc::strong_count(existing) == 1 {
                    map.remove(reference);
                }
            }
        }
        result
    }

    /// The docker-touching resolve kept behind its own private seam so a
    /// peer-aware fetcher can slot in later (§7 planned upgrade 3): inspect
    /// first (present → done, no pull), else pull under a permit and re-inspect.
    async fn pull_and_inspect(&self, reference: &str) -> Result<FetchedImage, StartError> {
        match self.inner.docker.inspect_image(reference).await {
            Ok(inspect) => {
                return Ok(FetchedImage {
                    digest: digest_of(&inspect),
                    inspect,
                })
            }
            Err(err) if api::status_code(&err) == Some(404) => {}
            Err(err) => return Err(classify::classify_pull_error(&err, reference)),
        }
        // Absent: pull under the global concurrent-pull permit (the inspect
        // above deliberately ran outside it — §7 bounds pulls, not inspects).
        {
            let _permit = self
                .inner
                .pull_semaphore
                .acquire()
                .await
                .expect("pull semaphore is never closed");
            self.inner.pulls_started.fetch_add(1, Ordering::Relaxed);
            let started = std::time::Instant::now();
            let options = CreateImageOptionsBuilder::new()
                .from_image(reference)
                .build();
            let mut stream =
                std::pin::pin!(self.inner.docker.create_image(Some(options), None, None));
            while let Some(item) = stream.next().await {
                item.map_err(|err| classify::classify_pull_error(&err, reference))?;
            }
            metrics::counter!(AGENT_IMAGE_PULLS_TOTAL).increment(1);
            metrics::histogram!(AGENT_IMAGE_PULL_DURATION).record(started.elapsed().as_secs_f64());
        }
        // Re-inspect the freshly-pulled image for its bytes/config/size.
        let inspect = self
            .inner
            .docker
            .inspect_image(reference)
            .await
            .map_err(|err| classify::classify_pull_error(&err, reference))?;
        Ok(FetchedImage {
            digest: digest_of(&inspect),
            inspect,
        })
    }

    /// Record a freshly resolved image (id, digest, size, `last_used_at = now`)
    /// and push the inventory gauges. Idempotent: re-fetching a tracked image
    /// just bumps its `last_used_at`.
    fn record(&self, fetched: &FetchedImage) {
        let now = Timestamp::now();
        {
            let mut st = self.lock_state();
            let id = fetched.inspect.id.clone().unwrap_or_default();
            // Docker reports the size as a signed integer; a negative reading is
            // nonsense rather than a real image, so it clamps to zero — the same
            // crossing `lifecycle.rs` and the disk plan make.
            let size = ByteSize::from_bytes(
                fetched
                    .inspect
                    .size
                    .map(|size| size.max(0) as u64)
                    .unwrap_or(0),
            );
            st.entries.insert(
                fetched.digest.clone(),
                Entry {
                    id,
                    digest: fetched.digest.clone(),
                    size,
                    last_used_at: now,
                },
            );
            st.push_gauges();
        }
        self.persist();
    }

    // ---- pinning (docker-executor.md §7) --------------------------------

    /// Pin `digest` for `allocation` (§7). Called by `start_inner` right after
    /// `fetch` resolves, before the container is created. Re-pinning the same
    /// allocation to a new digest replaces the old pin. Pins are in-memory only,
    /// never persisted (see the module comment).
    pub(crate) fn pin(&self, allocation: AllocationId, digest: &str) {
        let mut st = self.lock_state();
        // Replace a prior pin for this allocation on a different digest.
        if let Some(previous) = st.by_alloc.get(&allocation).cloned() {
            if previous != digest {
                remove_pin(&mut st.pins, &previous, allocation);
            }
        }
        st.by_alloc.insert(allocation, digest.to_string());
        st.pins
            .entry(digest.to_string())
            .or_default()
            .insert(allocation);
    }

    /// Unpin `allocation` (§7). When the digest's pin set drains and the entry
    /// still exists, stamp `last_used_at = now` — "the end of the last attempt
    /// using the image". Idempotent for an unknown allocation. Persists, since a
    /// stamped `last_used_at` is durable state.
    pub(crate) fn release(&self, allocation: AllocationId) {
        let mut stamped = false;
        {
            let mut st = self.lock_state();
            let Some(digest) = st.by_alloc.remove(&allocation) else {
                return;
            };
            let drained = remove_pin(&mut st.pins, &digest, allocation);
            if drained {
                if let Some(entry) = st.entries.get_mut(&digest) {
                    entry.last_used_at = Timestamp::now();
                    stamped = true;
                }
            }
        }
        if stamped {
            self.persist();
        }
    }

    // ---- eviction + the janitor (docker-executor.md §7, §9, §11) --------

    /// Run one janitor sweep with an injected `now` (the `cache_sweep_at` seam):
    /// reconcile the inventory against the daemon, then plan and execute
    /// evictions, returning the count actually removed. The janitor task calls
    /// this with `Timestamp::now()`.
    pub(crate) async fn sweep(&self, now: Timestamp) -> usize {
        if let Err(err) = self.reconcile(now).await {
            tracing::warn!(error = %err, "image-cache reconcile failed this sweep; retrying next tick");
        }
        let pressure = *self.inner.pressure.borrow();
        // The `High` byte target reads the same filesystems the pressure monitor
        // watches (§9); `Ok`/`Critical` don't consult it (`Critical` sweeps to
        // the floor, `Ok` is TTL-only).
        let bytes_to_free = if pressure == DiskPressure::High {
            self.bytes_to_free()
        } else {
            None
        };
        let ttl = core_duration(self.inner.config.ttl);

        let plan = {
            let st = self.lock_state();
            let views: Vec<EntryView> = st
                .entries
                .values()
                .map(|entry| EntryView {
                    digest: entry.digest.clone(),
                    size: entry.size,
                    last_used_at: entry.last_used_at,
                    pinned: st
                        .pins
                        .get(&entry.digest)
                        .is_some_and(|set| !set.is_empty()),
                })
                .collect();
            plan_evictions(&views, now, ttl, pressure, bytes_to_free)
        };

        let mut evicted = 0;
        for digest in plan {
            // Attribute the reason per image: an idle-past-TTL removal is `ttl`
            // even under `High`; anything the pressure escalation added is
            // `pressure`.
            let reason = {
                let st = self.lock_state();
                match st.entries.get(&digest) {
                    Some(entry) if now.duration_since(entry.last_used_at) >= ttl => REASON_TTL,
                    _ => REASON_PRESSURE,
                }
            };
            if self.evict(&digest, reason).await {
                evicted += 1;
            }
        }
        evicted
    }

    /// The `High`-pressure byte target: `used − high_pct%·total` over
    /// [`CacheOptions::pressure_paths`], taking the **max** across paths (§9).
    /// Empty paths or all-failed `statvfs` → `None`, which a `High` sweep reads
    /// as "no local reading" and drops every unpinned image (disk safety wins).
    fn bytes_to_free(&self) -> Option<ByteSize> {
        let mut target: Option<ByteSize> = None;
        for path in &self.inner.pressure_paths {
            if let Some(over) = crate::pressure::bytes_over_pct(path, self.inner.high_pct) {
                target = Some(target.map_or(over, |current| current.max(over)));
            }
        }
        target
    }

    /// Evict one image by its `id` (`remove_image`, `force: false`, §7). 404 is
    /// already-gone success; 409 (in use by a container / multiple tags) is a
    /// skip — the daemon is the backstop for references we don't track. On
    /// removal the entry is dropped, state persisted, gauges pushed, and the
    /// eviction counter incremented with `reason`. Returns whether an image was
    /// removed.
    async fn evict(&self, digest: &str, reason: &'static str) -> bool {
        let Some(id) = self
            .lock_state()
            .entries
            .get(digest)
            .map(|entry| entry.id.clone())
        else {
            return false;
        };
        let options = RemoveImageOptionsBuilder::new().force(false).build();
        let removed = match self
            .inner
            .docker
            .remove_image(&id, Some(options), None)
            .await
        {
            Ok(_) => true,
            Err(err) => match api::status_code(&err) {
                // Already gone: treat as a successful eviction (drop the entry).
                Some(404) => true,
                // In use by a container, or multiple tags share the id: leave it,
                // the daemon is the backstop.
                Some(409) => {
                    tracing::debug!(digest, image = %id, "skipping eviction: image in use or multiply tagged");
                    return false;
                }
                _ => {
                    tracing::warn!(digest, image = %id, error = %err, "image eviction failed; retrying next sweep");
                    return false;
                }
            },
        };
        if removed {
            {
                let mut st = self.lock_state();
                st.entries.remove(digest);
                st.push_gauges();
            }
            self.persist();
            metrics::counter!(AGENT_IMAGE_EVICTIONS_TOTAL, "reason" => reason).increment(1);
        }
        removed
    }

    // ---- inventory + hints (docker-executor.md §7, ADR 0010) ------------

    /// The `ImageCacheInventory` snapshot `cache_inventory()` returns for
    /// heartbeats (§7, ADR 0010): every tracked image (pinned included — they
    /// are cached) as a [`pb::CachedImage`], `Timestamp` rendered to µs like the
    /// rest of the proto boundary.
    pub(crate) fn inventory(&self) -> pb::ImageCacheInventory {
        let st = self.lock_state();
        let images = st
            .entries
            .values()
            .map(|entry| pb::CachedImage {
                digest: entry.digest.clone(),
                size_bytes: entry.size.as_u64(),
                last_used_at_us: entry.last_used_at.as_micros(),
            })
            .collect();
        pb::ImageCacheInventory { images }
    }

    /// The `PrepareCache` hint (§7, ADR 0010): warm `image` if convenient. Under
    /// `High`+ pressure it is dropped (a warm pull would fight the eviction
    /// sweep); otherwise a fetch is spawned on a self-clone, logging failure at
    /// info. Fire and forget — the hint is freely ignorable.
    pub(crate) fn prepare(&self, image: String) {
        if *self.inner.pressure.borrow() >= DiskPressure::High {
            tracing::debug!(%image, "dropping PrepareCache hint under high disk pressure (§7)");
            return;
        }
        let cache = self.clone();
        tokio::spawn(async move {
            if let Err(err) = cache.fetch(&image).await {
                tracing::info!(%image, error = %err, "PrepareCache warm pull failed (advisory)");
            }
        });
    }

    /// The `EvictImageHint` hint (§7, ADR 0010): evict `digest` if unpinned.
    /// Freely ignored when pinned or unknown. Fire and forget.
    pub(crate) fn evict_hint(&self, digest: String) {
        let cache = self.clone();
        tokio::spawn(async move {
            let pinned = {
                let st = cache.lock_state();
                st.pins.get(&digest).is_some_and(|set| !set.is_empty())
            };
            if pinned {
                tracing::debug!(%digest, "evict hint ignored: image is pinned (§7)");
                return;
            }
            if !cache.evict(&digest, REASON_HINT).await {
                tracing::debug!(%digest, "evict hint: nothing to evict (unknown or in use)");
            }
        });
    }

    /// The `cache_pulls_started` integration-test seam (§7): a monotone count of
    /// pulls actually performed.
    pub(crate) fn pulls_started(&self) -> u64 {
        self.inner.pulls_started.load(Ordering::Relaxed)
    }

    /// Persist the tracked entries to `state_path` (§7): serde_json to a
    /// `.json.tmp` sibling then `std::fs::rename`, using **plain `std::fs`, not
    /// the consensus `Fs` seam** — the file is lossy-OK by design (it is rebuilt
    /// from `docker image ls` on any read failure), so the journal's
    /// fsync-before-rename discipline would be pure overkill. A write failure is
    /// logged at info and swallowed. In-memory mode (`None`) is a no-op.
    fn persist(&self) {
        let Some(path) = self.inner.state_path.as_deref() else {
            return;
        };
        let snapshot: Vec<SerEntry> = {
            self.lock_state()
                .entries
                .values()
                .map(SerEntry::from)
                .collect()
        };
        let bytes = match serde_json::to_vec_pretty(&snapshot) {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::info!(error = %err, "serializing image-cache state failed (lossy-OK)");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        if let Err(err) = std::fs::write(&tmp, &bytes).and_then(|()| std::fs::rename(&tmp, path)) {
            tracing::info!(path = %path.display(), error = %err, "persisting image-cache state failed (lossy-OK)");
        }
    }
}

/// Spawn the janitor task, returning its handle (aborted on [`super::Inner`]
/// drop). Captures only an [`ImageCache`] clone and a pressure receiver — never
/// an `Arc<super::Inner>` — so the abort is what actually stops it, mirroring
/// `events::spawn`/`disk::spawn`. A [`DiskPressure`] transition wakes it
/// immediately (so a `High` transition reacts at once, not up to a tick late);
/// otherwise it sweeps every [`JANITOR_INTERVAL`].
pub(crate) fn spawn_janitor(
    cache: ImageCache,
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
            cache.sweep(Timestamp::now()).await;
        }
    })
}

// ---- pure helpers (the unit-test surface) -------------------------------

/// A read-only projection of an [`Entry`] plus its pinned bit, the pure input
/// to [`plan_evictions`] (docker-executor.md §7).
struct EntryView {
    digest: String,
    size: ByteSize,
    last_used_at: Timestamp,
    pinned: bool,
}

/// Most-stale-first eviction plan (docker-executor.md §7). Pure, so the policy
/// is unit-testable without a daemon.
///
/// Rules: **pinned images are never evicted.** `Ok` → entries idle at or past
/// `ttl`. `High` → those TTL-expired entries plus, most-stale-first, enough
/// additional unpinned entries to cover `bytes_to_free` (stopping once the
/// summed sizes reach it). `Critical` → every unpinned entry (§9 "sweep to
/// floor").
///
/// `bytes_to_free = None` under `High` means "no local reading" (a remote
/// daemon, §9): disk safety wins (ADR 0010) and every unpinned image goes.
fn plan_evictions(
    entries: &[EntryView],
    now: Timestamp,
    ttl: Duration,
    pressure: DiskPressure,
    bytes_to_free: Option<ByteSize>,
) -> Vec<String> {
    let expired = |entry: &EntryView| now.duration_since(entry.last_used_at) >= ttl;
    match pressure {
        DiskPressure::Ok => entries
            .iter()
            .filter(|entry| !entry.pinned && expired(entry))
            .map(|entry| entry.digest.clone())
            .collect(),
        DiskPressure::Critical => entries
            .iter()
            .filter(|entry| !entry.pinned)
            .map(|entry| entry.digest.clone())
            .collect(),
        DiskPressure::High => {
            let target = match bytes_to_free {
                // No local reading: every unpinned image goes.
                None => {
                    return entries
                        .iter()
                        .filter(|entry| !entry.pinned)
                        .map(|entry| entry.digest.clone())
                        .collect()
                }
                Some(target) => u128::from(target.as_u64()),
            };
            // TTL-expired unpinned entries go regardless, and their bytes count
            // toward the target.
            let mut chosen: Vec<&EntryView> = entries
                .iter()
                .filter(|entry| !entry.pinned && expired(entry))
                .collect();
            let mut freed: u128 = chosen
                .iter()
                .map(|entry| u128::from(entry.size.as_u64()))
                .sum();
            if freed < target {
                // Then the not-yet-expired unpinned entries, most-stale-first,
                // until the target is covered.
                let mut rest: Vec<&EntryView> = entries
                    .iter()
                    .filter(|entry| !entry.pinned && !expired(entry))
                    .collect();
                rest.sort_by_key(|entry| entry.last_used_at);
                for entry in rest {
                    if freed >= target {
                        break;
                    }
                    freed += u128::from(entry.size.as_u64());
                    chosen.push(entry);
                }
            }
            chosen.iter().map(|entry| entry.digest.clone()).collect()
        }
    }
}

/// The cache key/label digest for an image: the first `repo_digests` entry if
/// any, else the image `id` — the exact fallback `lifecycle.rs` used for
/// `LABEL_IMAGE_DIGEST`, kept in one place so label and cache key can never
/// diverge (docker-executor.md §7).
pub(crate) fn digest_of(inspect: &ImageInspect) -> String {
    pick_digest(inspect.repo_digests.as_deref(), inspect.id.as_deref())
}

/// The shared digest fallback, over either the `ImageInspect` or the
/// list-images summary shape (`repo_digests` first, else `id`).
fn pick_digest(repo_digests: Option<&[String]>, id: Option<&str>) -> String {
    repo_digests
        .and_then(|digests| digests.first())
        .cloned()
        .or_else(|| id.map(str::to_string))
        .unwrap_or_default()
}

/// Drop `allocation` from `digest`'s pin set, removing the now-empty set.
/// Returns whether the set drained (so `release` knows to stamp `last_used_at`).
fn remove_pin(
    pins: &mut HashMap<String, HashSet<AllocationId>>,
    digest: &str,
    allocation: AllocationId,
) -> bool {
    match pins.get_mut(digest) {
        Some(set) => {
            set.remove(&allocation);
            let drained = set.is_empty();
            if drained {
                pins.remove(digest);
            }
            drained
        }
        // No set at all is a drained set for the stamping purpose.
        None => true,
    }
}

/// Load the state file lossily (docker-executor.md §7): a missing, unreadable,
/// or corrupt file yields an empty map (logged at info), never an error — it is
/// rebuilt from `docker image ls` by the reconcile in [`ImageCache::recover`].
fn load_state(path: Option<&Path>) -> HashMap<String, Entry> {
    let Some(path) = path else {
        return HashMap::new();
    };
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(err) => {
            tracing::info!(path = %path.display(), error = %err, "reading image-cache state failed; starting empty (lossy-OK)");
            return HashMap::new();
        }
    };
    match serde_json::from_slice::<Vec<SerEntry>>(&bytes) {
        Ok(list) => list
            .into_iter()
            .filter_map(SerEntry::into_entry)
            .map(|entry| (entry.digest.clone(), entry))
            .collect(),
        Err(err) => {
            tracing::info!(path = %path.display(), error = %err, "image-cache state is corrupt; starting empty (lossy-OK)");
            HashMap::new()
        }
    }
}

/// Convert the config's `std::time::Duration` TTL into a
/// [`coppice_core::time::Duration`] for comparison against `now − last_used_at`,
/// saturating a nonsense value at [`Duration::MAX`].
fn core_duration(ttl: std::time::Duration) -> Duration {
    Duration::from_micros(i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // Hand-built timestamps the disk.rs/pressure.rs way: no clock object, just
    // `UNIX_EPOCH + Duration` (§12).
    fn at(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn view(digest: &str, last_used_secs: i64, size: u64, pinned: bool) -> EntryView {
        EntryView {
            digest: digest.to_string(),
            size: ByteSize::from_bytes(size),
            last_used_at: at(last_used_secs),
            pinned,
        }
    }

    const TTL: Duration = Duration::from_secs(1800); // 30m

    // ---- TTL boundary (docker-executor.md §7) ------------------------------

    #[test]
    fn ttl_evicts_at_or_past_the_boundary_only() {
        // last_used at t=0; ttl = 1800s. now just below / at / above 1800s.
        let entries = vec![view("d", 0, 10, false)];
        assert!(plan_evictions(&entries, at(1799), TTL, DiskPressure::Ok, None).is_empty());
        assert_eq!(
            plan_evictions(&entries, at(1800), TTL, DiskPressure::Ok, None),
            vec!["d".to_string()]
        );
        assert_eq!(
            plan_evictions(&entries, at(1801), TTL, DiskPressure::Ok, None),
            vec!["d".to_string()]
        );
    }

    #[test]
    fn pinned_image_past_ttl_survives() {
        let entries = vec![view("d", 0, 10, true)];
        assert!(plan_evictions(&entries, at(100_000), TTL, DiskPressure::Ok, None).is_empty());
    }

    // ---- pressure ordering (docker-executor.md §7, §9) ---------------------

    #[test]
    fn high_pressure_frees_most_stale_first_and_stops_when_covered() {
        // Three unpinned, none TTL-expired at now=100 (< 1800). Sizes 30/30/30.
        // Target 50 → the two stalest (oldest last_used) cover it, the freshest
        // survives.
        let entries = vec![
            view("stale", 0, 30, false),
            view("mid", 10, 30, false),
            view("fresh", 20, 30, false),
        ];
        let plan = plan_evictions(
            &entries,
            at(100),
            TTL,
            DiskPressure::High,
            Some(ByteSize::from_bytes(50)),
        );
        assert_eq!(plan.len(), 2, "two 30-byte images cover a 50-byte target");
        assert!(plan.contains(&"stale".to_string()));
        assert!(plan.contains(&"mid".to_string()));
        assert!(!plan.contains(&"fresh".to_string()));
    }

    #[test]
    fn high_pressure_takes_ttl_expired_regardless_of_target() {
        // One TTL-expired (last_used far in the past) plus fresh ones; a tiny
        // target the expired image already covers → only the expired one goes.
        let entries = vec![
            view("expired", 0, 5, false),
            view("fresh", 100_000, 30, false),
        ];
        let plan = plan_evictions(
            &entries,
            at(100_000),
            TTL,
            DiskPressure::High,
            Some(ByteSize::from_bytes(1)),
        );
        assert_eq!(plan, vec!["expired".to_string()]);
    }

    #[test]
    fn high_pressure_without_local_reading_drops_all_unpinned() {
        let entries = vec![
            view("a", 100_000, 5, false),
            view("b", 100_000, 5, true), // pinned survives even here
            view("c", 100_000, 5, false),
        ];
        let plan = plan_evictions(&entries, at(100_000), TTL, DiskPressure::High, None);
        assert_eq!(plan.len(), 2);
        assert!(!plan.contains(&"b".to_string()));
    }

    #[test]
    fn critical_drops_all_unpinned_and_only_unpinned() {
        let entries = vec![
            view("a", 100_000, 5, false),
            view("pinned", 0, 5, true),
            view("b", 100_000, 5, false),
        ];
        let plan = plan_evictions(
            &entries,
            at(100_000),
            TTL,
            DiskPressure::Critical,
            Some(ByteSize::from_bytes(1)),
        );
        assert_eq!(plan.len(), 2);
        assert!(!plan.contains(&"pinned".to_string()));
    }

    // ---- pin/unpin refcounts (state struct directly) -----------------------

    fn empty_state() -> CacheState {
        CacheState {
            entries: HashMap::new(),
            pins: HashMap::new(),
            by_alloc: HashMap::new(),
        }
    }

    #[test]
    fn two_allocs_one_digest_release_refcounts() {
        let mut st = empty_state();
        st.entries.insert(
            "d".to_string(),
            Entry {
                id: "id".into(),
                digest: "d".into(),
                size: ByteSize::from_bytes(1),
                last_used_at: at(0),
            },
        );
        let a = AllocationId::new();
        let b = AllocationId::new();
        // pin both to "d"
        for alloc in [a, b] {
            st.by_alloc.insert(alloc, "d".to_string());
            st.pins.entry("d".to_string()).or_default().insert(alloc);
        }
        // first release: still pinned, no stamp
        assert!(!remove_pin(&mut st.pins, "d", a));
        st.by_alloc.remove(&a);
        assert!(st.pins.get("d").is_some_and(|s| !s.is_empty()));
        // second release: drains → the caller would stamp last_used_at
        assert!(remove_pin(&mut st.pins, "d", b));
        st.by_alloc.remove(&b);
        assert!(!st.pins.contains_key("d"));
    }

    #[test]
    fn release_of_unknown_alloc_is_a_noop() {
        // `remove_pin` on an absent digest reports "drained" (nothing to keep),
        // and `release` returns early when `by_alloc` has no entry — modelled
        // here by the empty maps.
        let mut st = empty_state();
        assert!(st.by_alloc.remove(&AllocationId::new()).is_none());
        assert!(remove_pin(&mut st.pins, "absent", AllocationId::new()));
    }

    #[test]
    fn repin_same_alloc_to_new_digest_releases_the_old() {
        let mut st = empty_state();
        let alloc = AllocationId::new();
        // pin to "old"
        st.by_alloc.insert(alloc, "old".to_string());
        st.pins.entry("old".to_string()).or_default().insert(alloc);
        // re-pin to "new": drop from "old", add to "new" (the `pin` body).
        let previous = st.by_alloc.get(&alloc).cloned();
        if let Some(previous) = previous {
            if previous != "new" {
                remove_pin(&mut st.pins, &previous, alloc);
            }
        }
        st.by_alloc.insert(alloc, "new".to_string());
        st.pins.entry("new".to_string()).or_default().insert(alloc);

        assert!(!st.pins.contains_key("old"), "old pin dropped");
        assert!(st.pins.get("new").is_some_and(|s| s.contains(&alloc)));
    }

    // ---- state-file round-trip / reconcile (docker-executor.md §7) ---------

    #[test]
    fn state_file_round_trips() {
        let entry = Entry {
            id: "sha256:abc".into(),
            digest: "busybox@sha256:def".into(),
            size: ByteSize::from_mib(4),
            last_used_at: at(12_345),
        };
        let ser = SerEntry::from(&entry);
        let json = serde_json::to_vec(&vec![ser]).unwrap();
        let back = serde_json::from_slice::<Vec<SerEntry>>(&json).unwrap();
        let rebuilt = back.into_iter().next().unwrap().into_entry().unwrap();
        assert_eq!(rebuilt.id, entry.id);
        assert_eq!(rebuilt.digest, entry.digest);
        assert_eq!(rebuilt.size, entry.size);
        assert_eq!(rebuilt.last_used_at, entry.last_used_at);
    }

    #[test]
    fn corrupt_state_file_loads_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("image-cache.json");
        std::fs::write(&path, b"{ this is not json").unwrap();
        assert!(load_state(Some(&path)).is_empty());
    }

    #[test]
    fn missing_state_file_loads_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        assert!(load_state(Some(&path)).is_empty());
    }

    #[test]
    fn none_path_loads_empty() {
        assert!(load_state(None).is_empty());
    }

    #[test]
    fn digest_prefers_repo_digest_then_falls_back_to_id() {
        assert_eq!(
            pick_digest(
                Some(&["busybox@sha256:aaa".to_string()]),
                Some("sha256:bbb")
            ),
            "busybox@sha256:aaa"
        );
        assert_eq!(pick_digest(Some(&[]), Some("sha256:bbb")), "sha256:bbb");
        assert_eq!(pick_digest(None, Some("sha256:bbb")), "sha256:bbb");
        assert_eq!(pick_digest(None, None), "");
    }

    // ---- singleflight + semaphore (fetch_with seam, no daemon) --------------

    /// A cache with no daemon reachability but usable in-memory state, for the
    /// pure-orchestration tests. The `docker` client is never dialed by
    /// `fetch_with` (the closure stands in for the daemon-touching pull).
    fn test_cache(max_pulls: usize) -> ImageCache {
        let docker = api::connect("tcp://127.0.0.1:2375").expect("lazy http client");
        let (_tx, rx) = watch::channel(DiskPressure::Ok);
        ImageCache::new(
            docker,
            rx,
            CacheOptions {
                config: ImageCacheConfig {
                    ttl: std::time::Duration::from_secs(1800),
                    max_concurrent_pulls: max_pulls,
                },
                state_path: None,
                pressure_paths: Vec::new(),
                high_pct: 85,
            },
        )
    }

    fn fake_fetched(digest: &str) -> FetchedImage {
        FetchedImage {
            inspect: ImageInspect {
                id: Some(digest.to_string()),
                ..Default::default()
            },
            digest: digest.to_string(),
        }
    }

    #[tokio::test]
    async fn singleflight_collapses_concurrent_fetches_of_one_reference() {
        let cache = test_cache(2);
        let runs = Arc::new(AtomicUsize::new(0));
        // A start barrier so all callers enter `fetch_with` together and share
        // one flight; the leader's sleep then keeps followers queued on the
        // async mutex (never sweeping the entry) until it stores the result.
        const N: usize = 8;
        let start = Arc::new(tokio::sync::Barrier::new(N));

        let mut handles = Vec::new();
        for _ in 0..N {
            let cache = cache.clone();
            let runs = Arc::clone(&runs);
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await;
                cache
                    .fetch_with("busybox:same", || async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Ok(fake_fetched("busybox@sha256:same"))
                    })
                    .await
            }));
        }
        for handle in handles {
            let fetched = handle.await.unwrap().unwrap();
            assert_eq!(fetched.digest, "busybox@sha256:same");
        }
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "n concurrent fetches of one reference must run the closure once"
        );
    }

    #[tokio::test]
    async fn distinct_references_run_concurrently_within_the_pull_limit() {
        let cache = test_cache(2); // at most two pulls at once
        let inflight = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        const N: usize = 6;
        let start = Arc::new(tokio::sync::Barrier::new(N));

        let mut handles = Vec::new();
        for i in 0..N {
            let cache = cache.clone();
            let inflight = Arc::clone(&inflight);
            let high_water = Arc::clone(&high_water);
            let start = Arc::clone(&start);
            let reference = format!("image:{i}");
            handles.push(tokio::spawn(async move {
                start.wait().await;
                // A separate handle for the pull section, so `fetch_with` can keep
                // borrowing `reference` while the closure owns its own clones.
                let puller = cache.clone();
                let digest = format!("image@sha256:{i}");
                cache
                    .fetch_with(&reference, || async move {
                        // Distinct references → distinct flights → concurrent, but
                        // the semaphore caps how many are inside the pull section.
                        let permit = puller
                            .inner
                            .pull_semaphore
                            .acquire()
                            .await
                            .expect("semaphore open");
                        let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                        high_water.fetch_max(now, Ordering::SeqCst);
                        // Hold the permit while tasks line up, so the high-water
                        // reflects the true concurrent maximum.
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        inflight.fetch_sub(1, Ordering::SeqCst);
                        drop(permit);
                        Ok::<_, StartError>(fake_fetched(&digest))
                    })
                    .await
            }));
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert!(
            high_water.load(Ordering::SeqCst) <= 2,
            "no more than max_concurrent_pulls=2 pulls run at once, saw {}",
            high_water.load(Ordering::SeqCst)
        );
        assert!(
            high_water.load(Ordering::SeqCst) >= 1,
            "at least one pull ran"
        );
    }
}
