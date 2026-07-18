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
/// holder of the async mutex runs the puller and stores its result — success
/// *or* failure — so followers queued behind it never pull, and n concurrent
/// starts of one image collapse to one registry request even when that request
/// fails (a burst of starts for a missing image must not become N failed
/// pulls). The failure is scoped to this flight's cohort: once every holder
/// releases, the map entry is swept, and the next caller forms a fresh flight
/// that retries.
#[derive(Default)]
struct Flight {
    done: Option<Result<FetchedImage, StartError>>,
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
    /// Per-digest image locks serializing pin placement against eviction (§7).
    /// An eviction holds its digest's lock across the pin recheck **and** the
    /// `remove_image` await; `fetch` takes the same lock to revalidate-then-
    /// record-and-pin. Without this, a start could pin a digest after a sweep's
    /// pin check but before its removal completed — and with no container yet,
    /// the daemon's in-use 409 cannot protect it, so the start's create-by-id
    /// would fail on a vanished image. Same map hygiene as `inflight`.
    image_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    /// Global concurrent-pull limit (§7, config default 2). Acquired around the
    /// actual pull only, never the inspect.
    pull_semaphore: Semaphore,
    /// Monotone count of pulls actually performed, for the `cache_pulls_started`
    /// integration-test seam (§7). Incremented inside the pull path.
    pulls_started: AtomicU64,
    /// Serializes [`ImageCache::persist`]'s snapshot→write→rename sequence.
    /// Concurrent persists (a pull racing a release racing a sweep) share one
    /// `.json.tmp` path; unserialized, an older snapshot could overwrite a newer
    /// one — and a stale-but-valid `last_used_at` read back after a restart
    /// would TTL-evict prematurely. Never held across an await (the write is a
    /// tiny sync file op).
    persist_lock: Mutex<()>,
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
            image_locks: Mutex::new(HashMap::new()),
            pull_semaphore: Semaphore::new(permits),
            pulls_started: AtomicU64::new(0),
            persist_lock: Mutex::new(()),
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
    /// (`last_used_at = now`), the inventory gauges pushed, and — when
    /// `pin_for` names an allocation — the pin placed, all under the digest's
    /// image lock so an eviction cannot interleave (see `image_locks`).
    ///
    /// The start path **must** pin through here rather than pinning after the
    /// fact: only a pin placed under the image lock, with the image's presence
    /// revalidated, is ordered against an in-flight eviction. If an eviction
    /// won the race anyway (the image vanished between resolve and lock), the
    /// resolve is retried — the image is simply re-pulled, latency never
    /// correctness (ADR 0010).
    pub(crate) async fn fetch(
        &self,
        reference: &str,
        pin_for: Option<AllocationId>,
    ) -> Result<FetchedImage, StartError> {
        // Two retries cover the realistic race (one eviction in flight during
        // resolve); the bound exists so a pathological daemon cannot loop us.
        for _ in 0..3 {
            let fetched = self
                .fetch_with(reference, || self.pull_and_inspect(reference))
                .await?;
            let digest = fetched.digest.clone();
            let present = {
                let lock = self.image_lock(&digest);
                let _guard = lock.lock().await;
                // Revalidate under the lock: an eviction that completed between
                // our resolve and this acquisition removed the bytes, so the
                // resolved inspect is stale and the pull must rerun. Inspect by
                // the image id — that is what eviction removes.
                let target = fetched.inspect.id.as_deref().unwrap_or(reference);
                match self.inner.docker.inspect_image(target).await {
                    Ok(_) => {
                        self.record(&fetched);
                        if let Some(allocation) = pin_for {
                            self.pin(allocation, &digest);
                        }
                        Ok(true)
                    }
                    Err(err) if api::status_code(&err) == Some(404) => Ok(false),
                    Err(err) => Err(classify::classify_pull_error(&err, reference)),
                }
            };
            self.unlock_sweep(&digest);
            match present? {
                true => return Ok(fetched),
                false => {
                    // The flight may still be caching the now-stale success for
                    // its cohort; retire it so the retry forms a fresh flight
                    // and re-pulls.
                    self.inner
                        .inflight
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(reference);
                    tracing::debug!(
                        reference,
                        %digest,
                        "image evicted between resolve and pin; re-pulling (§7)"
                    );
                }
            }
        }
        Err(StartError::Start {
            user_error: false,
            message: format!("image {reference} kept vanishing between pull and pin (§7)"),
        })
    }

    /// The per-digest image lock (see `image_locks`), get-or-insert.
    fn image_lock(&self, digest: &str) -> Arc<AsyncMutex<()>> {
        let mut map = self
            .inner
            .image_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        map.entry(digest.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// Sweep a digest's image-lock entry once only the map holds it — the same
    /// hygiene `fetch_with` applies to `inflight`. Call after dropping both the
    /// guard and the local `Arc`.
    fn unlock_sweep(&self, digest: &str) {
        let mut map = self
            .inner
            .image_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = map.get(digest) {
            if Arc::strong_count(existing) == 1 {
                map.remove(digest);
            }
        }
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
                // A concurrent leader already resolved this reference: reuse its
                // result — success or failure — never pulling again (the
                // singleflight's whole point). A failure only lives as long as
                // this flight's cohort; the sweep below retires it, so a later,
                // independent caller retries on a fresh flight.
                Some(done) => done.clone(),
                None => {
                    let resolved = puller().await;
                    slot.done = Some(resolved.clone());
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
    /// reconcile the inventory against the daemon, then walk the ordered
    /// eviction candidates, returning the count actually removed. The janitor
    /// task calls this with `Timestamp::now()`.
    ///
    /// Under `High` pressure the loop is driven by **actual reclamation, not
    /// the plan**: the filesystem is resampled before each conditional
    /// candidate and the walk stops only once genuinely below the high-water
    /// mark. Planned sizes are never trusted as freed — a candidate that 409s
    /// (in use, multiply tagged) or whose shared layers reclaim less than its
    /// reported size simply moves the walk to the next candidate, instead of
    /// leaving the node stuck over the mark selecting the same unremovable
    /// image forever.
    pub(crate) async fn sweep(&self, now: Timestamp) -> usize {
        if let Err(err) = self.reconcile(now).await {
            tracing::warn!(error = %err, "image-cache reconcile failed this sweep; retrying next tick");
        }
        let pressure = *self.inner.pressure.borrow();
        let ttl = core_duration(self.inner.config.ttl);

        let candidates = {
            let st = self.lock_state();
            let views: Vec<EntryView> = st
                .entries
                .values()
                .map(|entry| EntryView {
                    digest: entry.digest.clone(),
                    last_used_at: entry.last_used_at,
                    pinned: st
                        .pins
                        .get(&entry.digest)
                        .is_some_and(|set| !set.is_empty()),
                })
                .collect();
            eviction_candidates(views, now, ttl, pressure)
        };

        let mut evicted = 0;
        for candidate in candidates {
            if !candidate.mandatory {
                // Conditional candidates exist only under `High`: resample the
                // watched filesystems (§9) and stop once genuinely below the
                // mark. `None` (no local reading — a remote daemon) keeps
                // going: disk safety wins (ADR 0010) and every unpinned image
                // goes.
                if let Some(over) = self.bytes_to_free() {
                    if over.is_zero() {
                        break;
                    }
                }
            }
            if self.evict(&candidate.digest, candidate.reason).await {
                evicted += 1;
            }
        }
        evicted
    }

    /// The `High`-pressure byte target: the minimum bytes to fall *strictly
    /// below* the high-water mark ([`crate::pressure::bytes_over_pct`]) over
    /// [`CacheOptions::pressure_paths`], taking the **max** across paths (§9).
    /// Zero means genuinely below the mark. Empty paths or all-failed `statvfs`
    /// → `None`, which a `High` sweep reads as "no local reading" and drops
    /// every unpinned image (disk safety wins).
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
    ///
    /// The whole check-then-remove runs under the digest's image lock, with the
    /// pin recheck inside it: a concurrent `fetch` either pins before we get the
    /// lock (we see the pin and abort) or waits for the lock and finds the image
    /// gone (it re-pulls). Without the lock, a pin landing mid-removal would be
    /// unprotected — the pinning start has no container yet, so no daemon 409
    /// saves it (see `image_locks`).
    async fn evict(&self, digest: &str, reason: &'static str) -> bool {
        let removed = {
            let lock = self.image_lock(digest);
            let _guard = lock.lock().await;
            let id = {
                let st = self.lock_state();
                if st.pins.get(digest).is_some_and(|set| !set.is_empty()) {
                    tracing::debug!(digest, "skipping eviction: image is pinned (§7)");
                    None
                } else {
                    st.entries.get(digest).map(|entry| entry.id.clone())
                }
            };
            let removed = match id {
                None => false,
                Some(id) => {
                    let options = RemoveImageOptionsBuilder::new().force(false).build();
                    match self
                        .inner
                        .docker
                        .remove_image(&id, Some(options), None)
                        .await
                    {
                        Ok(_) => true,
                        // Already gone: treat as a successful eviction (drop the
                        // entry).
                        Err(err) if api::status_code(&err) == Some(404) => true,
                        // In use by a container, or multiple tags share the id:
                        // leave it, the daemon is the backstop.
                        Err(err) if api::status_code(&err) == Some(409) => {
                            tracing::debug!(digest, image = %id, "skipping eviction: image in use or multiply tagged");
                            false
                        }
                        Err(err) => {
                            tracing::warn!(digest, image = %id, error = %err, "image eviction failed; retrying next sweep");
                            false
                        }
                    }
                }
            };
            // Drop the entry *inside* the lock scope: after release, a racing
            // `fetch` may already have re-pulled and re-recorded this digest,
            // and removing that fresh entry would blind the inventory until the
            // next reconcile.
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
        };
        self.unlock_sweep(digest);
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
            // A warm pull pins nothing: an evicted warm image is just latency.
            if let Err(err) = cache.fetch(&image, None).await {
                tracing::info!(%image, error = %err, "PrepareCache warm pull failed (advisory)");
            }
        });
    }

    /// The `EvictImageHint` hint (§7, ADR 0010): evict `digest` if unpinned.
    /// Freely ignored when pinned or unknown — [`ImageCache::evict`] makes the
    /// pin recheck itself, under the digest's image lock. Fire and forget.
    pub(crate) fn evict_hint(&self, digest: String) {
        let cache = self.clone();
        tokio::spawn(async move {
            if !cache.evict(&digest, REASON_HINT).await {
                tracing::debug!(%digest, "evict hint: nothing to evict (pinned, unknown, or in use)");
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
        // Serialize the whole snapshot→write→rename: concurrent persists share
        // one `.json.tmp`, and without this a slower writer could rename an
        // *older* snapshot over a newer one (a stale-but-valid `last_used_at`
        // read back after restart TTL-evicts prematurely). Taking the persist
        // lock before snapshotting makes write order match snapshot order.
        let _persist = self
            .inner
            .persist_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
/// to [`eviction_candidates`] (docker-executor.md §7).
struct EntryView {
    digest: String,
    last_used_at: Timestamp,
    pinned: bool,
}

/// One planned eviction. `mandatory` evictions run unconditionally;
/// conditional ones (the `High`-pressure ahead-of-TTL extras) run only while
/// the sweep's filesystem resample still reads over the high-water mark.
struct Candidate {
    digest: String,
    mandatory: bool,
    /// The metrics `reason` label: [`REASON_TTL`] for idle-past-TTL,
    /// [`REASON_PRESSURE`] for everything a pressure escalation added.
    reason: &'static str,
}

/// The ordered eviction candidates for one sweep (docker-executor.md §7).
/// Pure, so the policy is unit-testable without a daemon; the *stop* condition
/// lives in [`ImageCache::sweep`], driven by actual reclamation rather than
/// planned sizes.
///
/// Rules: **pinned images are never candidates** (and [`ImageCache::evict`]
/// rechecks under the image lock). `Ok` → entries idle at or past `ttl`,
/// mandatory. `High` → those TTL-expired entries first (mandatory), then every
/// other unpinned entry most-stale-first (conditional — the sweep stops once
/// below the high-water mark, or exhausts them when it has no local reading).
/// `Critical` → every unpinned entry, mandatory (§9 "sweep to floor"). All
/// tiers order most-stale-first so partial progress frees the longest-idle
/// images first.
fn eviction_candidates(
    entries: Vec<EntryView>,
    now: Timestamp,
    ttl: Duration,
    pressure: DiskPressure,
) -> Vec<Candidate> {
    let expired = |entry: &EntryView| now.duration_since(entry.last_used_at) >= ttl;
    let mut unpinned: Vec<EntryView> = entries.into_iter().filter(|entry| !entry.pinned).collect();
    unpinned.sort_by_key(|entry| entry.last_used_at);
    match pressure {
        DiskPressure::Ok => unpinned
            .iter()
            .filter(|entry| expired(entry))
            .map(|entry| Candidate {
                digest: entry.digest.clone(),
                mandatory: true,
                reason: REASON_TTL,
            })
            .collect(),
        DiskPressure::Critical => unpinned
            .iter()
            .map(|entry| Candidate {
                reason: if expired(entry) {
                    REASON_TTL
                } else {
                    REASON_PRESSURE
                },
                digest: entry.digest.clone(),
                mandatory: true,
            })
            .collect(),
        DiskPressure::High => {
            // TTL-expired first (they go regardless), then the ahead-of-TTL
            // extras most-stale-first, gated by the sweep's resample.
            let (mandatory, conditional): (Vec<&EntryView>, Vec<&EntryView>) =
                unpinned.iter().partition(|entry| expired(entry));
            mandatory
                .into_iter()
                .map(|entry| Candidate {
                    digest: entry.digest.clone(),
                    mandatory: true,
                    reason: REASON_TTL,
                })
                .chain(conditional.into_iter().map(|entry| Candidate {
                    digest: entry.digest.clone(),
                    mandatory: false,
                    reason: REASON_PRESSURE,
                }))
                .collect()
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

    fn view(digest: &str, last_used_secs: i64, pinned: bool) -> EntryView {
        EntryView {
            digest: digest.to_string(),
            last_used_at: at(last_used_secs),
            pinned,
        }
    }

    fn digests(candidates: &[Candidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| candidate.digest.as_str())
            .collect()
    }

    const TTL: Duration = Duration::from_secs(1800); // 30m

    // ---- TTL boundary (docker-executor.md §7) ------------------------------

    #[test]
    fn ttl_evicts_at_or_past_the_boundary_only() {
        // last_used at t=0; ttl = 1800s. now just below / at / above 1800s.
        let entries = || vec![view("d", 0, false)];
        assert!(eviction_candidates(entries(), at(1799), TTL, DiskPressure::Ok).is_empty());
        for now in [1800, 1801] {
            let plan = eviction_candidates(entries(), at(now), TTL, DiskPressure::Ok);
            assert_eq!(digests(&plan), vec!["d"]);
            assert!(plan[0].mandatory, "a TTL eviction is unconditional");
            assert_eq!(plan[0].reason, REASON_TTL);
        }
    }

    #[test]
    fn pinned_image_past_ttl_survives() {
        let entries = vec![view("d", 0, true)];
        assert!(eviction_candidates(entries, at(100_000), TTL, DiskPressure::Ok).is_empty());
    }

    // ---- pressure ordering (docker-executor.md §7, §9) ---------------------

    #[test]
    fn high_pressure_orders_expired_first_then_most_stale_conditional() {
        // One TTL-expired image plus three fresh ones at now=100_000. The
        // expired one leads and is mandatory (it goes regardless of the
        // watermark); the rest follow most-stale-first as conditional
        // candidates — the sweep stops walking them once the filesystem
        // resample reads below the mark, so the order *is* the policy.
        let entries = vec![
            view("fresh", 99_000, false),
            view("expired", 0, false),
            view("stale", 98_500, false),
            view("staler", 98_400, false),
        ];
        let plan = eviction_candidates(entries, at(100_000), TTL, DiskPressure::High);
        assert_eq!(digests(&plan), vec!["expired", "staler", "stale", "fresh"]);
        assert!(plan[0].mandatory);
        assert_eq!(plan[0].reason, REASON_TTL);
        for candidate in &plan[1..] {
            assert!(!candidate.mandatory, "ahead-of-TTL extras are conditional");
            assert_eq!(candidate.reason, REASON_PRESSURE);
        }
    }

    #[test]
    fn high_pressure_never_lists_pinned_images() {
        let entries = vec![
            view("a", 100_000, false),
            view("b", 100_000, true), // pinned survives even here
            view("c", 100_000, false),
        ];
        let plan = eviction_candidates(entries, at(100_000), TTL, DiskPressure::High);
        assert_eq!(plan.len(), 2);
        assert!(!digests(&plan).contains(&"b"));
    }

    #[test]
    fn critical_drops_all_unpinned_and_only_unpinned() {
        // Everything unpinned is mandatory (§9 "sweep to floor"), expired
        // entries attributed to ttl, the rest to pressure.
        let entries = vec![
            view("a", 100_000, false),
            view("pinned", 0, true),
            view("expired", 0, false),
        ];
        let plan = eviction_candidates(entries, at(100_000), TTL, DiskPressure::Critical);
        assert_eq!(digests(&plan), vec!["expired", "a"]);
        assert!(plan.iter().all(|candidate| candidate.mandatory));
        assert_eq!(plan[0].reason, REASON_TTL);
        assert_eq!(plan[1].reason, REASON_PRESSURE);
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
    async fn singleflight_shares_a_failure_with_its_cohort_then_retries_fresh() {
        // A burst of starts for a missing image must produce one registry
        // request, not N: the flight caches the *failure* for its cohort. Once
        // the cohort disperses (the map entry is swept), a later caller forms a
        // fresh flight and retries.
        let cache = test_cache(2);
        let runs = Arc::new(AtomicUsize::new(0));
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
                    .fetch_with("ghost:latest", || async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Err::<FetchedImage, _>(StartError::Pull {
                            user_error: true,
                            message: "no such image".to_string(),
                        })
                    })
                    .await
            }));
        }
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(
                matches!(
                    result,
                    Err(StartError::Pull {
                        user_error: true,
                        ..
                    })
                ),
                "every waiter shares the cohort's single failure"
            );
        }
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "a failing pull must run once per cohort, not once per waiter"
        );

        // The cohort has dispersed; a fresh caller retries (and may succeed).
        let retried = cache
            .fetch_with("ghost:latest", || async move {
                Ok(fake_fetched("ghost@sha256:found"))
            })
            .await
            .expect("a fresh flight retries rather than inheriting the old failure");
        assert_eq!(retried.digest, "ghost@sha256:found");
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
