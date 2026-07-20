//! The concrete Docker executor (docker-executor.md §2).
//!
//! Everything runtime-specific lives under this module; classification,
//! journaling, and fencing stay above the [`crate::executor::Executor`]
//! trait in the session. Later sessions add `stats` and `logs` beside these.
//!
//! [`DockerExecutor`] is a cheap `Clone` handle over a shared [`Inner`] (the
//! session runner clones it to drive its exit-watcher task). `Inner` owns the
//! bollard client, the shared [`ExecutorState`], the natural-exit channel, and
//! the events task's [`JoinHandle`]; its `Drop` aborts that task, so integration
//! tests that construct and drop executors to model agent restarts leave no
//! orphaned stream behind. The events task therefore captures only *clones*
//! (`docker`, `state`, `exit_tx`) — never an `Arc<Inner>`, which would keep the
//! handle alive and defeat the abort.

pub mod api;
pub mod cache;
pub mod classify;
pub mod cpuset;
pub mod disk;
pub mod events;
pub mod lifecycle;
pub mod limits;
pub mod state;

// The per-container telemetry collectors (docker-executor.md §8.1/§8.2), private
// to the executor: the metrics sampler and the log follower `spawn_collectors`
// wires up at start/adoption.
mod logs;
mod stats;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bollard::models::{ContainerStateStatusEnum, ContainerUpdateBody};
use bollard::query_parameters::ListContainersOptionsBuilder;
use bollard::Docker;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use coppice_core::id::{AllocationId, NodeId};
use coppice_core::time::Timestamp;
use coppice_proto::pb::agent::v1 as pb;

use crate::config::ExecutorConfig;
use crate::executor::{
    Executor, ExecutorError, ExitEvent, ObservedContainer, StartError, StartSpec, StopOutcome,
};
use crate::pressure::DiskPressure;
use crate::telemetry::{FilesystemSink, SinkKind, TelemetryHub};

// ---- container identity (docker-executor.md §5) -------------------------

/// The `coppice.allocation` label (and the `observe`/events list filter): the
/// reconciliation key. Typed `Display` form, `alloc-<uuid>` (ADR 0024).
pub(crate) const LABEL_ALLOCATION: &str = "coppice.allocation";
/// The `coppice.attempt` label — attempt monotonicity across restart (§5).
pub(crate) const LABEL_ATTEMPT: &str = "coppice.attempt";
/// The `coppice.job` label.
pub(crate) const LABEL_JOB: &str = "coppice.job";
/// The `coppice.node` label — this node's identity on every container it owns.
pub(crate) const LABEL_NODE: &str = "coppice.node";
/// The `coppice.image-digest` label — the resolved digest, for cache pinning
/// across restart (§7).
pub(crate) const LABEL_IMAGE_DIGEST: &str = "coppice.image-digest";
/// The `coppice.image-bytes` label — the resolved image's on-disk size in bytes
/// as a decimal string, stamped at create (§8.1). The metrics sampler reports it
/// as `disk_image_bytes` (constant per attempt), and adoption/observe recover it
/// from the surviving container rather than re-inspecting the image.
pub(crate) const LABEL_IMAGE_BYTES: &str = "coppice.image-bytes";
/// Marks containers whose `HostConfig.CpusetCpus` is an exclusive grant. The
/// cpuset itself remains the source of truth rebuilt during recovery (§6.3).
pub(crate) const LABEL_CPU_EXCLUSIVE: &str = "coppice.cpu-exclusive";
/// The `coppice.disk-mode` label — which disk-enforcement strategy (§6.2) chose
/// this container (`"quota"`/`"poll"`), so the poll enforcer resumes for the
/// right containers after an agent restart (§5).
pub(crate) const LABEL_DISK_MODE: &str = "coppice.disk-mode";
/// The `coppice.disk-budget` label — the enforced writable-layer budget in bytes
/// as a decimal string, stamped at create. The poll enforcer must resume
/// enforcement after an agent restart, and the container is the durable record
/// of its own runtime facts (§5); `limits.disk_bytes` is not otherwise
/// recoverable from the container.
pub(crate) const LABEL_DISK_BUDGET: &str = "coppice.disk-budget";

/// The deterministic container name for an allocation (§5): the Docker-level
/// idempotency backstop. `alloc-<uuid>` → `coppice-alloc-<uuid>`.
pub(crate) fn container_name(allocation: AllocationId) -> String {
    format!("coppice-{allocation}")
}

// ---- metrics (docker-executor.md §8.1) ----------------------------------

/// Containers currently running under this executor. A gauge, *pushed* at every
/// mutation of the `running` set (view.rs precedent) rather than sampled.
const AGENT_RUNNING_JOBS: &str = "agent_running_jobs";

/// Log chunks re-appended during a §8.2 restart-recovery replay: chunks whose
/// `at` falls at or below the derived resume boundary, which may already exist in
/// the store. An **error-level** signal only in aggregate — a small count is the
/// unavoidable cost of the at-least-once contract, but sustained growth means the
/// boundary is not advancing. Incremented by the follower (`logs.rs`) and the
/// reap catch-up drain, only when the boundary came from stored data.
pub(crate) const AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL: &str =
    "agent_log_resume_replayed_chunks_total";

/// Follower drains forced past `drain_force_after` (docker-executor.md §8.2): reap
/// proceeded without the follower reaching end-of-stream, so tail logs may be
/// lost. An **error-level** counter — forced tail loss is metered, never silent —
/// incremented by `reap` alongside a `tracing::error!`.
pub(crate) const AGENT_LOG_DRAIN_FORCED_TOTAL: &str = "agent_log_drain_forced_total";

/// Register this module's metric names (docker-executor.md §8.1). Part of the
/// crate-level `describe_metrics` fan-out.
pub(crate) fn describe_metrics() {
    metrics::describe_gauge!(
        AGENT_RUNNING_JOBS,
        metrics::Unit::Count,
        "Containers currently running under this agent's executor."
    );
    metrics::describe_counter!(
        AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL,
        metrics::Unit::Count,
        "Log chunks re-appended during a §8.2 restart-recovery replay (at-least-once)."
    );
    metrics::describe_counter!(
        AGENT_LOG_DRAIN_FORCED_TOTAL,
        metrics::Unit::Count,
        "Log follower drains forced past drain_force_after, risking tail loss (§8.2)."
    );
    disk::describe_metrics();
    cache::describe_metrics();
}

/// Point-in-time metric sampling for this module. Part of the crate-level
/// `gather_metrics` fan-out. A no-op: [`AGENT_RUNNING_JOBS`] is pushed on every
/// `running`-set transition (the view.rs push-on-transition convention), so
/// there is nothing to sample here. Later sessions add their own gauges.
pub(crate) fn gather_metrics() {
    disk::gather_metrics();
    cache::gather_metrics();
}

// ---- shared state (docker-executor.md §11) ------------------------------

/// The telemetry plumbing handed to the executor at construction (docker-executor.md
/// §2, §8): the hub the collectors feed plus the knobs and stores the drain and
/// resume logic need. `None` disables job-telemetry collection entirely — the
/// unit-test executors, and production when **zero** sinks are configured (nothing
/// consumes either stream, so nothing is collected, §8.3). Production passes `Some`
/// whenever any sink is configured; per-kind suppression (`hub.consumes`) handles a
/// partial config (metrics-only or logs-only) inside `spawn_collectors`.
pub struct TelemetryWiring {
    /// The fan-out hub the metrics/log collectors append to.
    pub hub: TelemetryHub,
    /// Every filesystem sink, in config order. The full set that all receive the
    /// attempt-ended marker (§8.4); the §8.2 resume boundary is derived from
    /// [`log_store`](Self::log_store), never `stores.first()`.
    pub stores: Vec<FilesystemSink>,
    /// The §8.2 resume authority: the first filesystem sink whose `kinds` include
    /// [`SinkKind::Logs`](crate::telemetry::SinkKind::Logs), i.e. the first store
    /// that consumes the log stream and so advances its `MAX(at)` boundary. `None`
    /// when no sink consumes logs. Deriving the boundary from a metrics-only
    /// `stores[0]` (whose log `MAX(at)` never advances) would replay the whole
    /// retained history into the real log sink on every adoption/reconnect.
    pub log_store: Option<FilesystemSink>,
    /// How often the metrics sampler polls the Docker stats API (§8.1).
    pub metrics_interval: std::time::Duration,
    /// The forced-drain backstop: past this age since exit-claim, reap proceeds
    /// without the follower's drain, metering the tail loss (§8.2).
    pub drain_force_after: std::time::Duration,
}

/// One allocation's slot in [`ExecutorState::collectors`] (docker-executor.md
/// §8.1/§8.2). `spawn_collectors` inserts a [`Reserved`](CollectorSlot::Reserved)
/// placeholder *synchronously* before the async resume-boundary derivation, then
/// [`activate`](ExecutorState::activate_collectors)s it to
/// [`Active`](CollectorSlot::Active) once the tasks are spawned. The reservation
/// is what lets an exit-claim or a reap that lands mid-initialization observe that
/// a collector is coming: an exit-claim carries its timestamp on the reservation
/// (so the forced-drain clock is never lost), and a reap either retries (the
/// reservation is present) or, once activation removes it, sees `Active`.
pub(crate) enum CollectorSlot {
    /// A placeholder inserted synchronously before the async boundary derivation,
    /// so exit-claim and reap observe that a collector is initializing; carries any
    /// claim or confirmed death that lands mid-initialization.
    Reserved {
        /// When the exit was claimed while still initializing, if it was — carried
        /// forward onto the [`Active`](CollectorSlot::Active) entry at activation.
        exit_claimed_at: Option<Timestamp>,
        /// The confirmed-dead fast-drain signal (§8.2), created **with** the
        /// reservation so [`note_container_dead`](ExecutorState::note_container_dead)
        /// fires the very channel the follower will poll — a death confirmed at
        /// any instant after the reservation is inserted reaches the follower,
        /// with no window between a pre-spawn check and activation in which it
        /// could be observed but not signalled. `spawn_collectors` keeps the
        /// paired receiver for the follower it spawns; activation moves this
        /// sender onto the [`Active`](CollectorSlot::Active) entry.
        died_tx: watch::Sender<bool>,
        /// When the reservation was made. Activation normally follows within
        /// milliseconds (one store query); if the initializing caller was
        /// cancelled mid-await the reservation would otherwise leak and wedge
        /// reap in its retry loop forever, so reap treats a reservation older
        /// than [`lifecycle::RESERVATION_STALE_AFTER`] as abandoned: it removes
        /// the slot and falls through to the catch-up drain. A late activation
        /// then finds its reservation gone and aborts its fresh tasks.
        reserved_at: Timestamp,
    },
    /// The live collectors, once spawned.
    Active(Collectors),
}

/// The per-container telemetry collectors and their drain bookkeeping
/// (docker-executor.md §8.1/§8.2). The [`Active`](CollectorSlot::Active) payload of
/// an [`ExecutorState::collectors`] slot; `spawn_collectors` activates one at
/// start/adoption and `reap` removes it after the container is gone.
pub(crate) struct Collectors {
    /// The container's ids, so `reap` can finalise the attempt without a re-parse.
    pub(crate) ids: ContainerIds,
    /// The metrics sampler, aborted at exit-claim time — a dead container's
    /// samples are noise (§8.1). `None` once aborted, or when no sink consumes
    /// metrics (no sampler was spawned, §8.3).
    pub(crate) sampler: Option<JoinHandle<()>>,
    /// The log follower; kept until reap so it can drain to end-of-stream (§8.2).
    /// `None` when no sink consumes logs — no follower is spawned and `drained` is
    /// pre-set `true` (nothing to wait for, §8.3).
    pub(crate) follower: Option<JoinHandle<()>>,
    /// Set `true` by the follower after its final flush (EOF drain complete);
    /// reap awaits it before removing the container (§8.2). A metrics-only config
    /// (no follower) initialises it `true`.
    pub(crate) drained: watch::Receiver<bool>,
    /// The confirmed-dead fast-drain signal (§8.2), fired by
    /// [`note_container_dead`](ExecutorState::note_container_dead): the follower
    /// races it against the follow stream and, on the signal, abandons the
    /// stream for a single `follow=false` catch-up fetch instead of waiting out
    /// the daemon's slow close of a dead stream. Deliberately **not** fired by
    /// [`note_exit_claimed`](ExecutorState::note_exit_claimed) — a disk kill
    /// claims before its SIGKILL, and draining a still-running container would
    /// end log collection early. Firing with no follower (a metrics-only
    /// config) is a harmless no-op.
    pub(crate) died_tx: watch::Sender<bool>,
    /// When the exit was claimed — the §8.2 `drain_force_after` clock. The first
    /// claim wins (idempotent), so repeated claims never reset it.
    pub(crate) exit_claimed_at: Option<Timestamp>,
    /// Set once reap has forced this follower's drain (docker-executor.md §8.2).
    /// The Force arm aborts/meters/error-logs only on the **first** pass, so a
    /// reap retry after a forced drain (e.g. a later flush timeout) cannot
    /// double-count `agent_log_drain_forced_total`. After a forced abort the
    /// follower's channel is closed, so the next reap hits the closed-channel
    /// catch-up path (which recovers whatever the daemon still retains).
    pub(crate) forced: bool,
}

/// The executor's shared mutable state, guarded by a plain `std::sync::Mutex`.
///
/// Held only for the span of a set mutation — **never across an await** (§11).
/// The agent runs O(dozens) of containers, so a mutex is ample and no lock-free
/// cleverness is warranted.
#[derive(Default)]
pub(crate) struct ExecutorState {
    /// Start sequences in flight *in this process*. `observe` consults it so it
    /// never removes a `created` container whose start is still running here
    /// (that container would otherwise look like crash debris).
    pub(crate) starting: HashSet<AllocationId>,
    /// Exits already surfaced (via `next_exit`, `stop`, or a resync): the §4
    /// best-effort duplicate-suppression set.
    pub(crate) claimed: HashSet<AllocationId>,
    /// Allocations with a running container, for the [`AGENT_RUNNING_JOBS`]
    /// gauge. A snapshot, replaced wholesale by `observe`.
    pub(crate) running: HashSet<AllocationId>,
    /// The per-container telemetry collectors (§8), keyed by allocation. Each slot
    /// is [`Reserved`](CollectorSlot::Reserved) during initialization then
    /// [`Active`](CollectorSlot::Active). Absent when telemetry is disabled
    /// (unit-test executors never populate it).
    pub(crate) collectors: HashMap<AllocationId, CollectorSlot>,
}

impl ExecutorState {
    /// Publish the running-count gauge. Call under the lock, at every mutation
    /// of `running`, so the pushed value never lags the set.
    pub(crate) fn push_running_gauge(&self) {
        metrics::gauge!(AGENT_RUNNING_JOBS).set(self.running.len() as f64);
    }

    /// Note that an allocation's exit was claimed (docker-executor.md §8.2): abort
    /// and drop its metrics sampler (a dead container's samples are noise, §8.1)
    /// and stamp the drain clock. Idempotent — the **first** claim time is kept,
    /// so a re-claim from a later resync never moves the `drain_force_after`
    /// deadline. A no-op when telemetry is disabled or the collector is gone.
    ///
    /// A claim is **not** proof of death — `disk::kill_over_budget` claims
    /// before its SIGKILL is even attempted — so this deliberately does not
    /// fire the follower's fast-drain signal; that is
    /// [`note_container_dead`](Self::note_container_dead), called only from
    /// sites holding proof (a `die` event, an exited/dead listing, terminal
    /// exit evidence).
    ///
    /// A [`Reserved`](CollectorSlot::Reserved) slot (the exit raced collector
    /// initialization) records the first claim time on the reservation; activation
    /// carries it forward and aborts the sampler then (§8.2), so the forced-drain
    /// clock is never lost to the race.
    pub(crate) fn note_exit_claimed(&mut self, allocation: AllocationId, now: Timestamp) {
        match self.collectors.get_mut(&allocation) {
            Some(CollectorSlot::Active(collectors)) => {
                if let Some(sampler) = collectors.sampler.take() {
                    sampler.abort();
                }
                if collectors.exit_claimed_at.is_none() {
                    collectors.exit_claimed_at = Some(now);
                }
            }
            // First claim wins: only stamp a reservation that has none yet.
            Some(CollectorSlot::Reserved {
                exit_claimed_at: exit_claimed_at @ None,
                ..
            }) => {
                *exit_claimed_at = Some(now);
            }
            Some(CollectorSlot::Reserved { .. }) | None => {}
        }
    }

    /// Note that an allocation's container is confirmed **dead** (docker-executor.md
    /// §8.2): fire the follower's fast-drain signal so it abandons the follow
    /// stream for a one-shot `follow=false` catch-up fetch instead of waiting
    /// out the daemon's slow close of a dead stream. Callers must hold proof of
    /// death — a `die` event, a listing filtered on exited/dead status, or
    /// terminal exit evidence from an inspect — never just a claim (a disk kill
    /// claims before its SIGKILL, and draining a still-running container would
    /// end its log collection early). Idempotent: watch keeps only the latest
    /// value. Both slot variants hold the signal sender — the channel is
    /// created with the reservation — so a death confirmed at any point after
    /// the slot exists reaches the follower, spawned or not. A no-op when
    /// telemetry is disabled or the collector is gone.
    pub(crate) fn note_container_dead(&mut self, allocation: AllocationId) {
        match self.collectors.get_mut(&allocation) {
            Some(CollectorSlot::Active(Collectors { died_tx, .. }))
            | Some(CollectorSlot::Reserved { died_tx, .. }) => {
                died_tx.send_replace(true);
            }
            None => {}
        }
    }

    /// Activate a reserved collector slot with its freshly spawned tasks
    /// (docker-executor.md §8.1/§8.2), the second half of `spawn_collectors` under
    /// the state lock. Three cases:
    ///
    /// - slot is [`Reserved`](CollectorSlot::Reserved) → build [`Collectors`]
    ///   carrying the reservation's `exit_claimed_at` **and its `died_tx`** (the
    ///   fast-drain sender lives in the slot from reservation on, so a death
    ///   confirmed during initialization already fired the channel the follower
    ///   polls — activation just moves the sender, §8.2); if the claim is `Some`
    ///   (an exit claimed mid-initialization) abort the sampler immediately — a
    ///   dead container's samples are noise (§8.1) — and store `sampler: None`;
    ///   insert [`Active`](CollectorSlot::Active).
    /// - slot is **absent** (a reap completed and removed the reservation
    ///   meanwhile) → abort both fresh tasks, insert nothing.
    /// - slot is already [`Active`](CollectorSlot::Active) (impossible — we hold the
    ///   reservation; defensive) → warn, abort the fresh tasks, leave the existing
    ///   entry.
    pub(crate) fn activate_collectors(
        &mut self,
        allocation: AllocationId,
        sampler: Option<JoinHandle<()>>,
        follower: Option<JoinHandle<()>>,
        drained: watch::Receiver<bool>,
        ids: ContainerIds,
    ) {
        let abort_fresh = |sampler: Option<JoinHandle<()>>, follower: Option<JoinHandle<()>>| {
            if let Some(sampler) = sampler {
                sampler.abort();
            }
            if let Some(follower) = follower {
                follower.abort();
            }
        };
        match self.collectors.remove(&allocation) {
            Some(CollectorSlot::Reserved {
                exit_claimed_at,
                died_tx,
                ..
            }) => {
                // A claim landed during initialization: the container's death is
                // imminent at worst, so its sampler's samples are noise — abort
                // it now. (A death confirmed mid-initialization already fired
                // `died_tx` in place — the sender lives in the slot — so there
                // is nothing to propagate here; the sender just moves.)
                let sampler = if exit_claimed_at.is_some() {
                    if let Some(sampler) = sampler {
                        sampler.abort();
                    }
                    None
                } else {
                    sampler
                };
                self.collectors.insert(
                    allocation,
                    CollectorSlot::Active(Collectors {
                        ids,
                        sampler,
                        follower,
                        drained,
                        died_tx,
                        exit_claimed_at,
                        forced: false,
                    }),
                );
            }
            None => {
                // A reap removed the reservation while we spawned: nothing to
                // activate, and the fresh tasks have no home — abort them.
                abort_fresh(sampler, follower);
            }
            Some(existing @ CollectorSlot::Active(_)) => {
                // Impossible while we hold the reservation; defensive: keep the
                // existing entry and drop the fresh tasks.
                tracing::warn!(
                    %allocation,
                    "activate_collectors found an already-active slot; dropping the fresh tasks"
                );
                self.collectors.insert(allocation, existing);
                abort_fresh(sampler, follower);
            }
        }
    }
}

/// The shared guts behind every [`DockerExecutor`] clone.
pub(crate) struct Inner {
    pub(crate) docker: Docker,
    /// Fallback UID for images that pin no non-root `USER` (§6).
    pub(crate) default_uid: u32,
    /// `PidsLimit` applied to every container (§6).
    pub(crate) pids_limit: i64,
    /// This node's identity, stamped as the `coppice.node` label.
    pub(crate) node: NodeId,
    /// The shared host disk-pressure signal (§9); `start` refuses under
    /// `Critical`.
    pub(crate) pressure: watch::Receiver<DiskPressure>,
    /// Whole-core allocator, absent when `whole_core_affinity = false`.
    pub(crate) cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    /// Serializes the cpuset plan/create boundary so a fractional allocation
    /// never becomes visible to a concurrent pool update before its container
    /// exists.
    pub(crate) cpu_start: AsyncMutex<()>,
    /// Shared with the events task; never held across an await.
    pub(crate) state: Arc<Mutex<ExecutorState>>,
    /// Natural exits flow from the events task into here; kept on `Inner` as a
    /// keep-alive so [`Executor::next_exit`]'s `recv()` never observes a closed
    /// channel while the executor lives (the events task holds the sender that
    /// actually sends). Held for its lifetime effect, never read.
    #[allow(dead_code)]
    pub(crate) exit_tx: mpsc::UnboundedSender<ExitEvent>,
    /// Drained by [`Executor::next_exit`]. A tokio mutex (not std): the single
    /// watcher task holds it across the `recv().await`.
    pub(crate) exit_rx: AsyncMutex<mpsc::UnboundedReceiver<ExitEvent>>,
    /// The disk-enforcement strategy chosen at startup (§6.2). The lifecycle
    /// layer asks it for the per-job create-time wiring; everything else about
    /// disk enforcement lives behind this seam.
    pub(crate) disk: disk::DiskEnforcer,
    /// The image cache manager (§7): pulls, pinning, eviction, and the
    /// inventory snapshot `cache_inventory` returns. A cheap `Clone` handle — the
    /// janitor task holds its own clone, so this one never keeps the janitor
    /// alive on its own.
    pub(crate) cache: cache::ImageCache,
    /// The events task, aborted on drop.
    events_task: JoinHandle<()>,
    /// The disk-enforcer poll task (§6.2), present only under the poll strategy;
    /// aborted on drop like the events task.
    disk_task: Option<JoinHandle<()>>,
    /// The image-cache janitor task (§7), aborted on drop like the others. It
    /// captures only a [`cache::ImageCache`] clone and a pressure receiver — the
    /// abort is what stops it, so `Inner::drop` leaves no orphaned sweeper.
    cache_task: JoinHandle<()>,
    /// The telemetry plumbing (§8): the hub the collectors feed plus the stores
    /// and knobs the drain/resume logic needs. `None` disables job telemetry
    /// (unit-test executors, and production with zero configured sinks); `Some`
    /// whenever any sink is configured (§8.3).
    pub(crate) telemetry: Option<TelemetryWiring>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The background tasks hold only clones, so these aborts are the sole
        // thing keeping them alive — dropping the last executor handle stops them.
        self.events_task.abort();
        if let Some(task) = self.disk_task.take() {
            task.abort();
        }
        self.cache_task.abort();
        // Abort every surviving per-container collector task (§8): the samplers
        // and followers also hold only clones, so this is what stops them when
        // the last executor handle drops. A `Reserved` slot holds no tasks yet.
        let mut state = lock_state(&self.state);
        for (_, slot) in state.collectors.drain() {
            if let CollectorSlot::Active(collectors) = slot {
                if let Some(sampler) = collectors.sampler {
                    sampler.abort();
                }
                if let Some(follower) = collectors.follower {
                    follower.abort();
                }
            }
        }
    }
}

/// Lock the shared state, recovering from a poisoned mutex (a panic while a set
/// was being mutated leaves the sets usable; the executor is best-effort).
pub(crate) fn lock_state(state: &Mutex<ExecutorState>) -> std::sync::MutexGuard<'_, ExecutorState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The real container runtime (docker-executor.md §3–§5, §11).
///
/// ADR 0011's locked-down posture is enforced unconditionally in `limits.rs`:
/// no privileged containers, no host mounts or host network, a non-root UID
/// (UID 0 forbidden), `no-new-privileges`, and a pinned capability set — with
/// no config knob to relax any of it.
#[derive(Clone)]
pub struct DockerExecutor {
    inner: Arc<Inner>,
}

impl DockerExecutor {
    /// Build the executor over an existing bollard client and pressure signal.
    ///
    /// **Must be called from within a tokio runtime** — it spawns the events
    /// task (§11), which live-tails `docker events` and resyncs via the daemon.
    /// The caller connects the client (`api::connect`) and spawns the pressure
    /// monitor (`pressure::spawn`) first; see `run_daemon`. `docker_host` is the
    /// endpoint already resolved by [`api::resolve_host`] (config → `DOCKER_HOST`
    /// → probed sockets) — the resolved *fact*, distinct from `config.docker_host`,
    /// which is the operator's optional *input* — and is used only to gate the
    /// non-Linux CPU-topology fallback on the transport being local.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        docker: Docker,
        config: &ExecutorConfig,
        docker_host: &str,
        capacity_cpu_millis: u64,
        reservation_cpu_millis: u64,
        node: NodeId,
        pressure: watch::Receiver<DiskPressure>,
        cache: cache::CacheOptions,
        telemetry: Option<TelemetryWiring>,
    ) -> Result<DockerExecutor, ExecutorError> {
        let state = Arc::new(Mutex::new(ExecutorState::default()));
        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        let cpuset = if config.whole_core_affinity {
            let topology = cpuset::Topology::discover(&docker, docker_host)
                .await
                .map_err(|err| {
                    ExecutorError::Other(format!("discovering daemon CPU topology: {err}"))
                })?;
            crate::config::validate_cpu_capacity(capacity_cpu_millis, topology.physical_cores())
                .map_err(ExecutorError::Other)?;
            let allocator = cpuset::Allocator::new(topology, reservation_cpu_millis)
                .map_err(ExecutorError::Other)?;
            let allocator = Arc::new(AsyncMutex::new(allocator));
            recover_cpu_allocations(&docker, &allocator).await?;
            update_fractional_containers(&docker, &mut *allocator.lock().await)
                .await
                .map_err(ExecutorError::Other)?;
            Some(allocator)
        } else {
            None
        };
        // Detect the disk-enforcement strategy once, honoring config (§6.2).
        let disk = disk::DiskEnforcer::detect(&docker, config.disk_enforcement).await?;

        // Clones only — never `Arc<Inner>` — so `Inner::drop` can abort it.
        let events_task = events::spawn(
            docker.clone(),
            Arc::clone(&state),
            cpuset.clone(),
            exit_tx.clone(),
        );
        // The poll enforcer runs under the poll strategy, and also under
        // native quotas when poll-labelled containers survived a restart
        // across a mode flip (e.g. an at-first-inconclusive `auto` probe
        // resolving to quota once images exist) — those containers have no
        // kernel quota, and the label contract (§5) says we resume for them.
        let recovered_poll = disk.mode() == disk::DiskMode::Quota
            && disk::has_recovered_poll_containers(&docker, node).await?;
        if recovered_poll {
            tracing::info!(
                "quota strategy selected but poll-labelled containers survived restart; \
                 running the poll enforcer for them (§6.2)"
            );
        }
        let disk_task = disk::poller_required(disk.mode(), recovered_poll).then(|| {
            disk::spawn(
                docker.clone(),
                Arc::clone(&state),
                cpuset.clone(),
                exit_tx.clone(),
                config.disk_poll_interval,
                node,
                disk.readings(),
            )
        });

        // Build the image cache (§7), reconcile it against the daemon's actual
        // images and re-pin surviving containers *before* the janitor spawns, so
        // the first sweep sees a truthful inventory. The janitor captures only a
        // cache clone and its own pressure receiver — never `Arc<Inner>` — so the
        // abort in `Inner::drop` is what stops it (the mod.rs no-cycle rule).
        let cache = cache::ImageCache::new(docker.clone(), pressure.clone(), cache);
        cache.recover().await;
        let cache_task = cache::spawn_janitor(cache.clone(), pressure.clone());

        Ok(DockerExecutor {
            inner: Arc::new(Inner {
                docker,
                default_uid: config.default_uid,
                pids_limit: config.pids_limit,
                node,
                pressure,
                cpuset,
                cpu_start: AsyncMutex::new(()),
                state,
                exit_tx,
                exit_rx: AsyncMutex::new(exit_rx),
                disk,
                cache,
                events_task,
                disk_task,
                cache_task,
                telemetry,
            }),
        })
    }
}

/// Re-inspect delays for [`settle_oom_flag`]: ~1.6 s total, front-loaded
/// because a lagging `OOMKilled` commit lands within milliseconds of the `die`
/// event when it lands at all (issue #34 measurements: on a healthy daemon the
/// flag is committed *before* the event publishes).
const OOM_FLAG_DELAYS: [std::time::Duration; 6] = [
    std::time::Duration::from_millis(25),
    std::time::Duration::from_millis(50),
    std::time::Duration::from_millis(100),
    std::time::Duration::from_millis(200),
    std::time::Duration::from_millis(400),
    std::time::Duration::from_millis(800),
];

/// Hard wall-clock ceiling on one [`settle_oom_flag`] call, covering the
/// re-inspect *requests* as well as the sleeps. Without it the bound above is
/// illusory: bollard requests carry a 120 s timeout (`api::DOCKER_TIMEOUT`),
/// so six inspects against an unresponsive daemon could pin the events task,
/// a stop, or observe for minutes. Slightly above the summed delays (~1.6 s)
/// to leave normal requests real headroom.
const OOM_SETTLE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

/// Give the daemon a bounded window to commit a lagging `OOMKilled` flag
/// before exit evidence is extracted (issue #34).
///
/// Some daemons set `OOMKilled` from an async OOM-event handler that can lag
/// the `die` event, so an inspect issued at event time can read exit 137 under
/// a memory limit with the flag still unset — which `classify::exit_info`
/// would report as `Natural`, misclassifying a real OOM kill as `Failed`
/// downstream. When `initial` has that racy shape
/// ([`classify::oom_flag_may_lag`]) this re-inspects on a short backoff until
/// the flag commits or the budget runs out, returning the freshest inspect
/// either way; on any other shape it returns `initial` untouched, cost-free.
///
/// The flag remains the sole OOM gate: an exhausted budget still classifies
/// `Natural` (an external SIGKILL of a memory-limited container is
/// indistinguishable by code alone, and fabricating `OomKilled` would breach
/// the documented `exit_info` contract). Callers are the *natural-exit*
/// evidence paths only — the stop post-inspect and the disk enforcer skip it
/// because a 137 there is expected from their own SIGKILL, and burning the
/// budget on every hard kill would be pure latency.
pub(crate) async fn settle_oom_flag(
    docker: &Docker,
    target: &str,
    initial: bollard::models::ContainerInspectResponse,
) -> bollard::models::ContainerInspectResponse {
    if !classify::oom_flag_may_lag(&initial) {
        return initial;
    }
    let mut latest = initial;
    let deadline = tokio::time::Instant::now() + OOM_SETTLE_DEADLINE;
    for delay in OOM_FLAG_DELAYS {
        tokio::time::sleep(delay).await;
        // The per-request clamp is what makes the settle's bound real: a
        // request still in flight at the deadline is abandoned, keeping the
        // evidence already held rather than riding out bollard's 120 s timeout.
        let request = docker.inspect_container(
            target,
            None::<bollard::query_parameters::InspectContainerOptions>,
        );
        match tokio::time::timeout_at(deadline, request).await {
            Ok(Ok(inspect)) => {
                if !classify::oom_flag_may_lag(&inspect) {
                    return inspect;
                }
                latest = inspect;
            }
            // A torn read mid-settle: keep the evidence we already hold.
            Ok(Err(err)) => {
                tracing::debug!(target, error = %err, "re-inspect failed while settling OOMKilled");
            }
            Err(_elapsed) => {
                tracing::debug!(
                    target,
                    "settle deadline hit mid-inspect; keeping prior evidence"
                );
                break;
            }
        }
    }
    tracing::warn!(
        target,
        "exit 137 under a memory limit but OOMKilled never committed; \
         classifying Natural (external SIGKILL, or a daemon that lost the OOM event)"
    );
    latest
}

/// Release an allocation's CPU bookkeeping and push the enlarged shared pool
/// to every surviving fractional container. Idempotent for duplicate exits.
pub(crate) async fn release_cpu(
    docker: &Docker,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    allocation: AllocationId,
) -> Result<(), String> {
    let Some(cpuset) = cpuset else {
        return Ok(());
    };
    let mut allocator = cpuset.lock().await;
    if allocator.release(allocation) {
        update_fractional_containers(docker, &mut allocator).await?;
    }
    Ok(())
}

pub(crate) async fn update_fractional_containers(
    docker: &Docker,
    allocator: &mut cpuset::Allocator,
) -> Result<(), String> {
    let pool = allocator.shared_cpuset();
    let mut errors = Vec::new();
    for allocation in allocator.fractional_allocations() {
        if let Err(err) = docker
            .update_container(
                &container_name(allocation),
                ContainerUpdateBody {
                    cpuset_cpus: Some(pool.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            // A die/reap can race the pool update. Its release path owns
            // removing the allocator entry; absence here must not reject an
            // unrelated whole-core start or prevent later updates in the loop.
            if api::status_code(&err) == Some(404) {
                tracing::debug!(%allocation, "fractional container vanished during cpuset update");
                continue;
            }
            errors.push(format!("updating fractional container {allocation}: {err}"));
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("; "));
    }
    Ok(())
}

async fn recover_cpu_allocations(
    docker: &Docker,
    cpuset: &Arc<AsyncMutex<cpuset::Allocator>>,
) -> Result<(), ExecutorError> {
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec![LABEL_ALLOCATION.to_string()]);
    let options = ListContainersOptionsBuilder::new()
        .all(true)
        .filters(&filters)
        .build();
    let summaries = docker.list_containers(Some(options)).await.map_err(|err| {
        ExecutorError::Other(format!("listing containers for cpuset recovery: {err}"))
    })?;
    let mut allocator = cpuset.lock().await;
    for summary in summaries {
        let Some(allocation) = summary
            .labels
            .as_ref()
            .and_then(|labels| labels.get(LABEL_ALLOCATION))
            .and_then(|raw| raw.parse::<AllocationId>().ok())
        else {
            continue;
        };
        let Some(target) = summary.id.as_deref() else {
            continue;
        };
        let inspect = match docker
            .inspect_container(
                target,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
        {
            Ok(inspect) => inspect,
            Err(err) if api::status_code(&err) == Some(404) => continue,
            Err(err) => {
                return Err(ExecutorError::Other(format!(
                    "inspecting {allocation} for cpuset recovery: {err}"
                )))
            }
        };
        let running = inspect
            .state
            .as_ref()
            .and_then(|state| state.status)
            .is_some_and(|status| {
                matches!(
                    status,
                    ContainerStateStatusEnum::RUNNING
                        | ContainerStateStatusEnum::PAUSED
                        | ContainerStateStatusEnum::RESTARTING
                )
            });
        if !running {
            continue;
        }
        let exclusive = inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .and_then(|labels| labels.get(LABEL_CPU_EXCLUSIVE))
            .is_some_and(|value| value == "true");
        if exclusive {
            let cpus = inspect
                .host_config
                .as_ref()
                .and_then(|host| host.cpuset_cpus.as_deref())
                .ok_or_else(|| {
                    ExecutorError::Other(format!(
                        "surviving exclusive container {allocation} has no HostConfig.CpusetCpus"
                    ))
                })?;
            allocator
                .rebuild_exclusive(allocation, cpus)
                .map_err(ExecutorError::Other)?;
        } else {
            allocator.rebuild_fractional(allocation);
        }
    }
    Ok(())
}

impl Executor for DockerExecutor {
    async fn start(&self, spec: StartSpec) -> Result<(), StartError> {
        lifecycle::start(&self.inner, spec).await
    }

    async fn stop(
        &self,
        allocation: AllocationId,
        grace: coppice_core::time::Duration,
    ) -> Result<StopOutcome, ExecutorError> {
        lifecycle::stop(&self.inner, allocation, grace).await
    }

    async fn observe(&self) -> Result<Vec<ObservedContainer>, ExecutorError> {
        lifecycle::observe(&self.inner).await
    }

    async fn reap(&self, allocation: AllocationId) -> Result<(), ExecutorError> {
        lifecycle::reap(&self.inner, allocation).await
    }

    fn next_exit(&self) -> impl std::future::Future<Output = ExitEvent> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            // One watcher task owns this receiver; the tokio mutex just satisfies
            // `&self`. A `None` cannot happen while `Inner` lives (it holds an
            // `exit_tx`); park forever if it somehow does, matching the trait's
            // "never resolves when there is nothing to watch" contract.
            let mut rx = inner.exit_rx.lock().await;
            match rx.recv().await {
                Some(event) => event,
                None => std::future::pending().await,
            }
        }
    }

    fn cache_inventory(&self) -> pb::ImageCacheInventory {
        self.inner.cache.inventory()
    }

    fn prepare_cache(&self, image: String) {
        self.inner.cache.prepare(image);
    }

    fn evict_image(&self, digest: String) {
        self.inner.cache.evict_hint(digest);
    }
}

impl DockerExecutor {
    /// Run one image-cache janitor sweep with an injected `now`, returning the
    /// number of images evicted (docker-executor.md §7). An integration-test
    /// seam: it lets a test drive a deterministic TTL eviction without waiting
    /// out a live 30-minute clock, and without a tiny live TTL that would evict
    /// other tests' shared images mid-suite.
    #[doc(hidden)]
    pub async fn cache_sweep_at(&self, now: coppice_core::time::Timestamp) -> usize {
        self.inner.cache.sweep(now).await
    }

    /// A monotone count of image pulls the cache manager actually performed
    /// (docker-executor.md §7). An integration-test seam: `n` concurrent starts
    /// of one image must bump this by exactly one (singleflight).
    #[doc(hidden)]
    pub fn cache_pulls_started(&self) -> u64 {
        self.inner.cache.pulls_started()
    }

    /// The number of per-container telemetry collector slots currently held —
    /// [`Reserved`](CollectorSlot::Reserved) or [`Active`](CollectorSlot::Active)
    /// (docker-executor.md §8.1/§8.2). An integration-test seam: it lets the
    /// empty-sinks suppression test prove *positively* that no collector was ever
    /// spawned (0 slots while a container runs), which the no-files assertion
    /// alone cannot — a wasteful collector feeding an empty hub would also write
    /// nothing.
    #[doc(hidden)]
    pub fn collector_slots(&self) -> usize {
        lock_state(&self.inner.state).collectors.len()
    }
}

/// A container's ids, recovered from its labels (§5). Foreign or malformed
/// labels yield `None` at the call site (warn + skip). `Copy` so the collectors
/// (§8) can thread one triple into the sampler, the follower, and the registry
/// entry without cloning.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ContainerIds {
    pub(crate) allocation: AllocationId,
    pub(crate) attempt: coppice_core::id::AttemptId,
    pub(crate) job: coppice_core::id::JobId,
}

/// Recover an allocation/attempt/job triple from a container's label map. Any
/// missing or unparseable member fails the whole parse — a container we cannot
/// fully identify is not ours to touch.
pub(crate) fn parse_container_ids(
    labels: Option<&std::collections::HashMap<String, String>>,
) -> Option<ContainerIds> {
    let labels = labels?;
    Some(ContainerIds {
        allocation: labels.get(LABEL_ALLOCATION)?.parse().ok()?,
        attempt: labels.get(LABEL_ATTEMPT)?.parse().ok()?,
        job: labels.get(LABEL_JOB)?.parse().ok()?,
    })
}

/// Read a container's [`LABEL_IMAGE_BYTES`] leniently (docker-executor.md §8.1):
/// absent or garbage → 0. The image size is the sampler's constant per-attempt
/// `disk_image_bytes`, and a missing/foreign label must not fail collection.
pub(crate) fn image_bytes_from_labels(
    labels: Option<&std::collections::HashMap<String, String>>,
) -> u64 {
    labels
        .and_then(|labels| labels.get(LABEL_IMAGE_BYTES))
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Spawn the per-container telemetry collectors for one allocation
/// (docker-executor.md §8.1/§8.2): a metrics sampler and a log follower, tracked
/// in [`ExecutorState::collectors`]. A no-op when telemetry is disabled, when no
/// sink consumes either stream (§8.3), or a collector already exists for the
/// allocation.
///
/// **Reserve, derive, activate.** A [`CollectorSlot::Reserved`] placeholder is
/// inserted *synchronously* under the lock before the async resume-boundary
/// derivation, so an exit-claim or a reap that lands while we `await` the boundary
/// query observes that a collector is initializing (the reservation carries any
/// mid-flight claim; a reap retries while it is present). The boundary is derived
/// **outside** the lock because on adoption it reads the log store's newest stored
/// timestamp (`max_log_timestamp`, async); a fresh start (`resume == false`) has
/// no earlier logs — boundary `None` (`since = 0`), no replay window — so it does
/// no store I/O. Then, under the lock, [`ExecutorState::activate_collectors`]
/// promotes the reservation to [`CollectorSlot::Active`] (or drops the fresh tasks
/// if a reap removed the reservation meanwhile).
///
/// Per-kind suppression (§8.3): the sampler is spawned only when a sink consumes
/// metrics and the follower only when one consumes logs. With no follower,
/// `drained` is pre-set `true` (nothing to wait for) — reap's verdict then
/// proceeds straight to flush + markers, correct for a metrics-only config (its
/// segments still need the ended marker for retention).
pub(crate) async fn spawn_collectors(
    inner: &Inner,
    ids: ContainerIds,
    container_name: &str,
    resume: bool,
    image_bytes: u64,
    started_at: Option<Timestamp>,
) {
    let Some(telemetry) = inner.telemetry.as_ref() else {
        return;
    };
    // A stream nobody stores is never collected: with no logs consumer no follower
    // is spawned, with no metrics consumer no sampler (§8.3). If neither stream is
    // consumed, reserve nothing and spawn nothing — reap's drain barrier still runs
    // its absent-entry catch-up path, which appends into a hub with no logs
    // consumer (a no-op) and marks the (empty) stores ended, which is acceptable.
    let want_metrics = telemetry.hub.consumes(SinkKind::Metrics);
    let want_logs = telemetry.hub.consumes(SinkKind::Logs);
    if !want_metrics && !want_logs {
        return;
    }

    // Reserve the slot synchronously before the async boundary query, and bail if
    // one is already present (either variant): the reservation both carries a
    // mid-initialization exit-claim and closes the concurrent double-spawn window
    // more cheaply than a post-spawn re-check. The confirmed-dead fast-drain
    // signal (§8.2) is created **with** the reservation: `note_container_dead`
    // fires the sender stored in the slot, and the follower spawned below polls
    // this same channel — so a death confirmed at any instant after this insert
    // reaches the follower, with no seed-vs-activation window. A dead-at-spawn
    // container therefore drains via a single catch-up fetch, never opening a
    // follow stream. (Created even with no follower — firing into no receivers
    // is a harmless no-op.)
    let died_rx = {
        let mut state = lock_state(&inner.state);
        if state.collectors.contains_key(&ids.allocation) {
            return;
        }
        let (died_tx, died_rx) = watch::channel(false);
        state.collectors.insert(
            ids.allocation,
            CollectorSlot::Reserved {
                exit_claimed_at: None,
                died_tx,
                reserved_at: Timestamp::now(),
            },
        );
        died_rx
    };

    // The first log-consuming store is the §8.2 resume authority; a metrics-only
    // `stores[0]` never advances its log boundary (Fix 1).
    let store = telemetry.log_store.clone();
    let (boundary, replay_max) = if resume && want_logs {
        // Adoption: the newest stored log timestamp is the boundary (floored),
        // and its raw value the replay window; else the container's start time,
        // with no replay window (nothing stored to replay against).
        match &store {
            Some(store) => match store.max_log_timestamp(&ids.job, &ids.attempt).await {
                Ok(Some(max)) => (Some(logs::floor_to_second(max)), Some(max)),
                Ok(None) => (started_at.map(logs::floor_to_second), None),
                Err(err) => {
                    tracing::debug!(
                        job = %ids.job,
                        attempt = %ids.attempt,
                        error = %err,
                        "deriving adoption log boundary from the store failed; using start time"
                    );
                    (started_at.map(logs::floor_to_second), None)
                }
            },
            None => (started_at.map(logs::floor_to_second), None),
        }
    } else {
        // Fresh start, or no logs consumer: no earlier logs to replay.
        (None, None)
    };

    let sampler = want_metrics.then(|| {
        stats::spawn_sampler(
            inner.docker.clone(),
            telemetry.hub.clone(),
            ids,
            container_name.to_string(),
            telemetry.metrics_interval,
            image_bytes,
            inner.disk.readings(),
        )
    });
    // No follower ⇒ pre-set `drained = true`: there is nothing for reap to wait on.
    let (follower, drained_rx) = if want_logs {
        let (drained_tx, drained_rx) = watch::channel(false);
        let follower = logs::spawn_follower(
            inner.docker.clone(),
            telemetry.hub.clone(),
            store,
            ids,
            container_name.to_string(),
            boundary,
            replay_max,
            died_rx,
            drained_tx,
        );
        (Some(follower), drained_rx)
    } else {
        (None, watch::channel(true).1)
    };

    // Promote the reservation to Active under the lock (or drop the fresh tasks if
    // a reap removed the reservation meanwhile).
    lock_state(&inner.state).activate_collectors(
        ids.allocation,
        sampler,
        follower,
        drained_rx,
        ids,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_name_round_trips_the_allocation() {
        let alloc = AllocationId::new();
        let name = container_name(alloc);
        // `coppice-` prefix, then the typed `alloc-<uuid>` Display form.
        let stripped = name.strip_prefix("coppice-").expect("coppice- prefix");
        assert_eq!(stripped, alloc.to_string());
        assert_eq!(stripped.parse::<AllocationId>().unwrap(), alloc);
    }

    #[test]
    fn parse_container_ids_needs_all_three_labels() {
        let alloc = AllocationId::new();
        let attempt = coppice_core::id::AttemptId::new();
        let job = coppice_core::id::JobId::new();

        let mut labels = std::collections::HashMap::new();
        labels.insert(LABEL_ALLOCATION.to_string(), alloc.to_string());
        // Missing attempt/job → no parse.
        assert!(parse_container_ids(Some(&labels)).is_none());

        labels.insert(LABEL_ATTEMPT.to_string(), attempt.to_string());
        labels.insert(LABEL_JOB.to_string(), job.to_string());
        let ids = parse_container_ids(Some(&labels)).expect("all three present");
        assert_eq!(ids.allocation, alloc);
        assert_eq!(ids.attempt, attempt);
        assert_eq!(ids.job, job);
    }

    #[test]
    fn parse_container_ids_rejects_foreign_labels() {
        let mut labels = std::collections::HashMap::new();
        labels.insert(LABEL_ALLOCATION.to_string(), "not-an-alloc".to_string());
        labels.insert(LABEL_ATTEMPT.to_string(), "also-bad".to_string());
        labels.insert(LABEL_JOB.to_string(), "nope".to_string());
        assert!(parse_container_ids(Some(&labels)).is_none());
        assert!(parse_container_ids(None).is_none());
    }

    // ---- CollectorSlot state machine (docker-executor.md §8.1/§8.2) ---------

    use std::sync::atomic::{AtomicBool, Ordering};

    fn ids() -> ContainerIds {
        ContainerIds {
            allocation: AllocationId::new(),
            attempt: coppice_core::id::AttemptId::new(),
            job: coppice_core::id::JobId::new(),
        }
    }

    fn ts(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + coppice_core::time::Duration::from_secs(secs)
    }

    /// A never-completing spawned task plus a flag its drop guard sets, so a test
    /// can observe that an `abort` actually cancelled and dropped the task's future.
    fn flagged_handle() -> (JoinHandle<()>, Arc<AtomicBool>) {
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let flag = Arc::new(AtomicBool::new(false));
        // Build the guard OUTSIDE the async block and move it in, so the future owns
        // it and drops it even when aborted before its first poll (a guard built
        // inside the body would never run).
        let guard = DropFlag(flag.clone());
        let handle = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        (handle, flag)
    }

    /// Poll `flag` until the aborted task has run its drop guard, or fail.
    async fn wait_flag(flag: &Arc<AtomicBool>) {
        for _ in 0..500 {
            if flag.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("aborted task never ran its drop guard");
    }

    #[tokio::test]
    async fn reserve_then_claim_then_activate_aborts_sampler_and_carries_claim() {
        let mut st = ExecutorState::default();
        let ids = ids();
        let (died_tx, died_rx) = watch::channel(false);
        st.collectors.insert(
            ids.allocation,
            CollectorSlot::Reserved {
                exit_claimed_at: None,
                died_tx,
                reserved_at: ts(0),
            },
        );
        // A claim lands mid-initialization: recorded on the reservation.
        st.note_exit_claimed(ids.allocation, ts(5));
        match st.collectors.get(&ids.allocation) {
            Some(CollectorSlot::Reserved {
                exit_claimed_at, ..
            }) => {
                assert_eq!(
                    *exit_claimed_at,
                    Some(ts(5)),
                    "the claim rode the reservation"
                )
            }
            _ => panic!("still reserved before activation"),
        }
        assert!(
            !*died_rx.borrow(),
            "a claim alone never fires the fast-drain signal (§8.2 — a disk \
             kill claims before its SIGKILL)"
        );

        let (sampler, sampler_flag) = flagged_handle();
        let (follower, _follower_flag) = flagged_handle();
        st.activate_collectors(
            ids.allocation,
            Some(sampler),
            Some(follower),
            watch::channel(true).1,
            ids,
        );
        assert!(
            !*died_rx.borrow(),
            "activation of a claimed-but-not-dead reservation must NOT fire \
             the fast-drain signal (§8.2)"
        );
        match st.collectors.get(&ids.allocation) {
            Some(CollectorSlot::Active(collectors)) => {
                assert!(
                    collectors.sampler.is_none(),
                    "a claimed reservation aborts the sampler at activation (§8.1)"
                );
                assert_eq!(
                    collectors.exit_claimed_at,
                    Some(ts(5)),
                    "claim carried forward"
                );
                assert!(
                    collectors.follower.is_some(),
                    "the follower survives activation"
                );
                assert!(!collectors.forced, "forced starts false");
            }
            _ => panic!("the reservation activated to Active"),
        }
        wait_flag(&sampler_flag).await;
    }

    #[tokio::test]
    async fn dead_on_reservation_fires_the_channel_immediately_and_survives_activation() {
        let mut st = ExecutorState::default();
        let ids = ids();
        let (died_tx, died_rx) = watch::channel(false);
        st.collectors.insert(
            ids.allocation,
            CollectorSlot::Reserved {
                exit_claimed_at: None,
                died_tx,
                reserved_at: ts(0),
            },
        );
        // Death is confirmed mid-initialization (a die event / resync listing).
        // The sender lives in the reservation, so the channel — the very one
        // the follower polls — fires at once: no seed-vs-activation window in
        // which a death is observed but not signalled (§8.2).
        st.note_container_dead(ids.allocation);
        assert!(
            *died_rx.borrow(),
            "death on a reservation fires the follower's channel immediately, \
             before activation (§8.2)"
        );

        let (follower, _follower_flag) = flagged_handle();
        st.activate_collectors(
            ids.allocation,
            None,
            Some(follower),
            watch::channel(false).1,
            ids,
        );
        // The sender moved onto the Active entry: a (re-)confirmation still
        // reaches the same channel.
        assert!(matches!(
            st.collectors.get(&ids.allocation),
            Some(CollectorSlot::Active(_))
        ));
        st.note_container_dead(ids.allocation);
        assert!(*died_rx.borrow(), "the channel survives activation");
    }

    #[tokio::test]
    async fn reserve_then_reap_removed_then_activate_drops_both_tasks() {
        let mut st = ExecutorState::default();
        let ids = ids();
        st.collectors.insert(
            ids.allocation,
            CollectorSlot::Reserved {
                exit_claimed_at: None,
                died_tx: watch::channel(false).0,
                reserved_at: ts(0),
            },
        );
        // A reap completed and removed the reservation while we spawned.
        st.collectors.remove(&ids.allocation);

        let (sampler, sampler_flag) = flagged_handle();
        let (follower, follower_flag) = flagged_handle();
        st.activate_collectors(
            ids.allocation,
            Some(sampler),
            Some(follower),
            watch::channel(true).1,
            ids,
        );
        assert!(
            st.collectors.is_empty(),
            "activation into an absent slot inserts nothing"
        );
        // Both fresh tasks were aborted.
        wait_flag(&sampler_flag).await;
        wait_flag(&follower_flag).await;
    }

    #[tokio::test]
    async fn note_exit_claimed_first_wins_on_both_variants() {
        let mut st = ExecutorState::default();

        // Reserved: the first claim time is kept.
        let reserved = AllocationId::new();
        st.collectors.insert(
            reserved,
            CollectorSlot::Reserved {
                exit_claimed_at: None,
                died_tx: watch::channel(false).0,
                reserved_at: ts(0),
            },
        );
        st.note_exit_claimed(reserved, ts(1));
        st.note_exit_claimed(reserved, ts(9));
        match st.collectors.get(&reserved) {
            Some(CollectorSlot::Reserved {
                exit_claimed_at, ..
            }) => {
                assert_eq!(
                    *exit_claimed_at,
                    Some(ts(1)),
                    "first claim wins on Reserved"
                )
            }
            _ => panic!("still reserved"),
        }

        // Active: the first claim time is kept and the sampler is aborted once.
        let active = ids();
        let (sampler, sampler_flag) = flagged_handle();
        let (died_tx, died_rx) = watch::channel(false);
        st.collectors.insert(
            active.allocation,
            CollectorSlot::Active(Collectors {
                ids: active,
                sampler: Some(sampler),
                follower: None,
                drained: watch::channel(true).1,
                died_tx,
                exit_claimed_at: None,
                forced: false,
            }),
        );
        st.note_exit_claimed(active.allocation, ts(2));
        st.note_exit_claimed(active.allocation, ts(8));
        match st.collectors.get(&active.allocation) {
            Some(CollectorSlot::Active(collectors)) => {
                assert_eq!(
                    collectors.exit_claimed_at,
                    Some(ts(2)),
                    "first claim wins on Active"
                );
                assert!(
                    collectors.sampler.is_none(),
                    "the sampler is aborted on first claim"
                );
            }
            _ => panic!("still active"),
        }
        assert!(
            !*died_rx.borrow(),
            "a claim alone must NOT fire the fast-drain signal — the disk kill \
             claims before its SIGKILL (§8.2)"
        );
        // Death confirmed (die event / exited listing / terminal evidence):
        // only now does the fast drain fire.
        st.note_container_dead(active.allocation);
        assert!(
            *died_rx.borrow(),
            "confirmed death fires the follower's fast-drain signal (§8.2)"
        );
        wait_flag(&sampler_flag).await;
    }

    #[test]
    fn an_occupied_slot_blocks_a_second_reservation() {
        // Mirrors `spawn_collectors`' guard: `contains_key` ⇒ bail, else reserve.
        fn try_reserve(st: &mut ExecutorState, alloc: AllocationId) -> bool {
            if st.collectors.contains_key(&alloc) {
                return false;
            }
            st.collectors.insert(
                alloc,
                CollectorSlot::Reserved {
                    exit_claimed_at: None,
                    died_tx: watch::channel(false).0,
                    reserved_at: ts(0),
                },
            );
            true
        }

        let mut st = ExecutorState::default();
        // A Reserved slot blocks a second reservation.
        let a = AllocationId::new();
        assert!(try_reserve(&mut st, a), "first reservation succeeds");
        assert!(
            !try_reserve(&mut st, a),
            "a Reserved slot blocks a re-reservation"
        );
        assert!(
            matches!(st.collectors.get(&a), Some(CollectorSlot::Reserved { .. })),
            "the existing reservation is untouched"
        );

        // An Active slot likewise blocks a reservation.
        let b = ids();
        st.collectors.insert(
            b.allocation,
            CollectorSlot::Active(Collectors {
                ids: b,
                sampler: None,
                follower: None,
                drained: watch::channel(true).1,
                died_tx: watch::channel(false).0,
                exit_claimed_at: None,
                forced: false,
            }),
        );
        assert!(
            !try_reserve(&mut st, b.allocation),
            "an Active slot blocks a reservation"
        );
        assert!(matches!(
            st.collectors.get(&b.allocation),
            Some(CollectorSlot::Active(_))
        ));
    }
}
