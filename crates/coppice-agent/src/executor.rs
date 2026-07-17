//! The container-runtime seam: all correctness logic (fencing, journaling,
//! reconciliation, outcome classification) is executor-agnostic and lives
//! above this trait, so a fake in-process runtime can drive the whole agent
//! deterministically in tests while real Docker slots in behind the same
//! methods later.
//!
//! Container identity rides as labels (ADR 0009): every started container is
//! tagged with its allocation, attempt, and job ids, so [`Executor::observe`]
//! can rebuild the running/exited set by label after an agent restart without
//! trusting agent memory.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;

/// Everything the agent needs to start one container. Ids are carried through
/// as labels so a restarted agent can find the container again (ADR 0009).
#[derive(Debug, Clone)]
pub struct StartSpec {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub image: String,
    /// The container command line, pre-tokenized (argv semantics). Never
    /// empty — the job spec requires it and StartJob copies it verbatim.
    pub command: Vec<String>,
    /// Entrypoint override; `None` runs the image's own entrypoint.
    pub entrypoint: Option<Vec<String>>,
    pub limits: Resources,
    /// Enforced runtime bound; the agent's watchdog kills the container past
    /// it (outcome `RuntimeLimitExceeded`). `None` = unbounded.
    pub max_runtime: Option<Duration>,
}

/// How a container exited, as observed from the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitInfo {
    pub code: i32,
    /// Why the container ended — the evidence the session classifies (ADR
    /// 0013). Replaces the bare `oom_killed` flag so the executor's own disk
    /// kill (§6.2) can be distinguished from a natural exit and a kernel OOM.
    pub cause: ExitCause,
    pub runtime: Duration,
    /// When the container exited (Docker's `FinishedAt`). Feeds the session
    /// janitor's age check when deciding whether an exited container is old
    /// enough to reap (§5); the fake stamps it from its own clock.
    pub finished_at: Timestamp,
}

/// What ended a container, from the runtime's point of view. `classify_exit`
/// maps each to an [`AttemptOutcome`]; only the kernel OOM kill and the
/// executor's disk kill (§6.2) are limit breaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCause {
    /// The container exited on its own (its exit code stands).
    Natural,
    /// The kernel OOM killer terminated it against the memory limit.
    OomKilled,
    /// The disk enforcer killed it for exceeding its writable-layer budget.
    DiskKilled,
}

/// An allocation's process having exited, with how it ended. Carried from
/// [`Executor::next_exit`] through the session runner's exit-watcher channel
/// to [`crate::session::Session::handle_observed_exit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitEvent {
    pub allocation: AllocationId,
    pub exit: ExitInfo,
}

/// A container's observed state (running or exited).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerState {
    Running { runtime: Duration },
    Exited(ExitInfo),
}

/// One container as seen in the runtime, identified by its labels. This is the
/// runtime half of restart reconciliation: it is trusted over the journal for
/// liveness (ADR 0009 — a survivor is never forgotten).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedContainer {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub state: ContainerState,
}

/// The result of a stop, which drives the truth-wins-the-race classification
/// (ADR 0013): the recorded outcome depends on whether *our* stop terminated
/// the container or it had already exited on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
    /// Our SIGTERM/SIGKILL terminated it. The caller assigns the kill reason
    /// (abort vs max-runtime).
    Stopped(ExitInfo),
    /// It had already exited before the stop took effect — the natural
    /// outcome wins.
    AlreadyExited(ExitInfo),
    /// The executor has no record of this allocation (already reaped, or
    /// never started here).
    Unknown,
}

/// Why a container failed to start, mapped to ADR 0013 outcomes. The pull vs.
/// start and user vs. platform distinctions decide the terminal outcome and
/// its default retry policy.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StartError {
    /// The image could not be pulled. `user_error` = bad image reference (no
    /// retry) vs. registry/platform trouble (retry).
    #[error("pull failed (user_error={user_error}): {message}")]
    Pull { user_error: bool, message: String },
    /// The container could not be started after a successful pull.
    #[error("start failed (user_error={user_error}): {message}")]
    Start { user_error: bool, message: String },
}

impl StartError {
    /// The ADR 0013 terminal outcome this start failure maps to.
    pub fn outcome(&self) -> AttemptOutcome {
        match self {
            StartError::Pull { user_error, .. } => AttemptOutcome::PullFailed {
                user_error: *user_error,
            },
            StartError::Start { user_error, .. } => AttemptOutcome::StartFailed {
                user_error: *user_error,
            },
        }
    }
}

/// A runtime-level failure not attributable to a specific container start.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExecutorError {
    #[error("executor operation unimplemented: {0}")]
    Unimplemented(&'static str),
    #[error("executor failure: {0}")]
    Other(String),
}

/// Classify a *naturally* observed exit (ADR 0013): the kernel's OOM kill, or
/// the container's own exit code. Kill-initiated outcomes (abort, max-runtime)
/// are assigned by the caller that initiated the kill, never inferred here.
pub fn classify_exit(exit: &ExitInfo) -> AttemptOutcome {
    match exit.cause {
        ExitCause::OomKilled => AttemptOutcome::MemoryLimitExceeded,
        ExitCause::DiskKilled => AttemptOutcome::DiskLimitExceeded,
        ExitCause::Natural => AttemptOutcome::Exited { code: exit.code },
    }
}

/// The container runtime. Every method is executor-agnostic from the agent
/// core's point of view; classification and journaling happen above it.
///
/// Native async-fn-in-trait is used deliberately (MSRV 1.79): the agent runs
/// one session at a time, so the auto-trait leakage the lint warns about is
/// not a concern here.
#[allow(async_fn_in_trait)]
pub trait Executor: Send + Sync + 'static {
    /// Start a container for `spec`. The intent has already been journaled and
    /// fsynced by the caller (ADR 0009), so a running container always has
    /// durable intent behind it.
    async fn start(&self, spec: StartSpec) -> Result<(), StartError>;

    /// Stop the container for `allocation` (SIGTERM, `grace`, SIGKILL). The
    /// returned [`StopOutcome`] drives truth-wins classification.
    async fn stop(
        &self,
        allocation: AllocationId,
        grace: Duration,
    ) -> Result<StopOutcome, ExecutorError>;

    /// The full set of containers this runtime knows about, by label. The
    /// runtime half of restart reconciliation (ADR 0009).
    async fn observe(&self) -> Result<Vec<ObservedContainer>, ExecutorError>;

    /// Remove an exited container's runtime record. Only the session may decide
    /// to reap, and only *after* the exit is durably journaled: exited
    /// containers are evidence and must survive the crash window (§5). Reaping
    /// an allocation the runtime no longer knows about is a no-op (`Ok`).
    async fn reap(&self, allocation: AllocationId) -> Result<(), ExecutorError>;

    /// Await the next natural container exit. The session loop runs this on a
    /// dedicated task, so the returned future must be `Send`; for runtimes with
    /// no containers it never resolves.
    fn next_exit(&self) -> impl std::future::Future<Output = ExitEvent> + Send;

    /// Summarized image-cache inventory (ADR 0010). Empty in v1.
    fn cache_inventory(&self) -> pb::ImageCacheInventory {
        pb::ImageCacheInventory::default()
    }
}

// ---- FakeExecutor -------------------------------------------------------

/// The internal, *synchronous* state of the fake runtime. Held behind an
/// `Arc<Mutex<_>>` so a test can keep a handle that outlives the agent — which
/// is exactly what proves containers survive an agent restart (their state
/// lives here, not in the agent).
struct FakeInner {
    /// Live containers, by allocation.
    running: std::collections::BTreeMap<AllocationId, ObservedContainer>,
    /// Exited containers still visible in the runtime (not yet reaped).
    exited: std::collections::BTreeMap<AllocationId, ObservedContainer>,
    /// Pre-programmed start failures, consumed in order per allocation.
    fail_starts: std::collections::BTreeMap<AllocationId, StartError>,
    /// Count of containers actually started per allocation. The direct witness
    /// of ADR 0009 idempotency: a re-delivered or duplicate `StartJob` must
    /// never bump this past 1 for a given allocation.
    start_counts: std::collections::BTreeMap<AllocationId, u32>,
    /// The fake's clock. Stamped onto stop-synthesized exits' `finished_at` and
    /// advanced by tests via [`FakeExecutor::set_now`]/[`FakeExecutor::advance`].
    /// Read from the real clock once at construction — the fake is an edge.
    now: Timestamp,
    /// Pre-programmed causes for stop-synthesized exits, consumed on `stop`.
    /// Models the daemon answering a stop with limit-kill evidence — the
    /// kernel's OOM kill (or the disk enforcer's) landing as our stop takes
    /// effect (docker-executor.md §4's carve-out on the 204 path).
    stop_causes: std::collections::BTreeMap<AllocationId, ExitCause>,
}

impl Default for FakeInner {
    fn default() -> FakeInner {
        FakeInner {
            running: std::collections::BTreeMap::new(),
            exited: std::collections::BTreeMap::new(),
            fail_starts: std::collections::BTreeMap::new(),
            start_counts: std::collections::BTreeMap::new(),
            now: Timestamp::now(),
            stop_causes: std::collections::BTreeMap::new(),
        }
    }
}

/// An in-process, deterministic [`Executor`] for tests.
///
/// The sync core ([`FakeInner`]) is wrapped in thin async methods, so crash
/// tests can drive the same shared state without a runtime, and session tests
/// can `await` it. Clone shares the same disk-like inner state AND the same
/// natural-exit queue — the session runner's exit-watcher task is a clone, so
/// it must observe the same [`FakeExecutor::finish`] enqueues.
///
/// [`FakeExecutor::fork`] is the one deliberate exception: it shares the
/// persistent container state but takes a fresh exit queue, modelling an agent
/// restart that reattaches to the same containers while the retired instance's
/// detached watcher drains a queue nothing feeds.
#[derive(Clone, Default)]
pub struct FakeExecutor {
    inner: Arc<Mutex<FakeInner>>,
    /// This instance's natural-exit queue, drained by [`Executor::next_exit`].
    /// Separated from [`FakeInner`] so [`FakeExecutor::fork`] can hand a
    /// restarted agent a private queue (see the type docs).
    exits: Arc<Mutex<VecDeque<ExitEvent>>>,
}

impl FakeExecutor {
    pub fn new() -> FakeExecutor {
        FakeExecutor::default()
    }

    /// A fresh handle over the same persistent container state (running/exited
    /// sets, start counts) but with its own natural-exit queue. Models an agent
    /// restart: the new instance reattaches to the surviving containers, while
    /// the retired instance's still-detached exit watcher polls an orphan queue
    /// no one feeds — so the two never race for the same natural exit.
    pub fn fork(&self) -> FakeExecutor {
        FakeExecutor {
            inner: Arc::clone(&self.inner),
            exits: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Program the next `start` for `allocation` to fail with `err`.
    pub fn fail_next_start(&self, allocation: AllocationId, err: StartError) {
        self.lock().fail_starts.insert(allocation, err);
    }

    /// Finish a running container with `exit`, moving it to the exited set and
    /// queuing a natural-exit notification for [`Executor::next_exit`].
    pub fn finish(&self, allocation: AllocationId, exit: ExitInfo) {
        let mut inner = self.lock();
        if let Some(mut c) = inner.running.remove(&allocation) {
            c.state = ContainerState::Exited(exit);
            inner.exited.insert(allocation, c);
            drop(inner);
            self.exits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_back(ExitEvent { allocation, exit });
        }
    }

    /// Whether a container for `allocation` is currently running.
    pub fn is_running(&self, allocation: AllocationId) -> bool {
        self.lock().running.contains_key(&allocation)
    }

    /// How many containers were actually started for `allocation`. Stays at 1
    /// across duplicate/re-delivered `StartJob`s and an agent restart — the
    /// idempotency witness for the protocol integration test (ADR 0009).
    pub fn start_count(&self, allocation: AllocationId) -> u32 {
        self.lock()
            .start_counts
            .get(&allocation)
            .copied()
            .unwrap_or(0)
    }

    /// The fake's current clock reading — the value stamped onto stop-synthesized
    /// exits' `finished_at`, so tests can hand-build exits that agree with it.
    pub fn now(&self) -> Timestamp {
        self.lock().now
    }

    /// Set the fake's clock (test control over `finished_at` and janitor aging).
    pub fn set_now(&self, now: Timestamp) {
        self.lock().now = now;
    }

    /// Advance the fake's clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        let mut inner = self.lock();
        inner.now += delta;
    }

    /// Pre-program the cause of the next `stop`-synthesized exit for
    /// `allocation`: the stop still reports `Stopped`, but with limit-kill
    /// evidence — the §4 carve-out race where the kernel's (or the disk
    /// enforcer's) kill lands as our stop takes effect.
    pub fn plan_stop_cause(&self, allocation: AllocationId, cause: ExitCause) {
        self.lock().stop_causes.insert(allocation, cause);
    }
}

impl Executor for FakeExecutor {
    async fn start(&self, spec: StartSpec) -> Result<(), StartError> {
        let mut inner = self.lock();
        if let Some(err) = inner.fail_starts.remove(&spec.allocation) {
            return Err(err);
        }
        *inner.start_counts.entry(spec.allocation).or_insert(0) += 1;
        inner.running.insert(
            spec.allocation,
            ObservedContainer {
                allocation: spec.allocation,
                attempt: spec.attempt,
                job: spec.job,
                state: ContainerState::Running {
                    runtime: Duration::ZERO,
                },
            },
        );
        Ok(())
    }

    async fn stop(
        &self,
        allocation: AllocationId,
        _grace: Duration,
    ) -> Result<StopOutcome, ExecutorError> {
        let mut inner = self.lock();
        // Already exited before the stop took effect: the natural outcome wins.
        if let Some(c) = inner.exited.get(&allocation) {
            if let ContainerState::Exited(exit) = c.state {
                return Ok(StopOutcome::AlreadyExited(exit));
            }
        }
        if let Some(mut c) = inner.running.remove(&allocation) {
            let cause = inner
                .stop_causes
                .remove(&allocation)
                .unwrap_or(ExitCause::Natural);
            let exit = ExitInfo {
                code: 137,
                cause,
                runtime: Duration::ZERO,
                finished_at: inner.now,
            };
            c.state = ContainerState::Exited(exit);
            inner.exited.insert(allocation, c);
            return Ok(StopOutcome::Stopped(exit));
        }
        Ok(StopOutcome::Unknown)
    }

    async fn observe(&self) -> Result<Vec<ObservedContainer>, ExecutorError> {
        let inner = self.lock();
        Ok(inner
            .running
            .values()
            .chain(inner.exited.values())
            .copied()
            .collect())
    }

    async fn reap(&self, allocation: AllocationId) -> Result<(), ExecutorError> {
        // Idempotent: reaping an absent allocation is a no-op.
        self.lock().exited.remove(&allocation);
        Ok(())
    }

    fn next_exit(&self) -> impl std::future::Future<Output = ExitEvent> + Send {
        let exits = Arc::clone(&self.exits);
        async move {
            loop {
                if let Some(item) = {
                    let mut guard = exits.lock().unwrap_or_else(|e| e.into_inner());
                    guard.pop_front()
                } {
                    return item;
                }
                // No exit pending: yield and poll again. Tests that need a
                // prompt exit call `finish` before awaiting; the live loop
                // runs this on its own task alongside the command pump.
                tokio::time::sleep(StdDuration::from_millis(1)).await;
            }
        }
    }
}

// ---- DockerExecutor (stub) ----------------------------------------------

/// The real container runtime — a stub for now.
///
/// Every method returns [`ExecutorError::Unimplemented`]. The real Docker
/// implementation lands behind this same trait later, with ADR 0011's
/// locked-down defaults enforced unconditionally: no privileged containers,
/// no host mounts or host network (each container gets its own network
/// namespace), a non-root UID (UID 0 forbidden absent an admin exception),
/// and CPU/memory/disk limits always applied. Those defaults are anchored
/// here so the config surface never grows a knob to relax them.
#[derive(Clone, Default)]
pub struct DockerExecutor;

impl DockerExecutor {
    pub fn new() -> DockerExecutor {
        DockerExecutor
    }
}

impl Executor for DockerExecutor {
    async fn start(&self, _spec: StartSpec) -> Result<(), StartError> {
        Err(StartError::Start {
            user_error: false,
            message: "DockerExecutor is not yet implemented".into(),
        })
    }

    async fn stop(
        &self,
        _allocation: AllocationId,
        _grace: Duration,
    ) -> Result<StopOutcome, ExecutorError> {
        Err(ExecutorError::Unimplemented("DockerExecutor::stop"))
    }

    async fn observe(&self) -> Result<Vec<ObservedContainer>, ExecutorError> {
        Err(ExecutorError::Unimplemented("DockerExecutor::observe"))
    }

    async fn reap(&self, _allocation: AllocationId) -> Result<(), ExecutorError> {
        Err(ExecutorError::Unimplemented("DockerExecutor::reap"))
    }

    fn next_exit(&self) -> impl std::future::Future<Output = ExitEvent> + Send {
        // No containers to watch until the runtime is implemented.
        std::future::pending()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit(code: i32, cause: ExitCause) -> ExitInfo {
        ExitInfo {
            code,
            cause,
            runtime: Duration::from_micros(1),
            finished_at: Timestamp::UNIX_EPOCH,
        }
    }

    #[test]
    fn classify_distinguishes_cause_from_exit_code() {
        assert_eq!(
            classify_exit(&exit(0, ExitCause::Natural)),
            AttemptOutcome::Exited { code: 0 }
        );
        assert_eq!(
            classify_exit(&exit(137, ExitCause::OomKilled)),
            AttemptOutcome::MemoryLimitExceeded
        );
        assert_eq!(
            classify_exit(&exit(137, ExitCause::DiskKilled)),
            AttemptOutcome::DiskLimitExceeded
        );
    }

    #[test]
    fn start_error_maps_to_outcome() {
        assert_eq!(
            StartError::Pull {
                user_error: true,
                message: String::new()
            }
            .outcome(),
            AttemptOutcome::PullFailed { user_error: true }
        );
        assert_eq!(
            StartError::Start {
                user_error: false,
                message: String::new()
            }
            .outcome(),
            AttemptOutcome::StartFailed { user_error: false }
        );
    }

    #[tokio::test]
    async fn fake_start_stop_truth_wins() {
        let exec = FakeExecutor::new();
        let alloc = AllocationId::new();
        exec.start(StartSpec {
            allocation: alloc,
            attempt: AttemptId::new(),
            job: JobId::new(),
            image: "img".into(),
            command: vec!["run".into()],
            entrypoint: None,
            limits: Resources::ZERO,
            max_runtime: None,
        })
        .await
        .unwrap();
        assert!(exec.is_running(alloc));

        // A natural finish before the stop → AlreadyExited wins.
        exec.finish(
            alloc,
            ExitInfo {
                code: 0,
                cause: ExitCause::Natural,
                runtime: Duration::from_micros(5),
                finished_at: Timestamp::UNIX_EPOCH,
            },
        );
        match exec.stop(alloc, Duration::ZERO).await.unwrap() {
            StopOutcome::AlreadyExited(e) => assert_eq!(e.code, 0),
            other => panic!("expected AlreadyExited, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fake_stop_of_running_reports_stopped() {
        let exec = FakeExecutor::new();
        let alloc = AllocationId::new();
        exec.start(StartSpec {
            allocation: alloc,
            attempt: AttemptId::new(),
            job: JobId::new(),
            image: "img".into(),
            command: vec!["run".into()],
            entrypoint: None,
            limits: Resources::ZERO,
            max_runtime: None,
        })
        .await
        .unwrap();
        assert!(matches!(
            exec.stop(alloc, Duration::ZERO).await.unwrap(),
            StopOutcome::Stopped(_)
        ));
        assert!(matches!(
            exec.stop(AllocationId::new(), Duration::ZERO)
                .await
                .unwrap(),
            StopOutcome::Unknown
        ));
    }
}
