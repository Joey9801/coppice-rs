//! The concrete Docker executor (docker-executor.md §2).
//!
//! Everything runtime-specific lives under this module; classification,
//! journaling, and fencing stay above the [`crate::executor::Executor`]
//! trait in the session. Later sessions add `cpuset`, `disk`, `cache`,
//! `stats`, and `logs` beside these.
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
pub mod classify;
pub mod events;
pub mod lifecycle;
pub mod limits;
pub mod state;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bollard::Docker;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use coppice_core::id::{AllocationId, NodeId};

use crate::config::ExecutorConfig;
use crate::executor::{
    Executor, ExecutorError, ExitEvent, ObservedContainer, StartError, StartSpec, StopOutcome,
};
use crate::pressure::DiskPressure;

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
/// across restart (§7). (`coppice.disk-mode` is S4's; not added here.)
pub(crate) const LABEL_IMAGE_DIGEST: &str = "coppice.image-digest";

/// The deterministic container name for an allocation (§5): the Docker-level
/// idempotency backstop. `alloc-<uuid>` → `coppice-alloc-<uuid>`.
pub(crate) fn container_name(allocation: AllocationId) -> String {
    format!("coppice-{allocation}")
}

// ---- metrics (docker-executor.md §8.1) ----------------------------------

/// Containers currently running under this executor. A gauge, *pushed* at every
/// mutation of the `running` set (view.rs precedent) rather than sampled.
const AGENT_RUNNING_JOBS: &str = "agent_running_jobs";

/// Register this module's metric names (docker-executor.md §8.1). Part of the
/// crate-level `describe_metrics` fan-out.
pub(crate) fn describe_metrics() {
    metrics::describe_gauge!(
        AGENT_RUNNING_JOBS,
        metrics::Unit::Count,
        "Containers currently running under this agent's executor."
    );
}

/// Point-in-time metric sampling for this module. Part of the crate-level
/// `gather_metrics` fan-out. A no-op: [`AGENT_RUNNING_JOBS`] is pushed on every
/// `running`-set transition (the view.rs push-on-transition convention), so
/// there is nothing to sample here. Later sessions add their own gauges.
pub(crate) fn gather_metrics() {}

// ---- shared state (docker-executor.md §11) ------------------------------

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
}

impl ExecutorState {
    /// Publish the running-count gauge. Call under the lock, at every mutation
    /// of `running`, so the pushed value never lags the set.
    pub(crate) fn push_running_gauge(&self) {
        metrics::gauge!(AGENT_RUNNING_JOBS).set(self.running.len() as f64);
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
    /// The events task, aborted on drop.
    events_task: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The events task holds only clones, so this abort is the sole thing
        // keeping it alive — dropping the last executor handle stops it.
        self.events_task.abort();
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
    /// monitor (`pressure::spawn`) first; see `run_daemon`.
    pub fn new(
        docker: Docker,
        config: &ExecutorConfig,
        node: NodeId,
        pressure: watch::Receiver<DiskPressure>,
    ) -> DockerExecutor {
        let state = Arc::new(Mutex::new(ExecutorState::default()));
        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        // Clones only — never `Arc<Inner>` — so `Inner::drop` can abort it.
        let events_task = events::spawn(docker.clone(), Arc::clone(&state), exit_tx.clone());
        DockerExecutor {
            inner: Arc::new(Inner {
                docker,
                default_uid: config.default_uid,
                pids_limit: config.pids_limit,
                node,
                pressure,
                state,
                exit_tx,
                exit_rx: AsyncMutex::new(exit_rx),
                events_task,
            }),
        }
    }
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
}

/// A container's ids, recovered from its labels (§5). Foreign or malformed
/// labels yield `None` at the call site (warn + skip).
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
}
