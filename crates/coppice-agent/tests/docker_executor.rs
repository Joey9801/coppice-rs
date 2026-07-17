//! Gated real-Docker integration suite for the concrete [`DockerExecutor`]
//! (docker-executor.md §12, S2 deliverable 10).
//!
//! # What this suite proves
//!
//! That the concrete Docker implementation *honors the `Executor` trait
//! contract* end to end against a real daemon: start/exit evidence, the
//! stop-vs-natural-exit race verdict (§4), OOM classification, TERM-trapping
//! stop grace, agent-restart adoption (§5), duplicate-start idempotency, the
//! §6 privilege-escalation posture, the §9 Critical-pressure start refusal, and
//! reap's no-op-on-unknown contract. These are the behaviours that can only be
//! observed against a live kernel/daemon (cgroup OOM kills, `no-new-privileges`,
//! the daemon's atomic race arbitration) — everything *correctness-bearing and
//! pure* (the §3 state table, §4 evidence extraction and start-error mapping,
//! §6 limits/UID translation, §9 threshold arithmetic) is unit-tested WITHOUT a
//! daemon in the modules themselves, per §12. This file deliberately does not
//! re-prove any of that; it only exercises the wired-together whole.
//!
//! # The gate
//!
//! Every test except [`pressure_critical_refuses_start`] begins with
//! `let Some(docker) = harness::docker().await else { return };` — if no Docker
//! daemon is reachable (the default here and in most dev checkouts) the test
//! prints a skip line and returns green. On a Docker-equipped host (or CI with
//! `DOCKER_HOST` pointed at a socket) the body runs for real. The tests are
//! written to be correct on that first real run: generous timeouts, filtered
//! exit draining (a live executor's events task sees die events for *every*
//! coppice container, including other concurrent tests', per §4/§11), and
//! `observe()`-polling where the exit channel is not itself under test.
//!
//! # Reuse (S3–S6)
//!
//! The [`harness`] module is itself a deliverable: later sessions (disk-kill,
//! log-follow, metrics, xfs-quota) reuse `docker()`, `executor()`, `spec()`,
//! the wait/cleanup helpers, and the pinned image consts rather than
//! re-deriving them. Keep additions here backward-compatible.
//!
//! No `bollard` dev-dependency was added: `bollard` is already a normal
//! dependency of `coppice-agent`, and normal dependencies are in scope for a
//! package's integration-test targets (the same way `crash_journal.rs` names
//! `uuid`). The suite drives everything through the public `Executor` API and
//! `executor::docker::api`, reaching for raw `bollard` only to count containers
//! by label as a second, independent witness of idempotency/adoption.

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use bollard::query_parameters::ListContainersOptionsBuilder;
use bollard::Docker;
use tokio::sync::watch;

use coppice_agent::config::ExecutorConfig;
use coppice_agent::executor::docker::api;
use coppice_agent::executor::{
    ContainerState, DockerExecutor, Executor, ExitCause, ExitInfo, ObservedContainer, StartError,
    StartSpec, StopOutcome,
};
use coppice_agent::pressure::DiskPressure;
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration as CoreDuration, Timestamp};

use anyhow::{anyhow, ensure};

/// Shared, S3–S6-reusable scaffolding: the daemon gate, executor construction,
/// spec minting, the exit/observe waiters, and best-effort cleanup.
mod harness {
    use super::*;

    /// Pinned busybox image — a small, single-layer image that carries `sh`,
    /// `id`, `su`, `sleep`, and `trap`, everything the suite needs. Pinned to an
    /// exact tag so a daemon that already has it never re-resolves, and so a
    /// registry change never shifts test behaviour under us.
    pub const BUSYBOX: &str = "busybox:1.37.0";

    /// The Docker endpoint under test. Honors `DOCKER_HOST` (so CI can point at
    /// a specific socket/daemon), else the daemon default.
    pub fn docker_host() -> String {
        std::env::var("DOCKER_HOST").unwrap_or_else(|_| "unix:///var/run/docker.sock".to_string())
    }

    /// The gate. Connects to the daemon and pings it under a 2s timeout; on any
    /// failure (no socket, unreachable, slow) prints a skip line and returns
    /// `None`, so a gated test can `let Some(docker) = harness::docker().await
    /// else { return };` and pass green on a daemon-less machine.
    pub async fn docker() -> Option<Docker> {
        let host = docker_host();
        let docker = match api::connect(&host) {
            Ok(docker) => docker,
            Err(err) => {
                eprintln!("skipping: no reachable Docker daemon (connect {host}: {err})");
                return None;
            }
        };
        match tokio::time::timeout(StdDuration::from_secs(2), docker.ping()).await {
            Ok(Ok(_)) => Some(docker),
            Ok(Err(err)) => {
                eprintln!("skipping: no reachable Docker daemon (ping: {err})");
                None
            }
            Err(_) => {
                eprintln!("skipping: no reachable Docker daemon (ping timed out)");
                None
            }
        }
    }

    /// Build a fresh executor over `docker` with a defaulted [`ExecutorConfig`]
    /// (docker_host `unix:///var/run/docker.sock`, default_uid 65534, pids_limit
    /// 4096 — overridden by nothing) and a fresh node identity. Returns the
    /// pressure [`watch::Sender`] so a test can flip host disk pressure (§9).
    ///
    /// Must be called inside a tokio runtime: [`DockerExecutor::new`] spawns the
    /// events task, and dropping the returned executor aborts it (agent death).
    pub fn executor(docker: Docker) -> (DockerExecutor, watch::Sender<DiskPressure>) {
        let config = ExecutorConfig::default();
        let node = NodeId::new();
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let exec = DockerExecutor::new(docker, &config, node, rx);
        (exec, tx)
    }

    /// A [`StartSpec`] with fresh v7 ids (`AllocationId::new()` mints
    /// `Uuid::now_v7`), the given image/command, no entrypoint override, no
    /// runtime bound. `limits` is applied verbatim (§6).
    pub fn spec(image: &str, cmd: &[&str], limits: Resources) -> StartSpec {
        StartSpec {
            allocation: AllocationId::new(),
            attempt: AttemptId::new(),
            job: JobId::new(),
            image: image.to_string(),
            command: cmd.iter().map(|s| s.to_string()).collect(),
            entrypoint: None,
            limits,
            max_runtime: None,
        }
    }

    /// Await a natural exit for exactly `alloc` via `next_exit`, draining and
    /// discarding exits for *other* allocations (fact §4/§11: this executor's
    /// events task sees die events for every coppice container, including other
    /// concurrent tests'). Fails on a `secs` timeout.
    pub async fn wait_exit(
        exec: &DockerExecutor,
        alloc: AllocationId,
        secs: u64,
    ) -> anyhow::Result<ExitInfo> {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(secs);
        loop {
            let now = tokio::time::Instant::now();
            ensure!(
                now < deadline,
                "timed out after {secs}s waiting for exit of {alloc}"
            );
            match tokio::time::timeout(deadline - now, exec.next_exit()).await {
                Ok(ev) if ev.allocation == alloc => return Ok(ev.exit),
                Ok(_) => continue, // another allocation's exit — keep draining
                Err(_) => anyhow::bail!("timed out after {secs}s waiting for exit of {alloc}"),
            }
        }
    }

    /// Poll `observe()` every 250ms until `alloc` reports [`ContainerState::Exited`],
    /// returning its evidence. Used where the *exit channel* is not the thing
    /// under test (so a startup-race missed die event, §11, cannot flake it):
    /// `observe()` reads the daemon's own container list directly.
    pub async fn wait_observed_exit(
        exec: &DockerExecutor,
        alloc: AllocationId,
        secs: u64,
    ) -> anyhow::Result<ExitInfo> {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(secs);
        loop {
            let observed = exec.observe().await.map_err(|e| anyhow!("observe: {e}"))?;
            if let Some(c) = observed.iter().find(|c| c.allocation == alloc) {
                if let ContainerState::Exited(info) = c.state {
                    return Ok(info);
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out after {secs}s waiting for {alloc} to be observed Exited"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }
    }

    /// Poll `observe()` every 250ms until `alloc` reports
    /// [`ContainerState::Running`], returning the full [`ObservedContainer`] (so
    /// callers can assert the recovered attempt/job ids, §5).
    pub async fn wait_observed_running(
        exec: &DockerExecutor,
        alloc: AllocationId,
        secs: u64,
    ) -> anyhow::Result<ObservedContainer> {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(secs);
        loop {
            let observed = exec.observe().await.map_err(|e| anyhow!("observe: {e}"))?;
            if let Some(c) = observed.iter().find(|c| c.allocation == alloc) {
                if matches!(c.state, ContainerState::Running { .. }) {
                    return Ok(*c);
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out after {secs}s waiting for {alloc} to be observed Running"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }
    }

    /// Raw-`bollard` count of containers (any state) carrying
    /// `coppice.allocation=<alloc>` — an independent witness of the "exactly one
    /// container per allocation" idempotency/adoption invariant (§5), separate
    /// from what `observe()` reports.
    pub async fn count_containers_by_alloc(
        docker: &Docker,
        alloc: AllocationId,
    ) -> anyhow::Result<usize> {
        let mut filters = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!("coppice.allocation={alloc}")],
        );
        let options = ListContainersOptionsBuilder::new()
            .all(true)
            .filters(&filters)
            .build();
        let list = docker
            .list_containers(Some(options))
            .await
            .map_err(|e| anyhow!("list_containers: {e}"))?;
        Ok(list.len())
    }

    /// Best-effort teardown: stop (tiny grace) then reap each allocation,
    /// ignoring every error. Called at the end of every test — including via the
    /// `let r = async {…}.await; cleanup(…).await; r.unwrap();` structure — so a
    /// container is removed even when an assertion fails mid-body.
    pub async fn cleanup(exec: &DockerExecutor, allocs: &[AllocationId]) {
        for &alloc in allocs {
            // A sub-second grace still ceils to a 1s SIGTERM→SIGKILL window
            // (grace_to_secs_ceil), which is plenty to tear a test container down.
            let _ = exec.stop(alloc, CoreDuration::from_millis(1)).await;
            let _ = exec.reap(alloc).await;
        }
    }
}

// ---- 1. exit 0 ----------------------------------------------------------

/// A container that exits 0 reports code 0 / `Natural` with a stamped
/// `finished_at`; its evidence is retained by `observe()` until `reap`, and
/// `reap` is idempotent.
#[tokio::test]
async fn exit_zero() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let sp = harness::spec(harness::BUSYBOX, &["sh", "-c", "exit 0"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        let info = harness::wait_exit(&exec, alloc, 30).await?;
        ensure!(info.code == 0, "expected code 0, got {}", info.code);
        ensure!(
            info.cause == ExitCause::Natural,
            "expected Natural, got {:?}",
            info.cause
        );
        ensure!(
            info.finished_at > Timestamp::UNIX_EPOCH,
            "finished_at was not stamped from the daemon's FinishedAt"
        );

        // Evidence retained until reap (§5): observe still reports it Exited.
        let observed = exec.observe().await?;
        ensure!(
            observed
                .iter()
                .any(|c| c.allocation == alloc && matches!(c.state, ContainerState::Exited(_))),
            "exit evidence must be retained until reap"
        );

        // Reap removes it from the runtime view.
        exec.reap(alloc).await?;
        let observed = exec.observe().await?;
        ensure!(
            !observed.iter().any(|c| c.allocation == alloc),
            "container must be gone from observe() after reap"
        );

        // Reap again: no-op on an already-gone allocation.
        exec.reap(alloc).await?;
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 2. exit N ----------------------------------------------------------

/// A non-zero natural exit surfaces its exact code with cause `Natural`.
#[tokio::test]
async fn exit_nonzero() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let sp = harness::spec(harness::BUSYBOX, &["sh", "-c", "exit 7"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        let info = harness::wait_exit(&exec, alloc, 30).await?;
        ensure!(info.code == 7, "expected code 7, got {}", info.code);
        ensure!(
            info.cause == ExitCause::Natural,
            "expected Natural, got {:?}",
            info.cause
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 3. OOM classification ---------------------------------------------

/// A container that blows a 16 MiB memory limit is kernel-OOM-killed and
/// classified `OomKilled` (§4/§6).
///
/// The hog grows a *shell variable* — `x="$x$x$x$x"` quadruples it each
/// iteration — so the allocation stays in the ash process heap (no external
/// binary, no pipe, nothing that could be accounted elsewhere). With
/// `memory_swap == memory` (§6, no swap headroom) the kernel OOM killer fires
/// against the limit, which is exactly the classification signal. (The
/// documented `cat /dev/zero | head | tail` fallback was not used: it spreads
/// bytes across piped processes and a page cache, a murkier accounting target
/// than a single heap-growing shell.)
#[tokio::test]
async fn oom_classification() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let limits = Resources {
        cpu_millis: 0,
        memory_bytes: 16 * 1024 * 1024,
        disk_bytes: 0,
    };
    let sp = harness::spec(
        harness::BUSYBOX,
        &["sh", "-c", "x=a; while true; do x=\"$x$x$x$x\"; done"],
        limits,
    );
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        let info = harness::wait_exit(&exec, alloc, 30).await?;
        ensure!(
            info.cause == ExitCause::OomKilled,
            "expected OomKilled, got {:?} (code {})",
            info.cause,
            info.code
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 4. stop grace, TERM-trapping container -----------------------------

/// Stopping a running container that traps SIGTERM and exits 0 during the grace
/// window returns `Stopped` (§4: an exit-0 while handling our TERM is still
/// attributed to the stop), and returns promptly — well under the 10s grace,
/// proving the TERM was honored rather than the container SIGKILL'd at grace
/// expiry.
#[tokio::test]
async fn stop_grace_term_trap() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let sp = harness::spec(
        harness::BUSYBOX,
        &[
            "sh",
            "-c",
            "trap 'exit 0' TERM; while true; do sleep 0.2; done",
        ],
        Resources::ZERO,
    );
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_observed_running(&exec, alloc, 30).await?;

        let t0 = tokio::time::Instant::now();
        let outcome = exec.stop(alloc, CoreDuration::from_secs(10)).await?;
        let elapsed = t0.elapsed();

        ensure!(
            matches!(outcome, StopOutcome::Stopped(_)),
            "expected Stopped, got {outcome:?}"
        );
        ensure!(
            elapsed < StdDuration::from_secs(8),
            "stop took {elapsed:?}; TERM was not honored within the grace"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 5. exit races stop: truth wins -------------------------------------

/// A container that has already exited before `stop` is called yields
/// `AlreadyExited` carrying the daemon's real exit code (§4: the natural-exit
/// verdict comes from the daemon's own already-exited answer via the
/// pre-inspect, never from our guess).
#[tokio::test]
async fn exit_races_stop_truth_wins() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let sp = harness::spec(harness::BUSYBOX, &["sh", "-c", "exit 3"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        // Let the exit land first (observe-poll, not the exit channel).
        let observed = harness::wait_observed_exit(&exec, alloc, 30).await?;
        ensure!(
            observed.code == 3,
            "observed exit code {} before stop, expected 3",
            observed.code
        );

        let outcome = exec.stop(alloc, CoreDuration::from_secs(1)).await?;
        match outcome {
            StopOutcome::AlreadyExited(info) => {
                ensure!(
                    info.code == 3,
                    "AlreadyExited must carry the real code 3, got {}",
                    info.code
                );
            }
            other => anyhow::bail!("expected AlreadyExited(code 3), got {other:?}"),
        }
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 6. agent-restart adoption ------------------------------------------

/// A container survives the death of the agent that started it; a fresh
/// executor over a fresh connection re-`observe()`s it Running with the same
/// attempt/job ids (§5), and re-`start()`ing the same spec adopts the survivor
/// (Ok) without creating a second container.
#[tokio::test]
async fn restart_adoption() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let sp = harness::spec(harness::BUSYBOX, &["sleep", "300"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        // Executor A starts the long-lived container, then "dies" (drop).
        let (exec_a, _tx_a) = harness::executor(docker.clone());
        exec_a.start(sp.clone()).await?;
        let before = harness::wait_observed_running(&exec_a, alloc, 30).await?;
        drop(exec_a); // agent death — the container survives daemon-side.

        // Executor B on a fresh connection adopts the survivor.
        let docker_b = api::connect(&harness::docker_host())
            .map_err(|e| anyhow!("fresh connect for executor B: {e}"))?;
        let (exec_b, _tx_b) = harness::executor(docker_b);

        let observed = exec_b.observe().await?;
        let adopted = observed
            .iter()
            .find(|c| c.allocation == alloc)
            .ok_or_else(|| anyhow!("executor B did not observe the surviving container"))?;
        ensure!(
            matches!(adopted.state, ContainerState::Running { .. }),
            "adopted container should be Running, got {:?}",
            adopted.state
        );
        ensure!(
            adopted.attempt == before.attempt && adopted.job == before.job,
            "adopted ids drifted: attempt/job must survive the restart (§5)"
        );

        // Re-start the same spec: adopt-on-name-conflict returns Ok, no new
        // container.
        exec_b.start(sp.clone()).await?;
        let observed = exec_b.observe().await?;
        let count = observed.iter().filter(|c| c.allocation == alloc).count();
        ensure!(count == 1, "observe reports {count} entries, expected 1");

        let raw = harness::count_containers_by_alloc(&docker, alloc).await?;
        ensure!(
            raw == 1,
            "daemon holds {raw} containers for the alloc, expected 1"
        );
        Ok(())
    }
    .await;

    // Clean up via a fresh executor over the shared client — A and B are owned
    // (and possibly dropped) inside the block above.
    let (cleaner, _tx) = harness::executor(docker.clone());
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 7. duplicate start is idempotent -----------------------------------

/// Two sequential `start`s of one spec on one executor both return Ok and leave
/// exactly one Running container (§5).
#[tokio::test]
async fn duplicate_start_idempotent() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker.clone());
    let sp = harness::spec(harness::BUSYBOX, &["sleep", "300"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp.clone()).await?;
        exec.start(sp.clone()).await?; // idempotent — adopt-on-name-conflict.

        let observed = exec.observe().await?;
        let running: Vec<_> = observed.iter().filter(|c| c.allocation == alloc).collect();
        ensure!(
            running.len() == 1,
            "observe reports {} entries, expected 1",
            running.len()
        );
        ensure!(
            matches!(running[0].state, ContainerState::Running { .. }),
            "the single container should be Running, got {:?}",
            running[0].state
        );

        let raw = harness::count_containers_by_alloc(&docker, alloc).await?;
        ensure!(
            raw == 1,
            "daemon holds {raw} containers for the alloc, expected 1"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 8. privilege-escalation denial (§6 posture) ------------------------

/// The §6 posture holds inside a live container: the workload runs as the
/// configured non-root default UID (65534) and cannot regain root. The command
/// exits 0 only if every check passes:
///
/// - `id -u` is 65534 (non-root default UID applied), else exit 10;
/// - `su root -c true` fails (busybox `su` cannot switch UID under
///   `no-new-privileges` with `SETUID`/`SETGID` dropped), else exit 11.
///
/// The setuid-binary variant (build a derived image whose `/suid-busybox` has
/// the setuid bit, and prove `no-new-privileges` neuters it) was **not**
/// implemented: `bollard::build_image` needs a tar-framed build context body,
/// and neither the `tar` crate nor any archive builder is in the workspace, and
/// the brief forbids adding third-party deps beyond the (unneeded) bollard
/// dev-dep. The `su` + euid assertions here are the required core of the posture
/// proof; `no-new-privileges` + dropped `SETUID`/`SETGID` is what defeats a
/// setuid binary too, and `limits.rs` unit tests pin that the flag and CapDrop
/// are actually set on every container.
#[tokio::test]
async fn privilege_escalation_denied() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    let sp = harness::spec(
        harness::BUSYBOX,
        &[
            "sh",
            "-c",
            r#"[ "$(id -u)" = "65534" ] || exit 10; su root -c true 2>/dev/null && exit 11; exit 0"#,
        ],
        Resources::ZERO,
    );
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        let info = harness::wait_exit(&exec, alloc, 30).await?;
        ensure!(
            info.cause == ExitCause::Natural,
            "expected Natural, got {:?}",
            info.cause
        );
        ensure!(
            info.code == 0,
            "posture breach: guard exited {} (10 = not uid 65534, 11 = su to root succeeded)",
            info.code
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 9. Critical pressure refuses start (UNGATED) -----------------------

/// Under `DiskPressure::Critical` the executor refuses a new start with a
/// platform (`user_error: false`) `StartError::Start` (§9).
///
/// **Ungated** — it runs even with no daemon, and must actively PASS on a
/// daemon-less machine rather than skip. The `start` pressure gate fires
/// *before* any daemon call, so no reachable daemon is required to prove the
/// refusal — but the client still has to be *built*. A `tcp://` endpoint is
/// used deliberately: bollard's `connect_with_http` builds a lazy client with
/// no connection attempt, whereas `connect_with_unix` eagerly errors when the
/// socket file is absent (as it is here). The placeholder address is never
/// actually dialed by `start` (the gate refuses first); the events task may
/// fail to reach it in the background, which is harmless and aborted on drop.
#[tokio::test]
async fn pressure_critical_refuses_start() {
    let docker =
        api::connect("tcp://127.0.0.1:2375").expect("http/tcp connect builds a lazy client");
    let (exec, tx) = harness::executor(docker);

    tx.send(DiskPressure::Critical)
        .expect("pressure receiver lives inside the executor");

    let sp = harness::spec(harness::BUSYBOX, &["true"], Resources::ZERO);
    let err = exec
        .start(sp)
        .await
        .expect_err("Critical pressure must refuse the start");

    match err {
        StartError::Start { user_error, .. } => {
            assert!(!user_error, "Critical-pressure refusal is a platform error");
        }
        other => panic!("expected StartError::Start, got {other:?}"),
    }
}

// ---- 10. reap of an unknown allocation is a no-op -----------------------

/// Reaping an allocation the runtime never knew about returns Ok (the trait's
/// no-op-on-unknown contract; the daemon answers the remove with 404).
#[tokio::test]
async fn reap_unknown_is_noop() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let (exec, _tx) = harness::executor(docker);
    exec.reap(AllocationId::new())
        .await
        .expect("reap of an unknown allocation must be Ok");
}
