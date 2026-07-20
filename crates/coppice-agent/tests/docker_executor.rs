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

use coppice_agent::config::{DiskEnforcement, ExecutorConfig, ImageCacheConfig, TelemetryConfig};
use coppice_agent::executor::docker::api;
use coppice_agent::executor::docker::cache::CacheOptions;
use coppice_agent::executor::docker::TelemetryWiring;
use coppice_agent::executor::{
    classify_exit, ContainerState, DockerExecutor, Executor, ExitCause, ExitInfo,
    ObservedContainer, StartError, StartSpec, StopOutcome,
};
use coppice_agent::pressure::DiskPressure;
use coppice_agent::telemetry::{
    FilesystemSink, FilesystemSinkOptions, HubSink, LogQuery, LogStream, MetricSample,
    SinkInstance, SinkKind, StoredLogChunk, Telemetry, TelemetryHub,
};
use coppice_core::attempt::AttemptOutcome;
use coppice_core::bytes::ByteSize;
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
    pub async fn executor(docker: Docker) -> (DockerExecutor, watch::Sender<DiskPressure>) {
        let config = ExecutorConfig {
            whole_core_affinity: false,
            ..Default::default()
        };
        // Existing S2 tests exercise lifecycle behavior and run concurrently;
        // keep them out of the host-global affinity allocator. The dedicated
        // S3 test below opts in explicitly.
        executor_with(docker, config).await
    }

    /// [`executor`] with a caller-supplied [`ExecutorConfig`] — the seam the S4
    /// disk-kill tests use to force `disk_enforcement = poll` with a short
    /// `disk_poll_interval`. Same fresh node identity and `Ok`-initial pressure
    /// channel as [`executor`]; existing tests are unaffected (they still go
    /// through [`executor`]).
    pub async fn executor_with(
        docker: Docker,
        config: ExecutorConfig,
    ) -> (DockerExecutor, watch::Sender<DiskPressure>) {
        let node = NodeId::new();
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        // `None` telemetry: these lifecycle tests do not assert on collection, and
        // the docker-gated telemetry suite (a later phase) wires it explicitly.
        let exec = DockerExecutor::new(docker, &config, 1000, 0, node, rx, cache_options(), None)
            .await
            .expect("initialize Docker executor");
        (exec, tx)
    }

    /// A default in-memory [`CacheOptions`] for the harness (docker-executor.md
    /// §7): no state file (`None`) since no cache test needs persistence across
    /// an executor restart — the reconcile against `docker image ls` rebuilds
    /// the inventory from reality on every construction — empty pressure paths
    /// (the live janitor stays TTL-only under `Ok` pressure), high_pct 85, and
    /// the default 30m TTL / 2-pull config. A tempdir-backed `state_path` would
    /// add nothing a test asserts on, so it is deliberately omitted.
    pub fn cache_options() -> CacheOptions {
        CacheOptions {
            config: ImageCacheConfig::default(),
            state_path: None,
            pressure_paths: Vec::new(),
            high_pct: 85,
        }
    }

    /// The busybox image's on-disk size, pulling it first if the daemon does not
    /// already have it. The poll disk-kill test sizes its budget from this so the
    /// enforced writable budget (`disk − image_size`) lands in a known,
    /// small window (§6.2).
    pub async fn image_size(docker: &Docker, image: &str) -> anyhow::Result<ByteSize> {
        if docker.inspect_image(image).await.is_err() {
            let options = bollard::query_parameters::CreateImageOptionsBuilder::new()
                .from_image(image)
                .build();
            let mut stream = docker.create_image(Some(options), None, None);
            use tokio_stream::StreamExt;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow!("pulling {image}: {e}"))?;
            }
        }
        let inspect = docker
            .inspect_image(image)
            .await
            .map_err(|e| anyhow!("inspecting {image}: {e}"))?;
        // Docker reports the size as a signed integer; this is the crossing
        // into a typed size, mirroring what `lifecycle.rs` does with the same
        // field.
        let size = ByteSize::from_bytes(inspect.size.unwrap_or(0).max(0) as u64);
        ensure!(
            !size.is_zero(),
            "image {image} reported a zero on-disk size"
        );
        Ok(size)
    }

    /// Affinity-enabled executor sized to the physical topology the daemon's
    /// cpusets apply to. Mirrors `Topology::discover`: sysfs sibling groups on
    /// Linux, the daemon's NCPU elsewhere (macOS, where the daemon is a Linux
    /// VM and host topology is the wrong numbering). Used only by the
    /// serial-in-itself S3 cpuset integration test.
    pub async fn physical_cores(docker: &Docker) -> u64 {
        if cfg!(target_os = "linux") {
            let mut groups = std::collections::BTreeSet::new();
            for entry in std::fs::read_dir("/sys/devices/system/cpu").expect("read CPU sysfs") {
                let entry = entry.expect("CPU sysfs entry");
                let name = entry.file_name();
                if !name.to_string_lossy().starts_with("cpu") {
                    continue;
                }
                if let Ok(siblings) =
                    std::fs::read_to_string(entry.path().join("topology/thread_siblings_list"))
                {
                    groups.insert(siblings.trim().to_string());
                }
            }
            u64::try_from(groups.len()).expect("physical core count fits u64")
        } else {
            let info = docker.info().await.expect("daemon /info");
            info.ncpu
                .filter(|n| *n > 0)
                .and_then(|n| u64::try_from(n).ok())
                .expect("daemon /info reported no usable NCPU")
        }
    }

    pub async fn affinity_executor(
        docker: Docker,
    ) -> (DockerExecutor, watch::Sender<DiskPressure>) {
        // The config's docker_host must describe the daemon actually under
        // test: on non-Linux hosts topology discovery gates its NCPU
        // fallback on the transport being local.
        let config = ExecutorConfig {
            docker_host: docker_host(),
            ..Default::default()
        };
        let physical = physical_cores(&docker).await;
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let exec = DockerExecutor::new(
            docker,
            &config,
            physical * 1000,
            0,
            NodeId::new(),
            rx,
            cache_options(),
            None,
        )
        .await
        .expect("initialize affinity executor");
        (exec, tx)
    }

    pub async fn cpuset(docker: &Docker, allocation: AllocationId) -> anyhow::Result<String> {
        let inspect = docker
            .inspect_container(
                &format!("coppice-{allocation}"),
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await?;
        inspect
            .host_config
            .and_then(|host| host.cpuset_cpus)
            .ok_or_else(|| anyhow!("container {allocation} has no CpusetCpus"))
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

    /// The `coppice.image-digest` label a container carries (§5) — the exact
    /// digest the cache pinned for it (§7), so a cache test can name the pin/entry
    /// without re-deriving `digest_of` (which is crate-internal).
    pub async fn image_digest_label(
        docker: &Docker,
        alloc: AllocationId,
    ) -> anyhow::Result<String> {
        let inspect = docker
            .inspect_container(
                &format!("coppice-{alloc}"),
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .map_err(|e| anyhow!("inspect: {e}"))?;
        inspect
            .config
            .and_then(|config| config.labels)
            .and_then(|labels| labels.get("coppice.image-digest").cloned())
            .filter(|digest| !digest.is_empty())
            .ok_or_else(|| anyhow!("container {alloc} has no coppice.image-digest label"))
    }

    /// Best-effort untag/remove of a dedicated test image, so a cache test that
    /// asserts a *pull happened* starts from a known-absent reference. Ignores
    /// every error (the image may not be present at all).
    pub async fn remove_image_best_effort(docker: &Docker, reference: &str) {
        let options = bollard::query_parameters::RemoveImageOptionsBuilder::new()
            .force(true)
            .build();
        let _ = docker.remove_image(reference, Some(options), None).await;
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

    // ---- S6b telemetry scaffolding (docker-executor.md §8) ------------------

    /// The [`AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL`] metric name (docker-executor.md
    /// §8.2). The const is `pub(crate)` in `executor::docker`, so the integration
    /// test names the stable public metric string directly.
    pub const REPLAY_COUNTER: &str = "agent_log_resume_replayed_chunks_total";

    /// Build a telemetry-wired executor over `docker`, its filesystem sink rooted
    /// at `root`. Returns the executor, the pressure sender, the shared hub, and
    /// the sink handle (a read handle over the same segments the collectors write).
    /// The metrics interval is a test-fast 1s and `drain_force_after` a long 10m,
    /// so reap's drain barrier always *waits* for the follower's real EOF — the
    /// forced-drain arm is the Part-1 unit matrix, not this suite.
    pub async fn executor_with_telemetry(
        docker: Docker,
        root: &std::path::Path,
    ) -> (
        DockerExecutor,
        watch::Sender<DiskPressure>,
        TelemetryHub,
        FilesystemSink,
    ) {
        executor_with_telemetry_opts(docker, root, |_| {}).await
    }

    /// [`executor_with_telemetry`] with a [`FilesystemSinkOptions`] mutation seam,
    /// mirroring `fs_sink.rs`'s `sink_with` idiom — the segment-roll test forces a
    /// tiny `segment_max`. Defaults otherwise match production
    /// ([`FilesystemSinkOptions::new`]).
    pub async fn executor_with_telemetry_opts(
        docker: Docker,
        root: &std::path::Path,
        mutate: impl FnOnce(&mut FilesystemSinkOptions),
    ) -> (
        DockerExecutor,
        watch::Sender<DiskPressure>,
        TelemetryHub,
        FilesystemSink,
    ) {
        let config = ExecutorConfig {
            whole_core_affinity: false,
            ..Default::default()
        };
        executor_with_telemetry_full(docker, root, config, mutate).await
    }

    /// [`executor_with_telemetry`] over a **caller-supplied** [`ExecutorConfig`] —
    /// the poll-mode disk-metric test needs `disk_enforcement = Poll` with a short
    /// `disk_poll_interval` so the enforcer's `GET /system/df` sweep populates the
    /// shared `DiskReadings` the samplers read `disk_writable_bytes` from. Same
    /// single both-kinds filesystem sink and fast 1s metrics interval as
    /// [`executor_with_telemetry`]; the `FilesystemSinkOptions` are left at their
    /// production defaults.
    pub async fn executor_with_telemetry_config(
        docker: Docker,
        root: &std::path::Path,
        config: ExecutorConfig,
    ) -> (
        DockerExecutor,
        watch::Sender<DiskPressure>,
        TelemetryHub,
        FilesystemSink,
    ) {
        executor_with_telemetry_full(docker, root, config, |_| {}).await
    }

    /// The shared body behind [`executor_with_telemetry_opts`] and
    /// [`executor_with_telemetry_config`]: build the single both-kinds filesystem
    /// sink (through the `mutate` seam), wire it into a hub, and construct a
    /// telemetry executor over the supplied `config`.
    async fn executor_with_telemetry_full(
        docker: Docker,
        root: &std::path::Path,
        config: ExecutorConfig,
        mutate: impl FnOnce(&mut FilesystemSinkOptions),
    ) -> (
        DockerExecutor,
        watch::Sender<DiskPressure>,
        TelemetryHub,
        FilesystemSink,
    ) {
        let mut opts = FilesystemSinkOptions::new(root.to_path_buf());
        mutate(&mut opts);
        let sink = FilesystemSink::new(opts)
            .await
            .expect("build filesystem telemetry sink");
        // One sink instance for both streams; a small queue is ample for a single
        // test container (§8.3 default is 1024).
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Filesystem(sink.clone()),
                kinds: vec![SinkKind::Metrics, SinkKind::Logs],
            }],
            64,
        );
        let wiring = TelemetryWiring {
            hub: hub.clone(),
            stores: vec![sink.clone()],
            // The single sink consumes logs, so it is also the §8.2 resume authority.
            log_store: Some(sink.clone()),
            metrics_interval: StdDuration::from_secs(1),
            drain_force_after: StdDuration::from_secs(600),
        };
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let exec = DockerExecutor::new(
            docker,
            &config,
            1000,
            0,
            NodeId::new(),
            rx,
            cache_options(),
            Some(wiring),
        )
        .await
        .expect("initialize telemetry executor");
        (exec, tx, hub, sink)
    }

    /// The daemon's own retained log lines for a container, via raw bollard logs
    /// (`follow=false, timestamps=false, stdout+stderr, tail=all`). The "every
    /// daemon-returned chunk is retained" oracle: what the daemon still holds must
    /// be a subsequence of what the store holds (§8.2). Payloads are concatenated
    /// in delivery order and split on newlines, dropping the trailing empty.
    pub async fn daemon_log_lines(docker: &Docker, name: &str) -> anyhow::Result<Vec<String>> {
        use bollard::container::LogOutput;
        use tokio_stream::StreamExt;
        let options = bollard::query_parameters::LogsOptionsBuilder::new()
            .follow(false)
            .stdout(true)
            .stderr(true)
            .timestamps(false)
            .tail("all")
            .build();
        let mut stream = docker.logs(name, Some(options));
        let mut blob: Vec<u8> = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(LogOutput::StdOut { message })
                | Ok(LogOutput::StdErr { message })
                | Ok(LogOutput::Console { message }) => blob.extend_from_slice(&message),
                Ok(LogOutput::StdIn { .. }) => {}
                Err(err) => return Err(anyhow!("daemon logs for {name}: {err}")),
            }
        }
        Ok(lines_of(&blob))
    }

    /// Every stored chunk for `(job, attempt)` across all live segments, in
    /// `(at, insertion order)` — the whole `UNIX_EPOCH..=far-future` range. An
    /// unknown attempt (nothing written yet) reads as empty.
    pub async fn stored_chunks(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
    ) -> Vec<StoredLogChunk> {
        let to = Timestamp::now() + CoreDuration::from_days(3650);
        sink.log_chunks(
            &job,
            &attempt,
            None,
            LogQuery::Range {
                from: Timestamp::UNIX_EPOCH,
                to,
            },
        )
        .await
        .unwrap_or_default()
    }

    /// The stored chunks rendered as log lines (payloads concatenated in stored
    /// order, split on newlines) — the store side of the retention oracle. Chunks
    /// are busybox's whole line-buffered lines; nothing is stripped beyond the
    /// newline framing the split consumes.
    pub fn chunk_lines(chunks: &[StoredLogChunk]) -> Vec<String> {
        let mut blob = Vec::new();
        for chunk in chunks {
            blob.extend_from_slice(&chunk.bytes);
        }
        lines_of(&blob)
    }

    /// Split a payload blob into non-empty newline-delimited lines.
    fn lines_of(blob: &[u8]) -> Vec<String> {
        String::from_utf8_lossy(blob)
            .split('\n')
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// `needle` is a subsequence of `haystack`: every element of `needle` appears
    /// in `haystack` in order (the store may hold strictly more). The §8.2
    /// "no daemon-returned chunk is discarded" oracle.
    pub fn is_subsequence(needle: &[String], haystack: &[String]) -> bool {
        let mut it = haystack.iter();
        needle
            .iter()
            .all(|want| it.by_ref().any(|have| have == want))
    }

    /// Poll `sink` every 250ms until `(job, attempt)` holds at least `min` stored
    /// chunks, returning them. Generous deadline — Colima is slow.
    pub async fn wait_for_chunks(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
        min: usize,
        secs: u64,
    ) -> anyhow::Result<Vec<StoredLogChunk>> {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(secs);
        loop {
            let chunks = stored_chunks(sink, job, attempt).await;
            if chunks.len() >= min {
                return Ok(chunks);
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out after {secs}s waiting for >= {min} stored chunks (have {})",
                chunks.len()
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }
    }

    /// Poll until at least one metric sample exists for `(job, attempt)`, returning
    /// them all.
    pub async fn wait_for_metric(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
        secs: u64,
    ) -> anyhow::Result<Vec<MetricSample>> {
        let to = Timestamp::now() + CoreDuration::from_days(3650);
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(secs);
        loop {
            let samples = sink
                .metric_samples(&job, &attempt, Timestamp::UNIX_EPOCH, to)
                .await
                .unwrap_or_default();
            if !samples.is_empty() {
                return Ok(samples);
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out after {secs}s waiting for a metric sample"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }
    }

    /// The current metric-sample count for `(job, attempt)`.
    pub async fn metric_count(sink: &FilesystemSink, job: JobId, attempt: AttemptId) -> usize {
        let to = Timestamp::now() + CoreDuration::from_days(3650);
        sink.metric_samples(&job, &attempt, Timestamp::UNIX_EPOCH, to)
            .await
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// The whole-second floor (in micros) of the store's current `MAX(at)` over
    /// the attempt's log rows — the §8.2 resume boundary a fresh executor derives
    /// on adoption. `None` when the attempt has no log rows yet.
    pub async fn boundary_floor_micros(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
    ) -> Option<i64> {
        let max = sink
            .max_log_timestamp(&job, &attempt)
            .await
            .ok()
            .flatten()?;
        let micros = max.as_micros();
        Some(micros - micros.rem_euclid(1_000_000))
    }

    /// Assert the §8.2 bounded-duplication contract over stored chunks: with
    /// unique `tick-N` payloads any line appearing more than once is a
    /// restart-recovery replay, so (a) it may appear at most `max_copies` times,
    /// and (b) every occurrence's `at` must fall inside one of the recovery
    /// `boundaries` (each a whole-second floor in micros; the boundary second is
    /// `[floor, floor+1s)`). Uninterrupted collection passes `&[]` boundaries with
    /// `max_copies = 1` — no line may repeat at all.
    pub fn assert_bounded_duplicates(
        chunks: &[StoredLogChunk],
        boundaries: &[i64],
        max_copies: usize,
    ) -> anyhow::Result<()> {
        let mut groups: HashMap<String, Vec<i64>> = HashMap::new();
        for chunk in chunks {
            let line = String::from_utf8_lossy(&chunk.bytes)
                .trim_end_matches('\n')
                .to_string();
            groups.entry(line).or_default().push(chunk.at.as_micros());
        }
        for (line, ats) in &groups {
            if ats.len() <= 1 {
                continue;
            }
            ensure!(
                ats.len() <= max_copies,
                "line {line:?} appears {} times, above the {max_copies} the recovery count permits (§8.2)",
                ats.len()
            );
            for &at in ats {
                ensure!(
                    boundaries
                        .iter()
                        .any(|&lo| at >= lo && at < lo + 1_000_000),
                    "duplicated line {line:?} at {at}µs is outside every recovery boundary second {boundaries:?} (§8.2)"
                );
            }
        }
        Ok(())
    }

    /// Whether an attempt is marked ended in the store (§8.4): its `list_attempts`
    /// row carries `ended_at`.
    pub async fn attempt_ended(sink: &FilesystemSink, job: JobId, attempt: AttemptId) -> bool {
        sink.list_attempts(Some(&job))
            .await
            .map(|attempts| {
                attempts
                    .iter()
                    .any(|a| a.attempt == attempt && a.ended_at.is_some())
            })
            .unwrap_or(false)
    }

    /// The number of segment files an attempt has on disk (§8.4).
    pub async fn attempt_segments(sink: &FilesystemSink, job: JobId, attempt: AttemptId) -> usize {
        sink.list_attempts(Some(&job))
            .await
            .ok()
            .and_then(|attempts| {
                attempts
                    .into_iter()
                    .find(|a| a.attempt == attempt)
                    .map(|a| a.segments)
            })
            .unwrap_or(0)
    }

    /// A process-global [`metrics`] debugging recorder, installed once (a metrics
    /// recorder can only be set once per process, and the gated tests share the
    /// binary). Returns the snapshotter, or `None` if another component already
    /// installed a recorder (nothing does today) — callers then skip counter
    /// assertions rather than fail.
    pub fn metrics_snapshotter() -> Option<&'static metrics_util::debugging::Snapshotter> {
        use metrics_util::debugging::DebuggingRecorder;
        use std::sync::OnceLock;
        static SNAP: OnceLock<Option<metrics_util::debugging::Snapshotter>> = OnceLock::new();
        SNAP.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            match recorder.install() {
                Ok(()) => Some(snapshotter),
                Err(_) => {
                    eprintln!(
                        "skipping counter assertions: a metrics recorder is already installed"
                    );
                    None
                }
            }
        })
        .as_ref()
    }

    /// The current value of a global counter, or `None` when no recorder could be
    /// installed (counter assertions are then skipped).
    pub fn counter_value(name: &str) -> Option<u64> {
        use metrics_util::debugging::DebugValue;
        metrics_snapshotter().map(|snap| {
            snap.snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(key, _unit, _desc, value)| (key.key().name() == name).then_some(value))
                .map(|value| match value {
                    DebugValue::Counter(n) => n,
                    _ => 0,
                })
                .unwrap_or(0)
        })
    }

    /// Build a [`TelemetryWiring`] from a [`Telemetry`] the exact way `lib.rs`'s
    /// `run_daemon` does — the whole point of the production-path test is that the
    /// wiring the executor runs on is assembled here, not hand-rolled with its
    /// `log_store` pre-chosen. `metrics_interval`/`drain_force_after` come from the
    /// same config the sinks were built from.
    pub fn wiring_of(telemetry: &Telemetry, config: &TelemetryConfig) -> TelemetryWiring {
        TelemetryWiring {
            hub: telemetry.hub.clone(),
            stores: telemetry.stores.clone(),
            log_store: telemetry.log_store.clone(),
            metrics_interval: config.metrics_interval,
            drain_force_after: config.drain_force_after,
        }
    }

    /// A telemetry executor over a **caller-supplied** [`TelemetryWiring`] — the
    /// seam the production-build and empty-sinks tests need, where the wiring comes
    /// from [`telemetry::build`](coppice_agent::telemetry::build) or from an empty
    /// config rather than from [`executor_with_telemetry`]'s single-sink shortcut.
    /// Same fresh node identity, `Ok`-initial pressure channel, and cache options as
    /// the rest of the harness.
    pub async fn executor_from_wiring(
        docker: Docker,
        wiring: TelemetryWiring,
    ) -> (DockerExecutor, watch::Sender<DiskPressure>) {
        let config = ExecutorConfig {
            whole_core_affinity: false,
            ..Default::default()
        };
        let (tx, rx) = watch::channel(DiskPressure::Ok);
        let exec = DockerExecutor::new(
            docker,
            &config,
            1000,
            0,
            NodeId::new(),
            rx,
            cache_options(),
            Some(wiring),
        )
        .await
        .expect("initialize telemetry executor from a supplied wiring");
        (exec, tx)
    }

    /// [`stored_chunks`] restricted to one stream (`Some`) or unfiltered (`None`),
    /// over the whole time range — the read side of the §8.2 stdout/stderr tagging.
    pub async fn stored_chunks_of(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
        stream: Option<LogStream>,
    ) -> Vec<StoredLogChunk> {
        let to = Timestamp::now() + CoreDuration::from_days(3650);
        sink.log_chunks(
            &job,
            &attempt,
            stream,
            LogQuery::Range {
                from: Timestamp::UNIX_EPOCH,
                to,
            },
        )
        .await
        .unwrap_or_default()
    }

    /// Every stored metric sample for `(job, attempt)` over the whole time range.
    pub async fn all_metric_samples(
        sink: &FilesystemSink,
        job: JobId,
        attempt: AttemptId,
    ) -> Vec<MetricSample> {
        let to = Timestamp::now() + CoreDuration::from_days(3650);
        sink.metric_samples(&job, &attempt, Timestamp::UNIX_EPOCH, to)
            .await
            .unwrap_or_default()
    }

    /// The number of stored chunks whose `at` falls inside the boundary second
    /// `[floor, floor + 1s)` — the at-most-one-extra-copy bound the §8.2
    /// indistinguishable-occurrences case is allowed (test 6).
    pub fn chunks_in_boundary_second(chunks: &[StoredLogChunk], floor: i64) -> usize {
        chunks
            .iter()
            .filter(|c| {
                let at = c.at.as_micros();
                at >= floor && at < floor + 1_000_000
            })
            .count()
    }

    /// A container's [`LABEL_IMAGE_BYTES`] value (`coppice.image-bytes`), parsed to a
    /// `u64` — the constant per-attempt `disk_image_bytes` the sampler stamps and
    /// adoption recovers (§8.1). Errors if the label is absent or not decimal.
    pub async fn image_bytes_label(docker: &Docker, alloc: AllocationId) -> anyhow::Result<u64> {
        let inspect = docker
            .inspect_container(
                &format!("coppice-{alloc}"),
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .map_err(|e| anyhow!("inspect: {e}"))?;
        let raw = inspect
            .config
            .and_then(|config| config.labels)
            .and_then(|labels| labels.get("coppice.image-bytes").cloned())
            .ok_or_else(|| anyhow!("container {alloc} has no coppice.image-bytes label"))?;
        raw.parse::<u64>()
            .map_err(|e| anyhow!("image-bytes label {raw:?} is not decimal: {e}"))
    }

    /// Whether a live container of the given name still exists daemon-side (§5) — an
    /// independent witness that a failed reap left the container intact (test 4).
    pub async fn container_exists(docker: &Docker, name: &str) -> bool {
        docker
            .inspect_container(
                name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .is_ok()
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
    let (exec, _tx) = harness::executor(docker).await;
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
    let (exec, _tx) = harness::executor(docker).await;
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
    let (exec, _tx) = harness::executor(docker).await;
    let limits = Resources {
        cpu_millis: 0,
        memory: ByteSize::from_mib(16),
        disk: ByteSize::ZERO,
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
    let (exec, _tx) = harness::executor(docker).await;
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
    let (exec, _tx) = harness::executor(docker).await;
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
        let (exec_a, _tx_a) = harness::executor(docker.clone()).await;
        exec_a.start(sp.clone()).await?;
        let before = harness::wait_observed_running(&exec_a, alloc, 30).await?;
        drop(exec_a); // agent death — the container survives daemon-side.

        // Executor B on a fresh connection adopts the survivor.
        let docker_b = api::connect(&harness::docker_host())
            .map_err(|e| anyhow!("fresh connect for executor B: {e}"))?;
        let (exec_b, _tx_b) = harness::executor(docker_b).await;

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
    let (cleaner, _tx) = harness::executor(docker.clone()).await;
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
    let (exec, _tx) = harness::executor(docker.clone()).await;
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
    let (exec, _tx) = harness::executor(docker).await;
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
    let (exec, tx) = harness::executor(docker).await;

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
    let (exec, _tx) = harness::executor(docker).await;
    exec.reap(AllocationId::new())
        .await
        .expect("reap of an unknown allocation must be Ok");
}

// ---- 11. whole-core exclusivity and live fractional updates (§6.3) -----

#[tokio::test]
async fn whole_core_grant_shrinks_and_release_grows_fractional_cpuset() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    if cfg!(not(target_os = "linux"))
        && !["unix://", "npipe://"]
            .iter()
            .any(|scheme| harness::docker_host().starts_with(scheme))
    {
        eprintln!(
            "skipping: whole-core affinity needs a local daemon on non-Linux hosts \
             (remote daemons may hide SMT siblings behind NCPU)"
        );
        return;
    }
    if harness::physical_cores(&docker).await < 2 {
        eprintln!("skipping: cpuset integration test needs at least two physical cores");
        return;
    }
    let (exec, _tx) = harness::affinity_executor(docker.clone()).await;
    let fractional = harness::spec(
        harness::BUSYBOX,
        &["sleep", "300"],
        Resources {
            cpu_millis: 500,
            memory: ByteSize::ZERO,
            disk: ByteSize::ZERO,
        },
    );
    let whole = harness::spec(
        harness::BUSYBOX,
        &["sleep", "300"],
        Resources {
            cpu_millis: 1000,
            memory: ByteSize::ZERO,
            disk: ByteSize::ZERO,
        },
    );
    let fractional_id = fractional.allocation;
    let whole_id = whole.allocation;

    let result: anyhow::Result<()> = async {
        exec.start(fractional).await?;
        harness::wait_observed_running(&exec, fractional_id, 30).await?;
        let full_pool = harness::cpuset(&docker, fractional_id).await?;

        exec.start(whole).await?;
        harness::wait_observed_running(&exec, whole_id, 30).await?;
        let exclusive = harness::cpuset(&docker, whole_id).await?;
        let shrunk_pool = harness::cpuset(&docker, fractional_id).await?;
        ensure!(shrunk_pool != full_pool, "fractional cpuset did not shrink");
        let exclusive_cpus: std::collections::BTreeSet<_> = exclusive.split(',').collect();
        let shared_cpus: std::collections::BTreeSet<_> = shrunk_pool.split(',').collect();
        ensure!(
            exclusive_cpus.is_disjoint(&shared_cpus),
            "exclusive cpuset {exclusive} overlaps fractional pool {shrunk_pool}"
        );

        exec.stop(whole_id, CoreDuration::from_millis(1)).await?;
        let grown_pool = harness::cpuset(&docker, fractional_id).await?;
        ensure!(
            grown_pool == full_pool,
            "fractional pool did not grow back: before={full_pool}, after={grown_pool}"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[fractional_id, whole_id]).await;
    result.unwrap();
}

// ---- 12. disk enforcement, poll strategy (§6.2) -------------------------
//
// Native xfs-quota mode is NOT exercised here and has no CI job: CI runs this
// gated suite without a Docker daemon (every test skips green), and the runners
// have no xfs-on-`pquota` data-root to make `storage_opt: size` enforceable.
// The strategy split is behind the `DiskEnforcer` seam either way — its pure
// pieces (budget arithmetic, the config × probe decision, the over-budget
// verdict) are unit-tested in `executor::docker::disk`. Quota mode is verified
// **manually** on an xfs-`pquota` overlay2 host:
//
//   1. run the agent with `[executor] disk_enforcement = "quota"` — startup
//      must log the `quota` strategy (a non-xfs host fails startup, proving the
//      probe refutation path);
//   2. start a job with a small `disk_bytes` and have it `dd` past its budget —
//      the write fails with `ENOSPC` and the container exits on its own,
//      classified `Exited{code}` (the accepted §6.2 asymmetry vs. the poll
//      kill), with `coppice.disk-mode=quota` on the container.

/// Under the poll strategy, a container that fills its writable layer past the
/// enforced budget is killed outright with cause [`ExitCause::DiskKilled`] →
/// [`AttemptOutcome::DiskLimitExceeded`]; its evidence survives (visible via
/// `observe()` as Exited) until `reap` removes it (§5, §6.2).
#[tokio::test]
async fn poll_mode_disk_kill() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let config = ExecutorConfig {
        whole_core_affinity: false,
        disk_enforcement: DiskEnforcement::Poll,
        disk_poll_interval: StdDuration::from_secs(1),
        ..Default::default()
    };
    let (exec, _tx) = harness::executor_with(docker.clone(), config).await;

    let r: anyhow::Result<()> = async {
        // Budget = 8 MiB of writable layer (disk_bytes − image_size); the
        // workload writes 32 MiB, comfortably over, then idles. The write goes
        // to /tmp (mode 1777 in busybox, and no tmpfs mount in our HostConfig,
        // so it lands in the overlay writable layer): containers run as the
        // non-root default_uid (§6), which cannot write at `/`. `&&` so a
        // failed fill exits the container instead of idling to the timeout.
        let image_size = harness::image_size(&docker, harness::BUSYBOX).await?;
        let limits = Resources {
            cpu_millis: 0,
            memory: ByteSize::ZERO,
            disk: image_size + ByteSize::from_mib(8),
        };
        let sp = harness::spec(
            harness::BUSYBOX,
            &[
                "sh",
                "-c",
                "dd if=/dev/zero of=/tmp/fill bs=1M count=32 && sleep 300",
            ],
            limits,
        );
        let alloc = sp.allocation;

        let inner: anyhow::Result<()> = async {
            exec.start(sp).await?;
            // The poller (1s floor) needs a sweep or two to catch the fill.
            let info = harness::wait_exit(&exec, alloc, 60).await?;
            ensure!(
                info.cause == ExitCause::DiskKilled,
                "expected DiskKilled, got {:?} (code {})",
                info.cause,
                info.code
            );
            ensure!(
                classify_exit(&info) == AttemptOutcome::DiskLimitExceeded,
                "DiskKilled must classify as DiskLimitExceeded, got {:?}",
                classify_exit(&info)
            );

            // Evidence survives until reap (§5): observe still reports it Exited.
            let observed = exec.observe().await?;
            ensure!(
                observed
                    .iter()
                    .any(|c| c.allocation == alloc && matches!(c.state, ContainerState::Exited(_))),
                "disk-killed container must remain as evidence until reap"
            );

            // Reap removes it.
            exec.reap(alloc).await?;
            let observed = exec.observe().await?;
            ensure!(
                !observed.iter().any(|c| c.allocation == alloc),
                "container must be gone from observe() after reap"
            );
            Ok(())
        }
        .await;

        harness::cleanup(&exec, &[alloc]).await;
        inner
    }
    .await;

    r.unwrap();
}

// ---- 13. disk-mode / disk-budget labels are stamped (§5, §6.2) ----------

/// A started container under the poll strategy carries the `coppice.disk-mode`
/// and `coppice.disk-budget` labels, so the poll enforcer can resume for it
/// after an agent restart (§5).
#[tokio::test]
async fn disk_labels_present_on_started_container() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let config = ExecutorConfig {
        whole_core_affinity: false,
        disk_enforcement: DiskEnforcement::Poll,
        disk_poll_interval: StdDuration::from_secs(30),
        ..Default::default()
    };
    let (exec, _tx) = harness::executor_with(docker.clone(), config).await;

    let r: anyhow::Result<()> = async {
        let image_size = harness::image_size(&docker, harness::BUSYBOX).await?;
        let budget = ByteSize::from_mib(8);
        let limits = Resources {
            cpu_millis: 0,
            memory: ByteSize::ZERO,
            disk: image_size + budget,
        };
        let sp = harness::spec(harness::BUSYBOX, &["sleep", "300"], limits);
        let alloc = sp.allocation;

        let inner: anyhow::Result<()> = async {
            exec.start(sp).await?;
            harness::wait_observed_running(&exec, alloc, 30).await?;

            let inspect = docker
                .inspect_container(
                    &format!("coppice-{alloc}"),
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await
                .map_err(|e| anyhow!("inspect: {e}"))?;
            let labels = inspect
                .config
                .and_then(|c| c.labels)
                .ok_or_else(|| anyhow!("container has no labels"))?;

            ensure!(
                labels.get("coppice.disk-mode").map(String::as_str) == Some("poll"),
                "coppice.disk-mode should be poll, got {:?}",
                labels.get("coppice.disk-mode")
            );
            // The label carries a bare decimal byte count, not a humane
            // rendering: the reaper parses it back after an agent restart.
            // Asserting on that spelling is the point of this check.
            let stamped = ByteSize::from_bytes(
                labels
                    .get("coppice.disk-budget")
                    .ok_or_else(|| anyhow!("coppice.disk-budget label missing"))?
                    .parse()
                    .map_err(|e| anyhow!("disk-budget not decimal: {e}"))?,
            );
            ensure!(
                stamped == budget,
                "disk-budget label {stamped} should equal disk − image_size ({budget})"
            );
            Ok(())
        }
        .await;

        harness::cleanup(&exec, &[alloc]).await;
        inner
    }
    .await;

    r.unwrap();
}

// ---- 14. concurrent starts of one image pull it once (§7 singleflight) ---

/// Three concurrent `start`s of the same image trigger exactly one pull: the
/// cache manager's per-reference singleflight collapses them (§7). A dedicated
/// tag (`busybox:musl`, used by no other test) is removed first so the pull is
/// forced, and `cache_pulls_started()` is the witness.
#[tokio::test]
async fn concurrent_starts_pull_once() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    const IMAGE: &str = "busybox:musl";
    // Force the pull: drop the tag before the executor exists so its recover
    // reconcile never adopts it.
    harness::remove_image_best_effort(&docker, IMAGE).await;

    let (exec, _tx) = harness::executor(docker.clone()).await;
    let sp1 = harness::spec(IMAGE, &["true"], Resources::ZERO);
    let sp2 = harness::spec(IMAGE, &["true"], Resources::ZERO);
    let sp3 = harness::spec(IMAGE, &["true"], Resources::ZERO);
    let allocs = [sp1.allocation, sp2.allocation, sp3.allocation];

    let r: anyhow::Result<()> = async {
        let before = exec.cache_pulls_started();
        // Concurrent (tokio::join! polls them on one task without spawning).
        let (r1, r2, r3) = tokio::join!(exec.start(sp1), exec.start(sp2), exec.start(sp3));
        r1?;
        r2?;
        r3?;
        let delta = exec.cache_pulls_started() - before;
        ensure!(
            delta == 1,
            "three concurrent starts of one image must pull exactly once, pulled {delta} times"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &allocs).await;
    r.unwrap();
}

// ---- 15. evict hint respects pins (§7, ADR 0010) ------------------------

/// `EvictImageHint` is honored only when the image is unpinned (§7). A running
/// container pins its image, so the hint is ignored; once the container is
/// stopped and reaped (unpinning it), the same hint is honored. A dedicated tag
/// (`busybox:uclibc`) keeps the eviction from disturbing the shared `busybox`
/// image other tests use.
#[tokio::test]
async fn evict_hint_respects_pins() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    const IMAGE: &str = "busybox:uclibc";
    let (exec, _tx) = harness::executor(docker.clone()).await;
    let sp = harness::spec(IMAGE, &["sleep", "300"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_observed_running(&exec, alloc, 30).await?;
        let digest = harness::image_digest_label(&docker, alloc).await?;

        // Pinned by the running container → the hint is ignored: the image must
        // survive a full second of repeated hints.
        exec.evict_image(digest.clone());
        for _ in 0..5 {
            tokio::time::sleep(StdDuration::from_millis(200)).await;
            ensure!(
                docker.inspect_image(IMAGE).await.is_ok(),
                "a pinned image must not be evicted by a hint"
            );
        }

        // Stop + reap → the pin drains → the hint is now honored.
        exec.stop(alloc, CoreDuration::from_millis(1)).await?;
        exec.reap(alloc).await?;
        exec.evict_image(digest);

        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(15);
        loop {
            if docker.inspect_image(IMAGE).await.is_err() {
                break; // gone — the unpinned hint was honored
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "an unpinned evict hint was not honored within 15s"
            );
            tokio::time::sleep(StdDuration::from_millis(200)).await;
        }
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 16. TTL eviction removes an idle image (§7) ------------------------

/// An unpinned image idle past the TTL is evicted by a janitor sweep (§7). The
/// sweep is driven through the `cache_sweep_at` seam with an injected +31m
/// `now` rather than a tiny live TTL: a sub-second live janitor TTL would evict
/// other tests' shared images every 60s mid-suite, whereas one injected sweep is
/// a single, controlled reclamation. A dedicated image (`hello-world:latest`)
/// is the assertion target.
///
/// (Note: a `cache_sweep_at(+31m)` still evicts *every* idle unpinned image this
/// executor's reconcile adopted — that is the cache manager doing its job, §7 —
/// but images with a live container elsewhere are protected by the daemon's
/// in-use `409`, and anything evicted is merely re-pulled on next use.)
#[tokio::test]
async fn ttl_eviction_removes_image() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    const IMAGE: &str = "hello-world:latest";
    let (exec, _tx) = harness::executor(docker.clone()).await;
    let sp = harness::spec(IMAGE, &["/hello"], Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        // hello-world runs and exits at once; wait for the exit, then read its
        // pinned digest before reaping removes the container.
        harness::wait_observed_exit(&exec, alloc, 30).await?;
        let digest = harness::image_digest_label(&docker, alloc).await?;
        exec.reap(alloc).await?; // release stamps last_used_at

        // One sweep 31 minutes in the future: past the 30m TTL for the idle,
        // now-unpinned image.
        let evicted = exec
            .cache_sweep_at(Timestamp::now() + CoreDuration::from_mins(31))
            .await;
        ensure!(evicted >= 1, "the sweep evicted nothing");
        ensure!(
            docker.inspect_image(IMAGE).await.is_err(),
            "the idle image must be gone after a past-TTL sweep"
        );
        let inventory = exec.cache_inventory();
        ensure!(
            !inventory.images.iter().any(|image| image.digest == digest),
            "the evicted image must no longer appear in the inventory"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 17. inventory lists a pulled image (§7, ADR 0010) ------------------

/// After a start the cache inventory lists the image with a real size and a sane
/// last-used timestamp (§7) — the snapshot `cache_inventory()` returns for
/// heartbeats (ADR 0010). A dedicated tag (`busybox:glibc`) keeps the assertion
/// independent of whatever else the daemon holds.
#[tokio::test]
async fn inventory_lists_pulled_image() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    const IMAGE: &str = "busybox:glibc";
    let (exec, _tx) = harness::executor(docker.clone()).await;
    let sp = harness::spec(IMAGE, &["true"], Resources::ZERO);
    let alloc = sp.allocation;

    let before = Timestamp::now();
    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        let digest = harness::image_digest_label(&docker, alloc).await?;

        let inventory = exec.cache_inventory();
        let cached = inventory
            .images
            .iter()
            .find(|image| image.digest == digest)
            .ok_or_else(|| anyhow!("started image {digest} absent from the inventory"))?;
        ensure!(
            cached.size_bytes > 0,
            "cached image reported a zero size: {}",
            cached.size_bytes
        );
        // last_used_at was stamped at fetch, at or after we started timing.
        let last_used = Timestamp::from_micros(cached.last_used_at_us)
            .ok_or_else(|| anyhow!("last_used_at_us {} is out of range", cached.last_used_at_us))?;
        ensure!(
            last_used >= before,
            "last_used_at {last_used} predates the start of the test"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ========================================================================
// S6b telemetry: gated end-to-end collection, at-least-once restart replay,
// and the reap drain barrier (docker-executor.md §8.2, §8.4, §12).
//
// Every test here uses its own `tempfile::TempDir` telemetry root, the pinned
// `harness::BUSYBOX` image, fresh v7 ids (so container names never collide),
// and the existing skip-gate + cleanup idiom. Assertions are the §8.2 retention
// contract — completeness (every daemon-returned line reaches the store) and
// *bounded* duplication (a replayed occurrence only in the boundary second) —
// never exact counts, since the at-least-once rule permits extra copies.
// ========================================================================

/// A container that prints a unique `tick-N` marker roughly every 200ms, forever.
const PRINTER: &[&str] = &[
    "sh",
    "-c",
    "i=0; while true; do echo tick-$i; i=$((i+1)); sleep 0.2; done",
];

/// A fat, fast printer: ~1KiB unique lines every 50ms, to force frequent segment
/// rolls under a tiny `segment_max` (test 3). The pad is built by doubling so no
/// `seq`/`tr`/`/dev/zero` dependency is assumed of busybox.
const FAT_PRINTER: &[&str] = &[
    "sh",
    "-c",
    "p=x; for k in 1 2 3 4 5 6 7 8 9 10; do p=\"$p$p\"; done; \
     i=0; while true; do echo \"tick-$i $p\"; i=$((i+1)); sleep 0.05; done",
];

/// A printer whose every line is the SAME literal `same-line` (~150ms cadence) —
/// the §8.2 indistinguishable-occurrences case: identical user writes are
/// semantically distinct, never deduplicated, so completeness is a count invariant
/// (test 6), not a set one.
const SAME_PRINTER: &[&str] = &[
    "sh",
    "-c",
    "while true; do echo same-line; sleep 0.15; done",
];

/// Interleaves a unique `out-N` on stdout and `err-N` on stderr each ~200ms, so the
/// store's stream tagging can be checked per stream (test 7).
const INTERLEAVED_PRINTER: &[&str] = &[
    "sh",
    "-c",
    "i=0; while true; do echo out-$i; echo err-$i 1>&2; i=$((i+1)); sleep 0.2; done",
];

// ---- 18. logs + metrics flow end to end (§8.1/§8.2/§8.4) -----------------

/// A running container's logs and metrics reach the filesystem store; a clean
/// stop→reap marks the attempt ended and preserves every daemon-returned line
/// exactly once (uninterrupted collection, §8.2).
#[tokio::test]
async fn telemetry_logs_and_metrics_flow_end_to_end() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let (exec, _tx, _hub, sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_for_chunks(&sink, job, attempt, 5, 60).await?;
        let samples = harness::wait_for_metric(&sink, job, attempt, 60).await?;
        ensure!(
            samples.iter().any(|s| s.memory_used_bytes > 0),
            "expected at least one metric sample with nonzero memory (§12 metrics sanity)"
        );
        // The counting side of the empty-sinks suppression seam: a live sink
        // spawns exactly one collector slot for this running container. (This test
        // proves the seam can count > 0; #26 proves it stays 0 when suppressed.)
        ensure!(
            exec.collector_slots() == 1,
            "a telemetry-wired executor must hold one collector slot per running \
             container, found {}",
            exec.collector_slots()
        );

        // The daemon's retained lines, captured before teardown — the oracle.
        let daemon = harness::daemon_log_lines(&docker, &name).await?;

        // Stop blocks until exit and claims it (so `next_exit` is intentionally
        // suppressed, §4); reap then runs the §8.2/§8.4 drain barrier.
        let outcome = exec.stop(alloc, CoreDuration::from_millis(1)).await?;
        ensure!(
            matches!(
                outcome,
                StopOutcome::Stopped(_) | StopOutcome::AlreadyExited(_)
            ),
            "unexpected stop outcome {outcome:?}"
        );
        exec.reap(alloc).await?;

        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "attempt must be marked ended after reap (§8.4)"
        );

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a daemon-returned line is missing from the store (§8.2)\n daemon={daemon:?}\n stored={stored_lines:?}"
        );
        // Uninterrupted collection appends each delivered chunk exactly once.
        let distinct: std::collections::HashSet<&String> = stored_lines.iter().collect();
        ensure!(
            distinct.len() == stored_lines.len(),
            "uninterrupted collection must not duplicate any line (§8.2): {stored_lines:?}"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 19. at-least-once log replay across a restart (§8.2) ----------------

/// A crash mid-collection followed by adoption over the same root replays only
/// the boundary second (at-least-once): every daemon line survives, duplicates
/// are confined to the single recovery's boundary second (≤2 copies), and the
/// replay counter advances.
#[tokio::test]
async fn log_replay_after_restart_is_at_least_once() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        // Executor A: start and collect, then crash (drop aborts collectors mid-flight).
        let (exec_a, _txa, hub_a, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_a.start(sp.clone()).await?;
        let pre = harness::wait_for_chunks(&sink, job, attempt, 10, 60)
            .await?
            .len();
        drop((exec_a, hub_a, _txa));

        // Executor B over the SAME root adopts the survivor and resumes collection.
        let replay_before = harness::counter_value(harness::REPLAY_COUNTER);
        let (exec_b, _txb, _hub_b, _sink_b) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        // The boundary B will derive == store MAX(at) floored; A is gone, so nothing
        // writes between this read and the follower's own read during observe.
        let boundary = harness::boundary_floor_micros(&sink, job, attempt)
            .await
            .ok_or_else(|| anyhow!("pre-crash store had no log rows to derive a boundary from"))?;
        exec_b.observe().await?; // adopt → resume the follower from `boundary`
        harness::wait_for_chunks(&sink, job, attempt, pre + 10, 60).await?;

        // Capture daemon retention (post-stop is stable), then drain + reap through B.
        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec_b.reap(alloc).await?;

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        // (a) completeness: every daemon line present, in order.
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a daemon line is missing from the store after restart (§8.2)"
        );
        // (b) duplicates confined to the single recovery's boundary second, ≤2 copies.
        harness::assert_bounded_duplicates(&stored, &[boundary], 2)?;
        // (c) the replay counter advanced over the restart.
        if let (Some(before), Some(after)) = (
            replay_before,
            harness::counter_value(harness::REPLAY_COUNTER),
        ) {
            ensure!(
                after > before,
                "replay counter did not advance over the restart (§8.2): {before} -> {after}"
            );
        }
        Ok(())
    }
    .await;

    // A and B are owned (and dropped) inside the block; clean up via a fresh one.
    let (cleaner, _tx, _hub, _sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 20. boundary second split across a segment roll (§8.2) --------------

/// The same restart shape, but a tiny `segment_max` rolls segments mid-stream so
/// the boundary second's chunks straddle a roll. The §8.2 boundary is the
/// *global* MAX(at) across segments, so recovery still confines duplicates to the
/// boundary second and loses nothing — proven here with ≥2 segment files present.
#[tokio::test]
async fn boundary_second_split_across_a_segment_roll() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, FAT_PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");
    // Tiny segments so the fat/fast printer rolls every ~32 lines (~1.6s).
    let tiny = |opts: &mut FilesystemSinkOptions| opts.segment_max = ByteSize::from_kib(32);

    let r: anyhow::Result<()> = async {
        let (exec_a, _txa, hub_a, sink) =
            harness::executor_with_telemetry_opts(docker.clone(), root.path(), tiny).await;
        exec_a.start(sp.clone()).await?;
        // Wait for enough to guarantee at least one roll before the crash.
        let pre = harness::wait_for_chunks(&sink, job, attempt, 60, 60).await?.len();
        drop((exec_a, hub_a, _txa));

        let (exec_b, _txb, _hub_b, _sink_b) =
            harness::executor_with_telemetry_opts(docker.clone(), root.path(), tiny).await;
        let boundary = harness::boundary_floor_micros(&sink, job, attempt)
            .await
            .ok_or_else(|| anyhow!("pre-crash store had no log rows to derive a boundary from"))?;
        exec_b.observe().await?;
        harness::wait_for_chunks(&sink, job, attempt, pre + 60, 60).await?;

        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec_b.reap(alloc).await?;

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a daemon line is missing from the store after a rolling restart (§8.2)"
        );
        harness::assert_bounded_duplicates(&stored, &[boundary], 2)?;
        // The boundary derivation had to span segments: prove there is more than one.
        let segments = harness::attempt_segments(&sink, job, attempt).await;
        ensure!(
            segments >= 2,
            "expected ≥2 segment files (a roll) so the boundary derivation crossed segments (§8.2), got {segments}"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx, _hub, _sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 21. metrics-only open segment resumes from container start (§8.2) ----

/// A container with no output has only metric rows, so the resume boundary is
/// `None` (no log rows) and adoption must fall back to the container start time
/// without wedging. B's collectors run (a new metric lands post-restart),
/// `max_log_timestamp` stays `None`, and a clean reap marks the attempt ended.
#[tokio::test]
async fn metrics_only_open_segment_resumes_from_container_start() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, &["sleep", "300"], Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;

    let r: anyhow::Result<()> = async {
        let (exec_a, _txa, hub_a, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_a.start(sp.clone()).await?;
        harness::wait_for_metric(&sink, job, attempt, 60).await?;
        let pre_metrics = harness::metric_count(&sink, job, attempt).await;
        drop((exec_a, hub_a, _txa));

        // Executor B adopts. `max_log_timestamp` is None → the container-start
        // fallback path; adoption must not wedge.
        let (exec_b, _txb, _hub_b, _sink_b) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_b.observe().await?;

        // A NEW metric sample must land post-restart (collectors resumed).
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
        loop {
            if harness::metric_count(&sink, job, attempt).await > pre_metrics {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "no new metric sample after adoption; collectors did not resume"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }

        // No log rows ever existed for this attempt.
        ensure!(
            sink.max_log_timestamp(&job, &attempt).await?.is_none(),
            "a container with no output must have no log timestamp (§8.2)"
        );

        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        exec_b.reap(alloc).await?;
        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "the metrics-only attempt must be marked ended after a clean reap (§8.4)"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx, _hub, _sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 22. repeated crashes during replay keep every daemon chunk (§8.2) ----

/// Crashing and re-adopting several times still loses no daemon-returned line;
/// any line with more than one copy is bounded by `1 + recoveries` (each recovery
/// permits one boundary-second copy), and the replay counter advances.
#[tokio::test]
async fn repeated_crashes_during_replay_keep_every_daemon_chunk() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    const RECOVERIES: usize = 3;
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        let replay_before = harness::counter_value(harness::REPLAY_COUNTER);
        let mut boundaries: Vec<i64> = Vec::new();

        // Executor A: start and collect, then crash.
        let (exec_a, _txa, hub_a, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_a.start(sp.clone()).await?;
        let mut prev = harness::wait_for_chunks(&sink, job, attempt, 10, 60)
            .await?
            .len();
        drop((exec_a, hub_a, _txa));

        // A number of crash/adopt cycles, each deriving its own boundary second.
        let mut adoptions = 0usize;
        for _ in 0..RECOVERIES {
            let (exec, _tx, hub, _s) =
                harness::executor_with_telemetry(docker.clone(), root.path()).await;
            if let Some(b) = harness::boundary_floor_micros(&sink, job, attempt).await {
                boundaries.push(b);
            }
            exec.observe().await?;
            adoptions += 1;
            harness::wait_for_chunks(&sink, job, attempt, prev + 5, 60).await?;
            prev = harness::stored_chunks(&sink, job, attempt).await.len();
            drop((exec, hub, _tx));
        }

        // Final executor adopts, grows, then stops + drains + reaps.
        let (exec_f, _txf, _hub_f, _sink_f) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        if let Some(b) = harness::boundary_floor_micros(&sink, job, attempt).await {
            boundaries.push(b);
        }
        exec_f.observe().await?;
        adoptions += 1;
        harness::wait_for_chunks(&sink, job, attempt, prev + 5, 60).await?;
        exec_f.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec_f.reap(alloc).await?;

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a daemon line is missing from the store after repeated crashes (§8.2)"
        );
        // Each adoption over stored data permits one boundary-second copy.
        harness::assert_bounded_duplicates(&stored, &boundaries, 1 + adoptions)?;
        if let (Some(before), Some(after)) = (
            replay_before,
            harness::counter_value(harness::REPLAY_COUNTER),
        ) {
            ensure!(
                after > before,
                "replay counter did not advance over the recoveries (§8.2): {before} -> {after}"
            );
        }
        Ok(())
    }
    .await;

    let (cleaner, _tx, _hub, _sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 23. daemon log rotation bounds catch-up (§8.2, §12) ------------------

/// A container created out-of-band with a rotating json-file log driver loses its
/// earliest lines to rotation *before* an executor adopts it. Catch-up from the
/// container-start boundary then captures only what the daemon still retains: the
/// §12 "partial and full daemon-log rotation" case — the pre-adoption gap is
/// simply absent, never an error, and every line the daemon still holds is in the
/// store.
#[tokio::test]
async fn daemon_log_rotation_bounds_catchup() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    // Fresh ids; the out-of-band container is named the coppice way so the
    // executor can adopt and reap it by allocation.
    let alloc = AllocationId::new();
    let attempt = AttemptId::new();
    let job = JobId::new();
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        harness::image_size(&docker, harness::BUSYBOX).await?; // ensure image present

        // 1. Create out-of-band with the full coppice label set and a small
        //    rotating json-file log driver (max-size 8k, max-file 2), running a
        //    fast fat printer so rotation actually cycles.
        let mut labels = HashMap::new();
        labels.insert("coppice.allocation".to_string(), alloc.to_string());
        labels.insert("coppice.attempt".to_string(), attempt.to_string());
        labels.insert("coppice.job".to_string(), job.to_string());
        labels.insert("coppice.node".to_string(), NodeId::new().to_string());
        // Adoption reads ids + start time for telemetry; disk-mode is stamped the
        // way a real container carries it (§5), harmless to the poll enforcer here.
        labels.insert("coppice.disk-mode".to_string(), "quota".to_string());

        let mut log_opts = HashMap::new();
        log_opts.insert("max-size".to_string(), "8k".to_string());
        log_opts.insert("max-file".to_string(), "2".to_string());
        let host_config = bollard::models::HostConfig {
            log_config: Some(bollard::models::HostConfigLogConfig {
                typ: Some("json-file".to_string()),
                config: Some(log_opts),
            }),
            ..Default::default()
        };
        let body = bollard::models::ContainerCreateBody {
            image: Some(harness::BUSYBOX.to_string()),
            cmd: Some(
                FAT_PRINTER
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            ),
            labels: Some(labels),
            host_config: Some(host_config),
            ..Default::default()
        };
        docker
            .create_container(
                Some(
                    bollard::query_parameters::CreateContainerOptionsBuilder::new()
                        .name(&name)
                        .build(),
                ),
                body,
            )
            .await
            .map_err(|e| anyhow!("create out-of-band container: {e}"))?;
        docker
            .start_container(
                &name,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await
            .map_err(|e| anyhow!("start out-of-band container: {e}"))?;

        // 2. Wait until the first line the container printed has rotated away — the
        //    "partial history" (in fact a full-rotation gap relative to the very
        //    start) that catch-up can never recover.
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
        loop {
            let lines = harness::daemon_log_lines(&docker, &name).await?;
            if !lines.iter().any(|l| l.starts_with("tick-0 ")) && !lines.is_empty() {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the daemon never rotated the first line away"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }

        // 3. Adopt with a telemetry executor over an EMPTY root: catch-up/follow
        //    from container start, capturing only what the daemon still retains.
        let (exec, _tx, _hub, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec.observe().await?;
        harness::wait_for_chunks(&sink, job, attempt, 20, 60).await?;

        // 4. Stop out-of-band, let the executor see the exit, reap.
        docker
            .stop_container(
                &name,
                Some(
                    bollard::query_parameters::StopContainerOptionsBuilder::new()
                        .t(1)
                        .build(),
                ),
            )
            .await
            .map_err(|e| anyhow!("stop out-of-band container: {e}"))?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec.reap(alloc).await?;

        // Assert: every line the daemon STILL retains is in the store; the earlier,
        // rotated-away lines are simply absent (a gap, never an error, §8.2/§12).
        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            !stored_lines.is_empty(),
            "catch-up collected nothing from a live, rotating container"
        );
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a still-retained daemon line is missing from the store (§8.2)\n daemon(last 5)={:?}",
            &daemon[daemon.len().saturating_sub(5)..]
        );
        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "the adopted-then-reaped attempt must be marked ended (§8.4)"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx, _hub, _sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 24. drain completes before reap removes the container (§8.2, §12) ----

/// The healthy drain-before-reap ordering: after a stop, reap rides out the 2s
/// `REAP_DRAIN_WAIT` for the follower's EOF drain, so the very last line the
/// daemon returned is preserved in the store and the attempt is marked ended.
/// (The forced-drain retry path against a healthy daemon is unreachable here and
/// is covered by the Part-1 `drain_verdict` unit matrix.)
#[tokio::test]
async fn drain_completes_before_reap_removes_the_container() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let (exec, _tx, _hub, sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_for_chunks(&sink, job, attempt, 5, 60).await?;

        // Stop (blocks until exit), capture the daemon's tail, then reap — which
        // must wait out the follower's EOF drain before removing the container.
        exec.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        let tail = daemon
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("the daemon returned no lines to tail"))?;
        exec.reap(alloc).await?; // Ok proves the drain rode out within REAP_DRAIN_WAIT

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            stored_lines.contains(&tail),
            "the drain-before-reap barrier lost the tail line {tail:?} (§8.2)"
        );
        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "the attempt must be marked ended after the drain barrier (§8.4)"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ========================================================================
// S6b review-gap closers: the production `telemetry::build` wiring path, the
// empty-sinks suppression seam, the §8.2 catch-up-on-reap and marker-failure
// retryable paths, image-bytes label recovery, indistinguishable-payload replay
// bounds, and stdout/stderr stream tagging. Same gate + cleanup + fresh-id
// idioms as the suite above.
// ========================================================================

// ---- 25. the production build path wires collection end to end (§8.2/§8.3) --

/// The PRODUCTION assembly — `telemetry::build` over a serde-parsed
/// [`TelemetryConfig`], wired exactly as `run_daemon` does — collects logs and
/// metrics correctly *and* survives an agent restart without mass-replay. The
/// config lists a **metrics-only** sink FIRST (dir A) and a **logs+metrics** sink
/// SECOND (dir B); `build` must therefore pick sink B as the §8.2 `log_store`
/// (the first LOG-consuming sink), never `stores[0]`. This is the exact shape of
/// the metrics-only-first resume bug: had the boundary been derived from the
/// metrics-only `stores[0]` (whose log `MAX(at)` never advances), adoption would
/// replay the container's whole retained history into sink B and blow the ≤2
/// bounded-duplication contract wide open.
#[tokio::test]
async fn production_build_wires_collection_end_to_end() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let dir_a = root.path().join("metrics-only");
    let dir_b = root.path().join("logs-and-metrics");
    let data_dir = root.path().join("data");

    // TOML → serde → `TelemetryConfig`, the operator-facing surface. Single-quoted
    // TOML literal strings take the dir paths verbatim (no escape processing). A
    // metrics-only sink first, a logs+metrics sink second: the ordering the resume
    // regression needs.
    let cfg_toml = format!(
        "metrics_interval = \"1s\"\n\
         drain_force_after = \"10m\"\n\
         \n\
         [[sinks]]\n\
         type = \"filesystem\"\n\
         kinds = [\"metrics\"]\n\
         dir = '{a}'\n\
         \n\
         [[sinks]]\n\
         type = \"filesystem\"\n\
         kinds = [\"metrics\", \"logs\"]\n\
         dir = '{b}'\n",
        a = dir_a.display(),
        b = dir_b.display(),
    );
    let config: TelemetryConfig = toml::from_str(&cfg_toml).expect("telemetry config parses");

    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        // Build the subsystem the production way and wire it like `run_daemon`.
        let (ptx_a, prx_a) = watch::channel(DiskPressure::Ok);
        let telemetry_a = coppice_agent::telemetry::build(&config, &data_dir, vec![], 85, prx_a)
            .await
            .map_err(|e| anyhow!("telemetry build A: {e}"))?;
        ensure!(telemetry_a.stores.len() == 2, "two sinks configured");
        let store_a = telemetry_a.stores[0].clone(); // metrics-only
        let store_b = telemetry_a.stores[1].clone(); // logs + metrics (the log_store)
        let (exec_a, _txa) = harness::executor_from_wiring(
            docker.clone(),
            harness::wiring_of(&telemetry_a, &config),
        )
        .await;

        exec_a.start(sp.clone()).await?;
        let pre = harness::wait_for_chunks(&store_b, job, attempt, 10, 60)
            .await?
            .len();
        harness::wait_for_metric(&store_b, job, attempt, 60).await?;
        harness::wait_for_metric(&store_a, job, attempt, 60).await?;

        // Fan-out (checked while the container runs): logs land ONLY under dir B,
        // metrics land under BOTH sinks.
        ensure!(
            harness::stored_chunks(&store_a, job, attempt)
                .await
                .is_empty(),
            "the metrics-only sink A must hold no log chunks (§8.3 routing)"
        );
        ensure!(
            harness::metric_count(&store_a, job, attempt).await > 0,
            "sink A (metrics) must hold metric samples"
        );
        ensure!(
            harness::metric_count(&store_b, job, attempt).await > 0,
            "sink B (metrics + logs) must hold metric samples"
        );

        // Crash: drop the executor and the subsystem (all hub handles + janitors).
        drop((exec_a, telemetry_a, ptx_a));

        // Rebuild over the SAME config/dirs and adopt. The boundary is read after A
        // has stopped writing and before B observes — the point B itself derives it.
        let (ptx_b, prx_b) = watch::channel(DiskPressure::Ok);
        let telemetry_b = coppice_agent::telemetry::build(&config, &data_dir, vec![], 85, prx_b)
            .await
            .map_err(|e| anyhow!("telemetry build B: {e}"))?;
        let (exec_b, _txb) = harness::executor_from_wiring(
            docker.clone(),
            harness::wiring_of(&telemetry_b, &config),
        )
        .await;
        let boundary = harness::boundary_floor_micros(&store_b, job, attempt)
            .await
            .ok_or_else(|| anyhow!("sink B had no log rows to derive a boundary from"))?;
        exec_b.observe().await?; // adopt → resume the follower from `boundary`
        harness::wait_for_chunks(&store_b, job, attempt, pre + 10, 60).await?;

        // Stop, capture the daemon's retained lines, then reap through B.
        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec_b.reap(alloc).await?;

        // The ended marker persisted on BOTH stores (§8.4).
        ensure!(
            harness::attempt_ended(&store_a, job, attempt).await,
            "sink A must be marked ended after reap (§8.4)"
        );
        ensure!(
            harness::attempt_ended(&store_b, job, attempt).await,
            "sink B must be marked ended after reap (§8.4)"
        );

        // Completeness + bounded duplication under the single recovery (§8.2). The
        // metrics-only-first resume bug would have driven duplicates far past ≤2.
        let stored = harness::stored_chunks(&store_b, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "a daemon line is missing from sink B after adoption (§8.2)"
        );
        harness::assert_bounded_duplicates(&stored, &[boundary], 2)?;
        ensure!(
            harness::stored_chunks(&store_a, job, attempt)
                .await
                .is_empty(),
            "sink A must never receive log chunks across the whole cycle"
        );
        // exec_b / telemetry_b live until here (kept for the whole cycle).
        drop((exec_b, telemetry_b, ptx_b));
        Ok(())
    }
    .await;

    let (cleaner, _tx) = harness::executor(docker.clone()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 26. an empty-sinks config collects nothing (§8.3 suppression) ----------

/// A [`TelemetryConfig`] with `sinks: vec![]` yields a hub consuming neither
/// stream. Passed as `Some(TelemetryWiring)` (not the `run_daemon` `None`
/// shortcut), it exercises the *per-kind* suppression seam inside
/// `spawn_collectors`: with no consumer for either stream, no collector is
/// reserved or spawned, nothing is written, and stop→reap still succeeds because
/// the drain barrier degrades to a no-op (no follower to await, an empty `stores`
/// list to mark, a hub with no queues to flush).
///
/// The `collector_slots() == 0` assertion (taken while the container is Running)
/// is what proves *no collector spawned* — absence is otherwise hard to witness,
/// because a wasteful collector feeding an empty hub would also write no files.
/// The no-files assertion is kept as a second, independent witness.
#[tokio::test]
async fn empty_sinks_config_spawns_no_collectors() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry data dir");
    let config = TelemetryConfig {
        sinks: vec![],
        ..Default::default()
    };
    let (ptx, prx) = watch::channel(DiskPressure::Ok);
    let telemetry = coppice_agent::telemetry::build(&config, root.path(), vec![], 85, prx)
        .await
        .expect("empty telemetry config builds");
    assert!(telemetry.stores.is_empty(), "no sinks ⇒ no stores");
    let wiring = harness::wiring_of(&telemetry, &config);
    assert!(
        !wiring.hub.consumes(SinkKind::Metrics),
        "no sink consumes metrics"
    );
    assert!(
        !wiring.hub.consumes(SinkKind::Logs),
        "no sink consumes logs"
    );

    let (exec, _tx) = harness::executor_from_wiring(docker.clone(), wiring).await;
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        // Prove no collector was spawned *positively*, while the container is
        // demonstrably RUNNING: the per-kind suppression seam leaves zero slots
        // (neither Reserved nor Active). This is the assertion the pre-fix
        // wasteful-collector implementation would fail — it would hold a slot per
        // container even with an empty hub, yet still write nothing.
        harness::wait_observed_running(&exec, alloc, 30).await?;
        ensure!(
            exec.collector_slots() == 0,
            "empty-sinks config must spawn no collector, found {} slot(s)",
            exec.collector_slots()
        );

        // Give any collector that would have spawned time to write something.
        tokio::time::sleep(StdDuration::from_secs(3)).await;
        ensure!(
            exec.collector_slots() == 0,
            "a collector slot appeared after the container settled: {}",
            exec.collector_slots()
        );

        // Nothing collected: `build` opened no sink roots and no default
        // <data_dir>/telemetry, so the data dir has no job directories.
        let entries: Vec<std::path::PathBuf> = std::fs::read_dir(root.path())
            .map(|rd| rd.filter_map(Result::ok).map(|e| e.path()).collect())
            .unwrap_or_default();
        ensure!(
            entries.is_empty(),
            "an empty-sinks config must write no telemetry, found: {entries:?}"
        );

        // Stop + reap still succeed (drain barrier no-op).
        exec.stop(alloc, CoreDuration::from_millis(1)).await?;
        exec.reap(alloc).await?;
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    drop((telemetry, ptx));
    r.unwrap();
}

// ---- 27. reap catch-up-drains an already-dead container (§8.2) --------------

/// A container started under a NON-telemetry executor, left dead (never reaped),
/// is adopted by a telemetry executor whose `observe()` reports it but spawns NO
/// collector (collectors are queued only for *running* containers). `reap` must
/// then take the §8.2 absent-entry **catch-up drain** path — the one no other
/// gated test reaches — capturing every line the daemon still retains and marking
/// the attempt ended.
#[tokio::test]
async fn already_dead_container_gets_catch_up_drain_on_reap() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let sp = harness::spec(
        harness::BUSYBOX,
        &["sh", "-c", "for i in 1 2 3 4 5; do echo line-$i; done"],
        Resources::ZERO,
    );
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");
    let root = tempfile::TempDir::new().expect("telemetry tempdir");

    let r: anyhow::Result<()> = async {
        // Executor A (no telemetry) starts the short-lived container, waits for its
        // exit, and drops WITHOUT reaping — the container stays dead daemon-side.
        let (exec_a, _txa) = harness::executor(docker.clone()).await;
        exec_a.start(sp.clone()).await?;
        harness::wait_exit(&exec_a, alloc, 30).await?;
        drop(exec_a);

        // Executor B (telemetry, fresh empty root) adopts. observe() reports the
        // dead container but creates no collector for it.
        let (exec_b, _txb, _hub_b, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_b.observe().await?;

        // The daemon's retained lines, captured BEFORE reaping (the oracle).
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        ensure!(!daemon.is_empty(), "the dead container returned no lines");

        // reap runs the absent-entry catch-up drain, then finalises the attempt.
        exec_b.reap(alloc).await?;

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_lines = harness::chunk_lines(&stored);
        ensure!(
            harness::is_subsequence(&daemon, &stored_lines),
            "catch-up drain must store every daemon-returned line (§8.2)\n daemon={daemon:?}\n stored={stored_lines:?}"
        );
        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "the catch-up-drained attempt must be marked ended (§8.4)"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx) = harness::executor(docker.clone()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 28. a marker-write failure keeps reap retryable (§8.4) -----------------

/// When the attempt directory is read-only the ended marker (temp-file + rename,
/// `fs_sink::write_marker`) cannot persist, so `attempt_ended` returns `Err` and
/// `reap` surfaces it retryably — leaving the container intact for the janitor's
/// retry. Restoring permissions and reaping again persists the marker and removes
/// the container. Unix-only (relies on `chmod`); if run as root (chmod
/// ineffective — not this Mac) the first reap would succeed, so the test
/// eprintln-skips rather than false-failing.
#[cfg(unix)]
#[tokio::test]
async fn marker_write_failure_keeps_reap_retryable() {
    use std::os::unix::fs::PermissionsExt;

    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let (exec, _tx, _hub, sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    let sp = harness::spec(harness::BUSYBOX, PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");
    let attempt_dir = root.path().join(job.to_string()).join(attempt.to_string());

    /// Restores the attempt dir to writable on drop, so a panicking assertion can
    /// never leave the tempdir undeletable.
    struct RestorePerms(std::path::PathBuf);
    impl Drop for RestorePerms {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_for_chunks(&sink, job, attempt, 5, 60).await?;

        // Stop so the follower drains and the only remaining reap step is the marker.
        exec.stop(alloc, CoreDuration::from_millis(1)).await?;

        // Make the attempt dir read-only (0o555 forbids creating the marker's temp
        // file). Guarded so any later panic still restores it.
        let _restore = RestorePerms(attempt_dir.clone());
        std::fs::set_permissions(&attempt_dir, std::fs::Permissions::from_mode(0o555))
            .map_err(|e| anyhow!("chmod 0o555 {}: {e}", attempt_dir.display()))?;

        // reap must fail (the marker cannot persist) and leave the container intact.
        let first = exec.reap(alloc).await;
        if first.is_ok() {
            eprintln!(
                "skipping: reap unexpectedly succeeded under a read-only attempt dir \
                 (running as root?)"
            );
            return Ok(());
        }
        ensure!(
            harness::container_exists(&docker, &name).await,
            "a failed reap must leave the container intact for the retry (§8.4)"
        );
        ensure!(
            !harness::attempt_ended(&sink, job, attempt).await,
            "the ended marker must not have persisted through the failed reap"
        );

        // Restore permissions and retry: the marker persists, the container is gone.
        std::fs::set_permissions(&attempt_dir, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| anyhow!("restore 0o755 {}: {e}", attempt_dir.display()))?;
        exec.reap(alloc).await?;
        ensure!(
            harness::attempt_ended(&sink, job, attempt).await,
            "the ended marker must persist after permissions are restored (§8.4)"
        );
        ensure!(
            !harness::container_exists(&docker, &name).await,
            "the retried reap must remove the container"
        );
        Ok(())
    }
    .await;

    // Ensure writable for teardown regardless of where we bailed.
    let _ = std::fs::set_permissions(&attempt_dir, std::fs::Permissions::from_mode(0o755));
    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 29. the image-bytes label flows into metric samples and survives adoption -

/// The resolved image's on-disk size rides the `coppice.image-bytes` label (§5)
/// and becomes each metric sample's constant `disk_image_bytes` (§8.1). After an
/// agent restart the sampler must recover it from the label (not lose it to 0), so
/// post-adoption samples still carry the same nonzero value. Finally the sampler
/// must stop at exit: after stop→reap the sample count does not grow.
#[tokio::test]
async fn image_bytes_label_flows_into_metric_samples() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, &["sleep", "300"], Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;

    let r: anyhow::Result<()> = async {
        let (exec_a, _txa, hub_a, sink_a) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_a.start(sp.clone()).await?;
        harness::wait_for_metric(&sink_a, job, attempt, 60).await?;

        let label_bytes = harness::image_bytes_label(&docker, alloc).await?;
        ensure!(label_bytes > 0, "the image-bytes label must be nonzero");
        let samples = harness::all_metric_samples(&sink_a, job, attempt).await;
        ensure!(
            samples.iter().all(|s| s.disk_image_bytes == label_bytes),
            "every metric sample must carry disk_image_bytes == the label ({label_bytes})"
        );
        let pre = harness::metric_count(&sink_a, job, attempt).await;
        drop((exec_a, hub_a, _txa));

        // Rebuild over the same root and adopt: the sampler recovers the image size
        // from the label, so post-adoption samples still carry it.
        let (exec_b, _txb, _hub_b, sink_b) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_b.observe().await?;
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
        loop {
            if harness::metric_count(&sink_b, job, attempt).await > pre {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "no new metric sample after adoption; the sampler did not resume"
            );
            tokio::time::sleep(StdDuration::from_millis(250)).await;
        }
        let post = harness::all_metric_samples(&sink_b, job, attempt).await;
        ensure!(
            post.iter().all(|s| s.disk_image_bytes == label_bytes),
            "post-adoption samples must still carry disk_image_bytes == the label \
             ({label_bytes}) — the label was recovered on adoption"
        );

        // Sampler shutdown after exit: no new samples once stopped and reaped.
        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        exec_b.reap(alloc).await?;
        let count = harness::metric_count(&sink_b, job, attempt).await;
        // 1s metrics interval; wait 2× to catch a sampler that failed to abort.
        tokio::time::sleep(StdDuration::from_millis(2500)).await;
        let count2 = harness::metric_count(&sink_b, job, attempt).await;
        ensure!(
            count2 == count,
            "the sampler kept emitting after exit/reap: {count} -> {count2}"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx) = harness::executor(docker.clone()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 30. identical payloads survive a restart within count bounds (§8.2) ----

/// The §8.2 indistinguishable-occurrences case: a container printing the SAME
/// literal `same-line` forever. Identical user writes are never deduplicated, so
/// completeness is a COUNT invariant, and the single recovery may double-store
/// only the pre-crash boundary-second chunks.
#[tokio::test]
async fn identical_payloads_survive_restart_within_bounds() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let sp = harness::spec(harness::BUSYBOX, SAME_PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;
    let name = format!("coppice-{alloc}");

    let r: anyhow::Result<()> = async {
        let (exec_a, _txa, hub_a, sink) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        exec_a.start(sp.clone()).await?;
        harness::wait_for_chunks(&sink, job, attempt, 10, 60).await?;
        drop((exec_a, hub_a, _txa));

        // Adopt over the same root. Snapshot the pre-adoption count, the resume
        // boundary (floored MAX(at) — exactly what B derives), and the number of
        // stored chunks in the boundary second — all read after A stopped writing
        // and before B observes.
        let (exec_b, _txb, _hub_b, _sink_b) =
            harness::executor_with_telemetry(docker.clone(), root.path()).await;
        let pre_chunks = harness::stored_chunks(&sink, job, attempt).await;
        let stored_count_pre = pre_chunks.len();
        let boundary = harness::boundary_floor_micros(&sink, job, attempt)
            .await
            .ok_or_else(|| anyhow!("pre-crash store had no log rows"))?;
        let n_boundary = harness::chunks_in_boundary_second(&pre_chunks, boundary);
        exec_b.observe().await?;
        harness::wait_for_chunks(&sink, job, attempt, stored_count_pre + 10, 60).await?;

        exec_b.stop(alloc, CoreDuration::from_millis(1)).await?;
        let daemon = harness::daemon_log_lines(&docker, &name).await?;
        exec_b.reap(alloc).await?;

        let stored = harness::stored_chunks(&sink, job, attempt).await;
        let stored_total = harness::chunk_lines(&stored).len();
        let daemon_total = daemon.len();
        // Arithmetic (no rotation here — short runtime — so the daemon retains every
        // line it emitted): every daemon-retained line is stored at least once (no
        // dedup) ⇒ stored_total >= daemon_total; only the pre-crash boundary-second
        // chunks can be double-stored by the single recovery's replay ⇒
        // stored_total <= daemon_total + n_boundary.
        ensure!(
            stored_total >= daemon_total,
            "completeness by count failed: stored {stored_total} < daemon {daemon_total}"
        );
        ensure!(
            stored_total <= daemon_total + n_boundary,
            "duplication past the boundary-second bound: stored {stored_total} > \
             daemon {daemon_total} + n_boundary {n_boundary}"
        );
        Ok(())
    }
    .await;

    let (cleaner, _tx) = harness::executor(docker.clone()).await;
    harness::cleanup(&cleaner, &[alloc]).await;
    r.unwrap();
}

// ---- 31. stdout and stderr are tagged and per-stream order is preserved (§8.2) -

/// A container interleaving unique `out-N` on stdout and `err-N` on stderr: the
/// store must tag each chunk with its stream, so a stream-filtered read returns
/// only that stream's lines, an unfiltered read returns both, and each stream's
/// relative order is preserved (the numeric suffixes ascend).
#[tokio::test]
async fn interleaved_stdout_stderr_streams_are_tagged() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let (exec, _tx, _hub, sink) =
        harness::executor_with_telemetry(docker.clone(), root.path()).await;
    let sp = harness::spec(harness::BUSYBOX, INTERLEAVED_PRINTER, Resources::ZERO);
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        harness::wait_for_chunks(&sink, job, attempt, 10, 60).await?;
        exec.stop(alloc, CoreDuration::from_millis(1)).await?;
        exec.reap(alloc).await?;

        let stdout = harness::chunk_lines(
            &harness::stored_chunks_of(&sink, job, attempt, Some(LogStream::Stdout)).await,
        );
        let stderr = harness::chunk_lines(
            &harness::stored_chunks_of(&sink, job, attempt, Some(LogStream::Stderr)).await,
        );
        let all = harness::chunk_lines(&harness::stored_chunks_of(&sink, job, attempt, None).await);

        ensure!(
            !stdout.is_empty() && !stderr.is_empty(),
            "both streams must have lines"
        );
        ensure!(
            stdout.iter().all(|l| l.starts_with("out-")),
            "the stdout stream leaked a non-out line: {stdout:?}"
        );
        ensure!(
            stderr.iter().all(|l| l.starts_with("err-")),
            "the stderr stream leaked a non-err line: {stderr:?}"
        );
        ensure!(
            all.iter().any(|l| l.starts_with("out-")) && all.iter().any(|l| l.starts_with("err-")),
            "the unfiltered read must contain both streams"
        );
        ensure!(
            is_ascending_suffix(&stdout, "out-"),
            "stdout relative order not preserved: {stdout:?}"
        );
        ensure!(
            is_ascending_suffix(&stderr, "err-"),
            "stderr relative order not preserved: {stderr:?}"
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

// ---- 32. poll-mode disk_writable_bytes reaches stored metric samples (§8.1/§6.2) -

/// Under the poll disk strategy the enforcer's `GET /system/df` sweep records
/// each container's writable-layer `SizeRw` into the shared `DiskReadings`, and
/// the metrics sampler reads that as `disk_writable_bytes`. This exercises the
/// whole chain end to end — df sweep → DiskReadings → sampler → filesystem store.
/// A container writes ~2 MiB into its writable layer then idles; a stored metric
/// sample must eventually carry `disk_writable_bytes > 0`. `disk_image_bytes > 0`
/// rides along on the same sample (the constant per-attempt image size the
/// sampler stamps, §8.1). The disk request is a large 1 GiB — far above the
/// ~2 MiB written — so this never trips the poll disk kill; only the *reading*
/// path is under test.
#[tokio::test]
async fn poll_mode_disk_reading_reaches_metric_samples() {
    let Some(docker) = harness::docker().await else {
        return;
    };
    let root = tempfile::TempDir::new().expect("telemetry tempdir");
    let config = ExecutorConfig {
        whole_core_affinity: false,
        disk_enforcement: DiskEnforcement::Poll,
        disk_poll_interval: StdDuration::from_secs(2),
        ..Default::default()
    };
    let (exec, _tx, _hub, sink) =
        harness::executor_with_telemetry_config(docker.clone(), root.path(), config).await;
    // 1 GiB disk request: the enforced writable budget (disk − image) is ~1 GiB,
    // so the ~2 MiB fill never trips the poll kill — only the reading path runs.
    let limits = Resources {
        cpu_millis: 0,
        memory: ByteSize::ZERO,
        disk: ByteSize::from_mib(1024),
    };
    let sp = harness::spec(
        harness::BUSYBOX,
        &[
            "sh",
            "-c",
            // /tmp is world-writable (1777) in busybox and, with no tmpfs mount in
            // our HostConfig, lands in the overlay writable layer df's SizeRw counts;
            // the container runs as a non-root uid and cannot write at `/`.
            "dd if=/dev/zero of=/tmp/fill bs=1024 count=2048 2>/dev/null; sleep 300",
        ],
        limits,
    );
    let alloc = sp.allocation;
    let job = sp.job;
    let attempt = sp.attempt;

    let r: anyhow::Result<()> = async {
        exec.start(sp).await?;
        // Poll until a stored sample shows a nonzero writable-layer reading. The
        // deadline is generous: recording SizeRw needs a df sweep (2s floor) AFTER
        // the fill lands, then a sampler tick to store it, and Colima's df
        // accounting can lag. Do NOT weaken this to `>= 0` — a zero would mean the
        // df → DiskReadings → sampler wiring is broken (e.g. a wrong disk-mode
        // label), which is exactly what this test exists to catch.
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);
        let sample = loop {
            let samples = harness::all_metric_samples(&sink, job, attempt).await;
            if let Some(sample) = samples.iter().find(|s| s.disk_writable_bytes > 0) {
                break sample.clone();
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "no metric sample reported disk_writable_bytes > 0 within 60s \
                 (df sweep → DiskReadings → sampler wiring); {} sample(s) seen so far",
                samples.len()
            );
            tokio::time::sleep(StdDuration::from_millis(500)).await;
        };
        ensure!(
            sample.disk_image_bytes > 0,
            "the same sample must carry the constant image size as disk_image_bytes, got {}",
            sample.disk_image_bytes
        );
        Ok(())
    }
    .await;

    harness::cleanup(&exec, &[alloc]).await;
    r.unwrap();
}

/// Whether every line's `prefix`-stripped integer suffix strictly ascends — the
/// per-stream ordering witness for the interleaved-streams test.
fn is_ascending_suffix(lines: &[String], prefix: &str) -> bool {
    let mut last: Option<u64> = None;
    for line in lines {
        let Some(n) = line
            .strip_prefix(prefix)
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return false;
        };
        if let Some(prev) = last {
            if n <= prev {
                return false;
            }
        }
        last = Some(n);
    }
    true
}
