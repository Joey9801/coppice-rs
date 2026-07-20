//! The container lifecycle: the start phase machine, stop's daemon-arbitrated
//! race discrimination, observe's reconciliation snapshot, and reap
//! (docker-executor.md §3–§5). The pure decisions (adopt-on-conflict,
//! grace→seconds) are factored out and unit-tested; the I/O around them talks
//! to the daemon through bollard.
//!
//! Classification proper stays *above* the trait (ADR 0013): these functions
//! only produce evidence (`StopOutcome`, `ObservedContainer`) and start-error
//! shapes. They never touch the journal or session state.

use std::collections::HashMap;

use bollard::models::{ContainerCreateBody, ContainerStateStatusEnum, ContainerUpdateBody};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, ListContainersOptionsBuilder, RemoveContainerOptions,
    RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
};

use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};

use super::state::Mapped;
use super::{
    api, cache, classify, container_name, cpuset, disk, limits, lock_state, logs, state,
    CollectorSlot, ContainerIds, Inner, TelemetryWiring, AGENT_LOG_DRAIN_FORCED_TOTAL,
    LABEL_ALLOCATION, LABEL_ATTEMPT, LABEL_CPU_EXCLUSIVE, LABEL_DISK_BUDGET, LABEL_DISK_MODE,
    LABEL_IMAGE_BYTES, LABEL_IMAGE_DIGEST, LABEL_JOB, LABEL_NODE,
};
use crate::executor::{
    ContainerState, ExecutorError, ObservedContainer, StartError, StartSpec, StopOutcome,
};
use crate::pressure::DiskPressure;
use crate::telemetry::FilesystemSink;

// ---- start (docker-executor.md §3 phase machine, §4 error mapping) ------

/// Ensures the allocation leaves the `starting` set on *every* exit path from
/// [`start`] — success, error, or panic. `observe` treats a `created` container
/// whose allocation is in `starting` as our in-flight create and must not
/// remove it, so the membership has to be exact.
struct StartingGuard<'a> {
    inner: &'a Inner,
    allocation: AllocationId,
}

impl Drop for StartingGuard<'_> {
    fn drop(&mut self) {
        lock_state(&self.inner.state)
            .starting
            .remove(&self.allocation);
    }
}

/// Releases the image pin taken right after `fetch` on every failure path out of
/// [`start_inner`] (docker-executor.md §7). The pin must be established before
/// the container is created so an assigned-but-not-yet-running allocation still
/// pins its image (ADR 0010), but a create/adopt/start failure has no container
/// to re-pin from — so unless the start commits, the pin has to be handed back
/// here. On the success paths (`AlreadyStarted`, and a clean start) the running
/// container owns the pin until reap, so the guard is [`commit`](PinGuard::commit)ted
/// and does nothing on drop.
struct PinGuard<'a> {
    inner: &'a Inner,
    allocation: AllocationId,
    committed: bool,
}

impl PinGuard<'_> {
    /// Keep the pin: a container now owns it and `reap` will release it.
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.inner.cache.release(self.allocation);
        }
    }
}

/// Start a container for `spec` (docker-executor.md §3: Resolving → Pulling →
/// Creating → Starting). Idempotent per allocation (§5): a duplicate delivery
/// racing an in-flight start returns `Ok(())`, and the deterministic name plus
/// adopt-on-conflict is the real backstop. `max_runtime` is the session
/// watchdog's concern and is ignored here.
pub(crate) async fn start(inner: &Inner, spec: StartSpec) -> Result<(), StartError> {
    // 1. Pressure gate (§9): refuse new starts under Critical host disk pressure
    //    rather than wedging the node.
    if *inner.pressure.borrow() == DiskPressure::Critical {
        return Err(StartError::Start {
            user_error: false,
            message: "host disk pressure critical; refusing new starts".to_string(),
        });
    }

    // 2. Idempotency short-circuit + in-flight registration. A duplicate racing
    //    the in-flight start returns Ok best-effort; the guard guarantees
    //    removal on every path below.
    {
        let mut st = lock_state(&inner.state);
        if st.starting.contains(&spec.allocation) {
            return Ok(());
        }
        st.starting.insert(spec.allocation);
    }
    let _guard = StartingGuard {
        inner,
        allocation: spec.allocation,
    };

    start_inner(inner, &spec).await
}

async fn start_inner(inner: &Inner, spec: &StartSpec) -> Result<(), StartError> {
    // 3. Resolve the image through the cache manager (§7): local wins, 404 →
    //    per-reference-singleflight pull under the global concurrency limit,
    //    then re-inspect. The digest fallback (repo-digest else id) lives in
    //    cache::digest_of, so label and cache key can never diverge. Passing
    //    the allocation makes `fetch` place the pin itself, under the digest's
    //    image lock — the only ordering that excludes a concurrent eviction
    //    (which cannot rely on a daemon 409: no container exists yet).
    let cache::FetchedImage { inspect, digest } = inner
        .cache
        .fetch(&spec.image, Some(spec.allocation))
        .await?;
    // The pin is placed; every failure path below must release it (the guard's
    // Drop). A running/exited container re-pins itself from its label; a failed
    // start must not leak a pin that no container will ever release (§7).
    let _pin = PinGuard {
        inner,
        allocation: spec.allocation,
        committed: false,
    };
    // Pin the exact bytes we resolved: create with the image `id`, not the
    // movable tag (tag-drift re-resolution is future work, §7).
    let image_id = inspect.id.clone().unwrap_or_default();
    let image_digest = digest;

    // 4. User (§6): honor a non-root image `USER`, else the config default;
    //    reject UID 0 as a user error.
    let image_user = inspect.config.as_ref().and_then(|cfg| cfg.user.as_deref());
    let user = limits::resolve_user(image_user, inner.default_uid)?;

    // Disk plan (§6.2): decide the create-time storage-opt/labels behind the
    // DiskEnforcer seam, using the already-resolved image size. An image that
    // alone exceeds the job's disk request fails here as a user error, before
    // the container (or any CPU grant) is created.
    // Docker reports the image size as a signed integer; this is where it
    // becomes a typed size. A negative reading is nonsense rather than a real
    // image, so it clamps to zero and the plan treats it as "no image cost".
    let image_size = ByteSize::from_bytes(inspect.size.map(|size| size.max(0) as u64).unwrap_or(0));
    let disk_plan = inner.disk.plan(&spec.limits, image_size)?;

    // Serialize the affinity-plan/create boundary. Without this, a concurrent
    // exclusive grant could try to `docker update` a fractional allocation
    // after it entered the allocator but before its container existed.
    let _cpu_start = inner.cpu_start.lock().await;
    let cpu = prepare_cpu(inner, spec).await?;

    // 5. Create, with adopt-on-name-conflict (§5).
    let name = container_name(spec.allocation);
    let body = build_create_body(
        inner,
        spec,
        &image_id,
        &image_digest,
        image_size.as_u64(),
        &user,
        cpu.as_ref().map(|allocation| &allocation.affinity),
        &disk_plan,
    );
    match inner
        .docker
        .create_container(
            Some(CreateContainerOptionsBuilder::new().name(&name).build()),
            body,
        )
        .await
    {
        Ok(_) => {}
        Err(err) if api::status_code(&err) == Some(409) => {
            match adopt(inner, &name, spec).await {
                Ok(AdoptOutcome::StartExisting) => {
                    // The survivor was created by the previous agent process
                    // with that process's then-current cpuset. Refresh it to
                    // the newly allocated grant/shared pool before starting,
                    // otherwise the runtime and allocator can silently disagree.
                    if let Some(cpu) = cpu.as_ref() {
                        if let Err(err) = update_created_affinity(inner, &name, &cpu.affinity).await
                        {
                            rollback_cpu(inner, spec.allocation, Some(cpu)).await;
                            return Err(err);
                        }
                    }
                }
                Ok(AdoptOutcome::AlreadyStarted(inspect)) => {
                    // The container already started; its pin stands until reap.
                    _pin.commit();
                    // Release the plan/create serialization before the async
                    // boundary derivation (which reads the store).
                    drop(_cpu_start);
                    // Adoption: resume telemetry collection (§8.2) from the
                    // surviving container's labels and start time.
                    let ids = ContainerIds {
                        allocation: spec.allocation,
                        attempt: spec.attempt,
                        job: spec.job,
                    };
                    let labels = inspect.config.as_ref().and_then(|cfg| cfg.labels.as_ref());
                    let image_bytes = super::image_bytes_from_labels(labels);
                    let started_at = inspect
                        .state
                        .as_ref()
                        .and_then(|st| st.started_at.as_deref())
                        .and_then(classify::parse_docker_time);
                    super::spawn_collectors(inner, ids, &name, true, image_bytes, started_at).await;
                    return Ok(());
                }
                Err(err) => {
                    rollback_cpu(inner, spec.allocation, cpu.as_ref()).await;
                    return Err(err);
                }
            }
        }
        Err(err) => {
            rollback_cpu(inner, spec.allocation, cpu.as_ref()).await;
            return Err(classify::classify_start_error(&err));
        }
    }

    // 6. Start. 304 (already started) is `Ok` — bollard maps NOT_MODIFIED to
    //    success. On failure, best-effort remove the created container (observe
    //    would eventually clear it as debris anyway), then classify.
    if let Err(err) = inner.docker.start_container(&name, START_OPTS).await {
        remove_best_effort(inner, &name, false).await;
        rollback_cpu(inner, spec.allocation, cpu.as_ref()).await;
        return Err(classify::classify_start_error(&err));
    }

    // 7. Success: record running, push the gauge. The running container holds
    //    the image pin until reap, so commit the guard (do not release). The
    //    starting-guard clears `starting`.
    {
        let mut st = lock_state(&inner.state);
        st.running.insert(spec.allocation);
        st.push_running_gauge();
    }
    _pin.commit();
    // Fresh start: spawn the telemetry collectors (§8). A brand-new container has
    // no earlier logs, so this derives no boundary and does no store I/O — but the
    // call stays after the lock block regardless (the never-across-await rule).
    let ids = ContainerIds {
        allocation: spec.allocation,
        attempt: spec.attempt,
        job: spec.job,
    };
    super::spawn_collectors(inner, ids, &name, false, image_size.as_u64(), None).await;
    Ok(())
}

async fn update_created_affinity(
    inner: &Inner,
    name: &str,
    affinity: &cpuset::Affinity,
) -> Result<(), StartError> {
    inner
        .docker
        .update_container(
            name,
            ContainerUpdateBody {
                cpuset_cpus: Some(affinity.cpuset_cpus.clone()),
                nano_cpus: (affinity.nano_cpus > 0).then_some(affinity.nano_cpus),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| StartError::Start {
            user_error: false,
            message: format!("updating adopted created container {name} CPU affinity: {err}"),
        })
}

/// `None` start options, spelled with the concrete type so the `Option<impl
/// Into<…>>` parameter can infer.
const START_OPTS: Option<bollard::query_parameters::StartContainerOptions> = None;
/// `None` inspect options.
const INSPECT_OPTS: Option<bollard::query_parameters::InspectContainerOptions> = None;

/// Assemble the create body: the resolved image bytes, the job's command and
/// (optional) entrypoint, the resolved user, the full label set (§5), and the
/// always-on [`limits::host_config`] posture (§6).
#[allow(clippy::too_many_arguments)]
fn build_create_body(
    inner: &Inner,
    spec: &StartSpec,
    image_id: &str,
    image_digest: &str,
    image_bytes: u64,
    user: &str,
    affinity: Option<&cpuset::Affinity>,
    disk_plan: &disk::DiskPlan,
) -> ContainerCreateBody {
    let mut labels = HashMap::new();
    labels.insert(LABEL_ALLOCATION.to_string(), spec.allocation.to_string());
    labels.insert(LABEL_ATTEMPT.to_string(), spec.attempt.to_string());
    labels.insert(LABEL_JOB.to_string(), spec.job.to_string());
    labels.insert(LABEL_NODE.to_string(), inner.node.to_string());
    labels.insert(LABEL_IMAGE_DIGEST.to_string(), image_digest.to_string());
    // The image's on-disk size, so the metrics sampler can report it as the
    // constant per-attempt `disk_image_bytes` and adoption/observe can recover it
    // without re-inspecting the image (§8.1).
    labels.insert(LABEL_IMAGE_BYTES.to_string(), image_bytes.to_string());
    if affinity.is_some_and(|affinity| affinity.exclusive) {
        labels.insert(LABEL_CPU_EXCLUSIVE.to_string(), "true".to_string());
    }
    // Disk-enforcement facts, so the poll enforcer can resume after a restart
    // (§5, §6.2): the strategy always, the enforced budget when there is one.
    labels.insert(
        LABEL_DISK_MODE.to_string(),
        disk_plan.mode_label.to_string(),
    );
    if let Some(budget) = &disk_plan.budget_label {
        labels.insert(LABEL_DISK_BUDGET.to_string(), budget.clone());
    }

    let mut host_config = limits::host_config(&spec.limits, inner.pids_limit);
    if let Some(affinity) = affinity {
        host_config.cpuset_cpus = Some(affinity.cpuset_cpus.clone());
        host_config.nano_cpus = (affinity.nano_cpus > 0).then_some(affinity.nano_cpus);
    }
    // Native quotas: cap the writable layer at create time (§6.2).
    host_config.storage_opt = disk_plan.storage_opt.clone();

    ContainerCreateBody {
        // Pin the resolved bytes, not the tag (§7).
        image: Some(image_id.to_string()),
        cmd: Some(spec.command.clone()),
        // `None` runs the image's own entrypoint (StartSpec contract).
        entrypoint: spec.entrypoint.clone(),
        user: Some(user.to_string()),
        labels: Some(labels),
        host_config: Some(host_config),
        ..Default::default()
    }
}

async fn prepare_cpu(
    inner: &Inner,
    spec: &StartSpec,
) -> Result<Option<cpuset::Allocation>, StartError> {
    let Some(cpuset) = &inner.cpuset else {
        return Ok(None);
    };
    let mut allocator = cpuset.lock().await;
    let allocation = allocator
        .allocate(spec.allocation, spec.limits.cpu_millis)
        .map_err(|message| StartError::Start {
            user_error: false,
            message,
        })?;
    if allocation.newly_assigned && allocation.affinity.exclusive {
        if let Err(message) =
            super::update_fractional_containers(&inner.docker, &mut allocator).await
        {
            allocator.release(spec.allocation);
            let _ = super::update_fractional_containers(&inner.docker, &mut allocator).await;
            return Err(StartError::Start {
                user_error: false,
                message,
            });
        }
    }
    Ok(Some(allocation))
}

async fn rollback_cpu(inner: &Inner, allocation: AllocationId, cpu: Option<&cpuset::Allocation>) {
    if !cpu.is_some_and(|cpu| cpu.newly_assigned) {
        return;
    }
    if let Err(err) = super::release_cpu(&inner.docker, &inner.cpuset, allocation).await {
        tracing::warn!(%allocation, error = %err, "failed to roll back CPU allocation after start failure");
    }
}

/// The disposition of a create-time name conflict (docker-executor.md §5),
/// decided purely from the survivor's inspect so it can be unit-tested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdoptDecision {
    /// Same allocation, still `created`: a prior process crashed between create
    /// and start. Config is deterministic, so start *this* container.
    StartExisting,
    /// Same allocation, already past `created` (running/exited/…): the start
    /// already happened — adopt and report success.
    AlreadyStarted,
    /// A different or missing allocation label: a conflict we can't resolve →
    /// platform `StartError` (§4).
    Unresolvable,
}

/// Decide a name conflict from the survivor's `coppice.allocation` label and
/// state, against the allocation we are starting.
pub(crate) fn adopt_decision(
    survivor_allocation: Option<&str>,
    survivor_status: Option<ContainerStateStatusEnum>,
    ours: AllocationId,
) -> AdoptDecision {
    match survivor_allocation {
        Some(label) if label == ours.to_string() => match survivor_status {
            Some(ContainerStateStatusEnum::CREATED) => AdoptDecision::StartExisting,
            _ => AdoptDecision::AlreadyStarted,
        },
        _ => AdoptDecision::Unresolvable,
    }
}

enum AdoptOutcome {
    StartExisting,
    /// The survivor already started; carries its inspect so the adopter can
    /// resume telemetry collection (§8.2) from its labels/started_at without a
    /// second round-trip. Boxed to keep the enum small.
    AlreadyStarted(Box<bollard::models::ContainerInspectResponse>),
}

/// Inspect the name-conflict survivor and apply [`adopt_decision`].
async fn adopt(inner: &Inner, name: &str, spec: &StartSpec) -> Result<AdoptOutcome, StartError> {
    let inspect = inner
        .docker
        .inspect_container(name, INSPECT_OPTS)
        .await
        .map_err(|err| StartError::Start {
            user_error: false,
            message: format!("inspecting name-conflict survivor {name}: {err}"),
        })?;
    let survivor_allocation = inspect
        .config
        .as_ref()
        .and_then(|cfg| cfg.labels.as_ref())
        .and_then(|labels| labels.get(LABEL_ALLOCATION))
        .map(String::as_str);
    let survivor_status = inspect.state.as_ref().and_then(|st| st.status);
    match adopt_decision(survivor_allocation, survivor_status, spec.allocation) {
        AdoptDecision::StartExisting => Ok(AdoptOutcome::StartExisting),
        AdoptDecision::AlreadyStarted => Ok(AdoptOutcome::AlreadyStarted(Box::new(inspect))),
        AdoptDecision::Unresolvable => Err(StartError::Start {
            user_error: false,
            message: format!(
                "container name {name} conflicts with a foreign or unlabeled container \
                 (docker-executor.md §5)"
            ),
        }),
    }
}

// ---- stop (docker-executor.md §4: daemon-arbitrated race) ---------------

/// Stop the container for `allocation`. The daemon's answer is the sole source
/// of truth for the stop-vs-natural-exit race — never inferred from timestamps
/// or ordering (docker-executor.md §4).
///
/// Note on the 304/204 distinction: Docker returns 304 (already exited) vs 204
/// (our stop terminated it), but bollard maps *both* to `Ok(())` — 304 is
/// treated as success, so `stop_container`'s return cannot tell them apart. The
/// pre-inspect (step 1) already yields `AlreadyExited` for a container that had
/// exited before we act; the residual race — a natural exit landing in the
/// window between pre-inspect and the stop taking effect — is therefore reported
/// as `Stopped` (with the OOM carve-out preserved via the post-inspect
/// evidence). This upholds §4's guard: a natural-exit verdict requires the
/// daemon's own already-exited answer, and absent that signal we attribute to
/// our stop rather than guess "natural".
pub(crate) async fn stop(
    inner: &Inner,
    allocation: AllocationId,
    grace: Duration,
) -> Result<StopOutcome, ExecutorError> {
    let name = container_name(allocation);

    // 1. Pre-inspect: already exited with usable evidence → the natural
    //    outcome wins.
    match inspect_container(inner, &name).await? {
        None => return Ok(StopOutcome::Unknown),
        Some(inspect) => {
            // A natural-exit verdict, so settle a lagging OOMKilled commit
            // first (issue #34); a running container has no exit code and
            // passes through untouched.
            let inspect = super::settle_oom_flag(&inner.docker, &name, inspect).await;
            if let Some(info) = inspect.state.as_ref().and_then(classify::exit_info) {
                claim_exit(inner, allocation).await;
                return Ok(StopOutcome::AlreadyExited(info));
            }
            // Running (or exited-without-evidence): proceed to the stop.
        }
    }

    // 2. Issue the stop with grace ceiled to whole seconds.
    let options = StopContainerOptionsBuilder::new()
        .t(grace_to_secs_ceil(grace))
        .build();
    match inner.docker.stop_container(&name, Some(options)).await {
        Ok(()) => {
            // Post-inspect for the terminal evidence. Deliberately *not*
            // settled via settle_oom_flag: a 137 here is expected from our own
            // grace-expiry SIGKILL, so the settle would burn its full budget on
            // every hard stop of a memory-limited container. The §4 OOM
            // carve-out stays best-effort single-inspect (a kill this close to
            // our own stop is attributed to the stop per ADR 0013 anyway).
            let inspect = match inspect_container(inner, &name).await? {
                Some(inspect) => inspect,
                None => return Ok(StopOutcome::Unknown),
            };
            match inspect.state.as_ref().and_then(classify::exit_info) {
                Some(info) => {
                    // `info.cause` already carries the §4 OOM carve-out: a kernel
                    // OOM kill lands as `ExitCause::OomKilled` (classify::exit_info),
                    // and the session's Stopped-with-limit-cause arm handles it.
                    // A plain stop is `ExitCause::Natural`; the caller assigns
                    // abort vs max-runtime attribution.
                    claim_exit(inner, allocation).await;
                    Ok(StopOutcome::Stopped(info))
                }
                // A stop that left no usable evidence is rare (a torn inspect);
                // surface it so the session can retry rather than fabricate.
                None => Err(ExecutorError::Other(format!(
                    "stop of {name} left no usable exit evidence"
                ))),
            }
        }
        Err(err) if api::status_code(&err) == Some(404) => Ok(StopOutcome::Unknown),
        Err(err) => Err(ExecutorError::Other(format!(
            "stopping container {name}: {err}"
        ))),
    }
    // Never remove here: an exited container is evidence until the session
    // journals its exit and calls reap (§5).
}

/// Whole seconds to wait before SIGKILL, rounding a sub-second grace *up* so a
/// e.g. 500 ms grace is not collapsed to an instant SIGKILL (docker-executor.md
/// §4). Clamped into `i32` for the query param; a zero/negative grace is 0.
fn grace_to_secs_ceil(grace: Duration) -> i32 {
    let micros = grace.as_micros();
    if micros <= 0 {
        return 0;
    }
    let secs = micros.saturating_add(999_999) / 1_000_000;
    secs.clamp(0, i32::MAX as i64) as i32
}

// ---- observe (docker-executor.md §5) ------------------------------------

/// The full set of labeled containers, mapped by the §3 table into the runtime
/// half of restart reconciliation. Debris is removed (best-effort) and not
/// reported; the running snapshot is replaced wholesale and the gauge pushed.
pub(crate) async fn observe(inner: &Inner) -> Result<Vec<ObservedContainer>, ExecutorError> {
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec![LABEL_ALLOCATION.to_string()]);
    let options = ListContainersOptionsBuilder::new()
        .all(true)
        .filters(&filters)
        .build();
    let summaries = inner
        .docker
        .list_containers(Some(options))
        .await
        .map_err(|err| ExecutorError::Other(format!("listing containers: {err}")))?;

    let now = Timestamp::now();
    let mut observed = Vec::new();
    let mut running = std::collections::HashSet::new();
    // Running containers not yet tracked by a collector — resumed as adoptions
    // (§8.2) after the state lock is released (the boundary derivation is async).
    let mut to_spawn: Vec<(ContainerIds, String, u64, Option<Timestamp>)> = Vec::new();

    for summary in summaries {
        let Some(ids) = super::parse_container_ids(summary.labels.as_ref()) else {
            tracing::warn!(
                container = summary.id.as_deref().unwrap_or("<unknown>"),
                "skipping container with missing or foreign coppice labels"
            );
            continue;
        };
        // Inspect by id where available (the name is a fallback).
        let cname = container_name(ids.allocation);
        let target = summary.id.as_deref().unwrap_or(&cname);
        let Some(inspect) = inspect_container(inner, target).await? else {
            // Vanished between list and inspect — nothing to report.
            continue;
        };
        // Recovery evidence feeds journaling like any natural exit, so give a
        // lagging OOMKilled commit its bounded window too (issue #34); almost
        // always a no-op this long after the exit.
        let inspect = super::settle_oom_flag(&inner.docker, target, inspect).await;
        let Some(cstate) = inspect.state.as_ref() else {
            continue;
        };

        match state::map_container(cstate, now) {
            Mapped::Report(runtime_state) => {
                if matches!(runtime_state, ContainerState::Running { .. }) {
                    running.insert(ids.allocation);
                    // Queue a collector resume for a running container we are not
                    // yet tracking (§8.2). The brief lock is released at once.
                    if inner.telemetry.is_some()
                        && !lock_state(&inner.state)
                            .collectors
                            .contains_key(&ids.allocation)
                    {
                        let image_bytes = super::image_bytes_from_labels(
                            inspect.config.as_ref().and_then(|cfg| cfg.labels.as_ref()),
                        );
                        let started_at = cstate
                            .started_at
                            .as_deref()
                            .and_then(classify::parse_docker_time);
                        to_spawn.push((ids, cname.clone(), image_bytes, started_at));
                    }
                }
                observed.push(report(ids, runtime_state));
            }
            Mapped::StartDebris => {
                // A `created` container: OUR in-flight create (skip silently) or
                // crash debris (remove; the journaled intent with no evidence
                // reports AgentError via observed.rs rule 3).
                let ours = lock_state(&inner.state).starting.contains(&ids.allocation);
                if !ours {
                    remove_best_effort(inner, target, false).await;
                    // Removing debris ends this allocation's life here, so drop
                    // its image pin too — a pin cannot leak past the reconciler
                    // (§7).
                    inner.cache.release(ids.allocation);
                }
            }
            Mapped::ReapInFlight => {
                // A reap already in flight; terminal evidence was captured.
            }
            Mapped::DeadUnusable => {
                // Force-remove; report nothing (same AgentError channel, §3).
                remove_best_effort(inner, target, true).await;
                // Same as the debris arm: the allocation is gone, so release its
                // image pin so the reconciler cannot leak one (§7).
                inner.cache.release(ids.allocation);
            }
        }
    }

    // Replace the running snapshot wholesale and push the gauge. Reported
    // containers are NOT filtered by `claimed`: observe is the full-state
    // reconciliation snapshot, and the session's idempotent exit handling is
    // the §4 backstop.
    {
        let mut st = lock_state(&inner.state);
        st.running = running;
        st.push_running_gauge();
    }
    // Resume the collectors for surviving running containers (§8.2), now that the
    // state lock is released — `spawn_collectors` re-checks the map itself.
    for (ids, cname, image_bytes, started_at) in to_spawn {
        super::spawn_collectors(inner, ids, &cname, true, image_bytes, started_at).await;
    }
    Ok(observed)
}

fn report(ids: ContainerIds, state: ContainerState) -> ObservedContainer {
    ObservedContainer {
        allocation: ids.allocation,
        attempt: ids.attempt,
        job: ids.job,
        state,
    }
}

// ---- reap (docker-executor.md §5, §8.2/§8.4) ----------------------------

/// How long reap awaits a live follower's drain before failing retryably
/// (docker-executor.md §8.2). Short: the follower drains an EOF stream in
/// milliseconds; a genuinely wedged one is caught by `drain_force_after`.
const REAP_DRAIN_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// How long reap awaits the hub flush before failing retryably (docker-executor.md
/// §8.4): "hub drained" must be ordered before the attempt-ended marker, but a
/// wedged sink must not block reap forever.
const REAP_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How old a [`CollectorSlot::Reserved`] may grow before reap treats it as
/// abandoned. Activation normally follows its reservation within milliseconds
/// (one store query), so a reservation this old means the initializing caller
/// was cancelled mid-await and will never activate — without this bound its
/// reap would return "still initializing" forever and the container would never
/// be removed. Generous by three orders of magnitude over the normal case.
pub(crate) const RESERVATION_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// Remove an exited container's runtime record. The contract is no-op-safe:
/// 404 (already gone) and 409 (removal already in progress) are `Ok`. Other
/// errors surface so the session's janitor retries. Anonymous volumes go with
/// the evidence (`v: true`); never force (`force: false`) — an exited container
/// is the terminal evidence, never a live one to kill.
///
/// When telemetry is configured, reap first runs the §8.2/§8.4 **drain barrier**
/// ([`drain_telemetry`]): it waits (bounded) for the follower to reach
/// end-of-stream, flushes the hub, and persists the attempt-ended marker — all
/// *before* the container is removed. Any of those failing (or timing out)
/// returns an `Err` that leaves the container intact, so the session's periodic
/// sweep retries and a slow drain never costs tail logs (the existing retryable
/// contract). The backstop for a wedged follower is forced past
/// `drain_force_after`, metered — never silent.
pub(crate) async fn reap(inner: &Inner, allocation: AllocationId) -> Result<(), ExecutorError> {
    let name = container_name(allocation);

    // Telemetry drain barrier (§8.2/§8.4), before the container is removed. A
    // retryable error here leaves the container intact for the janitor's retry.
    if let Some(telemetry) = inner.telemetry.as_ref() {
        drain_telemetry(inner, telemetry, allocation, &name).await?;
    }

    let options = RemoveContainerOptionsBuilder::new()
        .v(true)
        .force(false)
        .build();
    match inner.docker.remove_container(&name, Some(options)).await {
        Ok(()) => {}
        Err(err) => match api::status_code(&err) {
            Some(404) => {} // already gone — the contract says no-op
            Some(409) => {
                tracing::debug!(container = %name, "reap: removal already in progress");
            }
            _ => {
                return Err(ExecutorError::Other(format!("reaping {name}: {err}")));
            }
        },
    }
    // Clear all tracking for the allocation and push the gauge. The collector
    // entry is removed here and its tasks aborted defensively (the follower has
    // usually ended at EOF already, and the sampler at exit claim).
    {
        let mut st = lock_state(&inner.state);
        if let Some(CollectorSlot::Active(collectors)) = st.collectors.remove(&allocation) {
            if let Some(sampler) = collectors.sampler {
                sampler.abort();
            }
            if let Some(follower) = collectors.follower {
                follower.abort();
            }
        }
        st.claimed.remove(&allocation);
        st.running.remove(&allocation);
        st.starting.remove(&allocation);
        st.push_running_gauge();
    }
    // Reap is the terminal cleanup: drop the image pin here (evidence retention
    // kept it through the container's exited life, §7). This stamps
    // `last_used_at` on the image when its last pin drains, starting the TTL
    // clock from the end of the last attempt that used it.
    inner.cache.release(allocation);
    Ok(())
}

/// How reap resolves a live follower's drain state (docker-executor.md §8.2), the
/// pure decision behind [`drain_telemetry`]'s step 2 so its matrix is unit-tested
/// without a daemon. An integration test cannot exercise the [`DrainVerdict::Force`]
/// arm through the public trait surface — it would need a follower wedged mid-drain,
/// and a real follower reaches EOF in milliseconds (far too fast to race a
/// `drain_force_after` clock reliably), so the wedged-drain case is deliberately
/// unit-level here while the healthy drain-before-reap ordering is proven in the
/// gated suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainVerdict {
    /// The follower already signalled EOF: nothing to wait on, remove the
    /// container.
    Proceed,
    /// The follower is not drained and its exit was claimed at least
    /// `force_after` ago: abort it and proceed, metering the tail loss.
    Force,
    /// The follower is not drained and the force clock has not elapsed (or no
    /// exit was ever claimed, so there is no force clock): wait, bounded by
    /// [`REAP_DRAIN_WAIT`], and fail retryably on timeout.
    Wait,
}

/// Decide reap's drain disposition from the follower's drained flag and the exit
/// claim clock (docker-executor.md §8.2). Drained always wins, regardless of the
/// clock. An *unclaimed* exit has no force clock — it can only [`Wait`], never
/// [`Force`] — because `drain_force_after` is measured from the exit claim.
///
/// [`Wait`]: DrainVerdict::Wait
/// [`Force`]: DrainVerdict::Force
fn drain_verdict(
    drained: bool,
    exit_claimed_at: Option<Timestamp>,
    now: Timestamp,
    force_after: Duration,
) -> DrainVerdict {
    if drained {
        return DrainVerdict::Proceed;
    }
    match exit_claimed_at {
        Some(claimed) if (now - claimed) >= force_after => DrainVerdict::Force,
        _ => DrainVerdict::Wait,
    }
}

/// The §8.2/§8.4 telemetry drain barrier reap runs before removing a container.
///
/// Steps, in order: (1) snapshot the collector entry; (2) await the live
/// follower's drain, bounded by [`REAP_DRAIN_WAIT`], forcing past
/// `drain_force_after` per [`drain_verdict`]; (3) with no entry, catch-up drain a
/// still-dead container; (4) flush the hub, bounded by [`REAP_FLUSH_TIMEOUT`]; (5)
/// persist the attempt-ended marker on every store. Steps 2 and 4 return retryable
/// `Err`s that leave the container intact; step 5's first failure does too
/// (`attempt_ended` is idempotent, so retrying is safe).
async fn drain_telemetry(
    inner: &Inner,
    telemetry: &TelemetryWiring,
    allocation: AllocationId,
    name: &str,
) -> Result<(), ExecutorError> {
    // 1. Snapshot the collector slot under the lock (never held across await).
    // Three shapes: Active (drive the drain), Reserved (collectors still
    // initializing — retry), absent (catch-up drain a dead container). A
    // reservation older than [`RESERVATION_STALE_AFTER`] was abandoned by a
    // cancelled initializer and will never activate: it is removed here and
    // treated as absent, so the catch-up path finalises the container instead of
    // reap retrying "still initializing" forever. A late activation then finds
    // its reservation gone and aborts its fresh tasks.
    enum Snapshot {
        Active(
            ContainerIds,
            tokio::sync::watch::Receiver<bool>,
            Option<Timestamp>,
        ),
        Initializing,
        Absent,
    }
    let snapshot = {
        let mut st = lock_state(&inner.state);
        match st.collectors.get(&allocation) {
            Some(CollectorSlot::Active(collectors)) => Snapshot::Active(
                collectors.ids,
                collectors.drained.clone(),
                collectors.exit_claimed_at,
            ),
            Some(CollectorSlot::Reserved { reserved_at, .. }) => {
                let stale =
                    (Timestamp::now() - *reserved_at) >= Duration::from(RESERVATION_STALE_AFTER);
                if stale {
                    tracing::warn!(
                        %allocation,
                        "collector reservation abandoned (initializer cancelled?); \
                         removing it and falling back to the catch-up drain (§8.2)"
                    );
                    st.collectors.remove(&allocation);
                    Snapshot::Absent
                } else {
                    Snapshot::Initializing
                }
            }
            None => Snapshot::Absent,
        }
    };

    let ids = match snapshot {
        // Activation is imminent — the boundary query is fast — so fail retryably
        // and let the janitor's periodic sweep pick the container up once the slot
        // is Active. Removing it now would orphan the initializing tasks.
        Snapshot::Initializing => {
            return Err(ExecutorError::Other(format!(
                "telemetry collectors still initializing for {allocation}; reap will retry"
            )));
        }
        Snapshot::Active(ids, mut drained, exit_claimed_at) => {
            // 2. Await the follower's drain, or force past drain_force_after.
            // Read both flags into locals so the `watch::Ref` guard is dropped
            // (each on its own statement) before any await — never held across one
            // (§11) — and before the `Wait` arm borrows `drained` for `wait_for`.
            let is_drained = *drained.borrow();
            let channel_closed = drained.has_changed().is_err();
            // A closed channel (the follower ended/panicked/was aborted without
            // signalling `drained`) is detected up front, not only after the Wait
            // timeout: removing the container now would lose the tail unmetered, so
            // recover it via a one-shot catch-up drain, then proceed. This is also
            // where a *prior forced* reap lands on its next attempt — the Force arm
            // aborted the follower (closing its channel), so the follow-up reap runs
            // catch-up and recovers whatever the daemon still retains (§8.2).
            if !is_drained && channel_closed {
                catch_up_lost_follower(inner, telemetry, ids, name).await?;
            } else {
                match drain_verdict(
                    is_drained,
                    exit_claimed_at,
                    Timestamp::now(),
                    Duration::from(telemetry.drain_force_after),
                ) {
                    DrainVerdict::Proceed => {}
                    DrainVerdict::Force => {
                        // Abort + meter + error-log only the FIRST time this slot is
                        // forced (`forced` latch under the lock): a reap retry loop
                        // after a forced drain (e.g. a later flush timeout) must not
                        // double-count `agent_log_drain_forced_total`. The follower's
                        // channel closes on the abort, so the next reap attempt takes
                        // the closed-channel catch-up path above rather than this arm.
                        let newly_forced = {
                            let mut st = lock_state(&inner.state);
                            match st.collectors.get_mut(&allocation) {
                                Some(CollectorSlot::Active(collectors)) if !collectors.forced => {
                                    if let Some(follower) = collectors.follower.take() {
                                        follower.abort();
                                    }
                                    collectors.forced = true;
                                    true
                                }
                                _ => false,
                            }
                        };
                        if newly_forced {
                            metrics::counter!(AGENT_LOG_DRAIN_FORCED_TOTAL).increment(1);
                            tracing::error!(
                                %allocation,
                                "forced log drain past drain_force_after; tail logs may be lost (§8.2)"
                            );
                        }
                    }
                    DrainVerdict::Wait => {
                        // Reduce the `wait_for` outcome to a Send `bool` *before* the
                        // catch-up await: `wait_for` resolves to a `watch::Ref` guard,
                        // and awaiting while it is still the match scrutinee would hold
                        // that non-Send guard across the await (§11).
                        let closed =
                            match tokio::time::timeout(REAP_DRAIN_WAIT, drained.wait_for(|d| *d))
                                .await
                            {
                                Ok(Ok(_)) => false,
                                // The sender dropped without `true` (the follower panicked
                                // or was aborted mid-wait): recover the tail via catch-up.
                                Ok(Err(_)) => true,
                                Err(_elapsed) => {
                                    return Err(ExecutorError::Other(format!(
                                    "log drain still in flight for {allocation}; reap will retry"
                                )));
                                }
                            };
                        if closed {
                            catch_up_lost_follower(inner, telemetry, ids, name).await?;
                        }
                    }
                }
            }
            ids
        }
        // 3. No entry: catch-up drain a still-existing dead container (§8.2).
        Snapshot::Absent => match catch_up_drain(inner, telemetry, name).await? {
            Some(ids) => ids,
            // Container gone AND untracked: without ids there is no attempt to
            // mark, and observe/journal recovery owns those (§5) — nothing to do.
            None => return Ok(()),
        },
    };

    // 4. Flush the hub so every popped batch is persisted before the marker.
    if tokio::time::timeout(REAP_FLUSH_TIMEOUT, telemetry.hub.flush())
        .await
        .is_err()
    {
        return Err(ExecutorError::Other(format!(
            "telemetry hub flush timed out for {allocation}; reap will retry"
        )));
    }

    // 5. Persist the ended marker on every store in wiring order. Idempotent, so
    //    a retry after a partial failure is safe.
    let ended_at = Timestamp::now();
    for store in &telemetry.stores {
        store
            .attempt_ended(&ids.job, &ids.attempt, ended_at)
            .await
            .map_err(|err| {
                ExecutorError::Other(format!(
                    "telemetry ended marker not persisted for {}/{}: {err}; reap will retry",
                    ids.job, ids.attempt
                ))
            })?;
    }
    Ok(())
}

/// Reap step 3 (docker-executor.md §8.2): a one-shot at-least-once catch-up drain
/// of a container reap found already dead with no live follower (an
/// adopted-already-exited container, or a pre-telemetry one). Inspect for ids and
/// start time, derive the boundary from the primary store, and drain the logs.
/// Returns the ids to finalise with, or `None` when the container is gone or
/// unidentifiable (nothing to finalise).
async fn catch_up_drain(
    inner: &Inner,
    telemetry: &TelemetryWiring,
    name: &str,
) -> Result<Option<ContainerIds>, ExecutorError> {
    let Some(inspect) = inspect_container(inner, name).await? else {
        return Ok(None); // 404 — the container is gone
    };
    let Some(ids) =
        super::parse_container_ids(inspect.config.as_ref().and_then(|cfg| cfg.labels.as_ref()))
    else {
        return Ok(None); // unidentifiable — not ours to finalise
    };
    let started_at = inspect
        .state
        .as_ref()
        .and_then(|st| st.started_at.as_deref())
        .and_then(classify::parse_docker_time);
    let (boundary, replay_max) =
        derive_catchup_boundary(&telemetry.log_store, ids, started_at).await;
    logs::catch_up(
        &inner.docker,
        &telemetry.hub,
        ids,
        name,
        boundary,
        replay_max,
    )
    .await?;
    Ok(Some(ids))
}

/// A one-shot catch-up drain for a container whose live follower vanished before
/// signalling `drained` (docker-executor.md §8.2): reap knows the ids and name, so
/// it recovers the tail via [`logs::catch_up`] rather than removing the container
/// and losing it unmetered. The boundary is derived from `log_store` with
/// `started_at: None` — with no stored logs the boundary is then `None` ⇒
/// `since = 0`, a full replay of whatever the daemon still retains, the safe
/// at-least-once direction. A catch-up `Err` stays retryable (propagated).
async fn catch_up_lost_follower(
    inner: &Inner,
    telemetry: &TelemetryWiring,
    ids: ContainerIds,
    name: &str,
) -> Result<(), ExecutorError> {
    let (boundary, replay_max) = derive_catchup_boundary(&telemetry.log_store, ids, None).await;
    logs::catch_up(
        &inner.docker,
        &telemetry.hub,
        ids,
        name,
        boundary,
        replay_max,
    )
    .await?;
    tracing::warn!(
        allocation = %ids.allocation,
        "log follower vanished before signalling drained; ran catch-up drain (§8.2)"
    );
    Ok(())
}

/// Derive the §8.2 catch-up resume boundary from the log store (the resume
/// authority, Fix 1): the newest stored log timestamp is the boundary (floored)
/// and its raw value the replay window; with no store, an empty store, or a store
/// error, fall back to `started_at` (floored) with no replay window. Shared by
/// [`catch_up_drain`] and [`catch_up_lost_follower`].
async fn derive_catchup_boundary(
    log_store: &Option<FilesystemSink>,
    ids: ContainerIds,
    started_at: Option<Timestamp>,
) -> (Option<Timestamp>, Option<Timestamp>) {
    match log_store {
        Some(store) => match store.max_log_timestamp(&ids.job, &ids.attempt).await {
            Ok(Some(max)) => (Some(logs::floor_to_second(max)), Some(max)),
            Ok(None) => (started_at.map(logs::floor_to_second), None),
            Err(err) => {
                tracing::debug!(
                    job = %ids.job,
                    attempt = %ids.attempt,
                    error = %err,
                    "catch-up boundary from the store failed; using the container start time"
                );
                (started_at.map(logs::floor_to_second), None)
            }
        },
        None => (started_at.map(logs::floor_to_second), None),
    }
}

// ---- shared helpers -----------------------------------------------------

/// Inspect a container by name or id, folding a 404 into `None` (vanished /
/// never existed). Other errors surface as [`ExecutorError`].
async fn inspect_container(
    inner: &Inner,
    target: &str,
) -> Result<Option<bollard::models::ContainerInspectResponse>, ExecutorError> {
    match inner.docker.inspect_container(target, INSPECT_OPTS).await {
        Ok(inspect) => Ok(Some(inspect)),
        Err(err) if api::status_code(&err) == Some(404) => Ok(None),
        Err(err) => Err(ExecutorError::Other(format!("inspecting {target}: {err}"))),
    }
}

/// Best-effort container removal: log a failure and move on. Used for start-
/// failure cleanup and observe's debris removal, where the reconciliation loop
/// (or a later observe) is the backstop.
async fn remove_best_effort(inner: &Inner, target: &str, force: bool) {
    let options = RemoveContainerOptions {
        v: true,
        force,
        link: false,
    };
    if let Err(err) = inner.docker.remove_container(target, Some(options)).await {
        tracing::debug!(container = %target, error = %err, "best-effort container removal failed");
    }
}

/// Mark an exit as surfaced: claim it (duplicate suppression, §4), drop it from
/// the running set, and push the gauge. Called on both stop evidence paths —
/// each holds terminal exit evidence from an inspect, so the container is
/// proven dead and the follower's fast drain fires here too (§8.2).
async fn claim_exit(inner: &Inner, allocation: AllocationId) {
    {
        let mut st = lock_state(&inner.state);
        st.claimed.insert(allocation);
        // Stop this container's sampler and start its drain clock (§8.2).
        st.note_exit_claimed(allocation, Timestamp::now());
        st.note_container_dead(allocation);
        st.running.remove(&allocation);
        st.push_running_gauge();
    }
    if let Err(err) = super::release_cpu(&inner.docker, &inner.cpuset, allocation).await {
        tracing::warn!(%allocation, error = %err, "failed to grow fractional cpuset after stop");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grace_ceils_subsecond_up_and_floors_zero() {
        assert_eq!(grace_to_secs_ceil(Duration::ZERO), 0);
        // Sub-second grace must not collapse to an instant SIGKILL.
        assert_eq!(grace_to_secs_ceil(Duration::from_millis(1)), 1);
        assert_eq!(grace_to_secs_ceil(Duration::from_millis(500)), 1);
        assert_eq!(grace_to_secs_ceil(Duration::from_millis(999)), 1);
        assert_eq!(grace_to_secs_ceil(Duration::from_secs(1)), 1);
        // Exact whole seconds are not rounded up past themselves.
        assert_eq!(grace_to_secs_ceil(Duration::from_secs(30)), 30);
        // A hair over a whole second rounds to the next.
        assert_eq!(
            grace_to_secs_ceil(Duration::from_secs(30).saturating_add(Duration::from_micros(1))),
            31
        );
        // Negative grace floors at zero.
        assert_eq!(grace_to_secs_ceil(Duration::from_micros(-5)), 0);
    }

    #[test]
    fn grace_clamps_enormous_spans_to_i32_max() {
        assert_eq!(grace_to_secs_ceil(Duration::MAX), i32::MAX);
    }

    #[test]
    fn adopt_same_alloc_created_starts_existing() {
        let ours = AllocationId::new();
        assert_eq!(
            adopt_decision(
                Some(&ours.to_string()),
                Some(ContainerStateStatusEnum::CREATED),
                ours
            ),
            AdoptDecision::StartExisting
        );
    }

    #[test]
    fn adopt_same_alloc_running_or_exited_is_already_started() {
        let ours = AllocationId::new();
        for status in [
            ContainerStateStatusEnum::RUNNING,
            ContainerStateStatusEnum::EXITED,
            ContainerStateStatusEnum::PAUSED,
            ContainerStateStatusEnum::DEAD,
        ] {
            assert_eq!(
                adopt_decision(Some(&ours.to_string()), Some(status), ours),
                AdoptDecision::AlreadyStarted,
                "{status:?} for our own allocation is an already-started adopt"
            );
        }
    }

    // ---- drain_verdict matrix (docker-executor.md §8.2) ------------------
    //
    // The wedged-follower `Force` arm is proven here rather than in the gated
    // integration suite: an integration test cannot inject a follower that never
    // reaches EOF through the public `Executor` trait, and a real follower EOFs in
    // milliseconds — far too fast to race a `drain_force_after` clock reliably. The
    // pure verdict is therefore the level at which the force/wait boundary is
    // asserted; the healthy drain-before-reap ordering is proven in
    // `docker_executor.rs`'s `drain_completes_before_reap_removes_the_container`.

    /// A concrete `now`, and helpers for a claim at a given age.
    fn t(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn drained_proceeds_regardless_of_the_clock() {
        let force_after = Duration::from_mins(10);
        // Drained wins even with a claim well past the force window...
        assert_eq!(
            drain_verdict(true, Some(t(0)), t(10_000), force_after),
            DrainVerdict::Proceed
        );
        // ...and even with no exit claim at all.
        assert_eq!(
            drain_verdict(true, None, t(10_000), force_after),
            DrainVerdict::Proceed
        );
    }

    #[test]
    fn undrained_and_unclaimed_waits_never_forces() {
        // An unclaimed exit has no force clock: it can only wait, never force,
        // no matter how far `now` has advanced.
        let force_after = Duration::from_mins(10);
        assert_eq!(
            drain_verdict(false, None, t(10_000), force_after),
            DrainVerdict::Wait
        );
    }

    #[test]
    fn undrained_claimed_just_now_waits() {
        let force_after = Duration::from_mins(10);
        let now = t(1_000);
        // Claimed at `now`: zero elapsed, below the force window → wait.
        assert_eq!(
            drain_verdict(false, Some(now), now, force_after),
            DrainVerdict::Wait
        );
        // A hair under the window still waits.
        assert_eq!(
            drain_verdict(
                false,
                Some(t(1)),
                t(1) + force_after - Duration::from_micros(1),
                force_after
            ),
            DrainVerdict::Wait
        );
    }

    #[test]
    fn undrained_claimed_past_the_window_forces() {
        let force_after = Duration::from_mins(10);
        // Claimed well past the window → force.
        assert_eq!(
            drain_verdict(false, Some(t(0)), t(1_000), force_after),
            DrainVerdict::Force
        );
    }

    #[test]
    fn undrained_claimed_exactly_at_the_window_forces() {
        // The boundary is inclusive (`>= force_after`): now − claimed == force_after
        // forces rather than waits.
        let force_after = Duration::from_mins(10);
        let claimed = t(1);
        assert_eq!(
            drain_verdict(false, Some(claimed), claimed + force_after, force_after),
            DrainVerdict::Force
        );
    }

    #[test]
    fn adopt_foreign_or_missing_label_is_unresolvable() {
        let ours = AllocationId::new();
        let other = AllocationId::new();
        assert_eq!(
            adopt_decision(
                Some(&other.to_string()),
                Some(ContainerStateStatusEnum::CREATED),
                ours
            ),
            AdoptDecision::Unresolvable
        );
        assert_eq!(
            adopt_decision(None, Some(ContainerStateStatusEnum::RUNNING), ours),
            AdoptDecision::Unresolvable
        );
        assert_eq!(
            adopt_decision(Some("garbage"), None, ours),
            AdoptDecision::Unresolvable
        );
    }
}
