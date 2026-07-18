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

use bollard::models::{
    ContainerCreateBody, ContainerStateStatusEnum, ContainerUpdateBody, ImageInspect,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    RemoveContainerOptions, RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
};
use tokio_stream::StreamExt;

use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};

use super::state::Mapped;
use super::{
    api, classify, container_name, cpuset, disk, limits, lock_state, state, ContainerIds, Inner,
    LABEL_ALLOCATION, LABEL_ATTEMPT, LABEL_CPU_EXCLUSIVE, LABEL_DISK_BUDGET, LABEL_DISK_MODE,
    LABEL_IMAGE_DIGEST, LABEL_JOB, LABEL_NODE,
};
use crate::executor::{
    ContainerState, ExecutorError, ObservedContainer, StartError, StartSpec, StopOutcome,
};
use crate::pressure::DiskPressure;

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
    // 3. Resolve the image (local wins; 404 → pull, then re-inspect).
    let image = resolve_image(inner, &spec.image).await?;
    // Pin the exact bytes we resolved: create with the image `id`, not the
    // movable tag (tag-drift re-resolution is future work, §7).
    let image_id = image.id.clone().unwrap_or_default();
    // Resolved digest for the cache-pinning label: first repo-digest if any,
    // else the image id.
    let image_digest = image
        .repo_digests
        .as_ref()
        .and_then(|digests| digests.first().cloned())
        .or_else(|| image.id.clone())
        .unwrap_or_default();

    // 4. User (§6): honor a non-root image `USER`, else the config default;
    //    reject UID 0 as a user error.
    let image_user = image.config.as_ref().and_then(|cfg| cfg.user.as_deref());
    let user = limits::resolve_user(image_user, inner.default_uid)?;

    // Disk plan (§6.2): decide the create-time storage-opt/labels behind the
    // DiskEnforcer seam, using the already-resolved image size. An image that
    // alone exceeds the job's disk request fails here as a user error, before
    // the container (or any CPU grant) is created.
    // Docker reports the image size as a signed integer; this is where it
    // becomes a typed size. A negative reading is nonsense rather than a real
    // image, so it clamps to zero and the plan treats it as "no image cost".
    let image_size = ByteSize::from_bytes(image.size.map(|size| size.max(0) as u64).unwrap_or(0));
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
                Ok(AdoptOutcome::AlreadyStarted) => return Ok(()),
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

    // 7. Success: record running, push the gauge. The guard clears `starting`.
    {
        let mut st = lock_state(&inner.state);
        st.running.insert(spec.allocation);
        st.push_running_gauge();
    }
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

/// Resolve `image` to an [`ImageInspect`]: use the local image if present, else
/// pull it and inspect again (docker-executor.md §3 step 3). A non-404 inspect
/// error, or any pull/stream error, maps to [`StartError::Pull`].
async fn resolve_image(inner: &Inner, image: &str) -> Result<ImageInspect, StartError> {
    match inner.docker.inspect_image(image).await {
        Ok(inspect) => Ok(inspect),
        Err(err) if api::status_code(&err) == Some(404) => {
            pull_image(inner, image).await?;
            inner
                .docker
                .inspect_image(image)
                .await
                .map_err(|err| classify::classify_pull_error(&err, image))
        }
        Err(err) => Err(classify::classify_pull_error(&err, image)),
    }
}

/// Pull `image`, draining the whole progress stream. Any stream item error, or
/// a terminal error, maps to [`StartError::Pull`] (docker-executor.md §4). The
/// per-reference singleflight and tag/digest handling are the S3 cache
/// manager's job (§7); here the full reference is handed to `fromImage`.
async fn pull_image(inner: &Inner, image: &str) -> Result<(), StartError> {
    let options = CreateImageOptionsBuilder::new().from_image(image).build();
    let mut stream = std::pin::pin!(inner.docker.create_image(Some(options), None, None));
    while let Some(item) = stream.next().await {
        item.map_err(|err| classify::classify_pull_error(&err, image))?;
    }
    Ok(())
}

/// Assemble the create body: the resolved image bytes, the job's command and
/// (optional) entrypoint, the resolved user, the full label set (§5), and the
/// always-on [`limits::host_config`] posture (§6).
fn build_create_body(
    inner: &Inner,
    spec: &StartSpec,
    image_id: &str,
    image_digest: &str,
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
    AlreadyStarted,
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
        AdoptDecision::AlreadyStarted => Ok(AdoptOutcome::AlreadyStarted),
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
                }
            }
            Mapped::ReapInFlight => {
                // A reap already in flight; terminal evidence was captured.
            }
            Mapped::DeadUnusable => {
                // Force-remove; report nothing (same AgentError channel, §3).
                remove_best_effort(inner, target, true).await;
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

// ---- reap (docker-executor.md §5) ---------------------------------------

/// Remove an exited container's runtime record. The contract is no-op-safe:
/// 404 (already gone) and 409 (removal already in progress) are `Ok`. Other
/// errors surface so the session's janitor retries. Anonymous volumes go with
/// the evidence (`v: true`); never force (`force: false`) — an exited container
/// is the terminal evidence, never a live one to kill.
pub(crate) async fn reap(inner: &Inner, allocation: AllocationId) -> Result<(), ExecutorError> {
    let name = container_name(allocation);
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
    // Clear all tracking for the allocation and push the gauge.
    {
        let mut st = lock_state(&inner.state);
        st.claimed.remove(&allocation);
        st.running.remove(&allocation);
        st.starting.remove(&allocation);
        st.push_running_gauge();
    }
    Ok(())
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
/// the running set, and push the gauge. Called on both stop evidence paths.
async fn claim_exit(inner: &Inner, allocation: AllocationId) {
    {
        let mut st = lock_state(&inner.state);
        st.claimed.insert(allocation);
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
