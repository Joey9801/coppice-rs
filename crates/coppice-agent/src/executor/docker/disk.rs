//! Disk hard-limit enforcement, two strategies behind one seam
//! (docker-executor.md §6.2).
//!
//! A job's disk usage is defined as `writable_layer + image_size`, so the
//! *enforced* writable-layer budget is `limits.disk − image_size`. The
//! [`DiskEnforcer`] chooses a strategy once at startup and records the choice
//! (and the enforced budget) on every container it creates, so the poll
//! enforcer can resume for the right containers after an agent restart — the
//! container is the durable record of its own runtime facts (§5).
//!
//! - **Native (xfs project quotas):** the daemon enforces a per-container
//!   `storage_opt: size=<bytes>` hard cap on the writable layer. A job that
//!   fills its budget sees `ENOSPC` and exits on its own (a natural exit).
//! - **Poll fallback:** [`spawn`] runs a serial sweep every
//!   `disk_poll_interval` that reads writable-layer usage through the Docker
//!   API only (`GET /system/df`, with a single-container `ContainerInspect`
//!   recheck before any kill verdict) and kills a container past its budget
//!   outright, reporting [`ExitCause::DiskKilled`].
//!
//! The pure pieces — budget arithmetic, the strategy decision, and the
//! over-budget verdict — are factored out and unit-tested without a daemon
//! (§12). The poll loop is deliberately the skeleton the future soft-limit
//! killer generalizes across resources (§6.1): the sweep → verdict → kill
//! structure stays cleanly separated, with a pure verdict function over
//! `(size_rw, budget)`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use bollard::query_parameters::{
    DataUsageOptions, InspectContainerOptionsBuilder, KillContainerOptionsBuilder,
    ListContainersOptionsBuilder, ListImagesOptionsBuilder, RemoveContainerOptions,
};
use bollard::Docker;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;

use super::{
    classify, cpuset, lock_state, ExecutorState, LABEL_DISK_BUDGET, LABEL_DISK_MODE, LABEL_NODE,
};
use crate::config::DiskEnforcement;
use crate::executor::{ExecutorError, ExitCause, ExitEvent, StartError};

// ---- metrics (docker-executor.md §8.1) ----------------------------------

/// Wall-clock duration of one poll sweep, pushed per sweep. A histogram: the
/// design flags these `GET /system/df` + `ContainerInspect` calls as daemon-side
/// expensive, so the distribution (not just a mean) is what an operator watches.
const AGENT_DISK_POLL_DURATION: &str = "agent_disk_poll_duration_seconds";
/// Hard-limit kills, labelled by `kind` (docker-executor.md §8.1: "limit kills
/// by kind"). Incremented with `kind = "disk"` per [`ExitCause::DiskKilled`]
/// kill; the memory/runtime kinds join it as their kill paths gain counters.
const AGENT_LIMIT_KILLS_TOTAL: &str = "agent_limit_kills_total";
/// The `kind` label value for a disk-budget kill.
const KIND_DISK: &str = "disk";

/// Register this module's metric names (docker-executor.md §8.1). Part of the
/// docker module's `describe_metrics` fan-out.
pub(crate) fn describe_metrics() {
    metrics::describe_histogram!(
        AGENT_DISK_POLL_DURATION,
        metrics::Unit::Seconds,
        "Wall-clock duration of one disk-enforcer poll sweep (§6.2)."
    );
    metrics::describe_counter!(
        AGENT_LIMIT_KILLS_TOTAL,
        metrics::Unit::Count,
        "Containers killed by the executor for breaching a hard resource limit, by kind."
    );
}

/// Point-in-time sampling for this module. A no-op: both metrics are *pushed*
/// (per sweep / per kill), matching the view.rs push-on-event convention.
pub(crate) fn gather_metrics() {}

// ---- strategy selection (docker-executor.md §6.2) -----------------------

/// The disk-enforcement strategy chosen at startup and stamped on every
/// container ([`LABEL_DISK_MODE`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskMode {
    /// Native xfs project quotas: the daemon caps the writable layer via
    /// `storage_opt: size`.
    Quota,
    /// Poll fallback: [`spawn`] sweeps usage and kills over-budget containers.
    Poll,
}

impl DiskMode {
    /// The [`LABEL_DISK_MODE`] string value.
    pub(crate) fn label_value(self) -> &'static str {
        match self {
            DiskMode::Quota => "quota",
            DiskMode::Poll => "poll",
        }
    }
}

/// The outcome of the startup storage-opt probe (docker-executor.md §6.2). The
/// probe is ground truth; `docker info`'s driver/backing-filesystem fields are
/// only for the log message. Kept as an enum so [`decide_mode`] is pure and the
/// config × probe matrix is unit-testable without a daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    /// A probe create with a `size` storage-opt succeeded — native quotas work.
    Supported,
    /// The daemon rejected the `size` storage-opt as unsupported.
    Unsupported,
    /// No local image to probe with, or a create failure we can't attribute to
    /// storage-opt support (e.g. a transient daemon error).
    Inconclusive,
}

/// Decide the strategy from the configured selector and the probe outcome. Pure
/// (docker-executor.md §6.2):
///
/// - `poll` → [`DiskMode::Poll`] unconditionally (the probe is not even run).
/// - `auto` → [`DiskMode::Quota`] iff the probe confirmed support, else
///   [`DiskMode::Poll`] (an inconclusive probe falls back).
/// - `quota` (operator assertion) → [`DiskMode::Quota`] unless the probe
///   *refutes* support, which is a hard startup error; an inconclusive probe
///   proceeds as quota (per-job creates surface any failure as platform
///   [`StartError`]).
pub(crate) fn decide_mode(
    selector: DiskEnforcement,
    probe: ProbeOutcome,
) -> Result<DiskMode, String> {
    match selector {
        DiskEnforcement::Poll => Ok(DiskMode::Poll),
        DiskEnforcement::Auto => Ok(match probe {
            ProbeOutcome::Supported => DiskMode::Quota,
            ProbeOutcome::Unsupported | ProbeOutcome::Inconclusive => DiskMode::Poll,
        }),
        DiskEnforcement::Quota => match probe {
            ProbeOutcome::Supported | ProbeOutcome::Inconclusive => Ok(DiskMode::Quota),
            ProbeOutcome::Unsupported => Err(
                "disk_enforcement = \"quota\" but the daemon rejected a size storage-opt: \
                 native xfs project quotas are not available (docker-executor.md §6.2)"
                    .to_string(),
            ),
        },
    }
}

// ---- budget arithmetic (docker-executor.md §6.2) ------------------------

/// The enforced writable-layer budget for a job: `disk − image` (a job's usage
/// is `writable_layer + image`, §6.2).
///
/// - `disk` of zero means "no limit" — the same convention `limits.rs` uses for
///   zero/absent resource values — so there is no budget to enforce:
///   `Ok(None)` (no storage-opt, no budget label, the poller skips).
/// - `image > disk`: the image alone exceeds the request, a user error before
///   the container is ever created (§4 table).
/// - otherwise `Ok(Some(disk − image))`. A budget of exactly 0 is legal: any
///   write then kills.
///
/// The `image > disk` guard is load-bearing and must not be replaced by the
/// saturating subtraction below. `ByteSize`'s `Sub` clamps at zero, so dropping
/// the guard would quietly hand back a budget of zero — an *enforceable* budget
/// that kills the job on its first write — in place of the user error that
/// actually explains why the job cannot run.
pub(crate) fn writable_budget(
    disk: ByteSize,
    image: ByteSize,
) -> Result<Option<ByteSize>, StartError> {
    if disk.is_zero() {
        return Ok(None);
    }
    if image > disk {
        return Err(StartError::Start {
            user_error: true,
            message: format!(
                "image on-disk size ({image}) exceeds the job's disk request ({disk}); \
                 the writable-layer budget would be negative (docker-executor.md §6.2)"
            ),
        });
    }
    Ok(Some(disk - image))
}

/// Whether a container's writable-layer usage is over its budget (pure verdict,
/// the skeleton the future soft-limit ranking generalizes, §6.1). A negative or
/// absent `size_rw` (the daemon has not sized the writable layer) is never over
/// budget. A budget of zero means any positive usage is over.
///
/// `size_rw` stays a raw `Option<i64>`: it is Docker's inspect/df field as the
/// API hands it over, negatives and all, and the sign check *is* the crossing
/// into a `ByteSize`.
pub(crate) fn over_budget(size_rw: Option<i64>, budget: ByteSize) -> bool {
    match size_rw {
        Some(size) if size >= 0 => ByteSize::from_bytes(size as u64) > budget,
        _ => false,
    }
}

/// The create-time disk wiring the [`DiskEnforcer`] hands the lifecycle layer
/// (docker-executor.md §6.2). Everything strategy-specific is decided here; the
/// lifecycle layer only stamps the labels and splices in the storage-opt.
#[derive(Debug)]
pub(crate) struct DiskPlan {
    /// The [`LABEL_DISK_MODE`] value, always stamped.
    pub(crate) mode_label: &'static str,
    /// The [`LABEL_DISK_BUDGET`] value: present iff there is a budget to
    /// enforce. Absent means "no limit" (a disk request of zero) and the poller
    /// skips the container.
    ///
    /// A boundary, so a bare decimal count of bytes and never the humane
    /// `ByteSize` rendering: the label is a machine-readable record that
    /// [`sweep`] parses back after an agent restart, and the integration suite
    /// asserts on. `"16777216"` round-trips; `"16 MiB"` would not parse.
    pub(crate) budget_label: Option<String>,
    /// `HostConfig.storage_opt`: set only under [`DiskMode::Quota`] with a
    /// budget, so the daemon caps the writable layer natively.
    pub(crate) storage_opt: Option<HashMap<String, String>>,
}

// ---- the enforcer -------------------------------------------------------

/// The poll sweep's last per-allocation writable-layer reading (`SizeRw`), shared
/// with the metrics samplers (docker-executor.md §8.1: `disk_writable_bytes`
/// comes "from the disk poller's last reading").
///
/// Only the poll strategy writes this — it is the byproduct of a sweep already
/// running. Under native quotas there is no poll sweep, so the map stays empty
/// and quota-mode samplers read `None` and report `0` writable bytes: a
/// documented v1 gap (the "disk poller's last reading" only exists in poll mode).
pub(crate) type DiskReadings = Arc<Mutex<HashMap<AllocationId, u64>>>;

/// Chooses a disk-enforcement strategy at startup and produces the per-job
/// create-time wiring. Held on [`super::Inner`]; the lifecycle layer asks it for
/// a [`DiskPlan`] and knows nothing else about disk enforcement (the seam, §6.2).
pub(crate) struct DiskEnforcer {
    mode: DiskMode,
    /// The poll sweep's last writable-layer readings, shared with the metrics
    /// samplers (§8.1). Empty under native quotas (no sweep runs).
    readings: DiskReadings,
}

/// A fixed, unique name for the startup storage-opt probe container. It carries
/// no `coppice.allocation` label, so `observe`/events never touch it; it is
/// force-removed before and after the probe regardless of outcome.
const PROBE_NAME: &str = "coppice-disk-probe";

/// The writable-layer size the probe requests (docker-executor.md §6.2 example).
/// Never actually written to — the container is removed without starting — so
/// the value only has to be a well-formed storage-opt the daemon accepts.
///
/// Deliberately a string in *Docker's* size spelling rather than a
/// [`ByteSize`]: nothing here does arithmetic on it, and the one requirement is
/// that the daemon's own parser accepts it verbatim.
const PROBE_SIZE: &str = "16m";

impl DiskEnforcer {
    /// Detect the strategy honoring the configured `selector` (docker-executor.md
    /// §6.2). `poll` skips the probe entirely; `auto`/`quota` run a probe create
    /// with a `size` storage-opt (ground truth), logging the decision once at
    /// info with `docker info`'s driver + backing-filesystem for context.
    pub(crate) async fn detect(
        docker: &Docker,
        selector: DiskEnforcement,
    ) -> Result<DiskEnforcer, ExecutorError> {
        let (driver, backing) = info_driver(docker).await;

        // `poll` is an explicit override: no probe, decide directly.
        if selector == DiskEnforcement::Poll {
            let mode =
                decide_mode(selector, ProbeOutcome::Inconclusive).map_err(ExecutorError::Other)?;
            tracing::info!(
                ?mode,
                selector = ?selector,
                driver,
                backing_filesystem = backing,
                "disk enforcement forced to poll fallback (no probe run)"
            );
            return Ok(DiskEnforcer {
                mode,
                readings: DiskReadings::default(),
            });
        }

        let probe = run_probe(docker).await;
        if probe == ProbeOutcome::Inconclusive {
            // Under `quota` an inconclusive probe (typically: no local image yet
            // to probe with) proceeds as quota on the operator's word, but is
            // worth a warning — per-job creates will surface any real
            // unsupported-daemon failure as a platform StartError. Under `auto`
            // it is a benign fall back to the poll default.
            if selector == DiskEnforcement::Quota {
                tracing::warn!(
                    driver,
                    backing_filesystem = backing,
                    "disk_enforcement = \"quota\" but the storage-opt probe was inconclusive \
                     (no local image to probe with); proceeding as quota on the operator's \
                     assertion — per-job creates will surface any unsupported-daemon failure"
                );
            } else {
                tracing::info!(
                    driver,
                    backing_filesystem = backing,
                    "disk-enforcement probe was inconclusive; falling back to the poll default"
                );
            }
        }
        let mode = decide_mode(selector, probe).map_err(ExecutorError::Other)?;
        tracing::info!(
            ?mode,
            selector = ?selector,
            ?probe,
            driver,
            backing_filesystem = backing,
            "disk-enforcement strategy selected"
        );
        Ok(DiskEnforcer {
            mode,
            readings: DiskReadings::default(),
        })
    }

    /// The chosen strategy.
    pub(crate) fn mode(&self) -> DiskMode {
        self.mode
    }

    /// A clone of the shared writable-layer readings map (docker-executor.md
    /// §8.1). The metrics samplers hold their own clone and read this per tick.
    pub(crate) fn readings(&self) -> DiskReadings {
        Arc::clone(&self.readings)
    }

    /// Produce the create-time [`DiskPlan`] for a job (docker-executor.md §6.2).
    /// The image-larger-than-request check surfaces here as a user
    /// [`StartError`] before the container is created.
    ///
    /// `image_size` arrives as a bare `u64` because the caller reads it off
    /// Docker's image inspect (`Option<i64>`) and clamps it there; the crossing
    /// into a [`ByteSize`] happens on the way in here.
    pub(crate) fn plan(
        &self,
        limits: &Resources,
        image_size: ByteSize,
    ) -> Result<DiskPlan, StartError> {
        let budget = writable_budget(limits.disk, image_size)?;
        // storage-opt only under native quotas, and only when there is a budget.
        let storage_opt = match (self.mode, budget) {
            (DiskMode::Quota, Some(budget)) => {
                let mut opts = HashMap::new();
                // A bare byte count, like the budget label: `storage_opt.size`
                // is parsed by the daemon, which takes a plain integer (or its
                // own `16m`-style suffixes) and not a spaced IEC rendering.
                opts.insert("size".to_string(), budget.as_u64().to_string());
                Some(opts)
            }
            _ => None,
        };
        Ok(DiskPlan {
            mode_label: self.mode.label_value(),
            budget_label: budget.map(|budget| budget.as_u64().to_string()),
            storage_opt,
        })
    }
}

/// `docker info`'s storage driver and backing-filesystem, for the decision log
/// only (best-effort — an error yields `("<unknown>", "<unknown>")`).
async fn info_driver(docker: &Docker) -> (String, String) {
    let unknown = || "<unknown>".to_string();
    match docker.info().await {
        Ok(info) => {
            let driver = info.driver.unwrap_or_else(unknown);
            let backing = info
                .driver_status
                .and_then(|pairs| {
                    pairs
                        .into_iter()
                        .find(|pair| pair.first().map(String::as_str) == Some("Backing Filesystem"))
                        .and_then(|pair| pair.into_iter().nth(1))
                })
                .unwrap_or_else(unknown);
            (driver, backing)
        }
        Err(err) => {
            tracing::debug!(error = %err, "docker info failed during disk-enforcement probe");
            (unknown(), unknown())
        }
    }
}

/// Probe native storage-opt support: create (never start) a throwaway container
/// from any local image with a `size` storage-opt, tearing it down force either
/// way (docker-executor.md §6.2). The probe create is the ground truth.
async fn run_probe(docker: &Docker) -> ProbeOutcome {
    // Pick any locally present image; no image → inconclusive.
    let images = match docker
        .list_images(Some(ListImagesOptionsBuilder::new().build()))
        .await
    {
        Ok(images) => images,
        Err(err) => {
            tracing::debug!(error = %err, "listing images for the disk probe failed");
            return ProbeOutcome::Inconclusive;
        }
    };
    let Some(image) = images.into_iter().find_map(|image| {
        // Prefer a tag; fall back to the id so an untagged local image still works.
        image
            .repo_tags
            .into_iter()
            .find(|tag| tag != "<none>:<none>")
            .or(Some(image.id))
            .filter(|reference| !reference.is_empty())
    }) else {
        return ProbeOutcome::Inconclusive;
    };

    // Clear any stale probe from a crashed prior run, then create with the
    // storage-opt, then tear down whatever the create left behind.
    probe_remove(docker).await;
    let mut host_config = bollard::models::HostConfig::default();
    let mut storage_opt = HashMap::new();
    storage_opt.insert("size".to_string(), PROBE_SIZE.to_string());
    host_config.storage_opt = Some(storage_opt);
    let body = bollard::models::ContainerCreateBody {
        image: Some(image),
        host_config: Some(host_config),
        ..Default::default()
    };
    let outcome = match docker
        .create_container(
            Some(
                bollard::query_parameters::CreateContainerOptionsBuilder::new()
                    .name(PROBE_NAME)
                    .build(),
            ),
            body,
        )
        .await
    {
        Ok(_) => ProbeOutcome::Supported,
        Err(err) => {
            // The daemon rejects an unsupported storage-opt with a message
            // naming it ("--storage-opt is supported only for overlay over xfs
            // with 'pquota' mount option"). That, specifically, is a refutation;
            // any other failure is inconclusive (a transient/daemon error).
            if err.to_string().to_ascii_lowercase().contains("storage-opt") {
                ProbeOutcome::Unsupported
            } else {
                tracing::debug!(error = %err, "disk probe create failed for a non-storage-opt reason");
                ProbeOutcome::Inconclusive
            }
        }
    };
    probe_remove(docker).await;
    outcome
}

/// Force-remove the probe container, ignoring any error (it may not exist).
async fn probe_remove(docker: &Docker) {
    let options = RemoveContainerOptions {
        v: true,
        force: true,
        link: false,
    };
    let _ = docker.remove_container(PROBE_NAME, Some(options)).await;
}

/// Whether the poll task must run: always under [`DiskMode::Poll`], and under
/// [`DiskMode::Quota`] whenever containers created under the poll strategy
/// survived an agent restart (their `coppice.disk-mode = poll` label is the
/// resume contract, §5/§6.2). A mode flip across restart is real: an `auto`
/// probe that was inconclusive at first boot (no local image yet) can resolve
/// to quota once images exist — but the recovered poll containers have no
/// native quota, so dropping the poller would silently stop enforcing them.
/// Pure, so the matrix is unit-testable.
pub(crate) fn poller_required(mode: DiskMode, recovered_poll_containers: bool) -> bool {
    mode == DiskMode::Poll || recovered_poll_containers
}

/// Whether any *running* container on the daemon carries this node's
/// `coppice.node` label and `coppice.disk-mode = poll` — the recovery input to
/// [`poller_required`]. Exited poll containers don't matter here: their
/// writable layer can no longer grow, and enforcement ends at exit.
pub(crate) async fn has_recovered_poll_containers(
    docker: &Docker,
    node: coppice_core::id::NodeId,
) -> Result<bool, ExecutorError> {
    let mut filters = HashMap::new();
    filters.insert(
        "label".to_string(),
        vec![
            format!("{LABEL_DISK_MODE}={}", DiskMode::Poll.label_value()),
            format!("{LABEL_NODE}={node}"),
        ],
    );
    let options = ListContainersOptionsBuilder::new()
        .filters(&filters)
        .build();
    let summaries = docker
        .list_containers(Some(options))
        .await
        .map_err(|err| ExecutorError::Other(format!("listing poll-mode containers: {err}")))?;
    Ok(!summaries.is_empty())
}

// ---- poll task (docker-executor.md §6.2, §11) ---------------------------

/// `None` inspect options.
const INSPECT_OPTS: Option<bollard::query_parameters::InspectContainerOptions> = None;

/// Spawn the poll-fallback disk enforcer, returning its handle (aborted on
/// [`super::Inner`] drop). Captures only clones — never an `Arc<Inner>` — so the
/// abort is what actually stops it, mirroring [`super::events::spawn`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    docker: Docker,
    state: Arc<Mutex<ExecutorState>>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
    interval: StdDuration,
    node: coppice_core::id::NodeId,
    readings: DiskReadings,
) -> JoinHandle<()> {
    tokio::spawn(run(
        docker, state, cpuset, exit_tx, interval, node, readings,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run(
    docker: Docker,
    state: Arc<Mutex<ExecutorState>>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
    interval: StdDuration,
    node: coppice_core::id::NodeId,
    readings: DiskReadings,
) {
    let node = node.to_string();
    // A floor, not a deadline: the interval is measured from the *end* of the
    // prior sweep (Delay), and the whole sweep is awaited before the next tick,
    // so sweeps never overlap (docker-executor.md §6.2).
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if exit_tx.is_closed() {
            return;
        }
        let started = std::time::Instant::now();
        if let Err(err) = sweep(&docker, &state, &cpuset, &exit_tx, &node, &readings).await {
            tracing::warn!(error = %err, "disk poll sweep failed; retrying next interval");
        }
        metrics::histogram!(AGENT_DISK_POLL_DURATION).record(started.elapsed().as_secs_f64());
    }
}

/// One `GET /system/df` sweep: for every container carrying
/// `coppice.disk-mode = poll` and *this node's* `coppice.node` label, with a
/// parseable budget label, whose `SizeRw` is over budget, recheck with a
/// single-container inspect and, if still over, kill it (docker-executor.md
/// §6.2). The node filter keeps the kill authority scoped to containers this
/// executor owns — another agent's containers on a shared daemon (in practice:
/// the concurrent integration tests) are never this enforcer's to kill.
async fn sweep(
    docker: &Docker,
    state: &Mutex<ExecutorState>,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: &mpsc::UnboundedSender<ExitEvent>,
    node: &str,
    readings: &DiskReadings,
) -> Result<(), bollard::errors::Error> {
    let usage = docker.df(None::<DataUsageOptions>).await?;
    let containers = usage.containers.unwrap_or_default();
    // Rebuilt from scratch each sweep and swapped in at the end, so a departed
    // container's stale reading never lingers for the samplers (§8.1).
    let mut fresh: HashMap<AllocationId, u64> = HashMap::new();
    for summary in containers {
        let labels = summary.labels.as_ref();
        // Only our poll-enforced containers with a parseable budget.
        let is_poll = labels
            .and_then(|labels| labels.get(LABEL_DISK_MODE))
            .is_some_and(|mode| mode == DiskMode::Poll.label_value());
        let is_ours = labels
            .and_then(|labels| labels.get(LABEL_NODE))
            .is_some_and(|owner| owner == node);
        if !is_ours {
            continue;
        }
        let allocation = labels
            .and_then(|labels| labels.get(super::LABEL_ALLOCATION))
            .and_then(|raw| raw.parse::<AllocationId>().ok());
        // Record this container's writable-layer reading for the metrics samplers
        // (§8.1: `disk_writable_bytes` is "the disk poller's last reading"). A
        // negative/absent `SizeRw` reads as 0.
        if is_poll {
            if let Some(allocation) = allocation {
                let size = summary.size_rw.filter(|&rw| rw >= 0).unwrap_or(0) as u64;
                fresh.insert(allocation, size);
            }
        }
        // The label is the bare byte count `plan` stamped (see
        // `DiskPlan::budget_label`), so it parses as a `u64` and becomes a
        // `ByteSize` here — the one crossing on the way back in.
        let budget = labels
            .and_then(|labels| labels.get(LABEL_DISK_BUDGET))
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(ByteSize::from_bytes);
        let (Some(budget), true) = (budget, is_poll) else {
            continue;
        };
        if !over_budget(summary.size_rw, budget) {
            continue;
        }
        let Some(allocation) = allocation else {
            continue; // over budget but unidentifiable — not ours to kill
        };
        let target = summary.id.as_deref().unwrap_or_default();
        if target.is_empty() {
            continue;
        }

        // Recheck with the single-container ground truth before any verdict:
        // df's SizeRw can lag, and a kill is irreversible.
        let recheck = docker
            .inspect_container(
                target,
                Some(InspectContainerOptionsBuilder::new().size(true).build()),
            )
            .await;
        let inspect = match recheck {
            Ok(inspect) => inspect,
            Err(err) if super::api::status_code(&err) == Some(404) => continue, // vanished
            Err(err) => {
                tracing::warn!(%allocation, error = %err, "disk recheck inspect failed; skipping this sweep");
                continue;
            }
        };
        if !over_budget(inspect.size_rw, budget) {
            continue; // df was stale; not actually over budget
        }

        kill_over_budget(docker, state, cpuset, exit_tx, allocation, target, budget).await;
    }
    // Replace the shared readings wholesale (§8.1): full rebuild, so departed
    // containers drop out and each sampler sees only live per-allocation sizes.
    *readings.lock().unwrap() = fresh;
    Ok(())
}

/// The kill verdict path (docker-executor.md §6.2), in the exact order that
/// avoids racing the events task: claim the exit, kill outright, gather exit
/// evidence, then run the same cleanup a claimed exit does in `events.rs` and
/// send the [`ExitEvent`]. The container is left in place as evidence until the
/// session reaps it (§5).
async fn kill_over_budget(
    docker: &Docker,
    state: &Mutex<ExecutorState>,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: &mpsc::UnboundedSender<ExitEvent>,
    allocation: AllocationId,
    target: &str,
    budget: ByteSize,
) {
    // 1. Claim under the lock. If already claimed, someone else owns this exit
    //    (the events task, a stop, or a prior sweep) — leave it entirely.
    {
        let mut st = lock_state(state);
        if st.claimed.contains(&allocation) {
            return;
        }
        st.claimed.insert(allocation);
        // Stop this container's sampler and start its drain clock (§8.2).
        st.note_exit_claimed(allocation, Timestamp::now());
    }

    // 2. Kill outright — SIGKILL, no pause-first. A 404/409/"not running" means
    //    the container had already exited on its own: our kill was a no-op, but
    //    we still hold the claim and must surface the exit ourselves below.
    let killed_by_us = match docker
        .kill_container(
            target,
            Some(KillContainerOptionsBuilder::new().signal("SIGKILL").build()),
        )
        .await
    {
        Ok(()) => true,
        Err(err) => {
            let already_gone = matches!(super::api::status_code(&err), Some(404) | Some(409));
            if !already_gone {
                // Couldn't kill for a real reason — un-claim so a later sweep,
                // die event, or stop can still surface this exit.
                tracing::warn!(%allocation, error = %err, "disk kill failed; un-claiming for later retry");
                lock_state(state).claimed.remove(&allocation);
                return;
            }
            false
        }
    };

    // 3. Gather exit evidence. Our SIGKILL is asynchronous, so poll inspect
    //    briefly for the terminal state (reusing classify::exit_info). If our
    //    kill took effect the cause is DiskKilled; if the container had already
    //    exited on its own we keep the naturally-classified cause — never drop a
    //    claimed exit.
    let Some(mut info) = wait_exit_evidence(docker, target).await else {
        // No usable evidence after the kill (a torn inspect / vanished
        // container): un-claim so a resync can recover it rather than fabricate.
        tracing::warn!(%allocation, "disk kill left no usable exit evidence; un-claiming for later resync");
        lock_state(state).claimed.remove(&allocation);
        return;
    };
    if killed_by_us {
        info.cause = ExitCause::DiskKilled;
        metrics::counter!(AGENT_LIMIT_KILLS_TOTAL, "kind" => KIND_DISK).increment(1);
        tracing::info!(
            %allocation,
            // `%`, not a bare field: `ByteSize` is not a `tracing` primitive,
            // and recording it through `Display` is the point — the reader of
            // this line wants "8 MiB", not eight digits to count.
            %budget,
            exit_code = info.code,
            "killed container for exceeding its disk budget"
        );
    }

    // 4. The same cleanup a claimed exit does in events.rs: drop from running,
    //    push the gauge, grow the fractional cpuset, then send the exit.
    {
        let mut st = lock_state(state);
        st.running.remove(&allocation);
        st.push_running_gauge();
    }
    if let Err(err) = super::release_cpu(docker, cpuset, allocation).await {
        tracing::warn!(%allocation, error = %err, "failed to grow fractional cpuset after disk kill");
    }
    let _ = exit_tx.send(ExitEvent {
        allocation,
        exit: info,
    });
}

/// Poll `inspect` for terminal exit evidence after a kill, bounded so a wedged
/// container cannot stall the sweep. Returns the first usable
/// [`crate::executor::ExitInfo`], or `None` if none appears in time.
async fn wait_exit_evidence(docker: &Docker, target: &str) -> Option<crate::executor::ExitInfo> {
    const ATTEMPTS: usize = 30;
    const STEP: StdDuration = StdDuration::from_millis(100);
    for attempt in 0..ATTEMPTS {
        match docker.inspect_container(target, INSPECT_OPTS).await {
            Ok(inspect) => {
                if let Some(info) = inspect.state.as_ref().and_then(classify::exit_info) {
                    return Some(info);
                }
            }
            Err(err) if super::api::status_code(&err) == Some(404) => return None,
            Err(_) => {} // torn read — retry
        }
        if attempt + 1 < ATTEMPTS {
            tokio::time::sleep(STEP).await;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- writable_budget (docker-executor.md §6.2) -------------------------

    /// A size written as a plain byte count, for the arithmetic cases where the
    /// small numbers are the point and a unit would only obscure them.
    fn bytes(count: u64) -> ByteSize {
        ByteSize::from_bytes(count)
    }

    #[test]
    fn budget_subtracts_image_size() {
        let budget = writable_budget(bytes(100), bytes(30)).expect("under budget");
        assert_eq!(budget, Some(bytes(70)));
    }

    #[test]
    fn zero_disk_request_means_no_enforcement() {
        // Mirrors limits.rs: zero is "no limit", not "budget of zero".
        assert_eq!(
            writable_budget(ByteSize::ZERO, ByteSize::ZERO).unwrap(),
            None
        );
        assert_eq!(writable_budget(ByteSize::ZERO, bytes(12345)).unwrap(), None);
    }

    #[test]
    fn image_equal_to_request_is_a_legal_zero_budget() {
        // A budget of exactly 0 is legal: any write then kills.
        assert_eq!(
            writable_budget(bytes(100), bytes(100)).unwrap(),
            Some(ByteSize::ZERO)
        );
    }

    #[test]
    fn image_larger_than_request_is_a_user_error() {
        // Not the saturating-subtraction outcome: an image over the request is
        // a user error, never a silently-zero budget.
        let err = writable_budget(bytes(100), bytes(101)).unwrap_err();
        assert!(matches!(
            err,
            StartError::Start {
                user_error: true,
                ..
            }
        ));
    }

    #[test]
    fn image_too_big_error_names_both_sizes_humanely() {
        let err = writable_budget(ByteSize::from_mib(8), ByteSize::from_mib(16)).unwrap_err();
        let StartError::Start { message, .. } = err else {
            panic!("expected a start error");
        };
        assert!(
            message.contains("(16 MiB)") && message.contains("(8 MiB)"),
            "message should render both sizes in IEC units: {message}"
        );
    }

    // ---- over_budget verdict -----------------------------------------------

    #[test]
    fn over_budget_strictly_above() {
        assert!(!over_budget(Some(100), bytes(100))); // at the budget is not over
        assert!(over_budget(Some(101), bytes(100)));
        assert!(!over_budget(Some(0), bytes(100)));
    }

    #[test]
    fn zero_budget_kills_on_any_write() {
        assert!(!over_budget(Some(0), ByteSize::ZERO));
        assert!(over_budget(Some(1), ByteSize::ZERO));
    }

    #[test]
    fn absent_or_negative_size_is_never_over_budget() {
        assert!(!over_budget(None, ByteSize::ZERO));
        assert!(!over_budget(Some(-1), ByteSize::ZERO));
    }

    // ---- decide_mode matrix (docker-executor.md §6.2) ----------------------

    #[test]
    fn poll_selector_ignores_the_probe() {
        for probe in [
            ProbeOutcome::Supported,
            ProbeOutcome::Unsupported,
            ProbeOutcome::Inconclusive,
        ] {
            assert_eq!(
                decide_mode(DiskEnforcement::Poll, probe).unwrap(),
                DiskMode::Poll
            );
        }
    }

    #[test]
    fn auto_picks_quota_only_when_supported() {
        assert_eq!(
            decide_mode(DiskEnforcement::Auto, ProbeOutcome::Supported).unwrap(),
            DiskMode::Quota
        );
        assert_eq!(
            decide_mode(DiskEnforcement::Auto, ProbeOutcome::Unsupported).unwrap(),
            DiskMode::Poll
        );
        assert_eq!(
            decide_mode(DiskEnforcement::Auto, ProbeOutcome::Inconclusive).unwrap(),
            DiskMode::Poll
        );
    }

    #[test]
    fn quota_assertion_fails_only_when_refuted() {
        assert_eq!(
            decide_mode(DiskEnforcement::Quota, ProbeOutcome::Supported).unwrap(),
            DiskMode::Quota
        );
        // Inconclusive (no image to probe): proceed as quota, warn elsewhere.
        assert_eq!(
            decide_mode(DiskEnforcement::Quota, ProbeOutcome::Inconclusive).unwrap(),
            DiskMode::Quota
        );
        // Refuted: a hard startup error.
        assert!(decide_mode(DiskEnforcement::Quota, ProbeOutcome::Unsupported).is_err());
    }

    // ---- poller_required (§6.2 restart mode-flip) ---------------------------

    #[test]
    fn poller_runs_for_recovered_poll_containers_even_under_quota() {
        // Poll mode always polls; quota mode polls only when poll-labelled
        // containers survived a restart across a mode flip.
        assert!(poller_required(DiskMode::Poll, false));
        assert!(poller_required(DiskMode::Poll, true));
        assert!(!poller_required(DiskMode::Quota, false));
        assert!(poller_required(DiskMode::Quota, true));
    }

    // ---- plan / budget-label round-trip ------------------------------------

    fn resources(disk: ByteSize) -> Resources {
        Resources {
            cpu_millis: 0,
            memory: ByteSize::ZERO,
            disk,
        }
    }

    /// A test enforcer pinned to `mode` with an empty readings map — `plan` never
    /// touches the readings, so the map's contents are irrelevant to these tests.
    fn enforcer(mode: DiskMode) -> DiskEnforcer {
        DiskEnforcer {
            mode,
            readings: DiskReadings::default(),
        }
    }

    #[test]
    fn quota_plan_sets_storage_opt_and_budget_label() {
        let enforcer = enforcer(DiskMode::Quota);
        let plan = enforcer
            .plan(&resources(bytes(100)), bytes(30))
            .expect("under budget");
        assert_eq!(plan.mode_label, "quota");
        // Budget label round-trips back to the enforced budget: a bare decimal
        // byte count, which is what `sweep` parses after a restart.
        let parsed: u64 = plan
            .budget_label
            .as_deref()
            .expect("budget present")
            .parse()
            .expect("decimal budget");
        assert_eq!(ByteSize::from_bytes(parsed), bytes(70));
        assert_eq!(
            plan.storage_opt
                .expect("quota sets storage-opt")
                .get("size"),
            Some(&"70".to_string())
        );
    }

    #[test]
    fn poll_plan_stamps_budget_but_no_storage_opt() {
        let enforcer = enforcer(DiskMode::Poll);
        let plan = enforcer
            .plan(&resources(bytes(100)), bytes(30))
            .expect("under budget");
        assert_eq!(plan.mode_label, "poll");
        assert_eq!(plan.budget_label.as_deref(), Some("70"));
        assert!(plan.storage_opt.is_none(), "poll never sets a storage-opt");
    }

    #[test]
    fn no_limit_plan_omits_budget_and_storage_opt() {
        for mode in [DiskMode::Quota, DiskMode::Poll] {
            let enforcer = enforcer(mode);
            // A disk request of zero → no enforcement in either mode.
            let plan = enforcer
                .plan(&resources(ByteSize::ZERO), bytes(12345))
                .expect("no limit");
            assert_eq!(plan.mode_label, mode.label_value());
            assert!(
                plan.budget_label.is_none(),
                "{mode:?} label omitted at no-limit"
            );
            assert!(
                plan.storage_opt.is_none(),
                "{mode:?} storage-opt omitted at no-limit"
            );
        }
    }

    #[test]
    fn plan_propagates_the_image_too_big_user_error() {
        let enforcer = enforcer(DiskMode::Quota);
        let err = enforcer
            .plan(&resources(bytes(100)), bytes(200))
            .unwrap_err();
        assert!(matches!(
            err,
            StartError::Start {
                user_error: true,
                ..
            }
        ));
    }
}
