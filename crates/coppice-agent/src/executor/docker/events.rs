//! The docker-events task (docker-executor.md §11, S2 item 6).
//!
//! A single long-lived task live-tails `docker events` for container `die`
//! events on labeled containers, turns each into an [`ExitEvent`] on the
//! natural-exit channel that feeds [`crate::executor::Executor::next_exit`], and
//! resyncs against the daemon on every (re)subscribe — after priming the lazy
//! stream so the tail is up before the snapshot — plus a low-frequency periodic
//! sweep, so a stream gap can never swallow an exit for long. It is aborted on [`super::Inner`] drop, so it captures only
//! clones (`docker`, the shared state, the sender) — never an `Arc<Inner>`.
//!
//! Every resync — including the first, at construction — enqueues every
//! unclaimed exit it finds. Pre-existing exits are therefore delivered through
//! `next_exit` *in addition to* appearing in restart recovery's `ObservedSet`:
//! that duplication is deliberate and load-bearing. Recovery only *reports* a
//! runtime-observed exit (`on_register_accepted` journals nothing and reaps
//! only already-journaled exits); the `next_exit` delivery is what drives
//! `handle_observed_exit` to journal the exit and reap the container. Claiming
//! without enqueueing here would strand such an exit unjournaled forever. The
//! session's exit handling is idempotent on allocation, so the double surface
//! is safe (§4's backstop).
//!
//! The std `Mutex` on the shared state is never held across an await.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use bollard::models::EventMessageTypeEnum;
use bollard::query_parameters::{EventsOptionsBuilder, ListContainersOptionsBuilder};
use bollard::Docker;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use coppice_core::id::AllocationId;
use coppice_core::time::Timestamp;

use super::{classify, cpuset, lock_state, ExecutorState, LABEL_ALLOCATION};
use crate::executor::ExitEvent;

/// Backoff between an events-stream error/end and the reconnect-and-resync
/// (docker-executor.md §11 step 4). A short const — the resync is the real
/// safety net, this only avoids a hot loop against a flapping daemon.
const RECONNECT_BACKOFF: StdDuration = StdDuration::from_secs(1);

/// How long to poll the freshly-built events stream before the resync
/// snapshot. bollard's stream is lazy — the HTTP request is not sent until the
/// first poll — so without this an exit landing between the resync's
/// `list_containers` and the loop's first real poll would be in neither the
/// snapshot nor the tail. Generous against a ~ms local connection setup; the
/// periodic sweep covers a daemon slower than this.
const SUBSCRIBE_PRIME: StdDuration = StdDuration::from_millis(250);

/// Period of the steady-state resync sweep. Priming cannot *prove* the daemon
/// registered the subscription before the snapshot was taken, so a
/// low-frequency sweep bounds how long any exit that slipped between them can
/// stay unjournaled — cheap (one filtered list; claimed allocations are
/// skipped before any inspect) and unconditional.
const RESYNC_INTERVAL: StdDuration = StdDuration::from_secs(60);

/// `None` inspect options.
const INSPECT_OPTS: Option<bollard::query_parameters::InspectContainerOptions> = None;

/// Spawn the events task, returning its handle (aborted on [`super::Inner`]
/// drop). Captures only clones so the abort is what actually stops it.
pub(crate) fn spawn(
    docker: Docker,
    state: Arc<Mutex<ExecutorState>>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
) -> JoinHandle<()> {
    tokio::spawn(run(docker, state, cpuset, exit_tx))
}

async fn run(
    docker: Docker,
    state: Arc<Mutex<ExecutorState>>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
) {
    loop {
        // 1. Subscribe (live tail; no `since` — gaps are covered by the resync).
        let mut filters = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        filters.insert("event".to_string(), vec!["die".to_string()]);
        filters.insert("label".to_string(), vec![LABEL_ALLOCATION.to_string()]);
        let options = EventsOptionsBuilder::new().filters(&filters).build();
        let mut stream = Box::pin(docker.events(Some(options)));

        // 2. Prime the subscription. The stream is lazy (see SUBSCRIBE_PRIME):
        //    poll it for a beat so the live tail is established *before* the
        //    resync snapshot — the ordering the no-gap argument rests on. An
        //    event arriving this early is handled, never dropped.
        match tokio::time::timeout(SUBSCRIBE_PRIME, stream.next()).await {
            Err(_elapsed) => {} // no event during the prime — the normal case
            Ok(Some(Ok(event))) => handle_die(&docker, &state, &cpuset, &exit_tx, &event).await,
            Ok(Some(Err(err))) => {
                tracing::warn!(error = %err, "docker events stream error; reconnecting");
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
            Ok(None) => {
                tracing::warn!("docker events stream ended; reconnecting");
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
        }

        // 3. Resync immediately after the subscription is up, so exits that
        //    predate the stream (or fell into a gap) are surfaced through
        //    `next_exit` and reach the session's journaling path.
        if let Err(err) = resync(&docker, &state, &cpuset, &exit_tx).await {
            tracing::warn!(error = %err, "events resync failed; relying on later observe/resync");
        }

        // 4. Per die event, with the periodic sweep as backstop (see
        //    RESYNC_INTERVAL). The interval starts one full period out — step 3
        //    just resynced.
        let mut sweep = tokio::time::interval_at(
            tokio::time::Instant::now() + RESYNC_INTERVAL,
            RESYNC_INTERVAL,
        );
        loop {
            tokio::select! {
                item = stream.next() => match item {
                    Some(Ok(event)) => handle_die(&docker, &state, &cpuset, &exit_tx, &event).await,
                    Some(Err(err)) => {
                        tracing::warn!(error = %err, "docker events stream error; reconnecting");
                        break;
                    }
                    None => {
                        tracing::warn!("docker events stream ended; reconnecting");
                        break;
                    }
                },
                _ = sweep.tick() => {
                    if let Err(err) = resync(&docker, &state, &cpuset, &exit_tx).await {
                        tracing::warn!(error = %err, "periodic events resync failed; retrying next sweep");
                    }
                }
            }
        }

        // 5. Backoff, then reconnect at step 1.
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

/// List all exited/dead labeled containers and, for each with usable evidence
/// whose allocation is not already claimed, claim it and enqueue an
/// [`ExitEvent`]. Unconditional on purpose (see the module docs): an exit that
/// never flows through `next_exit` is never journaled by the session, so
/// suppressing the enqueue would strand its evidence forever. Duplicates are
/// bounded by the claim set here and the session's idempotency above.
async fn resync(
    docker: &Docker,
    state: &Mutex<ExecutorState>,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: &mpsc::UnboundedSender<ExitEvent>,
) -> Result<(), bollard::errors::Error> {
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec![LABEL_ALLOCATION.to_string()]);
    filters.insert(
        "status".to_string(),
        vec!["exited".to_string(), "dead".to_string()],
    );
    let options = ListContainersOptionsBuilder::new()
        .all(true)
        .filters(&filters)
        .build();
    let summaries = docker.list_containers(Some(options)).await?;

    for summary in summaries {
        let Some(allocation) = summary
            .labels
            .as_ref()
            .and_then(|labels| labels.get(LABEL_ALLOCATION))
            .and_then(|raw| raw.parse::<AllocationId>().ok())
        else {
            continue;
        };
        // Cheap pre-check to skip already-claimed ones before inspecting.
        if lock_state(state).claimed.contains(&allocation) {
            continue;
        }
        let target = summary.id.as_deref().unwrap_or_default();
        if target.is_empty() {
            continue;
        }
        let info = match docker.inspect_container(target, INSPECT_OPTS).await {
            Ok(inspect) => {
                // Bounded settle for a lagging OOMKilled commit (issue #34).
                let inspect = super::settle_oom_flag(docker, target, inspect).await;
                inspect.state.as_ref().and_then(classify::exit_info)
            }
            Err(_) => None, // vanished or torn — a later resync/stop can surface it
        };
        let Some(info) = info else {
            continue;
        };

        // Claim atomically (re-check under the lock: a die event may have raced
        // us since the pre-check).
        let enqueue = {
            let mut st = lock_state(state);
            if st.claimed.contains(&allocation) {
                false
            } else {
                st.claimed.insert(allocation);
                // Stop this container's sampler and start its drain clock (§8.2).
                st.note_exit_claimed(allocation, Timestamp::now());
                st.running.remove(&allocation);
                st.push_running_gauge();
                true
            }
        };
        if enqueue {
            if let Err(err) = super::release_cpu(docker, cpuset, allocation).await {
                tracing::warn!(%allocation, error = %err, "failed to grow fractional cpuset after exit");
            }
            let _ = exit_tx.send(ExitEvent {
                allocation,
                exit: info,
            });
        }
    }
    Ok(())
}

/// Turn one `die` event into an exit: parse the allocation from the actor's
/// `coppice.allocation` attribute, claim it (skip duplicates, §4), inspect for
/// evidence, and enqueue — un-claiming on an unusable/failed inspect so a later
/// resync or stop can still surface it.
async fn handle_die(
    docker: &Docker,
    state: &Mutex<ExecutorState>,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: &mpsc::UnboundedSender<ExitEvent>,
    event: &bollard::models::EventMessage,
) {
    // Defensive: the subscribe filters to container events, but re-check.
    if event.typ != Some(EventMessageTypeEnum::CONTAINER) {
        return;
    }
    let actor = match event.actor.as_ref() {
        Some(actor) => actor,
        None => return,
    };
    let Some(allocation) = actor
        .attributes
        .as_ref()
        .and_then(|attrs| attrs.get(LABEL_ALLOCATION))
        .and_then(|raw| raw.parse::<AllocationId>().ok())
    else {
        return; // missing/foreign allocation attribute — skip
    };

    // Claim first; a re-delivery of an already-claimed exit is suppressed (§4).
    let newly_claimed = {
        let mut st = lock_state(state);
        if st.claimed.contains(&allocation) {
            false
        } else {
            st.claimed.insert(allocation);
            // Stop this container's sampler and start its drain clock (§8.2).
            st.note_exit_claimed(allocation, Timestamp::now());
            true
        }
    };
    if !newly_claimed {
        return;
    }

    // Inspect the container (by actor id) for terminal evidence. The inspect
    // races the daemon's own OOMKilled commit, which can land *after* the die
    // event — settle it before extracting evidence (issue #34).
    let info = match actor.id.as_deref() {
        Some(id) => match docker.inspect_container(id, INSPECT_OPTS).await {
            Ok(inspect) => {
                let inspect = super::settle_oom_flag(docker, id, inspect).await;
                inspect.state.as_ref().and_then(classify::exit_info)
            }
            Err(_) => None,
        },
        None => None,
    };

    match info {
        Some(exit) => {
            {
                let mut st = lock_state(state);
                st.running.remove(&allocation);
                st.push_running_gauge();
            }
            if let Err(err) = super::release_cpu(docker, cpuset, allocation).await {
                tracing::warn!(%allocation, error = %err, "failed to grow fractional cpuset after exit");
            }
            let _ = exit_tx.send(ExitEvent { allocation, exit });
        }
        None => {
            // Inspect failed or unusable: un-claim so a later resync or stop can
            // still surface this exit.
            tracing::warn!(
                %allocation,
                "die event without usable exit evidence; un-claiming for later resync"
            );
            lock_state(state).claimed.remove(&allocation);
        }
    }
}
