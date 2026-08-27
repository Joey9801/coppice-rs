//! The docker-events task (docker-executor.md §11, S2 item 6).
//!
//! A single long-lived task live-tails `docker events` for container `die` and
//! `oom` events on labeled containers, turns each `die` into an [`ExitEvent`]
//! on the natural-exit channel that feeds
//! [`crate::executor::Executor::next_exit`], records each `oom` in the witness
//! registry that [`super::settle_oom_flag`] consults (§4), and
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

use coppice_core::id::{AllocationId, NodeId};
use coppice_core::time::Timestamp;

use super::{classify, cpuset, lock_state, oom, ExecutorState, LABEL_ALLOCATION, LABEL_NODE};
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
    node: NodeId,
    state: Arc<Mutex<ExecutorState>>,
    witness: Arc<oom::OomWitness>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
) -> JoinHandle<()> {
    tokio::spawn(run(docker, node, state, witness, cpuset, exit_tx))
}

/// One unit of work for the settle task, produced by the pump.
enum Work {
    /// A `die` event to turn into an [`ExitEvent`].
    Die(Box<bollard::models::EventMessage>),
    /// Re-snapshot the daemon: on every (re)subscribe, and per sweep.
    Resync,
}

/// Abort a task when this guard drops. The pump is a child of the settle task,
/// and [`super::Inner`]'s drop aborts only the handle it holds — without this
/// the pump would outlive the executor that owns it.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The settle half: drains [`Work`] and does everything that can block —
/// inspects, OOM settles, resync sweeps.
///
/// It is deliberately *not* the half that reads the event stream. Settling an
/// OOM means waiting for a witness that only [`pump`] can record, so a settle
/// running on the pump's own task could never observe the event it waits for:
/// an `oom` arriving after its `die`, or one buffered during a resync, would
/// sit unread in the stream until the settle had already given up and reported
/// the exit as cause-unconfirmed. Splitting the two is what makes the witness
/// wait mean anything — the pump keeps recording while a settle is parked.
///
/// Work is drained serially, so a settle still delays *other* exits; that is
/// bounded by the give-up memo and the kill-in-flight marker (§4), and exits
/// queue in the channel rather than being lost.
async fn run(
    docker: Docker,
    node: NodeId,
    state: Arc<Mutex<ExecutorState>>,
    witness: Arc<oom::OomWitness>,
    cpuset: Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: mpsc::UnboundedSender<ExitEvent>,
) {
    let (work_tx, mut work_rx) = mpsc::unbounded_channel();
    // Held for its drop effect: aborting this task also stops the pump.
    let _pump = AbortOnDrop(tokio::spawn(pump(
        docker.clone(),
        node,
        Arc::clone(&witness),
        work_tx,
    )));

    // Ends when the pump drops its sender — i.e. only when the pump itself
    // stops, which it does not do short of being aborted.
    while let Some(work) = work_rx.recv().await {
        match work {
            Work::Die(event) => {
                handle_die(&docker, &state, &witness, &cpuset, &exit_tx, &event).await
            }
            Work::Resync => {
                if let Err(err) = resync(&docker, node, &state, &witness, &cpuset, &exit_tx).await {
                    tracing::warn!(error = %err, "events resync failed; relying on later observe/resync");
                }
            }
        }
    }
}

/// The ingestion half: owns the events stream and never awaits anything slow.
///
/// `oom` events are recorded inline (a map insert and a notify — microseconds);
/// `die` events and resync requests are handed to [`run`]. Because nothing here
/// blocks, the tail stays live for as long as the subscription does, which is
/// the property the OOM witness depends on.
async fn pump(
    docker: Docker,
    node: NodeId,
    witness: Arc<oom::OomWitness>,
    work_tx: mpsc::UnboundedSender<Work>,
) {
    loop {
        // 1. Subscribe (live tail; no `since` — gaps are covered by the resync).
        let mut filters = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        // `oom` rides the same subscription as `die`, and on a healthy daemon
        // it arrives *first* (§4): both are published from the daemon's
        // container-event path, and the OOM notification is handled before the
        // task-exit one. Tailing it is what turns the OOM settle from polling
        // for a flag commit into waiting on the daemon's own signal.
        filters.insert(
            "event".to_string(),
            vec!["die".to_string(), "oom".to_string()],
        );
        // Scoped to this node: another agent's containers on a shared daemon
        // publish the same events, and their exits are not ours to journal.
        filters.insert(
            "label".to_string(),
            vec![LABEL_ALLOCATION.to_string(), format!("{LABEL_NODE}={node}")],
        );
        let options = EventsOptionsBuilder::new().filters(&filters).build();
        let mut stream = Box::pin(docker.events(Some(options)));

        // 2. Prime the subscription. The stream is lazy (see SUBSCRIBE_PRIME):
        //    poll it for a beat so the live tail is established *before* the
        //    resync snapshot — the ordering the no-gap argument rests on. An
        //    event arriving this early is handled, never dropped.
        match tokio::time::timeout(SUBSCRIBE_PRIME, stream.next()).await {
            Err(_elapsed) => {} // no event during the prime — the normal case
            Ok(Some(Ok(event))) => {
                if dispatch(&witness, &work_tx, event).is_err() {
                    return;
                }
            }
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

        // 3. Ask for a resync now the subscription is up, so exits that predate
        //    the stream (or fell into a gap) are surfaced through `next_exit`
        //    and reach the session's journaling path. The settle task runs it
        //    while this loop keeps draining the tail — so an `oom` published
        //    during the snapshot is recorded, not stranded behind it.
        if work_tx.send(Work::Resync).is_err() {
            return;
        }

        // 4. Per event, with the periodic sweep as backstop (see
        //    RESYNC_INTERVAL). The interval starts one full period out — step 3
        //    just asked for a resync.
        let mut sweep = tokio::time::interval_at(
            tokio::time::Instant::now() + RESYNC_INTERVAL,
            RESYNC_INTERVAL,
        );
        loop {
            tokio::select! {
                item = stream.next() => match item {
                    Some(Ok(event)) => {
                        if dispatch(&witness, &work_tx, event).is_err() {
                            return;
                        }
                    }
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
                    if work_tx.send(Work::Resync).is_err() {
                        return;
                    }
                }
            }
        }

        // 5. Backoff, then reconnect at step 1.
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

/// Route one container event off the stream: `oom` is recorded here and now,
/// `die` is queued for the settle task. `Err` means that task is gone and the
/// pump should stop.
fn dispatch(
    witness: &oom::OomWitness,
    work_tx: &mpsc::UnboundedSender<Work>,
    event: bollard::models::EventMessage,
) -> Result<(), ()> {
    match event.action.as_deref() {
        Some("oom") => {
            handle_oom(witness, &event);
            Ok(())
        }
        Some("die") => work_tx.send(Work::Die(Box::new(event))).map_err(|_| ()),
        // The subscription filters to these two; anything else is the daemon
        // widening a filter under us, and is not ours to interpret.
        _ => Ok(()),
    }
}

/// List this node's exited/dead labeled containers and, for each with usable evidence
/// whose allocation is not already claimed, claim it and enqueue an
/// [`ExitEvent`]. Unconditional on purpose (see the module docs): an exit that
/// never flows through `next_exit` is never journaled by the session, so
/// suppressing the enqueue would strand its evidence forever. Duplicates are
/// bounded by the claim set here and the session's idempotency above.
async fn resync(
    docker: &Docker,
    node: NodeId,
    state: &Mutex<ExecutorState>,
    witness: &oom::OomWitness,
    cpuset: &Option<Arc<AsyncMutex<cpuset::Allocator>>>,
    exit_tx: &mpsc::UnboundedSender<ExitEvent>,
) -> Result<(), bollard::errors::Error> {
    let mut filters = HashMap::new();
    filters.insert(
        "label".to_string(),
        vec![LABEL_ALLOCATION.to_string(), format!("{LABEL_NODE}={node}")],
    );
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
        // The listing filtered on exited/dead status, so this container is
        // proven dead regardless of who holds the claim: fire the follower's
        // fast drain (§8.2) — it also backstops a disk kill whose evidence
        // gathering failed before it could fire. Then the cheap pre-check to
        // skip already-claimed ones before inspecting.
        {
            let mut st = lock_state(state);
            st.note_container_dead(allocation);
            if st.claimed.contains(&allocation) {
                continue;
            }
        }
        let target = summary.id.as_deref().unwrap_or_default();
        if target.is_empty() {
            continue;
        }
        let info = match docker.inspect_container(target, INSPECT_OPTS).await {
            Ok(inspect) => {
                // Bounded settle for a lagging OOMKilled commit (issue #34).
                let (inspect, verdict) =
                    super::settle_oom_flag(docker, witness, allocation, target, inspect).await;
                inspect
                    .state
                    .as_ref()
                    .and_then(classify::exit_info)
                    .map(|info| classify::apply_oom_verdict(info, verdict))
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

/// Record the daemon's `oom` event for later settles (§4).
///
/// Deliberately does nothing else: an `oom` is not proof the *container* died
/// (the cgroup OOM killer may take a child the container survives), so it never
/// enqueues an exit or touches the claim set. It only becomes evidence when a
/// settle finds the racy shape — SIGKILL exit under a memory limit — and asks.
fn handle_oom(witness: &oom::OomWitness, event: &bollard::models::EventMessage) {
    if event.typ != Some(EventMessageTypeEnum::CONTAINER) {
        return;
    }
    let Some(allocation) = event
        .actor
        .as_ref()
        .and_then(|actor| actor.attributes.as_ref())
        .and_then(|attrs| attrs.get(LABEL_ALLOCATION))
        .and_then(|raw| raw.parse::<AllocationId>().ok())
    else {
        return;
    };
    tracing::debug!(%allocation, "daemon reported a cgroup OOM kill in this container");
    witness.record(allocation, Timestamp::now());
}

/// Turn one `die` event into an exit: parse the allocation from the actor's
/// `coppice.allocation` attribute, claim it (skip duplicates, §4), inspect for
/// evidence, and enqueue — un-claiming on an unusable/failed inspect so a later
/// resync or stop can still surface it.
async fn handle_die(
    docker: &Docker,
    state: &Mutex<ExecutorState>,
    witness: &oom::OomWitness,
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
    let our_kill;
    let newly_claimed = {
        let mut st = lock_state(state);
        // The die event is proof of death regardless of who holds the claim (a
        // disk kill claims *before* its SIGKILL, then this event confirms it):
        // fire the follower's fast drain unconditionally (§8.2).
        st.note_container_dead(allocation);
        our_kill = st.killing.contains(&allocation);
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
                // A stop of ours is in flight: the 137 about to be read is
                // most likely our own grace-expiry SIGKILL, so skip the settle
                // exactly as the stop's post-inspect does (§4) rather than
                // spend the window proving what we already know.
                //
                // "Skip the settle" is not "assume it was us", though. A
                // container can genuinely OOM inside the grace window, and the
                // oom-before-die ordering means that evidence is normally
                // already recorded — so consult the witness first. That is a
                // map lookup, not a wait, and the kernel's kill outranks ours
                // (§4's carve-out on the stopped path).
                let (inspect, verdict) = if our_kill {
                    let verdict = if witness.witnessed(allocation) {
                        oom::OomVerdict::Confirmed
                    } else {
                        oom::OomVerdict::NotInQuestion
                    };
                    (inspect, verdict)
                } else {
                    super::settle_oom_flag(docker, witness, allocation, id, inspect).await
                };
                inspect
                    .state
                    .as_ref()
                    .and_then(classify::exit_info)
                    .map(|info| classify::apply_oom_verdict(info, verdict))
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
