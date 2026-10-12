//! The live session loop: dial a coordinator over mTLS, open the bidi stream,
//! register, then pump commands down and reports up, reconnecting with
//! exponential backoff and rotating endpoints on failure (ADR 0009/0011).
//!
//! Endpoints come from the configured discovery backend (ADR 0037 §2), which is
//! built once — a bad `[discovery]` section is the unrecoverable configuration
//! error below — and then **consulted on every reconnect**. That re-consultation
//! is the point: a coordinator fleet that changes address (a DNS record edited,
//! an ASG scaled, a registration file dropped) reaches a running agent without a
//! restart, and a source that is momentarily unreachable degrades to an empty
//! list — backoff and retry — rather than pinning the agent to a stale address
//! it read at startup.
//!
//! All decisions live in [`Session`]; this file only moves bytes and owns the
//! timers (heartbeat cadence, max-runtime watchdogs, notice stop times). It is
//! not unit-tested — there is no live server in the unit suite — but every
//! branch it can take delegates to a [`Session`] method that is.
//!
//! # Notices and executor work do not wait (ADR 0044 §3, §5)
//!
//! An interruption gives the host two minutes, and the coordinator may be
//! unreachable for all of them. So the notice machinery is [`Background`],
//! owned by [`run`] rather than by a session and raced at **every** await the
//! loop makes (discovery, dial, live session, backoff). Reports are sent on a
//! live stream and dropped otherwise; the next registration's `ObservedSet`
//! repeats them.
//!
//! [`Background`] also runs every slow executor call (starts, stops) on its
//! own task: the session journals first and queues the call, and the result
//! returns as an event. No command ever blocks a notice, nor the reverse.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coppice_consensus::fs::Fs;
use coppice_core::id::AllocationId;
use coppice_core::time::Timestamp;
use coppice_proto::pb::agent::v1 as pb;
use coppice_tls::TlsStore;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

use crate::config::Config;
use crate::executor::{Executor, ExecutorError, StartError, StopOutcome};
use crate::interruption::Interruption;
use crate::session::renewal::Renewal;
use crate::session::{Session, StopRequest};

use coppice_net::session::Client;

/// Metadata key a follower uses to point the agent at the current leader.
const LEADER_HINT: &str = "x-coppice-leader-hint";
/// Bound on the outbound report channel — reports are small and infrequent.
const OUTBOUND_CAPACITY: usize = 64;
/// Bound on concurrently-running deferred reaps: each reap can spend seconds
/// in the telemetry drain barrier plus Docker and store work, so a burst of
/// exits must not fan out into unbounded daemon requests.
const MAX_CONCURRENT_REAPS: usize = 4;
/// Bound on deferred reaps queued for a worker slot. Overflow is dropped with
/// a warning — the janitor sweep reclaims anything dropped.
const REAP_QUEUE_CAPACITY: usize = 256;
/// How long [`run`] waits for the two tasks it owns (the exit watcher and the
/// reaper) once the loop has returned. Mirrors the coordinator's
/// `SHUTDOWN_DRAIN`: every step of a shutdown is bounded, because a step that
/// waits on an in-flight daemon request can wait forever (issue #111).
const TASK_DRAIN: Duration = Duration::from_secs(10);
/// Bound on the drain's closing handshake: the agent half-closes its outbound
/// stream and waits for the coordinator to end the command stream in reply,
/// which is this side's only confirmation that the last reports went out.
const OUTBOUND_FLUSH: Duration = Duration::from_millis(500);
/// The longest the notice timer sleeps before re-reading the wall clock: stop
/// times are wall-clock instants, so a long monotonic sleep would miss a
/// clock step.
const STOP_TIMER_GRANULARITY: Duration = Duration::from_secs(1);

/// Run the agent session until the process is asked to stop: connect, serve,
/// reconnect. Returns `Ok(())` once `shutdown` has flipped and the drain below
/// has finished, and an error only on an unrecoverable configuration error.
///
/// `tls` is the process's shared hot-reload store (ADR 0037 §4): each
/// (re)connect builds its client config from the *current* material, so a
/// rotation on disk reaches the next dial without a restart while the live
/// session finishes on the leaf it connected with.
///
/// # Shutdown is a drain (ADR 0041)
///
/// `shutdown` is the seam the daemon's SIGTERM/SIGINT handler flips, and that
/// an integration test flips directly, so no test ever raises a real signal.
/// Flipping it does **not** stop the loop: the session announces itself
/// `draining` on every subsequent `Register` and `Heartbeat` and keeps serving
/// until [`Drain`]'s invariant is satisfied. [`GraceDeadline`] is the only
/// thing that overrides it, and bounds the whole stop at
/// `config.shutdown_grace`.
pub async fn run<F, E>(
    mut session: Session<F, E>,
    config: &Config,
    tls: Arc<TlsStore>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    F: Fs,
    E: Executor + Clone,
{
    // One watcher task, shared executor state, forwarding natural exits into a
    // channel the serve loop selects on. Survives reconnects.
    let (exit_tx, mut exit_rx) = mpsc::channel(OUTBOUND_CAPACITY);
    let watcher = session.executor().clone();
    let watcher_join = tokio::spawn(async move {
        loop {
            let exit = watcher.next_exit().await;
            if exit_tx.send(exit).await.is_err() {
                break;
            }
        }
    });

    // One reaper task performing the session's deferred reaps
    // (report-before-reap, see `Session::pending_reaps`), at most
    // MAX_CONCURRENT_REAPS at a time — each reap can wait seconds on the
    // telemetry drain barrier, and a burst of exits must not fan out into
    // unbounded daemon requests. Survives reconnects; failures are logged and
    // the janitor sweep retries.
    let (reap_tx, mut reap_rx) =
        mpsc::channel::<coppice_core::id::AllocationId>(REAP_QUEUE_CAPACITY);
    let reaper = session.executor().clone();
    let reaper_join = tokio::spawn(async move {
        let mut inflight = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                next = reap_rx.recv(), if inflight.len() < MAX_CONCURRENT_REAPS => {
                    let Some(alloc) = next else { break };
                    let executor = reaper.clone();
                    inflight.spawn(async move {
                        if let Err(e) = executor.reap(alloc).await {
                            tracing::warn!(%alloc, error = %e, "deferred reap failed; janitor will retry");
                        }
                    });
                }
                Some(_) = inflight.join_next(), if !inflight.is_empty() => {}
            }
        }
        while inflight.join_next().await.is_some() {}
    });

    // Built once: an unusable `[discovery]` section is a configuration error,
    // not something to retry against (ADR 0037 §2). Consulting it, by contrast,
    // never fails — a backend that cannot reach its source answers with an empty
    // list and a warning of its own.
    let discovery = coppice_discovery::build(&config.discovery)?;

    // The notice machinery, owned here so it spans every phase of the loop.
    // Recovery runs before any coordinator is found; its reports are dropped
    // (the ObservedSet repeats them).
    let source: Option<tokio::sync::watch::Receiver<Option<Interruption>>> = None;
    let mut background = Background::new(source, session.executor().clone());
    session.resume_notices(Timestamp::now()).await?;

    let mut drain = Drain::new(config.shutdown_grace);
    // Pending max-runtime watchdog deadlines. Owned here, not by `serve_once`,
    // so they survive a reconnect even when the new session's `observe()`
    // fails and re-arms nothing (issue #166). Firing while no session is
    // connected is not attempted: a deadline that passes in the gap fires on
    // the first loop iteration of the next session.
    let mut deadlines: BTreeMap<coppice_core::id::AllocationId, Instant> = BTreeMap::new();
    // The one clock of the shutdown path, raced at every await below. Nothing
    // on the deadline path flushes: the coordinator's liveness backstop covers
    // whatever the dropped stream did not carry.
    let mut grace = GraceDeadline::new(shutdown.clone(), config.shutdown_grace);

    let mut backoff = config.reconnect_backoff_min;
    let mut endpoint_idx = 0usize;
    let outcome = loop {
        // Consulting discovery is a network step of its own (a DNS lookup, an
        // HTTP GET), so it is raced too: nothing between the flip and the exit
        // may be unbounded. With no stream there is nothing to announce on, so
        // an idle draining agent keeps reconnecting until it can say it is
        // leaving (ADR 0041) — the deadline is its only other way out.
        let Some(candidates) = with_notices(
            discovery.candidates(),
            &mut session,
            &mut background,
            &mut grace,
        )
        .await
        else {
            drain.warn_deadline(&session, "consulting discovery");
            break Ok(());
        };
        if candidates.is_empty() {
            // Nothing to dial this round: the source is unreachable or lists
            // nobody. Back off exactly as a failed session does — the next
            // consultation may well answer. Checked here rather than indexed
            // blindly, so an empty list can never be a modulo-by-zero panic.
            tracing::warn!(
                backend = config.discovery.backend.as_str(),
                ?backoff,
                "discovery returned no coordinator candidates; retrying after backoff"
            );
            if backoff_sleep(
                backoff,
                &mut grace,
                &mut drain,
                &mut shutdown,
                &mut session,
                &mut background,
            )
            .await
            {
                drain.warn_deadline(&session, "waiting to reconnect");
                break Ok(());
            }
            backoff = (backoff * 2).min(config.reconnect_backoff_max);
            continue;
        }
        let endpoint = candidates[endpoint_idx % candidates.len()].as_str();
        endpoint_idx += 1;

        // The dial and the stream open borrow nothing of the session, so the
        // notice machinery keeps running under them: a coordinator that takes
        // its time to answer must not hold up a stop.
        let register = session.register_report();
        let announced = session.active_plan().map(|p| p.deadline);
        let Some(connected) = with_notices(
            open_stream(endpoint, &tls, register),
            &mut session,
            &mut background,
            &mut grace,
        )
        .await
        else {
            drain.warn_deadline(&session, "dialling a coordinator");
            break Ok(());
        };

        // `serve_once` borrows `session` and `drain` mutably for as long as the
        // select expression lives, so the deadline is recorded as a flag and
        // acted on once those borrows have been released with the dropped
        // future.
        let mut expired = false;
        let served = match connected {
            Err(e) => Err(e),
            Ok(connected) => tokio::select! {
                served = serve_once(
                    &mut session,
                    connected,
                    announced,
                    tls.as_ref(),
                    config,
                    &mut exit_rx,
                    &reap_tx,
                    &mut shutdown,
                    &mut drain,
                    &mut deadlines,
                    &mut background,
                ) => served,
                // The grace window closed with a session step still in flight —
                // dropping the future above is what cancels it.
                _ = grace.elapsed() => {
                    expired = true;
                    Ok(Served::Drained)
                }
            },
        };
        if expired {
            drain.warn_deadline(&session, "serving the session");
            break Ok(());
        }

        match served {
            Ok(Served::Drained) => break Ok(()),
            Ok(Served::Closed) => {
                tracing::info!(endpoint, "session closed; reconnecting");
                backoff = config.reconnect_backoff_min;
            }
            Err(e) => {
                tracing::warn!(endpoint, error = %e, "session error; reconnecting");
            }
        }

        session.reset_session();
        // The announcement was session-scoped: whatever the broken stream did
        // or did not deliver, the next registration carries `draining` again
        // and re-establishes it at the coordinator (ADR 0041).
        drain.announced = false;
        if backoff_sleep(
            backoff,
            &mut grace,
            &mut drain,
            &mut shutdown,
            &mut session,
            &mut background,
        )
        .await
        {
            drain.warn_deadline(&session, "waiting to reconnect");
            break Ok(());
        }
        backoff = (backoff * 2).min(config.reconnect_backoff_max);
    };

    // The loop is done; the two tasks this function owns drain under one
    // bounded deadline, as every shutdown step must (issue #111).
    //
    // The reaper stops on its own once the queue sender drops, finishing the
    // reaps already in flight — those hold the telemetry drain barrier, so
    // they are worth waiting for. The exit watcher cannot: it is parked inside
    // `next_exit()`, which has no cancellation of its own, so it is aborted.
    // Nothing is lost — an exit that arrives after the run loop has stopped
    // reporting has nowhere to go, and the journal is the durable record
    // either way (ADR 0009).
    drop(reap_tx);
    // Dropping the background machinery aborts any start or stop still in
    // flight: the loop that would journal its result is gone. A journaled
    // tombstone re-runs a stop on the next start; a start cut short is
    // reconciled from the journal and runtime like any crash (ADR 0009).
    drop(background);
    let deadline = tokio::time::Instant::now() + TASK_DRAIN;
    crate::drain_task("reaper", reaper_join, deadline).await;
    watcher_join.abort();
    let _ = watcher_join.await;
    outcome
}

/// The shutdown grace window, as a single future raced against every await the
/// session loop makes.
///
/// It enforces exactly one invariant: **the process leaves the session loop no
/// later than `shutdown_grace` after the shutdown flip, whatever it is
/// mid-await.** Checking a deadline between `select!` iterations would not
/// bound anything — a branch body parked in `observe()`, which on the real
/// executor is a Docker list/inspect that can sit for Docker's own 120 s
/// request timeout, neither observes the flip nor reaches the top of the loop.
/// When this future wins a race, the in-flight future is dropped, cancelling
/// the request under it.
///
/// It is built from a *cloned* shutdown receiver, so the clock starts at the
/// flip itself rather than at whenever the loop got round to observing it, and
/// it spans reconnects. Once it has fired the loop always exits, so it is never
/// polled to completion twice.
struct GraceDeadline(std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>);

impl GraceDeadline {
    fn new(mut shutdown: tokio::sync::watch::Receiver<bool>, grace: Duration) -> GraceDeadline {
        GraceDeadline(Box::pin(async move {
            flipped(&mut shutdown).await;
            tokio::time::sleep(grace).await;
        }))
    }

    /// Resolve once the window has closed, and never before the flip.
    async fn elapsed(&mut self) {
        (&mut self.0).await
    }
}

/// Shutdown bookkeeping for one run of the session loop (ADR 0041).
///
/// It enforces exactly one invariant: **the loop does not exit early until the
/// drain has been announced on a live session and the accountable live work is
/// empty** ([`complete`](Drain::complete)). The only thing that overrides it is
/// [`GraceDeadline`]. A drain is never retracted: the only thing that flips the
/// shutdown watch is the process being told to stop.
struct Drain {
    /// The configured window, kept for the warning that names it.
    grace: Duration,
    /// Whether the shutdown flip has been observed.
    started: bool,
    /// Whether a report carrying `draining = true` has been handed to the
    /// *current* session's outbound stream. Reset on every reconnect, because
    /// a report the broken stream may have swallowed is not an announcement.
    announced: bool,
}

impl Drain {
    fn new(grace: Duration) -> Drain {
        Drain {
            grace,
            started: false,
            announced: false,
        }
    }

    /// Observe the shutdown flip: mark the session draining, so every later
    /// report carries it, including a re-registration.
    fn begin<F: Fs, E: Executor>(&mut self, session: &mut Session<F, E>) {
        if self.started {
            return;
        }
        session.set_draining(true);
        self.started = true;
        tracing::info!(
            grace = ?self.grace,
            outstanding = session.outstanding_live_work().len(),
            "shutdown requested; draining (announcing to the coordinator, then waiting for \
             running work to finish)"
        );
    }

    /// Whether the drain is done: announced on a live session, with nothing
    /// left that this agent is accountable for.
    fn complete<F: Fs, E: Executor>(&self, session: &Session<F, E>) -> bool {
        self.started && self.announced && session.outstanding_live_work().is_empty()
    }

    /// The one log of the deadline path: which step was in flight, and what
    /// work is being abandoned. The containers are deliberately left running
    /// for the coordinator's liveness backstop (ADR 0041).
    fn warn_deadline<F: Fs, E: Executor>(&self, session: &Session<F, E>, step: &str) {
        let outstanding = session.outstanding_live_work();
        let allocations: Vec<String> = outstanding.iter().map(|a| a.to_string()).collect();
        tracing::warn!(
            grace = ?self.grace,
            step,
            outstanding = allocations.len(),
            allocations = %allocations.join(","),
            "shutdown_grace elapsed; dropping the step in flight and exiting, leaving any \
             running containers to the coordinator's liveness backstop (ADR 0041)"
        );
    }
}

/// How a serve attempt ended: the stream closed (reconnect), or the drain
/// finished under it (stop the run loop).
enum Served {
    Closed,
    Drained,
}

/// Resolve once `shutdown` reads `true`, and never otherwise.
///
/// Level-triggered rather than edge-triggered, so a flip that happened before
/// this future was built still fires; a dropped sender parks forever, because
/// it means the flip can no longer come (that is `coppice dev`, which stops its
/// agent by dropping the task).
async fn flipped(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Sleep for the reconnect backoff, waking early on a shutdown flip (which
/// begins the drain and retries at once — a draining agent wants its
/// announcement on the wire), and serving notice events throughout. Returns
/// `true` if the grace window closed instead, which ends the run.
async fn backoff_sleep<F: Fs, E: Executor + Clone>(
    backoff: Duration,
    grace: &mut GraceDeadline,
    drain: &mut Drain,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    session: &mut Session<F, E>,
    background: &mut Background<E>,
) -> bool {
    let sleep = tokio::time::sleep(backoff);
    tokio::pin!(sleep);
    loop {
        let next_stop = session.next_stop_at();
        tokio::select! {
            _ = &mut sleep => return false,
            _ = flipped(shutdown), if !drain.started => {
                drain.begin(session);
                return false;
            }
            _ = grace.elapsed() => return true,
            event = background.next(next_stop) => {
                if let Err(e) = on_background_event(session, background, event, None).await {
                    tracing::warn!(error = %e, "notice event failed between connections");
                }
            }
        }
    }
}

/// Await `fut` — a step between connections that borrows nothing of the
/// session — while serving notice events. `None` if the grace window closed
/// first, which ends the run.
async fn with_notices<T, F, E>(
    fut: impl std::future::Future<Output = T>,
    session: &mut Session<F, E>,
    background: &mut Background<E>,
    grace: &mut GraceDeadline,
) -> Option<T>
where
    F: Fs,
    E: Executor + Clone,
{
    tokio::pin!(fut);
    loop {
        let next_stop = session.next_stop_at();
        tokio::select! {
            value = &mut fut => return Some(value),
            _ = grace.elapsed() => return None,
            event = background.next(next_stop) => {
                if let Err(e) = on_background_event(session, background, event, None).await {
                    tracing::warn!(error = %e, "notice event failed between connections");
                }
            }
        }
    }
}

enum BackgroundEvent {
    /// The interruption source published a notice with this deadline.
    Interruption(Timestamp),
    /// A stop time may have arrived.
    StopsDue,
    StartDone(AllocationId, Result<(), StartError>),
    StopDone(AllocationId, Result<StopOutcome, ExecutorError>),
}

/// The executor-bound work [`run`] owns across every phase of the loop (see
/// the module docs): the interruption source's watch, the stop-time timer,
/// and the spawned starts and stops.
struct Background<E> {
    /// `None` with no source configured, or once the source has gone away.
    source: Option<tokio::sync::watch::Receiver<Option<Interruption>>>,
    starts: tokio::task::JoinSet<(AllocationId, Result<(), StartError>)>,
    stops: tokio::task::JoinSet<(AllocationId, Result<StopOutcome, ExecutorError>)>,
    /// Which allocation each in-flight task is for, so a task that dies (a
    /// panic) still reports back: a start as failed, a stop for its retry.
    task_ids: BTreeMap<tokio::task::Id, AllocationId>,
    executor: E,
}

impl<E: Executor + Clone> Background<E> {
    fn new(
        source: Option<tokio::sync::watch::Receiver<Option<Interruption>>>,
        executor: E,
    ) -> Background<E> {
        Background {
            source,
            starts: tokio::task::JoinSet::new(),
            stops: tokio::task::JoinSet::new(),
            task_ids: BTreeMap::new(),
            executor,
        }
    }

    /// Spawn every start and stop the session has queued, plus `stops`.
    fn spawn<F: Fs>(&mut self, session: &mut Session<F, E>, stops: Vec<StopRequest>) {
        for spec in session.take_pending_starts() {
            let executor = self.executor.clone();
            let allocation = spec.allocation;
            let handle = self
                .starts
                .spawn(async move { (allocation, executor.start(spec).await) });
            self.task_ids.insert(handle.id(), allocation);
        }
        for StopRequest { allocation, grace } in
            stops.into_iter().chain(session.take_pending_stops())
        {
            let executor = self.executor.clone();
            let handle = self
                .stops
                .spawn(async move { (allocation, executor.stop(allocation, grace).await) });
            self.task_ids.insert(handle.id(), allocation);
        }
    }

    /// The next event. Must stay cancel-safe: every phase of the loop races
    /// it against its own work.
    async fn next(&mut self, next_stop_at: Option<Timestamp>) -> BackgroundEvent {
        let stop_wait = next_stop_at.map(|at| {
            at.duration_since(Timestamp::now())
                .to_std()
                .unwrap_or(Duration::ZERO)
                .min(STOP_TIMER_GRANULARITY)
        });
        loop {
            tokio::select! {
                changed = source_changed(&mut self.source) => {
                    if let Some(deadline) = changed {
                        return BackgroundEvent::Interruption(deadline);
                    }
                }
                _ = async {
                    match stop_wait {
                        Some(wait) => tokio::time::sleep(wait).await,
                        None => std::future::pending().await,
                    }
                } => return BackgroundEvent::StopsDue,
                Some(joined) = self.starts.join_next_with_id(), if !self.starts.is_empty() => {
                    match joined {
                        Ok((id, (allocation, result))) => {
                            self.task_ids.remove(&id);
                            return BackgroundEvent::StartDone(allocation, result);
                        }
                        Err(e) => {
                            if let Some(allocation) = self.task_ids.remove(&e.id()) {
                                return BackgroundEvent::StartDone(
                                    allocation,
                                    Err(StartError::Start {
                                        user_error: false,
                                        message: format!("start task failed: {e}"),
                                    }),
                                );
                            }
                        }
                    }
                }
                Some(joined) = self.stops.join_next_with_id(), if !self.stops.is_empty() => {
                    match joined {
                        Ok((id, (allocation, result))) => {
                            self.task_ids.remove(&id);
                            return BackgroundEvent::StopDone(allocation, result);
                        }
                        Err(e) => {
                            if let Some(allocation) = self.task_ids.remove(&e.id()) {
                                return BackgroundEvent::StopDone(
                                    allocation,
                                    Err(ExecutorError::Other(format!("stop task failed: {e}"))),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The next notice deadline from the source, or never: pends forever with no
/// source, and drops the source once its sender is gone. `Some(None)` cannot
/// escape — a change to "no notice" is not an event.
async fn source_changed(
    source: &mut Option<tokio::sync::watch::Receiver<Option<Interruption>>>,
) -> Option<Timestamp> {
    let Some(rx) = source.as_mut() else {
        return std::future::pending().await;
    };
    match rx.changed().await {
        Ok(()) => rx.borrow_and_update().map(|n| n.deadline),
        Err(_) => {
            // The source is gone; whatever it last published was already
            // seen. Nothing more will come.
            *source = None;
            None
        }
    }
}

/// Hand one background event to the session, spawn whatever it queued in
/// response, and send its reports when there is a stream (`tx`).
/// Disconnected reports are dropped: the next registration's ObservedSet
/// carries every running container, live mark and journaled exit.
async fn on_background_event<F, E>(
    session: &mut Session<F, E>,
    background: &mut Background<E>,
    event: BackgroundEvent,
    tx: Option<&mpsc::Sender<pb::AgentReport>>,
) -> anyhow::Result<()>
where
    F: Fs,
    E: Executor + Clone,
{
    // Clock read at the edge (workspace convention).
    let now = Timestamp::now();
    let mut stops = Vec::new();
    let reports = match event {
        BackgroundEvent::Interruption(deadline) => session.on_interruption(deadline, now).await?,
        BackgroundEvent::StopsDue => {
            stops = session.begin_due_stops(now)?;
            Vec::new()
        }
        BackgroundEvent::StartDone(allocation, result) => {
            session.finish_start(allocation, result, now).await?
        }
        BackgroundEvent::StopDone(allocation, result) => {
            session.finish_stop(allocation, result, now)?
        }
    };
    background.spawn(session, stops);
    session.refresh_health();
    if let Some(tx) = tx {
        send_all(tx, reports).await?;
    }
    Ok(())
}

/// Dial `endpoint` with a client config built from the store's *current*
/// material (ADR 0037 §4): the cluster CA as trust root, this node's leaf as
/// the client identity. Rebuilt per dial so a reconnect after a rotation
/// presents the fresh leaf.
async fn dial(endpoint: &str, store: &TlsStore) -> anyhow::Result<Channel> {
    let (host, _port) = coppice_tls::split_host_port(endpoint)?;
    let material = store.current();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(material.ca_pem()))
        .identity(Identity::from_pem(material.cert_pem(), material.key_pem()))
        .domain_name(host);
    let channel = Channel::from_shared(format!("https://{endpoint}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(channel)
}

/// A dialled coordinator with the session stream open and `Register` queued.
struct Connected {
    client: Client<Channel>,
    tx: mpsc::Sender<pb::AgentReport>,
    inbound: tonic::Streaming<pb::AgentCommand>,
}

/// Dial `endpoint` and open the session stream with `register` as its first
/// message (node_epoch = 0). Borrows nothing of the session, so the loop can
/// keep serving notice events while the network takes its time.
async fn open_stream(
    endpoint: &str,
    tls: &TlsStore,
    register: pb::AgentReport,
) -> anyhow::Result<Connected> {
    let channel = dial(endpoint, tls).await?;
    let mut client = Client::new(channel);

    let (tx, rx) = mpsc::channel::<pb::AgentReport>(OUTBOUND_CAPACITY);
    let outbound = ReceiverStream::new(rx);
    tx.send(register).await.ok();

    let inbound = client.session(Request::new(outbound)).await?.into_inner();
    Ok(Connected {
        client,
        tx,
        inbound,
    })
}

/// Serve one connected session until it closes or the drain finishes.
/// `announced` is the drain-plan deadline the queued `Register` carried: if
/// the plan has changed since (a notice that landed during the dial, or one
/// mid-session), a heartbeat re-announces it as soon as registration is done.
#[allow(clippy::too_many_arguments)] // wiring seam: each is a distinct loop input
async fn serve_once<F, E>(
    session: &mut Session<F, E>,
    connected: Connected,
    mut announced: Option<Timestamp>,
    tls: &TlsStore,
    config: &Config,
    exit_rx: &mut mpsc::Receiver<crate::executor::ExitEvent>,
    reap_tx: &mpsc::Sender<coppice_core::id::AllocationId>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    drain: &mut Drain,
    deadlines: &mut BTreeMap<coppice_core::id::AllocationId, Instant>,
    background: &mut Background<E>,
) -> anyhow::Result<Served>
where
    F: Fs,
    E: Executor + Clone,
{
    let Connected {
        mut client,
        tx,
        mut inbound,
    } = connected;

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Reap-janitor backstop (§5): the sweep bound is the configured age; the
    // tick cadence is capped at 1h so a 24h bound still checks regularly. Guard
    // against a zero cadence (a zero-duration `interval` panics).
    let reap_bound = config.executor.reap_janitor_after;
    let janitor_cadence = reap_bound
        .min(Duration::from_secs(60 * 60))
        .max(Duration::from_secs(1));
    let mut janitor = tokio::time::interval(janitor_cadence);
    janitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let reap_bound = coppice_core::time::Duration::from(reap_bound);
    // Leaf renewal (ADR 0037 §4) rides this channel, so its deadline is
    // recomputed here on every reconnect: a session that resumes near expiry
    // renews at once rather than waiting out a timer set before the break.
    let mut renewal = Renewal::new();
    let mut renew_at = Instant::now() + renewal.delay(tls);

    loop {
        // Reaps deferred by the session (report-before-reap: a reap can stall
        // seconds behind the telemetry drain barrier, and the terminal report
        // must be queued ahead of the next heartbeat or the coordinator
        // misclassifies the exit as a lost attempt). Handed to the bounded
        // reaper task so the drain never delays command processing either; a
        // full queue is dropped, the janitor sweep reclaims it.
        for alloc in session.take_pending_reaps() {
            if reap_tx.try_send(alloc).is_err() {
                tracing::warn!(%alloc, "deferred-reap queue full; janitor will reclaim the container");
            }
        }

        // The journal changed if the last event journaled anything; re-publish
        // what `/readyz` reports as `running` (ADR 0041). Cheap, and the only
        // place that knows an event has finished being handled.
        session.refresh_health();

        // Fold newly armed watchdogs into the deadline map — fresh starts and
        // registration re-arms alike (issue #166). Done at the top of every
        // iteration, not just after a command, so the re-arm that rides the
        // `RegisterAccepted` handling is picked up before the next select.
        // A re-arm keeps the earlier deadline: the retained one is monotonic,
        // while a rebuilt one rests on the runtime's wall-clock reading and can
        // drift later with a clock step, so it may tighten a deadline but
        // never postpone one. Allocations
        // with a journaled exit (e.g. stopped by `StopJob`) are dropped so the
        // map holds no dead entries.
        for w in session.take_armed_watchdogs() {
            let at = w.measured_at + w.fires_after.to_std().unwrap_or(Duration::ZERO);
            deadlines
                .entry(w.allocation)
                .and_modify(|d| *d = (*d).min(at))
                .or_insert(at);
        }
        deadlines.retain(|alloc, _| !session.state().exits.contains_key(alloc));

        // The drain's two obligations, evaluated after every event the loop
        // handled (a command, an exit, a heartbeat tick) rather than on a poll
        // of their own (ADR 0041).
        //
        // First: say it. A heartbeat sent the moment the session is registered
        // and draining is the announcement — the `Register` of a session opened
        // *while* draining already carries the flag, but this one covers the
        // flip landing mid-session, and re-sending it costs one small report.
        if drain.started && !drain.announced && session.is_registered() {
            let hb = session.heartbeat_report().await;
            send_all(&tx, vec![hb]).await?;
            drain.announced = true;
            tracing::info!("draining announced to the coordinator; no new placements from here");
        }
        // An interruption plan that started or changed since the `Register`
        // went out is announced the same way (ADR 0044 §5): the coordinator
        // must stop placing here now, not at the next tick.
        let plan = session.active_plan().map(|p| p.deadline);
        if plan != announced && session.is_registered() {
            let hb = session.heartbeat_report().await;
            send_all(&tx, vec![hb]).await?;
            announced = plan;
        }
        // Then: stop, if there is nothing left to be accountable for.
        if drain.complete(session) {
            tracing::info!("drain complete: no live work left, session closing");
            // Half-close the outbound stream rather than tearing the
            // connection down: dropping `tx` makes tonic send everything still
            // queued — the announcement above, any terminal status — and then
            // END_STREAM, which the coordinator answers by ending its command
            // stream. Draining `inbound` to that end is this side's only
            // confirmation the queue went out; the backstop covers a timeout.
            drop(tx);
            let _ = tokio::time::timeout(OUTBOUND_FLUSH, async {
                while let Ok(Some(_)) = inbound.message().await {}
            })
            .await;
            return Ok(Served::Drained);
        }

        // The next watchdog to fire, if any.
        let next_deadline = deadlines.values().min().copied();
        let watchdog = async {
            match next_deadline {
                Some(at) => {
                    tokio::time::sleep_until(at).await;
                }
                None => std::future::pending::<()>().await,
            }
        };

        let next_stop = session.next_stop_at();
        tokio::select! {
            event = background.next(next_stop) => {
                on_background_event(session, background, event, Some(&tx)).await?;
            }
            message = inbound.message() => {
                match message {
                    Ok(Some(cmd)) => {
                        let reports = session.handle_command(cmd).await?;
                        // Executor-bound work the command queued (a start, an
                        // abort's stop) runs on its own task.
                        background.spawn(session, Vec::new());
                        send_all(&tx, reports).await?;
                    }
                    Ok(None) => return Ok(Served::Closed),
                    Err(status) => {
                        if let Some(hint) = status.metadata().get(LEADER_HINT) {
                            tracing::info!(?hint, "coordinator refused with a leader hint; rotating");
                        }
                        return Err(status.into());
                    }
                }
            }
            _ = heartbeat.tick() => {
                if session.is_registered() {
                    let hb = session.heartbeat_report().await;
                    send_all(&tx, vec![hb]).await?;
                }
            }
            _ = tokio::time::sleep_until(renew_at) => {
                renew_at = Instant::now() + renewal.attempt(&mut client, tls).await;
            }
            _ = janitor.tick() => {
                // Clock read at the edge (workspace convention).
                session
                    .janitor_sweep(coppice_core::time::Timestamp::now(), reap_bound)
                    .await?;
            }
            exit = exit_rx.recv() => {
                if let Some(crate::executor::ExitEvent { allocation, exit: info }) = exit {
                    deadlines.remove(&allocation);
                    let reports = session.handle_observed_exit(allocation, info).await?;
                    send_all(&tx, reports).await?;
                }
            }
            _ = watchdog => {
                if let Some(alloc) = next_deadline.and_then(|_| due_allocation(deadlines)) {
                    deadlines.remove(&alloc);
                    let reports = session.trigger_max_runtime(alloc).await?;
                    send_all(&tx, reports).await?;
                }
            }
            // The shutdown flip, once. The loop keeps running; the top of the
            // next iteration announces the drain and re-evaluates its exit.
            _ = flipped(shutdown), if !drain.started => {
                drain.begin(session);
            }
        }
    }
}

/// The allocation whose deadline is earliest (already known to be due).
fn due_allocation(
    deadlines: &BTreeMap<coppice_core::id::AllocationId, Instant>,
) -> Option<coppice_core::id::AllocationId> {
    deadlines
        .iter()
        .min_by_key(|(_, at)| **at)
        .map(|(alloc, _)| *alloc)
}

async fn send_all(
    tx: &mpsc::Sender<pb::AgentReport>,
    reports: Vec<pb::AgentReport>,
) -> anyhow::Result<()> {
    for report in reports {
        tx.send(report)
            .await
            .map_err(|_| anyhow::anyhow!("outbound report channel closed"))?;
    }
    Ok(())
}
