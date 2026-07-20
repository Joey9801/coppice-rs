//! The live session loop: dial a coordinator over mTLS, open the bidi stream,
//! register, then pump commands down and reports up, reconnecting with
//! exponential backoff and rotating endpoints on failure (ADR 0009/0011).
//!
//! All decisions live in [`Session`]; this file only moves bytes and owns the
//! timers (heartbeat cadence, max-runtime watchdogs). It is not unit-tested —
//! there is no live server in the unit suite — but every branch it can take
//! delegates to a [`Session`] method that is.

use std::collections::BTreeMap;
use std::time::Duration;

use coppice_consensus::fs::Fs;
use coppice_proto::pb::agent::v1 as pb;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

use crate::config::Config;
use crate::executor::Executor;
use crate::session::Session;

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

/// Run the agent session forever: connect, serve, reconnect. Returns only on
/// an unrecoverable configuration error (e.g. unreadable TLS material).
pub async fn run<F, E>(mut session: Session<F, E>, config: &Config) -> anyhow::Result<()>
where
    F: Fs,
    E: Executor + Clone,
{
    let ca = std::fs::read(&config.tls.ca_path)?;
    let cert = std::fs::read(&config.tls.cert_path)?;
    let key = std::fs::read(&config.tls.key_path)?;

    // One watcher task, shared executor state, forwarding natural exits into a
    // channel the serve loop selects on. Survives reconnects.
    let (exit_tx, mut exit_rx) = mpsc::channel(OUTBOUND_CAPACITY);
    let watcher = session.executor().clone();
    tokio::spawn(async move {
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
    tokio::spawn(async move {
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

    let mut backoff = config.reconnect_backoff_min;
    let mut endpoint_idx = 0usize;
    loop {
        let endpoint = &config.coordinators[endpoint_idx % config.coordinators.len()];
        endpoint_idx += 1;

        match serve_once(
            &mut session,
            endpoint,
            &ca,
            &cert,
            &key,
            config,
            &mut exit_rx,
            &reap_tx,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(endpoint, "session closed; reconnecting");
                backoff = config.reconnect_backoff_min;
            }
            Err(e) => {
                tracing::warn!(endpoint, error = %e, "session error; reconnecting");
            }
        }

        session.reset_session();
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(config.reconnect_backoff_max);
    }
}

async fn dial(endpoint: &str, ca: &[u8], cert: &[u8], key: &[u8]) -> anyhow::Result<Channel> {
    let host = endpoint
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(endpoint)
        .to_string();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key))
        .domain_name(host);
    let channel = Channel::from_shared(format!("https://{endpoint}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(channel)
}

#[allow(clippy::too_many_arguments)]
async fn serve_once<F, E>(
    session: &mut Session<F, E>,
    endpoint: &str,
    ca: &[u8],
    cert: &[u8],
    key: &[u8],
    config: &Config,
    exit_rx: &mut mpsc::Receiver<crate::executor::ExitEvent>,
    reap_tx: &mpsc::Sender<coppice_core::id::AllocationId>,
) -> anyhow::Result<()>
where
    F: Fs,
    E: Executor + Clone,
{
    let channel = dial(endpoint, ca, cert, key).await?;
    let mut client = Client::new(channel);

    let (tx, rx) = mpsc::channel::<pb::AgentReport>(OUTBOUND_CAPACITY);
    let outbound = ReceiverStream::new(rx);

    // First message: Register (node_epoch = 0).
    tx.send(session.register_report()).await.ok();

    let mut inbound = client.session(Request::new(outbound)).await?.into_inner();

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
    // A monotonic map of pending watchdog deadlines.
    let mut deadlines: BTreeMap<coppice_core::id::AllocationId, Instant> = BTreeMap::new();

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

        tokio::select! {
            message = inbound.message() => {
                match message {
                    Ok(Some(cmd)) => {
                        let reports = session.handle_command(cmd).await?;
                        send_all(&tx, reports).await?;
                        for w in session.take_armed_watchdogs() {
                            deadlines.insert(
                                w.allocation,
                                Instant::now()
                                    + w.max_runtime.to_std().unwrap_or(Duration::ZERO),
                            );
                        }
                    }
                    Ok(None) => return Ok(()),
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
                if let Some(alloc) = next_deadline.and_then(|_| due_allocation(&deadlines)) {
                    deadlines.remove(&alloc);
                    let reports = session.trigger_max_runtime(alloc).await?;
                    send_all(&tx, reports).await?;
                }
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
