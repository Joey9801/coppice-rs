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

async fn serve_once<F, E>(
    session: &mut Session<F, E>,
    endpoint: &str,
    ca: &[u8],
    cert: &[u8],
    key: &[u8],
    config: &Config,
    exit_rx: &mut mpsc::Receiver<crate::executor::ExitEvent>,
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
    // A monotonic map of pending watchdog deadlines.
    let mut deadlines: BTreeMap<coppice_core::id::AllocationId, Instant> = BTreeMap::new();

    loop {
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
                                Instant::now() + Duration::from_micros(w.max_runtime_us),
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
