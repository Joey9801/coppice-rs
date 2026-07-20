//! The agent-hosted `NodeService` (ADR 0034): the one service an agent *hosts*
//! for coordinators to dial.
//!
//! This is the inverse direction of the agent [`session`](crate::session): there
//! the agent dials *out* to the leader over a fenced, push-only control stream;
//! here the agent *listens*, and any coordinator replica dials *in* as an
//! ordinary mTLS gRPC client to read a bounded page of an attempt's stored logs.
//! The [`FetchLogsService`] handler is a pure translation layer over the
//! telemetry store's [`log_page`](crate::telemetry::FilesystemSink::log_page)
//! read API — it proposes nothing, journals nothing, and never touches the
//! session's fenced state, so it carries no `CommandHeader` and needs no leader
//! involvement.
//!
//! Identity mirrors the coordinator's own mTLS acceptors (ADR 0011): the agent's
//! existing leaf is the server certificate, client certs are **mandatory**
//! (`client_auth_optional(false)`), and chain validation under the shared trust
//! root is the only gate in v1 — no CN or role binding (deferred to the OD-14/15
//! PKI work). Coordinators pin the server's typed node id as the TLS server-name,
//! so the leaf must carry `node-<uuid>` as a dNSName SAN; a stolen advertised
//! address is useless without the node's key.

use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;

use coppice_proto::pb::agent::v1 as pb;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::telemetry::{FilesystemSink, LogOrder, LogPageQuery, LogStream, ResumeAt, StoreError};
use coppice_core::time::Timestamp;

// ---- server-side ceilings (ADR 0034, "Bounded work per request") ---------

/// The server-side ceiling on `max_chunks`: a request asking for more (or for
/// zero, which would make no progress) is clamped to this, never rejected.
const MAX_CHUNKS_CEILING: usize = 10_000;

/// The server-side ceiling on `max_bytes`, ~256 KiB of chunk bytes per page
/// (ADR 0034). Clamped, never rejected; `0` clamps up to the ceiling so a
/// request always makes progress.
const MAX_BYTES_CEILING: u64 = 256 * 1024;

// ---- metrics (ADR 0034) --------------------------------------------------

/// `FetchLogs` requests served (accepted at the handler, before the store read).
const AGENT_NODE_SERVICE_FETCH_REQUESTS_TOTAL: &str = "agent_node_service_fetch_requests_total";

/// `FetchLogs` requests answered with the `UnknownAttempt` arm — the store held
/// no data for the attempt (fallen out of retention, telemetry disabled, or
/// never written; indistinguishable on the agent by design).
const AGENT_NODE_SERVICE_UNKNOWN_ATTEMPT_TOTAL: &str = "agent_node_service_unknown_attempt_total";

/// Cumulative chunk payload bytes returned across all `FetchLogs` pages.
const AGENT_NODE_SERVICE_BYTES_SERVED_TOTAL: &str = "agent_node_service_bytes_served_total";

/// Register this module's metric names (ADR 0034). Part of the crate-level
/// [`crate::describe_metrics`] fan-out.
pub fn describe_metrics() {
    metrics::describe_counter!(
        AGENT_NODE_SERVICE_FETCH_REQUESTS_TOTAL,
        metrics::Unit::Count,
        "FetchLogs requests served by the agent's NodeService (ADR 0034)."
    );
    metrics::describe_counter!(
        AGENT_NODE_SERVICE_UNKNOWN_ATTEMPT_TOTAL,
        metrics::Unit::Count,
        "FetchLogs requests answered UnknownAttempt — no data on this node (ADR 0034)."
    );
    metrics::describe_counter!(
        AGENT_NODE_SERVICE_BYTES_SERVED_TOTAL,
        metrics::Unit::Bytes,
        "Cumulative log-chunk payload bytes served by the agent's NodeService (ADR 0034)."
    );
}

/// Point-in-time sampling for this module. A no-op: every metric here is a
/// counter pushed at its event (the crate's push-style convention).
pub fn gather_metrics() {}

// ---- the mTLS listener (mirrors coordinator's AgentListener::bind) --------

/// The bound `NodeService` mTLS listener and its TLS config. Bound eagerly at
/// daemon start (fail-fast on a port conflict), then served by [`serve`].
pub struct NodeServiceListener {
    incoming: TcpIncoming,
    tls: ServerTlsConfig,
    local_addr: SocketAddr,
}

impl NodeServiceListener {
    /// Bind the `NodeService` mTLS listener on `addr` from PEM material already
    /// in memory (ADR 0011/0034).
    ///
    /// The same cert/key/ca as the agent's session client identity; client certs
    /// are REQUIRED (`client_auth_optional(false)`), so chain validation under
    /// the trust root is the sole gate — no CN or role binding in v1. A `:0` bind
    /// resolves to a concrete port, readable via [`local_addr`](Self::local_addr)
    /// (the in-process integration test relies on this).
    pub fn bind(
        addr: SocketAddr,
        cert_pem: &[u8],
        key_pem: &[u8],
        ca_pem: &[u8],
    ) -> Result<NodeServiceListener> {
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .client_ca_root(Certificate::from_pem(ca_pem))
            .client_auth_optional(false);

        // Bind eagerly so a port conflict fails the daemon here, then hand the
        // listener to tonic. Own the std bind so the resolved `:0` port is
        // readable (`TcpIncoming::new` hides it).
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| anyhow!("binding NodeService listener on {addr}: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| anyhow!("setting NodeService listener non-blocking: {e}"))?;
        let local_addr = std_listener
            .local_addr()
            .map_err(|e| anyhow!("reading NodeService listener address: {e}"))?;
        let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|e| anyhow!("adopting NodeService listener into tokio: {e}"))?;
        let incoming = TcpIncoming::from_listener(tokio_listener, true, None)
            .map_err(|e| anyhow!("wrapping NodeService listener for tonic: {e}"))?;
        tracing::info!(%local_addr, "NodeService mTLS listener bound (ADR 0034)");

        Ok(NodeServiceListener {
            incoming,
            tls,
            local_addr,
        })
    }

    /// The actual bound address (resolves a `:0` request).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Serve the `NodeService` over the bound listener until the process stops,
/// spawning the tonic server as a background task and returning its handle.
///
/// `log_store` is the first LOG-consuming store (the `TelemetryWiring.log_store`
/// precedent); `None` when telemetry is disabled, in which case every request is
/// answered `UnknownAttempt` — data cannot exist.
pub fn serve(
    listener: NodeServiceListener,
    log_store: Option<FilesystemSink>,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let NodeServiceListener { incoming, tls, .. } = listener;
    let service = coppice_net::node_service::Server::new(FetchLogsService::new(log_store));
    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)?
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
    })
}

/// Bind the `NodeService` listener from the config's `[tls]` paths (ADR
/// 0011/0034), naming each path on a read failure. Reads the same cert/key/ca
/// the session client uses.
pub fn prepare_listener(
    addr: SocketAddr,
    tls: &crate::config::TlsConfig,
) -> Result<NodeServiceListener> {
    let cert = std::fs::read(&tls.cert_path)
        .with_context(|| format!("reading TLS certificate {}", tls.cert_path.display()))?;
    let key = std::fs::read(&tls.key_path)
        .with_context(|| format!("reading TLS private key {}", tls.key_path.display()))?;
    let ca = std::fs::read(&tls.ca_path)
        .with_context(|| format!("reading TLS CA certificate {}", tls.ca_path.display()))?;
    NodeServiceListener::bind(addr, &cert, &key, &ca)
}

// ---- the FetchLogs handler ----------------------------------------------

/// The `NodeService` implementation: a pure translation layer over the
/// telemetry store. Holds only the log-consuming store handle (or `None` when
/// telemetry is disabled) — never the session, journal, or executor.
pub struct FetchLogsService {
    log_store: Option<FilesystemSink>,
}

impl FetchLogsService {
    /// Build the handler over the log-consuming store (`None` disables it).
    pub fn new(log_store: Option<FilesystemSink>) -> FetchLogsService {
        FetchLogsService { log_store }
    }
}

#[tonic::async_trait]
impl coppice_net::node_service::NodeService for FetchLogsService {
    async fn fetch_logs(
        &self,
        request: Request<pb::FetchLogsRequest>,
    ) -> Result<Response<pb::FetchLogsResponse>, Status> {
        metrics::counter!(AGENT_NODE_SERVICE_FETCH_REQUESTS_TOTAL).increment(1);
        let request = request.into_inner();

        // The coordinator resolves `(job, attempt)` from replicated state before
        // dialing, so a missing/malformed id is a caller bug, not a "gone"
        // attempt — reject it honestly rather than masquerading as UnknownAttempt.
        let job = request
            .job
            .ok_or_else(|| Status::invalid_argument("FetchLogsRequest.job is required"))?
            .try_into()
            .map_err(|_| Status::invalid_argument("FetchLogsRequest.job is malformed"))?;
        let attempt = request
            .attempt
            .ok_or_else(|| Status::invalid_argument("FetchLogsRequest.attempt is required"))?
            .try_into()
            .map_err(|_| Status::invalid_argument("FetchLogsRequest.attempt is malformed"))?;

        // No log-consuming store ⇒ data cannot exist on this node: every request
        // is honestly UnknownAttempt.
        let Some(store) = &self.log_store else {
            return Ok(Response::new(unknown_attempt()));
        };

        let query = LogPageQuery {
            stream: request_stream(request.stream),
            from: request.from_us.and_then(Timestamp::from_micros),
            until: request.until_us.and_then(Timestamp::from_micros),
            order: if request.ascending {
                LogOrder::Ascending
            } else {
                LogOrder::Descending
            },
            // A resume whose `at_us` is out of range is treated as absent rather
            // than errored — the walk simply starts at the window edge.
            resume: request.resume.and_then(|resume| {
                Timestamp::from_micros(resume.at_us).map(|at| ResumeAt {
                    at,
                    skip: resume.skip,
                })
            }),
            max_chunks: clamp_max_chunks(request.max_chunks),
            max_bytes: clamp_max_bytes(request.max_bytes),
        };

        match store.log_page(&job, &attempt, &query).await {
            Ok(page) => {
                let bytes_served: u64 = page.chunks.iter().map(|c| c.bytes.len() as u64).sum();
                metrics::counter!(AGENT_NODE_SERVICE_BYTES_SERVED_TOTAL).increment(bytes_served);
                Ok(Response::new(chunks_response(page)))
            }
            Err(StoreError::UnknownAttempt { .. }) => Ok(Response::new(unknown_attempt())),
            // A real storage fault is not a "gone" verdict — surface it as an
            // error so the coordinator records `unreachable`, not `expired`.
            Err(err @ (StoreError::Io(_) | StoreError::Sql(_))) => {
                tracing::warn!(%job, %attempt, error = %err, "FetchLogs store read failed");
                Err(Status::internal("telemetry store read failed"))
            }
        }
    }
}

/// The `UnknownAttempt` response arm, also bumping its counter.
fn unknown_attempt() -> pb::FetchLogsResponse {
    metrics::counter!(AGENT_NODE_SERVICE_UNKNOWN_ATTEMPT_TOTAL).increment(1);
    pb::FetchLogsResponse {
        outcome: Some(pb::fetch_logs_response::Outcome::UnknownAttempt(
            pb::UnknownAttempt {},
        )),
    }
}

/// Translate a store [`LogPage`](crate::telemetry::LogPage) into the `Chunks`
/// response arm.
fn chunks_response(page: crate::telemetry::LogPage) -> pb::FetchLogsResponse {
    let chunks = page
        .chunks
        .into_iter()
        .map(|chunk| pb::LogChunk {
            at_us: chunk.at.as_micros(),
            stream: pb_stream(chunk.stream) as i32,
            payload: chunk.bytes.to_vec(),
            truncated: chunk.truncated,
        })
        .collect();
    pb::FetchLogsResponse {
        outcome: Some(pb::fetch_logs_response::Outcome::Chunks(pb::Chunks {
            chunks,
            exhausted: page.exhausted,
            earliest_at_us: page.earliest_at.map(Timestamp::as_micros),
            latest_at_us: page.latest_at.map(Timestamp::as_micros),
        })),
    }
}

/// Map the request-side stream filter: the unspecified zero value (and any
/// unknown value) selects both streams.
fn request_stream(raw: i32) -> Option<LogStream> {
    match pb::LogStream::try_from(raw) {
        Ok(pb::LogStream::Stdout) => Some(LogStream::Stdout),
        Ok(pb::LogStream::Stderr) => Some(LogStream::Stderr),
        _ => None,
    }
}

/// Map a stored stream to its wire enum. `Unspecified` is never produced —
/// stored chunks always carry a concrete stream.
fn pb_stream(stream: LogStream) -> pb::LogStream {
    match stream {
        LogStream::Stdout => pb::LogStream::Stdout,
        LogStream::Stderr => pb::LogStream::Stderr,
    }
}

/// Clamp `max_chunks` into `1..=MAX_CHUNKS_CEILING`; `0` clamps up so a request
/// always makes progress.
fn clamp_max_chunks(requested: u32) -> usize {
    let requested = requested as usize;
    if requested == 0 {
        MAX_CHUNKS_CEILING
    } else {
        requested.min(MAX_CHUNKS_CEILING)
    }
}

/// Clamp `max_bytes` into `1..=MAX_BYTES_CEILING`; `0` clamps up so a request
/// always makes progress.
fn clamp_max_bytes(requested: u32) -> u64 {
    let requested = requested as u64;
    if requested == 0 {
        MAX_BYTES_CEILING
    } else {
        requested.min(MAX_BYTES_CEILING)
    }
}
