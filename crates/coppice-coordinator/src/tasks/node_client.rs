//! The replica-local log-fetch client (ADR 0034).
//!
//! Backs `ControlPlane::fetch_logs`: dials an agent's advertised
//! `NodeService` endpoint as an ordinary mTLS gRPC client and reads one bounded
//! page of an attempt's logs. Every replica makes this call identically — there
//! is **no leadership gating** — so log traffic load-balances across the
//! cluster instead of concentrating on the leader.
//!
//! Identity mirrors the raft mesh (`coppice_consensus::net::client`): the
//! coordinator's own leaf is the client certificate, the cluster CA is the
//! trust root, and the TLS server-name is pinned to the target's typed node id
//! (`node-<uuid>`) rather than its network host — a stolen advertised address
//! is useless without the node's key, since the dial fails closed on the SAN
//! mismatch. A small per-`(node, addr)` channel cache avoids redialing; a
//! re-registration under a new address drops the stale channel, and any RPC
//! error evicts it. A per-node semaphore caps in-flight fetches so one hot job
//! or one slow agent cannot pile up connections.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use coppice_api::{
    LogChunk, LogFetchError, LogFetchOutcome, LogFetchRequest, LogPage, LogStreamSelector,
};
use coppice_core::id::NodeId;
use coppice_net::node_service::Client;
use coppice_proto::pb::agent::v1 as apb;

/// Whole-fetch deadline. Bounds the entire fetch — waiting for a per-node
/// permit, the dial, and the RPC together — so a slow, gone, or *saturated*
/// agent surfaces as `Unreachable` within this bound rather than wedging a
/// request (ADR 0034). Bounding only the RPC would let a request queue for one
/// full deadline batch behind *each* slow call already holding a permit, so a
/// hot or slow node could retain API tasks far past the deadline and break the
/// isolation guarantee.
const RPC_DEADLINE: Duration = Duration::from_secs(5);

/// Maximum concurrent in-flight fetches per node (ADR 0034's per-node cap).
const PER_NODE_INFLIGHT: usize = 2;

const FETCHES: &str = "coordinator_node_log_fetches_total";
const TIMEOUTS: &str = "coordinator_node_log_fetch_timeouts_total";
const UNREACHABLE: &str = "coordinator_node_log_fetch_unreachable_total";
const UNKNOWN_ATTEMPT: &str = "coordinator_node_log_unknown_attempt_total";
const BYTES_FETCHED: &str = "coordinator_node_log_bytes_fetched_total";

/// Register the log-fetch counters (wired into `crate::describe_metrics`).
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        FETCHES,
        "FetchLogs RPCs attempted against agent node services."
    );
    metrics::describe_counter!(
        TIMEOUTS,
        "FetchLogs RPCs that failed with a deadline/timeout."
    );
    metrics::describe_counter!(
        UNREACHABLE,
        "FetchLogs RPCs that could not reach the node (dial failure or non-timeout error)."
    );
    metrics::describe_counter!(
        UNKNOWN_ATTEMPT,
        "FetchLogs answers reporting the attempt's logs are gone (UnknownAttempt)."
    );
    metrics::describe_counter!(BYTES_FETCHED, "Chunk payload bytes returned by FetchLogs.");
}

/// No point-in-time sampling behind the log-fetch counters; they are pushed as
/// RPCs resolve.
pub(crate) fn gather_metrics() {}

/// Dials agents' `NodeService` listeners to fetch job logs.
pub struct NodeLogClient {
    /// The mTLS client config sans server-name; the SNI/verification name is
    /// stamped per dial with the target node's typed id.
    tls: ClientTlsConfig,
    /// Per-node `(dialed address, channel)`. A re-registration at a new address
    /// drops the stale channel and redials.
    channels: Mutex<HashMap<NodeId, (String, Channel)>>,
    /// Per-node in-flight limiter.
    semaphores: Mutex<HashMap<NodeId, Arc<Semaphore>>>,
    /// The whole-fetch deadline (permit wait + dial + RPC). A field, not the
    /// bare const, so a test can shrink it to make the saturation bound fast.
    deadline: Duration,
}

impl NodeLogClient {
    /// Build from the coordinator's mTLS material (ADR 0011): its own leaf as
    /// client identity, the cluster CA as the trust root.
    pub fn new(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Self {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(cert_pem, key_pem));
        NodeLogClient {
            tls,
            channels: Mutex::new(HashMap::new()),
            semaphores: Mutex::new(HashMap::new()),
            deadline: RPC_DEADLINE,
        }
    }

    /// Fetch one page of `attempt`'s logs from `node` at `addr`.
    ///
    /// The **whole** fetch — waiting for a per-node permit, the dial, and the
    /// RPC — is bounded by a single [`Self::deadline`]. A request that queues
    /// behind a saturated per-node permit set therefore surfaces as
    /// `Unreachable` at the deadline rather than sitting for one full deadline
    /// batch per slow call ahead of it, which would let a hot or slow node
    /// retain API tasks well past the bound (ADR 0034 isolation guarantee).
    pub async fn fetch_logs(
        &self,
        node: NodeId,
        addr: &str,
        req: LogFetchRequest,
    ) -> Result<LogFetchOutcome, LogFetchError> {
        metrics::counter!(FETCHES).increment(1);

        // `acquired` flips true the instant a permit is in hand, so a deadline
        // expiry can name its cause: still false ⇒ we timed out *queueing* for a
        // permit (the node is saturated); true ⇒ the dial/RPC itself overran.
        let acquired = Arc::new(AtomicBool::new(false));
        match tokio::time::timeout(
            self.deadline,
            self.fetch_guarded(node, addr, req, Arc::clone(&acquired)),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                metrics::counter!(TIMEOUTS).increment(1);
                let reason = if acquired.load(Ordering::Acquire) {
                    format!(
                        "fetching logs from node {node} at {addr} exceeded the {:?} \
                         deadline (dial/RPC unresponsive)",
                        self.deadline
                    )
                } else {
                    // The permit was never granted: both in-flight slots stayed
                    // held by slow calls for the whole deadline.
                    format!(
                        "node {node} at {addr} saturated: all {PER_NODE_INFLIGHT} in-flight \
                         log-fetch slots stayed busy past the {:?} deadline",
                        self.deadline
                    )
                };
                Err(LogFetchError::Unreachable { reason })
            }
        }
    }

    /// The permit-guarded fetch body, run *inside* the whole-fetch deadline. It
    /// waits for a per-node permit, dials (cached), and makes the one RPC;
    /// `acquired` is flipped the moment the permit is granted so the caller can
    /// distinguish a queue-wait timeout from a dial/RPC timeout.
    async fn fetch_guarded(
        &self,
        node: NodeId,
        addr: &str,
        req: LogFetchRequest,
        acquired: Arc<AtomicBool>,
    ) -> Result<LogFetchOutcome, LogFetchError> {
        // Bound concurrent fetches to one node. `acquire` only errors if the
        // semaphore is closed, which never happens (it lives as long as self).
        let permit = self.semaphore(node);
        let _permit = permit
            .acquire()
            .await
            .expect("per-node log-fetch semaphore is never closed");
        acquired.store(true, Ordering::Release);

        let channel = self.channel_for(node, addr).map_err(|reason| {
            metrics::counter!(UNREACHABLE).increment(1);
            LogFetchError::Unreachable { reason }
        })?;

        let mut client = Client::new(channel);
        let pb_req = request_to_pb(req);
        match client.fetch_logs(tonic::Request::new(pb_req)).await {
            Ok(response) => {
                let outcome = response_from_pb(response.into_inner())?;
                if let LogFetchOutcome::Chunks(page) = &outcome {
                    let bytes: usize = page.chunks.iter().map(|c| c.payload.len()).sum();
                    metrics::counter!(BYTES_FETCHED).increment(bytes as u64);
                } else {
                    metrics::counter!(UNKNOWN_ATTEMPT).increment(1);
                }
                Ok(outcome)
            }
            Err(status) => {
                // Any RPC failure evicts the channel so the next call redials —
                // a re-registered agent, or one that just came back, is not
                // stuck behind a dead connection.
                self.evict(node);
                let timed_out = status.code() == tonic::Code::DeadlineExceeded;
                if timed_out {
                    metrics::counter!(TIMEOUTS).increment(1);
                } else {
                    metrics::counter!(UNREACHABLE).increment(1);
                }
                Err(LogFetchError::Unreachable {
                    reason: format!(
                        "fetching logs from node {node} at {addr} failed ({:?}): {}",
                        status.code(),
                        status.message()
                    ),
                })
            }
        }
    }

    /// The cached channel for `node`, redialing if its address changed or it
    /// was never dialed. The dial is lazy (`connect_lazy`), so a bad address
    /// surfaces on the RPC as an `Unreachable` status rather than here.
    fn channel_for(&self, node: NodeId, addr: &str) -> Result<Channel, String> {
        let mut map = self
            .channels
            .lock()
            .expect("log-fetch channel map poisoned");
        if let Some((existing, channel)) = map.get(&node) {
            if existing == addr {
                return Ok(channel.clone());
            }
        }
        let channel = build_channel(&self.tls, addr, &node.to_string(), self.deadline)
            .map_err(|e| format!("cannot dial node {node} at {addr}: {e}"))?;
        map.insert(node, (addr.to_string(), channel.clone()));
        Ok(channel)
    }

    fn evict(&self, node: NodeId) {
        self.channels
            .lock()
            .expect("log-fetch channel map poisoned")
            .remove(&node);
    }

    fn semaphore(&self, node: NodeId) -> Arc<Semaphore> {
        self.semaphores
            .lock()
            .expect("log-fetch semaphore map poisoned")
            .entry(node)
            .or_insert_with(|| Arc::new(Semaphore::new(PER_NODE_INFLIGHT)))
            .clone()
    }

    /// Test hook: shrink the whole-fetch deadline so a saturation/timeout test
    /// can assert its wall-clock bound quickly.
    #[cfg(test)]
    fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Test hook: install a pre-built channel for `(node, addr)`, so a test can
    /// point the client at a plaintext in-process server without the mTLS dial.
    #[cfg(test)]
    fn insert_channel(&self, node: NodeId, addr: &str, channel: Channel) {
        self.channels
            .lock()
            .expect("log-fetch channel map poisoned")
            .insert(node, (addr.to_string(), channel));
    }
}

/// Construct a lazily-connecting mTLS channel to `addr` (`host:port`), pinning
/// the TLS server-name to `server_name` — the target's typed node id, matching
/// the agent leaf's `node-<uuid>` dNSName SAN (ADR 0034). Mirrors
/// `coppice_consensus::net::client::build_channel`, differing only in that the
/// verified name is the node identity, not the dial host.
fn build_channel(
    tls: &ClientTlsConfig,
    addr: &str,
    server_name: &str,
    deadline: Duration,
) -> Result<Channel, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(format!("https://{addr}"))?
        .tls_config(tls.clone().domain_name(server_name.to_string()))?
        .connect_timeout(deadline)
        .timeout(deadline);
    Ok(endpoint.connect_lazy())
}

/// Convert the transport-neutral seam request into the wire message.
fn request_to_pb(req: LogFetchRequest) -> apb::FetchLogsRequest {
    apb::FetchLogsRequest {
        job: Some(req.job.into()),
        attempt: Some(req.attempt.into()),
        from_us: req.from_us,
        until_us: req.until_us,
        stream: pb_stream(req.stream) as i32,
        resume: req.resume.map(|r| apb::ResumePosition {
            at_us: r.at_us,
            skip: r.skip,
        }),
        ascending: req.ascending,
        max_chunks: req.max_chunks,
        max_bytes: req.max_bytes,
    }
}

/// The wire stream filter for a selector; `None` selects both streams
/// (`LOG_STREAM_UNSPECIFIED`).
fn pb_stream(selector: Option<LogStreamSelector>) -> apb::LogStream {
    match selector {
        None => apb::LogStream::Unspecified,
        Some(LogStreamSelector::Stdout) => apb::LogStream::Stdout,
        Some(LogStreamSelector::Stderr) => apb::LogStream::Stderr,
    }
}

/// Convert the wire response into the seam outcome. A missing `oneof` arm is a
/// malformed answer, surfaced as `Unreachable`.
fn response_from_pb(resp: apb::FetchLogsResponse) -> Result<LogFetchOutcome, LogFetchError> {
    match resp.outcome {
        Some(apb::fetch_logs_response::Outcome::Chunks(chunks)) => {
            Ok(LogFetchOutcome::Chunks(LogPage {
                chunks: chunks
                    .chunks
                    .into_iter()
                    .map(|c| LogChunk {
                        at_us: c.at_us,
                        stream: chunk_stream(c.stream),
                        payload: c.payload,
                        truncated: c.truncated,
                    })
                    .collect(),
                exhausted: chunks.exhausted,
                earliest_at_us: chunks.earliest_at_us,
                latest_at_us: chunks.latest_at_us,
            }))
        }
        Some(apb::fetch_logs_response::Outcome::UnknownAttempt(_)) => {
            Ok(LogFetchOutcome::UnknownAttempt)
        }
        None => Err(LogFetchError::Unreachable {
            reason: "agent returned a FetchLogs response with no outcome".to_string(),
        }),
    }
}

/// A returned chunk's stream; the wire never sets `UNSPECIFIED` on a chunk, so
/// anything but `STDERR` reads as `STDOUT`.
fn chunk_stream(stream: i32) -> LogStreamSelector {
    if stream == apb::LogStream::Stderr as i32 {
        LogStreamSelector::Stderr
    } else {
        LogStreamSelector::Stdout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::time::Instant;

    use coppice_core::id::{AttemptId, JobId};
    use coppice_net::node_service::{NodeService, Server as NodeServiceServer};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    // ---- pure conversion round-trips ------------------------------------

    #[test]
    fn request_maps_every_field_to_the_wire() {
        let job = JobId::new();
        let attempt = AttemptId::new();
        let pb = request_to_pb(LogFetchRequest {
            job,
            attempt,
            from_us: Some(10),
            until_us: Some(99),
            stream: Some(LogStreamSelector::Stderr),
            resume: Some(coppice_api::LogResumePosition { at_us: 42, skip: 3 }),
            ascending: true,
            max_chunks: 7,
            max_bytes: 4096,
        });
        assert_eq!(JobId::try_from(pb.job.unwrap()).unwrap(), job);
        assert_eq!(AttemptId::try_from(pb.attempt.unwrap()).unwrap(), attempt);
        assert_eq!(pb.from_us, Some(10));
        assert_eq!(pb.until_us, Some(99));
        assert_eq!(pb.stream, apb::LogStream::Stderr as i32);
        assert_eq!(pb.resume.as_ref().unwrap().at_us, 42);
        assert_eq!(pb.resume.as_ref().unwrap().skip, 3);
        assert!(pb.ascending);
        assert_eq!(pb.max_chunks, 7);
        assert_eq!(pb.max_bytes, 4096);
    }

    #[test]
    fn absent_stream_filter_is_unspecified() {
        assert_eq!(pb_stream(None), apb::LogStream::Unspecified);
    }

    #[test]
    fn chunks_response_decodes_to_a_page() {
        let resp = apb::FetchLogsResponse {
            outcome: Some(apb::fetch_logs_response::Outcome::Chunks(apb::Chunks {
                chunks: vec![
                    apb::LogChunk {
                        at_us: 1,
                        stream: apb::LogStream::Stdout as i32,
                        payload: b"hello".to_vec(),
                        truncated: false,
                    },
                    apb::LogChunk {
                        at_us: 2,
                        stream: apb::LogStream::Stderr as i32,
                        payload: b"world".to_vec(),
                        truncated: false,
                    },
                ],
                exhausted: false,
                earliest_at_us: Some(1),
                latest_at_us: Some(2),
            })),
        };
        let LogFetchOutcome::Chunks(page) = response_from_pb(resp).unwrap() else {
            panic!("expected chunks");
        };
        assert_eq!(page.chunks.len(), 2);
        assert_eq!(page.chunks[0].stream, LogStreamSelector::Stdout);
        assert_eq!(page.chunks[1].stream, LogStreamSelector::Stderr);
        assert!(!page.exhausted);
        assert_eq!(page.earliest_at_us, Some(1));
    }

    #[test]
    fn unknown_attempt_response_decodes() {
        let resp = apb::FetchLogsResponse {
            outcome: Some(apb::fetch_logs_response::Outcome::UnknownAttempt(
                apb::UnknownAttempt {},
            )),
        };
        assert_eq!(
            response_from_pb(resp).unwrap(),
            LogFetchOutcome::UnknownAttempt
        );
    }

    #[test]
    fn empty_outcome_is_unreachable() {
        let resp = apb::FetchLogsResponse { outcome: None };
        assert!(matches!(
            response_from_pb(resp),
            Err(LogFetchError::Unreachable { .. })
        ));
    }

    // ---- channel cache keying ------------------------------------------
    //
    // These use the empty-PEM client whose TLS material cannot build a real
    // channel — which is exactly what makes the address-change assertion
    // sharp: a matching address returns the seeded (plaintext) channel via the
    // cache-hit path, while a changed address forces a *rebuild* that fails on
    // the fake TLS, proving the stale channel was not reused.

    /// A lazily-connecting plaintext channel to a never-dialed address, to seed
    /// the cache with.
    fn dummy_channel() -> Channel {
        Endpoint::from_shared("http://127.0.0.1:1".to_string())
            .unwrap()
            .connect_lazy()
    }

    #[tokio::test]
    async fn same_address_reuses_the_cached_channel() {
        let client = NodeLogClient::new(b"", b"", b"");
        let node = NodeId::new();
        client.insert_channel(node, "10.0.0.1:9100", dummy_channel());
        // Cache hit: returns without a rebuild (a rebuild would fail on the
        // empty TLS material).
        assert!(client.channel_for(node, "10.0.0.1:9100").is_ok());
        assert_eq!(
            client.channels.lock().unwrap().get(&node).unwrap().0,
            "10.0.0.1:9100"
        );
    }

    #[tokio::test]
    async fn changed_address_does_not_reuse_the_stale_channel() {
        let client = NodeLogClient::new(b"", b"", b"");
        let node = NodeId::new();
        client.insert_channel(node, "10.0.0.1:9100", dummy_channel());
        // A re-registration at a new address must not reuse the stale channel:
        // `channel_for` redials, which here fails on the fake TLS — the proof
        // it did not hand back the cached channel keyed at the old address.
        assert!(client.channel_for(node, "10.0.0.2:9100").is_err());
    }

    #[tokio::test]
    async fn eviction_drops_the_cached_channel() {
        let client = NodeLogClient::new(b"", b"", b"");
        let node = NodeId::new();
        client.insert_channel(node, "10.0.0.1:9100", dummy_channel());
        assert!(client.channels.lock().unwrap().contains_key(&node));
        client.evict(node);
        assert!(!client.channels.lock().unwrap().contains_key(&node));
    }

    // ---- saturation: permit queueing counts against the deadline -----------

    /// When both per-node permits are held by slow in-flight calls, a further
    /// fetch must NOT queue behind them indefinitely: the whole-fetch deadline
    /// (permit wait included) fires and surfaces `Unreachable` naming
    /// saturation. Both permits are held for the whole test — directly, so the
    /// third fetch deterministically times out *queueing* (it never dials).
    #[tokio::test]
    async fn saturated_node_times_out_the_whole_fetch() {
        // A short deadline keeps the wall-clock assertion fast in CI.
        let deadline = Duration::from_millis(300);
        let client = NodeLogClient::new(b"", b"", b"").with_deadline(deadline);
        let node = NodeId::new();

        // Hold BOTH in-flight slots for the whole test (two slow fetches).
        let sem = client.semaphore(node);
        let _p1 = Arc::clone(&sem).acquire_owned().await.unwrap();
        let _p2 = sem.acquire_owned().await.unwrap();

        let addr = "10.0.0.9:9100";
        let start = Instant::now();
        let result = client
            .fetch_logs(
                node,
                addr,
                LogFetchRequest {
                    job: JobId::new(),
                    attempt: AttemptId::new(),
                    from_us: None,
                    until_us: None,
                    stream: None,
                    resume: None,
                    ascending: false,
                    max_chunks: 10,
                    max_bytes: 4096,
                },
            )
            .await;
        let elapsed = start.elapsed();

        match result {
            Err(LogFetchError::Unreachable { reason }) => assert!(
                reason.contains("saturated"),
                "the queue-wait timeout must name saturation, got: {reason}"
            ),
            other => panic!("expected Unreachable, got {other:?}"),
        }
        // It waited out the deadline (did not resolve early) but resolved near
        // it (did not queue indefinitely behind the held permits).
        assert!(
            elapsed >= deadline,
            "must wait out the whole-fetch deadline, waited {elapsed:?}"
        );
        assert!(
            elapsed < deadline * 6,
            "must resolve near the deadline, not queue indefinitely, waited {elapsed:?}"
        );
    }

    // ---- end-to-end dial against a plaintext in-process server ----------
    //
    // A full mTLS server would need node-id-SAN leaves the dev PKI mints but
    // the unit-test harness does not; the ADR permits testing the dial/convert
    // path over plaintext when the TLS plumbing is impractical here. We seed the
    // channel cache with a plaintext channel so `fetch_logs` exercises the real
    // request/response conversion and the semaphore path against a live tonic
    // `NodeService`.

    /// A stub `NodeService` that answers with whatever it was seeded with.
    struct StubNode {
        outcome: apb::FetchLogsResponse,
        seen: Arc<Mutex<Vec<apb::FetchLogsRequest>>>,
    }

    #[tonic::async_trait]
    impl NodeService for StubNode {
        async fn fetch_logs(
            &self,
            request: Request<apb::FetchLogsRequest>,
        ) -> Result<Response<apb::FetchLogsResponse>, Status> {
            self.seen.lock().unwrap().push(request.into_inner());
            Ok(Response::new(self.outcome.clone()))
        }
    }

    async fn spawn_stub(
        outcome: apb::FetchLogsResponse,
    ) -> (SocketAddr, Arc<Mutex<Vec<apb::FetchLogsRequest>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let service = StubNode {
            outcome,
            seen: seen.clone(),
        };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (addr, seen)
    }

    fn plaintext_channel(addr: SocketAddr) -> Channel {
        Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy()
    }

    #[tokio::test]
    async fn fetch_logs_round_trips_chunks_over_the_wire() {
        let outcome = apb::FetchLogsResponse {
            outcome: Some(apb::fetch_logs_response::Outcome::Chunks(apb::Chunks {
                chunks: vec![apb::LogChunk {
                    at_us: 5,
                    stream: apb::LogStream::Stdout as i32,
                    payload: b"line".to_vec(),
                    truncated: false,
                }],
                exhausted: true,
                earliest_at_us: Some(5),
                latest_at_us: Some(5),
            })),
        };
        let (addr, seen) = spawn_stub(outcome).await;

        let client = NodeLogClient::new(b"", b"", b"");
        let node = NodeId::new();
        let addr_str = addr.to_string();
        client.insert_channel(node, &addr_str, plaintext_channel(addr));

        let job = JobId::new();
        let attempt = AttemptId::new();
        let result = client
            .fetch_logs(
                node,
                &addr_str,
                LogFetchRequest {
                    job,
                    attempt,
                    from_us: None,
                    until_us: None,
                    stream: None,
                    resume: None,
                    ascending: false,
                    max_chunks: 200,
                    max_bytes: 4096,
                },
            )
            .await
            .expect("fetch");

        let LogFetchOutcome::Chunks(page) = result else {
            panic!("expected chunks");
        };
        assert_eq!(page.chunks.len(), 1);
        assert_eq!(page.chunks[0].payload, b"line");
        assert!(page.exhausted);
        // The server saw the converted request with our ids.
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(JobId::try_from(seen[0].job.clone().unwrap()).unwrap(), job);
    }

    #[tokio::test]
    async fn fetch_logs_maps_unknown_attempt() {
        let outcome = apb::FetchLogsResponse {
            outcome: Some(apb::fetch_logs_response::Outcome::UnknownAttempt(
                apb::UnknownAttempt {},
            )),
        };
        let (addr, _seen) = spawn_stub(outcome).await;
        let client = NodeLogClient::new(b"", b"", b"");
        let node = NodeId::new();
        let addr_str = addr.to_string();
        client.insert_channel(node, &addr_str, plaintext_channel(addr));

        let result = client
            .fetch_logs(
                node,
                &addr_str,
                LogFetchRequest {
                    job: JobId::new(),
                    attempt: AttemptId::new(),
                    from_us: None,
                    until_us: None,
                    stream: None,
                    resume: None,
                    ascending: false,
                    max_chunks: 10,
                    max_bytes: 4096,
                },
            )
            .await
            .expect("fetch");
        assert_eq!(result, LogFetchOutcome::UnknownAttempt);
    }
}
