//! Wire-path tests for the job-event subscription (ADR 0043).
//!
//! The server here is a hand-written HTTP/1.1 responder on a loopback socket
//! rather than an axum router, for one reason: every property worth pinning
//! down about a long-lived `text/event-stream` response is a property of
//! *timing and framing* — when bytes arrive, where a chunk boundary falls,
//! when the body ends — and a framework that owns the body is exactly what
//! takes those away. So each test scripts its connections by hand: what the
//! client sent is recorded, and what it gets back is written byte for byte.
//!
//! Nothing here mocks the client. It is the real request path — the rate
//! limiter, the credential, the query encoding, the `Last-Event-ID` header —
//! and the real reconnect loop.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use coppice_client::{
    Client, Error, ErrorCode, JobEventItem, JobFilter, JobPhase, JobWatchItem, SnapshotScope,
    WatchOptions,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One request head the canned server saw.
#[derive(Debug, Clone)]
struct Recorded {
    path: String,
    query: String,
    headers: HashMap<String, String>,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

/// The write half of one accepted connection, with the few moves a test
/// needs: answer it and be done, or open a stream and dribble frames into it.
struct Conn {
    stream: TcpStream,
}

impl Conn {
    /// Answer with one complete response and close. `body` is sent as JSON.
    async fn respond(self, status: u16, body: &str) {
        self.respond_at(status, body, 100).await;
    }

    /// Like [`respond`](Self::respond), with the `Coppice-Applied-Index` a
    /// test wants rather than the fixed `100`.
    async fn respond_at(mut self, status: u16, body: &str, applied_index: u64) {
        let head = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Coppice-Applied-Index: {applied_index}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        self.stream.write_all(head.as_bytes()).await.unwrap();
        self.stream.write_all(body.as_bytes()).await.unwrap();
        self.close().await;
    }

    /// Begin a `text/event-stream` response: no `Content-Length`, so the body
    /// runs until the connection closes — which is exactly the shape ADR 0043
    /// describes, and what makes [`Conn::close`] a clean stream ending.
    async fn open_stream(&mut self) {
        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                    Cache-Control: no-cache\r\nConnection: close\r\n\r\n";
        self.stream.write_all(head.as_bytes()).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Write raw body bytes. Tests pass whole frames, half frames, or a
    /// single byte — that is the point of taking bytes rather than a frame.
    async fn write(&mut self, raw: &str) {
        self.stream.write_all(raw.as_bytes()).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Like [`write`](Self::write), but a write failing (the peer having
    /// already closed its side) is not this test's problem: a handler that
    /// loops writing on a timer keeps going until the client gives up and
    /// drops the connection out from under it.
    async fn write_best_effort(&mut self, raw: &str) {
        if self.stream.write_all(raw.as_bytes()).await.is_ok() {
            let _ = self.stream.flush().await;
        }
    }

    /// A `batch` frame carrying one `job_submitted` event.
    async fn batch(&mut self, index: u64) {
        let job = "job-00000000-0000-0000-0000-000000000001";
        self.batch_for(index, &[job]).await;
    }

    /// A `batch` frame with one `job_submitted` event per job, ordinals
    /// ascending from zero — enough to watch the per-job suppression rule
    /// pick a batch apart.
    async fn batch_for(&mut self, index: u64, jobs: &[&str]) {
        let events: Vec<String> = jobs
            .iter()
            .enumerate()
            .map(|(ordinal, job)| {
                format!(
                    "{{\"index\":{index},\"ordinal\":{ordinal},\
                     \"at\":\"1970-01-01T00:00:01.000000Z\",\
                     \"kind\":\"job_submitted\",\"job\":\"{job}\"}}"
                )
            })
            .collect();
        self.write(&format!(
            "event: batch\nid: {index}\ndata: {{\"index\":{index},\
             \"at\":\"1970-01-01T00:00:01.000000Z\",\"events\":[{}]}}\n\n",
            events.join(",")
        ))
        .await;
    }

    /// A `progress` bookmark.
    async fn progress(&mut self, index: u64) {
        self.write(&format!(
            "event: progress\nid: {index}\ndata: {{\"index\":{index}}}\n\n"
        ))
        .await;
    }

    /// A `gap`, which deliberately carries no id.
    async fn gap(&mut self, earliest: u64) {
        self.write(&format!(
            "event: gap\ndata: {{\"earliest_available\":{earliest}}}\n\n"
        ))
        .await;
    }

    /// End the body cleanly — the server drain, or a credential expiring.
    async fn close(mut self) {
        self.stream.shutdown().await.ok();
    }
}

type Handler = Arc<
    dyn Fn(usize, Recorded, Conn) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
>;

/// A canned server: every accepted connection is handed to `handler` with its
/// zero-based sequence number, so a test can script "the first connection does
/// this, the second that".
struct Server {
    base: String,
    seen: Arc<Mutex<Vec<Recorded>>>,
}

impl Server {
    /// Every request head the server read, in arrival order.
    fn seen(&self) -> Vec<Recorded> {
        self.seen.lock().unwrap().clone()
    }
}

async fn spawn<F, Fut>(handler: F) -> Server
where
    F: Fn(usize, Recorded, Conn) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handler: Handler = Arc::new(move |n, req, conn| Box::pin(handler(n, req, conn)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::clone(&seen);
    tokio::spawn(async move {
        let count = AtomicUsize::new(0);
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let Some(request) = read_head(&mut stream).await else {
                continue;
            };
            let n = count.fetch_add(1, Ordering::SeqCst);
            store.lock().unwrap().push(request.clone());
            let handler = Arc::clone(&handler);
            tokio::spawn(async move { handler(n, request, Conn { stream }).await });
        }
    });
    Server {
        base: format!("http://{addr}"),
        seen,
    }
}

/// Read one request head — up to the blank line — and parse what the tests
/// assert on. `None` if the peer went away first.
async fn read_head(stream: &mut TcpStream) -> Option<Recorded> {
    let mut buf = Vec::new();
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            let mut lines = head.lines();
            let start = lines.next()?;
            let target = start.split_whitespace().nth(1)?;
            let (path, query) = match target.split_once('?') {
                Some((path, query)) => (path.to_string(), query.to_string()),
                None => (target.to_string(), String::new()),
            };
            let headers = lines
                .filter_map(|line| line.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                .collect();
            return Some(Recorded {
                path,
                query,
                headers,
            });
        }
        let mut chunk = [0u8; 1024];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// The filter every test subscribes with — an identity leaf, so nothing is
/// refused locally.
fn owned() -> JobFilter {
    JobFilter::metadata_equals("owner", "batch-service")
}

/// A client with the rate limiter off: these tests assert on reconnect
/// behaviour, and a one-second cell would be indistinguishable from a backoff.
fn client(server: &Server) -> Client {
    Client::builder(&server.base)
        .no_rate_limit()
        .build()
        .unwrap()
}

/// The backoff bounds a reconnect test wants: present, so the path is
/// exercised, but not worth waiting for.
fn brisk() -> WatchOptions {
    WatchOptions::new()
        .with_min_backoff(Duration::from_millis(10))
        .with_max_backoff(Duration::from_millis(50))
}

/// An empty job page, for the list half of `watch_jobs`.
const EMPTY_PAGE: &str = r#"{"jobs":[],"next_cursor":null}"#;

/// One `JobSummary` row, for the pages a snapshot test wants rows on. Only
/// the id matters to anything here; the rest is a valid row.
fn row(job: &str) -> String {
    format!(
        r#"{{"id":"{job}","state":"queued","attempt":null,"image":"alpine:3",
           "quota_entity":"quota-00000000-0000-0000-0000-000000000001",
           "quota_entity_name":"team","priority":0,
           "submitted_at":"1970-01-01T00:00:01.000000Z","submitted_by":null,
           "terminal_at":null,"node":null,"attempt_state":null,
           "funding_fraction":null,"cost_ucu":0,"outcome":null,"metadata":{{}}}}"#
    )
}

/// A page carrying `jobs`, continuing iff `next` is set.
fn page(jobs: &[&str], next: Option<&str>) -> String {
    let rows: Vec<String> = jobs.iter().map(|job| row(job)).collect();
    let cursor = match next {
        Some(cursor) => format!("\"{cursor}\""),
        None => "null".to_string(),
    };
    format!(r#"{{"jobs":[{}],"next_cursor":{cursor}}}"#, rows.join(","))
}

/// One query parameter's value, percent-decoding included — the filters ride
/// as JSON in `filter=`/`jobs=`, and asserting on them means decoding them.
fn param(query: &str, name: &str) -> String {
    let raw = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .unwrap_or_else(|| panic!("no `{name}` in {query:?}"))
        .1;
    let bytes = raw.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).expect("a percent escape"));
                i += 3;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out).expect("a UTF-8 parameter")
}

/// The same parameter as a JSON document.
fn json_param(query: &str, name: &str) -> serde_json::Value {
    serde_json::from_str(&param(query, name)).expect("a JSON parameter")
}

/// The phase leaf the `Live` scope adds: every phase but the terminal three.
fn live_phases() -> serde_json::Value {
    serde_json::json!({
        "phase": { "in": [
            "submitted", "accepted", "queued", "accruing", "preparing", "running", "finalizing",
        ] }
    })
}

// ---------------------------------------------------------------------------
// One connection
// ---------------------------------------------------------------------------

/// The three frames become the three items, with their bodies decoded — and
/// the request carries the filter as `jobs=`, which is the parameter's whole
/// contract with the server.
#[tokio::test]
async fn the_three_frames_become_the_three_items() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        conn.progress(4).await;
        conn.batch(5).await;
        conn.gap(3).await;
        conn.close().await;
    })
    .await;

    let mut stream = client(&server)
        .subscribe_job_events(&owned(), None)
        .await
        .expect("the subscription opens");

    assert_eq!(
        stream.next_item().await.unwrap(),
        Some(JobEventItem::Progress { index: 4 })
    );
    let Some(JobEventItem::Batch(batch)) = stream.next_item().await.unwrap() else {
        panic!("a batch frame");
    };
    assert_eq!(batch.index, 5);
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].ordinal, 0);
    assert_eq!(
        stream.next_item().await.unwrap(),
        Some(JobEventItem::Gap {
            earliest_available: 3
        })
    );
    // The body ended: a clean end, not an error.
    assert_eq!(stream.next_item().await.unwrap(), None);
    // …and the gap did not move the cursor off the last resumable frame.
    assert_eq!(stream.cursor(), Some(5));

    let seen = server.seen();
    assert_eq!(seen[0].path, "/api/v1/events");
    assert!(
        seen[0].query.contains("jobs=") && seen[0].query.contains("owner"),
        "the filter rides in `jobs=`: {:?}",
        seen[0].query
    );
    assert_eq!(seen[0].header("last-event-id"), None);
}

/// A frame split across arbitrary write boundaries — and an event name this
/// client has never heard of — must not disturb the stream. The unknown name
/// is skipped rather than refused, so a fourth frame in a later server does
/// not break a subscription to the three that exist.
#[tokio::test]
async fn a_split_frame_survives_and_an_unknown_event_name_is_skipped() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        conn.write("event: rumour\ndata: {\"who\":\"knows\"}\n\n: keep-alive\n\nevent: pro")
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        conn.write("gress\nid: 12\ndata: {\"ind").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        conn.write("ex\":12}\n\n").await;
        conn.close().await;
    })
    .await;

    let mut stream = client(&server)
        .subscribe_job_events(&owned(), None)
        .await
        .unwrap();
    assert_eq!(
        stream.next_item().await.unwrap(),
        Some(JobEventItem::Progress { index: 12 })
    );
    assert_eq!(stream.next_item().await.unwrap(), None);
}

/// The cursor a caller supplies goes out as `Last-Event-ID`, which is the
/// header the server honours ahead of `?cursor=`.
#[tokio::test]
async fn an_opening_cursor_is_sent_as_the_last_event_id() {
    let server = spawn(|_, _, conn: Conn| async move { conn.close().await }).await;
    let _ = client(&server)
        .subscribe_job_events(&owned(), Some(41))
        .await;
    assert_eq!(server.seen()[0].header("last-event-id"), Some("41"));
}

/// A filter naming a leaf a subscription cannot answer is refused here, before
/// a socket is opened: the base below is unroutable, so reaching the network
/// would surface as a transport error instead.
#[tokio::test]
async fn a_forbidden_leaf_is_refused_before_any_request() {
    let client = Client::new("http://127.0.0.1:1").unwrap();
    let filter = JobFilter::all([owned(), JobFilter::phase_in([JobPhase::Running])]);

    let err = client
        .subscribe_job_events(&filter, None)
        .await
        .expect_err("a phase leaf");
    assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
    assert!(err.to_string().contains("`phase`"), "{err}");

    // The watchers inherit it by going through the same call.
    let err = client
        .watch_job_events(filter, WatchOptions::new())
        .next_item()
        .await
        .expect_err("a phase leaf");
    assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
}

/// The client's request timeout must not cut a stream that is merely quiet.
/// The timeout here is a fraction of the silence the server keeps before it
/// says anything at all — and that silence (900 ms) is itself well inside the
/// default idle timeout (60 s), so nothing here should trip either bound.
#[tokio::test]
async fn the_request_timeout_does_not_cut_a_quiet_stream() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        conn.progress(3).await;
        conn.close().await;
    })
    .await;

    let client = Client::builder(&server.base)
        .no_rate_limit()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();

    let started = Instant::now();
    let mut stream = client.subscribe_job_events(&owned(), None).await.unwrap();
    assert_eq!(
        stream.next_item().await.unwrap(),
        Some(JobEventItem::Progress { index: 3 })
    );
    assert!(
        started.elapsed() > Duration::from_millis(500),
        "the frame arrived after the timeout would have fired: {:?}",
        started.elapsed()
    );

    // The exemption is the stream's alone: an ordinary read still gives up.
    let err = client
        .job("job-00000000-0000-0000-0000-000000000001".parse().unwrap())
        .await
        .expect_err("an ordinary read against a server that never answers");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
}

/// A connection that never sends another byte — no batch, no bookmark, not
/// even a clean close — is not merely quiet, it is dead: `next_item` must give
/// up once the idle timeout elapses, and having failed once must not fail
/// again out of thin air.
#[tokio::test]
async fn a_silent_connection_fails_with_stream_idle() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        // Never write anything else, and never close: the client alone must
        // notice this connection is not going anywhere.
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut stream = client(&server)
        .subscribe_job_events(&owned(), None)
        .await
        .unwrap()
        .with_idle_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let err = stream.next_item().await.expect_err("no frame ever came");
    assert!(matches!(err, Error::StreamIdle { .. }), "{err:?}");
    assert!(err.to_string().contains("silent"), "{err}");
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "gave up too early: {:?}",
        started.elapsed()
    );

    // The body is gone, same as any other post-`Err` call.
    assert_eq!(stream.next_item().await.unwrap(), None);
}

/// A server that only ever sends SSE comment lines is indistinguishable, to
/// the idle timeout, from one that sends nothing at all: bytes arriving is
/// not the same as a frame being dispatched, and a proxy that turns real
/// keepalives into comments must not be able to hold a dead stream open
/// forever.
#[tokio::test]
async fn comment_only_keepalives_do_not_postpone_the_idle_timeout() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        for _ in 0..40 {
            conn.write_best_effort(": keepalive\n\n").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    let mut stream = client(&server)
        .subscribe_job_events(&owned(), None)
        .await
        .unwrap()
        .with_idle_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let err = stream
        .next_item()
        .await
        .expect_err("a comment is not a frame");
    assert!(matches!(err, Error::StreamIdle { .. }), "{err:?}");
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "gave up too early: {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "a stream of comments must not push the deadline out indefinitely: {:?}",
        started.elapsed()
    );
}

/// A frame that never finishes is exactly as dead as no bytes at all: bytes
/// trickling in on a timer must not repeatedly re-arm the idle deadline just
/// because the connection is technically producing traffic.
#[tokio::test]
async fn a_frame_that_never_completes_does_not_postpone_the_idle_timeout() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        conn.write_best_effort("event: batch\nid: 1\ndata: {\"index\":1")
            .await;
        for _ in 0..40 {
            conn.write_best_effort("x").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    let mut stream = client(&server)
        .subscribe_job_events(&owned(), None)
        .await
        .unwrap()
        .with_idle_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let err = stream
        .next_item()
        .await
        .expect_err("the frame never finishes");
    assert!(matches!(err, Error::StreamIdle { .. }), "{err:?}");
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "gave up too early: {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "a trickling half-frame must not push the deadline out indefinitely: {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Reconnecting
// ---------------------------------------------------------------------------

/// The reconnect contract in one test: a clean ending is resumed from the last
/// id, the replay that resume produces is dropped, and what is genuinely new
/// is delivered exactly once.
#[tokio::test]
async fn a_clean_ending_resumes_from_the_last_id_and_drops_the_replay() {
    let server = spawn(|n, _, mut conn: Conn| async move {
        conn.open_stream().await;
        match n {
            0 => {
                conn.batch(5).await;
                // The credential expired, or the replica drained.
                conn.close().await;
            }
            _ => {
                // The catch-up may re-deliver the cursor's own batch.
                conn.batch(5).await;
                conn.batch(6).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    })
    .await;

    let mut watch = client(&server).watch_job_events(owned(), brisk());

    let Some(JobEventItem::Batch(first)) = watch.next_item().await.unwrap() else {
        panic!("a batch");
    };
    assert_eq!(first.index, 5);

    // Batch 5 arrives again on the new connection and must not be seen twice.
    let Some(JobEventItem::Batch(next)) = watch.next_item().await.unwrap() else {
        panic!("a batch");
    };
    assert_eq!(next.index, 6, "the replayed batch 5 was delivered twice");
    assert_eq!(watch.cursor(), Some(6));

    let seen = server.seen();
    assert_eq!(seen.len(), 2, "one reconnect: {seen:?}");
    assert_eq!(seen[0].header("last-event-id"), None);
    assert_eq!(
        seen[1].header("last-event-id"),
        Some("5"),
        "the resume carries the last id it processed"
    );
}

/// A bookmark is a resumable position too — that is the whole reason it
/// exists. A subscriber to a quiet set reconnects from the progress frame
/// rather than from an ever-staler cursor.
#[tokio::test]
async fn a_progress_bookmark_advances_the_resume_cursor() {
    let server = spawn(|n, _, mut conn: Conn| async move {
        conn.open_stream().await;
        if n == 0 {
            conn.progress(9).await;
            conn.close().await;
        } else {
            // Nothing new below the bookmark, then something above it.
            conn.progress(9).await;
            conn.batch(11).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    })
    .await;

    let mut watch = client(&server).watch_job_events(owned(), brisk());
    assert_eq!(
        watch.next_item().await.unwrap(),
        Some(JobEventItem::Progress { index: 9 })
    );
    // The repeated bookmark says nothing new and is not handed over again.
    let Some(JobEventItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("a batch");
    };
    assert_eq!(batch.index, 11);
    assert_eq!(server.seen()[1].header("last-event-id"), Some("9"));
}

/// A gap is the one thing a reconnect cannot repair, so the watcher hands it
/// to the caller rather than carrying on as if delivery had been continuous.
#[tokio::test]
async fn a_gap_is_surfaced_and_never_swallowed() {
    let server = spawn(|_, _, mut conn: Conn| async move {
        conn.open_stream().await;
        conn.gap(77).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_job_events(owned(), brisk());
    assert_eq!(
        watch.next_item().await.unwrap(),
        Some(JobEventItem::Gap {
            earliest_available: 77
        })
    );
    // …and it did not become the cursor.
    assert_eq!(watch.cursor(), None);
}

/// A connection that goes silent is exactly as recoverable as one that closes:
/// the watcher drops it once the idle timeout fires and reconnects from the
/// cursor, with no error reaching the caller.
#[tokio::test]
async fn an_idle_connection_is_dropped_and_reconnected() {
    let server = spawn(|n, _, mut conn: Conn| async move {
        conn.open_stream().await;
        match n {
            0 => {
                conn.batch(5).await;
                // Then nothing at all: no batch, no bookmark, no close.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            _ => {
                conn.batch(6).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    })
    .await;

    let options = brisk().with_idle_timeout(Duration::from_millis(200));
    let mut watch = client(&server).watch_job_events(owned(), options);

    let Some(JobEventItem::Batch(first)) = watch.next_item().await.unwrap() else {
        panic!("a batch");
    };
    assert_eq!(first.index, 5);

    // The idle connection is silently replaced — no error is ever surfaced —
    // and the caller just sees the next batch land.
    let Some(JobEventItem::Batch(next)) = watch.next_item().await.unwrap() else {
        panic!("a batch, after the idle reconnect");
    };
    assert_eq!(next.index, 6);
    assert!(!watch.is_finished());

    let seen = server.seen();
    assert_eq!(seen.len(), 2, "one reconnect: {seen:?}");
    assert_eq!(
        seen[1].header("last-event-id"),
        Some("5"),
        "the reconnect resumes from the last frame the idle connection delivered"
    );
}

/// `UNAVAILABLE` is the replica saying "not me, not now" — its subscription
/// cap, or a fanout that has not started — so the watcher comes back.
#[tokio::test]
async fn an_unavailable_replica_is_retried() {
    let server = spawn(|n, _, mut conn: Conn| async move {
        if n == 0 {
            conn.respond(
                503,
                r#"{"code":"UNAVAILABLE","message":"subscription cap reached"}"#,
            )
            .await;
            return;
        }
        conn.open_stream().await;
        conn.batch(2).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_job_events(owned(), brisk());
    let Some(JobEventItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("a batch, after the retry");
    };
    assert_eq!(batch.index, 2);
    assert_eq!(server.seen().len(), 2);
}

/// A refusal the client caused cannot be fixed by asking again, so it ends the
/// watch — reported once, and `None` from then on.
#[tokio::test]
async fn a_refusal_ends_the_watch_and_is_reported_once() {
    for (status, body, code) in [
        (
            400,
            r#"{"code":"INVALID_ARGUMENT","message":"bad filter"}"#,
            ErrorCode::InvalidArgument,
        ),
        (
            401,
            r#"{"code":"UNAUTHENTICATED","message":"no token"}"#,
            ErrorCode::Unauthenticated,
        ),
        (
            403,
            r#"{"code":"PERMISSION_DENIED","message":"nope"}"#,
            ErrorCode::PermissionDenied,
        ),
    ] {
        let body = body.to_string();
        let server = spawn(move |_, _, conn: Conn| {
            let body = body.clone();
            async move { conn.respond(status, &body).await }
        })
        .await;

        let mut watch = client(&server).watch_job_events(owned(), brisk());
        let err = watch.next_item().await.expect_err("a refusal");
        assert_eq!(err.status(), Some(status), "{err:?}");
        assert_eq!(err.code(), Some(&code), "{err:?}");
        assert!(watch.is_finished());
        assert_eq!(watch.next_item().await.unwrap(), None);
        assert_eq!(server.seen().len(), 1, "a refusal is not retried");
    }
}

/// Behind a load balancer, a snapshot's second page is an independent bounded
/// read that can land on a replica behind the one the first page hit. Once
/// the first page has reported an index, every later page must be pinned to
/// it with `min_index` — and the subscription that follows must resume from
/// that same index.
#[tokio::test]
async fn a_second_list_page_is_pinned_to_the_first_pages_index() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            if request.query.contains("cursor=") {
                // The second page: served, in this test, by a replica that
                // has not caught up to the one that answered the first.
                conn.respond_at(200, r#"{"jobs":[],"next_cursor":null}"#, 90)
                    .await;
            } else {
                conn.respond_at(200, r#"{"jobs":[],"next_cursor":"v1:next"}"#, 100)
                    .await;
            }
            return;
        }
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());
    let Some(JobWatchItem::SnapshotPage {
        jobs,
        index,
        first,
        last,
    }) = watch.next_item().await.unwrap()
    else {
        panic!("a snapshot page first");
    };
    assert!(jobs.is_empty());
    assert_eq!(index, Some(100), "the first page's own index");
    assert!(first && !last, "the run has a second page to come");

    // The second page — empty, but the end of the run, so it is surfaced.
    let Some(JobWatchItem::SnapshotPage {
        index, first, last, ..
    }) = watch.next_item().await.unwrap()
    else {
        panic!("the second page");
    };
    assert_eq!(index, Some(90), "each page reports its own index");
    assert!(!first && last);

    // The subscription is opened lazily, on the *next* `next_item` call —
    // give it a moment to connect, then move on: the server never sends
    // anything on it, so waiting on it any further would just hang.
    let _ = tokio::time::timeout(Duration::from_millis(200), watch.next_item()).await;

    let seen = server.seen();
    let jobs_requests: Vec<&Recorded> = seen.iter().filter(|r| r.path == "/api/v1/jobs").collect();
    assert_eq!(jobs_requests.len(), 2, "{seen:?}");
    assert!(
        !jobs_requests[0].query.contains("min_index"),
        "the first page carries no floor: {:?}",
        jobs_requests[0].query
    );
    assert!(
        jobs_requests[1].query.contains("min_index=100"),
        "the second page is pinned to the first page's index: {:?}",
        jobs_requests[1].query
    );

    let events_request = seen
        .iter()
        .find(|r| r.path == "/api/v1/events")
        .expect("the subscription is opened");
    assert_eq!(
        events_request.header("last-event-id"),
        Some("100"),
        "the subscription resumes from the same index the pages were pinned to"
    );
}

/// The floor is added to the caller's read options, not swapped in for them:
/// a client built to read `Strong` keeps doing so on the pinned pages, and a
/// `min_index` it already carried is raised, never lowered.
#[tokio::test]
async fn pinning_a_page_keeps_the_callers_read_options() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            if request.query.contains("cursor=") {
                conn.respond_at(200, r#"{"jobs":[],"next_cursor":null}"#, 100)
                    .await;
            } else {
                conn.respond_at(200, r#"{"jobs":[],"next_cursor":"v1:next"}"#, 100)
                    .await;
            }
            return;
        }
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let strong = client(&server)
        .with_read_options(coppice_client::ReadOptions::strong().with_min_index(250));
    let mut watch = strong.watch_jobs(owned(), brisk());
    for expected_last in [false, true] {
        let Some(JobWatchItem::SnapshotPage { last, .. }) = watch.next_item().await.unwrap() else {
            panic!("a snapshot page");
        };
        assert_eq!(last, expected_last);
    }

    let seen = server.seen();
    let pages: Vec<&Recorded> = seen.iter().filter(|r| r.path == "/api/v1/jobs").collect();
    assert_eq!(pages.len(), 2, "{seen:?}");
    for page in pages {
        assert!(
            page.query.contains("consistency=strong"),
            "every page stays strong: {:?}",
            page.query
        );
        assert!(
            page.query.contains("min_index=250"),
            "the caller's higher floor survives the pin to 100: {:?}",
            page.query
        );
    }
}

// ---------------------------------------------------------------------------
// The whole loop
// ---------------------------------------------------------------------------

/// The ADR 0043 loop end to end: the snapshot comes first, the subscription
/// resumes from that read's applied index, and a gap produces another snapshot
/// rather than a silence.
#[tokio::test]
async fn watch_jobs_snapshots_then_streams_then_resnapshots_on_a_gap() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            conn.respond(200, EMPTY_PAGE).await;
            return;
        }
        conn.open_stream().await;
        if request.header("last-event-id") == Some("100") {
            conn.batch(101).await;
            conn.gap(90).await;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());

    let Some(JobWatchItem::SnapshotPage {
        jobs,
        index,
        first,
        last,
    }) = watch.next_item().await.unwrap()
    else {
        panic!("a snapshot page first");
    };
    assert!(jobs.is_empty());
    assert!(first && last, "one page is both ends of the run");
    assert_eq!(
        index,
        Some(100),
        "the index is the first list page's `Coppice-Applied-Index`"
    );

    let Some(JobWatchItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("the events between snapshots");
    };
    assert_eq!(batch.index, 101);

    // The gap is answered with a re-list and a resubscribe, not swallowed and
    // not handed over raw: a snapshot is what a gap actually costs a caller.
    let Some(JobWatchItem::SnapshotPage { index, first, .. }) = watch.next_item().await.unwrap()
    else {
        panic!("a gap resyncs");
    };
    assert_eq!(index, Some(100));
    assert!(first, "a resync starts a fresh run");

    // Across that boundary delivery is honestly at-least-once: the resubscribe
    // resumes from the snapshot's own index, so the batch the snapshot already
    // reflects arrives again. This is the documented cost of a resync, not a
    // defect — and it is why a caller rebuilds from a snapshot rather than
    // patching one.
    let Some(JobWatchItem::Batch(again)) = watch.next_item().await.unwrap() else {
        panic!("the stream resumes after the resync");
    };
    assert_eq!(again.index, 101);

    let seen = server.seen();
    let paths: Vec<&str> = seen.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "/api/v1/jobs",
            "/api/v1/events",
            "/api/v1/jobs",
            "/api/v1/events"
        ]
    );
    assert!(
        seen[1].query.contains("jobs=") && seen[3].query.contains("jobs="),
        "both subscriptions carry the same selector: {seen:?}"
    );
    assert_eq!(seen[1].header("last-event-id"), Some("100"));
    // The first list has nothing to be fresher than; the one answering the gap
    // must be no older than the gap's `earliest_available`, or a lagging
    // replica could serve a list from before whatever raised it.
    assert!(
        !seen[0].query.contains("min_index"),
        "the opening list is unpinned: {:?}",
        seen[0].query
    );
    assert!(
        seen[2].query.contains("min_index=90"),
        "the resync list is pinned to the gap's floor: {:?}",
        seen[2].query
    );
}

// ---------------------------------------------------------------------------
// The snapshot: scope, paging, and what it suppresses
// ---------------------------------------------------------------------------

/// The default scope's two halves, which are deliberately different filters:
/// the list asks for the caller's filter AND a non-terminal phase, so the
/// snapshot is bounded by the live set; the subscription asks for the
/// caller's filter alone, so a job's events keep arriving through the
/// transition that ends it — and because the server forbids a `phase` leaf on
/// a subscription at all.
#[tokio::test]
async fn a_live_snapshot_lists_the_non_terminal_phases_and_subscribes_without_them() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            conn.respond(200, EMPTY_PAGE).await;
            return;
        }
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage { .. })
    ));
    let _ = tokio::time::timeout(Duration::from_millis(200), watch.next_item()).await;

    let seen = server.seen();
    let list = seen
        .iter()
        .find(|r| r.path == "/api/v1/jobs")
        .expect("a list");
    assert_eq!(
        json_param(&list.query, "filter"),
        serde_json::json!({
            "all": [{ "metadata": { "key": "owner", "equals": "batch-service" } }, live_phases()]
        })
    );
    // And it walks in the largest pages the server will serve.
    assert_eq!(
        param(&list.query, "limit"),
        coppice_client::MAX_LIST_JOBS_LIMIT.to_string()
    );

    let events = seen
        .iter()
        .find(|r| r.path == "/api/v1/events")
        .expect("a subscription");
    assert_eq!(
        json_param(&events.query, "jobs"),
        serde_json::json!({ "metadata": { "key": "owner", "equals": "batch-service" } }),
        "the subscription's filter is the caller's, with no phase leaf"
    );
}

/// `All` is the opt-out: the list filter is the caller's, terminal jobs
/// included, and the walk is as long as retention is deep.
#[tokio::test]
async fn an_all_scope_snapshot_lists_the_callers_filter_unchanged() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            conn.respond(200, EMPTY_PAGE).await;
            return;
        }
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let options = brisk().with_snapshot_scope(SnapshotScope::All);
    let mut watch = client(&server).watch_jobs(owned(), options);
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage { .. })
    ));

    let list = server.seen().into_iter().next().expect("a list");
    assert_eq!(
        json_param(&list.query, "filter"),
        serde_json::json!({ "metadata": { "key": "owner", "equals": "batch-service" } })
    );
}

/// A filter that is already a top-level `all` gains a child rather than a
/// level: the depth and node caps are finite, and spending a level of nesting
/// that buys nothing is how a caller's legal filter becomes an
/// `INVALID_ARGUMENT`.
#[tokio::test]
async fn a_callers_all_filter_gains_the_phase_leaf_without_nesting() {
    let server = spawn(|_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            conn.respond(200, EMPTY_PAGE).await;
            return;
        }
        conn.open_stream().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let filter = JobFilter::all([owned(), JobFilter::submitted_by("alice")]);
    let mut watch = client(&server).watch_jobs(filter, brisk());
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage { .. })
    ));

    let list = server.seen().into_iter().next().expect("a list");
    assert_eq!(
        json_param(&list.query, "filter"),
        serde_json::json!({
            "all": [
                { "metadata": { "key": "owner", "equals": "batch-service" } },
                { "submitted_by": "alice" },
                live_phases(),
            ]
        })
    );
}

/// The walk is driven by the consumer: one list request per item, each page
/// carrying its own index, the run's ends marked — and an empty page in the
/// middle, which the server produces whenever its scan budget runs out before
/// the limit does, is not worth a caller's attention.
#[tokio::test]
async fn a_multi_page_walk_surfaces_one_page_per_call_and_skips_an_empty_middle() {
    let a = "job-00000000-0000-0000-0000-00000000000a";
    let b = "job-00000000-0000-0000-0000-00000000000b";
    let server = spawn(move |_, request: Recorded, mut conn: Conn| async move {
        if request.path != "/api/v1/jobs" {
            conn.open_stream().await;
            tokio::time::sleep(Duration::from_secs(60)).await;
            return;
        }
        if request.query.contains("cursor=") {
            if param(&request.query, "cursor") == "v1:p2" {
                // A page the scan budget cut short: no rows, but not the end.
                conn.respond_at(200, &page(&[], Some("v1:p3")), 120).await;
            } else {
                conn.respond_at(200, &page(&[b], None), 121).await;
            }
        } else {
            conn.respond_at(200, &page(&[a], Some("v1:p2")), 120).await;
        }
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());

    let Some(JobWatchItem::SnapshotPage {
        jobs,
        index,
        first,
        last,
    }) = watch.next_item().await.unwrap()
    else {
        panic!("the first page");
    };
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id.to_string(), a);
    assert_eq!(index, Some(120));
    assert!(first && !last);

    // The empty middle page is consumed on the way to this one, never
    // surfaced: one `next_item`, two list requests.
    let Some(JobWatchItem::SnapshotPage {
        jobs,
        index,
        first,
        last,
    }) = watch.next_item().await.unwrap()
    else {
        panic!("the last page");
    };
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id.to_string(), b);
    assert_eq!(index, Some(121), "each page reports its own index");
    assert!(!first && last);

    let seen = server.seen();
    let pages: Vec<&Recorded> = seen.iter().filter(|r| r.path == "/api/v1/jobs").collect();
    assert_eq!(pages.len(), 3, "{seen:?}");
    assert!(!pages[0].query.contains("min_index"));
    for page in &pages[1..] {
        assert!(
            page.query.contains("min_index=120"),
            "every page after the first is pinned to the first page's index: {:?}",
            page.query
        );
    }
}

/// The fuzzy snapshot's guarantee, per job: a row read at index `i` already
/// reflects everything about that job up to `i`, so events for it at or below
/// `i` are dropped — and a batch left with nothing is not delivered at all.
/// Events for a job the snapshot never listed (already terminal, evicted, or
/// simply not matching) are never suppressed.
#[tokio::test]
async fn a_snapshot_row_suppresses_the_events_it_already_reflects() {
    let a = "job-00000000-0000-0000-0000-00000000000a";
    let b = "job-00000000-0000-0000-0000-00000000000b";
    let c = "job-00000000-0000-0000-0000-00000000000c";
    let server = spawn(move |_, request: Recorded, mut conn: Conn| async move {
        if request.path == "/api/v1/jobs" {
            if request.query.contains("cursor=") {
                // The second page landed on a replica further ahead: B's row
                // is newer than the cursor the subscription resumes from.
                conn.respond_at(200, &page(&[b], None), 120).await;
            } else {
                conn.respond_at(200, &page(&[a], Some("v1:p2")), 100).await;
            }
            return;
        }
        conn.open_stream().await;
        if request.header("last-event-id") == Some("100") {
            // Below B's row: B's event is old news, C's is not.
            conn.batch_for(110, &[b, c]).await;
            // Nothing but B, still below its row: nothing to deliver.
            conn.batch_for(115, &[b]).await;
            // Above it: B is news again.
            conn.batch_for(125, &[b]).await;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());
    for expected in [(Some(100), true, false), (Some(120), false, true)] {
        let Some(JobWatchItem::SnapshotPage {
            index, first, last, ..
        }) = watch.next_item().await.unwrap()
        else {
            panic!("a snapshot page");
        };
        assert_eq!((index, first, last), expected);
    }

    let Some(JobWatchItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("the batch C's event keeps alive");
    };
    assert_eq!(batch.index, 110);
    assert_eq!(
        batch
            .events
            .iter()
            .map(|e| (e.ordinal, e.body.job().unwrap().to_string()))
            .collect::<Vec<_>>(),
        vec![(1, c.to_string())],
        "B's event is dropped and C's keeps the ordinal it arrived with"
    );

    // The batch at 115 held nothing but B and was therefore not delivered:
    // what comes next is 125.
    let Some(JobWatchItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("the batch above B's row");
    };
    assert_eq!(batch.index, 125, "the emptied batch was skipped entirely");
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].body.job().unwrap().to_string(), b);

    // A's row was on the first page, at the same index the subscription
    // resumed from, so nothing about A could have been suppressed here — the
    // `JobEventWatcher`'s own cursor already covers that. (The unit tests in
    // `events.rs` pin the rule itself, including the map's retirement.)
    assert!(!watch.is_finished());
}

/// A gap replaces the snapshot, and the rows with it: what the old pages
/// reflected says nothing about a stream resumed from a new one.
#[tokio::test]
async fn a_resync_replaces_the_suppression_rows() {
    let a = "job-00000000-0000-0000-0000-00000000000a";
    let b = "job-00000000-0000-0000-0000-00000000000b";
    let c = "job-00000000-0000-0000-0000-00000000000c";
    let server = spawn(move |n, request: Recorded, mut conn: Conn| async move {
        match (n, request.path.as_str()) {
            // The first snapshot: B's row read at 120, ahead of the cursor.
            (0, "/api/v1/jobs") => conn.respond_at(200, &page(&[a], Some("v1:p2")), 100).await,
            (1, "/api/v1/jobs") => conn.respond_at(200, &page(&[b], None), 120).await,
            (2, "/api/v1/events") => {
                conn.open_stream().await;
                conn.gap(90).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            // The resync: new pages, new rows, and no mention of B.
            (3, "/api/v1/jobs") => conn.respond_at(200, &page(&[], Some("v1:p2")), 105).await,
            (4, "/api/v1/jobs") => conn.respond_at(200, &page(&[c], None), 105).await,
            _ => {
                conn.open_stream().await;
                conn.batch_for(110, &[b]).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());
    for _ in 0..2 {
        assert!(matches!(
            watch.next_item().await.unwrap(),
            Some(JobWatchItem::SnapshotPage { .. })
        ));
    }

    // The gap's resync: a fresh run, pinned to the gap's floor.
    let Some(JobWatchItem::SnapshotPage { first, index, .. }) = watch.next_item().await.unwrap()
    else {
        panic!("a resync");
    };
    assert!(first, "a gap starts a run over");
    assert_eq!(index, Some(105));
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage { last: true, .. })
    ));

    // B was recorded at 120 by the *old* snapshot, which would have
    // suppressed this batch. The new rows never mentioned B, so it lands.
    let Some(JobWatchItem::Batch(batch)) = watch.next_item().await.unwrap() else {
        panic!("the batch after the resync");
    };
    assert_eq!(batch.index, 110);
    assert_eq!(batch.events[0].body.job().unwrap().to_string(), b);

    let seen = server.seen();
    assert!(
        seen[3].query.contains("min_index=90"),
        "the resync list is pinned to the gap's floor: {:?}",
        seen[3].query
    );
}

/// A retryable failure part way through a walk retries **that page**: the
/// cursor is kept, so the pages already handed over are not read (or
/// delivered) twice.
#[tokio::test]
async fn a_retryable_failure_retries_the_same_page() {
    let server = spawn(|n, request: Recorded, mut conn: Conn| async move {
        match (n, request.path.as_str()) {
            (0, "/api/v1/jobs") => conn.respond_at(200, &page(&[], Some("v1:p2")), 100).await,
            (1, "/api/v1/jobs") => {
                conn.respond(
                    503,
                    r#"{"code":"UNAVAILABLE","message":"not this replica, not now"}"#,
                )
                .await
            }
            (2, "/api/v1/jobs") => conn.respond_at(200, EMPTY_PAGE, 100).await,
            _ => {
                conn.open_stream().await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    })
    .await;

    let mut watch = client(&server).watch_jobs(owned(), brisk());
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage {
            first: true,
            last: false,
            ..
        })
    ));
    assert!(matches!(
        watch.next_item().await.unwrap(),
        Some(JobWatchItem::SnapshotPage {
            first: false,
            last: true,
            ..
        })
    ));

    let seen = server.seen();
    let pages: Vec<&Recorded> = seen.iter().filter(|r| r.path == "/api/v1/jobs").collect();
    assert_eq!(pages.len(), 3, "{seen:?}");
    assert!(
        !pages[0].query.contains("cursor="),
        "the first page: {:?}",
        pages[0].query
    );
    for page in &pages[1..] {
        assert_eq!(
            param(&page.query, "cursor"),
            "v1:p2",
            "the retry re-sends the page that failed, not the walk: {:?}",
            page.query
        );
    }
}
