//! The per-container log follower (docker-executor.md §8.2).
//!
//! One follower task per running container tails the Docker logs API with
//! `follow=true, timestamps=true, since=<resume point>`, demuxes each frame into
//! a [`LogChunk`], and batches them into the [`TelemetryHub`] in daemon delivery
//! order. The recovery contract is deliberately **at-least-once** (§8.2): on a
//! stream error the follower re-derives a whole-second resume boundary from the
//! filesystem store's newest stored timestamp and reconnects — never deleting,
//! aligning, or deduplicating a returned chunk. Amplification of the boundary
//! second is metered by [`AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL`].
//!
//! After an exit the drain completes one of two ways (§8.2): the
//! confirmed-dead signal (`died`, fired by `note_container_dead` — never by a
//! mere claim, which can precede a limit kill) wins the race against the
//! stream — the follower drops the follow stream and runs one `follow=false`
//! [`catch_up`] fetch from the last forwarded second (some daemons hold a dead
//! container's follow stream open for 1–2 s past the `die` event, and waiting
//! that out delays reap) — or the stream EOFs on its own. Either way the
//! follower makes its final flush and signals `drained`, which the session's
//! `reap` awaits before removing the container. [`catch_up`] is the same
//! at-least-once drain applied *once* to an already-dead container that reap
//! found without a live follower.
//!
//! `parse_frame` (the timestamp-prefix split) and [`floor_to_second`] are pure
//! and unit-tested without a daemon (§12).
//!
//! [`AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL`]:
//! super::AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL

use bollard::container::LogOutput;
use bollard::query_parameters::LogsOptionsBuilder;
use bollard::Docker;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use coppice_core::time::Timestamp;

use super::{api, classify, ContainerIds};
use crate::executor::ExecutorError;
use crate::telemetry::{FilesystemSink, LogChunk, LogStream, TelemetryHub};

/// Flush the buffer once it reaches this many chunks (docker-executor.md §8.2).
const FLUSH_CHUNKS: usize = 512;
/// Flush at most this long after the first buffered chunk, so a low-volume
/// container's logs still land promptly (docker-executor.md §8.2).
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
/// Backoff after a stream error before re-deriving the resume point and
/// reconnecting (docker-executor.md §8.2/§11) — avoids a hot loop against a
/// flapping daemon; the resync boundary is the real safety net.
const RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// Floor a µs [`Timestamp`] to the whole second below it (docker-executor.md
/// §8.2): the logs API's `since` filter only accepts whole seconds, so the
/// resume boundary is derived at second granularity. `coppice_core::time` has no
/// such helper, so it lives here.
pub(crate) fn floor_to_second(at: Timestamp) -> Timestamp {
    let micros = at.as_micros();
    let floored = micros - micros.rem_euclid(1_000_000);
    // The floor of a representable instant is representable; fall back to the
    // input on the impossible out-of-range case rather than panicking.
    Timestamp::from_micros(floored).unwrap_or(at)
}

/// Whole seconds since the epoch for the logs API `since` param (`i32`),
/// clamped: a negative or out-of-range boundary becomes 0 ("from the start").
fn since_secs(boundary: Option<Timestamp>) -> i32 {
    match boundary {
        Some(at) => i32::try_from(at.as_micros() / 1_000_000)
            .unwrap_or(0)
            .max(0),
        None => 0,
    }
}

/// Map a demuxed Docker log frame onto its [`LogStream`] and payload bytes.
/// `StdOut`/`Console` are stdout, `StdErr` is stderr, and `StdIn` is ignored —
/// a container never produces it on the logs API.
fn classify_output(output: &LogOutput) -> Option<(LogStream, bytes::Bytes)> {
    match output {
        LogOutput::StdOut { message } | LogOutput::Console { message } => {
            Some((LogStream::Stdout, message.clone()))
        }
        LogOutput::StdErr { message } => Some((LogStream::Stderr, message.clone())),
        LogOutput::StdIn { .. } => None,
    }
}

/// Split a `timestamps=true` log frame into its per-line timestamp and payload
/// (docker-executor.md §8.2).
///
/// With `timestamps=true` each frame is `<RFC3339Nano> <payload>`. We split at
/// the **first** space, parse the prefix as a Docker timestamp, and return the
/// payload **only** — the timestamp prefix is transport decoration we requested,
/// not user content, and the payload itself is never re-framed (spaces and
/// newlines inside it are preserved byte-for-byte). An unparseable prefix yields
/// `(None, whole frame verbatim)`; the caller substitutes the last seen chunk
/// timestamp, else `Timestamp::now()`.
pub(crate) fn parse_frame(bytes: &[u8]) -> (Option<Timestamp>, bytes::Bytes) {
    if let Some(space) = bytes.iter().position(|&byte| byte == b' ') {
        let (prefix, rest) = bytes.split_at(space);
        if let Ok(prefix) = std::str::from_utf8(prefix) {
            if let Some(at) = classify::parse_docker_time(prefix) {
                // `rest` still leads with the split space; the payload is after it.
                return (Some(at), bytes::Bytes::copy_from_slice(&rest[1..]));
            }
        }
    }
    (None, bytes::Bytes::copy_from_slice(bytes))
}

/// Build one [`LogChunk`] from a parsed frame, threading the last-seen timestamp
/// forward for frames with an unparseable prefix, and metering a replayed chunk
/// (`at <= replay_max`) when the boundary came from stored data (§8.2).
fn chunk_from_frame(
    ids: ContainerIds,
    stream: LogStream,
    bytes: bytes::Bytes,
    last_at: &mut Option<Timestamp>,
    replay_max: Option<Timestamp>,
) -> LogChunk {
    let (parsed_at, payload) = parse_frame(&bytes);
    let at = parsed_at.or(*last_at).unwrap_or_else(Timestamp::now);
    *last_at = Some(at);
    if let Some(replay_max) = replay_max {
        if at <= replay_max {
            metrics::counter!(super::AGENT_LOG_RESUME_REPLAYED_CHUNKS_TOTAL).increment(1);
        }
    }
    LogChunk {
        allocation: ids.allocation,
        attempt: ids.attempt,
        job: ids.job,
        at,
        stream,
        bytes: payload,
    }
}

/// Spawn the log follower for one container (docker-executor.md §8.2), returning
/// its handle. Captures only clones (the docker client, the hub, the store) —
/// never an `Arc<Inner>` — so an abort is what stops it (the mod.rs no-cycle
/// rule). `died` is the confirmed-dead signal (`note_container_dead` fires it,
/// only on proof of death — never on a mere claim, which can precede a limit
/// kill): the follower races it against the follow stream and, once it fires,
/// abandons the stream for a single `follow=false` [`catch_up`] fetch — the
/// daemon can hold a dead container's follow stream open for 1–2 s past the
/// `die` event, and the catch-up returns the same tail immediately. `drained_tx` is set to `true`
/// after the final flush; reap awaits it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_follower(
    docker: Docker,
    hub: TelemetryHub,
    store: Option<FilesystemSink>,
    ids: ContainerIds,
    container_name: String,
    initial_boundary: Option<Timestamp>,
    initial_replay_max: Option<Timestamp>,
    mut died: watch::Receiver<bool>,
    drained_tx: watch::Sender<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut boundary = initial_boundary;
        let mut replay_max = initial_replay_max;
        // Warn-once per error streak (the fs_sink latch idiom, but a local bool
        // suffices inside one task): progress resets it, so a flapping daemon is
        // metered by the reconnect, not a log flood.
        let mut warned = false;
        // Set when the `died` sender vanished without firing (the collector
        // entry was dropped by reap or shutdown): disables the select arm so it
        // cannot spin; the EOF path or the abort still ends the task.
        let mut died_gone = false;
        /// How one follow connection ended.
        enum ConnEnd {
            /// EOF: the container stopped and the drain is complete.
            Eof,
            /// A stream error: reconnect after re-deriving the boundary.
            Reconnect,
            /// The confirmed-dead signal fired: switch to the catch-up drain.
            Died,
        }
        loop {
            // Fast drain (§8.2): once death is confirmed, the follow stream's
            // remaining life is only the daemon's slow close of a dead stream —
            // a single `follow=false` fetch returns the same tail immediately.
            // The boundary is the last forwarded chunk's second, so nothing
            // already forwarded is dropped and at most that boundary second
            // replays (the standard at-least-once rule).
            if *died.borrow() {
                match catch_up(&docker, &hub, ids, &container_name, boundary, replay_max).await {
                    Ok(()) => {
                        let _ = drained_tx.send(true);
                        return;
                    }
                    Err(err) => {
                        if !warned {
                            tracing::warn!(
                                container = %container_name,
                                error = %err,
                                "post-exit catch-up drain failed; retrying (§8.2)"
                            );
                            warned = true;
                        }
                        tokio::time::sleep(RECONNECT_BACKOFF).await;
                        (boundary, replay_max) =
                            re_derive_resume(&store, ids, None, boundary).await;
                        continue;
                    }
                }
            }

            let options = LogsOptionsBuilder::new()
                .follow(true)
                .stdout(true)
                .stderr(true)
                .timestamps(true)
                .since(since_secs(boundary))
                .tail("all")
                .build();
            let mut stream = docker.logs(&container_name, Some(options));

            let mut buffer: Vec<LogChunk> = Vec::new();
            let mut last_at: Option<Timestamp> = boundary;
            // The flush deadline is only armed while the buffer is non-empty (it
            // fires 500ms after the *first* buffered chunk, not on every chunk).
            let flush = tokio::time::sleep(FLUSH_INTERVAL);
            tokio::pin!(flush);
            let mut flush_armed = false;

            let end = loop {
                tokio::select! {
                    item = stream.next() => match item {
                        Some(Ok(output)) => {
                            let Some((stream_kind, bytes)) = classify_output(&output) else {
                                continue;
                            };
                            let chunk = chunk_from_frame(
                                ids,
                                stream_kind,
                                bytes,
                                &mut last_at,
                                replay_max,
                            );
                            buffer.push(chunk);
                            warned = false; // progress resets the warn-once latch
                            if buffer.len() >= FLUSH_CHUNKS {
                                hub.append_logs(std::mem::take(&mut buffer));
                                flush_armed = false;
                            } else if !flush_armed {
                                flush.as_mut().reset(tokio::time::Instant::now() + FLUSH_INTERVAL);
                                flush_armed = true;
                            }
                        }
                        Some(Err(err)) => {
                            // 404 = the container is gone (raced a reap / external
                            // remove): flush what we have, signal drained, end.
                            if api::status_code(&err) == Some(404) {
                                if !buffer.is_empty() {
                                    hub.append_logs(std::mem::take(&mut buffer));
                                }
                                let _ = drained_tx.send(true);
                                return;
                            }
                            if !buffer.is_empty() {
                                hub.append_logs(std::mem::take(&mut buffer));
                            }
                            if !warned {
                                tracing::warn!(
                                    container = %container_name,
                                    error = %err,
                                    "log follower stream error; reconnecting (§8.2)"
                                );
                                warned = true;
                            }
                            break ConnEnd::Reconnect;
                        }
                        None => {
                            // EOF: the container stopped and the drain is complete.
                            if !buffer.is_empty() {
                                hub.append_logs(std::mem::take(&mut buffer));
                            }
                            break ConnEnd::Eof;
                        }
                    },
                    result = died.wait_for(|dead| *dead), if !died_gone => match result {
                        // Death was confirmed: flush and switch to the fast
                        // catch-up drain instead of waiting out the daemon's
                        // slow close of the dead follow stream.
                        Ok(_) => {
                            if !buffer.is_empty() {
                                hub.append_logs(std::mem::take(&mut buffer));
                            }
                            break ConnEnd::Died;
                        }
                        // Sender gone without firing: reap or shutdown dropped
                        // the collector entry. Keep draining to EOF.
                        Err(_) => died_gone = true,
                    },
                    _ = flush.as_mut(), if flush_armed => {
                        if !buffer.is_empty() {
                            hub.append_logs(std::mem::take(&mut buffer));
                        }
                        flush_armed = false;
                    }
                }
            };

            match end {
                ConnEnd::Eof => {
                    let _ = drained_tx.send(true);
                    return;
                }
                ConnEnd::Died => {
                    // Resume the catch-up from the last forwarded chunk's
                    // second (§8.2): everything already forwarded stays put and
                    // only the boundary second can replay. `replay_max` is kept
                    // — it still marks what predates this attempt's adoption,
                    // if anything.
                    boundary = last_at.map(floor_to_second);
                }
                ConnEnd::Reconnect => {
                    // Recovery (§8.2): sleep, then re-derive the resume point
                    // from the store's newest stored timestamp, else the last
                    // chunk we saw, else the boundary we started this
                    // connection with — and reconnect.
                    tokio::time::sleep(RECONNECT_BACKOFF).await;
                    (boundary, replay_max) = re_derive_resume(&store, ids, last_at, boundary).await;
                }
            }
        }
    })
}

/// Re-derive the §8.2 resume boundary after a stream error. The store's
/// `max_log_timestamp` over the live segments is authoritative — its floor is
/// the new `since` and its raw value the new `replay_max` (so the boundary
/// second is metered as replay). With no store, an empty store, or a store
/// error, keep the last chunk's floored timestamp if any (no replay metering —
/// that boundary did not come from stored data), else the prior boundary.
async fn re_derive_resume(
    store: &Option<FilesystemSink>,
    ids: ContainerIds,
    last_at: Option<Timestamp>,
    prev_boundary: Option<Timestamp>,
) -> (Option<Timestamp>, Option<Timestamp>) {
    if let Some(store) = store {
        match store.max_log_timestamp(&ids.job, &ids.attempt).await {
            Ok(Some(max)) => return (Some(floor_to_second(max)), Some(max)),
            Ok(None) => {}
            Err(err) => tracing::debug!(
                job = %ids.job,
                attempt = %ids.attempt,
                error = %err,
                "re-deriving log resume from the store failed; using the in-memory boundary"
            ),
        }
    }
    match last_at {
        Some(at) => (Some(floor_to_second(at)), None),
        None => (prev_boundary, None),
    }
}

/// A one-shot at-least-once drain of an already-dead container's logs
/// (docker-executor.md §8.2), for reap's catch-up path when no live follower
/// exists. Derives the same boundary rule, fetches `follow=false` with `since`,
/// and appends every returned chunk in ≤[`FLUSH_CHUNKS`] batches. A 404 anywhere
/// means the container is gone — nothing more to drain (`Ok`); any other stream
/// error is retryable so reap fails and the janitor retries.
pub(crate) async fn catch_up(
    docker: &Docker,
    hub: &TelemetryHub,
    ids: ContainerIds,
    container_name: &str,
    boundary: Option<Timestamp>,
    replay_max: Option<Timestamp>,
) -> Result<(), ExecutorError> {
    let options = LogsOptionsBuilder::new()
        .follow(false)
        .stdout(true)
        .stderr(true)
        .timestamps(true)
        .since(since_secs(boundary))
        .tail("all")
        .build();
    let mut stream = docker.logs(container_name, Some(options));
    let mut buffer: Vec<LogChunk> = Vec::new();
    let mut last_at: Option<Timestamp> = boundary;
    while let Some(item) = stream.next().await {
        match item {
            Ok(output) => {
                let Some((stream_kind, bytes)) = classify_output(&output) else {
                    continue;
                };
                let chunk = chunk_from_frame(ids, stream_kind, bytes, &mut last_at, replay_max);
                buffer.push(chunk);
                if buffer.len() >= FLUSH_CHUNKS {
                    hub.append_logs(std::mem::take(&mut buffer));
                }
            }
            Err(err) => {
                if api::status_code(&err) == Some(404) {
                    break; // gone — nothing more to drain
                }
                if !buffer.is_empty() {
                    hub.append_logs(std::mem::take(&mut buffer));
                }
                return Err(ExecutorError::Other(format!(
                    "catch-up log drain for {container_name}: {err}"
                )));
            }
        }
    }
    if !buffer.is_empty() {
        hub.append_logs(buffer);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{FilesystemSinkOptions, HubSink, LogQuery, SinkInstance, SinkKind};
    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use coppice_core::time::Duration as CoreDuration;
    use std::time::Duration as StdDuration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    #[test]
    fn parse_frame_splits_a_good_prefix_and_keeps_the_payload() {
        let frame = b"2026-07-19T10:00:00.123456789Z hello world\n";
        let (at, payload) = parse_frame(frame);
        assert_eq!(
            at,
            classify::parse_docker_time("2026-07-19T10:00:00.123456789Z")
        );
        // Payload after the first space, verbatim — internal space and newline
        // preserved, timestamp prefix stripped.
        assert_eq!(payload.as_ref(), b"hello world\n");
    }

    #[test]
    fn parse_frame_preserves_bytes_after_the_first_space() {
        // A payload with several spaces keeps every one of them.
        let frame = b"2026-07-19T10:00:00Z a b  c";
        let (at, payload) = parse_frame(frame);
        assert!(at.is_some());
        assert_eq!(payload.as_ref(), b"a b  c");
    }

    #[test]
    fn parse_frame_without_a_parseable_prefix_returns_the_whole_frame() {
        // No space at all.
        let (at, payload) = parse_frame(b"nospacehere");
        assert_eq!(at, None);
        assert_eq!(payload.as_ref(), b"nospacehere");

        // A leading token that is not a timestamp.
        let (at, payload) = parse_frame(b"notatime hello");
        assert_eq!(at, None);
        assert_eq!(payload.as_ref(), b"notatime hello");
    }

    #[test]
    fn parse_frame_handles_the_empty_frame() {
        let (at, payload) = parse_frame(b"");
        assert_eq!(at, None);
        assert!(payload.is_empty());
    }

    #[test]
    fn floor_to_second_drops_the_sub_second_tail() {
        let at = Timestamp::UNIX_EPOCH
            + CoreDuration::from_secs(1_000)
            + CoreDuration::from_micros(123_456);
        assert_eq!(
            floor_to_second(at),
            Timestamp::UNIX_EPOCH + CoreDuration::from_secs(1_000)
        );
        // An already-floored value is unchanged.
        let whole = Timestamp::UNIX_EPOCH + CoreDuration::from_secs(42);
        assert_eq!(floor_to_second(whole), whole);
    }

    #[test]
    fn since_secs_clamps_none_and_negative_to_zero() {
        assert_eq!(since_secs(None), 0);
        assert_eq!(
            since_secs(Some(Timestamp::UNIX_EPOCH + CoreDuration::from_secs(90))),
            90
        );
        // A pre-epoch boundary clamps to 0.
        assert_eq!(
            since_secs(Some(Timestamp::UNIX_EPOCH - CoreDuration::from_secs(5))),
            0
        );
    }

    // ---- re_derive_resume prefers the store (docker-executor.md §8.2) --------

    /// Fresh `ContainerIds` for a test attempt.
    fn ids() -> ContainerIds {
        ContainerIds {
            allocation: AllocationId::new(),
            attempt: AttemptId::new(),
            job: JobId::new(),
        }
    }

    /// A whole-second [`Timestamp`] `secs` seconds past the epoch.
    fn ts(secs: i64) -> Timestamp {
        Timestamp::from_micros(secs * 1_000_000).expect("in range")
    }

    async fn temp_sink(root: std::path::PathBuf) -> FilesystemSink {
        FilesystemSink::new(FilesystemSinkOptions::new(root))
            .await
            .expect("build filesystem sink")
    }

    /// The store's `MAX(at)` is authoritative even when the in-memory `last_at`
    /// is *newer*: `re_derive_resume` must floor the store's value for the new
    /// `since` and carry the raw value as the replay window (§8.2).
    #[tokio::test]
    async fn re_derive_resume_prefers_the_store_max_over_last_at() {
        let root = tempfile::TempDir::new().unwrap();
        let sink = temp_sink(root.path().join("tel")).await;
        let ids = ids();
        // Append one chunk at a known `at` through the public write seam, with an
        // injected `now` so no clock is read (the fs_sink test idiom).
        let stored_at = ts(50);
        let chunk = LogChunk {
            allocation: ids.allocation,
            attempt: ids.attempt,
            job: ids.job,
            at: stored_at,
            stream: LogStream::Stdout,
            bytes: bytes::Bytes::from_static(b"x"),
        };
        sink.append_logs_at(std::slice::from_ref(&chunk), stored_at)
            .await;

        // `last_at` is deliberately LATER than the store's max: if the store were
        // ignored the boundary would floor `last_at` (999) instead of 50.
        let last_at = Some(ts(999));
        let (boundary, replay_max) = re_derive_resume(&Some(sink), ids, last_at, None).await;
        assert_eq!(
            boundary,
            Some(floor_to_second(stored_at)),
            "the store's floored max wins over the newer last_at"
        );
        assert_eq!(
            replay_max,
            Some(stored_at),
            "the raw store max becomes the replay window"
        );
    }

    // ---- log follower reconnect over a TCP stub (docker-executor.md §8.2) -----

    /// bollard's stdcopy stream_type for stdout.
    const STDOUT: u8 = 1;
    /// bollard's stdcopy stream_type for stderr.
    const STDERR: u8 = 2;

    /// One `timestamps=true` log line as an HTTP `chunked` fragment carrying a
    /// single stdcopy frame — an 8-byte header `[type,0,0,0,len_be_u32]` then the
    /// `<RFC3339Nano> <text>\n` payload bollard's `NewlineLogOutputDecoder`
    /// demuxes into one [`LogOutput`].
    fn frame_chunk(secs: i64, stream_type: u8, text: &str) -> Vec<u8> {
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).unwrap();
        let line = format!(
            "{} {}\n",
            dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            text
        );
        let payload = line.as_bytes();
        let mut stdcopy = Vec::with_capacity(8 + payload.len());
        stdcopy.push(stream_type);
        stdcopy.extend_from_slice(&[0, 0, 0]);
        stdcopy.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        stdcopy.extend_from_slice(payload);
        // Wrap the whole frame in one HTTP chunk: `{len:x}\r\n<bytes>\r\n`.
        let mut chunk = format!("{:x}\r\n", stdcopy.len()).into_bytes();
        chunk.extend_from_slice(&stdcopy);
        chunk.extend_from_slice(b"\r\n");
        chunk
    }

    /// Read a request head up to the terminating blank line.
    async fn read_head(sock: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = sock.read(&mut tmp).await.expect("read request head");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// The `since=` query value from a request line (missing ⇒ `None`), tolerant
    /// of bollard's surrounding params — we never assert the whole URL.
    fn parse_since(request_line: &str) -> Option<i64> {
        let start = request_line.find("since=")? + "since=".len();
        let rest = &request_line[start..];
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '-'))
            .unwrap_or(rest.len());
        rest[..end].parse::<i64>().ok()
    }

    /// The response head introducing a chunked multiplexed log stream.
    const RESP_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\n\
        Content-Type: application/vnd.docker.multiplexed-stream\r\n\
        Transfer-Encoding: chunked\r\n\r\n";

    /// A local Docker-logs stub: serves connection 1 then aborts mid-body (no
    /// terminal chunk ⇒ the follower's reconnect trigger), then serves connection
    /// 2 with a properly terminated body ⇒ clean EOF. Reports each accepted
    /// connection's index, `since`, and path-match over `events`.
    async fn stub_server(
        listener: TcpListener,
        name: String,
        conn1: Vec<Vec<u8>>,
        conn2: Vec<Vec<u8>>,
        events: mpsc::UnboundedSender<(usize, Option<i64>, bool)>,
    ) {
        let mut index = 0usize;
        loop {
            let (mut sock, _) = listener.accept().await.expect("accept");
            index += 1;
            let head = read_head(&mut sock).await;
            let first = head.lines().next().unwrap_or("");
            let path_ok = first.contains(&format!("/containers/{name}/logs"));
            let since = parse_since(first);
            let _ = events.send((index, since, path_ok));

            sock.write_all(RESP_HEAD).await.expect("write head");
            match index {
                1 => {
                    for chunk in &conn1 {
                        sock.write_all(chunk).await.expect("write frame");
                    }
                    sock.flush().await.expect("flush");
                    // Abort: drop WITHOUT the terminal `0\r\n\r\n` chunk, so the
                    // follower sees a body error and reconnects (§8.2).
                    drop(sock);
                }
                2 => {
                    for chunk in &conn2 {
                        sock.write_all(chunk).await.expect("write frame");
                    }
                    // Terminate the chunked body cleanly ⇒ the follower drains.
                    sock.write_all(b"0\r\n\r\n")
                        .await
                        .expect("write terminator");
                    sock.flush().await.expect("flush");
                    drop(sock);
                }
                _ => drop(sock), // a 3rd connection is unexpected; the test checks.
            }
        }
    }

    /// End-to-end §8.2: the follower tails a real bollard client pointed at the
    /// stub, loses the stream mid-body, flushes, backs off, re-derives the resume
    /// boundary from the filesystem store, reconnects with `since=floor(T3)`,
    /// stores the boundary-second replay (never deduped) plus the new lines, and
    /// signals drained on the clean EOF.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follower_reconnects_and_replays_the_boundary_second() {
        // Timestamps are fixed literals: the follower parses them, nothing reads a
        // real clock. Distinct whole seconds so flooring is unambiguous.
        let base = 1_600_000_000i64;
        let (t1, t2, t3, t4, t5) = (base + 1, base + 2, base + 3, base + 4, base + 5);

        let root = tempfile::TempDir::new().unwrap();
        let sink = temp_sink(root.path().join("tel")).await;
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Filesystem(sink.clone()),
                kinds: vec![SinkKind::Logs],
            }],
            64,
        );

        let ids = ids();
        let name = "cont-reconnect".to_string();

        // Connection 1: three stdout lines T1..T3 and a stderr line at T3.
        let conn1 = vec![
            frame_chunk(t1, STDOUT, "line-1"),
            frame_chunk(t2, STDOUT, "line-2"),
            frame_chunk(t3, STDOUT, "line-3"),
            frame_chunk(t3, STDERR, "err-3"),
        ];
        // Connection 2: the T3 boundary-second replay (a duplicate line-3) then
        // two new lines T4/T5.
        let conn2 = vec![
            frame_chunk(t3, STDOUT, "line-3"),
            frame_chunk(t4, STDOUT, "line-4"),
            frame_chunk(t5, STDOUT, "line-5"),
        ];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
        let stub = tokio::spawn(stub_server(listener, name.clone(), conn1, conn2, evt_tx));

        let docker = api::connect(&format!("tcp://127.0.0.1:{port}")).expect("connect");
        let (drained_tx, mut drained_rx) = watch::channel(false);
        let (_died_tx, died_rx) = watch::channel(false);
        let follower = spawn_follower(
            docker,
            hub.clone(),
            Some(sink.clone()),
            ids,
            name,
            None,
            None,
            died_rx,
            drained_tx,
        );

        // Connection 1 opens with `since=0` (initial boundary None).
        let (idx1, since1, ok1) = tokio::time::timeout(StdDuration::from_secs(10), evt_rx.recv())
            .await
            .expect("connection 1 arrives")
            .expect("event");
        assert_eq!(idx1, 1);
        assert!(ok1, "the request path is the container's logs endpoint");
        assert_eq!(
            since1.unwrap_or(0),
            0,
            "the first connection resumes from the start (since=0)"
        );

        // Connection 2 opens after the backoff with `since=floor(T3)` — the
        // re-derived boundary, proving re_derive_resume ran through the store.
        let (idx2, since2, ok2) = tokio::time::timeout(StdDuration::from_secs(10), evt_rx.recv())
            .await
            .expect("connection 2 arrives after reconnect")
            .expect("event");
        assert_eq!(idx2, 2);
        assert!(ok2, "the reconnect hits the same logs endpoint");
        assert_eq!(
            since2.unwrap_or(0),
            t3,
            "the reconnect resumes from the floored newest stored second (T3)"
        );

        // The clean EOF on connection 2 flips drained.
        tokio::time::timeout(StdDuration::from_secs(15), async {
            while !*drained_rx.borrow_and_update() {
                drained_rx.changed().await.expect("drained sender lives");
            }
        })
        .await
        .expect("the follower signals drained after the clean EOF");

        // Everything the follower appended is delivered before we read the store.
        tokio::time::timeout(StdDuration::from_secs(10), hub.flush())
            .await
            .expect("hub drains");

        let stored = sink
            .log_chunks(&ids.job, &ids.attempt, None, LogQuery::Tail { n: 64 })
            .await
            .expect("read stored chunks");
        let got: Vec<(i64, LogStream, Vec<u8>)> = stored
            .iter()
            .map(|chunk| {
                (
                    chunk.at.as_micros() / 1_000_000,
                    chunk.stream,
                    chunk.bytes.to_vec(),
                )
            })
            .collect();
        // In (at, insertion) order: BOTH copies of the T3 line survive (§8.2 never
        // dedupes) and the stderr chunk is tagged Stderr.
        let expected: Vec<(i64, LogStream, Vec<u8>)> = vec![
            (t1, LogStream::Stdout, b"line-1\n".to_vec()),
            (t2, LogStream::Stdout, b"line-2\n".to_vec()),
            (t3, LogStream::Stdout, b"line-3\n".to_vec()),
            (t3, LogStream::Stderr, b"err-3\n".to_vec()),
            (t3, LogStream::Stdout, b"line-3\n".to_vec()),
            (t4, LogStream::Stdout, b"line-4\n".to_vec()),
            (t5, LogStream::Stdout, b"line-5\n".to_vec()),
        ];
        assert_eq!(
            got, expected,
            "at-least-once store: no dedupe, stderr tagged"
        );
        // Substance of the boundary replay: the T3 line is stored twice.
        let t3_line3 = got
            .iter()
            .filter(|(at, s, b)| *at == t3 && *s == LogStream::Stdout && b == b"line-3\n")
            .count();
        assert_eq!(
            t3_line3, 2,
            "the boundary-second line-3 is replayed, not deduped"
        );

        // The follower ended on EOF (it returned), so no third connection is made.
        assert!(
            tokio::time::timeout(StdDuration::from_secs(2), evt_rx.recv())
                .await
                .is_err(),
            "exactly two connections; the follower does not reconnect after EOF"
        );

        stub.abort();
        follower.abort();
    }

    // ---- died-signal fast drain (docker-executor.md §8.2) ---------------------

    /// The `follow=` query value from a request line (missing ⇒ `None`).
    fn parse_follow(request_line: &str) -> Option<bool> {
        let start = request_line.find("follow=")? + "follow=".len();
        let rest = &request_line[start..];
        if rest.starts_with("true") {
            Some(true)
        } else if rest.starts_with("false") {
            Some(false)
        } else {
            None
        }
    }

    /// A Docker-logs stub with data-driven per-connection behavior: each entry
    /// of `conns` is `(chunks, terminate)`. A terminated connection gets the
    /// `0\r\n\r\n` chunked terminator and is closed (clean EOF); an unterminated
    /// one is **held open** after its chunks — the daemon holding a dead
    /// container's follow stream, the very behavior the died signal bypasses.
    /// Reports each connection's index, `since`, `follow`, and path-match.
    async fn stub_server_held(
        listener: TcpListener,
        name: String,
        conns: Vec<(Vec<Vec<u8>>, bool)>,
        events: mpsc::UnboundedSender<(usize, Option<i64>, Option<bool>, bool)>,
    ) {
        let mut held = Vec::new();
        let mut index = 0usize;
        loop {
            let (mut sock, _) = listener.accept().await.expect("accept");
            index += 1;
            let head = read_head(&mut sock).await;
            let first = head.lines().next().unwrap_or("");
            let path_ok = first.contains(&format!("/containers/{name}/logs"));
            let _ = events.send((index, parse_since(first), parse_follow(first), path_ok));

            sock.write_all(RESP_HEAD).await.expect("write head");
            match conns.get(index - 1) {
                Some((chunks, terminate)) => {
                    for chunk in chunks {
                        sock.write_all(chunk).await.expect("write frame");
                    }
                    if *terminate {
                        sock.write_all(b"0\r\n\r\n")
                            .await
                            .expect("write terminator");
                    }
                    sock.flush().await.expect("flush");
                    if *terminate {
                        drop(sock);
                    } else {
                        held.push(sock); // keep the stream open, no EOF
                    }
                }
                None => drop(sock),
            }
        }
    }

    /// Poll the sink until at least `n` chunks are stored for the attempt (the
    /// hub's drain task delivers asynchronously; an `UnknownAttempt` error just
    /// means nothing has landed yet), or panic after ~10 s.
    async fn wait_for_stored(sink: &FilesystemSink, ids: ContainerIds, n: usize) {
        for _ in 0..500 {
            if let Ok(stored) = sink
                .log_chunks(&ids.job, &ids.attempt, None, LogQuery::Tail { n: 64 })
                .await
            {
                if stored.len() >= n {
                    return;
                }
            }
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
        panic!("stored chunks never reached {n}");
    }

    /// §8.2 fast drain: the follower tails a follow stream the stub never
    /// closes (the slow-EOF daemon), the died signal fires, and the follower
    /// abandons the stream for one `follow=false` catch-up from the last
    /// forwarded second — replaying only the boundary second (never deduped,
    /// never dropping the tail) — then signals drained.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn died_signal_switches_to_a_catch_up_drain_and_replays_the_boundary_second() {
        let base = 1_600_000_000i64;
        let (t1, t2, t3, t4) = (base + 1, base + 2, base + 3, base + 4);

        let root = tempfile::TempDir::new().unwrap();
        let sink = temp_sink(root.path().join("tel")).await;
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Filesystem(sink.clone()),
                kinds: vec![SinkKind::Logs],
            }],
            64,
        );
        let ids = ids();
        let name = "cont-died".to_string();

        // Connection 1 (follow=true): three lines, then held open forever.
        // Connection 2 (the catch-up): the T3 boundary-second replay plus the
        // tail line T4 the follower never saw, cleanly terminated.
        let conns = vec![
            (
                vec![
                    frame_chunk(t1, STDOUT, "line-1"),
                    frame_chunk(t2, STDOUT, "line-2"),
                    frame_chunk(t3, STDOUT, "line-3"),
                ],
                false,
            ),
            (
                vec![
                    frame_chunk(t3, STDOUT, "line-3"),
                    frame_chunk(t4, STDOUT, "line-4"),
                ],
                true,
            ),
        ];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
        let stub = tokio::spawn(stub_server_held(listener, name.clone(), conns, evt_tx));

        let docker = api::connect(&format!("tcp://127.0.0.1:{port}")).expect("connect");
        let (drained_tx, mut drained_rx) = watch::channel(false);
        let (died_tx, died_rx) = watch::channel(false);
        let follower = spawn_follower(
            docker,
            hub.clone(),
            Some(sink.clone()),
            ids,
            name,
            None,
            None,
            died_rx,
            drained_tx,
        );

        // Connection 1 is the live follow stream.
        let (idx1, since1, follow1, ok1) =
            tokio::time::timeout(StdDuration::from_secs(10), evt_rx.recv())
                .await
                .expect("connection 1 arrives")
                .expect("event");
        assert_eq!(idx1, 1);
        assert!(ok1);
        assert_eq!(since1.unwrap_or(0), 0);
        assert_eq!(follow1, Some(true), "the live follower streams follow=true");

        // Wait until the three lines are stored, so the follower's `last_at`
        // has provably advanced to T3 before death is confirmed.
        wait_for_stored(&sink, ids, 3).await;

        // Confirmed death fires the fast-drain signal; the stub never closed
        // connection 1, so only the signal can end the drain promptly.
        died_tx.send(true).expect("follower listens");

        // The catch-up: follow=false, resuming from the boundary second T3.
        let (idx2, since2, follow2, ok2) =
            tokio::time::timeout(StdDuration::from_secs(10), evt_rx.recv())
                .await
                .expect("the catch-up connection arrives")
                .expect("event");
        assert_eq!(idx2, 2);
        assert!(ok2);
        assert_eq!(
            follow2,
            Some(false),
            "the fast drain is a follow=false fetch"
        );
        assert_eq!(
            since2.unwrap_or(0),
            t3,
            "the catch-up resumes from the last forwarded second"
        );

        // The catch-up completing flips drained.
        tokio::time::timeout(StdDuration::from_secs(10), async {
            while !*drained_rx.borrow_and_update() {
                drained_rx.changed().await.expect("drained sender lives");
            }
        })
        .await
        .expect("the follower signals drained after the catch-up");

        tokio::time::timeout(StdDuration::from_secs(10), hub.flush())
            .await
            .expect("hub drains");

        let stored = sink
            .log_chunks(&ids.job, &ids.attempt, None, LogQuery::Tail { n: 64 })
            .await
            .expect("read stored chunks");
        let got: Vec<(i64, Vec<u8>)> = stored
            .iter()
            .map(|chunk| (chunk.at.as_micros() / 1_000_000, chunk.bytes.to_vec()))
            .collect();
        // Everything forwarded live stays put, the boundary second replays once
        // (§8.2 never dedupes), and the tail line is not lost.
        let expected: Vec<(i64, Vec<u8>)> = vec![
            (t1, b"line-1\n".to_vec()),
            (t2, b"line-2\n".to_vec()),
            (t3, b"line-3\n".to_vec()),
            (t3, b"line-3\n".to_vec()),
            (t4, b"line-4\n".to_vec()),
        ];
        assert_eq!(
            got, expected,
            "boundary-second replay only: nothing dropped, nothing beyond the boundary duplicated"
        );

        // The follower returned after the catch-up: no third connection.
        assert!(
            tokio::time::timeout(StdDuration::from_secs(2), evt_rx.recv())
                .await
                .is_err(),
            "exactly two connections; the drain ends at the catch-up"
        );

        stub.abort();
        follower.abort();
    }

    /// §8.2 fast drain when death was confirmed before the follower connected
    /// (the signal channel is created with the collector reservation, so a
    /// death landing before the spawn has already fired the channel the
    /// follower polls): the follower skips the follow stream entirely and
    /// drains via a single `follow=false` fetch from its initial boundary.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn died_before_connecting_drains_via_a_single_catch_up_fetch() {
        let base = 1_600_000_000i64;
        let t1 = base + 1;

        let root = tempfile::TempDir::new().unwrap();
        let sink = temp_sink(root.path().join("tel")).await;
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Filesystem(sink.clone()),
                kinds: vec![SinkKind::Logs],
            }],
            64,
        );
        let ids = ids();
        let name = "cont-dead-at-spawn".to_string();

        // A single terminated connection: the whole drain.
        let conns = vec![(vec![frame_chunk(t1, STDOUT, "only-line")], true)];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
        let stub = tokio::spawn(stub_server_held(listener, name.clone(), conns, evt_tx));

        let docker = api::connect(&format!("tcp://127.0.0.1:{port}")).expect("connect");
        let (drained_tx, mut drained_rx) = watch::channel(false);
        // The signal is already `true` at spawn: the channel was created with
        // the collector reservation and a death confirmed before the spawn
        // fired it in place (the guarantee this test pins is that no follow
        // stream is ever opened).
        let (_died_tx, died_rx) = watch::channel(true);
        let follower = spawn_follower(
            docker,
            hub.clone(),
            Some(sink.clone()),
            ids,
            name,
            None,
            None,
            died_rx,
            drained_tx,
        );

        let (idx1, since1, follow1, ok1) =
            tokio::time::timeout(StdDuration::from_secs(10), evt_rx.recv())
                .await
                .expect("the catch-up connection arrives")
                .expect("event");
        assert_eq!(idx1, 1);
        assert!(ok1);
        assert_eq!(since1.unwrap_or(0), 0, "no boundary: drain from the start");
        assert_eq!(
            follow1,
            Some(false),
            "a dead-at-spawn container is drained without a follow stream"
        );

        tokio::time::timeout(StdDuration::from_secs(10), async {
            while !*drained_rx.borrow_and_update() {
                drained_rx.changed().await.expect("drained sender lives");
            }
        })
        .await
        .expect("the follower signals drained");

        tokio::time::timeout(StdDuration::from_secs(10), hub.flush())
            .await
            .expect("hub drains");
        let stored = sink
            .log_chunks(&ids.job, &ids.attempt, None, LogQuery::Tail { n: 64 })
            .await
            .expect("read stored chunks");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].bytes.as_ref(), b"only-line\n");

        // One connection total: the follow stream was never opened.
        assert!(
            tokio::time::timeout(StdDuration::from_secs(2), evt_rx.recv())
                .await
                .is_err(),
            "exactly one connection; the follow stream is skipped entirely"
        );

        stub.abort();
        follower.abort();
    }
}
