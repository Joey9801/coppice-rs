//! Subscribing to job events, and the list-then-subscribe loop built on it.
//!
//! `GET /api/v1/events` (ADR 0043) answers an open-ended `text/event-stream`
//! carrying three named frames — a `batch` of one command's events, a
//! `progress` bookmark, and a `gap` — and this module is the three layers a
//! consumer wants over them:
//!
//! - [`JobEventStream`] is **one connection**, opened by
//!   [`Client::subscribe_job_events`]. It ends when the server ends it, and it
//!   reconnects nothing.
//! - [`JobEventWatcher`] ([`Client::watch_job_events`]) keeps a stream open
//!   across the endings that are routine — the credential expiring, the
//!   replica draining, a connection dropping — resuming from the last id it
//!   saw and discarding the replay that a resume can produce.
//! - [`JobWatcher`] ([`Client::watch_jobs`]) is the loop ADR 0043 describes:
//!   list with the filter, subscribe from that read's applied index, and on a
//!   `gap` do both again. Its snapshot arrives **page by page**
//!   ([`JobWatchItem::SnapshotPage`]) and, by default, covers only the jobs
//!   that are still live.
//!
//! ## What this actually guarantees
//!
//! Delivery is **at-least-once** (ADR 0008). Within one
//! [`JobEventWatcher`] run the duplicates a reconnect produces are removed —
//! the watcher tracks the cursor and drops anything at or below it — so a
//! caller sees each batch once. Across a resync boundary it is genuinely
//! at-least-once: [`JobWatcher`] re-lists and resubscribes from the first
//! page's own index, and what that page already reflected is dropped from the
//! stream per job (see [`JobWatcher`]). The residue a caller may still see
//! twice is harmless by construction, because `(index, ordinal)` identifies an
//! event and a snapshot page is state rather than history.
//!
//! **A gap is never swallowed.** [`JobEventWatcher`] hands
//! [`JobEventItem::Gap`] to the caller; [`JobWatcher`] answers it with a fresh
//! snapshot — a new run of [`JobWatchItem::SnapshotPage`]s, starting with
//! `first: true` — which is the resync the gap demands. Neither quietly
//! carries on as if delivery had been continuous.
//!
//! **Payloads are thin.** An event carries identity, stamp, kind and scope
//! ids, never a job snapshot. A consumer that needs more reads the job with
//! [`ReadOptions::at_least`](crate::ReadOptions::at_least) set to the event's
//! `index`, so a lagging replica cannot answer with state older than the
//! event that prompted the read:
//!
//! ```no_run
//! # use coppice_client::{Client, JobId, ReadOptions};
//! # async fn go(client: &Client, index: u64, job: JobId) -> coppice_client::Result<()> {
//! let detail = client
//!     .with_read_options(ReadOptions::at_least(index))
//!     .job(job)
//!     .await?;
//! # let _ = detail; Ok(()) }
//! ```
//!
//! ## The loop, end to end
//!
//! ```no_run
//! # async fn go(client: &coppice_client::Client) -> coppice_client::Result<()> {
//! use coppice_client::{JobFilter, JobWatchItem, WatchOptions};
//!
//! // Every job this service owns, including ones submitted after we started.
//! let filter = JobFilter::metadata_equals("owner", "batch-service");
//! let mut watch = client.watch_jobs(filter, WatchOptions::new());
//! let mut live = std::collections::HashMap::new();
//!
//! while let Some(item) = watch.next_item().await? {
//!     match item {
//!         // The live set, one page at a time. `first` starts a fresh
//!         // reconciliation — the opening snapshot, or the one a gap forced —
//!         // and `last` ends it.
//!         JobWatchItem::SnapshotPage { jobs, index, first, last } => {
//!             if first {
//!                 live.clear();
//!             }
//!             for job in jobs {
//!                 live.insert(job.id, job.state);
//!             }
//!             if last {
//!                 println!("{} live jobs as of {index:?}", live.len());
//!             }
//!         }
//!         // One command's worth of transitions, never split.
//!         JobWatchItem::Batch(batch) => {
//!             for event in &batch.events {
//!                 println!("{}.{} {:?}", event.index, event.ordinal, event.body);
//!             }
//!         }
//!         // The enum is `#[non_exhaustive]`: a later release may add an item.
//!         _ => {}
//!     }
//! }
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use crate::client::{Client, Versioned};
use crate::error::{Error, Result};
use crate::id::JobId;
use crate::sse::{SseFrame, SseParser};
use crate::types::{
    EventBatchFrame, EventGapFrame, EventProgressFrame, JobFilter, JobPhase, JobSummary,
    ListJobsParams, ListJobsResponse, MAX_LIST_JOBS_LIMIT,
};

/// How long a [`JobEventWatcher`] waits before its first reconnect attempt.
pub const DEFAULT_MIN_RECONNECT_BACKOFF: Duration = Duration::from_millis(250);

/// The ceiling the reconnect backoff doubles up to.
///
/// Thirty seconds: long enough that a replica restarting under a thundering
/// herd of its own subscribers is not made worse by them, short enough that a
/// consumer of a quiet set is not left minutes behind once the replica is back.
pub const DEFAULT_MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

/// How long a [`JobEventStream`] may go without a single **complete frame**
/// before it gives up on the connection.
///
/// The server bookmarks progress every 15 s (`EVENT_PROGRESS_INTERVAL`, ADR
/// 0043), and that bookmark doubles as the stream's keepalive — a healthy
/// connection is never quiet for long. Four missed intervals is not "a quiet
/// set", it is a connection nothing is coming down any more (a load balancer
/// that dropped it silently, most often), so this is set well above one
/// interval but nowhere near patient enough to mistake the two for each other.
///
/// Only a fully dispatched frame — one the parser has assembled end to end,
/// whether or not this client acts on it — resets the clock. An SSE comment
/// line, or a frame trickling in one byte at a time and never finishing,
/// does not: something that looks like traffic but never lands a frame is
/// exactly the "not really there" connection this timeout exists to catch.
pub const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One item delivered by a job-event subscription — ADR 0043's three frames,
/// with the SSE framing already gone.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum JobEventItem {
    /// One applied command's events that the filter admitted, never split.
    Batch(EventBatchFrame),
    /// Everything this subscription matches at or below `index` has been
    /// delivered. Also the stream's keepalive.
    Progress {
        /// The covered-through applied index.
        index: u64,
    },
    /// Delivery was discontinuous: what comes next does **not** follow on from
    /// what came before. The stream continues live; a consumer that tracks
    /// state owes a re-query (see [`JobWatcher`]).
    Gap {
        /// The oldest index this replica could still have served.
        earliest_available: u64,
    },
}

/// One open subscription to `GET /api/v1/events`.
///
/// Built by [`Client::subscribe_job_events`]. Each
/// [`next_item`](Self::next_item) reads as much of the response body as the
/// next frame needs and returns it; `Ok(None)` is the server ending the stream
/// cleanly, which it does when the credential that opened the stream expires,
/// when the replica drains, or when its fanout stops (ADR 0043). None of those
/// are errors, and all of them mean the same thing: reconnect with the cursor.
/// [`JobEventWatcher`] is that, done for you.
///
/// Dropping the stream is how a consumer unsubscribes; the replica notices the
/// connection is gone and removes it from the fanout.
#[derive(Debug)]
pub struct JobEventStream {
    /// `None` once the body has ended — cleanly or not.
    response: Option<reqwest::Response>,
    parser: SseParser,
    cursor: Option<u64>,
    /// How long a read may wait for the next complete frame before the
    /// connection is declared dead. See [`DEFAULT_STREAM_IDLE_TIMEOUT`].
    idle_timeout: Duration,
    /// The instant the current wait must beat. One deadline, held across
    /// calls to [`next_item`](Self::next_item) and moved forward only when a
    /// complete frame is dispatched — never merely by bytes arriving. See
    /// [`DEFAULT_STREAM_IDLE_TIMEOUT`].
    deadline: tokio::time::Instant,
}

impl JobEventStream {
    pub(crate) fn new(response: reqwest::Response, cursor: Option<u64>) -> JobEventStream {
        JobEventStream {
            response: Some(response),
            parser: SseParser::new(),
            cursor,
            idle_timeout: DEFAULT_STREAM_IDLE_TIMEOUT,
            deadline: tokio::time::Instant::now() + DEFAULT_STREAM_IDLE_TIMEOUT,
        }
    }

    /// Replace the default idle timeout (see [`DEFAULT_STREAM_IDLE_TIMEOUT`]).
    pub fn with_idle_timeout(mut self, idle: Duration) -> JobEventStream {
        self.idle_timeout = idle;
        self.deadline = tokio::time::Instant::now() + idle;
        self
    }

    /// The next item, or `None` once the server has ended the stream.
    ///
    /// Cancel-safe only at the granularity a caller usually wants: dropping
    /// the future mid-body drops the stream with it, which is the same thing
    /// as unsubscribing. Do not call it again after an `Err` — the body is
    /// gone and every later call answers `None`.
    ///
    /// A read that produces no complete frame — not even the `progress`
    /// bookmark that doubles as the server's keepalive — for longer than the
    /// idle timeout ends the stream with [`Error::StreamIdle`], since a
    /// connection this quiet is not merely idle, it is gone. Bytes that do
    /// not add up to a frame — an SSE comment line, a chunk that trails off
    /// mid-frame — do not postpone that: they are not proof the other end is
    /// still there. See [`DEFAULT_STREAM_IDLE_TIMEOUT`].
    pub async fn next_item(&mut self) -> Result<Option<JobEventItem>> {
        loop {
            while let Some(frame) = self.parser.next_frame() {
                // A complete frame is proof of life, whether or not this
                // client acts on it — see `decode`.
                self.deadline = tokio::time::Instant::now() + self.idle_timeout;
                if let Some(item) = self.decode(frame)? {
                    return Ok(Some(item));
                }
            }
            let Some(response) = self.response.as_mut() else {
                return Ok(None);
            };
            match tokio::time::timeout_at(self.deadline, response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.parser.push(chunk.as_ref()),
                // A clean end of body. A frame that arrived without its
                // trailing blank line is still a frame; anything after it is
                // the next connection's problem.
                Ok(Ok(None)) => {
                    self.response = None;
                    if let Some(frame) = self.parser.finish() {
                        if let Some(item) = self.decode(frame)? {
                            return Ok(Some(item));
                        }
                    }
                    return Ok(None);
                }
                Ok(Err(e)) => {
                    self.response = None;
                    return Err(Error::Transport(e));
                }
                Err(_) => {
                    self.response = None;
                    return Err(Error::StreamIdle {
                        idle: self.idle_timeout,
                    });
                }
            }
        }
    }

    /// The last resumable position this stream saw — the id of the most recent
    /// `batch` or `progress` frame.
    ///
    /// This is what a reconnect sends as `Last-Event-ID`. A `gap` does not move
    /// it, deliberately: a gap is not a position anything can resume from.
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    /// One SSE frame as an item, or `None` for a frame this client has no
    /// business acting on.
    ///
    /// An unrecognized event name is ignored rather than refused: the server
    /// may name a fourth frame in a release this client predates, and a
    /// subscription that died on it would lose the three it does understand.
    fn decode(&mut self, frame: SseFrame) -> Result<Option<JobEventItem>> {
        let item = match frame.event.as_str() {
            "batch" => {
                let body: EventBatchFrame = parse(&frame.data)?;
                self.advance(&frame, body.index);
                JobEventItem::Batch(body)
            }
            "progress" => {
                let body: EventProgressFrame = parse(&frame.data)?;
                self.advance(&frame, body.index);
                JobEventItem::Progress { index: body.index }
            }
            "gap" => {
                let body: EventGapFrame = parse(&frame.data)?;
                JobEventItem::Gap {
                    earliest_available: body.earliest_available,
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(item))
    }

    /// Move the cursor to this frame's position.
    ///
    /// The SSE `id:` is the authority — it is what the protocol says a
    /// reconnect carries — and the body's own index is the fallback, for a
    /// frame whose id was somehow not a number. The two are the same value on
    /// every frame the server sends.
    fn advance(&mut self, frame: &SseFrame, index: u64) {
        let id = frame
            .id
            .as_deref()
            .and_then(|id| id.trim().parse::<u64>().ok());
        self.cursor = Some(id.unwrap_or(index));
    }
}

/// A frame's `data:` payload as the body it should be.
fn parse<T: serde::de::DeserializeOwned>(data: &str) -> Result<T> {
    serde_json::from_str(data).map_err(Error::Decode)
}

/// How much of the matching set a [`JobWatcher`]'s snapshot lists.
///
/// The snapshot exists to give a consumer a starting state to apply events
/// to, and the state worth starting from is the jobs whose execution has not
/// finished. A finished job is not silent — its metadata can still be
/// edited, and it is eventually evicted, and the subscription (whose filter
/// is the caller's, untouched) delivers both — but its outcome is settled,
/// and a consumer that needs it reads that one job. Listing every terminal
/// job the cluster still retains would make the snapshot's size a function
/// of retention — today the eviction horizon, tomorrow a durable history
/// store — rather than of the working set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotScope {
    /// The caller's filter **and** a non-terminal phase: only jobs that have
    /// not yet finished. The default.
    ///
    /// The phase leaf is added to the `ListJobs` query alone; the
    /// subscription's filter is the caller's, untouched, so a job's events
    /// keep arriving right through the transition that makes it terminal.
    #[default]
    Live,
    /// The caller's filter exactly as given: every job it matches that the
    /// cluster still retains, terminal ones included.
    ///
    /// The honest cost is unbounded: the walk is as long as retention is
    /// deep, and holds no more than a page at a time but issues a request per
    /// page. Worth it for "reconcile everything I have ever been told about",
    /// not for keeping a dashboard current.
    All,
}

/// How a [`JobEventWatcher`] or a [`JobWatcher`] should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WatchOptions {
    /// Resume from just after this applied index. `None` starts the
    /// subscription live — from whatever the replica has reached now.
    ///
    /// Ignored by [`Client::watch_jobs`], which takes its first cursor from
    /// the list it does anyway.
    pub cursor: Option<u64>,
    /// The first reconnect delay.
    pub min_backoff: Duration,
    /// The ceiling the delay doubles up to.
    pub max_backoff: Duration,
    /// How long a connection may go without a single frame before it is
    /// dropped and reopened from the cursor. See
    /// [`DEFAULT_STREAM_IDLE_TIMEOUT`].
    pub idle_timeout: Duration,
    /// How much of the matching set [`Client::watch_jobs`]'s snapshot lists.
    /// Ignored by [`Client::watch_job_events`], which lists nothing.
    pub snapshot_scope: SnapshotScope,
}

impl Default for WatchOptions {
    fn default() -> WatchOptions {
        WatchOptions {
            cursor: None,
            min_backoff: DEFAULT_MIN_RECONNECT_BACKOFF,
            max_backoff: DEFAULT_MAX_RECONNECT_BACKOFF,
            idle_timeout: DEFAULT_STREAM_IDLE_TIMEOUT,
            snapshot_scope: SnapshotScope::Live,
        }
    }
}

impl WatchOptions {
    /// Start live, with the default backoff bounds.
    pub fn new() -> WatchOptions {
        WatchOptions::default()
    }

    /// Resume from just after `cursor`.
    pub fn with_cursor(mut self, cursor: u64) -> WatchOptions {
        self.cursor = Some(cursor);
        self
    }

    /// Set the first reconnect delay.
    pub fn with_min_backoff(mut self, backoff: Duration) -> WatchOptions {
        self.min_backoff = backoff;
        self
    }

    /// Set the ceiling the reconnect delay doubles up to.
    pub fn with_max_backoff(mut self, backoff: Duration) -> WatchOptions {
        self.max_backoff = backoff;
        self
    }

    /// Set how long a connection may go without a frame before it is dropped
    /// and reopened.
    pub fn with_idle_timeout(mut self, idle: Duration) -> WatchOptions {
        self.idle_timeout = idle;
        self
    }

    /// Set how much of the matching set the snapshot lists. See
    /// [`SnapshotScope`]; the default is [`SnapshotScope::Live`].
    pub fn with_snapshot_scope(mut self, scope: SnapshotScope) -> WatchOptions {
        self.snapshot_scope = scope;
        self
    }
}

/// A subscription that survives its connections.
///
/// Built by [`Client::watch_job_events`]. It opens a [`JobEventStream`], and
/// when that stream ends — cleanly, because the credential expired or the
/// replica drained, or with a retryable failure — it opens another one from
/// the cursor it was holding, and drops the batches the resume replays. The
/// caller sees one uninterrupted sequence.
///
/// What it does **not** paper over:
///
/// - A [`JobEventItem::Gap`] is handed straight to the caller. Continuity is
///   the one thing a reconnect cannot restore, and pretending otherwise is the
///   bug this type exists to avoid.
/// - A non-retryable failure — a filter the server refuses, a credential it
///   rejects — ends the watch with that error. Reconnecting could only
///   reproduce it.
///
/// Each reconnect is an ordinary request: it waits for the client's rate-limit
/// slot and asks the [`TokenProvider`](crate::TokenProvider) for a token,
/// exactly as any other call does. An *open* stream holds neither.
#[derive(Debug)]
pub struct JobEventWatcher {
    client: Client,
    filter: JobFilter,
    options: WatchOptions,
    stream: Option<JobEventStream>,
    cursor: Option<u64>,
    /// The delay the next reconnect pays, doubling until `max_backoff`.
    backoff: Duration,
    /// Whether the next reconnect waits at all. A connection that delivered
    /// something was working, so the one replacing it starts immediately.
    delay_next: bool,
    /// Set when the watch has ended: after its one error, or once a caller
    /// has been told it is over.
    finished: bool,
}

impl JobEventWatcher {
    pub(crate) fn new(client: Client, filter: JobFilter, options: WatchOptions) -> JobEventWatcher {
        JobEventWatcher {
            client,
            filter,
            cursor: options.cursor,
            backoff: options.min_backoff,
            options,
            stream: None,
            delay_next: false,
            finished: false,
        }
    }

    /// The next item, reconnecting as often as it takes.
    ///
    /// Answers `Ok(None)` only once the watch is over, which today means only
    /// after it has already returned the error that ended it: the error is
    /// reported once, and every call after that is `Ok(None)`.
    pub async fn next_item(&mut self) -> Result<Option<JobEventItem>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            if self.stream.is_none() {
                if self.delay_next {
                    tokio::time::sleep(jittered(self.backoff)).await;
                    self.backoff = (self.backoff * 2).min(self.options.max_backoff);
                }
                self.delay_next = true;
                match self
                    .client
                    .subscribe_job_events(&self.filter, self.cursor)
                    .await
                {
                    Ok(stream) => {
                        self.stream = Some(stream.with_idle_timeout(self.options.idle_timeout))
                    }
                    Err(e) if e.is_retryable() => continue,
                    Err(e) => return Err(self.fail(e)),
                }
            }

            let stream = self.stream.as_mut().expect("just opened");
            match stream.next_item().await {
                Ok(Some(item)) => {
                    // The connection works: the next reconnect need not wait,
                    // and the one after a future failure starts from the floor.
                    self.backoff = self.options.min_backoff;
                    self.delay_next = false;
                    if let Some(item) = self.admit(item) {
                        return Ok(Some(item));
                    }
                }
                // The server ended it. Expected, and it says nothing is wrong.
                Ok(None) => self.stream = None,
                Err(e) if e.is_retryable() => self.stream = None,
                Err(e) => return Err(self.fail(e)),
            }
        }
    }

    /// The position a reconnect would resume from.
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    /// Whether the watch has ended.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Keep an item if it is new, and move the cursor with it.
    ///
    /// This is where at-least-once becomes exactly-once for the caller: a
    /// resume may re-deliver the batch the cursor named, and anything at or
    /// below the cursor has already been handed over. A gap is always kept and
    /// never moves the cursor.
    fn admit(&mut self, item: JobEventItem) -> Option<JobEventItem> {
        let seen = |cursor: Option<u64>, index: u64| cursor.is_some_and(|c| index <= c);
        match &item {
            JobEventItem::Batch(batch) if seen(self.cursor, batch.index) => None,
            JobEventItem::Batch(batch) => {
                self.cursor = Some(batch.index);
                Some(item)
            }
            JobEventItem::Progress { index } if seen(self.cursor, *index) => None,
            JobEventItem::Progress { index } => {
                self.cursor = Some(*index);
                Some(item)
            }
            JobEventItem::Gap { .. } => Some(item),
        }
    }

    /// End the watch on `e`, which is returned once and never again.
    fn fail(&mut self, e: Error) -> Error {
        self.finished = true;
        self.stream = None;
        e
    }
}

/// One item from [`Client::watch_jobs`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum JobWatchItem {
    /// One page of a snapshot of the matching set. A watch opens with a run
    /// of these — `first` on the first, `last` on the last — and every gap
    /// produces another run.
    ///
    /// The pages are read one per [`JobWatcher::next_item`] call, so a
    /// consumer holds only what it has chosen to keep, and the watcher never
    /// holds more than one page of rows. What the watcher does keep for the
    /// length of a run is a compact id → index entry for each job read on a
    /// page newer than the first, which is what lets it suppress events a
    /// row already reflects; it is dropped once the stream has passed the
    /// newest page. A run is **not** a consistent cut:
    /// each page carries its own `index`, all at or above the first page's.
    /// See [`JobWatcher`] for what that does and does not cost.
    SnapshotPage {
        /// The rows on this page. Empty is possible on `first` or `last` — an
        /// empty matching set is one empty page that is both — and an empty
        /// page in the middle of a run is never surfaced.
        jobs: Vec<JobSummary>,
        /// The applied index **this page** was served at. `None` in the one
        /// case the replica reported no index; on the first page that also
        /// means the subscription starts live, since there is no cursor to
        /// resume from.
        index: Option<u64>,
        /// This page starts a snapshot: discard whatever was reconciled from
        /// the previous run and begin again.
        first: bool,
        /// This page ends the snapshot: everything the list had to say has
        /// been said, and event batches follow.
        last: bool,
    },
    /// One applied command's events for the matching set, never split.
    ///
    /// Events a snapshot row already reflected are removed before delivery;
    /// a batch left with nothing is not delivered at all. Ordinals are the
    /// server's batch positions throughout — a surviving event keeps the
    /// ordinal it arrived with, gaps included.
    Batch(EventBatchFrame),
}

/// The ADR 0043 loop: list, subscribe from that read's index, and on a gap do
/// both again.
///
/// Built by [`Client::watch_jobs`]. The snapshot arrives as a run of
/// [`JobWatchItem::SnapshotPage`]s — one list request per item, so neither
/// this type nor the caller is ever holding the whole set's rows; what this
/// type keeps for the length of a run is an id and an index per job read
/// ahead of the cursor, for the suppression described below — and between
/// snapshots the event batches come through as [`JobEventWatcher`] delivers
/// them. Progress bookmarks are consumed here, since their job — keeping the
/// cursor fresh — is this type's own business.
///
/// ## The default snapshot is the live set
///
/// `ListJobs` is asked for the caller's filter **and** a non-terminal phase
/// (see [`SnapshotScope`]), so the snapshot is bounded by the cluster's
/// working set rather than by how much history it retains. The
/// *subscription's* filter is the caller's, unchanged — the server forbids a
/// `phase` leaf there anyway, and a stream must keep reporting a job through
/// the transition that ends it.
///
/// Two consequences a caller owns:
///
/// - **A job you track that is absent from a `Live` snapshot has left the
///   live set** — it finished, or it was evicted. The snapshot does not say
///   which, and cannot: it says only "not live now". Read it individually
///   ([`Client::job`]) if you need the outcome, passing
///   [`ReadOptions::at_least`](crate::ReadOptions::at_least) set to the page's
///   `index` so a lagging replica cannot answer with state from before the
///   list that omitted it.
/// - **At startup, jobs whose state you do not know are your problem.** A
///   watch tells you about the live set and everything that happens next; a
///   process resuming from its own durable state polls the old jobs it still
///   believes are unfinished, rather than waiting for a stream that will
///   never mention them again.
///
/// ## What a snapshot run promises
///
/// - The first item of a watch is a `SnapshotPage` with `first: true`, and the
///   run ends with `last: true`. Every `gap` starts another run, never a
///   silent discontinuity.
/// - Each page carries **its own** `index`. Pages after the first are read
///   with [`ReadOptions::at_least`](crate::ReadOptions::at_least) pinned to
///   the first page's index, so a run's indexes are non-decreasing in the
///   sense that matters: none is older than the cursor the subscription
///   resumes from. The run is therefore **not a consistent cut** — a later
///   page may reflect changes an earlier page could not have seen.
/// - Nothing is lost to that fuzziness, because the subscription resumes from
///   the **first** page's index: every change at or after it is delivered as
///   an event.
/// - **Per job, delivery is clean**: while walking, the watcher records which
///   page index carried each job's row, and drops from each batch every event
///   whose job has a recorded index at or above the batch's index — the row
///   already reflects it. A batch left with no events is not delivered.
///   Events for jobs the snapshot never mentioned (a job already terminal at
///   its page's index, under `Live`) pass through untouched, as do the
///   cluster-scoped kinds that name no job. So after a row for job `J` at
///   index `i`, a caller sees exactly the events for `J` above `i`. The
///   record is discarded once the stream has reached the highest page index
///   of the run, which is when it can no longer suppress anything.
/// - Ordinals are never rewritten: a surviving event keeps the batch position
///   the server assigned it.
///
/// ## Where it can still restart
///
/// A long walk is time spent not subscribed. If it outlasts the replica's
/// reconnection ring window, the subscription opened from the first page's
/// index meets a `gap` immediately and the whole snapshot starts over — with
/// the resync read pinned to the gap's `earliest_available`. Under
/// [`SnapshotScope::All`] over deep retention that is a real risk, and it is
/// the second reason the default scope is [`Live`](SnapshotScope::Live).
///
/// One more failure mode worth naming: under `Live` the phase leaf is added to
/// the caller's filter, which adds a node (and, unless the caller's filter is
/// already a top-level `all`, one level of nesting). A filter already at
/// `ListJobs`'s cap — depth
/// [`MAX_FILTER_DEPTH`](crate::MAX_FILTER_DEPTH), or
/// [`MAX_FILTER_NODES`](crate::MAX_FILTER_NODES) nodes — therefore becomes
/// invalid, which is an [`Error::InvalidRequest`] (the same wording the server
/// would have sent as `INVALID_ARGUMENT`), and that ends the watch. Leave a
/// node spare, or pass [`SnapshotScope::All`].
#[derive(Debug)]
pub struct JobWatcher {
    client: Client,
    filter: JobFilter,
    options: WatchOptions,
    events: Option<JobEventWatcher>,
    /// The snapshot walk in progress, if there is one. Present exactly
    /// between the first page being asked for and the last being returned.
    walk: Option<Walk>,
    /// Which page index carried each job's row, for the snapshot being
    /// reconciled. `None` once the stream has caught up with the newest page
    /// and there is nothing left to suppress.
    rows: Option<SnapshotRows>,
    /// The `earliest_available` of the gap being answered, which the resync
    /// list must be read no older than. A gap raised by something that
    /// happened *at* an index — a quota entity moving under a subtree filter
    /// (ADR 0043) — is only answered by a read that reflects that index; a
    /// lagging replica's older list would walk the new subscription straight
    /// back into the same gap.
    resync_floor: Option<u64>,
    backoff: Duration,
    finished: bool,
}

/// One snapshot walk, part way through.
#[derive(Debug)]
struct Walk {
    /// The next list request, cursor included. It advances only after a page
    /// has been read, which is what makes a retryable failure retry **that
    /// page** rather than restart the walk.
    params: ListJobsParams,
    /// The applied index the first page was served at: the pin for every
    /// later page, and the subscription's opening cursor.
    first_index: Option<u64>,
    /// Whether the first page has been read.
    started: bool,
}

/// What a snapshot's pages already said, so the subscription need not say it
/// again.
#[derive(Debug, Default)]
struct SnapshotRows {
    /// Job id → the applied index of the page its row came from. A page that
    /// reported no index records nothing: there is no index to compare
    /// against, so its rows suppress nothing.
    by_job: HashMap<JobId, u64>,
    /// The highest index any page of this run was served at. The stream
    /// reaching it — by batch or by bookmark — retires the whole map.
    highest: u64,
}

impl JobWatcher {
    pub(crate) fn new(client: Client, filter: JobFilter, options: WatchOptions) -> JobWatcher {
        JobWatcher {
            client,
            filter,
            backoff: options.min_backoff,
            options,
            events: None,
            walk: None,
            rows: None,
            resync_floor: None,
            finished: false,
        }
    }

    /// The next item: one snapshot page, or a batch of events since the
    /// snapshot.
    ///
    /// Exactly one list request per snapshot page, so a walk advances only as
    /// fast as it is consumed. Like [`JobEventWatcher::next_item`], a
    /// retryable failure is retried rather than returned — of the list as well
    /// as of the subscription, and a list retry re-sends the page that failed,
    /// cursor and all — and `Ok(None)` follows the one error that ends the
    /// watch.
    pub async fn next_item(&mut self) -> Result<Option<JobWatchItem>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            // Neither walking nor subscribed: the start of the watch, or the
            // resync a gap demanded.
            if self.walk.is_none() && self.events.is_none() {
                self.begin_snapshot();
            }

            if self.walk.is_some() {
                match self.next_page().await {
                    Ok(Some(page)) => return Ok(Some(page)),
                    // An empty page that neither opens nor closes the run:
                    // the server's scan budget ran out before its `limit`
                    // did, which says nothing a caller can act on.
                    Ok(None) => continue,
                    Err(e) if e.is_retryable() => {
                        tokio::time::sleep(jittered(self.backoff)).await;
                        self.backoff = (self.backoff * 2).min(self.options.max_backoff);
                        continue;
                    }
                    Err(e) => {
                        self.finished = true;
                        return Err(e);
                    }
                }
            }

            let events = self.events.as_mut().expect("the last page built one");
            match events.next_item().await {
                Ok(Some(JobEventItem::Batch(batch))) => {
                    if let Some(batch) = self.admit(batch) {
                        return Ok(Some(JobWatchItem::Batch(batch)));
                    }
                }
                // The bookmark's whole purpose is keeping the cursor fresh,
                // which the watcher underneath has already done — but it is
                // also proof the stream has passed an index, which is what
                // retires the snapshot's rows on a quiet set.
                Ok(Some(JobEventItem::Progress { index })) => self.settle(index),
                // The resync ADR 0008 asks for: the same filter, read again,
                // and a subscription from the index that read reports.
                Ok(Some(JobEventItem::Gap { earliest_available })) => {
                    self.resync_floor = Some(earliest_available);
                    self.events = None;
                }
                Ok(None) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(e) => {
                    self.finished = true;
                    return Err(e);
                }
            }
        }
    }

    /// Whether the watch has ended.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Start a snapshot: a fresh walk, and a fresh (empty) row record, which
    /// replaces whatever the previous snapshot left behind.
    fn begin_snapshot(&mut self) {
        let params = ListJobsParams::new()
            .with_filter(self.list_filter())
            // The whole point of the walk is to get through the set in as few
            // requests as the server permits.
            .with_limit(MAX_LIST_JOBS_LIMIT);
        self.walk = Some(Walk {
            params,
            first_index: None,
            started: false,
        });
        self.rows = Some(SnapshotRows::default());
    }

    /// The `ListJobs` filter for the current [`SnapshotScope`].
    ///
    /// Under [`SnapshotScope::Live`] the caller's filter is ANDed with a
    /// phase leaf naming every non-terminal phase — derived from
    /// [`JobPhase::ALL`] and [`JobPhase::is_terminal`], so a phase a later
    /// release adds is included without anyone having to remember to. The
    /// AND is built without deepening the tree where it need not be: a
    /// top-level `all` gets one more child, anything else is wrapped.
    fn list_filter(&self) -> JobFilter {
        match self.options.snapshot_scope {
            SnapshotScope::All => self.filter.clone(),
            SnapshotScope::Live => match self.filter.clone() {
                JobFilter::All(mut filters) => {
                    filters.push(live_phase_leaf());
                    JobFilter::All(filters)
                }
                filter => JobFilter::all([filter, live_phase_leaf()]),
            },
        }
    }

    /// Read the walk's next page, and hand it over unless it is an empty page
    /// in the middle of the run.
    ///
    /// Each page is its own independent bounded read, and behind a load
    /// balancer a later page can land on a replica that has not caught up to
    /// the one the first page hit. Without a floor, a job that sorts into
    /// that later page and changed in the gap between the two replicas'
    /// indexes would be missing from the snapshot *and* older than the cursor
    /// the subscription resumes from — invisible forever. So once the first
    /// page has reported an index, every remaining page is read with
    /// [`ReadOptions::at_least`](crate::ReadOptions::at_least) pinned to it.
    /// If the first page reports no index at all, there is nothing to pin
    /// later pages to, and they are read as-is.
    ///
    /// A snapshot answering a gap is additionally read no older than the
    /// gap's `earliest_available` (see `resync_floor`).
    async fn next_page(&mut self) -> Result<Option<JobWatchItem>> {
        let (params, first_index, first) = {
            let walk = self.walk.as_ref().expect("a walk in progress");
            (walk.params.clone(), walk.first_index, !walk.started)
        };
        let floor = [first_index, self.resync_floor].into_iter().flatten().max();
        let client = match floor {
            Some(floor) => self.floored(floor),
            None => self.client.clone(),
        };

        let Versioned {
            value: ListJobsResponse { jobs, next_cursor },
            applied_index: index,
            ..
        } = client.list_jobs(&params).await?;
        // The page landed, so the next failure starts from the backoff floor.
        self.backoff = self.options.min_backoff;
        let last = next_cursor.is_none();

        {
            let walk = self.walk.as_mut().expect("a walk in progress");
            walk.started = true;
            if first {
                walk.first_index = index;
            }
            if let Some(cursor) = next_cursor {
                walk.params.cursor = Some(cursor);
            }
        }
        // A page with no index of its own can suppress nothing: there is no
        // index to compare an event against.
        //
        // Nor does a page read at the first page's index need recording: the
        // subscription resumes strictly after that index, so nothing at or
        // below it is ever replayed. Only a page that was read *newer* than
        // the cursor can have rows ahead of the stream.
        let cursor = self.walk.as_ref().and_then(|walk| walk.first_index);
        let ahead_of_cursor = |index: u64| cursor.is_none_or(|first| index > first);
        if let (Some(index), Some(rows)) = (index, self.rows.as_mut()) {
            if ahead_of_cursor(index) {
                rows.highest = rows.highest.max(index);
                for job in &jobs {
                    rows.by_job.insert(job.id, index);
                }
            }
        }

        if last {
            // The subscription resumes from the *first* page's index: every
            // change at or after it is on the stream, whatever later pages
            // happened to reflect. Opening it is deferred to the first read.
            let mut options = self.options;
            options.cursor = self.walk.as_ref().and_then(|walk| walk.first_index);
            self.walk = None;
            self.events = Some(JobEventWatcher::new(
                self.client.clone(),
                self.filter.clone(),
                options,
            ));
            if self
                .rows
                .as_ref()
                .is_some_and(|rows| rows.by_job.is_empty())
            {
                self.rows = None;
            }
        }

        // The server ends a page early for its own reasons (a scan budget, a
        // byte cap), so an empty page mid-run is routine and says nothing.
        // The run's boundaries are always surfaced, empty or not.
        if jobs.is_empty() && !first && !last {
            return Ok(None);
        }
        Ok(Some(JobWatchItem::SnapshotPage {
            jobs,
            index,
            first,
            last,
        }))
    }

    /// Drop from `batch` the events the snapshot already reflected, and say
    /// whether anything is left worth delivering.
    ///
    /// An event survives unless its job has a recorded row index at or above
    /// the batch's index — the row was read after the event applied, so it
    /// already includes it. An event naming no job, or a job this snapshot
    /// never listed, is never suppressed.
    fn admit(&mut self, mut batch: EventBatchFrame) -> Option<EventBatchFrame> {
        let index = batch.index;
        if let Some(rows) = self.rows.as_ref() {
            batch.events.retain(|event| match event.body.job() {
                Some(job) => rows.by_job.get(&job).is_none_or(|row| *row < index),
                None => true,
            });
        }
        // Whether or not anything survived, the stream has now reached this
        // index, which may be the last one the rows could have mattered at.
        self.settle(index);
        (!batch.events.is_empty()).then_some(batch)
    }

    /// Retire the snapshot's rows once the stream has reached the newest page
    /// index they came from: above it, no row can be newer than an event.
    fn settle(&mut self, index: u64) {
        if self.rows.as_ref().is_some_and(|rows| index >= rows.highest) {
            self.rows = None;
        }
    }

    /// The caller's client with its reads floored at `index`.
    ///
    /// A floor is added to the caller's read options, never substituted for
    /// them: a client built to read `Strong` still does, and a `min_index` it
    /// already carried is only ever raised.
    fn floored(&self, index: u64) -> Client {
        let options = *self.client.read_options();
        let floor = options
            .min_index
            .map_or(index, |existing| existing.max(index));
        self.client.with_read_options(options.with_min_index(floor))
    }
}

/// The phase leaf naming every phase a job can still be changing in.
///
/// Derived from the vocabulary rather than listed: a phase added to
/// [`JobPhase::ALL`] is live unless [`JobPhase::is_terminal`] says otherwise,
/// so the live set cannot silently lose a new phase. `Unknown` is not in
/// `ALL` — it is a decoding catch-all, and no request may name it.
fn live_phase_leaf() -> JobFilter {
    JobFilter::phase_in(
        JobPhase::ALL
            .into_iter()
            .filter(|phase| !phase.is_terminal()),
    )
}

/// Half of `base`, plus a random slice of the other half.
///
/// Spread, not cryptography: the point is that a replica restarting does not
/// get every subscriber it dropped back at the same instant. The clock's
/// nanoseconds are entropy enough for that, and cost no dependency.
fn jittered(base: Duration) -> Duration {
    let half = base / 2;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    half + half.mul_f64(f64::from(nanos) / 1e9)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_inside_the_delay_it_spreads() {
        for base in [
            Duration::from_millis(1),
            DEFAULT_MIN_RECONNECT_BACKOFF,
            DEFAULT_MAX_RECONNECT_BACKOFF,
        ] {
            let delay = jittered(base);
            assert!(delay >= base / 2, "{delay:?} < {base:?}/2");
            assert!(delay <= base, "{delay:?} > {base:?}");
        }
    }

    #[test]
    fn watch_options_default_to_the_documented_bounds() {
        let options = WatchOptions::new();
        assert_eq!(options.cursor, None);
        assert_eq!(options.min_backoff, DEFAULT_MIN_RECONNECT_BACKOFF);
        assert_eq!(options.max_backoff, DEFAULT_MAX_RECONNECT_BACKOFF);
        assert_eq!(options.snapshot_scope, SnapshotScope::Live);
        assert_eq!(WatchOptions::new().with_cursor(9).cursor, Some(9));
        assert_eq!(
            WatchOptions::new()
                .with_snapshot_scope(SnapshotScope::All)
                .snapshot_scope,
            SnapshotScope::All
        );
    }

    /// The live set is every phase but the three terminal ones, and it is
    /// read off the vocabulary — this pins both halves of that: the three
    /// that must be absent, and that nothing else is.
    #[test]
    fn the_live_phase_leaf_is_every_non_terminal_phase() {
        let JobFilter::Phase(leaf) = live_phase_leaf() else {
            panic!("a phase leaf");
        };
        assert_eq!(
            leaf.r#in,
            vec![
                JobPhase::Submitted,
                JobPhase::Accepted,
                JobPhase::Queued,
                JobPhase::Accruing,
                JobPhase::Preparing,
                JobPhase::Running,
                JobPhase::Finalizing,
            ]
        );
        for terminal in [JobPhase::Succeeded, JobPhase::Failed, JobPhase::Aborted] {
            assert!(terminal.is_terminal());
            assert!(!leaf.r#in.contains(&terminal), "{terminal}");
        }
        assert!(live_phase_leaf().validate().is_ok());
    }

    fn watcher(filter: JobFilter, options: WatchOptions) -> JobWatcher {
        let client = Client::new("http://127.0.0.1:1").expect("a parseable base");
        JobWatcher::new(client, filter, options)
    }

    /// A caller's `all` gains a child; anything else is wrapped once. Both
    /// spend exactly one node on the phase leaf, and only the second spends a
    /// level of depth.
    #[test]
    fn the_live_list_filter_appends_rather_than_nests_where_it_can() {
        let leaf = JobFilter::metadata_equals("owner", "batch-service");
        let wrapped = watcher(leaf.clone(), WatchOptions::new()).list_filter();
        assert_eq!(wrapped, JobFilter::all([leaf.clone(), live_phase_leaf()]));

        let already_all = JobFilter::all([leaf.clone(), JobFilter::submitted_by("alice")]);
        assert_eq!(
            watcher(already_all, WatchOptions::new()).list_filter(),
            JobFilter::all([
                leaf.clone(),
                JobFilter::submitted_by("alice"),
                live_phase_leaf()
            ])
        );

        let all_scope = WatchOptions::new().with_snapshot_scope(SnapshotScope::All);
        assert_eq!(watcher(leaf.clone(), all_scope).list_filter(), leaf);
    }

    fn event(index: u64, ordinal: u32, job: Option<JobId>) -> crate::types::TimelineEvent {
        crate::types::TimelineEvent {
            index,
            ordinal,
            at: crate::Timestamp::UNIX_EPOCH,
            body: match job {
                Some(job) => crate::types::TimelineEventBody::JobSubmitted { job },
                None => crate::types::TimelineEventBody::PolicyUpdated,
            },
        }
    }

    fn batch(index: u64, events: Vec<crate::types::TimelineEvent>) -> EventBatchFrame {
        EventBatchFrame {
            index,
            at: crate::Timestamp::UNIX_EPOCH,
            events,
        }
    }

    /// The suppression rule, exactly: an event is dropped iff its job's row
    /// was read at or above the batch's index.
    #[test]
    fn a_row_suppresses_only_the_events_it_already_reflects() {
        let a = JobId::new();
        let b = JobId::new();
        let c = JobId::new();
        let mut watch = watcher(JobFilter::metadata_present("owner"), WatchOptions::new());
        watch.rows = Some(SnapshotRows {
            by_job: HashMap::from([(a, 120), (b, 100)]),
            highest: 120,
        });

        // Below A's row and above B's: A goes, B stays, and so do the job C
        // the snapshot never listed and the event naming no job at all.
        let admitted = watch
            .admit(batch(
                110,
                vec![
                    event(110, 0, Some(a)),
                    event(110, 1, Some(b)),
                    event(110, 2, Some(c)),
                    event(110, 3, None),
                ],
            ))
            .expect("three events survive");
        assert_eq!(
            admitted
                .events
                .iter()
                .map(|e| e.ordinal)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the survivors keep the ordinals the server assigned"
        );

        // Nothing but A, below A's row: nothing left to deliver.
        assert!(watch
            .admit(batch(115, vec![event(115, 0, Some(a))]))
            .is_none());
        assert!(watch.rows.is_some(), "the rows outlive a suppressed batch");

        // At or above the highest row, the map has done its job and goes.
        let admitted = watch
            .admit(batch(125, vec![event(125, 0, Some(a))]))
            .expect("above A's row, A's events are news again");
        assert_eq!(admitted.events.len(), 1);
        assert!(
            watch.rows.is_none(),
            "the rows are retired at the high mark"
        );
    }

    /// A bookmark retires the rows too: on a quiet set no batch may ever
    /// arrive, and holding the map forever would be a leak.
    #[test]
    fn a_bookmark_past_the_high_mark_retires_the_rows() {
        let mut watch = watcher(JobFilter::metadata_present("owner"), WatchOptions::new());
        watch.rows = Some(SnapshotRows {
            by_job: HashMap::from([(JobId::new(), 100)]),
            highest: 100,
        });
        watch.settle(99);
        assert!(watch.rows.is_some());
        watch.settle(100);
        assert!(watch.rows.is_none());
    }
}
