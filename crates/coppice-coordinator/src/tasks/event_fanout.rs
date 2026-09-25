//! Event fanout (every replica).
//!
//! Consumes the [`EventTapReceiver`], owns the ADR 0008 reconnection ring
//! (bounded: [`FANOUT_RING_MAX_AGE`] / [`FANOUT_RING_MAX_EVENTS`] /
//! [`FANOUT_RING_MAX_BYTES`]), and manages subscriptions. See
//! `docs/architecture/coordinator-runtime.md`, "Event fanout" and the
//! channel-inventory rows for "fanout ring" and "per-subscriber queue".
//!
//! ## Catch-up is pulled, never pushed
//!
//! A subscription registers at a [`head`](Subscription::head) — the highest
//! index the fanout has processed — and from that instant receives only live
//! items strictly above it. Everything a resuming client is owed *below* head
//! is fetched by that client's own connection task, one batch-aligned
//! [`catch_up`](FanoutHandle::catch_up) page at a time (ADR 0043). Pushing the
//! whole replay into the subscriber's queue at subscribe time — what this
//! module used to do — put an unbounded amount of work on the fanout task and
//! overflowed the queue into a gap for exactly the clients that were trying
//! hardest to avoid one.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

use coppice_api::events::JobSelector;
use coppice_consensus::{EventBatch, EventTapReceiver, TapItem};
use coppice_core::id::{JobId, NodeId};
use coppice_core::time::Timestamp;
use coppice_state::{Event, ScopeView};

use crate::limits::{
    EVENT_CATCH_UP_SCAN_BUDGET, EVENT_PROGRESS_INTERVAL, FANOUT_GAP_RETRY_INTERVAL,
    FANOUT_RING_MAX_AGE, FANOUT_RING_MAX_BYTES, FANOUT_RING_MAX_EVENTS, MAX_EVENT_SUBSCRIPTIONS,
    SUBSCRIBER_QUEUE_CAPACITY,
};

/// Cross-proposer clock skew, observed at the fanout: |batch `at` − local
/// receipt time| (ADR 0032). Chronic skew is an operational signal — a
/// misconfigured coordinator clock — not something any consumer corrects for.
const PROPOSER_SKEW_SECONDS: &str = "coordinator_event_proposer_skew_seconds";
/// Subscriptions the fanout is currently serving, against
/// [`MAX_EVENT_SUBSCRIPTIONS`].
const SUBSCRIPTIONS: &str = "coordinator_event_subscriptions";
/// Gap markers raised, labelled by what made delivery discontinuous:
/// `overflow` (a subscriber's own queue), `tap` (apply outran the fanout),
/// `catch_up` (a resume cursor below the ring's floor), `reparent` (a quota
/// entity moved, changing which jobs a subtree selector admits).
const GAPS_TOTAL: &str = "coordinator_event_gaps_total";
/// Catch-up pages served from the ring (ADR 0043's pull-based resume).
const CATCH_UP_PAGES_TOTAL: &str = "coordinator_event_catch_up_pages_total";
/// Filtered batches handed to a subscriber's queue.
const BATCHES_DELIVERED_TOTAL: &str = "coordinator_event_batches_delivered_total";
/// Worst subscriber lag in applied indexes (head − last delivered), sampled
/// on the fanout's own tick. A subscriber that never matches anything sits at
/// its lag honestly; the progress bookmark is what tells *it* it is current.
const SUBSCRIBER_LAG: &str = "coordinator_event_subscriber_lag";
/// Approximate bytes the reconnection ring is holding, against
/// [`FANOUT_RING_MAX_BYTES`].
const RING_BYTES: &str = "coordinator_event_ring_bytes";

pub(crate) fn describe_metrics() {
    metrics::describe_histogram!(
        PROPOSER_SKEW_SECONDS,
        metrics::Unit::Seconds,
        "Absolute difference between a batch's proposer stamp and this replica's clock at receipt."
    );
    metrics::describe_gauge!(
        SUBSCRIPTIONS,
        "Event subscriptions this replica is currently serving."
    );
    metrics::describe_counter!(
        GAPS_TOTAL,
        "Gap markers raised to subscribers, by cause (overflow, tap, catch_up, reparent)."
    );
    metrics::describe_counter!(
        CATCH_UP_PAGES_TOTAL,
        "Batch-aligned catch-up pages served from the reconnection ring."
    );
    metrics::describe_counter!(
        BATCHES_DELIVERED_TOTAL,
        "Filtered event batches enqueued to a subscriber."
    );
    metrics::describe_gauge!(
        SUBSCRIBER_LAG,
        "Highest subscriber lag in applied indexes (fanout head minus last delivered index)."
    );
    metrics::describe_gauge!(
        RING_BYTES,
        metrics::Unit::Bytes,
        "Approximate size of the reconnection ring's retained batches."
    );
}

pub(crate) fn gather_metrics() {
    // Everything here is pushed or sampled from inside the fanout task, which
    // is the only place that can see its own subscriber table; a scrape-time
    // hook has nothing to add.
}

/// What a read or a subscriber wants to see.
///
/// `Job`/`Node` scope by the ids carried on an event (ADR 0008); `All` is the
/// unscoped stream the internal tasks subscribe with. `Jobs` is the ADR 0043
/// client-facing selector, evaluated against the scope keys apply stamped
/// onto the batch.
#[derive(Debug, Clone)]
pub enum EventFilter {
    All,
    Job(JobId),
    /// Node scoping is matched by `event_matches` but awaits its own read and
    /// subscription endpoints; nothing outside tests constructs it yet.
    #[allow(dead_code)]
    Node(NodeId),
    /// A compiled, validated `jobs=` selector (ADR 0043), shared rather than
    /// cloned: one `Arc` per subscription, and the selector is evaluated once
    /// per (job, batch) rather than once per event.
    Jobs(Arc<JobSelector>),
}

/// A filtered view of one batch, delivered to a subscriber.
///
/// Ordinals are assigned once, before any filtering: each is the event's
/// position within its *full* batch as derived by apply, and it is part of
/// the event's identity from that moment on (ADR 0032). A scoped subscription
/// may legitimately see ordinal gaps within an index; renumbering after the
/// filter would give the same event a different identity per subscription
/// scope.
#[derive(Debug, Clone)]
pub struct FilteredBatch {
    /// The producing command's log index (ADR 0008's global cursor).
    pub applied_index: u64,
    /// The batch's advisory proposer stamp (ADR 0032); never an ordering key.
    pub at: Timestamp,
    /// Events admitted by the subscriber's filter, in batch order.
    pub events: Vec<OrdinalEvent>,
}

/// One event paired with the ordinal it was assigned within its full batch,
/// before any subscription filter ran (ADR 0032). The ordinal is part of the
/// event's identity from that moment on: a scoped subscription may
/// legitimately see ordinal gaps within an index, but it never sees a
/// different ordinal for the same event than an unscoped one would.
#[derive(Debug, Clone)]
pub struct OrdinalEvent {
    /// Position within the *full* batch as derived by apply.
    pub ordinal: u32,
    /// The event admitted at that ordinal.
    pub event: Event,
}

/// One event with its full identity and stamp, as served from the ring.
#[derive(Debug, Clone)]
pub struct StampedEvent {
    pub index: u64,
    pub ordinal: u32,
    pub at: Timestamp,
    pub event: Event,
}

/// An ascending, filtered slice of the ring for a point-in-time read — the
/// tier-1 backstop behind `GetJobTimeline` (ADR 0032), served identically by
/// every replica.
///
/// `floor_index` is the ring's exclusive coverage floor (see [`Ring::floor`]):
/// the timeline is complete for every applied index *strictly above* it and
/// claims nothing at or below it, so a job whose window predates the floor is
/// honestly partial. `next` is the `(index, ordinal)` content coordinate to
/// resume strictly after (the `TimelineCursor` convention, ADR 0034): `Some`
/// means the scan stopped early (its `limit` or
/// budget was reached) and more may exist above it; `None` means the scan
/// reached the newest entry the ring holds — the caller has everything this
/// replica currently retains.
#[derive(Debug, Clone)]
pub struct EventWindow {
    pub floor_index: u64,
    pub events: Vec<StampedEvent>,
    pub next: Option<(u64, u32)>,
}

/// One **batch-aligned** page of a resuming subscription's catch-up
/// (ADR 0043).
///
/// Whole batches only: a command's events are one indivisible unit on this
/// stream (ADR 0008), so a page that stopped mid-batch would hand the client
/// a cursor it could not resume from without either duplicating or dropping
/// the rest of that command's events. The budget is therefore checked at
/// batch boundaries, never inside one.
#[derive(Debug, Clone)]
pub struct CatchUpPage {
    /// Matching batches in ascending index order, each complete.
    pub batches: Vec<FilteredBatch>,
    /// The highest index this page examined; resume strictly after it.
    /// `None` means the page reached the requested upper bound and there is
    /// nothing further to catch up on.
    pub next: Option<u64>,
    /// The ring's replay floor as of this page (see [`Ring::floor`]).
    pub floor: u64,
    /// The oldest index the ring can still serve — what a gap would report.
    pub earliest_available: u64,
    /// Set when the page stopped *at* a batch that reparented a quota entity
    /// while the subscriber's selector reads an entity subtree (ADR 0043):
    /// the index of that batch, whose events this page does not carry.
    ///
    /// The range this resume asked for crosses a discontinuity the stream
    /// cannot express as events, so the caller owes its client a `gap` and
    /// must not go on paging. `batches` still holds everything below it,
    /// which is honest to deliver first. `next` is the resume point below the
    /// discontinuity — meaningful only to a caller that is not resuming a
    /// subscription.
    pub reparent_at: Option<u64>,
}

/// One item delivered to a subscriber.
#[derive(Debug, Clone)]
pub enum SubscriptionItem {
    /// A batch's events admitted by the subscriber's filter, with their
    /// batch-assigned ordinals.
    Events(FilteredBatch),
    /// Everything matching at or below `index` has been delivered — the
    /// ADR 0043 progress bookmark, which doubles as a keepalive on an idle
    /// stream. Sent only to subscribers that asked for it
    /// ([`ProgressItems::Send`]); the internal tasks opt out.
    Progress { index: u64 },
    /// One or more batches were skipped for this subscriber.
    ///
    /// Either a tap-level gap or its own queue overflowed; resync from `earliest_available`.
    Gap { earliest_available: u64 },
}

/// Whether a subscription wants [`SubscriptionItem::Progress`] bookmarks.
///
/// They ride the subscriber's own queue, so ordering against batches is free
/// — but they are meaningless to a consumer that is not rendering a cursor to
/// somebody, and `dispatch`/`derived_stats` would only have to discard them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressItems {
    Send,
    Omit,
}

/// A live subscription: the receiving half of the subscriber's bounded queue,
/// plus the coordinates its catch-up needs.
pub struct Subscription {
    pub items: mpsc::Receiver<SubscriptionItem>,
    /// The highest applied index the fanout had processed when this
    /// subscription registered. Every live item is strictly above it, which
    /// makes it both the exclusive upper bound of a catch-up and the first
    /// honest progress bookmark.
    pub head: u64,
    /// The ring's replay floor (see [`Ring::floor`]). A cursor at or above it
    /// can be caught up from the ring; below it, only a gap is honest.
    pub floor: u64,
    /// The oldest index the ring can still serve, for the gap that a cursor
    /// below the floor produces.
    pub earliest_available: u64,
}

/// Why a subscribe request could not be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubscribeError {
    /// The fanout task has shut down; no more subscriptions can be served.
    #[error("event fanout is shutting down")]
    Closed,
    /// This replica is already serving [`MAX_EVENT_SUBSCRIPTIONS`].
    #[error("this replica is already serving its maximum of {MAX_EVENT_SUBSCRIPTIONS} event subscriptions")]
    AtCapacity,
}

/// The fanout task has shut down; no more ring reads can be served.
#[derive(Debug, Clone, Copy)]
pub struct FanoutClosed;

impl std::fmt::Display for FanoutClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "event fanout is shutting down")
    }
}

impl std::error::Error for FanoutClosed {}

/// One request on the fanout task's subscribe inbox.
struct SubscribeRequest {
    filter: EventFilter,
    progress: ProgressItems,
    reply: oneshot::Sender<Result<Subscription, SubscribeError>>,
}

/// One request on the fanout task's inbox.
enum Request {
    Subscribe(SubscribeRequest),
    /// An ascending, filtered window of the ring resuming after `after` — the
    /// tier-1 backstop behind `GetJobTimeline` (ADR 0032). A point-in-time
    /// copy with an HTTP handler blocked on its reply.
    Window {
        filter: EventFilter,
        after: Option<(u64, u32)>,
        limit: usize,
        reply: oneshot::Sender<EventWindow>,
    },
    /// One batch-aligned catch-up page for a resuming subscription
    /// (ADR 0043).
    CatchUp {
        filter: EventFilter,
        after: u64,
        up_to: u64,
        reply: oneshot::Sender<CatchUpPage>,
    },
}

/// Cloneable handle to the fanout task's inbox.
#[derive(Clone)]
pub struct FanoutHandle {
    tx: mpsc::Sender<Request>,
}

impl FanoutHandle {
    /// Subscribe to events matching `filter`, from now on.
    ///
    /// No cursor: the returned [`Subscription`] carries the `head` it opened
    /// at, and a caller resuming from a cursor pages the range below it
    /// through [`catch_up`](Self::catch_up) (ADR 0043). Every live item is
    /// strictly above `head`, so the two ranges meet exactly.
    pub async fn subscribe(
        &self,
        filter: EventFilter,
        progress: ProgressItems,
    ) -> Result<Subscription, SubscribeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Subscribe(SubscribeRequest {
                filter,
                progress,
                reply: reply_tx,
            }))
            .await
            .map_err(|_| SubscribeError::Closed)?;
        reply_rx.await.map_err(|_| SubscribeError::Closed)?
    }

    /// One batch-aligned page of `filter`-matching batches with index in
    /// `(after, up_to]`, bounded by [`EVENT_CATCH_UP_SCAN_BUDGET`] events
    /// examined (ADR 0043).
    ///
    /// The budget is applied on the fanout task, like the `GetJobTimeline`
    /// window's, so one resuming client cannot stall delivery to everyone
    /// else; the caller continues from the returned `next`.
    pub async fn catch_up(
        &self,
        filter: EventFilter,
        after: u64,
        up_to: u64,
    ) -> Result<CatchUpPage, FanoutClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::CatchUp {
                filter,
                after,
                up_to,
                reply: reply_tx,
            })
            .await
            .map_err(|_| FanoutClosed)?;
        reply_rx.await.map_err(|_| FanoutClosed)
    }

    /// An ascending, `filter`-scoped window of the ring resuming strictly after
    /// `after`, bounded by `limit` matches (see [`EventWindow`]). The scan
    /// budget is [`EVENT_WINDOW_SCAN_BUDGET`](crate::limits::EVENT_WINDOW_SCAN_BUDGET),
    /// applied on the fanout task so one read cannot stall delivery unbounded.
    pub async fn window(
        &self,
        filter: EventFilter,
        after: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<EventWindow, FanoutClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Window {
                filter,
                after,
                limit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| FanoutClosed)?;
        reply_rx.await.map_err(|_| FanoutClosed)
    }
}

/// The bounded reconnection ring plus its running event count and approximate
/// byte size.
///
/// Eviction never has to rescan the whole ring to decide whether it's over budget.
struct Ring {
    entries: VecDeque<RingEntry>,
    event_count: usize,
    /// Running sum of the entries' [`approx_bytes`], against
    /// [`FANOUT_RING_MAX_BYTES`].
    bytes: usize,
    /// The smallest cursor a replay can resume from without silently crossing a
    /// discontinuity. Raised by eviction, tap gaps, and snapshot installs, and
    /// initialized to the index this replica recovered at — so a reconnect with
    /// a pre-restart cursor gaps instead of replaying across the boundary
    /// (KOI-3). A cursor below the floor cannot be served from the ring.
    floor: u64,
}

struct RingEntry {
    seen_at: Instant,
    /// Shared with whatever is still delivering it: the fanout hands every
    /// subscriber a borrow of the same allocation rather than a per-subscriber
    /// deep copy of the batch.
    batch: Arc<EventBatch>,
    bytes: usize,
}

/// A rough in-memory size for one batch, used only for the ring's byte
/// budget.
///
/// Approximate on purpose: it counts the inline enum payloads plus the heap
/// the scope keys own (ADR 0043 made batches heavier — an entity chain, a
/// submitter and a metadata map per job, and a second map on every eviction
/// and metadata update). A per-entry `BTreeMap` node's true overhead is not
/// knowable from here, so the map term uses a flat allowance per pair. The
/// budget it feeds is a safety bound on resident memory, not an accounting
/// figure.
fn approx_bytes(batch: &EventBatch) -> usize {
    /// Flat allowance for one `BTreeMap` entry's node share, beyond its
    /// key and value bytes.
    const MAP_ENTRY_OVERHEAD: usize = 64;

    fn scope_bytes(scope: &coppice_state::JobScope) -> usize {
        scope.entity_chain.len() * std::mem::size_of::<coppice_core::id::QuotaEntityId>()
            + scope.submitted_by.as_ref().map_or(0, String::len)
            + metadata_bytes(&scope.metadata)
    }
    fn metadata_bytes(metadata: &coppice_core::metadata::JobMetadata) -> usize {
        metadata
            .iter()
            .map(|(k, v)| k.len() + v.len() + MAP_ENTRY_OVERHEAD)
            .sum()
    }

    let events: usize = batch.events.len() * std::mem::size_of::<Event>()
        + batch
            .events
            .iter()
            .map(|e| match e {
                Event::JobEvicted { scope, .. } => scope_bytes(scope),
                Event::JobMetadataUpdated { previous, .. } => metadata_bytes(previous),
                _ => 0,
            })
            .sum::<usize>();
    let scopes: usize = batch
        .scopes
        .iter()
        .map(|(_, scope)| {
            std::mem::size_of::<(JobId, coppice_state::JobScope)>() + scope_bytes(scope)
        })
        .sum();
    std::mem::size_of::<EventBatch>() + events + scopes
}

impl Ring {
    fn new(floor: u64) -> Self {
        Ring {
            entries: VecDeque::new(),
            event_count: 0,
            bytes: 0,
            floor,
        }
    }

    /// `impl Into<Arc<_>>` so the hot path can hand over the allocation it
    /// already shares with the subscribers, and a caller holding a bare batch
    /// (tests) need not wrap it by hand.
    fn push(&mut self, batch: impl Into<Arc<EventBatch>>) {
        let batch = batch.into();
        let bytes = approx_bytes(&batch);
        self.event_count += batch.events.len();
        self.bytes += bytes;
        self.entries.push_back(RingEntry {
            seen_at: Instant::now(),
            batch,
            bytes,
        });
        self.evict();
    }

    /// Drop the oldest entry, accounting for it and raising the replay floor
    /// past the index it carried. Returns false when the ring is empty.
    fn evict_oldest(&mut self) -> bool {
        let Some(evicted) = self.entries.pop_front() else {
            return false;
        };
        self.event_count -= evicted.batch.events.len();
        self.bytes -= evicted.bytes;
        // The evicted index is no longer replayable.
        self.raise_floor(evicted.batch.applied_index);
        true
    }

    fn evict(&mut self) {
        // Three independent bounds, each evict-oldest: a reconnection buffer,
        // not history. Bytes is the one that matters once batches carry scope
        // keys, since a few hundred large metadata maps can outweigh a
        // million tiny events.
        while (self.event_count > FANOUT_RING_MAX_EVENTS || self.bytes > FANOUT_RING_MAX_BYTES)
            && self.evict_oldest()
        {}
        if let Some(cutoff) = Instant::now().checked_sub(FANOUT_RING_MAX_AGE) {
            while matches!(self.entries.front(), Some(e) if e.seen_at < cutoff) {
                if !self.evict_oldest() {
                    break;
                }
            }
        }
    }

    /// Raise the replay floor to at least `index`; never lowers it.
    fn raise_floor(&mut self, index: u64) {
        self.floor = self.floor.max(index);
    }

    /// The smallest cursor a replay can resume from (see [`Ring::floor`]).
    fn floor(&self) -> u64 {
        self.floor
    }

    /// The oldest applied index still retained, reported to clients as the
    /// resync point; falls back to the floor when the ring is empty. Never
    /// below the floor: a discontinuity can raise the floor past entries
    /// still retained, and those are no longer a complete resume point.
    fn earliest_available(&self) -> u64 {
        self.entries
            .front()
            .map(|e| e.batch.applied_index)
            .unwrap_or(self.floor)
            .max(self.floor)
    }

    /// One batch-aligned catch-up page over `(after, up_to]` (see
    /// [`CatchUpPage`]).
    ///
    /// The budget counts events examined and is checked only *between*
    /// batches, so a page never splits a command's events. `after` below the
    /// floor is the caller's problem to detect (it owes its client a gap
    /// first); this simply starts at the oldest retained entry.
    ///
    /// A page also stops *at* a batch that reparents a quota entity when the
    /// filter reads an entity subtree, reporting it as
    /// [`reparent_at`](CatchUpPage::reparent_at): that is a discontinuity for
    /// this subscriber and the caller owes it a gap (ADR 0043).
    fn catch_up(&self, filter: &EventFilter, after: u64, up_to: u64, budget: usize) -> CatchUpPage {
        let start = self
            .entries
            .partition_point(|e| e.batch.applied_index <= after);
        let mut batches = Vec::new();
        let mut examined = 0usize;
        let mut last: Option<u64> = None;
        let mut next: Option<u64> = None;
        let mut reparent_at: Option<u64> = None;

        for entry in self.entries.iter().skip(start) {
            let batch = entry.batch.as_ref();
            if batch.applied_index > up_to {
                break;
            }
            if examined >= budget {
                // Stopped short of `up_to`: the caller resumes strictly after
                // the last whole batch this page examined.
                next = last;
                break;
            }
            if reparent_gap(filter, batch) {
                // A resume whose range crosses a reparent gets the same gap a
                // live subscriber would have got at that instant (ADR 0043):
                // the filter's verdict on jobs below the moved entity differs
                // either side of this batch, so no continuation of this page
                // is honest.
                reparent_at = Some(batch.applied_index);
                next = last;
                break;
            }
            examined += batch.events.len();
            last = Some(batch.applied_index);
            if let Some(filtered) = filter_events(filter, batch) {
                batches.push(filtered);
            }
        }

        CatchUpPage {
            batches,
            next,
            floor: self.floor(),
            earliest_available: self.earliest_available(),
            reparent_at,
        }
    }

    /// An ascending, `filter`-scoped window of the ring, resuming strictly
    /// after the `after` content coordinate and bounded by `limit` matches and
    /// `budget` events examined.
    ///
    /// Batches are walked oldest→newest (the ring's natural order and ADR
    /// 0032's `(index, ordinal)` order). `after` is located with a
    /// `partition_point` on `applied_index`, so batches wholly below the cursor
    /// are skipped without a scan; within the resume batch, events at or before
    /// the cursor are pure resume mechanics — not examined, not counted, never
    /// re-served. An `after` below the ring's floor simply starts at the oldest
    /// retained entry, and the returned `floor_index` tells the caller about
    /// the gap below it.
    ///
    /// Every event strictly after the cursor counts against `budget`; matching
    /// ones (per [`event_matches`]) are collected until `limit`. When the scan
    /// stops early — `limit` filled or `budget` spent — `next` is the last
    /// event examined, so a follow-up `window(after = next, …)` resumes with no
    /// duplicate and no skip. `next` is `None` iff the walk reached the newest
    /// entry the ring holds (the caller has everything this replica retains).
    fn window(
        &self,
        filter: &EventFilter,
        after: Option<(u64, u32)>,
        limit: usize,
        budget: usize,
    ) -> EventWindow {
        let mut events = Vec::new();
        let mut examined = 0usize;
        let mut last: Option<(u64, u32)> = None;
        let mut next: Option<(u64, u32)> = None;

        // Entries are sorted ascending by `applied_index`, so every batch below
        // the cursor's index is fully consumed. A cursor below the floor lands
        // at 0 (start from the oldest retained entry); one above every entry
        // lands at the end (nothing to serve — the caller already has it).
        let start = match after {
            Some((index, _)) => self
                .entries
                .partition_point(|e| e.batch.applied_index < index),
            None => 0,
        };

        'outer: for entry in self.entries.iter().skip(start) {
            let batch = entry.batch.as_ref();
            let mut verdicts = JobVerdicts::default();
            for (ordinal, event) in batch.events.iter().enumerate() {
                let id = (batch.applied_index, ordinal as u32);
                // Resume strictly after the cursor: at-or-before it is resume
                // mechanics, neither examined nor counted.
                if matches!(after, Some(after) if id <= after) {
                    continue;
                }
                if events.len() == limit || examined == budget {
                    // Stopped early: content above `next` may exist.
                    next = last;
                    break 'outer;
                }
                last = Some(id);
                examined += 1;
                if event_matches(filter, batch, event, &mut verdicts) {
                    events.push(StampedEvent {
                        index: batch.applied_index,
                        ordinal: id.1,
                        at: batch.at,
                        event: event.clone(),
                    });
                }
            }
        }

        EventWindow {
            floor_index: self.floor(),
            events,
            next,
        }
    }
}

/// One registered subscriber's delivery state.
struct SubscriberState {
    filter: EventFilter,
    tx: mpsc::Sender<SubscriptionItem>,
    progress: ProgressItems,
    /// Highest applied index enqueued to this subscriber, for the lag gauge.
    /// Seeded with the head the subscription opened at: a subscriber whose
    /// filter matches nothing is caught up, not infinitely behind.
    last_delivered: u64,
    /// Set when a `try_send` found the subscriber's queue full.
    ///
    /// Delivery is paused until a `Gap` marker itself is accepted, at which
    /// point normal delivery resumes. See `docs/architecture/coordinator-runtime.md`
    /// ("per-subscriber queue").
    gapped: bool,
    /// A floor the pending gap must report at least: the index of a quota
    /// entity move (ADR 0043), so that a marker which had to wait for queue
    /// space still tells the client how fresh its resync read must be. Zero
    /// when the pending gap has no such floor.
    gap_floor: u64,
}

/// Spawn the fanout task.
///
/// `recovery_index` is the applied index this replica recovered at; it seeds
/// the ring's replay floor so a reconnect carrying a pre-restart cursor gaps
/// rather than replaying silently across the restart boundary (KOI-3).
///
/// Returns the handle other tasks subscribe through, plus its `JoinHandle`.
pub fn spawn(
    event_tap: EventTapReceiver,
    recovery_index: u64,
    shutdown: watch::Receiver<bool>,
) -> (FanoutHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Request>(crate::limits::SUBSCRIBE_REQUESTS_CAPACITY);
    let handle = FanoutHandle { tx };
    let join = tokio::spawn(run(event_tap, recovery_index, rx, shutdown));
    (handle, join)
}

async fn run(
    mut event_tap: EventTapReceiver,
    recovery_index: u64,
    mut subscribe_rx: mpsc::Receiver<Request>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ring = Ring::new(recovery_index);
    let mut subscribers: BTreeMap<u64, SubscriberState> = BTreeMap::new();
    let mut next_id: u64 = 0;
    // The highest applied index this fanout has processed. Advanced by
    // batches *and* by tap gaps, because a gap means indexes went by — a head
    // that ignored them would let a progress bookmark claim coverage the
    // stream never had.
    let mut head: u64 = recovery_index;

    // Retries a pending gap to any subscriber whose queue overflowed and then
    // idled, so it still learns to resync even with no further events; also
    // where dead subscribers are swept and the gauges sampled. Skip missed
    // ticks: a stalled loop needs one flush, not a burst.
    let mut gap_retry = tokio::time::interval(FANOUT_GAP_RETRY_INTERVAL);
    gap_retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The ADR 0043 progress bookmark, which is also this stream's keepalive.
    let mut progress = tokio::time::interval(EVENT_PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
    progress.reset(); // the first tick fires after one interval, not at once

    loop {
        // The biased select below polls the tap ahead of the request inbox,
        // so a saturated tap would starve requests indefinitely — and a
        // `Window` or `CatchUp` request has an HTTP handler blocked on its
        // reply. Requests are cheap and their inbox is small, so sweep what
        // is pending between select points. The sweep is bounded to one
        // inbox's capacity: concurrent senders can refill the channel while
        // it drains, and an unbounded `while try_recv` would let sustained
        // request traffic pin the loop here and starve the tap — the exact
        // starvation this sweep exists to prevent, reversed.
        for _ in 0..crate::limits::SUBSCRIBE_REQUESTS_CAPACITY {
            match subscribe_rx.try_recv() {
                Ok(req) => handle_request(&mut subscribers, &mut next_id, &ring, head, req),
                Err(_) => break,
            }
        }
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            item = event_tap.recv() => {
                match item {
                    Some(TapItem::Batch(batch)) => {
                        record_proposer_skew(&batch);
                        head = head.max(batch.applied_index);
                        // One allocation, shared by the ring and every
                        // subscriber's filter pass: nobody deep-copies the
                        // batch to read it.
                        let batch = Arc::new(batch);
                        ring.push(Arc::clone(&batch));
                        let earliest = ring.earliest_available();
                        subscribers.retain(|_, sub| deliver(sub, &batch, earliest));
                    }
                    Some(TapItem::Gap { earliest_replayable }) => {
                        // Record the discontinuity in the ring so later
                        // reconnects cannot replay silently across it, then
                        // notify live subscribers to resync.
                        head = head.max(earliest_replayable);
                        ring.raise_floor(earliest_replayable);
                        let earliest = ring.earliest_available();
                        subscribers.retain(|_, sub| {
                            let gap = SubscriptionItem::Gap { earliest_available: earliest };
                            match sub.tx.try_send(gap) {
                                Ok(()) => { sub.gapped = false; true }
                                Err(mpsc::error::TrySendError::Full(_)) => { sub.gapped = true; true }
                                // The receiver is gone: the connection task
                                // dropped it. Retrying it every tick forever
                                // is what this replaces.
                                Err(mpsc::error::TrySendError::Closed(_)) => false,
                            }
                        });
                        metrics::counter!(GAPS_TOTAL, "cause" => "tap").increment(1);
                    }
                    // The apply task (and its EventTap) is gone; nothing
                    // further will ever arrive.
                    None => break,
                }
            }
            req = subscribe_rx.recv() => {
                // `None` means no more producers will register new
                // subscriptions; existing ones keep being served.
                if let Some(req) = req {
                    handle_request(&mut subscribers, &mut next_id, &ring, head, req);
                }
            }
            _ = gap_retry.tick() => {
                flush_gaps(&mut subscribers, &ring);
                sample_gauges(&subscribers, &ring, head);
            }
            _ = progress.tick() => {
                send_progress(&mut subscribers, head);
            }
        }
    }
    tracing::debug!("event fanout shutting down");
    // Dropping `subscribers` here closes every subscription's channel, which
    // is the clean end of every open SSE stream (ADR 0043).
}

/// Serve one inbox request.
fn handle_request(
    subscribers: &mut BTreeMap<u64, SubscriberState>,
    next_id: &mut u64,
    ring: &Ring,
    head: u64,
    req: Request,
) {
    match req {
        Request::Subscribe(req) => {
            *next_id += 1;
            handle_subscribe(subscribers, *next_id, ring, head, req);
        }
        Request::Window {
            filter,
            after,
            limit,
            reply,
        } => {
            // The caller may be gone already; nothing to do.
            let _ = reply.send(ring.window(
                &filter,
                after,
                limit,
                crate::limits::EVENT_WINDOW_SCAN_BUDGET,
            ));
        }
        Request::CatchUp {
            filter,
            after,
            up_to,
            reply,
        } => {
            metrics::counter!(CATCH_UP_PAGES_TOTAL).increment(1);
            let _ = reply.send(ring.catch_up(&filter, after, up_to, EVENT_CATCH_UP_SCAN_BUDGET));
        }
    }
}

/// Re-attempt delivery of a pending gap to every gapped subscriber, and drop
/// any whose receiver has gone.
///
/// A subscriber goes gapped when its queue is full at gap time (a fresh
/// overflow, or an unflushable tap gap). The marker is normally retried on the
/// next batch, but a subscriber that then sees no events would stay wedged;
/// this timer-driven sweep clears it once the queue drains (KOI-3).
fn flush_gaps(subscribers: &mut BTreeMap<u64, SubscriberState>, ring: &Ring) {
    let earliest = ring.earliest_available();
    subscribers.retain(|_, sub| {
        // A disconnected client's subscription is torn down here rather than
        // retried forever; its connection task is already gone.
        if sub.tx.is_closed() {
            return false;
        }
        if sub.gapped {
            let gap = SubscriptionItem::Gap {
                earliest_available: earliest.max(sub.gap_floor),
            };
            if sub.tx.try_send(gap).is_ok() {
                sub.gapped = false;
                sub.gap_floor = 0;
            }
        }
        true
    });
}

/// Send the ADR 0043 progress bookmark to every subscriber that asked for one
/// and is not gapped.
///
/// Best effort by design: a full queue means real items are already waiting,
/// so the bookmark would be both redundant and (if it were allowed to
/// overflow) a reason to gap a subscriber that had done nothing wrong. It is
/// skipped instead. A gapped subscriber gets none either — it has been told
/// its coverage is broken, and "everything at or below N has been sent" would
/// contradict that.
fn send_progress(subscribers: &mut BTreeMap<u64, SubscriberState>, head: u64) {
    subscribers.retain(|_, sub| {
        if sub.progress == ProgressItems::Omit || sub.gapped {
            return true;
        }
        match sub.tx.try_send(SubscriptionItem::Progress { index: head }) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    });
}

/// Sample the point-in-time gauges from inside the task that owns their
/// inputs.
fn sample_gauges(subscribers: &BTreeMap<u64, SubscriberState>, ring: &Ring, head: u64) {
    metrics::gauge!(SUBSCRIPTIONS).set(subscribers.len() as f64);
    metrics::gauge!(RING_BYTES).set(ring.bytes as f64);
    let lag = subscribers
        .values()
        .map(|sub| head.saturating_sub(sub.last_delivered))
        .max()
        .unwrap_or(0);
    metrics::gauge!(SUBSCRIBER_LAG).set(lag as f64);
}

/// Observe |`at` − local now| for the arriving batch (see
/// [`PROPOSER_SKEW_SECONDS`]).
fn record_proposer_skew(batch: &EventBatch) {
    let skew = (Timestamp::now() - batch.at).abs();
    metrics::histogram!(PROPOSER_SKEW_SECONDS).record(skew.as_secs_f64());
}

/// Per-batch memo of the per-job verdict for a [`EventFilter::Jobs`]
/// selector.
///
/// A selector is evaluated once per (job, batch) and reused for every event
/// that job produced in that batch — the whole point of resolving scope keys
/// per job rather than per event (ADR 0043). A `Vec`, because a batch names
/// one or two jobs in the overwhelming majority of cases.
#[derive(Default)]
struct JobVerdicts(Vec<(JobId, bool)>);

impl JobVerdicts {
    fn admits(&mut self, selector: &JobSelector, batch: &EventBatch, job: JobId) -> bool {
        if let Some((_, verdict)) = self.0.iter().find(|(id, _)| *id == job) {
            return *verdict;
        }
        let verdict = job_admitted(selector, batch, job);
        self.0.push((job, verdict));
        verdict
    }
}

/// Whether `job` matched `selector` immediately **before or after** the
/// command that produced `batch` (ADR 0043).
///
/// Both halves come off the batch itself, never out of a later view: the
/// after-keys the apply loop stamped on, and — for the two commands that can
/// change or remove them — the before-keys the events carry. That is what
/// makes an update which *removes* the very key a subscriber filtered on
/// still reach that subscriber, instead of vanishing along with the match.
fn job_admitted(selector: &JobSelector, batch: &EventBatch, job: JobId) -> bool {
    let after = batch.scope(job);
    if after.is_some_and(|scope| selector.matches(job, scope.view())) {
        return true;
    }
    batch.events.iter().any(|event| match event {
        // An evicted job is gone from state, so its event carries the whole
        // scope.
        Event::JobEvicted { job: j, scope } if *j == job => selector.matches(job, scope.view()),
        // A metadata update changed only the map; the rest of the before-view
        // is the after-scope's, borrowed rather than rebuilt.
        Event::JobMetadataUpdated { job: j, previous } if *j == job => after.is_some_and(|scope| {
            selector.matches(
                job,
                ScopeView {
                    entity_chain: &scope.entity_chain,
                    submitted_by: scope.submitted_by.as_deref(),
                    metadata: previous,
                },
            )
        }),
        _ => false,
    })
}

/// Whether this batch is a discontinuity for `filter` rather than something it
/// can express as events (ADR 0043).
///
/// `ConfigureQuotaEntity` may move an existing entity under a different
/// parent. That changes the `entity_chain` stamped on every job below it from
/// the next command on — moving those jobs into or out of a subtree selector's
/// set — while emitting one entity-scoped event that names no job and so is
/// never delivered on a `jobs=` stream. A consumer tracking such a set would
/// otherwise keep jobs that no longer match and never learn of ones that now
/// do, with nothing on the stream to tell it. A gap does tell it, and the
/// resync read is the same filter against current state.
///
/// Only subtree selectors are affected: an exact-entity leaf reads the chain's
/// head, which a reparent above the job never moves, and `All`/`Job`/`Node`
/// filters do not read the chain at all. The filter test comes first because
/// it is a stored bool, while this scan is per batch.
fn reparent_gap(filter: &EventFilter, batch: &EventBatch) -> bool {
    let EventFilter::Jobs(selector) = filter else {
        return false;
    };
    selector.reads_entity_subtree()
        && batch.events.iter().any(|event| {
            matches!(
                event,
                Event::QuotaEntityConfigured {
                    reparented: true,
                    ..
                }
            )
        })
}

/// Filter one batch's events down to what `filter` admits, preserving each
/// event's batch-assigned ordinal (ADR 0032: ordinals are assigned before
/// any filtering, so an event's `(index, ordinal)` identity is the same
/// under every subscription scope).
///
/// Returns `None` if nothing in it survives (skip delivering an empty batch).
fn filter_events(filter: &EventFilter, batch: &EventBatch) -> Option<FilteredBatch> {
    let mut verdicts = JobVerdicts::default();
    let events: Vec<OrdinalEvent> = batch
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| event_matches(filter, batch, e, &mut verdicts))
        .map(|(ordinal, e)| OrdinalEvent {
            ordinal: ordinal as u32,
            event: e.clone(),
        })
        .collect();
    if events.is_empty() {
        None
    } else {
        Some(FilteredBatch {
            applied_index: batch.applied_index,
            at: batch.at,
            events,
        })
    }
}

/// Whether `event` is in `filter`'s scope, decided entirely by the scope
/// keys the event and its batch carry (ADR 0008, ADR 0043).
///
/// Attempt- and allocation-scoped events are stamped with their owning job
/// and node during apply, so a `Job`/`Node` subscription sees the complete
/// documented set — no cross-index lookups against state that may have moved
/// on by delivery time.
fn event_matches(
    filter: &EventFilter,
    batch: &EventBatch,
    event: &Event,
    verdicts: &mut JobVerdicts,
) -> bool {
    match filter {
        EventFilter::All => true,
        EventFilter::Job(job) => event.job() == Some(*job),
        EventFilter::Node(node) => match event {
            Event::StopRequested { node: n, .. }
            | Event::NodeEpochBumped { node: n, .. }
            | Event::AttemptStateChanged { node: n, .. }
            | Event::AllocationFunded { node: n, .. } => n == node,
            _ => false,
        },
        // Non-job events are never delivered on a `jobs=` stream: a node
        // epoch, a policy edit or a quota reconfiguration has no job to judge
        // against the selector (ADR 0043).
        EventFilter::Jobs(selector) => match event.job() {
            Some(job) => verdicts.admits(selector, batch, job),
            None => false,
        },
    }
}

/// Deliver one freshly-tapped batch to a subscriber, returning whether to
/// keep the subscription.
///
/// Applies the gap recovery and full-queue policy of the "per-subscriber
/// queue" channel row. A closed receiver means the client is gone: the
/// subscription is dropped rather than retried on every batch forever.
fn deliver(sub: &mut SubscriberState, batch: &EventBatch, ring_earliest: u64) -> bool {
    // A quota-entity reparent is a discontinuity for a subtree subscriber
    // (see [`reparent_gap`]): it gets a gap in place of this batch, never the
    // batch filtered under a membership that changed underneath it.
    let reparent = reparent_gap(&sub.filter, batch);

    if sub.gapped {
        if reparent {
            // The pending marker absorbs this move; it must report it too.
            sub.gap_floor = sub.gap_floor.max(batch.applied_index);
        }
        let gap = SubscriptionItem::Gap {
            earliest_available: ring_earliest.max(sub.gap_floor),
        };
        match sub.tx.try_send(gap) {
            Ok(()) => {
                sub.gapped = false;
                sub.gap_floor = 0;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            // Still backed up; try again on the next batch. A reparent in this
            // batch needs no separate marker: the pending gap covers it.
            Err(mpsc::error::TrySendError::Full(_)) => return true,
        }
        if reparent {
            // The gap just flushed already told this subscriber to resync, and
            // carried this move's index as its floor.
            sub.last_delivered = sub.last_delivered.max(batch.applied_index);
            return true;
        }
    }
    if reparent {
        metrics::counter!(GAPS_TOTAL, "cause" => "reparent").increment(1);
        // The resync has to reflect the move, so the move's own index is the
        // earliest position a complete resume can start from — not the
        // ring's, which may sit far below it.
        let gap = SubscriptionItem::Gap {
            earliest_available: batch.applied_index.max(ring_earliest),
        };
        match sub.tx.try_send(gap) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            // A full queue must not lose the marker: the same pending-gap
            // mechanics an overflow uses retry it on the next batch, and the
            // sweep clears it on an idle stream.
            Err(mpsc::error::TrySendError::Full(_)) => {
                sub.gapped = true;
                sub.gap_floor = sub.gap_floor.max(batch.applied_index);
            }
        }
        sub.last_delivered = sub.last_delivered.max(batch.applied_index);
        return true;
    }
    let Some(filtered) = filter_events(&sub.filter, batch) else {
        // Nothing matched, but the subscriber is nonetheless current up to
        // this index — which is what its next progress bookmark will say.
        sub.last_delivered = sub.last_delivered.max(batch.applied_index);
        return true;
    };
    let index = filtered.applied_index;
    match sub.tx.try_send(SubscriptionItem::Events(filtered)) {
        Ok(()) => {
            sub.last_delivered = sub.last_delivered.max(index);
            metrics::counter!(BATCHES_DELIVERED_TOTAL).increment(1);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => return false,
        Err(mpsc::error::TrySendError::Full(_)) => {
            sub.gapped = true;
            metrics::counter!(GAPS_TOTAL, "cause" => "overflow").increment(1);
        }
    }
    true
}

/// Serve a subscribe request.
///
/// Registers the subscriber and reports the coordinates its connection task
/// needs — nothing is replayed here. A resuming client pages the range below
/// `head` through [`FanoutHandle::catch_up`] (ADR 0043).
fn handle_subscribe(
    subscribers: &mut BTreeMap<u64, SubscriberState>,
    id: u64,
    ring: &Ring,
    head: u64,
    req: SubscribeRequest,
) {
    // The cap bounds the per-subscriber queues (and the catch-up traffic they
    // imply) a single replica can be made to hold. Refused rather than
    // queued: a client told "not now" retries against another replica, which
    // is a better answer than a stream that exists but cannot keep up.
    if subscribers.len() >= MAX_EVENT_SUBSCRIPTIONS {
        let _ = req.reply.send(Err(SubscribeError::AtCapacity));
        return;
    }
    let (tx, rx) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
    subscribers.insert(
        id,
        SubscriberState {
            filter: req.filter,
            tx,
            progress: req.progress,
            last_delivered: head,
            gapped: false,
            gap_floor: 0,
        },
    );
    metrics::gauge!(SUBSCRIPTIONS).set(subscribers.len() as f64);
    // The receiver may be gone already (an impatient caller); the next tick's
    // sweep collects the subscriber either way.
    let _ = req.reply.send(Ok(Subscription {
        items: rx,
        head,
        floor: ring.floor(),
        earliest_available: ring.earliest_available(),
    }));
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coppice_api::http::dto;
    use coppice_core::attempt::AttemptState;
    use coppice_core::id::{AllocationId, AttemptId, QuotaEntityId};
    use coppice_core::metadata::JobMetadata;
    use coppice_state::JobScope;

    use super::*;

    fn job_event(job: JobId) -> Event {
        Event::JobSubmitted { job }
    }

    /// The attempt/allocation-scoped variants, all owned by `job` on `node`.
    fn scoped_events(job: JobId, node: NodeId) -> Vec<Event> {
        vec![
            Event::AttemptStateChanged {
                attempt: AttemptId::new(),
                job,
                node,
                state: AttemptState::Ready,
            },
            Event::AllocationFunded {
                allocation: AllocationId::new(),
                job,
                node,
            },
            Event::StopRequested {
                node,
                allocation: AllocationId::new(),
                job,
            },
        ]
    }

    fn batch_of(applied_index: u64, events: Vec<Event>) -> EventBatch {
        EventBatch {
            applied_index,
            at: Timestamp::UNIX_EPOCH,
            events,
            scopes: Vec::new(),
        }
    }

    fn metadata(pairs: &[(&str, &str)]) -> JobMetadata {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn scope(
        chain: &[QuotaEntityId],
        submitted_by: Option<&str>,
        meta: &[(&str, &str)],
    ) -> JobScope {
        JobScope {
            entity_chain: chain.to_vec(),
            submitted_by: submitted_by.map(str::to_string),
            metadata: metadata(meta),
        }
    }

    /// A `jobs=` selector over one metadata leaf — the filter a client
    /// watching "my team's jobs" actually sends.
    fn metadata_selector(key: &str, equals: Option<&str>) -> EventFilter {
        selector(dto::JobFilter::Metadata(dto::MetadataFilter {
            key: key.to_string(),
            equals: equals.map(str::to_string),
        }))
    }

    fn selector(filter: dto::JobFilter) -> EventFilter {
        EventFilter::Jobs(Arc::new(
            JobSelector::compile(&filter).expect("an allowed leaf"),
        ))
    }

    #[test]
    fn all_filter_admits_everything() {
        let batch = batch_of(1, vec![job_event(JobId::new())]);
        assert!(filter_events(&EventFilter::All, &batch).is_some());
    }

    #[test]
    fn job_filter_only_admits_its_own_job() {
        let job = JobId::new();
        let other = JobId::new();
        let batch = batch_of(1, vec![job_event(job)]);
        assert!(filter_events(&EventFilter::Job(job), &batch).is_some());
        assert!(filter_events(&EventFilter::Job(other), &batch).is_none());
    }

    #[test]
    fn node_filter_only_admits_its_own_node() {
        let node = NodeId::new();
        let other = NodeId::new();
        let event = Event::NodeEpochBumped { node, epoch: 1 };
        let batch = batch_of(1, vec![event]);
        assert!(filter_events(&EventFilter::Node(node), &batch).is_some());
        assert!(filter_events(&EventFilter::Node(other), &batch).is_none());
    }

    #[test]
    fn job_filter_admits_attempt_and_allocation_events() {
        let job = JobId::new();
        let other = JobId::new();
        let batch = batch_of(1, scoped_events(job, NodeId::new()));
        let filtered = filter_events(&EventFilter::Job(job), &batch)
            .expect("attempt/allocation events carry their owning job");
        assert_eq!(filtered.events.len(), 3);
        assert!(filter_events(&EventFilter::Job(other), &batch).is_none());
    }

    /// ADR 0032 (T6): ordinals are batch positions assigned before the
    /// filter, so a scoped subscription sees the same `(index, ordinal)` for
    /// an event as an all-events one — with gaps where its filter skipped
    /// events, never a renumbering.
    #[test]
    fn filtering_preserves_batch_assigned_ordinals() {
        let job_a = JobId::new();
        let job_b = JobId::new();
        let batch = batch_of(9, vec![job_event(job_a), job_event(job_b)]);

        let all = filter_events(&EventFilter::All, &batch).expect("all admits both");
        assert_eq!(
            all.events.iter().map(|e| e.ordinal).collect::<Vec<_>>(),
            vec![0, 1]
        );

        // job_b's event keeps ordinal 1 even though it is the only survivor.
        let scoped = filter_events(&EventFilter::Job(job_b), &batch).expect("admits job_b");
        assert_eq!(
            scoped.events.iter().map(|e| e.ordinal).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(scoped.at, batch.at);
    }

    // ---- ADR 0043 `jobs=` selector matching -----------------------------

    /// The ordinary case: the selector is judged against the after-keys the
    /// apply loop stamped on, per job, and a job that does not match
    /// contributes nothing.
    #[test]
    fn jobs_selector_matches_on_the_after_keys() {
        let mine = JobId::new();
        let theirs = JobId::new();
        let batch = EventBatch {
            scopes: vec![
                (mine, scope(&[], None, &[("team", "platform")])),
                (theirs, scope(&[], None, &[("team", "storage")])),
            ],
            ..batch_of(4, vec![job_event(mine), job_event(theirs)])
        };

        let filtered = filter_events(&metadata_selector("team", Some("platform")), &batch)
            .expect("the platform job matched");
        assert_eq!(
            filtered
                .events
                .iter()
                .map(|e| e.ordinal)
                .collect::<Vec<_>>(),
            vec![0],
            "only the matching job's event, at its batch-assigned ordinal"
        );
    }

    /// A `jobs=` stream carries job-scoped events and nothing else: a policy
    /// edit or a node epoch bump has no job to judge against the selector.
    #[test]
    fn jobs_selector_never_admits_a_non_job_event() {
        let batch = batch_of(
            4,
            vec![
                Event::PolicyUpdated,
                Event::NodeEpochBumped {
                    node: NodeId::new(),
                    epoch: 1,
                },
                Event::QuotaEntityConfigured {
                    entity: QuotaEntityId::new(),
                    reparented: false,
                },
            ],
        );
        assert!(filter_events(&metadata_selector("team", None), &batch).is_none());
    }

    /// The before-OR-after rule, in the case that motivates it: a patch that
    /// **removes** the very key the subscriber filtered on. The after-keys no
    /// longer match, so only the event's own before-keys can deliver it — and
    /// they must, or the subscriber's last word on that job would be a state
    /// it has already left.
    #[test]
    fn jobs_selector_admits_a_metadata_update_that_removed_the_matching_key() {
        let job = JobId::new();
        let batch = EventBatch {
            // Post-apply the key is gone.
            scopes: vec![(job, scope(&[], None, &[]))],
            ..batch_of(
                7,
                vec![Event::JobMetadataUpdated {
                    job,
                    previous: metadata(&[("team", "platform")]),
                }],
            )
        };

        assert!(
            filter_events(&metadata_selector("team", Some("platform")), &batch).is_some(),
            "the removal itself must reach a subscriber that matched before it"
        );
        // A selector that matched neither before nor after still sees nothing.
        assert!(filter_events(&metadata_selector("team", Some("storage")), &batch).is_none());
    }

    /// The converse half of before-OR-after: an update that *adds* the key is
    /// delivered on the after-keys, so a subscriber learns about a job the
    /// moment it enters its scope.
    #[test]
    fn jobs_selector_admits_a_metadata_update_that_added_the_matching_key() {
        let job = JobId::new();
        let batch = EventBatch {
            scopes: vec![(job, scope(&[], None, &[("team", "platform")]))],
            ..batch_of(
                7,
                vec![Event::JobMetadataUpdated {
                    job,
                    previous: JobMetadata::new(),
                }],
            )
        };
        assert!(filter_events(&metadata_selector("team", Some("platform")), &batch).is_some());
    }

    /// An eviction has no after-keys at all — the job is gone from state — so
    /// the whole verdict rests on the scope the event carries.
    #[test]
    fn jobs_selector_admits_an_eviction_on_its_before_keys() {
        let job = JobId::new();
        let batch = batch_of(
            9,
            vec![Event::JobEvicted {
                job,
                scope: scope(&[], Some("alice"), &[("team", "platform")]),
            }],
        );
        assert!(batch.scope(job).is_none(), "the job is gone post-apply");

        assert!(filter_events(&metadata_selector("team", Some("platform")), &batch).is_some());
        assert!(filter_events(&metadata_selector("team", Some("storage")), &batch).is_none());
        assert!(filter_events(
            &selector(dto::JobFilter::SubmittedBy("alice".into())),
            &batch
        )
        .is_some());
    }

    /// The entity leaf resolves off the stamped chain, so `exact` is the
    /// job's own entity and `subtree` is any ancestor — with no tree walk at
    /// delivery time.
    #[test]
    fn jobs_selector_entity_scopes_read_the_stamped_chain() {
        let job = JobId::new();
        let root = QuotaEntityId::new();
        let team = QuotaEntityId::new();
        let batch = EventBatch {
            scopes: vec![(job, scope(&[team, root], None, &[]))],
            ..batch_of(3, vec![job_event(job)])
        };
        let entity = |id, s| {
            selector(dto::JobFilter::Entity(dto::EntityFilter {
                entity: coppice_core::id::QuotaEntityId::into(id),
                scope: s,
            }))
        };

        assert!(filter_events(&entity(team, dto::EntityScope::Exact), &batch).is_some());
        assert!(filter_events(&entity(root, dto::EntityScope::Exact), &batch).is_none());
        assert!(filter_events(&entity(root, dto::EntityScope::Subtree), &batch).is_some());
    }

    // ---- delivery ------------------------------------------------------

    fn subscriber(
        filter: EventFilter,
        progress: ProgressItems,
        capacity: usize,
    ) -> (SubscriberState, mpsc::Receiver<SubscriptionItem>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            SubscriberState {
                filter,
                tx,
                progress,
                last_delivered: 0,
                gapped: false,
                gap_floor: 0,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn subscriber_gaps_when_its_queue_overflows_then_recovers() {
        let (mut sub, mut rx) = subscriber(EventFilter::All, ProgressItems::Omit, 2);

        let b1 = batch_of(1, vec![job_event(JobId::new())]);
        let b2 = batch_of(2, vec![job_event(JobId::new())]);
        let b3 = batch_of(3, vec![job_event(JobId::new())]);

        assert!(deliver(&mut sub, &b1, 0)); // 1/2
        assert!(deliver(&mut sub, &b2, 0)); // 2/2, still not full
        assert!(deliver(&mut sub, &b3, 0)); // full -> gapped, b3 dropped
        assert!(sub.gapped);

        match rx.recv().await {
            Some(SubscriptionItem::Events(b)) => assert_eq!(b.applied_index, 1),
            other => panic!("expected the first batch, got {other:?}"),
        }
        match rx.recv().await {
            Some(SubscriptionItem::Events(b)) => assert_eq!(b.applied_index, 2),
            other => panic!("expected the second batch, got {other:?}"),
        }

        // Queue is empty again; the next delivery clears the gap and gets
        // its own batch through in the same call.
        let b4 = batch_of(4, vec![job_event(JobId::new())]);
        assert!(deliver(&mut sub, &b4, 2));
        assert!(!sub.gapped);

        match rx.recv().await {
            Some(SubscriptionItem::Gap { earliest_available }) => assert_eq!(earliest_available, 2),
            other => panic!("expected a gap, got {other:?}"),
        }
        match rx.recv().await {
            Some(SubscriptionItem::Events(b)) => assert_eq!(b.applied_index, 4),
            other => panic!("expected the fourth batch, got {other:?}"),
        }
    }

    // ---- quota-entity reparents (ADR 0043) ------------------------------

    fn reparent_batch(index: u64, reparented: bool) -> EventBatch {
        batch_of(
            index,
            vec![Event::QuotaEntityConfigured {
                entity: QuotaEntityId::new(),
                reparented,
            }],
        )
    }

    fn subtree_selector(entity: QuotaEntityId) -> EventFilter {
        selector(dto::JobFilter::Entity(dto::EntityFilter {
            entity: entity.into(),
            scope: dto::EntityScope::Subtree,
        }))
    }

    fn exact_selector(entity: QuotaEntityId) -> EventFilter {
        selector(dto::JobFilter::Entity(dto::EntityFilter {
            entity: entity.into(),
            scope: dto::EntityScope::Exact,
        }))
    }

    /// The finding this exists for: a reparent moves every job under the moved
    /// entity into or out of a subtree subscriber's set while naming none of
    /// them. The subscriber is told to resync instead of being left with a
    /// silently wrong set.
    #[tokio::test]
    async fn a_reparent_gaps_a_subtree_subscriber() {
        let (mut sub, mut rx) = subscriber(
            subtree_selector(QuotaEntityId::new()),
            ProgressItems::Send,
            4,
        );

        assert!(deliver(&mut sub, &reparent_batch(7, true), 3));
        match rx.try_recv() {
            Ok(SubscriptionItem::Gap { earliest_available }) => {
                assert_eq!(earliest_available, 7, "the move's own index")
            }
            other => panic!("expected a gap, got {other:?}"),
        }
        assert!(!sub.gapped, "the marker was accepted; delivery continues");
    }

    /// A reconfiguration that did not move the entity is an ordinary
    /// entity-scoped event: nothing a `jobs=` stream carries, and nothing to
    /// resync for.
    #[tokio::test]
    async fn a_reconfiguration_that_did_not_reparent_gaps_nobody() {
        let (mut sub, mut rx) = subscriber(
            subtree_selector(QuotaEntityId::new()),
            ProgressItems::Send,
            4,
        );
        assert!(deliver(&mut sub, &reparent_batch(7, false), 3));
        assert!(rx.try_recv().is_err(), "no gap, no events");
    }

    /// Only ancestry-reading selectors are affected. A metadata selector and
    /// an exact-entity one both read keys a reparent cannot move — the
    /// chain's head is the job's own entity — and `All` reads none at all.
    #[tokio::test]
    async fn a_reparent_does_not_gap_selectors_that_read_no_subtree() {
        let unaffected = [
            metadata_selector("team", Some("platform")),
            exact_selector(QuotaEntityId::new()),
            EventFilter::All,
            EventFilter::Job(JobId::new()),
        ];
        for filter in unaffected {
            let (mut sub, mut rx) = subscriber(filter, ProgressItems::Send, 4);
            assert!(deliver(&mut sub, &reparent_batch(7, true), 3));
            assert!(
                !matches!(rx.try_recv(), Ok(SubscriptionItem::Gap { .. })),
                "a reparent is not a discontinuity for this selector"
            );
        }
    }

    /// A full queue must not swallow the marker: it takes the overflow path's
    /// pending-gap state, so the retry sweep still delivers it once the client
    /// drains — the one thing a lost reparent gap would cost is a set that is
    /// wrong forever.
    #[tokio::test]
    async fn a_reparent_gap_survives_a_full_queue() {
        let entity = QuotaEntityId::new();
        let job = JobId::new();
        let (mut sub, mut rx) = subscriber(subtree_selector(entity), ProgressItems::Send, 1);

        // Fill the queue with a matching batch the subscriber has not drained.
        let matching = EventBatch {
            scopes: vec![(job, scope(&[QuotaEntityId::new(), entity], None, &[]))],
            ..batch_of(6, vec![job_event(job)])
        };
        assert!(deliver(&mut sub, &matching, 1));

        assert!(deliver(&mut sub, &reparent_batch(7, true), 1));
        assert!(sub.gapped, "the marker is pending, not dropped");

        // Drain, then let the sweep flush it.
        assert!(matches!(
            rx.try_recv(),
            Ok(SubscriptionItem::Events(b)) if b.applied_index == 6
        ));
        let mut ring = Ring::new(0);
        ring.push(one_event_batch(4));
        let mut subs = BTreeMap::new();
        subs.insert(1, sub);
        flush_gaps(&mut subs, &ring);
        assert!(!subs[&1].gapped);
        match rx.try_recv() {
            // The marker waited for queue space, but still reports the move's
            // index rather than the ring's older floor: the resync read has to
            // be at least that fresh.
            Ok(SubscriptionItem::Gap { earliest_available }) => assert_eq!(earliest_available, 7),
            other => panic!("expected the retried gap, got {other:?}"),
        }
    }

    /// A resume whose range crosses a reparent cannot be served whole either:
    /// the page stops at that batch, carrying what is below it and the marker
    /// that says the rest is a gap.
    #[test]
    fn catch_up_stops_at_a_reparent_for_a_subtree_selector() {
        let entity = QuotaEntityId::new();
        let job = JobId::new();
        let matching = |index: u64| EventBatch {
            scopes: vec![(job, scope(&[QuotaEntityId::new(), entity], None, &[]))],
            ..batch_of(index, vec![job_event(job)])
        };
        let mut ring = Ring::new(0);
        ring.push(matching(4));
        ring.push(reparent_batch(5, true));
        ring.push(matching(6));

        let page = ring.catch_up(&subtree_selector(entity), 0, 6, 1_000);
        assert_eq!(
            page.batches
                .iter()
                .map(|b| b.applied_index)
                .collect::<Vec<_>>(),
            vec![4],
            "everything below the reparent is still owed and still honest"
        );
        assert_eq!(page.reparent_at, Some(5));

        // A selector that reads no subtree pages straight across it.
        let page = ring.catch_up(&metadata_selector("team", None), 0, 6, 1_000);
        assert_eq!(page.reparent_at, None);
        assert_eq!(page.next, None, "the whole range was served");
    }

    /// A dropped receiver used to be retried on every batch and every tick,
    /// forever. Delivery now reports the subscription dead so the table drops
    /// it.
    #[tokio::test]
    async fn a_dropped_receiver_ends_the_subscription() {
        let (mut sub, rx) = subscriber(EventFilter::All, ProgressItems::Omit, 4);
        drop(rx);
        assert!(
            !deliver(&mut sub, &batch_of(1, vec![job_event(JobId::new())]), 0),
            "a closed receiver must retire the subscriber, not gap it"
        );
    }

    /// The same, through the periodic sweep: a client that disconnects while
    /// the stream is idle is collected on the next tick rather than lingering.
    #[tokio::test]
    async fn the_sweep_collects_subscribers_whose_receiver_is_gone() {
        let ring = Ring::new(0);
        let mut subs = BTreeMap::new();
        let (live, _live_rx) = subscriber(EventFilter::All, ProgressItems::Omit, 4);
        let (dead, dead_rx) = subscriber(EventFilter::All, ProgressItems::Omit, 4);
        subs.insert(1, live);
        subs.insert(2, dead);
        drop(dead_rx);

        flush_gaps(&mut subs, &ring);
        assert_eq!(subs.keys().copied().collect::<Vec<_>>(), vec![1]);
    }

    // ---- progress bookmarks (ADR 0043) ---------------------------------

    /// A bookmark is never allowed to overtake a batch the subscriber has not
    /// yet drained: both ride the same queue, so ordering is structural. And
    /// when that queue is full the bookmark is *skipped*, never allowed to
    /// gap a subscriber that had done nothing wrong.
    #[tokio::test]
    async fn progress_rides_the_queue_in_order_and_is_skipped_when_it_is_full() {
        let (sub, mut rx) = subscriber(EventFilter::All, ProgressItems::Send, 1);
        let mut subs = BTreeMap::new();
        subs.insert(1, sub);

        // One undelivered batch occupies the whole queue.
        let batch = batch_of(5, vec![job_event(JobId::new())]);
        assert!(deliver(subs.get_mut(&1).unwrap(), &batch, 0));

        send_progress(&mut subs, 5);
        assert!(!subs[&1].gapped, "a skipped bookmark must not gap anyone");

        // The batch comes out first — the bookmark never jumped it — and the
        // skipped bookmark simply is not there.
        match rx.recv().await {
            Some(SubscriptionItem::Events(b)) => assert_eq!(b.applied_index, 5),
            other => panic!("expected the batch first, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "the full-queue bookmark was skipped"
        );

        // With room again, the next tick delivers it.
        send_progress(&mut subs, 5);
        match rx.try_recv() {
            Ok(SubscriptionItem::Progress { index }) => assert_eq!(index, 5),
            other => panic!("expected the bookmark, got {other:?}"),
        }
    }

    /// A gapped subscriber gets no bookmark: it has just been told its
    /// coverage is broken, and "everything at or below N has been sent" would
    /// contradict that.
    #[tokio::test]
    async fn progress_is_withheld_while_a_subscriber_is_gapped() {
        let (mut sub, mut rx) = subscriber(EventFilter::All, ProgressItems::Send, 4);
        sub.gapped = true;
        let mut subs = BTreeMap::new();
        subs.insert(1, sub);

        send_progress(&mut subs, 9);
        assert!(rx.try_recv().is_err());
    }

    /// Internal subscribers opt out entirely (`dispatch`, `derived_stats`):
    /// nothing to discard on their side.
    #[tokio::test]
    async fn progress_is_omitted_for_subscribers_that_did_not_ask() {
        let (sub, mut rx) = subscriber(EventFilter::All, ProgressItems::Omit, 4);
        let mut subs = BTreeMap::new();
        subs.insert(1, sub);
        send_progress(&mut subs, 9);
        assert!(rx.try_recv().is_err());
    }

    /// The timer is the keepalive, so it must actually fire on an idle
    /// stream. Paused time, so this costs no wall-clock.
    #[tokio::test(start_paused = true)]
    async fn the_progress_timer_bookmarks_an_idle_stream() {
        let (tap, tap_rx) = coppice_consensus::EventTap::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (handle, join) = spawn(tap_rx, 42, shutdown_rx);

        let mut sub = handle
            .subscribe(EventFilter::All, ProgressItems::Send)
            .await
            .expect("subscribe");
        assert_eq!(sub.head, 42, "a fresh ring opens at the recovery index");

        // Nothing is ever emitted; the bookmark still arrives.
        tokio::time::advance(EVENT_PROGRESS_INTERVAL + Duration::from_secs(1)).await;
        match sub.items.recv().await {
            Some(SubscriptionItem::Progress { index }) => assert_eq!(index, 42),
            other => panic!("expected an idle-stream bookmark, got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    // ---- subscription cap ----------------------------------------------

    /// Over the cap, subscribe is refused rather than queued: the caller
    /// renders 503 and the client retries against another replica.
    #[tokio::test]
    async fn the_subscription_cap_is_enforced() {
        let ring = Ring::new(0);
        let mut subs = BTreeMap::new();
        for id in 0..MAX_EVENT_SUBSCRIPTIONS as u64 {
            let (sub, rx) = subscriber(EventFilter::All, ProgressItems::Omit, 1);
            // Keep the receivers alive: a closed one is a different refusal.
            std::mem::forget(rx);
            subs.insert(id, sub);
        }

        let (reply_tx, mut reply_rx) = oneshot::channel();
        handle_subscribe(
            &mut subs,
            9_999,
            &ring,
            0,
            SubscribeRequest {
                filter: EventFilter::All,
                progress: ProgressItems::Send,
                reply: reply_tx,
            },
        );
        assert_eq!(
            reply_rx.try_recv().expect("replied synchronously").err(),
            Some(SubscribeError::AtCapacity)
        );
        assert_eq!(
            subs.len(),
            MAX_EVENT_SUBSCRIPTIONS,
            "nothing was registered"
        );
    }

    // ---- catch-up (ADR 0043) -------------------------------------------

    fn one_event_batch(index: u64) -> EventBatch {
        batch_of(index, vec![job_event(JobId::new())])
    }

    /// The defining property: pages cut between batches, never inside one, so
    /// resuming from a page's `next` neither duplicates nor skips a command's
    /// events. The budget here is one event, yet the three-event batch comes
    /// back whole.
    #[test]
    fn catch_up_pages_are_batch_aligned_and_resume_exactly() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(
            5,
            vec![job_event(job), job_event(job), job_event(job)],
        ));
        ring.push(batch_of(6, vec![job_event(job)]));
        ring.push(batch_of(7, vec![job_event(job)]));

        let page1 = ring.catch_up(&EventFilter::Job(job), 0, 7, 1);
        assert_eq!(
            page1
                .batches
                .iter()
                .map(|b| b.applied_index)
                .collect::<Vec<_>>(),
            vec![5],
            "the batch is served whole even though it blows the budget"
        );
        assert_eq!(
            page1.batches[0].events.len(),
            3,
            "a batch is never split across pages"
        );
        assert_eq!(page1.next, Some(5));

        let page2 = ring.catch_up(&EventFilter::Job(job), page1.next.unwrap(), 7, 1);
        assert_eq!(
            page2
                .batches
                .iter()
                .map(|b| b.applied_index)
                .collect::<Vec<_>>(),
            vec![6],
            "resumes strictly after the previous page: no duplicate, no skip"
        );
        assert_eq!(page2.next, Some(6));

        let page3 = ring.catch_up(&EventFilter::Job(job), page2.next.unwrap(), 7, 1_000);
        assert_eq!(
            page3
                .batches
                .iter()
                .map(|b| b.applied_index)
                .collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(page3.next, None, "reached the requested bound");
    }

    /// `up_to` is the head the subscription opened at, and it is exclusive of
    /// nothing above it: batches past it belong to the live stream, and
    /// serving them here would duplicate them.
    #[test]
    fn catch_up_never_reaches_past_its_upper_bound() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(5, vec![job_event(job)]));
        ring.push(batch_of(9, vec![job_event(job)]));

        let page = ring.catch_up(&EventFilter::Job(job), 0, 5, 1_000);
        assert_eq!(
            page.batches
                .iter()
                .map(|b| b.applied_index)
                .collect::<Vec<_>>(),
            vec![5]
        );
        assert_eq!(page.next, None);
    }

    /// A catch-up honours the same filter the subscription runs, including a
    /// `jobs=` selector's before-keys — a job evicted while the client was
    /// away is exactly what it reconnects to find out about.
    #[test]
    fn catch_up_applies_the_jobs_selector_including_before_keys() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(
            4,
            vec![Event::JobEvicted {
                job,
                scope: scope(&[], None, &[("team", "platform")]),
            }],
        ));

        let page = ring.catch_up(&metadata_selector("team", Some("platform")), 0, 4, 1_000);
        assert_eq!(page.batches.len(), 1);
        assert!(ring
            .catch_up(&metadata_selector("team", Some("storage")), 0, 4, 1_000)
            .batches
            .is_empty());
    }

    // ---- ring bounds ---------------------------------------------------

    /// The byte budget is a real bound: a ring well under the count bound is
    /// still evicted once its batches are heavy enough, and eviction raises
    /// the replay floor exactly as the other two bounds do — so a cursor into
    /// the evicted range gaps rather than replaying a hole.
    #[test]
    fn the_ring_byte_budget_evicts_and_raises_the_floor() {
        let mut ring = Ring::new(0);
        // One metadata map big enough that a handful of batches exceed the
        // budget, while the event count stays trivially small.
        let fat = |index: u64| {
            let job = JobId::new();
            EventBatch {
                scopes: vec![(
                    job,
                    JobScope {
                        entity_chain: Vec::new(),
                        submitted_by: None,
                        metadata: (0..64)
                            .map(|i| (format!("key{i}"), "v".repeat(FANOUT_RING_MAX_BYTES / 512)))
                            .collect(),
                    },
                )],
                ..batch_of(index, vec![job_event(job)])
            }
        };

        for index in 1..=32 {
            ring.push(fat(index));
        }
        assert!(
            ring.bytes <= FANOUT_RING_MAX_BYTES,
            "the ring stayed inside its byte budget"
        );
        assert!(
            ring.event_count < FANOUT_RING_MAX_EVENTS,
            "the count bound never came close; bytes is what bound this ring"
        );
        assert!(
            ring.floor() > 0,
            "byte eviction must raise the replay floor like every other eviction"
        );
        assert!(ring.earliest_available() >= ring.floor());
    }

    // ---- Ring::window (GetJobTimeline backstop, ADR 0032) ---------------

    #[test]
    fn window_filters_by_job_in_ascending_identity_order() {
        let job = JobId::new();
        let other = JobId::new();
        let mut ring = Ring::new(0);
        // A mixed batch (other, job) then a job-only batch, both retained.
        ring.push(batch_of(5, vec![job_event(other), job_event(job)]));
        ring.push(batch_of(9, vec![job_event(job)]));

        let window = ring.window(&EventFilter::Job(job), None, 100, 1_000);
        let ids: Vec<(u64, u32)> = window.events.iter().map(|e| (e.index, e.ordinal)).collect();
        // Ascending order, and the filtered event at (5,0) is dropped while the
        // surviving one keeps its batch-assigned ordinal 1 (never renumbered).
        assert_eq!(ids, vec![(5, 1), (9, 0)]);
        assert_eq!(window.floor_index, 0);
        // The walk reached the newest entry, so the caller has everything.
        assert_eq!(window.next, None);
    }

    #[test]
    fn window_after_cursor_resumes_without_duplicates_or_skips() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(5, vec![job_event(job), job_event(job)]));
        ring.push(batch_of(9, vec![job_event(job)]));

        // Page 1: `limit` 2 fills on batch 5, cutting before batch 9.
        let page1 = ring.window(&EventFilter::Job(job), None, 2, 1_000);
        assert_eq!(
            page1
                .events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(5, 0), (5, 1)]
        );
        assert_eq!(page1.next, Some((5, 1)));

        // Page 2: resume strictly after (5,1) — batch 9 only, no (5,*) repeat.
        let page2 = ring.window(&EventFilter::Job(job), page1.next, 2, 1_000);
        assert_eq!(
            page2
                .events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(9, 0)]
        );
        assert_eq!(page2.next, None);
    }

    #[test]
    fn window_limit_cuts_mid_batch_and_stays_resumable() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(
            7,
            vec![job_event(job), job_event(job), job_event(job)],
        ));

        let page = ring.window(&EventFilter::Job(job), None, 2, 1_000);
        assert_eq!(
            page.events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(7, 0), (7, 1)]
        );
        assert_eq!(page.next, Some((7, 1)));

        // Resuming inside the same batch yields only its tail.
        let rest = ring.window(&EventFilter::Job(job), page.next, 100, 1_000);
        assert_eq!(
            rest.events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(7, 2)]
        );
        assert_eq!(rest.next, None);
    }

    #[test]
    fn window_budget_cut_counts_examined_events_and_stays_resumable() {
        let job = JobId::new();
        let other = JobId::new();
        let mut ring = Ring::new(0);
        // Non-matching events precede the match, so the budget is spent on
        // events the filter never collects — `next` must still advance.
        ring.push(batch_of(3, vec![job_event(other), job_event(other)]));
        ring.push(batch_of(4, vec![job_event(job)]));

        // Budget 2 examines (3,0) and (3,1), then stops before (4,0).
        let page1 = ring.window(&EventFilter::Job(job), None, 100, 2);
        assert!(page1.events.is_empty(), "no match within the budget");
        assert_eq!(page1.next, Some((3, 1)));

        // A follow-up with a fresh budget resumes past the examined span and
        // reaches the match — no event skipped by the budget cut.
        let page2 = ring.window(&EventFilter::Job(job), page1.next, 100, 2);
        assert_eq!(
            page2
                .events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(4, 0)]
        );
        assert_eq!(page2.next, None);
    }

    #[test]
    fn window_reports_the_raised_floor_after_eviction() {
        let job = JobId::new();
        let mut ring = Ring::new(0);
        ring.push(batch_of(5, vec![job_event(job)]));
        // Eviction raises the floor to the evicted index (`raise_floor` is what
        // `evict` calls); here a discontinuity raised it past index 4.
        ring.raise_floor(4);

        let window = ring.window(&EventFilter::Job(job), None, 100, 1_000);
        // The floor rides through so the timeline is honestly partial below it,
        // while the event still retained above it is served.
        assert_eq!(window.floor_index, 4);
        assert_eq!(
            window
                .events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(5, 0)]
        );
    }

    #[test]
    fn window_after_below_the_floor_starts_at_the_oldest_and_reports_the_gap() {
        let job = JobId::new();
        let mut ring = Ring::new(10); // recovered at 10; nothing below retained
        ring.push(batch_of(12, vec![job_event(job)]));

        // A cursor below the floor is fine: the scan starts at the oldest
        // retained entry and the floor tells the caller about the gap below.
        let window = ring.window(&EventFilter::Job(job), Some((3, 0)), 100, 1_000);
        assert_eq!(window.floor_index, 10);
        assert_eq!(
            window
                .events
                .iter()
                .map(|e| (e.index, e.ordinal))
                .collect::<Vec<_>>(),
            vec![(12, 0)]
        );
        assert_eq!(window.next, None);
    }

    #[test]
    fn window_is_empty_for_a_job_with_no_ring_events() {
        let mut ring = Ring::new(2);
        ring.push(batch_of(5, vec![job_event(JobId::new())]));

        let window = ring.window(&EventFilter::Job(JobId::new()), None, 100, 1_000);
        assert!(window.events.is_empty());
        // Honest floor even with no matches — distinguishable from index 0.
        assert_eq!(window.floor_index, 2);
        assert_eq!(window.next, None);
    }

    #[test]
    fn empty_ring_reports_floor_as_earliest_available() {
        // After a restart the ring is empty but the floor carries the recovery
        // index — not 0, which would let a stale cursor replay silently.
        let ring = Ring::new(42);
        assert_eq!(ring.floor(), 42);
        assert_eq!(ring.earliest_available(), 42);
    }

    #[test]
    fn earliest_available_never_reports_below_the_floor() {
        // A tap gap can raise the floor above entries still retained; those
        // are not a complete resume point and must not be advertised as one.
        let mut ring = Ring::new(0);
        ring.push(one_event_batch(5));
        ring.raise_floor(21);
        assert_eq!(ring.earliest_available(), 21);
    }

    #[test]
    fn raise_floor_is_monotonic() {
        let mut ring = Ring::new(10);
        ring.raise_floor(5);
        assert_eq!(ring.floor(), 10, "never lowers");
        ring.raise_floor(20);
        assert_eq!(ring.floor(), 20);
    }

    // ---- the running task ----------------------------------------------

    /// A tap that never goes idle must not starve the request inbox: the
    /// biased select polls the tap first, so requests are drained between
    /// tap items instead (an HTTP handler is blocked on the `Window` reply).
    #[tokio::test]
    async fn ring_reads_are_served_under_sustained_event_traffic() {
        let (mut tap, tap_rx) = coppice_consensus::EventTap::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (handle, join) = spawn(tap_rx, 0, shutdown_rx);

        // Keep the tap permanently ready: refill it as fast as the fanout
        // drains it.
        let producer = tokio::spawn(async move {
            let mut index = 1u64;
            loop {
                tap.emit(one_event_batch(index));
                index += 1;
                tokio::task::yield_now().await;
            }
        });

        // Every ring-read round-trip below competes with the saturated tap;
        // under the old biased-select-only loop none of them ever resolved.
        let window = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let window = handle
                    .window(EventFilter::All, None, 3)
                    .await
                    .expect("fanout alive");
                if window.events.len() == 3 {
                    return window;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ring reads must not starve behind sustained tap traffic");
        assert_eq!(window.events.len(), 3);

        producer.abort();
        let _ = shutdown_tx.send(true);
        let _ = join.await;
    }

    /// The converse: a sustained flood of requests must not pin the sweep
    /// and starve the tap (the sweep is bounded per select point). Multi-
    /// threaded so the flooders genuinely refill the inbox while it drains.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tap_makes_progress_under_sustained_request_traffic() {
        let (mut tap, tap_rx) = coppice_consensus::EventTap::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (handle, join) = spawn(tap_rx, 0, shutdown_rx);

        let mut sub = handle
            .subscribe(EventFilter::All, ProgressItems::Omit)
            .await
            .expect("subscribe");

        // Enough concurrent clients to keep the 64-slot inbox refilled.
        let flooders: Vec<_> = (0..64)
            .map(|_| {
                let handle = handle.clone();
                tokio::spawn(async move {
                    while handle.window(EventFilter::All, None, 1).await.is_ok() {}
                })
            })
            .collect();

        // Emitted batches must still reach the subscriber: any delivery
        // (events, or a gap from tap overflow) proves the loop is servicing
        // the tap under the flood.
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            let mut index = 1u64;
            loop {
                tap.emit(one_event_batch(index));
                index += 1;
                match sub.items.try_recv() {
                    Ok(_) => return,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await;
        assert!(
            delivered.is_ok(),
            "tap starved behind sustained request traffic"
        );

        let _ = shutdown_tx.send(true);
        for flooder in flooders {
            flooder.abort();
        }
        let _ = join.await;
    }

    /// A tap-level gap records the discontinuity in the ring, so a later
    /// reconnect across it cannot be caught up silently: the subscription
    /// reports a floor above the client's cursor, which is its cue to gap.
    #[tokio::test]
    async fn tap_gap_raises_the_ring_floor() {
        // Drive `run` with a real tap: deliver one batch, then drop the next as
        // the trailing event so the receiver surfaces a gap (KOI-3).
        let (mut tap, tap_rx) = coppice_consensus::EventTap::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (handle, join) = spawn(tap_rx, 0, shutdown_rx);

        tap.emit(one_event_batch(10));
        tap.emit(one_event_batch(20)); // dropped: channel full -> trailing gap

        // Let the (current-thread) fanout drain the batch and surface the
        // trailing gap, which raises the ring floor past index 10. Biased
        // select drains tap items ahead of subscribe requests.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let sub = handle
            .subscribe(EventFilter::All, ProgressItems::Send)
            .await
            .expect("subscribe");
        assert!(
            sub.floor > 10,
            "a cursor at 10 must now be below the floor, so the connection \
             gaps instead of catching up across the hole"
        );
        // The head advanced over the dropped range too: a bookmark must never
        // claim coverage of indexes the stream lost.
        assert!(sub.head >= sub.floor);

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// KOI-3: cursors are portable across replicas (ADR 0008), so a trailing
    /// gap's floor must cover the *whole* dropped range. A cursor from another
    /// replica that falls inside it must gap, not resume silently.
    #[tokio::test]
    async fn cursor_inside_trailing_drop_range_is_below_the_floor() {
        // Global batches at 10, 15, 20. This replica delivers 10, then drops
        // 15 and 20 as the trailing emissions (tap overflow, then idle): no
        // yields between emits, so the current-thread fanout cannot drain and
        // 10 occupies the single slot.
        let (mut tap, tap_rx) = coppice_consensus::EventTap::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (handle, join) = spawn(tap_rx, 0, shutdown_rx);

        tap.emit(one_event_batch(10));
        tap.emit(one_event_batch(15)); // dropped: channel full
        tap.emit(one_event_batch(20)); // dropped: channel full
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // A client that saw batch 15 on another replica fails over here.
        // Batch 20 was dropped and never entered this ring.
        let sub = handle
            .subscribe(EventFilter::All, ProgressItems::Send)
            .await
            .expect("subscribe");
        assert!(sub.floor > 15, "silent resume across dropped batch 20");

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }
}
