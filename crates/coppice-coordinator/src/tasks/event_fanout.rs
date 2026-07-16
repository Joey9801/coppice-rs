//! Event fanout (every replica).
//!
//! Consumes the [`EventTapReceiver`], owns the ADR 0008 reconnection ring
//! (bounded: [`FANOUT_RING_MAX_AGE`] / [`FANOUT_RING_MAX_EVENTS`]), and
//! manages subscriptions. See `docs/architecture/coordinator-runtime.md`,
//! "Event fanout" and the channel-inventory rows for "fanout ring" and
//! "per-subscriber queue".

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

use coppice_consensus::{EventBatch, EventTapReceiver, TapItem};
use coppice_core::id::{JobId, NodeId};
use coppice_core::time::Timestamp;
use coppice_state::Event;

use crate::limits::{
    FANOUT_GAP_RETRY_INTERVAL, FANOUT_RING_MAX_AGE, FANOUT_RING_MAX_EVENTS,
    SUBSCRIBER_QUEUE_CAPACITY,
};

/// Cross-proposer clock skew, observed at the fanout: |batch `at` − local
/// receipt time| (ADR 0032). Chronic skew is an operational signal — a
/// misconfigured coordinator clock — not something any consumer corrects for.
const PROPOSER_SKEW_SECONDS: &str = "coordinator_event_proposer_skew_seconds";

pub(crate) fn describe_metrics() {
    metrics::describe_histogram!(
        PROPOSER_SKEW_SECONDS,
        metrics::Unit::Seconds,
        "Absolute difference between a batch's proposer stamp and this replica's clock at receipt."
    );
}

pub(crate) fn gather_metrics() {
    // The histogram is pushed as batches arrive; nothing needs sampling.
}

/// What a subscriber wants to see.
///
/// `Job`/`Node` scope by the entity carried on an event (ADR 0008); `All` is
/// the unscoped stream dispatch subscribes with internally. `Job`/`Node` are
/// for the future HTTP subscription endpoints (no transport built yet, so
/// nothing constructs them today — see the module doc on `tasks::api_server`).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventFilter {
    All,
    Job(JobId),
    Node(NodeId),
}

/// A filtered view of one batch, delivered to a subscriber.
///
/// Ordinals are assigned once, before any filtering: each is the event's
/// position within its *full* batch as derived by apply, and it is part of
/// the event's identity from that moment on (ADR 0032). A `Job`- or
/// `Node`-scoped subscription may legitimately see ordinal gaps within an
/// index; renumbering after the filter would give the same event a different
/// identity per subscription scope.
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
/// event's identity from that moment on: a `Job`- or `Node`-scoped
/// subscription may legitimately see ordinal gaps within an index, but it
/// never sees a different ordinal for the same event than an unscoped one
/// would — renumbering after the filter would give the same event a
/// different identity per subscription scope.
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

/// The most recent events the ring retains, newest first.
///
/// `floor_index` is an **exclusive coverage cursor**, the same convention as
/// the ring's replay floor: the window is complete for every applied index
/// *strictly above* it, and claims nothing at or below it. That is ADR
/// 0032's honest-absence vocabulary — an empty `events` on a freshly
/// restarted coordinator carries the recovery index as its cursor,
/// distinguishable from a quiet cluster whose cursor is 0.
#[derive(Debug, Clone)]
pub struct RecentEvents {
    pub floor_index: u64,
    pub events: Vec<StampedEvent>,
}

/// One item delivered to a subscriber.
#[derive(Debug, Clone)]
pub enum SubscriptionItem {
    /// A batch's events admitted by the subscriber's filter, with their
    /// batch-assigned ordinals.
    Events(FilteredBatch),
    /// One or more batches were skipped for this subscriber.
    ///
    /// Either a tap-level gap or its own queue overflowed; resync from `earliest_available`.
    Gap { earliest_available: u64 },
}

/// A live subscription: the receiving half of the subscriber's bounded queue.
pub struct Subscription {
    pub items: mpsc::Receiver<SubscriptionItem>,
}

/// The fanout task has shut down; no more subscriptions can be served.
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
    cursor: Option<u64>,
    reply: oneshot::Sender<Result<Subscription, FanoutClosed>>,
}

/// One request on the fanout task's inbox.
enum Request {
    Subscribe(SubscribeRequest),
    /// The most recent `limit` events from the ring, newest first — the
    /// bounded most-recent cache behind the overview's `recent_events`
    /// (ADR 0032, tier 1). A point-in-time copy, not a subscription.
    Recent {
        limit: usize,
        reply: oneshot::Sender<RecentEvents>,
    },
}

/// Cloneable handle to the fanout task's inbox.
#[derive(Clone)]
pub struct FanoutHandle {
    tx: mpsc::Sender<Request>,
}

impl FanoutHandle {
    /// Subscribe to events matching `filter`.
    ///
    /// `cursor`, when given, resumes from just after that applied index if it
    /// is still within the reconnection ring's retention; otherwise the
    /// subscription opens with an immediate [`SubscriptionItem::Gap`].
    pub async fn subscribe(
        &self,
        filter: EventFilter,
        cursor: Option<u64>,
    ) -> Result<Subscription, FanoutClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Subscribe(SubscribeRequest {
                filter,
                cursor,
                reply: reply_tx,
            }))
            .await
            .map_err(|_| FanoutClosed)?;
        reply_rx.await.map_err(|_| FanoutClosed)?
    }

    /// The most recent `limit` events retained by the ring, newest first,
    /// with the coverage floor (see [`RecentEvents`]).
    pub async fn recent(&self, limit: usize) -> Result<RecentEvents, FanoutClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Recent {
                limit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| FanoutClosed)?;
        reply_rx.await.map_err(|_| FanoutClosed)
    }
}

/// The bounded reconnection ring plus its running event count.
///
/// Eviction never has to rescan the whole ring to decide whether it's over budget.
struct Ring {
    entries: VecDeque<(Instant, EventBatch)>,
    event_count: usize,
    /// The smallest cursor a replay can resume from without silently crossing a
    /// discontinuity. Raised by eviction, tap gaps, and snapshot installs, and
    /// initialized to the index this replica recovered at — so a reconnect with
    /// a pre-restart cursor gaps instead of replaying across the boundary
    /// (KOI-3). A cursor below the floor cannot be served from the ring.
    floor: u64,
}

impl Ring {
    fn new(floor: u64) -> Self {
        Ring {
            entries: VecDeque::new(),
            event_count: 0,
            floor,
        }
    }

    fn push(&mut self, batch: EventBatch) {
        self.event_count += batch.events.len();
        self.entries.push_back((Instant::now(), batch));
        self.evict();
    }

    fn evict(&mut self) {
        while self.event_count > FANOUT_RING_MAX_EVENTS {
            let Some((_, evicted)) = self.entries.pop_front() else {
                break;
            };
            self.event_count -= evicted.events.len();
            // The evicted index is no longer replayable.
            self.raise_floor(evicted.applied_index);
        }
        if let Some(cutoff) = Instant::now().checked_sub(FANOUT_RING_MAX_AGE) {
            while matches!(self.entries.front(), Some((seen_at, _)) if *seen_at < cutoff) {
                let Some((_, evicted)) = self.entries.pop_front() else {
                    break;
                };
                self.event_count -= evicted.events.len();
                self.raise_floor(evicted.applied_index);
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
            .map(|(_, batch)| batch.applied_index)
            .unwrap_or(self.floor)
            .max(self.floor)
    }

    fn iter(&self) -> impl Iterator<Item = &EventBatch> {
        self.entries.iter().map(|(_, batch)| batch)
    }

    /// The most recent `limit` events, newest first, each with its
    /// batch-assigned ordinal and stamp.
    ///
    /// The returned cursor is the coverage the *response* can claim,
    /// exclusive like [`Ring::floor`]: the ring's own floor when everything
    /// retained fit, raised to the cut batch's index when `limit` truncated
    /// (a partially served batch is not covered — the response must not
    /// claim to be the complete record at that index).
    fn recent(&self, limit: usize) -> RecentEvents {
        let mut events = Vec::with_capacity(limit.min(self.event_count));
        let mut floor_index = self.floor();
        'outer: for (_, batch) in self.entries.iter().rev() {
            for (ordinal, event) in batch.events.iter().enumerate().rev() {
                if events.len() == limit {
                    // This event did not fit: coverage is complete only
                    // strictly above its batch's index.
                    floor_index = floor_index.max(batch.applied_index);
                    break 'outer;
                }
                events.push(StampedEvent {
                    index: batch.applied_index,
                    ordinal: ordinal as u32,
                    at: batch.at,
                    event: event.clone(),
                });
            }
        }
        RecentEvents {
            floor_index,
            events,
        }
    }
}

/// One registered subscriber's delivery state.
struct SubscriberState {
    filter: EventFilter,
    tx: mpsc::Sender<SubscriptionItem>,
    /// Set when a `try_send` found the subscriber's queue full.
    ///
    /// Delivery is paused until a `Gap` marker itself is accepted, at which
    /// point normal delivery resumes. See `docs/architecture/coordinator-runtime.md`
    /// ("per-subscriber queue").
    gapped: bool,
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

    // Retries a pending gap to any subscriber whose queue overflowed and then
    // idled, so it still learns to resync even with no further events. Skip
    // missed ticks: a stalled loop needs one flush, not a burst.
    let mut gap_retry = tokio::time::interval(FANOUT_GAP_RETRY_INTERVAL);
    gap_retry.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        // The biased select below polls the tap ahead of the request inbox,
        // so a saturated tap would starve requests indefinitely — and a
        // `Recent` request has an HTTP handler blocked on its reply. Requests
        // are cheap and their inbox is small, so sweep what is pending
        // between select points. The sweep is bounded to one inbox's
        // capacity: concurrent senders can refill the channel while it
        // drains, and an unbounded `while try_recv` would let sustained
        // request traffic pin the loop here and starve the tap — the exact
        // starvation this sweep exists to prevent, reversed.
        for _ in 0..crate::limits::SUBSCRIBE_REQUESTS_CAPACITY {
            match subscribe_rx.try_recv() {
                Ok(req) => handle_request(&mut subscribers, &mut next_id, &ring, req),
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
                        ring.push(batch.clone());
                        let earliest = ring.earliest_available();
                        for sub in subscribers.values_mut() {
                            deliver(sub, &batch, earliest);
                        }
                    }
                    Some(TapItem::Gap { earliest_replayable }) => {
                        // Record the discontinuity in the ring so later
                        // reconnects cannot replay silently across it, then
                        // notify live subscribers to resync.
                        ring.raise_floor(earliest_replayable);
                        let earliest = ring.earliest_available();
                        for sub in subscribers.values_mut() {
                            let gap = SubscriptionItem::Gap { earliest_available: earliest };
                            sub.gapped = sub.tx.try_send(gap).is_err();
                        }
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
                    handle_request(&mut subscribers, &mut next_id, &ring, req);
                }
            }
            _ = gap_retry.tick() => {
                flush_gaps(&mut subscribers, &ring);
            }
        }
    }
    tracing::debug!("event fanout shutting down");
    // Dropping `subscribers` here closes every subscription's channel.
}

/// Serve one inbox request.
fn handle_request(
    subscribers: &mut BTreeMap<u64, SubscriberState>,
    next_id: &mut u64,
    ring: &Ring,
    req: Request,
) {
    match req {
        Request::Subscribe(req) => {
            *next_id += 1;
            handle_subscribe(subscribers, *next_id, ring, req);
        }
        Request::Recent { limit, reply } => {
            // The caller may be gone already; nothing to do.
            let _ = reply.send(ring.recent(limit));
        }
    }
}

/// Re-attempt delivery of a pending gap to every gapped subscriber.
///
/// A subscriber goes gapped when its queue is full at gap time (a fresh
/// overflow, an unflushable tap gap, or a replay that filled the queue). The
/// marker is normally retried on the next batch, but a subscriber that then
/// sees no events would stay wedged; this timer-driven sweep clears it once the
/// queue drains (KOI-3).
fn flush_gaps(subscribers: &mut BTreeMap<u64, SubscriberState>, ring: &Ring) {
    let earliest = ring.earliest_available();
    for sub in subscribers.values_mut() {
        if sub.gapped {
            let gap = SubscriptionItem::Gap {
                earliest_available: earliest,
            };
            if sub.tx.try_send(gap).is_ok() {
                sub.gapped = false;
            }
        }
    }
}

/// Observe |`at` − local now| for the arriving batch (see
/// [`PROPOSER_SKEW_SECONDS`]).
fn record_proposer_skew(batch: &EventBatch) {
    let skew = (Timestamp::now() - batch.at).abs();
    metrics::histogram!(PROPOSER_SKEW_SECONDS).record(skew.as_secs_f64());
}

/// Filter one batch's events down to what `filter` admits, preserving each
/// event's batch-assigned ordinal (ADR 0032: ordinals are assigned before
/// any filtering, so an event's `(index, ordinal)` identity is the same
/// under every subscription scope).
///
/// Returns `None` if nothing in it survives (skip delivering an empty batch).
fn filter_events(filter: &EventFilter, batch: &EventBatch) -> Option<FilteredBatch> {
    let events: Vec<OrdinalEvent> = batch
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| event_matches(filter, e))
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
/// keys the event carries (ADR 0008).
///
/// Attempt- and allocation-scoped events are stamped with their owning job
/// and node during apply, so a `Job`/`Node` subscription sees the complete
/// documented set — no cross-index lookups against state that may have moved
/// on by delivery time.
fn event_matches(filter: &EventFilter, event: &Event) -> bool {
    match (filter, event) {
        (EventFilter::All, _) => true,
        (EventFilter::Job(job), Event::JobSubmitted { job: j }) => j == job,
        (EventFilter::Job(job), Event::JobStateChanged { job: j, .. }) => j == job,
        (EventFilter::Job(job), Event::JobEvicted { job: j }) => j == job,
        (EventFilter::Job(job), Event::AttemptStateChanged { job: j, .. }) => j == job,
        (EventFilter::Job(job), Event::AllocationFunded { job: j, .. }) => j == job,
        (EventFilter::Job(job), Event::StopRequested { job: j, .. }) => j == job,
        (EventFilter::Node(node), Event::StopRequested { node: n, .. }) => n == node,
        (EventFilter::Node(node), Event::NodeEpochBumped { node: n, .. }) => n == node,
        (EventFilter::Node(node), Event::AttemptStateChanged { node: n, .. }) => n == node,
        (EventFilter::Node(node), Event::AllocationFunded { node: n, .. }) => n == node,
        _ => false,
    }
}

/// Deliver one freshly-tapped batch to a subscriber.
///
/// Applies the gap recovery and full-queue policy of the "per-subscriber queue" channel row.
fn deliver(sub: &mut SubscriberState, batch: &EventBatch, ring_earliest: u64) {
    if sub.gapped {
        let gap = SubscriptionItem::Gap {
            earliest_available: ring_earliest,
        };
        match sub.tx.try_send(gap) {
            Ok(()) => sub.gapped = false,
            Err(_) => return, // still backed up; try again on the next batch
        }
    }
    let Some(filtered) = filter_events(&sub.filter, batch) else {
        return;
    };
    if sub.tx.try_send(SubscriptionItem::Events(filtered)).is_err() {
        sub.gapped = true;
    }
}

/// Serve a subscribe request.
///
/// Replay from the ring when the requested cursor is at or above the replay
/// floor, else open with an immediate gap.
fn handle_subscribe(
    subscribers: &mut BTreeMap<u64, SubscriberState>,
    id: u64,
    ring: &Ring,
    req: SubscribeRequest,
) {
    let (tx, rx) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
    let mut gapped = false;

    match req.cursor {
        // A cursor at or above the floor is replayable: the ring holds a
        // complete, gap-free record from just after it.
        Some(cursor) if cursor >= ring.floor() => {
            for batch in ring.iter() {
                if batch.applied_index <= cursor {
                    continue;
                }
                if let Some(filtered) = filter_events(&req.filter, batch) {
                    if tx.try_send(SubscriptionItem::Events(filtered)).is_err() {
                        gapped = true;
                        break;
                    }
                }
            }
        }
        // A cursor below the floor (older than retention, or predating a
        // restart/snapshot/tap-gap discontinuity) cannot be served completely.
        Some(_) => gapped = true,
        // No cursor (fresh subscription): caught up from now, no replay.
        None => {}
    }
    // If the replay itself filled the queue, the gap marker will not fit; carry
    // the gapped flag into the subscriber so the flush retries it, rather than
    // dropping the marker and recording the subscriber as caught up (KOI-3).
    let mut pending_gap = false;
    if gapped
        && tx
            .try_send(SubscriptionItem::Gap {
                earliest_available: ring.earliest_available(),
            })
            .is_err()
    {
        pending_gap = true;
    }

    subscribers.insert(
        id,
        SubscriberState {
            filter: req.filter,
            tx,
            gapped: pending_gap,
        },
    );
    // The receiver may be gone already (an impatient caller); nothing to do
    // either way.
    let _ = req.reply.send(Ok(Subscription { items: rx }));
}

#[cfg(test)]
mod tests {
    use coppice_core::attempt::AttemptState;
    use coppice_core::id::{AllocationId, AttemptId};

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
        }
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

    #[test]
    fn node_filter_admits_attempt_and_allocation_events() {
        let node = NodeId::new();
        let other = NodeId::new();
        let batch = batch_of(1, scoped_events(JobId::new(), node));
        let filtered = filter_events(&EventFilter::Node(node), &batch)
            .expect("attempt/allocation events carry their node");
        assert_eq!(filtered.events.len(), 3);
        assert!(filter_events(&EventFilter::Node(other), &batch).is_none());
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

    #[tokio::test]
    async fn subscriber_gaps_when_its_queue_overflows_then_recovers() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut sub = SubscriberState {
            filter: EventFilter::All,
            tx,
            gapped: false,
        };

        let b1 = batch_of(1, vec![job_event(JobId::new())]);
        let b2 = batch_of(2, vec![job_event(JobId::new())]);
        let b3 = batch_of(3, vec![job_event(JobId::new())]);

        deliver(&mut sub, &b1, 0); // 1/2
        deliver(&mut sub, &b2, 0); // 2/2, still not full
        deliver(&mut sub, &b3, 0); // full -> gapped, b3 dropped
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
        deliver(&mut sub, &b4, 2);
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

    fn one_event_batch(index: u64) -> EventBatch {
        batch_of(index, vec![job_event(JobId::new())])
    }

    fn subscribe(
        subscribers: &mut BTreeMap<u64, SubscriberState>,
        id: u64,
        ring: &Ring,
        cursor: Option<u64>,
    ) -> Subscription {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        handle_subscribe(
            subscribers,
            id,
            ring,
            SubscribeRequest {
                filter: EventFilter::All,
                cursor,
                reply: reply_tx,
            },
        );
        reply_rx
            .try_recv()
            .expect("handle_subscribe replies synchronously")
            .expect("subscription")
    }

    /// A tap that never goes idle must not starve the request inbox: the
    /// biased select polls the tap first, so requests are drained between
    /// tap items instead (an HTTP handler is blocked on the `Recent` reply).
    #[tokio::test]
    async fn recent_is_served_under_sustained_event_traffic() {
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

        // Every `recent` round-trip below competes with the saturated tap;
        // under the old biased-select-only loop none of them ever resolved.
        let recent = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let recent = handle.recent(3).await.expect("fanout alive");
                if recent.events.len() == 3 {
                    return recent;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recent must not starve behind sustained tap traffic");
        assert_eq!(recent.events.len(), 3);

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
            .subscribe(EventFilter::All, None)
            .await
            .expect("subscribe");

        // Enough concurrent clients to keep the 64-slot inbox refilled.
        let flooders: Vec<_> = (0..64)
            .map(|_| {
                let handle = handle.clone();
                tokio::spawn(async move { while handle.recent(1).await.is_ok() {} })
            })
            .collect();

        // Emitted batches must still reach the subscriber: any delivery
        // (events, or a gap from tap overflow) proves the loop is servicing
        // the tap under the flood.
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
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

    #[test]
    fn recent_serves_newest_first_with_the_ring_floor() {
        let mut ring = Ring::new(2);
        ring.push(batch_of(5, vec![job_event(JobId::new())]));
        ring.push(batch_of(
            7,
            vec![job_event(JobId::new()), job_event(JobId::new())],
        ));

        let recent = ring.recent(10);
        // Everything retained fit, so coverage is the ring's own exclusive
        // floor: complete strictly above 2 — which *includes* the oldest
        // served batch at 5.
        assert_eq!(recent.floor_index, 2);
        let identities: Vec<(u64, u32)> =
            recent.events.iter().map(|e| (e.index, e.ordinal)).collect();
        // Newest first: batch 7's events (in reverse batch order), then 5's.
        assert_eq!(identities, vec![(7, 1), (7, 0), (5, 0)]);
    }

    /// A `limit` that cuts a batch must not claim coverage of that batch's
    /// index: the cursor rises to it, so coverage is complete only strictly
    /// above.
    #[test]
    fn recent_truncated_by_limit_raises_its_coverage_floor() {
        let mut ring = Ring::new(0);
        ring.push(batch_of(
            5,
            vec![job_event(JobId::new()), job_event(JobId::new())],
        ));
        ring.push(batch_of(7, vec![job_event(JobId::new())]));

        // Room for batch 7 and only one of batch 5's two events.
        let recent = ring.recent(2);
        assert_eq!(
            recent.events.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![7, 5]
        );
        assert_eq!(recent.floor_index, 5, "batch 5 was cut, so not covered");
    }

    /// ADR 0032's honest-absence vocabulary: an empty window on a restarted
    /// coordinator carries the recovery index as its cursor ("nothing above
    /// 42 has been missed") — distinguishable from a quiet cluster whose
    /// cursor is 0.
    #[test]
    fn recent_on_an_empty_ring_reports_the_floor_not_a_quiet_cluster() {
        let ring = Ring::new(42);
        let recent = ring.recent(10);
        assert!(recent.events.is_empty());
        assert_eq!(recent.floor_index, 42);
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

    /// KOI-3: a cursor from before a restart/snapshot boundary must gap, not
    /// replay across the empty (or discontinuous) ring silently.
    #[tokio::test]
    async fn cursor_below_the_floor_opens_with_a_gap() {
        let mut ring = Ring::new(100); // recovered at index 100
        ring.push(one_event_batch(101));

        let mut subs = BTreeMap::new();
        let mut sub = subscribe(&mut subs, 1, &ring, Some(50));

        match sub.items.recv().await {
            Some(SubscriptionItem::Gap { .. }) => {}
            other => panic!("expected an immediate gap, got {other:?}"),
        }
        assert!(
            !subs.get(&1).unwrap().gapped,
            "the gap fit, nothing pending"
        );
    }

    #[tokio::test]
    async fn cursor_at_the_floor_replays() {
        let mut ring = Ring::new(100);
        ring.push(one_event_batch(101));

        let mut subs = BTreeMap::new();
        let mut sub = subscribe(&mut subs, 1, &ring, Some(100));

        match sub.items.recv().await {
            Some(SubscriptionItem::Events(b)) => assert_eq!(b.applied_index, 101),
            other => panic!("expected the replayed batch, got {other:?}"),
        }
        assert!(!subs.get(&1).unwrap().gapped);
    }

    /// KOI-3: when a cursor replay fills the queue, the gap marker cannot be
    /// enqueued. The subscriber must be recorded *gapped* (not caught up), and a
    /// later flush must deliver the pending gap once the queue drains.
    #[tokio::test]
    async fn replay_overflow_stays_gapped_until_flushed() {
        let mut ring = Ring::new(0);
        for i in 1..=(SUBSCRIBER_QUEUE_CAPACITY as u64 + 5) {
            ring.push(one_event_batch(i));
        }

        let mut subs = BTreeMap::new();
        let mut sub = subscribe(&mut subs, 1, &ring, Some(0));

        // The old bug recorded this subscriber as caught up with the gap marker
        // silently dropped.
        assert!(
            subs.get(&1).unwrap().gapped,
            "an overflowing replay must leave the subscriber gapped"
        );

        // Drain the replayed backlog so the queue has room again.
        while sub.items.try_recv().is_ok() {}

        // A flush now delivers the pending gap and clears the flag.
        flush_gaps(&mut subs, &ring);
        assert!(!subs.get(&1).unwrap().gapped);
        match sub.items.try_recv() {
            Ok(SubscriptionItem::Gap { .. }) => {}
            other => panic!("expected the flushed gap, got {other:?}"),
        }
    }

    /// A tap-level gap records the discontinuity in the ring, so a later
    /// reconnect across it gaps instead of replaying silently.
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

        // A reconnect at cursor 10 must now gap rather than replay across the
        // hole the drop left.
        let mut sub = handle
            .subscribe(EventFilter::All, Some(10))
            .await
            .expect("subscribe");
        match sub.items.recv().await {
            Some(SubscriptionItem::Gap { .. }) => {}
            other => panic!("expected a gap at a cursor below the raised floor, got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// KOI-3: cursors are portable across replicas (ADR 0008), so a trailing
    /// gap's floor must cover the *whole* dropped range. A cursor from another
    /// replica that falls inside it must gap, not replay silently.
    #[tokio::test]
    async fn cursor_inside_trailing_drop_range_gaps() {
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
        // Batch 20 was dropped and never entered this ring, so this must gap.
        let mut sub = handle
            .subscribe(EventFilter::All, Some(15))
            .await
            .expect("subscribe");
        match sub.items.try_recv() {
            Ok(SubscriptionItem::Gap { .. }) => {}
            other => panic!("silent replay across dropped batch 20: {other:?}"),
        }

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }
}
