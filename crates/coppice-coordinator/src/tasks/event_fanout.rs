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

use coppice_consensus::{EventBatch, EventTapReceiver, TapItem};
use coppice_core::id::{JobId, NodeId};
use coppice_state::Event;

use crate::limits::{FANOUT_RING_MAX_AGE, FANOUT_RING_MAX_EVENTS, SUBSCRIBER_QUEUE_CAPACITY};

/// What a subscriber wants to see. `Job`/`Node` scope by the entity carried
/// on an event (ADR 0008); `All` is the unscoped stream dispatch subscribes
/// with internally. `Job`/`Node` are for the future HTTP subscription
/// endpoints (no transport built yet, so nothing constructs them today —
/// see the module doc on `tasks::api_server`).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventFilter {
    All,
    Job(JobId),
    Node(NodeId),
}

/// One item delivered to a subscriber.
#[derive(Debug, Clone)]
pub enum SubscriptionItem {
    /// A batch of events admitted by the subscriber's filter.
    Events(EventBatch),
    /// One or more batches were skipped for this subscriber (a tap-level
    /// gap, or its own queue overflowed); resync from `earliest_available`.
    Gap { earliest_available: u64 },
}

/// A live subscription: the receiving half of the subscriber's bounded
/// queue.
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

/// Cloneable handle to the fanout task's subscribe inbox.
#[derive(Clone)]
pub struct FanoutHandle {
    tx: mpsc::Sender<SubscribeRequest>,
}

impl FanoutHandle {
    /// Subscribe to events matching `filter`. `cursor`, when given, resumes
    /// from just after that applied index if it is still within the
    /// reconnection ring's retention; otherwise the subscription opens with
    /// an immediate [`SubscriptionItem::Gap`].
    pub async fn subscribe(
        &self,
        filter: EventFilter,
        cursor: Option<u64>,
    ) -> Result<Subscription, FanoutClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SubscribeRequest { filter, cursor, reply: reply_tx })
            .await
            .map_err(|_| FanoutClosed)?;
        reply_rx.await.map_err(|_| FanoutClosed)?
    }
}

/// The bounded reconnection ring plus its running event count, so eviction
/// never has to rescan the whole ring to decide whether it's over budget.
struct Ring {
    entries: VecDeque<(Instant, EventBatch)>,
    event_count: usize,
}

impl Ring {
    fn new() -> Self {
        Ring { entries: VecDeque::new(), event_count: 0 }
    }

    fn push(&mut self, batch: EventBatch) {
        self.event_count += batch.events.len();
        self.entries.push_back((Instant::now(), batch));
        self.evict();
    }

    fn evict(&mut self) {
        while self.event_count > FANOUT_RING_MAX_EVENTS {
            let Some((_, evicted)) = self.entries.pop_front() else { break };
            self.event_count -= evicted.events.len();
        }
        if let Some(cutoff) = Instant::now().checked_sub(FANOUT_RING_MAX_AGE) {
            while matches!(self.entries.front(), Some((seen_at, _)) if *seen_at < cutoff) {
                let Some((_, evicted)) = self.entries.pop_front() else { break };
                self.event_count -= evicted.events.len();
            }
        }
    }

    /// The oldest applied index still retained, or 0 when the ring is empty
    /// (an empty ring retains everything from the start).
    fn earliest_index(&self) -> u64 {
        self.entries.front().map(|(_, batch)| batch.applied_index).unwrap_or(0)
    }

    fn iter(&self) -> impl Iterator<Item = &EventBatch> {
        self.entries.iter().map(|(_, batch)| batch)
    }
}

/// One registered subscriber's delivery state.
struct SubscriberState {
    filter: EventFilter,
    tx: mpsc::Sender<SubscriptionItem>,
    /// Set when a `try_send` found the subscriber's queue full. Delivery is
    /// paused until a `Gap` marker itself is accepted, at which point normal
    /// delivery resumes (`docs/architecture/coordinator-runtime.md`,
    /// "per-subscriber queue").
    gapped: bool,
}

/// Spawn the fanout task. Returns the handle other tasks subscribe through,
/// plus its `JoinHandle`.
pub fn spawn(
    event_tap: EventTapReceiver,
    shutdown: watch::Receiver<bool>,
) -> (FanoutHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(crate::limits::SUBSCRIBE_REQUESTS_CAPACITY);
    let handle = FanoutHandle { tx };
    let join = tokio::spawn(run(event_tap, rx, shutdown));
    (handle, join)
}

async fn run(
    mut event_tap: EventTapReceiver,
    mut subscribe_rx: mpsc::Receiver<SubscribeRequest>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ring = Ring::new();
    let mut subscribers: BTreeMap<u64, SubscriberState> = BTreeMap::new();
    let mut next_id: u64 = 0;

    loop {
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
                        ring.push(batch.clone());
                        for sub in subscribers.values_mut() {
                            deliver(sub, &batch, ring.earliest_index());
                        }
                    }
                    Some(TapItem::Gap { resume_after_index }) => {
                        for sub in subscribers.values_mut() {
                            let gap = SubscriptionItem::Gap { earliest_available: resume_after_index };
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
                    next_id += 1;
                    handle_subscribe(&mut subscribers, next_id, &ring, req);
                }
            }
        }
    }
    tracing::info!("event fanout: shutting down");
    // Dropping `subscribers` here closes every subscription's channel.
}

/// Filter one batch's events down to what `filter` admits, or `None` if
/// nothing in it survives (skip delivering an empty batch).
fn filter_events(filter: &EventFilter, batch: &EventBatch) -> Option<EventBatch> {
    if matches!(filter, EventFilter::All) {
        return Some(batch.clone());
    }
    let events: Vec<Event> = batch.events.iter().filter(|e| event_matches(filter, e)).cloned().collect();
    if events.is_empty() {
        None
    } else {
        Some(EventBatch { applied_index: batch.applied_index, events })
    }
}

/// Whether `event` is in `filter`'s scope. // scope keys per ADR 0008
///
/// Only the event variants that carry a job or node id directly can be
/// scoped this way; attempt/allocation-scoped events would need a
/// job/node cross-index to place into a `Job`/`Node` filter, which is
/// future work — they are simply not delivered to scoped subscribers today.
fn event_matches(filter: &EventFilter, event: &Event) -> bool {
    match (filter, event) {
        (EventFilter::All, _) => true,
        (EventFilter::Job(job), Event::JobSubmitted { job: j }) => j == job,
        (EventFilter::Job(job), Event::JobStateChanged { job: j, .. }) => j == job,
        (EventFilter::Job(job), Event::JobEvicted { job: j }) => j == job,
        (EventFilter::Node(node), Event::StopRequested { node: n, .. }) => n == node,
        (EventFilter::Node(node), Event::NodeEpochBumped { node: n, .. }) => n == node,
        _ => false,
    }
}

/// Deliver one freshly-tapped batch to a subscriber, applying the gap
/// recovery and full-queue policy of the "per-subscriber queue" channel row.
fn deliver(sub: &mut SubscriberState, batch: &EventBatch, ring_earliest: u64) {
    if sub.gapped {
        let gap = SubscriptionItem::Gap { earliest_available: ring_earliest };
        match sub.tx.try_send(gap) {
            Ok(()) => sub.gapped = false,
            Err(_) => return, // still backed up; try again on the next batch
        }
    }
    let Some(filtered) = filter_events(&sub.filter, batch) else { return };
    if sub.tx.try_send(SubscriptionItem::Events(filtered)).is_err() {
        sub.gapped = true;
    }
}

/// Serve a subscribe request: replay from the ring when the requested
/// cursor is within retention, else open with an immediate gap.
fn handle_subscribe(
    subscribers: &mut BTreeMap<u64, SubscriberState>,
    id: u64,
    ring: &Ring,
    req: SubscribeRequest,
) {
    let (tx, rx) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
    let mut gapped = false;

    match req.cursor {
        Some(cursor) if cursor.saturating_add(1) >= ring.earliest_index() => {
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
        // No cursor (fresh subscription) or one older than retention.
        Some(_) => gapped = true,
        None => {}
    }
    if gapped {
        let _ = tx.try_send(SubscriptionItem::Gap { earliest_available: ring.earliest_index() });
    }

    subscribers.insert(id, SubscriberState { filter: req.filter, tx, gapped: false });
    // The receiver may be gone already (an impatient caller); nothing to do
    // either way.
    let _ = req.reply.send(Ok(Subscription { items: rx }));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_event(job: JobId) -> Event {
        Event::JobSubmitted { job }
    }

    #[test]
    fn all_filter_admits_everything() {
        let batch = EventBatch { applied_index: 1, events: vec![job_event(JobId::new())] };
        assert!(filter_events(&EventFilter::All, &batch).is_some());
    }

    #[test]
    fn job_filter_only_admits_its_own_job() {
        let job = JobId::new();
        let other = JobId::new();
        let batch = EventBatch { applied_index: 1, events: vec![job_event(job)] };
        assert!(filter_events(&EventFilter::Job(job), &batch).is_some());
        assert!(filter_events(&EventFilter::Job(other), &batch).is_none());
    }

    #[test]
    fn node_filter_only_admits_its_own_node() {
        let node = NodeId::new();
        let other = NodeId::new();
        let event = Event::NodeEpochBumped { node, epoch: 1 };
        let batch = EventBatch { applied_index: 1, events: vec![event] };
        assert!(filter_events(&EventFilter::Node(node), &batch).is_some());
        assert!(filter_events(&EventFilter::Node(other), &batch).is_none());
    }

    #[tokio::test]
    async fn subscriber_gaps_when_its_queue_overflows_then_recovers() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut sub = SubscriberState { filter: EventFilter::All, tx, gapped: false };

        let b1 = EventBatch { applied_index: 1, events: vec![job_event(JobId::new())] };
        let b2 = EventBatch { applied_index: 2, events: vec![job_event(JobId::new())] };
        let b3 = EventBatch { applied_index: 3, events: vec![job_event(JobId::new())] };

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
        let b4 = EventBatch { applied_index: 4, events: vec![job_event(JobId::new())] };
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
}
