//! One client's event subscription: the connection task behind
//! `GET /api/v1/events` (ADR 0043).
//!
//! The fanout deliberately knows nothing about resuming. It hands a new
//! subscriber the `head` it registered at and a live queue of everything
//! strictly above it; the range *below* head that a resuming client is owed is
//! pulled here, one batch-aligned page at a time, by a task that belongs to
//! that one connection. Three things follow from putting it here rather than
//! in the fanout:
//!
//! - a slow or enormous resume costs the fanout task one page-sized scan at a
//!   time, instead of an unbounded replay that blocks every other subscriber;
//! - the live queue keeps filling during the catch-up, so the two ranges meet
//!   with no hole — and if it overflows, the ordinary overflow→gap path
//!   applies, exactly as it would at any other moment;
//! - the task owns the drain: it ends the stream the moment the shutdown watch
//!   flips, so an open SSE connection can never hold the API listener's drain
//!   to its deadline (`docs/agent-notes/architecture-gotchas.md`).
//!
//! The task exits — closing the stream — when the client disconnects (its
//! receiver drops), the fanout shuts down, or shutdown is signalled.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use coppice_api::events::{
    EventBatchItem, EventStreamItem, EventSubscription, JobSelector, OrdinalEvent,
};
use coppice_api::ApiError;

use crate::limits::EVENT_STREAM_QUEUE_CAPACITY;
use crate::tasks::event_fanout::{
    EventFilter, FanoutHandle, FilteredBatch, ProgressItems, SubscribeError, Subscription,
    SubscriptionItem,
};

/// Open one subscription and spawn the task that drives it.
///
/// Registering with the fanout happens *before* the task is spawned, so a
/// refusal — the subscription cap, or a fanout already shutting down — is
/// reported to the HTTP handler synchronously as a 503 rather than as a
/// stream that opens and immediately ends.
pub(crate) async fn open(
    fanout: &FanoutHandle,
    selector: Arc<JobSelector>,
    cursor: Option<u64>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<EventSubscription, ApiError> {
    let filter = EventFilter::Jobs(selector);
    let subscription = fanout
        .subscribe(filter.clone(), ProgressItems::Send)
        .await
        .map_err(|e| match e {
            // Both are "not right now, try another replica": one because this
            // replica is full, one because it is going away.
            SubscribeError::AtCapacity | SubscribeError::Closed => {
                ApiError::Unavailable(e.to_string())
            }
        })?;

    let (tx, items) = mpsc::channel(EVENT_STREAM_QUEUE_CAPACITY);
    tokio::spawn(drive(
        fanout.clone(),
        filter,
        subscription,
        cursor,
        tx,
        shutdown,
    ));
    Ok(EventSubscription { items })
}

/// Resolve once `stop` has been flipped, or never when there is no watch.
///
/// Borrowing inside the condition rather than using `watch::Receiver::wait_for`
/// keeps the (non-`Send`) read guard out of the future, which is held across
/// awaits in the `select!`s below — the same reason `clientedge` does it this
/// way. A dropped sender counts as stopped: nobody is left to signal.
async fn stopped(stop: &mut Option<watch::Receiver<bool>>) {
    let Some(stop) = stop else {
        return std::future::pending().await;
    };
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// Drive one subscription: catch up to `head`, then relay the live queue.
async fn drive(
    fanout: FanoutHandle,
    filter: EventFilter,
    subscription: Subscription,
    cursor: Option<u64>,
    tx: mpsc::Sender<EventStreamItem>,
    mut shutdown: Option<watch::Receiver<bool>>,
) {
    let Subscription {
        mut items,
        head,
        floor,
        earliest_available,
    } = subscription;

    // The highest index this connection has *sent*. It is what makes the
    // catch-up and the live queue meet exactly: a live batch at or below it
    // has already gone out (or was deliberately skipped) and is dropped.
    let mut last_sent = head;
    // Whether to open with a progress bookmark. A stream that opened with a
    // gap must not: the client has just been told its coverage is broken, and
    // "everything at or below N has been sent" would contradict that.
    let mut bookmark = true;

    match cursor {
        // Fresh subscription: caught up from now, nothing owed below head.
        None => {}
        // The client has seen further than this replica has applied — an
        // ordinary failover onto a lagging follower. Nothing to catch up on,
        // and every live batch at or below its cursor is one it already has.
        Some(cursor) if cursor >= head => last_sent = cursor,
        // Below the ring's floor: older than retention, or on the far side of
        // a restart, snapshot install, or tap gap. No complete resume exists,
        // so say so once and continue live (ADR 0008 gap-and-resync).
        Some(cursor) if cursor < floor => {
            if !forward(
                &tx,
                EventStreamItem::Gap { earliest_available },
                &mut shutdown,
            )
            .await
            {
                return;
            }
            bookmark = false;
        }
        Some(cursor) => match catch_up(&fanout, &filter, cursor, head, &tx, &mut shutdown).await {
            CatchUp::Done => {}
            CatchUp::Gapped => bookmark = false,
            CatchUp::Ended => return,
        },
    }

    if bookmark
        && !forward(
            &tx,
            EventStreamItem::Progress { index: last_sent },
            &mut shutdown,
        )
        .await
    {
        return;
    }

    loop {
        let item = tokio::select! {
            biased;
            _ = stopped(&mut shutdown) => return,
            // The client hung up. Without this arm a quiet stream would only
            // find out at its next item — up to a progress interval later —
            // and hold its slot under the subscription cap until then.
            _ = tx.closed() => return,
            item = items.recv() => item,
        };
        // `None` is the fanout closing its subscribers — the clean end of
        // this stream.
        let Some(item) = item else { return };
        let out = match item {
            // Everything at or below `last_sent` was served by the catch-up
            // (or belongs to a cursor ahead of this replica): dropping it here
            // is what makes the handoff duplicate-free.
            SubscriptionItem::Events(batch) if batch.applied_index <= last_sent => continue,
            SubscriptionItem::Events(batch) => {
                last_sent = batch.applied_index;
                EventStreamItem::Batch(convert(batch))
            }
            SubscriptionItem::Gap { earliest_available } => {
                EventStreamItem::Gap { earliest_available }
            }
            // A bookmark enqueued while this task was still catching up can
            // name an index it had not reached yet; forwarding it would claim
            // coverage the client has not been given.
            SubscriptionItem::Progress { index } if index < last_sent => continue,
            SubscriptionItem::Progress { index } => EventStreamItem::Progress { index },
        };
        if !forward(&tx, out, &mut shutdown).await {
            return;
        }
    }
}

/// How a catch-up ended.
enum CatchUp {
    /// The whole `(cursor, head]` range was served.
    Done,
    /// The range could not be served whole — retention overtook the resume
    /// point mid-way, or it crossed a quota-entity reparent this selector
    /// reads (ADR 0043). A gap was sent and the stream continues live.
    Gapped,
    /// The client is gone, the fanout is gone, or shutdown fired.
    Ended,
}

/// Page the ring over `(cursor, up_to]`, forwarding whole batches.
async fn catch_up(
    fanout: &FanoutHandle,
    filter: &EventFilter,
    cursor: u64,
    up_to: u64,
    tx: &mpsc::Sender<EventStreamItem>,
    shutdown: &mut Option<watch::Receiver<bool>>,
) -> CatchUp {
    let mut after = cursor;
    loop {
        let page = tokio::select! {
            biased;
            _ = stopped(shutdown) => return CatchUp::Ended,
            page = fanout.catch_up(filter.clone(), after, up_to) => match page {
                Ok(page) => page,
                // The fanout went away mid-catch-up.
                Err(_closed) => return CatchUp::Ended,
            },
        };
        // Eviction can overtake a slow catch-up: by the time this page was
        // read, the floor may have risen past where the last one stopped, and
        // the range in between is simply gone. Better an honest gap than a
        // page that silently starts above the hole.
        if page.floor > after {
            let gap = EventStreamItem::Gap {
                earliest_available: page.earliest_available,
            };
            return if forward(tx, gap, shutdown).await {
                CatchUp::Gapped
            } else {
                CatchUp::Ended
            };
        }
        for batch in page.batches {
            if !forward(tx, EventStreamItem::Batch(convert(batch)), shutdown).await {
                return CatchUp::Ended;
            }
        }
        // The page stopped at a quota-entity reparent, which changes which
        // jobs this selector admits without any event naming them (ADR 0043).
        // Everything below it has just gone out; the rest of the range is a
        // gap, and the stream continues live from head — the same shape as a
        // resume point retention overtook.
        if let Some(reparent_at) = page.reparent_at {
            // The resync has to reflect the move, so the move's own index is
            // the earliest position a complete resume can start from — not
            // the ring's, which may sit far below it.
            let gap = EventStreamItem::Gap {
                earliest_available: reparent_at.max(page.earliest_available),
            };
            return if forward(tx, gap, shutdown).await {
                CatchUp::Gapped
            } else {
                CatchUp::Ended
            };
        }
        match page.next {
            Some(next) => after = next,
            None => return CatchUp::Done,
        }
    }
}

/// Hand one item to the HTTP handler, or report that this stream is over.
///
/// The send is bounded by the shutdown watch, not just by the client: a
/// connection whose reader has stopped draining would otherwise park here
/// indefinitely and hold the listener's drain to its deadline (issue #111's
/// shape, in a different place).
async fn forward(
    tx: &mpsc::Sender<EventStreamItem>,
    item: EventStreamItem,
    shutdown: &mut Option<watch::Receiver<bool>>,
) -> bool {
    tokio::select! {
        biased;
        _ = stopped(shutdown) => false,
        sent = tx.send(item) => sent.is_ok(),
    }
}

/// Move a filtered batch across the seam. The events are moved, not cloned:
/// this batch was filtered for this subscriber and nobody else holds it.
fn convert(batch: FilteredBatch) -> EventBatchItem {
    EventBatchItem {
        index: batch.applied_index,
        at: batch.at,
        events: batch
            .events
            .into_iter()
            .map(|e| OrdinalEvent {
                ordinal: e.ordinal,
                event: e.event,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coppice_api::http::dto;
    use coppice_consensus::{EventBatch, EventTap};
    use coppice_core::id::JobId;
    use coppice_core::time::Timestamp;
    use coppice_state::Event;

    use super::*;

    fn any_job_selector() -> Arc<JobSelector> {
        // `not` of a metadata key nothing sets: admits every job, so these
        // tests exercise the stream mechanics rather than the matcher (which
        // `event_fanout` covers).
        Arc::new(
            JobSelector::compile(&dto::JobFilter::Not(Box::new(dto::JobFilter::Metadata(
                dto::MetadataFilter {
                    key: "never-set".into(),
                    equals: None,
                },
            ))))
            .expect("allowed leaves"),
        )
    }

    fn batch(index: u64) -> EventBatch {
        let job = JobId::new();
        EventBatch {
            applied_index: index,
            at: Timestamp::UNIX_EPOCH,
            events: vec![Event::JobSubmitted { job }],
            scopes: vec![(
                job,
                coppice_state::JobScope {
                    entity_chain: Vec::new(),
                    submitted_by: None,
                    metadata: Default::default(),
                },
            )],
        }
    }

    /// A batch holding one job whose stamped chain sits under `entity`.
    fn batch_under(index: u64, entity: coppice_core::id::QuotaEntityId) -> EventBatch {
        let job = JobId::new();
        EventBatch {
            applied_index: index,
            at: Timestamp::UNIX_EPOCH,
            events: vec![Event::JobSubmitted { job }],
            scopes: vec![(
                job,
                coppice_state::JobScope {
                    entity_chain: vec![coppice_core::id::QuotaEntityId::new(), entity],
                    submitted_by: None,
                    metadata: Default::default(),
                },
            )],
        }
    }

    /// The batch a `ConfigureQuotaEntity` that moved an existing entity
    /// produces: one entity-scoped event naming no job at all.
    fn reparent_batch(index: u64) -> EventBatch {
        EventBatch {
            applied_index: index,
            at: Timestamp::UNIX_EPOCH,
            events: vec![Event::QuotaEntityConfigured {
                entity: coppice_core::id::QuotaEntityId::new(),
                reparented: true,
            }],
            scopes: Vec::new(),
        }
    }

    fn subtree_selector(entity: coppice_core::id::QuotaEntityId) -> Arc<JobSelector> {
        Arc::new(
            JobSelector::compile(&dto::JobFilter::Entity(dto::EntityFilter {
                entity: entity.into(),
                scope: dto::EntityScope::Subtree,
            }))
            .expect("allowed leaf"),
        )
    }

    /// Drain items until `want` batch indexes have arrived, collecting every
    /// item seen. Bounded so a bug shows up as a failure, not a hang.
    async fn collect(sub: &mut EventSubscription, want: usize) -> (Vec<u64>, Vec<EventStreamItem>) {
        let mut indexes = Vec::new();
        let mut all = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while indexes.len() < want {
                let Some(item) = sub.items.recv().await else {
                    break;
                };
                if let EventStreamItem::Batch(b) = &item {
                    indexes.push(b.index);
                }
                all.push(item);
            }
        })
        .await
        .expect("the stream must produce the expected batches");
        (indexes, all)
    }

    /// Let the current-thread fanout drain whatever is queued on the tap.
    async fn settle() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    /// The handoff: a client resumes mid-history *while* new batches keep
    /// arriving. Everything in `(cursor, ∞)` must arrive exactly once, in
    /// order — the catch-up range and the live range meeting with no overlap
    /// and no hole is the whole point of pulling one and pushing the other.
    #[tokio::test]
    async fn catch_up_hands_off_to_live_with_no_loss_and_no_duplicates() {
        let (mut tap, tap_rx) = EventTap::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx.clone());

        // History the client missed.
        for index in 1..=5 {
            tap.emit(batch(index));
        }
        settle().await;

        let mut sub = open(&fanout, any_job_selector(), Some(2), Some(shutdown_rx))
            .await
            .expect("subscribe");

        // More batches land while the catch-up is in flight.
        for index in 6..=9 {
            tap.emit(batch(index));
        }

        let (indexes, _) = collect(&mut sub, 7).await;
        assert_eq!(
            indexes,
            vec![3, 4, 5, 6, 7, 8, 9],
            "ascending, complete, and each index exactly once"
        );

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// A cursor below the ring's floor cannot be served completely, so the
    /// stream opens with a gap — and then keeps going live, rather than
    /// ending and making the client reconnect.
    #[tokio::test]
    async fn a_cursor_below_the_floor_opens_with_a_gap_then_runs_live() {
        let (mut tap, tap_rx) = EventTap::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Recovered at index 100: nothing below it is replayable (KOI-3).
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 100, shutdown_rx.clone());
        tap.emit(batch(101));
        settle().await;

        let mut sub = open(&fanout, any_job_selector(), Some(50), Some(shutdown_rx))
            .await
            .expect("subscribe");

        match tokio::time::timeout(Duration::from_secs(5), sub.items.recv())
            .await
            .expect("an immediate gap")
        {
            Some(EventStreamItem::Gap { earliest_available }) => {
                assert!(earliest_available >= 100)
            }
            other => panic!("expected a gap, got {other:?}"),
        }

        // The stream stays open: live batches keep coming.
        tap.emit(batch(102));
        let (indexes, _) = collect(&mut sub, 1).await;
        assert_eq!(indexes, vec![102]);

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// A cursor above this replica's head — a failover onto a lagging
    /// follower — must not re-serve what the client already saw elsewhere.
    /// Cursors are portable across replicas (ADR 0008), so this is ordinary,
    /// not an error.
    #[tokio::test]
    async fn a_cursor_above_the_head_delivers_no_duplicates() {
        let (mut tap, tap_rx) = EventTap::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx.clone());
        for index in 1..=3 {
            tap.emit(batch(index));
        }
        settle().await;

        // The client saw up to index 5 on another replica; this one has
        // applied only 3.
        let mut sub = open(&fanout, any_job_selector(), Some(5), Some(shutdown_rx))
            .await
            .expect("subscribe");

        // Indexes 4 and 5 arrive here late and must be swallowed; 6 is new.
        for index in 4..=6 {
            tap.emit(batch(index));
        }
        let (indexes, _) = collect(&mut sub, 1).await;
        assert_eq!(indexes, vec![6], "nothing at or below the client's cursor");

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// A resume whose range crosses a quota-entity reparent gets the batches
    /// below it, then a gap, then the live stream — and no opening bookmark,
    /// which would claim the coverage the gap just denied (ADR 0043).
    #[tokio::test]
    async fn a_catch_up_across_a_reparent_gaps_and_goes_live() {
        let entity = coppice_core::id::QuotaEntityId::new();
        let (mut tap, tap_rx) = EventTap::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx.clone());

        tap.emit(batch_under(1, entity));
        tap.emit(batch_under(2, entity));
        tap.emit(reparent_batch(3));
        tap.emit(batch_under(4, entity));
        settle().await;

        let mut sub = open(
            &fanout,
            subtree_selector(entity),
            Some(0),
            Some(shutdown_rx),
        )
        .await
        .expect("subscribe");

        // Below the reparent, delivered; then the discontinuity itself.
        let (indexes, items) = collect(&mut sub, 2).await;
        assert_eq!(indexes, vec![1, 2]);
        match tokio::time::timeout(Duration::from_secs(5), sub.items.recv())
            .await
            .expect("a gap at the reparent")
        {
            Some(EventStreamItem::Gap { .. }) => {}
            other => panic!("expected a gap, got {other:?}"),
        }
        assert!(
            !items
                .iter()
                .any(|i| matches!(i, EventStreamItem::Progress { .. })),
            "a stream that gapped must not bookmark its way past the hole"
        );

        // The stream stays open and live: batch 4 was above the reparent and
        // below head, so it is part of what the resync covers; 5 is new.
        tap.emit(batch_under(5, entity));
        let (indexes, items) = collect(&mut sub, 1).await;
        assert_eq!(indexes, vec![5]);
        assert!(!items
            .iter()
            .any(|i| matches!(i, EventStreamItem::Gap { .. })));

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// The same history, read by a selector that names no subtree: a reparent
    /// cannot move a job into or out of its set, so the catch-up runs straight
    /// through with no gap at all.
    #[tokio::test]
    async fn a_selector_that_reads_no_subtree_pages_across_a_reparent() {
        let entity = coppice_core::id::QuotaEntityId::new();
        let (mut tap, tap_rx) = EventTap::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx.clone());

        tap.emit(batch_under(1, entity));
        tap.emit(batch_under(2, entity));
        tap.emit(reparent_batch(3));
        tap.emit(batch_under(4, entity));
        settle().await;

        let mut sub = open(&fanout, any_job_selector(), Some(0), Some(shutdown_rx))
            .await
            .expect("subscribe");

        let (indexes, items) = collect(&mut sub, 3).await;
        assert_eq!(indexes, vec![1, 2, 4], "the whole range, in order");
        assert!(
            !items
                .iter()
                .any(|i| matches!(i, EventStreamItem::Gap { .. })),
            "no discontinuity for a selector that reads no ancestry"
        );

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// A fresh subscription opens with a bookmark, so a client with no cursor
    /// has one to reconnect with before anything has happened.
    #[tokio::test]
    async fn a_fresh_subscription_opens_with_a_progress_bookmark() {
        let (tap, tap_rx) = EventTap::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 77, shutdown_rx.clone());

        let mut sub = open(&fanout, any_job_selector(), None, Some(shutdown_rx))
            .await
            .expect("subscribe");
        match tokio::time::timeout(Duration::from_secs(5), sub.items.recv())
            .await
            .expect("an opening bookmark")
        {
            Some(EventStreamItem::Progress { index }) => assert_eq!(index, 77),
            other => panic!("expected the opening bookmark, got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// The drain contract: an open stream ends *promptly* when shutdown is
    /// signalled, so the API listener's drain never waits on it. Asserted
    /// against the shutdown watch alone — the fanout task is not even given a
    /// chance to close the subscription here.
    #[tokio::test]
    async fn the_stream_ends_promptly_when_shutdown_is_signalled() {
        let (tap, tap_rx) = EventTap::channel(8);
        let (fanout_shutdown_tx, fanout_shutdown_rx) = watch::channel(false);
        let (drain_tx, drain_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, fanout_shutdown_rx);

        let mut sub = open(&fanout, any_job_selector(), None, Some(drain_rx))
            .await
            .expect("subscribe");
        // Consume the opening bookmark so the task is parked on the live
        // queue, which is where a real connection spends its life.
        assert!(matches!(
            sub.items.recv().await,
            Some(EventStreamItem::Progress { .. })
        ));

        let _ = drain_tx.send(true);
        let ended = tokio::time::timeout(Duration::from_secs(5), sub.items.recv())
            .await
            .expect("the stream must end at the drain, not at its deadline");
        assert!(ended.is_none(), "the stream closed rather than stalling");

        let _ = fanout_shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }

    /// The other clean end: the fanout going away closes the stream even
    /// though this task's own drain signal never fired.
    #[tokio::test]
    async fn the_stream_ends_when_the_fanout_closes() {
        let (tap, tap_rx) = EventTap::channel(8);
        let (fanout_shutdown_tx, fanout_shutdown_rx) = watch::channel(false);
        let (_drain_tx, drain_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, fanout_shutdown_rx);

        let mut sub = open(&fanout, any_job_selector(), None, Some(drain_rx))
            .await
            .expect("subscribe");
        assert!(matches!(
            sub.items.recv().await,
            Some(EventStreamItem::Progress { .. })
        ));

        let _ = fanout_shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;

        let ended = tokio::time::timeout(Duration::from_secs(5), sub.items.recv())
            .await
            .expect("the stream must end when the fanout does");
        assert!(ended.is_none());
    }
}
