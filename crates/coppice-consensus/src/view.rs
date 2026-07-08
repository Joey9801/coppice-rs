//! Published read views of applied state.
//!
//! The apply task is the single writer of [`coppice_state::StateMachine`]. It
//! never lends that mutable state out; instead it publishes immutable
//! [`StateView`]s through a [`tokio::sync::watch`] channel. Every other
//! subsystem — the API read path, the scheduler, the event fanout — reads
//! views, so there is exactly one owner of the mutable state and no locks on
//! the read path (`docs/architecture/coordinator-runtime.md`).
//!
//! Publishing clones the whole [`coppice_state::StateMachine`]; that full-state
//! clone is the price of a publish, and the cadence bound in
//! [`ViewPublisherConfig`] caps the clone rate. When that cost bites, the
//! escape hatch is persistent (`im`-style) maps inside the state machine — see
//! the runtime doc. Wall-clock time drives the cadence here, which is safe:
//! publishing is outside replicated state, so it may read the clock freely
//! (unlike apply, which must be a pure function of the command log).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};

use coppice_state::StateMachine;

use crate::error::ConsensusError;

/// An immutable snapshot of applied control-plane state at a known log index.
///
/// Cheap to clone: it is an [`Arc`] to the state plus a cursor. Carries **both**
/// coordinates that the rest of the system must keep distinct — the Raft
/// applied [`log index`](StateView::applied_index) and the state machine's
/// [`version`](StateView::version).
#[derive(Clone)]
pub struct StateView {
    state: Arc<StateMachine>,
    applied_index: u64,
}

impl StateView {
    /// The applied state.
    ///
    /// Borrowed, not owned: a view lends read access, never a handle that
    /// could mutate the single-writer state.
    pub fn state(&self) -> &StateMachine {
        &self.state
    }

    /// The Raft applied **log index** this view reflects — the read/event
    /// cursor of ADR 0007/0008. Compare against a [`Consensus::read_index`](crate::Consensus::read_index)
    /// barrier for strong reads.
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// The state machine's `version` — the count of applied commands, used
    /// by the scheduler as `expected_version`.
    ///
    /// Distinct from [`applied_index`](StateView::applied_index): the log
    /// index counts *log entries* (including ones this replica has yet to
    /// publish), the version counts *applied commands*. Never substitute one
    /// for the other.
    pub fn version(&self) -> u64 {
        self.state.version
    }
}

/// Shared demand signal: the highest applied index a reader is currently
/// blocked waiting for, plus a wakeup the publisher parks on while idle.
struct ViewDemand {
    /// Monotonic high-water mark of outstanding [`StateViews::at_least`]
    /// requests. `fetch_max` only ever raises it; the publisher reads it to
    /// decide whether an early publish is warranted.
    requested: AtomicU64,
    /// Wakes the apply loop's idle wait so it publishes for a new demand
    /// without waiting out the cadence tick.
    notify: Notify,
}

/// Reader-side handle to the published views.
///
/// Clone freely; every clone reads the same [`watch`] channel and shares
/// the same demand signal.
#[derive(Clone)]
pub struct StateViews {
    rx: watch::Receiver<StateView>,
    demand: Arc<ViewDemand>,
}

impl StateViews {
    /// The most recently published view, without waiting.
    pub fn latest(&self) -> StateView {
        self.rx.borrow().clone()
    }

    /// Await a view whose applied index is at least `index` — the read side of
    /// a strong read (pair with [`Consensus::read_index`](crate::Consensus::read_index)).
    ///
    /// Records the demand so the publisher can publish early (its normal
    /// cadence would otherwise delay a fresh strong read by up to
    /// [`ViewPublisherConfig::cadence`]), pokes the publisher's idle wait via
    /// `notify_one` — which stores a permit even if the publisher is not yet
    /// parked, so the wakeup is never lost to a race — then waits for a publish
    /// that satisfies the demand. Returns [`ConsensusError::Shutdown`] if the
    /// publisher (apply task) has gone away.
    pub async fn at_least(&self, index: u64) -> Result<StateView, ConsensusError> {
        self.demand.requested.fetch_max(index, Ordering::Relaxed);
        self.demand.notify.notify_one();

        let mut rx = self.rx.clone();
        loop {
            {
                let view = rx.borrow_and_update();
                if view.applied_index >= index {
                    return Ok(view.clone());
                }
            }
            if rx.changed().await.is_err() {
                return Err(ConsensusError::Shutdown);
            }
        }
    }
}

/// Tuning for [`ViewPublisher`]: how often applied state is republished.
#[derive(Debug, Clone)]
pub struct ViewPublisherConfig {
    /// Minimum spacing between routine publishes while state keeps changing.
    /// Bounds the full-state clone rate.
    pub cadence: Duration,
    /// Minimum spacing between demand-driven early publishes, so a burst of
    /// strong reads cannot force a publish per apply batch.
    pub demand_spacing: Duration,
}

impl Default for ViewPublisherConfig {
    fn default() -> Self {
        ViewPublisherConfig {
            cadence: Duration::from_millis(100),
            demand_spacing: Duration::from_millis(10),
        }
    }
}

/// Apply-task-side half of the view channel: the single owner that publishes
/// [`StateView`]s.
///
/// All the publish methods are synchronous and NEVER await — they run inside
/// the apply loop, which must not block on the read path. The one async method
/// is [`ViewPublisher::idle_wakeup`], which the apply loop *selects on* while it
/// has nothing to apply, so it wakes to publish for outstanding demand. The
/// cadence tick itself lives in the apply loop (a `tokio::time::interval`), not
/// here — this half only records timestamps and decides whether to publish.
pub struct ViewPublisher {
    tx: watch::Sender<StateView>,
    demand: Arc<ViewDemand>,
    config: ViewPublisherConfig,
    last_published: Option<Instant>,
    published_index: u64,
}

impl ViewPublisher {
    /// Create a publisher seeded with `initial` state (at applied index 0) and
    /// the matching reader handle. The apply task republishes the true index
    /// with [`ViewPublisher::publish_now`] once it has replayed the log.
    pub fn new(initial: StateMachine, config: ViewPublisherConfig) -> (ViewPublisher, StateViews) {
        let view = StateView {
            state: Arc::new(initial),
            applied_index: 0,
        };
        let (tx, rx) = watch::channel(view);
        let demand = Arc::new(ViewDemand {
            requested: AtomicU64::new(0),
            notify: Notify::new(),
        });
        let publisher = ViewPublisher {
            tx,
            demand: Arc::clone(&demand),
            config,
            last_published: None,
            published_index: 0,
        };
        let views = StateViews { rx, demand };
        (publisher, views)
    }

    /// Publish a fresh view if it is warranted: there is unpublished state and
    /// either the cadence has elapsed, or an outstanding demand names a
    /// not-yet-published index and the demand spacing has elapsed. A no-op
    /// otherwise, so the apply loop can call it after every batch cheaply.
    pub fn maybe_publish(&mut self, state: &StateMachine, applied_index: u64) {
        if applied_index <= self.published_index {
            return;
        }
        let now = Instant::now();
        let elapsed_since = |since: Duration| match self.last_published {
            None => true,
            Some(last) => now.duration_since(last) >= since,
        };

        let cadence_due = elapsed_since(self.config.cadence);
        let demand_due = self.demand.requested.load(Ordering::Relaxed) > self.published_index
            && elapsed_since(self.config.demand_spacing);

        if cadence_due || demand_due {
            self.publish_at(state, applied_index, now);
        }
    }

    /// Publish unconditionally.
    ///
    /// Used for the first post-replay publish and for snapshot handoff,
    /// where the reader must see the exact index regardless of cadence.
    pub fn publish_now(&mut self, state: &StateMachine, applied_index: u64) {
        self.publish_at(state, applied_index, Instant::now());
    }

    fn publish_at(&mut self, state: &StateMachine, applied_index: u64, now: Instant) {
        let view = StateView {
            state: Arc::new(state.clone()),
            applied_index,
        };
        self.published_index = applied_index;
        self.last_published = Some(now);
        // Fails only when every reader has dropped; nothing to publish to, so
        // the drop is the correct outcome.
        let _ = self.tx.send(view);
    }

    /// Awaited by the apply loop while idle: completes when a reader records
    /// demand via [`StateViews::at_least`]. Pairs with that method's
    /// `notify_one`, so a demand raised just before the loop parks still wakes
    /// it (the stored permit is consumed on the next await).
    pub async fn idle_wakeup(&self) {
        self.demand.notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn at_least_resolves_after_publish() {
        let (mut publisher, views) = ViewPublisher::new(StateMachine::default(), ViewPublisherConfig::default());

        let waiter = tokio::spawn(async move { views.at_least(5).await });

        // Let the waiter register its demand and park on `changed()`.
        tokio::task::yield_now().await;

        let state = StateMachine {
            version: 42,
            ..StateMachine::default()
        };
        publisher.publish_now(&state, 5);

        let view = waiter.await.expect("join").expect("view");
        assert_eq!(view.applied_index(), 5);
        assert_eq!(view.version(), 42);
    }

    #[tokio::test]
    async fn idle_wakeup_fires_on_demand() {
        let (publisher, views) = ViewPublisher::new(StateMachine::default(), ViewPublisherConfig::default());

        let woke = tokio::spawn(async move {
            publisher.idle_wakeup().await;
            true
        });

        // Recording demand must wake the parked publisher.
        tokio::spawn(async move {
            let _ = views.at_least(1).await;
        });

        let result = tokio::time::timeout(Duration::from_secs(1), woke).await;
        assert!(matches!(result, Ok(Ok(true))));
    }
}
