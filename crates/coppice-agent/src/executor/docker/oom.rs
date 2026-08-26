//! The OOM-event witness (docker-executor.md §4).
//!
//! Docker's `State.OOMKilled` flag is *not* the daemon's only OOM signal, and
//! it is not the earliest one: the daemon publishes a container `oom` event
//! from the same handler that commits the flag, and that event reaches a live
//! `docker events` tail **before** the container's `die` event. Measured on a
//! healthy daemon (29.5, cgroup v2), `oom` precedes `die` by ~80 ms — so an
//! executor that tails events already holds the OOM proof by the time it
//! learns the container exited, while an executor that only re-inspects has to
//! wait for a flag commit it may never see.
//!
//! This module is that push channel. The events task records every `oom` event
//! against its allocation here; [`super::settle_oom_flag`] consults the
//! registry first (free, and usually already positive) and otherwise *waits on
//! it* rather than only polling `inspect`.
//!
//! Scope discipline: a witness is consulted only where the inspect already has
//! the racy shape ([`super::classify::oom_flag_may_lag`] — SIGKILL exit under
//! an explicit memory limit with the flag unset). That keeps its semantics
//! exactly those of the flag it stands in for. Docker sets `OOMKilled`, and
//! emits `oom`, when the cgroup OOM-kills *any* process in the container —
//! including one the container survived — so neither signal on its own proves
//! the container died of memory. Gating both on the racy shape is what makes
//! them evidence.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::Notify;

use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};

/// How long a witness is retained when nothing reaps it. A witness is normally
/// dropped when its allocation is reaped; this bounds the registry against
/// containers that vanish some other way (an external `docker rm`, a start
/// that never reached us). Far longer than any settle window, so a retained
/// entry is never the reason a settle misses.
const WITNESS_TTL: Duration = Duration::from_secs(600);

/// What a bounded OOM settle concluded about one exit.
///
/// The three arms are exhaustive over the racy shape: either it was never in
/// question, or the daemon confirmed the kill (flag or `oom` event), or the
/// window closed with no confirmation either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OomVerdict {
    /// The inspect never had the racy shape — a plain exit, an exit under no
    /// memory limit, or a flag already committed. Nothing was settled and the
    /// evidence stands as `exit_info` extracted it.
    NotInQuestion,
    /// The daemon confirmed a cgroup OOM kill, through the committed
    /// `OOMKilled` flag or a witnessed `oom` event.
    Confirmed,
    /// PID 1 died to SIGKILL under a memory limit and the window closed with
    /// no OOM confirmation. **Not** "not an OOM": an external SIGKILL of a
    /// memory-limited container and an OOM whose daemon-side notification was
    /// lost are indistinguishable from here, which is exactly why this is its
    /// own answer rather than either guess.
    Unconfirmed,
}

/// Container `oom` events seen on the live tail, keyed by allocation.
///
/// Keyed by allocation rather than container id because that is the one
/// identifier every settle call site holds (`stop` addresses its container by
/// name, the events paths by id), and allocation ids are never reused.
#[derive(Debug, Default)]
pub(crate) struct OomWitness {
    seen: Mutex<HashMap<AllocationId, Timestamp>>,
    /// Allocations a settle already gave up on. Bounds the cost of the widened
    /// window: an unconfirmable exit is inspected more than once (the die
    /// event, then a stop's pre-inspect, then `observe`), and only the first of
    /// those needs to wait — see [`note_unconfirmed`](Self::note_unconfirmed).
    unconfirmed: Mutex<HashMap<AllocationId, Timestamp>>,
    /// Woken on every record, so a settle blocked on this allocation re-checks
    /// immediately instead of waiting out its next re-inspect delay.
    notify: Notify,
}

impl OomWitness {
    /// Record an `oom` event for `allocation` and wake every waiting settle.
    /// Idempotent — the first sighting's timestamp is kept.
    pub(crate) fn record(&self, allocation: AllocationId, now: Timestamp) {
        {
            let mut seen = self.lock();
            seen.retain(|_, at| now - *at < WITNESS_TTL);
            seen.entry(allocation).or_insert(now);
        }
        // Outside the lock: waiters re-check under it.
        self.notify.notify_waiters();
    }

    /// Whether an `oom` event has been seen for `allocation`.
    pub(crate) fn witnessed(&self, allocation: AllocationId) -> bool {
        self.lock().contains_key(&allocation)
    }

    /// Remember that a settle for `allocation` closed without confirmation, so
    /// the next evidence path for the same exit answers instantly.
    ///
    /// Safe to short-circuit on because it can only ever repeat the same
    /// answer: a settle consults this *after* the two channels that could
    /// change it. If the flag commits late, the next inspect no longer has the
    /// racy shape and never reaches the memo; if a late `oom` event lands, the
    /// witness check runs first. The memo only fires where both are still
    /// silent — exactly the state that produced the original verdict.
    pub(crate) fn note_unconfirmed(&self, allocation: AllocationId, now: Timestamp) {
        let mut unconfirmed = Self::guard(&self.unconfirmed);
        unconfirmed.retain(|_, at| now - *at < WITNESS_TTL);
        unconfirmed.entry(allocation).or_insert(now);
    }

    /// Whether a settle for `allocation` already gave up.
    pub(crate) fn is_unconfirmed(&self, allocation: AllocationId) -> bool {
        Self::guard(&self.unconfirmed).contains_key(&allocation)
    }

    /// Drop everything remembered about `allocation`. Called from reap: past
    /// that point no evidence path can ask about it again.
    pub(crate) fn forget(&self, allocation: AllocationId) {
        self.lock().remove(&allocation);
        Self::guard(&self.unconfirmed).remove(&allocation);
    }

    /// Resolve as soon as an `oom` event has been witnessed for `allocation`,
    /// or at `deadline`; `true` iff it was witnessed.
    ///
    /// The registration-before-check ordering ([`Notified::enable`]) is what
    /// makes this lossless: a `record` landing between the check and the await
    /// still wakes this waiter.
    pub(crate) async fn witnessed_by(
        &self,
        allocation: AllocationId,
        deadline: tokio::time::Instant,
    ) -> bool {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.witnessed(allocation) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<AllocationId, Timestamp>> {
        Self::guard(&self.seen)
    }

    fn guard(
        map: &Mutex<HashMap<AllocationId, Timestamp>>,
    ) -> std::sync::MutexGuard<'_, HashMap<AllocationId, Timestamp>> {
        map.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc() -> AllocationId {
        AllocationId::new()
    }

    #[test]
    fn record_is_idempotent_and_first_sighting_wins() {
        let w = OomWitness::default();
        let a = alloc();
        let t0 = Timestamp::now();
        w.record(a, t0);
        w.record(a, t0 + Duration::from_secs(1));
        assert!(w.witnessed(a));
        assert_eq!(w.lock().get(&a).copied(), Some(t0));
    }

    #[test]
    fn unrelated_allocations_are_not_witnessed() {
        let w = OomWitness::default();
        w.record(alloc(), Timestamp::now());
        assert!(!w.witnessed(alloc()));
    }

    #[test]
    fn forget_drops_both_the_witness_and_the_give_up_memo() {
        let w = OomWitness::default();
        let a = alloc();
        w.record(a, Timestamp::now());
        w.note_unconfirmed(a, Timestamp::now());
        w.forget(a);
        assert!(!w.witnessed(a));
        assert!(!w.is_unconfirmed(a));
    }

    #[test]
    fn the_give_up_memo_is_per_allocation_and_pruned() {
        let w = OomWitness::default();
        let (old, fresh) = (alloc(), alloc());
        let t0 = Timestamp::now();
        w.note_unconfirmed(old, t0);
        assert!(w.is_unconfirmed(old));
        assert!(!w.is_unconfirmed(fresh));
        w.note_unconfirmed(fresh, t0 + WITNESS_TTL + Duration::from_secs(1));
        assert!(!w.is_unconfirmed(old), "a memo past its TTL must be pruned");
        assert!(w.is_unconfirmed(fresh));
    }

    #[test]
    fn stale_witnesses_are_pruned_on_record() {
        let w = OomWitness::default();
        let (old, fresh) = (alloc(), alloc());
        let t0 = Timestamp::now();
        w.record(old, t0);
        w.record(fresh, t0 + WITNESS_TTL + Duration::from_secs(1));
        assert!(!w.witnessed(old), "a witness past its TTL must be pruned");
        assert!(w.witnessed(fresh));
    }

    #[tokio::test]
    async fn witnessed_by_returns_immediately_when_already_seen() {
        let w = OomWitness::default();
        let a = alloc();
        w.record(a, Timestamp::now());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        // Already recorded: resolves without consuming any of the deadline.
        assert!(w.witnessed_by(a, deadline).await);
    }

    #[tokio::test]
    async fn witnessed_by_wakes_on_a_later_record() {
        let w = std::sync::Arc::new(OomWitness::default());
        let a = alloc();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let waiter = {
            let w = std::sync::Arc::clone(&w);
            tokio::spawn(async move { w.witnessed_by(a, deadline).await })
        };
        // Let the waiter register before the record lands.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        w.record(a, Timestamp::now());
        assert!(waiter.await.expect("waiter did not panic"));
    }

    #[tokio::test]
    async fn witnessed_by_gives_up_at_the_deadline() {
        let w = OomWitness::default();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
        assert!(!w.witnessed_by(alloc(), deadline).await);
    }
}
