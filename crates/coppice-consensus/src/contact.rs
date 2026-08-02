//! Leader-side per-peer contact evidence (ADR 0037 §7).
//!
//! The evidence-gated removal / learner-GC rules need to know how long the
//! leader's replication to a peer has been *failing* — the input to
//! `removal_grace` (evidence-gated voter removal) and `learner_expiry`
//! (stale-learner GC). openraft 0.9 does not expose per-follower
//! acknowledgement recency (its `clock_progress` is crate-private and
//! `RaftMetrics` carries only matched log ids), and matched-index progress is
//! the wrong proxy: in a fully idle cluster no follower's matched index
//! advances, so a live-but-idle voter would be indistinguishable from a dead
//! one.
//!
//! Instead the evidence is recorded at this crate's own network seam. openraft
//! drives *all* leader→follower traffic — including the periodic heartbeats it
//! sends even when the log is idle — through `RaftNetwork::append_entries`, so
//! the [`GrpcRaftNetwork`](crate::net) client notes every send attempt and
//! every acknowledged round-trip here, and the membership decisions read "how
//! long has this peer gone unanswered while I was actually trying" from the
//! same tracker. A live-but-idle voter keeps acknowledging heartbeats and
//! therefore never qualifies as dead; a dead one accumulates attempts with no
//! acknowledgement.
//!
//! Any reply counts as contact — a conflict/rejection response is still a
//! reachable peer; only a transport-level failure is a non-answer.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::CoordinatorId;

/// The attempt-gap that starts a new evidence epoch.
///
/// A leader attempts contact with every replication target at least every
/// ~1.5s (300ms heartbeats; a down peer is retried after openraft's constant
/// 500ms backoff behind a 1s RPC timeout). A gap in attempts longer than this
/// therefore means the leader itself was not trying — it lost leadership, was
/// paused, or just started — and silence accrued across such a gap is not
/// evidence about the peer. The epoch resets so the grace clock restarts from
/// the moment attempts resume, and a read that lands inside such a gap voids
/// the failure window entirely rather than reporting stale evidence.
const EPOCH_GAP: Duration = Duration::from_secs(5);

/// One peer's contact state within the current evidence epoch.
#[derive(Debug, Clone, Copy)]
struct ContactState {
    /// When this epoch's first send attempt was made. The grace clock for a
    /// peer that has never acknowledged runs from here, so a peer the leader
    /// only just started trying to reach is never immediately condemned.
    epoch_start: Instant,
    /// The most recent send attempt; a gap beyond [`EPOCH_GAP`] resets the
    /// epoch.
    last_attempt: Instant,
    /// The most recent acknowledged round-trip in this epoch, if any.
    last_ack: Option<Instant>,
}

/// Shared per-peer contact recency, written by the Raft network client on
/// every AppendEntries round-trip and read by the membership decision paths
/// (ADR 0037 §7).
#[derive(Debug, Default)]
pub struct ContactTracker {
    peers: Mutex<HashMap<CoordinatorId, ContactState>>,
}

impl ContactTracker {
    /// Record that a send to `peer` is being attempted, now.
    pub fn note_attempt(&self, peer: CoordinatorId) {
        self.note_attempt_at(peer, Instant::now());
    }

    /// Record that `peer` answered an RPC (any reply, success or rejection),
    /// now.
    pub fn note_ack(&self, peer: CoordinatorId) {
        self.note_ack_at(peer, Instant::now());
    }

    /// The most recent acknowledged round-trip from `peer` in the current
    /// epoch, if any. `None` both when `peer` has never been attempted and
    /// when it has been attempted but never acknowledged (a genuinely dead or
    /// not-yet-reached peer) — the two are indistinguishable from this alone;
    /// callers that need to tell them apart use [`Self::failed_contact_for`].
    pub fn last_ack(&self, peer: CoordinatorId) -> Option<Instant> {
        let peers = self.peers.lock().expect("contact tracker poisoned");
        peers.get(&peer).and_then(|s| s.last_ack)
    }

    /// How long `peer` has gone without a successful acknowledgement while the
    /// leader was continuously attempting contact (ADR 0037 §7's
    /// `removal_grace` / `learner_expiry` input).
    ///
    /// `None` means the window carries no evidence at all, in any of three
    /// cases: `peer` has never been attempted; `peer` is currently
    /// acking — the most recent attempt has already been answered, so
    /// nothing is failing; or the epoch-gap rule voids the window because
    /// `now` itself is more than [`EPOCH_GAP`] past the last attempt, i.e.
    /// this node is not (or is no longer) actively trying to reach `peer`, so
    /// silence since then is not evidence about the peer.
    pub fn failed_contact_for(&self, peer: CoordinatorId, now: Instant) -> Option<Duration> {
        let peers = self.peers.lock().expect("contact tracker poisoned");
        let state = peers.get(&peer)?;

        // Epoch-gap rule, re-checked at read time: if this node has not
        // attempted `peer` recently, it cannot testify to the peer's
        // liveness either way.
        if now.saturating_duration_since(state.last_attempt) > EPOCH_GAP {
            return None;
        }

        // "Currently acking": the most recent attempt has already been
        // answered (the ack is at least as recent as the attempt), so there
        // is no unanswered round to measure a failure against.
        if state.last_ack.is_some_and(|ack| ack >= state.last_attempt) {
            return None;
        }

        let last_seen = state.last_ack.unwrap_or(state.epoch_start);
        Some(now.saturating_duration_since(last_seen))
    }

    /// Whether `peer` has proven reachable within `staleness_bound` — the
    /// "live majority from the leader's vantage" postcondition (ADR 0037 §7).
    ///
    /// Life is proven **only by a successful acknowledgement**: an attempt is
    /// this node talking, not the peer answering, and a fresh epoch's
    /// `epoch_start` is merely when we started asking. Counting either would
    /// let a dead-but-recently-attempted peer inflate the leader's quorum
    /// view for a window — exactly the inflation the postcondition exists to
    /// prevent. The asymmetry with [`Self::failed_contact_for`] is
    /// deliberate: *removal* evidence errs toward "reachable until proven
    /// otherwise" (measuring failure from `epoch_start`), while *liveness*
    /// errs toward "dead until it answers".
    pub fn is_live(&self, peer: CoordinatorId, staleness_bound: Duration) -> bool {
        self.is_live_at(peer, staleness_bound, Instant::now())
    }

    fn is_live_at(&self, peer: CoordinatorId, staleness_bound: Duration, now: Instant) -> bool {
        let peers = self.peers.lock().expect("contact tracker poisoned");
        peers.get(&peer).is_some_and(|state| {
            state
                .last_ack
                .is_some_and(|ack| now.saturating_duration_since(ack) < staleness_bound)
        })
    }

    fn note_attempt_at(&self, peer: CoordinatorId, now: Instant) {
        let mut peers = self.peers.lock().expect("contact tracker poisoned");
        let entry = peers.entry(peer).or_insert(ContactState {
            epoch_start: now,
            last_attempt: now,
            last_ack: None,
        });
        if now.saturating_duration_since(entry.last_attempt) > EPOCH_GAP {
            *entry = ContactState {
                epoch_start: now,
                last_attempt: now,
                last_ack: None,
            };
        } else {
            entry.last_attempt = now;
        }
    }

    fn note_ack_at(&self, peer: CoordinatorId, now: Instant) {
        let mut peers = self.peers.lock().expect("contact tracker poisoned");
        // An ack without a recorded attempt (races with an epoch reset are
        // benign but possible) still counts as contact.
        let entry = peers.entry(peer).or_insert(ContactState {
            epoch_start: now,
            last_attempt: now,
            last_ack: None,
        });
        entry.last_ack = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_attempted_peer_has_no_evidence() {
        let tracker = ContactTracker::default();
        let now = Instant::now();
        assert_eq!(tracker.failed_contact_for(1, now), None);
        assert_eq!(tracker.last_ack(1), None);
        assert!(!tracker.is_live_at(1, Duration::from_secs(60), now));
    }

    #[test]
    fn unacknowledged_peer_accrues_a_failure_window_from_epoch_start() {
        // A peer that never answers: evidence runs from the epoch's first
        // attempt, so grace runs from when the leader started trying, not
        // from process start.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        tracker.note_attempt_at(1, t0 + Duration::from_secs(1));
        tracker.note_attempt_at(1, t0 + Duration::from_secs(2));
        assert_eq!(
            tracker.failed_contact_for(1, t0 + Duration::from_secs(2)),
            Some(Duration::from_secs(2))
        );
        assert_eq!(tracker.last_ack(1), None);
    }

    #[test]
    fn acknowledgement_clears_the_failure_window() {
        // A live-but-idle peer keeps acking heartbeats: the most recent
        // attempt is already answered, so there is nothing failing.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        tracker.note_ack_at(1, t0);
        let t1 = t0 + Duration::from_secs(60);
        tracker.note_attempt_at(1, t1);
        tracker.note_ack_at(1, t1);
        assert_eq!(tracker.failed_contact_for(1, t1), None);
        assert_eq!(tracker.last_ack(1), Some(t1));
        assert!(tracker.is_live_at(1, Duration::from_secs(1), t1));
    }

    #[test]
    fn ack_then_silence_measures_from_the_last_ack() {
        // The peer acked once, then stopped answering while the leader kept
        // attempting: the failure window measures from the last real
        // evidence of life, the ack — not from the epoch start.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        tracker.note_ack_at(1, t0);
        let mut t = t0;
        for _ in 0..10 {
            t += Duration::from_secs(1);
            tracker.note_attempt_at(1, t);
        }
        assert_eq!(
            tracker.failed_contact_for(1, t),
            Some(Duration::from_secs(10))
        );
        assert!(!tracker.is_live_at(1, Duration::from_secs(5), t));
    }

    #[test]
    fn attempt_gap_starts_a_fresh_epoch() {
        // The leader stopped attempting (lost leadership, paused) and resumed:
        // pre-gap silence must not count, so the evidence clock restarts at
        // the resumed attempt and the old ack is discarded.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        tracker.note_ack_at(1, t0);
        let resumed = t0 + EPOCH_GAP + Duration::from_secs(1);
        tracker.note_attempt_at(1, resumed);
        assert_eq!(tracker.failed_contact_for(1, resumed), Some(Duration::ZERO));
        assert_eq!(tracker.last_ack(1), None);
    }

    #[test]
    fn a_read_that_lands_inside_an_unattempted_gap_is_voided() {
        // Even without a fresh `note_attempt` call, a query landing more than
        // `EPOCH_GAP` past the last attempt must not report the peer as
        // failing: this node itself has stopped trying (e.g. it lost
        // leadership), and silence it never probed for is not evidence.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        let stale_read = t0 + EPOCH_GAP + Duration::from_secs(1);
        assert_eq!(tracker.failed_contact_for(1, stale_read), None);
    }

    #[test]
    fn continuous_attempts_keep_the_epoch() {
        // Steady attempts (each within EPOCH_GAP of the last) never reset the
        // epoch, however long the total span — that is exactly the
        // failing-peer case the grace period must be allowed to expire
        // against.
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        for i in 0..30 {
            tracker.note_attempt_at(1, t0 + Duration::from_secs(i * 2));
        }
        let last = t0 + Duration::from_secs(29 * 2);
        assert_eq!(tracker.failed_contact_for(1, last), Some(last - t0));
    }

    #[test]
    fn is_live_reflects_the_staleness_bound() {
        let tracker = ContactTracker::default();
        let t0 = Instant::now();
        tracker.note_attempt_at(1, t0);
        tracker.note_ack_at(1, t0);
        assert!(tracker.is_live_at(1, Duration::from_secs(30), t0 + Duration::from_secs(10)));
        assert!(!tracker.is_live_at(1, Duration::from_secs(5), t0 + Duration::from_secs(10)));
    }
}
