//! Read-side projection of accrual `projected_ready` bounds (ADR 0014/0027).
//!
//! `projected_ready` — the earliest time guaranteed releases fully fund an
//! accruing allocation — is *derived* state: it is a deterministic, pure
//! function of the replicated state (the node's accrual queue in `seq` order
//! plus the guaranteed release events of bounded running work). Rather than
//! replicate it, both the scheduler pass and the API read models recompute it
//! from the snapshot through the same functions below, so no surface can
//! drift from the scheduler's semantics:
//!
//! - only *genuinely guaranteed* releases count: a `Running` attempt with a
//!   recorded start on a job with an enforced `max_runtime` frees its funded
//!   capacity at `start + max_runtime` — anything else contributes nothing;
//! - pledges preserve commit order: freed capacity funds the accrual queue in
//!   ascending `seq`, exactly as apply's `pledge_node` does;
//! - where guaranteed releases run out, the bound stays indefinite (`None`):
//!   an unbounded projection is a real answer, never a fabricated time.
//!
//! The pass ([`crate::HeuristicScheduler`]) applies batch effects (opens,
//! moves, lends) on top of these same pass-start values; a read projection
//! deliberately stops at committed state — a proposal that has not landed
//! must not be shown as a start time.
//!
//! One deliberate divergence from the pass: the read projection covers
//! accruals queued on nodes absent from `state.nodes` (a hand-built state),
//! which the pass never walks. Harmless, and more honest for reads; the
//! drift only exists for states apply could not have built.
//!
//! No clocks, no I/O; pure over the snapshot. (The memoized entry point
//! times the sweep for metrics only — a side channel that never feeds a
//! decision.)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::id::{AllocationId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;
use coppice_state::{StateMachine, ViewMemos};

// ---- metrics (facade pattern; described via this crate root) -------------

/// Duration of one full `projected_starts` sweep, i.e. one miss on a view's
/// memo table. Steady state under accrual-detail polling is roughly one of
/// these per publish cadence per replica, so the histogram's rate × duration
/// is the number that says what the per-view memo buys (and what a future
/// index would save).
const PROJECTED_STARTS_SWEEP_SECONDS: &str = "scheduler_projected_starts_sweep_seconds";

/// Full sweeps computed. Divide by elapsed time to read the effective sweep
/// rate; the gap to the request rate is the memo's hit rate.
const PROJECTED_STARTS_SWEEPS_TOTAL: &str = "scheduler_projected_starts_sweeps_total";

/// Describe this crate's metrics (the workspace facade pattern; the
/// coordinator calls this once when it installs the recorder).
pub fn describe_metrics() {
    metrics::describe_histogram!(
        PROJECTED_STARTS_SWEEP_SECONDS,
        metrics::Unit::Seconds,
        "Duration of one full projected-starts sweep (a view-memo miss)."
    );
    metrics::describe_counter!(
        PROJECTED_STARTS_SWEEPS_TOTAL,
        metrics::Unit::Count,
        "Full projected-starts sweeps computed (view-memo misses)."
    );
}

/// A guaranteed future capacity release on a node (ADR 0014): at `at`, the
/// allocation with this `seq` (used only to break ties when two releases land
/// at the same instant, matching accrual funding order) returns `freed` to
/// the node's free pool.
#[derive(Debug, Clone)]
pub(crate) struct ReleaseEvent {
    pub(crate) at: Timestamp,
    pub(crate) seq: u64,
    pub(crate) freed: Resources,
}

/// Projected full-funding time for every accruing allocation in the snapshot.
///
/// The map carries each allocation in `state.accrual_queue` to the
/// `projected_ready` the scheduler's own sweep derives for it: the event time
/// its remaining need first reaches zero, or `None` when no guaranteed
/// release completes it. Allocations missing from the allocation map (a
/// hand-built state) are absent from the result rather than defaulted.
pub fn projected_starts(snapshot: &StateMachine) -> BTreeMap<AllocationId, Option<Timestamp>> {
    // Group the queue per node preserving `seq` order (the map key is
    // `(node, seq)`, so ascending iteration is funding order per node).
    let mut queues: BTreeMap<NodeId, Vec<AllocationId>> = BTreeMap::new();
    for ((node, _seq), alloc_id) in &snapshot.accrual_queue {
        queues.entry(*node).or_default().push(*alloc_id);
    }

    let mut events = collect_release_events(snapshot);
    let mut out = BTreeMap::new();
    for (node, allocs) in queues {
        let mut evs = events.remove(&node).unwrap_or_default();
        sort_release_events(&mut evs);
        // Keep each allocation id glued to its own remaining need through the
        // filter below, so a record missing from the allocation map (a
        // hand-built state) drops out of the result rather than shifting its
        // neighbours' projections onto the wrong ids.
        let queue: Vec<(AllocationId, Resources)> = allocs
            .into_iter()
            .filter_map(|id| {
                snapshot.allocations.get(&id).map(|rec| {
                    let need = rec
                        .allocation
                        .requested
                        .saturating_sub(&rec.allocation.funded);
                    (id, need)
                })
            })
            .collect();
        let needs: Vec<Resources> = queue.iter().map(|(_, need)| *need).collect();
        let ready = sweep_projected_ready(&evs, &needs);
        for ((alloc_id, _), t) in queue.into_iter().zip(ready) {
            out.insert(alloc_id, t);
        }
    }
    out
}

/// The memoized projection of one published view: a map from accrual
/// allocation id to its `projected_ready` bound (see [`projected_starts`]).
///
/// A dedicated newtype so the memo table's type key can only ever name this
/// projection.
#[derive(Debug, Default)]
pub struct ProjectedStarts(BTreeMap<AllocationId, Option<Timestamp>>);

impl std::ops::Deref for ProjectedStarts {
    type Target = BTreeMap<AllocationId, Option<Timestamp>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The projection behind a `projected_start` read view — computed once per
/// published view and shared by every read served from it.
///
/// The full sweep collects release events over the whole allocation map
/// (job-scaled, potentially millions of entries), so per-request
/// recomputation would put an O(all allocations) scan behind every accrued
/// job poll. Instead the caller passes the memo table of the published view
/// that owns `state` — the consensus layer's `StateView::memos`, carried
/// through the API's `ReadView`: the first read computes the projection,
/// concurrent and later reads of the same view share it, and a newer view
/// starts from an empty table so a memo can never outlive the state it
/// describes.
///
/// Pairing table and state is the caller's obligation: a memo computed from
/// one view's state must never be served against another's.
pub fn projected_starts_cached(state: &StateMachine, memos: &ViewMemos) -> Arc<ProjectedStarts> {
    // Warm path is a hash lookup plus an `Arc` clone; the first request for
    // a view pays the full sweep once, and a concurrent first request may
    // recompute (the memo computes outside its lock) but cannot skew the
    // answer — both sweeps read the same published state.
    memos.memo(|| {
        let started = Instant::now();
        let map = projected_starts(state);
        metrics::histogram!(PROJECTED_STARTS_SWEEP_SECONDS).record(started.elapsed().as_secs_f64());
        metrics::counter!(PROJECTED_STARTS_SWEEPS_TOTAL).increment(1);
        ProjectedStarts(map)
    })
}

/// Guaranteed release events per node: an allocation whose attempt is
/// `Running` with a start time, on a job with an enforced `max_runtime`,
/// releases its funded capacity at `start + max_runtime` (ADR 0014). Any
/// other live allocation has no guaranteed bound and contributes nothing.
/// Collected for every node — candidate bounds for opening or moving an
/// accrual sweep any eligible node (ADR 0027), not only current hosts.
pub(crate) fn collect_release_events(
    snapshot: &StateMachine,
) -> BTreeMap<NodeId, Vec<ReleaseEvent>> {
    let mut events: BTreeMap<NodeId, Vec<ReleaseEvent>> = BTreeMap::new();
    for rec in snapshot.allocations.values() {
        let node = rec.allocation.node;
        if rec.allocation.state == AllocationState::Released {
            continue;
        }
        let Some(at) = snapshot.attempts.get(&rec.allocation.attempt) else {
            continue;
        };
        if at.attempt.state != AttemptState::Running {
            continue;
        }
        let Some(started) = at.started_at else {
            continue;
        };
        let Some(job) = snapshot.jobs.get(&rec.allocation.job) else {
            continue;
        };
        let Some(runtime) = job.spec.max_runtime else {
            continue;
        };
        let release = started + runtime;
        events.entry(node).or_default().push(ReleaseEvent {
            at: release,
            seq: rec.seq,
            freed: rec.allocation.funded,
        });
    }
    events
}

/// Order release events for [`sweep_projected_ready`]: ascending time, ties
/// by allocation `seq`.
pub(crate) fn sort_release_events(events: &mut [ReleaseEvent]) {
    events.sort_unstable_by(|a, b| a.at.cmp(&b.at).then(a.seq.cmp(&b.seq)));
}

/// Sweep guaranteed release events to compute `projected_ready` per accrual
/// (ADR 0014): walk events in time order (pre-sorted by
/// [`sort_release_events`]), pool the freed capacity, and pledge it to the
/// accrual queue in `seq` order exactly as `pledge_node` would. An accrual's
/// `projected_ready` is the event time its remaining need first reaches zero,
/// or `None` if events run out.
pub(crate) fn sweep_projected_ready(
    events: &[ReleaseEvent],
    remaining: &[Resources],
) -> Vec<Option<Timestamp>> {
    let mut rem: Vec<Resources> = remaining.to_vec();
    let mut ready: Vec<Option<Timestamp>> = vec![None; remaining.len()];
    let mut pool = Resources::ZERO;
    for ev in events.iter() {
        pool = pool.saturating_add(&ev.freed);
        for (r, rd) in rem.iter_mut().zip(ready.iter_mut()) {
            if pool.is_zero() {
                break;
            }
            if r.is_zero() {
                continue;
            }
            let pledge = pool.component_min(r);
            pool = pool.saturating_sub(&pledge);
            *r = r.saturating_sub(&pledge);
            if r.is_zero() && rd.is_none() {
                *rd = Some(ev.at);
            }
        }
    }
    ready
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;
    use coppice_core::time::Duration;

    fn res(cpu: u64, mem: ByteSize, disk: ByteSize) -> Resources {
        Resources {
            cpu_millis: cpu,
            memory: mem,
            disk,
        }
    }

    /// The release instants the sweep tests use, as µs past the epoch — the
    /// absolute instant is irrelevant, only the order and the tie-breaks.
    fn at(micros: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_micros(micros)
    }

    #[test]
    fn projected_ready_is_the_event_that_completes_the_need() {
        // One accrual needing 16 cpu; a single release at t=100 frees 16.
        let events = vec![ReleaseEvent {
            at: at(100),
            seq: 0,
            freed: res(16_000, ByteSize::ZERO, ByteSize::ZERO),
        }];
        let ready = sweep_projected_ready(&events, &[res(16_000, ByteSize::ZERO, ByteSize::ZERO)]);
        assert_eq!(ready, vec![Some(at(100))]);
    }

    #[test]
    fn projected_ready_unbounded_when_events_fall_short() {
        // Needs 32 cpu but only 16 is ever guaranteed to free ⇒ unbounded.
        let events = vec![ReleaseEvent {
            at: at(100),
            seq: 0,
            freed: res(16_000, ByteSize::ZERO, ByteSize::ZERO),
        }];
        let ready = sweep_projected_ready(&events, &[res(32_000, ByteSize::ZERO, ByteSize::ZERO)]);
        assert_eq!(ready, vec![None]);
    }

    #[test]
    fn projected_ready_pledges_in_seq_order() {
        // Two releases (t=50, t=100) each free 16; head accrual (seq order)
        // completes at the first, the next at the second.
        let mut events = vec![
            ReleaseEvent {
                at: at(100),
                seq: 1,
                freed: res(16_000, ByteSize::ZERO, ByteSize::ZERO),
            },
            ReleaseEvent {
                at: at(50),
                seq: 0,
                freed: res(16_000, ByteSize::ZERO, ByteSize::ZERO),
            },
        ];
        sort_release_events(&mut events);
        let ready = sweep_projected_ready(
            &events,
            &[
                res(16_000, ByteSize::ZERO, ByteSize::ZERO),
                res(16_000, ByteSize::ZERO, ByteSize::ZERO),
            ],
        );
        assert_eq!(ready, vec![Some(at(50)), Some(at(100))]);
    }
}
