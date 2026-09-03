//! Read-side projection tests for [`coppice_scheduler::projected_starts`]
//! (ADR 0014/0027): the `projected_ready` bound every accruing allocation
//! carries into the job/node read views.
//!
//! The bound is a pure function of committed state, so these tests drive
//! real apply-built states (for the guaranteed releases) and, where the
//! scheduler's own rules would forbid the shape (two accruals queued on one
//! node, ADR 0027 rule 5), hand-insert the accrual records the read model
//! must still project honestly. The single-accrual cases also assert the
//! projection against the bound the scheduling pass itself used, which is
//! the same sweep by construction.

mod common;

use common::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use coppice_core::allocation::{Allocation, AllocationState};
use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::quota::PriorityMultiplier;
use coppice_core::resource::Resources;
use coppice_core::time::Duration;
use coppice_scheduler::{
    projected_starts, projected_starts_cached, HeuristicScheduler, PlacementProposal,
    ProjectedStarts, Scheduler,
};
use coppice_state::ViewMemos;
use coppice_state::{AllocationRecord, Command, StateMachine};

fn res(cpu_millis: u64, memory: ByteSize, disk: ByteSize) -> Resources {
    Resources {
        cpu_millis,
        memory,
        disk,
    }
}

/// Submit, place, dispatch, and start a job on `nid(node)`, all at `TS`.
/// `max_runtime_s = None` makes the hold unbounded (no guaranteed release).
fn seed_running(
    sm: &mut StateMachine,
    n: u128,
    node: u128,
    requests: Resources,
    max_runtime_s: Option<i64>,
) {
    apply_ok(
        sm,
        submit_cmd(
            jid(n),
            requests,
            max_runtime_s,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        sm,
        place_cmd(
            placement(jid(n), aid(n), alid(n), nid(node), requests),
            base_ts(),
        ),
    );
    apply_ok(sm, dispatch_cmd(aid(n), base_ts()));
    apply_ok(sm, started_cmd(aid(n), base_ts()));
}

/// Hand-insert an accruing allocation for `job` on `node` with the given
/// funding and commit-order `seq`, queued on the node's accrual queue.
fn seed_accrual(
    sm: &mut StateMachine,
    alloc: AllocationId,
    job: coppice_core::id::JobId,
    node: coppice_core::id::NodeId,
    requested: Resources,
    funded: Resources,
    seq: u64,
) {
    sm.allocations.insert(
        alloc,
        AllocationRecord {
            allocation: Allocation {
                id: alloc,
                job,
                attempt: aid(0),
                node,
                requested,
                funded,
                state: AllocationState::Accruing,
            },
            seq,
        },
    );
    sm.accrual_queue.insert((node, seq), alloc);
}

/// Schedule one queued job and commit the proposal, returning the minted
/// (attempt, allocation) ids per placement.
fn commit_proposal(sm: &mut StateMachine, proposal: &PlacementProposal) {
    let mut minted = Vec::new();
    let mut mint = minter();
    let cmd = proposal.to_commit_placements(&mut || {
        let ids = mint();
        minted.push(ids);
        ids
    });
    sm.apply(&Command::CommitPlacements(cmd))
        .expect("the batch applies");
}

// ---- one accrual, finite bound ----

/// A bounded runner holds 16 of 32 cpu; a 32-cpu whale accrues behind it.
/// The scheduler opens the whale's accrual against the guaranteed release at
/// `TS + 3600 s`, and the read projection must report exactly that instant.
#[test]
fn one_accrual_with_a_sufficient_bounded_release_projects_the_release_time() {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(2),
            cpu(32_000),
            None,
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );

    let proposal = HeuristicScheduler::default().schedule(&sm, ts(TS_US + 50));
    assert_eq!(proposal.placements.len(), 1);
    assert!(
        !proposal.placements[0].expect_funded,
        "the whale accrues behind the bounded runner"
    );
    commit_proposal(&mut sm, &proposal);

    let whale_alloc = sm
        .accrual_queue
        .values()
        .next()
        .copied()
        .expect("the whale holds an accrual");
    let starts = projected_starts(&sm);
    assert_eq!(
        starts.get(&whale_alloc),
        Some(&Some(base_ts() + Duration::from_secs(3600))),
        "the projection is the same bound the pass opened the accrual against"
    );
}

// ---- one accrual, insufficient releases ----

/// The guaranteed releases never cover the remaining need: the bound stays
/// indefinite — a real `None`, never a fabricated time.
#[test]
fn one_accrual_with_insufficient_releases_stays_unbounded() {
    let mut sm = setup(cpu(64_000), 4);
    // 16 cpu frees at TS + 3600; 16 more never does (unbounded runner).
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));
    seed_running(&mut sm, 2, 1, cpu(16_000), None);
    // Remaining need: 64 − 32 funded = 32 cpu, but only 16 is ever guaranteed.
    seed_accrual(
        &mut sm,
        alid(50),
        jid(3),
        nid(1),
        cpu(64_000),
        cpu(32_000),
        1,
    );

    let starts = projected_starts(&sm);
    assert_eq!(starts.get(&alid(50)), Some(&None));
}

// ---- multiple accruals: commit-order pledges ----

/// Two accruals queued behind one bounded release stream are funded in `seq`
/// order: the head completes on the first release, the tail — which the head
/// did not starve — on the second.
#[test]
fn multiple_accruals_pledge_in_commit_order() {
    let mut sm = setup(cpu(64_000), 4);
    // Two bounded runners, each releasing 16 cpu: at TS + 3600 and TS + 7200.
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(9),
            cpu(16_000),
            Some(7200),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(9), aid(9), alid(9), nid(1), cpu(16_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(9), base_ts()));
    apply_ok(&mut sm, started_cmd(aid(9), base_ts()));

    // Head accrual (seq 1) needs 16 more; tail (seq 2) needs 16 more. Each
    // release covers exactly one pledge step, and the tail only sees the
    // second because the head is ahead of it in commit order.
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(32_000),
        cpu(16_000),
        1,
    );
    seed_accrual(
        &mut sm,
        alid(51),
        jid(3),
        nid(1),
        cpu(32_000),
        cpu(16_000),
        2,
    );

    let starts = projected_starts(&sm);
    assert_eq!(
        starts.get(&alid(50)),
        Some(&Some(base_ts() + Duration::from_secs(3600))),
        "the head accrual completes on the first release"
    );
    assert_eq!(
        starts.get(&alid(51)),
        Some(&Some(base_ts() + Duration::from_secs(7200))),
        "the tail accrual waits for the head's pledge, then the second release"
    );
}

// ---- mixed resource dimensions ----

/// A release only counts toward the dimensions it actually frees: a cpu-only
/// release cannot complete a need that also owes memory.
#[test]
fn mixed_dimensions_fund_per_dimension() {
    // Runner holds 16 cpu + 4 GiB, bounded: both free at TS + 3600.
    let mut sm = setup(res(32_000, ByteSize::from_gib(16), ByteSize::ZERO), 4);
    seed_running(
        &mut sm,
        1,
        1,
        res(16_000, ByteSize::from_gib(4), ByteSize::ZERO),
        Some(3600),
    );

    // Accrual funded to half on both dims: the release covers both
    // remainders exactly ⇒ finite.
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        res(32_000, ByteSize::from_gib(8), ByteSize::ZERO),
        res(16_000, ByteSize::from_gib(4), ByteSize::ZERO),
        1,
    );
    let starts = projected_starts(&sm);
    assert_eq!(
        starts.get(&alid(50)),
        Some(&Some(base_ts() + Duration::from_secs(3600)))
    );

    // Same shape, but the runner only ever held cpu: the memory remainder is
    // never guaranteed ⇒ unbounded even though the cpu side completes.
    let mut sm = setup(res(32_000, ByteSize::from_gib(16), ByteSize::ZERO), 4);
    seed_running(
        &mut sm,
        1,
        1,
        res(16_000, ByteSize::ZERO, ByteSize::ZERO),
        Some(3600),
    );
    seed_accrual(
        &mut sm,
        alid(51),
        jid(2),
        nid(1),
        res(32_000, ByteSize::from_gib(8), ByteSize::ZERO),
        res(16_000, ByteSize::from_gib(4), ByteSize::ZERO),
        1,
    );
    let starts = projected_starts(&sm);
    assert_eq!(starts.get(&alid(51)), Some(&None));
}

// ---- unbounded runners ----

/// Capacity held by a job without an enforced `max_runtime` is never
/// guaranteed to free, so it contributes no release event: an accrual behind
/// it stays unbounded no matter how large the hold is.
#[test]
fn unbounded_runners_never_bound_the_projection() {
    let mut sm = setup(cpu(64_000), 4);
    seed_running(&mut sm, 1, 1, cpu(32_000), None);
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(64_000),
        cpu(32_000),
        1,
    );

    let starts = projected_starts(&sm);
    assert_eq!(starts.get(&alid(50)), Some(&None));
}

/// A `Running` attempt with no recorded start is not a guaranteed release
/// either (its runtime bound cannot be anchored), so the projection ignores
/// it — matching the pass's own event collection.
#[test]
fn running_without_a_start_time_contributes_no_release() {
    let mut sm = setup(cpu(64_000), 4);
    // Dispatched but never started: the attempt is Running in no state this
    // projection trusts, so no release event exists.
    apply_ok(
        &mut sm,
        submit_cmd(
            jid(1),
            cpu(32_000),
            Some(3600),
            PriorityMultiplier::ONE,
            base_ts(),
        ),
    );
    apply_ok(
        &mut sm,
        place_cmd(
            placement(jid(1), aid(1), alid(1), nid(1), cpu(32_000)),
            base_ts(),
        ),
    );
    apply_ok(&mut sm, dispatch_cmd(aid(1), base_ts()));
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(64_000),
        cpu(32_000),
        1,
    );

    let starts = projected_starts(&sm);
    assert_eq!(starts.get(&alid(50)), Some(&None));
}

/// Nodes with no accruals produce no entries, and the map is keyed by
/// allocation alone — an allocation's bound never leaks to another node's
/// queue.
#[test]
fn the_projection_only_covers_queued_accruals() {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));
    // A funded allocation (not accruing) is never in the map.
    let starts = projected_starts(&sm);
    assert!(starts.is_empty());

    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(32_000),
        cpu(16_000),
        1,
    );
    let starts = projected_starts(&sm);
    assert_eq!(
        BTreeMap::from([(alid(50), Some(base_ts() + Duration::from_secs(3600)))]),
        starts
    );
}

// ---- queue records missing from the allocation map ----

/// A queue entry whose allocation record is missing (a hand-built state)
/// must drop out of the result without shifting its neighbours' projections
/// onto the wrong ids — the id stays glued to its own need through the
/// per-node filter.
#[test]
fn a_missing_queue_record_does_not_shift_neighbours() {
    let mut sm = setup(cpu(64_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));

    // seq 0 has no allocation record at all; the valid accrual sits behind
    // it (seq 1) and needs exactly the one guaranteed release.
    sm.accrual_queue.insert((nid(1), 0), alid(99));
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(32_000),
        cpu(16_000),
        1,
    );

    let starts = projected_starts(&sm);
    assert_eq!(
        BTreeMap::from([(alid(50), Some(base_ts() + Duration::from_secs(3600)))]),
        starts,
        "the valid accrual keeps its own projection; the missing id is absent"
    );
}

// ---- per-view memoization ----

/// The cached entry point computes once per memo table and always answers
/// exactly what a fresh sweep of the same state would.
#[test]
fn the_cached_projection_is_shared_per_view_and_correct() {
    let mut sm = setup(cpu(32_000), 4);
    seed_running(&mut sm, 1, 1, cpu(16_000), Some(3600));
    seed_accrual(
        &mut sm,
        alid(50),
        jid(2),
        nid(1),
        cpu(32_000),
        cpu(16_000),
        1,
    );

    let memos = ViewMemos::default();
    let first = projected_starts_cached(&sm, &memos);
    let second = projected_starts_cached(&sm, &memos);
    assert!(
        Arc::ptr_eq(&first, &second),
        "one view, one computation: later reads share the memo"
    );

    let expected: BTreeMap<_, _> =
        BTreeMap::from([(alid(50), Some(base_ts() + Duration::from_secs(3600)))]);
    assert_eq!(&**first, &expected);
    assert_eq!(
        &**projected_starts_cached(&sm, &ViewMemos::default()),
        &expected
    );

    // The cached map derefs to the plain projection of the same state.
    let plain: BTreeMap<_, _> = projected_starts(&sm);
    let wrapped: &BTreeMap<_, _> = &first;
    assert_eq!(wrapped, &plain);
    let _typed: &ProjectedStarts = &first;
}
