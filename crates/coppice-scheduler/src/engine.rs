//! The v1 heuristic scheduling engine.
//!
//! One pass is a pure function of `(snapshot, now_us)`: it scores the queued
//! backlog in effective-score order (ADR 0021), packs jobs onto nodes best-fit,
//! and uses accruing allocations as the license to backfill *past* a blocked
//! high-score job (ADR 0014), lending pledged capacity only against finite
//! `projected_ready` bounds and steering accruals toward nodes that give them
//! one (ADR 0027). Every proposal it emits is shaped so the apply
//! side (`coppice_state::apply::commit_placements`) accepts it: the engine
//! carries its own faithful simulator of apply's funding arithmetic and accrual
//! guard, and never proposes a batch that simulator would see rejected. No
//! clocks, no randomness, no I/O; iteration is over `BTreeMap`s only, and `f64`
//! scores order the pass and are discarded (ADR 0019).
//!
//! See `docs/scheduling/scheduler-v1.md`, `docs/scheduling/scheduling-model.md`,
//! and this crate's `README.md`.

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::id::{AllocationId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, JobState};
use coppice_core::node::Node;
use coppice_core::resource::Resources;
use coppice_state::StateMachine;

use crate::score::{self, Rank};
use crate::{PlacementProposal, ProposedPlacement, Scheduler, SchedulerConfig};

/// The v1 heuristic scheduler: effective-score ordering (ADR 0021), best-fit
/// packing, accruing allocations as the license to backfill (ADR 0014).
#[derive(Debug, Default)]
pub struct HeuristicScheduler {
    config: SchedulerConfig,
}

impl HeuristicScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        HeuristicScheduler { config }
    }
}

impl Scheduler for HeuristicScheduler {
    fn schedule(&self, snapshot: &StateMachine, now_us: i64) -> PlacementProposal {
        let mut pass = Pass::new(snapshot, now_us, &self.config);
        // Re-plan first: moving an existing accrual to a node where its full
        // request now fits frees the accrual queue before new work competes
        // for the same space (scheduling-model.md).
        pass.replan_existing_accruals();
        pass.seat_candidates();
        pass.into_proposal()
    }
}

// ---- v1 seams (pure, unit-tested) ----

/// The hard label constraints a job demands of a node.
///
/// The frozen `Job` proto carries no label selector yet, so v1 returns the
/// empty selector — every node satisfies it. This is the seam for hard
/// placement constraints (`docs/scheduling/scheduling-model.md`).
fn required_labels(_job: &Job) -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// Whether `labels` satisfies every `(k, v)` in `required` (present with an
/// equal value). An empty selector is satisfied by any node.
fn node_satisfies_labels(
    required: &BTreeMap<String, String>,
    labels: &BTreeMap<String, String>,
) -> bool {
    required.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// Image-cache soft affinity (ADR 0010): a bonus folded into the best-fit node
/// choice so a warm image cache can pull a job toward a node later.
///
/// v1 returns no bonus, so node choice is pure best-fit; the seam exists so
/// image-cache scoring slots in without reshaping the packing loop.
fn cache_affinity_bonus(_job: &Job, _node: &Node) -> f64 {
    0.0
}

// ---- pure packing keys ----

/// Best-fit key: the dominant leftover fraction after seating `requested` in
/// `free` on a node of `capacity` — `max` over dims of
/// `(free − requested) / capacity`. Tighter packings score lower. Empty
/// dimensions (`capacity == 0`) contribute `0.0`.
fn dominant_leftover_fraction(
    free: &Resources,
    requested: &Resources,
    capacity: &Resources,
) -> f64 {
    let after = free.saturating_sub(requested);
    let frac = |a: u64, c: u64| if c == 0 { 0.0 } else { a as f64 / c as f64 };
    frac(after.cpu_millis, capacity.cpu_millis)
        .max(frac(after.memory_bytes, capacity.memory_bytes))
        .max(frac(after.disk_bytes, capacity.disk_bytes))
}

/// Borrowed-capacity key for a strict-backfill lend: the sum over dims of the
/// pledged capacity a lend must borrow, `max(0, requested − free)`, normalized
/// by capacity. A node lending less scores lower.
fn borrowed_fraction(free: &Resources, requested: &Resources, capacity: &Resources) -> f64 {
    let borrow = requested.saturating_sub(free);
    let frac = |b: u64, c: u64| if c == 0 { 0.0 } else { b as f64 / c as f64 };
    frac(borrow.cpu_millis, capacity.cpu_millis)
        + frac(borrow.memory_bytes, capacity.memory_bytes)
        + frac(borrow.disk_bytes, capacity.disk_bytes)
}

/// Accrual-opening key: the fraction of `requested` a node could immediately
/// pledge from `free`. A node that funds more of the request scores higher, so
/// the opened accrual has the smallest remaining need.
fn pledge_fraction(free: &Resources, requested: &Resources) -> f64 {
    let pledge = free.component_min(requested);
    let frac = |a: u64, r: u64| if r == 0 { 1.0 } else { a as f64 / r as f64 };
    frac(pledge.cpu_millis, requested.cpu_millis)
        + frac(pledge.memory_bytes, requested.memory_bytes)
        + frac(pledge.disk_bytes, requested.disk_bytes)
}

// ---- per-node working model ----

/// A surviving accruing allocation in the pass's working model. Built in
/// ascending `seq` (funding order, ADR 0014) from the accrual queue.
#[derive(Debug, Clone)]
struct SimAccrual {
    alloc: AllocationId,
    job: JobId,
    requested: Resources,
    /// Pledge so far. Mirrors `free_capacity`: already excluded from the node's
    /// base free capacity.
    funded: Resources,
    /// The strict-backfill bound (ADR 0014): the earliest time guaranteed
    /// releases fully fund this accrual, or `None` if unbounded (no guaranteed
    /// release ever completes it).
    projected_ready: Option<i64>,
}

/// A node's per-pass working model. Borrows the snapshot's [`Node`] so the hot
/// packing loops read capacity and labels without a map lookup.
struct NodeModel<'a> {
    node: &'a Node,
    /// Free capacity mirroring `free_capacity`, decremented as the batch seats
    /// work and incremented (then re-pledged) as it revokes accruals.
    sim_free: Resources,
    /// Surviving accrual queue in ascending `seq` (funding order, ADR 0014).
    accruals: Vec<SimAccrual>,
    /// Guaranteed release events on this node, sorted by `(time, seq)`
    /// (`collect_release_events`), for `projected_ready` sweeps against both
    /// the existing queue and would-be accrual placements (ADR 0027).
    events: Vec<(i64, u64, Resources)>,
    /// Remaining needs of accruals this batch placed on the node (opens and
    /// moves, in payload order — they fund behind the surviving queue), so
    /// later candidate sweeps see the whole claim on the node's events.
    pending_accruals: Vec<Resources>,
}

// ---- the pass ----

struct Pass<'a> {
    snapshot: &'a StateMachine,
    now_us: i64,
    max_candidates: usize,
    max_placements: usize,
    w_age: f64,
    horizon: i64,
    /// K, the replicated accrual cap.
    accrual_limit: usize,
    /// The finite→finite move threshold (ADR 0027):
    /// `SchedulerConfig::replan_min_improvement_us`.
    min_improvement_us: i64,
    /// Distinct jobs holding an accrual at pass start. When this already meets
    /// K, no new accrual (an open or a lend's net growth) can be admitted, so
    /// those paths short-circuit rather than scan (cheap in a healthy cluster,
    /// where accruals are ≤ K; the guard against a pathological backlog).
    base_before: usize,
    /// Free capacity per node before any batch effect (`free_capacity` for
    /// every node), computed once so the simulator never rescans allocations.
    base_free: BTreeMap<NodeId, Resources>,
    nodes: Vec<NodeModel<'a>>,
    node_index: BTreeMap<NodeId, usize>,
    /// Nodes that already carry a batch placement; a lend must not revoke on
    /// them (revocations apply before placements, so revoking after placing
    /// would reorder the funding the placement already saw).
    placed_nodes: BTreeSet<NodeId>,
    /// Nodes that already carry a batch revocation; kept disjoint from lends
    /// for the same ordering reason.
    revoked_nodes: BTreeSet<NodeId>,
    revocations: Vec<AllocationId>,
    placements: Vec<ProposedPlacement>,
}

impl<'a> Pass<'a> {
    fn new(snapshot: &'a StateMachine, now_us: i64, config: &SchedulerConfig) -> Pass<'a> {
        let base_free = free_capacity_map(snapshot);
        let horizon = score::age_horizon_us(&snapshot.policy.decay);

        let accrual_nodes: BTreeSet<NodeId> = snapshot
            .accrual_queue
            .keys()
            .map(|(node, _)| *node)
            .collect();
        // Events for every node, not only accrual hosts: candidate bounds for
        // opening or moving an accrual sweep any eligible node (ADR 0027).
        let mut events = collect_release_events(snapshot);

        let mut nodes: Vec<NodeModel> = Vec::new();
        let mut node_index: BTreeMap<NodeId, usize> = BTreeMap::new();
        for (id, rec) in &snapshot.nodes {
            let hosts_accrual = accrual_nodes.contains(id);
            // Unschedulable nodes matter only as accrual sources to re-plan
            // off of; a drained node with no accruals is dead weight.
            if !rec.node.schedulable && !hosts_accrual {
                continue;
            }
            let mut accruals: Vec<SimAccrual> = Vec::new();
            for (_, alloc_id) in snapshot.accrual_queue.range((*id, 0)..=(*id, u64::MAX)) {
                let Some(a) = snapshot.allocations.get(alloc_id) else {
                    continue;
                };
                accruals.push(SimAccrual {
                    alloc: *alloc_id,
                    job: a.allocation.job,
                    requested: a.allocation.requested,
                    funded: a.allocation.funded,
                    projected_ready: None,
                });
            }
            let mut evs = events.remove(id).unwrap_or_default();
            sort_release_events(&mut evs);
            if !accruals.is_empty() {
                let remaining: Vec<Resources> = accruals
                    .iter()
                    .map(|a| a.requested.saturating_sub(&a.funded))
                    .collect();
                let ready = sweep_projected_ready(&evs, &remaining);
                for (a, t) in accruals.iter_mut().zip(ready) {
                    a.projected_ready = t;
                }
            }
            let idx = nodes.len();
            node_index.insert(*id, idx);
            nodes.push(NodeModel {
                node: &rec.node,
                sim_free: base_free.get(id).copied().unwrap_or(Resources::ZERO),
                accruals,
                events: evs,
                pending_accruals: Vec::new(),
            });
        }

        let base_before = distinct_accruing_jobs(snapshot);

        Pass {
            snapshot,
            now_us,
            max_candidates: config.max_candidates,
            max_placements: config.max_placements_per_cycle,
            w_age: config.w_age,
            horizon,
            accrual_limit: snapshot.policy.accrual_limit as usize,
            min_improvement_us: config.replan_min_improvement_us,
            base_before,
            base_free,
            nodes,
            node_index,
            placed_nodes: BTreeSet::new(),
            revoked_nodes: BTreeSet::new(),
            revocations: Vec::new(),
            placements: Vec::new(),
        }
    }

    /// Score the queued backlog and take the top `max_candidates` by the
    /// ADR 0021 total order (score desc, then FIFO, then `JobId`).
    fn select_candidates(&self) -> Vec<Rank> {
        if self.max_candidates == 0 {
            return Vec::new();
        }
        let snapshot = self.snapshot;
        let mut memo: BTreeMap<QuotaEntityId, f64> = BTreeMap::new();
        let mut ranks: Vec<Rank> = Vec::new();
        for (job_id, jr) in &snapshot.jobs {
            if jr.state != JobState::Queued {
                continue;
            }
            let leaf = jr.spec.quota_entity;
            let penalty = match memo.get(&leaf) {
                Some(v) => *v,
                None => {
                    let v = score::penalty_product(
                        &snapshot.quota_entities,
                        leaf,
                        &snapshot.policy,
                        self.now_us,
                    );
                    memo.insert(leaf, v);
                    v
                }
            };
            let s = score::effective_score(
                jr.multiplier,
                penalty,
                jr.submitted_at_us,
                self.now_us,
                self.horizon,
                self.w_age,
            );
            ranks.push(Rank {
                score: s,
                submitted_at_us: jr.submitted_at_us,
                job: *job_id,
            });
        }
        if ranks.len() > self.max_candidates {
            ranks.select_nth_unstable(self.max_candidates - 1);
            ranks.truncate(self.max_candidates);
        }
        ranks.sort_unstable();
        ranks
    }

    /// Re-plan existing accruals (scheduling-model.md): if a distinct
    /// accruing job's full request now fits on another schedulable node,
    /// revoke its accrual and reseat it there, funded. Failing that, move it
    /// to a node that meaningfully improves its `projected_ready` bound
    /// (ADR 0027): always when the move turns an indefinite bound finite,
    /// and past the configured improvement threshold between finite bounds.
    ///
    /// Anti-churn (ADR 0014): a revocation is emitted only when it enables a
    /// concrete, strictly better reseat in the same batch — never a
    /// revoke-in-place.
    fn replan_existing_accruals(&mut self) {
        let snapshot = self.snapshot;
        let entries: Vec<(JobId, AllocationId, NodeId)> = snapshot
            .accrual_queue
            .iter()
            .filter_map(|((node, _), alloc)| {
                let a = snapshot.allocations.get(alloc)?;
                Some((a.allocation.job, *alloc, *node))
            })
            .collect();
        let mut seen: BTreeSet<JobId> = BTreeSet::new();
        // Improvement scans sweep every node's events, so bound them by K per
        // pass: a healthy cluster holds at most K accruals anyway, and a
        // pathological over-cap backlog must not wedge the pass.
        let mut improvement_scans = 0usize;
        for (job_id, alloc, source) in entries {
            if !seen.insert(job_id) {
                continue;
            }
            // Bounded like the seating loop: re-planning cannot emit more than
            // the placement cap, and a healthy cluster has ≤ K accruals anyway.
            if self.placements.len() >= self.max_placements || seen.len() > self.max_placements {
                break;
            }
            let Some(jr) = snapshot.jobs.get(&job_id) else {
                continue;
            };
            // Only a Preparing job whose current attempt holds this accrual is
            // reseatable (validate_placement's reseat path); an aborting job is
            // winding down, so moving it is pointless churn.
            if jr.state != JobState::Preparing || jr.spec.abort_requested.is_some() {
                continue;
            }
            let is_current = jr
                .current_attempt
                .and_then(|at| snapshot.attempts.get(&at))
                .map(|a| a.attempt.allocation == alloc)
                .unwrap_or(false);
            if !is_current {
                continue;
            }
            // The source will take a revocation; it must not already carry a
            // placement (revocations apply first, so that would reorder).
            if self.placed_nodes.contains(&source) {
                continue;
            }
            let requested = jr.spec.requests;
            let required = required_labels(&jr.spec);
            if let Some(target_idx) =
                self.best_reseat_target(source, &jr.spec, &requested, &required)
            {
                self.revoke_accrual_on_source(source, alloc);
                self.revocations.push(alloc);
                self.revoked_nodes.insert(source);
                let target_id = self.nodes[target_idx].node.id;
                let pledge = self.nodes[target_idx].sim_free.component_min(&requested);
                self.nodes[target_idx].sim_free =
                    self.nodes[target_idx].sim_free.saturating_sub(&pledge);
                self.placed_nodes.insert(target_id);
                self.placements.push(ProposedPlacement {
                    job: job_id,
                    node: target_id,
                    requested,
                    expect_funded: true,
                });
                continue;
            }
            if improvement_scans < self.accrual_limit {
                improvement_scans += 1;
                self.try_improve_accrual_bound(job_id, alloc, source, &requested, &required);
            }
        }
    }

    /// The ADR 0027 improvement move: reseat one accrual on a node that gives
    /// it a strictly better `projected_ready` bound. Same batch shape as a
    /// lend's reseat — revoke on the source, place (usually accruing again)
    /// on the target — so, like the lend, it is gated on the exact simulator:
    /// a move never grows the distinct accruing-job count, but an over-cap
    /// cluster would still trip apply's guard.
    fn try_improve_accrual_bound(
        &mut self,
        job_id: JobId,
        alloc: AllocationId,
        source: NodeId,
        requested: &Resources,
        required: &BTreeMap<String, String>,
    ) -> bool {
        let Some(&source_idx) = self.node_index.get(&source) else {
            return false;
        };
        let Some(current) = self.accrual_projected_ready(source_idx, alloc) else {
            return false;
        };
        let Some(target_idx) = self.best_improvement_target(source, requested, required, current)
        else {
            return false;
        };
        let target_id = self.nodes[target_idx].node.id;
        let mut revocations = self.revocations.clone();
        revocations.push(alloc);
        let mut placements = self.placements.clone();
        placements.push(ProposedPlacement {
            job: job_id,
            node: target_id,
            requested: *requested,
            expect_funded: false,
        });
        if simulate_batch(self.snapshot, &self.base_free, &revocations, &placements).rejects_accrual
        {
            return false;
        }
        self.revoke_accrual_on_source(source, alloc);
        self.revocations = revocations;
        self.revoked_nodes.insert(source);
        let pledge = self.nodes[target_idx].sim_free.component_min(requested);
        self.nodes[target_idx].sim_free = self.nodes[target_idx].sim_free.saturating_sub(&pledge);
        let remaining = requested.saturating_sub(&pledge);
        if !remaining.is_zero() {
            self.nodes[target_idx].pending_accruals.push(remaining);
        }
        self.placed_nodes.insert(target_id);
        self.placements = placements;
        true
    }

    /// This accrual's `projected_ready` on its current node, recomputed from
    /// the pass's working model (an earlier revocation on the node may have
    /// grown the survivors' funding, making the pass-start value stale).
    /// Outer `None` when the allocation is not in the model.
    fn accrual_projected_ready(&self, node_idx: usize, alloc: AllocationId) -> Option<Option<i64>> {
        let nm = &self.nodes[node_idx];
        let pos = nm.accruals.iter().position(|a| a.alloc == alloc)?;
        let needs: Vec<Resources> = nm
            .accruals
            .iter()
            .map(|a| a.requested.saturating_sub(&a.funded))
            .collect();
        Some(sweep_projected_ready(&nm.events, &needs)[pos])
    }

    /// Best node — schedulable, not the source, not already a revocation
    /// source — that gives the moved accrual a finite `projected_ready`
    /// meaningfully better than `current` (ADR 0027): any finite bound when
    /// `current` is indefinite, otherwise earlier by at least the configured
    /// threshold. Earliest bound wins, ties by largest immediately-pledged
    /// fraction, then lowest `NodeId`.
    fn best_improvement_target(
        &self,
        source: NodeId,
        requested: &Resources,
        required: &BTreeMap<String, String>,
        current: Option<i64>,
    ) -> Option<usize> {
        let mut best: Option<(i64, f64, usize)> = None;
        for (i, nm) in self.nodes.iter().enumerate() {
            if nm.node.id == source || !nm.node.schedulable {
                continue;
            }
            if self.revoked_nodes.contains(&nm.node.id) {
                continue;
            }
            if !requested.fits_within(&nm.node.capacity)
                || !node_satisfies_labels(required, &nm.node.labels)
            {
                continue;
            }
            let pledge = nm.sim_free.component_min(requested);
            let remaining = requested.saturating_sub(&pledge);
            let ready = if remaining.is_zero() {
                // A full immediate fit: the best possible bound. Reached only
                // on accrual-hosting nodes — `best_reseat_target` already took
                // any accrual-free full fit.
                self.now_us
            } else {
                match candidate_projected_ready(nm, &remaining) {
                    Some(t) => t,
                    // An indefinite bound is never worth moving to.
                    None => continue,
                }
            };
            let improves = match current {
                None => true,
                Some(c) => ready < c && ready.saturating_add(self.min_improvement_us) <= c,
            };
            if !improves {
                continue;
            }
            let pledged = pledge_fraction(&nm.sim_free, requested);
            let better = match best {
                None => true,
                Some((best_ready, best_pledged, _)) => {
                    ready < best_ready || (ready == best_ready && pledged > best_pledged)
                }
            };
            if better {
                best = Some((ready, pledged, i));
            }
        }
        best.map(|(_, _, i)| i)
    }

    /// Best schedulable node — other than `source`, accrual-free, and not
    /// already a revocation source — whose free capacity holds the whole
    /// request. Accrual-free keeps the target off any future revocation, so the
    /// batch never places-then-revokes on one node.
    fn best_reseat_target(
        &self,
        source: NodeId,
        job: &Job,
        requested: &Resources,
        required: &BTreeMap<String, String>,
    ) -> Option<usize> {
        let mut best: Option<(f64, usize)> = None;
        for (i, nm) in self.nodes.iter().enumerate() {
            if nm.node.id == source || !nm.node.schedulable || !nm.accruals.is_empty() {
                continue;
            }
            if self.revoked_nodes.contains(&nm.node.id) {
                continue;
            }
            if !requested.fits_within(&nm.node.capacity)
                || !node_satisfies_labels(required, &nm.node.labels)
                || !requested.fits_within(&nm.sim_free)
            {
                continue;
            }
            let key = dominant_leftover_fraction(&nm.sim_free, requested, &nm.node.capacity)
                - cache_affinity_bonus(job, nm.node);
            let better = match best {
                None => true,
                Some((bk, _)) => key < bk,
            };
            if better {
                best = Some((key, i));
            }
        }
        best.map(|(_, i)| i)
    }

    /// Revoke one accrual on its node: return its pledge to free capacity, then
    /// pledge onward to the surviving accruals in `seq` order, mirroring apply's
    /// revocation effect exactly.
    fn revoke_accrual_on_source(&mut self, source: NodeId, alloc: AllocationId) {
        let Some(&idx) = self.node_index.get(&source) else {
            return;
        };
        let nm = &mut self.nodes[idx];
        let Some(pos) = nm.accruals.iter().position(|a| a.alloc == alloc) else {
            return;
        };
        let removed = nm.accruals.remove(pos);
        let mut free = nm.sim_free.saturating_add(&removed.funded);
        let mut funded_now: Vec<usize> = Vec::new();
        for (j, a) in nm.accruals.iter_mut().enumerate() {
            if free.is_zero() {
                break;
            }
            let need = a.requested.saturating_sub(&a.funded);
            if need.is_zero() {
                continue;
            }
            let pledge = free.component_min(&need);
            a.funded = a.funded.saturating_add(&pledge);
            free = free.saturating_sub(&pledge);
            if a.funded == a.requested {
                funded_now.push(j);
            }
        }
        nm.sim_free = free;
        // A fully-pledged survivor is no longer accruing; drop it in reverse so
        // earlier indices stay valid.
        for j in funded_now.into_iter().rev() {
            nm.accruals.remove(j);
        }
    }

    /// Seat the scored candidates in order, stopping at the placement cap.
    fn seat_candidates(&mut self) {
        let snapshot = self.snapshot;
        let ranks = self.select_candidates();
        for rank in ranks {
            if self.placements.len() >= self.max_placements {
                break;
            }
            let Some(jr) = snapshot.jobs.get(&rank.job) else {
                continue;
            };
            if jr.state != JobState::Queued {
                continue;
            }
            let requested = jr.spec.requests;
            let required = required_labels(&jr.spec);
            if self.try_free_fit(rank.job, &jr.spec, &requested, &required) {
                continue;
            }
            if self.try_backfill(rank.job, &jr.spec, &requested, &required) {
                continue;
            }
            self.try_open_accrual(rank.job, &jr.spec, &requested, &required);
        }
    }

    /// Seat on the best-fit node whose free capacity holds the whole request.
    fn try_free_fit(
        &mut self,
        job_id: JobId,
        job: &Job,
        requested: &Resources,
        required: &BTreeMap<String, String>,
    ) -> bool {
        let mut best: Option<(f64, usize)> = None;
        for (i, nm) in self.nodes.iter().enumerate() {
            if !nm.node.schedulable
                || !requested.fits_within(&nm.node.capacity)
                || !node_satisfies_labels(required, &nm.node.labels)
                || !requested.fits_within(&nm.sim_free)
            {
                continue;
            }
            let key = dominant_leftover_fraction(&nm.sim_free, requested, &nm.node.capacity)
                - cache_affinity_bonus(job, nm.node);
            let better = match best {
                None => true,
                Some((bk, _)) => key < bk,
            };
            if better {
                best = Some((key, i));
            }
        }
        let Some((_, idx)) = best else { return false };
        let node_id = self.nodes[idx].node.id;
        // Fits within sim_free ⇒ apply funds it fully; the placement will not
        // join the accrual set.
        let pledge = self.nodes[idx].sim_free.component_min(requested);
        self.nodes[idx].sim_free = self.nodes[idx].sim_free.saturating_sub(&pledge);
        self.placed_nodes.insert(node_id);
        self.placements.push(ProposedPlacement {
            job: job_id,
            node: node_id,
            requested: *requested,
            expect_funded: true,
        });
        true
    }

    /// Strict backfill (ADR 0014): if the job carries an enforced `max_runtime`,
    /// lend a node's pledged capacity by revoking its whole accrual queue,
    /// seating the job, and reseating each accrual after it — but only when the
    /// job is guaranteed to finish before every touched accrual would otherwise
    /// become ready.
    fn try_backfill(
        &mut self,
        job_id: JobId,
        job: &Job,
        requested: &Resources,
        required: &BTreeMap<String, String>,
    ) -> bool {
        let Some(runtime) = job.max_runtime_us else {
            return false;
        };
        // A lend reseats the survivors it revokes, so it never shrinks the
        // accruing set; if the cluster is already at K, any lend is doomed by
        // the guard. Skip the scan.
        if self.base_before >= self.accrual_limit {
            return false;
        }
        let deadline = self.now_us.saturating_add(runtime as i64);
        let mut best: Option<(f64, usize)> = None;
        for (i, nm) in self.nodes.iter().enumerate() {
            if !nm.node.schedulable || nm.accruals.is_empty() {
                continue;
            }
            // One lend per node per pass; keep it off any node the batch has
            // already touched so revocation-before-placement holds.
            if self.placed_nodes.contains(&nm.node.id) || self.revoked_nodes.contains(&nm.node.id) {
                continue;
            }
            if !requested.fits_within(&nm.node.capacity)
                || !node_satisfies_labels(required, &nm.node.labels)
            {
                continue;
            }
            // Fits once the pledged capacity of every survivor is lent back.
            let mut lendable = nm.sim_free;
            for a in &nm.accruals {
                lendable = lendable.saturating_add(&a.funded);
            }
            if !requested.fits_within(&lendable) {
                continue;
            }
            // Conservative touch set: the strict rule must hold for every
            // surviving accrual. `projected_ready == None` forbids the lend
            // (ADR 0027): an accrual with no guaranteed funding bound keeps
            // every unit it accrues, so backfill can never starve it.
            let safe = nm.accruals.iter().all(|a| match a.projected_ready {
                None => false,
                Some(ready) => deadline <= ready,
            });
            if !safe {
                continue;
            }
            let key = borrowed_fraction(&nm.sim_free, requested, &nm.node.capacity);
            let better = match best {
                None => true,
                Some((bk, _)) => key < bk,
            };
            if better {
                best = Some((key, i));
            }
        }
        let Some((_, idx)) = best else { return false };

        // Build the lend as a candidate batch and let the exact simulator gate
        // it: the reseats keep the accrual set unchanged, but a cluster already
        // over the accrual cap could still trip the guard, so verify.
        let survivors: Vec<SimAccrual> = self.nodes[idx].accruals.clone();
        let node_id = self.nodes[idx].node.id;
        let mut revocations = self.revocations.clone();
        let mut placements = self.placements.clone();
        for a in &survivors {
            revocations.push(a.alloc);
        }
        placements.push(ProposedPlacement {
            job: job_id,
            node: node_id,
            requested: *requested,
            expect_funded: true,
        });
        for a in &survivors {
            placements.push(ProposedPlacement {
                job: a.job,
                node: node_id,
                requested: a.requested,
                expect_funded: false,
            });
        }
        if simulate_batch(self.snapshot, &self.base_free, &revocations, &placements).rejects_accrual
        {
            return false;
        }

        // Commit. sim_free reflects revoking every survivor (their pledge
        // returns whole, nothing survives to re-pledge), then the job, then the
        // reseats in seq order; the reseats' remainders stay visible to later
        // candidate sweeps as pending claims on the node's events.
        self.revocations = revocations;
        self.placements = placements;
        self.revoked_nodes.insert(node_id);
        self.placed_nodes.insert(node_id);
        let mut free = self.nodes[idx].sim_free;
        for a in &survivors {
            free = free.saturating_add(&a.funded);
        }
        free = free.saturating_sub(&free.component_min(requested));
        let mut pending: Vec<Resources> = Vec::with_capacity(survivors.len());
        for a in &survivors {
            let pledge = free.component_min(&a.requested);
            free = free.saturating_sub(&pledge);
            let remaining = a.requested.saturating_sub(&pledge);
            if !remaining.is_zero() {
                pending.push(remaining);
            }
        }
        self.nodes[idx].sim_free = free;
        self.nodes[idx].accruals.clear();
        self.nodes[idx].pending_accruals.extend(pending);
        true
    }

    /// Open an accrual for a blocked head-of-order job — the license to
    /// backfill past it (ADR 0014) — when the accrual guard permits another
    /// accruing job and some node passes the hard filters.
    ///
    /// Node choice prefers a finite `projected_ready` (ADR 0027): a node on
    /// which the accrual's bound would be indefinite is never chosen while
    /// some eligible node offers a finite one. Finite candidates rank by
    /// earliest bound, ties by largest immediately-pledged fraction;
    /// indefinite candidates rank by pledged fraction alone. Ties to lowest
    /// `NodeId` throughout (iteration is `NodeId`-ascending, keep the first).
    fn try_open_accrual(
        &mut self,
        job_id: JobId,
        _job: &Job,
        requested: &Resources,
        required: &BTreeMap<String, String>,
    ) -> bool {
        // Opening adds one accruing job; if the cluster is already at K, the
        // guard rejects it. Skip the scan and the simulation.
        if self.base_before >= self.accrual_limit {
            return false;
        }
        let mut best: Option<(Option<i64>, f64, usize)> = None;
        for (i, nm) in self.nodes.iter().enumerate() {
            if !nm.node.schedulable
                || !requested.fits_within(&nm.node.capacity)
                || !node_satisfies_labels(required, &nm.node.labels)
            {
                continue;
            }
            // Nonzero: a full free fit was already taken by `try_free_fit`,
            // which scans the same nodes under the same filters.
            let remaining = requested.saturating_sub(&nm.sim_free.component_min(requested));
            let ready = candidate_projected_ready(nm, &remaining);
            let pledged = pledge_fraction(&nm.sim_free, requested);
            let better = match &best {
                None => true,
                Some((best_ready, best_pledged, _)) => match (ready, *best_ready) {
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (Some(t), Some(bt)) => t < bt || (t == bt && pledged > *best_pledged),
                    (None, None) => pledged > *best_pledged,
                },
            };
            if better {
                best = Some((ready, pledged, i));
            }
        }
        let Some((_, _, idx)) = best else {
            return false;
        };
        let node_id = self.nodes[idx].node.id;
        let mut placements = self.placements.clone();
        placements.push(ProposedPlacement {
            job: job_id,
            node: node_id,
            requested: *requested,
            expect_funded: false,
        });
        if simulate_batch(
            self.snapshot,
            &self.base_free,
            &self.revocations,
            &placements,
        )
        .rejects_accrual
        {
            return false;
        }
        self.placements = placements;
        self.placed_nodes.insert(node_id);
        let pledge = self.nodes[idx].sim_free.component_min(requested);
        self.nodes[idx].sim_free = self.nodes[idx].sim_free.saturating_sub(&pledge);
        self.nodes[idx]
            .pending_accruals
            .push(requested.saturating_sub(&pledge));
        true
    }

    fn into_proposal(mut self) -> PlacementProposal {
        // Authoritative `expect_funded`: re-derive every placement's funding
        // from a faithful replay of apply's effects, so the prediction matches
        // the outcome apply will compute bit for bit.
        let sim = simulate_batch(
            self.snapshot,
            &self.base_free,
            &self.revocations,
            &self.placements,
        );
        for (p, funded) in self.placements.iter_mut().zip(sim.funded) {
            p.expect_funded = funded;
        }
        PlacementProposal {
            against_version: self.snapshot.version,
            now_us: self.now_us,
            revocations: self.revocations,
            placements: self.placements,
        }
    }
}

// ---- apply-faithful helpers ----

/// The number of distinct jobs holding an accruing allocation — apply's
/// `before` at pass start (`check_accrual_limit`).
fn distinct_accruing_jobs(snapshot: &StateMachine) -> usize {
    let mut jobs: BTreeSet<JobId> = BTreeSet::new();
    for alloc_id in snapshot.accrual_queue.values() {
        if let Some(a) = snapshot.allocations.get(alloc_id) {
            jobs.insert(a.allocation.job);
        }
    }
    jobs.len()
}

/// Free capacity per node: advertised capacity minus the funded holds of every
/// non-Released allocation (`StateMachine::free_capacity`), computed for all
/// nodes in one allocation scan.
fn free_capacity_map(snapshot: &StateMachine) -> BTreeMap<NodeId, Resources> {
    let mut used: BTreeMap<NodeId, Resources> = BTreeMap::new();
    for r in snapshot.allocations.values() {
        if r.allocation.state != AllocationState::Released {
            let e = used.entry(r.allocation.node).or_insert(Resources::ZERO);
            *e = e.saturating_add(&r.allocation.funded);
        }
    }
    snapshot
        .nodes
        .iter()
        .map(|(id, rec)| {
            let u = used.get(id).copied().unwrap_or(Resources::ZERO);
            (*id, rec.node.capacity.saturating_sub(&u))
        })
        .collect()
}

/// Guaranteed release events per node: an allocation whose attempt is
/// `Running` with a start time, on a job with an enforced `max_runtime`,
/// releases its funded capacity at `start + max_runtime` (ADR 0014). Any
/// other live allocation has no guaranteed bound and contributes nothing.
/// Collected for every node — candidate bounds for opening or moving an
/// accrual sweep any eligible node (ADR 0027), not only current hosts.
fn collect_release_events(snapshot: &StateMachine) -> BTreeMap<NodeId, Vec<(i64, u64, Resources)>> {
    let mut events: BTreeMap<NodeId, Vec<(i64, u64, Resources)>> = BTreeMap::new();
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
        let Some(started) = at.started_at_us else {
            continue;
        };
        let Some(job) = snapshot.jobs.get(&rec.allocation.job) else {
            continue;
        };
        let Some(runtime) = job.spec.max_runtime_us else {
            continue;
        };
        let release = started.saturating_add(runtime as i64);
        events
            .entry(node)
            .or_default()
            .push((release, rec.seq, rec.allocation.funded));
    }
    events
}

/// Order release events for [`sweep_projected_ready`]: ascending time, ties
/// by allocation `seq`.
fn sort_release_events(events: &mut [(i64, u64, Resources)]) {
    events.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
}

/// Sweep guaranteed release events to compute `projected_ready` per accrual
/// (ADR 0014): walk events in time order (pre-sorted by
/// [`sort_release_events`]), pool the freed capacity, and pledge it to the
/// accrual queue in `seq` order exactly as `pledge_node` would. An accrual's
/// `projected_ready` is the event time its remaining need first reaches zero,
/// or `None` if events run out.
fn sweep_projected_ready(
    events: &[(i64, u64, Resources)],
    remaining: &[Resources],
) -> Vec<Option<i64>> {
    let mut rem: Vec<Resources> = remaining.to_vec();
    let mut ready: Vec<Option<i64>> = vec![None; remaining.len()];
    let mut pool = Resources::ZERO;
    for (time, _seq, freed) in events.iter() {
        pool = pool.saturating_add(freed);
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
                *rd = Some(*time);
            }
        }
    }
    ready
}

/// The `projected_ready` a new accrual with `remaining` unfunded need would
/// get on this node (ADR 0027): appended behind the surviving queue and the
/// batch's pending accruals — funding is `seq` order, and batch placements
/// take seqs after the survivors — then swept against the node's guaranteed
/// releases.
fn candidate_projected_ready(nm: &NodeModel, remaining: &Resources) -> Option<i64> {
    let mut needs: Vec<Resources> = nm
        .accruals
        .iter()
        .map(|a| a.requested.saturating_sub(&a.funded))
        .collect();
    needs.extend(nm.pending_accruals.iter().copied());
    needs.push(*remaining);
    sweep_projected_ready(&nm.events, &needs)
        .pop()
        .expect("needs is nonempty")
}

/// The outcome of a faithful batch simulation.
struct BatchSim {
    /// Per placement (payload order): whether apply funds it fully.
    funded: Vec<bool>,
    /// Whether apply's accrual guard would reject the batch.
    rejects_accrual: bool,
}

/// Replay a batch through apply's effects exactly (`commit_placements` +
/// `check_accrual_limit`): revocations first (freed capacity pledged onward to
/// surviving accruals in `seq` order), then placements in payload order, each
/// funded from the recomputed free capacity. Reads only the snapshot and the
/// precomputed base free capacity; never mutates.
fn simulate_batch(
    snapshot: &StateMachine,
    base_free: &BTreeMap<NodeId, Resources>,
    revocations: &[AllocationId],
    placements: &[ProposedPlacement],
) -> BatchSim {
    let revoked: BTreeSet<AllocationId> = revocations.iter().copied().collect();

    let mut accruing: BTreeSet<JobId> = BTreeSet::new();
    for id in snapshot.accrual_queue.values() {
        if revoked.contains(id) {
            continue;
        }
        if let Some(a) = snapshot.allocations.get(id) {
            accruing.insert(a.allocation.job);
        }
    }
    let before = accruing.len();

    let mut touched: BTreeSet<NodeId> = BTreeSet::new();
    for id in revocations {
        if let Some(a) = snapshot.allocations.get(id) {
            touched.insert(a.allocation.node);
        }
    }
    for p in placements {
        touched.insert(p.node);
    }

    let mut sim_free: BTreeMap<NodeId, Resources> = BTreeMap::new();
    for node in &touched {
        let mut free = base_free.get(node).copied().unwrap_or(Resources::ZERO);
        for id in revocations {
            if let Some(a) = snapshot.allocations.get(id) {
                if a.allocation.node == *node {
                    free = free.saturating_add(&a.allocation.funded);
                }
            }
        }
        for (_, alloc_id) in snapshot.accrual_queue.range((*node, 0)..=(*node, u64::MAX)) {
            if revoked.contains(alloc_id) {
                continue;
            }
            let Some(rec) = snapshot.allocations.get(alloc_id) else {
                continue;
            };
            let need = rec
                .allocation
                .requested
                .saturating_sub(&rec.allocation.funded);
            let pledge = free.component_min(&need);
            free = free.saturating_sub(&pledge);
            if pledge == need {
                accruing.remove(&rec.allocation.job);
            }
            if free.is_zero() {
                break;
            }
        }
        sim_free.insert(*node, free);
    }

    let mut funded = Vec::with_capacity(placements.len());
    for p in placements {
        let Some(free) = sim_free.get_mut(&p.node) else {
            funded.push(false);
            continue;
        };
        let pledge = free.component_min(&p.requested);
        *free = free.saturating_sub(&pledge);
        let full = pledge == p.requested;
        funded.push(full);
        if !full {
            accruing.insert(p.job);
        }
    }

    let after = accruing.len();
    let limit = snapshot.policy.accrual_limit as usize;
    BatchSim {
        funded,
        rejects_accrual: after > limit && after > before,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::allocation::Allocation;
    use coppice_core::id::AttemptId;
    use coppice_state::{AllocationRecord, NodeRecord, PolicyConfig};

    fn res(cpu: u64, mem: u64, disk: u64) -> Resources {
        Resources {
            cpu_millis: cpu,
            memory_bytes: mem,
            disk_bytes: disk,
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn empty_selector_matches_every_node() {
        assert!(node_satisfies_labels(
            &BTreeMap::new(),
            &labels(&[("zone", "a")])
        ));
    }

    #[test]
    fn label_filter_requires_every_pair() {
        let node = labels(&[("zone", "a"), ("pool", "batch")]);
        assert!(node_satisfies_labels(&labels(&[("zone", "a")]), &node));
        assert!(node_satisfies_labels(
            &labels(&[("zone", "a"), ("pool", "batch")]),
            &node
        ));
        // Wrong value and missing key both fail.
        assert!(!node_satisfies_labels(&labels(&[("zone", "b")]), &node));
        assert!(!node_satisfies_labels(&labels(&[("gpu", "true")]), &node));
    }

    #[test]
    fn best_fit_prefers_the_tighter_leftover() {
        let req = res(8_000, 0, 0);
        // A big node leaves a large fraction free; a snug node leaves little.
        let big = dominant_leftover_fraction(&res(64_000, 0, 0), &req, &res(64_000, 0, 0));
        let snug = dominant_leftover_fraction(&res(10_000, 0, 0), &req, &res(16_000, 0, 0));
        assert!(
            snug < big,
            "tighter packing must score lower: {snug} !< {big}"
        );
        // Empty dimensions never dominate the key.
        assert_eq!(
            dominant_leftover_fraction(&res(0, 0, 0), &res(0, 0, 0), &res(0, 0, 0)),
            0.0
        );
    }

    #[test]
    fn projected_ready_is_the_event_that_completes_the_need() {
        // One accrual needing 16 cpu; a single release at t=100 frees 16.
        let events = vec![(100_i64, 0_u64, res(16_000, 0, 0))];
        let ready = sweep_projected_ready(&events, &[res(16_000, 0, 0)]);
        assert_eq!(ready, vec![Some(100)]);
    }

    #[test]
    fn projected_ready_unbounded_when_events_fall_short() {
        // Needs 32 cpu but only 16 is ever guaranteed to free ⇒ unbounded.
        let events = vec![(100_i64, 0_u64, res(16_000, 0, 0))];
        let ready = sweep_projected_ready(&events, &[res(32_000, 0, 0)]);
        assert_eq!(ready, vec![None]);
    }

    #[test]
    fn projected_ready_pledges_in_seq_order() {
        // Two releases (t=50, t=100) each free 16; head accrual (seq order)
        // completes at the first, the next at the second.
        let mut events = vec![(100_i64, 1, res(16_000, 0, 0)), (50, 0, res(16_000, 0, 0))];
        sort_release_events(&mut events);
        let ready = sweep_projected_ready(&events, &[res(16_000, 0, 0), res(16_000, 0, 0)]);
        assert_eq!(ready, vec![Some(50), Some(100)]);
    }

    /// A minimal single-node state for exercising the simulator's funding
    /// arithmetic without going through apply.
    fn one_node_state(capacity: Resources) -> StateMachine {
        let mut sm = StateMachine {
            policy: PolicyConfig {
                accrual_limit: 4,
                ..PolicyConfig::default()
            },
            ..StateMachine::default()
        };
        let id = NodeId(uuid::Uuid::from_u128(1));
        sm.nodes.insert(
            id,
            NodeRecord {
                node: Node {
                    id,
                    capacity,
                    labels: BTreeMap::new(),
                    schedulable: true,
                },
                epoch: 1,
            },
        );
        sm
    }

    #[test]
    fn simulator_funds_until_capacity_then_accrues() {
        let sm = one_node_state(res(32_000, 0, 0));
        let base = free_capacity_map(&sm);
        let node = NodeId(uuid::Uuid::from_u128(1));
        let placements = vec![
            ProposedPlacement {
                job: JobId(uuid::Uuid::from_u128(10)),
                node,
                requested: res(20_000, 0, 0),
                expect_funded: false,
            },
            ProposedPlacement {
                job: JobId(uuid::Uuid::from_u128(11)),
                node,
                requested: res(20_000, 0, 0),
                expect_funded: false,
            },
        ];
        let sim = simulate_batch(&sm, &base, &[], &placements);
        // First fits in 32; the second finds only 12 free ⇒ accruing.
        assert_eq!(sim.funded, vec![true, false]);
        // One new accruing job, none before ⇒ within the default cap of 4.
        assert!(!sim.rejects_accrual);
    }

    #[test]
    fn simulator_flags_the_accrual_guard() {
        let mut sm = one_node_state(res(1_000, 0, 0));
        sm.policy.accrual_limit = 0;
        let base = free_capacity_map(&sm);
        let node = NodeId(uuid::Uuid::from_u128(1));
        // A request past the tiny capacity accrues; with a zero cap and no
        // prior accruals, that trips the guard (after=1 > limit=0, > before=0).
        let placements = vec![ProposedPlacement {
            job: JobId(uuid::Uuid::from_u128(10)),
            node,
            requested: res(2_000, 0, 0),
            expect_funded: false,
        }];
        let sim = simulate_batch(&sm, &base, &[], &placements);
        assert_eq!(sim.funded, vec![false]);
        assert!(sim.rejects_accrual);
    }

    #[test]
    fn simulator_revocation_frees_then_pledges_onward() {
        // Node cap 32; an existing accrual funded 16 (needs 32). Revoking it
        // returns 16, so a placement can then be fully funded from 32.
        let mut sm = one_node_state(res(32_000, 0, 0));
        let node = NodeId(uuid::Uuid::from_u128(1));
        let alloc = AllocationId(uuid::Uuid::from_u128(100));
        let attempt = AttemptId(uuid::Uuid::from_u128(101));
        let job = JobId(uuid::Uuid::from_u128(10));
        sm.allocations.insert(
            alloc,
            AllocationRecord {
                allocation: Allocation {
                    id: alloc,
                    job,
                    attempt,
                    node,
                    requested: res(32_000, 0, 0),
                    funded: res(16_000, 0, 0),
                    state: AllocationState::Accruing,
                },
                seq: 0,
            },
        );
        sm.accrual_queue.insert((node, 0), alloc);
        let base = free_capacity_map(&sm);
        assert_eq!(base[&node], res(16_000, 0, 0), "16 funded ⇒ 16 free");
        let placements = vec![ProposedPlacement {
            job: JobId(uuid::Uuid::from_u128(11)),
            node,
            requested: res(32_000, 0, 0),
            expect_funded: false,
        }];
        let sim = simulate_batch(&sm, &base, &[alloc], &placements);
        assert_eq!(sim.funded, vec![true], "revoked 16 + 16 free funds the 32");
        assert!(!sim.rejects_accrual);
    }
}
