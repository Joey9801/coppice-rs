# coppice-scheduler

The scheduler engine turns queued jobs into proposed placement decisions. This
is a high-level summary of the method; the full specification lives in the
main docs, linked throughout.

## Operating model

The scheduler is an asynchronous, CPU-intensive subsystem that **never mutates
authoritative state**. Each pass it:

1. Takes a consistent snapshot of applied cluster state.
2. Orders queued jobs by **effective score** — each job's scalar cost adjusted
   by the soft-quota penalties of its owning entity chain (there are no hard
   limits; see [quotas-and-priorities](../../docs/scheduling/quotas-and-priorities.md)
   and [ADR 0005](../../docs/decisions/0005-cost-based-soft-quotas.md)).
3. Seats jobs in score order onto free capacity, honoring hard constraints and
   scoring soft preferences.
4. Submits the resulting batch of placements, accruals, and revocations to the
   coordinator leader, valid only against the snapshot's state version.

Proposals routinely fail validation because the world moved on (a node died, a
job was aborted, a competing proposal committed first). That is the normal
path: the scheduler recomputes and proposes again. Commitment through Raft is
the only thing that makes a placement real.

## What happens on a full cluster

When the farm is saturated and a significant queue builds, almost every job is
"blocked" in the trivial sense that nothing fits right now. The scheduler
does **not** react by pinning queued jobs to nodes:

- **The default state of a queued job is unpinned.** It sits in the queue and
  is seated by ordinary score-order placement the moment any node frees enough
  capacity, wherever that happens to be. No speculation about which node
  frees first is involved.
- **Accruing allocations are the license to backfill, nothing more.** When the
  scheduler wants to hand freed capacity to a job *behind* a blocked
  higher-score job, it must prove the blocked job isn't starved. That proof
  requires holding accumulating capacity somewhere concrete, so the blocked
  job gets an *accruing allocation* on chosen node(s). Only the top-K blocked
  jobs in score order hold accruals at once (K is replicated config,
  default 4). Everyone else waits, unpinned and infinitely re-plannable.
- **An accrual is a hold, not a destination commitment.** Every pass the
  scheduler re-plans: if the job's slot actually appears on another node
  first, it is seated there and the accrual is revoked — outcome `Revoked`,
  which requeues the affected attempt at no cost to the job.
- **Backfill safety never rests on runtime guesses.** A job may jump the queue
  into pledged capacity only if it has an *enforced* `max_runtime` ending
  before the accrual's `projected_ready`, a worst-case bound computed from the
  enforced `max_runtime`s of the jobs currently holding that capacity.
  Advisory estimates influence which node an accrual targets — never whether
  a backfill is safe.
- **Starvation protection comes from score dynamics, not from accruals.**
  Decayed usage keeps effective scores moving, so the protected top-K window
  rotates rather than being camped on. Declaring a tight `max_runtime` both
  lowers a job's cost and is the only way to backfill — predictability is
  rewarded twice.

See [scheduling-model](../../docs/scheduling/scheduling-model.md) for the full
model, and [ADR 0014](../../docs/decisions/0014-accruing-allocations-replace-reservations.md)
for why accruing allocations replaced standalone reservations.

## Boundaries

- Deterministic funding of accruing allocations happens in the coordinator's
  apply loop, not here; the scheduler only proposes.
- Placement policy (scoring, K, backfill aggressiveness, hysteresis) is
  deliberately outside the replicated state machine, so it can be tuned or
  rolled back without state-format concerns
  ([versioning](../../docs/architecture/versioning.md)).
- The scheduler must be able to explain every pending job: quota penalty,
  priority ordering, constraints, resource shortage, accrual, or policy
  ([observability](../../docs/operations/observability.md)).
