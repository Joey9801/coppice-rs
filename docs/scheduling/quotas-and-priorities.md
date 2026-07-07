# Quotas and Priorities

Quota and priority management is built into the scheduler rather than bolted
on later. The model was decided in
[ADR 0005](../decisions/0005-cost-based-soft-quotas.md): **cost-based soft
quotas over a generic entity tree, with no hard limits**.

## The quota-entity tree

Quota policy is expressed over a tree of **quota entities**. Levels carry no
built-in meaning — one deployment may use org → team → user, another just
user. Each entity has a parent, a soft quota (a cost rate it is expected to
stay within), and its configuration is replicated policy state. Every job is
submitted under exactly one leaf entity and charges every ancestor on its
path.

## Job cost

Each job gets a single scalar **cost**, computed deterministically at
submission:

```
cost = resource_cost(requests) × max_runtime × priority_multiplier(user_priority)
```

- `resource_cost` is a weighted sum over resource dimensions; weights are
  replicated policy (so new dimensions like GPUs can be priced).
- Declaring a tighter `max_runtime` lowers cost — and makes the job
  backfillable (see [scheduling-model](scheduling-model.md)).
- The user-chosen priority multiplies cost: users burn budget faster to push
  one important job forward. Priority is not a free lane.

## Soft quotas and effective priority

There is no quota-based admission rejection. Exceeding a soft quota never
blocks work; it lowers the effective priority of the owner's queued jobs, so a
quiet cluster is always fully usable.

Each entity accumulates **decayed usage** — charged cost with an exponential
half-life (default 24 h) — charged at placement and trued up on completion.
Queued jobs are ordered by:

```
effective_score = base(job) / Π over ancestors a of penalty(usage_a / quota_a)
```

where `penalty(x)` is 1 within quota and grows monotonically past it. Ties
break FIFO by submission time.

## Determinism and replication

The entity tree, quota configuration, cost weights, and per-entity
`(accumulated_usage, last_update_timestamp)` are Raft-replicated. Decay is
computed from timestamps carried in committed commands, never from wall clock
during apply. Effective scores are derived state recomputed by the scheduler.

## Explainability

The scheduler must be able to explain why a job is pending: quota penalty
(entity at n× its soft quota), priority ordering, constraints, resource
shortage, allocation accrual, or policy. This requirement is shared with
[../operations/observability.md](../operations/observability.md).

## Deliberately excluded from v1

Hard resource limits and preemption. Hard caps can be added later as an
optional per-entity field without disturbing this model.
