# 5. Cost-based soft quotas over a generic entity tree

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-4](../roadmap/open-decisions.md#od-4-quota-and-priority-policy)

## Context

The quota/priority policy decides who gets capacity under contention. Options
ranged from hard project caps with priority bands to full hierarchical
fair-share. Two requirements drove the decision: a quiet cluster must remain
fully usable by anyone (hard caps waste idle capacity), and the ownership
hierarchy must not hardcode particular levels (user/team/org vary by
deployment).

## Decision

### No hard limits

There is no admission-control rejection on quota grounds in v1. Quotas are
**soft**: exceeding them never blocks a job, it lowers the effective priority
of the owner's queued work. A lone user on an idle cluster gets the whole
cluster.

### A generic quota-entity tree

Quota policy is expressed over a **tree of quota entities**. Levels carry no
built-in meaning — a deployment might use org → team → user, another just
user. Each entity has: a parent, a display name, a **soft quota** (a cost
rate it is expected to stay within), and configuration is replicated policy
state. Every job is submitted under exactly one leaf entity and charges every
ancestor on its path.

### One scalar cost per job

Each job gets a single float **cost**, computed deterministically at
submission from its parameters:

```
cost = resource_cost(requests) × max_runtime × priority_multiplier(user_priority)
```

- `resource_cost` is a weighted sum over resource dimensions (weights are
  replicated policy, so e.g. GPUs can be priced high later).
- `max_runtime` makes declared runtime bounds part of the price, which also
  makes jobs backfillable ([ADR 0006](0006-reservations-and-strict-backfill.md)).
- The user-chosen priority is a **multiplier on cost**: a user can burn budget
  faster to push one important job toward the front of the queue. Priority is
  not a free lane; it is expensive.

### Decayed usage and effective priority

Each entity accumulates **decayed usage**: charged cost decays exponentially
with a configurable half-life (default 24 h), so past consumption ages out and
fairness is over a rolling horizon, not a calendar reset. Usage is charged when
a placement commits (the job's cost) and trued up on completion to reflect
actual runtime versus `max_runtime`.

Queue ordering uses an **effective score** per queued job:

```
effective_score = base(job) / Π over ancestors a of penalty(usage_a / quota_a)
```

`penalty(x)` is 1 while an entity is within its soft quota and grows
monotonically as `x` exceeds 1 — so breaching a quota degrades, gracefully and
increasingly, the priority of everything under that entity. The scheduler
simply orders candidates by effective score; ties break FIFO by submission.

### Determinism and replication

The entity tree, quota configuration, cost weights, and each entity's
`(accumulated_usage, last_update_timestamp)` pair are Raft-replicated. Decay is
computed lazily from timestamps carried **in committed commands** (the leader
stamps proposal time), never from wall clock during apply, preserving state
machine determinism. Per-job effective scores are derived state, recomputed by
the scheduler.

## Consequences

- Quiet clusters are always fully usable; contention is resolved by recent
  spend rather than static caps. Starvation is bounded: heavy users decay back
  toward parity, and light users always outrank them under contention.
- One scalar cost keeps accounting, explanation ("your team is at 3.1× its
  quota, penalty 4.2"), and the scheduler's ordering simple and cheap at
  1M-queued-job scale.
- Deployments that need true hard caps (e.g. licensed resources) don't get them
  in v1; if required later, hard limits can be added as an optional per-entity
  field without disturbing this model.
- `priority: i32` on `Job` becomes a cost multiplier input, and jobs gain a
  quota-entity reference and computed cost. Code follow-up required in
  `coppice-core`.
- Choosing `penalty()`'s shape and the default half-life will need tuning on
  real workloads; both are replicated policy, changeable at runtime.
