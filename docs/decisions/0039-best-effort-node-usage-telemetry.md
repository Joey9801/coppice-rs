# 39. Best-effort node usage telemetry

- **Status:** Accepted
- **Date:** 2026-09-01
- **Amends:** [ADR 0031](0031-http-api-surface.md) (`GetNodeUtilization`
  leaves provisional status; `NodeSummary.used` and `ClusterCapacity.used`
  become nullable)
- **Extends:** [ADR 0009](0009-agent-coordinator-protocol.md) — heartbeats
  gain one observed-only field, peeled off at the same ingestion boundary
  that keeps agent reports out of the log
- **Sibling of:** [ADR 0032](0032-advisory-event-timestamps.md) tier 3 —
  the same replica-local rolling-bucket shape, for a *measured* quantity
  rather than a counted one
- **Distinct from:** [ADR 0036](0036-best-effort-job-usage-retrieval.md) —
  that answers "what did *this job* consume", from the agent's durable
  segment store, on demand; this answers "what is *this node* consuming
  right now", from a live in-memory window

## Context

Two surfaces have been dishonest since ADR 0031 routed them. The overview's
`ClusterCapacity.used` and every `NodeSummary.used` were hardcoded to
`Resources::ZERO` with a comment promising agent telemetry, and
`GET /api/v1/nodes/{node}/utilization` answered `501 UNIMPLEMENTED`, so the
node detail page's utilization panel had nothing to draw. A cluster running
flat out reported zero consumption, which is worse than reporting nothing:
a zero is a claim.

The measurement itself was never the hard part. Every running container is
already sampled by the docker executor
([docker-executor.md](../architecture/docker-executor.md) §8.1) for the
telemetry segment store ADR 0036 reads. What was missing was a node-level
fold of those samples, a way to carry it to a coordinator, and a decision
about what to *do* with it once there — because the obvious answers are all
wrong:

- **Replicating it** would push a periodic measurement through consensus for
  every node, forever, to store a number that is stale by the time it
  applies. ADR 0032 already rejected this shape for per-attempt samples; it
  is no better per-node.
- **Persisting it** to the coordinator's disk would build a second,
  worse time-series database next to the one every operator already runs.
- **Deriving it from events** is impossible for the reason ADR 0032 gives:
  measurement is not transition. No command carries a CPU reading.

## Decision

Node usage is **best-effort, observed-only telemetry**: measured by the
agent, reported on the heartbeat, held in coordinator memory, and exported
to Prometheus. It is never a replicated fact.

### `used` is job-attributable on all three dimensions

The agent folds the per-container samples it already collects — it does not
read a host-level total. CPU is the summed rate over the last two readings
per live container, memory the summed current resident set, and disk the
summed per-allocation usage, which **includes the image half** as
docker-executor.md §6.2 defines usage (image + writable layer), because that
is the same quantity the scheduler funds.

That choice is what makes the number worth showing. A node's `used` is
directly comparable to its `allocated`: both are "what this cluster's work
is doing on this node", so `used < allocated` reads as slack and
`used > allocated` reads as overcommit, with no host baseline to subtract
first. A host-level reading would have been cheaper to collect and
meaningless to compare.

Cost and quota are untouched: pricing stays on funded resources (ADR 0019),
and nothing here is priced, ever.

### Absence is not zero

A `NodeUsage` absent from a heartbeat means "not measured". It never means
zero, and no layer substitutes one:

- The proto field is `optional`; the agent omits it when its executor has
  nothing fresh.
- The coordinator's sink drops readings older than `USAGE_SAMPLE_MAX_AGE`
  (90 s, matched to the ADR 0009 liveness deadline — a reading that outlives
  the deadline has no claim to describe anything current).
- `NodeSummary.used` and `ClusterCapacity.used` are `null` when nothing is
  reported. A rolling-window bucket carries `used: null` for a node that
  reported nothing during it, which the charts render as a **gap**.
- The `coppice_node_used_*` gauges are simply not emitted for a
  non-reporting node.
- A cluster total is a *partial* sum, and every bucket carries
  `reporting_nodes` / `total_nodes` so it cannot be mistaken for a total.

### In-memory, leader-only, one rolling hour

The coordinator keeps a `Mutex<BTreeMap<NodeId, sample>>` sink — the same
shape as the ADR 0009 liveness map, and for the same reason: ingestion
writes it and read handlers read it with no `.await` edge either way, so the
deadlock-freedom argument in
[coordinator-runtime.md](../architecture/coordinator-runtime.md) is
unchanged. A usage-history task closes a bucket every 30 s
(`USAGE_BUCKET_INTERVAL`) holding each node's `{capacity, allocated, used}`
plus a cluster bucket, retaining 120 of them — one hour
(`USAGE_WINDOW_MAX_BUCKETS`), the same budget ADR 0032 gave the queue
window. Nothing here is on the `StateMachine`, in the log, or in a snapshot,
and none of it survives a restart.

Because agent sessions terminate on the leader, only the leader has samples.
A follower serves the same endpoints with an empty snapshot, so its `used`
is `null` and its windows are empty — honestly absent, not wrong. There is
deliberately **no read-forwarding** for these reads: forwarding a dashboard
poll to the leader to fetch an unreplicated number would put leader load on
every viewer, and "the follower shows no usage" is a correct answer under
the absence rule this ADR already commits to. If it becomes a real problem,
the fix is to point dashboards at `/metrics`, which is where the retained
answer lives anyway.

### Prometheus owns retention

The one-hour window exists to draw a chart. Long-term retention is not the
coordinator's job, and the coordinator's `/metrics` endpoint (issue #46)
already exists: this ADR adds `coppice_node_used_*`,
`coppice_node_allocated_*`, `coppice_node_capacity_*`,
`coppice_node_usage_sample_age_seconds`, and the cluster totals, all through
the repo's `describe_metrics()` / `gather_metrics()` pattern.

These are the tree's first *sampled* gauges on the coordinator — every other
module's `gather_metrics` is a no-op because its metrics are pushed at the
event that changes them, and a usage fold has no such event. The
node-labelled series are **not** garbage-collected when a node leaves: a
departed node's last label set lingers until the process restarts.
Prometheus's own staleness handling is the answer; a bespoke label-GC layer
would only add a way for the two to disagree.

The `agent_node_used_*` gauges the agent exports (its own fold, before any
coordinator sees it) keep the `agent_` prefix precisely so `coppice dev` —
one process, one recorder, both roles — cannot collide them.

## Consequences

- The capacity card and the node utilization panel show real numbers, and
  the `used: Resources::ZERO` placeholder is gone from the projection.
- `NodeSummary.used` and `ClusterCapacity.used` are now nullable in the DTO
  and in `types.ts`; every consumer must render absence rather than
  formatting a zero. This is a breaking change to the read shape, which
  costs nothing today (there are no deployments) and is the whole point.
- `GET /api/v1/nodes/{node}/utilization` leaves provisional status. It still
  404s an unknown node — an unknown node and a known-but-uncollected one are
  different answers.
- A coordinator restart or a leadership move loses the window. The chart
  refills over the next hour, showing honest absence in the meantime. This
  is accepted: the durable copy is in Prometheus.
- The heartbeat grows by one small message per interval, and ingestion does
  one map insert per heartbeat. No daemon call is made on either the
  heartbeat or the scrape path.
- `used` is observed-only by construction, not just by convention: it is
  peeled off *outside* the normalizer, whose only outputs are log commands
  and stop routes, so there is no path by which it could become a command.
- Nothing here changes scheduling. The scheduler funds and places against
  `allocated`; `used` is for humans and for Prometheus. A future decision
  could feed measured usage back into placement, and would be its own ADR.
