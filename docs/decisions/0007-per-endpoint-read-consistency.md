# 7. Per-endpoint read-consistency defaults with client override

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-6](../roadmap/open-decisions.md#od-6-follower-read-consistency-model)

## Context

[high-availability](../architecture/high-availability.md) defines three read
categories — strong (leader read-index), stale-tolerant follower reads, and
eventually consistent reads from derived stores — but not which API endpoints
use which, nor how staleness is surfaced. Routing everything to the leader is
simple but makes it a read bottleneck; defaulting to stale breaks
read-your-writes ("submit, query, 404").

## Decision

Every read endpoint has a **default consistency class**, and any caller may
override upward with a `consistency=strong|bounded|eventual` request
parameter (downgrades below the endpoint's floor are also allowed where
harmless):

- **Strong (leader read-index)** — default for: read-after-write flows
  (fetching a job just submitted/cancelled — the write response carries the
  commit index so the client can also do a bounded read at that index),
  anything feeding automated decisions (admin/policy reads, drain status
  checks), and quota-entity configuration reads.
- **Bounded-stale follower reads** — default for: job/node/queue list and
  detail queries, quota usage views. Followers serve from their applied state
  and every response carries `applied_index` and the follower's last-known
  leader commit index, so staleness is visible, not hidden. A follower that
  cannot bound its lag (partitioned, snapshot-installing) rejects with
  redirect-to-leader.
- **Eventual (derived stores)** — default for: UI aggregates, historical/
  analytical queries, and anything served from the job-history store
  ([ADR 0012](0012-data-retention.md)).

Writes always route to the leader (forwarded internally by followers).

## Consequences

- Read load scales across replicas from day one without lying to clients:
  staleness is explicit in response metadata rather than a surprise.
- Read-your-writes holds by default on the flows where users notice.
- The API layer needs read-index plumbing, follower lag tracking, and the
  consistency parameter in its contract — modest, well-understood machinery.
- Endpoint-class assignments live with the API definitions; adding an endpoint
  means choosing its class deliberately.
