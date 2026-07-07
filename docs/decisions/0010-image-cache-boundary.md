# 10. Image cache: agents own eviction, center gets observed state and gives hints

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-9](../roadmap/open-decisions.md#od-9-image-cache-autonomy-vs-central-hints)

## Context

Image locality improves startup latency and reduces registry load, but local
disk safety must always win, and cache detail must not bloat replicated state
(see [image-cache](../scheduling/image-cache.md),
[state-model](../architecture/state-model.md)).

## Decision

- **Agents own eviction, absolutely.** Under disk pressure an agent evicts on
  its own authority (LRU in v1), never blocking on the coordinator. Images
  referenced by assigned or running allocations are pinned and never evicted.
- **Cache state is observed, never replicated.** Agents report a summarized
  inventory (image digests, sizes, last-use) in periodic reports. The
  scheduler consumes this as observed state for **soft scoring only** — a
  cache hit is an optimization, never a correctness assumption. Nothing about
  caches enters the Raft state machine in v1.
- **Central hints are advisory.** The coordinator may send
  `PrepareCache` (prefetch) and `EvictHint` messages; agents may ignore both
  freely. No scheduling decision depends on a hint having been honored.

Revisit only if a future feature makes durable decisions depend on cache state
(e.g. reservations contingent on prefetch completion) — that feature's ADR
must then define exactly which cache facts become replicated.

## Consequences

- The replicated state machine stays free of high-churn cache data; a node
  with a full disk can always save itself.
- Cache-aware placement can be wrong at worst by a redundant image pull —
  latency, not correctness.
- Prefetch/warming/eviction-scoring remain open future work behind an
  already-defined boundary.
