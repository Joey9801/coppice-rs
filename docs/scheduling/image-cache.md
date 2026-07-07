# Image Cache Management

Image caching should be part of scheduling policy but not dominate correctness.

Agents should report local image-cache inventory and disk pressure. The
scheduler can use this as a soft placement preference to improve startup latency
and reduce registry load.

Image cache state may be stale. The scheduler should treat cache hits as an
optimization, not as a hard guarantee unless the system explicitly supports
pre-pulled image requirements.

Agents should own local cache eviction under disk pressure. The coordinator may
provide hints or policy, but local safety must come first.

## Future work

For intelligent caching, the system may later add:

- Predictive prefetching.
- Queue-aware image warming.
- Project-specific cache policy.
- Eviction scoring based on recency, frequency, size, and expected demand.
- Registry rate-limit awareness.

The autonomy boundary was decided in
[ADR 0010](../decisions/0010-image-cache-boundary.md): agents own eviction
absolutely (LRU in v1, with images pinned while an assigned or running
allocation references them); cache inventory is reported periodically and
treated as observed state used for soft scoring only (see
[../architecture/state-model.md](../architecture/state-model.md)); central
`PrepareCache`/`EvictHint` messages are advisory and may be ignored. Nothing
cache-related enters replicated state in v1 — any future feature that makes
durable decisions depend on cache state must define exactly which facts become
replicated.
