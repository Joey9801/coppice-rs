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

Only cache metadata that affects durable scheduling or policy decisions needs to
enter the replicated state. Detailed cache contents can be reported periodically
and treated as observed state (see
[../architecture/state-model.md](../architecture/state-model.md)).

The boundary between local agent autonomy and central scheduling hints is an
[open design decision](../roadmap/open-decisions.md).
