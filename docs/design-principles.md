# Design Principles

The design should follow several principles. These are the tie-breakers to reach
for when a specific decision is ambiguous.

- Keep the replicated state machine deterministic and boring.
- Keep expensive scheduling computation outside the Raft apply path.
- Commit decisions, not computations.
- Separate desired state from observed state.
- Treat agents as unreliable but reconcilable executors.
- Make every command idempotent or safely retryable.
- Use epochs or fencing tokens wherever stale leaders could cause harm.
- Persist semantic state, not telemetry noise.
- Design for explanation and debugging from the start.
- Prefer simple, correct scheduling policies initially, with clear extension
  points.
- Assume schema and behavior will evolve, and design upgrade paths explicitly.
- Avoid making rollback impossible accidentally.
- Use derived state and caches aggressively, but make them rebuildable.
- Make failure recovery a normal path, not a special case.
