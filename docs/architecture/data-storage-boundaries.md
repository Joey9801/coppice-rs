# Data Storage Boundaries

Different classes of data belong in different stores. Keeping these boundaries
clear is what keeps the replicated state machine small, deterministic, and fast.

- **Raft** should store authoritative control-plane state.
- **A metrics system** should store high-frequency telemetry.
- **A logging system** should store logs.
- **An event or notification layer** should handle client subscriptions.
- **A durable artifact store** may be needed for job outputs, logs, metadata, or
  diagnostics, depending on product requirements.
- **A relational or analytical store** may be useful for historical reporting,
  but it should not become a second source of truth for active scheduling
  decisions unless explicitly designed as such.

See [state-model.md](state-model.md) for the desired/observed/derived
distinction that drives these boundaries, and
[../operations/observability.md](../operations/observability.md) for what flows
into the metrics and logging systems.
