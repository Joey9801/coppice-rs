# Suggested Initial Scope

The initial version should aim for a small but structurally correct system.

A reasonable first cut includes:

- Coordinator cluster using Raft.
- Job submission and abort.
- Basic job lifecycle.
- Node agent registration and heartbeat.
- Docker container execution.
- CPU, memory, and disk resource requests.
- Basic hard constraints using node labels.
- Simple priority queues.
- Basic quota accounting.
- Basic scheduler with heuristic bin packing.
- Assignment commitment through Raft.
- Agent reconciliation.
- Basic event streaming for job updates.
- Prometheus metrics.
- Snapshot and restart support.
- Minimal web UI or CLI for operation.

The first version should avoid overfitting to advanced features, but it should
leave room for them in the state model and APIs.

See [open-decisions.md](open-decisions.md) for the questions that must be
resolved as this scope is implemented.
