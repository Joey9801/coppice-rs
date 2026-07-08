# Architecture

This section describes the structure of the system and the invariants that keep
it correct under failure and change.

- [components.md](components.md) — the main subsystems and how they relate.
- [state-model.md](state-model.md) — desired vs. observed vs. derived state.
- [high-availability.md](high-availability.md) — the Raft-based HA model and
  determinism requirements.
- [command-catalog.md](command-catalog.md) — every replicated command and the
  apply contract (rejections, funding, ingestion boundary).
- [coordinator-runtime.md](coordinator-runtime.md) — the coordinator's task
  topology: state ownership, every channel and its bound, proposal lifecycle,
  and leader transitions.
- [versioning.md](versioning.md) — evolving the replicated state model safely.
- [schema-style.md](schema-style.md) — the protobuf schema corpus (`proto/`)
  and its evolution rules: tag discipline, representation rules, and the
  breaking-change gate.
- [data-storage-boundaries.md](data-storage-boundaries.md) — which store owns
  what.
- [storage-testing.md](storage-testing.md) — the filesystem seam, the SimFs
  crash model, the crash-injection harness and its invariants, and the
  storage benchmark/compliance gates.

## Crate map

The subsystems described here map onto the workspace crates roughly as follows:

| Subsystem | Crate(s) |
| --- | --- |
| Shared domain model | `coppice-core` |
| Serialization boundary (generated protobuf + conversions) | `coppice-proto` |
| Replicated state machine | `coppice-state` |
| Raft consensus | `coppice-consensus` |
| Scheduler engine | `coppice-scheduler` |
| External API layer | `coppice-api` |
| Coordinator daemon | `coppice-coordinator` |
| Node agent daemon | `coppice-agent` |
| CLI client | `coppice-cli` |

The web UI is not yet scaffolded; it will be built on the public API surface
exposed by `coppice-api`.
