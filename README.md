# Coppice

[![CI](https://github.com/Joey9801/coppice-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Joey9801/coppice-rs/actions/workflows/ci.yml)

Coppice is a distributed **batch job scheduler** for running containerized
workloads across a fleet of compute nodes, written in Rust.

Users submit jobs as Docker images with resource requirements, placement
constraints, priorities, and quotas. Coppice schedules those jobs onto nodes,
supervises execution through per-node agents, tracks state, and stays available
through a Raft-replicated control plane. It is built for throughput, fairness,
correctness, and debuggability rather than millisecond-level latency — a batch
system, not a serverless platform.

> **Status:** early skeleton. The workspace compiles and the architecture is
> documented, but the crates are stubs — no scheduling, consensus, or execution
> is implemented yet.

## Workspace layout

Coppice is a Cargo workspace of focused crates:

| Crate | Kind | Responsibility |
| --- | --- | --- |
| [`coppice-core`](crates/coppice-core) | lib | Shared domain model: ids, resources, jobs, nodes, epochs. |
| [`coppice-proto`](crates/coppice-proto) | lib | Wire protocol: public API and agent–coordinator messages. |
| [`coppice-state`](crates/coppice-state) | lib | Deterministic replicated state machine and its commands. |
| [`coppice-consensus`](crates/coppice-consensus) | lib | Raft integration: replication, elections, snapshots. |
| [`coppice-scheduler`](crates/coppice-scheduler) | lib | Asynchronous scheduler engine and placement proposals. |
| [`coppice-api`](crates/coppice-api) | lib | External API surface used by the UI and CLI. |
| [`coppice-coordinator`](crates/coppice-coordinator) | bin | Control-plane daemon binding consensus, scheduling, and API. |
| [`coppice-agent`](crates/coppice-agent) | bin | Node agent: container execution and reconciliation. |
| [`coppice-cli`](crates/coppice-cli) | bin (`coppice`) | Command-line client over the public API. |

## Building

```sh
cargo build      # build the whole workspace
cargo test       # run tests
cargo run -p coppice-coordinator
cargo run -p coppice-agent
cargo run -p coppice-cli
```

Requires a recent stable Rust toolchain (see `rust-version` in the root
`Cargo.toml`). The schema corpus is compiled in-process by `protox`, so no
system `protoc` is needed.

CI runs exactly these three checks — run them locally to match:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
```

## Documentation

Design and architecture documentation lives in [`docs/`](docs/) and is meant to
be iterated on over time. Good entry points:

- [Overview](docs/overview.md) — purpose, scale, and responsibilities.
- [Architecture](docs/architecture/) — components, state model, HA, versioning.
- [Design principles](docs/design-principles.md) — the ideas that hold it together.
- [Initial scope](docs/roadmap/initial-scope.md) and
  [open decisions](docs/roadmap/open-decisions.md) — what's next.
- [Decision records](docs/decisions/) — why the design is the way it is.

## License

Licensed under the Apache License, Version 2.0.
