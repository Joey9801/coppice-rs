# coppice-net

The tonic client/server stubs for every Coppice gRPC surface. It is a thin
codegen crate: it generates only the service glue, not the message types.

## What it is

`coppice-net` is where the tonic/hyper transport stack enters the build graph.
It merges what were previously two separate tonic stub crates into a single
home for all generated service code, covering:

- **`coppice.raft.v1`** — the coordinator↔coordinator Raft transport
  (`RaftTransportService`: AppendEntries, Vote, streaming InstallSnapshot;
  [ADR 0002](../../docs/decisions/0002-openraft-with-custom-segment-storage.md))
  and the membership admin surface (`RaftAdminService`: add-learner,
  promote-voter, remove-node, cluster-status;
  [ADR 0016](../../docs/decisions/0016-coordinator-rebuild-learner-join.md)).
- **`coppice.agent.v1`** — the agent↔coordinator session (`AgentService`: one
  long-lived bidirectional stream per agent, reports up and commands down;
  [ADR 0009](../../docs/decisions/0009-fencing-and-reconciliation.md), see
  [agent-coordinator](../../docs/protocols/agent-coordinator.md)).

## How it works

Message types are **not** generated here. They are owned exclusively by
[`coppice-proto`](../coppice-proto), the single owner of the schema corpus
([ADR 0003](../../docs/decisions/0003-protobuf-serialization-and-cluster-version-gates.md)).
The `build.rs` compiles the whole `proto/` corpus with `protox` (pure Rust, no
system `protoc`), then hands the descriptor set to `tonic-build` with every
message package `extern_path`ed to `coppice_proto::pb`. So prost regenerates no
message structs — the only output is the client/server code for the services.

That split is load-bearing. `coppice-proto` is a prost-only dependency of the
deterministic core (state machine, storage formats), so the transport stack
enters the build only for processes that actually open sockets. Domain
conversions stay with the endpoints ([`coppice-consensus`](../coppice-consensus)
for openraft, the coordinator gateway and the agent for the session), keeping
transport-library types out of this crate entirely.

## Layout

- `pb` — the raw tonic output, one module per proto package that defines a
  service. Nothing here redefines a schema type.
- `transport`, `admin`, `session` — re-export the client, server, and
  server-trait types under stable, readable aliases (`Client`, `Server`, and
  the service trait).

## Boundaries

- No transport wiring lives here: no TLS setup, connection pooling, retry, or
  channel construction. mTLS between nodes and enrollment is a coordinator/agent
  concern
  ([ADR 0011](../../docs/decisions/0011-container-security-posture.md)); the
  coordinator's task and channel architecture is in
  [coordinator-runtime](../../docs/architecture/coordinator-runtime.md).
- No message types, no domain logic — those belong to `coppice-proto` and the
  endpoint crates respectively.
