# coppice-cli

The `coppice` command — the user- and operator-facing client for driving a
Coppice cluster from the terminal. It is a thin client over the public API
([coppice-api](../coppice-api)), the same surface the web UI is built on
([components](../../docs/architecture/components.md)).

> **Status: skeleton.** `src/main.rs` today is a placeholder `main` with no
> argument parsing and no commands wired up. The role below is the intended
> shape; the doc links are the specification it will be built against.

## Role

`coppice` is the client end of the control plane. It does not hold state or
make scheduling decisions — it translates human intent into public-API
requests against a coordinator replica, which routes mutations to the Raft
leader and applies them as durable state transitions
([components](../../docs/architecture/components.md)).

Intended command surface, following the initial scope
([initial-scope](../../docs/roadmap/initial-scope.md)):

- **Jobs** — submit, abort, retry, and query job status and lifecycle history.
- **Cluster views** — query node, queue, and quota-usage status.
- **Streaming** — subscribe to job and queue updates, reconnecting by cursor
  ([event delivery, ADR 0008](../../docs/decisions/0008-event-delivery-guarantees.md)).
- **Policy administration** — `coppice-cli cluster init --policy …` seeds
  initial replicated policy exactly once at cluster creation, and
  `coppice-cli policy …` reads and updates it at runtime. Each change is a
  committed Raft command, and the CLI performs the human-friendly →
  replicated-representation conversions (decay half-life, quota rates) so the
  state machine never has to
  ([configuration](../../docs/operations/configuration.md),
  [ADR 0019](../../docs/decisions/0019-deterministic-quota-arithmetic.md)).

## How this differs from `coppice-coordinator`

Two binaries expose a CLI, and they are not the same thing:

- **`coppice-coordinator`** is the coordinator *daemon*. Its CLI is a
  deliberately tiny startup shell — `--config`, `--bootstrap` / `--join` — that
  loads a per-node TOML file and runs the server process
  ([ADR 0020](../../docs/decisions/0020-node-config-vs-replicated-policy.md)).
- **`coppice`** (this crate) is a *client*. It runs anywhere, holds no server
  state, and talks to a running cluster over the public API to submit and query
  work and to administer replicated policy.

The bright line: node-local process settings live in the coordinator's config
file; anything replicas must agree on is replicated policy, changed only
through this CLI ([configuration](../../docs/operations/configuration.md)).

## Boundaries

- No business logic lives here. Validation, authorization, and durable state
  transitions are the API and coordinator's responsibility; the CLI is a
  presentation and transport shell over them.
- Job submission and abort are modelled as desired-state transitions committed
  through Raft, not as imperative commands to workers
  ([components](../../docs/architecture/components.md)).
