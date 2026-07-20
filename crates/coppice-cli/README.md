# coppice-cli

The single `coppice` binary: every component behind one entry point, so a
deployment ships exactly one artifact.

- `coppice coordinator --config …` — run a coordinator replica
  ([coppice-coordinator](../coppice-coordinator)), including the hidden
  `admin` membership verbs;
- `coppice agent --config …` — run a node agent ([coppice-agent](../coppice-agent));
- `coppice dev` — a self-contained single-node dev cluster: one coordinator
  plus an in-process agent over localhost, a throwaway per-run CA (the
  production mTLS paths run unmodified, but there is effectively **no
  authentication** — localhost development only), and a temp data directory
  unless `--data-dir` pins one for restartable state. `--executor fake`
  (default) runs the job lifecycle without containers until the Docker
  executor lands;
- `coppice job …` — client commands over the public API
  ([coppice-api](../coppice-api)), the same surface the web UI is built on
  ([components](../../docs/architecture/components.md)).

> **Status:** the daemon subcommands and the four `job` verbs
> (`submit`/`status`/`logs`/`abort`) are wired against the coordinator's
> public JSON HTTP API (ADR 0031). The wider client role below — retry,
> streaming subscriptions, cluster views, policy administration — is the
> intended shape; the doc links are the specification the rest will be built
> against.

## `coppice job`

Client verbs against a running cluster's public API
([ADR 0031](../../docs/decisions/0031-http-api-surface.md)). Every verb speaks
the same JSON-over-HTTP surface the web UI is built on:

```
coppice job --api <URL> submit <spec.toml> [--job <job-id>]
coppice job --api <URL> status <job>
coppice job --api <URL> logs <job> [--stream stdout|stderr] [--attempt <id>] [--order asc|desc] [--follow]
coppice job --api <URL> abort <job> [--reason <text>]
```

`--api` is a global flag (it may appear before or after the verb) and also reads
from `COPPICE_API`; it defaults to `http://127.0.0.1:7070`. Both a bare base URL
and one ending in `/api/v1` are accepted — the `coppice dev` banner prints the
latter, so you can paste it verbatim.

- **submit** loads and validates the spec, mints a job id (or reuses `--job` for
  an idempotent resubmission — the client-minted id is the idempotency key,
  [ADR 0026](../../docs/decisions/0026-client-minted-job-ids-idempotent-submission.md)), POSTs it, and
  prints the id and the apply log index.
- **status** renders the job's current phase, spec, requests, timings, and its
  attempts.
- **logs** streams the job's output best-effort
  ([ADR 0034](../../docs/decisions/0034-best-effort-job-log-retrieval.md)),
  chronologically by default. `--follow` polls until the job is terminal.
  Attempts whose logs have expired or whose node is unreachable are noted on
  stderr rather than silently dropped.
- **abort** requests a desired-state transition (it does not synchronously stop
  the container).

### The job spec

A spec file describes a single job. It uses the same TOML conventions as the
daemon config files (`deny_unknown_fields`, humane duration strings, byte-size
units), so a typo fail-stops naming the key:

```toml
image = "busybox:1.36"
command = ["sh", "-c", "echo hello"]
# entrypoint = ["/bin/sh", "-c"]   # optional override; the image default when absent
quota_entity = "quota-00000000-0000-0000-0000-000000000001"
priority = 0            # optional, default 0 (a multiplier index; dev seeds -2..=2)
max_runtime = "1h"      # optional; whole seconds, positive

[resources]
cpu_millis = 500
memory = "256MiB"
disk = "1GiB"

[retry]                   # optional
max_retries = 3           # default 3
retry_user_errors = false # default false
```

The format is deliberately **single-job** for now; batches, arrays, and gangs
are future work.

### A dev-cluster walkthrough

```console
$ coppice dev
Coppice dev is ready
  …
  API             http://localhost:7070/api/v1 (coppice job --api http://localhost:7070 …)
  Quota entity    quota-00000000-0000-0000-0000-000000000001 ("dev", seeded; priorities -2..=2)
  …

# In another shell (the spec above, saved as hello.toml):
$ coppice job submit hello.toml
submitted job-… (log index 12)

$ coppice job status job-…
$ coppice job logs job-… --follow
$ coppice job abort job-…
```

The `--api` default already points at a local `coppice dev`, so `--api` is
optional against one.

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
