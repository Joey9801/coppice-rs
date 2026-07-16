# 31. HTTP API surface: axum + JSON DTOs on the client listener

- **Status:** Accepted
- **Date:** 2026-07-13
- **Builds on:** [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md)
  (JSON at the edge), [ADR 0007](0007-per-endpoint-read-consistency.md)
  (read consistency), [ADR 0008](0008-event-delivery-guarantees.md) (event
  cursors), [ADR 0022](0022-oidc-identity-and-authentication.md) /
  [ADR 0023](0023-scoped-role-bindings.md) (authn/z),
  [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md)
  (idempotent submission)

## Context

The web UI (`web/`) runs entirely on mock data behind the `CoppiceApi`
interface, one method per future endpoint. The CLI needs the same surface
for job commands. On the server side the seams already exist —
`coppice_api::ControlPlane` is implemented by the coordinator
(`tasks/api_server.rs`), `StateViews`/`read_index()` provide the ADR 0007
read classes, and `listen.client_addr` (default `:7070`) is parsed but
unbound — but there is no HTTP listener, no route map, no JSON codec, and
no error contract. Future sessions will implement endpoints one at a time;
without a fixed shape each one would re-litigate routing, serialization,
consistency, and error conventions.

## Decision

### Stack

The client listener is **axum** (tower/hyper), added to the workspace and
owned by `coppice-api` (crate gains an `http` module with the `Router` and
all transport plumbing). The coordinator binds it on `listen.client_addr`
in `tasks/api_server.rs` and injects its `ControlPlane`/`QueryPlane`
implementations. `coppice-net` remains gRPC-only (raft/agent/admin planes);
the client edge is deliberately a separate stack because its consumers are
browsers and curl, not fenced internal peers.

### Wire format: handwritten serde DTOs

*(Amended 2026-07-14, before the first read model shipped: the JSON
surface — read models and write bodies alike — is handwritten serde
DTOs, not the proto3 JSON mapping this ADR originally specified.)*

Every request and response body is a **handwritten, versioned serde DTO**
in `coppice-api::http::dto`, versioned with the route prefix (`/api/v1` ⇔
that module) and mirroring `web/src/api/types.ts` by name and semantics.
Proto3 JSON is protobuf's internal wire model, and exposing it would
freeze its idioms — wrapped id objects, u64-as-string, `SCREAMING_CASE`
enum names, empty lists omitted, zero messages as `{}` — into a public
compatibility commitment for browser, CLI, and REST consumers, coupling
the public contract to internal schema style. Instead the DTOs own the
JSON contract, converted to/from domain types at the HTTP boundary
(request DTOs also make required-ness explicit, where proto3 JSON treats
every field as optional); **protobuf stays canonical for internal RPC,
storage, and replication**. DTO conventions, fixed for v1: snake_case
keys (`"cpu_millis"`) and snake_case string enums (`"unknown"`,
`"oom_killed"`), ids as their bare typed strings (`"job-<uuid>"`),
instants as ISO 8601 strings (`"2026-07-16T09:30:00Z"`) and durations as
`_seconds`-suffixed JSON numbers, other integers as JSON numbers, `null`
for absent optionals, `[]` for empty lists. Time is spelled out rather
than left as a bare integer because an integer carries neither its epoch
nor its scale, so every client would have to be told what
`1760000000000000` meant; a duration has no epoch or timezone to lose,
which is the argument for stringifying an instant, so it stays a number
with the unit in the key. The web client maps snake_case wire keys onto
its camelCase `types.ts` shapes at its boundary.

The `coppice.api.v1` proto messages remain the cross-language description
of this surface (and what a future gRPC client plane would serve); the
HTTP edge no longer serializes them, and `coppice-api` no longer depends
on `coppice-proto` at all — `ControlPlane` speaks DTOs.

Read models are **not designed up front**: each endpoint's DTOs land in
`coppice-api::http::dto` in the same change that implements it. Naming is
fixed now: `<Verb><Noun>Request` / `<Verb><Noun>Response`, verbs `Get`,
`List`, `Submit`, `Abort`, `Configure`. Every response is an object
envelope (never a bare array) so fields can be added later.

### Route map

All routes sit under **`/api/v1`**. Reads are `GET` with query parameters;
mutations are `POST` with a request-message body. One route per
`CoppiceApi` method (`web/src/api/client.ts`) plus the two existing writes:

| Route | Message pair | Class (ADR 0007) |
| --- | --- | --- |
| `GET  /api/v1/session` | `GetSession*` | local |
| `GET  /api/v1/overview` | `GetClusterOverview*` | bounded |
| `GET  /api/v1/queue/stats` | `GetQueueStats*` | bounded |
| `GET  /api/v1/jobs?filter=&cursor=&limit=` | `ListJobs*` | bounded |
| `POST /api/v1/jobs` | `SubmitJob*` (exists) | write |
| `GET  /api/v1/jobs/{job}` | `GetJob*` | bounded |
| `POST /api/v1/jobs/{job}/abort` | `AbortJob*` (exists) | write |
| `GET  /api/v1/jobs/{job}/timeline` | `GetJobTimeline*` | bounded |
| `GET  /api/v1/jobs/{job}/usage?attempt=` | `GetJobUsage*` | eventual |
| `GET  /api/v1/jobs/{job}/logs?cursor=&limit=` | `GetJobLogs*` | eventual, **provisional** |
| `GET  /api/v1/nodes` | `ListNodes*` | bounded |
| `GET  /api/v1/nodes/{node}` | `GetNode*` | bounded |
| `GET  /api/v1/nodes/{node}/utilization` | `GetNodeUtilization*` | eventual |
| `GET  /api/v1/nodes/{node}/history` | `GetNodeHistory*` | eventual |
| `GET  /api/v1/nodes/{node}/logs?cursor=&limit=` | `GetNodeLogs*` | eventual, **provisional** |
| `GET  /api/v1/coordinators` | `GetCoordinatorStatus*` | local |
| `GET  /api/v1/coordinators/{id}/logs?cursor=&limit=` | `GetCoordinatorLogs*` | eventual, **provisional** |
| `GET  /api/v1/quota-entities` | `ListQuotaEntities*` | bounded |
| `GET  /api/v1/quota-entities/{entity}` | `GetQuotaEntity*` | strong |
| `POST /api/v1/quota-entities` | `ConfigureQuotaEntity*` (upsert, ADR-0023-gated) | write |
| `GET  /api/v1/events?cursor=` | ADR 0008 subscription (SSE) | **reserved** |

Path ids are the typed string forms (ADR 0024); a prefix/uuid that fails
validation is `INVALID_ARGUMENT`, not `NOT_FOUND`. **Provisional** rows
are routed now but return `UNIMPLEMENTED` until their backing store exists
(no log storage yet — `LogChunk` in the web UI is a proposal, and these
routes must be reconciled with the real log design before leaving
provisional status). The events route is reserved so nothing else claims
the path; when built it is an SSE stream of server-throttled bounded
batches with ADR 0008 cursors — never a raw firehose.

*(ListJobs signature amended 2026-07-16, when it shipped: the sketched flat
`?phase=&entity=&node=&search=` params are replaced by a single URL-encoded
JSON `filter` AST (`coppice-api::http::dto::JobFilter` — an `all`/`any`/`not`
tree over phase/entity/node/image/id/search/submitted/requests leaves), and
paging is keyset — `?cursor=` a `v1:<job-id>` token walking JobId descending
— rather than an offset, with no `total` on the response since an exact count
would force a full filtered scan.)*

The table's "message pair" naming survives the wire-format amendment
unchanged: the pairs are the same-named DTOs in
`coppice-api::http::dto`, with the `coppice.api.v1` messages as their
cross-language mirror.

### Consistency plumbing (ADR 0007 made concrete)

- Every read accepts `?consistency=strong|bounded|eventual`, overriding
  the endpoint default upward or downward as ADR 0007 allows. The
  parameter contract is mechanical, not per-handler discipline: shared
  extractors (`IdPath`, `ReadQuery` in `coppice-api::http`) validate the
  typed path id and the read parameters on **every** read route, stubs
  included — a malformed id or a bogus `consistency` value is
  `INVALID_ARGUMENT` even on endpoints that answer `UNIMPLEMENTED`, and a
  real handler inherits the extractors rather than re-adding validation.
  The staleness headers below are attached through a typed response part
  (`ReadIndexes`) for the same reason.
- Every read accepts `?min_index=N`: serve from a view with
  `applied_index ≥ N` (`StateViews::at_least`), the read-your-writes pair
  for a write response's `logIndex`.
- `strong` = `Consensus::read_index()` then `at_least(that index)`;
  `bounded` = `StateViews::latest()` with staleness surfaced;
  `eventual`/`local` = whatever derived store or local watch backs the
  endpoint.
- Every response carries `Coppice-Applied-Index` and
  `Coppice-Committed-Index` headers (decimal). Headers rather than a body
  envelope keep bodies pure response messages.
- Writes always execute on the leader. v1 behavior on a follower is a
  `NOT_LEADER` error; the `Coppice-Leader` hint header, when present,
  carries the leader's advertised **client-API address** — a value the
  caller can actually retry against, never an internal identifier such as
  the raft `CoordinatorId`. Today no producer can supply that address
  (raft membership records only the peer-plane address), so the header is
  simply absent and clients must fall back to trying their configured
  coordinator list. Closing this properly means either advertising client
  addresses through membership or internal forwarding (ADR 0007's end
  state); both slot in without changing the contract, since clients that
  handled the hint-less error keep working.

### Error contract

Errors are `application/json`:

```json
{ "code": "NOT_FOUND", "message": "job job-… does not exist" }
```

`code` is a closed vocabulary with fixed status mapping:

| code | HTTP | source |
| --- | --- | --- |
| `INVALID_ARGUMENT` | 400 | synchronous validation, bad id syntax, `ApiError::Invalid` |
| `UNAUTHENTICATED` | 401 | missing/invalid credential (ADR 0022) |
| `PERMISSION_DENIED` | 403 | role-binding check (ADR 0023) |
| `NOT_FOUND` | 404 | id not in the read view |
| `REJECTED` | 409 | committed-and-refused at apply (`ApiError::Rejected`) — normal race outcome, carries the `RejectionReason` text |
| `NOT_LEADER` | 421 | write hit a follower; `Coppice-Leader` header when known |
| `UNAVAILABLE` | 503 | write didn't resolve; follower can't bound staleness |
| `UNIMPLEMENTED` | 501 | provisional/reserved route |
| `INTERNAL` | 500 | bug; details logged, not leaked |

This is the JSON rendering of `coppice_api::ApiError` plus the read-side
codes; the web client maps it onto its `ApiError` at its boundary.

### AuthN/Z

`Authorization: Bearer <OIDC JWT>` validated offline per ADR 0022; the
authn middleware resolves it to the `Actor` that every proposed command
already carries, and `GET /api/v1/session` echoes the resolved principal.
Until the middleware lands, the listener is open and `session` returns the
static dev principal (matching the web UI's "Demo User" stub) — the
middleware is a seam in `coppice-api::http`, not a redesign. Operator-cert
break-glass (ADR 0023) authenticates on the mTLS admin plane, not this
listener. TLS on the client listener follows the deployment posture
(terminate here via the node-config `[tls]` server cert, or in front of
it); it is config, not contract.

### Serving the UI

The same listener serves `web/dist` (per `web/README.md`): static assets
at `/`, SPA fallback to `index.html` for client routes, `/api/v1` reserved
for the API — same-origin, no CORS, one port. Mechanism: `rust-embed` over
`web/dist` as the router fallback (`/api/*` misses keep the JSON error
contract). Release builds embed the assets in the binary; debug builds
read the folder from disk per request, so `coppice dev` picks up a fresh
`npm --prefix web run build` without recompiling. `web/dist` stays a
gitignored npm product — a clean checkout compiles without Node, and the
source tree is never written at build time (it may be read-only under
packaging systems): `coppice-api`'s build script points the embed at
`web/dist` when it exists and at an empty `OUT_DIR` placeholder otherwise,
in which case UI paths 404 with the build command. Vite's content-hashed `assets/` get
`immutable` caching; entry points revalidate.

### Agents stay off the client edge

Browsers and CLI talk **only to coordinators**. Agents get no HTTP
listener: usage samples already ride the mTLS agent-session stream
(heartbeat reports), and job logs — when log storage is designed — will be
fetched by the coordinator over that same fenced plane and re-served under
`/api/v1/jobs/{job}/logs`. This keeps the ADR 0011 security posture (one
authenticated ingress per plane) and spares agents a second identity.

## Consequences

- Future sessions implement one endpoint per change: response DTOs →
  projection → handler swap-in for the `UNIMPLEMENTED` stub → flip the
  matching method in `web/src/api/index.ts`. Routing, consistency,
  errors, and auth are already decided and mechanically enforced by the
  shared plumbing in `coppice-api::http`.
- axum/tower enter the workspace. JSON compatibility is owned entirely
  by the DTO module, where a rename is a deliberate contract change, not
  a schema side effect; `coppice.api.v1` field renames no longer touch
  the JSON surface (schema-style rules still apply to the protos as the
  cross-language mirror). The pbjson codegen pass over the api/core
  packages now has no production consumer and can be retired.
- Read models get frozen shapes only as their UIs stabilize, instead of
  speculatively today; the cost is that `web/src/api/types.ts` remains
  the reference shape until each DTO lands.
- The `NOT_LEADER`-with-hint compromise means dumb clients need retry
  logic until internal forwarding is built; the error contract already
  accommodates that upgrade invisibly.
