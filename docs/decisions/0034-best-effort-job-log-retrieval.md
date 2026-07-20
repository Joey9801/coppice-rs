# 34. Best-effort job log retrieval via an agent-hosted node service

- **Status:** Accepted
- **Date:** 2026-07-20
- **Amends:** [ADR 0031](0031-http-api-surface.md) (`GetJobLogs` leaves
  provisional status; the "Agents stay off the client edge" mechanism is
  revised), [ADR 0011](0011-container-security-posture.md) (agents gain
  one mTLS listener on the control-plane trust root)
- **Depends on:** the telemetry segment store
  ([docker-executor.md](../architecture/docker-executor.md) §8, landed),
  [ADR 0012](0012-data-retention.md) (on-node retention)

## Context

`GET /api/v1/jobs/{job}/logs` has been routed-but-`UNIMPLEMENTED` since
ADR 0031, which required the route to be "reconciled with the real log
design before leaving provisional status." The real design now has a
concrete substrate: every attempt's stdout/stderr lands in the agent's
local telemetry segment store (docker-executor.md §8), which already
exposes a tested read API (`log_chunks` by attempt, time range or tail,
per stream) and enforces purely local retention — segments are deleted
wholesale some time after the attempt ends (default 60m), earlier under
disk pressure, and a running attempt's closed segments are capped at
24h. Deletion is silent: once an attempt's segments and `ended` marker
are gone, the store answers `UnknownAttempt`, indistinguishable on the
agent from an attempt that never ran there.

There is deliberately no durable, cluster-level log store, and Coppice
must keep supporting deployments that never add one. Logs also must not
enter the raft log or the replicated `StateMachine` — they are bulk,
non-transitional data (the same reasoning that scoped measured usage
series out of ADR 0032). So the API can only ever be **best-effort**:
serve what still exists on the agent that ran the attempt, and say so
honestly when data has expired or the node is gone (e.g. decommissioned
by cluster auto-scaling).

Constraints that shaped the transport:

- Agent sessions terminate on the **leader** only, justified by the
  write path: "every inbound report must be normalized and proposed by
  the leader anyway" (coordinator-runtime.md, "Agent gateway"). That
  rationale does not apply to a read.
- Followers already serve bounded-stale reads from applied state
  (ADR 0007), and every replica can resolve job → attempts → node
  locally (`Attempt.node`, `Allocation.{job, attempt, node}`).
- We explicitly want **followers to service log requests directly** —
  not redirect to the leader, not relay through it — so that log
  traffic load-balances across replicas instead of concentrating fetch
  fan-in on the leader.
- The agent session stream is a fenced, push-only control channel with
  no request/response correlation; bulk log pages riding it would need
  a correlation broker and would contend with heartbeats.

## Decision

### Agents host a service coordinators dial: `NodeService`

Agents gain a gRPC listener: `coppice.agent.v1.NodeService`, served by
the agent over mTLS, that coordinators dial as ordinary gRPC clients.
v1 has one read-only RPC:

```protobuf
service NodeService {
  // Read a bounded page of stored log chunks for one attempt.
  rpc FetchLogs(FetchLogsRequest) returns (FetchLogsResponse);
}
```

The name is deliberately generic rather than telemetry-flavored: this
listener may over time become the natural home for more
coordinator→agent traffic (commands, event-stream subscriptions), with
the agent-dialed session relegated to a bootstrapping role. None of
that is designed or promised here — v1 is read-only — but the name
should not have to change if it happens.

`FetchLogsRequest` names `(job, attempt)`, an optional half-open
`[from_us, until_us)` time range, an optional stream filter
(stdout/stderr), a direction, an exclusive resume position
`(at_us, skip)` — `skip` being the number of chunks already consumed at
exactly that microsecond, since the store orders by `(at, insertion)` —
and hard caps (`max_chunks`, `max_bytes`). `FetchLogsResponse` is a
rich-enum `oneof` (schema-style.md): `Chunks { chunks, exhausted,
earliest_at_us, latest_at_us }` or `UnknownAttempt {}`. The RPC is a
pure translation layer over the existing `FilesystemSink` read API,
exactly as docker-executor.md §8 anticipated; it proposes nothing,
journals nothing, and never touches the session's fenced state, so it
carries no `CommandHeader` and needs no leader involvement.

The service is optional: an agent without a configured `[listen]`
listener simply never advertises an address, and its logs are
unreachable off-node — a legitimate deployment posture, reported
honestly by the API rather than treated as an error.

This deliberately amends ADR 0031's sketch (leader fetches over the
session plane). Browsers and CLI still talk **only to coordinators**;
what changes is the coordinator→agent leg, which becomes a normal RPC
any replica can make. The session plane keeps its single job: fenced,
leader-only control flow.

### Identity: same cert, both directions, id-pinned dialing

The control plane keeps its single trust root (ADR 0011). The agent's
existing leaf (subject CN = typed `node-<uuid>`) also serves as the
`NodeService` listener's server certificate; the coordinator's leaf
(already used as client identity in the raft mesh) is presented as the
client certificate, and the agent requires chain-valid client certs —
`client_auth_optional(false)`, same acceptor posture as the
coordinator's own listeners. Finer peer-role binding (coordinator vs.
operator leaves) is deferred to the OD-14/15 PKI work.

Coordinators pin the server's identity, not its network name: the TLS
server-name for the dial is set to the target's typed node id, so a
node leaf must be usable in both TLS roles and carry `node-<uuid>` as a
dNSName SAN alongside `ServerAuth` EKU. The dev PKI already mints
dual-EKU leaves and gains the SAN. That dual-role-leaf property is the
only durable constraint this ADR places on certificate issuance —
enrollment and agent bootstrapping are being redefined wholesale under
OD-15, and their mechanics are out of scope here. A stolen advertised
address is useless without the node's key: the dial fails closed on
the SAN mismatch.

### Address advertisement rides registration

Following the doctrine that authoritative addressing lives in
replicated membership, the agent advertises its `NodeService` endpoint
at registration: `Register` gains an optional `service_addr`
(`host:port`, from a new agent `[listen]` table with the bind/advertise
split as the coordinator's `ListenConfig` — `0.0.0.0` binds are never
dialable, so the advertise host is explicit). The field flows through
the existing chain — `Register` → ingestion `normalize` →
`RegisterNode` command → `Node` — and re-registration overwrites it,
so an agent restarting with a new address heals on reconnect. All
proto changes are additive (descriptor gate: additions allowed).

Absent or empty `service_addr` means "no `NodeService`"; the field is
surfaced to readers via `NodeRecord` like capacity and labels.

### The HTTP contract

`GET /api/v1/jobs/{job}/logs` is served by **every replica** from its
own applied state (eventual class, ADR 0031; `ReadQuery` honored as on
other reads). No `NOT_LEADER` outcome exists for this route.

Query parameters:

| Param | Meaning |
| --- | --- |
| `cursor` | opaque resume token, `v1:` prefixed |
| `limit` | max entries per page, 1..=1000, default 200 |
| `stream` | `stdout` \| `stderr`; absent = both |
| `attempt` | scope to one attempt id; absent = all attempts |
| `from` | RFC 3339 (≤µs precision), **inclusive** lower bound |
| `to` | RFC 3339, **exclusive** upper bound by default |
| `to_inclusive` | `true` closes the upper bound (`[from, to]`) |
| `order` | `asc` \| `desc` (default `desc`, newest first) |

Half-open `[from, to)` is the native form; `to_inclusive=true` maps to
`to + 1µs` internally (timestamps are µs-quantised). Entries are
ordered by attempt (creation order, direction-matched), then by
`(at, ingest order)` within an attempt — a deterministic concatenation
of attempts, not a global timestamp merge.

Response body:

```jsonc
{
  "entries": [
    { "attempt": "attempt-…", "at": "2026-07-20T10:11:12.123456Z",
      "stream": "stdout", "text": "…",
      "truncated": false }           // payload cut to fit the page cap
  ],
  // One record per attempt this page covered, in page order.
  "sources": [
    { "attempt": "attempt-…", "node": "node-…",
      "availability": "available",   // | expired | unreachable
                                     // | not_started
      "truncated": false,            // older lines already pruned
      "earliest_available_at": "…",  // advisory, when known
      "reason": null }               // human-readable detail
  ],
  "next_cursor": "v1:desc:attempt-…:1753003872123456:0"  // or null
}
```

Chunk bytes are decoded UTF-8-lossily into `text`; raw bytes are not
recoverable through this API. A single stored chunk larger than the
page byte cap is served **cut to the remaining budget** with
`entries[].truncated: true`, and the cursor advances past the whole
chunk — the dropped tail is permanently unretrievable through this
API. The two `truncated` fields are distinct: `entries[].truncated`
means *this entry's payload was cut to fit the page*, while
`sources[].truncated` means *older lines in the requested range were
pruned from the store*. The cursor token is
`v1:<order>:<attempt-id>:<at_us>:<skip>`, formatted and parsed in one
place like `JobCursor`; a cursor whose direction disagrees with `order`
is `INVALID_ARGUMENT`. `next_cursor: null` means the walk is complete;
a short page with a cursor means "continue" (ListJobs precedent).

### Honesty semantics

The coordinator knows, from replicated state, which attempts exist and
which node each ran on; the agent knows what data still exists. The
join of the two is the availability verdict, per attempt:

- **`not_started`** — the attempt never reached `Running`; no RPC made.
- **`available`** — chunks returned. `truncated: true` when the store's
  `earliest_at` lies inside the requested range — older lines existed
  and have been pruned (live-retention cap or disk pressure).
- **`expired`** — the node answered `UnknownAttempt`: replicated state
  proves the attempt ran there, so its telemetry has fallen out of
  retention (or telemetry was disabled/never written — on-node, these
  are indistinguishable by design, and the practical answer is the
  same: gone).
- **`unreachable`** — no advertised endpoint, dial/deadline
  failure, or the node record is gone. Covers decommissioned and
  autoscaled-away nodes; `reason` carries the detail.

A request that yields no entries at all is still `200` with the full
`sources` accounting — the query succeeded; the answer is "nothing is
retrievable, and here is why, per attempt." `404` remains reserved for
an unknown job id; `400` for malformed parameters.

### Bounded work per request

Each HTTP request makes at most **4** `FetchLogs` RPCs (attempts that
resolve without an RPC — `not_started`, no endpoint — don't count),
sequentially, each with a 5s deadline and page-derived
`max_chunks`/`max_bytes` (server-side cap ~256 KiB of chunk bytes per
page). When the RPC budget, row limit, or byte cap trips, the page ends
early with a cursor. An `unreachable` or `expired` attempt is recorded
in `sources` and the cursor **advances past it** — a dead node must not
wedge pagination; a client that wants to retry one attempt scopes with
`attempt=`. Coordinators keep a small per-node client cache and a
per-node in-flight cap so one hot job or one slow agent cannot pile up
connections. The 5s deadline bounds the **whole** fetch — the wait for a
per-node in-flight permit as well as the dial and the RPC — so a request
that queues behind a saturated permit set surfaces `unreachable` at the
deadline rather than sitting for one full deadline batch per slow call
ahead of it; that queue-wait timeout's `reason` names the saturation.
Both sides expose counters through the standing
`describe_metrics`/`gather_metrics` pattern (fetches, timeouts,
unknown-attempt answers, bytes served).

### Forward compatibility: a durable store fuses beneath this contract

When an opt-in durable log store is one day added (ADR 0012's
aspirational shipping row), it slots in **underneath** this API, not
beside it: the coordinator's fetch layer becomes a merge over two
sources — the durable store for what it holds, the live agent for what
only the agent still has — and the client sees one seamless stream
through the same route, same cursor, same response shape. The contract
is built for that fusion now:

- The cursor addresses positions by `(attempt, at_us, skip)` — content
  coordinates, not storage coordinates — so a token minted against a
  live agent read remains valid when the same range is later served
  from the store, and a single page may span both sources.
- Availability verdicts are already a join, so they degrade gracefully:
  `expired`/`unreachable` come to mean "in neither source," and ranges
  the store has absorbed simply stay `available` after the agent prunes
  or vanishes. No response field changes meaning.

What the durable store looks like, and how logs ship into it, is a
future ADR; the commitment made here is only that it must not surface
as a second API or a client-visible seam.

### Non-goals (v1)

- **Durable/cluster log storage** — a future ADR; when it lands it
  fuses beneath this contract as above, never beside it.
- **`GetNodeLogs` / `GetCoordinatorLogs`** — stay provisional; agent
  and coordinator process logs are different animals (nothing captures
  them as streams today).
- **Streaming/follow** — polling with cursors only; a live tail would
  ride the ADR 0008 subscription machinery, not this RPC.
- **Web UI reconciliation** — `LogChunk` in `web/src/api/types.ts` is
  superseded by this contract; the swap happens separately.

## Consequences

- New proto surface: `proto/coppice/agent/v1/node_service.proto`
  (service + messages), `Register.service_addr`,
  `RegisterNode.service_addr`, `Node.service_addr`; baseline
  regenerated (additions only).
- The agent binds its first-ever listening socket. Deployment guidance:
  the service port needs to be reachable from coordinators only; job
  containers gain nothing from reaching it (client certs are
  mandatory), but firewalling it from the container network is still
  good hygiene. Egress-only/NAT'd agents cannot be reached — their
  logs are simply `unreachable` off-node, which the API states plainly.
  If such deployments later need log retrieval, the recorded
  alternative (agent-dialed query streams) can be revisited.
- `coppice dev` mints the node-id SAN, picks a service port, and wires
  the advertise address, so the endpoint works end-to-end in the dev
  harness.
- Coordinator runtime gains a small, replica-local log-fetch component
  (client cache + caps) behind a `ControlPlane` seam; the API crate's
  handler swaps out the `unimplemented_id_read` stub. Reads stay open
  per ADR 0023 (any authenticated principal, once authn middleware
  lands).
- The leader-only session invariant is untouched; agent-gateway code
  does not change. Log traffic scales with replicas, not with the
  leader.
- Retention behavior is unchanged: honesty comes from joining
  replicated attempt records against the agent's answer, not from new
  tombstones on the agent.

## Alternatives considered

- **Leader-relayed fetch over the session stream** (ADR 0031's original
  sketch): followers answer `NOT_LEADER` or forward internally; a
  correlation broker matches response reports to pending requests.
  Rejected: concentrates all log traffic on the leader, adds
  request/response machinery to a deliberately push-only fenced
  channel, contends with heartbeats, and dies with every leader change.
- **Agent-dialed query streams to every coordinator**: keeps agents
  listener-free (NAT-friendly) and any replica can query over the
  standing stream. Rejected for v1: agents would need to track
  coordinator membership (static seed lists today; OD-14 explicitly
  open), maintain N standing streams, and the correlation broker
  returns. Recorded as the revisit path for egress-only deployments.
- **Coordinator-side durable log store**: complete answers, no
  dependency on node liveness — but it is exactly the persistent
  storage this project must not require, and it belongs to a future
  opt-in shipping design, not the baseline read path.
