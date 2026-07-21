# 36. Best-effort job usage retrieval via `NodeService.FetchMetrics`

- **Status:** Accepted
- **Date:** 2026-07-21
- **Amends:** [ADR 0031](0031-http-api-surface.md) (`GetJobUsage` leaves
  provisional status)
- **Extends:** [ADR 0034](0034-best-effort-job-log-retrieval.md) — same
  `NodeService`, same best-effort/bounded-page/honest-availability
  architecture, applied to metric samples instead of log chunks
- **Resolves:** the job half of [ADR 0032](0032-advisory-event-timestamps.md)
  item 7's scope cut ([KOI-6](../roadmap/known-open-issues.md#koi-6-nothing-records-when-anything-happened-so-no-windowed-read-can-be-served))
- **Depends on:** the telemetry segment store
  ([docker-executor.md](../architecture/docker-executor.md) §8.1, landed)

## Context

`GET /api/v1/jobs/{job}/usage` has been routed-but-`UNIMPLEMENTED` since
ADR 0031, and ADR 0032 confirmed why it could not be resolved by the
event-timestamp work: measured usage is **measurement, not transition** —
no command carries CPU/memory/disk/network samples, so no event derivation
at any granularity can produce them, and pushing periodic per-attempt
samples through consensus at the 1M-job target would be absurd. ADR 0032
scoped the measurement pipeline out as a new open decision.

That pipeline already exists on the read side: every running container is
sampled into the agent's local telemetry segment store
(docker-executor.md §8.1), on the same substrate and under the same
retention rules ADR 0034 already built a log-retrieval contract on top of.
Metrics need no new transport, trust model, or availability vocabulary —
only a second RPC and a second route riding the infrastructure ADR 0034
already justified in full. This ADR records only the deltas.

## Decision

Everything ADR 0034 decided applies unchanged: the agent-hosted
`NodeService` (mTLS, id-pinned dialing, address advertised via
registration), every replica dialing directly with no leader involvement,
bounded work per HTTP request against a small per-node client cache, and
the three-verdict-plus-`not_started` honesty vocabulary
(`available`/`expired`/`unreachable`/`not_started`) joining replicated
attempt→node knowledge against what the agent still holds. None of that is
restated here.

### `FetchMetrics` beside `FetchLogs`

`NodeService` gains a second RPC (already authored in
`proto/coppice/agent/v1/node_service.proto`):

```protobuf
service NodeService {
  rpc FetchLogs(FetchLogsRequest) returns (FetchLogsResponse);
  rpc FetchMetrics(FetchMetricsRequest) returns (FetchMetricsResponse);
}
```

Same shape as `FetchLogs` — `(job, attempt)`, half-open `[from_us,
until_us)`, an exclusive `(at_us, skip)` resume cursor, a direction, a
rich-enum `oneof` response (`Samples` or `UnknownAttempt`) — with two
deltas that follow directly from what a metric sample *is*:

- **No byte budget.** Samples are fixed-size rows (`MetricSample`, one
  struct per docker-executor.md §8.1), so a page caps on `max_samples`
  only; there is no analogue of `FetchLogsRequest.max_bytes` and no
  analogue of a chunk cut mid-payload. `Samples.exhausted` plays the same
  role `Chunks.exhausted` does.
- **No stream filter.** Logs have stdout/stderr; a metric sample is one
  row covering the whole container, so `FetchMetricsRequest` carries no
  `LogStream` equivalent.

`MetricSample` mirrors the agent's stored row field-for-field (CPU, memory,
disk, network, block-I/O — docker-executor.md §8.1). All counters are
**cumulative** (`cpu_usage_total_us`, `net_rx_bytes_total`, etc., µs for
CPU time): readers derive rates and utilization client-side, exactly as
docker-executor.md §8.1 already specifies for the stored row, so the wire
contract adds no new derivation rule. This is also why the byte-budget cut
that can truncate a log chunk mid-payload has no counterpart here — a
missed *sample* loses resolution (one fewer point on the curve), never
mass (no partial row, no invalid running total).

### The HTTP contract: `GetJobUsage`

`GET /api/v1/jobs/{job}/usage` is served by every replica, same query
surface as `GetJobLogs` (`cursor`, `limit`, `attempt`,
`from`/`to`/`to_inclusive`, `order`) minus `stream`, plus the same
`sources` per-attempt availability accounting and the same
`v1:<order>:<attempt-id>:<at_us>:<skip>` cursor shape. `limit` is
1..=5000 default 1000 — higher than logs' 200 because samples are small
fixed-size rows and a day of one attempt at the 10 s cadence is ~8 640 of
them; a default that paginates a routine chart fetch would be a default
nobody wants.

**One default flips: `order` defaults to `asc`, not `desc`.** Logs default
newest-first because an operator chasing an incident reads backward from
"now." Usage is consumed as a time series — a chart, a rate computation —
which reads forward; ascending is what every caller wants first and
newest-first would force a client-side reverse on every response.
`desc` remains a legal explicit choice.

Response body mirrors `GetJobLogsResponse`'s shape: a `samples` array of
raw samples (the cumulative counters above, verbatim — this endpoint does
not compute rates), a `sources` array with the identical availability
verdicts and semantics as ADR 0034, and `next_cursor`.

### Non-goals (v1)

- **Server-side rate computation.** Entries are raw cumulative counters;
  turning `cpu_usage_total_us` into a utilization percentage is a client
  concern, matching docker-executor.md §8.1's existing rule for the stored
  row.
- **Downsampling or rollups.** No bucketing, no decimation — a page is a
  contiguous run of stored samples, exactly as `FetchLogs` returns a
  contiguous run of chunks.
- **A durable, cluster-level usage store.** Same reasoning as ADR 0034's
  log non-goal: this is a live-agent read, best-effort by construction; a
  durable store, if one is ever built, fuses beneath this contract the same
  way ADR 0034 designed for logs.
- **`GetNodeUtilization`'s `used` half, and `GetNodeHistory`.** Node-level
  aggregation across all attempts on a node is a different query shape
  (many attempts, one node) than this per-job fetch, and remains the open
  half of ADR 0032 item 7 and KOI-6 respectively.

## Consequences

- New proto surface only: `FetchMetricsRequest`/`FetchMetricsResponse`/
  `Samples`/`MetricSample` in `node_service.proto` (already authored,
  additive per the descriptor gate). No changes to `Register`/`Node` or
  to identity, addressing, or transport — ADR 0034 already built all of
  that for the same listener.
- Coordinator runtime's replica-local fetch component (client cache,
  in-flight caps, bounded-RPC-per-request budget) is shared code with
  `GetJobLogs`, parameterized over which RPC and response shape it drives.
- This closes the job half of ADR 0032 item 7 / KOI-6: `GetJobUsage` is no
  longer `501`. `GetNodeUtilization`'s `used` half and `GetNodeHistory`
  stay open — node-level aggregation and the durable tier-2 history store
  are unresolved by this ADR.
- `coppice job usage` (CLI) is a thin client over the new route, following
  the same DTO-reuse pattern as `coppice job status`/`logs`.

## Alternatives considered

Same alternatives ADR 0034 already weighed and rejected (leader-relayed
fetch, agent-dialed query streams, a coordinator-side durable store) apply
identically here and are not re-litigated; adding a second RPC to the
already-decided transport is strictly cheaper than any alternative that
would need its own.
