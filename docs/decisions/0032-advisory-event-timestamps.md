# 32. Advisory event timestamps and the observability retention line

- **Status:** Accepted
- **Date:** 2026-07-15
- **Amends:** [ADR 0008](0008-event-delivery-guarantees.md) (payload shape),
  [ADR 0012](0012-data-retention.md) (history-store contents),
  [ADR 0031](0031-http-api-surface.md) (per-field consistency of windowed
  reads)
- **Resolves:** [KOI-6](../roadmap/known-open-issues.md#koi-6-nothing-records-when-anything-happened-so-no-windowed-read-can-be-served)
  — the design half; the issue stays open until its closure criteria are met
  in code

## Context

KOI-6 records that no time-ranged read the API promises can be served:
`coppice_state::Event` carries no timestamp (apply may not read a clock),
nothing retains events (the fanout ring is a reconnection buffer, the history
store a stub), and replicated state records facts, not transitions. The
overview's queue rates are `null`, its history `[]`, `recent_events` does not
exist on the wire, and `GetJobTimeline`, `GetNodeHistory`, `GetNodeUtilization`,
`GetJobUsage`, and `SubscribeEvents` are `501`.

Three facts, verified in source, anchor this decision:

- **Every command carries exactly one proposer stamp.** All fifteen `Command`
  variants have a single `*_at_us` field, stamped by the coordinator task that
  builds the command (API server, ingestion normalizer, scheduler driver,
  housekeeping) — never by a client, and never by apply. Nested sub-items
  (`Placement`, `LostAttempt`, `AllocationSpec`, the eviction job list) carry
  no stamp of their own.
- **Events are ephemeral.** `Event`/`EventBatch` appear nowhere in the log
  entry encoding or the snapshot sections. Attaching a timestamp to them costs
  zero bytes of durable format by construction.
- **Batches are one-per-command** (KOI-3), tagged with the command's own log
  index, and the emitted stream is a pure function of the committed log.

Because a command has one stamp and a batch is one command, per-event and
per-batch stamping are the same value; the batch-level placement is simply
cheaper, and it forces the nested-item answer (sub-items inherit the batch's
stamp).

One promise in KOI-6's text does not survive analysis. Measured usage series —
`GetJobUsage`'s cpu/memory/disk samples and the `used` half of node
utilization — are **measurements, not transitions**. No command carries them
(the only measurement anywhere in the catalog is `actual_runtime_us`, a
scalar), so no event derivation at any timestamp granularity can produce them,
and pushing periodic per-attempt samples through consensus at the 1M-job
target would be absurd. They need an off-consensus measurement pipeline,
which is out of this decision's scope.

Alternatives considered and rejected:

- **Let apply read a clock.** Breaks the determinism contract outright;
  replicas would derive different streams from the same log.
- **Replicate derived series** (bucketed rates, rolling windows) in the
  `StateMachine`. Violates the standing rule against derived state on the
  replicated struct, grows every snapshot, and turns a rendering concern into
  a consensus concern.
- **Gate raft-log purge on history-store progress** to guarantee gap-free
  timelines. A dead Postgres would then grow the log without bound: ADR 0012
  declares history a sink whose loss degrades history, never correctness, and
  purge-gating inverts that.
- **Clamp skewed stamps to monotonic at ingest.** The stored record would
  diverge from what every replica deterministically derives, and
  re-derivation would no longer be idempotent. Clamping is a consumer-side
  concern, exactly as ADR 0019 (quota ticks) and ADR 0021 (age term) already
  treat proposer skew.

## Decision

1. **Batch-level advisory stamp.** `EventBatch` gains `at_us: i64`, populated
   by the apply loop from a new exhaustive accessor
   `Command::stamped_at_us(&self) -> i64` (a `match` with no wildcard arm, so
   a future command without a timestamp fails to compile). The `Event` enum,
   `Applied`, and every emit site in apply are untouched. The stamp is
   **advisory**: apply never reads it back, nothing branches on it, and it is
   never an ordering key — the log index remains the order (KOI-3).

2. **Semantics: proposer-asserted time.** `at_us` means "when the proposer
   asserted this fact," not "when it physically happened." Sub-items inherit
   the command's stamp: every `LostAttempt` in a `ReconcileNode` is stamped
   at the report's `observed_at_us` even though each attempt died earlier at
   an unknown instant, and all ~N events of a batch command
   (`CommitPlacements`, `EvictTerminalJobs`) share one stamp. This
   flattening is documented behavior; no consumer may interpolate or
   "correct" it.

3. **Skew: store raw, order by index.** Stamps come from different replicas'
   clocks (submit from the API-serving replica, dispatch from the leader), so
   `at_us` regressing as the index advances is normal, not exceptional. The
   record keeps the raw value everywhere. Every wire event carries
   `(index, ordinal, at_us)` — `ordinal` is the event's position within its
   batch, deterministic per KOI-3 — and **all ordering, rendering, and
   deduplication key on `(index, ordinal)`, never on `at_us`** (the web UI's
   current timeline sort and feed dedup key must change accordingly).
   Windowing consumers may clamp locally when bucketing. The fanout exports a
   `proposer_skew` measurement (|`at_us` − local receipt time|) via the
   standard `describe_metrics()`/`gather_metrics()` pattern so a
   misconfigured coordinator clock is an alert, not a support ticket.

4. **The retention line.** Three tiers, each serving what it is shaped for;
   no fourth place to put event data may be introduced:

   | Tier | Contents | Bounds | Serves |
   | --- | --- | --- | --- |
   | Fanout ring (exists) | `EventBatch` incl. `at_us` | 1 h / 1M events, reconnection buffer, unchanged | `SubscribeEvents` replay; a bounded most-recent cache for the overview's `recent_events` |
   | History event table (new, in the ADR 0012 store) | one row per event: `(applied_index, ordinal)` PK, `at_us`, kind, scope keys, payload | 90 days, daily partitions dropped whole | `GetJobTimeline`, `GetNodeHistory`; the durable substrate for any later windowed projection |
   | Derived stats (new, in-memory per replica) | 30 s rolling buckets: queue arrivals/drains counted from the event stream; utilization-`allocated` and quota usage sampled from published views | ≤ 1 h of buckets, task-local, never on `StateMachine`, never snapshotted | queue `history` and both rates; `GetNodeUtilization` (allocated); `QuotaEntityStats.usageHistory` |

   One wire shape — the timeline event carrying `(index, ordinal, at_us)`
   plus kind and scope fields — is shared by the overview's `recent_events`,
   `GetJobTimeline`, and the ADR 0008 subscription payload. No endpoint
   invents its own event shape.

5. **History pipeline: best-effort, honest gaps, guaranteed backstop.** A
   leader-scoped history-writer task consumes the event stream and batch-
   inserts rows. Rows are idempotent (`(applied_index, ordinal)` PK, inserts
   on conflict do nothing), so overlap after a leader change is harmless. The
   writer's durable cursor lives **in the history store**, so a new leader
   resumes from wherever the old one durably reached, backfilling from its
   own ring (every replica derives identical batches; the missing span is
   present locally if within the ring window). When the writer cannot cover a
   span — tap overflow, restart beyond the ring window, a store outage longer
   than an hour — it writes an **explicit gap row** (`from_index`,
   `to_index`), and the timeline renders the hole; it never serves a silently
   smooth line across one. The pipeline never blocks apply, never gates log
   purge, and never fails a proposal: exactly ADR 0012's sink. The one
   *blocking* history write in the system remains KOI-1's terminal-record
   write sequenced before `EvictTerminalJobs`, which guarantees final
   outcomes durably even where intermediate transitions gapped.

6. **Honest absence gets a vocabulary.** A window that is not retained is
   absent (`null`), never `0` — unchanged. A window that is *partially*
   covered now says so: `recent_events` carries the coverage floor (the
   ring's earliest-available index), and bucketed series mark buckets that
   predate the process or a gap as missing rather than zero. An empty ring on
   a freshly restarted coordinator is thereby distinguishable from a quiet
   cluster.

7. **Scope cut: measured usage series.** `GetJobUsage` and the `used` half of
   `GetNodeUtilization` are not resolvable by this decision and stay
   honestly `501`, with the measurement pipeline (agent-reported samples,
   off-consensus, likely landing in the history store or the metrics stack)
   recorded as a new open decision. KOI-6's affected-capability list narrows
   accordingly.

8. **Consistency re-class.** ADR 0031's table gains per-field annotations: an
   endpoint keeps its class for point-in-time fields, while embedded windowed
   fields are **derived** — served from tier 1 or 3 (replica-local, coverage-
   annotated) or tier 2 (eventual per ADR 0007). `GetJobTimeline` moves from
   bounded to eventual with a bounded *tail*: the response is the store's
   prefix merged with the local ring above the durable cursor, so a live
   job's newest transitions appear without waiting for the writer.

**Properties, stated for tests:**

- **(T1) Determinism.** An identical committed log yields an identical
  `(index, ordinal, at_us, event)` sequence on every replica, under any
  apply batching (extends `event_stream_is_invariant_under_apply_batching`).
- **(T2) Advisory.** State after apply is a function of `(state, command)`
  only; the event stamp is attached outside `apply()` and no code path reads
  it back into a decision.
- **(T3) Honest gaps.** A forced writer outage spanning committed commands
  produces a gap row whose bounds cover exactly the unwritten span, and the
  served timeline for an affected job includes the gap; after the outage the
  cursor advances and rows resume with no duplicates.
- **(T4) Handoff.** Writer resumption on a different replica from the stored
  cursor, within the ring window, produces the same table as no handoff.

## Consequences

- **Apply, log, and snapshot pay nothing.** The stamp is one `i64` copy per
  command in the apply loop; commands already carry the value in the log, and
  events appear in neither the log nor the snapshot, so both formats are
  byte-identical to today. The ring grows 8 bytes per batch against a
  worst case of tens of MB; the SSE payload grows ~15 bytes per event.
- **The store must be budgeted, not assumed.** A job lifecycle emits roughly
  8–12 events; at a sustained 1M jobs/day that is ~10M rows (~1–2 GB)/day and
  ~1B rows at the 90-day horizon. Daily partitions with drop-based retention
  and indexes on `(job_id, applied_index)` and `(at_us)` are part of the
  schema from day one, and the writer inserts in batches.
- **Hot-path reads never touch SQL.** The 2-second-polled overview and queue
  stats are O(recent-N) and O(buckets) reads from replica-local caches; only
  the per-job timeline and node history pay a store query, at eventual class.
- **Timelines are deliberately best-effort-complete.** An operator can see an
  explicit gap after a store outage; what they can never see is a fabricated
  continuity or a lost final outcome (the blocking terminal write survives).
  This is the same trade ADR 0008 makes for subscribers, extended to the
  durable record.
- **Rendered time can disagree with rendered order.** Cross-proposer skew
  means a timeline ordered by index may show a locally decreasing `at_us`;
  that is the truth of the record, surfaced instead of laundered. The
  `proposer_skew` metric makes chronic skew an operational signal.
- **Derived stats restart empty.** Rolling buckets are per-replica and
  in-memory; a restarted or newly-led coordinator serves a partially covered
  window (honestly marked) until it refills. Backfilling buckets from the
  event table is possible later without changing any contract.
- The web UI changes sort/dedup keys from `atUs` to `(index, ordinal)`, and
  the mock's `TimelineEvent` gains those fields before the real client swap.
- What this does **not** unblock stays visibly unblocked: measured usage
  series wait on the measurement-pipeline decision, and container logs remain
  provisional per ADR 0031.
