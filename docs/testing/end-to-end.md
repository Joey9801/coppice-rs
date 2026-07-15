# End-to-end test register

Behaviours that have only ever been checked by driving a real cluster by hand.
Each entry is written so an agent can execute it from this file alone, and so
it can later be lifted into an automated integration suite with no
re-derivation. Until that suite exists, this register **is** the suite: an
agent touching an area below re-runs its cases before opening a PR.

Cases here are end-to-end only — a real coordinator, a real agent, over HTTP.
Anything provable in a unit or crate integration test belongs there instead;
this file is expensive to run and must stay small.

## Fixture

Unless a case says otherwise, every case runs against one shared cluster:

```
cargo run -q -p coppice-cli -- dev --executor fake
```

`dev` prints a ready summary with the client-API port (ephemeral), the cluster
id, the registered agent's node id, and a seeded quota entity
(`quota-00000000-0000-0000-0000-000000000001`, priorities -2..=2). Export
`API=http://localhost:<client-port>/api/v1` and reuse the same cluster for a
whole run; cases that mutate state say so and are ordered accordingly.

Jobs are submitted with a client-minted id (`job-<uuid>`, ADR 0026):

```
curl -s -X POST $API/jobs -H 'content-type: application/json' -d '{
  "job":"job-<uuid>","image":"busybox","command":["sleep","600"],
  "requests":{"cpu_millis":2000,"memory_bytes":1073741824,"disk_bytes":0},
  "quota_entity":"quota-00000000-0000-0000-0000-000000000001"}'
```

## Structure of a case

Four fields, in this order. Keep each to one or two lines; link the code the
case pins rather than restating it.

- **Do** — the request(s), in order.
- **Expect** — the observable result. Exact where exactness is the point.
- **Guards** — the regression this catches. One line; omit if self-evident.
- **State** — `mutates` (and what) or nothing. Omit when read-only.

---

## Cluster overview (`GET /api/v1/overview`)

### E2E-1 — Overview reports the replica's own cluster identity

- **Do:** `GET $API/overview` on a freshly booted cluster.
- **Expect:** `200`; `cluster_id` equals the id in `dev`'s ready summary;
  `coppice-applied-index` and `coppice-committed-index` headers present.
- **Guards:** the cluster id comes from node config through `ControlPlane`, not
  from replicated state (nothing in the `StateMachine` carries it).

### E2E-2 — Overview sums the registered agent's capacity

- **Do:** `GET $API/overview` with only the dev agent registered.
- **Expect:** `capacity.nodes` = `{total:1, schedulable:1, lost:0}`; `capacity.capacity`
  is the agent's advertised capacity; `allocated` and `used` all zero.
- **Guards:** capacity is projected from the node map, not fabricated.

### E2E-3 — A placed job reads as `running` and holds its allocation

- **Do:** submit the fixture job (2000 cpu_millis / 1 GiB); wait ~3 s;
  `GET $API/overview`.
- **Expect:** submit returns the same `job` id and a `log_index`;
  `queue.by_state.running == 1`; `capacity.allocated.cpu_millis == 2000`.
- **Guards:** the ADR 0030 read-time phase join — `Attempting(attempt)` plus a
  `Running` attempt reads as `running`, and no raw `attempting` ever reaches a
  client.
- **State:** mutates — leaves one running job.

### E2E-4 — An unplaceable job queues, and its age is computed at read time

- **Do:** submit a job requesting more cpu than any node has (e.g.
  `cpu_millis: 99000`); wait ~4 s; `GET $API/overview?consistency=strong`
  twice, ~3 s apart.
- **Expect:** `queue.depth == 1` and `by_state.queued == 1` on both reads;
  `oldest_queued_age_us` is non-null and **larger on the second read**, with no
  intervening command.
- **Guards:** the queued age is a read-time wall-clock measure, not replicated
  state — apply never reads a clock. Also covers `?consistency=strong` on a
  real handler.
- **State:** mutates — leaves one permanently queued job.

### E2E-5 — Abort tallies as `aborted` and releases the allocation

- **Do:** `POST $API/jobs/<running-job>/abort` with `{"reason":"..."}`; wait
  ~3 s; `GET $API/overview`.
- **Expect:** `200 {}`; `by_state.aborted == 1`, `by_state.running == 0`;
  `capacity.allocated.cpu_millis` back to `0`.
- **Guards:** abort is a desired-state transition that reaches terminal state
  and frees funding; the overview follows both.
- **State:** mutates — terminates the job from E2E-3.

### E2E-6 — The overview never fabricates what it cannot know

- **Do:** `GET $API/overview` within ~30 s of cluster start (before the first
  derived bucket closes).
- **Expect:** `queue.drain_rate_per_minute` and `arrival_rate_per_minute` are
  `null` and `queue.history` is `[]` (no closed bucket yet — coverage, not
  activity, is what they report); `capacity.used` is all zeros (no
  measurement pipeline, ADR 0032 item 7); `recent_events.floor_index` is
  present even when `events` is empty, so a fresh replica is distinguishable
  from a quiet cluster.
- **Guards:** [KOI-6](../roadmap/known-open-issues.md) /
  [ADR 0032](../decisions/0032-advisory-event-timestamps.md) honest absence —
  a window without coverage is `null`/absent, never a fabricated `0`.

### E2E-9 — Queue rates, history, and recent events serve from retained data

- **Do:** submit two jobs (one placeable, one not); wait for the 30 s bucket
  containing them to close (~35 s); `GET $API/overview`.
- **Expect:** `arrival_rate_per_minute`/`drain_rate_per_minute` non-null and
  consistent with the bucket counts scaled per minute; `history` has one
  sample per closed bucket, oldest first, each with the sampled `depth`;
  `recent_events.events` is newest-first, each carrying
  `(index, ordinal, at_us, kind)` with ordinals dense within an index and one
  shared `at_us` per index (the batch stamp); no event is ordered by `at_us`.
- **Guards:** ADR 0032 tiers 1/3 — the overview's windowed fields come from
  the derived bucket window and the fanout ring, keyed by `(index, ordinal)`.
- **State:** mutates — same jobs as E2E-3/E2E-4 (run this alongside them).

## API contract

### E2E-7 — A bogus read parameter is `INVALID_ARGUMENT`, on any read

- **Do:** `GET $API/overview?consistency=bogus`.
- **Expect:** `400`, body `{"code":"INVALID_ARGUMENT", ...}` naming the
  accepted values.
- **Guards:** the ADR 0007 parameter contract is enforced by the shared
  extractor, so it holds on implemented handlers and stubs alike.

### E2E-8 — An unimplemented read is `501`, naming its endpoint

- **Do:** `GET $API/queue/stats` (any still-stubbed read).
- **Expect:** `501`, body `{"code":"UNIMPLEMENTED","message":"GetQueueStats is
  not implemented yet"}`.
- **Guards:** stub routes are claimed and honest — a typo'd path 404s
  distinctly instead of hitting the UI fallback.

---

## Gaps

Not yet covered end-to-end, in rough priority order: `GET /nodes` and
`GET /nodes/{node}` (unit-tested only); read-your-writes via
`?min_index=<log_index>` from a submit response; `NOT_LEADER` on a follower
(needs a multi-replica fixture, unlike everything above); idempotent
resubmission over HTTP (covered at the `ControlPlane` layer by
`submit_retry.rs`, never over the wire).
