# 43. Filtered job event subscriptions over SSE

- **Status:** Accepted
- **Date:** 2026-09-21
- **Builds on:** [ADR 0008](0008-event-delivery-guarantees.md) (apply-index
  cursors, derived per-replica stream, at-least-once, gap-and-resync),
  [ADR 0032](0032-advisory-event-timestamps.md) (`(index, ordinal)` identity,
  the one timeline wire shape), [ADR 0031](0031-http-api-surface.md) (HTTP
  conventions, the `JobFilter` AST, the reserved `/api/v1/events` route),
  [ADR 0042](0042-job-metadata.md) (metadata filter leaves),
  [ADR 0023](0023-scoped-role-bindings.md) (job reads are unscoped)

## Context

ADR 0008 settled what a subscription guarantees, and the fanout ring behind
it exists on every replica, but `GET /api/v1/events` has answered `501` since
it was reserved: nothing decided what a client may subscribe *to*, or what the
bytes on the connection are.

The case that forces the question is a service that manages large sets of
jobs across many queues. Its jobs run for tens of minutes to hours, so
polling each one is almost entirely redundant requests, and the only
subscription scope the fanout knows — one job id — would mean one connection
per job. What it needs is a subscription to an **open set**: every job
matching a predicate, including jobs submitted after the subscription opened.
Such a service stamps the jobs it owns with a metadata key, so a presence
filter on that key names exactly its set.

Three things make that harder than widening a filter enum:

- Events carry only ids. The fanout holds no state to evaluate a predicate
  against, and the state it could borrow — a published view — has moved on by
  delivery time and differs between replicas at any instant.
- The ring is a reconnection buffer sized in minutes, while these subscribers
  see a matching event perhaps once an hour. A cursor that only advances on a
  match is nearly always stale by the time it is needed.
- At design scale a single command can emit over a thousand events, and a
  replica restart reconnects every one of its subscribers somewhere else at
  once. Replay cannot be an unbounded scan on the task that delivers to
  everyone.

## Decision

### The subscription selects an open set of jobs

`GET /api/v1/events?jobs=<filter>` subscribes to every job matching the
filter.

- **The filter is the ListJobs filter, restricted.** `jobs` takes the
  `JobFilter` JSON AST with the same size caps, but only the leaves that say
  *which job this is* rather than *what it is doing*: `metadata` (presence or
  exact value), `entity` (exact or subtree), `id`, `submitted_by`, under the
  `all`/`any`/`not` combinators. Any other leaf is `INVALID_ARGUMENT`, naming
  it. A subscription filter is therefore always a valid ListJobs filter, so
  the resync read and the stream select the same set by construction.
- `jobs` is required. The parameter is named for its selector so node- and
  entity-scoped selectors can sit beside it later without reshaping the route.
- Any authenticated principal may subscribe: job reads are unscoped, and the
  stream says nothing a read would not.

### Matching reads scope keys stamped at apply

The apply loop resolves, for each distinct job a command's events name, the
keys the permitted leaves read — the job's metadata, its submitter, and its
quota entity with that entity's ancestor chain — from the state the command
just produced, and carries them on the event batch. This extends the rule
events already follow for their owning job and node ids: a scope key is
stamped while the association is authoritative, and delivery never looks one
up in state that may have moved on.

**An event is delivered if its job matched immediately before *or*
immediately after the producing command.** Only two commands can move a job
across a filter, a metadata update and an eviction, and in both the apply
handler already holds the value it is replacing or removing; their events
carry the prior keys. A subscriber therefore sees the update that took a job
out of its set, and the eviction of a job that no longer exists to be looked
up. Events that name no job are never delivered on a `jobs` subscription.

Nothing is added to the replicated state, and the prior keys stay off the
wire.

**Moving a quota entity is a gap for subtree selectors.** A third command
moves jobs across a filter without naming any of them: reconfiguring an
existing entity with a different parent changes the ancestor chain of every
job beneath it. Its event says whether it was such a move, and a subscription
whose filter reads an `entity` subtree anywhere — under `not` included — gets
a `gap` in place of that batch, live or while catching up across it (batches
below the move are delivered first). The gap's `earliest_available` is the
move's own index, so the resync read is at least that fresh; `coppice-client`
reads its resync list with `min_index` set to it. Enumerating the affected
jobs instead would make one command's event batch proportional to a subtree,
and a resync is what a consumer tracking that subtree needs anyway. Moves are
rare administrative acts; selectors that read no subtree, exact-entity ones
included, are untouched.

Rejected:

- *Evaluate against the latest published view at delivery.* The view is up to
  a publish cadence newer than the event, an evicted job is absent from it,
  and a replay from the ring would evaluate against state minutes newer still.
  Two replicas would deliver different streams for the same indexes, which
  breaks the property that lets any replica resume any subscriber.
- *Hand the fanout a clone of the jobs map per command.* The clone is O(1),
  but holding it makes the next command path-copy tree nodes — and the job
  records stored inline in them — on the serial apply path.

### Payloads are thin

Events use ADR 0032's one timeline wire shape — identity, stamp, kind, scope
ids — and carry no job snapshot. A consumer that needs more than the
transition reads the job, passing `min_index` = the event's index so a lagging
replica cannot answer with older state. One read per transition of interest is
a small fraction of what polling cost, and it keeps the ring small; whether
snapshots earn their place is a question for after a real consumer has run
this way.

### Transport: SSE, one frame per log index

- **`batch`** — `id:` is the apply index; the data is that index's admitted
  events with their full-batch ordinals. A command's events are never split
  across frames, so the cursor stays the bare index of ADR 0008 and a resume
  can never land inside a command.
- **`progress`** — `id:` is an apply index at or below which everything the
  filter admits has already been sent. Sent when a subscription finishes
  catching up and periodically after. Without it a subscriber to a quiet set
  would reconnect with a cursor the ring evicted long ago and be forced to
  resync having missed nothing. It doubles as the connection keepalive, and
  its cadence is part of the contract: a healthy stream carries a frame at
  least every 15 s, so a client may treat several missed intervals as a dead
  connection and reconnect from its cursor (`coppice-client` waits 60 s).
- **`gap`** — carries `earliest_available` and no `id:`: a gap is not a
  position to resume from. The stream stays open and continues live, so a
  consumer that only renders recent activity may carry on, while one that
  tracks state must resync. No `progress` is sent while a subscriber has an
  undelivered gap, and a stream that opens with a gap does not open with a
  bookmark: neither may claim coverage the gap just denied.
- The cursor comes from `Last-Event-ID`, else `?cursor=`.

Log compaction is not itself a gap source, because the stream is derived at
apply and never read back from the Raft log. What opens a gap is the ring
evicting past the cursor, a replica restart (the ring is memory; its floor
restarts at the recovered index), a follower installing a snapshot (its
applied index jumps over entries it never applied), the apply-side tap
overflowing, and a subscriber's own queue overflowing. Because the cursor is
the global apply index, a client bounced off one replica resumes on any other
whose ring still covers it.

### A stream ends with its credential, and never holds a shutdown

A stream ends cleanly when the bearer token that opened it expires, when the
replica drains for shutdown, or when its fanout stops. Clients must reconnect
with their cursor anyway, so ending at expiry costs little and means a stream
never outlives the credential that authorized it. Mechanisms without an expiry
impose no deadline.

### Catch-up is pulled, live delivery is pushed

A resuming connection first registers for live delivery, learning the highest
index the fanout has processed, then pages the ring from its cursor up to that
index in whole batches under a scan budget, then switches to its live queue,
dropping what it has already sent. If retention overtakes a catch-up part
way through, the stream says so with a `gap` rather than resuming above the
hole.

Pushing the backlog into the subscriber's bounded queue instead would turn
every reconnect further behind than the queue's depth into a forced resync
even though the ring still held the data, and would run each replay as one
unbounded scan on the task that delivers to everyone else — worst exactly when
a replica restarts and its subscribers all arrive elsewhere together.

### Bounds

- Subscriptions per replica are capped; past the cap a request is refused
  `UNAVAILABLE` rather than degrading delivery for those already connected.
- The ring is bounded by bytes as well as by age and event count, since scope
  keys make a batch's size depend on its jobs' metadata.
- A subscriber whose connection is gone is removed from the fanout, not
  retried.

### The client's loop

List with the filter, keep the first page's `Coppice-Applied-Index`, read the
remaining pages no older than it (`min_index`), and subscribe from it; on
`gap`, do the same again, reading no older than its `earliest_available`.

**The cursor must be at or below every page's index.** Reads have a floor and
no ceiling — a replica holds only its latest view — so a multi-page list is
never a consistent cut: each page reflects its own index, all at or above the
first page's. That is harmless in one direction only. A page *newer* than the
cursor costs events the row already reflects; a page *older* than it loses
whatever happened in between. Paging is keyset on the job id, so each job is
judged exactly once, at its page's index, and any change after the first
page's index — a job leaving a filter on mutable state mid-walk included — is
delivered by the subscription, whose selector reads identity only.

`coppice-client`'s `watch_jobs` packages this, with three choices of its own:

- **The snapshot is scoped to live jobs by default**: the list filter is the
  caller's filter and "phase is non-terminal", while the subscription keeps the
  caller's filter as is — a finished job can still have its metadata edited,
  and is eventually evicted, and the stream says so. A snapshot is then
  bounded by the working set rather
  than by how much history the cluster retains, which a durable history store
  would otherwise make unbounded. A job the caller tracks that is absent from
  such a snapshot has left the live set, and is read on its own; a caller
  starting up polls for old jobs whose state it does not know.
- **The snapshot arrives a page at a time**, each page with its own index and
  marked first or last, so neither side holds the set's rows in memory (the
  watcher keeps only an id and an index per job, for the suppression below). A walk that
  outlasts the ring's window meets a gap when it subscribes and starts over —
  another reason the default snapshot is the small one.
- **Events a row already reflects are suppressed.** The watcher remembers the
  page index each job's row was read at — for pages read newer than the
  first, the only ones that can be ahead of the cursor — and drops that job's
  events at or below it, forgetting the rows once the stream passes the newest
  page. After
  a row for a job, a caller sees only what is newer than the row, instead of
  watching a replay walk the job back through states it has already left.

## Consequences

- A job-managing service replaces per-job polling with one connection, plus
  one read per transition it cares about. The route reserved by ADR 0031 is
  served, from every replica, and is that surface's one long-lived response.
- Resyncs are full filtered scans (metadata is not indexed), so their rarity
  matters. Progress frames and cross-replica resume keep them to genuine
  discontinuities.
- Leaves over mutable, non-identity attributes (phase, node) are not
  subscribable. A consumer wanting "jobs entering phase X" subscribes to the
  set and reads the transitions.
- The apply loop does a lookup and a small clone per distinct job per command
  whether or not anyone is subscribed, because the ring must hold the keys for
  a later resume.
- One frame per index means a command touching very many jobs is one large
  frame. Commands that batch over jobs need their own size bounds.
- Filter evaluation is one task's work per replica, linear in subscribers ×
  jobs per batch. That is ample for the expected hundreds of subscribers;
  indexing subscribers by metadata key is the available next step and changes
  nothing on the wire.
- The durable history store ([ADR 0032](0032-advisory-event-timestamps.md)
  tier 2) is not a dependency: a gap resyncs from state, not from history.
