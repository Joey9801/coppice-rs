# coppice-api

The external API layer — the user-facing entry point for job submission,
abort, status queries, event subscriptions, and administrative actions. The web
UI and CLI are both built on this same surface.

> **Status: early stub.** Today the crate defines only the boundary types — the
> [`ApiError`](src/lib.rs) taxonomy and the [`ControlPlane`](src/lib.rs) trait
> with `submit_job` / `abort_job`. There is no transport, no read path, no
> subscription fanout, and no authn/z yet. This README describes the intended
> role so the shape those additions grow into is on record; the design docs
> linked throughout are authoritative.

## Intended role

The API runs on **every coordinator replica**, not only the leader. It sits in
front of consensus and state and translates client requests into durable state
transitions — endpoints are modelled as state machines, never as imperative
control of workers (aborting a job commits a desired transition; agents observe
and enforce it). See
[../../docs/architecture/components.md](../../docs/architecture/components.md)
(External API Layer).

### Writes route to the leader

Mutating requests (`SubmitJob`, `AbortJob`, admin/policy commands) resolve to a
Raft command; a follower forwards internally to the current leader. A call
returns only once its command is **committed and applied** — never merely queued
or committed — matching `Consensus::propose`'s contract. The error taxonomy
mirrors that seam:

- `NotLeader { leader_hint }` — retry against the named leader.
- `Invalid` — failed synchronous validation; the request is wrong, retrying
  as-is will not help.
- `Rejected` — the command committed and apply refused it *deterministically*
  (a racing proposer, e.g. `DuplicateJob`, `JobTerminal`): normal control flow,
  not a server fault. The rejection taxonomy and which command produces which is
  in
  [../../docs/architecture/command-catalog.md](../../docs/architecture/command-catalog.md).
- `Unavailable` — the write never resolved to a replicated decision (timeout,
  overload, seam shutting down); safe to retry.

The API is the proposer for the `SubmitJob`, `AbortJob`, and admin commands
(`SetNodeSchedulable`, `ConfigureQuotaEntity`, `UpdatePolicy`,
`BumpClusterVersion`) in the catalog, and pre-validates them so apply-time
rejections stay a backstop for races.

### Reads have a per-endpoint consistency class

Each read endpoint carries a **default consistency class**, and callers may
override with `consistency=strong|bounded|eventual`
([ADR 0007](../../docs/decisions/0007-per-endpoint-read-consistency.md)):

- **Strong** (leader read-index) for read-after-write and decision-feeding
  reads. A write response carries its commit index so the client can pair it
  with a bounded read for read-your-writes.
- **Bounded-stale** follower reads (default for list/detail/quota-usage views):
  responses carry `applied_index` and last-known leader commit so staleness is
  explicit; a follower that cannot bound its lag redirects to the leader.
- **Eventual** for UI aggregates and history served from derived stores.

### Event subscriptions: cursor, order, gap-and-resync

Clients subscribe to scoped update streams (job / queue / quota entity / node).
Events are **derived from committed state**, tagged with the Raft apply index as
a global sequence number, delivered per-scope in commit order, at-least-once
(dedupe by sequence). A subscription resumes from a cursor while it is inside the
bounded reconnection buffer; if it has fallen out, the server sends a **gap
indication** and the client re-queries authoritative state and resubscribes.
Streaming is never a substitute for the query path. Because every replica derives
an identical stream, followers can serve subscriptions and keep fanout off the
leader. Full guarantees in
[ADR 0008](../../docs/decisions/0008-event-delivery-guarantees.md).

### Authn / authz

User-facing access authenticates via SSO. Authorization is enforced **at the API
layer and again at command validation** where appropriate, covering submit /
view / abort / retry, log and artifact access, queue and quota administration,
node drain, and policy changes. See
[../../docs/operations/security.md](../../docs/operations/security.md). (The
authn/z ADRs are settled but not yet written; the enforcement code is pending.)

## Boundaries

- The API proposes and reads; it owns no state. Consensus and state-machine
  application live in the coordinator
  ([../../docs/architecture/coordinator-runtime.md](../../docs/architecture/coordinator-runtime.md)).
- Deterministic apply-time rejection is the coordinator's; the API's job is to
  pre-validate, route, and map rejections to client-facing errors.
- Event derivation and the bounded reconnection buffer belong to the coordinator
  runtime; this crate exposes the subscription surface over them.
