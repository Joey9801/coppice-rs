# 8. Event delivery: apply-index cursors, per-scope order, gap-and-resync

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-7](../roadmap/open-decisions.md#od-7-event-subscription-delivery-guarantees)

## Context

Clients (UI, integrations) subscribe to updates for subsets of jobs, queues,
entities, or nodes. They need ordering they can reason about and a defined
recovery path when they miss events — without turning the Raft log into a
retained user-facing event stream.

## Decision

- **Events are derived, not authoritative.** During apply, each committed
  command emits zero or more events tagged with the **Raft apply index as the
  global sequence number** and with scope keys (job, queue, quota entity,
  node). Because apply is deterministic, every replica derives an identical
  event stream; the event log is *not* replicated through Raft.
- **Bounded event log.** Each coordinator keeps the derived event log in a
  bounded in-memory ring with optional disk spill; retention defaults to
  1 hour or 1M events, whichever is smaller. It is a reconnection buffer, not
  history — history lives in the job-history store
  ([ADR 0012](0012-data-retention.md)).
- **Ordering guarantee: per scope, total order by sequence.** Events for one
  job (or one queue, etc.) are delivered in commit order. A single connection
  additionally sees monotonically increasing sequence numbers overall.
- **Delivery: at-least-once.** Duplicates are possible around reconnects;
  consumers dedupe by sequence number.
- **Cursors and gaps.** A subscription starts from an optional cursor (last
  seen sequence). If the cursor is still in the retained window, delivery
  resumes exactly from it. If not, the server sends a **gap indication**
  carrying the earliest available sequence; the client must re-query
  authoritative state (a normal API read at a known index,
  [ADR 0007](0007-per-endpoint-read-consistency.md)) and resubscribe from the
  index that query reported. Streaming is never a substitute for the query
  path.

## Consequences

- Cursor semantics are trivial to implement and reason about: one global
  sequence, already totally ordered by Raft.
- Followers can serve subscriptions (their derived stream is identical),
  keeping fanout load off the leader.
- Clients must implement the gap-resync path; the client library will provide
  it so integrations don't each reinvent it.
- Retention of the reconnection buffer is a tuning knob; too small only causes
  more resyncs, never incorrectness.
