# 26. Client-minted job ids make submission idempotent

- **Status:** Accepted
- **Date:** 2026-07-10
- **Resolves:** [KOI-2](../roadmap/known-open-issues.md#koi-2-job-submission-is-not-idempotent-across-an-unknown-outcome)

## Context

`SubmitJobRequest` carried no idempotency identity: the API minted a fresh
random `JobId` per invocation. The `DuplicateJob` rejection therefore only
protected *re-proposal of the same command* (a leader-change replay); a
client retry at the request boundary was a brand-new command with a fresh
id. If the first request committed but its response was lost — timeout,
connection drop, leader change — an ordinary retry created a second job:
duplicate execution, duplicate charges, and no safe retry policy for
clients (KOI-2).

Fixing this needs a stable identity that survives the unknown-outcome
window, and deduplication state that every future leader answers
identically. Two shapes were considered:

1. a separate idempotency-key table in the replicated state machine, keyed
   by a client token, with its own retention and eviction; or
2. making the `JobId` itself client-minted, so the retry *is* the same
   submission and the existing `jobs` map *is* the deduplication state.

## Decision

The **client mints the `JobId`** (`job-<uuidv7>`, ADR 0024) and sends it in
`SubmitJobRequest.job`; the field is required and the id is the
submission's idempotency identity. The API no longer mints ids. Retrying an
unknown outcome means re-sending the identical request, byte for byte.

Apply resolves a `SubmitJob` whose id already exists in state by comparing
the client-supplied spec fields (image, command, entrypoint, requests,
priority, `max_runtime_us`, quota entity, retry policy) against the
committed record:

- **Identical spec** → an **accepted no-op**: no state change beyond the
  `version` bump, no events, and the proposer observes success. The API
  returns the original `JobId` plus the repeat's own apply `log_index`
  (which is ≥ the original commit, so it remains a valid ADR 0007
  read-your-writes cursor).
- **Different spec** → the deterministic rejection `SubmitSpecMismatch`
  (replacing `DuplicateJob`): reusing an id for a different payload is a
  client bug, never silently resolved to either job.

`abort_requested`, the command's `multiplier`, and `submitted_at_us` are
**not** identity: the first is apply-owned after commit (an abort may have
landed between the attempts), and the latter two are re-stamped by the
proposer on every send — the original commit's values stay authoritative.

**Scope and retention window.** Deduplication holds exactly as long as the
job resides in replicated state: from the original commit until
`EvictTerminalJobs` removes the terminal record per the ADR 0012 retention
policy (72 h post-terminal by default). A retry arriving after eviction is
indistinguishable from a new submission and will create a new job with the
same id; retries operate on the scale of seconds to minutes, so the window
is comfortably sufficient, but it is a documented bound, not infinity.

`SubmitJobResponse` also gains `log_index`, closing the "future proto
revision" note on the `ControlPlane` contract.

## Consequences

- Clients can apply ordinary retry policies to every transient submission
  failure — timeout, `NotLeader` redirect, connection loss — with at most
  one durable job per logical submission, satisfying KOI-2's closure
  criteria.
- No new replicated state, snapshot field, or eviction coupling: the jobs
  map is the dedup table, and its retention is already governed by
  ADR 0012.
- The API loses the ability to guarantee id uniqueness; a colliding or
  probing client is answered by `SubmitSpecMismatch` (or, for an identical
  spec, the existing job). Ids are unguessable UUIDs, and per-caller
  submission authorization arrives with the authn/z ADRs.
- Server-side minting is gone: every client (CLI, UI, curl) must generate
  `job-<uuid>` before submitting. This is the standard cost of
  client-supplied idempotency keys.
