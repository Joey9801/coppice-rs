# 42. Job metadata: small, mutable, replicated string annotations

- **Status:** Accepted
- **Date:** 2026-09-14
- **Builds on:** [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md)
  (proto as the replicated encoding), [ADR 0023](0023-scoped-role-bindings.md)
  (ownership and role gating), [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md)
  (idempotent submission), [ADR 0031](0031-http-api-surface.md) (HTTP
  conventions, `ListJobs` filter AST), [ADR 0038](0038-internal-forwarding-of-follower-received-writes.md)
  (forwarded writes)

## Context

A job today is only its execution spec. Nothing on it says *what it is for*:
there is no friendly name, no link back to the pipeline or ticket that
produced it, no pointer to the job it was retried from or the node it is
meant to be compared against. Users work around that by encoding meaning in
the image tag or the command line, which the UI cannot interpret and the
list endpoint cannot filter on. The `ListJobs` filter grammar has reserved a
`label` leaf since it shipped, precisely for this gap.

The annotations wanted are small, user-owned, and change after submission
(a pipeline learns the ticket number later; an operator tags a job as
"investigated"). They must survive leader changes and restarts, so they live
in the replicated state, not in a coordinator-local side table.

A first cut of this design allowed arbitrary JSON values. That bought a
recursive value type, a canonical JSON writer, a numeric-equality rule
across integer and float renderings, depth limits, nested protobuf
conversion, and a regex filter, none of which the motivating cases need. It
also ran into the replicated-state rule that matters here: nothing that
compares by floating point belongs in the state machine, and JSON numbers
do. Strings cover every case above and keep every one of those problems
out of the corpus.

## Decision

### Model

Every job carries `metadata`: a `BTreeMap<String, String>` on
`coppice_core::job::Job`, replicated as part of the job spec and snapshot
like every other field. There is no value type: a value is a UTF-8 string,
and any structure a caller wants is theirs to encode in it.

On the wire (`proto/coppice/core/v1/job.proto`) the map is `repeated
MetadataEntry { key, value }`, the same repeated-entry shape as node
labels and under the same corpus rule (`schema-style.md`): writers emit
ascending key order, readers accept any order and reject duplicate keys.
Domain → pb is canonical by construction (a `BTreeMap` iterates in key
order), so identical metadata encodes to identical bytes without the
reader having to police it.

**Limits**, enforced at the API for admission and every mutation, and
re-checked at apply so the replicated state can never hold an oversized
map regardless of proposer:

| Limit | Value |
| --- | --- |
| Key | 1–64 bytes, ASCII letters, digits, `.`, `_`, `-`, `/`, `:` |
| Value | ≤ 1024 bytes of UTF-8; empty is allowed |
| Keys per job | ≤ 64 |

Metadata is descriptive only: the scheduler, admission, quota arithmetic
and the executor never read it.

### Well-known keys

Keys are free-form, but the UI gives some a meaning:

- `name` — used as the job's title wherever a title is shown, with the job
  id demoted to a subtitle. An empty `name` is ignored.

Any other value is rendered by shape, not by key: an `http://` or
`https://` URL becomes an external link, and a typed Coppice id (ADR
0024's `<prefix>-<uuid>` form) becomes an `IdLink` to that object's page
where one exists (jobs, nodes, quota entities). Everything else is plain
text.

### Mutation

One new API-proposed, actor-carrying command, `UpdateJobMetadata`
(envelope tag 10), carrying either a full **replacement** map or a
**patch** (`set` map merged over the current map, then `unset` keys
removed; a key in both is rejected at the API). Apply:

1. rejects `UnknownJob`;
2. authorizes `Verb::UpdateJobMetadata { entity, submitted_by }`, which is
   evaluated exactly like `Verb::Abort` — the job's submitter, or
   `operator` or higher over the job's quota entity — with ownership
   re-derived from the stored record, never trusted from the proposer;
3. computes the resulting map and rejects `InvalidJobMetadata` if it
   breaks a limit above;
4. stores it and emits `Event::JobMetadataUpdated { job }`.

`SubmitJob` re-checks the same limits, and metadata is part of the
submission's identity (ADR 0026): a retried id with the same map is the
idempotent no-op, a different map is `SubmitSpecMismatch`.

Terminal jobs accept metadata updates: annotating a finished job ("root
cause: OOM", a link to the incident) is a primary use, and the record stays
until retention evicts it. An update that leaves the map unchanged is an
accepted no-op, so a retried forward is safe.

### HTTP surface (ADR 0031 amendment)

| Route | Message pair | Class |
| --- | --- | --- |
| `PUT  /api/v1/jobs/{job}/metadata` | `ReplaceJobMetadata*` | write |
| `POST /api/v1/jobs/{job}/metadata` | `UpdateJobMetadata*` | write |

- `SubmitJobRequest` gains `metadata` (object of strings, default `{}`),
  validated by the same limits; `JobSummary` and `JobDetail` gain
  `metadata` (object of strings, always present, `{}` when empty).
- `PUT` body `{ "metadata": { … } }` replaces the whole map — the same
  full-replacement shape as `PUT /authorization`, for a caller that owns
  the map; `metadata` is required, so a body that forgot it is a 400
  rather than a silent wipe. `POST` body `{ "set": { … }, "unset": [ … ] }`
  is the small patch for a caller that owns some keys; both halves default
  to empty, and a body that sets and unsets the same key is
  `INVALID_ARGUMENT`. Both respond `{ "job": id, "log_index": n }`; a
  client reads the job back with `?min_index=` (ADR 0007).
- Limit breaches are `INVALID_ARGUMENT` at the edge; the apply re-check
  surfaces as `REJECTED` (a patch's merged map only exists at apply, so a
  patch that overflows the key count is a 409, a replacement a 400).
  Authorization follows the abort rule via the `precheck` gate, then
  apply. An unknown job is `REJECTED`, as it is for abort.
- Followers forward both writes to the leader over the admin plane
  (`ForwardUpdateJobMetadata`, ADR 0038), and a forwarded submission
  carries its metadata.

### Filtering (ADR 0031 `ListJobs` amendment)

The reserved `label` leaf is replaced by a `metadata` leaf:

```json
{ "metadata": { "key": "name" } }
{ "metadata": { "key": "ticket", "equals": "INC-1234" } }
```

- `key` alone is **presence**: the job has that key, whatever the value.
- `equals` is **exact string equality**, byte for byte, case-sensitive.
- The key is validated under the stored-key rules (a key that could never
  be stored can never match, and refusing it names the typo); the leaf
  counts as one node against the existing depth and node caps.

Regex or substring matching is deliberately absent. Presence and equality
cover the automation cases in view (find the job for this ticket, list
everything tagged by this pipeline); a pattern match can be added as a
further leaf if a real need appears, without touching the model.

### Clients

- **CLI:** the TOML job spec accepts a `[metadata]` table of strings;
  `coppice job status` prints the map; `coppice job metadata <job> --set
  k=v … --unset k … [--replace]` proposes the patch or replacement;
  `coppice job list` gains `--metadata-key k` and `--metadata-equals k=v`.
- **Web UI:** the job page is titled by `name`, shows a metadata card with
  the shape-based rendering above and an inline editor (add, edit, remove
  a key) driven by the patch endpoint; the jobs list shows the name under
  the id and its filter bar grows a key and value pair (value empty means
  presence).

## Consequences

- `Job` gains a field that every constructor must fill; the proto baseline
  is regenerated in the same change (additive only).
- No new dependency enters the workspace, and no floating-point value or
  JSON text enters replicated state.
- Metadata rides `JobSummary`, so a list row carries at most 64 short
  strings; the limits are set so the common case is a handful.
- Metadata is not indexed: `equals` scans within the existing `ListJobs`
  scan budget like every other leaf.
- A richer value model (structured values, pattern filters) is a new ADR
  that supersedes this one, not an amendment; the string map is the
  contract until then.
