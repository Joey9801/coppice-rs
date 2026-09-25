# 45. Quota entity paths as the primary client-facing reference

- **Status:** Accepted
- **Date:** 2026-09-25
- **Builds on:** [ADR 0019](0019-deterministic-quota-arithmetic.md) (the quota tree),
  [ADR 0023](0023-scoped-role-bindings.md) (subtree-scoped bindings),
  [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md)
  (client-minted ids), [ADR 0031](0031-http-api-surface.md) (HTTP
  conventions, the `ListJobs` filter AST), [ADR 0043](0043-filtered-job-event-subscriptions.md)
  (filtered event subscriptions)

## Context

Every client-facing reference to a quota entity is its `quota-<uuid>` id: the
job spec's `quota_entity`, the `ListJobs` entity leaf, the detail route,
the configure upsert's `parent`, and an authorization binding's `scope`. The
id is a fine storage identity but means nothing to a person. What people
actually think in is the tree: `acme/eng/platform`.

The tree cannot be addressed by path today because a path is not
well-defined. An entity's `name` is an unvalidated string — it may be empty,
contain `/`, or repeat a sibling's name — and no path is stored, only the
parent pointer. The web UI's mock has already drifted into treating `name`
as a full slash path, which the server never did.

## Decision

### Names are segments, unique among siblings

A quota entity's `name` is one path **segment**:

- 1–63 characters from `[A-Za-z0-9._-]`, the first alphanumeric;
- must not itself parse as a `QuotaEntityId` (`quota-<uuid>`), so a
  reference string is never ambiguous (below);
- compared case-sensitively.

No two entities with the same parent — including two roots — may share a
name. Both rules are enforced **at apply** by `ConfigureQuotaEntity` (a
create, a rename, and a reparent alike), so every replica agrees on them:
a bad segment is rejected `InvalidQuotaEntityName`, a clash
`QuotaEntityNameTaken` naming the holder. A bad segment is a 400 (the HTTP
handler pre-checks the grammar; the apply rejection carries its own
rejection kind so it maps to 400 too); the sibling clash is a 409. The uniqueness check
is a scan of the (bounded, ~1k) entity map at apply; no index is stored on
the state machine.

### A path is derived, never stored

An entity's **path** is its ancestors' names, root first, joined by `/`
(`acme/eng/platform`) — no leading or trailing slash. It is computed at
read time from the parent chain, bounded by the existing tree-depth cap.
Renaming or moving an entity changes its path and every descendant's; the
id never changes. Replicated state and every replicated command stay
id-keyed.

### `QuotaEntityRef`: one string, id or path

Everywhere a client *names* an entity, the wire accepts a
`QuotaEntityRef`: a JSON string that is either a `quota-<uuid>` id or a
path. A string that parses as an id is an id; anything else is parsed as a
path and every segment must meet the grammar. The segment rule above makes
this unambiguous.

The HTTP layer resolves a path to an id against the serving replica's read
view **before** proposing, and the proposed command carries the id. A rename
between resolution and apply is benign: the request binds to the entity the
path named when it was read. A path that does not resolve is reported
naming the path, with the same status an unknown id gets on that route
(409 on job submission and the configure `parent`, 400 on a binding scope
as for an unknown scope id, 404 on the detail read). One
deliberate asymmetry: in a **filter** (`ListJobs`, event subscriptions) an
unknown *id* keeps today's behaviour (it matches nothing — ids are
client-minted and may name something not yet visible), but an unresolvable
*path* is a 400: a path is a human-typed lookup, and a typo must not quietly
return an empty list.

Accepted as a ref:

| Surface | Field |
|---|---|
| `POST /api/v1/jobs` | `quota_entity` |
| `ListJobs` filter / event subscription filter | `{"entity": {"ref": "…", "scope": "subtree"}}` (was `id`) |
| `GET /api/v1/quota-entities/{entity}` | the path segment, percent-encoded (`acme%2Feng`) |
| `POST /api/v1/quota-entities` | `parent` |
| `PUT /api/v1/authorization` | each binding's `scope` |

`ConfigureQuotaEntityRequest.entity` stays a required client-minted id: it
is the upsert's idempotency identity (ADR 0026). Path-first *configuration*
is a client concern: the CLI resolves a path, updates the entity it names
if one exists, and otherwise mints an id and creates it under the path's
parent. Because siblings are unique, a retry after an unknown outcome
re-resolves to the entity the first attempt created, so the path-based
upsert keeps its idempotency.

### Every read that names an entity carries its path

| Response | Change |
|---|---|
| `QuotaEntityNode`, `QuotaEntityView` | add `path`; `name` stays the segment |
| `JobSummary` | `quota_entity_name` replaced by `quota_entity_path` |
| `JobDetail` spec, `PenaltyLink` | add the entity's path alongside its id |
| `ConfigureQuotaEntityResponse` | add `path` |
| `GetAuthorizationResponse` and session bindings | `scope` stays the id; add `scope_path` |

Ids stay on every response — the path is the primary rendering, not a
replacement. Historical records (timeline events) stay id-only, since a
path read later may not be the path the entity had then.

### Clients

- **CLI.** The job spec's `quota_entity`, `job list --entity`,
  `quota show`, a policy file's binding `scope`, and the formation policy's
  `[[quota_entity]]` entries (declared in any order, parents resolved within
  the policy) all take paths (ids still accepted where a ref
  is). `quota configure <PATH> --quota-ucu N [--entity <id>]` and its file
  form replace `--name`/`--parent`; the file is spelled like one formation
  `[[quota_entity]]` entry (`path`, `quota`, optional `id`), so an operator
  learns one vocabulary.
  Tables print the path first, with the id alongside.
- **Web UI.** Paths are the default rendering of an entity everywhere; the
  id is shown as subtext or a tooltip and stays copyable. Entity routes stay
  id-keyed (`/entities/quota-…`) so a bookmark survives a rename.

## Consequences

- A path is a convenient, not a durable, identity: automation that must
  survive renames should keep using ids, which remain accepted everywhere.
- The segment grammar is a breaking change for any existing entity names
  that do not fit it; per the project's no-back-compat posture, no
  migration is provided.
- Resolution happens on the receiving replica, so a just-created entity
  may briefly fail to resolve on a lagging follower, like any stale read.
