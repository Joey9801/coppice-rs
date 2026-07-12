# 23. Authorization: subtree-scoped role bindings, enforced at API and apply

- **Status:** Accepted
- **Date:** 2026-07-08

## Context

[operations/security.md](../operations/security.md) lists the verbs
authorization must cover — submit, view, abort/retry, logs, queue and quota
administration, node drain, policy change — but no model behind them.
[ADR 0020](0020-node-config-vs-replicated-policy.md) already ruled that
anything authorization-shaped is replicated policy, and
[ADR 0022](0022-oidc-identity-and-authentication.md) defines who is asking:
principals and group claims arriving on verified tokens, plus operator
certificates.

Three existing structures want to carry the rest of the weight:

- **The quota-entity tree** ([ADR 0005](0005-cost-based-soft-quotas.md)) is
  already the replicated organizational hierarchy — every job attaches to a
  leaf and charges its ancestors. An authorization system with its *own*
  org hierarchy would give operators two trees to keep aligned forever.
- **The apply contract**
  ([command-catalog.md](../architecture/command-catalog.md)) already gives
  deterministic validation, a closed rejection taxonomy, and pre-validation
  at the API with apply as the backstop for races and proposer bugs.
  Authorization fits that mold exactly — *if* commands carry who acted.
- **The log is ordered history.** If every user-initiated command records
  its actor, the audit trail the UI promises
  ([components.md](../architecture/components.md)) exists by construction
  instead of as a bolted-on subsystem.

Today commands carry no principal and jobs have no owner, so even "abort
your own job" is inexpressible.

## Decision

### Ownership: jobs record their submitter

`Job` gains `submitted_by`, the submitting principal, set from the command's
actor at apply. A principal may always **abort and retry a job it
submitted**. This is the only implicit grant besides operator certificates.

### Reads are open to authenticated principals in v1

Any authenticated principal may read jobs, queues, nodes, quota usage,
events, and job logs/artifacts. Batch clusters live on debuggability
("why is my job pending, and what is ahead of it?"), job specs are declared
non-secret by the v1 no-secrets posture, and fairness is the quota system's
job, not secrecy's. Read scoping, if ever wanted, is a purely API-layer
change — reads never traverse the log — and does not touch this ADR's
replicated machinery.

### Three built-in roles

| Role | Grants within scope | Cluster verbs (unscoped binding only) |
| --- | --- | --- |
| `submitter` | Submit jobs under any quota entity in scope | — |
| `operator` | Everything `submitter` has, plus abort/retry *anyone's* jobs under entities in scope | Drain/undrain nodes |
| `admin` | Everything `operator` has, plus configure quota entities within scope | Everything: policy, authorization, cluster version, node operations, coordinator membership, agent-enrollment administration |

Verbs compose upward and evaluation is **deny by default**; there are no
negative grants. Custom roles are out of scope for v1 — the closed set keeps
apply-side evaluation a total function and the model explainable in one
table.

### Bindings: replicated policy, optionally scoped to an entity subtree

```
Binding { subject: Group(name) | Principal(sub), role, scope: optional QuotaEntityId }
```

- A binding with a `scope` grants its role over the **subtree rooted at that
  entity**; an unscoped binding grants it cluster-wide.
- **Cluster verbs require an unscoped binding**: node drain, `UpdatePolicy`,
  `UpdateAuthorization`, `BumpClusterVersion` — and the API surfaces that
  never become state-machine commands but reshape the cluster itself:
  coordinator membership operations (add-learner / promote / remove, the
  RPCs OD-14's self-join automation will drive) and agent-enrollment
  administration (minting enrollment tokens, OD-15). These are unscoped
  `admin` (membership, enrollment) or `operator` (drain). A subtree-scoped
  `admin` can configure quota entities whose position (including any new
  parent) stays within the subtree — delegated team administration — but
  cannot touch bindings. Delegated *binding* management (a scoped admin
  granting roles inside their subtree) is the natural extension, deferred
  for the escalation-prevention checks it requires.
- Subjects match by exact string against the token's `sub` or its groups
  (claim name per `groups_claim` policy, ADR 0022). A principal's effective
  authority is the union over all bindings its subject or groups match.
- Operator certificates (ADR 0022) are an implicit unscoped `admin`, not
  representable in — and not removable through — the bindings list.

The quota-entity tree thus becomes load-bearing for authorization as well as
accounting. That is the point: one hierarchy, one place to reorganize a
team. It also means **reparenting an entity moves authority** with it —
`ConfigureQuotaEntity` by a scoped admin is confined to their subtree, and
cross-subtree moves take an unscoped admin and are visible in the audit log
like everything else.

### Enforcement: the API validates, apply re-checks

Every API-proposed command gains an actor:

```
Actor { principal: string, groups: string[], operator_cert: bool }
```

transcribed by the API layer from the *verified* token (or operator
certificate) at proposal time. Group membership is a claim, not replicated
state, so it must ride in the command for apply to remain a pure function of
(state, command).

- **API layer** — authoritative for user experience: authenticate, resolve
  the actor, evaluate bindings, reject synchronously with a real 403 before
  anything reaches the log. Also the *only* enforcement point for reads,
  which never become commands.
- **Apply** — re-evaluates the same decision deterministically against the
  replicated bindings *as of the command's position in the log*, plus
  ownership from state, and rejects with `PermissionDenied`. Pure BTreeMap
  lookups over a small bindings list: no clock, no I/O, no floats — the
  apply contract is undisturbed. This closes the revocation race (a binding
  removed while a command is in flight resolves in log order, on every
  replica identically) and backstops proposer bugs, which is precisely the
  role the command catalog already assigns to apply-time rejection.

Internal proposers — the scheduler, agent ingestion, node lifecycle,
housekeeping — carry no actor. Their command types are structurally
distinct, never reachable through the API, and their authority is the
system's own.

The actor-carrying commands are: `SubmitJob`, `AbortJob`,
`SetNodeSchedulable`, `ConfigureQuotaEntity`, `UpdatePolicy`,
`UpdateAuthorization`, `BumpClusterVersion`.

### `UpdateAuthorization`: full replacement, unscoped-admin only

Bindings change through one new command mirroring `UpdatePolicy`'s
full-replacement shape: the CLI reads, edits, and writes the whole list;
concurrent edits resolve last-writer-wins in log order. Validation, in
apply's usual read-only phase:

- the actor holds unscoped `admin`;
- every scoped binding references an existing quota entity
  (`UnknownQuotaEntity`);
- roles are from the closed set and subjects are non-empty
  (`InvalidAuthorization`);
- the new list retains **at least one unscoped `admin` binding**
  (`AuthorizationLockout`). Operator certificates make lockout recoverable
  rather than fatal, but an empty admin list is almost always an accident,
  and a deterministic one-line check keeps the accident loud. A deployment
  that genuinely wants cert-only administration can bind a reserved group.

### Audit falls out of the log

Every actor-carrying command — accepted or rejected — attributes a principal
to a mutation, in committed order, replayable. The job-history store
(ADR [0012](0012-data-retention.md)) carries `submitted_by` and the aborting
actor on its rows, so attribution survives the 72-hour eviction of
replicated state. Read auditing, where wanted, is best-effort API access
logging and stays out of replicated state entirely.

## Consequences

- Authorization outcomes are deterministic functions of replicated state and
  the committed command: no two replicas can disagree about who may do what,
  meeting ADR 0020's requirement by construction rather than by discipline.
- Mutation audit exists on day one with no additional subsystem; "who did
  this" is answerable from the log and the history store.
- Command payloads grow an `Actor`. Group claims ride in every user command,
  so log entry size is bounded by the IdP's group list — deployments whose
  tokens carry hundreds of groups should filter the claim at the IdP; the
  operations doc says so.
- One hierarchy serves quota and authority. Reorganizing the entity tree is
  now also an authorization action, with the sharp edge (reparenting moves
  authority) contained by scope checks and audit visibility.
- Groups match by string; IdP-side renames silently orphan bindings.
- Deferred, in rough order of expected demand: delegated binding management
  for scoped admins, custom roles, read scoping, per-queue grants (queues
  today are not an authorization boundary; the entity tree is).
