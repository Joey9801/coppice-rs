# 11. Locked-down container execution with admin escape hatches

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-10](../roadmap/open-decisions.md#od-10-container-execution-security-model)

## Context

User workloads are untrusted code on shared nodes. Security boundaries are
painful to retrofit once users depend on permissive behavior, but full runtime
isolation (gVisor/Kata) plus secret management would be a major scope increase
before the scheduler itself works.

## Decision

**Default-deny container posture (v1):**

- No privileged containers.
- No host mounts and no host network; containers get their own network
  namespace with outbound access.
- Containers run as a non-root UID by default; requesting a specific UID is
  allowed, UID 0 is not, absent an exception.
- Resource limits (CPU, memory, disk) are always enforced.

**Escape hatches are administrative, not user-requestable.** Admins may
allowlist specific exceptions (a host mount path, host networking, a
privileged capability) scoped to a queue or node pool; jobs needing them must
be submitted there. Exceptions are replicated policy and appear in the audit
log.

**Node and control-plane identity.** Coordinator↔agent communication uses
mutual TLS from day one: a node bootstraps with a one-time enrollment token,
submits a CSR, and receives a per-node certificate; `NodeId` is bound to that
certificate identity. User-facing API access authenticates via SSO as already
specified in [security](../operations/security.md).

**Secrets are deferred.** v1 stores no secrets: job environment comes only
from the job spec, which is treated as non-secret. Secret-manager integration
(reference-only injection at container start) is future work; nothing in v1
may create a place where secret values land in logs, events, snapshots, or
the UI.

Stronger runtime isolation (gVisor/Kata) is explicitly out of scope for v1 but
nothing in the agent design may preclude swapping the container runtime later.

## Consequences

- The user-visible contract is strict from the first release, so tightening
  never breaks users; loosening is a deliberate, audited admin act.
- Some real workloads (device access, host networking) need admin involvement
  — accepted friction.
- Certificate enrollment adds operational setup per node, and is also the
  foundation the fencing protocol's authenticity assumptions rest on
  ([ADR 0009](0009-fencing-and-reconciliation.md)).
- Users needing secrets in v1 must use external mechanisms; the platform makes
  no confidentiality promise about job specs and says so.
