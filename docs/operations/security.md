# Security Model

User-facing access should use SSO.

Internal communication between coordinators and agents should be authenticated
and encrypted. Agents should have node identity. Coordinators should
authenticate to agents, and agents should reject unauthenticated or stale
commands.

## Authorization

Authorization should cover:

- Submitting jobs.
- Viewing jobs.
- Aborting or retrying jobs.
- Accessing logs or artifacts.
- Administering queues and quotas.
- Draining or disabling nodes.
- Changing policy.
- Viewing node-level information.

## Container execution posture

Decided in [ADR 0011](../decisions/0011-container-security-posture.md):
default-deny.

- No privileged containers, no host mounts, no host network; containers get
  their own network namespace with outbound access.
- Containers run as a non-root UID by default; UID 0 is not requestable.
- Resource limits are always enforced.
- Exceptions (a host mount path, host networking, a privileged capability) are
  admin-allowlisted per queue or node pool, replicated as policy, and audited.
  They are never user-requestable directly.
- Stronger runtime isolation (gVisor/Kata) is out of scope for v1, but the
  agent design must not preclude swapping the container runtime later.

## Node identity

Coordinator↔agent communication uses mutual TLS: a node bootstraps with a
one-time enrollment token, submits a CSR, and receives a per-node certificate.
`NodeId` is bound to that certificate identity, which also underpins the
fencing protocol's authenticity assumptions
([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)).

## Secrets

Secrets should not be stored casually in job definitions. **v1 stores no
secrets**: job environment comes only from the job spec, which is treated as
non-secret, and the platform says so. Secret-manager integration
(reference-only injection at container start) is future work; nothing in v1
may create a place where secret values land in logs, events, snapshots, or UI.
