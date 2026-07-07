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
- Cancelling or retrying jobs.
- Accessing logs or artifacts.
- Administering queues and quotas.
- Draining or disabling nodes.
- Changing policy.
- Viewing node-level information.

## Container execution posture

Job execution should use container isolation, resource limits, and a clearly
defined security posture around privileged containers, host mounts, network
access, secrets, and user identity.

The security model for container execution needs clear boundaries; this is an
[open design decision](../roadmap/open-decisions.md).

## Secrets

Secrets should not be stored casually in job definitions. If jobs need secrets,
the system should integrate with a secret-management mechanism and avoid
exposing secret values in logs, events, snapshots, or UI.
