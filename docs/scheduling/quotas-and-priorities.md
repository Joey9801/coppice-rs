# Quotas and Priorities

Quota and priority management should be built into the scheduler rather than
bolted on later.

The system should support multiple levels of ownership, such as user, project,
organization, queue, or service account.

The design should distinguish between:

- Admission control.
- Queue ordering.
- Scheduling priority.
- Fair-share allocation.
- Hard resource limits.
- Burst allowances.
- Preemption policy, if added later.

Quota accounting should be replicated when it affects scheduling decisions.
Recomputable projections may be derived, but the committed state must be
sufficient to avoid inconsistent decisions after failover.

The scheduler should be able to explain why a job is pending due to quota,
priority, constraints, resource shortage, reservation, or policy. This
explainability requirement is shared with
[../operations/observability.md](../operations/observability.md).

The precise quota and priority policy specification is an
[open design decision](../roadmap/open-decisions.md).
