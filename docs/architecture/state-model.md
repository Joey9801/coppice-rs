# State Model

The system should be explicit about desired state, observed state, and derived
state.

**Desired state** is what the control plane intends to happen. Examples include
submitted jobs, abort requests, assignments, accruing allocations, node drain
requests,
and policy configuration.

**Observed state** is what agents and monitoring systems report. Examples include
running containers, exited containers, resource usage, health checks, and image
cache contents.

**Derived state** is recomputable from desired and observed state. Examples
include indexes, queue projections, scheduling candidate sets, UI aggregates,
and many metrics.

Only durable semantic state required for correctness should be stored in the
Raft state machine. Derived state should be rebuilt from snapshots or committed
state where possible.

See also [data-storage-boundaries.md](data-storage-boundaries.md) for where each
kind of state physically lives, and
[../architecture/components.md](components.md) for the specific facts the Raft
state machine replicates.
