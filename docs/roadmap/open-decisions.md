# Important Open Design Decisions

Several areas require further design before implementation. Each of these should
eventually be resolved by an [Architecture Decision Record](../decisions/).

- The exact Raft library and persistence layer need to be selected.
- The command and snapshot schema versioning strategy needs to be defined.
- The job lifecycle state machine needs to be formalized.
- The scheduler's quota and priority policy needs a precise specification.
- The reservation and backfilling model for large jobs needs more detailed
  design.
- The consistency model for reads from followers needs to be decided.
- The event subscription delivery guarantees need to be specified.
- The agent fencing and reconciliation protocol needs to be defined.
- The image-cache policy needs to distinguish between local agent autonomy and
  central scheduling hints.
- The security model for container execution needs clear boundaries.
- The data retention policy for events, metrics, logs, and job history needs to
  be chosen.
