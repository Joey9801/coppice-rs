# State-Machine Evolution and Versioning

The replicated state model must be designed for evolution. The concrete
mechanism — protobuf with additive-only evolution rules and a Raft-replicated
`ClusterVersion` gating semantic changes — was decided in
[ADR 0003](../decisions/0003-protobuf-serialization-and-cluster-version-gates.md).
The *container* formats around those payloads (segment files, vote file,
manifest, snapshot files) are versioned separately, under the same
`ClusterVersion` gate — see
[ADR 0015](../decisions/0015-durable-format-versioning.md).

The schemas themselves live in the `proto/` tree at the workspace root
(compiled into `coppice_proto::pb`); the day-to-day evolution rules — tag
discipline, enum policy, representation rules, and the mechanical
breaking-change gate (`scripts/proto-check.sh`, backed by the committed
descriptor baseline `proto/baseline.binpb`) — are in
[schema-style.md](schema-style.md).

Old log entries may be replayed by newer binaries. Snapshots may be read by
newer binaries. During rolling upgrades, different coordinator replicas may
briefly run different versions.

The system should use explicit versioning for:

- Command formats.
- Snapshot formats.
- Durable state schemas.
- Policy definitions.
- Feature gates.
- Agent protocol compatibility.

Backward-compatible changes are the safest. Examples include adding optional
fields, adding new command types not emitted until all replicas support them, or
adding derived indexes.

Riskier changes require migration planning. Examples include changing command
semantics, changing scheduler policy in a way that affects existing
accruing allocations, changing quota accounting, or changing job lifecycle
meaning.

## Upgrade strategy

A useful upgrade strategy is:

1. Deploy code that can read old and new formats but still writes old format.
2. Confirm the whole cluster is upgraded.
3. Enable a feature gate or cluster-version bump through Raft.
4. Begin writing the new format or using the new semantics.
5. Keep downgrade limitations explicit.

## Rollback

Rollback is not always possible. If a new version writes log entries or
snapshots that the old version cannot understand, rolling back to the old binary
may be unsafe or impossible without a forward-fix. To preserve rollback
capability, the system must avoid enabling irreversible format or semantic
changes until it is intentionally committed to them.

Bad scheduler behavior is easier to roll back than bad state-machine format
changes if placement policy is kept outside the deterministic application path.
However, any committed placements, allocations, or quota changes remain part of
history and must be corrected through new commands rather than by rewriting the
log.
