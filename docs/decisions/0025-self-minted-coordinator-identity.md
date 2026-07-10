# 25. Coordinator raft identity is minted at init, not configured

- **Status:** Accepted
- **Date:** 2026-07-10
- **Amends:** the operational halves of
  [ADR 0016](0016-coordinator-rebuild-learner-join.md) ("the operator
  allocates a fresh node id") and
  [ADR 0020](0020-node-config-vs-replicated-policy.md) ("`node_id` is in the
  file"). The safety rules of ADR 0016 — allocate-once identities, never
  reused, empty disks enter only as learners — are unchanged; only *who
  picks the number* changes.

## Context

A coordinator's raft node id was a `u64` the operator chose and wrote into
`coordinator.toml`, cross-checked against the data directory's manifest
stamp at startup. ADR 0016 already demanded that ids be allocate-once
instance identities ("a rebuilt machine always joins with a fresh node ID…
no node ID is ever reused" and "discovery/config cannot hard-code
'node 1/2/3' forever") — but left the allocation itself manual.

Manual allocation is exactly the kind of ceremony that goes wrong during a
3 a.m. rolling rebuild: pick a number, check it was never used before
(against what ledger?), keep it in lockstep with the disk stamp, repeat per
replica per rebuild. The failure modes ADR 0016's cross-check catches —
config and volume disagreeing — exist *because* the same fact lives in two
places.

## Decision

The raft node id is **minted by the process itself** at data-directory
initialization (`--bootstrap` / `--join`): 64 uniform random bits, stamped
into the manifest exactly like the instance UUID, and **read back from the
stamp on every restart**. `node_id` is deleted from the config file;
`StorageOptions` no longer carries a node id; `storage::init` returns the
minted id and `storage::open` surfaces the stamped one.

Consequences for the ADR 0016 startup matrix:

- **Restart** resumes whatever replica the attached volume carries. The
  node-id half of the identity cross-check disappears — with a single
  authority there is nothing to disagree. The cluster-UUID cross-check
  stays (the config still names which cluster this process should serve,
  and a wrong-cluster volume still fail-stops).
- **Bootstrap / Join** mint a fresh id on the empty directory, preserving
  "an empty disk can only enter as a new instance" — now with zero
  opportunity to accidentally reuse a number.

Random rather than sequential because sequential requires an allocator (a
ledger someone must own — the exact ceremony being removed); collision
probability across a cluster's entire membership history is negligible at
64 bits (birthday bound ≈ n²/2⁶⁵), and openraft's id type stays `u64` so
neither the manifest binary format, the raft protos, nor the admin surface
change shape.

The operator learns a replica's identity instead of assigning it:

- every start logs `coordinator raft identity` (`node_id = …`);
- `admin status` reports the local id and full membership;
- the `add-learner` / `promote --remove` verbs take the logged id, exactly
  as before.

## Consequences

- One less hand-maintained fact per replica; a rolling rebuild's per-node
  config becomes identical apart from addresses, which is what makes
  templated/immutable infrastructure (ASGs, cloud-init) practical.
- The "volume attached to the wrong instance" refusal is gone by
  construction: the volume *is* the instance. What remains detectable —
  and detected — is a volume from the wrong cluster.
- Two processes must never share a data directory; that was already true
  and is enforced by the `LOCK` file (ADR 0017) plus mundane operational
  hygiene (one volume, one attachment).
- Join choreography now reads the id from the new node's log/status rather
  than knowing it a priori; the planned `coordinator replace` automation
  (ADR 0016) should carry the id programmatically end to end.
- Test harnesses that used to pin ids (`node_id = 1`) discover them from
  the booted handle instead.
