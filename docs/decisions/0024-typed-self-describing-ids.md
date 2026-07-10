# 24. Typed, self-describing entity ids, generated as UUIDv7

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

Every entity id (job, node, allocation, attempt, group, quota entity,
cluster) was a bare random UUIDv4. Two problems surfaced in review:

1. **A serialized id says nothing about its type.** In the Rust domain
   model the newtypes (`JobId`, `NodeId`, …) make ids unmixable, and on the
   wire the typed messages in `coppice.core.v1` do the same — but only at
   the schema level. The moment an id leaves those type systems (a log
   line, a JSON body, a support bundle, a captured payload, a value pasted
   into a CLI), it degrades to an anonymous
   `1683852a-993f-4497-a48b-6527b458fbd1` and the reader must guess what it
   names. Confusing an allocation id for an attempt id in an operational
   incident is exactly the mistake the newtypes exist to prevent.

2. **Random v4 ids are hostile to ordered indexes.** The state machine
   keys `BTreeMap<JobId, …>`, `BTreeMap<AttemptId, …>`,
   `BTreeMap<AllocationId, …>` (and more) directly on the id bytes. With
   v4, inserts land uniformly across the tree: poor memory locality, more
   node splits/rebalancing, and any future disk index built in id order is
   built in random order.

There are no deployments yet, so wire-format and config-format
compatibility carry no weight (the proto baseline gate is simply
re-baselined in the same change).

## Decision

### Every serialized form is `<prefix>-<uuid>`

The canonical textual form of an id is its type prefix, a hyphen, then the
canonical hyphenated UUID, e.g. `job-1683852a-993f-4497-a48b-6527b458fbd1`:

| Type | Prefix |
| --- | --- |
| `JobId` | `job` |
| `NodeId` | `node` |
| `AllocationId` | `alloc` |
| `AttemptId` | `attempt` |
| `GroupId` | `group` |
| `QuotaEntityId` | `quota` |
| `ClusterId` | `cluster` |

This form is used **everywhere an id is serialized**: `Display` (so every
log line is self-describing), serde (config files, future JSON edges), and
the protobuf wire form — the `coppice.core.v1` id messages carry
`string value` holding the typed form instead of 16 raw bytes. Parsing
(`FromStr`, serde, and the pb→domain conversion boundary) requires the
prefix to match the expected type: a `job-…` string offered where a
`NodeId` is expected is rejected (`ConvertError::InvalidId`), which extends
the "ids stay unmixable" guarantee down to raw payload bytes.

In-memory, ids remain newtypes over `uuid::Uuid` (16 bytes, `Copy`); the
prefix exists only at serialization boundaries. Entropy is unchanged.

Two identity stamps deliberately stay raw 16-byte `bytes`: the storage
manifest's cluster/instance uuids and the raft transport's `cluster_uuid`
cross-check. They are internal fail-stop guards inside fixed binary
formats, not ids a human routes on; `ClusterId` converts to raw bytes at
that boundary (`id.0.as_bytes()`).

The agent's mTLS certificate CN now carries the typed form
(`node-<uuid>`), since the gateway compares the CN against
`NodeId::to_string()`.

### `new()` mints UUIDv7

`JobId::new()` (and every other id's `new()`) generates UUIDv7 —
millisecond timestamp prefix, random tail — instead of v4. New ids
therefore sort roughly by creation time, so inserts land at the right edge
of the `BTreeMap`s: better locality, less rebalance churn, and
creation-ordered iteration for free. Uniqueness still rests on the 74
random bits; nothing anywhere *relies* on the timestamp ordering (ids from
different processes with skewed clocks may interleave arbitrarily), it is
an mechanical sympathy optimization only.

`new()` lives on the id type itself so call sites never choose a UUID
version; the testkit's deterministic generator keeps minting ids from its
seeded RNG (reproducibility outranks locality in synthetic corpora).

## Consequences

- An id encountered anywhere — logs, TOML, JSON, a decoded protobuf, a
  colleague's Slack message — names its own type. Grepping for one id
  across systems works because there is exactly one textual form.
- Type confusion becomes a parse error at every boundary, not just a Rust
  compile error.
- Id messages on the wire grow from ~18 bytes to ~40–48 bytes. Accepted:
  clarity outranks bytes at this scale, and the protobuf schema remains
  free to move back to packed forms behind the same conversion boundary if
  it ever matters.
- B-tree-keyed state gets locality and mostly-append behaviour from v7's
  time-ordered prefix.
- Ids now leak coarse creation time. Acceptable for a batch scheduler's
  internal entities; anything privacy-sensitive would need its own
  decision.
- The proto baseline (`proto/baseline.binpb`) was re-baselined: `bytes` →
  `string` is a wire-format break, permitted exactly because nothing is
  deployed.
