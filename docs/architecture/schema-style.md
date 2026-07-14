# Protobuf Schema Style and Evolution

The `proto/` tree at the workspace root is the canonical schema corpus for
every durable and cross-process format
([ADR 0003](../decisions/0003-protobuf-serialization-and-cluster-version-gates.md)).
This document is the rulebook for changing it. The rules exist because the
bytes these schemas describe are replicated over the wire, fsynced into log
segments, and replayed for years across rolling upgrades — a field tag chosen
here is frozen forever.

Package layout, one directory per versioned package:

| Package | Contents |
| --- | --- |
| `coppice.core.v1` | Shared vocabulary: typed ids, resources, job/attempt/allocation/node, quota and policy types |
| `coppice.command.v1` | The Raft log command set in its versioned envelope (frozen from the [command catalog](command-catalog.md)) |
| `coppice.raft.v1` | Our own Vote/LogId/membership messages and the log-entry envelope (ADR 0018: never openraft's serde forms) |
| `coppice.storage.v1` | Manifest and snapshot payloads (ADR 0015/0017/0018) |
| `coppice.agent.v1` | Agent↔coordinator protocol (ADR 0009) |
| `coppice.api.v1` | Public API types, JSON-mapped at the edge |

Generated Rust lands in `coppice_proto::pb`, compiled by
`crates/coppice-proto/build.rs` (protox + prost-build; no system `protoc`).

Remember the two-layer split (ADR 0015): file magic, container versions,
framing, and CRCs are fixed-layout binary owned by the storage layer. These
schemas are the *payloads* inside those containers and evolve by the rules
here; containers evolve by ADR 0015's.

## The migration decision: generated types at the boundary only

Decided with the schema corpus (the ADR 0003 consequence, resolved per
layer):

- **Wire and disk: generated types are the only types.** Anything encoded,
  decoded, or hashed is a `coppice_proto::pb` type. No hand-written struct
  may shadow a wire format — the deleted serde placeholders in
  `coppice-proto` do not come back.
- **The domain keeps hand-written types where invariants and behavior
  live.** `coppice-core` (state machines, saturating arithmetic, typed ids)
  and `coppice-state` (commands, records, apply) stay ergonomic Rust,
  mirroring the schema field for field.
- **`coppice_proto::convert` is the boundary.** Domain → pb is infallible
  and canonicalizing; pb → domain is fallible with a typed `ConvertError`.
  What a conversion error *means* depends on the surface:
  - **Committed log entries**: a deterministic `InvalidCommand` rejection at
    apply — decode is a pure function of the bytes, so every replica refuses
    identically. Shape rules the catalog assigns to apply (the v1
    single-allocation placement) are deliberately *not* checked in
    conversion, so apply can reject them itself as
    `UnsupportedPlacementShape`.
  - **Snapshots and the manifest**: fail-stop. A CRC-valid record that fails
    to decode or convert is a schema break or corruption, never something to
    skip.
  - **API and agent edges**: an invalid request/report, rejected at ingress.
- **serde is not a wire encoding.** It may return for genuinely non-wire
  uses (the TOML node config file of
  [ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md)); it
  must never re-appear as a parallel encoding of anything in `proto/`.

## Tag discipline

- **Never reuse or renumber a tag. Never rename-in-place a tag's meaning.**
  Changing a field's type, label (`repeated` ↔ singular), or containing
  oneof is breakage the same as renumbering.
- **Removal is by reservation.** Removing a field: delete it and add both
  `reserved <tag>;` and `reserved "<name>";` in the same change. Same for
  enum values. The name reservation protects the JSON mapping.
- **Do not use `reserved` for headroom.** `reserved` means *never again*;
  future growth is expressed by allocation-plan comments (see the `Command`
  envelope's body-tag ranges) and by simply taking the next free tag later.
- **Mind the 1–15 one-byte range.** Spend low tags on fields that are hot or
  on every message instance; envelopes keep 1–7 for envelope metadata and
  start payload arms at 8.
- New fields are always additive and take the next free tag in their range;
  a field with semantic weight is *written* only after the `ClusterVersion`
  gate permits (readable-by-all comes first — ADR 0003's read-new/write-old
  choreography).

## Enums

- Zero value is always `<ENUM_NAME>_UNSPECIFIED` and is never a legal stored
  value; every value carries the enum-name prefix (buf STANDARD style).
- **Replicated payloads treat enums as closed.** An unknown value at the
  conversion boundary is an error (`ConvertError::UnknownEnum`), never a
  silent default or drop — an unknown value in committed bytes means the
  ClusterVersion write gate was violated, and the loud failure is the point.
  This applies doubly to priced dimensions: dropping an unknown
  `ResourceKind` would corrupt accounting.
- New enum values are additive and version-gated like new fields.

## Rich enums are oneofs

A Rust enum whose variants carry data (`AttemptOutcome::Exited { code }`)
is a `oneof` of per-variant messages. Every variant gets its own message
**even when empty** (`OomKilled {}`), so any variant can grow fields later
without restructuring. Flat data-free enums (`JobState`) stay proto enums.

Never encode a value that is *derived* from another field: outcome
classification (success / user error / user request / platform) is a pure
function of `AttemptOutcome` per ADR 0013's table, so a settable
classification field could only ever disagree with it.

## Repeated fields: presence and required-ness

A proto3 `repeated` field has no presence: empty and absent are the same
bytes. Two conventions follow (first instances: `Job.command` /
`Job.entrypoint`):

- **A required repeated field is enforced as non-empty at conversion.**
  Emptiness *is* the missing-field check — pb → domain rejects it with
  `ConvertError::MissingField`, same as a missing required message.
- **An optional repeated field is wrapped in its own message** (e.g.
  `Entrypoint { repeated string argv = 1; }`) so absence is real presence
  information. The wrapped list must be non-empty: present-but-empty would
  be a second encoding of "absent", and canonical form allows only one.

## Scalars and representations

- **Timestamps are `int64` Unix microseconds**, field names `*_at_us`
  (durations: `*_us`). Never `google.protobuf.Timestamp`: it splits
  seconds/nanos (two varints, not fixed-width semantics), maps to an
  RFC 3339 *string* in JSON, and invites nanosecond precision the
  deterministic apply contract never wants.
- **Entity ids are typed strings** (`<prefix>-<uuid>`, e.g.
  `job-1683852a-…`), wrapped in typed id messages (`JobId`, `NodeId`, …) so
  ids stay unmixable in generated code *and* self-describing in any captured
  payload, log line, or JSON body (ADR 0024). The prefix and the uuid are
  validated at the conversion boundary (`ConvertError::InvalidId`), never
  assumed. (Raft *coordinator* ids are allocate-once `uint64`s per ADR 0016
  and are not UUIDs; the cluster/instance identity stamps in the storage
  manifest and raft transport remain raw 16-byte `bytes` — internal
  cross-checks, not user-facing ids.)
- **No `float`/`double` anywhere replicated or hashed**
  ([ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md)): cost
  and usage are `uint64` µCU, weights and multipliers `uint64` Q32.32, the
  decay factor `uint64` Q0.64. Field names carry unit and representation:
  `*_ucu`, `*_q32_32`, `*_q0_64`. Floats exist only in derived scheduler
  state and are never serialized.
- Signed values that can be negative (`priority`, exit codes) use `sint32`
  (zigzag), not `int32`.

## No proto maps in anything replicated or hashed

`map<k, v>` serialization order is unspecified — two correct encoders may
produce different bytes for equal values. Anything replicated, snapshotted,
or hashed uses **repeated entry messages with a canonical order** instead:

- Canonical form: ascending by key (byte order for strings, numeric
  otherwise), unique keys, zero/empty entries omitted.
- Writers *always* emit canonical form — `coppice_proto::convert` does this
  by construction (`BTreeMap` iteration, zero-skipping).
- Readers accept any order (the domain type re-sorts) but reject duplicate
  keys.

Current instances: `Resources.quantities`, `CostWeights.weights`,
`Node.labels` / `RegisterNode.labels`,
`PolicyConfig.priority_multipliers`, `Membership.members`,
`VoterConfig.voters`. Note the same caveat applies to protobuf encoding in
general: it is not canonical across implementations, so byte-level
determinism is a *writer discipline* — one implementation (prost via
`coppice-proto`), canonical entry order, no unknown fields retained.

## Envelopes and unknown payloads

The log-entry chain is `coppice.raft.v1.LogEntry` (Normal / Membership /
Blank) wrapping `coppice.command.v1.Command { version, oneof body }`:

- `Command.version` is the ClusterVersion the proposer wrote under. New
  command arms take the next tag in their allocated range (the envelope
  documents per-proposer ranges; gang scheduling grows the scheduler range).
- A decoder that does not know an arm sees an empty `body` (prost discards
  unknown fields) and errors at conversion. That is unreachable while the
  write gate holds: a new arm is not *written* until every replica *reads*
  it. The error, not a skip, is what makes gate violations visible.
- The same pattern governs `coppice.agent.v1.AgentCommand` /
  `AgentReport` — and every coordinator→agent command carries the common
  `CommandHeader` (fencing token + `command_seq`, ADR 0009) rather than
  copy-pasted fields.

## Packages and the version suffix

`.v1` in the package name is the *schema generation*, not the semantic
version: semantic evolution happens additively inside `.v1` under the
ClusterVersion gate. A `.v2` package is a last resort for an incompatible
redesign, requires an ADR, and means carrying both generations through a
migration window. Do not create one casually.

## JSON at the edge

The HTTP API does **not** serialize these messages: its JSON bodies are
the handwritten serde DTOs in `coppice-api::http::dto` (ADR 0031 as
amended), kept in sync with the `coppice.api.v1` shapes by review. A
schema field rename therefore no longer leaks into the JSON contract —
but renames remain forbidden here for the protobuf reasons above, and a
rename in the DTO module is a deliberate public-API break with its own
review bar.

## The breaking-change gate

Tag breakage is caught mechanically, not by review:

- `crates/coppice-proto/tests/breaking.rs` diffs the compiled descriptor set
  against the committed baseline `proto/baseline.binpb` and fails on:
  removed/renamed messages or enums; a field number whose type, label,
  name, or oneof changed; a removed field number or enum value that was not
  reserved (number *and* name); removed reservations.
- Run it via `scripts/proto-check.sh` (CI entry point) or directly:
  `cargo test -p coppice-proto --test breaking`.
- If [`buf`](https://buf.build) is installed the script additionally runs
  `buf lint` (STANDARD) and `buf breaking` against the committed `main`
  baseline using the repo `buf.yaml`; buf is optional — the vendored
  descriptor diff is the gate of record.
- After an *intentional, rules-compliant* schema change (additive field,
  new command arm, reservation), refresh the baseline with
  `UPDATE_PROTO_BASELINE=1 cargo test -p coppice-proto --test breaking`
  and commit `proto/baseline.binpb` in the same change. The diff of the
  baseline is part of review.

## Checklist for common changes

**Adding a field**: next free tag in the message's range; scalar defaults
must be safe for old data (absent = zero/empty); conversion updated; if the
field changes behavior, gate the *writer* on ClusterVersion; refresh the
baseline.

**Adding a command**: catalog entry first
([command-catalog.md](command-catalog.md)), then the message + envelope arm
in its proposer's tag range, domain type + conversion + apply handler,
write-gated by ClusterVersion; refresh the baseline.

**Adding an enum value / resource kind**: additive, prefixed, gated;
readers of the previous binary will *reject* payloads carrying it — that is
the design, so the gate must precede any writer.

**Removing anything**: reserve the tag and the name in the same commit;
domain type and conversion drop it; the baseline diff will show exactly the
reservation.
