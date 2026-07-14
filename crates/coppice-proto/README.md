# coppice-proto

The serialization boundary. It owns the protobuf wire schema for every durable
and cross-process format, and the conversions between the generated types and
the hand-written domain types in `coppice-core` / `coppice-state`.

## The proto corpus

The schema source of truth is the `proto/` tree at the workspace root — one
directory per versioned package: shared vocabulary (`coppice.core.v1`), the
Raft log command set (`coppice.command.v1`), our own Raft envelope and
membership types (`coppice.raft.v1`), snapshot and manifest payloads
(`coppice.storage.v1`), the agent↔coordinator protocol (`coppice.agent.v1`),
and the public API (`coppice.api.v1`, the cross-language description of the
HTTP surface — the edge itself serializes handwritten DTOs, not these types;
ADR 0031 as amended). These are the *only* types on the wire and on disk;
nothing else may shadow a wire format.

Field tags are frozen forever — the bytes are replicated over the wire, fsynced
into log segments, and replayed across years of rolling upgrades. All the
evolution rules (tag discipline, enum policy, no floats, no proto maps in
anything hashed, timestamps as `int64` microseconds, UUIDs as `bytes(16)` in
typed id wrappers) live in [schema-style.md](../../docs/architecture/schema-style.md).
The design rationale is [ADR 0003](../../docs/decisions/0003-protobuf-serialization-and-cluster-version-gates.md)
(protobuf + `ClusterVersion` gates) and
[ADR 0018](../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)
(why these records live inside parallel-decodable containers rather than one
monolithic message).

## Codegen

[`build.rs`](build.rs) compiles the whole corpus with [`protox`](https://docs.rs/protox)
— a pure-Rust protobuf compiler, so the build needs no system `protoc` — and
hands the resulting descriptor set to `prost-build`. Generated Rust lands under
[`pb`](src/pb.rs), one module per package (`pb::command::v1`, `pb::core::v1`, …).

The compiled `FileDescriptorSet` is also written to `OUT_DIR`, which feeds the
**breaking-change gate** in [`tests/breaking.rs`](tests/breaking.rs): a
mechanical descriptor diff against the committed baseline `proto/baseline.binpb`,
so tag breakage (a removed/renamed message, a field that changed type/label/
name/oneof, a removal without a reservation) is caught by CI rather than by
review. After an intentional, rules-compliant change, refresh the baseline with
`UPDATE_PROTO_BASELINE=1 cargo test -p coppice-proto --test breaking` and commit
it in the same change. `buf breaking` (via `scripts/proto-check.sh`) is an
optional extra check; this vendored diff is the gate of record.

## The convert layer

[`convert`](src/convert.rs) is the boundary between generated wire types and the
ergonomic domain types that hold the real invariants, arithmetic, and behavior.
The direction of fallibility is the contract:

- **domain → pb is infallible**, and *canonicalizing*: repeated key-sorted
  entries are emitted in key order with zero/empty entries omitted, so equal
  domain values encode to identical bytes (byte determinism is writer
  discipline — see schema-style).
- **pb → domain is fallible**, returning a typed `ConvertError` (wrong-length
  UUID, missing required message, unknown enum value, duplicate key). What the
  error *means* depends on the surface: a deterministic `InvalidCommand`
  rejection on a committed log entry, fail-stop corruption on a snapshot or
  manifest, a bad request at the API or agent edge.

Submodules mirror the packages: `convert/core.rs` (ids, resources, the
quota/policy vocabulary), `convert/command.rs` (the versioned `Command` envelope
and every command arm, frozen from the
[command catalog](../../docs/architecture/command-catalog.md)), and
`convert/snapshot.rs` (per-entity snapshot records and whole-state
assembly/disassembly). Catalog-level shape rules — such as the v1
single-allocation placement — are deliberately *not* checked here, so apply can
see those payloads and reject them itself.

## Cluster version gating

New command arms, new fields with semantic weight, and new enum values are
readable by any supporting binary but not *written* until the replicated
`ClusterVersion` is bumped (read-new/write-old choreography, ADR 0003). A
decoder that meets an arm it does not know sees an empty envelope body and
errors at conversion — unreachable while the write gate holds, and a loud
signal if it is ever violated. See
[versioning.md](../../docs/architecture/versioning.md) for the upgrade model.
