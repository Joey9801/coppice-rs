//! # coppice-proto
//!
//! The serialization boundary: prost-generated protobuf types for every
//! durable and cross-process format (ADR 0003), plus the conversions
//! between them and the domain types in `coppice-core` / `coppice-state`.
//!
//! The schema source of truth is the `proto/` tree at the workspace root —
//! six versioned packages covering the Raft log ([`pb::command`],
//! [`pb::raft`]), snapshots and the manifest ([`pb::storage`]), the
//! agent–coordinator protocol ([`pb::agent`]), and the public API
//! ([`pb::api`]). Field tags there are frozen; evolution follows
//! `docs/architecture/schema-style.md`, and `tests/breaking.rs` enforces it
//! mechanically against the committed baseline.
//!
//! The migration decision (documented in schema-style.md): generated types
//! are the *only* types on the wire and on disk; the domain keeps
//! hand-written types where invariants and behavior live, converting at
//! this crate's [`convert`] boundary — infallible outbound, fallible
//! inbound.

pub mod convert;
pub mod pb;
