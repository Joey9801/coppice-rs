//! # coppice-testkit
//!
//! The storage-layer test infrastructure mandated by ADR 0002 ("the
//! crash-injection suite gates any change to the storage layer") and
//! ADR 0018 (the benchmark suite with the same gating role). See
//! `docs/architecture/storage-testing.md` for the crash model, the invariant
//! list, and how to reproduce failures from a seed.
//!
//! Contents:
//!
//! - [`rng`] — a tiny, dependency-free, forever-stable deterministic RNG.
//!   Every random choice in this crate flows from one logged seed.
//! - [`simfs`] — [`SimFs`](simfs::SimFs), the fault-injecting implementation
//!   of the `coppice_consensus::fs::Fs` seam: tracks data vs. metadata
//!   durability separately and lets a seeded adversary decide the fate of
//!   every un-fsynced write at a simulated crash.
//! - [`harness`] — the crash-injection driver: enumerate crash points,
//!   crash, recover, assert the durability invariants.
//! - [`toy`] — a miniature reference storage engine implementing the
//!   ADR 0017 protocol (segments, manifest, vote, snapshot, recovery)
//!   against the fs seam. It exists to prove the harness catches real bugs
//!   and to document the recovery procedure executably; the real engine
//!   replaces it in the suite when it lands.
//! - [`synth`] — the synthetic `StateMachine` generator shared by the
//!   snapshot benchmarks and the determinism suite.
//!
//! This crate is test infrastructure: it appears only in `[dev-dependencies]`
//! of other crates, never in a shipping dependency graph.

pub mod harness;
pub mod rng;
pub mod simfs;
pub mod synth;
pub mod toy;
