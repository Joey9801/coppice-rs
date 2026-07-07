//! Wire-up point for openraft's storage compliance suite (ADR 0002).
//!
//! ADR 0002: "The storage implementation must pass openraft's storage test
//! suite, plus our own crash-injection tests." This file is the first half of
//! that sentence. It is cfg-gated because the segment storage engine does not
//! exist yet; the module below is the exact wiring the engine task completes.
//!
//! # STORAGE TASK: FLIP THIS ON
//!
//! When the segment storage engine lands in this crate:
//!
//! 1. Point the `use` lines in `suite` at the real `RaftLogStorage` and
//!    `RaftStateMachine` implementations and finish `build()`.
//! 2. Run `cargo test -p coppice-consensus --features storage-compliance`.
//! 3. Once green, delete the feature gate (here and in Cargo.toml) so the
//!    suite runs unconditionally — it is a merge gate, not an option.

/// A visible reminder in every ordinary test run that the compliance suite is
/// not yet wired to a real engine. Deliberately not deleted until step 3
/// above happens.
#[test]
#[ignore = "segment storage engine not yet implemented; see module docs — run with --features storage-compliance once it lands"]
fn openraft_storage_compliance_is_pending() {
    panic!(
        "the openraft compliance suite must be enabled (and this placeholder deleted) \
         when the segment storage engine lands"
    );
}

#[cfg(feature = "storage-compliance")]
mod suite {
    use openraft::testing::{StoreBuilder, Suite};
    use openraft::StorageError;

    use coppice_consensus::{CoordinatorId, TypeConfig};
    // TODO(storage-engine): these are the intended names; adjust to reality.
    // The log store and state-machine adapter must be constructible over any
    // `coppice_consensus::fs::Fs`; the compliance suite runs them on `RealFs`
    // in a tempdir.
    use coppice_consensus::storage::{SegmentLogStorage, StateMachineStore};

    struct SegmentStoreBuilder;

    impl StoreBuilder<TypeConfig, SegmentLogStorage, StateMachineStore, tempfile::TempDir>
        for SegmentStoreBuilder
    {
        async fn build(
            &self,
        ) -> Result<
            (tempfile::TempDir, SegmentLogStorage, StateMachineStore),
            StorageError<CoordinatorId>,
        > {
            let dir = tempfile::tempdir().expect("tempdir for compliance suite");
            // TODO(storage-engine): initialize a data dir on RealFs::new(dir.path())
            // (identity stamps per ADR 0016), open through recovery, and return
            // the log store + state-machine adapter.
            todo!("construct SegmentLogStorage / StateMachineStore over RealFs")
        }
    }

    #[test]
    fn openraft_storage_compliance() {
        // `Suite::test_all` exercises vote persistence, log append/truncate/
        // purge, snapshot build/install, and restart state — the contract our
        // recovery procedure (ADR 0017) must satisfy from openraft's side.
        Suite::test_all(SegmentStoreBuilder).expect("openraft storage compliance");
    }
}
