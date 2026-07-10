//! openraft's storage compliance suite over the segment storage engine
//! (ADR 0002).
//!
//! ADR 0002: "The storage implementation must pass openraft's storage test
//! suite, plus our own crash-injection tests." This file is the first half of
//! that sentence (the crash suite is `crash_storage.rs`). It runs
//! unconditionally — a merge gate, not an option.

use openraft::testing::{StoreBuilder, Suite};
use openraft::StorageError;

use coppice_consensus::fs::RealFs;
use coppice_consensus::storage::{self, SegmentLogStorage, StateMachineStore, StorageOptions};
use coppice_consensus::{CoordinatorId, TypeConfig};

/// One fixed cluster identity for the whole suite: `transfer_snapshot` builds
/// a snapshot on one store and installs it on another, and the engine
/// refuses cross-cluster snapshots (ADR 0016), so every built store must
/// belong to the same "cluster".
const CLUSTER_UUID: [u8; 16] = *b"compliance-suite";

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
        let options = StorageOptions::new(CLUSTER_UUID);
        storage::init(&RealFs::new(dir.path()), &options).expect("init data dir");
        let recovered =
            storage::open(RealFs::new(dir.path()), options).expect("open through recovery");
        let (log, sm) = recovered.into_stores_with_local_apply_task();
        Ok((dir, log, sm))
    }
}

#[test]
fn openraft_storage_compliance() {
    // `Suite::test_all` exercises vote persistence, log append/truncate/
    // purge, snapshot build/install, and restart state — the contract our
    // recovery procedure (ADR 0017) must satisfy from openraft's side.
    Suite::test_all(SegmentStoreBuilder).expect("openraft storage compliance");
}
