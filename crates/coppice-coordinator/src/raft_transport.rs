//! The Raft transport as this daemon mounts it: the consensus crate's handler
//! behind this daemon's inbound test gates.
//!
//! A pass-through in every real deployment — [`Failpoints`] is disarmed for
//! any config without `[test_failpoints]`, and that section cannot load in a
//! release build — so the only thing this wrapper adds on the hot path is one
//! `Option` discriminant per `AppendEntries`. It exists so a debug-build test
//! can hold a replica's replication stream still while the rest of the daemon
//! runs (issue #148): the consensus crate owns the handler, the coordinator
//! owns the failpoints, and a mounted tonic service cannot be unwrapped, so
//! the seam has to sit here, between the two.

use tonic::{Request, Response, Status, Streaming};

use coppice_consensus::{RaftTransportHandler, RaftTransportService};
use coppice_proto::pb::raft::v1 as pb;

use crate::failpoints::{self, Failpoints};

/// [`RaftTransportHandler`] with this daemon's inbound gates in front of it.
pub(crate) struct GatedRaftTransport {
    inner: RaftTransportHandler,
    failpoints: Failpoints,
}

impl GatedRaftTransport {
    pub(crate) fn new(inner: RaftTransportHandler, failpoints: Failpoints) -> Self {
        GatedRaftTransport { inner, failpoints }
    }
}

#[tonic::async_trait]
impl RaftTransportService for GatedRaftTransport {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        // Before the handler sees the request at all: while the gate is held
        // this replica's log, membership and leader knowledge all stand
        // exactly where they were, which is the state issue #148 needs to
        // hold open. Disarmed — and unarmable — in every real deployment.
        self.failpoints
            .gate_all_if_armed(failpoints::RAFT_APPEND_ENTRIES_RECEIVED)
            .await;
        self.inner.append_entries(request).await
    }

    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        self.inner.vote(request).await
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<pb::InstallSnapshotRequest>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        self.inner.install_snapshot(request).await
    }
}
