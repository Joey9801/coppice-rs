//! The server side of the Raft transport: a [`RaftTransportService`] over the
//! local [`Raft`] handle (ADR 0002/0016).
//!
//! Every RPC first checks the request's stamped cluster identity against this
//! node's, refusing cross-cluster contact before touching Raft state (ADR
//! 0016). Protocol-level Raft outcomes (a higher vote, a log conflict) are not
//! errors — they ride back inside the response message; only a
//! [`Fatal`](openraft::error::Fatal) raft fault and a malformed request become
//! a [`Status`].

// tonic's generated service trait returns `Result<_, Status>`; `Status` is a
// large error type, and the signature (plus `check_cluster`, which feeds it)
// is dictated by that trait.
#![allow(clippy::result_large_err)]

use std::fmt::Write as _;
use std::io;

use tonic::{Request, Response, Status, Streaming};

use openraft::{Raft, Snapshot};

use coppice_proto::pb::raft::v1 as pb;
use coppice_net::transport::RaftTransportService;

use crate::adapter::TypeConfig;
use crate::storage::{raftpb, SnapshotFile};

use super::convert;

/// Serves the coordinator Raft transport over this node's [`Raft`] handle.
///
/// Wrapped in the generated `Server` and mounted on the coordinator's mTLS
/// server (ADR 0011); see `node.rs`.
pub struct RaftTransportHandler {
    raft: Raft<TypeConfig>,
    cluster_uuid: [u8; 16],
}

impl RaftTransportHandler {
    /// Bind the handler to the local Raft node and its stamped cluster identity.
    pub fn new(raft: Raft<TypeConfig>, cluster_uuid: [u8; 16]) -> Self {
        RaftTransportHandler { raft, cluster_uuid }
    }

    /// Refuse a request stamped for a different cluster (ADR 0016).
    ///
    /// Names both identities in hex so an operator can see the cross-cluster
    /// mixup at a glance.
    fn check_cluster(&self, incoming: &[u8]) -> Result<(), Status> {
        if incoming == self.cluster_uuid {
            return Ok(());
        }
        Err(Status::failed_precondition(format!(
            "request is from cluster {}, this node is stamped for cluster {} — \
             cross-cluster contact refused (ADR 0016)",
            hex(incoming),
            hex(&self.cluster_uuid),
        )))
    }
}

/// Lowercase hex of raw identity bytes, for operator-facing error messages.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[tonic::async_trait]
impl RaftTransportService for RaftTransportHandler {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        let rpc = convert::append_entries_from_pb(req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(convert::append_response_to_pb(resp)))
    }

    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        let rpc = convert::vote_request_from_pb(req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(convert::vote_response_to_pb(&resp)))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<pb::InstallSnapshotRequest>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let mut stream = request.into_inner();

        // The first frame must be the header (ADR 0018 wire order).
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("install_snapshot stream was empty"))?;
        let header = match first.chunk {
            Some(pb::install_snapshot_request::Chunk::Header(h)) => h,
            _ => {
                return Err(Status::invalid_argument(
                    "first install_snapshot frame must be the header (ADR 0018)",
                ))
            }
        };
        self.check_cluster(&header.cluster_uuid)?;
        let vote_pb = header
            .vote
            .ok_or_else(|| Status::invalid_argument("install_snapshot header missing vote"))?;
        let vote = raftpb::vote_from_pb(std::path::Path::new("raft-rpc"), &vote_pb)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ident = header
            .meta
            .ok_or_else(|| Status::invalid_argument("install_snapshot header missing meta"))?;
        let meta = convert::snapshot_meta_from_pb(ident)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Spool the `data` frames straight to disk: the `SnapshotData`
        // binding is the file-backed `SnapshotFile` (the engine's receive
        // spool), so one wire chunk is the only buffered snapshot data,
        // however large the container (ADR 0018). Each append is seam IO,
        // run on the blocking pool.
        let mut data: Box<SnapshotFile> = self
            .raft
            .begin_receiving_snapshot()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        while let Some(frame) = stream.message().await? {
            match frame.chunk {
                Some(pb::install_snapshot_request::Chunk::Data(bytes)) => {
                    data = tokio::task::spawn_blocking(
                        move || -> io::Result<Box<SnapshotFile>> {
                            data.append(&bytes)?;
                            Ok(data)
                        },
                    )
                    .await
                    .map_err(|e| Status::internal(format!("snapshot spool task panicked: {e}")))?
                    .map_err(|e| {
                        Status::internal(format!("cannot spool snapshot chunk: {e}"))
                    })?;
                }
                Some(pb::install_snapshot_request::Chunk::Header(_)) => {
                    return Err(Status::invalid_argument(
                        "unexpected second header frame in install_snapshot stream",
                    ))
                }
                None => {
                    return Err(Status::invalid_argument(
                        "install_snapshot frame carried neither header nor data",
                    ))
                }
            }
        }

        let snapshot = Snapshot {
            meta,
            snapshot: data,
        };
        let resp = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::InstallSnapshotResponse {
            vote: Some(raftpb::vote_to_pb(&resp.vote)),
        }))
    }
}
