//! pb↔openraft conversions for the Raft RPC surface (ADR 0002/0018).
//!
//! Every Raft value that crosses the wire is converted here through the exact
//! same [`storage::raftpb`](crate::storage::raftpb) converters the durable
//! formats use — ADR 0018's "our own representations, never openraft serde"
//! holds identically on disk and on the wire. Domain→pb is total; pb→domain is
//! fallible, and a decode failure is an [`io::Error`] the caller maps to a
//! transport error: [`tonic::Status::invalid_argument`] on the server side, a
//! [`NetworkError`](openraft::error::NetworkError) on the client side.

use std::io;
use std::path::Path;

use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::storage::SnapshotMeta;

use coppice_proto::pb::raft::v1 as pb;

use crate::adapter::TypeConfig;
use crate::membership::CoordinatorNode;
use crate::storage::raftpb;
use crate::CoordinatorId;

/// The path name fail-stop errors attribute wire-decode failures to.
const WIRE: &str = "raft-rpc";

fn missing(field: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("raft RPC message missing required field: {field}"),
    )
}

// ---- AppendEntries ---------------------------------------------------------

/// Convert an outgoing `AppendEntries`, stamping the cluster identity (ADR 0016).
pub fn append_entries_to_pb(
    rpc: &AppendEntriesRequest<TypeConfig>,
    cluster_uuid: [u8; 16],
) -> pb::AppendEntriesRequest {
    pb::AppendEntriesRequest {
        cluster_uuid: cluster_uuid.to_vec(),
        vote: Some(raftpb::vote_to_pb(&rpc.vote)),
        prev_log_id: rpc.prev_log_id.as_ref().map(raftpb::log_id_to_pb),
        leader_commit: rpc.leader_commit.as_ref().map(raftpb::log_id_to_pb),
        entries: rpc.entries.iter().map(raftpb::entry_to_pb).collect(),
    }
}

/// Convert a received `AppendEntries` request (server side).
pub fn append_entries_from_pb(
    req: pb::AppendEntriesRequest,
) -> io::Result<AppendEntriesRequest<TypeConfig>> {
    let path = Path::new(WIRE);
    let vote = raftpb::vote_from_pb(path, req.vote.as_ref().ok_or_else(|| missing("vote"))?)?;
    let prev_log_id = req
        .prev_log_id
        .as_ref()
        .map(|id| raftpb::log_id_from_pb(path, id))
        .transpose()?;
    let leader_commit = req
        .leader_commit
        .as_ref()
        .map(|id| raftpb::log_id_from_pb(path, id))
        .transpose()?;
    let entries = req
        .entries
        .into_iter()
        .map(|entry| {
            // Each entry carries its own log id; use its claimed index as the
            // contiguity assertion `entry_from_pb` enforces.
            let index = entry.log_id.as_ref().map(|id| id.index).unwrap_or(0);
            raftpb::entry_from_pb(path, index, entry)
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(AppendEntriesRequest {
        vote,
        prev_log_id,
        leader_commit,
        entries,
    })
}

/// Convert an `AppendEntries` response (server side).
pub fn append_response_to_pb(
    resp: AppendEntriesResponse<CoordinatorId>,
) -> pb::AppendEntriesResponse {
    use pb::append_entries_response::Result as R;
    let result = match resp {
        AppendEntriesResponse::Success => R::Success(pb::AppendSuccess {}),
        AppendEntriesResponse::PartialSuccess(upto) => {
            R::PartialSuccess(pb::AppendPartialSuccess {
                upto: upto.as_ref().map(raftpb::log_id_to_pb),
            })
        }
        AppendEntriesResponse::Conflict => R::Conflict(pb::AppendConflict {}),
        AppendEntriesResponse::HigherVote(vote) => R::HigherVote(raftpb::vote_to_pb(&vote)),
    };
    pb::AppendEntriesResponse {
        result: Some(result),
    }
}

/// Convert a received `AppendEntries` response (client side).
pub fn append_response_from_pb(
    resp: pb::AppendEntriesResponse,
) -> io::Result<AppendEntriesResponse<CoordinatorId>> {
    use pb::append_entries_response::Result as R;
    let path = Path::new(WIRE);
    match resp.result {
        Some(R::Success(_)) => Ok(AppendEntriesResponse::Success),
        Some(R::PartialSuccess(p)) => Ok(AppendEntriesResponse::PartialSuccess(
            p.upto
                .as_ref()
                .map(|id| raftpb::log_id_from_pb(path, id))
                .transpose()?,
        )),
        Some(R::Conflict(_)) => Ok(AppendEntriesResponse::Conflict),
        Some(R::HigherVote(v)) => Ok(AppendEntriesResponse::HigherVote(raftpb::vote_from_pb(
            path, &v,
        )?)),
        None => Err(missing("AppendEntriesResponse.result")),
    }
}

// ---- Vote ------------------------------------------------------------------

/// Convert an outgoing `Vote` request, stamping the cluster identity (ADR 0016).
pub fn vote_request_to_pb(
    rpc: &VoteRequest<CoordinatorId>,
    cluster_uuid: [u8; 16],
) -> pb::VoteRequest {
    pb::VoteRequest {
        cluster_uuid: cluster_uuid.to_vec(),
        vote: Some(raftpb::vote_to_pb(&rpc.vote)),
        last_log_id: rpc.last_log_id.as_ref().map(raftpb::log_id_to_pb),
    }
}

/// Convert a received `Vote` request (server side).
pub fn vote_request_from_pb(req: pb::VoteRequest) -> io::Result<VoteRequest<CoordinatorId>> {
    let path = Path::new(WIRE);
    Ok(VoteRequest {
        vote: raftpb::vote_from_pb(path, req.vote.as_ref().ok_or_else(|| missing("vote"))?)?,
        last_log_id: req
            .last_log_id
            .as_ref()
            .map(|id| raftpb::log_id_from_pb(path, id))
            .transpose()?,
    })
}

/// Convert a `Vote` response (server side).
pub fn vote_response_to_pb(resp: &VoteResponse<CoordinatorId>) -> pb::VoteResponse {
    pb::VoteResponse {
        vote: Some(raftpb::vote_to_pb(&resp.vote)),
        vote_granted: resp.vote_granted,
        last_log_id: resp.last_log_id.as_ref().map(raftpb::log_id_to_pb),
    }
}

/// Convert a received `Vote` response (client side).
pub fn vote_response_from_pb(resp: pb::VoteResponse) -> io::Result<VoteResponse<CoordinatorId>> {
    let path = Path::new(WIRE);
    Ok(VoteResponse {
        vote: raftpb::vote_from_pb(path, resp.vote.as_ref().ok_or_else(|| missing("vote"))?)?,
        vote_granted: resp.vote_granted,
        last_log_id: resp
            .last_log_id
            .as_ref()
            .map(|id| raftpb::log_id_from_pb(path, id))
            .transpose()?,
    })
}

// ---- Snapshot meta ---------------------------------------------------------

/// Convert snapshot metadata into the `InstallSnapshot` header's `SnapshotIdent`.
pub fn snapshot_ident_to_pb(
    meta: &SnapshotMeta<CoordinatorId, CoordinatorNode>,
) -> pb::SnapshotIdent {
    pb::SnapshotIdent {
        last_log_id: meta.last_log_id.as_ref().map(raftpb::log_id_to_pb),
        last_membership: Some(raftpb::stored_membership_to_pb(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
    }
}

/// Convert a received `SnapshotIdent` back into snapshot metadata.
pub fn snapshot_meta_from_pb(
    ident: pb::SnapshotIdent,
) -> io::Result<SnapshotMeta<CoordinatorId, CoordinatorNode>> {
    let path = Path::new(WIRE);
    let last_log_id = ident
        .last_log_id
        .as_ref()
        .map(|id| raftpb::log_id_from_pb(path, id))
        .transpose()?;
    let last_membership = ident
        .last_membership
        .as_ref()
        .map(|m| raftpb::stored_membership_from_pb(path, m))
        .transpose()?
        .unwrap_or_default();
    Ok(SnapshotMeta {
        last_log_id,
        last_membership,
        snapshot_id: ident.snapshot_id,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership, Vote};

    use super::*;

    fn log_id(term: u64, node: u64, index: u64) -> LogId<CoordinatorId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, node),
            index,
        }
    }

    fn sample_entries() -> Vec<openraft::Entry<TypeConfig>> {
        vec![
            openraft::Entry {
                log_id: log_id(3, 1, 7),
                payload: openraft::EntryPayload::Blank,
            },
            openraft::Entry {
                log_id: log_id(3, 1, 8),
                payload: openraft::EntryPayload::Membership(Membership::new(
                    vec![BTreeSet::from([1, 2])],
                    None,
                )),
            },
        ]
    }

    #[test]
    fn append_entries_request_roundtrips() {
        let rpc = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(4, 1),
            prev_log_id: Some(log_id(3, 1, 6)),
            leader_commit: Some(log_id(3, 1, 6)),
            entries: sample_entries(),
        };
        let cluster = [7u8; 16];
        let pb = append_entries_to_pb(&rpc, cluster);
        assert_eq!(pb.cluster_uuid, cluster.to_vec());
        let back = append_entries_from_pb(pb).unwrap();
        assert_eq!(back.vote, rpc.vote);
        assert_eq!(back.prev_log_id, rpc.prev_log_id);
        assert_eq!(back.leader_commit, rpc.leader_commit);
        assert_eq!(back.entries.len(), rpc.entries.len());
        for (a, b) in back.entries.iter().zip(rpc.entries.iter()) {
            assert_eq!(a.log_id, b.log_id);
        }
    }

    #[test]
    fn append_entries_response_roundtrips_every_arm() {
        let cases = [
            AppendEntriesResponse::<CoordinatorId>::Success,
            AppendEntriesResponse::PartialSuccess(Some(log_id(3, 1, 8))),
            AppendEntriesResponse::PartialSuccess(None),
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(Vote::new_committed(9, 2)),
        ];
        for resp in cases {
            let pb = append_response_to_pb(resp.clone_for_test());
            let back = append_response_from_pb(pb).unwrap();
            assert_eq!(back, resp);
        }
    }

    // `AppendEntriesResponse` is not `Clone`; a tiny local helper keeps the
    // round-trip test readable without widening the openraft type's API.
    impl AppendEntriesResponseTestExt for AppendEntriesResponse<CoordinatorId> {
        fn clone_for_test(&self) -> Self {
            match self {
                AppendEntriesResponse::Success => AppendEntriesResponse::Success,
                AppendEntriesResponse::PartialSuccess(m) => {
                    AppendEntriesResponse::PartialSuccess(*m)
                }
                AppendEntriesResponse::Conflict => AppendEntriesResponse::Conflict,
                AppendEntriesResponse::HigherVote(v) => AppendEntriesResponse::HigherVote(*v),
            }
        }
    }
    trait AppendEntriesResponseTestExt {
        fn clone_for_test(&self) -> Self;
    }

    #[test]
    fn vote_request_and_response_roundtrip() {
        let req = VoteRequest::<CoordinatorId>::new(Vote::new(5, 1), Some(log_id(4, 1, 3)));
        let back = vote_request_from_pb(vote_request_to_pb(&req, [1u8; 16])).unwrap();
        assert_eq!(back.vote, req.vote);
        assert_eq!(back.last_log_id, req.last_log_id);

        let resp = VoteResponse::<CoordinatorId>::new(Vote::new(6, 2), Some(log_id(4, 1, 3)), true);
        let back = vote_response_from_pb(vote_response_to_pb(&resp)).unwrap();
        assert_eq!(back.vote, resp.vote);
        assert_eq!(back.vote_granted, resp.vote_granted);
        assert_eq!(back.last_log_id, resp.last_log_id);
    }

    #[test]
    fn snapshot_meta_roundtrips() {
        let meta = SnapshotMeta::<CoordinatorId, CoordinatorNode> {
            last_log_id: Some(log_id(3, 1, 42)),
            last_membership: StoredMembership::new(
                Some(log_id(3, 1, 40)),
                Membership::new(
                    vec![BTreeSet::from([1, 2, 3])],
                    BTreeMap::from([(1, CoordinatorNode::new("a:1", "coord-1"))]),
                ),
            ),
            snapshot_id: "0000000000000005".to_string(),
        };
        let back = snapshot_meta_from_pb(snapshot_ident_to_pb(&meta)).unwrap();
        assert_eq!(back.last_log_id, meta.last_log_id);
        assert_eq!(back.snapshot_id, meta.snapshot_id);
        assert_eq!(back.last_membership, meta.last_membership);
    }

    #[test]
    fn missing_vote_is_a_decode_error() {
        let pb = pb::VoteRequest {
            cluster_uuid: vec![0u8; 16],
            vote: None,
            last_log_id: None,
        };
        assert!(vote_request_from_pb(pb).is_err());
    }
}
