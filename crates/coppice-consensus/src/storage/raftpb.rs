//! Conversions between openraft's in-memory types and our protobuf shapes
//! (`coppice.raft.v1`, ADR 0018).
//!
//! ADR 0018 forbids persisting openraft's serde representations: what lands on
//! disk is our schema, converted here at the storage boundary, so an openraft
//! upgrade cannot silently change the durable format. Domain→pb is total;
//! pb→domain is fallible and every failure is a fail-stop `io::Error` — a log
//! entry or vote that does not decode is corruption of possibly-committed
//! state, never something to skip (ADR 0017).
//!
//! These same converters back **both** durable formats (the segment log and
//! snapshot meta here) **and** the wire transport (the [`net`](crate::net)
//! module's gRPC Raft RPCs). That sharing is deliberate: ADR 0018 mandates one
//! set of *our own* representations for a Raft value — never openraft's serde
//! forms — regardless of whether the value is going to disk or over the wire.
//! The pb-struct-level [`entry_to_pb`]/[`entry_from_pb`] exist so the net
//! module reuses the exact log-entry conversion the log store persists, rather
//! than growing a second, drift-prone copy.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use openraft::{BasicNode, CommittedLeaderId, LeaderId, LogId, Membership, StoredMembership, Vote};
use prost::Message;

use coppice_proto::convert::{command_from_pb, command_to_pb};
use coppice_proto::pb::raft::v1 as pb;

use crate::adapter::TypeConfig;
use crate::CoordinatorId;

use super::container::fail_stop;

/// The ClusterVersion stamped into persisted command envelopes.
///
/// Every command in the v1 catalog is cluster-version 1 (ADR 0003); once
/// proposers carry a live version, stamping moves to the proposal path and
/// this constant goes away.
const WRITTEN_CLUSTER_VERSION: u32 = 1;

pub fn vote_to_pb(vote: &Vote<CoordinatorId>) -> pb::Vote {
    pb::Vote {
        leader_id: Some(pb::LeaderId {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id,
        }),
        committed: vote.committed,
    }
}

pub fn vote_from_pb(path: &Path, vote: &pb::Vote) -> io::Result<Vote<CoordinatorId>> {
    let leader = vote
        .leader_id
        .as_ref()
        .ok_or_else(|| fail_stop(path, 0, "vote record missing leader_id"))?;
    Ok(Vote {
        leader_id: LeaderId::new(leader.term, leader.node_id),
        committed: vote.committed,
    })
}

pub fn log_id_to_pb(id: &LogId<CoordinatorId>) -> pb::LogId {
    pb::LogId {
        leader_id: Some(pb::LeaderId {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
        }),
        index: id.index,
    }
}

pub fn log_id_from_pb(path: &Path, id: &pb::LogId) -> io::Result<LogId<CoordinatorId>> {
    let leader = id
        .leader_id
        .as_ref()
        .ok_or_else(|| fail_stop(path, 0, "LogId missing leader_id"))?;
    Ok(LogId {
        leader_id: CommittedLeaderId::new(leader.term, leader.node_id),
        index: id.index,
    })
}

pub fn membership_to_pb(membership: &Membership<CoordinatorId, BasicNode>) -> pb::Membership {
    pb::Membership {
        configs: membership
            .get_joint_config()
            .iter()
            .map(|config| pb::VoterConfig {
                // BTreeSet iteration gives the canonical ascending order
                // schema-style.md requires of replicated repeated keys.
                voters: config.iter().copied().collect(),
            })
            .collect(),
        members: membership
            .nodes()
            .map(|(id, node)| pb::RaftMember {
                node_id: *id,
                address: node.addr.clone(),
            })
            .collect(),
    }
}

pub fn membership_from_pb(
    path: &Path,
    membership: &pb::Membership,
) -> io::Result<Membership<CoordinatorId, BasicNode>> {
    let configs: Vec<BTreeSet<CoordinatorId>> = membership
        .configs
        .iter()
        .map(|config| config.voters.iter().copied().collect())
        .collect();
    let mut nodes: BTreeMap<CoordinatorId, BasicNode> = BTreeMap::new();
    for member in &membership.members {
        let prev = nodes.insert(
            member.node_id,
            BasicNode {
                addr: member.address.clone(),
            },
        );
        if prev.is_some() {
            return Err(fail_stop(
                path,
                0,
                format!("duplicate membership entry for node {}", member.node_id),
            ));
        }
    }
    Ok(Membership::new(configs, nodes))
}

pub fn stored_membership_to_pb(
    stored: &StoredMembership<CoordinatorId, BasicNode>,
) -> pb::StoredMembership {
    pb::StoredMembership {
        log_id: stored.log_id().as_ref().map(log_id_to_pb),
        membership: Some(membership_to_pb(stored.membership())),
    }
}

pub fn stored_membership_from_pb(
    path: &Path,
    stored: &pb::StoredMembership,
) -> io::Result<StoredMembership<CoordinatorId, BasicNode>> {
    let log_id = stored
        .log_id
        .as_ref()
        .map(|id| log_id_from_pb(path, id))
        .transpose()?;
    let membership = stored
        .membership
        .as_ref()
        .ok_or_else(|| fail_stop(path, 0, "StoredMembership missing membership"))?;
    Ok(StoredMembership::new(
        log_id,
        membership_from_pb(path, membership)?,
    ))
}

/// Convert one openraft log entry into its `coppice.raft.v1.LogEntry` pb form.
///
/// Total (Domain→pb). Shared by the durable log store and the wire transport
/// (module docs): both encode an entry exactly this way.
pub fn entry_to_pb(entry: &openraft::Entry<TypeConfig>) -> pb::LogEntry {
    use openraft::EntryPayload;
    let payload = match &entry.payload {
        EntryPayload::Blank => pb::log_entry::Payload::Blank(pb::Blank {}),
        EntryPayload::Normal(command) => {
            pb::log_entry::Payload::Normal(command_to_pb(command, WRITTEN_CLUSTER_VERSION))
        }
        EntryPayload::Membership(membership) => {
            pb::log_entry::Payload::Membership(membership_to_pb(membership))
        }
    };
    pb::LogEntry {
        log_id: Some(log_id_to_pb(&entry.log_id)),
        payload: Some(payload),
    }
}

/// Convert a `coppice.raft.v1.LogEntry` pb back into an openraft entry.
///
/// Fallible (pb→Domain): `path` and `index` name the source for fail-stop
/// errors, and the entry's own `log_id.index` must equal `index` — on the
/// durable path `index` is the framing position; on the wire path it is the
/// entry's claimed index, so the check is the caller's contiguity assertion.
pub fn entry_from_pb(
    path: &Path,
    index: u64,
    entry: pb::LogEntry,
) -> io::Result<openraft::Entry<TypeConfig>> {
    use openraft::EntryPayload;
    let log_id = entry
        .log_id
        .as_ref()
        .ok_or_else(|| fail_stop(path, 0, format!("log entry {index} missing log_id")))?;
    let log_id = log_id_from_pb(path, log_id)?;
    if log_id.index != index {
        return Err(fail_stop(
            path,
            0,
            format!(
                "log entry claims index {} but is framed at {index}",
                log_id.index
            ),
        ));
    }
    let payload = match entry.payload {
        Some(pb::log_entry::Payload::Blank(_)) => EntryPayload::Blank,
        Some(pb::log_entry::Payload::Normal(command)) => {
            let (_version, command) = command_from_pb(command).map_err(|e| {
                fail_stop(
                    path,
                    0,
                    format!("log entry {index} command does not convert: {e}"),
                )
            })?;
            EntryPayload::Normal(command)
        }
        Some(pb::log_entry::Payload::Membership(membership)) => {
            EntryPayload::Membership(membership_from_pb(path, &membership)?)
        }
        None => {
            return Err(fail_stop(
                path,
                0,
                format!("log entry {index} has no payload"),
            ));
        }
    };
    Ok(openraft::Entry { log_id, payload })
}

/// Encode one log entry as the durable `coppice.raft.v1.LogEntry` payload.
///
/// The durable log store's entry point; delegates to [`entry_to_pb`] so the
/// wire and disk representations can never drift apart.
pub fn entry_to_bytes(entry: &openraft::Entry<TypeConfig>) -> Vec<u8> {
    entry_to_pb(entry).encode_to_vec()
}

/// Decode a durable log-entry payload back into an openraft entry.
///
/// `path` and `index` name the source for fail-stop errors; delegates the
/// pb→domain conversion to [`entry_from_pb`].
pub fn entry_from_bytes(
    path: &Path,
    index: u64,
    bytes: &[u8],
) -> io::Result<openraft::Entry<TypeConfig>> {
    let entry = pb::LogEntry::decode(bytes)
        .map_err(|e| fail_stop(path, 0, format!("log entry {index} does not decode: {e}")))?;
    entry_from_pb(path, index, entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_and_membership_roundtrip() {
        let path = Path::new("t");
        let vote = Vote::<CoordinatorId>::new_committed(7, 3);
        assert_eq!(vote_from_pb(path, &vote_to_pb(&vote)).unwrap(), vote);

        let membership = Membership::<CoordinatorId, BasicNode>::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([(4, BasicNode { addr: "b:1".into() })]),
        );
        assert_eq!(
            membership_from_pb(path, &membership_to_pb(&membership)).unwrap(),
            membership
        );

        // The suite's `Membership::new(configs, None)` shape (voters filled
        // with default nodes) must survive the round trip exactly.
        let bare = Membership::<CoordinatorId, BasicNode>::new(vec![BTreeSet::from([1, 2])], None);
        assert_eq!(
            membership_from_pb(path, &membership_to_pb(&bare)).unwrap(),
            bare
        );
    }

    #[test]
    fn entries_roundtrip() {
        let path = Path::new("t");
        let log_id = LogId {
            leader_id: CommittedLeaderId::new(2, 1),
            index: 9,
        };
        for payload in [
            openraft::EntryPayload::<TypeConfig>::Blank,
            openraft::EntryPayload::Membership(Membership::new(vec![BTreeSet::from([1])], None)),
        ] {
            let entry = openraft::Entry { log_id, payload };
            let bytes = entry_to_bytes(&entry);
            let back = entry_from_bytes(path, 9, &bytes).unwrap();
            assert_eq!(back, entry);
            // A framed index that disagrees with the payload is corruption.
            assert!(entry_from_bytes(path, 10, &bytes).is_err());
        }
    }
}
