//! The snapshot container: protobuf records inside a parallel-decodable
//! layout (ADR 0018).
//!
//! One file, three regions:
//!
//! ```text
//! [16B container header]                         magic CPC_SNAP, version, CRC
//! [meta record]                                  SnapshotMeta, len+CRC framed
//! [section bytes ...]                            per (kind, shard), contiguous
//! [index record]                                 SectionIndex, len+CRC framed
//! [index record length  u32 LE]
//! [total CRC32C         u32 LE]                  covers header..index start
//! [8B closing magic]                             CPC_SNPE, written last
//! ```
//!
//! Sections are independent streams of length-delimited protobuf records —
//! one per (entity type, hash shard) — each with its own CRC32C, record
//! count, encoding id, and compression, so N shards decode on N cores while
//! the serial rebuild consumes records through a channel. The footer is
//! written last: a truncated snapshot can never carry a valid closing magic,
//! so "has a footer" is the adoption test (ADR 0018), and the manifest only
//! ever points at fully renamed files anyway (ADR 0017).
//!
//! Compression: the enum is plumbed end to end, but this writer only emits
//! `COMPRESSION_NONE`; `COMPRESSION_ZSTD` decodes to a fail-stop
//! "unsupported" error until a ClusterVersion gate enables it (ADR 0015).

use std::io;
use std::path::Path;

use prost::Message;

use coppice_proto::convert::StateRecords;
use coppice_proto::pb::storage::v1 as pb;

use super::container::{
    check_header, fail_stop, frame_record, header, read_record, HEADER_LEN, SNAPSHOT_FOOTER_MAGIC,
    SNAPSHOT_MAGIC,
};

/// The only record encoding this writer produces (ADR 0018's escape hatch:
/// a hot section may move to a denser encoding behind a ClusterVersion gate).
pub const ENCODING_PROTOBUF_LD: &str = "protobuf-ld";

/// Fixed trailer past the section-index record: its length, the total CRC,
/// and the closing magic.
const TRAILER_LEN: usize = 4 + 4 + 8;

/// One section's bytes plus its index entry, ready for assembly. Exposed so
/// the crash suite can drive the container layer with opaque payloads.
pub struct RawSection {
    pub kind: pb::SectionKind,
    pub shard: u32,
    pub encoding: String,
    pub record_count: u64,
    pub bytes: Vec<u8>,
}

/// Assemble a complete container from already-encoded sections.
pub fn assemble_container(meta: &pb::SnapshotMeta, sections: Vec<RawSection>) -> Vec<u8> {
    let mut out = header(SNAPSHOT_MAGIC).to_vec();
    frame_record(&meta.encode_to_vec(), &mut out);

    let mut index = pb::SectionIndex::default();
    for section in &sections {
        index.sections.push(pb::SectionEntry {
            kind: section.kind as i32,
            shard: section.shard,
            offset: out.len() as u64,
            length: section.bytes.len() as u64,
            record_count: section.record_count,
            encoding: section.encoding.clone(),
            compression: pb::Compression::None as i32,
            crc32c: crc32c::crc32c(&section.bytes),
        });
        out.extend_from_slice(&section.bytes);
    }

    let index_start = out.len();
    frame_record(&index.encode_to_vec(), &mut out);
    let index_len = (out.len() - index_start) as u32;
    let total_crc = crc32c::crc32c(&out[..index_start]);
    out.extend_from_slice(&index_len.to_le_bytes());
    out.extend_from_slice(&total_crc.to_le_bytes());
    out.extend_from_slice(&SNAPSHOT_FOOTER_MAGIC);
    out
}

/// Validate a container end to end without decoding any records: header,
/// closing magic, total CRC, meta record, and every section's bounds and
/// CRC32C. This is the adoption gate for `install_snapshot` — every section
/// CRC is checked before anything durable points at the bytes (ADR 0016).
pub fn validate_container(
    path: &Path,
    bytes: &[u8],
) -> io::Result<(pb::SnapshotMeta, pb::SectionIndex)> {
    check_header(path, bytes, SNAPSHOT_MAGIC)?;
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(fail_stop(
            path,
            bytes.len() as u64,
            "snapshot truncated before its footer",
        ));
    }
    let trailer = &bytes[bytes.len() - TRAILER_LEN..];
    if trailer[8..16] != SNAPSHOT_FOOTER_MAGIC {
        return Err(fail_stop(
            path,
            (bytes.len() - 8) as u64,
            "snapshot has no closing magic (truncated write was never completed)",
        ));
    }
    let index_len = u32::from_le_bytes(trailer[..4].try_into().expect("4 bytes")) as usize;
    let total_crc = u32::from_le_bytes(trailer[4..8].try_into().expect("4 bytes"));
    let index_start = bytes
        .len()
        .checked_sub(TRAILER_LEN + index_len)
        .filter(|&s| s >= HEADER_LEN)
        .ok_or_else(|| fail_stop(path, 0, "snapshot section index length is out of bounds"))?;
    if crc32c::crc32c(&bytes[..index_start]) != total_crc {
        return Err(fail_stop(path, 0, "snapshot total CRC32C mismatch"));
    }
    let (index_payload, _) = read_record(path, bytes, index_start)?;
    let index = pb::SectionIndex::decode(index_payload).map_err(|e| {
        fail_stop(
            path,
            index_start as u64,
            format!("section index does not decode: {e}"),
        )
    })?;

    let (meta_payload, sections_start) = read_record(path, bytes, HEADER_LEN)?;
    let meta = pb::SnapshotMeta::decode(meta_payload).map_err(|e| {
        fail_stop(
            path,
            HEADER_LEN as u64,
            format!("snapshot meta does not decode: {e}"),
        )
    })?;

    for entry in &index.sections {
        let start = usize::try_from(entry.offset)
            .ok()
            .filter(|&s| s >= sections_start)
            .ok_or_else(|| fail_stop(path, entry.offset, "section offset out of bounds"))?;
        let end = entry
            .length
            .try_into()
            .ok()
            .and_then(|len: usize| start.checked_add(len))
            .filter(|&e| e <= index_start)
            .ok_or_else(|| fail_stop(path, entry.offset, "section length out of bounds"))?;
        if crc32c::crc32c(&bytes[start..end]) != entry.crc32c {
            return Err(fail_stop(
                path,
                entry.offset,
                format!(
                    "section (kind {}, shard {}) CRC32C mismatch",
                    entry.kind, entry.shard
                ),
            ));
        }
    }
    Ok((meta, index))
}

/// Borrow one validated section's bytes.
pub fn section_bytes<'a>(bytes: &'a [u8], entry: &pb::SectionEntry) -> &'a [u8] {
    &bytes[entry.offset as usize..(entry.offset + entry.length) as usize]
}

/// Encode a state's records into a full container, sharding each entity
/// section `shards` ways and encoding the shards on parallel threads
/// (ADR 0018: snapshot cost scales with cores). Shard assignment is a writer
/// choice readers never depend on; contiguous chunks keep it deterministic.
pub fn encode_state(meta: &pb::SnapshotMeta, records: &StateRecords, shards: u32) -> Vec<u8> {
    let shards = shards.max(1) as usize;
    let mut sections: Vec<RawSection> = Vec::new();

    fn shard_section<M: Message>(
        kind: pb::SectionKind,
        records: &[M],
        shards: usize,
        out: &mut Vec<RawSection>,
    ) {
        let per = records.len().div_ceil(shards).max(1);
        let chunks: Vec<&[M]> = records.chunks(per).collect();
        let mut encoded: Vec<(usize, Vec<u8>, u64)> = Vec::new();
        std::thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .iter()
                .enumerate()
                .map(|(shard, chunk)| {
                    scope.spawn(move || {
                        let mut buf = Vec::new();
                        for record in *chunk {
                            record
                                .encode_length_delimited(&mut buf)
                                .expect("Vec<u8> writes are infallible");
                        }
                        (shard, buf, chunk.len() as u64)
                    })
                })
                .collect();
            for handle in handles {
                encoded.push(handle.join().expect("encode shard panicked"));
            }
        });
        encoded.sort_by_key(|(shard, ..)| *shard);
        for (shard, bytes, record_count) in encoded {
            out.push(RawSection {
                kind,
                shard: shard as u32,
                encoding: ENCODING_PROTOBUF_LD.to_string(),
                record_count,
                bytes,
            });
        }
    }

    shard_section(pb::SectionKind::Job, &records.jobs, shards, &mut sections);
    shard_section(
        pb::SectionKind::Attempt,
        &records.attempts,
        shards,
        &mut sections,
    );
    shard_section(
        pb::SectionKind::Allocation,
        &records.allocations,
        shards,
        &mut sections,
    );
    shard_section(pb::SectionKind::Node, &records.nodes, shards, &mut sections);
    shard_section(
        pb::SectionKind::QuotaEntity,
        &records.quota_entities,
        shards,
        &mut sections,
    );
    if let Some(cluster) = &records.cluster {
        let mut bytes = Vec::new();
        cluster
            .encode_length_delimited(&mut bytes)
            .expect("Vec<u8> writes are infallible");
        sections.push(RawSection {
            kind: pb::SectionKind::ClusterState,
            shard: 0,
            encoding: ENCODING_PROTOBUF_LD.to_string(),
            record_count: 1,
            bytes,
        });
    }
    assemble_container(meta, sections)
}

/// Decode a validated container back into records, sections in parallel
/// (the learner-rebuild path, ADR 0016/0018). Rebuild is order-independent
/// across sections; shards are merged in (kind, shard) order only for
/// determinism of the intermediate `StateRecords`.
pub fn decode_state(path: &Path, bytes: &[u8]) -> io::Result<(pb::SnapshotMeta, StateRecords)> {
    let (meta, index) = validate_container(path, bytes)?;

    fn decode_section<M: Message + Default>(
        path: &Path,
        entry: &pb::SectionEntry,
        bytes: &[u8],
    ) -> io::Result<Vec<M>> {
        if entry.encoding != ENCODING_PROTOBUF_LD {
            return Err(fail_stop(
                path,
                entry.offset,
                format!("unknown section encoding {:?}", entry.encoding),
            ));
        }
        match entry.compression() {
            pb::Compression::None => {}
            pb::Compression::Zstd => {
                return Err(fail_stop(
                    path,
                    entry.offset,
                    "zstd-compressed sections are not yet enabled in this binary (ADR 0018)",
                ));
            }
            pb::Compression::Unspecified => {
                return Err(fail_stop(
                    path,
                    entry.offset,
                    "section compression is unspecified",
                ));
            }
        }
        let mut cursor = section_bytes(bytes, entry);
        let mut out = Vec::new();
        while !cursor.is_empty() {
            let record = M::decode_length_delimited(&mut cursor).map_err(|e| {
                fail_stop(
                    path,
                    entry.offset,
                    format!("section record does not decode: {e}"),
                )
            })?;
            out.push(record);
        }
        if out.len() as u64 != entry.record_count {
            return Err(fail_stop(
                path,
                entry.offset,
                format!(
                    "section holds {} records, index claims {}",
                    out.len(),
                    entry.record_count
                ),
            ));
        }
        Ok(out)
    }

    let mut entries: Vec<&pb::SectionEntry> = index.sections.iter().collect();
    entries.sort_by_key(|e| (e.kind, e.shard));

    let mut records = StateRecords::default();
    let results: Vec<io::Result<Decoded>> = std::thread::scope(|scope| {
        let handles: Vec<_> = entries
            .iter()
            .map(|entry| {
                scope.spawn(move || -> io::Result<Decoded> {
                    Ok(match entry.kind() {
                        pb::SectionKind::Job => Decoded::Jobs(decode_section(path, entry, bytes)?),
                        pb::SectionKind::Attempt => {
                            Decoded::Attempts(decode_section(path, entry, bytes)?)
                        }
                        pb::SectionKind::Allocation => {
                            Decoded::Allocations(decode_section(path, entry, bytes)?)
                        }
                        pb::SectionKind::Node => {
                            Decoded::Nodes(decode_section(path, entry, bytes)?)
                        }
                        pb::SectionKind::QuotaEntity => {
                            Decoded::QuotaEntities(decode_section(path, entry, bytes)?)
                        }
                        pb::SectionKind::ClusterState => {
                            Decoded::Cluster(decode_section(path, entry, bytes)?)
                        }
                        pb::SectionKind::Unspecified => {
                            Err(fail_stop(path, entry.offset, "section kind is unspecified"))?
                        }
                    })
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("decode section panicked"))
            .collect()
    });

    for result in results {
        match result? {
            Decoded::Jobs(mut v) => records.jobs.append(&mut v),
            Decoded::Attempts(mut v) => records.attempts.append(&mut v),
            Decoded::Allocations(mut v) => records.allocations.append(&mut v),
            Decoded::Nodes(mut v) => records.nodes.append(&mut v),
            Decoded::QuotaEntities(mut v) => records.quota_entities.append(&mut v),
            Decoded::Cluster(v) => {
                if records.cluster.is_some() || v.len() != 1 {
                    return Err(fail_stop(
                        path,
                        0,
                        "snapshot must carry exactly one ClusterStateRecord",
                    ));
                }
                records.cluster = v.into_iter().next();
            }
        }
    }
    Ok((meta, records))
}

enum Decoded {
    Jobs(Vec<pb::JobRecord>),
    Attempts(Vec<pb::AttemptRecord>),
    Allocations(Vec<pb::AllocationRecord>),
    Nodes(Vec<pb::NodeRecord>),
    QuotaEntities(Vec<pb::QuotaEntityRecord>),
    Cluster(Vec<pb::ClusterStateRecord>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> pb::SnapshotMeta {
        pb::SnapshotMeta {
            cluster_uuid: vec![7u8; 16],
            snapshot_id: "000000000000002a".into(),
            last_applied: None,
            membership: None,
            cluster_version: 1,
            shard_count: 2,
        }
    }

    #[test]
    fn empty_state_roundtrips() {
        let records = StateRecords {
            cluster: Some(pb::ClusterStateRecord::default()),
            ..StateRecords::default()
        };
        let bytes = encode_state(&meta(), &records, 2);
        let (got_meta, got) = decode_state(Path::new("t"), &bytes).unwrap();
        assert_eq!(got_meta, meta());
        assert!(got.jobs.is_empty());
        assert_eq!(got.cluster, records.cluster);
    }

    #[test]
    fn truncation_and_bitflips_are_refused() {
        let records = StateRecords {
            cluster: Some(pb::ClusterStateRecord::default()),
            ..StateRecords::default()
        };
        let bytes = encode_state(&meta(), &records, 1);

        // Any truncation loses the closing magic.
        for cut in [1usize, 8, TRAILER_LEN, bytes.len() / 2] {
            let torn = &bytes[..bytes.len() - cut];
            assert!(
                validate_container(Path::new("t"), torn).is_err(),
                "cut {cut}"
            );
        }
        // A bitflip anywhere fails a CRC (header, meta, section, index, or
        // total).
        for at in [
            0usize,
            HEADER_LEN + 2,
            bytes.len() - TRAILER_LEN - 2,
            bytes.len() / 2,
        ] {
            let mut corrupt = bytes.clone();
            corrupt[at] ^= 0x40;
            assert!(
                validate_container(Path::new("t"), &corrupt).is_err(),
                "flip at {at}"
            );
        }
    }

    #[test]
    fn opaque_sections_pass_container_validation() {
        // The crash suite drives the container layer with opaque payloads;
        // validation must not require decodable records.
        let sections = vec![RawSection {
            kind: pb::SectionKind::Job,
            shard: 0,
            encoding: "opaque-test".into(),
            record_count: 0,
            bytes: vec![0xAB; 100],
        }];
        let bytes = assemble_container(&meta(), sections);
        let (_, index) = validate_container(Path::new("t"), &bytes).unwrap();
        assert_eq!(section_bytes(&bytes, &index.sections[0]), &[0xAB; 100][..]);
        // But record decode refuses the unknown encoding.
        assert!(decode_state(Path::new("t"), &bytes).is_err());
    }
}
