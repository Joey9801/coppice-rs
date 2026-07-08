//! Container framing shared by every durable file (ADR 0015, ADR 0018).
//!
//! Two layers, versioned independently:
//!
//! - The **container**: a fixed 16-byte header (8-byte file-type magic,
//!   `u32` container version, `u32` header CRC), plus the length-prefixed,
//!   CRC32C-checked record framing that carries protobuf payloads. One header
//!   layout, four magics (segment, vote, manifest, snapshot). This module owns
//!   that layer.
//! - The **payload**: the protobuf messages inside the frames, which evolve by
//!   ADR 0003's rules and never appear here.
//!
//! A reader that encounters a container version it does not support refuses to
//! start (ADR 0015: "there is no best-effort parse of an unknown container").
//! Bad header CRCs, wrong magics, and short headers are the same class of
//! fail-stop: the disk is not something this binary can interpret, and the
//! error names the file so an operator can act.
//!
//! Log-segment records additionally carry the entry's `LogId` (index, term,
//! leader node) in the frame itself, so recovery scans, suffix truncation, and
//! `get_log_state` never protobuf-decode an entry payload — the container
//! layer stays payload-agnostic (ADR 0018: apply, not parsing, is the
//! limiter), and the crash suite can drive the engine with opaque payloads.

use std::io;
use std::path::Path;

/// Current container format version, shared by all four file kinds. Bumps are
/// ClusterVersion-gated per ADR 0015 and documented by a new ADR.
pub const CONTAINER_VERSION: u32 = 1;

/// Size of the fixed container header: magic (8) + version (4) + CRC (4).
pub const HEADER_LEN: usize = 16;

/// File-type magic for log segments (`log/<start-index>.seg`).
pub const SEGMENT_MAGIC: [u8; 8] = *b"CPC_SEG\0";
/// File-type magic for the vote file.
pub const VOTE_MAGIC: [u8; 8] = *b"CPC_VOTE";
/// File-type magic for the manifest.
pub const MANIFEST_MAGIC: [u8; 8] = *b"CPC_MANI";
/// File-type magic for snapshot files (`snap/<snapshot-id>.snap`).
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"CPC_SNAP";
/// Closing magic ending a snapshot's footer; written last, so a valid closing
/// magic proves the file was written to completion (ADR 0018).
pub const SNAPSHOT_FOOTER_MAGIC: [u8; 8] = *b"CPC_SNPE";

/// Byte overhead of one plain record frame: length (4) + CRC32C (4).
pub const RECORD_OVERHEAD: usize = 8;

/// Byte overhead of one log-entry frame: length (4) + index (8) + term (8) +
/// leader node id (8) + CRC32C (4).
pub const ENTRY_OVERHEAD: usize = 32;

/// The fail-stop error for durable-state damage recovery must not repair:
/// names the file (and offset, where meaningful) and tells the operator the
/// remedy is `coordinator replace` (ADR 0017).
pub fn fail_stop(path: &Path, offset: u64, what: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!(
        "{} at offset {offset}: {what}; possibly-committed state is damaged and is never \
         repaired by guesswork — rebuild this replica with `coordinator replace` (ADR 0017)",
        path.display()
    ))
}

/// A fail-stop error for problems that are not byte-level corruption (missing
/// files, identity mismatches); same remedy, no offset.
pub fn fail_stop_file(path: &Path, what: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!(
        "{}: {what}; rebuild this replica with `coordinator replace` (ADR 0017)",
        path.display()
    ))
}

/// Add fail-stop context to a seam read error only when the error's kind
/// proves a data-shape problem (the file is missing or shorter than claimed).
/// Every other error — I/O trouble, and in the crash suite the injected
/// `SimCrashed` marker — passes through untouched, because wrapping would
/// destroy the payload the harness (and any caller inspecting sources)
/// relies on.
pub fn fail_stop_on_shape(
    path: &Path,
    offset: u64,
    what: impl std::fmt::Display,
    err: io::Error,
) -> io::Error {
    match err.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof => {
            fail_stop(path, offset, format!("{what}: {err}"))
        }
        _ => err,
    }
}

/// Encode the 16-byte container header for `magic`.
pub fn header(magic: [u8; 8]) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..8].copy_from_slice(&magic);
    out[8..12].copy_from_slice(&CONTAINER_VERSION.to_le_bytes());
    let crc = crc32c::crc32c(&out[..12]);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Validate a container header read from `path`: magic, supported version,
/// header CRC. Unknown-or-above-range versions fail stop naming the readable
/// range (ADR 0015).
pub fn check_header(path: &Path, bytes: &[u8], magic: [u8; 8]) -> io::Result<()> {
    if bytes.len() < HEADER_LEN {
        return Err(fail_stop(path, 0, "container header truncated"));
    }
    if bytes[..8] != magic {
        return Err(fail_stop(
            path,
            0,
            format!(
                "wrong file-type magic {:02x?} (expected {:02x?})",
                &bytes[..8],
                magic
            ),
        ));
    }
    let crc_stored = u32::from_le_bytes(bytes[12..16].try_into().expect("4 bytes"));
    if crc32c::crc32c(&bytes[..12]) != crc_stored {
        return Err(fail_stop(path, 0, "container header CRC mismatch"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
    if version != CONTAINER_VERSION {
        return Err(io::Error::other(format!(
            "{}: container version {version} is outside this binary's readable range \
             (1..={CONTAINER_VERSION}); run a binary that supports it (ADR 0015)",
            path.display()
        )));
    }
    Ok(())
}

/// Append one plain record frame — `[len u32][crc32c u32][payload]` — to `out`.
/// Used by the vote file, the manifest, and snapshot meta/index records.
pub fn frame_record(payload: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(payload.len()).expect("record payload under 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Read the plain record frame starting at `offset` in `bytes`, returning the
/// payload slice and the offset just past the frame.
pub fn read_record<'a>(
    path: &Path,
    bytes: &'a [u8],
    offset: usize,
) -> io::Result<(&'a [u8], usize)> {
    let hdr_end = offset
        .checked_add(RECORD_OVERHEAD)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| fail_stop(path, offset as u64, "record frame truncated"))?;
    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4 bytes")) as usize;
    let crc_stored = u32::from_le_bytes(bytes[offset + 4..hdr_end].try_into().expect("4 bytes"));
    let end = hdr_end
        .checked_add(len)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| fail_stop(path, offset as u64, "record payload truncated"))?;
    let payload = &bytes[hdr_end..end];
    if crc32c::crc32c(payload) != crc_stored {
        return Err(fail_stop(path, offset as u64, "record CRC32C mismatch"));
    }
    Ok((payload, end))
}

/// The log id carried in every log-entry frame; mirrors
/// `coppice.raft.v1.LogId` without requiring a protobuf decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLogId {
    pub index: u64,
    pub term: u64,
    pub node_id: u64,
}

/// Append one log-entry frame —
/// `[len u32][index u64][term u64][node u64][crc32c u32][payload]` — to `out`.
/// The CRC covers index, term, node, and payload.
pub fn frame_entry(id: FrameLogId, payload: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(payload.len()).expect("entry payload under 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    let mut crc = crc32c::crc32c(&id.index.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &id.term.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &id.node_id.to_le_bytes());
    crc = crc32c::crc32c_append(crc, payload);
    out.extend_from_slice(&id.index.to_le_bytes());
    out.extend_from_slice(&id.term.to_le_bytes());
    out.extend_from_slice(&id.node_id.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// One parse step over a segment's entry frames.
#[derive(Debug)]
pub enum FrameStep<'a> {
    /// A well-formed frame: its log id, payload, and the offset past it.
    Entry {
        id: FrameLogId,
        payload: &'a [u8],
        next: usize,
    },
    /// Clean end of input at a frame boundary.
    End,
    /// The bytes at this offset are not a complete, CRC-valid frame. Whether
    /// this is a self-healable torn tail or fail-stop corruption is the
    /// caller's decision (ADR 0017 recovery step 4).
    Torn,
}

/// Parse the entry frame at `offset`, if any.
pub fn parse_entry(bytes: &[u8], offset: usize) -> FrameStep<'_> {
    if offset == bytes.len() {
        return FrameStep::End;
    }
    let Some(hdr_end) = offset
        .checked_add(ENTRY_OVERHEAD)
        .filter(|&e| e <= bytes.len())
    else {
        return FrameStep::Torn;
    };
    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4 bytes")) as usize;
    let Some(end) = hdr_end.checked_add(len).filter(|&e| e <= bytes.len()) else {
        return FrameStep::Torn;
    };
    let field = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
    let id = FrameLogId {
        index: field(offset + 4),
        term: field(offset + 12),
        node_id: field(offset + 20),
    };
    let crc_stored = u32::from_le_bytes(bytes[offset + 28..hdr_end].try_into().expect("4 bytes"));
    let payload = &bytes[hdr_end..end];
    let mut crc = crc32c::crc32c(&bytes[offset + 4..offset + 28]);
    crc = crc32c::crc32c_append(crc, payload);
    if crc != crc_stored {
        return FrameStep::Torn;
    }
    FrameStep::Entry {
        id,
        payload,
        next: end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_failures() {
        let path = Path::new("manifest");
        let h = header(MANIFEST_MAGIC);
        check_header(path, &h, MANIFEST_MAGIC).unwrap();

        // Wrong magic.
        assert!(check_header(path, &h, VOTE_MAGIC).is_err());
        // Truncated.
        assert!(check_header(path, &h[..12], MANIFEST_MAGIC).is_err());
        // Flipped version bit fails the header CRC before the version check.
        let mut bad = h;
        bad[8] ^= 1;
        assert!(check_header(path, &bad, MANIFEST_MAGIC).is_err());
        // Unknown version with a recomputed CRC is refused by the range check.
        let mut future = [0u8; HEADER_LEN];
        future[..8].copy_from_slice(&MANIFEST_MAGIC);
        future[8..12].copy_from_slice(&99u32.to_le_bytes());
        let crc = crc32c::crc32c(&future[..12]);
        future[12..16].copy_from_slice(&crc.to_le_bytes());
        let err = check_header(path, &future, MANIFEST_MAGIC).unwrap_err();
        assert!(err.to_string().contains("readable range"), "{err}");
    }

    #[test]
    fn record_roundtrip_and_corruption() {
        let path = Path::new("vote");
        let mut buf = Vec::new();
        frame_record(b"hello", &mut buf);
        frame_record(b"", &mut buf);
        let (p1, next) = read_record(path, &buf, 0).unwrap();
        assert_eq!(p1, b"hello");
        let (p2, end) = read_record(path, &buf, next).unwrap();
        assert_eq!(p2, b"");
        assert_eq!(end, buf.len());

        let mut torn = buf.clone();
        torn.truncate(buf.len() - 1);
        // Second record's frame is fine; first still reads.
        read_record(path, &torn, 0).unwrap();
        let mut corrupt = buf.clone();
        corrupt[RECORD_OVERHEAD] ^= 0xFF;
        assert!(read_record(path, &corrupt, 0).is_err());
    }

    #[test]
    fn entry_frames_scan_and_detect_tears() {
        let id = |i: u64| FrameLogId {
            index: i,
            term: 3,
            node_id: 7,
        };
        let mut buf = Vec::new();
        frame_entry(id(5), b"aaaa", &mut buf);
        frame_entry(id(6), b"bb", &mut buf);

        let FrameStep::Entry {
            id: got,
            payload,
            next,
        } = parse_entry(&buf, 0)
        else {
            panic!("expected entry");
        };
        assert_eq!((got, payload), (id(5), &b"aaaa"[..]));
        let FrameStep::Entry {
            id: got,
            payload,
            next,
        } = parse_entry(&buf, next)
        else {
            panic!("expected entry");
        };
        assert_eq!((got, payload), (id(6), &b"bb"[..]));
        assert!(matches!(parse_entry(&buf, next), FrameStep::End));

        // A truncated tail is Torn, not an error.
        assert!(matches!(
            parse_entry(&buf[..buf.len() - 1], ENTRY_OVERHEAD + 4),
            FrameStep::Torn
        ));
        // A payload bitflip is Torn too; policy is the caller's.
        let mut corrupt = buf.clone();
        corrupt[ENTRY_OVERHEAD] ^= 0x01;
        assert!(matches!(parse_entry(&corrupt, 0), FrameStep::Torn));
    }
}
