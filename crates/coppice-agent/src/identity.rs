//! Self-minted, persistent agent identity (`docs/roadmap/deployment-story.md`
//! A1; mirrors the coordinator's machine identity, ADR 0037 §7).
//!
//! An agent's [`NodeId`] is *its own*, not a value a human types into
//! `agent.toml`: on first boot with fresh state the agent mints one and writes
//! it to `<data_dir>/node-identity`; every later boot reads it back. That
//! inverts the CN↔NodeId binding the way enrollment (A2) already wants it —
//! the identity exists first, and the certificate is issued *for* it.
//!
//! Two rules make the file safe to depend on:
//!
//! * **A file that exists is authoritative.** A `node-identity` that cannot be
//!   read or parsed is a hard error naming the path, never a cue to re-mint:
//!   silently minting over a corrupt file would hand the node a second identity
//!   while its journal, its leaf's CN, and the coordinator's view of it all
//!   still name the first.
//! * **A data directory with prior state but no identity file is a hard
//!   error.** That shape only arises when the identity file has gone missing
//!   from a directory the agent already ran in. Minting there would break
//!   journal fencing (the recovered `(leader_term, node_epoch)` watermark
//!   belongs to the old id) and the certificate CN binding, so the agent
//!   refuses and tells the operator to restore the id or start fresh.
//!
//! "Prior state" is probed as the presence of the agent's journal
//! ([`crate::journal::JOURNAL`], `<data_dir>/journal`). It is the right probe
//! because the journal is created — and rewritten atomically — by
//! [`crate::journal::Journal::open`] on *every* agent start, before any other
//! per-node file appears, so it is present in exactly the runs that already had
//! an identity. Sibling artifacts (`LOCK`, `telemetry/`, `image-cache.json`)
//! are either created for a fresh boot too or absent on a node that never ran a
//! job, so neither is a sound signal on its own.

use std::path::Path;

use anyhow::{bail, Context, Result};
use coppice_consensus::fs::{read_to_vec, write_atomic, Fs, RealFs};
use coppice_core::id::NodeId;

/// The file, in the agent's data directory, holding the node identity string
/// (`node-<uuid>`, ADR 0024). Written durably beside the journal.
pub const NODE_IDENTITY_FILE: &str = "node-identity";

/// The temp name [`write_atomic`] swaps through. Fixed rather than unique: the
/// identity is not a secret, and a stale temp from an earlier crash is expected
/// and replaced.
const NODE_IDENTITY_TMP: &str = "node-identity.tmp";

/// Load the persisted node identity from `<data_dir>/node-identity`.
///
/// `Ok(None)` when the file is absent — the caller decides whether that is a
/// fresh installation (mint) or a missing file beside prior state (refuse).
/// `Err` when the file
/// exists but cannot be read or does not parse as a `node-<uuid>`: a directory
/// this agent must not paper over.
pub fn load_node_identity(data_dir: &Path) -> Result<Option<NodeId>> {
    let fs = RealFs::new(data_dir);
    let rel = Path::new(NODE_IDENTITY_FILE);
    let display = data_dir.join(NODE_IDENTITY_FILE);
    if !fs
        .exists(rel)
        .with_context(|| format!("checking for {}", display.display()))?
    {
        return Ok(None);
    }
    let raw = read_to_vec(&fs, rel)
        .with_context(|| format!("reading node identity {}", display.display()))?;
    let text = String::from_utf8(raw)
        .with_context(|| format!("node identity {} is not UTF-8", display.display()))?;
    let id = text
        .trim()
        .parse::<NodeId>()
        .with_context(|| format!("parsing node identity {}", display.display()))?;
    Ok(Some(id))
}

/// The agent's persistent [`NodeId`]: read it back, or mint and persist one.
///
/// * File present → parse and return it (corrupt ⇒ error, never a re-mint).
/// * File absent, no prior agent state → mint [`NodeId::new`], write it
///   durably (`tmp` + fsync + rename + directory fsync, ADR 0017), return it.
/// * File absent but a journal is present → error instructing the operator to
///   restore the node id this installation already goes by, or start fresh.
///
/// `data_dir` is created if missing, so an agent pointed at a fresh path mints
/// without a separate setup step.
pub fn load_or_mint_node_identity(data_dir: &Path) -> Result<NodeId> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;

    if let Some(id) = load_node_identity(data_dir)? {
        return Ok(id);
    }

    let fs = RealFs::new(data_dir);
    let journal = Path::new(crate::journal::JOURNAL);
    let has_prior_state = fs.exists(journal).with_context(|| {
        format!(
            "checking for a prior agent journal in {}",
            data_dir.display()
        )
    })?;
    if has_prior_state {
        bail!(
            "{data} holds an agent journal but no {file}: the identity file has gone \
             missing from a directory this agent already ran in. Minting a fresh \
             identity would break journal fencing and the certificate CN binding, so \
             restore the node id this agent goes by into {path} (one line, \
             `node-<uuid>`) and restart — or point the agent at a fresh data_dir.",
            data = data_dir.display(),
            file = NODE_IDENTITY_FILE,
            path = data_dir.join(NODE_IDENTITY_FILE).display(),
        );
    }

    let id = NodeId::new();
    write_atomic(
        &fs,
        Path::new(NODE_IDENTITY_FILE),
        Path::new(NODE_IDENTITY_TMP),
        format!("{id}\n").as_bytes(),
    )
    .with_context(|| {
        format!(
            "writing node identity {}",
            data_dir.join(NODE_IDENTITY_FILE).display()
        )
    })?;
    tracing::info!(
        node_id = %id,
        path = %data_dir.join(NODE_IDENTITY_FILE).display(),
        "minted a fresh node identity"
    );
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_dir_mints_and_persists() {
        let dir = tempfile::tempdir().expect("temp dir");
        let id = load_or_mint_node_identity(dir.path()).expect("mints");
        let raw = std::fs::read_to_string(dir.path().join(NODE_IDENTITY_FILE)).expect("persisted");
        assert_eq!(raw.trim().parse::<NodeId>().expect("typed form"), id);
    }

    #[test]
    fn a_missing_data_dir_is_created() {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("var").join("lib").join("agent");
        let id = load_or_mint_node_identity(&nested).expect("mints");
        assert_eq!(load_node_identity(&nested).expect("loads"), Some(id));
    }

    #[test]
    fn second_call_returns_the_same_id() {
        let dir = tempfile::tempdir().expect("temp dir");
        let first = load_or_mint_node_identity(dir.path()).expect("mints");
        let second = load_or_mint_node_identity(dir.path()).expect("loads");
        assert_eq!(first, second);
    }

    #[test]
    fn absent_file_loads_as_none() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(load_node_identity(dir.path()).expect("absent"), None);
    }

    #[test]
    fn a_corrupt_file_errors_and_is_left_alone() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join(NODE_IDENTITY_FILE);
        std::fs::write(&path, "not-a-node-id\n").expect("write");

        let err = load_or_mint_node_identity(dir.path()).expect_err("corrupt file is fatal");
        let text = format!("{err:#}");
        assert!(
            text.contains(&path.display().to_string()),
            "the error names the path: {text}"
        );
        // Never re-minted over.
        assert_eq!(
            std::fs::read_to_string(&path).expect("still there"),
            "not-a-node-id\n"
        );
    }

    #[test]
    fn a_wrongly_typed_id_is_corrupt_too() {
        let dir = tempfile::tempdir().expect("temp dir");
        // A well-formed id of the *wrong* type must not be accepted (ADR 0024).
        std::fs::write(
            dir.path().join(NODE_IDENTITY_FILE),
            format!("{}\n", coppice_core::id::JobId::new()),
        )
        .expect("write");
        assert!(load_node_identity(dir.path()).is_err());
    }

    #[test]
    fn a_journal_without_an_identity_file_errors() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join(crate::journal::JOURNAL), b"").expect("write");

        let err =
            load_or_mint_node_identity(dir.path()).expect_err("a missing identity file is fatal");
        let text = format!("{err:#}");
        assert!(
            text.contains(NODE_IDENTITY_FILE),
            "the error names the file to restore: {text}"
        );
        assert!(
            !dir.path().join(NODE_IDENTITY_FILE).exists(),
            "nothing is minted over an existing installation"
        );
    }

    #[test]
    fn a_seeded_identity_beside_a_journal_loads() {
        // The documented remedy for the case above: restore the id, restart.
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join(crate::journal::JOURNAL), b"").expect("write");
        let existing = NodeId::new();
        std::fs::write(dir.path().join(NODE_IDENTITY_FILE), format!("{existing}\n"))
            .expect("write");
        assert_eq!(
            load_or_mint_node_identity(dir.path()).expect("loads"),
            existing
        );
    }
}
