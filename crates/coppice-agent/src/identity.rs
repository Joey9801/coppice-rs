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
//!
//! [`load_or_mint_node_identity`] is only the story under `[tls] source =
//! "cluster"` (issue #127), where the identity is minted first and a leaf is
//! later issued *for* it. Under `source = "external"` there is no issuer to
//! ask, so [`adopt_external_node_identity`] runs the binding the other way:
//! the operator-provisioned leaf already names a node id, the coordinator's
//! agent gateway already requires the leaf's CN to equal the claimed
//! [`NodeId`] (ADR 0037 §4), and minting a fresh random id here would simply
//! never be able to authenticate.

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
    persist_node_identity(data_dir, &id)?;
    tracing::info!(
        node_id = %id,
        path = %data_dir.join(NODE_IDENTITY_FILE).display(),
        "minted a fresh node identity"
    );
    Ok(id)
}

/// Durably write `id` to `<data_dir>/node-identity` (`tmp` + fsync + rename +
/// directory fsync, ADR 0017). Shared by [`load_or_mint_node_identity`]'s
/// mint path and [`adopt_external_node_identity`]'s adopt path — both need
/// exactly the same durable write, just for an id that arrived by a different
/// route (self-minted vs. read off a verified leaf).
fn persist_node_identity(data_dir: &Path, id: &NodeId) -> Result<()> {
    let fs = RealFs::new(data_dir);
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
    })
}

/// The agent's persistent [`NodeId`] under `[tls] source = "external"`
/// (issue #127, ADR 0037 §4/§7): **adopted from the verified leaf**, never
/// minted.
///
/// [`load_or_mint_node_identity`] mints a random id and only later has a
/// certificate issued *for* it — that inversion is exactly what enrollment
/// (deployment-story A2) wants, because the cluster is the one minting the
/// leaf and can always be told which id to put in it. Under external
/// provenance there is no such feedback loop: the leaf already exists,
/// provisioned by whatever issued it out of band, and the coordinator's agent
/// gateway authenticates a node by requiring the leaf's `CN` to equal its
/// claimed [`NodeId`] (ADR 0037 §4). A self-minted id that happens to differ
/// from the leaf's `CN` would simply never be able to log in, so on this path
/// the leaf is the authority and the on-disk identity file follows it rather
/// than the other way around.
///
/// Verifies the current material's leaf against its own CA bundle
/// ([`coppice_tls::pki::verify_leaf`]) and requires it to classify as
/// [`coppice_tls::pki::Profile::Agent`] — a coordinator or operator leaf
/// pointed at an agent's `[tls]` paths is a misconfiguration, not a node id.
/// Then reconciles the leaf's node id with `<data_dir>/node-identity`
/// ([`load_node_identity`], never [`load_or_mint_node_identity`] — minting a
/// random id on this path is exactly the bug this function exists to close):
///
/// * absent → persist the leaf's id durably and return it (first boot);
/// * present and equal → return it (every later boot: idempotent);
/// * present and different → a hard error naming both ids and both paths. The
///   leaf is still authoritative for identity, but the stored id may be load-
///   bearing for journal fencing or prior enrollment state, so this refuses
///   rather than silently overwriting it — the operator must either reissue
///   the leaf for the stored id, or start the node from a fresh data dir if
///   the leaf's id is the one that should stick.
///
/// `data_dir` is created if missing, mirroring
/// [`load_or_mint_node_identity`], so an agent pointed at a fresh path adopts
/// without a separate setup step.
pub fn adopt_external_node_identity(
    data_dir: &Path,
    tls: &coppice_tls::TlsStore,
) -> Result<NodeId> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;

    let cert_path = tls.paths().cert.display().to_string();
    let subject_requirements =
        "an external agent leaf must carry no OU, CN = the node id in `node-<uuid>` form \
         (ADR 0024), and SANs covering the host this agent advertises under [listen] \
         (ADR 0037 §4, issue #127)";

    let material = tls.current();
    let verified = coppice_tls::pki::verify_leaf(material.ca_pem(), material.cert_pem())
        .with_context(|| {
            format!(
                "verifying the externally-provisioned agent leaf {cert_path} against its CA \
                 bundle; {subject_requirements}"
            )
        })?;

    let leaf_node = match verified.profile {
        coppice_tls::pki::Profile::Agent(node) => node,
        coppice_tls::pki::Profile::Coordinator(machine) => {
            bail!(
                "the externally-provisioned leaf {cert_path} classifies as a coordinator leaf \
                 (OU=coppice-coordinator, CN={machine}), not an agent leaf; {subject_requirements}"
            );
        }
        coppice_tls::pki::Profile::Operator { cn } => {
            bail!(
                "the externally-provisioned leaf {cert_path} classifies as an operator leaf \
                 (OU=coppice-operators, CN={cn}), not an agent leaf; {subject_requirements}"
            );
        }
    };

    match load_node_identity(data_dir)? {
        None => {
            persist_node_identity(data_dir, &leaf_node)?;
            tracing::info!(
                node_id = %leaf_node,
                path = %data_dir.join(NODE_IDENTITY_FILE).display(),
                cert = %cert_path,
                "adopted the node identity named by the externally-provisioned leaf"
            );
            Ok(leaf_node)
        }
        Some(stored) if stored == leaf_node => Ok(stored),
        Some(stored) => {
            bail!(
                "the node identity stored at {identity_path} ({stored}) does not match the \
                 node id named by the externally-provisioned leaf {cert_path} ({leaf_node}); \
                 under [tls] source = \"external\" the leaf is authoritative for identity, so \
                 either reissue the leaf for {stored} or start this node from a fresh data_dir \
                 to adopt {leaf_node} (issue #127)",
                identity_path = data_dir.join(NODE_IDENTITY_FILE).display(),
            );
        }
    }
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

    // -----------------------------------------------------------------
    // adopt_external_node_identity
    // -----------------------------------------------------------------

    /// A throwaway root plus an agent leaf for `node`, written into `dir` as
    /// `node.crt` / `node.key` / `ca.crt` and loaded into a [`TlsStore`] —
    /// mirrors `crates/coppice-agent/tests/enrollment_config.rs`'s external
    /// fixture, so `tls.paths().cert` is a real, meaningful path for the error
    /// messages under test.
    fn seed_agent_tls(
        dir: &std::path::Path,
        node: &NodeId,
    ) -> std::sync::Arc<coppice_tls::TlsStore> {
        let ca = coppice_tls::pki::mint_root_ca().expect("mint a throwaway root");
        let signer =
            coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
        let (cert, key) =
            coppice_tls::pki::mint_agent_local(&signer, node, &["node-1.example.com".to_string()])
                .expect("mint a throwaway leaf");
        write_and_load_tls(dir, &ca.cert_pem, &cert, &key)
    }

    fn write_and_load_tls(
        dir: &std::path::Path,
        ca_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> std::sync::Arc<coppice_tls::TlsStore> {
        let paths = coppice_tls::TlsPaths {
            cert: dir.join("node.crt"),
            key: dir.join("node.key"),
            ca: dir.join("ca.crt"),
        };
        std::fs::write(&paths.cert, cert_pem).expect("write cert");
        std::fs::write(&paths.key, key_pem).expect("write key");
        std::fs::write(&paths.ca, ca_pem).expect("write ca");
        coppice_tls::TlsStore::load(paths).expect("load tls store")
    }

    #[test]
    fn adopt_external_identity_absent_file_adopts_and_persists() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");
        let node = NodeId::new();
        let tls = seed_agent_tls(tls_dir.path(), &node);

        let adopted = adopt_external_node_identity(data_dir.path(), &tls).expect("adopts");
        assert_eq!(adopted, node);

        let raw =
            std::fs::read_to_string(data_dir.path().join(NODE_IDENTITY_FILE)).expect("persisted");
        assert_eq!(raw.trim().parse::<NodeId>().expect("typed form"), node);
    }

    #[test]
    fn adopt_external_identity_is_idempotent() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");
        let node = NodeId::new();
        let tls = seed_agent_tls(tls_dir.path(), &node);

        let first = adopt_external_node_identity(data_dir.path(), &tls).expect("adopts");
        let second = adopt_external_node_identity(data_dir.path(), &tls).expect("still ok");
        assert_eq!(first, second);
    }

    #[test]
    fn adopt_external_identity_mismatch_names_both_ids() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");
        let leaf_node = NodeId::new();
        let tls = seed_agent_tls(tls_dir.path(), &leaf_node);

        let stored = NodeId::new();
        std::fs::write(
            data_dir.path().join(NODE_IDENTITY_FILE),
            format!("{stored}\n"),
        )
        .expect("write");

        let err = adopt_external_node_identity(data_dir.path(), &tls)
            .expect_err("a mismatched stored id is fatal");
        let text = format!("{err:#}");
        assert!(
            text.contains(&stored.to_string()) && text.contains(&leaf_node.to_string()),
            "the error names both ids: {text}"
        );
    }

    #[test]
    fn adopt_external_identity_rejects_a_coordinator_leaf() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");
        let ca = coppice_tls::pki::mint_root_ca().expect("mint a throwaway root");
        let signer =
            coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
        let (cert, key) = coppice_tls::pki::mint_coordinator_local(
            &signer,
            &coppice_core::id::MachineId::new(),
            &["coord-1.example.com".to_string()],
        )
        .expect("mint a throwaway coordinator leaf");
        let tls = write_and_load_tls(tls_dir.path(), &ca.cert_pem, &cert, &key);

        let err = adopt_external_node_identity(data_dir.path(), &tls)
            .expect_err("a coordinator leaf is not an agent leaf");
        let text = format!("{err:#}");
        assert!(
            text.contains("no OU") && text.contains("node id"),
            "the error explains the agent subject requirements: {text}"
        );
    }

    #[test]
    fn adopt_external_identity_rejects_an_operator_leaf() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");
        let ca = coppice_tls::pki::mint_root_ca().expect("mint a throwaway root");
        let signer =
            coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
        let (cert, key) = coppice_tls::pki::mint_operator_local(&signer, "alice")
            .expect("mint a throwaway operator leaf");
        let tls = write_and_load_tls(tls_dir.path(), &ca.cert_pem, &cert, &key);

        let err = adopt_external_node_identity(data_dir.path(), &tls)
            .expect_err("an operator leaf is not an agent leaf");
        let text = format!("{err:#}");
        assert!(
            text.contains("no OU") && text.contains("node id"),
            "the error explains the agent subject requirements: {text}"
        );
    }

    #[test]
    fn adopt_external_identity_rejects_a_leaf_that_does_not_chain() {
        let data_dir = tempfile::tempdir().expect("temp dir");
        let tls_dir = tempfile::tempdir().expect("temp dir");

        // Sign the leaf under a different root than the one the store trusts,
        // so the chain check fails.
        let issuing_ca = coppice_tls::pki::mint_root_ca().expect("mint issuing root");
        let signer = coppice_tls::pki::CaSigner::load(&issuing_ca.cert_pem, &issuing_ca.key_pem)
            .expect("load the signer");
        let (cert, key) = coppice_tls::pki::mint_agent_local(
            &signer,
            &NodeId::new(),
            &["node-1.example.com".to_string()],
        )
        .expect("mint a throwaway leaf");

        let untrusted_ca = coppice_tls::pki::mint_root_ca().expect("mint unrelated root");
        let tls = write_and_load_tls(tls_dir.path(), &untrusted_ca.cert_pem, &cert, &key);

        let cert_path = tls.paths().cert.display().to_string();
        let err = adopt_external_node_identity(data_dir.path(), &tls)
            .expect_err("a leaf that does not chain to the CA is fatal");
        let text = format!("{err:#}");
        assert!(
            text.contains(&cert_path),
            "the error names the leaf path: {text}"
        );
    }
}
