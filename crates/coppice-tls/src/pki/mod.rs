//! Cluster-owned certificate issuance (ADR 0037 §4).
//!
//! ADR 0022 fixed certificate *semantics* and left the trust root's provenance
//! open; ADR 0037 §4 decides it: **the cluster owns its CA by default**, and a
//! minimal deployment provisions no certificates at all. This module is that
//! CA. It mints the cluster root (§4), signs the three ADR 0022 leaf profiles
//! (coordinator / agent / operator), verifies a leaf back to the CA for authz,
//! mints and persists the opaque machine identity a coordinator leaf's subject
//! carries (§7), hashes enrollment-token secrets (§5), and holds the local half
//! of CA-key custody (§4) — the durable owner-only files a data directory keeps.
//!
//! ## Audit scope (a convention, kept by construction)
//!
//! **All production certificate, CSR-signing, CA-key, and machine-identity
//! minting in the workspace lives under `coppice_tls::pki`.** Nothing else in
//! the tree calls rcgen's signing entry points for real material — the only
//! other rcgen use is throwaway test fixtures (`coppice-cli`'s `dev` PKI and the
//! coordinator/consensus test harnesses), which never touch this module. Keeping
//! issuance in one place is what makes the ADR 0037 §4 blast-radius reasoning
//! auditable: **only coordinator code paths may call the signing entry points**
//! ([`CaSigner`] / `mint_*` / `issue_*`), because signing runs on the leader,
//! which is always a voter and therefore always holds the CA key. A grep for
//! `CaSigner` / `mint_root_ca` / `issue_` is the whole audit surface.
//!
//! ## Layout
//!
//! - [`ca`] — [`mint_root_ca`] and the [`CaSigner`] that signs under a loaded CA.
//! - [`issue`] — the three leaf profiles from a submitted CSR (`issue_*`) or from
//!   an in-process keypair (`mint_*_local`, the no-CSR formation path, §3 step 5).
//! - [`verify`] — [`verify_leaf`], a real webpki chain check plus profile
//!   classification, for later chunks' authorization.
//! - [`csr`] — [`generate_key_and_csr`], the enrollee's pre-credential half.
//! - [`machine`] — [`mint_machine_identity`] and its data-directory persistence.
//! - [`token`] — enrollment-token secret generation, hashing, and verification (§5).
//! - [`custody`] — the local CA-key file: durable owner-only write, and a load
//!   that refuses loose permissions and a key that does not match the CA (§4).

use std::io;
use std::path::Path;

use coppice_core::id::{MachineId, NodeId};

use crate::LeafSubject;

pub mod ca;
pub mod csr;
pub mod custody;
pub mod issue;
pub mod machine;
pub mod token;
pub mod verify;

pub use ca::{mint_root_ca, CaError, CaMaterial, CaSigner};
pub use csr::{generate_key_and_csr, CsrError};
pub use custody::{install_leaf_material, load_ca_key, write_ca_key, CustodyError, CA_KEY_FILE};
pub use issue::{
    issue_agent, issue_coordinator, issue_operator, mint_agent_local, mint_coordinator_local,
    mint_operator_local, IssueError,
};
pub use machine::{
    load_machine_identity, mint_machine_identity, persist_machine_identity, MachineIdentityError,
    MACHINE_IDENTITY_FILE,
};
pub use token::{generate_secret, hash_secret, verify_secret, TokenError, TOKEN_PREFIX};
pub use verify::{verify_leaf, VerifyError};

// ---------------------------------------------------------------------------
// Lifetimes and profile markers — the single place these values are defined.
// ---------------------------------------------------------------------------

/// The cluster root CA's validity span (ADR 0037 §4, "long-lived"). ~20 years:
/// the root outlives every leaf generation and the cluster's expected life; a
/// suspected-compromise response is re-rooting (§4), not waiting out an expiry.
pub const CA_LIFETIME: time::Duration = time::Duration::days(365 * 20);

/// A leaf certificate's default validity span. Deliberately short: renewal
/// preserves the subject and is free (ADR 0037 §4), and with short leaves,
/// operator-marked revocation aged out by expiry is v1's revocation mechanism
/// (no CRL, no OCSP).
pub const LEAF_LIFETIME: time::Duration = time::Duration::days(30);

/// Backdate `not_before` by this much so a freshly-issued leaf validates on a
/// peer whose clock trails the issuer's by a little.
pub const CLOCK_SKEW_ALLOWANCE: time::Duration = time::Duration::minutes(5);

/// The subject common name of the cluster root CA.
pub const CA_COMMON_NAME: &str = "coppice-cluster-ca";

/// The organizational-unit profile marker on an **operator** leaf (ADR 0022 /
/// ADR 0037 §4: break-glass and day-0 admin authority). Plural, matching
/// ADR 0022's `OU=coppice-operators`.
pub const OPERATOR_OU: &str = "coppice-operators";

/// The organizational-unit profile marker on a **coordinator** (machine) leaf
/// (ADR 0037 §4). Its common name is the cluster-minted [`MachineId`] (§7).
pub const COORDINATOR_OU: &str = "coppice-coordinator";

/// The parsed identity carried in a verified leaf's subject — the crate-root
/// [`LeafSubject`] (`CN`/`OU`), reused so `pki` and the reload store speak one
/// leaf-subject type.
pub type LeafIdentity = LeafSubject;

/// A leaf's certificate profile, classified from its subject by [`verify_leaf`]
/// after the chain has been verified. Each variant carries the typed identity a
/// caller authorizes against (ADR 0022's three profiles, ADR 0037 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Profile {
    /// A coordinator machine leaf: `OU=coppice-coordinator`, `CN` = the
    /// cluster-minted machine identity (ADR 0037 §7).
    Coordinator(MachineId),
    /// An agent (compute-node) leaf: no `OU`, `CN` = the node id (ADR 0011).
    Agent(NodeId),
    /// An operator leaf: `OU=coppice-operators`, `CN` = the operator name; the
    /// principal is `cert:<CN>` (ADR 0022).
    Operator {
        /// The operator's common name (the `cert:<cn>` principal).
        cn: String,
    },
}

/// A leaf that verified against the cluster CA: its classified [`Profile`] and
/// the raw subject it was classified from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLeaf {
    /// The classified certificate profile plus its typed identity.
    pub profile: Profile,
    /// The raw subject (`CN`/`OU`) the profile was read from.
    pub subject: LeafIdentity,
}

// ---------------------------------------------------------------------------
// Shared internals: validity windows, public-key comparison, durable writes.
// ---------------------------------------------------------------------------

/// A `(not_before, not_after)` window: `now - CLOCK_SKEW_ALLOWANCE` to
/// `now + lifetime`.
pub(crate) fn validity_window(
    lifetime: time::Duration,
) -> (time::OffsetDateTime, time::OffsetDateTime) {
    let now = time::OffsetDateTime::now_utc();
    (now - CLOCK_SKEW_ALLOWANCE, now + lifetime)
}

/// The `SubjectPublicKeyInfo` DER of the first certificate in a CA PEM bundle —
/// what a private key's own SPKI is compared against to prove they are a pair.
pub(crate) fn ca_public_key_der(ca_cert_pem: &[u8]) -> Result<Vec<u8>, String> {
    let leaf = rustls_pemfile::certs(&mut std::io::Cursor::new(ca_cert_pem))
        .next()
        .ok_or_else(|| "no certificate in CA PEM".to_string())?
        .map_err(|e| format!("parsing CA certificate: {e}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|e| format!("parsing CA certificate DER: {e}"))?;
    Ok(cert.public_key().raw.to_vec())
}

/// The `SubjectPublicKeyInfo` DER a private key PEM would present in a
/// certificate — used to check a key matches a CA certificate (ADR 0037 §4:
/// loading refuses a key that is not the CA's).
pub(crate) fn key_public_key_der(key_pem: &[u8]) -> Result<Vec<u8>, String> {
    let s = std::str::from_utf8(key_pem).map_err(|_| "key PEM is not UTF-8".to_string())?;
    let key = rcgen::KeyPair::from_pem(s).map_err(|e| format!("parsing private key: {e}"))?;
    Ok(key.public_key_der())
}

/// Durable, owner-only (`0600`) write with `tmp + fsync + rename + dir fsync`
/// (ADR 0037 §4/§7: the CA key and the machine identity are written this way).
///
/// The temp file has a unique name and is opened `create_new` with mode `0600`,
/// so the secret never exists on disk group/world-readable: a fixed temp name
/// could pre-exist with loose permissions (mode is only applied at creation) or
/// as a symlink, and `create_new` + uniqueness forecloses both. The rename is
/// atomic, so a reader sees either the old file or the complete new one, never
/// a torn write; the directory fsync makes the rename itself durable, and its
/// failure is a write failure — this function never reports durability it did
/// not achieve. The temp file is removed on any failure.
pub(crate) fn atomic_write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
    let tmp = dir.join(tmp_name);

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(&tmp)?;

    fn commit(
        mut file: std::fs::File,
        tmp: &Path,
        path: &Path,
        dir: &Path,
        bytes: &[u8],
    ) -> io::Result<()> {
        use io::Write;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(tmp, path)?;
        // fsync the directory so the rename itself is durable. Chunk 06's
        // key-transfer ack rides on this claim, so an fsync failure here is a
        // failed write, not a warning.
        #[cfg(unix)]
        {
            std::fs::File::open(dir)?.sync_all()?;
        }
        #[cfg(not(unix))]
        let _ = dir;
        Ok(())
    }

    let result = commit(file, &tmp, path, dir, bytes);
    if result.is_err() {
        // Best-effort: after a successful rename the temp no longer exists and
        // this is a harmless no-op; before it, don't leave the partial secret.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests;
