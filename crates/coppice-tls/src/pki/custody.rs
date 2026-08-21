//! CA-key custody: the local half (ADR 0037 §4).
//!
//! The CA private key never enters replicated state; it lives as an owner-only
//! file in a voter's data directory. This module owns that file's durable write
//! and a load that enforces the two custody invariants a later chunk's transfer
//! protocol builds on: the file must not be group/world-readable, and the key
//! must match the CA certificate. Chunk 06 layers the key-before-membership
//! transfer protocol (§4) on top of these primitives.
//!
//! # Two key files, not one
//!
//! A re-root needs somewhere durable to put a root's private key *before* that
//! root is the one the cluster signs under, because the rotation's whole safety
//! property is that no root reaches the active bundle position until every
//! current voter durably holds its key. So a data directory holds up to two:
//! [`CA_KEY_FILE`], the private half of the bundle's **active** root, and
//! [`CA_STAGED_KEY_FILE`], the private half of a **pending** one. The
//! invariants are identical — same permissions check, same key-matches-cert
//! check — only the certificate they are held against differs.

use std::io;
use std::path::{Path, PathBuf};

use super::{atomic_write_private, ca_public_key_der, key_public_key_der};

/// The CA private key file within a voter's data directory — the **live**
/// one, the private half of the bundle's active (position 0) root.
pub const CA_KEY_FILE: &str = "ca.key";

/// The **staged** CA private key file: the private half of a re-root's pending
/// root, while that root sits at a non-active bundle position (ADR 0037 §4).
///
/// A rotation's durability invariant is that a root never becomes the active
/// signing root until its key is durably held by every current voter, and this
/// file is where "durably held" is made true. It is written before the pending
/// root is even recorded, so a crash at any point in the staged phase leaves
/// the outgoing root active and fully held — nothing signs under a key that
/// exists in one process's memory.
pub const CA_STAGED_KEY_FILE: &str = "ca-staged.key";

/// The staged root's **certificate**, written beside [`CA_STAGED_KEY_FILE`].
///
/// Redundant with replicated state — the pending root is a certificate of the
/// recorded bundle — and deliberately so: it is what lets an operator (or a
/// post-mortem) pair a staged key file with the root it belongs to without a
/// running cluster, which is exactly the situation the staged phase exists to
/// survive.
pub const CA_STAGED_CERT_FILE: &str = "ca-staged.crt";

/// A failure writing or loading the custody CA key.
#[derive(Debug, thiserror::Error)]
pub enum CustodyError {
    /// The key file could not be written.
    #[error("writing CA key {}: {source}", path.display())]
    Write { path: PathBuf, source: io::Error },

    /// The key file could not be read.
    #[error("reading CA key {}: {source}", path.display())]
    Read { path: PathBuf, source: io::Error },

    /// The key file is absent — a distinct signal from a read failure.
    #[error("CA key {} not found", path.display())]
    NotFound { path: PathBuf },

    /// The key file is group- or world-accessible; a private key must be
    /// owner-only (ADR 0037 §4). Distinct from every other error so an operator
    /// sees a permissions problem, not a generic load failure.
    #[error("CA key {} has insecure permissions {mode:04o} (must be owner-only)", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },

    /// The CA certificate handed in could not be parsed.
    #[error("parsing CA certificate for the custody match check: {0}")]
    BadCaCert(String),

    /// The stored key could not be parsed.
    #[error("parsing stored CA key: {0}")]
    BadKey(String),

    /// The stored key does not match the CA certificate. Its **own** variant,
    /// distinct from missing / unreadable / bad-permissions: ADR 0037 §4 treats
    /// a mismatched key as a repair condition that must refuse membership
    /// changes, not as a plain load error.
    #[error("stored CA key does not match the CA certificate")]
    KeyMismatch { path: PathBuf },
}

/// The PEM bytes of a CA private key, returned by [`load_ca_key`].
pub type KeyPem = Vec<u8>;

/// Write `key_pem` to `<dir>/ca.key` durably and owner-only (ADR 0037 §4). The
/// file is `0600` before any bytes are written, and the write is
/// `tmp + fsync + rename + dir fsync`.
pub fn write_ca_key(dir: &Path, key_pem: &[u8]) -> Result<(), CustodyError> {
    write_key_file(dir, CA_KEY_FILE, key_pem)
}

/// Durably stage a re-root's pending root on this disk (ADR 0037 §4): its
/// certificate to [`CA_STAGED_CERT_FILE`] and its key to
/// [`CA_STAGED_KEY_FILE`], each an owner-only `tmp + fsync + rename + dir
/// fsync`.
///
/// The **key is written last**, for the same reason
/// [`install_leaf_material`] writes it last: a crash mid-stage must never
/// leave a key file whose certificate is missing, because the pairing is what
/// makes the key identifiable at all. Both orders are safe for the cluster —
/// nothing has been recorded yet — but only this one leaves a legible disk.
///
/// Idempotent by overwrite: re-staging the same material is a no-op in effect,
/// and staging a *replacement* pending root (a re-run of `begin` on a leader
/// that never received the previous one) correctly supersedes it.
pub fn stage_ca_material(dir: &Path, cert_pem: &[u8], key_pem: &[u8]) -> Result<(), CustodyError> {
    write_cert_file(dir, CA_STAGED_CERT_FILE, cert_pem)?;
    write_key_file(dir, CA_STAGED_KEY_FILE, key_pem)
}

/// Load the staged CA key, enforcing the same invariants as [`load_ca_key`]
/// against `ca_cert_pem` — which for this file is the **pending** root of the
/// recorded bundle, not the active one.
///
/// The mismatch case is the load-bearing one: a leader resuming a rotation
/// asks exactly this question ("is the staged key on my disk the private half
/// of the root this cluster currently stages?"), and a `KeyMismatch` is the
/// answer "no, that key belongs to a superseded pending root" — which is what
/// tells `begin` to mint a replacement rather than distribute a key no bundle
/// mentions.
pub fn load_staged_ca_key(dir: &Path, ca_cert_pem: &[u8]) -> Result<KeyPem, CustodyError> {
    load_key_file(dir, CA_STAGED_KEY_FILE, ca_cert_pem)
}

/// The staged root's certificate as written by [`stage_ca_material`], or
/// `None` when nothing is staged on this disk.
pub fn load_staged_ca_cert(dir: &Path) -> Result<Option<Vec<u8>>, CustodyError> {
    let path = dir.join(CA_STAGED_CERT_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CustodyError::Read { path, source }),
    }
}

/// Remove the staged pair, tolerating either file's absence.
///
/// Called once a staged root has been promoted to the live key (it is no
/// longer *staged*, it is the CA), and when a resumed rotation supersedes a
/// pending root. Leaving the files behind would be a root-equivalent copy with
/// no accounting entry, which is the one thing §4 custody does not permit.
pub fn discard_staged_ca_material(dir: &Path) -> Result<(), CustodyError> {
    for name in [CA_STAGED_KEY_FILE, CA_STAGED_CERT_FILE] {
        let path = dir.join(name);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(CustodyError::Write { path, source }),
        }
    }
    Ok(())
}

fn write_key_file(dir: &Path, name: &str, key_pem: &[u8]) -> Result<(), CustodyError> {
    let path = dir.join(name);
    atomic_write_private(&path, key_pem).map_err(|source| CustodyError::Write { path, source })
}

/// The certificate half of a staged pair. Owner-only like the key beside it:
/// the certificate is public material, but a `0600` neighbour is cheaper to
/// reason about than a per-file exception, and nothing reads this file across
/// a trust boundary.
fn write_cert_file(dir: &Path, name: &str, cert_pem: &[u8]) -> Result<(), CustodyError> {
    let path = dir.join(name);
    atomic_write_private(&path, cert_pem).map_err(|source| CustodyError::Write { path, source })
}

/// Install cluster-minted machine-plane material into the `[tls]` paths a
/// [`TlsStore`](crate::TlsStore) watches (ADR 0037 §4).
///
/// Formation (§3 step 3) and, from chunk 04, enrollment are the two producers
/// of that material; both land it here rather than writing the three files by
/// hand, so the private key is never momentarily group-readable and a
/// half-written trio is never observable by the reload poll. Each file is an
/// owner-only `tmp + fsync + rename + dir fsync`, and the **key is written
/// last**: the store's fingerprint gate only advances once all three agree, and
/// a crash mid-install leaves the previous key in place rather than a leaf
/// whose key is missing.
///
/// The caller triggers the pickup — [`TlsStore::force_reload`](crate::TlsStore::force_reload)
/// for an immediate swap, or nothing at all to let the mtime poll find it.
pub fn install_leaf_material(
    paths: &crate::TlsPaths,
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<(), CustodyError> {
    for (path, bytes) in [
        (&paths.ca, ca_pem),
        (&paths.cert, cert_pem),
        (&paths.key, key_pem),
    ] {
        atomic_write_private(path, bytes).map_err(|source| CustodyError::Write {
            path: path.clone(),
            source,
        })?;
    }
    Ok(())
}

/// Install just the **trust anchor bundle** into the `[tls] ca_path` a
/// [`TlsStore`](crate::TlsStore) watches, leaving the leaf and key alone.
///
/// Trust anchors are replicated state (ADR 0037 §4): the bundle the cluster
/// records *is* the set of roots every replica must trust, and adopting it
/// takes no signature, no dial and no leader — which is precisely why it is
/// separable from renewal. A re-root's ordering depends on that separation: a
/// node has to trust the incoming root **before** anyone signs under it, and a
/// node that could only learn the new anchors by dialing a peer already signing
/// under them could never catch up.
///
/// Safe to call with a leaf that does not chain to `ca_pem`: the store's
/// material carries the two independently (the anchors verify *peers*, the leaf
/// is what this node presents), so a node whose leaf is still under the
/// outgoing root keeps serving it while the bundle it trusts moves ahead.
///
/// Like [`install_leaf_material`], the write is an owner-only
/// `tmp + fsync + rename + dir fsync` and the caller triggers the pickup with
/// [`TlsStore::force_reload`](crate::TlsStore::force_reload).
pub fn install_ca_bundle(paths: &crate::TlsPaths, ca_pem: &[u8]) -> Result<(), CustodyError> {
    atomic_write_private(&paths.ca, ca_pem).map_err(|source| CustodyError::Write {
        path: paths.ca.clone(),
        source,
    })
}

/// Whether `key_pem` is the private half of `ca_cert_pem` — the same SPKI
/// comparison [`load_ca_key`] makes, on material that is not on disk yet.
///
/// The key-transfer recipient of ADR 0037 §4 checks this **before** writing:
/// the key it was handed must match the CA certificate the cluster already
/// replicates, or the transfer is a misdirected (or hostile) push and the
/// candidate must refuse rather than overwrite its own custody file.
pub fn key_matches_ca(ca_cert_pem: &[u8], key_pem: &[u8]) -> Result<(), CustodyError> {
    let ca_spki = ca_public_key_der(ca_cert_pem).map_err(CustodyError::BadCaCert)?;
    let key_spki = key_public_key_der(key_pem).map_err(CustodyError::BadKey)?;
    if ca_spki != key_spki {
        return Err(CustodyError::KeyMismatch {
            path: PathBuf::from(CA_KEY_FILE),
        });
    }
    Ok(())
}

/// Load `<dir>/ca.key`, enforcing the custody invariants (ADR 0037 §4):
///
/// - the file must exist ([`CustodyError::NotFound`] otherwise);
/// - it must **not** be group/world-readable ([`CustodyError::InsecurePermissions`]);
/// - its key must match `ca_cert_pem` ([`CustodyError::KeyMismatch`], its own
///   variant).
pub fn load_ca_key(dir: &Path, ca_cert_pem: &[u8]) -> Result<KeyPem, CustodyError> {
    load_key_file(dir, CA_KEY_FILE, ca_cert_pem)
}

/// The shared body of [`load_ca_key`] and [`load_staged_ca_key`]: one set of
/// custody invariants, applied to whichever of the two files is named, so the
/// staged key can never be held to a weaker standard than the live one.
fn load_key_file(dir: &Path, name: &str, ca_cert_pem: &[u8]) -> Result<KeyPem, CustodyError> {
    let path = dir.join(name);

    let meta = match std::fs::metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(CustodyError::NotFound { path })
        }
        Err(source) => return Err(CustodyError::Read { path, source }),
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(CustodyError::InsecurePermissions { path, mode });
        }
    }
    #[cfg(not(unix))]
    let _ = meta;

    let key_pem = std::fs::read(&path).map_err(|source| CustodyError::Read {
        path: path.clone(),
        source,
    })?;

    let ca_spki = ca_public_key_der(ca_cert_pem).map_err(CustodyError::BadCaCert)?;
    let key_spki = key_public_key_der(&key_pem).map_err(CustodyError::BadKey)?;
    if ca_spki != key_spki {
        return Err(CustodyError::KeyMismatch { path });
    }

    Ok(key_pem)
}
