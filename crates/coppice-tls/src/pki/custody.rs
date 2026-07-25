//! CA-key custody: the local half (ADR 0037 §4).
//!
//! The CA private key never enters replicated state; it lives as an owner-only
//! file in a voter's data directory. This module owns that file's durable write
//! and a load that enforces the two custody invariants a later chunk's transfer
//! protocol builds on: the file must not be group/world-readable, and the key
//! must match the CA certificate. Chunk 06 layers the key-before-membership
//! transfer protocol (§4) on top of these primitives.

use std::io;
use std::path::{Path, PathBuf};

use super::{atomic_write_private, ca_public_key_der, key_public_key_der};

/// The CA private key file within a voter's data directory.
pub const CA_KEY_FILE: &str = "ca.key";

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
    let path = dir.join(CA_KEY_FILE);
    atomic_write_private(&path, key_pem).map_err(|source| CustodyError::Write { path, source })
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

/// Load `<dir>/ca.key`, enforcing the custody invariants (ADR 0037 §4):
///
/// - the file must exist ([`CustodyError::NotFound`] otherwise);
/// - it must **not** be group/world-readable ([`CustodyError::InsecurePermissions`]);
/// - its key must match `ca_cert_pem` ([`CustodyError::KeyMismatch`], its own
///   variant).
pub fn load_ca_key(dir: &Path, ca_cert_pem: &[u8]) -> Result<KeyPem, CustodyError> {
    let path = dir.join(CA_KEY_FILE);

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
