//! Machine identity: minting and data-directory persistence (ADR 0037 §7).
//!
//! A machine identity is a cluster-minted opaque [`MachineId`] carried in a
//! coordinator leaf's subject and bound to at most one raft node id, ever. Its
//! required behavior (§7): stable across restarts that retain persistent state,
//! and freshly minted when an installation starts with fresh state. This module
//! owns both the mint (kept here so the audit-scope convention — see the module
//! docs — covers identity minting too) and the durable file beside the manifest.

use std::io;
use std::path::Path;

use coppice_core::id::MachineId;

use super::atomic_write_private;

/// The file, in the installation's data directory, holding the machine identity
/// string. Written durably and owner-only, beside the storage manifest.
pub const MACHINE_IDENTITY_FILE: &str = "machine-identity";

/// A failure persisting or loading the machine identity.
#[derive(Debug, thiserror::Error)]
pub enum MachineIdentityError {
    /// The identity file could not be written.
    #[error("writing machine identity {}: {source}", path.display())]
    Write {
        path: std::path::PathBuf,
        source: io::Error,
    },

    /// The identity file could not be read.
    #[error("reading machine identity {}: {source}", path.display())]
    Read {
        path: std::path::PathBuf,
        source: io::Error,
    },

    /// The identity file's contents did not parse as a `machine-<uuid>` id.
    #[error("parsing machine identity {}: {reason}", path.display())]
    Parse {
        path: std::path::PathBuf,
        reason: String,
    },
}

/// Mint a fresh machine identity (ADR 0037 §7). A thin wrapper over
/// [`MachineId::new`], present so the audit-scope convention (module docs)
/// covers identity minting alongside certificate minting.
pub fn mint_machine_identity() -> MachineId {
    MachineId::new()
}

/// Persist `id` to `<dir>/machine-identity` durably and owner-only (ADR 0037
/// §7: alongside the manifest). Overwrites any existing file atomically.
pub fn persist_machine_identity(dir: &Path, id: &MachineId) -> Result<(), MachineIdentityError> {
    let path = dir.join(MACHINE_IDENTITY_FILE);
    atomic_write_private(&path, id.to_string().as_bytes())
        .map_err(|source| MachineIdentityError::Write { path, source })
}

/// Load the machine identity from `<dir>/machine-identity`.
///
/// Returns `Ok(None)` when the file is absent — the ADR 0037 §7 "fresh
/// persistent state" case, where the caller mints a new identity. `Ok(Some(id))`
/// when it is present and parses; `Err` when it exists but cannot be read or its
/// contents do not parse (a corrupt directory the caller must not paper over).
pub fn load_machine_identity(dir: &Path) -> Result<Option<MachineId>, MachineIdentityError> {
    let path = dir.join(MACHINE_IDENTITY_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(MachineIdentityError::Read { path, source }),
    };
    let id = raw
        .trim()
        .parse::<MachineId>()
        .map_err(|e| MachineIdentityError::Parse {
            path,
            reason: e.to_string(),
        })?;
    Ok(Some(id))
}
