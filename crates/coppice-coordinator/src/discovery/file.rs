//! The `file` discovery backend (ADR 0037 §2): a well-known directory in which
//! each file is one candidate whose **first line** is the raft address.
//!
//! This is what makes port-0 multi-process clusters on one dev machine work
//! with no harness coordination beyond a shared directory. Registrations are
//! **run-scoped**: each process, on binding its listeners, writes
//! `<dir>/<run-id>` (a fresh random token per launch — *not* the instance UUID;
//! registration happens before any identity exists) containing its advertised
//! raft address. The file is removed on graceful shutdown; a stale file from a
//! crash costs only a failed dial, because no protocol step requires every
//! discovered candidate to respond.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use uuid::Uuid;

use super::Discovery;

/// Enumerates a registration directory: each regular file contributes its first
/// line as one candidate raft address. Unreadable or empty files are skipped
/// with a warning — a stale or partially-written file must never wedge the
/// enumeration (ADR 0037 §2).
pub(crate) struct FileDiscovery {
    dir: PathBuf,
}

impl FileDiscovery {
    pub(crate) fn new(dir: PathBuf) -> Self {
        FileDiscovery { dir }
    }
}

#[tonic::async_trait]
impl Discovery for FileDiscovery {
    async fn candidates(&self) -> Vec<String> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(err) => {
                // A missing directory is normal before the first registration.
                tracing::warn!(
                    dir = %self.dir.display(),
                    error = %err,
                    "file discovery: reading registration directory failed; no candidates"
                );
                return Vec::new();
            }
        };

        let mut out = Vec::new();
        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    let path = entry.path();
                    // Skip subdirectories; only regular files are registrations.
                    if entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    match read_first_line(&path).await {
                        Some(addr) => out.push(addr),
                        None => tracing::warn!(
                            file = %path.display(),
                            "file discovery: registration file empty or unreadable; skipping"
                        ),
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    tracing::warn!(
                        dir = %self.dir.display(),
                        error = %err,
                        "file discovery: directory iteration failed; returning partial list"
                    );
                    break;
                }
            }
        }
        out
    }
}

/// Read and trim the first line of `path`, or `None` if the file is empty,
/// blank, or unreadable.
async fn read_first_line(path: &Path) -> Option<String> {
    let contents = tokio::fs::read_to_string(path).await.ok()?;
    let first = contents.lines().next()?.trim();
    if first.is_empty() {
        None
    } else {
        Some(first.to_string())
    }
}

/// A process's own registration in a `file` discovery directory (ADR 0037 §2).
///
/// Minting a **run id** — a fresh random token per process launch, distinct
/// from the instance UUID — is deliberate: registration happens before any raft
/// identity exists and needs none, it is advisory dialing information only.
/// The file is removed on [`remove`](FileRegistration::remove) at graceful
/// shutdown and, as a backstop, on [`Drop`]. A leftover file from a crash is
/// tolerated by design — a failed dial, nothing more.
pub struct FileRegistration {
    path: PathBuf,
}

impl FileRegistration {
    /// Register `advertised_addr` under `dir/<run-id>`, creating `dir` if
    /// absent. The run id is a fresh random token, so concurrent processes on
    /// one host never collide.
    pub fn register(dir: &Path, advertised_addr: &str) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| {
            format!(
                "creating file-discovery registration directory {}",
                dir.display()
            )
        })?;
        let run_id = Uuid::new_v4().to_string();
        let path = dir.join(run_id);
        std::fs::write(&path, format!("{advertised_addr}\n"))
            .with_context(|| format!("writing discovery registration file {}", path.display()))?;
        tracing::info!(
            file = %path.display(),
            addr = %advertised_addr,
            "file discovery: registered this process"
        );
        Ok(FileRegistration { path })
    }

    /// Remove the registration explicitly at graceful shutdown. Idempotent: a
    /// not-found removal is success (the [`Drop`] backstop may have run, or the
    /// file was cleaned externally).
    pub async fn remove(&self) {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => tracing::info!(
                file = %self.path.display(),
                "file discovery: removed this process's registration"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                file = %self.path.display(),
                error = %err,
                "file discovery: removing registration failed (stale file tolerated)"
            ),
        }
    }
}

impl Drop for FileRegistration {
    fn drop(&mut self) {
        // Best-effort synchronous backstop for non-graceful shutdown paths that
        // never call `remove`. A leftover file is harmless (ADR 0037 §2).
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_register_enumerate_remove() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileDiscovery::new(dir.path().to_path_buf());

        // Empty directory → no candidates.
        assert!(backend.candidates().await.is_empty());

        // Register one process; it becomes the single candidate.
        let reg = FileRegistration::register(dir.path(), "localhost:7071").expect("register");
        assert_eq!(
            backend.candidates().await,
            vec!["localhost:7071".to_string()]
        );

        // Explicit removal clears it.
        reg.remove().await;
        assert!(backend.candidates().await.is_empty());
    }

    #[tokio::test]
    async fn multiple_registrations_are_all_candidates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileDiscovery::new(dir.path().to_path_buf());

        let _a = FileRegistration::register(dir.path(), "localhost:1").expect("register a");
        let _b = FileRegistration::register(dir.path(), "localhost:2").expect("register b");

        let mut got = backend.candidates().await;
        got.sort();
        assert_eq!(
            got,
            vec!["localhost:1".to_string(), "localhost:2".to_string()]
        );
    }

    #[tokio::test]
    async fn drop_removes_the_registration_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileDiscovery::new(dir.path().to_path_buf());
        {
            let _reg = FileRegistration::register(dir.path(), "localhost:7071").expect("register");
            assert_eq!(backend.candidates().await.len(), 1);
        }
        // Drop backstop cleaned the file.
        assert!(backend.candidates().await.is_empty());
    }

    #[tokio::test]
    async fn stale_and_empty_files_are_tolerated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileDiscovery::new(dir.path().to_path_buf());

        // A good registration, an empty file, and a subdirectory.
        std::fs::write(dir.path().join("good"), "coord-9:7071\n").expect("write good");
        std::fs::write(dir.path().join("empty"), "").expect("write empty");
        std::fs::create_dir(dir.path().join("subdir")).expect("mkdir");

        // Only the good file contributes; the empty file and subdir are skipped.
        assert_eq!(backend.candidates().await, vec!["coord-9:7071".to_string()]);
    }

    #[tokio::test]
    async fn missing_directory_yields_empty_not_error() {
        let backend = FileDiscovery::new(PathBuf::from("/nonexistent/coppice/discovery/xyz"));
        assert!(backend.candidates().await.is_empty());
    }

    #[test]
    fn first_line_only_is_the_address() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("multi");
        std::fs::write(&path, "coord-1:7071\nignored second line\n").expect("write");
        let got = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(read_first_line(&path));
        assert_eq!(got, Some("coord-1:7071".to_string()));
    }
}
