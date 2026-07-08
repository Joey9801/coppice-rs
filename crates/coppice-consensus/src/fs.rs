//! The filesystem seam the storage engine is written against.
//!
//! ADRs 0002 and 0015–0018 define the durable formats in terms of a small set
//! of *distinct durability events*: appending to an open stream, fsyncing a
//! file's data, renaming, fsyncing a parent directory so a create/rename/delete
//! survives, scanning a directory, and holding the `LOCK` file. This trait
//! exposes exactly those events and nothing more — it is deliberately not a
//! general VFS. Modeling only what the format ADRs do keeps the seam cheap to
//! implement twice ([`RealFs`] here; the fault-injecting `SimFs` in
//! `coppice-testkit`) and cheap to use, which is what keeps the crash-injection
//! suite honest: storage code that bypasses the seam is invisible to the suite
//! and must not exist.
//!
//! # Why the seam is synchronous
//!
//! openraft's storage traits are async, but this seam is sync, adapted at the
//! storage-engine layer (dedicated writer thread or `spawn_blocking`). Two
//! reasons, decided once here:
//!
//! - The write path is fsync-dominated. A group-commit loop is a thread that
//!   blocks on `fdatasync` by design; running it as blocking tasks on the
//!   tokio pool adds scheduling latency to every commit and starves the pool
//!   under load. A dedicated thread with an mpsc of pending appends is the
//!   shape the engine wants anyway.
//! - Determinism in the crash suite. A sync seam has no await points, so a
//!   simulated crash at operation *k* is exactly reproducible from a seed;
//!   an async seam would interleave with the runtime's scheduling.
//!
//! # Path discipline
//!
//! All paths are **relative to the storage root** (the `<data-dir>` of
//! ADR 0017) and must be plain relative paths — no absolute paths, no `..`.
//! [`RealFs`] anchors them at a root directory; `SimFs` uses them as keys in
//! its simulated namespace.
//!
//! # Durability contract (what the crash suite enforces)
//!
//! - [`FsFile::append`] makes bytes *visible* to readers, not durable.
//! - [`FsFile::sync_data`] makes the file's current contents (and length)
//!   durable.
//! - [`Fs::create_new`], [`Fs::rename`], and [`Fs::remove_file`] change the
//!   namespace *visibly*; the change is durable only after
//!   [`Fs::sync_dir`] on the parent directory. In particular a file whose
//!   data was fsynced but whose directory entry was not can vanish entirely
//!   at a crash — this is the failure mode the atomic-swap discipline exists
//!   to close, and `SimFs` simulates it.
//! - [`Fs::rename`] is atomic with respect to a crash: after recovery the
//!   destination holds either the old or the new file, never a mixture.

use std::io;
use std::path::{Path, PathBuf};

/// The filesystem operations the storage layer is allowed to perform.
///
/// Implementations: [`RealFs`] (thin wrapper over `std::fs`), and
/// `coppice_testkit::SimFs` (deterministic fault injection). The storage
/// engine is generic over this trait; every durability decision it makes is
/// therefore observable and perturbable by the crash suite.
pub trait Fs: Send + Sync + 'static {
    /// An open file handle.
    ///
    /// Independent handles to the same file observe the same visible
    /// contents.
    type File: FsFile;

    /// Guard for the storage `LOCK` file; dropping it releases the lock. A
    /// process crash releases it implicitly (advisory locks die with the fd).
    type Lock: Send + 'static;

    /// Take the exclusive advisory lock at `path`, creating the file if
    /// absent. Fails if another live holder exists — this is the ADR 0017
    /// second-opener refusal, not a blocking wait.
    fn lock(&self, path: &Path) -> io::Result<Self::Lock>;

    /// Create a directory and any missing parents. Idempotent.
    ///
    /// Durability of the new entries follows the same rule as files:
    /// [`Fs::sync_dir`] on the parent.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// File names (not paths) of the entries in a directory, sorted, so
    /// recovery scans (`log/`, `snap/`) are deterministic.
    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>>;

    /// Whether a file or directory is visible at `path`.
    fn exists(&self, path: &Path) -> io::Result<bool>;

    /// Open an existing file for reading ([`FsFile::read_at`], [`FsFile::len`]).
    fn open_read(&self, path: &Path) -> io::Result<Self::File>;

    /// Open an existing file for appending; reads through the same handle see
    /// the file's full visible contents. Used for the active log segment.
    fn open_append(&self, path: &Path) -> io::Result<Self::File>;

    /// Create a file that must not already exist, open for append.
    ///
    /// Used for new segments and the temp side of every atomic swap.
    fn create_new(&self, path: &Path) -> io::Result<Self::File>;

    /// Atomically rename `from` to `to`, replacing any existing `to`.
    ///
    /// Visible immediately; durable after [`Fs::sync_dir`] of the parent.
    /// Both paths must share a parent directory (all ADR 0017 swaps do).
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Delete a file.
    ///
    /// Visible immediately; durable after [`Fs::sync_dir`] of the parent —
    /// an un-synced delete can *reappear* after a crash, which is why
    /// recovery's orphan cleanup must be idempotent.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// fsync a directory, making all namespace changes inside it (creates,
    /// renames, deletes) durable.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
}

/// An open file.
///
/// Only the access patterns the formats need: append at the tail,
/// positioned reads, truncate (torn-tail repair only), and data fsync.
// `len` here is a fallible size query, not a collection length; an
// `is_empty` counterpart would be noise.
#[allow(clippy::len_without_is_empty)]
pub trait FsFile: Send + 'static {
    /// Write `data` at the current end of file.
    ///
    /// Visible to all handles immediately; durable only after
    /// [`FsFile::sync_data`].
    fn append(&mut self, data: &[u8]) -> io::Result<()>;

    /// Read up to `buf.len()` bytes at `offset`, returning the count read
    /// (short only at end of file).
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Current visible length.
    fn len(&self) -> io::Result<u64>;

    /// Shrink the file to `len`.
    ///
    /// Exists solely for recovery's torn-tail repair of the active segment
    /// (ADR 0002); sealed bytes are never rewritten (ADR 0017). Durable
    /// after [`FsFile::sync_data`].
    fn truncate(&mut self, len: u64) -> io::Result<()>;

    /// Make the file's visible contents and length durable (`fdatasync`).
    /// This is the acknowledgement barrier: nothing is acked to openraft or
    /// a client until the bytes it depends on are behind one of these.
    fn sync_data(&self) -> io::Result<()>;

    /// Read exactly `buf.len()` bytes at `offset` or fail with
    /// [`io::ErrorKind::UnexpectedEof`].
    fn read_exact_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(offset, buf)? {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "read_exact_at past end of file",
                    ))
                }
                n => {
                    offset += n as u64;
                    buf = &mut buf[n..];
                }
            }
        }
        Ok(())
    }
}

/// Write a small file with the ADR 0017 atomic-swap discipline: write-new to
/// `tmp`, fsync, rename over `path`, fsync the parent directory. On return the
/// new contents are durable; a crash at any interior point leaves either the
/// old file or the new one, never a mixture.
///
/// This is *the* way the vote file and manifest are updated — engine code
/// should call this rather than restating the sequence, so the discipline has
/// one implementation to audit and one for the crash suite to break.
pub fn write_atomic<F: Fs>(fs: &F, path: &Path, tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    // A stale temp file from an earlier crash is expected; replace it.
    if fs.exists(tmp)? {
        fs.remove_file(tmp)?;
    }
    let mut file = fs.create_new(tmp)?;
    file.append(bytes)?;
    file.sync_data()?;
    drop(file);
    fs.rename(tmp, path)?;
    fs.sync_dir(parent_of(path))?;
    Ok(())
}

/// Read a whole (small) file.
///
/// For the vote file and manifest; segments and snapshots are streamed,
/// not slurped.
pub fn read_to_vec<F: Fs>(fs: &F, path: &Path) -> io::Result<Vec<u8>> {
    let file = fs.open_read(path)?;
    let len = file.len()?;
    let len = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file too large to slurp"))?;
    let mut buf = vec![0u8; len];
    file.read_exact_at(0, &mut buf)?;
    Ok(buf)
}

/// The parent of a root-relative path ("" for a top-level file, which both
/// implementations treat as the storage root itself).
fn parent_of(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new(""))
}

/// The real filesystem, anchored at a root directory.
///
/// A thin, unclever mapping to `std::fs` — all policy lives in the engine,
/// all fault injection in `SimFs`.
///
/// Note on macOS: `fsync` there does not guarantee platter durability without
/// `F_FULLFSYNC`; we accept `fsync` semantics uniformly, as production targets
/// Linux.
pub struct RealFs {
    root: PathBuf,
}

impl RealFs {
    /// Anchor at `root`, which must already exist.
    pub fn new(root: impl Into<PathBuf>) -> RealFs {
        RealFs { root: root.into() }
    }

    /// Resolve a root-relative path, rejecting absolute paths and `..`.
    fn full(&self, path: &Path) -> io::Result<PathBuf> {
        use std::path::Component;
        if path
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("storage paths must be root-relative: {}", path.display()),
            ));
        }
        Ok(self.root.join(path))
    }
}

/// Holds the `LOCK` file open; the advisory lock lives exactly as long as
/// this guard (or the process).
pub struct RealLock {
    file: std::fs::File,
}

impl Drop for RealLock {
    fn drop(&mut self) {
        // Unlock explicitly for clarity; closing the fd would release it too.
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

/// A real file opened for reading and/or appending.
pub struct RealFile {
    file: std::fs::File,
}

impl Fs for RealFs {
    type File = RealFile;
    type Lock = RealLock;

    fn lock(&self, path: &Path) -> io::Result<RealLock> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.full(path)?)?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(RealLock { file })
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(self.full(path)?)
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(self.full(path)?)? {
            names.push(entry?.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        Ok(names)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        Ok(self.full(path)?.exists())
    }

    fn open_read(&self, path: &Path) -> io::Result<RealFile> {
        let file = std::fs::OpenOptions::new().read(true).open(self.full(path)?)?;
        Ok(RealFile { file })
    }

    fn open_append(&self, path: &Path) -> io::Result<RealFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(self.full(path)?)?;
        Ok(RealFile { file })
    }

    fn create_new(&self, path: &Path) -> io::Result<RealFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(self.full(path)?)?;
        Ok(RealFile { file })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(self.full(from)?, self.full(to)?)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(self.full(path)?)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        // Opening a directory read-only and fsyncing it is the portable Unix
        // idiom for making its entries durable.
        std::fs::File::open(self.full(path)?)?.sync_all()
    }
}

impl FsFile for RealFile {
    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        // O_APPEND positions every write at the tail regardless of any reads.
        io::Write::write_all(&mut self.file, data)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(&self.file, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{read_to_vec, write_atomic, Fs, FsFile, RealFs};

    fn realfs() -> (tempfile::TempDir, RealFs) {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = RealFs::new(dir.path());
        (dir, fs)
    }

    #[test]
    fn append_read_len_truncate_roundtrip() {
        let (_dir, fs) = realfs();
        let mut f = fs.create_new(Path::new("seg")).unwrap();
        f.append(b"hello ").unwrap();
        f.append(b"world").unwrap();
        assert_eq!(f.len().unwrap(), 11);

        let mut buf = [0u8; 5];
        f.read_exact_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"world");

        f.truncate(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
        assert_eq!(read_to_vec(&fs, Path::new("seg")).unwrap(), b"hello");
    }

    #[test]
    fn atomic_swap_replaces_contents() {
        let (_dir, fs) = realfs();
        write_atomic(&fs, Path::new("manifest"), Path::new("manifest.tmp"), b"v1").unwrap();
        write_atomic(&fs, Path::new("manifest"), Path::new("manifest.tmp"), b"v2").unwrap();
        assert_eq!(read_to_vec(&fs, Path::new("manifest")).unwrap(), b"v2");
        assert!(!fs.exists(Path::new("manifest.tmp")).unwrap());
    }

    #[test]
    fn create_new_refuses_existing() {
        let (_dir, fs) = realfs();
        fs.create_new(Path::new("f")).unwrap();
        assert!(fs.create_new(Path::new("f")).is_err());
    }

    #[test]
    fn list_dir_is_sorted_names() {
        let (_dir, fs) = realfs();
        fs.create_dir_all(Path::new("log")).unwrap();
        fs.create_new(Path::new("log/10.seg")).unwrap();
        fs.create_new(Path::new("log/2.seg")).unwrap();
        assert_eq!(fs.list_dir(Path::new("log")).unwrap(), vec!["10.seg", "2.seg"]);
    }

    #[test]
    fn second_locker_is_refused() {
        let (_dir, fs) = realfs();
        let guard = fs.lock(Path::new("LOCK")).unwrap();
        assert!(fs.lock(Path::new("LOCK")).is_err());
        drop(guard);
        fs.lock(Path::new("LOCK")).unwrap();
    }

    #[test]
    fn paths_may_not_escape_the_root() {
        let (_dir, fs) = realfs();
        assert!(fs.exists(Path::new("../escape")).is_err());
        assert!(fs.exists(Path::new("/abs")).is_err());
    }
}
