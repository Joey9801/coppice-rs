//! `SimFs`: the deterministic fault-injecting implementation of the
//! `coppice_consensus::fs` seam.
//!
//! `RealFs` cannot exercise the failure modes the durable formats (ADRs
//! 0002, 0015–0018) are designed around, because on a healthy machine the
//! page cache makes every write look durable. `SimFs` models the cache
//! explicitly: every operation is *visible* immediately but *durable* only
//! after the matching sync, with file data and directory entries tracked
//! separately — a file whose bytes were fsynced but whose `create_new` was
//! never made durable by a parent-directory sync vanishes wholesale at a
//! crash, and an un-synced `remove_file` resurrects its victim. A seeded
//! adversary ([`SimFs::crash`]) decides the fate of every un-synced
//! operation — dropped, applied, or (for appends) torn mid-write — so a
//! single logged `u64` reproduces any failure exactly ([`crate::rng`] is the
//! only randomness source). [`SimFs::set_crash_at`] additionally makes every
//! seam call an enumerable crash point: the harness can kill the simulated
//! process at op *k* for every *k* and assert recovery from each disk the
//! adversary can produce.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use coppice_consensus::fs::{Fs, FsFile};

use crate::rng::Rng;

/// Tuning for the crash adversary.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Torn appends normally lose a suffix at a multiple of this many bytes
    /// from the start of the write (page-cache granularity). Must be nonzero.
    /// A minority of tears land at an arbitrary byte instead, modeling
    /// sub-page/sector tears; CRCs must catch both.
    pub tear_granularity: u64,
}

impl Default for SimConfig {
    fn default() -> SimConfig {
        SimConfig { tear_granularity: 4096 }
    }
}

/// Marker payload of the [`io::Error`] returned by an injected crash point.
/// Detect it with [`is_sim_crash`]; storage code must treat it like any other
/// I/O error (its error-path cleanup runs against a poisoned fs and can do no
/// further damage).
#[derive(Debug)]
pub struct SimCrashed;

impl std::fmt::Display for SimCrashed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("simulated crash injected by SimFs")
    }
}

impl std::error::Error for SimCrashed {}

/// Whether an error is an injected [`SimCrashed`] crash point (as opposed to
/// a genuine seam error like `NotFound`, which the harness must not confuse
/// with a crash).
pub fn is_sim_crash(err: &io::Error) -> bool {
    err.get_ref().is_some_and(|e| e.is::<SimCrashed>())
}

fn crash_error() -> io::Error {
    io::Error::other(SimCrashed)
}

fn stale_handle_error() -> io::Error {
    io::Error::other("stale SimFs handle: the owning process has crashed since it was opened")
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no such file: {}", path.display()))
}

fn to_usize(v: u64) -> usize {
    usize::try_from(v).expect("simulated offsets fit in usize")
}

type Ino = u64;

/// The root directory's inode. It exists implicitly and `Path::new("")`
/// resolves to it, matching `fs::parent_of` for top-level files.
const ROOT: Ino = 0;

/// An un-synced mutation of one file's data, kept until `sync_data` promotes
/// it or the crash adversary decides its fate.
enum DataOp {
    /// `offset` is the visible end of file when the write happened, so the
    /// adversary can replay surviving writes at their true positions even
    /// when earlier ones were dropped (zero-filling the gap, as a page cache
    /// with a hole of never-written pages would).
    Append { offset: u64, bytes: Vec<u8> },
    Truncate { len: u64 },
}

/// An un-synced mutation of one directory's name table, kept until
/// `sync_dir` promotes it or the crash adversary decides its fate.
enum NsOp {
    /// `create_new` of a file or a `create_dir_all` step.
    Create { name: String, ino: Ino },
    Remove { name: String, ino: Ino },
    /// Same-directory rename; atomic across a crash (fully happened or
    /// fully didn't).
    Rename { from: String, to: String, ino: Ino },
}

/// A regular file: the crash-surviving image plus the journal of visible but
/// un-synced changes. `visible` is maintained eagerly (it always equals
/// `durable` with `journal` applied in order) so reads are cheap.
#[derive(Default)]
struct FileNode {
    durable: Vec<u8>,
    visible: Vec<u8>,
    journal: Vec<DataOp>,
}

/// A directory: the crash-surviving name table plus the journal of visible
/// but un-synced namespace changes. Same eager-visible invariant as files.
#[derive(Default)]
struct DirNode {
    durable: BTreeMap<String, Ino>,
    visible: BTreeMap<String, Ino>,
    journal: Vec<NsOp>,
}

enum Node {
    File(FileNode),
    Dir(DirNode),
}

struct Inner {
    config: SimConfig,
    /// `BTreeMap` (not `HashMap`) so the adversary visits inodes in a
    /// deterministic order; inode ids are allocated sequentially, so the same
    /// operation history always yields the same iteration order.
    nodes: BTreeMap<Ino, Node>,
    next_ino: Ino,
    op_count: u64,
    crash_at: Option<u64>,
    poisoned: bool,
    /// Bumped by `crash`; handles and locks from before the bump belonged to
    /// the dead process and must fail every operation.
    generation: u64,
    lock_holder: Option<u64>,
    next_lock_token: u64,
}

fn lock_inner(m: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    // A panicking test must not wedge every other test on mutex poisoning;
    // the data is a plain state machine, always valid.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Split a path into its component names, rejecting absolute paths and `..`
/// exactly as `RealFs` does.
fn path_names(path: &Path) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::Normal(name) => match name.to_str() {
                Some(s) => names.push(s.to_owned()),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("non-UTF-8 storage path: {}", path.display()),
                    ))
                }
            },
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("storage paths must be root-relative: {}", path.display()),
                ))
            }
        }
    }
    Ok(names)
}

impl Inner {
    /// Account for one seam call and fire the armed crash point if this is
    /// it. Called first by every `Fs`/`FsFile` method, reads included.
    fn enter(&mut self) -> io::Result<()> {
        let index = self.op_count;
        self.op_count += 1;
        if self.poisoned {
            return Err(crash_error());
        }
        if self.crash_at == Some(index) {
            self.poisoned = true;
            return Err(crash_error());
        }
        Ok(())
    }

    fn alloc(&mut self, node: Node) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        self.nodes.insert(ino, node);
        ino
    }

    fn file(&self, ino: Ino) -> &FileNode {
        match self.nodes.get(&ino) {
            Some(Node::File(f)) => f,
            _ => unreachable!("inode {ino} is a live file"),
        }
    }

    fn file_mut(&mut self, ino: Ino) -> &mut FileNode {
        match self.nodes.get_mut(&ino) {
            Some(Node::File(f)) => f,
            _ => unreachable!("inode {ino} is a live file"),
        }
    }

    fn dir_mut(&mut self, ino: Ino) -> &mut DirNode {
        match self.nodes.get_mut(&ino) {
            Some(Node::Dir(d)) => d,
            _ => unreachable!("inode {ino} is a live directory"),
        }
    }

    /// Walk the visible namespace to the inode at `names`, if any.
    fn lookup(&self, names: &[String]) -> Option<Ino> {
        let mut cur = ROOT;
        for name in names {
            match self.nodes.get(&cur) {
                Some(Node::Dir(d)) => cur = *d.visible.get(name)?,
                _ => return None,
            }
        }
        Some(cur)
    }

    fn resolve_dir(&self, path: &Path) -> io::Result<Ino> {
        let names = path_names(path)?;
        let ino = self.lookup(&names).ok_or_else(|| not_found(path))?;
        match self.nodes.get(&ino) {
            Some(Node::Dir(_)) => Ok(ino),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("not a directory: {}", path.display()),
            )),
        }
    }

    fn resolve_file(&self, path: &Path) -> io::Result<Ino> {
        let names = path_names(path)?;
        let ino = self.lookup(&names).ok_or_else(|| not_found(path))?;
        match self.nodes.get(&ino) {
            Some(Node::File(_)) => Ok(ino),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("is a directory: {}", path.display()),
            )),
        }
    }

    /// The (existing, visible) parent directory of `path` plus the final
    /// name. Errors if `path` is empty — the root has no name.
    fn resolve_parent(&self, path: &Path) -> io::Result<(Ino, String)> {
        let mut names = path_names(path)?;
        let name = names.pop().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path has no file name")
        })?;
        let parent = self.lookup(&names).ok_or_else(|| not_found(path))?;
        match self.nodes.get(&parent) {
            Some(Node::Dir(_)) => Ok((parent, name)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("parent is not a directory: {}", path.display()),
            )),
        }
    }
}

/// Surviving prefix length of a torn append of `len` bytes. Usually a
/// multiple of `gran` measured from the start of the write; with probability
/// 1/4 an arbitrary byte count instead (sub-page/sector tear). Always
/// strictly less than `len` — a full survival is the "applied" fate.
fn tear_len(rng: &mut Rng, len: u64, gran: u64) -> u64 {
    if rng.chance(1, 4) {
        rng.below(len)
    } else {
        rng.below(len.div_ceil(gran)) * gran
    }
}

/// Decide the fate of every un-synced data op of one file and rebuild its
/// durable image. Surviving ops land at their recorded offsets; if a dropped
/// or torn earlier append leaves a hole before a surviving later one, the
/// hole reads as zeros — never-written pages — and the file length grows to
/// cover the later write, exactly as a page cache flushing out of order does.
fn replay_file(f: &mut FileNode, rng: &mut Rng, gran: u64) {
    let mut img = std::mem::take(&mut f.durable);
    for op in f.journal.drain(..) {
        match op {
            DataOp::Truncate { len } => {
                if rng.chance(1, 2) {
                    img.truncate(to_usize(len));
                }
            }
            DataOp::Append { offset, bytes } => {
                let keep = match rng.below(3) {
                    0 => 0,
                    1 => bytes.len() as u64,
                    _ => tear_len(rng, bytes.len() as u64, gran),
                };
                if keep > 0 {
                    let (start, keep) = (to_usize(offset), to_usize(keep));
                    if img.len() < start + keep {
                        img.resize(start + keep, 0);
                    }
                    img[start..start + keep].copy_from_slice(&bytes[..keep]);
                }
            }
        }
    }
    f.durable = img;
    f.visible = f.durable.clone();
}

/// Decide the fate of every un-synced namespace op of one directory and
/// rebuild its durable name table. Each op independently survives with
/// probability 1/2; an op invalidated by an earlier casualty (create of a
/// name that still exists, remove/rename of an entry that isn't there)
/// degrades to a no-op. Renames are atomic: the destination ends up holding
/// the old inode or the new one, never a mixture.
fn replay_dir(d: &mut DirNode, rng: &mut Rng) {
    let mut table = std::mem::take(&mut d.durable);
    for op in d.journal.drain(..) {
        let survives = rng.chance(1, 2);
        if !survives {
            continue;
        }
        match op {
            NsOp::Create { name, ino } => {
                table.entry(name).or_insert(ino);
            }
            NsOp::Remove { name, ino } => {
                if table.get(&name) == Some(&ino) {
                    table.remove(&name);
                }
            }
            NsOp::Rename { from, to, ino } => {
                if table.get(&from) == Some(&ino) {
                    table.remove(&from);
                    table.insert(to, ino);
                }
            }
        }
    }
    d.durable = table;
    d.visible = d.durable.clone();
}

/// The fault-injecting filesystem. Cheap to clone (all clones share one
/// simulated disk); hand clones to the storage engine, keep one for the
/// harness controls.
#[derive(Clone)]
pub struct SimFs {
    inner: Arc<Mutex<Inner>>,
}

impl SimFs {
    pub fn new(config: SimConfig) -> SimFs {
        assert!(config.tear_granularity > 0, "tear_granularity must be nonzero");
        let mut nodes = BTreeMap::new();
        nodes.insert(ROOT, Node::Dir(DirNode::default()));
        SimFs {
            inner: Arc::new(Mutex::new(Inner {
                config,
                nodes,
                next_ino: ROOT + 1,
                op_count: 0,
                crash_at: None,
                poisoned: false,
                generation: 0,
                lock_holder: None,
                next_lock_token: 0,
            })),
        }
    }

    /// Seam operations attempted so far (`Fs` and `FsFile` methods, reads
    /// included). A clean workload run followed by `op_count` tells the
    /// harness how many crash points there are to enumerate.
    pub fn op_count(&self) -> u64 {
        lock_inner(&self.inner).op_count
    }

    /// Arm the crash point: the `op_index`-th seam call (0-based) does not
    /// execute; it fails with a [`SimCrashed`] error and poisons the fs so
    /// every later call fails the same way until [`SimFs::crash`]. Arming an
    /// index that has already passed has no effect.
    pub fn set_crash_at(&self, op_index: u64) {
        lock_inner(&self.inner).crash_at = Some(op_index);
    }

    /// Remove an armed crash point (an already-fired one keeps the fs
    /// poisoned).
    pub fn disarm(&self) {
        lock_inner(&self.inner).crash_at = None;
    }

    /// Whether an injected crash point has fired and the "process" is dead.
    pub fn is_poisoned(&self) -> bool {
        lock_inner(&self.inner).poisoned
    }

    /// Kill the simulated process and let the seeded adversary settle the
    /// disk: every un-synced data and namespace op is independently dropped,
    /// applied, or torn (see [`replay_file`]/[`replay_dir`]); inodes no
    /// directory survives pointing at are gone. Afterwards the state is fully
    /// durable, the fs is unpoisoned, the lock is released (advisory locks
    /// die with the process), and all previously open handles fail every
    /// operation. Deterministic: same seed and same history, same disk.
    pub fn crash(&self, seed: u64) {
        let mut inner = lock_inner(&self.inner);
        let mut rng = Rng::new(seed);
        let gran = inner.config.tear_granularity;
        // Every node is replayed — even ones about to become unreachable —
        // so rng consumption depends only on the operation history.
        let ids: Vec<Ino> = inner.nodes.keys().copied().collect();
        for ino in ids {
            match inner.nodes.get_mut(&ino) {
                Some(Node::File(f)) => replay_file(f, &mut rng, gran),
                Some(Node::Dir(d)) => replay_dir(d, &mut rng),
                None => unreachable!("no nodes are removed during replay"),
            }
        }
        // A file whose directory entry did not survive is simply gone, no
        // matter how thoroughly its data was fsynced.
        let mut reachable = BTreeSet::new();
        let mut stack = vec![ROOT];
        while let Some(ino) = stack.pop() {
            if !reachable.insert(ino) {
                continue;
            }
            if let Some(Node::Dir(d)) = inner.nodes.get(&ino) {
                stack.extend(d.visible.values().copied());
            }
        }
        inner.nodes.retain(|ino, _| reachable.contains(ino));
        inner.generation += 1;
        inner.lock_holder = None;
        inner.poisoned = false;
    }

    /// Every visible file as `path -> contents`, for assertions and failure
    /// dumps. Immediately after [`SimFs::crash`] this is exactly the durable
    /// state.
    pub fn dump(&self) -> BTreeMap<String, Vec<u8>> {
        let inner = lock_inner(&self.inner);
        let mut out = BTreeMap::new();
        let mut stack: Vec<(String, Ino)> = vec![(String::new(), ROOT)];
        while let Some((prefix, ino)) = stack.pop() {
            match &inner.nodes[&ino] {
                Node::Dir(d) => {
                    for (name, &child) in &d.visible {
                        let path = if prefix.is_empty() {
                            name.clone()
                        } else {
                            format!("{prefix}/{name}")
                        };
                        stack.push((path, child));
                    }
                }
                Node::File(f) => {
                    out.insert(prefix, f.visible.clone());
                }
            }
        }
        out
    }
}

/// An open simulated file. Stamped with the generation it was opened in;
/// after a crash the stamp no longer matches and every operation fails —
/// the handle belonged to the dead process.
pub struct SimFile {
    inner: Arc<Mutex<Inner>>,
    ino: Ino,
    generation: u64,
    writable: bool,
}

impl std::fmt::Debug for SimFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimFile")
            .field("ino", &self.ino)
            .field("generation", &self.generation)
            .field("writable", &self.writable)
            .finish()
    }
}

impl SimFile {
    fn check_live(&self, inner: &Inner) -> io::Result<()> {
        if self.generation != inner.generation {
            return Err(stale_handle_error());
        }
        Ok(())
    }
}

/// Guard for the simulated `LOCK` file. Dropping it releases the lock;
/// [`SimFs::crash`] also releases it (and a stale guard dropped afterwards
/// must not release a lock taken by the "new process" — the token check
/// prevents that).
pub struct SimLock {
    inner: Arc<Mutex<Inner>>,
    token: u64,
}

impl Drop for SimLock {
    fn drop(&mut self) {
        let mut inner = lock_inner(&self.inner);
        if inner.lock_holder == Some(self.token) {
            inner.lock_holder = None;
        }
    }
}

impl Fs for SimFs {
    type File = SimFile;
    type Lock = SimLock;

    fn lock(&self, path: &Path) -> io::Result<SimLock> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        if inner.lock_holder.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "LOCK is held by another opener (ADR 0017 second-opener refusal)",
            ));
        }
        // Create the lock file if absent, like RealFs's create(true) open.
        let (parent, name) = inner.resolve_parent(path)?;
        match self.nodes_entry(&inner, parent, &name) {
            Some(ino) => {
                inner.resolve_file(path).map(|_| ino)?;
            }
            None => {
                let ino = inner.alloc(Node::File(FileNode::default()));
                let dir = inner.dir_mut(parent);
                dir.visible.insert(name.clone(), ino);
                dir.journal.push(NsOp::Create { name, ino });
            }
        }
        let token = inner.next_lock_token;
        inner.next_lock_token += 1;
        inner.lock_holder = Some(token);
        Ok(SimLock { inner: Arc::clone(&self.inner), token })
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let names = path_names(path)?;
        let mut cur = ROOT;
        for name in names {
            let existing = match inner.nodes.get(&cur) {
                Some(Node::Dir(d)) => d.visible.get(&name).copied(),
                _ => unreachable!("walk only descends into directories"),
            };
            match existing {
                Some(child) => match inner.nodes.get(&child) {
                    Some(Node::Dir(_)) => cur = child,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("path component is a file: {}", path.display()),
                        ))
                    }
                },
                None => {
                    let child = inner.alloc(Node::Dir(DirNode::default()));
                    let dir = inner.dir_mut(cur);
                    dir.visible.insert(name.clone(), child);
                    dir.journal.push(NsOp::Create { name, ino: child });
                    cur = child;
                }
            }
        }
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let ino = inner.resolve_dir(path)?;
        match &inner.nodes[&ino] {
            // BTreeMap iteration is already sorted, as the trait requires.
            Node::Dir(d) => Ok(d.visible.keys().cloned().collect()),
            Node::File(_) => unreachable!("resolve_dir returned a directory"),
        }
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let names = path_names(path)?;
        Ok(inner.lookup(&names).is_some())
    }

    fn open_read(&self, path: &Path) -> io::Result<SimFile> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let ino = inner.resolve_file(path)?;
        Ok(SimFile {
            inner: Arc::clone(&self.inner),
            ino,
            generation: inner.generation,
            writable: false,
        })
    }

    fn open_append(&self, path: &Path) -> io::Result<SimFile> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let ino = inner.resolve_file(path)?;
        Ok(SimFile {
            inner: Arc::clone(&self.inner),
            ino,
            generation: inner.generation,
            writable: true,
        })
    }

    fn create_new(&self, path: &Path) -> io::Result<SimFile> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let (parent, name) = inner.resolve_parent(path)?;
        if self.nodes_entry(&inner, parent, &name).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("already exists: {}", path.display()),
            ));
        }
        let ino = inner.alloc(Node::File(FileNode::default()));
        let dir = inner.dir_mut(parent);
        dir.visible.insert(name.clone(), ino);
        dir.journal.push(NsOp::Create { name, ino });
        Ok(SimFile {
            inner: Arc::clone(&self.inner),
            ino,
            generation: inner.generation,
            writable: true,
        })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let (from_parent, from_name) = inner.resolve_parent(from)?;
        let (to_parent, to_name) = inner.resolve_parent(to)?;
        // The seam contract restricts renames to one directory (all ADR 0017
        // swaps are); the per-directory journal model depends on it.
        if from_parent != to_parent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename source and destination must share a parent directory",
            ));
        }
        let ino = self
            .nodes_entry(&inner, from_parent, &from_name)
            .ok_or_else(|| not_found(from))?;
        let dir = inner.dir_mut(from_parent);
        dir.visible.remove(&from_name);
        dir.visible.insert(to_name.clone(), ino);
        dir.journal.push(NsOp::Rename { from: from_name, to: to_name, ino });
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let (parent, name) = inner.resolve_parent(path)?;
        let ino = inner.resolve_file(path)?;
        let dir = inner.dir_mut(parent);
        dir.visible.remove(&name);
        dir.journal.push(NsOp::Remove { name, ino });
        // The inode stays in `nodes`: open handles keep working (POSIX
        // unlink semantics) and an un-synced remove can resurrect it.
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        let ino = inner.resolve_dir(path)?;
        let dir = inner.dir_mut(ino);
        // `visible` is `durable` plus the journal applied in order, so
        // promoting the journal is exactly adopting the visible table.
        dir.durable = dir.visible.clone();
        dir.journal.clear();
        Ok(())
    }
}

impl SimFs {
    /// The visible child `name` of directory `parent`, if any.
    fn nodes_entry(&self, inner: &Inner, parent: Ino, name: &str) -> Option<Ino> {
        match inner.nodes.get(&parent) {
            Some(Node::Dir(d)) => d.visible.get(name).copied(),
            _ => None,
        }
    }
}

impl FsFile for SimFile {
    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        self.check_live(&inner)?;
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file was opened read-only",
            ));
        }
        if data.is_empty() {
            return Ok(());
        }
        let file = inner.file_mut(self.ino);
        let offset = file.visible.len() as u64;
        file.journal.push(DataOp::Append { offset, bytes: data.to_vec() });
        file.visible.extend_from_slice(data);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        self.check_live(&inner)?;
        let file = inner.file(self.ino);
        if offset >= file.visible.len() as u64 {
            return Ok(0);
        }
        let start = to_usize(offset);
        let n = buf.len().min(file.visible.len() - start);
        buf[..n].copy_from_slice(&file.visible[start..start + n]);
        Ok(n)
    }

    fn len(&self) -> io::Result<u64> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        self.check_live(&inner)?;
        Ok(inner.file(self.ino).visible.len() as u64)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        self.check_live(&inner)?;
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file was opened read-only",
            ));
        }
        let file = inner.file_mut(self.ino);
        file.journal.push(DataOp::Truncate { len });
        file.visible.truncate(to_usize(len));
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut inner = lock_inner(&self.inner);
        inner.enter()?;
        self.check_live(&inner)?;
        let file = inner.file_mut(self.ino);
        file.durable = file.visible.clone();
        file.journal.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use coppice_consensus::fs::{read_to_vec, write_atomic, Fs, FsFile};

    use super::{is_sim_crash, SimConfig, SimFs};

    fn simfs() -> SimFs {
        SimFs::new(SimConfig::default())
    }

    const SEEDS: u64 = 256;

    #[test]
    fn visible_semantics_roundtrip() {
        let fs = simfs();
        fs.create_dir_all(Path::new("log")).unwrap();
        let mut f = fs.create_new(Path::new("log/1.seg")).unwrap();
        f.append(b"hello ").unwrap();
        f.append(b"world").unwrap();
        assert_eq!(f.len().unwrap(), 11);

        let mut buf = [0u8; 5];
        f.read_exact_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"world");

        // A second handle sees the same visible contents.
        assert_eq!(read_to_vec(&fs, Path::new("log/1.seg")).unwrap(), b"hello world");

        f.truncate(5).unwrap();
        assert_eq!(read_to_vec(&fs, Path::new("log/1.seg")).unwrap(), b"hello");

        assert!(fs.exists(Path::new("log")).unwrap());
        assert!(fs.exists(Path::new("")).unwrap());
        assert!(!fs.exists(Path::new("log/2.seg")).unwrap());
        assert!(fs.create_new(Path::new("log/1.seg")).is_err());
        fs.create_new(Path::new("log/0.seg")).unwrap();
        assert_eq!(fs.list_dir(Path::new("log")).unwrap(), vec!["0.seg", "1.seg"]);

        let mut ro = fs.open_read(Path::new("log/1.seg")).unwrap();
        assert!(ro.append(b"x").is_err());
    }

    #[test]
    fn paths_may_not_escape_the_root() {
        let fs = simfs();
        assert!(fs.exists(Path::new("../escape")).is_err());
        assert!(fs.exists(Path::new("/abs")).is_err());
        assert!(fs.create_new(Path::new("a/../b")).is_err());
    }

    #[test]
    fn fsynced_append_survives_every_seed() {
        for seed in 0..SEEDS {
            let fs = simfs();
            let mut f = fs.create_new(Path::new("f")).unwrap();
            f.append(b"hello").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("")).unwrap();
            fs.crash(seed);
            assert_eq!(fs.dump().get("f").unwrap(), b"hello", "seed {seed}");
        }
    }

    #[test]
    fn unsynced_append_can_drop_apply_or_tear() {
        let payload = vec![0xAB_u8; 8192];
        let (mut dropped, mut applied, mut torn_page, mut torn_odd) = (false, false, false, false);
        for seed in 0..SEEDS {
            let fs = simfs();
            let f = fs.create_new(Path::new("f")).unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("")).unwrap();
            let mut f = fs.open_append(Path::new("f")).unwrap();
            f.append(&payload).unwrap();
            drop(f);
            fs.crash(seed);
            let dump = fs.dump();
            let contents = dump.get("f").expect("durable create must survive");
            // Whatever survives is a strict prefix of the write, never garbage.
            assert_eq!(contents[..], payload[..contents.len()], "seed {seed}");
            match contents.len() {
                0 => dropped = true,
                8192 => applied = true,
                4096 => torn_page = true,
                _ => torn_odd = true,
            }
        }
        assert!(dropped, "no seed dropped the append");
        assert!(applied, "no seed applied the append");
        assert!(torn_page, "no seed tore at the 4096 boundary");
        assert!(torn_odd, "no seed tore at an arbitrary byte");
    }

    #[test]
    fn dropped_earlier_append_zero_fills_before_surviving_later_one() {
        let mut seen_gap = false;
        for seed in 0..SEEDS {
            let fs = simfs();
            let f = fs.create_new(Path::new("f")).unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("")).unwrap();
            let mut f = fs.open_append(Path::new("f")).unwrap();
            f.append(&[0x11; 4]).unwrap();
            f.append(&[0x22; 4]).unwrap();
            drop(f);
            fs.crash(seed);
            let dump = fs.dump();
            let contents = dump.get("f").unwrap();
            if contents.len() == 8 && contents[..4] == [0, 0, 0, 0] {
                assert_eq!(contents[4..], [0x22; 4], "seed {seed}");
                seen_gap = true;
            }
        }
        assert!(seen_gap, "no seed dropped the first append while keeping the second");
    }

    #[test]
    fn rename_is_visible_immediately_but_can_revert_without_dir_sync() {
        let setup = || {
            let fs = simfs();
            let mut f = fs.create_new(Path::new("dst")).unwrap();
            f.append(b"old").unwrap();
            f.sync_data().unwrap();
            let mut f = fs.create_new(Path::new("src")).unwrap();
            f.append(b"new").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("")).unwrap();
            fs.rename(Path::new("src"), Path::new("dst")).unwrap();
            fs
        };

        // Visible immediately.
        let fs = setup();
        assert!(!fs.exists(Path::new("src")).unwrap());
        assert_eq!(read_to_vec(&fs, Path::new("dst")).unwrap(), b"new");

        let (mut reverted, mut survived) = (false, false);
        for seed in 0..SEEDS {
            let fs = setup();
            fs.crash(seed);
            let dump = fs.dump();
            match (dump.get("src"), dump.get("dst")) {
                // Rename did not survive: destination holds the old file.
                (Some(src), Some(dst)) => {
                    assert_eq!(src, b"new", "seed {seed}");
                    assert_eq!(dst, b"old", "seed {seed}");
                    reverted = true;
                }
                // Rename survived atomically.
                (None, Some(dst)) => {
                    assert_eq!(dst, b"new", "seed {seed}");
                    survived = true;
                }
                other => panic!("seed {seed}: impossible post-crash state {other:?}"),
            }
        }
        assert!(reverted, "no seed reverted the rename");
        assert!(survived, "no seed kept the rename");
    }

    #[test]
    fn atomic_swap_survives_every_crash_point_and_seed() {
        let manifest = Path::new("manifest");
        let tmp = Path::new("manifest.tmp");
        let setup = || {
            let fs = simfs();
            write_atomic(&fs, manifest, tmp, b"v1").unwrap();
            fs
        };

        let mut crash_point = 0u64;
        loop {
            let mut completed = false;
            for seed in 0..16 {
                let fs = setup();
                fs.set_crash_at(fs.op_count() + crash_point);
                let result = write_atomic(&fs, manifest, tmp, b"v2");
                if let Err(e) = &result {
                    assert!(is_sim_crash(e), "point {crash_point}: unexpected error {e}");
                } else {
                    completed = true;
                }
                fs.crash(seed);
                let dump = fs.dump();
                let contents = dump.get("manifest").expect("manifest must never vanish");
                assert!(
                    contents == b"v1" || contents == b"v2",
                    "point {crash_point} seed {seed}: manifest torn: {contents:?}"
                );
                if result.is_ok() {
                    // write_atomic returned: the new contents are durable.
                    assert_eq!(contents, b"v2", "point {crash_point} seed {seed}");
                }
            }
            if completed {
                break;
            }
            crash_point += 1;
        }
        assert!(crash_point > 0, "crash injection never interrupted write_atomic");
    }

    #[test]
    fn fsynced_data_with_unsynced_create_can_vanish() {
        let (mut vanished, mut survived) = (false, false);
        for seed in 0..SEEDS {
            let fs = simfs();
            let mut f = fs.create_new(Path::new("orphan")).unwrap();
            f.append(b"payload").unwrap();
            f.sync_data().unwrap();
            // No sync_dir: the directory entry is not durable.
            fs.crash(seed);
            match fs.dump().get("orphan") {
                None => vanished = true,
                // When the entry survives, the fsynced data is never torn.
                Some(contents) => {
                    assert_eq!(contents, b"payload", "seed {seed}");
                    survived = true;
                }
            }
        }
        assert!(vanished, "no seed dropped the un-synced create");
        assert!(survived, "no seed kept the un-synced create");
    }

    #[test]
    fn unsynced_remove_can_resurrect_the_file() {
        let (mut resurrected, mut stayed_gone) = (false, false);
        for seed in 0..SEEDS {
            let fs = simfs();
            let mut f = fs.create_new(Path::new("f")).unwrap();
            f.append(b"back from the dead").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("")).unwrap();
            fs.remove_file(Path::new("f")).unwrap();
            assert!(!fs.exists(Path::new("f")).unwrap());
            fs.crash(seed);
            match fs.dump().get("f") {
                Some(contents) => {
                    assert_eq!(contents, b"back from the dead", "seed {seed}");
                    resurrected = true;
                }
                None => stayed_gone = true,
            }
        }
        assert!(resurrected, "no seed resurrected the un-synced remove");
        assert!(stayed_gone, "no seed made the remove stick");
    }

    #[test]
    fn crash_is_deterministic_for_a_seed_and_history() {
        let setup = || {
            let fs = simfs();
            fs.create_dir_all(Path::new("log")).unwrap();
            let mut a = fs.create_new(Path::new("log/1.seg")).unwrap();
            a.append(&[0x11; 5000]).unwrap();
            a.sync_data().unwrap();
            fs.sync_dir(Path::new("log")).unwrap();
            a.append(&[0x22; 9000]).unwrap();
            a.truncate(10_000).unwrap();
            let mut b = fs.create_new(Path::new("manifest.tmp")).unwrap();
            b.append(b"manifest v2").unwrap();
            b.sync_data().unwrap();
            fs.rename(Path::new("manifest.tmp"), Path::new("manifest")).unwrap();
            fs.create_new(Path::new("log/2.seg")).unwrap();
            fs.remove_file(Path::new("log/2.seg")).unwrap();
            fs
        };
        for seed in [0, 1, 42, 0xDEAD_BEEF] {
            let x = setup();
            x.crash(seed);
            let y = setup();
            y.crash(seed);
            assert_eq!(x.dump(), y.dump(), "seed {seed}");
        }
    }

    #[test]
    fn crash_point_injection_poisons_until_crash() {
        let fs = simfs();
        fs.set_crash_at(2);
        fs.create_new(Path::new("a")).unwrap(); // op 0
        assert!(fs.exists(Path::new("a")).unwrap()); // op 1
        let err = fs.create_new(Path::new("b")).unwrap_err(); // op 2: armed
        assert!(is_sim_crash(&err));
        assert!(fs.is_poisoned());
        // The armed op did not execute.
        assert!(!fs.dump().contains_key("b"));
        // Everything fails until the crash is taken, reads included.
        let err = fs.exists(Path::new("a")).unwrap_err();
        assert!(is_sim_crash(&err));
        let err = fs.list_dir(Path::new("")).unwrap_err();
        assert!(is_sim_crash(&err));
        fs.crash(7);
        assert!(!fs.is_poisoned());
        fs.exists(Path::new("a")).unwrap();

        // Disarming before the point is reached prevents the crash.
        let fs = simfs();
        fs.set_crash_at(1);
        fs.disarm();
        fs.create_new(Path::new("a")).unwrap();
        fs.exists(Path::new("a")).unwrap();
    }

    #[test]
    fn stale_handles_fail_after_crash() {
        let fs = simfs();
        let mut f = fs.create_new(Path::new("f")).unwrap();
        f.append(b"x").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("")).unwrap();
        fs.crash(0);
        // The old handle belonged to the dead process.
        for err in [
            f.append(b"y").unwrap_err(),
            f.sync_data().unwrap_err(),
            f.truncate(0).unwrap_err(),
            f.read_at(0, &mut [0u8; 1]).unwrap_err(),
            f.len().unwrap_err(),
        ] {
            assert!(!is_sim_crash(&err), "stale-handle failure is not a crash point");
        }
        // A fresh handle works and sees the durable contents.
        assert_eq!(read_to_vec(&fs, Path::new("f")).unwrap(), b"x");
    }

    #[test]
    fn lock_refusal_and_release_on_crash() {
        let fs = simfs();
        let lock_path = Path::new("LOCK");
        let stale = fs.lock(lock_path).unwrap();
        assert!(fs.lock(lock_path).is_err(), "second opener must be refused");
        fs.crash(0);
        // The dead process's lock is released; the "new process" takes it.
        let fresh = fs.lock(lock_path).unwrap();
        // Dropping the stale guard must not release the fresh lock.
        drop(stale);
        assert!(fs.lock(lock_path).is_err());
        drop(fresh);
        fs.lock(lock_path).unwrap();
    }
}
