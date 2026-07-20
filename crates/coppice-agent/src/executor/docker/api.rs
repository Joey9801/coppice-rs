//! Thin bollard convenience layer (docker-executor.md §2).
//!
//! Deliberately **not** a mock seam: correctness testing stays on
//! [`crate::executor::FakeExecutor`] above the trait plus the gated real-Docker
//! integration suite. This module only owns connection setup, the couple of
//! daemon queries `run_daemon` needs before the executor exists, and small
//! error-shape helpers the rest of the module shares. Everything lifecycle-
//! shaped lives in `lifecycle.rs`/`events.rs`.

use std::path::{Path, PathBuf};

use bollard::Docker;

use crate::executor::ExecutorError;

/// bollard's default per-request timeout, in seconds. bollard keeps its own
/// `DEFAULT_TIMEOUT` private, so it is pinned here to match the library default
/// rather than inventing a new one.
const DOCKER_TIMEOUT: u64 = 120;

/// Resolve the Docker daemon endpoint the executor should dial, filling in a
/// sensible default when `[executor] docker_host` is left unset.
///
/// Precedence, highest first:
///  1. `configured` — the explicit `[executor] docker_host` value, verbatim.
///  2. the `DOCKER_HOST` environment variable when set and non-empty, verbatim.
///  3. a probe of well-known local Unix sockets, yielding `unix://<path>` for
///     the first candidate that exists **and** is a socket.
///  4. otherwise an error naming every probed path.
///
/// The default socket (`/var/run/docker.sock`) is absent on macOS under Colima
/// (`~/.colima/default/docker.sock`) or Docker Desktop without the privileged
/// symlink (`~/.docker/run/docker.sock`), so probing the well-known locations
/// lets the agent connect wherever the `docker` CLI already does.
///
/// Docker CLI *context* files (`~/.docker/contexts/…`) are deliberately **not**
/// consulted — parsing them is out of scope. An operator on a non-default
/// context can export `DOCKER_HOST` or set `[executor] docker_host` instead.
pub fn resolve_host(configured: Option<&str>) -> Result<String, ExecutorError> {
    let env_docker_host = std::env::var("DOCKER_HOST").ok();
    resolve_host_core(
        configured,
        env_docker_host.as_deref(),
        &default_candidates(),
    )
}

/// The well-known local socket paths probed by [`resolve_host`], in precedence
/// order. Entries gated on an env var are skipped when it is unset.
fn default_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![PathBuf::from("/var/run/docker.sock")];
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        // Docker Desktop for macOS (unprivileged install), then Colima, then
        // Rancher Desktop.
        candidates.push(home.join(".docker/run/docker.sock"));
        candidates.push(home.join(".colima/default/docker.sock"));
        candidates.push(home.join(".rd/docker.sock"));
    }
    // Rootless dockerd publishes its socket under the user runtime dir.
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        candidates.push(PathBuf::from(runtime).join("docker.sock"));
    }
    candidates
}

/// The pure core of [`resolve_host`]: precedence over explicit inputs so the
/// env and candidate list can be supplied by a test rather than the process
/// environment.
fn resolve_host_core(
    configured: Option<&str>,
    env_docker_host: Option<&str>,
    candidates: &[PathBuf],
) -> Result<String, ExecutorError> {
    if let Some(host) = configured {
        tracing::info!(
            docker_host = host,
            source = "config",
            "resolved Docker endpoint"
        );
        return Ok(host.to_string());
    }
    // An empty `DOCKER_HOST` is treated as unset (as the docker CLI does).
    if let Some(host) = env_docker_host.filter(|value| !value.is_empty()) {
        tracing::info!(
            docker_host = host,
            source = "env",
            "resolved Docker endpoint"
        );
        return Ok(host.to_string());
    }
    for path in candidates {
        if is_socket(path) {
            let host = format!("unix://{}", path.display());
            tracing::info!(docker_host = %host, source = "probe", "resolved Docker endpoint");
            return Ok(host);
        }
    }
    let probed = candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(ExecutorError::Other(format!(
        "no reachable Docker daemon: probed {probed}; set DOCKER_HOST or \
         [executor] docker_host to the daemon endpoint"
    )))
}

/// Whether `path` exists and is a Unix socket. Follows symlinks (Docker
/// Desktop's socket is often a link to the real one), so a link to a socket
/// counts; a plain file or missing path does not.
fn is_socket(path: &Path) -> bool {
    use std::os::unix::fs::FileTypeExt;
    std::fs::metadata(path)
        .map(|meta| meta.file_type().is_socket())
        .unwrap_or(false)
}

/// Connect to the Docker daemon named by `docker_host`.
///
/// `unix://…` dials a local socket; `tcp://…`/`http://…` a remote daemon over
/// **plaintext** HTTP (bollard's `connect_with_http` is the unsecured
/// transport). `https://` is rejected outright rather than silently downgraded:
/// TLS to a daemon needs bollard's `ssl` feature plus client-certificate
/// configuration that the agent does not carry yet — advertising it would
/// connect in the clear while claiming otherwise. Anything else is a clear
/// configuration error (we never guess a scheme). Uses bollard's default
/// timeout and negotiated API version.
///
/// Public (not `pub(crate)`) so both `run_daemon` and `coppice dev` can build a
/// client before constructing a [`super::DockerExecutor`] with
/// [`super::DockerExecutor::new`]; the integration harness holds its own client
/// the same way.
pub fn connect(docker_host: &str) -> Result<Docker, ExecutorError> {
    let result = if docker_host.starts_with("unix://") {
        Docker::connect_with_unix(docker_host, DOCKER_TIMEOUT, bollard::API_DEFAULT_VERSION)
    } else if docker_host.starts_with("tcp://") || docker_host.starts_with("http://") {
        Docker::connect_with_http(docker_host, DOCKER_TIMEOUT, bollard::API_DEFAULT_VERSION)
    } else if docker_host.starts_with("https://") {
        return Err(ExecutorError::Other(format!(
            "docker_host {docker_host:?}: TLS to the daemon is not supported yet \
             (needs bollard's ssl feature + client certificates); use unix:// for a \
             local daemon or tcp:// for an explicitly-plaintext remote one"
        )));
    } else {
        return Err(ExecutorError::Other(format!(
            "unsupported docker_host scheme in {docker_host:?}: expected unix:// or \
             tcp:// (plaintext)"
        )));
    };
    result.map_err(|err| {
        ExecutorError::Other(format!("connecting to Docker at {docker_host:?}: {err}"))
    })
}

/// Probe the daemon and return its data-root directory (`docker info` →
/// `DockerRootDir`) as a *local* path for the disk-pressure monitor (§9) —
/// `None` unless `docker_host` names a local Unix-socket daemon.
///
/// This call is double-duty and the `info()` is unconditional on purpose: it is
/// startup's **reachability probe**. `connect` builds the client lazily (for
/// `tcp://`, no connection is attempted at all), so skipping `info()` here
/// would let an agent with an unreachable remote daemon sail through startup,
/// register capacity, and accept jobs it can never run — callers rely on this
/// `Err` to fail fast instead.
///
/// The path gate matters separately: `DockerRootDir` is a path in the
/// **daemon's** filesystem namespace. For a `tcp://` daemon that path is not
/// ours — and if the same string happens to exist on the agent host, statvfs
/// would sample an unrelated local filesystem and could wrongly refuse (or
/// wrongly permit) starts. A daemon reached via `unix://` is the one case worth
/// sampling; remote daemons need daemon-side pressure reporting, which v1 does
/// not have — for them the monitor covers `data_dir` only. (A VM-backed local
/// socket, e.g. Docker Desktop, still yields a non-existent local path;
/// `pressure.rs` skips-and-warns on it.)
pub async fn data_root(
    docker: &Docker,
    docker_host: &str,
) -> Result<Option<PathBuf>, ExecutorError> {
    let info = docker
        .info()
        .await
        .map_err(|err| ExecutorError::Other(format!("querying docker info: {err}")))?;
    // Log collection and §8.2 restart-resume need the `local`/`json-file` driver;
    // warn (never fail) if the daemon defaults to anything else, so telemetry is
    // best-effort rather than silently empty.
    if let Some(driver) = info.logging_driver.as_deref() {
        if !matches!(driver, "json-file" | "local") {
            tracing::warn!(
                driver,
                "Docker log driver is not local/json-file; log collection and §8.2 resume need those drivers"
            );
        }
    }
    if !docker_host.starts_with("unix://") {
        tracing::debug!(
            docker_host,
            "remote daemon reachable; its data-root is not a local path, so the \
             pressure monitor will cover data_dir only"
        );
        return Ok(None);
    }
    Ok(info
        .docker_root_dir
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from))
}

/// The HTTP status code carried by a bollard error, when it is a server-side
/// response error. The lifecycle layer matches on 404/409 (304 never surfaces
/// here — bollard treats it as success, see `lifecycle::stop`).
pub(crate) fn status_code(err: &bollard::errors::Error) -> Option<u16> {
    match err {
        bollard::errors::Error::DockerResponseServerError { status_code, .. } => Some(*status_code),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;

    use super::*;

    /// A real bound Unix socket in `dir`; the returned listener must be kept
    /// alive for the socket file to stay present.
    fn bind(dir: &Path, name: &str) -> (PathBuf, UnixListener) {
        let path = dir.join(name);
        let listener = UnixListener::bind(&path).expect("bind unix socket");
        (path, listener)
    }

    #[test]
    fn configured_wins_over_everything() {
        let dir = tempfile::tempdir().unwrap();
        let (sock, _listener) = bind(dir.path(), "docker.sock");
        let host = resolve_host_core(
            Some("tcp://explicit:2375"),
            Some("unix:///env/docker.sock"),
            std::slice::from_ref(&sock),
        )
        .unwrap();
        assert_eq!(host, "tcp://explicit:2375");
    }

    #[test]
    fn env_wins_over_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let (sock, _listener) = bind(dir.path(), "docker.sock");
        let host = resolve_host_core(
            None,
            Some("tcp://from-env:2375"),
            std::slice::from_ref(&sock),
        )
        .unwrap();
        assert_eq!(host, "tcp://from-env:2375");
    }

    #[test]
    fn empty_env_is_unset() {
        let dir = tempfile::tempdir().unwrap();
        let (sock, _listener) = bind(dir.path(), "docker.sock");
        let host = resolve_host_core(None, Some(""), std::slice::from_ref(&sock)).unwrap();
        assert_eq!(host, format!("unix://{}", sock.display()));
    }

    #[test]
    fn first_existing_socket_wins() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.sock");
        let (first, _f) = bind(dir.path(), "first.sock");
        let (second, _s) = bind(dir.path(), "second.sock");
        let host = resolve_host_core(None, None, &[missing, first.clone(), second]).unwrap();
        assert_eq!(host, format!("unix://{}", first.display()));
    }

    #[test]
    fn plain_file_is_not_a_socket() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("docker.sock");
        std::fs::write(&file, b"not a socket").unwrap();
        let err = resolve_host_core(None, None, std::slice::from_ref(&file)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&file.display().to_string()), "message: {msg}");
    }

    #[test]
    fn no_match_errors_with_every_probed_path() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.sock");
        let b = dir.path().join("b.sock");
        let err = resolve_host_core(None, None, &[a.clone(), b.clone()]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&a.display().to_string()), "message: {msg}");
        assert!(msg.contains(&b.display().to_string()), "message: {msg}");
    }
}
