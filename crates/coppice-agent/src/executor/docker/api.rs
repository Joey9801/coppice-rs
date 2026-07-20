//! Thin bollard convenience layer (docker-executor.md §2).
//!
//! Deliberately **not** a mock seam: correctness testing stays on
//! [`crate::executor::FakeExecutor`] above the trait plus the gated real-Docker
//! integration suite. This module only owns connection setup, the couple of
//! daemon queries `run_daemon` needs before the executor exists, and small
//! error-shape helpers the rest of the module shares. Everything lifecycle-
//! shaped lives in `lifecycle.rs`/`events.rs`.

use std::path::PathBuf;

use bollard::Docker;

use crate::executor::ExecutorError;

/// bollard's default per-request timeout, in seconds. bollard keeps its own
/// `DEFAULT_TIMEOUT` private, so it is pinned here to match the library default
/// rather than inventing a new one.
const DOCKER_TIMEOUT: u64 = 120;

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
