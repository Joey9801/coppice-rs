//! Minimal `sd_notify(3)` client for `Type=notify` units (ADR 0037 §7).
//!
//! The systemd unit runs `Type=notify`, so the daemon signals `READY=1` once
//! its listeners are serving — unit ordering (and any `After=`/`Requires=` on
//! the coordinator) then works, while *cluster* and *node* readiness stay
//! `/readyz`'s job (a parked daemon is `READY=1`, phase `waiting`, HTTP 503:
//! visible, alive, deliberately not "ready"). It also emits `STOPPING=1` when
//! shutdown begins, so systemd knows the exit is intentional.
//!
//! The protocol is one newline-free datagram (`READY=1`, `STOPPING=1`) to the
//! `AF_UNIX` socket named by `$NOTIFY_SOCKET`. It is hand-rolled here — a few
//! lines, no dependency — over `std`'s Unix datagram support, which since 1.70
//! covers both the filesystem-path and the abstract-namespace (`@`-prefixed)
//! forms systemd uses. When `$NOTIFY_SOCKET` is unset (every non-systemd
//! launch: dev, tests, a bare `./coppice coordinator`) this is a silent no-op,
//! and on any non-Linux target it compiles to nothing.

/// Signal `READY=1`: the daemon's listeners are up and serving.
pub fn notify_ready() {
    send("READY=1");
}

/// Signal `STOPPING=1`: shutdown has begun, so systemd expects the exit.
pub fn notify_stopping() {
    send("STOPPING=1");
}

/// Send one sd_notify datagram to `$NOTIFY_SOCKET`, or do nothing when it is
/// unset or the send fails (notification is strictly best-effort — a failed
/// notify must never affect the daemon).
#[cfg(target_os = "linux")]
fn send(message: &str) {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixDatagram;

    let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let path = socket.as_bytes();
    if path.is_empty() {
        return;
    }
    let Ok(sock) = UnixDatagram::unbound() else {
        return;
    };

    if path[0] == b'@' {
        // Abstract-namespace socket: the leading '@' stands for a NUL byte.
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::SocketAddr;
        if let Ok(addr) = SocketAddr::from_abstract_name(&path[1..]) {
            let _ = sock.send_to_addr(message.as_bytes(), &addr);
        }
    } else {
        let _ = sock.send_to(message.as_bytes(), socket);
    }
}

/// Non-Linux targets have no systemd; the notify calls compile to nothing.
#[cfg(not(target_os = "linux"))]
fn send(_message: &str) {}
