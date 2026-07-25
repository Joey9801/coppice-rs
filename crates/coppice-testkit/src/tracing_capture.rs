//! Tracing capture for assertions like "secrets never reach the logs".
//!
//! Enrollment tokens (`cpk_...`, ADR 0037) must never be written to a log
//! line, and the only way to prove that is to run real code under a real
//! `tracing` subscriber and inspect what it emitted. [`capture`] installs a
//! [`tracing_subscriber::fmt`] subscriber over an in-memory buffer for the
//! duration of a closure and hands back both the closure's result and the
//! captured text; [`assert_no_secret`] then checks that text for a forbidden
//! needle.
//!
//! The buffer captures everything from `TRACE` up — callers asserting a
//! secret's absence need to know it wasn't logged at *any* level, not just
//! whatever level production configures.

use std::io;
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::MakeWriter;

/// An in-memory sink for a [`tracing_subscriber::fmt`] subscriber.
///
/// Cloning shares the underlying buffer, which is what lets [`capture`] hold
/// on to a handle after installing the subscriber's own clone as its writer.
#[derive(Debug, Clone, Default)]
pub struct CaptureBuffer {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl CaptureBuffer {
    pub fn new() -> CaptureBuffer {
        CaptureBuffer::default()
    }

    /// The bytes written so far, decoded lossily (`tracing_subscriber`
    /// output is always UTF-8 in practice, but this never panics if it
    /// somehow isn't).
    pub fn contents(&self) -> String {
        let buf = self.buf.lock().expect("capture buffer poisoned");
        String::from_utf8_lossy(&buf).into_owned()
    }
}

impl io::Write for CaptureBuffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf
            .lock()
            .expect("capture buffer poisoned")
            .extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureBuffer {
    type Writer = CaptureBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `f` under a `tracing` subscriber that writes every `TRACE`-and-up
/// event to an in-memory buffer, and return `f`'s result alongside the
/// captured text.
///
/// Uses [`tracing::subscriber::with_default`], so the subscriber is scoped
/// to `f` and does not leak into the rest of the test process — safe to call
/// from multiple tests without a global-subscriber race.
pub fn capture<T>(f: impl FnOnce() -> T) -> (T, String) {
    let buffer = CaptureBuffer::new();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let result = tracing::subscriber::with_default(subscriber, f);
    (result, buffer.contents())
}

/// Panic if `needle` appears anywhere in `captured`.
///
/// Intended for asserting that a secret — e.g. an enrollment token's
/// `cpk_`-prefixed material — never reached the logs. The panic message
/// names the needle and the (0-based) line it was found on, but not the
/// surrounding line text, so a passing test's failure output doesn't itself
/// become a place the secret leaked.
pub fn assert_no_secret(captured: &str, needle: &str) {
    if let Some(line_index) = captured.lines().position(|line| line.contains(needle)) {
        panic!("captured tracing output contains forbidden needle {needle:?} on line {line_index}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_records_events_and_hides_secrets_from_assertion() {
        let (_, captured) = capture(|| {
            tracing::info!("hello cpk_secret");
        });

        assert!(captured.contains("hello"));

        let result = std::panic::catch_unwind(|| assert_no_secret(&captured, "cpk_"));
        assert!(
            result.is_err(),
            "assert_no_secret should panic when the needle is present"
        );

        // An absent needle must not panic.
        assert_no_secret(&captured, "cpk_this_needle_is_not_present");
    }
}
