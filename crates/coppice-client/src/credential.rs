//! The credential a client sends, and the redaction that keeps it out of
//! everything that prints.
//!
//! A bearer token is the one value in this crate that must never reach a log
//! line, a panic message, or a `{:?}` of the client holding it. [`Client`] and
//! [`ClientBuilder`] are both `Debug` — they are handed to `tracing`, embedded
//! in other structs, and printed in tests — so the token cannot be a bare
//! `String` sitting in a `#[derive(Debug)]` struct.
//!
//! [`BearerToken`] is that guard: a newtype whose `Debug` prints
//! `BearerToken(<redacted>)` and nothing else, with no `Display` and no
//! `Serialize` to route around it, and a single deliberately-named
//! [`expose`](BearerToken::expose) accessor at the one place the value is
//! actually needed — building the `Authorization` header, which is then marked
//! sensitive so `reqwest`'s own `Debug` redacts it too.
//!
//! [`Client`]: crate::Client
//! [`ClientBuilder`]: crate::ClientBuilder

use std::fmt;

/// A bearer token, held so that nothing prints it by accident.
///
/// Construct one with [`BearerToken::new`], which carries this crate's
/// rule about empty credentials: a token is trimmed, and one that is empty or
/// whitespace-only is *no token at all* rather than an empty one, because an
/// environment variable that is set but empty must behave exactly like one
/// that is unset.
///
/// ```
/// use coppice_client::BearerToken;
///
/// let token = BearerToken::new("  s3cr3t  ").expect("a non-empty token");
/// assert_eq!(token.expose(), "s3cr3t");
/// assert_eq!(format!("{token:?}"), "BearerToken(<redacted>)");
/// assert_eq!(BearerToken::new("   "), None);
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
    /// The token in `raw`, trimmed — or `None` when what is left is empty.
    pub fn new(raw: impl Into<String>) -> Option<BearerToken> {
        let raw = raw.into();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(BearerToken(trimmed.to_string()))
        }
    }

    /// The token itself.
    ///
    /// Named for what it does rather than `as_str`, so that a call site that
    /// takes the value out of its wrapper reads as the deliberate act it is.
    /// The only caller inside this crate is the one that builds the
    /// `Authorization` header.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerToken {
    /// Never the value, and never its length either: how long a credential is
    /// is itself a fact about it that a log line has no business carrying.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_or_whitespace_token_is_no_token() {
        assert_eq!(BearerToken::new(""), None);
        assert_eq!(BearerToken::new("   \t\n"), None);
    }

    #[test]
    fn a_token_is_trimmed_but_otherwise_verbatim() {
        let token = BearerToken::new(" abc.def ").unwrap();
        assert_eq!(token.expose(), "abc.def");
    }

    /// The whole point of the type: no formatting of it reveals the value.
    #[test]
    fn debug_never_reveals_the_token() {
        let token = BearerToken::new("s3cr3t").unwrap();
        let rendered = format!("{token:?}");
        assert_eq!(rendered, "BearerToken(<redacted>)");
        assert!(!rendered.contains("s3cr3t"));
        // Nor the length, which a naive redaction often leaks.
        assert!(!rendered.contains('6'));
    }
}
