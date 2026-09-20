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
//! The other half of this module is *where* a token comes from. A static one
//! covers a CLI or a short-lived tool, whose process does not outlive its
//! credential. A long-running one does outlive it, so it supplies a
//! [`TokenProvider`] instead and the client asks for a token immediately
//! before every request.
//!
//! [`Client`]: crate::Client
//! [`ClientBuilder`]: crate::ClientBuilder

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Error;

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

/// Whatever a [`TokenProvider`] failed with.
///
/// Deliberately opaque: obtaining a credential is the caller's business —
/// an HTTP round trip to an identity provider, a file read, a keychain
/// lookup — and this crate has no vocabulary for any of it. The error
/// arrives at the caller again as [`Error::Credential`]'s source, unchanged.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Supplies the bearer token for one request.
///
/// A process that runs longer than its credential does cannot use
/// [`ClientBuilder::token`](crate::ClientBuilder::token): the token it was
/// built with expires, and every later request fails with a `401` the client
/// has no way to recover from. Such a process implements this instead, and
/// [`ClientBuilder::token_provider`](crate::ClientBuilder::token_provider)
/// hands it over.
///
/// **The client calls this once per request, immediately before sending, and
/// caches nothing.** Refresh, caching, and single-flight locking (so that ten
/// concurrent requests do not each mint a token) are all the provider's
/// business — which is the only place they can be, since only the provider
/// knows what a token costs and how long it lasts. Returning `Ok(None)` sends
/// no `Authorization` header at all, exactly as a client with no token does;
/// returning `Err` fails the request with [`Error::Credential`] before
/// anything is put on the wire.
///
/// The method returns a boxed future rather than being an `async fn` so the
/// trait stays object-safe without an `async-trait` dependency — this crate
/// publishes on its own and keeps its dependency list short.
///
/// ```
/// use std::future::Future;
/// use std::pin::Pin;
/// use std::sync::Mutex;
/// use std::time::{Duration, Instant};
///
/// use coppice_client::{BearerToken, BoxError, TokenProvider};
///
/// /// Mints a token, then serves it from memory until it is nearly expired.
/// struct Refreshing {
///     cached: Mutex<Option<(BearerToken, Instant)>>,
/// }
///
/// impl Refreshing {
///     /// Where a real implementation talks to its identity provider.
///     fn mint(&self) -> Result<(BearerToken, Instant), BoxError> {
///         let token = BearerToken::new("a-fresh-access-token")
///             .ok_or("the token endpoint returned an empty token")?;
///         Ok((token, Instant::now() + Duration::from_secs(300)))
///     }
/// }
///
/// impl TokenProvider for Refreshing {
///     fn token(
///         &self,
///     ) -> Pin<Box<dyn Future<Output = Result<Option<BearerToken>, BoxError>> + Send + '_>> {
///         Box::pin(async move {
///             // Held across no await point: minting is what would need the
///             // lock released, and a real one would use an async mutex.
///             let mut cached = self.cached.lock().expect("the cache lock is never poisoned");
///             if let Some((token, expiry)) = &*cached {
///                 if *expiry > Instant::now() {
///                     return Ok(Some(token.clone()));
///                 }
///             }
///             let (token, expiry) = self.mint()?;
///             *cached = Some((token.clone(), expiry));
///             Ok(Some(token))
///         })
///     }
/// }
/// ```
pub trait TokenProvider: Send + Sync + 'static {
    /// The token for the request about to be sent, or `None` to send none.
    fn token(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BearerToken>, BoxError>> + Send + '_>>;
}

/// Where a [`Client`](crate::Client)'s token comes from, if anywhere.
///
/// Private, and carried by both the builder and the built client, so the two
/// cannot disagree about the posture. Its `Debug` is written out rather than
/// derived so that neither the token nor anything about a provider escapes
/// through a client's own `Debug`.
#[derive(Clone, Default)]
pub(crate) enum Credential {
    /// No credential: no `Authorization` header is sent at all.
    #[default]
    None,
    /// One token, fixed when the client was built.
    Static(BearerToken),
    /// Asked for a token before every request.
    Provider(Arc<dyn TokenProvider>),
}

impl Credential {
    /// Whether this client sends a credential at all — true for a provider
    /// even before it has ever been asked, since whether it answers with a
    /// token is a per-request question.
    pub(crate) fn is_some(&self) -> bool {
        !matches!(self, Credential::None)
    }

    /// The token to attach to the request about to be sent.
    pub(crate) async fn resolve(&self) -> crate::Result<Option<BearerToken>> {
        match self {
            Credential::None => Ok(None),
            Credential::Static(token) => Ok(Some(token.clone())),
            Credential::Provider(provider) => provider.token().await.map_err(Error::Credential),
        }
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Credential::None => f.write_str("None"),
            // `BearerToken` redacts itself; naming it here keeps the shape
            // legible without the derive that would print the field.
            Credential::Static(token) => write!(f, "Static({token:?})"),
            Credential::Provider(_) => f.write_str("Provider(..)"),
        }
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

    /// A provider that answers with whatever it was last told to.
    struct Canned(std::sync::Mutex<std::result::Result<Option<BearerToken>, String>>);

    impl TokenProvider for Canned {
        fn token(
            &self,
        ) -> Pin<
            Box<
                dyn Future<Output = std::result::Result<Option<BearerToken>, BoxError>> + Send + '_,
            >,
        > {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .clone()
                    .map_err(|e| -> BoxError { e.into() })
            })
        }
    }

    #[tokio::test]
    async fn a_credential_resolves_to_what_its_source_says() {
        assert_eq!(Credential::None.resolve().await.unwrap(), None);

        let token = BearerToken::new("t").unwrap();
        let fixed = Credential::Static(token.clone());
        assert_eq!(fixed.resolve().await.unwrap(), Some(token.clone()));

        let canned = Arc::new(Canned(std::sync::Mutex::new(Ok(Some(token.clone())))));
        let provider = Credential::Provider(canned.clone());
        assert_eq!(provider.resolve().await.unwrap(), Some(token));

        *canned.0.lock().unwrap() = Ok(None);
        assert_eq!(provider.resolve().await.unwrap(), None);

        *canned.0.lock().unwrap() = Err("the token endpoint is down".to_string());
        let err = provider.resolve().await.expect_err("the provider failed");
        assert!(matches!(err, Error::Credential(_)), "{err:?}");
    }

    /// The enum is what a client's derived `Debug` reaches, so it redacts on
    /// its own account.
    #[test]
    fn debug_of_a_credential_reveals_neither_token_nor_provider() {
        assert_eq!(format!("{:?}", Credential::None), "None");
        assert_eq!(
            format!(
                "{:?}",
                Credential::Static(BearerToken::new("s3cr3t").unwrap())
            ),
            "Static(BearerToken(<redacted>))"
        );
        let provider = Credential::Provider(Arc::new(Canned(std::sync::Mutex::new(Ok(None)))));
        assert_eq!(format!("{provider:?}"), "Provider(..)");
    }
}
