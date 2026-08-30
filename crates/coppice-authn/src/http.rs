//! The outbound HTTP client used to reach the IdP.

use std::time::Duration;

/// Every IdP request is a small JSON GET on a healthy path and a hang on an
/// unhealthy one. Ten seconds is long enough for a slow TLS handshake across a
/// region and short enough that a wedged IdP cannot pin the refresh task.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The client the JWKS cache should use unless the caller has one of its own.
///
/// The posture matches [`coppice_enroll::client::EnrollClient`] and the
/// coordinator's `PublicEdge`:
///
/// - **redirects disabled.** Discovery and JWKS URLs come from a document the
///   IdP controls; following a 3xx to a host of the response's choosing is how
///   a compromised or misconfigured IdP would point key fetches somewhere else.
///   A redirect surfaces as a non-2xx status and becomes a fetch failure, which
///   is exactly the visible outcome we want.
/// - **rustls with the built-in roots.** An IdP is a public-internet service
///   fronted by a public CA; there is no cluster-CA relationship here.
/// - **a bounded timeout**, so a dark IdP costs a backoff cycle, not a task.
///
/// # Panics
///
/// Only if rustls fails to initialise, which is a broken build rather than a
/// runtime condition — every other caller of `reqwest::Client::builder` in the
/// workspace treats it the same way.
pub fn default_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .use_rustls_tls()
        .tls_built_in_root_certs(true)
        .build()
        .expect("build the OIDC HTTP client")
}
