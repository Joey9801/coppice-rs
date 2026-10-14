//! The HTTP client every AWS SDK client in the workspace uses.
//!
//! The SDK crates are built without their default HTTPS client (rustls on
//! aws-lc-rs), so nothing is wired in implicitly: every SDK client and every
//! IMDS client must be handed this one, or its first request fails with "no
//! HTTP client". It is the SDK's own hyper-1 client on rustls with the ring
//! provider, trusting the platform's native roots.

use aws_config::provider_config::ProviderConfig;
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use aws_smithy_http_client::Builder;
use aws_smithy_runtime_api::client::http::SharedHttpClient;

/// A fresh SDK HTTP client (its own connection pool) on the ring provider.
pub fn http_client() -> SharedHttpClient {
    Builder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
        .build_https()
}

/// A [`ProviderConfig`] carrying [`http_client`], for clients configured
/// through one (the IMDS client's `configure`).
pub fn provider_config() -> ProviderConfig {
    ProviderConfig::default().with_http_client(http_client())
}
