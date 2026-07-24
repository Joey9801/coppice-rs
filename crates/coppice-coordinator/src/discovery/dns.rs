//! The `dns` discovery backend (ADR 0037 §2): resolve one configured name per
//! consultation and treat every distinct resolved address as a candidate.
//!
//! Both record shapes are honored: **SRV** records carry their own target and
//! port, while **A/AAAA** records carry only an address and take the configured
//! fallback `port`. TTL staleness is tolerable because discovery only seeds
//! dialing (ADR 0037 §2). Resolution failures degrade to an empty/partial list
//! with a warning — SRV being absent is normal and logged only at debug.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use anyhow::Result;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;

use super::Discovery;

/// Resolves a single DNS name (A/AAAA + SRV) into candidate raft addresses on
/// every [`candidates`](Discovery::candidates) call.
pub(crate) struct DnsDiscovery {
    /// The name resolved on each consultation.
    name: String,
    /// Fallback port applied to A/AAAA records (SRV records carry their own).
    port: u16,
    /// System-configured async resolver, built once at construction.
    resolver: TokioAsyncResolver,
}

impl DnsDiscovery {
    /// Build the backend, initializing the async resolver from system config
    /// (`/etc/resolv.conf` and equivalents). Falls back to a default resolver
    /// configuration if the system configuration cannot be read, so a missing
    /// `resolv.conf` degrades rather than fails at startup.
    pub(crate) fn new(name: String, port: u16) -> Result<Self> {
        let resolver = match TokioAsyncResolver::tokio_from_system_conf() {
            Ok(resolver) => resolver,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "dns discovery: reading system resolver config failed; \
                     falling back to default resolver configuration"
                );
                TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
            }
        };
        // Reject an empty name early; the resolver would otherwise error on
        // every consultation, which discovery would silently swallow.
        anyhow::ensure!(
            !name.trim().is_empty(),
            "dns discovery name must not be empty"
        );
        Ok(DnsDiscovery {
            name,
            port,
            resolver,
        })
    }

    /// SRV lookups: `target:port` per record. Absent SRV records are normal
    /// (a plain A/AAAA deployment), so lookup failure is logged at debug and
    /// contributes nothing rather than warning.
    async fn srv_candidates(&self) -> Vec<String> {
        match self.resolver.srv_lookup(&self.name).await {
            Ok(lookup) => lookup
                .iter()
                .map(|srv| {
                    let target = srv.target().to_utf8();
                    let host = target.strip_suffix('.').unwrap_or(&target);
                    format!("{}:{}", host, srv.port())
                })
                .collect(),
            Err(err) => {
                tracing::debug!(
                    name = %self.name,
                    error = %err,
                    "dns discovery: no SRV records (normal for A/AAAA deployments)"
                );
                Vec::new()
            }
        }
    }

    /// A/AAAA lookups: every resolved IP paired with the fallback `port`.
    /// Failure here is warned — an A/AAAA deployment expects these to resolve.
    async fn ip_candidates(&self) -> Vec<String> {
        match self.resolver.lookup_ip(&self.name).await {
            Ok(lookup) => lookup
                .iter()
                .map(|ip| SocketAddr::new(ip, self.port).to_string())
                .collect(),
            Err(err) => {
                tracing::warn!(
                    name = %self.name,
                    error = %err,
                    "dns discovery: A/AAAA resolution failed; returning no A/AAAA candidates"
                );
                Vec::new()
            }
        }
    }
}

#[tonic::async_trait]
impl Discovery for DnsDiscovery {
    async fn candidates(&self) -> Vec<String> {
        // De-duplicate across the two record shapes; a name with both an SRV
        // and A record to the same endpoint must not be dialed twice.
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for addr in self
            .srv_candidates()
            .await
            .into_iter()
            .chain(self.ip_candidates().await)
        {
            if seen.insert(addr.clone()) {
                out.push(addr);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_empty_name() {
        // `DnsDiscovery` holds a resolver that is not `Debug`, so match on the
        // Result rather than using `expect_err`.
        match DnsDiscovery::new("   ".into(), 7071) {
            Ok(_) => panic!("empty name should be rejected"),
            Err(err) => assert!(format!("{err:#}").contains("empty"), "{err:#}"),
        }
    }

    #[test]
    fn new_accepts_a_name_and_port() {
        // Constructing the resolver must succeed on any host with a resolver
        // config (or via the default fallback); no live lookup is performed.
        let backend = match DnsDiscovery::new("coord.batch.example.com".into(), 7071) {
            Ok(backend) => backend,
            Err(err) => panic!("construction should succeed: {err:#}"),
        };
        assert_eq!(backend.name, "coord.batch.example.com");
        assert_eq!(backend.port, 7071);
    }
}
