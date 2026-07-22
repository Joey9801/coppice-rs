//! Coordinator discovery (ADR 0037 §2): pluggable, seed-only, non-blocking.
//!
//! A [`Discovery`] backend answers exactly one question — *the current set of
//! coordinator candidates*, as dialable raft addresses. That answer is
//! **advisory**: it seeds who a converging process probes and who a leader
//! reconciles membership against, but it is never authoritative and never on
//! the raft hot path. Raft membership remains the sole authority on who is in
//! the cluster and at what address (ADR 0016, restated by ADR 0037).
//!
//! Because discovery only seeds dialing, a backend that is stale, partial, or
//! down may *delay* convergence but must never *wedge* it: every backend
//! degrades a failure to an empty or partial candidate list with a `tracing`
//! warning rather than propagating an error to its caller. No protocol step
//! requires every discovered candidate to respond, so a stale entry costs only
//! a skipped dial.
//!
//! Backends at this stage: [`static_backend`] (a literal list), [`dns`] (one
//! name resolved per consultation), [`file`] (a directory of run-scoped
//! registration files), and `ec2-asg` (config variant reserved; the backend is
//! not built in this PR — [`build`] returns a clear error).

use std::sync::Arc;

use anyhow::Result;

use crate::config::{BackendKind, DiscoveryConfig};

mod dns;
mod file;
mod static_backend;

pub(crate) use dns::DnsDiscovery;
pub(crate) use file::FileDiscovery;
pub use file::FileRegistration;
pub(crate) use static_backend::StaticDiscovery;

/// A source of coordinator candidates: dialable raft addresses (`"host:port"`)
/// to probe when converging or reconciling membership (ADR 0037 §2).
///
/// The contract is advisory and non-blocking: [`candidates`](Discovery::candidates)
/// returns whatever it can and must not error out to the caller. A backend that
/// cannot reach its source logs a warning and returns an empty or partial list.
#[tonic::async_trait]
pub trait Discovery: Send + Sync {
    /// The current candidate raft addresses. May be empty (nothing found, or
    /// the source is unreachable) or partial; never an error.
    async fn candidates(&self) -> Vec<String>;
}

/// Construct the discovery backend named by `config`.
///
/// The `ec2-asg` variant is reserved but deliberately not built in this PR
/// (heavy AWS SDK dependencies, ADR 0037 §2): selecting it is a clear
/// startup-time error rather than a silent no-op. The seam for a future
/// `Ec2AsgDiscovery` — a thin adapter over this same trait — is this match arm.
pub(crate) fn build(config: &DiscoveryConfig) -> Result<Arc<dyn Discovery>> {
    match config.backend {
        BackendKind::Static => Ok(Arc::new(StaticDiscovery::new(
            config.static_addrs().to_vec(),
        ))),
        BackendKind::Dns => {
            let dns = config
                .dns
                .as_ref()
                .expect("validated: dns backend has a [discovery.dns] table");
            Ok(Arc::new(DnsDiscovery::new(dns.name.clone(), dns.port)?))
        }
        BackendKind::File => {
            let file = config
                .file
                .as_ref()
                .expect("validated: file backend has a [discovery.file] table");
            Ok(Arc::new(FileDiscovery::new(file.dir.clone())))
        }
        // ADR 0037 §2 seam: the EC2 ASG backend is a thin adapter over this same
        // trait, deferred out of this PR to avoid the AWS SDK dependency weight.
        BackendKind::Ec2Asg => {
            anyhow::bail!(
                "discovery backend \"ec2-asg\" is not yet implemented; \
                 use \"static\", \"dns\", or \"file\" (ADR 0037 §2)"
            )
        }
    }
}
