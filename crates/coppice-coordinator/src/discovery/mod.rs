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
//! registration files), and [`ec2_asg`] (Auto Scaling group membership — the
//! one platform-specific backend, with liveness semantics for the leader's
//! removal rule, ADR 0037 §5).

use std::sync::Arc;

use anyhow::Result;
use coppice_consensus::LivenessAttestor;

use crate::config::{BackendKind, DiscoveryConfig};

mod dns;
mod ec2_asg;
mod file;
mod static_backend;

pub(crate) use dns::DnsDiscovery;
pub(crate) use ec2_asg::Ec2AsgDiscovery;
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

    /// The backend's liveness attestor (ADR 0037 §5), if it has liveness
    /// semantics. Only backends that can attest an instance is *genuinely gone*
    /// (e.g. [`ec2_asg`]) return `Some`; `static`/`dns`/`file` return the
    /// default `None` and contribute nothing to the leader's evidence-gated
    /// overflow removal, so a stale registration or unedited list can never
    /// block a legitimate removal.
    fn liveness_attestor(&self) -> Option<Arc<dyn LivenessAttestor>> {
        None
    }
}

/// Construct the discovery backend named by `config`.
///
/// The `ec2-asg` variant is a thin adapter over the same [`Discovery`] trait
/// (ADR 0037 §2); its real AWS client is built lazily on the first consultation,
/// so `build` never touches IMDS or the network and cannot hang startup.
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
        BackendKind::Ec2Asg => {
            let ec2 = config
                .ec2_asg
                .as_ref()
                .expect("validated: ec2-asg backend has a [discovery.ec2_asg] table");
            Ok(Ec2AsgDiscovery::new(
                ec2.port,
                ec2.region.clone(),
                ec2.timeout,
            ))
        }
    }
}
