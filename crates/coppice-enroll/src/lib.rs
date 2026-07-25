//! The enrollment wire contract (ADR 0037 §4) — and, in [`client`], the
//! machine-side flow that speaks it.
//!
//! `POST /api/v1/enroll` is certless first contact: a machine holding only a
//! role-scoped bearer token and the cluster's address sends a CSR and gets
//! back a cluster-signed leaf plus the CA bundle. These types are that
//! endpoint's request and response bodies, shared verbatim by the server
//! (`coppice-api`) and the enrolling daemons so the contract cannot drift.

pub mod client;

pub use client::{
    ensure_enrolled, validate_endpoint, Claim, EnrollClientError, EnrollmentConfig, Outcome,
    Secret, TokenSource,
};

use coppice_core::id::{MachineId, NodeId};
use serde::{Deserialize, Serialize};

/// The path the enrollment endpoint is mounted at, relative to the client
/// listener's root.
pub const ENROLL_PATH: &str = "/api/v1/enroll";

/// Request body for `POST /api/v1/enroll`.
///
/// The token travels in the `Authorization: Bearer` header by preference; the
/// `token` body field is the redacted-body alternative ADR 0037 §4 allows.
/// It must never appear in a query parameter, a log line, or a `Debug`
/// rendering — `Debug` is hand-written below to enforce the last of those.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollRequest {
    /// PEM-encoded certificate signing request. Contributes only its public
    /// key; the cluster dictates the subject.
    pub csr_pem: String,
    /// Bearer-token alternative to the `Authorization` header. When both are
    /// present the header wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// The node id an agent-role enrollee claims (ADR 0011).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// The machine identity a coordinator-role enrollee minted and persisted
    /// for itself (ADR 0037 §7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<MachineId>,
}

impl std::fmt::Debug for EnrollRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollRequest")
            .field("csr_pem_len", &self.csr_pem.len())
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("node_id", &self.node_id)
            .field("machine_id", &self.machine_id)
            .finish()
    }
}

/// Success body for `POST /api/v1/enroll`: the issued leaf and the CA bundle
/// that anchors it. From this moment the machine speaks cluster-CA mTLS.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollResponse {
    /// PEM-encoded issued leaf certificate.
    pub cert_pem: String,
    /// PEM-encoded cluster CA bundle.
    pub ca_pem: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_debug_never_prints_the_token() {
        let request = EnrollRequest {
            csr_pem: "-----BEGIN CERTIFICATE REQUEST-----".to_string(),
            token: Some("cpk_super_secret".to_string()),
            node_id: None,
            machine_id: None,
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("cpk_"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn an_absent_token_serializes_away() {
        let request = EnrollRequest {
            csr_pem: "csr".to_string(),
            token: None,
            node_id: Some(NodeId::new()),
            machine_id: None,
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(!json.contains("token"), "{json}");
        let back: EnrollRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node_id, request.node_id);
    }
}
