//! The three ADR 0022 leaf profiles (ADR 0037 §4).
//!
//! Every profile is server+client capable (each node dials and serves with one
//! leaf) and short-lived by default ([`super::LEAF_LIFETIME`]). Two entry points
//! per profile:
//!
//! - `issue_*(signer, csr_pem, …)` — sign an enrollee's CSR. The CSR contributes
//!   **only its public key**; the cluster dictates the subject, EKUs, validity,
//!   and SANs entirely (ADR 0037 §4: "the cluster decides every subject").
//! - `mint_*_local(signer, …)` — generate the keypair in-process and sign it,
//!   returning `(cert_pem, key_pem)`. This is the no-CSR formation path
//!   (§3 step 5): the forming voter mints its own coordinator leaf, and `init`
//!   can mint an operator leaf locally when no CSR is supplied.

use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, KeyPair,
    KeyUsagePurpose,
};

use coppice_core::id::{MachineId, NodeId};

use super::ca::{CaError, CaSigner};
use super::{validity_window, COORDINATOR_OU, LEAF_LIFETIME, OPERATOR_OU};

/// A failure issuing a leaf certificate.
#[derive(Debug, thiserror::Error)]
pub enum IssueError {
    /// The submitted CSR PEM could not be parsed or its self-signature did not
    /// verify (rcgen checks CSR proof-of-possession on parse).
    #[error("parsing the certificate signing request: {0}")]
    BadCsr(String),

    /// Building the leaf parameters (e.g. an unrepresentable SAN string) failed.
    #[error("building leaf parameters: {0}")]
    Params(String),

    /// Generating a key pair for a `mint_*_local` variant failed.
    #[error("generating a leaf key pair: {0}")]
    Generate(String),

    /// The CA signer rejected the leaf.
    #[error(transparent)]
    Ca(#[from] CaError),
}

// ---------------------------------------------------------------------------
// Per-profile parameter builders — the single place each profile's subject,
// EKUs, key usages, validity, and SAN policy are defined.
// ---------------------------------------------------------------------------

/// The common shape of every leaf: server+client EKUs, `DigitalSignature +
/// KeyEncipherment` key usages, and the short default validity window. `sans`
/// are classified into IP/DNS SANs by rcgen.
fn base_leaf_params(sans: &[String]) -> Result<CertificateParams, IssueError> {
    let mut params =
        CertificateParams::new(sans.to_vec()).map_err(|e| IssueError::Params(e.to_string()))?;
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    let (not_before, not_after) = validity_window(LEAF_LIFETIME);
    params.not_before = not_before;
    params.not_after = not_after;
    Ok(params)
}

/// Coordinator profile: `OU=coppice-coordinator`, `CN` = the machine identity
/// (ADR 0037 §4/§7). Hostnames/IPs are metadata carried as SANs, never identity.
fn coordinator_params(
    machine: &MachineId,
    sans: &[String],
) -> Result<CertificateParams, IssueError> {
    let mut params = base_leaf_params(sans)?;
    params
        .distinguished_name
        .push(DnType::CommonName, machine.to_string());
    params
        .distinguished_name
        .push(DnType::OrganizationalUnitName, COORDINATOR_OU);
    Ok(params)
}

/// Agent profile: **no** `OU`, `CN` = the node id (ADR 0011). The typed node id
/// string is also added as a dNSName SAN so a coordinator's id-pinned dial (TLS
/// server-name = `node-<uuid>`) validates (ADR 0034), ahead of any caller SANs.
fn agent_params(node: &NodeId, sans: &[String]) -> Result<CertificateParams, IssueError> {
    let mut all_sans = Vec::with_capacity(sans.len() + 1);
    all_sans.push(node.to_string());
    all_sans.extend_from_slice(sans);
    let mut params = base_leaf_params(&all_sans)?;
    params
        .distinguished_name
        .push(DnType::CommonName, node.to_string());
    Ok(params)
}

/// Operator profile: `OU=coppice-operators`, `CN` = the supplied name; the
/// principal is `cert:<CN>` with full admin authority (ADR 0022 break-glass).
fn operator_params(cn: &str) -> Result<CertificateParams, IssueError> {
    let mut params = base_leaf_params(&[])?;
    params.distinguished_name.push(DnType::CommonName, cn);
    params
        .distinguished_name
        .push(DnType::OrganizationalUnitName, OPERATOR_OU);
    Ok(params)
}

/// Parse and verify a submitted CSR, yielding its parameters (of which only the
/// public key is used).
fn parse_csr(csr_pem: &[u8]) -> Result<CertificateSigningRequestParams, IssueError> {
    let s =
        std::str::from_utf8(csr_pem).map_err(|_| IssueError::BadCsr("not UTF-8".to_string()))?;
    CertificateSigningRequestParams::from_pem(s).map_err(|e| IssueError::BadCsr(e.to_string()))
}

// ---------------------------------------------------------------------------
// CSR-signing entry points.
// ---------------------------------------------------------------------------

/// Issue a **coordinator** leaf by signing `csr_pem`'s public key under a
/// cluster-dictated subject (`CN` = `machine`, `OU=coppice-coordinator`). The
/// CSR's own subject is ignored entirely; `sans` are caller-supplied
/// hostnames/IPs (metadata, never identity).
pub fn issue_coordinator(
    signer: &CaSigner,
    csr_pem: &[u8],
    machine: &MachineId,
    sans: &[String],
) -> Result<Vec<u8>, IssueError> {
    let csr = parse_csr(csr_pem)?;
    let params = coordinator_params(machine, sans)?;
    Ok(signer.sign(params, &csr.public_key)?)
}

/// Issue an **agent** leaf: `CN` = `node`, no `OU`, the node id string as a
/// dNSName SAN, plus `sans`. The CSR contributes only its public key.
pub fn issue_agent(
    signer: &CaSigner,
    csr_pem: &[u8],
    node: &NodeId,
    sans: &[String],
) -> Result<Vec<u8>, IssueError> {
    let csr = parse_csr(csr_pem)?;
    let params = agent_params(node, sans)?;
    Ok(signer.sign(params, &csr.public_key)?)
}

/// Issue an **operator** leaf: `CN` = `cn`, `OU=coppice-operators` (ADR 0022
/// break-glass / day-0). The CSR contributes only its public key.
pub fn issue_operator(signer: &CaSigner, csr_pem: &[u8], cn: &str) -> Result<Vec<u8>, IssueError> {
    let csr = parse_csr(csr_pem)?;
    let params = operator_params(cn)?;
    Ok(signer.sign(params, &csr.public_key)?)
}

// ---------------------------------------------------------------------------
// Local-keypair entry points (formation, no CSR — ADR 0037 §3 step 5).
// ---------------------------------------------------------------------------

fn mint_local(
    signer: &CaSigner,
    params: CertificateParams,
) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    let key = KeyPair::generate().map_err(|e| IssueError::Generate(e.to_string()))?;
    let cert_pem = signer.sign(params, &key)?;
    Ok((cert_pem, key.serialize_pem().into_bytes()))
}

/// Mint a **coordinator** leaf with an in-process keypair, returning
/// `(cert_pem, key_pem)`. The forming voter's own leaf (ADR 0037 §3 step 3).
pub fn mint_coordinator_local(
    signer: &CaSigner,
    machine: &MachineId,
    sans: &[String],
) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    mint_local(signer, coordinator_params(machine, sans)?)
}

/// Mint an **agent** leaf with an in-process keypair, returning
/// `(cert_pem, key_pem)`.
pub fn mint_agent_local(
    signer: &CaSigner,
    node: &NodeId,
    sans: &[String],
) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    mint_local(signer, agent_params(node, sans)?)
}

/// Mint an **operator** leaf with an in-process keypair, returning
/// `(cert_pem, key_pem)`. The no-CSR `init` path (ADR 0037 §3 step 5): both
/// halves are printed for the operator's SSH session to collect.
pub fn mint_operator_local(signer: &CaSigner, cn: &str) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    mint_local(signer, operator_params(cn)?)
}
