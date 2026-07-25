//! The enrollee's half of issuance: a fresh keypair and the CSR that carries
//! its public key to the cluster (ADR 0037 §4).
//!
//! This is the only part of `pki` a machine runs *before* it has any cluster
//! credential — it signs nothing and needs no CA. The CSR's subject is
//! deliberately empty: the cluster dictates every subject (`issue_*`), so
//! asking for one would only invite the illusion that it is honoured.
//!
//! The private key never leaves the caller; [`install_leaf_material`] is where
//! it lands on disk once the issued leaf comes back.
//!
//! [`install_leaf_material`]: super::install_leaf_material

use rcgen::{CertificateParams, KeyPair};

/// A failure generating a keypair or serializing the CSR.
#[derive(Debug, thiserror::Error)]
pub enum CsrError {
    /// Generating the enrollee's key pair failed.
    #[error("generating an enrollment key pair: {0}")]
    Generate(String),

    /// Building or serializing the certificate signing request failed.
    #[error("building the certificate signing request: {0}")]
    Build(String),
}

/// Generate a fresh ECDSA P-256 keypair and a CSR over its public key,
/// returning `(key_pem, csr_pem)`.
///
/// The curve matches every other key this crate mints (rcgen's default
/// signature algorithm), so an enrolled leaf is indistinguishable from a
/// formation-minted one.
pub fn generate_key_and_csr() -> Result<(Vec<u8>, Vec<u8>), CsrError> {
    let key = KeyPair::generate().map_err(|e| CsrError::Generate(e.to_string()))?;
    let params =
        CertificateParams::new(Vec::<String>::new()).map_err(|e| CsrError::Build(e.to_string()))?;
    let csr = params
        .serialize_request(&key)
        .map_err(|e| CsrError::Build(e.to_string()))?;
    let csr_pem = csr.pem().map_err(|e| CsrError::Build(e.to_string()))?;
    Ok((key.serialize_pem().into_bytes(), csr_pem.into_bytes()))
}
