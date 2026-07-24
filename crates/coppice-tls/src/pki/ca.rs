//! The cluster root CA: minting it, and signing under a loaded one (ADR 0037 §4).

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose,
    PublicKeyData,
};

use super::{ca_public_key_der, validity_window, CA_COMMON_NAME, CA_LIFETIME};

/// A failure minting or loading cluster CA material.
#[derive(Debug, thiserror::Error)]
pub enum CaError {
    /// A key pair could not be generated.
    #[error("generating CA key pair: {0}")]
    Generate(String),

    /// Assembling or self-signing the CA certificate failed.
    #[error("minting the CA certificate: {0}")]
    Mint(String),

    /// The supplied CA certificate PEM could not be parsed.
    #[error("parsing the CA certificate: {0}")]
    BadCaCert(String),

    /// The supplied CA private key PEM could not be parsed.
    #[error("parsing the CA private key: {0}")]
    BadKey(String),

    /// The private key does not match the CA certificate (their public keys
    /// differ). A **distinct** signal, deliberately: ADR 0037 §4 makes a
    /// corrupt/mismatched key file a refuse-and-repair condition, and later
    /// chunks decline membership changes on it rather than sign under the wrong
    /// key.
    #[error("the supplied private key does not match the CA certificate")]
    KeyMismatch,

    /// Signing a leaf failed (bad CSR public key, serialization error).
    #[error("signing a leaf certificate: {0}")]
    Sign(String),
}

/// Freshly-minted root CA material: the certificate (public, replicated state
/// per ADR 0037 §4) and its private key (which stays on voter disks only).
#[derive(Debug, Clone)]
pub struct CaMaterial {
    /// The CA certificate, PEM-encoded — the cluster trust root.
    pub cert_pem: Vec<u8>,
    /// The CA private key, PEM-encoded — never enters replicated state.
    pub key_pem: Vec<u8>,
}

/// Mint the cluster root CA (ADR 0037 §4): ECDSA P-256, long-lived
/// ([`CA_LIFETIME`]), self-signed, with a recognizable common name
/// ([`CA_COMMON_NAME`]).
///
/// `IsCa::Ca(BasicConstraints::Unconstrained)` — the root is deliberately
/// unconstrained in path length: v1 issues leaves directly under the root, and
/// the designed upgrade path for bounding voter-disk authority is a cluster-held
/// *intermediate* under this root (§4), which an explicit path-len-0 constraint
/// here would forbid. Key usages are the signing set a CA needs:
/// `KeyCertSign` + `CrlSign` + `DigitalSignature`.
pub fn mint_root_ca() -> Result<CaMaterial, CaError> {
    let key = KeyPair::generate().map_err(|e| CaError::Generate(e.to_string()))?;
    let mut params =
        CertificateParams::new(Vec::<String>::new()).map_err(|e| CaError::Mint(e.to_string()))?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, CA_COMMON_NAME);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let (not_before, not_after) = validity_window(CA_LIFETIME);
    params.not_before = not_before;
    params.not_after = not_after;

    let cert = params
        .self_signed(&key)
        .map_err(|e| CaError::Mint(e.to_string()))?;
    Ok(CaMaterial {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
    })
}

/// A loaded cluster CA that signs leaves. Constructed from the CA certificate
/// and private key PEM; construction fails with [`CaError::KeyMismatch`] when
/// the two do not pair (ADR 0037 §4).
///
/// This is a **signing entry point** in the audit sense (see the module docs):
/// only coordinator code paths, running on the leader that holds the CA key,
/// construct and use it.
pub struct CaSigner {
    /// The issuer certificate rcgen signs against — reconstructed from the
    /// loaded CA's parsed parameters, so leaves carry the loaded CA's subject as
    /// their issuer and its subject-key-id as their authority-key-id.
    issuer: Certificate,
    /// The CA private key that signs.
    key: KeyPair,
    /// The loaded CA certificate PEM, handed back to enrollees with their leaf
    /// (ADR 0037 §4: the enrollment response is the leaf plus the CA bundle).
    ca_cert_pem: Vec<u8>,
}

impl std::fmt::Debug for CaSigner {
    /// Deliberately never renders the CA key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaSigner")
            .field("ca_cert_pem_len", &self.ca_cert_pem.len())
            .finish_non_exhaustive()
    }
}

impl CaSigner {
    /// Load a signer from CA certificate + key PEM.
    ///
    /// Fails with [`CaError::KeyMismatch`] — distinct from the parse errors —
    /// when the key's public key does not equal the certificate's, so a
    /// swapped, stale, or corrupt key file is caught before it can sign under a
    /// certificate it does not own.
    pub fn load(ca_cert_pem: &[u8], ca_key_pem: &[u8]) -> Result<CaSigner, CaError> {
        let key_str = std::str::from_utf8(ca_key_pem)
            .map_err(|_| CaError::BadKey("not UTF-8".to_string()))?;
        let key = KeyPair::from_pem(key_str).map_err(|e| CaError::BadKey(e.to_string()))?;

        let ca_spki = ca_public_key_der(ca_cert_pem).map_err(CaError::BadCaCert)?;
        if ca_spki != key.public_key_der() {
            return Err(CaError::KeyMismatch);
        }

        let cert_str = std::str::from_utf8(ca_cert_pem)
            .map_err(|_| CaError::BadCaCert("not UTF-8".to_string()))?;
        let params = CertificateParams::from_ca_cert_pem(cert_str)
            .map_err(|e| CaError::BadCaCert(e.to_string()))?;
        let issuer = params
            .self_signed(&key)
            .map_err(|e| CaError::BadCaCert(e.to_string()))?;

        Ok(CaSigner {
            issuer,
            key,
            ca_cert_pem: ca_cert_pem.to_vec(),
        })
    }

    /// The CA certificate PEM (the bundle handed to an enrollee alongside its
    /// leaf).
    pub fn ca_cert_pem(&self) -> &[u8] {
        &self.ca_cert_pem
    }

    /// Sign `params` for `public_key`, returning the leaf certificate PEM. The
    /// cluster always dictates `params`; the CSR contributes only its public
    /// key (ADR 0037 §4: the cluster decides every subject).
    pub(crate) fn sign(
        &self,
        params: CertificateParams,
        public_key: &impl PublicKeyData,
    ) -> Result<Vec<u8>, CaError> {
        let cert = params
            .signed_by(public_key, &self.issuer, &self.key)
            .map_err(|e| CaError::Sign(e.to_string()))?;
        Ok(cert.pem().into_bytes())
    }
}
