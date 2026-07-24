//! Leaf verification and profile classification (ADR 0037 §4/§7).
//!
//! [`verify_leaf`] does a **real** chain verification — a webpki
//! signature-and-validity check against the cluster CA, not an issuer-name
//! comparison — and only then reads the subject to classify the profile. Later
//! chunks' authorization is built on it: a leaf that classifies as
//! `Coordinator(machine)` is an authenticated machine identity, etc.

use std::io::Cursor;
use std::time::SystemTime;

use tokio_rustls::rustls::pki_types::{CertificateDer, UnixTime};
use webpki::{EndEntityCert, KeyUsage};

use coppice_core::id::{MachineId, NodeId};

use crate::parse_leaf_subject_der;

use super::{Profile, VerifiedLeaf, COORDINATOR_OU, OPERATOR_OU};

/// A failure verifying or classifying a leaf.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The leaf or CA PEM/DER could not be decoded.
    #[error("decoding certificate input: {0}")]
    Decode(String),

    /// The leaf did not chain to the CA, or its signature did not verify.
    #[error("leaf does not verify against the cluster CA: {0}")]
    UntrustedChain(String),

    /// The leaf is outside its validity window (expired or not yet valid).
    #[error("leaf is outside its validity window: {0}")]
    Validity(String),

    /// The leaf verified, but its subject could not be parsed.
    #[error("verified leaf has an unparseable subject")]
    UnparseableSubject,

    /// The leaf verified, but its subject matches no known profile (ADR 0037
    /// §4): an unrecognized `OU`, or a `CN` that does not parse as the id the
    /// profile requires.
    #[error("verified leaf matches no known profile: {0}")]
    Unclassifiable(String),
}

/// Verify `leaf` against `ca_pem` and classify its profile (ADR 0037 §4).
///
/// `leaf` may be PEM or raw DER. Verification is a full webpki chain check
/// (signature over the leaf by the CA key, and the leaf's validity window)
/// against the CA as the sole trust anchor with no intermediates — v1 issues
/// directly under the root. Classification then reads the subject:
///
/// - `OU=coppice-operators` ⇒ [`Profile::Operator`] (`CN` = the operator name);
/// - `OU=coppice-coordinator` + `CN` parsing as a [`MachineId`] ⇒
///   [`Profile::Coordinator`];
/// - no `OU` + `CN` parsing as a [`NodeId`] ⇒ [`Profile::Agent`];
/// - anything else ⇒ [`VerifyError::Unclassifiable`].
pub fn verify_leaf(ca_pem: &[u8], leaf: &[u8]) -> Result<VerifiedLeaf, VerifyError> {
    let leaf_der = coerce_to_der(leaf)?;
    let ca_der = first_cert_der(ca_pem)?;

    let anchor = webpki::anchor_from_trusted_cert(&ca_der)
        .map_err(|e| VerifyError::UntrustedChain(format!("trust anchor: {e}")))?;
    let ee = EndEntityCert::try_from(&leaf_der)
        .map_err(|e| VerifyError::Decode(format!("leaf certificate: {e}")))?;

    let now = UnixTime::since_unix_epoch(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default(),
    );

    // Server+client EKUs are present on every profile; require client-auth (the
    // authorization use — the peer is authenticating).
    ee.verify_for_usage(
        webpki::ALL_VERIFICATION_ALGS,
        &[anchor],
        &[],
        now,
        KeyUsage::client_auth(),
        None,
        None,
    )
    .map_err(map_webpki_error)?;

    let subject =
        parse_leaf_subject_der(leaf_der.as_ref()).ok_or(VerifyError::UnparseableSubject)?;
    let profile = classify(&subject)?;
    Ok(VerifiedLeaf { profile, subject })
}

/// Map a webpki verification error to the coarse validity-vs-trust distinction
/// callers care about.
fn map_webpki_error(e: webpki::Error) -> VerifyError {
    match e {
        webpki::Error::CertExpired { .. } | webpki::Error::CertNotValidYet { .. } => {
            VerifyError::Validity(e.to_string())
        }
        other => VerifyError::UntrustedChain(other.to_string()),
    }
}

/// Classify a verified leaf's subject into a [`Profile`] (ADR 0037 §4).
fn classify(subject: &crate::LeafSubject) -> Result<Profile, VerifyError> {
    let cn = || {
        subject
            .common_name
            .clone()
            .ok_or_else(|| VerifyError::Unclassifiable("leaf has no common name".to_string()))
    };
    match subject.org_unit.as_deref() {
        Some(OPERATOR_OU) => Ok(Profile::Operator { cn: cn()? }),
        Some(COORDINATOR_OU) => {
            let cn = cn()?;
            let machine = cn.parse::<MachineId>().map_err(|_| {
                VerifyError::Unclassifiable(format!(
                    "coordinator-profile CN {cn:?} is not a machine id"
                ))
            })?;
            Ok(Profile::Coordinator(machine))
        }
        None => {
            let cn = cn()?;
            let node = cn.parse::<NodeId>().map_err(|_| {
                VerifyError::Unclassifiable(format!("no-OU leaf CN {cn:?} is not a node id"))
            })?;
            Ok(Profile::Agent(node))
        }
        Some(other) => Err(VerifyError::Unclassifiable(format!(
            "unrecognized profile OU {other:?}"
        ))),
    }
}

/// Decode a leaf that may be PEM or raw DER into an owned [`CertificateDer`].
fn coerce_to_der(input: &[u8]) -> Result<CertificateDer<'static>, VerifyError> {
    if looks_like_pem(input) {
        first_cert_der(input)
    } else {
        Ok(CertificateDer::from(input.to_vec()))
    }
}

/// The first certificate in a PEM bundle, as owned DER.
fn first_cert_der(pem: &[u8]) -> Result<CertificateDer<'static>, VerifyError> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .next()
        .ok_or_else(|| VerifyError::Decode("no certificate in PEM".to_string()))?
        .map(|der| der.into_owned())
        .map_err(|e| VerifyError::Decode(format!("parsing certificate PEM: {e}")))
}

/// A cheap sniff: PEM is ASCII beginning (after any leading whitespace) with the
/// `-----BEGIN` armor; DER begins with the `0x30` SEQUENCE tag.
fn looks_like_pem(input: &[u8]) -> bool {
    input
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|&b| b == b'-')
}
