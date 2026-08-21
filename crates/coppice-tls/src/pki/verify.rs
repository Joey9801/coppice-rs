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
/// with no intermediates — v1 issues directly under a root. Classification
/// then reads the subject:
///
/// - `OU=coppice-operators` ⇒ [`Profile::Operator`] (`CN` = the operator name);
/// - `OU=coppice-coordinator` + `CN` parsing as a [`MachineId`] ⇒
///   [`Profile::Coordinator`];
/// - no `OU` + `CN` parsing as a [`NodeId`] ⇒ [`Profile::Agent`];
/// - anything else ⇒ [`VerifyError::Unclassifiable`].
///
/// # Every certificate in the bundle is a trust anchor
///
/// `ca_pem` is the cluster's *trust-anchor set*, not one certificate, and a
/// leaf verifying under **any** member of it verifies. That is what makes the
/// dual-trust window of a re-root (ADR 0037 §4) work: `rotate-ca begin`
/// records a two-root bundle, and for the length of the rotation both the
/// incoming root's leaves and the outgoing root's still-unexpired leaves must
/// authenticate on the same listener. Anchoring on only the first entry would
/// have made the recorded chain decorative — every peer that had not yet
/// renewed would have been refused at the instant the new bundle committed,
/// which is a flag day, not a rotation.
///
/// Bundle **order** still carries meaning, just not here: position 0 is the
/// *active signing* root (what [`super::load_ca_key`] and
/// [`CaSigner::load`](super::CaSigner::load) pair a key against, and therefore
/// what new leaves are issued under). Verification is order-independent;
/// issuance is not.
pub fn verify_leaf(ca_pem: &[u8], leaf: &[u8]) -> Result<VerifiedLeaf, VerifyError> {
    let leaf_der = coerce_to_der(leaf)?;
    let ca_ders = all_cert_ders(ca_pem)?;

    let anchors = ca_ders
        .iter()
        .map(|der| {
            webpki::anchor_from_trusted_cert(der)
                .map_err(|e| VerifyError::UntrustedChain(format!("trust anchor: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
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
        &anchors,
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

/// The dNSName and iPAddress SANs a leaf carries, in certificate order.
///
/// The addresses a leaf may **serve** under, as distinct from the subject that
/// says who it is (ADR 0037 §4: hostnames are metadata, never identity). What
/// needs this is renewal: a re-issue must re-declare the same serving
/// addresses, or a renewed replica would silently stop terminating TLS for the
/// name its peers dial it by. Returns an empty vector for a leaf with no SAN
/// extension, which is a legitimate shape (an operator credential serves
/// nothing).
pub fn leaf_sans(leaf: &[u8]) -> Result<Vec<String>, VerifyError> {
    let der = coerce_to_der(leaf)?;
    let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref())
        .map_err(|e| VerifyError::Decode(format!("parsing certificate DER: {e}")))?;
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Ok(Vec::new());
    };
    Ok(san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            x509_parser::extensions::GeneralName::DNSName(dns) => Some((*dns).to_string()),
            x509_parser::extensions::GeneralName::IPAddress(bytes) => match bytes.len() {
                4 => {
                    let octets: [u8; 4] = (*bytes).try_into().ok()?;
                    Some(std::net::Ipv4Addr::from(octets).to_string())
                }
                16 => {
                    let octets: [u8; 16] = (*bytes).try_into().ok()?;
                    Some(std::net::Ipv6Addr::from(octets).to_string())
                }
                _ => None,
            },
            _ => None,
        })
        .collect())
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

/// Every certificate in a PEM bundle, as owned DER, in bundle order.
///
/// The whole trust-anchor set, because a re-root's dual-trust window records
/// two roots and both must authenticate (see [`verify_leaf`]).
fn all_cert_ders(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, VerifyError> {
    let ders = rustls_pemfile::certs(&mut Cursor::new(pem))
        .map(|der| der.map(|d| d.into_owned()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| VerifyError::Decode(format!("parsing certificate PEM: {e}")))?;
    if ders.is_empty() {
        return Err(VerifyError::Decode("no certificate in PEM".to_string()));
    }
    Ok(ders)
}

/// A cheap sniff: PEM is ASCII beginning (after any leading whitespace) with the
/// `-----BEGIN` armor; DER begins with the `0x30` SEQUENCE tag.
fn looks_like_pem(input: &[u8]) -> bool {
    input
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|&b| b == b'-')
}
