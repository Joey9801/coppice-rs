//! `pki` unit tests plus a real-mTLS interop round-trip.
//!
//! The crypto units (token hashing, key custody, machine persistence, CA
//! loading) are pure and fast. The interop test mints a CA, issues the
//! coordinator and agent leaves through `pki`, stands up the real [`crate::serve`]
//! acceptor, drives a rustls client, and checks the captured peer certs
//! round-trip through [`verify_leaf`] to the right profile and identity.

use std::io::Cursor;
use std::sync::Arc;

use rcgen::{CertificateParams, DnType, KeyPair};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;

use coppice_core::id::{MachineId, NodeId};

use super::*;
use crate::{serve, TlsPaths, TlsStore};

const LOCAL_SANS: &[&str] = &["localhost", "127.0.0.1"];

fn local_sans() -> Vec<String> {
    LOCAL_SANS.iter().map(|s| s.to_string()).collect()
}

// ---- token ----------------------------------------------------------------

#[test]
fn token_secret_round_trips() {
    let secret = generate_secret();
    assert!(secret.starts_with("cpk_"), "{secret}");
    let hash = hash_secret(&secret).unwrap();
    assert!(hash.starts_with("$argon2id$"), "{hash}");
    assert!(
        verify_secret(&secret, &hash),
        "the minted secret must verify"
    );
}

#[test]
fn token_wrong_secret_fails() {
    let hash = hash_secret(&generate_secret()).unwrap();
    assert!(!verify_secret("cpk_not-the-secret", &hash));
}

#[test]
fn token_malformed_hash_returns_false_not_panic() {
    assert!(!verify_secret("cpk_whatever", "not a phc string"));
    assert!(!verify_secret("cpk_whatever", ""));
    assert!(!verify_secret("cpk_whatever", "$argon2id$garbage"));
}

#[test]
fn token_two_hashes_of_one_secret_differ() {
    // A random salt per hash: the same secret hashes to two distinct PHC
    // strings, and each verifies.
    let secret = generate_secret();
    let a = hash_secret(&secret).unwrap();
    let b = hash_secret(&secret).unwrap();
    assert_ne!(a, b, "distinct salts must produce distinct hashes");
    assert!(verify_secret(&secret, &a));
    assert!(verify_secret(&secret, &b));
}

// ---- machine identity -----------------------------------------------------

#[test]
fn machine_identity_persists_and_reloads() {
    let dir = tempfile::tempdir().unwrap();
    // Fresh state: nothing on disk.
    assert!(load_machine_identity(dir.path()).unwrap().is_none());

    let id = mint_machine_identity();
    persist_machine_identity(dir.path(), &id).unwrap();
    let loaded = load_machine_identity(dir.path()).unwrap();
    assert_eq!(loaded, Some(id), "the persisted identity must reload");
}

#[cfg(unix)]
#[test]
fn machine_identity_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let id = mint_machine_identity();
    persist_machine_identity(dir.path(), &id).unwrap();
    let mode = std::fs::metadata(dir.path().join(MACHINE_IDENTITY_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "identity file must be 0600, got {mode:04o}");
}

// ---- CA + custody ---------------------------------------------------------

#[test]
fn ca_signer_loads_matching_material() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    assert_eq!(signer.ca_cert_pem(), ca.cert_pem.as_slice());
}

#[test]
fn ca_signer_rejects_mismatched_key() {
    let ca = mint_root_ca().unwrap();
    let other = mint_root_ca().unwrap();
    let err = CaSigner::load(&ca.cert_pem, &other.key_pem).unwrap_err();
    assert!(matches!(err, CaError::KeyMismatch), "got {err:?}");
}

#[test]
fn custody_write_then_load_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let ca = mint_root_ca().unwrap();
    write_ca_key(dir.path(), &ca.key_pem).unwrap();
    let loaded = load_ca_key(dir.path(), &ca.cert_pem).unwrap();
    assert_eq!(loaded, ca.key_pem);
}

/// Regression: with a fixed temp-file name, a pre-existing loose temp file
/// would be reused as-is (`mode(0o600)` only applies at creation), truncated,
/// and renamed into place — leaving the CA key group/world-readable and the
/// next `load_ca_key` refusing a file `write_ca_key` itself produced. The
/// unique `create_new` temp must ignore such droppings entirely.
#[cfg(unix)]
#[test]
fn custody_write_is_owner_only_despite_preexisting_loose_tmp() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let loose = dir.path().join(".ca.key.tmp");
    std::fs::write(&loose, b"stale").unwrap();
    std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o644)).unwrap();

    let ca = mint_root_ca().unwrap();
    write_ca_key(dir.path(), &ca.key_pem).unwrap();

    let mode = std::fs::metadata(dir.path().join(CA_KEY_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "key written {mode:04o} despite loose tmp");
    let loaded = load_ca_key(dir.path(), &ca.cert_pem).unwrap();
    assert_eq!(loaded, ca.key_pem);
    // No temp droppings left behind (the stale decoy itself is untouched).
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".tmp") && n != ".ca.key.tmp")
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
}

#[cfg(unix)]
#[test]
fn custody_write_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let ca = mint_root_ca().unwrap();
    write_ca_key(dir.path(), &ca.key_pem).unwrap();
    let mode = std::fs::metadata(dir.path().join(CA_KEY_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "ca key must be 0600, got {mode:04o}");
}

#[test]
fn custody_load_when_absent_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let ca = mint_root_ca().unwrap();
    let err = load_ca_key(dir.path(), &ca.cert_pem).unwrap_err();
    assert!(matches!(err, CustodyError::NotFound { .. }), "got {err:?}");
}

#[cfg(unix)]
#[test]
fn custody_refuses_group_readable_key() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let ca = mint_root_ca().unwrap();
    write_ca_key(dir.path(), &ca.key_pem).unwrap();
    // Loosen to group+world readable: custody must refuse.
    let path = dir.path().join(CA_KEY_FILE);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = load_ca_key(dir.path(), &ca.cert_pem).unwrap_err();
    assert!(
        matches!(err, CustodyError::InsecurePermissions { .. }),
        "got {err:?}"
    );
}

#[test]
fn custody_detects_key_from_a_different_ca() {
    let dir = tempfile::tempdir().unwrap();
    let ca = mint_root_ca().unwrap();
    let other = mint_root_ca().unwrap();
    // Store a key that does not match the CA cert we later check against.
    write_ca_key(dir.path(), &other.key_pem).unwrap();
    let err = load_ca_key(dir.path(), &ca.cert_pem).unwrap_err();
    assert!(
        matches!(err, CustodyError::KeyMismatch { .. }),
        "got {err:?}"
    );
}

// ---- issuance + verification (local keypair path) -------------------------

#[test]
fn issued_leaves_verify_to_their_profiles() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();

    let machine = mint_machine_identity();
    let node = NodeId::new();

    let (coord_cert, _coord_key) =
        mint_coordinator_local(&signer, &machine, &local_sans()).unwrap();
    let (agent_cert, _agent_key) = mint_agent_local(&signer, &node, &local_sans()).unwrap();
    let (op_cert, _op_key) = mint_operator_local(&signer, "alice").unwrap();

    let coord = verify_leaf(&ca.cert_pem, &coord_cert).unwrap();
    assert_eq!(coord.profile, Profile::Coordinator(machine));

    let agent = verify_leaf(&ca.cert_pem, &agent_cert).unwrap();
    assert_eq!(agent.profile, Profile::Agent(node));
    // The agent CN is exactly the typed node id string.
    assert_eq!(
        agent.subject.common_name.as_deref(),
        Some(node.to_string().as_str())
    );

    let op = verify_leaf(&ca.cert_pem, &op_cert).unwrap();
    assert_eq!(
        op.profile,
        Profile::Operator {
            cn: "alice".to_string()
        }
    );
}

#[test]
fn verify_rejects_a_leaf_from_a_foreign_ca() {
    let ca = mint_root_ca().unwrap();
    let foreign = mint_root_ca().unwrap();
    let foreign_signer = CaSigner::load(&foreign.cert_pem, &foreign.key_pem).unwrap();
    let (leaf, _key) =
        mint_coordinator_local(&foreign_signer, &mint_machine_identity(), &local_sans()).unwrap();

    // Verified against the WRONG CA, a real signature check must reject it.
    let err = verify_leaf(&ca.cert_pem, &leaf).unwrap_err();
    assert!(matches!(err, VerifyError::UntrustedChain(_)), "got {err:?}");
}

// ---- issuance from a real CSR ---------------------------------------------

/// Build a minimal CSR PEM for a fresh keypair (the enrollee's side).
fn make_csr() -> (Vec<u8>, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    // The subject here is deliberately junk: the cluster ignores it entirely.
    params
        .distinguished_name
        .push(DnType::CommonName, "whatever-the-client-asked-for");
    let csr = params.serialize_request(&key).unwrap();
    (csr.pem().unwrap().into_bytes(), key)
}

#[test]
fn issue_coordinator_from_csr_ignores_requested_subject() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    let (csr_pem, _key) = make_csr();

    let machine = mint_machine_identity();
    let leaf = issue_coordinator(&signer, &csr_pem, &machine, &local_sans()).unwrap();

    let verified = verify_leaf(&ca.cert_pem, &leaf).unwrap();
    // The CLUSTER's chosen subject wins, not the CSR's.
    assert_eq!(verified.profile, Profile::Coordinator(machine));
    assert_eq!(
        verified.subject.common_name.as_deref(),
        Some(machine.to_string().as_str())
    );
}

#[test]
fn issue_agent_from_csr_sets_node_identity() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    let (csr_pem, _key) = make_csr();

    let node = NodeId::new();
    let leaf = issue_agent(&signer, &csr_pem, &node, &[]).unwrap();
    let verified = verify_leaf(&ca.cert_pem, &leaf).unwrap();
    assert_eq!(verified.profile, Profile::Agent(node));
}

#[test]
fn issue_rejects_a_malformed_csr() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    let err =
        issue_operator(&signer, b"-----BEGIN CERTIFICATE REQUEST-----\nnope\n", "x").unwrap_err();
    assert!(matches!(err, IssueError::BadCsr(_)), "got {err:?}");
}

// ---- interop: a real mTLS handshake with pki-minted leaves ----------------

fn parse_certs(pem: &[u8]) -> Vec<CertificateDer<'static>> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn parse_key(pem: &[u8]) -> PrivateKeyDer<'static> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .unwrap()
        .unwrap()
}

/// A rustls client trusting `ca_pem` and presenting `(cert, key)`.
fn client_config(ca_pem: &[u8], cert: &[u8], key: &[u8]) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    for c in parse_certs(ca_pem) {
        roots.add(c).unwrap();
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(parse_certs(cert), parse_key(key))
        .unwrap()
}

#[tokio::test]
async fn mtls_round_trip_with_pki_minted_leaves() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();

    let machine = mint_machine_identity();
    let node = NodeId::new();
    // Coordinator leaf serves; agent leaf dials as the client.
    let (server_cert, server_key) =
        mint_coordinator_local(&signer, &machine, &local_sans()).unwrap();
    let (client_cert, client_key) = mint_agent_local(&signer, &node, &local_sans()).unwrap();

    let store = TlsStore::from_pem(
        TlsPaths {
            cert: "unused-cert".into(),
            key: "unused-key".into(),
            ca: "unused-ca".into(),
        },
        ca.cert_pem.clone(),
        server_cert.clone(),
        server_key,
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = serve(listener, store);

    // Drive one client handshake; capture the server leaf the client saw.
    let cfg = client_config(&ca.cert_pem, &client_cert, &client_key);
    let ca_for_client = ca.cert_pem.clone();
    let client = tokio::spawn(async move {
        let connector = TlsConnector::from(Arc::new(cfg));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(name, tcp).await.unwrap();
        let (_, conn) = tls.get_ref();
        let server_leaf = conn
            .peer_certificates()
            .and_then(|c| c.first())
            .map(|c| c.as_ref().to_vec())
            .expect("server presented a certificate");
        // The client verifies the server leaf back to a coordinator profile.
        verify_leaf(&ca_for_client, &server_leaf)
            .expect("server leaf verifies")
            .profile
    });

    // Server side: the captured client leaf verifies to the agent profile.
    let stream = tokio::time::timeout(std::time::Duration::from_secs(5), incoming.next())
        .await
        .expect("accept within timeout")
        .expect("an incoming connection")
        .expect("handshake ok");
    let (_, conn) = stream.get_ref();
    let peer = conn
        .peer_certificates()
        .and_then(|c| c.first())
        .map(|c| c.as_ref().to_vec())
        .expect("client cert captured server-side");
    let verified = verify_leaf(&ca.cert_pem, &peer).expect("client leaf verifies");
    assert_eq!(verified.profile, Profile::Agent(node));
    assert_eq!(
        verified.subject.common_name.as_deref(),
        Some(node.to_string().as_str()),
        "agent leaf CN is the node id string"
    );

    let client_saw = client.await.unwrap();
    assert_eq!(client_saw, Profile::Coordinator(machine));
}

// ---- enrollee-generated CSRs ----------------------------------------------

#[test]
fn generated_csr_round_trips_through_agent_issuance() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    let (key_pem, csr_pem) = generate_key_and_csr().unwrap();

    let node = NodeId::new();
    let leaf = issue_agent(&signer, &csr_pem, &node, &[]).unwrap();
    let verified = verify_leaf(&ca.cert_pem, &leaf).unwrap();
    assert_eq!(verified.profile, Profile::Agent(node));

    // The private key the enrollee kept must pair with the leaf it got back.
    let key = String::from_utf8(key_pem).unwrap();
    assert!(key.contains("PRIVATE KEY"), "{key}");
    let paths = TlsPaths {
        cert: "cert.pem".into(),
        key: "key.pem".into(),
        ca: "ca.pem".into(),
    };
    crate::TlsMaterial::from_pem(
        &paths,
        ca.cert_pem.clone(),
        leaf.clone(),
        key.clone().into_bytes(),
    )
    .expect("issued leaf pairs with the generated key");
}

#[test]
fn generated_csr_round_trips_through_coordinator_issuance() {
    let ca = mint_root_ca().unwrap();
    let signer = CaSigner::load(&ca.cert_pem, &ca.key_pem).unwrap();
    let (_key_pem, csr_pem) = generate_key_and_csr().unwrap();

    let machine = mint_machine_identity();
    let leaf = issue_coordinator(&signer, &csr_pem, &machine, &local_sans()).unwrap();
    let verified = verify_leaf(&ca.cert_pem, &leaf).unwrap();
    assert_eq!(verified.profile, Profile::Coordinator(machine));
}

#[test]
fn each_generated_csr_carries_a_fresh_key() {
    let (a_key, a_csr) = generate_key_and_csr().unwrap();
    let (b_key, b_csr) = generate_key_and_csr().unwrap();
    assert_ne!(a_key, b_key);
    assert_ne!(a_csr, b_csr);
}

// Keep an explicit reference to MachineId's Display form the classifier relies
// on, so a change to the id prefix breaks here rather than silently.
#[test]
fn machine_and_node_id_prefixes_are_what_the_classifier_expects() {
    assert_eq!(MachineId::PREFIX, "machine");
    assert_eq!(NodeId::PREFIX, "node");
}
