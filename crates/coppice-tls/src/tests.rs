//! Reload-store and connection-time-acceptor tests.
//!
//! The reload-semantics tests are pure and fast. The handshake tests drive the
//! real [`serve`] acceptor with a raw [`tokio_rustls`] client (no gRPC needed
//! at this layer) to prove three things the coordinator relies on: client auth
//! is mandatory, a valid client cert is captured on the server side (the
//! `peer_certs` foundation), and a leaf rotation is observed by a fresh
//! connection while the store's paths are unchanged.

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;

use super::*;

// ---- fixtures -------------------------------------------------------------

/// A self-signed CA that can sign server+client leaves, mirroring the
/// coordinator test harness.
struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
    pem: Vec<u8>,
}

impl Ca {
    fn new() -> Ca {
        let key = KeyPair::generate().expect("ca key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "coppice-tls-test-ca");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let cert = params.self_signed(&key).expect("self-sign ca");
        let pem = cert.pem().into_bytes();
        Ca { cert, key, pem }
    }

    /// Issue a server+client leaf with the given CN and `localhost` SANs.
    fn leaf(&self, cn: &str) -> (Vec<u8>, Vec<u8>) {
        let key = KeyPair::generate().expect("leaf key");
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .expect("leaf params");
        params.distinguished_name.push(DnType::CommonName, cn);
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        let cert = params
            .signed_by(&key, &self.cert, &self.key)
            .expect("sign leaf");
        (cert.pem().into_bytes(), key.serialize_pem().into_bytes())
    }
}

/// Lay `cert`/`key`/`ca` PEM into `dir` and return the [`TlsPaths`].
fn write_material(dir: &Path, cert: &[u8], key: &[u8], ca: &[u8]) -> TlsPaths {
    let paths = TlsPaths {
        cert: dir.join("node.crt"),
        key: dir.join("node.key"),
        ca: dir.join("ca.crt"),
    };
    std::fs::write(&paths.cert, cert).unwrap();
    std::fs::write(&paths.key, key).unwrap();
    std::fs::write(&paths.ca, ca).unwrap();
    paths
}

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

/// A rustls client config trusting `ca_pem`, optionally presenting a client
/// leaf. `None` client material exercises the mandatory-client-auth refusal.
fn client_config(ca_pem: &[u8], client: Option<(&[u8], &[u8])>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    for c in parse_certs(ca_pem) {
        roots.add(c).unwrap();
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    match client {
        Some((cert, key)) => builder
            .with_client_auth_cert(parse_certs(cert), parse_key(key))
            .unwrap(),
        None => builder.with_no_client_auth(),
    }
}

// ---- reload semantics -----------------------------------------------------

#[test]
fn reload_is_a_noop_when_nothing_changed() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert, key) = ca.leaf("node-a");
    let paths = write_material(dir.path(), &cert, &key, &ca.pem);

    let store = TlsStore::load(paths).unwrap();
    assert!(!store.reload().unwrap(), "unchanged files must not swap");
    assert!(!store.reload().unwrap());
}

#[test]
fn reload_swaps_in_a_rotated_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert1, key1) = ca.leaf("node-a");
    let paths = write_material(dir.path(), &cert1, &key1, &ca.pem);

    let store = TlsStore::load(paths.clone()).unwrap();
    let before = store.current().cert_pem().to_vec();

    // Rotate the leaf (same CA). A rewrite bumps mtime; the sleep guards
    // against coarse-mtime filesystems.
    std::thread::sleep(Duration::from_millis(10));
    let (cert2, key2) = ca.leaf("node-a");
    std::fs::write(&paths.cert, &cert2).unwrap();
    std::fs::write(&paths.key, &key2).unwrap();

    assert!(store.reload().unwrap(), "a rotated leaf must swap in");
    assert_eq!(store.current().cert_pem(), cert2.as_slice());
    assert_ne!(store.current().cert_pem(), before.as_slice());
}

#[test]
fn broken_pem_keeps_the_old_material() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert1, key1) = ca.leaf("node-a");
    let paths = write_material(dir.path(), &cert1, &key1, &ca.pem);

    let store = TlsStore::load(paths.clone()).unwrap();
    let good = store.current().cert_pem().to_vec();

    // A half-written cert file: valid PEM framing gone.
    std::thread::sleep(Duration::from_millis(10));
    std::fs::write(&paths.cert, b"-----BEGIN CERTIFICATE-----\nnot base64\n").unwrap();

    let err = store.reload().unwrap_err();
    assert!(matches!(err, TlsError::Cert { .. }), "got {err:?}");
    // Old material still serves.
    assert_eq!(store.current().cert_pem(), good.as_slice());

    // Fixing the file lets the next reload succeed (the broken write did not
    // latch the fingerprint).
    std::thread::sleep(Duration::from_millis(10));
    let (cert2, key2) = ca.leaf("node-a");
    std::fs::write(&paths.cert, &cert2).unwrap();
    std::fs::write(&paths.key, &key2).unwrap();
    assert!(store.reload().unwrap());
    assert_eq!(store.current().cert_pem(), cert2.as_slice());
}

#[test]
fn not_after_is_parsed_for_the_gauge() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert, key) = ca.leaf("node-a");
    let paths = write_material(dir.path(), &cert, &key, &ca.pem);
    let store = TlsStore::load(paths).unwrap();
    let not_after = store.current().not_after_unix().expect("leaf notAfter");
    // rcgen's default validity is in the future.
    assert!(not_after > 0);
}

#[test]
fn missing_file_fails_load() {
    let dir = tempfile::tempdir().unwrap();
    let paths = TlsPaths {
        cert: dir.path().join("absent.crt"),
        key: dir.path().join("absent.key"),
        ca: dir.path().join("absent.ca"),
    };
    assert!(matches!(TlsStore::load(paths), Err(TlsError::Read { .. })));
}

// ---- connection-time acceptor --------------------------------------------

/// Drive one client handshake to `addr`, returning the server leaf the client
/// saw (its first peer certificate DER).
async fn client_handshake_server_cert(
    addr: std::net::SocketAddr,
    config: ClientConfig,
) -> std::io::Result<Vec<u8>> {
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await?;
    let name = ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(name, tcp).await?;
    let (_, conn) = tls.get_ref();
    let cert = conn
        .peer_certificates()
        .and_then(|c| c.first())
        .map(|c| c.as_ref().to_vec())
        .expect("server presented a certificate");
    Ok(cert)
}

#[tokio::test]
async fn valid_client_cert_completes_and_server_captures_the_peer_cert() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert, key) = ca.leaf("server");
    let paths = write_material(dir.path(), &cert, &key, &ca.pem);
    let store = TlsStore::load(paths).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = serve(listener, store);

    let (client_cert, client_key) = ca.leaf("client-42");
    let cfg = client_config(&ca.pem, Some((&client_cert, &client_key)));
    let client = tokio::spawn(client_handshake_server_cert(addr, cfg));

    // The server side yields the established stream; the client's leaf is
    // captured on it — this is exactly the DER `request.peer_certs()` exposes.
    let stream = tokio::time::timeout(Duration::from_secs(5), incoming.next())
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
    assert_eq!(peer, parse_certs(&client_cert)[0].as_ref());

    // And the client saw our server leaf.
    let server_cert = client.await.unwrap().expect("client handshake ok");
    assert_eq!(server_cert, parse_certs(&cert)[0].as_ref());
}

#[tokio::test]
async fn missing_client_cert_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert, key) = ca.leaf("server");
    let paths = write_material(dir.path(), &cert, &key, &ca.pem);
    let store = TlsStore::load(paths).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _incoming = serve(listener, store);

    // No client cert: the mandatory verifier refuses. Under TLS 1.3 the client
    // finishes its half before the server's rejection alert arrives, so the
    // failure surfaces on the connect OR on the first read — either proves the
    // connection is refused (a served connection would read a graceful stream,
    // not an error).
    let cfg = client_config(&ca.pem, None);
    let connector = TlsConnector::from(Arc::new(cfg));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let refused = match connector.connect(name, tcp).await {
        Err(_) => true,
        Ok(mut tls) => {
            let mut buf = [0u8; 1];
            tls.read(&mut buf).await.is_err()
        }
    };
    assert!(refused, "no-client-cert handshake must be refused");
}

#[tokio::test]
async fn a_reloaded_leaf_is_served_to_new_connections() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert1, key1) = ca.leaf("server");
    let paths = write_material(dir.path(), &cert1, &key1, &ca.pem);
    let store = TlsStore::load(paths.clone()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = serve(listener, Arc::clone(&store));

    let (client_cert, client_key) = ca.leaf("client");
    let cfg = client_config(&ca.pem, Some((&client_cert, &client_key)));
    let seen_first = client_handshake_server_cert(addr, cfg).await.unwrap();
    // Consume the server-side stream so the accept task completes.
    let _ = incoming.next().await;
    assert_eq!(seen_first, parse_certs(&cert1)[0].as_ref());

    // Rotate the server leaf on disk (same CA) and reload the store.
    std::thread::sleep(Duration::from_millis(10));
    let (cert2, key2) = ca.leaf("server");
    std::fs::write(&paths.cert, &cert2).unwrap();
    std::fs::write(&paths.key, &key2).unwrap();
    assert!(store.reload().unwrap());

    // A fresh connection is served the new leaf.
    let cfg = client_config(&ca.pem, Some((&client_cert, &client_key)));
    let seen_second = client_handshake_server_cert(addr, cfg).await.unwrap();
    let _ = incoming.next().await;
    assert_eq!(seen_second, parse_certs(&cert2)[0].as_ref());
    assert_ne!(
        seen_first, seen_second,
        "new connection must see the rotated leaf"
    );
}

#[tokio::test]
async fn a_rotated_ca_is_served_to_new_connections() {
    // Leaf rotation onto a brand-new CA: a client trusting only the new CA
    // succeeds after reload, proving the trust root swaps too.
    let dir = tempfile::tempdir().unwrap();
    let ca1 = Ca::new();
    let (cert1, key1) = ca1.leaf("server");
    let paths = write_material(dir.path(), &cert1, &key1, &ca1.pem);
    let store = TlsStore::load(paths.clone()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = serve(listener, Arc::clone(&store));

    // Rotate onto a NEW ca: leaf, key, and CA all replaced.
    let ca2 = Ca::new();
    let (cert2, key2) = ca2.leaf("server");
    std::thread::sleep(Duration::from_millis(10));
    std::fs::write(&paths.cert, &cert2).unwrap();
    std::fs::write(&paths.key, &key2).unwrap();
    std::fs::write(&paths.ca, &ca2.pem).unwrap();
    assert!(store.reload().unwrap());

    // A client that presents a ca2 leaf and trusts ca2 connects; the server now
    // both serves the ca2 leaf and verifies the client against ca2.
    let (client_cert, client_key) = ca2.leaf("client");
    let cfg = client_config(&ca2.pem, Some((&client_cert, &client_key)));
    let seen = client_handshake_server_cert(addr, cfg).await.unwrap();
    let _ = incoming.next().await;
    assert_eq!(seen, parse_certs(&cert2)[0].as_ref());
}

#[tokio::test]
async fn reload_task_picks_up_a_change_by_polling() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let (cert1, key1) = ca.leaf("node-a");
    let paths = write_material(dir.path(), &cert1, &key1, &ca.pem);
    let store = TlsStore::load(paths.clone()).unwrap();
    let before = store.current().cert_pem().to_vec();

    let _task = spawn_reload_task(
        Arc::clone(&store),
        ReloadOptions {
            poll_interval: Duration::from_millis(20),
            sighup: false,
        },
    );

    std::thread::sleep(Duration::from_millis(10));
    let (cert2, key2) = ca.leaf("node-a");
    std::fs::write(&paths.cert, &cert2).unwrap();
    std::fs::write(&paths.key, &key2).unwrap();

    // Poll until the background task swaps (bounded).
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if store.current().cert_pem() == cert2.as_slice() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "reload task never swapped"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(store.current().cert_pem(), before.as_slice());
}
