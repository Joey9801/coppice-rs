//! A real enrollment endpoint to point the client at.
//!
//! The posture matrix is about what happens on the wire, so nothing here is a
//! mock: [`Stub`] is a listener that speaks HTTP/1.1 (optionally under a real
//! rustls acceptor with a throwaway certificate) and answers the enrollment
//! contract. Every socket is wrapped in [`Recording`], which keeps a copy of
//! every byte the *client* sent at the TCP layer — before TLS, if TLS is on.
//! That is what lets a test assert the strong form of "verification precedes
//! the token": not that the client reported an error, but that the token never
//! reached the socket.
//!
//! Shared by more than one test binary, each of which uses a different part of
//! it; `dead_code` is allowed for that reason and no other.
#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

// ---------------------------------------------------------------------------
// Throwaway PKI
// ---------------------------------------------------------------------------

/// A self-signed CA that signs server certificates for the stubs. Nothing to do
/// with a cluster CA — it stands in for the *public* certificate an enrollment
/// endpoint is fronted by, which is why the client must be told to trust it
/// through the test-only root seam.
pub struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
    pub pem: Vec<u8>,
}

impl TestCa {
    pub fn new() -> TestCa {
        let key = KeyPair::generate().expect("CA key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "coppice-enroll-test-ca");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let cert = params.self_signed(&key).expect("self-sign");
        let pem = cert.pem().into_bytes();
        TestCa { cert, key, pem }
    }

    /// A server certificate valid for `sans`.
    pub fn server_cert(&self, sans: &[&str]) -> (Vec<u8>, Vec<u8>) {
        let key = KeyPair::generate().expect("leaf key");
        let mut params =
            CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<String>>())
                .expect("leaf params");
        params
            .distinguished_name
            .push(DnType::CommonName, sans.first().copied().unwrap_or("stub"));
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
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

/// A rustls server config for `(cert_pem, key_pem)`, ALPN pinned to HTTP/1.1 so
/// the stub never has to speak h2.
pub fn server_config(cert_pem: &[u8], key_pem: &[u8]) -> Arc<ServerConfig> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut io::Cursor::new(cert_pem))
        .collect::<Result<_, _>>()
        .expect("decode certs");
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut io::Cursor::new(key_pem))
        .expect("decode key")
        .expect("a key");
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("server config");
    config.alpn_protocols.push(b"http/1.1".to_vec());
    Arc::new(config)
}

// ---------------------------------------------------------------------------
// The byte recorder
// ---------------------------------------------------------------------------

/// A `TcpStream` that keeps a copy of everything read from it. Sits *under* the
/// TLS layer, so what it records is whatever the client actually put on the
/// wire.
pub struct Recording {
    inner: TcpStream,
    log: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for Recording {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &polled {
            let fresh = &buf.filled()[before..];
            if !fresh.is_empty() {
                this.log.lock().expect("log").extend_from_slice(fresh);
            }
        }
        polled
    }
}

impl AsyncWrite for Recording {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// ---------------------------------------------------------------------------
// The stub endpoint
// ---------------------------------------------------------------------------

/// A running stub enrollment endpoint.
pub struct Stub {
    pub addr: SocketAddr,
    /// Every byte the client sent at the TCP layer, across all connections.
    raw: Arc<Mutex<Vec<u8>>>,
    /// Every fully-read HTTP request, decrypted. Empty when no handshake
    /// completed.
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Stub {
    /// The base URL to configure as `enrollment.endpoint`. `host` decides the
    /// name the client verifies against, which is how the hostname-mismatch
    /// case is expressed.
    pub fn endpoint(&self, scheme: &str, host: &str) -> String {
        format!("{scheme}://{host}:{}", self.addr.port())
    }

    pub fn raw_bytes(&self) -> Vec<u8> {
        self.raw.lock().expect("raw log").clone()
    }

    pub fn requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().expect("requests").clone()
    }
}

/// Start a stub that answers every request with `body`, under `tls` when given.
pub async fn spawn(tls: Option<Arc<ServerConfig>>, body: String) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let raw = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));

    let task = tokio::spawn({
        let raw = Arc::clone(&raw);
        let requests = Arc::clone(&requests);
        async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let socket = Recording {
                    inner: socket,
                    log: Arc::clone(&raw),
                };
                let tls = tls.clone();
                let body = body.clone();
                let requests = Arc::clone(&requests);
                tokio::spawn(async move {
                    match tls {
                        // A handshake that fails here is the point of the
                        // untrusted-endpoint tests: the connection dies with the
                        // recorder holding only handshake bytes.
                        Some(config) => {
                            if let Ok(stream) = TlsAcceptor::from(config).accept(socket).await {
                                let _ = serve_http(stream, &body, &requests).await;
                            }
                        }
                        None => {
                            let _ = serve_http(socket, &body, &requests).await;
                        }
                    }
                });
            }
        }
    });

    Stub {
        addr,
        raw,
        requests,
        _task: task,
    }
}

/// Read one HTTP/1.1 request (headers plus a `Content-Length` body) and answer
/// `200` with `body`. Deliberately minimal: the enrollment contract is one POST
/// with a JSON body, and a full server would only obscure what is asserted.
async fn serve_http<S>(
    mut stream: S,
    body: &str,
    requests: &Arc<Mutex<Vec<Vec<u8>>>>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..n]);
        if let Some(end) = find_headers_end(&request) {
            let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
            let expected = content_length(&headers).unwrap_or(0);
            if request.len() >= end + 4 + expected {
                break;
            }
        }
    }
    requests.lock().expect("requests").push(request);

    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Option<usize> {
    headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
}

/// The body a happy-path stub returns: a real cluster CA and a real agent leaf
/// for `node`, so the caller can verify what got installed.
pub fn issued_body(node: coppice_core::id::NodeId) -> (String, Vec<u8>) {
    use coppice_tls::pki;

    let ca = pki::mint_root_ca().expect("mint a cluster CA");
    let signer = pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the CA");
    let (cert_pem, _key_pem) = pki::mint_agent_local(&signer, &node, &[]).expect("mint a leaf");
    let response = coppice_enroll::EnrollResponse {
        cert_pem: String::from_utf8(cert_pem).expect("PEM is UTF-8"),
        ca_pem: String::from_utf8(ca.cert_pem.clone()).expect("PEM is UTF-8"),
    };
    (
        serde_json::to_string(&response).expect("serialize"),
        ca.cert_pem,
    )
}
