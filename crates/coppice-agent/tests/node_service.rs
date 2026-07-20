//! In-process mTLS integration test for the agent-hosted `NodeService`
//! (ADR 0034): the full stack — a throwaway CA, dual-EKU leaves, an id-pinned
//! client dial, and mandatory client certificates — over a real tonic server
//! reading from a real [`FilesystemSink`].
//!
//! The server leaf carries the node id's typed string as a dNSName SAN, and the
//! client dials with that string as the TLS server-name, proving id-pinned
//! dialing works: a coordinator reaches the node by its identity, not its
//! network name. Client certs are mandatory, so a certless dial is rejected at
//! the handshake.

use std::time::Duration as StdDuration;

use coppice_agent::node_service::{serve, NodeServiceListener};
use coppice_agent::telemetry::{
    FilesystemSink, FilesystemSinkOptions, LogChunk, LogSink, LogStream,
};
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::time::Timestamp;
use coppice_net::node_service::Client;
use coppice_proto::pb::agent::v1 as pb;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

/// One PEM cert/key pair.
struct CertKey {
    cert: Vec<u8>,
    key: Vec<u8>,
}

/// A throwaway CA plus a server and client leaf, mirroring `coppice dev`'s
/// `mint_pki`: both leaves carry the dual `ServerAuth`+`ClientAuth` EKU (a node
/// leaf must be usable in both TLS roles, ADR 0034), and the server leaf gets a
/// dNSName SAN of the node id typed string so the client can pin it.
struct Pki {
    ca_pem: Vec<u8>,
    server: CertKey,
    client: CertKey,
}

fn mint_pki(node_id: NodeId) -> Pki {
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "coppice-test-ca");
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign ca");

    let leaf = |cn: &str, sans: Vec<String>| -> CertKey {
        let key = KeyPair::generate().expect("leaf key");
        let mut params = CertificateParams::new(sans).expect("leaf params");
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
            .signed_by(&key, &ca_cert, &ca_key)
            .expect("sign leaf");
        CertKey {
            cert: cert.pem().into_bytes(),
            key: key.serialize_pem().into_bytes(),
        }
    };

    Pki {
        ca_pem: ca_cert.pem().into_bytes(),
        // The server leaf's SAN is the typed node id string — the client pins it.
        server: leaf("node-service-server", vec![node_id.to_string()]),
        client: leaf("coppice-test-coordinator", vec!["coordinator".to_string()]),
    }
}

/// A running `NodeService` and everything a client needs to dial it.
struct Harness {
    node_id: NodeId,
    endpoint: String,
    ca_pem: Vec<u8>,
    client: CertKey,
    // Kept alive: the sink's segment tree and the server task must outlive the test.
    _tempdir: tempfile::TempDir,
    _server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Build a sink, seed it with `chunks`, bind the mTLS listener on an ephemeral
/// port, and start serving.
async fn spawn(chunks: Vec<LogChunk>) -> Harness {
    let node_id = NodeId::new();
    let pki = mint_pki(node_id);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let sink = FilesystemSink::new(FilesystemSinkOptions::new(tempdir.path().join("tel")))
        .await
        .expect("build sink");
    if !chunks.is_empty() {
        LogSink::append(&sink, &chunks).await;
    }

    let listener = NodeServiceListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &pki.server.cert,
        &pki.server.key,
        &pki.ca_pem,
    )
    .expect("bind listener");
    let addr = listener.local_addr();
    let server = serve(listener, Some(sink));

    Harness {
        node_id,
        endpoint: format!("127.0.0.1:{}", addr.port()),
        ca_pem: pki.ca_pem,
        client: pki.client,
        _tempdir: tempdir,
        _server: server,
    }
}

/// Dial the harness, pinning the server's node id as the TLS server-name and
/// presenting the client leaf as the client identity (or none when `identity`
/// is false, to exercise the mandatory-client-cert gate).
async fn dial(harness: &Harness, identity: bool) -> Result<Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&harness.ca_pem))
        // Pin the node id typed string, NOT the network host (127.0.0.1).
        .domain_name(harness.node_id.to_string());
    if identity {
        tls = tls.identity(Identity::from_pem(
            &harness.client.cert,
            &harness.client.key,
        ));
    }
    // Retry the connect briefly: the socket is bound before `serve` starts
    // accepting, so the first handshake can race the server task's startup.
    let mut last_err = None;
    for _ in 0..50 {
        match Channel::from_shared(format!("https://{}", harness.endpoint))
            .expect("valid uri")
            .tls_config(tls.clone())
            .expect("client tls")
            .connect()
            .await
        {
            Ok(channel) => return Ok(channel),
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(StdDuration::from_millis(20)).await;
            }
        }
    }
    Err(last_err.expect("at least one attempt"))
}

fn request(job: JobId, attempt: AttemptId) -> pb::FetchLogsRequest {
    pb::FetchLogsRequest {
        job: Some(job.into()),
        attempt: Some(attempt.into()),
        from_us: None,
        until_us: None,
        stream: pb::LogStream::Unspecified as i32,
        resume: None,
        ascending: true,
        max_chunks: 100,
        max_bytes: 1 << 20,
    }
}

fn chunk(
    job: JobId,
    attempt: AttemptId,
    alloc: AllocationId,
    stream: LogStream,
    bytes: &[u8],
) -> LogChunk {
    LogChunk {
        allocation: alloc,
        attempt,
        job,
        at: Timestamp::now(),
        stream,
        bytes: bytes::Bytes::copy_from_slice(bytes),
    }
}

#[tokio::test]
async fn happy_path_returns_the_stored_chunks() {
    let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
    let harness = spawn(vec![
        chunk(job, attempt, alloc, LogStream::Stdout, b"hello "),
        chunk(job, attempt, alloc, LogStream::Stderr, b"world"),
    ])
    .await;

    let channel = dial(&harness, true).await.expect("id-pinned mTLS dial");
    let mut client = Client::new(channel);
    let response = client
        .fetch_logs(request(job, attempt))
        .await
        .expect("fetch_logs")
        .into_inner();

    match response.outcome {
        Some(pb::fetch_logs_response::Outcome::Chunks(chunks)) => {
            assert_eq!(chunks.chunks.len(), 2, "both stored chunks returned");
            assert!(chunks.exhausted, "the whole attempt fit in one page");
            let joined: Vec<u8> = chunks
                .chunks
                .iter()
                .flat_map(|c| c.payload.clone())
                .collect();
            assert_eq!(joined, b"hello world", "in ascending (at, insertion) order");
        }
        other => panic!("expected Chunks, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_attempt_when_the_store_has_no_data() {
    // A server with an empty store: every attempt is unknown.
    let harness = spawn(Vec::new()).await;
    let channel = dial(&harness, true).await.expect("mTLS dial");
    let mut client = Client::new(channel);

    let response = client
        .fetch_logs(request(JobId::new(), AttemptId::new()))
        .await
        .expect("fetch_logs")
        .into_inner();
    assert!(
        matches!(
            response.outcome,
            Some(pb::fetch_logs_response::Outcome::UnknownAttempt(_))
        ),
        "an attempt with no telemetry directory answers UnknownAttempt"
    );
}

#[tokio::test]
async fn a_client_without_a_certificate_is_rejected_at_the_handshake() {
    let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
    let harness = spawn(vec![chunk(
        job,
        attempt,
        alloc,
        LogStream::Stdout,
        b"secret",
    )])
    .await;

    // No client identity: `client_auth_optional(false)` must reject the peer.
    // The rejection can surface at connect or on the first RPC, depending on
    // when the handshake completes — both are acceptable, a successful fetch is
    // not.
    let rejected = match dial(&harness, false).await {
        Err(_) => true,
        Ok(channel) => Client::new(channel)
            .fetch_logs(request(job, attempt))
            .await
            .is_err(),
    };
    assert!(
        rejected,
        "a certless client must be refused by the mandatory-client-cert gate"
    );
}

#[tokio::test]
async fn a_leaf_with_a_different_node_id_san_fails_id_pinning() {
    // The stolen-`service_addr` threat (ADR 0034): an attacker stands up a
    // NodeService whose leaf is chain-valid under the SAME CA but vouches for a
    // DIFFERENT node id than the coordinator expects. The coordinator pins the
    // advertised (expected) node id as the TLS server-name, so chain validation
    // passing is not enough — the SAN mismatch must fail the dial closed.
    let actual_id = NodeId::new();
    let expected_id = NodeId::new();
    // The server leaf's SAN is `actual_id`; the client will pin `expected_id`.
    let pki = mint_pki(actual_id);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let sink = FilesystemSink::new(FilesystemSinkOptions::new(tempdir.path().join("tel")))
        .await
        .expect("build sink");
    let listener = NodeServiceListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &pki.server.cert,
        &pki.server.key,
        &pki.ca_pem,
    )
    .expect("bind listener");
    let addr = listener.local_addr();
    let server = serve(listener, Some(sink));

    // The harness advertises `expected_id`, which `dial` pins as the server-name
    // — but the leaf only carries `actual_id` as its SAN.
    let harness = Harness {
        node_id: expected_id,
        endpoint: format!("127.0.0.1:{}", addr.port()),
        ca_pem: pki.ca_pem,
        client: pki.client,
        _tempdir: tempdir,
        _server: server,
    };
    assert_ne!(actual_id, expected_id, "the two ids must differ");

    // The mismatch can surface at connect or on the first RPC depending on when
    // the handshake completes — both fail closed; a successful fetch does not.
    let rejected = match dial(&harness, true).await {
        Err(_) => true,
        Ok(channel) => Client::new(channel)
            .fetch_logs(request(JobId::new(), AttemptId::new()))
            .await
            .is_err(),
    };
    assert!(
        rejected,
        "a leaf whose SAN is a different node id must fail id-pinned dialing"
    );
}
