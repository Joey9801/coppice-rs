//! The CA private key never appears in a snapshot (ADR 0037 §4).
//!
//! Only the CA *certificate* is replicated state; the private key resides on
//! voter disks and the *fact* of possession is replicated, the key never is.
//! This test mints a real keypair + CA with rcgen, records the certificate in
//! a `StateMachine`, encodes it through both snapshot paths (the slice encoder
//! `encode_state` and the streaming `write_state_direct`), and asserts the raw
//! container bytes carry the certificate but never the private key — neither
//! its PEM body nor its raw DER encoding.

use std::path::Path;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

use coppice_consensus::fs::{Fs, FsFile, RealFs};
use coppice_consensus::storage::raw::{encode_state, write_state_direct};
use coppice_proto::convert::state_to_records;
use coppice_proto::pb::storage::v1::SnapshotMeta;
use coppice_state::command::RecordCaCertificate;
use coppice_state::{Command, StateMachine};

fn bare_meta() -> SnapshotMeta {
    SnapshotMeta {
        history_id: vec![7; 16],
        snapshot_id: "secrecy".to_string(),
        last_applied: None,
        membership: None,
        cluster_version: 1,
        shard_count: 2,
    }
}

/// Whether `needle` appears as a contiguous subsequence of `haystack`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn ca_private_key_never_appears_in_a_snapshot() {
    // A real CA: self-signed, with its own keypair.
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "coppice-secrecy-ca");
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign ca");

    let cert_pem = ca_cert.pem();
    let key_pem = ca_key.serialize_pem();
    let key_der = ca_key.serialize_der();

    // The private-key PEM body as one contiguous base64 blob (headers and line
    // wrapping stripped). The full encoding includes the private scalar, so —
    // unlike an individual wrapped line, which for an ECDSA PKCS#8 key can
    // collide with the public-key material the certificate legitimately shares
    // — this whole blob can only appear if the key itself leaked.
    let key_body: String = key_pem
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .flat_map(|l| l.trim().chars())
        .collect();
    assert!(key_body.len() > 40, "key PEM must have a base64 body");

    // Record only the public certificate — the replicated fact.
    let mut state = StateMachine::default();
    state
        .apply(&Command::RecordCaCertificate(RecordCaCertificate {
            bundle: coppice_state::CaCertBundle::parse(cert_pem.clone())
                .expect("a real CA cert PEM is a valid bundle"),
            recorded_at: coppice_core::time::Timestamp::from_micros(1_760_000_000_000_000)
                .expect("in range"),
        }))
        .expect("record CA accepted");
    assert!(state.ca.is_some());

    // Path 1: the slice encoder.
    let slice_bytes = encode_state(&bare_meta(), &state_to_records(&state), 2);

    // Path 2: the streaming state-direct writer.
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = RealFs::new(dir.path());
    let mut file = fs.create_new(Path::new("c.snap")).expect("create");
    write_state_direct(&mut file, &bare_meta(), &state, 2).expect("write_state_direct");
    let len = file.len().expect("len") as usize;
    let mut direct_bytes = vec![0u8; len];
    file.read_exact_at(0, &mut direct_bytes).expect("read back");

    for (label, bytes) in [
        ("encode_state", &slice_bytes),
        ("write_state_direct", &direct_bytes),
    ] {
        // Teeth: the certificate *is* present, so a false-absence would fail.
        assert!(
            contains(bytes, cert_pem.as_bytes()),
            "{label}: CA certificate should be present in the snapshot"
        );
        // The private key must never appear — not its DER bytes...
        assert!(
            !contains(bytes, &key_der),
            "{label}: CA private key DER leaked into the snapshot"
        );
        // ...and not its PEM body.
        assert!(
            !contains(bytes, key_body.as_bytes()),
            "{label}: CA private key PEM body leaked into the snapshot"
        );
    }
}
