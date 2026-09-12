//! The agent's `[enrollment]` table (ADR 0037 §4/§8).
//!
//! Its own test binary because of the last test here: `tracing` caches callsite
//! interest globally, so a capture assertion sharing a process with tests that
//! hit the same log statements without a subscriber can come back empty and
//! prove nothing.

use std::io::Write;

use coppice_testkit::tracing_capture::{assert_no_secret, capture};

const TOKEN: &str = "cpk_agent_startup_secret";

/// A minimal but complete agent config, with `[enrollment]` spliced in.
///
/// `source = "cluster"` because every test below that splices a table in is
/// about enrollment, and enrollment is what cluster-managed material means
/// (issue #127): the two sections only make sense together.
fn config_with(enrollment: &str) -> String {
    format!(
        r#"
data_dir = "/var/lib/coppice-agent"

[discovery]
backend = "static"

[discovery.static]
addrs = ["coord-1.example.com:7072"]

[tls]
source = "cluster"

{enrollment}
"#
    )
}

/// The same config with externally-provisioned material, naming three files
/// that really exist — the agent reads and parses them at load.
fn external_config(dir: &std::path::Path, enrollment: &str) -> String {
    let paths = seed_external_material(dir);
    format!(
        r#"
data_dir = "/var/lib/coppice-agent"

[discovery]
backend = "static"

[discovery.static]
addrs = ["coord-1.example.com:7072"]

[tls]
source = "external"
cert_path = "{cert}"
key_path  = "{key}"
ca_path   = "{ca}"

{enrollment}
"#,
        cert = paths.cert.display(),
        key = paths.key.display(),
        ca = paths.ca.display(),
    )
}

/// A throwaway root and agent leaf on disk: external provenance is only
/// honest if the daemon actually loads what it was pointed at.
fn seed_external_material(dir: &std::path::Path) -> coppice_tls::TlsPaths {
    let ca = coppice_tls::pki::mint_root_ca().expect("mint a throwaway root");
    let signer =
        coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
    let (cert, key) = coppice_tls::pki::mint_agent_local(
        &signer,
        &coppice_core::id::NodeId::new(),
        &["node-1.example.com".to_string()],
    )
    .expect("mint a throwaway leaf");
    let paths = coppice_tls::TlsPaths {
        cert: dir.join("node.crt"),
        key: dir.join("node.key"),
        ca: dir.join("ca.crt"),
    };
    std::fs::write(&paths.cert, &cert).expect("write cert");
    std::fs::write(&paths.key, &key).expect("write key");
    std::fs::write(&paths.ca, &ca.cert_pem).expect("write ca");
    paths
}

fn load(contents: &str) -> anyhow::Result<coppice_agent::config::Config> {
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    file.write_all(contents.as_bytes()).expect("write");
    coppice_agent::config::load(file.path())
}

/// The `[tls]` source and `[enrollment]` state one thing between them
/// (issue #127): cluster-managed material is obtained by enrolling, and
/// externally-provisioned material is never written by this agent at all.
#[test]
fn enrollment_is_required_under_cluster_and_refused_under_external() {
    let dir = tempfile::tempdir().expect("tempdir");
    let enrollment = r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
token_path = "/run/secrets/coppice-enroll-token"
"#;

    let config = load(&config_with(enrollment)).expect("cluster + enrollment is the normal shape");
    assert_eq!(
        config.tls_source(),
        coppice_agent::config::TlsSource::Cluster
    );
    assert_eq!(
        config.tls_paths().cert,
        std::path::Path::new("/var/lib/coppice-agent/pki/node.crt")
    );

    let err =
        load(&config_with("")).expect_err("cluster without [enrollment] cannot obtain a leaf");
    assert!(format!("{err:#}").contains("[enrollment]"), "{err:#}");

    let config = load(&external_config(dir.path(), ""))
        .expect("external without [enrollment] is the out-of-band shape");
    assert_eq!(
        config.tls_source(),
        coppice_agent::config::TlsSource::External
    );

    let err = load(&external_config(dir.path(), enrollment))
        .expect_err("external + [enrollment] states two provenances at once");
    let rendered = format!("{err:#}");
    assert!(rendered.contains("[enrollment]"), "{rendered}");
    assert!(rendered.contains("source = \"cluster\""), "{rendered}");
}

/// External provenance is fail-stop when the material is not there: the agent
/// has no enrollment fallback to reach for, so a mistyped path must not look
/// like a healthy startup.
#[test]
fn external_material_must_exist_at_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let contents = external_config(dir.path(), "");
    std::fs::remove_file(dir.path().join("node.key")).expect("remove the key");
    let err = load(&contents).expect_err("a missing external file must fail at load");
    assert!(format!("{err:#}").contains("node.key"), "{err:#}");
}

/// Cluster provenance owns the layout, so naming a path there is a config
/// error rather than a setting that quietly loses.
#[test]
fn cluster_source_rejects_every_path_key() {
    for key in ["cert_path", "key_path", "ca_path"] {
        let contents = config_with(&format!(
            "{key} = \"/etc/coppice/x.pem\"\n\n[enrollment]\n             endpoint = \"https://coppice.example.com:7070\"\n             token_path = \"/run/secrets/coppice-enroll-token\"\n"
        ));
        let err = load(&contents).expect_err("a path under cluster provenance must fail");
        let rendered = format!("{err:#}");
        assert!(rendered.contains(key), "{rendered}");
        assert!(rendered.contains("source = \"external\""), "{rendered}");
    }
}

#[test]
fn a_token_path_and_an_https_endpoint_is_the_production_shape() {
    let config = load(&config_with(
        r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
token_path = "/run/secrets/coppice-enroll-token"
"#,
    ))
    .expect("valid");
    let enrollment = config.enrollment.expect("the table parsed");
    assert_eq!(enrollment.endpoint, "https://coppice.example.com:7070");
    assert_eq!(enrollment.token_kind(), "path");
    assert!(!enrollment.insecure, "insecure defaults off");
}

#[test]
fn a_cleartext_endpoint_without_the_opt_in_fails_at_startup() {
    let error = load(&config_with(
        r#"
[enrollment]
endpoint = "http://10.0.0.1:7070"
token = "cpk_dev"
"#,
    ))
    .expect_err("a cleartext endpoint needs the conspicuous opt-in");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("enrollment.insecure"), "{rendered}");
}

#[test]
fn a_cleartext_endpoint_with_the_opt_in_loads() {
    let config = load(&config_with(
        r#"
[enrollment]
endpoint = "http://10.0.0.1:7070"
token = "cpk_dev"
insecure = true
"#,
    ))
    .expect("the opt-in is what makes it valid");
    assert!(config.enrollment.expect("the table parsed").insecure);
}

#[test]
fn exactly_one_token_form_is_required() {
    let neither = load(&config_with(
        r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
"#,
    ))
    .expect_err("a token is required");
    assert!(format!("{neither:#}").contains("token_path"), "{neither:#}");

    let both = load(&config_with(
        r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
token = "cpk_dev"
token_path = "/run/secrets/token"
"#,
    ))
    .expect_err("both forms is ambiguous");
    assert!(format!("{both:#}").contains("not both"), "{both:#}");
}

#[test]
fn an_unknown_key_in_the_table_fail_stops() {
    load(&config_with(
        r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
token = "cpk_dev"
retries = 3
"#,
    ))
    .expect_err("deny_unknown_fields catches a typo'd knob");
}

#[test]
fn the_startup_log_names_the_endpoint_and_never_the_token() {
    let config = load(&config_with(&format!(
        r#"
[enrollment]
endpoint = "https://coppice.example.com:7070"
token = "{TOKEN}"
"#
    )))
    .expect("valid");

    let (_, captured) = capture(|| config.log_effective());

    assert!(
        captured.contains("coppice.example.com"),
        "the endpoint is logged: {captured}"
    );
    assert!(
        captured.contains("token_source=\"inline\"") || captured.contains("token_source=inline"),
        "the *kind* of token source is logged: {captured}"
    );
    assert_no_secret(&captured, "cpk_");
}
