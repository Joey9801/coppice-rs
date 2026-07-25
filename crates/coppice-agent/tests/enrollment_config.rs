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
fn config_with(enrollment: &str) -> String {
    format!(
        r#"
node_id = "node-5f0e6e6a-9c2a-4b8e-9a2b-1f4b6c8d9e10"
data_dir = "/var/lib/coppice-agent"
coordinators = ["coord-1.example.com:7072"]

[tls]
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[capacity]
cpu_millis = 32000
memory = "128GiB"
disk = "1TiB"

{enrollment}
"#
    )
}

fn load(contents: &str) -> anyhow::Result<coppice_agent::config::Config> {
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    file.write_all(contents.as_bytes()).expect("write");
    coppice_agent::config::load(file.path())
}

#[test]
fn the_table_is_optional() {
    let config = load(&config_with("")).expect("a config without [enrollment] is valid");
    assert!(
        config.enrollment.is_none(),
        "an agent whose leaf is provisioned out of band configures no enrollment"
    );
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
