//! Apply-contract tests for the cluster PKI / identity commands (ADR 0037 §4,
//! §5, §7): the accept/reject matrix for each command, the query surface, and
//! the enrollment-token TTL filter. These commands are administrative facts
//! with no event subscribers in v1, so — like `SetNodeSchedulable` — an
//! accepted apply emits no events.

mod common;

use common::*;
use coppice_core::id::{EnrollTokenId, MachineId};
use coppice_core::time::Timestamp;
use coppice_state::command::{
    BindMachineIdentity, ConfirmKeyPossession, MintEnrollToken, RebindMachineAddress,
    RecordCaCertificate, RecordEnrolledIdentity, RevokeEnrollToken, RevokeIdentity,
};
use coppice_state::{
    CaCertBundle, Command, EnrollRole, RejectionReason, RevokedIdentity, StateMachine,
};
use uuid::Uuid;

fn mid(n: u128) -> MachineId {
    MachineId(Uuid::from_u128(n))
}

fn tok(n: u128) -> EnrollTokenId {
    EnrollTokenId(Uuid::from_u128(n))
}

/// A real self-signed CA certificate PEM with `cn` as its CommonName, so
/// tests can tell two bundles apart. `parse` DER-validates every block, so
/// only genuine certificates pass.
fn ca_cert_pem(cn: &str) -> String {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, cn);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    params.self_signed(&key).unwrap().pem()
}

fn cert_bundle(cn: &str) -> CaCertBundle {
    CaCertBundle::parse(ca_cert_pem(cn)).unwrap()
}

fn record_ca(bundle: &CaCertBundle) -> Command {
    Command::RecordCaCertificate(RecordCaCertificate {
        bundle: bundle.clone(),
        recorded_at: base_ts(),
    })
}

fn bind(machine: MachineId, raft_node_id: u64, address: &str) -> Command {
    Command::BindMachineIdentity(BindMachineIdentity {
        machine,
        raft_node_id,
        address: address.into(),
        bound_at: base_ts(),
    })
}

fn mint(
    token: EnrollTokenId,
    hash: &str,
    role: EnrollRole,
    expires_at: Option<Timestamp>,
) -> Command {
    Command::MintEnrollToken(MintEnrollToken {
        token,
        hash: hash.into(),
        role,
        label: "l".into(),
        expires_at,
        minted_at: base_ts(),
    })
}

// ---- RecordCaCertificate ----

#[test]
fn record_ca_sets_then_replaces() {
    let mut sm = StateMachine::default();
    assert!(sm.ca.is_none());

    let (a, b) = (cert_bundle("rootA"), cert_bundle("rootB"));
    apply_ok(&mut sm, record_ca(&a));
    assert_eq!(sm.ca.as_ref().unwrap().bundle, a);

    // Replacement is re-rooting: the new bundle wholly supersedes the old.
    apply_ok(&mut sm, record_ca(&b));
    assert_eq!(sm.ca.as_ref().unwrap().bundle, b);
}

/// The key-can-never-enter-replicated-state guarantee is enforced at
/// [`CaCertBundle::parse`], the only construction path — a command carrying a
/// key cannot exist, so apply never sees one (ADR 0037 §4). The label alone
/// proves nothing: every block must DER-parse as a real X.509 CA certificate.
#[test]
fn ca_bundle_refuses_anything_but_ca_certificate_blocks() {
    use base64::Engine as _;
    use coppice_state::InvalidCaBundle;

    let ca = ca_cert_pem("root");

    // A private key alone.
    assert!(
        CaCertBundle::parse("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
            .is_err()
    );
    // A legitimate certificate with the key appended — the sneaky variant.
    assert!(CaCertBundle::parse(format!(
        "{ca}-----BEGIN EC PRIVATE KEY-----\nBBBB\n-----END EC PRIVATE KEY-----\n"
    ))
    .is_err());
    // Empty / garbage / truncated / base64-but-not-DER.
    assert!(CaCertBundle::parse("").is_err());
    assert!(CaCertBundle::parse("not pem at all").is_err());
    assert!(CaCertBundle::parse("-----BEGIN CERTIFICATE-----\nAAAA\n").is_err());
    assert!(matches!(
        CaCertBundle::parse("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"),
        Err(InvalidCaBundle::NotACertificate { index: 0 })
    ));
    // Content smuggled between two valid blocks.
    assert!(CaCertBundle::parse(format!("{ca}raw key bytes here\n{ca}")).is_err());

    // Private-key DER relabeled as a CERTIFICATE: the label is a lie the DER
    // parse catches (the P1 regression this validation exists for).
    let key_der = rcgen::KeyPair::generate().unwrap().serialize_der();
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key_der);
    let relabeled = format!("-----BEGIN CERTIFICATE-----\n{key_b64}\n-----END CERTIFICATE-----\n");
    assert!(matches!(
        CaCertBundle::parse(relabeled),
        Err(InvalidCaBundle::NotACertificate { index: 0 })
    ));

    // A genuine certificate that is not a CA (a leaf) is refused too: the
    // bundle is the cluster's trust-anchor set.
    let leaf = {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, "leaf");
        params.self_signed(&key).unwrap().pem()
    };
    assert!(matches!(
        CaCertBundle::parse(leaf),
        Err(InvalidCaBundle::NotACa { index: 0 })
    ));

    // A chain of CA certificates is fine.
    assert!(CaCertBundle::parse(format!("{ca}{}", ca_cert_pem("intermediate"))).is_ok());
}

// ---- BindMachineIdentity ----

#[test]
fn bind_replay_is_idempotent_but_address_change_is_refused() {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, bind(mid(1), 1, "10.0.0.1:7000"));
    let first = sm.machine_binding(&mid(1)).unwrap().clone();
    assert_eq!(first.raft_node_id, 1);
    assert_eq!(first.bound_at, base_ts());

    // Exact same (machine, node, address) again: an accepted no-op replay;
    // bound_at stays the first-binding instant.
    apply_ok(
        &mut sm,
        Command::BindMachineIdentity(BindMachineIdentity {
            machine: mid(1),
            raft_node_id: 1,
            address: "10.0.0.1:7000".into(),
            bound_at: ts(TS_US + 5_000_000),
        }),
    );
    let after = sm.machine_binding(&mid(1)).unwrap().clone();
    assert_eq!(after.address, "10.0.0.1:7000");
    assert_eq!(after.bound_at, base_ts(), "bound_at must not move");

    // Same pair at a different address: refused (ADR 0037 §7 — address
    // changes are operator set-address, never re-admission), state untouched.
    let err = sm
        .apply(&Command::BindMachineIdentity(BindMachineIdentity {
            machine: mid(1),
            raft_node_id: 1,
            address: "10.0.0.9:7000".into(),
            bound_at: ts(TS_US + 9_000_000),
        }))
        .unwrap_err();
    assert!(
        matches!(err, RejectionReason::MachineAddressConflict { .. }),
        "got {err:?}"
    );
    assert_eq!(sm.machine_binding(&mid(1)).unwrap(), &after);
}

#[test]
fn bind_rejects_machine_moving_to_a_different_node() {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, bind(mid(1), 1, "a"));
    let rej = sm.apply(&bind(mid(1), 2, "a")).unwrap_err();
    assert_eq!(
        rej,
        RejectionReason::MachineIdentityConflict {
            machine: mid(1),
            raft_node_id: 2,
        }
    );
    // Unchanged: the original binding stands.
    assert_eq!(sm.machine_binding(&mid(1)).unwrap().raft_node_id, 1);
}

// ---- RebindMachineAddress ----

fn rebind(raft_node_id: u64, address: &str, at_us_offset: i64) -> Command {
    Command::RebindMachineAddress(RebindMachineAddress {
        raft_node_id,
        address: address.into(),
        rebound_at: ts(TS_US + at_us_offset),
    })
}

#[test]
fn rebind_repoints_an_existing_binding_and_replays_as_a_noop() {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, bind(mid(1), 1, "10.0.0.1:7000"));

    // The operator set-address path: the binding follows the membership
    // change to the new address (ADR 0037 §6), and `bound_at` stays the
    // original admission instant — the fact it dates is the binding.
    apply_ok(&mut sm, rebind(1, "10.0.0.9:7000", 5_000_000));
    let binding = sm.machine_binding(&mid(1)).unwrap().clone();
    assert_eq!(binding.address, "10.0.0.9:7000");
    assert_eq!(binding.raft_node_id, 1);
    assert_eq!(binding.bound_at, base_ts(), "bound_at must not move");

    // Exact replay: accepted no-op, state unchanged.
    apply_ok(&mut sm, rebind(1, "10.0.0.9:7000", 9_000_000));
    assert_eq!(sm.machine_binding(&mid(1)).unwrap(), &binding);

    // And after the rebind, re-admission at the NEW address is the accepted
    // replay — this is the whole reason the command exists: the moved
    // daemon's convergence loop re-offers its (machine, seat) pair at the
    // new address on every restart, and must not be refused forever.
    apply_ok(
        &mut sm,
        Command::BindMachineIdentity(BindMachineIdentity {
            machine: mid(1),
            raft_node_id: 1,
            address: "10.0.0.9:7000".into(),
            bound_at: ts(TS_US + 12_000_000),
        }),
    );
    assert_eq!(
        sm.machine_binding(&mid(1)).unwrap().address,
        "10.0.0.9:7000"
    );
}

#[test]
fn rebind_refuses_a_seat_with_no_binding() {
    // Repoints only, never creates: a seat with no binding has nothing an
    // endpoint was ever verified against.
    let mut sm = StateMachine::default();
    let err = sm.apply(&rebind(1, "10.0.0.9:7000", 0)).unwrap_err();
    assert_eq!(
        err,
        RejectionReason::UnknownMachineBinding { raft_node_id: 1 }
    );
    assert!(sm.machine_binding(&mid(1)).is_none());
}

#[test]
fn bind_rejects_node_taken_by_a_different_machine() {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, bind(mid(1), 1, "a"));
    let rej = sm.apply(&bind(mid(2), 1, "b")).unwrap_err();
    assert_eq!(
        rej,
        RejectionReason::MachineIdentityConflict {
            machine: mid(2),
            raft_node_id: 1,
        }
    );
    assert!(sm.machine_binding(&mid(2)).is_none());
    assert_eq!(sm.machine_for_raft_node(1), Some(&mid(1)));
}

// ---- MintEnrollToken / RevokeEnrollToken ----

#[test]
fn mint_rejects_duplicate_id_and_empty_hash() {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, mint(tok(1), "h1", EnrollRole::Agent, None));
    assert_eq!(sm.enroll_tokens.len(), 1);

    let dup = sm
        .apply(&mint(tok(1), "h2", EnrollRole::Agent, None))
        .unwrap_err();
    assert_eq!(dup, RejectionReason::DuplicateEnrollToken(tok(1)));

    let empty = sm
        .apply(&mint(tok(2), "", EnrollRole::Coordinator, None))
        .unwrap_err();
    assert!(matches!(empty, RejectionReason::InvalidCommand(_)));
    assert!(!sm.enroll_tokens.contains_key(&tok(2)));
}

#[test]
fn revoke_token_unknown_rejects_and_repeat_is_idempotent() {
    let mut sm = StateMachine::default();
    let unknown = sm
        .apply(&Command::RevokeEnrollToken(RevokeEnrollToken {
            token: tok(404),
            revoked_at: base_ts(),
        }))
        .unwrap_err();
    assert_eq!(unknown, RejectionReason::UnknownEnrollToken(tok(404)));

    apply_ok(&mut sm, mint(tok(1), "h", EnrollRole::Agent, None));
    let revoke = || {
        Command::RevokeEnrollToken(RevokeEnrollToken {
            token: tok(1),
            revoked_at: base_ts(),
        })
    };
    apply_ok(&mut sm, revoke());
    assert!(sm.enroll_tokens[&tok(1)].revoked);
    // Already-revoked: accepted no-op (first revocation wins).
    apply_ok(&mut sm, revoke());
    assert!(sm.enroll_tokens[&tok(1)].revoked);
}

#[test]
fn live_enroll_tokens_filters_revoked_and_expired() {
    let mut sm = StateMachine::default();
    let now = ts(TS_US + 1_000_000);
    // Never expires — live.
    apply_ok(&mut sm, mint(tok(1), "h", EnrollRole::Agent, None));
    // Expires in the future — live.
    apply_ok(
        &mut sm,
        mint(tok(2), "h", EnrollRole::Agent, Some(ts(TS_US + 2_000_000))),
    );
    // Already expired at `now` — filtered.
    apply_ok(
        &mut sm,
        mint(tok(3), "h", EnrollRole::Agent, Some(ts(TS_US + 500_000))),
    );
    // Never-expiring but revoked — filtered.
    apply_ok(&mut sm, mint(tok(4), "h", EnrollRole::Coordinator, None));
    apply_ok(
        &mut sm,
        Command::RevokeEnrollToken(RevokeEnrollToken {
            token: tok(4),
            revoked_at: base_ts(),
        }),
    );

    let live: Vec<_> = sm.live_enroll_tokens(now).map(|(id, _)| *id).collect();
    assert_eq!(live, vec![tok(1), tok(2)]);
}

// ---- RevokeIdentity ----

#[test]
fn revoke_identity_inserts_and_is_idempotent() {
    let mut sm = StateMachine::default();
    let node_id = RevokedIdentity::Node(nid(7));
    let machine_id = RevokedIdentity::Machine(mid(7));
    let cmd = |identity: RevokedIdentity| {
        Command::RevokeIdentity(RevokeIdentity {
            identity,
            revoked_at: base_ts(),
        })
    };
    apply_ok(&mut sm, cmd(node_id.clone()));
    apply_ok(&mut sm, cmd(machine_id.clone()));
    assert!(sm.is_identity_revoked(&node_id));
    assert!(sm.is_identity_revoked(&machine_id));
    assert!(!sm.is_identity_revoked(&RevokedIdentity::Node(nid(8))));

    // Re-revoking is an accepted no-op that still bumps version.
    let before = sm.version;
    apply_ok(&mut sm, cmd(node_id.clone()));
    assert_eq!(sm.revoked_identities.len(), 2);
    assert_eq!(sm.version, before + 1);
}

// ---- ConfirmKeyPossession ----

#[test]
fn confirm_key_possession_inserts_and_reconfirmation_overwrites() {
    let mut sm = StateMachine::default();
    assert!(!sm.has_key_confirmation(3));
    apply_ok(
        &mut sm,
        Command::ConfirmKeyPossession(ConfirmKeyPossession {
            raft_node_id: 3,
            confirmed_at: base_ts(),
        }),
    );
    assert!(sm.has_key_confirmation(3));
    assert_eq!(sm.key_confirmations[&3], base_ts());

    let later = ts(TS_US + 9_000_000);
    apply_ok(
        &mut sm,
        Command::ConfirmKeyPossession(ConfirmKeyPossession {
            raft_node_id: 3,
            confirmed_at: later,
        }),
    );
    assert_eq!(
        sm.key_confirmations[&3], later,
        "re-confirmation overwrites"
    );
}

// ---- RecordEnrolledIdentity ----

#[test]
fn record_enrolled_identity_inserts_and_reenrollment_keeps_the_first_stamp() {
    let mut sm = StateMachine::default();
    assert!(!sm.is_identity_enrolled(&mid(4)));
    apply_ok(
        &mut sm,
        Command::RecordEnrolledIdentity(RecordEnrolledIdentity {
            machine: mid(4),
            recorded_at: base_ts(),
        }),
    );
    assert!(sm.is_identity_enrolled(&mid(4)));
    assert_eq!(sm.enrolled_identities[&mid(4)], base_ts());

    // Unlike ConfirmKeyPossession, first write wins: a re-enrollment is an
    // accepted no-op that still bumps version.
    let before = sm.version;
    apply_ok(
        &mut sm,
        Command::RecordEnrolledIdentity(RecordEnrolledIdentity {
            machine: mid(4),
            recorded_at: ts(TS_US + 9_000_000),
        }),
    );
    assert_eq!(sm.enrolled_identities.len(), 1);
    assert_eq!(
        sm.enrolled_identities[&mid(4)],
        base_ts(),
        "re-enrollment keeps the first stamp"
    );
    assert_eq!(sm.version, before + 1);

    // Enrollment is per identity, not global.
    assert!(!sm.is_identity_enrolled(&mid(5)));
}

// ---- No events ----

#[test]
fn pki_commands_emit_no_events() {
    let mut sm = StateMachine::default();
    assert!(apply_ok(&mut sm, record_ca(&cert_bundle("root")))
        .events
        .is_empty());
    assert!(apply_ok(&mut sm, bind(mid(1), 1, "a")).events.is_empty());
    assert!(
        apply_ok(&mut sm, mint(tok(1), "h", EnrollRole::Agent, None))
            .events
            .is_empty()
    );
    assert!(apply_ok(
        &mut sm,
        Command::RecordEnrolledIdentity(RecordEnrolledIdentity {
            machine: mid(2),
            recorded_at: base_ts(),
        })
    )
    .events
    .is_empty());
}
