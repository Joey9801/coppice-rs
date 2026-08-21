//! The bootstrap-policy TOML schema and its idempotent command proposals
//! (ADR 0037 §3, ADR 0020's `cluster init` reservation).
//!
//! `coppice cluster init --policy <file>` ships a TOML document to a forming
//! coordinator, which applies it as part of formation and re-applies it
//! idempotently if formation is re-run. The schema is deliberately
//! minimal: it seeds exactly the replicated state a fresh cluster needs before
//! it can accept a job — the priority-multiplier table and one or more quota
//! entities — and `coppice dev` seeds its state through this same schema via
//! its local `init`, so the two never drift. Everything else in [`coppice_state::PolicyConfig`] (cost
//! weights, decay, surcharges) keeps its booted defaults and is left to the
//! ordinary admin tooling; this is not a general policy-editing surface.
//!
//! The command construction lives here, shared by the server-side
//! `apply_formation_policy` (which parses operator TOML) and by `coppice dev`
//! (which builds a [`FormationPolicy`] in memory). Both turn the policy into
//! the SAME idempotent proposals:
//!
//! - the priority table is seeded with one full-replacement `UpdatePolicy`
//!   **only while the replicated table is still empty** — so re-application is
//!   a no-op and an operator's later edits survive;
//! - each quota entity is created **only when absent** by id — an existing
//!   entity is left untouched (reconfiguration is not an amnesty, and re-init
//!   must not reset accumulated usage);
//! - each `[[enroll_token]]` is minted **only when no live token carries its
//!   label** (ADR 0037 §5), which is why labels are required and unique here.
//!
//! Human-facing multipliers are floats in the TOML; they are converted to the
//! replicated Q32.32 fixed-point [`PriorityMultiplier`] here, at the parse
//! edge, exactly as other human forms (rates, half-lives) are converted before
//! proposal (ADR 0019). No float is ever replicated.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use coppice_core::id::{EnrollTokenId, QuotaEntityId};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{ConfigureQuotaEntity, MintEnrollToken, UpdatePolicy};
use coppice_state::{Command, EnrollRole, StateMachine};
use coppice_tls::pki;

/// `2^32`, the scale of the Q32.32 fixed-point [`PriorityMultiplier`].
const Q32_SCALE: f64 = 4_294_967_296.0;

/// A parsed bootstrap-policy document (ADR 0037 §3).
///
/// Constructed either by [`FormationPolicy::parse_toml`] (operator-supplied
/// `--policy` file) or in memory with public fields (`coppice dev`). Turned
/// into idempotent command proposals by [`FormationPolicy::commands`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormationPolicy {
    /// The priority-multiplier table, as `[[priority_multiplier]]` array
    /// entries. Empty = leave the replicated table untouched.
    #[serde(default, rename = "priority_multiplier")]
    pub priority_multipliers: Vec<PriorityMultiplierSpec>,
    /// The quota entities to create, as `[[quota_entity]]` array entries.
    #[serde(default, rename = "quota_entity")]
    pub quota_entities: Vec<QuotaEntitySpec>,
    /// The enrollment tokens to seed, as `[[enroll_token]]` array entries
    /// (ADR 0037 §5). This is the launch-template story: bake a secret into
    /// user-data, name it here, form, and the fleet enrolls.
    #[serde(default, rename = "enroll_token")]
    pub enroll_tokens: Vec<EnrollTokenSpec>,
}

/// One `[[enroll_token]]` entry: an operator-supplied secret, the role it
/// grants, and the label that identifies it (ADR 0037 §5).
///
/// The secret is operator-chosen here rather than cluster-minted precisely
/// because this is the *seeding* path: the value is already in the launch
/// template when the cluster forms, so the cluster cannot be the one to choose
/// it. `mint` (the admin verb) is the other direction and generates its own.
///
/// `Debug` is hand-written and redacts the secret.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollTokenSpec {
    /// The bearer secret enrolling machines will present. Only its salted hash
    /// is replicated.
    pub secret: String,
    /// `"agent"` or `"coordinator"` — never both (ADR 0037 §5).
    pub role: EnrollRoleSpec,
    /// A non-empty label, unique within the document. It is the **idempotency
    /// key**: re-applying the policy mints nothing when a live token already
    /// carries this label.
    pub label: String,
    /// Optional time-to-live (`"15m"`, `"720h"`). Absent = never expires, the
    /// supported long-lived launch-template default (ADR 0037 §5).
    #[serde(default, with = "humantime_serde::option")]
    pub ttl: Option<std::time::Duration>,
}

impl std::fmt::Debug for EnrollTokenSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollTokenSpec")
            .field("secret", &"<redacted>")
            .field("role", &self.role)
            .field("label", &self.label)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// The TOML spelling of an enrollment token's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrollRoleSpec {
    Agent,
    Coordinator,
}

impl From<EnrollRoleSpec> for EnrollRole {
    fn from(spec: EnrollRoleSpec) -> EnrollRole {
        match spec {
            EnrollRoleSpec::Agent => EnrollRole::Agent,
            EnrollRoleSpec::Coordinator => EnrollRole::Coordinator,
        }
    }
}

/// One `[[priority_multiplier]]` entry: a user-facing `priority: i32` index and
/// the human-form cost multiplier it maps to (`1.0` = 1×, `0.5` = half price).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriorityMultiplierSpec {
    /// The `priority` index a job declares.
    pub index: i32,
    /// The cost multiplier for that priority, as a decimal (`0.25`..`4.0`…).
    pub multiplier: f64,
}

/// One `[[quota_entity]]` entry: a quota leaf jobs charge against.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaEntitySpec {
    /// The entity id (`quota-<uuid>`).
    pub id: QuotaEntityId,
    /// A human label recorded on the entity.
    pub name: String,
    /// The quota stock in µCU (ADR 0019).
    pub quota: u64,
    /// Optional parent entity for hierarchical accounting.
    #[serde(default)]
    pub parent: Option<QuotaEntityId>,
}

impl FormationPolicy {
    /// Parse and validate a bootstrap-policy TOML document.
    ///
    /// `deny_unknown_fields` throughout, so a typo fail-stops naming the key
    /// rather than silently defaulting — the same posture the job spec takes.
    pub fn parse_toml(bytes: &[u8]) -> Result<FormationPolicy> {
        let text = std::str::from_utf8(bytes).context("formation policy is not valid UTF-8")?;
        let policy: FormationPolicy =
            toml::from_str(text).context("parsing formation policy TOML")?;
        policy.validate()?;
        Ok(policy)
    }

    /// Reject values serde alone cannot catch: non-finite/negative multipliers,
    /// duplicate priority indices, and duplicate quota-entity ids.
    fn validate(&self) -> Result<()> {
        let mut seen_index = std::collections::BTreeSet::new();
        for pm in &self.priority_multipliers {
            if !pm.multiplier.is_finite() || pm.multiplier < 0.0 {
                bail!(
                    "priority multiplier for index {} must be a finite, non-negative number \
                     (got {})",
                    pm.index,
                    pm.multiplier
                );
            }
            if !seen_index.insert(pm.index) {
                bail!("duplicate priority multiplier for index {}", pm.index);
            }
        }
        let mut seen_id = std::collections::BTreeSet::new();
        for qe in &self.quota_entities {
            if !seen_id.insert(qe.id) {
                bail!("duplicate quota entity {}", qe.id);
            }
        }
        // Labels carry idempotency for token seeding, so they must be present
        // and distinct or a re-apply cannot tell two entries apart.
        let mut seen_label = std::collections::BTreeSet::new();
        for token in &self.enroll_tokens {
            if token.label.trim().is_empty() {
                bail!(
                    "every [[enroll_token]] needs a non-empty label: it is what makes \
                       re-applying the policy idempotent (ADR 0037 §5)"
                );
            }
            if !seen_label.insert(token.label.as_str()) {
                bail!("duplicate enrollment token label {:?}", token.label);
            }
            if token.secret.trim().is_empty() {
                bail!("enrollment token {:?} has an empty secret", token.label);
            }
        }
        Ok(())
    }

    /// The replicated Q32.32 priority table this policy describes.
    fn multiplier_table(&self) -> BTreeMap<i32, PriorityMultiplier> {
        self.priority_multipliers
            .iter()
            .map(|pm| {
                // Float-to-int casts saturate in Rust; `validate` already
                // rejected negative/non-finite inputs, so this is exact for the
                // representable range and clamps only on absurd overflow.
                let raw = (pm.multiplier * Q32_SCALE).round();
                (pm.index, PriorityMultiplier(raw as u64))
            })
            .collect()
    }

    /// Build the idempotent proposals to apply this policy against `state`
    /// (the current applied state), stamped `now` (ADR 0037 §3).
    ///
    /// Returns an empty vec when everything the policy describes is already
    /// present — a re-run therefore proposes nothing and has no duplicate
    /// effect.
    ///
    /// Quota entities are emitted **parent before child** regardless of their
    /// document order: the state machine rejects a child whose parent does not
    /// exist yet, so a valid hierarchy listed child-first must not fail midway
    /// through seeding. A parent that is neither in the document nor already in
    /// `state`, or a parent cycle within the document, is an error here — at
    /// the seeding edge — rather than a mid-apply rejection.
    ///
    /// `kdf` is the cost the seeding node hashes `[[enroll_token]]` secrets
    /// at (`[token_kdf]` in its config); only the resulting PHC strings are
    /// replicated, so the choice is node-local.
    pub fn commands(
        &self,
        state: &StateMachine,
        now: Timestamp,
        kdf: pki::TokenKdf,
    ) -> Result<Vec<Command>> {
        let mut commands = Vec::new();

        // Priority table: seed only while the replicated table is still empty.
        // `UpdatePolicy` is a full replacement, so clone the current policy and
        // change only the table — every other field keeps its booted default.
        if !self.priority_multipliers.is_empty() && state.policy.priority_multipliers.is_empty() {
            let mut policy = state.policy.clone();
            policy.priority_multipliers = self.multiplier_table();
            commands.push(Command::UpdatePolicy(UpdatePolicy {
                policy,
                updated_at: now,
            }));
        }

        // Quota entities: create only those not already present, parent before
        // child. An existing entity is left untouched — reconfiguration is not
        // an amnesty, and a re-run must not reset accumulated usage.
        for qe in self.ordered_entities(state)? {
            if !state.quota_entities.contains_key(&qe.id) {
                commands.push(Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
                    entity: qe.id,
                    parent: qe.parent,
                    name: qe.name.clone(),
                    quota: CostUnits(qe.quota),
                    updated_at: now,
                }));
            }
        }

        // Enrollment tokens: mint only those whose label is not already
        // carried by a LIVE token (ADR 0037 §5). Label, not id and not hash, is
        // the natural key here — the id would have to be minted fresh on every
        // apply to stay unpredictable, and the hash is salted, so neither can
        // be derived deterministically from the document. A revoked or expired
        // token with the same label does not block a re-mint: that is exactly
        // the rotation an operator performs by revoking and re-running.
        let live_labels: std::collections::BTreeSet<&str> = state
            .live_enroll_tokens(now)
            .map(|(_, t)| t.label.as_str())
            .collect();
        for spec in &self.enroll_tokens {
            if live_labels.contains(spec.label.as_str()) {
                continue;
            }
            let hash = pki::hash_secret_with(&spec.secret, kdf).with_context(|| {
                format!("hashing the enrollment token secret for {:?}", spec.label)
            })?;
            let expires_at = spec.ttl.map(|ttl| {
                now.saturating_add(Duration::from_micros(
                    i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX),
                ))
            });
            commands.push(Command::MintEnrollToken(MintEnrollToken {
                token: EnrollTokenId::new(),
                hash,
                role: spec.role.into(),
                label: spec.label.clone(),
                expires_at,
                minted_at: now,
            }));
        }

        Ok(commands)
    }

    /// The document's quota entities in parent-before-child order.
    ///
    /// Kahn-style: an entity is ready once its parent is `None`, already in
    /// `state`, or already emitted. A pass that emits nothing means every
    /// remaining entity waits on a parent that can never appear — either a
    /// reference to an entity that exists nowhere, or a cycle within the
    /// document — and both are reported naming the entities involved.
    fn ordered_entities(&self, state: &StateMachine) -> Result<Vec<&QuotaEntitySpec>> {
        let in_doc: std::collections::BTreeSet<_> =
            self.quota_entities.iter().map(|qe| qe.id).collect();
        let mut emitted = std::collections::BTreeSet::new();
        let mut remaining: Vec<&QuotaEntitySpec> = self.quota_entities.iter().collect();
        let mut ordered = Vec::with_capacity(remaining.len());

        while !remaining.is_empty() {
            let emitted_before = ordered.len();
            let mut stuck = Vec::with_capacity(remaining.len());
            for qe in remaining {
                let ready = match qe.parent {
                    None => true,
                    Some(parent) => {
                        emitted.contains(&parent) || state.quota_entities.contains_key(&parent)
                    }
                };
                if ready {
                    emitted.insert(qe.id);
                    ordered.push(qe);
                } else {
                    stuck.push(qe);
                }
            }

            // A pass that emitted nothing can never make progress: every stuck
            // entity waits on a parent that will never appear. A parent outside
            // the document (and absent from state) is a dangling reference;
            // otherwise the stuck set forms a cycle within the document.
            if ordered.len() == emitted_before {
                for qe in &stuck {
                    let parent = qe.parent.expect("unparented entities are never stuck");
                    if !in_doc.contains(&parent) {
                        bail!(
                            "quota entity {} references parent {} which is neither in this \
                             policy document nor already in cluster state",
                            qe.id,
                            parent
                        );
                    }
                }
                let ids: Vec<String> = stuck.iter().map(|qe| qe.id.to_string()).collect();
                bail!(
                    "quota entity parent cycle in policy document involving: {}",
                    ids.join(", ")
                );
            }
            remaining = stuck;
        }

        Ok(ordered)
    }
}

/// Propose every command in `commands`, riding out the leaderless window right
/// after formation (`NotLeader` / `Timeout`) for up to 10 seconds.
///
/// Shared by the server-side formation-policy application and `coppice dev`'s
/// seeding: both propose idempotent puts immediately after a single-node
/// cluster forms, when the initial election may still be in flight. A
/// rejection at apply, or any non-retryable consensus error, fails fast.
/// Returns the log index of the last command applied (`None` for an empty
/// batch), so callers can wait for the published views to include the batch
/// before acting on "it is written".
pub async fn propose_all<C: coppice_consensus::Consensus>(
    consensus: &C,
    commands: Vec<Command>,
) -> Result<Option<u64>> {
    use coppice_consensus::ConsensusError;
    use std::time::Duration;

    let mut last_index = None;
    for command in commands {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match consensus.propose(command.clone()).await {
                Ok(applied) => {
                    applied
                        .outcome
                        .map_err(|reason| anyhow::anyhow!("policy command rejected: {reason}"))?;
                    last_index = Some(applied.log_index);
                    break;
                }
                Err(e @ (ConsensusError::NotLeader { .. } | ConsensusError::Timeout)) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(
                            anyhow::Error::new(e).context("proposing a formation policy command")
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    return Err(
                        anyhow::Error::new(e).context("proposing a formation policy command")
                    );
                }
            }
        }
    }
    Ok(last_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal argon2 cost: these tests assert seeding logic, not KDF
    /// strength, and the default cost is ~300ms per hash in a debug build.
    const CHEAP_KDF: pki::TokenKdf = pki::TokenKdf {
        m_cost_kib: 8,
        t_cost: 1,
        p_cost: 1,
    };

    const SAMPLE: &str = r#"
[[priority_multiplier]]
index = -1
multiplier = 0.5

[[priority_multiplier]]
index = 0
multiplier = 1.0

[[priority_multiplier]]
index = 2
multiplier = 4.0

[[quota_entity]]
id = "quota-00000000-0000-0000-0000-000000000001"
name = "default"
quota = 1000000000000
"#;

    #[test]
    fn parses_the_sample_schema() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).expect("sample parses");
        assert_eq!(policy.priority_multipliers.len(), 3);
        assert_eq!(policy.quota_entities.len(), 1);
        assert_eq!(policy.quota_entities[0].name, "default");
        assert_eq!(policy.quota_entities[0].quota, 1_000_000_000_000);
        assert!(policy.quota_entities[0].parent.is_none());
    }

    #[test]
    fn multiplier_table_is_exact_for_power_of_two_factors() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).unwrap();
        let table = policy.multiplier_table();
        assert_eq!(table[&-1], PriorityMultiplier(1 << 31)); // 0.5×
        assert_eq!(table[&0], PriorityMultiplier::ONE); // 1.0×
        assert_eq!(table[&2], PriorityMultiplier(4 << 32)); // 4.0×
    }

    #[test]
    fn unknown_key_is_rejected() {
        let bad = format!("{SAMPLE}\n[[quota_entity]]\nid = \"quota-00000000-0000-0000-0000-000000000002\"\nname = \"x\"\nquota = 1\nbogus = 3\n");
        assert!(FormationPolicy::parse_toml(bad.as_bytes()).is_err());
    }

    #[test]
    fn negative_multiplier_is_rejected() {
        let bad = "[[priority_multiplier]]\nindex = 0\nmultiplier = -1.0\n";
        let err = FormationPolicy::parse_toml(bad.as_bytes()).expect_err("negative rejected");
        assert!(format!("{err:#}").contains("non-negative"), "{err:#}");
    }

    #[test]
    fn duplicate_priority_index_is_rejected() {
        let bad = "[[priority_multiplier]]\nindex = 0\nmultiplier = 1.0\n\
                   [[priority_multiplier]]\nindex = 0\nmultiplier = 2.0\n";
        let err = FormationPolicy::parse_toml(bad.as_bytes()).expect_err("dup rejected");
        assert!(format!("{err:#}").contains("duplicate"), "{err:#}");
    }

    #[test]
    fn empty_document_parses_to_no_commands() {
        let policy = FormationPolicy::parse_toml(b"").expect("empty parses");
        let state = StateMachine::default();
        assert!(policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect("valid policy")
            .is_empty());
    }

    #[test]
    fn commands_seed_table_and_entities_on_a_fresh_state() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).unwrap();
        let state = StateMachine::default();
        let commands = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect("valid policy");
        // One UpdatePolicy (table) + one ConfigureQuotaEntity.
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], Command::UpdatePolicy(_)));
        assert!(matches!(commands[1], Command::ConfigureQuotaEntity(_)));
    }

    #[test]
    fn commands_are_a_noop_when_already_applied() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).unwrap();
        // A state that already has the table and the entity: re-application
        // proposes nothing (idempotent re-init).
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.policy.priority_multipliers = policy.multiplier_table();
        let id = policy.quota_entities[0].id;
        state.quota_entities.insert(
            id,
            coppice_state::QuotaEntity {
                parent: None,
                name: "default".to_string(),
                quota: CostUnits(1_000_000_000_000),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        assert!(policy
            .commands(&state, now, CHEAP_KDF)
            .expect("valid policy")
            .is_empty());
    }

    #[test]
    fn table_is_skipped_when_already_seeded() {
        // Only the priority table differs; the replicated table is already
        // non-empty, so it is left untouched (an operator's edits survive).
        let policy = FormationPolicy::parse_toml(
            b"[[priority_multiplier]]\nindex = 0\nmultiplier = 1.0\n" as &[u8],
        )
        .unwrap();
        let mut state = StateMachine::default();
        state
            .policy
            .priority_multipliers
            .insert(0, PriorityMultiplier(9 << 32));
        assert!(policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect("valid policy")
            .is_empty());
    }

    // ---- enrollment token seeding (ADR 0037 §5) --------------------------

    const TOKEN_DOC: &str = r#"
[[enroll_token]]
secret = "cpk_launch-template-secret"
role = "agent"
label = "fleet-agents"

[[enroll_token]]
secret = "cpk_coordinator-secret"
role = "coordinator"
label = "coordinators"
ttl = "15m"
"#;

    /// The `MintEnrollToken` commands a policy emits, in order.
    fn minted(commands: &[Command]) -> Vec<&MintEnrollToken> {
        commands
            .iter()
            .filter_map(|c| match c {
                Command::MintEnrollToken(m) => Some(m),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn enroll_tokens_are_hashed_at_the_seeding_edge() {
        let policy = FormationPolicy::parse_toml(TOKEN_DOC.as_bytes()).expect("parses");
        let now = Timestamp::now();
        let commands = policy
            .commands(&StateMachine::default(), now, CHEAP_KDF)
            .expect("valid policy");
        let tokens = minted(&commands);
        assert_eq!(tokens.len(), 2);

        let agent = tokens.iter().find(|t| t.label == "fleet-agents").unwrap();
        assert_eq!(agent.role, EnrollRole::Agent);
        assert!(agent.expires_at.is_none(), "no ttl means never expires");
        // The clear secret is nowhere in the command; only its salted hash is.
        assert!(agent.hash.starts_with("$argon2id$"), "{}", agent.hash);
        assert!(pki::verify_secret(
            "cpk_launch-template-secret",
            &agent.hash
        ));

        let coord = tokens.iter().find(|t| t.label == "coordinators").unwrap();
        assert_eq!(coord.role, EnrollRole::Coordinator);
        assert_eq!(
            coord.expires_at,
            Some(now.saturating_add(Duration::from_secs(900)))
        );
    }

    #[test]
    fn re_seeding_mints_nothing_when_a_live_token_carries_the_label() {
        let policy = FormationPolicy::parse_toml(TOKEN_DOC.as_bytes()).unwrap();
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        // Apply once, then fold the results into state as the apply loop would.
        for command in policy.commands(&state, now, CHEAP_KDF).unwrap() {
            if let Command::MintEnrollToken(m) = command {
                state.enroll_tokens.insert(
                    m.token,
                    coppice_state::EnrollToken {
                        hash: m.hash,
                        role: m.role,
                        label: m.label,
                        expires_at: m.expires_at,
                        minted_at: m.minted_at,
                        revoked: false,
                    },
                );
            }
        }
        assert!(
            minted(&policy.commands(&state, now, CHEAP_KDF).unwrap()).is_empty(),
            "a re-apply must mint nothing — labels are the idempotency key"
        );
    }

    #[test]
    fn a_revoked_label_is_re_minted() {
        // Revoke-and-reseed is how an operator rotates a launch-template
        // secret, so a revoked token must NOT block its label.
        let policy = FormationPolicy::parse_toml(TOKEN_DOC.as_bytes()).unwrap();
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.enroll_tokens.insert(
            EnrollTokenId::new(),
            coppice_state::EnrollToken {
                hash: pki::hash_secret("old").unwrap(),
                role: EnrollRole::Agent,
                label: "fleet-agents".to_string(),
                expires_at: None,
                minted_at: now,
                revoked: true,
            },
        );
        let commands = policy.commands(&state, now, CHEAP_KDF).unwrap();
        let labels: Vec<&str> = minted(&commands).iter().map(|m| m.label.as_str()).collect();
        assert!(labels.contains(&"fleet-agents"), "{labels:?}");
    }

    #[test]
    fn duplicate_and_empty_labels_are_rejected() {
        let dup = "[[enroll_token]]\nsecret = \"a\"\nrole = \"agent\"\nlabel = \"x\"\n\
                   [[enroll_token]]\nsecret = \"b\"\nrole = \"agent\"\nlabel = \"x\"\n";
        let err = FormationPolicy::parse_toml(dup.as_bytes()).expect_err("duplicate label");
        assert!(format!("{err:#}").contains("duplicate"), "{err:#}");

        let empty = "[[enroll_token]]\nsecret = \"a\"\nrole = \"agent\"\nlabel = \"\"\n";
        let err = FormationPolicy::parse_toml(empty.as_bytes()).expect_err("empty label");
        assert!(format!("{err:#}").contains("non-empty label"), "{err:#}");
    }

    #[test]
    fn an_unknown_role_is_rejected() {
        let bad = "[[enroll_token]]\nsecret = \"a\"\nrole = \"admin\"\nlabel = \"x\"\n";
        assert!(FormationPolicy::parse_toml(bad.as_bytes()).is_err());
    }

    #[test]
    fn the_spec_debug_never_prints_the_secret() {
        let policy = FormationPolicy::parse_toml(TOKEN_DOC.as_bytes()).unwrap();
        let rendered = format!("{:?}", policy.enroll_tokens);
        assert!(!rendered.contains("cpk_"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    // ---- entity ordering (parent before child) ---------------------------

    const PARENT: &str = "quota-00000000-0000-0000-0000-00000000000a";
    const CHILD: &str = "quota-00000000-0000-0000-0000-00000000000b";
    const GRANDCHILD: &str = "quota-00000000-0000-0000-0000-00000000000c";

    fn entity_toml(id: &str, parent: Option<&str>) -> String {
        let parent_line = parent
            .map(|p| format!("parent = \"{p}\"\n"))
            .unwrap_or_default();
        format!("[[quota_entity]]\nid = \"{id}\"\nname = \"x\"\nquota = 1\n{parent_line}\n")
    }

    /// The quota-entity id each `ConfigureQuotaEntity` command creates, in
    /// emission order.
    fn configured_ids(commands: &[Command]) -> Vec<QuotaEntityId> {
        commands
            .iter()
            .map(|c| match c {
                Command::ConfigureQuotaEntity(cfg) => cfg.entity,
                other => panic!("expected ConfigureQuotaEntity, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn entities_listed_child_first_are_emitted_parent_first() {
        // Grandchild, child, parent — reverse hierarchy order in the document.
        let toml = format!(
            "{}{}{}",
            entity_toml(GRANDCHILD, Some(CHILD)),
            entity_toml(CHILD, Some(PARENT)),
            entity_toml(PARENT, None),
        );
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let commands = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect("valid hierarchy");
        assert_eq!(
            configured_ids(&commands),
            vec![
                PARENT.parse().unwrap(),
                CHILD.parse().unwrap(),
                GRANDCHILD.parse().unwrap(),
            ],
            "commands must run parent before child regardless of document order"
        );
    }

    #[test]
    fn child_of_a_parent_already_in_state_needs_no_document_parent() {
        // The parent exists only in replicated state (e.g. operator-created or
        // a prior seeding run); the document lists just the child.
        let toml = entity_toml(CHILD, Some(PARENT));
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.quota_entities.insert(
            PARENT.parse().unwrap(),
            coppice_state::QuotaEntity {
                parent: None,
                name: "parent".to_string(),
                quota: CostUnits(1),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        let commands = policy
            .commands(&state, now, CHEAP_KDF)
            .expect("parent found in state");
        assert_eq!(configured_ids(&commands), vec![CHILD.parse().unwrap()]);
    }

    #[test]
    fn missing_parent_is_rejected_before_any_command_is_emitted() {
        let toml = entity_toml(CHILD, Some(PARENT)); // PARENT nowhere
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("dangling parent must be rejected");
        let message = format!("{err:#}");
        assert!(message.contains(CHILD), "{message}");
        assert!(message.contains(PARENT), "{message}");
        assert!(
            message.contains("neither in this policy document nor already in cluster state"),
            "{message}"
        );
    }

    #[test]
    fn parent_cycle_is_rejected() {
        let toml = format!(
            "{}{}",
            entity_toml(PARENT, Some(CHILD)),
            entity_toml(CHILD, Some(PARENT)),
        );
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("parent cycle must be rejected");
        assert!(format!("{err:#}").contains("cycle"), "{err:#}");
    }

    #[test]
    fn self_parent_is_rejected_as_a_cycle() {
        let toml = entity_toml(PARENT, Some(PARENT));
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("self-parent must be rejected");
        assert!(format!("{err:#}").contains("cycle"), "{err:#}");
    }
}
