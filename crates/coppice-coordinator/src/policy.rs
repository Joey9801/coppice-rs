//! The bootstrap-policy TOML schema and its idempotent command proposals
//! (ADR 0037 §3, ADR 0020's `cluster init` reservation).
//!
//! `coppice cluster init --policy <file>` ships a TOML document to a forming
//! coordinator, which applies it as part of formation and re-applies it
//! idempotently on every same-token re-init. The schema is deliberately
//! minimal: it seeds exactly the replicated state a fresh cluster needs before
//! it can accept a job — the priority-multiplier table and one or more quota
//! entities — mirroring what `coppice dev`'s `seed_dev_state` seeds so the two
//! never drift. Everything else in [`coppice_state::PolicyConfig`] (cost
//! weights, decay, surcharges) keeps its booted defaults and is left to the
//! ordinary admin tooling; this is not a general policy-editing surface.
//!
//! The command construction lives here, shared by the server-side
//! `apply_formation_policy` (which parses operator TOML) and by `coppice dev`
//! (which builds a [`FormationPolicy`] in memory). Both turn the policy into
//! the SAME idempotent proposals:
//!
//! - the priority table is seeded with one full-replacement `UpdatePolicy`
//!   **only while the replicated table is still empty** — so a re-init is a
//!   no-op and an operator's later edits survive;
//! - each quota entity is created **only when absent** by id — an existing
//!   entity is left untouched (reconfiguration is not an amnesty, and re-init
//!   must not reset accumulated usage).
//!
//! Human-facing multipliers are floats in the TOML; they are converted to the
//! replicated Q32.32 fixed-point [`PriorityMultiplier`] here, at the parse
//! edge, exactly as other human forms (rates, half-lives) are converted before
//! proposal (ADR 0019). No float is ever replicated.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use coppice_core::id::QuotaEntityId;
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, UpdatePolicy};
use coppice_state::{Command, StateMachine};

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
    /// present — a same-token re-init therefore proposes nothing and has no
    /// duplicate effect.
    pub fn commands(&self, state: &StateMachine, now: Timestamp) -> Vec<Command> {
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

        // Quota entities: create only those not already present. An existing
        // entity is left untouched — reconfiguration is not an amnesty, and a
        // re-init must not reset accumulated usage.
        for qe in &self.quota_entities {
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

        commands
    }
}

/// Propose every command in `commands`, riding out the leaderless window right
/// after formation (`NotLeader` / `Timeout`) for up to 10 seconds.
///
/// Shared by the server-side formation-policy application and `coppice dev`'s
/// seeding: both propose idempotent puts immediately after a single-node
/// cluster forms, when the initial election may still be in flight. A
/// rejection at apply, or any non-retryable consensus error, fails fast.
pub async fn propose_all<C: coppice_consensus::Consensus>(
    consensus: &C,
    commands: Vec<Command>,
) -> Result<()> {
    use coppice_consensus::ConsensusError;
    use std::time::Duration;

    for command in commands {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match consensus.propose(command.clone()).await {
                Ok(applied) => {
                    applied
                        .outcome
                        .map_err(|reason| anyhow::anyhow!("policy command rejected: {reason}"))?;
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(policy.commands(&state, Timestamp::now()).is_empty());
    }

    #[test]
    fn commands_seed_table_and_entities_on_a_fresh_state() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).unwrap();
        let state = StateMachine::default();
        let commands = policy.commands(&state, Timestamp::now());
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
        assert!(policy.commands(&state, now).is_empty());
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
        assert!(policy.commands(&state, Timestamp::now()).is_empty());
    }
}
