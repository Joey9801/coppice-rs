//! The bootstrap-policy TOML schema and its idempotent command proposals
//! (ADR 0037 §3, ADR 0020's `cluster init` reservation).
//!
//! `coppice cluster init --policy <file>` ships a TOML document to a forming
//! coordinator, which applies it as part of formation and re-applies it
//! idempotently if formation is re-run. The schema is deliberately
//! minimal: it seeds exactly the replicated state a fresh cluster needs before
//! it can accept a job — the priority-multiplier table, the resource prices,
//! and one or more quota entities — and `coppice dev` seeds its state through
//! this same schema via its local `init`, so the two never drift. Everything
//! else in [`coppice_state::PolicyConfig`] (decay, surcharges) keeps its
//! booted defaults and is left to the ordinary admin tooling; this is not a
//! general policy-editing surface.
//!
//! The command construction lives here, shared by the server-side
//! `apply_formation_policy` (which parses operator TOML) and by `coppice dev`
//! (which builds a [`FormationPolicy`] in memory). Both turn the policy into
//! the SAME idempotent proposals:
//!
//! - the priority table is seeded with one full-replacement `UpdatePolicy`
//!   **only while the replicated table is still empty** — so re-application is
//!   a no-op and an operator's later edits survive; the `[cost_weights]`
//!   prices ride the same command under the same rule, seeded only while the
//!   replicated weights are still all-zero (the booted "everything is free"
//!   default); the `[retention]` windows ride it too, each seeded only while
//!   the replicated field still holds its booted default;
//! - each quota entity is named by its **path** (ADR 0045) and created
//!   **only when its path does not already resolve** in replicated state —
//!   an existing entity is left untouched (reconfiguration is not an
//!   amnesty, and re-init must not reset accumulated usage); entries may
//!   appear in any order, parents before children or not, and a declared
//!   `id` that collides with a *different* entity already holding that path
//!   is a document error;
//! - each `[[enroll_token]]` is minted **only when no live token carries its
//!   label** (ADR 0037 §5), which is why labels are required and unique here;
//! - the `[authorization]` role bindings are installed with one
//!   full-replacement `UpdateAuthorization` **only while the replicated
//!   binding list is still empty** — the day-0 authority the security doc
//!   describes: a fresh cluster denies everyone but operator certificates, and
//!   a deployment that terminates TLS in front of the coordinators has no way
//!   to present one, so the first bindings must ride formation's local-socket
//!   authority. Once any binding exists, `coppice policy authz set` owns the
//!   list and a re-init leaves it alone.
//!
//! Human-facing multipliers and prices are floats in the TOML; they are
//! converted to the replicated Q32.32 fixed-point [`PriorityMultiplier`] and
//! [`CostWeights`] here, at the parse edge, exactly as other human forms
//! (rates, half-lives) are converted before proposal (ADR 0019). No float is
//! ever replicated.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use coppice_core::entity_ref::{QuotaEntityPath, QuotaEntityRef};
use coppice_core::id::{EnrollTokenId, QuotaEntityId};
use coppice_core::quota::{CostUnits, CostWeights, PriorityMultiplier, MICRO_PER_COST_UNIT};
use coppice_core::time::{Duration, Timestamp};
use coppice_state::authz::{Binding, Role, Subject};
use coppice_state::command::{
    ConfigureQuotaEntity, MintEnrollToken, UpdateAuthorization, UpdatePolicy,
};
use coppice_state::{Command, EnrollRole, PolicyConfig, StateMachine};
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
    /// What a CU buys, as the `[cost_weights]` table. Absent = leave the
    /// replicated weights untouched (a fresh cluster's are all zero, i.e.
    /// every job is free).
    #[serde(default)]
    pub cost_weights: Option<CostWeightsSpec>,
    /// The quota entities to create, as `[[quota_entity]]` array entries.
    #[serde(default, rename = "quota_entity")]
    pub quota_entities: Vec<QuotaEntitySpec>,
    /// The enrollment tokens to seed, as `[[enroll_token]]` array entries
    /// (ADR 0037 §5). This is the launch-template story: bake a secret into
    /// user-data, name it here, form, and the fleet enrolls.
    #[serde(default, rename = "enroll_token")]
    pub enroll_tokens: Vec<EnrollTokenSpec>,
    /// The initial role bindings, as the `[authorization]` table (ADR 0023).
    /// Absent = leave the replicated bindings untouched.
    #[serde(default)]
    pub authorization: Option<AuthorizationSpec>,
    /// How long replicated records outlive what they describe, as the
    /// `[retention]` table. Absent = leave both windows at their booted
    /// defaults.
    #[serde(default)]
    pub retention: Option<RetentionSpec>,
}

/// The `[retention]` table: how long replicated records outlive what they
/// describe.
///
/// ```toml
/// [retention]
/// node = "24h"      # PolicyConfig::node_retention (ADR 0041)
/// terminal = "72h"  # PolicyConfig::terminal_retention (ADR 0012)
/// ```
///
/// Both are humantime spans and both are optional; an omitted field leaves
/// that window at its booted default. Like the prices, each is seeded **only
/// while the replicated field still holds that default**, so a re-run of
/// `init` never overwrites an operator's later edit.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionSpec {
    /// How long a drained, empty, silent node record is kept before the
    /// leader's housekeeping GC evicts it (ADR 0041). Default 24 h.
    #[serde(default, with = "humantime_serde::option")]
    pub node: Option<std::time::Duration>,
    /// How long a terminal job is kept in replicated state after it finished
    /// (ADR 0012). Default 72 h.
    #[serde(default, with = "humantime_serde::option")]
    pub terminal: Option<std::time::Duration>,
}

impl RetentionSpec {
    /// Reject a zero window: it would evict a record the instant it
    /// qualifies, which is a footgun rather than a shorter retention.
    fn validate(&self) -> Result<()> {
        for (field, value) in [("node", self.node), ("terminal", self.terminal)] {
            if value.is_some_and(|v| v.is_zero()) {
                bail!("[retention] {field} must be a non-zero duration (for example \"24h\")");
            }
        }
        Ok(())
    }
}

/// The `[authorization]` table: the day-0 role bindings and, optionally, the
/// token claim group names are read from.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationSpec {
    /// Rename of `PolicyConfig::groups_claim` (default `groups`), installed
    /// at the same log position as the bindings. Cognito, for one, puts a
    /// user's groups under `cognito:groups`.
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// One `[[authorization.binding]]` per binding. At least one must be an
    /// unscoped `admin`, the same lockout rule apply enforces (ADR 0023).
    #[serde(default, rename = "binding")]
    pub bindings: Vec<BindingSpec>,
}

impl AuthorizationSpec {
    /// Every binding in document order, or the first entry that cannot be
    /// parsed on its own. A binding's `scope` is left as an unresolved
    /// [`QuotaEntityRef`] here — resolving it against seeded and existing
    /// quota entities needs `state`, and is [`FormationPolicy::commands`]'s
    /// job.
    fn bindings(&self) -> Result<Vec<ParsedBinding>> {
        self.bindings
            .iter()
            .enumerate()
            .map(|(i, b)| b.binding(i))
            .collect()
    }
}

/// One role binding: exactly one of `group`/`principal`, a role, and an
/// optional quota-entity scope (absent = the whole tree), named by
/// [`QuotaEntityRef`] — an id or a path (ADR 0045).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingSpec {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub principal: Option<String>,
    pub role: RoleSpec,
    #[serde(default)]
    pub scope: Option<QuotaEntityRef>,
}

/// One `[[authorization.binding]]`, parsed but with its `scope` (if any)
/// still an unresolved [`QuotaEntityRef`].
struct ParsedBinding {
    subject: Subject,
    role: Role,
    scope: Option<QuotaEntityRef>,
}

/// The wire spelling of [`coppice_state::authz::Role`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleSpec {
    Submitter,
    Operator,
    Admin,
}

impl From<RoleSpec> for Role {
    fn from(r: RoleSpec) -> Self {
        match r {
            RoleSpec::Submitter => Role::Submitter,
            RoleSpec::Operator => Role::Operator,
            RoleSpec::Admin => Role::Admin,
        }
    }
}

impl BindingSpec {
    /// The parsed binding, or why this entry cannot be one. Mirrors the
    /// API's wire → domain conversion and apply's own subject check so the
    /// document fails here, at the seeding edge, rather than mid-formation.
    /// `scope` is left unresolved (see [`ParsedBinding`]).
    fn binding(&self, index: usize) -> Result<ParsedBinding> {
        let subject = match (&self.group, &self.principal) {
            (Some(name), None) => Subject::Group(name.clone()),
            (None, Some(sub)) => Subject::Principal(sub.clone()),
            (Some(_), Some(_)) => bail!(
                "[[authorization.binding]] {} names both `group` and `principal`; exactly one",
                index + 1
            ),
            (None, None) => bail!(
                "[[authorization.binding]] {} names neither `group` nor `principal`; exactly one",
                index + 1
            ),
        };
        if subject.name().trim().is_empty() {
            bail!(
                "[[authorization.binding]] {} has an empty subject",
                index + 1
            );
        }
        Ok(ParsedBinding {
            subject,
            role: self.role.into(),
            scope: self.scope.clone(),
        })
    }
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

/// The `[cost_weights]` table: what one CU buys of each resource, written the
/// way pricing is stated rather than the way it is stored.
///
/// The replicated [`CostWeights`] are Q32.32 µCU per (unit × second) — a
/// per-byte-second price is a number no human should have to write. Operators
/// price a resource by saying how much of it a CU buys:
///
/// ```toml
/// [cost_weights]
/// core_hours_per_cu = 1.0        # 1 core-hour costs 1 CU
/// memory_gib_hours_per_cu = 2.0  # 2 GiB-hours of memory cost 1 CU
/// disk_gib_hours_per_cu = 15.0   # 15 GiB-hours of disk cost 1 CU
/// ```
///
/// Bytes are binary throughout Coppice (`ByteSize` prints and parses `GiB`),
/// so a "GiB-hour" here is 2³⁰ bytes held for an hour — the same unit a job
/// spec's `memory = "256MiB"` counts in.
///
/// An omitted dimension is **free**, which is what a fresh cluster's default
/// weights already say; a zero or negative value is rejected rather than
/// silently meaning "free", because "0 core-hours per CU" reads as infinitely
/// expensive, not as free.
///
/// # Prices a Q32.32 weight cannot hold
///
/// The byte-granular dimensions are where the representation runs out. One
/// Q32.32 tick is ~2.3×10⁻¹⁰ µCU per byte-second, which is a price of ~1100
/// GiB-hours per CU — so a price near or beyond that rounds to a *materially*
/// different number, and past it to zero, i.e. free. Quoting
/// `disk_gib_hours_per_cu = 3000` and silently getting free disk is the
/// failure this guards: [`CostWeightsSpec::weights`] converts back and
/// **rejects** any price whose stored form differs by more than
/// [`PRICE_TOLERANCE`], naming the price it would actually have charged.
///
/// A rejected price is not a dead end. Cost is a scalar with no external unit
/// — only the ratios between the dimensions and the quota stock mean anything
/// — so an operator who wants disk very cheap relative to CPU prices *CPU up*
/// (and scales the quota entities to match) rather than pricing disk below
/// what a weight can hold.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostWeightsSpec {
    /// CPU core-hours that cost one CU.
    #[serde(default)]
    pub core_hours_per_cu: Option<f64>,
    /// Memory GiB-hours that cost one CU.
    #[serde(default)]
    pub memory_gib_hours_per_cu: Option<f64>,
    /// Disk GiB-hours that cost one CU.
    #[serde(default)]
    pub disk_gib_hours_per_cu: Option<f64>,
}

/// Seconds in an hour, the time unit every `*_per_cu` price is quoted over.
const SECONDS_PER_HOUR: f64 = 3600.0;
/// Milli-CPU in a core: the unit `Resources::cpu_millis` counts in.
const MILLI_PER_CORE: f64 = 1000.0;
/// Bytes in a GiB: the unit `Resources::memory`/`disk` count in.
const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;

/// How far a stored Q32.32 weight may sit from the price that was quoted: 5%.
///
/// Rounding to a whole tick is the only error this bounds, and it is
/// negligible until a byte-granular price is down to its last few ticks — so
/// the bar's job is to catch prices that are *materially* wrong (a price that
/// charges half, or nothing), not to promise exactness the charge path does
/// not offer anyway: `resource_rate` truncates the summed rate to whole µCU
/// per second, which already moves a small job's charge by more than a
/// percent. Tighter than this would reject ordinary cheap-storage prices
/// while buying no real accuracy.
const PRICE_TOLERANCE: f64 = 0.05;

impl CostWeightsSpec {
    /// The replicated Q32.32 weights this table describes, or an error naming
    /// a price the representation cannot hold faithfully.
    ///
    /// Each weight is `1 CU / (units × seconds that CU buys)`, in µCU, scaled
    /// by 2³². The result is rounded to a whole tick and then converted *back*
    /// to a price: a stored price further than [`PRICE_TOLERANCE`] from the
    /// quoted one — including the two extremes, a price so cheap it rounds to
    /// zero (free) and one so expensive it overflows `u64` — is an error at
    /// this edge rather than a silent mispricing in replicated state.
    fn weights(&self) -> Result<CostWeights> {
        Ok(CostWeights {
            per_cpu_milli_second: Self::weight(
                "core_hours_per_cu",
                self.core_hours_per_cu,
                MILLI_PER_CORE,
            )?,
            per_memory_byte_second: Self::weight(
                "memory_gib_hours_per_cu",
                self.memory_gib_hours_per_cu,
                BYTES_PER_GIB,
            )?,
            per_disk_byte_second: Self::weight(
                "disk_gib_hours_per_cu",
                self.disk_gib_hours_per_cu,
                BYTES_PER_GIB,
            )?,
        })
    }

    /// One dimension's Q32.32 weight: `units_per_quantity` is how many of the
    /// unit the *replicated* resource counts in (milli-CPU, bytes) make up one
    /// of the unit the price is quoted in (a core, a GiB).
    fn weight(field: &str, per_cu: Option<f64>, units_per_quantity: f64) -> Result<u64> {
        // Absent = free, the booted default for that dimension.
        let Some(per_cu) = per_cu else {
            return Ok(0);
        };
        // `validate` rejected non-finite and non-positive prices already, so
        // `exact` is finite and positive here.
        let unit_seconds = per_cu * units_per_quantity * SECONDS_PER_HOUR;
        let exact = (MICRO_PER_COST_UNIT as f64 / unit_seconds) * Q32_SCALE;

        // Too expensive to represent: the weight would saturate `u64` (a
        // float-to-int cast clamps silently, which is exactly the mispricing
        // this rejects).
        if exact > u64::MAX as f64 {
            bail!(
                "[cost_weights] {field} = {per_cu} is too expensive to price: one unit-second \
                 would cost more than a cost weight can hold"
            );
        }
        let stored = exact.round() as u64;
        // Too cheap to represent: the weight rounds to zero, which does not
        // mean "nearly free", it means free.
        if stored == 0 {
            bail!(
                "[cost_weights] {field} = {per_cu} is too cheap to price: it rounds to a weight \
                 of zero, which would make the resource free. The cheapest representable price \
                 is about {cheapest:.0}; price the other dimensions up instead — only the ratios \
                 between prices and quota stocks mean anything.",
                cheapest = Self::price_of(1, units_per_quantity),
            );
        }
        // Representable, but is it the price that was asked for? Convert the
        // stored weight back and compare.
        let charged = Self::price_of(stored, units_per_quantity);
        let error = (charged - per_cu).abs() / per_cu;
        if error > PRICE_TOLERANCE {
            bail!(
                "[cost_weights] {field} = {per_cu} cannot be priced accurately: the nearest \
                 representable weight charges as {charged:.4} units per CU ({percent:.1}% off, \
                 over the {tolerance:.0}% this schema allows). Prices this cheap run out of \
                 cost-weight resolution — price the other dimensions up instead, since only the \
                 ratios between prices and quota stocks mean anything.",
                percent = error * 100.0,
                tolerance = PRICE_TOLERANCE * 100.0,
            );
        }
        Ok(stored)
    }

    /// The price, in units per CU, that a stored Q32.32 `weight` actually
    /// charges — the inverse of the conversion in [`Self::weight`].
    fn price_of(weight: u64, units_per_quantity: f64) -> f64 {
        (MICRO_PER_COST_UNIT as f64 * Q32_SCALE)
            / (weight as f64 * units_per_quantity * SECONDS_PER_HOUR)
    }

    /// Reject prices serde cannot: non-finite, negative, or zero.
    fn validate(&self) -> Result<()> {
        let check = |field: &str, value: Option<f64>| -> Result<()> {
            match value {
                None => Ok(()),
                Some(price) if price.is_finite() && price > 0.0 => Ok(()),
                Some(price) => bail!(
                    "[cost_weights] {field} must be a finite, positive number of units per CU \
                     (got {price}); omit the field to leave that resource free"
                ),
            }
        };
        check("core_hours_per_cu", self.core_hours_per_cu)?;
        check("memory_gib_hours_per_cu", self.memory_gib_hours_per_cu)?;
        check("disk_gib_hours_per_cu", self.disk_gib_hours_per_cu)?;
        // Fidelity is part of validity: a price that cannot be stored as the
        // price it quotes fails here, at parse, not at seeding time.
        self.weights()?;
        Ok(())
    }
}

/// One `[[quota_entity]]` entry: a quota leaf jobs charge against, named by
/// its **path** (ADR 0045) — the parent is derived from the path, never
/// stated separately.
///
/// Entries may appear in any document order: [`FormationPolicy::commands`]
/// processes them in ascending path depth, so a listed parent is always
/// resolved before its children regardless of where it sits in the file. A
/// single-segment path is a root. Each entry's parent path (`path.parent()`)
/// must be declared elsewhere in the same document or already exist in
/// cluster state — otherwise seeding fails naming both paths, rather than
/// leaving a half-built tree.
///
/// Idempotent re-apply, by path: an entry whose path already resolves in
/// state is left untouched (reconfiguration is not an amnesty, and a re-run
/// must not reset accumulated usage). If it also declares an `id` that
/// differs from the entity actually holding that path, seeding fails naming
/// the path, the holder, and the declared id — a document that thinks it is
/// re-seeding an id-known entity but is aimed at the wrong path is a mistake,
/// not a no-op. An entry whose declared `id` already exists in state under a
/// *different* path (e.g. it was renamed or moved since a prior seeding run)
/// is left untouched too, since the entity this document meant to create
/// already exists. Its stale path does not resolve to anything for the rest
/// of the document, though: a child entry beneath it, or an
/// `[[authorization.binding]]` scope naming it, fails seeding with the
/// entity's current path, rather than silently landing under (or binding to)
/// wherever the entity lives now.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaEntitySpec {
    /// The entity's path from a root, e.g. `acme/eng/platform`. Every
    /// segment must meet the name grammar (ADR 0045); the last segment
    /// becomes the created entity's `name`.
    pub path: QuotaEntityPath,
    /// The quota stock in µCU (ADR 0019).
    pub quota: u64,
    /// The entity id (`quota-<uuid>`) to create it with, when creation is
    /// needed. Minted with `QuotaEntityId::new()` when absent. An explicit id
    /// lets other parts of the document, or a later document, reference this
    /// entity without waiting on path resolution.
    #[serde(default)]
    pub id: Option<QuotaEntityId>,
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
        if let Some(weights) = &self.cost_weights {
            weights.validate()?;
        }
        if let Some(retention) = &self.retention {
            retention.validate()?;
        }
        let mut seen_path = std::collections::BTreeSet::new();
        let mut seen_id = std::collections::BTreeSet::new();
        for qe in &self.quota_entities {
            if !seen_path.insert(&qe.path) {
                bail!("duplicate quota entity path {}", qe.path);
            }
            if let Some(id) = qe.id {
                if !seen_id.insert(id) {
                    bail!("duplicate quota entity id {id}");
                }
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
        if let Some(authz) = &self.authorization {
            if authz
                .groups_claim
                .as_deref()
                .is_some_and(|claim| claim.trim().is_empty())
            {
                bail!("[authorization] groups_claim is empty or whitespace-only");
            }
            let bindings = authz.bindings()?;
            // Apply refuses a list with no unscoped admin (AuthorizationLockout);
            // refusing it here names the document instead of failing formation
            // halfway through. An empty list is the same lockout: absence of
            // the table already means "leave authorization untouched", so a
            // table that is present but binds nobody is a mistake, not a
            // no-op — it would leave an OIDC-only cluster looking initialised
            // while nobody can act.
            if !bindings
                .iter()
                .any(|b| b.role == Role::Admin && b.scope.is_none())
            {
                bail!(
                    "[authorization] would install no unscoped admin binding; at least one is \
                     required so an accidental lockout stays loud (ADR 0023)"
                );
            }
            // Scopes are checked in `commands`, where the existing entities
            // are known.
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
    /// Quota entities are resolved **in ascending path depth** regardless of
    /// their document order (see [`resolve_entities`](Self::resolve_entities)):
    /// a listed parent is always resolved before its children. A parent path
    /// that is neither declared elsewhere in the document nor already in
    /// `state` is an error here — at the seeding edge — rather than a
    /// mid-apply rejection. Unlike the old id-parent scheme, a path can never
    /// cycle: a path's parent is always a strictly shorter string.
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

        // Priority table and resource prices: seed each only while the
        // replicated value is still its booted default — an empty table, and
        // all-zero (free) weights. `UpdatePolicy` is a full replacement, so
        // clone the current policy and change only what this document seeds;
        // every other field keeps its booted default, and an operator's later
        // edits survive a re-apply.
        let seeds_multipliers =
            !self.priority_multipliers.is_empty() && state.policy.priority_multipliers.is_empty();
        // Compare the weights this document *produces*, not the presence of
        // the table: a `[cost_weights]` that prices nothing (every field
        // omitted) converts to the default weights, and proposing those over
        // the identical replicated ones would be a no-op command emitted on
        // every re-run.
        let weights = self.cost_weights.map(|spec| spec.weights()).transpose()?;
        let seeds_weights = weights.is_some_and(|weights| {
            weights != CostWeights::default() && state.policy.cost_weights == CostWeights::default()
        });
        // Retention windows, under the same seed-only-from-default rule.
        let booted = PolicyConfig::default();
        let retention = self.retention.unwrap_or_default();
        let seeds_node_retention = retention.node.map(Duration::from).is_some_and(|window| {
            window != booted.node_retention && state.policy.node_retention == booted.node_retention
        });
        let seeds_terminal_retention =
            retention
                .terminal
                .map(Duration::from)
                .is_some_and(|window| {
                    window != booted.terminal_retention
                        && state.policy.terminal_retention == booted.terminal_retention
                });
        if seeds_multipliers || seeds_weights || seeds_node_retention || seeds_terminal_retention {
            let mut policy = state.policy.clone();
            if seeds_multipliers {
                policy.priority_multipliers = self.multiplier_table();
            }
            if let (true, Some(weights)) = (seeds_weights, weights) {
                policy.cost_weights = weights;
            }
            if let (true, Some(window)) = (seeds_node_retention, retention.node) {
                policy.node_retention = Duration::from(window);
            }
            if let (true, Some(window)) = (seeds_terminal_retention, retention.terminal) {
                policy.terminal_retention = Duration::from(window);
            }
            commands.push(Command::UpdatePolicy(UpdatePolicy {
                policy,
                updated_at: now,
                actor: None,
            }));
        }

        // Quota entities: create only those whose path does not already
        // resolve, parent before child. An entity already at its path is left
        // untouched — reconfiguration is not an amnesty, and a re-run must not
        // reset accumulated usage.
        let resolved_entities = self.resolve_entities(state)?;
        for entity in &resolved_entities {
            if entity.create {
                commands.push(Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
                    entity: entity.id,
                    parent: entity.parent,
                    name: entity.spec.path.leaf().to_string(),
                    quota: CostUnits(entity.spec.quota),
                    updated_at: now,
                    actor: None,
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

        // Role bindings: one full replacement, only while the replicated list
        // is still empty (the booted deny-everyone default). Once anything is
        // bound, the list belongs to `coppice policy authz set`, and a re-init
        // must not revert an operator's edits. A scoped binding may name an
        // entity resolved above (by path or by id): `seeded_by_path` /
        // `seeded_ids` cover both.
        if let Some(authz) = &self.authorization {
            let parsed = authz.bindings()?;
            // `validate` guaranteed at least one unscoped admin, so the list
            // is never empty here.
            if state.bindings.is_empty() {
                let seeded_by_path: BTreeMap<&QuotaEntityPath, QuotaEntityId> = resolved_entities
                    .iter()
                    .filter(|e| !e.moved)
                    .map(|e| (&e.spec.path, e.id))
                    .collect();
                // A moved entry's path names nothing any more; a scope naming
                // it is refused below rather than bound to the entity's new
                // location, which the document author may not intend.
                let moved_by_path: BTreeMap<&QuotaEntityPath, QuotaEntityId> = resolved_entities
                    .iter()
                    .filter(|e| e.moved)
                    .map(|e| (&e.spec.path, e.id))
                    .collect();
                let seeded_ids: BTreeSet<QuotaEntityId> =
                    resolved_entities.iter().map(|e| e.id).collect();
                let mut bindings = Vec::with_capacity(parsed.len());
                for pb in &parsed {
                    let scope = match &pb.scope {
                        None => None,
                        Some(scope_ref) => {
                            let resolved = match scope_ref {
                                QuotaEntityRef::Id(id) => (seeded_ids.contains(id)
                                    || state.quota_entities.contains_key(id))
                                .then_some(*id),
                                QuotaEntityRef::Path(path) => {
                                    if let Some(id) = moved_by_path.get(path) {
                                        bail!(
                                            "[[authorization.binding]] scope {path} names a \
                                             [[quota_entity]] entry whose declared id {id} now \
                                             lives at {}; update the document to the current path",
                                            current_path(state, *id)
                                        );
                                    }
                                    seeded_by_path
                                        .get(path)
                                        .copied()
                                        .or_else(|| state.resolve_quota_entity_path(path))
                                }
                            };
                            let Some(resolved) = resolved else {
                                bail!(
                                    "[[authorization.binding]] scope {scope_ref} is neither \
                                     seeded by this document nor an existing quota entity"
                                );
                            };
                            Some(resolved)
                        }
                    };
                    bindings.push(Binding {
                        subject: pb.subject.clone(),
                        role: pb.role,
                        scope,
                    });
                }
                commands.push(Command::UpdateAuthorization(UpdateAuthorization {
                    bindings,
                    actor: None,
                    updated_at: now,
                    groups_claim: authz.groups_claim.clone(),
                }));
            }
        }

        Ok(commands)
    }

    /// This document's quota entities resolved against `state`, in ascending
    /// path depth — a listed parent is always resolved before its children,
    /// whatever order the document lists them in. A path can never cycle (a
    /// path's parent is always a strictly shorter string), so unlike an
    /// id-parent scheme this cannot get stuck.
    ///
    /// For each entry:
    /// - if its path already resolves in `state`, it is left untouched
    ///   (`create: false`) — unless the entry also declares an `id` that
    ///   disagrees with the entity actually holding that path, which is a
    ///   document error naming the path, the holder, and the declared id;
    /// - otherwise, if it declares an `id` that already exists in `state`
    ///   (renamed or moved since a prior seeding run), it is left untouched
    ///   too (`moved: true`) — the entity this entry meant to create already
    ///   exists. Its stale path is *not* recorded as resolving to that id: a
    ///   later child entry beneath it is a document error naming the
    ///   entity's current path, rather than being silently created under the
    ///   entity's new location;
    /// - otherwise its parent path is resolved (against `state`, or against
    ///   the id this same run chose for an earlier, shallower entry) and it
    ///   is created (`create: true`) with the declared id or a freshly minted
    ///   one.
    ///
    /// Every entry's resolved id — created or not — is returned, so a caller
    /// can resolve a child's parent path or an `[[authorization.binding]]`
    /// `scope` against exactly what this run seeds.
    fn resolve_entities(&self, state: &StateMachine) -> Result<Vec<ResolvedEntity<'_>>> {
        let mut specs: Vec<&QuotaEntitySpec> = self.quota_entities.iter().collect();
        specs.sort_by_key(|qe| qe.path.depth());

        let mut resolved = Vec::with_capacity(specs.len());
        let mut chosen: BTreeMap<&QuotaEntityPath, QuotaEntityId> = BTreeMap::new();
        // Paths of entries whose declared id now lives elsewhere; a child
        // under one of these must not resolve its parent at all.
        let mut moved: BTreeMap<&QuotaEntityPath, QuotaEntityId> = BTreeMap::new();

        for spec in specs {
            if let Some(existing) = state.resolve_quota_entity_path(&spec.path) {
                if let Some(declared) = spec.id {
                    if declared != existing {
                        bail!(
                            "quota entity path {} is held by {existing}, but this document \
                             declares id {declared} for it",
                            spec.path
                        );
                    }
                }
                chosen.insert(&spec.path, existing);
                resolved.push(ResolvedEntity {
                    spec,
                    id: existing,
                    parent: None,
                    create: false,
                    moved: false,
                });
                continue;
            }

            if let Some(declared) = spec.id {
                if state.quota_entities.contains_key(&declared) {
                    moved.insert(&spec.path, declared);
                    resolved.push(ResolvedEntity {
                        spec,
                        id: declared,
                        parent: None,
                        create: false,
                        moved: true,
                    });
                    continue;
                }
            }

            let parent = match spec.path.parent() {
                None => None,
                Some(parent_path) => {
                    if let Some(parent_id) = moved.get(&parent_path) {
                        bail!(
                            "quota entity {} has parent path {parent_path}, but that entry's \
                             declared id {parent_id} now lives at {}; update the document to \
                             the entity's current path",
                            spec.path,
                            current_path(state, *parent_id)
                        );
                    }
                    let parent_id = state
                        .resolve_quota_entity_path(&parent_path)
                        .or_else(|| chosen.get(&parent_path).copied());
                    let Some(parent_id) = parent_id else {
                        bail!(
                            "quota entity {} has parent path {parent_path}, which is neither \
                             declared in this policy document nor already in cluster state",
                            spec.path
                        );
                    };
                    Some(parent_id)
                }
            };
            let id = spec.id.unwrap_or_else(QuotaEntityId::new);
            chosen.insert(&spec.path, id);
            resolved.push(ResolvedEntity {
                spec,
                id,
                parent,
                create: true,
                moved: false,
            });
        }

        Ok(resolved)
    }
}

/// `id`'s current path in `state`, for error messages about an entry whose
/// declared id has moved. The id is known to exist, so the fallback is only
/// defensive.
fn current_path(state: &StateMachine, id: QuotaEntityId) -> String {
    state
        .quota_entity_path(id)
        .unwrap_or_else(|| id.to_string())
}

/// One [`QuotaEntitySpec`] resolved against replicated state by
/// [`FormationPolicy::resolve_entities`]: the id this run assigns to its
/// path (whether newly minted or already the path's/id's holder), the
/// resolved parent id, and whether a `ConfigureQuotaEntity` must be proposed
/// to realize it.
struct ResolvedEntity<'a> {
    spec: &'a QuotaEntitySpec,
    id: QuotaEntityId,
    parent: Option<QuotaEntityId>,
    create: bool,
    /// The entry's declared id exists in state but no longer at the entry's
    /// path (renamed or moved since a prior seeding). Its path must not
    /// resolve to anything for a child entry or a binding scope.
    moved: bool,
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
    use coppice_core::bytes::ByteSize;
    use coppice_core::resource::Resources;

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
path = "default"
quota = 1000000000000
"#;

    /// The host-packaging template (`deploy/examples/policy.toml`), the
    /// document the AWS demo's formation step renders and applies. Compiled in
    /// so a schema change fails here rather than at `coppice coordinator init`
    /// on an instance mid-bringup, which is the one moment with no fallback.
    const DEPLOY_EXAMPLE: &str = include_str!("../../../deploy/examples/policy.toml");

    #[test]
    fn deploy_example_parses() {
        let policy =
            FormationPolicy::parse_toml(DEPLOY_EXAMPLE.as_bytes()).expect("deploy example parses");
        assert_eq!(policy.quota_entities.len(), 1);
        assert!(policy.cost_weights.is_some());
        // The retention windows the demo states explicitly (ADR 0012, 0041).
        let retention = policy.retention.expect("[retention]");
        assert!(retention.node.is_some() && retention.terminal.is_some());
        // Both launch roles are seeded, each under the label that makes
        // re-applying the policy a no-op (ADR 0037 §5).
        let labels: Vec<&str> = policy
            .enroll_tokens
            .iter()
            .map(|token| token.label.as_str())
            .collect();
        assert_eq!(labels, ["coordinator-launch", "agent-launch"]);
        assert!(policy.enroll_tokens.iter().all(|token| token.ttl.is_none()));
        // The placeholders must differ: `verify_enroll_token` keeps the last
        // hash that matches, so two roles seeded from one secret would hand
        // whichever token sorts last to every enrollee, and one role's
        // enrollment would fail.
        let secrets: std::collections::HashSet<&str> = policy
            .enroll_tokens
            .iter()
            .map(|token| token.secret.as_str())
            .collect();
        assert_eq!(secrets.len(), policy.enroll_tokens.len());
        // Day-0 authorization: the demo's Cognito group is an unscoped admin,
        // read from the claim Cognito actually uses.
        let authz = policy.authorization.as_ref().expect("[authorization]");
        assert_eq!(authz.groups_claim.as_deref(), Some("cognito:groups"));
        let bindings = authz.bindings().expect("valid bindings");
        assert!(bindings.iter().any(|b| {
            b.subject == Subject::Group("coppice-admins".to_string())
                && b.role == Role::Admin
                && b.scope.is_none()
        }));
    }

    #[test]
    fn parses_the_sample_schema() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).expect("sample parses");
        assert_eq!(policy.priority_multipliers.len(), 3);
        assert_eq!(policy.quota_entities.len(), 1);
        assert_eq!(policy.quota_entities[0].path.as_str(), "default");
        assert_eq!(policy.quota_entities[0].quota, 1_000_000_000_000);
        assert!(policy.quota_entities[0].id.is_none());
    }

    /// The `[retention]` table parses as humantime spans and rejects a zero
    /// window.
    #[test]
    fn retention_windows_parse_and_reject_zero() {
        let policy =
            FormationPolicy::parse_toml(b"[retention]\nnode = \"6h\"\nterminal = \"7d\"\n")
                .expect("retention parses");
        let retention = policy.retention.expect("[retention]");
        assert_eq!(
            retention.node,
            Some(std::time::Duration::from_secs(6 * 3600))
        );
        assert_eq!(
            retention.terminal,
            Some(std::time::Duration::from_secs(7 * 24 * 3600))
        );

        // Either field alone is fine; the other keeps its booted default.
        let partial = FormationPolicy::parse_toml(b"[retention]\nnode = \"30m\"\n")
            .expect("a partial table parses");
        assert_eq!(partial.retention.expect("[retention]").terminal, None);

        let err = FormationPolicy::parse_toml(b"[retention]\nnode = \"0s\"\n")
            .expect_err("zero is rejected");
        assert!(format!("{err:#}").contains("non-zero"), "{err:#}");
    }

    /// Retention rides the same full-replacement `UpdatePolicy` as the prices
    /// and the priority table, under the same rule: seeded only while the
    /// replicated field still holds its booted default, so a re-apply is a
    /// no-op and an operator's edit survives.
    #[test]
    fn retention_seeds_the_policy_command_only_from_the_booted_default() {
        let policy =
            FormationPolicy::parse_toml(b"[retention]\nnode = \"6h\"\nterminal = \"7d\"\n")
                .unwrap();
        let now = Timestamp::now();

        let fresh = StateMachine::default();
        let commands = policy.commands(&fresh, now, CHEAP_KDF).expect("valid");
        match &commands[..] {
            [Command::UpdatePolicy(up)] => {
                assert_eq!(up.policy.node_retention, Duration::from_hours(6));
                assert_eq!(up.policy.terminal_retention, Duration::from_days(7));
                // A full replacement that changes nothing else.
                assert_eq!(up.policy.cost_weights, fresh.policy.cost_weights);
            }
            other => panic!("expected one UpdatePolicy, got {other:?}"),
        }

        // Already applied: nothing to propose.
        let mut applied = StateMachine::default();
        applied.policy.node_retention = Duration::from_hours(6);
        applied.policy.terminal_retention = Duration::from_days(7);
        assert!(policy
            .commands(&applied, now, CHEAP_KDF)
            .expect("valid")
            .is_empty());

        // An operator moved one window off the default: that one is left
        // alone, and only the other is seeded.
        let mut edited = StateMachine::default();
        edited.policy.node_retention = Duration::from_hours(48);
        match &policy.commands(&edited, now, CHEAP_KDF).expect("valid")[..] {
            [Command::UpdatePolicy(up)] => {
                assert_eq!(up.policy.node_retention, Duration::from_hours(48));
                assert_eq!(up.policy.terminal_retention, Duration::from_days(7));
            }
            other => panic!("expected one UpdatePolicy, got {other:?}"),
        }

        // A document that restates the booted defaults proposes nothing.
        let restated =
            FormationPolicy::parse_toml(b"[retention]\nnode = \"24h\"\nterminal = \"72h\"\n")
                .unwrap();
        assert!(restated
            .commands(&StateMachine::default(), now, CHEAP_KDF)
            .expect("valid")
            .is_empty());
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
        let bad = format!("{SAMPLE}\n[[quota_entity]]\npath = \"other\"\nquota = 1\nbogus = 3\n");
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
        let id = QuotaEntityId::new();
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

    // ---- resource prices (`[cost_weights]`) ------------------------------

    const PRICES: &str = r#"
[cost_weights]
core_hours_per_cu = 1.0
memory_gib_hours_per_cu = 2.0
disk_gib_hours_per_cu = 15.0
"#;

    /// The gross cost, in µCU, of holding `requests` for an hour at the
    /// document's prices — the number the quoted prices are a statement
    /// about.
    fn cost_per_hour(weights: &CostWeights, requests: Resources) -> u64 {
        let rate = coppice_core::quota::resource_rate(&requests, weights);
        coppice_core::quota::cost_from_rate(rate, 3600, PriorityMultiplier::ONE).0
    }

    /// The prices mean what they say: one core-hour, two GiB-hours of memory,
    /// and fifteen GiB-hours of disk each cost one CU. Two roundings sit
    /// between the quote and the charge — the Q32.32 weight, and
    /// `resource_rate`'s truncation to whole µCU per second — so assert to
    /// within a percent rather than exactly.
    #[test]
    fn quoted_prices_charge_what_they_quote() {
        let policy = FormationPolicy::parse_toml(PRICES.as_bytes()).expect("prices parse");
        let weights = policy
            .cost_weights
            .expect("weights present")
            .weights()
            .expect("prices are representable");

        let one_cu = MICRO_PER_COST_UNIT;
        let tolerance = one_cu / 100;
        for (what, requests) in [
            (
                "1 core for an hour",
                Resources {
                    cpu_millis: 1000,
                    memory: ByteSize::ZERO,
                    disk: ByteSize::ZERO,
                },
            ),
            (
                "2 GiB of memory for an hour",
                Resources {
                    cpu_millis: 0,
                    memory: ByteSize::from_gib(2),
                    disk: ByteSize::ZERO,
                },
            ),
            (
                "15 GiB of disk for an hour",
                Resources {
                    cpu_millis: 0,
                    memory: ByteSize::ZERO,
                    disk: ByteSize::from_gib(15),
                },
            ),
        ] {
            let charged = cost_per_hour(&weights, requests);
            assert!(
                charged.abs_diff(one_cu) <= tolerance,
                "{what} should cost ~1 CU, charged {charged} µCU"
            );
        }
    }

    /// An omitted dimension is free, and leaves the other two priced.
    #[test]
    fn an_omitted_dimension_is_free() {
        let policy = FormationPolicy::parse_toml(b"[cost_weights]\ncore_hours_per_cu = 1.0\n")
            .expect("partial table parses");
        let weights = policy
            .cost_weights
            .expect("weights present")
            .weights()
            .expect("prices are representable");
        assert_eq!(weights.per_memory_byte_second, 0);
        assert_eq!(weights.per_disk_byte_second, 0);
        assert!(weights.per_cpu_milli_second > 0);
    }

    #[test]
    fn a_zero_or_negative_price_is_rejected() {
        for bad in [
            "[cost_weights]\ncore_hours_per_cu = 0.0\n",
            "[cost_weights]\nmemory_gib_hours_per_cu = -2.0\n",
        ] {
            let err = FormationPolicy::parse_toml(bad.as_bytes()).expect_err("rejected");
            assert!(
                format!("{err:#}").contains("positive number of units per CU"),
                "{err:#}"
            );
        }
    }

    /// A price the Q32.32 weight cannot hold is rejected at parse, naming what
    /// it would actually have charged. `3000` GiB-hours per CU rounds to a
    /// zero weight (free disk); `2000` rounds to one tick, ~1100 GiB-hours per
    /// CU — nearly half price; and an absurdly small price saturates.
    #[test]
    fn a_price_the_weight_cannot_hold_is_rejected() {
        let too_cheap = FormationPolicy::parse_toml(
            b"[cost_weights]\ndisk_gib_hours_per_cu = 3000.0\n" as &[u8],
        )
        .expect_err("free disk is rejected");
        assert!(
            format!("{too_cheap:#}").contains("too cheap to price"),
            "{too_cheap:#}"
        );

        let inaccurate = FormationPolicy::parse_toml(
            b"[cost_weights]\ndisk_gib_hours_per_cu = 2000.0\n" as &[u8],
        )
        .expect_err("a materially different price is rejected");
        let message = format!("{inaccurate:#}");
        assert!(message.contains("cannot be priced accurately"), "{message}");
        assert!(message.contains("units per CU"), "{message}");

        let too_dear = FormationPolicy::parse_toml(
            b"[cost_weights]\nmemory_gib_hours_per_cu = 1e-30\n" as &[u8],
        )
        .expect_err("a saturating price is rejected");
        assert!(
            format!("{too_dear:#}").contains("too expensive to price"),
            "{too_dear:#}"
        );
    }

    /// The accuracy bar is a boundary, not a vibe. One Q32.32 tick on a byte
    /// dimension is a price of ~1100 GiB-hours per CU, so prices are exact
    /// while they round to many ticks and get coarse as the ticks run out;
    /// the bar sits where the rounding starts to matter.
    #[test]
    fn the_accuracy_bar_is_where_the_ticks_run_out() {
        // Tens of ticks or more: the dev disk price and its neighbourhood.
        for price in ["1.0", "15.0", "100.0"] {
            let doc = format!("[cost_weights]\ndisk_gib_hours_per_cu = {price}\n");
            FormationPolicy::parse_toml(doc.as_bytes())
                .unwrap_or_else(|e| panic!("{price} GiB-hours per CU should price: {e:#}"));
        }
        // A handful of ticks or fewer: rounding moves the charge by more than
        // the bar allows, so the price is refused rather than approximated.
        for price in ["500.0", "1000.0", "2000.0"] {
            let doc = format!("[cost_weights]\ndisk_gib_hours_per_cu = {price}\n");
            let err = FormationPolicy::parse_toml(doc.as_bytes())
                .err()
                .unwrap_or_else(|| panic!("{price} GiB-hours per CU should be rejected"));
            assert!(
                format!("{err:#}").contains("cannot be priced accurately"),
                "{err:#}"
            );
        }
    }

    /// A `[cost_weights]` table that prices nothing is a no-op, not a command:
    /// it converts to the same all-zero weights the cluster already has, and
    /// emitting an `UpdatePolicy` for it would grow the raft log on every
    /// `init` re-run (which is idempotent by construction).
    #[test]
    fn an_empty_price_table_proposes_nothing_however_often_it_is_applied() {
        let policy = FormationPolicy::parse_toml(b"[cost_weights]\n").expect("empty table parses");
        let state = StateMachine::default();
        for _ in 0..3 {
            assert!(policy
                .commands(&state, Timestamp::now(), CHEAP_KDF)
                .expect("valid policy")
                .is_empty());
        }
    }

    /// Prices ride the one `UpdatePolicy` the priority table already emits,
    /// rather than a second full-replacement command that would race it.
    #[test]
    fn prices_and_the_table_seed_in_one_command() {
        let doc = format!("{SAMPLE}{PRICES}");
        let policy = FormationPolicy::parse_toml(doc.as_bytes()).expect("doc parses");
        let commands = policy
            .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
            .expect("valid policy");
        let updates: Vec<_> = commands
            .iter()
            .filter_map(|c| match c {
                Command::UpdatePolicy(u) => Some(u),
                _ => None,
            })
            .collect();
        assert_eq!(updates.len(), 1);
        assert!(!updates[0].policy.priority_multipliers.is_empty());
        assert_ne!(updates[0].policy.cost_weights, CostWeights::default());
    }

    /// Already-priced weights are an operator's, not ours: a re-apply leaves
    /// them alone, exactly as it leaves a seeded priority table alone.
    #[test]
    fn prices_are_skipped_when_already_seeded() {
        let policy = FormationPolicy::parse_toml(PRICES.as_bytes()).expect("prices parse");
        let mut state = StateMachine::default();
        state.policy.cost_weights = CostWeights {
            per_cpu_milli_second: 7,
            ..CostWeights::default()
        };
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

    // ---- entity ordering and resolution (ADR 0045 paths) ------------------

    /// One `[[quota_entity]]` entry, at `path`, optionally with an explicit
    /// `id`.
    fn entity_toml(path: &str, id: Option<QuotaEntityId>) -> String {
        let id_line = id.map(|id| format!("id = \"{id}\"\n")).unwrap_or_default();
        format!("[[quota_entity]]\npath = \"{path}\"\nquota = 1\n{id_line}\n")
    }

    #[test]
    fn entities_listed_child_first_are_emitted_parent_first() {
        // Grandchild, child, parent — reverse hierarchy order in the document.
        let toml = format!(
            "{}{}{}",
            entity_toml("a/b/c", None),
            entity_toml("a/b", None),
            entity_toml("a", None),
        );
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let commands = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect("valid hierarchy");
        // The state resolved after each command, so children are emitted
        // exactly once their parent is present — regardless of document order.
        assert_eq!(commands.len(), 3);
        let mut applied = state.clone();
        for command in &commands {
            let Command::ConfigureQuotaEntity(cfg) = command else {
                panic!("expected ConfigureQuotaEntity, got {command:?}")
            };
            if let Some(parent) = cfg.parent {
                assert!(
                    applied.quota_entities.contains_key(&parent),
                    "parent must already exist when its child is proposed"
                );
            }
            applied.quota_entities.insert(
                cfg.entity,
                coppice_state::QuotaEntity {
                    parent: cfg.parent,
                    name: cfg.name.clone(),
                    quota: cfg.quota,
                    usage: coppice_core::quota::UsageState::new(Timestamp::now()),
                    created_at: Timestamp::now(),
                    updated_at: Timestamp::now(),
                },
            );
        }
        assert_eq!(
            applied.quota_entity_path(commands_leaf_id(&commands, "c")),
            Some("a/b/c".to_string())
        );
    }

    /// The id a `ConfigureQuotaEntity` command created whose `name` is `leaf`.
    fn commands_leaf_id(commands: &[Command], leaf: &str) -> QuotaEntityId {
        commands
            .iter()
            .find_map(|c| match c {
                Command::ConfigureQuotaEntity(cfg) if cfg.name == leaf => Some(cfg.entity),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no ConfigureQuotaEntity named {leaf:?}"))
    }

    #[test]
    fn entities_may_be_listed_in_any_order() {
        // Every permutation of parent-before/after-child must resolve to the
        // same tree.
        for toml in [
            format!(
                "{}{}{}",
                entity_toml("a", None),
                entity_toml("a/b", None),
                entity_toml("a/b/c", None),
            ),
            format!(
                "{}{}{}",
                entity_toml("a/b", None),
                entity_toml("a/b/c", None),
                entity_toml("a", None),
            ),
        ] {
            let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
            let commands = policy
                .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
                .expect("valid hierarchy, whatever the document order");
            assert_eq!(commands.len(), 3);
        }
    }

    #[test]
    fn child_of_a_parent_already_in_state_needs_no_document_parent() {
        // The parent exists only in replicated state (e.g. operator-created or
        // a prior seeding run); the document lists just the child.
        let toml = entity_toml("a/b", None);
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        let parent_id = QuotaEntityId::new();
        state.quota_entities.insert(
            parent_id,
            coppice_state::QuotaEntity {
                parent: None,
                name: "a".to_string(),
                quota: CostUnits(1),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        let commands = policy
            .commands(&state, now, CHEAP_KDF)
            .expect("parent found in state");
        assert_eq!(commands.len(), 1);
        let Command::ConfigureQuotaEntity(cfg) = &commands[0] else {
            panic!("expected ConfigureQuotaEntity");
        };
        assert_eq!(cfg.parent, Some(parent_id));
        assert_eq!(cfg.name, "b");
    }

    #[test]
    fn missing_parent_is_rejected_before_any_command_is_emitted() {
        let toml = entity_toml("a/b", None); // "a" nowhere
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let state = StateMachine::default();
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("dangling parent must be rejected");
        let message = format!("{err:#}");
        assert!(message.contains("a/b"), "{message}");
        assert!(message.contains("parent path a"), "{message}");
        assert!(
            message
                .contains("neither declared in this policy document nor already in cluster state"),
            "{message}"
        );
    }

    #[test]
    fn duplicate_path_is_rejected() {
        let toml = format!("{}{}", entity_toml("a", None), entity_toml("a", None));
        let err = FormationPolicy::parse_toml(toml.as_bytes()).expect_err("duplicate path");
        assert!(
            format!("{err:#}").contains("duplicate quota entity path"),
            "{err:#}"
        );
    }

    #[test]
    fn bad_segment_in_a_path_is_rejected_at_parse() {
        let toml = entity_toml("acme/-eng", None);
        let err = FormationPolicy::parse_toml(toml.as_bytes()).expect_err("bad segment");
        assert!(format!("{err:#}").contains("acme/-eng"), "{err:#}");
    }

    #[test]
    fn reapply_against_state_is_idempotent_even_with_minted_ids() {
        // No explicit id: the first apply mints one. A second apply against
        // the resulting state must find the path already resolved and emit
        // nothing, without ever needing to know the minted id.
        let toml = format!("{}{}", entity_toml("a", None), entity_toml("a/b", None));
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        for command in policy
            .commands(&state, now, CHEAP_KDF)
            .expect("first apply")
        {
            let Command::ConfigureQuotaEntity(cfg) = command else {
                panic!("expected ConfigureQuotaEntity")
            };
            state.quota_entities.insert(
                cfg.entity,
                coppice_state::QuotaEntity {
                    parent: cfg.parent,
                    name: cfg.name,
                    quota: cfg.quota,
                    usage: coppice_core::quota::UsageState::new(now),
                    created_at: now,
                    updated_at: now,
                },
            );
        }
        assert!(policy
            .commands(&state, now, CHEAP_KDF)
            .expect("second apply")
            .is_empty());
    }

    #[test]
    fn declared_id_disagreeing_with_the_paths_holder_is_rejected() {
        let holder = QuotaEntityId::new();
        let declared = QuotaEntityId::new();
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.quota_entities.insert(
            holder,
            coppice_state::QuotaEntity {
                parent: None,
                name: "a".to_string(),
                quota: CostUnits(1),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        let toml = entity_toml("a", Some(declared));
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let err = policy
            .commands(&state, now, CHEAP_KDF)
            .expect_err("mismatched declared id is rejected");
        let message = format!("{err:#}");
        assert!(message.contains("a"), "{message}");
        assert!(message.contains(&holder.to_string()), "{message}");
        assert!(message.contains(&declared.to_string()), "{message}");
    }

    #[test]
    fn declared_id_already_existing_elsewhere_is_left_untouched() {
        // The entity was renamed/moved since a prior seeding run: its id
        // exists, but no longer at this document's path.
        let id = QuotaEntityId::new();
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.quota_entities.insert(
            id,
            coppice_state::QuotaEntity {
                parent: None,
                name: "renamed".to_string(),
                quota: CostUnits(1),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        let toml = entity_toml("a", Some(id));
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        assert!(policy
            .commands(&state, now, CHEAP_KDF)
            .expect("left untouched, not an error")
            .is_empty());
    }

    /// State with `other` (a root) and the declared entity moved beneath it
    /// as `other/acme`, as if an operator reparented it after a first seeding
    /// run of a document that still names it `acme`.
    fn state_with_moved_entity(moved: QuotaEntityId) -> StateMachine {
        let now = Timestamp::now();
        let other = QuotaEntityId::new();
        let mut state = StateMachine::default();
        for (id, parent, name) in [(other, None, "other"), (moved, Some(other), "acme")] {
            state.quota_entities.insert(
                id,
                coppice_state::QuotaEntity {
                    parent,
                    name: name.to_string(),
                    quota: CostUnits(1),
                    usage: coppice_core::quota::UsageState::new(now),
                    created_at: now,
                    updated_at: now,
                },
            );
        }
        state
    }

    #[test]
    fn child_of_a_moved_entry_is_rejected_not_created_at_the_new_location() {
        let moved = QuotaEntityId::new();
        let state = state_with_moved_entity(moved);
        let toml = format!(
            "{}{}",
            entity_toml("acme", Some(moved)),
            entity_toml("acme/child", None)
        );
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("a child beneath a moved entry must not be created");
        let message = format!("{err:#}");
        assert!(message.contains("acme/child"), "{message}");
        assert!(message.contains(&moved.to_string()), "{message}");
        assert!(message.contains("now lives at other/acme"), "{message}");
    }

    #[test]
    fn binding_scope_naming_a_moved_entrys_path_is_rejected() {
        let moved = QuotaEntityId::new();
        let state = state_with_moved_entity(moved);
        let toml = format!(
            "{}[authorization]\n\
             [[authorization.binding]]\ngroup = \"admins\"\nrole = \"admin\"\n\
             [[authorization.binding]]\ngroup = \"team\"\nrole = \"submitter\"\nscope = \"acme\"\n",
            entity_toml("acme", Some(moved)),
        );
        let policy = FormationPolicy::parse_toml(toml.as_bytes()).expect("parses");
        let err = policy
            .commands(&state, Timestamp::now(), CHEAP_KDF)
            .expect_err("a scope naming a stale path must not bind the moved entity");
        let message = format!("{err:#}");
        assert!(message.contains("scope acme"), "{message}");
        assert!(message.contains("now lives at other/acme"), "{message}");
    }

    /// Formation seeding proposes **actorless** commands, and must keep
    /// doing so (ADR 0023).
    ///
    /// The regression this pins is the mirror image of PR3's work: the API
    /// write path now attaches an `Actor` to every command it proposes, and
    /// an over-eager sweep of the same treatment across the codebase would
    /// break bootstrap outright. Seeding runs before any operator can hold a
    /// binding — the bindings list it would be checked against does not exist
    /// yet — so `actor: None` is not an omission here, it is the only thing
    /// that can work. `StateMachine::authorize` reads it as "an internal
    /// proposer carrying the system's own authority" and skips the check.
    #[test]
    fn seeding_commands_carry_no_actor() {
        let policy = FormationPolicy::parse_toml(SAMPLE.as_bytes()).unwrap();
        let commands = policy
            .commands(&StateMachine::default(), Timestamp::UNIX_EPOCH, CHEAP_KDF)
            .expect("seeding commands");
        assert!(!commands.is_empty(), "the sample seeds something");
        for command in &commands {
            let actor = match command {
                Command::UpdatePolicy(c) => c.actor.as_ref(),
                Command::ConfigureQuotaEntity(c) => c.actor.as_ref(),
                Command::MintEnrollToken(_) => continue,
                other => panic!("unexpected seeding command {other:?}"),
            };
            assert!(actor.is_none(), "seeding is not an API-originated write");
        }
    }

    const AUTHZ_SAMPLE: &str = r#"
[[quota_entity]]
path = "default"
quota = 1000000000000

[authorization]
groups_claim = "cognito:groups"

[[authorization.binding]]
group = "coppice-admins"
role = "admin"

[[authorization.binding]]
principal = "user-42"
role = "submitter"
scope = "default"
"#;

    #[test]
    fn authorization_seeds_the_bindings_after_the_entities_on_a_fresh_state() {
        let policy = FormationPolicy::parse_toml(AUTHZ_SAMPLE.as_bytes()).unwrap();
        let commands = policy
            .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
            .expect("valid policy");
        // The entity first (the scoped binding needs it to exist), then one
        // full-replacement UpdateAuthorization.
        assert_eq!(commands.len(), 2);
        let Command::ConfigureQuotaEntity(cfg) = &commands[0] else {
            panic!("expected ConfigureQuotaEntity, got {:?}", commands[0]);
        };
        let entity_id = cfg.entity;
        let Command::UpdateAuthorization(update) = &commands[1] else {
            panic!("expected UpdateAuthorization, got {:?}", commands[1]);
        };
        assert_eq!(update.groups_claim.as_deref(), Some("cognito:groups"));
        assert!(update.actor.is_none());
        assert_eq!(update.bindings.len(), 2);
        assert_eq!(
            update.bindings[0].subject,
            Subject::Group("coppice-admins".to_string())
        );
        assert_eq!(update.bindings[0].role, Role::Admin);
        assert!(update.bindings[0].scope.is_none());
        assert_eq!(
            update.bindings[1].subject,
            Subject::Principal("user-42".to_string())
        );
        assert_eq!(update.bindings[1].role, Role::Submitter);
        // The path-scoped binding resolved to the id this same run just
        // minted for it, before that id existed anywhere but this document.
        assert_eq!(update.bindings[1].scope, Some(entity_id));
    }

    #[test]
    fn authorization_scope_by_id_resolves_against_the_seeded_entity() {
        // A scope may also be given as an id (ADR 0045 accepts either) —
        // here, the explicit id this document declares for the entity.
        let explicit_id = QuotaEntityId::new();
        let doc = format!(
            "[[quota_entity]]\npath = \"default\"\nquota = 1\nid = \"{explicit_id}\"\n\n\
             [authorization]\n[[authorization.binding]]\ngroup = \"admins\"\nrole = \"admin\"\n\
             [[authorization.binding]]\ngroup = \"team\"\nrole = \"submitter\"\nscope = \"{explicit_id}\"\n"
        );
        let policy = FormationPolicy::parse_toml(doc.as_bytes()).expect("parses");
        let commands = policy
            .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
            .expect("valid policy");
        let Command::UpdateAuthorization(update) = commands
            .iter()
            .find(|c| matches!(c, Command::UpdateAuthorization(_)))
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(update.bindings[1].scope, Some(explicit_id));
    }

    #[test]
    fn authorization_is_left_alone_once_any_binding_exists() {
        let policy = FormationPolicy::parse_toml(AUTHZ_SAMPLE.as_bytes()).unwrap();
        let now = Timestamp::now();
        let mut state = StateMachine::default();
        state.quota_entities.insert(
            QuotaEntityId::new(),
            coppice_state::QuotaEntity {
                parent: None,
                name: "default".to_string(),
                quota: CostUnits(1_000_000_000_000),
                usage: coppice_core::quota::UsageState::new(now),
                created_at: now,
                updated_at: now,
            },
        );
        // An operator has since replaced the list with something else
        // entirely; a re-init must not put the document's bindings back.
        state.bindings = vec![Binding {
            subject: Subject::Group("ops".to_string()),
            role: Role::Admin,
            scope: None,
        }];
        assert!(policy
            .commands(&state, now, CHEAP_KDF)
            .expect("valid policy")
            .is_empty());
    }

    #[test]
    fn authorization_without_an_unscoped_admin_is_refused_at_the_edge() {
        let err = FormationPolicy::parse_toml(
            br#"
[authorization]
[[authorization.binding]]
group = "submitters"
role = "submitter"
"# as &[u8],
        )
        .expect_err("a lockout document is refused");
        assert!(
            format!("{err:#}").contains("no unscoped admin binding"),
            "{err:#}"
        );
    }

    #[test]
    fn a_present_but_empty_authorization_table_is_refused() {
        // Absence means "leave authorization alone"; presence must bind an
        // admin, or the table has silently installed nothing.
        for doc in [
            "[authorization]\n",
            "[authorization]\ngroups_claim = \"cognito:groups\"\n",
            "[authorization]\nbinding = []\n",
        ] {
            let err = FormationPolicy::parse_toml(doc.as_bytes()).expect_err(doc);
            assert!(
                format!("{err:#}").contains("no unscoped admin binding"),
                "{doc}: {err:#}"
            );
        }
    }

    #[test]
    fn authorization_binding_needs_exactly_one_subject() {
        for (doc, needle) in [
            (
                "[authorization]\n[[authorization.binding]]\nrole = \"admin\"\n",
                "neither `group` nor `principal`",
            ),
            (
                "[authorization]\n[[authorization.binding]]\ngroup = \"a\"\nprincipal = \"b\"\nrole = \"admin\"\n",
                "both `group` and `principal`",
            ),
            (
                "[authorization]\n[[authorization.binding]]\ngroup = \" \"\nrole = \"admin\"\n",
                "empty subject",
            ),
            (
                "[authorization]\ngroups_claim = \" \"\n[[authorization.binding]]\ngroup = \"a\"\nrole = \"admin\"\n",
                "groups_claim is empty",
            ),
        ] {
            let err = FormationPolicy::parse_toml(doc.as_bytes()).expect_err(doc);
            assert!(format!("{err:#}").contains(needle), "{doc}: {err:#}");
        }
    }

    #[test]
    fn authorization_scope_must_be_seeded_or_existing() {
        let policy = FormationPolicy::parse_toml(
            br#"
[authorization]
[[authorization.binding]]
group = "admins"
role = "admin"
[[authorization.binding]]
group = "team"
role = "submitter"
scope = "quota-00000000-0000-0000-0000-0000000000ee"
"# as &[u8],
        )
        .unwrap();
        let err = policy
            .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
            .expect_err("unknown scope is refused");
        assert!(
            format!("{err:#}").contains("neither seeded by this document nor an existing"),
            "{err:#}"
        );
    }

    #[test]
    fn authorization_scope_by_unknown_path_is_refused() {
        let policy = FormationPolicy::parse_toml(
            br#"
[authorization]
[[authorization.binding]]
group = "admins"
role = "admin"
[[authorization.binding]]
group = "team"
role = "submitter"
scope = "acme/eng"
"# as &[u8],
        )
        .unwrap();
        let err = policy
            .commands(&StateMachine::default(), Timestamp::now(), CHEAP_KDF)
            .expect_err("a path naming nothing is refused");
        let message = format!("{err:#}");
        assert!(
            message.contains("neither seeded by this document nor an existing"),
            "{message}"
        );
        assert!(message.contains("acme/eng"), "{message}");
    }
}
