//! The breaking-change gate: a mechanical descriptor-set diff against the
//! committed baseline, so tag breakage is caught by CI, never by review
//! (`docs/architecture/schema-style.md`).
//!
//! `build.rs` writes the current corpus's `FileDescriptorSet` to `OUT_DIR`;
//! this test compares it, message by message and field by field, against
//! `proto/baseline.binpb`. It fails on anything the evolution rules forbid:
//!
//! - a message or enum that existed in the baseline is gone or renamed;
//! - a field number changed its name, type, label (repeated ↔ singular),
//!   presence (`optional`), or containing oneof;
//! - a field number or enum value was removed without reserving both the
//!   number and the name;
//! - a baseline reservation no longer holds, or a reserved number/name came
//!   back to life as a field.
//!
//! Additions are always allowed. After an intentional, rules-compliant
//! change, refresh the baseline (and commit it in the same change):
//!
//! ```text
//! UPDATE_PROTO_BASELINE=1 cargo test -p coppice-proto --test breaking
//! ```
//!
//! `buf breaking` (via `scripts/proto-check.sh`) is an optional extra
//! check; this diff is the gate of record.

use std::collections::BTreeMap;
use std::fs;

use prost::Message;
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorSet};

const CURRENT_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.binpb"));
const BASELINE_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto/baseline.binpb");

#[test]
fn schema_is_backward_compatible_with_the_committed_baseline() {
    if std::env::var_os("UPDATE_PROTO_BASELINE").is_some() {
        fs::write(BASELINE_PATH, CURRENT_BYTES).expect("baseline must be writable");
        println!("baseline refreshed: {BASELINE_PATH}");
        return;
    }

    let baseline_bytes = fs::read(BASELINE_PATH).unwrap_or_else(|e| {
        panic!(
            "missing committed baseline {BASELINE_PATH} ({e}); generate it with \
             UPDATE_PROTO_BASELINE=1 cargo test -p coppice-proto --test breaking"
        )
    });
    let baseline =
        FileDescriptorSet::decode(baseline_bytes.as_slice()).expect("baseline must decode");
    let current = FileDescriptorSet::decode(CURRENT_BYTES).expect("current set must decode");

    let baseline = index(&baseline);
    let current = index(&current);
    let mut problems = Vec::new();

    for (name, base_msg) in &baseline.messages {
        match current.messages.get(name) {
            None => problems.push(format!("message {name} was removed or renamed")),
            Some(cur_msg) => compare_message(name, base_msg, cur_msg, &mut problems),
        }
    }
    for (name, base_enum) in &baseline.enums {
        match current.enums.get(name) {
            None => problems.push(format!("enum {name} was removed or renamed")),
            Some(cur_enum) => compare_enum(name, base_enum, cur_enum, &mut problems),
        }
    }

    assert!(
        problems.is_empty(),
        "breaking schema change(s) against proto/baseline.binpb:\n  - {}\n\
         (if every change is intentional and rules-compliant per \
         docs/architecture/schema-style.md, refresh the baseline)",
        problems.join("\n  - ")
    );
}

// ---- Indexing ----

struct Index<'a> {
    messages: BTreeMap<String, &'a DescriptorProto>,
    enums: BTreeMap<String, &'a EnumDescriptorProto>,
}

fn index(set: &FileDescriptorSet) -> Index<'_> {
    let mut idx = Index {
        messages: BTreeMap::new(),
        enums: BTreeMap::new(),
    };
    for file in &set.file {
        let package = file.package();
        for e in &file.enum_type {
            idx.enums.insert(format!("{package}.{}", e.name()), e);
        }
        for m in &file.message_type {
            walk_message(package, m, &mut idx);
        }
    }
    idx
}

fn walk_message<'a>(prefix: &str, message: &'a DescriptorProto, idx: &mut Index<'a>) {
    let name = format!("{prefix}.{}", message.name());
    for e in &message.enum_type {
        idx.enums.insert(format!("{name}.{}", e.name()), e);
    }
    for nested in &message.nested_type {
        walk_message(&name, nested, idx);
    }
    idx.messages.insert(name, message);
}

// ---- Messages ----

fn compare_message(
    name: &str,
    base: &DescriptorProto,
    cur: &DescriptorProto,
    problems: &mut Vec<String>,
) {
    // Message reserved ranges use an *exclusive* end.
    let base_reserved: Vec<(i32, i32)> = base
        .reserved_range
        .iter()
        .map(|r| (r.start(), r.end() - 1))
        .collect();
    let cur_reserved: Vec<(i32, i32)> = cur
        .reserved_range
        .iter()
        .map(|r| (r.start(), r.end() - 1))
        .collect();

    for base_field in &base.field {
        let tag = base_field.number();
        match cur.field.iter().find(|f| f.number() == tag) {
            Some(cur_field) => {
                if base_field.name() != cur_field.name() {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) was renamed to {}",
                        base_field.name(),
                        cur_field.name()
                    ));
                }
                if base_field.r#type != cur_field.r#type
                    || base_field.type_name != cur_field.type_name
                {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) changed type",
                        base_field.name()
                    ));
                }
                if base_field.label != cur_field.label {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) changed label (repeated/singular)",
                        base_field.name()
                    ));
                }
                if base_field.proto3_optional.unwrap_or(false)
                    != cur_field.proto3_optional.unwrap_or(false)
                {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) changed `optional` presence",
                        base_field.name()
                    ));
                }
                if real_oneof(base, base_field) != real_oneof(cur, cur_field) {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) moved in or out of a oneof",
                        base_field.name()
                    ));
                }
            }
            None => {
                if !covers(&cur_reserved, tag) {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) was removed without reserving its number",
                        base_field.name()
                    ));
                }
                if !cur.reserved_name.iter().any(|n| n == base_field.name()) {
                    problems.push(format!(
                        "{name}.{} (tag {tag}) was removed without reserving its name",
                        base_field.name()
                    ));
                }
            }
        }
    }

    // Reservations are permanent: every baseline reservation must still
    // hold, and nothing may come back to life under a reserved number/name.
    for &(start, end) in &base_reserved {
        if !covers_range(&cur_reserved, start, end) {
            problems.push(format!("{name} dropped reserved range {start}..={end}"));
        }
    }
    for reserved_name in &base.reserved_name {
        if !cur.reserved_name.contains(reserved_name) {
            problems.push(format!("{name} dropped reserved name {reserved_name:?}"));
        }
    }
    for cur_field in &cur.field {
        if covers(&base_reserved, cur_field.number()) {
            problems.push(format!(
                "{name}.{} reuses reserved tag {}",
                cur_field.name(),
                cur_field.number()
            ));
        }
        if base.reserved_name.iter().any(|n| n == cur_field.name()) {
            problems.push(format!(
                "{name}.{} reuses a reserved name",
                cur_field.name()
            ));
        }
    }
}

fn real_oneof(
    message: &DescriptorProto,
    field: &prost_types::FieldDescriptorProto,
) -> Option<String> {
    // proto3 `optional` is implemented as a synthetic single-field oneof;
    // only real oneof membership is a wire-visible property.
    if field.proto3_optional.unwrap_or(false) {
        return None;
    }
    let index = field.oneof_index?;
    message
        .oneof_decl
        .get(index as usize)
        .map(|o| o.name().to_string())
}

// ---- Enums ----

fn compare_enum(
    name: &str,
    base: &EnumDescriptorProto,
    cur: &EnumDescriptorProto,
    problems: &mut Vec<String>,
) {
    // Enum reserved ranges use an *inclusive* end.
    let base_reserved: Vec<(i32, i32)> = base
        .reserved_range
        .iter()
        .map(|r| (r.start(), r.end()))
        .collect();
    let cur_reserved: Vec<(i32, i32)> = cur
        .reserved_range
        .iter()
        .map(|r| (r.start(), r.end()))
        .collect();

    for base_value in &base.value {
        let number = base_value.number();
        match cur.value.iter().find(|v| v.number() == number) {
            Some(cur_value) => {
                if base_value.name() != cur_value.name() {
                    problems.push(format!(
                        "{name}.{} ({number}) was renamed to {}",
                        base_value.name(),
                        cur_value.name()
                    ));
                }
            }
            None => {
                if !covers(&cur_reserved, number) {
                    problems.push(format!(
                        "{name}.{} ({number}) was removed without reserving its number",
                        base_value.name()
                    ));
                }
                if !cur.reserved_name.iter().any(|n| n == base_value.name()) {
                    problems.push(format!(
                        "{name}.{} ({number}) was removed without reserving its name",
                        base_value.name()
                    ));
                }
            }
        }
    }

    for &(start, end) in &base_reserved {
        if !covers_range(&cur_reserved, start, end) {
            problems.push(format!("{name} dropped reserved range {start}..={end}"));
        }
    }
    for cur_value in &cur.value {
        if covers(&base_reserved, cur_value.number()) {
            problems.push(format!(
                "{name}.{} reuses reserved number {}",
                cur_value.name(),
                cur_value.number()
            ));
        }
    }
}

// ---- Range helpers (inclusive intervals) ----

fn covers(ranges: &[(i32, i32)], n: i32) -> bool {
    ranges.iter().any(|&(start, end)| start <= n && n <= end)
}

/// Whether `ranges` covers every number in `start..=end`, allowing the
/// cover to be split across several ranges.
fn covers_range(ranges: &[(i32, i32)], start: i32, end: i32) -> bool {
    let mut sorted: Vec<(i32, i32)> = ranges.to_vec();
    sorted.sort_unstable();
    let mut next_uncovered = start;
    for (s, e) in sorted {
        if s > next_uncovered {
            break;
        }
        if e >= next_uncovered {
            next_uncovered = e.saturating_add(1);
        }
        if next_uncovered > end {
            return true;
        }
    }
    next_uncovered > end
}
