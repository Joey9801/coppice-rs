//! Quota-entity names, paths, and references (ADR 0045).
//!
//! A quota entity's `name` is one path **segment**: 1–63 characters from
//! `[A-Za-z0-9._-]`, the first alphanumeric, and never itself a
//! [`QuotaEntityId`] (`quota-<uuid>`). Siblings are unique by name, so the
//! slash-joined names from a root down to an entity — its **path**, e.g.
//! `acme/eng/platform` — name exactly one entity. The path is derived at read
//! time from the parent chain and never stored; replicated state stays
//! id-keyed.
//!
//! A [`QuotaEntityRef`] is how a client *names* an entity on the wire: one
//! string that is either an id or a path. A string that parses as an id is an
//! id; anything else must parse as a path. The segment rule makes that
//! unambiguous — no path segment can ever look like an id.

use std::fmt;
use std::str::FromStr;

use crate::id::QuotaEntityId;

/// Maximum length of one path segment (a quota entity's `name`), in bytes —
/// which, under the ASCII-only charset, is also characters.
pub const MAX_SEGMENT_LEN: usize = 63;

/// The separator between the segments of a [`QuotaEntityPath`].
pub const PATH_SEPARATOR: char = '/';

/// A string failed the quota-entity segment grammar (ADR 0045).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid quota entity name {input:?}: {reason}")]
pub struct InvalidSegment {
    /// The offending input, truncated to 80 characters for display safety.
    pub input: String,
    /// What rule it broke.
    pub reason: &'static str,
}

/// Check one segment against the grammar: 1–63 characters from
/// `[A-Za-z0-9._-]`, the first alphanumeric, and not a `quota-<uuid>` id.
pub fn validate_segment(segment: &str) -> Result<(), InvalidSegment> {
    let fail = |reason| {
        Err(InvalidSegment {
            input: segment.chars().take(80).collect(),
            reason,
        })
    };
    let Some(first) = segment.chars().next() else {
        return fail("a name must not be empty");
    };
    if segment.len() > MAX_SEGMENT_LEN {
        return fail("a name is at most 63 characters");
    }
    if !first.is_ascii_alphanumeric() {
        return fail("a name must start with a letter or digit");
    }
    if !segment
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return fail("a name may contain only letters, digits, `.`, `_`, and `-`");
    }
    if segment.parse::<QuotaEntityId>().is_ok() {
        return fail("a name must not itself be a quota entity id");
    }
    Ok(())
}

/// A string failed to parse as a [`QuotaEntityPath`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParsePathError {
    #[error("a quota entity path must not be empty")]
    Empty,
    #[error("invalid quota entity path {input:?}: {segment}")]
    Segment {
        /// The whole offending path, truncated to 200 characters.
        input: String,
        /// The first segment that broke the grammar.
        segment: InvalidSegment,
    },
}

/// A syntactically valid quota-entity path: one or more
/// [segments](validate_segment) joined by `/`, root first, with no leading or
/// trailing slash (`acme/eng/platform`).
///
/// Only the *syntax* is checked: whether an entity lives at the path is a
/// question for a state view, answered at read time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuotaEntityPath(String);

impl QuotaEntityPath {
    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The segments, root first. Never empty.
    pub fn segments(&self) -> impl DoubleEndedIterator<Item = &str> + Clone {
        self.0.split(PATH_SEPARATOR)
    }

    /// The number of segments (the entity's depth, a root being 1).
    pub fn depth(&self) -> usize {
        self.segments().count()
    }

    /// The last segment — the named entity's own `name`.
    pub fn leaf(&self) -> &str {
        self.0
            .rsplit_once(PATH_SEPARATOR)
            .map_or(self.0.as_str(), |(_, leaf)| leaf)
    }

    /// The path of the parent, or `None` for a single-segment (root) path.
    pub fn parent(&self) -> Option<QuotaEntityPath> {
        self.0
            .rsplit_once(PATH_SEPARATOR)
            .map(|(parent, _)| QuotaEntityPath(parent.to_string()))
    }

    /// This path extended by one validated segment.
    pub fn child(&self, segment: &str) -> Result<QuotaEntityPath, InvalidSegment> {
        validate_segment(segment)?;
        Ok(QuotaEntityPath(format!(
            "{}{PATH_SEPARATOR}{segment}",
            self.0
        )))
    }

    /// Build a path from segments, root first, validating each one before
    /// joining — so an empty segment (or one containing the separator) is
    /// refused rather than vanishing into the join.
    pub fn from_segments<'a>(
        segments: impl IntoIterator<Item = &'a str>,
    ) -> Result<QuotaEntityPath, ParsePathError> {
        let segments: Vec<&str> = segments.into_iter().collect();
        if segments.is_empty() {
            return Err(ParsePathError::Empty);
        }
        let joined = segments.join(PATH_SEPARATOR.encode_utf8(&mut [0; 4]));
        for segment in &segments {
            validate_segment(segment).map_err(|segment| ParsePathError::Segment {
                input: joined.chars().take(200).collect(),
                segment,
            })?;
        }
        Ok(QuotaEntityPath(joined))
    }
}

impl fmt::Display for QuotaEntityPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for QuotaEntityPath {
    type Err = ParsePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParsePathError::Empty);
        }
        for segment in s.split(PATH_SEPARATOR) {
            validate_segment(segment).map_err(|segment| ParsePathError::Segment {
                input: s.chars().take(200).collect(),
                segment,
            })?;
        }
        Ok(QuotaEntityPath(s.to_string()))
    }
}

impl serde::Serialize for QuotaEntityPath {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for QuotaEntityPath {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// How a client names a quota entity: its id or its path (ADR 0045).
///
/// On the wire a plain string. Parsing tries the id first — a string that
/// parses as `quota-<uuid>` is an id — and parses anything else as a path,
/// every segment of which must meet the grammar. `Display` round-trips.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuotaEntityRef {
    Id(QuotaEntityId),
    Path(QuotaEntityPath),
}

impl QuotaEntityRef {
    /// The id, when this reference is one.
    pub fn as_id(&self) -> Option<QuotaEntityId> {
        match self {
            QuotaEntityRef::Id(id) => Some(*id),
            QuotaEntityRef::Path(_) => None,
        }
    }

    /// The path, when this reference is one.
    pub fn as_path(&self) -> Option<&QuotaEntityPath> {
        match self {
            QuotaEntityRef::Id(_) => None,
            QuotaEntityRef::Path(p) => Some(p),
        }
    }
}

impl From<QuotaEntityId> for QuotaEntityRef {
    fn from(id: QuotaEntityId) -> Self {
        QuotaEntityRef::Id(id)
    }
}

impl From<QuotaEntityPath> for QuotaEntityRef {
    fn from(path: QuotaEntityPath) -> Self {
        QuotaEntityRef::Path(path)
    }
}

impl fmt::Display for QuotaEntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuotaEntityRef::Id(id) => id.fmt(f),
            QuotaEntityRef::Path(path) => path.fmt(f),
        }
    }
}

impl FromStr for QuotaEntityRef {
    type Err = ParsePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(id) = s.parse::<QuotaEntityId>() {
            return Ok(QuotaEntityRef::Id(id));
        }
        s.parse().map(QuotaEntityRef::Path)
    }
}

impl serde::Serialize for QuotaEntityRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for QuotaEntityRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_grammar_accepts_the_documented_charset() {
        for ok in [
            "a",
            "acme",
            "Team-A",
            "eng.platform",
            "v1_2",
            "0day",
            "quota",
            "quota-team",
            &"x".repeat(MAX_SEGMENT_LEN),
        ] {
            validate_segment(ok).unwrap_or_else(|e| panic!("{ok:?}: {e}"));
        }
    }

    #[test]
    fn segment_grammar_rejects_everything_else() {
        let id = QuotaEntityId::new().to_string();
        for bad in [
            "",
            "-lead",
            ".hidden",
            "_x",
            "a/b",
            "a b",
            "ünï",
            "a:b",
            &"x".repeat(MAX_SEGMENT_LEN + 1),
            &id,
        ] {
            assert!(validate_segment(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn paths_parse_split_and_rebuild() {
        let p: QuotaEntityPath = "acme/eng/platform".parse().unwrap();
        assert_eq!(
            p.segments().collect::<Vec<_>>(),
            ["acme", "eng", "platform"]
        );
        assert_eq!(p.depth(), 3);
        assert_eq!(p.leaf(), "platform");
        assert_eq!(p.parent().unwrap().as_str(), "acme/eng");
        assert_eq!(p.parent().unwrap().parent().unwrap().as_str(), "acme");
        assert!("acme"
            .parse::<QuotaEntityPath>()
            .unwrap()
            .parent()
            .is_none());
        assert_eq!(p.parent().unwrap().child("platform").unwrap(), p);
        assert_eq!(
            QuotaEntityPath::from_segments(["acme", "eng", "platform"]).unwrap(),
            p
        );
    }

    #[test]
    fn from_segments_refuses_empty_and_separator_bearing_segments() {
        for bad in [
            &["", "acme"][..],
            &["acme", ""],
            &["acme", "", "eng"],
            &["acme/eng"],
            &[],
        ] {
            assert!(
                QuotaEntityPath::from_segments(bad.iter().copied()).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn malformed_paths_are_refused() {
        for bad in ["", "/acme", "acme/", "acme//eng", "acme/-eng", "acme/e ng"] {
            assert!(bad.parse::<QuotaEntityPath>().is_err(), "{bad:?}");
        }
        let id = QuotaEntityId::new();
        assert!(format!("acme/{id}").parse::<QuotaEntityPath>().is_err());
    }

    #[test]
    fn a_ref_is_an_id_when_it_parses_as_one_and_a_path_otherwise() {
        let id = QuotaEntityId::new();
        assert_eq!(
            id.to_string().parse::<QuotaEntityRef>().unwrap(),
            QuotaEntityRef::Id(id)
        );
        // Id-lookalikes that are not ids are paths — when they fit the grammar.
        let r: QuotaEntityRef = "quota-team".parse().unwrap();
        assert_eq!(r.as_path().unwrap().as_str(), "quota-team");
        assert!("acme/eng"
            .parse::<QuotaEntityRef>()
            .unwrap()
            .as_id()
            .is_none());
        // A string that is neither is refused.
        assert!("acme//eng".parse::<QuotaEntityRef>().is_err());
        assert!("".parse::<QuotaEntityRef>().is_err());
        // Nor can an id hide inside a path.
        assert!(format!("acme/{id}").parse::<QuotaEntityRef>().is_err());
    }

    #[test]
    fn refs_serialize_as_plain_strings_and_display_round_trips() {
        let id = QuotaEntityId::new();
        for r in [
            QuotaEntityRef::Id(id),
            QuotaEntityRef::Path("acme/eng".parse().unwrap()),
        ] {
            let json = serde_json::to_string(&r).unwrap();
            assert_eq!(json, format!("\"{r}\""));
            assert_eq!(serde_json::from_str::<QuotaEntityRef>(&json).unwrap(), r);
            assert_eq!(r.to_string().parse::<QuotaEntityRef>().unwrap(), r);
        }
        assert!(serde_json::from_str::<QuotaEntityRef>("\"a//b\"").is_err());
    }
}
