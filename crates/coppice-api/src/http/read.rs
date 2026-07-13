//! Read-consistency plumbing (ADR 0007 / ADR 0031).
//!
//! Every read endpoint accepts the same two query parameters; handlers
//! extract them with `axum::extract::Query<ReadParams>` and resolve the
//! effective class against their endpoint default. The mechanics:
//! `strong` pairs `Consensus::read_index()` with `StateViews::at_least`;
//! `bounded` serves `StateViews::latest()` with staleness surfaced in the
//! response headers; `eventual` reads whatever derived store backs the
//! endpoint.

use serde::Deserialize;

/// The caller-selectable consistency class (`?consistency=`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    Strong,
    Bounded,
    Eventual,
}

/// Query parameters common to every read endpoint.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct ReadParams {
    /// Override of the endpoint's default class.
    pub consistency: Option<Consistency>,
    /// Serve from a view with `applied_index >= min_index` — the
    /// read-your-writes pair for a write response's `logIndex`.
    pub min_index: Option<u64>,
}

impl ReadParams {
    /// The class this request reads at, given the endpoint's default.
    pub fn class(self, default: Consistency) -> Consistency {
        self.consistency.unwrap_or(default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(flatten)]
        params: ReadParams,
    }

    #[test]
    fn parses_lowercase_consistency_and_min_index() {
        let w: Wrapper =
            serde_json::from_str(r#"{ "consistency": "strong", "min_index": 42 }"#).unwrap();
        assert_eq!(w.params.consistency, Some(Consistency::Strong));
        assert_eq!(w.params.min_index, Some(42));
        assert_eq!(w.params.class(Consistency::Bounded), Consistency::Strong);
    }

    #[test]
    fn absent_params_fall_back_to_the_endpoint_default() {
        let w: Wrapper = serde_json::from_str("{}").unwrap();
        assert_eq!(w.params.class(Consistency::Bounded), Consistency::Bounded);
    }
}
