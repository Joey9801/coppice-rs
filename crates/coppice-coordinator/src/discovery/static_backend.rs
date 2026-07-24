//! The `static` discovery backend (ADR 0037 §2): a literal list of raft
//! addresses from `[discovery.static] addrs = [...]` — the successor to the old
//! top-level `peers` seed list.

use super::Discovery;

/// Returns a fixed, config-supplied candidate list. The simplest backend: no
/// I/O, so [`candidates`](Discovery::candidates) never fails or blocks.
pub(crate) struct StaticDiscovery {
    addrs: Vec<String>,
}

impl StaticDiscovery {
    pub(crate) fn new(addrs: Vec<String>) -> Self {
        StaticDiscovery { addrs }
    }
}

#[tonic::async_trait]
impl Discovery for StaticDiscovery {
    async fn candidates(&self) -> Vec<String> {
        self.addrs.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_the_configured_list_verbatim() {
        let backend = StaticDiscovery::new(vec!["coord-1:7071".into(), "coord-2:7071".into()]);
        assert_eq!(
            backend.candidates().await,
            vec!["coord-1:7071".to_string(), "coord-2:7071".to_string()]
        );
    }

    #[tokio::test]
    async fn empty_list_is_allowed() {
        let backend = StaticDiscovery::new(vec![]);
        assert!(backend.candidates().await.is_empty());
    }
}
