//! Read-view memoization: typed caches that live and die with one published
//! state view.
//!
//! Read-model projections that would otherwise rescan the whole state per
//! request (job-scaled maps can hold millions of entries) can be computed
//! once per published view and shared by every read served from it. The
//! table hangs off the view itself — a fresh [`StateMachine`] clone is
//! published per view (KOI-5), so a memo written here can never outlive the
//! state it was computed from, and a newer view simply starts with an empty
//! table. This is read-path plumbing, never replicated state.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// A typed memoization table for one published state view.
///
/// Values are keyed by their type: one [`ViewMemos`] serves at most one
/// cached value per type, so the type should identify the projection (wrap
/// in a dedicated newtype if two distinct caches could ever share a shape).
/// Empty tables are cheap; entries die with the view that owns the table.
#[derive(Debug, Default)]
pub struct ViewMemos {
    slots: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl ViewMemos {
    /// The memoized `T` for this view, computing it with `f` on the first
    /// request and returning the shared value thereafter.
    ///
    /// The computation runs under the table lock: concurrent first requests
    /// for the same view share one computation instead of racing to scan the
    /// state several times. Keep `f` a pure function of the view's state —
    /// the cached result will be served to every later read of this view.
    pub fn memo<T: Send + Sync + 'static>(&self, f: impl FnOnce() -> T) -> Arc<T> {
        let key = TypeId::of::<T>();
        let mut slots = self.lock();
        if let Some(cached) = slots.get(&key) {
            return cached
                .clone()
                .downcast::<T>()
                .expect("a memo slot's value always matches its type key");
        }
        let value = Arc::new(f());
        slots.insert(key, value.clone());
        value
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<TypeId, Arc<dyn Any + Send + Sync>>> {
        self.slots
            .lock()
            .expect("view memo table is never poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memoizes_one_value_per_type() {
        let memos = ViewMemos::default();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let calls1 = Arc::clone(&calls);
        let a = memos.memo(move || {
            calls1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            vec![1u64, 2, 3]
        });
        let calls2 = Arc::clone(&calls);
        let b = memos.memo(move || {
            calls2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            vec![9u64]
        });

        // Same type: the first computation is shared, the closure runs once.
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*b, vec![1, 2, 3]);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // A different type gets its own slot.
        let c = memos.memo(|| 7u32);
        assert_eq!(*c, 7);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn tables_are_independent_per_view() {
        let memos = ViewMemos::default();
        let first = memos.memo(|| 1u8);
        // A fresh view's table starts empty: same type, new computation.
        let second = ViewMemos::default().memo(|| 2u8);
        assert_eq!(*first, 1);
        assert_eq!(*second, 2);
    }
}
