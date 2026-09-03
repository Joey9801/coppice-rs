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
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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
    /// The lookup happens under the table lock, but `f` runs **outside** it:
    /// a slow projection (the accrual sweep is O(all allocations)) must not
    /// serialize unrelated memo types on the same view, nor hold the lock
    /// across a job-scaled scan on a tokio worker thread. The cost is that
    /// concurrent first requests for the same type may each compute once;
    /// the first insertion wins and the later value is discarded. Since a
    /// projection is a pure function of the view's state, whichever wins is
    /// the same answer.
    ///
    /// `f` must be a pure function of the view's state and must not request
    /// its **own** type's memo (the recursion would never terminate); a
    /// different type's memo is fine — the lock is not held during `f`.
    pub fn memo<T: Send + Sync + 'static>(&self, f: impl FnOnce() -> T) -> Arc<T> {
        let key = TypeId::of::<T>();
        if let Some(cached) = self.lock().get(&key) {
            return cached
                .clone()
                .downcast::<T>()
                .expect("a memo slot's value always matches its type key");
        }
        let value = Arc::new(f());
        let mut slots = self.lock();
        match slots.entry(key) {
            std::collections::hash_map::Entry::Occupied(raced) => raced
                .get()
                .clone()
                .downcast::<T>()
                .expect("a memo slot's value always matches its type key"),
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(Arc::clone(&value) as Arc<dyn Any + Send + Sync>);
                value
            }
        }
    }

    /// Locked access to the table. Poisoning is recovered rather than
    /// propagated: a panic inside one memo closure must not turn every later
    /// read of this view into a panic — the cached values are unaffected and
    /// recomputing a lost insert is correct anyway.
    fn lock(&self) -> MutexGuard<'_, HashMap<TypeId, Arc<dyn Any + Send + Sync>>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// One closure panicking must not take the table (and so every later
    /// read of the view) down with it: the lock is recovered, other types
    /// still memoize, and the failed type simply recomputes next time.
    #[test]
    fn a_panicking_memo_does_not_poison_the_table() {
        let memos = ViewMemos::default();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            memos.memo(|| panic!("a projection exploded"));
        }));

        let other = memos.memo(|| 42u32);
        assert_eq!(*other, 42, "the failed type recomputes cleanly");
        let different_type = memos.memo(|| 43u64);
        assert_eq!(*different_type, 43);
    }
}
