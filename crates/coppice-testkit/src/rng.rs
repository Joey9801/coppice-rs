//! A tiny deterministic RNG (SplitMix64).
//!
//! Hand-rolled on purpose: crash-suite failures are reproduced from a logged
//! seed, so the seed→stream mapping must never change out from under us. A
//! `rand` upgrade can (and does) change `StdRng`'s stream; ten lines of
//! SplitMix64 cannot. Not a cryptographic generator — it only has to be
//! adversarially *varied*, not unpredictable.

/// Deterministic RNG. The same seed yields the same stream, forever.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng { state: seed }
    }

    /// Derive an independent child stream; used to give each simulated crash
    /// its own adversary without coupling it to how much randomness earlier
    /// crashes consumed.
    pub fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        // SplitMix64 (Steele, Lea, Flood 2014). Public-domain constants.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, bound)`. `bound` must be nonzero.
    pub fn below(&mut self, bound: u64) -> u64 {
        // Modulo bias is irrelevant at test-adversary quality.
        self.next_u64() % bound
    }

    /// Uniform in `[lo, hi)`. `lo < hi` required.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo)
    }

    /// True with probability `num / den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    /// A uniformly chosen element of a non-empty slice.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn stream_is_stable_forever() {
        // These values are part of the reproducibility contract: if this test
        // fails, logged seeds from old failures no longer reproduce. Do not
        // update the expectations; fix the regression.
        let mut rng = Rng::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        let mut rng = Rng::new(42);
        assert_eq!(rng.next_u64(), 0xBDD7_3226_2FEB_6E95);
    }

    #[test]
    fn helpers_stay_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0..1000 {
            let v = rng.range(10, 20);
            assert!((10..20).contains(&v));
        }
        let items = [1, 2, 3];
        for _ in 0..100 {
            assert!(items.contains(rng.pick(&items)));
        }
    }
}
