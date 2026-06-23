//! A tiny, explicit, fully deterministic PRNG (SplitMix64).
//!
//! We implement it by hand rather than pulling a crate because *being in control
//! of every random bit* is the entire point of deterministic simulation: one
//! seed must reproduce one universe, byte for byte, on any machine.

/// SplitMix64 — a fast, well-distributed 64-bit generator (Steele et al.).
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next 64-bit value. Deterministic given the seed.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `[lo, hi)`. Returns `lo` if the range is empty.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo)
    }

    /// `true` with probability `num/den`. Uses integer math (no floats) so the
    /// result is identical across platforms.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        if num == 0 {
            return false;
        }
        if num >= den {
            return true;
        }
        self.next_u64() % den < num
    }

    /// Index uniformly into `0..len`.
    pub fn index(&mut self, len: usize) -> usize {
        if len == 0 {
            return 0;
        }
        (self.next_u64() % len as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(0xDEAD_BEEF);
        let mut b = Rng::new(0xDEAD_BEEF);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let mut differences = 0;
        for _ in 0..100 {
            if a.next_u64() != b.next_u64() {
                differences += 1;
            }
        }
        assert!(
            differences > 90,
            "streams should be essentially independent"
        );
    }

    #[test]
    fn range_is_bounded() {
        let mut r = Rng::new(42);
        for _ in 0..10_000 {
            let v = r.range(10, 20);
            assert!((10..20).contains(&v));
        }
    }

    #[test]
    fn chance_is_roughly_calibrated() {
        let mut r = Rng::new(7);
        let mut hits = 0;
        for _ in 0..100_000 {
            if r.chance(1, 4) {
                hits += 1;
            }
        }
        // ~25% with generous slack — this is a sanity bound, not a stats test.
        assert!((20_000..30_000).contains(&hits), "got {hits}");
    }
}
