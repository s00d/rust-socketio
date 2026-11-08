//! socket.io-client's reconnection backoff (`contrib/backo2.js`), ported exactly.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{Duration, SystemTime};

/// Exponential backoff: `min * 2^attempts`, randomised by ±`jitter`, capped at `max`.
///
/// Unlike the `backoff` crate it replaces, it never stops by elapsed time: the attempt limit is
/// the caller's (`max_reconnect_attempts`, socket.io-client `reconnectionAttempts`).
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    min_ms: u64,
    max_ms: u64,
    jitter: f64,
    attempts: u32,
}

/// socket.io-client `randomizationFactor` default.
pub(crate) const DEFAULT_JITTER: f64 = 0.5;

impl Backoff {
    pub(crate) fn new(min_ms: u64, max_ms: u64, jitter: f64) -> Self {
        Backoff {
            min_ms,
            max_ms,
            // backo2 ignores a jitter outside (0, 1]
            jitter: if jitter > 0.0 && jitter <= 1.0 {
                jitter
            } else {
                0.0
            },
            attempts: 0,
        }
    }

    /// Attempts made since the last reset (socket.io-client `backoff.attempts`).
    pub(crate) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// The delay before the next attempt.
    pub(crate) fn duration(&mut self) -> Duration {
        self.duration_with(random_unit())
    }

    /// `duration()` with the random number given: backo2 uses one `Math.random()` both for the
    /// deviation and for its sign.
    pub(crate) fn duration_with(&mut self, rand: f64) -> Duration {
        let mut ms = self.min_ms as f64 * 2f64.powi(self.attempts.min(62) as i32);
        self.attempts = self.attempts.saturating_add(1);
        if self.jitter > 0.0 {
            let deviation = (rand * self.jitter * ms).floor();
            ms = if ((rand * 10.0).floor() as u64) & 1 == 0 {
                ms - deviation
            } else {
                ms + deviation
            };
        }
        Duration::from_millis(ms.min(self.max_ms as f64).max(0.0) as u64)
    }

    #[cfg(test)]
    pub(crate) fn reset(&mut self) {
        self.attempts = 0;
    }
}

/// A random number in `[0, 1)` from the std hasher's per-process random keys.
fn random_unit() -> f64 {
    let bits = RandomState::new().hash_one(SystemTime::now());
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(d: Duration) -> u64 {
        d.as_millis() as u64
    }

    #[test]
    fn without_jitter_doubles_up_to_max() {
        let mut b = Backoff::new(1000, 5000, 0.0);
        let got: Vec<u64> = (0..5).map(|_| ms(b.duration())).collect();
        assert_eq!(got, [1000, 2000, 4000, 5000, 5000]);
        assert_eq!(b.attempts(), 5);
        b.reset();
        assert_eq!(ms(b.duration()), 1000);
    }

    #[test]
    fn jitter_matches_backo2() {
        // Values computed with socket.io-client's Backoff and Math.random stubbed.
        // rand 0.25: floor(2.5)=2 even -> minus; deviation floor(0.25*0.5*1000)=125
        assert_eq!(ms(Backoff::new(1000, 5000, 0.5).duration_with(0.25)), 875);
        // rand 0.35: floor(3.5)=3 odd -> plus; deviation 175
        assert_eq!(ms(Backoff::new(1000, 5000, 0.5).duration_with(0.35)), 1175);
        // capped after the deviation
        let mut b = Backoff::new(1000, 5000, 0.5);
        b.attempts = 3;
        assert_eq!(ms(b.duration_with(0.99)), 5000);
    }

    #[test]
    fn invalid_jitter_is_ignored() {
        assert_eq!(ms(Backoff::new(100, 1000, 1.5).duration_with(0.35)), 100);
    }

    #[test]
    fn random_stays_in_range() {
        for _ in 0..1000 {
            let r = random_unit();
            assert!((0.0..1.0).contains(&r));
        }
        let mut b = Backoff::new(1000, 5000, 0.5);
        for _ in 0..1000 {
            let d = ms(b.duration());
            assert!((500..=5000).contains(&d), "{d}");
            b.reset();
        }
    }
}
