//! Bounded backoff for a service that asks for fewer requests (P7 §8, Q11).
//!
//! Throttling ([`FailureClass::Throttled`](crate::model::FailureClass::Throttled)) is not a
//! failure of the entry or of the session: the same request may succeed a moment later. A
//! listing therefore repeats a throttled request after a growing, jittered delay, and only once
//! it has slept its whole budget (about two minutes per listing) does it report the throttling —
//! as a failure of that one listing, never of the traversal. The delays are plain sleeps inside
//! the listing's future, so cancelling the listing (or dropping its stream) stops them at once.

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// How long and how hard to back off.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ThrottleBackoff {
    /// The ceiling of the first delay; each later one doubles it.
    pub(crate) first: Duration,
    /// No single delay is longer.
    pub(crate) max_delay: Duration,
    /// The most one operation (one listing) sleeps in total before it gives up.
    pub(crate) budget: Duration,
}

impl ThrottleBackoff {
    /// One listing backs off for about two minutes in total (Q11).
    pub(crate) const LISTING: Self = Self {
        first: Duration::from_millis(250),
        max_delay: Duration::from_secs(15),
        budget: Duration::from_mins(2),
    };

    /// A fresh budget for one operation.
    pub(crate) const fn start(self) -> ThrottleBudget {
        ThrottleBudget {
            policy: self,
            slept: Duration::ZERO,
            attempts: 0,
        }
    }
}

/// What one operation has left of its [`ThrottleBackoff`].
#[derive(Debug)]
pub(crate) struct ThrottleBudget {
    policy: ThrottleBackoff,
    slept: Duration,
    attempts: u32,
}

impl ThrottleBudget {
    /// The delay before the next attempt, or `None` once the budget is spent.
    ///
    /// Equal jitter: half of the (doubling, capped) ceiling for sure, the other half at random,
    /// so concurrent listings that were throttled together do not come back together, and no
    /// delay collapses to nothing. The last delay is cut to what is left of the budget.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        let remaining = self.policy.budget.saturating_sub(self.slept);
        if remaining.is_zero() {
            return None;
        }
        let doubling = 1_u32.checked_shl(self.attempts.min(16)).unwrap_or(u32::MAX);
        let ceiling = self
            .policy
            .first
            .saturating_mul(doubling)
            .min(self.policy.max_delay);
        let half = ceiling / 2;
        let delay = half
            .saturating_add(jitter(half, self.attempts))
            .max(Duration::from_millis(1))
            .min(remaining);
        self.slept = self.slept.saturating_add(delay);
        self.attempts = self.attempts.saturating_add(1);
        Some(delay)
    }

    /// The request went through: the next throttling starts again from the first delay, while
    /// what was already slept still counts against the budget.
    pub(crate) const fn succeeded(&mut self) {
        self.attempts = 0;
    }

    /// Delays taken so far since the last success.
    pub(crate) const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Total time slept so far.
    #[cfg(test)]
    pub(crate) const fn slept(&self) -> Duration {
        self.slept
    }
}

/// A uniformly spread duration in `0..=span`, from the process's randomly keyed hasher: no
/// cryptographic quality is needed to spread retries apart.
fn jitter(span: Duration, salt: u32) -> Duration {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u32(salt);
    let nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
    match nanos.checked_add(1) {
        Some(range) => Duration::from_nanos(hasher.finish() % range),
        None => span,
    }
}

/// Runs `attempt` until it succeeds, fails with something other than throttling, or `budget`
/// is spent; in the last case the last throttling failure is returned.
pub(crate) async fn retry_throttled<T, E, F, Fut>(
    budget: &mut ThrottleBudget,
    is_throttled: impl Fn(&E) -> bool,
    mut attempt: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    loop {
        match attempt().await {
            Err(failure) if is_throttled(&failure) => {
                let Some(delay) = budget.next_delay() else {
                    return Err(failure);
                };
                tracing::debug!(
                    attempt = budget.attempts(),
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "throttled by the service; backing off"
                );
                tokio::time::sleep(delay).await;
            }
            result => {
                if result.is_ok() {
                    budget.succeeded();
                }
                return result;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST: ThrottleBackoff = ThrottleBackoff {
        first: Duration::from_millis(100),
        max_delay: Duration::from_millis(400),
        budget: Duration::from_secs(1),
    };

    #[test]
    fn delays_grow_stay_capped_and_stop_at_the_budget() {
        let mut budget = TEST.start();
        let mut delays = Vec::new();
        while let Some(delay) = budget.next_delay() {
            delays.push(delay);
            assert!(delays.len() < 100, "the budget must run out");
        }
        assert_eq!(
            budget.slept(),
            TEST.budget,
            "the whole budget is slept, no more"
        );
        for (index, delay) in delays.iter().enumerate() {
            assert!(*delay <= TEST.max_delay, "{index}: {delay:?}");
            let last = index + 1 == delays.len();
            // Equal jitter: at least half of the capped ceiling, unless cut by the budget.
            let ceiling = TEST
                .first
                .saturating_mul(1 << index.min(16))
                .min(TEST.max_delay);
            assert!(last || *delay >= ceiling / 2, "{index}: {delay:?}");
        }
        assert_eq!(budget.next_delay(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_throttled_attempt_is_repeated_until_it_succeeds() {
        let mut budget = TEST.start();
        let mut calls = 0;
        let result: Result<u32, &str> = retry_throttled(
            &mut budget,
            |failure| *failure == "throttled",
            || {
                calls += 1;
                let answer = if calls < 3 { Err("throttled") } else { Ok(7) };
                async move { answer }
            },
        )
        .await;
        assert_eq!(result, Ok(7));
        // Two delays were slept; the success starts the doubling over, not the budget.
        assert_eq!(budget.attempts(), 0);
        assert!(budget.slept() >= TEST.first);
        let next = budget.next_delay().unwrap_or_default();
        assert!(next <= TEST.first, "{next:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn throttling_past_the_budget_and_other_failures_are_returned() {
        let mut budget = TEST.start();
        let result: Result<(), &str> = retry_throttled(
            &mut budget,
            |failure| *failure == "throttled",
            || async { Err("throttled") },
        )
        .await;
        assert_eq!(result, Err("throttled"));
        assert_eq!(budget.slept(), TEST.budget);
        let mut fresh = TEST.start();
        let result: Result<(), &str> = retry_throttled(
            &mut fresh,
            |failure| *failure == "throttled",
            || async { Err("denied") },
        )
        .await;
        assert_eq!(result, Err("denied"));
        assert_eq!(fresh.attempts(), 0, "only throttling is repeated");
    }
}
