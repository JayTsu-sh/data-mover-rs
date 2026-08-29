use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use num_traits::ToPrimitive as _;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const HARD_SHAPING_QUANTUM: Duration = Duration::from_millis(10);

/// Immutable source-read shaping policy shared by a transfer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceQosPolicy {
    soft_bytes_per_second: Option<u64>,
    hard_bytes_per_second: Option<u64>,
    bandwidth_peak_duration: Duration,
    max_read_bytes: u64,
    soft_iops: Option<u32>,
    hard_iops: Option<u32>,
    iops_peak_duration: Duration,
}

impl SourceQosPolicy {
    /// Creates one immutable source-only policy.
    ///
    /// # Errors
    /// Rejects zero, incomplete, or inverted soft/hard limits.
    pub fn new(
        bandwidth: Option<(u64, u64, Duration)>,
        max_read_bytes: u64,
        iops: Option<(u32, u32, Duration)>,
    ) -> Result<Self, SourceQosValueError> {
        if max_read_bytes == 0 {
            return Err(SourceQosValueError("maximum read size must be non-zero"));
        }
        let (soft_bytes_per_second, hard_bytes_per_second, bandwidth_peak_duration) = bandwidth
            .map_or((None, None, Duration::ZERO), |(soft, hard, peak)| {
                (Some(soft), Some(hard), peak)
            });
        if matches!(
            (soft_bytes_per_second, hard_bytes_per_second),
            (Some(0), _) | (_, Some(0))
        ) || soft_bytes_per_second
            .zip(hard_bytes_per_second)
            .is_some_and(|(soft, hard)| hard < soft)
        {
            return Err(SourceQosValueError(
                "bandwidth limits must be positive and ordered",
            ));
        }
        let (soft_iops, hard_iops, iops_peak_duration) = iops
            .map_or((None, None, Duration::ZERO), |(soft, hard, peak)| {
                (Some(soft), Some(hard), peak)
            });
        if matches!((soft_iops, hard_iops), (Some(0), _) | (_, Some(0)))
            || soft_iops
                .zip(hard_iops)
                .is_some_and(|(soft, hard)| hard < soft)
        {
            return Err(SourceQosValueError(
                "IOPS limits must be positive and ordered",
            ));
        }
        Ok(Self {
            soft_bytes_per_second,
            hard_bytes_per_second,
            bandwidth_peak_duration,
            max_read_bytes,
            soft_iops,
            hard_iops,
            iops_peak_duration,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceQosValueError(&'static str);

impl fmt::Display for SourceQosValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for SourceQosValueError {}

/// One shared, fair source-read budget. Clones join the same transfer group.
#[derive(Clone, Debug)]
pub struct SourceQosGroup(Arc<GroupInner>);

#[derive(Debug)]
struct GroupInner {
    policy: SourceQosPolicy,
    state: Mutex<SchedulerState>,
}

#[derive(Debug)]
struct SchedulerState {
    bandwidth_credit: f64,
    bandwidth_credit_at: tokio::time::Instant,
    next_hard_bandwidth: tokio::time::Instant,
    iops_credit: f64,
    iops_credit_at: tokio::time::Instant,
    next_hard_iops: tokio::time::Instant,
}

impl SourceQosGroup {
    #[must_use]
    pub fn new(policy: SourceQosPolicy) -> Self {
        let now = tokio::time::Instant::now();
        Self(Arc::new(GroupInner {
            policy,
            state: Mutex::new(SchedulerState {
                bandwidth_credit: 0.0,
                bandwidth_credit_at: now,
                next_hard_bandwidth: now,
                iops_credit: 0.0,
                iops_credit_at: now,
                next_hard_iops: now,
            }),
        }))
    }

    #[must_use]
    pub(crate) fn transfer_budget(&self) -> SourceQosBudget {
        SourceQosBudget {
            group: self.clone(),
            stats: Arc::new(SourceQosCounters::default()),
        }
    }
}

#[derive(Debug, Default)]
struct SourceQosCounters {
    logical_bytes: AtomicU64,
    shaped_bytes: AtomicU64,
    source_read_operations: AtomicU64,
}

/// Per-transfer accounting handle backed by a shared group scheduler.
#[derive(Clone, Debug)]
pub struct SourceQosBudget {
    group: SourceQosGroup,
    stats: Arc<SourceQosCounters>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceQosStats {
    pub logical_bytes: u64,
    pub client_streamed_shaped_bytes: u64,
    pub native_bytes: u64,
    pub source_read_operations: u64,
    pub native_requests: u64,
    pub native_payload_shaped: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SourceQosCancelled;

impl SourceQosBudget {
    pub(crate) fn set_logical_bytes(&self, bytes: u64) {
        self.stats.logical_bytes.store(bytes, Ordering::Relaxed);
    }

    #[must_use]
    pub fn stats(&self) -> SourceQosStats {
        SourceQosStats {
            logical_bytes: self.stats.logical_bytes.load(Ordering::Relaxed),
            client_streamed_shaped_bytes: self.stats.shaped_bytes.load(Ordering::Relaxed),
            native_bytes: 0,
            source_read_operations: self.stats.source_read_operations.load(Ordering::Relaxed),
            native_requests: 0,
            native_payload_shaped: false,
        }
    }

    pub(crate) async fn admit_read(
        &self,
        requested: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        self.admit(requested, cancel).await
    }

    async fn admit(
        &self,
        requested: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        let hard_quantum = self
            .group
            .0
            .policy
            .hard_bytes_per_second
            .map_or(u64::MAX, |hard| {
                u64::try_from(
                    u128::from(hard)
                        .saturating_mul(HARD_SHAPING_QUANTUM.as_nanos())
                        .div_ceil(1_000_000_000),
                )
                .unwrap_or(u64::MAX)
                .max(1)
            });
        let granted = requested
            .min(self.group.0.policy.max_read_bytes)
            .min(hard_quantum);
        if granted == 0 {
            return Ok(0);
        }
        let mut state = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(SourceQosCancelled),
            state = self.group.0.state.lock() => state,
        };
        let now = tokio::time::Instant::now();
        let bandwidth_deadline = bandwidth_deadline(&self.group.0.policy, &mut state, now, granted);
        let iops_deadline = iops_deadline(&self.group.0.policy, &mut state, now);
        let deadline = bandwidth_deadline.max(iops_deadline);
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(SourceQosCancelled),
            () = tokio::time::sleep_until(deadline) => {}
        }
        let released_at = tokio::time::Instant::now();
        commit_bandwidth(&self.group.0.policy, &mut state, released_at, granted);
        commit_iops(&self.group.0.policy, &mut state, released_at);
        drop(state);
        self.stats
            .source_read_operations
            .fetch_add(1, Ordering::Relaxed);
        Ok(granted)
    }

    pub(crate) fn record_read_bytes(&self, bytes: u64) {
        self.stats.shaped_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn bandwidth_deadline(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    now: tokio::time::Instant,
    bytes: u64,
) -> tokio::time::Instant {
    let (Some(soft), Some(hard)) = (policy.soft_bytes_per_second, policy.hard_bytes_per_second)
    else {
        return now;
    };
    let elapsed = now
        .saturating_duration_since(state.bandwidth_credit_at)
        .as_secs_f64();
    let soft = soft.to_f64().unwrap_or(f64::MAX);
    let hard = hard.to_f64().unwrap_or(f64::MAX);
    let bytes = bytes.to_f64().unwrap_or(f64::MAX);
    let capacity = (hard - soft) * policy.bandwidth_peak_duration.as_secs_f64();
    state.bandwidth_credit = (state.bandwidth_credit + elapsed * soft).min(capacity);
    let soft_wait = if state.bandwidth_credit >= bytes {
        Duration::ZERO
    } else {
        Duration::from_secs_f64((bytes - state.bandwidth_credit) / soft)
    };
    let hard_wait = Duration::from_secs_f64(bytes / hard);
    let nominal_hard = state.next_hard_bandwidth + hard_wait;
    let hard_deadline = if now <= nominal_hard {
        nominal_hard
    } else {
        now + hard_wait
    };
    (now + soft_wait).max(hard_deadline)
}

fn commit_bandwidth(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    deadline: tokio::time::Instant,
    bytes: u64,
) {
    if policy.soft_bytes_per_second.is_none() {
        return;
    }
    state.bandwidth_credit = (state.bandwidth_credit - bytes.to_f64().unwrap_or(f64::MAX)).max(0.0);
    state.bandwidth_credit_at = deadline;
    state.next_hard_bandwidth = deadline;
}

fn iops_deadline(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    now: tokio::time::Instant,
) -> tokio::time::Instant {
    let (Some(soft), Some(hard)) = (policy.soft_iops, policy.hard_iops) else {
        return now;
    };
    let elapsed = now
        .saturating_duration_since(state.iops_credit_at)
        .as_secs_f64();
    let capacity = f64::from(hard - soft) * policy.iops_peak_duration.as_secs_f64();
    state.iops_credit = (state.iops_credit + elapsed * f64::from(soft)).min(capacity);
    let soft_wait = if state.iops_credit >= 1.0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64((1.0 - state.iops_credit) / f64::from(soft))
    };
    let hard_wait = Duration::from_secs_f64(1.0 / f64::from(hard));
    let nominal_hard = state.next_hard_iops + hard_wait;
    let hard_deadline = if now <= nominal_hard {
        nominal_hard
    } else {
        now + hard_wait
    };
    (now + soft_wait).max(hard_deadline)
}

fn commit_iops(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    deadline: tokio::time::Instant,
) {
    if policy.soft_iops.is_none() {
        return;
    }
    state.iops_credit = (state.iops_credit - 1.0).max(0.0);
    state.iops_credit_at = deadline;
    state.next_hard_iops = deadline;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(max_read_bytes: u64) -> SourceQosPolicy {
        SourceQosPolicy::new(None, max_read_bytes, None).unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn policy_rejects_zero_and_inverted_limits() {
        assert!(SourceQosPolicy::new(None, 0, None).is_err());
        assert!(SourceQosPolicy::new(Some((2, 1, Duration::ZERO)), 1, None).is_err());
        assert!(SourceQosPolicy::new(None, 1, Some((2, 1, Duration::ZERO))).is_err());
    }

    #[tokio::test]
    async fn cancellation_while_waiting_is_immediate_and_uncharged() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(Some((1, 1, Duration::ZERO)), 1, None)
                .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        let waiting = {
            let budget = budget.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { budget.admit_read(1, &cancel).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        assert!(waiting.await.is_ok_and(|result| result.is_err()));
        assert_eq!(budget.stats(), SourceQosStats::default());
    }

    #[tokio::test]
    async fn cloned_group_shares_admission_but_keeps_per_transfer_counters() {
        let group = SourceQosGroup::new(policy(4));
        let first = group.transfer_budget();
        let second = group.clone().transfer_budget();
        let cancel = CancellationToken::new();
        assert_eq!(first.admit_read(9, &cancel).await, Ok(4));
        first.record_read_bytes(4);
        assert_eq!(second.admit_read(2, &cancel).await, Ok(2));
        second.record_read_bytes(2);
        assert_eq!(first.stats().client_streamed_shaped_bytes, 4);
        assert_eq!(first.stats().source_read_operations, 1);
        assert_eq!(second.stats().client_streamed_shaped_bytes, 2);
        assert_eq!(second.stats().source_read_operations, 1);
    }

    #[tokio::test]
    async fn repeated_source_attempts_charge_each_operation_but_only_returned_payload() {
        let group = SourceQosGroup::new(policy(4));
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        assert_eq!(budget.admit_read(4, &cancel).await, Ok(4));
        assert_eq!(budget.admit_read(4, &cancel).await, Ok(4));
        budget.record_read_bytes(4);
        assert_eq!(budget.stats().source_read_operations, 2);
        assert_eq!(budget.stats().client_streamed_shaped_bytes, 4);
    }

    #[tokio::test]
    async fn queued_transfers_share_one_fair_budget_without_starvation() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(None, 1, Some((20, 20, Duration::ZERO)))
                .unwrap_or_else(|error| panic!("{error}")),
        );
        let first = group.transfer_budget();
        let second = group.transfer_budget();
        let cancel = CancellationToken::new();
        let first_task = {
            let budget = first.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { budget.admit_read(1, &cancel).await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second_task = {
            let budget = second.clone();
            tokio::spawn(async move { budget.admit_read(1, &cancel).await })
        };
        let joined = tokio::time::timeout(Duration::from_millis(300), async {
            (first_task.await, second_task.await)
        })
        .await;
        assert!(matches!(joined, Ok((Ok(Ok(1)), Ok(Ok(1))))));
        assert_eq!(first.stats().source_read_operations, 1);
        assert_eq!(second.stats().source_read_operations, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn soft_average_hard_ceiling_and_peak_credit_are_bounded() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(Some((100, 200, Duration::from_millis(100))), 20, None)
                .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        let first_started = tokio::time::Instant::now();
        assert_eq!(budget.admit_read(20, &cancel).await, Ok(2));
        assert!(first_started.elapsed() >= Duration::from_millis(20));

        tokio::time::sleep(Duration::from_secs(1)).await;
        let credited_started = tokio::time::Instant::now();
        for _ in 0..5 {
            assert_eq!(budget.admit_read(20, &cancel).await, Ok(2));
        }
        assert!(credited_started.elapsed() >= Duration::from_millis(50));
        assert!(credited_started.elapsed() < Duration::from_millis(100));
    }
}
