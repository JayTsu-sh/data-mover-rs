use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use num_traits::ToPrimitive as _;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const HARD_SHAPING_QUANTUM: Duration = Duration::from_millis(10);

mod wait;

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
    read_grant_bytes: u64,
    immediate_interval: Option<Duration>,
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
        let read_grant_bytes = max_read_bytes.min(hard_bytes_per_second.map_or(u64::MAX, |hard| {
            u64::try_from(
                (u128::from(hard) * HARD_SHAPING_QUANTUM.as_nanos()).div_ceil(1_000_000_000),
            )
            .unwrap_or(u64::MAX)
            .max(1)
        }));
        let bandwidth_strict =
            soft_bytes_per_second == hard_bytes_per_second || bandwidth_peak_duration.is_zero();
        let iops_strict = soft_iops == hard_iops || iops_peak_duration.is_zero();
        let immediate_interval = (bandwidth_strict && iops_strict).then(|| {
            soft_bytes_per_second
                .map_or(Duration::ZERO, |rate| rate_interval(max_read_bytes, rate))
                .max(soft_iops.map_or(Duration::ZERO, |rate| rate_interval(1, u64::from(rate))))
        });
        Ok(Self {
            soft_bytes_per_second,
            hard_bytes_per_second,
            bandwidth_peak_duration,
            max_read_bytes,
            soft_iops,
            hard_iops,
            iops_peak_duration,
            read_grant_bytes,
            immediate_interval,
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
    last_release: tokio::time::Instant,
    bandwidth_credit: f64,
    bandwidth_credit_at: tokio::time::Instant,
    next_hard_bandwidth: tokio::time::Instant,
    iops_credit: f64,
    iops_credit_at: tokio::time::Instant,
    next_hard_iops: tokio::time::Instant,
    #[cfg(any(test, feature = "test-support"))]
    admission_trace: Option<Vec<(tokio::time::Instant, u64)>>,
}

impl SourceQosGroup {
    #[must_use]
    pub fn new(policy: SourceQosPolicy) -> Self {
        let now = tokio::time::Instant::now();
        Self(Arc::new(GroupInner {
            policy,
            state: Mutex::new(SchedulerState {
                last_release: now,
                bandwidth_credit: 0.0,
                bandwidth_credit_at: now,
                next_hard_bandwidth: now,
                iops_credit: 0.0,
                iops_credit_at: now,
                next_hard_iops: now,
                #[cfg(any(test, feature = "test-support"))]
                admission_trace: None,
            }),
        }))
    }

    #[must_use]
    pub fn transfer_budget(&self) -> SourceQosBudget {
        SourceQosBudget {
            group: self.clone(),
            stats: Arc::new(SourceQosCounters::default()),
        }
    }

    /// Enables an unbounded admission trace for finite diagnostic workloads only.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn enable_admission_trace(&self) {
        self.0.state.lock().await.admission_trace = Some(Vec::new());
    }

    /// Drains and disables diagnostic tracing without changing the scheduler.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn take_admission_trace(&self) -> Vec<(tokio::time::Instant, u64)> {
        self.0
            .state
            .lock()
            .await
            .admission_trace
            .take()
            .unwrap_or_default()
    }
}

#[derive(Debug, Default)]
struct SourceQosCounters {
    logical_bytes: AtomicU64,
    shaped_bytes: AtomicU64,
    source_read_operations: AtomicU64,
    native_bytes: AtomicU64,
    native_requests: AtomicU64,
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
pub struct SourceQosCancelled;

impl SourceQosBudget {
    pub fn set_logical_bytes(&self, bytes: u64) {
        self.stats.logical_bytes.store(bytes, Ordering::Relaxed);
    }

    #[must_use]
    pub fn stats(&self) -> SourceQosStats {
        SourceQosStats {
            logical_bytes: self.stats.logical_bytes.load(Ordering::Relaxed),
            client_streamed_shaped_bytes: self.stats.shaped_bytes.load(Ordering::Relaxed),
            native_bytes: self.stats.native_bytes.load(Ordering::Relaxed),
            source_read_operations: self.stats.source_read_operations.load(Ordering::Relaxed),
            native_requests: self.stats.native_requests.load(Ordering::Relaxed),
            native_payload_shaped: false,
        }
    }

    pub async fn admit_read(
        &self,
        requested: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        self.admit(requested, cancel).await
    }

    /// Plans a packetized read without reserving time or consuming an operation.
    #[must_use]
    pub fn read_size(&self, requested: u64) -> u64 {
        requested.min(self.group.0.policy.read_grant_bytes)
    }

    /// Admit one bounded protocol request without turning bandwidth time slices into requests.
    /// The entire grant is paced before dispatch; `max_read_bytes` bounds its burst and memory.
    pub async fn admit_request(
        &self,
        requested: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        self.schedule(self.request_size(requested), cancel).await
    }

    /// Plans one bounded request without consuming quota or issuing IO.
    #[must_use]
    pub fn request_size(&self, requested: u64) -> u64 {
        requested.min(self.group.0.policy.max_read_bytes)
    }

    async fn admit(
        &self,
        requested: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        let granted = self.read_size(requested);
        self.schedule(granted, cancel).await
    }

    async fn schedule(
        &self,
        granted: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, SourceQosCancelled> {
        if granted == 0 {
            return Ok(0);
        }
        if cancel.is_cancelled() {
            return Err(SourceQosCancelled);
        }
        // An uncontended permit must not consume Tokio's cooperative budget.
        // Otherwise fast storage yields every 128 permits even at nonbinding rates.
        // try_lock cannot bypass permits already assigned to FIFO waiters.
        let mut state = if let Ok(state) = self.group.0.state.try_lock() {
            state
        } else {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(SourceQosCancelled),
                state = self.group.0.state.lock() => state,
            }
        };
        let now = tokio::time::Instant::now();
        // The immutable policy caches an upper bound for one strict request's
        // service time. If even the largest request is already paid, neither
        // token refill arithmetic nor deadline construction is needed.
        let immediate = self
            .group
            .0
            .policy
            .immediate_interval
            .is_some_and(|interval| now.saturating_duration_since(state.last_release) >= interval);
        let deadline =
            if immediate {
                now
            } else {
                bandwidth_deadline(&self.group.0.policy, &mut state, now, granted)
                    .max(iops_deadline(&self.group.0.policy, &mut state, now))
            };
        if !immediate && deadline > tokio::time::Instant::now() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(SourceQosCancelled),
                () = wait::until(deadline) => {}
            }
        }
        if cancel.is_cancelled() {
            return Err(SourceQosCancelled);
        }
        let released_at = tokio::time::Instant::now();
        if immediate {
            state.bandwidth_credit_at = released_at;
            state.next_hard_bandwidth = released_at;
            state.iops_credit_at = released_at;
            state.next_hard_iops = released_at;
        } else {
            commit_bandwidth(&self.group.0.policy, &mut state, released_at, granted);
            commit_iops(&self.group.0.policy, &mut state, released_at);
        }
        state.last_release = released_at;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = &mut state.admission_trace {
            trace.push((released_at, granted));
        }
        drop(state);
        self.stats
            .source_read_operations
            .fetch_add(1, Ordering::Relaxed);
        Ok(granted)
    }

    pub fn record_read_bytes(&self, bytes: u64) {
        self.stats.shaped_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_native(&self, bytes: u64, requests: u64) {
        self.stats.native_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.stats
            .native_requests
            .fetch_add(requests, Ordering::Relaxed);
    }
}

// Round up: rounding a hard interval down by even one nanosecond can exceed
// the rate when checking intervals between individual admission events.
fn rate_interval(units: u64, rate: u64) -> Duration {
    let seconds = units / rate;
    let nanos = (u128::from(units % rate) * 1_000_000_000).div_ceil(u128::from(rate));
    Duration::from_secs(seconds) + Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
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
    let soft_rate = soft.to_f64().unwrap_or(f64::MAX);
    let byte_count = bytes.to_f64().unwrap_or(f64::MAX);
    let capacity =
        (hard - soft).to_f64().unwrap_or(f64::MAX) * policy.bandwidth_peak_duration.as_secs_f64();
    state.bandwidth_credit = (state.bandwidth_credit + elapsed * soft_rate).min(capacity);
    state.bandwidth_credit_at = now;
    let soft_wait = if state.bandwidth_credit >= byte_count {
        Duration::ZERO
    } else {
        Duration::from_secs_f64((byte_count - state.bandwidth_credit) / soft_rate)
    };
    let hard_wait = rate_interval(bytes, hard);
    let nominal_hard = state.next_hard_bandwidth + hard_wait;
    let soft_deadline = if capacity == 0.0 {
        state.next_hard_bandwidth + rate_interval(bytes, soft)
    } else {
        now + soft_wait
    };
    soft_deadline.max(nominal_hard).max(now)
}

fn commit_bandwidth(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    released_at: tokio::time::Instant,
    bytes: u64,
) {
    let (Some(soft), Some(hard)) = (policy.soft_bytes_per_second, policy.hard_bytes_per_second)
    else {
        return;
    };
    let capacity =
        (hard - soft).to_f64().unwrap_or(f64::MAX) * policy.bandwidth_peak_duration.as_secs_f64();
    let soft = soft.to_f64().unwrap_or(f64::MAX);
    let refilled = released_at
        .saturating_duration_since(state.bandwidth_credit_at)
        .as_secs_f64()
        * soft;
    // Waiting for either resource replenishes the soft budget too. Charge the
    // admitted packet before capping the remainder: packets may exceed capacity.
    state.bandwidth_credit = (state.bandwidth_credit + refilled
        - bytes.to_f64().unwrap_or(f64::MAX))
    .clamp(0.0, capacity);
    state.bandwidth_credit_at = released_at;
    // Actual release, rather than the requested deadline, prevents catch-up
    // bursts after timer/scheduler stalls. IO time satisfies the next slot.
    state.next_hard_bandwidth = released_at;
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
    state.iops_credit_at = now;
    let soft_wait = if state.iops_credit >= 1.0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64((1.0 - state.iops_credit) / f64::from(soft))
    };
    let hard_wait = rate_interval(1, u64::from(hard));
    let nominal_hard = state.next_hard_iops + hard_wait;
    let soft_deadline = if capacity == 0.0 {
        state.next_hard_iops + rate_interval(1, u64::from(soft))
    } else {
        now + soft_wait
    };
    soft_deadline.max(nominal_hard).max(now)
}

fn commit_iops(
    policy: &SourceQosPolicy,
    state: &mut SchedulerState,
    released_at: tokio::time::Instant,
) {
    let (Some(soft), Some(hard)) = (policy.soft_iops, policy.hard_iops) else {
        return;
    };
    let capacity = f64::from(hard - soft) * policy.iops_peak_duration.as_secs_f64();
    let refilled = released_at
        .saturating_duration_since(state.iops_credit_at)
        .as_secs_f64()
        * f64::from(soft);
    state.iops_credit = (state.iops_credit + refilled - 1.0).clamp(0.0, capacity);
    state.iops_credit_at = released_at;
    state.next_hard_iops = released_at;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(max_read_bytes: u64) -> SourceQosPolicy {
        SourceQosPolicy::new(None, max_read_bytes, None).unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn hard_intervals_round_up_to_nanosecond_resolution() {
        assert_eq!(rate_interval(1, 3), Duration::from_nanos(333_333_334));
        assert_eq!(rate_interval(1, 1_000_000_000), Duration::from_nanos(1));
        assert_eq!(rate_interval(u64::MAX, u64::MAX), Duration::from_secs(1));
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

    #[tokio::test]
    async fn nonbinding_admissions_do_not_exhaust_the_source_tasks_cooperative_budget() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(
                Some((1 << 50, 1 << 50, Duration::ZERO)),
                64 * 1024,
                Some((1_000_000_000, 1_000_000_000, Duration::ZERO)),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        // One-nanosecond slots are already satisfied by the call overhead.
        for operation in 0..1024 {
            let admission = budget.admit_read(64 * 1024, &cancel);
            tokio::pin!(admission);
            assert!(
                futures::poll!(admission).is_ready(),
                "unnecessary yield at operation {operation}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cached_strict_interval_neither_batches_slots_nor_overdelays_small_requests() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(Some((1_000, 1_000, Duration::ZERO)), 64, None)
                .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_request(8, &cancel).await, Ok(8));
        assert!(started.elapsed() >= Duration::from_millis(8));
        assert!(started.elapsed() < Duration::from_millis(16));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_request(8, &cancel).await, Ok(8));
        assert_eq!(started.elapsed(), Duration::ZERO);
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_request(4, &cancel).await, Ok(4));
        assert!(started.elapsed() >= Duration::from_millis(4));
        assert!(started.elapsed() < Duration::from_millis(8));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_precise_tail_is_uncharged_and_keeps_the_next_slot() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(None, 1, Some((1_000, 1_000, Duration::ZERO)))
                .unwrap_or_else(|error| panic!("{error}")),
        );
        group.enable_admission_trace().await;
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        let waiting = budget.admit_read(1, &cancel);
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        cancel.cancel();
        assert_eq!(waiting.await, Err(SourceQosCancelled));
        assert_eq!(budget.stats(), SourceQosStats::default());
        assert!(group.take_admission_trace().await.is_empty());
        tokio::time::advance(Duration::from_millis(2)).await;
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_read(1, &CancellationToken::new()).await, Ok(1));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn completed_io_time_satisfies_strict_bandwidth_and_iops_slots() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(
                Some((1_000, 1_000, Duration::ZERO)),
                8,
                Some((125, 125, Duration::ZERO)),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        assert_eq!(budget.admit_read(8, &cancel).await, Ok(8));
        // Actual source IO already consumed more than both 8 ms intervals.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_read(8, &cancel).await, Ok(8));
        assert_eq!(started.elapsed(), Duration::ZERO);
        let started = tokio::time::Instant::now();
        assert_eq!(budget.admit_read(8, &cancel).await, Ok(8));
        assert!(started.elapsed() >= Duration::from_millis(8));
    }

    #[tokio::test(start_paused = true)]
    async fn hard_wait_refills_soft_credit_for_the_full_peak_duration() {
        let group = SourceQosGroup::new(
            SourceQosPolicy::new(
                Some((100, 200, Duration::from_millis(500))),
                20,
                Some((5, 10, Duration::from_millis(500))),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        );
        let budget = group.transfer_budget();
        let cancel = CancellationToken::new();
        assert_eq!(budget.admit_request(20, &cancel).await, Ok(20));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let started = tokio::time::Instant::now();
        for _ in 0..5 {
            assert_eq!(budget.admit_request(20, &cancel).await, Ok(20));
        }
        assert!(started.elapsed() >= Duration::from_millis(500));
        assert!(
            started.elapsed() <= Duration::from_millis(510),
            "{:?}",
            started.elapsed()
        );
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
        // After idle, one read may proceed immediately. Later reads still
        // require their full hard interval; missed slots never accumulate.
        assert!(credited_started.elapsed() >= Duration::from_millis(40));
        assert!(credited_started.elapsed() < Duration::from_millis(100));
    }
}
