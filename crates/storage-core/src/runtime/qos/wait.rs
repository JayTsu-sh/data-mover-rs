//! Keep millisecond timer rounding out of the final source-admission deadline.
use std::hint::spin_loop;
use std::thread::sleep;
use std::time::{Duration, Instant as WallInstant};

use tokio::task::{spawn_blocking, yield_now};
use tokio::time::{Instant as TokioInstant, sleep_until};

const PRECISE_TAIL: Duration = Duration::from_millis(2);
const WAKE_MARGIN: Duration = Duration::from_micros(200);
const INLINE_LIMIT: Duration = Duration::from_micros(8);

pub(super) async fn until(deadline: TokioInstant) {
    let remaining = deadline.saturating_duration_since(TokioInstant::now());
    if remaining.is_zero() {
        return;
    }
    if remaining <= INLINE_LIMIT {
        // A runtime handoff can cost much more than an almost-satisfied hard
        // interval. Bound both CPU occupancy and cancellation latency; the wall
        // limit also prevents paused test clocks from spinning indefinitely.
        let wall_deadline = WallInstant::now() + INLINE_LIMIT;
        while deadline > TokioInstant::now() && WallInstant::now() < wall_deadline {
            spin_loop();
        }
        if deadline <= TokioInstant::now() {
            return;
        }
    }
    if remaining > PRECISE_TAIL {
        sleep_until(deadline - PRECISE_TAIL).await;
    }
    let remaining = deadline.saturating_duration_since(TokioInstant::now());
    if remaining > WAKE_MARGIN {
        // OS sleep gives a finer deadline than Tokio's timer wheel. Only the
        // bounded tail occupies a blocking worker. Cancellation may detach at
        // most this two-millisecond tail.
        let _ = spawn_blocking(move || {
            sleep(remaining.saturating_sub(WAKE_MARGIN));
        })
        .await;
    }
    // The blocking pool's return hop and OS sleep can overshoot a deadline.
    // Yield cooperatively for the bounded final margin instead of adding that
    // error to every hard-rate interval. Other tasks and cancellation can run.
    let wall_deadline = WallInstant::now() + WAKE_MARGIN;
    while deadline > TokioInstant::now() && WallInstant::now() < wall_deadline {
        yield_now().await;
    }
    // Paused clocks do not advance while a yielding task remains runnable.
    // Also retain a conservative fallback if the clock or pool is shutting down.
    if deadline > TokioInstant::now() {
        sleep_until(deadline).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    #[tokio::test]
    async fn microsecond_deadlines_do_not_force_a_runtime_handoff() {
        for _ in 0..100 {
            let deadline = TokioInstant::now() + Duration::from_micros(4);
            assert!(until(deadline).now_or_never().is_some());
        }
    }
}
