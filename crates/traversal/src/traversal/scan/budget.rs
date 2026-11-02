//! Recovery charges reconnects, faulting reads and failed replay, not healthy rebuilding.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};

#[derive(Clone)]
pub(in crate::traversal) struct RecoveryBudget(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    remaining: Duration,
    updated: Instant,
    // Successful replay is free. Snapshot this union clock to charge a failed
    // replay read without charging overlapping target reads or consumer waits twice.
    replay_total: Duration,
    reconnecting: bool,
    targets: usize,
    consumers: usize,
    complete: bool,
}

impl State {
    fn settle(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.updated);
        self.updated = now;
        if !self.complete {
            if self.reconnecting || self.targets > 0 {
                self.remaining = self.remaining.saturating_sub(elapsed);
            } else if self.consumers == 0 {
                self.replay_total += elapsed;
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::traversal) struct ReadClock {
    replay: Duration,
}

pub(in crate::traversal) struct TargetRead(RecoveryBudget);
pub(in crate::traversal) struct ConsumerWait(RecoveryBudget);

impl RecoveryBudget {
    pub(in crate::traversal) fn new(remaining: Duration) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                remaining,
                updated: Instant::now(),
                replay_total: Duration::ZERO,
                reconnecting: true,
                targets: 0,
                consumers: 0,
                complete: false,
            }),
            changed: Notify::new(),
        }))
    }

    fn update(&self, update: impl FnOnce(&mut State)) {
        let mut state = self.0.state.lock();
        state.settle();
        update(&mut state);
        drop(state);
        self.0.changed.notify_waiters();
    }

    pub(in crate::traversal) fn reconnecting(&self, active: bool) {
        self.update(|state| state.reconnecting = active);
    }

    pub(in crate::traversal) fn target_read(&self) -> TargetRead {
        self.update(|state| state.targets += 1);
        TargetRead(self.clone())
    }

    pub(in crate::traversal) fn consumer_wait(&self) -> ConsumerWait {
        self.update(|state| state.consumers += 1);
        ConsumerWait(self.clone())
    }

    pub(in crate::traversal) fn clock(&self) -> ReadClock {
        let mut state = self.0.state.lock();
        state.settle();
        ReadClock {
            replay: state.replay_total,
        }
    }

    pub(in crate::traversal) fn failed_replay(&self, clock: ReadClock) {
        self.update(|state| {
            if !state.complete {
                let replay = state.replay_total.saturating_sub(clock.replay);
                state.remaining = state.remaining.saturating_sub(replay);
            }
        });
    }

    pub(in crate::traversal) fn complete(&self) {
        self.update(|state| state.complete = true);
    }

    pub(in crate::traversal) fn exhaust(&self) {
        self.update(|state| state.remaining = Duration::ZERO);
    }

    pub(in crate::traversal) fn is_exhausted(&self) -> bool {
        let mut state = self.0.state.lock();
        state.settle();
        !state.complete && state.remaining.is_zero()
    }

    pub(in crate::traversal) async fn exhausted(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (remaining, running, complete) = {
                let mut state = self.0.state.lock();
                state.settle();
                (
                    state.remaining,
                    state.reconnecting || state.targets > 0,
                    state.complete,
                )
            };
            if !complete && remaining.is_zero() {
                return;
            }
            if running && !complete {
                tokio::select! { () = sleep(remaining) => {}, () = &mut changed => {} }
            } else {
                changed.await;
            }
        }
    }
}

impl Drop for TargetRead {
    fn drop(&mut self) {
        self.0
            .update(|state| state.targets = state.targets.saturating_sub(1));
    }
}

impl Drop for ConsumerWait {
    fn drop(&mut self) {
        self.0
            .update(|state| state.consumers = state.consumers.saturating_sub(1));
    }
}
