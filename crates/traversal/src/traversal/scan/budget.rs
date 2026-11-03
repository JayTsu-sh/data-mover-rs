//! One wall-clock deadline from first fault detection through original-read verification.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

#[derive(Clone)]
pub(in crate::traversal) struct RecoveryBudget(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Clone, Copy)]
enum State {
    Active(Instant),
    Completed,
    Exhausted,
}

impl RecoveryBudget {
    pub(in crate::traversal) fn new(deadline: Instant) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State::Active(deadline)),
            changed: Notify::new(),
        }))
    }

    pub(in crate::traversal) fn complete(&self) {
        *self.0.state.lock() = State::Completed;
        self.0.changed.notify_waiters();
    }

    pub(in crate::traversal) fn exhaust(&self) {
        let mut state = self.0.state.lock();
        if !matches!(*state, State::Completed) {
            *state = State::Exhausted;
        }
        drop(state);
        self.0.changed.notify_waiters();
    }

    pub(in crate::traversal) fn is_exhausted(&self) -> bool {
        match *self.0.state.lock() {
            State::Active(deadline) => Instant::now() >= deadline,
            State::Completed => false,
            State::Exhausted => true,
        }
    }

    pub(in crate::traversal) async fn exhausted(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let state = *self.0.state.lock();
            match state {
                State::Active(deadline) if Instant::now() < deadline => {
                    tokio::select! {
                        () = sleep_until(deadline) => {},
                        () = &mut changed => {},
                    }
                }
                State::Completed => changed.await,
                State::Active(_) | State::Exhausted => return,
            }
        }
    }
}
