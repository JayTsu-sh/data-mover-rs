//! Session-owned consumer work. Output backpressure never owns the feedback path.

use std::collections::{HashMap, VecDeque, hash_map::RandomState};
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

use super::{TraversalItem, TraversalTerminalFailure, channel};
use crate::model::ObservedEntry;

#[cfg(test)]
#[path = "consumer_tests.rs"]
mod tests;

pub(super) struct ConsumerState {
    session: [u8; 16],
    execution: u64,
    window: usize,
    ledger: Mutex<Ledger>,
    room: Arc<Semaphore>,
    changed: Notify,
    failure_changed: Notify,
    pub(super) stopped: CancellationToken,
}

#[derive(Default)]
struct Ledger {
    sequence: u64,
    pending: HashMap<[u8; 32], Work>,
    failure: Option<TraversalTerminalFailure>,
    closed: bool,
    /// Only facts from admitted pages/output that have not reached the consumer yet.
    diagnostics: VecDeque<TraversalItem>,
}

struct Work {
    issued: bool,
    result: oneshot::Sender<bool>,
    _permit: OwnedSemaphorePermit,
}

/// Cancelling an unpublished send releases its reservation synchronously.
struct Reservation {
    state: Arc<ConsumerState>,
    id: [u8; 32],
    published: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.published {
            self.state.ledger.lock().pending.remove(&self.id);
            self.state.changed.notify_one();
        }
    }
}

impl ConsumerState {
    pub(super) fn new(
        window: NonZeroUsize,
        execution: u64,
        cancel: &CancellationToken,
    ) -> Arc<Self> {
        let random = RandomState::new();
        let mut session = [0; 16];
        session[..8].copy_from_slice(&random.hash_one(0_u8).to_le_bytes());
        session[8..].copy_from_slice(&random.hash_one(1_u8).to_le_bytes());
        Arc::new(Self {
            session,
            execution,
            window: window.get().min(Semaphore::MAX_PERMITS),
            ledger: Mutex::new(Ledger::default()),
            room: Arc::new(Semaphore::new(window.get().min(Semaphore::MAX_PERMITS))),
            changed: Notify::new(),
            failure_changed: Notify::new(),
            stopped: cancel.child_token(),
        })
    }

    /// Both stages retain the complete observation; a finalize stage has its own ID.
    pub(super) async fn send(
        self: &Arc<Self>,
        output: &channel::ItemSender,
        entry: Box<ObservedEntry>,
        subtree_ready: bool,
    ) -> Result<oneshot::Receiver<bool>, ()> {
        let permit = tokio::select! {
            biased;
            () = self.stopped.cancelled() => return Err(()),
            permit = self.room.clone().acquire_owned() => permit.map_err(|_| ())?,
        };
        let (mut reservation, result) = self.reserve(permit, subtree_ready)?;
        let payload = Box::new((reservation.id, *entry));
        let item = if subtree_ready {
            TraversalItem::SubtreeReady(payload)
        } else {
            TraversalItem::ConsumerEntry(payload)
        };
        tokio::select! {
            biased;
            () = self.stopped.cancelled() => return Err(()),
            sent = output.send_one(item) => sent.map_err(|_| ())?,
        }
        reservation.published = true;
        Ok(result)
    }

    fn reserve(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        subtree_ready: bool,
    ) -> Result<(Reservation, oneshot::Receiver<bool>), ()> {
        let mut ledger = self.ledger.lock();
        if ledger.closed || self.stopped.is_cancelled() {
            return Err(());
        }
        ledger.sequence = ledger.sequence.checked_add(1).ok_or(())?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"data-mover consumer phase v1");
        hash.update(&self.session);
        hash.update(&self.execution.to_le_bytes());
        hash.update(&ledger.sequence.to_le_bytes());
        hash.update(&[u8::from(subtree_ready)]);
        let id = *hash.finalize().as_bytes();
        let (result, receiver) = oneshot::channel();
        ledger.pending.insert(
            id,
            Work {
                issued: false,
                result,
                _permit: permit,
            },
        );
        Ok((
            Reservation {
                state: self.clone(),
                id,
                published: false,
            },
            receiver,
        ))
    }

    pub(super) fn issue(&self, id: &[u8; 32]) -> bool {
        let mut ledger = self.ledger.lock();
        if ledger.closed || self.stopped.is_cancelled() {
            return false;
        }
        let Some(work) = ledger.pending.get_mut(id) else {
            return false;
        };
        if work.issued {
            return false;
        }
        work.issued = true;
        true
    }

    /// The caller retains its Vec until every decision is committed, with no await.
    pub(super) fn submit(&self, completed: &mut Vec<([u8; 32], bool)>) {
        if completed.len() > self.window {
            self.fail(TraversalTerminalFailure::ConsumerFeedbackLimitExceeded {
                limit: self.window,
            });
            completed.clear();
            return;
        }
        let mut ledger = self.ledger.lock();
        let mut rejected = false;
        for (id, success) in completed.iter() {
            if ledger.closed || self.stopped.is_cancelled() {
                break;
            }
            if !ledger.pending.get(id).is_some_and(|work| work.issued) {
                continue; // Unknown, unissued, cross-session, duplicate and late feedback.
            }
            if let Some(work) = ledger.pending.remove(id) {
                let _ = work.result.send(*success);
            }
            if !success {
                ledger
                    .failure
                    .get_or_insert(TraversalTerminalFailure::ConsumerFailed);
                rejected = true;
            }
        }
        completed.clear();
        drop(ledger);
        self.changed.notify_one();
        if rejected {
            self.close();
        }
    }

    pub(super) fn failure(&self) -> Option<TraversalTerminalFailure> {
        self.ledger.lock().failure.clone()
    }

    pub(super) fn fail(&self, failure: TraversalTerminalFailure) {
        self.record_failure(failure);
        self.close();
    }

    pub(super) fn record_failure(&self, failure: TraversalTerminalFailure) {
        self.record_observation(Vec::new(), Some(failure));
    }

    /// Registration is the observation's linearization point. Closing seals the cutoff:
    /// detached I/O completing later cannot change the terminal or append diagnostics.
    pub(super) fn record_observation(
        &self,
        diagnostics: Vec<TraversalItem>,
        failure: Option<TraversalTerminalFailure>,
    ) {
        let mut ledger = self.ledger.lock();
        if ledger.closed {
            return;
        }
        let failed = failure.is_some();
        if let Some(failure) = failure {
            ledger.failure.get_or_insert(failure);
        }
        ledger
            .diagnostics
            .extend(diagnostics.into_iter().filter(is_diagnostic));
        drop(ledger);
        if failed {
            self.failure_changed.notify_one();
        }
    }

    /// The output copy has reached its caller; remove exactly its pending fallback copy.
    /// This retains no delivered history. Cancellation instead seals/drops normal output.
    pub(super) fn delivered_diagnostic(&self, item: &TraversalItem) {
        if !is_diagnostic(item) {
            return;
        }
        let mut ledger = self.ledger.lock();
        if let Some(position) = ledger
            .diagnostics
            .iter()
            .position(|pending| pending == item)
        {
            ledger.diagnostics.remove(position);
        }
    }

    pub(super) fn next_diagnostic(&self) -> Option<TraversalItem> {
        self.ledger.lock().diagnostics.pop_front()
    }

    pub(super) fn close(&self) {
        let mut ledger = self.ledger.lock();
        ledger.closed = true;
        ledger.pending.clear();
        drop(ledger);
        self.room.close();
        self.stopped.cancel();
        self.changed.notify_one();
    }

    /// A dedicated actor notification cannot steal the session's ACK/EOF wake-up.
    pub(super) async fn failed(&self) {
        loop {
            let changed = self.failure_changed.notified();
            if self.failure().is_some() {
                return;
            }
            changed.await;
        }
    }

    pub(super) async fn settled(&self) {
        loop {
            let changed = self.changed.notified();
            if self.ledger.lock().pending.is_empty() {
                return;
            }
            tokio::select! {
                biased;
                () = self.stopped.cancelled() => return,
                () = changed => {}
            }
        }
    }
}

fn is_diagnostic(item: &TraversalItem) -> bool {
    matches!(
        item,
        TraversalItem::Failure(_) | TraversalItem::ObservationFailure(_)
    )
}
