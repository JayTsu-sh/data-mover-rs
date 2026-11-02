//! Drains each attempt before changing connection or directory checkpoint.

use std::{task::Poll, time::Duration};

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{Outcome, Recovery};
use crate::model::{FailureClass, StoragePath};
use crate::traversal::{
    ScanFailureSignal, StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalSession,
    TraversalSource, TraversalTerminalFailure,
};

struct AttemptCancellation(CancellationToken);
impl Drop for AttemptCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Default)]
struct HistoryBoundary(Option<(StoragePath, u32)>);

impl HistoryBoundary {
    fn accept(&mut self, item: &TraversalItem) -> bool {
        let TraversalItem::Entry(entry) = item else {
            return self.0.is_none() || matches!(item, TraversalItem::ObservationFailure(_));
        };
        let Some(history) = entry.history() else {
            return self.0.is_none();
        };
        let expected = self.0.as_ref().map_or(0, |(_, next)| *next);
        if history.segment() != expected {
            return false;
        }
        if self
            .0
            .as_ref()
            .is_some_and(|(path, _)| path != entry.path())
        {
            return false;
        }
        self.0 = if history.is_complete() {
            None
        } else {
            Some((entry.path().clone(), expected.saturating_add(1)))
        };
        true
    }
}

impl Recovery {
    pub(super) async fn attempt(
        &mut self,
        source: StorageTraversalSource,
        signal: &ScanFailureSignal,
    ) -> Outcome {
        self.restoring = self.window.is_some();
        let request = self.request_for_attempt();
        let cancel = AttemptCancellation(request.cancel.clone());
        let mut inner = source.traverse(request);
        let mut history = HistoryBoundary::default();
        loop {
            self.observe(signal);
            if signal.failure().is_some() {
                cancel.0.cancel();
            }
            if self.expired() {
                return self.exhausted();
            }
            let Some(item) = self.next_item(&mut inner, signal).await? else {
                break;
            };
            if !history.accept(&item) {
                return self.unsafe_history();
            }
            if self.forward(item, signal).await.is_err() {
                return if self.expired() {
                    self.exhausted()
                } else {
                    Ok(TraversalOutcome::Cancelled)
                };
            }
        }
        if history.0.is_some() {
            return self.unsafe_history();
        }
        inner.finish().await
    }

    fn unsafe_history(&mut self) -> Outcome {
        if let Some(window) = &mut self.window {
            window.budget.exhaust();
            self.exhausted()
        } else {
            Err(TraversalTerminalFailure::Internal)
        }
    }

    fn expired(&self) -> bool {
        !self.request.cancel.is_cancelled()
            && self
                .window
                .as_ref()
                .is_some_and(|window| window.budget.is_exhausted())
    }

    async fn next_item(
        &mut self,
        inner: &mut TraversalSession,
        signal: &ScanFailureSignal,
    ) -> Result<Option<TraversalItem>, TraversalTerminalFailure> {
        loop {
            self.observe(signal);
            if self.expired() {
                return self.exhausted().map(|_| None);
            }
            tokio::select! {
                biased;
                () = self.request.cancel.cancelled(), if !self.drains() => return Ok(None),
                () = signal.failed(), if signal.failure().is_none() => {
                    self.observe(signal);
                }
                item = inner.next_item() => return Ok(item),
                () = sleep(Duration::from_millis(10)) => {}
            }
        }
    }

    fn drains(&self) -> bool {
        self.request.resumable.every().is_some()
    }

    pub(super) async fn forward(
        &mut self,
        item: TraversalItem,
        signal: &ScanFailureSignal,
    ) -> Result<(), ()> {
        if transient_item(&item) && signal.failure().is_some() {
            return Ok(());
        }
        if let TraversalItem::Checkpoint(saved) = &item {
            self.checkpoint = Some((**saved).clone());
            if !self.drains() {
                return Ok(());
            }
        }
        self.observe(signal);
        if self.expired() {
            return Err(());
        }
        let output = self.producer.items.clone();
        let sent = output.send_one(item);
        tokio::pin!(sent);
        loop {
            if let Poll::Ready(result) = futures::poll!(&mut sent) {
                self.start_window(signal, true);
                return result.map_err(|_| ());
            }
            self.start_window(signal, true);
            let _waiting = self
                .window
                .as_ref()
                .map(|window| window.budget.consumer_wait());
            self.observe(signal);
            tokio::select! {
                biased;
                () = self.request.cancel.cancelled(), if !self.drains() => return Err(()),
                () = signal.failed(), if signal.failure().is_none() => {
                    self.start_window(signal, true);
                    self.observe(signal);
                }
                result = &mut sent => {
                    self.start_window(signal, true);
                    return result.map_err(|_| ());
                }
                () = sleep(Duration::from_millis(10)) => {}
            }
        }
    }
}

fn transient_item(item: &TraversalItem) -> bool {
    let class = match item {
        TraversalItem::Failure(failure) => failure.failure.class(),
        TraversalItem::ObservationFailure(failure) => failure.class,
        _ => return false,
    };
    matches!(class, FailureClass::Connectivity | FailureClass::Timeout)
}
