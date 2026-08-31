//! Bounded streams of immutable storage observations.

use std::fmt;
use std::num::NonZeroUsize;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::model::{
    BackendSessionFailure, EntryOperationFailure, ObservationPlan, ObservedEntry, StoragePath,
};

#[allow(dead_code)]
pub(crate) mod local;
mod storage;
pub use storage::StorageTraversalSource;
#[cfg(test)]
mod hdfs_tests;

/// Traversal output order. Concurrent completion never changes admission order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraversalOrder {
    Admission,
}

/// A bounded traversal request.
#[derive(Clone, Debug)]
pub struct TraversalRequest {
    pub root: StoragePath,
    pub order: TraversalOrder,
    pub max_inflight_operations: NonZeroUsize,
    pub max_buffered_items: NonZeroUsize,
    pub observation_plan: ObservationPlan,
    pub cancel: CancellationToken,
}

/// One ordered traversal item. Entry failures do not terminate the session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraversalItem {
    Entry(Box<ObservedEntry>),
    EntryFailure(EntryOperationFailure),
}

/// Positive evidence that enumeration reached its normal terminal boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraversalCompletion {
    pub observed_entries: u64,
    pub entry_failures: u64,
}

/// Normal terminal outcomes, distinct from backend/runtime failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraversalOutcome {
    Completed(TraversalCompletion),
    Cancelled,
}

/// A terminal outcome that cannot be represented as an entry item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraversalTerminalFailure {
    Session(BackendSessionFailure),
    Internal,
}

impl fmt::Display for TraversalTerminalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(formatter),
            Self::Internal => formatter.write_str("traversal runtime failed"),
        }
    }
}

impl std::error::Error for TraversalTerminalFailure {}

/// Bounded item receiver paired with mandatory completion evidence.
pub struct TraversalSession {
    items: mpsc::Receiver<TraversalItem>,
    completion: oneshot::Receiver<Result<TraversalOutcome, TraversalTerminalFailure>>,
    cancel: CancellationToken,
    exhausted: bool,
}

/// Bounded producer paired with a [`TraversalSession`] for external traversal implementations.
pub struct TraversalProducer {
    items: mpsc::Sender<TraversalItem>,
    completion: Option<oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>>,
}

impl TraversalProducer {
    /// Sends one ordered item while preserving the session's backpressure bound.
    ///
    /// # Errors
    /// Returns the item when the consumer has already closed the session.
    pub async fn send(&self, item: TraversalItem) -> Result<(), TraversalItem> {
        self.items.send(item).await.map_err(|error| error.0)
    }

    /// Closes the item stream and publishes its mandatory terminal evidence.
    pub fn finish(mut self, outcome: Result<TraversalOutcome, TraversalTerminalFailure>) {
        drop(self.items);
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(outcome);
        }
    }
}

impl TraversalSession {
    /// Creates a bounded producer/session pair for a [`TraversalSource`] implementation.
    #[must_use]
    pub fn bounded(capacity: NonZeroUsize, cancel: CancellationToken) -> (TraversalProducer, Self) {
        let (item_tx, item_rx) = mpsc::channel(capacity.get());
        let (completion_tx, completion_rx) = oneshot::channel();
        (
            TraversalProducer {
                items: item_tx,
                completion: Some(completion_tx),
            },
            Self::new(item_rx, completion_rx, cancel),
        )
    }

    pub(crate) fn new(
        items: mpsc::Receiver<TraversalItem>,
        completion: oneshot::Receiver<Result<TraversalOutcome, TraversalTerminalFailure>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            items,
            completion,
            cancel,
            exhausted: false,
        }
    }

    /// Receives the next ordered item with consumer-driven backpressure.
    pub async fn next_item(&mut self) -> Option<TraversalItem> {
        let item = tokio::select! {
            biased;
            () = self.cancel.cancelled() => {
                self.items.close();
                while self.items.try_recv().is_ok() {}
                None
            }
            item = self.items.recv() => item,
        };
        self.exhausted |= item.is_none();
        item
    }

    /// Returns positive completion evidence or the unique terminal failure.
    ///
    /// # Errors
    /// Returns `Internal` if called before EOF or if the producer disappears without evidence.
    pub async fn finish(self) -> Result<TraversalOutcome, TraversalTerminalFailure> {
        if !self.exhausted {
            return Err(TraversalTerminalFailure::Internal);
        }
        self.completion
            .await
            .unwrap_or(Err(TraversalTerminalFailure::Internal))
    }
}

/// Backend-neutral traversal source.
pub trait TraversalSource: Send + Sync {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession;
}

#[cfg(test)]
mod producer_tests {
    use super::*;

    #[tokio::test]
    async fn bounded_pair_delivers_items_before_completion_evidence() {
        let (producer, mut session) =
            TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        let task = tokio::spawn(async move {
            producer.send(failure()).await.unwrap();
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 0,
                entry_failures: 1,
            })));
        });
        assert!(matches!(
            session.next_item().await,
            Some(TraversalItem::EntryFailure(_))
        ));
        assert!(session.next_item().await.is_none());
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn producer_drop_without_evidence_is_a_terminal_failure() {
        let (producer, mut session) =
            TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        drop(producer);
        assert!(session.next_item().await.is_none());
        assert_eq!(
            session.finish().await,
            Err(TraversalTerminalFailure::Internal)
        );
    }

    fn failure() -> TraversalItem {
        TraversalItem::EntryFailure(
            EntryOperationFailure::new(
                StoragePath::new("denied").unwrap(),
                crate::model::Operation::Observe,
                crate::model::FailureClass::PermissionDenied,
                crate::model::Transience::Permanent,
                "denied",
            )
            .unwrap(),
        )
    }
}
