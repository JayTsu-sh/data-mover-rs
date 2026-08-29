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

impl TraversalSession {
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
