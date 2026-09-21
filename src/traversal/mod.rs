//! Bounded streams of immutable storage observations.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, ObservationPlan, ObservedEntry,
    StoragePath, StorageTimestamp,
};

mod storage;
pub use storage::StorageTraversalSource;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod local_tests;

/// Traversal output order. Concurrent completion never changes admission order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraversalOrder {
    Admission,
}

/// Facts known about one directory child when the traversal decides whether to emit it,
/// descend into it, and keep filtering below it.
#[derive(Clone, Copy, Debug)]
pub struct TraversalCandidate<'a> {
    /// Path relative to the **traversal root**, slash-separated and without a leading slash.
    ///
    /// This is the frame of reference the legacy walkers used, so an expression written against
    /// a scan root keeps matching. It is deliberately not the backend-relative path that the
    /// emitted [`ObservedEntry`] carries.
    pub path: &'a str,
    /// Final path component.
    pub name: &'a str,
    pub kind: EntryKind,
    pub size: Option<u64>,
    /// Present only when the enumerating operation or a metadata observation already
    /// supplied it; see [`TraversalFilter::needs_modified`].
    pub modified: Option<StorageTimestamp>,
}

/// Per-entry traversal decision. The three components are independent:
///
/// | field | meaning |
/// |---|---|
/// | `emit` | deliver this entry as a [`TraversalItem::Entry`] |
/// | `descend` | list this directory's children (ignored for non-directories) |
/// | `filter_children` | keep consulting the filter below this directory; `false` admits the whole subtree unfiltered |
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraversalDecision {
    pub emit: bool,
    pub descend: bool,
    pub filter_children: bool,
}

impl TraversalDecision {
    /// The decision for an entry that is not subject to filtering.
    #[must_use]
    pub const fn unfiltered(kind: EntryKind) -> Self {
        Self {
            emit: true,
            descend: matches!(kind, EntryKind::Directory),
            filter_children: false,
        }
    }
}

/// Caller-supplied admission and pruning policy consulted for every enumerated child.
///
/// The trait keeps the traversal independent of any concrete expression language; the
/// crate's DSL adapter lives outside this module.
pub trait TraversalFilter: fmt::Debug + Send + Sync {
    /// Whether [`TraversalCandidate::modified`] must be known before [`Self::decide`] is
    /// meaningful. When `true`, the traversal observes every candidate before deciding and
    /// never evaluates the filter with an absent timestamp.
    fn needs_modified(&self) -> bool;

    /// Decides one candidate. May be called in any order, before the entry is admitted, and for
    /// entries that are never delivered if the traversal ends early; implementations must not
    /// depend on call order or keep state between calls that affects decisions.
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision;
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
    /// Optional admission / pruning policy. `None` admits every entry and descends into every
    /// directory.
    pub filter: Option<Arc<dyn TraversalFilter>>,
    /// Maximum depth to enumerate. Children of `root` are depth 1; a directory at depth `d`
    /// is listed only when `d < max_depth`. `None` means unlimited.
    pub max_depth: Option<NonZeroUsize>,
}

/// Re-expresses a backend-relative path in the traversal root's frame of reference.
///
/// A leading `./` is stripped, so a `path` expression written against the scan root matches
/// however the path was spelled. No backend emits one today; the Local namespace role spells
/// root children without it.
///
/// The root only matches on a component boundary. A raw string prefix would rewrite a sibling
/// that merely starts with the root's spelling — `keeper/a` under root `keep` would become
/// `er/a` — which silently corrupts both the emitted path and anything matched against it.
pub(crate) fn relative_to<'a>(root: &StoragePath, path: &'a StoragePath) -> &'a str {
    let value = path.as_str().strip_prefix("./").unwrap_or(path.as_str());
    let root = root.as_str();
    if root.is_empty() {
        return value;
    }
    value
        .strip_prefix(root)
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .map_or(value, |rest| rest.trim_start_matches('/'))
}

#[cfg(test)]
mod relative_tests {
    use super::{StoragePath, relative_to};

    fn path(value: &str) -> StoragePath {
        StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn candidate_paths_are_normalised_against_the_traversal_root() {
        assert_eq!(relative_to(&StoragePath::root(), &path("a/b")), "a/b");
        assert_eq!(relative_to(&StoragePath::root(), &path("./a/b")), "a/b");
        assert_eq!(relative_to(&path("keep"), &path("keep/a/b")), "a/b");
        assert_eq!(relative_to(&path("keep"), &path("./keep/a/b")), "a/b");
        // A path outside the root keeps its own spelling rather than being silently truncated.
        assert_eq!(relative_to(&path("keep"), &path("other/a")), "other/a");
    }

    #[test]
    fn a_sibling_sharing_the_roots_spelling_is_not_rewritten() {
        // `keeper` merely starts with `keep`; it is not inside it. A raw string prefix would
        // have turned this into `er/a`, corrupting the emitted path and every expression
        // matched against it.
        assert_eq!(relative_to(&path("keep"), &path("keeper/a")), "keeper/a");
        assert_eq!(relative_to(&path("keep"), &path("keeper")), "keeper");
        assert_eq!(relative_to(&path("a/b"), &path("a/bc/d")), "a/bc/d");
        // The root itself rebases to the empty path, which is the traversal root's own name.
        assert_eq!(relative_to(&path("keep"), &path("keep")), "");
        // A deeper match still rebases normally.
        assert_eq!(relative_to(&path("a/b"), &path("a/b/c")), "c");
    }
}

impl TraversalRequest {
    /// Whether a directory at `depth` (direct children of the traversal root are 1) may be
    /// listed under `max_depth`.
    #[must_use]
    pub(crate) fn admits_depth(&self, depth: usize) -> bool {
        self.max_depth.is_none_or(|limit| depth < limit.get())
    }
}

/// One ordered traversal item. Entry failures do not terminate the session.
///
/// A listed directory `D` produces, in order: its block, which is one item per admitted child;
/// then [`TraversalItem::DirectoryListed`] for `D`; then, for every subdirectory `D` descends
/// into, that same sequence recursively; and last [`TraversalItem::SubtreeComplete`] for `D`.
/// A traversal that runs to completion therefore ends with the root's `SubtreeComplete`.
///
/// Only a directory that was actually listed gets those two items. A directory `max_depth`
/// stops at is emitted as an entry and never listed, so it has neither; a directory the filter
/// hides but still descends into has both, with no [`TraversalItem::Entry`] of its own.
///
/// A `DirectoryListed` can arrive without its `SubtreeComplete`: cancellation stops the
/// traversal wherever it stands, and only [`TraversalOutcome::Completed`] guarantees the pair.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalItem {
    Entry(Box<ObservedEntry>),
    EntryFailure(EntryOperationFailure),
    /// Every direct child of one listed directory has been delivered.
    DirectoryListed(DirectoryListed),
    /// One listed directory's whole subtree has been delivered.
    SubtreeComplete(Box<SubtreeComplete>),
}

/// Every direct child of one listed directory has been delivered.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DirectoryListed {
    pub path: StoragePath,
    pub listing: DirectoryListing,
    /// Direct subdirectories the filter did not descend into. Each was still emitted as an
    /// entry, and none of them has completion items of its own.
    pub pruned_children: u64,
    /// Direct subdirectories `max_depth` stopped at. Each was still emitted as an entry, and
    /// none of them has completion items of its own.
    pub truncated_children: u64,
}

/// How complete one directory's block is.
///
/// The states are exclusive. When more than one applies, the strongest holds: `Failed` over
/// `Partial` over `Filtered` over `Complete`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DirectoryListing {
    /// Every child was described and emitted.
    Complete,
    /// Every child was described, but the filter kept at least one out of the output. A
    /// subdirectory that was merely not descended into does not make a listing `Filtered`,
    /// because its own entry was still emitted; see [`DirectoryListed::pruned_children`].
    Filtered,
    /// The listing succeeded, but `failures` children produced an entry failure instead of an
    /// entry: the listing could not name them, or observing them failed. Each one is an item in
    /// this block.
    Partial { failures: u64 },
    /// The directory could not be listed at all. Why is in the `Operation::Traverse` entry
    /// failure that is the only item in its block.
    Failed,
}

/// One listed directory's whole subtree has been delivered.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SubtreeComplete {
    pub path: StoragePath,
    pub summary: SubtreeSummary,
}

/// What one subtree, the directory at its root included, turned out to hold.
///
/// A listing that failed counts in both `entry_failures` and `failed_listings`: the first counts
/// items, the second counts directories, and one failed listing is one of each.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct SubtreeSummary {
    pub directories_listed: u64,
    pub filtered_listings: u64,
    pub partial_listings: u64,
    pub failed_listings: u64,
    pub observed_entries: u64,
    pub entry_failures: u64,
    /// Subdirectories the filter did not descend into.
    pub pruned_directories: u64,
    /// Subdirectories `max_depth` stopped at.
    pub truncated_directories: u64,
}

impl SubtreeSummary {
    /// Whether this subtree is a complete account of what the source holds: nothing hidden,
    /// pruned, truncated or failed. Only then may a caller treat what it did not see as absent.
    #[must_use]
    pub const fn is_exhaustive(&self) -> bool {
        self.filtered_listings == 0
            && self.partial_listings == 0
            && self.failed_listings == 0
            && self.entry_failures == 0
            && self.pruned_directories == 0
            && self.truncated_directories == 0
    }
}

/// Whether an item is a directory completion item rather than an entry. Tests about entry
/// order skip these; the ones in `storage::tests::events` are about them.
#[cfg(test)]
pub(crate) const fn is_completion(item: &TraversalItem) -> bool {
    matches!(
        item,
        TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_)
    )
}

/// Positive evidence that enumeration reached its normal terminal boundary.
///
/// These are item tallies for the whole traversal. The structural account of what was and was
/// not seen is [`SubtreeSummary`], carried by the root's [`TraversalItem::SubtreeComplete`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TraversalCompletion {
    pub observed_entries: u64,
    pub entry_failures: u64,
    /// Directories that were listed, the traversal root included. Every one of them produced a
    /// [`TraversalItem::DirectoryListed`].
    pub directories_listed: u64,
}

impl TraversalCompletion {
    #[must_use]
    pub const fn new(observed_entries: u64, entry_failures: u64, directories_listed: u64) -> Self {
        Self {
            observed_entries,
            entry_failures,
            directories_listed,
        }
    }
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
        let (producer, mut session) = TraversalSession::bounded(
            NonZeroUsize::new(1).unwrap_or_else(|| unreachable!()),
            CancellationToken::new(),
        );
        let task = tokio::spawn(async move {
            producer
                .send(failure())
                .await
                .unwrap_or_else(|_| panic!("session receiver dropped"));
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion::new(
                0, 1, 0,
            ))));
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
        task.await
            .unwrap_or_else(|error| panic!("producer task failed: {error}"));
    }

    #[tokio::test]
    async fn producer_drop_without_evidence_is_a_terminal_failure() {
        let (producer, mut session) = TraversalSession::bounded(
            NonZeroUsize::new(1).unwrap_or_else(|| unreachable!()),
            CancellationToken::new(),
        );
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
                StoragePath::new("denied").unwrap_or_else(|error| panic!("{error}")),
                crate::model::Operation::Observe,
                crate::model::FailureClass::PermissionDenied,
                crate::model::Transience::Permanent,
                "denied",
            )
            .unwrap_or_else(|error| panic!("{error}")),
        )
    }
}
