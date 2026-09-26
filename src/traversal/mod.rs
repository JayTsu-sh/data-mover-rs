//! Bounded streams of immutable storage observations.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::model::{
    BackendSessionFailure, EntryKind, ObservationPlan, StoragePath, StorageTimestamp,
};

mod counts;
mod events;
mod storage;
pub use counts::{BlockCounts, ScanTotals};
pub use events::{
    Checkpoint, DirectoryListed, FailureScope, ListingOutcome, ScanFailure, TraversalItem,
};
pub use storage::StorageTraversalSource;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod local_tests;
#[cfg(test)]
mod s3_tests;

/// Traversal output order. Concurrent completion never changes admission order.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalOrder {
    /// Each directory's children arrive in the order its backend listed them, so two backends
    /// can deliver the same tree in two different orders.
    #[default]
    Admission,
    /// Each directory's children arrive sorted by the bytes of their final path component, and
    /// its subdirectories are descended into in that same order. Together with the block order
    /// every traversal already keeps, that makes the stream a total order the same tree produces
    /// on any backend, which is what two independently running traversals need to be compared by
    /// a streaming two-pointer merge that holds nothing: a run of paths present on one side only
    /// is a run of copies, or of deletions. The descent half of the guarantee is also what bounds
    /// how far the two sides can drift, because it is what lets a consumer tell which side is
    /// behind and stop reading from the other.
    ///
    /// **The key to compare by is `(parent directory, name)`, not the whole path.** Blocks do not
    /// interleave: every child of one directory arrives before any grandchild, and directories
    /// are visited in sorted depth-first order. So `a` (a directory holding `a/x`) and `a.txt`
    /// arrive as `a`, `a.txt`, …, `a/x` — both are the root's children, so both precede anything
    /// under `a`. Comparing whole paths one component at a time would put `a/x` second and
    /// silently misalign the merge, which reads as "present at the destination, absent at the
    /// source" and licenses a delete. Whole-path comparison agrees inside a single directory, so
    /// a small tree can pass while the rule is already broken.
    ///
    /// The order is over raw UTF-8 bytes and is **no server's collation**: a case-insensitive
    /// share sorts `README.md` next to `readme.md`, this does not, and such a destination cannot
    /// hold both at once. Children the listing itself could not describe have no name to sort by
    /// and stay ahead of the block, outside the order: the guarantee covers described children,
    /// and two backends that fail to name the same two children may still put those two failures
    /// in two different sequences. A merge that walks the stream literally has to skip them.
    ///
    /// **On S3 a name can repeat within one directory**: an object `a` and the prefix `a/` are
    /// both the child `a`, and a traversal of every version ([`TraversalVersions::All`]) emits one
    /// child per version of a key. Ties are ordered — the object before the directory (S3's own
    /// key order, `a` < `a/`), and one key's versions oldest first — so the stream is still a
    /// total order, and the full merge key is `(parent directory, name, kind, version position)`.
    NameBytes,
}

/// Which entries a traversal emits for the objects of a versioned store (ADR-0006 C22).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalVersions {
    /// Current objects only. A key whose latest entry is a delete marker is absent, as it is
    /// from the store's own current view. Every source supports it; entries carry no listed
    /// version ([`ObservedEntry::version`](crate::model::ObservedEntry::version) is `None`).
    #[default]
    Current,
    /// Every stored version and delete marker of every object, each object's entries contiguous,
    /// oldest first and the latest last. Each entry carries its [`EntryVersion`] (version id,
    /// latest flag, delete-marker flag), and
    /// [`ObservedEntry::source_version`](crate::model::ObservedEntry::source_version) is the
    /// selector that copies exactly that version with `TransferRequest::with_source_version`. A
    /// delete marker has no size and nothing to copy.
    ///
    /// Only a source that keeps versions (S3) supports it. Any other source ends the traversal
    /// before any I/O with an `Unsupported` session failure, never with its current entries in
    /// place of the versions asked for. Both modes hide `.data-mover-*` names.
    ///
    /// [`EntryVersion`]: crate::model::EntryVersion
    All,
}

/// Facts known about one directory child when the traversal decides whether to emit it,
/// descend into it, and keep filtering below it.
#[derive(Clone, Copy, Debug)]
pub struct TraversalCandidate<'a> {
    /// Path relative to the **traversal root**, slash-separated and without a leading slash.
    ///
    /// This is the frame of reference the legacy walkers used, so an expression written against
    /// a scan root keeps matching. It is deliberately not the backend-relative path that the
    /// emitted [`ObservedEntry`](crate::model::ObservedEntry) carries.
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

    /// Decides one candidate. May be called in any order, concurrently from several tasks (each
    /// listing decides its own children as it arrives), before the entry is admitted, and for
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
    /// Current objects only, or every stored version (see [`TraversalVersions`]).
    pub versions: TraversalVersions,
}

/// Re-expresses a backend-relative path in the traversal root's frame of reference.
///
/// A leading `./` is stripped, so a `path` expression written against the scan root matches
/// however the path was spelled.
///
/// The root only matches on a component boundary. A raw string prefix would rewrite a sibling
/// that merely starts with the root's spelling — `keeper/a` under root `keep` would become
/// `er/a` — which silently corrupts both the emitted path and anything matched against it.
///
/// A listing spells every child as its directory's path verbatim, a `/` and the child's name
/// (P7 C1), so a path below a root spelled `./x` or `x/` starts with that very spelling; it is
/// stripped as it is before any `./` normalisation is tried.
pub(crate) fn relative_to<'a>(root: &StoragePath, path: &'a StoragePath) -> &'a str {
    let root = root.as_str();
    let value = path.as_str().strip_prefix("./").unwrap_or(path.as_str());
    if root.is_empty() {
        return value;
    }
    below(root, path.as_str())
        .or_else(|| below(root, value))
        .unwrap_or(value)
}

/// What follows `root` in `value`, when `value` is `root` itself or lies below it on a component
/// boundary.
fn below<'a>(root: &str, value: &'a str) -> Option<&'a str> {
    let rest = value.strip_prefix(root)?;
    (root.ends_with('/') || rest.is_empty() || rest.starts_with('/'))
        .then(|| rest.trim_start_matches('/'))
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

    /// Children spelled from the root verbatim (P7 C1): `./`, `./x` and `x/` roots.
    #[test]
    fn a_root_spelled_verbatim_in_its_children_is_stripped_as_spelled() {
        assert_eq!(relative_to(&path("./"), &path(".//a/b")), "a/b");
        assert_eq!(relative_to(&path("./x"), &path("./x/a")), "a");
        assert_eq!(relative_to(&path("x/"), &path("x//a")), "a");
        assert_eq!(relative_to(&path("x/"), &path("x/a")), "a");
        assert_eq!(relative_to(&path("./x"), &path("./x")), "");
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

/// Positive evidence that enumeration reached its normal terminal boundary, with what it
/// counted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TraversalCompletion {
    pub totals: ScanTotals,
}

impl TraversalCompletion {
    /// Completion evidence carrying `totals`.
    #[must_use]
    pub const fn new(totals: ScanTotals) -> Self {
        Self { totals }
    }
}

/// Whether an item is a directory event or a checkpoint rather than an entry or a failure.
/// Tests about entry order skip these; the ones in `storage::tests::events` are about them.
#[cfg(test)]
pub(crate) const fn is_marker(item: &TraversalItem) -> bool {
    matches!(
        item,
        TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_)
    )
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
            let totals = ScanTotals {
                listed: 1,
                failed_children: 1,
                ..ScanTotals::default()
            };
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion::new(
                totals,
            ))));
        });
        assert!(matches!(
            session.next_item().await,
            Some(TraversalItem::Failure(failure)) if failure.scope == FailureScope::Child
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
        TraversalItem::Failure(Box::new(ScanFailure::child(
            crate::model::EntryOperationFailure::new(
                StoragePath::new("denied").unwrap_or_else(|error| panic!("{error}")),
                crate::model::Operation::Observe,
                crate::model::FailureClass::PermissionDenied,
                crate::model::Transience::Permanent,
                "denied",
            )
            .unwrap_or_else(|error| panic!("{error}")),
        )))
    }
}
