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
mod cifs_tests;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod local_tests;
#[cfg(test)]
mod nfs_tests;
#[cfg(test)]
mod s3_directory_object_tests;
#[cfg(test)]
mod s3_tests;

/// How a traversal orders what it delivers (P7 §3). Concurrent completion never changes it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalOrder {
    /// Depth-first by *blocks*. A block is every direct child of one listed directory, delivered
    /// contiguously **in the order the backend listed them** — not sorted — followed by the
    /// directory's [`TraversalItem::DirectoryListed`] (an empty directory has one too). Once a
    /// block is closed, the directory's subdirectories are descended into in the **raw UTF-8 byte
    /// order of their names**, each contributing its own block, recursively.
    ///
    /// What this makes deterministic is the order *directories* are visited in: the same on
    /// every backend, so two traversals of the same tree on two backends advance directory by
    /// directory in step, and how far one can drift ahead of the other is bounded. Within a block
    /// the order is the backend's own and may differ between backends and between runs; a
    /// consumer comparing two sides matches one directory's children by name (a hash map of one
    /// block), and concludes that a child is missing on one side only once **both** sides have
    /// delivered that directory's `DirectoryListed`.
    ///
    /// Blocks never interleave: every child of a directory precedes every grandchild. So `a` (a
    /// directory holding `a/x`) and `a.txt` both arrive before `a/x`; the order is not a
    /// depth-first preorder over paths, and a consumer keys by `(parent directory, name)`, never
    /// by comparing whole paths.
    ///
    /// The descent order is over raw UTF-8 bytes and is **no server's collation**: a
    /// case-insensitive share would sort `README` next to `readme`, this does not (`B` < `a`).
    /// On S3 a name can repeat within one directory — an object `a` and the prefix `a/` are both
    /// the child `a` — which only the prefix is descended into.
    ///
    /// A block is read a page at a time: its first entries are delivered once its first page is
    /// listed, and what the traversal holds of one directory is a few pages, however large the
    /// directory. Only the subdirectories still to descend into are remembered whole (one slot
    /// each), because the descent is sorted once the listing has ended.
    #[default]
    Blocks,
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

/// A bounded traversal request, built with [`TraversalRequest::new`] and its setters:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use data_mover::model::StoragePath;
/// # use data_mover::traversal::{TraversalOrder, TraversalRequest};
/// let request = TraversalRequest::new(StoragePath::root())
///     .order(TraversalOrder::Blocks)
///     .max_depth(NonZeroUsize::new(3))
///     .max_inflight_operations(NonZeroUsize::MIN.saturating_add(63));
/// assert_eq!(request.max_inflight_operations.get(), 64);
/// ```
///
/// The fields stay readable; new ones arrive with defaults, so a request is never spelled as a
/// struct literal outside this crate (`non_exhaustive`).
#[derive(Clone, Debug)]
#[non_exhaustive]
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

/// Operations in flight when the caller does not choose.
const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(63);
/// Items buffered ahead of the consumer when the caller does not choose.
const DEFAULT_MAX_BUFFERED: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1023);

impl TraversalRequest {
    /// A traversal of `root` with every other setting at its default: [`TraversalOrder`]'s
    /// default order, 64 operations in flight, 1024 items buffered, the default observation plan,
    /// a fresh cancellation token, no filter, no depth limit, current objects only.
    #[must_use]
    pub fn new(root: StoragePath) -> Self {
        Self {
            root,
            order: TraversalOrder::default(),
            max_inflight_operations: DEFAULT_MAX_INFLIGHT,
            max_buffered_items: DEFAULT_MAX_BUFFERED,
            observation_plan: ObservationPlan::default(),
            cancel: CancellationToken::new(),
            filter: None,
            max_depth: None,
            versions: TraversalVersions::default(),
        }
    }

    /// The order entries are delivered in.
    #[must_use]
    pub const fn order(mut self, order: TraversalOrder) -> Self {
        self.order = order;
        self
    }

    /// Operations (observations, listings) in flight at once, and entries admitted but not yet
    /// delivered.
    #[must_use]
    pub const fn max_inflight_operations(mut self, limit: NonZeroUsize) -> Self {
        self.max_inflight_operations = limit;
        self
    }

    /// Items the session buffers ahead of its consumer.
    #[must_use]
    pub const fn max_buffered_items(mut self, limit: NonZeroUsize) -> Self {
        self.max_buffered_items = limit;
        self
    }

    /// What is observed of each entry beyond what its listing carried.
    #[must_use]
    pub const fn observation_plan(mut self, plan: ObservationPlan) -> Self {
        self.observation_plan = plan;
        self
    }

    /// The token that cancels the traversal.
    #[must_use]
    pub fn cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// The admission and pruning policy; see [`TraversalFilter`].
    #[must_use]
    pub fn filter(mut self, filter: Arc<dyn TraversalFilter>) -> Self {
        self.filter = Some(filter);
        self
    }

    /// The deepest level listed (the root's children are depth 1); `None` is unlimited.
    #[must_use]
    pub const fn max_depth(mut self, max_depth: Option<NonZeroUsize>) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Current objects only, or every stored version.
    #[must_use]
    pub const fn versions(mut self, versions: TraversalVersions) -> Self {
        self.versions = versions;
        self
    }

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
///
/// Throttling never ends a traversal (P7 §8): a listing that stays throttled past its backoff
/// budget is that listing's failure, and a throttled observation is that child's.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
