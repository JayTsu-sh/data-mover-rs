//! `TraversalOrder::NameBytes`: what it sorts, what it leaves alone, and what it must not change.

use super::events::events;
use super::*;
use crate::traversal::{ChildOrder, DirectoryListing, TraversalOrder};

/// A tree whose every listing arrives in the reverse of its byte order, so nothing passes by
/// accident, plus the three shapes the contract makes claims about: `a`, `a/x` and `a.txt`
/// together (all of the root's children precede anything under `a`, which is what makes the
/// comparison key `(parent, name)` rather than the whole path), an uppercase name (byte order is
/// not any server's case-insensitive collation), and a listing that also carries a failure.
struct ReversedNamespace;

#[async_trait]
impl Namespace for ReversedNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        Ok(match directory.as_str() {
            "" => NamespaceResult::Entries(vec![
                descriptor("part", EntryKind::Directory),
                descriptor("b", EntryKind::Directory),
                descriptor("a.txt", EntryKind::File),
                descriptor("a", EntryKind::Directory),
                descriptor("B", EntryKind::File),
            ]),
            "a" => NamespaceResult::Entries(vec![descriptor("a/x", EntryKind::File)]),
            "b" => NamespaceResult::Entries(vec![
                descriptor("b/z", EntryKind::File),
                descriptor("b/y", EntryKind::File),
            ]),
            "part" => NamespaceResult::Listing {
                entries: vec![
                    descriptor("part/q", EntryKind::File),
                    descriptor("part/p", EntryKind::File),
                ],
                failures: vec![entry_failure(&path("part/bad"), FailureClass::Protocol)],
            },
            other => panic!("unexpected listing of {other}"),
        })
    }
}

fn ordered_request(order: TraversalOrder) -> TraversalRequest {
    TraversalRequest {
        root: StoragePath::root(),
        order,
        max_inflight_operations: NonZeroUsize::new(2)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(1)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: ObservationPlan::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: None,
        max_depth: None,
    }
}

async fn reversed_tree(order: TraversalOrder) -> (Vec<String>, TraversalOutcome) {
    let source =
        StorageTraversalSource::with_roles(Arc::new(ReversedNamespace), Arc::new(FakeMetadata));
    events(source.traverse(ordered_request(order))).await
}

/// The control. Without the option nothing moves, so any later assertion that `NameBytes`
/// sorted something is really about the option and not about the fixture.
#[tokio::test]
async fn admission_leaves_the_backend_listing_order_alone() {
    let (rendered, _) = reversed_tree(TraversalOrder::Admission).await;
    assert_eq!(
        rendered,
        [
            "entry part",
            "entry b",
            "entry a.txt",
            "entry a",
            "entry B",
            "listed <root> Complete pruned=0 truncated=0",
            "failure part/bad",
            "entry part/q",
            "entry part/p",
            "listed part Partial { failures: 1 } pruned=0 truncated=0",
            "subtree part dirs=1 entries=2 partial=1 failures=1 exhaustive=false",
            "entry b/z",
            "entry b/y",
            "listed b Complete pruned=0 truncated=0",
            "subtree b dirs=1 entries=2 exhaustive=true",
            "entry a/x",
            "listed a Complete pruned=0 truncated=0",
            "subtree a dirs=1 entries=1 exhaustive=true",
            "subtree <root> dirs=4 entries=10 partial=1 failures=1 exhaustive=false",
        ]
    );
}

/// The whole contract in one sequence: siblings sorted, subdirectories descended into in that
/// same order, and blocks that never interleave.
///
/// `a`, `a/x` and `a.txt` are the case that fixes the comparison key. The sequence is `a`,
/// `a.txt`, …, `a/x`: both are the root's children, so both precede everything under `a`. It is
/// **not** the tree's depth-first preorder, which would be `a`, `a/x`, `a.txt` and would need
/// the traversal to descend mid-block. So a consumer compares `(parent, name)`; comparing whole
/// paths one component at a time would put `a/x` second and misalign a two-sided merge.
/// `B` before `a` is the other case: `0x42 < 0x61`, which no case-insensitive server would
/// agree with.
#[tokio::test]
async fn name_bytes_orders_both_the_block_and_the_descent() {
    let (rendered, outcome) = reversed_tree(TraversalOrder::NameBytes).await;
    assert_eq!(
        rendered,
        [
            "entry B",
            "entry a",
            "entry a.txt",
            "entry b",
            "entry part",
            "listed <root> Complete pruned=0 truncated=0",
            "entry a/x",
            "listed a Complete pruned=0 truncated=0",
            "subtree a dirs=1 entries=1 exhaustive=true",
            "entry b/y",
            "entry b/z",
            "listed b Complete pruned=0 truncated=0",
            "subtree b dirs=1 entries=2 exhaustive=true",
            "failure part/bad",
            "entry part/p",
            "entry part/q",
            "listed part Partial { failures: 1 } pruned=0 truncated=0",
            "subtree part dirs=1 entries=2 partial=1 failures=1 exhaustive=false",
            "subtree <root> dirs=4 entries=10 partial=1 failures=1 exhaustive=false",
        ]
    );
    assert!(
        matches!(
            outcome,
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 10,
                entry_failures: 1,
                directories_listed: 4,
                ..
            })
        ),
        "{outcome:?}"
    );
}

/// `b` and `part` are sorted too, not only the root: a sort that ran once on the first listing
/// would leave these two blocks reversed.
#[tokio::test]
async fn name_bytes_holds_below_the_root() {
    let (rendered, _) = reversed_tree(TraversalOrder::NameBytes).await;
    let block = |directory: &str| {
        rendered
            .iter()
            .skip_while(|line| !line.starts_with(&format!("entry {directory}/")))
            .take_while(|line| line.starts_with("entry "))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(block("b"), ["entry b/y", "entry b/z"]);
    assert_eq!(block("part"), ["entry part/p", "entry part/q"]);
}

/// `part` arrives as `NamespaceResult::Listing`, the variant that carries per-child failures
/// alongside the described children. Sorting only the plain `Entries` variant would leave this
/// block in listing order while every other block looked right.
#[tokio::test]
async fn name_bytes_sorts_listings_that_also_carry_failures() {
    let (rendered, _) = reversed_tree(TraversalOrder::NameBytes).await;
    let part = rendered
        .iter()
        .skip_while(|line| *line != "failure part/bad")
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    // The failure has no name to sort by, so it stays ahead of the block, outside the order.
    assert_eq!(part, ["failure part/bad", "entry part/p", "entry part/q"]);
}

/// Ordering moves items; it must not change what is counted. Every marker and every tally is
/// the same under both orders, so a summary that shifted with the order would be caught here
/// rather than by whoever trusted it at the destination.
#[tokio::test]
async fn name_bytes_changes_the_order_and_nothing_else() {
    let (admission, admission_outcome) = reversed_tree(TraversalOrder::Admission).await;
    let (sorted, sorted_outcome) = reversed_tree(TraversalOrder::NameBytes).await;
    assert_ne!(admission, sorted, "the fixture must not be pre-sorted");
    let multiset = |lines: &[String]| {
        let mut lines = lines.to_vec();
        lines.sort();
        lines
    };
    assert_eq!(multiset(&admission), multiset(&sorted));
    assert_eq!(
        format!("{admission_outcome:?}"),
        format!("{sorted_outcome:?}")
    );
}

/// The per-directory report, which is what a consumer reads to know the order actually held.
#[tokio::test]
async fn child_order_is_reported_on_every_listed_directory() {
    for (order, expected) in [
        (TraversalOrder::Admission, ChildOrder::Listing),
        (TraversalOrder::NameBytes, ChildOrder::NameBytes),
    ] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(ReversedNamespace), Arc::new(FakeMetadata));
        let mut session = source.traverse(ordered_request(order));
        let mut reported = 0;
        while let Some(item) = session.next_item().await {
            if let TraversalItem::DirectoryListed(listed) = item {
                assert_eq!(listed.child_order, expected, "{:?}", listed.path);
                reported += 1;
            }
        }
        assert_eq!(reported, 4);
    }
}

/// Hides one file wherever it appears, and can insist on `modified` so the traversal has to
/// defer the decision to each child's observation instead of making it when the listing lands.
#[derive(Debug)]
struct HideOneFile {
    needs_modified: bool,
}

impl crate::traversal::TraversalFilter for HideOneFile {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(&self, candidate: &crate::traversal::TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: candidate.name != "z",
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// The two filter paths reach the cursor differently: an immediate decision is made when the
/// listing lands, a deferred one only once the child's observation settles, and it travels as a
/// `Pending` slot. Both must still deliver the sorted order, because a consumer comparing two
/// sides has no way to tell which path either side took.
#[tokio::test]
async fn name_bytes_agrees_between_the_immediate_and_deferred_filter_paths() {
    let mut rendered = Vec::new();
    for needs_modified in [false, true] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(ReversedNamespace), Arc::new(FakeMetadata));
        let mut request = ordered_request(TraversalOrder::NameBytes);
        request.filter = Some(Arc::new(HideOneFile { needs_modified }));
        let (lines, _) = events(source.traverse(request)).await;
        rendered.push(lines);
    }
    assert_eq!(rendered[0], rendered[1]);
    // `b/z` is gone and only `b` reports `Filtered`; everything else keeps its sorted place.
    assert_eq!(
        rendered[0],
        [
            "entry B",
            "entry a",
            "entry a.txt",
            "entry b",
            "entry part",
            "listed <root> Complete pruned=0 truncated=0",
            "entry a/x",
            "listed a Complete pruned=0 truncated=0",
            "subtree a dirs=1 entries=1 exhaustive=true",
            "entry b/y",
            "listed b Filtered pruned=0 truncated=0",
            "subtree b dirs=1 entries=1 filtered=1 exhaustive=false",
            "failure part/bad",
            "entry part/p",
            "entry part/q",
            "listed part Partial { failures: 1 } pruned=0 truncated=0",
            "subtree part dirs=1 entries=2 partial=1 failures=1 exhaustive=false",
            "subtree <root> dirs=4 entries=9 filtered=1 partial=1 failures=1 exhaustive=false",
        ]
    );
}

/// Concurrency decides only when the cursor may take its next step, never which sequence a
/// result receives. Widening the window changes how many listings are in flight and in what
/// order they come back, and the ordered stream must not notice.
#[tokio::test]
async fn name_bytes_does_not_depend_on_how_much_runs_at_once() {
    let mut rendered = Vec::new();
    for window in [1, 2, 8, 64] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(ReversedNamespace), Arc::new(FakeMetadata));
        let mut request = ordered_request(TraversalOrder::NameBytes);
        request.max_inflight_operations =
            NonZeroUsize::new(window).unwrap_or_else(|| unreachable!("constants are nonzero"));
        let (lines, _) = events(source.traverse(request)).await;
        rendered.push(lines);
    }
    assert!(
        rendered.windows(2).all(|pair| pair[0] == pair[1]),
        "{rendered:#?}"
    );
}

/// A listing that fails outright still closes like any other directory, with an empty block.
struct FailingListNamespace;

#[async_trait]
impl Namespace for FailingListNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        match directory.as_str() {
            "" => Ok(NamespaceResult::Entries(vec![descriptor(
                "sub",
                EntryKind::Directory,
            )])),
            _ => Err(StorageRoleFailure::Entry(entry_failure(
                &directory,
                FailureClass::Protocol,
            ))),
        }
    }
}

/// A directory that could not be listed has an empty block, so there is nothing to put in order
/// — but it still reports the order that was asked for. Reporting the default instead would read
/// as "the request was not honored", and a consumer guarding its merge on `child_order` would
/// trip on every unreadable directory rather than on the one thing that matters, `listing`.
#[tokio::test]
async fn a_directory_that_could_not_be_listed_still_reports_the_requested_order() {
    let source =
        StorageTraversalSource::with_roles(Arc::new(FailingListNamespace), Arc::new(FakeMetadata));
    let mut session = source.traverse(ordered_request(TraversalOrder::NameBytes));
    let mut seen = Vec::new();
    while let Some(item) = session.next_item().await {
        if let TraversalItem::DirectoryListed(listed) = item {
            seen.push((
                listed.path.as_str().to_owned(),
                listed.listing,
                listed.child_order,
            ));
        }
    }
    assert_eq!(
        seen,
        [
            (
                String::new(),
                DirectoryListing::Complete,
                ChildOrder::NameBytes
            ),
            (
                "sub".to_owned(),
                DirectoryListing::Failed,
                ChildOrder::NameBytes
            ),
        ]
    );
}
