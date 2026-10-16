//! `TraversalOrder::Blocks`: a block in listing order, the descent in name byte order, and
//! nothing that depends on how the backend happened to list.

use super::events::events;
use super::*;
use crate::traversal::{ListingOutcome, TraversalOrder};

/// A tree whose listings arrive in reverse byte order (or, `forward`, in byte order), so neither
/// a sorted block nor an unsorted descent passes by accident. It has the shapes the contract
/// makes claims about: `a`, `a/x` and `a.txt` together (all of the root's children precede
/// anything under `a`), an uppercase directory `B` (byte order is not a case-insensitive
/// collation: `B` < `a`), subdirectories below the root (`b/z`, `b/y`) whose descent is sorted
/// too, and a listing that also carries a failure.
struct ReversedNamespace {
    forward: bool,
}

#[async_trait]
impl Namespace for ReversedNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let (mut children, failures) = match directory.as_str() {
            "" => (
                vec![
                    descriptor("part", EntryKind::Directory),
                    descriptor("b", EntryKind::Directory),
                    descriptor("a.txt", EntryKind::File),
                    descriptor("a", EntryKind::Directory),
                    descriptor("B", EntryKind::Directory),
                ],
                Vec::new(),
            ),
            "a" => (vec![descriptor("a/x", EntryKind::File)], Vec::new()),
            "b" => (
                vec![
                    descriptor("b/z", EntryKind::Directory),
                    descriptor("b/y", EntryKind::Directory),
                ],
                Vec::new(),
            ),
            "part" => (
                vec![
                    descriptor("part/q", EntryKind::File),
                    descriptor("part/p", EntryKind::File),
                ],
                vec![entry_failure(&path("part/bad"), FailureClass::Protocol)],
            ),
            "B" | "b/y" | "b/z" => (Vec::new(), Vec::new()),
            other => panic!("unexpected listing of {other}"),
        };
        if self.forward {
            children.reverse();
        }
        answer(&directory, Ok((children, failures)))
    }
}

fn ordered_request() -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(
            NonZeroUsize::new(2).unwrap_or_else(|| unreachable!("constant is nonzero")),
        )
        .max_buffered_items(
            NonZeroUsize::new(1).unwrap_or_else(|| unreachable!("constant is nonzero")),
        )
        .cancel(tokio_util::sync::CancellationToken::new())
}

async fn tree(forward: bool, request: TraversalRequest) -> (Vec<String>, TraversalOutcome) {
    let source = StorageTraversalSource::with_roles(
        Arc::new(ReversedNamespace { forward }),
        Arc::new(FakeMetadata),
    );
    events(source.traverse(request)).await
}

/// The whole contract in one sequence: each block in the order the backend listed it (reversed
/// here, so a sorted block fails), the descent in name byte order (`B`, `a`, `b`, `part` —
/// `0x42 < 0x61`, which no case-insensitive server agrees with), below the root too (`b/y`
/// before `b/z`), and blocks that never interleave: `a` and `a.txt` are both the root's children,
/// so both precede everything under `a`.
#[tokio::test]
async fn blocks_keep_the_listing_order_and_sort_only_the_descent() {
    let (rendered, outcome) = tree(false, ordered_request()).await;
    assert_eq!(
        rendered,
        [
            "entry part",
            "entry b",
            "entry a.txt",
            "entry a",
            "entry B",
            "listed <root> Complete listed=5 emitted=5",
            "listed B Complete listed=0 emitted=0",
            "entry a/x",
            "listed a Complete listed=1 emitted=1",
            "entry b/z",
            "entry b/y",
            "listed b Complete listed=2 emitted=2",
            "listed b/y Complete listed=0 emitted=0",
            "listed b/z Complete listed=0 emitted=0",
            "failure child part/bad",
            "entry part/q",
            "entry part/p",
            "listed part Complete listed=3 emitted=2 failed=1",
        ]
    );
    assert!(
        matches!(
            outcome,
            TraversalOutcome::Completed(TraversalCompletion {
                totals: crate::traversal::ScanTotals {
                    listed: 11,
                    emitted: 10,
                    failed_children: 1,
                    ..
                },
                ..
            })
        ),
        "{outcome:?}"
    );
}

/// Only the listing order moves anything, and only inside a block. Reversing every listing
/// leaves the sequence of directories — the `DirectoryListed` events — exactly as it was, and
/// every block holds the same items; nothing is counted differently. This is what two backends
/// that list the same tree in different orders can rely on.
#[tokio::test]
async fn the_listing_order_moves_items_only_inside_their_block() {
    let (reversed, reversed_outcome) = tree(false, ordered_request()).await;
    let (forward, forward_outcome) = tree(true, ordered_request()).await;
    assert_ne!(reversed, forward, "the fixture must list differently");
    let directories = |lines: &[String]| {
        lines
            .iter()
            .filter(|line| line.starts_with("listed "))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(directories(&reversed), directories(&forward));
    let blocks = |lines: &[String]| {
        let mut blocks = Vec::new();
        let mut block: Vec<String> = Vec::new();
        for line in lines {
            if line.starts_with("listed ") {
                block.sort();
                blocks.push((line.clone(), std::mem::take(&mut block)));
            } else {
                block.push(line.clone());
            }
        }
        blocks
    };
    assert_eq!(blocks(&reversed), blocks(&forward));
    assert_eq!(
        format!("{reversed_outcome:?}"),
        format!("{forward_outcome:?}")
    );
}

/// Hides one file wherever it appears, and can insist on `modified` so the traversal has to
/// defer the decision to each child's observation instead of making it when the page lands.
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
            emit: candidate.name != "q",
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// The two filter paths reach the cursor differently: an immediate decision places a `Descend`
/// slot when the page lands, a deferred one a `Pending` slot that is filled once the child's
/// observation settles. The descent is sorted over both kinds alike, so the two agree, and a
/// consumer comparing two sides has no way to tell which path either side took.
#[tokio::test]
async fn blocks_agree_between_the_immediate_and_deferred_filter_paths() {
    let mut rendered = Vec::new();
    for needs_modified in [false, true] {
        let request = ordered_request().filter(Arc::new(HideOneFile { needs_modified }));
        let (lines, _) = tree(false, request).await;
        rendered.push(lines);
    }
    assert_eq!(rendered[0], rendered[1]);
    assert_eq!(
        rendered[0]
            .iter()
            .filter(|line| line.starts_with("listed "))
            .map(|line| line.split(' ').nth(1).unwrap_or_default())
            .collect::<Vec<_>>(),
        ["<root>", "B", "a", "b", "b/y", "b/z", "part"]
    );
    assert!(
        rendered[0]
            .iter()
            .any(|line| line == "listed part Complete listed=3 emitted=1 filtered=1 failed=1"),
        "{:#?}",
        rendered[0]
    );
}

/// Concurrency decides only when the cursor may take its next step, never which sequence a
/// result receives. Widening the window changes how many listings are in flight and in what
/// order they come back, and the stream must not notice.
#[tokio::test]
async fn blocks_do_not_depend_on_how_much_runs_at_once() {
    let mut rendered = Vec::new();
    for window in [1, 2, 8, 64] {
        let request = ordered_request().max_inflight_operations(
            NonZeroUsize::new(window).unwrap_or_else(|| unreachable!("constants are nonzero")),
        );
        let (lines, _) = tree(false, request).await;
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
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        if directory.as_str().is_empty() {
            entries(&directory, vec![descriptor("sub", EntryKind::Directory)])
        } else {
            let failure = entry_failure(&directory, FailureClass::Protocol);
            answer(&directory, Err(StorageRoleFailure::Entry(failure)))
        }
    }
}

/// A directory that could not be listed has an empty block and nothing to descend into; it is
/// unreadable, whatever the order.
#[tokio::test]
async fn a_directory_that_could_not_be_listed_is_unreadable() {
    let source =
        StorageTraversalSource::with_roles(Arc::new(FailingListNamespace), Arc::new(FakeMetadata));
    let mut session = source.traverse(ordered_request());
    let mut seen = Vec::new();
    while let Some(item) = session.next_item().await {
        if let TraversalItem::DirectoryListed(listed) = item {
            seen.push((listed.path.as_str().to_owned(), listed.outcome));
        }
    }
    assert_eq!(
        seen,
        [
            (String::new(), ListingOutcome::Complete),
            ("sub".to_owned(), ListingOutcome::Unreadable),
        ]
    );
}
