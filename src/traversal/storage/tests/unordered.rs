//! The Unordered traversal (P7 C10) against Blocks on the same trees: the same entries, the same
//! failures and the same totals, in an order that only keeps a directory's entry ahead of its
//! children and a directory's listing failure behind the children it delivered; no directory
//! events; termination under failure, cancellation and a consumer that walks away.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::events::render;
use super::filter::{PathFilter, TreeNamespace};
use super::pages::{MixedNamespace, TimesMetadata, mixed_request};
use super::streaming::{HideFiles, PagedTree, Pages, PanicsAt, denied, paged_source, pages};
use super::vanish::{Root, VanishingTree, source as vanishing_source};
use super::*;
use crate::model::BackendSessionFailure;
use crate::storage::PageSink;
use crate::storage::listing::child_path;
use crate::traversal::{
    FailureScope, TraversalCandidate, TraversalDecision, TraversalFilter, TraversalOrder,
};

/// What a whole traversal delivered: its items, and its outcome or terminal failure.
type Run = (
    Vec<TraversalItem>,
    Result<TraversalOutcome, TraversalTerminalFailure>,
);

async fn run(source: &StorageTraversalSource, request: TraversalRequest) -> Run {
    let mut session = source.traverse(request);
    let drained = tokio::time::timeout(Duration::from_secs(20), async {
        let mut items = Vec::new();
        while let Some(item) = session.next_item().await {
            items.push(item);
        }
        (items, session.finish().await)
    })
    .await;
    drained.unwrap_or_else(|_| panic!("the traversal did not end"))
}

/// Everything about an item but its position, for comparing two orders.
fn line(item: &TraversalItem) -> String {
    match item {
        TraversalItem::Entry(entry) => format!(
            "entry {} {:?} {:?} {:?}",
            entry.path(),
            entry.kind(),
            entry.size(),
            entry.modified()
        ),
        TraversalItem::Failure(failure) => format!(
            "failure {:?} {} {:?} {:?}",
            failure.scope,
            failure.path,
            failure.failure.class(),
            failure.delivered
        ),
        other => render(other),
    }
}

/// The directory `path` is in, or `None` at the root.
fn parent(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(parent, _)| parent)
}

/// What only Unordered promises about positions: no directory events; a directory's entry, when
/// it has one, ahead of every item below it; a directory's listing failure behind every child
/// it delivered.
fn assert_unordered_shape(items: &[TraversalItem]) {
    let mut seen_directories = HashMap::new();
    for (index, item) in items.iter().enumerate() {
        match item {
            TraversalItem::DirectoryListed(_) => panic!("Unordered delivered a directory event"),
            TraversalItem::Entry(entry) if entry.kind() == EntryKind::Directory => {
                seen_directories.insert(entry.path().as_str().to_owned(), index);
            }
            _ => {}
        }
    }
    let position = |path: &str| seen_directories.get(path).copied();
    for (index, item) in items.iter().enumerate() {
        let (path, below_listing) = match item {
            TraversalItem::Entry(entry) => (entry.path().as_str(), None),
            TraversalItem::Failure(failure) if failure.scope == FailureScope::Listing => {
                (failure.path.as_str(), Some(failure.path.as_str()))
            }
            TraversalItem::Failure(failure) => (failure.path.as_str(), None),
            _ => continue,
        };
        let mut ancestor = if below_listing.is_some() {
            Some(path)
        } else {
            parent(path)
        };
        while let Some(directory) = ancestor {
            if let Some(at) = position(directory) {
                assert!(at < index, "{directory} delivered after {path}");
            }
            ancestor = parent(directory);
        }
        if let Some(directory) = below_listing {
            let prefix = format!("{directory}/");
            let late = items[index..].iter().any(|later| {
                matches!(later, TraversalItem::Entry(entry)
                    if entry.path().as_str().starts_with(&prefix)
                        && parent(entry.path().as_str()) == Some(directory))
            });
            assert!(!late, "a child of {directory} after its listing failure");
        }
    }
}

/// Runs `request` in both orders over sources `make` builds, and asserts Unordered delivered
/// exactly what Blocks did, apart from order and directory events, with the same outcome.
async fn same_as_blocks(
    make: impl Fn() -> StorageTraversalSource,
    request: &TraversalRequest,
) -> Run {
    let blocks = run(&make(), request.clone().order(TraversalOrder::Blocks)).await;
    let unordered = run(&make(), request.clone().order(TraversalOrder::Unordered)).await;
    assert_unordered_shape(&unordered.0);
    let sorted = |items: &[TraversalItem]| {
        let mut lines: Vec<String> = items
            .iter()
            .filter(|item| !crate::traversal::is_marker(item))
            .map(line)
            .collect();
        lines.sort();
        lines
    };
    match (&blocks.1, &unordered.1) {
        (Ok(expected), Ok(got)) => {
            assert_eq!(sorted(&unordered.0), sorted(&blocks.0));
            assert_eq!(got, expected);
            if let Ok(TraversalOutcome::Completed(completion)) = &unordered.1 {
                assert!(completion.totals.is_balanced(), "{completion:?}");
            }
        }
        (Err(expected), Err(got)) => assert_eq!(got, expected),
        (expected, got) => panic!("Blocks ended {expected:?}, Unordered {got:?}"),
    }
    unordered
}

fn window(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| unreachable!("tests pass nonzero"))
}

/// Children observed in the listing and through the metadata role, symlinks read, a filter that
/// needs `modified` deferring decisions — at every admission window and buffer size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unordered_delivers_what_blocks_does_on_a_mixed_tree() {
    for inline in [true, false] {
        for filtered in [false, true] {
            for (inflight, buffered) in [(1, 1), (3, 2), (64, 1024)] {
                let request = mixed_request(TraversalOrder::Unordered, inflight, filtered)
                    .max_buffered_items(window(buffered));
                let make = || {
                    StorageTraversalSource::with_roles(
                        Arc::new(MixedNamespace {
                            inline,
                            holdout: true,
                        }),
                        Arc::new(TimesMetadata::default()),
                    )
                };
                let (items, _) = same_as_blocks(make, &request).await;
                assert!(items.len() > 100, "{}", items.len());
            }
        }
    }
}

/// A filter that hides, prunes and stops filtering, decided at once or once observed, under
/// every depth limit.
#[tokio::test]
async fn filters_and_depth_limits_decide_the_same_subtrees() {
    for needs_modified in [false, true] {
        for max_depth in [None, Some(1), Some(2)] {
            let mut request = request(tokio_util::sync::CancellationToken::new())
                .filter(Arc::new(PathFilter { needs_modified }));
            request.max_depth = max_depth.and_then(NonZeroUsize::new);
            let make = || {
                StorageTraversalSource::with_roles(
                    Arc::new(TreeNamespace::new()),
                    Arc::new(super::filter::CountingMetadata::new()),
                )
            };
            let _ = same_as_blocks(make, &request).await;
        }
    }
}

/// Vanished children and listings, throttled listings and observations, a missing root, `NotFound`
/// about something else: counted and reported exactly as Blocks does.
#[tokio::test]
async fn the_error_model_is_the_same_as_blocks() {
    for root in [
        Root::Plain,
        Root::Throttle,
        Root::GoneDirectory,
        Root::StillThere,
        Root::UnnamedNotFound,
        Root::ThrottledConfirmation,
    ] {
        let request = request(tokio_util::sync::CancellationToken::new());
        let make = || {
            vanishing_source(Arc::new(VanishingTree {
                root,
                ..VanishingTree::default()
            }))
        };
        let _ = same_as_blocks(make, &request).await;
    }
    let mut missing = request(tokio_util::sync::CancellationToken::new());
    missing.root = path("nowhere");
    let (items, outcome) = same_as_blocks(
        || vanishing_source(Arc::new(VanishingTree::default())),
        &missing,
    )
    .await;
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(completion))
        if completion.totals.listing_failures == 1)
    );
    assert!(items.iter().any(|item| matches!(item,
        TraversalItem::Failure(failure) if failure.scope == FailureScope::Listing)));
}

fn gone(at: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(&path(at), FailureClass::NotFound))
}

/// Listings that stop part way: the delivered children, then the failure with how many; a
/// subdirectory gone before its first page is a vanished listing; one gone after it an
/// interruption.
#[tokio::test]
async fn listings_that_stop_part_way_are_interrupted_after_what_they_delivered() {
    let make = || {
        paged_source(PagedTree::new(vec![
            (
                "",
                Pages {
                    pages: vec![
                        vec![
                            ("f0", EntryKind::File),
                            ("sub", EntryKind::Directory),
                            ("early", EntryKind::Directory),
                        ],
                        vec![("late", EntryKind::Directory), ("f1", EntryKind::File)],
                    ],
                    then: Some(denied("")),
                },
            ),
            ("sub", pages(vec![vec![("x", EntryKind::File)], vec![]])),
            (
                "early",
                Pages {
                    pages: Vec::new(),
                    then: Some(gone("early")),
                },
            ),
            (
                "late",
                Pages {
                    pages: vec![vec![("y", EntryKind::File)]],
                    then: Some(gone("late")),
                },
            ),
        ]))
        .0
    };
    let request = super::streaming::paged_request(4);
    let (items, outcome) = same_as_blocks(make, &request).await;
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some((failure.path.as_str(), failure.delivered)),
            _ => None,
        })
        .collect();
    assert_eq!(failures.len(), 2, "{failures:?}");
    assert!(failures.contains(&("", Some(5))));
    assert!(failures.contains(&("late", Some(1))));
    let Ok(TraversalOutcome::Completed(completion)) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(completion.totals.vanished_listings, 1);
    assert_eq!(completion.totals.interrupted_listings, 2);
    assert_eq!(completion.totals.listing_failures, 2);
}

/// Every descended directory's listing carries the hint its parent's listing gave it, on the
/// immediate and on the deferred path; the root's carries none.
#[tokio::test]
async fn the_descent_hint_reaches_the_listing_of_the_child() {
    let tree = || {
        PagedTree::new(vec![
            (
                "",
                pages(vec![vec![
                    ("a", EntryKind::Directory),
                    ("b", EntryKind::Directory),
                ]]),
            ),
            ("a", pages(vec![vec![("c", EntryKind::Directory)]])),
        ])
    };
    for needs_modified in [false, true] {
        let (source, recorded) = paged_source(tree());
        let mut request = super::streaming::paged_request(4).order(TraversalOrder::Unordered);
        if needs_modified {
            // Every decision waits for the child's observation: the deferred path.
            request = request.filter(Arc::new(HideFiles {
                needs_modified: true,
            }));
        }
        let (_, outcome) = run(&source, request).await;
        assert!(outcome.is_ok());
        let mut requests = recorded.requests();
        requests.sort();
        let hint = |name: &str| Some(format!("hint:{name}").into_bytes());
        assert_eq!(
            requests,
            [
                (String::new(), None),
                ("a".to_owned(), hint("a")),
                ("a/c".to_owned(), hint("c")),
                ("b".to_owned(), hint("b")),
            ]
        );
    }
}

/// A lost session ends the traversal with that failure; a filter that panics ends it `Internal`.
/// Neither leaves it waiting.
#[tokio::test]
async fn terminal_failures_end_the_traversal_without_a_hang() {
    let session_failure = StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Connectivity,
            Transience::Transient,
            "the connection was reset",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    );
    let (source, _) = paged_source(PagedTree::new(vec![
        (
            "",
            pages(vec![vec![
                ("a", EntryKind::Directory),
                ("f", EntryKind::File),
            ]]),
        ),
        (
            "a",
            Pages {
                pages: vec![vec![("x", EntryKind::File)]],
                then: Some(session_failure),
            },
        ),
    ]));
    let request = super::streaming::paged_request(4).order(TraversalOrder::Unordered);
    let (_, outcome) = run(&source, request.clone()).await;
    assert!(
        matches!(outcome, Err(TraversalTerminalFailure::Session(failure))
        if failure.class() == FailureClass::Connectivity)
    );

    let (source, _) = paged_source(PagedTree::new(vec![(
        "",
        pages(vec![
            vec![("a", EntryKind::File)],
            vec![("boom", EntryKind::File)],
        ]),
    )]));
    let (_, outcome) = run(&source, request.filter(Arc::new(PanicsAt("boom")))).await;
    assert_eq!(outcome, Err(TraversalTerminalFailure::Internal));

    let source = StorageTraversalSource::with_roles(
        Arc::new(FakeNamespace),
        Arc::new(FailingMetadata(MetadataFailureMode::Session)),
    );
    let mut traversal = super::request(tokio_util::sync::CancellationToken::new());
    traversal.order = TraversalOrder::Unordered;
    let (_, outcome) = run(&source, traversal).await;
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
}

/// Cancelled while every observation hangs, or while the consumer takes nothing: `Cancelled`,
/// promptly, and never a listing failure for a listing the cancellation stopped (R10).
#[tokio::test]
async fn cancellation_ends_the_traversal_as_cancelled() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let source =
        StorageTraversalSource::with_roles(Arc::new(FakeNamespace), Arc::new(BlockingMetadata));
    let mut traversal = super::request(cancel.clone());
    traversal.order = TraversalOrder::Unordered;
    let mut session = source.traverse(traversal);
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    assert!(session.next_item().await.is_none());
    let ended = tokio::time::timeout(Duration::from_secs(2), session.finish()).await;
    assert_eq!(
        ended.unwrap_or_else(|_| panic!("the cancelled traversal did not end")),
        Ok(TraversalOutcome::Cancelled)
    );

    let cancel = tokio_util::sync::CancellationToken::new();
    let source = synthetic(4, 4, 6, false);
    let request = TraversalRequest::new(StoragePath::root())
        .max_inflight_operations(window(8))
        .max_buffered_items(window(1))
        .cancel(cancel.clone());
    let mut session = source.0.traverse(request);
    let mut items = Vec::new();
    for _ in 0..5 {
        items.extend(session.next_item().await);
    }
    // Let the workers fill the channel and stop on it.
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let ended = tokio::time::timeout(Duration::from_secs(2), session.finish()).await;
    assert_eq!(
        ended.unwrap_or_else(|_| panic!("the cancelled traversal did not end")),
        Ok(TraversalOutcome::Cancelled)
    );
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Failure(_))),
        "a cancelled listing was reported as failed"
    );
    // Every listing the cancellation stopped has dropped its stream.
    tokio::time::timeout(Duration::from_secs(2), async {
        while source.1.open.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a listing stayed open after the traversal was cancelled"));
}

/// A consumer that drops the session part way: every worker stops and every listing is dropped.
#[tokio::test]
async fn a_consumer_that_walks_away_stops_every_listing() {
    let (source, counts) = synthetic(4, 4, 6, false);
    let request = TraversalRequest::new(StoragePath::root())
        .max_inflight_operations(window(16))
        .max_buffered_items(window(2));
    let mut session = source.traverse(request);
    for _ in 0..10 {
        let _ = session.next_item().await;
    }
    drop(session);
    tokio::time::timeout(Duration::from_secs(2), async {
        while counts.open.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a listing stayed open after the consumer left"));
    assert!(
        counts.opened.load(Ordering::SeqCst) < 341,
        "the traversal kept going"
    );
}

/// Hides every entry and still descends everywhere: a traversal with nothing to hand over.
#[derive(Debug)]
struct HideAll;

impl TraversalFilter for HideAll {
    fn needs_modified(&self) -> bool {
        false
    }

    fn decide(&self, _candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: false,
            descend: true,
            filter_children: true,
        }
    }
}

/// A consumer that drops the session while there is nothing to hand it: the workers notice at
/// their next hand-over, empty as it is, and stop listing rather than walk the rest of the tree.
///
/// Fault injection: let an empty hand-over skip the check and all 1365 directories are listed.
#[tokio::test]
async fn a_consumer_that_walks_away_is_noticed_with_nothing_to_send() {
    let (source, counts) = synthetic(4, 5, 6, true);
    let request = TraversalRequest::new(StoragePath::root())
        .max_inflight_operations(window(4))
        .max_buffered_items(window(2))
        .filter(Arc::new(HideAll));
    drop(source.traverse(request));
    tokio::time::timeout(Duration::from_secs(2), async {
        while counts.open.load(Ordering::SeqCst) > 0 || counts.opened.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a listing stayed open after the consumer left"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let opened = counts.opened.load(Ordering::SeqCst);
    assert!(
        opened < 100,
        "the traversal walked on: {opened} directories listed"
    );
}

/// What a synthetic tree's listings did.
#[derive(Default)]
struct Counts {
    /// Listings started.
    opened: AtomicUsize,
    /// Listings whose stream is still held.
    open: AtomicUsize,
    /// The most held at once.
    peak: AtomicUsize,
}

/// A uniform tree: every directory above `depth` holds `fanout` subdirectories `d<i>` and `files`
/// files `f<i>`, listed in pages of three, each page after a pseudo-random pause when `jitter`, so
/// workers finish in every order.
struct Synthetic {
    fanout: usize,
    depth: usize,
    files: usize,
    jitter: bool,
    counts: Arc<Counts>,
}

/// Counts a listing's stream while it is held.
struct Held(Arc<Counts>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Namespace for Synthetic {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let level = if directory.as_str().is_empty() {
            0
        } else {
            directory.as_str().matches('/').count() + 1
        };
        let mut children = Vec::new();
        if level < self.depth {
            children
                .extend((0..self.fanout).map(|index| (format!("d{index}"), EntryKind::Directory)));
            children.extend((0..self.files).map(|index| (format!("f{index}"), EntryKind::File)));
        }
        self.counts.opened.fetch_add(1, Ordering::SeqCst);
        let open = self.counts.open.fetch_add(1, Ordering::SeqCst) + 1;
        self.counts.peak.fetch_max(open, Ordering::SeqCst);
        let held = Held(Arc::clone(&self.counts));
        let jitter = self.jitter;
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            let _held = held;
            let seed = directory.as_str().bytes().fold(7_usize, |sum, byte| {
                sum.wrapping_mul(31).wrapping_add(usize::from(byte))
            });
            for (index, chunk) in children.chunks(3).enumerate() {
                if jitter {
                    for _ in 0..(seed + index) % 5 {
                        tokio::task::yield_now().await;
                    }
                }
                let described = chunk
                    .iter()
                    .map(|(name, kind)| {
                        let child =
                            child_path(&directory, name).unwrap_or_else(|error| panic!("{error}"));
                        descriptor(child.as_str(), *kind)
                    })
                    .collect();
                let page = ListingPage::from_descriptors(&directory, described, Vec::new(), 0);
                if sink.send(page).await.is_err() {
                    return;
                }
            }
        })
    }
}

fn synthetic(
    fanout: usize,
    depth: usize,
    files: usize,
    jitter: bool,
) -> (StorageTraversalSource, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let source = StorageTraversalSource::with_roles(
        Arc::new(Synthetic {
            fanout,
            depth,
            files,
            jitter,
            counts: Arc::clone(&counts),
        }),
        Arc::new(FakeMetadata),
    );
    (source, counts)
}

/// Directories in a uniform tree of `fanout` and `depth`, and entries below its root.
const fn tree_size(fanout: usize, depth: usize, files: usize) -> (usize, usize) {
    let mut directories = 1;
    let mut level = 1;
    let mut width = 1;
    while level <= depth {
        width *= fanout;
        directories += width;
        level += 1;
    }
    // Every directory above the last level lists `files` files.
    let listing = directories - width;
    (directories, directories - 1 + listing * files)
}

/// Many workers, shapes from a chain to a wide fan, every order of completion: every entry once,
/// the totals exact, the traversal ending exactly when the last directory is done — never early
/// (a lost subtree) and never not at all (a hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_shape_completes_exactly_under_many_workers() {
    for (round, (fanout, depth, files, workers, buffered)) in [
        (1, 40, 1, 64, 4),
        (8, 3, 0, 64, 1),
        (3, 5, 2, 16, 1024),
        (5, 3, 7, 3, 2),
        (2, 7, 0, 64, 64),
    ]
    .into_iter()
    .enumerate()
    {
        for repeat in 0..10 {
            let (source, counts) = synthetic(fanout, depth, files, true);
            let request = TraversalRequest::new(StoragePath::root())
                .max_inflight_operations(window(workers))
                .max_buffered_items(window(buffered));
            let (items, outcome) = run(&source, request).await;
            let (directories, entries) = tree_size(fanout, depth, files);
            let context = format!("shape {round} repeat {repeat}");
            assert_eq!(items.len(), entries, "{context}");
            assert_unordered_shape(&items);
            let Ok(TraversalOutcome::Completed(completion)) = outcome else {
                panic!("{context}: {outcome:?}")
            };
            let totals = completion.totals;
            assert!(totals.is_balanced(), "{context}");
            assert_eq!(totals.emitted, entries as u64, "{context}");
            assert_eq!(totals.listed, entries as u64, "{context}");
            assert_eq!(
                counts.opened.load(Ordering::SeqCst),
                directories,
                "{context}"
            );
            assert_eq!(counts.open.load(Ordering::SeqCst), 0, "{context}");
            assert!(
                counts.peak.load(Ordering::SeqCst) <= workers.min(64),
                "{context}"
            );
        }
    }
}
