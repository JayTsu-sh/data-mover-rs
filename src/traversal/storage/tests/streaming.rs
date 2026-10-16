//! Listings consumed page by page (P7 C4): a block is admitted as its pages arrive, closes only
//! once its listing has ended, and holds a few pages however wide the directory; a listing that
//! stops part way is `Interrupted` with what it returned; the descent hint travels from a child's
//! listing to its own.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::Semaphore;

use super::events::{drain, events, render};
use super::filter::{CountingMetadata, TreeNamespace};
use super::*;
use crate::model::BackendSessionFailure;
use crate::storage::listing::child_path;
use crate::storage::{CollectLimits, DescentHint, ListedChild, PageSink, collect_listing};
use crate::traversal::{FailureScope, TraversalCandidate, TraversalDecision, TraversalFilter};

/// One directory's listing: its pages of `(name, kind)`, then how it ends.
#[derive(Clone)]
pub(super) struct Pages {
    pub(super) pages: Vec<Vec<(&'static str, EntryKind)>>,
    /// `None` ends the listing normally; otherwise it fails with this after the pages.
    pub(super) then: Option<StorageRoleFailure>,
}

pub(super) fn pages(pages: Vec<Vec<(&'static str, EntryKind)>>) -> Pages {
    Pages { pages, then: None }
}

/// A tree whose listings arrive in explicit pages. With a gate, every page after a listing's
/// first waits for one permit, so a test decides when the rest of a directory arrives. Every
/// directory child is handed back with a descent hint (`hint:` and its path), and every request
/// is recorded with the hint it carried.
pub(super) struct PagedTree {
    listings: HashMap<&'static str, Pages>,
    gate: Option<Arc<Semaphore>>,
    requests: Mutex<Vec<(String, Option<Vec<u8>>)>>,
}

impl PagedTree {
    pub(super) fn new(listings: Vec<(&'static str, Pages)>) -> Self {
        Self {
            listings: listings.into_iter().collect(),
            gate: None,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn gated(mut self) -> (Self, Arc<Semaphore>) {
        let gate = Arc::new(Semaphore::new(0));
        self.gate = Some(Arc::clone(&gate));
        (self, gate)
    }

    pub(super) fn requests(&self) -> Vec<(String, Option<Vec<u8>>)> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn hinted(directory: &StoragePath, children: &[(&'static str, EntryKind)]) -> ListingPage {
    let described = children
        .iter()
        .map(|(name, kind)| {
            let child = child_path(directory, name).unwrap_or_else(|error| panic!("{error}"));
            descriptor(child.as_str(), *kind)
        })
        .collect();
    let mut page = ListingPage::from_descriptors(directory, described, Vec::new(), 0);
    page.children = page
        .children
        .into_iter()
        .map(|child| {
            if child.kind == EntryKind::Directory {
                let hint = format!("hint:{}", child.name());
                child.with_descent_hint(DescentHint::copied_from(hint.as_bytes()))
            } else {
                child
            }
        })
        .collect::<Vec<ListedChild>>();
    page
}

#[async_trait]
impl Namespace for PagedTree {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path.clone();
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((
                directory.as_str().to_owned(),
                request
                    .descent_hint
                    .as_ref()
                    .map(|hint| hint.as_bytes().to_vec()),
            ));
        let listing = self
            .listings
            .get(directory.as_str())
            .cloned()
            .unwrap_or_else(|| pages(Vec::new()));
        let gate = self.gate.clone();
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            for (index, children) in listing.pages.iter().enumerate() {
                if index > 0
                    && let Some(gate) = &gate
                {
                    let Ok(permit) = gate.acquire().await else {
                        return;
                    };
                    permit.forget();
                }
                if sink.send(hinted(&directory, children)).await.is_err() {
                    return;
                }
            }
            if let Some(failure) = listing.then {
                sink.fail(failure).await;
            }
        })
    }
}

pub(super) fn paged_request(window: usize) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(crate::traversal::TraversalOrder::Blocks)
        .max_inflight_operations(
            NonZeroUsize::new(window).unwrap_or_else(|| unreachable!("callers pass nonzero")),
        )
        .max_buffered_items(NonZeroUsize::MIN)
        .cancel(tokio_util::sync::CancellationToken::new())
}

pub(super) fn paged_source(tree: PagedTree) -> (StorageTraversalSource, Arc<PagedTree>) {
    let tree = Arc::new(tree);
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&tree) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    );
    (source, tree)
}

/// The next item, which must arrive: waits generously, so a loaded machine does not fail it.
async fn next_now(session: &mut TraversalSession) -> Option<TraversalItem> {
    tokio::time::timeout(Duration::from_secs(5), session.next_item())
        .await
        .ok()
        .flatten()
}

/// Whether nothing arrives for a while: what a held-back page must cause.
async fn nothing_soon(session: &mut TraversalSession) -> bool {
    tokio::time::timeout(Duration::from_millis(100), session.next_item())
        .await
        .is_err()
}

/// The root's first page is delivered while the rest of its listing is held back: the block is
/// admitted page by page, not once the whole directory has been read.
#[tokio::test]
async fn a_block_is_delivered_page_by_page() {
    let (tree, gate) = PagedTree::new(vec![(
        "",
        pages(vec![
            vec![("f1", EntryKind::File), ("f2", EntryKind::File)],
            vec![("f3", EntryKind::File)],
            vec![("f4", EntryKind::File)],
        ]),
    )])
    .gated();
    let (source, _) = paged_source(tree);
    let mut session = source.traverse(paged_request(4));
    let mut first = Vec::new();
    for _ in 0..2 {
        let item = next_now(&mut session)
            .await
            .unwrap_or_else(|| panic!("the first page was not delivered before the second"));
        first.push(render(&item));
    }
    assert_eq!(first, ["entry f1", "entry f2"]);
    assert!(
        nothing_soon(&mut session).await,
        "something was delivered past the first page"
    );
    gate.add_permits(2);
    let (rest, outcome) = drain(session).await;
    assert_eq!(
        rest.iter().map(render).collect::<Vec<_>>(),
        [
            "entry f3",
            "entry f4",
            "listed <root> Complete listed=4 emitted=4"
        ]
    );
    assert!(matches!(outcome, TraversalOutcome::Completed(_)));
}

/// `z`, `a` and `m` arrive on three pages in that order. The descent is by name — `a`, `m`, `z`
/// — so it cannot start before the last page: a subdirectory seen early is not descended into
/// while the block is still open, and the block's `DirectoryListed` comes first.
#[tokio::test]
async fn the_descent_waits_for_the_block_to_close_and_is_sorted_by_name() {
    let listings = || {
        let leaf = |name| pages(vec![vec![(name, EntryKind::File)]]);
        vec![
            (
                "",
                pages(vec![
                    vec![("z", EntryKind::Directory), ("f1", EntryKind::File)],
                    vec![("a", EntryKind::Directory), ("f2", EntryKind::File)],
                    vec![("m", EntryKind::Directory)],
                ]),
            ),
            ("a", leaf("x")),
            ("m", leaf("x")),
            ("z", leaf("x")),
        ]
    };
    let (tree, gate) = PagedTree::new(listings()).gated();
    let (source, _) = paged_source(tree);
    let mut session = source.traverse(paged_request(8));
    // Two pages of the root; the third is held back.
    gate.add_permits(1);
    let mut seen = Vec::new();
    for _ in 0..4 {
        let item = next_now(&mut session)
            .await
            .unwrap_or_else(|| panic!("the first two pages were not delivered: {seen:?}"));
        seen.push(render(&item));
    }
    assert!(
        nothing_soon(&mut session).await,
        "descended while the root's listing was still open: {seen:?}"
    );
    gate.add_permits(64);
    let (rest, outcome) = drain(session).await;
    let mut items = Vec::new();
    for line in &seen {
        items.push(line.clone());
    }
    items.extend(rest.iter().map(render));
    assert_eq!(
        items,
        [
            "entry z",
            "entry f1",
            "entry a",
            "entry f2",
            "entry m",
            "listed <root> Complete listed=5 emitted=5",
            "entry a/x",
            "listed a Complete listed=1 emitted=1",
            "entry m/x",
            "listed m Complete listed=1 emitted=1",
            "entry z/x",
            "listed z Complete listed=1 emitted=1",
        ]
    );
    assert!(matches!(outcome, TraversalOutcome::Completed(_)));

    // Ungated, over every window: the same sequence, and well formed.
    for window in [1, 2, 8, 64] {
        let (source, _) = paged_source(PagedTree::new(listings()));
        let (lines, _) = events(source.traverse(paged_request(window))).await;
        assert_eq!(lines, items, "window {window}");
    }
}

pub(super) fn denied(at: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(&path(at), FailureClass::PermissionDenied))
}

/// Hides every file whose name starts with `f`, and can insist on `modified`.
#[derive(Debug)]
pub(super) struct HideFiles {
    pub(super) needs_modified: bool,
}

impl TraversalFilter for HideFiles {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: !candidate.name.starts_with('f'),
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// The root returns two pages, then fails. What it returned is delivered, and its subdirectory is
/// still descended into; the failure follows them inside the block with `delivered` = the six
/// children the listing returned — whether the filter emitted them or not — and the block is
/// `Interrupted`, never `Complete` (deletion-safety rule 2) and never `Unreadable`.
#[tokio::test]
async fn a_listing_that_fails_after_a_page_is_interrupted_with_what_it_returned() {
    let listings = || {
        vec![
            (
                "",
                Pages {
                    pages: vec![
                        vec![
                            ("f0", EntryKind::File),
                            ("sub", EntryKind::Directory),
                            ("f1", EntryKind::File),
                        ],
                        vec![("f2", EntryKind::File), ("f3", EntryKind::File)],
                        vec![("f4", EntryKind::File)],
                    ],
                    then: Some(denied("")),
                },
            ),
            ("sub", pages(vec![vec![("x", EntryKind::File)]])),
        ]
    };
    let (source, _) = paged_source(PagedTree::new(listings()));
    let (items, outcome) = drain(source.traverse(paged_request(4))).await;
    super::events::assert_well_formed(&items, &outcome);
    assert_eq!(
        items.iter().map(render).collect::<Vec<_>>(),
        [
            "entry f0",
            "entry sub",
            "entry f1",
            "entry f2",
            "entry f3",
            "entry f4",
            "failure listing <root>",
            "listed <root> Interrupted listed=6 emitted=6",
            "entry sub/x",
            "listed sub Complete listed=1 emitted=1",
        ]
    );
    let failure = items.iter().find_map(|item| match item {
        TraversalItem::Failure(failure) => Some(failure),
        _ => None,
    });
    assert!(
        matches!(
            failure,
            Some(failure) if failure.scope == FailureScope::Listing
                && failure.delivered == Some(6)
                && failure.failure.class() == FailureClass::PermissionDenied
        ),
        "{failure:?}"
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        (
            completion.totals.listing_failures,
            completion.totals.interrupted_listings
        ),
        (1, 1)
    );

    // `delivered` counts what the listing returned, not what was emitted: with every file
    // hidden, on either filter path, it is still six.
    for needs_modified in [false, true] {
        let (source, _) = paged_source(PagedTree::new(listings()));
        let request = paged_request(4).filter(Arc::new(HideFiles { needs_modified }));
        let (lines, _) = events(source.traverse(request)).await;
        assert_eq!(
            lines,
            [
                "entry sub",
                "failure listing <root>",
                "listed <root> Interrupted listed=6 emitted=1 filtered=5",
                "entry sub/x",
                "listed sub Complete listed=1 emitted=1",
            ],
            "needs_modified={needs_modified}"
        );
    }
}

/// A subdirectory whose listing returned a child and then reported `NotFound` did not vanish
/// before it was listed — it was listed in part — so it is `Interrupted`, not an empty complete
/// block; one that is `NotFound` before returning anything did vanish (Q12).
#[tokio::test]
async fn not_found_after_a_page_is_an_interruption_not_a_vanish() {
    let gone = |at| StorageRoleFailure::Entry(entry_failure(&path(at), FailureClass::NotFound));
    let (source, _) = paged_source(PagedTree::new(vec![
        (
            "",
            pages(vec![vec![
                ("early", EntryKind::Directory),
                ("late", EntryKind::Directory),
            ]]),
        ),
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
                pages: vec![vec![("x", EntryKind::File)]],
                then: Some(gone("late")),
            },
        ),
    ]));
    let (lines, outcome) = events(source.traverse(paged_request(4))).await;
    assert_eq!(
        lines,
        [
            "entry early",
            "entry late",
            "listed <root> Complete listed=2 emitted=2",
            "listed early Complete gone listed=0 emitted=0",
            "entry late/x",
            "failure listing late",
            "listed late Interrupted listed=1 emitted=1",
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(completion.totals.vanished_listings, 1);
    assert_eq!(completion.totals.interrupted_listings, 1);
}

/// A lost session part way through a listing ends the traversal; it is never a directory's
/// failure.
#[tokio::test]
async fn a_session_failure_after_a_page_ends_the_traversal() {
    let session_failure = StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Connectivity,
            Transience::Transient,
            "the connection was reset",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    );
    let (source, _) = paged_source(PagedTree::new(vec![(
        "",
        Pages {
            pages: vec![vec![("f", EntryKind::File)]],
            then: Some(session_failure),
        },
    )]));
    let mut session = source.traverse(paged_request(4));
    while session.next_item().await.is_some() {}
    assert!(matches!(
        session.finish().await,
        Err(TraversalTerminalFailure::Session(failure))
            if failure.class() == FailureClass::Connectivity
    ));
}

/// The listing request of every descended directory carries the hint the parent's listing gave
/// it — on the immediate filter path (a `Descend` slot), the deferred one (a `Pending` slot the
/// decision fills), whether the listing was prefetched or started when the cursor got there —
/// and the root's carries none.
#[tokio::test]
async fn the_descent_hint_reaches_the_listing_of_the_child() {
    let listings = || {
        vec![
            (
                "",
                pages(vec![
                    vec![("d1", EntryKind::Directory), ("f", EntryKind::File)],
                    vec![("d0", EntryKind::Directory)],
                ]),
            ),
            ("d0", pages(vec![vec![("e", EntryKind::Directory)]])),
            ("d1", pages(vec![vec![("g", EntryKind::File)]])),
        ]
    };
    for window in [1, 8] {
        for filter in [None, Some(false), Some(true)] {
            let (source, tree) = paged_source(PagedTree::new(listings()));
            let mut request = paged_request(window);
            if let Some(needs_modified) = filter {
                request = request.filter(Arc::new(HideFiles { needs_modified }));
            }
            let (_, outcome) = events(source.traverse(request)).await;
            assert!(matches!(outcome, TraversalOutcome::Completed(_)));
            let mut requests = tree.requests();
            requests.sort();
            let hint = |value: &str| Some(format!("hint:{value}").into_bytes());
            assert_eq!(
                requests,
                [
                    (String::new(), None),
                    ("d0".to_owned(), hint("d0")),
                    ("d0/e".to_owned(), hint("e")),
                    ("d1".to_owned(), hint("d1")),
                ],
                "window {window}, filter {filter:?}"
            );
        }
    }
}

/// Wraps a namespace and re-cuts each of its listings into pages of `size`, so the same tree can
/// be read in pages of any size.
struct Repaged<N> {
    inner: N,
    size: usize,
}

#[async_trait]
impl<N: Namespace> Namespace for Repaged<N> {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path.clone();
        let whole = self.inner.list(request);
        let size = self.size;
        ListingStream::from_producer(directory, move |sink: PageSink| async move {
            let page = match collect_listing(whole, CollectLimits::UNBOUNDED).await {
                Ok(page) => page,
                Err(failure) => return sink.fail(failure).await,
            };
            let ListingPage {
                children,
                failures,
                internal_hidden,
            } = page;
            let mut children = children.into_iter().peekable();
            let mut first = Some((failures, internal_hidden));
            while first.is_some() || children.peek().is_some() {
                let (failures, hidden) = first.take().unwrap_or_default();
                let page =
                    ListingPage::new(children.by_ref().take(size).collect(), failures, hidden);
                if sink.send(page).await.is_err() {
                    return;
                }
            }
        })
    }

    async fn read_link(&self, link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(link).await
    }
}

/// Paging changes when things arrive, never what or in what order: the event tests' tree read in
/// pages of one, two and three children gives exactly the sequence it gives read whole, with a
/// filter on either path and without.
#[tokio::test]
async fn the_page_size_changes_nothing_in_the_output() {
    for filter in [None, Some(false), Some(true)] {
        let mut sequences = Vec::new();
        for size in [1, 2, 3, usize::MAX] {
            let namespace = Repaged {
                inner: TreeNamespace::new(),
                size,
            };
            let source = StorageTraversalSource::with_roles(
                Arc::new(namespace),
                Arc::new(CountingMetadata::new()),
            );
            let mut request = request(tokio_util::sync::CancellationToken::new());
            if let Some(needs_modified) = filter {
                request = request.filter(Arc::new(HideFiles { needs_modified }));
            }
            let (lines, outcome) = events(source.traverse(request)).await;
            sequences.push((lines, outcome));
        }
        assert!(sequences[0].0.len() > 5, "{:?}", sequences[0]);
        assert!(
            sequences.windows(2).all(|pair| pair[0] == pair[1]),
            "filter {filter:?}: {sequences:#?}"
        );
    }
}

/// A listing that returns the directory `d` twice, on two pages: it is descended into twice, each
/// time through its own listing, and each block is whole — two listings of one path are told
/// apart by the listing, not by the path.
#[tokio::test]
async fn a_directory_listed_twice_is_descended_into_twice() {
    let (source, _) = paged_source(PagedTree::new(vec![
        (
            "",
            pages(vec![
                vec![("d", EntryKind::Directory), ("f", EntryKind::File)],
                vec![("d", EntryKind::Directory)],
            ]),
        ),
        (
            "d",
            pages(vec![
                vec![("x", EntryKind::File), ("y", EntryKind::File)],
                vec![("z", EntryKind::File)],
            ]),
        ),
    ]));
    for window in [1, 8] {
        let (items, outcome) = drain(source.traverse(paged_request(window))).await;
        assert_eq!(
            items.iter().map(render).collect::<Vec<_>>(),
            [
                "entry d",
                "entry f",
                "entry d",
                "listed <root> Complete listed=3 emitted=3",
                "entry d/x",
                "entry d/y",
                "entry d/z",
                "listed d Complete listed=3 emitted=3",
                "entry d/x",
                "entry d/y",
                "entry d/z",
                "listed d Complete listed=3 emitted=3",
            ],
            "window {window}"
        );
        assert!(matches!(outcome, TraversalOutcome::Completed(_)));
    }
}

/// Panics at one name, as a broken filter would.
#[derive(Debug)]
pub(super) struct PanicsAt(pub(super) &'static str);

impl TraversalFilter for PanicsAt {
    fn needs_modified(&self) -> bool {
        false
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        assert_ne!(candidate.name, self.0, "the filter broke");
        TraversalDecision::unfiltered(candidate.kind)
    }
}

/// A listing's task that stops without handing over its end — here a filter panics while the
/// page is prepared — ends the traversal with an `Internal` failure; it never leaves the
/// traversal waiting for a page that cannot come.
#[tokio::test]
async fn a_listing_task_that_breaks_ends_the_traversal() {
    let (source, _) = paged_source(PagedTree::new(vec![(
        "",
        pages(vec![
            vec![("a", EntryKind::File)],
            vec![("boom", EntryKind::File)],
        ]),
    )]));
    let mut session = source.traverse(paged_request(4).filter(Arc::new(PanicsAt("boom"))));
    let finished = tokio::time::timeout(Duration::from_secs(5), async {
        while session.next_item().await.is_some() {}
        session.finish().await
    })
    .await
    .unwrap_or_else(|_| panic!("the traversal waited for a listing that broke"));
    assert_eq!(finished, Err(TraversalTerminalFailure::Internal));
}
