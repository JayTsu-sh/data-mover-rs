//! Directory completion items: their order against the entries, and what their tallies mean.

use super::filter::{CountingMetadata, TreeNamespace};
use super::*;
use crate::traversal::{DirectoryListing, TraversalCandidate, TraversalDecision, TraversalFilter};

/// Root renders as an empty path, which is hard to read in an expected sequence.
fn name(value: &StoragePath) -> &str {
    if value.as_str().is_empty() {
        "<root>"
    } else {
        value.as_str()
    }
}

/// One line per item, so a whole event sequence can be asserted at once.
fn render(item: &TraversalItem) -> String {
    match item {
        TraversalItem::Entry(entry) => format!("entry {}", name(entry.path())),
        TraversalItem::EntryFailure(error) => format!("failure {}", name(error.path())),
        TraversalItem::DirectoryListed(listed) => format!(
            "listed {} {:?} pruned={} truncated={}",
            name(&listed.path),
            listed.listing,
            listed.pruned_children,
            listed.truncated_children
        ),
        TraversalItem::SubtreeComplete(complete) => format!(
            "subtree {} dirs={} entries={} exhaustive={}",
            name(&complete.path),
            complete.summary.directories_listed,
            complete.summary.observed_entries,
            complete.summary.is_exhaustive()
        ),
    }
}

async fn drain(mut session: TraversalSession) -> (Vec<TraversalItem>, TraversalOutcome) {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let outcome = session
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    (items, outcome)
}

async fn events(session: TraversalSession) -> (Vec<String>, TraversalOutcome) {
    let (items, outcome) = drain(session).await;
    assert_well_formed(&items);
    (items.iter().map(render).collect(), outcome)
}

/// The guarantees that hold for any traversal that ran to completion: one `DirectoryListed` and
/// one `SubtreeComplete` per listed directory, an ancestor listed before its descendants, and
/// nothing but a directory's own descendants between its two items.
fn assert_well_formed(items: &[TraversalItem]) {
    let mut open: Vec<&StoragePath> = Vec::new();
    let mut listed: Vec<&str> = Vec::new();
    let mut completed: Vec<&str> = Vec::new();
    for item in items {
        match item {
            TraversalItem::DirectoryListed(directory) => {
                assert!(
                    !listed.contains(&directory.path.as_str()),
                    "{} was listed twice",
                    name(&directory.path)
                );
                listed.push(directory.path.as_str());
                open.push(&directory.path);
            }
            TraversalItem::SubtreeComplete(complete) => {
                let innermost = open.pop().unwrap_or_else(|| {
                    panic!("{} completed without being listed", name(&complete.path))
                });
                assert_eq!(
                    innermost.as_str(),
                    complete.path.as_str(),
                    "subtree items do not nest"
                );
                completed.push(complete.path.as_str());
            }
            TraversalItem::Entry(_) | TraversalItem::EntryFailure(_) => {}
        }
    }
    assert!(open.is_empty(), "a completed traversal left {open:?} open");
    assert_eq!(listed.len(), completed.len());
    assert!(
        matches!(items.last(), Some(TraversalItem::SubtreeComplete(_))),
        "a completed traversal ends with the root's subtree item"
    );
}

/// Hides `a` but descends into it, and keeps `b` while refusing to descend: the two ways a
/// directory can be held back, which the listing verdict must not confuse.
#[derive(Debug)]
struct HideAndPrune {
    needs_modified: bool,
}

impl TraversalFilter for HideAndPrune {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        match candidate.path {
            "keep" => TraversalDecision {
                emit: false,
                descend: true,
                filter_children: true,
            },
            "drop" => TraversalDecision {
                emit: true,
                descend: false,
                filter_children: false,
            },
            _ => TraversalDecision::unfiltered(candidate.kind),
        }
    }
}

/// Lists the root but cannot list the directory in it.
struct FailingListNamespace;

#[async_trait]
impl Namespace for FailingListNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::List(root) if root == StoragePath::root() => {
                Ok(NamespaceResult::Entries(vec![
                    descriptor("sub", EntryKind::Directory),
                    descriptor("file", EntryKind::File),
                ]))
            }
            NamespaceRequest::List(directory) => Err(StorageRoleFailure::Entry(entry_failure(
                &directory,
                FailureClass::PermissionDenied,
            ))),
            _ => unreachable!("only List is issued"),
        }
    }
}

/// Never answers for entries below `hold`, so a subtree can be left unfinished.
struct HoldingMetadata {
    hold: &'static str,
}

#[async_trait]
impl Metadata for HoldingMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if path.as_str().starts_with(self.hold) {
            std::future::pending::<()>().await;
        }
        FakeMetadata.observe(path, plan).await
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal never applies metadata")
    }
}

fn tree_source() -> StorageTraversalSource {
    StorageTraversalSource::with_roles(
        Arc::new(TreeNamespace::new()) as Arc<dyn Namespace>,
        Arc::new(CountingMetadata::new()) as Arc<dyn Metadata>,
    )
}

#[tokio::test]
async fn a_directory_is_reported_after_its_children_and_again_after_its_subtree() {
    let (lines, outcome) =
        events(tree_source().traverse(request(tokio_util::sync::CancellationToken::new()))).await;

    assert_eq!(
        lines,
        [
            "entry keep",
            "entry drop",
            "entry top",
            "listed <root> Complete pruned=0 truncated=0",
            "entry keep/a",
            "entry keep/b",
            "listed keep Complete pruned=0 truncated=0",
            "entry keep/b/c",
            "listed keep/b Complete pruned=0 truncated=0",
            "subtree keep/b dirs=1 entries=1 exhaustive=true",
            "subtree keep dirs=2 entries=3 exhaustive=true",
            "entry drop/x",
            "entry drop/y",
            "listed drop Complete pruned=0 truncated=0",
            "entry drop/y/z",
            "listed drop/y Complete pruned=0 truncated=0",
            "subtree drop/y dirs=1 entries=1 exhaustive=true",
            "subtree drop dirs=2 entries=3 exhaustive=true",
            "subtree <root> dirs=5 entries=9 exhaustive=true",
        ]
    );
    assert!(matches!(
        outcome,
        TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 9,
            entry_failures: 0,
            directories_listed: 5,
        })
    ));
}

#[tokio::test]
async fn the_event_sequence_does_not_depend_on_the_admission_window() {
    let mut sequences = Vec::new();
    for window in [1, 2, 8, 64] {
        let mut traversal = request(tokio_util::sync::CancellationToken::new());
        traversal.max_inflight_operations =
            NonZeroUsize::new(window).unwrap_or_else(|| unreachable!("constant is nonzero"));
        let (lines, _) = events(tree_source().traverse(traversal)).await;
        sequences.push((window, lines));
    }
    let (_, first) = &sequences[0];
    for (window, lines) in &sequences {
        assert_eq!(lines, first, "window {window} changed the event sequence");
    }
}

#[tokio::test]
async fn a_hidden_directory_is_still_reported_while_a_pruned_one_is_not() {
    for needs_modified in [false, true] {
        let mut traversal = request(tokio_util::sync::CancellationToken::new());
        traversal.filter = Some(Arc::new(HideAndPrune { needs_modified }));
        let (lines, _) = events(tree_source().traverse(traversal)).await;

        assert_eq!(
            lines,
            [
                // `keep` is hidden, so the root's listing is `Filtered`; `drop` is emitted but
                // not descended into, which is `pruned_children`, not `Filtered`.
                "entry drop",
                "entry top",
                "listed <root> Filtered pruned=1 truncated=0",
                "entry keep/a",
                "entry keep/b",
                "listed keep Complete pruned=0 truncated=0",
                "entry keep/b/c",
                "listed keep/b Complete pruned=0 truncated=0",
                "subtree keep/b dirs=1 entries=1 exhaustive=true",
                "subtree keep dirs=2 entries=3 exhaustive=true",
                "subtree <root> dirs=3 entries=5 exhaustive=false",
            ],
            "needs_modified={needs_modified}"
        );
    }
}

#[tokio::test]
async fn a_directory_max_depth_stops_at_is_emitted_without_completion_items() {
    let mut traversal = request(tokio_util::sync::CancellationToken::new());
    traversal.max_depth = NonZeroUsize::new(2);
    let (lines, _) = events(tree_source().traverse(traversal)).await;

    assert_eq!(
        lines,
        [
            "entry keep",
            "entry drop",
            "entry top",
            "listed <root> Complete pruned=0 truncated=0",
            "entry keep/a",
            "entry keep/b",
            "listed keep Complete pruned=0 truncated=1",
            "subtree keep dirs=1 entries=2 exhaustive=false",
            "entry drop/x",
            "entry drop/y",
            "listed drop Complete pruned=0 truncated=1",
            "subtree drop dirs=1 entries=2 exhaustive=false",
            "subtree <root> dirs=3 entries=7 exhaustive=false",
        ]
    );
}

#[tokio::test]
async fn a_directory_that_cannot_be_listed_still_gets_both_items() {
    let source =
        StorageTraversalSource::with_roles(Arc::new(FailingListNamespace), Arc::new(FakeMetadata));
    let (lines, outcome) =
        events(source.traverse(request(tokio_util::sync::CancellationToken::new()))).await;

    assert_eq!(
        lines,
        [
            "entry sub",
            "entry file",
            // The root listed fine: the failure belongs to `sub`'s own block, not this one.
            "listed <root> Complete pruned=0 truncated=0",
            "failure sub",
            "listed sub Failed pruned=0 truncated=0",
            "subtree sub dirs=1 entries=0 exhaustive=false",
            "subtree <root> dirs=2 entries=2 exhaustive=false",
        ]
    );
    assert!(matches!(
        outcome,
        TraversalOutcome::Completed(TraversalCompletion {
            entry_failures: 1,
            directories_listed: 2,
            ..
        })
    ));
}

#[tokio::test]
async fn an_undescribable_child_makes_the_listing_partial_and_not_merely_failed() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(PartialListingNamespace),
        Arc::new(FakeMetadata),
    );
    let (items, _) =
        drain(source.traverse(request(tokio_util::sync::CancellationToken::new()))).await;
    assert_well_formed(&items);

    assert_eq!(
        items.iter().map(render).collect::<Vec<_>>(),
        [
            "entry dir",
            "listed <root> Complete pruned=0 truncated=0",
            "failure dir/bad",
            "entry dir/ok",
            "listed dir Partial { failures: 1 } pruned=0 truncated=0",
            "subtree dir dirs=1 entries=1 exhaustive=false",
            "subtree <root> dirs=2 entries=2 exhaustive=false",
        ]
    );
    let listing = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(directory) if directory.path.as_str() == "dir" => {
            Some(directory.listing)
        }
        _ => None,
    });
    assert_eq!(listing, Some(DirectoryListing::Partial { failures: 1 }));
}

#[tokio::test]
async fn cancellation_can_leave_a_listed_directory_without_its_subtree() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let source = StorageTraversalSource::with_roles(
        Arc::new(FakeNamespace),
        Arc::new(HoldingMetadata { hold: "dir/" }),
    );
    let mut session = source.traverse(request(cancel.clone()));

    let mut lines = Vec::new();
    while let Some(item) = session.next_item().await {
        let line = render(&item);
        let root_listed = line.starts_with("listed <root>");
        lines.push(line);
        if root_listed {
            cancel.cancel();
            break;
        }
    }
    while session.next_item().await.is_some() {}

    assert!(
        lines.iter().any(|line| line.starts_with("listed <root>")),
        "{lines:?}"
    );
    assert!(
        !lines.iter().any(|line| line.starts_with("subtree")),
        "an unfinished subtree must not be reported complete: {lines:?}"
    );
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
}
