//! Directory events: their order against the entries, what their counts mean, and how failures
//! are scoped.

use std::fmt::Write as _;

use super::filter::{CountingMetadata, TreeNamespace};
use super::*;
use crate::traversal::{
    BlockCounts, FailureScope, ListingOutcome, ScanTotals, TraversalCandidate, TraversalDecision,
    TraversalFilter,
};

/// Root renders as an empty path, which is hard to read in an expected sequence.
fn name(value: &StoragePath) -> &str {
    if value.as_str().is_empty() {
        "<root>"
    } else {
        value.as_str()
    }
}

/// `listed=… emitted=…`, then only the counts that are set, which keeps an expected sequence
/// short while still pinning every field: transpose two of them in `close_block` and the number
/// turns up under the other name.
pub(super) fn render_counts(counts: &BlockCounts) -> String {
    [
        ("filtered", counts.filtered),
        ("failed", counts.failed_children),
        ("vanished", counts.vanished),
        ("hidden", counts.internal_hidden),
        ("pruned", counts.pruned_dirs),
        ("truncated", counts.truncated_dirs),
    ]
    .into_iter()
    .filter(|(_, count)| *count > 0)
    .fold(
        format!("listed={} emitted={}", counts.listed, counts.emitted),
        |mut set, (label, count)| {
            let _ = write!(set, " {label}={count}");
            set
        },
    )
}

/// One line per item, so a whole event sequence can be asserted at once.
pub(super) fn render(item: &TraversalItem) -> String {
    match item {
        TraversalItem::Entry(entry) => format!("entry {}", name(entry.path())),
        TraversalItem::Failure(failure) => format!(
            "failure {} {}",
            match failure.scope {
                FailureScope::Child => "child",
                FailureScope::Listing => "listing",
            },
            name(&failure.path)
        ),
        TraversalItem::DirectoryListed(listed) => format!(
            "listed {} {:?}{} {}",
            name(&listed.path),
            listed.outcome,
            if listed.vanished { " gone" } else { "" },
            render_counts(&listed.counts)
        ),
        TraversalItem::Checkpoint(_) => "checkpoint".to_owned(),
        TraversalItem::ObservationFailure(failure) => format!(
            "observation failure {} {:?} {:?}",
            failure.path, failure.family, failure.version
        ),
    }
}

pub(super) async fn drain(mut session: TraversalSession) -> (Vec<TraversalItem>, TraversalOutcome) {
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

pub(super) async fn events(session: TraversalSession) -> (Vec<String>, TraversalOutcome) {
    let (items, outcome) = drain(session).await;
    assert_well_formed(&items, &outcome);
    (items.iter().map(render).collect(), outcome)
}

/// The directory `path` is in: the empty root path for a path with no `/`.
fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

/// The guarantees that hold for any traversal that ran to completion (P7 §4, §7).
///
/// - One `DirectoryListed` per listed directory, after its parent's, and every block balanced.
/// - A block's entries and child failures are exactly what its counts say were emitted and
///   failed.
/// - A listing failure precedes its own directory's `DirectoryListed`, inside that block, and
///   the outcome is not `Complete`; a non-`Complete` outcome has a listing failure. Rule 2 of
///   the deletion-safety contract rests on this pairing.
/// - The completion's totals are the blocks' counts summed, and balanced.
pub(super) fn assert_well_formed(items: &[TraversalItem], outcome: &TraversalOutcome) {
    let mut listed: Vec<&str> = Vec::new();
    let mut summed = ScanTotals::default();
    let (mut emitted, mut failed) = (0_u64, 0_u64);
    let mut listing_failure: Option<&str> = None;
    for item in items {
        match item {
            TraversalItem::Entry(_) => emitted += 1,
            TraversalItem::Failure(failure) => match failure.scope {
                FailureScope::Child => failed += 1,
                FailureScope::Listing => {
                    assert!(
                        listing_failure.is_none(),
                        "two listing failures in one block"
                    );
                    listing_failure = Some(failure.path.as_str());
                }
            },
            TraversalItem::DirectoryListed(directory) => {
                let path = directory.path.as_str();
                assert!(
                    !listed.contains(&path),
                    "{} was listed twice",
                    name(&directory.path)
                );
                assert!(
                    listed.is_empty() || listed.contains(&parent(path)),
                    "{} was listed before its parent",
                    name(&directory.path)
                );
                listed.push(path);
                let counts = &directory.counts;
                assert!(
                    counts.is_balanced(),
                    "{}: {counts:?}",
                    name(&directory.path)
                );
                assert_eq!(
                    (counts.emitted, counts.failed_children),
                    (emitted, failed),
                    "{}: block items disagree with its counts",
                    name(&directory.path)
                );
                assert_eq!(
                    listing_failure.take().is_some_and(|failed| failed == path),
                    !directory.outcome.is_complete(),
                    "{}: a listing failure and a non-complete outcome go together",
                    name(&directory.path)
                );
                summed.absorb(counts);
                if !directory.outcome.is_complete() {
                    summed.listing_failures += 1;
                }
                if directory.outcome == ListingOutcome::Interrupted {
                    summed.interrupted_listings += 1;
                }
                (emitted, failed) = (0, 0);
            }
            TraversalItem::Checkpoint(_) | TraversalItem::ObservationFailure(_) => {}
        }
    }
    assert_eq!((emitted, failed), (0, 0), "items after the last block");
    assert!(
        listing_failure.is_none(),
        "a listing failure without its block"
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("not completed: {outcome:?}")
    };
    // A vanished listing closes like an empty directory; its own tests count it.
    summed.vanished_listings = completion.totals.vanished_listings;
    assert_eq!(completion.totals, summed, "totals are the blocks summed");
    assert!(completion.totals.is_balanced(), "{:?}", completion.totals);
}

/// Hides `a` but descends into it, and keeps `b` while refusing to descend: the two ways a
/// directory can be held back, which the listing verdict must not confuse.
#[derive(Debug)]
struct HideAndPrune {
    needs_modified: bool,
}

impl TraversalFilter for HideAndPrune {
    fn required_metadata(&self) -> MetadataRequirements {
        if self.needs_modified {
            MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
        } else {
            MetadataRequirements::NONE
        }
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        match candidate.path {
            "keep" => TraversalDecision {
                emit: false,
                descend: true,
                filter_children: true,
            },
            // Emitted but not descended into. `keep/b` sits in a block with nothing hidden,
            // which is what separates `pruned_dirs` from `filtered`.
            "keep/b" => TraversalDecision {
                emit: true,
                descend: false,
                filter_children: false,
            },
            // Neither emitted nor descended into: what the DSL adapter produces for a path
            // exclude, and the only shape where one child is hidden *and* pruned.
            "drop" => TraversalDecision {
                emit: false,
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
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        if directory == StoragePath::root() {
            entries(
                &directory,
                vec![
                    descriptor("sub", EntryKind::Directory),
                    descriptor("file", EntryKind::File),
                ],
            )
        } else {
            let failure = entry_failure(&directory, FailureClass::PermissionDenied);
            answer(&directory, Err(StorageRoleFailure::Entry(failure)))
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
async fn a_directory_is_reported_after_its_children() {
    let (lines, outcome) =
        events(tree_source().traverse(request(tokio_util::sync::CancellationToken::new()))).await;

    assert_eq!(
        lines,
        [
            "entry keep",
            "entry drop",
            "entry top",
            "listed <root> Complete listed=3 emitted=3",
            // The root's block keeps the listing's order; its subdirectories are descended
            // into by name, so `drop` before `keep`.
            "entry drop/x",
            "entry drop/y",
            "listed drop Complete listed=2 emitted=2",
            "entry drop/y/z",
            "listed drop/y Complete listed=1 emitted=1",
            "entry keep/a",
            "entry keep/b",
            "listed keep Complete listed=2 emitted=2",
            "entry keep/b/c",
            "listed keep/b Complete listed=1 emitted=1",
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    let totals = completion.totals;
    assert_eq!((totals.listed, totals.emitted), (9, 9));
    assert_eq!((totals.files, totals.directories), (5, 4));
    // The fake gives every file three bytes.
    assert_eq!(totals.bytes, 15);
    assert_eq!(totals.listing_failures, 0);
}

#[tokio::test]
async fn an_empty_directory_is_reported_too() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(WideNamespace) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata) as Arc<dyn Metadata>,
    );
    let mut traversal = request(tokio_util::sync::CancellationToken::new());
    traversal.root = path("empty");
    let (lines, _) = events(source.traverse(traversal)).await;
    assert_eq!(lines, ["listed empty Complete listed=0 emitted=0"]);
}

#[tokio::test]
async fn the_event_sequence_does_not_depend_on_the_admission_window() {
    let mut sequences = Vec::new();
    for window in [1, 2, 8, 64] {
        let mut traversal = request(tokio_util::sync::CancellationToken::new());
        traversal.max_inflight_operations =
            NonZeroUsize::new(window).unwrap_or_else(|| unreachable!("constant is nonzero"));
        let (lines, outcome) = events(tree_source().traverse(traversal)).await;
        sequences.push((window, lines, outcome));
    }
    let (_, first, first_outcome) = &sequences[0];
    for (window, lines, outcome) in &sequences {
        assert_eq!(lines, first, "window {window} changed the event sequence");
        assert_eq!(outcome, first_outcome, "window {window} changed the totals");
    }
}

#[tokio::test]
async fn a_hidden_directory_is_still_reported_while_a_pruned_one_is_not() {
    for needs_modified in [false, true] {
        let mut traversal = request(tokio_util::sync::CancellationToken::new());
        traversal.filter = Some(Arc::new(HideAndPrune { needs_modified }));
        let (lines, outcome) = events(tree_source().traverse(traversal)).await;

        assert_eq!(
            lines,
            [
                // `keep` and `drop` are both hidden, and `drop` is pruned on top of that: one
                // child, counted as filtered and as pruned.
                "entry top",
                "listed <root> Complete listed=3 emitted=1 filtered=2 pruned=1",
                "entry keep/a",
                "entry keep/b",
                "listed keep Complete listed=2 emitted=2 pruned=1",
            ],
            "needs_modified={needs_modified}"
        );
        let TraversalOutcome::Completed(completion) = outcome else {
            panic!("{outcome:?}")
        };
        assert_eq!(
            (completion.totals.filtered, completion.totals.pruned_dirs),
            (2, 2),
            "needs_modified={needs_modified}"
        );
    }
}

#[tokio::test]
async fn a_directory_max_depth_stops_at_is_emitted_without_a_directory_event() {
    let mut traversal = request(tokio_util::sync::CancellationToken::new());
    traversal.max_depth = NonZeroUsize::new(2);
    let (lines, _) = events(tree_source().traverse(traversal)).await;

    assert_eq!(
        lines,
        [
            "entry keep",
            "entry drop",
            "entry top",
            "listed <root> Complete listed=3 emitted=3",
            "entry drop/x",
            "entry drop/y",
            "listed drop Complete listed=2 emitted=2 truncated=1",
            "entry keep/a",
            "entry keep/b",
            "listed keep Complete listed=2 emitted=2 truncated=1",
        ]
    );
}

/// Deletion safety, rule 2 (P7 §4.1): a directory that could not be listed is a `Listing`
/// failure, not a child's, and its own event says the listing is not complete. Were it reported
/// `Complete`, or its failure scoped to a child, a consumer would read the empty block as "this
/// directory is now empty" and delete everything it knew below it.
#[tokio::test]
async fn a_directory_that_cannot_be_listed_is_a_listing_failure_and_not_complete() {
    let source =
        StorageTraversalSource::with_roles(Arc::new(FailingListNamespace), Arc::new(FakeMetadata));
    let (items, outcome) =
        drain(source.traverse(request(tokio_util::sync::CancellationToken::new()))).await;
    assert_well_formed(&items, &outcome);

    assert_eq!(
        items.iter().map(render).collect::<Vec<_>>(),
        [
            "entry sub",
            "entry file",
            // The root listed fine: the failure belongs to `sub`'s own block, not this one.
            "listed <root> Complete listed=2 emitted=2",
            "failure listing sub",
            "listed sub Unreadable listed=0 emitted=0",
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
                && failure.path.as_str() == "sub"
                && failure.delivered == Some(0)
                && failure.failure.class() == FailureClass::PermissionDenied
        ),
        "{failure:?}"
    );
    let sub = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(listed) if listed.path.as_str() == "sub" => {
            Some(listed.outcome)
        }
        _ => None,
    });
    assert_eq!(sub, Some(ListingOutcome::Unreadable));
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    // A listing failure is a directory's, not a child's: it is not in the child equation.
    assert_eq!(completion.totals.listing_failures, 1);
    assert_eq!(completion.totals.interrupted_listings, 0);
    assert_eq!(completion.totals.failed_children, 0);
}

/// The other half of the scoping: a child the listing could not describe is that child's
/// failure, and leaves its directory's listing complete.
#[tokio::test]
async fn an_undescribable_child_is_a_child_failure_in_a_complete_listing() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(PartialListingNamespace),
        Arc::new(FakeMetadata),
    );
    let (items, outcome) =
        drain(source.traverse(request(tokio_util::sync::CancellationToken::new()))).await;
    assert_well_formed(&items, &outcome);

    assert_eq!(
        items.iter().map(render).collect::<Vec<_>>(),
        [
            "entry dir",
            "listed <root> Complete listed=1 emitted=1",
            "failure child dir/bad",
            "entry dir/ok",
            "listed dir Complete listed=2 emitted=1 failed=1",
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(completion.totals.failed_children, 1);
    assert_eq!(completion.totals.listing_failures, 0);
}

/// Totals are completion evidence: a cancelled traversal has none, so nothing it delivered can
/// be read as a complete account.
#[tokio::test]
async fn cancellation_leaves_no_totals() {
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
        !lines.iter().any(|line| line.starts_with("listed dir ")),
        "a directory whose children were held back must not be reported: {lines:?}"
    );
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
}
