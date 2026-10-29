//! The counts invariant (P7 §7): every listed child is emitted, filtered, failed or vanished,
//! exactly once, per block and in total, whichever filter path decided it.

use super::events::{assert_well_formed, drain, render};
use super::*;
use crate::traversal::{ScanTotals, TraversalCandidate, TraversalDecision, TraversalFilter};

/// A tree that exercises every count at once:
///
/// ```text
/// <root>   a.txt  link  hide/  prune/  deep/  locked/  bad  + one undescribable child
///          + two hidden transfer artifacts
/// hide/    x  drop.tmp
/// deep/    sub/
/// locked/  (cannot be listed)
/// ```
struct CountsNamespace;

#[async_trait]
impl Namespace for CountsNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        match directory.as_str() {
            "" => {
                let page = ListingPage::from_descriptors(
                    &directory,
                    vec![
                        descriptor("a.txt", EntryKind::File),
                        descriptor("link", EntryKind::Symlink),
                        descriptor("hide", EntryKind::Directory),
                        descriptor("prune", EntryKind::Directory),
                        descriptor("deep", EntryKind::Directory),
                        descriptor("locked", EntryKind::Directory),
                        descriptor("bad", EntryKind::File),
                    ],
                    vec![entry_failure(
                        &path("undescribable"),
                        FailureClass::Unsupported,
                    )],
                    2,
                );
                ListingStream::from_pages(directory, [Ok(page)])
            }
            "hide" => entries(
                &directory,
                vec![
                    descriptor("hide/x", EntryKind::File),
                    descriptor("hide/drop.tmp", EntryKind::File),
                ],
            ),
            "deep" => entries(
                &directory,
                vec![descriptor("deep/sub", EntryKind::Directory)],
            ),
            "locked" => answer(
                &directory,
                Err(StorageRoleFailure::Entry(entry_failure(
                    &directory,
                    FailureClass::PermissionDenied,
                ))),
            ),
            other => panic!("{other:?} must not be listed"),
        }
    }

    async fn read_link(&self, _link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Ok(
            SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"a.txt".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
        )
    }
}

/// Observes like [`FakeMetadata`], except that `bad` cannot be observed.
struct FailOneMetadata;

#[async_trait]
impl Metadata for FailOneMetadata {
    async fn observe(
        &self,
        observed: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if observed.as_str() == "bad" {
            return Err(StorageRoleFailure::Entry(entry_failure(
                observed,
                FailureClass::PermissionDenied,
            )));
        }
        FakeMetadata.observe(observed, plan).await
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

/// Hides `hide` but descends into it, hides `hide/drop.tmp`, keeps `prune` without descending.
#[derive(Debug)]
struct CountsFilter {
    needs_modified: bool,
}

impl TraversalFilter for CountsFilter {
    fn required_metadata(&self) -> MetadataRequirements {
        if self.needs_modified {
            MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
        } else {
            MetadataRequirements::NONE
        }
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let (emit, descend) = match candidate.path {
            "hide" => (false, true),
            "hide/drop.tmp" => (false, false),
            "prune" => (true, false),
            _ => return TraversalDecision::unfiltered(candidate.kind),
        };
        TraversalDecision {
            emit,
            descend,
            filter_children: true,
        }
    }
}

async fn counted(needs_modified: bool) -> (Vec<String>, ScanTotals) {
    let source =
        StorageTraversalSource::with_roles(Arc::new(CountsNamespace), Arc::new(FailOneMetadata));
    let mut traversal = request(tokio_util::sync::CancellationToken::new());
    traversal.filter = Some(Arc::new(CountsFilter { needs_modified }));
    traversal.max_depth = NonZeroUsize::new(2);
    let (items, outcome) = drain(source.traverse(traversal)).await;
    // Per block: balanced, and the block's items are what its counts say. In total: the blocks
    // summed.
    assert_well_formed(&items, &outcome);
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    (items.iter().map(render).collect(), completion.totals)
}

/// Every category at once, with exact values: a count that went to the wrong field or was
/// dropped shows up here as a wrong number and in `assert_well_formed` as an unbalanced block.
#[tokio::test]
async fn every_listed_child_is_counted_once() {
    let (lines, totals) = counted(false).await;
    assert_eq!(
        lines,
        [
            "failure child undescribable",
            "entry a.txt",
            "entry link",
            "entry prune",
            "entry deep",
            "entry locked",
            "failure child bad",
            "listed <root> Complete listed=8 emitted=5 filtered=1 failed=2 hidden=2 pruned=1",
            // Descended in name order, not listing order: `deep` < `hide` < `locked`.
            "entry deep/sub",
            "listed deep Complete listed=1 emitted=1 truncated=1",
            "entry hide/x",
            "listed hide Complete listed=2 emitted=1 filtered=1",
            "failure listing locked",
            "listed locked Unreadable listed=0 emitted=0",
        ]
    );
    let expected = ScanTotals {
        listed: 11,
        emitted: 7,
        filtered: 2,
        failed_children: 2,
        internal_hidden: 2,
        listing_failures: 1,
        pruned_dirs: 1,
        truncated_dirs: 1,
        bytes: 6,
        // No histories: every file's bytes are its only version's.
        all_versions_bytes: 6,
        files: 2,
        directories: 4,
        symlinks: 1,
        ..ScanTotals::default()
    };
    assert_eq!(totals, expected);
    assert!(totals.is_balanced());
}

/// A decision made when the listing lands and one made once the child's observation settles
/// travel different paths to the counts (the listing task's facts, or the slot); both must count
/// the same.
#[tokio::test]
async fn the_immediate_and_deferred_filter_paths_count_the_same() {
    let immediate = counted(false).await;
    let deferred = counted(true).await;
    assert_eq!(immediate, deferred);
}
