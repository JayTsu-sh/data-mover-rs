//! The error model's two non-failures (P7 §8, C3): a child or subdirectory that vanished since it
//! was listed is counted, not reported; a throttled request fails only what it was for, never the
//! traversal.

use std::sync::Mutex;

use super::events::{drain, events};
use super::*;
use crate::model::BackendSessionFailure;
use crate::traversal::{
    FailureScope, ListingOutcome, TraversalCandidate, TraversalDecision, TraversalFilter,
};

/// ```text
/// <root>
/// ├── a        file
/// ├── gone     file, NotFound when observed
/// ├── link     symlink, NotFound when its target is read
/// ├── flicker  returned by the listing, NotFound when the listing described it
/// ├── dir/     directory holding `x`
/// ├── ghost/   directory, NotFound when listed
/// ├── busy/    directory whose listing is throttled past its budget (a throttled session failure)
/// └── slow     file whose observation is throttled
/// ```
#[derive(Default)]
struct VanishingTree {
    listed: Mutex<Vec<String>>,
    root: Root,
}

/// What the root of a [`VanishingTree`] lists.
#[derive(Clone, Copy, Default, PartialEq)]
enum Root {
    /// The tree drawn above, without `busy` and `slow`.
    #[default]
    Plain,
    /// The tree drawn above, with `busy` and `slow`.
    Throttle,
    /// Only `a` and `gonedir`.
    GoneDirectory,
    /// Only `xattr-gone`: observing it reports `NotFound` although it exists.
    StillThere,
    /// Only `a`, with a `NotFound` read failure blamed on the root itself.
    UnnamedNotFound,
    /// Only `busy-stat`: observing it reports `NotFound`, and confirming that is throttled.
    ThrottledConfirmation,
}

fn throttled() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Throttled,
            Transience::Transient,
            "the service asked for fewer requests",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn gone(at: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(&path(at), FailureClass::NotFound))
}

#[async_trait]
impl Namespace for VanishingTree {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        self.listed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(directory.as_str().to_owned());
        match directory.as_str() {
            "" if self.root == Root::ThrottledConfirmation => {
                entries(&directory, vec![descriptor("busy-stat", EntryKind::File)])
            }
            "" if self.root == Root::UnnamedNotFound => {
                let unnamed = entry_failure(&directory, FailureClass::NotFound);
                answer(
                    &directory,
                    Ok((vec![descriptor("a", EntryKind::File)], vec![unnamed])),
                )
            }
            "" if self.root == Root::StillThere => {
                entries(&directory, vec![descriptor("xattr-gone", EntryKind::File)])
            }
            "" if self.root == Root::GoneDirectory => entries(
                &directory,
                vec![
                    descriptor("a", EntryKind::File),
                    descriptor("gonedir", EntryKind::Directory),
                ],
            ),
            "" => {
                let mut children = vec![
                    descriptor("a", EntryKind::File),
                    descriptor("gone", EntryKind::File),
                    descriptor("link", EntryKind::Symlink),
                    descriptor("dir", EntryKind::Directory),
                    descriptor("ghost", EntryKind::Directory),
                ];
                if self.root == Root::Throttle {
                    children.push(descriptor("busy", EntryKind::Directory));
                    children.push(descriptor("slow", EntryKind::File));
                }
                let vanished = entry_failure(&path("flicker"), FailureClass::NotFound);
                answer(&directory, Ok((children, vec![vanished])))
            }
            "dir" => entries(&directory, vec![descriptor("dir/x", EntryKind::File)]),
            "busy" => answer(&directory, Err(throttled())),
            _ => answer(&directory, Err(gone(directory.as_str()))),
        }
    }

    async fn read_link(&self, link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(gone(link.as_str()))
    }

    /// What a child's observation `NotFound` is confirmed with: gone for everything this tree
    /// makes vanish, there for `xattr-gone` (whose `NotFound` was about an attribute).
    async fn stat(&self, at: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        match at.as_str() {
            "xattr-gone" => Ok(descriptor(at.as_str(), EntryKind::File)),
            "busy-stat" => Err(throttled()),
            _ => Err(gone(at.as_str())),
        }
    }
}

/// Observes everything but `gone` (vanished) and `slow` (throttled).
struct VanishingMetadata;

#[async_trait]
impl Metadata for VanishingMetadata {
    async fn observe(
        &self,
        observed: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        match observed.as_str() {
            "gone" | "gonedir" | "xattr-gone" | "busy-stat" => Err(gone(observed.as_str())),
            "slow" => Err(throttled()),
            _ => FakeMetadata.observe(observed, plan).await,
        }
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

fn source(tree: Arc<VanishingTree>) -> StorageTraversalSource {
    StorageTraversalSource::with_roles(tree, Arc::new(VanishingMetadata))
}

/// A child gone when it was observed, or when the listing described it, and a subdirectory gone
/// when it was listed: nothing is emitted for any of them, no failure item, the listing stays
/// complete, and they are counted as vanished. The traversal completes and balances.
///
/// Fault injection: settle a `NotFound` observation as a child failure (the pre-C3 behaviour)
/// and the `vanished=3` / zero-failure assertions go red; count the vanished listing as a listing
/// failure and the `ghost` line and `listing_failures` go red.
#[tokio::test]
async fn vanished_children_and_listings_are_counted_not_reported() {
    let tree = Arc::new(VanishingTree::default());
    let request = request(tokio_util::sync::CancellationToken::new());
    let (lines, outcome) = events(source(Arc::clone(&tree)).traverse(request)).await;
    assert_eq!(
        lines,
        [
            "entry a",
            "entry dir",
            "entry ghost",
            "listed <root> Complete listed=6 emitted=3 vanished=3",
            "entry dir/x",
            "listed dir Complete listed=1 emitted=1",
            // Gone since its parent listed it: complete and empty, not a listing failure.
            "listed ghost Complete gone listed=0 emitted=0",
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    let totals = completion.totals;
    assert_eq!((totals.listed, totals.emitted, totals.vanished), (7, 4, 3));
    assert_eq!(
        (
            totals.failed_children,
            totals.listing_failures,
            totals.vanished_listings
        ),
        (0, 0, 1)
    );
    assert!(totals.is_balanced());
}

/// The traversal root is never "vanished": a root that does not exist is a listing failure, so a
/// mistyped root cannot read as an empty, complete source.
#[tokio::test]
async fn a_missing_root_is_still_a_listing_failure() {
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.root = path("nowhere");
    let (lines, outcome) =
        events(source(Arc::new(VanishingTree::default())).traverse(request)).await;
    assert_eq!(
        lines,
        [
            "failure listing nowhere",
            "listed nowhere Unreadable listed=0 emitted=0"
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        (
            completion.totals.listing_failures,
            completion.totals.vanished_listings
        ),
        (1, 0)
    );
}

/// Admits everything, but only once `modified` is known: every decision is deferred.
#[derive(Debug)]
struct Deferring;

impl TraversalFilter for Deferring {
    fn needs_modified(&self) -> bool {
        true
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision::unfiltered(candidate.kind)
    }
}

/// A directory whose deferred observation found it gone is neither a failure nor descended into:
/// a failed observation still lists the directory (so its subtree is not dropped silently), but
/// one that vanished has no subtree left to list.
#[tokio::test]
async fn a_vanished_deferred_directory_is_not_descended_into() {
    let tree = Arc::new(VanishingTree {
        root: Root::GoneDirectory,
        ..VanishingTree::default()
    });
    let source = source(Arc::clone(&tree));
    let request = request(tokio_util::sync::CancellationToken::new()).filter(Arc::new(Deferring));
    let (lines, _) = events(source.traverse(request)).await;
    assert_eq!(
        lines,
        [
            "entry a",
            "listed <root> Complete listed=2 emitted=1 vanished=1"
        ]
    );
    let listed = tree
        .listed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(listed, [""], "the vanished directory is never listed");
}

/// Throttling asks for fewer requests; it is no lost session (P7 §8, Q11). A listing still
/// throttled once the backend's budget is spent is that listing's failure (`Throttled`, the
/// directory `Unreadable`), a throttled observation is that child's, and the traversal goes on
/// to complete.
///
/// Fault injection: let a throttled session failure through as a session failure (drop
/// `throttling_scoped_to`) and the traversal ends with it instead of completing — red.
#[tokio::test]
async fn throttling_fails_only_what_it_was_for() {
    let tree = Arc::new(VanishingTree {
        root: Root::Throttle,
        ..VanishingTree::default()
    });
    let request = request(tokio_util::sync::CancellationToken::new());
    let (items, outcome) = drain(source(tree).traverse(request)).await;
    let TraversalOutcome::Completed(completion) = &outcome else {
        panic!("throttling ended the traversal: {outcome:?}")
    };
    super::events::assert_well_formed(&items, &outcome);
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some((
                failure.scope,
                failure.path.as_str(),
                failure.failure.class(),
                failure.failure.transience(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        [
            (
                FailureScope::Child,
                "slow",
                FailureClass::Throttled,
                Transience::Transient
            ),
            (
                FailureScope::Listing,
                "busy",
                FailureClass::Throttled,
                Transience::Transient
            ),
        ]
    );
    let busy = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(listed) if listed.path.as_str() == "busy" => {
            Some(listed.outcome)
        }
        _ => None,
    });
    assert_eq!(busy, Some(ListingOutcome::Unreadable));
    assert_eq!(completion.totals.listing_failures, 1);
    assert_eq!(completion.totals.failed_children, 1);
}

/// Without a filter the descent into `gonedir` is decided from the listing, before its
/// observation (which needs I/O) finds it gone. The output does not depend on which finishes
/// first: the directory is vanished as a child and still listed, and its listing — gone too — is
/// complete, empty and marked `gone`, not a failure.
#[tokio::test]
async fn a_directory_gone_at_observation_is_listed_as_gone() {
    let tree = Arc::new(VanishingTree {
        root: Root::GoneDirectory,
        ..VanishingTree::default()
    });
    let request = request(tokio_util::sync::CancellationToken::new());
    let (lines, outcome) = events(source(tree).traverse(request)).await;
    assert_eq!(
        lines,
        [
            "entry a",
            "listed <root> Complete listed=2 emitted=1 vanished=1",
            "listed gonedir Complete gone listed=0 emitted=0",
        ]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        (
            completion.totals.vanished,
            completion.totals.vanished_listings
        ),
        (1, 1)
    );
}

/// A `NotFound` about something other than the child — an extended attribute removed between
/// listing and reading it — is not a vanish: the child is confirmed still there, so it is that
/// child's failure (`Conflict`, transient), which keeps it out of deletion. Fault injection: trust
/// the `NotFound` without confirming and the child silently vanishes — red.
#[tokio::test]
async fn a_not_found_about_part_of_a_child_that_is_still_there_is_a_failure() {
    let tree = Arc::new(VanishingTree {
        root: Root::StillThere,
        ..VanishingTree::default()
    });
    let request = request(tokio_util::sync::CancellationToken::new());
    let (items, outcome) = drain(source(tree).traverse(request)).await;
    super::events::assert_well_formed(&items, &outcome);
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some((
                failure.scope,
                failure.path.as_str(),
                failure.failure.class(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        [(FailureScope::Child, "xattr-gone", FailureClass::Conflict)]
    );
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        (
            completion.totals.vanished,
            completion.totals.failed_children
        ),
        (0, 1)
    );
}

/// A `NotFound` the listing blames on the directory itself names no child — a read of the
/// directory failed part way — so it cannot be a vanish: it stays a child failure, which keeps
/// every child of the directory out of deletion. Fault injection: let `is_vanished` ignore the
/// path and it is silently counted as vanished — red.
#[tokio::test]
async fn a_not_found_blamed_on_the_directory_is_a_failure_not_a_vanish() {
    let tree = Arc::new(VanishingTree {
        root: Root::UnnamedNotFound,
        ..VanishingTree::default()
    });
    let request = request(tokio_util::sync::CancellationToken::new());
    let (lines, _) = events(source(tree).traverse(request)).await;
    assert_eq!(
        lines,
        [
            "failure child <root>",
            "entry a",
            "listed <root> Complete listed=2 emitted=1 failed=1",
        ]
    );
}

/// Confirming a vanish is a request too, and a throttled one confirms nothing: the child is that
/// child's failure, and the traversal completes (P7 §8). Fault injection: return the throttled
/// session failure of the confirming `stat` as it is and the traversal ends — red.
#[tokio::test]
async fn a_throttled_confirmation_is_a_child_failure_not_the_end() {
    let tree = Arc::new(VanishingTree {
        root: Root::ThrottledConfirmation,
        ..VanishingTree::default()
    });
    let request = request(tokio_util::sync::CancellationToken::new());
    let (lines, outcome) = events(source(tree).traverse(request)).await;
    assert_eq!(
        lines,
        [
            "failure child busy-stat",
            "listed <root> Complete listed=1 emitted=0 failed=1",
        ]
    );
    assert!(matches!(outcome, TraversalOutcome::Completed(_)));
}
