//! How a descend target's path is arrived at, and the spellings that punish guessing.
//!
//! Listings carry names, not paths (P7 §5.4.4): a child's full path is, by contract, the listed
//! directory's path as it was requested, a `/`, and the child's name — no normalisation of
//! either. The entry the traversal emits and the directory it descends into are both built from
//! that one join, so they cannot disagree, whatever the spelling of the directory:
//!
//! * [`StoragePath`] deliberately keeps `a//b` distinct from `a/b`; a traversal rooted at `a//b`
//!   descends into `a//b/sub`, and that is also the path its entry carries.
//! * A caller may root the traversal at `./t`; the children are `./t/d`, not `t/d`.
//!
//! A backend whose own spelling of a child is not that join — one that resolves paths
//! component-wise and hands `a/b/sub` back for a listing of `a//b` — must not have its child
//! renamed silently: the directory's entry would then disagree with its own `DirectoryListed`,
//! and a consumer merging two sides by path reads that as "the other side has something this
//! side does not" and deletes. Such a child is a failure item at the path the backend spelled,
//! never an entry and never descended into. These tests pin both halves.
//!
//! The fixture answers a directory it never handed out with an *empty* listing and records the
//! request, and it refuses to serve more listings than the tree has directories. A fixture that
//! answered every unknown directory with directories again turns a wrong spelling into an
//! endless tree; so does a fault that lands back on a directory the fixture *does* know, such as
//! a join that drops the child's name and re-lists the parent forever. Either way the fault has
//! to surface as a named failure, not as a hung test.

use std::sync::{Mutex, PoisonError};

use super::*;
use crate::model::BackendSessionFailure;

/// More listings than any tree here has directories, and few enough that a fault which re-lists
/// one directory forever stops within a test's patience rather than the harness's.
const MAX_LISTINGS: usize = 16;

/// Lists a small tree, spelling each directory's children however the caller says — which is how
/// a real backend's own normalisation would show up: the request uses one spelling, the backend's
/// listing another.
struct SpellingNamespace {
    /// Each directory this fixture knows, with the listing it answers with. The spellings are
    /// the point, so listings are keyed by the exact path the traversal asks for.
    listings: Vec<(String, Vec<(String, EntryKind)>)>,
    served: Mutex<Served>,
}

/// What the fixture has been asked for, which is the evidence the tests read.
#[derive(Default)]
struct Served {
    /// Requests for directories no listing in this fixture ever handed out.
    unexpected: Vec<String>,
    /// How many listings were answered, so an endless re-listing ends the session.
    listings: usize,
}

impl SpellingNamespace {
    fn new(listings: &[(&str, &[(&str, EntryKind)])]) -> Self {
        Self {
            listings: listings
                .iter()
                .map(|(directory, entries)| {
                    let entries = entries
                        .iter()
                        .map(|(path, kind)| ((*path).to_owned(), *kind))
                        .collect();
                    ((*directory).to_owned(), entries)
                })
                .collect(),
            served: Mutex::new(Served::default()),
        }
    }

    fn served(&self) -> std::sync::MutexGuard<'_, Served> {
        self.served.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn unexpected(&self) -> Vec<String> {
        self.served().unexpected.clone()
    }
}

#[async_trait]
impl Namespace for SpellingNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let entries = {
            let mut served = self.served();
            served.listings += 1;
            if served.listings > MAX_LISTINGS {
                let failure = BackendSessionFailure::new(
                    Operation::Traverse,
                    FailureClass::Connectivity,
                    Transience::Permanent,
                    "fixture was asked for more listings than the tree has directories",
                )
                .unwrap_or_else(|error| panic!("{error}"));
                return answer(&directory, Err(StorageRoleFailure::Session(failure)));
            }
            let found = self
                .listings
                .iter()
                .find(|(known, _)| known == directory.as_str());
            if let Some((_, entries)) = found {
                entries.clone()
            } else {
                served.unexpected.push(directory.as_str().to_owned());
                Vec::new()
            }
        };
        let described = entries
            .iter()
            .map(|(path, kind)| descriptor(path, *kind))
            .collect();
        super::entries(&directory, described)
    }
}

/// Every directory with a `DirectoryListed` also has an entry spelled identically.
///
/// Returns what it checked, as `"<kind> <path>"` and sorted: sorted because this file pins
/// spelling agreement and not order — order is pinned in `events.rs` and
/// `order.rs`. A caller asserts on the result, since an invariant that holds over an empty set
/// proves nothing.
fn assert_entries_and_markers_agree(items: &[TraversalItem], root: &str) -> Vec<String> {
    let entries: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.path().as_str()),
            _ => None,
        })
        .collect();
    let mut checked = Vec::new();
    for item in items {
        let (kind, path) = match item {
            TraversalItem::DirectoryListed(listed) => ("listed", listed.path.as_str()),
            _ => continue,
        };
        // The traversal root is not one of its own entries, so it has nothing to agree with.
        if path == root {
            continue;
        }
        assert!(
            entries.contains(&path),
            "{kind} {path:?} has no entry spelled the same way; entries: {entries:?}"
        );
        checked.push(format!("{kind} {path}"));
    }
    checked.sort();
    checked
}

/// Runs the traversal to completion over the fixture and returns its items alongside every
/// request for a directory the fixture never handed out — the direct signature of a guessed
/// prefix.
async fn spelled(
    root: &str,
    listings: &[(&str, &[(&str, EntryKind)])],
) -> (Vec<TraversalItem>, Vec<String>) {
    let namespace = Arc::new(SpellingNamespace::new(listings));
    let role = Arc::clone(&namespace) as Arc<dyn Namespace>;
    let source = StorageTraversalSource::with_roles(role, Arc::new(FakeMetadata));
    let mut session = source.traverse(
        TraversalRequest::new(path(root))
            .max_inflight_operations(
                NonZeroUsize::new(4).unwrap_or_else(|| unreachable!("constant is nonzero")),
            )
            .max_buffered_items(
                NonZeroUsize::new(4).unwrap_or_else(|| unreachable!("constant is nonzero")),
            )
            .cancel(tokio_util::sync::CancellationToken::new()),
    );
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let outcome = match session.finish().await {
        Ok(outcome) => outcome,
        Err(error) => {
            // The ceiling's own failure says only "session failed", so name the reason here:
            // a descend target that lands back on a directory the fixture already listed
            // re-lists it forever, and the count is the only place that shows.
            let served = namespace.served().listings;
            assert!(
                served <= MAX_LISTINGS,
                "the fixture served {served} listings for a tree of {} directories: a descend \
                 target is landing back on a directory it already listed",
                namespace.listings.len()
            );
            panic!("{error}")
        }
    };
    assert!(
        matches!(outcome, TraversalOutcome::Completed(_)),
        "the traversal ended without completing"
    );
    (items, namespace.unexpected())
}

/// Failure items, as their paths, so a test can name the children that were refused.
fn failures(items: &[TraversalItem]) -> Vec<&str> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some(failure.path.as_str()),
            _ => None,
        })
        .collect()
}

/// A traversal rooted at `a//b` keeps the doubled separator in every path below it: the children
/// are `a//b/sub`, their entries and directory events agree, and every listing asked for is one
/// the backend handed out.
#[tokio::test]
async fn a_doubled_separator_is_kept_verbatim_in_entries_and_markers() {
    let (items, unexpected) = spelled(
        "a//b",
        &[
            ("a//b", &[("a//b/sub", EntryKind::Directory)]),
            ("a//b/sub", &[("a//b/sub/leaf", EntryKind::File)]),
        ],
    )
    .await;
    assert!(
        unexpected.is_empty(),
        "asked for directories the backend never handed out: {unexpected:?}"
    );
    assert!(failures(&items).is_empty(), "{:?}", failures(&items));
    let checked = assert_entries_and_markers_agree(&items, "a//b");
    assert_eq!(
        checked,
        ["listed a//b/sub"],
        "the doubled separator was not exercised: {checked:?}"
    );
}

/// A backend that collapses the separator — `a//b` listed, its child spelled `a/b/sub` — cannot
/// have that child renamed to `a//b/sub`: it is a failure item at the backend's spelling, never
/// an entry and never descended into, and the traversal still completes.
#[tokio::test]
async fn a_child_spelled_off_the_requested_path_is_a_failure_not_a_renamed_entry() {
    let (items, unexpected) = spelled(
        "a//b",
        &[
            ("a//b", &[("a/b/sub", EntryKind::Directory)]),
            ("a/b/sub", &[("a/b/sub/leaf", EntryKind::File)]),
        ],
    )
    .await;
    assert!(
        unexpected.is_empty(),
        "asked for directories the backend never handed out: {unexpected:?}"
    );
    assert_eq!(failures(&items), ["a/b/sub"]);
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Entry(_))),
        "a misspelled child became an entry"
    );
    let checked = assert_entries_and_markers_agree(&items, "a//b");
    assert!(checked.is_empty(), "{checked:?}");
}

/// A traversal rooted at `./t` whose backend spells its children from the request, `./t/d`: the
/// root's spelling is part of every path below it, entries and descend targets alike.
#[tokio::test]
async fn a_dot_slash_root_is_part_of_every_path_below_it() {
    const ROOT: &str = "./t";
    let (items, unexpected) = spelled(
        ROOT,
        &[
            (ROOT, &[("./t/d", EntryKind::Directory)]),
            ("./t/d", &[("./t/d/leaf", EntryKind::File)]),
        ],
    )
    .await;
    assert!(
        unexpected.is_empty(),
        "asked for directories the backend never handed out: {unexpected:?}"
    );
    let checked = assert_entries_and_markers_agree(&items, ROOT);
    assert_eq!(
        checked,
        ["listed ./t/d"],
        "the descend target lost the requested spelling: {checked:?}"
    );
}

/// The same root with a backend that spells the children `t/d`, as a normalising one would: the
/// child is refused, not descended into under either spelling.
#[tokio::test]
async fn a_dot_slash_root_does_not_adopt_a_normalised_child_spelling() {
    const ROOT: &str = "./t";
    let (items, unexpected) = spelled(
        ROOT,
        &[
            (ROOT, &[("t/d", EntryKind::Directory)]),
            ("t/d", &[("t/d/leaf", EntryKind::File)]),
        ],
    )
    .await;
    assert!(
        unexpected.is_empty(),
        "asked for directories the backend never handed out: {unexpected:?}"
    );
    assert_eq!(failures(&items), ["t/d"]);
    let checked = assert_entries_and_markers_agree(&items, ROOT);
    assert!(checked.is_empty(), "{checked:?}");
}
