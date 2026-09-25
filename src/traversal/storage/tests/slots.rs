//! How a descend target's path is arrived at, and the spellings that punish guessing.
//!
//! A slot could store the child's name instead of its whole path and rebuild the path when the
//! cursor descends — the prefix is shared by every child of one directory, so storing it once
//! per directory rather than once per child is most of what a wide directory costs. The rebuild
//! only holds if the descend target can be derived from the frame's path and the child's name.
//! It cannot. **The descend target has to be the listing's own spelling, verbatim**, because a
//! backend is free to spell a child in a way no normalisation of the parent's path reaches:
//!
//! * [`StoragePath`] deliberately keeps `a//b` distinct from `a/b`, while a backend that
//!   resolves paths component-wise hands the collapsed spelling back for that directory's own
//!   children: the parent was handed out as `a//b`, its children come back as `a/b/sub`.
//!   `frame.path + separator + name` gives `a//b/sub`, which no entry carries.
//! * A caller may root the traversal at `./t`, and the backend still spells its children `t/d`,
//!   so the root's own spelling leaks into every descend target below it.
//!
//! When the two disagree, the directory's entry disagrees with its own completion items, and the
//! traversal reports a subtree under a path no entry ever carried. A consumer merging two sides
//! by path reads that as "the other side has something this side does not" and deletes. These
//! tests pin the agreement, so a rebuild that guesses fails here instead.
//!
//! The first fixture fails a naive rebuild twice over, at two different layers: at the root,
//! where `"" + "b"` misses the doubled separator, and again one level down, where the collapse
//! actually happens. Keeping both layers is what makes the case bite a rebuild that normalises
//! the frame path before joining — that one survives the root layer.
//!
//! The fixture answers a directory it never handed out with an *empty* listing and records the
//! request, and it refuses to serve more listings than the tree has directories. A fixture that
//! answered every unknown directory with directories again — the shape this file first had —
//! turns a wrong spelling into an endless tree; so does a fault that lands back on a directory
//! the fixture *does* know, such as a rebuild that drops the child's name and re-lists the
//! parent forever. Either way the fault has to surface as a named failure, not as a hung test.

use std::sync::{Mutex, PoisonError};

use super::*;
use crate::model::BackendSessionFailure;

/// More listings than any tree here has directories, and few enough that a fault which re-lists
/// one directory forever stops within a test's patience rather than the harness's.
const MAX_LISTINGS: usize = 16;

/// Lists a small tree, spelling each directory's children however the caller says — which is how
/// a real backend's own normalisation shows up: the parent hands out one spelling, that
/// directory's own listing uses another.
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
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            // Anything but a listing is a fixture bug rather than a traversal one. Record it the
            // same way an unknown directory is recorded, so it cannot hide inside an
            // `EntryFailure` the assertions never look at.
            self.served().unexpected.push(format!("{request:?}"));
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        let entries = {
            let mut served = self.served();
            served.listings += 1;
            if served.listings > MAX_LISTINGS {
                return Err(StorageRoleFailure::Session(
                    BackendSessionFailure::new(
                        Operation::Traverse,
                        FailureClass::Connectivity,
                        Transience::Permanent,
                        "fixture was asked for more listings than the tree has directories",
                    )
                    .unwrap_or_else(|error| panic!("{error}")),
                ));
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
        Ok(NamespaceResult::Entries(
            entries
                .iter()
                .map(|(path, kind)| descriptor(path, *kind))
                .collect(),
        ))
    }
}

/// Every directory with completion items also has an entry spelled identically.
///
/// Returns what it checked, as `"<kind> <path>"` and sorted: the kind so that one
/// `DirectoryListed` plus one `SubtreeComplete` cannot be confused with two of either, sorted
/// because this file pins spelling agreement and not order — order is pinned in `events.rs` and
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
            TraversalItem::SubtreeComplete(complete) => ("subtree", complete.path.as_str()),
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
    let mut session = source.traverse(TraversalRequest {
        root: path(root),
        order: TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(4)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(4)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: ObservationPlan::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions: TraversalVersions::Current,
    });
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

/// A backend that resolves paths component-wise: the root hands out `a//b`, and `a//b`'s own
/// listing spells its child `a/b/sub` — the separator collapsed. `frame.path + '/' + name` gives
/// `a//b/sub` there, and misses the doubled separator at the root a layer above, so both a naive
/// rebuild and one that normalises the frame path first are caught.
#[tokio::test]
async fn a_collapsed_separator_keeps_entries_and_markers_in_step() {
    let (items, unexpected) = spelled(
        "",
        &[
            ("", &[("a//b", EntryKind::Directory)]),
            ("a//b", &[("a/b/sub", EntryKind::Directory)]),
            ("a/b/sub", &[("a/b/sub/leaf", EntryKind::File)]),
        ],
    )
    .await;
    assert!(
        unexpected.is_empty(),
        "asked for directories the backend never handed out: {unexpected:?}"
    );
    let checked = assert_entries_and_markers_agree(&items, "");
    assert_eq!(
        checked,
        [
            "listed a//b",
            "listed a/b/sub",
            "subtree a//b",
            "subtree a/b/sub"
        ],
        "the collapsed spelling was not exercised: {checked:?}"
    );
}

/// A traversal rooted at `./t` whose backend spells the children `t/d`: the descend target must
/// keep the backend's spelling, not the root's. Rebuilding from the frame would descend into
/// `./t/d` and then report a subtree under a path no entry carries.
#[tokio::test]
async fn a_dot_slash_root_does_not_leak_into_descend_targets() {
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
    let checked = assert_entries_and_markers_agree(&items, ROOT);
    assert_eq!(
        checked,
        ["listed t/d", "subtree t/d"],
        "the descend target lost the backend's spelling: {checked:?}"
    );
}
