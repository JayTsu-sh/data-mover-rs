//! Directory objects (P7 F5): a directory's own object `p/d/`, handed over by the listing of `p/d`
//! as the child with the empty name, is a file entry of that block — counted, filtered and cut by
//! `max_depth` like its siblings, named after its directory — except for the traversal root's own
//! object, which is not part of the traversal.

use std::sync::{Mutex, PoisonError};

use tokio_util::sync::CancellationToken;

use super::events::{assert_well_formed, drain};
use super::*;
use crate::traversal::{
    ScanTotals, TraversalCandidate, TraversalDecision, TraversalFilter, TraversalOrder,
};

/// ```text
/// <root>  p/  top
/// p/      p/ (own object, 5 B)  f  d/
/// p/d/    p/d/ (own object, 0 B: a folder marker)  x
/// ```
struct ObjectsNamespace;

fn own_object(key: &str, size: u64) -> SourceDescriptor {
    let mut own = descriptor(key, EntryKind::File);
    own.size = Some(size);
    own
}

#[async_trait]
impl Namespace for ObjectsNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let children = match directory.as_str() {
            "" => vec![
                descriptor("p", EntryKind::Directory),
                descriptor("top", EntryKind::File),
            ],
            "p" => vec![
                own_object("p/", 5),
                descriptor("p/f", EntryKind::File),
                descriptor("p/d", EntryKind::Directory),
            ],
            "p/d" => vec![own_object("p/d/", 0), descriptor("p/d/x", EntryKind::File)],
            other => panic!("{other:?} must not be listed"),
        };
        entries(&directory, children)
    }
}

fn traverse(root: &str, max_depth: Option<usize>) -> TraversalSession {
    let mut request = TraversalRequest::new(path(root))
        .order(TraversalOrder::Blocks)
        .cancel(CancellationToken::new());
    request.max_depth = max_depth.and_then(NonZeroUsize::new);
    StorageTraversalSource::with_roles(Arc::new(ObjectsNamespace), Arc::new(FakeMetadata))
        .traverse(request)
}

/// Entries as `path kind size`, and child failures as `failure path class`.
fn rows(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(format!(
                "{} {:?} {:?}",
                entry.path(),
                entry.kind(),
                entry.size()
            )),
            TraversalItem::Failure(failure) => Some(format!(
                "failure {} {:?}",
                failure.path,
                failure.failure.class()
            )),
            _ => None,
        })
        .collect()
}

fn totals(outcome: &TraversalOutcome) -> ScanTotals {
    match outcome {
        TraversalOutcome::Completed(completion) => completion.totals,
        TraversalOutcome::Cancelled => panic!("the traversal was not cancelled"),
    }
}

#[tokio::test]
async fn directory_objects_are_entries_counted_as_files() {
    let (items, outcome) = drain(traverse("", None)).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(
        rows(&items),
        [
            "p Directory None",
            "top File Some(3)",
            "p/f File Some(3)",
            "p/d Directory None",
            "p/ File Some(5)",
            "p/d/x File Some(3)",
            "p/d/ File Some(0)",
        ]
    );
    let entries: Vec<bool> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.is_directory_object()),
            _ => None,
        })
        .collect();
    assert_eq!(entries, [false, false, false, false, true, false, true]);
    assert_eq!(
        totals(&outcome),
        ScanTotals {
            listed: 7,
            emitted: 7,
            files: 5,
            directories: 2,
            bytes: 14,
            all_versions_bytes: 14,
            ..ScanTotals::default()
        }
    );
}

/// The root `p`'s own object would have the root's own relative path (`""`): it holds content, so
/// it is that key's `Unsupported` child failure, reported, never emitted. The root `p/d`'s own
/// object is a zero-byte folder marker: dropped, uncounted.
#[tokio::test]
async fn the_traversal_roots_own_object_is_reported_not_emitted() {
    let (items, outcome) = drain(traverse("p", None)).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(
        rows(&items),
        [
            "failure p/ Unsupported",
            "p/f File Some(3)",
            "p/d Directory None",
            "p/d/x File Some(3)",
            "p/d/ File Some(0)",
        ]
    );
    let scan = totals(&outcome);
    assert_eq!((scan.listed, scan.emitted, scan.failed_children), (5, 4, 1));

    let (items, outcome) = drain(traverse("p/d", None)).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(rows(&items), ["p/d/x File Some(3)"]);
    let scan = totals(&outcome);
    assert_eq!((scan.listed, scan.emitted, scan.failed_children), (1, 1, 0));
}

/// Records every candidate the traversal asks about, and hides nothing (still consulted below
/// every directory).
#[derive(Debug, Default)]
struct Recording(Mutex<Vec<String>>);

impl TraversalFilter for Recording {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "{} name={} {:?}",
                candidate.path, candidate.name, candidate.kind
            ));
        TraversalDecision {
            emit: true,
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// D7: the filter sees `p/d/` as a file named after its directory (`d`, never empty), at the path
/// with its trailing `/`, relative to the traversal root.
#[tokio::test]
async fn filters_see_a_directory_object_by_its_directory_name() {
    let recording = Arc::new(Recording::default());
    let mut request = TraversalRequest::new(path("p"))
        .order(TraversalOrder::Blocks)
        .cancel(CancellationToken::new());
    request.filter = Some(Arc::clone(&recording) as Arc<dyn TraversalFilter>);
    let source =
        StorageTraversalSource::with_roles(Arc::new(ObjectsNamespace), Arc::new(FakeMetadata));
    let (items, outcome) = drain(source.traverse(request)).await;
    assert_well_formed(&items, &outcome);
    let mut seen = recording
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    seen.sort();
    assert_eq!(
        seen,
        [
            "d name=d Directory",
            "d/ name=d File",
            "d/x name=x File",
            "f name=f File",
        ]
    );
}

/// The object `p/d/` sits at its directory's children's depth: `max_depth` cuts it together with
/// the rest of `p/d`'s block.
#[tokio::test]
async fn max_depth_cuts_a_directory_object_with_its_directory() {
    let (items, outcome) = drain(traverse("", Some(1))).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(rows(&items), ["p Directory None", "top File Some(3)"]);
    let (items, outcome) = drain(traverse("", Some(2))).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(
        rows(&items),
        [
            "p Directory None",
            "top File Some(3)",
            "p/f File Some(3)",
            "p/d Directory None",
            "p/ File Some(5)",
        ]
    );
}

/// The root `r` lists its own key on two pages (a history split across them: content, a zero-byte
/// version, a marker without size): one failure, once; nothing of it is emitted or counted twice.
struct TwoPages;

#[async_trait]
impl Namespace for TwoPages {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let mut first = ListingPage::from_descriptors(
            &directory,
            vec![descriptor("r/a", EntryKind::File)],
            vec![],
            0,
        );
        first.push_directory_object(&directory, own_object("r/", 5));
        let mut marker = own_object("r/", 0);
        marker.size = None;
        let mut second = ListingPage::default();
        for record in [own_object("r/", 0), marker, own_object("r/", 3)] {
            second.push_directory_object(&directory, record);
        }
        ListingStream::from_pages(directory, [Ok(first), Ok(second)])
    }
}

#[tokio::test]
async fn the_traversal_roots_own_object_is_reported_once_across_pages() {
    let request = TraversalRequest::new(path("r"))
        .order(TraversalOrder::Blocks)
        .cancel(CancellationToken::new());
    let source = StorageTraversalSource::with_roles(Arc::new(TwoPages), Arc::new(FakeMetadata));
    let (items, outcome) = drain(source.traverse(request)).await;
    assert_well_formed(&items, &outcome);
    assert_eq!(rows(&items), ["failure r/ Unsupported", "r/a File Some(3)"]);
    let scan = totals(&outcome);
    assert_eq!((scan.listed, scan.emitted, scan.failed_children), (2, 1, 1));
}
