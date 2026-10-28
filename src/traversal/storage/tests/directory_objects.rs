//! Generic traversal consumes the namespace's logical projection before filtering and counting.
//! These fixed pages omit folded markers; S3 root-object classification is covered through the
//! real S3 adapter in `s3_directory_object_tests`.

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
    fn capabilities(&self) -> crate::storage::NamespaceCapabilities {
        crate::storage::NamespaceCapabilities {
            directory_objects: true,
            ..crate::storage::NamespaceCapabilities::default()
        }
    }

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

    fn list_for_traversal(&self, request: ListRequest, is_root: bool) -> ListingStream {
        // Fixed logical pages supplied by this fixture, without a second classification algorithm.
        let children = match (request.path.as_str(), is_root) {
            ("p", true) => vec![
                descriptor("p/f", EntryKind::File),
                descriptor("p/d", EntryKind::Directory),
            ],
            ("p/d", _) => vec![descriptor("p/d/x", EntryKind::File)],
            _ => return self.list(request),
        };
        entries(&request.path, children)
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
        ]
    );
    let entries: Vec<bool> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.is_directory_object()),
            _ => None,
        })
        .collect();
    assert_eq!(entries, [false, false, false, false, true, false]);
    assert_eq!(
        totals(&outcome),
        ScanTotals {
            listed: 6,
            emitted: 6,
            files: 4,
            directories: 2,
            bytes: 14,
            all_versions_bytes: 14,
            ..ScanTotals::default()
        }
    );
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

/// The filter sees the directory but never its folded marker; paths stay relative to the
/// specified traversal root, not the bucket root.
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
        ["d name=d Directory", "d/x name=x File", "f name=f File",]
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
