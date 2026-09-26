//! Traversals over the NFS role listing (P7 C7): descending follows each directory's descent
//! hint, with no path resolution per directory, and a directory whose listing fails after its
//! first page is `Interrupted` after the children it delivered.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ListingOutcome, StorageTraversalSource, TraversalItem, TraversalOutcome,
    TraversalRequest, TraversalSource as _,
};
use crate::model::{
    EntryKind, FailureClass, MetadataObservations, ObservationPlan, StoragePath, Transience,
};
use crate::storage::backends::nfs::listing_fake::{FakeNfs, Fault, locked};
use crate::storage::backends::nfs::namespace::{NfsReadFailure, NfsRestart};
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The listing carries every timestamp the default plan asks for: nothing is observed.
struct NoMetadata;

#[async_trait]
impl Metadata for NoMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        _plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        panic!("{path} was observed: the listing carried everything")
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        unreachable!("traversal never applies metadata")
    }
}

fn children(entries: &[(&str, EntryKind)]) -> Vec<(String, EntryKind)> {
    entries
        .iter()
        .map(|(name, kind)| ((*name).to_owned(), *kind))
        .collect()
}

/// `a/{a1/{x,y}, z}`, `b/{w}`, `f` under the root.
fn tree(fake: FakeNfs) -> FakeNfs {
    let (dir, file) = (EntryKind::Directory, EntryKind::File);
    fake.directory("", children(&[("a", dir), ("b", dir), ("f", file)]))
        .directory("a", children(&[("a1", dir), ("z", file)]))
        .directory("a/a1", children(&[("x", file), ("y", file)]))
        .directory("b", children(&[("w", file)]))
}

async fn traverse(fake: &Arc<FakeNfs>, concurrency: usize) -> Result<Vec<TraversalItem>> {
    let source =
        StorageTraversalSource::with_roles(Arc::new(fake.namespace()), Arc::new(NoMetadata));
    let slots = NonZeroUsize::new(concurrency).ok_or("concurrency is not zero")?;
    let mut session = source.traverse(
        TraversalRequest::new(StoragePath::root())
            .max_inflight_operations(slots)
            .max_buffered_items(slots)
            .cancel(CancellationToken::new()),
    );
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    match session.finish().await? {
        TraversalOutcome::Completed(_) => Ok(items),
        TraversalOutcome::Cancelled => Err("the traversal was cancelled".into()),
    }
}

fn entries(items: &[TraversalItem]) -> usize {
    items
        .iter()
        .filter(|item| matches!(item, TraversalItem::Entry(_)))
        .count()
}

/// Only the root is resolved by path; every other directory is read with the handle its parent's
/// listing handed back, at any concurrency.
#[tokio::test]
async fn descending_reads_the_hinted_handles_without_resolving_paths() -> Result {
    for concurrency in [1, 4, 32] {
        let fake = Arc::new(tree(FakeNfs::new(2)));
        let items = traverse(&fake, concurrency).await?;
        assert_eq!(entries(&items), 8);
        assert_eq!(
            FakeNfs::count(&fake.lookups),
            1,
            "only the root is looked up"
        );
        let mut reads = locked(&fake.read_with).clone();
        reads.sort();
        let expected: Vec<_> = ["", "a", "a/a1", "b"]
            .into_iter()
            .map(|path| (path.to_owned(), fake.handle(path)))
            .collect();
        assert_eq!(reads, expected, "concurrency {concurrency}");
    }
    Ok(())
}

/// A hint that went stale between the parent's listing and the child's costs that child one
/// path resolution, and nothing else changes.
#[tokio::test]
async fn a_stale_hint_costs_one_resolution_of_that_directory() -> Result {
    let fake = Arc::new(tree(FakeNfs::new(2)).listed_stale("a/a1"));
    let items = traverse(&fake, 4).await?;
    assert_eq!(entries(&items), 8);
    assert_eq!(FakeNfs::count(&fake.lookups), 2, "the root and a/a1");
    assert_eq!(FakeNfs::count(&fake.reads_started), 5, "a/a1 read twice");
    Ok(())
}

/// A listing that fails after its first page: its delivered children are entries (a delivered
/// subdirectory is still descended into), then the listing's failure with the count delivered,
/// then `Interrupted`.
#[tokio::test]
async fn a_listing_failing_after_its_first_page_is_interrupted() -> Result {
    let mut names: Vec<(String, EntryKind)> = (0..1100)
        .map(|index| (format!("f{index:04}"), EntryKind::File))
        .collect();
    names.insert(3, ("sub".to_owned(), EntryKind::Directory));
    let fake = Arc::new(
        FakeNfs::new(100)
            .directory("", names)
            .directory("sub", children(&[("inner", EntryKind::File)])),
    );
    fake.fault(Fault {
        path: String::new(),
        after: 1050,
        failure: NfsReadFailure {
            failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
            restart: NfsRestart::Relookup,
        },
        times: 1,
    });
    let items = traverse(&fake, 4).await?;
    assert_eq!(
        entries(&items),
        1051,
        "1050 read before the failure, and sub/inner"
    );
    let listing_failure = items.iter().find_map(|item| match item {
        TraversalItem::Failure(failure) if failure.scope == FailureScope::Listing => Some(failure),
        _ => None,
    });
    let failure = listing_failure.ok_or("no listing failure")?;
    assert_eq!(failure.delivered, Some(1050));
    let root = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(listed) if listed.path == StoragePath::root() => {
            Some(listed.outcome)
        }
        _ => None,
    });
    assert_eq!(root, Some(ListingOutcome::Interrupted));
    assert_eq!(
        FakeNfs::count(&fake.reads_started),
        2,
        "the root once, sub once"
    );
    Ok(())
}
