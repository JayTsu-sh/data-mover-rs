//! Traversals over the CIFS role listing (P7 C9, C10): parked listings stay within the namespace's
//! limit of open directory handles in both orders, every handle is closed by the end, and a
//! directory whose listing fails after its first page is `Interrupted` after the children it
//! delivered.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ListingOutcome, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource as _,
};
use crate::model::{EntryKind, MetadataObservations, ObservationPlan, StoragePath};
use crate::storage::backends::cifs::listing_fake::{FakeCifs, Fault};
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

/// Wide subdirectories under the root, each several listing pages long, so a prefetched one parks
/// on its open handle after its first pages.
const WIDE_DIRECTORIES: usize = 20;
const WIDE_FILES: usize = 4500;

fn wide_tree() -> FakeCifs {
    let subdirectories = (0..WIDE_DIRECTORIES)
        .map(|index| (format!("d{index:02}"), EntryKind::Directory))
        .collect();
    (0..WIDE_DIRECTORIES).fold(
        FakeCifs::new().directory("", subdirectories),
        |fake, index| fake.flat(&format!("d{index:02}"), WIDE_FILES),
    )
}

/// Runs a traversal of the whole fake, taking items slowly for a while so prefetched listings
/// park, and returns every item.
async fn traverse(fake: &Arc<FakeCifs>, concurrency: usize) -> Result<Vec<TraversalItem>> {
    traverse_in(fake, concurrency, TraversalOrder::Blocks).await
}

async fn traverse_in(
    fake: &Arc<FakeCifs>,
    concurrency: usize,
    order: TraversalOrder,
) -> Result<Vec<TraversalItem>> {
    let source = StorageTraversalSource::with_roles(fake.role(), Arc::new(NoMetadata));
    let slots = NonZeroUsize::new(concurrency).ok_or("concurrency is not zero")?;
    let mut session = source.traverse(
        TraversalRequest::new(StoragePath::root())
            .order(order)
            .max_inflight_operations(slots)
            .cancel(CancellationToken::new()),
    );
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        if items.len() < 50 {
            // A slow consumer at first: prefetch runs as far ahead as it may.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        items.push(item);
    }
    match session.finish().await? {
        TraversalOutcome::Completed(_) => Ok(items),
        TraversalOutcome::Cancelled => Err("the traversal was cancelled".into()),
    }
}

fn entry_paths(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.path().as_str().to_owned()),
            _ => None,
        })
        .collect()
}

/// However many directories prefetch reaches, at most the namespace's limit of listings started
/// ahead of the cursor are unfinished at once, plus the one the cursor reads: every open CIFS
/// directory handle is one of them. Every handle is closed by the end, and the output is the same
/// block order at every concurrency.
#[tokio::test]
async fn parked_listings_stay_within_the_handle_limit() -> Result {
    let limit = Arc::new(FakeCifs::new())
        .role()
        .parked_listing_limit()
        .ok_or("CIFS limits parked listings")?
        .get();
    let mut orders = Vec::new();
    for concurrency in [1, 4, 32] {
        let fake = Arc::new(wide_tree());
        let items = traverse(&fake, concurrency).await?;
        let paths = entry_paths(&items);
        assert_eq!(paths.len(), WIDE_DIRECTORIES * (WIDE_FILES + 1));
        let counts = &fake.counts;
        let peak = FakeCifs::count(&counts.peak_open);
        assert!(
            peak <= limit + 1,
            "concurrency {concurrency}: {peak} directory handles open at once"
        );
        if concurrency == 32 {
            assert!(peak > 2, "prefetch never ran ahead: {peak}");
        }
        assert_eq!(FakeCifs::count(&counts.open_now), 0, "a handle left open");
        assert_eq!(
            FakeCifs::count(&counts.opens),
            WIDE_DIRECTORIES + 1,
            "each directory listed once"
        );
        orders.push(paths);
    }
    assert!(orders.windows(2).all(|pair| pair[0] == pair[1]));
    Ok(())
}

/// Unordered keeps every worker's listing open while it waits for the consumer, so any open
/// listing may be parked: it lists at most the namespace's limit of directories at once, whatever
/// the concurrency. Every handle is closed by the end and the entries are the same as Blocks'.
///
/// Fault injection: size the workers by the concurrency alone and c = 32 opens 21 handles — red.
#[tokio::test]
async fn unordered_listings_stay_within_the_handle_limit() -> Result {
    let limit = Arc::new(FakeCifs::new())
        .role()
        .parked_listing_limit()
        .ok_or("CIFS limits parked listings")?
        .get();
    let blocks = {
        let mut paths = entry_paths(&traverse(&Arc::new(wide_tree()), 4).await?);
        paths.sort();
        paths
    };
    for concurrency in [1, 4, 32] {
        let fake = Arc::new(wide_tree());
        let items = traverse_in(&fake, concurrency, TraversalOrder::Unordered).await?;
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, TraversalItem::DirectoryListed(_)))
        );
        let mut paths = entry_paths(&items);
        paths.sort();
        assert_eq!(paths, blocks, "concurrency {concurrency}");
        let counts = &fake.counts;
        let peak = FakeCifs::count(&counts.peak_open);
        assert!(
            peak <= limit,
            "concurrency {concurrency}: {peak} directory handles open at once"
        );
        if concurrency == 32 {
            assert!(peak > 2, "the workers never listed side by side: {peak}");
        }
        assert_eq!(FakeCifs::count(&counts.open_now), 0, "a handle left open");
        assert_eq!(
            FakeCifs::count(&counts.opens),
            WIDE_DIRECTORIES + 1,
            "each directory listed once"
        );
    }
    Ok(())
}

/// A listing that fails after its first page: its delivered children are entries, then the
/// listing's failure with the count delivered, then `Interrupted`.
#[tokio::test]
async fn a_listing_failing_after_its_first_page_is_interrupted() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("", 1100));
    fake.fault(Fault {
        path: String::new(),
        after: 1050,
        error: || smb_domain::Error::StaleObject,
        times: 1,
    });
    let items = traverse(&fake, 4).await?;
    assert_eq!(entry_paths(&items).len(), 1050);
    let failure = items
        .iter()
        .find_map(|item| match item {
            TraversalItem::Failure(failure) if failure.scope == FailureScope::Listing => {
                Some(failure)
            }
            _ => None,
        })
        .ok_or("no listing failure")?;
    assert_eq!(failure.delivered, Some(1050));
    let root = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(listed) if listed.path == StoragePath::root() => {
            Some(listed.outcome)
        }
        _ => None,
    });
    assert_eq!(root, Some(ListingOutcome::Interrupted));
    assert_eq!(FakeCifs::count(&fake.counts.opens), 1, "not reopened");
    assert_eq!(FakeCifs::count(&fake.counts.open_now), 0);
    Ok(())
}
