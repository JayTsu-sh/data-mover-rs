use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ListingOutcome, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource,
};
use crate::model::{ObservedEntry, StoragePath};
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::backends::hdfs::{connect, test_identity};
use crate::storage::listing::DEFAULT_PAGE_HINT;

#[tokio::test]
async fn hdfs_facts_survive_snapshot_without_backend_requery()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"payload"))
        .await;
    let storage = connect(protocol, test_identity("hdfs-traversal")?)?;
    let traversal = StorageTraversalSource::new(&storage)?;
    let mut session = traversal.traverse(
        TraversalRequest::new(StoragePath::root())
            .order(TraversalOrder::Blocks)
            .max_inflight_operations(NonZeroUsize::new(2).ok_or("invalid inflight")?)
            .cancel(CancellationToken::new()),
    );
    let Some(TraversalItem::Entry(entry)) = session.next_item().await else {
        return Err("HDFS traversal did not return an entry".into());
    };
    let rebuilt = ObservedEntry::decode_snapshot(
        entry.encode_snapshot().as_bytes(),
        entry.source_identity().backend(),
    )?;
    assert_eq!(*entry, rebuilt);
    while let Some(item) = session.next_item().await {
        assert!(crate::traversal::is_marker(&item));
    }
    session.finish().await?;
    Ok(())
}

/// A child the HDFS listing cannot describe is one child failure at its own path: the siblings
/// are still delivered, and the directory's listing is still complete, with the child counted as
/// failed.
#[tokio::test]
async fn an_undescribable_child_is_a_child_failure_in_a_complete_listing()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryHdfs::default());
    for name in ["a", "bad", "c"] {
        protocol.insert(name, Bytes::from_static(b"payload")).await;
    }
    protocol.make_undescribable("bad").await;
    let storage = connect(protocol, test_identity("hdfs-traversal")?)?;
    let traversal = StorageTraversalSource::new(&storage)?;
    let mut session = traversal.traverse(
        TraversalRequest::new(StoragePath::root())
            .order(TraversalOrder::Blocks)
            .max_inflight_operations(NonZeroUsize::new(2).ok_or("invalid inflight")?)
            .cancel(CancellationToken::new()),
    );
    let mut entries = Vec::new();
    let mut failures = Vec::new();
    let mut listing = None;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => entries.push(entry.path().to_string()),
            TraversalItem::Failure(failure) => {
                failures.push((failure.scope, failure.path.to_string()));
            }
            TraversalItem::DirectoryListed(listed) => {
                listing = Some((listed.outcome, listed.counts));
            }
            TraversalItem::Checkpoint(_) => {}
        }
    }
    session.finish().await?;
    // A block is in the order HDFS listed it, which the in-memory store does not fix.
    entries.sort();
    assert_eq!(entries, ["a", "c"]);
    assert_eq!(failures, [(FailureScope::Child, "bad".to_owned())]);
    let Some((outcome, counts)) = listing else {
        return Err("the root was not reported".into());
    };
    assert_eq!(outcome, ListingOutcome::Complete);
    assert!(counts.is_balanced(), "{counts:?}");
    assert_eq!(
        (counts.listed, counts.emitted, counts.failed_children),
        (3, 2, 1)
    );

    Ok(())
}

/// An HDFS listing that fails after its first page (P7 C8, §8): the children read before it are
/// emitted, then one listing failure carrying how many were delivered, and the block closes
/// `Interrupted`, not `Complete` and not `Unreadable`.
#[tokio::test]
async fn a_listing_failing_after_the_first_page_is_interrupted()
-> Result<(), Box<dyn std::error::Error>> {
    // The failure falls in the second page of the traversal's listings.
    let fail_at = DEFAULT_PAGE_HINT.get() + 26;
    let protocol = Arc::new(MemoryHdfs::default());
    for index in 0..fail_at + 50 {
        protocol
            .insert(&format!("f{index:05}"), Bytes::from_static(b"x"))
            .await;
    }
    protocol.fail_listings_after(fail_at);
    let storage = connect(protocol, test_identity("hdfs-traversal")?)?;
    let traversal = StorageTraversalSource::new(&storage)?;
    let mut session = traversal.traverse(
        TraversalRequest::new(StoragePath::root())
            .order(TraversalOrder::Blocks)
            .max_inflight_operations(NonZeroUsize::new(4).ok_or("invalid inflight")?)
            .cancel(CancellationToken::new()),
    );
    let (mut entries, mut failures, mut outcome) = (0_usize, Vec::new(), None);
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(_) => entries += 1,
            TraversalItem::Failure(failure) => failures.push((failure.scope, failure.delivered)),
            TraversalItem::DirectoryListed(listed) => outcome = Some(listed.outcome),
            TraversalItem::Checkpoint(_) => {}
        }
    }
    let completion = match session.finish().await? {
        TraversalOutcome::Completed(completion) => completion,
        TraversalOutcome::Cancelled => return Err("unexpected cancellation".into()),
    };
    assert_eq!(entries, fail_at);
    assert_eq!(
        failures,
        [(FailureScope::Listing, Some(u64::try_from(fail_at)?))]
    );
    assert_eq!(outcome, Some(ListingOutcome::Interrupted));
    assert_eq!(completion.totals.interrupted_listings, 1);
    assert!(completion.totals.is_balanced());
    Ok(())
}
