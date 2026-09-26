use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ListingOutcome, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalRequest, TraversalSource, TraversalVersions,
};
use crate::model::{ObservationPlan, ObservedEntry, StoragePath};
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::backends::hdfs::{connect, test_identity};

#[tokio::test]
async fn hdfs_facts_survive_snapshot_without_backend_requery()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"payload"))
        .await;
    let storage = connect(protocol, test_identity("hdfs-traversal")?)?;
    let traversal = StorageTraversalSource::new(&storage)?;
    let mut session = traversal.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(2).ok_or("invalid inflight")?,
        max_buffered_items: NonZeroUsize::new(2).ok_or("invalid buffer")?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions: TraversalVersions::Current,
    });
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
    let mut session = traversal.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::NameBytes,
        max_inflight_operations: NonZeroUsize::new(2).ok_or("invalid inflight")?,
        max_buffered_items: NonZeroUsize::new(8).ok_or("invalid buffer")?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions: TraversalVersions::Current,
    });
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
