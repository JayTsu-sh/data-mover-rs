use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{
    StorageTraversalSource, TraversalItem, TraversalOrder, TraversalRequest, TraversalSource,
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
    });
    let Some(TraversalItem::Entry(entry)) = session.next_item().await else {
        return Err("HDFS traversal did not return an entry".into());
    };
    let rebuilt = ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes())?;
    assert_eq!(*entry, rebuilt);
    assert!(session.next_item().await.is_none());
    session.finish().await?;
    Ok(())
}
