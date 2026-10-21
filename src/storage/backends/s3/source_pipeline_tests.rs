use bytes::Bytes;
use futures::StreamExt as _;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use super::{
    S3Protocol, connect,
    tests::{MemoryS3, RangeGate, identity},
};
use crate::model::{SourceVersion, StoragePath};
use crate::storage::ReadRequest;

#[test]
fn backend_defaults_use_the_measured_two_read_four_write_baseline() {
    let protocol = MemoryS3::default();
    assert_eq!(protocol.read_concurrency(), 2);
    assert_eq!(protocol.write_concurrency(), 4);
}

#[tokio::test]
async fn parallel_range_reads_are_bounded_and_delivered_in_byte_order()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::with_read_window(4));
    let payload = Bytes::from((0_u8..32).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let gate = Arc::new(RangeGate {
        offset: 0,
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    *protocol.range_gate.lock().await = Some(gate.clone());
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let descriptor = source.describe(&StoragePath::new("source")?).await?;
    let mut stream = source
        .read(ReadRequest {
            path: descriptor.path,
            range: Some(0..32),
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 4,
            read_inflight: 128,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let task = tokio::spawn(async move {
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk?);
        }
        Ok::<_, crate::storage::StorageRoleFailure>(chunks)
    });
    tokio::time::timeout(Duration::from_secs(5), gate.started.notified()).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while protocol.range_observations.lock().await.len() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // Later reads completed, but the first range still fences delivery and further prefetch.
    assert_eq!(protocol.range_observations.lock().await.len(), 4);
    assert!(!task.is_finished());
    gate.release.notify_one();
    let chunks = tokio::time::timeout(Duration::from_secs(5), task).await???;
    assert_eq!(chunks.len(), 8);
    for (chunk, expected) in chunks.iter().zip(payload.chunks(4)) {
        assert_eq!(chunk.as_ref(), expected);
    }
    Ok(())
}
