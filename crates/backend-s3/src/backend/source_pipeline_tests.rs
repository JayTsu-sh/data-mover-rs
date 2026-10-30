use bytes::Bytes;
use futures::StreamExt as _;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use super::{
    S3Protocol, connect,
    tests::{MemoryS3, RangeGate, identity},
};
use crate::model::{SourceVersion, StoragePath};
use crate::storage::{ReadRequest, SourceQosGroup, SourceQosPolicy};

#[test]
fn backend_defaults_use_the_measured_two_read_four_write_baseline() {
    let protocol = MemoryS3::default();
    assert_eq!(protocol.read_concurrency(), 2);
    assert_eq!(protocol.write_concurrency(), 4);
}

#[tokio::test(start_paused = true)]
async fn qos_admission_is_at_get_dispatch_even_when_the_first_get_is_parked()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"abcdefgh"));
    let gate = Arc::new(RangeGate {
        offset: 0,
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    *protocol.range_gate.lock().await = Some(gate.clone());
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let descriptor = source.describe(&StoragePath::new("source")?).await?;
    let group = SourceQosGroup::new(SourceQosPolicy::new(
        Some((4, 4, Duration::ZERO)),
        4,
        Some((2, 2, Duration::ZERO)),
    )?);
    let started = tokio::time::Instant::now();
    let mut stream = source
        .read(ReadRequest {
            path: descriptor.path,
            range: Some(0..8),
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 4,
            read_inflight: 2,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: Some(group.transfer_budget()),
            version: SourceVersion::Current,
        })
        .await?;
    let task = tokio::spawn(async move {
        let mut payload = Vec::new();
        while let Some(chunk) = stream.next().await {
            payload.extend_from_slice(&chunk?);
        }
        Ok::<_, crate::storage::StorageRoleFailure>(payload)
    });
    tokio::time::timeout(Duration::from_secs(5), gate.started.notified()).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let requests = protocol.range_dispatches.lock().await.clone();
    assert_eq!(requests.len(), 2, "QoS must preserve the two-GET window");
    assert!(requests[0].0.duration_since(started) >= Duration::from_secs(1));
    assert!(
        requests[1].0.duration_since(requests[0].0) >= Duration::from_secs(1),
        "queued permits must not turn into simultaneous GET dispatches"
    );
    assert!(!task.is_finished());
    gate.release.notify_one();
    assert_eq!(task.await??, b"abcdefgh");
    Ok(())
}

#[tokio::test]
async fn parallel_range_reads_are_bounded_and_delivered_in_byte_order()
-> Result<(), Box<dyn std::error::Error>> {
    for limited in [false, true] {
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
        let group = SourceQosGroup::new(SourceQosPolicy::new(
            Some((1_000_000_000, 1_000_000_000, Duration::ZERO)),
            4,
            Some((1_000_000, 1_000_000, Duration::ZERO)),
        )?);
        let mut stream = source
            .read(ReadRequest {
                path: descriptor.path,
                range: Some(0..32),
                expected_source: Some(descriptor.source_identity),
                maximum_chunk_bytes: 4,
                read_inflight: 128,
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: limited.then(|| group.transfer_budget()),
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
    }
    Ok(())
}
