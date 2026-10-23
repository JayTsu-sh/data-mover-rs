use super::*;
use crate::storage::backends::cifs::source::entry_failure;
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::range_hash::RangeHash;

#[path = "published_checkpoint_tests.rs"]
mod published_checkpoint_tests;

#[path = "privacy_checkpoint_tests.rs"]
mod privacy_checkpoint_tests;

fn hash_request(
    backend: &BackendIdentity,
    size: u64,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    Ok(request(backend, size, BINDING, ResumeMode::Discover)?.with_checkpoint_hash(true))
}

fn arm(stage: &PreparedStage, bytes: &[u8]) -> TestResult {
    let mut hash = RangeHash::new(bytes.len() as u64);
    hash.observe(0, bytes)?;
    stage.enable_checkpoint(bytes.len() as u64, Bytes::from(hash.encode()?));
    Ok(())
}

fn checkpoint(protocol: &MemoryProtocol) -> Result<CheckpointData, Box<dyn std::error::Error>> {
    let pointer = DestinationPointer::decode(
        &file(protocol, &name(ArtifactKind::Pointer, false)).ok_or("pointer")?,
    )
    .map_err(|_| "pointer decode")?;
    let data = CheckpointData::decode(&pointer.checkpoint.ok_or("hash checkpoint")?)
        .map_err(|_| "checkpoint decode")?;
    assert_eq!(pointer.durable_prefix, Some(data.prefix()));
    Ok(data)
}

#[tokio::test]
async fn takeover_fences_hash_only_updates_and_release() -> TestResult {
    let (protocol, backend) = share()?;
    let first = adapter(&protocol, &backend);
    let held = first
        .prepare_at_destination(hash_request(&backend, 6)?)
        .await?;
    arm(&held, b"abcdef")?;
    first.persist_hash_checkpoint(&held).await?;
    let second = adapter(&protocol, &backend);
    let taken = second
        .prepare_at_destination(hash_request(&backend, 6)?)
        .await?;
    let pointer = name(ArtifactKind::Pointer, false);
    let expected = file(&protocol, &pointer);
    held.checkpoint_set_phase(CheckpointPhase::Published);
    assert!(first.persist_hash_checkpoint(&held).await.is_err());
    assert!(first.release_hash_checkpoint(&held).await.is_err());
    assert_eq!(file(&protocol, &pointer), expected);
    second.discard(taken).await?;
    Ok(())
}

#[tokio::test]
async fn hash_only_pointer_does_not_flush_or_promote_confirmed_writes() -> TestResult {
    let (protocol, backend) = share()?;
    let destination = adapter(&protocol, &backend);
    assert!(destination.supports_hash_checkpoint());
    let stage = destination
        .prepare_at_destination(hash_request(&backend, 12)?.with_recoverable(false))
        .await?;
    arm(&stage, b"abcdefghijkl")?;
    put(
        &protocol,
        &name(ArtifactKind::Stage, false),
        b"abcdefghijkl".to_vec(),
    );
    stage.confirmed_write(0, 4);
    stage.confirmed_write(8, 4);
    destination.persist_hash_checkpoint(&stage).await?;
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
    let saved = checkpoint(&protocol)?;
    assert!(saved.ranges.is_empty());
    assert_eq!(saved.prefix(), 0);
    assert!(destination.release_hash_checkpoint(&stage).await.is_err());
    destination.discard(stage).await?;
    Ok(())
}

async fn interrupt_sparse(
    first: &CifsStagedDestination,
    stage: &PreparedStage,
    protocol: &MemoryProtocol,
) -> TestResult {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    for (offset, data) in [(8, &b"ijkl"[..]), (0, &b"abcd"[..])] {
        sender
            .send(Ok(crate::storage::PositionedChunk {
                offset,
                data: Bytes::from_static(data),
            }))
            .await?;
    }
    let input = Box::pin(stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|item| (item, receiver))
    }));
    let cancel = async {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while checkpoint(protocol).is_ok_and(|data| data.prefix() == 0) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        sender
            .send(Err(entry_failure(
                stage.final_destination.path(),
                Operation::Read,
                FailureClass::Cancelled,
            )))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (written, sent) = tokio::join!(first.write_positioned(stage, input), cancel);
    sent?;
    assert!(written.is_err());
    Ok(())
}

async fn fill_hole(
    destination: &CifsStagedDestination,
    stage: &PreparedStage,
    protocol: &MemoryProtocol,
) -> TestResult {
    arm(stage, b"abcdefghijkl")?;
    protocol
        .writes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    let input = Box::pin(stream::iter([Ok(crate::storage::PositionedChunk {
        offset: 4,
        data: Bytes::from_static(b"efgh"),
    })]));
    assert_eq!(
        destination
            .write_positioned(stage, input)
            .await?
            .persisted_bytes,
        12
    );
    assert_eq!(
        *protocol
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [(4, 4)]
    );
    Ok(())
}

#[tokio::test]
async fn sparse_checkpoint_survives_error_and_prepare_then_fills_only_the_hole() -> TestResult {
    let (protocol, backend) = share()?;
    let first = adapter(&protocol, &backend);
    let mut stage = first
        .prepare_at_destination(hash_request(&backend, 12)?.with_recoverable(false))
        .await?;
    arm(&stage, b"abcdefghijkl")?;
    stage.deferred_checkpoint = Some(crate::storage::roles::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: 12,
    });
    first.persist_hash_checkpoint(&stage).await?;
    interrupt_sparse(&first, &stage, &protocol).await?;
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..4, 8..12]);
    drop(stage);
    drop(first);
    let second = adapter(&protocol, &backend);
    let stage = second
        .prepare_at_destination(hash_request(&backend, 12)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: 8 });
    assert_eq!(stage.prepare_fact().reused_bytes(), 8);
    assert_eq!(stage.write_offset, 4);
    assert!(!stage.checkpoint_enabled());
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..4, 8..12]);
    fill_hole(&second, &stage, &protocol).await?;
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..12]);
    assert_eq!(
        file(&protocol, &name(ArtifactKind::Stage, false)),
        Some(b"abcdefghijkl".to_vec())
    );
    second.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn failed_flush_never_promotes_confirmed_ranges() -> TestResult {
    let (protocol, backend) = share()?;
    let destination = adapter(&protocol, &backend);
    let stage = destination
        .prepare_at_destination(hash_request(&backend, 6)?)
        .await?;
    arm(&stage, b"abcdef")?;
    destination.persist_hash_checkpoint(&stage).await?;
    protocol.fail_flush.store(true, Ordering::SeqCst);
    assert!(write(&destination, &stage, b"abcdef").await.is_err());
    assert!(stage.checkpoint_ranges().is_empty());
    assert!(checkpoint(&protocol)?.ranges.is_empty());
    protocol.fail_flush.store(false, Ordering::SeqCst);
    destination.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn published_hash_pointer_is_retained_until_retryable_release() -> TestResult {
    for lost_reply in [false, true] {
        let (protocol, backend) = share()?;
        let destination = adapter(&protocol, &backend);
        let stage = destination
            .prepare_at_destination(hash_request(&backend, 6)?)
            .await?;
        arm(&stage, b"abcdef")?;
        destination.persist_hash_checkpoint(&stage).await?;
        write(&destination, &stage, b"abcdef").await?;
        stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
        destination.persist_hash_checkpoint(&stage).await?;
        protocol
            .fail_rename_after_commit
            .store(lost_reply, Ordering::SeqCst);
        publish(&destination, &stage, b"abcdef")
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(checkpoint(&protocol)?.phase, CheckpointPhase::Published);
        assert_eq!(
            artifacts_left(&protocol),
            [name(ArtifactKind::Pointer, false)]
        );
        protocol.fail_pointer_delete.store(true, Ordering::SeqCst);
        assert!(destination.release_hash_checkpoint(&stage).await.is_err());
        assert!(file(&protocol, &name(ArtifactKind::Pointer, false)).is_some());
        destination.release_hash_checkpoint(&stage).await?;
        destination.release_hash_checkpoint(&stage).await?;
        assert!(artifacts_left(&protocol).is_empty());
        assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    }
    Ok(())
}
