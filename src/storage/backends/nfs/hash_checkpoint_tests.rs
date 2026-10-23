use super::*;
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::range_hash::RangeHash;

#[path = "published_checkpoint_tests.rs"]
mod published_checkpoint_tests;

fn hash_request(
    backend: &BackendIdentity,
    size: usize,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    Ok(request(backend, size, BINDING, ResumeMode::Discover)?.with_checkpoint_hash(true))
}

fn arm(stage: &PreparedStage, bytes: &[u8]) -> TestResult {
    let mut hash = RangeHash::new(bytes.len() as u64);
    hash.observe(0, bytes)?;
    stage.enable_checkpoint(bytes.len() as u64, Bytes::from(hash.encode()?));
    Ok(())
}

fn checkpoint(protocol: &FakeProtocol) -> Result<CheckpointData, Box<dyn std::error::Error>> {
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
    let (first, protocol, backend) = adapter();
    let held = first
        .prepare_at_destination(hash_request(&backend, 6)?)
        .await?;
    arm(&held, b"abcdef")?;
    first.persist_hash_checkpoint(&held).await?;
    let second = NfsStagedDestinationAdapter::new(protocol.clone(), backend.clone());
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
async fn hash_only_pointer_does_not_promote_confirmed_writes() -> TestResult {
    let (destination, protocol, backend) = adapter();
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
    let saved = checkpoint(&protocol)?;
    assert!(saved.ranges.is_empty());
    assert_eq!(saved.prefix(), 0);
    assert_eq!(saved.phase, CheckpointPhase::Receiving);
    assert!(destination.release_hash_checkpoint(&stage).await.is_err());
    destination.discard(stage).await?;
    Ok(())
}

async fn interrupt_sparse(
    first: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> TestResult {
    let input = Box::pin(stream::iter([
        Ok(crate::storage::PositionedChunk {
            offset: 8,
            data: Bytes::from_static(b"ijkl"),
        }),
        Ok(crate::storage::PositionedChunk {
            offset: 0,
            data: Bytes::from_static(b"abcd"),
        }),
        Err(entry_failure(
            stage.final_destination.path(),
            Operation::Read,
            FailureClass::Cancelled,
            Transience::Transient,
        )),
    ]));
    let failed = first.write_positioned(stage, input).await;
    assert!(
        matches!(failed, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    Ok(())
}

async fn fill_hole(destination: &NfsStagedDestinationAdapter, stage: &PreparedStage) -> TestResult {
    arm(stage, b"abcdefghijkl")?;
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
    Ok(())
}

#[tokio::test]
async fn sparse_checkpoint_survives_error_and_prepare_then_fills_only_the_hole() -> TestResult {
    let (first, protocol, backend) = adapter();
    let mut stage = first
        .prepare_at_destination(hash_request(&backend, 12)?.with_recoverable(false))
        .await?;
    arm(&stage, b"abcdefghijkl")?;
    stage.deferred_checkpoint = Some(crate::storage::roles::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: 12,
    });
    first.persist_hash_checkpoint(&stage).await?;
    interrupt_sparse(&first, &stage).await?;
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..4, 8..12]);
    let retained = file(&protocol, &name(ArtifactKind::Stage, false)).ok_or("stage")?;
    assert_eq!(retained.len(), 12);
    assert_eq!(&retained[8..], b"ijkl");
    drop(stage);
    drop(first);

    let second = NfsStagedDestinationAdapter::new(protocol.clone(), backend.clone());
    let stage = second
        .prepare_at_destination(hash_request(&backend, 12)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: 8 });
    assert_eq!(stage.prepare_fact().reused_bytes(), 8);
    assert_eq!(stage.write_offset, 4);
    assert!(
        !stage.checkpoint_enabled(),
        "only the ordinary engine arms restored state"
    );
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..4, 8..12]);
    assert_eq!(
        file(&protocol, &name(ArtifactKind::Stage, false)),
        Some(retained)
    );
    fill_hole(&second, &stage).await?;
    assert_eq!(checkpoint(&protocol)?.ranges, vec![0..12]);
    assert_eq!(
        file(&protocol, &name(ArtifactKind::Stage, false)),
        Some(b"abcdefghijkl".to_vec())
    );
    second.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn published_hash_pointer_is_retained_until_fenced_idempotent_release() -> TestResult {
    for lost_reply in [false, true] {
        let (destination, protocol, backend) = adapter();
        let stage = destination
            .prepare_at_destination(hash_request(&backend, 6)?)
            .await?;
        arm(&stage, b"abcdef")?;
        destination.persist_hash_checkpoint(&stage).await?;
        write(&destination, &stage, b"abcdef").await?;
        stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
        destination.persist_hash_checkpoint(&stage).await?;
        protocol
            .rename_mode
            .store(if lost_reply { 4 } else { 0 }, Ordering::SeqCst);
        publish(&destination, &stage, b"abcdef")
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(checkpoint(&protocol)?.phase, CheckpointPhase::Published);
        assert_eq!(
            artifacts_left(&protocol),
            [name(ArtifactKind::Pointer, false)]
        );
        destination.release_hash_checkpoint(&stage).await?;
        destination.release_hash_checkpoint(&stage).await?;
        assert!(artifacts_left(&protocol).is_empty());
        assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    }
    Ok(())
}
