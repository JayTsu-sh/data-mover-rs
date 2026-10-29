//! Armed ordinary-transfer behavior, separate from the legacy expert protocol tests.

use super::*;
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::range_hash::RangeHash;

fn checkpoint_request(
    recoverable: bool,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    Ok(request(BINDING, ResumeMode::Discover, recoverable)?.with_checkpoint_hash(true))
}

fn arm(stage: &PreparedStage) -> TestResult {
    let mut hash = RangeHash::new(PAYLOAD.len() as u64);
    hash.observe(0, PAYLOAD)?;
    stage.enable_checkpoint(PAYLOAD.len() as u64, Bytes::from(hash.encode()?));
    Ok(())
}

async fn pointer(protocol: &MemoryHdfs) -> Result<DestinationPointer, Box<dyn std::error::Error>> {
    let bytes = protocol
        .get(&name(ArtifactKind::Pointer))
        .await
        .ok_or("pointer")?;
    DestinationPointer::decode(&bytes).map_err(|_| "pointer decode".into())
}

fn checkpoint(pointer: &DestinationPointer) -> Result<CheckpointData, Box<dyn std::error::Error>> {
    CheckpointData::decode(pointer.checkpoint.as_deref().ok_or("hash checkpoint")?)
        .map_err(|_| "checkpoint decode".into())
}

#[tokio::test]
async fn intent_commit_waits_for_the_shared_stage_gate_without_double_locking() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    let guard = stage.checkpoint_guard().await;
    let persist = destination.persist_hash_checkpoint(&stage);
    tokio::pin!(persist);
    assert!(futures::poll!(&mut persist).is_pending());
    assert_eq!(protocol.checkpoint_io_counts(), (0, 0, 0));
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(2), persist).await??;
    assert_eq!(protocol.checkpoint_io_counts(), (1, 0, 0));
    assert_eq!(pointer(&protocol).await?.durable_prefix, Some(0));
    Ok(())
}

#[tokio::test]
async fn source_intent_persistence_does_not_promote_unflushed_writes() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    stage.confirmed_write(0, 4);
    protocol
        .insert(
            &name(ArtifactKind::Stage),
            Bytes::copy_from_slice(&PAYLOAD[..4]),
        )
        .await;
    destination.persist_hash_checkpoint(&stage).await?;
    let saved = pointer(&protocol).await?;
    let data = checkpoint(&saved)?;
    assert_eq!(saved.durable_prefix, Some(0));
    assert!(data.ranges.is_empty());
    assert!(stage.recovery_enabled());
    assert!(RangeHash::decode(&data.hash, PAYLOAD.len() as u64)?.covers(0..PAYLOAD.len() as u64));
    assert_eq!(protocol.checkpoint_io_counts(), (1, 0, 0));
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        0
    );
    assert!(destination.release_hash_checkpoint(&stage).await.is_err());
    assert_eq!(pointer(&protocol).await?, saved);
    Ok(())
}

#[tokio::test]
async fn hsync_and_final_close_record_durable_data_with_source_intent() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let mut stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    stage.set_deferred_checkpoint(Some(crate::storage::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: PAYLOAD.len() as u64,
    }));
    destination.persist_hash_checkpoint(&stage).await?;
    protocol.fail_once_after_hsync();
    assert!(destination.write(&stage, input(PAYLOAD)).await.is_err());
    let saved = pointer(&protocol).await?;
    assert_eq!(saved.durable_prefix, Some(4));
    assert_eq!(checkpoint(&saved)?.ranges, vec![0..4]);
    assert_eq!(protocol.hsync_calls(), 1);
    drop(stage);

    let stage = destination
        .prepare_at_destination(checkpoint_request(true)?)
        .await?;
    assert!(!stage.checkpoint_enabled());
    assert_eq!(stage.write_offset(), 4);
    let restored = stage.restored_checkpoint().ok_or("restored checkpoint")?;
    stage.enable_checkpoint(restored.size, restored.hash);
    destination.write(&stage, input(&PAYLOAD[4..])).await?;
    let saved = pointer(&protocol).await?;
    assert_eq!(saved.durable_prefix, Some(PAYLOAD.len() as u64));
    assert_eq!(checkpoint(&saved)?.ranges, vec![0..PAYLOAD.len() as u64]);
    assert_eq!(stage.checkpoint_prefix(), PAYLOAD.len() as u64);
    Ok(())
}

#[tokio::test]
async fn lease_recovered_tail_preserves_hash_intent_across_unarmed_takeover() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let first = adapter(&protocol)?;
    let stage = first
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    first.persist_hash_checkpoint(&stage).await?;
    let intent = checkpoint(&pointer(&protocol).await?)?.hash;
    assert!(first.write(&stage, input(&PAYLOAD[..4])).await.is_err());
    assert_eq!(pointer(&protocol).await?.durable_prefix, Some(0));
    drop(stage);
    protocol
        .reveal_tail_during_lease_recovery(Bytes::copy_from_slice(&PAYLOAD[..6]))
        .await;
    let second = adapter(&protocol)?;
    let stage = second
        .prepare_at_destination(checkpoint_request(true)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: 6 });
    assert!(!stage.checkpoint_enabled());
    let saved = pointer(&protocol).await?;
    let recovered = checkpoint(&saved)?;
    assert_eq!(saved.durable_prefix, Some(6));
    assert_eq!(recovered.ranges, vec![0..6]);
    assert_eq!(recovered.hash, intent);
    assert_eq!(
        stage
            .restored_checkpoint()
            .ok_or("restored checkpoint")?
            .hash,
        intent
    );
    assert!(RangeHash::decode(&intent, PAYLOAD.len() as u64)?.covers(0..6));
    assert_eq!(
        protocol.get(&name(ArtifactKind::Stage)).await.as_deref(),
        Some(&PAYLOAD[..6])
    );
    Ok(())
}

#[tokio::test]
async fn published_hash_pointer_survives_lost_reply_until_verified_release() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    destination.persist_hash_checkpoint(&stage).await?;
    destination.write(&stage, input(PAYLOAD)).await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    destination.persist_hash_checkpoint(&stage).await?;
    protocol.fail_rename_after_commit();
    let publication = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: PAYLOAD.len() as u64,
                expected_blake3: Some(*blake3::hash(PAYLOAD).as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        checkpoint(&pointer(&protocol).await?)?.phase,
        CheckpointPhase::Published
    );
    assert!(protocol.get(&name(ArtifactKind::Stage)).await.is_none());
    destination
        .verify(
            &stage,
            VerifyRequest {
                expected_size: PAYLOAD.len() as u64,
                expected_blake3: *blake3::hash(PAYLOAD).as_bytes(),
                cancel: tokio_util::sync::CancellationToken::new(),
                published: Some(publication),
            },
        )
        .await?;
    destination.release_hash_checkpoint(&stage).await?;
    destination.release_hash_checkpoint(&stage).await?;
    assert!(no_artifacts(&protocol).await);
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    Ok(())
}

#[tokio::test]
async fn stale_owner_cannot_persist_or_release_another_owners_hash_intent() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let held = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&held)?;
    destination.persist_hash_checkpoint(&held).await?;
    let taken = destination
        .prepare_at_destination(checkpoint_request(true)?)
        .await?;
    let saved = pointer(&protocol).await?;
    assert!(is_conflict(
        &destination.persist_hash_checkpoint(&held).await
    ));
    held.checkpoint_set_phase(CheckpointPhase::Published);
    assert!(is_conflict(
        &destination.release_hash_checkpoint(&held).await
    ));
    assert_eq!(pointer(&protocol).await?, saved);
    destination.discard(held).await?;
    assert_eq!(pointer(&protocol).await?, saved);
    destination.discard(taken).await?;
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

async fn seed_publication_gap(
    protocol: &Arc<MemoryHdfs>,
    phase: CheckpointPhase,
) -> Result<DestinationPointer, Box<dyn std::error::Error>> {
    let destination = adapter(protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    destination.persist_hash_checkpoint(&stage).await?;
    destination.write(&stage, input(PAYLOAD)).await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    destination.persist_hash_checkpoint(&stage).await?;
    if phase == CheckpointPhase::Published {
        publish(&destination, &stage)
            .await
            .map_err(|failure| failure.error)?;
    } else {
        protocol
            .rename(
                &StoragePath::new(name(ArtifactKind::Stage))?,
                &StoragePath::new("dir/final.bin")?,
                true,
            )
            .await?;
    }
    pointer(protocol).await
}

fn final_verification() -> VerifyRequest {
    VerifyRequest {
        expected_size: PAYLOAD.len() as u64,
        expected_blake3: *blake3::hash(PAYLOAD).as_bytes(),
        cancel: tokio_util::sync::CancellationToken::new(),
        published: None,
    }
}

#[tokio::test]
async fn publication_gaps_recover_without_opening_the_missing_stage() -> TestResult {
    for phase in [CheckpointPhase::ReadyToPublish, CheckpointPhase::Published] {
        let protocol = Arc::new(MemoryHdfs::default());
        let saved = seed_publication_gap(&protocol, phase).await?;
        let destination = adapter(&protocol)?;
        let before = protocol.checkpoint_io_counts();
        let stage = destination
            .prepare_at_destination(checkpoint_request(true)?)
            .await?;
        assert!(stage.recovered_publication().is_some());
        assert!(stage.checkpoint_enabled());
        assert_eq!(stage.checkpoint_phase(), CheckpointPhase::Published);
        assert_eq!(stage.write_offset(), PAYLOAD.len() as u64);
        assert_eq!(
            protocol.checkpoint_io_counts(),
            (before.0 + 1, before.1, before.2)
        );
        assert_eq!(protocol.stabilize_calls(), 0);
        assert!(protocol.get(&name(ArtifactKind::Stage)).await.is_none());
        let taken = pointer(&protocol).await?;
        assert_ne!(taken.extension, saved.extension);
        assert_eq!(checkpoint(&taken)?.hash, checkpoint(&saved)?.hash);
        finish_recovered(&destination, &stage, &protocol, taken).await?;
    }
    Ok(())
}

async fn finish_recovered(
    destination: &HdfsStagedDestination,
    stage: &PreparedStage,
    protocol: &MemoryHdfs,
    taken: DestinationPointer,
) -> TestResult {
    destination.verify(stage, final_verification()).await?;
    let metadata_before = protocol.metadata_calls().await;
    destination
        .apply_metadata(
            stage,
            crate::storage::MetadataMutation::Mode(0),
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    assert_eq!(protocol.metadata_calls().await, metadata_before);
    publish(destination, stage)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        pointer(protocol).await?,
        taken,
        "recovered publish must be a no-op"
    );
    destination.release_hash_checkpoint(stage).await?;
    assert!(no_artifacts(protocol).await);
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    Ok(())
}

#[tokio::test]
async fn incompatible_published_hash_root_is_not_removed_by_legacy_cleanup() -> TestResult {
    for other_transfer in [false, true] {
        let protocol = Arc::new(MemoryHdfs::default());
        let saved = seed_publication_gap(&protocol, CheckpointPhase::ReadyToPublish).await?;
        let destination = adapter(&protocol)?;
        let mut request = checkpoint_request(true)?;
        if other_transfer {
            request.transfer_identity = [8; 32];
        } else {
            request.prepare.recovery_binding = [8; 32];
        }
        assert!(destination.prepare_at_destination(request).await.is_err());
        assert_eq!(pointer(&protocol).await?, saved);
        assert_eq!(
            protocol.get("dir/final.bin").await.as_deref(),
            Some(PAYLOAD)
        );
    }
    Ok(())
}

#[tokio::test]
async fn wrong_final_content_fails_verification_and_retains_recovery_root() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    seed_publication_gap(&protocol, CheckpointPhase::ReadyToPublish).await?;
    protocol
        .insert("dir/final.bin", Bytes::from_static(b"XXXXXXXXXX"))
        .await;
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(true)?)
        .await?;
    let saved = pointer(&protocol).await?;
    let failed = destination.verify(&stage, final_verification()).await;
    assert!(
        matches!(failed, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Corruption)
    );
    destination.discard(stage).await?;
    assert_eq!(pointer(&protocol).await?, saved);
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(b"XXXXXXXXXX".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn wrong_final_size_refuses_recovery_without_removing_the_pointer() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let saved = seed_publication_gap(&protocol, CheckpointPhase::ReadyToPublish).await?;
    protocol
        .insert("dir/final.bin", Bytes::from_static(b"short"))
        .await;
    let destination = adapter(&protocol)?;
    assert!(
        destination
            .prepare_at_destination(checkpoint_request(true)?)
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, saved);
    Ok(())
}

#[tokio::test]
async fn hash_fragments_remain_private_when_payload_metadata_is_restricted() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    destination.persist_hash_checkpoint(&stage).await?;
    assert_eq!(
        protocol.file_mode(&name(ArtifactKind::Pointer)).await,
        Some(0o600)
    );
    destination.write(&stage, input(PAYLOAD)).await?;
    protocol
        .set_mode(&StoragePath::new(name(ArtifactKind::Stage))?, 0o600)
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    destination.persist_hash_checkpoint(&stage).await?;
    publish(&destination, &stage)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(protocol.file_mode("dir/final.bin").await, Some(0o600));
    assert_eq!(
        protocol.file_mode(&name(ArtifactKind::Pointer)).await,
        Some(0o600)
    );
    destination.release_hash_checkpoint(&stage).await?;
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

#[tokio::test]
async fn expert_prepare_cannot_silently_adopt_an_ordinary_hash_checkpoint() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    destination.persist_hash_checkpoint(&stage).await?;
    let saved = pointer(&protocol).await?;
    let before = protocol.checkpoint_io_counts();
    let refused = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await;
    assert!(
        matches!(refused, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported)
    );
    assert_eq!(protocol.checkpoint_io_counts(), before);
    assert_eq!(protocol.stabilize_calls(), 0);
    assert_eq!(pointer(&protocol).await?, saved);
    Ok(())
}

#[tokio::test]
async fn positioned_checkpoint_input_still_reaches_the_append_writer_in_order() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(checkpoint_request(false)?)
        .await?;
    arm(&stage)?;
    destination.persist_hash_checkpoint(&stage).await?;
    let input = Box::pin(stream::iter([6..10, 0..4, 4..6].map(|range| {
        Ok(crate::storage::PositionedChunk {
            offset: range.start as u64,
            data: Bytes::copy_from_slice(&PAYLOAD[range]),
        })
    })));
    destination.write_checkpointed(&stage, input).await?;
    assert_eq!(
        protocol.get(&name(ArtifactKind::Stage)).await.as_deref(),
        Some(PAYLOAD)
    );
    assert_eq!(
        pointer(&protocol).await?.durable_prefix,
        Some(PAYLOAD.len() as u64)
    );
    assert_eq!(protocol.io_peaks().2, 1);
    Ok(())
}
