use super::*;
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::range_hash::RangeHash;
use crate::storage::{PositionedChunk, VerifyRequest};

#[path = "published_recovery_tests.rs"]
mod published_recovery_tests;

#[cfg(unix)]
#[path = "cleanup_fencing_tests.rs"]
mod cleanup_fencing_tests;

fn hash_request(size: usize) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    Ok(discover_request(size)?.with_checkpoint_hash(true))
}

fn arm(stage: &PreparedStage, bytes: &[u8]) -> TestResult {
    let mut hash = RangeHash::new(bytes.len() as u64);
    hash.observe(0, bytes)?;
    stage.enable_checkpoint(bytes.len() as u64, Bytes::from(hash.encode()?));
    Ok(())
}

fn read_checkpoint(
    root: &Root,
) -> Result<(DestinationPointer, CheckpointData), Box<dyn std::error::Error>> {
    let pointer = DestinationPointer::decode(&std::fs::read(root.artifact(ArtifactKind::Pointer))?)
        .map_err(|_| "decode pointer")?;
    let data = CheckpointData::decode(pointer.checkpoint.as_ref().ok_or("checkpoint absent")?)
        .map_err(|_| "decode checkpoint")?;
    Ok((pointer, data))
}

#[tokio::test]
async fn hash_intent_does_not_promote_confirmed_writes() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter.prepare_at_destination(hash_request(12)?).await?;
    arm(&stage, b"abcdefghijkl")?;
    stage.confirmed_write(8, 4);
    adapter.persist_hash_checkpoint(&stage).await?;
    let (pointer, data) = read_checkpoint(&root)?;
    assert_eq!(pointer.durable_prefix, Some(0));
    assert!(data.ranges.is_empty());
    assert!(!data.hash.is_empty());
    assert_eq!(
        std::fs::metadata(root.artifact(ArtifactKind::Stage))?.len(),
        0
    );
    assert!(adapter.release_hash_checkpoint(&stage).await.is_err());
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn sparse_cancel_resume_retains_hash_and_fills_only_the_hole() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let bytes = b"abcdefghijkl";
    let stage = adapter
        .prepare_at_destination(hash_request(bytes.len())?)
        .await?;
    arm(&stage, bytes)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    let cancelled = failure(
        stage.final_destination().path(),
        Operation::Read,
        FailureClass::Cancelled,
    );
    let input = Box::pin(stream::iter(vec![
        Ok(PositionedChunk {
            offset: 8,
            data: Bytes::from_static(b"ijkl"),
        }),
        Ok(PositionedChunk {
            offset: 0,
            data: Bytes::from_static(b"abcd"),
        }),
        Err(cancelled),
    ]));
    let error = adapter
        .write_positioned(&stage, input)
        .await
        .err()
        .ok_or("cancel accepted")?;
    assert_eq!(
        class(&error).map(|(class, _)| class),
        Some(FailureClass::Cancelled)
    );
    let (pointer, data) = read_checkpoint(&root)?;
    assert_eq!(pointer.durable_prefix, Some(4));
    assert_eq!(data.ranges, vec![0..4, 8..12]);
    assert_eq!(
        std::fs::metadata(root.artifact(ArtifactKind::Stage))?.len(),
        12
    );
    drop(stage);
    complete_sparse_resume(&root, bytes, &data).await
}

async fn complete_sparse_resume(
    root: &Root,
    bytes: &[u8],
    previous: &CheckpointData,
) -> TestResult {
    let adapter = adapter(root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(bytes.len())?)
        .await?;
    assert_eq!(stage.write_offset(), 4);
    assert_eq!(stage.checkpoint_ranges(), vec![0..4, 8..12]);
    let restored = stage
        .restored_checkpoint()
        .ok_or("missing restored checkpoint")?;
    assert_eq!(restored.hash, previous.hash);
    let hash = RangeHash::decode(&restored.hash, bytes.len() as u64)?;
    assert_eq!(hash.finalize()?, *blake3::hash(bytes).as_bytes());
    stage.enable_checkpoint(bytes.len() as u64, restored.hash);
    let error = adapter
        .write_positioned(&stage, Box::pin(stream::empty()))
        .await
        .err()
        .ok_or("hole accepted at EOF")?;
    assert_eq!(
        class(&error).map(|(class, _)| class),
        Some(FailureClass::Corruption)
    );
    assert_eq!(
        std::fs::metadata(root.artifact(ArtifactKind::Stage))?.len(),
        12
    );
    let input = Box::pin(stream::iter([Ok(PositionedChunk {
        offset: 4,
        data: Bytes::from_static(b"efgh"),
    })]));
    assert_eq!(
        adapter
            .write_positioned(&stage, input)
            .await?
            .persisted_bytes,
        12
    );
    assert_eq!(stage.checkpoint_ranges(), vec![0..12]);
    verify_publish_release(root, &adapter, &stage, bytes).await
}

async fn verify_publish_release(
    root: &Root,
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    bytes: &[u8],
) -> TestResult {
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(stage).await?;
    adapter
        .verify(
            stage,
            VerifyRequest {
                expected_size: 12,
                expected_blake3: *blake3::hash(bytes).as_bytes(),
                cancel: tokio_util::sync::CancellationToken::new(),
                published: None,
            },
        )
        .await?;
    publish(adapter, stage, bytes).await?;
    assert!(root.artifact(ArtifactKind::Pointer).is_file());
    assert!(root.artifact(ArtifactKind::Claim).is_file());
    assert_eq!(read_checkpoint(root)?.1.phase, CheckpointPhase::Published);
    adapter.release_hash_checkpoint(stage).await?;
    adapter.release_hash_checkpoint(stage).await?;
    assert!(root.artifacts_left()?.is_empty());
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    Ok(())
}

#[tokio::test]
async fn hash_intent_requires_the_original_locked_claim() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter.prepare_at_destination(hash_request(12)?).await?;
    arm(&stage, b"abcdefghijkl")?;
    adapter.persist_hash_checkpoint(&stage).await?;
    let pointer = std::fs::read(root.artifact(ArtifactKind::Pointer))?;
    std::fs::remove_file(root.artifact(ArtifactKind::Claim))?;
    std::fs::write(root.artifact(ArtifactKind::Claim), b"another owner")?;
    assert!(adapter.persist_hash_checkpoint(&stage).await.is_err());
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Pointer))?,
        pointer
    );
    stage.release_claim();
    assert!(adapter.persist_hash_checkpoint(&stage).await.is_err());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn v2_source_fragments_remain_private_after_payload_metadata_is_tightened() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter.prepare_at_destination(hash_request(12)?).await?;
    std::fs::set_permissions(
        root.artifact(ArtifactKind::Pointer),
        std::fs::Permissions::from_mode(0o644),
    )?;
    arm(&stage, b"abcdefghijkl")?;
    adapter.persist_hash_checkpoint(&stage).await?;
    let pointer = root.artifact(ArtifactKind::Pointer);
    let data = read_checkpoint(&root)?.1;
    assert!(data.hash.windows(12).any(|bytes| bytes == b"abcdefghijkl"));
    assert_eq!(
        std::fs::metadata(&pointer)?.permissions().mode() & 0o777,
        0o600
    );
    adapter
        .apply_metadata(
            &stage,
            crate::storage::MetadataMutation::Mode(0o400),
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    adapter.persist_hash_checkpoint(&stage).await?;
    assert_eq!(
        std::fs::metadata(&pointer)?.permissions().mode() & 0o777,
        0o600
    );
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn publication_reply_loss_retains_the_full_source_checkpoint() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let bytes = b"abcdefghijkl";
    let stage = adapter
        .prepare_at_destination(hash_request(bytes.len())?)
        .await?;
    arm(&stage, bytes)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_positioned(
            &stage,
            Box::pin(stream::iter([Ok(PositionedChunk {
                offset: 0,
                data: Bytes::from_static(bytes),
            })])),
        )
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_probe
        .fail_after_publication_commit
        .store(true, Ordering::SeqCst);
    let failed = try_publish(&adapter, &stage, bytes)
        .await
        .err()
        .ok_or("reply loss not injected")?;
    assert!(failed.final_destination_changed);
    assert!(!root.artifact(ArtifactKind::Stage).exists());
    assert!(root.artifact(ArtifactKind::Claim).exists());
    let (_, data) = read_checkpoint(&root)?;
    assert_eq!(data.phase, CheckpointPhase::ReadyToPublish);
    assert_eq!(data.ranges, vec![0..bytes.len() as u64]);
    let hash = RangeHash::decode(&data.hash, bytes.len() as u64)?;
    assert_eq!(hash.finalize()?, *blake3::hash(bytes).as_bytes());
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    Ok(())
}

#[tokio::test]
async fn unarmed_hash_hooks_preserve_the_existing_pointer() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(discover_request(12)?)
        .await?;
    assert!(!stage.checkpoint_enabled());
    let pointer = std::fs::read(root.artifact(ArtifactKind::Pointer))?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter.release_hash_checkpoint(&stage).await?;
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Pointer))?,
        pointer
    );
    assert!(root.artifact(ArtifactKind::Claim).is_file());
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn malformed_hash_checkpoint_does_not_truncate_or_replace_the_stage() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter.prepare_at_destination(hash_request(12)?).await?;
    std::fs::write(root.artifact(ArtifactKind::Stage), b"keep the end")?;
    let mut record =
        DestinationPointer::decode(&std::fs::read(root.artifact(ArtifactKind::Pointer))?)
            .map_err(|_| "decode pointer")?;
    record.checkpoint = Some(Bytes::from_static(b"invalid checkpoint"));
    let encoded = record.encode().map_err(|_| "encode pointer")?;
    std::fs::write(root.artifact(ArtifactKind::Pointer), &encoded)?;
    drop(stage);
    let error = adapter
        .prepare_at_destination(hash_request(12)?)
        .await
        .err()
        .ok_or("invalid checkpoint was accepted")?;
    assert_eq!(
        class(&error).map(|(class, _)| class),
        Some(FailureClass::Unsupported)
    );
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Stage))?,
        b"keep the end"
    );
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Pointer))?,
        encoded
    );
    assert!(!root.artifact(ArtifactKind::Claim).exists());
    Ok(())
}

#[tokio::test]
async fn reopening_sparse_ranges_retains_the_suffix_and_rejects_short_files() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(discover_request(12)?)
        .await?;
    let path = root.artifact(ArtifactKind::Stage);
    std::fs::write(&path, b"abcd\0\0\0\0ijkl")?;
    let artifacts = LocalArtifacts {
        directory: adapter.stage_directory(&stage, Operation::Prepare).await?,
        stage: artifact(stage.final_destination().path(), ArtifactKind::Stage, false)?,
        pointer: artifact(
            stage.final_destination().path(),
            ArtifactKind::Pointer,
            false,
        )?,
        pointer_temporary: artifact(
            stage.final_destination().path(),
            ArtifactKind::Pointer,
            true,
        )?,
    };
    drop(reopen_stage(&artifacts, stage.final_destination().path(), 4, Some(12)).await?);
    assert_eq!(std::fs::read(&path)?, b"abcd\0\0\0\0ijkl");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(10)?;
    assert!(
        reopen_stage(&artifacts, stage.final_destination().path(), 4, Some(12))
            .await
            .is_err()
    );
    assert_eq!(std::fs::metadata(&path)?.len(), 10);
    adapter.discard(stage).await?;
    Ok(())
}
