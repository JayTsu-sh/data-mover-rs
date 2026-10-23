//! Focused ordinary checkpoint integration over the repository's `MemoryS3` fixture.

#[path = "checkpoint_cleanup_tests.rs"]
mod checkpoint_cleanup_tests;

#[path = "etag_receipt_tests.rs"]
mod etag_receipt_tests;

use std::ops::Range;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::checkpoint_record::CheckpointRecord;
use super::final_upload;
use super::upload_pointer::{self, accepted};
use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::backends::s3::tests::{MemoryS3, content_md5, identity};
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::pointer::DestinationPointer;
use crate::storage::range_hash::RangeHash;
use crate::storage::{FinalDestination, PositionedChunk, SourceDescriptor};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const PART: usize = 5 * 1024 * 1024;
const KEY: &str = "checkpoint/final";

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    S3StagedDestination::new(protocol.clone(), identity())
        .with_single_put_threshold(None)
        .with_checkpoint_interval(1)
}

fn request(size: usize) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new(KEY)?),
            source: SourceDescriptor::new(
                StoragePath::new("source")?,
                EntryKind::File,
                Some(size as u64),
                SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
            ),
            recovery_binding: [7; 32],
        },
        [4; 32],
    )
    .with_checkpoint_hash(true))
}

async fn prepare(
    adapter: &S3StagedDestination<MemoryS3>,
    size: usize,
) -> TestResult<PreparedStage> {
    Ok(adapter
        .prepare_final_upload(
            request(size)?,
            PART,
            upload_pointer::pointer_path(&StoragePath::new(KEY)?)?,
        )
        .await?)
}

fn arm(stage: &PreparedStage, data: &[u8]) -> TestResult {
    let mut hash = RangeHash::new(data.len() as u64);
    hash.observe(0, data)?;
    stage.enable_checkpoint(data.len() as u64, hash.encode()?.into());
    Ok(())
}

async fn pointer(protocol: &MemoryS3) -> TestResult<DestinationPointer> {
    let key = upload_pointer::pointer_path(&StoragePath::new(KEY)?)?;
    let bytes = protocol
        .objects
        .lock()
        .await
        .get(key.as_str())
        .cloned()
        .ok_or("pointer missing")?;
    DestinationPointer::decode(&bytes).map_err(|_| "invalid pointer".into())
}

async fn seed_part(
    protocol: &MemoryS3,
    stage: &PreparedStage,
    number: i32,
    data: Bytes,
) -> TestResult {
    let upload = final_upload::of(stage).ok_or("upload")?;
    protocol
        .upload_part(
            KEY,
            &upload.upload_id,
            number,
            data.clone().into(),
            &content_md5(&data),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(())
}

fn positioned(data: &Bytes, ranges: &[Range<usize>]) -> PositionedByteStream {
    Box::pin(stream::iter(
        ranges
            .iter()
            .map(|range| {
                Ok(PositionedChunk {
                    offset: range.start as u64,
                    data: data.slice(range.clone()),
                })
            })
            .collect::<Vec<_>>(),
    ))
}

fn positioned_range(data: &Bytes, range: Range<usize>) -> PositionedByteStream {
    positioned(data, std::slice::from_ref(&range))
}

fn checkpoint(pointer: &DestinationPointer) -> TestResult<CheckpointData> {
    CheckpointData::decode(pointer.checkpoint.as_deref().ok_or("missing hash")?)
        .map_err(|_| "invalid checkpoint".into())
}

#[tokio::test]
async fn persisting_source_intent_does_not_promote_confirmed_writes() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![9; PART + 17]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    stage.confirmed_write(0, PART as u64);
    adapter.persist_hash_checkpoint(&stage).await?;
    let pointer = pointer(&protocol).await?;
    assert_eq!(pointer.durable_prefix, Some(0));
    assert!(checkpoint(&pointer)?.ranges.is_empty());
    assert_eq!(*protocol.part_uploads.lock().await, 0);
    assert!(adapter.release_hash_checkpoint(&stage).await.is_err());
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn sparse_parts_resume_in_frozen_geometry_and_survive_publication_until_release() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from((0..=250_u8).cycle().take(4 * PART + 17).collect::<Vec<_>>());
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    seed_part(&protocol, &stage, 1, data.slice(..PART)).await?;
    seed_part(&protocol, &stage, 3, data.slice(2 * PART..3 * PART)).await?;
    seed_part(&protocol, &stage, 5, data.slice(4 * PART..)).await?;
    let old = pointer(&protocol).await?;
    drop(stage);

    // Ordinary prepare would plan 8 MiB, but this upload keeps its original 5 MiB parts.
    let stage = adapter.prepare_at_destination(request(data.len())?).await?;
    assert!(!stage.checkpoint_enabled());
    assert_eq!(stage.write_offset, PART as u64);
    assert_eq!(stage.prepare_fact().reused_bytes(), (2 * PART + 17) as u64);
    assert_eq!(
        stage.checkpoint_ranges(),
        vec![
            0..PART as u64,
            (2 * PART) as u64..(3 * PART) as u64,
            (4 * PART) as u64..data.len() as u64
        ]
    );
    let resumed = pointer(&protocol).await?;
    assert_ne!(
        accepted(&old).ok_or("record")?.nonce,
        accepted(&resumed).ok_or("record")?.nonce
    );
    assert_eq!(accepted(&resumed).ok_or("record")?.part_size, PART as u64);
    let restored = stage.restored_checkpoint().ok_or("restored")?;
    stage.enable_checkpoint(data.len() as u64, restored.hash);
    adapter.persist_hash_checkpoint(&stage).await?;
    let cut = PART + 32771;
    let input = positioned(&data, &[cut..2 * PART, 3 * PART..4 * PART, PART..cut]);
    assert_eq!(
        adapter
            .write_checkpointed(&stage, input)
            .await?
            .persisted_bytes,
        data.len() as u64
    );
    assert_eq!(
        *protocol.part_uploads.lock().await,
        5,
        "only missing parts are uploaded"
    );
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    let published = adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(&data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let retained = pointer(&protocol).await?;
    assert_eq!(checkpoint(&retained)?.phase, CheckpointPhase::Published);
    assert!(
        CheckpointRecord::decode(&retained.extension)
            .ok_or("record")?
            .published
            .is_some()
    );
    adapter
        .verify(
            &stage,
            VerifyRequest {
                expected_size: data.len() as u64,
                expected_blake3: *blake3::hash(&data).as_bytes(),
                cancel: CancellationToken::new(),
                published: Some(published),
            },
        )
        .await?;
    adapter.release_hash_checkpoint(&stage).await?;
    adapter.release_hash_checkpoint(&stage).await?;
    assert!(pointer(&protocol).await.is_err());
    assert_eq!(protocol.objects.lock().await.get(KEY), Some(&data));
    Ok(())
}

#[tokio::test]
async fn part_dispatch_requires_successfully_persisted_source_intent() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![4; PART + 3]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    assert!(
        adapter
            .write_checkpointed(&stage, positioned_range(&data, 0..PART))
            .await
            .is_err()
    );
    assert_eq!(*protocol.part_uploads.lock().await, 0);
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn an_unanchored_listed_part_fails_before_takeover() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![4; 2 * PART]);
    let stage = prepare(&adapter, data.len()).await?;
    let mut hash = RangeHash::new(data.len() as u64);
    hash.observe(0, &data[..PART])?;
    stage.enable_checkpoint(data.len() as u64, hash.encode()?.into());
    adapter.persist_hash_checkpoint(&stage).await?;
    seed_part(&protocol, &stage, 2, data.slice(PART..)).await?;
    let old = pointer(&protocol).await?;
    assert!(
        adapter
            .prepare_at_destination(request(data.len())?)
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, old);
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_replaced_committed_part_receipt_fails_before_takeover() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![4; PART + 17]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    adapter.persist_hash_checkpoint(&stage).await?;
    let old = pointer(&protocol).await?;
    seed_part(&protocol, &stage, 1, Bytes::from(vec![5; PART])).await?;
    assert!(
        adapter
            .prepare_at_destination(request(data.len())?)
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, old);
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_published_checkpoint_resumes_without_creating_another_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![4; PART + 17]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(&data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let begins = *protocol.multipart_begins.lock().await;
    let parts = *protocol.part_uploads.lock().await;
    let resumed = adapter.prepare_at_destination(request(data.len())?).await?;
    assert!(resumed.recovered_publication.is_some());
    assert_eq!(resumed.write_offset, data.len() as u64);
    assert_eq!(*protocol.multipart_begins.lock().await, begins);
    assert_eq!(*protocol.part_uploads.lock().await, parts);
    verify_recovered(&adapter, &resumed, &data).await?;
    assert_eq!(*protocol.completes.lock().await, 1);
    adapter.release_hash_checkpoint(&resumed).await?;
    Ok(())
}

async fn verify_recovered(
    adapter: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    data: &Bytes,
) -> TestResult {
    let evidence = adapter
        .publish(
            stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    adapter
        .verify(
            stage,
            VerifyRequest {
                expected_size: data.len() as u64,
                expected_blake3: *blake3::hash(data).as_bytes(),
                published: Some(evidence),
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn complete_before_a_failed_published_pointer_put_resumes_from_ready() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![6; PART + 19]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    *protocol.put_fails_without_commit.lock().await = true;
    let failure = adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(&data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .err()
        .ok_or("publish should lose its pointer update")?;
    assert!(failure.final_destination_changed);
    assert_eq!(
        checkpoint(&pointer(&protocol).await?)?.phase,
        CheckpointPhase::ReadyToPublish
    );
    drop(stage);
    let resumed = adapter.prepare_at_destination(request(data.len())?).await?;
    assert!(resumed.recovered_publication.is_some());
    verify_recovered(&adapter, &resumed, &data).await?;
    adapter.release_hash_checkpoint(&resumed).await?;
    assert_eq!(*protocol.completes.lock().await, 1);
    Ok(())
}

#[tokio::test]
async fn published_final_replacement_fails_without_deleting_the_source_root() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![6; PART + 19]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(&data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let old = pointer(&protocol).await?;
    let replacement = Bytes::from(vec![3; data.len()]);
    protocol
        .put_object(KEY, replacement.clone().into(), &content_md5(&replacement))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(
        adapter
            .prepare_at_destination(request(data.len())?)
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, old);
    adapter.release_hash_checkpoint(&stage).await?;
    Ok(())
}

#[tokio::test]
async fn ineligible_discovery_refuses_v2_without_replacing_it() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![3; PART + 1]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    let old = pointer(&protocol).await?;
    assert!(
        adapter
            .prepare_at_destination(request(data.len())?.with_checkpoint_hash(false))
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, old);
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn native_eligibility_and_actual_stage_capability_exclude_single_requests() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol).with_native_sizing(PART as u64, PART as u64);
    assert!(!adapter.native_hash_checkpoint_eligible(&request(PART)?.prepare.source));
    assert!(adapter.native_hash_checkpoint_eligible(&request(PART + 1)?.prepare.source));
    let single = S3StagedDestination::new(protocol.clone(), identity())
        .prepare_at_destination(request(1)?)
        .await?;
    assert!(!adapter.stage_supports_hash_checkpoint(&single));
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    Ok(())
}

#[tokio::test]
async fn ten_thousand_part_receipts_fit_a_constant_size_extension() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let stage = prepare(&adapter, 10_000 * PART).await?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts = (1..=10_000)
        .map(|number| (number, format!("opaque-etag-{number}")))
        .collect();
    let receipts = super::checkpoint_record::receipts(
        upload,
        (10_000 * PART) as u64,
        std::slice::from_ref(&(0..(10_000 * PART) as u64)),
    )
    .ok_or("invalid receipts")?;
    let record = CheckpointRecord {
        upload: accepted(&pointer(&protocol).await?).ok_or("upload record")?,
        receipts,
        completion_etag: None,
        published: None,
    };
    let encoded = record.encode().ok_or("record")?;
    assert!(encoded.len() < 256);
    assert_eq!(
        CheckpointRecord::decode(&encoded).ok_or("decode")?.receipts,
        receipts
    );
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn changing_the_single_put_threshold_resumes_the_old_multipart_geometry() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let original = destination(&protocol);
    let data = Bytes::from(vec![3; PART + 1]);
    let stage = prepare(&original, data.len()).await?;
    arm(&stage, &data)?;
    original.persist_hash_checkpoint(&stage).await?;
    seed_part(&protocol, &stage, 1, data.slice(..PART)).await?;
    drop(stage);
    let changed = S3StagedDestination::new(protocol.clone(), identity());
    let heads = protocol.head_versions.lock().await.len();
    let gets = protocol.range_observations.lock().await.len();
    let stage = changed.prepare_at_destination(request(data.len())?).await?;
    assert_eq!(protocol.head_versions.lock().await.len() - heads, 1);
    assert_eq!(protocol.range_observations.lock().await.len() - gets, 1);
    assert_eq!(stage.write_offset, PART as u64);
    assert_eq!(final_upload::of(&stage).ok_or("multipart")?.part_size, PART);
    let restored = stage.restored_checkpoint().ok_or("restored")?;
    stage.enable_checkpoint(data.len() as u64, restored.hash);
    changed
        .write_checkpointed(&stage, positioned_range(&data, PART..data.len()))
        .await?;
    assert_eq!(
        *protocol.part_uploads.lock().await,
        2,
        "durable part 1 is reused"
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.aborts.lock().await, 0);
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    changed.persist_hash_checkpoint(&stage).await?;
    verify_recovered(&changed, &stage, &data).await?;
    changed.release_hash_checkpoint(&stage).await?;
    Ok(())
}

#[tokio::test]
async fn changing_native_single_copy_sizing_resumes_the_old_multipart_geometry() -> TestResult {
    use crate::storage::backends::s3::S3NativeCopySource;
    use crate::storage::backends::s3::tests::etag_of;

    let protocol = Arc::new(MemoryS3::default());
    let original = destination(&protocol);
    let data = Bytes::from(vec![3; PART + 1]);
    let stage = prepare(&original, data.len()).await?;
    arm(&stage, &data)?;
    original.persist_hash_checkpoint(&stage).await?;
    seed_part(&protocol, &stage, 1, data.slice(..PART)).await?;
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), data.clone());
    drop(stage);
    let changed =
        destination(&protocol).with_native_sizing((2 * PART) as u64, super::PART_SIZE as u64);
    let spec = request(data.len())?;
    assert!(!changed.native_hash_checkpoint_eligible(&spec.prepare.source));
    let heads = protocol.head_versions.lock().await.len();
    let gets = protocol.range_observations.lock().await.len();
    let stage = changed.prepare_native_stage(spec).await?;
    assert_eq!(protocol.head_versions.lock().await.len() - heads, 1);
    assert_eq!(protocol.range_observations.lock().await.len() - gets, 1);
    assert_eq!(stage.write_offset, PART as u64);
    assert_eq!(final_upload::of(&stage).ok_or("multipart")?.part_size, PART);
    let restored = stage.restored_checkpoint().ok_or("restored")?;
    stage.enable_checkpoint(data.len() as u64, restored.hash);
    let source = S3NativeCopySource {
        bucket: "memory".into(),
        key: "source".into(),
        etag: etag_of(&data),
        version_id: None,
        size: data.len() as u64,
    };
    let copied = changed
        .fill_native(&stage, source, CancellationToken::new())
        .await
        .map_err(|failure| failure.error.to_string())?;
    assert_eq!(copied.write.persisted_bytes, data.len() as u64);
    assert_eq!(copied.native_bytes, 1, "durable part 1 is reused");
    assert_eq!(*protocol.part_copies.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.aborts.lock().await, 0);
    changed.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_held_v2_root_keeps_typed_pending_cleanup_after_publication() -> TestResult {
    use crate::storage::backends::s3::tests::Versioning;

    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![7; PART + 1]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    verify_recovered(&adapter, &stage, &data).await?;
    let held = pointer(&protocol).await?;
    let key = upload_pointer::pointer_path(&StoragePath::new(KEY)?)?;
    protocol.lock_versions(key.as_str());
    protocol.lock_refuses_delete_markers();
    for _ in 0..2 {
        let failure = adapter
            .release_hash_checkpoint(&stage)
            .await
            .err()
            .ok_or("a held root must retain pending cleanup")?;
        assert!(matches!(failure, StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::PermissionDenied));
        assert!(
            !final_upload::of(&stage)
                .ok_or("upload")?
                .lock()
                .checkpoint_released
        );
        assert_eq!(pointer(&protocol).await?, held);
    }
    assert_eq!(protocol.objects.lock().await.get(KEY), Some(&data));
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_native_sparse_resume_reports_the_complete_object_as_written() -> TestResult {
    use crate::storage::backends::s3::S3NativeCopySource;
    use crate::storage::backends::s3::tests::etag_of;

    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![8; 4 * PART + 17]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), data.clone());
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    seed_part(&protocol, &stage, 1, data.slice(..PART)).await?;
    seed_part(&protocol, &stage, 3, data.slice(2 * PART..3 * PART)).await?;
    seed_part(&protocol, &stage, 5, data.slice(4 * PART..)).await?;
    drop(stage);
    let stage = prepare(&adapter, data.len()).await?;
    assert_eq!(stage.prepare_fact().reused_bytes(), (2 * PART + 17) as u64);
    let restored = stage.restored_checkpoint().ok_or("restored")?;
    stage.enable_checkpoint(data.len() as u64, restored.hash);
    adapter.persist_hash_checkpoint(&stage).await?;
    let source = S3NativeCopySource {
        bucket: "memory".into(),
        key: "source".into(),
        etag: etag_of(&data),
        version_id: None,
        size: data.len() as u64,
    };
    let copied = adapter
        .fill_native(&stage, source, CancellationToken::new())
        .await
        .map_err(|failure| failure.error.to_string())?;
    assert_eq!(copied.write.persisted_bytes, data.len() as u64);
    assert_eq!(copied.native_bytes, (2 * PART) as u64);
    assert_eq!(copied.native_requests, 2);
    assert_eq!(
        adapter.observe_checkpoint(&stage).await?.durable_prefix,
        data.len() as u64
    );
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_single_request_redirect_retains_published_recovery_context() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let original = destination(&protocol);
    let data = Bytes::from(vec![9; PART + 1]);
    let stage = prepare(&original, data.len()).await?;
    arm(&stage, &data)?;
    original.persist_hash_checkpoint(&stage).await?;
    original
        .write_checkpointed(&stage, positioned_range(&data, 0..data.len()))
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    original.persist_hash_checkpoint(&stage).await?;
    verify_recovered(&original, &stage, &data).await?;
    drop(stage);

    let changed = S3StagedDestination::new(protocol.clone(), identity());
    let gets = protocol.range_observations.lock().await.len();
    let stage = changed.prepare_at_destination(request(data.len())?).await?;
    assert_eq!(protocol.range_observations.lock().await.len() - gets, 1);
    assert!(stage.recovered_publication.is_some());
    assert_eq!(stage.write_offset, data.len() as u64);
    assert_eq!(final_upload::of(&stage).ok_or("multipart")?.part_size, PART);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    verify_recovered(&changed, &stage, &data).await?;
    assert_eq!(*protocol.completes.lock().await, 1);
    changed.release_hash_checkpoint(&stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_single_request_redirect_requires_the_ordinary_checkpoint_interface() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let original = destination(&protocol);
    let data = Bytes::from(vec![3; PART + 1]);
    let stage = prepare(&original, data.len()).await?;
    arm(&stage, &data)?;
    original.persist_hash_checkpoint(&stage).await?;
    let old = pointer(&protocol).await?;
    let changed = S3StagedDestination::new(protocol.clone(), identity());
    assert!(
        changed
            .prepare_at_destination(request(data.len())?.with_checkpoint_hash(false))
            .await
            .is_err()
    );
    assert_eq!(pointer(&protocol).await?, old);
    assert_eq!(*protocol.aborts.lock().await, 0);
    original.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn a_single_request_discovery_preserves_v2_on_context_or_prefix_mismatch() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let original = destination(&protocol);
    let data = Bytes::from(vec![3; PART + 1]);
    let stage = prepare(&original, data.len()).await?;
    arm(&stage, &data)?;
    original.persist_hash_checkpoint(&stage).await?;
    let old = pointer(&protocol).await?;
    let changed = S3StagedDestination::new(protocol.clone(), identity());
    for mismatch in 0..4 {
        let mut spec = request(data.len())?;
        let mut expected = old.clone();
        match mismatch {
            0 => spec.prepare.recovery_binding = [8; 32],
            1 => spec.transfer_identity = [5; 32],
            2 => spec.prepare.source.size = Some(data.len() as u64 + 1),
            _ => {
                expected.durable_prefix = Some(1);
                let key = upload_pointer::pointer_path(&StoragePath::new(KEY)?)?;
                let bytes = expected.encode().map_err(|_| "encode")?;
                protocol
                    .objects
                    .lock()
                    .await
                    .insert(key.as_str().into(), bytes.into());
            }
        }
        let failure = changed
            .prepare_at_destination(spec)
            .await
            .err()
            .ok_or("must fail closed")?;
        assert!(matches!(failure, StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::Unsupported));
        assert_eq!(pointer(&protocol).await?, expected);
    }
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.aborts.lock().await, 0);
    original.discard(stage).await?;
    Ok(())
}
