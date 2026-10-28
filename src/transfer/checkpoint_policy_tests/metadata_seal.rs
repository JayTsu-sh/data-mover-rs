//! Publication metadata is bound to values and selectors, not just payload identity.

use super::*;
use crate::model::ObjectTag;
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of};
use crate::storage::backends::s3::{S3Protocol, S3ProtocolFailure, connect_native_at_destination};
use crate::storage::checkpoint::CheckpointData;
use crate::storage::pointer::DestinationPointer;
use crate::transfer::CopiedMetadataRequest;
use crate::transfer::engine::run_until_transferred;
use bytes::Bytes;

#[tokio::test]
async fn changed_selectors_cannot_accept_a_published_final_without_a_metadata_plan() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    leave_unverified_final(&fixture).await?;
    let request = fixture.request(TransferPolicy::Checkpointed)?;
    let failure = transfer(
        request
            .clone()
            .with_copied_metadata(CopiedMetadataRequest::default().with_acl().with_xattrs()),
    )
    .await
    .err()
    .ok_or("changed selectors must not accept the old final")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    let calls = fixture.calls.snapshot();
    assert!(
        !calls.iter().any(|call| matches!(
            call,
            Call::OrderedRead(_)
                | Call::PositionedRead
                | Call::VerifyPublished
                | Call::Publish
                | Call::ReleaseHash
        )),
        "{calls:?}"
    );
    drop(failure);
    fixture
        .faults
        .published_verify
        .store(false, Ordering::SeqCst);
    transfer(request).await?;
    fixture.assert_published()
}

#[tokio::test]
async fn receiving_payload_can_be_reused_with_different_metadata_selectors() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let request = fixture.request(TransferPolicy::Checkpointed)?;
    let transferred = run_until_transferred(request.clone()).await?;
    drop(transferred);
    fixture.calls.clear();
    transfer(request.with_copied_metadata(CopiedMetadataRequest::default().with_acl())).await?;
    assert!(
        !fixture
            .calls
            .snapshot()
            .iter()
            .any(|call| matches!(call, Call::OrderedRead(_) | Call::PositionedRead))
    );
    fixture.assert_published()
}

#[tokio::test]
async fn surviving_ready_stage_reenters_receiving_before_persisting_new_selectors() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let request = fixture.request(TransferPolicy::Checkpointed)?;
    drop(run_until_transferred(request.clone()).await?);
    let path = fixture
        .destination_root
        .path()
        .join(artifact_name(PATH, ArtifactKind::Pointer));
    let mut pointer =
        DestinationPointer::decode(&std::fs::read(&path)?).map_err(|e| format!("{e:?}"))?;
    let mut data = CheckpointData::decode(pointer.checkpoint.as_deref().ok_or("v2 root")?)?;
    assert!(data.metadata_binding.is_some());
    data.phase = CheckpointPhase::ReadyToPublish;
    pointer.checkpoint = Some(data.encode());
    std::fs::write(path, pointer.encode().map_err(|e| format!("{e:?}"))?)?;
    fixture.calls.clear();
    transfer(request.with_copied_metadata(CopiedMetadataRequest::default().with_acl())).await?;
    let calls = fixture.calls.snapshot();
    let first_phase = calls.iter().find_map(|call| match call {
        Call::HashPhase(phase) => Some(*phase),
        _ => None,
    });
    assert_eq!(first_phase, Some(CheckpointPhase::Receiving));
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, Call::OrderedRead(_) | Call::PositionedRead))
    );
    fixture.assert_published()
}

#[tokio::test]
async fn missing_metadata_seal_cannot_accept_ready_or_published_roots() -> TestResult {
    for phase in [CheckpointPhase::ReadyToPublish, CheckpointPhase::Published] {
        let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
        leave_unverified_final(&fixture).await?;
        let path = fixture
            .destination_root
            .path()
            .join(artifact_name(PATH, ArtifactKind::Pointer));
        let mut pointer =
            DestinationPointer::decode(&std::fs::read(&path)?).map_err(|e| format!("{e:?}"))?;
        let mut data = CheckpointData::decode(pointer.checkpoint.as_deref().ok_or("v2 root")?)?;
        data.metadata_binding = None;
        data.phase = phase;
        pointer.checkpoint = Some(data.encode());
        std::fs::write(&path, pointer.encode().map_err(|e| format!("{e:?}"))?)?;
        let failure = transfer(fixture.request(TransferPolicy::Checkpointed)?)
            .await
            .err()
            .ok_or("publication recovery requires a metadata seal")?;
        assert_eq!(failure.phase(), TransferPhase::Metadata);
        assert!(failure.has_pending_cleanup());
        assert!(path.exists());
        assert_eq!(
            std::fs::read(fixture.destination_root.path().join(PATH))?,
            fixture.expected
        );
        assert!(!fixture.calls.snapshot().iter().any(|call| matches!(
            call,
            Call::OrderedRead(_) | Call::PositionedRead | Call::VerifyPublished | Call::Publish
        )));
    }
    Ok(())
}

const MIB: usize = 1024 * 1024;

fn s3_request(protocol: &Arc<MemoryS3>) -> TestResult<TransferRequest> {
    let source = crate::storage::backends::s3::tests::source_storage(protocol, "src")?;
    let destination = connect_native_at_destination(
        protocol.clone(),
        endpoint_of(protocol),
        10 * MIB as u64,
        (6 * MIB as u64, 5 * MIB as u64),
    )?;
    Ok(TransferRequest::new(
        source,
        StoragePath::new("file")?,
        destination,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

async fn leave_s3_final(protocol: &Arc<MemoryS3>, tags: &[ObjectTag]) -> TestResult<String> {
    leave_s3_final_requested(protocol, tags, s3_request(protocol)?).await
}

async fn leave_s3_final_requested(
    protocol: &Arc<MemoryS3>,
    tags: &[ObjectTag],
    request: TransferRequest,
) -> TestResult<String> {
    seed_multipart_source(protocol).await?;
    let descriptor = s3_source_descriptor(protocol).await?;
    protocol
        .put_tags("src/file", tags)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let pointer = artifact_name("file", ArtifactKind::Upload);
    *protocol.delete_failure.lock().await = Some((
        pointer.clone(),
        S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            "retain publication root",
        ),
    ));
    let failure = transfer(request)
        .await
        .err()
        .ok_or("checkpoint release must fail after publication")?;
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    drop(failure);
    assert_eq!(
        s3_source_descriptor(protocol).await?,
        descriptor,
        "publication must not change the fake source's content binding"
    );
    let bytes = protocol
        .objects
        .lock()
        .await
        .get(&pointer)
        .cloned()
        .ok_or("published root must remain")?;
    let pointer_data = DestinationPointer::decode(&bytes).map_err(|e| format!("{e:?}"))?;
    let data = CheckpointData::decode(pointer_data.checkpoint.as_deref().ok_or("v2 root")?)?;
    assert_eq!(data.phase, CheckpointPhase::Published);
    assert!(data.metadata_binding.is_some());
    Ok(pointer)
}

/// `MemoryS3` caches multipart `ETags` by body MD5, not key. Seed identical source geometry so
/// publishing the copied body cannot itself turn the source's single-PUT `ETag` into a composite.
async fn seed_multipart_source(protocol: &MemoryS3) -> TestResult {
    let key = "src/file";
    let upload = protocol
        .begin_multipart(key)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut parts = Vec::new();
    for (index, size) in [5 * MIB, 5 * MIB, MIB].into_iter().enumerate() {
        let bytes = Bytes::from(vec![19; size]);
        let md5 = crate::storage::backends::s3::tests::content_md5(&bytes);
        let number = i32::try_from(index + 1)?;
        let etag = protocol
            .upload_part(key, &upload, number, bytes.into(), &md5)
            .await
            .map_err(|e| format!("{e:?}"))?;
        parts.push((number, etag));
    }
    protocol
        .complete_multipart(key, &upload, &parts)
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn s3_source_descriptor(protocol: &Arc<MemoryS3>) -> TestResult<SourceDescriptor> {
    Ok(
        crate::storage::backends::s3::tests::source_storage(protocol, "src")?
            .read_source()?
            .describe(&StoragePath::new("file")?)
            .await?,
    )
}

#[tokio::test]
async fn changed_source_tags_with_unchanged_payload_cannot_adopt_the_previous_final() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    let old_tags = vec![ObjectTag::new("generation", "old")?];
    let pointer = Box::pin(leave_s3_final(&protocol, &old_tags)).await?;
    let descriptor = s3_source_descriptor(&protocol).await?;
    let new_tags = vec![ObjectTag::new("generation", "new")?];
    change_tags_without_changing_payload(&protocol, &new_tags).await?;
    assert_eq!(s3_source_descriptor(&protocol).await?, descriptor);
    reject_changed_s3_metadata(&protocol).await?;
    assert!(protocol.objects.lock().await.contains_key(&pointer));
    assert_eq!(
        protocol
            .get_tags("file", None)
            .await
            .map_err(|e| format!("{e:?}"))?,
        old_tags
    );
    protocol
        .put_tags("src/file", &old_tags)
        .await
        .map_err(|e| format!("{e:?}"))?;
    *protocol.delete_failure.lock().await = None;
    transfer(s3_request(&protocol)?).await?;
    assert!(!protocol.objects.lock().await.contains_key(&pointer));
    Ok(())
}

#[tokio::test]
async fn completion_before_tags_cannot_recover_as_success() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    seed_multipart_source(&protocol).await?;
    let expected = vec![ObjectTag::new("generation", "copied")?];
    protocol
        .put_tags("src/file", &expected)
        .await
        .map_err(|e| format!("{e:?}"))?;
    *protocol.tag_write_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "interrupt after Complete before tags",
    ));
    let failure = transfer(s3_request(&protocol)?)
        .await
        .err()
        .ok_or("tag failure must retain the completed object")?;
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    drop(failure);
    let pointer = artifact_name("file", ArtifactKind::Upload);
    assert!(protocol.objects.lock().await.contains_key(&pointer));
    *protocol.tag_write_failure.lock().await = None;

    let failure = transfer(s3_request(&protocol)?)
        .await
        .err()
        .ok_or("recovery must reject the final whose tags never committed")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    assert!(protocol.objects.lock().await.contains_key(&pointer));
    assert!(
        protocol
            .get_tags("file", None)
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn recovered_final_validates_tags_without_reapplying_them() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(crate::storage::backends::s3::tests::Versioning::Enabled);
    let tags = vec![
        ObjectTag::new("first", "one")?,
        ObjectTag::new("second", "two")?,
    ];
    let pointer = Box::pin(leave_s3_final(&protocol, &tags)).await?;
    let published_version = protocol
        .head("file")
        .await
        .map_err(|e| format!("{e:?}"))?
        .version_id
        .ok_or("versioned final")?;
    let reversed: Vec<_> = tags.into_iter().rev().collect();
    protocol
        .put_tags("file", &reversed)
        .await
        .map_err(|e| format!("{e:?}"))?;
    *protocol.delete_failure.lock().await = None;
    protocol.tag_versions.lock().await.clear();
    *protocol.tag_write_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Unsupported,
        Transience::Permanent,
        "recovered final must never write tags",
    ));
    let outcome = transfer(s3_request(&protocol)?).await?;
    assert!(outcome.metadata.is_none());
    assert!(
        protocol
            .tag_versions
            .lock()
            .await
            .contains(&Some(published_version))
    );
    assert!(!protocol.objects.lock().await.contains_key(&pointer));
    assert_eq!(
        protocol
            .get_tags("file", None)
            .await
            .map_err(|e| format!("{e:?}"))?,
        reversed
    );
    Ok(())
}

async fn change_tags_without_changing_payload(
    protocol: &MemoryS3,
    tags: &[ObjectTag],
) -> TestResult {
    let before = protocol
        .head("src/file")
        .await
        .map_err(|e| format!("{e:?}"))?;
    protocol
        .put_tags("src/file", tags)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let after = protocol
        .head("src/file")
        .await
        .map_err(|e| format!("{e:?}"))?;
    assert_eq!(before.etag, after.etag);
    assert_eq!(before.version_id, after.version_id);
    Ok(())
}

async fn reject_changed_s3_metadata(protocol: &Arc<MemoryS3>) -> TestResult {
    let qos = crate::runtime::qos::SourceQosGroup::new(crate::runtime::qos::SourceQosPolicy::new(
        None,
        CHUNK as u64,
        None,
    )?);
    let failure = transfer(s3_request(protocol)?.with_source_qos(qos))
        .await
        .err()
        .ok_or("new actual tags must not match the saved Ready plan")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata, "{failure:?}");
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    assert_eq!(failure.source_qos().source_read_operations, 0);
    Ok(())
}

#[tokio::test]
async fn recovered_transfer_reports_acl_skip_and_checkpoint_mutations_without_replaying_writes()
-> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let tags = vec![ObjectTag::new("retained", "after-recovery")?];
    let request =
        s3_request(&protocol)?.with_copied_metadata(CopiedMetadataRequest::default().with_acl());
    let pointer = leave_s3_final_requested(&protocol, &tags, request.clone()).await?;
    *protocol.delete_failure.lock().await = None;
    *protocol.tag_write_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Unsupported,
        Transience::Permanent,
        "recovered transfer must not replay tag writes",
    ));
    let copies_before = *protocol.native_copies.lock().await;
    let qos = crate::runtime::qos::SourceQosGroup::new(crate::runtime::qos::SourceQosPolicy::new(
        None,
        CHUNK as u64,
        None,
    )?);
    let outcome = transfer(request.with_source_qos(qos)).await?;
    let report = outcome.metadata.ok_or("recovered ACL report missing")?;
    assert_recovered_acl_skip(&report);
    assert_eq!(*protocol.native_copies.lock().await, copies_before);
    assert_eq!(outcome.source_qos.source_read_operations, 0);
    assert_eq!(
        protocol
            .get_tags("file", None)
            .await
            .map_err(|e| format!("{e:?}"))?,
        tags
    );
    assert!(!protocol.objects.lock().await.contains_key(&pointer));
    Ok(())
}

fn assert_recovered_acl_skip(report: &crate::metadata::MetadataApplicationReport) {
    use crate::metadata::{ApplicationOutcome, MetadataFamily, RefusalCause};
    assert!(
        report
            .skipped()
            .iter()
            .any(|item| item.family == MetadataFamily::Acl
                && item.reason == RefusalCause::DestinationCannotStore)
    );
    assert!(
        report
            .outcomes()
            .iter()
            .any(|item| item.family == MetadataFamily::Acl
                && item.outcome == ApplicationOutcome::Unsupported)
    );
    assert!(
        report
            .outcomes()
            .iter()
            .any(|item| item.family == MetadataFamily::Tags
                && item.outcome == ApplicationOutcome::RecoveredFromCheckpoint)
    );
}
