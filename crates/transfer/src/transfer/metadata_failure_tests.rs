//! 元数据失败的数据发布与完整就绪分离；真实 Local 文件及恢复凭据均独立核验。
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{
    MetadataFailurePolicy, MetadataTransferMode, ReadBackVerification, TransferPhase,
    TransferPolicy, TransferRequest, transfer,
};
use crate::metadata::{
    AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
    OwnershipTarget, TimestampTargetCapability, ValueTarget, compile_metadata_plan,
};
use crate::model::{
    MetadataObservation, MetadataObservations, MetadataProvenance, ObjectTag, StoragePath,
};
use crate::storage::backends::local::{test_destination_storage_with_role, test_source_storage};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

// 模拟计划与实际能力不符；真实 Local 对 tags 的写入会返回有类型 Unsupported。
fn refused_plan() -> std::result::Result<Arc<MetadataPlan>, Box<dyn std::error::Error>> {
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: vec![ObjectTag::new("key", "value")?],
            provenance: MetadataProvenance::Inline,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    Ok(Arc::new(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::Supported,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::NotApplicable,
        },
        policies: MetadataPolicies::default().with_tags(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?))
}

#[tokio::test]
async fn explicitly_published_data_is_still_a_metadata_failure_in_every_write_policy() -> Result {
    for policy in [
        TransferPolicy::Direct,
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
    ] {
        for read_back in [
            ReadBackVerification::Enabled,
            ReadBackVerification::Disabled,
        ] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), b"new payload")?;
            std::fs::write(target_root.path().join("file"), b"old payload")?;
            let source = test_source_storage(source_root.path(), "partial-source")?.0;
            let (target, _) =
                test_destination_storage_with_role(target_root.path(), "partial-target")?;
            let failure = transfer(
                TransferRequest::new(
                    source,
                    StoragePath::new("file")?,
                    target,
                    CancellationToken::new(),
                )
                .with_transfer_policy(policy)
                .with_read_back_verification(read_back)
                .with_metadata_plan(refused_plan()?)
                .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
            )
            .await
            .err()
            .ok_or("metadata refusal became success")?;
            assert_eq!(failure.phase(), TransferPhase::Metadata);
            assert!(failure.metadata_failure().is_some());
            let data = failure
                .published_data()
                .ok_or("published data evidence missing")?;
            assert_eq!(data.transferred_bytes, 11);
            assert_eq!(data.read_back, read_back);
            assert_eq!(
                data.blake3.is_some(),
                read_back == ReadBackVerification::Enabled
            );
            assert!(data.metadata.is_none());
            assert!(failure.final_destination_changed());
            assert!(!failure.has_unpublished_stage() && !failure.has_pending_cleanup());
            assert_eq!(
                std::fs::read(target_root.path().join("file"))?,
                b"new payload"
            );
            assert_eq!(
                std::fs::read(source_root.path().join("file"))?,
                b"new payload"
            );
            let failure = failure.cleanup_for_policy(policy).await;
            assert!(failure.cleanup_failure().is_none());
            assert_eq!(
                std::fs::read(target_root.path().join("file"))?,
                b"new payload"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn default_policy_stops_staged_publication_and_never_claims_data_completion() -> Result {
    for policy in [
        TransferPolicy::Direct,
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
    ] {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"new payload")?;
        std::fs::write(target_root.path().join("file"), b"old payload")?;
        let source = test_source_storage(source_root.path(), "strict-source")?.0;
        let (target, _) = test_destination_storage_with_role(target_root.path(), "strict-target")?;
        let failure = transfer(
            TransferRequest::new(
                source,
                StoragePath::new("file")?,
                target,
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_metadata_plan(refused_plan()?),
        )
        .await
        .err()
        .ok_or("strict metadata refusal became success")?;
        assert_eq!(failure.phase(), TransferPhase::Metadata);
        assert!(failure.published_data().is_none());
        assert_eq!(
            std::fs::read(target_root.path().join("file"))?,
            if policy == TransferPolicy::Direct {
                b"new payload"
            } else {
                b"old payload"
            }
        );
        if policy != TransferPolicy::Direct {
            failure.discard_stage().await?;
        }
        assert_eq!(
            std::fs::read(source_root.path().join("file"))?,
            b"new payload"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_crash_after_data_only_publication_cannot_recover_as_metadata_success() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let payload = vec![0x5a; 3 * 64 * 1024 + 17];
    std::fs::write(source_root.path().join("file"), &payload)?;
    let (source, reader) = test_source_storage(source_root.path(), "partial-crash-source")?;
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "partial-crash-target")?;
    role.set_automatic_checkpoint_interval(64 * 1024);
    role.fail_after_publication_commit();
    let request = TransferRequest::new(
        source,
        StoragePath::new("file")?,
        target,
        CancellationToken::new(),
    )
    .with_metadata_plan(refused_plan()?)
    .with_metadata_failure_policy(MetadataFailurePolicy::PublishData);
    let failure = transfer(request.clone())
        .await
        .err()
        .ok_or("post-commit fault ignored")?;
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(failure.final_destination_changed() && failure.metadata_failure().is_some());
    assert!(
        failure
            .to_string()
            .contains("metadata application also failed")
    );
    assert!(failure.published_data().is_none());
    assert_eq!(std::fs::read(target_root.path().join("file"))?, payload);
    drop(failure);
    let reads = reader.read_call_count();
    let recovered = transfer(request.clone())
        .await
        .err()
        .ok_or("incomplete metadata recovered as success")?;
    assert_eq!(recovered.phase(), TransferPhase::Metadata);
    assert!(recovered.has_pending_cleanup());
    assert!(recovered.published_data().is_none());
    assert_eq!(reader.read_call_count(), reads);
    let recovered = recovered
        .cleanup_for_policy(TransferPolicy::Checkpointed)
        .await;
    assert!(recovered.cleanup_failure().is_none());
    assert_eq!(std::fs::read(target_root.path().join("file"))?, payload);
    transfer(request.with_metadata_mode(MetadataTransferMode::CallerManaged)).await?;
    assert_eq!(std::fs::read(source_root.path().join("file"))?, payload);
    Ok(())
}

#[tokio::test]
async fn completed_data_only_checkpoint_releases_its_pointer_and_keeps_the_payload() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let payload = vec![0x5a; 3 * 64 * 1024 + 17];
    std::fs::write(source_root.path().join("file"), &payload)?;
    let source = test_source_storage(source_root.path(), "partial-release-source")?.0;
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "partial-release-target")?;
    role.set_automatic_checkpoint_interval(64 * 1024);
    let plan = refused_plan()?;
    let failure = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file")?,
            target,
            CancellationToken::new(),
        )
        .with_metadata_plan(Arc::clone(&plan))
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
    )
    .await
    .err()
    .ok_or("metadata refusal became success")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    let data = failure
        .published_data()
        .ok_or("completed data proof missing")?;
    assert_eq!(data.recovery, super::EffectiveRecovery::Checkpointed);
    assert_eq!(data.transferred_bytes, payload.len() as u64);
    assert_eq!(Arc::strong_count(&plan), 1);
    assert!(!failure.has_unpublished_stage() && !failure.has_pending_cleanup());
    for entry in std::fs::read_dir(target_root.path())? {
        assert!(
            !entry?
                .file_name()
                .to_string_lossy()
                .starts_with(".data-mover-")
        );
    }
    assert_eq!(std::fs::read(target_root.path().join("file"))?, payload);
    assert_eq!(std::fs::read(source_root.path().join("file"))?, payload);
    Ok(())
}

#[tokio::test]
async fn corrupt_data_is_never_published_under_the_metadata_failure_policy() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"new payload")?;
    std::fs::write(target_root.path().join("file"), b"old payload")?;
    let source = test_source_storage(source_root.path(), "corrupt-partial-source")?.0;
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "corrupt-partial-target")?;
    role.corrupt_before_verify();
    let failure = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file")?,
            target,
            CancellationToken::new(),
        )
        .with_metadata_plan(refused_plan()?)
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
    )
    .await
    .err()
    .ok_or("corrupt data published")?;
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(failure.published_data().is_none() && failure.metadata_failure().is_none());
    assert_eq!(
        std::fs::read(target_root.path().join("file"))?,
        b"old payload"
    );
    failure.discard_stage().await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_does_not_publish_a_completed_stage() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"new payload")?;
    std::fs::write(target_root.path().join("file"), b"old payload")?;
    let source = test_source_storage(source_root.path(), "cancel-partial-source")?.0;
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "cancel-partial-target")?;
    role.pause_verification();
    let cancel = CancellationToken::new();
    let request = TransferRequest::new(source, StoragePath::new("file")?, target, cancel.clone())
        .with_metadata_plan(refused_plan()?)
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData);
    let task = tokio::spawn(transfer(request));
    timeout(Duration::from_secs(2), async {
        while !role.verification_started() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    cancel.cancel();
    let failure = task.await?.err().ok_or("cancelled metadata published")?;
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(failure.published_data().is_none() && !failure.final_destination_changed());
    assert_eq!(
        std::fs::read(target_root.path().join("file"))?,
        b"old payload"
    );
    failure.discard_stage().await?;
    Ok(())
}
