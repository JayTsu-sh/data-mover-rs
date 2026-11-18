//! Direct 默认保留目标；只有显式授权的数据失败才能清理本次坏输出。
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::metadata_failure_tests::refused_plan;

use crate::model::{FailureClass, StoragePath};
use crate::storage::backends::local::{test_destination_storage_with_role, test_source_storage};
use crate::storage::{Storage, StorageRoleFailure};
use crate::transfer::{
    FailedDataCleanup, MetadataFailurePolicy, PublicationDurability, TransferPhase, TransferPolicy,
    TransferRequest, transfer,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn request(
    source: Storage,
    target: Storage,
) -> Result<TransferRequest, Box<dyn std::error::Error>> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new("file")?,
        target,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct)
    .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned))
}

#[tokio::test]
async fn opted_in_bad_data_is_removed_without_claiming_unpublished_recovery() -> TestResult {
    for existing in [false, true] {
        let source = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"source payload")?;
        if existing {
            std::fs::write(target.path().join("file"), b"old payload")?;
        }
        let (destination, writer) =
            test_destination_storage_with_role(target.path(), "cleanup-verify")?;
        writer.corrupt_before_verify();
        let failure = transfer(request(
            test_source_storage(source.path(), "cleanup-source")?.0,
            destination,
        )?)
        .await
        .err()
        .ok_or("corrupt output succeeded")?;
        assert_eq!(failure.phase(), TransferPhase::Verify);
        assert!(failure.has_failed_data_cleanup());
        assert!(!failure.has_unpublished_stage());
        assert!(!failure.has_recoverable_stage());
        assert!(!failure.has_pending_cleanup());
        let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
        assert!(failure.cleanup_failure().is_none(), "{failure}");
        assert!(!target.path().join("file").exists());
        assert_eq!(
            std::fs::read(source.path().join("file"))?,
            b"source payload"
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_after_a_data_error_is_retained_with_both_causes() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"source payload")?;
    let (destination, writer) =
        test_destination_storage_with_role(target.path(), "cleanup-replaced")?;
    writer.corrupt_before_verify();
    let failure = transfer(request(
        test_source_storage(source.path(), "cleanup-source")?.0,
        destination,
    )?)
    .await
    .err()
    .ok_or("corrupt output succeeded")?;
    std::fs::rename(target.path().join("file"), target.path().join("held"))?;
    std::fs::write(target.path().join("file"), b"unrelated replacement")?;
    let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(
        matches!(failure.cleanup_failure(), Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );
    assert!(failure.to_string().contains("direct data cleanup failed"));
    assert_eq!(
        std::fs::read(target.path().join("file"))?,
        b"unrelated replacement"
    );
    assert!(target.path().join("held").is_file());
    Ok(())
}

#[tokio::test]
async fn unknown_persistence_failure_never_authorizes_output_removal() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"source payload")?;
    let (destination, writer) =
        test_destination_storage_with_role(target.path(), "cleanup-publish")?;
    writer.fail_direct_data_sync();
    let failure = transfer(
        request(
            test_source_storage(source.path(), "cleanup-source")?.0,
            destination,
        )?
        .with_publication_durability(PublicationDurability::Required),
    )
    .await
    .err()
    .ok_or("failed persistence succeeded")?;
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(!failure.has_failed_data_cleanup());
    let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(
        std::fs::read(target.path().join("file"))?,
        b"source payload"
    );
    assert_eq!(
        std::fs::read(source.path().join("file"))?,
        b"source payload"
    );
    Ok(())
}

#[tokio::test]
async fn opted_in_metadata_failure_keeps_published_data() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"source payload")?;
    let (destination, _) = test_destination_storage_with_role(target.path(), "cleanup-metadata")?;
    let failure = transfer(
        request(
            test_source_storage(source.path(), "cleanup-source")?.0,
            destination,
        )?
        .with_metadata_plan(refused_plan()?)
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
    )
    .await
    .err()
    .ok_or("metadata refusal succeeded")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    assert!(failure.published_data().is_some());
    assert!(!failure.has_failed_data_cleanup());
    let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(
        std::fs::read(target.path().join("file"))?,
        b"source payload"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_writes_settle_before_owned_data_is_removed() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let payload = vec![0x5a; 4 * 1024 * 1024];
    std::fs::write(source.path().join("file"), &payload)?;
    let (origin, reader) = test_source_storage(source.path(), "cleanup-cancel")?;
    reader.delay_reads(Duration::from_millis(200));
    let (destination, _) = test_destination_storage_with_role(target.path(), "cleanup-cancel")?;
    let cancel = CancellationToken::new();
    let work = TransferRequest::new(
        origin,
        StoragePath::new("file")?,
        destination,
        cancel.clone(),
    )
    .with_transfer_policy(TransferPolicy::Direct)
    .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned);
    let operation = tokio::spawn(transfer(work));
    let deadline = Instant::now() + Duration::from_secs(2);
    while !target.path().join("file").exists() {
        if Instant::now() >= deadline {
            return Err("Direct target was not prepared".into());
        }
        sleep(Duration::from_millis(1)).await;
    }
    cancel.cancel();
    let failure = operation
        .await?
        .err()
        .ok_or("cancelled data became success")?;
    assert!(failure.has_failed_data_cleanup());
    let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
    assert!(failure.cleanup_failure().is_none(), "{failure}");
    assert!(!target.path().join("file").exists());
    sleep(Duration::from_millis(50)).await;
    assert!(
        !target.path().join("file").exists(),
        "a writer outlived cleanup"
    );
    assert_eq!(std::fs::read(source.path().join("file"))?, payload);
    Ok(())
}
