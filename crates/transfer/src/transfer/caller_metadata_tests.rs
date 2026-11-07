//! 数据发布与调用方元数据阶段分离时，不得重新观察源端元数据。

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use crate::metadata::{
    AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
    OwnershipTarget, TimestampTargetCapability, ValueTarget, compile_metadata_plan,
};
use crate::model::{
    EntryOperationFailure, FailureClass, MetadataObservations, ObservationMode, ObservationPlan,
    Operation, StoragePath, Transience,
};
#[cfg(unix)]
use crate::storage::Storage;
#[cfg(unix)]
use crate::storage::backends::local::test_destination_storage_with_role;
use crate::storage::backends::local::{test_destination_storage, test_source_storage};
use crate::storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure};
use crate::transfer::{MetadataTransferMode, TransferPolicy, TransferRequest, transfer};
#[cfg(unix)]
use crate::transfer::{PublicationDurability, TransferPhase};

struct FailingMetadata(Arc<AtomicUsize>);

#[async_trait]
impl Metadata for FailingMetadata {
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(ObservationPlan::default().with_timestamps(ObservationMode::Required))
    }

    async fn observe(
        &self,
        path: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Metadata,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "source metadata refused",
            )
            .unwrap(),
        ))
    }

    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("source metadata must never be written")
    }
}

#[tokio::test]
async fn caller_managed_copy_publishes_data_without_source_metadata_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let original = test_source_storage(source_root.path(), "caller-source")?.0;
    let reads = Arc::new(AtomicUsize::new(0));
    let source = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .metadata(Arc::new(FailingMetadata(Arc::clone(&reads))))
        .build()?;
    let target = test_destination_storage(target_root.path(), "caller-target")?;
    let policies = [TransferPolicy::AtomicReplace, TransferPolicy::Checkpointed]
        .into_iter()
        .chain(
            target
                .staged_destination()?
                .supports_direct()
                .then_some(TransferPolicy::Direct),
        );
    for policy in policies {
        let path = format!("file-{policy:?}");
        std::fs::write(source_root.path().join(&path), b"readable data")?;
        let outcome = transfer(
            TransferRequest::new(
                source.clone(),
                StoragePath::new(&path)?,
                target.clone(),
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_metadata_mode(MetadataTransferMode::CallerManaged),
        )
        .await?;
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert!(outcome.metadata.is_none());
        assert_eq!(
            std::fs::read(target_root.path().join(Path::new(&path)))?,
            b"readable data"
        );
        assert!(source_root.path().join(&path).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(target_root.path().join(path))?
                    .permissions()
                    .mode()
                    & 0o077,
                0
            );
        }
    }
    Ok(())
}

#[cfg(unix)]
fn spied_source(
    original: &Storage,
    reads: &Arc<AtomicUsize>,
) -> Result<Storage, Box<dyn std::error::Error>> {
    Ok(
        StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
            .read_source(original.read_source()?)
            .metadata(Arc::new(FailingMetadata(Arc::clone(reads))))
            .build()?,
    )
}

#[cfg(unix)]
async fn capture_xattrs(
    source: &Storage,
    path: &StoragePath,
) -> Result<Arc<MetadataPlan>, Box<dyn std::error::Error>> {
    let captured = source
        .metadata()?
        .observe(
            path,
            ObservationPlan::default().with_xattrs(ObservationMode::Required),
        )
        .await?;
    Ok(Arc::new(compile_metadata_plan(&MetadataPlanRequest {
        observations: &captured,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::Supported,
            tags: ValueTarget::NotApplicable,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::NotApplicable,
        },
        policies: MetadataPolicies::default().with_xattrs(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?))
}

#[cfg(unix)]
#[tokio::test]
async fn a_shared_captured_plan_applies_values_without_any_later_source_metadata_request()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let original = test_source_storage(source_root.path(), "captured-source")?.0;
    let reads = Arc::new(AtomicUsize::new(0));
    let source = spied_source(&original, &reads)?;
    let target = test_destination_storage(target_root.path(), "captured-target")?;
    for policy in [
        TransferPolicy::AtomicReplace,
        TransferPolicy::Checkpointed,
        TransferPolicy::Direct,
    ] {
        let name = format!("captured-{policy:?}");
        let path = StoragePath::new(&name)?;
        std::fs::write(source_root.path().join(&name), b"readable payload")?;
        xattr::set(
            source_root.path().join(&name),
            "user.captured-plan",
            b"captured value",
        )?;
        let plan = capture_xattrs(&original, &path).await?;
        let request = TransferRequest::new(
            source.clone(),
            path,
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(policy)
        .with_metadata_plan(Arc::clone(&plan));
        assert_eq!(Arc::strong_count(&plan), 2);
        let outcome = transfer(request.clone()).await?;
        assert!(outcome.metadata.is_some());
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert_eq!(Arc::strong_count(&plan), 2);
        drop(request);
        assert_eq!(Arc::strong_count(&plan), 1);
        assert_eq!(
            std::fs::read(target_root.path().join(&name))?,
            b"readable payload"
        );
        assert_eq!(
            xattr::get(target_root.path().join(&name), "user.captured-plan")?.as_deref(),
            Some(b"captured value".as_slice())
        );
        assert_eq!(
            std::fs::read(source_root.path().join(&name))?,
            b"readable payload"
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn an_explicit_metadata_mode_replaces_a_previously_supplied_plan()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in [
        MetadataTransferMode::CallerManaged,
        MetadataTransferMode::Automatic,
    ] {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"payload")?;
        xattr::set(
            source_root.path().join("file"),
            "user.captured-plan",
            b"captured value",
        )?;
        let path = StoragePath::new("file")?;
        let original = test_source_storage(source_root.path(), "replaced-plan-source")?.0;
        let plan = capture_xattrs(&original, &path).await?;
        let reads = Arc::new(AtomicUsize::new(0));
        let request = TransferRequest::new(
            spied_source(&original, &reads)?,
            path,
            test_destination_storage(target_root.path(), "replaced-plan-target")?,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_metadata_plan(Arc::clone(&plan))
        .with_metadata_mode(mode);
        assert_eq!(Arc::strong_count(&plan), 1);
        let result = transfer(request).await;
        if mode == MetadataTransferMode::CallerManaged {
            assert!(result?.metadata.is_none());
            assert_eq!(reads.load(Ordering::Relaxed), 0);
            assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
            assert!(xattr::get(target_root.path().join("file"), "user.captured-plan")?.is_none());
        } else {
            let failure = result
                .err()
                .ok_or("automatic metadata refusal was ignored")?;
            assert!(failure.to_string().contains("source metadata refused"));
            assert_eq!(reads.load(Ordering::Relaxed), 1);
            assert!(!target_root.path().join("file").exists());
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_failed_captured_metadata_barrier_releases_the_plan_and_preserves_source_and_old_target()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"source payload")?;
    xattr::set(
        source_root.path().join("file"),
        "user.captured-plan",
        b"captured value",
    )?;
    std::fs::write(target_root.path().join("file"), b"old target")?;
    let original = test_source_storage(source_root.path(), "captured-failure-source")?.0;
    let path = StoragePath::new("file")?;
    let plan = capture_xattrs(&original, &path).await?;
    let reads = Arc::new(AtomicUsize::new(0));
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "captured-failure-target")?;
    role.fail_metadata_sync();
    let result = transfer(
        TransferRequest::new(
            spied_source(&original, &reads)?,
            path,
            target,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed)
        .with_publication_durability(PublicationDurability::Required)
        .with_metadata_plan(Arc::clone(&plan)),
    )
    .await;
    let failure = result
        .err()
        .ok_or("metadata persistence failure was accepted")?;
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    assert!(failure.metadata_failure().is_some());
    assert!(!failure.final_destination_changed());
    assert_eq!(Arc::strong_count(&plan), 1);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    assert_eq!(
        std::fs::read(source_root.path().join("file"))?,
        b"source payload"
    );
    assert_eq!(
        std::fs::read(target_root.path().join("file"))?,
        b"old target"
    );
    failure.discard_stage().await?;
    Ok(())
}
