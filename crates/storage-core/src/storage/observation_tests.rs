//! 单路径观察的可选失败不会抹掉基础事实；必需属性、取消和会话失败仍保持终止语义。

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{EntryObservationFailure, observe_descriptor};
use crate::model::{
    BackendIdentity, BackendKind, BackendSessionFailure, EntryKind, EntryOperationFailure,
    FailureClass, IdentityStrength, MetadataObservations, ObservationMode, ObservationPlan,
    Operation, OwnershipMode, SourceIdentity, StoragePath, StorageTimestamp, TimePrecision,
    TimestampMetadata, Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, InlineOwnership, Metadata, MetadataMutation,
    SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure, UnsupportedReason,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct FailedMetadata(StorageRoleFailure);

#[async_trait]
impl Metadata for FailedMetadata {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Err(self.0.clone())
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Err(self.0.clone())
    }
}

fn fixture(failure: StorageRoleFailure) -> TestResult<(Storage, SourceDescriptor)> {
    let backend = BackendIdentity::new(BackendKind::Local, "observation-test")?;
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("not needed")?);
    let capabilities = BackendCapabilities::new(
        unavailable.clone(),
        unavailable.clone(),
        unavailable,
        CapabilityAvailability::Supported,
    );
    let storage = StorageBuilder::new(backend.clone(), capabilities)
        .metadata(Arc::new(FailedMetadata(failure)))
        .build()?;
    let path = StoragePath::new("file")?;
    let mut descriptor = SourceDescriptor::new(
        path,
        EntryKind::File,
        Some(7),
        SourceIdentity::new(backend, IdentityStrength::PathScoped, b"file")?,
    );
    descriptor.inline_timestamps = Some(TimestampMetadata {
        accessed: None,
        modified: Some(StorageTimestamp::new(
            3_000_000_000,
            TimePrecision::Seconds,
        )?),
        created: None,
    });
    descriptor.inline_ownership = Some(InlineOwnership::Known(OwnershipMode {
        uid: 1000,
        gid: 100,
        mode: 0o640,
    }));
    Ok((storage, descriptor))
}

fn entry_failure(class: FailureClass) -> TestResult<StorageRoleFailure> {
    Ok(StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("file")?,
        Operation::Metadata,
        class,
        Transience::Permanent,
        "source metadata read refused",
    )?))
}

fn plan(mode: ObservationMode) -> ObservationPlan {
    ObservationPlan::default()
        .with_tags(mode)
        .with_timestamps(ObservationMode::InlineOnly)
        .with_ownership_mode(ObservationMode::InlineOnly)
}

#[tokio::test]
async fn optional_failure_preserves_readable_entry_and_inline_facts() -> TestResult {
    for class in [
        FailureClass::PermissionDenied,
        FailureClass::NotFound,
        FailureClass::Unsupported,
    ] {
        let (storage, descriptor) = fixture(entry_failure(class)?)?;
        let entry =
            observe_descriptor(&storage, descriptor, plan(ObservationMode::BestEffort)).await?;
        assert_eq!(entry.size(), Some(7));
        assert_eq!(
            entry
                .metadata()
                .ownership_mode()
                .value()
                .map(|owner| owner.mode),
            Some(0o640)
        );
        assert_eq!(
            entry.modified().map(StorageTimestamp::unix_nanos),
            Some(3_000_000_000)
        );
        let failures: Vec<_> = entry.metadata().failures().collect();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].class, class);
    }
    Ok(())
}

#[tokio::test]
async fn required_metadata_and_cancellation_remain_typed_errors() -> TestResult {
    for (class, mode) in [
        (FailureClass::PermissionDenied, ObservationMode::Required),
        (FailureClass::Cancelled, ObservationMode::BestEffort),
    ] {
        let (storage, descriptor) = fixture(entry_failure(class)?)?;
        assert!(
            matches!(observe_descriptor(&storage, descriptor, plan(mode)).await,
            Err(EntryObservationFailure::Role(StorageRoleFailure::Entry(error))) if error.class() == class)
        );
    }
    Ok(())
}

#[tokio::test]
async fn session_failure_is_not_demoted_to_an_optional_attribute() -> TestResult {
    let failure = StorageRoleFailure::Session(BackendSessionFailure::new(
        Operation::Metadata,
        FailureClass::Connectivity,
        Transience::Transient,
        "source disconnected",
    )?);
    let (storage, descriptor) = fixture(failure.clone())?;
    assert!(
        matches!(observe_descriptor(&storage, descriptor, plan(ObservationMode::BestEffort)).await,
        Err(EntryObservationFailure::Role(error)) if error == failure)
    );
    Ok(())
}
