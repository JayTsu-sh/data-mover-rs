//! 数据发布与调用方元数据阶段分离时，不得重新观察源端元数据。

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryOperationFailure, FailureClass, MetadataObservations, ObservationMode, ObservationPlan,
    Operation, StoragePath, Transience,
};
use crate::storage::backends::local::{test_destination_storage, test_source_storage};
use crate::storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure};
use crate::transfer::{MetadataTransferMode, TransferPolicy, TransferRequest, transfer};

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
    std::fs::write(source_root.path().join("file"), b"readable data")?;
    let original = test_source_storage(source_root.path(), "caller-source")?.0;
    let reads = Arc::new(AtomicUsize::new(0));
    let source = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .metadata(Arc::new(FailingMetadata(Arc::clone(&reads))))
        .build()?;
    let target = test_destination_storage(target_root.path(), "caller-target")?;
    let outcome = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file")?,
            target,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed)
        .with_metadata_mode(MetadataTransferMode::CallerManaged),
    )
    .await?;
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    assert!(outcome.metadata.is_none());
    assert_eq!(
        std::fs::read(target_root.path().join(Path::new("file")))?,
        b"readable data"
    );
    assert!(source_root.path().join("file").exists());
    Ok(())
}
