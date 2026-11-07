//! 在真实本地文件系统上验证身份检查和非递归清理。

use super::{Result, connect, path};
use data_mover::model::{FailureClass, ObservationMode, ObservationPlan, ObservedEntry};
use data_mover::storage::{PurgeOutcome, Storage, StorageRoleFailure, observe_descriptor};
use tokio_util::sync::CancellationToken;

async fn observed(storage: &Storage, name: &str) -> Result<ObservedEntry> {
    let descriptor = storage.namespace()?.stat(&path(name)?).await?;
    Ok(observe_descriptor(
        storage,
        descriptor,
        ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
    )
    .await?)
}

#[tokio::test]
async fn purge_is_idempotent_and_refuses_a_changed_source() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "file").await?;
    std::fs::write(root.path().join("file"), b"uncopied and longer")?;
    assert!(
        matches!(storage.namespace()?.purge_observed(&entry, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(
        std::fs::read(root.path().join("file"))?,
        b"uncopied and longer"
    );
    let entry = observed(&storage, "file").await?;
    assert_eq!(
        storage
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::Deleted { records: 1 }
    );
    assert_eq!(
        storage
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::AlreadyAbsent
    );
    Ok(())
}

#[tokio::test]
async fn purge_does_not_recurse_and_accepts_directory_mtime_changes_after_child_cleanup() -> Result
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("tree"))?;
    std::fs::write(root.path().join("tree/file"), b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "tree").await?;
    assert!(
        storage
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(root.path().join("tree/file"))?, b"copied");
    let file = observed(&storage, "tree/file").await?;
    storage
        .namespace()?
        .purge_observed(&file, CancellationToken::new())
        .await?;
    storage
        .namespace()?
        .purge_observed(&entry, CancellationToken::new())
        .await?;
    assert!(!root.path().join("tree").exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_retains_source() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "file").await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        matches!(storage.namespace()?.purge_observed(&entry, cancel).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    assert!(root.path().join("file").exists());
    Ok(())
}
