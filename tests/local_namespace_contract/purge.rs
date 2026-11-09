//! 在真实本地文件系统上验证身份检查和非递归清理。

use super::{Result, connect, path};
use data_mover::model::{FailureClass, ObservationMode, ObservationPlan, ObservedEntry};
#[cfg(unix)]
use data_mover::storage::PurgeOutcome;
use data_mover::storage::{Storage, StorageRoleFailure, observe_descriptor};
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use filetime::{FileTime, set_file_mtime};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
#[cfg(unix)]
use std::time::Duration;

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
#[cfg(unix)]
async fn purge_is_idempotent_and_refuses_a_changed_source() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "file").await?;
    std::fs::write(root.path().join("file"), b"uncopied and longer")?;
    assert!(
        matches!(storage.namespace()?.purge_observed(&entry, CancellationToken::new()).await,
        Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict))
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
#[cfg(unix)]
async fn purge_does_not_recurse_and_accepts_directory_mtime_changes_after_child_cleanup() -> Result
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("tree"))?;
    std::fs::write(root.path().join("tree/file"), b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "tree").await?;
    let before = std::fs::metadata(root.path().join("tree"))?;
    assert!(
        storage
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(root.path().join("tree/file"))?, b"copied");
    let file = observed(&storage, "tree/file").await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    storage
        .namespace()?
        .purge_observed(&file, CancellationToken::new())
        .await?;
    let after = std::fs::metadata(root.path().join("tree"))?;
    assert_ne!(
        (before.ctime(), before.ctime_nsec()),
        (after.ctime(), after.ctime_nsec())
    );
    storage
        .namespace()?
        .purge_observed(&entry, CancellationToken::new())
        .await?;
    assert!(!root.path().join("tree").exists());
    Ok(())
}

#[cfg(not(unix))]
#[tokio::test]
async fn purge_without_stable_identity_and_change_evidence_retains_every_kind() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"retained")?;
    std::fs::create_dir(root.path().join("directory"))?;
    let storage = connect(root.path()).await?;
    for name in ["file", "directory"] {
        let entry = observed(&storage, name).await?;
        assert!(
            matches!(storage.namespace()?.purge_observed(&entry, CancellationToken::new()).await,
            Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Unsupported))
        );
        assert!(root.path().join(name).exists());
    }
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
        Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Cancelled))
    );
    assert!(root.path().join("file").exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn purge_refuses_equal_length_rewrites_even_when_mtime_is_restored() -> Result {
    let root = tempfile::tempdir()?;
    let file = root.path().join("file");
    std::fs::write(&file, b"copied")?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "file").await?;
    let snapshot = entry.encode_snapshot();
    let entry = ObservedEntry::decode_snapshot(snapshot.as_bytes(), storage.identity())?;
    let before = std::fs::metadata(&file)?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    std::fs::write(&file, b"newest")?;
    set_file_mtime(&file, FileTime::from_last_modification_time(&before))?;
    let after = std::fs::metadata(&file)?;
    assert_eq!(
        (before.dev(), before.ino(), before.len()),
        (after.dev(), after.ino(), after.len())
    );
    assert_eq!(
        FileTime::from_last_modification_time(&before),
        FileTime::from_last_modification_time(&after)
    );
    assert_ne!(
        (before.ctime(), before.ctime_nsec()),
        (after.ctime(), after.ctime_nsec())
    );
    assert!(
        matches!(storage.namespace()?.purge_observed(&entry, CancellationToken::new()).await,
        Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict
            && error.diagnostic().contains("mtime/ctime changed")))
    );
    assert_eq!(std::fs::read(file)?, b"newest");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn purge_retains_files_without_current_change_evidence() -> Result {
    for facts in [None, Some(vec![2]), Some(vec![1; 57])] {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"retained")?;
        let storage = connect(root.path()).await?;
        let captured = observed(&storage, "file").await?;
        let mut entry = ObservedEntry::new(
            captured.path().clone(),
            captured.kind(),
            captured.size(),
            captured.modified(),
            captured.source_identity().clone(),
        )?;
        if let Some(bytes) = facts {
            entry = entry.with_backend_fact_bytes(bytes)?;
        }
        assert!(
            matches!(storage.namespace()?.purge_observed(&entry, CancellationToken::new()).await,
            Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Unsupported
                && error.diagnostic().contains("change evidence")))
        );
        assert_eq!(std::fs::read(root.path().join("file"))?, b"retained");
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn protected_directory_purge_retains_every_hidden_child_and_subtree() -> Result {
    let root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("tree"))?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "tree").await?;
    let hidden = root.path().join("tree/.data-mover-other-task.stage");
    let nested = root
        .path()
        .join("tree/.data-mover-unrelated/nested/private");
    let linked = root.path().join("tree/.data-mover-link");
    std::fs::write(&hidden, b"other task recovery")?;
    std::fs::create_dir_all(nested.parent().ok_or("nested parent")?)?;
    std::fs::write(&nested, b"unrelated subtree")?;
    std::fs::write(outside.path().join("private"), b"outside")?;
    std::os::unix::fs::symlink(outside.path(), &linked)?;
    let result = storage
        .namespace()?
        .purge_observed(&entry, CancellationToken::new())
        .await;
    assert!(
        matches!(&result, Err(purge) if matches!(purge.source.as_ref(), StorageRoleFailure::Entry(error)
        if error.class() == FailureClass::Conflict && error.diagnostic().contains("not empty"))),
        "{result:?}"
    );
    assert_eq!(std::fs::read(hidden)?, b"other task recovery");
    assert_eq!(std::fs::read(nested)?, b"unrelated subtree");
    assert!(std::fs::symlink_metadata(linked)?.is_symlink());
    assert_eq!(std::fs::read(outside.path().join("private"))?, b"outside");
    assert!(root.path().join("tree").is_dir());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn protected_empty_directory_purge_is_successful_and_idempotent() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("tree"))?;
    let storage = connect(root.path()).await?;
    let entry = observed(&storage, "tree").await?;
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
    assert!(!root.path().join("tree").exists());
    Ok(())
}
