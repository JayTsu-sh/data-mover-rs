#![cfg(unix)]

use data_mover::model::{
    FailureClass, StoragePath, StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding,
    TimePrecision,
};
use data_mover::storage::{
    BackendConfig, CreateSymlinkRequest, LocalBackendConfig, StorageRoleFailure, connect_backend,
};
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn create_link_preserves_target_and_link_times_without_touching_target() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("target"), b"unchanged")?;
    let target_before = std::fs::metadata(root.path().join("target"))?;
    let slots = NonZeroUsize::new(2).ok_or("nonzero")?;
    let storage = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.path().into(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?;
    let namespace = storage.namespace()?;
    let time = StorageTimestamp::new(1_700_000_000_123_456_789, TimePrecision::Nanoseconds)?;
    let request = CreateSymlinkRequest {
        path: StoragePath::new("link")?,
        target: SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec())?,
        uid: None,
        gid: None,
        accessed: Some(time),
        modified: Some(time),
    };
    namespace.create_symlink(request.clone()).await?;
    assert_eq!(namespace.read_link(&request.path).await?, request.target);
    let link = std::fs::symlink_metadata(root.path().join("link"))?;
    assert!(link.file_type().is_symlink());
    assert_eq!(
        (link.mtime(), link.mtime_nsec()),
        (1_700_000_000, 123_456_789)
    );
    let target_after = std::fs::metadata(root.path().join("target"))?;
    assert_eq!(
        (target_before.mtime(), target_before.mtime_nsec()),
        (target_after.mtime(), target_after.mtime_nsec())
    );
    assert!(
        matches!(namespace.create_symlink(request).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );
    Ok(())
}

#[tokio::test]
async fn create_link_refuses_symlink_parents_and_escaping_destinations() -> Result {
    let root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))?;
    let slots = NonZeroUsize::new(1).ok_or("nonzero")?;
    let storage = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.path().into(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?;
    let namespace = storage.namespace()?;
    for (path, target) in [
        ("escape/link", "target"),
        ("../link", "target"),
        ("/link", "target"),
    ] {
        assert!(
            namespace
                .create_symlink(CreateSymlinkRequest {
                    path: StoragePath::new(path)?,
                    target: SymlinkTarget::new(
                        SymlinkTargetEncoding::UnixBytes,
                        target.as_bytes().to_vec()
                    )?,
                    uid: None,
                    gid: None,
                    accessed: None,
                    modified: None,
                })
                .await
                .is_err()
        );
    }
    assert!(!outside.path().join("link").exists());
    assert!(std::fs::symlink_metadata(root.path().join("link")).is_err());
    Ok(())
}

#[tokio::test]
async fn create_link_preserves_opaque_absolute_parent_and_non_utf8_targets() -> Result {
    let root = tempfile::tempdir()?;
    let outside = tempfile::NamedTempFile::new()?;
    let before = outside.as_file().metadata()?;
    let slots = NonZeroUsize::new(1).ok_or("nonzero")?;
    let storage = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.path().into(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?;
    let namespace = storage.namespace()?;
    let targets = [
        b"../outside".to_vec(),
        outside.path().as_os_str().as_bytes().to_vec(),
        b"opaque-\xff-target".to_vec(),
    ];
    for (index, target) in targets.into_iter().enumerate() {
        let request = CreateSymlinkRequest {
            path: StoragePath::new(format!("link-{index}"))?,
            target: SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, target)?,
            uid: None,
            gid: None,
            accessed: None,
            modified: Some(StorageTimestamp::new(
                1_700_000_000_000_000_000,
                TimePrecision::Nanoseconds,
            )?),
        };
        namespace.create_symlink(request.clone()).await?;
        assert_eq!(namespace.read_link(&request.path).await?, request.target);
    }
    let after = outside.as_file().metadata()?;
    assert_eq!(
        (before.mtime(), before.mtime_nsec()),
        (after.mtime(), after.mtime_nsec())
    );
    Ok(())
}
