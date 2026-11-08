use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::sync::atomic::Ordering;

use super::*;
use crate::storage::Namespace;
use crate::storage::backends::local::test_identity;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn namespace(root: &Path) -> Result<LocalNamespace, Box<dyn std::error::Error>> {
    let root = std::fs::canonicalize(root)?;
    let directory = Arc::new(Dir::open_ambient_dir(&root, ambient_authority())?);
    Ok(
        LocalNamespace::from_root(directory, test_identity("published-persistence"))
            .with_persistence_root(root),
    )
}

#[tokio::test]
async fn files_directories_and_later_metadata_are_persisted_without_body_replay() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("new/endpoint");
    std::fs::create_dir_all(root.join("tree"))?;
    std::fs::write(root.join("tree/file"), b"payload")?;
    let namespace = namespace(&root)?;
    for name in ["tree/file", "tree", ""] {
        let path = crate::model::StoragePath::new(name)?;
        let before = namespace.stat(&path).await?;
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await?;
        assert_eq!(before, namespace.stat(&path).await?);
    }
    std::fs::set_permissions(
        root.join("tree/file"),
        std::fs::Permissions::from_mode(0o640),
    )?;
    let before = namespace
        .stat(&crate::model::StoragePath::new("tree/file")?)
        .await?;
    namespace
        .persist_observed(&before, CancellationToken::new())
        .await?;
    assert_eq!(std::fs::read(root.join("tree/file"))?, b"payload");
    assert_eq!(
        std::fs::metadata(root.join("tree/file"))?
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
    assert!(
        namespace
            .persistence
            .as_ref()
            .is_some_and(|anchor| anchor.directory_sync_calls.load(Ordering::SeqCst) > 0)
    );
    Ok(())
}

#[tokio::test]
async fn stale_data_and_replaced_inodes_are_refused() -> TestResult {
    let root = tempfile::tempdir()?;
    let namespace = namespace(root.path())?;
    std::fs::write(root.path().join("file"), b"before")?;
    let path = crate::model::StoragePath::new("file")?;
    let before = namespace.stat(&path).await?;
    std::fs::write(root.path().join("file"), b"after!")?;
    assert!(
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await
            .is_err()
    );
    let current = namespace.stat(&path).await?;
    std::fs::rename(root.path().join("file"), root.path().join("old"))?;
    std::fs::write(root.path().join("file"), b"after!")?;
    assert!(
        namespace
            .persist_observed(&current, CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(root.path().join("file"))?, b"after!");
    assert_eq!(std::fs::read(root.path().join("old"))?, b"after!");
    Ok(())
}

#[tokio::test]
async fn file_and_directory_sync_failures_and_cancellation_never_confirm() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"payload")?;
    let namespace = namespace(root.path())?;
    let before = namespace
        .stat(&crate::model::StoragePath::new("file")?)
        .await?;
    let anchor = namespace.persistence.as_ref().ok_or("missing anchor")?;
    anchor.fail_directory_sync.store(true, Ordering::SeqCst);
    assert!(
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(anchor.directory_sync_calls.load(Ordering::SeqCst), 1);
    anchor.fail_directory_sync.store(false, Ordering::SeqCst);
    anchor.fail_file_sync.store(true, Ordering::SeqCst);
    assert!(
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await
            .is_err()
    );
    anchor.fail_file_sync.store(false, Ordering::SeqCst);
    namespace
        .persist_observed(&before, CancellationToken::new())
        .await?;
    anchor.fail_directory_sync.store(true, Ordering::SeqCst);
    assert!(
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await
            .is_err()
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(namespace.persist_observed(&before, cancel).await,
        Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Cancelled));
    assert_eq!(std::fs::read(root.path().join("file"))?, b"payload");
    Ok(())
}

#[tokio::test]
async fn symlink_substitution_never_follows_or_modifies_an_unrelated_target() -> TestResult {
    let root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("tree"))?;
    std::fs::write(root.path().join("tree/file"), b"owned")?;
    std::fs::write(outside.path().join("file"), b"unrelated")?;
    let namespace = namespace(root.path())?;
    let before = namespace
        .stat(&crate::model::StoragePath::new("tree/file")?)
        .await?;
    std::fs::rename(root.path().join("tree"), root.path().join("held"))?;
    symlink(outside.path(), root.path().join("tree"))?;
    assert!(
        namespace
            .persist_observed(&before, CancellationToken::new())
            .await
            .is_err()
    );
    std::fs::write(root.path().join("flat"), b"owned")?;
    let flat = namespace
        .stat(&crate::model::StoragePath::new("flat")?)
        .await?;
    std::fs::rename(root.path().join("flat"), root.path().join("held-flat"))?;
    symlink(outside.path().join("file"), root.path().join("flat"))?;
    assert!(
        namespace
            .persist_observed(&flat, CancellationToken::new())
            .await
            .is_err()
    );
    symlink(outside.path().join("file"), root.path().join("link"))?;
    let link = namespace
        .stat(&crate::model::StoragePath::new("link")?)
        .await?;
    assert!(
        matches!(namespace.persist_observed(&link, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Unsupported)
    );
    assert_eq!(std::fs::read(outside.path().join("file"))?, b"unrelated");
    assert_eq!(std::fs::read(root.path().join("held/file"))?, b"owned");
    Ok(())
}

#[tokio::test]
async fn replacing_the_root_before_or_after_its_first_confirmation_is_refused() -> TestResult {
    for confirm_first in [false, true] {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("endpoint");
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("file"), b"owned")?;
        let namespace = namespace(&root)?;
        let before = namespace
            .stat(&crate::model::StoragePath::new("file")?)
            .await?;
        if confirm_first {
            namespace
                .persist_observed(&before, CancellationToken::new())
                .await?;
        }
        std::fs::rename(&root, temporary.path().join("held"))?;
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("file"), b"unrelated")?;
        assert!(
            namespace
                .persist_observed(&before, CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(root.join("file"))?, b"unrelated");
        assert_eq!(std::fs::read(temporary.path().join("held/file"))?, b"owned");
    }
    Ok(())
}

#[tokio::test]
async fn moving_the_same_root_away_and_back_still_syncs_all_ancestors() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("endpoint");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("file"), b"payload")?;
    let namespace = namespace(&root)?;
    let before = namespace
        .stat(&crate::model::StoragePath::new("file")?)
        .await?;
    namespace
        .persist_observed(&before, CancellationToken::new())
        .await?;
    let anchor = namespace.persistence.as_ref().ok_or("missing anchor")?;
    let first = anchor.directory_sync_calls.load(Ordering::SeqCst);
    assert!(
        first > 1,
        "root-only synchronization misses the endpoint's parent"
    );
    std::fs::rename(&root, temporary.path().join("moved"))?;
    std::fs::rename(temporary.path().join("moved"), &root)?;
    assert_eq!(
        before,
        namespace
            .stat(&crate::model::StoragePath::new("file")?)
            .await?
    );
    namespace
        .persist_observed(&before, CancellationToken::new())
        .await?;
    assert_eq!(
        anchor.directory_sync_calls.load(Ordering::SeqCst),
        first * 2
    );
    assert_eq!(std::fs::read(root.join("file"))?, b"payload");
    Ok(())
}
