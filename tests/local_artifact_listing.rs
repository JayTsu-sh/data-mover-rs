//! ADR-0006 C3b on a real Local tree: every listing hides `.data-mover-*` transfer artifacts —
//! legacy `walkdir` and role traversal — and every directory delete still
//! removes them.

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::filter::parse_filter_expression;
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, DeleteTreeRequest, LocalBackendConfig, PreflightPolicy, Storage,
    StorageRoleFailure, connect_backend, delete_tree,
};
use data_mover::storage_enum::{StorageEnum, create_storage};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalRequest, TraversalSource as _,
};
use data_mover::{CreateStorageOptions, StorageEntryMessage, WalkOptions};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const VISIBLE: [&str; 3] = ["a.txt", "sub", "sub/b.txt"];

/// A tree with artifacts at every depth, including a file (`y`) whose own name is ordinary but
/// that lies inside an artifact directory.
fn fixture() -> TestResult<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    let base = root.path();
    std::fs::create_dir_all(base.join("sub/.data-mover-stage/x"))?;
    std::fs::create_dir_all(base.join(".data-mover-staging"))?;
    for file in [
        "a.txt",
        "sub/b.txt",
        ".data-mover-0123.stage",
        "sub/.data-mover-feed.upload",
        ".data-mover-staging/old.stage",
        "sub/.data-mover-stage/x/y",
    ] {
        std::fs::write(base.join(file), b"x")?;
    }
    Ok(root)
}

async fn legacy(root: &Path) -> TestResult<StorageEnum> {
    Ok(create_storage(&root.to_string_lossy(), CreateStorageOptions::default()).await?)
}

async fn roles(root: &Path) -> TestResult<Storage> {
    let slots = NonZeroUsize::new(2).ok_or("non-zero")?;
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?)
}

async fn walkdir_paths(storage: &StorageEnum, options: WalkOptions) -> TestResult<Vec<String>> {
    let iter = storage.walkdir(None, options).await?;
    let mut paths = Vec::new();
    while let Some(message) = iter.next().await {
        if let StorageEntryMessage::Scanned(entry) = message {
            paths.push(
                entry
                    .get_relative_path()
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    paths.sort();
    Ok(paths)
}

#[tokio::test]
async fn legacy_walkdir_hides_artifacts_even_under_a_descending_match() -> TestResult {
    let root = fixture()?;
    let storage = legacy(root.path()).await?;
    assert_eq!(
        walkdir_paths(&storage, WalkOptions::default()).await?,
        VISIBLE
    );
    // A match skips directories but keeps descending into them: the artifact check must come first,
    // or `sub/.data-mover-stage/x/y` would be reported.
    let matching = WalkOptions {
        match_expressions: Some(parse_filter_expression(
            "name == \"y\" or name == \"b.txt\"",
        )?),
        ..WalkOptions::default()
    };
    assert_eq!(walkdir_paths(&storage, matching).await?, ["sub/b.txt"]);
    Ok(())
}

#[tokio::test]
async fn role_traversal_hides_artifacts() -> TestResult {
    let root = fixture()?;
    let storage = roles(root.path()).await?;
    let slots = NonZeroUsize::new(2).ok_or("non-zero")?;
    let mut session = StorageTraversalSource::new(&storage)?.traverse(
        TraversalRequest::new(StoragePath::root())
            .max_inflight_operations(slots)
            .max_buffered_items(slots)
            .cancel(CancellationToken::new()),
    );
    let mut traversed = Vec::new();
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = item {
            traversed.push(entry.path().as_str().to_owned());
        }
    }
    traversed.sort();
    assert_eq!(traversed, VISIBLE);

    Ok(())
}

#[tokio::test]
async fn directory_deletes_still_remove_the_artifacts_in_them() -> TestResult {
    let legacy_root = fixture()?;
    let events = legacy(legacy_root.path())
        .await?
        .delete_dir_all_with_progress(Some(Path::new("sub")), 2)?;
    while events.next().await.is_some() {}
    assert!(!legacy_root.path().join("sub").exists());

    let role_root = fixture()?;
    let storage = roles(role_root.path()).await?;
    let slots = NonZeroUsize::new(2).ok_or("non-zero")?;
    let mut session = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::new("sub")?,
            delete_root: true,
            max_inflight_operations: slots,
            max_buffered_items: slots,
            cancel: CancellationToken::new(),
        },
    )?;
    while session.next_item().await.is_some() {}
    session
        .finish()
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    assert!(!role_root.path().join("sub").exists());
    // Artifacts outside the deleted directory are left alone.
    assert!(role_root.path().join(".data-mover-0123.stage").exists());
    Ok(())
}

/// The sweep is only for a directory empty as seen through `List`: a visible entry keeps the
/// directory, the conflict, and every artifact beside it.
#[tokio::test]
async fn deleting_a_directory_with_a_visible_entry_touches_no_artifact() -> TestResult {
    let root = fixture()?;
    let namespace = roles(root.path())
        .await?
        .namespace(&PreflightPolicy::production())?;
    let refused = namespace.delete(&StoragePath::new("sub")?).await;
    assert!(
        matches!(&refused, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Conflict),
        "{refused:?}"
    );
    assert!(root.path().join("sub/b.txt").exists());
    assert!(root.path().join("sub/.data-mover-feed.upload").exists());
    assert!(root.path().join("sub/.data-mover-stage/x/y").exists());
    Ok(())
}

/// A symlinked artifact is removed as a link: the sweep never follows it out of the tree.
#[cfg(unix)]
#[tokio::test]
async fn sweeping_a_symlinked_artifact_removes_the_link_not_its_target() -> TestResult {
    let outside = tempfile::tempdir()?;
    std::fs::write(outside.path().join("precious"), b"keep")?;
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("gone"))?;
    std::os::unix::fs::symlink(outside.path(), root.path().join("gone/.data-mover-link"))?;
    roles(root.path())
        .await?
        .namespace(&PreflightPolicy::production())?
        .delete(&StoragePath::new("gone")?)
        .await?;
    assert!(!root.path().join("gone").exists());
    assert!(outside.path().join("precious").exists());
    Ok(())
}
