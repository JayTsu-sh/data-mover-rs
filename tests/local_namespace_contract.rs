//! Public-API contract for the Local namespace role: recursive create, recursive delete,
//! NDX paging and the generic traversal all run against a real filesystem, with no external
//! dependency.

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{EntryKind, StoragePath};
use data_mover::storage::{
    BackendConfig, DeleteTreeCompletion, DeleteTreeOutcome, DeleteTreeRequest, LocalBackendConfig,
    Storage, connect_backend, create_directory_all, delete_tree,
};
use data_mover::traversal::{
    ListingOutcome, StorageTraversalSource, TraversalItem, TraversalOrder, TraversalOutcome,
    TraversalRequest, TraversalSource,
};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn nonzero(value: usize) -> Result<NonZeroUsize> {
    Ok(NonZeroUsize::new(value).ok_or("value must be non-zero")?)
}

async fn connect(root: &Path) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: nonzero(2)?,
        write_concurrency: nonzero(2)?,
    }))
    .await?)
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

/// `tree/{a.txt, sub/{b.txt, deep/c.txt}}`.
fn seed(root: &Path) -> Result {
    std::fs::create_dir_all(root.join("tree/sub/deep"))?;
    std::fs::write(root.join("tree/a.txt"), b"a")?;
    std::fs::write(root.join("tree/sub/b.txt"), b"bb")?;
    std::fs::write(root.join("tree/sub/deep/c.txt"), b"ccc")?;
    Ok(())
}

/// Names chosen so byte order differs from any case-insensitive order (`B.txt` before `a`) and
/// so the directory `a`, the file `a.txt` and the child `a/x.txt` are all present at once.
fn seed_for_ordering(root: &Path) -> Result {
    std::fs::create_dir_all(root.join("ordered/Zoo"))?;
    std::fs::create_dir_all(root.join("ordered/a"))?;
    std::fs::create_dir_all(root.join("ordered/bar"))?;
    std::fs::write(root.join("ordered/B.txt"), b"B")?;
    std::fs::write(root.join("ordered/a.txt"), b"a")?;
    std::fs::write(root.join("ordered/a/x.txt"), b"x")?;
    std::fs::write(root.join("ordered/bar/y.txt"), b"y")?;
    Ok(())
}

#[tokio::test]
async fn create_directory_all_builds_every_missing_level_idempotently() -> Result {
    let temp = tempfile::tempdir()?;
    let storage = connect(temp.path()).await?;
    let target = path("x/y/z")?;
    create_directory_all(&storage, &target).await?;
    create_directory_all(&storage, &target).await?;
    assert!(temp.path().join("x/y/z").is_dir());
    Ok(())
}

#[tokio::test]
async fn traversal_lists_the_whole_tree_through_the_namespace_role() -> Result {
    let temp = tempfile::tempdir()?;
    seed(temp.path())?;
    let storage = connect(temp.path()).await?;
    let source = StorageTraversalSource::new(&storage)?;
    let mut session = source.traverse(
        TraversalRequest::new(path("tree")?)
            .order(TraversalOrder::Blocks)
            .max_inflight_operations(nonzero(4)?)
            .max_buffered_items(nonzero(4)?)
            .cancel(CancellationToken::new()),
    );
    let mut paths = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => paths.push(entry.path().as_str().to_owned()),
            // Directory events are ordering evidence, not entries.
            TraversalItem::DirectoryListed(_) => {}
            other => return Err(format!("unexpected failure item: {other:?}").into()),
        }
    }
    paths.sort();
    assert_eq!(
        paths,
        [
            "tree/a.txt",
            "tree/sub",
            "tree/sub/b.txt",
            "tree/sub/deep",
            "tree/sub/deep/c.txt"
        ]
    );
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(completion)
            if completion.totals.failed_children == 0
                && completion.totals.listing_failures == 0
                && completion.totals.emitted == 5
                && completion.totals.is_balanced()
    ));
    Ok(())
}

#[tokio::test]
async fn traversal_retains_local_modification_times() -> Result {
    let temp = tempfile::tempdir()?;
    seed(temp.path())?;
    let storage = connect(temp.path()).await?;
    let mut session = StorageTraversalSource::new(&storage)?
        .traverse(TraversalRequest::new(path("tree")?).max_inflight_operations(nonzero(2)?));
    let mut entries = 0;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                assert!(entry.modified().is_some());
                entries += 1;
            }
            other => return Err(format!("unexpected traversal item: {other:?}").into()),
        }
    }
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    assert_eq!(entries, 5);
    Ok(())
}

async fn delete(storage: &Storage, root: &str) -> Result<DeleteTreeCompletion> {
    let mut session = delete_tree(
        storage,
        DeleteTreeRequest {
            root: path(root)?,
            delete_root: true,
            max_inflight_operations: nonzero(4)?,
            max_buffered_items: nonzero(4)?,
            cancel: CancellationToken::new(),
        },
    )?;
    while session.next_item().await.is_some() {}
    match session.finish().await? {
        DeleteTreeOutcome::Completed(completion) => Ok(completion),
        DeleteTreeOutcome::Cancelled => Err("unexpected cancellation".into()),
    }
}

#[tokio::test]
async fn delete_tree_removes_the_subtree_and_its_root() -> Result {
    let temp = tempfile::tempdir()?;
    seed(temp.path())?;
    let storage = connect(temp.path()).await?;
    let completion = delete(&storage, "tree").await?;
    assert_eq!(completion.entry_failures, 0);
    assert_eq!(completion.deleted_entries, 3);
    assert_eq!(completion.deleted_directories, 3);
    assert!(!temp.path().join("tree").exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn delete_tree_removes_a_directory_symlink_but_never_its_target() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir_all(temp.path().join("precious"))?;
    std::fs::write(temp.path().join("precious/keep.txt"), b"keep")?;
    std::fs::create_dir(temp.path().join("tree"))?;
    std::os::unix::fs::symlink("../precious", temp.path().join("tree/link"))?;
    let storage = connect(temp.path()).await?;

    let completion = delete(&storage, "tree").await?;
    assert_eq!(completion.entry_failures, 0);
    assert!(!temp.path().join("tree").exists());
    assert!(temp.path().join("precious/keep.txt").exists());
    Ok(())
}

/// The root of the delete is itself a link: nothing under its target may be touched.
#[cfg(unix)]
#[tokio::test]
async fn delete_tree_whose_root_is_a_symlink_never_touches_the_target() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir_all(temp.path().join("precious/sub"))?;
    std::fs::write(temp.path().join("precious/keep.txt"), b"keep")?;
    std::fs::write(temp.path().join("precious/sub/deep.txt"), b"deep")?;
    std::os::unix::fs::symlink("precious", temp.path().join("link"))?;
    let storage = connect(temp.path()).await?;

    let completion = delete(&storage, "link").await?;
    assert_eq!(completion.deleted_entries, 0);
    assert_eq!(completion.deleted_directories, 0);
    assert!(completion.entry_failures > 0);
    assert!(temp.path().join("precious/keep.txt").exists());
    assert!(temp.path().join("precious/sub/deep.txt").exists());
    Ok(())
}

#[tokio::test]
async fn directories_are_listed_as_directories_not_files() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("only"))?;
    let storage = connect(temp.path()).await?;
    let source = StorageTraversalSource::new(&storage)?;
    let mut session = source.traverse(
        TraversalRequest::new(StoragePath::root())
            .order(TraversalOrder::Blocks)
            .max_inflight_operations(nonzero(1)?)
            .max_buffered_items(nonzero(1)?)
            .cancel(CancellationToken::new()),
    );
    let Some(TraversalItem::Entry(entry)) = session.next_item().await else {
        return Err("expected one entry".into());
    };
    assert_eq!(entry.path().as_str(), "only");
    assert_eq!(entry.kind(), EntryKind::Directory);
    Ok(())
}

/// The block contract as a caller outside the crate sees it (`TraversalOrder::Blocks`, P7 §3),
/// asserted on the unsorted sequence, each directory's event readable from here:
///
/// * Every directory's children arrive together, before its `DirectoryListed`, in whatever order
///   the filesystem listed them — so each block is compared as a set.
/// * Subdirectories are descended into by raw name bytes: `Zoo` before `a` before `bar`, because
///   `0x5A < 0x61`. Every case-insensitive order puts `a` first, so this is the assertion that
///   says "raw bytes, not a collation".
/// * `a` (a directory) and `a.txt` (a file) are both in the root's block, so both precede
///   `a/x.txt`: blocks do not interleave, and the key to compare by is `(parent, name)`.
/// * The same tree read twice visits its directories in the same order.
#[tokio::test]
async fn blocks_are_contiguous_and_descended_by_name_for_a_caller_outside_the_crate() -> Result {
    let temp = tempfile::tempdir()?;
    seed_for_ordering(temp.path())?;
    let storage = connect(temp.path()).await?;
    let source = StorageTraversalSource::new(&storage)?;
    let mut runs = Vec::new();
    for _ in 0..2 {
        let mut session = source.traverse(
            TraversalRequest::new(path("ordered")?)
                .order(TraversalOrder::Blocks)
                .max_inflight_operations(nonzero(4)?)
                .max_buffered_items(nonzero(4)?)
                .cancel(CancellationToken::new()),
        );
        let mut blocks: Vec<(String, Vec<String>)> = Vec::new();
        let mut block = Vec::new();
        while let Some(item) = session.next_item().await {
            match item {
                TraversalItem::Entry(entry) => block.push(entry.path().as_str().to_owned()),
                TraversalItem::DirectoryListed(directory) => {
                    assert_eq!(
                        directory.outcome,
                        ListingOutcome::Complete,
                        "{}",
                        directory.path
                    );
                    assert!(directory.counts.is_balanced(), "{directory:?}");
                    block.sort();
                    blocks.push((
                        directory.path.as_str().to_owned(),
                        std::mem::take(&mut block),
                    ));
                }
                other => return Err(format!("unexpected failure item: {other:?}").into()),
            }
        }
        assert!(block.is_empty(), "entries after the last block: {block:?}");
        runs.push(blocks);
    }
    let block = |directory: &str, entries: &[&str]| {
        (
            directory.to_owned(),
            entries
                .iter()
                .map(|entry| (*entry).to_owned())
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        runs[0],
        [
            block(
                "ordered",
                &[
                    "ordered/B.txt",
                    "ordered/Zoo",
                    "ordered/a",
                    "ordered/a.txt",
                    "ordered/bar",
                ],
            ),
            block("ordered/Zoo", &[]),
            block("ordered/a", &["ordered/a/x.txt"]),
            block("ordered/bar", &["ordered/bar/y.txt"]),
        ]
    );
    assert_eq!(runs[0], runs[1]);
    Ok(())
}
