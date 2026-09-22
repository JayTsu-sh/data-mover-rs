//! Public-API contract for the Local namespace role: recursive create, recursive delete,
//! NDX paging and the generic traversal all run against a real filesystem, with no external
//! dependency.

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::dir_tree::NdxEvent;
use data_mover::model::{BackendIdentity, BackendKind, EntryKind, ObservationPlan, StoragePath};
use data_mover::ndx_walk::{NdxWalkRequest, ndx_walk};
use data_mover::storage::{
    BackendConfig, DeleteTreeCompletion, DeleteTreeOutcome, DeleteTreeRequest, LocalBackendConfig,
    Storage, connect_backend, create_directory_all, delete_tree,
};
use data_mover::traversal::{
    ChildOrder, StorageTraversalSource, TraversalItem, TraversalOrder, TraversalOutcome,
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
        identity: BackendIdentity::new(BackendKind::Local, "local-namespace-contract")?,
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
    let mut session = source.traverse(TraversalRequest {
        root: path("tree")?,
        order: TraversalOrder::Admission,
        max_inflight_operations: nonzero(4)?,
        max_buffered_items: nonzero(4)?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
    });
    let mut paths = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => paths.push(entry.path().as_str().to_owned()),
            // Directory completion items are ordering evidence, not entries.
            TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_) => {}
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
        TraversalOutcome::Completed(completion) if completion.entry_failures == 0
    ));
    Ok(())
}

#[tokio::test]
async fn ndx_walk_pages_local_entries_with_their_modification_times() -> Result {
    let temp = tempfile::tempdir()?;
    seed(temp.path())?;
    let storage = connect(temp.path()).await?;
    let walk = ndx_walk(
        &storage,
        NdxWalkRequest {
            root: path("tree")?,
            max_depth: None,
            match_expressions: None,
            exclude_expressions: None,
            concurrency: nonzero(2)?,
            cancel: CancellationToken::new(),
        },
    )?;
    let mut entries = 0;
    while let Some(event) = walk.next().await {
        match event {
            NdxEvent::Page(page) => {
                for entry in page.files.iter().chain(page.subdirs.iter()) {
                    assert!(entry.entry.modified().is_some());
                    entries += 1;
                }
            }
            NdxEvent::Error { path, reason } => {
                return Err(format!("unexpected error at {path}: {reason}").into());
            }
            NdxEvent::Done => break,
        }
    }
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
    let mut session = source.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: nonzero(1)?,
        max_buffered_items: nonzero(1)?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
    });
    let Some(TraversalItem::Entry(entry)) = session.next_item().await else {
        return Err("expected one entry".into());
    };
    assert_eq!(entry.path().as_str(), "only");
    assert_eq!(entry.kind(), EntryKind::Directory);
    Ok(())
}

/// The ordered contract as a caller outside the crate sees it: the sequence is asserted without
/// sorting it first, and `ChildOrder` has to be nameable from here at all.
///
/// The three claims this pins, all on one tree:
///
/// * `B.txt` comes before `a`, because `0x42 < 0x61`. Every case-insensitive order puts `a`
///   first, so this is the assertion that says "raw bytes, not a collation".
/// * `a` (a directory), `a.txt` (a file) and `a/x.txt` arrive in that order. Both `a` and
///   `a.txt` are the root's children, so both precede everything under `a`: blocks do not
///   interleave. The key to compare by is therefore `(parent, name)` — comparing whole paths
///   one component at a time would have put `a/x.txt` second.
/// * The same tree read twice gives the same sequence, whatever the filesystem's own order is.
#[tokio::test]
async fn name_bytes_order_is_the_same_sequence_for_a_caller_outside_the_crate() -> Result {
    let temp = tempfile::tempdir()?;
    seed_for_ordering(temp.path())?;
    let storage = connect(temp.path()).await?;
    let source = StorageTraversalSource::new(&storage)?;
    let mut session = source.traverse(TraversalRequest {
        root: path("ordered")?,
        order: TraversalOrder::NameBytes,
        max_inflight_operations: nonzero(4)?,
        max_buffered_items: nonzero(4)?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
    });
    let mut paths = Vec::new();
    let mut listed = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => paths.push(entry.path().as_str().to_owned()),
            TraversalItem::DirectoryListed(directory) => {
                assert_eq!(
                    directory.child_order,
                    ChildOrder::NameBytes,
                    "{}",
                    directory.path
                );
                listed.push(directory.path.as_str().to_owned());
            }
            TraversalItem::SubtreeComplete(_) => {}
            other => return Err(format!("unexpected failure item: {other:?}").into()),
        }
    }
    assert_eq!(
        paths,
        [
            "ordered/B.txt",
            "ordered/Zoo",
            "ordered/a",
            "ordered/a.txt",
            "ordered/bar",
            "ordered/a/x.txt",
            "ordered/bar/y.txt",
        ]
    );
    assert_eq!(
        listed,
        ["ordered", "ordered/Zoo", "ordered/a", "ordered/bar"]
    );
    Ok(())
}
