//! The Unordered traversal (P7 C10) over the Local backend on a real filesystem: the same
//! entries and totals as Blocks, a directory's entry ahead of everything below it, no directory
//! events, listing failures scoped to their directory, and cancellation.

use std::collections::HashMap;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ScanTotals, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource,
};
use crate::model::{EntryKind, FailureClass, StoragePath};
use crate::storage::backends::local::namespace::LocalNamespace;
use crate::storage::backends::local::test_traversal_storage;
use crate::storage::listing::DEFAULT_PAGE_HINT;

fn source(root: &Path) -> io::Result<(StorageTraversalSource, Arc<LocalNamespace>)> {
    let (storage, namespace, _) = test_traversal_storage(root, "local-unordered-test")
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok((
        StorageTraversalSource::new(&storage).map_err(io::Error::other)?,
        namespace,
    ))
}

fn nonzero(value: usize) -> io::Result<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("zero"))
}

fn request(
    order: TraversalOrder,
    inflight: usize,
    buffered: usize,
) -> io::Result<TraversalRequest> {
    Ok(TraversalRequest::new(StoragePath::root())
        .order(order)
        .max_inflight_operations(nonzero(inflight)?)
        .max_buffered_items(nonzero(buffered)?))
}

async fn run(
    source: &StorageTraversalSource,
    request: TraversalRequest,
) -> io::Result<(Vec<TraversalItem>, TraversalOutcome)> {
    let mut session = source.traverse(request);
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let outcome = session.finish().await.map_err(io::Error::other)?;
    Ok((items, outcome))
}

fn totals(outcome: &TraversalOutcome) -> io::Result<ScanTotals> {
    match outcome {
        TraversalOutcome::Completed(completion) => Ok(completion.totals),
        TraversalOutcome::Cancelled => Err(io::Error::other("unexpected cancellation")),
    }
}

/// Entries and failures as sorted lines, without positions or directory events.
fn lines(items: &[TraversalItem]) -> Vec<String> {
    let mut lines: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(format!(
                "entry {} {:?} {:?}",
                entry.path(),
                entry.kind(),
                entry.size()
            )),
            TraversalItem::Failure(failure) => Some(format!(
                "failure {:?} {} {:?} {:?}",
                failure.scope,
                failure.path,
                failure.failure.class(),
                failure.delivered
            )),
            _ => None,
        })
        .collect();
    lines.sort();
    lines
}

/// No directory events, and every directory's entry ahead of anything below it.
fn assert_unordered(items: &[TraversalItem]) {
    let mut directories = HashMap::new();
    for (index, item) in items.iter().enumerate() {
        match item {
            TraversalItem::DirectoryListed(_) => panic!("Unordered delivered a directory event"),
            TraversalItem::Entry(entry) => {
                let path = entry.path().as_str();
                let mut ancestor = path.rsplit_once('/').map(|(parent, _)| parent);
                while let Some(directory) = ancestor {
                    let seen = directories.get(directory);
                    assert!(
                        seen.is_some_and(|at| *at < index),
                        "{path} delivered before its directory {directory}"
                    );
                    ancestor = directory.rsplit_once('/').map(|(parent, _)| parent);
                }
                if entry.kind() == EntryKind::Directory {
                    directories.insert(path.to_owned(), index);
                }
            }
            _ => {}
        }
    }
}

/// A tree with depth, width across a page boundary, empty directories and a symlink.
fn tree(root: &Path) -> io::Result<usize> {
    let mut entries = 0;
    for top in 0..6 {
        let top_dir = root.join(format!("t{top}"));
        std::fs::create_dir(&top_dir)?;
        entries += 1;
        for sub in 0..4 {
            let sub_dir = top_dir.join(format!("s{sub}"));
            std::fs::create_dir(&sub_dir)?;
            entries += 1;
            for file in 0..5 {
                std::fs::write(sub_dir.join(format!("f{file}")), b"12345")?;
                entries += 1;
            }
        }
        std::fs::create_dir(top_dir.join("empty"))?;
        entries += 1;
    }
    let mut chain = root.join("chain");
    for _ in 0..30 {
        std::fs::create_dir(&chain)?;
        entries += 1;
        chain = chain.join("c");
    }
    std::fs::create_dir(root.join("wide"))?;
    entries += 1;
    for file in 0..DEFAULT_PAGE_HINT.get() + 100 {
        std::fs::write(root.join(format!("wide/w{file:05}")), b"")?;
        entries += 1;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("t0", root.join("link"))?;
        entries += 1;
    }
    Ok(entries)
}

/// The same entries and totals as Blocks at every concurrency, in an order that keeps each
/// directory ahead of everything below it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_tree_is_traversed_exactly_as_blocks_does() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let entries = tree(root.path())?;
    let (source, _) = source(root.path())?;
    let (blocks, blocks_outcome) = run(&source, request(TraversalOrder::Blocks, 8, 64)?).await?;
    let expected = totals(&blocks_outcome)?;
    assert_eq!(expected.emitted, entries as u64);
    for (inflight, buffered) in [(1, 1), (4, 2), (64, 1024)] {
        let (items, outcome) = run(
            &source,
            request(TraversalOrder::Unordered, inflight, buffered)?,
        )
        .await?;
        assert_unordered(&items);
        assert_eq!(lines(&items), lines(&blocks), "c={inflight}");
        let counted = totals(&outcome)?;
        assert_eq!(counted, expected, "c={inflight}");
        assert!(counted.is_balanced());
    }
    // The default order is Unordered.
    let (items, _) = run(&source, TraversalRequest::new(StoragePath::root())).await?;
    assert_unordered(&items);
    assert_eq!(lines(&items), lines(&blocks));
    Ok(())
}

/// A directory that cannot be listed is one listing failure after nothing delivered; its siblings
/// and their subtrees are traversed in full.
#[tokio::test]
async fn an_unreadable_directory_fails_alone() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    for name in ["a", "b"] {
        std::fs::create_dir_all(root.path().join(name).join("inner"))?;
        std::fs::write(root.path().join(name).join("inner/file"), b"x")?;
    }
    let (source, namespace) = source(root.path())?;
    namespace.fail_list_path(StoragePath::new("a").map_err(io::Error::other)?);
    let (items, outcome) = run(&source, request(TraversalOrder::Unordered, 4, 4)?).await?;
    assert_unordered(&items);
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some((
                failure.scope,
                failure.path.as_str().to_owned(),
                failure.failure.class(),
                failure.delivered,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        [(
            FailureScope::Listing,
            "a".to_owned(),
            FailureClass::PermissionDenied,
            Some(0)
        )]
    );
    let counted = totals(&outcome)?;
    assert_eq!(
        (
            counted.listing_failures,
            counted.interrupted_listings,
            counted.emitted
        ),
        (1, 0, 4)
    );
    assert!(counted.is_balanced());
    Ok(())
}

/// Cancelled part way through a real tree, with the consumer stalled: `Cancelled`, promptly.
#[tokio::test]
async fn cancellation_part_way_is_cancelled() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    tree(root.path())?;
    let (source, _) = source(root.path())?;
    let cancel = CancellationToken::new();
    let mut session =
        source.traverse(request(TraversalOrder::Unordered, 8, 1)?.cancel(cancel.clone()));
    for _ in 0..20 {
        let _ = session.next_item().await;
    }
    cancel.cancel();
    while session.next_item().await.is_some() {}
    let ended = tokio::time::timeout(std::time::Duration::from_secs(2), session.finish())
        .await
        .map_err(|_| io::Error::other("the cancelled traversal did not end"))?;
    assert_eq!(
        ended.map_err(io::Error::other)?,
        TraversalOutcome::Cancelled
    );
    Ok(())
}
