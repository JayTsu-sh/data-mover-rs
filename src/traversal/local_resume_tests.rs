//! Resumable traversal (P7 C11) over the Local backend on a real filesystem, in both orders:
//! resuming from any checkpoint of a run, or through a chain of cancels, loses no entry and no
//! directory (duplicates are counted, and allowed, §9.2); the tree changing between runs is
//! handled like any change during a scan; another root is refused; and how large a checkpoint
//! gets for a deep chain and for a wide directory.

use std::collections::BTreeSet;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{
    Checkpoint, Resumable, ResumeRefusal, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource, TraversalTerminalFailure,
};
use crate::model::{EntryKind, StoragePath};
use crate::storage::backends::local::test_traversal_storage;
use crate::storage::listing::DEFAULT_PAGE_HINT;

type Outcome = Result<TraversalOutcome, TraversalTerminalFailure>;

fn source(root: &Path) -> io::Result<StorageTraversalSource> {
    let (storage, _, _) = test_traversal_storage(root, "local-resume-test")
        .map_err(|error| io::Error::other(error.to_string()))?;
    StorageTraversalSource::new(&storage).map_err(io::Error::other)
}

fn request(order: TraversalOrder, inflight: usize, buffered: usize) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(order)
        .max_inflight_operations(NonZeroUsize::new(inflight).unwrap_or(NonZeroUsize::MIN))
        .max_buffered_items(NonZeroUsize::new(buffered).unwrap_or(NonZeroUsize::MIN))
}

/// Runs to the end; with `cancel_after`, cancels after that many entries and drains.
async fn run(
    source: &StorageTraversalSource,
    request: TraversalRequest,
    cancel_after: Option<usize>,
) -> io::Result<(Vec<TraversalItem>, Outcome)> {
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let ran = tokio::time::timeout(Duration::from_mins(1), async {
        let mut items = Vec::new();
        let mut entries = 0;
        while let Some(item) = session.next_item().await {
            entries += usize::from(matches!(item, TraversalItem::Entry(_)));
            items.push(item);
            if cancel_after.is_some_and(|after| entries == after) {
                cancel.cancel();
            }
        }
        (items, session.finish().await)
    })
    .await;
    ran.map_err(|_| io::Error::other("the traversal did not end"))
}

fn entries(items: &[TraversalItem]) -> Vec<(String, String)> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some((
                entry.path().as_str().to_owned(),
                format!("{:?}", entry.kind()),
            )),
            _ => None,
        })
        .collect()
}

fn checkpoints(items: &[TraversalItem]) -> Vec<(usize, Checkpoint)> {
    items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            TraversalItem::Checkpoint(checkpoint) => Some((index, (**checkpoint).clone())),
            _ => None,
        })
        .collect()
}

/// A tree with depth (a 30-level chain), a directory of `wide` files (wider than a page by
/// default), empty directories, a symlink and ordinary nesting.
fn tree(root: &Path, wide: usize) -> io::Result<()> {
    for top in 0..5 {
        for sub in 0..4 {
            let dir = root.join(format!("t{top}/s{sub}"));
            std::fs::create_dir_all(&dir)?;
            for file in 0..5 {
                std::fs::write(dir.join(format!("f{file}")), b"12345")?;
            }
        }
        std::fs::create_dir(root.join(format!("t{top}/empty")))?;
    }
    let mut chain = root.join("chain");
    for _ in 0..30 {
        chain = chain.join("c");
    }
    std::fs::create_dir_all(chain)?;
    std::fs::create_dir(root.join("wide"))?;
    for file in 0..wide {
        std::fs::write(root.join(format!("wide/w{file:05}")), b"")?;
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink("t0", root.join("link"))?;
    Ok(())
}

fn set(entries: &[(String, String)]) -> BTreeSet<(String, String)> {
    entries.iter().cloned().collect()
}

/// Nothing lost, nothing invented; returns the entries delivered twice.
fn no_loss(delivered: &[(String, String)], full: &BTreeSet<(String, String)>, case: &str) -> usize {
    let union = set(delivered);
    let missing: Vec<_> = full.difference(&union).take(5).collect();
    let extra: Vec<_> = union.difference(full).take(5).collect();
    assert!(missing.is_empty(), "{case}: lost {missing:?}");
    assert!(extra.is_empty(), "{case}: invented {extra:?}");
    delivered.len() - full.len()
}

/// Every checkpoint a full run emits, taken as the last one a consumer committed before
/// crashing: what it had received before it and a traversal resumed from it cover the tree —
/// every directory included, the chain's deepest and the wide one's every page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resuming_a_real_tree_from_any_checkpoint_loses_nothing() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    tree(root.path(), DEFAULT_PAGE_HINT.get() + 50)?;
    let source = source(root.path())?;
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        for (inflight, buffered) in [(1, 1), (8, 16)] {
            let case = format!("{order:?} c={inflight} b={buffered}");
            let make = || request(order, inflight, buffered);
            let (items, _) = run(&source, make(), None).await?;
            let full = set(&entries(&items));
            let every = Resumable::On {
                every: Duration::ZERO,
            };
            let (items, _) = run(&source, make().resumable(every), None).await?;
            assert_eq!(set(&entries(&items)), full, "{case}: with checkpoints on");
            let saved = checkpoints(&items);
            // Every checkpoint of a run takes minutes at c=1 (one per flush); a spread of them
            // exercises the same positions: in the chain, in the wide directory, between blocks.
            let step = (saved.len() / 60).max(1);
            let (mut most, mut total, mut resumed_from) = (0, 0, 0);
            for (at, checkpoint) in saved.iter().step_by(step) {
                resumed_from += 1;
                let (resumed, outcome) =
                    run(&source, make().resume_from(checkpoint.clone()), None).await?;
                assert!(
                    matches!(outcome, Ok(TraversalOutcome::Completed(_))),
                    "{case}"
                );
                let mut delivered = entries(&items[..*at]);
                delivered.extend(entries(&resumed));
                let duplicates = no_loss(&delivered, &full, &format!("{case} @{at}"));
                most = most.max(duplicates);
                total += duplicates;
            }
            eprintln!(
                "{case}: {} entries, {} checkpoints, duplicates per resume max {most} mean {}",
                full.len(),
                saved.len(),
                total / resumed_from.max(1)
            );
            assert!(saved.len() > 10, "{case}");
        }
    }
    Ok(())
}

/// Cancelled again and again, each time resumed from the last checkpoint, until one run
/// completes: together they delivered every entry. Each round delivers more than any one
/// directory holds: a resumed directory is listed again from its start, so a directory larger
/// than what a round delivers would never be finished (resuming is by directory, §9.2, §16).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_of_cancels_and_resumes_covers_a_real_tree() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    tree(root.path(), 30)?;
    let source = source(root.path())?;
    let (items, _) = run(&source, request(TraversalOrder::Blocks, 4, 4), None).await?;
    let full = set(&entries(&items));
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let make = || request(order, 4, 1).resumable(Resumable::on());
        let (mut delivered, mut rounds) = (Vec::new(), 0);
        let mut from: Option<Checkpoint> = None;
        loop {
            rounds += 1;
            assert!(rounds < 500, "{order:?}: no progress");
            let request = from
                .take()
                .map_or_else(make, |checkpoint| make().resume_from(checkpoint));
            let (items, outcome) = run(&source, request, Some(40)).await?;
            delivered.extend(entries(&items));
            if matches!(outcome, Ok(TraversalOutcome::Completed(_))) {
                break;
            }
            match items.last() {
                Some(TraversalItem::Checkpoint(last)) => from = Some((**last).clone()),
                other => panic!("{order:?}: ended with {other:?}"),
            }
        }
        let duplicates = no_loss(&delivered, &full, &format!("{order:?}"));
        eprintln!(
            "{order:?}: {rounds} rounds, {} entries, {duplicates} delivered twice",
            full.len()
        );
        assert!(rounds > 3);
    }
    Ok(())
}

/// The directories some entry of `seen` is in.
fn parents(seen: &BTreeSet<(String, String)>) -> BTreeSet<String> {
    seen.iter()
        .map(|(path, _)| {
            path.rsplit_once('/')
                .map_or("", |(parent, _)| parent)
                .to_owned()
        })
        .collect()
}

/// A tree that changed between runs is scanned as a tree changing during one scan: the deep chain
/// deleted since (in Blocks, the very position resumed from) has nothing below it and no failure;
/// every directory the first run had not reached is listed in full, a file added there included.
#[tokio::test]
async fn a_tree_changed_between_runs_resumes_like_a_scan_during_change() -> io::Result<()> {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let root = tempfile::tempdir()?;
        tree(root.path(), DEFAULT_PAGE_HINT.get() + 50)?;
        let source = source(root.path())?;
        let make = || request(order, 1, 1).resumable(Resumable::on());
        let (items, _) = run(&source, make(), Some(25)).await?;
        let Some(TraversalItem::Checkpoint(last)) = items.last() else {
            panic!("{order:?}: no final checkpoint")
        };
        let seen = set(&entries(&items));
        let touched = parents(&seen);
        std::fs::remove_dir_all(root.path().join("chain"))?;
        let added = (0..5)
            .flat_map(|top| (0..4).map(move |sub| format!("t{top}/s{sub}")))
            .find(|dir| !touched.contains(dir));
        if let Some(dir) = &added {
            std::fs::write(root.path().join(dir).join("added"), b"x")?;
        }
        let (resumed, outcome) = run(&source, make().resume_from((**last).clone()), None).await?;
        assert!(
            matches!(outcome, Ok(TraversalOutcome::Completed(_))),
            "{order:?}"
        );
        let failures: Vec<_> = resumed
            .iter()
            .filter(|item| matches!(item, TraversalItem::Failure(_)))
            .collect();
        assert!(failures.is_empty(), "{order:?}: {failures:?}");
        let delivered = set(&entries(&resumed));
        assert!(
            delivered
                .iter()
                .all(|(path, _)| !path.starts_with("chain/"))
        );
        let (now, _) = run(&source, request(order, 4, 4), None).await?;
        let unreached: Vec<_> = set(&entries(&now))
            .into_iter()
            .filter(|(path, _)| {
                !touched.contains(path.rsplit_once('/').map_or("", |(parent, _)| parent))
            })
            .filter(|entry| !delivered.contains(entry))
            .collect();
        assert!(
            unreached.is_empty(),
            "{order:?}: not listed after resuming: {unreached:?}"
        );
        if let Some(dir) = added {
            let file = (format!("{dir}/added"), format!("{:?}", EntryKind::File));
            assert!(delivered.contains(&file), "{order:?}: {dir}/added");
        }
    }
    Ok(())
}

/// A checkpoint of the whole tree does not resume a traversal of one of its subtrees.
#[tokio::test]
async fn a_checkpoint_of_another_root_is_refused() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    tree(root.path(), DEFAULT_PAGE_HINT.get() + 50)?;
    let source = source(root.path())?;
    let (items, _) = run(
        &source,
        request(TraversalOrder::Unordered, 2, 1).resumable(Resumable::on()),
        Some(10),
    )
    .await?;
    let Some(TraversalItem::Checkpoint(last)) = items.last() else {
        panic!("no final checkpoint")
    };
    let subtree = TraversalRequest {
        root: StoragePath::new("t0").map_err(io::Error::other)?,
        ..request(TraversalOrder::Unordered, 2, 1)
    }
    .resume_from((**last).clone())
    .cancel(CancellationToken::new());
    let (items, outcome) = run(&source, subtree, None).await?;
    assert!(items.is_empty());
    assert_eq!(
        outcome,
        Err(TraversalTerminalFailure::Resume(ResumeRefusal::Mismatch))
    );
    Ok(())
}

/// How large checkpoints get with the shape of the tree. Blocks records one entry per level of
/// its path, so a 200-level chain costs a few kilobytes and a directory of 3,000 subdirectories
/// nothing extra; Unordered records every directory still waiting, so the wide directory's
/// subdirectories are all in its checkpoints until they are listed.
#[tokio::test]
async fn checkpoint_size_follows_the_shape_of_a_real_tree() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut chain = root.path().join("chain");
    for _ in 0..200 {
        chain = chain.join("level");
    }
    std::fs::create_dir_all(chain)?;
    for index in 0..3000 {
        std::fs::create_dir_all(root.path().join(format!("wide/sub{index:04}")))?;
    }
    let source = source(root.path())?;
    let every = Resumable::On {
        every: Duration::ZERO,
    };
    let mut largest = Vec::new();
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let (items, _) = run(&source, request(order, 1, 64).resumable(every), None).await?;
        let sizes: Vec<usize> = checkpoints(&items)
            .iter()
            .map(|(_, checkpoint)| checkpoint.as_bytes().len())
            .collect();
        let max = sizes.iter().copied().max().unwrap_or(0);
        eprintln!(
            "{order:?}: {} checkpoints, largest {max} bytes",
            sizes.len()
        );
        largest.push(max);
    }
    // Blocks: at most 201 levels of 13 bytes and a 6-byte name each, on 58 fixed bytes.
    assert!(
        largest[0] <= 58 + 13 + 201 * (13 + 6),
        "Blocks {}",
        largest[0]
    );
    // Unordered: up to every waiting subdirectory of `wide`, each 13 bytes and its differing tail.
    assert!(
        largest[1] <= 58 + 3000 * (13 + 13) + 201 * (13 + 6),
        "Unordered {}",
        largest[1]
    );
    assert!(
        largest[1] > 3000 * 13 / 2,
        "Unordered held the wide directory's subdirectories"
    );
    Ok(())
}
