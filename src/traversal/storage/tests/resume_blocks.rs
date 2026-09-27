//! Resumable Blocks (P7 C11): a resumed traversal continues the full run's directory order; an
//! ancestor listed again that fails now is its own failure, and one gone since is counted
//! vanished, neither touching the next block's outcome; and a checkpoint is the position before
//! what was delivered, not what the cursor had reached while the output lagged behind it.

use std::collections::BTreeSet;
use std::time::Duration;

use super::resume::{Tree, checkpoints, entry_paths, every_chance, request, run, setup};
use super::*;
use crate::traversal::checkpoint::{FrontierEntry, fingerprint, open};
use crate::traversal::{Checkpoint, FailureScope, ListingOutcome, Resumable, TraversalOrder};

fn listed(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::DirectoryListed(listed) => Some(listed.path.as_str().to_owned()),
            _ => None,
        })
        .collect()
}

/// The position a checkpoint of `request` names.
fn position(request: &TraversalRequest, checkpoint: &Checkpoint) -> Vec<FrontierEntry> {
    let identity = crate::storage::backends::local::test_identity("traversal-test");
    let print = fingerprint(&identity, request).unwrap_or_else(|refusal| panic!("{refusal}"));
    open(checkpoint, &print, request.order, &request.root)
        .unwrap_or_else(|refusal| panic!("{refusal}"))
}

/// Resumed from any checkpoint, the directories come in the full run's order from the one the
/// position names on: its ancestors' remaining subdirectories are exactly those after the next
/// link of the chain, in the same byte order, and none of the ancestors' own blocks come again.
#[tokio::test]
async fn a_resumed_traversal_continues_the_full_runs_directory_order() {
    let setup = setup(Tree::generate(3, 3, 2, 2), false);
    let make = || request(TraversalOrder::Blocks, 4, 2);
    let (items, _) = run(&setup.source, make().resumable(every_chance()), None).await;
    let full = listed(&items);
    let saved = checkpoints(&items);
    assert!(saved.len() > 10);
    for (at, checkpoint) in saved {
        let chain = position(&make(), &checkpoint);
        let (resumed, outcome) = run(&setup.source, make().resume_from(checkpoint), None).await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        let resumed = listed(&resumed);
        let Some(top) = chain.last() else {
            assert!(resumed.is_empty(), "@{at}: the end resumes to nothing");
            continue;
        };
        assert_eq!(
            resumed.first().map(String::as_str),
            Some(top.path.as_str()),
            "@{at}"
        );
        let from = full
            .iter()
            .position(|path| path == top.path.as_str())
            .unwrap_or_else(|| panic!("@{at}: {} never listed", top.path));
        assert_eq!(resumed, full[from..], "@{at}");
    }
}

/// A checkpoint whose position is at least two levels below the root.
async fn deep_checkpoint(setup: &super::resume::Setup) -> (Checkpoint, Vec<FrontierEntry>) {
    let make = || request(TraversalOrder::Blocks, 2, 1);
    let (items, _) = run(&setup.source, make().resumable(every_chance()), None).await;
    checkpoints(&items)
        .into_iter()
        .map(|(_, checkpoint)| {
            let chain = position(&make(), &checkpoint);
            (checkpoint, chain)
        })
        .find(|(_, chain)| chain.len() >= 3)
        .unwrap_or_else(|| panic!("no checkpoint two levels down"))
}

/// An ancestor that cannot be listed again is reported first, as that directory's listing
/// failure (its subtree takes no part in deletion), and counted; the block that follows is the
/// next directory's, and its outcome stays its own.
#[tokio::test]
async fn an_ancestor_that_fails_now_is_its_own_failure() {
    let setup = setup(Tree::generate(3, 3, 2, 2), false);
    let (checkpoint, chain) = deep_checkpoint(&setup).await;
    let ancestor = chain[1].path.as_str().to_owned();
    setup.tree.deny(&ancestor);
    let (items, outcome) = run(
        &setup.source,
        request(TraversalOrder::Blocks, 2, 1).resume_from(checkpoint),
        None,
    )
    .await;
    let Some(TraversalItem::Failure(first)) = items.first() else {
        panic!("the ancestor's failure is not first: {:?}", items.first())
    };
    assert_eq!(
        (first.scope, first.path.as_str()),
        (FailureScope::Listing, ancestor.as_str())
    );
    let Some(TraversalItem::DirectoryListed(next)) = items
        .iter()
        .find(|item| matches!(item, TraversalItem::DirectoryListed(_)))
    else {
        panic!("no block after resuming")
    };
    assert_eq!(next.outcome, ListingOutcome::Complete, "{}", next.path);
    let Ok(TraversalOutcome::Completed(completion)) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(completion.totals.listing_failures, 1);
}

/// An ancestor gone since the checkpoint has nothing left below it: counted as a vanished
/// listing, and no failure.
#[tokio::test]
async fn an_ancestor_gone_since_is_counted_vanished() {
    let setup = setup(Tree::generate(3, 3, 2, 2), false);
    let (checkpoint, chain) = deep_checkpoint(&setup).await;
    setup.tree.remove(chain[1].path.as_str());
    let (items, outcome) = run(
        &setup.source,
        request(TraversalOrder::Blocks, 2, 1).resume_from(checkpoint),
        None,
    )
    .await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Failure(_))),
        "{items:?}"
    );
    let Ok(TraversalOutcome::Completed(completion)) = outcome else {
        panic!("{outcome:?}")
    };
    assert!(completion.totals.vanished_listings >= 1);
}

/// While the root's first child (a symlink whose target is slow to read) holds the output back,
/// the cursor has entered subdirectories further on; every checkpoint taken meanwhile names the
/// root, the position before the first item not delivered, and resuming from one loses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_is_the_position_before_what_was_delivered() {
    let (tree, gate) = Tree::generate(3, 3, 2, 2).with_gated_link();
    let setup = setup(tree, false);
    let full = setup.tree.all_entries();
    let make = || request(TraversalOrder::Blocks, 64, 64);
    let mut session = setup.source.traverse(make().resumable(every_chance()));
    let mut held = Vec::new();
    while let Ok(Some(item)) =
        tokio::time::timeout(Duration::from_millis(300), session.next_item()).await
    {
        held.push(item);
    }
    let saved = checkpoints(&held);
    assert!(
        !saved.is_empty(),
        "no checkpoint while the output was held back"
    );
    assert!(
        entry_paths(&held).is_empty(),
        "delivered past the held-back link"
    );
    for (_, checkpoint) in &saved {
        let chain = position(&make(), checkpoint);
        let paths: Vec<_> = chain.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, [""], "a checkpoint ran ahead of the output");
    }
    gate.add_permits(1024);
    while session.next_item().await.is_some() {}
    let (_, checkpoint) = saved.last().cloned().unwrap_or_else(|| unreachable!());
    let (resumed, _) = run(&setup.source, make().resume_from(checkpoint), None).await;
    let resumed: BTreeSet<String> = entry_paths(&resumed).into_iter().collect();
    assert_eq!(resumed, full);
}

/// Cancelled before a first period has passed, the final checkpoint still follows what was
/// delivered — not the start — and resuming from it loses nothing.
#[tokio::test]
async fn cancelling_before_the_first_period_resumes_from_the_last_position() {
    let setup = setup(Tree::generate(4, 3, 3, 2), false);
    let full = setup.tree.all_entries();
    let make = || request(TraversalOrder::Blocks, 3, 1).resumable(Resumable::on());
    let (items, outcome) = run(&setup.source, make(), Some(40)).await;
    assert_eq!(outcome, Ok(TraversalOutcome::Cancelled));
    let Some(TraversalItem::Checkpoint(last)) = items.last() else {
        panic!("no final checkpoint")
    };
    let chain = position(&make(), last);
    assert!(
        chain.len() > 1,
        "the position did not follow the output: {chain:?}"
    );
    let (resumed, _) = run(&setup.source, make().resume_from((**last).clone()), None).await;
    let union: BTreeSet<String> = entry_paths(&items)
        .into_iter()
        .chain(entry_paths(&resumed))
        .collect();
    assert_eq!(union, full);
}

/// A periodic checkpoint never splits a block: each comes right after a `DirectoryListed` (or
/// before any item); only the final one of a cancelled traversal may follow part of a block.
#[tokio::test]
async fn periodic_checkpoints_come_between_blocks() {
    let setup = setup(Tree::generate(3, 3, 4, 2), false);
    for (inflight, buffered) in [(1, 1), (4, 2), (64, 64)] {
        let request = request(TraversalOrder::Blocks, inflight, buffered).resumable(every_chance());
        let (items, _) = run(&setup.source, request, None).await;
        let saved = checkpoints(&items);
        assert!(saved.len() > 5, "c={inflight}");
        for (at, _) in saved {
            let before = at.checked_sub(1).map(|index| &items[index]);
            assert!(
                matches!(
                    before,
                    None | Some(TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_))
                ),
                "c={inflight}: a checkpoint inside a block, after {before:?}"
            );
        }
    }
}
