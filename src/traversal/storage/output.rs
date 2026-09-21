//! The reorder buffer: releases settled slots strictly in sequence, and tallies what it emits.
//!
//! The cursor assigns every slot its sequence in output order; observations settle in any order.
//! Holding settled slots here until their turn comes is what turns concurrent completion back
//! into deterministic output. Because a slot is only ever emitted once every earlier slot has
//! been, whatever is counted here is counted exactly, in order.
//!
//! That is also why the directory completion items are built here and not at the cursor. When a
//! block's end marker comes up, every slot in that block has already gone past, deferred
//! decisions included; and when a subtree's end marker comes up, so has every block below it.
//! The cursor could only have estimated both.

use std::collections::BTreeMap;

use super::Runtime;
use crate::model::StoragePath;
use crate::traversal::{
    DirectoryListed, DirectoryListing, SubtreeComplete, SubtreeSummary, TraversalItem,
    TraversalTerminalFailure,
};

/// Where one child's own subtree went.
///
/// A decision made when the listing arrived is tallied on the block as the child is admitted,
/// because a child the filter hides never gets an output slot at all and would otherwise be
/// invisible here. A deferred decision is not known until the child's observation settles, so
/// its slot carries the outcome and the tally happens at the block's end marker instead.
#[derive(Clone, Copy)]
pub(super) enum ChildDescent {
    Listed,
    Pruned,
    Truncated,
}

/// What one directory's block knew as it was admitted, for the parts no output slot records.
#[derive(Clone, Copy, Default)]
pub(super) struct BlockFacts {
    /// The directory could not be listed. Its block holds only that failure.
    pub(super) listing_failed: bool,
    /// Children an immediate decision kept out of the output. These have no slot at all.
    pub(super) hidden: u64,
    /// Subdirectories an immediate decision did not descend into.
    pub(super) pruned: u64,
    /// Subdirectories `max_depth` stopped at, by an immediate decision.
    pub(super) truncated: u64,
}

/// What settled into one output slot.
pub(super) enum Settled {
    /// One admitted child. `None` is a slot a deferred filter then dropped: it keeps its
    /// sequence so the slots behind it stay in order, and emits nothing.
    Child {
        item: Option<TraversalItem>,
        /// Carried only for a deferred decision; see [`ChildDescent`].
        descent: Option<ChildDescent>,
    },
    /// Every direct child of `path` has been admitted.
    BlockEnd {
        path: StoragePath,
        facts: BlockFacts,
    },
    /// Every descendant of `path` has been admitted.
    SubtreeEnd { path: StoragePath },
}

/// The slots seen since the last marker, which are one directory's block.
struct BlockCounts {
    observed: u64,
    failures: u64,
    /// Children a deferred decision kept out of the output. Their slots settled as `None`.
    hidden: u64,
    pruned: u64,
    truncated: u64,
}

impl BlockCounts {
    const fn new() -> Self {
        Self {
            observed: 0,
            failures: 0,
            hidden: 0,
            pruned: 0,
            truncated: 0,
        }
    }
}

/// A directory whose block has ended while its subtree is still being delivered.
struct Open {
    path: StoragePath,
    summary: SubtreeSummary,
}

/// The reorder buffer and the tallies taken as it emits.
pub(super) struct Output {
    next: u64,
    observed: u64,
    failed: u64,
    listed: u64,
    /// Settled slots waiting for their turn.
    pending: BTreeMap<u64, Settled>,
    block: BlockCounts,
    /// Directories whose block has ended, innermost last.
    open: Vec<Open>,
}

impl Output {
    pub(super) const fn new() -> Self {
        Self {
            next: 0,
            observed: 0,
            failed: 0,
            listed: 0,
            pending: BTreeMap::new(),
            block: BlockCounts::new(),
            open: Vec::new(),
        }
    }

    /// The sequence still to be emitted. Everything below it has left the buffer.
    pub(super) const fn next(&self) -> u64 {
        self.next
    }

    pub(super) const fn observed(&self) -> u64 {
        self.observed
    }

    pub(super) const fn failed(&self) -> u64 {
        self.failed
    }

    pub(super) const fn listed(&self) -> u64 {
        self.listed
    }

    pub(super) fn settle(&mut self, sequence: u64, settled: Settled) {
        self.pending.insert(sequence, settled);
    }

    /// Emits every settled slot that is next in sequence.
    pub(super) async fn flush(
        &mut self,
        runtime: &Runtime<'_>,
    ) -> Result<(), TraversalTerminalFailure> {
        while let Some(settled) = self.pending.remove(&self.next) {
            let item = match settled {
                Settled::Child { item, descent } => self.settle_child(item, descent),
                Settled::BlockEnd { path, facts } => Some(self.close_block(path, facts)),
                Settled::SubtreeEnd { path } => Some(self.end_subtree(&path)?),
            };
            if let Some(item) = item
                && !self.emit(runtime, item).await?
            {
                return Ok(());
            }
            self.next += 1;
        }
        Ok(())
    }

    /// Tallies one child against the block it belongs to.
    fn settle_child(
        &mut self,
        item: Option<TraversalItem>,
        descent: Option<ChildDescent>,
    ) -> Option<TraversalItem> {
        match &item {
            Some(TraversalItem::Entry(_)) => self.block.observed += 1,
            Some(TraversalItem::EntryFailure(_)) => self.block.failures += 1,
            // A marker never settles into a child slot. Naming both rather than wildcarding
            // makes the next variant added to `TraversalItem` a compile error at this tally.
            Some(TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_)) => {}
            None => self.block.hidden += 1,
        }
        match descent {
            Some(ChildDescent::Pruned) => self.block.pruned += 1,
            Some(ChildDescent::Truncated) => self.block.truncated += 1,
            Some(ChildDescent::Listed) | None => {}
        }
        item
    }

    /// Turns the block just delivered into its verdict, and opens the subtree it heads.
    fn close_block(&mut self, path: StoragePath, facts: BlockFacts) -> TraversalItem {
        let block = std::mem::replace(&mut self.block, BlockCounts::new());
        let listing = if facts.listing_failed {
            DirectoryListing::Failed
        } else if block.failures > 0 {
            DirectoryListing::Partial {
                failures: block.failures,
            }
        } else if facts.hidden > 0 || block.hidden > 0 {
            DirectoryListing::Filtered
        } else {
            DirectoryListing::Complete
        };
        let pruned_children = facts.pruned.saturating_add(block.pruned);
        let truncated_children = facts.truncated.saturating_add(block.truncated);
        self.open.push(Open {
            path: path.clone(),
            summary: SubtreeSummary {
                directories_listed: 1,
                filtered_listings: u64::from(listing == DirectoryListing::Filtered),
                partial_listings: u64::from(matches!(listing, DirectoryListing::Partial { .. })),
                failed_listings: u64::from(listing == DirectoryListing::Failed),
                observed_entries: block.observed,
                entry_failures: block.failures,
                pruned_directories: pruned_children,
                truncated_directories: truncated_children,
            },
        });
        TraversalItem::DirectoryListed(DirectoryListed {
            path,
            listing,
            pruned_children,
            truncated_children,
        })
    }

    /// Closes the innermost open subtree and folds it into the one that contains it.
    fn end_subtree(
        &mut self,
        path: &StoragePath,
    ) -> Result<TraversalItem, TraversalTerminalFailure> {
        // Markers nest strictly, so the innermost open subtree is always the one ending. A
        // mismatch means the cursor and this buffer disagree about the tree.
        let Some(open) = self.open.pop().filter(|open| &open.path == path) else {
            return Err(TraversalTerminalFailure::Internal);
        };
        if let Some(parent) = self.open.last_mut() {
            absorb(&mut parent.summary, &open.summary);
        }
        Ok(TraversalItem::SubtreeComplete(Box::new(SubtreeComplete {
            path: open.path,
            summary: open.summary,
        })))
    }

    /// Sends one item. `false` means cancellation won the race, which ends the flush.
    async fn emit(
        &mut self,
        runtime: &Runtime<'_>,
        item: TraversalItem,
    ) -> Result<bool, TraversalTerminalFailure> {
        match &item {
            TraversalItem::Entry(_) => self.observed += 1,
            TraversalItem::EntryFailure(_) => self.failed += 1,
            TraversalItem::DirectoryListed(_) => self.listed += 1,
            // A subtree marker restates what its blocks already said; it is not its own tally.
            TraversalItem::SubtreeComplete(_) => {}
        }
        tokio::select! {
            biased;
            () = runtime.request.cancel.cancelled() => Ok(false),
            result = runtime.items.send(item) => {
                result.map_err(|_| TraversalTerminalFailure::Internal)?;
                Ok(true)
            }
        }
    }
}

/// Folds a finished subtree into the one that contains it.
fn absorb(total: &mut SubtreeSummary, part: &SubtreeSummary) {
    total.directories_listed = total
        .directories_listed
        .saturating_add(part.directories_listed);
    total.filtered_listings = total
        .filtered_listings
        .saturating_add(part.filtered_listings);
    total.partial_listings = total.partial_listings.saturating_add(part.partial_listings);
    total.failed_listings = total.failed_listings.saturating_add(part.failed_listings);
    total.observed_entries = total.observed_entries.saturating_add(part.observed_entries);
    total.entry_failures = total.entry_failures.saturating_add(part.entry_failures);
    total.pruned_directories = total
        .pruned_directories
        .saturating_add(part.pruned_directories);
    total.truncated_directories = total
        .truncated_directories
        .saturating_add(part.truncated_directories);
}
