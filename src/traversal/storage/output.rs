//! The reorder buffer: releases settled slots strictly in sequence, and tallies what it emits.
//!
//! The cursor assigns every slot its sequence in output order; observations settle in any order.
//! Holding settled slots here until their turn comes is what turns concurrent completion back
//! into deterministic output. Because a slot is only ever emitted once every earlier slot has
//! been, whatever is counted here is counted exactly, in order.
//!
//! That is also why each directory's [`DirectoryListed`] is built here and not at the cursor.
//! When a block's end marker comes up, every slot in that block has already gone past, deferred
//! decisions included, so its counts are exact. The cursor could only have estimated them.
//!
//! Most slots settle in order — every child its listing's task already observed settles the
//! moment it is admitted — so a slot that is next in line goes straight onto a ready queue, and
//! only one that settles ahead of an earlier one waits in the ordered map.

use std::collections::{BTreeMap, VecDeque};
use std::mem;

use super::blocks::Runtime;
use crate::model::StoragePath;
use crate::traversal::{
    BlockCounts, DirectoryListed, ListingOutcome, ScanTotals, TraversalItem,
    TraversalTerminalFailure,
};

/// Where one child's own subtree went.
///
/// A decision made when the child's page arrived is tallied by the listing's task and carried on
/// the block's facts, because a child the filter hides never gets an output slot at all and would
/// otherwise be invisible here. A deferred decision is not known until the child's observation
/// settles, so its slot carries the outcome and the tally happens as the slot is emitted instead.
#[derive(Clone, Copy)]
pub(super) enum ChildDescent {
    Listed,
    Pruned,
    Truncated,
}

/// What one directory's block knew as it was admitted, for the parts no output slot records.
#[derive(Clone, Copy, Default)]
pub(super) struct BlockFacts {
    /// Children the listing returned, described or not.
    pub(super) listed: u64,
    /// Entries the listing hid (see `BlockCounts::internal_hidden`).
    pub(super) internal_hidden: u64,
    /// Children a decision made in the listing's task kept out of the output. These have no
    /// slot at all.
    pub(super) filtered: u64,
    /// Subdirectories a decision made in the listing's task did not descend into.
    pub(super) pruned: u64,
    /// Subdirectories `max_depth` stopped at, by a decision made in the listing's task.
    pub(super) truncated: u64,
    /// Children the listing returned that were gone when it described them.
    pub(super) vanished: u64,
    /// The directory itself was gone when its listing began: its block is empty and complete.
    pub(super) vanished_listing: bool,
}

impl BlockFacts {
    /// Adds one more page's facts to the block's.
    pub(super) const fn absorb(&mut self, page: &Self) {
        self.listed = self.listed.saturating_add(page.listed);
        self.internal_hidden = self.internal_hidden.saturating_add(page.internal_hidden);
        self.filtered = self.filtered.saturating_add(page.filtered);
        self.pruned = self.pruned.saturating_add(page.pruned);
        self.truncated = self.truncated.saturating_add(page.truncated);
        self.vanished = self.vanished.saturating_add(page.vanished);
    }

    /// Adds what these facts counted to `block`, whose own tallies are the children's slots.
    pub(super) const fn count_into(&self, block: &mut BlockCounts) {
        block.listed = block.listed.saturating_add(self.listed);
        block.internal_hidden = block.internal_hidden.saturating_add(self.internal_hidden);
        block.filtered = block.filtered.saturating_add(self.filtered);
        block.pruned_dirs = block.pruned_dirs.saturating_add(self.pruned);
        block.truncated_dirs = block.truncated_dirs.saturating_add(self.truncated);
        block.vanished = block.vanished.saturating_add(self.vanished);
    }
}

/// What one child's output slot holds.
pub(super) enum ChildItem {
    /// An entry or a failure to deliver.
    Item(TraversalItem),
    /// A deferred filter decision kept the child out: the slot keeps its sequence so the slots
    /// behind it stay in order, and emits nothing.
    Filtered,
    /// The child was gone when it was observed: nothing is emitted, and it is counted as
    /// vanished, not failed.
    Vanished,
}

/// What settled into one output slot.
pub(super) enum Settled {
    /// One admitted child, or a listing failure.
    Child {
        item: ChildItem,
        /// Carried only for a deferred decision; see [`ChildDescent`].
        descent: Option<ChildDescent>,
    },
    /// Every direct child of `path` has been admitted.
    BlockEnd {
        path: StoragePath,
        facts: BlockFacts,
    },
}

/// The reorder buffer and the tallies taken as it emits.
pub(super) struct Output {
    next: u64,
    /// Settled slots whose turn has come: `next`, `next + 1`, … in order, none missing.
    ready: VecDeque<Settled>,
    /// Settled slots waiting behind one that has not settled yet.
    pending: BTreeMap<u64, Settled>,
    /// The slots seen since the last block end, which are one directory's block.
    block: BlockCounts,
    /// What the listing failure among those slots, if any, makes of the block's outcome. The
    /// outcome is derived from the failure item itself rather than recorded beside it, so a
    /// listing failure can never close a block reported `Complete` (deletion-safety rule 2).
    listing: Option<ListingOutcome>,
    totals: ScanTotals,
}

impl Output {
    pub(super) const fn new() -> Self {
        Self {
            next: 0,
            ready: VecDeque::new(),
            pending: BTreeMap::new(),
            block: BlockCounts::new(),
            listing: None,
            totals: ScanTotals::new(),
        }
    }

    /// The sequence still to be emitted. Everything below it has left the buffer.
    pub(super) const fn next(&self) -> u64 {
        self.next
    }

    /// Every block emitted so far, summed. Exact once the traversal has emitted everything.
    pub(super) const fn totals(&self) -> ScanTotals {
        self.totals
    }

    pub(super) fn settle(&mut self, sequence: u64, settled: Settled) {
        if sequence != self.ready_end() {
            self.pending.insert(sequence, settled);
            return;
        }
        self.ready.push_back(settled);
        // The slot just settled may have been the gap that held later ones back.
        while !self.pending.is_empty() {
            let Some(settled) = self.pending.remove(&self.ready_end()) else {
                break;
            };
            self.ready.push_back(settled);
        }
    }

    /// The sequence the ready queue continues with.
    fn ready_end(&self) -> u64 {
        self.next
            .saturating_add(u64::try_from(self.ready.len()).unwrap_or(u64::MAX))
    }

    /// Emits every settled slot that is next in sequence, as one batch.
    pub(super) async fn flush(
        &mut self,
        runtime: &Runtime<'_>,
    ) -> Result<(), TraversalTerminalFailure> {
        let mut batch = Vec::with_capacity(self.ready.len());
        while let Some(settled) = self.ready.pop_front() {
            let item = match settled {
                Settled::Child { item, descent } => self.settle_child(item, descent),
                Settled::BlockEnd { path, facts } => Some(self.close_block(path, facts)),
            };
            // Counted as passed as it joins the batch, so the ready queue keeps starting at
            // `next` even when cancellation wins the send and ends the flush.
            self.next += 1;
            batch.extend(item);
        }
        if batch.is_empty() {
            return Ok(());
        }
        emit(runtime, batch).await
    }

    /// Tallies one child against the block it belongs to.
    fn settle_child(
        &mut self,
        item: ChildItem,
        descent: Option<ChildDescent>,
    ) -> Option<TraversalItem> {
        let item = tally_child(&mut self.block, item, descent)?;
        // A listing failure is the directory's, not a child's: it decides the block's outcome and
        // its block end counts it.
        if let TraversalItem::Failure(failure) = &item
            && let Some(outcome) = failure.listing_outcome()
        {
            self.listing.get_or_insert(outcome);
        }
        Some(item)
    }

    /// Turns the block just delivered into its [`DirectoryListed`], and adds it to the totals.
    fn close_block(&mut self, path: StoragePath, facts: BlockFacts) -> TraversalItem {
        let mut counts = mem::take(&mut self.block);
        facts.count_into(&mut counts);
        self.totals.absorb(&counts);
        if facts.vanished_listing {
            self.totals.vanished_listings = self.totals.vanished_listings.saturating_add(1);
        }
        let outcome = self.listing.take().unwrap_or(ListingOutcome::Complete);
        count_listing(&mut self.totals, outcome);
        let mut listed = DirectoryListed::new(path, outcome, counts);
        listed.vanished = facts.vanished_listing;
        TraversalItem::DirectoryListed(Box::new(listed))
    }
}

/// Tallies one child into `block` and returns the item to deliver, if any. A listing failure is
/// returned untallied: it is its directory's, not a child's, and the caller counts it as such.
pub(super) fn tally_child(
    block: &mut BlockCounts,
    item: ChildItem,
    descent: Option<ChildDescent>,
) -> Option<TraversalItem> {
    match descent {
        Some(ChildDescent::Pruned) => {
            block.pruned_dirs = block.pruned_dirs.saturating_add(1);
        }
        Some(ChildDescent::Truncated) => {
            block.truncated_dirs = block.truncated_dirs.saturating_add(1);
        }
        Some(ChildDescent::Listed) | None => {}
    }
    let item = match item {
        ChildItem::Item(item) => item,
        ChildItem::Filtered => {
            block.filtered = block.filtered.saturating_add(1);
            return None;
        }
        ChildItem::Vanished => {
            block.vanished = block.vanished.saturating_add(1);
            return None;
        }
    };
    match &item {
        TraversalItem::Entry(entry) => block.count_emitted(entry),
        TraversalItem::Failure(failure) if failure.listing_outcome().is_none() => {
            block.failed_children = block.failed_children.saturating_add(1);
        }
        // Naming both rather than wildcarding makes the next variant added to `TraversalItem` a
        // compile error at this tally.
        TraversalItem::Failure(_)
        | TraversalItem::DirectoryListed(_)
        | TraversalItem::Checkpoint(_) => {}
    }
    Some(item)
}

/// Counts how one directory's listing ended into `totals`, derived from the listing's own
/// outcome so a failed listing can never be counted complete.
pub(super) const fn count_listing(totals: &mut ScanTotals, outcome: ListingOutcome) {
    match outcome {
        ListingOutcome::Complete => {}
        ListingOutcome::Interrupted => {
            totals.listing_failures = totals.listing_failures.saturating_add(1);
            totals.interrupted_listings = totals.interrupted_listings.saturating_add(1);
        }
        ListingOutcome::Unreadable => {
            totals.listing_failures = totals.listing_failures.saturating_add(1);
        }
    }
}

/// Sends one batch. Cancellation winning the race ends the flush; a batch larger than the bound
/// goes over in pieces, so some of it may have been sent by then, which does not matter: the
/// traversal is ending.
async fn emit(
    runtime: &Runtime<'_>,
    batch: Vec<TraversalItem>,
) -> Result<(), TraversalTerminalFailure> {
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => Ok(()),
        result = runtime.items.send(batch) => {
            result.map_err(|_| TraversalTerminalFailure::Internal)
        }
    }
}
