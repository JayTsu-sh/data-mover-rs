//! The reorder buffer: releases settled slots strictly in sequence, and tallies what it emits.
//!
//! The cursor assigns every slot its sequence in output order; observations settle in any order.
//! Holding settled slots here until their turn comes is what turns concurrent completion back
//! into deterministic output. Because a slot is only ever emitted once every earlier slot has
//! been, whatever is counted here is counted exactly, in order.

use std::collections::BTreeMap;

use super::Runtime;
use crate::traversal::{TraversalItem, TraversalTerminalFailure};

/// What settled into one output slot.
pub(super) enum Settled {
    /// One admitted child. `None` is a slot a deferred filter then dropped: it keeps its
    /// sequence so the slots behind it stay in order, and emits nothing.
    Child(Option<TraversalItem>),
}

/// The reorder buffer and the tallies taken as it emits.
pub(super) struct Output {
    next: u64,
    observed: u64,
    failed: u64,
    /// Settled slots waiting for their turn.
    pending: BTreeMap<u64, Settled>,
}

impl Output {
    pub(super) const fn new() -> Self {
        Self {
            next: 0,
            observed: 0,
            failed: 0,
            pending: BTreeMap::new(),
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

    pub(super) fn settle(&mut self, sequence: u64, settled: Settled) {
        self.pending.insert(sequence, settled);
    }

    /// Emits every settled slot that is next in sequence.
    pub(super) async fn flush(
        &mut self,
        runtime: &Runtime<'_>,
    ) -> Result<(), TraversalTerminalFailure> {
        while let Some(settled) = self.pending.remove(&self.next) {
            match settled {
                Settled::Child(Some(item)) => {
                    if !self.emit(runtime, item).await? {
                        return Ok(());
                    }
                }
                Settled::Child(None) => {}
            }
            self.next += 1;
        }
        Ok(())
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
            // Completion items are neither entries nor failures; they have their own tallies.
            TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_) => {}
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
