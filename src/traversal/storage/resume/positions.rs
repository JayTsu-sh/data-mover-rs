//! Blocks positions (P7 §9.2): where a resumed Blocks traversal would start, recorded each time
//! the cursor enters a directory, by the output sequence that directory's block begins at.
//!
//! A position is the chain of directories from the traversal root to the one entered. Resuming
//! from it lists that last directory whole (its block, then its subtree) and, for each ancestor,
//! descends into the subdirectories named after the next link of the chain, in the byte order
//! Blocks descends in: exactly what the cursor still had to do when it entered the directory. So a
//! record `(s, P)` stands for everything from sequence `s` on, and a checkpoint may use it once
//! every item before `s` has been delivered — the latest such record is the checkpoint.
//!
//! Each record costs one allocation, the level it adds to its parent's chain; a chain is only
//! spelled out when a checkpoint is taken. Records older than the latest delivered one are
//! dropped as the output advances, so what is held is bounded by the admission window.

use std::collections::VecDeque;
use std::sync::Arc;

use super::entry_of;
use crate::traversal::checkpoint::FrontierEntry;
use crate::traversal::storage::DirectoryWork;

/// One directory on the cursor's path, linked to the level it was entered from.
pub(crate) struct Level {
    parent: Option<Arc<Level>>,
    entry: FrontierEntry,
}

impl Level {
    /// The level of a directory entered from `parent` (the traversal root has none).
    pub(crate) fn enter(parent: Option<&Arc<Self>>, work: &DirectoryWork) -> Arc<Self> {
        Arc::new(Self {
            parent: parent.cloned(),
            entry: entry_of(work),
        })
    }

    /// The chain from the traversal root to this level.
    pub(crate) fn chain(&self) -> Vec<FrontierEntry> {
        let mut chain = vec![self.entry.clone()];
        let mut level = self.parent.as_deref();
        while let Some(current) = level {
            chain.push(current.entry.clone());
            level = current.parent.as_deref();
        }
        chain.reverse();
        chain
    }
}

/// Positions recorded by the sequence their block begins at, oldest first. `None` is the end of
/// the traversal: nothing is left to do from there.
#[derive(Default)]
pub(crate) struct Positions {
    records: VecDeque<(u64, Option<Arc<Level>>)>,
}

impl Positions {
    /// Records that from `sequence` on, what is left is `level` (and its ancestors' remainders).
    pub(crate) fn record(&mut self, sequence: u64, level: Option<Arc<Level>>) {
        if self
            .records
            .back()
            .is_some_and(|(recorded, _)| *recorded == sequence)
        {
            self.records.pop_back();
        }
        self.records.push_back((sequence, level));
    }

    /// Drops every record older than the latest one at or before `delivered`: no checkpoint will
    /// ever use them.
    pub(crate) fn prune(&mut self, delivered: u64) {
        while self
            .records
            .get(1)
            .is_some_and(|(sequence, _)| *sequence <= delivered)
        {
            self.records.pop_front();
        }
    }

    /// The position of the latest record at or before `delivered`, as checkpoint entries (none:
    /// the traversal had ended). `None` when no record is that old.
    pub(crate) fn latest(&self, delivered: u64) -> Option<Vec<FrontierEntry>> {
        self.records
            .iter()
            .rev()
            .find(|(sequence, _)| *sequence <= delivered)
            .map(|(_, level)| level.as_deref().map_or_else(Vec::new, Level::chain))
    }

    /// Records held, for the tests that bound them.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}
