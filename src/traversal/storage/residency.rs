//! What the traversal is holding on to, sampled by the driver so tests can assert its shape.
//!
//! The cursor's memory is not visible in the item stream at all: a directory's pending descend
//! slots never appear as items, so no assertion over the public contract can tell a bounded
//! traversal from one that keeps every subdirectory of a ten-million-entry tree. These counters
//! are the only instrument that can, which makes them the acceptance criterion for any change
//! that claims to bound what is resident.
//!
//! Counts, not bytes, are the primary measure. Bytes drift with the allocator and the machine;
//! counts are exact and reproducible, and the shape of a curve — does this grow with the number
//! of entries, or with the page size? — is what actually distinguishes a fix from a wash.
//! `path_bytes` is the one exception, and it is there because prefix duplication is a byte
//! problem that no count can express.

use std::sync::atomic::{AtomicUsize, Ordering};

/// One sample of what the cursor holds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Residency {
    /// Listed children waiting to be admitted, across every frame and every arrived listing the
    /// cursor has not taken yet. Grows with the number of entries; this is what paging bounds.
    ///
    /// Only the top frame ever has any: a block is admitted in full before the cursor descends,
    /// so every ancestor's queue is already empty. The stack does **not** hold one pending block
    /// per level.
    pub(super) block_children: usize,
    /// Room those queues still occupy. An emptied `VecDeque` does not shrink, so an ancestor
    /// frame keeps its widest block's allocation until it pops — invisible in `block_children`,
    /// which is exactly why it is counted apart.
    pub(super) block_capacity: usize,
    /// Subdirectories recorded to descend into later. Grows with the number of subdirectories
    /// and cannot be paged away: returning to ten million subdirectories means remembering ten
    /// million places to return to.
    pub(super) descend_slots: usize,
    /// Deferred filter decisions waiting for their observation to settle. Shares an exit with
    /// `descend_slots` — both are consumed only once a block closes — so under a filter that
    /// needs `modified` it grows just as far.
    pub(super) deferred_decisions: usize,
    /// Listings started and not yet arrived.
    pub(super) listings_inflight: usize,
    /// Listings arrived and not yet taken by the cursor.
    pub(super) listings_arrived: usize,
    /// Bytes of path text held by everything counted above.
    pub(super) path_bytes: usize,
}

/// The high-water mark of every counter, shared with whoever wants to read it afterwards.
///
/// Per-field maxima, not a snapshot of one moment: the peaks need not coincide, and a change
/// that trades one for another should be visible as exactly that.
#[derive(Debug, Default)]
pub(super) struct ResidencyPeak {
    block_children: AtomicUsize,
    block_capacity: AtomicUsize,
    descend_slots: AtomicUsize,
    deferred_decisions: AtomicUsize,
    listings_inflight: AtomicUsize,
    listings_arrived: AtomicUsize,
    path_bytes: AtomicUsize,
}

impl ResidencyPeak {
    pub(super) fn record(&self, sample: Residency) {
        for (slot, value) in [
            (&self.block_children, sample.block_children),
            (&self.block_capacity, sample.block_capacity),
            (&self.descend_slots, sample.descend_slots),
            (&self.deferred_decisions, sample.deferred_decisions),
            (&self.listings_inflight, sample.listings_inflight),
            (&self.listings_arrived, sample.listings_arrived),
            (&self.path_bytes, sample.path_bytes),
        ] {
            slot.fetch_max(value, Ordering::Relaxed);
        }
    }

    pub(super) fn peak(&self) -> Residency {
        Residency {
            block_children: self.block_children.load(Ordering::Relaxed),
            block_capacity: self.block_capacity.load(Ordering::Relaxed),
            descend_slots: self.descend_slots.load(Ordering::Relaxed),
            deferred_decisions: self.deferred_decisions.load(Ordering::Relaxed),
            listings_inflight: self.listings_inflight.load(Ordering::Relaxed),
            listings_arrived: self.listings_arrived.load(Ordering::Relaxed),
            path_bytes: self.path_bytes.load(Ordering::Relaxed),
        }
    }
}
