//! What a traversal counted (P7 §7): per directory in [`BlockCounts`], for the whole traversal in
//! [`ScanTotals`].
//!
//! Every child a listing returned is accounted for exactly once:
//!
//! ```text
//! listed == emitted + filtered + failed_children + vanished
//! ```
//!
//! Hidden entries (`internal_hidden`: transfer artifacts, ONTAP's `NFSv3` snapshot directory) are
//! not children and are not in `listed`. Pruned and truncated subdirectories are orthogonal: a
//! subdirectory can be emitted and not descended into. A listing that failed is not a child
//! either; `listed` counts only what it returned before it failed.

use crate::model::{EntryKind, ObservedEntry};

/// What one listed directory's block held. Carried by [`super::DirectoryListed`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct BlockCounts {
    /// Children the listing returned, described or not.
    pub listed: u64,
    /// Children delivered as [`super::TraversalItem::Entry`].
    pub emitted: u64,
    /// Children the filter kept out of the output, whether it decided from the listing or from
    /// the child's observation.
    pub filtered: u64,
    /// Children delivered as a [`super::FailureScope::Child`] failure instead of an entry.
    pub failed_children: u64,
    /// Children that were gone by the time they were described or observed: the listing
    /// returned them, and describing or observing them found nothing (`NotFound`). Not failures
    /// and no item is emitted for them (P7 §8, Q12): the entry no longer exists, which is what a
    /// listing taken a moment later would say too.
    pub vanished: u64,
    /// Entries the listing hid. Not children: transfer artifacts (`.data-mover-*`), and ONTAP's
    /// snapshot directory where `NFSv3` lists it without attributes (P7 C7b; `NFSv4` never lists
    /// it).
    pub internal_hidden: u64,
    /// Subdirectories the filter did not descend into.
    pub pruned_dirs: u64,
    /// Subdirectories `max_depth` stopped at, whatever the filter said about descending.
    pub truncated_dirs: u64,
    /// Sum of the sizes of the emitted files. Other kinds' sizes are not content.
    pub bytes: u64,
    /// Emitted entries by kind; together they are `emitted`.
    pub files: u64,
    pub directories: u64,
    pub symlinks: u64,
    pub other: u64,
}

impl BlockCounts {
    /// Nothing counted yet.
    pub(crate) const fn new() -> Self {
        Self {
            listed: 0,
            emitted: 0,
            filtered: 0,
            failed_children: 0,
            vanished: 0,
            internal_hidden: 0,
            pruned_dirs: 0,
            truncated_dirs: 0,
            bytes: 0,
            files: 0,
            directories: 0,
            symlinks: 0,
            other: 0,
        }
    }

    /// Whether every listed child is accounted for: `listed == emitted + filtered +
    /// failed_children + vanished`, and the kinds add up to `emitted`.
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        balanced(
            self.listed,
            [
                self.emitted,
                self.filtered,
                self.failed_children,
                self.vanished,
            ],
        ) && balanced(
            self.emitted,
            [self.files, self.directories, self.symlinks, self.other],
        )
    }

    /// Counts one emitted entry.
    pub(crate) fn count_emitted(&mut self, entry: &ObservedEntry) {
        self.emitted = self.emitted.saturating_add(1);
        let tally = match entry.kind() {
            EntryKind::File => {
                self.bytes = self.bytes.saturating_add(entry.size().unwrap_or(0));
                &mut self.files
            }
            EntryKind::Directory => &mut self.directories,
            EntryKind::Symlink => &mut self.symlinks,
            EntryKind::Special(_) => &mut self.other,
        };
        *tally = tally.saturating_add(1);
    }
}

/// What a whole traversal counted. Carried by [`super::TraversalCompletion`].
///
/// The child counts are the sums of every [`BlockCounts`]; `listing_failures`,
/// `interrupted_listings` and `vanished_listings` count directories.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ScanTotals {
    /// See [`BlockCounts::listed`].
    pub listed: u64,
    /// See [`BlockCounts::emitted`].
    pub emitted: u64,
    /// See [`BlockCounts::filtered`].
    pub filtered: u64,
    /// See [`BlockCounts::failed_children`].
    pub failed_children: u64,
    /// See [`BlockCounts::vanished`].
    pub vanished: u64,
    /// See [`BlockCounts::internal_hidden`].
    pub internal_hidden: u64,
    /// Directories whose listing did not run to its end: one per
    /// [`super::FailureScope::Listing`] failure.
    pub listing_failures: u64,
    /// Those of `listing_failures` that stopped after delivering some children
    /// ([`super::ListingOutcome::Interrupted`]).
    pub interrupted_listings: u64,
    /// Subdirectories that were gone by the time their own listing began (`NotFound`; never the
    /// traversal root, whose absence is a listing failure). Each was already accounted for as its
    /// parent's child — emitted, or vanished when its own observation found it gone after the
    /// listing had already decided to descend — so it is not counted again in the child equation;
    /// its listing closes [`super::ListingOutcome::Complete`] and empty, with
    /// [`super::DirectoryListed::vanished`] set, and is no listing failure (P7 §8, Q12).
    pub vanished_listings: u64,
    /// See [`BlockCounts::pruned_dirs`].
    pub pruned_dirs: u64,
    /// See [`BlockCounts::truncated_dirs`].
    pub truncated_dirs: u64,
    /// See [`BlockCounts::bytes`].
    pub bytes: u64,
    /// Emitted entries by kind, as in [`BlockCounts`].
    pub files: u64,
    pub directories: u64,
    pub symlinks: u64,
    pub other: u64,
}

impl ScanTotals {
    /// Nothing counted yet.
    pub(crate) const fn new() -> Self {
        Self {
            listed: 0,
            emitted: 0,
            filtered: 0,
            failed_children: 0,
            vanished: 0,
            internal_hidden: 0,
            listing_failures: 0,
            interrupted_listings: 0,
            vanished_listings: 0,
            pruned_dirs: 0,
            truncated_dirs: 0,
            bytes: 0,
            files: 0,
            directories: 0,
            symlinks: 0,
            other: 0,
        }
    }

    /// Whether every listed child is accounted for; see [`BlockCounts::is_balanced`].
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        balanced(
            self.listed,
            [
                self.emitted,
                self.filtered,
                self.failed_children,
                self.vanished,
            ],
        ) && balanced(
            self.emitted,
            [self.files, self.directories, self.symlinks, self.other],
        ) && self.interrupted_listings <= self.listing_failures
    }

    /// Adds one directory's block.
    pub(crate) const fn absorb(&mut self, block: &BlockCounts) {
        self.listed = self.listed.saturating_add(block.listed);
        self.emitted = self.emitted.saturating_add(block.emitted);
        self.filtered = self.filtered.saturating_add(block.filtered);
        self.failed_children = self.failed_children.saturating_add(block.failed_children);
        self.vanished = self.vanished.saturating_add(block.vanished);
        self.internal_hidden = self.internal_hidden.saturating_add(block.internal_hidden);
        self.pruned_dirs = self.pruned_dirs.saturating_add(block.pruned_dirs);
        self.truncated_dirs = self.truncated_dirs.saturating_add(block.truncated_dirs);
        self.bytes = self.bytes.saturating_add(block.bytes);
        self.files = self.files.saturating_add(block.files);
        self.directories = self.directories.saturating_add(block.directories);
        self.symlinks = self.symlinks.saturating_add(block.symlinks);
        self.other = self.other.saturating_add(block.other);
    }
}

/// Whether `parts` add up to `total`, without overflowing.
const fn balanced(total: u64, parts: [u64; 4]) -> bool {
    let mut sum: u64 = 0;
    let mut index = 0;
    while index < parts.len() {
        sum = match sum.checked_add(parts[index]) {
            Some(sum) => sum,
            None => return false,
        };
        index += 1;
    }
    sum == total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balance_holds_only_when_every_listed_child_is_accounted_for() {
        let mut block = BlockCounts {
            listed: 4,
            emitted: 2,
            filtered: 1,
            failed_children: 1,
            files: 1,
            directories: 1,
            ..BlockCounts::default()
        };
        assert!(block.is_balanced());
        // Hidden artifacts and pruned / truncated subdirectories are outside the equation.
        block.internal_hidden = 5;
        block.pruned_dirs = 1;
        block.truncated_dirs = 1;
        assert!(block.is_balanced());
        block.filtered = 0;
        assert!(!block.is_balanced());
        block.filtered = 1;
        block.symlinks = 1;
        assert!(!block.is_balanced(), "kinds must add up to emitted");

        let mut totals = ScanTotals::default();
        totals.absorb(&BlockCounts {
            symlinks: 0,
            ..block
        });
        totals.absorb(&BlockCounts {
            symlinks: 0,
            ..block
        });
        assert!(totals.is_balanced());
        assert_eq!(totals.listed, 8);
        totals.interrupted_listings = 1;
        assert!(
            !totals.is_balanced(),
            "an interrupted listing is a listing failure"
        );
        totals.listing_failures = 1;
        assert!(totals.is_balanced());
    }

    #[test]
    fn balance_does_not_wrap() {
        let block = BlockCounts {
            listed: 0,
            emitted: u64::MAX,
            filtered: 1,
            ..BlockCounts::default()
        };
        assert!(!block.is_balanced());
    }
}
