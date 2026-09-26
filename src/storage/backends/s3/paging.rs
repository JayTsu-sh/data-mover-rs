//! Bounds on paging through one S3 directory (ADR-0006 C22). A listing collects every page of a
//! directory before its block is built, so a directory is held in memory whole; these bounds keep
//! a huge directory or a broken server from paging (and growing) without end. Hitting one fails
//! that directory's listing — the traversal reports it `Failed` and goes on with its siblings,
//! never a silently truncated block and never the end of the session.

use std::collections::HashSet;
use std::hash::Hash;

use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;
use crate::storage::throttle::ThrottleBackoff;

/// How far one directory's listing may page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ListingLimits {
    /// Objects, versions, delete markers and common prefixes one directory may list.
    pub(super) max_entries: usize,
    /// Pages in a row that list nothing yet claim more follow.
    pub(super) max_empty_pages: usize,
    /// How long one listing backs off while the service throttles it (P7 §8, Q11).
    pub(super) throttle: ThrottleBackoff,
}

impl ListingLimits {
    /// 5 million entries (5 000 full pages): about as much as one directory's descriptors should
    /// hold in memory before listings stream (traversal plan P7).
    pub(super) const PRODUCTION: Self = Self {
        max_entries: 5_000_000,
        max_empty_pages: 16,
        throttle: ThrottleBackoff::LISTING,
    };
}

/// The paging state of one directory's listing: continuations followed, entries listed, and the
/// current run of empty pages.
pub(super) struct Paging<T> {
    limits: ListingLimits,
    seen: HashSet<T>,
    entries: usize,
    empty_run: usize,
}

impl<T: Clone + Eq + Hash> Paging<T> {
    pub(super) fn new(limits: ListingLimits) -> Self {
        Self {
            limits,
            seen: HashSet::new(),
            entries: 0,
            empty_run: 0,
        }
    }

    /// Records a page of `listed` entries continuing at `next`, and returns where to continue.
    ///
    /// # Errors
    /// Each fails this directory only (entry-scoped, `Permanent`), so the traversal goes on with
    /// its siblings and reports this listing `Failed`: a continuation followed before (on the
    /// previous page or any earlier one), since the server would page this directory forever
    /// (`Protocol`); too many entries (`Capacity`); too many empty pages in a row (`Protocol`).
    pub(super) fn advance(
        &mut self,
        directory: &StoragePath,
        listed: usize,
        next: Option<T>,
    ) -> Result<Option<T>, StorageRoleFailure> {
        self.entries = self.entries.saturating_add(listed);
        if self.entries > self.limits.max_entries {
            return Err(failure(
                directory,
                FailureClass::Capacity,
                "the S3 directory lists more entries than one listing may hold",
            ));
        }
        self.empty_run = if listed == 0 { self.empty_run + 1 } else { 0 };
        let Some(next) = next else {
            return Ok(None);
        };
        if self.empty_run > self.limits.max_empty_pages {
            return Err(failure(
                directory,
                FailureClass::Protocol,
                "the S3 listing keeps returning empty pages that claim more follow",
            ));
        }
        if !self.seen.insert(next.clone()) {
            return Err(failure(
                directory,
                FailureClass::Protocol,
                "the S3 listing continuation came back: paging would never end",
            ));
        }
        Ok(Some(next))
    }
}

fn failure(directory: &StoragePath, class: FailureClass, diagnostic: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            directory.clone(),
            Operation::Traverse,
            class,
            Transience::Permanent,
            diagnostic,
        )
        .unwrap_or_else(|_| unreachable!("static S3 paging diagnostics are valid")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> StoragePath {
        StoragePath::new("d").unwrap_or_else(|error| panic!("{error}"))
    }

    fn class(result: &Result<Option<u32>, StorageRoleFailure>) -> Option<(bool, FailureClass)> {
        match result {
            Ok(_) => None,
            Err(StorageRoleFailure::Entry(f)) => Some((false, f.class())),
            Err(StorageRoleFailure::Session(f)) => Some((true, f.class())),
        }
    }

    #[test]
    fn a_continuation_seen_before_fails_the_directory() {
        let mut paging = Paging::new(ListingLimits::PRODUCTION);
        assert_eq!(paging.advance(&directory(), 1, Some(1)), Ok(Some(1)));
        assert_eq!(paging.advance(&directory(), 1, Some(2)), Ok(Some(2)));
        let repeated = paging.advance(&directory(), 1, Some(1));
        assert_eq!(class(&repeated), Some((false, FailureClass::Protocol)));
        let Err(StorageRoleFailure::Entry(failure)) = repeated else {
            panic!("a looping continuation must fail the directory: {repeated:?}");
        };
        assert_eq!(failure.path(), &directory());
        assert_eq!(failure.transience(), Transience::Permanent);
    }

    #[test]
    fn too_many_entries_fail_the_directory() {
        let limits = ListingLimits {
            max_entries: 3,
            max_empty_pages: 16,
            ..ListingLimits::PRODUCTION
        };
        let mut paging = Paging::new(limits);
        assert_eq!(paging.advance(&directory(), 3, Some(1)), Ok(Some(1)));
        let over = paging.advance(&directory(), 1, None);
        assert_eq!(class(&over), Some((false, FailureClass::Capacity)));
    }

    #[test]
    fn a_run_of_empty_truncated_pages_fails_the_directory() {
        let limits = ListingLimits {
            max_entries: 100,
            max_empty_pages: 2,
            ..ListingLimits::PRODUCTION
        };
        let mut paging = Paging::new(limits);
        assert_eq!(paging.advance(&directory(), 0, Some(1)), Ok(Some(1)));
        assert_eq!(paging.advance(&directory(), 5, Some(2)), Ok(Some(2)));
        assert_eq!(paging.advance(&directory(), 0, Some(3)), Ok(Some(3)));
        assert_eq!(paging.advance(&directory(), 0, Some(4)), Ok(Some(4)));
        let stuck = paging.advance(&directory(), 0, Some(5));
        assert_eq!(class(&stuck), Some((false, FailureClass::Protocol)));
        // An empty last page is fine.
        let mut last = Paging::<u32>::new(limits);
        assert_eq!(last.advance(&directory(), 0, None), Ok(None));
    }
}
