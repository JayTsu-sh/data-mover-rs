//! How one S3 directory's listing pages (ADR-0006 C22, P7 C6): the bounds on paging, and how a
//! page request that failed for a passing reason is sent again.
//!
//! - **Bounds.** A broken server can page without end (a continuation that comes back, empty pages
//!   that claim more follow) and a huge directory can list more than a listing should hold (the
//!   continuation and subdirectory sets of a streaming listing, a whole directory of versions).
//!   Hitting a bound fails that directory's listing — the traversal reports it and goes on with its
//!   siblings, never a silently truncated block and never the end of the session.
//! - **Retries.** A page request is stateless: the same continuation sent again asks for the same
//!   page, before the first page as after it (P7 §5.2, §8). Throttling backs off within its own
//!   budget ([`ThrottleBackoff`]); any other transient failure (a lost connection, a 5xx) is sent
//!   again a few times, the count starting over after every page that arrives. Only a failure that
//!   outlasts both ends the listing, with its own scope.

use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::time::Duration;

use tokio::time::sleep;

use super::{S3ProtocolFailure, S3Result};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;
use crate::storage::throttle::{ThrottleBackoff, ThrottleBudget, retry_throttled};

/// How far one directory's listing may page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ListingLimits {
    /// Objects, versions, delete markers and common prefixes one directory may list.
    pub(super) max_entries: usize,
    /// Pages in a row that list nothing yet claim more follow.
    pub(super) max_empty_pages: usize,
    /// How long one listing backs off while the service throttles it (P7 §8, Q11).
    pub(super) throttle: ThrottleBackoff,
    /// The delays before each resend of a page request that failed transiently (not throttled)
    /// without a page arriving in between; one delay per resend.
    pub(super) resends: &'static [Duration],
}

impl ListingLimits {
    /// 5 million entries (5 000 full pages), kept from before listings streamed (traversal plan
    /// P7): a directory of versions is still held whole, and a streaming listing of current
    /// objects still holds its continuations and subdirectory names. Three resends 0.25–2 s
    /// apart, on top of the SDK's own attempts, as the HDFS listing does (P7 C8).
    pub(super) const PRODUCTION: Self = Self {
        max_entries: 5_000_000,
        max_empty_pages: 16,
        throttle: ThrottleBackoff::LISTING,
        resends: &[
            Duration::from_millis(250),
            Duration::from_secs(1),
            Duration::from_secs(2),
        ],
    };
}

/// What one listing has left of its retries: the throttling budget, spent over the whole listing,
/// and the resends of a transient failure, renewed by every page that arrives.
pub(super) struct PageRetry {
    throttle: ThrottleBudget,
    resends: &'static [Duration],
    /// Resends since the last page arrived.
    resent: usize,
}

impl PageRetry {
    pub(super) const fn new(limits: ListingLimits) -> Self {
        Self {
            throttle: limits.throttle.start(),
            resends: limits.resends,
            resent: 0,
        }
    }

    /// Runs one page request, sent again while it is throttled (within the budget) or fails
    /// transiently (within the resends). Each attempt is a new request for the same page: the
    /// caller's continuation does not change. The sleeps are plain sleeps in this future, so
    /// dropping it abandons them at once.
    ///
    /// # Errors
    /// The failure that outlasted its retries, or any other failure as it came. Throttling past
    /// the budget is this listing's `Throttled` / `Transient` failure ([`past_budget`]).
    pub(super) async fn fetch<T, F, Fut>(&mut self, mut attempt: F) -> S3Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = S3Result<T>>,
    {
        loop {
            match retry_throttled(&mut self.throttle, is_throttled, &mut attempt).await {
                Ok(page) => {
                    self.resent = 0;
                    return Ok(page);
                }
                Err(failure) if resendable(&failure) => {
                    let Some(delay) = self.resends.get(self.resent).copied() else {
                        return Err(failure);
                    };
                    self.resent += 1;
                    tracing::debug!(
                        resend = self.resent,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "S3 listing page failed transiently; sending the same request again"
                    );
                    sleep(delay).await;
                }
                Err(failure) => return Err(past_budget(failure)),
            }
        }
    }
}

/// Whether the service asked this listing to slow down; it backs off rather than fails.
fn is_throttled(failure: &S3ProtocolFailure) -> bool {
    class_of(failure) == FailureClass::Throttled
}

/// A transient failure other than throttling (which has its own budget): the same request may
/// well succeed a moment later.
fn resendable(failure: &S3ProtocolFailure) -> bool {
    let (S3ProtocolFailure::Entry { transience, .. }
    | S3ProtocolFailure::Session { transience, .. }) = failure;
    *transience == Transience::Transient && !is_throttled(failure)
}

const fn class_of(failure: &S3ProtocolFailure) -> FailureClass {
    let (S3ProtocolFailure::Entry { class, .. } | S3ProtocolFailure::Session { class, .. }) =
        failure;
    *class
}

/// A listing still throttled once its backoff budget is spent fails that listing alone: the
/// service is busy, the session is fine, and the traversal goes on (P7 §8, Q11).
fn past_budget(failure: S3ProtocolFailure) -> S3ProtocolFailure {
    match failure {
        S3ProtocolFailure::Session {
            class: FailureClass::Throttled,
            diagnostic,
            ..
        } => S3ProtocolFailure::entry(
            FailureClass::Throttled,
            Transience::Transient,
            format!("{diagnostic}; still throttled after the listing's backoff budget"),
        ),
        other => other,
    }
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
        self.advance_keeping(directory, listed, listed, next)
    }

    /// [`Self::advance`] for a page of `listed` entries of which the listing kept `kept`: only
    /// those count towards the entry limit (a streaming listing drops a common prefix it already
    /// handed over, which an unordered store repeats wherever one of its keys falls), while
    /// `listed` decides whether the page was empty.
    ///
    /// # Errors
    /// As [`Self::advance`].
    pub(super) fn advance_keeping(
        &mut self,
        directory: &StoragePath,
        listed: usize,
        kept: usize,
        next: Option<T>,
    ) -> Result<Option<T>, StorageRoleFailure> {
        self.entries = self.entries.saturating_add(kept);
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
