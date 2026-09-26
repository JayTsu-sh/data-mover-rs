//! What a traversal delivers: entries, failures scoped to a child or to a listing, directory
//! events, and checkpoints (P7 §4).

use bytes::Bytes;

use super::counts::BlockCounts;
use crate::model::{EntryOperationFailure, ObservedEntry, StoragePath};

/// One traversal item. Failures do not terminate the session.
///
/// A listed directory `D` produces its block — one item per admitted child, in listing order, page
/// by page, each page's children the listing could not describe ahead of that page's others, and
/// last `D`'s own [`FailureScope::Listing`] failure if its listing stopped — then
/// [`TraversalItem::DirectoryListed`] for `D`, and then, for every subdirectory `D` descends into,
/// in the byte order of their names, that same sequence recursively
/// ([`super::TraversalOrder::Blocks`]). Only a directory that was
/// actually listed gets a `DirectoryListed`: a directory `max_depth` stops at is emitted as an
/// entry and never listed, and a directory the filter hides but still descends into has its
/// `DirectoryListed` and no [`TraversalItem::Entry`] of its own.
///
/// What a consumer may conclude from a traversal is governed by the deletion-safety rules on
/// [`ScanFailure`]; only [`super::TraversalOutcome::Completed`] makes any of it final.
///
/// Every variant is boxed, so an item is two words however much its payload grows: items are
/// moved through the session's channel one per entry.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalItem {
    Entry(Box<ObservedEntry>),
    /// A child that could not be observed, or a directory that could not be listed; see
    /// [`ScanFailure::scope`].
    Failure(Box<ScanFailure>),
    /// Every direct child of one listed directory has been delivered.
    DirectoryListed(Box<DirectoryListed>),
    /// A position a later traversal can resume from. Only a resumable traversal emits it (P7
    /// §9, not yet implemented): no traversal emits one today.
    Checkpoint(Box<Checkpoint>),
}

/// What one failure item is about.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FailureScope {
    /// This one child: the listing returned it but could not describe it, or observing it
    /// failed. Its siblings and the listing are unaffected.
    Child,
    /// The listing of the directory at [`ScanFailure::path`] is incomplete: it could not be
    /// listed at all, or stopped part way. Siblings of that directory are unaffected.
    Listing,
}

/// A failure item.
///
/// **Deletion safety** (the consumer's contract, P7 §4.1):
///
/// 1. Only a traversal whose outcome is [`super::TraversalOutcome::Completed`] may drive delete
///    detection.
/// 2. For a [`FailureScope::Listing`] failure, the **whole previously known subtree** of that
///    directory takes no part in deletion: a listing that stopped may never have shown the
///    subdirectories below it.
/// 3. A [`FailureScope::Child`] failure's own entry takes no part in deletion.
/// 4. With a filter or `max_depth`, previously known entries are judged by the same filter and
///    depth, and anything they exclude or cut off takes no part in deletion.
/// 5. On a case- or normalisation-insensitive destination, two names equal after folding but
///    different in bytes are reported as a conflict, not deleted.
///
/// A child failure whose `path` is the listed directory itself — the listing could not even name
/// the child — cannot say which child it was, so none of that directory's previously known direct
/// children take part in deletion (rule 3 applied to every child it could have been). Its
/// diagnostic carries the child's raw name, and its identity is derived from it.
///
/// Two things are **not** failures and produce no item (P7 §8): a child that was listed and was
/// gone by the time it was described or observed ([`super::BlockCounts::vanished`]; it no longer
/// exists, so it may be treated as deleted), and throttling, which a listing backs off from and
/// which fails at most one listing or one child, never the traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ScanFailure {
    /// The child for [`FailureScope::Child`] (the listed directory, when the listing could not
    /// even name it); the directory for [`FailureScope::Listing`].
    pub path: StoragePath,
    pub scope: FailureScope,
    /// Class, transience and diagnostic.
    pub failure: EntryOperationFailure,
    /// For [`FailureScope::Listing`]: how many children the listing returned before it failed —
    /// emitted, filtered, failed or vanished alike, the block's [`BlockCounts::listed`] — all of
    /// them ahead of this failure in the block. `0`: it returned none, and the listing is
    /// [`ListingOutcome::Unreadable`]; more, and it is [`ListingOutcome::Interrupted`]. `None`
    /// for a child failure.
    pub delivered: Option<u64>,
}

impl ScanFailure {
    /// One child's failure, at the failure's own path.
    #[must_use]
    pub fn child(failure: EntryOperationFailure) -> Self {
        Self {
            path: failure.path().clone(),
            scope: FailureScope::Child,
            failure,
            delivered: None,
        }
    }

    /// The listing of `directory` failed after `delivered` of its children were delivered.
    #[must_use]
    pub const fn listing(
        directory: StoragePath,
        failure: EntryOperationFailure,
        delivered: u64,
    ) -> Self {
        Self {
            path: directory,
            scope: FailureScope::Listing,
            failure,
            delivered: Some(delivered),
        }
    }

    /// The outcome this failure gives its directory's listing, or `None` for a child failure,
    /// which leaves the listing complete.
    #[must_use]
    pub const fn listing_outcome(&self) -> Option<ListingOutcome> {
        match (self.scope, self.delivered) {
            (FailureScope::Child, _) => None,
            (FailureScope::Listing, Some(0) | None) => Some(ListingOutcome::Unreadable),
            (FailureScope::Listing, Some(_)) => Some(ListingOutcome::Interrupted),
        }
    }
}

/// How one directory's listing ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ListingOutcome {
    /// The listing ran to its end. Children that failed individually are counted in
    /// [`BlockCounts::failed_children`]; they do not make a listing incomplete.
    Complete,
    /// The listing stopped after returning some children, which were delivered (and whose
    /// subdirectories are still descended into). A [`FailureScope::Listing`] failure for the
    /// directory, carrying how many, precedes this event.
    Interrupted,
    /// Not one child could be listed. A [`FailureScope::Listing`] failure for the directory
    /// precedes this event.
    Unreadable,
}

impl ListingOutcome {
    /// Whether the listing ran to its end. Anything else excludes the directory's whole subtree
    /// from deletion (rule 2 on [`ScanFailure`]).
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Every direct child of one listed directory has been delivered.
///
/// Emitted after the block's last item, and for an empty directory too.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DirectoryListed {
    pub path: StoragePath,
    pub outcome: ListingOutcome,
    /// What this directory's block held; see [`BlockCounts`].
    pub counts: BlockCounts,
    /// The directory was gone when its own listing began (`NotFound`, never the traversal root):
    /// the block is [`ListingOutcome::Complete`] and empty because nothing exists below it any
    /// more, not because the directory is empty. Counted in
    /// [`super::ScanTotals::vanished_listings`]. The converse does not hold: a directory reported
    /// vanished as a child may still be listed with children, when it was recreated before its
    /// own listing ran — a scan is not a snapshot.
    pub vanished: bool,
}

impl DirectoryListed {
    #[must_use]
    pub const fn new(path: StoragePath, outcome: ListingOutcome, counts: BlockCounts) -> Self {
        Self {
            path,
            outcome,
            counts,
            vanished: false,
        }
    }
}

/// An opaque resume position (P7 §9). Its format is versioned and bound to the request that
/// produced it; nothing emits one yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint(Bytes);

impl Checkpoint {
    /// A checkpoint from the bytes a consumer persisted.
    #[must_use]
    pub const fn from_bytes(bytes: Bytes) -> Self {
        Self(bytes)
    }

    /// The bytes to persist.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
