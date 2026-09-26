//! The streaming listing of one S3 directory's current objects (P7 C6, §5.2, §5.3):
//! `ListObjectsV2` with delimiter `/` under `<path>/`, each server page described and handed over
//! — in pages of at most the request's `page_hint` children — as soon as it arrives. The consumer
//! holds the first children after one request, whatever the size of the directory.
//!
//! **No order is assumed.** A store need not return keys sorted, nor each common prefix once:
//! - Every subdirectory is handed over once. The listing keeps the name of every common prefix it
//!   has handed over (or hidden, or failed) — the listing's and those rolled up from keys a store
//!   reported below a deeper `/` — so a prefix repeated on a later page, or out of order, is not a
//!   second subdirectory (whose whole subtree would be scanned twice). Memory: one name per
//!   subdirectory of this directory (P7 §5.3).
//! - An object repeated across a page boundary is dropped when the previous page listed it
//!   (memory: one page of keys). One repeated further apart is handed over again: the listing
//!   contract allows a child twice, and a consumer deduplicates by (parent, name) (Q12, Q13).
//! - Children are in S3 key order within a server page; across pages, in the order the store
//!   returned its pages.
//!
//! **Paging.** The continuation token is opaque and passed back as it came. A throttled or
//! transiently failed page is requested again with the same token ([`PageRetry`]): the request is
//! stateless, so a listing resumes where it was rather than restart or end (§5.2, §8). The bounds
//! of [`Paging`] still hold: a token that comes back, a run of empty pages or too many entries
//! (counted as kept: a repeated prefix does not count again) fail this directory's listing —
//! after the pages already handed over, which a traversal then reports `Interrupted`. The page
//! that trips a bound is not handed over (a page naming a token seen before is usually a page
//! served before).
//!
//! A hidden `.data-mover-*` object, or a key outside the directory, that a store repeats on pages
//! further apart is counted, or reported, again: only prefixes are remembered for the whole
//! listing.
//!
//! **Absence.** A prefix under which nothing at all is stored does not exist: only a listing that
//! reached its end without listing anything is `NotFound` (no page has been handed over then).

use std::collections::HashSet;

use super::listing::{Block, Child, directory_prefix, merge, place, refuse_absent};
use super::paging::{ListingLimits, PageRetry, Paging};
use super::source::role_failure;
use super::{S3ObjectPage, S3Protocol};
use crate::model::{BackendIdentity, Operation, StoragePath};
use crate::storage::listing::cancelled;
use crate::storage::{ListRequest, ListingPage, PageSink, StorageRoleFailure};

/// Produces the listing `request` asks for through `sink`, page by page. Cancellation, or the
/// consumer dropping the stream, abandons the request in flight (and any retry delay) at once:
/// nothing stays open between S3 requests.
pub(super) async fn produce_current<P: S3Protocol + ?Sized>(
    protocol: &P,
    identity: &BackendIdentity,
    limits: ListingLimits,
    request: &ListRequest,
    sink: &PageSink,
) {
    let mut listing = match CurrentListing::new(protocol, identity, &request.path, limits) {
        Ok(listing) => listing,
        Err(failure) => return sink.fail(failure).await,
    };
    let mut sent = false;
    loop {
        let next = tokio::select! {
            biased;
            () = request.cancel.cancelled() => return sink.fail(cancelled(&request.path)).await,
            () = sink.closed() => return,
            next = listing.next_page() => next,
        };
        match next {
            // A page of nothing (empty, or only the directory's own marker) is not handed over.
            Ok(Some(page)) if page.is_empty() && page.internal_hidden == 0 => {}
            Ok(Some(page)) => {
                if sink.send_split(page, request).await.is_err() {
                    return;
                }
                sent = true;
            }
            Ok(None) => {
                // An empty directory still hands over its one (empty) page.
                if !sent {
                    let _ = sink.send(ListingPage::default()).await;
                }
                return;
            }
            Err(failure) => return sink.fail(failure).await,
        }
    }
}

/// Where one listing of current objects stands.
struct CurrentListing<'a, P: ?Sized> {
    protocol: &'a P,
    identity: &'a BackendIdentity,
    directory: &'a StoragePath,
    /// `<path>/`, or empty for the storage root.
    prefix: String,
    paging: Paging<String>,
    retry: PageRetry,
    /// The continuation of the next page; `None` before the first page and after the last.
    token: Option<String>,
    finished: bool,
    /// Whether any page listed anything at all (the directory exists).
    listed_any: bool,
    seen: Seen,
}

/// What the listing remembers across pages so that it hands each child over once.
#[derive(Default)]
struct Seen {
    /// Names (below the directory's prefix, with the trailing `/`) of the common prefixes handed
    /// over, hidden or failed.
    prefixes: HashSet<Box<str>>,
    /// Object keys of the previous server page.
    previous: HashSet<String>,
    /// Keys whose undecodable entry was reported.
    rejected: HashSet<String>,
    /// Whether a directory marker with content was reported.
    own_marker: bool,
}

impl<'a, P: S3Protocol + ?Sized> CurrentListing<'a, P> {
    fn new(
        protocol: &'a P,
        identity: &'a BackendIdentity,
        directory: &'a StoragePath,
        limits: ListingLimits,
    ) -> Result<Self, StorageRoleFailure> {
        Ok(Self {
            protocol,
            identity,
            directory,
            prefix: directory_prefix(directory)?,
            paging: Paging::new(limits),
            retry: PageRetry::new(limits),
            token: None,
            finished: false,
            listed_any: false,
            seen: Seen::default(),
        })
    }

    /// The next server page, described; `Ok(None)` once the directory is listed to its end.
    ///
    /// # Errors
    /// The page request's failure once its retries are spent (with its own scope), a paging bound
    /// hit (this directory's failure), or `NotFound` when the listing ended having listed nothing
    /// under a prefix other than the storage root.
    async fn next_page(&mut self) -> Result<Option<ListingPage>, StorageRoleFailure> {
        if self.finished {
            return Ok(None);
        }
        let (protocol, prefix, token) =
            (self.protocol, self.prefix.as_str(), self.token.as_deref());
        let mut page = self
            .retry
            .fetch(|| protocol.list_objects_page(prefix, token))
            .await
            .map_err(|e| role_failure(self.directory, Operation::Traverse, e))?;
        let listed = page.objects.len() + page.prefixes.len() + page.rejected.len();
        self.listed_any |= listed > 0;
        let next = page.next.take();
        let described = self.describe(page);
        let kept = described.children.len() + described.failures.len();
        let hidden = usize::try_from(described.internal_hidden).unwrap_or(usize::MAX);
        self.token = self.paging.advance_keeping(
            self.directory,
            listed,
            kept.saturating_add(hidden),
            next,
        )?;
        if self.token.is_none() {
            self.finished = true;
            refuse_absent(self.directory, !self.listed_any)?;
        }
        Ok(Some(described))
    }

    /// One server page as a listing page: its objects, less those the previous page listed; its
    /// subdirectories, less those already handed over; its undecodable entries.
    fn describe(&mut self, page: S3ObjectPage) -> ListingPage {
        let S3ObjectPage {
            mut objects,
            prefixes,
            rejected,
            next: _,
        } = page;
        let mut keys = HashSet::with_capacity(objects.len());
        let previous = &self.seen.previous;
        objects.retain(|object| !previous.contains(&object.key) && keys.insert(object.key.clone()));
        self.seen.previous = keys;
        let placed = place(&self.prefix, objects, |object| &object.key);
        let mut block = Block::new(self.identity, self.directory, &self.prefix);
        block.outside(&placed.outside);
        if !self.seen.own_marker && placed.own.iter().any(|object| object.size > 0) {
            self.seen.own_marker = true;
            block.own_marker(true);
        }
        let mut rolled = placed.rolled;
        block.rejected(rejected, &mut rolled, &mut self.seen.rejected);
        let fresh = first_sightings(
            &self.prefix,
            &mut self.seen.prefixes,
            prefixes.into_iter().chain(rolled),
        );
        let children = placed.children.into_iter().map(Child::Object);
        block.build(merge(children, fresh, Vec::new()));
        block.finish()
    }
}

/// The common prefixes of `prefixes` not in `seen` yet, each once (and added to it). One outside
/// the directory `prefix` (a server ignoring the prefix) is kept every time: it becomes a
/// failure, never a child.
fn first_sightings(
    prefix: &str,
    seen: &mut HashSet<Box<str>>,
    prefixes: impl Iterator<Item = String>,
) -> Vec<String> {
    prefixes
        .filter(|common| {
            common
                .strip_prefix(prefix)
                .is_none_or(|name| seen.insert(Box::from(name)))
        })
        .collect()
}
