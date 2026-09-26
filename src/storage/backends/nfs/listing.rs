//! The NFS role listing, page by page (P7 C7: §5.2, §8, §12.7 fix A).
//!
//! One producer per listing ([`produce`]) reads the directory with READDIRPLUS through
//! [`NfsNamespaceProtocol::read_directory`] and hands it over in pages of at most
//! `request.page_hint` children as it reads: the first page goes out long before a large
//! directory is read to its end, and backpressure stops the reader after one page ahead. Dropping
//! the stream or cancelling the request stops it before its next request to the server; a
//! request already in flight is let finish (see [`NfsNamespaceProtocol::read_directory`]).
//!
//! **Descent hint.** Every child directory is handed back with a [`DescentHint`] holding a copy of
//! its file handle (never a slice of the READDIRPLUS reply, which would keep the whole reply alive
//! while the directory waits to be listed). A request carrying one reads that handle straight
//! away, with no LOOKUP. Without a hint (a traversal's root, a caller that keeps none), or when
//! the hinted handle no longer names the directory (`STALE`, `BADHANDLE`, `FHEXPIRED`), the path is
//! resolved through [`NfsNamespaceProtocol::directory_handle`]. The listing itself never caches a
//! handle: before P7 C7 it put every listed subdirectory's handle into the process-wide handle
//! cache, which held about 625 B per directory of a whole tree (§12.7).
//!
//! **Failures.** Until the first page is handed over, a failure the protocol says a fresh read
//! could cure is retried from the directory's start, so no child is handed over twice: a handle
//! that no longer holds is resolved again (at most [`MAX_RELOOKUPS`] times after the hint), a busy
//! server is backed off within the listing's throttle budget, a transport failure nfs-rs certifies
//! safe to resend is read anew after [`RESEND_DELAYS`]. What no retry cured keeps its scope (a
//! connectivity failure ends the session). After the first page nothing is retried —
//! READDIRPLUS could only be continued from its cookie, which nfs-rs does not expose (Q18) — so
//! the children read so far are handed over and the listing ends with the failure, which the
//! traversal records as `Interrupted`; a transport failure safe to resend is then this listing's
//! alone: nfs-rs certified resending it on the session it has.

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::sleep;

use super::namespace::{
    HINT_TAG_LEN, NfsListedChild, NfsListingPages, NfsNamespaceAdapter, NfsNamespaceProtocol,
    NfsReadEnd, NfsReadFailure, NfsRestart, NfsUndescribedChild, checked_allow_root, descriptor,
    is_internal, undescribed,
};
use super::source::{NfsProtocolFailure, entry_scoped, role_failure};
use crate::model::{EntryKind, Operation, StoragePath};
use crate::storage::listing::{PageSink, cancelled, respelled};
use crate::storage::throttle::{ThrottleBackoff, ThrottleBudget};
use crate::storage::{ChildFailure, DescentHint, ListRequest, ListingPage, StorageRoleFailure};

/// How a listing's failure is scoped: [`role_failure`] or [`entry_scoped`].
type Scope = fn(&StoragePath, Operation, NfsProtocolFailure) -> StorageRoleFailure;

/// How often a listing that has handed nothing over yet resolves its path again after the handle
/// it read went stale — the retries `lookup_fh` callers have always had (`MAX_STALE_RETRIES`).
pub(super) const MAX_RELOOKUPS: u8 = 3;

/// The pauses before a listing that has handed nothing over yet reads its directory again after a
/// transport failure nfs-rs certifies safe to resend — what the S3 listing waits before resending
/// a page (P7 C6).
pub(super) const RESEND_DELAYS: [Duration; 3] = [
    Duration::from_millis(250),
    Duration::from_secs(1),
    Duration::from_secs(2),
];

/// Produces the listing of `request.path` into `sink`.
pub(super) async fn produce(namespace: NfsNamespaceAdapter, request: ListRequest, sink: PageSink) {
    if let Err(failure) = checked_allow_root(&request.path) {
        return sink.fail(failure).await;
    }
    let path = &request.path;
    let protocol = Arc::clone(&namespace.protocol);
    let mut pages = Pages::new(&namespace, &request, &sink);
    // The handle to read with, and whether it came from the hint.
    let mut handle = request
        .descent_hint
        .as_ref()
        .and_then(|hint| hinted_handle(&namespace, hint))
        .map(|handle| (handle, true));
    let mut restarts = Restarts {
        relookups: 0,
        resends: 0,
        backoff: ThrottleBackoff::LISTING.start(),
    };
    loop {
        if pages.stopping() {
            return pages.stop().await;
        }
        let (current, hinted) =
            match known_or_resolved(handle.take(), protocol.as_ref(), path).await {
                Ok(handle) => handle,
                Err(failure) => return pages.fail(failure, role_failure).await,
            };
        let read = protocol.read_directory(path, current.clone(), &mut pages);
        let failure = match read.await {
            Ok(NfsReadEnd::Complete) => return pages.finish().await,
            Ok(NfsReadEnd::Stopped) => return pages.stop().await,
            Err(failure) => failure,
        };
        match restarts
            .after(failure, hinted, protocol.as_ref(), &pages)
            .await
        {
            // Nothing has been handed over: reading the directory again from its start repeats
            // no child. What this attempt had read is dropped.
            Next::Reread => {
                pages.discard();
                handle = Some((current, hinted));
            }
            Next::Resolve => pages.discard(),
            Next::Fail(failure) => return pages.fail(failure, role_failure).await,
            Next::Interrupt(failure) => return pages.fail(failure, entry_scoped).await,
            Next::Stop => return pages.stop().await,
        }
    }
}

/// The longest file handle a hint carries: `NFS4_FHSIZE` (an `NFSv3` handle is at most 64 bytes).
const MAX_HINTED_HANDLE: usize = 128;

/// The descent hint for a listed subdirectory whose handle is `handle`: the namespace's tag, then
/// a copy of the handle. `None` for a handle no READDIRPLUS could be sent with.
fn descent_hint(namespace: &NfsNamespaceAdapter, handle: &[u8]) -> Option<DescentHint> {
    if handle.is_empty() || handle.len() > MAX_HINTED_HANDLE {
        return None;
    }
    let mut hint = [0_u8; HINT_TAG_LEN + MAX_HINTED_HANDLE];
    hint[..HINT_TAG_LEN].copy_from_slice(&namespace.hint_tag);
    hint[HINT_TAG_LEN..HINT_TAG_LEN + handle.len()].copy_from_slice(handle);
    Some(DescentHint::copied_from(
        &hint[..HINT_TAG_LEN + handle.len()],
    ))
}

/// The handle a request's hint carries, when this namespace made it. A hint from any other
/// namespace (another export, another root) could name a directory outside this one's root, so
/// it is ignored and the path resolved instead.
fn hinted_handle(namespace: &NfsNamespaceAdapter, hint: &DescentHint) -> Option<Bytes> {
    hint.as_bytes()
        .strip_prefix(namespace.hint_tag.as_slice())
        .filter(|handle| !handle.is_empty())
        .map(Bytes::copy_from_slice)
}

/// The handle to read with, and whether it came from the hint: the one in hand, or else the
/// directory's path resolved.
async fn known_or_resolved(
    known: Option<(Bytes, bool)>,
    protocol: &dyn NfsNamespaceProtocol,
    path: &StoragePath,
) -> Result<(Bytes, bool), NfsProtocolFailure> {
    match known {
        Some(known) => Ok(known),
        None => Ok((protocol.directory_handle(path).await?, false)),
    }
}

/// What a listing does after a failed read.
enum Next {
    /// Read the directory again from its start, with the same handle.
    Reread,
    /// Resolve the directory's path, then read it again from its start.
    Resolve,
    /// End with this failure.
    Fail(NfsProtocolFailure),
    /// End with this failure as this listing's alone, even when its class is a connectivity one:
    /// nfs-rs certified resending it on the session it has.
    Interrupt(NfsProtocolFailure),
    /// The listing was cancelled or dropped meanwhile.
    Stop,
}

/// What one listing has left of its restarts.
struct Restarts {
    relookups: u8,
    resends: usize,
    backoff: ThrottleBudget,
}

impl Restarts {
    /// Whether to read the directory again after `failure`: only while no page has been handed
    /// over, and only for what a fresh read can cure.
    async fn after(
        &mut self,
        failure: NfsReadFailure,
        hinted: bool,
        protocol: &dyn NfsNamespaceProtocol,
        pages: &Pages<'_>,
    ) -> Next {
        if pages.handed_over > 0 {
            return match failure.restart {
                NfsRestart::Resend => Next::Interrupt(failure.failure),
                _ => Next::Fail(failure.failure),
            };
        }
        match failure.restart {
            // The hint went stale: resolve the path, which costs what a listing without a hint
            // always costs.
            NfsRestart::Relookup if hinted => Next::Resolve,
            NfsRestart::Relookup if self.relookups < MAX_RELOOKUPS => {
                self.relookups += 1;
                match protocol.forget_directory(&pages.request.path).await {
                    Ok(()) => Next::Resolve,
                    Err(forget) => Next::Fail(forget),
                }
            }
            NfsRestart::Backoff => match self.backoff.next_delay() {
                Some(delay) => {
                    if pages.wait(delay).await {
                        Next::Reread
                    } else {
                        Next::Stop
                    }
                }
                None => Next::Fail(failure.failure),
            },
            NfsRestart::Resend => match RESEND_DELAYS.get(self.resends) {
                Some(delay) => {
                    self.resends += 1;
                    if pages.wait(*delay).await {
                        Next::Reread
                    } else {
                        Next::Stop
                    }
                }
                None => Next::Fail(failure.failure),
            },
            NfsRestart::Relookup | NfsRestart::Never => Next::Fail(failure.failure),
        }
    }
}

/// The pages of one listing: children are described as they are read, so the READDIRPLUS reply
/// they came from is not kept alive by the page.
struct Pages<'a> {
    namespace: &'a NfsNamespaceAdapter,
    request: &'a ListRequest,
    sink: &'a PageSink,
    page: ListingPage,
    /// Children and child failures in `page`; hidden entries (artifacts, ONTAP's `NFSv3` snapshot
    /// directory) do not count against the hint.
    held: usize,
    handed_over: usize,
}

impl<'a> Pages<'a> {
    fn new(
        namespace: &'a NfsNamespaceAdapter,
        request: &'a ListRequest,
        sink: &'a PageSink,
    ) -> Self {
        Self {
            namespace,
            request,
            sink,
            page: ListingPage::default(),
            held: 0,
            handed_over: 0,
        }
    }

    fn discard(&mut self) {
        self.page = ListingPage::default();
        self.held = 0;
    }

    fn has_content(&self) -> bool {
        !self.page.is_empty() || self.page.internal_hidden > 0
    }

    /// The directory was read to its end: hands over what is left — one empty page when the
    /// directory is empty.
    async fn finish(&mut self) {
        if (self.has_content() || self.handed_over == 0) && !self.hand_over().await {
            self.stop().await;
        }
    }

    /// Hands over the children read so far, then ends the listing with `failure`, scoped by
    /// `scope` (`role_failure` promotes a connectivity class to the session).
    async fn fail(&mut self, failure: NfsProtocolFailure, scope: Scope) {
        if self.has_content() && !self.hand_over().await {
            return self.stop().await;
        }
        let path = &self.request.path;
        self.sink
            .fail(scope(path, Operation::Namespace, failure))
            .await;
    }

    /// The listing is over without a failure of its own: a cancelled one ends with `Cancelled`
    /// (a signal, R10); one whose consumer is gone just ends.
    async fn stop(&self) {
        if self.request.cancel.is_cancelled() {
            self.sink.fail(cancelled(&self.request.path)).await;
        }
    }

    /// Sleeps `delay`, or less when the listing is cancelled or dropped meanwhile (`false`).
    async fn wait(&self, delay: Duration) -> bool {
        tokio::select! {
            biased;
            () = self.request.cancel.cancelled() => false,
            () = self.sink.closed() => false,
            () = sleep(delay) => true,
        }
    }

    fn add(&mut self, child: NfsListedChild) {
        let directory = &self.request.path;
        match child {
            Ok(entry) if is_internal(&entry.path) => {
                self.page.internal_hidden = self.page.internal_hidden.saturating_add(1);
                return;
            }
            Ok(entry) => {
                let hint = (entry.kind == EntryKind::Directory)
                    .then(|| descent_hint(self.namespace, &entry.file_handle))
                    .flatten();
                let spelled = respelled(directory, entry.path.clone());
                match descriptor(&self.namespace.identity, spelled, &entry) {
                    Ok(described) => self.page.push_descriptor(directory, described, hint),
                    Err(failure) => self
                        .page
                        .push_failure(ChildFailure::from_failure(directory, failure)),
                }
            }
            Err(NfsUndescribedChild {
                raw_name: Some(raw),
                reason,
                failure,
                ..
            }) => self.page.push_failure(ChildFailure::unnamed(
                &self.namespace.identity,
                directory,
                raw,
                failure.class,
                reason,
            )),
            Err(child) if is_internal(&child.path) => {
                self.page.internal_hidden = self.page.internal_hidden.saturating_add(1);
                return;
            }
            Err(child) => self
                .page
                .push_failure(ChildFailure::from_failure(directory, undescribed(child))),
        }
        self.held += 1;
    }
}

#[async_trait]
impl NfsListingPages for Pages<'_> {
    fn push(&mut self, child: NfsListedChild) -> bool {
        self.add(child);
        self.held >= self.request.page_hint.get()
    }

    fn hide(&mut self) {
        self.page.internal_hidden = self.page.internal_hidden.saturating_add(1);
    }

    async fn hand_over(&mut self) -> bool {
        let page = mem::take(&mut self.page);
        self.held = 0;
        let sent = tokio::select! {
            biased;
            () = self.request.cancel.cancelled() => false,
            sent = self.sink.send(page) => sent.is_ok(),
        };
        if sent {
            self.handed_over += 1;
        }
        sent
    }

    fn stopping(&self) -> bool {
        self.request.cancel.is_cancelled() || self.sink.is_closed()
    }
}

#[cfg(test)]
#[path = "listing_tests.rs"]
mod tests;
