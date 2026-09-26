//! The CIFS role listing, page by page (P7 C9: §5.1, §5.2, §6.2, §8).
//!
//! One producer per listing ([`produce`]) opens the directory, reads its `QUERY_DIRECTORY`
//! records through [`CifsDirectory::entries`] and hands them over in pages of at most
//! `request.page_hint` children as it reads: the first page goes out long before a large
//! directory is read to its end, and backpressure stops the reader after one page ahead.
//!
//! **The producer holds the handle.** The directory stays open while the producer waits for its
//! consumer to take a page — a *parked* listing holds a real server handle, which is why the
//! namespace caps how many a consumer may keep parked
//! ([`Namespace::parked_listing_limit`](crate::storage::Namespace::parked_listing_limit)). Every
//! exit closes it: the directory read to its end, the consumer gone or the request cancelled
//! (checked before each record is taken, so the record in flight — and the request that brings
//! it — is let finish), and a failed read. The last page is handed over after the close. A
//! producer is never aborted from outside (§16, M1): a handle cannot be closed once its owner is
//! detached.
//!
//! **Failures.** Until the first page is handed over, a handle the server or smb-rs no longer
//! honours is cured by opening the directory again and reading it from its start, after
//! [`REOPEN_DELAYS`]; nothing has been handed over, so no child is handed over twice. smb-rs
//! reopens no directory handle after a reconnect (directory opens are not durable): the next
//! request on it fails with `InvalidState` ("stale share generation"), one in flight when the
//! connection died with `StaleObject`, and a request racing a recovery with `InvalidState` too
//! (smb-rs #79); the server itself answers `STATUS_FILE_CLOSED` or `STATUS_INVALID_HANDLE`. After
//! the first page nothing is retried — a directory's enumeration cannot be resumed on a new
//! handle — so the children read so far are handed over and the listing ends with the failure,
//! which the traversal records as `Interrupted`. Every failure keeps the class `classify` gives
//! it; a connectivity failure still ends the session. CIFS maps no status to `Throttled`, so
//! there is no backoff here. The first failed record ends the read: smb-rs's stream would send
//! another request after it.
//!
//! A close that fails is logged, never the listing's outcome: after a complete read every child
//! was read and is handed over, and a session that is really gone shows on the next request;
//! after a stop or a failed read the listing already has its outcome.
//!
//! **No descent hint.** A CIFS `CREATE` opens a directory by its share-relative path in one round
//! trip whatever its depth, and the facade offers no open by file id, so a listed child has
//! nothing to hand down that would spare its listing a request.

use std::mem;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};

use super::namespace::{
    CifsDirectory, CifsListedChild, CifsNamespace, CifsNamespaceProtocol, CifsUnspelled,
};
use super::source::{classify, entry_failure_with_transience};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::listing::{PageSink, cancelled};
use crate::storage::{ChildFailure, ListRequest, ListingPage, StorageRoleFailure};

/// The pauses before a listing that has handed nothing over yet opens its directory again after
/// the handle it read went stale: long enough for smb-rs to finish a reconnect (milliseconds on
/// ONTAP), and as many as the NFS listing's re-lookups.
pub(super) const REOPEN_DELAYS: [Duration; 3] = [
    Duration::from_millis(50),
    Duration::from_millis(250),
    Duration::from_secs(1),
];

const STATUS_INVALID_HANDLE: u32 = 0xC000_0008;
const STATUS_FILE_CLOSED: u32 = 0xC000_0128;

/// Produces the listing of `request.path` into `sink`.
pub(super) async fn produce(namespace: CifsNamespace, request: ListRequest, sink: PageSink) {
    let protocol = namespace.protocol.clone();
    let mut pages = Pages::new(&namespace, &request, &sink);
    let mut reopens = REOPEN_DELAYS.iter();
    loop {
        if pages.stopping() {
            return pages.stop().await;
        }
        let error = match pages.attempt(protocol.as_ref()).await {
            Ok(ReadEnd::Complete) => return pages.finish().await,
            Ok(ReadEnd::Stopped) => return pages.stop().await,
            Err(error) => error,
        };
        // Nothing handed over yet: reading the directory again from its start on a fresh handle
        // repeats no child. What this attempt had read is dropped.
        if pages.handed_over == 0
            && handle_lost(&error)
            && let Some(delay) = reopens.next()
        {
            tracing::debug!(
                path = request.path.as_str(),
                %error,
                "[CIFS] directory handle lost before the first page; reading it again"
            );
            pages.discard();
            if !pages.wait(*delay).await {
                return pages.stop().await;
            }
            continue;
        }
        let failure = classify(&request.path, Operation::Traverse, &error);
        return pages.fail(failure).await;
    }
}

/// Reads a whole directory, for callers that need every child of a small one at once (the
/// artifact sweep of `delete`). The read error wins over the close's.
pub(super) async fn read_whole(
    protocol: &dyn CifsNamespaceProtocol,
    path: &StoragePath,
) -> smb_domain::Result<Vec<CifsListedChild>> {
    let directory = protocol.open_directory(path).await?;
    let entries = directory.entries().try_collect::<Vec<_>>().await;
    let close = directory.close().await;
    let entries = entries?;
    close?;
    Ok(entries)
}

/// Whether `error` says the directory handle is no longer honoured, so a fresh open may succeed.
fn handle_lost(error: &smb_domain::Error) -> bool {
    match error {
        smb_domain::Error::StaleObject | smb_domain::Error::InvalidState(_) => true,
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => {
            matches!(*status, STATUS_FILE_CLOSED | STATUS_INVALID_HANDLE)
        }
        _ => false,
    }
}

/// How one read of the directory ended, when it did not fail.
enum ReadEnd {
    /// Every child was read.
    Complete,
    /// The consumer is gone or the request was cancelled.
    Stopped,
}

/// The pages of one listing: children are described as they are read.
struct Pages<'a> {
    namespace: &'a CifsNamespace,
    request: &'a ListRequest,
    sink: &'a PageSink,
    page: ListingPage,
    /// Children and child failures in `page`; hidden transfer artifacts do not count against the
    /// hint.
    held: usize,
    handed_over: usize,
}

impl<'a> Pages<'a> {
    fn new(namespace: &'a CifsNamespace, request: &'a ListRequest, sink: &'a PageSink) -> Self {
        Self {
            namespace,
            request,
            sink,
            page: ListingPage::default(),
            held: 0,
            handed_over: 0,
        }
    }

    /// Opens the directory, reads it, and closes it whatever the read did. The read decides the
    /// outcome; a failed close is logged (see the module docs).
    async fn attempt(
        &mut self,
        protocol: &dyn CifsNamespaceProtocol,
    ) -> smb_domain::Result<ReadEnd> {
        let directory = protocol.open_directory(&self.request.path).await?;
        let read = self.read(directory.as_ref()).await;
        if let Err(error) = directory.close().await {
            tracing::warn!(
                path = self.request.path.as_str(),
                %error,
                "[CIFS] closing a listed directory's handle failed"
            );
        }
        read
    }

    /// Reads `directory` record by record, handing over each full page. The entries stream is
    /// dropped when this returns, before the handle is closed.
    async fn read(&mut self, directory: &dyn CifsDirectory) -> smb_domain::Result<ReadEnd> {
        let mut entries = directory.entries();
        loop {
            if self.stopping() {
                return Ok(ReadEnd::Stopped);
            }
            let Some(child) = entries.next().await else {
                return Ok(ReadEnd::Complete);
            };
            if self.push(child?) && !self.hand_over().await {
                return Ok(ReadEnd::Stopped);
            }
        }
    }

    fn stopping(&self) -> bool {
        self.request.cancel.is_cancelled() || self.sink.is_closed()
    }

    /// Sleeps `delay`, or less when the listing is cancelled or dropped meanwhile (`false`).
    async fn wait(&self, delay: Duration) -> bool {
        tokio::select! {
            biased;
            () = self.request.cancel.cancelled() => false,
            () = self.sink.closed() => false,
            () = tokio::time::sleep(delay) => true,
        }
    }

    fn discard(&mut self) {
        self.page = ListingPage::default();
        self.held = 0;
    }

    fn has_content(&self) -> bool {
        !self.page.is_empty() || self.page.internal_hidden > 0
    }

    /// Adds one listed child; `true` once the page is full.
    fn push(&mut self, listed: CifsListedChild) -> bool {
        let directory = &self.request.path;
        match listed {
            Err(CifsUnspelled { name }) => self.page.push_failure(ChildFailure::unnamed(
                &self.namespace.identity,
                directory,
                Bytes::from(name.into_bytes()),
                FailureClass::Unsupported,
                "a CIFS name cannot be spelled as a storage path (its UTF-16 is not valid, or \
                 the path is too long)",
            )),
            // Transfer artifacts never appear in a listing (ADR-0006 C3b).
            Ok((child, _)) if is_artifact_path(child.as_str()) => {
                tracing::trace!("[CIFS] listing skips transfer artifact {}", child.as_str());
                self.page.internal_hidden = self.page.internal_hidden.saturating_add(1);
                return false;
            }
            // A child that cannot be described is reported on its own, beside its siblings.
            Ok((child, inline)) => match self.namespace.describe_listed(&child, &inline) {
                Ok(descriptor) => self.page.push_descriptor(directory, descriptor, None),
                Err(failure) => self.page.push_failure(ChildFailure::from_failure(
                    directory,
                    undescribed(&child, &failure),
                )),
            },
        }
        self.held += 1;
        self.held >= self.request.page_hint.get()
    }

    /// Hands over the page gathered so far; `false` once the listing is over (the consumer is
    /// gone, or the request was cancelled while the page waited).
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

    /// The directory was read to its end and closed: hands over what is left — one empty page
    /// when the directory is empty.
    async fn finish(&mut self) {
        if (self.has_content() || self.handed_over == 0) && !self.hand_over().await {
            self.stop().await;
        }
    }

    /// Hands over the children read so far, then ends the listing with `failure`.
    async fn fail(&mut self, failure: StorageRoleFailure) {
        if self.has_content() && !self.hand_over().await {
            return self.stop().await;
        }
        self.sink.fail(failure).await;
    }

    /// The listing is over without a failure of its own: a cancelled one ends with `Cancelled`
    /// (a signal, R10); one whose consumer is gone just ends.
    async fn stop(&self) {
        if self.request.cancel.is_cancelled() {
            self.sink.fail(cancelled(&self.request.path)).await;
        }
    }
}

/// A listed child's failure, saying that the listing could not describe it.
fn undescribed(child: &StoragePath, failure: &StorageRoleFailure) -> EntryOperationFailure {
    let (class, transience) = match failure {
        StorageRoleFailure::Entry(failure) => (failure.class(), failure.transience()),
        StorageRoleFailure::Session(failure) => (failure.class(), failure.transience()),
    };
    match entry_failure_with_transience(
        child,
        Operation::Traverse,
        class,
        transience,
        "CIFS listing could not describe the entry",
    ) {
        StorageRoleFailure::Entry(failure) => failure,
        StorageRoleFailure::Session(_) => unreachable!("an entry failure is built"),
    }
}

#[cfg(test)]
#[path = "listing_tests.rs"]
mod tests;
