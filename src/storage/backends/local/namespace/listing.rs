//! The Local listing, page by page (P7 C5, §5.2).
//!
//! A listing is one producer task. Each page is one blocking call that takes the open directory
//! stream (`ReadDir`) in, reads at most `page_hint` children — `lstat` of each through the
//! directory's own descriptor, never following a symlink — and hands the stream back out with the
//! page. Between pages the listing holds one directory descriptor and no blocking thread, so a
//! listing parked behind a full channel (the consumer has not taken the previous page) costs one
//! descriptor and at most two pages, whatever the directory's size.
//!
//! The producer checks the request's cancellation token before every page and while it waits to
//! hand one over: a cancelled listing ends with a `Cancelled` failure, a signal, not an error
//! (R10). A consumer that drops the stream makes the next hand-over fail; the producer then
//! returns, and the directory stream is closed with it. Neither reads the directory to its end.
//!
//! A cancelled or dropped listing closes its directory stream before it reports anything, so a
//! listing whose consumer is not reading does not keep the descriptor while its `Cancelled`
//! failure waits for room in the channel.
//!
//! A failure of the directory read itself (`getdents`) ends the listing, after the children read
//! before it (the traversal reports `Interrupted` with that many delivered, `Unreadable` if none).
//! The stream cannot be resumed — after an error it reports no more entries — so the listing ends
//! whatever the failure's transience (`classify_io`); a lost network mount (`NotConnected`,
//! `BrokenPipe`) is a session failure, as everywhere in this namespace. A child that cannot be
//! described is that child's failure, beside its siblings; one that vanished between the read and
//! its `lstat` is reported vanished (P7 Q12).

use std::ffi::OsStr;
use std::io;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt as _;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use cap_std::fs::{Dir, DirEntry, ReadDir};

use super::hooks::Hooks;
use super::{LocalNamespace, describe, entry_failure_value, io_failure, open_directory};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_name;
use crate::storage::listing::{PageSink, cancelled, child_path};
use crate::storage::{
    ChildFailure, ListRequest, ListingPage, SourceDescriptor, StorageRoleFailure,
};

/// Where one page's read stopped.
enum PageEnd {
    /// The page is full; the rest of the directory is still to read from this stream.
    More(ReadDir),
    /// The directory was read to its end.
    End,
    /// The directory read failed; the page holds what was read before it.
    Failed(StorageRoleFailure),
}

/// One page's read: the page, where it stopped, and how many directory entries the listing has
/// read so far.
struct PageRead {
    page: ListingPage,
    end: PageEnd,
    read: usize,
}

impl LocalNamespace {
    /// Produces the listing of `request.path`, resolved to `native`, into `sink` page by page.
    pub(super) async fn produce_listing(
        self,
        request: ListRequest,
        native: PathBuf,
        sink: PageSink,
    ) {
        #[cfg(test)]
        if let Some(failure) = self.injected_list_failure(&request.path) {
            return sink.fail(failure).await;
        }
        let mut stream = None;
        let (mut read, mut sent) = (0_usize, false);
        loop {
            if request.cancel.is_cancelled() {
                drop(stream.take());
                return sink.fail(cancelled(&request.path)).await;
            }
            if sink.is_closed() {
                return;
            }
            let page = match self.read_page(stream.take(), &request, &native, read).await {
                Ok(page) => page,
                Err(failure) => return sink.fail(failure).await,
            };
            read = page.read;
            // An empty page is handed over only for an empty directory (its end, nothing sent
            // yet) or to count hidden artifacts. Whether a failed block is `Unreadable` or
            // `Interrupted` depends on the children it listed, not on the pages it received.
            let worth_sending = !page.page.is_empty()
                || page.page.internal_hidden > 0
                || (!sent && matches!(page.end, PageEnd::End));
            if worth_sending {
                match hand_over(&sink, &request, page.page).await {
                    HandOver::Sent => sent = true,
                    HandOver::Gone => return,
                    HandOver::Cancelled => {
                        drop(page.end);
                        return sink.fail(cancelled(&request.path)).await;
                    }
                }
            }
            match page.end {
                PageEnd::More(rest) => stream = Some(rest),
                PageEnd::End => return,
                PageEnd::Failed(failure) => return sink.fail(failure).await,
            }
        }
    }

    /// Reads the next page in one blocking call: opens the directory first when `stream` is
    /// `None`. Failing to open it is the listing's failure.
    async fn read_page(
        &self,
        stream: Option<ReadDir>,
        request: &ListRequest,
        native: &Path,
        read: usize,
    ) -> Result<PageRead, StorageRoleFailure> {
        let reader = PageReader {
            directory: request.path.clone(),
            native: native.to_path_buf(),
            hooks: self.hooks(),
            hint: request.page_hint,
        };
        self.blocking(&request.path, Operation::Traverse, move |root, identity| {
            let stream = match stream {
                Some(stream) => stream,
                None => reader.open(root)?,
            };
            Ok(reader.read(stream, identity, read))
        })
        .await
    }
}

/// How handing a page over ended.
enum HandOver {
    Sent,
    /// The consumer dropped the stream.
    Gone,
    /// The request was cancelled while the page waited for room.
    Cancelled,
}

/// Hands `page` over, waiting while the consumer still holds the previous one, unless the
/// request is cancelled meanwhile.
async fn hand_over(sink: &PageSink, request: &ListRequest, page: ListingPage) -> HandOver {
    tokio::select! {
        biased;
        () = request.cancel.cancelled() => HandOver::Cancelled,
        delivered = sink.send(page) => match delivered {
            Ok(()) => HandOver::Sent,
            Err(_) => HandOver::Gone,
        },
    }
}

/// What one blocking page read needs, owned so it can move onto a blocking thread.
struct PageReader {
    directory: StoragePath,
    native: PathBuf,
    hooks: Hooks,
    hint: NonZeroUsize,
}

impl PageReader {
    /// Opens the directory, one component at a time and refusing symlinks, and starts reading it.
    /// Only the directory stream's own descriptor stays open.
    fn open(&self, root: &Dir) -> Result<ReadDir, StorageRoleFailure> {
        self.hooks.before_list(&self.native);
        open_directory(root, &self.native)
            .and_then(|opened| opened.entries())
            .map_err(|error| io_failure(&self.directory, Operation::Traverse, &error))
    }

    /// Reads at most `hint` children (described or failed) from `stream`; `read` counts the
    /// directory entries read before this page, transfer artifacts included.
    fn read(&self, mut stream: ReadDir, identity: &BackendIdentity, mut read: usize) -> PageRead {
        let mut described = Vec::new();
        let mut undescribed = Vec::new();
        let mut hidden = 0_u32;
        let end = loop {
            if described.len() + undescribed.len() >= self.hint.get() {
                break PageEnd::More(stream);
            }
            if let Some(error) = self.hooks.read_failure(&self.native, read) {
                break PageEnd::Failed(io_failure(&self.directory, Operation::Traverse, &error));
            }
            let entry = match stream.next() {
                None => break PageEnd::End,
                Some(Ok(entry)) => entry,
                // The directory read itself failed; the stream reports nothing after an error.
                Some(Err(error)) => {
                    break PageEnd::Failed(io_failure(
                        &self.directory,
                        Operation::Traverse,
                        &error,
                    ));
                }
            };
            read += 1;
            let name = entry.file_name();
            if is_artifact_name(&name) {
                hidden = hidden.saturating_add(1);
                continue;
            }
            self.hooks.before_describe(&self.native, &name);
            match describe_child(identity, &self.directory, &name, &entry) {
                Ok(Ok(descriptor)) => described.push(descriptor),
                Ok(Err(failure)) => undescribed.push(failure),
                Err(session) => break PageEnd::Failed(session),
            }
        };
        let mut page =
            ListingPage::from_descriptors(&self.directory, described, Vec::new(), hidden);
        for failure in undescribed {
            page.push_failure(failure);
        }
        PageRead { page, end, read }
    }
}

/// One listed child: its descriptor, or why it cannot be described (the outer `Err` is a lost
/// session).
///
/// A child that vanished between the directory read and its `lstat` is reported vanished
/// ([`ChildFailure::vanished`]): the traversal counts it and emits nothing (P7 Q12). A name no
/// path can spell is an unnamed child failure carrying its raw bytes.
fn describe_child(
    identity: &BackendIdentity,
    directory: &StoragePath,
    name: &OsStr,
    entry: &DirEntry,
) -> Result<Result<SourceDescriptor, ChildFailure>, StorageRoleFailure> {
    let Some(path) = name
        .to_str()
        .and_then(|spelled| child_path(directory, spelled).ok())
    else {
        return Ok(Err(ChildFailure::unnamed(
            identity,
            directory,
            raw_name(name),
            FailureClass::Unsupported,
            "a Local name cannot be spelled as a storage path (not UTF-8, or too long)",
        )));
    };
    let metadata = match entry.metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let vanished = ChildFailure::vanished(directory, &path_name(&path));
            return Ok(Err(vanished.unwrap_or_else(|_| internal(directory))));
        }
        Err(error) => {
            return child_failure(directory, io_failure(&path, Operation::Traverse, &error));
        }
    };
    match describe(identity, path, &metadata, Operation::Traverse) {
        Ok(descriptor) => Ok(Ok(descriptor)),
        Err(failure) => child_failure(directory, failure),
    }
}

/// A per-child failure beside its siblings; a lost session stays the whole listing's.
fn child_failure(
    directory: &StoragePath,
    failure: StorageRoleFailure,
) -> Result<Result<SourceDescriptor, ChildFailure>, StorageRoleFailure> {
    match failure {
        StorageRoleFailure::Entry(failure) => {
            Ok(Err(ChildFailure::from_failure(directory, failure)))
        }
        session @ StorageRoleFailure::Session(_) => Err(session),
    }
}

/// The last segment of a path the listing itself spelled.
fn path_name(path: &StoragePath) -> String {
    path.as_str()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn internal(directory: &StoragePath) -> ChildFailure {
    let failure = entry_failure_value(
        directory,
        Operation::Traverse,
        FailureClass::Internal,
        Transience::Permanent,
    );
    ChildFailure::from_failure(directory, failure)
}

/// The native bytes of a name: as stored on Unix, UTF-16LE code units on Windows.
#[cfg(unix)]
fn raw_name(name: &OsStr) -> Bytes {
    Bytes::copy_from_slice(name.as_bytes())
}

#[cfg(windows)]
fn raw_name(name: &OsStr) -> Bytes {
    name.encode_wide().flat_map(u16::to_le_bytes).collect()
}

#[cfg(all(not(unix), not(windows)))]
fn raw_name(name: &OsStr) -> Bytes {
    Bytes::copy_from_slice(name.to_string_lossy().as_bytes())
}
