//! Streaming directory listings: the one listing entry of the [`Namespace`](super::Namespace)
//! role (P7 §5.1, §5.4.2–§5.4.4).
//!
//! A listing is a [`ListingStream`] of [`ListingPage`]s. The stream ending normally means the
//! directory was listed to its end; an `Err` item ends it early, and nothing follows it.
//!
//! **Dropping the stream at any point leaks nothing on the server.** A backend produces its pages
//! from one producer task per listing ([`ListingStream::from_producer`]) and hands them over a
//! channel of capacity one, so a full channel stops the producer from reading further (one page
//! read ahead). When the consumer drops the stream, the producer's next [`PageSink::send`] fails
//! and the producer leaves through its own clean-up path — a CIFS directory handle is closed
//! there, never abandoned. The task is never aborted from outside, which is what makes that hold.
//!
//! **Pages carry names, not paths.** A child's full path is, by contract, the requested path, a
//! `/`, and the child's name ([`child_path`]). A backend whose own spelling of a child differs
//! reports that child as a [`ChildFailure`] rather than hand out a name that does not join back
//! to what it listed ([`ListingPage::from_descriptors`] enforces it).
//!
//! Single-directory callers (recursive delete, the NDX walk) collect a whole listing with
//! [`collect_listing`].
//!
//! **Where each backend stands (P7 C1).** Every backend still reads the whole directory first and
//! then hands it over in pages, so backpressure and cancellation only act between pages: a
//! dropped or cancelled listing of a large directory is still read to its end in the background
//! (S3 excepted, which abandons its stateless requests at once). True paging comes per backend
//! in C5–C9. A producer detached from a runtime that shuts down mid-listing is dropped with the
//! runtime, as every other detached task is.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt as _, Stream, StreamExt as _, future, stream};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{InlineOwnership, ListingFacts, SourceDescriptor, StorageRoleFailure};
pub use failure::{ChildFailure, is_vanished};
pub use names::child_path;
pub(crate) use names::respelled;
use names::{child_name, is_segment};

use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass, ModelValueError,
    Operation, SourceIdentity, SourceVersion, StoragePath, TimestampMetadata, Transience,
};

/// Children per page when the caller has no reason to choose: about one native page of the
/// larger protocols (S3 returns up to 1000 keys, HDFS 1000 statuses).
pub const DEFAULT_PAGE_HINT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1023);

/// One listing of a directory's direct children.
#[derive(Clone, Debug)]
pub struct ListRequest {
    /// The directory to list, relative to the storage root; the empty path is the root.
    pub path: StoragePath,
    /// Stops paging as soon as possible. A stateless listing (S3) abandons the request in flight;
    /// one that holds a handle (CIFS) finishes the request in flight and closes the handle first.
    /// A cancelled listing ends with a `Cancelled` entry failure.
    pub cancel: CancellationToken,
    /// Most children one page may hold. A backend may return fewer, never more.
    pub page_hint: NonZeroUsize,
}

impl ListRequest {
    /// A request for `path` with [`DEFAULT_PAGE_HINT`].
    #[must_use]
    pub const fn new(path: StoragePath, cancel: CancellationToken) -> Self {
        Self {
            path,
            cancel,
            page_hint: DEFAULT_PAGE_HINT,
        }
    }

    /// The same request with another page size bound.
    #[must_use]
    pub const fn with_page_hint(mut self, page_hint: NonZeroUsize) -> Self {
        self.page_hint = page_hint;
        self
    }
}

/// One page of a listing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListingPage {
    /// Children this page describes.
    pub children: Vec<ListedChild>,
    /// Children the listing returned but could not describe; each concerns that child alone. One
    /// whose class is `NotFound` vanished before it could be described ([`ChildFailure`]).
    pub failures: Vec<ChildFailure>,
    /// Transfer artifacts (`.data-mover-*`) this page hid. They are not children.
    pub internal_hidden: u32,
}

impl ListingPage {
    /// A page of `children` and `failures`, with `internal_hidden` artifacts hidden.
    #[must_use]
    pub const fn new(
        children: Vec<ListedChild>,
        failures: Vec<ChildFailure>,
        internal_hidden: u32,
    ) -> Self {
        Self {
            children,
            failures,
            internal_hidden,
        }
    }

    /// Whether the page holds neither a child nor a failure.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && self.failures.is_empty()
    }

    /// A page of `directory`'s children from descriptors a backend spelled with full paths.
    ///
    /// A descriptor whose path is not `directory`, `/` and one name becomes a [`ChildFailure`]
    /// at the path it carried: its name would not join back to what the backend listed.
    #[must_use]
    pub(crate) fn from_descriptors(
        directory: &StoragePath,
        descriptors: Vec<SourceDescriptor>,
        failures: Vec<EntryOperationFailure>,
        internal_hidden: u32,
    ) -> Self {
        let mut page = Self {
            children: Vec::with_capacity(descriptors.len()),
            failures: failures
                .into_iter()
                .map(|failure| ChildFailure::from_failure(directory, failure))
                .collect(),
            internal_hidden,
        };
        for descriptor in descriptors {
            page.push_descriptor(directory, descriptor);
        }
        page
    }

    /// Adds the listed child of `directory` that `descriptor` describes, or a failure at the path
    /// it carried when that path is not `directory`, `/` and one name.
    fn push_descriptor(&mut self, directory: &StoragePath, descriptor: SourceDescriptor) {
        let Some(name) = child_name(directory, &descriptor.path) else {
            self.failures.push(ChildFailure {
                name: None,
                raw_name: None,
                failure: EntryOperationFailure::new(
                    descriptor.path,
                    Operation::Traverse,
                    FailureClass::Protocol,
                    Transience::Permanent,
                    "the listing spelled a child that is not the listed directory's path and one name",
                )
                .unwrap_or_else(|_| unreachable!("the static listing diagnostic is valid")),
            });
            return;
        };
        let name = name.to_owned();
        let SourceDescriptor {
            path: _,
            kind,
            size,
            source_identity,
            backend_fact,
            content_version,
            inline_timestamps,
            inline_mode,
            inline_ownership,
            version,
            listing,
        } = descriptor;
        self.children.push(ListedChild {
            name,
            kind,
            size,
            source_identity,
            backend_fact,
            content_version,
            inline_timestamps,
            inline_mode,
            inline_ownership,
            version,
            listing,
        });
    }

    /// The children of `directory` with their full paths, and every failure of the page.
    ///
    /// A child whose joined path exceeds the storage path limit becomes an entry failure at
    /// `directory`, beside its siblings.
    #[must_use]
    pub fn into_descriptors(
        self,
        directory: &StoragePath,
    ) -> (Vec<SourceDescriptor>, Vec<EntryOperationFailure>) {
        let mut failures: Vec<EntryOperationFailure> = self
            .failures
            .into_iter()
            .map(|failure| failure.failure)
            .collect();
        let mut descriptors = Vec::with_capacity(self.children.len());
        for child in self.children {
            match child_path(directory, &child.name) {
                Ok(path) => descriptors.push(child.at(path)),
                Err(_) => failures.push(too_long(directory, &child.name)),
            }
        }
        (descriptors, failures)
    }

    /// Adds a child `self` could not describe.
    pub fn push_failure(&mut self, failure: ChildFailure) {
        self.failures.push(failure);
    }

    /// Appends `page` to this one.
    fn absorb(&mut self, page: Self) {
        if self.children.is_empty() {
            self.children = page.children;
        } else {
            self.children.extend(page.children);
        }
        self.failures.extend(page.failures);
        self.internal_hidden = self.internal_hidden.saturating_add(page.internal_hidden);
    }
}

/// One child of a listed directory: [`SourceDescriptor`] with its full path replaced by its name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedChild {
    name: String,
    /// File, directory, symlink or special; a CIFS reparse point is a file.
    pub kind: EntryKind,
    /// A file's size; `None` where a size means nothing (a directory, an S3 prefix).
    pub size: Option<u64>,
    /// The stable identity the listing reported for the child.
    pub source_identity: SourceIdentity,
    pub(crate) backend_fact: Option<Bytes>,
    pub(crate) content_version: Option<Bytes>,
    pub(crate) inline_timestamps: Option<TimestampMetadata>,
    pub(crate) inline_mode: Option<u32>,
    pub(crate) inline_ownership: Option<InlineOwnership>,
    pub(crate) version: SourceVersion,
    pub(crate) listing: ListingFacts,
}

impl ListedChild {
    /// A child named `name`, without backend-private facts.
    ///
    /// # Errors
    /// Returns an error when `name` is not one path segment: empty, `.`, `..`, or containing `/`
    /// or NUL.
    pub fn new(
        name: impl Into<String>,
        kind: EntryKind,
        size: Option<u64>,
        source_identity: SourceIdentity,
    ) -> Result<Self, ModelValueError> {
        let name = name.into();
        if !is_segment(&name) {
            return Err(ModelValueError::new(
                "listed_child_name",
                "must be one non-empty path segment other than '.' and '..'",
            ));
        }
        Ok(Self {
            name,
            kind,
            size,
            source_identity,
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            inline_ownership: None,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
        })
    }

    /// The child's name: the last segment of its path.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Timestamps the listing already returned, if any.
    #[must_use]
    pub const fn inline_timestamps(&self) -> Option<TimestampMetadata> {
        self.inline_timestamps
    }

    /// Permission bits the listing could already derive, if any (display-grade, see
    /// [`SourceDescriptor::inline_mode`]).
    #[must_use]
    pub const fn inline_mode(&self) -> Option<u32> {
        self.inline_mode
    }

    /// The descriptor of this child of `directory`, its path being [`child_path`].
    ///
    /// # Errors
    /// Returns an error when the joined path exceeds the storage path limit.
    pub fn into_descriptor(
        self,
        directory: &StoragePath,
    ) -> Result<SourceDescriptor, ModelValueError> {
        let path = child_path(directory, &self.name)?;
        Ok(self.at(path))
    }

    /// The descriptor of this child at `path`.
    fn at(self, path: StoragePath) -> SourceDescriptor {
        SourceDescriptor {
            path,
            kind: self.kind,
            size: self.size,
            source_identity: self.source_identity,
            backend_fact: self.backend_fact,
            content_version: self.content_version,
            inline_timestamps: self.inline_timestamps,
            inline_mode: self.inline_mode,
            inline_ownership: self.inline_ownership,
            version: self.version,
            listing: self.listing,
        }
    }
}

type Item = Result<ListingPage, StorageRoleFailure>;

/// What a producer hands the stream: an item, or the end of a listing it finished.
enum Message {
    Item(Item),
    End,
}

/// A directory listing, page by page. See the [module docs](self) for its contract.
pub struct ListingStream {
    path: StoragePath,
    inner: Pin<Box<dyn Stream<Item = Item> + Send>>,
}

impl ListingStream {
    /// A listing of `path` whose pages `producer` sends through the [`PageSink`] it is given.
    ///
    /// The producer runs as its own detached task, started when the stream is first polled (a
    /// stream dropped unpolled never touches the server). It returns once the listing is
    /// finished, has failed, or [`PageSink::send`] reports the stream dropped; on every one of
    /// those exits it releases whatever it holds. A producer that stops without returning (a
    /// panic) ends the stream with an `Internal` entry failure, never with a normal end that
    /// would read as a complete listing.
    pub fn from_producer<F, Fut>(path: StoragePath, producer: F) -> Self
    where
        F: FnOnce(PageSink) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let start: Start = Box::new(move |sink| producer(sink).boxed());
        let inner = Produced {
            path: path.clone(),
            state: ProducerState::Idle(Some(start)),
        };
        Self {
            path,
            inner: Box::pin(inner),
        }
    }

    /// A listing of `path` made of `pages`, as they are; for listings already in memory.
    pub fn from_pages(path: StoragePath, pages: impl IntoIterator<Item = Item>) -> Self {
        let pages: Vec<Item> = pages.into_iter().collect();
        Self {
            path,
            inner: Box::pin(ended_by_failure(stream::iter(pages))),
        }
    }

    /// A listing of `path` that fails at once with `failure`.
    #[must_use]
    pub fn failed(path: StoragePath, failure: StorageRoleFailure) -> Self {
        Self::from_pages(path, [Err(failure)])
    }

    /// The directory this stream lists.
    #[must_use]
    pub const fn path(&self) -> &StoragePath {
        &self.path
    }
}

impl Stream for ListingStream {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Item>> {
        self.inner.as_mut().poll_next(context)
    }
}

impl fmt::Debug for ListingStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListingStream")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// Ends `stream` after its first `Err` item, as the listing contract says.
fn ended_by_failure(
    stream: impl Stream<Item = Item> + Send + 'static,
) -> impl Stream<Item = Item> + Send + 'static {
    stream.scan(false, |failed, item| {
        let next = if *failed {
            None
        } else {
            *failed = item.is_err();
            Some(item)
        };
        future::ready(next)
    })
}

type Start = Box<dyn FnOnce(PageSink) -> BoxFuture<'static, ()> + Send>;

enum ProducerState {
    /// Not polled yet: nothing has been started.
    Idle(Option<Start>),
    Running(mpsc::Receiver<Message>),
    Finished,
}

/// The consumer side of [`ListingStream::from_producer`].
struct Produced {
    path: StoragePath,
    state: ProducerState,
}

impl Produced {
    fn start(&mut self, start: Start) -> Option<Item> {
        let Ok(runtime) = Handle::try_current() else {
            self.state = ProducerState::Finished;
            return Some(Err(no_runtime()));
        };
        let (sender, receiver) = mpsc::channel(1);
        let end = sender.clone();
        let producer = start(PageSink { sender });
        // Detached, never aborted: dropping the stream ends the producer through its own close
        // path. `End` marks a listing the producer finished rather than abandoned.
        drop(runtime.spawn(async move {
            producer.await;
            let _ = end.send(Message::End).await;
        }));
        self.state = ProducerState::Running(receiver);
        None
    }
}

impl Stream for Produced {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Item>> {
        loop {
            match &mut self.state {
                ProducerState::Idle(start) => {
                    let Some(start) = start.take() else {
                        self.state = ProducerState::Finished;
                        continue;
                    };
                    if let Some(failure) = self.start(start) {
                        return Poll::Ready(Some(failure));
                    }
                }
                ProducerState::Running(receiver) => {
                    let message = match receiver.poll_recv(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(message) => message,
                    };
                    let item = match message {
                        Some(Message::Item(item)) => item,
                        Some(Message::End) => {
                            self.state = ProducerState::Finished;
                            return Poll::Ready(None);
                        }
                        None => Err(abandoned(&self.path)),
                    };
                    if item.is_err() {
                        // Nothing follows a failure; dropping the receiver tells the producer.
                        self.state = ProducerState::Finished;
                    }
                    return Poll::Ready(Some(item));
                }
                ProducerState::Finished => return Poll::Ready(None),
            }
        }
    }
}

/// The consumer dropped the listing; the producer should release what it holds and return.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListingClosed;

impl fmt::Display for ListingClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the listing's consumer is gone")
    }
}

impl std::error::Error for ListingClosed {}

/// The producer side of [`ListingStream::from_producer`].
pub struct PageSink {
    sender: mpsc::Sender<Message>,
}

impl PageSink {
    /// Sends one page, waiting while the consumer still holds the previous one.
    ///
    /// # Errors
    /// Returns [`ListingClosed`] when the consumer dropped the stream (or took a failure and
    /// stopped); the producer must release what it holds and return.
    pub async fn send(&self, page: ListingPage) -> Result<(), ListingClosed> {
        self.sender
            .send(Message::Item(Ok(page)))
            .await
            .map_err(|_| ListingClosed)
    }

    /// Ends the listing with `failure`. Nothing the producer sends afterwards is delivered.
    pub async fn fail(&self, failure: StorageRoleFailure) {
        let _ = self.sender.send(Message::Item(Err(failure))).await;
    }

    /// Resolves once the consumer has dropped the stream, so a stateless producer can abandon
    /// the request in flight at once.
    pub async fn closed(&self) {
        self.sender.closed().await;
    }

    /// Whether the consumer has dropped the stream.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Hands over a listing a backend read whole, in pages of at most `request.page_hint`
    /// children; the first page carries the failures and the hidden-artifact count. Stops at
    /// cancellation with a `Cancelled` failure, and as soon as the consumer is gone.
    pub(crate) async fn deliver(&self, listing: Item, request: &ListRequest) {
        let ListingPage {
            children,
            failures,
            internal_hidden,
        } = match listing {
            Ok(page) => page,
            Err(failure) => return self.fail(failure).await,
        };
        let hint = request.page_hint.get();
        let mut children = children.into_iter();
        let mut page = ListingPage::new(
            children.by_ref().take(hint).collect(),
            failures,
            internal_hidden,
        );
        loop {
            if request.cancel.is_cancelled() {
                return self.fail(cancelled(&request.path)).await;
            }
            if self.send(page).await.is_err() {
                return;
            }
            let next: Vec<ListedChild> = children.by_ref().take(hint).collect();
            if next.is_empty() {
                return;
            }
            page = ListingPage::new(next, Vec::new(), 0);
        }
    }
}

impl PageSink {
    /// [`PageSink::deliver`] for a listing of stateless requests (S3): nothing stays open between
    /// them, so cancellation, or the consumer dropping the stream, abandons the request in flight
    /// at once instead of paging on in the background.
    pub(crate) async fn deliver_abortable(
        &self,
        listing: impl Future<Output = Item>,
        request: &ListRequest,
    ) {
        tokio::select! {
            biased;
            () = request.cancel.cancelled() => self.fail(cancelled(&request.path)).await,
            () = self.closed() => {}
            listing = listing => self.deliver(listing, request).await,
        }
    }
}

impl fmt::Debug for PageSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageSink")
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Bounds on what [`collect_listing`] holds in memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectLimits {
    /// Most children and child failures one collected listing may hold; `None` is unbounded.
    pub max_children: Option<NonZeroUsize>,
}

impl CollectLimits {
    /// No bound: the whole directory is held, however large.
    pub const UNBOUNDED: Self = Self { max_children: None };

    /// At most `max_children` children and child failures.
    #[must_use]
    pub const fn at_most(max_children: NonZeroUsize) -> Self {
        Self {
            max_children: Some(max_children),
        }
    }
}

/// Reads a whole listing into one page: every child, every failure, the hidden count summed.
///
/// # Errors
/// Returns the listing's own failure, or a `Capacity` entry failure at the listed directory once
/// it holds more than `limits` allows; the stream is dropped then, which ends its producer.
pub async fn collect_listing(
    mut stream: ListingStream,
    limits: CollectLimits,
) -> Result<ListingPage, StorageRoleFailure> {
    let mut whole = ListingPage::default();
    while let Some(page) = stream.next().await {
        whole.absorb(page?);
        if let Some(max) = limits.max_children
            && whole.children.len() + whole.failures.len() > max.get()
        {
            return Err(StorageRoleFailure::Entry(listing_failure(
                stream.path(),
                FailureClass::Capacity,
                Transience::Permanent,
                "the directory holds more children than this caller collects",
            )));
        }
    }
    Ok(whole)
}

/// The failure a listing ends with when its request was cancelled.
#[must_use]
pub(crate) fn cancelled(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(listing_failure(
        path,
        FailureClass::Cancelled,
        Transience::Transient,
        "the listing was cancelled",
    ))
}

/// A child of `directory` whose joined path exceeds the storage path limit, named in the
/// diagnostic when the name itself fits.
fn too_long(directory: &StoragePath, name: &str) -> EntryOperationFailure {
    EntryOperationFailure::new(
        directory.clone(),
        Operation::Traverse,
        FailureClass::Unsupported,
        Transience::Permanent,
        format!("the path of listed child {name:?} exceeds the storage path limit"),
    )
    .unwrap_or_else(|_| {
        listing_failure(
            directory,
            FailureClass::Unsupported,
            Transience::Permanent,
            "a listed child's path exceeds the storage path limit",
        )
    })
}

fn abandoned(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(listing_failure(
        path,
        FailureClass::Internal,
        Transience::Unknown,
        "the listing's producer stopped before the listing ended",
    ))
}

fn no_runtime() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Internal,
            Transience::Permanent,
            "a listing needs a Tokio runtime to run its producer",
        )
        .unwrap_or_else(|_| unreachable!("the static listing diagnostic is valid")),
    )
}

fn listing_failure(
    path: &StoragePath,
    class: FailureClass,
    transience: Transience,
    diagnostic: &'static str,
) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Traverse,
        class,
        transience,
        diagnostic,
    )
    .unwrap_or_else(|_| unreachable!("the static listing diagnostic is valid"))
}

mod failure;
mod names;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
