//! The Local listing streams (P7 C5): the first page is handed over before the directory is read
//! to its end, a dropped or cancelled listing stops reading within a page or two, and a directory
//! read that fails after the first page ends the listing with that failure, after the children
//! read before it.
//!
//! "Read" is counted by the describe seam, which runs once per child between its directory read
//! and its `lstat`. Fault injection: a producer that reads the whole directory before its first
//! page, or one that keeps reading after its consumer is gone, turns the first two red; one that
//! turns a failed read into a child failure (the stream reports no more entries after an error,
//! so the listing would end "complete") turns the third red.

use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::LocalNamespace;
use crate::model::{FailureClass, StoragePath};
use crate::storage::backends::local::test_identity;
use crate::storage::delete_tree::delete_tree_with_namespace;
use crate::storage::{DeleteTreeItem, DeleteTreeRequest};
use crate::storage::{ListRequest, ListingPage, ListingStream, Namespace as _, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const FILES: usize = 1000;
const HINT: usize = 10;
/// Most children a listing may have read once its consumer holds one page: that page, one in
/// the channel, and the one the producer holds while it waits for room.
const READ_AHEAD: usize = 3 * HINT;

/// A directory of `files` empty files.
fn wide(files: usize) -> Result<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    for index in 0..files {
        std::fs::write(root.path().join(format!("f{index:05}")), b"")?;
    }
    Ok(root)
}

/// The namespace of `root`, and how many children its listings have read.
fn namespace(root: &Path) -> Result<(LocalNamespace, Arc<AtomicUsize>)> {
    let dir = Dir::open_ambient_dir(std::fs::canonicalize(root)?, ambient_authority())?;
    let namespace = LocalNamespace::from_root(Arc::new(dir), test_identity("local-streaming"));
    let read = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&read);
    namespace.before_describe(Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    Ok((namespace, read))
}

fn listing(namespace: &LocalNamespace, cancel: CancellationToken) -> Result<ListingStream> {
    let hint = NonZeroUsize::new(HINT).ok_or("the page hint is not zero")?;
    let request = ListRequest::new(StoragePath::root(), cancel).with_page_hint(hint);
    Ok(namespace.list(request))
}

/// How many children were read once the producer has stopped: the count holds still.
async fn settled(read: &AtomicUsize) -> usize {
    let mut last = read.load(Ordering::SeqCst);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = read.load(Ordering::SeqCst);
        if now == last {
            return now;
        }
        last = now;
    }
}

/// The next page's child count, failing on anything but a page.
async fn next_page(stream: &mut ListingStream) -> Result<usize> {
    match stream.next().await {
        Some(Ok(page)) => Ok(page.children.len() + page.failures.len()),
        other => Err(format!("expected a page, got {other:?}").into()),
    }
}

#[tokio::test]
async fn the_first_page_arrives_before_the_directory_is_read_to_its_end() -> Result {
    let root = wide(FILES)?;
    let (namespace, read) = namespace(root.path())?;
    let mut stream = listing(&namespace, CancellationToken::new())?;

    assert_eq!(next_page(&mut stream).await?, HINT);
    let ahead = settled(&read).await;
    assert!(ahead <= READ_AHEAD, "read {ahead} children for one page");

    let mut total = HINT;
    let mut pages = 1;
    while let Some(page) = stream.next().await {
        let page = page.map_err(|failure| format!("{failure:?}"))?;
        assert!(page.children.len() <= HINT);
        total += page.children.len();
        pages += 1;
    }
    assert_eq!((total, read.load(Ordering::SeqCst)), (FILES, FILES));
    assert!(pages >= FILES / HINT, "{pages} pages");
    Ok(())
}

#[tokio::test]
async fn dropping_the_stream_stops_the_read() -> Result {
    let root = wide(FILES)?;
    let (namespace, read) = namespace(root.path())?;
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    drop(stream);
    let total = settled(&read).await;
    assert!(
        total <= READ_AHEAD,
        "a dropped listing read {total} children"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_the_request_stops_the_read_with_a_cancelled_signal() -> Result {
    let root = wide(FILES)?;
    let (namespace, read) = namespace(root.path())?;
    let cancel = CancellationToken::new();
    let mut stream = listing(&namespace, cancel.clone())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    cancel.cancel();
    let mut last = None;
    let mut pages = 0;
    while let Some(item) = stream.next().await {
        pages += usize::from(item.is_ok());
        last = Some(item);
    }
    assert!(pages <= 1, "{pages} pages after cancellation");
    assert!(matches!(
        last,
        Some(Err(StorageRoleFailure::Entry(failure))) if failure.class() == FailureClass::Cancelled
    ));
    let total = settled(&read).await;
    assert!(
        total <= READ_AHEAD,
        "a cancelled listing read {total} children"
    );
    Ok(())
}

/// The root's directory reads fail at entry `at`.
fn failing_at(namespace: &LocalNamespace, at: usize) {
    namespace.failing_directory_reads(Arc::new(move |native: &Path, read: usize| {
        (native.as_os_str().is_empty() && read == at).then(|| io::Error::other("injected EIO"))
    }));
}

#[tokio::test]
async fn a_read_failing_after_the_first_page_ends_the_listing_after_what_was_read() -> Result {
    let root = wide(FILES)?;
    let (namespace, _) = namespace(root.path())?;
    failing_at(&namespace, 15);
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    assert_eq!(next_page(&mut stream).await?, 5);
    assert!(matches!(
        stream.next().await,
        Some(Err(StorageRoleFailure::Entry(failure)))
            if failure.path().as_str().is_empty() && failure.class() == FailureClass::Protocol
    ));
    assert!(stream.next().await.is_none());
    Ok(())
}

/// A read failing before any child is the listing's failure alone: no empty page precedes it,
/// so the traversal reads it as unreadable, not as interrupted.
#[tokio::test]
async fn a_read_failing_before_any_child_is_the_first_item() -> Result {
    let root = wide(3)?;
    let (namespace, _) = namespace(root.path())?;
    failing_at(&namespace, 0);
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert!(matches!(
        stream.next().await,
        Some(Err(StorageRoleFailure::Entry(_)))
    ));
    assert!(stream.next().await.is_none());
    Ok(())
}

/// An empty directory is one empty page and a normal end; a directory of only transfer
/// artifacts is one page counting them.
#[tokio::test]
async fn an_empty_directory_is_one_empty_page() -> Result {
    let root = wide(0)?;
    let (namespace, _) = namespace(root.path())?;
    let pages: Vec<_> = listing(&namespace, CancellationToken::new())?
        .collect()
        .await;
    assert!(matches!(pages.as_slice(), [Ok(page)] if page.is_empty() && page.internal_hidden == 0));

    for index in 0..=(2 * HINT) {
        std::fs::write(root.path().join(format!(".data-mover-{index}.stage")), b"")?;
    }
    let pages: Vec<_> = listing(&namespace, CancellationToken::new())?
        .collect()
        .await;
    let hidden: u32 = pages
        .iter()
        .map(|page| page.as_ref().map_or(0, |page| page.internal_hidden))
        .sum();
    assert_eq!((pages.len(), hidden), (1, 21));
    assert!(
        pages
            .iter()
            .all(|page| page.as_ref().is_ok_and(ListingPage::is_empty))
    );
    Ok(())
}

/// A lost network mount met mid-listing is the session's failure, after the pages already read.
#[tokio::test]
async fn a_lost_mount_mid_listing_is_a_session_failure() -> Result {
    let root = wide(FILES)?;
    let (namespace, _) = namespace(root.path())?;
    namespace.failing_directory_reads(Arc::new(|native: &Path, read: usize| {
        (native.as_os_str().is_empty() && read == 12)
            .then(|| io::Error::from(io::ErrorKind::NotConnected))
    }));
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    assert_eq!(next_page(&mut stream).await?, 2);
    assert!(matches!(
        stream.next().await,
        Some(Err(StorageRoleFailure::Session(_)))
    ));
    Ok(())
}

/// Whether this process holds a descriptor open on `directory`.
#[cfg(target_os = "linux")]
fn holds_descriptor_on(directory: &Path) -> Result<bool> {
    for descriptor in std::fs::read_dir("/proc/self/fd")? {
        if std::fs::read_link(descriptor?.path()).is_ok_and(|target| target == directory) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A listing cancelled while its consumer is not reading closes its directory at once, before
/// its `Cancelled` failure gets room in the channel. Fault injection: report the cancellation
/// while still holding the directory stream, and the descriptor stays open until the consumer
/// reads again.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_listing_cancelled_while_parked_closes_its_directory_at_once() -> Result {
    // A subdirectory: the namespace's root capability holds a descriptor on the root itself.
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("sub"))?;
    for index in 0..FILES {
        std::fs::write(root.path().join(format!("sub/f{index:05}")), b"")?;
    }
    let directory = std::fs::canonicalize(root.path().join("sub"))?;
    let (namespace, read) = namespace(root.path())?;
    let cancel = CancellationToken::new();
    let hint = NonZeroUsize::new(HINT).ok_or("the page hint is not zero")?;
    let request = ListRequest::new(StoragePath::new("sub")?, cancel.clone()).with_page_hint(hint);
    let mut stream = namespace.list(request);
    assert_eq!(next_page(&mut stream).await?, HINT);
    settled(&read).await;
    assert!(
        holds_descriptor_on(&directory)?,
        "a parked listing holds its directory"
    );

    cancel.cancel();
    let mut closed = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !holds_descriptor_on(&directory)? {
            closed = true;
            break;
        }
    }
    assert!(closed, "the cancelled listing still holds its directory");
    let rest: Vec<_> = stream.collect().await;
    assert!(matches!(
        rest.last(),
        Some(Err(StorageRoleFailure::Entry(failure))) if failure.class() == FailureClass::Cancelled
    ));
    Ok(())
}

/// Recursive delete collects each directory whole: a directory read failing midway fails that
/// directory's listing, so none of its children is deleted and the directory is kept (before C5 a
/// failed read was a child failure beside what had been read, and those children were deleted).
#[tokio::test]
async fn delete_tree_keeps_a_directory_whose_read_fails_midway() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("top"))?;
    for index in 0..5 {
        std::fs::write(root.path().join(format!("top/f{index}")), b"x")?;
    }
    let (namespace, _) = namespace(root.path())?;
    namespace.failing_directory_reads(Arc::new(|native: &Path, read: usize| {
        (native == Path::new("top") && read == 3).then(|| io::Error::other("injected EIO"))
    }));
    let request = DeleteTreeRequest {
        root: StoragePath::new("top")?,
        delete_root: true,
        max_inflight_operations: NonZeroUsize::MIN,
        max_buffered_items: NonZeroUsize::MIN,
        cancel: CancellationToken::new(),
    };
    let mut session = delete_tree_with_namespace(Arc::new(namespace), request);
    let mut failures = Vec::new();
    while let Some(item) = session.next_item().await {
        if let DeleteTreeItem::EntryFailure(failure) = item {
            failures.push((failure.path().as_str().to_owned(), failure.class()));
        }
    }
    session
        .finish()
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    // The listing's failure, then the directory kept because its children were not deleted.
    assert_eq!(
        failures.first(),
        Some(&("top".to_owned(), FailureClass::Protocol))
    );
    assert!(
        failures.iter().all(|(path, _)| path == "top"),
        "{failures:?}"
    );
    assert_eq!(std::fs::read_dir(root.path().join("top"))?.count(), 5);
    Ok(())
}
