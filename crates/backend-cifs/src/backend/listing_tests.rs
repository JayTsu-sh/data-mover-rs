//! The CIFS role listing streams, holds its directory handle only while it reads, and closes it on
//! every exit (P7 C9).
//!
//! Against [`FakeCifs`]: "read" counts records taken out of directory streams, "open now" the
//! handles not yet closed. Fault injection (see the commit): a producer that reads the whole
//! directory before its first page, keeps reading after its consumer is gone, skips the close on
//! one of its exits, reopens after its first page, or reads on after a failed record turns these
//! red.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::super::listing_fake::{FakeCifs, Fault, MakeError};
use super::{REOPEN_DELAYS, read_whole};
use crate::model::{EntryKind, FailureClass, StoragePath};
use crate::storage::{ListRequest, ListingPage, ListingStream, Namespace as _, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const FILES: usize = 100;
const HINT: usize = 10;
/// Most records a listing may have taken while its consumer holds one page: that page, one in the
/// channel, and the one the producer waits to hand over.
const READ_AHEAD: usize = 3 * HINT;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn listing(fake: &Arc<FakeCifs>, directory: &str, cancel: CancellationToken) -> ListingStream {
    let hint = NonZeroUsize::new(HINT).unwrap_or(NonZeroUsize::MIN);
    fake.namespace()
        .list(ListRequest::new(path(directory), cancel).with_page_hint(hint))
}

async fn next_page(stream: &mut ListingStream) -> Result<ListingPage> {
    match tokio::time::timeout(Duration::from_secs(5), stream.next()).await? {
        Some(Ok(page)) => Ok(page),
        other => Err(format!("expected a page, got {other:?}").into()),
    }
}

async fn drain(mut stream: ListingStream) -> (Vec<ListingPage>, Option<StorageRoleFailure>) {
    let mut pages = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(page) => pages.push(page),
            Err(failure) => return (pages, Some(failure)),
        }
    }
    (pages, None)
}

fn names(pages: &[ListingPage]) -> Vec<String> {
    pages
        .iter()
        .flat_map(|page| page.children.iter().map(|child| child.name().to_owned()))
        .collect()
}

fn expected_names(files: usize) -> Vec<String> {
    (0..files).map(|index| format!("f{index:05}")).collect()
}

fn class(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    }
}

fn fault(directory: &str, after: usize, error: MakeError, times: usize) -> Fault {
    Fault {
        path: directory.to_owned(),
        after,
        error,
        times,
    }
}

fn file_closed() -> smb_domain::Error {
    smb_domain::Error::UnexpectedMessageStatus(0xC000_0128)
}

fn access_denied() -> smb_domain::Error {
    smb_domain::Error::UnexpectedMessageStatus(0xC000_0022)
}

/// Waits until the producer holds no handle any more; the listing's records taken by then.
async fn closed(fake: &FakeCifs) -> Result<usize> {
    let counts = &fake.counts;
    tokio::time::timeout(Duration::from_secs(5), async {
        while FakeCifs::count(&counts.open_now) != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| "the directory handle was never closed")?;
    assert_eq!(
        FakeCifs::count(&counts.opens),
        FakeCifs::count(&counts.closes)
    );
    Ok(FakeCifs::count(&counts.read))
}

/// How many records were taken once the producer has stopped: the count holds still.
async fn settled(fake: &FakeCifs) -> usize {
    let mut last = FakeCifs::count(&fake.counts.read);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = FakeCifs::count(&fake.counts.read);
        if now == last {
            return now;
        }
        last = now;
    }
}

/// The first page is handed over while the rest of the directory has not been read, and the
/// handle is closed once the directory has been read to its end.
#[tokio::test]
async fn the_first_page_arrives_before_the_directory_is_read_to_its_end() -> Result {
    let gate = Arc::new(Semaphore::new(HINT));
    let fake = Arc::new(FakeCifs::new().flat("d", FILES).gated(Arc::clone(&gate)));
    let mut stream = listing(&fake, "d", CancellationToken::new());
    assert_eq!(next_page(&mut stream).await?.children.len(), HINT);
    assert_eq!(FakeCifs::count(&fake.counts.read), HINT);
    gate.add_permits(FILES);
    let (pages, failure) = drain(stream).await;
    assert!(failure.is_none(), "{failure:?}");
    assert!(pages.iter().all(|page| page.children.len() <= HINT));
    assert_eq!(names(&pages).len(), FILES - HINT);
    assert_eq!(closed(&fake).await?, FILES);
    Ok(())
}

/// With every record available, the producer runs no further ahead than a page or two, and a
/// listing waiting for its consumer — parked — holds its handle.
#[tokio::test]
async fn a_parked_listing_reads_no_further_ahead_and_holds_its_handle() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    let mut stream = listing(&fake, "d", CancellationToken::new());
    assert_eq!(next_page(&mut stream).await?.children.len(), HINT);
    let ahead = settled(&fake).await;
    assert!(
        ahead <= READ_AHEAD,
        "read {ahead} ahead of a {HINT}-child page"
    );
    assert_eq!(
        FakeCifs::count(&fake.counts.open_now),
        1,
        "parked on its handle"
    );
    let (pages, failure) = drain(stream).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages).len(), FILES - HINT);
    assert_eq!(closed(&fake).await?, FILES);
    Ok(())
}

/// Dropping the stream stops the reading within a page and closes the handle.
#[tokio::test]
async fn dropping_the_stream_stops_reading_and_closes_the_handle() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    let mut stream = listing(&fake, "d", CancellationToken::new());
    next_page(&mut stream).await?;
    drop(stream);
    let read = closed(&fake).await?;
    assert!(read <= READ_AHEAD, "read {read} after the consumer left");
    assert_eq!(
        settled(&fake).await,
        read,
        "reading went on after the close"
    );
    Ok(())
}

/// Cancelling stops the reading with a `Cancelled` signal, and closes the handle.
#[tokio::test]
async fn cancelling_stops_reading_with_a_cancelled_signal() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    let cancel = CancellationToken::new();
    let mut stream = listing(&fake, "d", cancel.clone());
    next_page(&mut stream).await?;
    cancel.cancel();
    let (_, failure) = drain(stream).await;
    let failure = failure.ok_or("a cancelled listing ends with a signal")?;
    assert_eq!(class(&failure), FailureClass::Cancelled);
    assert!(closed(&fake).await? <= READ_AHEAD);
    Ok(())
}

/// After the first page nothing is retried, even a lost handle: what was read is handed over, the
/// listing ends with the failure, and the directory was opened once.
#[tokio::test]
async fn a_failure_after_the_first_page_ends_the_listing_after_what_was_read() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 25, file_closed, 1));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert_eq!(names(&pages), expected_names(25));
    let failure = failure.ok_or("the listing failed")?;
    assert!(matches!(failure, StorageRoleFailure::Entry(_)));
    assert_eq!(class(&failure), FailureClass::Protocol);
    assert_eq!(FakeCifs::count(&fake.counts.opens), 1);
    assert_eq!(closed(&fake).await?, 26, "the failed record ends the read");
    Ok(())
}

/// The first failed record ends the read: smb-rs's stream would go on with another request.
#[tokio::test]
async fn the_read_stops_at_the_first_failed_record() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 3, access_denied, 1));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert_eq!(
        names(&pages),
        expected_names(3),
        "read so far, then the failure"
    );
    assert_eq!(
        class(&failure.ok_or("the listing failed")?),
        FailureClass::PermissionDenied
    );
    assert_eq!(closed(&fake).await?, 4);
    assert_eq!(FakeCifs::count(&fake.counts.opens), 1, "not retried");
    Ok(())
}

/// Before the first page, a lost handle is cured by opening the directory again and reading it
/// from its start: every child is handed over exactly once.
#[tokio::test(start_paused = true)]
async fn a_lost_handle_before_the_first_page_reopens_without_repeating_a_child() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 5, file_closed, 1));
    fake.fault(fault("d", 0, || smb_domain::Error::StaleObject, 1));
    fake.open_fault(fault(
        "d",
        0,
        || smb_domain::Error::InvalidState("Resource belongs to a stale share generation".into()),
        1,
    ));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages), expected_names(FILES));
    assert_eq!(FakeCifs::count(&fake.counts.open_attempts), 4);
    assert_eq!(FakeCifs::count(&fake.counts.opens), 3);
    closed(&fake).await?;
    Ok(())
}

/// Reopening is bounded, and a failure no reopen can cure is not retried.
#[tokio::test(start_paused = true)]
async fn reopening_is_bounded_and_other_failures_are_not_retried() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 2, file_closed, 10));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert_eq!(
        names(&pages),
        expected_names(2),
        "the last attempt's children"
    );
    assert_eq!(
        class(&failure.ok_or("the listing failed")?),
        FailureClass::Protocol
    );
    assert_eq!(FakeCifs::count(&fake.counts.opens), 1 + REOPEN_DELAYS.len());
    closed(&fake).await?;

    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.open_fault(fault("d", 0, access_denied, 1));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert!(pages.is_empty());
    assert_eq!(
        class(&failure.ok_or("the listing failed")?),
        FailureClass::PermissionDenied
    );
    assert_eq!(FakeCifs::count(&fake.counts.open_attempts), 1);
    Ok(())
}

/// A close that fails after the directory was read to its end does not fail the listing: every
/// child was read. The handle counts as closed either way.
#[tokio::test]
async fn a_failed_close_after_a_complete_read_keeps_the_listing_complete() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fail_closes();
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages), expected_names(FILES));
    closed(&fake).await?;
    Ok(())
}

/// Cancelling while waiting to reopen ends the listing with the signal at once.
#[tokio::test(start_paused = true)]
async fn cancelling_while_waiting_to_reopen_ends_the_listing() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 0, file_closed, 10));
    let cancel = CancellationToken::new();
    let mut stream = listing(&fake, "d", cancel.clone());
    let next = tokio::spawn(async move { stream.next().await.map(|item| item.map(|_| ())) });
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel.cancel();
    match next.await? {
        Some(Err(failure)) => assert_eq!(class(&failure), FailureClass::Cancelled),
        other => return Err(format!("expected the cancelled signal, got {other:?}").into()),
    }
    assert_eq!(FakeCifs::count(&fake.counts.opens), 1, "never reopened");
    closed(&fake).await?;
    Ok(())
}

/// Dropping the stream while it waits to reopen ends the listing: no second open.
#[tokio::test(start_paused = true)]
async fn dropping_the_stream_while_waiting_to_reopen_opens_nothing_more() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 0, file_closed, 10));
    let mut stream = listing(&fake, "d", CancellationToken::new());
    assert!(futures::poll!(stream.next()).is_pending());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        FakeCifs::count(&fake.counts.opens),
        1,
        "the first read failed"
    );
    drop(stream);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        FakeCifs::count(&fake.counts.open_attempts),
        1,
        "reopened after the drop"
    );
    closed(&fake).await?;
    Ok(())
}

/// A close that fails after a failed read leaves the read's failure as the outcome.
#[tokio::test]
async fn a_failed_close_after_a_failed_read_keeps_the_read_failure() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    fake.fault(fault("d", 3, access_denied, 1));
    fake.fail_closes();
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())).await;
    assert_eq!(names(&pages), expected_names(3));
    assert_eq!(
        class(&failure.ok_or("the listing failed")?),
        FailureClass::PermissionDenied
    );
    closed(&fake).await?;
    Ok(())
}

/// An empty directory is one empty page; transfer artifacts are counted, not listed, and do not
/// count against the page hint.
#[tokio::test]
async fn empty_pages_and_hidden_artifacts() -> Result {
    let fake = Arc::new(FakeCifs::new().directory("e", Vec::new()));
    let (pages, failure) = drain(listing(&fake, "e", CancellationToken::new())).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(pages, vec![ListingPage::default()]);
    closed(&fake).await?;

    let mut children: Vec<(String, EntryKind)> = (0..HINT)
        .map(|index| (format!("f{index:05}"), EntryKind::File))
        .collect();
    children.insert(1, (".data-mover-x.part".to_owned(), EntryKind::File));
    children.insert(4, (".data-mover-y.pointer".to_owned(), EntryKind::File));
    let fake = Arc::new(FakeCifs::new().directory("h", children));
    let (pages, failure) = drain(listing(&fake, "h", CancellationToken::new())).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(pages.len(), 1, "one full page of {HINT} children");
    assert_eq!(names(&pages), expected_names(HINT));
    assert_eq!(pages[0].internal_hidden, 2);
    closed(&fake).await?;
    Ok(())
}

/// A whole read (the artifact sweep of `delete`) closes its handle too, and fails with the read's
/// error before the close's.
#[tokio::test]
async fn a_whole_read_closes_its_handle() -> Result {
    let fake = Arc::new(FakeCifs::new().flat("d", FILES));
    let children = read_whole(fake.as_ref(), &path("d")).await?;
    assert_eq!(children.len(), FILES);
    closed(&fake).await?;
    fake.fault(fault("d", 7, access_denied, 1));
    fake.fail_closes();
    let error = read_whole(fake.as_ref(), &path("d"))
        .await
        .err()
        .ok_or("the read failed")?;
    assert!(error.to_string().contains("Access Denied"), "{error}");
    closed(&fake).await?;
    Ok(())
}
