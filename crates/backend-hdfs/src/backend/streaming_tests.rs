//! The HDFS role listing streams (P7 C8): the first page is handed over before the directory is
//! listed to its end, a dropped or cancelled listing stops asking for pages, and a listing that
//! fails after its first page ends with that failure after the children read before it.
//!
//! "Read" counts the statuses the in-memory name node handed out. Fault injection: a producer
//! that collects the whole listing before its first page, or one that keeps paging after its
//! consumer is gone or its request is cancelled, turns these red.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::contract_tests::MemoryHdfs;
use super::namespace::HdfsNamespace;
use super::test_identity;
use crate::model::{FailureClass, StoragePath};
use crate::storage::{ListRequest, ListingStream, Namespace as _, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const FILES: usize = 1000;
const HINT: usize = 10;
/// Most statuses a listing may have read once its consumer holds one page: that page, one in
/// the channel, and the one the producer holds while it waits for room.
const READ_AHEAD: usize = 3 * HINT;

async fn wide(files: usize) -> Result<(Arc<MemoryHdfs>, HdfsNamespace)> {
    let protocol = Arc::new(MemoryHdfs::default());
    for index in 0..files {
        protocol
            .insert(&format!("f{index:05}"), Bytes::from_static(b"x"))
            .await;
    }
    let namespace = HdfsNamespace::new(Arc::clone(&protocol), test_identity("hdfs-streaming")?);
    Ok((protocol, namespace))
}

fn listing(namespace: &HdfsNamespace, cancel: CancellationToken) -> Result<ListingStream> {
    let hint = NonZeroUsize::new(HINT).ok_or("the page hint is not zero")?;
    let request = ListRequest::new(StoragePath::root(), cancel).with_page_hint(hint);
    Ok(namespace.list(request))
}

/// How many statuses were read once the producer has stopped: the count holds still.
async fn settled(protocol: &MemoryHdfs) -> usize {
    let mut last = protocol.listed_statuses();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = protocol.listed_statuses();
        if now == last {
            return now;
        }
        last = now;
    }
}

async fn next_page(stream: &mut ListingStream) -> Result<usize> {
    match stream.next().await {
        Some(Ok(page)) => Ok(page.children.len() + page.failures.len()),
        other => Err(format!("expected a page, got {other:?}").into()),
    }
}

#[tokio::test]
async fn the_first_page_arrives_before_the_directory_is_listed_to_its_end() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    let ahead = settled(&protocol).await;
    assert!(ahead <= READ_AHEAD, "read {ahead} statuses for one page");
    let mut total = HINT;
    while let Some(page) = stream.next().await {
        let page = page.map_err(|failure| format!("{failure:?}"))?;
        assert!(page.children.len() <= HINT);
        total += page.children.len();
    }
    assert_eq!((total, protocol.listed_statuses()), (FILES, FILES));
    Ok(())
}

#[tokio::test]
async fn dropping_the_stream_stops_the_listing() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    drop(stream);
    let total = settled(&protocol).await;
    assert!(
        total <= READ_AHEAD,
        "a dropped listing read {total} statuses"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_the_request_stops_the_listing_with_a_cancelled_signal() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    let cancel = CancellationToken::new();
    let mut stream = listing(&namespace, cancel.clone())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    cancel.cancel();
    let rest: Vec<_> = stream.collect().await;
    assert!(rest.len() <= 2, "{} items after cancellation", rest.len());
    assert!(matches!(
        rest.last(),
        Some(Err(StorageRoleFailure::Entry(failure))) if failure.class() == FailureClass::Cancelled
    ));
    let total = settled(&protocol).await;
    assert!(
        total <= READ_AHEAD,
        "a cancelled listing read {total} statuses"
    );
    Ok(())
}

#[tokio::test]
async fn a_failure_after_the_first_page_ends_the_listing_after_what_was_read() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    protocol.fail_listings_after(15);
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

#[tokio::test]
async fn an_empty_directory_is_one_empty_page() -> Result {
    let (_, namespace) = wide(0).await?;
    let pages: Vec<_> = listing(&namespace, CancellationToken::new())?
        .collect()
        .await;
    assert!(matches!(pages.as_slice(), [Ok(page)] if page.is_empty() && page.internal_hidden == 0));
    Ok(())
}

/// Waits until `protocol` has dropped `count` listings, for at most a few seconds.
async fn dropped(protocol: &MemoryHdfs, count: usize) -> bool {
    for _ in 0..100 {
        if protocol.listings_dropped() >= count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// A page request that never answers is abandoned at once when the request is cancelled: the
/// listing ends `Cancelled` and the protocol listing is dropped, without waiting for the name
/// node. Fault injection: stop selecting on the token while a page is requested and this hangs.
#[tokio::test]
async fn cancelling_abandons_a_page_request_in_flight() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    protocol.stall_listings_after(HINT);
    let cancel = CancellationToken::new();
    let mut stream = listing(&namespace, cancel.clone())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    cancel.cancel();
    let next = tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;
    assert!(matches!(
        next,
        Some(Err(StorageRoleFailure::Entry(failure))) if failure.class() == FailureClass::Cancelled
    ));
    assert!(
        dropped(&protocol, 1).await,
        "the stalled listing was not dropped"
    );
    Ok(())
}

/// A consumer that drops the stream while a page request is in flight makes the producer drop
/// the protocol listing at once. Fault injection: stop selecting on the closed channel and the
/// stalled request keeps the listing forever.
#[tokio::test]
async fn dropping_the_stream_abandons_a_page_request_in_flight() -> Result {
    let (protocol, namespace) = wide(FILES).await?;
    protocol.stall_listings_after(HINT);
    let mut stream = listing(&namespace, CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?, HINT);
    drop(stream);
    assert!(
        dropped(&protocol, 1).await,
        "the stalled listing was not dropped"
    );
    Ok(())
}
