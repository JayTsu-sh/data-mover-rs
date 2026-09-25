//! The listing stream's contract: pages by hint, failures end it, a dropped stream ends its
//! producer through the producer's own clean-up path, and a producer that dies is never read as
//! a finished listing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use super::*;
use crate::model::{BackendIdentity, BackendKind, IdentityStrength};

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn identity(value: &str) -> SourceIdentity {
    SourceIdentity::new(
        BackendIdentity::new(BackendKind::Local, "listing-test")
            .unwrap_or_else(|error| panic!("{error}")),
        IdentityStrength::StableWithinBackend,
        value.as_bytes(),
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

fn child(name: &str) -> ListedChild {
    ListedChild::new(name, EntryKind::File, Some(1), identity(name))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn descriptor(value: &str) -> SourceDescriptor {
    SourceDescriptor::new(path(value), EntryKind::File, Some(1), identity(value))
}

fn failure(value: &str) -> EntryOperationFailure {
    listing_failure(
        &path(value),
        FailureClass::Unsupported,
        Transience::Permanent,
        "test failure",
    )
}

fn names(page: &ListingPage) -> Vec<&str> {
    page.children.iter().map(ListedChild::name).collect()
}

fn hint(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("hint must be nonzero"))
}

/// Waits (bounded) until `condition` holds, so a broken producer fails the test instead of
/// hanging it.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("timed out waiting until {what}");
}

#[test]
fn a_child_path_is_the_directory_a_slash_and_the_name_verbatim() {
    assert_eq!(child_path(&StoragePath::root(), "a"), Ok(path("a")));
    assert_eq!(child_path(&path("d"), "a"), Ok(path("d/a")));
    // No normalisation of either part: the listing contract joins, nothing more.
    assert_eq!(child_path(&path("a//b"), "c"), Ok(path("a//b/c")));
    assert_eq!(child_path(&path("./t"), "d"), Ok(path("./t/d")));
}

#[test]
fn a_child_name_is_one_segment() {
    for bad in ["", ".", "..", "a/b", "a\0b"] {
        assert!(
            ListedChild::new(bad, EntryKind::File, None, identity("x")).is_err(),
            "{bad:?} must be refused as a name"
        );
    }
    assert_eq!(child("a b.txt").name(), "a b.txt");
}

/// A descriptor whose path is not the directory and one name becomes a child failure at the path
/// it carried; one that is keeps everything but its path, which it gets back when joined.
#[test]
fn descriptors_become_names_and_a_misspelled_child_fails_alone() {
    let directory = path("a//b");
    let page = ListingPage::from_descriptors(
        &directory,
        vec![
            descriptor("a//b/ok"),
            descriptor("a/b/collapsed"),
            descriptor("a//b/deeper/x"),
            descriptor("elsewhere"),
        ],
        vec![failure("a//b/bad"), failure("a//b")],
        3,
    );
    assert_eq!(names(&page), ["ok"]);
    assert_eq!(page.internal_hidden, 3);
    let failed: Vec<(Option<&str>, &str)> = page
        .failures
        .iter()
        .map(|failure| (failure.name.as_deref(), failure.failure.path().as_str()))
        .collect();
    assert_eq!(
        failed,
        [
            (Some("bad"), "a//b/bad"),
            (None, "a//b"),
            (None, "a/b/collapsed"),
            (None, "a//b/deeper/x"),
            (None, "elsewhere"),
        ]
    );
    let (descriptors, failures) = page.into_descriptors(&directory);
    assert_eq!(descriptors, [descriptor("a//b/ok")]);
    assert_eq!(failures.len(), 5);
}

/// A server's normalised spelling of a child of the requested directory is respelled from the
/// request; a path that is not such a child is left alone, so it is refused, not renamed.
#[test]
fn respelling_renames_only_children_of_the_requested_directory() {
    assert_eq!(respelled(&path("a//b"), path("a/b/x")), path("a//b/x"));
    assert_eq!(respelled(&path("./"), path("x")), path(".//x"));
    assert_eq!(respelled(&path("d/"), path("d/x")), path("d//x"));
    assert_eq!(respelled(&path("d"), path("d/x")), path("d/x"));
    assert_eq!(respelled(&path("a//b"), path("c/x")), path("c/x"));
    assert_eq!(respelled(&path("a"), path("a/b/x")), path("a/b/x"));
    assert_eq!(respelled(&path("a"), path("a/..")), path("a/.."));
    let named = ChildFailure::from_failure(&path("a//b"), failure("a/b/bad"));
    assert_eq!(named.name.as_deref(), Some("bad"));
}

#[tokio::test]
async fn a_whole_listing_is_delivered_in_pages_of_at_most_the_hint() {
    let request = ListRequest::new(path("d"), CancellationToken::new()).with_page_hint(hint(2));
    let whole = ListingPage::new(
        ["a", "b", "c", "d", "e"].into_iter().map(child).collect(),
        vec![ChildFailure::from_failure(&path("d"), failure("d/x"))],
        4,
    );
    let stream = ListingStream::from_producer(request.path.clone(), move |sink| async move {
        sink.deliver(Ok(whole), &request).await;
    });
    let pages: Vec<ListingPage> = stream
        .map(|page| page.unwrap_or_else(|error| panic!("{error}")))
        .collect()
        .await;
    let shapes: Vec<(Vec<&str>, usize, u32)> = pages
        .iter()
        .map(|page| (names(page), page.failures.len(), page.internal_hidden))
        .collect();
    assert_eq!(
        shapes,
        [
            (vec!["a", "b"], 1, 4),
            (vec!["c", "d"], 0, 0),
            (vec!["e"], 0, 0)
        ]
    );
}

#[tokio::test]
async fn an_empty_directory_is_one_empty_page_and_a_normal_end() {
    let request = ListRequest::new(path("d"), CancellationToken::new());
    let stream = ListingStream::from_producer(request.path.clone(), move |sink| async move {
        sink.deliver(Ok(ListingPage::default()), &request).await;
    });
    let collected = collect_listing(stream, CollectLimits::UNBOUNDED).await;
    assert_eq!(collected, Ok(ListingPage::default()));
}

#[tokio::test]
async fn collecting_joins_every_page_and_sums_the_hidden_count() {
    let stream = ListingStream::from_pages(
        path("d"),
        [
            Ok(ListingPage::new(vec![child("a")], Vec::new(), 1)),
            Ok(ListingPage::new(
                vec![child("b"), child("c")],
                vec![ChildFailure::from_failure(&path("d"), failure("d/x"))],
                2,
            )),
        ],
    );
    let page = collect_listing(stream, CollectLimits::UNBOUNDED)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(names(&page), ["a", "b", "c"]);
    assert_eq!(page.failures.len(), 1);
    assert_eq!(page.internal_hidden, 3);
}

#[tokio::test]
async fn collecting_past_the_limit_is_a_capacity_failure_at_the_directory() {
    let stream = ListingStream::from_pages(
        path("d"),
        [
            Ok(ListingPage::new(
                vec![child("a"), child("b")],
                Vec::new(),
                0,
            )),
            Ok(ListingPage::new(vec![child("c")], Vec::new(), 0)),
        ],
    );
    let collected = collect_listing(stream, CollectLimits::at_most(hint(2))).await;
    assert!(
        matches!(collected, Err(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Capacity && failure.path() == &path("d")),
        "{collected:?}"
    );
}

#[tokio::test]
async fn nothing_follows_a_failure() {
    let stream = ListingStream::from_pages(
        path("d"),
        [
            Ok(ListingPage::new(vec![child("a")], Vec::new(), 0)),
            Err(StorageRoleFailure::Entry(failure("d"))),
            Ok(ListingPage::new(vec![child("b")], Vec::new(), 0)),
        ],
    );
    let items: Vec<bool> = stream.map(|item| item.is_ok()).collect().await;
    assert_eq!(items, [true, false]);

    let sent_after_failure = Arc::new(AtomicBool::new(false));
    let recorded = Arc::clone(&sent_after_failure);
    let stream = ListingStream::from_producer(path("d"), move |sink| async move {
        sink.fail(StorageRoleFailure::Entry(failure("d"))).await;
        let late = sink.send(ListingPage::new(vec![child("b")], Vec::new(), 0));
        recorded.store(late.await.is_ok(), Ordering::SeqCst);
    });
    let items: Vec<bool> = stream.map(|item| item.is_ok()).collect().await;
    assert_eq!(items, [false]);
    assert!(!sent_after_failure.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_cancelled_request_ends_with_a_cancelled_failure() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let request = ListRequest::new(path("d"), cancel);
    let stream = ListingStream::from_producer(request.path.clone(), move |sink| async move {
        let whole = ListingPage::new(vec![child("a")], Vec::new(), 0);
        sink.deliver(Ok(whole), &request).await;
    });
    let collected = collect_listing(stream, CollectLimits::UNBOUNDED).await;
    assert!(
        matches!(collected, Err(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Cancelled),
        "{collected:?}"
    );
}

/// A resource a producer holds between pages, standing in for a CIFS directory handle.
#[derive(Default)]
struct Handles {
    open: AtomicUsize,
    finished: AtomicBool,
}

/// Opens a handle, sends pages until the consumer is gone, and closes the handle on the way out,
/// as every backend producer must.
fn paging_producer(handles: Arc<Handles>) -> ListingStream {
    ListingStream::from_producer(path("d"), move |sink| async move {
        handles.open.fetch_add(1, Ordering::SeqCst);
        for index in 0.. {
            let page = ListingPage::new(vec![child(&format!("c{index}"))], Vec::new(), 0);
            if sink.send(page).await.is_err() {
                break;
            }
        }
        // The close path: only reached because the producer is never aborted from outside.
        handles.open.fetch_sub(1, Ordering::SeqCst);
        handles.finished.store(true, Ordering::SeqCst);
    })
}

/// Fault injection for the no-leak contract: dropping the stream mid-listing must let the
/// producer run its own close path. A stream that aborted its producer on drop would leave the
/// handle open and fail this test.
#[tokio::test]
async fn dropping_the_stream_mid_listing_ends_the_producer_through_its_close_path() {
    let handles = Arc::new(Handles::default());
    let mut stream = paging_producer(Arc::clone(&handles));
    let first = stream.next().await;
    assert!(matches!(first, Some(Ok(ref page)) if names(page) == ["c0"]));
    assert_eq!(handles.open.load(Ordering::SeqCst), 1);
    drop(stream);
    eventually("the producer closed its handle", || {
        handles.finished.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(handles.open.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_stream_dropped_unpolled_never_starts_its_producer() {
    let handles = Arc::new(Handles::default());
    drop(paging_producer(Arc::clone(&handles)));
    tokio::task::yield_now().await;
    assert_eq!(handles.open.load(Ordering::SeqCst), 0);
    assert!(!handles.finished.load(Ordering::SeqCst));
}

/// A producer that dies without finishing must not read as a complete listing.
#[tokio::test]
async fn a_producer_that_panics_ends_the_stream_with_an_internal_failure() {
    let stream = ListingStream::from_producer(path("d"), |sink| async move {
        let _ = sink
            .send(ListingPage::new(vec![child("a")], Vec::new(), 0))
            .await;
        panic!("producer failure injected by the test");
    });
    let items: Vec<Result<ListingPage, StorageRoleFailure>> = stream.collect().await;
    assert_eq!(items.len(), 2, "{items:?}");
    assert!(items[0].is_ok());
    assert!(
        matches!(items[1], Err(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Internal),
        "{items:?}"
    );
}

/// A stateless listing abandons the request in flight at cancellation, and when its stream is
/// dropped.
#[tokio::test]
async fn an_abortable_listing_stops_at_cancellation_and_at_drop() {
    let never = Arc::new(Notify::new());
    let cancel = CancellationToken::new();
    let request = ListRequest::new(path("d"), cancel.clone());
    let pending = Arc::clone(&never);
    let stream = ListingStream::from_producer(request.path.clone(), move |sink| async move {
        let listing = async move {
            pending.notified().await;
            Ok(ListingPage::default())
        };
        sink.deliver_abortable(listing, &request).await;
    });
    cancel.cancel();
    let collected = collect_listing(stream, CollectLimits::UNBOUNDED).await;
    assert!(
        matches!(collected, Err(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Cancelled),
        "{collected:?}"
    );

    let finished = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&finished);
    let request = ListRequest::new(path("d"), CancellationToken::new());
    let pending = Arc::clone(&never);
    let mut stream = ListingStream::from_producer(request.path.clone(), move |sink| async move {
        let listing = async move {
            pending.notified().await;
            Ok(ListingPage::default())
        };
        sink.deliver_abortable(listing, &request).await;
        done.store(true, Ordering::SeqCst);
    });
    // Start the producer, then walk away while its request is in flight.
    let polled = futures::poll!(stream.next());
    assert!(polled.is_pending());
    drop(stream);
    eventually("the abortable producer returned", || {
        finished.load(Ordering::SeqCst)
    })
    .await;
}
