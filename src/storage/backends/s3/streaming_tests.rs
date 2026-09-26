//! The S3 listing of current objects streams (P7 C6, §5.2, §5.3, §8): the first page reaches the
//! consumer before the directory is listed to its end, a dropped or cancelled listing abandons
//! its request in flight, every subdirectory is handed over once whatever order and repetition
//! the store returns prefixes in, and a throttled or transiently failed page is requested again
//! with the same continuation instead of restarting the listing or ending it.
//!
//! Nothing here assumes the store lists keys in order: `ListingKnobs::unordered` scrambles them
//! across pages (ONTAP S3 with deduplication or compression, Q22).
//!
//! Fault injection (each turns a test here red): gathering the whole directory before the first
//! page (the listing never arrives while a later request hangs, and every page is read at once);
//! dropping the subdirectory set (a prefix repeated on a later page is a second subdirectory);
//! deciding absence before the end (leading empty pages read as `NotFound`);
//! restarting after a throttled page (the first continuation is sent again) or giving up on it
//! (the listing fails); treating a looping continuation as anything but this directory's failure.

use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, FailureClass, Transience};
use crate::storage::backends::s3::namespace::S3Namespace;
use crate::storage::backends::s3::paging::ListingLimits;
use crate::storage::throttle::ThrottleBackoff;
use crate::storage::{
    CollectLimits, ListRequest, ListedChild, ListingPage, ListingStream, Namespace,
    StorageRoleFailure, collect_listing,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const HINT: usize = 10;
/// Most page requests a listing may have made once its consumer holds its first page: that one,
/// the one whose pages wait in the channel, and the one the producer holds while it waits.
const READ_AHEAD: usize = 3;
const PATIENCE: Duration = Duration::from_secs(5);
const QUICK_RESENDS: &[Duration] = &[
    Duration::from_millis(1),
    Duration::from_millis(2),
    Duration::from_millis(3),
];

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

async fn bucket(keys: impl IntoIterator<Item = String>) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for key in keys {
        let body = Bytes::from_static(b"x");
        protocol
            .put_object(&key, body.clone(), &content_md5(&body))
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    Ok(protocol)
}

/// `d/f00000` … : `count` objects in the directory `d`.
async fn wide(count: usize) -> TestResult<Arc<MemoryS3>> {
    bucket((0..count).map(|index| format!("d/f{index:05}"))).await
}

/// Backoff and resends of a few milliseconds, so a test spends no real time retrying.
fn quick(protocol: &Arc<MemoryS3>) -> S3Namespace<MemoryS3> {
    S3Namespace::new(protocol.clone(), identity()).with_limits(ListingLimits {
        throttle: ThrottleBackoff {
            first: Duration::from_millis(2),
            max_delay: Duration::from_millis(8),
            budget: Duration::from_secs(5),
        },
        resends: QUICK_RESENDS,
        ..ListingLimits::PRODUCTION
    })
}

fn listing(
    namespace: &S3Namespace<MemoryS3>,
    directory: &str,
    cancel: CancellationToken,
) -> TestResult<ListingStream> {
    let hint = NonZeroUsize::new(HINT).ok_or("the page hint is not zero")?;
    let request = ListRequest::new(path(directory), cancel).with_page_hint(hint);
    Ok(namespace.list(request))
}

async fn next_page(stream: &mut ListingStream) -> TestResult<ListingPage> {
    match timeout(PATIENCE, stream.next()).await? {
        Some(Ok(page)) => Ok(page),
        other => Err(format!("expected a page, got {other:?}").into()),
    }
}

/// Page requests made once the producer has stopped: the count holds still.
async fn settled(protocol: &MemoryS3) -> usize {
    let mut last = protocol.listing().calls.len();
    loop {
        sleep(Duration::from_millis(100)).await;
        let now = protocol.listing().calls.len();
        if now == last {
            return now;
        }
        last = now;
    }
}

/// Waits until no page request hangs any more (a dropped request leaves at once).
async fn no_request_hangs(protocol: &MemoryS3) -> TestResult {
    let hanging = Arc::clone(&protocol.listing().hanging);
    timeout(PATIENCE, async {
        while hanging.load(Ordering::SeqCst) > 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "a page request is still in flight".into())
}

fn names(page: &ListingPage) -> Vec<&str> {
    page.children.iter().map(ListedChild::name).collect()
}

/// The first page reaches the consumer while the request for the next server page is still
/// unanswered; a listing that gathers the directory first never delivers it. Dropping the stream
/// then abandons that request.
#[tokio::test]
async fn the_first_page_arrives_while_the_next_request_is_unanswered() -> TestResult {
    let protocol = wide(100).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = HINT;
        knobs.hang_from_call = Some(1);
    }
    let namespace = quick(&protocol);
    let mut stream = listing(&namespace, "d", CancellationToken::new())?;
    let first = next_page(&mut stream).await?;
    assert_eq!(names(&first).first(), Some(&"f00000"));
    assert_eq!(first.children.len(), HINT);
    // The read-ahead request is in flight, and stays so while the consumer holds its page.
    timeout(PATIENCE, async {
        while protocol.listing().hanging.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(stream);
    no_request_hangs(&protocol).await?;
    assert_eq!(protocol.listing().calls.len(), 2);
    Ok(())
}

/// Each server page is handed over in pages of at most the hint as it arrives: when the consumer
/// holds its first page, a bounded number of requests were made, not one per page of the
/// directory; read to its end, the listing has every child once.
#[tokio::test]
async fn the_first_page_arrives_before_the_directory_is_listed_to_its_end() -> TestResult {
    let protocol = wide(1000).await?;
    protocol.listing().page_size = 25;
    let namespace = quick(&protocol);
    let mut stream = listing(&namespace, "d", CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?.children.len(), HINT);
    let ahead = settled(&protocol).await;
    assert!(ahead <= READ_AHEAD, "{ahead} requests for one page");
    let mut total = HINT;
    while let Some(page) = timeout(PATIENCE, stream.next()).await? {
        let page = page.map_err(|failure| format!("{failure:?}"))?;
        assert!(!page.children.is_empty() && page.children.len() <= HINT);
        total += page.children.len();
    }
    assert_eq!((total, protocol.listing().calls.len()), (1000, 40));
    Ok(())
}

#[tokio::test]
async fn dropping_the_stream_stops_the_listing() -> TestResult {
    let protocol = wide(1000).await?;
    protocol.listing().page_size = HINT;
    let namespace = quick(&protocol);
    let mut stream = listing(&namespace, "d", CancellationToken::new())?;
    next_page(&mut stream).await?;
    drop(stream);
    let requests = settled(&protocol).await;
    assert!(
        requests <= READ_AHEAD,
        "a dropped listing made {requests} requests"
    );
    Ok(())
}

/// Cancellation ends the listing with the `Cancelled` signal (R10) — also while a page request
/// hangs, which is abandoned at once.
#[tokio::test]
async fn cancelling_abandons_the_listing_with_a_cancelled_signal() -> TestResult {
    for hang in [None, Some(1)] {
        let protocol = wide(1000).await?;
        {
            let mut knobs = protocol.listing();
            knobs.page_size = HINT;
            knobs.hang_from_call = hang;
        }
        let namespace = quick(&protocol);
        let cancel = CancellationToken::new();
        let mut stream = listing(&namespace, "d", cancel.clone())?;
        next_page(&mut stream).await?;
        cancel.cancel();
        let rest: Vec<_> = timeout(PATIENCE, stream.collect()).await?;
        assert!(
            rest.len() <= 2,
            "{hang:?}: {} items after cancelling",
            rest.len()
        );
        assert!(
            matches!(
                rest.last(),
                Some(Err(StorageRoleFailure::Entry(failure)))
                    if failure.class() == FailureClass::Cancelled
            ),
            "{hang:?}: {rest:?}"
        );
        no_request_hangs(&protocol).await?;
        let requests = settled(&protocol).await;
        assert!(requests <= READ_AHEAD, "{hang:?}: {requests} requests");
    }
    Ok(())
}

/// A store that returns common prefixes unsorted, twice on a page and again on the next page
/// hands over each subdirectory once (P7 §5.3): otherwise its whole subtree would be scanned
/// twice. A hidden artifact prefix is counted once, and a store that ignores the delimiter rolls
/// the keys of one subdirectory up into it once however many pages they span.
#[tokio::test]
async fn repeated_and_unsorted_common_prefixes_are_one_subdirectory_each() -> TestResult {
    let mut keys: Vec<String> = (0..12).map(|index| format!("d/s{index:02}/x")).collect();
    keys.extend((0..12).map(|index| format!("d/s{index:02}/y")));
    keys.extend((0..8).map(|index| format!("d/f{index}")));
    keys.push("d/.data-mover-stage/z".to_owned());
    let protocol = bucket(keys).await?;
    for flat in [false, true] {
        {
            let mut knobs = protocol.listing();
            knobs.page_size = 3;
            knobs.disordered_prefixes = true;
            knobs.ignore_delimiter = flat;
        }
        let namespace = quick(&protocol);
        let stream = listing(&namespace, "d", CancellationToken::new())?;
        let page = collect_listing(stream, CollectLimits::UNBOUNDED).await?;
        let hidden = page.internal_hidden;
        let (entries, failures) = page.into_descriptors(&path("d"));
        assert!(failures.is_empty(), "{failures:?}");
        let mut directories: Vec<&str> = entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::Directory)
            .map(|entry| entry.path.as_str())
            .collect();
        let listed = directories.len();
        directories.sort_unstable();
        directories.dedup();
        assert_eq!((listed, directories.len()), (12, 12), "flat {flat}");
        assert_eq!(entries.len(), 20, "flat {flat}");
        assert_eq!(hidden, 1, "flat {flat}");
    }
    Ok(())
}

/// An object a store repeats across a page boundary is handed over once.
#[tokio::test]
async fn an_object_repeated_across_a_page_boundary_is_listed_once() -> TestResult {
    let protocol = wide(7).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 2;
        knobs.repeat_last_object = true;
    }
    let namespace = quick(&protocol);
    let stream = listing(&namespace, "d", CancellationToken::new())?;
    let page = collect_listing(stream, CollectLimits::UNBOUNDED).await?;
    assert_eq!(
        names(&page),
        [
            "f00000", "f00001", "f00002", "f00003", "f00004", "f00005", "f00006"
        ]
    );
    Ok(())
}

/// What the role protocol maps a 503 `SlowDown` to (pinned by its own test in `role_protocol`).
fn slow_down() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Throttled,
        Transience::Transient,
        "S3 ListObjectsV2 request failed",
    )
}

/// What the role protocol maps a 5xx or a lost connection to.
fn connection_lost() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "S3 ListObjectsV2 request failed",
    )
}

/// The continuation the fake names after the object `d/<after>`, as it records it.
fn token(after: &str) -> String {
    format!("token:d/{after}")
}

/// Five objects, one per server page, whose third page request fails `times` times with
/// `failure` after two pages were handed over.
async fn failing_third_page(failure: S3ProtocolFailure, times: usize) -> TestResult<Arc<MemoryS3>> {
    let protocol = bucket(["a", "b", "c", "d", "e"].map(|name| format!("d/{name}"))).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        knobs.throttled_after = 2;
        knobs.throttled = Some((times, failure));
    }
    Ok(protocol)
}

/// A page throttled in the middle of the listing is requested again with its own continuation,
/// after a backoff, and the listing goes on from there: every child once, no failure. Restarting
/// from the first page would send no continuation again; giving up would end the listing.
#[tokio::test]
async fn a_throttled_page_mid_listing_is_requested_again_with_the_same_token() -> TestResult {
    for (failure, times) in [(slow_down(), 3), (connection_lost(), 3)] {
        let protocol = failing_third_page(failure.clone(), times).await?;
        let namespace = quick(&protocol);
        let mut stream = listing(&namespace, "d", CancellationToken::new())?;
        let mut children = Vec::new();
        while let Some(page) = timeout(PATIENCE, stream.next()).await? {
            let page = page.map_err(|failure| format!("{failure:?}"))?;
            assert!(page.failures.is_empty(), "{page:?}");
            children.extend(names(&page).into_iter().map(str::to_owned));
        }
        assert_eq!(children, ["a", "b", "c", "d", "e"], "{failure:?}");
        let expected = [
            vec![None, Some(token("a"))],
            vec![Some(token("b")); 1 + times],
            vec![Some(token("c")), Some(token("d"))],
        ]
        .concat();
        assert_eq!(protocol.listing().tokens, expected, "{failure:?}");
    }
    Ok(())
}

/// The resends start over after every page that arrives: two pages that each fail transiently as
/// often as the resends allow still list whole (the first page's resends do not use up the
/// second's).
#[tokio::test]
async fn the_resends_start_over_after_every_page() -> TestResult {
    let protocol = bucket(["a", "b", "c", "d", "e"].map(|name| format!("d/{name}"))).await?;
    let resends = QUICK_RESENDS.len();
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        // Requests: None, a, then b fails `resends` times, b, c, then d fails as often, d.
        let (b, d) = (2, 2 + resends + 2);
        knobs.failing_calls = (b..b + resends)
            .chain(d..d + resends)
            .map(|call| (call, connection_lost()))
            .collect();
    }
    let namespace = quick(&protocol);
    let stream = listing(&namespace, "d", CancellationToken::new())?;
    let page = timeout(PATIENCE, collect_listing(stream, CollectLimits::UNBOUNDED)).await??;
    assert_eq!(names(&page), ["a", "b", "c", "d", "e"]);
    assert_eq!(protocol.listing().calls.len(), 5 + 2 * resends);
    Ok(())
}

/// Cancelling while the listing backs off from a throttled page ends it at once with the
/// `Cancelled` signal: the backoff sleep is abandoned, not slept out.
#[tokio::test]
async fn cancelling_during_a_backoff_ends_the_listing_at_once() -> TestResult {
    let protocol = failing_third_page(slow_down(), usize::MAX).await?;
    // Backoff that would outlast the test's patience many times over.
    let namespace = S3Namespace::new(protocol.clone(), identity()).with_limits(ListingLimits {
        throttle: ThrottleBackoff {
            first: Duration::from_secs(60),
            max_delay: Duration::from_secs(60),
            budget: Duration::from_secs(3600),
        },
        ..ListingLimits::PRODUCTION
    });
    let cancel = CancellationToken::new();
    let mut stream = listing(&namespace, "d", cancel.clone())?;
    assert_eq!(names(&next_page(&mut stream).await?), ["a"]);
    assert_eq!(names(&next_page(&mut stream).await?), ["b"]);
    // The third page's request has been throttled: the listing now sleeps.
    timeout(PATIENCE, async {
        while protocol.listing().calls.len() < 3 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    cancel.cancel();
    let rest: Vec<_> = timeout(PATIENCE, stream.collect()).await?;
    assert!(
        matches!(
            rest.as_slice(),
            [Err(StorageRoleFailure::Entry(failure))] if failure.class() == FailureClass::Cancelled
        ),
        "{rest:?}"
    );
    assert_eq!(
        protocol.listing().calls.len(),
        3,
        "no request after the cancellation"
    );
    Ok(())
}

/// A transient failure that outlasts its resends ends the listing — after the pages already
/// handed over, with its own (session) scope; the page was requested once plus once per resend,
/// always with its own continuation.
#[tokio::test]
async fn a_transient_failure_past_its_resends_ends_the_listing_after_what_was_handed_over()
-> TestResult {
    let protocol = failing_third_page(connection_lost(), usize::MAX).await?;
    let namespace = quick(&protocol);
    let items: Vec<_> = timeout(
        PATIENCE,
        listing(&namespace, "d", CancellationToken::new())?.collect(),
    )
    .await?;
    let shape: Vec<Result<Vec<&str>, FailureClass>> = items
        .iter()
        .map(|item| match item {
            Ok(page) => Ok(names(page)),
            Err(StorageRoleFailure::Session(failure)) => Err(failure.class()),
            Err(StorageRoleFailure::Entry(failure)) => panic!("{failure:?}"),
        })
        .collect();
    assert_eq!(
        shape,
        [
            Ok(vec!["a"]),
            Ok(vec!["b"]),
            Err(FailureClass::Connectivity)
        ]
    );
    let resends = QUICK_RESENDS.len();
    let expected = [
        vec![None, Some(token("a"))],
        vec![Some(token("b")); 1 + resends],
    ]
    .concat();
    assert_eq!(protocol.listing().tokens, expected);
    Ok(())
}

/// A server whose continuation comes back after pages were handed over (a `MinIO`-style restart
/// serving the first page again) fails this directory — `Protocol` / `Permanent`, at the
/// directory — after those pages, never hands a page over twice and never fails the session.
#[tokio::test]
async fn a_looping_continuation_after_handed_over_pages_fails_only_the_directory() -> TestResult {
    for unordered in [false, true] {
        let protocol = bucket(["d/x".to_owned(), "d/y".to_owned()]).await?;
        {
            let mut knobs = protocol.listing();
            knobs.page_size = 1;
            knobs.unordered = unordered;
            knobs.restart_prefix = Some("d/".to_owned());
        }
        let namespace = quick(&protocol);
        let items: Vec<_> = timeout(
            PATIENCE,
            listing(&namespace, "d", CancellationToken::new())?.collect(),
        )
        .await?;
        assert_eq!(items.len(), 2, "unordered {unordered}: {items:?}");
        assert!(
            matches!(&items[0], Ok(page) if page.children.len() == 1),
            "unordered {unordered}: {items:?}"
        );
        assert!(
            matches!(
                &items[1],
                Err(StorageRoleFailure::Entry(failure))
                    if failure.class() == FailureClass::Protocol
                        && failure.transience() == Transience::Permanent
                        && failure.path() == &path("d")
            ),
            "unordered {unordered}: {items:?}"
        );
    }
    Ok(())
}

/// The entry limit counts what the listing keeps: an unordered store names a subdirectory's
/// prefix once per key under it, and a directory with one such subdirectory of many keys is not
/// "too large". Past the limit in kept entries, it still fails (`Capacity`).
#[tokio::test]
async fn the_entry_limit_counts_kept_entries_not_repeated_prefixes() -> TestResult {
    let protocol = bucket((0..12).map(|key| format!("d/s/k{key:02}"))).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        knobs.unordered = true;
    }
    for (max_entries, fits) in [(1, true), (0, false)] {
        let namespace = S3Namespace::new(protocol.clone(), identity()).with_limits(ListingLimits {
            max_entries,
            ..ListingLimits::PRODUCTION
        });
        let stream = listing(&namespace, "d", CancellationToken::new())?;
        let result = timeout(PATIENCE, collect_listing(stream, CollectLimits::UNBOUNDED)).await?;
        match result {
            Ok(page) if fits => assert_eq!(names(&page), ["s"]),
            Err(StorageRoleFailure::Entry(failure)) if !fits => {
                assert_eq!(failure.class(), FailureClass::Capacity);
            }
            other => panic!("max {max_entries}: {other:?}"),
        }
    }
    Ok(())
}

/// A store that keeps no order (ONTAP S3 with deduplication or compression, Q22) lists keys
/// scrambled across pages, and a subdirectory's common prefix again on every page where one of
/// its keys falls: the listing relies on no order — it continues from the opaque token, hands
/// every object over, every subdirectory once, and its first page before the rest.
#[tokio::test]
async fn an_unordered_store_lists_every_child_and_each_subdirectory_once() -> TestResult {
    let mut keys: Vec<String> = (0..20)
        .flat_map(|dir| (0..3).map(move |key| format!("d/s{dir:02}/k{key}")))
        .collect();
    keys.extend((0..30).map(|index| format!("d/f{index:02}")));
    keys.extend([
        "d/.data-mover-stage/a".to_owned(),
        "d/.data-mover-stage/b".to_owned(),
    ]);
    let protocol = bucket(keys).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 7;
        knobs.unordered = true;
    }
    let namespace = quick(&protocol);
    let stream = listing(&namespace, "d", CancellationToken::new())?;
    let page = timeout(PATIENCE, collect_listing(stream, CollectLimits::UNBOUNDED)).await??;
    let hidden = page.internal_hidden;
    let (entries, failures) = page.into_descriptors(&path("d"));
    assert!(failures.is_empty(), "{failures:?}");
    let listed = |kind: EntryKind| -> Vec<&str> {
        entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .map(|entry| entry.path.as_str())
            .collect()
    };
    let (mut files, mut directories) = (listed(EntryKind::File), listed(EntryKind::Directory));
    // Scrambled indeed: the store did not list the files in key order.
    assert!(!files.is_sorted(), "{files:?}");
    files.sort_unstable();
    directories.sort_unstable();
    let expected_files: Vec<String> = (0..30).map(|index| format!("d/f{index:02}")).collect();
    let expected_directories: Vec<String> = (0..20).map(|dir| format!("d/s{dir:02}")).collect();
    assert_eq!(files, expected_files);
    assert_eq!(directories, expected_directories);
    assert_eq!(hidden, 1);
    // The first page arrives while the second request hangs.
    {
        let mut knobs = protocol.listing();
        knobs.hang_from_call = Some(knobs.calls.len() + 1);
    }
    let mut stream = listing(&namespace, "d", CancellationToken::new())?;
    assert!(!next_page(&mut stream).await?.is_empty());
    drop(stream);
    no_request_hangs(&protocol).await?;
    Ok(())
}

/// A prefix under which nothing is stored is `NotFound` only once it is listed to its end, and
/// hands over no page before; one whose first pages are empty lists what follows. One holding
/// only its own zero-byte marker, or only hidden artifacts, is an existing directory.
#[tokio::test]
async fn absence_is_decided_at_the_end_and_before_any_page() -> TestResult {
    let protocol = bucket(["h/.data-mover-x".to_owned(), "d/e".to_owned()]).await?;
    protocol
        .put_object("m/", Bytes::new(), &content_md5(&Bytes::new()))
        .await
        .map_err(|e| format!("{e:?}"))?;
    let namespace = quick(&protocol);
    // Pages that list nothing yet claim more follow prove nothing, before content as after it.
    for (leading, unordered) in [(0, false), (3, false), (3, true)] {
        {
            let mut knobs = protocol.listing();
            knobs.leading_empty_pages = leading;
            knobs.unordered = unordered;
            knobs.calls.clear();
        }
        let items: Vec<_> = listing(&namespace, "missing", CancellationToken::new())?
            .collect()
            .await;
        assert!(
            matches!(
                items.as_slice(),
                [Err(StorageRoleFailure::Entry(failure))]
                    if failure.class() == FailureClass::NotFound
            ),
            "{leading}: {items:?}"
        );
        assert_eq!(
            protocol.listing().calls.len(),
            leading + 1,
            "listed to its end"
        );
        let found = listing(&namespace, "d", CancellationToken::new())?;
        let page = collect_listing(found, CollectLimits::UNBOUNDED).await?;
        assert_eq!(names(&page), ["e"], "{leading}");
    }
    protocol.listing().leading_empty_pages = 0;
    let marker: Vec<_> = listing(&namespace, "m", CancellationToken::new())?
        .collect()
        .await;
    assert!(
        matches!(marker.as_slice(), [Ok(page)] if page.is_empty() && page.internal_hidden == 0),
        "{marker:?}"
    );
    let hidden: Vec<_> = listing(&namespace, "h", CancellationToken::new())?
        .collect()
        .await;
    assert!(
        matches!(hidden.as_slice(), [Ok(page)] if page.is_empty() && page.internal_hidden == 1),
        "{hidden:?}"
    );
    Ok(())
}
