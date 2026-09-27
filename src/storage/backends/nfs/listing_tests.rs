//! The NFS role listing streams, follows descent hints (P7 C7) and continues a directory from its
//! cookie after the first page (P7 C7c).
//!
//! Against [`FakeNfs`]: "read" counts children taken out of READDIRPLUS replies, "lookups" path
//! resolutions. Fault injection (see the commits): a producer that reads the whole directory
//! before its first page, keeps reading after its consumer is gone, restarts after its first page,
//! ignores the hint, or does not fall back from a stale one turns these red; so does one that does
//! not continue after its first page, hands a child over again after continuing, follows cookies
//! that loop, or continues from a cookie the server rejected.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::super::listing_fake::{Cookies, FakeNfs, Fault, locked};
use super::super::namespace::{NfsReadFailure, NfsRestart};
use super::super::source::NfsProtocolFailure;
use super::{MAX_FROM_START, MAX_RELOOKUPS, descent_hint, hinted_handle};
use crate::model::{EntryKind, FailureClass, StoragePath, Transience};
use crate::storage::{
    CollectLimits, DescentHint, ListRequest, ListingPage, ListingStream, Namespace as _,
    StorageRoleFailure, collect_listing,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const FILES: usize = 1000;
const HINT: usize = 10;
/// Entries per READDIRPLUS reply.
const REPLY: usize = 25;
/// Most children a listing may have read while its consumer holds one page: that page, one in
/// the channel, the one being gathered, and the rest of the reply it is working through.
const READ_AHEAD: usize = 3 * HINT + REPLY;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn request(directory: &str, cancel: CancellationToken) -> Result<ListRequest> {
    let hint = NonZeroUsize::new(HINT).ok_or("the page hint is not zero")?;
    Ok(ListRequest::new(path(directory), cancel).with_page_hint(hint))
}

fn listing(
    fake: &Arc<FakeNfs>,
    directory: &str,
    cancel: CancellationToken,
) -> Result<ListingStream> {
    Ok(fake.namespace().list(request(directory, cancel)?))
}

async fn next_page(stream: &mut ListingStream) -> Result<ListingPage> {
    match tokio::time::timeout(Duration::from_secs(5), stream.next()).await? {
        Some(Ok(page)) => Ok(page),
        other => Err(format!("expected a page, got {other:?}").into()),
    }
}

/// How many children were read once the producer has stopped: the count holds still.
async fn settled(fake: &FakeNfs) -> usize {
    let mut last = FakeNfs::count(&fake.read);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = FakeNfs::count(&fake.read);
        if now == last {
            return now;
        }
        last = now;
    }
}

fn names(pages: &[ListingPage]) -> Vec<String> {
    pages
        .iter()
        .flat_map(|page| page.children.iter().map(|child| child.name().to_owned()))
        .collect()
}

async fn drain(stream: ListingStream) -> (Vec<ListingPage>, Option<StorageRoleFailure>) {
    let mut pages = Vec::new();
    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(page) => pages.push(page),
            Err(failure) => return (pages, Some(failure)),
        }
    }
    (pages, None)
}

fn fault(directory: &str, after: usize, restart: NfsRestart, class: FailureClass) -> Fault {
    Fault {
        path: directory.to_owned(),
        after,
        failure: NfsReadFailure {
            failure: NfsProtocolFailure::new(class, Transience::Transient),
            restart,
        },
        times: 1,
    }
}

/// The first page is handed over while the rest of the directory has not been read: the second
/// READDIRPLUS reply is held back and the page still arrives.
#[tokio::test]
async fn the_first_page_arrives_before_the_directory_is_read_to_its_end() -> Result {
    let gate = Arc::new(Semaphore::new(1));
    let fake = Arc::new(
        FakeNfs::new(REPLY)
            .flat("d", FILES)
            .gated(Arc::clone(&gate)),
    );
    let mut stream = listing(&fake, "d", CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?.children.len(), HINT);
    assert_eq!(
        FakeNfs::count(&fake.replies),
        1,
        "one reply for the first page"
    );
    gate.add_permits(FILES);
    let mut total = HINT;
    while let Some(page) = stream.next().await {
        let page = page.map_err(|failure| format!("{failure:?}"))?;
        assert!(page.children.len() <= HINT);
        total += page.children.len();
    }
    assert_eq!((total, FakeNfs::count(&fake.read)), (FILES, FILES));
    assert_eq!(FakeNfs::count(&fake.replies), FILES / REPLY);
    Ok(())
}

/// With every reply available, the producer still runs no further ahead than a page or two.
#[tokio::test]
async fn the_listing_reads_no_further_ahead_than_a_page_or_two() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    let mut stream = listing(&fake, "d", CancellationToken::new())?;
    assert_eq!(next_page(&mut stream).await?.children.len(), HINT);
    let ahead = settled(&fake).await;
    assert!(ahead <= READ_AHEAD, "read {ahead} children for one page");
    Ok(())
}

#[tokio::test]
async fn dropping_the_stream_stops_reading() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    let mut stream = listing(&fake, "d", CancellationToken::new())?;
    next_page(&mut stream).await?;
    drop(stream);
    let total = settled(&fake).await;
    assert!(
        total <= READ_AHEAD,
        "a dropped listing read {total} children"
    );
    assert_eq!(FakeNfs::count(&fake.reads_ended), 1, "the read returned");
    Ok(())
}

#[tokio::test]
async fn cancelling_stops_reading_with_a_cancelled_signal() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    let cancel = CancellationToken::new();
    let mut stream = listing(&fake, "d", cancel.clone())?;
    next_page(&mut stream).await?;
    cancel.cancel();
    let (rest, failure) = drain(stream).await;
    assert!(rest.len() <= 2, "{} pages after cancellation", rest.len());
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::Cancelled
    ));
    let total = settled(&fake).await;
    assert!(
        total <= READ_AHEAD,
        "a cancelled listing read {total} children"
    );
    Ok(())
}

/// A failure after the first page that no retry can cure ends the listing: the children read
/// before it are handed over, then the failure, and nothing is read twice (the traversal records
/// `Interrupted`).
#[tokio::test]
async fn a_failure_after_the_first_page_ends_the_listing_after_what_was_read() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    fake.fault(fault("d", 15, NfsRestart::Never, FailureClass::Protocol));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    let sizes: Vec<usize> = pages.iter().map(|page| page.children.len()).collect();
    assert_eq!(sizes, [10, 5]);
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Protocol && failure.path() == &path("d")
    ));
    assert_eq!(FakeNfs::count(&fake.reads_started), 1, "not read again");
    assert_eq!(FakeNfs::count(&fake.forgets), 0);
    Ok(())
}

/// Before the first page, a stale handle is resolved again and the directory read anew; the
/// children of the failed attempt are dropped, so none is handed over twice.
#[tokio::test]
async fn a_failure_before_the_first_page_restarts_the_directory_once_resolved_again() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    fake.fault(fault("d", 7, NfsRestart::Relookup, FailureClass::Protocol));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(failure.is_none(), "{failure:?}");
    let listed = names(&pages);
    let mut unique = listed.clone();
    unique.sort();
    unique.dedup();
    assert_eq!((listed.len(), unique.len()), (30, 30));
    assert_eq!(FakeNfs::count(&fake.reads_started), 2);
    assert_eq!(FakeNfs::count(&fake.forgets), 1);
    assert_eq!(FakeNfs::count(&fake.lookups), 2);
    Ok(())
}

/// A handle that stays stale is resolved again at most `MAX_RELOOKUPS` times, then the listing
/// fails; a failure nothing can cure fails at once.
#[tokio::test]
async fn relooking_up_is_bounded_and_other_failures_are_not_retried() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut always = fault("d", 0, NfsRestart::Relookup, FailureClass::Protocol);
    always.times = usize::MAX;
    fake.fault(always);
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(pages.is_empty());
    assert!(failure.is_some());
    assert_eq!(FakeNfs::count(&fake.forgets), usize::from(MAX_RELOOKUPS));

    let refused = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    refused.fault(fault(
        "d",
        0,
        NfsRestart::Never,
        FailureClass::PermissionDenied,
    ));
    let (pages, failure) = drain(listing(&refused, "d", CancellationToken::new())?).await;
    assert!(pages.is_empty());
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::PermissionDenied
    ));
    assert_eq!(FakeNfs::count(&refused.reads_started), 1);
    Ok(())
}

/// A busy server (`NFS4ERR_DELAY`, `NFS3ERR_JUKEBOX`) before the first page is backed off and the
/// directory read anew; after the listing's budget the throttling is the listing's failure.
#[tokio::test(start_paused = true)]
async fn a_busy_server_before_the_first_page_is_backed_off() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut busy = fault("d", 3, NfsRestart::Backoff, FailureClass::Throttled);
    busy.times = 2;
    fake.fault(busy);
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages).len(), 30);
    assert_eq!(FakeNfs::count(&fake.reads_started), 3);

    let saturated = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut always = fault("d", 0, NfsRestart::Backoff, FailureClass::Throttled);
    always.times = usize::MAX;
    saturated.fault(always);
    let (_, failure) = drain(listing(&saturated, "d", CancellationToken::new())?).await;
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Throttled && failure.transience() == Transience::Transient
    ));
    Ok(())
}

/// A request carrying a hint reads that handle at once: no lookup.
#[tokio::test]
async fn a_hinted_listing_reads_the_hinted_handle_without_a_lookup() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 3));
    let namespace = fake.namespace();
    let hint = descent_hint(&namespace, &fake.handle("d")).ok_or("a handle makes a hint")?;
    let hinted = request("d", CancellationToken::new())?.with_descent_hint(Some(hint));
    let listed = collect_listing(namespace.list(hinted), CollectLimits::UNBOUNDED).await?;
    assert_eq!(listed.children.len(), 3);
    assert_eq!(FakeNfs::count(&fake.lookups), 0);
    assert_eq!(
        locked(&fake.read_with).as_slice(),
        [("d".to_owned(), fake.handle("d"))]
    );
    Ok(())
}

/// A hint that no longer names the directory falls back to resolving the path, once; a
/// directory gone altogether is then `NotFound`.
#[tokio::test]
async fn a_stale_hint_falls_back_to_a_lookup() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 3));
    let namespace = fake.namespace();
    let hint = descent_hint(&namespace, &fake.handle("d")).ok_or("a handle makes a hint")?;
    fake.replace("d");
    let hinted = request("d", CancellationToken::new())?.with_descent_hint(Some(hint.clone()));
    let listed = collect_listing(namespace.list(hinted), CollectLimits::UNBOUNDED).await?;
    assert_eq!(listed.children.len(), 3);
    assert_eq!(FakeNfs::count(&fake.lookups), 1);
    assert_eq!(
        FakeNfs::count(&fake.forgets),
        0,
        "a stale hint needs no forgetting"
    );

    let empty = Arc::new(FakeNfs::new(REPLY).flat("other", 1));
    let hinted = request("d", CancellationToken::new())?.with_descent_hint(Some(hint));
    let gone = collect_listing(empty.namespace().list(hinted), CollectLimits::UNBOUNDED).await;
    assert!(matches!(
        gone,
        Err(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::NotFound
    ));
    Ok(())
}

/// Every listed subdirectory carries a copy of its handle as its descent hint; files carry none.
#[tokio::test]
async fn listed_directories_carry_their_handle_as_a_hint() -> Result {
    let children = vec![
        ("sub".to_owned(), EntryKind::Directory),
        ("file".to_owned(), EntryKind::File),
    ];
    let fake = Arc::new(FakeNfs::new(REPLY).directory("", children));
    let listed = collect_listing(
        fake.namespace()
            .list(request("", CancellationToken::new())?),
        CollectLimits::UNBOUNDED,
    )
    .await?;
    let namespace = fake.namespace();
    let hints: Vec<(&str, Option<Bytes>)> = listed
        .children
        .iter()
        .map(|child| {
            (
                child.name(),
                child
                    .descent_hint()
                    .and_then(|hint| hinted_handle(&namespace, hint)),
            )
        })
        .collect();
    assert_eq!(hints, [("sub", Some(fake.handle("sub"))), ("file", None)]);
    Ok(())
}

/// A hint only its own namespace reads: one from another endpoint (another export, another root)
/// could name a directory outside this root, so the path is resolved instead; a hint with no
/// handle in it is no hint.
#[tokio::test]
async fn a_hint_from_another_namespace_is_not_read() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 3));
    let other = fake.namespace_named("another-export");
    let foreign = descent_hint(&other, &fake.handle("d")).ok_or("a handle makes a hint")?;
    let raw = DescentHint::copied_from(&fake.handle("d"));
    for hint in [foreign, raw] {
        let hinted = request("d", CancellationToken::new())?.with_descent_hint(Some(hint));
        let listed =
            collect_listing(fake.namespace().list(hinted), CollectLimits::UNBOUNDED).await?;
        assert_eq!(listed.children.len(), 3);
    }
    assert_eq!(FakeNfs::count(&fake.lookups), 2);
    assert!(descent_hint(&fake.namespace(), b"").is_none());
    Ok(())
}

/// A listing cancelled while it backs off a busy server ends at once with the `Cancelled`
/// signal, without reading again.
#[tokio::test(start_paused = true)]
async fn cancelling_during_a_backoff_ends_the_listing_with_a_cancelled_signal() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut always = fault("d", 0, NfsRestart::Backoff, FailureClass::Throttled);
    always.times = usize::MAX;
    fake.fault(always);
    let cancel = CancellationToken::new();
    let stream = listing(&fake, "d", cancel.clone())?;
    let cancelling = async {
        while FakeNfs::count(&fake.reads_started) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cancel.cancel();
    };
    let ((pages, failure), ()) = tokio::join!(drain(stream), cancelling);
    assert!(pages.is_empty());
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::Cancelled
    ));
    let reads = FakeNfs::count(&fake.reads_started);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        FakeNfs::count(&fake.reads_started),
        reads,
        "read again after cancellation"
    );
    Ok(())
}

/// An empty directory is one empty page.
#[tokio::test]
async fn an_empty_directory_is_one_empty_page() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).directory("d", Vec::new()));
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(failure.is_none());
    assert_eq!(pages, [ListingPage::default()]);
    Ok(())
}

/// An entry the reader hides (ONTAP's `NFSv3` snapshot directory, P7 C7b) is counted in the
/// page's `internal_hidden`, neither handed over nor a failure.
#[tokio::test]
async fn a_hidden_entry_is_counted_not_listed() -> Result {
    let (dir, file) = (EntryKind::Directory, EntryKind::File);
    let children = [("a", file), (".snapshot", dir), ("b", file), ("c", file)]
        .into_iter()
        .map(|(name, kind)| (name.to_owned(), kind))
        .collect();
    let fake = Arc::new(
        FakeNfs::new(REPLY)
            .directory("d", children)
            .hiding(".snapshot"),
    );
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(failure.is_none());
    assert_eq!(names(&pages), ["a", "b", "c"]);
    assert!(pages.iter().all(|page| page.failures.is_empty()));
    let hidden: u32 = pages.iter().map(|page| page.internal_hidden).sum();
    assert_eq!(hidden, 1);
    Ok(())
}

/// A transport failure nfs-rs certifies safe to resend: before the first page the directory is
/// read anew after a pause, a few times, and what that does not cure keeps its connectivity scope
/// (the session); after the first page it is this listing's failure alone — the session is still
/// usable, only the READDIRPLUS position is lost.
#[tokio::test(start_paused = true)]
async fn a_safe_transport_failure_is_resent_before_the_first_page_and_ends_only_the_listing_after()
-> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut blip = fault("d", 4, NfsRestart::Resend, FailureClass::Connectivity);
    blip.times = 2;
    fake.fault(blip);
    let (pages, failure) = drain(listing(&fake, "d", CancellationToken::new())?).await;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages).len(), 30);
    assert_eq!(FakeNfs::count(&fake.reads_started), 3);

    let down = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    let mut always = fault("d", 0, NfsRestart::Resend, FailureClass::Connectivity);
    always.times = usize::MAX;
    down.fault(always);
    let (_, failure) = drain(listing(&down, "d", CancellationToken::new())?).await;
    assert!(
        matches!(failure, Some(StorageRoleFailure::Session(_))),
        "{failure:?}"
    );
    assert_eq!(
        FakeNfs::count(&down.reads_started),
        super::RESEND_DELAYS.len() + 1
    );

    let later = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    let mut always = fault("d", 15, NfsRestart::Resend, FailureClass::Connectivity);
    always.times = usize::MAX;
    later.fault(always);
    let (pages, failure) = drain(listing(&later, "d", CancellationToken::new())?).await;
    assert_eq!(names(&pages).len(), 15);
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::Connectivity
    ));
    assert_eq!(
        FakeNfs::count(&later.reads_started),
        super::RESEND_DELAYS.len() + 1
    );
    Ok(())
}

/// After backing off a busy server or pausing for a resend, the directory is read again with the
/// handle it had: the handle still names it, so nothing is resolved.
#[tokio::test(start_paused = true)]
async fn rereading_after_a_pause_keeps_the_hinted_handle() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", 30));
    fake.fault(fault("d", 3, NfsRestart::Backoff, FailureClass::Throttled));
    fake.fault(fault(
        "d",
        5,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let namespace = fake.namespace();
    let hint = descent_hint(&namespace, &fake.handle("d")).ok_or("a handle makes a hint")?;
    let hinted = request("d", CancellationToken::new())?.with_descent_hint(Some(hint));
    let listed = collect_listing(namespace.list(hinted), CollectLimits::UNBOUNDED).await?;
    assert_eq!(listed.children.len(), 30);
    assert_eq!(FakeNfs::count(&fake.reads_started), 3);
    assert_eq!(FakeNfs::count(&fake.lookups), 0);
    Ok(())
}

/// Every child name the listing handed over, sorted, and whether none came twice.
fn listed_once(pages: &[ListingPage]) -> (Vec<String>, bool) {
    let listed = names(pages);
    let mut unique = listed.clone();
    unique.sort();
    unique.dedup();
    let once = unique.len() == listed.len();
    (unique, once)
}

/// All of `d`'s `FILES` children, sorted.
fn all_files() -> Vec<String> {
    (0..FILES).map(|index| format!("f{index:05}")).collect()
}

/// Drains the listing of `d` from `fake`, within a bound: a listing that loops fails the test
/// instead of hanging it (the fake yields once per reply, so a loop does not starve the timer).
async fn drained(fake: &Arc<FakeNfs>) -> Result<(Vec<ListingPage>, Option<StorageRoleFailure>)> {
    let stream = listing(fake, "d", CancellationToken::new())?;
    Ok(tokio::time::timeout(Duration::from_secs(30), drain(stream)).await?)
}

/// After the first page, a stale handle, a busy server and a transport failure safe to resend
/// are retried as before it — but from the cookie after the last reply taken whole: the reply the
/// failure cut short (children 50–59) comes again and is skipped, and every child is handed over
/// exactly once.
#[tokio::test(start_paused = true)]
async fn after_the_first_page_a_read_continues_from_the_last_cookie() -> Result {
    for (restart, class) in [
        (NfsRestart::Relookup, FailureClass::Protocol),
        (NfsRestart::Backoff, FailureClass::Throttled),
        (NfsRestart::Resend, FailureClass::Connectivity),
    ] {
        let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
        fake.fault(fault("d", 60, restart, class));
        let (pages, failure) = drained(&fake).await?;
        assert!(failure.is_none(), "{restart:?}: {failure:?}");
        assert_eq!(listed_once(&pages), (all_files(), true), "{restart:?}");
        assert_eq!(locked(&fake.read_from).as_slice(), [0, 50], "{restart:?}");
        assert_eq!(FakeNfs::count(&fake.read), FILES + 10, "{restart:?}");
    }
    Ok(())
}

/// A server that ignores the cookie and sends the directory from its start again: what the
/// listing already took is skipped, the rest handed over once, and the listing completes.
#[tokio::test(start_paused = true)]
async fn a_server_that_restarts_on_a_cookie_hands_nothing_over_twice() -> Result {
    let fake = Arc::new(
        FakeNfs::new(REPLY)
            .flat("d", FILES)
            .cookies("d", Cookies::Restarts),
    );
    fake.fault(fault(
        "d",
        60,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let (pages, failure) = drained(&fake).await?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(listed_once(&pages), (all_files(), true));
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 50]);
    assert_eq!(FakeNfs::count(&fake.read), FILES + 60);
    Ok(())
}

/// A server that rejects the cookie (as ONTAP rejects a stale verifier), or answers it with an
/// empty end of directory (as ONTAP and DXN-2 answer a cookie they do not know): the directory is
/// read again from its start, what was taken skipped, and every child handed over once.
#[tokio::test(start_paused = true)]
async fn a_rejected_or_forgotten_cookie_reads_the_directory_from_its_start() -> Result {
    for cookies in [Cookies::Rejects, Cookies::Forgets] {
        let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES).cookies("d", cookies));
        fake.fault(fault(
            "d",
            60,
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ));
        let (pages, failure) = drained(&fake).await?;
        assert!(failure.is_none(), "{cookies:?}: {failure:?}");
        assert_eq!(listed_once(&pages), (all_files(), true), "{cookies:?}");
        assert_eq!(
            locked(&fake.read_from).as_slice(),
            [0, 50, 0],
            "{cookies:?}"
        );
    }
    Ok(())
}

/// Reading from the start after a rejected cookie is bounded: a directory that keeps rejecting
/// without handing over anything new ends after `MAX_FROM_START` such reads, after what was read.
#[tokio::test]
async fn reading_from_the_start_again_is_bounded() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    let mut always = fault("d", 60, NfsRestart::FromStart, FailureClass::Protocol);
    always.times = usize::MAX;
    fake.fault(always);
    let (pages, failure) = drained(&fake).await?;
    assert_eq!(listed_once(&pages).0.len(), 60);
    assert!(listed_once(&pages).1);
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::Protocol
    ));
    assert_eq!(
        FakeNfs::count(&fake.reads_started),
        usize::from(MAX_FROM_START) + 1
    );
    Ok(())
}

/// Cookies that loop within one read end the listing (`Protocol` / Permanent, P7 §8) after what
/// was read, instead of reading forever.
#[tokio::test]
async fn looping_cookies_end_the_listing() -> Result {
    let fake = Arc::new(
        FakeNfs::new(REPLY)
            .flat("d", FILES)
            .cookies("d", Cookies::Loops),
    );
    let (pages, failure) = drained(&fake).await?;
    assert_eq!(listed_once(&pages).0.len(), 2 * REPLY);
    assert!(matches!(
        failure,
        Some(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::Protocol
                && failure.transience() == Transience::Permanent
                && failure.path() == &path("d")
    ));
    assert_eq!(FakeNfs::count(&fake.reads_started), 1);
    Ok(())
}

/// A cookie means nothing on another handle: when the directory's path resolves to a new handle
/// after the first page, the directory is read from its start (what was taken skipped).
#[tokio::test]
async fn a_new_handle_after_the_first_page_reads_from_the_start() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    fake.fault(fault("d", 60, NfsRestart::Relookup, FailureClass::Protocol));
    let mut stream = listing(&fake, "d", CancellationToken::new())?;
    let first = next_page(&mut stream).await?;
    assert!(settled(&fake).await < 60, "read up to the fault already");
    fake.replace("d");
    let (rest, failure) = drain(stream).await;
    assert!(failure.is_none(), "{failure:?}");
    let mut pages = vec![first];
    pages.extend(rest);
    assert_eq!(listed_once(&pages), (all_files(), true));
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 0]);
    assert_eq!(FakeNfs::count(&fake.lookups), 2);
    Ok(())
}

/// Every child newly handed over refills the retries: more transport failures in one listing
/// than `RESEND_DELAYS` allows, each after progress, still complete it.
#[tokio::test(start_paused = true)]
async fn progress_refills_the_retries() -> Result {
    let fake = Arc::new(FakeNfs::new(REPLY).flat("d", FILES));
    for after in [60, 200, 400, 600, 800] {
        fake.fault(fault(
            "d",
            after,
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ));
    }
    let (pages, failure) = drained(&fake).await?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(listed_once(&pages), (all_files(), true));
    assert_eq!(FakeNfs::count(&fake.reads_started), 6);
    Ok(())
}
