//! Continuation must preserve progress even when no complete output page exists yet.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::super::super::listing_fake::{Cookies, FakeNfs, locked};
use super::super::super::listing_resume::MAX_REMEMBERED;
use super::super::super::namespace::NfsRestart;
use super::super::{MAX_FROM_START, MAX_RELOOKUPS, RESEND_DELAYS};
use super::{Result, drain, fault, listed_once, names, path};
use crate::model::{EntryKind, FailureClass};
use crate::storage::{ListRequest, ListingPage, Namespace as _, StorageRoleFailure};

async fn with_hint(
    fake: &Arc<FakeNfs>,
    hint: usize,
) -> Result<(Vec<ListingPage>, Option<StorageRoleFailure>)> {
    let request = ListRequest::new(path("d"), CancellationToken::new())
        .with_page_hint(NonZeroUsize::new(hint).ok_or("nonzero page hint")?);
    Ok(tokio::time::timeout(
        Duration::from_secs(30),
        drain(fake.namespace().list(request)),
    )
    .await?)
}

fn expected_files(files: usize) -> Vec<String> {
    (0..files).map(|index| format!("f{index:05}")).collect()
}

/// More than three failures before any output page, each with new entries, must still finish.
/// A partial last reply is replayed; accepted entries stay buffered and are not repeated.
#[tokio::test(start_paused = true)]
async fn new_entries_refill_retries_before_the_first_page() -> Result {
    for (restart, class) in [
        (NfsRestart::Relookup, FailureClass::Protocol),
        (NfsRestart::Backoff, FailureClass::Throttled),
        (NfsRestart::Resend, FailureClass::Connectivity),
    ] {
        let fake = Arc::new(FakeNfs::new(2).flat("d", 15));
        for after in [2, 4, 6, 8, 9] {
            fake.fault(fault("d", after, restart, class));
        }
        let (pages, failure) = with_hint(&fake, 20).await?;
        assert!(failure.is_none(), "{restart:?}: {failure:?}");
        assert_eq!(listed_once(&pages), (expected_files(15), true));
        assert_eq!(pages.len(), 1);
        assert_eq!(locked(&fake.read_from).as_slice(), [0, 2, 4, 6, 8, 8]);
        assert_eq!(FakeNfs::count(&fake.read), 16, "one entry replayed");
    }
    Ok(())
}

/// Replayed entries are not progress. Exhaustion still has the session scope used before the
/// first page, even though the final buffered entries are handed over before that failure.
#[tokio::test(start_paused = true)]
async fn replay_without_new_entries_exhausts_the_pre_page_retry_budget() -> Result {
    for cookies in [Cookies::Honours, Cookies::Restarts] {
        let fake = Arc::new(FakeNfs::new(25).flat("d", 100).cookies("d", cookies));
        let mut repeated = fault("d", 60, NfsRestart::Resend, FailureClass::Connectivity);
        repeated.times = usize::MAX;
        fake.fault(repeated);
        let (pages, failure) = with_hint(&fake, 101).await?;
        assert_eq!(listed_once(&pages), (expected_files(60), true));
        assert_eq!(pages.len(), 1);
        assert!(matches!(failure, Some(StorageRoleFailure::Session(_))));
        assert_eq!(FakeNfs::count(&fake.reads_started), RESEND_DELAYS.len() + 1);
        assert_eq!(locked(&fake.read_from).as_slice(), [0, 50, 50, 50]);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn rejected_or_empty_continuations_rewind_without_discarding_the_buffer() -> Result {
    for cookies in [Cookies::Rejects, Cookies::Forgets] {
        let fake = Arc::new(FakeNfs::new(25).flat("d", 100).cookies("d", cookies));
        fake.fault(fault(
            "d",
            60,
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ));
        let (pages, failure) = with_hint(&fake, 101).await?;
        assert!(failure.is_none(), "{cookies:?}: {failure:?}");
        assert_eq!(listed_once(&pages), (expected_files(100), true));
        assert_eq!(pages.len(), 1);
        assert_eq!(locked(&fake.read_from).as_slice(), [0, 50, 0]);
        assert_eq!(FakeNfs::count(&fake.read), 160);
    }
    Ok(())
}

#[tokio::test]
async fn repeated_rewinds_or_relookups_without_progress_are_bounded() -> Result {
    for (restart, retries) in [
        (NfsRestart::FromStart, MAX_FROM_START),
        (NfsRestart::Relookup, MAX_RELOOKUPS),
    ] {
        let fake = Arc::new(FakeNfs::new(25).flat("d", 100));
        let mut repeated = fault("d", 60, restart, FailureClass::Protocol);
        repeated.times = usize::MAX;
        fake.fault(repeated);
        let (pages, failure) = with_hint(&fake, 101).await?;
        assert_eq!(listed_once(&pages), (expected_files(60), true));
        assert!(matches!(failure, Some(StorageRoleFailure::Entry(_))));
        assert_eq!(
            FakeNfs::count(&fake.reads_started),
            usize::from(retries) + 1
        );
    }
    Ok(())
}

/// A refreshed handle cannot use the old cookie, but it must not duplicate the retained buffer.
#[tokio::test]
async fn a_changed_handle_before_the_first_page_rewinds_and_deduplicates() -> Result {
    let gate = Arc::new(Semaphore::new(1));
    let fake = Arc::new(FakeNfs::new(2).flat("d", 15).gated(Arc::clone(&gate)));
    fake.fault(fault("d", 3, NfsRestart::Relookup, FailureClass::Protocol));
    let replace = async {
        while FakeNfs::count(&fake.read) < 2 {
            yield_now().await;
        }
        fake.replace("d");
        gate.add_permits(20);
    };
    let (result, ()) = tokio::join!(with_hint(&fake, 20), replace);
    let (pages, failure) = result?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(listed_once(&pages), (expected_files(15), true));
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 0]);
    assert_eq!(FakeNfs::count(&fake.read), 18);
    assert_eq!(FakeNfs::count(&fake.lookups), 2);
    Ok(())
}

/// A failed snapshot lookup has seen its entry but not accepted it; continuation must retry it.
#[tokio::test(start_paused = true)]
async fn an_entry_seen_but_not_taken_is_not_lost_on_continuation() -> Result {
    let fake = Arc::new(FakeNfs::new(2).flat("d", 15).faulting_after_sight());
    fake.fault(fault(
        "d",
        3,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let (pages, failure) = with_hint(&fake, 20).await?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(listed_once(&pages), (expected_files(15), true));
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 2]);
    assert_eq!(
        FakeNfs::count(&fake.read),
        17,
        "one accepted and one pending entry replay"
    );
    Ok(())
}

/// Both kinds of hidden entries and child failures are accepted entries. None is counted twice
/// when the incomplete reply is replayed, and hidden entries do not fill the output page.
#[tokio::test(start_paused = true)]
async fn replay_preserves_hidden_and_child_failure_counts_once() -> Result {
    let children = [".snapshot", ".data-mover-stage", "broken", "good", "last"]
        .map(|name| (name.to_owned(), EntryKind::File))
        .to_vec();
    let fake = Arc::new(
        FakeNfs::new(5)
            .directory("d", children)
            .hiding(".snapshot")
            .omitting("broken"),
    );
    fake.fault(fault(
        "d",
        4,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let (pages, failure) = with_hint(&fake, 3).await?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(pages.len(), 1);
    assert_eq!(names(&pages), ["good", "last"]);
    assert_eq!(pages[0].internal_hidden, 2);
    assert_eq!(pages[0].failures.len(), 1);
    assert_eq!(pages[0].failures[0].name.as_deref(), Some("broken"));
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 0]);
    Ok(())
}

/// Every hidden entry has a distinct fileid, even though the lossy name is the same. They use
/// up remembrance without publishing a page or creating one descriptor per entry.
fn beyond_remembrance() -> Arc<FakeNfs> {
    let mut children = vec![(".snapshot".to_owned(), EntryKind::Directory); MAX_REMEMBERED + 1];
    children.push(("tail".to_owned(), EntryKind::File));
    Arc::new(
        FakeNfs::new(512)
            .directory("d", children)
            .hiding(".snapshot"),
    )
}

#[tokio::test(start_paused = true)]
async fn an_unpublished_listing_can_fall_back_after_remembrance_runs_out() -> Result {
    let fake = beyond_remembrance();
    fake.fault(fault(
        "d",
        MAX_REMEMBERED + 1,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let (pages, failure) = with_hint(&fake, 10).await?;
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(names(&pages), ["tail"]);
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].internal_hidden as usize, MAX_REMEMBERED + 1);
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 0]);
    Ok(())
}

/// Once fallback discards remembrance, a replay of fewer than MAX_REMEMBERED entries must not
/// re-enable it and gain fresh retry budgets. Only the last unpublished attempt counts.
#[tokio::test(start_paused = true)]
async fn fallback_never_reenables_remembrance_or_refills_retries() -> Result {
    let fake = beyond_remembrance();
    fake.fault(fault(
        "d",
        MAX_REMEMBERED + 1,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let mut repeated = fault("d", 7, NfsRestart::Resend, FailureClass::Connectivity);
    repeated.times = usize::MAX;
    fake.fault(repeated);
    let (pages, failure) = with_hint(&fake, 10).await?;
    assert!(matches!(failure, Some(StorageRoleFailure::Session(_))));
    assert_eq!(pages.len(), 1);
    assert!(names(&pages).is_empty());
    assert_eq!(pages[0].internal_hidden, 7);
    assert_eq!(FakeNfs::count(&fake.reads_started), RESEND_DELAYS.len() + 1);
    assert_eq!(locked(&fake.read_from).as_slice(), [0, 0, 0, 0]);
    Ok(())
}

/// Exhausting remembrance after a visible page keeps the existing fail-without-retry rule.
#[tokio::test(start_paused = true)]
async fn a_published_listing_cannot_fall_back_after_remembrance_runs_out() -> Result {
    let mut children = vec![("first".to_owned(), EntryKind::File)];
    children.extend(vec![
        (".snapshot".to_owned(), EntryKind::Directory);
        MAX_REMEMBERED
    ]);
    children.push(("tail".to_owned(), EntryKind::File));
    let fake = Arc::new(
        FakeNfs::new(512)
            .directory("d", children)
            .hiding(".snapshot"),
    );
    fake.fault(fault(
        "d",
        MAX_REMEMBERED + 1,
        NfsRestart::Resend,
        FailureClass::Connectivity,
    ));
    let (pages, failure) = with_hint(&fake, 1).await?;
    assert_eq!(names(&pages), ["first"]);
    assert_eq!(
        pages
            .iter()
            .map(|page| page.internal_hidden as usize)
            .sum::<usize>(),
        MAX_REMEMBERED
    );
    assert!(
        matches!(failure, Some(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::Connectivity)
    );
    assert_eq!(FakeNfs::count(&fake.reads_started), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_a_pre_page_retry_stops_without_publishing_the_buffer() -> Result {
    let fake = Arc::new(FakeNfs::new(2).flat("d", 15));
    let mut repeated = fault("d", 3, NfsRestart::Backoff, FailureClass::Throttled);
    repeated.times = usize::MAX;
    fake.fault(repeated);
    let cancel = CancellationToken::new();
    let request = ListRequest::new(path("d"), cancel.clone());
    let cancelling = async {
        while FakeNfs::count(&fake.reads_ended) == 0 {
            yield_now().await;
        }
        cancel.cancel();
    };
    let ((pages, failure), ()) = tokio::join!(drain(fake.namespace().list(request)), cancelling);
    assert!(pages.is_empty());
    assert!(
        matches!(failure, Some(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::Cancelled)
    );
    assert_eq!(FakeNfs::count(&fake.reads_started), 1);
    Ok(())
}

#[tokio::test]
async fn an_uncurable_failure_flushes_the_pre_page_buffer_and_keeps_its_scope() -> Result {
    for class in [FailureClass::Connectivity, FailureClass::PermissionDenied] {
        let fake = Arc::new(FakeNfs::new(2).flat("d", 15));
        fake.fault(fault("d", 3, NfsRestart::Never, class));
        let (pages, failure) = with_hint(&fake, 20).await?;
        assert_eq!(listed_once(&pages), (expected_files(3), true));
        assert_eq!(FakeNfs::count(&fake.reads_started), 1);
        match class {
            FailureClass::Connectivity => {
                assert!(matches!(failure, Some(StorageRoleFailure::Session(_))));
            }
            _ => assert!(
                matches!(failure, Some(StorageRoleFailure::Entry(ref failure))
                if failure.class() == class)
            ),
        }
    }
    Ok(())
}
