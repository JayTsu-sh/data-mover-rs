//! Traversal over the S3 namespace role (ADR-0006 C22), through the same
//! `StorageTraversalSource` every other backend uses, against the in-memory S3 of the role tests.
//! A versioned bucket's keys with their histories: `s3_history_tests`.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ScanTotals, StorageTraversalSource, TraversalCandidate, TraversalDecision,
    TraversalFilter, TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest,
    TraversalSource,
};
use crate::model::{
    EntryKind, FailureClass, MetadataFamily, MetadataRequirements, ObservationMode,
    ObservationPlan, ObservedEntry, StoragePath, StorageTimestamp, TimePrecision, Transience,
};
use crate::storage::backends::s3::tests::{MemoryS3, content_md5, identity};
use crate::storage::backends::s3::{
    S3Protocol as _, S3ProtocolFailure, S3VersioningAtConnect, connect,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

pub(super) fn request() -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(limit(8))
        .cancel(CancellationToken::new())
}

pub(super) async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone().into(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

pub(super) async fn delete(protocol: &MemoryS3, key: &str) -> TestResult {
    protocol
        .delete_object(key)
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

pub(super) async fn traverse(
    protocol: &Arc<MemoryS3>,
    request: TraversalRequest,
) -> TestResult<(Vec<TraversalItem>, TraversalOutcome)> {
    let storage = connect(protocol.clone(), identity(), None)?;
    let mut session = StorageTraversalSource::new(&storage)?.traverse(request);
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    Ok((items, session.finish().await?))
}

pub(super) fn entries(items: &[TraversalItem]) -> Vec<&ObservedEntry> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.as_ref()),
            _ => None,
        })
        .collect()
}

/// `path kind [id[ L][ M], …]` of an entry (the brackets for a key with its history, one per
/// record), `failure scope path`, `listed path outcome [set counts]`.
pub(super) fn render(item: &TraversalItem) -> String {
    match item {
        TraversalItem::Entry(entry) => {
            let history = entry.history().map_or(String::new(), |history| {
                let records: Vec<String> = history
                    .records()
                    .iter()
                    .map(|record| {
                        format!(
                            "{}{}{}",
                            record.version_id().unwrap_or("null"),
                            if record.is_latest() { " L" } else { "" },
                            if record.is_delete_marker() { " M" } else { "" }
                        )
                    })
                    .collect();
                format!(" [{}]", records.join(", "))
            });
            format!("{} {:?}{history}", entry.path(), entry.kind())
        }
        TraversalItem::Failure(failure) => format!("failure {:?} {}", failure.scope, failure.path),
        TraversalItem::DirectoryListed(listed) => {
            let counts = &listed.counts;
            assert!(counts.is_balanced(), "{listed:?}");
            [
                ("hidden", counts.internal_hidden),
                ("filtered", counts.filtered),
                ("failed", counts.failed_children),
                ("pruned", counts.pruned_dirs),
                ("truncated", counts.truncated_dirs),
            ]
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .fold(
                format!("listed {} {:?}", listed.path, listed.outcome),
                |mut line, (label, count)| {
                    let _ = write!(line, " {label}={count}");
                    line
                },
            )
        }
        TraversalItem::Checkpoint(_) => "checkpoint".to_owned(),
        TraversalItem::ObservationFailure(failure) => format!(
            "observation failure {} {:?} {:?}",
            failure.path, failure.family, failure.version
        ),
    }
}

/// The totals of a completed traversal.
pub(super) fn totals(outcome: &TraversalOutcome) -> ScanTotals {
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert!(completion.totals.is_balanced(), "{completion:?}");
    completion.totals
}

pub(super) fn rendered(items: &[TraversalItem]) -> Vec<String> {
    items.iter().map(render).collect()
}

/// Prefixes are directories: each listed one gets its block and its `DirectoryListed`, in block
/// order, and a hidden transfer artifact is counted as hidden, not as a child.
#[tokio::test]
async fn prefixes_are_directories_with_directory_events() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    for key in ["a/x", "a/y/z", "b", "k", ".data-mover-stage/s"] {
        put(&protocol, key, b"1").await?;
    }
    let (items, outcome) = traverse(&protocol, request()).await?;
    assert_eq!(
        rendered(&items),
        [
            "a Directory",
            "b File",
            "k File",
            "listed  Complete hidden=1",
            "a/x File",
            "a/y Directory",
            "listed a Complete",
            "a/y/z File",
            "listed a/y Complete",
        ]
    );
    let totals = totals(&outcome);
    assert_eq!((totals.listed, totals.emitted), (6, 6));
    assert_eq!((totals.files, totals.directories), (4, 2));
    assert_eq!(totals.listing_failures, 0);
    Ok(())
}

async fn tree() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for key in ["keep/a", "keep/deep/c", "skip/b", "top"] {
        put(&protocol, key, b"1").await?;
    }
    Ok(protocol)
}

/// Hides and does not descend into the directory `skip`.
#[derive(Debug)]
struct PruneSkip;

impl TraversalFilter for PruneSkip {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let skip = candidate.name == "skip";
        TraversalDecision {
            emit: !skip,
            descend: !skip && candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// Classification probes one level before filtering/depth decisions. Pruned subtrees are not
/// traversed further and their contents are never emitted; the probe is not a whole-tree scan.
#[tokio::test]
async fn pruned_and_truncated_prefixes_are_only_probed_for_classification() -> TestResult {
    let protocol = tree().await?;
    let mut pruned = request();
    pruned.filter = Some(Arc::new(PruneSkip));
    let (items, _) = traverse(&protocol, pruned).await?;
    assert!(!rendered(&items).iter().any(|item| item.contains("skip")));
    assert_eq!(
        protocol.listing().calls,
        ["", "keep/", "skip/", "keep/", "keep/deep/", "keep/deep/"]
    );

    protocol.listing().calls.clear();
    // Never versioned, refusing to say: its keys are listed with their histories (P7 Q26).
    protocol.answer_at_connect(S3VersioningAtConnect::Refused);
    let mut shallow = request();
    shallow.max_depth = Some(limit(1));
    let (items, _) = traverse(&protocol, shallow).await?;
    assert_eq!(protocol.listing().calls, ["", "keep/", "skip/"]);
    assert_eq!(
        rendered(&items),
        [
            "keep Directory",
            "skip Directory",
            "top File [null L]",
            "listed  Complete truncated=2",
        ]
    );
    Ok(())
}

/// Admits files modified after an instant; directories, which have no time on S3, are kept and
/// descended.
#[derive(Debug)]
struct ModifiedAfter(StorageTimestamp);

impl TraversalFilter for ModifiedAfter {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let directory = candidate.kind == EntryKind::Directory;
        TraversalDecision {
            emit: directory
                || candidate
                    .modified
                    .is_some_and(|time| time.unix_nanos() > self.0.unix_nanos()),
            descend: directory,
            filter_children: true,
        }
    }
}

/// A `modified` filter is decided from the listing's own times: no object is looked up, so a
/// store whose HEAD would fail still traverses cleanly, and prefixes (no time) are descended.
#[tokio::test]
async fn a_modified_filter_uses_the_listing_times() -> TestResult {
    let protocol = tree().await?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        1_800_000_000_000_000_000,
        TimePrecision::Milliseconds,
    )?);
    let failure = S3ProtocolFailure::entry(FailureClass::Protocol, Transience::Permanent, "head");
    *protocol.head_failure.lock().await = Some(("keep/a".to_string(), failure));
    let filter = ModifiedAfter(StorageTimestamp::new(
        1_700_000_000_000_000_000,
        TimePrecision::Seconds,
    )?);
    let mut filtered = request();
    filtered.filter = Some(Arc::new(filter));
    filtered.observation_plan =
        ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly);
    let (items, _) = traverse(&protocol, filtered).await?;
    let files: Vec<String> = entries(&items)
        .iter()
        .filter(|entry| entry.kind() == EntryKind::File)
        .map(|entry| entry.path().to_string())
        .collect();
    assert_eq!(files, ["top", "keep/a", "keep/deep/c", "skip/b"]);
    assert!(
        !rendered(&items)
            .iter()
            .any(|item| item.starts_with("failure"))
    );
    Ok(())
}

/// A traversal rooted at a prefix under which nothing is stored fails that root's listing, so
/// its listing is unreadable: an absent source never reads as an empty one.
#[tokio::test]
async fn an_absent_root_is_not_an_empty_source() -> TestResult {
    let protocol = tree().await?;
    let mut missing = request();
    missing.root = StoragePath::new("missing")?;
    let (items, _) = traverse(&protocol, missing).await?;
    assert_eq!(
        rendered(&items),
        ["failure Listing missing", "listed missing Unreadable",]
    );
    Ok(())
}

/// Cancelling stops an S3 listing between pages: its requests are stateless, so it is dropped
/// rather than left paging through a large directory in the background.
#[tokio::test]
async fn cancellation_stops_an_s3_listing_between_pages() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    for index in 0..40 {
        put(&protocol, &format!("k{index:02}"), b"1").await?;
    }
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        knobs.page_delay = Some(Duration::from_millis(20));
    }
    let storage = connect(protocol.clone(), identity(), None)?;
    let cancel = CancellationToken::new();
    let mut listing = request();
    listing.cancel = cancel.clone();
    let mut session = StorageTraversalSource::new(&storage)?.traverse(listing);
    sleep(Duration::from_millis(70)).await;
    cancel.cancel();
    while session.next_item().await.is_some() {}
    assert_eq!(session.finish().await?, TraversalOutcome::Cancelled);
    let at_cancel = protocol.listing().calls.len();
    sleep(Duration::from_millis(200)).await;
    let later = protocol.listing().calls.len();
    assert!(
        at_cancel < 40 && later <= at_cancel + 1,
        "{at_cancel} then {later}"
    );
    Ok(())
}

/// What the role protocol maps a 503 `SlowDown` to (pinned by its own test; traversal may not
/// import the private S3 client).
fn slow_down() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Throttled,
        Transience::Transient,
        "S3 ListObjectsV2 request failed",
    )
}

/// A throttled S3 page (503 `SlowDown`) is requested again within the listing's backoff budget
/// and the traversal completes with every entry (P7 §8, Q11; P7 review issue 1: one `SlowDown`
/// used to end the whole traversal). Fault injection: let the listing stop backing off on
/// `Throttled`, or the traversal stop scoping it, and the traversal ends with it — red. Paused
/// time: the production budget (about two minutes of backoff) passes instantly.
#[tokio::test(start_paused = true)]
async fn a_throttled_page_does_not_end_the_traversal() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    for key in ["a/x", "a/y", "b", "c"] {
        put(&protocol, key, b"1").await?;
    }
    protocol.listing().page_size = 1;
    protocol.listing().throttled = Some((4, slow_down()));
    let (items, outcome) = traverse(&protocol, request()).await?;
    let totals = totals(&outcome);
    assert_eq!((totals.emitted, totals.failed_children), (5, 0));
    assert_eq!(totals.listing_failures, 0);
    assert!(
        items
            .iter()
            .all(|item| !matches!(item, TraversalItem::Failure(_))),
        "{:?}",
        rendered(&items)
    );
    Ok(())
}

/// Throttled past the budget, the listing fails — that directory `Unreadable`, `Throttled` /
/// `Transient` — and the traversal still completes rather than ending with a session failure.
#[tokio::test(start_paused = true)]
async fn throttling_past_the_budget_fails_one_listing_not_the_traversal() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    for key in ["a/x", "b"] {
        put(&protocol, key, b"1").await?;
    }
    protocol.listing().throttled = Some((usize::MAX, slow_down()));
    let listing_of_a = TraversalRequest::new(StoragePath::new("a")?)
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(limit(8));
    let (items, outcome) = traverse(&protocol, listing_of_a).await?;
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some((
                failure.scope,
                failure.path.to_string(),
                failure.failure.class(),
                failure.failure.transience(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        [(
            FailureScope::Listing,
            "a".to_owned(),
            FailureClass::Throttled,
            Transience::Transient
        )]
    );
    assert_eq!(totals(&outcome).listing_failures, 1);
    Ok(())
}
