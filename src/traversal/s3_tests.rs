//! Traversal over the S3 namespace role (ADR-0006 C22), through the same
//! `StorageTraversalSource` every other backend uses, in both version modes, against the
//! in-memory S3 of the role tests.

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
    TraversalSource, TraversalVersions,
};
use crate::model::{
    EntryKind, FailureClass, MetadataObservation, ObservationMode, ObservationPlan, ObservedEntry,
    SourceVersion, StoragePath, StorageTimestamp, TimePrecision, Transience,
};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, content_md5, identity};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, connect};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn request(versions: TraversalVersions) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(limit(8))
        .max_buffered_items(limit(8))
        .cancel(CancellationToken::new())
        .versions(versions)
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn delete(protocol: &MemoryS3, key: &str) -> TestResult {
    protocol
        .delete_object(key)
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn traverse(
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

fn entries(items: &[TraversalItem]) -> Vec<&ObservedEntry> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.as_ref()),
            _ => None,
        })
        .collect()
}

/// `path kind [id L M]` of an entry, `failure scope path`, `listed path outcome [set counts]`.
fn render(item: &TraversalItem) -> String {
    match item {
        TraversalItem::Entry(entry) => {
            let version = entry.version().map_or(String::new(), |version| {
                format!(
                    " {}{}{}",
                    version.id().unwrap_or("null"),
                    if version.is_latest() { " L" } else { "" },
                    if version.is_delete_marker() { " M" } else { "" }
                )
            });
            format!("{} {:?}{version}", entry.path(), entry.kind())
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
    }
}

/// The totals of a completed traversal.
fn totals(outcome: &TraversalOutcome) -> ScanTotals {
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert!(completion.totals.is_balanced(), "{completion:?}");
    completion.totals
}

fn rendered(items: &[TraversalItem]) -> Vec<String> {
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
    let (items, outcome) = traverse(&protocol, request(TraversalVersions::Current)).await?;
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

/// `k`: v1, v2, a delete marker, v3; `gone`: written, then deleted; an artifact.
async fn history() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "k", b"v1").await?;
    put(&protocol, "k", b"v2").await?;
    delete(&protocol, "k").await?;
    put(&protocol, "k", b"v3!").await?;
    put(&protocol, "gone", b"g").await?;
    delete(&protocol, "gone").await?;
    put(&protocol, ".data-mover-x", b"artifact").await?;
    Ok(protocol)
}

/// Current lists v3 of `k` only and no `gone`; All lists every version and marker, oldest first,
/// the latest last, each selecting itself for a copy.
#[tokio::test]
async fn current_and_all_on_one_history() -> TestResult {
    let protocol = history().await?;
    let (items, _) = traverse(&protocol, request(TraversalVersions::Current)).await?;
    assert_eq!(rendered(&items), ["k File", "listed  Complete hidden=1"]);
    let current = entries(&items);
    assert_eq!(current[0].size(), Some(3));
    assert_eq!(current[0].source_version(), Some(SourceVersion::Current));

    let (items, outcome) = traverse(&protocol, request(TraversalVersions::All)).await?;
    assert_eq!(
        rendered(&items),
        [
            "gone File v0005",
            "gone File v0006 L M",
            "k File v0001",
            "k File v0002",
            "k File v0003 M",
            "k File v0004 L",
            "listed  Complete hidden=1",
        ]
    );
    let totals = totals(&outcome);
    // One child per version and marker; a marker has no size.
    assert_eq!((totals.listed, totals.emitted, totals.files), (6, 6, 6));
    assert_eq!(totals.bytes, 2 + 2 + 3 + 1);
    let all = entries(&items);
    let selected: Vec<Option<SourceVersion>> =
        all.iter().map(|entry| entry.source_version()).collect();
    let id = |value: &str| Some(SourceVersion::Id(value.to_string()));
    assert_eq!(
        selected,
        [
            id("v0005"),
            None,
            id("v0001"),
            id("v0002"),
            None,
            id("v0004")
        ]
    );
    assert_eq!((all[2].size(), all[4].size()), (Some(2), None));
    Ok(())
}

/// A version entry keeps its version through a snapshot (format v6); an entry without one is
/// still written as v5, byte for byte what it was.
#[tokio::test]
async fn version_entries_survive_a_snapshot() -> TestResult {
    let protocol = history().await?;
    for versions in [TraversalVersions::Current, TraversalVersions::All] {
        let (items, _) = traverse(&protocol, request(versions)).await?;
        for entry in entries(&items) {
            let snapshot = entry.encode_snapshot();
            assert_eq!(
                snapshot.as_bytes()[4],
                if entry.version().is_some() { 6 } else { 5 }
            );
            let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes(), &identity())?;
            assert_eq!(&rebuilt, entry);
        }
    }
    Ok(())
}

/// Optional metadata of an old version is that version's own: its tags are read by its id, and a
/// delete marker, which has nothing behind it, asks the store nothing.
#[tokio::test]
async fn metadata_of_a_version_is_bound_to_that_version() -> TestResult {
    let protocol = history().await?;
    let plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
    let mut all = request(TraversalVersions::All);
    all.observation_plan = plan;
    let (items, _) = traverse(&protocol, all).await?;
    let read: Vec<Option<String>> = protocol.tag_versions.lock().await.clone();
    let id = |value: &str| Some(value.to_string());
    assert_eq!(read, [id("v0005"), id("v0001"), id("v0002"), id("v0004")]);
    let marker = entries(&items)[1];
    assert!(matches!(
        marker.metadata().tags(),
        MetadataObservation::NotApplicable
    ));
    protocol.tag_versions.lock().await.clear();
    let mut current = request(TraversalVersions::Current);
    current.observation_plan = plan;
    traverse(&protocol, current).await?;
    assert_eq!(*protocol.tag_versions.lock().await, [None]);
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
    fn needs_modified(&self) -> bool {
        false
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

/// A subtree the filter prunes, or `max_depth` stops at, is never listed: S3 prunes on the
/// server side, one prefix at a time.
#[tokio::test]
async fn pruned_and_truncated_prefixes_are_never_listed() -> TestResult {
    let protocol = tree().await?;
    let mut pruned = request(TraversalVersions::Current);
    pruned.filter = Some(Arc::new(PruneSkip));
    let (items, _) = traverse(&protocol, pruned).await?;
    assert!(!rendered(&items).iter().any(|item| item.contains("skip")));
    assert_eq!(protocol.listing().calls, ["", "keep/", "keep/deep/"]);

    protocol.listing().calls.clear();
    let mut shallow = request(TraversalVersions::All);
    shallow.max_depth = Some(limit(1));
    let (items, _) = traverse(&protocol, shallow).await?;
    assert_eq!(protocol.listing().calls, [""]);
    assert_eq!(
        rendered(&items),
        [
            "keep Directory",
            "skip Directory",
            "top File null L",
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
    fn needs_modified(&self) -> bool {
        true
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
    let mut filtered = request(TraversalVersions::Current);
    filtered.filter = Some(Arc::new(filter));
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

/// A name can repeat: the object `a` with forty versions and the prefix `a/`. A block is in
/// listing order (P7 C4, no sibling sort), so the key's versions stay contiguous and oldest first
/// as the listing gave them, and the prefix `a` stays where S3's key order put it, after `a-39`.
#[tokio::test]
async fn a_repeated_name_keeps_the_listing_order() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for _ in 0..40 {
        put(&protocol, "a", b"1").await?;
    }
    put(&protocol, "a!", b"1").await?;
    for index in 0..40 {
        put(&protocol, &format!("a-{index:02}"), b"1").await?;
    }
    put(&protocol, "a/x", b"1").await?;
    let mut blocks = request(TraversalVersions::All);
    blocks.order = TraversalOrder::Blocks;
    blocks.max_depth = Some(limit(1));
    let (items, _) = traverse(&protocol, blocks).await?;
    let mut expected: Vec<String> = (1..=40)
        .map(|n| format!("a File v{n:04}{}", if n == 40 { " L" } else { "" }))
        .collect();
    expected.push("a! File v0041 L".into());
    expected.extend((0..40).map(|index| format!("a-{index:02} File v{:04} L", 42 + index)));
    expected.push("a Directory".into());
    let got: Vec<String> = items
        .iter()
        .filter(|item| matches!(item, TraversalItem::Entry(_)))
        .map(render)
        .collect();
    assert_eq!(got, expected);
    Ok(())
}

/// A `"null"` version is observed bound to itself: an older one by `?versionId=null`, the latest
/// one as the current object, each only while the store still holds what was listed.
#[tokio::test]
async fn the_null_version_is_observed_bound_to_itself() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "s", b"real").await?;
    protocol.set_versioning(Versioning::Suspended);
    put(&protocol, "s", b"null").await?;
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "s", b"newer").await?;
    let mut all = request(TraversalVersions::All);
    all.observation_plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
    let (items, _) = traverse(&protocol, all).await?;
    assert!(
        !rendered(&items)
            .iter()
            .any(|item| item.starts_with("failure"))
    );
    let selected: Vec<Option<SourceVersion>> = entries(&items)
        .iter()
        .map(|entry| entry.source_version())
        .collect();
    let id = |value: &str| Some(SourceVersion::Id(value.to_string()));
    assert_eq!(selected, [id("v0001"), id("null"), id("v0002")]);
    let read = protocol.tag_versions.lock().await.clone();
    let named = |value: &str| Some(value.to_string());
    assert_eq!(read, [named("v0001"), named("null"), named("v0002")]);
    Ok(())
}

/// A traversal rooted at a prefix under which nothing is stored fails that root's listing, so
/// its listing is unreadable: an absent source never reads as an empty one.
#[tokio::test]
async fn an_absent_root_is_not_an_empty_source() -> TestResult {
    let protocol = tree().await?;
    let mut missing = request(TraversalVersions::Current);
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
    let mut listing = request(TraversalVersions::Current);
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

/// A directory whose listing pages in a loop (a MinIO-style server serving its first page again,
/// the same continuation marker coming back) fails that directory alone: the traversal lists the
/// directories beside it and finishes, in both version modes. The listing of current objects
/// streams (P7 C6): its first page (`a/x`) was handed over before the loop showed, so that
/// directory is `Interrupted` after it, never listed twice. The listing of every version is still
/// gathered whole, so there nothing of `a` is handed over and it is `Unreadable`.
#[tokio::test]
async fn a_looping_listing_fails_only_its_directory() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for key in ["a/x", "a/y", "b/z", "c"] {
        put(&protocol, key, b"1").await?;
    }
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        knobs.minio_markers = true;
        knobs.restart_prefix = Some("a/".to_string());
    }
    for versions in [TraversalVersions::Current, TraversalVersions::All] {
        let traversal = traverse(&protocol, request(versions));
        let (items, outcome) = tokio::time::timeout(Duration::from_secs(10), traversal).await??;
        let files = |item: &String| item.starts_with("b/z File") || item.starts_with("c File");
        let rendered = rendered(&items);
        assert_eq!(
            rendered.iter().filter(|item| files(item)).count(),
            2,
            "{rendered:?}"
        );
        let streamed = versions == TraversalVersions::Current;
        let expected: &[&str] = if streamed {
            &["a/x File", "failure Listing a", "listed a Interrupted"]
        } else {
            &["failure Listing a", "listed a Unreadable"]
        };
        let around_a: Vec<&String> = rendered
            .iter()
            .filter(|item| {
                item.starts_with("a/") || item.ends_with(" a") || item.starts_with("listed a ")
            })
            .collect();
        assert_eq!(around_a, expected, "{versions:?}: {rendered:?}");
        assert!(
            rendered.contains(&"listed b Complete".to_string()),
            "{rendered:?}"
        );
        let failure = items.iter().find_map(|item| match item {
            TraversalItem::Failure(failure) => Some(failure),
            _ => None,
        });
        assert!(
            failure.is_some_and(|f| f.scope == FailureScope::Listing
                && f.failure.class() == FailureClass::Protocol
                && f.failure.transience() == Transience::Permanent),
            "{failure:?}"
        );
        // The one looping directory is the scan's one listing failure: its subtree is out of
        // any deletion, and nothing else is.
        let totals = totals(&outcome);
        assert_eq!(
            (totals.listing_failures, totals.interrupted_listings),
            (1, u64::from(streamed)),
            "{versions:?}: {totals:?}"
        );
    }
    Ok(())
}

/// What the role protocol maps a 503 `SlowDown` to (pinned by its own test; traversal may not
/// import `crate::s3`).
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
    let (items, outcome) = traverse(&protocol, request(TraversalVersions::Current)).await?;
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
        .max_inflight_operations(limit(8))
        .max_buffered_items(limit(8));
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

/// Unordered, every version: one object's versions stay together however its listing is paged
/// and however the other directories' pages interleave — here a key of 1100 versions straddles
/// two listing pages while eight workers deliver forty other directories side by side, and the
/// session holds one batch at a time, so a worker waiting for room is let in between any two
/// batches of another.
///
/// Fault injection: send each page's trailing run with that page (no carry), or let another
/// worker's pieces land between a split batch's pieces, and a run is broken — red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unordered_keeps_every_version_of_an_object_together() -> TestResult {
    const VERSIONS: usize = 1100;
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for directory in 0..40 {
        for index in 0..50 {
            put(&protocol, &format!("d{directory}/o{index:03}"), b"1").await?;
        }
    }
    put(&protocol, "v/a", b"1").await?;
    for _ in 0..VERSIONS {
        put(&protocol, "v/k", b"1").await?;
    }
    put(&protocol, "v/z", b"1").await?;
    for round in 0..5 {
        let request = request(TraversalVersions::All)
            .order(TraversalOrder::Unordered)
            .max_buffered_items(limit(1));
        let (items, outcome) = traverse(&protocol, request).await?;
        assert!(
            matches!(outcome, TraversalOutcome::Completed(_)),
            "{outcome:?}"
        );
        let keys: Vec<&str> = entries(&items)
            .iter()
            .filter(|entry| entry.kind() == EntryKind::File)
            .map(|entry| entry.path().as_str())
            .collect();
        assert_eq!(keys.len(), 40 * 50 + VERSIONS + 2, "round {round}");
        let mut runs: Vec<&str> = keys.clone();
        runs.dedup();
        let mut distinct = runs.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            runs.len(),
            distinct.len(),
            "round {round}: a key's versions were split"
        );
    }
    Ok(())
}
