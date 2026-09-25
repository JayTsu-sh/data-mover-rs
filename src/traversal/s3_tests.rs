//! Traversal over the S3 namespace role (ADR-0006 C22), through the same
//! `StorageTraversalSource` every other backend uses, in both version modes, against the
//! in-memory S3 of the role tests.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{
    StorageTraversalSource, TraversalCandidate, TraversalCompletion, TraversalDecision,
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
    TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: limit(8),
        max_buffered_items: limit(8),
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions,
    }
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

/// `path kind [id L M]` of an entry, `listed path listing`, `subtree path exhaustive`.
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
        TraversalItem::EntryFailure(failure) => format!("failure {}", failure.path()),
        TraversalItem::DirectoryListed(listed) => {
            format!("listed {} {:?}", listed.path, listed.listing)
        }
        TraversalItem::SubtreeComplete(complete) => format!(
            "subtree {} {}",
            complete.path,
            complete.summary.is_exhaustive()
        ),
    }
}

fn rendered(items: &[TraversalItem]) -> Vec<String> {
    items.iter().map(render).collect()
}

/// Prefixes are directories: each listed one gets its block, its `DirectoryListed` and its
/// `SubtreeComplete`, in block order, and the root's summary is exhaustive.
#[tokio::test]
async fn prefixes_are_directories_with_completion_items() -> TestResult {
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
            "listed  Complete",
            "a/x File",
            "a/y Directory",
            "listed a Complete",
            "a/y/z File",
            "listed a/y Complete",
            "subtree a/y true",
            "subtree a true",
            "subtree  true",
        ]
    );
    assert_eq!(
        outcome,
        TraversalOutcome::Completed(TraversalCompletion::new(6, 0, 3))
    );
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
    assert_eq!(
        rendered(&items),
        ["k File", "listed  Complete", "subtree  true"]
    );
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
            "listed  Complete",
            "subtree  true",
        ]
    );
    assert_eq!(
        outcome,
        TraversalOutcome::Completed(TraversalCompletion::new(6, 0, 1))
    );
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
            "listed  Complete",
            "subtree  false",
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

/// Sorted by name, a name can repeat: the object `a` with forty versions and the prefix `a/`.
/// The versions keep their order (oldest first) and the object precedes the directory, although
/// the listing put the directory after `a-39`. Forty ties are well past the length below which an
/// unstable sort happens to keep them.
#[tokio::test]
async fn name_order_keeps_repeated_names_in_a_defined_order() -> TestResult {
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
    let mut sorted = request(TraversalVersions::All);
    sorted.order = TraversalOrder::NameBytes;
    sorted.max_depth = Some(limit(1));
    let (items, _) = traverse(&protocol, sorted).await?;
    let mut expected: Vec<String> = (1..=40)
        .map(|n| format!("a File v{n:04}{}", if n == 40 { " L" } else { "" }))
        .collect();
    expected.push("a Directory".into());
    expected.push("a! File v0041 L".into());
    expected.extend((0..40).map(|index| format!("a-{index:02} File v{:04} L", 42 + index)));
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
/// its summary is not exhaustive: an absent source never reads as an empty one.
#[tokio::test]
async fn an_absent_root_is_not_an_empty_source() -> TestResult {
    let protocol = tree().await?;
    let mut missing = request(TraversalVersions::Current);
    missing.root = StoragePath::new("missing")?;
    let (items, _) = traverse(&protocol, missing).await?;
    assert_eq!(
        rendered(&items),
        [
            "failure missing",
            "listed missing Failed",
            "subtree missing false"
        ]
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
