//! Directory objects through the S3 traversal (P7 F5): `p/d/` is File `p/d/` in its parent's block
//! when any version holds content; all-zero markers become Directory `p/d`, retaining history.
//! Only actual descendants add a directory alongside a content File. The root's own object is reported (content or history) or
//! dropped (zero bytes), never emitted at the root's own relative path; and `p/d/`, the file
//! `p/d` and the directory `p/d` never share an identity key or a snapshot (their transfer
//! identities differ too: `transfer::identity_tests`). Every case runs in both traversal orders:
//! the root's own object is taken out of the root's listing by the page preparation both share.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ScanTotals, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource,
};
use crate::model::{
    EntryKind, FailureClass, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
    Transience, VersionRecord,
};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, content_md5, identity};
use crate::storage::backends::s3::{
    S3Protocol as _, S3ProtocolFailure, S3VersioningAtConnect, connect,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const ORDERS: [TraversalOrder; 2] = [TraversalOrder::Unordered, TraversalOrder::Blocks];

fn request(root: &str, order: TraversalOrder) -> TestResult<TraversalRequest> {
    let limit = NonZeroUsize::new(8).ok_or("nonzero")?;
    Ok(TraversalRequest::new(StoragePath::new(root)?)
        .order(order)
        .max_inflight_operations(limit)
        .cancel(CancellationToken::new()))
}

/// What `protocol` answers at connection: histories listed (a refusal to say, Q26) or not.
fn lists_histories(protocol: &MemoryS3, histories: bool) {
    protocol.answer_at_connect(if histories {
        S3VersioningAtConnect::Refused
    } else {
        S3VersioningAtConnect::Never
    });
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone().into(), &content_md5(&bytes))
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

/// `path kind size`, with ` M` for a key deleted now; `failure scope path class`.
fn rows(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(format!(
                "{} {:?} {:?}{}",
                entry.path(),
                entry.kind(),
                entry.size(),
                if entry
                    .history()
                    .is_some_and(|history| history.current().is_none())
                {
                    " M"
                } else {
                    ""
                }
            )),
            TraversalItem::Failure(failure) => Some(format!(
                "failure {:?} {} {:?}",
                failure.scope,
                failure.path,
                failure.failure.class()
            )),
            _ => None,
        })
        .collect()
}

fn totals(outcome: &TraversalOutcome) -> ScanTotals {
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    assert!(completion.totals.is_balanced(), "{completion:?}");
    completion.totals
}

/// `p/` (5 B), `p/f`, `p/d/` (a zero-byte folder marker), `p/d/x`, `top`.
async fn tree() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    put(&protocol, "p/", b"fives").await?;
    put(&protocol, "p/f", b"1").await?;
    put(&protocol, "p/d/", b"").await?;
    put(&protocol, "p/d/x", b"1").await?;
    put(&protocol, "top", b"1").await?;
    Ok(protocol)
}

/// All-zero markers, including their histories, are represented by the directory. Nonzero
/// objects stay files and actual descendants independently establish the directory structure.
#[tokio::test]
async fn directory_objects_are_entries_counted_as_files() -> TestResult {
    for order in ORDERS {
        let protocol = tree().await?;
        for histories in [false, true] {
            lists_histories(&protocol, histories);
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            let mut listed = rows(&items);
            listed.sort();
            let expected = vec![
                "p Directory None",
                "p/ File Some(5)",
                "p/d Directory None",
                "p/d/x File Some(1)",
                "p/f File Some(1)",
                "top File Some(1)",
            ];
            assert_eq!(listed, expected, "{order:?} {histories}");
            let scan = totals(&outcome);
            let count = 6;
            assert_eq!(
                (scan.listed, scan.emitted, scan.failed_children),
                (count, count, 0)
            );
            assert_eq!(
                (scan.files, scan.directories, scan.bytes),
                (count - 2, 2, 8)
            );
        }
    }
    Ok(())
}

/// No stored keys invent no directories; a marker-only directory is emitted once even when its
/// marker arrives on another page. A normal zero-byte file with the same name survives too.
#[tokio::test]
async fn current_markers_are_directories_not_extra_files() -> TestResult {
    for order in ORDERS {
        for unordered in [false, true] {
            let protocol = Arc::new(MemoryS3::default());
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            assert!(entries(&items).is_empty());
            assert_eq!(totals(&outcome).listed, 0);
            put(&protocol, "empty/", b"").await?;
            put(&protocol, "empty", b"").await?;
            put(&protocol, "tree/", b"").await?;
            put(&protocol, "tree/file", b"x").await?;
            {
                let mut knobs = protocol.listing();
                knobs.page_size = 1;
                knobs.unordered = unordered;
                knobs.leading_empty_pages = 1;
            }
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            let mut listed = rows(&items);
            listed.sort();
            assert_eq!(
                listed,
                [
                    "empty Directory None",
                    "empty File Some(0)",
                    "tree Directory None",
                    "tree/file File Some(1)"
                ]
            );
            let scan = totals(&outcome);
            assert_eq!((scan.listed, scan.emitted, scan.filtered), (4, 4, 0));
            assert_eq!((scan.files, scan.directories, scan.bytes), (2, 2, 1));
            let (items, outcome) = traverse(&protocol, request("empty", order)?).await?;
            assert!(rows(&items).is_empty());
            assert_eq!(totals(&outcome).listed, 0);
        }
    }
    Ok(())
}

/// A trailing slash is not evidence of a subtree. A leaf content object is one File even at
/// depth one; an actual child independently adds a Directory without hiding that File.
#[tokio::test]
async fn a_content_leaf_is_one_file_and_only_real_children_add_a_directory() -> TestResult {
    for order in ORDERS {
        for histories in [false, true] {
            let protocol = Arc::new(MemoryS3::default());
            lists_histories(&protocol, histories);
            put(&protocol, "data/", b"content").await?;
            protocol.listing().page_size = 1;
            protocol.listing().unordered = true;
            let shallow =
                || -> TestResult<_> { Ok(request("", order)?.max_depth(Some(NonZeroUsize::MIN))) };
            let (items, outcome) = traverse(&protocol, shallow()?).await?;
            assert_eq!(rows(&items), ["data/ File Some(7)"]);
            assert_eq!(
                (totals(&outcome).files, totals(&outcome).directories),
                (1, 0)
            );
            put(&protocol, "data/child", b"x").await?;
            let (items, outcome) = traverse(&protocol, shallow()?).await?;
            let mut listed = rows(&items);
            listed.sort();
            assert_eq!(listed, ["data Directory None", "data/ File Some(7)"]);
            assert_eq!(totals(&outcome).truncated_dirs, 1);
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            let mut listed = rows(&items);
            listed.sort();
            assert_eq!(
                listed,
                [
                    "data Directory None",
                    "data/ File Some(7)",
                    "data/child File Some(1)"
                ]
            );
            assert_eq!(
                (
                    totals(&outcome).files,
                    totals(&outcome).directories,
                    totals(&outcome).bytes
                ),
                (2, 1, 8)
            );
        }
    }
    Ok(())
}

/// Zero-byte versions become a Directory, but each record is still observed at its raw `d/`
/// key, including a suspended null version. Snapshots and segments must retain that mapping.
#[tokio::test]
async fn directory_marker_history_keeps_its_raw_key_metadata_and_segments() -> TestResult {
    for order in ORDERS {
        let protocol = marker_with_null_history().await?;
        protocol.listing().page_size = 1;
        let storage = connect(protocol.clone(), identity(), None)?;
        let source = StorageTraversalSource::new(&storage)?.history_segment(1);
        let plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
        let mut session = source.traverse(request("", order)?.observation_plan(plan));
        let mut found = Vec::new();
        while let Some(item) = session.next_item().await {
            if let TraversalItem::Entry(entry) = item {
                found.push(entry);
            } else {
                assert!(!matches!(item, TraversalItem::Failure(_)), "{item:?}");
            }
        }
        let scan = totals(&session.finish().await?);
        assert_eq!(
            (scan.files, scan.directories, scan.versions, scan.bytes),
            (0, 1, 3, 0)
        );
        assert_eq!(found.len(), 3);
        for (index, entry) in found.iter().enumerate() {
            assert_eq!(entry.kind(), EntryKind::Directory);
            assert_eq!(entry.path().as_str(), "d");
            assert_eq!(entry.source_version(), None);
            assert_eq!(entry.history_source_path()?, Some(StoragePath::new("d/")?));
            let history = entry.history().ok_or("missing marker history")?;
            assert_eq!(history.segment() as usize, index);
            assert!(
                history.records()[0]
                    .metadata()
                    .ok_or("unobserved record")?
                    .tags
                    .value()
                    .is_some()
            );
            let snapshot = entry.encode_snapshot();
            let decoded = ObservedEntry::decode_snapshot(snapshot.as_bytes(), &identity())?;
            assert_eq!(&decoded, entry.as_ref());
        }
        assert_eq!(protocol.tag_versions.lock().await.len(), 3);
    }
    Ok(())
}

async fn marker_with_null_history() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for versioning in [
        Versioning::Enabled,
        Versioning::Suspended,
        Versioning::Enabled,
    ] {
        protocol.set_versioning(versioning);
        put(&protocol, "d/", b"").await?;
    }
    Ok(protocol)
}

#[tokio::test]
async fn an_unversioned_marker_retains_its_raw_metadata_observation() -> TestResult {
    for order in ORDERS {
        let protocol = Arc::new(MemoryS3::default());
        put(&protocol, "marker/", b"").await?;
        put(&protocol, "virtual/child", b"x").await?;
        let plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
        let (items, outcome) =
            traverse(&protocol, request("", order)?.observation_plan(plan)).await?;
        let found = entries(&items);
        let directory = found
            .iter()
            .find(|entry| entry.path().as_str() == "marker")
            .ok_or("missing marker directory")?;
        let marker = directory
            .directory_marker_object()
            .ok_or("missing raw marker")?;
        assert_eq!(marker.path().as_str(), "marker/");
        assert_eq!(marker.kind(), EntryKind::File);
        assert_eq!(marker.size(), Some(0));
        assert!(marker.metadata().tags().value().is_some());
        assert!(marker.history().is_none());
        assert!(
            found
                .iter()
                .find(|entry| entry.path().as_str() == "virtual")
                .ok_or("missing virtual directory")?
                .directory_marker_object()
                .is_none()
        );
        let decoded =
            ObservedEntry::decode_snapshot(directory.encode_snapshot().as_bytes(), &identity())?;
        assert_eq!(&decoded, *directory);
        assert_eq!(totals(&outcome).files, 1);
        assert_eq!(totals(&outcome).directories, 2);
        assert_eq!(totals(&outcome).failed_children, 0);
    }
    Ok(())
}

/// A delete marker has no size. A history made only of deletions must not be mistaken for
/// zero-byte directory markers by an empty `all(size == 0)` check.
#[tokio::test]
async fn deletion_only_history_does_not_invent_a_directory() -> TestResult {
    for order in ORDERS {
        let protocol = Arc::new(MemoryS3::default());
        protocol.set_versioning(Versioning::Enabled);
        delete(&protocol, "deleted/").await?;
        let (items, outcome) = traverse(&protocol, request("", order)?).await?;
        assert_eq!(rows(&items), ["deleted/ File None M"]);
        let scan = totals(&outcome);
        assert_eq!(
            (
                scan.files,
                scan.directories,
                scan.versions,
                scan.delete_markers
            ),
            (1, 0, 0, 1)
        );
    }
    Ok(())
}

async fn directory_histories() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "empty/", b"").await?;
    put(&protocol, "mixed/", b"old").await?;
    put(&protocol, "mixed/", b"").await?;
    put(&protocol, "deleted/", b"data").await?;
    put(&protocol, "deleted/", b"").await?;
    delete(&protocol, "deleted/").await?;
    put(&protocol, "zero-deleted/", b"").await?;
    delete(&protocol, "zero-deleted/").await?;
    put(&protocol, "plain", b"").await?;
    Ok(protocol)
}

/// Neither a current zero-byte object nor a latest delete marker says anything about the older
/// versions. Enabled and suspended buckets keep every record, across pages and in both orders.
#[tokio::test]
async fn whole_history_classifies_markers_without_losing_any_records() -> TestResult {
    for order in ORDERS {
        for versioning in [Versioning::Enabled, Versioning::Suspended] {
            let protocol = directory_histories().await?;
            protocol.set_versioning(versioning);
            protocol.listing().page_size = 1;
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            let found = entries(&items);
            for (key, expected) in [
                ("empty", vec![Some(0)]),
                ("mixed/", vec![Some(3), Some(0)]),
                ("deleted/", vec![Some(4), Some(0), None]),
                ("zero-deleted", vec![Some(0), None]),
            ] {
                let matches: Vec<_> = found.iter().filter(|e| e.path().as_str() == key).collect();
                assert_eq!(matches.len(), 1, "{key} {versioning:?} {order:?}");
                assert_eq!(
                    matches[0].kind(),
                    if key.ends_with('/') {
                        EntryKind::File
                    } else {
                        EntryKind::Directory
                    }
                );
                let history = matches[0].history().ok_or("missing history")?;
                assert_eq!(
                    history
                        .records()
                        .iter()
                        .map(VersionRecord::size)
                        .collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(history.current().is_none(), key.contains("deleted"));
                assert!(history.records().last().ok_or("empty history")?.is_latest());
            }
            let scan = totals(&outcome);
            assert_eq!((scan.listed, scan.emitted, scan.failed_children), (5, 5, 0));
            assert_eq!((scan.files, scan.directories, scan.bytes), (3, 2, 0));
            assert_eq!(
                (scan.versions, scan.delete_markers, scan.all_versions_bytes),
                (7, 2, 7)
            );
        }
    }
    Ok(())
}

/// Both request roots and URL prefixes constrain the actual S3 requests, not just the emitted
/// paths. Prefixes sharing a string prefix (chosen-other/) must never be traversed.
#[tokio::test]
async fn a_specified_prefix_never_lists_the_bucket_root_or_siblings() -> TestResult {
    for order in ORDERS {
        for histories in [false, true] {
            let protocol = Arc::new(MemoryS3::default());
            lists_histories(&protocol, histories);
            put(&protocol, "chosen/empty/", b"").await?;
            put(&protocol, "chosen/tree/file", b"x").await?;
            put(&protocol, "chosen-other/outside", b"y").await?;
            put(&protocol, "outside", b"z").await?;
            for url_prefix in [false, true] {
                protocol.listing().calls.clear();
                let source = if url_prefix {
                    protocol.source_root("chosen")
                } else {
                    protocol.clone()
                };
                let root = if url_prefix { "" } else { "chosen" };
                let (items, outcome) = traverse(&source, request(root, order)?).await?;
                let mut found: Vec<_> = entries(&items)
                    .iter()
                    .map(|entry| {
                        entry
                            .path()
                            .as_str()
                            .strip_prefix("chosen/")
                            .unwrap_or(entry.path().as_str())
                            .to_owned()
                    })
                    .collect();
                found.sort();
                let expected = vec!["empty", "tree", "tree/file"];
                assert_eq!(found, expected, "{order:?} {histories} {url_prefix}");
                assert_eq!(totals(&outcome).failed_children, 0);
                let mut calls = protocol.listing().calls.clone();
                calls.sort();
                calls.dedup();
                assert_eq!(calls, ["chosen/", "chosen/empty/", "chosen/tree/"]);
            }
        }
    }
    Ok(())
}

/// Old S3 checkpoints cannot silently resume with a different projection/counting contract.
#[tokio::test]
async fn checkpoints_bind_directory_marker_semantics() -> TestResult {
    use super::checkpoint::{FrontierEntry, encode, fingerprint};
    use super::{ResumeRefusal, TraversalTerminalFailure};

    for order in ORDERS {
        for histories in [false, true] {
            let protocol = Arc::new(MemoryS3::default());
            lists_histories(&protocol, histories);
            let storage = connect(protocol.clone(), identity(), None)?;
            let source = StorageTraversalSource::new(&storage)?;
            let request = request("chosen", order)?;
            let old_key = [u8::from(histories)];
            let old = fingerprint(&identity(), &request, &old_key)?;
            let frontier = [FrontierEntry {
                path: request.root.clone(),
                child_depth: 1,
                filter_children: false,
            }];
            let checkpoint = encode(order, &old, &frontier).ok_or("checkpoint too large")?;
            let mut session = source.traverse(request.resume_from(checkpoint));
            assert!(session.next_item().await.is_none());
            assert!(matches!(
                session.finish().await,
                Err(TraversalTerminalFailure::Resume(ResumeRefusal::Mismatch))
            ));
            assert!(protocol.listing().calls.is_empty(), "refuse before I/O");
        }
    }
    Ok(())
}

/// The root `p`: its own object holds content, so it is one `Unsupported` child failure at `p/`,
/// never an entry; the root `p/d`: its zero-byte marker is dropped, uncounted. The same with every
/// version of a history of content, zero-byte versions and markers: one failure, once.
#[tokio::test]
async fn the_traversal_roots_own_object_is_reported_not_emitted() -> TestResult {
    for order in ORDERS {
        let protocol = tree().await?;
        let (items, outcome) = traverse(&protocol, request("p", order)?).await?;
        let listed = rows(&items);
        assert_eq!(
            listed
                .iter()
                .filter(|row| row.starts_with("failure"))
                .collect::<Vec<_>>(),
            ["failure Child p/ Unsupported"]
        );
        assert!(
            !listed.iter().any(|row| row.starts_with("p/ ")),
            "{listed:?}"
        );
        let scan = totals(&outcome);
        assert_eq!((scan.emitted, scan.failed_children), (3, 1));
        let (items, outcome) = traverse(&protocol, request("p/d", order)?).await?;
        assert_eq!(rows(&items), ["p/d/x File Some(1)"]);
        assert_eq!(totals(&outcome).listed, 1);

        let versioned = Arc::new(MemoryS3::default());
        versioned.set_versioning(Versioning::Enabled);
        put(&versioned, "m/", b"fives").await?;
        put(&versioned, "m/", b"").await?;
        delete(&versioned, "m/").await?;
        put(&versioned, "m/", b"333").await?;
        put(&versioned, "m/x", b"1").await?;
        let (items, outcome) = traverse(&versioned, request("m", order)?).await?;
        assert_eq!(
            rows(&items),
            ["failure Child m/ Unsupported", "m/x File Some(1)"]
        );
        assert_eq!(totals(&outcome).failed_children, 1);
        // Deleted now, the key holds content in an older version: reported all the same (D4).
        put(&versioned, "n/", b"fives").await?;
        delete(&versioned, "n/").await?;
        put(&versioned, "n/x", b"1").await?;
        let (items, _) = traverse(&versioned, request("n", order)?).await?;
        assert_eq!(
            rows(&items),
            ["failure Child n/ Unsupported", "n/x File Some(1)"]
        );
        // Even an all-zero history contains records that cannot be represented by a directory.
        let zero = Arc::new(MemoryS3::default());
        zero.set_versioning(Versioning::Enabled);
        put(&zero, "z/", b"").await?;
        delete(&zero, "z/").await?;
        put(&zero, "z/x", b"1").await?;
        let (items, _) = traverse(&zero, request("z", order)?).await?;
        assert_eq!(
            rows(&items),
            ["failure Child z/ Unsupported", "z/x File Some(1)"]
        );
    }
    Ok(())
}

/// `p/d/`, the file `p/d` (same bytes) and the directory `p/d` coexist in S3 and stay three
/// entries: distinct identity keys, and snapshots that decode back to each, with or without their
/// histories.
#[tokio::test]
async fn directory_object_entries_survive_a_snapshot() -> TestResult {
    for order in ORDERS {
        let protocol = Arc::new(MemoryS3::default());
        put(&protocol, "p/d", b"same").await?;
        put(&protocol, "p/d/", b"same").await?;
        put(&protocol, "p/d/x", b"1").await?;
        for histories in [false, true] {
            lists_histories(&protocol, histories);
            let (items, _) = traverse(&protocol, request("", order)?).await?;
            let found = entries(&items);
            let named: Vec<&ObservedEntry> = found
                .into_iter()
                .filter(|entry| entry.path().as_str().starts_with("p/d"))
                .filter(|entry| entry.path().as_str() != "p/d/x")
                .collect();
            assert_eq!(named.len(), 3, "{order:?} {histories}");
            let keys: HashSet<_> = named.iter().map(|entry| entry.identity_key()).collect();
            assert_eq!(
                keys.len(),
                3,
                "identity keys must differ: {order:?} {histories}"
            );
            let own = named
                .iter()
                .find(|entry| entry.is_directory_object())
                .ok_or("p/d/ is listed")?;
            assert_eq!(own.kind(), EntryKind::File);
            for entry in &named {
                let snapshot = entry.encode_snapshot();
                assert_eq!(snapshot.as_bytes()[4], 9);
                let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes(), &identity())?;
                assert_eq!(&rebuilt, *entry);
            }
        }
    }
    Ok(())
}

/// An own object gone between its listing and its observation (timestamps asked of the metadata
/// role) is counted vanished, not reported.
#[tokio::test]
async fn a_vanished_directory_object_is_counted_vanished() -> TestResult {
    for order in ORDERS {
        let protocol = Arc::new(MemoryS3::default());
        put(&protocol, "d/", b"own").await?;
        put(&protocol, "d/x", b"1").await?;
        *protocol.head_failure.lock().await = Some((
            "d/".to_string(),
            S3ProtocolFailure::entry(FailureClass::NotFound, Transience::Permanent, "gone"),
        ));
        let mut asked = request("", order)?;
        asked.observation_plan =
            ObservationPlan::default().with_timestamps(ObservationMode::Required);
        let (items, outcome) = traverse(&protocol, asked).await?;
        let listed = rows(&items);
        assert!(
            !listed.iter().any(|row| row.starts_with("d/ ")),
            "{listed:?}"
        );
        assert!(
            !items.iter().any(
                |item| matches!(item, TraversalItem::Failure(f) if f.scope == FailureScope::Child)
            ),
            "{listed:?}"
        );
        assert_eq!(totals(&outcome).vanished, 1);
    }
    Ok(())
}
