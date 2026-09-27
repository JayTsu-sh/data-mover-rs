//! Directory objects through the S3 traversal (P7 F5): the key `p/d/` is a file entry of the block
//! of `p/d`, listed by current objects or with its history; the traversal root's own object is reported (content) or
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
    Transience,
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
        .max_buffered_items(limit)
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

/// Each own object is a file entry of its directory's block, counted as a file with its bytes;
/// the parent sees the directory only.
#[tokio::test]
async fn directory_objects_are_entries_counted_as_files() -> TestResult {
    for order in ORDERS {
        let protocol = tree().await?;
        for histories in [false, true] {
            lists_histories(&protocol, histories);
            let (items, outcome) = traverse(&protocol, request("", order)?).await?;
            let mut listed = rows(&items);
            listed.sort();
            assert_eq!(
                listed,
                [
                    "p Directory None",
                    "p/ File Some(5)",
                    "p/d Directory None",
                    "p/d/ File Some(0)",
                    "p/d/x File Some(1)",
                    "p/f File Some(1)",
                    "top File Some(1)",
                ],
                "{order:?} {histories}"
            );
            let scan = totals(&outcome);
            assert_eq!((scan.listed, scan.emitted, scan.failed_children), (7, 7, 0));
            assert_eq!((scan.files, scan.directories, scan.bytes), (5, 2, 8));
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
        assert_eq!((scan.emitted, scan.failed_children), (4, 1));
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
        // An all-zero history of the root's own key is dropped entirely.
        let zero = Arc::new(MemoryS3::default());
        zero.set_versioning(Versioning::Enabled);
        put(&zero, "z/", b"").await?;
        delete(&zero, "z/").await?;
        put(&zero, "z/x", b"1").await?;
        let (items, _) = traverse(&zero, request("z", order)?).await?;
        assert_eq!(rows(&items), ["z/x File Some(1)"]);
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
                assert_eq!(snapshot.as_bytes()[4], 7);
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
