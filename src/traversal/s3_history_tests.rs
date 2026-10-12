//! Traversal of a versioned S3 bucket (P7 §10): each key is one entry with its whole history —
//! keys deleted now included — in both orders; the entry-level facts are the current object's;
//! what the plan asks beyond the listing is observed record by record, bound to each version, a
//! record's failure staying on that record; and the answer at connection decides between
//! histories and current objects.

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use super::s3_tests::{delete, entries, put, render, rendered, request, totals, traverse};
use super::{FailureScope, TraversalItem, TraversalOrder, TraversalOutcome};
use crate::model::{
    FailureClass, HistoryOrder, MetadataObservation, MetadataProvenance, ObjectHistory,
    ObservationMode, ObservationPlan, ObservedEntry, SourceVersion, Transience, VersionRecord,
};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, identity};
use crate::storage::backends::s3::{S3ProtocolFailure, S3VersioningAtConnect};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const ORDERS: [TraversalOrder; 2] = [TraversalOrder::Blocks, TraversalOrder::Unordered];

/// `k`: v1, v2, a delete marker, v3; `gone`: written, then deleted; an artifact.
pub(super) async fn history() -> TestResult<Arc<MemoryS3>> {
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

fn by_path<'a>(entries: &[&'a ObservedEntry]) -> BTreeMap<String, &'a ObservedEntry> {
    entries
        .iter()
        .map(|entry| (entry.path().to_string(), *entry))
        .collect()
}

/// One entry per key with every version and marker, oldest first, the latest last — `gone`,
/// deleted now, too — in both orders, field for field the same. A key's size and copy selector
/// are its current object's; a key deleted now has neither.
#[tokio::test]
async fn each_key_is_one_entry_with_its_history() -> TestResult {
    let protocol = history().await?;
    let (items, outcome) = traverse(&protocol, request()).await?;
    assert_eq!(
        rendered(&items),
        [
            "gone File [v0005, v0006 L M]",
            "k File [v0001, v0002, v0003 M, v0004 L]",
            "listed  Complete hidden=1",
        ]
    );
    let totals = totals(&outcome);
    assert_eq!((totals.listed, totals.emitted, totals.files), (2, 2, 2));
    // Per key; the records apart: `k` v1, v2, v3 and a marker, `gone` a version and a marker.
    assert_eq!((totals.versions, totals.delete_markers), (4, 2));
    assert_eq!(
        (totals.bytes, totals.all_versions_bytes),
        (3, 2 + 2 + 3 + 1)
    );
    let blocks = by_path(&entries(&items));
    let (gone, k) = (blocks["gone"], blocks["k"]);
    assert_eq!((gone.size(), gone.source_version()), (None, None));
    assert_eq!(
        (k.size(), k.source_version()),
        (Some(3), Some(SourceVersion::Id("v0004".into())))
    );
    let history = k.history().ok_or("k has a history")?;
    assert_eq!((history.segment(), history.is_complete()), (0, true));
    assert_eq!(history.order(), HistoryOrder::Server);

    let (items, outcome) = traverse(&protocol, request().order(TraversalOrder::Unordered)).await?;
    assert!(matches!(outcome, TraversalOutcome::Completed(_)));
    assert_eq!(by_path(&entries(&items)), blocks);
    Ok(())
}

/// The answer at connection decides: a bucket that answered never versioned lists its current
/// objects — `k` as v3, no `gone`, no history — with the same identity the key has above.
#[tokio::test]
async fn a_never_versioned_answer_lists_current_objects() -> TestResult {
    let protocol = history().await?;
    let (items, _) = traverse(&protocol, request()).await?;
    let with_history = entries(&items)
        .iter()
        .find(|entry| entry.path().as_str() == "k")
        .map(|entry| (*entry).clone())
        .ok_or("k listed")?;
    protocol.answer_at_connect(S3VersioningAtConnect::Never);
    let (items, _) = traverse(&protocol, request()).await?;
    assert_eq!(rendered(&items), ["k File", "listed  Complete hidden=1"]);
    let current = entries(&items)[0];
    assert_eq!(current.size(), Some(3));
    assert_eq!(current.source_version(), Some(SourceVersion::Current));
    assert_eq!(current.identity_key(), with_history.identity_key());
    Ok(())
}

/// A key's entry — its history with it — survives a snapshot; so does every other entry.
#[tokio::test]
async fn history_entries_survive_a_snapshot() -> TestResult {
    let protocol = history().await?;
    let (items, _) = traverse(&protocol, request()).await?;
    for entry in entries(&items) {
        let snapshot = entry.encode_snapshot();
        assert_eq!(snapshot.as_bytes()[4], 7);
        let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes(), &identity())?;
        assert_eq!(&rebuilt, entry);
    }
    Ok(())
}

/// The tags each version of a key's history reports, `None` where nothing was observed.
fn record_tags(entry: &ObservedEntry) -> Vec<Option<String>> {
    entry
        .history()
        .map(ObjectHistory::records)
        .unwrap_or_default()
        .iter()
        .map(|record| {
            record.metadata().map(|metadata| match metadata.tags() {
                MetadataObservation::Value { .. } => "value".to_owned(),
                other => format!("{other:?}"),
            })
        })
        .collect()
}

/// Optional metadata of each version is that version's own: its tags are read by its id, one
/// version after another; a delete marker, which has nothing behind it, asks the store nothing.
/// The entry's own metadata is its current version's; a key deleted now has nothing behind it.
#[tokio::test]
async fn metadata_of_each_version_is_bound_to_that_version() -> TestResult {
    let plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
    for order in ORDERS {
        let protocol = history().await?;
        let (items, _) = traverse(&protocol, request().order(order).observation_plan(plan)).await?;
        let mut read: Vec<Option<String>> = protocol.tag_versions.lock().await.clone();
        read.sort();
        let id = |value: &str| Some(value.to_string());
        assert_eq!(read, [id("v0001"), id("v0002"), id("v0004"), id("v0005")]);
        let found = by_path(&entries(&items));
        let value = || Some("value".to_owned());
        assert_eq!(record_tags(found["k"]), [value(), value(), None, value()]);
        assert_eq!(record_tags(found["gone"]), [value(), None]);
        assert!(matches!(
            found["k"].metadata().tags(),
            MetadataObservation::Value { .. }
        ));
        assert!(matches!(
            found["gone"].metadata().tags(),
            MetadataObservation::NotApplicable
        ));
    }
    let protocol = history().await?;
    protocol.answer_at_connect(S3VersioningAtConnect::Never);
    traverse(&protocol, request().observation_plan(plan)).await?;
    assert_eq!(*protocol.tag_versions.lock().await, [None]);
    Ok(())
}

/// A version whose observation fails keeps the failure on its own record, whatever the mode —
/// `Required` here — and the key is still delivered with every other record observed.
#[tokio::test]
async fn a_failed_record_observation_stays_on_that_record() -> TestResult {
    let plan = ObservationPlan::default().with_tags(ObservationMode::Required);
    for order in ORDERS {
        let protocol = history().await?;
        let failure =
            S3ProtocolFailure::entry(FailureClass::Protocol, Transience::Permanent, "tags");
        *protocol.tag_failure.lock().await = Some(("v0002".into(), failure));
        let (items, outcome) =
            traverse(&protocol, request().order(order).observation_plan(plan)).await?;
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, TraversalItem::Failure(_))),
            "{:?}",
            rendered(&items)
        );
        assert_eq!(totals(&outcome).failed_children, 0);
        let found = by_path(&entries(&items));
        let failed = Some(format!(
            "{:?}",
            MetadataObservation::<()>::Failed {
                class: FailureClass::Protocol,
                transience: Transience::Permanent
            }
        ));
        let value = || Some("value".to_owned());
        assert_eq!(record_tags(found["k"]), [value(), failed, None, value()]);
    }
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
    let plan = ObservationPlan::default().with_tags(ObservationMode::BestEffort);
    let (items, _) = traverse(&protocol, request().observation_plan(plan)).await?;
    assert!(
        !rendered(&items)
            .iter()
            .any(|item| item.starts_with("failure"))
    );
    let s = entries(&items)[0];
    let selected: Vec<Option<SourceVersion>> = s
        .history()
        .ok_or("s has a history")?
        .records()
        .iter()
        .map(VersionRecord::source)
        .collect();
    let id = |value: &str| Some(SourceVersion::Id(value.to_string()));
    assert_eq!(selected, [id("v0001"), id("null"), id("v0002")]);
    let read = protocol.tag_versions.lock().await.clone();
    let named = |value: &str| Some(value.to_string());
    assert_eq!(read, [named("v0001"), named("null"), named("v0002")]);
    Ok(())
}

/// A name can repeat: the object `a` with forty versions and the prefix `a/`. A block is in
/// listing order (P7 C4, no sibling sort): `a` is one entry with its forty versions, oldest first,
/// and the prefix `a` stays where S3's key order put it, after `a-39`.
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
    let mut blocks = request();
    blocks.max_depth = Some(NonZeroUsize::MIN);
    let (items, _) = traverse(&protocol, blocks).await?;
    let first: Vec<String> = (1..=40)
        .map(|n| format!("v{n:04}{}", if n == 40 { " L" } else { "" }))
        .collect();
    let mut expected = vec![format!("a File [{}]", first.join(", "))];
    expected.push("a! File [v0041 L]".into());
    expected.extend((0..40).map(|index| format!("a-{index:02} File [v{:04} L]", 42 + index)));
    expected.push("a Directory".into());
    let got: Vec<String> = items
        .iter()
        .filter(|item| matches!(item, TraversalItem::Entry(_)))
        .map(render)
        .collect();
    assert_eq!(got, expected);
    Ok(())
}

/// A directory whose listing pages in a loop (a MinIO-style server serving its first page again,
/// the same continuation marker coming back) fails that directory alone: the traversal lists the
/// directories beside it and finishes. A versioned bucket's directory is gathered whole, so nothing
/// of `a` is handed over and it is `Unreadable`; listed by current objects it streams, its first
/// page (`a/x`) handed over before the loop showed, so it is `Interrupted` after it.
#[tokio::test]
async fn a_looping_listing_fails_only_its_directory() -> TestResult {
    for histories in [true, false] {
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
        if !histories {
            protocol.answer_at_connect(S3VersioningAtConnect::Never);
        }
        let traversal = traverse(&protocol, request());
        let (items, outcome) = tokio::time::timeout(Duration::from_secs(10), traversal).await??;
        let rendered = rendered(&items);
        let files = |item: &&String| item.starts_with("b/z File") || item.starts_with("c File");
        assert_eq!(rendered.iter().filter(files).count(), 2, "{rendered:?}");
        let expected: &[&str] = if histories {
            &["failure Listing a", "listed a Unreadable"]
        } else {
            &["a/x File", "failure Listing a", "listed a Interrupted"]
        };
        let around_a: Vec<&String> = rendered
            .iter()
            .filter(|item| {
                item.starts_with("a/") || item.ends_with(" a") || item.starts_with("listed a ")
            })
            .collect();
        assert_eq!(around_a, expected, "histories={histories}: {rendered:?}");
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
        let totals = totals(&outcome);
        assert_eq!(
            (totals.listing_failures, totals.interrupted_listings),
            (1, u64::from(!histories)),
            "histories={histories}: {totals:?}"
        );
    }
    Ok(())
}

/// Unordered: a key is one entry however its listing is paged and however the other directories'
/// pages interleave — here a key of 1100 versions straddles two listing pages while eight workers
/// deliver forty other directories side by side, and the session holds one batch at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unordered_delivers_each_key_once_with_its_whole_history() -> TestResult {
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
    for round in 0..3 {
        let request = request()
            .order(TraversalOrder::Unordered)
            .max_buffered_items(NonZeroUsize::MIN);
        let (items, outcome) = traverse(&protocol, request).await?;
        let totals = totals(&outcome);
        assert_eq!(totals.files, 40 * 50 + 3, "round {round}");
        let found = entries(&items);
        let k: Vec<&&ObservedEntry> = found
            .iter()
            .filter(|entry| entry.path().as_str() == "v/k")
            .collect();
        assert_eq!(k.len(), 1, "round {round}");
        assert_eq!(
            k[0].history().map(|history| history.records().len()),
            Some(VERSIONS),
            "round {round}"
        );
    }
    Ok(())
}

/// Inline-only metadata never turns a versioned scan into one HEAD per stored version. Listed
/// timestamps stay inline; a store omitting them stays unknown instead of being queried.
#[tokio::test]
async fn inline_only_history_observation_uses_no_object_requests() -> TestResult {
    let inline = ObservationMode::InlineOnly;
    let plan = ObservationPlan::default()
        .with_tags(inline)
        .with_timestamps(inline);
    for order in ORDERS {
        for missing_time in [false, true] {
            let protocol = history().await?;
            protocol.listing().omit_version_timestamps = missing_time;
            protocol.head_versions.lock().await.clear();
            let (items, outcome) =
                traverse(&protocol, request().order(order).observation_plan(plan)).await?;
            assert!(matches!(outcome, TraversalOutcome::Completed(_)));
            assert!(protocol.head_versions.lock().await.is_empty(), "{order:?}");
            assert!(protocol.tag_versions.lock().await.is_empty(), "{order:?}");
            assert_inline_history_metadata(&items, missing_time)?;
        }
    }
    Ok(())
}

fn assert_inline_history_metadata(items: &[TraversalItem], missing_time: bool) -> TestResult {
    let found = by_path(&entries(items));
    let k = found["k"];
    assert_eq!(k.metadata().tags(), &MetadataObservation::Unsupported);
    for entry in found.values() {
        for record in entry.history().ok_or("each key carries history")?.records() {
            if record.is_delete_marker() {
                assert!(record.metadata().is_none());
                continue;
            }
            let metadata = record.metadata().ok_or("inline request is answered")?;
            assert_eq!(metadata.tags(), &MetadataObservation::Unsupported);
            if missing_time {
                assert_eq!(metadata.timestamps(), &MetadataObservation::Unsupported);
            } else {
                assert!(matches!(metadata.timestamps(), MetadataObservation::Value {
                    value, provenance: MetadataProvenance::Inline
                } if value.modified == record.modified()));
            }
        }
    }
    Ok(())
}
