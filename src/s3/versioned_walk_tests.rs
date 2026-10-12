//! What the legacy versioned listings send for one page of `ListObjectVersions` entries, without a
//! server (no tags asked, so nothing is requested): a key whose newest entry is a delete marker is
//! listed too (ADR-0006 D4).

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

use super::tests::make_test_storage;
use super::version_listing::{ListedEntry, VersionOrDeleteMarker};
use super::{S3Storage, VersionedScan};
use crate::filter::{FilterExpression, parse_filter_expression};
use crate::{EntryEnum, StorageEntryMessage};

type TestResult = Result<(), Box<dyn Error>>;

fn version(key: &str, id: &str, millis: i64, latest: bool) -> VersionOrDeleteMarker {
    VersionOrDeleteMarker::Version(
        ObjectVersion::builder()
            .key(key)
            .version_id(id)
            .size(3)
            .is_latest(latest)
            .last_modified(DateTime::from_millis(millis))
            .build(),
    )
}

fn marker(key: &str, id: &str, millis: i64, latest: bool) -> VersionOrDeleteMarker {
    VersionOrDeleteMarker::DeleteMarker(
        DeleteMarkerEntry::builder()
            .key(key)
            .version_id(id)
            .is_latest(latest)
            .last_modified(DateTime::from_millis(millis))
            .build(),
    )
}

/// `live`: v1, v2 (latest). `gone`: g1, g2, then a latest marker. `k`: v1, a marker, v2 (latest).
/// In the server's order (each key newest first), each entry at its document position.
fn page() -> Vec<ListedEntry> {
    let listed = vec![
        marker("gone", "gm", 30, true),
        version("gone", "g2", 20, false),
        version("gone", "g1", 10, false),
        version("k", "k2", 30, true),
        marker("k", "km", 20, false),
        version("k", "k1", 10, false),
        version("live", "l2", 20, true),
        version("live", "l1", 10, false),
    ];
    (0..)
        .zip(listed)
        .map(|(position, entry)| ListedEntry {
            position: Some(position),
            entry,
        })
        .collect()
}

/// `(version id, latest, delete marker, version count)` of every entry listed for `key`, in the
/// order sent.
fn rows(entries: &[Arc<EntryEnum>], key: &str) -> Vec<(String, bool, bool, Option<u32>)> {
    entries
        .iter()
        .filter(|entry| entry.get_relative_path().to_str() == Some(key))
        .map(|entry| {
            (
                entry.get_version_id().unwrap_or_default().to_string(),
                entry.get_is_latest(),
                entry.get_is_delete_marker(),
                entry.get_version_count(),
            )
        })
        .collect()
}

fn row(id: &str, latest: bool, deleted: bool, count: u32) -> (String, bool, bool, Option<u32>) {
    (id.to_string(), latest, deleted, Some(count))
}

/// What `walkdir` sends for `page` under the `match` / `exclude` expressions.
async fn walkdir_page(
    storage: &S3Storage,
    entries: Vec<ListedEntry>,
    filters: [Option<&FilterExpression>; 2],
) -> Result<Vec<Arc<EntryEnum>>, Box<dyn Error>> {
    let (tx, rx) = async_channel::unbounded();
    let scan = VersionedScan {
        tx: &tx,
        entries,
        include_tags: false,
        match_expressions: filters[0],
        exclude_expressions: filters[1],
        total_file_count: Arc::new(AtomicUsize::new(0)),
    };
    storage.process_versioned_entries(scan).await?;
    drop(tx);
    let mut sent = Vec::new();
    while let Ok(StorageEntryMessage::Scanned(entry)) = rx.recv().await {
        sent.push(entry);
    }
    Ok(sent)
}

/// `walkdir`: every version and marker of a deleted key, oldest first, the marker latest.
#[tokio::test]
async fn walkdir_lists_a_key_whose_newest_entry_is_a_delete_marker() -> TestResult {
    let storage = make_test_storage(None);
    let sent = walkdir_page(&storage, page(), [None, None]).await?;
    assert_eq!(
        rows(&sent, "gone"),
        [
            row("g1", false, false, 3),
            row("g2", false, false, 3),
            row("gm", true, true, 3)
        ]
    );
    assert_eq!(
        rows(&sent, "k"),
        [
            row("k1", false, false, 3),
            row("km", false, true, 3),
            row("k2", true, false, 3)
        ]
    );
    assert_eq!(
        rows(&sent, "live"),
        [row("l1", false, false, 2), row("l2", true, false, 2)]
    );
    assert_eq!(sent.len(), 8);
    Ok(())
}

/// `walkdir`: a key that holds only a delete marker sends that marker, latest.
#[tokio::test]
async fn walkdir_lists_a_key_holding_only_a_marker() -> TestResult {
    let storage = make_test_storage(None);
    let only = vec![ListedEntry {
        position: Some(0),
        entry: marker("ghost", "gm", 5, true),
    }];
    let sent = walkdir_page(&storage, only, [None, None]).await?;
    assert_eq!(rows(&sent, "ghost"), [row("gm", true, true, 1)]);
    Ok(())
}

/// `walkdir` judges a marker by `name` / `path` like its versions — an excluded deleted key sends
/// nothing, not its latest marker — but a `size` condition never hides it (it has no size).
#[tokio::test]
async fn walkdir_filters_markers_by_path_but_not_by_size() -> TestResult {
    let storage = make_test_storage(None);
    let exclude = parse_filter_expression("path == \"gone\"")?;
    let sent = walkdir_page(&storage, page(), [None, Some(&exclude)]).await?;
    assert!(rows(&sent, "gone").is_empty());
    assert_eq!(rows(&sent, "k").len(), 3);

    // Every version is 3 bytes: `size > 5` keeps none of them, only the markers.
    let include = parse_filter_expression("size > 5")?;
    let sent = walkdir_page(&storage, page(), [Some(&include), None]).await?;
    assert_eq!(rows(&sent, "gone"), [row("gm", true, true, 3)]);
    assert_eq!(rows(&sent, "k"), [row("km", false, true, 3)]);
    assert!(rows(&sent, "live").is_empty());
    Ok(())
}
