use std::fmt::Write as _;

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

use super::{
    ListedEntry, VersionOrDeleteMarker, VersionPages, oldest_first_by_key, versions_with_latest,
};
use crate::model::HistoryOrder;
use crate::s3::version_order::{DocumentOrder, VersionsInOrder};
use crate::storage::backends::s3::KeyHistory;

fn version(key: &str, id: &str, millis: i64, latest: bool) -> VersionOrDeleteMarker {
    VersionOrDeleteMarker::Version(
        ObjectVersion::builder()
            .key(key)
            .version_id(id)
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

/// `entries` as listed from `first` on: each at its document position.
fn at(first: u64, entries: Vec<VersionOrDeleteMarker>) -> Vec<ListedEntry> {
    (first..)
        .zip(entries)
        .map(|(position, entry)| ListedEntry {
            position: Some(position),
            entry,
        })
        .collect()
}

/// `entries` of a response whose order could not be read.
fn unordered(entries: Vec<VersionOrDeleteMarker>) -> Vec<ListedEntry> {
    entries
        .into_iter()
        .map(|entry| ListedEntry {
            position: None,
            entry,
        })
        .collect()
}

fn ids(entries: &[VersionOrDeleteMarker]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| match entry {
            VersionOrDeleteMarker::Version(version) => {
                version.version_id().unwrap_or_default().to_string()
            }
            VersionOrDeleteMarker::DeleteMarker(marker) => {
                format!("marker:{}", marker.version_id().unwrap_or_default())
            }
        })
        .collect()
}

fn history<'a>(
    histories: &'a [KeyHistory<VersionOrDeleteMarker>],
    key: &str,
) -> &'a KeyHistory<VersionOrDeleteMarker> {
    histories
        .iter()
        .find(|history| history.key == key)
        .unwrap_or_else(|| panic!("no history for {key}"))
}

#[test]
fn a_key_split_across_pages_is_listed_once_oldest_first() {
    let mut pages = VersionPages::default();
    // Page 1 ends inside "z": its newest version and a marker; "a" is complete.
    let done = pages.complete_keys(
        at(
            0,
            vec![
                version("a", "a2", 20, true),
                version("a", "a1", 10, false),
                version("z", "z3", 300, true),
                marker("z", "m", 250, false),
            ],
        ),
        true,
    );
    let first = oldest_first_by_key(done);
    assert_eq!(first.len(), 1);
    assert_eq!(ids(&history(&first, "a").entries), ["a1", "a2"]);

    // Page 2 (the last) holds the rest of "z".
    let done = pages.complete_keys(
        at(
            4,
            vec![
                version("z", "z2", 200, false),
                version("z", "z1", 100, false),
            ],
        ),
        false,
    );
    let second = oldest_first_by_key(done);
    assert_eq!(second.len(), 1);
    let z = history(&second, "z");
    assert_eq!(ids(&z.entries), ["z1", "z2", "marker:m", "z3"]);
    assert_eq!(z.order, HistoryOrder::Server);
}

#[test]
fn the_last_key_of_a_page_is_held_until_a_later_key_or_the_end() {
    let mut pages = VersionPages::default();
    // A page's last key may continue on the next page even if it ended at a boundary.
    let done = pages.complete_keys(at(0, vec![version("a", "a1", 10, true)]), true);
    assert!(done.is_empty());
    // The next page moves on to "b": "a" is complete, "b" is now held.
    let done = pages.complete_keys(at(1, vec![version("b", "b1", 10, true)]), true);
    let grouped = oldest_first_by_key(done);
    assert_eq!(grouped.len(), 1);
    assert_eq!(ids(&history(&grouped, "a").entries), ["a1"]);
    // A page with no entries (only common prefixes) keeps holding it.
    assert!(pages.complete_keys(Vec::new(), true).is_empty());
    let grouped = oldest_first_by_key(pages.complete_keys(Vec::new(), false));
    assert_eq!(ids(&history(&grouped, "b").entries), ["b1"]);
}

#[test]
fn a_key_whose_newest_entries_are_markers_is_held_whole() {
    let mut pages = VersionPages::default();
    // Page 1 ends with the newest entry of "k", a delete marker.
    let done = pages.complete_keys(
        at(
            0,
            vec![version("a", "a1", 1, true), marker("k", "m", 30, true)],
        ),
        true,
    );
    assert_eq!(oldest_first_by_key(done).len(), 1);
    let done = pages.complete_keys(at(2, vec![version("k", "k1", 10, false)]), false);
    let grouped = oldest_first_by_key(done);
    assert_eq!(ids(&history(&grouped, "k").entries), ["k1", "marker:m"]);
}

/// Versions and markers of one millisecond interleave as the server listed them (P7 review,
/// existing issue 8): the time merge used before put the version first.
#[test]
fn entries_of_one_millisecond_keep_the_servers_order() {
    let grouped = oldest_first_by_key(at(
        0,
        vec![
            marker("k", "m2", 5, true),
            version("k", "v2", 5, false),
            marker("k", "m1", 5, false),
            version("k", "v1", 5, false),
        ],
    ));
    let k = history(&grouped, "k");
    assert_eq!(ids(&k.entries), ["v1", "marker:m1", "v2", "marker:m2"]);
    assert_eq!(k.order, HistoryOrder::Server);
}

#[test]
fn a_key_spanning_three_pages_is_released_once_whole() {
    let mut pages = VersionPages::default();
    let page = |ids: &[(&str, i64)]| {
        ids.iter()
            .map(|(id, millis)| version("k", id, *millis, *id == "k5"))
            .collect::<Vec<_>>()
    };
    assert!(
        pages
            .complete_keys(at(0, page(&[("k5", 50), ("k4", 40)])), true)
            .is_empty()
    );
    assert!(
        pages
            .complete_keys(at(2, page(&[("k3", 30), ("k2", 20)])), true)
            .is_empty()
    );
    let mut last = page(&[("k1", 10)]);
    last.push(version("l", "l1", 1, true));
    let done = pages.complete_keys(at(4, last), true);
    let grouped = oldest_first_by_key(done);
    assert_eq!(grouped.len(), 1);
    assert_eq!(
        ids(&history(&grouped, "k").entries),
        ["k1", "k2", "k3", "k4", "k5"]
    );
    let grouped = oldest_first_by_key(pages.complete_keys(Vec::new(), false));
    assert_eq!(ids(&history(&grouped, "l").entries), ["l1"]);
}

/// Without the document order (or when the listing breaks the checks) a key is ordered by
/// `LastModified` — entries of one millisecond in listing order reversed, a version before a
/// marker, the latest last; entries without a time first — and marked inferred.
#[test]
fn without_the_servers_order_a_key_is_inferred_by_time() {
    let grouped = oldest_first_by_key(unordered(vec![
        version("k", "v3", 5, true),
        version("k", "v2", 5, false),
        version("k", "v1", 5, false),
    ]));
    assert_eq!(ids(&history(&grouped, "k").entries), ["v1", "v2", "v3"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Inferred);
    let grouped = oldest_first_by_key(unordered(vec![
        version("k", "v2", 9, true),
        version("k", "v1", 5, false),
        marker("k", "m", 5, false),
    ]));
    assert_eq!(
        ids(&history(&grouped, "k").entries),
        ["v1", "marker:m", "v2"]
    );
    let undated = |id: &str| {
        VersionOrDeleteMarker::Version(ObjectVersion::builder().key("k").version_id(id).build())
    };
    let grouped = oldest_first_by_key(unordered(vec![
        version("k", "v3", 5, true),
        undated("u2"),
        undated("u1"),
    ]));
    assert_eq!(ids(&history(&grouped, "k").entries), ["u1", "u2", "v3"]);
}

/// A shuffled listing (latest not first) is not trusted: inferred by time, marked.
#[test]
fn a_shuffled_listing_falls_back_to_time() {
    let grouped = oldest_first_by_key(at(
        0,
        vec![
            version("k", "v1", 10, false),
            version("k", "v3", 30, true),
            marker("k", "m", 20, false),
            version("k", "v2", 15, false),
        ],
    ));
    let k = history(&grouped, "k");
    assert_eq!(k.order, HistoryOrder::Inferred);
    assert_eq!(ids(&k.entries), ["v1", "v2", "marker:m", "v3"]);
}

fn latest_flags(entries: Vec<VersionOrDeleteMarker>) -> Vec<(String, bool)> {
    versions_with_latest(entries)
        .into_iter()
        .map(|(version, latest)| (version.version_id().unwrap_or_default().to_string(), latest))
        .collect()
}

#[test]
fn versions_only_mark_the_newest_latest_unless_a_marker_is_newer() {
    let listed = |v2_latest: bool, newest_marker: Option<VersionOrDeleteMarker>| {
        let mut entries = Vec::new();
        entries.extend(newest_marker.clone().filter(|_| !v2_latest));
        entries.push(version("k", "v2", 20, v2_latest));
        entries.extend(newest_marker.filter(|_| v2_latest));
        entries.push(version("k", "v1", 10, false));
        oldest_first_by_key(at(0, entries))
            .into_iter()
            .next()
            .map(|history| history.entries)
            .unwrap_or_default()
    };
    // A marker between the versions: v2 is still the key's latest entry.
    assert_eq!(
        latest_flags(listed(true, Some(marker("k", "m", 15, false)))),
        [("v1".to_string(), false), ("v2".to_string(), true)]
    );
    // The newest entry is a marker (D4): both versions are listed, neither latest.
    assert_eq!(
        latest_flags(listed(false, Some(marker("k", "m", 30, true)))),
        [("v1".to_string(), false), ("v2".to_string(), false)]
    );
    // Only markers: no version to list.
    let only = oldest_first_by_key(at(0, vec![marker("k", "m", 30, true)]));
    assert!(
        latest_flags(
            only.into_iter()
                .flat_map(|history| history.entries)
                .collect()
        )
        .is_empty()
    );
}

/// One response page: `entries` as `(kind, key, id, latest)` in document order, parsed both ways.
fn response(entries: &[(&str, &str, &str, bool)]) -> VersionsInOrder {
    let mut xml = String::from("<ListVersionsResult>");
    let mut output =
        aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput::builder();
    for (kind, key, id, latest) in entries {
        let _ = write!(
            xml,
            "<{kind}><Key>{key}</Key><VersionId>{id}</VersionId><IsLatest>{latest}</IsLatest></{kind}>"
        );
        match (
            *kind,
            version(key, id, 7, *latest),
            marker(key, id, 7, *latest),
        ) {
            ("Version", VersionOrDeleteMarker::Version(v), _) => output = output.versions(v),
            (_, _, VersionOrDeleteMarker::DeleteMarker(m)) => output = output.delete_markers(m),
            _ => unreachable!("static test data"),
        }
    }
    xml.push_str("</ListVersionsResult>");
    let output = output.build();
    let order = DocumentOrder::read(xml.as_bytes(), &output);
    assert!(order.is_some(), "the test page must be readable");
    VersionsInOrder { output, order }
}

/// `walkdir` filters keys (artifacts, the prefix itself) before ordering: a filtered entry still
/// counts towards the listing-wide positions, and a key split across pages is ordered by the
/// server's order across the boundary — all entries here share one millisecond.
#[test]
fn positions_count_filtered_entries_and_run_across_pages() {
    let mut pages = VersionPages::default();
    let keep = |key: &str| key != ".data-mover-x";
    let first = response(&[
        ("Version", ".data-mover-x", "a1", true),
        ("DeleteMarker", "k", "km", true),
        ("Version", "k", "k3", false),
    ]);
    let entries = pages.positioned(&first, keep);
    // Versions first, then markers, each at its document position.
    assert_eq!(
        entries.iter().map(|e| e.position).collect::<Vec<_>>(),
        [Some(2), Some(1)]
    );
    assert!(pages.complete_keys(entries, true).is_empty());
    let second = response(&[("Version", "k", "k2", false), ("Version", "k", "k1", false)]);
    let entries = pages.positioned(&second, keep);
    assert_eq!(
        entries.iter().map(|e| e.position).collect::<Vec<_>>(),
        [Some(3), Some(4)]
    );
    let grouped = oldest_first_by_key(pages.complete_keys(entries, false));
    let k = history(&grouped, "k");
    assert_eq!(k.order, HistoryOrder::Server);
    assert_eq!(ids(&k.entries), ["k1", "k2", "k3", "marker:km"]);
}
