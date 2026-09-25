use super::*;
use crate::model::TimePrecision;
use crate::storage::backends::s3::S3VersionFacts;

/// An entry of `key` written at `millis`, listed at `position` (`None`: order unknown).
fn entry(
    key: &str,
    id: &str,
    millis: Option<i128>,
    latest: bool,
    marker: bool,
    position: Option<u64>,
) -> S3ListedVersion {
    S3ListedVersion {
        key: key.to_string(),
        facts: S3VersionFacts {
            version_id: id.to_string(),
            is_latest: latest,
            delete_marker: marker,
            size: u64::from(!marker),
            etag: String::new(),
        },
        last_modified: millis.map(|millis| {
            StorageTimestamp::new(millis * 1_000_000, TimePrecision::Milliseconds)
                .unwrap_or_else(|error| panic!("{error}"))
        }),
        position,
    }
}

/// `(key, id, millis, marker)` entries listed newest first per key, in this order: each gets its
/// document position, the first entry of each key is the latest. Split into the SDK's two lists.
fn listing(entries: &[(&str, &str, i128, bool)]) -> (Vec<S3ListedVersion>, Vec<S3ListedVersion>) {
    let mut previous_key = None;
    let (mut versions, mut markers) = (Vec::new(), Vec::new());
    for (position, (key, id, millis, marker)) in (0_u64..).zip(entries) {
        let latest = previous_key != Some(*key);
        previous_key = Some(*key);
        let listed = entry(key, id, Some(*millis), latest, *marker, Some(position));
        if *marker {
            markers.push(listed);
        } else {
            versions.push(listed);
        }
    }
    (versions, markers)
}

fn unpositioned(entries: Vec<S3ListedVersion>) -> Vec<S3ListedVersion> {
    entries
        .into_iter()
        .map(|mut entry| {
            entry.position = None;
            entry
        })
        .collect()
}

fn history<'a>(
    grouped: &'a [KeyHistory<S3ListedVersion>],
    key: &str,
) -> &'a KeyHistory<S3ListedVersion> {
    grouped
        .iter()
        .find(|history| history.key == key)
        .unwrap_or_else(|| panic!("no history for {key}"))
}

fn ids(grouped: &[KeyHistory<S3ListedVersion>], key: &str) -> Vec<String> {
    history(grouped, key)
        .entries
        .iter()
        .map(|entry| entry.facts.version_id.clone())
        .collect()
}

#[test]
fn a_history_is_the_servers_order_reversed() {
    let (versions, markers) = listing(&[
        ("b", "b1", 5, false),
        ("k", "v3", 40, false),
        ("k", "m", 30, true),
        ("k", "v2", 20, false),
        ("k", "v1", 10, false),
    ]);
    let grouped = oldest_first(versions, markers);
    let keys: Vec<&str> = grouped.iter().map(|history| history.key.as_str()).collect();
    assert_eq!(keys, ["b", "k"]);
    assert_eq!(ids(&grouped, "k"), ["v1", "v2", "m", "v3"]);
    assert!(grouped.iter().all(|h| h.order == HistoryOrder::Server));
}

/// Concurrent writes share milliseconds: versions and markers of one instant interleave as the
/// server listed them, which `LastModified` cannot tell (a time merge puts the version first).
#[test]
fn entries_of_one_millisecond_interleave_as_the_server_listed_them() {
    let (versions, markers) = listing(&[
        ("k", "m3", 7, true),
        ("k", "v3", 7, false),
        ("k", "m2", 7, true),
        ("k", "v2", 7, false),
        ("k", "v1", 7, false),
        ("k", "m1", 7, true),
    ]);
    let grouped = oldest_first(versions.clone(), markers.clone());
    assert_eq!(ids(&grouped, "k"), ["m1", "v1", "v2", "m2", "v3", "m3"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Server);
    // Without the document order the same listing can only be inferred, and comes out wrong.
    let inferred = oldest_first(unpositioned(versions), unpositioned(markers));
    assert_eq!(history(&inferred, "k").order, HistoryOrder::Inferred);
    assert_ne!(ids(&inferred, "k"), ["m1", "v1", "v2", "m2", "v3", "m3"]);
}

/// Clock skew between a store's nodes cannot reorder a history listed in the server's order.
#[test]
fn the_servers_order_wins_over_skewed_clocks() {
    let (versions, markers) = listing(&[
        ("k", "v3", 30, false),
        ("k", "m", 5, true),
        ("k", "v2", 10, false),
        ("k", "v1", 20, false),
    ]);
    let grouped = oldest_first(versions, markers);
    assert_eq!(ids(&grouped, "k"), ["v1", "v2", "m", "v3"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Server);
}

/// A key whose entries straddle page boundaries keeps one listing-wide order.
#[test]
fn a_key_straddling_pages_keeps_the_listing_wide_order() {
    // Positions 998..1003: pages of 1 000 end after position 999.
    let listed = [
        ("k", "v5", false, 998),
        ("k", "m2", true, 999),
        ("k", "v4", false, 1000),
        ("k", "v3", false, 1001),
        ("k", "m1", true, 1002),
        ("k", "v1", false, 1003),
    ];
    let entries: Vec<S3ListedVersion> = listed
        .iter()
        .map(|(key, id, marker, position)| {
            entry(key, id, Some(1), *id == "v5", *marker, Some(*position))
        })
        .collect();
    let (markers, versions): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|entry| entry.facts.delete_marker);
    let grouped = oldest_first(versions, markers);
    assert_eq!(ids(&grouped, "k"), ["v1", "m1", "v3", "v4", "m2", "v5"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Server);
}

/// A listing that breaks the checks for a key is not trusted for that key: its history is
/// inferred from `LastModified`, the latest last; other keys keep the server's order.
#[test]
fn a_non_conforming_listing_falls_back_to_time_and_is_marked_inferred() {
    // "k" is listed oldest first (latest last), "n" is split by "p", "q" has two latest entries,
    // "r" none; "a" conforms.
    let at = |key, id, millis, latest, marker, position| {
        entry(key, id, Some(millis), latest, marker, Some(position))
    };
    let versions = vec![
        at("a", "a1", 1, true, false, 0),
        at("k", "k1", 10, false, false, 1),
        at("k", "k3", 30, true, false, 3),
        at("n", "n2", 20, true, false, 4),
        at("p", "p1", 5, true, false, 5),
        at("n", "n1", 10, false, false, 6),
        at("q", "q2", 20, true, false, 7),
        at("q", "q1", 10, true, false, 8),
        at("r", "r2", 20, false, false, 9),
        at("r", "r1", 10, false, false, 10),
    ];
    let markers = vec![at("k", "km", 20, false, true, 2)];
    let grouped = oldest_first(versions, markers);
    assert_eq!(history(&grouped, "a").order, HistoryOrder::Server);
    assert_eq!(history(&grouped, "p").order, HistoryOrder::Server);
    for key in ["k", "n", "q", "r"] {
        assert_eq!(
            history(&grouped, key).order,
            HistoryOrder::Inferred,
            "{key}"
        );
    }
    assert_eq!(ids(&grouped, "k"), ["k1", "km", "k3"]);
    assert_eq!(ids(&grouped, "n"), ["n1", "n2"]);
    assert_eq!(ids(&grouped, "r"), ["r1", "r2"]);
}

/// A key some of whose entries have no position (the order of a page could not be read) is
/// inferred.
#[test]
fn a_key_without_positions_is_inferred() {
    let versions = vec![
        entry("k", "v2", Some(20), true, false, Some(0)),
        entry("k", "v1", Some(10), false, false, None),
    ];
    let grouped = oldest_first(versions, Vec::new());
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Inferred);
    assert_eq!(ids(&grouped, "k"), ["v1", "v2"]);
}

/// Without positions a store may still list correctly: each list keeps its own listing order
/// (clock skew cannot reorder it), `LastModified` only interleaves versions with markers.
#[test]
fn without_positions_each_lists_order_is_kept_and_time_interleaves() {
    let grouped = oldest_first(
        vec![
            entry("k", "v3", Some(30), true, false, None),
            entry("k", "v2", Some(10), false, false, None),
            entry("k", "v1", Some(20), false, false, None),
        ],
        vec![entry("k", "m", Some(25), false, true, None)],
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "v2", "m", "v3"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Inferred);
}

/// A listing that fails the checks is not trusted even within one list: pure time order.
#[test]
fn a_listing_failing_the_checks_is_ordered_by_time_alone() {
    // Listed oldest first (the latest last): the checks fail, v1 and v2 are put by their times.
    let grouped = oldest_first(
        vec![
            entry("k", "v2", Some(10), false, false, Some(0)),
            entry("k", "v1", Some(5), false, false, Some(1)),
            entry("k", "v3", Some(30), true, false, Some(2)),
        ],
        Vec::new(),
    );
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Inferred);
    assert_eq!(ids(&grouped, "k"), ["v1", "v2", "v3"]);
}

/// The inferred order without positions: `LastModified`, entries of one millisecond in listing
/// order reversed (each list's own), a version before a marker of the same millisecond, the latest
/// last.
#[test]
fn the_inferred_order_is_by_time_with_the_latest_last() {
    let grouped = oldest_first(
        vec![
            entry("k", "v3", Some(5), true, false, None),
            entry("k", "v2", Some(5), false, false, None),
            entry("k", "v1", Some(5), false, false, None),
        ],
        Vec::new(),
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "v2", "v3"]);
    let grouped = oldest_first(
        vec![entry("k", "v1", Some(5), false, false, None)],
        vec![entry("k", "m", Some(5), true, true, None)],
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "m"]);
    let grouped = oldest_first(
        vec![
            entry("k", "v2", Some(9), true, false, None),
            entry("k", "v1", Some(5), false, false, None),
        ],
        vec![entry("k", "m", Some(5), false, true, None)],
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "m", "v2"]);
    // A latest marker is last even if a version claims a later time.
    let grouped = oldest_first(
        vec![entry("k", "v1", Some(50), false, false, None)],
        vec![entry("k", "m", Some(10), true, true, None)],
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "m"]);
    // Entries without a time come first, in listing order reversed.
    let grouped = oldest_first(
        vec![
            entry("k", "v3", Some(5), true, false, None),
            entry("k", "u2", None, false, false, None),
            entry("k", "u1", None, false, false, None),
        ],
        Vec::new(),
    );
    assert_eq!(ids(&grouped, "k"), ["u1", "u2", "v3"]);
    assert!(grouped.iter().all(|h| h.order == HistoryOrder::Inferred));
}

/// An entry a server repeated across a page boundary is kept once, and does not make its key look
/// split.
#[test]
fn a_repeated_entry_is_kept_once() {
    let grouped = oldest_first(
        vec![
            entry("k", "v2", Some(20), true, false, Some(0)),
            entry("k", "v1", Some(10), false, false, Some(1)),
            entry("k", "v1", Some(10), false, false, Some(2)),
        ],
        Vec::new(),
    );
    assert_eq!(ids(&grouped, "k"), ["v1", "v2"]);
    assert_eq!(history(&grouped, "k").order, HistoryOrder::Server);
}
