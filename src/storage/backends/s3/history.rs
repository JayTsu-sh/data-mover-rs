//! Each key's history, oldest first (ADR-0006 C20 / C22).
//!
//! `ListObjectVersions` lists keys in ascending order and each key's versions and delete markers
//! newest first, and the SDK parses versions and delete markers into two separate lists, losing
//! how they interleave. [`oldest_first`] rebuilds each key's history from one directory's whole
//! listing (every page, so no key can be split): grouped by key, the latest last.
//!
//! The listing's own order is authoritative within each list: a key's versions, reversed, are
//! oldest first, and so are its delete markers — whatever their `LastModified` says, which a
//! store whose nodes' clocks disagree can get wrong, or omit. `LastModified` only decides how the
//! two lists interleave: a version before a marker of the same millisecond (the listing cannot
//! tell those apart), and the latest entry, whichever list holds it, last. An entry the listing
//! reported twice (a server repeating one across a page boundary) is kept once, first as listed.

use std::collections::{BTreeMap, HashSet, VecDeque};

use super::S3ListedVersion;
use crate::model::StorageTimestamp;

fn written(entry: &S3ListedVersion) -> i128 {
    entry
        .last_modified
        .map_or(i128::MIN, StorageTimestamp::unix_nanos)
}

/// One key's versions and markers, each oldest first.
#[derive(Default)]
struct KeyHistory {
    versions: VecDeque<S3ListedVersion>,
    markers: VecDeque<S3ListedVersion>,
}

impl KeyHistory {
    /// Interleaves the two lists, each kept in its own order.
    fn merge(mut self) -> Vec<S3ListedVersion> {
        let mut merged = Vec::with_capacity(self.versions.len() + self.markers.len());
        loop {
            let take_version = match (self.versions.front(), self.markers.front()) {
                (None, None) => return merged,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(version), Some(marker)) => {
                    !version.facts.is_latest
                        && (marker.facts.is_latest || written(version) <= written(marker))
                }
            };
            let next = if take_version {
                self.versions.pop_front()
            } else {
                self.markers.pop_front()
            };
            merged.extend(next);
        }
    }
}

/// Groups `versions` and `markers` (each in listing order) by key, keys in ascending byte order,
/// each key's entries oldest first with the latest last.
pub(super) fn oldest_first(
    versions: Vec<S3ListedVersion>,
    markers: Vec<S3ListedVersion>,
) -> Vec<(String, Vec<S3ListedVersion>)> {
    let mut grouped: BTreeMap<String, KeyHistory> = BTreeMap::new();
    for (entries, marker) in [
        (first_listed(versions), false),
        (first_listed(markers), true),
    ] {
        for entry in entries.into_iter().rev() {
            let history = grouped.entry(entry.key.clone()).or_default();
            if marker {
                history.markers.push_back(entry);
            } else {
                history.versions.push_back(entry);
            }
        }
    }
    grouped
        .into_iter()
        .map(|(key, history)| (key, history.merge()))
        .collect()
}

/// `entries` without the repeats of an entry (same key and version id) listed twice.
fn first_listed(entries: Vec<S3ListedVersion>) -> Vec<S3ListedVersion> {
    let mut seen = HashSet::new();
    entries
        .into_iter()
        .filter(|entry| seen.insert((entry.key.clone(), entry.facts.version_id.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimePrecision;
    use crate::storage::backends::s3::S3VersionFacts;

    fn entry(
        key: &str,
        id: &str,
        millis: Option<i128>,
        latest: bool,
        marker: bool,
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
        }
    }

    fn ids(grouped: &[(String, Vec<S3ListedVersion>)], key: &str) -> Vec<String> {
        grouped
            .iter()
            .find(|(listed, _)| listed == key)
            .map(|(_, entries)| {
                entries
                    .iter()
                    .map(|entry| entry.facts.version_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn a_history_is_rebuilt_oldest_first_across_versions_and_markers() {
        let grouped = oldest_first(
            vec![
                entry("b", "b1", Some(5), true, false),
                entry("k", "v3", Some(40), true, false),
                entry("k", "v2", Some(20), false, false),
                entry("k", "v1", Some(10), false, false),
            ],
            vec![entry("k", "m", Some(30), false, true)],
        );
        let keys: Vec<&str> = grouped.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, ["b", "k"]);
        assert_eq!(ids(&grouped, "k"), ["v1", "v2", "m", "v3"]);
    }

    #[test]
    fn one_millisecond_keeps_the_listing_order_reversed_with_the_latest_last() {
        let grouped = oldest_first(
            vec![
                entry("k", "v3", Some(5), true, false),
                entry("k", "v2", Some(5), false, false),
                entry("k", "v1", Some(5), false, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "v2", "v3"]);
        let grouped = oldest_first(
            vec![entry("k", "v1", Some(5), false, false)],
            vec![entry("k", "m", Some(5), true, true)],
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "m"]);
        let grouped = oldest_first(
            vec![
                entry("k", "v2", Some(9), true, false),
                entry("k", "v1", Some(5), false, false),
            ],
            vec![entry("k", "m", Some(5), false, true)],
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "m", "v2"]);
    }

    /// Clock skew: the listing says v3 (latest), v2, v1, newest first, but the times disagree.
    /// The listing's order wins; times only interleave versions with markers.
    #[test]
    fn the_listing_order_wins_over_skewed_clocks() {
        let grouped = oldest_first(
            vec![
                entry("k", "v3", Some(30), true, false),
                entry("k", "v2", Some(10), false, false),
                entry("k", "v1", Some(20), false, false),
            ],
            vec![entry("k", "m", Some(25), false, true)],
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "v2", "m", "v3"]);
        let grouped = oldest_first(
            vec![
                entry("k", "v2", Some(10), true, false),
                entry("k", "v1", Some(20), false, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "v2"]);
    }

    /// Entries without a time keep their listed place.
    #[test]
    fn entries_without_a_time_keep_the_listing_order() {
        let grouped = oldest_first(
            vec![
                entry("k", "v3", Some(5), true, false),
                entry("k", "u2", None, false, false),
                entry("k", "u1", None, false, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped, "k"), ["u1", "u2", "v3"]);
        let grouped = oldest_first(
            vec![
                entry("k", "u2", None, true, false),
                entry("k", "v1", Some(5), false, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "u2"]);
    }

    /// A latest delete marker is last even if a version claims a later time.
    #[test]
    fn a_latest_marker_is_last() {
        let grouped = oldest_first(
            vec![entry("k", "v1", Some(50), false, false)],
            vec![entry("k", "m", Some(10), true, true)],
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "m"]);
    }

    /// An entry a server repeated across a page boundary is kept once.
    #[test]
    fn a_repeated_entry_is_kept_once() {
        let grouped = oldest_first(
            vec![
                entry("k", "v2", Some(20), true, false),
                entry("k", "v1", Some(10), false, false),
                entry("k", "v1", Some(10), false, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped, "k"), ["v1", "v2"]);
    }
}
