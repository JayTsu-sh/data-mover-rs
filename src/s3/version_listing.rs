//! One key's entries of the legacy versioned listing (`ListObjectVersions`), oldest first.
//!
//! The service lists keys in ascending order and each key's versions and delete markers newest
//! first, at most 1 000 entries a page, so a page can end inside a key: its newer entries on this
//! page, its older ones on the next. The SDK also parses versions and delete markers into two
//! separate lists, losing how they interleave. [`VersionPages`] holds back the key a page ended
//! in until the listing moves past it, and [`oldest_first_by_key`] puts each complete key's
//! entries in the order they were written.
//!
//! The key a page ended in is its greatest key, not `NextKeyMarker`: `MinIO` answers that with a
//! continuation token of its own (`p/z[minio_cache:v2,return:]` for the key `p/z`, measured on
//! RELEASE.2023-03-20), which only works as the next request's `key-marker`.

use std::collections::HashMap;
use std::mem;

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

use crate::time_util::combine_secs_nanos;

/// One entry of a key's history.
pub(super) enum VersionOrDeleteMarker {
    Version(ObjectVersion),
    DeleteMarker(DeleteMarkerEntry),
}

/// When an entry was written; an entry without `LastModified` (never seen in this listing) sorts
/// first rather than at "now", so the order does not depend on when it is computed.
fn written(time: Option<&DateTime>) -> i64 {
    time.map_or(i64::MIN, |time| {
        combine_secs_nanos(time.secs(), time.subsec_nanos())
    })
}

impl VersionOrDeleteMarker {
    /// When the entry was written, and last among entries written in the same instant if it
    /// is the latest: `LastModified` has a millisecond resolution.
    fn order(&self) -> (i64, bool) {
        match self {
            Self::Version(version) => (
                written(version.last_modified()),
                version.is_latest().unwrap_or(false),
            ),
            Self::DeleteMarker(marker) => (
                written(marker.last_modified()),
                marker.is_latest().unwrap_or(false),
            ),
        }
    }
}

/// The entries of the key the last page ended in, which may continue on the next page.
#[derive(Default)]
pub(super) struct VersionPages {
    key: Option<String>,
    versions: Vec<ObjectVersion>,
    markers: Vec<DeleteMarkerEntry>,
}

impl VersionPages {
    /// Takes one page's entries (in listing order) and returns those of every key the listing is
    /// done with: everything after the last page (`more_pages` false); otherwise everything
    /// except the page's greatest key — keys are listed in ascending order, so the page ended in
    /// it — which is held back and returned with a later page. Held entries come first: they are
    /// the newer ones, a key being listed newest first.
    pub(super) fn complete_keys(
        &mut self,
        versions: impl IntoIterator<Item = ObjectVersion>,
        markers: impl IntoIterator<Item = DeleteMarkerEntry>,
        more_pages: bool,
    ) -> (Vec<ObjectVersion>, Vec<DeleteMarkerEntry>) {
        let versions: Vec<_> = versions.into_iter().collect();
        let markers: Vec<_> = markers.into_iter().collect();
        let page_last = versions
            .iter()
            .filter_map(ObjectVersion::key)
            .chain(markers.iter().filter_map(DeleteMarkerEntry::key))
            .max()
            .map(str::to_string);
        let page_last = match page_last {
            Some(key) if more_pages => key,
            // The last page, or one without entries: nothing new to hold back.
            _ => {
                self.versions.extend(versions);
                self.markers.extend(markers);
                if more_pages {
                    return (Vec::new(), Vec::new());
                }
                self.key = None;
                return (mem::take(&mut self.versions), mem::take(&mut self.markers));
            }
        };
        if self.key.as_deref() == Some(page_last.as_str()) {
            // The whole page continues the held key: keep holding, without rescanning it.
            self.versions.extend(versions);
            self.markers.extend(markers);
            return (Vec::new(), Vec::new());
        }
        let (held_versions, page_versions): (Vec<_>, Vec<_>) = versions
            .into_iter()
            .partition(|version| version.key() == Some(page_last.as_str()));
        let (held_markers, page_markers): (Vec<_>, Vec<_>) = markers
            .into_iter()
            .partition(|marker| marker.key() == Some(page_last.as_str()));
        let mut done_versions = mem::replace(&mut self.versions, held_versions);
        let mut done_markers = mem::replace(&mut self.markers, held_markers);
        done_versions.extend(page_versions);
        done_markers.extend(page_markers);
        self.key = Some(page_last);
        (done_versions, done_markers)
    }
}

/// Groups complete keys' entries by key, each key oldest first, the latest entry last.
///
/// `versions` and `markers` are each in listing order (newest first per key). Entries written in
/// the same millisecond keep their listing order reversed, with the latest last; a version and
/// a delete marker written in the same millisecond, neither of them the latest, cannot be ordered
/// from the listing and are put version first.
pub(super) fn oldest_first_by_key(
    versions: Vec<ObjectVersion>,
    markers: Vec<DeleteMarkerEntry>,
) -> HashMap<String, Vec<VersionOrDeleteMarker>> {
    let mut grouped: HashMap<String, Vec<VersionOrDeleteMarker>> = HashMap::new();
    for version in versions.into_iter().rev() {
        if let Some(key) = version.key().map(str::to_string) {
            grouped
                .entry(key)
                .or_default()
                .push(VersionOrDeleteMarker::Version(version));
        }
    }
    for marker in markers.into_iter().rev() {
        if let Some(key) = marker.key().map(str::to_string) {
            grouped
                .entry(key)
                .or_default()
                .push(VersionOrDeleteMarker::DeleteMarker(marker));
        }
    }
    for entries in grouped.values_mut() {
        entries.sort_by_key(VersionOrDeleteMarker::order);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

    use super::{VersionOrDeleteMarker, VersionPages, oldest_first_by_key};

    fn version(key: &str, id: &str, millis: i64, latest: bool) -> ObjectVersion {
        ObjectVersion::builder()
            .key(key)
            .version_id(id)
            .is_latest(latest)
            .last_modified(DateTime::from_millis(millis))
            .build()
    }

    fn marker(key: &str, id: &str, millis: i64, latest: bool) -> DeleteMarkerEntry {
        DeleteMarkerEntry::builder()
            .key(key)
            .version_id(id)
            .is_latest(latest)
            .last_modified(DateTime::from_millis(millis))
            .build()
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

    #[test]
    fn a_key_split_across_pages_is_listed_once_oldest_first() {
        let mut pages = VersionPages::default();
        // Page 1 ends inside "z": its newest version and a marker; "a" is complete.
        let (versions, markers) = pages.complete_keys(
            vec![
                version("a", "a2", 20, true),
                version("a", "a1", 10, false),
                version("z", "z3", 300, true),
            ],
            vec![marker("z", "m", 250, false)],
            true,
        );
        let first = oldest_first_by_key(versions, markers);
        assert_eq!(first.len(), 1);
        assert_eq!(ids(&first["a"]), ["a1", "a2"]);

        // Page 2 (the last) holds the rest of "z".
        let (versions, markers) = pages.complete_keys(
            vec![
                version("z", "z2", 200, false),
                version("z", "z1", 100, false),
            ],
            Vec::new(),
            false,
        );
        let second = oldest_first_by_key(versions, markers);
        assert_eq!(second.len(), 1);
        assert_eq!(ids(&second["z"]), ["z1", "z2", "marker:m", "z3"]);
    }

    #[test]
    fn the_last_key_of_a_page_is_held_until_a_later_key_or_the_end() {
        let mut pages = VersionPages::default();
        // A page's last key may continue on the next page even if it ended at a boundary.
        let (versions, _) =
            pages.complete_keys(vec![version("a", "a1", 10, true)], Vec::new(), true);
        assert!(versions.is_empty());
        // The next page moves on to "b": "a" is complete, "b" is now held.
        let (versions, _) =
            pages.complete_keys(vec![version("b", "b1", 10, true)], Vec::new(), true);
        let grouped = oldest_first_by_key(versions, Vec::new());
        assert_eq!(grouped.len(), 1);
        assert_eq!(ids(&grouped["a"]), ["a1"]);
        // A page with no entries (only common prefixes) keeps holding it.
        let (versions, markers) = pages.complete_keys(Vec::new(), Vec::new(), true);
        assert!(versions.is_empty() && markers.is_empty());
        let (versions, _) = pages.complete_keys(Vec::new(), Vec::new(), false);
        let grouped = oldest_first_by_key(versions, Vec::new());
        assert_eq!(ids(&grouped["b"]), ["b1"]);
    }

    #[test]
    fn a_key_whose_newest_entries_are_markers_is_held_whole() {
        let mut pages = VersionPages::default();
        // Page 1 ends with the newest entry of "k", a delete marker.
        let (versions, markers) = pages.complete_keys(
            vec![version("a", "a1", 1, true)],
            vec![marker("k", "m", 30, true)],
            true,
        );
        assert_eq!(oldest_first_by_key(versions, markers).len(), 1);
        let (versions, markers) =
            pages.complete_keys(vec![version("k", "k1", 10, false)], Vec::new(), false);
        let grouped = oldest_first_by_key(versions, markers);
        assert_eq!(ids(&grouped["k"]), ["k1", "marker:m"]);
    }

    #[test]
    fn entries_of_one_millisecond_keep_the_listing_order_reversed_latest_last() {
        let grouped = oldest_first_by_key(
            vec![
                version("k", "v3", 5, true),
                version("k", "v2", 5, false),
                version("k", "v1", 5, false),
            ],
            Vec::new(),
        );
        assert_eq!(ids(&grouped["k"]), ["v1", "v2", "v3"]);

        // A latest delete marker written in the same millisecond as the version it hides.
        let grouped = oldest_first_by_key(
            vec![version("k", "v1", 5, false)],
            vec![marker("k", "m", 5, true)],
        );
        assert_eq!(ids(&grouped["k"]), ["v1", "marker:m"]);

        // Neither latest: the listing cannot tell, the version goes first.
        let grouped = oldest_first_by_key(
            vec![version("k", "v2", 9, true), version("k", "v1", 5, false)],
            vec![marker("k", "m", 5, false)],
        );
        assert_eq!(ids(&grouped["k"]), ["v1", "marker:m", "v2"]);
    }

    #[test]
    fn a_key_spanning_three_pages_is_released_once_whole() {
        let mut pages = VersionPages::default();
        let page = |ids: &[(&str, i64)]| {
            ids.iter()
                .map(|(id, millis)| version("k", id, *millis, *id == "k5"))
                .collect::<Vec<_>>()
        };
        let (versions, markers) =
            pages.complete_keys(page(&[("k5", 50), ("k4", 40)]), Vec::new(), true);
        assert!(versions.is_empty() && markers.is_empty());
        let (versions, markers) =
            pages.complete_keys(page(&[("k3", 30), ("k2", 20)]), Vec::new(), true);
        assert!(versions.is_empty() && markers.is_empty());
        let (versions, markers) = pages.complete_keys(
            page(&[("k1", 10)])
                .into_iter()
                .chain([version("l", "l1", 1, true)]),
            Vec::new(),
            true,
        );
        let grouped = oldest_first_by_key(versions, markers);
        assert_eq!(grouped.len(), 1);
        assert_eq!(ids(&grouped["k"]), ["k1", "k2", "k3", "k4", "k5"]);
        let (versions, markers) = pages.complete_keys(Vec::new(), Vec::new(), false);
        assert_eq!(ids(&oldest_first_by_key(versions, markers)["l"]), ["l1"]);
    }

    #[test]
    fn entries_without_a_time_sort_first_in_listing_order_reversed() {
        let undated = |id: &str| ObjectVersion::builder().key("k").version_id(id).build();
        let grouped = oldest_first_by_key(
            vec![version("k", "v3", 5, true), undated("u2"), undated("u1")],
            Vec::new(),
        );
        assert_eq!(ids(&grouped["k"]), ["u1", "u2", "v3"]);
    }
}
