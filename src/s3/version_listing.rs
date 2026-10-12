//! One key's entries of the legacy versioned listing (`ListObjectVersions`), oldest first.
//!
//! The service lists keys in ascending order and each key's versions and delete markers newest
//! first, as one sequence of elements, at most 1 000 a page, so a page can end inside a key: its
//! newer entries on this page, its older ones on the next. The SDK parses versions and delete
//! markers into two separate lists; [`VersionPages::positioned`] gives every entry its place in
//! the server's document order (`crate::s3::version_order`), numbered across the whole listing.
//! [`VersionPages::complete_keys`] holds back the key a page ended in until the listing moves past
//! it, and [`oldest_first_by_key`] orders each complete key by the server's order — or, when the
//! listing fails the checks for that key, by `LastModified`, marked inferred — the shared rules of
//! `storage::backends::s3::history`, which the role-based traversal uses too.
//!
//! A key whose newest entry is a delete marker is listed too (ADR-0006 D4): `walkdir` sends every
//! version and marker, the marker last and latest.
//!
//! The key a page ended in is its greatest key, not `NextKeyMarker`: `MinIO` answers that with a
//! continuation token of its own (`p/z[minio_cache:v2,return:]` for the key `p/z`, measured on
//! RELEASE.2023-03-20), which only works as the next request's `key-marker`.

use std::mem;

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

use super::version_order::VersionsInOrder;
use crate::storage::backends::s3::{KeyHistory, ListedFacts, key_histories, warn_inferred};
use crate::time_util::combine_secs_nanos;

/// One entry of a key's history.
#[derive(Clone, Debug)]
pub(super) enum VersionOrDeleteMarker {
    Version(ObjectVersion),
    DeleteMarker(DeleteMarkerEntry),
}

impl VersionOrDeleteMarker {
    fn key(&self) -> Option<&str> {
        match self {
            Self::Version(version) => version.key(),
            Self::DeleteMarker(marker) => marker.key(),
        }
    }
}

/// One listed entry with its place in the server's document order of the whole listing.
#[derive(Clone, Debug)]
pub(super) struct ListedEntry {
    pub(super) position: Option<u64>,
    pub(super) entry: VersionOrDeleteMarker,
}

impl ListedEntry {
    fn facts(&self) -> ListedFacts<'_> {
        let (key, version_id, latest, time, delete_marker) = match &self.entry {
            VersionOrDeleteMarker::Version(version) => (
                version.key(),
                version.version_id(),
                version.is_latest(),
                version.last_modified(),
                false,
            ),
            VersionOrDeleteMarker::DeleteMarker(marker) => (
                marker.key(),
                marker.version_id(),
                marker.is_latest(),
                marker.last_modified(),
                true,
            ),
        };
        ListedFacts {
            key: key.unwrap_or_default(),
            version_id: version_id.unwrap_or("null"),
            delete_marker,
            latest: latest.unwrap_or(false),
            position: self.position,
            written: time.map(written),
        }
    }
}

fn written(time: &DateTime) -> i128 {
    i128::from(combine_secs_nanos(time.secs(), time.subsec_nanos()))
}

/// The entries of the key the last page ended in, which may continue on the next page, and how
/// many entries the listing's earlier pages held.
#[derive(Default)]
pub(super) struct VersionPages {
    key: Option<String>,
    held: Vec<ListedEntry>,
    listed_before: u64,
}

impl VersionPages {
    /// The entries of one page whose key `keep` accepts, each with its listing-wide position:
    /// versions first, then delete markers, each list in listing order. An entry without a key is
    /// dropped.
    pub(super) fn positioned(
        &mut self,
        page: &VersionsInOrder,
        keep: impl Fn(&str) -> bool,
    ) -> Vec<ListedEntry> {
        let before = self.listed_before;
        let output = &page.output;
        let at = |position: Option<u64>| position.map(|position| before + position);
        let order = page.order.as_ref();
        let versions = output
            .versions()
            .iter()
            .enumerate()
            .map(|(index, version)| ListedEntry {
                position: at(order.and_then(|order| order.version(index))),
                entry: VersionOrDeleteMarker::Version(version.clone()),
            });
        let markers = output
            .delete_markers()
            .iter()
            .enumerate()
            .map(|(index, marker)| ListedEntry {
                position: at(order.and_then(|order| order.marker(index))),
                entry: VersionOrDeleteMarker::DeleteMarker(marker.clone()),
            });
        let entries = versions
            .chain(markers)
            .filter(|entry| entry.entry.key().is_some_and(&keep))
            .collect();
        self.listed_before += (output.versions().len() + output.delete_markers().len()) as u64;
        entries
    }

    /// Takes one page's entries (in listing order) and returns those of every key the listing is
    /// done with: everything after the last page (`more_pages` false); otherwise everything
    /// except the page's greatest key — keys are listed in ascending order, so the page ended in
    /// it — which is held back and returned with a later page.
    pub(super) fn complete_keys(
        &mut self,
        entries: Vec<ListedEntry>,
        more_pages: bool,
    ) -> Vec<ListedEntry> {
        let page_last = entries
            .iter()
            .filter_map(|entry| entry.entry.key())
            .max()
            .map(str::to_string);
        let page_last = match page_last {
            Some(key) if more_pages => key,
            // The last page, or one without entries: nothing new to hold back.
            _ => {
                self.held.extend(entries);
                if more_pages {
                    return Vec::new();
                }
                self.key = None;
                return mem::take(&mut self.held);
            }
        };
        if self.key.as_deref() == Some(page_last.as_str()) {
            // The whole page continues the held key: keep holding, without rescanning it.
            self.held.extend(entries);
            return Vec::new();
        }
        let (held, page): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|entry| entry.entry.key() == Some(page_last.as_str()));
        let mut done = mem::replace(&mut self.held, held);
        done.extend(page);
        self.key = Some(page_last);
        done
    }
}

/// Groups complete keys' entries by key (keys ascending), each key oldest first, the latest entry
/// last: by the server's document order when the listing passes the checks for the key,
/// otherwise by `LastModified` with the history marked [`crate::model::HistoryOrder::Inferred`].
/// The contiguity check sees only this batch: a key split by the page's held-back last key, or
/// listed again after an earlier batch released it, is not caught (only a store that lists keys
/// out of order does either; the role-based listing checks the whole directory).
pub(super) fn oldest_first_by_key(
    entries: Vec<ListedEntry>,
) -> Vec<KeyHistory<VersionOrDeleteMarker>> {
    let histories = key_histories(entries, ListedEntry::facts);
    warn_inferred("legacy ListObjectVersions page", &histories);
    histories
        .into_iter()
        .map(|history| KeyHistory {
            key: history.key,
            order: history.order,
            entries: history
                .entries
                .into_iter()
                .map(|listed| listed.entry)
                .collect(),
        })
        .collect()
}

#[cfg(test)]
#[path = "version_listing_tests.rs"]
mod tests;
