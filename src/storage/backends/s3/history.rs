//! Each key's history, oldest first (ADR-0006 C20 / C22; P7 review, existing issue 8).
//!
//! `ListObjectVersions` lists keys in ascending order and each key's versions and delete markers
//! newest first, as one sequence of `<Version>` and `<DeleteMarker>` elements. The SDK parses the
//! two element kinds into separate lists, so every listed entry carries its place in the server's
//! document order (`position`, read from the raw response — `crate::s3::version_order`), and
//! [`key_histories`] rebuilds each key's history from one whole listing (every page, so no key is
//! split):
//!
//! - **Server order** ([`HistoryOrder::Server`]): the key's entries by position, reversed — never
//!   `LastModified`, which has a millisecond resolution that concurrent writes share. Used when the
//!   listing passes the checks for the key: every entry has a position, the key's entries are
//!   contiguous among the entries passed in, and exactly one entry is the latest and it is listed
//!   first. The role-based listing passes the whole directory listing (every page), so a key
//!   split anywhere in it is caught; legacy `walkdir` / `walkdir_2` pass one batch of complete
//!   keys at a time (`crate::s3::version_listing`), so a key split by the page's held-back last
//!   key, or listed again after it was released, is not.
//! - **Inferred** ([`HistoryOrder::Inferred`]), in two cases, the latest entry last in both:
//!   - some entry of the key has no position (the response's document order could not be read,
//!     the store may still list correctly): each list — versions, delete markers — keeps its own
//!     listing order reversed, and `LastModified` only decides how the two interleave (a version
//!     before a marker of the same millisecond; an entry without a time where it is listed) — the
//!     merge used before the document order was read;
//!   - the key has positions but fails the checks (a store whose listing order is wrong): nothing
//!     the listing says about order is trusted; each list's listing order reversed only breaks
//!     ties of a stable sort by `LastModified` (an entry without one first).
//!
//!   Entries of one millisecond may be out of order.
//!
//! An entry the listing reported twice (a server repeating one across a page boundary) is kept
//! once, first as listed. The checks only see the entries passed in: an entry removed before
//! (another key's artifact, a directory's own marker, a key rolled up into a prefix) or one
//! without a position cannot reveal that a key was split around it — a limit that only matters for
//! a store whose listing is already wrong.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::S3ListedVersion;
use crate::model::{HistoryOrder, StorageTimestamp};

/// What ordering needs to know about one listed entry.
pub(crate) struct ListedFacts<'a> {
    pub key: &'a str,
    pub version_id: &'a str,
    pub delete_marker: bool,
    pub latest: bool,
    /// The entry's place in the server's document order of the whole listing (positions of later
    /// pages follow those of earlier ones); `None` when it is unknown.
    pub position: Option<u64>,
    /// `LastModified` in Unix nanoseconds.
    pub written: Option<i128>,
}

/// One key's history.
#[derive(Debug)]
pub(crate) struct KeyHistory<T> {
    pub key: String,
    pub order: HistoryOrder,
    /// Oldest first, the latest entry last.
    pub entries: Vec<T>,
}

/// Groups `entries` (a whole listing, each list in listing order) by key, keys in ascending byte
/// order, each key's entries oldest first.
pub(crate) fn key_histories<T>(
    entries: Vec<T>,
    facts: impl Fn(&T) -> ListedFacts<'_>,
) -> Vec<KeyHistory<T>> {
    let mut seen = HashSet::new();
    let mut grouped: BTreeMap<String, Vec<T>> = BTreeMap::new();
    for entry in entries {
        let listed = facts(&entry);
        let first = seen.insert((
            listed.key.to_string(),
            listed.version_id.to_string(),
            listed.delete_marker,
        ));
        if first {
            grouped
                .entry(listed.key.to_string())
                .or_default()
                .push(entry);
        }
    }
    let split = split_keys(&grouped, &facts);
    grouped
        .into_iter()
        .map(|(key, entries)| {
            let contiguous = !split.contains(&key);
            let positioned = entries.iter().all(|entry| facts(entry).position.is_some());
            let (order, sequence) = if positioned {
                server_order(&entries, contiguous, &facts).map_or_else(
                    || (HistoryOrder::Inferred, time_order(&entries, &facts)),
                    |sequence| (HistoryOrder::Server, sequence),
                )
            } else {
                (HistoryOrder::Inferred, merged_order(&entries, &facts))
            };
            KeyHistory {
                key,
                order,
                entries: permuted(entries, &sequence),
            }
        })
        .collect()
}

/// The keys whose positioned entries are not contiguous in the listing's document order: another
/// key's entry lies between two of theirs.
fn split_keys<T>(
    grouped: &BTreeMap<String, Vec<T>>,
    facts: &impl Fn(&T) -> ListedFacts<'_>,
) -> HashSet<String> {
    let mut placed: Vec<(u64, &str)> = grouped
        .iter()
        .flat_map(|(key, entries)| {
            entries
                .iter()
                .filter_map(|entry| facts(entry).position)
                .map(move |position| (position, key.as_str()))
        })
        .collect();
    placed.sort_unstable();
    let mut runs: HashMap<&str, usize> = HashMap::new();
    let mut previous: Option<&str> = None;
    for (_, key) in placed {
        if previous != Some(key) {
            *runs.entry(key).or_default() += 1;
        }
        previous = Some(key);
    }
    runs.into_iter()
        .filter(|(_, runs)| *runs > 1)
        .map(|(key, _)| key.to_string())
        .collect()
}

/// The key's entries oldest first by the server's order (indices into `entries`), when the
/// listing passes the checks for this key.
fn server_order<T>(
    entries: &[T],
    contiguous: bool,
    facts: &impl Fn(&T) -> ListedFacts<'_>,
) -> Option<Vec<usize>> {
    if !contiguous {
        return None;
    }
    let mut placed = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let listed = facts(entry);
        placed.push((listed.position?, listed.latest, index));
    }
    placed.sort_unstable();
    let (newest, older) = placed.split_first()?;
    if !newest.1 || older.iter().any(|(_, latest, _)| *latest) {
        return None;
    }
    Some(placed.iter().rev().map(|(_, _, index)| *index).collect())
}

/// The indices of the key's versions (`marker` false) or delete markers, listing order reversed.
fn listed_oldest_first<T>(
    entries: &[T],
    facts: &impl Fn(&T) -> ListedFacts<'_>,
    marker: bool,
) -> VecDeque<usize> {
    entries
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, entry)| facts(entry).delete_marker == marker)
        .map(|(index, _)| index)
        .collect()
}

/// A listing that failed the checks: the key's entries oldest first as `LastModified` says, the
/// latest last (indices into `entries`).
fn time_order<T>(entries: &[T], facts: &impl Fn(&T) -> ListedFacts<'_>) -> Vec<usize> {
    let mut order: Vec<usize> = listed_oldest_first(entries, facts, false)
        .into_iter()
        .chain(listed_oldest_first(entries, facts, true))
        .collect();
    order.sort_by_key(|index| {
        let listed = facts(&entries[*index]);
        (listed.latest, listed.written.unwrap_or(i128::MIN))
    });
    order
}

/// No document order: each list keeps its listing order reversed, `LastModified` only interleaves
/// the two, the latest last (indices into `entries`).
fn merged_order<T>(entries: &[T], facts: &impl Fn(&T) -> ListedFacts<'_>) -> Vec<usize> {
    let mut versions = listed_oldest_first(entries, facts, false);
    let mut markers = listed_oldest_first(entries, facts, true);
    let written = |index: usize| facts(&entries[index]).written.unwrap_or(i128::MIN);
    let latest = |index: usize| facts(&entries[index]).latest;
    let mut order = Vec::with_capacity(entries.len());
    loop {
        let take_version = match (versions.front(), markers.front()) {
            (None, None) => return order,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(&version), Some(&marker)) => {
                !latest(version) && (latest(marker) || written(version) <= written(marker))
            }
        };
        let next = if take_version {
            versions.pop_front()
        } else {
            markers.pop_front()
        };
        order.extend(next);
    }
}

/// `entries` rearranged as `order` (a permutation of their indices) says.
fn permuted<T>(entries: Vec<T>, order: &[usize]) -> Vec<T> {
    let mut slots: Vec<Option<T>> = entries.into_iter().map(Some).collect();
    order
        .iter()
        .filter_map(|index| slots.get_mut(*index).and_then(Option::take))
        .collect()
}

/// One warning per listing whose order had to be inferred for some key: a store that keeps failing
/// the checks (or whose responses cannot be read) must not degrade silently.
pub(crate) fn warn_inferred<T>(listed: &str, histories: &[KeyHistory<T>]) {
    let inferred = histories
        .iter()
        .filter(|history| history.order == HistoryOrder::Inferred)
        .count();
    if inferred > 0 {
        tracing::warn!(
            listing = %listed,
            keys = inferred,
            "S3 version histories ordered by LastModified: the server's listing order failed its checks or could not be read"
        );
    }
}

/// [`key_histories`] of one directory's listed versions and markers (every page; `position`s
/// already made listing-wide).
pub(super) fn oldest_first(
    versions: Vec<S3ListedVersion>,
    markers: Vec<S3ListedVersion>,
) -> Vec<KeyHistory<S3ListedVersion>> {
    let entries: Vec<S3ListedVersion> = versions.into_iter().chain(markers).collect();
    key_histories(entries, |entry| ListedFacts {
        key: &entry.key,
        version_id: &entry.facts.version_id,
        delete_marker: entry.facts.delete_marker,
        latest: entry.facts.is_latest,
        position: entry.position,
        written: entry.last_modified.map(StorageTimestamp::unix_nanos),
    })
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
