//! Delimiter listings of [`MemoryS3`] (ADR-0006 C22): `ListObjectsV2` and `ListObjectVersions`
//! with delimiter `/`, as S3 pages them.
//!
//! - Every element — an object, one version or delete marker, a common prefix — counts towards
//!   the page size ([`ListingKnobs::page_size`], 1000 by default), so a page can end inside a
//!   key's versions.
//! - A versions page continues from (key marker, version id marker). With
//!   [`ListingKnobs::minio_markers`] the key marker is spelled as `MinIO` spells it,
//!   `<key>[minio_cache:v2,return:]`, which is not a key: a lister that compares it with keys or
//!   edits it loses entries.
//! - Version `n` of a key (oldest is 1) was written at `n` seconds, or all at one instant with
//!   [`ListingKnobs::same_instant`]. An unversioned bucket lists every object as version `"null"`.
//! - A store that keeps no order ([`ListingKnobs::unordered`], as ONTAP S3 with deduplication or
//!   compression does) lists the keys of a `ListObjectsV2` in a scrambled order, so a common
//!   prefix comes again wherever another of its keys falls, on any page; its continuation is a
//!   position, not a key. [`ListingKnobs::disordered_prefixes`] returns each page's common prefixes
//!   reversed, twice, and again on the next page; [`ListingKnobs::repeat_last_object`] starts each
//!   page with the previous page's last object; [`ListingKnobs::leading_empty_pages`] starts each
//!   listing with empty pages that claim more follow.

use std::future::pending;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::task::yield_now;
use tokio::time::sleep;

use super::MemoryS3;
use crate::model::{StorageTimestamp, TimePrecision};
use crate::storage::backends::s3::{
    S3ListedObject, S3ListedVersion, S3ObjectPage, S3ProtocolFailure, S3RejectedEntry, S3Result,
    S3VersionFacts, S3VersionMarker, S3VersionPage,
};

const MINIO_SUFFIX: &str = "[minio_cache:v2,return:]";

/// How the fake pages its listings, and what it records about them.
/// Independent test switches, one per server behaviour.
#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
pub(crate) struct ListingKnobs {
    /// Elements per page; 0 means 1000.
    pub(crate) page_size: usize,
    pub(crate) minio_markers: bool,
    pub(crate) same_instant: bool,
    /// List every key under the prefix flat, as a store that ignores the delimiter does.
    pub(crate) ignore_delimiter: bool,
    /// A non-conforming store: each key's versions are listed oldest first (the latest last)
    /// instead of newest first.
    pub(crate) oldest_first_versions: bool,
    /// A broken server: every page is truncated and its continuation cycles through this many
    /// tokens (0: off).
    pub(crate) cycle_tokens: usize,
    /// Pages served while `cycle_tokens` or `endless_empty` is on.
    cycled_pages: usize,
    /// A broken server: every page is empty yet names a new continuation.
    pub(crate) endless_empty: bool,
    /// A broken server: listing this prefix ignores the continuation and serves the first page
    /// again, so the first page's continuation comes back on the second.
    pub(crate) restart_prefix: Option<String>,
    /// Each page answers after this long.
    pub(crate) page_delay: Option<Duration>,
    /// Listing this prefix fails with this failure.
    pub(crate) failure: Option<(String, S3ProtocolFailure)>,
    /// The next this many page requests, of any prefix, fail with this failure (a throttling
    /// service answers 503 `SlowDown` for a while) — once `throttled_after` requests were served.
    pub(crate) throttled: Option<(usize, S3ProtocolFailure)>,
    /// Page requests served before `throttled` starts failing them (0: from the first).
    pub(crate) throttled_after: usize,
    /// Page requests (0-based, any prefix) that fail with the failure given, whatever they ask.
    pub(crate) failing_calls: Vec<(usize, S3ProtocolFailure)>,
    /// Page requests from this one on (0-based, any prefix) never answer.
    pub(crate) hang_from_call: Option<usize>,
    /// Page requests waiting on `hang_from_call` right now; a dropped request leaves.
    pub(crate) hanging: Arc<AtomicUsize>,
    /// A store keeping no order: a `ListObjectsV2` lists its keys scrambled (the same way every
    /// time), each key below a deeper `/` as its common prefix where the key falls, and continues
    /// from a position.
    pub(crate) unordered: bool,
    /// Each `ListObjectsV2` starts with this many empty pages that name a continuation.
    pub(crate) leading_empty_pages: usize,
    /// A store keeping no order: each object page's common prefixes come reversed and twice, and
    /// the previous page's again. Remembered across calls of any prefix: for a test that runs one
    /// listing at a time (as `repeat_last_object`).
    pub(crate) disordered_prefixes: bool,
    /// Common prefixes of the previous object page, for `disordered_prefixes`.
    last_prefixes: Vec<String>,
    /// A store repeating an object across a page boundary: each object page after the first
    /// starts with the previous page's last object again.
    pub(crate) repeat_last_object: bool,
    /// The last object of the previous object page, for `repeat_last_object`.
    last_object: Option<S3ListedObject>,
    /// The continuation of every `ListObjectsV2` page requested, throttled and failed ones
    /// included, in order.
    pub(crate) tokens: Vec<Option<String>>,
    /// Elements the first page of this prefix could not decode.
    pub(crate) rejected: Vec<(String, S3RejectedEntry)>,
    /// Objects the first page of this prefix lists besides its own, keyed as given (a server
    /// that ignores the prefix lists keys outside it).
    pub(crate) stray: Vec<(String, S3ListedObject)>,
    /// The prefix of every listing page requested.
    pub(crate) calls: Vec<String>,
}

/// One element of a listing, in S3 order.
enum Element {
    Prefix(String),
    Object(S3ListedObject),
    Version(S3ListedVersion),
}

impl Element {
    /// (key, version id) as a continuation marker names it.
    fn position(&self) -> (&str, Option<&str>) {
        match self {
            Self::Prefix(prefix) => (prefix, None),
            Self::Object(object) => (&object.key, None),
            Self::Version(version) => (&version.key, Some(&version.facts.version_id)),
        }
    }
}

fn instant(seconds: i128) -> Option<StorageTimestamp> {
    StorageTimestamp::new(seconds * 1_000_000_000, TimePrecision::Milliseconds).ok()
}

impl MemoryS3 {
    pub(crate) fn listing(&self) -> MutexGuard<'_, ListingKnobs> {
        self.listing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the call, waits the page delay, applies an injected failure, and returns the page
    /// size.
    async fn listing_call(&self, prefix: &str) -> S3Result<usize> {
        let delay = self.listing().page_delay;
        match delay {
            Some(delay) => sleep(delay).await,
            // A real request is never ready at once; yielding lets a test's timeout fire on a
            // listing that pages forever.
            None => yield_now().await,
        }
        let hang = {
            let mut knobs = self.listing();
            knobs.calls.push(prefix.to_string());
            let call = knobs.calls.len() - 1;
            knobs
                .hang_from_call
                .is_some_and(|from| call >= from)
                .then(|| Arc::clone(&knobs.hanging))
        };
        if let Some(hanging) = hang {
            let _waiting = Waiting::new(hanging);
            pending::<()>().await;
        }
        let mut knobs = self.listing();
        if let Some((failed, failure)) = &knobs.failure
            && failed == prefix
        {
            return Err(failure.clone());
        }
        let served = knobs.calls.len() - 1;
        if let Some((_, failure)) = knobs.failing_calls.iter().find(|(call, _)| *call == served) {
            return Err(failure.clone());
        }
        let after = knobs.throttled_after;
        if let Some((remaining, failure)) = knobs.throttled.as_mut()
            && *remaining > 0
            && served >= after
        {
            *remaining -= 1;
            return Err(failure.clone());
        }
        Ok(if knobs.page_size == 0 {
            1000
        } else {
            knobs.page_size
        })
    }

    /// An empty page with a new token, from an endlessly empty server.
    fn endless_page(&self) -> Option<String> {
        let mut knobs = self.listing();
        if !knobs.endless_empty {
            return None;
        }
        knobs.cycled_pages += 1;
        Some(format!("empty:{}", knobs.cycled_pages))
    }

    /// Whether listing `prefix` ignores its continuation.
    fn restarts(&self, prefix: &str) -> bool {
        self.listing().restart_prefix.as_deref() == Some(prefix)
    }

    /// The next token of a cycling server, if the fake is one.
    fn cycled_token(&self) -> Option<String> {
        let mut knobs = self.listing();
        if knobs.cycle_tokens == 0 {
            return None;
        }
        knobs.cycled_pages += 1;
        Some(format!("cycle:{}", knobs.cycled_pages % knobs.cycle_tokens))
    }

    /// Every key under `prefix` (current objects, and keys with recorded versions), sorted.
    async fn keys_under(&self, prefix: &str, with_history: bool) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().await.keys().cloned().collect();
        if with_history {
            keys.extend(self.keys_with_versions());
        }
        keys.retain(|key| key.starts_with(prefix));
        keys.sort();
        keys.dedup();
        keys
    }

    /// `key` rolled up to its first-level prefix under `prefix`, if it lies deeper and the fake
    /// honours the delimiter.
    fn rolled_up(&self, prefix: &str, key: &str) -> Option<String> {
        if self.listing().ignore_delimiter {
            return None;
        }
        let rest = key.get(prefix.len()..)?;
        rest.find('/')
            .map(|end| format!("{prefix}{}", &rest[..=end]))
    }

    async fn object_elements(&self, prefix: &str, unordered: bool) -> Vec<Element> {
        let last_modified = *self.last_modified.lock().await;
        let objects = self.objects.lock().await.clone();
        let mut keys = self.keys_under(prefix, false).await;
        if unordered {
            keys.sort_by_cached_key(|key| scrambled(key));
        }
        let mut elements = Vec::new();
        for key in keys {
            if let Some(common) = self.rolled_up(prefix, &key) {
                let repeated =
                    matches!(elements.last(), Some(Element::Prefix(last)) if *last == common);
                if unordered || !repeated {
                    elements.push(Element::Prefix(common));
                }
                continue;
            }
            let bytes: Bytes = objects.get(&key).cloned().unwrap_or_default();
            elements.push(Element::Object(S3ListedObject {
                size: bytes.len() as u64,
                etag: self.etag_for(&bytes),
                key,
                last_modified,
            }));
        }
        elements
    }

    async fn version_elements(&self, prefix: &str) -> S3Result<Vec<Element>> {
        let (same_instant, oldest_first) = {
            let knobs = self.listing();
            (knobs.same_instant, knobs.oldest_first_versions)
        };
        let mut elements = Vec::new();
        for key in self.keys_under(prefix, true).await {
            if let Some(common) = self.rolled_up(prefix, &key) {
                if !matches!(elements.last(), Some(Element::Prefix(last)) if *last == common) {
                    elements.push(Element::Prefix(common));
                }
                continue;
            }
            let history: Vec<S3VersionFacts> = self.versions_of(&key).await?;
            let count = history.len();
            let mut listed: Vec<Element> = history
                .into_iter()
                .enumerate()
                .map(|(newest_first, facts)| {
                    let written = if same_instant {
                        1
                    } else {
                        count - newest_first
                    };
                    Element::Version(S3ListedVersion {
                        key: key.clone(),
                        facts,
                        last_modified: instant(i128::try_from(written).unwrap_or(i128::MAX)),
                        position: None,
                    })
                })
                .collect();
            if oldest_first {
                listed.reverse();
            }
            elements.extend(listed);
        }
        Ok(elements)
    }

    /// The elements of one page starting after `after`, and whether more follow.
    fn page_of(
        elements: Vec<Element>,
        after: Option<(&str, Option<&str>)>,
        size: usize,
    ) -> (Vec<Element>, bool) {
        let start = after.map_or(0, |after| {
            elements
                .iter()
                .position(|element| element.position() == after)
                .map_or(elements.len(), |index| index + 1)
        });
        let more = elements.len() > start + size;
        (elements.into_iter().skip(start).take(size).collect(), more)
    }

    pub(super) async fn list_objects_page_in_memory(
        &self,
        prefix: &str,
        token: Option<&str>,
    ) -> S3Result<S3ObjectPage> {
        let (unordered, leading) = {
            let mut knobs = self.listing();
            knobs.tokens.push(token.map(str::to_owned));
            (knobs.unordered, knobs.leading_empty_pages)
        };
        let size = self.listing_call(prefix).await?;
        let lead = lead_in(token, leading);
        if let Some(next) = self.endless_page().or_else(|| lead.clone().err()) {
            return Ok(S3ObjectPage {
                next: Some(next),
                ..S3ObjectPage::default()
            });
        }
        let token = lead.ok().flatten().filter(|_| !self.restarts(prefix));
        let elements = self.object_elements(prefix, unordered).await;
        let (page, next) = if unordered {
            positional_page(elements, token, size)
        } else {
            keyed_page(elements, token, size)
        };
        let mut listed = S3ObjectPage {
            next: self.cycled_token().or(next),
            ..S3ObjectPage::default()
        };
        for element in page {
            match element {
                Element::Prefix(prefix) => listed.prefixes.push(prefix),
                Element::Object(object) => listed.objects.push(object),
                Element::Version(_) => {}
            }
        }
        if token.is_none() {
            let mut knobs = self.listing();
            listed
                .rejected
                .extend(injected(&mut knobs.rejected, prefix));
            listed.objects.extend(injected(&mut knobs.stray, prefix));
        }
        self.misbehave(&mut listed, token.is_none());
        Ok(listed)
    }

    /// What a store that keeps no order, or repeats an object across pages, does to `page`.
    fn misbehave(&self, page: &mut S3ObjectPage, first: bool) {
        let mut knobs = self.listing();
        if knobs.disordered_prefixes {
            let own = page.prefixes.clone();
            let mut served: Vec<String> = own.iter().rev().cloned().collect();
            served.extend(own.iter().cloned());
            if !first {
                served.extend(mem::take(&mut knobs.last_prefixes));
            }
            knobs.last_prefixes = own;
            page.prefixes = served;
        }
        if knobs.repeat_last_object {
            let last = page.objects.last().cloned();
            if let Some(repeated) = knobs.last_object.take().filter(|_| !first) {
                page.objects.insert(0, repeated);
            }
            knobs.last_object = last;
        }
    }

    pub(super) async fn list_versions_page_in_memory(
        &self,
        prefix: &str,
        marker: Option<&S3VersionMarker>,
    ) -> S3Result<S3VersionPage> {
        let size = self.listing_call(prefix).await?;
        if let Some(key_marker) = self.endless_page() {
            return Ok(S3VersionPage {
                next: Some(S3VersionMarker {
                    key_marker,
                    version_id_marker: None,
                }),
                ..S3VersionPage::default()
            });
        }
        let minio = self.listing().minio_markers;
        let elements = self.version_elements(prefix).await?;
        let marker = marker.filter(|_| !self.restarts(prefix));
        let after = marker.map(|marker| {
            (
                marker
                    .key_marker
                    .strip_suffix(MINIO_SUFFIX)
                    .unwrap_or(&marker.key_marker),
                marker.version_id_marker.as_deref(),
            )
        });
        let (page, more) = Self::page_of(elements, after, size);
        let next = more
            .then(|| page.last().map(|last| next_marker(last, minio)))
            .flatten();
        let cycled = self.cycled_token().map(|key_marker| S3VersionMarker {
            key_marker,
            version_id_marker: None,
        });
        let mut listed = S3VersionPage {
            next: cycled.or(next),
            ..S3VersionPage::default()
        };
        // The page's document order: versions and markers counted together, as S3 sends them.
        let mut position = 0;
        for element in page {
            match element {
                Element::Prefix(prefix) => listed.prefixes.push(prefix),
                Element::Version(mut version) => {
                    version.position = Some(position);
                    position += 1;
                    if version.facts.delete_marker {
                        listed.markers.push(version);
                    } else {
                        listed.versions.push(version);
                    }
                }
                Element::Object(_) => {}
            }
        }
        if marker.is_none() {
            listed
                .rejected
                .extend(injected(&mut self.listing().rejected, prefix));
        }
        Ok(listed)
    }
}

/// Where a key falls in an unordered store's listing: the same place every time.
fn scrambled(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// The page of `elements` at the position `token` names (`at:<index>`; the start without one),
/// and the continuation after it when more follow.
fn positional_page(
    elements: Vec<Element>,
    token: Option<&str>,
    size: usize,
) -> (Vec<Element>, Option<String>) {
    let start = token
        .and_then(|token| token.strip_prefix("at:"))
        .and_then(|index| index.parse::<usize>().ok())
        .unwrap_or(0);
    let more = elements.len() > start + size;
    let page = elements.into_iter().skip(start).take(size).collect();
    (page, more.then(|| format!("at:{}", start + size)))
}

/// The page of `elements` after the key `token` names (`token:<key>`; the start without one), and
/// the continuation after its last element when more follow.
fn keyed_page(
    elements: Vec<Element>,
    token: Option<&str>,
    size: usize,
) -> (Vec<Element>, Option<String>) {
    let after = token.map(|token| (token.trim_start_matches("token:"), None));
    let (page, more) = MemoryS3::page_of(elements, after, size);
    let next = more
        .then(|| {
            page.last()
                .map(|last| format!("token:{}", last.position().0))
        })
        .flatten();
    (page, next)
}

/// The listing's continuation once its `leading` empty pages are served (`None`: the first page
/// of content), or `Err` with the continuation of the next empty page to serve.
fn lead_in(token: Option<&str>, leading: usize) -> Result<Option<&str>, String> {
    match token.and_then(|token| token.strip_prefix("lead:")) {
        Some("0") => Ok(None),
        Some(left) => {
            let left = left.parse::<usize>().unwrap_or(1).saturating_sub(1);
            Err(format!("lead:{left}"))
        }
        None if token.is_none() && leading > 0 => Err(format!("lead:{}", leading - 1)),
        None => Ok(token),
    }
}

/// Takes the injected elements of `prefix`'s first page.
fn injected<T>(knob: &mut Vec<(String, T)>, prefix: &str) -> Vec<T> {
    let (served, kept): (Vec<_>, Vec<_>) = mem::take(knob)
        .into_iter()
        .partition(|(of, _)| of == prefix);
    *knob = kept;
    served.into_iter().map(|(_, element)| element).collect()
}

/// The marker after `last`, spelled as `MinIO` spells it when `minio`.
fn next_marker(last: &Element, minio: bool) -> S3VersionMarker {
    let (key, version) = last.position();
    S3VersionMarker {
        key_marker: if minio {
            format!("{key}{MINIO_SUFFIX}")
        } else {
            key.to_string()
        },
        version_id_marker: version.map(str::to_string),
    }
}

/// Counts a page request while it hangs, and stops counting it once it is dropped.
struct Waiting(Arc<AtomicUsize>);

impl Waiting {
    fn new(hanging: Arc<AtomicUsize>) -> Self {
        hanging.fetch_add(1, Ordering::SeqCst);
        Self(hanging)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
