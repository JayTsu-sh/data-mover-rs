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

use std::sync::{MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::task::yield_now;
use tokio::time::sleep;

use super::MemoryS3;
use crate::model::{StorageTimestamp, TimePrecision};
use crate::storage::backends::s3::{
    S3ListedObject, S3ListedVersion, S3ObjectPage, S3ProtocolFailure, S3Result, S3VersionFacts,
    S3VersionMarker, S3VersionPage,
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
    /// A broken server: every page is truncated and its continuation cycles through this many
    /// tokens (0: off).
    pub(crate) cycle_tokens: usize,
    /// Pages served while `cycle_tokens` or `endless_empty` is on.
    cycled_pages: usize,
    /// A broken server: every page is empty yet names a new continuation.
    pub(crate) endless_empty: bool,
    /// Each page answers after this long.
    pub(crate) page_delay: Option<Duration>,
    /// Listing this prefix fails with this failure.
    pub(crate) failure: Option<(String, S3ProtocolFailure)>,
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
        let mut knobs = self.listing();
        knobs.calls.push(prefix.to_string());
        if let Some((failed, failure)) = &knobs.failure
            && failed == prefix
        {
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

    async fn object_elements(&self, prefix: &str) -> Vec<Element> {
        let last_modified = *self.last_modified.lock().await;
        let objects = self.objects.lock().await.clone();
        let mut elements = Vec::new();
        for key in self.keys_under(prefix, false).await {
            if let Some(common) = self.rolled_up(prefix, &key) {
                if !matches!(elements.last(), Some(Element::Prefix(last)) if *last == common) {
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
        let same_instant = self.listing().same_instant;
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
            for (newest_first, facts) in history.into_iter().enumerate() {
                let written = if same_instant {
                    1
                } else {
                    count - newest_first
                };
                elements.push(Element::Version(S3ListedVersion {
                    key: key.clone(),
                    facts,
                    last_modified: instant(i128::try_from(written).unwrap_or(i128::MAX)),
                }));
            }
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
        let size = self.listing_call(prefix).await?;
        if let Some(next) = self.endless_page() {
            return Ok(S3ObjectPage {
                next: Some(next),
                ..S3ObjectPage::default()
            });
        }
        let elements = self.object_elements(prefix).await;
        let after = token.map(|token| (token.trim_start_matches("token:"), None));
        let (page, more) = Self::page_of(elements, after, size);
        let next = more
            .then(|| {
                page.last()
                    .map(|last| format!("token:{}", last.position().0))
            })
            .flatten();
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
        Ok(listed)
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
        for element in page {
            match element {
                Element::Prefix(prefix) => listed.prefixes.push(prefix),
                Element::Version(version) if version.facts.delete_marker => {
                    listed.markers.push(version);
                }
                Element::Version(version) => listed.versions.push(version),
                Element::Object(_) => {}
            }
        }
        Ok(listed)
    }
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
