//! A directory's own object in the legacy walks: the key `p/d/` itself, listed only by the listing
//! of the prefix `p/d/` (the parent's delimiter listing rolls it into the common prefix).
//!
//! All four measured object stores (`MinIO`, `RustFS`, ONTAP object buckets, DXN) store content
//! under such a key (P7 existing issue 12). The legacy `S3Entry` has no shape for it — its paths
//! never end in `/` and legacy copy consumers have never seen one — so the legacy walks do not list
//! it; they report it instead (P7 F5a), since skipping it silently would lose its bytes. A zero-byte
//! key is an ordinary folder marker and stays skipped: the directory entry already stands for it.
//! The role-based traversal lists the object as a file entry at `p/d/` (P7 F5).
//!
//! The report ignores the walk's filter: legacy never judged such a key, and a job whose filter
//! would have excluded it still learns that the bucket holds data the legacy walk cannot list.
//! Under a delimiter only the listed prefix's own key can end in `/`; `walkdir_2` also reports any
//! other such key rather than drop it, should a store ever list one.

use std::path::PathBuf;

use aws_sdk_s3::types::ObjectVersion;
use tracing::warn;

use super::version_listing::VersionOrDeleteMarker;
use crate::{ErrorEvent, Result, StorageEntryMessage, error::StorageError};

/// Refuses a multipart upload (streamed or `UploadPartCopy`) to `key` ending in '/', before
/// anything is sent: a directory object is written only by one request (P7 D13). `MinIO` refuses
/// such an upload (400 `XMinioInvalidObjectName`); `RustFS` 1.0.0 stores a completed one under the
/// key without its '/', over a real file there. The legacy walks never list such a key; this
/// guards the legacy writers a caller hands one to: beginning an upload, taking an open one over
/// (the resumable path), and completing one (whoever began it).
pub(super) fn refuse_multipart(key: &str) -> Result<()> {
    if key.ends_with('/') {
        return Err(StorageError::UnsupportedType(format!(
            "S3 multipart upload to {key}: a directory object (a key ending in '/') is written \
             only by one request"
        )));
    }
    Ok(())
}

/// A listed size in bytes; a missing or negative one counts as zero.
pub(super) fn listed_bytes(size: Option<i64>) -> u64 {
    size.and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0)
}

/// The largest size among the versions of `own_key` on one page, when one of them holds content.
pub(super) fn own_versions_with_content<'a>(
    versions: impl IntoIterator<Item = &'a ObjectVersion>,
    own_key: &str,
) -> Option<u64> {
    versions
        .into_iter()
        .filter(|version| version.key() == Some(own_key))
        .map(|version| listed_bytes(version.size()))
        .filter(|bytes| *bytes > 0)
        .max()
}

/// The largest size among one key's versions, when one of them holds content.
pub(super) fn history_with_content(entries: &[VersionOrDeleteMarker]) -> Option<u64> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            VersionOrDeleteMarker::Version(version) => Some(listed_bytes(version.size())),
            VersionOrDeleteMarker::DeleteMarker(_) => None,
        })
        .filter(|bytes| *bytes > 0)
        .max()
}

/// Why the legacy walk reports `path` (a key ending in `/`, relative to the storage root)
/// instead of listing it. The empty path is the storage root's own key (the URL prefix itself).
pub(super) fn not_listed(path: &str, bytes: u64) -> String {
    let what = if path.is_empty() {
        "at the storage root's own key".to_string()
    } else {
        path.to_string()
    };
    format!(
        "S3 directory object {what} holds {bytes} bytes and is not listed: the legacy walk has no \
         entry for a key ending in '/'; the role-based traversal lists it as a file (P7 F5)"
    )
}

/// Sends the `walkdir` scan error for a directory object `path` holding `bytes` (P7 F5a).
pub(super) async fn report(
    tx: &async_channel::Sender<StorageEntryMessage>,
    path: &str,
    bytes: u64,
) {
    let reason = not_listed(path, bytes);
    warn!("[S3] {reason}");
    let _ = tx
        .send(StorageEntryMessage::Error {
            event: ErrorEvent::Scan,
            path: PathBuf::from(path),
            entry: None,
            reason,
        })
        .await;
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::DeleteMarkerEntry;

    use super::*;

    fn version(key: &str, size: i64) -> ObjectVersion {
        ObjectVersion::builder().key(key).size(size).build()
    }

    #[test]
    fn only_the_own_keys_versions_with_content_count() {
        let page = [version("p/d/", 0), version("p/d/x", 9), version("p/d/", 5)];
        assert_eq!(own_versions_with_content(&page, "p/d/"), Some(5));
        let markers_only = [version("p/d/", 0), version("p/d/x", 9)];
        assert_eq!(own_versions_with_content(&markers_only, "p/d/"), None);
        assert_eq!(own_versions_with_content(&page, "p/e/"), None);
    }

    #[test]
    fn a_history_with_content_in_any_version_is_reported() {
        let marker = VersionOrDeleteMarker::DeleteMarker(DeleteMarkerEntry::builder().build());
        let history = [
            VersionOrDeleteMarker::Version(version("d/", 5)),
            VersionOrDeleteMarker::Version(version("d/", 0)),
            marker.clone(),
        ];
        assert_eq!(history_with_content(&history), Some(5));
        let empty = [VersionOrDeleteMarker::Version(version("d/", 0)), marker];
        assert_eq!(history_with_content(&empty), None);
    }

    #[test]
    fn negative_or_missing_sizes_are_zero() {
        assert_eq!(listed_bytes(None), 0);
        assert_eq!(listed_bytes(Some(-1)), 0);
        assert_eq!(listed_bytes(Some(7)), 7);
    }

    #[test]
    fn the_reason_names_the_key_and_its_bytes() {
        let reason = not_listed("p/d/", 5);
        assert!(reason.contains("p/d/"), "{reason}");
        assert!(reason.contains("5 bytes"), "{reason}");
        let root = not_listed("", 2);
        assert!(root.contains("storage root's own key"), "{root}");
    }

    #[tokio::test]
    async fn walkdir_gets_one_scan_error_at_the_key() {
        let (tx, rx) = async_channel::bounded(1);
        report(&tx, "p/d/", 5).await;
        let message = rx.try_recv();
        assert!(
            matches!(
                message,
                Ok(StorageEntryMessage::Error { event: ErrorEvent::Scan, ref path, entry: None, .. })
                    if path.as_os_str() == "p/d/"
            ),
            "{message:?}"
        );
    }
}
