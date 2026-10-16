//! What a bucket cannot write, decided from the key and the size alone, before any I/O (P7 §14.2,
//! from the N0 measurements of an ONTAP NAS bucket):
//!
//! - over [`S3BucketCapabilities::max_object_bytes`] (NAS: 5 GiB, one `PutObject`, no multipart
//!   upload — the server itself takes more, but a failed single PUT starts over, so the limit is a
//!   design choice kept from the plan), or of unknown size there;
//! - on a bucket whose keys are file paths ([`S3BucketCapabilities::file_backed_keys`]): a key
//!   with an empty, `.` or `..` segment, which the server resolves to another key — even above
//!   the prefix — and a segment over 255 bytes, which it answers with a 500 that looks
//!   retryable but is not.
//!
//! The transfer asks through `StagedDestination::final_refusal` at `Preflight`; each S3 prepare
//! asks again, so a caller of the role alone is refused the same way (`Unsupported`, permanent,
//! for that entry), with no request sent.

use super::super::S3BucketCapabilities;
use super::super::source::classified_entry;
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;

const GIB: u64 = 1024 * 1024 * 1024;

/// The longest file name a NAS bucket stores: 255 bytes.
const MAX_SEGMENT_BYTES: usize = 255;

/// Why a bucket with `capabilities` cannot write `path` holding `size` bytes; `None` when it can.
pub(super) fn refusal(
    capabilities: S3BucketCapabilities,
    path: &StoragePath,
    size: Option<u64>,
) -> Option<String> {
    if let Some(limit) = capabilities.max_object_bytes
        && size.is_none_or(|size| size > limit)
    {
        let size = size.map_or_else(
            || "an unknown number of".to_string(),
            |size| size.to_string(),
        );
        let limit = if limit % GIB == 0 {
            format!("{} GiB", limit / GIB)
        } else {
            format!("{limit}-byte")
        };
        return Some(format!(
            "bucket_type=nas: an object of {size} bytes cannot be written; one PutObject \
             carries at most {limit}, and ONTAP NAS buckets do not support multipart upload"
        ));
    }
    if !capabilities.file_backed_keys {
        return None;
    }
    let segments = || path.as_str().split('/');
    if segments().any(|segment| matches!(segment, "" | "." | "..")) {
        return Some(
            "bucket_type=nas: the key has an empty, '.' or '..' segment, which an ONTAP NAS \
             bucket would store under another key"
                .to_string(),
        );
    }
    if segments().any(|segment| segment.len() > MAX_SEGMENT_BYTES) {
        return Some(format!(
            "bucket_type=nas: a key segment is longer than {MAX_SEGMENT_BYTES} bytes, which an \
             ONTAP NAS bucket cannot store as a file name"
        ));
    }
    None
}

/// A multipart upload on a bucket without them (an ONTAP NAS bucket answers
/// `CreateMultipartUpload` with 501 and `ListMultipartUploads` with 403): refused before any
/// request — no pointer is looked for, no upload listed or begun.
pub(super) fn refuse_multipart(
    capabilities: S3BucketCapabilities,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    if capabilities.multipart {
        return Ok(());
    }
    Err(classified_entry(
        path,
        Operation::Prepare,
        FailureClass::Unsupported,
        Transience::Permanent,
        "this S3 bucket has no multipart uploads (bucket_type=nas): an object over the single-PUT \
         threshold is written by one streamed PutObject",
    ))
}

/// [`refusal`] as the prepare's `Unsupported` failure of that entry.
pub(super) fn refuse(
    capabilities: S3BucketCapabilities,
    path: &StoragePath,
    size: Option<u64>,
) -> Result<(), StorageRoleFailure> {
    match refusal(capabilities, path, size) {
        Some(reason) => Err(classified_entry(
            path,
            Operation::Prepare,
            FailureClass::Unsupported,
            Transience::Permanent,
            reason,
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(path: &str, size: Option<u64>) -> bool {
        let Ok(path) = StoragePath::new(path) else {
            panic!("an invalid test key {path:?}");
        };
        refusal(S3BucketCapabilities::NAS, &path, size).is_some()
    }

    /// Up to 5 GiB of a known size passes; one byte more, or an unknown size, does not. An object
    /// bucket refuses neither.
    #[test]
    fn a_nas_bucket_writes_at_most_five_gib() {
        let limit = 5 * 1024 * 1024 * 1024;
        assert!(!refused("a/b", Some(limit)));
        assert!(!refused("a/b", Some(0)));
        assert!(refused("a/b", Some(limit + 1)));
        assert!(refused("a/b", None));
        let object = S3BucketCapabilities::OBJECT;
        let path = StoragePath::root();
        assert_eq!(refusal(object, &path, Some(limit + 1)), None);
        assert_eq!(refusal(object, &path, None), None);
    }

    /// Keys a NAS bucket would store elsewhere, or cannot store, are refused; ordinary names —
    /// dots inside a name, 255-byte segments, spaces, UTF-8 — pass.
    #[test]
    fn a_nas_bucket_refuses_keys_it_would_rewrite() {
        for key in [
            "a/../x", "a/./y", "a//b", "./x", "../x", "/x", "x/", "x/..", "",
        ] {
            assert!(refused(key, Some(1)), "{key:?}");
        }
        let long = "n".repeat(256);
        assert!(refused(&format!("d/{long}"), Some(1)));
        let fits = "n".repeat(255);
        for key in [
            fits.as_str(),
            "a/b.c",
            "a/.hidden",
            "a/..b",
            "dir with space/é",
            "x...",
        ] {
            assert!(!refused(key, Some(1)), "{key:?}");
        }
        assert_eq!(
            refusal(S3BucketCapabilities::OBJECT, &StoragePath::root(), Some(1)),
            None
        );
    }
}
