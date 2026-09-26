//! Directory objects (P7 F5): a final key ending in `/` (`p/d/`) is written only by **one request**
//! (P7 §14.1 D13) — one `PutObject`, or for a native copy one `CopyObject` — whatever the
//! single-PUT threshold or the native single-copy limit say, and never through a multipart upload
//! or `UploadPartCopy`. Measured 2026-09-26: `MinIO` refuses a multipart upload to such a key
//! (400 `XMinioInvalidObjectName`), and `RustFS` 1.0.0 stores a completed one — streamed or copied
//! part by part — under `p/d`, overwriting a real file there.
//!
//! One request carries at most 5 GiB, and its size must be known before it is sent: a larger
//! directory object, or one of unknown size, is that entry's `Unsupported` (permanent), refused
//! before any I/O; there is no fallback. Such a stage is never resumed: a failure starts over.
//! A streamed one up to the single-PUT threshold is buffered before its `PutObject` (as every
//! single-PUT stage is); a larger one is one streamed `PutObject` (P7 F5h,
//! [`streaming`](super::streaming)) — its last byte held until publication, no more than a chunk
//! buffered — instead of being held whole in memory (up to 5 GiB per transfer, as F5g did). A
//! native one buffers nothing.

use super::super::source::classified_entry;
use super::single::MAX_SINGLE_PUT_THRESHOLD;
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;

/// The most bytes one `PutObject` or `CopyObject` writes, and so the largest directory object an
/// S3 destination writes: 5 GiB.
pub(crate) const MAX_DIRECTORY_OBJECT_BYTES: u64 = MAX_SINGLE_PUT_THRESHOLD;

/// For a directory object's final key, its size when one request can write it, and the entry's
/// `Unsupported` when none can (larger than 5 GiB, or of unknown size); `None` for any other key.
/// Decided before any I/O.
pub(super) fn single_request_size(
    path: &StoragePath,
    size: Option<u64>,
) -> Result<Option<u64>, StorageRoleFailure> {
    if !path.names_directory_object() {
        return Ok(None);
    }
    match size {
        Some(size) if size <= MAX_DIRECTORY_OBJECT_BYTES => Ok(Some(size)),
        _ => Err(classified_entry(
            path,
            Operation::Prepare,
            FailureClass::Unsupported,
            Transience::Permanent,
            "an S3 directory object (a key ending in '/') is written only by one request, which \
             needs its size up front and carries at most 5 GiB",
        )),
    }
}
