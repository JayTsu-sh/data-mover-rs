//! Multipart writes cannot represent S3 directory objects safely.

use crate::{Result, error::StorageError};

/// Refuses a multipart upload (streamed or `UploadPartCopy`) to `key` ending in '/', before
/// anything is sent: a directory object is written only by one request (P7 D13). `MinIO` refuses
/// such an upload (400 `XMinioInvalidObjectName`); `RustFS` 1.0.0 stores a completed one under the
/// key without its '/', over a real file there. This guards the writers a caller hands one to: beginning an upload, taking an open one over
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
