//! The in-memory S3's server-side copies to a final key (ADR-0006 C18): `CopyObject` and
//! `UploadPartCopy`, pinned to the source's `ETag` and version as the real requests are.
//!
//! - A source whose bytes (at its pinned version) no longer carry the pinned `ETag` is a
//!   permanent `Conflict` (S3 answers 412), a missing one `NotFound`.
//! - `CopyObject` counts in `native_copies`, obeys `native_failure` (fails, copies nothing) and
//!   `copy_commits_then_fails` (stores the copy, then loses the reply), and stores the copy as a
//!   write: a new version on a versioned fake.
//! - `UploadPartCopy` names an upload of exactly its key (else `NoSuchUpload`), counts in
//!   `part_copies` (and, while in flight, `part_copies_active` / `part_copies_peak`), obeys `part_failure` / `part_failure_waits` like
//!   `UploadPart`, and cancels `cancel_on_part_copy`'s token when that part is copied (after
//!   storing it), so a test can cut a copy between parts. A part's `ETag` is its MD5.

use std::ops::Range;

use bytes::Bytes;
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::{MemoryS3, bare_upload_id, etag_of, missing_upload, not_found};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3NativeCopySource, S3ProtocolFailure, S3Result, S3WriteFacts};

fn changed_source() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        FailureClass::Conflict,
        Transience::Permanent,
        "PreconditionFailed: native source changed",
    )
}

fn lost_reply() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "copy response lost",
    )
}

impl MemoryS3 {
    /// The source's bytes, when they are still the pinned object.
    async fn pinned_source(&self, source: &S3NativeCopySource) -> S3Result<Bytes> {
        let bytes = self
            .bytes_at(&source.key, source.version_id.as_deref())
            .await
            .map_err(|_| not_found())?;
        if self.etag_for(&bytes) != source.etag || bytes.len() as u64 != source.size {
            return Err(changed_source());
        }
        Ok(bytes)
    }

    pub(super) async fn memory_copy_from(
        &self,
        source: &S3NativeCopySource,
        to: &str,
    ) -> S3Result<S3WriteFacts> {
        if let Some(failure) = self.native_failure.lock().await.clone() {
            return Err(failure);
        }
        let bytes = self.pinned_source(source).await?;
        let etag = self.etag_for(&bytes);
        let version = self.store_written(to, bytes).await;
        *self.native_copies.lock().await += 1;
        if *self.copy_commits_then_fails.lock().await {
            return Err(lost_reply());
        }
        Ok(S3WriteFacts::new(etag, version))
    }

    /// One `UploadPartCopy`, counted while it is in flight: it yields once, so the copies a caller
    /// runs at once overlap and `part_copies_peak` records how many there were.
    pub(super) async fn memory_upload_part_copy(
        &self,
        source: &S3NativeCopySource,
        upload: (&str, &str),
        number: i32,
        range: Range<u64>,
    ) -> S3Result<String> {
        {
            let mut active = self.part_copies_active.lock().await;
            *active += 1;
            let mut peak = self.part_copies_peak.lock().await;
            *peak = (*peak).max(*active);
        }
        yield_now().await;
        let copied = self.copy_one_part(source, upload, number, range).await;
        *self.part_copies_active.lock().await -= 1;
        copied
    }

    async fn copy_one_part(
        &self,
        source: &S3NativeCopySource,
        (key, upload_id): (&str, &str),
        number: i32,
        range: Range<u64>,
    ) -> S3Result<String> {
        *self.part_copies.lock().await += 1;
        let id = bare_upload_id(upload_id);
        let failing = self.part_failure.lock().await.clone();
        if let Some((_, failure)) = failing.filter(|(failed, _)| *failed == number) {
            if *self.part_failure_waits.lock().await {
                self.wait_for_parts_below(&id, number).await;
            }
            return Err(failure);
        }
        let bytes = self.pinned_source(source).await?;
        let (Ok(start), Ok(end)) = (usize::try_from(range.start), usize::try_from(range.end))
        else {
            return Err(S3ProtocolFailure::protocol("range too large"));
        };
        let part = bytes
            .get(start..end)
            .map(|_| bytes.slice(start..end))
            .ok_or_else(|| S3ProtocolFailure::protocol("InvalidRange"))?;
        let etag = etag_of(&part);
        {
            let mut uploads = self.uploads.lock().await;
            let upload = uploads
                .get_mut(&id)
                .filter(|upload| upload.0 == key)
                .ok_or_else(|| missing_upload("S3 UploadPartCopy request failed"))?;
            let parts = &mut upload.1;
            parts.retain(|stored| stored.0 != number);
            parts.push((number, part));
        }
        let cancel = self.cancel_on_part_copy.lock().await.clone();
        if let Some((_, token)) = cancel.filter(|(at, _)| *at == number) {
            token.cancel();
        }
        Ok(etag)
    }

    /// Cancels `token` once part `number` has been copied.
    pub(crate) async fn cancel_after_part_copy(&self, number: i32, token: CancellationToken) {
        *self.cancel_on_part_copy.lock().await = Some((number, token));
    }
}
