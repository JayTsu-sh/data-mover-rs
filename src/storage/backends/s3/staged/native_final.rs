//! Native S3→S3 copies to the final key (ADR-0006 C18): no temp key, no `.data-mover-stage/`.
//!
//! - A source of at most 64 MiB is one `CopyObject` to the final key at publication — a single
//!   stage ([`single`](super::single)) that records the source instead of buffering bytes.
//!   Before it, only the pointer is looked at (as for a single `PutObject`).
//! - A larger source is a multipart upload on the final key ([`FinalUpload`]) filled with
//!   `UploadPartCopy`, parts of `max(64 MiB, ceil(size / 10 000))`, up to six copied at once,
//!   and completed at publication exactly as a streamed upload is. It is prepared through the same
//!   discovery, so a `Checkpointed` copy writes its `.upload` pointer when the upload begins and a
//!   later attempt — native or streamed — resumes from the parts `ListParts` proves.
//! - A resume keeps the part size its pointer records, whichever route wrote it: a native copy
//!   continues a streamed upload with `UploadPartCopy` parts of that size (more requests, no
//!   memory), and a streamed attempt continues a native upload with its 64 MiB parts (buffering up
//!   to five such parts). Anything else found on the key is cleaned up as the decision table says.
//! - A cancellation stops starting parts and waits for those in flight, so every copied part
//!   counts for the next attempt; a failed part fails the copy at once. Recovery turns on once the
//!   parts copied reach the automatic interval, when the upload has its pointer.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio_util::sync::CancellationToken;

use super::super::S3WriteFacts;
use super::super::source::{cancelled, classified_entry, role_failure};
use super::super::{S3NativeCopyEvidence, S3NativeCopySource, S3Protocol, S3ProtocolFailure};
use super::final_upload::FinalUpload;
use super::native::{NativeFillFailure, native_role_failure};
use super::parts::UploadedParts;
use super::single::{self, SingleStage, changed, same_etag, unchanged};
use super::upload_pointer::pointer_path;
use super::{S3StagedDestination, part_size_at_least};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    DestinationPrepareRequest, PreparedStage, PublicationFailure, StorageRoleFailure,
};

const MIB: u64 = 1024 * 1024;
/// The largest source copied with one `CopyObject`.
const SINGLE_COPY_MAX: u64 = 64 * MIB;
/// The smallest `UploadPartCopy` part.
const COPY_PART_SIZE: u64 = 64 * MIB;
/// `UploadPartCopy` requests in flight at once.
const PARALLEL_PARTS: usize = 6;

/// How a native copy to the final key is split.
#[derive(Clone, Copy, Debug)]
pub(super) struct NativeSizing {
    /// Sources up to this size are one `CopyObject`.
    pub(super) single_max: u64,
    /// The smallest part of a native multipart copy.
    pub(super) part_size: u64,
}

impl Default for NativeSizing {
    fn default() -> Self {
        Self {
            single_max: SINGLE_COPY_MAX,
            part_size: COPY_PART_SIZE,
        }
    }
}

/// The part size of a new native multipart copy of `size` bytes.
pub(super) fn native_part_size(
    size: u64,
    minimum: u64,
    path: &StoragePath,
) -> Result<usize, StorageRoleFailure> {
    part_size_at_least(Some(size), minimum, path)
}

fn refused(path: &StoragePath, operation: Operation, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        path,
        operation,
        FailureClass::InvalidInput,
        Transience::Permanent,
        diagnostic,
    )
}

/// The parts one fill copied.
#[derive(Default)]
struct CopiedParts {
    parts: Vec<(i32, String)>,
    bytes: u64,
    requests: u64,
}

impl CopiedParts {
    fn failure(&self, error: StorageRoleFailure) -> NativeFillFailure {
        NativeFillFailure {
            error,
            bytes: self.bytes,
            requests: self.requests,
        }
    }
}

/// The parts after `from` bytes of a `size`-byte source, numbered from `first`.
pub(super) fn part_ranges(
    from: u64,
    size: u64,
    part_size: u64,
    first: i32,
) -> Vec<(i32, Range<u64>)> {
    let mut ranges = Vec::new();
    let (mut start, mut number) = (from, first);
    while start < size {
        let end = start.saturating_add(part_size).min(size);
        ranges.push((number, start..end));
        start = end;
        number = number.saturating_add(1);
    }
    ranges
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// Prepares a native copy at the destination: one `CopyObject` up to the single-copy limit
    /// (after cleaning up a leftover pointer), otherwise an upload on the final key found by, or
    /// begun after, discovery.
    pub(in crate::storage::backends::s3) async fn prepare_native_stage(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path().clone();
        let pointer = pointer_path(&path)?;
        let size = request.prepare.source.size.ok_or_else(|| {
            refused(
                &path,
                Operation::Prepare,
                "a native S3 copy needs the source's size",
            )
        })?;
        if size <= self.native.single_max {
            let fact = self.clear_leftover(&request, &pointer).await?;
            let mut stage = self.prepare_single(request.prepare, size);
            stage.mark_at_destination(fact);
            return Ok(stage);
        }
        let part_size = native_part_size(size, self.native.part_size, &path)?;
        self.prepare_final_upload(request, part_size, pointer).await
    }

    /// Copies the parts after the resumed prefix with `UploadPartCopy`; `publish` completes the
    /// upload.
    pub(super) async fn fill_native_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        source: &S3NativeCopySource,
        (cancel, operations): (&CancellationToken, usize),
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        let path = stage.final_destination.path();
        let fail = |diagnostic| native_role_failure(refused(path, Operation::Write, diagnostic), 0);
        if upload.expected_size != Some(source.size) {
            return Err(fail("the native S3 source differs from the prepared size"));
        }
        let mut parts = {
            let current = upload.lock();
            if current.written.is_some() {
                return Err(fail("S3 upload is already written"));
            }
            current.parts.clone()
        };
        let resumed = parts.len();
        let first = i32::try_from(resumed + 1).map_err(|_| fail("S3 part number overflow"))?;
        let ranges = part_ranges(
            stage.write_offset,
            source.size,
            upload.part_size as u64,
            first,
        );
        let copied = self
            .copy_parts(stage, upload, source, ranges, (cancel, operations))
            .await?;
        parts.extend(copied.parts.iter().cloned());
        parts.sort_by_key(|part| part.0);
        // The parts' `ETag`s come from the store, not from an MD5 computed here, so nothing proves
        // they are MD5s (SSE-KMS): the completion is not held to their composite. Read-back
        // verification checks the content.
        let sent = UploadedParts {
            md5_etags: false,
            parts,
            bytes: copied.bytes,
        };
        upload
            .record_written(path, sent, resumed, stage.write_offset + copied.bytes)
            .map_err(|error| copied.failure(error))?;
        Ok(S3NativeCopyEvidence {
            bytes: copied.bytes,
            requests: copied.requests,
        })
    }

    /// Copies `ranges`, up to [`PARALLEL_PARTS`] at once and never more than `operations` (the
    /// transfer's operation bound). A cancellation starts no more parts and waits for those in
    /// flight; a failed part fails at once.
    async fn copy_parts(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        source: &S3NativeCopySource,
        ranges: Vec<(i32, Range<u64>)>,
        (cancel, operations): (&CancellationToken, usize),
    ) -> Result<CopiedParts, NativeFillFailure> {
        let path = stage.final_destination.path();
        let parallel = PARALLEL_PARTS.min(operations).max(1);
        let total = ranges.len();
        let mut pending = ranges.into_iter();
        let mut inflight = FuturesUnordered::new();
        let mut copied = CopiedParts::default();
        loop {
            while inflight.len() < parallel && !cancel.is_cancelled() {
                let Some((number, range)) = pending.next() else {
                    break;
                };
                inflight.push(copy_part(
                    Arc::clone(&self.protocol),
                    source,
                    path.as_str(),
                    &upload.upload_id,
                    number,
                    range,
                ));
                copied.requests += 1;
            }
            let Some((number, length, result)) = inflight.next().await else {
                break;
            };
            // Failing at once drops the requests still in flight; one the server still completes
            // is removed with the upload by the discard (or the next prepare's clean-up).
            let etag = result
                .map_err(|error| copied.failure(role_failure(path, Operation::Write, error)))?;
            copied.parts.push((number, etag));
            copied.bytes += length;
            self.native_checkpoint(stage, upload, copied.bytes);
        }
        if copied.parts.len() < total {
            return Err(copied.failure(cancelled(path, Operation::Write)));
        }
        Ok(copied)
    }

    /// Turns recovery on once the parts copied reach the automatic interval, when the upload has
    /// its pointer (a resumable prepare wrote it when the upload began).
    fn native_checkpoint(&self, stage: &PreparedStage, upload: &FinalUpload, copied: u64) {
        if copied >= self.checkpoint_interval && upload.pointer_written() {
            stage.recovery_enabled.store(true, Ordering::Release);
        }
    }
}

/// One `UploadPartCopy`: (part number, bytes, `ETag` or failure).
async fn copy_part<P: S3Protocol>(
    protocol: Arc<P>,
    source: &S3NativeCopySource,
    key: &str,
    upload_id: &str,
    number: i32,
    range: Range<u64>,
) -> (i32, u64, Result<String, S3ProtocolFailure>) {
    let length = range.end - range.start;
    let result = if (1..=10_000).contains(&number) {
        protocol
            .upload_part_copy(source, key, upload_id, number, range)
            .await
    } else {
        Err(S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            "S3 multipart part limit exceeded",
        ))
    };
    (number, length, result)
}

/// Publishes a native single stage: one `CopyObject` of `source` to the final key, then the
/// pending tags. A refusal the service answered before copying leaves the final key unchanged;
/// any other failure is settled by HEAD — our size and the source's `ETag` count as copied (a
/// copy of an object uploaded in parts gets a new `ETag` on some stores, and then reports the
/// final key changed).
pub(super) async fn send_copy<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    single: &SingleStage,
    source: &S3NativeCopySource,
    expected_size: u64,
) -> Result<S3WriteFacts, PublicationFailure> {
    if source.size != expected_size {
        return Err(unchanged(refused(
            path,
            Operation::Publish,
            "S3 object content differs from the published size",
        )));
    }
    let facts = match adapter.protocol.copy_from(source, path.as_str()).await {
        Ok(facts) => facts,
        Err(failure) if single::definite_refusal(&failure) => {
            return Err(unchanged(role_failure(path, Operation::Publish, failure)));
        }
        Err(failure) => match adapter.protocol.head(path.as_str()).await {
            Ok(head) if head.size == source.size && same_etag(&head.etag, &source.etag) => {
                S3WriteFacts::new(head.etag, head.version_id)
            }
            _ => return Err(changed(role_failure(path, Operation::Publish, failure))),
        },
    };
    if let Some(tags) = single.pending() {
        adapter
            .protocol
            .put_tags(path.as_str(), &tags)
            .await
            .map_err(|failure| changed(role_failure(path, Operation::Publish, failure)))?;
    }
    single.record_published(facts.clone());
    Ok(facts)
}
