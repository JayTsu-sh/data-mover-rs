//! `Direct` writes to S3 (ADR-0006 C14c): the object appears at its final key inside `write`.
//!
//! A source of known size up to the single-PUT threshold T is buffered and sent as one
//! `PutObject` at the end of `write`. Anything larger, or of unknown size, is a multipart upload
//! on the final key that `write` begins, fills (every part with `Content-MD5`) and completes —
//! checking the composite `ETag` when one can be computed — and aborts on any failure, so a failed
//! `Direct` write leaves no upload behind. The upload is begun inside `write`, not at prepare: a
//! transfer that fails before writing leaves nothing either. Publication has nothing left to do,
//! verification reads the final object back pinned to what the write created, and discard never
//! deletes the final key.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio_util::sync::CancellationToken;

use super::super::source::{cancelled, classified_entry, entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, S3WriteFacts, composite_etag};
use super::completion::completed_object;
use super::parts::{PartTarget, UploadedParts};
use super::{S3StagedDestination, cleanup_result, planned_part_size, single};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, PrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence,
    PublicationFailure, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

/// The upload-id half of a multipart `Direct` stage's token; a real upload id is printable.
const DIRECT_MARKER: &str = "\u{1}direct";

/// A multipart `Direct` write to the final key.
pub(super) struct DirectUpload {
    part_size: usize,
    /// The source's size, when known: a different byte count is never completed, since a
    /// `Direct` completion replaces the final object irreversibly.
    expected_size: Option<u64>,
    state: Mutex<DirectState>,
}

#[derive(Default)]
struct DirectState {
    /// The upload `write` began and has neither completed nor aborted.
    open: Option<String>,
    /// The completed object: its bytes and what the completion reported.
    written: Option<(u64, S3WriteFacts)>,
}

impl DirectUpload {
    fn lock(&self) -> MutexGuard<'_, DirectState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn upload_of(stage: &PreparedStage) -> Option<&DirectUpload> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<DirectUpload>()
}

/// What the write that made the object reported, once it is written.
fn written_facts(stage: &PreparedStage) -> Option<S3WriteFacts> {
    match single::of(stage) {
        Some(single) => single.published(),
        None => upload_of(stage)?
            .lock()
            .written
            .clone()
            .map(|(_, facts)| facts),
    }
}

fn not_written(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    entry(path, operation, "the S3 Direct object was not written")
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// A `Direct` stage: a single stage for a known size up to T, otherwise a multipart one whose
    /// upload `write` begins. Neither keeps recovery state.
    pub(super) fn prepare_direct_stage(
        &self,
        request: PrepareRequest,
        cancel: &CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.final_destination.path();
        if cancel.is_cancelled() {
            return Err(cancelled(path, Operation::Prepare));
        }
        if request.source.source_identity.backend() == &self.identity
            && request.source.path == *path
        {
            return Err(classified_entry(
                path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
                "a Direct write cannot replace its own S3 source",
            ));
        }
        let size = request
            .source
            .size
            .filter(|size| self.is_single_put(Some(*size)));
        let mut stage = if let Some(size) = size {
            self.prepare_single(request, size)
        } else {
            let part_size = planned_part_size(request.source.size, path)?;
            let token = Self::encode_token(path.as_str(), DIRECT_MARKER);
            let mut stage = PreparedStage::new(
                self.identity.clone(),
                request.final_destination,
                token,
                request.recovery_binding,
                0,
                None,
            )
            .disable_recovery();
            stage.backend_state = Some(Arc::new(DirectUpload {
                part_size,
                expected_size: request.source.size,
                state: Mutex::new(DirectState::default()),
            }));
            stage
        };
        stage.direct = true;
        stage.durable_publication = false;
        Ok(stage)
    }

    /// Writes the whole object at its final key.
    pub(super) async fn write_direct(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let path = stage.final_destination.path();
        if let Some(single) = single::of(stage) {
            let written = single::write(stage, single, input).await?;
            single::send(self, path, single, single.expected_size())
                .await
                .map_err(|failure| failure.error)?;
            return Ok(written);
        }
        let upload = upload_of(stage).ok_or_else(|| not_written(path, Operation::Write))?;
        if upload.lock().written.is_some() {
            return Err(entry(
                path,
                Operation::Write,
                "S3 object is already written",
            ));
        }
        let upload_id = self.begin_direct_upload(path, upload).await?;
        match self
            .fill_direct_upload(path, upload, &upload_id, input)
            .await
        {
            Ok(evidence) => Ok(evidence),
            Err(error) => {
                self.abort_direct_upload(path, upload).await;
                Err(error)
            }
        }
    }

    /// Aborts the uploads an earlier writer left on the final key (none can be live: one key is
    /// never written by two transfers at once), then begins this one.
    async fn begin_direct_upload(
        &self,
        path: &StoragePath,
        upload: &DirectUpload,
    ) -> Result<String, StorageRoleFailure> {
        match self.protocol.list_uploads(path.as_str()).await {
            Ok(orphans) => {
                for orphan in orphans {
                    let aborted = self.protocol.abort_multipart(path.as_str(), &orphan).await;
                    if let Err(error) = cleanup_result(path, aborted) {
                        tracing::warn!(path = %path.as_str(), ?error, "could not abort an orphan S3 upload");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(path = %path.as_str(), ?error, "could not list orphan S3 uploads");
            }
        }
        let upload_id = self
            .protocol
            .begin_multipart(path.as_str())
            .await
            .map_err(|failure| role_failure(path, Operation::Write, failure))?;
        upload.lock().open = Some(upload_id.clone());
        Ok(upload_id)
    }

    async fn fill_direct_upload(
        &self,
        path: &StoragePath,
        upload: &DirectUpload,
        upload_id: &str,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let target = PartTarget {
            path,
            key: path.as_str(),
            upload_id,
            part_size: upload.part_size,
            checkpoint: None,
        };
        let sent = self.upload_parts(&target, Vec::new(), input).await?;
        let size = sent.bytes;
        if upload
            .expected_size
            .is_some_and(|expected| expected != size)
        {
            return Err(classified_entry(
                path,
                Operation::Write,
                FailureClass::InvalidInput,
                Transience::Permanent,
                "the S3 Direct input differs from the source's size",
            ));
        }
        let facts = self.complete_direct(path, upload_id, &sent, size).await?;
        let mut state = upload.lock();
        state.open = None;
        state.written = Some((size, facts));
        Ok(WriteEvidence {
            persisted_bytes: size,
        })
    }

    /// Completes the upload. When every part came back with its MD5 as `ETag` and the object's
    /// `ETag` has the multipart form, it must be the composite of the parts (a store that
    /// encrypts with KMS reports other part `ETag`s, one that reports another object `ETag` form
    /// computes no comparable composite: neither is checked). A failed completion — a lost reply, or the retry of a
    /// completion that already committed answered `NoSuchUpload` — counts as completed when the
    /// final object has our size and that composite `ETag` (see [`Self::settle_direct_completion`]).
    async fn complete_direct(
        &self,
        path: &StoragePath,
        upload_id: &str,
        sent: &UploadedParts,
        size: u64,
    ) -> Result<S3WriteFacts, StorageRoleFailure> {
        let etags: Vec<String> = sent.parts.iter().map(|part| part.1.clone()).collect();
        let expected = composite_etag(&etags);
        let facts = match self
            .protocol
            .complete_multipart(path.as_str(), upload_id, &sent.parts)
            .await
        {
            Ok(facts) => facts,
            Err(failure) => {
                self.settle_direct_completion(path, upload_id, size, expected.as_deref(), failure)
                    .await?
            }
        };
        match expected {
            Some(expected)
                if sent.md5_etags
                    && is_composite(&facts.etag)
                    && !single::same_etag(&facts.etag, &expected) =>
            {
                Err(classified_entry(
                    path,
                    Operation::Write,
                    FailureClass::Corruption,
                    Transience::Permanent,
                    "the completed S3 object's ETag is not the composite of its parts",
                ))
            }
            _ => Ok(facts),
        }
    }

    /// Settles a failed completion by aborting the upload first. One already gone (`NoSuchUpload`)
    /// completed: the object is ours when it is the key's latest version with our size and
    /// composite `ETag`, and that version is claimed (ADR-0006 C17). One that could still be
    /// aborted — or whose abort failed — may never have completed, and an identical earlier object
    /// would match as well: it counts as written, but no version is claimed.
    async fn settle_direct_completion(
        &self,
        path: &StoragePath,
        upload_id: &str,
        size: u64,
        expected: Option<&str>,
        failure: S3ProtocolFailure,
    ) -> Result<S3WriteFacts, StorageRoleFailure> {
        let aborted = self
            .protocol
            .abort_multipart(path.as_str(), upload_id)
            .await;
        let gone = matches!(
            &aborted,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            })
        );
        if let Err(error) = cleanup_result(path, aborted) {
            tracing::warn!(path = %path.as_str(), ?error, "could not abort a failed Direct S3 upload");
        }
        let found = match expected {
            Some(expected) => completed_object(&*self.protocol, path, size, expected, gone).await,
            None => None,
        };
        found.ok_or_else(|| role_failure(path, Operation::Write, failure))
    }

    /// Aborts the stage's open upload, if any, trying twice. The engine keeps no failed `Direct`
    /// stage to discard later, so an upload whose abort fails both times is left to the next
    /// `Direct` write of the key (which aborts the uploads it finds there) or to the bucket's
    /// lifecycle rule; it is logged.
    async fn abort_direct_upload(&self, path: &StoragePath, upload: &DirectUpload) {
        let Some(upload_id) = upload.lock().open.clone() else {
            return;
        };
        for attempt in 1..=2 {
            let aborted = self
                .protocol
                .abort_multipart(path.as_str(), &upload_id)
                .await;
            match cleanup_result(path, aborted) {
                Ok(()) => {
                    upload.lock().open = None;
                    return;
                }
                Err(error) => {
                    tracing::warn!(path = %path.as_str(), attempt, ?error, "could not abort a Direct S3 upload");
                }
            }
        }
    }

    /// Discards a `Direct` stage: aborts an upload still open, never deletes the final key.
    pub(super) async fn discard_direct(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        let path = stage.final_destination.path();
        let Some(upload) = upload_of(stage) else {
            return Ok(());
        };
        let Some(upload_id) = upload.lock().open.clone() else {
            return Ok(());
        };
        let aborted = self
            .protocol
            .abort_multipart(path.as_str(), &upload_id)
            .await;
        cleanup_result(path, aborted)?;
        upload.lock().open = None;
        Ok(())
    }

    /// The bytes the object holds once written.
    pub(super) fn direct_durable_bytes(stage: &PreparedStage) -> u64 {
        match single::of(stage) {
            Some(single) => single.durable_len(),
            None => upload_of(stage)
                .and_then(|upload| upload.lock().written.as_ref().map(|written| written.0))
                .unwrap_or(0),
        }
    }

    /// Publication of a `Direct` object: `write` already made it visible.
    pub(super) fn publish_direct(
        &self,
        stage: &PreparedStage,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let path = stage.final_destination.path();
        let unchanged = |error| PublicationFailure {
            error,
            final_destination_changed: false,
        };
        self.validate(stage).map_err(unchanged)?;
        let facts =
            written_facts(stage).ok_or_else(|| unchanged(not_written(path, Operation::Publish)))?;
        Ok(PublicationEvidence {
            final_destination: path.clone(),
            disposition: PublicationDisposition::Published,
            version: facts.version_id,
        })
    }

    /// Reads the `Direct` object back, pinned to what its write created.
    pub(super) async fn verify_direct(
        &self,
        stage: &PreparedStage,
        request: &VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let path = stage.final_destination.path();
        self.validate(stage)?;
        let facts = written_facts(stage).ok_or_else(|| not_written(path, Operation::Verify))?;
        single::verify_written(self, path, facts, request).await
    }
}

/// Whether `etag` has the multipart form `"<hex>-<parts>"`.
pub(super) fn is_composite(etag: &str) -> bool {
    etag.trim_matches('"')
        .rsplit_once('-')
        .is_some_and(|(_, count)| {
            !count.is_empty() && count.bytes().all(|digit| digit.is_ascii_digit())
        })
}

#[cfg(test)]
#[path = "direct_tests.rs"]
mod tests;
