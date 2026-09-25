//! Destination-resident recovery for S3 (ADR-0006 C15b): an object larger than the single-PUT
//! threshold is a multipart upload on its final key, and a checkpointed one is resumable through
//! the `.upload` pointer object beside that key (see [`upload_pointer`](super::upload_pointer)).
//!
//! The pointer names the upload and carries no durable prefix: the parts `ListParts` reports are
//! the durable record. A resume continues from their **contiguous prefix** — parts `1..=k`, each
//! exactly the pointer's part size, the last one shorter only when it ends exactly at the source's
//! size. A gap is not corruption (parts finish out of order, so a process killed mid-write leaves
//! some): the parts after it are uploaded again, and uploading a part number again replaces it.
//!
//! As on NFS, CIFS and HDFS, every prepare draws a nonce and records it in the pointer (a resume
//! rewrites the pointer with its own); before it writes the pointer, completes the upload, or
//! cleans up, a stage reads the pointer back, and another nonce there — or no pointer after it
//! wrote one — means another writer took the upload over (`Conflict`, permanent). The fence is a
//! check, not a lock, and a stage without a pointer has nothing to fence with until its first
//! checkpoint; both are two writers of one key, which the caller contract excludes.
//!
//! Publication completes the upload; nothing is written to the final key before it, and the final
//! key is never deleted. Verification reads the object back after publication.
//!
//! In a versioned bucket (ADR-0006 C17) the pointer is deleted by the version its write reported,
//! a resume deletes the version of the pointer it replaced, and a completion whose reply was lost
//! is settled through the key's versions, claiming the version it made (see
//! [`completion`](super::completion)).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::source::{cancelled, classified_entry, entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, S3WriteFacts, composite_etag};
use super::completion::completed_object;
use super::direct::is_composite;
use super::parts::{PartTarget, PartsCheckpoint, UploadedParts};
use super::upload_discovery::{S3Artifacts, leftover_reason};
use super::upload_pointer::{self, NONCE_BYTES, UploadRecord, pointer_path};
use super::{S3StagedDestination, cleanup_result, metadata_unavailable, planned_part_size, single};
use crate::model::{FailureClass, ObjectTag, Operation, StoragePath, Transience};
use crate::storage::discovery::discover;
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{
    ByteStream, CheckpointObservation, DestinationPrepareRequest, MetadataMutation, PrepareFact,
    PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    ResumeMode, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

/// A multipart upload on the final key, kept at the destination.
pub(super) struct FinalUpload {
    upload_id: String,
    part_size: usize,
    /// The source's size, when known: a different byte count is never completed.
    expected_size: Option<u64>,
    transfer_identity: [u8; 32],
    nonce: [u8; NONCE_BYTES],
    pointer: StoragePath,
    /// Whether this stage wrote its pointer; a pointer gone after that was removed by another
    /// writer.
    pointer_written: AtomicBool,
    state: Mutex<UploadState>,
}

#[derive(Default)]
struct UploadState {
    /// Every part the completion names (number, `ETag`): the resumed prefix, then what `write`
    /// sent.
    parts: Vec<(i32, String)>,
    /// Whether every part `write` sent came back with its MD5 as `ETag`; `None` when it sent none.
    /// The parts of one upload share its encryption, so the resumed prefix answers the same way.
    md5_etags: Option<bool>,
    /// The bytes the parts hold once `write` finished.
    written: Option<u64>,
    /// Tags to set once the object exists.
    tags: Option<Vec<ObjectTag>>,
    /// What the completion reported.
    published: Option<S3WriteFacts>,
    /// The version this stage's pointer write reported (`"null"` included), which deleting it
    /// names; `None` in an unversioned bucket — or when a resume could not delete the version it
    /// replaced, so that a plain delete hides both behind a marker.
    pointer_version: Option<String>,
    /// Contents no pointer version may keep once ours is deleted: ours, and a replaced one's.
    stale_pointers: Vec<Vec<u8>>,
}

impl FinalUpload {
    fn new(
        request: &DestinationPrepareRequest,
        upload_id: String,
        part_size: u64,
        pointer: StoragePath,
    ) -> Result<Self, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        Ok(Self {
            upload_id,
            part_size: usize::try_from(part_size)
                .map_err(|_| failure(path, Operation::Prepare, FailureClass::Unsupported))?,
            expected_size: request.prepare.source.size,
            transfer_identity: request.transfer_identity,
            nonce: *Uuid::new_v4().as_bytes(),
            pointer,
            pointer_written: AtomicBool::new(false),
            state: Mutex::new(UploadState::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, UploadState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn pointer_written(&self) -> bool {
        self.pointer_written.load(Ordering::Acquire)
    }

    /// Records what `write` sent: `size` bytes in all, which must be the source's size when it is
    /// known.
    fn record_written(
        &self,
        path: &StoragePath,
        sent: UploadedParts,
        resumed_parts: usize,
        size: u64,
    ) -> Result<(), StorageRoleFailure> {
        if self.expected_size.is_some_and(|expected| expected != size) {
            return Err(classified_entry(
                path,
                Operation::Write,
                FailureClass::InvalidInput,
                Transience::Permanent,
                "the S3 upload input differs from the source's size",
            ));
        }
        let mut state = self.lock();
        state.md5_etags = (sent.parts.len() > resumed_parts).then_some(sent.md5_etags);
        state.parts = sent.parts;
        state.written = Some(size);
        Ok(())
    }

    /// Records the pointer this stage wrote: the version its write reported and its bytes.
    fn record_pointer(&self, version: Option<String>, bytes: Vec<u8>) {
        let mut state = self.lock();
        state.pointer_version = version;
        state.stale_pointers.push(bytes);
    }

    /// Records the pointer a resume replaced. When its version could not be deleted, deleting
    /// ours by version would make it current again: ours is then hidden behind a delete marker.
    fn forget_replaced(&self, bytes: Vec<u8>, deleted: bool) {
        let mut state = self.lock();
        state.stale_pointers.push(bytes);
        if !deleted {
            state.pointer_version = None;
        }
    }

    fn extension(&self) -> Option<Bytes> {
        UploadRecord {
            nonce: self.nonce,
            part_size: self.part_size as u64,
            upload_id: self.upload_id.clone(),
        }
        .encode()
    }
}

/// The upload on the final key `stage` is, if it is one.
pub(super) fn of(stage: &PreparedStage) -> Option<&FinalUpload> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<FinalUpload>()
}

/// Whether the pointer is still this stage's.
enum Fence {
    /// Present with our nonce (`pointer`), or — before we wrote one — absent.
    Ours {
        pointer: bool,
    },
    TakenOver,
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    classified_entry(
        path,
        operation,
        class,
        Transience::Permanent,
        "S3 upload on the final key",
    )
}

fn taken_over(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    // Another writer holds the upload now; retrying would take it back and forth.
    classified_entry(
        path,
        operation,
        FailureClass::Conflict,
        Transience::Permanent,
        "another writer took the S3 upload over",
    )
}

fn is_not_found(failure: &S3ProtocolFailure) -> bool {
    matches!(
        failure,
        S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }
    )
}

/// A completion the service refused before it could commit anything. `NoSuchUpload` is not one:
/// it is also how a retried completion that already committed is answered.
fn refused_before_commit(failure: &S3ProtocolFailure) -> bool {
    single::definite_refusal(failure) && !is_not_found(failure)
}

fn unchanged(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

fn changed(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: true,
    }
}

/// At the upload's first deferred checkpoint: writes the pointer unless prepare already did, and
/// turns the stage's recovery on.
struct PointerCheckpoint<'a, P> {
    adapter: &'a S3StagedDestination<P>,
    stage: &'a PreparedStage,
    upload: &'a FinalUpload,
}

#[async_trait]
impl<P: S3Protocol + 'static> PartsCheckpoint for PointerCheckpoint<'_, P> {
    async fn reached(&self) -> Result<(), StorageRoleFailure> {
        if !self.upload.pointer_written() {
            self.adapter
                .write_pointer(self.stage, self.upload, false)
                .await?;
        }
        self.stage.recovery_enabled.store(true, Ordering::Release);
        Ok(())
    }
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// Prepares by looking at the destination first. Up to T a single stage (after cleaning up a
    /// leftover pointer); above T, or of unknown size, a multipart upload on the final key that
    /// resumes or starts over as the decision table says.
    pub(super) async fn prepare_at_destination_stage(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path().clone();
        let pointer = pointer_path(&path)?;
        let size = request.prepare.source.size;
        if let Some(size) = size.filter(|size| self.is_single_put(Some(*size))) {
            return self.prepare_small(request, size, &pointer).await;
        }
        let part_size = planned_part_size(size, &path)?;
        let artifacts = S3Artifacts::new(&*self.protocol, &path, &pointer, &request);
        let found = discover(&artifacts, &request).await?;
        let Some(point) = found.resume else {
            return self
                .start_upload(&request, part_size, pointer, found.fact)
                .await;
        };
        let left = artifacts.into_found();
        let record = left
            .record
            .ok_or_else(|| failure(&path, Operation::Prepare, FailureClass::Internal))?;
        let upload = FinalUpload::new(&request, record.upload_id, record.part_size, pointer)?;
        upload.lock().parts = left.prefix;
        let stage = self.upload_stage(&request, upload, point.prefix, found.fact);
        let upload =
            of(&stage).ok_or_else(|| failure(&path, Operation::Prepare, FailureClass::Internal))?;
        // Take the upload over first, then abort whatever else an earlier writer left on the key.
        self.write_pointer(&stage, upload, true).await?;
        let ours = upload.lock().pointer_version.clone();
        let replaced = left.pointer.unwrap_or_default();
        let replaced_gone = upload_pointer::delete_replaced(
            &*self.protocol,
            &upload.pointer,
            replaced.version.as_deref(),
            ours.as_deref(),
        )
        .await;
        upload.forget_replaced(replaced.bytes, replaced_gone);
        upload_pointer::abort_uploads(&*self.protocol, &path, Some(&upload.upload_id)).await?;
        Ok(stage)
    }

    /// A single stage (ADR-0006 C14b). Only the pointer is looked at — no listing, for cost: a
    /// leftover one is removed with the uploads on the key, and the prepare reports the restart.
    async fn prepare_small(
        &self,
        request: DestinationPrepareRequest,
        size: u64,
        pointer: &StoragePath,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        let fact =
            match upload_pointer::read(&*self.protocol, pointer, MAX_POINTER_BYTES + 1).await? {
                None => PrepareFact::Fresh,
                Some(found) => {
                    let reason = leftover_reason(&request, &found.bytes);
                    let stale = [found.bytes.as_slice()];
                    let version = found.version.as_deref();
                    upload_pointer::delete(&*self.protocol, pointer, version, &stale).await?;
                    upload_pointer::abort_uploads(&*self.protocol, path, None).await?;
                    PrepareFact::Restarted { reason }
                }
            };
        let mut stage = self.prepare_single(request.prepare, size);
        stage.mark_at_destination(fact);
        Ok(stage)
    }

    fn upload_stage(
        &self,
        request: &DestinationPrepareRequest,
        upload: FinalUpload,
        write_offset: u64,
        fact: PrepareFact,
    ) -> PreparedStage {
        let final_destination = request.prepare.final_destination.clone();
        let token = Self::encode_token(final_destination.path().as_str(), &upload.upload_id);
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            final_destination,
            token,
            request.prepare.recovery_binding,
            write_offset,
            None,
        );
        stage.backend_state = Some(Arc::new(upload));
        stage.mark_at_destination(fact);
        stage
    }

    /// Begins a new upload on the final key (discovery aborted any other). A recoverable stage
    /// over the automatic interval writes its pointer at once, and so does a resumable one of
    /// known size over it (ADR-0006 C16); any other at its first deferred checkpoint, if it has
    /// one.
    async fn start_upload(
        &self,
        request: &DestinationPrepareRequest,
        part_size: usize,
        pointer: StoragePath,
        fact: PrepareFact,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        let upload_id = self
            .protocol
            .begin_multipart(path.as_str())
            .await
            .map_err(|error| role_failure(path, Operation::Prepare, error))?;
        let upload = FinalUpload::new(request, upload_id, part_size as u64, pointer)?;
        let mut stage = self.upload_stage(request, upload, 0, fact);
        if !self.pointer_at_prepare(request) {
            stage = stage.disable_recovery();
            if !self.pointer_before_checkpoint(request) {
                return Ok(stage);
            }
        }
        let upload =
            of(&stage).ok_or_else(|| failure(path, Operation::Prepare, FailureClass::Internal))?;
        if let Err(error) = self.write_pointer(&stage, upload, false).await {
            // Nothing names this new upload unless the PUT stored the pointer after all; then the
            // next prepare finds a pointer without its upload and cleans it up.
            let aborted = self
                .protocol
                .abort_multipart(path.as_str(), &upload.upload_id)
                .await;
            if let Err(abort) = cleanup_result(path, aborted) {
                tracing::warn!(path = %path.as_str(), ?abort, "could not abort a new S3 upload");
            }
            return Err(error);
        }
        Ok(stage)
    }

    /// Whether a fresh upload is recoverable from prepare, pointer written: a `recoverable` one
    /// over the automatic interval (D3: a pointer only for checkpointed objects over 64 MiB). The
    /// expert
    /// destination half asks for every checkpointed object over one chunk; a smaller one keeps
    /// no pointer and is not resumable.
    fn pointer_at_prepare(&self, request: &DestinationPrepareRequest) -> bool {
        request.recoverable
            && request
                .prepare
                .source
                .size
                .is_none_or(|size| size > self.checkpoint_interval)
    }

    /// Whether an upload that is not recoverable yet still writes its pointer at once: a
    /// resumable one of known size over the automatic interval — the one the engine arms a
    /// deferred checkpoint for (`plan_request` in `transfer/engine.rs`; keep the two in step). A crash (a killed container) before that checkpoint then leaves an
    /// upload the next prepare resumes from its listed parts, instead of up to one interval of
    /// parts without a pointer, which it could only abort. Recovery still turns on at the
    /// checkpoint: a failure or cancellation before it discards the upload and the pointer.
    fn pointer_before_checkpoint(&self, request: &DestinationPrepareRequest) -> bool {
        request.resume == ResumeMode::Discover
            && request
                .prepare
                .source
                .size
                .is_some_and(|size| size > self.checkpoint_interval)
    }

    async fn fence(&self, upload: &FinalUpload) -> Result<Fence, StorageRoleFailure> {
        let found =
            upload_pointer::read(&*self.protocol, &upload.pointer, MAX_POINTER_BYTES + 1).await?;
        Ok(match found {
            None if upload.pointer_written() => Fence::TakenOver,
            None => Fence::Ours { pointer: false },
            Some(stored)
                if DestinationPointer::decode(&stored.bytes)
                    .is_ok_and(|found| Some(found.extension) == upload.extension()) =>
            {
                Fence::Ours { pointer: true }
            }
            Some(_) => Fence::TakenOver,
        })
    }

    /// Writes the pointer: unless `take_over`, only while the pointer is still this stage's.
    async fn write_pointer(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        take_over: bool,
    ) -> Result<(), StorageRoleFailure> {
        let path = stage.final_destination.path();
        if !take_over && matches!(self.fence(upload).await?, Fence::TakenOver) {
            return Err(taken_over(path, Operation::Write));
        }
        let internal = || failure(path, Operation::Write, FailureClass::Internal);
        let bytes = DestinationPointer {
            binding: stage.recovery_binding,
            transfer_identity: upload.transfer_identity,
            durable_prefix: None,
            extension: upload.extension().ok_or_else(internal)?,
        }
        .encode()
        .map_err(|_| internal())?;
        let version = upload_pointer::put(&*self.protocol, &upload.pointer, bytes.clone()).await?;
        upload.record_pointer(version, bytes);
        upload.pointer_written.store(true, Ordering::Release);
        Ok(())
    }

    /// Sends the parts after the resumed prefix; the first deferred checkpoint turns recovery on
    /// (and writes the pointer, if prepare did not). The upload is completed by `publish`.
    pub(super) async fn write_final_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let path = stage.final_destination.path();
        let prefix = {
            let current = upload.lock();
            if current.written.is_some() {
                return Err(entry(
                    path,
                    Operation::Write,
                    "S3 upload is already written",
                ));
            }
            current.parts.clone()
        };
        let resumed_parts = prefix.len();
        let hook = PointerCheckpoint {
            adapter: self,
            stage,
            upload,
        };
        let checkpoint = stage
            .deferred_checkpoint
            .as_ref()
            .filter(|_| !stage.recovery_enabled())
            .map(|checkpoint| (checkpoint.interval_bytes, &hook as &dyn PartsCheckpoint));
        let target = PartTarget {
            path,
            key: path.as_str(),
            upload_id: &upload.upload_id,
            part_size: upload.part_size,
            checkpoint,
        };
        let sent = self.upload_parts(&target, prefix, input).await?;
        let size = stage.write_offset + sent.bytes;
        upload.record_written(path, sent, resumed_parts, size)?;
        Ok(WriteEvidence {
            persisted_bytes: size,
        })
    }

    /// The bytes the upload holds, once the pointer is confirmed still this stage's.
    pub(super) async fn observe_final_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        if matches!(self.fence(upload).await?, Fence::TakenOver) {
            return Err(taken_over(
                stage.final_destination.path(),
                Operation::Observe,
            ));
        }
        let durable_prefix = upload.lock().written.unwrap_or(stage.write_offset);
        Ok(CheckpointObservation { durable_prefix })
    }

    /// Keeps tags for the object, which exists only once the upload completes.
    pub(super) fn apply_final_upload_metadata(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        mutation: MetadataMutation,
        cancel: &CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if self.metadata.is_none() {
            return Err(metadata_unavailable(stage));
        }
        let path = stage.final_destination.path();
        let tags = single::pending_tags(path, self.tags_supported, mutation, cancel)?;
        upload.lock().tags = Some(tags);
        Ok(())
    }

    /// Completes the upload, while it is still this stage's, then sets the pending tags and
    /// removes the pointer.
    pub(super) async fn publish_final_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        request: &PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let path = stage.final_destination.path();
        self.validate(stage).map_err(unchanged)?;
        if request.cancel.is_cancelled() {
            return Err(unchanged(cancelled(path, Operation::Publish)));
        }
        let (parts, md5_etags, tags) = {
            let current = upload.lock();
            if current.written != Some(request.expected_size) || current.published.is_some() {
                return Err(unchanged(entry(
                    path,
                    Operation::Publish,
                    "S3 upload content differs from the published size",
                )));
            }
            (
                current.parts.clone(),
                current.md5_etags,
                current.tags.clone(),
            )
        };
        let Fence::Ours { pointer } = self.fence(upload).await.map_err(unchanged)? else {
            return Err(unchanged(taken_over(path, Operation::Publish)));
        };
        let facts = self
            .complete_final_upload(path, upload, &parts, md5_etags, request.expected_size)
            .await?;
        upload.lock().published = Some(facts.clone());
        if let Some(tags) = tags {
            self.protocol
                .put_tags(path.as_str(), &tags)
                .await
                .map_err(|error| changed(role_failure(path, Operation::Publish, error)))?;
        }
        if pointer {
            self.delete_own_pointer(upload).await.map_err(changed)?;
        }
        Ok(PublicationEvidence {
            final_destination: path.clone(),
            disposition: PublicationDisposition::Published,
            version: facts.version_id,
        })
    }

    /// `CompleteMultipartUpload` with every part. When every part this stage sent came back with
    /// its MD5 as `ETag` and the object's `ETag` has the multipart form, it must be the composite
    /// of the parts; a mismatch is a permanent `Corruption` with the final key changed.
    async fn complete_final_upload(
        &self,
        path: &StoragePath,
        upload: &FinalUpload,
        parts: &[(i32, String)],
        md5_etags: Option<bool>,
        size: u64,
    ) -> Result<S3WriteFacts, PublicationFailure> {
        let etags: Vec<String> = parts.iter().map(|part| part.1.clone()).collect();
        let expected = composite_etag(&etags);
        let facts = match self
            .protocol
            .complete_multipart(path.as_str(), &upload.upload_id, parts)
            .await
        {
            Ok(facts) => facts,
            Err(error) => {
                self.settle_completion(path, upload, size, expected.as_deref(), error)
                    .await?
            }
        };
        match expected {
            Some(expected)
                if md5_etags == Some(true)
                    && is_composite(&facts.etag)
                    && !single::same_etag(&facts.etag, &expected) =>
            {
                Err(changed(classified_entry(
                    path,
                    Operation::Publish,
                    FailureClass::Corruption,
                    Transience::Permanent,
                    "the completed S3 object's ETag is not the composite of its parts",
                )))
            }
            _ => Ok(facts),
        }
    }

    /// Settles a failed completion. Only a refusal the service answered before it could commit
    /// (a 4xx such as `InvalidPart`, `EntityTooSmall`, `AccessDenied`) leaves the final key
    /// unchanged. Any other failure may still complete on the server — a completion whose reply
    /// timed out can finish after `ListParts` still lists the upload — so an upload still listed,
    /// or one that cannot be listed, reports the final key changed. One that is gone
    /// (`NoSuchUpload`) completed if the key's latest version has our size and composite `ETag`,
    /// and that version is claimed (ADR-0006 C17; none in an unversioned bucket); otherwise
    /// something else happened to the key (`Conflict`, changed).
    async fn settle_completion(
        &self,
        path: &StoragePath,
        upload: &FinalUpload,
        size: u64,
        expected: Option<&str>,
        error: S3ProtocolFailure,
    ) -> Result<S3WriteFacts, PublicationFailure> {
        if refused_before_commit(&error) {
            return Err(unchanged(role_failure(path, Operation::Publish, error)));
        }
        let key = path.as_str();
        match self.protocol.list_parts(key, &upload.upload_id).await {
            Err(listed) if is_not_found(&listed) => {}
            _ => return Err(changed(role_failure(path, Operation::Publish, error))),
        }
        let completed = match expected {
            Some(expected) => completed_object(&*self.protocol, path, size, expected, true).await,
            None => None,
        };
        match completed {
            Some(facts) => {
                let aborted = self.protocol.abort_multipart(key, &upload.upload_id).await;
                if let Err(error) = cleanup_result(path, aborted) {
                    tracing::warn!(path = %key, ?error, "could not abort a reconciled S3 upload");
                }
                Ok(facts)
            }
            None => Err(changed(classified_entry(
                path,
                Operation::Publish,
                FailureClass::Conflict,
                Transience::Permanent,
                "the S3 upload is gone and the final object is not the one it would complete",
            ))),
        }
    }

    /// Removes the pointer, then aborts the upload — only while the pointer is still this
    /// stage's: a taken-over upload is the other writer's now. The final key is never touched.
    pub(super) async fn discard_final_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
    ) -> Result<(), StorageRoleFailure> {
        let Fence::Ours { pointer } = self.fence(upload).await? else {
            return Ok(());
        };
        if pointer {
            self.delete_own_pointer(upload).await?;
        }
        let path = stage.final_destination.path();
        cleanup_result(
            path,
            self.protocol
                .abort_multipart(path.as_str(), &upload.upload_id)
                .await,
        )
    }

    /// Deletes the pointer this stage wrote, by the version its write reported (ADR-0006 C17).
    async fn delete_own_pointer(&self, upload: &FinalUpload) -> Result<(), StorageRoleFailure> {
        let (version, stale) = {
            let state = upload.lock();
            (state.pointer_version.clone(), state.stale_pointers.clone())
        };
        let stale: Vec<&[u8]> = stale.iter().map(Vec::as_slice).collect();
        upload_pointer::delete(&*self.protocol, &upload.pointer, version.as_deref(), &stale).await
    }

    /// Reads the completed object back, pinned to what the completion created.
    pub(super) async fn verify_final_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        request: &VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let path = stage.final_destination.path();
        self.validate(stage)?;
        let facts = upload.lock().published.clone().ok_or_else(|| {
            entry(
                path,
                Operation::Verify,
                "S3 object is verified only after publication",
            )
        })?;
        single::verify_written(self, path, facts, request).await
    }
}

#[cfg(test)]
#[path = "at_destination_tests.rs"]
mod tests;
