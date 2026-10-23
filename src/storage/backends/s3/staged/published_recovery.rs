//! Resume after Complete consumed the upload, retaining source H and the final object's pin.

use super::super::source::{classified_entry, role_failure};
use super::super::{S3ObjectFacts, S3Protocol, S3WriteFacts};
use super::S3StagedDestination;
use super::at_destination::{Fence, taken_over};
use super::checkpoint_record::CheckpointRecord;
use super::final_upload::FinalUpload;
use super::single;
use super::upload_pointer::{self, StoredPointer};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::discovery::ResumePoint;
use crate::storage::pointer::DestinationPointer;
use crate::storage::range_hash::RangeHash;
use crate::storage::{
    DestinationPrepareRequest, MetadataMutation, PrepareFact, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    StorageRoleFailure,
};
use tokio_util::sync::CancellationToken;

fn conflict(path: &StoragePath, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Prepare,
        FailureClass::Conflict,
        Transience::Permanent,
        diagnostic,
    )
}

/// Published facts must still name the current final. A Ready checkpoint has no completion
/// receipt: use its provable composite `ETag`, or compare a conditionally pinned final with H.
pub(super) async fn observe_final<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    pointer: &DestinationPointer,
) -> Result<S3WriteFacts, StorageRoleFailure> {
    let record = CheckpointRecord::decode(&pointer.extension)
        .ok_or_else(|| conflict(path, "invalid S3 completion evidence"))?;
    let data = pointer
        .checkpoint
        .as_deref()
        .and_then(|bytes| CheckpointData::decode(bytes).ok())
        .ok_or_else(|| conflict(path, "missing S3 published source root"))?;
    let current = protocol
        .head(path.as_str())
        .await
        .map_err(|error| role_failure(path, Operation::Prepare, error))?;
    if current.size != data.size
        || current.etag.is_empty()
        || !protocol.bucket_capabilities().conditional_reads
    {
        return Err(conflict(
            path,
            "the final S3 object cannot be pinned to the checkpoint",
        ));
    }
    if let Some(published) = record.published {
        if !same_object(&current, &published) {
            return Err(conflict(
                path,
                "the final S3 object was replaced after checkpointed publication",
            ));
        }
        return Ok(published);
    }
    if let Some(expected) = record.completion_etag {
        if !single::same_etag(&current.etag, &expected) {
            return Err(conflict(
                path,
                "the final S3 object differs from the checkpointed multipart completion",
            ));
        }
    } else {
        verify_source_root(protocol, path, &current, &data).await?;
    }
    Ok(S3WriteFacts::new(current.etag, current.version_id))
}

fn same_object(current: &S3ObjectFacts, published: &S3WriteFacts) -> bool {
    single::same_etag(&current.etag, &published.etag)
        && published
            .version_id
            .as_ref()
            .is_none_or(|version| current.version_id.as_ref() == Some(version))
}

async fn verify_source_root<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    current: &S3ObjectFacts,
    data: &CheckpointData,
) -> Result<(), StorageRoleFailure> {
    let expected = RangeHash::decode(&data.hash, data.size)
        .and_then(|hash| hash.finalize())
        .map_err(|_| conflict(path, "incomplete S3 source hash at publication"))?;
    let mut hash = blake3::Hasher::new();
    let mut offset = 0;
    while offset < data.size {
        let end = (offset + super::PART_SIZE as u64).min(data.size);
        let bytes = protocol
            .get_range(path.as_str(), offset..end, current)
            .await
            .map_err(|error| role_failure(path, Operation::Verify, error))?;
        if bytes.len() as u64 != end - offset {
            return Err(conflict(
                path,
                "short final S3 read during completion reconciliation",
            ));
        }
        hash.update(&bytes);
        offset = end;
    }
    if hash.finalize().as_bytes() != &expected {
        return Err(conflict(
            path,
            "the final S3 object differs from the retained source root",
        ));
    }
    Ok(())
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(super) async fn restore_published_upload(
        &self,
        request: &DestinationPrepareRequest,
        upload: FinalUpload,
        point: ResumePoint,
        fact: PrepareFact,
        replaced: Option<StoredPointer>,
        published: Option<S3WriteFacts>,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        let facts = published
            .ok_or_else(|| conflict(path, "S3 final reconciliation returned no identity"))?;
        let record = CheckpointRecord::decode(&point.pointer.extension)
            .ok_or_else(|| conflict(path, "invalid S3 publication checkpoint"))?;
        {
            let mut state = upload.lock();
            state.published = Some(facts.clone());
            state.written = Some(point.prefix);
            state.recovered_receipts = Some(record.receipts);
        }
        let mut stage = self.upload_stage(request, upload, point.prefix, fact);
        stage
            .restore_checkpoint(point.pointer.checkpoint)
            .map_err(|_| conflict(path, "invalid S3 published source checkpoint"))?;
        let restored = stage
            .restored_checkpoint()
            .ok_or_else(|| conflict(path, "missing S3 source root"))?;
        stage.enable_checkpoint(restored.size, restored.hash);
        stage.checkpoint_set_phase(CheckpointPhase::Published);
        stage.recovered_publication = Some(PublicationEvidence {
            final_destination: path.clone(),
            disposition: PublicationDisposition::Published,
            version: facts.version_id,
        });
        let upload = super::final_upload::of(&stage)
            .ok_or_else(|| conflict(path, "missing S3 upload state"))?;
        self.write_pointer(&stage, upload, true).await?;
        let ours = upload.lock().pointer_version.clone();
        let replaced = replaced.unwrap_or_default();
        let deleted = upload_pointer::delete_replaced(
            &*self.protocol,
            &upload.pointer,
            replaced.version.as_deref(),
            ours.as_deref(),
        )
        .await;
        upload.forget_replaced(replaced.bytes, deleted);
        Ok(stage)
    }

    pub(super) async fn publish_recovered_upload(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        request: &PublishRequest,
        evidence: &PublicationEvidence,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let changed = |error| PublicationFailure {
            error,
            final_destination_changed: true,
        };
        let _guard = stage.checkpoint_guard().await;
        if request.expected_size != stage.write_offset || request.cancel.is_cancelled() {
            return Err(changed(conflict(
                stage.final_destination.path(),
                "invalid recovered S3 publication request",
            )));
        }
        if matches!(self.fence(upload).await.map_err(changed)?, Fence::TakenOver) {
            return Err(changed(taken_over(
                stage.final_destination.path(),
                Operation::Publish,
            )));
        }
        Ok(evidence.clone())
    }

    /// Complete and tag application are separate S3 operations. Recovery only accepts tags
    /// already committed on the pinned final; it never repairs metadata on a published object.
    pub(super) async fn validate_recovered_upload_metadata(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        mutations: Vec<MetadataMutation>,
        cancel: &CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let _guard = stage.checkpoint_guard().await;
        let path = stage.final_destination.path();
        let mut expected = None;
        for mutation in mutations {
            expected = Some(single::pending_tags(
                path,
                self.tags_supported,
                mutation,
                cancel,
            )?);
        }
        let Some(mut expected) = expected else {
            return Ok(());
        };
        if self.metadata.is_none() {
            return Err(super::metadata_unavailable(stage));
        }
        let saved = upload
            .lock()
            .published
            .clone()
            .ok_or_else(|| conflict(path, "missing recovered S3 object identity"))?;
        let current = self
            .protocol
            .head(path.as_str())
            .await
            .map_err(|error| role_failure(path, Operation::Metadata, error))?;
        if !same_object(&current, &saved) || matches!(self.fence(upload).await?, Fence::TakenOver) {
            return Err(conflict(path, "recovered S3 metadata target was replaced"));
        }
        let mut observed = self
            .protocol
            .get_tags(path.as_str(), saved.version_id.as_deref())
            .await
            .map_err(|error| role_failure(path, Operation::Metadata, error))?;
        expected.sort_unstable_by(|left, right| {
            left.key()
                .cmp(right.key())
                .then_with(|| left.value().cmp(right.value()))
        });
        observed.sort_unstable_by(|left, right| {
            left.key()
                .cmp(right.key())
                .then_with(|| left.value().cmp(right.value()))
        });
        let current = self
            .protocol
            .head(path.as_str())
            .await
            .map_err(|error| role_failure(path, Operation::Metadata, error))?;
        if !same_object(&current, &saved) || matches!(self.fence(upload).await?, Fence::TakenOver) {
            return Err(conflict(path, "recovered S3 metadata target was replaced"));
        }
        if expected != observed {
            return Err(conflict(
                path,
                "recovered S3 final tags do not match the checkpoint metadata plan",
            ));
        }
        Ok(())
    }
}
