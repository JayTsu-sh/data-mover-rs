//! A single-request prepare may discover a checkpoint written with older multipart sizing.
//! Preserve that upload's geometry and reuse the pointer read for ordinary discovery.

use super::super::source::classified_entry;
use super::upload_discovery::leftover_reason;
use super::upload_pointer::{self, StoredPointer};
use super::{S3Protocol, S3StagedDestination};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES, is_checkpoint_pointer};
use crate::storage::range_hash::RangeHash;
use crate::storage::{
    DestinationPrepareRequest, PrepareFact, PreparedStage, ResumeMode, StorageRoleFailure,
};

pub(super) enum Leftover {
    Cleared(PrepareFact),
    Resume(StoredPointer),
}

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Prepare,
        FailureClass::Unsupported,
        Transience::Permanent,
        "this single-request S3 prepare cannot adopt the hash checkpoint",
    )
}

/// Only the ordinary eligible interface may redirect, with the exact transfer/source binding.
/// An unknown checkpoint format is never interpreted as disposable legacy state.
fn redirects(
    request: &DestinationPrepareRequest,
    found: &StoredPointer,
) -> Result<bool, StorageRoleFailure> {
    if request.resume != ResumeMode::Discover || !is_checkpoint_pointer(&found.bytes) {
        return Ok(false);
    }
    let path = request.prepare.final_destination.path();
    if !request.checkpoint_hash || path.names_directory_object() {
        return Err(unsupported(path));
    }
    let pointer = DestinationPointer::decode(&found.bytes).map_err(|_| unsupported(path))?;
    if upload_pointer::accepted(&pointer).is_none() {
        return Err(unsupported(path));
    }
    let size = validate_hash_pointer(&pointer).ok_or_else(|| unsupported(path))?;
    if pointer.binding != request.prepare.recovery_binding
        || pointer.transfer_identity != request.transfer_identity
        || request.prepare.source.size != Some(size)
    {
        return Err(unsupported(path));
    }
    Ok(true)
}

/// V2 Discover never falls back to destructive legacy cleanup. Multipart discovery repeats
/// these checks when consuming the preloaded pointer before it adopts any parts.
fn validate_hash_pointer(pointer: &DestinationPointer) -> Option<u64> {
    let data = CheckpointData::decode(pointer.checkpoint.as_deref()?).ok()?;
    let hash = RangeHash::decode(&data.hash, data.size).ok()?;
    let valid = pointer
        .durable_prefix
        .is_none_or(|prefix| prefix == data.prefix())
        && data.ranges.iter().all(|range| hash.covers(range.clone()))
        && (data.phase == CheckpointPhase::Receiving
            || (hash.covers(0..data.size) && data.prefix() == data.size));
    valid.then_some(data.size)
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// Fresh and legacy single-request stages retain their original pointer lookup and cleanup.
    /// NAS keeps no progress and performs neither lookup nor multipart discovery.
    pub(super) async fn clear_leftover(
        &self,
        request: &DestinationPrepareRequest,
        pointer: &StoragePath,
    ) -> Result<Leftover, StorageRoleFailure> {
        if !self.protocol.bucket_capabilities().resumable {
            return Ok(Leftover::Cleared(PrepareFact::Fresh));
        }
        let Some(found) =
            upload_pointer::read(&*self.protocol, pointer, MAX_POINTER_BYTES + 1).await?
        else {
            return Ok(Leftover::Cleared(PrepareFact::Fresh));
        };
        if redirects(request, &found)? {
            return Ok(Leftover::Resume(found));
        }
        let path = request.prepare.final_destination.path();
        let reason = leftover_reason(request, &found.bytes);
        upload_pointer::delete(
            &*self.protocol,
            pointer,
            found.version.as_deref(),
            &[found.bytes.as_slice()],
        )
        .await?;
        upload_pointer::abort_uploads(&*self.protocol, path, None).await?;
        Ok(Leftover::Cleared(PrepareFact::Restarted { reason }))
    }

    pub(super) async fn resume_single_upload(
        &self,
        request: DestinationPrepareRequest,
        pointer: StoragePath,
        found: StoredPointer,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        let record = DestinationPointer::decode(&found.bytes)
            .ok()
            .and_then(|pointer| upload_pointer::accepted(&pointer))
            .ok_or_else(|| unsupported(path))?;
        let part_size = usize::try_from(record.part_size).map_err(|_| unsupported(path))?;
        self.prepare_final_upload_preloaded(request, part_size, pointer, Some(found))
            .await
    }
}
