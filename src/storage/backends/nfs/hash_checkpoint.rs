//! Hash checkpoint hooks keep data barriers separate from hash-only pointer updates.
use super::{
    artifact_path, fence, invalid, read_regular, remove_pointer, stage_path, taken_over,
    write_pointer,
};
use crate::model::{FailureClass, Operation};
use crate::storage::artifacts::ArtifactKind;
use crate::storage::backends::nfs::{source::role_failure, staged::NfsStagedDestinationAdapter};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{PreparedStage, StorageRoleFailure};

pub(super) async fn recover_publication(
    adapter: &NfsStagedDestinationAdapter,
    stage: &mut PreparedStage,
    prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let data = stage
        .restored_checkpoint()
        .filter(|data| data.size == prefix && data.prefix() == prefix)
        .ok_or_else(|| invalid(stage.final_destination.path(), FailureClass::Corruption))?;
    stage.enable_checkpoint(data.size, data.hash);
    stage.write_offset = prefix;
    stage.checkpoint_set_phase(CheckpointPhase::Published);
    stage.recovered_publication = Some(crate::storage::PublicationEvidence {
        final_destination: stage.final_destination.path().clone(),
        disposition: crate::storage::PublicationDisposition::Published,
        version: None,
    });
    adapter.claim_authority(stage.token.clone());
    if let Err(error) = write_pointer(adapter, stage, prefix, true).await {
        adapter.release_authority(stage);
        return Err(error);
    }
    Ok(())
}

pub(in crate::storage::backends::nfs) async fn checkpoint_barrier(
    stage: &PreparedStage,
    prefix: u64,
) -> Result<(), StorageRoleFailure> {
    if stage.checkpoint_enabled() {
        let _guard = stage.checkpoint_guard().await;
        stage.checkpoint_barrier(prefix);
    }
    Ok(())
}

pub(in crate::storage::backends::nfs) async fn finish_publication(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if stage.checkpoint_enabled() {
        stage.checkpoint_set_phase(CheckpointPhase::Published);
        write_pointer(adapter, stage, stage.checkpoint_prefix(), false).await?;
    } else {
        remove_pointer(adapter, stage).await?;
        adapter.release_authority(stage);
    }
    Ok(())
}

pub(in crate::storage::backends::nfs) async fn release_hash_checkpoint(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !stage.checkpoint_enabled() {
        return Ok(());
    }
    stage
        .validate_owner(&adapter.identity)
        .map_err(|_| taken_over(stage.final_destination.path()))?;
    stage_path(stage)?;
    let fence = fence(stage)?;
    let _guard = stage.checkpoint_guard().await;
    if stage.checkpoint_phase() != CheckpointPhase::Published {
        return Err(taken_over(stage.final_destination.path()));
    }
    let path = artifact_path(stage.final_destination.path(), ArtifactKind::Pointer, false)?;
    if let Some(bytes) = read_regular(adapter, &path, MAX_POINTER_BYTES + 1)
        .await
        .map_err(|error| role_failure(stage.final_destination.path(), Operation::Write, error))?
    {
        let found = DestinationPointer::decode(&bytes)
            .map_err(|_| taken_over(stage.final_destination.path()))?;
        if found.extension != fence.extension() {
            return Err(taken_over(stage.final_destination.path()));
        }
    }
    remove_pointer(adapter, stage).await?;
    adapter.release_authority(stage);
    Ok(())
}
