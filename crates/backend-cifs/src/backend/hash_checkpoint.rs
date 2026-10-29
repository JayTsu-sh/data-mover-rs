//! Hash-only persistence never flushes data or promotes confirmed writes.
use std::sync::atomic::Ordering;

use super::{
    artifact_path, fence, read_regular, remove_pointer, stage_path, taken_over, write_pointer,
};
use crate::model::{FailureClass, Operation};
use crate::storage::artifacts::ArtifactKind;
use crate::storage::backends::cifs::{
    source::entry_failure,
    staged::{CifsStagedDestination, state},
};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{PreparedStage, StorageRoleFailure};

pub(super) async fn recover_publication(
    adapter: &CifsStagedDestination,
    stage: &mut PreparedStage,
    prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let data = stage
        .restored_checkpoint()
        .filter(|data| data.size == prefix && data.prefix() == prefix)
        .ok_or_else(|| {
            entry_failure(
                stage.final_destination().path(),
                Operation::Prepare,
                FailureClass::Corruption,
            )
        })?;
    stage.enable_checkpoint(data.size, data.hash);
    stage.set_write_offset(prefix);
    stage.checkpoint_set_phase(CheckpointPhase::Published);
    stage.set_recovered_publication(Some(crate::storage::PublicationEvidence::new(
        stage.final_destination().path().clone(),
        crate::storage::PublicationDisposition::Published,
        None,
    )));
    state(stage)?.published.store(true, Ordering::Release);
    adapter.claim(stage.token().clone());
    if let Err(error) = write_pointer(adapter, stage, prefix, true).await {
        adapter.release(stage.token());
        return Err(error);
    }
    Ok(())
}

pub(in crate::backend) async fn checkpoint_barrier(
    stage: &PreparedStage,
    prefix: u64,
) -> Result<(), StorageRoleFailure> {
    if stage.checkpoint_enabled() {
        let _guard = stage.checkpoint_guard().await;
        stage.checkpoint_barrier(prefix);
    }
    Ok(())
}

pub(in crate::backend) async fn finish_publication(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if stage.checkpoint_enabled() {
        stage.checkpoint_set_phase(CheckpointPhase::Published);
        write_pointer(adapter, stage, stage.checkpoint_prefix(), false).await?;
    } else {
        remove_pointer(adapter, stage).await?;
        adapter.release(stage.token());
    }
    Ok(())
}

pub(in crate::backend) async fn release_hash_checkpoint(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !stage.checkpoint_enabled() {
        return Ok(());
    }
    let final_path = stage.final_destination().path();
    stage
        .validate_owner(adapter.identity())
        .map_err(|_| taken_over(final_path, Operation::Write))?;
    stage_path(stage)?;
    let fence = fence(stage)?;
    let _guard = stage.checkpoint_guard().await;
    if stage.checkpoint_phase() != CheckpointPhase::Published {
        return Err(taken_over(final_path, Operation::Write));
    }
    let path = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    if let Some(bytes) = read_regular(
        adapter,
        &path,
        MAX_POINTER_BYTES + 1,
        Operation::Write,
        true,
    )
    .await?
    {
        let found = DestinationPointer::decode(&bytes)
            .map_err(|_| taken_over(final_path, Operation::Write))?;
        if found.extension != fence.extension() {
            return Err(taken_over(final_path, Operation::Write));
        }
    }
    remove_pointer(adapter, stage).await?;
    adapter.release(stage.token());
    Ok(())
}
