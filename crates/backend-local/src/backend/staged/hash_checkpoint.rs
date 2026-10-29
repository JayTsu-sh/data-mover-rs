//! Local hash checkpoint lifecycle. Hash-only persistence never promotes pending data.

use super::{LocalStagedDestination, at_destination, checkpoint, failure};
use crate::model::{FailureClass, Operation};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::{PreparedStage, StorageRoleFailure};

/// The shared body also serves release, whose checkpoint gate is already held.
pub(super) async fn cleanup(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    operation: Operation,
) -> Result<(), StorageRoleFailure> {
    let _guard = if stage.checkpoint_enabled() && at_destination::holds_claim(stage) {
        Some(lock(stage).await?)
    } else {
        None
    };
    at_destination::cleanup_locked(adapter, stage, operation).await
}

#[cfg(unix)]
pub(super) fn claim_matches(
    seen: &cap_std::fs::Metadata,
    file: &std::fs::File,
) -> std::io::Result<bool> {
    Ok(at_destination::same_file(seen, &file.metadata()?))
}

#[cfg(windows)]
pub(super) fn claim_matches(
    seen: &cap_std::fs::Metadata,
    file: &std::fs::File,
) -> std::io::Result<bool> {
    use cap_primitives::fs::_WindowsByHandle as _;
    let opened = cap_std::fs::Metadata::from_file(file)?;
    let named_id = seen.volume_serial_number().zip(seen.file_index());
    let held_id = opened.volume_serial_number().zip(opened.file_index());
    Ok(named_id
        .zip(held_id)
        .is_some_and(|(named, held)| named == held))
}

#[cfg(not(any(unix, windows)))]
pub(super) fn claim_matches(
    _seen: &cap_std::fs::Metadata,
    _file: &std::fs::File,
) -> std::io::Result<bool> {
    Ok(false)
}

pub(super) async fn lock(
    stage: &PreparedStage,
) -> Result<tokio::sync::MutexGuard<'_, ()>, StorageRoleFailure> {
    let cached =
        LocalStagedDestination::local_state(stage, Operation::Verify)?.ok_or_else(|| {
            failure(
                stage.final_destination().path(),
                Operation::Verify,
                FailureClass::Conflict,
            )
        })?;
    let guard = stage.checkpoint_guard().await;
    if !at_destination::holds_claim(stage) {
        return Err(failure(
            stage.final_destination().path(),
            Operation::Verify,
            FailureClass::Conflict,
        ));
    }
    at_destination::fence_claim(stage, std::sync::Arc::clone(&cached.directory)).await?;
    Ok(guard)
}

pub(super) async fn persist(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !stage.checkpoint_enabled() {
        return Ok(());
    }
    checkpoint::persist(adapter, stage, stage.checkpoint_prefix()).await?;
    stage.set_recovery_enabled(true);
    Ok(())
}

pub(super) async fn barrier(stage: &PreparedStage, prefix: u64) -> Result<(), StorageRoleFailure> {
    if stage.checkpoint_enabled() {
        let _guard = lock(stage).await?;
        stage.checkpoint_barrier(prefix);
    }
    Ok(())
}

pub(super) async fn published(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    stage.checkpoint_set_phase(CheckpointPhase::Published);
    persist(adapter, stage).await
}

pub(super) async fn release(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !stage.checkpoint_enabled() {
        return Ok(());
    }
    stage.validate_owner(&adapter.identity).map_err(|_| {
        failure(
            stage.final_destination().path(),
            Operation::Verify,
            FailureClass::Conflict,
        )
    })?;
    // Cleanup is idempotent after releasing the claim and never removes the final file.
    if !at_destination::holds_claim(stage) {
        return Ok(());
    }
    let _guard = lock(stage).await?;
    if stage.checkpoint_phase() != CheckpointPhase::Published {
        return Err(failure(
            stage.final_destination().path(),
            Operation::Verify,
            FailureClass::Conflict,
        ));
    }
    if stage.recovered_publication().is_some() {
        super::published_recovery::validate(adapter, stage).await?;
    }
    at_destination::cleanup_locked(adapter, stage, Operation::Verify).await
}
