use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::storage::durability::sync_directory;
use cap_std::fs::OpenOptions;

use super::{LocalStagedDestination, at_destination, failure, io_failure, publication};
use crate::model::{FailureClass, Operation};
use crate::storage::artifacts::ArtifactKind;
use crate::storage::pointer::MAX_POINTER_BYTES;
use crate::storage::{PreparedStage, StorageRoleFailure};

pub(super) async fn persist(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    durable_prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let checkpoint = adapter.checkpoint_name(stage, Operation::Verify)?;
    let (temporary, record) = record_for(stage, durable_prefix)?;
    let staging = adapter.stage_directory(stage, Operation::Verify).await?;
    let probe = Arc::clone(&adapter.write_probe);
    #[cfg(test)]
    probe
        .checkpoint_prefixes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(durable_prefix);
    #[cfg(test)]
    if probe.pause_checkpoint.swap(false, Ordering::SeqCst) {
        probe.checkpoint_started.notify_one();
        probe.checkpoint_release.notified().await;
    }
    tokio::task::spawn_blocking(move || {
        let result = (|| {
            // Never opened over what is there: `create_new` does not follow a symlink.
            publication::remove_if_present(&staging, &temporary)?;
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            let mut file = staging.open_with(&temporary, &options)?.into_std();
            file.write_all(&record)?;
            file.sync_all()?;
            probe.fail_checkpoint_at(1)?;
            staging.rename(&temporary, &staging, &checkpoint)?;
            probe.fail_checkpoint_at(2)?;
            sync_directory(&staging)
        })();
        if result.is_err() {
            let _ = publication::remove_if_present(&staging, &temporary);
        }
        result
    })
    .await
    .map_err(|_| {
        failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Internal,
        )
    })?
    .map_err(|error| io_failure(stage.final_destination.path(), Operation::Verify, &error))
}

/// The pointer's temporary name and bytes. The temporary is a fixed name a crash may have left
/// behind; the claim makes this process its only writer.
fn record_for(
    stage: &PreparedStage,
    durable_prefix: u64,
) -> Result<(OsString, Vec<u8>), StorageRoleFailure> {
    let identity = LocalStagedDestination::transfer_identity(stage, Operation::Verify)?;
    let pointer = at_destination::pointer(stage, identity, durable_prefix)
        .encode()
        .map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Internal,
            )
        })?;
    let temporary =
        at_destination::artifact(stage.final_destination.path(), ArtifactKind::Pointer, true)?;
    Ok((temporary, pointer))
}

pub(super) async fn reobserve(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<u64, StorageRoleFailure> {
    let stage_name = adapter.stage_name(stage, Operation::Verify)?;
    let checkpoint_name = adapter.checkpoint_name(stage, Operation::Verify)?;
    let identity = LocalStagedDestination::transfer_identity(stage, Operation::Verify)?;
    let staging = adapter.stage_directory(stage, Operation::Verify).await?;
    let (record, stage_len) = tokio::task::spawn_blocking(move || {
        let mut record = Vec::new();
        staging
            .open(checkpoint_name)?
            .into_std()
            .take(MAX_POINTER_BYTES as u64 + 1)
            .read_to_end(&mut record)?;
        Ok::<_, io::Error>((record, staging.metadata(stage_name)?.len()))
    })
    .await
    .map_err(|_| {
        failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Internal,
        )
    })?
    .map_err(|error| io_failure(stage.final_destination.path(), Operation::Verify, &error))?;
    let durable_prefix = at_destination::proven_prefix(stage, identity, &record);
    let Some(durable_prefix) = durable_prefix.filter(|prefix| *prefix <= stage_len) else {
        return Err(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        ));
    };
    Ok(durable_prefix)
}
