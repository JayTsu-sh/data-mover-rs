use std::ffi::{OsStr, OsString};
use std::io::{self, Read as _, Write as _};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::storage::durability::sync_directory;
use cap_std::fs::OpenOptions;

use super::{LocalStagedDestination, at_destination, failure, io_failure, publication};
use crate::model::{FailureClass, Operation};
use crate::storage::artifacts::{ArtifactKind, temporary_name};
use crate::storage::pointer::MAX_POINTER_BYTES;
use crate::storage::{PreparedStage, StorageRoleFailure};

fn record(stage: &PreparedStage, durable_prefix: u64) -> [u8; 112] {
    let mut record = [0_u8; 112];
    record[..8].copy_from_slice(b"DMLSTG02");
    record[8..40].copy_from_slice(
        blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
    );
    record[40..72].copy_from_slice(&stage.recovery_binding);
    record[72..80].copy_from_slice(&durable_prefix.to_le_bytes());
    let checksum = blake3::hash(&record[..80]);
    record[80..].copy_from_slice(checksum.as_bytes());
    record
}

pub(super) async fn persist(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    durable_prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let checkpoint = adapter.checkpoint_name(stage, Operation::Verify)?;
    let (temporary, record, replace_temporary) = record_for(stage, &checkpoint, durable_prefix)?;
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
            if replace_temporary {
                // Never opened over what is there: `create_new` does not follow a symlink.
                publication::remove_if_present(&staging, &temporary)?;
            }
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

/// The temporary to write, the record, and whether the temporary is a fixed name a crash may have
/// left behind (a destination-kept stage's pointer; the claim makes this process its only writer).
fn record_for(
    stage: &PreparedStage,
    checkpoint: &OsStr,
    durable_prefix: u64,
) -> Result<(OsString, Vec<u8>, bool), StorageRoleFailure> {
    if !stage.at_destination {
        let temporary = OsString::from(temporary_name(&checkpoint.to_string_lossy()));
        return Ok((temporary, record(stage, durable_prefix).to_vec(), false));
    }
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
    Ok((temporary, pointer, true))
}

pub(super) async fn reobserve(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<u64, StorageRoleFailure> {
    let stage_name = adapter.stage_name(stage, Operation::Verify)?;
    let checkpoint_name = adapter.checkpoint_name(stage, Operation::Verify)?;
    let identity = stage
        .at_destination
        .then(|| LocalStagedDestination::transfer_identity(stage, Operation::Verify))
        .transpose()?;
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
    let durable_prefix = match identity {
        Some(identity) => at_destination::proven_prefix(stage, identity, &record),
        None => legacy_prefix(stage, &record),
    };
    let Some(durable_prefix) = durable_prefix.filter(|prefix| *prefix <= stage_len) else {
        return Err(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        ));
    };
    Ok(durable_prefix)
}

/// The prefix a `DMLSTG02` record proves for this stage, or `None` if it is not this stage's.
fn legacy_prefix(stage: &PreparedStage, record: &[u8]) -> Option<u64> {
    let expected_hash =
        *blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes();
    if record.len() != 112
        || &record[..8] != b"DMLSTG02"
        || record[8..40] != expected_hash
        || record[40..72] != stage.recovery_binding
        || record[80..112] != *blake3::hash(&record[..80]).as_bytes()
    {
        return None;
    }
    let mut durable_bytes = [0_u8; 8];
    durable_bytes.copy_from_slice(&record[72..80]);
    Some(u64::from_le_bytes(durable_bytes))
}
