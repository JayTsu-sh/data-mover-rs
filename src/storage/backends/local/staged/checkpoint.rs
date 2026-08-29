use std::io::{self, Read as _, Write as _};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use cap_std::fs::OpenOptions;

use super::{LocalStagedDestination, STAGE_SEQUENCE, failure, io_failure, publication};
use crate::model::{FailureClass, Operation};
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
    let mut temporary = checkpoint.clone();
    temporary.push(format!(
        ".tmp-{:016x}",
        STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let record = record(stage, durable_prefix);
    let staging = adapter
        .open_staging(Operation::Verify, stage.final_destination.path())
        .await?;
    let probe = Arc::clone(&adapter.write_probe);
    tokio::task::spawn_blocking(move || {
        let result = (|| {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            let mut file = staging.open_with(&temporary, &options)?.into_std();
            file.write_all(&record)?;
            file.sync_all()?;
            probe.fail_checkpoint_at(1)?;
            staging.rename(&temporary, &staging, &checkpoint)?;
            probe.fail_checkpoint_at(2)?;
            staging.open(".")?.sync_all()
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

pub(super) async fn reobserve(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<u64, StorageRoleFailure> {
    let stage_name = adapter.stage_name(stage, Operation::Verify)?;
    let checkpoint_name = adapter.checkpoint_name(stage, Operation::Verify)?;
    let expected_hash =
        *blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes();
    let staging = adapter
        .open_staging(Operation::Verify, stage.final_destination.path())
        .await?;
    let (record, stage_len) = tokio::task::spawn_blocking(move || {
        let mut record = Vec::new();
        staging
            .open(checkpoint_name)?
            .into_std()
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
    if record.len() != 112
        || &record[..8] != b"DMLSTG02"
        || record[8..40] != expected_hash
        || record[40..72] != stage.recovery_binding
        || record[80..112] != *blake3::hash(&record[..80]).as_bytes()
    {
        return Err(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        ));
    }
    let mut durable_bytes = [0_u8; 8];
    durable_bytes.copy_from_slice(&record[72..80]);
    let durable_prefix = u64::from_le_bytes(durable_bytes);
    if durable_prefix > stage_len {
        return Err(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        ));
    }
    Ok(durable_prefix)
}
