use bytes::Bytes;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use super::staged::{NfsStageState, NfsStagedDestinationAdapter};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{PreparedStage, StorageRoleFailure};

const MAGIC: &[u8; 8] = b"DMNCKP01";
const PAYLOAD_SIZE: usize = 8 + 32 + 32 + 32 + 8;
const RECORD_SIZE: usize = PAYLOAD_SIZE + 32;

pub(super) async fn persist(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    durable_prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let stage_id = NfsStagedDestinationAdapter::stage_id(&stage.token, final_path)?;
    let checkpoint = checkpoint_path(&stage_id, final_path)?;
    let temporary = temporary_path(&stage_id, final_path)?;
    let record = encode(
        stage.recovery_binding,
        final_path,
        &stage_id,
        durable_prefix,
    );
    let handle = adapter
        .protocol
        .create_empty_open(&temporary)
        .await
        .map_err(|error| role_failure(final_path, Operation::Write, error))?;
    let write_result = handle.write_at(0, Bytes::copy_from_slice(&record)).await;
    let close_result = handle.close().await;
    match (write_result, close_result) {
        (Ok(written), Ok(())) if written == RECORD_SIZE as u64 => {}
        (Ok(_), Ok(())) => {
            let _ = adapter.protocol.delete(&temporary).await;
            return Err(invalid(
                final_path,
                Operation::Write,
                FailureClass::Corruption,
            ));
        }
        (Err(error), _) | (_, Err(error)) => {
            let _ = adapter.protocol.delete(&temporary).await;
            return Err(role_failure(final_path, Operation::Write, error));
        }
    }

    if let Err(rename_error) = adapter.protocol.rename(&temporary, &checkpoint).await {
        // RENAME can return an error after the server has committed it. Re-observe the
        // destination before deciding whether the atomic replacement failed.
        let committed = load(adapter, stage.recovery_binding, final_path, &stage_id)
            .await
            .is_ok_and(|prefix| prefix == durable_prefix);
        let _ = adapter.protocol.delete(&temporary).await;
        if !committed {
            return Err(role_failure(final_path, Operation::Write, rename_error));
        }
    }
    checkpoint_state(stage)?
        .checkpoint_created
        .store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

pub(super) async fn load(
    adapter: &NfsStagedDestinationAdapter,
    recovery_binding: [u8; 32],
    final_path: &StoragePath,
    stage_id: &str,
) -> Result<u64, StorageRoleFailure> {
    let path = checkpoint_path(stage_id, final_path)?;
    let handle = adapter
        .protocol
        .open_read(&path)
        .await
        .map_err(|error| role_failure(final_path, Operation::Prepare, error))?;
    let read_result = handle.read_at(0, RECORD_SIZE + 1).await;
    let close_result = handle.close().await;
    let bytes = read_result.map_err(|error| role_failure(final_path, Operation::Prepare, error))?;
    close_result.map_err(|error| role_failure(final_path, Operation::Prepare, error))?;
    decode(&bytes, recovery_binding, final_path, stage_id)
}

pub(super) async fn remove(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let stage_state = checkpoint_state(stage)?;
    if !stage_state
        .checkpoint_created
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Ok(());
    }
    let stage_id = NfsStagedDestinationAdapter::stage_id(&stage.token, final_path)?;
    let path = checkpoint_path(&stage_id, final_path)?;
    match adapter.protocol.delete(&path).await {
        Ok(())
        | Err(NfsProtocolFailure {
            class: FailureClass::NotFound,
            ..
        }) => {
            stage_state
                .checkpoint_created
                .store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        }
        Err(error) => Err(role_failure(final_path, Operation::Write, error)),
    }
}

fn encode(
    recovery_binding: [u8; 32],
    final_path: &StoragePath,
    stage_id: &str,
    prefix: u64,
) -> [u8; RECORD_SIZE] {
    let mut record = [0_u8; RECORD_SIZE];
    record[..8].copy_from_slice(MAGIC);
    record[8..40].copy_from_slice(&recovery_binding);
    record[40..72].copy_from_slice(blake3::hash(final_path.as_str().as_bytes()).as_bytes());
    record[72..104].copy_from_slice(blake3::hash(stage_id.as_bytes()).as_bytes());
    record[104..112].copy_from_slice(&prefix.to_le_bytes());
    let checksum = blake3::hash(&record[..PAYLOAD_SIZE]);
    record[PAYLOAD_SIZE..].copy_from_slice(checksum.as_bytes());
    record
}

fn decode(
    bytes: &[u8],
    recovery_binding: [u8; 32],
    final_path: &StoragePath,
    stage_id: &str,
) -> Result<u64, StorageRoleFailure> {
    if bytes.len() != RECORD_SIZE
        || &bytes[..8] != MAGIC
        || bytes[8..40] != recovery_binding
        || bytes[40..72] != *blake3::hash(final_path.as_str().as_bytes()).as_bytes()
        || bytes[72..104] != *blake3::hash(stage_id.as_bytes()).as_bytes()
        || bytes[PAYLOAD_SIZE..] != *blake3::hash(&bytes[..PAYLOAD_SIZE]).as_bytes()
    {
        return Err(invalid(
            final_path,
            Operation::Prepare,
            FailureClass::Corruption,
        ));
    }
    Ok(u64::from_le_bytes(bytes[104..112].try_into().map_err(
        |_| invalid(final_path, Operation::Prepare, FailureClass::Corruption),
    )?))
}

fn checkpoint_path(
    stage_id: &str,
    final_path: &StoragePath,
) -> Result<StoragePath, StorageRoleFailure> {
    super::staged::sibling_path(final_path, &format!("{stage_id}.checkpoint"))
}

fn temporary_path(
    stage_id: &str,
    final_path: &StoragePath,
) -> Result<StoragePath, StorageRoleFailure> {
    let checkpoint = checkpoint_path(stage_id, final_path)?;
    StoragePath::new(crate::storage::artifacts::temporary_name(
        checkpoint.as_str(),
    ))
    .map_err(|_| invalid(final_path, Operation::Prepare, FailureClass::InvalidInput))
}

fn checkpoint_state(stage: &PreparedStage) -> Result<&NfsStageState, StorageRoleFailure> {
    stage
        .backend_state
        .as_ref()
        .and_then(|state| state.downcast_ref::<NfsStageState>())
        .ok_or_else(|| {
            invalid(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Corruption,
            )
        })
}

fn invalid(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, operation, class, Transience::Permanent)
}
