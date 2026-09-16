//! Durable per-stage prefix records: data FLUSH precedes record replacement.
use super::source::{classify, entry_failure};
use super::staged::{CifsStagedDestination, token_path};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::{PreparedStage, StorageRoleFailure};
use bytes::Bytes;
const MAGIC: &[u8; 8] = b"DMCCKP01";
const LENGTH: usize = 112;

pub(super) fn path(stage: &StoragePath) -> Result<StoragePath, StorageRoleFailure> {
    let (parent, name) = stage
        .as_str()
        .rsplit_once('/')
        .unwrap_or(("", stage.as_str()));
    let base_name = name.split_once(".claim-").map_or(name, |(base, _)| base);
    let base = if parent.is_empty() {
        base_name.to_owned()
    } else {
        format!("{parent}/{base_name}")
    };
    StoragePath::new(format!("{base}.checkpoint")).map_err(|_| invalid(stage))
}
fn invalid(path: &StoragePath) -> StorageRoleFailure {
    entry_failure(path, Operation::Observe, FailureClass::Corruption)
}

pub(super) async fn persist(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let stage_path = token_path(&stage.token, stage.final_destination.path())?;
    let checkpoint = path(&stage_path)?;
    let temporary = StoragePath::new(crate::storage::artifacts::temporary_name(
        checkpoint.as_str(),
    ))
    .map_err(|_| invalid(&checkpoint))?;
    let mut record = Vec::with_capacity(LENGTH);
    record.extend_from_slice(MAGIC);
    record.extend_from_slice(&stage.recovery_binding);
    record.extend_from_slice(blake3::hash(checkpoint.as_str().as_bytes()).as_bytes());
    record.extend_from_slice(&prefix.to_le_bytes());
    record.extend_from_slice(blake3::hash(&record).as_bytes());
    let result = write_record(adapter, &temporary, Bytes::from(record)).await;
    if let Err(error) = result {
        let _ = adapter.protocol.delete(&temporary).await;
        return Err(error);
    }
    if let Err(error) = adapter.protocol.rename(&temporary, &checkpoint, true).await {
        let committed = load(adapter, &stage_path, &stage.recovery_binding)
            .await
            .is_ok_and(|value| value == prefix);
        let _ = adapter.protocol.delete(&temporary).await;
        if !committed {
            return Err(classify(&checkpoint, Operation::Write, &error));
        }
    }
    super::staged::state(stage)?
        .checkpoint_created
        .store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

async fn write_record(
    adapter: &CifsStagedDestination,
    path: &StoragePath,
    mut bytes: Bytes,
) -> Result<(), StorageRoleFailure> {
    adapter
        .protocol
        .create_empty(path)
        .await
        .map_err(|e| classify(path, Operation::Write, &e))?;
    let file = adapter
        .protocol
        .open(path)
        .await
        .map_err(|e| classify(path, Operation::Write, &e))?;
    let result = async {
        let maximum = file.maximum_write_chunk() as usize;
        if maximum == 0 {
            return Err(invalid(path));
        }
        let mut offset = 0;
        while !bytes.is_empty() {
            let part = bytes.split_to(bytes.len().min(maximum));
            let length = part.len() as u64;
            file.write_all_at(offset, part)
                .await
                .map_err(|e| classify(path, Operation::Write, &e))?;
            offset += length;
        }
        file.flush()
            .await
            .map_err(|e| classify(path, Operation::Write, &e))
    }
    .await;
    let close = file
        .close()
        .await
        .map_err(|e| classify(path, Operation::Write, &e));
    result.and(close)
}

pub(super) async fn load(
    adapter: &CifsStagedDestination,
    stage: &StoragePath,
    binding: &[u8; 32],
) -> Result<u64, StorageRoleFailure> {
    let path = path(stage)?;
    if adapter
        .protocol
        .size(&path)
        .await
        .map_err(|e| classify(&path, Operation::Observe, &e))?
        != LENGTH as u64
    {
        return Err(invalid(&path));
    }
    let file = adapter
        .protocol
        .open(&path)
        .await
        .map_err(|e| classify(&path, Operation::Observe, &e))?;
    let result = async {
        let mut record = Vec::with_capacity(LENGTH);
        while record.len() < LENGTH {
            let count = (LENGTH - record.len()).min(file.maximum_read_chunk() as usize);
            if count == 0 {
                return Err(invalid(&path));
            }
            let bytes = file
                .read_at(
                    record.len() as u64,
                    u32::try_from(count).map_err(|_| invalid(&path))?,
                )
                .await
                .map_err(|e| classify(&path, Operation::Observe, &e))?;
            if bytes.len() != count {
                return Err(invalid(&path));
            }
            record.extend_from_slice(&bytes);
        }
        if &record[..8] != MAGIC
            || &record[8..40] != binding
            || record[40..72] != *blake3::hash(path.as_str().as_bytes()).as_bytes()
            || record[80..] != *blake3::hash(&record[..80]).as_bytes()
        {
            return Err(invalid(&path));
        }
        Ok(u64::from_le_bytes(
            record[72..80].try_into().map_err(|_| invalid(&path))?,
        ))
    }
    .await;
    let close = file
        .close()
        .await
        .map_err(|e| classify(&path, Operation::Observe, &e));
    let prefix = result?;
    close?;
    Ok(prefix)
}

pub(super) async fn remove(
    adapter: &CifsStagedDestination,
    stage: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let path = path(stage)?;
    match adapter.protocol.delete(&path).await {
        Ok(()) => Ok(()),
        Err(e) if super::staged::is_not_found(&e) => Ok(()),
        Err(e) => Err(classify(&path, Operation::Namespace, &e)),
    }
}
