use bytes::Bytes;

use super::{LocalStagedDestination, failure};
use crate::model::{FailureClass, Operation};
use crate::storage::{PreparedStage, RecoverRequest, RecoveryIdentity, StorageRoleFailure};

pub(super) async fn export(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<RecoveryIdentity, StorageRoleFailure> {
    let _ = adapter.reobserve_checkpoint(stage).await?;
    let token_len = u16::try_from(stage.token.len()).map_err(|_| {
        failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        )
    })?;
    let mut bytes = Vec::with_capacity(106 + stage.token.len());
    bytes.extend_from_slice(b"DMLRCV01");
    bytes.extend_from_slice(&stage.recovery_binding);
    bytes.extend_from_slice(
        blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
    );
    bytes.extend_from_slice(&token_len.to_le_bytes());
    bytes.extend_from_slice(&stage.token);
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    RecoveryIdentity::from_bytes(Bytes::from(bytes)).map_err(|_| {
        failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Corruption,
        )
    })
}

pub(super) async fn recover(
    adapter: &LocalStagedDestination,
    request: RecoverRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let bytes = request.identity.as_bytes();
    if bytes.len() < 106 || &bytes[..8] != b"DMLRCV01" {
        return Err(invalid(&request, FailureClass::Corruption));
    }
    let token_len = u16::from_le_bytes([bytes[72], bytes[73]]) as usize;
    let token_end = 74_usize
        .checked_add(token_len)
        .ok_or_else(|| invalid(&request, FailureClass::Corruption))?;
    if token_end + 32 != bytes.len()
        || bytes[8..40] != request.recovery_binding
        || bytes[40..72]
            != *blake3::hash(request.final_destination.path().as_str().as_bytes()).as_bytes()
        || bytes[token_end..] != *blake3::hash(&bytes[..token_end]).as_bytes()
    {
        return Err(invalid(&request, FailureClass::Conflict));
    }
    let mut stage = PreparedStage::new(
        adapter.identity.clone(),
        request.final_destination,
        bytes.slice(74..token_end),
        request.recovery_binding,
        0,
        None,
    );
    let claim = adapter.acquire_claim(&stage, false).await?;
    stage.claim = std::sync::Mutex::new(Some(claim));
    stage.write_offset = adapter.reobserve_checkpoint(&stage).await?;
    Ok(stage)
}

fn invalid(request: &RecoverRequest, class: FailureClass) -> StorageRoleFailure {
    failure(request.final_destination.path(), Operation::Prepare, class)
}
