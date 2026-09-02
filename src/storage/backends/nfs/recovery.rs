use bytes::Bytes;

use super::source::{entry_failure, role_failure};
use super::staged::NfsStagedDestinationAdapter;
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{PreparedStage, RecoverRequest, RecoveryIdentity, StorageRoleFailure};

const MAGIC: &[u8; 8] = b"DMNRCV01";
const FIXED_PREFIX: usize = 74;
const CHECKSUM_SIZE: usize = 32;

pub(super) async fn export(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<RecoveryIdentity, StorageRoleFailure> {
    let _ = adapter.reobserve_checkpoint(stage).await?;
    let token_len = u16::try_from(stage.token.len()).map_err(|_| invalid_stage(stage))?;
    let mut bytes = Vec::with_capacity(FIXED_PREFIX + stage.token.len() + CHECKSUM_SIZE);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&stage.recovery_binding);
    bytes.extend_from_slice(
        blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
    );
    bytes.extend_from_slice(&token_len.to_le_bytes());
    bytes.extend_from_slice(&stage.token);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    RecoveryIdentity::from_bytes(Bytes::from(bytes)).map_err(|_| invalid_stage(stage))
}

pub(super) async fn handoff(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<RecoveryIdentity, StorageRoleFailure> {
    let identity = export(adapter, stage).await?;
    adapter.release_authority(stage);
    Ok(identity)
}

pub(super) async fn recover(
    adapter: &NfsStagedDestinationAdapter,
    request: RecoverRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let token = decode(&request)?;
    let previous = NfsStagedDestinationAdapter::validate_token_shape(
        &token,
        request.final_destination.path(),
    )?;
    let mut claim_hasher = blake3::Hasher::new();
    claim_hasher.update(b"data-mover/nfs-recovery-claim/v1\0");
    claim_hasher.update(request.identity.as_bytes());
    claim_hasher.update(&request.claim_token);
    let claim_hash = claim_hasher.finalize();
    let claimed_token = Bytes::from(format!(
        ".data-mover-staging/recovered-{}.part",
        &claim_hash.to_hex()[..32]
    ));
    let claimed = NfsStagedDestinationAdapter::validate_token_shape(
        &claimed_token,
        request.final_destination.path(),
    )?;
    let durable_prefix = claim_durable_prefix(adapter, &request, &previous, &claimed).await?;
    if !adapter.claim_authority(claimed_token.clone()) {
        return Err(entry_failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Conflict,
            Transience::Transient,
        ));
    }
    Ok(PreparedStage::new(
        adapter.identity.clone(),
        request.final_destination,
        claimed_token,
        request.recovery_binding,
        durable_prefix,
        None,
    ))
}

async fn claim_durable_prefix(
    adapter: &NfsStagedDestinationAdapter,
    request: &RecoverRequest,
    previous: &crate::model::StoragePath,
    claimed: &crate::model::StoragePath,
) -> Result<u64, StorageRoleFailure> {
    match adapter.protocol.size(claimed).await {
        Ok(size) => return Ok(size),
        Err(error) if error.class == FailureClass::NotFound => {}
        Err(error) => {
            return Err(role_failure(
                request.final_destination.path(),
                Operation::Prepare,
                error,
            ));
        }
    }
    let expected = adapter.protocol.size(previous).await.map_err(|error| {
        role_failure(request.final_destination.path(), Operation::Prepare, error)
    })?;
    let rename_result = adapter.protocol.rename(previous, claimed).await;
    let claimed_size = adapter.protocol.size(claimed).await;
    if claimed_size.as_ref().is_ok_and(|size| *size == expected) {
        return Ok(expected);
    }
    let previous_consumed = adapter
        .protocol
        .size(previous)
        .await
        .is_err_and(|error| error.class == FailureClass::NotFound);
    if previous_consumed
        && claimed_size
            .as_ref()
            .is_err_and(|error| error.class == FailureClass::NotFound)
    {
        return Err(entry_failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Conflict,
            Transience::Permanent,
        ));
    }
    let error = rename_result
        .err()
        .or_else(|| claimed_size.err())
        .unwrap_or_else(super::source::NfsProtocolFailure::protocol);
    Err(role_failure(
        request.final_destination.path(),
        Operation::Prepare,
        error,
    ))
}

fn decode(request: &RecoverRequest) -> Result<Bytes, StorageRoleFailure> {
    let bytes = request.identity.as_bytes();
    if bytes.len() < FIXED_PREFIX + CHECKSUM_SIZE || &bytes[..8] != MAGIC {
        return Err(invalid_request(request, FailureClass::Corruption));
    }
    let token_len = u16::from_le_bytes([bytes[72], bytes[73]]) as usize;
    let token_end = FIXED_PREFIX
        .checked_add(token_len)
        .ok_or_else(|| invalid_request(request, FailureClass::Corruption))?;
    if token_end + CHECKSUM_SIZE != bytes.len()
        || bytes[8..40] != request.recovery_binding
        || bytes[40..72]
            != *blake3::hash(request.final_destination.path().as_str().as_bytes()).as_bytes()
        || bytes[token_end..] != *blake3::hash(&bytes[..token_end]).as_bytes()
    {
        return Err(invalid_request(request, FailureClass::Conflict));
    }
    Ok(bytes.slice(FIXED_PREFIX..token_end))
}

fn invalid_stage(stage: &PreparedStage) -> StorageRoleFailure {
    entry_failure(
        stage.final_destination.path(),
        Operation::Verify,
        FailureClass::Corruption,
        Transience::Permanent,
    )
}

fn invalid_request(request: &RecoverRequest, class: FailureClass) -> StorageRoleFailure {
    entry_failure(
        request.final_destination.path(),
        Operation::Prepare,
        class,
        Transience::Permanent,
    )
}
