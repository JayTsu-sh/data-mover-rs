use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use blake3::Hasher;
use bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;

use super::protocol::{HdfsProtocol, cancelled, entry_failure};
use crate::model::{BackendIdentity, EntryKind, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, FinalDestination, PrepareRequest,
    PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};

const STAGE_TOKEN_MAGIC: &[u8] = b"hdfs-stage-v1\0";
const RECOVERY_MAGIC: &[u8] = b"hdfs-recovery-v1\0";

struct HdfsStageToken {
    expected_size: u64,
    nonce: [u8; 16],
    base_path: StoragePath,
    partial_path: StoragePath,
}

struct HdfsRecoveryToken {
    binding: [u8; 32],
    expected_size: u64,
    nonce: [u8; 16],
    base_path: StoragePath,
    final_hash: [u8; 32],
}

impl HdfsStageToken {
    fn encode(&self) -> Result<Bytes, StorageRoleFailure> {
        let mut value = BytesMut::from(STAGE_TOKEN_MAGIC);
        value.extend_from_slice(&self.expected_size.to_le_bytes());
        value.extend_from_slice(&self.nonce);
        let base = self.base_path.as_str().as_bytes();
        let base_len = u16::try_from(base.len()).map_err(|_| {
            failure(
                &self.base_path,
                Operation::Prepare,
                FailureClass::InvalidInput,
            )
        })?;
        value.extend_from_slice(&base_len.to_le_bytes());
        value.extend_from_slice(base);
        value.extend_from_slice(self.partial_path.as_str().as_bytes());
        Ok(value.freeze())
    }

    fn decode(stage: &PreparedStage) -> Result<Self, StorageRoleFailure> {
        let payload = stage.token.strip_prefix(STAGE_TOKEN_MAGIC).ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
            )
        })?;
        let (size, payload) = payload.split_at_checked(size_of::<u64>()).ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
            )
        })?;
        let expected_size = u64::from_le_bytes(size.try_into().map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
            )
        })?);
        let (nonce, payload) = take(payload, 16, stage.final_destination.path())?;
        let (base_path, partial_path) =
            decode_stage_paths(payload, stage.final_destination.path())?;
        Ok(Self {
            expected_size,
            nonce: array(nonce, stage.final_destination.path())?,
            base_path,
            partial_path,
        })
    }
}

fn decode_stage_paths(
    payload: &[u8],
    diagnostic: &StoragePath,
) -> Result<(StoragePath, StoragePath), StorageRoleFailure> {
    let (base_len, payload) = take(payload, 2, diagnostic)?;
    let base_len = usize::from(u16::from_le_bytes(array(base_len, diagnostic)?));
    let (base, partial) = take(payload, base_len, diagnostic)?;
    Ok((
        decode_stage_path(base, diagnostic)?,
        decode_stage_path(partial, diagnostic)?,
    ))
}

fn decode_stage_path(
    bytes: &[u8],
    diagnostic: &StoragePath,
) -> Result<StoragePath, StorageRoleFailure> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| failure(diagnostic, Operation::Prepare, FailureClass::Protocol))?;
    StoragePath::new(text)
        .map_err(|_| failure(diagnostic, Operation::Prepare, FailureClass::Protocol))
}

impl HdfsRecoveryToken {
    fn encode(&self) -> Result<RecoveryIdentity, StorageRoleFailure> {
        let path = self.base_path.as_str().as_bytes();
        let path_len = u16::try_from(path.len()).map_err(|_| {
            failure(
                &self.base_path,
                Operation::Prepare,
                FailureClass::InvalidInput,
            )
        })?;
        let mut value = BytesMut::from(RECOVERY_MAGIC);
        value.extend_from_slice(&self.binding);
        value.extend_from_slice(&self.expected_size.to_le_bytes());
        value.extend_from_slice(&self.nonce);
        value.extend_from_slice(&path_len.to_le_bytes());
        value.extend_from_slice(path);
        value.extend_from_slice(&self.final_hash);
        let checksum = blake3::hash(&value);
        value.extend_from_slice(checksum.as_bytes());
        RecoveryIdentity::from_bytes(value.freeze()).map_err(|_| {
            failure(
                &self.base_path,
                Operation::Prepare,
                FailureClass::InvalidInput,
            )
        })
    }

    fn decode(identity: &RecoveryIdentity, path: &StoragePath) -> Result<Self, StorageRoleFailure> {
        let bytes = identity.as_bytes();
        let checksum_offset = bytes
            .len()
            .checked_sub(32)
            .ok_or_else(|| failure(path, Operation::Prepare, FailureClass::Corruption))?;
        let (payload, checksum) = bytes.split_at(checksum_offset);
        if checksum != blake3::hash(payload).as_bytes() {
            return Err(failure(path, Operation::Prepare, FailureClass::Corruption));
        }
        decode_recovery_payload(payload, path)
    }
}

fn decode_recovery_payload(
    payload: &[u8],
    path: &StoragePath,
) -> Result<HdfsRecoveryToken, StorageRoleFailure> {
    let payload = payload
        .strip_prefix(RECOVERY_MAGIC)
        .ok_or_else(|| failure(path, Operation::Prepare, FailureClass::Corruption))?;
    let (binding, payload) = take(payload, 32, path)?;
    let (expected, payload) = take(payload, 8, path)?;
    let (nonce, payload) = take(payload, 16, path)?;
    let (path_len, payload) = take(payload, 2, path)?;
    let path_len = usize::from(u16::from_le_bytes(array(path_len, path)?));
    let (base_path, payload) = take(payload, path_len, path)?;
    let (final_hash, trailing) = take(payload, 32, path)?;
    if !trailing.is_empty() {
        return Err(failure(path, Operation::Prepare, FailureClass::Corruption));
    }
    Ok(HdfsRecoveryToken {
        binding: array(binding, path)?,
        expected_size: u64::from_le_bytes(array(expected, path)?),
        nonce: array(nonce, path)?,
        base_path: StoragePath::new(
            std::str::from_utf8(base_path)
                .map_err(|_| failure(path, Operation::Prepare, FailureClass::Corruption))?,
        )
        .map_err(|_| failure(path, Operation::Prepare, FailureClass::Corruption))?,
        final_hash: array(final_hash, path)?,
    })
}

fn take<'a>(
    bytes: &'a [u8],
    count: usize,
    path: &StoragePath,
) -> Result<(&'a [u8], &'a [u8]), StorageRoleFailure> {
    bytes
        .split_at_checked(count)
        .ok_or_else(|| failure(path, Operation::Prepare, FailureClass::Corruption))
}

fn array<const N: usize>(bytes: &[u8], path: &StoragePath) -> Result<[u8; N], StorageRoleFailure> {
    bytes
        .try_into()
        .map_err(|_| failure(path, Operation::Prepare, FailureClass::Corruption))
}

pub(super) struct HdfsStagedDestination {
    protocol: Arc<dyn HdfsProtocol>,
    identity: BackendIdentity,
}

impl HdfsStagedDestination {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    fn part(&self, stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Conflict,
            )
        })?;
        Ok(HdfsStageToken::decode(stage)?.partial_path)
    }
}

#[async_trait]
impl StagedDestination for HdfsStagedDestination {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let expected = request.source.size.ok_or_else(|| {
            failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Unsupported,
            )
        })?;
        let nonce = *uuid::Uuid::new_v4().as_bytes();
        let part = partial_path(&request.final_destination, request.recovery_binding, &nonce)?;
        self.protocol.create_empty_stage_exclusive(&part).await?;
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            HdfsStageToken {
                expected_size: expected,
                nonce,
                base_path: part.clone(),
                partial_path: part,
            }
            .encode()?,
            request.recovery_binding,
            0,
            None,
        ))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        let token = HdfsStageToken::decode(stage)?;
        let observed = self.protocol.stat(&self.part(stage)?).await?;
        if observed.kind != EntryKind::File
            || observed.size.is_none_or(|size| size > token.expected_size)
        {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Corruption,
            ));
        }
        encode_recovery(stage, &token)
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        recover(self, request).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let part = self.part(stage)?;
        let expected = expected_size(stage)?;
        let persisted = self
            .protocol
            .append_stage(&part, stage.write_offset, expected, input)
            .await?;
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let token = HdfsStageToken::decode(stage)?;
        let durable_prefix = observe_prefix(self, &self.part(stage)?, token.expected_size)
            .await?
            .ok_or_else(|| {
                failure(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    FailureClass::NotFound,
                )
            })?;
        Ok(CheckpointObservation { durable_prefix })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let part = self.part(stage)?;
        let digest = hash_file(
            &*self.protocol,
            &part,
            stage.final_destination.path(),
            request.expected_size,
            &request.cancel,
        )
        .await?;
        if digest != request.expected_blake3 {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        Ok(VerificationEvidence {
            verified_bytes: request.expected_size,
            blake3: digest,
        })
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        publish(self, stage, request).await
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.protocol
            .delete(&self.part(&stage)?, EntryKind::File)
            .await
    }
}

fn encode_recovery(
    stage: &PreparedStage,
    token: &HdfsStageToken,
) -> Result<RecoveryIdentity, StorageRoleFailure> {
    HdfsRecoveryToken {
        binding: stage.recovery_binding,
        expected_size: token.expected_size,
        nonce: token.nonce,
        base_path: token.base_path.clone(),
        final_hash: *blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
    }
    .encode()
}

async fn recover(
    adapter: &HdfsStagedDestination,
    request: RecoverRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let token = HdfsRecoveryToken::decode(&request.identity, request.final_destination.path())?;
    validate_recovery(&token, &request)?;
    let claimed = claimed_path(&token.base_path, request.claim_token)?;
    let claimed_size = observe_prefix(adapter, &claimed, token.expected_size).await?;
    let base_size = observe_prefix(adapter, &token.base_path, token.expected_size).await?;
    let size = match (base_size, claimed_size) {
        (None, Some(size)) => size,
        (Some(_), None) => claim_base(adapter, &token, &claimed).await?,
        (Some(_), Some(_)) | (None, None) => {
            return Err(failure(
                &claimed,
                Operation::Prepare,
                FailureClass::Conflict,
            ));
        }
    };
    Ok(PreparedStage::new(
        adapter.identity.clone(),
        request.final_destination,
        HdfsStageToken {
            expected_size: token.expected_size,
            nonce: token.nonce,
            base_path: token.base_path,
            partial_path: claimed,
        }
        .encode()?,
        request.recovery_binding,
        size,
        None,
    ))
}

fn validate_recovery(
    token: &HdfsRecoveryToken,
    request: &RecoverRequest,
) -> Result<(), StorageRoleFailure> {
    let final_hash = blake3::hash(request.final_destination.path().as_str().as_bytes());
    let expected_base = partial_path(
        &request.final_destination,
        request.recovery_binding,
        &token.nonce,
    )?;
    if token.binding != request.recovery_binding
        || token.final_hash != *final_hash.as_bytes()
        || request.source.size != Some(token.expected_size)
        || token.base_path != expected_base
    {
        return Err(failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Conflict,
        ));
    }
    Ok(())
}

async fn claim_base(
    adapter: &HdfsStagedDestination,
    token: &HdfsRecoveryToken,
    claimed: &StoragePath,
) -> Result<u64, StorageRoleFailure> {
    if let Err(rename_error) = adapter
        .protocol
        .claim_stage(&token.base_path, claimed)
        .await
    {
        let base = observe_prefix(adapter, &token.base_path, token.expected_size).await?;
        let claimed = observe_prefix(adapter, claimed, token.expected_size).await?;
        return match (base, claimed) {
            (None, Some(size)) => Ok(size),
            (Some(_), Some(_)) | (None, None) => Err(failure(
                &token.base_path,
                Operation::Prepare,
                FailureClass::Conflict,
            )),
            (Some(_), None) => Err(rename_error),
        };
    }
    observe_prefix(adapter, claimed, token.expected_size)
        .await?
        .ok_or_else(|| failure(claimed, Operation::Prepare, FailureClass::Conflict))
}

async fn observe_prefix(
    adapter: &HdfsStagedDestination,
    path: &StoragePath,
    expected: u64,
) -> Result<Option<u64>, StorageRoleFailure> {
    match adapter.protocol.stat(path).await {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            Ok(None)
        }
        Err(error) => Err(error),
        Ok(facts)
            if facts.kind == EntryKind::File && facts.size.is_some_and(|size| size <= expected) =>
        {
            Ok(facts.size)
        }
        Ok(_) => Err(failure(path, Operation::Prepare, FailureClass::Corruption)),
    }
}

fn claimed_path(base: &StoragePath, claim: [u8; 32]) -> Result<StoragePath, StorageRoleFailure> {
    let base_path = PathBuf::from(base.as_str());
    let mut hasher = Hasher::new();
    hasher.update(b"data-mover/hdfs-claim/v1\0");
    hasher.update(base.as_str().as_bytes());
    hasher.update(&claim);
    let digest = hasher.finalize().to_hex();
    StoragePath::new(
        base_path
            .with_file_name(format!(".data-mover-{}.claimed", &digest[..32]))
            .to_string_lossy(),
    )
    .map_err(|_| failure(base, Operation::Prepare, FailureClass::InvalidInput))
}

async fn hash_file(
    protocol: &dyn HdfsProtocol,
    native: &StoragePath,
    diagnostic_path: &StoragePath,
    size: u64,
    cancel: &CancellationToken,
) -> Result<[u8; 32], StorageRoleFailure> {
    let mut offset = 0;
    let mut hasher = Hasher::new();
    while offset < size {
        if cancel.is_cancelled() {
            return Err(cancelled(diagnostic_path, Operation::Verify));
        }
        let end = (offset + 1024 * 1024).min(size);
        let bytes = protocol.read_range(native, offset..end).await?;
        if bytes.len() as u64 != end - offset {
            return Err(failure(
                diagnostic_path,
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        hasher.update(&bytes);
        offset = end;
    }
    Ok(*hasher.finalize().as_bytes())
}

async fn publish(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    request: PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    if request.cancel.is_cancelled() {
        return Err(publication_failure(cancelled(
            stage.final_destination.path(),
            Operation::Namespace,
        )));
    }
    let part = adapter.part(stage).map_err(publication_failure)?;
    let disposition = match request.policy {
        ExistingDestinationPolicy::Overwrite => {
            adapter
                .protocol
                .rename(&part, stage.final_destination.path(), true)
                .await
                .map_err(publication_may_have_changed)?;
            PublicationDisposition::Published
        }
        ExistingDestinationPolicy::FailIfExists => {
            adapter
                .protocol
                .rename(&part, stage.final_destination.path(), false)
                .await
                .map_err(publication_may_have_changed)?;
            PublicationDisposition::Published
        }
        ExistingDestinationPolicy::VerifyOrSkip => {
            publish_or_skip(adapter, stage, &part, &request).await?
        }
    };
    Ok(PublicationEvidence {
        final_destination: stage.final_destination.path().clone(),
        disposition,
    })
}

async fn publish_or_skip(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    part: &StoragePath,
    request: &PublishRequest,
) -> Result<PublicationDisposition, PublicationFailure> {
    match adapter.protocol.stat(stage.final_destination.path()).await {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            adapter
                .protocol
                .rename(part, stage.final_destination.path(), false)
                .await
                .map_err(publication_may_have_changed)?;
            Ok(PublicationDisposition::Published)
        }
        Err(error) => Err(publication_failure(error)),
        Ok(facts) if facts.size == Some(request.expected_size) => {
            let digest = hash_file(
                &*adapter.protocol,
                stage.final_destination.path(),
                stage.final_destination.path(),
                request.expected_size,
                &request.cancel,
            )
            .await
            .map_err(publication_failure)?;
            if digest != request.expected_blake3 {
                return Err(publication_failure(failure(
                    stage.final_destination.path(),
                    Operation::Verify,
                    FailureClass::Conflict,
                )));
            }
            adapter
                .protocol
                .delete(part, EntryKind::File)
                .await
                .map_err(publication_failure)?;
            Ok(PublicationDisposition::ExistingEquivalent)
        }
        Ok(_) => Err(publication_failure(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Conflict,
        ))),
    }
}

fn publication_failure(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

fn publication_may_have_changed(error: StorageRoleFailure) -> PublicationFailure {
    let final_destination_changed = !matches!(
        &error,
        StorageRoleFailure::Entry(value) if value.class() == FailureClass::Conflict
    );
    PublicationFailure {
        error,
        final_destination_changed,
    }
}

fn partial_path(
    destination: &FinalDestination,
    binding: [u8; 32],
    nonce: &[u8],
) -> Result<StoragePath, StorageRoleFailure> {
    let final_path = PathBuf::from(destination.path().as_str());
    if final_path.as_os_str().is_empty() {
        return Err(failure(
            destination.path(),
            Operation::Prepare,
            FailureClass::InvalidInput,
        ));
    }
    let mut hasher = Hasher::new();
    hasher.update(b"data-mover/hdfs-stage/v1\0");
    hasher.update(&binding);
    hasher.update(nonce);
    let digest = hasher.finalize().to_hex();
    StoragePath::new(
        final_path
            .with_file_name(format!(".data-mover-{}.part", &digest[..32]))
            .to_string_lossy(),
    )
    .map_err(|_| {
        failure(
            destination.path(),
            Operation::Prepare,
            FailureClass::InvalidInput,
        )
    })
}

fn expected_size(stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
    Ok(HdfsStageToken::decode(stage)?.expected_size)
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, operation, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::model::{BackendKind, IdentityStrength, SourceIdentity};

    #[test]
    fn deterministic_partial_is_same_directory_and_binding_scoped()
    -> Result<(), Box<dyn std::error::Error>> {
        let destination = FinalDestination::new(StoragePath::new("dir/final.bin")?);
        let first = partial_path(&destination, [1; 32], b"attempt-1")?;
        assert_eq!(first, partial_path(&destination, [1; 32], b"attempt-1")?);
        assert_ne!(first, partial_path(&destination, [1; 32], b"attempt-2")?);
        assert_eq!(Path::new(first.as_str()).parent(), Some(Path::new("dir")));
        Ok(())
    }

    #[test]
    fn recovery_rejects_a_validly_encoded_foreign_base_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = BackendIdentity::new(BackendKind::Hdfs, "test")?;
        let request = RecoverRequest {
            identity: RecoveryIdentity::from_bytes(Bytes::from_static(b"unused"))?,
            final_destination: FinalDestination::new(StoragePath::new("dir/final.bin")?),
            source: crate::storage::SourceDescriptor::new(
                StoragePath::new("source.bin")?,
                EntryKind::File,
                Some(7),
                SourceIdentity::new(backend, IdentityStrength::PathScoped, b"source")?,
            ),
            recovery_binding: [4; 32],
            claim_token: [5; 32],
        };
        let token = HdfsRecoveryToken {
            binding: request.recovery_binding,
            expected_size: 7,
            nonce: [6; 16],
            base_path: StoragePath::new("source.bin")?,
            final_hash: *blake3::hash(b"dir/final.bin").as_bytes(),
        };
        assert!(validate_recovery(&token, &request).is_err());
        Ok(())
    }
}
