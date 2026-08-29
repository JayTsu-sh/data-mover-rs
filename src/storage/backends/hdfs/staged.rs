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

struct HdfsStageToken {
    expected_size: u64,
    partial_path: StoragePath,
}

impl HdfsStageToken {
    fn encode(&self) -> Bytes {
        let mut value = BytesMut::from(STAGE_TOKEN_MAGIC);
        value.extend_from_slice(&self.expected_size.to_le_bytes());
        value.extend_from_slice(self.partial_path.as_str().as_bytes());
        value.freeze()
    }

    fn decode(stage: &PreparedStage) -> Result<Self, StorageRoleFailure> {
        let payload = stage.token.strip_prefix(STAGE_TOKEN_MAGIC).ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
            )
        })?;
        let (size, path) = payload.split_at_checked(size_of::<u64>()).ok_or_else(|| {
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
        let partial_path =
            StoragePath::new(String::from_utf8_lossy(path).as_ref()).map_err(|_| {
                failure(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    FailureClass::Protocol,
                )
            })?;
        Ok(Self {
            expected_size,
            partial_path,
        })
    }
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
        let part = partial_path(&request.final_destination, request.recovery_binding)?;
        self.protocol.prepare_stage(&part, expected).await?;
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            HdfsStageToken {
                expected_size: expected,
                partial_path: part,
            }
            .encode(),
            request.recovery_binding,
            0,
            None,
        ))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        Err(failure(
            stage.final_destination.path(),
            Operation::Prepare,
            FailureClass::Unsupported,
        ))
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Unsupported,
        ))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let part = self.part(stage)?;
        let expected = expected_size(stage)?;
        let persisted = self.protocol.write_stage(&part, expected, input).await?;
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let observed = self.protocol.stat(&self.part(stage)?).await?;
        Ok(CheckpointObservation {
            durable_prefix: observed.size.unwrap_or_default(),
        })
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
) -> Result<StoragePath, StorageRoleFailure> {
    let final_path = PathBuf::from(destination.path().as_str());
    if final_path.as_os_str().is_empty() {
        return Err(failure(
            destination.path(),
            Operation::Prepare,
            FailureClass::InvalidInput,
        ));
    }
    let digest = blake3::hash(&binding).to_hex();
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

    #[test]
    fn deterministic_partial_is_same_directory_and_binding_scoped()
    -> Result<(), Box<dyn std::error::Error>> {
        let destination = FinalDestination::new(StoragePath::new("dir/final.bin")?);
        let first = partial_path(&destination, [1; 32])?;
        assert_eq!(first, partial_path(&destination, [1; 32])?);
        assert_ne!(first, partial_path(&destination, [2; 32])?);
        assert_eq!(Path::new(first.as_str()).parent(), Some(Path::new("dir")));
        Ok(())
    }
}
