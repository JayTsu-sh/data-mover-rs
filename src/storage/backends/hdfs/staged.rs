use std::str;
use std::sync::Arc;

use async_trait::async_trait;
use blake3::Hasher;
use bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;

use super::at_destination::{Fence, artifact_path, resident};
use super::protocol::{HdfsProtocol, cancelled, entry_failure};
use crate::model::{
    BackendIdentity, EntryKind, FailureClass, Operation, StoragePath, TimePrecision, Transience,
};
use crate::storage::artifacts::ArtifactKind;
use crate::storage::{
    ByteStream, CheckpointObservation, CopiedAclTarget, CopiedMetadataTarget,
    CopiedOwnershipTarget, CopiedTimestampTarget, CopiedValueTarget, DestinationPrepareRequest,
    Metadata, MetadataMutation, PrepareRequest, PreparedStage, PublicationDisposition,
    PublicationEvidence, PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

const STAGE_TOKEN_MAGIC: &[u8] = b"hdfs-stage-v2\0";

/// A stage's token: the size it must reach and the path it writes — the final path for a direct
/// stage, the deterministic `.stage` beside it for one kept at the destination.
struct HdfsStageToken {
    expected_size: u64,
    path: StoragePath,
}

impl HdfsStageToken {
    fn encode(&self) -> Bytes {
        let mut value = BytesMut::from(STAGE_TOKEN_MAGIC);
        value.extend_from_slice(&self.expected_size.to_le_bytes());
        value.extend_from_slice(self.path.as_str().as_bytes());
        value.freeze()
    }

    fn decode(stage: &PreparedStage) -> Result<Self, StorageRoleFailure> {
        let invalid = || {
            failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
            )
        };
        let payload = stage
            .token
            .strip_prefix(STAGE_TOKEN_MAGIC)
            .ok_or_else(invalid)?;
        let (size, path) = payload
            .split_first_chunk::<{ size_of::<u64>() }>()
            .ok_or_else(invalid)?;
        let path = str::from_utf8(path).map_err(|_| invalid())?;
        Ok(Self {
            expected_size: u64::from_le_bytes(*size),
            path: StoragePath::new(path).map_err(|_| invalid())?,
        })
    }
}

/// What a stage kept at the destination carries beyond its token (ADR-0006).
pub(super) struct HdfsStageState {
    pub(super) fence: Fence,
}

/// The token of a stage kept at the destination: its deterministic path and the size it must
/// reach.
pub(super) fn stage_token(path: &StoragePath, expected_size: u64) -> Bytes {
    HdfsStageToken {
        expected_size,
        path: path.clone(),
    }
    .encode()
}

pub(super) struct HdfsStagedDestination {
    pub(super) protocol: Arc<dyn HdfsProtocol>,
    identity: BackendIdentity,
    metadata: Option<Arc<dyn Metadata>>,
}

impl HdfsStagedDestination {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: BackendIdentity,
    ) -> Self {
        Self {
            protocol,
            identity,
            metadata: None,
        }
    }

    pub(super) fn with_metadata(mut self, metadata: Arc<dyn Metadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub(super) const fn identity(&self) -> &BackendIdentity {
        &self.identity
    }

    /// The path this stage writes: the `.stage` beside the final file for a stage kept at the
    /// destination, the final path for a direct one. Any other stage is not this adapter's.
    pub(super) fn part(&self, stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
        let final_path = stage.final_destination.path();
        let conflict = || failure(final_path, Operation::Prepare, FailureClass::Conflict);
        stage
            .validate_owner(&self.identity)
            .map_err(|_| conflict())?;
        let path = HdfsStageToken::decode(stage)?.path;
        let expected = if resident(stage) {
            artifact_path(final_path, ArtifactKind::Stage, false)?
        } else if stage.direct {
            final_path.clone()
        } else {
            return Err(conflict());
        };
        if path != expected {
            return Err(conflict());
        }
        Ok(path)
    }
}

#[async_trait]
impl StagedDestination for HdfsStagedDestination {
    fn supports_direct(&self) -> bool {
        true
    }

    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        self.metadata.as_ref().map(|_| CopiedMetadataTarget {
            timestamps: CopiedTimestampTarget::Stored(TimePrecision::Milliseconds),
            ownership: CopiedOwnershipTarget::ModeOnly,
            // The metadata role reports both families unavailable on observation too.
            acl: CopiedAclTarget::Unsupported,
            xattrs: CopiedValueTarget::Unsupported,
        })
    }

    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        Some(crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES)
    }

    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(cancelled(
                request.final_destination.path(),
                Operation::Prepare,
            ));
        }
        if request.source.source_identity.backend() == &self.identity
            && request.source.path == *request.final_destination.path()
        {
            return Err(failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Conflict,
            ));
        }
        let expected_size = request.source.size.ok_or_else(|| {
            failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Unsupported,
            )
        })?;
        let token = HdfsStageToken {
            expected_size,
            path: request.final_destination.path().clone(),
        }
        .encode();
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        )
        .disable_recovery();
        stage.direct = true;
        stage.durable_publication = false;
        Ok(stage)
    }

    /// HDFS keeps its recovery state at the destination: every staged transfer is prepared
    /// through [`StagedDestination::prepare_at_destination`].
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(unsupported(request.final_destination.path()))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        Err(unsupported(stage.final_destination.path()))
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(unsupported(request.final_destination.path()))
    }

    fn recovery_at_destination(&self) -> bool {
        true
    }

    async fn prepare_at_destination(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        super::at_destination::prepare(self, request).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        super::writer::write(self, stage, input).await
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let part = self.part(stage)?;
        let durable_prefix = if resident(stage) {
            super::at_destination::reobserve(self, stage, &part).await?
        } else {
            direct_length(self, stage, &part).await?
        };
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

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let path = self.part(stage)?;
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                Operation::Metadata,
                FailureClass::Unsupported,
            )
        })?;
        metadata.apply(&path, mutation, cancel).await
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        publish(self, stage, request).await
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        if stage.direct {
            return Ok(());
        }
        self.part(&stage)?;
        super::at_destination::discard(self, &stage).await
    }
}

/// The length a direct stage has written to the final path: a file no longer than the source.
async fn direct_length(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    path: &StoragePath,
) -> Result<u64, StorageRoleFailure> {
    let expected = expected_size(stage)?;
    let facts = adapter.protocol.stat(path).await?;
    match (facts.kind, facts.size) {
        (EntryKind::File, Some(size)) if size <= expected => Ok(size),
        _ => Err(failure(path, Operation::Observe, FailureClass::Corruption)),
    }
}

pub(super) async fn hash_file(
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

/// Publishes a stage kept at the destination by renaming it over the final file; a direct stage
/// already is the final file.
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
    if resident(stage) {
        return super::at_destination::publish(adapter, stage, &part, &request).await;
    }
    Ok(PublicationEvidence {
        final_destination: stage.final_destination.path().clone(),
        disposition: PublicationDisposition::Published,
        version: None,
    })
}

pub(super) fn publication_failure(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

pub(super) fn publication_may_have_changed(error: StorageRoleFailure) -> PublicationFailure {
    let final_destination_changed = !matches!(
        &error,
        StorageRoleFailure::Entry(value) if value.class() == FailureClass::Conflict
    );
    PublicationFailure {
        error,
        final_destination_changed,
    }
}

pub(super) fn expected_size(stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
    Ok(HdfsStageToken::decode(stage)?.expected_size)
}

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    failure(path, Operation::Prepare, FailureClass::Unsupported)
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, operation, class, Transience::Permanent)
}
