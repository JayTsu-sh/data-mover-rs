//! The S3 destination (ADR-0006): every stage writes the final key. Up to the single-PUT
//! threshold an object is one `PutObject` (a native copy up to 64 MiB one `CopyObject`); a larger
//! one is a multipart upload on the final key, resumable through the `.upload` pointer beside it;
//! a `Direct` write completes inside `write`. Nothing is recorded where data-mover runs, and no
//! temp key is written (the `.data-mover-stage/` temp-key path was removed in C19).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, CheckpointObservation, DestinationPrepareRequest, Metadata, MetadataMutation,
    PrepareRequest, PreparedStage, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerificationPoint, VerifyRequest, WriteEvidence,
};

use super::source::{classified_entry, entry, role_failure};

mod at_destination;
mod completion;
mod direct;
mod final_upload;
mod native;
mod native_final;
#[cfg(test)]
mod native_final_tests;
mod parts;
mod single;
#[cfg(test)]
mod single_tests;
#[cfg(test)]
mod sizing_tests;
mod upload_discovery;
mod upload_pointer;
use super::{S3Protocol, S3ProtocolFailure};
use native_final::NativeSizing;
pub(crate) use single::{DEFAULT_SINGLE_PUT_THRESHOLD, single_put_threshold};
#[cfg(test)]
pub(crate) use single::{MAX_SINGLE_PUT_THRESHOLD, MIN_SINGLE_PUT_THRESHOLD};

const PART_SIZE: usize = 8 * 1024 * 1024;
const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024;
const MAX_INFLIGHT_PARTS: usize = 4;

fn planned_part_size(size: Option<u64>, path: &StoragePath) -> Result<usize, StorageRoleFailure> {
    part_size_at_least(size, PART_SIZE as u64, path)
}

/// The part size for `size` bytes in at most 10 000 parts, and at least `minimum`; parts over
/// 5 GiB are refused.
fn part_size_at_least(
    size: Option<u64>,
    minimum: u64,
    path: &StoragePath,
) -> Result<usize, StorageRoleFailure> {
    let size = size.unwrap_or(0);
    let part = size.div_ceil(10_000).max(minimum);
    if part > 5 * 1024 * 1024 * 1024 {
        return Err(entry(
            path,
            Operation::Prepare,
            "S3 multipart capacity exceeded",
        ));
    }
    usize::try_from(part)
        .map_err(|_| entry(path, Operation::Prepare, "S3 part cannot fit address space"))
}

pub(crate) struct S3StagedDestination<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    metadata: Option<Arc<dyn Metadata>>,
    /// Sources of at most this many bytes go as one `PutObject` (ADR-0006 C14b); `None` sends
    /// every object through a multipart upload.
    single_put_threshold: Option<u64>,
    tags_supported: bool,
    /// The automatic checkpoint interval: where a checkpointed upload writes its pointer.
    checkpoint_interval: u64,
    /// How a native copy to the final key is split (ADR-0006 C18).
    native: NativeSizing,
}

impl<P> S3StagedDestination<P> {
    pub(crate) fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            metadata: None,
            single_put_threshold: Some(single::DEFAULT_SINGLE_PUT_THRESHOLD),
            tags_supported: true,
            // Named in full: the architecture guard refuses imports between backend modules.
            checkpoint_interval: crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES,
            native: NativeSizing::default(),
        }
    }

    /// Smaller native copy sizes, so engine tests need not copy 64 MiB objects.
    #[cfg(test)]
    pub(crate) fn with_native_sizing(mut self, single_max: u64, part_size: u64) -> Self {
        self.native = NativeSizing {
            single_max,
            part_size,
        };
        self
    }

    /// A shorter automatic checkpoint interval, so engine tests need not move 64 MiB.
    #[cfg(test)]
    pub(crate) fn with_checkpoint_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Sends sources of at most `threshold` bytes as one `PutObject`; `None` sends every object
    /// through a multipart upload. A configured value is validated by `single_put_threshold`.
    pub(crate) fn with_single_put_threshold(mut self, threshold: Option<u64>) -> Self {
        self.single_put_threshold = threshold;
        self
    }

    pub(crate) fn with_tag_support(mut self, supported: bool) -> Self {
        self.tags_supported = supported;
        self
    }

    pub(crate) fn with_metadata(mut self, metadata: Arc<dyn Metadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    fn key(stage: &PreparedStage) -> Result<String, StorageRoleFailure> {
        let (key, _) = Self::decode_token(&stage.token).map_err(|()| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                "invalid S3 stage identity",
            )
        })?;
        Ok(key)
    }

    fn encode_token(key: &str, upload_id: &str) -> Bytes {
        let mut token = Vec::with_capacity(key.len() + upload_id.len() + 1);
        token.extend_from_slice(key.as_bytes());
        token.push(0);
        token.extend_from_slice(upload_id.as_bytes());
        Bytes::from(token)
    }

    fn decode_token(token: &Bytes) -> Result<(String, String), ()> {
        let split = token.iter().position(|byte| *byte == 0).ok_or(())?;
        let key = String::from_utf8(token[..split].to_vec()).map_err(|_| ())?;
        let upload_id = String::from_utf8(token[split + 1..].to_vec()).map_err(|_| ())?;
        if key.is_empty() || upload_id.is_empty() {
            return Err(());
        }
        Ok((key, upload_id))
    }

    fn validate(&self, stage: &PreparedStage) -> Result<String, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                "S3 stage belongs to another backend",
            )
        })?;
        Self::key(stage)
    }
}

fn cleanup_result(
    path: &StoragePath,
    result: super::S3Result<()>,
) -> Result<(), StorageRoleFailure> {
    match result {
        Ok(())
        | Err(S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }) => Ok(()),
        Err(failure) => Err(role_failure(path, Operation::Namespace, failure)),
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> StagedDestination for S3StagedDestination<P> {
    /// An object stores none of a file's metadata: its time is when it was written, and owner,
    /// mode, ACL and extended attributes have nowhere to go. Declared rather than left undeclared,
    /// so a copy reports each family as skipped because of the destination instead of silently
    /// carrying nothing.
    fn copied_metadata_target(&self) -> Option<crate::storage::CopiedMetadataTarget> {
        Some(crate::storage::CopiedMetadataTarget {
            timestamps: crate::storage::CopiedTimestampTarget::NotStored,
            ownership: crate::storage::CopiedOwnershipTarget::Unsupported,
            acl: crate::storage::CopiedAclTarget::Unsupported,
            xattrs: crate::storage::CopiedValueTarget::Unsupported,
        })
    }

    /// `Direct` writes the object at its final key (ADR-0006 C14c).
    fn supports_direct(&self) -> bool {
        true
    }

    /// S3 keeps its recovery state at the destination (ADR-0006 C15c): every staged transfer is
    /// prepared through [`StagedDestination::prepare_at_destination`].
    fn recovery_at_destination(&self) -> bool {
        true
    }

    /// The 64 MiB interval (D3: a checkpointed object up to it never writes a pointer).
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        Some(self.checkpoint_interval)
    }

    async fn prepare_at_destination(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare_at_destination_stage(request).await
    }

    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare_direct_stage(request, &cancel)
    }

    async fn write_single(
        &self,
        stage: &PreparedStage,
        data: Bytes,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.validate(stage)?;
        match single::of(stage) {
            Some(single) if !stage.direct => single::write_single(stage, single, data),
            _ => {
                self.write(
                    stage,
                    Box::pin(futures::stream::once(async move { Ok(data) })),
                )
                .await
            }
        }
    }

    /// The store-era prepare (a temp key under `.data-mover-stage/`) was removed in ADR-0006
    /// C19: use [`StagedDestination::prepare_at_destination`].
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(unsupported(request.final_destination.path()))
    }

    /// Nothing is recorded where data-mover runs: the pointer beside the final key is the
    /// recovery state.
    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        Err(unsupported(stage.final_destination.path()))
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(unsupported(request.final_destination.path()))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.validate(stage)?;
        if stage.direct {
            return self.write_direct(stage, input).await;
        }
        if let Some(single) = single::of(stage) {
            return single::write(stage, single, input).await;
        }
        let upload = at_destination::of(stage).ok_or_else(|| foreign(stage, Operation::Write))?;
        self.write_final_upload(stage, upload, input).await
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.validate(stage)?;
        if stage.direct {
            return Ok(CheckpointObservation {
                durable_prefix: Self::direct_durable_bytes(stage),
            });
        }
        if let Some(single) = single::of(stage) {
            return Ok(CheckpointObservation {
                durable_prefix: single.written_len(),
            });
        }
        let upload = at_destination::of(stage).ok_or_else(|| foreign(stage, Operation::Prepare))?;
        self.observe_final_upload(stage, upload).await
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        if stage.direct {
            return self.verify_direct(stage, &request).await;
        }
        if let Some(single) = single::of(stage) {
            return single::verify(self, stage, single, &request).await;
        }
        let upload = at_destination::of(stage).ok_or_else(|| foreign(stage, Operation::Verify))?;
        self.verify_final_upload(stage, upload, &request).await
    }

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.validate(stage)?;
        if stage.direct {
            // The object is already at its final key.
            let metadata = self
                .metadata
                .as_ref()
                .ok_or_else(|| metadata_unavailable(stage))?;
            return metadata
                .apply(stage.final_destination.path(), mutation, cancel)
                .await;
        }
        if let Some(single) = single::of(stage) {
            if self.metadata.is_none() {
                return Err(metadata_unavailable(stage));
            }
            return single::apply_metadata(stage, single, self.tags_supported, mutation, &cancel);
        }
        let upload =
            at_destination::of(stage).ok_or_else(|| foreign(stage, Operation::Metadata))?;
        self.apply_final_upload_metadata(stage, upload, mutation, &cancel)
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        if stage.direct {
            return self.publish_direct(stage);
        }
        if let Some(single) = single::of(stage) {
            return single::publish(self, stage, single, &request).await;
        }
        let upload = at_destination::of(stage).ok_or_else(|| PublicationFailure {
            error: foreign(stage, Operation::Publish),
            final_destination_changed: false,
        })?;
        self.publish_final_upload(stage, upload, &request).await
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.validate(&stage)?;
        if stage.direct {
            return self.discard_direct(&stage).await;
        }
        if single::of(&stage).is_some() {
            // Nothing was sent before publication, and a published object is never deleted.
            return Ok(());
        }
        let upload =
            at_destination::of(&stage).ok_or_else(|| foreign(&stage, Operation::Namespace))?;
        self.discard_final_upload(&stage, upload).await
    }

    /// Every S3 stage writes the final key itself — a single PUT or `CopyObject`, a `Direct`
    /// write, an upload on the final key — so it is read back only after publication.
    fn verification_point(&self, _stage: &PreparedStage) -> VerificationPoint {
        VerificationPoint::AfterPublish
    }
}

fn metadata_unavailable(stage: &PreparedStage) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination.path(),
        Operation::Metadata,
        FailureClass::Unsupported,
        Transience::Permanent,
        "staged S3 metadata is unavailable",
    )
}

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Prepare,
        FailureClass::Unsupported,
        Transience::Permanent,
        "S3 keeps its recovery state at the destination: prepare it there",
    )
}

/// A stage this adapter did not prepare at the destination, nor as `Direct` (the store era's
/// temp-key stage, ADR-0006 C19): refused before anything is touched.
fn foreign(stage: &PreparedStage, operation: Operation) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination.path(),
        operation,
        FailureClass::Conflict,
        Transience::Permanent,
        "S3 stage was not prepared at the destination",
    )
}
