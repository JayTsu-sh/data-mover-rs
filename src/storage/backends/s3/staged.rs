use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::model::{BackendIdentity, FailureClass, Operation, Transience};
use crate::storage::artifacts::ARTIFACT_PREFIX;
use crate::storage::{
    ByteStream, CheckpointObservation, DestinationPrepareRequest, Metadata, MetadataMutation,
    PrepareRequest, PreparedStage, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerificationPoint, VerifyRequest, WriteEvidence,
};

use super::source::{cancelled, classified_entry, entry, role_failure};

mod at_destination;
mod completion;
mod direct;
#[cfg(test)]
mod manifest_tests;
mod native;
#[cfg(test)]
mod native_tests;
mod parts;
mod publication;
mod recovery;
mod single;
#[cfg(test)]
mod single_tests;
#[cfg(test)]
mod sizing_tests;
mod upload_discovery;
mod upload_pointer;
use super::{S3Protocol, S3ProtocolFailure};
use recovery::resumable_parts;
pub(crate) use single::{DEFAULT_SINGLE_PUT_THRESHOLD, single_put_threshold};
#[cfg(test)]
pub(crate) use single::{MAX_SINGLE_PUT_THRESHOLD, MIN_SINGLE_PUT_THRESHOLD};

const PART_SIZE: usize = 8 * 1024 * 1024;
const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024;
const MAX_INFLIGHT_PARTS: usize = 4;

fn planned_part_size(
    size: Option<u64>,
    path: &crate::model::StoragePath,
) -> Result<usize, StorageRoleFailure> {
    let size = size.unwrap_or(0);
    let part = size.div_ceil(10_000).max(PART_SIZE as u64);
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

#[derive(Clone, Default)]
struct StageState {
    persisted: u64,
    expected_size: Option<u64>,
    part_size: usize,
    upload_id: String,
    parts: Vec<(i32, String)>,
    completed: bool,
}

pub(crate) struct S3StagedDestination<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    states: Mutex<HashMap<Vec<u8>, StageState>>,
    metadata: Option<Arc<dyn Metadata>>,
    /// Sources of at most this many bytes go as one `PutObject` (ADR-0006 C14b); `None` sends
    /// every object through a multipart upload.
    single_put_threshold: Option<u64>,
    tags_supported: bool,
    /// Whether recovery state is kept at the destination (`prepare_at_destination`, ADR-0006
    /// C15b) instead of the local recovery store: on since C15c; off only in tests of the old
    /// temp-key path, which C19 removes.
    recovery_at_destination: bool,
    /// The automatic checkpoint interval on the at-destination route: where a checkpointed upload
    /// writes its pointer.
    checkpoint_interval: u64,
}

impl<P> S3StagedDestination<P> {
    pub(crate) fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            states: Mutex::new(HashMap::new()),
            metadata: None,
            single_put_threshold: Some(single::DEFAULT_SINGLE_PUT_THRESHOLD),
            tags_supported: true,
            recovery_at_destination: true,
            // Named in full: the architecture guard refuses imports between backend modules.
            checkpoint_interval: crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES,
        }
    }

    /// A shorter automatic checkpoint interval, so engine tests need not move 64 MiB.
    #[cfg(test)]
    pub(crate) fn with_checkpoint_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Whether recovery state is kept at the destination (the default since ADR-0006 C15c);
    /// `false` puts the old temp-key path and the local recovery store back, for its tests.
    #[cfg(test)]
    pub(crate) fn with_recovery_at_destination(mut self, enabled: bool) -> Self {
        self.recovery_at_destination = enabled;
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

    fn temp_key(request: &PrepareRequest) -> String {
        format!(
            "{ARTIFACT_PREFIX}stage/{}/{}",
            blake3::Hash::from_bytes(request.recovery_binding).to_hex(),
            blake3::hash(request.final_destination.path().as_str().as_bytes()).to_hex()
        )
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
    path: &crate::model::StoragePath,
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

impl<P: S3Protocol> S3StagedDestination<P> {
    async fn stage_state(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<StageState, StorageRoleFailure> {
        self.states
            .lock()
            .await
            .get(stage.token.as_ref())
            .cloned()
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    operation,
                    "S3 stage is not claimed",
                )
            })
    }

    async fn content_digest(
        &self,
        path: &crate::model::StoragePath,
        expected_size: u64,
        cancel: &tokio_util::sync::CancellationToken,
        operation: Operation,
    ) -> Result<Option<[u8; 32]>, StorageRoleFailure> {
        let facts = match self.protocol.head(path.as_str()).await {
            Ok(facts) => facts,
            Err(S3ProtocolFailure::Entry {
                class: crate::model::FailureClass::NotFound,
                ..
            }) => return Ok(None),
            Err(failure) => return Err(role_failure(path, operation, failure)),
        };
        if facts.size != expected_size {
            return Ok(None);
        }
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0;
        while offset < facts.size {
            if cancel.is_cancelled() {
                return Err(cancelled(path, operation));
            }
            let end = (offset + PART_SIZE as u64).min(facts.size);
            let bytes = self
                .protocol
                .get_range(path.as_str(), offset..end, &facts)
                .await
                .map_err(|failure| role_failure(path, operation, failure))?;
            if bytes.len() as u64 != end - offset {
                return Ok(None);
            }
            hasher.update(&bytes);
            offset = end;
        }
        Ok(Some(*hasher.finalize().as_bytes()))
    }

    async fn content_matches(
        &self,
        path: &crate::model::StoragePath,
        expected_size: u64,
        expected_blake3: &[u8; 32],
        cancel: &tokio_util::sync::CancellationToken,
        operation: Operation,
    ) -> Result<bool, StorageRoleFailure> {
        self.content_digest(path, expected_size, cancel, operation)
            .await
            .map(|digest| digest.as_ref() == Some(expected_blake3))
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

    fn recovery_at_destination(&self) -> bool {
        self.recovery_at_destination
    }

    /// The 64 MiB interval (D3: a checkpointed object up to it never writes a pointer). Only on the
    /// at-destination route: the old store path keeps its own planning (every checkpointed upload
    /// registers from the start).
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        self.recovery_at_destination
            .then_some(self.checkpoint_interval)
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

    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        if let Some(size) = request
            .source
            .size
            .filter(|size| self.is_single_put(Some(*size)))
        {
            return Ok(self.prepare_single(request, size));
        }
        let part_size = planned_part_size(request.source.size, request.final_destination.path())?;
        let key = Self::temp_key(&request);
        let upload_id =
            self.protocol.begin_multipart(&key).await.map_err(|e| {
                role_failure(request.final_destination.path(), Operation::Prepare, e)
            })?;
        let token = Self::encode_token(&key, &upload_id);
        self.states.lock().await.insert(
            token.to_vec(),
            StageState {
                persisted: 0,
                expected_size: request.source.size,
                part_size,
                upload_id,
                parts: Vec::new(),
                completed: false,
            },
        );
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        ))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        self.validate(stage)?;
        if stage.at_destination {
            // Its recovery state is the pointer beside the final key; nothing is kept locally.
            return Err(classified_entry(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Unsupported,
                Transience::Permanent,
                "an S3 stage kept at the destination has no local recovery identity",
            ));
        }
        RecoveryIdentity::from_bytes(stage.token.clone()).map_err(|e| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                e.to_string(),
            )
        })
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        // Source size is bound into recovery_binding, so retry selects the same sizing.
        let part_size = planned_part_size(request.source.size, request.final_destination.path())?;
        let (token, key, upload_id) = Self::validated_recovery(&request)?;
        if upload_id == single::SINGLE_MARKER {
            // A single PUT keeps nothing to resume: start it again.
            let size = request.source.size.unwrap_or(0);
            return Ok(self.prepare_single(
                PrepareRequest {
                    final_destination: request.final_destination,
                    source: request.source,
                    recovery_binding: request.recovery_binding,
                },
                size,
            ));
        }
        // Who may resume is settled before this call: the engine holds the recovery record's
        // exclusive lease for the whole attempt, and one destination key is never written by two
        // transfers at once. S3 needs no marker object of its own — the conditional PUT that one
        // relied on is not supported everywhere (MinIO RELEASE.2023-03-20 answers 404).
        let observed = self.recovered_state(&request, &key, &upload_id).await;
        let (persisted, parts, completed) = match observed {
            Ok(state) => state,
            Err(error)
                if matches!(&error, StorageRoleFailure::Entry(failure)
                    if failure.class() == FailureClass::Corruption) =>
            {
                self.remove_invalid_upload(&request, &key, &upload_id)
                    .await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        self.states.lock().await.insert(
            token.to_vec(),
            StageState {
                persisted,
                expected_size: request.source.size,
                part_size,
                upload_id,
                parts,
                completed,
            },
        );
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            persisted,
            None,
        ))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let key = self.validate(stage)?;
        if stage.direct {
            return self.write_direct(stage, input).await;
        }
        if let Some(single) = single::of(stage) {
            return single::write(stage, single, input).await;
        }
        if let Some(upload) = at_destination::of(stage) {
            return self.write_final_upload(stage, upload, input).await;
        }
        let initial = self.stage_state(stage, Operation::Write).await?;
        if initial.completed {
            return Ok(WriteEvidence {
                persisted_bytes: initial.persisted,
            });
        }
        let upload_id = initial.upload_id.clone();
        let part_size = initial.part_size;
        let path = stage.final_destination.path();
        let target = parts::PartTarget {
            path,
            key: &key,
            upload_id: &upload_id,
            part_size,
            checkpoint: None,
        };
        let parts = self
            .upload_parts(&target, initial.parts, input)
            .await?
            .parts;
        // The temp key's facts are not needed: publication copies it to the final key.
        self.protocol
            .complete_multipart(&key, &upload_id, &parts)
            .await
            .map_err(|e| role_failure(path, Operation::Write, e))?;
        let persisted = self
            .protocol
            .head(&key)
            .await
            .map_err(|e| role_failure(stage.final_destination.path(), Operation::Write, e))?
            .size;
        self.states.lock().await.insert(
            stage.token.to_vec(),
            StageState {
                persisted,
                expected_size: initial.expected_size,
                part_size,
                upload_id,
                parts,
                completed: true,
            },
        );
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let key = self.validate(stage)?;
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
        if let Some(upload) = at_destination::of(stage) {
            return self.observe_final_upload(stage, upload).await;
        }
        let stage_state = self
            .states
            .lock()
            .await
            .get(stage.token.as_ref())
            .cloned()
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    "S3 stage is not claimed",
                )
            })?;
        let durable_prefix = if stage_state.completed {
            stage_state.persisted
        } else {
            let parts = self
                .protocol
                .list_parts(&key, &stage_state.upload_id)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Prepare, e))?;
            let (persisted, parts) = resumable_parts(
                stage.final_destination.path(),
                parts,
                stage_state.expected_size,
            )?;
            self.states.lock().await.insert(
                stage.token.to_vec(),
                StageState {
                    persisted,
                    expected_size: stage_state.expected_size,
                    part_size: stage_state.part_size,
                    upload_id: stage_state.upload_id,
                    parts,
                    completed: false,
                },
            );
            persisted
        };
        Ok(CheckpointObservation { durable_prefix })
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
        if let Some(upload) = at_destination::of(stage) {
            return self.verify_final_upload(stage, upload, &request).await;
        }
        let key = self.validate(stage)?;
        let facts = self
            .protocol
            .head(&key)
            .await
            .map_err(|e| role_failure(stage.final_destination.path(), Operation::Verify, e))?;
        if facts.size != request.expected_size {
            return Err(entry(
                stage.final_destination.path(),
                Operation::Verify,
                "S3 staged size mismatch",
            ));
        }
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0;
        while offset < facts.size {
            if request.cancel.is_cancelled() {
                return Err(cancelled(stage.final_destination.path(), Operation::Verify));
            }
            let end = (offset + PART_SIZE as u64).min(facts.size);
            let bytes = self
                .protocol
                .get_range(&key, offset..end, &facts)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Verify, e))?;
            hasher.update(&bytes);
            offset = end;
        }
        let actual = *hasher.finalize().as_bytes();
        if actual != request.expected_blake3 {
            return Err(entry(
                stage.final_destination.path(),
                Operation::Verify,
                "S3 staged checksum mismatch",
            ));
        }
        Ok(VerificationEvidence {
            verified_bytes: facts.size,
            blake3: actual,
        })
    }

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let mut key = self.validate(stage)?;
        if stage.direct {
            // The object is already at its final key.
            key = stage.final_destination.path().as_str().to_string();
        } else if let Some(single) = single::of(stage) {
            if self.metadata.is_none() {
                return Err(metadata_unavailable(stage));
            }
            return single::apply_metadata(stage, single, self.tags_supported, mutation, &cancel);
        } else if let Some(upload) = at_destination::of(stage) {
            return self.apply_final_upload_metadata(stage, upload, mutation, &cancel);
        }
        let path = crate::model::StoragePath::new(key).map_err(|error| {
            entry(
                stage.final_destination.path(),
                Operation::Metadata,
                error.to_string(),
            )
        })?;
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| metadata_unavailable(stage))?;
        metadata.apply(&path, mutation, cancel).await
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
        if let Some(upload) = at_destination::of(stage) {
            return self.publish_final_upload(stage, upload, &request).await;
        }
        publication::publish(self, stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let key = self.validate(&stage)?;
        if stage.direct {
            return self.discard_direct(&stage).await;
        }
        if single::of(&stage).is_some() {
            // Nothing was sent before publication, and a published object is never deleted.
            return Ok(());
        }
        if let Some(upload) = at_destination::of(&stage) {
            return self.discard_final_upload(&stage, upload).await;
        }
        let stage_state = self
            .states
            .lock()
            .await
            .get(stage.token.as_ref())
            .cloned()
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    Operation::Namespace,
                    "S3 stage is not claimed",
                )
            })?;
        if stage_state.completed || stage_state.upload_id.is_empty() {
            cleanup_result(
                stage.final_destination.path(),
                publication::delete_temp_key(&*self.protocol, &key).await,
            )?;
        } else {
            cleanup_result(
                stage.final_destination.path(),
                self.protocol
                    .abort_multipart(&key, &stage_state.upload_id)
                    .await,
            )?;
            cleanup_result(
                stage.final_destination.path(),
                publication::delete_temp_key(&*self.protocol, &key).await,
            )?;
        }
        self.states.lock().await.remove(stage.token.as_ref());
        Ok(())
    }

    /// A single PUT, a `Direct` write and an upload on the final key write the final key itself,
    /// so it is read back only after publication.
    fn verification_point(&self, stage: &PreparedStage) -> VerificationPoint {
        if stage.direct || single::of(stage).is_some() || at_destination::of(stage).is_some() {
            VerificationPoint::AfterPublish
        } else {
            VerificationPoint::BeforePublish
        }
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
