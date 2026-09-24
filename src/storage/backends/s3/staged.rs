use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::model::{BackendIdentity, FailureClass, Operation, Transience};
use crate::storage::artifacts::ARTIFACT_PREFIX;
use crate::storage::{
    ByteStream, CheckpointObservation, Metadata, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerificationPoint, VerifyRequest,
    WriteEvidence,
};

use super::source::{cancelled, classified_entry, entry, role_failure};

#[cfg(test)]
mod manifest_tests;
mod native;
#[cfg(test)]
mod native_tests;
mod publication;
mod recovery;
mod single;
#[cfg(test)]
mod single_tests;
#[cfg(test)]
mod sizing_tests;
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

async fn upload<P: S3Protocol>(
    protocol: Arc<P>,
    key: String,
    upload_id: String,
    number: i32,
    bytes: Bytes,
) -> Result<(i32, String), S3ProtocolFailure> {
    if !(1..=10_000).contains(&number) {
        return Err(S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            "S3 multipart part limit exceeded",
        ));
    }
    let etag = protocol
        .upload_part(&key, &upload_id, number, bytes)
        .await?;
    Ok((number, etag))
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
        }
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

    async fn write_single(
        &self,
        stage: &PreparedStage,
        data: Bytes,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.validate(stage)?;
        match single::of(stage) {
            Some(single) => single::write_single(stage, single, data),
            None => {
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
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let key = self.validate(stage)?;
        if let Some(single) = single::of(stage) {
            return single::write(stage, single, input).await;
        }
        let initial = self.stage_state(stage, Operation::Write).await?;
        if initial.completed {
            return Ok(WriteEvidence {
                persisted_bytes: initial.persisted,
            });
        }
        let upload_id = initial.upload_id.clone();
        let part_size = initial.part_size;
        let mut buffered = BytesMut::with_capacity(part_size);
        let mut parts = initial.parts;
        let mut number = parts.iter().map(|part| part.0).max().unwrap_or(0) + 1;
        let result: Result<u64, StorageRoleFailure> = async {
            let mut inflight = futures::stream::FuturesUnordered::new();
            while let Some(chunk) = input.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(input_failure) => {
                        while inflight.next().await.is_some() {}
                        return Err(input_failure);
                    }
                };
                buffered.extend_from_slice(&chunk);
                while buffered.len() >= part_size {
                    let part = buffered.split_to(part_size).freeze();
                    inflight.push(upload(
                        self.protocol.clone(),
                        key.clone(),
                        upload_id.clone(),
                        number,
                        part,
                    ));
                    number += 1;
                    if inflight.len() >= MAX_INFLIGHT_PARTS {
                        let Some(completed) = inflight.next().await else {
                            return Err(entry(
                                stage.final_destination.path(),
                                Operation::Write,
                                "S3 inflight upload disappeared",
                            ));
                        };
                        parts.push(completed.map_err(|e| {
                            role_failure(stage.final_destination.path(), Operation::Write, e)
                        })?);
                    }
                }
            }
            if !buffered.is_empty() || (parts.is_empty() && inflight.is_empty()) {
                inflight.push(upload(
                    self.protocol.clone(),
                    key.clone(),
                    upload_id.clone(),
                    number,
                    buffered.freeze(),
                ));
            }
            while let Some(part) = inflight.next().await {
                parts.push(part.map_err(|e| {
                    role_failure(stage.final_destination.path(), Operation::Write, e)
                })?);
            }
            parts.sort_by_key(|part| part.0);
            self.protocol
                .complete_multipart(&key, &upload_id, &parts)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Write, e))?;
            Ok(parts.iter().map(|_| 0u64).sum())
        }
        .await;
        result?;
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
        if let Some(single) = single::of(stage) {
            return Ok(CheckpointObservation {
                durable_prefix: single.written_len(),
            });
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
        if let Some(single) = single::of(stage) {
            return single::verify(self, stage, single, &request).await;
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
        let key = self.validate(stage)?;
        if let Some(single) = single::of(stage) {
            if self.metadata.is_none() {
                return Err(metadata_unavailable(stage));
            }
            return single::apply_metadata(stage, single, self.tags_supported, mutation, &cancel);
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
        if let Some(single) = single::of(stage) {
            return single::publish(self, stage, single, &request).await;
        }
        publication::publish(self, stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let key = self.validate(&stage)?;
        if single::of(&stage).is_some() {
            // Nothing was sent before publication, and a published object is never deleted.
            return Ok(());
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
                self.protocol.delete_object(&key).await,
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
                self.protocol.delete_object(&key).await,
            )?;
        }
        self.states.lock().await.remove(stage.token.as_ref());
        Ok(())
    }

    /// A single PUT writes the final key itself, so it is read back only after publication.
    fn verification_point(&self, stage: &PreparedStage) -> VerificationPoint {
        if single::of(stage).is_some() {
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
