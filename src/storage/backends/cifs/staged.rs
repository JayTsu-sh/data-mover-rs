use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::source::{classify, entry_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath};
use crate::storage::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, Metadata, MetadataMutation,
    PrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure,
    PublishRequest, RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure,
    VerificationEvidence, VerifyRequest, WriteEvidence,
};

const STAGING_DIRECTORY: &str = ".data-mover-staging";
const VERIFY_CHUNK: u32 = 1024 * 1024;
const MAX_INFLIGHT_WRITES: usize = 4;

#[async_trait]
pub(super) trait CifsStageFile: Send + Sync {
    fn maximum_read_chunk(&self) -> u32;
    fn maximum_write_chunk(&self) -> u32;
    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes>;
    async fn write_all_at(&self, offset: u64, bytes: Bytes) -> smb_domain::Result<()>;
    async fn flush(&self) -> smb_domain::Result<()>;
    async fn close(self: Box<Self>) -> smb_domain::Result<()>;
}

#[async_trait]
pub(super) trait CifsStagedProtocol: Send + Sync {
    async fn create_empty(&self, path: &StoragePath) -> smb_domain::Result<()>;
    async fn open(&self, path: &StoragePath) -> smb_domain::Result<Box<dyn CifsStageFile>>;
    async fn size(&self, path: &StoragePath) -> smb_domain::Result<u64>;
    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        replace: bool,
    ) -> smb_domain::Result<()>;
    async fn delete(&self, path: &StoragePath) -> smb_domain::Result<()>;
}

pub(super) struct CifsStagedDestination {
    protocol: Arc<dyn CifsStagedProtocol>,
    identity: BackendIdentity,
    owned: Mutex<HashSet<Bytes>>,
    metadata: Option<Arc<dyn Metadata>>,
}

impl CifsStagedDestination {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsStagedProtocol + 'static,
    {
        Self {
            protocol,
            identity,
            owned: Mutex::new(HashSet::new()),
            metadata: None,
        }
    }

    pub(super) fn with_metadata(mut self, metadata: Arc<dyn Metadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    fn stage_path(&self, stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            entry_failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Conflict,
            )
        })?;
        if !self
            .owned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&stage.token)
        {
            return Err(entry_failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Conflict,
            ));
        }
        token_path(&stage.token, stage.final_destination.path())
    }

    fn claim(&self, token: Bytes) -> bool {
        self.owned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token)
    }

    fn release(&self, token: &Bytes) {
        self.owned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(token);
    }

    async fn hash(
        &self,
        path: &StoragePath,
        expected_size: u64,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<[u8; 32], StorageRoleFailure> {
        let file = self
            .protocol
            .open(path)
            .await
            .map_err(|error| classify(path, Operation::Verify, &error))?;
        let mut offset = 0;
        let mut hasher = blake3::Hasher::new();
        while offset < expected_size {
            if cancel.is_cancelled() {
                let _ = file.close().await;
                return Err(entry_failure(
                    path,
                    Operation::Verify,
                    FailureClass::Cancelled,
                ));
            }
            let count = u32::try_from(expected_size - offset)
                .unwrap_or(u32::MAX)
                .min(VERIFY_CHUNK)
                .min(file.maximum_read_chunk());
            let bytes = match file.read_at(offset, count).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    let failure = classify(path, Operation::Verify, &error);
                    let _ = file.close().await;
                    return Err(failure);
                }
            };
            if bytes.len() != count as usize {
                let _ = file.close().await;
                return Err(entry_failure(
                    path,
                    Operation::Verify,
                    FailureClass::Corruption,
                ));
            }
            hasher.update(&bytes);
            offset += bytes.len() as u64;
        }
        file.close()
            .await
            .map_err(|error| classify(path, Operation::Verify, &error))?;
        Ok(*hasher.finalize().as_bytes())
    }

    async fn verify_existing(
        &self,
        stage: &PreparedStage,
        request: &PublishRequest,
        stage_path: &StoragePath,
    ) -> Result<Option<PublicationEvidence>, PublicationFailure> {
        if request.policy != ExistingDestinationPolicy::VerifyOrSkip {
            return Ok(None);
        }
        let final_path = stage.final_destination.path();
        let size = match self.protocol.size(final_path).await {
            Ok(size) if size == request.expected_size => size,
            Ok(_) => return Err(publication_conflict(final_path)),
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => {
                return Err(publication_unchanged(classify(
                    final_path,
                    Operation::Publish,
                    &error,
                )));
            }
        };
        let hash = self
            .hash(final_path, size, &request.cancel)
            .await
            .map_err(publication_unchanged)?;
        if hash != request.expected_blake3 {
            return Err(publication_conflict(final_path));
        }
        self.protocol.delete(stage_path).await.map_err(|error| {
            publication_unchanged(classify(final_path, Operation::Publish, &error))
        })?;
        self.release(&stage.token);
        Ok(Some(PublicationEvidence {
            final_destination: final_path.clone(),
            disposition: PublicationDisposition::ExistingEquivalent,
        }))
    }

    async fn reconcile_rename(
        &self,
        stage: &PreparedStage,
        request: &PublishRequest,
        stage_path: &StoragePath,
        rename_error: &smb_domain::Error,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let final_path = stage.final_destination.path();
        let stage_observation = self.protocol.size(stage_path).await;
        let committed = matches!(stage_observation, Err(ref error) if is_not_found(error))
            && self
                .protocol
                .size(final_path)
                .await
                .is_ok_and(|size| size == request.expected_size)
            && self
                .hash(final_path, request.expected_size, &request.cancel)
                .await
                .is_ok_and(|hash| hash == request.expected_blake3);
        if committed {
            self.release(&stage.token);
            return Ok(published(final_path));
        }
        Err(PublicationFailure {
            error: classify(final_path, Operation::Publish, rename_error),
            final_destination_changed: stage_observation.is_err(),
        })
    }
}

#[async_trait]
impl StagedDestination for CifsStagedDestination {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        validate_final(request.final_destination.path())?;
        let final_hash = blake3::hash(request.final_destination.path().as_str().as_bytes());
        let token = format!(
            "{STAGING_DIRECTORY}/{}-{}.part",
            &final_hash.to_hex()[..16],
            uuid::Uuid::new_v4().simple()
        );
        let path = StoragePath::new(&token).map_err(|_| {
            entry_failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::InvalidInput,
            )
        })?;
        self.protocol.create_empty(&path).await.map_err(|error| {
            classify(request.final_destination.path(), Operation::Prepare, &error)
        })?;
        let token = Bytes::from(token);
        let _ = self.claim(token.clone());
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
        let _ = self.stage_path(stage)?;
        let mut value = BytesMut::new();
        value.extend_from_slice(b"data-mover:cifs-recovery:v1\0");
        value.extend_from_slice(&stage.recovery_binding);
        value.extend_from_slice(
            blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
        );
        value.extend_from_slice(&stage.token);
        RecoveryIdentity::from_bytes(value.freeze()).map_err(|_| {
            entry_failure(
                stage.final_destination.path(),
                Operation::Observe,
                FailureClass::Protocol,
            )
        })
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let bytes = request.identity.as_bytes();
        let header = b"data-mover:cifs-recovery:v1\0";
        let fixed = header.len() + 64;
        if bytes.len() <= fixed
            || &bytes[..header.len()] != header
            || bytes[header.len()..header.len() + 32] != request.recovery_binding
            || bytes[header.len() + 32..fixed]
                != *blake3::hash(request.final_destination.path().as_str().as_bytes()).as_bytes()
        {
            return Err(entry_failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Conflict,
            ));
        }
        let original = Bytes::copy_from_slice(&bytes[fixed..]);
        let original_path = token_path(&original, request.final_destination.path())?;
        let claim = format!(
            "{}.claim-{}",
            original_path.as_str(),
            hex_prefix(&request.claim_token)
        );
        let claimed_path = StoragePath::new(&claim).map_err(|_| {
            entry_failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::InvalidInput,
            )
        })?;
        if let Err(rename_error) = self
            .protocol
            .rename(&original_path, &claimed_path, false)
            .await
        {
            self.protocol.size(&claimed_path).await.map_err(|_| {
                classify(
                    request.final_destination.path(),
                    Operation::Prepare,
                    &rename_error,
                )
            })?;
        }
        let write_offset = self.protocol.size(&claimed_path).await.map_err(|error| {
            classify(request.final_destination.path(), Operation::Observe, &error)
        })?;
        let token = Bytes::from(claim);
        let _ = self.claim(token.clone());
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            write_offset,
            None,
        ))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let path = self.stage_path(stage)?;
        let file =
            self.protocol.open(&path).await.map_err(|error| {
                classify(stage.final_destination.path(), Operation::Write, &error)
            })?;
        let offset = match write_inflight(
            file.as_ref(),
            input,
            stage.write_offset,
            stage.final_destination.path(),
        )
        .await
        {
            Ok(offset) => offset,
            Err(failure) => {
                let _ = file.close().await;
                return Err(failure);
            }
        };
        file.close()
            .await
            .map_err(|error| classify(stage.final_destination.path(), Operation::Write, &error))?;
        Ok(WriteEvidence {
            persisted_bytes: offset,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let path = self.stage_path(stage)?;
        let durable_prefix = self.protocol.size(&path).await.map_err(|error| {
            classify(stage.final_destination.path(), Operation::Observe, &error)
        })?;
        Ok(CheckpointObservation { durable_prefix })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let path = self.stage_path(stage)?;
        let size =
            self.protocol.size(&path).await.map_err(|error| {
                classify(stage.final_destination.path(), Operation::Verify, &error)
            })?;
        if size != request.expected_size {
            return Err(entry_failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        let hash = self.hash(&path, size, &request.cancel).await?;
        if hash != request.expected_blake3 {
            return Err(entry_failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        Ok(VerificationEvidence {
            verified_bytes: size,
            blake3: hash,
        })
    }

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let path = self.stage_path(stage)?;
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            entry_failure(
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
        let path = self.stage_path(stage).map_err(publication_unchanged)?;
        if request.cancel.is_cancelled() {
            return Err(publication_unchanged(entry_failure(
                stage.final_destination.path(),
                Operation::Publish,
                FailureClass::Cancelled,
            )));
        }
        if let Some(evidence) = self.verify_existing(stage, &request, &path).await? {
            return Ok(evidence);
        }
        let replace = request.policy == ExistingDestinationPolicy::Overwrite;
        let rename = self
            .protocol
            .rename(&path, stage.final_destination.path(), replace)
            .await;
        if let Err(error) = rename {
            return self.reconcile_rename(stage, &request, &path, &error).await;
        }
        self.release(&stage.token);
        Ok(published(stage.final_destination.path()))
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let path = self.stage_path(&stage)?;
        self.protocol.delete(&path).await.map_err(|error| {
            classify(stage.final_destination.path(), Operation::Namespace, &error)
        })?;
        self.release(&stage.token);
        Ok(())
    }
}

async fn write_inflight(
    file: &dyn CifsStageFile,
    mut input: ByteStream,
    mut offset: u64,
    path: &StoragePath,
) -> Result<u64, StorageRoleFailure> {
    let maximum = usize::try_from(file.maximum_write_chunk())
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| entry_failure(path, Operation::Write, FailureClass::Protocol))?;
    let mut inflight = futures::stream::FuturesUnordered::new();
    while let Some(item) = input.next().await {
        let mut bytes = item?;
        while !bytes.is_empty() {
            let chunk = bytes.split_to(bytes.len().min(maximum));
            let chunk_offset = offset;
            offset = offset
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| entry_failure(path, Operation::Write, FailureClass::InvalidInput))?;
            inflight.push(file.write_all_at(chunk_offset, chunk));
            if inflight.len() == MAX_INFLIGHT_WRITES {
                finish_write(inflight.next().await, path)?;
            }
        }
    }
    while let Some(result) = inflight.next().await {
        result.map_err(|error| classify(path, Operation::Write, &error))?;
    }
    file.flush()
        .await
        .map_err(|error| classify(path, Operation::Write, &error))?;
    Ok(offset)
}

fn finish_write(
    result: Option<smb_domain::Result<()>>,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    result
        .ok_or_else(|| entry_failure(path, Operation::Write, FailureClass::Protocol))?
        .map_err(|error| classify(path, Operation::Write, &error))
}

fn validate_final(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    if path.as_str().is_empty()
        || path.as_str().split('/').any(|part| part == "..")
        || path.as_str().starts_with(STAGING_DIRECTORY)
    {
        return Err(entry_failure(
            path,
            Operation::Prepare,
            FailureClass::InvalidInput,
        ));
    }
    Ok(())
}

fn token_path(token: &Bytes, final_path: &StoragePath) -> Result<StoragePath, StorageRoleFailure> {
    let token = std::str::from_utf8(token)
        .map_err(|_| entry_failure(final_path, Operation::Observe, FailureClass::Corruption))?;
    let suffix = token
        .strip_prefix(&format!("{STAGING_DIRECTORY}/"))
        .ok_or_else(|| entry_failure(final_path, Operation::Observe, FailureClass::Conflict))?;
    if suffix.is_empty() || suffix.contains('/') || suffix.contains("..") {
        return Err(entry_failure(
            final_path,
            Operation::Observe,
            FailureClass::Conflict,
        ));
    }
    StoragePath::new(token)
        .map_err(|_| entry_failure(final_path, Operation::Observe, FailureClass::Corruption))
}

fn hex_prefix(value: &[u8; 32]) -> String {
    value[..8].iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    })
}

fn publication_unchanged(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

fn publication_conflict(path: &StoragePath) -> PublicationFailure {
    publication_unchanged(entry_failure(
        path,
        Operation::Publish,
        FailureClass::Conflict,
    ))
}

fn published(path: &StoragePath) -> PublicationEvidence {
    PublicationEvidence {
        final_destination: path.clone(),
        disposition: PublicationDisposition::Published,
    }
}

fn is_not_found(error: &smb_domain::Error) -> bool {
    matches!(
        classify(&StoragePath::root(), Operation::Observe, error),
        StorageRoleFailure::Entry(error) if error.class() == FailureClass::NotFound
    )
}
