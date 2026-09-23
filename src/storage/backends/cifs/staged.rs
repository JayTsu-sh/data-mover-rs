use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use super::source::{classify, entry_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath};
use crate::storage::{
    ByteStream, CheckpointObservation, Metadata, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};

const STAGING_DIRECTORY: &str = ".data-mover-staging";
const VERIFY_CHUNK: u32 = 1024 * 1024;

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

#[derive(Default)]
pub(super) struct CifsStageState {
    pub(super) checkpoint_created: std::sync::atomic::AtomicBool,
    published: std::sync::atomic::AtomicBool,
}

pub(super) fn state(stage: &PreparedStage) -> Result<&CifsStageState, StorageRoleFailure> {
    stage
        .backend_state
        .as_ref()
        .and_then(|v| v.downcast_ref::<CifsStageState>())
        .ok_or_else(|| {
            entry_failure(
                stage.final_destination.path(),
                Operation::Observe,
                FailureClass::Internal,
            )
        })
}

pub(super) struct CifsStagedDestination {
    pub(super) protocol: Arc<dyn CifsStagedProtocol>,
    identity: BackendIdentity,
    owned: Mutex<HashSet<Bytes>>,
    metadata: Option<Arc<dyn Metadata>>,
    pub(super) write_inflight: usize,
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
            write_inflight: 8,
        }
    }

    pub(super) fn with_write_inflight(mut self, depth: std::num::NonZeroUsize) -> Self {
        self.write_inflight = depth.get();
        self
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
            if count == 0 {
                let _ = file.close().await;
                return Err(entry_failure(
                    path,
                    Operation::Verify,
                    FailureClass::Protocol,
                ));
            }
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
            && match request.expected_blake3 {
                Some(expected) => self
                    .hash(final_path, request.expected_size, &request.cancel)
                    .await
                    .is_ok_and(|hash| hash == expected),
                None => true,
            };
        if committed {
            state(stage)
                .map_err(publication_unchanged)?
                .published
                .store(true, std::sync::atomic::Ordering::Release);
            if state(stage)
                .map_err(publication_unchanged)?
                .checkpoint_created
                .load(std::sync::atomic::Ordering::Acquire)
            {
                super::checkpoint::remove(self, stage_path)
                    .await
                    .map_err(|error| PublicationFailure {
                        error,
                        final_destination_changed: true,
                    })?;
            }
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
    fn copied_metadata_target(&self) -> Option<crate::storage::CopiedMetadataTarget> {
        self.metadata
            .as_ref()
            .map(|_| crate::storage::CopiedMetadataTarget {
                timestamp_precision: crate::model::TimePrecision::HundredNanoseconds,
                ownership: crate::storage::CopiedOwnershipTarget::Unsupported,
                // `metadata::decode` accepts only this encoding, so anything else has to be
                // refused as a mapping problem rather than written wrong.
                acl: crate::storage::CopiedAclTarget::Encoding(
                    crate::model::AclEncoding::WindowsSecurityDescriptor,
                ),
                xattrs: crate::storage::CopiedValueTarget::Unsupported,
            })
    }

    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        Some(64 * 1024 * 1024)
    }
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        validate_final(request.final_destination.path())?;
        let destination = request.final_destination.path().as_str();
        let name = crate::storage::artifacts::stage_name(destination);
        let token = match destination.rsplit_once('/') {
            Some((parent, _)) => format!("{parent}/{name}"),
            None => name,
        };
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
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        );
        stage.backend_state = Some(Arc::new(CifsStageState::default()));
        Ok(stage)
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
        let (parent, name) = original_path
            .as_str()
            .rsplit_once('/')
            .unwrap_or(("", original_path.as_str()));
        let base = name.split_once(".claim-").map_or(name, |(base, _)| base);
        let prefix = if parent.is_empty() {
            String::new()
        } else {
            format!("{parent}/")
        };
        let claim = format!("{prefix}{base}.claim-{}", hex_prefix(&request.claim_token));
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
            if !matches!(self.protocol.size(&original_path).await, Err(ref error) if is_not_found(error))
            {
                return Err(classify(
                    request.final_destination.path(),
                    Operation::Prepare,
                    &rename_error,
                ));
            }
            self.protocol.size(&claimed_path).await.map_err(|_| {
                classify(
                    request.final_destination.path(),
                    Operation::Prepare,
                    &rename_error,
                )
            })?;
        }
        let size = self.protocol.size(&claimed_path).await.map_err(|error| {
            classify(request.final_destination.path(), Operation::Observe, &error)
        })?;
        let write_offset =
            super::checkpoint::load(self, &claimed_path, &request.recovery_binding).await?;
        if size < write_offset || request.source.size.is_none_or(|expected| size > expected) {
            return Err(entry_failure(
                &claimed_path,
                Operation::Prepare,
                FailureClass::Corruption,
            ));
        }
        let token = Bytes::from(claim);
        let _ = self.claim(token.clone());
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            write_offset,
            None,
        );
        stage.backend_state = Some(Arc::new(CifsStageState {
            checkpoint_created: std::sync::atomic::AtomicBool::new(true),
            ..CifsStageState::default()
        }));
        Ok(stage)
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
        let result = super::writer::write(self, stage, file.as_ref(), input).await;
        let close = file.close().await;
        let offset = result?;
        close
            .map_err(|error| classify(stage.final_destination.path(), Operation::Write, &error))?;
        Ok(WriteEvidence {
            persisted_bytes: offset,
        })
    }

    fn supports_positioned_write(&self) -> bool {
        true
    }

    async fn write_positioned(
        &self,
        stage: &PreparedStage,
        input: crate::storage::PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let path = self.stage_path(stage)?;
        let file =
            self.protocol.open(&path).await.map_err(|error| {
                classify(stage.final_destination.path(), Operation::Write, &error)
            })?;
        let result = super::positioned_writer::write(self, stage, file.as_ref(), input).await;
        let close = file.close().await;
        let offset = result?;
        close
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
        let durable_prefix = if stage.recovery_enabled() {
            super::checkpoint::load(self, &path, &stage.recovery_binding).await?
        } else {
            self.protocol
                .size(&path)
                .await
                .map_err(|e| classify(&path, Operation::Observe, &e))?
        };
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
        metadata.apply(&path, mutation, cancel.clone()).await?;
        if stage.durable_publication {
            if cancel.is_cancelled() {
                return Err(entry_failure(
                    &path,
                    Operation::Metadata,
                    FailureClass::Cancelled,
                ));
            }
            let file = self
                .protocol
                .open(&path)
                .await
                .map_err(|error| classify(&path, Operation::Metadata, &error))?;
            let flushed = file.flush().await;
            let closed = file.close().await;
            flushed.map_err(|error| classify(&path, Operation::Metadata, &error))?;
            closed.map_err(|error| classify(&path, Operation::Metadata, &error))?;
        }
        Ok(())
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
        let rename = self
            .protocol
            .rename(&path, stage.final_destination.path(), true)
            .await;
        if let Err(error) = rename {
            return self.reconcile_rename(stage, &request, &path, &error).await;
        }
        state(stage)
            .map_err(publication_unchanged)?
            .published
            .store(true, std::sync::atomic::Ordering::Release);
        if state(stage)
            .map_err(publication_unchanged)?
            .checkpoint_created
            .load(std::sync::atomic::Ordering::Acquire)
        {
            super::checkpoint::remove(self, &path)
                .await
                .map_err(|error| PublicationFailure {
                    error,
                    final_destination_changed: true,
                })?;
        }
        self.release(&stage.token);
        Ok(published(stage.final_destination.path()))
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            entry_failure(
                stage.final_destination.path(),
                Operation::Namespace,
                FailureClass::Conflict,
            )
        })?;
        let published = state(&stage)?
            .published
            .load(std::sync::atomic::Ordering::Acquire);
        let path = if published {
            token_path(&stage.token, stage.final_destination.path())?
        } else {
            self.stage_path(&stage)?
        };
        if !published {
            match self.protocol.delete(&path).await {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    return Err(classify(
                        stage.final_destination.path(),
                        Operation::Namespace,
                        &error,
                    ));
                }
            }
        }
        if state(&stage)?
            .checkpoint_created
            .load(std::sync::atomic::Ordering::Acquire)
        {
            super::checkpoint::remove(self, &path).await?;
        }
        self.release(&stage.token);
        Ok(())
    }
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

pub(super) fn token_path(
    token: &Bytes,
    final_path: &StoragePath,
) -> Result<StoragePath, StorageRoleFailure> {
    let invalid = || entry_failure(final_path, Operation::Observe, FailureClass::Conflict);
    let token = std::str::from_utf8(token).map_err(|_| invalid())?;
    let (parent, name) = token.rsplit_once('/').unwrap_or(("", token));
    let expected_parent = final_path
        .as_str()
        .rsplit_once('/')
        .map_or("", |(parent, _)| parent);
    if parent != expected_parent
        || crate::storage::artifacts::stage_base(name, final_path.as_str()).is_none()
    {
        return Err(invalid());
    }
    StoragePath::new(token).map_err(|_| invalid())
}

fn hex_prefix(value: &[u8; 32]) -> String {
    value[..16].iter().fold(String::new(), |mut output, byte| {
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

fn published(path: &StoragePath) -> PublicationEvidence {
    PublicationEvidence {
        final_destination: path.clone(),
        disposition: PublicationDisposition::Published,
    }
}

pub(super) fn is_not_found(error: &smb_domain::Error) -> bool {
    matches!(
        classify(&StoragePath::root(), Operation::Observe, error),
        StorageRoleFailure::Entry(error) if error.class() == FailureClass::NotFound
    )
}
