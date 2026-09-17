use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesOrdered;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, CheckpointObservation, Metadata, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};

#[path = "positioned_writer.rs"]
mod positioned_writer;

pub(crate) const INTERNAL_PREFIX: &str = ".data-mover-";

#[async_trait]
pub(crate) trait NfsStageFile: Send + Sync {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure>;
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure>;
    async fn write_uncommitted_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> Result<u64, NfsProtocolFailure> {
        self.write_at(offset, data).await
    }
    async fn write_deferred_at(&self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure> {
        self.write_at(offset, data).await
    }
    async fn write_until_checkpoint_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> Result<u64, NfsProtocolFailure> {
        self.write_deferred_at(offset, data).await
    }
    async fn checkpoint(&self) -> Result<(), NfsProtocolFailure> {
        Ok(())
    }
    async fn set_len(&self, size: u64) -> Result<(), NfsProtocolFailure>;
    async fn close(&self) -> Result<(), NfsProtocolFailure>;
    async fn close_uncommitted(&self) -> Result<(), NfsProtocolFailure> {
        self.close().await
    }
}

#[async_trait]
pub(crate) trait NfsStagedProtocol: Send + Sync {
    fn read_inflight(&self) -> usize;
    fn write_inflight(&self) -> usize;
    fn maximum_read_chunk_bytes(&self) -> usize {
        1024 * 1024
    }
    fn maximum_write_chunk_bytes(&self) -> usize {
        1024 * 1024
    }
    async fn create_empty(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
    async fn create_empty_open(
        &self,
        path: &StoragePath,
    ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
        self.create_empty(path).await?;
        match self.open_write(path).await {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let _ = self.delete(path).await;
                Err(error)
            }
        }
    }
    async fn open_read(
        &self,
        path: &StoragePath,
    ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure>;
    async fn open_write(
        &self,
        path: &StoragePath,
    ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure>;
    async fn size(&self, path: &StoragePath) -> Result<u64, NfsProtocolFailure>;
    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), NfsProtocolFailure>;
    async fn delete(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
}

pub(crate) struct NfsStagedDestinationAdapter {
    pub(super) protocol: Arc<dyn NfsStagedProtocol>,
    pub(super) identity: BackendIdentity,
    owned_stages: Mutex<HashSet<Bytes>>,
    prepared_handles: Mutex<HashMap<Bytes, Arc<dyn NfsStageFile>>>,
    metadata: Option<Arc<dyn Metadata>>,
}

pub(super) struct NfsStageState {
    pub(super) checkpoint_created: std::sync::atomic::AtomicBool,
}

struct NfsWriteProgress {
    issued: u64,
    persisted: u64,
    failure: Option<StorageRoleFailure>,
    deferred_writes_used: bool,
}

impl NfsWriteProgress {
    fn finished(
        issued: u64,
        persisted: u64,
        failure: Option<StorageRoleFailure>,
        deferred_writes_used: bool,
    ) -> Self {
        Self {
            issued,
            persisted,
            failure,
            deferred_writes_used,
        }
    }
}

impl NfsStagedDestinationAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsStagedProtocol>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            owned_stages: Mutex::new(HashSet::new()),
            prepared_handles: Mutex::new(HashMap::new()),
            metadata: None,
        }
    }

    pub(crate) fn with_metadata(mut self, metadata: Arc<dyn Metadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    fn validate(&self, stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                FailureClass::Conflict,
                Transience::Permanent,
            )
        })?;
        let path = Self::validate_token_shape(&stage.token, stage.final_destination.path())?;
        if !self
            .owned_stages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&stage.token)
        {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        Ok(path)
    }

    fn take_prepared_handle(&self, token: &Bytes) -> Option<Arc<dyn NfsStageFile>> {
        self.prepared_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(token)
    }

    async fn open_stage_for_write(
        &self,
        stage: &PreparedStage,
        native: &StoragePath,
    ) -> Result<Arc<dyn NfsStageFile>, StorageRoleFailure> {
        if let Some(handle) = self.take_prepared_handle(&stage.token) {
            return Ok(handle);
        }
        self.protocol
            .open_write(native)
            .await
            .map(Arc::from)
            .map_err(|error| role_failure(stage.final_destination.path(), Operation::Write, error))
    }

    async fn prepare_stage(
        &self,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        checked_final(request.final_destination.path())?;
        let name = crate::storage::artifacts::stage_name(request.final_destination.path().as_str());
        let native = sibling_path(request.final_destination.path(), &name)?;
        let token = Bytes::copy_from_slice(native.as_str().as_bytes());
        let open_handle: Arc<dyn NfsStageFile> = self
            .protocol
            .create_empty_open(&native)
            .await
            .map_err(|error| {
                role_failure(request.final_destination.path(), Operation::Prepare, error)
            })?
            .into();
        let _ = self.claim_authority(token.clone());
        self.prepared_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token.clone(), open_handle);
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token.clone(),
            request.recovery_binding,
            0,
            None,
        );
        stage.backend_state = Some(Arc::new(NfsStageState {
            checkpoint_created: std::sync::atomic::AtomicBool::new(false),
        }));
        Ok(stage)
    }

    pub(super) fn validate_token_shape(
        token: &Bytes,
        final_path: &StoragePath,
    ) -> Result<StoragePath, StorageRoleFailure> {
        Self::stage_token(token, final_path).map(|(path, _)| path)
    }

    pub(super) fn stage_id(
        token: &Bytes,
        final_path: &StoragePath,
    ) -> Result<String, StorageRoleFailure> {
        Self::stage_token(token, final_path).map(|(_, stage_id)| stage_id)
    }

    fn stage_token(
        token: &Bytes,
        final_path: &StoragePath,
    ) -> Result<(StoragePath, String), StorageRoleFailure> {
        let token = std::str::from_utf8(token)
            .map_err(|_| failure(final_path, FailureClass::Corruption, Transience::Permanent))?;
        let path = PathBuf::from(token);
        let final_native = PathBuf::from(final_path.as_str());
        let expected_parent = final_native
            .parent()
            .unwrap_or_else(|| std::path::Path::new(""));
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| failure(final_path, FailureClass::Corruption, Transience::Permanent))?;
        let stage_id = crate::storage::artifacts::stage_base(name, final_path.as_str())
            .ok_or_else(|| failure(final_path, FailureClass::Conflict, Transience::Permanent))?;
        if path.parent().unwrap_or_else(|| std::path::Path::new("")) != expected_parent
            || path == final_native
        {
            return Err(failure(
                final_path,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let path = StoragePath::new(path.to_string_lossy())
            .map_err(|_| failure(final_path, FailureClass::Corruption, Transience::Permanent))?;
        Ok((path, stage_id.to_owned()))
    }

    pub(super) fn claim_authority(&self, token: Bytes) -> bool {
        self.owned_stages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token)
    }

    pub(super) fn release_authority(&self, stage: &PreparedStage) {
        self.owned_stages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&stage.token);
    }

    pub(super) async fn close_prepared_handle(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        if let Some(handle) = self.take_prepared_handle(&stage.token) {
            handle.close().await.map_err(|error| {
                role_failure(stage.final_destination.path(), Operation::Write, error)
            })?;
        }
        Ok(())
    }

    async fn size(&self, stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
        let path = self.validate(stage)?;
        self.protocol
            .size(&path)
            .await
            .map_err(|error| role_failure(stage.final_destination.path(), Operation::Write, error))
    }

    async fn finish_write_handle(
        stage: &PreparedStage,
        handle: &Arc<dyn NfsStageFile>,
        deferred_writes_used: bool,
        write_failed: bool,
        persisted: u64,
        issued: u64,
    ) -> Result<(), StorageRoleFailure> {
        let checkpoint_required = stage.recovery_enabled()
            || (stage.durable_publication && deferred_writes_used && !write_failed);
        let checkpoint_failure = if checkpoint_required {
            handle.checkpoint().await.err()
        } else {
            None
        };
        let retained_prefix = if checkpoint_failure.is_some() {
            stage.write_offset
        } else {
            persisted
        };
        let truncate_failure = if write_failed && retained_prefix < issued {
            handle.set_len(retained_prefix).await.err()
        } else {
            None
        };
        let close_failure = if stage.recovery_enabled() || stage.durable_publication {
            handle.close().await.err()
        } else {
            handle.close_uncommitted().await.err()
        };
        if let Some(error) = truncate_failure.or(checkpoint_failure).or(close_failure) {
            return Err(role_failure(
                stage.final_destination.path(),
                Operation::Write,
                error,
            ));
        }
        Ok(())
    }

    async fn persist_deferred_checkpoint_record(
        &self,
        stage: &PreparedStage,
        durable_prefix: u64,
    ) -> Result<(), StorageRoleFailure> {
        // The caller has already confirmed this prefix durable. Later writes may proceed.
        super::checkpoint::persist(self, stage, durable_prefix).await?;
        if stage.recovery_enabled() {
            return Ok(());
        }
        let identity = super::recovery::export(self, stage).await?;
        let checkpoint = stage.deferred_checkpoint.as_ref().ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                FailureClass::Internal,
                Transience::Permanent,
            )
        })?;
        checkpoint.registration.register(stage, identity).await?;
        stage
            .recovery_enabled
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn checkpoint_prefix(
        stage: &PreparedStage,
        handle: &Arc<dyn NfsStageFile>,
        persisted: u64,
        issued: u64,
    ) -> Result<(), StorageRoleFailure> {
        if persisted != issued {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Unknown,
            ));
        }
        handle
            .checkpoint()
            .await
            .map_err(|error| role_failure(stage.final_destination.path(), Operation::Write, error))
    }

    pub(super) async fn reobserve_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<u64, StorageRoleFailure> {
        let durable_prefix = super::checkpoint::load(
            self,
            stage.recovery_binding,
            stage.final_destination.path(),
            &Self::stage_id(&stage.token, stage.final_destination.path())?,
        )
        .await?;
        if self.size(stage).await? < durable_prefix {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Permanent,
            ));
        }
        Ok(durable_prefix)
    }

    async fn hash(
        &self,
        path: &StoragePath,
        native: StoragePath,
        expected_size: u64,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<[u8; 32], StorageRoleFailure> {
        let handle: Arc<dyn NfsStageFile> = self
            .protocol
            .open_read(&native)
            .await
            .map_err(|error| role_failure(path, Operation::Verify, error))?
            .into();
        let concurrency = self.protocol.read_inflight().max(1);
        let maximum_chunk_bytes = self.protocol.maximum_read_chunk_bytes().max(1);
        let mut reads = FuturesOrdered::<NfsHashFuture>::new();
        let mut next_issue = 0_u64;
        let mut eof_probe_issued = false;
        let mut hasher = blake3::Hasher::new();
        let mut first_failure = None;

        loop {
            while first_failure.is_none()
                && reads.len() < concurrency
                && (next_issue < expected_size || !eof_probe_issued)
            {
                if cancel.is_cancelled() {
                    first_failure = Some(failure(
                        path,
                        FailureClass::Cancelled,
                        Transience::Transient,
                    ));
                    break;
                }
                if next_issue == expected_size {
                    eof_probe_issued = true;
                    let handle = Arc::clone(&handle);
                    reads.push_back(Box::pin(async move {
                        // An empty read at the expected EOF proves there is no unverified tail.
                        // The zero expected length lets the common result path reject any byte.
                        (0, handle.read_at(expected_size, 1).await)
                    }));
                    continue;
                }
                let count =
                    usize::try_from((expected_size - next_issue).min(maximum_chunk_bytes as u64))
                        .map_err(|_| {
                        failure(path, FailureClass::InvalidInput, Transience::Permanent)
                    })?;
                let offset = next_issue;
                next_issue = next_issue.checked_add(count as u64).ok_or_else(|| {
                    failure(path, FailureClass::InvalidInput, Transience::Permanent)
                })?;
                let handle = Arc::clone(&handle);
                reads.push_back(Box::pin(async move {
                    (count, handle.read_at(offset, count).await)
                }));
            }

            let Some((count, result)) = reads.next().await else {
                break;
            };
            match result {
                Ok(bytes) if bytes.len() == count && first_failure.is_none() => {
                    hasher.update(&bytes);
                }
                Ok(bytes) if bytes.len() != count && first_failure.is_none() => {
                    first_failure =
                        Some(failure(path, FailureClass::Corruption, Transience::Unknown));
                }
                Err(error) if first_failure.is_none() => {
                    first_failure = Some(role_failure(path, Operation::Verify, error));
                }
                Ok(_) | Err(_) => {}
            }
        }
        handle
            .close()
            .await
            .map_err(|error| role_failure(path, Operation::Verify, error))?;
        if let Some(error) = first_failure {
            return Err(error);
        }
        Ok(*hasher.finalize().as_bytes())
    }

    async fn reconcile_rename_failure(
        &self,
        stage: &PreparedStage,
        final_path: StoragePath,
        request: &PublishRequest,
        rename_error: NfsProtocolFailure,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let staged_path = self.validate(stage).map_err(|error| PublicationFailure {
            error,
            final_destination_changed: true,
        })?;
        let final_observation = self.protocol.size(&final_path).await;
        let final_size_matches = final_observation
            .as_ref()
            .is_ok_and(|size| *size == request.expected_size);
        let final_equivalent = match request.expected_blake3 {
            Some(expected) if final_size_matches => self
                .hash(
                    stage.final_destination.path(),
                    final_path,
                    request.expected_size,
                    &request.cancel,
                )
                .await
                .is_ok_and(|hash| hash == expected),
            None => final_size_matches,
            Some(_) => false,
        };
        match self.protocol.size(&staged_path).await {
            Err(NfsProtocolFailure {
                class: FailureClass::NotFound,
                ..
            }) => {
                if final_equivalent {
                    super::checkpoint::remove(self, stage)
                        .await
                        .map_err(|error| PublicationFailure {
                            error,
                            final_destination_changed: true,
                        })?;
                    self.release_authority(stage);
                    Ok(PublicationEvidence {
                        final_destination: stage.final_destination.path().clone(),
                        disposition: PublicationDisposition::Published,
                    })
                } else {
                    Err(PublicationFailure {
                        error: role_failure(
                            stage.final_destination.path(),
                            Operation::Publish,
                            rename_error,
                        ),
                        final_destination_changed: true,
                    })
                }
            }
            Ok(_) if final_equivalent && request.expected_blake3.is_some() => {
                self.protocol
                    .delete(&staged_path)
                    .await
                    .map_err(|error| PublicationFailure {
                        error: role_failure(
                            stage.final_destination.path(),
                            Operation::Publish,
                            error,
                        ),
                        final_destination_changed: true,
                    })?;
                super::checkpoint::remove(self, stage)
                    .await
                    .map_err(|error| PublicationFailure {
                        error,
                        final_destination_changed: true,
                    })?;
                self.release_authority(stage);
                Ok(PublicationEvidence {
                    final_destination: stage.final_destination.path().clone(),
                    disposition: PublicationDisposition::Published,
                })
            }
            Ok(_) => Err(PublicationFailure {
                error: role_failure(
                    stage.final_destination.path(),
                    Operation::Publish,
                    rename_error,
                ),
                final_destination_changed: false,
            }),
            Err(error) => Err(PublicationFailure {
                error: role_failure(stage.final_destination.path(), Operation::Publish, error),
                final_destination_changed: true,
            }),
        }
    }
}

#[async_trait]
impl StagedDestination for NfsStagedDestinationAdapter {
    fn copied_metadata_target(&self) -> Option<crate::storage::CopiedMetadataTarget> {
        self.metadata
            .as_ref()
            .map(|_| crate::storage::CopiedMetadataTarget {
                timestamp_precision: crate::model::TimePrecision::Nanoseconds,
                ownership: crate::storage::CopiedOwnershipTarget::Numeric,
            })
    }

    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        Some(crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES)
    }

    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let stage = self.prepare_stage(request).await?;
        if let Err(error) = super::checkpoint::persist(self, &stage, 0).await {
            let native = Self::validate_token_shape(&stage.token, stage.final_destination.path())?;
            if let Some(handle) = self.take_prepared_handle(&stage.token) {
                let _ = handle.close().await;
            }
            let _ = self.protocol.delete(&native).await;
            self.release_authority(&stage);
            return Err(error);
        }
        Ok(stage)
    }

    async fn prepare_ephemeral(
        &self,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare_stage(request)
            .await
            .map(PreparedStage::disable_recovery)
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        super::recovery::export(self, stage).await
    }

    async fn handoff_recovery(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        super::recovery::handoff(self, stage).await
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        super::recovery::recover(self, request).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let native = self.validate(stage)?;
        let handle = self.open_stage_for_write(stage, &native).await?;
        let concurrency = self.protocol.write_inflight().max(1);
        let maximum_chunk_bytes = self.protocol.maximum_write_chunk_bytes().max(1);
        let progress = self
            .consume_input(stage, &mut input, &handle, concurrency, maximum_chunk_bytes)
            .await;
        Self::finish_write_handle(
            stage,
            &handle,
            progress.deferred_writes_used,
            progress.failure.is_some(),
            progress.persisted,
            progress.issued,
        )
        .await?;
        if let Some(error) = progress.failure {
            return Err(error);
        }
        if progress.persisted != progress.issued {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Unknown,
            ));
        }
        if stage.recovery_enabled() {
            super::checkpoint::persist(self, stage, progress.persisted).await?;
        }
        Ok(WriteEvidence {
            persisted_bytes: progress.persisted,
        })
    }

    fn supports_positioned_write(&self) -> bool {
        true
    }

    async fn write_positioned(
        &self,
        stage: &PreparedStage,
        mut input: crate::storage::PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let native = self.validate(stage)?;
        let handle = self.open_stage_for_write(stage, &native).await?;
        let concurrency = self.protocol.write_inflight().max(1);
        let maximum_chunk_bytes = self.protocol.maximum_write_chunk_bytes().max(1);
        let progress = self
            .consume_positioned_stream(stage, &mut input, &handle, concurrency, maximum_chunk_bytes)
            .await;
        Self::finish_write_handle(
            stage,
            &handle,
            progress.deferred_writes_used,
            progress.failure.is_some(),
            progress.persisted,
            progress.issued,
        )
        .await?;
        if let Some(error) = progress.failure {
            return Err(error);
        }
        if progress.persisted != progress.issued {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Unknown,
            ));
        }
        if stage.recovery_enabled() {
            super::checkpoint::persist(self, stage, progress.persisted).await?;
        }
        Ok(WriteEvidence {
            persisted_bytes: progress.persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        Ok(CheckpointObservation {
            durable_prefix: if stage.recovery_enabled() {
                self.reobserve_checkpoint(stage).await?
            } else {
                self.size(stage).await?
            },
        })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Cancelled,
                Transience::Transient,
            ));
        }
        let native = self.validate(stage)?;
        let hash = self
            .hash(
                stage.final_destination.path(),
                native,
                request.expected_size,
                &request.cancel,
            )
            .await?;
        if hash != request.expected_blake3 {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Permanent,
            ));
        }
        Ok(VerificationEvidence {
            verified_bytes: request.expected_size,
            blake3: hash,
        })
    }

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let path = self.validate(stage)?;
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                FailureClass::Unsupported,
                Transience::Permanent,
            )
        })?;
        metadata.apply(&path, mutation, cancel).await
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        if request.cancel.is_cancelled() {
            return Err(publication_failure(stage, FailureClass::Cancelled, false));
        }
        let staged = self.validate(stage).map_err(|error| PublicationFailure {
            error,
            final_destination_changed: false,
        })?;
        let final_path = stage.final_destination.path().clone();
        if let Err(rename_error) = self.protocol.rename(&staged, &final_path).await {
            return self
                .reconcile_rename_failure(stage, final_path, &request, rename_error)
                .await;
        }
        super::checkpoint::remove(self, stage)
            .await
            .map_err(|error| PublicationFailure {
                error,
                final_destination_changed: true,
            })?;
        self.release_authority(stage);
        Ok(PublicationEvidence {
            final_destination: stage.final_destination.path().clone(),
            disposition: PublicationDisposition::Published,
        })
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let native = self.validate(&stage)?;
        let close_failure = if let Some(handle) = self.take_prepared_handle(&stage.token) {
            handle.close().await.err()
        } else {
            None
        };
        let stage_delete_failure = match self.protocol.delete(&native).await {
            Ok(())
            | Err(NfsProtocolFailure {
                class: FailureClass::NotFound,
                ..
            }) => None,
            Err(error) => Some(role_failure(
                stage.final_destination.path(),
                Operation::Write,
                error,
            )),
        };
        let checkpoint_delete_failure = super::checkpoint::remove(self, &stage).await.err();
        if let Some(error) = stage_delete_failure.or(checkpoint_delete_failure) {
            return Err(error);
        }
        self.release_authority(&stage);
        if let Some(error) = close_failure {
            return Err(role_failure(
                stage.final_destination.path(),
                Operation::Write,
                error,
            ));
        }
        Ok(())
    }
}

type NfsWriteFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = (u64, u64, Result<u64, NfsProtocolFailure>)> + Send>,
>;

fn next_deferred_checkpoint(stage: &PreparedStage, from: u64) -> Option<u64> {
    stage
        .deferred_checkpoint
        .as_ref()
        .and_then(|checkpoint| from.checked_add(checkpoint.interval_bytes))
}

fn deferred_checkpoint_due(
    stage: &PreparedStage,
    next_checkpoint: Option<u64>,
    issued: u64,
) -> bool {
    next_checkpoint.is_some_and(|boundary| {
        issued >= boundary
            && stage
                .deferred_checkpoint
                .as_ref()
                .is_some_and(|checkpoint| boundary < checkpoint.source_size)
    })
}

type NfsHashFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = (usize, Result<Bytes, NfsProtocolFailure>)> + Send>,
>;

fn checked_final(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    let native = PathBuf::from(path.as_str());
    if path.as_str().is_empty()
        || native.is_absolute()
        || native.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
        || native.components().any(|component| {
            matches!(component, std::path::Component::Normal(name) if name.to_str().is_some_and(|name| name.starts_with(INTERNAL_PREFIX)))
        })
    {
        Err(failure(
            path,
            FailureClass::InvalidInput,
            Transience::Permanent,
        ))
    } else {
        Ok(())
    }
}

pub(super) fn sibling_path(
    final_path: &StoragePath,
    name: &str,
) -> Result<StoragePath, StorageRoleFailure> {
    let final_native = PathBuf::from(final_path.as_str());
    let parent = final_native
        .parent()
        .unwrap_or_else(|| std::path::Path::new(""));
    StoragePath::new(parent.join(name).to_string_lossy()).map_err(|_| {
        failure(
            final_path,
            FailureClass::InvalidInput,
            Transience::Permanent,
        )
    })
}

fn failure(path: &StoragePath, class: FailureClass, transience: Transience) -> StorageRoleFailure {
    entry_failure(path, Operation::Write, class, transience)
}

fn publication_failure(
    stage: &PreparedStage,
    class: FailureClass,
    changed: bool,
) -> PublicationFailure {
    PublicationFailure {
        error: failure(stage.final_destination.path(), class, Transience::Permanent),
        final_destination_changed: changed,
    }
}

include!("staged_tests.rs");
