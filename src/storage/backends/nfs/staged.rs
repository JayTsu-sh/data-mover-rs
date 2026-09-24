use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesOrdered;

use super::at_destination::{remove_pointer, write_pointer};
use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_native;
use crate::storage::{
    ByteStream, CheckpointObservation, Metadata, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};

#[path = "positioned_writer.rs"]
mod positioned_writer;

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
    /// Whether the negotiated mount accepts an ACL. Asked of the protocol rather than cached at
    /// connect time, so a remount that renegotiates is reflected. Defaults to refusing, which is
    /// the safe answer for a protocol object that does not know.
    fn supports_acl(&self) -> bool {
        false
    }
    /// Whether the negotiated mount stores named attributes.
    fn supports_xattrs(&self) -> bool {
        false
    }
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
    /// Whether `path` is a regular file, and its size. The default treats whatever `size`
    /// answers for as a file; a protocol that sees file types says otherwise.
    async fn stat(&self, path: &StoragePath) -> Result<(bool, u64), NfsProtocolFailure> {
        self.size(path).await.map(|size| (true, size))
    }
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
    /// The stage's pointer's nonce and transfer identity (ADR-0006).
    pub(super) fence: super::at_destination::Fence,
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

    pub(super) fn validate(
        &self,
        stage: &PreparedStage,
    ) -> Result<StoragePath, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                FailureClass::Conflict,
                Transience::Permanent,
            )
        })?;
        let path = super::at_destination::stage_path(stage)?;
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

    /// Gives a stage file its owner's write permission back, through the metadata role; without
    /// one there is nothing to do it with.
    pub(super) async fn restore_owner_write(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Err(failure(
                path,
                FailureClass::PermissionDenied,
                Transience::Permanent,
            ));
        };
        metadata
            .apply(
                path,
                MetadataMutation::Mode(0o600),
                tokio_util::sync::CancellationToken::new(),
            )
            .await
    }

    pub(super) fn cache_prepared_handle(&self, token: Bytes, handle: Arc<dyn NfsStageFile>) {
        self.prepared_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token, handle);
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

    pub(super) fn claim_authority(&self, token: Bytes) {
        self.owned_stages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token);
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
        // The caller has already confirmed this prefix durable. Later writes may proceed. The
        // pointer is the whole recovery record: nothing registers where data-mover runs.
        write_pointer(self, stage, durable_prefix, false).await?;
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
                    remove_pointer(self, stage)
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
                remove_pointer(self, stage)
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
                timestamps: crate::storage::CopiedTimestampTarget::Stored(
                    crate::model::TimePrecision::Nanoseconds,
                ),
                ownership: crate::storage::CopiedOwnershipTarget::Numeric,
                acl: if self.protocol.supports_acl() {
                    crate::storage::CopiedAclTarget::Encoding(crate::model::AclEncoding::NfsV4)
                } else {
                    crate::storage::CopiedAclTarget::Unsupported
                },
                xattrs: if self.protocol.supports_xattrs() {
                    crate::storage::CopiedValueTarget::Supported
                } else {
                    crate::storage::CopiedValueTarget::Unsupported
                },
            })
    }

    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        Some(crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES)
    }

    /// NFS keeps its recovery state at the destination: every stage is prepared through
    /// [`StagedDestination::prepare_at_destination`].
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
        request: crate::storage::DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        super::at_destination::prepare(self, request).await
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
            write_pointer(self, stage, progress.persisted, false).await?;
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
            write_pointer(self, stage, progress.persisted, false).await?;
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
                super::at_destination::reobserve(self, stage).await?
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
        super::at_destination::before_publication(self, stage)
            .await
            .map_err(|error| PublicationFailure {
                error,
                final_destination_changed: false,
            })?;
        if let Err(rename_error) = self.protocol.rename(&staged, &final_path).await {
            return self
                .reconcile_rename_failure(stage, final_path, &request, rename_error)
                .await;
        }
        remove_pointer(self, stage)
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
        self.validate(&stage)?;
        super::at_destination::discard(self, &stage).await
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

pub(super) fn checked_final(path: &StoragePath) -> Result<(), StorageRoleFailure> {
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
        || is_artifact_native(&native)
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

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    entry_failure(
        path,
        Operation::Prepare,
        FailureClass::Unsupported,
        Transience::Permanent,
    )
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
