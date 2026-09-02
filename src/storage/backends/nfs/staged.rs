use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, Metadata, MetadataMutation,
    PrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure,
    PublishRequest, RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure,
    VerificationEvidence, VerifyRequest, WriteEvidence,
};

const STAGING_DIR: &str = ".data-mover-staging";

#[async_trait]
pub(crate) trait NfsStageFile: Send {
    async fn read_at(&mut self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure>;
    async fn write_at(&mut self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure>;
    async fn close(self: Box<Self>) -> Result<(), NfsProtocolFailure>;
}

#[async_trait]
pub(crate) trait NfsStagedProtocol: Send + Sync {
    async fn create_empty(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
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
    metadata: Option<Arc<dyn Metadata>>,
}

impl NfsStagedDestinationAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsStagedProtocol>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            owned_stages: Mutex::new(HashSet::new()),
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

    pub(super) fn validate_token_shape(
        token: &Bytes,
        final_path: &StoragePath,
    ) -> Result<StoragePath, StorageRoleFailure> {
        let token = std::str::from_utf8(token)
            .map_err(|_| failure(final_path, FailureClass::Corruption, Transience::Permanent))?;
        let path = PathBuf::from(token);
        if !path.starts_with(STAGING_DIR) || path.components().count() != 2 {
            return Err(failure(
                final_path,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        StoragePath::new(path.to_string_lossy())
            .map_err(|_| failure(final_path, FailureClass::Corruption, Transience::Permanent))
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

    async fn size(&self, stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
        let path = self.validate(stage)?;
        self.protocol
            .size(&path)
            .await
            .map_err(|error| role_failure(stage.final_destination.path(), Operation::Write, error))
    }

    pub(super) async fn reobserve_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<u64, StorageRoleFailure> {
        self.size(stage).await
    }

    async fn hash(
        &self,
        path: &StoragePath,
        native: StoragePath,
        expected_size: u64,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<[u8; 32], StorageRoleFailure> {
        let mut handle = self
            .protocol
            .open_read(&native)
            .await
            .map_err(|error| role_failure(path, Operation::Verify, error))?;
        let mut offset = 0_u64;
        let mut hasher = blake3::Hasher::new();
        while offset < expected_size {
            if cancel.is_cancelled() {
                return Err(failure(
                    path,
                    FailureClass::Cancelled,
                    Transience::Transient,
                ));
            }
            let count = usize::try_from((expected_size - offset).min(1024 * 1024))
                .map_err(|_| failure(path, FailureClass::InvalidInput, Transience::Permanent))?;
            let bytes = handle
                .read_at(offset, count)
                .await
                .map_err(|error| role_failure(path, Operation::Verify, error))?;
            if bytes.len() != count {
                return Err(failure(path, FailureClass::Corruption, Transience::Unknown));
            }
            hasher.update(&bytes);
            offset += bytes.len() as u64;
        }
        handle
            .close()
            .await
            .map_err(|error| role_failure(path, Operation::Verify, error))?;
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
        let final_equivalent = if final_observation
            .as_ref()
            .is_ok_and(|size| *size == request.expected_size)
        {
            self.hash(
                stage.final_destination.path(),
                final_path,
                request.expected_size,
                &request.cancel,
            )
            .await
            .is_ok_and(|hash| hash == request.expected_blake3)
        } else {
            false
        };
        match self.protocol.size(&staged_path).await {
            Err(NfsProtocolFailure {
                class: FailureClass::NotFound,
                ..
            }) => {
                if final_equivalent {
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
            Ok(_) if final_equivalent => {
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
                self.release_authority(stage);
                Ok(PublicationEvidence {
                    final_destination: stage.final_destination.path().clone(),
                    disposition: PublicationDisposition::ExistingEquivalent,
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
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        checked_final(request.final_destination.path())?;
        let binding = blake3::hash(request.final_destination.path().as_str().as_bytes());
        let token = format!(
            "{STAGING_DIR}/{}-{}.part",
            &binding.to_hex()[..16],
            uuid::Uuid::new_v4().simple()
        );
        let native = StoragePath::new(&token).map_err(|_| {
            failure(
                request.final_destination.path(),
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        self.protocol.create_empty(&native).await.map_err(|error| {
            role_failure(request.final_destination.path(), Operation::Prepare, error)
        })?;
        let _ = self.claim_authority(Bytes::copy_from_slice(token.as_bytes()));
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            Bytes::from(token),
            request.recovery_binding,
            0,
            None,
        ))
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
        let mut handle = self.protocol.open_write(&native).await.map_err(|error| {
            role_failure(stage.final_destination.path(), Operation::Write, error)
        })?;
        let mut offset = stage.write_offset;
        while let Some(bytes) = input.next().await.transpose()? {
            let expected = bytes.len() as u64;
            let written = handle.write_at(offset, bytes).await.map_err(|error| {
                role_failure(stage.final_destination.path(), Operation::Write, error)
            })?;
            if written != expected {
                return Err(failure(
                    stage.final_destination.path(),
                    FailureClass::Corruption,
                    Transience::Unknown,
                ));
            }
            offset += written;
        }
        handle.close().await.map_err(|error| {
            role_failure(stage.final_destination.path(), Operation::Write, error)
        })?;
        Ok(WriteEvidence {
            persisted_bytes: offset,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        Ok(CheckpointObservation {
            durable_prefix: self.size(stage).await?,
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
        if self.size(stage).await? != request.expected_size {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Permanent,
            ));
        }
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
        if request.policy != ExistingDestinationPolicy::Overwrite {
            match self.protocol.size(&final_path).await {
                Ok(_) if request.policy == ExistingDestinationPolicy::FailIfExists => {
                    return Err(publication_failure(stage, FailureClass::Conflict, false));
                }
                Ok(_) => {
                    let hash = self
                        .hash(
                            stage.final_destination.path(),
                            final_path,
                            request.expected_size,
                            &request.cancel,
                        )
                        .await
                        .map_err(|error| PublicationFailure {
                            error,
                            final_destination_changed: false,
                        })?;
                    if hash == request.expected_blake3 {
                        self.protocol.delete(&staged).await.map_err(|error| {
                            PublicationFailure {
                                error: role_failure(
                                    stage.final_destination.path(),
                                    Operation::Publish,
                                    error,
                                ),
                                final_destination_changed: false,
                            }
                        })?;
                        self.release_authority(stage);
                        return Ok(PublicationEvidence {
                            final_destination: stage.final_destination.path().clone(),
                            disposition: PublicationDisposition::ExistingEquivalent,
                        });
                    }
                    return Err(publication_failure(stage, FailureClass::Conflict, false));
                }
                Err(error) if error.class == FailureClass::NotFound => {}
                Err(error) => {
                    return Err(PublicationFailure {
                        error: role_failure(
                            stage.final_destination.path(),
                            Operation::Publish,
                            error,
                        ),
                        final_destination_changed: false,
                    });
                }
            }
        }
        if let Err(rename_error) = self.protocol.rename(&staged, &final_path).await {
            return self
                .reconcile_rename_failure(stage, final_path, &request, rename_error)
                .await;
        }
        self.release_authority(stage);
        Ok(PublicationEvidence {
            final_destination: stage.final_destination.path().clone(),
            disposition: PublicationDisposition::Published,
        })
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let native = self.validate(&stage)?;
        match self.protocol.delete(&native).await {
            Ok(())
            | Err(NfsProtocolFailure {
                class: FailureClass::NotFound,
                ..
            }) => {}
            Err(error) => {
                return Err(role_failure(
                    stage.final_destination.path(),
                    Operation::Write,
                    error,
                ));
            }
        }
        self.release_authority(&stage);
        Ok(())
    }
}

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
        || native.starts_with(STAGING_DIR)
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
