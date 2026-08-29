use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    RecoverRequest, RecoveryIdentity, StagedDestination, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
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
    protocol: Arc<dyn NfsStagedProtocol>,
    identity: BackendIdentity,
    owned_stages: Mutex<HashSet<Bytes>>,
}

impl NfsStagedDestinationAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsStagedProtocol>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            owned_stages: Mutex::new(HashSet::new()),
        }
    }

    fn validate(&self, stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                FailureClass::Conflict,
                Transience::Permanent,
            )
        })?;
        let token = std::str::from_utf8(&stage.token).map_err(|_| {
            failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Permanent,
            )
        })?;
        let path = PathBuf::from(token);
        if !path.starts_with(STAGING_DIR) || path.components().count() != 2 {
            return Err(failure(
                stage.final_destination.path(),
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
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
        StoragePath::new(path.to_string_lossy()).map_err(|_| {
            failure(
                stage.final_destination.path(),
                FailureClass::Corruption,
                Transience::Permanent,
            )
        })
    }

    fn release(&self, stage: &PreparedStage) {
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
                    self.release(stage);
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
                self.release(stage);
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
        self.owned_stages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Bytes::copy_from_slice(token.as_bytes()));
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
        Err(failure(
            stage.final_destination.path(),
            FailureClass::Unsupported,
            Transience::Permanent,
        ))
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(failure(
            request.final_destination.path(),
            FailureClass::Unsupported,
            Transience::Permanent,
        ))
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
                        self.release(stage);
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
        self.release(stage);
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
        self.release(&stage);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity};
    use crate::storage::{FinalDestination, SourceDescriptor};
    use futures::stream;

    #[derive(Default)]
    struct FakeProtocol {
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        closes: Arc<std::sync::atomic::AtomicU64>,
        rename_mode: std::sync::atomic::AtomicU8,
    }

    struct FakeFile {
        path: String,
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        closes: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait]
    impl NfsStageFile for FakeFile {
        async fn read_at(
            &mut self,
            offset: u64,
            count: usize,
        ) -> Result<Bytes, NfsProtocolFailure> {
            let files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files
                .get(&self.path)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
            let end = start
                .checked_add(count)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            Ok(Bytes::copy_from_slice(
                value
                    .get(start..end)
                    .ok_or_else(NfsProtocolFailure::protocol)?,
            ))
        }

        async fn write_at(&mut self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files
                .get_mut(&self.path)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
            let end = start
                .checked_add(data.len())
                .ok_or_else(NfsProtocolFailure::protocol)?;
            value.resize(end, 0);
            value[start..end].copy_from_slice(&data);
            Ok(data.len() as u64)
        }

        async fn close(self: Box<Self>) -> Result<(), NfsProtocolFailure> {
            self.closes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl NfsStagedProtocol for FakeProtocol {
        async fn create_empty(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if files.insert(path.as_str().to_owned(), Vec::new()).is_some() {
                return Err(NfsProtocolFailure {
                    class: FailureClass::Conflict,
                    transience: Transience::Permanent,
                });
            }
            Ok(())
        }

        async fn open_read(
            &self,
            path: &StoragePath,
        ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
            self.open_write(path).await
        }

        async fn open_write(
            &self,
            path: &StoragePath,
        ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
            if !self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(path.as_str())
            {
                return Err(NfsProtocolFailure::protocol());
            }
            Ok(Box::new(FakeFile {
                path: path.as_str().to_owned(),
                files: Arc::clone(&self.files),
                closes: Arc::clone(&self.closes),
            }))
        }

        async fn size(&self, path: &StoragePath) -> Result<u64, NfsProtocolFailure> {
            self.files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(path.as_str())
                .map(|value| value.len() as u64)
                .ok_or(NfsProtocolFailure {
                    class: FailureClass::NotFound,
                    transience: Transience::Permanent,
                })
        }

        async fn rename(
            &self,
            from: &StoragePath,
            to: &StoragePath,
        ) -> Result<(), NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files
                .remove(from.as_str())
                .ok_or_else(NfsProtocolFailure::protocol)?;
            match self.rename_mode.load(std::sync::atomic::Ordering::SeqCst) {
                2 => Err(NfsProtocolFailure::protocol()),
                3 => {
                    files.insert(to.as_str().to_owned(), b"wrong".to_vec());
                    Err(NfsProtocolFailure::protocol())
                }
                _ => {
                    files.insert(to.as_str().to_owned(), value);
                    Ok(())
                }
            }
        }

        async fn delete(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            self.files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(path.as_str())
                .map(|_| ())
                .ok_or(NfsProtocolFailure {
                    class: FailureClass::NotFound,
                    transience: Transience::Permanent,
                })
        }
    }

    fn adapter() -> (
        NfsStagedDestinationAdapter,
        Arc<FakeProtocol>,
        BackendIdentity,
    ) {
        let protocol = Arc::new(FakeProtocol::default());
        let identity = BackendIdentity::new(BackendKind::Nfs, "test-nfs")
            .unwrap_or_else(|error| panic!("{error:?}"));
        (
            NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone()),
            protocol,
            identity,
        )
    }

    fn prepare_request(identity: &BackendIdentity) -> PrepareRequest {
        PrepareRequest {
            final_destination: FinalDestination::new(
                StoragePath::new("final.bin").unwrap_or_else(|error| panic!("{error}")),
            ),
            source: SourceDescriptor {
                path: StoragePath::new("source.bin").unwrap_or_else(|error| panic!("{error}")),
                kind: EntryKind::File,
                size: Some(6),
                source_identity: SourceIdentity::new(
                    identity.clone(),
                    IdentityStrength::StableWithinBackend,
                    b"source",
                )
                .unwrap_or_else(|error| panic!("{error}")),
            },
            recovery_binding: [7; 32],
        }
    }

    #[test]
    fn final_and_stage_paths_are_confined() {
        assert!(
            checked_final(&StoragePath::new("file").unwrap_or_else(|error| panic!("{error}")))
                .is_ok()
        );
        assert!(checked_final(&StoragePath::root()).is_err());
        assert!(
            checked_final(&StoragePath::new("../escape").unwrap_or_else(|error| panic!("{error}")))
                .is_err()
        );
        assert!(
            checked_final(
                &StoragePath::new(".data-mover-staging/forged")
                    .unwrap_or_else(|error| panic!("{error}"))
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn staged_lifecycle_writes_verifies_publishes_and_closes_handles() {
        let (adapter, protocol, identity) = adapter();
        let stage = adapter
            .prepare(prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let input: ByteStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]));
        assert_eq!(
            adapter
                .write(&stage, input)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .persisted_bytes,
            6
        );
        assert_eq!(
            adapter
                .observe_checkpoint(&stage)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            6
        );
        let hash = *blake3::hash(b"abcdef").as_bytes();
        assert_eq!(
            adapter
                .verify(
                    &stage,
                    VerifyRequest {
                        expected_size: 6,
                        expected_blake3: hash,
                        cancel: tokio_util::sync::CancellationToken::new()
                    }
                )
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .blake3,
            hash
        );
        let published = adapter
            .publish(
                &stage,
                PublishRequest {
                    policy: ExistingDestinationPolicy::Overwrite,
                    expected_size: 6,
                    expected_blake3: hash,
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(published.disposition, PublicationDisposition::Published);
        assert_eq!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get("final.bin"),
            Some(&b"abcdef".to_vec())
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn recovery_and_precancel_boundaries_fail_without_remote_mutation() {
        let (adapter, protocol, identity) = adapter();
        let stage = adapter
            .prepare(prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(adapter.recovery_identity(&stage).await.is_err());
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        assert!(
            adapter
                .verify(
                    &stage,
                    VerifyRequest {
                        expected_size: 0,
                        expected_blake3: *blake3::hash(b"").as_bytes(),
                        cancel
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 0);
        adapter
            .discard(stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn missing_stage_makes_rename_failure_ambiguous_when_final_is_missing_or_mismatched() {
        for mode in [2, 3] {
            let (adapter, protocol, identity) = adapter();
            let stage = adapter
                .prepare(prepare_request(&identity))
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            protocol
                .rename_mode
                .store(mode, std::sync::atomic::Ordering::SeqCst);
            let result = adapter
                .publish(
                    &stage,
                    PublishRequest {
                        policy: ExistingDestinationPolicy::Overwrite,
                        expected_size: 0,
                        expected_blake3: *blake3::hash(b"").as_bytes(),
                        cancel: tokio_util::sync::CancellationToken::new(),
                    },
                )
                .await;
            let failure = match result {
                Ok(evidence) => panic!("unexpected publication: {evidence:?}"),
                Err(failure) => failure,
            };
            assert!(failure.final_destination_changed);
            adapter
                .discard(stage)
                .await
                .unwrap_or_else(|error| panic!("cleanup authority failed: {error}"));
        }
    }
}
