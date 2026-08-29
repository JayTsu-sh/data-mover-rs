use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience,
};
use crate::storage::{
    ByteStream, CheckpointObservation, PrepareRequest, PreparedStage, PublicationEvidence,
    PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity, StagedDestination,
    StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

mod checkpoint;
mod probe;
mod publication;
mod recovery;
mod verification;

use probe::WriteProbe;

const STAGING_DIRECTORY: &str = ".data-mover-staging";
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct LocalStagedDestination {
    #[cfg(test)]
    root: Arc<PathBuf>,
    root_dir: Arc<Dir>,
    identity: BackendIdentity,
    write_concurrency: usize,
    lifecycle: Mutex<()>,
    write_probe: Arc<WriteProbe>,
}

impl LocalStagedDestination {
    fn validate_prepare_request(request: &PrepareRequest) -> Result<(), StorageRoleFailure> {
        if request.source.kind != crate::model::EntryKind::File {
            return Err(failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Unsupported,
            ));
        }
        Self::checked_relative(request.final_destination.path(), Operation::Prepare)?;
        let reserved = Path::new(request.final_destination.path().as_str())
            .components()
            .next()
            .is_some_and(|component| {
                matches!(component, Component::Normal(value) if value == STAGING_DIRECTORY)
            });
        if reserved {
            return Err(failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Conflict,
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        root: impl Into<PathBuf>,
        identity: BackendIdentity,
        write_concurrency: usize,
    ) -> Result<Self, StorageRoleFailure> {
        if write_concurrency == 0 {
            return Err(failure(
                &StoragePath::root(),
                Operation::Prepare,
                FailureClass::InvalidInput,
            ));
        }
        let root = root.into();
        let root_dir = Dir::open_ambient_dir(&root, ambient_authority())
            .map_err(|error| io_failure(&StoragePath::root(), Operation::Prepare, &error))?;
        Ok(Self {
            #[cfg(test)]
            root: Arc::new(root),
            root_dir: Arc::new(root_dir),
            identity,
            write_concurrency,
            lifecycle: Mutex::new(()),
            write_probe: Arc::new(WriteProbe::default()),
        })
    }

    async fn open_staging(
        &self,
        operation: Operation,
        path: &StoragePath,
    ) -> Result<Dir, StorageRoleFailure> {
        let root = Arc::clone(&self.root_dir);
        tokio::task::spawn_blocking(move || {
            root.create_dir_all(STAGING_DIRECTORY)?;
            root.open_dir(STAGING_DIRECTORY)
        })
        .await
        .map_err(|_| failure(path, operation, FailureClass::Internal))?
        .map_err(|error| io_failure(path, operation, &error))
    }

    fn stage_name(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<std::ffi::OsString, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Conflict,
            )
        })?;
        let relative = Self::stage_relative(stage, operation)?;
        relative
            .file_name()
            .map(std::ffi::OsStr::to_os_string)
            .ok_or_else(|| {
                failure(
                    stage.final_destination.path(),
                    operation,
                    FailureClass::Corruption,
                )
            })
    }

    fn checkpoint_name(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<std::ffi::OsString, StorageRoleFailure> {
        let mut name = self.stage_name(stage, operation)?;
        name.push(".checkpoint");
        Ok(name)
    }

    fn claim_name(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<std::ffi::OsString, StorageRoleFailure> {
        let mut name = self.stage_name(stage, operation)?;
        name.push(".claim");
        Ok(name)
    }

    async fn acquire_claim(
        &self,
        stage: &PreparedStage,
        create: bool,
    ) -> Result<std::fs::File, StorageRoleFailure> {
        let name = self.claim_name(stage, Operation::Prepare)?;
        let staging = self
            .open_staging(Operation::Prepare, stage.final_destination.path())
            .await?;
        let path = stage.final_destination.path().clone();
        tokio::task::spawn_blocking(move || {
            let mut options = OpenOptions::new();
            options
                .read(true)
                .write(true)
                .create(create)
                .create_new(create);
            let file = staging.open_with(name, &options)?.into_std();
            file.try_lock()?;
            Ok::<_, io::Error>(file)
        })
        .await
        .map_err(|_| failure(&path, Operation::Prepare, FailureClass::Internal))?
        .map_err(|error| {
            if error.kind() == io::ErrorKind::WouldBlock {
                failure_with_transience(
                    &path,
                    Operation::Prepare,
                    FailureClass::Conflict,
                    Transience::Transient,
                )
            } else {
                io_failure(&path, Operation::Prepare, &error)
            }
        })
    }

    async fn persist_checkpoint(
        &self,
        stage: &PreparedStage,
        durable_prefix: u64,
    ) -> Result<(), StorageRoleFailure> {
        checkpoint::persist(self, stage, durable_prefix).await
    }

    async fn reobserve_checkpoint(&self, stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
        checkpoint::reobserve(self, stage).await
    }

    fn checked_relative(
        path: &StoragePath,
        operation: Operation,
    ) -> Result<PathBuf, StorageRoleFailure> {
        let path_buf = PathBuf::from(path.as_str());
        if path_buf.as_os_str().is_empty()
            || path_buf.is_absolute()
            || path_buf.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(failure(path, operation, FailureClass::InvalidInput));
        }
        Ok(path_buf)
    }

    fn stage_relative(
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<PathBuf, StorageRoleFailure> {
        let encoded = std::str::from_utf8(&stage.token).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Corruption,
            )
        })?;
        let storage_path = StoragePath::new(encoded.to_owned()).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Corruption,
            )
        })?;
        let relative = Self::checked_relative(&storage_path, operation)?;
        let mut components = relative.components();
        let valid = matches!(components.next(), Some(Component::Normal(value)) if value == STAGING_DIRECTORY)
            && matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none();
        if !valid {
            return Err(failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Corruption,
            ));
        }
        Ok(relative)
    }

    #[cfg(test)]
    fn stage_full_path(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<PathBuf, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Conflict,
            )
        })?;
        Ok(self.root.join(Self::stage_relative(stage, operation)?))
    }

    async fn write_piece(
        file: Arc<std::fs::File>,
        probe: Arc<WriteProbe>,
        offset: u64,
        data: Bytes,
    ) -> Result<u64, io::Error> {
        tokio::task::spawn_blocking(move || {
            probe.before_write(offset);
            let mut written = 0usize;
            while written < data.len() {
                let position = offset.checked_add(written as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
                })?;
                #[cfg(unix)]
                let count = file.write_at(&data[written..], position)?;
                #[cfg(windows)]
                let count = file.seek_write(&data[written..], position)?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "short local write",
                    ));
                }
                written += count;
            }
            probe.after_write(offset);
            Ok(data.len() as u64)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn settle_one(
        writes: &mut JoinSet<Result<u64, io::Error>>,
        path: &StoragePath,
    ) -> Result<u64, StorageRoleFailure> {
        match writes.join_next().await {
            Some(Ok(Ok(written))) => Ok(written),
            Some(Ok(Err(error))) => Err(io_failure(path, Operation::Write, &error)),
            Some(Err(_)) | None => Err(failure(path, Operation::Write, FailureClass::Internal)),
        }
    }

    async fn open_stage_file(
        &self,
        stage: &PreparedStage,
    ) -> Result<Arc<std::fs::File>, StorageRoleFailure> {
        let name = self.stage_name(stage, Operation::Write)?;
        let staging = self
            .open_staging(Operation::Write, stage.final_destination.path())
            .await?;
        let result = tokio::task::spawn_blocking(move || {
            let mut options = OpenOptions::new();
            options.read(true).write(true);
            staging
                .open_with(name, &options)
                .map(cap_std::fs::File::into_std)
        })
        .await
        .map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Internal,
            )
        })?;
        result
            .map(Arc::new)
            .map_err(|error| io_failure(stage.final_destination.path(), Operation::Write, &error))
    }

    async fn consume_input(
        &self,
        stage: &PreparedStage,
        input: &mut ByteStream,
        file: &Arc<std::fs::File>,
        writes: &mut JoinSet<Result<u64, io::Error>>,
    ) -> (u64, u64, Option<StorageRoleFailure>) {
        let (mut issued, mut persisted) = (stage.write_offset, stage.write_offset);
        while let Some(item) = input.next().await {
            let data = match item {
                Ok(data) => data,
                Err(error) => return (issued, persisted, Some(error)),
            };
            if data.is_empty() {
                continue;
            }
            let offset = issued;
            let Some(next_offset) = issued.checked_add(data.len() as u64) else {
                return (
                    issued,
                    persisted,
                    Some(failure(
                        stage.final_destination.path(),
                        Operation::Write,
                        FailureClass::InvalidInput,
                    )),
                );
            };
            issued = next_offset;
            writes.spawn(Self::write_piece(
                Arc::clone(file),
                Arc::clone(&self.write_probe),
                offset,
                data,
            ));
            if writes.len() >= self.write_concurrency {
                match Self::settle_one(writes, stage.final_destination.path()).await {
                    Ok(written) => persisted += written,
                    Err(error) => return (issued, persisted, Some(error)),
                }
            }
        }
        (issued, persisted, None)
    }

    async fn drain_writes(
        writes: &mut JoinSet<Result<u64, io::Error>>,
        path: &StoragePath,
        persisted: &mut u64,
        first_failure: &mut Option<StorageRoleFailure>,
    ) {
        while !writes.is_empty() {
            match Self::settle_one(writes, path).await {
                Ok(written) => *persisted += written,
                Err(error) if first_failure.is_none() => *first_failure = Some(error),
                Err(_) => {}
            }
        }
    }

    async fn sync_written_file(
        file: Arc<std::fs::File>,
        issued: u64,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        tokio::task::spawn_blocking(move || {
            file.set_len(issued)?;
            file.sync_all()
        })
        .await
        .map_err(|_| failure(path, Operation::Write, FailureClass::Internal))?
        .map_err(|error| io_failure(path, Operation::Write, &error))
    }

    async fn cleanup_stage_artifacts(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<(), StorageRoleFailure> {
        let stage_name = self.stage_name(stage, operation)?;
        let checkpoint_name = self.checkpoint_name(stage, operation)?;
        let mut guard_name = stage_name.clone();
        guard_name.push(".existing");
        let claim_name = self.claim_name(stage, operation)?;
        let path = stage.final_destination.path();
        let staging = self.open_staging(operation, path).await?;
        let claim_staging = staging
            .try_clone()
            .map_err(|error| io_failure(path, operation, &error))?;
        tokio::task::spawn_blocking(move || {
            let stage_result = publication::remove_if_present(&staging, &stage_name);
            let checkpoint_result = publication::remove_if_present(&staging, &checkpoint_name);
            let guard_result = publication::remove_if_present(&staging, &guard_name);
            let sync_result = staging.open(".").and_then(|directory| directory.sync_all());
            stage_result
                .and(checkpoint_result)
                .and(guard_result)
                .and(sync_result)
        })
        .await
        .map_err(|_| failure(path, operation, FailureClass::Internal))?
        .map_err(|error| io_failure(path, operation, &error))?;

        #[cfg(test)]
        {
            self.write_probe
                .discard_contents_removed
                .store(true, Ordering::SeqCst);
            if self
                .write_probe
                .slow_discard_before_release
                .load(Ordering::SeqCst)
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        stage.release_claim();
        tokio::task::spawn_blocking(move || {
            publication::remove_if_present(&claim_staging, &claim_name)?;
            claim_staging.open(".")?.sync_all()
        })
        .await
        .map_err(|_| failure(path, operation, FailureClass::Internal))?
        .map_err(|error| io_failure(path, operation, &error))
    }

    async fn initialize_stage(
        &self,
        stage: PreparedStage,
        file: std::fs::File,
        staging: Arc<Dir>,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = stage.final_destination.path();
        let sync_result = tokio::task::spawn_blocking(move || {
            file.sync_all()?;
            staging.open(".")?.sync_all()
        })
        .await
        .map_err(|_| failure(path, Operation::Prepare, FailureClass::Internal))?
        .map_err(|error| io_failure(path, Operation::Prepare, &error));
        if let Err(error) = sync_result {
            return self.rollback_prepare(&stage, error).await;
        }
        if let Err(error) = self.persist_checkpoint(&stage, 0).await {
            return self.rollback_prepare(&stage, error).await;
        }
        Ok(stage)
    }

    async fn rollback_prepare(
        &self,
        stage: &PreparedStage,
        original: StorageRoleFailure,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        match self
            .cleanup_stage_artifacts(stage, Operation::Prepare)
            .await
        {
            Ok(()) => Err(original),
            Err(cleanup) => Err(cleanup),
        }
    }

    async fn create_stage_candidate(
        &self,
        request: &PrepareRequest,
        staging: Arc<Dir>,
        destination_hash: &blake3::Hash,
    ) -> Result<Option<PreparedStage>, StorageRoleFailure> {
        let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}-{sequence:016x}.stage", &destination_hash.to_hex()[..16]);
        let open_name = name.clone();
        let staging_for_open = Arc::clone(&staging);
        let result = tokio::task::spawn_blocking(move || {
            let mut options = OpenOptions::new();
            options.create_new(true).read(true).write(true);
            staging_for_open
                .open_with(&open_name, &options)
                .map(cap_std::fs::File::into_std)
        })
        .await
        .map_err(|_| {
            failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Internal,
            )
        })?;
        let file = match result {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
            Err(error) => {
                return Err(io_failure(
                    request.final_destination.path(),
                    Operation::Prepare,
                    &error,
                ));
            }
        };
        let token = PathBuf::from(STAGING_DIRECTORY)
            .join(name)
            .to_string_lossy()
            .into_owned();
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination.clone(),
            Bytes::from(token),
            request.recovery_binding,
            0,
            None,
        );
        let claim = match self.acquire_claim(&stage, true).await {
            Ok(claim) => claim,
            Err(error) => return self.rollback_prepare(&stage, error).await.map(Some),
        };
        stage.claim = std::sync::Mutex::new(Some(claim));
        self.initialize_stage(stage, file, staging).await.map(Some)
    }
}

#[async_trait]
impl StagedDestination for LocalStagedDestination {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Self::validate_prepare_request(&request)?;
        let _lifecycle = self.lifecycle.lock().await;
        let staging = self
            .open_staging(Operation::Prepare, request.final_destination.path())
            .await?;
        let staging = Arc::new(staging);

        let destination_hash = blake3::hash(request.final_destination.path().as_str().as_bytes());
        for _ in 0..32 {
            if let Some(stage) = self
                .create_stage_candidate(&request, Arc::clone(&staging), &destination_hash)
                .await?
            {
                return Ok(stage);
            }
        }
        Err(failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Conflict,
        ))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        recovery::export(self, stage).await
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let _lifecycle = self.lifecycle.lock().await;
        recovery::recover(self, request).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let file = self.open_stage_file(stage).await?;
        let mut writes = JoinSet::new();
        let (issued, mut persisted, mut first_failure) = self
            .consume_input(stage, &mut input, &file, &mut writes)
            .await;
        Self::drain_writes(
            &mut writes,
            stage.final_destination.path(),
            &mut persisted,
            &mut first_failure,
        )
        .await;
        if let Some(error) = first_failure {
            if persisted == issued {
                Self::sync_written_file(Arc::clone(&file), issued, stage.final_destination.path())
                    .await?;
                self.persist_checkpoint(stage, persisted).await?;
            }
            return Err(error);
        }
        if persisted != issued {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Corruption,
            ));
        }
        Self::sync_written_file(file, issued, stage.final_destination.path()).await?;
        self.persist_checkpoint(stage, persisted).await?;
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let durable_prefix = self.reobserve_checkpoint(stage).await?;
        Ok(CheckpointObservation { durable_prefix })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let name = self.stage_name(stage, Operation::Verify)?;
        let staging = self
            .open_staging(Operation::Verify, stage.final_destination.path())
            .await?;
        let path = stage.final_destination.path().clone();
        let probe = Arc::clone(&self.write_probe);
        tokio::task::spawn_blocking(move || {
            verification::verify_local(&staging, &name, &request, &probe)
        })
        .await
        .map_err(|_| failure(&path, Operation::Verify, FailureClass::Internal))?
        .map_err(|error| {
            let class = if error.kind() == io::ErrorKind::Interrupted {
                FailureClass::Cancelled
            } else if error.kind() == io::ErrorKind::InvalidData {
                FailureClass::Corruption
            } else {
                return io_failure(&path, Operation::Verify, &error);
            };
            failure(&path, Operation::Verify, class)
        })
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let _lifecycle = self.lifecycle.lock().await;
        let precommit = |error| PublicationFailure {
            error,
            final_destination_changed: false,
        };
        let stage_name = self
            .stage_name(stage, Operation::Publish)
            .map_err(precommit)?;
        let checkpoint_name = self
            .checkpoint_name(stage, Operation::Publish)
            .map_err(precommit)?;
        let final_relative =
            Self::checked_relative(stage.final_destination.path(), Operation::Publish)
                .map_err(precommit)?;
        let final_destination = stage.final_destination.path().clone();
        let staging = self
            .open_staging(Operation::Publish, &final_destination)
            .await
            .map_err(precommit)?;
        let root = Arc::clone(&self.root_dir);
        let probe = Arc::clone(&self.write_probe);
        let result = tokio::task::spawn_blocking(move || {
            publication::publish_local(
                &root,
                &staging,
                &stage_name,
                &checkpoint_name,
                &final_relative,
                &request,
                &probe,
            )
        })
        .await
        .map_err(|_| PublicationFailure {
            error: failure(
                &final_destination,
                Operation::Publish,
                FailureClass::Internal,
            ),
            final_destination_changed: false,
        })?;
        match result {
            Ok(disposition) => {
                stage.release_claim();
                let claim_name = self
                    .claim_name(stage, Operation::Publish)
                    .map_err(precommit)?;
                let staging = self
                    .open_staging(Operation::Publish, &final_destination)
                    .await
                    .map_err(precommit)?;
                tokio::task::spawn_blocking(move || {
                    publication::remove_if_present(&staging, &claim_name)?;
                    staging.open(".")?.sync_all()
                })
                .await
                .map_err(|_| PublicationFailure {
                    error: failure(
                        &final_destination,
                        Operation::Publish,
                        FailureClass::Internal,
                    ),
                    final_destination_changed: true,
                })?
                .map_err(|error| PublicationFailure {
                    error: io_failure(&final_destination, Operation::Publish, &error),
                    final_destination_changed: true,
                })?;
                Ok(PublicationEvidence {
                    final_destination,
                    disposition,
                })
            }
            Err(error) => Err(PublicationFailure {
                error: io_failure(&final_destination, Operation::Publish, &error.error),
                final_destination_changed: error.committed,
            }),
        }
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let _lifecycle = self.lifecycle.lock().await;
        self.cleanup_stage_artifacts(&stage, Operation::Namespace)
            .await
    }
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    failure_with_transience(path, operation, class, Transience::Permanent)
}

fn failure_with_transience(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    let failure = EntryOperationFailure::new(
        path.clone(),
        operation,
        class,
        transience,
        "local staged operation failed",
    )
    .unwrap_or_else(|_| unreachable!("the static diagnostic is valid"));
    StorageRoleFailure::Entry(failure)
}

fn io_failure(path: &StoragePath, operation: Operation, error: &io::Error) -> StorageRoleFailure {
    let class = match error.kind() {
        io::ErrorKind::NotFound => FailureClass::NotFound,
        io::ErrorKind::PermissionDenied => FailureClass::PermissionDenied,
        io::ErrorKind::AlreadyExists | io::ErrorKind::WouldBlock => FailureClass::Conflict,
        io::ErrorKind::Interrupted => FailureClass::Cancelled,
        io::ErrorKind::InvalidInput => FailureClass::InvalidInput,
        _ => FailureClass::Protocol,
    };
    failure(path, operation, class)
}

#[cfg(test)]
mod tests;
