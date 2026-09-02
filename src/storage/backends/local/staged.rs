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
use tokio::task::JoinSet;

use crate::model::{
    AclEncoding, BackendIdentity, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};
use crate::storage::{
    ByteStream, CheckpointObservation, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

mod checkpoint;
mod probe;
mod publication;
mod recovery;
mod verification;

use probe::WriteProbe;

const STAGING_DIRECTORY: &str = ".data-mover-staging";
/// Maximum size of one positional write submitted by the Local destination.
///
/// This is independent of the Local source's 2 MiB read ceiling. Upstream
/// pieces at or below this limit are submitted whole; larger pieces are split
/// into zero-copy `Bytes` slices by `consume_input`.
const LOCAL_MAX_WRITE_CHUNK_BYTES: usize = 5 * 1024 * 1024;
#[cfg(not(test))]
const LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024 * 1024;
#[cfg(test)]
const LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES: u64 = 4 * 64 * 1024;
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn write_all_at(
    data: &[u8],
    offset: u64,
    mut write_at: impl FnMut(&[u8], u64) -> io::Result<usize>,
) -> io::Result<u64> {
    let mut written = 0usize;
    while written < data.len() {
        let position = offset
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        let count = match write_at(&data[written..], position) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short local write",
            ));
        }
        if count > data.len() - written {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "local write exceeded the submitted buffer",
            ));
        }
        written += count;
    }
    Ok(written as u64)
}

pub(crate) struct LocalStagedDestination {
    #[cfg(test)]
    root: Arc<PathBuf>,
    root_dir: Arc<Dir>,
    identity: BackendIdentity,
    write_concurrency: usize,
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
            let written = write_all_at(&data, offset, |remaining, position| {
                #[cfg(unix)]
                let result = file.write_at(remaining, position);
                #[cfg(windows)]
                let result = file.seek_write(remaining, position);
                result
            })?;
            probe.after_write(offset);
            Ok(written)
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

    async fn open_stage_file_for(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<Arc<std::fs::File>, StorageRoleFailure> {
        let name = self.stage_name(stage, operation)?;
        let staging = self
            .open_staging(operation, stage.final_destination.path())
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
                operation,
                FailureClass::Internal,
            )
        })?;
        result
            .map(Arc::new)
            .map_err(|error| io_failure(stage.final_destination.path(), operation, &error))
    }

    async fn consume_input(
        &self,
        stage: &PreparedStage,
        input: &mut ByteStream,
        file: &Arc<std::fs::File>,
        writes: &mut JoinSet<Result<u64, io::Error>>,
    ) -> (u64, u64, Option<StorageRoleFailure>, bool) {
        let (mut issued, mut persisted) = (stage.write_offset, stage.write_offset);
        let mut durable_prefix = stage.write_offset;
        while let Some(item) = input.next().await {
            let data = match item {
                Ok(data) => data,
                Err(error) => return (issued, persisted, Some(error), false),
            };
            if data.is_empty() {
                continue;
            }
            let mut piece_start = 0usize;
            while piece_start < data.len() {
                let piece_end = piece_start
                    .saturating_add(LOCAL_MAX_WRITE_CHUNK_BYTES)
                    .min(data.len());
                let piece = data.slice(piece_start..piece_end);
                let offset = issued;
                let Some(next_offset) = issued.checked_add(piece.len() as u64) else {
                    return (
                        issued,
                        persisted,
                        Some(failure(
                            stage.final_destination.path(),
                            Operation::Write,
                            FailureClass::InvalidInput,
                        )),
                        false,
                    );
                };
                issued = next_offset;
                writes.spawn(Self::write_piece(
                    Arc::clone(file),
                    Arc::clone(&self.write_probe),
                    offset,
                    piece,
                ));
                piece_start = piece_end;
                if writes.len() >= self.write_concurrency {
                    match Self::settle_one(writes, stage.final_destination.path()).await {
                        Ok(written) => persisted += written,
                        Err(error) => return (issued, persisted, Some(error), false),
                    }
                }
                if stage.recovery_enabled()
                    && issued.saturating_sub(durable_prefix)
                        >= LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES
                {
                    if let Err(error) = self
                        .durable_checkpoint_barrier(stage, file, writes, issued, &mut persisted)
                        .await
                    {
                        return (issued, persisted, Some(error), true);
                    }
                    durable_prefix = issued;
                }
            }
        }
        (issued, persisted, None, false)
    }

    async fn durable_checkpoint_barrier(
        &self,
        stage: &PreparedStage,
        file: &Arc<std::fs::File>,
        writes: &mut JoinSet<Result<u64, io::Error>>,
        issued: u64,
        persisted: &mut u64,
    ) -> Result<(), StorageRoleFailure> {
        let mut first_failure = None;
        Self::drain_writes(
            writes,
            stage.final_destination.path(),
            persisted,
            &mut first_failure,
        )
        .await;
        if let Some(error) = first_failure {
            return Err(error);
        }
        if *persisted != issued {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Corruption,
            ));
        }
        Self::sync_written_file(Arc::clone(file), issued, stage.final_destination.path()).await?;
        self.persist_checkpoint(stage, issued).await
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
            // Recovery and safe publication both need file contents plus the length metadata
            // required to read them. They do not require unrelated inode metadata.
            file.sync_data()
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
        let claim_staging = if stage.recovery_enabled() {
            Some(
                staging
                    .try_clone()
                    .map_err(|error| io_failure(path, operation, &error))?,
            )
        } else {
            None
        };
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
        let Some(claim_staging) = claim_staging else {
            return Ok(());
        };
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

    fn initialize_ephemeral_stage(stage: PreparedStage) -> PreparedStage {
        stage.disable_recovery()
    }

    #[cfg(test)]
    pub(crate) fn fail_checkpoint_at(&self, point: u64) {
        self.write_probe
            .checkpoint_failure
            .store(point, Ordering::SeqCst);
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
        recovery_enabled: bool,
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
        if !recovery_enabled {
            return Ok(Some(Self::initialize_ephemeral_stage(stage)));
        }
        let claim = match self.acquire_claim(&stage, true).await {
            Ok(claim) => claim,
            Err(error) => return self.rollback_prepare(&stage, error).await.map(Some),
        };
        stage.claim = std::sync::Mutex::new(Some(claim));
        self.initialize_stage(stage, file, staging).await.map(Some)
    }

    async fn prepare_mode(
        &self,
        request: PrepareRequest,
        recovery_enabled: bool,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        Self::validate_prepare_request(&request)?;
        let staging = self
            .open_staging(Operation::Prepare, request.final_destination.path())
            .await?;
        let staging = Arc::new(staging);

        let destination_hash = blake3::hash(request.final_destination.path().as_str().as_bytes());
        for _ in 0..32 {
            if let Some(stage) = self
                .create_stage_candidate(
                    &request,
                    Arc::clone(&staging),
                    &destination_hash,
                    recovery_enabled,
                )
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
}

#[async_trait]
impl StagedDestination for LocalStagedDestination {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare_mode(request, true).await
    }

    async fn prepare_ephemeral(
        &self,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare_mode(request, false).await
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        if !stage.recovery_enabled() {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Prepare,
                FailureClass::Unsupported,
            ));
        }
        recovery::export(self, stage).await
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        recovery::recover(self, request).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let file = self.open_stage_file_for(stage, Operation::Write).await?;
        let mut writes = JoinSet::new();
        let (issued, mut persisted, mut first_failure, checkpoint_failed) = self
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
            if checkpoint_failed {
                return Err(error);
            }
            if persisted == issued {
                Self::sync_written_file(Arc::clone(&file), issued, stage.final_destination.path())
                    .await?;
                if stage.recovery_enabled() {
                    self.persist_checkpoint(stage, persisted).await?;
                }
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
        if stage.recovery_enabled() {
            self.persist_checkpoint(stage, persisted).await?;
        }
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        if !stage.recovery_enabled() {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Observe,
                FailureClass::Unsupported,
            ));
        }
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

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Metadata,
                FailureClass::Cancelled,
            ));
        }
        if !local_metadata_supported(&mutation) {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Metadata,
                FailureClass::Unsupported,
            ));
        }
        let file = self.open_stage_file_for(stage, Operation::Metadata).await?;
        let path = stage.final_destination.path().clone();
        tokio::task::spawn_blocking(move || {
            apply_local_metadata(&file, mutation)?;
            file.sync_all()
        })
        .await
        .map_err(|_| failure(&path, Operation::Metadata, FailureClass::Internal))?
        .map_err(|error| io_failure(&path, Operation::Metadata, &error))
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
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
                if !stage.recovery_enabled() {
                    return Ok(PublicationEvidence {
                        final_destination,
                        disposition,
                    });
                }
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
        self.cleanup_stage_artifacts(&stage, Operation::Namespace)
            .await
    }
}

#[cfg(unix)]
fn local_metadata_supported(mutation: &MetadataMutation) -> bool {
    match mutation {
        MetadataMutation::Acl(acl) => acl.encoding() == AclEncoding::Posix,
        MetadataMutation::Xattrs(_) | MetadataMutation::NumericOwnership(_) => true,
        MetadataMutation::Timestamps(value) => value.created.is_none(),
        MetadataMutation::Tags(_) | MetadataMutation::MappedOwnership(_) => false,
    }
}

#[cfg(not(unix))]
fn local_metadata_supported(_mutation: &MetadataMutation) -> bool {
    false
}

#[cfg(unix)]
fn apply_local_metadata(file: &std::fs::File, mutation: MetadataMutation) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::{PermissionsExt as _, fchown};
    use xattr::FileExt as _;

    match mutation {
        MetadataMutation::Acl(acl) => {
            set_optional_xattr(file, "system.posix_acl_access", acl.access())?;
            set_optional_xattr(file, "system.posix_acl_default", acl.default_acl())
        }
        MetadataMutation::Xattrs(values) => {
            for value in values {
                let name = std::ffi::OsStr::from_bytes(value.name());
                file.set_xattr(name, value.value())?;
            }
            Ok(())
        }
        MetadataMutation::NumericOwnership(value) => {
            fchown(file, Some(value.uid), Some(value.gid))?;
            file.set_permissions(std::fs::Permissions::from_mode(value.mode))
        }
        MetadataMutation::Timestamps(value) => {
            let atime = value.accessed.map(local_file_time).transpose()?;
            let mtime = value.modified.map(local_file_time).transpose()?;
            filetime::set_file_handle_times(file, atime, mtime)
        }
        MetadataMutation::Tags(_) | MetadataMutation::MappedOwnership(_) => {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }
}

#[cfg(unix)]
fn set_optional_xattr(file: &std::fs::File, name: &str, value: Option<&[u8]>) -> io::Result<()> {
    use xattr::FileExt as _;

    match value {
        Some(value) => file.set_xattr(name, value),
        None => match file.remove_xattr(name) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        },
    }
}

#[cfg(unix)]
fn local_file_time(value: crate::model::StorageTimestamp) -> io::Result<filetime::FileTime> {
    let nanos = i64::try_from(value.unix_nanos())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    Ok(crate::time_util::nanos_to_filetime_local(nanos))
}

#[cfg(not(unix))]
fn apply_local_metadata(_file: &std::fs::File, _mutation: MetadataMutation) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
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
