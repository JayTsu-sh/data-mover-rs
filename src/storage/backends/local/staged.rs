use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use tokio::task::JoinSet;

#[cfg(unix)]
use crate::model::AclEncoding;
use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience,
};
use crate::storage::durability::sync_directory;
use crate::storage::{
    ByteStream, CheckpointObservation, MetadataMutation, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedDestination, StagedMetadataApplicationFailure, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};

mod checkpoint;
mod direct;
mod directory_sync;
mod positioned;
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
const LOCAL_MAX_WRITE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Recovery cadence is a durability policy and must not change when the write-task ceiling is
/// tuned. Files must be strictly larger than this interval to enable deferred recovery.
#[cfg(not(test))]
const LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024 * 1024;
#[cfg(test)]
const LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES: u64 = 4 * 64 * 1024;

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

// Per-attempt state: only the first write to an exclusively created empty stage
// can infer EOF from completed contiguous positional writes. Reuse and recovery
// must still normalize any previous tail.
struct LocalStageState {
    directory: Arc<Dir>,
    file: std::sync::Mutex<Option<Arc<std::fs::File>>>,
    token: Bytes,
    final_path: StoragePath,
    relative: PathBuf,
    name: std::ffi::OsString,
    first_write: AtomicBool,
}

pub(crate) struct LocalStagedDestination {
    #[cfg(test)]
    root: Arc<PathBuf>,
    root_dir: Arc<Dir>,
    identity: BackendIdentity,
    write_concurrency: usize,
    write_probe: Arc<WriteProbe>,
    directory_sync: Arc<directory_sync::DirectorySync>,
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
        let relative =
            Self::checked_relative(request.final_destination.path(), Operation::Prepare)?;
        if relative.components().any(|component| matches!(component, Component::Normal(name) if name.to_str().is_some_and(|name| name.starts_with(".data-mover-")))) {
            return Err(failure(request.final_destination.path(), Operation::Prepare, FailureClass::Conflict));
        }
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
            directory_sync: Arc::new(directory_sync::DirectorySync::default()),
        })
    }

    async fn stage_directory(
        &self,
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<Arc<Dir>, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Conflict,
            )
        })?;
        if let Some(state) = Self::local_state(stage, operation)? {
            return Ok(Arc::clone(&state.directory));
        }
        let relative = Self::stage_relative(stage, operation)?;
        let parent = relative
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        let root = Arc::clone(&self.root_dir);
        tokio::task::spawn_blocking(move || root.open_dir(parent))
            .await
            .map_err(|_| {
                failure(
                    stage.final_destination.path(),
                    operation,
                    FailureClass::Internal,
                )
            })?
            .map(Arc::new)
            .map_err(|error| io_failure(stage.final_destination.path(), operation, &error))
    }

    fn local_state(
        stage: &PreparedStage,
        operation: Operation,
    ) -> Result<Option<&LocalStageState>, StorageRoleFailure> {
        let Some(cached) = &stage.backend_state else {
            return Ok(None);
        };
        let cached = cached
            .downcast_ref::<LocalStageState>()
            .filter(|cached| {
                cached.token == stage.token && &cached.final_path == stage.final_destination.path()
            })
            .ok_or_else(|| {
                failure(
                    stage.final_destination.path(),
                    operation,
                    FailureClass::Corruption,
                )
            })?;
        Ok(Some(cached))
    }

    fn cache_stage(
        stage: &mut PreparedStage,
        directory: Arc<Dir>,
        file: Option<Arc<std::fs::File>>,
        fresh: bool,
    ) -> Result<(), StorageRoleFailure> {
        let relative = Self::stage_relative(stage, Operation::Prepare)?;
        let name = relative
            .file_name()
            .ok_or_else(|| {
                failure(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    FailureClass::Corruption,
                )
            })?
            .to_owned();
        stage.backend_state = Some(Arc::new(LocalStageState {
            directory,
            file: std::sync::Mutex::new(file),
            token: stage.token.clone(),
            final_path: stage.final_destination.path().clone(),
            relative,
            name,
            first_write: AtomicBool::new(fresh),
        }));
        Ok(())
    }

    fn take_fresh_write(stage: &PreparedStage) -> bool {
        stage
            .backend_state
            .as_ref()
            .and_then(|state| state.downcast_ref::<LocalStageState>())
            .is_some_and(|state| state.first_write.swap(false, Ordering::Relaxed))
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
        if let Some(state) = Self::local_state(stage, operation)? {
            return Ok(state.name.clone());
        }
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
        let staging = self.stage_directory(stage, Operation::Prepare).await?;
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
        if let Some(state) = Self::local_state(stage, operation)? {
            return Ok(state.relative.clone());
        }
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
        let final_relative = Self::checked_relative(stage.final_destination.path(), operation)?;
        let colocated = relative.parent() == final_relative.parent()
            && relative
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    crate::storage::artifacts::stage_base(
                        name,
                        stage.final_destination.path().as_str(),
                    ) == Some(name)
                })
            && relative.extension() == Some(std::ffi::OsStr::new("stage"))
            && relative != final_relative;
        if !colocated {
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

    fn write_piece(
        file: &std::fs::File,
        probe: &WriteProbe,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, io::Error> {
        probe.before_write(offset);
        let written = write_all_at(data, offset, |remaining, position| {
            #[cfg(unix)]
            let result = file.write_at(remaining, position);
            #[cfg(windows)]
            let result = file.seek_write(remaining, position);
            result
        })?;
        probe.after_write(offset);
        Ok(written)
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
        stage.validate_owner(&self.identity).map_err(|_| {
            failure(
                stage.final_destination.path(),
                operation,
                FailureClass::Conflict,
            )
        })?;
        if let Some(state) = Self::local_state(stage, operation)? {
            let cached = state
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(unix)]
            if let Some(file) = cached.as_ref() {
                return Ok(Arc::clone(file));
            }
            #[cfg(not(unix))]
            {
                let mut cached = cached;
                if let Some(file) = cached.take() {
                    return Ok(file);
                }
            }
        }
        let name = self.stage_name(stage, operation)?;
        let staging = self.stage_directory(stage, operation).await?;
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
        let mut checkpoint = None;
        let result = self
            .consume_input_chunks(stage, input, file, writes, &mut checkpoint)
            .await;
        // A pending checkpoint owns persistence work: settle it before cleanup or final truncation.
        if let Some(pending) = checkpoint
            && let Err(error) = pending.await
        {
            return (result.0, result.1, Some(error), true);
        }
        result
    }

    async fn consume_input_chunks<'a>(
        &'a self,
        stage: &'a PreparedStage,
        input: &mut ByteStream,
        file: &Arc<std::fs::File>,
        writes: &mut JoinSet<Result<u64, io::Error>>,
        checkpoint: &mut Option<BoxFuture<'a, Result<(), StorageRoleFailure>>>,
    ) -> (u64, u64, Option<StorageRoleFailure>, bool) {
        let (mut issued, mut persisted) = (stage.write_offset, stage.write_offset);
        let interval = stage
            .deferred_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.interval_bytes)
            .or_else(|| {
                stage
                    .recovery_enabled()
                    .then_some(LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES)
            });
        let mut next_checkpoint = interval.and_then(|bytes| stage.write_offset.checked_add(bytes));
        loop {
            let item = if let Some(pending) = checkpoint.as_mut() {
                tokio::select! {
                    biased;
                    result = pending => {
                        *checkpoint = None;
                        if let Err(error) = result {
                            return (issued, persisted, Some(error), true);
                        }
                        continue;
                    }
                    item = input.next() => item,
                }
            } else {
                input.next().await
            };
            let Some(item) = item else {
                break;
            };
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
                let write_file = Arc::clone(file);
                let probe = Arc::clone(&self.write_probe);
                writes
                    .spawn_blocking(move || Self::write_piece(&write_file, &probe, offset, &piece));
                piece_start = piece_end;
                if writes.len() >= self.write_concurrency {
                    match Self::settle_one(writes, stage.final_destination.path()).await {
                        Ok(written) => persisted += written,
                        Err(error) => return (issued, persisted, Some(error), false),
                    }
                }
                if next_checkpoint.is_some_and(|threshold| issued >= threshold)
                    && stage
                        .deferred_checkpoint
                        .as_ref()
                        .is_none_or(|checkpoint| issued < checkpoint.source_size)
                {
                    if let Some(pending) = checkpoint.take()
                        && let Err(error) = pending.await
                    {
                        return (issued, persisted, Some(error), true);
                    }
                    if let Err(error) = self
                        .drain_checkpoint_window(stage, writes, issued, &mut persisted)
                        .await
                    {
                        return (issued, persisted, Some(error), true);
                    }
                    *checkpoint = Some(
                        self.persist_synced_progress(stage, Arc::clone(file), issued)
                            .boxed(),
                    );
                    next_checkpoint = interval.and_then(|bytes| issued.checked_add(bytes));
                }
            }
        }
        (issued, persisted, None, false)
    }

    async fn drain_checkpoint_window(
        &self,
        stage: &PreparedStage,
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
        Ok(())
    }

    async fn persist_synced_progress(
        &self,
        stage: &PreparedStage,
        file: Arc<std::fs::File>,
        prefix: u64,
    ) -> Result<(), StorageRoleFailure> {
        // Subsequent positional writes may be in flight. Never truncate to this older prefix.
        tokio::task::spawn_blocking(move || file.sync_data())
            .await
            .map_err(|_| {
                failure(
                    stage.final_destination.path(),
                    Operation::Write,
                    FailureClass::Internal,
                )
            })?
            .map_err(|error| {
                io_failure(stage.final_destination.path(), Operation::Write, &error)
            })?;
        self.persist_progress(stage, prefix).await
    }

    async fn persist_progress(
        &self,
        stage: &PreparedStage,
        persisted: u64,
    ) -> Result<(), StorageRoleFailure> {
        let first = !stage.recovery_enabled();
        if first {
            let claim = self.acquire_claim(stage, true).await?;
            *stage
                .claim
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(claim);
        }
        self.persist_checkpoint(stage, persisted).await?;
        if first {
            // Once the durable checkpoint exists, failures must preserve recoverable stage state.
            stage.recovery_enabled.store(true, Ordering::Release);
            let checkpoint = stage.deferred_checkpoint.as_ref().ok_or_else(|| {
                failure(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    FailureClass::Internal,
                )
            })?;
            let identity = recovery::export(self, stage).await?;
            checkpoint.registration.register(stage, identity).await?;
        }
        Ok(())
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
        normalize_length: bool,
        durable: bool,
    ) -> Result<(), StorageRoleFailure> {
        tokio::task::spawn_blocking(move || {
            if normalize_length {
                file.set_len(issued)?;
            }
            // Recovery and safe publication both need file contents plus the length metadata
            // required to read them. They do not require unrelated inode metadata.
            if durable {
                file.sync_data()?;
            }
            Ok::<_, io::Error>(())
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
        let staging = self.stage_directory(stage, operation).await?;
        let claim_staging = if stage.recovery_enabled() || stage.deferred_checkpoint.is_some() {
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
            let sync_result = sync_directory(&staging);
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
            sync_directory(&claim_staging)
        })
        .await
        .map_err(|_| failure(path, operation, FailureClass::Internal))?
        .map_err(|error| io_failure(path, operation, &error))
    }

    async fn initialize_stage(
        &self,
        stage: PreparedStage,
        file: Arc<std::fs::File>,
        staging: Arc<Dir>,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = stage.final_destination.path();
        let sync_result = tokio::task::spawn_blocking(move || {
            file.sync_all()?;
            sync_directory(&staging)
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
    pub(crate) fn set_automatic_checkpoint_interval(&self, bytes: u64) {
        self.write_probe
            .automatic_interval
            .store(bytes, Ordering::Relaxed);
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

    fn open_or_create_parent(root: &Dir, parent: &Path) -> io::Result<Dir> {
        match root.open_dir(parent) {
            Ok(directory) => return Ok(directory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let mut directory = root.try_clone()?;
        for component in parent.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            let next = match directory.open_dir(name) {
                Ok(next) => next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match directory.create_dir(name) {
                        Ok(()) => sync_directory(&directory)?,
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error),
                    }
                    directory.open_dir(name)?
                }
                Err(error) => return Err(error),
            };
            directory = next;
        }
        Ok(directory)
    }

    async fn prepare_mode(
        &self,
        request: PrepareRequest,
        recovery_enabled: bool,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        Self::validate_prepare_request(&request)?;
        let root = Arc::clone(&self.root_dir);
        let destination_path = request.final_destination.path().as_str().to_owned();
        let relative =
            Self::checked_relative(request.final_destination.path(), Operation::Prepare)?;
        let parent = relative
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        let token_parent = relative
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .to_owned();
        // Directory resolution and exclusive file creation are one blocking operation.
        let (staging, file, name) = tokio::task::spawn_blocking(move || {
            let staging = Self::open_or_create_parent(&root, &parent)?;
            let mut options = OpenOptions::new();
            options.create_new(true).read(true).write(true);
            for _ in 0..32 {
                let name = crate::storage::artifacts::stage_name(&destination_path);
                match staging.open_with(&name, &options) {
                    Ok(file) => return Ok((Arc::new(staging), file.into_std(), name)),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
        })
        .await
        .map_err(|_| {
            failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Internal,
            )
        })?
        .map_err(|error| {
            io_failure(request.final_destination.path(), Operation::Prepare, &error)
        })?;
        let token = token_parent.join(name).to_string_lossy().into_owned();
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            Bytes::from(token),
            request.recovery_binding,
            0,
            None,
        );
        let file = Arc::new(file);
        Self::cache_stage(
            &mut stage,
            Arc::clone(&staging),
            Some(Arc::clone(&file)),
            true,
        )?;
        if !recovery_enabled {
            return Ok(Self::initialize_ephemeral_stage(stage));
        }
        let claim = match self.acquire_claim(&stage, true).await {
            Ok(claim) => claim,
            Err(error) => return self.rollback_prepare(&stage, error).await,
        };
        stage.claim = std::sync::Mutex::new(Some(claim));
        self.initialize_stage(stage, file, staging).await
    }
}

#[async_trait]
impl StagedDestination for LocalStagedDestination {
    fn supports_direct(&self) -> bool {
        cfg!(unix)
    }

    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.open_direct(request, cancel).await
    }
    fn copied_metadata_target(&self) -> Option<crate::storage::CopiedMetadataTarget> {
        #[cfg(unix)]
        {
            Some(crate::storage::CopiedMetadataTarget {
                timestamp_precision: crate::model::TimePrecision::Nanoseconds,
                ownership: crate::storage::CopiedOwnershipTarget::Numeric,
                acl: crate::metadata::AclTarget::Encoding(crate::model::AclEncoding::Posix),
                xattrs: crate::metadata::ValueTarget::Supported,
            })
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        #[cfg(test)]
        if self.write_probe.automatic_interval.load(Ordering::Relaxed) != 0 {
            return Some(self.write_probe.automatic_interval.load(Ordering::Relaxed));
        }
        Some(crate::storage::backends::DEFAULT_CHECKPOINT_INTERVAL_BYTES)
    }

    async fn write_single(
        &self,
        stage: &PreparedStage,
        data: Bytes,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        if data.len() > LOCAL_MAX_WRITE_CHUNK_BYTES || stage.recovery_enabled() {
            return self
                .write(
                    stage,
                    Box::pin(futures::stream::once(async move { Ok(data) })),
                )
                .await;
        }
        let file = self.open_stage_file_for(stage, Operation::Write).await?;
        let fresh = Self::take_fresh_write(stage);
        let probe = Arc::clone(&self.write_probe);
        let durable = stage.durable_publication;
        let direct = stage.direct;
        let written = tokio::task::spawn_blocking(move || {
            if direct {
                file.set_len(0)?;
            }
            let written = Self::write_piece(&file, &probe, 0, &data)?;
            if !fresh {
                file.set_len(written)?;
            }
            if durable {
                #[cfg(test)]
                probe.final_data_sync_calls.fetch_add(1, Ordering::SeqCst);
                file.sync_data()?;
            }
            Ok::<_, io::Error>(written)
        })
        .await
        .map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Internal,
            )
        })?
        .map_err(|error| io_failure(stage.final_destination.path(), Operation::Write, &error))?;
        Ok(WriteEvidence {
            persisted_bytes: written,
        })
    }

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

    fn supports_positioned_write(&self) -> bool {
        true
    }

    async fn write_positioned(
        &self,
        stage: &PreparedStage,
        input: crate::storage::PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        positioned::write(self, stage, input).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let file = self.open_stage_file_for(stage, Operation::Write).await?;
        let fresh = Self::take_fresh_write(stage);
        if stage.direct {
            let target = Arc::clone(&file);
            tokio::task::spawn_blocking(move || target.set_len(0))
                .await
                .map_err(|_| {
                    failure(
                        stage.final_destination.path(),
                        Operation::Write,
                        FailureClass::Internal,
                    )
                })?
                .map_err(|e| io_failure(stage.final_destination.path(), Operation::Write, &e))?;
        }
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
                Self::sync_written_file(
                    Arc::clone(&file),
                    issued,
                    stage.final_destination.path(),
                    true,
                    stage.durable_publication,
                )
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
        if stage.durable_publication || !fresh {
            #[cfg(test)]
            if stage.durable_publication {
                self.write_probe
                    .final_data_sync_calls
                    .fetch_add(1, Ordering::SeqCst);
            }
            Self::sync_written_file(
                file,
                issued,
                stage.final_destination.path(),
                !fresh,
                stage.durable_publication,
            )
            .await?;
        }
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
        let staging = self.stage_directory(stage, Operation::Verify).await?;
        let path = stage.final_destination.path().clone();
        let direct_file = if stage.direct {
            Some(self.open_stage_file_for(stage, Operation::Verify).await?)
        } else {
            None
        };
        let probe = Arc::clone(&self.write_probe);
        tokio::task::spawn_blocking(move || {
            if let Some(file) = direct_file {
                use std::io::Seek as _;
                let mut file = file.try_clone()?;
                file.rewind()?;
                return verification::verify_file(file, &request, &probe);
            }
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
        let durable = stage.durable_publication;
        tokio::task::spawn_blocking(move || {
            apply_local_metadata(&file, mutation)?;
            if durable {
                file.sync_all()?;
            }
            Ok(())
        })
        .await
        .map_err(|_| failure(&path, Operation::Metadata, FailureClass::Internal))?
        .map_err(|error| io_failure(&path, Operation::Metadata, &error))
    }

    async fn apply_metadata_batch(
        &self,
        stage: &PreparedStage,
        mutations: Vec<MetadataMutation>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StagedMetadataApplicationFailure> {
        if mutations.is_empty() {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err(StagedMetadataApplicationFailure {
                failed_index: 0,
                completed: 0,
                error: None,
            });
        }
        if let Some(index) = mutations
            .iter()
            .position(|mutation| !local_metadata_supported(mutation))
        {
            return Err(StagedMetadataApplicationFailure {
                failed_index: index,
                completed: 0,
                error: Some(failure(
                    stage.final_destination.path(),
                    Operation::Metadata,
                    FailureClass::Unsupported,
                )),
            });
        }
        let file = self
            .open_stage_file_for(stage, Operation::Metadata)
            .await
            .map_err(|error| StagedMetadataApplicationFailure {
                failed_index: 0,
                completed: 0,
                error: Some(error),
            })?;
        let path = stage.final_destination.path().clone();
        let durable = stage.durable_publication;
        let last_index = mutations.len().saturating_sub(1);
        #[cfg(test)]
        self.write_probe
            .metadata_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        if durable {
            self.write_probe
                .metadata_sync_calls
                .fetch_add(1, Ordering::SeqCst);
        }
        let result = tokio::task::spawn_blocking(move || {
            for (index, mutation) in mutations.into_iter().enumerate() {
                if cancel.is_cancelled() {
                    return Err((index, None));
                }
                if let Err(error) = apply_local_metadata(&file, mutation) {
                    return Err((index, Some(error)));
                }
            }
            if durable && let Err(error) = file.sync_all() {
                return Err((last_index, Some(error)));
            }
            Ok(())
        })
        .await
        .map_err(|_| StagedMetadataApplicationFailure {
            failed_index: 0,
            completed: 0,
            error: Some(failure(&path, Operation::Metadata, FailureClass::Internal)),
        })?;
        result.map_err(|(completed, error)| StagedMetadataApplicationFailure {
            failed_index: completed,
            completed,
            error: error.map(|error| io_failure(&path, Operation::Metadata, &error)),
        })
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        if stage.direct {
            return self.finish_direct(stage, request).await;
        }
        let precommit = |error| PublicationFailure {
            error,
            final_destination_changed: false,
        };
        let stage_name = self
            .stage_name(stage, Operation::Publish)
            .map_err(precommit)?;
        let checkpoint_name = stage
            .recovery_enabled()
            .then(|| self.checkpoint_name(stage, Operation::Publish))
            .transpose()
            .map_err(precommit)?;
        let final_relative =
            Self::checked_relative(stage.final_destination.path(), Operation::Publish)
                .map_err(precommit)?;
        let final_destination = stage.final_destination.path().clone();
        let staging = self
            .stage_directory(stage, Operation::Publish)
            .await
            .map_err(precommit)?;
        let root = Arc::clone(&self.root_dir);
        let probe = Arc::clone(&self.write_probe);
        let colocated = Self::stage_relative(stage, Operation::Publish)
            .map_err(precommit)?
            .parent()
            == final_relative.parent();
        let directory_sync = Arc::clone(&self.directory_sync);
        let durable = stage.durable_publication;
        let result = tokio::task::spawn_blocking(move || {
            publication::publish_local(
                publication::Directories {
                    root: &root,
                    staging: &staging,
                    sync: &directory_sync,
                    colocated,
                    durable,
                },
                &stage_name,
                checkpoint_name.as_deref(),
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
                    .stage_directory(stage, Operation::Publish)
                    .await
                    .map_err(precommit)?;
                tokio::task::spawn_blocking(move || {
                    publication::remove_if_present(&staging, &claim_name)?;
                    sync_directory(&staging)
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
        if stage.direct {
            return Ok(());
        }
        self.cleanup_stage_artifacts(&stage, Operation::Namespace)
            .await
    }
}

#[cfg(unix)]
pub(super) fn local_metadata_supported(mutation: &MetadataMutation) -> bool {
    match mutation {
        MetadataMutation::Acl(acl) => acl.encoding() == AclEncoding::Posix,
        MetadataMutation::Xattrs(_)
        | MetadataMutation::NumericOwnership(_)
        | MetadataMutation::Mode(_) => true,
        MetadataMutation::Timestamps(value) => value.created.is_none(),
        MetadataMutation::Tags(_) | MetadataMutation::MappedOwnership(_) => false,
    }
}

#[cfg(not(unix))]
pub(super) fn local_metadata_supported(_mutation: &MetadataMutation) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn apply_local_metadata(
    file: &std::fs::File,
    mutation: MetadataMutation,
) -> io::Result<()> {
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
        MetadataMutation::Mode(mode) => {
            file.set_permissions(std::fs::Permissions::from_mode(mode & 0o7777))
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
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    let nanos = i64::try_from(value.unix_nanos())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    Ok(filetime::FileTime::from_unix_time(
        nanos.div_euclid(NANOS_PER_SECOND),
        u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND))
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
    ))
}

#[cfg(not(unix))]
pub(super) fn apply_local_metadata(
    _file: &std::fs::File,
    _mutation: MetadataMutation,
) -> io::Result<()> {
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
