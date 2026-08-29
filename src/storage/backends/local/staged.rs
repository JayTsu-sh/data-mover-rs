use std::io;
use std::io::{Read as _, Write as _};
#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::{collections::HashMap, time::Duration};

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
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

const STAGING_DIRECTORY: &str = ".data-mover-staging";
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct WriteProbe {
    #[cfg(test)]
    delays: std::sync::Mutex<HashMap<u64, Duration>>,
    #[cfg(test)]
    completion_order: std::sync::Mutex<Vec<u64>>,
    #[cfg(test)]
    force_out_of_order: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    later_write_started: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    checkpoint_failure: std::sync::atomic::AtomicU64,
}

impl WriteProbe {
    #[cfg_attr(not(test), allow(clippy::unnecessary_wraps, clippy::unused_self))]
    fn fail_checkpoint_at(&self, point: u64) -> io::Result<()> {
        #[cfg(test)]
        if self.checkpoint_failure.load(Ordering::SeqCst) == point {
            return Err(io::Error::other("injected checkpoint failure"));
        }
        #[cfg(not(test))]
        let _ = point;
        Ok(())
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn before_write(&self, offset: u64) {
        #[cfg(test)]
        if offset == 0 && self.force_out_of_order.load(Ordering::SeqCst) {
            while !self.later_write_started.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        } else if offset > 0 {
            self.later_write_started.store(true, Ordering::SeqCst);
        }
        #[cfg(test)]
        if let Some(delay) = self
            .delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&offset)
            .copied()
        {
            std::thread::sleep(delay);
        }
        #[cfg(not(test))]
        let _ = offset;
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn after_write(&self, offset: u64) {
        #[cfg(test)]
        self.completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(offset);
        #[cfg(not(test))]
        let _ = offset;
    }
}

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

    fn checkpoint_record(stage: &PreparedStage, durable_prefix: u64) -> [u8; 80] {
        let mut record = [0_u8; 80];
        record[..8].copy_from_slice(b"DMLSTG01");
        record[8..40].copy_from_slice(
            blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes(),
        );
        record[40..48].copy_from_slice(&durable_prefix.to_le_bytes());
        let checksum = blake3::hash(&record[..48]);
        record[48..].copy_from_slice(checksum.as_bytes());
        record
    }

    async fn persist_checkpoint(
        &self,
        stage: &PreparedStage,
        durable_prefix: u64,
    ) -> Result<(), StorageRoleFailure> {
        let checkpoint = self.checkpoint_name(stage, Operation::Verify)?;
        let mut temporary = checkpoint.clone();
        temporary.push(format!(
            ".tmp-{:016x}",
            STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let record = Self::checkpoint_record(stage, durable_prefix);
        let staging = self
            .open_staging(Operation::Verify, stage.final_destination.path())
            .await?;
        let probe = Arc::clone(&self.write_probe);
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                let mut file = staging.open_with(&temporary, &options)?.into_std();
                file.write_all(&record)?;
                file.sync_all()?;
                probe.fail_checkpoint_at(1)?;
                staging.rename(&temporary, &staging, &checkpoint)?;
                probe.fail_checkpoint_at(2)?;
                staging.open(".")?.sync_all()
            })();
            if result.is_err() {
                let _ = remove_if_present(&staging, &temporary);
            }
            result
        })
        .await
        .map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Internal,
            )
        })?
        .map_err(|error| io_failure(stage.final_destination.path(), Operation::Verify, &error))
    }

    async fn reobserve_checkpoint(&self, stage: &PreparedStage) -> Result<u64, StorageRoleFailure> {
        let stage_name = self.stage_name(stage, Operation::Verify)?;
        let checkpoint_name = self.checkpoint_name(stage, Operation::Verify)?;
        let expected_hash =
            *blake3::hash(stage.final_destination.path().as_str().as_bytes()).as_bytes();
        let staging = self
            .open_staging(Operation::Verify, stage.final_destination.path())
            .await?;
        let (record, stage_len) = tokio::task::spawn_blocking(move || {
            let mut record = Vec::new();
            staging
                .open(checkpoint_name)?
                .into_std()
                .read_to_end(&mut record)?;
            let stage_len = staging.metadata(stage_name)?.len();
            Ok::<_, io::Error>((record, stage_len))
        })
        .await
        .map_err(|_| {
            failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Internal,
            )
        })?
        .map_err(|error| io_failure(stage.final_destination.path(), Operation::Verify, &error))?;
        if record.len() != 80
            || &record[..8] != b"DMLSTG01"
            || record[8..40] != expected_hash
            || record[48..80] != *blake3::hash(&record[..48]).as_bytes()
        {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        let mut durable_bytes = [0_u8; 8];
        durable_bytes.copy_from_slice(&record[40..48]);
        let durable_prefix = u64::from_le_bytes(durable_bytes);
        if durable_prefix > stage_len {
            return Err(failure(
                stage.final_destination.path(),
                Operation::Verify,
                FailureClass::Corruption,
            ));
        }
        Ok(durable_prefix)
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
        let (mut issued, mut persisted) = (0_u64, 0_u64);
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
                Ok(written) if first_failure.is_none() => *persisted += written,
                Err(error) if first_failure.is_none() => *first_failure = Some(error),
                Ok(_) | Err(_) => {}
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
        let path = stage.final_destination.path();
        let staging = self.open_staging(operation, path).await?;
        tokio::task::spawn_blocking(move || {
            let stage_result = remove_if_present(&staging, &stage_name);
            let checkpoint_result = remove_if_present(&staging, &checkpoint_name);
            let sync_result = staging.open(".").and_then(|directory| directory.sync_all());
            stage_result.and(checkpoint_result).and(sync_result)
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
        let stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination.clone(),
            Bytes::from(token),
        );
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
        _request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        Err(failure(
            stage.final_destination.path(),
            Operation::Verify,
            FailureClass::Unsupported,
        ))
    }

    async fn publish(
        &self,
        stage: PreparedStage,
    ) -> Result<PublicationEvidence, StorageRoleFailure> {
        Err(failure(
            stage.final_destination.path(),
            Operation::Publish,
            FailureClass::Unsupported,
        ))
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.cleanup_stage_artifacts(&stage, Operation::Namespace)
            .await
    }
}

fn remove_if_present(directory: &Dir, name: &std::ffi::OsStr) -> io::Result<()> {
    match directory.remove_file(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    let failure = EntryOperationFailure::new(
        path.clone(),
        operation,
        class,
        Transience::Permanent,
        "local staged operation failed",
    )
    .unwrap_or_else(|_| unreachable!("the static diagnostic is valid"));
    StorageRoleFailure::Entry(failure)
}

fn io_failure(path: &StoragePath, operation: Operation, error: &io::Error) -> StorageRoleFailure {
    let class = match error.kind() {
        io::ErrorKind::NotFound => FailureClass::NotFound,
        io::ErrorKind::PermissionDenied => FailureClass::PermissionDenied,
        io::ErrorKind::AlreadyExists => FailureClass::Conflict,
        io::ErrorKind::InvalidInput => FailureClass::InvalidInput,
        _ => FailureClass::Protocol,
    };
    failure(path, operation, class)
}

#[cfg(test)]
mod tests;
