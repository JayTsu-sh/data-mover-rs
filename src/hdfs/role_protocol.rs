use std::ops::Range as RoleRange;

use bytes::Bytes as RoleBytes;
use tokio::sync::mpsc as role_mpsc;

use crate::DataChunk as RoleDataChunk;
use crate::error::HdfsErrorKind as RoleHdfsErrorKind;
use crate::model::{
    EntryKind as RoleEntryKind, FailureClass as RoleFailureClass, Operation as RoleOperation,
    StoragePath as RoleStoragePath, Transience as RoleTransience,
};
use crate::storage::backends::hdfs::protocol::{
    HdfsEntryFacts as RoleHdfsEntryFacts, HdfsProtocol as RoleHdfsProtocol,
    entry_failure as role_entry_failure, session_failure as role_session_failure,
};
use crate::storage::{ByteStream as RoleByteStream, StorageRoleFailure as RoleFailure};

const HDFS_ROLE_MAX_CHUNK: usize = 1024 * 1024;

#[async_trait::async_trait]
impl RoleHdfsProtocol for HDFSStorage {
    async fn stat(&self, path: &RoleStoragePath) -> Result<RoleHdfsEntryFacts, RoleFailure> {
        let entry = self
            .get_metadata(Path::new(path.as_str()))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Observe, error))?;
        Ok(hdfs_role_facts(path.clone(), entry))
    }

    async fn list(&self, path: &RoleStoragePath) -> Result<Vec<RoleHdfsEntryFacts>, RoleFailure> {
        self.list_directory(Path::new(path.as_str()))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Traverse, error))?
            .into_iter()
            .map(|entry| {
                let entry_path = RoleStoragePath::new(entry.relative_path.to_string_lossy())
                    .map_err(|_| {
                        hdfs_role_entry(
                            path,
                            RoleOperation::Traverse,
                            RoleFailureClass::Protocol,
                            RoleTransience::Permanent,
                        )
                    })?;
                Ok(hdfs_role_facts(entry_path, entry))
            })
            .collect()
    }

    async fn read_range(
        &self,
        path: &RoleStoragePath,
        range: RoleRange<u64>,
    ) -> Result<RoleBytes, RoleFailure> {
        let native = Path::new(path.as_str());
        let file = self
            .open_file(native)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Read, error))?;
        self.read_at(&file, range.start, range.end.saturating_sub(range.start))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Read, error))
    }

    async fn create_directory(&self, path: &RoleStoragePath) -> Result<(), RoleFailure> {
        self.create_dir_all(Path::new(path.as_str()), 0o755)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Namespace, error))
    }

    async fn delete(&self, path: &RoleStoragePath, kind: RoleEntryKind) -> Result<(), RoleFailure> {
        let result = match kind {
            RoleEntryKind::File => self.delete_file(Path::new(path.as_str())).await,
            RoleEntryKind::Directory => self.delete_dir_all(Path::new(path.as_str())).await,
            _ => {
                return Err(hdfs_role_entry(
                    path,
                    RoleOperation::Namespace,
                    RoleFailureClass::Unsupported,
                    RoleTransience::Permanent,
                ));
            }
        };
        match result {
            Ok(()) | Err(StorageError::FileNotFound(_)) => Ok(()),
            Err(error) => Err(hdfs_role_error(path, RoleOperation::Namespace, error)),
        }
    }

    async fn rename(
        &self,
        from: &RoleStoragePath,
        to: &RoleStoragePath,
        overwrite: bool,
    ) -> Result<(), RoleFailure> {
        self.rename_with_overwrite(Path::new(from.as_str()), Path::new(to.as_str()), overwrite)
            .await
            .map_err(|error| hdfs_role_error(from, RoleOperation::Publish, error))
    }

    async fn claim_stage(
        &self,
        from: &RoleStoragePath,
        claimed: &RoleStoragePath,
    ) -> Result<(), RoleFailure> {
        self.rename_with_overwrite(Path::new(from.as_str()), Path::new(claimed.as_str()), false)
            .await
            .map_err(|error| hdfs_role_error(from, RoleOperation::Prepare, error))
    }

    async fn create_empty_stage_exclusive(
        &self,
        path: &RoleStoragePath,
    ) -> Result<(), RoleFailure> {
        let native = Path::new(path.as_str());
        if let Some(parent) = native.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent, 0o755)
                .await
                .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
        }
        let resolved = self
            .resolve_path(native)
            .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
        let options = hdfs_native::WriteOptions::default()
            .block_size(self.block_size)
            .permission(0o644)
            .overwrite(false);
        let mut writer = self
            .client
            .create(&resolved, options)
            .await
            .map_err(|error| {
                hdfs_role_error(
                    path,
                    RoleOperation::Prepare,
                    hdfs_operation_error("create exclusive stage", Some(native), &error),
                )
            })?;
        Box::pin(writer.close()).await.map_err(|error| {
            hdfs_role_error(
                path,
                RoleOperation::Prepare,
                hdfs_operation_error("close exclusive stage", Some(native), &error),
            )
        })?;
        validate_empty_stage(self, path, native).await
    }

    async fn append_stage(
        &self,
        path: &RoleStoragePath,
        start_offset: u64,
        expected_size: u64,
        input: RoleByteStream,
    ) -> Result<u64, RoleFailure> {
        let capacity = self.transfer_concurrency().write().max(1);
        let (sender, receiver) = role_mpsc::channel(capacity);
        let feed = feed_stage_chunks(path.clone(), start_offset, input, sender);
        let append = self.append_stream(
            receiver,
            Path::new(path.as_str()),
            start_offset,
            expected_size,
        );
        let (feed_result, append_result) = tokio::join!(feed, append);
        resolve_stage_append(
            feed_result,
            append_result.map_err(|error| hdfs_role_error(path, RoleOperation::Write, error)),
        )
    }

    async fn set_mapped_ownership(
        &self,
        path: &RoleStoragePath,
        owner: &str,
        group: &str,
        mode: u32,
    ) -> Result<(), RoleFailure> {
        let native = Path::new(path.as_str());
        self.set_owner_group(native, Some(owner), Some(group))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))?;
        self.set_permission(native, mode)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))
    }

    async fn set_timestamps(
        &self,
        path: &RoleStoragePath,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> Result<(), RoleFailure> {
        self.set_metadata(Path::new(path.as_str()), atime, mtime, None)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))
    }
}

async fn validate_empty_stage(
    storage: &HDFSStorage,
    path: &RoleStoragePath,
    native: &Path,
) -> Result<(), RoleFailure> {
    let metadata = storage
        .get_metadata(native)
        .await
        .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
    if metadata.is_dir || metadata.size != 0 {
        return Err(hdfs_role_entry(
            path,
            RoleOperation::Prepare,
            RoleFailureClass::Corruption,
            RoleTransience::Permanent,
        ));
    }
    Ok(())
}

async fn feed_stage_chunks(
    path: RoleStoragePath,
    start_offset: u64,
    mut input: RoleByteStream,
    sender: role_mpsc::Sender<RoleDataChunk>,
) -> Result<StageFeedOutcome, RoleFailure> {
    let mut offset = start_offset;
    while let Some(value) = input.next().await {
        let data = value?;
        let length = checked_stage_chunk_length(&path, &data)?;
        if sender.send(RoleDataChunk { offset, data }).await.is_err() {
            return Ok(StageFeedOutcome::ReceiverClosed);
        }
        offset = offset
            .checked_add(length)
            .ok_or_else(|| invalid_stage_chunk(&path))?;
    }
    Ok(StageFeedOutcome::Complete)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StageFeedOutcome {
    Complete,
    ReceiverClosed,
}

fn resolve_stage_append(
    feed: Result<StageFeedOutcome, RoleFailure>,
    append: Result<u64, RoleFailure>,
) -> Result<u64, RoleFailure> {
    match feed {
        Err(input_error) => Err(input_error),
        Ok(StageFeedOutcome::Complete | StageFeedOutcome::ReceiverClosed) => append,
    }
}

#[cfg(test)]
mod stage_bridge_tests {
    use super::*;

    fn path() -> RoleStoragePath {
        RoleStoragePath::new("partial").unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn receiver_closure_preserves_real_destination_failure() {
        let path = path();
        for class in [RoleFailureClass::Conflict, RoleFailureClass::Connectivity] {
            let destination = hdfs_role_entry(
                &path,
                RoleOperation::Write,
                class,
                RoleTransience::Transient,
            );
            let result =
                resolve_stage_append(Ok(StageFeedOutcome::ReceiverClosed), Err(destination));
            assert!(matches!(result, Err(RoleFailure::Entry(error))
                if error.operation() == RoleOperation::Write && error.class() == class));
        }
    }

    #[test]
    fn real_input_cancellation_wins_over_destination_failure() {
        let path = path();
        let input = hdfs_role_entry(
            &path,
            RoleOperation::Read,
            RoleFailureClass::Cancelled,
            RoleTransience::Transient,
        );
        let destination = hdfs_role_entry(
            &path,
            RoleOperation::Write,
            RoleFailureClass::Connectivity,
            RoleTransience::Transient,
        );

        let result = resolve_stage_append(Err(input), Err(destination));

        assert!(matches!(result, Err(RoleFailure::Entry(error))
            if error.operation() == RoleOperation::Read
                && error.class() == RoleFailureClass::Cancelled));
    }
}

fn checked_stage_chunk_length(
    path: &RoleStoragePath,
    data: &RoleBytes,
) -> Result<u64, RoleFailure> {
    if data.len() > HDFS_ROLE_MAX_CHUNK {
        return Err(invalid_stage_chunk(path));
    }
    u64::try_from(data.len()).map_err(|_| invalid_stage_chunk(path))
}

fn invalid_stage_chunk(path: &RoleStoragePath) -> RoleFailure {
    hdfs_role_entry(
        path,
        RoleOperation::Write,
        RoleFailureClass::InvalidInput,
        RoleTransience::Permanent,
    )
}

fn hdfs_role_facts(path: RoleStoragePath, entry: crate::HDFSEntry) -> RoleHdfsEntryFacts {
    RoleHdfsEntryFacts {
        path,
        kind: if entry.is_dir {
            RoleEntryKind::Directory
        } else {
            RoleEntryKind::File
        },
        size: (!entry.is_dir).then_some(entry.size),
        atime: entry.atime,
        mtime: entry.mtime,
        mode: entry.mode,
        owner: entry.owner,
        group: entry.group,
        replication: entry.replication,
        block_size: entry.block_size,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn hdfs_role_error(
    path: &RoleStoragePath,
    operation: RoleOperation,
    error: StorageError,
) -> RoleFailure {
    let (class, transience, session) = hdfs_role_classify(&error);
    if session {
        role_session_failure(operation, class, transience)
    } else {
        hdfs_role_entry(path, operation, class, transience)
    }
}

fn hdfs_role_entry(
    path: &RoleStoragePath,
    operation: RoleOperation,
    class: RoleFailureClass,
    transience: RoleTransience,
) -> RoleFailure {
    role_entry_failure(path, operation, class, transience)
}

fn hdfs_role_classify(error: &StorageError) -> (RoleFailureClass, RoleTransience, bool) {
    match error {
        StorageError::Cancelled => (
            RoleFailureClass::Cancelled,
            RoleTransience::Transient,
            false,
        ),
        StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_) => {
            (RoleFailureClass::NotFound, RoleTransience::Permanent, false)
        }
        StorageError::PermissionDenied(_) => (
            RoleFailureClass::PermissionDenied,
            RoleTransience::Permanent,
            false,
        ),
        StorageError::InsufficientSpace(_) => {
            (RoleFailureClass::Capacity, RoleTransience::Permanent, false)
        }
        StorageError::InvalidPath(_) | StorageError::ConfigError(_) => (
            RoleFailureClass::InvalidInput,
            RoleTransience::Permanent,
            false,
        ),
        StorageError::HdfsOperation(details) => {
            let transience = if details.retryable {
                RoleTransience::Transient
            } else {
                RoleTransience::Permanent
            };
            match details.kind {
                RoleHdfsErrorKind::Authentication => {
                    (RoleFailureClass::Authentication, transience, true)
                }
                RoleHdfsErrorKind::AlreadyExists => (RoleFailureClass::Conflict, transience, false),
                RoleHdfsErrorKind::Unsupported => {
                    (RoleFailureClass::Unsupported, transience, false)
                }
                RoleHdfsErrorKind::Io | RoleHdfsErrorKind::Rpc => {
                    (RoleFailureClass::Connectivity, transience, true)
                }
                RoleHdfsErrorKind::BlocksMissing | RoleHdfsErrorKind::DataTransfer => {
                    (RoleFailureClass::Corruption, transience, false)
                }
                _ => (RoleFailureClass::Protocol, transience, false),
            }
        }
        _ => (RoleFailureClass::Protocol, RoleTransience::Unknown, false),
    }
}
