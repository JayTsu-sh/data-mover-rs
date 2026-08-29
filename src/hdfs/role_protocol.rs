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
    async fn stat(
        &self,
        path: &RoleStoragePath,
    ) -> Result<RoleHdfsEntryFacts, RoleFailure> {
        let entry = self.get_metadata(Path::new(path.as_str())).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Observe, error))?;
        Ok(hdfs_role_facts(path.clone(), entry))
    }

    async fn list(
        &self,
        path: &RoleStoragePath,
    ) -> Result<Vec<RoleHdfsEntryFacts>, RoleFailure> {
        self.list_directory(Path::new(path.as_str())).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Traverse, error))?
            .into_iter().map(|entry| {
                let entry_path = RoleStoragePath::new(entry.relative_path.to_string_lossy())
                    .map_err(|_| hdfs_role_entry(path, RoleOperation::Traverse,
                        RoleFailureClass::Protocol, RoleTransience::Permanent))?;
                Ok(hdfs_role_facts(entry_path, entry))
            }).collect()
    }

    async fn read_range(
        &self,
        path: &RoleStoragePath,
        range: RoleRange<u64>,
    ) -> Result<RoleBytes, RoleFailure> {
        let native = Path::new(path.as_str());
        let file = self.open_file(native).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Read, error))?;
        self.read_at(&file, range.start, range.end.saturating_sub(range.start)).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Read, error))
    }

    async fn create_directory(&self, path: &RoleStoragePath) -> Result<(), RoleFailure> {
        self.create_dir_all(Path::new(path.as_str()), 0o755).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Namespace, error))
    }

    async fn delete(&self, path: &RoleStoragePath, kind: RoleEntryKind) -> Result<(), RoleFailure> {
        let result = match kind {
            RoleEntryKind::File => self.delete_file(Path::new(path.as_str())).await,
            RoleEntryKind::Directory => self.delete_dir_all(Path::new(path.as_str())).await,
            _ => return Err(hdfs_role_entry(path, RoleOperation::Namespace,
                RoleFailureClass::Unsupported, RoleTransience::Permanent)),
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
            .await.map_err(|error| hdfs_role_error(from, RoleOperation::Publish, error))
    }

    async fn prepare_stage(
        &self,
        path: &RoleStoragePath,
        expected_size: u64,
    ) -> Result<(), RoleFailure> {
        self.prepare_tail_transfer(Path::new(path.as_str()), expected_size, false, 0o644, None)
            .await.map(|_| ())
            .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))
    }

    async fn write_stage(
        &self,
        path: &RoleStoragePath,
        expected_size: u64,
        mut input: RoleByteStream,
    ) -> Result<u64, RoleFailure> {
        let capacity = self.transfer_concurrency().write().max(1);
        let (sender, receiver) = role_mpsc::channel(capacity);
        let feed = async move {
            let mut offset = 0;
            while let Some(value) = input.next().await {
                let data = value?;
                if data.len() > HDFS_ROLE_MAX_CHUNK {
                    return Err(hdfs_role_entry(path, RoleOperation::Write,
                        RoleFailureClass::InvalidInput, RoleTransience::Permanent));
                }
                let length = u64::try_from(data.len()).map_err(|_| hdfs_role_entry(path,
                    RoleOperation::Write, RoleFailureClass::InvalidInput,
                    RoleTransience::Permanent))?;
                sender.send(RoleDataChunk { offset, data }).await.map_err(|_| hdfs_role_entry(
                    path, RoleOperation::Write, RoleFailureClass::Cancelled,
                    RoleTransience::Transient))?;
                offset += length;
            }
            Ok::<(), RoleFailure>(())
        };
        let write = self.write_stream(receiver, Path::new(path.as_str()), expected_size, 0o644, None);
        let (feed_result, write_result) = tokio::join!(feed, write);
        feed_result?;
        write_result.map_err(|error| hdfs_role_error(path, RoleOperation::Write, error))
    }

    async fn set_mapped_ownership(
        &self,
        path: &RoleStoragePath,
        owner: &str,
        group: &str,
        mode: u32,
    ) -> Result<(), RoleFailure> {
        let native = Path::new(path.as_str());
        self.set_owner_group(native, Some(owner), Some(group)).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))?;
        self.set_permission(native, mode).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))
    }

    async fn set_timestamps(
        &self,
        path: &RoleStoragePath,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> Result<(), RoleFailure> {
        self.set_metadata(Path::new(path.as_str()), atime, mtime, None).await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Metadata, error))
    }
}

fn hdfs_role_facts(
    path: RoleStoragePath,
    entry: crate::HDFSEntry,
) -> RoleHdfsEntryFacts {
    RoleHdfsEntryFacts {
        path,
        kind: if entry.is_dir { RoleEntryKind::Directory } else { RoleEntryKind::File },
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

fn hdfs_role_classify(error: &StorageError)
-> (RoleFailureClass, RoleTransience, bool) {
    match error {
        StorageError::Cancelled => (RoleFailureClass::Cancelled, RoleTransience::Transient, false),
        StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_) =>
            (RoleFailureClass::NotFound, RoleTransience::Permanent, false),
        StorageError::PermissionDenied(_) =>
            (RoleFailureClass::PermissionDenied, RoleTransience::Permanent, false),
        StorageError::InsufficientSpace(_) =>
            (RoleFailureClass::Capacity, RoleTransience::Permanent, false),
        StorageError::InvalidPath(_) | StorageError::ConfigError(_) =>
            (RoleFailureClass::InvalidInput, RoleTransience::Permanent, false),
        StorageError::HdfsOperation(details) => {
            let transience = if details.retryable {
                RoleTransience::Transient
            } else {
                RoleTransience::Permanent
            };
            match details.kind {
                RoleHdfsErrorKind::Authentication =>
                    (RoleFailureClass::Authentication, transience, true),
                RoleHdfsErrorKind::AlreadyExists =>
                    (RoleFailureClass::Conflict, transience, false),
                RoleHdfsErrorKind::Unsupported =>
                    (RoleFailureClass::Unsupported, transience, false),
                RoleHdfsErrorKind::Io | RoleHdfsErrorKind::Rpc =>
                    (RoleFailureClass::Connectivity, transience, true),
                RoleHdfsErrorKind::BlocksMissing | RoleHdfsErrorKind::DataTransfer =>
                    (RoleFailureClass::Corruption, transience, false),
                _ => (RoleFailureClass::Protocol, transience, false),
            }
        }
        _ => (RoleFailureClass::Protocol, RoleTransience::Unknown, false),
    }
}
