use std::ops::Range as RoleRange;

use bytes::Bytes as RoleBytes;

use crate::error::HdfsErrorKind as RoleHdfsErrorKind;
use crate::model::{
    EntryKind as RoleEntryKind, EntryOperationFailure as RoleEntryFailure,
    FailureClass as RoleFailureClass, Operation as RoleOperation, StoragePath as RoleStoragePath,
    Transience as RoleTransience,
};
use crate::storage::StorageRoleFailure as RoleFailure;
use crate::storage::backends::hdfs::protocol::{
    HdfsEntryFacts as RoleHdfsEntryFacts, HdfsListedChild as RoleHdfsListedChild,
    HdfsProtocol as RoleHdfsProtocol, HdfsWriteSession as RoleHdfsWriteSession,
    entry_failure as role_entry_failure, session_failure as role_session_failure,
};

#[async_trait::async_trait]
impl RoleHdfsProtocol for HDFSStorage {
    fn read_concurrency(&self) -> usize {
        self.transfer_concurrency().read()
    }
    async fn open_reader(
        &self,
        path: &RoleStoragePath,
    ) -> Result<
        Option<Arc<dyn crate::storage::backends::hdfs::protocol::HdfsReadCursor>>,
        RoleFailure,
    > {
        let file = self
            .open_file(Path::new(path.as_str()))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Read, error))?;
        Ok(Some(Arc::new(RoleReadCursor {
            file,
            path: path.clone(),
        })))
    }
    async fn stat(&self, path: &RoleStoragePath) -> Result<RoleHdfsEntryFacts, RoleFailure> {
        let entry = self
            .get_metadata(Path::new(path.as_str()))
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Observe, error))?;
        Ok(entry)
    }

    async fn open_listing(&self, path: &RoleStoragePath) -> Box<dyn RoleHdfsListing> {
        self.role_listing(path)
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
            RoleEntryKind::Directory => self.delete_resolved(Path::new(path.as_str()), false).await,
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

    async fn cleanup_transfer_artifacts(
        &self,
        path: &RoleStoragePath,
        require_empty: bool,
    ) -> Result<(), RoleFailure> {
        let mut listing = self.role_listing(path);
        let mut artifacts = Vec::new();
        while let Some(children) = listing
            .next_page(std::num::NonZeroUsize::new(1000).unwrap_or(std::num::NonZeroUsize::MIN))
            .await?
        {
            for child in children {
                let child = child.map_err(RoleFailure::Entry)?;
                if crate::storage::artifacts::is_artifact_path(child.path.as_str()) {
                    artifacts.push(child);
                } else if require_empty {
                    return Err(hdfs_role_entry(
                        path,
                        RoleOperation::Namespace,
                        RoleFailureClass::Conflict,
                        RoleTransience::Permanent,
                    ));
                }
            }
        }
        for artifact in artifacts {
            self.delete_resolved(
                Path::new(artifact.path.as_str()),
                artifact.kind == RoleEntryKind::Directory,
            )
            .await
            .map_err(|e| hdfs_role_error(path, RoleOperation::Namespace, e))?;
        }
        Ok(())
    }

    async fn delete_subtree(
        &self,
        path: &RoleStoragePath,
        allow_storage_root: bool,
    ) -> Result<(), RoleFailure> {
        if path == &RoleStoragePath::root() && !allow_storage_root {
            return Err(hdfs_role_entry(
                path,
                RoleOperation::Namespace,
                RoleFailureClass::InvalidInput,
                RoleTransience::Permanent,
            ));
        }
        let result = if path == &RoleStoragePath::root() {
            self.delete_resolved(Path::new(""), true).await
        } else {
            self.delete_dir_all(Path::new(path.as_str())).await
        };
        result.map_err(|e| hdfs_role_error(path, RoleOperation::Namespace, e))
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

    async fn open_stage_writer(
        &self,
        path: &RoleStoragePath,
        start_offset: u64,
        direct: bool,
    ) -> Result<Box<dyn RoleHdfsWriteSession + '_>, RoleFailure> {
        let native = Path::new(path.as_str());
        let resolved = self
            .resolve_path(native)
            .map_err(|error| hdfs_role_error(path, RoleOperation::Write, error))?;
        let writer = if direct {
            if let Some(parent) = native
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                self.create_dir_all(parent, 0o755)
                    .await
                    .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
            }
            if start_offset != 0 {
                return Err(invalid_stage_chunk(path));
            }
            self.client
                .create(
                    &resolved,
                    hdfs_native::WriteOptions::default()
                        .block_size(self.block_size)
                        .overwrite(true),
                )
                .await
                .map_err(|error| {
                    hdfs_role_error(
                        path,
                        RoleOperation::Write,
                        hdfs_operation_error("create direct target", Some(native), &error),
                    )
                })?
        } else {
            let metadata = self
                .get_metadata(native)
                .await
                .map_err(|error| hdfs_role_error(path, RoleOperation::Write, error))?;
            if metadata.kind == RoleEntryKind::Directory || metadata.size != Some(start_offset) {
                return Err(hdfs_role_entry(
                    path,
                    RoleOperation::Write,
                    RoleFailureClass::Conflict,
                    RoleTransience::Permanent,
                ));
            }
            open_append_after_lease_recovery(&self.client, &resolved, native)
                .await
                .map_err(|error| hdfs_role_error(path, RoleOperation::Write, error))?
        };
        Ok(Box::new(RoleWriteSession {
            writer,
            path: path.clone(),
        }))
    }

    async fn stabilize_recovered_stage(&self, path: &RoleStoragePath) -> Result<u64, RoleFailure> {
        let native = Path::new(path.as_str());
        let resolved = self
            .resolve_path(native)
            .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
        recover_lease_until_closed(&self.client, &resolved, native)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
        let metadata = self
            .get_metadata(native)
            .await
            .map_err(|error| hdfs_role_error(path, RoleOperation::Prepare, error))?;
        if metadata.kind == RoleEntryKind::Directory {
            return Err(hdfs_role_entry(
                path,
                RoleOperation::Prepare,
                RoleFailureClass::Corruption,
                RoleTransience::Permanent,
            ));
        }
        metadata.size.ok_or_else(|| invalid_stage_chunk(path))
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

    async fn set_mode(&self, path: &RoleStoragePath, mode: u32) -> Result<(), RoleFailure> {
        self.set_permission(Path::new(path.as_str()), mode)
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

struct RoleWriteSession {
    writer: FileWriter,
    path: RoleStoragePath,
}

#[async_trait::async_trait]
impl RoleHdfsWriteSession for RoleWriteSession {
    async fn write(&mut self, data: RoleBytes) -> Result<usize, RoleFailure> {
        Box::pin(self.writer.write_bytes(data))
            .await
            .map_err(|error| {
                hdfs_role_error(
                    &self.path,
                    RoleOperation::Write,
                    hdfs_operation_error(
                        "write role stream",
                        Some(Path::new(self.path.as_str())),
                        &error,
                    ),
                )
            })
    }

    async fn hsync(&mut self) -> Result<(), RoleFailure> {
        Box::pin(self.writer.hsync()).await.map_err(|error| {
            hdfs_role_error(
                &self.path,
                RoleOperation::Write,
                hdfs_operation_error(
                    "hsync role writer",
                    Some(Path::new(self.path.as_str())),
                    &error,
                ),
            )
        })
    }

    async fn close(mut self: Box<Self>) -> Result<(), RoleFailure> {
        Box::pin(self.writer.close()).await.map_err(|error| {
            hdfs_role_error(
                &self.path,
                RoleOperation::Write,
                hdfs_operation_error(
                    "close role writer",
                    Some(Path::new(self.path.as_str())),
                    &error,
                ),
            )
        })
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
    if metadata.kind == RoleEntryKind::Directory || metadata.size != Some(0) {
        return Err(hdfs_role_entry(
            path,
            RoleOperation::Prepare,
            RoleFailureClass::Corruption,
            RoleTransience::Permanent,
        ));
    }
    Ok(())
}

fn invalid_stage_chunk(path: &RoleStoragePath) -> RoleFailure {
    hdfs_role_entry(
        path,
        RoleOperation::Write,
        RoleFailureClass::InvalidInput,
        RoleTransience::Permanent,
    )
}

/// One child of a role listing. A child that cannot be described is an entry failure under its
/// own path (under the directory's when it has none here): the name node sent a status this
/// client cannot represent, so it is `Protocol` / `Permanent`, never the session's problem.
fn hdfs_role_child(
    directory: &RoleStoragePath,
    child: Result<RoleHdfsEntryFacts, UndescribedHdfsChild>,
) -> RoleHdfsListedChild {
    let blamed = match child {
        Ok(entry) => return Ok(entry),
        Err(undescribed) => undescribed
            .relative_path
            .and_then(|path| RoleStoragePath::new(path.to_string_lossy()).ok())
            .unwrap_or_else(|| directory.clone()),
    };
    Err(RoleEntryFailure::new(
        blamed,
        RoleOperation::Traverse,
        RoleFailureClass::Protocol,
        RoleTransience::Permanent,
        "HDFS listing could not describe the entry",
    )
    .unwrap_or_else(|_| unreachable!("static HDFS listing diagnostic is valid")))
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

struct RoleReadCursor {
    file: HdfsReadHandle,
    path: RoleStoragePath,
}

#[async_trait::async_trait]
impl crate::storage::backends::hdfs::protocol::HdfsReadCursor for RoleReadCursor {
    async fn read_range(&self, range: RoleRange<u64>) -> Result<RoleBytes, RoleFailure> {
        let offset = usize::try_from(range.start).map_err(|_| invalid_stage_chunk(&self.path))?;
        let length = usize::try_from(range.end - range.start)
            .map_err(|_| invalid_stage_chunk(&self.path))?;
        retry_hdfs_read(
            "read role cursor",
            Some(Path::new(self.path.as_str())),
            None,
            || self.file.reader.read_range(offset, length),
        )
        .await
        .map_err(|error| hdfs_role_error(&self.path, RoleOperation::Read, error))
    }
}
