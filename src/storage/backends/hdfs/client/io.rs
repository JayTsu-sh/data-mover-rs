impl HDFSStorage {
    /// Open a regular HDFS file for independent positional reads.
    ///
    /// # Errors
    ///
    /// Returns an error for an escaping path, missing file, directory, or
    /// upstream open failure.
    async fn open_file(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<HdfsReadHandle, StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.kind == RoleEntryKind::Directory {
            return Err(StorageError::InvalidPath(format!(
                "HDFS read target is a directory: {}",
                relative_path.display()
            )));
        }
        let path = self.resolve_path(relative_path)?;
        let reader = retry_hdfs_read("open file", Some(relative_path), None, || {
            self.client.read(&path)
        })
        .await?;
        let length = u64::try_from(reader.file_length())
            .map_err(|_| config_error("HDFS file length does not fit u64"))?;
        Ok(HdfsReadHandle {
            reader: Arc::new(reader),
            length,
            relative_path: relative_path.to_path_buf(),
        })
    }

    /// Read at most `count` bytes at `offset` without changing a shared cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform cannot represent the planned range or
    /// the upstream positional read fails.
    async fn read_at(
        &self,
        file: &HdfsReadHandle,
        offset: u64,
        count: u64,
    ) -> Result<bytes::Bytes, StorageError> {
        let Some((offset, length)) = plan_read_range(file.length, offset, count)? else {
            return Ok(bytes::Bytes::new());
        };
        let path = self.resolve_path(&file.relative_path)?;
        retry_hdfs_read_indexed("read range", Some(&file.relative_path), None, |attempt| {
            let reader = file.reader.clone();
            let client = self.client.clone();
            let path = path.clone();
            async move {
                if attempt == 0 {
                    reader.read_range(offset, length).await
                } else {
                    let refreshed = client.read(&path).await?;
                    // A verification retry may observe a truncated stage. Do not pass a
                    // range planned against the old length to the native reader (it panics).
                    let length = length.min(refreshed.file_length().saturating_sub(offset));
                    if length == 0 {
                        return Ok(bytes::Bytes::new());
                    }
                    refreshed.read_range(offset, length).await
                }
            }
        })
        .await
    }

    /// Delete one regular file below the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error for traversal, missing paths, directories, upstream
    /// failures, or an upstream `false` result.
    pub async fn delete_file(&self, relative_path: &std::path::Path) -> Result<(), StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.kind == RoleEntryKind::Directory {
            return Err(StorageError::InvalidPath(format!(
                "HDFS file delete target is a directory: {}",
                relative_path.display()
            )));
        }
        self.delete_resolved(relative_path, false).await
    }

    /// Recursively delete a directory while protecting the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error for the root, traversal, missing paths, files, upstream
    /// failures, or an upstream `false` result.
    pub async fn delete_dir_all(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        if relative_path.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "refusing to delete the configured HDFS root".to_string(),
            ));
        }
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.kind != RoleEntryKind::Directory {
            return Err(StorageError::InvalidPath(format!(
                "HDFS directory delete target is a file: {}",
                relative_path.display()
            )));
        }
        self.delete_resolved(relative_path, true).await
    }

    pub(crate) async fn rename_with_overwrite(
        &self,
        from: &std::path::Path,
        to: &std::path::Path,
        overwrite: bool,
    ) -> Result<(), StorageError> {
        if from.as_os_str().is_empty() || to.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "HDFS rename cannot use the configured storage root".to_string(),
            ));
        }
        let from_path = self.resolve_path(from)?;
        let to_path = self.resolve_path(to)?;
        let source = self.get_metadata(from).await?;
        if from == to {
            return Ok(());
        }
        if source.kind == RoleEntryKind::Directory && to.starts_with(from) {
            return Err(StorageError::InvalidPath(format!(
                "HDFS cannot rename a directory into its own subtree: {} -> {}",
                from.display(),
                to.display()
            )));
        }
        if let Some(parent) = to.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent, 0o755).await?;
        }
        self.client
            .rename(&from_path, &to_path, overwrite)
            .await
            .map_err(|error| hdfs_operation_error("rename", Some(from), &error))
    }

    async fn delete_resolved(
        &self,
        relative_path: &std::path::Path,
        recursive: bool,
    ) -> Result<(), StorageError> {
        let path = self.resolve_path(relative_path)?;
        let deleted = self
            .client
            .delete(&path, recursive)
            .await
            .map_err(|error| hdfs_operation_error("delete", Some(relative_path), &error))?;
        deleted
            .then_some(())
            .ok_or_else(|| StorageError::FileNotFound(relative_path.display().to_string()))
    }
}

async fn open_append_after_lease_recovery(
    client: &Client,
    path: &str,
    relative_path: &Path,
) -> Result<FileWriter, StorageError> {
    let started = tokio::time::Instant::now();
    let mut attempt = 0_u32;
    loop {
        match client.append(path).await {
            Ok(writer) => return Ok(writer),
            Err(error) => {
                if let Some(delay) = append_open_retry_delay(&error, attempt, started.elapsed()) {
                    sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                if is_append_lease_recovery_error(&error) {
                    let class = match &error {
                        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => {
                            Some(class.as_str())
                        }
                        _ => None,
                    };
                    return Err(structured_hdfs_error(
                        "append file",
                        Some(relative_path),
                        crate::error::HdfsErrorKind::Rpc,
                        class,
                        "lease recovery did not finish within the retry window",
                        true,
                    ));
                }
                return Err(hdfs_operation_error(
                    "append file",
                    Some(relative_path),
                    &error,
                ));
            }
        }
    }
}

const HDFS_LEASE_RECOVERY_TIMEOUT: Duration = Duration::from_mins(2);
const HDFS_LEASE_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) async fn recover_lease_until_closed(
    client: &Client,
    path: &str,
    relative_path: &Path,
) -> Result<(), StorageError> {
    let started = tokio::time::Instant::now();
    loop {
        match client.recover_lease(path).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) if is_recovery_in_progress_error(&error) => {}
            Err(error) => {
                return Err(hdfs_operation_error(
                    "recover file lease",
                    Some(relative_path),
                    &error,
                ));
            }
        }

        let Some(delay) = lease_recovery_poll_delay(started.elapsed()) else {
            return Err(structured_hdfs_error(
                "recover file lease",
                Some(relative_path),
                crate::error::HdfsErrorKind::Rpc,
                None,
                "lease recovery did not finish within the retry window",
                true,
            ));
        };
        sleep(delay).await;
    }
}

fn is_recovery_in_progress_error(error: &HdfsError) -> bool {
    match error {
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => {
            class_has_suffix(class, &["RecoveryInProgressException"])
        }
        _ => false,
    }
}

fn lease_recovery_poll_delay(elapsed: Duration) -> Option<Duration> {
    let remaining = HDFS_LEASE_RECOVERY_TIMEOUT.checked_sub(elapsed)?;
    (HDFS_LEASE_RECOVERY_POLL_INTERVAL < remaining).then_some(HDFS_LEASE_RECOVERY_POLL_INTERVAL)
}

fn is_append_lease_recovery_error(error: &HdfsError) -> bool {
    match error {
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => {
            class_has_suffix(class, &["RecoveryInProgressException"])
        }
        _ => hdfs_structured_attributes(error).0 == HdfsErrorKind::AlreadyExists,
    }
}

fn append_open_retry_delay(error: &HdfsError, attempt: u32, elapsed: Duration) -> Option<Duration> {
    if !is_append_lease_recovery_error(error) {
        return None;
    }
    let delay = match error {
        HdfsError::RPCError(_, _) | HdfsError::FatalRPCError(_, _) => Duration::from_secs(1),
        _ => Duration::from_secs(1_u64 << attempt.min(3)),
    };
    let remaining = HDFS_LEASE_RECOVERY_TIMEOUT.checked_sub(elapsed)?;
    (delay < remaining).then_some(delay)
}
