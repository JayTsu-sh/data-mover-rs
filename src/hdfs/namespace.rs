/// A child of an HDFS listing that could not be described.
#[derive(Debug)]
pub(crate) struct UndescribedHdfsChild {
    /// The child's path relative to the root, when the listing named an immediate child of the
    /// listed directory; `None` blames the directory itself.
    pub(crate) relative_path: Option<PathBuf>,
    pub(crate) error: StorageError,
}

/// The legacy listing contract: every child, or the first child that could not be described.
fn first_failure(
    children: Vec<Result<HDFSEntry, UndescribedHdfsChild>>,
) -> Result<Vec<HDFSEntry>, StorageError> {
    children
        .into_iter()
        .map(|child| child.map_err(|undescribed| undescribed.error))
        .collect()
}

/// Concrete HDFS client ownership and root-isolation shell.
#[derive(Clone, Debug)]
pub struct HDFSStorage {
    client: Client,
    location: HdfsLocation,
    block_size: u64,
    transfer_concurrency: crate::TransferConcurrency,
}

impl HDFSStorage {
    /// Builds the architecture-ready role handle for this connected HDFS backend. Its identity is
    /// the location's canonical endpoint (ADR-0006), the one
    /// [`crate::storage::endpoint_identity`] derives from the same location.
    ///
    /// # Errors
    /// Returns an error when no endpoint can be derived or roles contradict capabilities.
    pub fn architecture_storage(
        &self,
    ) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
        let identity = self.location.endpoint_identity()?;
        crate::storage::backends::hdfs::connect(Arc::new(self.clone()), identity)
    }

    pub(crate) fn transfer_namespace(&self) -> String {
        format!("{}{}", self.location.endpoint(), self.location.root())
    }
    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }

    #[must_use]
    pub const fn location(&self) -> &HdfsLocation {
        &self.location
    }

    #[must_use]
    pub const fn block_size(&self) -> u64 {
        self.block_size
    }

    #[must_use]
    pub const fn transfer_concurrency(&self) -> crate::TransferConcurrency {
        self.transfer_concurrency
    }

    #[must_use]
    pub const fn with_transfer_concurrency(
        mut self,
        concurrency: crate::TransferConcurrency,
    ) -> Self {
        self.transfer_concurrency = concurrency;
        self
    }

    /// Resolve a relative adapter path below the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error for absolute paths, traversal, or non-UTF-8 components.
    pub fn resolve_path(&self, relative: &std::path::Path) -> Result<String, StorageError> {
        let mut parts = Vec::new();
        for component in relative.components() {
            match component {
                std::path::Component::Normal(value) => parts.push(
                    value
                        .to_str()
                        .ok_or_else(|| config_error("HDFS paths must be valid UTF-8"))?,
                ),
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_) => {
                    return Err(config_error(
                        "HDFS adapter paths must remain relative to the configured root",
                    ));
                }
            }
        }
        if parts.is_empty() {
            Ok(self.location.root.clone())
        } else if self.location.root == "/" {
            Ok(format!("/{}", parts.join("/")))
        } else {
            Ok(format!("{}/{}", self.location.root, parts.join("/")))
        }
    }

    /// Convert an upstream status into metadata relative to this storage root.
    ///
    /// # Errors
    ///
    /// Returns an error if the status is outside the configured root or its
    /// millisecond timestamps cannot be represented as nanoseconds.
    pub fn entry_from_status(
        &self,
        status: hdfs_native::client::FileStatus,
    ) -> Result<HDFSEntry, StorageError> {
        let relative = strip_root(&status.path, self.location.root())?;
        let relative_path = PathBuf::from(relative);
        let name = relative_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let extension = relative_path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string);
        Ok(HDFSEntry {
            name,
            relative_path,
            is_dir: status.isdir,
            size: u64::try_from(status.length)
                .map_err(|_| config_error("HDFS file size does not fit u64"))?,
            mtime: millis_to_nanos(status.modification_time)?,
            atime: millis_to_nanos(status.access_time)?,
            mode: u32::from(status.permission),
            owner: status.owner,
            group: status.group,
            replication: status.replication,
            block_size: status.blocksize,
            extension,
        })
    }

    /// Look up metadata for a path relative to the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes the configured root, does not
    /// exist, or the upstream status cannot be represented by `HDFSEntry`.
    pub async fn get_metadata(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<HDFSEntry, StorageError> {
        let path = self.resolve_path(relative_path)?;
        let status = retry_hdfs_read("get metadata", Some(relative_path), None, || {
            self.client.get_file_info(&path)
        })
        .await?;
        self.entry_from_status(status)
    }

    /// Set Unix permission bits on a file or directory below this storage root.
    ///
    /// # Errors
    ///
    /// Returns an error for the storage root, escaping paths, permission bits
    /// outside HDFS' `0o7777` mask, or an upstream mutation failure.
    pub async fn set_permission(
        &self,
        relative_path: &std::path::Path,
        mode: u32,
    ) -> Result<(), StorageError> {
        if mode > 0o7777 {
            return Err(config_error("HDFS permission must fit in 0o7777"));
        }
        let path = self.resolve_mutation_path(relative_path)?;
        self.client
            .set_permission(&path, mode)
            .await
            .map_err(|error| hdfs_operation_error("set permission", Some(relative_path), &error))
    }

    /// Set modification time while preserving the current HDFS access time.
    ///
    /// The public timestamp uses the crate's nanosecond convention. HDFS stores
    /// millisecond timestamps, so sub-millisecond precision is truncated.
    ///
    /// # Errors
    ///
    /// Returns an error for a negative timestamp, the storage root, an escaping
    /// path, metadata lookup failure, or an upstream mutation failure.
    pub async fn set_mtime(
        &self,
        relative_path: &std::path::Path,
        mtime: i64,
    ) -> Result<(), StorageError> {
        let mtime = nanos_to_millis(mtime)?;
        let path = self.resolve_mutation_path(relative_path)?;
        let current = self.get_metadata(relative_path).await?;
        let atime = nanos_to_millis(current.atime)?;
        self.client
            .set_times(&path, mtime, atime)
            .await
            .map_err(|error| hdfs_operation_error("set times", Some(relative_path), &error))
    }

    /// Selectively update HDFS timestamps and permission bits.
    ///
    /// Omitted timestamps retain their current values. HDFS identities are
    /// string-valued, so numeric uid/gid translation remains the caller's
    /// responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid timestamps or permissions, paths outside
    /// the configured root, metadata lookup failures, or upstream mutations.
    pub async fn set_metadata(
        &self,
        relative_path: &std::path::Path,
        atime: Option<i64>,
        mtime: Option<i64>,
        mode: Option<u32>,
    ) -> Result<(), StorageError> {
        if atime.is_some() || mtime.is_some() {
            let current = self.get_metadata(relative_path).await?;
            let atime = nanos_to_millis(atime.unwrap_or(current.atime))?;
            let mtime = nanos_to_millis(mtime.unwrap_or(current.mtime))?;
            let path = self.resolve_mutation_path(relative_path)?;
            self.client
                .set_times(&path, mtime, atime)
                .await
                .map_err(|error| hdfs_operation_error("set times", Some(relative_path), &error))?;
        }
        if let Some(mode) = mode {
            self.set_permission(relative_path, mode & 0o7777).await?;
        }
        Ok(())
    }

    /// Set HDFS string owner and/or group, leaving omitted values unchanged.
    ///
    /// Empty identity strings are treated as omitted. No numeric or `NFSv4`
    /// identity mapping is performed.
    ///
    /// # Errors
    ///
    /// Returns an error for the storage root, an escaping path, or an upstream
    /// mutation failure.
    pub async fn set_owner_group(
        &self,
        relative_path: &std::path::Path,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> Result<(), StorageError> {
        let path = self.resolve_mutation_path(relative_path)?;
        let owner = owner.filter(|value| !value.is_empty());
        let group = group.filter(|value| !value.is_empty());
        if owner.is_none() && group.is_none() {
            return Ok(());
        }
        self.client
            .set_owner(&path, owner, group)
            .await
            .map_err(|error| hdfs_operation_error("set owner", Some(relative_path), &error))
    }

    fn resolve_mutation_path(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<String, StorageError> {
        if relative_path.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "refusing to mutate the configured HDFS root".to_string(),
            ));
        }
        self.resolve_path(relative_path)
    }

    /// List exactly the immediate children of one directory below this root, transfer artifacts
    /// (`.data-mover-*`, ADR-0006) included: walks hide them, raw callers must see them.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is missing, is not a directory, escapes
    /// the configured root, or upstream returns a non-immediate child.
    pub async fn list_directory(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        first_failure(self.list_directory_children(relative_path).await?)
    }

    /// [`Self::list_directory`] that keeps going past a child it cannot describe: each child is
    /// its entry or why it could not be described, in listing order. Only a failure that concerns
    /// the directory itself (missing, not a directory, the listing RPC) fails the whole call.
    pub(crate) async fn list_directory_children(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<Vec<Result<HDFSEntry, UndescribedHdfsChild>>, StorageError> {
        let directory = self.get_metadata(relative_path).await?;
        if !directory.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS listing target is not a directory: {}",
                relative_path.display()
            )));
        }
        let path = self.resolve_path(relative_path)?;
        let statuses = retry_hdfs_read("list directory", Some(relative_path), None, || {
            self.client.list_status(&path, false)
        })
        .await?;
        Ok(self.children_from_listing(statuses, &directory.relative_path))
    }

    /// [`Self::list_directory`] without transfer artifacts (ADR-0006 C3b).
    /// The raw listing stays available for callers that must see them.
    pub(crate) async fn list_visible(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        let mut entries = self.list_directory(relative_path).await?;
        entries.retain(|entry| {
            let artifact = is_artifact_native(&entry.relative_path);
            if artifact {
                tracing::trace!(
                    "[HDFS] listing skips transfer artifact {}",
                    entry.relative_path.display()
                );
            }
            !artifact
        });
        Ok(entries)
    }

    #[cfg(test)]
    fn entries_from_listing(
        &self,
        statuses: Vec<hdfs_native::client::FileStatus>,
        directory: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        first_failure(self.children_from_listing(statuses, directory))
    }

    /// Converts one listing child by child. A child is blamed under its own path when that path
    /// is an immediate child of `directory`; otherwise (outside the root, or not immediate) the
    /// failure has no path of its own in this directory and is blamed on the directory.
    fn children_from_listing(
        &self,
        statuses: Vec<hdfs_native::client::FileStatus>,
        directory: &std::path::Path,
    ) -> Vec<Result<HDFSEntry, UndescribedHdfsChild>> {
        statuses
            .into_iter()
            .map(|status| {
                let own_path = strip_root(&status.path, self.location.root())
                    .ok()
                    .map(PathBuf::from)
                    .filter(|path| path.parent() == Some(directory));
                let undescribed = |error| UndescribedHdfsChild {
                    relative_path: own_path.clone(),
                    error,
                };
                let entry = self.entry_from_status(status).map_err(undescribed)?;
                if entry.relative_path.parent() != Some(directory) {
                    return Err(undescribed(config_error(
                        "HDFS directory listing returned a non-immediate child",
                    )));
                }
                Ok(entry)
            })
            .collect()
    }



    /// Recursively create a directory below the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes the root, an existing component
    /// is not a directory, or HDFS rejects the requested operation.
    pub async fn create_dir_all(
        &self,
        relative_path: &std::path::Path,
        mode: u16,
    ) -> Result<(), StorageError> {
        let path = self.resolve_path(relative_path)?;
        if relative_path.as_os_str().is_empty() {
            let status = retry_hdfs_read("get root metadata", None, None, || {
                self.client.get_file_info(&path)
            })
            .await?;
            if !status.isdir {
                return Err(StorageError::InvalidPath(
                    "configured HDFS root is not a directory".to_string(),
                ));
            }
            return Ok(());
        }
        self.client
            .mkdirs(&path, u32::from(mode), true)
            .await
            .map_err(|error| hdfs_operation_error("create directory", Some(relative_path), &error))
    }
}
