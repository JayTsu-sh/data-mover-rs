/// A child of an HDFS listing that could not be described.
#[derive(Debug)]
pub struct UndescribedHdfsChild {
    /// The child's path relative to the root, when the listing named an immediate child of the
    /// listed directory; `None` blames the directory itself.
    pub relative_path: Option<PathBuf>,
    pub _error: StorageError,
}

/// Concrete HDFS client ownership and root-isolation shell.
#[derive(Clone, Debug)]
pub struct HDFSStorage {
    client: Client,
    location: HdfsLocation,
    block_size: u64,
    transfer_concurrency: crate::transfer_concurrency::TransferConcurrency,
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

    #[must_use]
    pub const fn transfer_concurrency(&self) -> crate::transfer_concurrency::TransferConcurrency {
        self.transfer_concurrency
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
            Ok(self.location.root().to_owned())
        } else if self.location.root() == "/" {
            Ok(format!("/{}", parts.join("/")))
        } else {
            Ok(format!("{}/{}", self.location.root(), parts.join("/")))
        }
    }

    /// Convert an upstream status into metadata relative to this storage root.
    ///
    /// # Errors
    ///
    /// Returns an error if the status is outside the configured root or its
    /// millisecond timestamps cannot be represented as nanoseconds.
    pub fn facts_from_status(
        &self,
        status: hdfs_native::client::FileStatus,
    ) -> Result<RoleHdfsEntryFacts, StorageError> {
        let relative = strip_root(&status.path, self.location.root())?;
        let path =
            RoleStoragePath::new(relative).map_err(|_| config_error("invalid HDFS status path"))?;
        Ok(RoleHdfsEntryFacts {
            path,
            kind: if status.isdir {
                RoleEntryKind::Directory
            } else {
                RoleEntryKind::File
            },
            size: if status.isdir {
                None
            } else {
                Some(
                    u64::try_from(status.length)
                        .map_err(|_| config_error("HDFS file size does not fit u64"))?,
                )
            },
            mtime: millis_to_nanos(status.modification_time)?,
            atime: millis_to_nanos(status.access_time)?,
            mode: u32::from(status.permission),
            owner: status.owner,
            group: status.group,
            replication: status.replication,
            block_size: status.blocksize,
        })
    }

    /// Look up metadata for a path relative to the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes the configured root, does not
    /// exist, or the upstream status cannot be represented by `RoleHdfsEntryFacts`.
    pub async fn get_metadata(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<RoleHdfsEntryFacts, StorageError> {
        let path = self.resolve_path(relative_path)?;
        let status = retry_hdfs_read("get metadata", Some(relative_path), None, || {
            self.client.get_file_info(&path)
        })
        .await?;
        self.facts_from_status(status)
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

    /// Converts one listing child by child. A child is blamed under its own path when that path
    /// is an immediate child of `directory`; otherwise (outside the root, or not immediate) the
    /// failure has no path of its own in this directory and is blamed on the directory.
    fn children_from_listing(
        &self,
        statuses: Vec<hdfs_native::client::FileStatus>,
        directory: &std::path::Path,
    ) -> Vec<Result<RoleHdfsEntryFacts, UndescribedHdfsChild>> {
        statuses
            .into_iter()
            .map(|status| {
                let own_path = strip_root(&status.path, self.location.root())
                    .ok()
                    .map(PathBuf::from)
                    .filter(|path| path.parent() == Some(directory));
                let undescribed = |error| UndescribedHdfsChild {
                    relative_path: own_path.clone(),
                    _error: error,
                };
                let entry = self.facts_from_status(status).map_err(undescribed)?;
                if Path::new(entry.path.as_str()).parent() != Some(directory) {
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
