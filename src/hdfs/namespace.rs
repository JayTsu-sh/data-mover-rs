/// Concrete HDFS client ownership and root-isolation shell.
#[derive(Clone, Debug)]
pub struct HDFSStorage {
    client: Client,
    location: HdfsLocation,
    block_size: u64,
    transfer_concurrency: crate::TransferConcurrency,
}

impl HDFSStorage {
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
                .map_err(|error| {
                    hdfs_operation_error("set times", Some(relative_path), &error)
                })?;
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

    /// List exactly the immediate children of one directory below this root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is missing, is not a directory, escapes
    /// the configured root, or upstream returns a non-immediate child.
    pub async fn list_directory(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
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
        self.entries_from_listing(statuses, &directory.relative_path)
    }

    fn entries_from_listing(
        &self,
        statuses: Vec<hdfs_native::client::FileStatus>,
        directory: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        statuses
            .into_iter()
            .map(|status| {
                let entry = self.entry_from_status(status)?;
                if entry.relative_path.parent() != Some(directory) {
                    return Err(config_error(
                        "HDFS directory listing returned a non-immediate child",
                    ));
                }
                Ok(entry)
            })
            .collect()
    }

    /// Start a bounded recursive HDFS scan without applying common walk
    /// filtering or message semantics.
    ///
    /// # Errors
    ///
    /// Returns an error when the starting path escapes the configured root or
    /// concurrency is outside the validated `1..=16` range. Runtime listing
    /// failures are emitted as [`HdfsScanEvent::Error`].
    pub fn scan_recursive(
        &self,
        sub_path: Option<&std::path::Path>,
        max_depth: Option<usize>,
        concurrency: usize,
    ) -> Result<crate::AsyncReceiver<HdfsScanEvent>, StorageError> {
        if !(1..=crate::TransferConcurrency::MAX).contains(&concurrency) {
            return Err(config_error(&format!(
                "HDFS scan concurrency must be between 1 and {}, got {concurrency}",
                crate::TransferConcurrency::MAX
            )));
        }
        let root = sub_path.unwrap_or_else(|| std::path::Path::new(""));
        self.resolve_path(root)?;
        let (sender, receiver) = async_channel::bounded(SCAN_CHANNEL_CAPACITY);
        let storage = self.clone();
        let root = root.to_path_buf();
        tokio::spawn(async move {
            storage
                .run_recursive_scan(root, max_depth, concurrency, sender)
                .await;
        });
        Ok(crate::AsyncReceiver::new(receiver))
    }

    /// Adapt the HDFS scanner to the common walk stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scan options, an escaping subpath, or HDFS
    /// packaging, which is not implemented.
    pub fn walkdir(
        &self,
        sub_path: Option<&std::path::Path>,
        options: crate::storage_enum::WalkOptions,
    ) -> Result<crate::WalkDirAsyncIterator, StorageError> {
        if options.packaged {
            return Err(StorageError::UnsupportedType(
                "HDFS packaged walkdir is not implemented".to_string(),
            ));
        }
        let max_depth = options.depth.filter(|depth| *depth != 0);
        let raw = self.scan_recursive(sub_path, max_depth, options.concurrency)?;
        let (sender, receiver) = async_channel::bounded(SCAN_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            while let Some(event) = raw.next().await {
                let message = match event {
                    HdfsScanEvent::Entry(entry) => {
                        if hdfs_entry_is_filtered(
                            &entry,
                            options.match_expressions.as_ref(),
                            options.exclude_expressions.as_ref(),
                        ) {
                            continue;
                        }
                        crate::StorageEntryMessage::Scanned(Arc::new(crate::EntryEnum::HDFS(entry)))
                    }
                    HdfsScanEvent::Error { path, error } => crate::StorageEntryMessage::Error {
                        event: crate::ErrorEvent::Scan,
                        path,
                        entry: None,
                        reason: error.to_string(),
                    },
                };
                if sender.send(message).await.is_err() {
                    break;
                }
            }
        });
        Ok(crate::AsyncReceiver::new(receiver))
    }

    /// Read and sort one HDFS directory for the shared `walkdir_2` DFS driver.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-HDFS handle or when the directory cannot be
    /// listed safely inside the configured root.
    pub async fn read_dir_sorted(
        &self,
        dir_path: &str,
        handle: &crate::dir_tree::DirHandle,
        ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult, StorageError> {
        let crate::dir_tree::DirHandle::Hdfs(relative_path) = handle else {
            return Err(StorageError::MismatchedType);
        };
        let entries = self.list_directory(relative_path).await?;
        Ok(hdfs_read_result(dir_path, entries, ctx))
    }

    /// Run HDFS directory paging through the shared deterministic DFS driver.
    ///
    /// # Errors
    ///
    /// Returns an error when the subpath escapes the configured root or reader
    /// concurrency is outside the validated `1..=16` range.
    pub fn walkdir_2(
        &self,
        sub_path: Option<&std::path::Path>,
        depth: Option<usize>,
        match_expressions: Option<crate::FilterExpression>,
        exclude_expressions: Option<crate::FilterExpression>,
        concurrency: usize,
    ) -> Result<crate::WalkDirAsyncIterator2, StorageError> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        if !(1..=crate::TransferConcurrency::MAX).contains(&concurrency) {
            return Err(config_error(&format!(
                "HDFS walkdir_2 concurrency must be between 1 and {}, got {concurrency}",
                crate::TransferConcurrency::MAX
            )));
        }
        let start_path = sub_path.unwrap_or_else(|| std::path::Path::new(""));
        self.resolve_path(start_path)?;
        let start_path = start_path.to_path_buf();
        let (request_sender, request_receiver) =
            async_channel::bounded::<ReadRequest>(concurrency * 2);
        let (output_sender, output_receiver) = async_channel::bounded(64);

        for _ in 0..concurrency {
            let storage = self.clone();
            let receiver = request_receiver.clone();
            tokio::spawn(async move {
                while let Ok(request) = receiver.recv().await {
                    let result = storage
                        .read_dir_sorted(&request.dir_path, &request.handle, &request.ctx)
                        .await;
                    let _ = request.reply.send(result);
                }
            });
        }

        let context = ReadContext {
            match_expr: Arc::new(match_expressions),
            exclude_expr: Arc::new(exclude_expressions),
            current_depth: 0,
            max_depth: depth.unwrap_or(0),
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        tokio::spawn(run_dfs_driver(
            request_sender,
            output_sender,
            PathBuf::new(),
            DirHandle::Hdfs(start_path),
            context,
        ));
        Ok(crate::AsyncReceiver::new(output_receiver))
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
