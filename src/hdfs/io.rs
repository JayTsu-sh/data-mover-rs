impl HDFSStorage {
    /// Open a regular HDFS file for independent positional reads.
    ///
    /// # Errors
    ///
    /// Returns an error for an escaping path, missing file, directory, or
    /// upstream open failure.
    pub async fn open_file(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<HDFSFileHandle, StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
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
        Ok(HDFSFileHandle {
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
    pub async fn read_at(
        &self,
        file: &HDFSFileHandle,
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
                    client.read(&path).await?.read_range(offset, length).await
                }
            }
        })
        .await
    }

    pub(crate) async fn read_file(
        &self,
        relative_path: &std::path::Path,
        size: u64,
    ) -> Result<bytes::Bytes, StorageError> {
        let file = self.open_file(relative_path).await?;
        self.read_at(&file, 0, size).await
    }

    pub(crate) async fn read_data(
        &self,
        sender: tokio::sync::mpsc::Sender<crate::DataChunk>,
        relative_path: &std::path::Path,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<crate::QosManager>,
    ) -> Result<Option<HashCalculator>, StorageError> {
        if size == 0 {
            return Ok(None);
        }
        let file = self.open_file(relative_path).await?;
        let end = size.min(file.length);
        let chunk_size = transfer_chunk_size(self.block_size);
        let mut next_offset = 0_u64;
        let mut inflight = FuturesOrdered::new();
        let mut hasher = create_hash_calculator(enable_integrity_check);
        loop {
            while inflight.len() < self.transfer_concurrency.read() && next_offset < end {
                let requested = chunk_size.min(end - next_offset);
                let count = if let Some(qos) = qos.as_ref() {
                    let granted = qos.acquire_bandwidth_grant(requested).await;
                    qos.acquire_iops().await;
                    granted
                } else {
                    requested
                };
                let storage = self.clone();
                let file = file.clone();
                let offset = next_offset;
                inflight.push_back(
                    async move { (offset, storage.read_at(&file, offset, count).await) },
                );
                next_offset += count;
            }
            let Some((offset, data)) = inflight.next().await else {
                break;
            };
            let data = data?;
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&data);
            }
            if sender
                .send(crate::DataChunk { offset, data })
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(hasher)
    }

    pub(crate) async fn read_data_intervals(
        &self,
        sender: tokio::sync::mpsc::Sender<crate::DataChunk>,
        relative_path: &std::path::Path,
        intervals: &[(u64, u64)],
        qos: Option<crate::QosManager>,
    ) -> Result<(), StorageError> {
        let file = self.open_file(relative_path).await?;
        let chunk_size = transfer_chunk_size(self.block_size);
        for &(start, end) in intervals {
            if start > end {
                return Err(config_error("HDFS read interval start exceeds end"));
            }
            let mut offset = start;
            while offset < end.min(file.length) {
                let requested = chunk_size.min(end - offset);
                let count = if let Some(qos) = qos.as_ref() {
                    let granted = qos.acquire_bandwidth_grant(requested).await;
                    qos.acquire_iops().await;
                    granted
                } else {
                    requested
                };
                let data = self.read_at(&file, offset, count).await?;
                if data.is_empty() {
                    break;
                }
                let read = u64::try_from(data.len())
                    .map_err(|_| config_error("HDFS chunk length does not fit u64"))?;
                if sender
                    .send(crate::DataChunk { offset, data })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                offset = offset
                    .checked_add(read)
                    .ok_or_else(|| config_error("HDFS read offset overflow"))?;
            }
        }
        Ok(())
    }

    /// Write a fresh file through one strictly sequential upstream writer.
    pub(crate) async fn write_data(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        expected_size: u64,
        mode: u32,
        replication: Option<u32>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64, StorageError> {
        if let Some(parent) = relative_path.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent, 0o755).await?;
        }
        let path = self.resolve_path(relative_path)?;
        let options = hdfs_native::WriteOptions::default()
            .block_size(self.block_size)
            .permission(mode & 0o7777)
            .overwrite(true);
        let options = replication.map_or(options.clone(), |value| options.replication(value));
        let mut writer =
            self.client.create(&path, options).await.map_err(|error| {
                hdfs_operation_error("create file", Some(relative_path), &error)
            })?;
        let result = self
            .consume_sequential_chunks(
                &mut writer,
                &mut receiver,
                SequentialWriteContext {
                    relative_path,
                    start_offset: 0,
                    expected_size,
                    require_final_size: true,
                    bytes_counter: bytes_counter.as_ref(),
                },
            )
            .await;
        if result.is_err() {
            receiver.close();
            let _ = self.client.delete(&path, false).await;
        }
        result
    }

    /// Append a contiguous streamed tail to an existing regular file.
    ///
    /// `start_offset` must equal the current file length and every received
    /// chunk must form one contiguous tail ending at `expected_final_size`.
    /// This is sequential append, not positional write support.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid paths, missing files, directories, a stale
    /// prefix length, malformed chunks, append/write failures, or close failure.
    pub async fn append_stream(
        &self,
        receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        start_offset: u64,
        expected_final_size: u64,
    ) -> Result<u64, StorageError> {
        self.append_stream_with_progress(
            receiver,
            HdfsAppendTarget::Raw {
                path: relative_path,
                prefix_len: start_offset,
            },
            AppendCompletion::Complete(expected_final_size),
            None,
            None,
        )
        .await
    }

    pub(crate) async fn append_stream_with_progress(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        target: HdfsAppendTarget<'_>,
        completion: AppendCompletion,
        bytes_counter: Option<&Arc<AtomicU64>>,
        on_committed: Option<&crate::CommitCallback>,
    ) -> Result<u64, StorageError> {
        let relative_path = target.path();
        let start_offset = target.prefix_len();
        let (expected_final_size, require_final_size) = match completion {
            AppendCompletion::Complete(size) => (size, true),
            AppendCompletion::PartialUpTo(size) => (size, false),
        };
        if expected_final_size < start_offset {
            return Err(config_error(
                "HDFS append final size cannot precede its starting offset",
            ));
        }
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS append target is a directory: {}",
                relative_path.display()
            )));
        }
        target.validate_current_prefix(metadata.size)?;
        if start_offset == expected_final_size {
            receiver.close();
            return Ok(start_offset);
        }
        let path = self.resolve_path(relative_path)?;
        let mut writer =
            self.client.append(&path).await.map_err(|error| {
                hdfs_operation_error("append file", Some(relative_path), &error)
            })?;
        let write_result = self
            .consume_sequential_chunks(
            &mut writer,
            &mut receiver,
            SequentialWriteContext {
                relative_path,
                start_offset,
                expected_size: expected_final_size,
                require_final_size,
                bytes_counter,
            },
        )
        .await;
        let written_end = match write_result {
            Ok(written_end) => written_end,
            Err(error) => {
                return settle_append_progress(start_offset, Err(error), None, on_committed);
            }
        };
        let persisted_size = self.get_metadata(relative_path).await?.size;
        settle_append_progress(
            start_offset,
            Ok(written_end),
            Some(persisted_size),
            on_committed,
        )
    }

    /// Prepare a trusted HDFS temporary file for tail-only resume.
    ///
    /// Returns the validated contiguous prefix length. Missing and explicitly
    /// fresh state is created as an empty file; overlong state is rebuilt.
    ///
    /// # Errors
    ///
    /// Returns an error for a root/escaping path, directory state, inaccessible
    /// metadata, or failure to rebuild the temporary file.
    pub async fn prepare_tail_resume(
        &self,
        part_path: &std::path::Path,
        expected_size: u64,
        resume: bool,
    ) -> Result<u64, StorageError> {
        self.prepare_tail_transfer(part_path, expected_size, resume, 0o644, None)
            .await
            .map(|state| state.prefix_len())
    }

    pub(crate) async fn prepare_tail_transfer(
        &self,
        part_path: &std::path::Path,
        expected_size: u64,
        resume: bool,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<HdfsPreparedTransfer, StorageError> {
        let requested = HdfsPreparedTransfer::new(
            part_path.to_path_buf(),
            0,
            expected_size,
            expected_size,
            mode,
            replication,
        )?;
        self.resolve_path(requested.part_path())?;
        let prefix_len = if resume {
            match self.get_metadata(requested.part_path()).await {
                Ok(metadata) if metadata.is_dir => Err(StorageError::InvalidPath(format!(
                    "HDFS resume temporary path is a directory: {}",
                    requested.part_path().display()
                ))),
                Ok(metadata) if metadata.size <= expected_size => Ok(metadata.size),
                Ok(_) | Err(StorageError::FileNotFound(_)) => {
                    self.rebuild_resume_file(
                        requested.part_path(),
                        requested.mode(),
                        requested.replication(),
                    )
                    .await?;
                    Ok(0)
                }
                Err(error) => Err(error),
            }
            ?
        } else {
            self.rebuild_resume_file(
                requested.part_path(),
                requested.mode(),
                requested.replication(),
            )
            .await?;
            0
        };
        HdfsPreparedTransfer::new(
            requested.part_path().to_path_buf(),
            prefix_len,
            requested.expected_size(),
            requested.expected_size(),
            requested.mode(),
            requested.replication(),
        )
    }

    pub(crate) async fn append_prepared_tail(
        &self,
        receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        state: &HdfsPreparedTransfer,
        bytes_counter: Option<&Arc<AtomicU64>>,
        on_committed: Option<&crate::CommitCallback>,
    ) -> Result<u64, StorageError> {
        self.append_stream_with_progress(
            receiver,
            HdfsAppendTarget::Prepared(state),
            AppendCompletion::PartialUpTo(state.expected_size()),
            bytes_counter,
            on_committed,
        )
        .await
    }

    /// Atomically publish one completed HDFS resume temporary file.
    ///
    /// # Errors
    ///
    /// Returns an error unless the temporary path is a regular file with the
    /// exact expected length, or when native same-root rename fails.
    pub async fn commit_tail_resume(
        &self,
        part_path: &std::path::Path,
        final_path: &std::path::Path,
        expected_size: u64,
    ) -> Result<(), StorageError> {
        let metadata = self.get_metadata(part_path).await?;
        if metadata.is_dir || metadata.size != expected_size {
            return Err(StorageError::OperationError(format!(
                "HDFS resume temporary file is not commit-ready: size={}, expected={expected_size}: {}",
                metadata.size,
                part_path.display()
            )));
        }
        self.rename(part_path, final_path).await
    }

    pub(crate) async fn commit_prepared_tail(
        &self,
        state: &HdfsPreparedTransfer,
        final_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.commit_tail_resume(state.part_path(), final_path, state.expected_size())
            .await
    }

    async fn rebuild_resume_file(
        &self,
        part_path: &std::path::Path,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<(), StorageError> {
        if let Ok(metadata) = self.get_metadata(part_path).await
            && metadata.is_dir
        {
            return Err(StorageError::InvalidPath(format!(
                "HDFS resume temporary path is a directory: {}",
                part_path.display()
            )));
        }
        self.write_file(part_path, bytes::Bytes::new(), mode, replication)
            .await
    }

    /// Stream a fresh HDFS file from bounded chunks using one sequential writer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid ranges, gaps beyond the configured reorder
    /// window, size mismatch, path escape, or upstream create/write/close failure.
    pub async fn write_stream(
        &self,
        receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        expected_size: u64,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<u64, StorageError> {
        self.write_data(
            receiver,
            relative_path,
            expected_size,
            mode,
            replication,
            None,
        )
        .await
    }

    pub(crate) async fn write_file(
        &self,
        relative_path: &std::path::Path,
        data: bytes::Bytes,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<(), StorageError> {
        let size = u64::try_from(data.len())
            .map_err(|_| config_error("HDFS write data length does not fit u64"))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(crate::DataChunk { offset: 0, data })
            .await
            .map_err(|_| StorageError::OperationError("HDFS write channel closed".to_string()))?;
        drop(sender);
        self.write_data(receiver, relative_path, size, mode, replication, None)
            .await
            .map(|_| ())
    }

    /// Delete one regular file below the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error for traversal, missing paths, directories, upstream
    /// failures, or an upstream `false` result.
    pub async fn delete_file(&self, relative_path: &std::path::Path) -> Result<(), StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
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
        if !metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS directory delete target is a file: {}",
                relative_path.display()
            )));
        }
        self.delete_resolved(relative_path, true).await
    }

    /// Explicit HDFS-specific storage-root cleanup for isolated lifecycle code.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is missing or HDFS rejects deletion.
    pub async fn delete_storage_root(&self) -> Result<(), StorageError> {
        self.delete_resolved(std::path::Path::new(""), true).await
    }

    /// Atomically rename a file or directory below the configured root.
    ///
    /// Missing destination parents are created with mode `0o755`. Existing
    /// destinations are replaced using native HDFS overwrite semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for either root, path escape, a missing source, a
    /// directory moved below itself, or an upstream rename failure.
    pub async fn rename(
        &self,
        from: &std::path::Path,
        to: &std::path::Path,
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
        if source.is_dir && to.starts_with(from) {
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
            .rename(&from_path, &to_path, true)
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

    async fn consume_sequential_chunks(
        &self,
        writer: &mut hdfs_native::file::FileWriter,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::DataChunk>,
        context: SequentialWriteContext<'_>,
    ) -> Result<u64, StorageError> {
        let result = self
            .consume_sequential_chunk_data(writer, receiver, context)
            .await;
        if result.is_err() {
            receiver.close();
        }
        let close_result = Box::pin(writer.close()).await.map_err(|error| {
            hdfs_operation_error("close writer", Some(context.relative_path), &error)
        });
        match (result, close_result) {
            (Ok(offset), Ok(())) => Ok(offset),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(operation), Err(close)) => Err(StorageError::OperationError(format!(
                "HDFS write failed: {operation}; additionally failed to close writer: {close}"
            ))),
        }
    }

    async fn consume_sequential_chunk_data(
        &self,
        writer: &mut hdfs_native::file::FileWriter,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::DataChunk>,
        context: SequentialWriteContext<'_>,
    ) -> Result<u64, StorageError> {
        let window = self.transfer_concurrency.write();
        let mut pending = BTreeMap::<u64, bytes::Bytes>::new();
        let mut next_offset = context.start_offset;
        while let Some(chunk) = receiver.recv().await {
            validate_write_chunk(&pending, next_offset, context.expected_size, &chunk)?;
            if !chunk.data.is_empty() {
                pending.insert(chunk.offset, chunk.data);
            }
            while let Some(data) = pending.remove(&next_offset) {
                let length = u64::try_from(data.len())
                    .map_err(|_| config_error("HDFS write chunk length does not fit u64"))?;
                let written = Box::pin(writer.write_bytes(data)).await.map_err(|error| {
                    hdfs_operation_error("write data", Some(context.relative_path), &error)
                })?;
                if u64::try_from(written).ok() != Some(length) {
                    return Err(StorageError::OperationError(format!(
                        "HDFS short write: expected {length} bytes, wrote {written}"
                    )));
                }
                next_offset = next_offset
                    .checked_add(length)
                    .ok_or_else(|| config_error("HDFS write offset overflow"))?;
                if let Some(counter) = context.bytes_counter {
                    counter.fetch_add(length, Ordering::Relaxed);
                }
            }
            if pending.len() > window {
                return Err(StorageError::OperationError(format!(
                    "HDFS write gap at offset {next_offset} exceeded reorder window {window}"
                )));
            }
        }
        validate_sequential_end(
            next_offset,
            context.expected_size,
            !pending.is_empty(),
            context.require_final_size,
        )?;
        Ok(next_offset)
    }

    async fn run_recursive_scan(
        self,
        root: PathBuf,
        max_depth: Option<usize>,
        concurrency: usize,
        sender: async_channel::Sender<HdfsScanEvent>,
    ) {
        let mut pending = VecDeque::from([(root, 0_usize)]);
        let mut active = futures::stream::FuturesUnordered::new();
        loop {
            while active.len() < concurrency {
                let Some((directory, depth)) = pending.pop_front() else {
                    break;
                };
                let storage = self.clone();
                active.push(async move {
                    let result = storage.list_directory(&directory).await;
                    (directory, depth, result)
                });
            }
            let Some((directory, depth, result)) = active.next().await else {
                break;
            };
            match result {
                Ok(entries) => {
                    for entry in entries {
                        let descend = entry.is_dir
                            && max_depth.is_none_or(|limit| depth.saturating_add(1) < limit);
                        let child_path = entry.relative_path.clone();
                        if sender.send(HdfsScanEvent::Entry(entry)).await.is_err() {
                            return;
                        }
                        if descend {
                            pending.push_back((child_path, depth.saturating_add(1)));
                        }
                    }
                }
                Err(error) => {
                    if sender
                        .send(HdfsScanEvent::Error {
                            path: directory,
                            error,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

fn confirmed_append_range(
    start_offset: u64,
    written_end: u64,
    persisted_size: u64,
) -> Result<Option<(u64, u64)>, StorageError> {
    if persisted_size != written_end {
        return Err(StorageError::OperationError(format!(
            "HDFS append persisted length {persisted_size} does not match written end {written_end}"
        )));
    }
    let length = written_end.checked_sub(start_offset).ok_or_else(|| {
        StorageError::OperationError(
            "HDFS append written end cannot precede its starting offset".to_string(),
        )
    })?;
    Ok((length > 0).then_some((start_offset, length)))
}

fn settle_append_progress(
    start_offset: u64,
    write_result: Result<u64, StorageError>,
    persisted_size: Option<u64>,
    on_committed: Option<&crate::CommitCallback>,
) -> Result<u64, StorageError> {
    let written_end = write_result?;
    let persisted_size = persisted_size.ok_or_else(|| {
        StorageError::OperationError(
            "HDFS append completed without persisted length confirmation".to_string(),
        )
    })?;
    if let Some((offset, length)) =
        confirmed_append_range(start_offset, written_end, persisted_size)?
        && let Some(callback) = on_committed
    {
        callback(offset, length);
    }
    Ok(written_end)
}
