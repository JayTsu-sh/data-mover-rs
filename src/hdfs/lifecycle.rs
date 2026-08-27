impl HDFSStorage {
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
        part_path: &Path,
        expected_size: u64,
        resume: bool,
    ) -> Result<u64, StorageError> {
        self.prepare_tail_transfer(part_path, expected_size, resume, 0o644, None)
            .await
            .map(|state| state.prefix_len())
    }

    pub(crate) async fn prepare_tail_transfer(
        &self,
        part_path: &Path,
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
        let mode = if resume {
            HdfsResumeMode::Auto
        } else {
            HdfsResumeMode::Restart
        };
        self.prepare_requested_tail_transfer(requested, mode).await
    }

    /// Prepare a deterministic, source-bound HDFS tail transfer.
    ///
    /// # Errors
    ///
    /// Returns an error when the derived partial path is unsafe, inaccessible,
    /// a directory, or cannot be rebuilt for this request.
    pub async fn prepare_stable_tail_transfer(
        &self,
        request: &HdfsTransferRequest,
        resume: bool,
    ) -> Result<HdfsPreparedTransfer, StorageError> {
        let mode = if resume {
            HdfsResumeMode::Auto
        } else {
            HdfsResumeMode::Restart
        };
        self.prepare_staged_tail_transfer(request, mode).await
    }

    /// Prepare one stable HDFS staged transfer under an explicit resume mode.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe/directory state, inaccessible metadata,
    /// failed rebuilds, or when `Require` cannot use an existing partial.
    pub async fn prepare_staged_tail_transfer(
        &self,
        request: &HdfsTransferRequest,
        mode: HdfsResumeMode,
    ) -> Result<HdfsPreparedTransfer, StorageError> {
        let requested = HdfsPreparedTransfer::from_stable_request(request, 0)?;
        self.prepare_requested_tail_transfer(requested, mode).await
    }

    async fn prepare_requested_tail_transfer(
        &self,
        requested: HdfsPreparedTransfer,
        mode: HdfsResumeMode,
    ) -> Result<HdfsPreparedTransfer, StorageError> {
        self.resolve_path(requested.part_path())?;
        let partial = match self.get_metadata(requested.part_path()).await {
            Ok(metadata) if metadata.is_dir => HdfsPartialObservation::Directory,
            Ok(metadata) => HdfsPartialObservation::File(metadata.size),
            Err(StorageError::FileNotFound(_)) => HdfsPartialObservation::Missing,
            Err(error) => return Err(error),
        };
        let prefix_len = match plan_staged_prepare(mode, partial, requested.expected_size())? {
            HdfsPrepareAction::Resume(prefix_len) => prefix_len,
            HdfsPrepareAction::Rebuild => {
                self.create_empty_resume_file(
                    requested.part_path(),
                    requested.mode(),
                    requested.replication(),
                )
                .await?;
                0
            }
        };
        requested.with_prefix(prefix_len)
    }

    async fn create_empty_resume_file(
        &self,
        part_path: &Path,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<(), StorageError> {
        self.write_file(part_path, bytes::Bytes::new(), mode, replication)
            .await
    }
}
