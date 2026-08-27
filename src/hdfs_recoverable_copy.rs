use tokio_util::sync::CancellationToken;

use crate::hdfs::{HDFSStorage, HdfsPreparedTransfer, HdfsResumeMode, HdfsTransferRequest};
use crate::storage_enum::{COPY_PIPELINE_CAPACITY, await_copy_pipeline};
use crate::{CommitCallback, CopyOptions, EntryEnum, Result, StorageEnum, error::StorageError};

/// HDFS-specific inputs for the explicit recoverable-copy entry point.
#[derive(Clone)]
pub struct HdfsRecoverableCopyOptions {
    transfer_identity: String,
    resume_mode: HdfsResumeMode,
    on_committed: CommitCallback,
}

impl HdfsRecoverableCopyOptions {
    /// Create options in the default [`HdfsResumeMode::Auto`] mode.
    #[must_use]
    pub fn new(transfer_identity: impl Into<String>, on_committed: CommitCallback) -> Self {
        Self {
            transfer_identity: transfer_identity.into(),
            resume_mode: HdfsResumeMode::Auto,
            on_committed,
        }
    }

    /// Select an explicit HDFS preparation mode.
    #[must_use]
    pub const fn with_resume_mode(mut self, resume_mode: HdfsResumeMode) -> Self {
        self.resume_mode = resume_mode;
        self
    }
}

impl StorageEnum {
    /// Copy one file through the stable HDFS staged lifecycle.
    ///
    /// This opt-in entry point does not change the behavior of [`Self::copy_file`].
    ///
    /// # Errors
    ///
    /// Returns an error for a non-HDFS destination, invalid stable request,
    /// preparation/read/write/verification/commit failure, or cancellation.
    pub async fn copy_file_hdfs_recoverable(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        copy_options: CopyOptions,
        recovery: HdfsRecoverableCopyOptions,
    ) -> Result<()> {
        ensure_not_cancelled(copy_options.cancel.as_ref())?;
        let (destination, request, state) = prepare_recoverable(to, entry, &recovery).await?;
        let CopyOptions {
            qos,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
            cancel,
        } = copy_options;
        run_recoverable_pipeline(
            from,
            entry,
            &destination,
            &state,
            qos,
            bytes_counter,
            &recovery.on_committed,
            cancel.as_ref(),
        )
        .await?;
        let integrity_matches = Self::hdfs_partial_integrity_matches(
            from,
            to,
            entry,
            state.part_path(),
            enable_integrity_check,
        )
        .await?;
        if !integrity_matches {
            revalidate_recoverable_source(from, entry, &request, cancel.as_ref()).await?;
            let _ = destination.delete_file(state.part_path()).await;
            return Err(StorageError::OperationError(
                "integrity check failed: source and HDFS partial hashes differ".to_string(),
            ));
        }
        revalidate_recoverable_source(from, entry, &request, cancel.as_ref()).await?;
        ensure_not_cancelled(cancel.as_ref())?;
        destination
            .commit_prepared_tail(&state, request.final_path())
            .await?;
        Self::complete_copied_entry(from, to, entry, is_source_reserved).await
    }
}

async fn revalidate_recoverable_source(
    source: &StorageEnum,
    entry: &EntryEnum,
    request: &HdfsTransferRequest,
    cancel: Option<&CancellationToken>,
) -> Result<()> {
    ensure_not_cancelled(cancel)?;
    let current_source = source_metadata_for_revalidation(source, entry).await;
    ensure_not_cancelled(cancel)?;
    request.validate_source_fingerprint(&crate::hdfs_source_fingerprint(&current_source?))
}

async fn source_metadata_for_revalidation(
    source: &StorageEnum,
    entry: &EntryEnum,
) -> Result<EntryEnum> {
    match (source, entry) {
        (StorageEnum::S3(storage), EntryEnum::S3(entry)) => {
            let version_id = if entry.is_latest {
                None
            } else {
                entry.version_id.as_deref()
            };
            storage
                .get_metadata_version(&entry.relative_path, version_id)
                .await
        }
        _ => source.get_metadata(entry.get_relative_path()).await,
    }
}

async fn prepare_recoverable(
    to: &StorageEnum,
    entry: &EntryEnum,
    recovery: &HdfsRecoverableCopyOptions,
) -> Result<(HDFSStorage, HdfsTransferRequest, HdfsPreparedTransfer)> {
    let StorageEnum::HDFS(destination) = to else {
        return Err(StorageError::OperationError(
            "copy_file_hdfs_recoverable requires an HDFS destination".to_string(),
        ));
    };
    let request = crate::hdfs_transfer_request(
        entry,
        &recovery.transfer_identity,
        entry.get_relative_path().to_path_buf(),
    )?;
    let state = destination
        .prepare_staged_tail_transfer(&request, recovery.resume_mode)
        .await?;
    Ok((destination.clone(), request, state))
}

#[allow(clippy::too_many_arguments)]
async fn run_recoverable_pipeline(
    from: &StorageEnum,
    entry: &EntryEnum,
    destination: &HDFSStorage,
    state: &HdfsPreparedTransfer,
    qos: Option<crate::QosManager>,
    bytes_counter: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    on_committed: &CommitCallback,
    cancel: Option<&CancellationToken>,
) -> Result<()> {
    let intervals = state.missing_tail().into_iter().collect();
    let (receiver, read_task) = StorageEnum::read_chunk_stream(
        from,
        entry,
        Some(intervals),
        qos,
        false,
        COPY_PIPELINE_CAPACITY,
    );
    let write_destination = destination.clone();
    let write_state = state.clone();
    let on_committed = on_committed.clone();
    let write_task = tokio::spawn(async move {
        write_destination
            .append_prepared_tail(
                receiver,
                &write_state,
                bytes_counter.as_ref(),
                Some(&on_committed),
            )
            .await
    });
    await_copy_pipeline(read_task, write_task, cancel).await?;
    ensure_not_cancelled(cancel)
}

fn ensure_not_cancelled(cancel: Option<&CancellationToken>) -> Result<()> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        Err(StorageError::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{CreateStorageOptions, HDFSEntry, create_storage};

    static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn test_directory(name: &str) -> std::path::PathBuf {
        let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-hdfs-revalidation-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("create source root: {error}"));
        path
    }

    #[tokio::test]
    async fn explicit_hdfs_recoverable_copy_rejects_other_destinations() {
        let root = "/tmp/data-mover-hdfs-recoverable-wrong-destination";
        let destination = create_storage(root, CreateStorageOptions::new(None, true))
            .await
            .unwrap_or_else(|error| panic!("create local destination: {error}"));
        let entry = EntryEnum::HDFS(HDFSEntry {
            name: "file.bin".to_string(),
            relative_path: "file.bin".into(),
            is_dir: false,
            size: 0,
            mtime: 0,
            atime: 0,
            mode: 0o644,
            owner: String::new(),
            group: String::new(),
            replication: None,
            block_size: None,
            extension: Some("bin".to_string()),
        });
        let callback: CommitCallback = Arc::new(|_, _| {});
        let result = StorageEnum::copy_file_hdfs_recoverable(
            &destination,
            &destination,
            &entry,
            CopyOptions::default(),
            HdfsRecoverableCopyOptions::new("transfer", callback),
        )
        .await;
        assert!(matches!(result, Err(StorageError::OperationError(_))));
    }

    #[tokio::test]
    async fn source_revalidation_preserves_metadata_read_failure() {
        let root = test_directory("metadata-failure");
        let source_path = root.join("source.bin");
        tokio::fs::write(&source_path, b"source")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let source = create_storage(
            root.to_str()
                .unwrap_or_else(|| panic!("source root is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create source storage: {error}"));
        let entry = source
            .get_metadata(Path::new("source.bin"))
            .await
            .unwrap_or_else(|error| panic!("read source metadata: {error}"));
        let request =
            crate::hdfs_transfer_request(&entry, "transfer", Path::new("source.bin").into())
                .unwrap_or_else(|error| panic!("create transfer request: {error}"));
        tokio::fs::remove_file(source_path)
            .await
            .unwrap_or_else(|error| panic!("remove source: {error}"));

        let result = revalidate_recoverable_source(&source, &entry, &request, None).await;

        assert!(matches!(result, Err(StorageError::FileNotFound(_))));
        std::fs::remove_dir_all(root).unwrap_or_else(|error| panic!("remove source root: {error}"));
    }

    #[tokio::test]
    async fn cancellation_precedes_source_metadata_read_failure() {
        let root = test_directory("cancelled");
        let source_path = root.join("source.bin");
        tokio::fs::write(&source_path, b"source")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let source = create_storage(
            root.to_str()
                .unwrap_or_else(|| panic!("source root is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create source storage: {error}"));
        let entry = source
            .get_metadata(Path::new("source.bin"))
            .await
            .unwrap_or_else(|error| panic!("read source metadata: {error}"));
        let request =
            crate::hdfs_transfer_request(&entry, "transfer", Path::new("source.bin").into())
                .unwrap_or_else(|error| panic!("create transfer request: {error}"));
        tokio::fs::remove_file(source_path)
            .await
            .unwrap_or_else(|error| panic!("remove source: {error}"));
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = revalidate_recoverable_source(&source, &entry, &request, Some(&cancel)).await;

        assert!(matches!(result, Err(StorageError::Cancelled)));
        std::fs::remove_dir_all(root).unwrap_or_else(|error| panic!("remove source root: {error}"));
    }
}
