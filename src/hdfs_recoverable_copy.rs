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
        Self::verify_hdfs_partial_integrity(
            from,
            to,
            entry,
            state.part_path(),
            enable_integrity_check,
        )
        .await?;
        ensure_not_cancelled(cancel.as_ref())?;
        destination
            .commit_prepared_tail(&state, request.final_path())
            .await?;
        Self::complete_copied_entry(from, to, entry, is_source_reserved).await
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
    use std::sync::Arc;

    use super::*;
    use crate::{CreateStorageOptions, HDFSEntry, create_storage};

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
}
