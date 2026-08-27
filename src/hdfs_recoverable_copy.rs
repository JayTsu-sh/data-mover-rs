use tokio_util::sync::CancellationToken;

use crate::hdfs::{
    HDFSStorage, HdfsCancellationDisposition, HdfsExistingFinalPolicy, HdfsPreparedTransfer,
    HdfsResumeMode, HdfsTransferRequest,
};
use crate::storage_enum::{COPY_PIPELINE_CAPACITY, await_copy_pipeline};
use crate::{CommitCallback, CopyOptions, EntryEnum, Result, StorageEnum, error::StorageError};

/// HDFS-specific inputs for the explicit recoverable-copy entry point.
#[derive(Clone)]
pub struct HdfsRecoverableCopyOptions {
    transfer_identity: String,
    resume_mode: HdfsResumeMode,
    cancellation_disposition: HdfsCancellationDisposition,
    existing_final_policy: HdfsExistingFinalPolicy,
    on_committed: CommitCallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingFinalDecision {
    Continue,
    Accept,
    Conflict,
}

fn decide_existing_final(
    policy: HdfsExistingFinalPolicy,
    final_size: Option<u64>,
    expected_size: u64,
    integrity_matches: bool,
) -> ExistingFinalDecision {
    match (policy, final_size) {
        (HdfsExistingFinalPolicy::Overwrite, _) | (_, None) => ExistingFinalDecision::Continue,
        (HdfsExistingFinalPolicy::VerifyOrConflict, Some(size))
            if size == expected_size && integrity_matches =>
        {
            ExistingFinalDecision::Accept
        }
        (
            HdfsExistingFinalPolicy::FailIfExists | HdfsExistingFinalPolicy::VerifyOrConflict,
            Some(_),
        ) => ExistingFinalDecision::Conflict,
    }
}

impl HdfsRecoverableCopyOptions {
    /// Create options in the default [`HdfsResumeMode::Auto`] mode.
    #[must_use]
    pub fn new(transfer_identity: impl Into<String>, on_committed: CommitCallback) -> Self {
        Self {
            transfer_identity: transfer_identity.into(),
            resume_mode: HdfsResumeMode::Auto,
            cancellation_disposition: HdfsCancellationDisposition::Preserve,
            existing_final_policy: HdfsExistingFinalPolicy::Overwrite,
            on_committed,
        }
    }

    /// Select an explicit HDFS preparation mode.
    #[must_use]
    pub const fn with_resume_mode(mut self, resume_mode: HdfsResumeMode) -> Self {
        self.resume_mode = resume_mode;
        self
    }

    /// Select how cancellation treats the current request's trusted partial.
    #[must_use]
    pub const fn with_cancellation_disposition(
        mut self,
        disposition: HdfsCancellationDisposition,
    ) -> Self {
        self.cancellation_disposition = disposition;
        self
    }

    /// Return the selected cancellation disposition.
    #[must_use]
    pub const fn cancellation_disposition(&self) -> HdfsCancellationDisposition {
        self.cancellation_disposition
    }

    /// Select how an existing final path is handled.
    #[must_use]
    pub const fn with_existing_final_policy(mut self, policy: HdfsExistingFinalPolicy) -> Self {
        self.existing_final_policy = policy;
        self
    }

    /// Return the selected existing-final policy.
    #[must_use]
    pub const fn existing_final_policy(&self) -> HdfsExistingFinalPolicy {
        self.existing_final_policy
    }
}

impl StorageEnum {
    /// Copy one file through the stable HDFS staged lifecycle.
    ///
    /// [`Self::copy_file`] uses this lifecycle in `Auto` mode for HDFS destinations.
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
        let (destination, request) = recoverable_request(to, entry, &recovery)?;
        if try_complete_existing_final(
            from,
            to,
            entry,
            &destination,
            &request,
            &recovery,
            &copy_options,
        )
        .await?
        {
            return Ok(());
        }
        let state = destination
            .prepare_staged_tail_transfer(&request, recovery.resume_mode)
            .await?;
        let CopyOptions {
            qos,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
            cancel,
        } = copy_options;
        let result = async {
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
            let commit = commit_prepared_by_policy(
                &destination,
                &state,
                &request,
                recovery.existing_final_policy,
            )
            .await;
            if let Err(commit_error) = commit {
                if resolve_existing_final(
                    from,
                    to,
                    entry,
                    &request,
                    recovery.existing_final_policy,
                    enable_integrity_check,
                )
                .await?
                {
                    revalidate_recoverable_source(from, entry, &request, cancel.as_ref()).await?;
                    return complete_matching_existing_final(
                        from,
                        to,
                        entry,
                        &destination,
                        &request,
                        is_source_reserved,
                    )
                    .await;
                }
                return Err(commit_error);
            }
            Self::complete_copied_entry(from, to, entry, is_source_reserved).await
        }
        .await;
        if matches!(result, Err(StorageError::Cancelled))
            && recovery.cancellation_disposition == HdfsCancellationDisposition::Discard
        {
            return preserve_cancellation_after_discard(
                result,
                destination.discard_prepared_tail(&state).await,
                state.part_path(),
            );
        }
        result
    }
}

async fn try_complete_existing_final(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    destination: &HDFSStorage,
    request: &HdfsTransferRequest,
    recovery: &HdfsRecoverableCopyOptions,
    copy_options: &CopyOptions,
) -> Result<bool> {
    if !resolve_existing_final(
        from,
        to,
        entry,
        request,
        recovery.existing_final_policy,
        copy_options.enable_integrity_check,
    )
    .await?
    {
        return Ok(false);
    }
    revalidate_recoverable_source(from, entry, request, copy_options.cancel.as_ref()).await?;
    ensure_not_cancelled(copy_options.cancel.as_ref())?;
    complete_matching_existing_final(
        from,
        to,
        entry,
        destination,
        request,
        copy_options.is_source_reserved,
    )
    .await?;
    Ok(true)
}

async fn complete_matching_existing_final(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    destination: &HDFSStorage,
    request: &HdfsTransferRequest,
    is_source_reserved: bool,
) -> Result<()> {
    StorageEnum::apply_copied_metadata(to, entry).await?;
    destination.discard_recoverable_state(request).await?;
    if !is_source_reserved {
        from.delete_file(entry).await?;
    }
    Ok(())
}

async fn commit_prepared_by_policy(
    destination: &HDFSStorage,
    state: &HdfsPreparedTransfer,
    request: &HdfsTransferRequest,
    policy: HdfsExistingFinalPolicy,
) -> Result<()> {
    match policy {
        HdfsExistingFinalPolicy::Overwrite => {
            destination
                .commit_prepared_tail(state, request.final_path())
                .await
        }
        HdfsExistingFinalPolicy::VerifyOrConflict | HdfsExistingFinalPolicy::FailIfExists => {
            destination
                .commit_prepared_tail_if_absent(state, request.final_path())
                .await
        }
    }
}

fn preserve_cancellation_after_discard(
    cancellation: Result<()>,
    discard: Result<()>,
    partial_path: &std::path::Path,
) -> Result<()> {
    if let Err(error) = discard {
        tracing::warn!(
            partial = %partial_path.display(),
            %error,
            "failed to discard cancelled HDFS transfer partial"
        );
    }
    cancellation
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

fn recoverable_request(
    to: &StorageEnum,
    entry: &EntryEnum,
    recovery: &HdfsRecoverableCopyOptions,
) -> Result<(HDFSStorage, HdfsTransferRequest)> {
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
    Ok((destination.clone(), request))
}

async fn resolve_existing_final(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    request: &HdfsTransferRequest,
    policy: HdfsExistingFinalPolicy,
    enable_integrity_check: bool,
) -> Result<bool> {
    if policy == HdfsExistingFinalPolicy::Overwrite {
        return Ok(false);
    }
    let StorageEnum::HDFS(destination) = to else {
        return Err(StorageError::OperationError(
            "existing-final resolution requires an HDFS destination".to_string(),
        ));
    };
    let final_metadata = match destination.get_metadata(request.final_path()).await {
        Ok(metadata) => metadata,
        Err(StorageError::FileNotFound(_)) => return Ok(false),
        Err(error) => return Err(error),
    };
    if final_metadata.is_dir {
        return Err(existing_final_conflict(request));
    }
    let integrity_matches = if policy == HdfsExistingFinalPolicy::VerifyOrConflict
        && final_metadata.size == request.expected_size()
    {
        StorageEnum::hdfs_partial_integrity_matches(
            from,
            to,
            entry,
            request.final_path(),
            enable_integrity_check,
        )
        .await?
    } else {
        false
    };
    match decide_existing_final(
        policy,
        Some(final_metadata.size),
        request.expected_size(),
        integrity_matches,
    ) {
        ExistingFinalDecision::Accept => Ok(true),
        ExistingFinalDecision::Conflict => Err(existing_final_conflict(request)),
        ExistingFinalDecision::Continue => Ok(false),
    }
}

fn existing_final_conflict(request: &HdfsTransferRequest) -> StorageError {
    StorageError::OperationError(format!(
        "HDFS final path conflicts with recoverable transfer: {}",
        request.final_path().display()
    ))
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

    #[test]
    fn recoverable_copy_preserves_cancelled_state_unless_discard_is_explicit() {
        let callback: CommitCallback = Arc::new(|_, _| {});
        let default = HdfsRecoverableCopyOptions::new("transfer", callback.clone());
        assert_eq!(
            default.cancellation_disposition(),
            crate::hdfs::HdfsCancellationDisposition::Preserve
        );

        let discard = HdfsRecoverableCopyOptions::new("transfer", callback)
            .with_cancellation_disposition(crate::hdfs::HdfsCancellationDisposition::Discard);
        assert_eq!(
            discard.cancellation_disposition(),
            crate::hdfs::HdfsCancellationDisposition::Discard
        );
    }

    #[test]
    fn recoverable_copy_overwrites_existing_final_unless_another_policy_is_selected() {
        let callback: CommitCallback = Arc::new(|_, _| {});
        let default = HdfsRecoverableCopyOptions::new("transfer", callback.clone());
        assert_eq!(
            default.existing_final_policy(),
            crate::hdfs::HdfsExistingFinalPolicy::Overwrite
        );

        let conflict = HdfsRecoverableCopyOptions::new("transfer", callback)
            .with_existing_final_policy(crate::hdfs::HdfsExistingFinalPolicy::FailIfExists);
        assert_eq!(
            conflict.existing_final_policy(),
            crate::hdfs::HdfsExistingFinalPolicy::FailIfExists
        );
    }

    #[test]
    fn existing_final_policy_decision_matrix_is_explicit() {
        use ExistingFinalDecision::{Accept, Conflict, Continue};
        use HdfsExistingFinalPolicy::{FailIfExists, Overwrite, VerifyOrConflict};

        assert_eq!(decide_existing_final(Overwrite, None, 8, false), Continue);
        assert_eq!(
            decide_existing_final(Overwrite, Some(99), 8, false),
            Continue
        );
        assert_eq!(
            decide_existing_final(FailIfExists, None, 8, false),
            Continue
        );
        assert_eq!(
            decide_existing_final(FailIfExists, Some(8), 8, true),
            Conflict
        );
        assert_eq!(
            decide_existing_final(VerifyOrConflict, None, 8, false),
            Continue
        );
        assert_eq!(
            decide_existing_final(VerifyOrConflict, Some(7), 8, true),
            Conflict
        );
        assert_eq!(
            decide_existing_final(VerifyOrConflict, Some(8), 8, false),
            Conflict
        );
        assert_eq!(
            decide_existing_final(VerifyOrConflict, Some(8), 8, true),
            Accept
        );
    }

    #[test]
    fn discard_failure_never_masks_cancellation() {
        let result = preserve_cancellation_after_discard(
            Err(StorageError::Cancelled),
            Err(StorageError::OperationError("delete failed".to_string())),
            Path::new(".data-mover-request.part"),
        );
        assert!(matches!(result, Err(StorageError::Cancelled)));
    }

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
