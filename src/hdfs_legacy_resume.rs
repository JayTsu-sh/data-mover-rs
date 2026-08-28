//! Compatibility orchestration for the legacy HDFS resume and wire APIs.
//!
//! The public entry points and `StreamHandle` wire type remain in
//! `storage_enum`; this module owns the HDFS tail-transfer invariants used to
//! implement those compatibility surfaces.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::StorageError;
use crate::hdfs::{HDFSStorage, HdfsPreparedTransfer};
use crate::hdfs_transfer_mapping::hdfs_write_options;
use crate::storage_copy_pipeline::await_copy_pipeline;
use crate::storage_enum::COPY_PIPELINE_CAPACITY;
use crate::{
    CommitCallback, CopyOptions, DataChunk, EntryEnum, Result, ResumeContext, StorageEnum,
    StreamHandle,
};

pub(crate) fn prepared_from_handle(
    part_path: PathBuf,
    prefix_len: u64,
    expected_size: u64,
    entry: &EntryEnum,
) -> Result<HdfsPreparedTransfer> {
    let (mode, replication) = hdfs_write_options(entry);
    HdfsPreparedTransfer::new(
        part_path,
        prefix_len,
        expected_size,
        entry.get_size(),
        mode,
        replication,
    )
}

pub(crate) async fn prepare(
    storage: &HDFSStorage,
    entry: &EntryEnum,
    part_path: &Path,
    resume: bool,
) -> Result<(Vec<(u64, u64)>, StreamHandle)> {
    let size = entry.get_size();
    let (mode, replication) = hdfs_write_options(entry);
    let state = storage
        .prepare_tail_transfer(part_path, size, resume, mode, replication)
        .await?;
    let missing = (state.prefix_len() < size).then_some((state.prefix_len(), size));
    Ok((
        missing.into_iter().collect(),
        StreamHandle::Hdfs {
            part_path: state.part_path().to_path_buf(),
            prefix_len: state.prefix_len(),
            expected_size: state.expected_size(),
        },
    ))
}

pub(crate) async fn write(
    storage: &HDFSStorage,
    entry: &EntryEnum,
    rx: mpsc::Receiver<DataChunk>,
    handle: StreamHandle,
    bytes_counter: Option<Arc<AtomicU64>>,
    on_committed: CommitCallback,
) -> Result<()> {
    let StreamHandle::Hdfs {
        part_path,
        prefix_len,
        expected_size,
    } = handle
    else {
        return Err(StorageError::MismatchedType);
    };
    let state = prepared_from_handle(part_path, prefix_len, expected_size, entry)?;
    storage
        .append_prepared_tail(rx, &state, bytes_counter.as_ref(), Some(&on_committed))
        .await
        .map(|_| ())
}

pub(crate) async fn commit(
    dest: &StorageEnum,
    entry: &EntryEnum,
    size: u64,
    handle: StreamHandle,
) -> Result<()> {
    let StorageEnum::HDFS(storage) = dest else {
        return Err(StorageError::MismatchedType);
    };
    let StreamHandle::Hdfs {
        part_path,
        prefix_len,
        expected_size,
    } = handle
    else {
        return Err(StorageError::MismatchedType);
    };
    if size != expected_size {
        return Err(StorageError::OperationError(format!(
            "HDFS resume commit size {size} does not match handle size {expected_size}"
        )));
    }
    let state = prepared_from_handle(part_path, prefix_len, expected_size, entry)?;
    storage
        .commit_prepared_tail(&state, entry.get_relative_path())
        .await?;
    StorageEnum::apply_copied_metadata(dest, entry).await
}

async fn verify_partial_integrity(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    part_path: &Path,
    enabled: bool,
) -> Result<()> {
    if crate::hdfs_transfer_integrity::partial_matches(from, to, entry, part_path, enabled).await? {
        return Ok(());
    }
    if let StorageEnum::HDFS(destination) = to {
        let _ = destination.delete_file(part_path).await;
    }
    Err(StorageError::OperationError(
        "integrity check failed: source and HDFS partial hashes differ".to_string(),
    ))
}

pub(crate) async fn copy_file_resumable(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    options: CopyOptions,
    resume: ResumeContext,
) -> Result<()> {
    ensure_not_cancelled(options.cancel.as_ref())?;
    let StorageEnum::HDFS(storage) = to else {
        return Err(StorageError::MismatchedType);
    };
    let size = entry.get_size();
    let ResumeContext {
        part_relative_path,
        missing_intervals,
        on_committed,
    } = resume;
    let (actual_missing, handle) = prepare(storage, entry, &part_relative_path, true).await?;
    if missing_intervals != actual_missing {
        return Err(StorageError::OperationError(format!(
            "HDFS resume intervals do not match persistent tail: requested={missing_intervals:?}, actual={actual_missing:?}"
        )));
    }
    let CopyOptions {
        qos,
        enable_integrity_check,
        is_source_reserved,
        bytes_counter,
        cancel,
    } = options;
    let (rx, read_task) = StorageEnum::read_chunk_stream(
        from,
        entry,
        Some(actual_missing),
        qos,
        false,
        COPY_PIPELINE_CAPACITY,
    );
    let write_storage = storage.clone();
    let write_entry = entry.clone();
    let write_handle = handle.clone();
    let write_task = tokio::spawn(async move {
        write(
            &write_storage,
            &write_entry,
            rx,
            write_handle,
            bytes_counter,
            on_committed,
        )
        .await
    });
    await_copy_pipeline(read_task, write_task, cancel.as_ref()).await?;
    ensure_not_cancelled(cancel.as_ref())?;
    verify_partial_integrity(from, to, entry, &part_relative_path, enable_integrity_check).await?;
    ensure_not_cancelled(cancel.as_ref())?;
    commit(to, entry, size, handle).await?;
    if !is_source_reserved {
        from.delete_file(entry).await?;
    }
    Ok(())
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
    use super::*;
    use crate::HDFSEntry;

    #[test]
    fn wire_handle_conversion_rejects_impossible_prefix_or_entry_size() {
        let entry = EntryEnum::HDFS(HDFSEntry {
            name: "file.bin".to_string(),
            relative_path: PathBuf::from("file.bin"),
            is_dir: false,
            size: 16,
            mtime: 0,
            atime: 0,
            mode: 0o644,
            owner: String::new(),
            group: String::new(),
            replication: None,
            block_size: None,
            extension: Some("bin".to_string()),
        });

        assert!(prepared_from_handle(PathBuf::from("file.part"), 4, 16, &entry).is_ok());
        assert!(prepared_from_handle(PathBuf::from("file.part"), 17, 16, &entry).is_err());
        assert!(prepared_from_handle(PathBuf::from("file.part"), 4, 15, &entry).is_err());
    }
}
