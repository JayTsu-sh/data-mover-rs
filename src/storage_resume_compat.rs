//! Compatibility orchestration for the public resume/wire APIs.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::StorageError;
use crate::pipeline_primitives::WriteProgress;
use crate::s3::S3Storage;
use crate::storage_copy_pipeline::COPY_PIPELINE_CAPACITY;
use crate::storage_copy_pipeline::await_copy_pipeline;
use crate::storage_enum::path_to_s3_key;
use crate::{
    CommitCallback, CopyOptions, DataChunk, EntryEnum, Result, ResumeContext, StorageEnum,
    StreamHandle,
};

pub(crate) async fn prepare_s3(
    storage: &S3Storage,
    entry: &EntryEnum,
) -> Result<(Vec<(u64, u64)>, StreamHandle)> {
    let size = entry.get_size();
    let (dst_rel, tags) = match entry {
        EntryEnum::S3(e) => (e.relative_path.clone(), e.tags.clone()),
        EntryEnum::NAS(e) => (path_to_s3_key(&e.relative_path).into_owned(), None),
        EntryEnum::HDFS(e) => (path_to_s3_key(&e.relative_path).into_owned(), None),
    };
    let part_size = storage.resume_part_size(size);
    let (upload_id, missing) = storage
        .prepare_resumable_upload(
            &dst_rel,
            size,
            part_size,
            tags.as_ref(),
            Some(entry.get_mtime()),
        )
        .await?;
    Ok((
        missing,
        StreamHandle::S3 {
            upload_id,
            part_size,
            dst_key: dst_rel,
        },
    ))
}

pub(crate) async fn prepare_nas(
    dest: &StorageEnum,
    entry: &EntryEnum,
    part_path: &Path,
    resume: bool,
) -> Result<(Vec<(u64, u64)>, StreamHandle)> {
    let size = entry.get_size();
    let missing = if resume {
        match Box::pin(dest.get_metadata(part_path)).await {
            Ok(existing) => match existing.get_size().cmp(&size) {
                std::cmp::Ordering::Less => vec![(existing.get_size(), size)],
                std::cmp::Ordering::Equal => vec![],
                std::cmp::Ordering::Greater => vec![(0, size)],
            },
            Err(_) => vec![(0, size)],
        }
    } else {
        vec![(0, size)]
    };
    Ok((
        missing,
        StreamHandle::Nas {
            part_path: part_path.to_path_buf(),
        },
    ))
}

pub(crate) async fn write_s3(
    storage: &S3Storage,
    entry: &EntryEnum,
    rx: mpsc::Receiver<DataChunk>,
    upload_id: &str,
    part_size: u64,
    dst_key: &str,
    progress: WriteProgress,
) -> Result<()> {
    storage
        .write_data_resumable(
            rx,
            dst_key,
            entry.get_size(),
            part_size,
            upload_id,
            progress,
        )
        .await
}

pub(crate) async fn write_nas(
    dest: &StorageEnum,
    entry: &EntryEnum,
    rx: mpsc::Receiver<DataChunk>,
    part_path: &Path,
    bytes_counter: Option<Arc<AtomicU64>>,
    on_committed: CommitCallback,
) -> Result<()> {
    let (uid, gid, mode) = match entry {
        EntryEnum::NAS(entry) => (entry.uid, entry.gid, Some(entry.mode)),
        EntryEnum::S3(_) => (None, None, None),
        EntryEnum::HDFS(entry) => (None, None, Some(entry.mode & 0o7777)),
    };
    let progress = WriteProgress {
        bytes_counter,
        on_committed,
    };
    match dest {
        StorageEnum::Local(storage) => {
            storage
                .write_data_resumable(rx, part_path, uid, gid, mode, progress)
                .await
        }
        StorageEnum::NFS(storage) => {
            storage
                .write_data_resumable(rx, part_path, uid, gid, mode, progress)
                .await
        }
        StorageEnum::CIFS(storage) => {
            storage
                .write_data_resumable(rx, part_path, uid, gid, mode, progress)
                .await
        }
        StorageEnum::S3(_) => Err(StorageError::OperationError(
            "write_chunk_stream: Nas StreamHandle used with an S3 destination".to_string(),
        )),
        StorageEnum::HDFS(_) => Err(StorageError::MismatchedType),
    }
}

pub(crate) async fn commit_nas(
    dest: &StorageEnum,
    entry: &EntryEnum,
    size: u64,
    part_path: &Path,
) -> Result<()> {
    dest.set_file_len(part_path, size).await?;
    dest.rename_with_expected_size(part_path, entry.get_relative_path(), Some(entry.get_size()))
        .await?;
    StorageEnum::apply_copied_metadata(dest, entry).await
}

pub(crate) async fn commit_s3(
    storage: &S3Storage,
    dest: &StorageEnum,
    entry: &EntryEnum,
    size: u64,
    upload_id: &str,
    part_size: u64,
    dst_key: &str,
) -> Result<()> {
    storage
        .finalize_resumable_upload(dst_key, size, part_size, upload_id)
        .await?;
    StorageEnum::apply_copied_metadata(dest, entry).await
}

pub(crate) async fn copy_nas(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    options: CopyOptions,
    resume: ResumeContext,
) -> Result<()> {
    let CopyOptions {
        qos,
        enable_integrity_check,
        is_source_reserved,
        bytes_counter,
        cancel,
    } = options;
    let size = entry.get_size();
    let ResumeContext {
        part_relative_path,
        missing_intervals,
        on_committed,
    } = resume;
    let handle = StreamHandle::Nas {
        part_path: part_relative_path.clone(),
    };
    let (rx, read_task) = StorageEnum::read_chunk_stream(
        from,
        entry,
        Some(missing_intervals),
        qos,
        false,
        COPY_PIPELINE_CAPACITY,
    );
    let to_c = to.clone();
    let entry_w = entry.clone();
    let handle_w = handle.clone();
    let write_task = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(&to_c, &entry_w, rx, &handle_w, bytes_counter, on_committed)
            .await
    });
    await_copy_pipeline(read_task, write_task, cancel.as_ref()).await?;
    ensure_not_cancelled(cancel.as_ref())?;
    if enable_integrity_check {
        let src_hash = from.compute_hash(entry.get_relative_path(), size).await?;
        let dst_hash = to.compute_hash(&part_relative_path, size).await?;
        if src_hash != dst_hash {
            return Err(StorageError::OperationError(
                "integrity check failed: source and destination hashes differ".to_string(),
            ));
        }
    }
    ensure_not_cancelled(cancel.as_ref())?;
    StorageEnum::commit_chunk_stream(to, entry, size, handle).await?;
    if !is_source_reserved {
        from.delete_file(entry).await?;
    }
    Ok(())
}

pub(crate) async fn copy_s3(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    options: CopyOptions,
    on_committed: CommitCallback,
) -> Result<()> {
    ensure_not_cancelled(options.cancel.as_ref())?;
    let CopyOptions {
        qos,
        enable_integrity_check,
        is_source_reserved,
        bytes_counter,
        cancel,
    } = options;
    let size = entry.get_size();
    let (missing, handle) =
        StorageEnum::resume_prepare(to, entry, entry.get_relative_path(), true).await?;
    let expected_session_bytes: u64 = missing.iter().map(|(start, end)| end - start).sum();
    let session_bytes = Arc::new(AtomicU64::new(0));
    let callback: CommitCallback = {
        let session_bytes = session_bytes.clone();
        Arc::new(move |offset, len| {
            session_bytes.fetch_add(len, Ordering::Relaxed);
            on_committed(offset, len);
        })
    };
    let (rx, read_task) = StorageEnum::read_chunk_stream(
        from,
        entry,
        Some(missing),
        qos,
        false,
        COPY_PIPELINE_CAPACITY,
    );
    let to_c = to.clone();
    let entry_w = entry.clone();
    let handle_w = handle.clone();
    let write_task = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(&to_c, &entry_w, rx, &handle_w, bytes_counter, callback)
            .await
    });
    await_copy_pipeline(read_task, write_task, cancel.as_ref()).await?;
    ensure_not_cancelled(cancel.as_ref())?;
    let uploaded = session_bytes.load(Ordering::Relaxed);
    if uploaded != expected_session_bytes {
        return Err(StorageError::OperationError(format!(
            "size check failed before multipart completion: session uploaded {uploaded} bytes, missing intervals require {expected_session_bytes}: {}",
            entry.get_relative_path().display()
        )));
    }
    ensure_not_cancelled(cancel.as_ref())?;
    StorageEnum::commit_chunk_stream(to, entry, size, handle).await?;
    if enable_integrity_check {
        let src_hash = from.compute_hash(entry.get_relative_path(), size).await?;
        let dst_hash = to.compute_hash(entry.get_relative_path(), size).await?;
        if src_hash != dst_hash {
            StorageEnum::cleanup_mismatched_dest(to, entry).await;
            return Err(StorageError::OperationError(
                "integrity check failed: source and destination hashes differ".to_string(),
            ));
        }
    }
    ensure_not_cancelled(cancel.as_ref())?;
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
