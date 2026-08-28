//! Common non-resumable copy orchestration and pipeline coordination.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::checksum::{ConsistencyCheck, HashCalculator};
use crate::error::StorageError;
use crate::qos::QosManager;
use crate::storage_enum::path_to_s3_key;
use crate::{CopyOptions, DataChunk, EntryEnum, Result, S3Entry, S3Storage, StorageEnum};

pub(crate) async fn copy_s3_to_s3_native(
    src: &S3Storage,
    dst: &S3Storage,
    entry: &S3Entry,
    source: &StorageEnum,
    source_entry: &EntryEnum,
    bytes_counter: Option<&Arc<AtomicU64>>,
    is_source_reserved: bool,
) -> Result<()> {
    let src_key = src.build_full_key(&entry.relative_path);
    let dst_key = dst.build_full_key(&entry.relative_path);
    if src.endpoint == dst.endpoint {
        src.copy_object(src.bucket(), &src_key, dst.bucket(), &dst_key)
            .await?;
    } else {
        src.stream_copy_to(dst, &src_key, &dst_key, entry.size, entry.tags.clone())
            .await?;
    }
    if let Some(counter) = bytes_counter {
        counter.fetch_add(source_entry.get_size(), Ordering::Relaxed);
    }
    if !is_source_reserved {
        source.delete_file(source_entry).await?;
    }
    Ok(())
}

/// 哈希计算 / 大文件读取 pipeline 的 channel 容量（读写并行，4 个 chunk 缓冲）
pub(crate) const HASH_CHANNEL_CAPACITY: usize = 4;
/// 文件拷贝 read→write pipeline 的 channel 容量。
///
/// 读写两端各自有 inflight pipeline（如 NFS 读 4 / 写 8），channel 是两级
/// 之间的解耦缓冲：容量 2 时写端一次落盘抖动即填满 channel、反压打空读端
/// 流水线；4 可吸收单次抖动。内存上界 = 容量 × chunk 大小 × 并发文件数
/// （NFS chunk ≤ 1MB；CIFS chunk 可达 8MB，增大容量时需关注）。
pub(crate) const COPY_PIPELINE_CAPACITY: usize = 4;

struct MultiCopyOptions {
    qos: Option<QosManager>,
    enable_integrity_check: bool,
    is_source_reserved: bool,
    bytes_counter: Option<Arc<AtomicU64>>,
    cancel: Option<CancellationToken>,
}

fn source_read_error(error: &StorageError) -> StorageError {
    StorageError::ReadError(format!("Source read failed: {error}"))
}

fn destination_write_error(error: &StorageError) -> StorageError {
    StorageError::WriteError(format!("Destination write failed: {error}"))
}

fn resolve_copy_pipeline<R, W>(read_result: Result<R>, write_result: Result<W>) -> Result<(R, W)> {
    match (read_result, write_result) {
        (_, Err(error)) => Err(destination_write_error(&error)),
        (Err(error), Ok(_)) => Err(source_read_error(&error)),
        (Ok(read), Ok(written)) => Ok((read, written)),
    }
}

pub(crate) async fn await_copy_pipeline<R, W>(
    read_task: tokio::task::JoinHandle<Result<R>>,
    write_task: tokio::task::JoinHandle<Result<W>>,
    cancel: Option<&CancellationToken>,
) -> Result<(R, W)> {
    let read_abort = read_task.abort_handle();
    let write_abort = write_task.abort_handle();
    let joined = async { (read_task.await, write_task.await) };
    tokio::pin!(joined);
    let (read_result, write_result) = match cancel {
        Some(token) => tokio::select! {
            result = &mut joined => result,
            () = token.cancelled() => {
                read_abort.abort();
                write_abort.abort();
                let _ = joined.await;
                return Err(StorageError::Cancelled);
            }
        },
        None => joined.await,
    };
    let read_result = read_result
        .map_err(|error| StorageError::OperationError(format!("read task panicked: {error:?}")))?;
    let write_result = write_result
        .map_err(|error| StorageError::OperationError(format!("write task panicked: {error:?}")))?;
    resolve_copy_pipeline(read_result, write_result)
}

impl StorageEnum {
    pub(crate) async fn compute_hash_and_len(
        &self,
        relative_path: &Path,
        size: u64,
    ) -> Result<(String, u64)> {
        if size == 0 {
            return Ok((String::new(), 0));
        }
        let (tx, mut rx) = mpsc::channel::<DataChunk>(HASH_CHANNEL_CAPACITY);
        let storage_c = self.clone();
        let path = relative_path.to_path_buf();
        let read_task = tokio::spawn(async move {
            match &storage_c {
                StorageEnum::Local(s) => s.read_data(tx, &path, size, true, None).await,
                StorageEnum::NFS(s) => s.read_data(tx, &path, size, true, None).await,
                StorageEnum::CIFS(s) => s.read_data(tx, &path, size, true, None).await,
                StorageEnum::S3(s) => {
                    let key = path_to_s3_key(&path);
                    s.read_data(tx, &key, size, true, None).await
                }
                StorageEnum::HDFS(s) => s.read_data(tx, &path, size, true, None).await,
            }
        });
        // Drain channel so the producer can complete（顺带累计读回字节数）。
        let mut read_back: u64 = 0;
        while let Some(chunk) = rx.recv().await {
            read_back += chunk.data.len() as u64;
        }
        let hasher = read_task
            .await
            .map_err(|e| StorageError::OperationError(format!("hash task panicked: {e:?}")))??;
        Ok((
            hasher.map(ConsistencyCheck::finalize).unwrap_or_default(),
            read_back,
        ))
    }

    /// size/hash mismatch 失败路径的 best-effort 目标端清理：删除已落地的
    /// 坏文件/坏对象（issue #58）。清理失败只告警，不遮蔽原 mismatch 错误。
    pub(crate) async fn cleanup_mismatched_dest(to: &StorageEnum, entry: &EntryEnum) {
        if let Err(e) = to.delete_file(entry).await {
            warn!(
                "failed to clean up mismatched destination {:?}: {e}",
                entry.get_relative_path()
            );
        }
    }

    pub(crate) async fn compute_entry_hash(
        storage: &StorageEnum,
        entry: &EntryEnum,
    ) -> Result<String> {
        if entry.get_size() == 0 {
            return Ok(String::new());
        }
        let (mut receiver, read_task) =
            Self::read_chunk_stream(storage, entry, None, None, true, HASH_CHANNEL_CAPACITY);
        while receiver.recv().await.is_some() {}
        let hasher = read_task.await.map_err(|error| {
            StorageError::OperationError(format!("hash task panicked: {error:?}"))
        })??;
        Ok(hasher.map(ConsistencyCheck::finalize).unwrap_or_default())
    }

    /// integrity 读回校验（issue #58）：hash 读回过程顺带核对读回字节数
    /// （零额外存储 RPC），再比对 BLAKE3。任一 mismatch → best-effort 清理
    /// 目标端坏文件后返回 Err。
    pub(crate) async fn verify_dest_integrity(
        to: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        src_hash: &str,
    ) -> Result<()> {
        let (dst_hash, read_back) = to
            .compute_hash_and_len(entry.get_relative_path(), size)
            .await?;
        if read_back != size {
            Self::cleanup_mismatched_dest(to, entry).await;
            return Err(StorageError::OperationError(format!(
                "integrity check failed: destination read-back returned {read_back} bytes, expected {size}: {}",
                entry.get_relative_path().display()
            )));
        }
        if src_hash != dst_hash {
            Self::cleanup_mismatched_dest(to, entry).await;
            return Err(StorageError::OperationError(
                "integrity check failed: source and destination hashes differ".to_string(),
            ));
        }
        Ok(())
    }

    async fn copy_single_chunk(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        options: &CopyOptions,
    ) -> Result<()> {
        if options
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(StorageError::Cancelled);
        }
        let data = if let Some(qos) = options.qos.clone() {
            let (mut chunks, read_task) = Self::read_chunk_stream(
                from,
                entry,
                None,
                Some(qos),
                false,
                COPY_PIPELINE_CAPACITY,
            );
            let mut collected = Vec::new();
            while let Some(chunk) = chunks.recv().await {
                collected.extend_from_slice(&chunk.data);
            }
            read_task
                .await
                .map_err(|error| {
                    StorageError::OperationError(format!("read task panicked: {error:?}"))
                })?
                .map_err(|error| source_read_error(&error))?;
            Bytes::from(collected)
        } else {
            Self::read_file_from(from, entry, size)
                .await
                .map_err(|error| source_read_error(&error))?
        };
        let read_len = data.len() as u64;
        if read_len != size {
            return Err(StorageError::OperationError(format!(
                "size check failed: read {read_len} bytes from source, expected {size}: {}",
                entry.get_relative_path().display()
            )));
        }
        let source_hash = if options.enable_integrity_check && !data.is_empty() {
            let mut hasher = HashCalculator::new();
            hasher.update(&data);
            Some(hasher.finalize())
        } else {
            None
        };
        if let Err(error) =
            Self::write_file_from_bytes_at(to, entry, data, entry.get_relative_path()).await
        {
            return Err(destination_write_error(&error));
        }
        if let Some(counter) = &options.bytes_counter {
            counter.fetch_add(size, Ordering::Relaxed);
        }
        if let Some(source_hash) = source_hash {
            Self::verify_dest_integrity(to, entry, size, &source_hash).await?;
        }
        Self::complete_copied_entry(from, to, entry, options.is_source_reserved).await
    }

    /// Copy a file with optional `QoS` rate limiting and integrity verification.
    ///
    /// - `qos`: if provided, bandwidth + IOPS rate limiting per-chunk (multi-chunk) or per-file (single-chunk)
    /// - `enable_integrity_check`: if true, BLAKE3 hashes of source and destination are compared
    /// - `is_source_reserved`: if true, source file is not deleted after copy (S3 only)
    ///
    /// S3→S3 copies delegate directly to server-side `CopyObject` / `stream_copy_to` and skip
    /// QoS/integrity (S3 guarantees consistency internally).
    ///
    /// # Errors
    ///
    pub(crate) async fn copy_file_inner(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        options: CopyOptions,
    ) -> Result<()> {
        let single_options = options.clone();
        let CopyOptions {
            qos,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
            cancel,
        } = options;
        // cancel 在 chunk 边界触发时会终止读写 task；已经写出的目标数据不回滚。
        // Top-of-function cancel check: avoids issuing any IO if already cancelled.
        if let Some(ref token) = cancel
            && token.is_cancelled()
        {
            return Err(StorageError::Cancelled);
        }

        let size = entry.get_size();
        let is_single_chunk = size <= from.block_size();

        if is_single_chunk {
            return Self::copy_single_chunk(from, to, entry, size, &single_options).await;
        }

        Self::copy_multi_chunk(
            from,
            to,
            entry,
            size,
            MultiCopyOptions {
                qos,
                enable_integrity_check,
                is_source_reserved,
                bytes_counter,
                cancel,
            },
        )
        .await
    }

    async fn copy_multi_chunk(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        options: MultiCopyOptions,
    ) -> Result<()> {
        let (source_hasher, bytes_written) =
            Self::run_copy_pipeline(from, to, entry, size, &options).await?;
        if bytes_written != size {
            Self::cleanup_mismatched_dest(to, entry).await;
            return Err(StorageError::OperationError(format!(
                "size check failed: wrote {bytes_written} bytes, expected {size}: {}",
                entry.get_relative_path().display()
            )));
        }
        if options
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(StorageError::Cancelled);
        }
        let source_hash = options
            .enable_integrity_check
            .then(|| source_hasher.map(ConsistencyCheck::finalize))
            .flatten();
        if let Some(src_hash) = source_hash {
            Self::verify_dest_integrity(to, entry, size, &src_hash).await?;
        }
        Self::apply_copied_metadata(to, entry).await?;
        if !options.is_source_reserved {
            from.delete_file(entry).await?;
        }
        Ok(())
    }

    async fn run_copy_pipeline(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        options: &MultiCopyOptions,
    ) -> Result<(Option<HashCalculator>, u64)> {
        let (tx, rx) = mpsc::channel::<DataChunk>(COPY_PIPELINE_CAPACITY);
        let from_c = from.clone();
        let to_c = to.clone();
        let entry_r = entry.clone();
        let entry_w = entry.clone();
        let write_path = entry.get_relative_path().to_path_buf();
        let qos = options.qos.clone();
        let enable_integrity_check = options.enable_integrity_check;
        let bytes_counter = options.bytes_counter.clone();
        let read_task = tokio::spawn(async move {
            Box::pin(Self::read_data_from(
                &from_c,
                &entry_r,
                tx,
                size,
                enable_integrity_check,
                qos,
            ))
            .await
        });
        let write_task = tokio::spawn(async move {
            Self::write_copy_data(&to_c, &entry_w, &write_path, rx, size, bytes_counter).await
        });
        await_copy_pipeline(read_task, write_task, options.cancel.as_ref()).await
    }
}
