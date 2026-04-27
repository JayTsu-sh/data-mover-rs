use std::borrow::Cow;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;

/// 哈希计算 / 大文件读取 pipeline 的 channel 容量（读写并行，4 个 chunk 缓冲）
const HASH_CHANNEL_CAPACITY: usize = 4;
/// 文件拷贝 read→write pipeline 的 channel 容量（最小背压，避免内存膨胀）
const COPY_PIPELINE_CAPACITY: usize = 2;
/// TAR 打包 pipeline 的 channel 容量（多文件顺序读，适当放大缓冲）
const TAR_PIPELINE_CAPACITY: usize = 16;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[cfg(windows)]
use crate::acl;
use crate::checksum::{ConsistencyCheck, HashCalculator};
use crate::cifs::{CifsStorage, create_cifs_storage, create_cifs_storage_ensuring_dir};
use crate::error::StorageError;
use crate::filter::FilterExpression;
use crate::local::{LocalStorage, create_local_storage, create_local_storage_ensuring_dir};
use crate::nfs::{NFSStorage, create_nfs_storage, create_nfs_storage_ensuring_dir};
use crate::qos::QosManager;
use crate::s3::{S3Storage, create_s3_storage};
use crate::tar_pack::{build_header_for_entry, tar_eof_marker, tar_padding};
use crate::{DataChunk, DeleteDirIterator, EntryEnum, Result, WalkDirAsyncIterator, WalkDirAsyncIterator2};

/// 存储类型枚举
#[derive(Debug, PartialEq, Eq)]
pub enum StorageType {
    Local,
    Nfs,
    S3,
    Cifs,
}

/// 统一的存储枚举类型
#[derive(Clone, Debug)]
pub enum StorageEnum {
    Local(LocalStorage),
    NFS(NFSStorage),
    S3(S3Storage),
    CIFS(CifsStorage),
}

impl StorageEnum {
    /// 验证存储连通性
    ///
    /// - Local: 检查根路径是否存在且可访问
    /// - NFS: 创建成功即已连通（mount 操作在构造时完成）
    /// - S3: 执行 `HeadBucket` 验证 bucket 可访问性及凭据有效性
    pub async fn check_connectivity(&self) -> Result<()> {
        match self {
            StorageEnum::Local(storage) => {
                if !storage.root_path.exists() {
                    return Err(StorageError::IoError(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Path does not exist: {}", storage.root_path.display()),
                    )));
                }
                Ok(())
            }
            StorageEnum::NFS(_) => Ok(()),
            StorageEnum::S3(storage) => storage.check_connectivity().await,
            StorageEnum::CIFS(storage) => storage.check_connectivity().await,
        }
    }

    /// 探测存储服务端时间
    ///
    /// 在存储上写入临时文件 → 读取 mtime → 删除 → 返回服务端时间戳（秒）。
    /// 本地存储返回 None（mtime 等于系统时钟，无校验意义）。
    pub async fn probe_server_time(&self) -> Result<Option<i64>> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp_name = format!(".~ts_{nanos:x}");

        match self {
            StorageEnum::Local(_) => Ok(None),
            StorageEnum::NFS(s) => {
                let tmp_path = PathBuf::from(&tmp_name);
                s.write_file(&tmp_path, Bytes::from_static(b"\0"), None, None, None)
                    .await?;
                let entry = s.get_metadata(&tmp_path).await?;
                let mtime = entry.get_mtime();
                let _ = s.delete_file(&tmp_path).await;
                Ok(Some(mtime))
            }
            StorageEnum::S3(s) => {
                s.write_file(&tmp_name, Bytes::from_static(b"\0"), 0, None).await?;
                let entry = s.get_metadata(&tmp_name).await?;
                let mtime = entry.get_mtime();
                let _ = s.delete_object(&tmp_name).await;
                Ok(Some(mtime))
            }
            StorageEnum::CIFS(s) => {
                let tmp_path = PathBuf::from(&tmp_name);
                s.write_file(&tmp_path, Bytes::from_static(b"\0"), None, None, None)
                    .await?;
                let entry = s.get_metadata(&tmp_path).await?;
                let mtime = entry.get_mtime();
                let _ = s.delete_file(&tmp_path).await;
                Ok(Some(mtime))
            }
        }
    }

    pub async fn delete_file(&self, entry: &EntryEnum) -> Result<()> {
        match (self, entry) {
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => storage.delete_file(&entry.relative_path).await,
            (StorageEnum::Local(storage), EntryEnum::S3(entry)) => {
                storage.delete_file(Path::new(&entry.relative_path)).await
            }
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => storage.delete_file(&entry.relative_path).await,
            (StorageEnum::NFS(storage), EntryEnum::S3(entry)) => {
                storage.delete_file(Path::new(&entry.relative_path)).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => storage.delete_file(&entry.relative_path).await,
            (StorageEnum::CIFS(storage), EntryEnum::S3(entry)) => {
                storage.delete_file(Path::new(&entry.relative_path)).await
            }
            (StorageEnum::S3(storage), EntryEnum::S3(entry)) => {
                let key = storage.build_full_key(&entry.relative_path);
                storage.delete_object(&key).await
            }
            (StorageEnum::S3(storage), EntryEnum::NAS(entry)) => {
                let key = storage.build_full_key(&path_to_s3_key(&entry.relative_path));
                storage.delete_object(&key).await
            }
        }
    }

    pub async fn create_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        match (self, entry) {
            // local storage will create all dirs if it does not exist
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => storage.create_dir_all(&entry.relative_path).await,
            (StorageEnum::Local(storage), EntryEnum::S3(entry)) => {
                storage.create_dir_all(Path::new(&entry.relative_path)).await
            }
            // nfs storage will create all dirs if it deos not exist
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage.create_dir_all(&entry.relative_path).await.map(|_| ())
            }
            (StorageEnum::NFS(storage), EntryEnum::S3(entry)) => storage
                .create_dir_all(Path::new(&entry.relative_path))
                .await
                .map(|_| ()),
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => storage.create_dir_all(&entry.relative_path).await,
            (StorageEnum::CIFS(storage), EntryEnum::S3(entry)) => {
                storage.create_dir_all(Path::new(&entry.relative_path)).await
            }
            // s3 storage will has no dir concept, so we just return Ok(())
            _ => Ok(()),
        }
    }

    pub async fn delete_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        let iter = self.delete_dir_all_with_progress(Some(entry.get_relative_path()), 4)?;
        while iter.next().await.is_some() {}
        Ok(())
    }

    pub async fn create_symlink(&self, entry: &EntryEnum, target: &Path) -> Result<()> {
        match (self, entry) {
            // only EntryEnum::NAS will create symlink
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage
                    .create_symlink(
                        &entry.relative_path,
                        target,
                        entry.atime,
                        entry.mtime,
                        entry.uid,
                        entry.gid,
                    )
                    .await
            }
            // only EntryEnum::NAS will create symlink
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage
                    .create_symlink(
                        Path::new(&entry.relative_path),
                        target,
                        entry.atime,
                        entry.mtime,
                        entry.uid,
                        entry.gid,
                    )
                    .await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage
                    .create_symlink(
                        &entry.relative_path,
                        target,
                        entry.atime,
                        entry.mtime,
                        entry.uid,
                        entry.gid,
                    )
                    .await
            }
            _ => Ok(()),
        }
    }

    pub async fn read_symlink(&self, entry: &EntryEnum) -> Result<PathBuf> {
        match (self, entry) {
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => storage.read_symlink(&entry.relative_path).await,
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => storage.read_symlink(&entry.relative_path).await,
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => storage.read_symlink(&entry.relative_path).await,
            _ => Ok(PathBuf::new()),
        }
    }

    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        match self {
            StorageEnum::Local(storage) => storage.get_metadata(relative_path).await,
            StorageEnum::NFS(storage) => storage.get_metadata(relative_path).await,
            StorageEnum::S3(storage) => storage.get_metadata(&path_to_s3_key(relative_path)).await,
            StorageEnum::CIFS(storage) => storage.get_metadata(relative_path).await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn walkdir(
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, include_tags: bool, packaged: bool,
        package_depth: usize,
    ) -> Result<WalkDirAsyncIterator> {
        match self {
            StorageEnum::Local(s) => {
                s.walkdir(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    packaged,
                    package_depth,
                )
                .await
            }
            StorageEnum::NFS(s) => {
                s.walkdir(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    packaged,
                    package_depth,
                )
                .await
            }
            StorageEnum::S3(s) => {
                let key = sub_path.map(|p| path_to_s3_key(p));
                s.walkdir(
                    key.as_deref(),
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    include_tags,
                    packaged,
                    package_depth,
                )
                .await
            }
            StorageEnum::CIFS(s) => {
                s.walkdir(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    packaged,
                    package_depth,
                )
                .await
            }
        }
    }

    /// `walkdir_2`: 目录分页遍历，DFS 顺序分配 NDX，页级输出
    pub async fn walkdir_2(
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, include_tags: bool,
    ) -> Result<WalkDirAsyncIterator2> {
        match self {
            StorageEnum::Local(s) => {
                s.walkdir_2(sub_path, depth, match_expressions, exclude_expressions, concurrency)
                    .await
            }
            StorageEnum::NFS(s) => {
                s.walkdir_2(sub_path, depth, match_expressions, exclude_expressions, concurrency)
                    .await
            }
            StorageEnum::S3(s) => {
                let key = sub_path.map(|p| path_to_s3_key(p));
                s.walkdir_2(
                    key.as_deref(),
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    include_tags,
                )
                .await
            }
            StorageEnum::CIFS(s) => {
                s.walkdir_2(sub_path, depth, match_expressions, exclude_expressions, concurrency)
                    .await
            }
        }
    }

    /// Rename a file or directory within the same storage.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.rename(from, to).await,
            StorageEnum::NFS(s) => s.rename(from, to).await,
            StorageEnum::S3(_) => Err(StorageError::OperationError("S3 does not support rename".to_string())),
            StorageEnum::CIFS(s) => s.rename(from, to).await,
        }
    }

    /// Update metadata selectively (timestamps, ownership, permissions).
    /// Pass `None` to skip updating a specific field.
    pub async fn set_metadata(
        &self, relative_path: &Path, atime: Option<i64>, mtime: Option<i64>, uid: Option<u32>, gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.set_metadata(relative_path, atime, mtime, uid, gid, mode).await,
            StorageEnum::NFS(s) => s.update_metadata(relative_path, atime, mtime, uid, gid, mode).await,
            StorageEnum::CIFS(s) => s.update_metadata(relative_path, atime, mtime, uid, gid, mode).await,
            StorageEnum::S3(_) => Ok(()),
        }
    }

    /// Update file metadata (timestamps, ownership, permissions) from an entry.
    pub async fn set_entry_metadata(&self, entry: &EntryEnum) -> Result<()> {
        match (self, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                s.set_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                s.update_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                s.update_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            _ => Ok(()),
        }
    }

    /// 并行删除目录下所有文件和子目录，返回进度迭代器
    pub fn delete_dir_all_with_progress(
        &self, relative_path: Option<&Path>, concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        match self {
            StorageEnum::Local(s) => s.delete_dir_all_with_progress(relative_path, concurrency),
            StorageEnum::NFS(s) => s.delete_dir_all_with_progress(relative_path, concurrency),
            StorageEnum::CIFS(s) => s.delete_dir_all_with_progress(relative_path, concurrency),
            StorageEnum::S3(s) => {
                let key = relative_path.map(|p| path_to_s3_key(p));
                s.delete_dir_all_with_progress(key.as_deref(), concurrency)
            }
        }
    }

    /// Compute BLAKE3 hash of a file by streaming it through the storage's `read_data`.
    pub async fn compute_hash(&self, relative_path: &Path, size: u64) -> Result<String> {
        if size == 0 {
            return Ok(String::new());
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
            }
        });
        // Drain channel so the producer can complete.
        while rx.recv().await.is_some() {}
        let hasher = read_task
            .await
            .map_err(|e| StorageError::OperationError(format!("hash task panicked: {e:?}")))??;
        Ok(hasher.map(ConsistencyCheck::finalize).unwrap_or_default())
    }

    /// Copy a file with optional `QoS` rate limiting and integrity verification.
    ///
    /// - `qos`: if provided, bandwidth + IOPS rate limiting per-chunk (multi-chunk) or per-file (single-chunk)
    /// - `enable_integrity_check`: if true, BLAKE3 hashes of source and destination are compared
    /// - `is_source_reserved`: if true, source file is not deleted after copy (S3 only)
    ///
    /// S3→S3 copies delegate directly to server-side `CopyObject` / `stream_copy_to` and skip
    /// QoS/integrity (S3 guarantees consistency internally).
    pub async fn copy_file(
        from: &StorageEnum, to: &StorageEnum, entry: &EntryEnum, qos: Option<QosManager>, enable_integrity_check: bool,
        is_source_reserved: bool, bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        // Backwards-compatible wrapper: no cancellation.
        Self::copy_file_with_cancel(
            from,
            to,
            entry,
            qos,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
            None,
        )
        .await
    }

    /// 与 [`copy_file`] 相同，但额外接受一个 [`CancellationToken`]：
    /// - 当 token 在 chunk 边界被触发时，正在跑的 read/write 任务会被 abort，
    ///   函数立即返回 [`StorageError::Cancelled`]。
    /// - 已经写出的部分目标对象 **不会** 被回滚——调用方需要自己 `delete_file`
    ///   清理（HSM copytool 通常通过 `llapi_hsm_action_end(rc=ECANCELED)` +
    ///   后续 cleanup action 处理）。
    /// - `cancel = None` 时行为与 [`copy_file`] 完全一致。
    ///
    /// 取消粒度：
    /// - 单块路径（文件 ≤ block_size）：在 read 之前检查一次。已发起的 IO
    ///   不会被打断。
    /// - 多块管道：read_data / write_data 在独立 task 中运行，token 触发时通过
    ///   `AbortHandle` 强制结束。chunk 边界响应延迟 ≤ 一个 chunk 的 IO 时间。
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_file_with_cancel(
        from: &StorageEnum, to: &StorageEnum, entry: &EntryEnum, qos: Option<QosManager>, enable_integrity_check: bool,
        is_source_reserved: bool, bytes_counter: Option<Arc<AtomicU64>>, cancel: Option<CancellationToken>,
    ) -> Result<()> {
        // Top-of-function cancel check: avoids issuing any IO if already cancelled.
        if let Some(ref token) = cancel
            && token.is_cancelled()
        {
            return Err(StorageError::Cancelled);
        }

        let size = entry.get_size();

        // ── S3 → S3（无 QoS 时走原生路径；有 QoS 时 fall-through 到下方单块/多块逻辑）
        if let (StorageEnum::S3(src), StorageEnum::S3(dst), EntryEnum::S3(e)) = (from, to, entry)
            && qos.is_none()
        {
            let src_key = src.build_full_key(&e.relative_path);
            let dst_key = dst.build_full_key(&e.relative_path);
            let result = if src.endpoint == dst.endpoint {
                src.copy_object(src.bucket(), &src_key, dst.bucket(), &dst_key).await
            } else {
                src.stream_copy_to(dst, &src_key, &dst_key, e.size, e.tags.clone())
                    .await
            };
            result?;
            if let Some(ref counter) = bytes_counter {
                counter.fetch_add(size, Ordering::Relaxed);
            }
            if !is_source_reserved {
                from.delete_file(entry).await?;
            }
            return Ok(());
        }

        // ── Single-chunk ──────────────────────────────────────────────────────────
        let is_single_chunk = size <= from.block_size();

        if is_single_chunk {
            // QoS: 带宽 + IOPS 限流
            if let Some(ref qos_mgr) = qos {
                qos_mgr.acquire(size).await;
            }

            // QoS may have suspended us; re-check cancel before spending IO.
            if let Some(ref token) = cancel
                && token.is_cancelled()
            {
                return Err(StorageError::Cancelled);
            }

            let data = match (from, entry) {
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await?,
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await?,
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await?,
                (StorageEnum::S3(s), EntryEnum::S3(e)) => s.read_file(&e.relative_path, size).await?,
                _ => {
                    return Err(StorageError::OperationError(format!(
                        "unsupported source/entry combination for copy: {entry:?}"
                    )));
                }
            };

            // Integrity: hash the data we just read from source.
            let source_hash = if enable_integrity_check && !data.is_empty() {
                let mut h = HashCalculator::new();
                h.update(&data);
                Some(h.finalize())
            } else {
                None
            };

            match (to, entry) {
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await?;
                }
                (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await?;
                }
                (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await?;
                }
                (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.write_file(&e.relative_path, data, e.mtime, e.tags.clone()).await?;
                }
                (StorageEnum::S3(s), EntryEnum::NAS(e)) => {
                    s.write_file(&path_to_s3_key(&e.relative_path), data, e.mtime, None)
                        .await?;
                }
            }

            // per-chunk 带宽统计：单块路径，写完后一次性增量
            if let Some(ref counter) = bytes_counter {
                counter.fetch_add(size, Ordering::Relaxed);
            }

            if let Some(src_hash) = source_hash {
                let dst_hash = to.compute_hash(entry.get_relative_path(), size).await?;
                if src_hash != dst_hash {
                    return Err(StorageError::OperationError(
                        "integrity check failed: source and destination hashes differ".to_string(),
                    ));
                }
            }
            if !is_source_reserved {
                from.delete_file(entry).await?;
            }
            return Ok(());
        }

        // ── Multi-chunk pipeline with QoS + integrity ─────────────────────────────
        // read_data in Local/NFS already handles per-chunk QoS and hash computation.
        let (tx, rx) = mpsc::channel::<DataChunk>(COPY_PIPELINE_CAPACITY);

        let from_c = from.clone();
        let to_c = to.clone();
        let entry_r = entry.clone();
        let entry_w = entry.clone();

        let read_task = tokio::spawn(async move {
            match (&from_c, &entry_r) {
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                    s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                        .await
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                        .await
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                        .await
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                        .await
                }
                _ => Err(StorageError::OperationError(format!(
                    "unsupported source/entry combination for multi-chunk copy: {entry_r:?}"
                ))),
            }
        });

        let bytes_counter_w = bytes_counter.clone();
        let write_task = tokio::spawn(async move {
            match (&to_c, &entry_w) {
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                    s.write_data(rx, &e.relative_path, e.uid, e.gid, Some(e.mode), bytes_counter_w)
                        .await
                }
                (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                    s.write_data(rx, Path::new(&e.relative_path), None, None, None, bytes_counter_w)
                        .await
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.write_data(rx, &e.relative_path, e.uid, e.gid, Some(e.mode), bytes_counter_w)
                        .await
                }
                (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                    s.write_data(rx, Path::new(&e.relative_path), None, None, None, bytes_counter_w)
                        .await
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.write_data(rx, &e.relative_path, e.uid, e.gid, Some(e.mode), bytes_counter_w)
                        .await
                }
                (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                    s.write_data(rx, Path::new(&e.relative_path), None, None, None, bytes_counter_w)
                        .await
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.write_data(rx, &e.relative_path, size, e.mtime, e.tags.clone(), bytes_counter_w)
                        .await
                }
                (StorageEnum::S3(s), EntryEnum::NAS(e)) => {
                    s.write_data(
                        rx,
                        &path_to_s3_key(&e.relative_path),
                        size,
                        e.mtime,
                        None,
                        bytes_counter_w,
                    )
                    .await
                }
            }
        });

        let read_abort = read_task.abort_handle();
        let write_abort = write_task.abort_handle();

        // Race the joined IO against the cancel token (if any). On cancel we abort
        // the spawned tasks; their JoinHandles will then resolve with a Cancelled
        // JoinError, which we discard in favour of returning StorageError::Cancelled.
        let join_io = async {
            let r = read_task.await;
            let w = write_task.await;
            (r, w)
        };

        let (read_res, write_res) = match cancel.as_ref() {
            Some(token) => {
                tokio::select! {
                    pair = join_io => pair,
                    () = token.cancelled() => {
                        read_abort.abort();
                        write_abort.abort();
                        return Err(StorageError::Cancelled);
                    }
                }
            }
            None => join_io.await,
        };

        let source_hasher =
            read_res.map_err(|e| StorageError::OperationError(format!("read task panicked: {e:?}")))??;
        write_res.map_err(|e| StorageError::OperationError(format!("write task panicked: {e:?}")))??;

        // Final cancel check before integrity verification (which itself does IO).
        if let Some(ref token) = cancel
            && token.is_cancelled()
        {
            return Err(StorageError::Cancelled);
        }

        if enable_integrity_check && let Some(src_h) = source_hasher {
            let src_hash = src_h.finalize();
            let dst_hash = to.compute_hash(entry.get_relative_path(), size).await?;
            if src_hash != dst_hash {
                return Err(StorageError::OperationError(
                    "integrity check failed: source and destination hashes differ".to_string(),
                ));
            }
        }

        if !is_source_reserved {
            from.delete_file(entry).await?;
        }

        Ok(())
    }

    /// 将多个源端文件打包为一个 tar 文件写入目标端。
    ///
    /// 参考 `copy_file` 的 multi-chunk 管道模式：
    /// - spawn `write_task` 根据目标存储类型 dispatch `write_data`
    /// - 当前 task 作为 producer：遍历 entries，依次发送 ustar header + 文件数据 + padding
    /// - 最后发送 EOF marker（两个 512B 全零块）
    ///
    /// # 参数
    /// - `from`: 源端存储
    /// - `to`: 目标端存储
    /// - `entries`: 需要打包的条目列表（阶段 1 walkdir 收集的结果）
    /// - `tar_path`: 目标 .tar 文件的相对路径
    /// - `tar_size`: `calculate_tar_size()` 计算的总大小（S3 用于 singlepart/multipart 决策）
    /// - `tar_mtime`: tar 文件的 mtime（通常取源端目录的 mtime）
    /// - `qos`: 可选的 `QoS` 限速管理器
    /// - `bytes_counter`: 可选的字节计数器
    #[allow(clippy::too_many_arguments)]
    pub async fn pack_files_to_tar(
        from: &StorageEnum, to: &StorageEnum, entries: &[Arc<EntryEnum>], tar_path: &Path, tar_size: u64,
        tar_mtime: i64, qos: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        // 从 tar_path 推导出被打包目录的路径（去掉 .tar 扩展名）
        let base_path = tar_path.with_extension("");
        let (tx, rx) = mpsc::channel::<DataChunk>(TAR_PIPELINE_CAPACITY);

        let tar_key = path_to_s3_key(tar_path).to_string();
        let tar_path_buf = tar_path.to_path_buf();

        // ── Write task: dispatch by destination storage type ──
        // channel 容量 16：tar 打包涉及多个文件串行读取，写入端（尤其 S3）可能较慢，需要足够 buffer 避免生产者阻塞
        let to_c = to.clone();
        let bytes_counter_w = bytes_counter.clone();
        let write_task = tokio::spawn(async move {
            match &to_c {
                StorageEnum::Local(s) => s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w).await,
                StorageEnum::NFS(s) => s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w).await,
                StorageEnum::CIFS(s) => s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w).await,
                StorageEnum::S3(s) => {
                    s.write_data(rx, &tar_key, tar_size, tar_mtime, None, bytes_counter_w)
                        .await
                }
            }
        });

        // ── Producer: iterate entries, send headers + data + padding ──
        let mut offset = 0u64;
        let block_size = from.block_size();

        for entry in entries {
            // 读取 symlink 目标（如果是 symlink）
            let link_target = if entry.get_is_symlink() {
                match from.read_symlink(entry).await {
                    Ok(target) => target.to_string_lossy().to_string(),
                    Err(e) => {
                        warn!(
                            "Failed to read symlink target for {:?}: {}",
                            entry.get_relative_path(),
                            e
                        );
                        String::new()
                    }
                }
            } else {
                String::new()
            };

            // tar 内路径：strip_prefix 得到相对于被打包目录的路径，统一使用 '/' 分隔符
            let tar_internal_path = entry
                .get_relative_path()
                .strip_prefix(&base_path)
                .unwrap_or(entry.get_relative_path())
                .iter()
                .map(|c| c.to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");

            // 发送 ustar header
            let header_bytes = build_header_for_entry(entry, &tar_internal_path, &link_target);
            if tx
                .send(DataChunk {
                    offset,
                    data: header_bytes,
                })
                .await
                .is_err()
            {
                return Err(StorageError::OperationError(
                    "tar write channel closed during header send".to_string(),
                ));
            }
            offset += 512;

            // 发送文件数据（仅普通文件）
            if entry.get_is_regular_file() {
                let file_size = entry.get_size();
                if file_size > 0 {
                    if file_size <= block_size {
                        // 小文件：单块读取
                        if let Some(ref qos_mgr) = qos {
                            qos_mgr.acquire(file_size).await;
                        }
                        let data = Self::read_file_from(from, entry, file_size).await?;
                        if tx.send(DataChunk { offset, data }).await.is_err() {
                            return Err(StorageError::OperationError(
                                "tar write channel closed during file data send".to_string(),
                            ));
                        }
                        offset += file_size;
                    } else {
                        // 大文件：read_data channel 分块转发
                        let (sub_tx, mut sub_rx) = mpsc::channel::<DataChunk>(HASH_CHANNEL_CAPACITY);
                        let from_c = from.clone();
                        let entry_c = entry.clone();
                        let qos_c = qos.clone();

                        let read_task = tokio::spawn(async move {
                            Self::read_data_from(&from_c, &entry_c, sub_tx, file_size, qos_c).await
                        });

                        while let Some(chunk) = sub_rx.recv().await {
                            let chunk_len = chunk.data.len() as u64;
                            if tx
                                .send(DataChunk {
                                    offset,
                                    data: chunk.data,
                                })
                                .await
                                .is_err()
                            {
                                return Err(StorageError::OperationError(
                                    "tar write channel closed during large file transfer".to_string(),
                                ));
                            }
                            offset += chunk_len;
                        }

                        // 等待读取任务完成并检查错误
                        read_task
                            .await
                            .map_err(|e| StorageError::OperationError(format!("read task panicked: {e:?}")))??;
                    }

                    // 发送 padding
                    if let Some(padding) = tar_padding(file_size) {
                        let padding_len = padding.len() as u64;
                        if tx.send(DataChunk { offset, data: padding }).await.is_err() {
                            return Err(StorageError::OperationError(
                                "tar write channel closed during padding send".to_string(),
                            ));
                        }
                        offset += padding_len;
                    }
                }
            }
        }

        // 发送 EOF marker
        let eof = tar_eof_marker();
        if tx.send(DataChunk { offset, data: eof }).await.is_err() {
            return Err(StorageError::OperationError(
                "tar write channel closed during EOF send".to_string(),
            ));
        }

        // 关闭 producer channel
        drop(tx);

        // 等待写入任务完成
        write_task
            .await
            .map_err(|e| StorageError::OperationError(format!("tar write task panicked: {e:?}")))??;

        Ok(())
    }

    /// 读取文件完整内容（单块读取，适用于小文件或需要全量数据的场景）
    pub async fn read_file_from(from: &StorageEnum, entry: &EntryEnum, size: u64) -> Result<Bytes> {
        match (from, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::S3(s), EntryEnum::S3(e)) => s.read_file(&e.relative_path, size).await,
            _ => Err(StorageError::OperationError(format!(
                "unsupported source/entry combination for tar read: {entry:?}"
            ))),
        }
    }

    /// 将 Bytes 数据写入目标存储的指定 entry 路径
    ///
    /// 用于 delta 重建后的写入，entry 提供路径和元数据（uid/gid/mode 等）。
    pub async fn write_file_from_bytes(to: &StorageEnum, entry: &EntryEnum, data: Bytes) -> Result<()> {
        match (to, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await
            }
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await
            }
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode)).await
            }
            (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                s.write_file(&e.relative_path, data, e.mtime, e.tags.clone()).await
            }
            (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None).await
            }
            (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None).await
            }
            (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None).await
            }
            (StorageEnum::S3(s), EntryEnum::NAS(e)) => {
                s.write_file(&path_to_s3_key(&e.relative_path), data, e.mtime, None)
                    .await
            }
        }
    }

    /// 从源端分块读取文件数据到 channel（内部辅助方法）
    async fn read_data_from(
        from: &StorageEnum, entry: &EntryEnum, tx: mpsc::Sender<DataChunk>, size: u64, qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        match (from, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => s.read_data(tx, &e.relative_path, size, false, qos).await,
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => s.read_data(tx, &e.relative_path, size, false, qos).await,
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => s.read_data(tx, &e.relative_path, size, false, qos).await,
            (StorageEnum::S3(s), EntryEnum::S3(e)) => s.read_data(tx, &e.relative_path, size, false, qos).await,
            _ => Err(StorageError::OperationError(format!(
                "unsupported source/entry combination for tar read_data: {entry:?}"
            ))),
        }
    }

    pub fn block_size(&self) -> u64 {
        match self {
            StorageEnum::Local(s) => s.config.block_size,
            StorageEnum::NFS(s) => s.config.block_size,
            StorageEnum::CIFS(s) => s.config.block_size,
            StorageEnum::S3(s) => s.block_size,
        }
    }

    pub fn is_bucket_versioned(&self) -> bool {
        matches!(self, StorageEnum::S3(storage) if storage.is_bucket_versioned)
    }

    /// 从源端复制 ACL（非继承的显式 ACE + 继承保护状态）到目标端
    ///
    /// 支持组合：
    /// - Local → Local（仅 Windows，Win32 API）
    /// - CIFS → CIFS（跨平台，smb-rs 直通）
    /// - NFS → NFS（仅当双方都支持 ACL，即 `NFSv4+`）
    /// - 跨类型或不支持的组合静默跳过
    pub async fn copy_acl(from: &StorageEnum, to: &StorageEnum, relative_path: &Path) -> Result<()> {
        match (from, to) {
            // CIFS → CIFS：smb-rs SecurityDescriptor 直通（跨平台）
            (StorageEnum::CIFS(src), StorageEnum::CIFS(dst)) => {
                let sd = src.get_security_descriptor(relative_path).await?;
                dst.set_security_descriptor(relative_path, &sd).await
            }

            // NFS → NFS：NFSv4 ACL 直通（仅当双方都支持 ACL）
            (StorageEnum::NFS(src), StorageEnum::NFS(dst)) => {
                if src.supports_acl() && dst.supports_acl() {
                    let acl = src.get_acl(relative_path).await?;
                    dst.set_acl(relative_path, &acl).await?;
                }
                Ok(())
            }

            // Local → Local：Win32 API（仅 Windows）
            #[cfg(windows)]
            (StorageEnum::Local(src), StorageEnum::Local(dst)) => {
                let source_abs = src.root_path.join(relative_path);
                let target_abs = dst.root_path.join(relative_path);
                acl::copy_acl(&source_abs, &target_abs)
            }

            // 其他组合不支持 ACL，静默跳过
            _ => Ok(()),
        }
    }

    /// 从源端复制所有 extended attributes (xattr) 到目标端
    ///
    /// 支持组合：
    /// - NFS → NFS（仅当双方都支持 xattr，即 `NFSv4+`）
    /// - 其他组合静默跳过
    pub async fn copy_xattr(from: &StorageEnum, to: &StorageEnum, relative_path: &Path) -> Result<()> {
        match (from, to) {
            (StorageEnum::NFS(src), StorageEnum::NFS(dst)) => {
                if !src.supports_xattr() || !dst.supports_xattr() {
                    return Ok(());
                }
                let names = match src.list_xattr(relative_path).await {
                    Ok(names) => names,
                    Err(e) => {
                        // Unsupported 错误（v3 server）静默跳过；其他错误记录 warn
                        if !e.to_string().contains("Unsupported") {
                            warn!("Failed to list xattr for {:?}, skipping: {}", relative_path, e);
                        }
                        return Ok(());
                    }
                };
                for name in names {
                    let value = src.get_xattr(relative_path, &name).await?;
                    dst.set_xattr(relative_path, &name, value).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// 读取 ACL 数据为二进制字节（用于跨进程传输）
    ///
    /// 返回 `Some(bytes)` 表示有 ACL 数据，`None` 表示不支持或无 ACL。
    /// NFS ACL 使用自定义二进制格式（见 `serialize_nfs_acl`/`deserialize_nfs_acl`）。
    pub async fn get_acl_bytes(&self, relative_path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageEnum::CIFS(s) => {
                use binrw::BinWrite;
                let sd = s.get_security_descriptor(relative_path).await?;
                // 用 binrw 序列化 SecurityDescriptor 为字节
                let mut buf = std::io::Cursor::new(Vec::new());
                sd.write_le(&mut buf)
                    .map_err(|e| StorageError::OperationError(format!("serialize SD: {e}")))?;
                Ok(Some(buf.into_inner()))
            }
            StorageEnum::NFS(s) if s.supports_acl() => {
                match s.get_acl(relative_path).await {
                    Ok(acl) if !acl.aces.is_empty() => Ok(Some(serialize_nfs_acl(&acl))),
                    _ => Ok(None), // 空 ACL 或不支持时静默跳过
                }
            }
            #[cfg(windows)]
            StorageEnum::Local(s) => {
                let abs_path = s.root_path.join(relative_path);
                match acl::get_acl_bytes(&abs_path) {
                    Ok(bytes) if bytes.is_empty() => Ok(None),
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) => Err(e),
                }
            }
            _ => Ok(None),
        }
    }

    /// 从二进制字节设置 ACL（用于跨进程传输）
    pub async fn set_acl_bytes(&self, relative_path: &Path, acl_data: &[u8]) -> Result<()> {
        match self {
            StorageEnum::CIFS(s) => {
                use binrw::BinRead;
                // 用 binrw 反序列化字节为 SecurityDescriptor
                let mut cursor = std::io::Cursor::new(acl_data);
                let sd = smb::SecurityDescriptor::read_le(&mut cursor)
                    .map_err(|e| StorageError::OperationError(format!("deserialize SD: {e}")))?;
                s.set_security_descriptor(relative_path, &sd).await
            }
            StorageEnum::NFS(s) if s.supports_acl() => {
                let acl = deserialize_nfs_acl(acl_data)?;
                s.set_acl(relative_path, &acl).await
            }
            #[cfg(windows)]
            StorageEnum::Local(s) => {
                let abs_path = s.root_path.join(relative_path);
                acl::set_acl_bytes(&abs_path, acl_data)
            }
            _ => Ok(()),
        }
    }

    /// 读取所有 xattr 为 key-value 对（用于跨进程传输）
    ///
    /// 返回 `Some(bytes)` 表示有 xattr 数据，`None` 表示不支持或无 xattr。
    /// 二进制格式：`[u32 count] [u32 name_len] [name] [u32 value_len] [value] ...`
    pub async fn get_xattr_bytes(&self, relative_path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageEnum::NFS(s) if s.supports_xattr() => {
                let names = match s.list_xattr(relative_path).await {
                    Ok(names) if !names.is_empty() => names,
                    _ => return Ok(None),
                };
                let mut buf = Vec::new();
                buf.extend_from_slice(&(names.len() as u32).to_le_bytes());
                for name in &names {
                    let name_bytes = name.as_bytes();
                    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(name_bytes);
                    let value = s.get_xattr(relative_path, name).await?;
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&value);
                }
                Ok(Some(buf))
            }
            _ => Ok(None),
        }
    }

    /// 从二进制字节设置所有 xattr（用于跨进程传输）
    pub async fn set_xattr_bytes(&self, relative_path: &Path, xattr_data: &[u8]) -> Result<()> {
        match self {
            StorageEnum::NFS(s) if s.supports_xattr() => {
                let pairs = deserialize_xattr(xattr_data)?;
                for (name, value) in pairs {
                    s.set_xattr(relative_path, &name, Bytes::from(value)).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

// ============================================================
// NFS ACL / xattr 二进制序列化（跨进程传输用）
// ============================================================

/// 将 `NFSv4` ACL 序列化为二进制字节。
///
/// 格式：`[u32 ace_count] [ace...]`
/// 每个 ace：`[u32 type] [u32 flags] [u32 mask] [u32 who_len] [who_bytes]`
fn serialize_nfs_acl(acl: &nfs_rs::Acl) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(acl.aces.len() as u32).to_le_bytes());
    for ace in &acl.aces {
        buf.extend_from_slice(&(ace.ace_type as u32).to_le_bytes());
        buf.extend_from_slice(&ace.flags.0.to_le_bytes());
        buf.extend_from_slice(&ace.access_mask.0.to_le_bytes());
        let who_bytes = ace.who.as_bytes();
        buf.extend_from_slice(&(who_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(who_bytes);
    }
    buf
}

/// 反序列化长度上限常量（防止恶意/损坏数据导致 OOM）
const MAX_ACE_COUNT: usize = 4096;
const MAX_ACE_WHO_LEN: usize = 1024;
const MAX_XATTR_COUNT: usize = 1024;
const MAX_XATTR_NAME_LEN: usize = 256;
const MAX_XATTR_VALUE_LEN: usize = 64 * 1024; // 64 KiB

/// 从 cursor 读取一个 little-endian u32
fn read_u32_le(cursor: &mut io::Cursor<&[u8]>, context: &str) -> Result<u32> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|e| StorageError::OperationError(format!("deserialize {context}: {e}")))?;
    Ok(u32::from_le_bytes(buf))
}

/// 从 cursor 读取指定长度的字节，带上限检查防止 OOM
fn read_bytes_checked(cursor: &mut io::Cursor<&[u8]>, len: usize, max: usize, context: &str) -> Result<Vec<u8>> {
    if len > max {
        return Err(StorageError::OperationError(format!(
            "{context} length {len} exceeds maximum {max}"
        )));
    }
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|e| StorageError::OperationError(format!("deserialize {context}: {e}")))?;
    Ok(buf)
}

/// 从二进制字节反序列化 `NFSv4` ACL。
fn deserialize_nfs_acl(data: &[u8]) -> Result<nfs_rs::Acl> {
    let mut cursor = io::Cursor::new(data);

    let count = read_u32_le(&mut cursor, "ACL count")? as usize;
    if count > MAX_ACE_COUNT {
        return Err(StorageError::OperationError(format!(
            "ACE count {count} exceeds maximum {MAX_ACE_COUNT}"
        )));
    }

    let mut aces = Vec::with_capacity(count);
    for _ in 0..count {
        let ace_type = match read_u32_le(&mut cursor, "ACE type")? {
            0 => nfs_rs::AceType::AccessAllowed,
            1 => nfs_rs::AceType::AccessDenied,
            2 => nfs_rs::AceType::SystemAudit,
            3 => nfs_rs::AceType::SystemAlarm,
            v => return Err(StorageError::OperationError(format!("unknown ACE type: {v}"))),
        };

        let flags = nfs_rs::AceFlags(read_u32_le(&mut cursor, "ACE flags")?);
        let access_mask = nfs_rs::AceMask(read_u32_le(&mut cursor, "ACE mask")?);

        let who_len = read_u32_le(&mut cursor, "ACE who len")? as usize;
        let who_buf = read_bytes_checked(&mut cursor, who_len, MAX_ACE_WHO_LEN, "ACE 'who'")?;
        let who = String::from_utf8(who_buf)
            .map_err(|e| StorageError::OperationError(format!("invalid ACE who UTF-8: {e}")))?;

        aces.push(nfs_rs::NfsAce {
            ace_type,
            flags,
            access_mask,
            who,
        });
    }

    Ok(nfs_rs::Acl { aces })
}

/// 从二进制字节反序列化 xattr key-value 对。
///
/// 格式：`[u32 count] [u32 name_len] [name] [u32 value_len] [value] ...`
fn deserialize_xattr(data: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    let mut cursor = io::Cursor::new(data);

    let count = read_u32_le(&mut cursor, "xattr count")? as usize;
    if count > MAX_XATTR_COUNT {
        return Err(StorageError::OperationError(format!(
            "xattr count {count} exceeds maximum {MAX_XATTR_COUNT}"
        )));
    }

    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let name_len = read_u32_le(&mut cursor, "xattr name len")? as usize;
        let name_buf = read_bytes_checked(&mut cursor, name_len, MAX_XATTR_NAME_LEN, "xattr name")?;
        let name = String::from_utf8(name_buf)
            .map_err(|e| StorageError::OperationError(format!("invalid xattr name UTF-8: {e}")))?;

        let value_len = read_u32_le(&mut cursor, "xattr value len")? as usize;
        let value_buf = read_bytes_checked(&mut cursor, value_len, MAX_XATTR_VALUE_LEN, "xattr value")?;

        pairs.push((name, value_buf));
    }

    Ok(pairs)
}

/// 将 Path 转为 S3 兼容的字符串（正斜杠分隔）。
/// Linux 上零开销（直接返回 `Cow::Borrowed`），Windows 上仅在含 `\` 时分配新 `String`。
#[inline]
fn path_to_s3_key(path: &Path) -> Cow<'_, str> {
    let s = path.to_string_lossy();
    #[cfg(windows)]
    {
        if s.contains('\\') {
            return Cow::Owned(s.replace('\\', "/"));
        }
    }
    s
}

/// Detects the storage type from a path by checking its prefix.
/// This handles NFS and S3 paths specially by checking for their respective prefixes.
pub fn detect_storage_type(path: &str) -> StorageType {
    match path {
        p if p.starts_with("smb://") => StorageType::Cifs,
        p if p.starts_with("nfs://") => StorageType::Nfs,
        p if p.starts_with("s3://")
            || p.starts_with("s3+http://")
            || p.starts_with("s3+https://")
            || p.starts_with("s3+hcp://") =>
        {
            StorageType::S3
        }
        _ => StorageType::Local,
    }
}

/// 创建目标存储实例，如果 prefix 目录不存在则自动创建
pub async fn create_storage_for_dest(path: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    debug!("Creating destination storage for path: {}", path);
    match detect_storage_type(path) {
        StorageType::Cifs => create_cifs_storage_ensuring_dir(path, block_size).await,
        StorageType::Nfs => create_nfs_storage_ensuring_dir(path, block_size).await,
        StorageType::S3 => create_s3_storage(path, block_size).await,
        StorageType::Local => create_local_storage_ensuring_dir(path, block_size),
    }
}

/// 根据路径前缀创建对应的存储实例
pub async fn create_storage(path: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    debug!("Creating storage for path: {}", path);
    let storage_type = detect_storage_type(path);
    match storage_type {
        StorageType::Cifs => {
            debug!("Creating CIFS storage");
            create_cifs_storage(path, block_size).await
        }
        StorageType::Nfs => {
            debug!("Creating NFS storage");
            create_nfs_storage(path, block_size).await
        }
        StorageType::S3 => {
            debug!("Creating S3 storage");
            create_s3_storage(path, block_size).await
        }
        StorageType::Local => {
            debug!("Creating local storage");
            create_local_storage(path, block_size)
        }
    }
}
