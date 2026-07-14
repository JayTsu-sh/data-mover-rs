use std::borrow::Cow;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// 哈希计算 / 大文件读取 pipeline 的 channel 容量（读写并行，4 个 chunk 缓冲）
const HASH_CHANNEL_CAPACITY: usize = 4;
/// 文件拷贝 read→write pipeline 的 channel 容量。
///
/// 读写两端各自有 inflight pipeline（如 NFS 读 4 / 写 8），channel 是两级
/// 之间的解耦缓冲：容量 2 时写端一次落盘抖动即填满 channel、反压打空读端
/// 流水线；4 可吸收单次抖动。内存上界 = 容量 × chunk 大小 × 并发文件数
/// （NFS chunk ≤ 1MB；CIFS chunk 可达 8MB，增大容量时需关注）。
const COPY_PIPELINE_CAPACITY: usize = 4;
/// TAR 打包 pipeline 的 channel 容量（多文件顺序读，适当放大缓冲）
const TAR_PIPELINE_CAPACITY: usize = 16;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[cfg(windows)]
use crate::acl;
use crate::checksum::{ConsistencyCheck, HashCalculator};
use crate::cifs::{CifsStorage, create_cifs_storage};
use crate::error::StorageError;
use crate::filter::FilterExpression;
use crate::local::{LocalStorage, create_local_storage};
use crate::nfs::{NFSStorage, create_nfs_storage};
use crate::qos::QosManager;
use crate::s3::{S3Storage, create_s3_storage};
use crate::tar_pack::{build_header_for_entry, tar_eof_marker, tar_padding};
use crate::{
    CommitCallback, DataChunk, DeleteDirIterator, EntryEnum, Result, ResumeContext,
    WalkDirAsyncIterator, WalkDirAsyncIterator2,
};

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

/// 字节级续传的目标端流式写句柄（issue #21：`resume_prepare` 产出，
/// `write_chunk_stream`/`commit_chunk_stream` 消费）。跨 transport 传递
/// （双进程场景下 Receiver 侧 prepare、由 Sender 侧对称使用同一份区间信息），
/// 故派生 `Serialize`/`Deserialize`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamHandle {
    /// NAS（Local/NFS/CIFS）目标端：写 `.part` 临时文件；
    /// commit = `set_file_len` + `rename`。
    Nas { part_path: PathBuf },
    /// S3 目标端：写 in-progress multipart upload；commit = `CompleteMultipartUpload`。
    S3 {
        upload_id: String,
        part_size: u64,
        dst_key: String,
    },
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
        let nanos = crate::time_util::now_nanos();
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
                s.write_file(&tmp_name, Bytes::from_static(b"\0"), 0, None)
                    .await?;
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
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
            (StorageEnum::Local(storage), EntryEnum::S3(entry)) => {
                storage.delete_file(Path::new(&entry.relative_path)).await
            }
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
            (StorageEnum::NFS(storage), EntryEnum::S3(entry)) => {
                storage.delete_file(Path::new(&entry.relative_path)).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
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
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage.create_dir_all(&entry.relative_path).await
            }
            (StorageEnum::Local(storage), EntryEnum::S3(entry)) => {
                storage
                    .create_dir_all(Path::new(&entry.relative_path))
                    .await
            }
            // nfs storage will create all dirs if it deos not exist
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => storage
                .create_dir_all(&entry.relative_path)
                .await
                .map(|_| ()),
            (StorageEnum::NFS(storage), EntryEnum::S3(entry)) => storage
                .create_dir_all(Path::new(&entry.relative_path))
                .await
                .map(|_| ()),
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage.create_dir_all(&entry.relative_path).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::S3(entry)) => {
                storage
                    .create_dir_all(Path::new(&entry.relative_path))
                    .await
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
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path).await
            }
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path).await
            }
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
        &self,
        sub_path: Option<&Path>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        include_tags: bool,
        packaged: bool,
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
        &self,
        sub_path: Option<&Path>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        include_tags: bool,
    ) -> Result<WalkDirAsyncIterator2> {
        match self {
            StorageEnum::Local(s) => {
                s.walkdir_2(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                )
                .await
            }
            StorageEnum::NFS(s) => {
                s.walkdir_2(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                )
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
                s.walkdir_2(
                    sub_path,
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                )
                .await
            }
        }
    }

    /// Rename a file or directory within the same storage.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.rename(from, to).await,
            StorageEnum::NFS(s) => s.rename(from, to).await,
            StorageEnum::S3(_) => Err(StorageError::OperationError(
                "S3 does not support rename".to_string(),
            )),
            StorageEnum::CIFS(s) => s.rename(from, to).await,
        }
    }

    /// 将文件长度规整为 `len`（字节级续传收尾：截掉 `.part` 遗留尾部）。
    pub async fn set_file_len(&self, relative_path: &Path, len: u64) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::NFS(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::CIFS(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::S3(_) => Err(StorageError::OperationError(
                "S3 does not support byte-level resume".to_string(),
            )),
        }
    }

    /// Update metadata selectively (timestamps, ownership, permissions).
    /// Pass `None` to skip updating a specific field.
    pub async fn set_metadata(
        &self,
        relative_path: &Path,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        match self {
            StorageEnum::Local(s) => {
                s.set_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
            StorageEnum::NFS(s) => {
                s.update_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
            StorageEnum::CIFS(s) => {
                s.update_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
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
        &self,
        relative_path: Option<&Path>,
        concurrency: usize,
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
        self.compute_hash_and_len(relative_path, size)
            .await
            .map(|(hash, _)| hash)
    }

    /// 与 [`Self::compute_hash`] 相同，但额外返回实际读回的字节数——integrity
    /// 读回过程顺带取目标端 size（issue #58），不新增独立 get_metadata RPC。
    async fn compute_hash_and_len(&self, relative_path: &Path, size: u64) -> Result<(String, u64)> {
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
    async fn cleanup_mismatched_dest(to: &StorageEnum, entry: &EntryEnum) {
        if let Err(e) = to.delete_file(entry).await {
            warn!(
                "failed to clean up mismatched destination {:?}: {e}",
                entry.get_relative_path()
            );
        }
    }

    /// integrity 读回校验（issue #58）：hash 读回过程顺带核对读回字节数
    /// （零额外存储 RPC），再比对 BLAKE3。任一 mismatch → best-effort 清理
    /// 目标端坏文件后返回 Err。
    async fn verify_dest_integrity(
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
                "integrity check failed: destination read-back returned {read_back} bytes, expected {size}: {:?}",
                entry.get_relative_path()
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

    /// Copy a file with optional `QoS` rate limiting and integrity verification.
    ///
    /// - `qos`: if provided, bandwidth + IOPS rate limiting per-chunk (multi-chunk) or per-file (single-chunk)
    /// - `enable_integrity_check`: if true, BLAKE3 hashes of source and destination are compared
    /// - `is_source_reserved`: if true, source file is not deleted after copy (S3 only)
    ///
    /// S3→S3 copies delegate directly to server-side `CopyObject` / `stream_copy_to` and skip
    /// QoS/integrity (S3 guarantees consistency internally).
    pub async fn copy_file(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        is_source_reserved: bool,
        bytes_counter: Option<Arc<AtomicU64>>,
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
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        is_source_reserved: bool,
        bytes_counter: Option<Arc<AtomicU64>>,
        cancel: Option<CancellationToken>,
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
                src.copy_object(src.bucket(), &src_key, dst.bucket(), &dst_key)
                    .await
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
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                    s.read_file(&e.relative_path, size).await?
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.read_file(&e.relative_path, size).await?
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.read_file(&e.relative_path, size).await?
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.read_file(&e.relative_path, size).await?
                }
                _ => {
                    return Err(StorageError::OperationError(format!(
                        "unsupported source/entry combination for copy: {entry:?}"
                    )));
                }
            };

            // 写前断言（issue #58，无条件）：源读回字节数必须等于 entry 声明的
            // size——源截断/扫描后并发变更防线。数据已在内存，纯本地比较，
            // 零额外 IO；尚未写入目标端，无需清理。
            let read_len = data.len() as u64;
            if read_len != size {
                return Err(StorageError::OperationError(format!(
                    "size check failed: read {read_len} bytes from source, expected {size}: {:?}",
                    entry.get_relative_path()
                )));
            }

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
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                        .await?;
                }
                (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                        .await?;
                }
                (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                        .await?;
                }
                (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                    s.write_file(Path::new(&e.relative_path), data, None, None, None)
                        .await?;
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.write_file(&e.relative_path, data, e.mtime, e.tags.clone())
                        .await?;
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
                Self::verify_dest_integrity(to, entry, size, &src_hash).await?;
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
                    s.write_data(
                        rx,
                        &e.relative_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                    s.write_data(
                        rx,
                        Path::new(&e.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.write_data(
                        rx,
                        &e.relative_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                    s.write_data(
                        rx,
                        Path::new(&e.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.write_data(
                        rx,
                        &e.relative_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                    s.write_data(
                        rx,
                        Path::new(&e.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter_w,
                    )
                    .await
                }
                (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                    s.write_data(
                        rx,
                        &e.relative_path,
                        size,
                        e.mtime,
                        e.tags.clone(),
                        bytes_counter_w,
                    )
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

        let source_hasher = read_res
            .map_err(|e| StorageError::OperationError(format!("read task panicked: {e:?}")))??;
        let bytes_written = write_res
            .map_err(|e| StorageError::OperationError(format!("write task panicked: {e:?}")))??;

        // 写端本地计数断言（issue #58，无条件）：实际写入字节数必须等于 entry
        // 声明的 size——源截断时读端提前 EOF、写端静默少写的防线。纯本地比较，
        // 零额外存储 RPC；不等则清理目标端残留坏文件后报错。
        if bytes_written != size {
            Self::cleanup_mismatched_dest(to, entry).await;
            return Err(StorageError::OperationError(format!(
                "size check failed: wrote {bytes_written} bytes, expected {size}: {:?}",
                entry.get_relative_path()
            )));
        }

        // Final cancel check before integrity verification (which itself does IO).
        if let Some(ref token) = cancel
            && token.is_cancelled()
        {
            return Err(StorageError::Cancelled);
        }

        if enable_integrity_check && let Some(src_h) = source_hasher {
            let src_hash = src_h.finalize();
            Self::verify_dest_integrity(to, entry, size, &src_hash).await?;
        }

        if !is_source_reserved {
            from.delete_file(entry).await?;
        }

        Ok(())
    }

    // ========================================================
    // 字节级续传三段式 API（issue #21）：resume_prepare / read_chunk_stream /
    // write_chunk_stream / commit_chunk_stream。
    //
    // 拆分动机：双进程场景下 Receiver 持目标端（负责 prepare 定缺失区间 +
    // write 落盘）、Sender 持源端（负责按缺失区间 read），三段必须能独立
    // 跨 transport 调用，不能像融合式 `copy_file_resumable` 那样揉在一个
    // 进程内完成。内部全部复用各后端已有的 `write_data_resumable` /
    // `prepare_resumable_upload` / `finalize_resumable_upload` /
    // `set_file_len` / `rename`，不新写落盘逻辑；S3 内部实现零改动，仅在此
    // 处新增公开壳做 dispatch。
    // ========================================================

    /// ① 准备：确定临时载体 + 反推/加载缺失区间。
    ///
    /// - S3 目标端：`resume` 参数无意义——S3 自身状态（ListParts）即续传进度
    ///   真值，直接复用 `prepare_resumable_upload`（无 in-progress upload 时
    ///   自动视为全新，等价于 `resume=false`）。
    /// - NAS 目标端：`resume=false` 或 `.part` 不存在时，missing 为全量
    ///   `[(0, size)]`；`.part` 存在时按其当前文件长度反推：
    ///   `len < size` → `[(len, size)]`（续传剩余部分）；
    ///   `len == size` → `[]`（已写满，无需再传）；
    ///   `len > size` → `[(0, size)]`（残留脏数据，视为不可信，全量重写）。
    pub async fn resume_prepare(
        dest: &StorageEnum,
        entry: &EntryEnum,
        part_path: &Path,
        resume: bool,
    ) -> Result<(Vec<(u64, u64)>, StreamHandle)> {
        let size = entry.get_size();

        if let StorageEnum::S3(to_s3) = dest {
            let (dst_rel, tags) = match entry {
                EntryEnum::S3(e) => (e.relative_path.clone(), e.tags.clone()),
                EntryEnum::NAS(e) => (path_to_s3_key(&e.relative_path).into_owned(), None),
            };
            let part_size = to_s3.resume_part_size(size);
            let (upload_id, missing) = to_s3
                .prepare_resumable_upload(&dst_rel, size, part_size, tags.as_ref())
                .await?;
            return Ok((
                missing,
                StreamHandle::S3 {
                    upload_id,
                    part_size,
                    dst_key: dst_rel,
                },
            ));
        }

        let missing = if resume {
            match dest.get_metadata(part_path).await {
                Ok(existing) => {
                    let existing_len = existing.get_size();
                    match existing_len.cmp(&size) {
                        std::cmp::Ordering::Less => vec![(existing_len, size)],
                        std::cmp::Ordering::Equal => vec![],
                        std::cmp::Ordering::Greater => vec![(0, size)],
                    }
                }
                Err(_) => vec![(0, size)], // .part 不存在：视为全新
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

    /// ② 写：从 `rx` 收 `DataChunk` 写入临时载体（`.part` 或 multipart
    /// upload），每 chunk/part 落盘确认后触发 `on_committed`。不做提交
    /// （rename/Complete）、不做 hash 校验、不删源。
    pub async fn write_chunk_stream(
        dest: &StorageEnum,
        entry: &EntryEnum,
        rx: mpsc::Receiver<DataChunk>,
        handle: &StreamHandle,
        bytes_counter: Option<Arc<AtomicU64>>,
        on_committed: CommitCallback,
    ) -> Result<()> {
        match handle {
            StreamHandle::S3 {
                upload_id,
                part_size,
                dst_key,
            } => {
                let StorageEnum::S3(to_s3) = dest else {
                    return Err(StorageError::OperationError(
                        "write_chunk_stream: S3 StreamHandle requires an S3 destination"
                            .to_string(),
                    ));
                };
                to_s3
                    .write_data_resumable(
                        rx,
                        dst_key,
                        entry.get_size(),
                        *part_size,
                        upload_id,
                        bytes_counter,
                        on_committed,
                    )
                    .await
            }
            StreamHandle::Nas { part_path } => match (dest, entry) {
                (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::Local(s), EntryEnum::S3(_)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        None,
                        None,
                        None,
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::NFS(s), EntryEnum::S3(_)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        None,
                        None,
                        None,
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        e.uid,
                        e.gid,
                        Some(e.mode),
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::CIFS(s), EntryEnum::S3(_)) => {
                    s.write_data_resumable(
                        rx,
                        part_path,
                        None,
                        None,
                        None,
                        bytes_counter,
                        on_committed,
                    )
                    .await
                }
                (StorageEnum::S3(_), _) => Err(StorageError::OperationError(
                    "write_chunk_stream: Nas StreamHandle used with an S3 destination".to_string(),
                )),
            },
        }
    }

    /// 对称只读：源端按缺失区间（`intervals=Some`，续传）或整文件（`intervals=None`，
    /// 全量）分块读，`rx` 转发给 transport；`intervals=None` 时返回的
    /// `JoinHandle` 收尾带上整文件 hash（`enable_integrity_check` 时）。
    /// `intervals=Some` 时续传无需逐块 hash（完整性走收尾的端到端校验），
    /// 恒返回 `None`。
    pub fn read_chunk_stream(
        from: &StorageEnum,
        entry: &EntryEnum,
        intervals: Option<Vec<(u64, u64)>>,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        capacity: usize,
    ) -> (
        mpsc::Receiver<DataChunk>,
        tokio::task::JoinHandle<Result<Option<HashCalculator>>>,
    ) {
        let (tx, rx) = mpsc::channel::<DataChunk>(capacity);
        let from_c = from.clone();
        let entry_c = entry.clone();
        let handle = tokio::spawn(async move {
            match intervals {
                Some(ivals) => match (&from_c, &entry_c) {
                    (StorageEnum::Local(s), EntryEnum::NAS(e)) => s
                        .read_data_intervals(tx, &e.relative_path, &ivals, qos)
                        .await
                        .map(|()| None),
                    (StorageEnum::NFS(s), EntryEnum::NAS(e)) => s
                        .read_data_intervals(tx, &e.relative_path, &ivals, qos)
                        .await
                        .map(|()| None),
                    (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => s
                        .read_data_intervals(tx, &e.relative_path, &ivals, qos)
                        .await
                        .map(|()| None),
                    (StorageEnum::S3(s), EntryEnum::S3(e)) => s
                        .read_data_intervals(tx, &e.relative_path, &ivals, qos)
                        .await
                        .map(|()| None),
                    _ => Err(StorageError::OperationError(format!(
                        "read_chunk_stream: unsupported source/entry combination: {entry_c:?}"
                    ))),
                },
                None => {
                    let size = entry_c.get_size();
                    match (&from_c, &entry_c) {
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
                            "read_chunk_stream: unsupported source/entry combination: {entry_c:?}"
                        ))),
                    }
                }
            }
        });
        (rx, handle)
    }

    /// ③ 提交：hash 校验通过后调用方触发原子提交（NAS `set_file_len` + `rename`；
    /// S3 `CompleteMultipartUpload`）。
    pub async fn commit_chunk_stream(
        dest: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        handle: StreamHandle,
    ) -> Result<()> {
        match handle {
            StreamHandle::Nas { part_path } => {
                dest.set_file_len(&part_path, size).await?;
                dest.rename(&part_path, entry.get_relative_path()).await
            }
            StreamHandle::S3 {
                upload_id,
                part_size,
                dst_key,
            } => {
                let StorageEnum::S3(to_s3) = dest else {
                    return Err(StorageError::OperationError(
                        "commit_chunk_stream: S3 StreamHandle requires an S3 destination"
                            .to_string(),
                    ));
                };
                to_s3
                    .finalize_resumable_upload(&dst_key, size, part_size, &upload_id)
                    .await
            }
        }
    }

    /// 字节级断点续传复制（仅多块大文件，源端：Local/NFS/CIFS/S3，目标端：全部后端）。
    ///
    /// 与 `copy_file` 的差异：
    /// - 源端只读缺失的 offset 区间；
    /// - NAS 目标端写到 `resume.part_relative_path`（`.part`），不截断已写字节；
    ///   每个 chunk 确认落盘后回调 `resume.on_committed`（供上层持久化进度）；
    ///   收尾规整 `.part` 长度 → 可选完整性校验 → 原子 rename 成最终文件。
    /// - S3 目标端走 multipart part 粒度续传（见 [`Self::copy_file_resumable_to_s3`]），
    ///   `.part`/rename/set_file_len 模型不适用于对象存储。
    ///
    /// 进程中断时进度保留（NAS 目标：`.part` + 上层状态文件；S3 目标：in-progress
    /// multipart upload），重跑时只补未完成区间。
    pub async fn copy_file_resumable(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        is_source_reserved: bool,
        bytes_counter: Option<Arc<AtomicU64>>,
        resume: ResumeContext,
    ) -> Result<()> {
        if matches!(to, StorageEnum::S3(_)) {
            return Self::copy_file_resumable_to_s3(
                from,
                to,
                entry,
                qos,
                enable_integrity_check,
                is_source_reserved,
                bytes_counter,
                resume.on_committed,
            )
            .await;
        }

        let size = entry.get_size();
        let ResumeContext {
            part_relative_path,
            missing_intervals,
            on_committed,
        } = resume;
        // NAS 目标端：沿用调用方给定的 missing_intervals（不重新用 resume_prepare
        // 推断——融合式 API 向后兼容，caller 的状态文件才是既有行为下的进度真值）。
        let handle = StreamHandle::Nas {
            part_path: part_relative_path.clone(),
        };

        // ── 源端：只读缺失区间（read_chunk_stream 内部 spawn）──
        let (rx, read_handle) = Self::read_chunk_stream(
            from,
            entry,
            Some(missing_intervals),
            qos,
            false,
            COPY_PIPELINE_CAPACITY,
        );

        // ── 目标端：续写 .part ──
        let to_c = to.clone();
        let entry_w = entry.clone();
        let handle_w = handle.clone();
        let write_handle = tokio::spawn(async move {
            Self::write_chunk_stream(&to_c, &entry_w, rx, &handle_w, bytes_counter, on_committed)
                .await
        });

        let read_res = read_handle
            .await
            .map_err(|e| StorageError::OperationError(format!("read task panicked: {e:?}")))?;
        let write_res = write_handle
            .await
            .map_err(|e| StorageError::OperationError(format!("write task panicked: {e:?}")))?;
        read_res?;
        write_res?;

        // ── hash 比对（早于 commit：NAS `.part` 可独立读取，校验失败时最终路径
        //    不会被 rename 污染，见 T5）──
        if enable_integrity_check {
            let src_hash = from.compute_hash(entry.get_relative_path(), size).await?;
            let dst_hash = to.compute_hash(&part_relative_path, size).await?;
            if src_hash != dst_hash {
                return Err(StorageError::OperationError(
                    "integrity check failed: source and destination hashes differ".to_string(),
                ));
            }
        }

        // ── 提交：规整长度 + 原子 rename ──
        Self::commit_chunk_stream(to, entry, size, handle).await?;

        if !is_source_reserved {
            from.delete_file(entry).await?;
        }

        Ok(())
    }

    /// S3 目标端字节级断点续传：multipart upload part 粒度。
    ///
    /// 进度真值是目标端 in-progress multipart upload 本身（`resume_prepare` 内部
    /// ListParts 反推缺失区间），不使用上层状态文件传入的 `missing_intervals`——
    /// upload 可能被外部（lifecycle 规则、手动 abort）清掉，且上层记录只可能滞后
    /// 于真实进度，以目标端反推永远正确。`on_committed` 仍逐 part 回调，供上层
    /// 记录进度；`.part` 路径与 rename/set_file_len 不适用于对象存储，均不使用。
    ///
    /// 失败时**不 abort** upload，已上传 parts 即续传进度；成功时
    /// `CompleteMultipartUpload` 原子生效，目标端不存在半截可见对象。
    /// Complete 之前有写端本地会话字节断言（issue #58）：本次确认上传的字节数
    /// 不等于缺失区间总和（如源读截断）则不提交，坏对象根本不落地。
    ///
    /// hash 比对晚于 `commit_chunk_stream`（`CompleteMultipartUpload`）——
    /// in-progress multipart 的 parts 在 Complete 前不能作为一个连续对象读取，
    /// 这是对象存储的固有限制，维持现状顺序（区别于 NAS 分支的「先 hash 后
    /// commit」）。
    #[allow(clippy::too_many_arguments)]
    async fn copy_file_resumable_to_s3(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        is_source_reserved: bool,
        bytes_counter: Option<Arc<AtomicU64>>,
        on_committed: crate::CommitCallback,
    ) -> Result<()> {
        if !matches!(to, StorageEnum::S3(_)) {
            return Err(StorageError::OperationError(
                "copy_file_resumable_to_s3 requires an S3 destination".to_string(),
            ));
        }
        let size = entry.get_size();

        // part_path 对 S3 分支无意义（resume_prepare 内部按 dest 类型分流，S3
        // 分支不使用该参数），传入 entry 自身路径仅作占位。
        let (missing, handle) =
            Self::resume_prepare(to, entry, entry.get_relative_path(), true).await?;

        // 写端本地会话计数（issue #58）：wrap on_committed 累计本次确认上传的
        // 字节数（零额外存储 RPC），供 CompleteMultipartUpload 前的 size 断言。
        let expected_session_bytes: u64 = missing.iter().map(|(start, end)| end - start).sum();
        let session_bytes = Arc::new(AtomicU64::new(0));
        let on_committed: CommitCallback = {
            let session_bytes = session_bytes.clone();
            let inner = on_committed;
            Arc::new(move |offset, len| {
                session_bytes.fetch_add(len, Ordering::Relaxed);
                inner(offset, len);
            })
        };

        // ── 源端：只读缺失区间（以目标端 ListParts 反推为准）──
        let (rx, read_handle) = Self::read_chunk_stream(
            from,
            entry,
            Some(missing),
            qos,
            false,
            COPY_PIPELINE_CAPACITY,
        );

        // ── 目标端：缺失 parts 并发 UploadPart ──
        let to_c = to.clone();
        let entry_w = entry.clone();
        let handle_w = handle.clone();
        let write_handle = tokio::spawn(async move {
            Self::write_chunk_stream(&to_c, &entry_w, rx, &handle_w, bytes_counter, on_committed)
                .await
        });

        let read_res = read_handle
            .await
            .map_err(|e| StorageError::OperationError(format!("read task panicked: {e:?}")))?;
        let write_res = write_handle
            .await
            .map_err(|e| StorageError::OperationError(format!("write task panicked: {e:?}")))?;
        read_res?;
        write_res?;

        // 本地字节计数断言（issue #58，前移到 CompleteMultipartUpload 之前）：
        // 本次会话确认上传的字节数必须恰好补齐全部缺失区间，不等则不提交——
        // 坏对象根本不落地。沿用「失败不 abort」设计：已上传 parts 是合法续传
        // 进度，保留供重试补齐（源 size 变更场景由 prepare_resumable_upload 的
        // stale upload 处理收拾）。
        let uploaded = session_bytes.load(Ordering::Relaxed);
        if uploaded != expected_session_bytes {
            return Err(StorageError::OperationError(format!(
                "size check failed before multipart completion: session uploaded {uploaded} bytes, missing intervals require {expected_session_bytes}: {:?}",
                entry.get_relative_path()
            )));
        }

        // ── 提交：校验 parts 全覆盖 → CompleteMultipartUpload ──
        Self::commit_chunk_stream(to, entry, size, handle).await?;

        if enable_integrity_check {
            let src_hash = from.compute_hash(entry.get_relative_path(), size).await?;
            let dst_hash = to.compute_hash(entry.get_relative_path(), size).await?;
            if src_hash != dst_hash {
                // Complete 已提交，坏对象已可见：best-effort 清理（issue #58）。
                Self::cleanup_mismatched_dest(to, entry).await;
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
        from: &StorageEnum,
        to: &StorageEnum,
        entries: &[Arc<EntryEnum>],
        tar_path: &Path,
        tar_size: u64,
        tar_mtime: i64,
        qos: Option<QosManager>,
        bytes_counter: Option<Arc<AtomicU64>>,
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
                StorageEnum::Local(s) => {
                    s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w)
                        .await
                }
                StorageEnum::NFS(s) => {
                    s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w)
                        .await
                }
                StorageEnum::CIFS(s) => {
                    s.write_data(rx, &tar_path_buf, None, None, None, bytes_counter_w)
                        .await
                }
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
                        let (sub_tx, mut sub_rx) =
                            mpsc::channel::<DataChunk>(HASH_CHANNEL_CAPACITY);
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
                                    "tar write channel closed during large file transfer"
                                        .to_string(),
                                ));
                            }
                            offset += chunk_len;
                        }

                        // 等待读取任务完成并检查错误
                        read_task.await.map_err(|e| {
                            StorageError::OperationError(format!("read task panicked: {e:?}"))
                        })??;
                    }

                    // 发送 padding
                    if let Some(padding) = tar_padding(file_size) {
                        let padding_len = padding.len() as u64;
                        if tx
                            .send(DataChunk {
                                offset,
                                data: padding,
                            })
                            .await
                            .is_err()
                        {
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
        write_task.await.map_err(|e| {
            StorageError::OperationError(format!("tar write task panicked: {e:?}"))
        })??;

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
    pub async fn write_file_from_bytes(
        to: &StorageEnum,
        entry: &EntryEnum,
        data: Bytes,
    ) -> Result<()> {
        match (to, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                    .await
            }
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                    .await
            }
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                s.write_file(&e.relative_path, data, e.uid, e.gid, Some(e.mode))
                    .await
            }
            (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                s.write_file(&e.relative_path, data, e.mtime, e.tags.clone())
                    .await
            }
            (StorageEnum::Local(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None)
                    .await
            }
            (StorageEnum::NFS(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None)
                    .await
            }
            (StorageEnum::CIFS(s), EntryEnum::S3(e)) => {
                s.write_file(Path::new(&e.relative_path), data, None, None, None)
                    .await
            }
            (StorageEnum::S3(s), EntryEnum::NAS(e)) => {
                s.write_file(&path_to_s3_key(&e.relative_path), data, e.mtime, None)
                    .await
            }
        }
    }

    /// 从源端分块读取文件数据到 channel（内部辅助方法）
    async fn read_data_from(
        from: &StorageEnum,
        entry: &EntryEnum,
        tx: mpsc::Sender<DataChunk>,
        size: u64,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        match (from, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
                s.read_data(tx, &e.relative_path, size, false, qos).await
            }
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
                s.read_data(tx, &e.relative_path, size, false, qos).await
            }
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
                s.read_data(tx, &e.relative_path, size, false, qos).await
            }
            (StorageEnum::S3(s), EntryEnum::S3(e)) => {
                s.read_data(tx, &e.relative_path, size, false, qos).await
            }
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

    /// 后端是否拥有真实的目录对象（具有独立 inode/元数据）。
    ///
    /// - `true`：NFS / CIFS / Local — 目录是一等对象，可读写 mode/uid/gid/atime/mtime；
    /// - `false`：S3 — 目录仅作为 key prefix 的隐式存在，没有自身元数据。
    ///
    /// 调用方（如 integrity-check 的目录元数据校验、tar_pack 的目录条目写入）
    /// 据此决定是否跳过目录元数据相关步骤。
    pub fn has_real_directory_objects(&self) -> bool {
        !matches!(self, StorageEnum::S3(_))
    }

    /// 从源端复制 ACL（非继承的显式 ACE + 继承保护状态）到目标端
    ///
    /// 支持组合：
    /// - Local → Local（仅 Windows，Win32 API）
    /// - CIFS → CIFS（跨平台，smb-rs 直通）
    /// - NFS → NFS（仅当双方都支持 ACL，即 `NFSv4+`）
    /// - 跨类型或不支持的组合静默跳过
    pub async fn copy_acl(
        from: &StorageEnum,
        to: &StorageEnum,
        relative_path: &Path,
    ) -> Result<()> {
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
    pub async fn copy_xattr(
        from: &StorageEnum,
        to: &StorageEnum,
        relative_path: &Path,
    ) -> Result<()> {
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
                            warn!(
                                "Failed to list xattr for {:?}, skipping: {}",
                                relative_path, e
                            );
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
                    s.set_xattr(relative_path, &name, Bytes::from(value))
                        .await?;
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
fn read_bytes_checked(
    cursor: &mut io::Cursor<&[u8]>,
    len: usize,
    max: usize,
    context: &str,
) -> Result<Vec<u8>> {
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
            v => {
                return Err(StorageError::OperationError(format!(
                    "unknown ACE type: {v}"
                )));
            }
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
        let value_buf =
            read_bytes_checked(&mut cursor, value_len, MAX_XATTR_VALUE_LEN, "xattr value")?;

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

/// 根据路径前缀创建对应的存储实例
///
/// `ensure_dir = true` 用于目标端：prefix 目录不存在时自动创建；
/// `ensure_dir = false` 用于源端：prefix 不存在时报错。
/// S3 无目录概念，该参数对其无效果。
pub async fn create_storage(
    path: &str,
    block_size: Option<u64>,
    ensure_dir: bool,
) -> Result<StorageEnum> {
    let storage_type = detect_storage_type(path);
    debug!(
        "Creating {:?} storage for path: {} (ensure_dir={})",
        storage_type, path, ensure_dir
    );
    match storage_type {
        StorageType::Cifs => create_cifs_storage(path, block_size, ensure_dir).await,
        StorageType::Nfs => create_nfs_storage(path, block_size, ensure_dir).await,
        StorageType::S3 => create_s3_storage(path, block_size).await,
        StorageType::Local => create_local_storage(path, block_size, ensure_dir),
    }
}
