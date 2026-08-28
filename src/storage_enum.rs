use std::borrow::Cow;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// 哈希计算 / 大文件读取 pipeline 的 channel 容量（读写并行，4 个 chunk 缓冲）
pub(crate) const HASH_CHANNEL_CAPACITY: usize = 4;
/// 文件拷贝 read→write pipeline 的 channel 容量。
///
/// 读写两端各自有 inflight pipeline（如 NFS 读 4 / 写 8），channel 是两级
/// 之间的解耦缓冲：容量 2 时写端一次落盘抖动即填满 channel、反压打空读端
/// 流水线；4 可吸收单次抖动。内存上界 = 容量 × chunk 大小 × 并发文件数
/// （NFS chunk ≤ 1MB；CIFS chunk 可达 8MB，增大容量时需关注）。
pub(crate) const COPY_PIPELINE_CAPACITY: usize = 4;
/// TAR 打包 pipeline 的 channel 容量（多文件顺序读，适当放大缓冲）
const TAR_PIPELINE_CAPACITY: usize = 16;
#[derive(Clone)]
pub(crate) struct WriteProgress {
    pub bytes_counter: Option<Arc<AtomicU64>>,
    pub on_committed: CommitCallback,
}

async fn copy_s3_to_s3_native(
    src: &S3Storage,
    dst: &S3Storage,
    entry: &crate::S3Entry,
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

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[cfg(windows)]
use crate::acl;
use crate::checksum::HashCalculator;
use crate::cifs::{CifsStorage, create_cifs_storage};
use crate::error::StorageError;
use crate::filter::FilterExpression;
use crate::hdfs::{HDFSStorage, create_hdfs_storage};
use crate::local::{LocalStorage, create_local_storage};
use crate::nfs::{NFSStorage, create_nfs_storage};
use crate::qos::QosManager;
use crate::s3::{S3Storage, create_s3_storage};
pub use crate::storage_options::{
    BackendConfig, CopyOptions, CreateStorageOptions, TarPackOptions, WalkOptions,
};
use crate::tar_pack::{build_header_for_entry, tar_eof_marker, tar_padding};
use crate::{
    CommitCallback, DataChunk, DeleteDirIterator, EntryEnum, Result, ResumeContext,
    TransferConcurrency, WalkDirAsyncIterator, WalkDirAsyncIterator2,
};

/// 存储类型枚举
#[derive(Debug, PartialEq, Eq)]
pub enum StorageType {
    Local,
    Nfs,
    S3,
    Cifs,
    Hdfs,
}

/// 统一的存储枚举类型
#[derive(Clone, Debug)]
pub enum StorageEnum {
    Local(LocalStorage),
    NFS(NFSStorage),
    S3(S3Storage),
    CIFS(CifsStorage),
    HDFS(HDFSStorage),
}

/// 字节级续传的目标端流式写句柄（issue #21：`resume_prepare` 产出，
/// `write_chunk_stream`/`commit_chunk_stream` 消费）。跨 transport 传递
/// （双进程场景下 Receiver 侧 prepare、由 Sender 侧对称使用同一份区间信息），
/// 故派生 `Serialize`/`Deserialize`。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
    /// HDFS tail-only resume state transferable to a separate receiver process.
    Hdfs {
        part_path: PathBuf,
        prefix_len: u64,
        expected_size: u64,
    },
}

impl StorageEnum {
    /// 验证存储连通性
    ///
    /// - Local: 检查根路径是否存在且可访问
    /// - NFS: 创建成功即已连通（mount 操作在构造时完成）
    /// - S3: 执行 `HeadBucket` 验证 bucket 可访问性及凭据有效性
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
            StorageEnum::HDFS(storage) => storage
                .client()
                .get_file_info(storage.location().root())
                .await
                .map(|_| ())
                .map_err(|error| StorageError::OperationError(error.to_string())),
        }
    }

    /// 探测存储服务端时间
    ///
    /// 在存储上写入临时文件 → 读取 mtime → 删除 → 返回服务端时间戳（秒）。
    /// 本地存储返回 None（mtime 等于系统时钟，无校验意义）。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn probe_server_time(&self) -> Result<Option<i64>> {
        let nanos = crate::time_util::now_nanos();
        let tmp_name = format!(".~ts_{nanos:x}");

        match self {
            StorageEnum::Local(_) => Ok(None),
            StorageEnum::NFS(s) => {
                let tmp_path = PathBuf::from(&tmp_name);
                s.write_file(&tmp_path, Bytes::from_static(b"\0"), None, None, None)
                    .await?;
                let entry = Box::pin(s.get_metadata(&tmp_path)).await?;
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
                let entry = Box::pin(s.get_metadata(&tmp_path)).await?;
                let mtime = entry.get_mtime();
                let _ = s.delete_file(&tmp_path).await;
                Ok(Some(mtime))
            }
            StorageEnum::HDFS(s) => {
                let tmp_path = PathBuf::from(&tmp_name);
                s.write_file(&tmp_path, Bytes::from_static(b"\0"), 0o600, None)
                    .await?;
                let result = s
                    .get_metadata(&tmp_path)
                    .await
                    .map(|entry| Some(entry.mtime));
                let cleanup = s.delete_file(&tmp_path).await;
                match (result, cleanup) {
                    (Ok(mtime), Ok(())) => Ok(mtime),
                    (Err(error), _) | (Ok(_), Err(error)) => Err(error),
                }
            }
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
            (StorageEnum::HDFS(storage), entry) => {
                storage.delete_file(entry.get_relative_path()).await
            }
            (StorageEnum::Local(storage), EntryEnum::HDFS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
            (StorageEnum::NFS(storage), EntryEnum::HDFS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::HDFS(entry)) => {
                storage.delete_file(&entry.relative_path).await
            }
            (StorageEnum::S3(storage), EntryEnum::HDFS(entry)) => {
                let key = storage.build_full_key(&path_to_s3_key(&entry.relative_path));
                storage.delete_object(&key).await
            }
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn create_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        match (self, entry) {
            (StorageEnum::HDFS(storage), entry) => {
                let mode = entry.get_mode().unwrap_or(0o755) & 0o7777;
                let mode = u16::try_from(mode).map_err(|_| {
                    StorageError::InvalidPath(format!("invalid HDFS directory mode: {mode:o}"))
                })?;
                storage
                    .create_dir_all(entry.get_relative_path(), mode)
                    .await
            }
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
            // s3 storage has no directory concept.
            (StorageEnum::S3(_), _) => Ok(()),
            (StorageEnum::Local(storage), EntryEnum::HDFS(entry)) => {
                storage.create_dir_all(&entry.relative_path).await
            }
            (StorageEnum::NFS(storage), EntryEnum::HDFS(entry)) => storage
                .create_dir_all(&entry.relative_path)
                .await
                .map(|_| ()),
            (StorageEnum::CIFS(storage), EntryEnum::HDFS(entry)) => {
                storage.create_dir_all(&entry.relative_path).await
            }
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn delete_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        if let StorageEnum::HDFS(storage) = self {
            return storage.delete_dir_all(entry.get_relative_path()).await;
        }
        let iter = self.delete_dir_all_with_progress(Some(entry.get_relative_path()), 4)?;
        while let Some(event) = iter.next().await {
            if let Some(error) = event.error {
                return Err(StorageError::OperationError(error));
            }
        }
        Ok(())
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => storage.create_symlink(
                &entry.relative_path,
                target,
                entry.atime,
                entry.mtime,
                entry.uid,
                entry.gid,
            ),
            _ => Ok(()),
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn read_symlink(&self, entry: &EntryEnum) -> Result<PathBuf> {
        match (self, entry) {
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path).await
            }
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path).await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage.read_symlink(&entry.relative_path)
            }
            _ => Ok(PathBuf::new()),
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        match self {
            StorageEnum::Local(storage) => storage.get_metadata(relative_path).await,
            StorageEnum::NFS(storage) => storage.get_metadata(relative_path).await,
            StorageEnum::S3(storage) => storage.get_metadata(&path_to_s3_key(relative_path)).await,
            StorageEnum::CIFS(storage) => Box::pin(storage.get_metadata(relative_path)).await,
            StorageEnum::HDFS(storage) => storage
                .get_metadata(relative_path)
                .await
                .map(EntryEnum::HDFS),
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn walkdir(
        &self,
        sub_path: Option<&Path>,
        options: WalkOptions,
    ) -> Result<WalkDirAsyncIterator> {
        match self {
            StorageEnum::Local(s) => s.walkdir(sub_path, options),
            StorageEnum::NFS(s) => s.walkdir(sub_path, options).await,
            StorageEnum::S3(s) => {
                let key = sub_path.map(|p| path_to_s3_key(p));
                s.walkdir(key.as_deref(), options)
            }
            StorageEnum::CIFS(s) => s.walkdir(sub_path, options),
            StorageEnum::HDFS(storage) => storage.walkdir(sub_path, options),
        }
    }

    /// `walkdir_2`: 目录分页遍历，DFS 顺序分配 NDX，页级输出
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
            StorageEnum::Local(s) => s.walkdir_2(
                sub_path,
                depth,
                match_expressions,
                exclude_expressions,
                concurrency,
            ),
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
            }
            StorageEnum::CIFS(s) => s.walkdir_2(
                sub_path,
                depth,
                match_expressions,
                exclude_expressions,
                concurrency,
            ),
            StorageEnum::HDFS(storage) => storage.walkdir_2(
                sub_path,
                depth,
                match_expressions,
                exclude_expressions,
                concurrency,
            ),
        }
    }

    /// Rename a file or directory within the same storage.
    ///
    /// S3 implements object rename as a server-side copy followed by deletion
    /// of the source. Object data does not pass through data-mover.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.rename_with_expected_size(from, to, None).await
    }

    /// Rename with an optional expected size for validating an idempotent S3
    /// retry. Filesystem backends perform an atomic rename and ignore the size.
    pub(crate) async fn rename_with_expected_size(
        &self,
        from: &Path,
        to: &Path,
        expected_size: Option<u64>,
    ) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.rename(from, to).await,
            StorageEnum::NFS(s) => s.rename(from, to).await,
            StorageEnum::S3(s) => s.rename_with_expected_size(from, to, expected_size).await,
            StorageEnum::CIFS(s) => s.rename(from, to).await,
            StorageEnum::HDFS(s) => s.rename(from, to).await,
        }
    }

    /// 将文件长度规整为 `len`（字节级续传收尾：截掉 `.part` 遗留尾部）。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_file_len(&self, relative_path: &Path, len: u64) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::NFS(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::CIFS(s) => s.set_file_len(relative_path, len).await,
            StorageEnum::S3(_) => Err(StorageError::OperationError(
                "S3 does not support byte-level resume".to_string(),
            )),
            StorageEnum::HDFS(_) => Err(StorageError::UnsupportedType(
                "HDFS cannot provide general POSIX set_file_len semantics; the current client also does not expose truncate"
                    .to_string(),
            )),
        }
    }

    /// 并行删除目录下所有文件和子目录，返回进度迭代器
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
            StorageEnum::HDFS(storage) => {
                let Some(path) = relative_path.map(Path::to_path_buf) else {
                    return Err(StorageError::InvalidPath(
                        "HDFS progress deletion requires an explicit path below the storage root"
                            .to_string(),
                    ));
                };
                let storage = storage.clone();
                let event_path = path.clone();
                let (tx, rx) = async_channel::bounded(1);
                tokio::spawn(async move {
                    let error = storage
                        .delete_dir_all(&path)
                        .await
                        .err()
                        .map(|error| error.to_string());
                    let _ = tx
                        .send(crate::DeleteEvent {
                            relative_path: event_path,
                            is_dir: true,
                            error,
                        })
                        .await;
                });
                Ok(DeleteDirIterator::new(rx))
            }
        }
    }

    /// Compute BLAKE3 hash of a file by streaming it through the storage's `read_data`.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn compute_hash(&self, relative_path: &Path, size: u64) -> Result<String> {
        self.compute_hash_and_len(relative_path, size)
            .await
            .map(|(hash, _)| hash)
    }

    /// Copy a file with optional `QoS`, integrity verification, and cancellation.
    ///
    /// HDFS destinations use the default recoverable lifecycle. Other backend
    /// combinations are coordinated by the common copy pipeline module.
    ///
    /// # Errors
    ///
    /// Returns an error when reading, writing, verification, metadata
    /// application, source deletion, or cancellation fails.
    pub async fn copy_file(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        options: CopyOptions,
    ) -> Result<()> {
        if matches!(to, StorageEnum::HDFS(_)) {
            let recovery = crate::HdfsRecoverableCopyOptions::new(
                crate::hdfs_transfer_mapping::hdfs_default_transfer_identity(from, entry),
                Arc::new(|_, _| {}),
            );
            return Box::pin(Self::copy_file_hdfs_recoverable(
                from, to, entry, options, recovery,
            ))
            .await;
        }
        if options
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(StorageError::Cancelled);
        }
        if let (StorageEnum::S3(src), StorageEnum::S3(dst), EntryEnum::S3(e)) = (from, to, entry)
            && options.qos.is_none()
        {
            return Box::pin(copy_s3_to_s3_native(
                src,
                dst,
                e,
                from,
                entry,
                options.bytes_counter.as_ref(),
                options.is_source_reserved,
            ))
            .await;
        }
        Box::pin(Self::copy_file_inner(from, to, entry, options)).await
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn resume_prepare(
        dest: &StorageEnum,
        entry: &EntryEnum,
        part_path: &Path,
        resume: bool,
    ) -> Result<(Vec<(u64, u64)>, StreamHandle)> {
        if let StorageEnum::HDFS(storage) = dest {
            return crate::hdfs_legacy_resume::prepare(storage, entry, part_path, resume).await;
        }

        if let StorageEnum::S3(to_s3) = dest {
            return crate::storage_resume_compat::prepare_s3(to_s3, entry).await;
        }
        crate::storage_resume_compat::prepare_nas(dest, entry, part_path, resume).await
    }

    /// ② 写：从 `rx` 收 `DataChunk` 写入临时载体（`.part` 或 multipart
    /// upload），每 chunk/part 落盘确认后触发 `on_committed`。不做提交
    /// （rename/Complete）、不做 hash 校验、不删源。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
                crate::storage_resume_compat::write_s3(
                    to_s3,
                    entry,
                    rx,
                    upload_id,
                    *part_size,
                    dst_key,
                    WriteProgress {
                        bytes_counter,
                        on_committed,
                    },
                )
                .await
            }
            StreamHandle::Nas { part_path } => {
                crate::storage_resume_compat::write_nas(
                    dest,
                    entry,
                    rx,
                    part_path,
                    bytes_counter,
                    on_committed,
                )
                .await
            }
            StreamHandle::Hdfs {
                part_path,
                prefix_len,
                expected_size,
            } => {
                let StorageEnum::HDFS(storage) = dest else {
                    return Err(StorageError::OperationError(
                        "write_chunk_stream: HDFS handle requires an HDFS destination".to_string(),
                    ));
                };
                crate::hdfs_legacy_resume::write(
                    storage,
                    entry,
                    rx,
                    StreamHandle::Hdfs {
                        part_path: part_path.clone(),
                        prefix_len: *prefix_len,
                        expected_size: *expected_size,
                    },
                    bytes_counter,
                    on_committed,
                )
                .await
            }
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
            if let Some(ivals) = intervals {
                match (&from_c, &entry_c) {
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
                        .read_data_intervals_version(
                            tx,
                            &e.relative_path,
                            e.version_id.as_deref(),
                            &ivals,
                            qos,
                        )
                        .await
                        .map(|()| None),
                    (StorageEnum::HDFS(s), EntryEnum::HDFS(e)) => s
                        .read_data_intervals(tx, &e.relative_path, &ivals, qos)
                        .await
                        .map(|()| None),
                    _ => Err(StorageError::OperationError(format!(
                        "read_chunk_stream: unsupported source/entry combination: {entry_c:?}"
                    ))),
                }
            } else {
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
                        s.read_data_version(
                            tx,
                            &e.relative_path,
                            e.version_id.as_deref(),
                            size,
                            enable_integrity_check,
                            qos,
                        )
                        .await
                    }
                    (StorageEnum::HDFS(s), EntryEnum::HDFS(e)) => {
                        s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                            .await
                    }
                    _ => Err(StorageError::OperationError(format!(
                        "read_chunk_stream: unsupported source/entry combination: {entry_c:?}"
                    ))),
                }
            }
        });
        (rx, handle)
    }

    /// ③ 提交：hash 校验通过后调用方触发原子提交（NAS `set_file_len` + `rename`；
    /// S3 `CompleteMultipartUpload`），随后写回源条目的元数据。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn commit_chunk_stream(
        dest: &StorageEnum,
        entry: &EntryEnum,
        size: u64,
        handle: StreamHandle,
    ) -> Result<()> {
        match handle {
            StreamHandle::Nas { part_path } => {
                return crate::storage_resume_compat::commit_nas(dest, entry, size, &part_path)
                    .await;
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
                return crate::storage_resume_compat::commit_s3(
                    to_s3, dest, entry, size, &upload_id, part_size, &dst_key,
                )
                .await;
            }
            StreamHandle::Hdfs {
                part_path,
                prefix_len,
                expected_size,
            } => {
                let StorageEnum::HDFS(_) = dest else {
                    return Err(StorageError::OperationError(
                        "commit_chunk_stream: HDFS handle requires an HDFS destination".to_string(),
                    ));
                };
                return crate::hdfs_legacy_resume::commit(
                    dest,
                    entry,
                    size,
                    StreamHandle::Hdfs {
                        part_path,
                        prefix_len,
                        expected_size,
                    },
                )
                .await;
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
    /// - S3 目标端走 compatibility 模块中的 multipart part 粒度续传，
    ///   `.part`/`rename/set_file_len` 模型不适用于对象存储。
    ///
    /// 进程中断时进度保留（NAS 目标：`.part` + 上层状态文件；S3 目标：in-progress
    /// multipart upload），重跑时只补未完成区间。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn copy_file_resumable(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        options: CopyOptions,
        resume: ResumeContext,
    ) -> Result<()> {
        if options
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(StorageError::Cancelled);
        }
        if matches!(to, StorageEnum::HDFS(_)) {
            return Box::pin(crate::hdfs_legacy_resume::copy_file_resumable(
                from, to, entry, options, resume,
            ))
            .await;
        }
        if matches!(to, StorageEnum::S3(_)) {
            return Box::pin(crate::storage_resume_compat::copy_s3(
                from,
                to,
                entry,
                options,
                resume.on_committed,
            ))
            .await;
        }
        Box::pin(crate::storage_resume_compat::copy_nas(
            from, to, entry, options, resume,
        ))
        .await
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn pack_files_to_tar(
        from: &StorageEnum,
        to: &StorageEnum,
        entries: &[Arc<EntryEnum>],
        tar_path: &Path,
        tar_size: u64,
        tar_mtime: i64,
        options: TarPackOptions,
    ) -> Result<()> {
        let TarPackOptions { qos, bytes_counter } = options;
        // 从 tar_path 推导出被打包目录的路径（去掉 .tar 扩展名）
        let base_path = tar_path.with_extension("");
        let (tx, rx) = mpsc::channel::<DataChunk>(TAR_PIPELINE_CAPACITY);
        let to_c = to.clone();
        let tar_path_buf = tar_path.to_path_buf();
        let bytes_counter_w = bytes_counter.clone();
        let write_task = tokio::spawn(async move {
            Self::write_tar_stream(
                &to_c,
                rx,
                &tar_path_buf,
                tar_size,
                tar_mtime,
                bytes_counter_w,
            )
            .await
        });

        // ── Producer: iterate entries, send headers + data + padding ──
        let mut offset = 0u64;
        for entry in entries {
            let header_bytes = Self::build_tar_header(from, entry, &base_path).await;
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
                    let (sub_tx, mut sub_rx) = mpsc::channel(HASH_CHANNEL_CAPACITY);
                    let read_task = Self::spawn_tar_read(
                        from.clone(),
                        entry.clone(),
                        sub_tx,
                        file_size,
                        qos.clone(),
                    );

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
                                "tar write channel closed during file transfer".to_string(),
                            ));
                        }
                        offset += chunk_len;
                    }

                    read_task.await.map_err(|e| {
                        StorageError::OperationError(format!("read task panicked: {e:?}"))
                    })??;

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

        Self::finish_tar_stream(tx, offset, write_task).await
    }

    async fn finish_tar_stream(
        tx: mpsc::Sender<DataChunk>,
        offset: u64,
        write_task: tokio::task::JoinHandle<Result<u64>>,
    ) -> Result<()> {
        tx.send(DataChunk {
            offset,
            data: tar_eof_marker(),
        })
        .await
        .map_err(|_| {
            StorageError::OperationError("tar write channel closed during EOF send".to_string())
        })?;
        drop(tx);
        write_task.await.map_err(|error| {
            StorageError::OperationError(format!("tar write task panicked: {error:?}"))
        })??;
        Ok(())
    }

    async fn build_tar_header(storage: &StorageEnum, entry: &EntryEnum, base_path: &Path) -> Bytes {
        let link_target = if entry.get_is_symlink() {
            storage.read_symlink(entry).await.map_or_else(
                |error| {
                    warn!(
                        "Failed to read symlink target for {:?}: {}",
                        entry.get_relative_path(),
                        error
                    );
                    String::new()
                },
                |target| target.to_string_lossy().to_string(),
            )
        } else {
            String::new()
        };
        let internal_path = entry
            .get_relative_path()
            .strip_prefix(base_path)
            .unwrap_or(entry.get_relative_path())
            .iter()
            .map(|component| component.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        build_header_for_entry(entry, &internal_path, &link_target)
    }

    async fn write_tar_stream(
        storage: &StorageEnum,
        rx: mpsc::Receiver<DataChunk>,
        tar_path: &Path,
        tar_size: u64,
        tar_mtime: i64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        match storage {
            StorageEnum::Local(local) => {
                local
                    .write_data(rx, tar_path, None, None, None, bytes_counter)
                    .await
            }
            StorageEnum::NFS(nfs) => {
                nfs.write_data(rx, tar_path, None, None, None, bytes_counter)
                    .await
            }
            StorageEnum::CIFS(cifs) => {
                cifs.write_data(rx, tar_path, None, None, None, bytes_counter)
                    .await
            }
            StorageEnum::S3(s3) => {
                let tar_key = path_to_s3_key(tar_path);
                s3.write_data(rx, &tar_key, tar_size, tar_mtime, None, bytes_counter)
                    .await
            }
            StorageEnum::HDFS(hdfs) => {
                hdfs.write_data(rx, tar_path, tar_size, 0o644, None, bytes_counter)
                    .await
            }
        }
    }

    /// 读取文件完整内容（单块读取，适用于小文件或需要全量数据的场景）
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn read_file_from(from: &StorageEnum, entry: &EntryEnum, size: u64) -> Result<Bytes> {
        match (from, entry) {
            (StorageEnum::Local(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::NFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::S3(s), EntryEnum::S3(e)) => s.read_file(&e.relative_path, size).await,
            (StorageEnum::HDFS(s), EntryEnum::HDFS(e)) => s.read_file(&e.relative_path, size).await,
            _ => Err(StorageError::OperationError(format!(
                "unsupported source/entry combination for tar read: {entry:?}"
            ))),
        }
    }

    /// 将 Bytes 数据写入目标存储的指定 entry 路径
    ///
    /// 用于 delta 重建后的写入，entry 提供路径和元数据（uid/gid/mode 等）。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn write_file_from_bytes(
        to: &StorageEnum,
        entry: &EntryEnum,
        data: Bytes,
    ) -> Result<()> {
        Self::write_file_from_bytes_at(to, entry, data, entry.get_relative_path()).await
    }

    pub(crate) async fn write_file_from_bytes_at(
        to: &StorageEnum,
        entry: &EntryEnum,
        data: Bytes,
        destination_path: &Path,
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
            (StorageEnum::Local(s), EntryEnum::HDFS(e)) => {
                s.write_file(&e.relative_path, data, None, None, Some(e.mode & 0o7777))
                    .await
            }
            (StorageEnum::NFS(s), EntryEnum::HDFS(e)) => {
                s.write_file(&e.relative_path, data, None, None, Some(e.mode & 0o7777))
                    .await
            }
            (StorageEnum::CIFS(s), EntryEnum::HDFS(e)) => {
                s.write_file(&e.relative_path, data, None, None, Some(e.mode & 0o7777))
                    .await
            }
            (StorageEnum::S3(s), EntryEnum::NAS(e)) => {
                s.write_file(&path_to_s3_key(&e.relative_path), data, e.mtime, None)
                    .await
            }
            (StorageEnum::S3(s), EntryEnum::HDFS(e)) => {
                s.write_file(&path_to_s3_key(&e.relative_path), data, e.mtime, None)
                    .await
            }
            (StorageEnum::HDFS(storage), entry) => {
                let replication = match entry {
                    EntryEnum::HDFS(entry) => entry.replication,
                    EntryEnum::NAS(_) | EntryEnum::S3(_) => None,
                };
                storage
                    .write_file(
                        destination_path,
                        data,
                        entry.get_mode().unwrap_or(0o644),
                        replication,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn write_copy_data(
        to: &StorageEnum,
        entry: &EntryEnum,
        destination_path: &Path,
        rx: mpsc::Receiver<DataChunk>,
        size: u64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        match (to, entry) {
            (StorageEnum::Local(storage), EntryEnum::NAS(entry)) => {
                storage
                    .write_data(
                        rx,
                        &entry.relative_path,
                        entry.uid,
                        entry.gid,
                        Some(entry.mode),
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::Local(storage), EntryEnum::S3(entry)) => {
                storage
                    .write_data(
                        rx,
                        Path::new(&entry.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::NFS(storage), EntryEnum::NAS(entry)) => {
                storage
                    .write_data(
                        rx,
                        &entry.relative_path,
                        entry.uid,
                        entry.gid,
                        Some(entry.mode),
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::NFS(storage), EntryEnum::S3(entry)) => {
                storage
                    .write_data(
                        rx,
                        Path::new(&entry.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::CIFS(storage), EntryEnum::NAS(entry)) => {
                storage
                    .write_data(
                        rx,
                        &entry.relative_path,
                        entry.uid,
                        entry.gid,
                        Some(entry.mode),
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::CIFS(storage), EntryEnum::S3(entry)) => {
                storage
                    .write_data(
                        rx,
                        Path::new(&entry.relative_path),
                        None,
                        None,
                        None,
                        bytes_counter,
                    )
                    .await
            }
            (StorageEnum::S3(storage), EntryEnum::S3(entry)) => {
                Self::write_s3_copy_data(storage, entry, rx, size, bytes_counter).await
            }
            (StorageEnum::S3(storage), EntryEnum::NAS(entry)) => {
                Self::write_s3_nas_copy_data(storage, entry, rx, size, bytes_counter).await
            }
            (StorageEnum::S3(storage), EntryEnum::HDFS(entry)) => {
                Self::write_s3_hdfs_copy_data(storage, entry, rx, size, bytes_counter).await
            }
            (StorageEnum::HDFS(storage), entry) => {
                Box::pin(Self::write_hdfs_copy_data(
                    storage,
                    entry,
                    destination_path,
                    rx,
                    size,
                    bytes_counter,
                ))
                .await
            }
            (storage, EntryEnum::HDFS(entry)) => {
                Self::write_hdfs_copy_to_nas(storage, entry, rx, bytes_counter).await
            }
        }
    }

    async fn write_hdfs_copy_to_nas(
        storage: &StorageEnum,
        entry: &crate::HDFSEntry,
        rx: mpsc::Receiver<DataChunk>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        let mode = Some(entry.mode & 0o7777);
        match storage {
            StorageEnum::Local(storage) => {
                storage
                    .write_data(rx, &entry.relative_path, None, None, mode, bytes_counter)
                    .await
            }
            StorageEnum::NFS(storage) => {
                storage
                    .write_data(rx, &entry.relative_path, None, None, mode, bytes_counter)
                    .await
            }
            StorageEnum::CIFS(storage) => {
                storage
                    .write_data(rx, &entry.relative_path, None, None, mode, bytes_counter)
                    .await
            }
            StorageEnum::S3(_) | StorageEnum::HDFS(_) => Err(StorageError::MismatchedType),
        }
    }

    async fn write_s3_copy_data(
        storage: &crate::s3::S3Storage,
        entry: &crate::S3Entry,
        rx: mpsc::Receiver<DataChunk>,
        size: u64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        storage
            .write_data(
                rx,
                &entry.relative_path,
                size,
                entry.mtime,
                entry.tags.clone(),
                bytes_counter,
            )
            .await
    }

    async fn write_s3_hdfs_copy_data(
        storage: &crate::s3::S3Storage,
        entry: &crate::HDFSEntry,
        rx: mpsc::Receiver<DataChunk>,
        size: u64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        storage
            .write_data(
                rx,
                &path_to_s3_key(&entry.relative_path),
                size,
                entry.mtime,
                None,
                bytes_counter,
            )
            .await
    }

    async fn write_s3_nas_copy_data(
        storage: &crate::s3::S3Storage,
        entry: &crate::NASEntry,
        rx: mpsc::Receiver<DataChunk>,
        size: u64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        storage
            .write_data(
                rx,
                &path_to_s3_key(&entry.relative_path),
                size,
                entry.mtime,
                None,
                bytes_counter,
            )
            .await
    }

    async fn write_hdfs_copy_data(
        storage: &crate::hdfs::HDFSStorage,
        entry: &EntryEnum,
        destination_path: &Path,
        rx: mpsc::Receiver<DataChunk>,
        size: u64,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        let replication = match entry {
            EntryEnum::HDFS(entry) => entry.replication,
            EntryEnum::NAS(_) | EntryEnum::S3(_) => None,
        };
        storage
            .write_data(
                rx,
                destination_path,
                size,
                entry.get_mode().unwrap_or(0o644),
                replication,
                bytes_counter,
            )
            .await
    }

    /// 从源端分块读取文件数据到 channel（内部辅助方法）
    pub(crate) async fn read_data_from(
        from: &StorageEnum,
        entry: &EntryEnum,
        tx: mpsc::Sender<DataChunk>,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        match (from, entry) {
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
            (StorageEnum::HDFS(s), EntryEnum::HDFS(e)) => {
                s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                    .await
            }
            _ => Err(StorageError::OperationError(format!(
                "unsupported source/entry combination for tar read_data: {entry:?}"
            ))),
        }
    }

    async fn read_tar_data(
        from: &StorageEnum,
        entry: &EntryEnum,
        tx: mpsc::Sender<DataChunk>,
        size: u64,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        Box::pin(Self::read_data_from(from, entry, tx, size, false, qos)).await
    }

    fn spawn_tar_read(
        from: StorageEnum,
        entry: Arc<EntryEnum>,
        tx: mpsc::Sender<DataChunk>,
        size: u64,
        qos: Option<QosManager>,
    ) -> tokio::task::JoinHandle<Result<Option<HashCalculator>>> {
        tokio::spawn(
            async move { Box::pin(Self::read_tar_data(&from, &entry, tx, size, qos)).await },
        )
    }

    #[must_use]
    pub fn block_size(&self) -> u64 {
        match self {
            StorageEnum::Local(s) => s.config.block_size,
            StorageEnum::NFS(s) => s.config.block_size,
            StorageEnum::CIFS(s) => s.config.block_size,
            StorageEnum::S3(s) => s.block_size,
            StorageEnum::HDFS(s) => s.block_size(),
        }
    }

    /// Returns the configured per-file read and write concurrency.
    #[must_use]
    pub fn transfer_concurrency(&self) -> TransferConcurrency {
        match self {
            Self::Local(storage) => storage.config.transfer_concurrency,
            Self::NFS(storage) => storage.config.transfer_concurrency,
            Self::CIFS(storage) => storage.config.transfer_concurrency,
            Self::S3(storage) => storage.transfer_concurrency,
            Self::HDFS(storage) => storage.transfer_concurrency(),
        }
    }

    /// Overrides the per-file read and write concurrency for any backend.
    #[must_use]
    pub fn with_transfer_concurrency(self, concurrency: TransferConcurrency) -> Self {
        match self {
            Self::Local(storage) => Self::Local(storage.with_transfer_concurrency(concurrency)),
            Self::NFS(storage) => Self::NFS(storage.with_transfer_concurrency(concurrency)),
            Self::CIFS(storage) => Self::CIFS(storage.with_transfer_concurrency(concurrency)),
            Self::S3(storage) => Self::S3(storage.with_transfer_concurrency(concurrency)),
            Self::HDFS(storage) => Self::HDFS(storage.with_transfer_concurrency(concurrency)),
        }
    }

    #[must_use]
    pub fn is_bucket_versioned(&self) -> bool {
        matches!(self, StorageEnum::S3(storage) if storage.is_bucket_versioned)
    }

    /// 后端是否拥有真实的目录对象（具有独立 inode/元数据）。
    ///
    /// - `true`：NFS / CIFS / Local — 目录是一等对象，可读写 mode/uid/gid/atime/mtime；
    /// - `false`：S3 — 目录仅作为 key prefix 的隐式存在，没有自身元数据。
    ///
    /// 调用方（如 integrity-check `的目录元数据校验、tar_pack` 的目录条目写入）
    /// 据此决定是否跳过目录元数据相关步骤。
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
                    Ok(acl) if !acl.aces.is_empty() => Ok(Some(serialize_nfs_acl(&acl)?)),
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_xattr_bytes(&self, relative_path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageEnum::NFS(s) if s.supports_xattr() => {
                let names = match s.list_xattr(relative_path).await {
                    Ok(names) if !names.is_empty() => names,
                    _ => return Ok(None),
                };
                let mut buf = Vec::new();
                let count = u32::try_from(names.len()).map_err(|_| {
                    StorageError::OperationError("too many xattrs to serialize".to_string())
                })?;
                buf.extend_from_slice(&count.to_le_bytes());
                for name in &names {
                    let name_bytes = name.as_bytes();
                    let name_len = u32::try_from(name_bytes.len()).map_err(|_| {
                        StorageError::OperationError(
                            "xattr name is too long to serialize".to_string(),
                        )
                    })?;
                    buf.extend_from_slice(&name_len.to_le_bytes());
                    buf.extend_from_slice(name_bytes);
                    let value = s.get_xattr(relative_path, name).await?;
                    let value_len = u32::try_from(value.len()).map_err(|_| {
                        StorageError::OperationError(
                            "xattr value is too large to serialize".to_string(),
                        )
                    })?;
                    buf.extend_from_slice(&value_len.to_le_bytes());
                    buf.extend_from_slice(&value);
                }
                Ok(Some(buf))
            }
            _ => Ok(None),
        }
    }

    /// 从二进制字节设置所有 xattr（用于跨进程传输）
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
fn serialize_nfs_acl(acl: &nfs_rs::Acl) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let ace_count = u32::try_from(acl.aces.len()).map_err(|_| {
        StorageError::OperationError("too many NFS ACL entries to serialize".to_string())
    })?;
    buf.extend_from_slice(&ace_count.to_le_bytes());
    for ace in &acl.aces {
        buf.extend_from_slice(&(ace.ace_type as u32).to_le_bytes());
        buf.extend_from_slice(&ace.flags.0.to_le_bytes());
        buf.extend_from_slice(&ace.access_mask.0.to_le_bytes());
        let who_bytes = ace.who.as_bytes();
        let who_len = u32::try_from(who_bytes.len()).map_err(|_| {
            StorageError::OperationError("NFS ACL principal is too long to serialize".to_string())
        })?;
        buf.extend_from_slice(&who_len.to_le_bytes());
        buf.extend_from_slice(who_bytes);
    }
    Ok(buf)
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
pub(crate) fn path_to_s3_key(path: &Path) -> Cow<'_, str> {
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
#[must_use]
pub fn detect_storage_type(path: &str) -> StorageType {
    match path {
        p if p.starts_with("hdfs://") => StorageType::Hdfs,
        p if p.starts_with("smb://") => StorageType::Cifs,
        p if p.starts_with("nfs://") => StorageType::Nfs,
        p if p.starts_with("s3://")
            || p.starts_with("s3+http://")
            || p.starts_with("s3+https://")
            || p.starts_with("s3+sg://")
            || p.starts_with("s3+sg+https://")
            || p.starts_with("s3+dxn://")
            || p.starts_with("s3+dxn+https://")
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
///
/// # Errors
///
/// Returns an error when the requested storage operation cannot be completed.
pub async fn create_storage(path: &str, options: CreateStorageOptions) -> Result<StorageEnum> {
    let CreateStorageOptions {
        block_size,
        ensure_dir,
        backend,
    } = options;
    let storage_type = detect_storage_type(path);
    let hdfs_config = match (&storage_type, backend) {
        (StorageType::Hdfs, BackendConfig::Hdfs(config)) => Some(config),
        (StorageType::Hdfs, BackendConfig::Default) => {
            return Err(StorageError::ConfigError(
                "HDFS location requires BackendConfig::Hdfs".to_string(),
            ));
        }
        (_, BackendConfig::Hdfs(_)) => {
            return Err(StorageError::ConfigError(
                "BackendConfig::Hdfs requires an hdfs:// location".to_string(),
            ));
        }
        (_, BackendConfig::Default) => None,
    };
    debug!(
        "Creating {:?} storage for path: {} (ensure_dir={})",
        storage_type, path, ensure_dir
    );
    match storage_type {
        StorageType::Cifs => create_cifs_storage(path, block_size, ensure_dir).await,
        StorageType::Nfs => create_nfs_storage(path, block_size, ensure_dir).await,
        StorageType::S3 => create_s3_storage(path, block_size).await,
        StorageType::Hdfs => Ok(StorageEnum::HDFS(
            create_hdfs_storage(
                path,
                hdfs_config
                    .as_ref()
                    .ok_or_else(|| StorageError::ConfigError("missing HDFS config".to_string()))?,
                block_size,
                ensure_dir,
            )
            .await?,
        )),
        StorageType::Local => create_local_storage(path, block_size, ensure_dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssertTestError, AssertTestValue};
    use crate::{HDFSEntry, S3Entry};
    use filetime::FileTime;

    async fn reset_dir(dir: &str) {
        let _ = tokio::fs::remove_dir_all(dir).await;
        tokio::fs::create_dir_all(dir)
            .await
            .assert_value("test value should be present");
    }

    fn hdfs_entry(relative_path: &str, is_dir: bool) -> EntryEnum {
        EntryEnum::HDFS(HDFSEntry {
            name: relative_path.to_string(),
            relative_path: PathBuf::from(relative_path),
            is_dir,
            size: 0,
            mtime: 0,
            atime: 0,
            mode: if is_dir { 0o750 } else { 0o640 },
            owner: "hdfs-owner".to_string(),
            group: "hdfs-group".to_string(),
            replication: Some(2),
            block_size: Some(128 * 1024 * 1024),
            extension: None,
        })
    }

    #[tokio::test]
    async fn hdfs_entries_map_to_local_create_and_delete_operations() {
        let directory = "/tmp/dm-hdfs-entry-local-dispatch";
        reset_dir(directory).await;
        let storage = create_storage(directory, CreateStorageOptions::default())
            .await
            .assert_value("create local storage");

        let directory_entry = hdfs_entry("nested/created", true);
        storage
            .create_dir_all(&directory_entry)
            .await
            .assert_value("create directory from HDFS entry");
        assert!(Path::new(directory).join("nested/created").is_dir());

        let file_path = Path::new(directory).join("nested/file.bin");
        tokio::fs::write(&file_path, b"fixture")
            .await
            .assert_value("create deletion fixture");
        storage
            .delete_file(&hdfs_entry("nested/file.bin", false))
            .await
            .assert_value("delete file from HDFS entry");
        assert!(!file_path.exists());
    }

    #[test]
    fn hdfs_resume_handle_round_trips_across_process_serialization() {
        let handle = StreamHandle::Hdfs {
            part_path: PathBuf::from("目录/文件.bin.part"),
            prefix_len: 123,
            expected_size: 456,
        };
        let encoded = serde_json::to_vec(&handle).assert_value("serialize HDFS handle");
        let decoded: StreamHandle =
            serde_json::from_slice(&encoded).assert_value("deserialize HDFS handle");
        assert_eq!(decoded, handle);

        let legacy_fixture =
            r#"{"Hdfs":{"part_path":"目录/文件.bin.part","prefix_len":123,"expected_size":456}}"#;
        let decoded_fixture: StreamHandle = serde_json::from_slice(legacy_fixture.as_bytes())
            .assert_value("deserialize legacy HDFS handle fixture");
        assert_eq!(decoded_fixture, handle);
    }

    #[tokio::test]
    async fn hdfs_resume_handle_rejects_non_hdfs_destination() {
        let directory = "/tmp/dm-hdfs-handle-mismatch";
        reset_dir(directory).await;
        tokio::fs::write(format!("{directory}/file.bin"), b"data")
            .await
            .assert_value("write shape file");
        let storage = create_storage(directory, CreateStorageOptions::default())
            .await
            .assert_value("create local storage");
        let entry = storage
            .get_metadata(Path::new("file.bin"))
            .await
            .assert_value("read shape metadata");
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        let handle = StreamHandle::Hdfs {
            part_path: PathBuf::from("file.bin.part"),
            prefix_len: 0,
            expected_size: 4,
        };
        assert!(
            StorageEnum::write_chunk_stream(
                &storage,
                &entry,
                receiver,
                &handle,
                None,
                Arc::new(|_, _| {})
            )
            .await
            .is_err()
        );
        assert!(
            StorageEnum::commit_chunk_stream(&storage, &entry, 4, handle)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn copy_file_preserves_local_mtime_and_mode() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = "/tmp/dm-copy-metadata-src";
        let dst_dir = "/tmp/dm-copy-metadata-dst";
        reset_dir(src_dir).await;
        reset_dir(dst_dir).await;

        let src_path = format!("{src_dir}/fixture.bin");
        tokio::fs::write(&src_path, b"metadata fixture")
            .await
            .assert_value("test value should be present");
        tokio::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o640))
            .await
            .assert_value("test value should be present");
        let expected_mtime = FileTime::from_unix_time(1_700_000_000, 123_456_789);
        filetime::set_file_mtime(&src_path, expected_mtime)
            .assert_value("test value should be present");

        let source = create_storage(src_dir, CreateStorageOptions::default())
            .await
            .assert_value("test value should be present");
        let destination = create_storage(dst_dir, CreateStorageOptions::new(None, true))
            .await
            .assert_value("test value should be present");
        let entry = Box::pin(source.get_metadata(Path::new("fixture.bin")))
            .await
            .assert_value("test value should be present");
        StorageEnum::copy_file(
            &source,
            &destination,
            &entry,
            CopyOptions {
                enable_integrity_check: true,
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await
        .assert_value("test value should be present");

        let copied = Box::pin(destination.get_metadata(Path::new("fixture.bin")))
            .await
            .assert_value("test value should be present");
        assert_eq!(copied.get_mtime(), entry.get_mtime());
        assert_eq!(copied.get_mode().map(|mode| mode & 0o7777), Some(0o640));
        assert_eq!(copied.get_uid(), entry.get_uid());
        assert_eq!(copied.get_gid(), entry.get_gid());
    }

    #[tokio::test]
    async fn s3_entry_metadata_sets_local_timestamp_without_posix_attrs() {
        use std::os::unix::fs::PermissionsExt;

        let dst_dir = "/tmp/dm-s3-mtime-dst";
        reset_dir(dst_dir).await;
        let dst_path = format!("{dst_dir}/fixture.bin");
        tokio::fs::write(&dst_path, b"fixture")
            .await
            .assert_value("test value should be present");
        tokio::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(0o640))
            .await
            .assert_value("test value should be present");

        let destination = create_storage(dst_dir, CreateStorageOptions::new(None, true))
            .await
            .assert_value("test value should be present");
        let expected_mtime = 1_700_000_000_123_000_000_i64;
        let entry = EntryEnum::S3(S3Entry {
            name: "fixture.bin".to_string(),
            relative_path: "fixture.bin".to_string(),
            extension: Some("bin".to_string()),
            size: 7,
            mtime: expected_mtime,
            tags: None,
            version_id: None,
            is_latest: true,
            is_delete_marker: false,
            version_count: None,
            is_dir: false,
        });

        destination
            .set_entry_metadata(&entry)
            .await
            .assert_value("test value should be present");
        let copied = destination
            .get_metadata(Path::new("fixture.bin"))
            .await
            .assert_value("test value should be present");
        assert_eq!(copied.get_mtime(), expected_mtime);
        assert_eq!(copied.get_mode().map(|mode| mode & 0o7777), Some(0o640));
    }

    /// hash mismatch（size 相同、内容不同）→ Err 且目标坏文件被清理（issue #58）。
    #[tokio::test]
    async fn verify_dest_integrity_hash_mismatch_cleans_dest() {
        let dir = "/tmp/dm-verify-hash-mismatch";
        reset_dir(dir).await;
        tokio::fs::write(format!("{dir}/good.bin"), vec![0xAAu8; 4096])
            .await
            .assert_value("test value should be present");
        tokio::fs::write(format!("{dir}/blob.bin"), vec![0xBBu8; 4096])
            .await
            .assert_value("test value should be present");

        let storage = create_storage(dir, CreateStorageOptions::default())
            .await
            .assert_value("test value should be present");
        let entry = Box::pin(storage.get_metadata(Path::new("blob.bin")))
            .await
            .assert_value("test value should be present");
        let src_hash = storage
            .compute_hash(Path::new("good.bin"), 4096)
            .await
            .assert_value("test value should be present");

        let err = StorageEnum::verify_dest_integrity(&storage, &entry, 4096, &src_hash)
            .await
            .assert_error("copy should report a destination integrity mismatch");
        assert!(
            err.to_string().contains("hashes differ"),
            "unexpected error: {err}"
        );
        assert!(
            tokio::fs::metadata(format!("{dir}/blob.bin"))
                .await
                .is_err(),
            "mismatched destination file should be cleaned up"
        );
    }

    /// 读回字节数不足（目标比声明 size 短）→ Err 且目标被清理（issue #58）。
    #[tokio::test]
    async fn verify_dest_integrity_short_readback_cleans_dest() {
        let dir = "/tmp/dm-verify-short-readback";
        reset_dir(dir).await;
        tokio::fs::write(format!("{dir}/blob.bin"), vec![0xCCu8; 3000])
            .await
            .assert_value("test value should be present");

        let storage = create_storage(dir, CreateStorageOptions::default())
            .await
            .assert_value("test value should be present");
        let entry = Box::pin(storage.get_metadata(Path::new("blob.bin")))
            .await
            .assert_value("test value should be present");

        // 期望 4096 字节但目标只有 3000：hash 读回顺带的字节数核对失败
        let err = StorageEnum::verify_dest_integrity(&storage, &entry, 4096, "irrelevant")
            .await
            .assert_error("copy should report a short destination read");
        assert!(
            err.to_string().contains("read-back returned 3000"),
            "unexpected error: {err}"
        );
        assert!(
            tokio::fs::metadata(format!("{dir}/blob.bin"))
                .await
                .is_err(),
            "short destination file should be cleaned up"
        );
    }

    /// size/hash 全匹配 → Ok 且目标文件保留。
    #[tokio::test]
    async fn verify_dest_integrity_match_keeps_dest() {
        let dir = "/tmp/dm-verify-match";
        reset_dir(dir).await;
        tokio::fs::write(format!("{dir}/blob.bin"), vec![0xDDu8; 4096])
            .await
            .assert_value("test value should be present");

        let storage = create_storage(dir, CreateStorageOptions::default())
            .await
            .assert_value("test value should be present");
        let entry = Box::pin(storage.get_metadata(Path::new("blob.bin")))
            .await
            .assert_value("test value should be present");
        let src_hash = storage
            .compute_hash(Path::new("blob.bin"), 4096)
            .await
            .assert_value("test value should be present");

        StorageEnum::verify_dest_integrity(&storage, &entry, 4096, &src_hash)
            .await
            .assert_value("test value should be present");
        assert!(
            tokio::fs::metadata(format!("{dir}/blob.bin")).await.is_ok(),
            "matching destination file must be kept"
        );
    }
}
