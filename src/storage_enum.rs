use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::checksum::HashCalculator;
use crate::cifs::CifsStorage;
use crate::error::StorageError;
use crate::filter::FilterExpression;
use crate::hdfs::HDFSStorage;
use crate::local::LocalStorage;
use crate::nfs::NFSStorage;
use crate::pipeline_primitives::WriteProgress;
use crate::qos::QosManager;
use crate::s3::S3Storage;
pub use crate::storage_options::{
    BackendConfig, CopyOptions, CreateStorageOptions, TarPackOptions, WalkOptions,
};
use crate::{
    CommitCallback, DataChunk, DeleteDirIterator, EntryEnum, Result, ResumeContext,
    TransferConcurrency, WalkDirAsyncIterator, WalkDirAsyncIterator2,
};
use tokio_util::sync::CancellationToken;

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
            return Box::pin(crate::storage_copy_pipeline::copy_s3_to_s3_native(
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
        crate::tar_pack::pack_files_to_tar(
            from, to, entries, tar_path, tar_size, tar_mtime, options,
        )
        .await
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
}

pub(crate) use crate::storage_factory::path_to_s3_key;
pub use crate::storage_factory::{create_storage, detect_storage_type};

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
