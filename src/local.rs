// 标准库
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt, lchown};
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::UNIX_EPOCH;

// 外部crate
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use rayon::prelude::*;
use tokio::fs::OpenOptions;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, trace};

use crate::checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
use crate::error::StorageError;
use crate::qos::QosManager;
use crate::storage_enum::StorageEnum;
use crate::time_util;
use crate::transfer_concurrency::{
    TransferBackend, TransferConcurrency, resolve_transfer_concurrency,
};
use crate::write_pipeline::{ChunkSink, CommitPolicy, write_pipeline_core};
use crate::{DataChunk, DeleteDirIterator, DeleteEvent, EntryEnum, MB, NASEntry, Result};

type LocalReadFuture<'a> = Pin<Box<dyn Future<Output = (u64, Result<Bytes>)> + Send + 'a>>;

impl NASEntry {
    /// 从本地文件系统 `Metadata` 构建 `NASEntry`。
    /// Unix 与 Windows 的差异封装在此函数内部。
    pub(crate) fn from_local_metadata(
        name: String,
        relative_path: PathBuf,
        extension: Option<String>,
        metadata: &std::fs::Metadata,
        is_symlink: bool,
    ) -> Self {
        #[cfg(unix)]
        let mode = metadata.permissions().mode();
        #[cfg(windows)]
        let mode = if metadata.is_dir() {
            0o755
        } else if metadata.permissions().readonly() {
            0o444
        } else {
            0o644
        };

        #[cfg(unix)]
        let (hard_links, uid, gid, ino) = (
            Some(u32::try_from(metadata.nlink()).unwrap_or(u32::MAX)),
            Some(metadata.uid()),
            Some(metadata.gid()),
            Some(metadata.ino()),
        );
        #[cfg(windows)]
        let (hard_links, uid, gid, ino) = (None, None, None, None);

        Self {
            name,
            relative_path,
            extension,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
            atime: time_util::system_time_to_nanos(metadata.accessed().unwrap_or(UNIX_EPOCH)),
            ctime: time_util::system_time_to_nanos(metadata.created().unwrap_or(UNIX_EPOCH)),
            mtime: time_util::system_time_to_nanos(metadata.modified().unwrap_or(UNIX_EPOCH)),
            mode,
            is_symlink,
            hard_links,
            file_handle: None,
            uid,
            gid,
            ino,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        }
    }
}

/// 本地文件句柄包装
#[derive(Debug)]
pub(crate) struct LocalFileHandle {
    // `FileExt::{read_at, write_at}` operate on `&File`, so clones of this
    // handle can safely issue independent positional I/O concurrently.
    inner: Arc<std::fs::File>,
}

impl LocalFileHandle {
    async fn new(file: tokio::fs::File) -> Self {
        Self {
            inner: Arc::new(file.into_std().await),
        }
    }

    async fn commit(&self) -> Result<()> {
        let file = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || file.sync_all())
            .await?
            .map_err(StorageError::IoError)
    }
}

/// Local 的 `ChunkSink`：通过位置写实现 `write_at`，不共享文件游标，因此多个
/// chunk 可以安全地并发写入同一个文件。
pub(crate) struct LocalChunkSink {
    storage: LocalStorage,
    file: LocalFileHandle,
}

impl LocalChunkSink {
    fn new(storage: LocalStorage, file: LocalFileHandle) -> Self {
        Self { storage, file }
    }
}

#[async_trait]
impl ChunkSink for LocalChunkSink {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<u64> {
        let len = data.len() as u64;
        let written = self.storage.write(&self.file, offset, data).await? as u64;
        if written < len {
            return Err(StorageError::OperationError(format!(
                "Short write at offset {offset}: {written} of {len} bytes"
            )));
        }
        Ok(written)
    }

    async fn flush(&self) -> Result<()> {
        self.file.commit().await
    }
}

const DEFAULT_BLOCK_SIZE: u64 = 2 * MB;
const DEFAULT_TRANSFER_CONCURRENCY: TransferConcurrency = TransferConcurrency::defaults(4, 8);

#[derive(Clone, Debug)]
pub(crate) struct StorageConfig {
    /// 块大小，默认2MB
    pub block_size: u64,
    pub transfer_concurrency: TransferConcurrency,
}

/// 本地存储实现
#[derive(Clone, Debug)]
pub struct LocalStorage {
    pub root_path: Arc<PathBuf>,
    pub(crate) config: StorageConfig,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>, block_size: Option<u64>) -> Self {
        Self {
            root_path: Arc::new(root.into()),
            config: StorageConfig {
                block_size: block_size.map_or(DEFAULT_BLOCK_SIZE, |size| {
                    std::cmp::min(size, DEFAULT_BLOCK_SIZE)
                }),
                transfer_concurrency: DEFAULT_TRANSFER_CONCURRENCY,
            },
        }
    }

    /// Overrides the per-file read and write concurrency for this adapter.
    #[must_use]
    pub fn with_transfer_concurrency(mut self, concurrency: TransferConcurrency) -> Self {
        self.config.transfer_concurrency = concurrency;
        self
    }
}

impl LocalStorage {
    #[inline]
    fn get_full_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }

    pub(crate) async fn open(&self, relative_path: &Path) -> Result<LocalFileHandle> {
        let inner = tokio::fs::File::open(self.get_full_path(relative_path)).await?;
        Ok(LocalFileHandle::new(inner).await)
    }

    /// 创建/打开目标文件。
    ///
    /// `truncate`：是否将已存在文件截断为 0 字节。覆盖写场景（`write_file`/
    /// `write_data`）必须传 `true`，否则新内容比旧文件短时会残留旧文件的尾部
    /// 字节（数据损坏）。断点续传写 `.part`（`write_data_resumable`）必须传
    /// `false`，否则会截掉已写入的续传进度。
    async fn create_file(
        &self,
        relative_path: &Path,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
        truncate: bool,
    ) -> Result<LocalFileHandle> {
        let full_path = self.get_full_path(relative_path);

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut options: OpenOptions = OpenOptions::new();
        options
            .create(true)
            .write(true)
            .read(true)
            .truncate(truncate);

        let file = options.open(&full_path).await?;

        self.set_metadata(relative_path, None, None, uid, gid, mode)
            .await?;

        Ok(LocalFileHandle::new(file).await)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn delete_file(&self, relative_path: &Path) -> Result<()> {
        let full_path = self.get_full_path(relative_path);
        tokio::fs::remove_file(&full_path)
            .await
            .map_err(StorageError::IoError)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn create_symlink(
        &self,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        relative_path: &Path,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        target: &Path,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        atime: i64,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        mtime: i64,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        uid: Option<u32>,
        #[cfg_attr(
            windows,
            expect(
                unused_variables,
                reason = "symlink creation is unsupported on Windows"
            )
        )]
        gid: Option<u32>,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            let full_path = self.get_full_path(relative_path);

            // 安全校验：拒绝指向绝对路径或包含 ".." 的符号链接目标，防止路径穿越
            if target.is_absolute()
                || target
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
            {
                return Err(StorageError::OperationError(format!(
                    "Unsafe symlink target rejected: {} (absolute paths and '..' are not allowed)",
                    target.display()
                )));
            }

            tokio::fs::symlink(target, &full_path).await?;

            // 设置文件所有者和组
            if let (Some(uid), Some(gid)) = (uid, gid) {
                lchown(&full_path, Some(uid), Some(gid))?;
            }

            // 将纳秒时间戳转换为FileTime
            let atime = time_util::nanos_to_filetime_local(atime);
            let mtime = time_util::nanos_to_filetime_local(mtime);

            match tokio::task::spawn_blocking(move || {
                filetime::set_symlink_file_times(&full_path, atime, mtime)
            })
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(StorageError::from(err)),
                Err(err) => Err(StorageError::from(std::io::Error::other(format!(
                    "Task spawn failed: {err:?}"
                )))),
            }
        }

        #[cfg(not(unix))]
        {
            Ok(())
        }
    }

    /// 读取符号链接的目标路径
    /// 如果符号链接的目标是self.root的子目录，则返回相对于self.root的路径
    /// 否则返回错误
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn read_symlink(&self, relative_path: &Path) -> Result<PathBuf> {
        let full_path = self.get_full_path(relative_path);

        tokio::fs::read_link(&full_path)
            .await
            .map_err(StorageError::IoError)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn create_dir_all(&self, relative_path: &Path) -> Result<()> {
        let full_path = self.get_full_path(relative_path);

        tokio::fs::create_dir_all(&full_path)
            .await
            .map_err(StorageError::IoError)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn delete_dir_all(&self, relative_path: Option<&Path>) -> Result<()> {
        let iter = self.delete_dir_all_with_progress(relative_path, 4)?;
        while iter.next().await.is_some() {}
        Ok(())
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub fn delete_dir_all_with_progress(
        &self,
        relative_path: Option<&Path>,
        concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let full_path = match relative_path {
            Some(p) => self.get_full_path(p),
            None => (*self.root_path).clone(),
        };
        let root_path = full_path.clone();
        let (tx, rx) = async_channel::bounded::<DeleteEvent>(1000);
        let concurrency = concurrency.clamp(1, 64);

        tokio::task::spawn_blocking(move || {
            let pool = match rayon::ThreadPoolBuilder::new()
                .num_threads(concurrency)
                .build()
            {
                Ok(pool) => pool,
                Err(e) => {
                    error!("Failed to build rayon thread pool: {}", e);
                    return;
                }
            };
            pool.install(|| {
                delete_recursive(&full_path, &root_path, &tx);
            });
            // tx drop → channel 关闭
        });

        Ok(DeleteDirIterator::new(rx))
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from_full_path = self.get_full_path(from);
        let to_full_path = self.get_full_path(to);
        tokio::fs::rename(&from_full_path, &to_full_path)
            .await
            .map_err(StorageError::IoError)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        let path = relative_path.to_path_buf();
        let full_path = self.get_full_path(relative_path);
        let metadata = tokio::fs::symlink_metadata(&full_path)
            .await
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    StorageError::FileNotFound(relative_path.display().to_string())
                } else {
                    StorageError::IoError(error)
                }
            })?;
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let is_symlink = metadata.is_symlink();
        Ok(EntryEnum::NAS(NASEntry::from_local_metadata(
            name, path, None, &metadata, is_symlink,
        )))
    }

    /// 更新文件元数据: 包括修改时间、访问时间、所有者UID、组ID和权限模式.
    /// 该函数会同步更新文件和目录的元数据（不包含软链接）.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_metadata(
        &self,
        relative_path: &Path,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        let full_path = self.get_full_path(relative_path);

        trace!(
            "Setting mtime for {:?} to {:?}, atime to {:?}, uid to {:?}, gid to {:?}, mode to {:?}",
            full_path, mtime, atime, uid, gid, mode
        );

        let mut tasks = Vec::new();

        // 处理时间戳更新；未提供的一侧保留目标文件当前值。
        if atime.is_some() || mtime.is_some() {
            let path_clone = full_path.clone();
            tasks.push(tokio::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    let metadata = std::fs::metadata(&path_clone)?;
                    let atime = atime.map_or_else(
                        || filetime::FileTime::from_last_access_time(&metadata),
                        time_util::nanos_to_filetime_local,
                    );
                    let mtime = mtime.map_or_else(
                        || filetime::FileTime::from_last_modification_time(&metadata),
                        time_util::nanos_to_filetime_local,
                    );
                    filetime::set_file_times(&path_clone, atime, mtime)
                })
                .await
                .map_err(|err| {
                    StorageError::from(std::io::Error::other(format!("Task spawn failed: {err:?}")))
                })?
                .map_err(StorageError::from)
            }));
        }

        // 在Unix系统上设置权限和所有权
        #[cfg(unix)]
        {
            // 处理所有者和组
            if let (Some(uid), Some(gid)) = (uid, gid) {
                let path_clone = full_path.clone();
                tasks.push(tokio::spawn(async move {
                    tokio::task::spawn_blocking(move || lchown(&path_clone, Some(uid), Some(gid)))
                        .await
                        .map_err(|err| {
                            StorageError::from(std::io::Error::other(format!(
                                "Task spawn failed: {err:?}"
                            )))
                        })?
                        .map_err(StorageError::from)
                }));
            }

            // 处理权限模式
            if let Some(mode) = mode {
                let path_clone = full_path.clone();
                tasks.push(tokio::spawn(async move {
                    tokio::fs::set_permissions(&path_clone, std::fs::Permissions::from_mode(mode))
                        .await
                        .map_err(StorageError::from)
                }));
            }
        }

        // 等待所有任务完成
        for task in tasks {
            task.await??;
        }

        Ok(())
    }

    async fn read(&self, file: &LocalFileHandle, offset: u64, count: u64) -> Result<Bytes> {
        let capacity = usize::try_from(count).map_err(|_| {
            StorageError::OperationError(format!("read size {count} exceeds platform capacity"))
        })?;
        let file = Arc::clone(&file.inner);
        let buffer = tokio::task::spawn_blocking(move || {
            let mut buffer = vec![0_u8; capacity];
            let mut filled = 0usize;
            while filled < capacity {
                let position = offset.checked_add(filled as u64).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "read offset overflow")
                })?;
                #[cfg(unix)]
                let read = file.read_at(&mut buffer[filled..], position)?;
                #[cfg(windows)]
                let read = file.seek_read(&mut buffer[filled..], position)?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            buffer.truncate(filled);
            Ok::<_, std::io::Error>(buffer)
        })
        .await??;

        trace!(
            "read {} bytes from file in local storage using positional I/O",
            buffer.len()
        );
        Ok(Bytes::from(buffer))
    }

    /// 向文件句柄写入数据
    async fn write(&self, file: &LocalFileHandle, offset: u64, data: Bytes) -> Result<usize> {
        trace!(
            "write file in local storage: offset {}, data len {}",
            offset,
            data.len()
        );
        let length = data.len();
        let file = Arc::clone(&file.inner);
        tokio::task::spawn_blocking(move || {
            let mut written = 0usize;
            while written < data.len() {
                let position = offset.checked_add(written as u64).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "write offset overflow")
                })?;
                #[cfg(unix)]
                let n = file.write_at(&data[written..], position)?;
                #[cfg(windows)]
                let n = file.seek_write(&data[written..], position)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the complete local file chunk",
                    ));
                }
                written += n;
            }
            Ok::<_, std::io::Error>(written)
        })
        .await??;

        trace!("Wrote {} bytes at offset {}", length, offset);

        Ok(length)
    }

    pub(crate) async fn read_file(&self, path: &Path, size: u64) -> Result<Bytes> {
        let handle = self.open(path).await?;
        self.read(&handle, 0, size).await
    }

    pub(crate) async fn write_file(
        &self,
        path: &Path,
        data: Bytes,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        let handle = self.create_file(path, uid, gid, mode, true).await?;
        self.write(&handle, 0, data).await?;
        handle.commit().await
    }

    /// 处理单个文件或目录的复制
    /// 根据文件大小计算合适的块大小并记录大文件日志
    #[inline]
    fn calculate_chunk_size(&self, file_size: u64) -> u64 {
        // 根据文件大小动态调整块大小，优化内存使用
        // chunk size最小为一个字节，最大为2MB
        std::cmp::min(file_size, self.config.block_size).max(1)
    }

    pub(crate) async fn read_data(
        &self,
        tx: Sender<DataChunk>,
        relative_path: &Path,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        // 如果文件大小为0，直接返回
        if size == 0 {
            debug!("File {:?} is empty, skipping read", relative_path);
            return Ok(None);
        }

        let chunk_size = self.calculate_chunk_size(size);
        trace!(
            "Starting read data for file {:?}, size: {}, chunk_size: {}",
            relative_path, size, chunk_size
        );

        // 打开一次文件，避免重复打开
        let source_file = match self.open(relative_path).await {
            Ok(file) => {
                debug!("Successfully opened source file: {:?}", relative_path);
                file
            }
            Err(e) => {
                error!("Failed to open source file {:?}: {:?}", relative_path, e);
                return Err(StorageError::OperationError(format!(
                    "Failed to open source file {}: {e:?}",
                    relative_path.display()
                )));
            }
        };

        let mut issue_offset = 0u64;
        let mut bytes_read: u64 = 0;
        let mut hasher = create_hash_calculator(enable_integrity_check);
        let mut inflight: FuturesOrdered<LocalReadFuture<'_>> = FuturesOrdered::new();

        loop {
            self.fill_read_pipeline(
                &mut inflight,
                &source_file,
                &mut issue_offset,
                size,
                chunk_size,
                qos.as_ref(),
            )
            .await;

            let Some((offset, result)) = inflight.next().await else {
                break;
            };
            let data = result.map_err(|error| {
                error!("Failed to read data chunk at offset {offset}: {error:?}");
                error
            })?;

            let data_length = data.len() as u64;
            if data.is_empty() {
                debug!("Reached end of file {:?}", relative_path);
                break;
            }

            // 如果启用了校验和检查，更新源文件哈希值
            if let Some(ref mut hasher) = hasher {
                hasher.update(&data);
                trace!(
                    "Updated hash calculation for file {:?}, offset: {}",
                    relative_path, offset
                );
            }
            // 发送数据块到通道
            if tx.send(DataChunk { offset, data }).await.is_err() {
                break;
            }

            bytes_read += data_length;
            trace!(
                "Read {} bytes from file {:?}, progress: {}/{} bytes",
                data_length,
                relative_path,
                bytes_read.min(size),
                size
            );
        }

        trace!(
            "Finished read_data_task for file {:?}, total bytes processed: {}",
            relative_path, bytes_read
        );

        Ok(hasher)
    }

    async fn fill_read_pipeline<'a>(
        &'a self,
        inflight: &mut FuturesOrdered<LocalReadFuture<'a>>,
        file: &'a LocalFileHandle,
        issue_offset: &mut u64,
        end: u64,
        chunk_size: u64,
        qos: Option<&QosManager>,
    ) {
        while inflight.len() < self.config.transfer_concurrency.read() && *issue_offset < end {
            let requested = chunk_size.min(end - *issue_offset);
            let want = if let Some(qos) = qos {
                let granted = qos.acquire_bandwidth_grant(requested).await;
                qos.acquire_iops().await;
                granted
            } else {
                requested
            };
            let offset = *issue_offset;
            inflight.push_back(Box::pin(async move {
                let result = self.read(file, offset, want).await;
                (offset, result)
            }));
            *issue_offset += want;
        }
    }

    /// 返回实际写入的累计字节数（写端本地计数，issue #58）。
    pub(crate) async fn write_data(
        &self,
        rx: tokio::sync::mpsc::Receiver<DataChunk>,
        relative_path: &Path,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64> {
        trace!("Starting write_data_task for file {:?}", relative_path);

        // 注意：这里需要重新创建目标文件，因为我们不能在线程间共享文件句柄
        let dest_file = self
            .create_file(relative_path, uid, gid, mode, true)
            .await?;
        debug!(
            "Created destination file {:?} with mode: {:?}",
            relative_path,
            mode.map(|m| format!("{m:o}"))
        );

        let sink = LocalChunkSink::new(self.clone(), dest_file);
        let written = write_pipeline_core(
            rx,
            &sink,
            None,
            self.config.transfer_concurrency.write(),
            CommitPolicy::None,
            bytes_counter,
        )
        .await?;

        trace!("Finished write_data_task for file {:?}", relative_path);
        Ok(written)
    }

    // ========================================================
    // 字节级断点续传变体（仅多块大文件）
    // ========================================================

    /// 只读取给定缺失区间的数据，按 `chunk.offset` 发送 `DataChunk`。
    ///
    /// 与 `read_data` 的区别：不从 0 顺序读整文件，只读 `intervals` 覆盖的部分，
    /// 续传时避免重读已完成的数据。
    pub(crate) async fn read_data_intervals(
        &self,
        tx: Sender<DataChunk>,
        relative_path: &Path,
        intervals: &[(u64, u64)],
        qos: Option<QosManager>,
    ) -> Result<()> {
        let chunk_size = self.config.block_size.max(1);
        let source_file = self.open(relative_path).await?;
        for &(start, end) in intervals {
            let mut issue_offset = start;
            let mut inflight: FuturesOrdered<LocalReadFuture<'_>> = FuturesOrdered::new();
            loop {
                self.fill_read_pipeline(
                    &mut inflight,
                    &source_file,
                    &mut issue_offset,
                    end,
                    chunk_size,
                    qos.as_ref(),
                )
                .await;
                let Some((offset, result)) = inflight.next().await else {
                    break;
                };
                let data = result?;
                if data.is_empty() {
                    break;
                }
                if tx.send(DataChunk { offset, data }).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// 续写到 `.part` 文件：打开已存在文件**不截断**，按 `chunk.offset` 随机写。
    ///
    /// 正确性：每 `SYNC_BARRIER` 个 chunk 做一次 `sync_all` 落盘屏障，
    /// **屏障之后才**对这批 chunk 触发 `on_committed` —— 保证回调上报的区间
    /// 已确认落盘，进度记录永不超前于真实数据（崩溃时最坏丢一批、幂等重传）。
    pub(crate) async fn write_data_resumable(
        &self,
        rx: tokio::sync::mpsc::Receiver<DataChunk>,
        part_path: &Path,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
        progress: crate::pipeline_primitives::WriteProgress,
    ) -> Result<()> {
        const SYNC_BARRIER: usize = 16;
        let crate::pipeline_primitives::WriteProgress {
            bytes_counter,
            on_committed,
        } = progress;
        // truncate=false：保留 .part 中已写字节（续传基础）
        let dest_file = self.create_file(part_path, uid, gid, mode, false).await?;
        let sink = LocalChunkSink::new(self.clone(), dest_file);
        write_pipeline_core(
            rx,
            &sink,
            None,
            self.config.transfer_concurrency.write(),
            CommitPolicy::Barrier {
                every: SYNC_BARRIER,
                cb: on_committed,
            },
            bytes_counter,
        )
        .await
        .map(|_| ())
    }

    /// 将文件长度规整为 `len`（截掉续传遗留的尾部多余字节），并落盘。
    pub(crate) async fn set_file_len(&self, relative_path: &Path, len: u64) -> Result<()> {
        let full_path = self.get_full_path(relative_path);
        let file = OpenOptions::new().write(true).open(&full_path).await?;
        file.set_len(len).await.map_err(StorageError::IoError)?;
        file.sync_all().await.map_err(StorageError::IoError)
    }
}

/// 将用户输入路径规范化：canonicalize + Windows 长路径前缀处理
/// 注意：canonicalize 要求路径已存在
fn normalize_local_path(path: &str) -> Result<String> {
    let canonical_path = std::fs::canonicalize(path).map_err(|e| {
        StorageError::InvalidPath(format!("Failed to canonicalize path '{path}': {e}"))
    })?;

    #[cfg(windows)]
    {
        let path_str = canonical_path.to_string_lossy().into_owned();
        let processed = if path_str.starts_with(r"\\?\UNC\") {
            // \\?\UNC\server\share -> \\server\share
            path_str.replacen(r"\\?\UNC\", r"\\", 1)
        } else if path_str.starts_with(r"\\?\") {
            // \\?\C:\path -> C:\path
            path_str.replacen(r"\\?\", "", 1)
        } else {
            path_str
        };
        Ok(processed)
    }

    #[cfg(not(windows))]
    {
        Ok(canonical_path.to_string_lossy().into_owned())
    }
}

/// 创建本地存储实例
///
/// `ensure_dir = true`（目标端）时目录不存在则自动递归创建。
///
/// # Errors
///
/// Returns an error when the requested storage operation cannot be completed.
pub fn create_local_storage(
    path: &str,
    block_size: Option<u64>,
    ensure_dir: bool,
) -> Result<StorageEnum> {
    if ensure_dir {
        // create_dir_all 是幂等操作：目录已存在时不报错，不存在时递归创建
        std::fs::create_dir_all(path).map_err(StorageError::IoError)?;
    }
    debug!("In create local storage Raw path: {}", path);

    // canonicalize 要求路径已存在，路径不存在时会返回 InvalidPath 错误
    let local_path = normalize_local_path(path)?;
    debug!("In create local storage Normalized path: {}", local_path);

    let concurrency =
        resolve_transfer_concurrency(TransferBackend::Local, DEFAULT_TRANSFER_CONCURRENCY, None)?;
    let local_storage =
        LocalStorage::new(&local_path, block_size).with_transfer_concurrency(concurrency);
    Ok(StorageEnum::Local(local_storage))
}

/// Rayon 并行递归删除：后序遍历，先删文件再删目录
fn delete_recursive(path: &Path, root: &Path, tx: &async_channel::Sender<DeleteEvent>) {
    let entries: Vec<_> = match std::fs::read_dir(path) {
        Ok(rd) => rd.filter_map(std::result::Result::ok).collect(),
        Err(e) => {
            error!("Failed to read dir {:?}: {}", path, e);
            return;
        }
    };

    entries.par_iter().for_each(|entry| {
        let entry_path = entry.path();
        match entry.file_type() {
            Ok(ft) if ft.is_dir() => {
                delete_recursive(&entry_path, root, tx);
            }
            Ok(_) => {
                if let Err(e) = std::fs::remove_file(&entry_path) {
                    error!("Failed to delete file {:?}: {}", entry_path, e);
                } else {
                    let rel = entry_path.strip_prefix(root).unwrap_or(&entry_path);
                    let _ = tx.send_blocking(DeleteEvent {
                        relative_path: rel.to_path_buf(),
                        is_dir: false,
                        error: None,
                    });
                }
            }
            Err(e) => error!("Failed to get file type {:?}: {}", entry_path, e),
        }
    });

    // 递归返回 = 所有子文件/子目录已删除 → 安全删除当前目录
    if let Err(e) = std::fs::remove_dir(path) {
        error!("Failed to remove dir {:?}: {}", path, e);
    } else if let Ok(rel) = path.strip_prefix(root) {
        let _ = tx.send_blocking(DeleteEvent {
            relative_path: rel.to_path_buf(),
            is_dir: true,
            error: None,
        });
    }
}
