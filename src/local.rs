// 标准库
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt, lchown};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// 外部crate
use bytes::{Bytes, BytesMut};
use filetime::FileTime;
use rayon::prelude::*;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace};

use crate::checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
use crate::error::StorageError;
use crate::filter::{FilterExpression, dir_matches_date_filter, should_skip};
use crate::qos::QosManager;
use crate::storage_enum::StorageEnum;
use crate::walk_scheduler::{create_worker_contexts, run_worker_loop};
use crate::{
    DataChunk, DeleteDirIterator, DeleteEvent, EntryEnum, ErrorEvent, MB, NASEntry, Result, StorageEntryMessage,
    WalkDirAsyncIterator,
};

/// 从文件系统元数据构建 `NASEntry`
fn build_nas_entry(
    name: String, relative_path: PathBuf, extension: Option<String>, metadata: &std::fs::Metadata, is_symlink: bool,
) -> NASEntry {
    let mode = {
        #[cfg(unix)]
        {
            metadata.permissions().mode()
        }
        #[cfg(windows)]
        {
            if metadata.is_dir() {
                0o755
            } else if metadata.permissions().readonly() {
                0o444
            } else {
                0o644
            }
        }
    };
    NASEntry {
        name,
        relative_path,
        extension,
        is_dir: metadata.is_dir(),
        size: metadata.len(),
        atime: system_time_to_i64(metadata.accessed().unwrap_or(UNIX_EPOCH)),
        ctime: system_time_to_i64(metadata.created().unwrap_or(UNIX_EPOCH)),
        mtime: system_time_to_i64(metadata.modified().unwrap_or(UNIX_EPOCH)),
        mode,
        is_symlink,
        hard_links: {
            #[cfg(unix)]
            {
                Some(metadata.nlink() as u32)
            }
            #[cfg(windows)]
            {
                None
            }
        },
        file_handle: None,
        uid: {
            #[cfg(unix)]
            {
                Some(metadata.uid())
            }
            #[cfg(windows)]
            {
                None
            }
        },
        gid: {
            #[cfg(unix)]
            {
                Some(metadata.gid())
            }
            #[cfg(windows)]
            {
                None
            }
        },
        ino: {
            #[cfg(unix)]
            {
                Some(metadata.ino())
            }
            #[cfg(windows)]
            {
                None
            }
        },
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    }
}

/// 将 `SystemTime` 转换为纳秒时间戳
fn system_time_to_i64(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// 将纳秒时间戳转换为 `FileTime`
fn i64_to_file_time(timestamp: i64) -> FileTime {
    let seconds = timestamp / 1_000_000_000;
    let nanos = (timestamp % 1_000_000_000) as u32;
    FileTime::from_unix_time(seconds, nanos)
}

/// 本地文件句柄包装
#[derive(Debug)]
pub(crate) struct LocalFileHandle {
    inner: tokio::fs::File,
}

impl LocalFileHandle {
    fn new(file: tokio::fs::File) -> Self {
        Self { inner: file }
    }

    async fn commit(&self) -> Result<()> {
        self.inner.sync_all().await.map_err(StorageError::IoError)
    }
}

const DEFAULT_BLOCK_SIZE: u64 = 2 * MB;

#[derive(Clone, Debug)]
pub(crate) struct StorageConfig {
    /// 块大小，默认2MB
    pub block_size: u64,
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
                block_size: block_size.map_or(DEFAULT_BLOCK_SIZE, |size| std::cmp::min(size, DEFAULT_BLOCK_SIZE)),
            },
        }
    }
}

impl LocalStorage {
    #[inline]
    fn get_full_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }

    pub(crate) async fn open(&self, relative_path: &Path) -> Result<LocalFileHandle> {
        let inner = tokio::fs::File::open(self.get_full_path(relative_path)).await?;
        Ok(LocalFileHandle { inner })
    }

    async fn create_file(
        &self, relative_path: &Path, #[allow(unused)] uid: Option<u32>, #[allow(unused)] gid: Option<u32>,
        #[allow(unused)] mode: Option<u32>,
    ) -> Result<LocalFileHandle> {
        let full_path = self.get_full_path(relative_path);

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut options: OpenOptions = OpenOptions::new();
        options.create(true).write(true).read(true);

        let file = options.open(&full_path).await?;

        self.set_metadata(relative_path, None, None, uid, gid, mode).await?;

        Ok(LocalFileHandle::new(file))
    }

    pub async fn delete_file(&self, relative_path: &Path) -> Result<()> {
        let full_path = self.get_full_path(relative_path);
        tokio::fs::remove_file(&full_path).await.map_err(StorageError::IoError)
    }

    pub async fn create_symlink(
        &self, #[allow(unused)] relative_path: &Path, #[allow(unused)] target: &Path, #[allow(unused)] atime: i64,
        #[allow(unused)] mtime: i64, #[allow(unused)] uid: Option<u32>, #[allow(unused)] gid: Option<u32>,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            let full_path = self.get_full_path(relative_path);

            // 安全校验：拒绝指向绝对路径或包含 ".." 的符号链接目标，防止路径穿越
            if target.is_absolute() || target.components().any(|c| c == std::path::Component::ParentDir) {
                return Err(StorageError::OperationError(format!(
                    "Unsafe symlink target rejected: {:?} (absolute paths and '..' are not allowed)",
                    target
                )));
            }

            tokio::fs::symlink(target, &full_path).await?;

            // 设置文件所有者和组
            if let (Some(uid), Some(gid)) = (uid, gid) {
                lchown(&full_path, Some(uid), Some(gid))?;
            }

            // 将纳秒时间戳转换为FileTime
            let atime = i64_to_file_time(atime);
            let mtime = i64_to_file_time(mtime);

            match tokio::task::spawn_blocking(move || filetime::set_symlink_file_times(&full_path, atime, mtime)).await
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
    pub async fn read_symlink(&self, relative_path: &Path) -> Result<PathBuf> {
        let full_path = self.get_full_path(relative_path);

        tokio::fs::read_link(&full_path).await.map_err(StorageError::IoError)
    }

    pub async fn create_dir_all(&self, relative_path: &Path) -> Result<()> {
        let full_path = self.get_full_path(relative_path);

        tokio::fs::create_dir_all(&full_path)
            .await
            .map_err(StorageError::IoError)
    }

    pub async fn delete_dir_all(&self, relative_path: Option<&Path>) -> Result<()> {
        let iter = self.delete_dir_all_with_progress(relative_path, 4)?;
        while iter.next().await.is_some() {}
        Ok(())
    }

    pub fn delete_dir_all_with_progress(
        &self, relative_path: Option<&Path>, concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let full_path = match relative_path {
            Some(p) => self.get_full_path(p),
            None => (*self.root_path).clone(),
        };
        let root_path = full_path.clone();
        let (tx, rx) = async_channel::bounded::<DeleteEvent>(1000);
        let concurrency = concurrency.clamp(1, 64);

        tokio::task::spawn_blocking(move || {
            let pool = match rayon::ThreadPoolBuilder::new().num_threads(concurrency).build() {
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

    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from_full_path = self.get_full_path(from);
        let to_full_path = self.get_full_path(to);
        tokio::fs::rename(&from_full_path, &to_full_path)
            .await
            .map_err(StorageError::IoError)
    }

    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        let path = relative_path.to_path_buf();
        let full_path = self.get_full_path(relative_path);
        let metadata = tokio::fs::symlink_metadata(&full_path).await?;
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        let is_symlink = metadata.is_symlink();
        Ok(EntryEnum::NAS(build_nas_entry(name, path, None, &metadata, is_symlink)))
    }

    /// 更新文件元数据: 包括修改时间、访问时间、所有者UID、组ID和权限模式.
    /// 该函数会同步更新文件和目录的元数据（不包含软链接）.
    pub async fn set_metadata(
        &self, relative_path: &Path, atime: Option<i64>, mtime: Option<i64>, #[allow(unused)] uid: Option<u32>,
        #[allow(unused)] gid: Option<u32>, #[allow(unused)] mode: Option<u32>,
    ) -> Result<()> {
        let full_path = self.get_full_path(relative_path);

        trace!(
            "Setting mtime for {:?} to {:?}, atime to {:?}, uid to {:?}, gid to {:?}, mode to {:?}",
            full_path, mtime, atime, uid, gid, mode
        );

        let mut tasks = Vec::new();

        // 处理时间戳更新
        if let (Some(atime), Some(mtime)) = (atime, mtime) {
            let path_clone = full_path.clone();
            tasks.push(tokio::spawn(async move {
                let atime = i64_to_file_time(atime);
                let mtime = i64_to_file_time(mtime);

                tokio::task::spawn_blocking(move || filetime::set_file_times(&path_clone, atime, mtime))
                    .await
                    .map_err(|err| StorageError::from(std::io::Error::other(format!("Task spawn failed: {err:?}"))))?
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
                            StorageError::from(std::io::Error::other(format!("Task spawn failed: {err:?}")))
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

    #[allow(clippy::too_many_arguments, clippy::unused_async)]
    pub async fn walkdir(
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, packaged: bool, package_depth: usize,
    ) -> Result<WalkDirAsyncIterator> {
        let (tx, rx) = async_channel::bounded(1000); // 缓冲区大小1000

        // 使用子目录或根目录作为遍历路径
        let root_path = match sub_path {
            Some(p) => self.get_full_path(p),
            None => (*self.root_path).clone(),
        };

        // 设置最大深度，0表示无限深度
        let max_depth = depth.unwrap_or(0);

        // 全局文件计数器
        let total_file_count = Arc::new(AtomicUsize::new(0));

        // 调用iterative_walkdir函数执行实际遍历
        let self_clone = self.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = self_clone
                .iterative_walkdir(
                    &root_path,
                    tx_clone.clone(),
                    max_depth,
                    &match_expressions,
                    &exclude_expressions,
                    concurrency,
                    total_file_count,
                    packaged,
                    package_depth,
                )
                .await
            {
                error!("[Walkdir] Iterative walkdir failed: {:?}", e);
                let _ = tx_clone
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: std::path::PathBuf::from(format!("{e:?}")),
                        reason: format!("{e:?}"),
                    })
                    .await;
            }
        });

        Ok(WalkDirAsyncIterator::new(rx))
    }

    /// 迭代式目录遍历函数，使用工作窃取队列实现高效并发
    #[allow(clippy::too_many_arguments, clippy::ref_option)]
    async fn iterative_walkdir(
        &self, root_path: &Path, tx: async_channel::Sender<StorageEntryMessage>, max_depth: usize,
        match_expressions: &Option<FilterExpression>, exclude_expressions: &Option<FilterExpression>,
        concurrency: usize, total_file_count: Arc<AtomicUsize>, packaged: bool, package_depth: usize,
    ) -> Result<()> {
        let contexts =
            create_worker_contexts(concurrency, (root_path.to_path_buf(), 0usize, true, None::<usize>)).await;
        let match_expr = Arc::new(match_expressions.clone());
        let exclude_expr = Arc::new(exclude_expressions.clone());

        info!("Creating {} producer tasks", contexts.len());

        let mut handles = Vec::with_capacity(contexts.len());
        for ctx in contexts {
            let self_clone = self.clone();
            let tx_clone = tx.clone();
            let match_expr_clone = Arc::clone(&match_expr);
            let exclude_expr_clone = Arc::clone(&exclude_expr);
            let total_file_count_clone = Arc::clone(&total_file_count);

            handles.push(tokio::spawn(async move {
                run_worker_loop(
                    &ctx,
                    |(dir_path, current_depth, skip_filter, package_remaining)| {
                        self_clone.process_dir(
                            ctx.worker_id,
                            dir_path,
                            current_depth,
                            &tx_clone,
                            &ctx,
                            &match_expr_clone,
                            &exclude_expr_clone,
                            max_depth,
                            &total_file_count_clone,
                            skip_filter,
                            packaged,
                            package_depth,
                            package_remaining,
                        )
                    },
                    |task| format!("{}", task.0.display()),
                )
                .await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    /// 处理单个目录，读取条目并过滤，发送符合条件的 `StorageEntry`
    #[allow(clippy::too_many_arguments)]
    async fn process_dir(
        &self, producer_id: usize, dir_path: PathBuf, current_depth: usize,
        tx: &async_channel::Sender<StorageEntryMessage>,
        ctx: &crate::walk_scheduler::WorkerContext<(PathBuf, usize, bool, Option<usize>)>,
        match_expr: &Arc<Option<FilterExpression>>, exclude_expr: &Arc<Option<FilterExpression>>, max_depth: usize,
        total_file_count: &Arc<AtomicUsize>, skip_filter: bool, packaged: bool, package_depth: usize,
        package_remaining: Option<usize>,
    ) -> Result<()> {
        // 使用tokio::fs::read_dir读取目录条目
        let mut dir_entries = tokio::fs::read_dir(&dir_path).await?;

        // 遍历目录条目
        while let Some(entry) = dir_entries.next_entry().await? {
            let Some(file_name) = entry.file_name().to_str().map(str::to_string) else {
                debug!(
                    "[Producer {}] Skipping entry with invalid name: {:?}",
                    producer_id,
                    entry.file_name()
                );
                continue;
            };

            // 跳过当前目录(".")和父目录("..")
            if file_name == "." || file_name == ".." {
                continue;
            }

            // 构建完整路径
            let full_path = entry.path();

            // 计算相对路径
            let Ok(relative_path) = full_path.strip_prefix(&*self.root_path) else {
                error!("[Producer {}] Failed to strip prefix from {:?}", producer_id, full_path);
                let _ = tx
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: full_path.clone(),
                        reason: "Failed to strip prefix".to_string(),
                    })
                    .await;
                continue;
            };
            // 确保相对路径不包含前导斜杠，与原始实现保持一致
            let relative_path = relative_path.to_path_buf();
            debug!(
                "[Producer {}] Processing entry: {:?}, skip_filter={}",
                producer_id, relative_path, skip_filter
            );

            // 提取扩展名
            let extension = relative_path.extension().and_then(|ext| ext.to_str());

            // symlink_metadata 对符号链接本身 lstat，同时提供 file_type，避免双重 syscall
            let metadata = match tokio::fs::symlink_metadata(&full_path).await {
                Ok(meta) => meta,
                Err(e) => {
                    error!(
                        "[Producer {}] Failed to get metadata for {:?}: {}",
                        producer_id, relative_path, e
                    );
                    let _ = tx
                        .send(StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: relative_path.clone(),
                            reason: format!("Failed to get metadata: {e}"),
                        })
                        .await;
                    continue;
                }
            };

            let is_dir = metadata.is_dir();
            let is_symlink = metadata.is_symlink();

            // 规范化路径分隔符：Windows 上将 '\' 转为 '/'，与 NFS/S3 及 FilterExpression 保持一致
            #[cfg(windows)]
            let normalized_path = relative_path.to_string_lossy().replace('\\', "/");
            #[cfg(not(windows))]
            let normalized_path = relative_path.to_string_lossy();

            // 过滤：基于文件名、路径、文件类型、修改时间、大小和扩展名
            let (skip_entry, continue_scan, need_submatch) = if skip_filter {
                should_skip(
                    match_expr.as_ref().as_ref(),
                    exclude_expr.as_ref().as_ref(),
                    Some(&file_name),
                    Some(&normalized_path),
                    Some(if is_symlink {
                        "symlink"
                    } else if is_dir {
                        "dir"
                    } else {
                        "file"
                    }),
                    Some(
                        metadata
                            .modified()
                            .unwrap_or(UNIX_EPOCH)
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0),
                    ),
                    Some(metadata.len()),
                    extension.or(Some("")),
                )
            } else {
                // skip_filter=false 表示父目录已匹配，子项无需过滤
                // need_submatch=false 确保免过滤传递给所有后代
                (false, true, false)
            };
            debug!(
                "[Producer {}] Filter result: skip={}, continue_scan={}, need_submatch={}",
                producer_id, skip_entry, continue_scan, need_submatch
            );

            // 计算条目的实际深度：目录深度+1
            let entry_depth = current_depth + 1;
            let mut send_packaged = false;

            // package 深度追踪模式：只处理目录，跳过文件和 filter
            if let Some(remaining) = package_remaining {
                if !is_dir {
                    continue;
                }
                if remaining > 1 {
                    ctx.push_task((full_path.clone(), current_depth + 1, false, Some(remaining - 1)))
                        .await;
                    continue;
                }
                // remaining <= 1：到达目标深度，标记发送 Packaged
                send_packaged = true;
            }

            if !send_packaged && skip_entry {
                // 如果skip_entry为true，但continue_scan为true，且是目录，则继续扫描其子目录
                if continue_scan && is_dir && (current_depth < max_depth || max_depth == 0) {
                    ctx.push_task((full_path.clone(), current_depth + 1, need_submatch, None))
                        .await;
                }
                debug!("[Producer {}] Skipping entry {:?} (filter)", producer_id, relative_path);
                continue;
            }

            // 创建StorageEntry
            let entry = EntryEnum::NAS(build_nas_entry(
                file_name.clone(),
                relative_path.clone(),
                extension.map(str::to_string),
                &metadata,
                is_symlink,
            ));

            // packaged 模式：目录匹配 DirDate 条件时决定打包策略
            if !send_packaged && packaged && is_dir && dir_matches_date_filter(match_expr.as_ref().as_ref(), &file_name)
            {
                if max_depth > 0 && entry_depth + package_depth > max_depth {
                    continue;
                }
                if package_depth > 0 {
                    ctx.push_task((full_path.clone(), current_depth + 1, false, Some(package_depth)))
                        .await;
                    continue;
                }
                send_packaged = true;
            }

            // 统一的 Packaged 发送
            if send_packaged {
                debug!(
                    "[Producer {}] Packaged dir {:?} (depth: {})",
                    producer_id, relative_path, entry_depth
                );
                if tx.send(StorageEntryMessage::Packaged(Arc::new(entry))).await.is_err() {
                    error!("[Producer {}] Output channel closed, stopping processing", producer_id);
                    break;
                }
                total_file_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // 检查深度限制：只有当条目深度在允许范围内时才发送
            // 0表示无限深度
            if max_depth == 0 || entry_depth <= max_depth {
                trace!(
                    "[Producer {}] Sending entry {:?} (depth: {})",
                    producer_id, entry, entry_depth
                );
                // 发送StorageEntry到通道
                if tx.send(StorageEntryMessage::Scanned(Arc::new(entry))).await.is_err() {
                    error!("[Producer {}] Output channel closed, stopping processing", producer_id);
                    break;
                }

                // 更新全局文件计数器
                total_file_count.fetch_add(1, Ordering::Relaxed);
            }

            // 如果是目录且未达到最大深度，将其添加到任务队列
            // 注意：current_depth是当前目录的深度，我们需要确保只处理到max_depth深度
            if is_dir && (current_depth < max_depth || max_depth == 0) {
                ctx.push_task((full_path.clone(), current_depth + 1, need_submatch, None))
                    .await;
            }
        }

        Ok(())
    }

    async fn read(&self, file: &mut LocalFileHandle, offset: u64, count: u64) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(count as usize);
        let mut current_offset = offset;
        let mut remaining = count;

        while remaining > 0 {
            file.inner.seek(std::io::SeekFrom::Start(current_offset)).await?;
            let read_bytes = file.inner.read_buf(&mut buffer).await? as u64;

            if read_bytes == 0 {
                break;
            }

            current_offset += read_bytes;
            remaining -= read_bytes;
        }

        trace!("read {} bytes from file in local storage using tokio", buffer.len());
        Ok(buffer.split().freeze())
    }

    /// 向文件句柄写入数据
    async fn write(&self, file: &mut LocalFileHandle, offset: u64, data: Bytes) -> Result<usize> {
        trace!(
            "write file in local storage: offset {}, data len {}",
            offset,
            data.len()
        );
        let length = data.len();

        file.inner.seek(std::io::SeekFrom::Start(offset)).await?;
        let written = file.inner.write_buf(&mut std::io::Cursor::new(data)).await?;

        if written != length {
            return Err(StorageError::IoError(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("Incomplete write: expected {length} bytes, wrote {written} bytes"),
            )));
        }

        trace!("Wrote {} bytes at offset {}", written, offset);

        Ok(written)
    }

    pub(crate) async fn read_file(&self, path: &Path, size: u64) -> Result<Bytes> {
        let mut handle = self.open(path).await?;
        self.read(&mut handle, 0, size).await
    }

    pub(crate) async fn write_file(
        &self, path: &Path, data: Bytes, uid: Option<u32>, gid: Option<u32>, mode: Option<u32>,
    ) -> Result<()> {
        let mut handle = self.create_file(path, uid, gid, mode).await?;
        self.write(&mut handle, 0, data).await?;
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
        &self, tx: Sender<DataChunk>, relative_path: &Path, size: u64, enable_integrity_check: bool,
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
        let mut source_file = match self.open(relative_path).await {
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

        // 简单循环持续读取文件直到文件结束
        let mut offset = 0;
        let mut bytes_read: u64 = 0;

        let mut hasher = create_hash_calculator(enable_integrity_check);

        loop {
            // 如果提供了 QoS 管理器，则进行带宽 + IOPS 限流
            if let Some(ref qos) = qos {
                qos.acquire(chunk_size).await;
                debug!("QoS acquired {} bytes for file {:?}", chunk_size, relative_path);
            }

            let data = match self.read(&mut source_file, offset, chunk_size).await {
                Ok(data) => data,
                Err(e) => {
                    error!(
                        "Failed to read data chunk (offset: {}, chunk size: {}): {:?}",
                        offset, chunk_size, e
                    );
                    break;
                }
            };

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
            if let Err(e) = tx.send(DataChunk { offset, data }).await {
                error!("Failed to send data chunk: {:?}", e);
                // 通道已关闭，退出循环
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

            // 更新偏移量
            offset += data_length;
            // 如果已经读取了整个文件，退出循环
            if offset >= size {
                debug!("Completed reading file {:?}", relative_path);
                break;
            }
        }

        trace!(
            "Finished read_data_task for file {:?}, total bytes processed: {}",
            relative_path, bytes_read
        );

        Ok(hasher)
    }

    pub(crate) async fn write_data(
        &self, rx: tokio::sync::mpsc::Receiver<DataChunk>, relative_path: &Path, #[allow(unused)] uid: Option<u32>,
        #[allow(unused)] gid: Option<u32>, #[allow(unused)] mode: Option<u32>, bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        trace!("Starting write_data_task for file {:?}", relative_path);

        let mut reader = rx;

        // 注意：这里需要重新创建目标文件，因为我们不能在线程间共享文件句柄
        let mut dest_file = self.create_file(relative_path, uid, gid, mode).await?;
        debug!(
            "Created destination file {:?} with mode: {:?}",
            relative_path,
            mode.map(|m| format!("{m:o}"))
        );

        let mut current_offset = 0;

        // 处理从通道接收的数据块
        while let Some(chunk) = reader.recv().await {
            let data = chunk.data;

            trace!(
                "Received chunk of {} bytes at offset {} for file {:?}",
                data.len(),
                chunk.offset,
                relative_path
            );

            let written = self.write(&mut dest_file, current_offset, data).await? as u64;
            if let Some(ref c) = bytes_counter {
                c.fetch_add(written, Ordering::Relaxed);
            }
            trace!("Wrote {} bytes at offset {}", written, current_offset);
            // 更新当前偏移量
            current_offset += written;
        }

        trace!("Finished write_data_task for file {:?}", relative_path);
        Ok(())
    }
}

// ============================================================
// walkdir_2: 目录分页 + NDX 编号 + 并行预读
// ============================================================
impl LocalStorage {
    /// 读取单个目录，返回排序后的 files + subdirs。Reader Worker 调用此函数。
    pub(crate) async fn read_dir_sorted(
        &self, dir_path: &str, handle: &crate::dir_tree::DirHandle, ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult> {
        use crate::dir_tree::{DirHandle, ReadResult, SubdirEntry};

        let full_path = match handle {
            DirHandle::Local(p) => p.clone(),
            _ => {
                return Err(StorageError::OperationError(
                    "DirHandle type mismatch: expected Local".into(),
                ));
            }
        };

        let mut files: Vec<Arc<EntryEnum>> = Vec::new();
        let mut subdirs: Vec<SubdirEntry> = Vec::new();
        let mut errors: Vec<String> = Vec::new();

        let mut dir = match tokio::fs::read_dir(&full_path).await {
            Ok(d) => d,
            Err(e) => {
                return Ok(ReadResult {
                    dir_path: dir_path.to_string(),
                    files: Vec::new(),
                    subdirs: Vec::new(),
                    errors: vec![e.to_string()],
                });
            }
        };

        while let Ok(Some(entry)) = dir.next_entry().await {
            let file_name = match entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };
            if file_name == "." || file_name == ".." {
                continue;
            }

            let entry_full_path = entry.path();
            let Ok(relative_path) = entry_full_path.strip_prefix(&*self.root_path) else {
                errors.push(format!("Failed to strip prefix: {}", entry_full_path.display()));
                continue;
            };
            let relative_path = relative_path.to_path_buf();

            let metadata = match tokio::fs::symlink_metadata(&entry_full_path).await {
                Ok(m) => m,
                Err(e) => {
                    errors.push(format!("{}: {}", relative_path.display(), e));
                    continue;
                }
            };

            let is_dir = metadata.is_dir();
            let is_symlink = entry.file_type().await.map(|ft| ft.is_symlink()).unwrap_or(false);
            let extension_owned = relative_path.extension().and_then(|e| e.to_str()).map(str::to_string);

            // 应用 filter（仅当 apply_filter=true 时）
            let (skip_entry, continue_scan, need_submatch) = if ctx.apply_filter {
                #[cfg(windows)]
                let normalized = relative_path.to_string_lossy().replace('\\', "/");
                #[cfg(not(windows))]
                let normalized = relative_path.to_string_lossy();

                crate::filter::should_skip(
                    ctx.match_expr.as_ref().as_ref(),
                    ctx.exclude_expr.as_ref().as_ref(),
                    Some(&file_name),
                    Some(&normalized),
                    Some(if is_symlink {
                        "symlink"
                    } else if is_dir {
                        "dir"
                    } else {
                        "file"
                    }),
                    metadata
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64),
                    Some(metadata.len()),
                    extension_owned.as_deref().or(Some("")),
                )
            } else {
                // 父目录已匹配，子项无需过滤
                (false, true, false)
            };

            if skip_entry {
                // 目录被跳过但 continue_scan=true → 加入 subdirs 但 visible=false
                if is_dir && continue_scan && (ctx.max_depth == 0 || ctx.current_depth + 1 < ctx.max_depth) {
                    let nas = build_nas_entry(file_name, relative_path, extension_owned, &metadata, is_symlink);
                    subdirs.push(SubdirEntry {
                        entry: Arc::new(EntryEnum::NAS(nas)),
                        visible: false,
                        need_filter: need_submatch,
                    });
                }
                continue;
            }

            let nas = build_nas_entry(file_name, relative_path, extension_owned, &metadata, is_symlink);
            let entry_enum = Arc::new(EntryEnum::NAS(nas));

            // 深度检查：超过 max_depth 的子目录不进入 subdirs（不递归），但仍作为 entry 记录
            if is_dir && ctx.max_depth > 0 && ctx.current_depth + 1 >= ctx.max_depth {
                files.push(entry_enum);
            } else if is_dir {
                subdirs.push(SubdirEntry {
                    entry: entry_enum,
                    visible: true,
                    need_filter: need_submatch,
                });
            } else {
                files.push(entry_enum);
            }
        }

        // 排序
        files.sort_by(|a, b| a.get_name().cmp(b.get_name()));
        subdirs.sort_by(|a, b| a.entry.get_name().cmp(b.entry.get_name()));

        Ok(ReadResult {
            dir_path: dir_path.to_string(),
            files,
            subdirs,
            errors,
        })
    }

    /// `walkdir_2`: 目录分页遍历，DFS 顺序分配 NDX，页级输出
    #[allow(clippy::unused_async)]
    pub async fn walkdir_2(
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<crate::FilterExpression>,
        exclude_expressions: Option<crate::FilterExpression>, concurrency: usize,
    ) -> Result<crate::WalkDirAsyncIterator2> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        let root_full = match sub_path {
            Some(p) if !p.as_os_str().is_empty() => self.get_full_path(p),
            _ => (*self.root_path).clone(),
        };

        let concurrency = concurrency.clamp(1, 64);
        let (req_tx, req_rx) = async_channel::bounded::<ReadRequest>(concurrency * 2);
        let (out_tx, out_rx) = async_channel::bounded(64);

        // 启动 Reader Worker
        for _ in 0..concurrency {
            let storage = self.clone();
            let rx = req_rx.clone();
            tokio::spawn(async move {
                while let Ok(req) = rx.recv().await {
                    let result = storage.read_dir_sorted(&req.dir_path, &req.handle, &req.ctx).await;
                    let _ = req.reply.send(result);
                }
            });
        }

        let root_handle = DirHandle::Local(root_full);
        let root_path = (*self.root_path).clone();
        let base_ctx = ReadContext {
            match_expr: Arc::new(match_expressions),
            exclude_expr: Arc::new(exclude_expressions),
            current_depth: 0,
            max_depth: depth.unwrap_or(0),
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };

        tokio::spawn(run_dfs_driver(req_tx, out_tx, root_path, root_handle, base_ctx));

        Ok(crate::AsyncReceiver::new(out_rx))
    }
}

/// 将用户输入路径规范化：canonicalize + Windows 长路径前缀处理
/// 注意：canonicalize 要求路径已存在
fn normalize_local_path(path: &str) -> Result<String> {
    let canonical_path = std::fs::canonicalize(path)
        .map_err(|e| StorageError::InvalidPath(format!("Failed to canonicalize path '{path}': {e}")))?;

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

/// 创建本地目标存储实例，如果目录不存在则自动创建
pub fn create_local_storage_ensuring_dir(path: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    // create_dir_all 是幂等操作：目录已存在时不报错，不存在时递归创建
    std::fs::create_dir_all(path).map_err(StorageError::IoError)?;
    create_local_storage(path, block_size)
}

/// 创建本地存储实例
pub fn create_local_storage(path: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    debug!("In create local storage Raw path: {}", path);

    // canonicalize 要求路径已存在，路径不存在时会返回 InvalidPath 错误
    let local_path = normalize_local_path(path)?;
    debug!("In create local storage Normalized path: {}", local_path);

    let local_storage = LocalStorage::new(&local_path, block_size);
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
        });
    }
}
