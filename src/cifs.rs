// 标准库
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// 外部 crate
use bytes::Bytes;
use futures::StreamExt;
use smb::binrw_util::file_time::FileTime;
use smb::{
    ACE, ACL, AclRevision, AdditionalInfo, Client, ClientConfig, CreateOptions, FileAccessMask, FileAttributes,
    FileBasicInformation, FileCreateArgs, FileDirectoryInformation, FileStandardInformation, ReadAt, Resource,
    ResourceHandle, SecurityDescriptor, UncPath, WriteAt,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

// 内部模块
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

/// Windows FILETIME epoch (1601-01-01) 与 Unix epoch (1970-01-01) 之间的 100ns 间隔数
const FILETIME_UNIX_EPOCH_DIFF: i64 = 116_444_736_000_000_000;

/// 将 SMB `FileTime` (100ns since 1601-01-01) 转换为纳秒时间戳 (ns since Unix epoch)
fn filetime_to_nanos(ft: FileTime) -> i64 {
    // FileTime Deref<Target=u64>，值是 100ns 间隔数
    let raw = *ft as i64;
    (raw - FILETIME_UNIX_EPOCH_DIFF) * 100
}

/// 将纳秒时间戳 (ns since Unix epoch) 转换为 SMB `FileTime`
fn nanos_to_filetime(ns: i64) -> FileTime {
    let raw = (ns / 100 + FILETIME_UNIX_EPOCH_DIFF) as u64;
    FileTime::from(raw)
}

/// 将 SMB 文件属性映射为 Unix mode 近似值
fn smb_attributes_to_mode(is_dir: bool, is_readonly: bool) -> u32 {
    if is_dir {
        if is_readonly { 0o555 } else { 0o755 }
    } else if is_readonly {
        0o444
    } else {
        0o644
    }
}

/// 简易 percent-decode：替换 %XX 为对应字节
fn url_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            result.push(byte);
            i += 3;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_else(|_| s.to_string())
}

const DEFAULT_BLOCK_SIZE: u64 = 2 * MB;

#[derive(Clone, Debug)]
pub(crate) struct StorageConfig {
    pub block_size: u64,
}

/// SMB/CIFS 存储后端
///
/// 使用 smb crate 实现 SMB2/3 协议客户端，支持文件扫描和迁移。
/// Client 通过 Arc 共享以实现连接复用。
#[derive(Clone)]
pub struct CifsStorage {
    /// SMB 客户端实例（Arc 共享，跨 worker 复用）
    client: Arc<Client>,
    /// SMB 共享路径（\\server\share）
    share_path: Arc<UncPath>,
    /// 共享内的子目录前缀（相对路径）
    root: Arc<String>,
    /// 存储配置
    pub(crate) config: StorageConfig,
}

impl std::fmt::Debug for CifsStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CifsStorage")
            .field("share_path", &self.share_path)
            .field("root", &self.root)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// 脱敏 SMB URL，将密码部分替换为 ***，用于错误日志
///
/// `smb://user:password@host/share` → `smb://user:***@host/share`
fn redact_smb_url(url_str: &str) -> String {
    // rfind('@') 而非 find：密码可能含 %40（URL 编码的 @）
    if let Some(at_pos) = url_str.rfind('@')
        && url_str.starts_with("smb://")
    {
        let after_scheme = &url_str["smb://".len()..at_pos];
        if let Some(colon_pos) = after_scheme.find(':') {
            let user_end = "smb://".len() + colon_pos;
            return format!("{}:***{}", &url_str[..user_end], &url_str[at_pos..]);
        }
    }
    // 无法定位密码时，整体脱敏
    "<smb-url-redacted>".to_string()
}

/// 解析 SMB URL
///
/// 格式：`smb://user:pass@host[:port]/share[/sub/path]`
/// 密码支持 URL 编码（%40 = @, %3A = : 等）
///
/// 返回 (server, port, share, `sub_path`, username, password)
fn parse_smb_url(url_str: &str) -> Result<(String, u16, String, String, String, String)> {
    if !url_str.starts_with("smb://") {
        return Err(StorageError::CifsError(format!(
            "Invalid SMB URL format, must start with smb://: {}",
            redact_smb_url(url_str)
        )));
    }

    // url crate 不认识 smb scheme，替换为 http 来复用解析器
    let http_url = format!("http{}", &url_str[3..]);
    let parsed = url::Url::parse(&http_url)
        .map_err(|e| StorageError::CifsError(format!("Failed to parse SMB URL '{}': {e}", redact_smb_url(url_str))))?;

    let username_raw = parsed.username();
    if username_raw.is_empty() {
        return Err(StorageError::CifsError(format!(
            "Missing username in SMB URL: {}",
            redact_smb_url(url_str)
        )));
    }
    // percent-decode 用户名和密码
    let username = url_decode(username_raw);

    let password_raw = parsed
        .password()
        .ok_or_else(|| StorageError::CifsError(format!("Missing password in SMB URL: {}", redact_smb_url(url_str))))?;
    let password = url_decode(password_raw);

    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::CifsError(format!("Missing host in SMB URL: {}", redact_smb_url(url_str))))?
        .to_string();

    let port = parsed.port().unwrap_or(445);

    // path 格式: /share[/sub/path]
    let path = parsed.path().trim_start_matches('/');
    if path.is_empty() {
        return Err(StorageError::CifsError(format!(
            "Missing share name in SMB URL: {}",
            redact_smb_url(url_str)
        )));
    }

    let (share, sub_path) = if let Some((s, p)) = path.split_once('/') {
        (s.to_string(), p.trim_end_matches('/').to_string())
    } else {
        (path.to_string(), String::new())
    };

    if share.is_empty() {
        return Err(StorageError::CifsError(format!(
            "Empty share name in SMB URL: {}",
            redact_smb_url(url_str)
        )));
    }

    Ok((host, port, share, sub_path, username, password))
}

impl CifsStorage {
    /// 创建 `CifsStorage` 实例
    ///
    /// 解析 URL → 创建 Client → 连接共享 → 验证连通性
    pub async fn new(url: &str, block_size: Option<u64>) -> Result<Self> {
        let (host, port, share, sub_path, username, password) = parse_smb_url(url)?;

        info!("Connecting to SMB share \\\\{host}:{port}/{share}");

        let client = Client::new(ClientConfig::default());
        let unc = format!(r"\\{host}:{port}\{share}");
        let share_path = UncPath::from_str(&unc)
            .map_err(|e| StorageError::CifsError(format!("Failed to parse UNC path '{unc}': {e}")))?;

        client
            .share_connect(&share_path, &username, password)
            .await
            .map_err(|e| {
                StorageError::CifsError(format!("Failed to connect to SMB share \\\\{host}:{port}/{share}: {e}"))
            })?;

        let effective_block_size = block_size.unwrap_or(DEFAULT_BLOCK_SIZE).min(DEFAULT_BLOCK_SIZE);

        let storage = CifsStorage {
            client: Arc::new(client),
            share_path: Arc::new(share_path),
            root: Arc::new(sub_path),
            config: StorageConfig {
                block_size: effective_block_size,
            },
        };

        // 验证连通性：尝试打开根目录
        storage.check_connectivity().await?;

        info!("Successfully connected to SMB share \\\\{host}:{port}/{share}");
        Ok(storage)
    }

    /// 构建完整的 UNC 路径（`share_path` + root + `relative_path`）
    fn build_unc_path(&self, relative_path: &Path) -> UncPath {
        let rel = relative_path.to_string_lossy().replace('/', "\\");
        if self.root.is_empty() {
            if rel.is_empty() {
                (*self.share_path).clone()
            } else {
                (*self.share_path).clone().with_path(&rel)
            }
        } else {
            let full = if rel.is_empty() {
                self.root.replace('/', "\\")
            } else {
                format!("{}\\{rel}", self.root.replace('/', "\\"))
            };
            (*self.share_path).clone().with_path(&full)
        }
    }

    /// 构建相对路径字符串（root + `relative_path`，使用 '/' 分隔）
    fn build_relative_path(dir_path: &str, name: &str) -> String {
        if dir_path.is_empty() {
            name.to_string()
        } else {
            format!("{dir_path}/{name}")
        }
    }

    /// 验证存储连通性
    pub async fn check_connectivity(&self) -> Result<()> {
        let root_unc = self.build_unc_path(Path::new(""));
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        self.client
            .create_file(&root_unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Connectivity check failed: {e}")))?;
        Ok(())
    }

    /// 块大小计算
    #[inline]
    fn calculate_chunk_size(&self, file_size: u64) -> u64 {
        std::cmp::min(file_size, self.config.block_size).max(1)
    }

    // ========================================================================
    // 文件读取
    // ========================================================================

    /// 单块读取整个文件内容
    pub(crate) async fn read_file(&self, path: &Path, size: u64) -> Result<Bytes> {
        if size == 0 {
            return Ok(Bytes::new());
        }

        let unc = self.build_unc_path(path);
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource = self
            .client
            .create_file(&unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to open file {}: {e}", path.display())))?;

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                path.display()
            )));
        };

        let mut buf = vec![0u8; size as usize];
        let bytes_read = file
            .read_at(&mut buf, 0)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to read file {}: {e}", path.display())))?;

        buf.truncate(bytes_read);
        let _ = file.close().await;

        // Vec<u8> → Bytes 零拷贝转移所有权
        Ok(Bytes::from(buf))
    }

    /// 多块流式读取文件，通过 channel 发送 `DataChunk`
    pub(crate) async fn read_data(
        &self, tx: mpsc::Sender<DataChunk>, relative_path: &Path, size: u64, enable_integrity_check: bool,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        if size == 0 {
            trace!("File {:?} is empty, skipping read", relative_path);
            return Ok(None);
        }

        let chunk_size = self.calculate_chunk_size(size);
        trace!(
            "Starting CIFS read_data for file {:?}, size: {}, chunk_size: {}",
            relative_path, size, chunk_size
        );

        let unc = self.build_unc_path(relative_path);
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource =
            self.client.create_file(&unc, &args).await.map_err(|e| {
                StorageError::CifsError(format!("Failed to open file {}: {e}", relative_path.display()))
            })?;

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                relative_path.display()
            )));
        };

        let mut offset = 0u64;
        let mut hasher = create_hash_calculator(enable_integrity_check);

        loop {
            if let Some(ref qos) = qos {
                qos.acquire(chunk_size).await;
            }

            let read_size = std::cmp::min(chunk_size, size - offset) as usize;
            let mut buf = vec![0u8; read_size];

            let bytes_read = match file.read_at(&mut buf, offset).await {
                Ok(n) => n,
                Err(e) => {
                    error!("Failed to read data chunk at offset {}: {:?}", offset, e);
                    break;
                }
            };

            if bytes_read == 0 {
                trace!("Reached end of file {:?}", relative_path);
                break;
            }

            buf.truncate(bytes_read);
            let data = Bytes::from(buf);

            if let Some(ref mut h) = hasher {
                h.update(&data);
            }

            if tx.send(DataChunk { offset, data }).await.is_err() {
                error!("Data channel closed for file {:?}", relative_path);
                break;
            }

            offset += bytes_read as u64;
            if offset >= size {
                break;
            }
        }

        let _ = file.close().await;
        Ok(hasher)
    }

    // ========================================================================
    // 文件写入
    // ========================================================================

    /// 单块写入文件
    pub(crate) async fn write_file(
        &self, path: &Path, data: Bytes, _uid: Option<u32>, _gid: Option<u32>, _mode: Option<u32>,
    ) -> Result<()> {
        // 确保父目录存在
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent).await?;
        }

        let unc = self.build_unc_path(path);
        let args = FileCreateArgs::make_create_new(FileAttributes::default(), CreateOptions::default());

        let resource = if let Ok(r) = self.client.create_file(&unc, &args).await {
            r
        } else {
            // 文件可能已存在，尝试打开覆盖写入
            let open_args = FileCreateArgs::make_open_existing(
                FileAccessMask::new().with_generic_write(true).with_generic_read(true),
            );
            self.client
                .create_file(&unc, &open_args)
                .await
                .map_err(|e| StorageError::CifsError(format!("Failed to create/open file {}: {e}", path.display())))?
        };

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                path.display()
            )));
        };

        // SMB write_at(0) automatically truncates file when writing from offset 0
        file.write_at(&data, 0)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to write file {}: {e}", path.display())))?;

        let _ = file.close().await;
        Ok(())
    }

    /// 多块流式写入文件
    pub(crate) async fn write_data(
        &self, rx: mpsc::Receiver<DataChunk>, relative_path: &Path, _uid: Option<u32>, _gid: Option<u32>,
        _mode: Option<u32>, bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        trace!("Starting CIFS write_data for file {:?}", relative_path);

        // 确保父目录存在
        if let Some(parent) = relative_path.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent).await?;
        }

        let unc = self.build_unc_path(relative_path);
        let args = FileCreateArgs::make_create_new(FileAttributes::default(), CreateOptions::default());

        let resource = if let Ok(r) = self.client.create_file(&unc, &args).await {
            r
        } else {
            let open_args = FileCreateArgs::make_open_existing(
                FileAccessMask::new().with_generic_write(true).with_generic_read(true),
            );
            self.client.create_file(&unc, &open_args).await.map_err(|e| {
                StorageError::CifsError(format!("Failed to create/open file {}: {e}", relative_path.display()))
            })?
        };

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                relative_path.display()
            )));
        };

        // SMB write_at(0) automatically truncates file when writing from offset 0
        // No explicit truncate needed for multi-chunk writes

        let mut reader = rx;
        while let Some(chunk) = reader.recv().await {
            let len = chunk.data.len() as u64;
            file.write_at(&chunk.data, chunk.offset).await.map_err(|e| {
                StorageError::CifsError(format!("Failed to write data at offset {}: {e}", chunk.offset))
            })?;

            if let Some(ref c) = bytes_counter {
                c.fetch_add(len, Ordering::Relaxed);
            }
        }

        let _ = file.close().await;
        trace!("Finished CIFS write_data for file {:?}", relative_path);
        Ok(())
    }

    // ========================================================================
    // 目录操作
    // ========================================================================

    /// 递归创建目录（逐级）
    pub async fn create_dir_all(&self, relative_path: &Path) -> Result<()> {
        debug!("CIFS create_dir_all: {:?}", relative_path);

        let path_str = relative_path.to_string_lossy().replace('\\', "/");
        let components: Vec<&str> = path_str.split('/').filter(|s| !s.is_empty()).collect();

        let mut current = String::new();
        for component in components {
            if current.is_empty() {
                current = component.to_string();
            } else {
                current = format!("{current}/{component}");
            }

            let unc = self.build_unc_path(Path::new(&current));

            // 尝试创建目录，如果已存在则忽略
            let args = FileCreateArgs::make_create_new(
                FileAttributes::new().with_directory(true),
                CreateOptions::new().with_directory_file(true),
            );
            if let Ok(resource) = self.client.create_file(&unc, &args).await {
                // 目录创建成功，关闭句柄
                if let Resource::Directory(d) = resource {
                    let _ = d.close().await;
                }
            } else {
                // 目录可能已存在，验证一下
                let open_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
                if let Err(e) = self.client.create_file(&unc, &open_args).await {
                    return Err(StorageError::CifsError(format!(
                        "Failed to create directory '{current}': {e}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// 删除文件或目录
    ///
    /// 通过 `set_info<FileDispositionInformation>` 标记删除，关闭时生效
    pub async fn delete_file(&self, relative_path: &Path) -> Result<()> {
        trace!("CIFS removing file {:?}", relative_path);

        let unc = self.build_unc_path(relative_path);

        // 打开文件并标记为删除
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_write(true).with_delete(true));
        let resource = self.client.create_file(&unc, &args).await.map_err(|e| {
            StorageError::CifsError(format!("Failed to open for deletion {}: {e}", relative_path.display()))
        })?;

        // 使用 set_info 标记删除
        let disposition = smb::FileDispositionInformation {
            delete_pending: true.into(),
        };
        match &resource {
            Resource::File(f) => {
                f.set_info(disposition).await.map_err(|e| {
                    StorageError::CifsError(format!("Failed to delete {}: {e}", relative_path.display()))
                })?;
                let _ = f.close().await;
            }
            Resource::Directory(d) => {
                d.set_info(disposition).await.map_err(|e| {
                    StorageError::CifsError(format!("Failed to delete {}: {e}", relative_path.display()))
                })?;
                let _ = d.close().await;
            }
            Resource::Pipe(_) => {}
        }

        Ok(())
    }

    /// 删除目录（单个空目录）
    async fn delete_dir(&self, relative_path: &Path) -> Result<()> {
        self.delete_file(relative_path).await
    }

    /// 并行删除目录下所有文件和子目录，返回进度迭代器
    pub fn delete_dir_all_with_progress(
        &self, relative_path: Option<&Path>, concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let (tx, rx) = async_channel::bounded::<DeleteEvent>(1000);
        let concurrency = concurrency.clamp(1, 64);
        let storage = self.clone();
        let sub_path = relative_path.map(PathBuf::from);

        tokio::spawn(async move {
            let walkdir_result = match storage
                .walkdir(sub_path.as_deref(), None, None, None, concurrency, false, 0)
                .await
            {
                Ok(iter) => iter,
                Err(e) => {
                    error!("Failed to start walkdir for delete: {:?}", e);
                    return;
                }
            };

            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut dir_paths: Vec<(PathBuf, usize)> = Vec::new();
            let mut file_handles = Vec::new();

            while let Some(result) = walkdir_result.next().await {
                match result {
                    StorageEntryMessage::Scanned(entry) => {
                        if entry.get_is_dir() {
                            let depth = entry.get_relative_path().components().count();
                            dir_paths.push((entry.get_relative_path().to_path_buf(), depth));
                        } else {
                            let Ok(permit) = semaphore.clone().acquire_owned().await else {
                                break;
                            };
                            let storage_c = storage.clone();
                            let tx_c = tx.clone();
                            let path = entry.get_relative_path().to_path_buf();
                            file_handles.push(tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = storage_c.delete_file(&path).await {
                                    error!("Failed to delete file {:?}: {:?}", path, e);
                                } else {
                                    let _ = tx_c
                                        .send(DeleteEvent {
                                            relative_path: path,
                                            is_dir: false,
                                        })
                                        .await;
                                }
                            }));
                        }
                    }
                    StorageEntryMessage::Error { event, path, reason } => {
                        error!("Walkdir error during delete [{}] {:?}: {}", event, path, reason);
                    }
                    _ => {}
                }
            }

            for h in file_handles {
                let _ = h.await;
            }

            dir_paths.sort_by(|a, b| b.1.cmp(&a.1));
            for (path, _) in dir_paths {
                if let Err(e) = storage.delete_dir(&path).await {
                    error!("Failed to delete dir {:?}: {:?}", path, e);
                } else {
                    let _ = tx
                        .send(DeleteEvent {
                            relative_path: path,
                            is_dir: true,
                        })
                        .await;
                }
            }
        });

        Ok(DeleteDirIterator::new(rx))
    }

    /// 重命名文件或目录
    ///
    /// 通过 `set_info<FileRenameInformation>` 实现
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        trace!("CIFS rename {:?} to {:?}", from, to);

        // 确保目标父目录存在
        if let Some(parent) = to.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent).await?;
        }

        let from_unc = self.build_unc_path(from);
        let to_unc = self.build_unc_path(to);
        let to_path_str = to_unc.to_string();

        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_write(true).with_delete(true));
        let resource = self
            .client
            .create_file(&from_unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to open {} for rename: {e}", from.display())))?;

        let rename_info = smb::FileRenameInformation {
            replace_if_exists: true.into(),
            file_name: to_path_str.into(),
            root_directory: 0,
        };

        match &resource {
            Resource::File(f) => {
                f.set_info(rename_info).await.map_err(|e| {
                    StorageError::CifsError(format!("Failed to rename {} to {}: {e}", from.display(), to.display()))
                })?;
                let _ = f.close().await;
            }
            Resource::Directory(d) => {
                d.set_info(rename_info).await.map_err(|e| {
                    StorageError::CifsError(format!("Failed to rename {} to {}: {e}", from.display(), to.display()))
                })?;
                let _ = d.close().await;
            }
            Resource::Pipe(_) => {
                return Err(StorageError::CifsError(format!(
                    "Cannot rename {}: unsupported resource type",
                    from.display()
                )));
            }
        }

        Ok(())
    }

    // ========================================================================
    // 元数据操作
    // ========================================================================

    /// 获取文件或目录的元数据
    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        debug!("CIFS get_metadata {:?}", relative_path);

        let unc = self.build_unc_path(relative_path);
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));

        let resource = self.client.create_file(&unc, &args).await.map_err(|e| {
            StorageError::CifsError(format!("Failed to open {} for metadata: {e}", relative_path.display()))
        })?;

        let (basic_info, standard_info, is_dir) = match &resource {
            Resource::File(f) => {
                let basic = f
                    .query_info::<FileBasicInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query basic info: {e}")))?;
                let standard = f
                    .query_info::<FileStandardInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query standard info: {e}")))?;
                (basic, standard, false)
            }
            Resource::Directory(d) => {
                let basic = d
                    .query_info::<FileBasicInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query basic info: {e}")))?;
                let standard = d
                    .query_info::<FileStandardInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query standard info: {e}")))?;
                (basic, standard, true)
            }
            Resource::Pipe(_) => {
                return Err(StorageError::CifsError(format!(
                    "Unsupported resource type for {}",
                    relative_path.display()
                )));
            }
        };

        let filename = if relative_path.as_os_str().is_empty() {
            String::from("/")
        } else {
            relative_path
                .file_name()
                .map_or_else(|| "/".to_string(), |n| n.to_string_lossy().to_string())
        };

        let extension = relative_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(std::string::ToString::to_string);

        let is_readonly = basic_info.file_attributes.readonly();
        let mode = smb_attributes_to_mode(is_dir, is_readonly);

        let entry = EntryEnum::NAS(NASEntry {
            name: filename,
            relative_path: relative_path.to_path_buf(),
            is_dir,
            size: standard_info.end_of_file,
            extension,
            mtime: filetime_to_nanos(basic_info.last_write_time),
            atime: filetime_to_nanos(basic_info.last_access_time),
            ctime: filetime_to_nanos(basic_info.creation_time),
            mode,
            hard_links: None,
            is_symlink: basic_info.file_attributes.reparse_point(),
            file_handle: None,
            uid: None,
            gid: None,
            ino: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        });

        // 关闭资源
        match resource {
            Resource::File(f) => {
                let _ = f.close().await;
            }
            Resource::Directory(d) => {
                let _ = d.close().await;
            }
            Resource::Pipe(_) => {}
        }

        Ok(entry)
    }

    /// 更新文件的时间戳元数据
    ///
    /// 注意：SMB 不原生支持 Unix uid/gid/mode，仅设置时间戳
    pub async fn update_metadata(
        &self, relative_path: &Path, atime: Option<i64>, mtime: Option<i64>, _uid: Option<u32>, _gid: Option<u32>,
        _mode: Option<u32>,
    ) -> Result<()> {
        debug!("CIFS update_metadata {:?}", relative_path);

        let unc = self.build_unc_path(relative_path);
        let args =
            FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_write(true).with_generic_read(true));

        let resource = self.client.create_file(&unc, &args).await.map_err(|e| {
            StorageError::CifsError(format!(
                "Failed to open {} for metadata update: {e}",
                relative_path.display()
            ))
        })?;

        // 构建 FileBasicInformation 来设置时间戳
        // 只有在有值时才设置对应的时间字段
        // 先读取当前的 FileBasicInformation，然后修改时间戳字段
        let open_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let read_resource = self.client.create_file(&unc, &open_args).await.map_err(|e| {
            StorageError::CifsError(format!(
                "Failed to open {} for reading metadata: {e}",
                relative_path.display()
            ))
        })?;

        let mut basic = match &read_resource {
            Resource::File(f) => f
                .query_info::<FileBasicInformation>()
                .await
                .map_err(|e| StorageError::CifsError(format!("Failed to query basic info: {e}")))?,
            Resource::Directory(d) => d
                .query_info::<FileBasicInformation>()
                .await
                .map_err(|e| StorageError::CifsError(format!("Failed to query basic info: {e}")))?,
            Resource::Pipe(_) => {
                return Err(StorageError::CifsError("Unsupported resource type".to_string()));
            }
        };
        // 关闭只读句柄
        match read_resource {
            Resource::File(f) => {
                let _ = f.close().await;
            }
            Resource::Directory(d) => {
                let _ = d.close().await;
            }
            Resource::Pipe(_) => {}
        }

        if let Some(atime_ns) = atime {
            basic.last_access_time = nanos_to_filetime(atime_ns);
        }
        if let Some(mtime_ns) = mtime {
            basic.last_write_time = nanos_to_filetime(mtime_ns);
        }

        match &resource {
            Resource::File(f) => {
                f.set_info(basic)
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to set metadata: {e}")))?;
                let _ = f.close().await;
            }
            Resource::Directory(d) => {
                d.set_info(basic)
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to set metadata: {e}")))?;
                let _ = d.close().await;
            }
            Resource::Pipe(_) => {}
        }

        Ok(())
    }

    // ========================================================================
    // 符号链接（SMB 有限支持）
    // ========================================================================

    /// 创建符号链接
    ///
    /// SMB 通过 reparse point 支持符号链接。如果服务端不支持，静默返回 Ok(())
    #[allow(clippy::unused_async)]
    pub async fn create_symlink(
        &self, _relative_path: &Path, _target_path: &Path, _atime: i64, _mtime: i64, _uid: Option<u32>,
        _gid: Option<u32>,
    ) -> Result<()> {
        // SMB 符号链接创建需要特殊权限（SeCreateSymbolicLinkPrivilege）
        // 大多数场景下不可用，静默跳过
        warn!("CIFS symlink creation is not supported, skipping");
        Ok(())
    }

    /// 读取符号链接目标
    #[allow(clippy::unused_async)]
    pub async fn read_symlink(&self, _relative_path: &Path) -> Result<PathBuf> {
        // SMB 符号链接读取需要 FSCTL_GET_REPARSE_POINT
        // 当前返回空路径
        warn!("CIFS symlink reading is not supported");
        Ok(PathBuf::new())
    }

    // ========================================================================
    // 目录遍历（核心 — 性能关键）
    // ========================================================================

    /// 并行遍历目录树
    ///
    /// 使用 work-stealing scheduler 实现高效并行目录遍历。
    /// 每个 worker 独立查询子目录，通过 bounded channel 控制内存。
    #[allow(clippy::too_many_arguments, clippy::unused_async)]
    pub async fn walkdir(
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, packaged: bool, package_depth: usize,
    ) -> Result<WalkDirAsyncIterator> {
        let start_root = match sub_path {
            Some(p) if !p.as_os_str().is_empty() => {
                if self.root.is_empty() {
                    p.to_string_lossy().replace('\\', "/")
                } else {
                    format!("{}/{}", self.root, p.to_string_lossy().replace('\\', "/"))
                }
            }
            _ => (*self.root).clone(),
        };

        let (tx, rx) = async_channel::bounded(1000);
        let total_file_count = Arc::new(AtomicUsize::new(0));
        let max_depth = depth.unwrap_or(0);

        let storage = self.clone();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            if let Err(err) = storage
                .iterative_walkdir(
                    &start_root,
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
                error!("Error during CIFS directory traversal: {err}");
                let _ = tx_clone
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::new(),
                        reason: format!("{err}"),
                    })
                    .await;
            }
        });

        Ok(WalkDirAsyncIterator::new(rx))
    }

    /// 迭代式目录遍历，使用工作窃取队列实现高效并发
    #[allow(clippy::too_many_arguments, clippy::ref_option)]
    async fn iterative_walkdir(
        &self, root_path: &str, tx: async_channel::Sender<StorageEntryMessage>, max_depth: usize,
        match_expressions: &Option<FilterExpression>, exclude_expressions: &Option<FilterExpression>,
        concurrency: usize, total_file_count: Arc<AtomicUsize>, packaged: bool, package_depth: usize,
    ) -> Result<()> {
        // task 类型: (dir_path: String, depth: usize, skip_filter: bool, package_remaining: Option<usize>)
        let contexts = create_worker_contexts(concurrency, (root_path.to_string(), 0usize, true, None::<usize>)).await;

        let match_expr = Arc::new(match_expressions.clone());
        let exclude_expr = Arc::new(exclude_expressions.clone());

        info!("Creating {} CIFS producer tasks", contexts.len());

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
                    |task| task.0.clone(),
                )
                .await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    /// 处理单个目录：查询条目、过滤、发送
    #[allow(clippy::too_many_arguments)]
    async fn process_dir(
        &self, producer_id: usize, dir_path: String, current_depth: usize,
        tx: &async_channel::Sender<StorageEntryMessage>,
        ctx: &crate::walk_scheduler::WorkerContext<(String, usize, bool, Option<usize>)>,
        match_expr: &Arc<Option<FilterExpression>>, exclude_expr: &Arc<Option<FilterExpression>>, max_depth: usize,
        total_file_count: &Arc<AtomicUsize>, skip_filter: bool, packaged: bool, package_depth: usize,
        package_remaining: Option<usize>,
    ) -> Result<()> {
        // 构建目录的 UNC 路径
        let dir_relative = if self.root.is_empty()
            || dir_path.is_empty()
            || dir_path == *self.root
            || dir_path.starts_with(&*self.root)
        {
            dir_path.clone()
        } else {
            format!("{}/{dir_path}", self.root)
        };

        let unc = if dir_relative.is_empty() {
            (*self.share_path).clone()
        } else {
            (*self.share_path).clone().with_path(&dir_relative.replace('/', "\\"))
        };

        // 打开目录
        let dir_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource = match self.client.create_file(&unc, &dir_args).await {
            Ok(r) => r,
            Err(e) => {
                error!(
                    "[Producer {}] Failed to open directory {}: {}",
                    producer_id, dir_path, e
                );
                let _ = tx
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(&dir_path),
                        reason: format!("Failed to open directory: {e}"),
                    })
                    .await;
                return Ok(());
            }
        };

        let Resource::Directory(directory) = resource else {
            warn!("[Producer {}] Path {} is not a directory", producer_id, dir_path);
            return Ok(());
        };

        let dir_arc = Arc::new(directory);

        // 使用 FileDirectoryInformation 查询目录内容（包含 size + timestamps）
        let mut stream = match smb::Directory::query::<FileDirectoryInformation>(&dir_arc, "*").await {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "[Producer {}] Failed to query directory {}: {}",
                    producer_id, dir_path, e
                );
                let _ = tx
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(&dir_path),
                        reason: format!("Failed to query directory: {e}"),
                    })
                    .await;
                return Ok(());
            }
        };

        // 流式处理每个目录条目
        while let Some(entry_result) = stream.next().await {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    error!(
                        "[Producer {}] Failed to read entry in directory {}: {:?}",
                        producer_id, dir_path, e
                    );
                    let _ = tx
                        .send(StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: PathBuf::from(&dir_path),
                            reason: format!("Failed to read directory entry: {e}"),
                        })
                        .await;
                    continue;
                }
            };

            // 将 BaseSizedString<u16> 转换为 String
            let file_name_str = entry.file_name.to_string();

            // 跳过 . 和 ..
            if file_name_str == "." || file_name_str == ".." {
                continue;
            }

            // 构建路径：去掉 root 前缀，保留相对于 root 的路径
            let full_path = Self::build_relative_path(&dir_path, &file_name_str);
            // 从 full_path 中去掉 root 前缀，得到纯相对路径
            let relative_path = if !self.root.is_empty() && full_path.starts_with(&*self.root) {
                let stripped = full_path.strip_prefix(&*self.root).unwrap_or(&full_path);
                stripped.trim_start_matches('/').to_string()
            } else {
                full_path.clone()
            };

            let extension = file_name_str.rsplit_once('.').map(|(_, ext)| ext.to_string());
            let file_name = &file_name_str;

            let is_dir = entry.file_attributes.directory();
            let is_symlink = entry.file_attributes.reparse_point();
            let is_readonly = entry.file_attributes.readonly();

            // 过滤逻辑
            let modified_epoch = Some(filetime_to_nanos(entry.last_write_time) / 1_000_000_000);

            let (skip_entry, continue_scan, need_submatch) = if skip_filter {
                should_skip(
                    match_expr.as_ref().as_ref(),
                    exclude_expr.as_ref().as_ref(),
                    Some(file_name),
                    Some(&relative_path),
                    Some(if is_symlink {
                        "symlink"
                    } else if is_dir {
                        "dir"
                    } else {
                        "file"
                    }),
                    modified_epoch,
                    Some(entry.end_of_file as u64),
                    extension.as_deref().or(Some("")),
                )
            } else {
                (false, true, false)
            };

            let entry_depth = current_depth + 1;
            let mut send_packaged = false;

            // package 深度追踪模式
            if let Some(remaining) = package_remaining {
                if !is_dir {
                    continue;
                }
                if remaining > 1 {
                    ctx.push_task((full_path.clone(), current_depth + 1, false, Some(remaining - 1)))
                        .await;
                    continue;
                }
                send_packaged = true;
            }

            if !send_packaged && skip_entry {
                if continue_scan && is_dir && (current_depth < max_depth || max_depth == 0) {
                    ctx.push_task((full_path.clone(), current_depth + 1, need_submatch, None))
                        .await;
                }
                continue;
            }

            // 构建 NASEntry
            let mode = smb_attributes_to_mode(is_dir, is_readonly);
            let storage_entry = EntryEnum::NAS(NASEntry {
                name: file_name_str.clone(),
                relative_path: PathBuf::from(&relative_path),
                is_dir,
                size: entry.end_of_file as u64,
                extension: extension.clone(),
                mtime: filetime_to_nanos(entry.last_write_time),
                atime: filetime_to_nanos(entry.last_access_time),
                ctime: filetime_to_nanos(entry.creation_time),
                mode,
                hard_links: None,
                is_symlink,
                file_handle: None,
                uid: None,
                gid: None,
                ino: None,
                acl: None,
                owner: None,
                owner_group: None,
                xattrs: None,
            });

            // packaged 模式
            if !send_packaged
                && packaged
                && is_dir
                && dir_matches_date_filter(match_expr.as_ref().as_ref(), storage_entry.get_name())
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
                    "[Producer {}] Packaged dir {} (depth: {})",
                    producer_id, relative_path, entry_depth
                );
                total_file_count.fetch_add(1, Ordering::Relaxed);
                if tx
                    .send(StorageEntryMessage::Packaged(Arc::new(storage_entry)))
                    .await
                    .is_err()
                {
                    error!("[Producer {}] Output channel closed, stopping", producer_id);
                    break;
                }
                continue;
            }

            // 如果是目录且未达到最大深度，加入任务队列
            if is_dir && (current_depth < max_depth || max_depth == 0) {
                ctx.push_task((full_path.clone(), current_depth + 1, need_submatch, None))
                    .await;
            }

            // 发送 entry
            if max_depth == 0 || entry_depth <= max_depth {
                total_file_count.fetch_add(1, Ordering::Relaxed);
                if tx
                    .send(StorageEntryMessage::Scanned(Arc::new(storage_entry)))
                    .await
                    .is_err()
                {
                    error!("[Producer {}] Output channel closed, stopping", producer_id);
                    break;
                }
            }
        }

        // dir_arc will be dropped, closing the directory handle
        Ok(())
    }

    // ── ACL 操作 ──────────────────────────────────────────────────────────────

    /// 读取路径的安全描述符（仅显式 ACE + 继承保护状态）
    ///
    /// 返回 smb-rs `SecurityDescriptor`，包含：
    /// - `dacl`: 仅非继承的显式 ACE
    /// - `control.dacl_protected`: 继承保护位（`true`=禁用继承）
    pub async fn get_security_descriptor(&self, relative_path: &Path) -> Result<SecurityDescriptor> {
        let unc = self.build_unc_path(relative_path);
        let access = FileAccessMask::new().with_read_control(true);
        let args = FileCreateArgs::make_open_existing(access);
        let resource = self
            .client
            .create_file(&unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to open for ACL read: {e}")))?;
        let handle = cifs_resource_handle(&resource);

        let info = AdditionalInfo::new().with_dacl_security_information(true);
        let sd = match handle.query_security_info(info).await {
            Ok(sd) => sd,
            Err(e) => {
                let _ = handle.close().await;
                return Err(StorageError::CifsError(format!("Failed to query security info: {e}")));
            }
        };
        let _ = handle.close().await;

        trace!(
            "get_security_descriptor {:?} raw DACL:\n    {}",
            relative_path,
            format_dacl_summary(&sd)
        );

        let filtered = filter_explicit_aces(sd);

        let explicit_count = filtered.dacl.as_ref().map_or(0, |d| d.ace.len());
        debug!(
            "get_security_descriptor {:?}: {} explicit ACE(s), protected={}",
            relative_path,
            explicit_count,
            filtered.control.dacl_protected()
        );

        Ok(filtered)
    }

    /// 将安全描述符（显式 ACE + 继承保护状态）写入目标路径
    ///
    /// 处理逻辑：
    /// 1. 读取目标当前 DACL（含继承 ACE）
    /// 2. 合并：源端显式 ACE + 目标端继承 ACE → 完整 DACL
    /// 3. 写入合并后的 DACL（`SMB2` `SET_INFO` 会替换整个 DACL，需保留继承 ACE）
    /// 4. 如果源/目标都无显式 ACE 且保护位相同 → 跳过
    pub async fn set_security_descriptor(&self, relative_path: &Path, source_sd: &SecurityDescriptor) -> Result<()> {
        let unc = self.build_unc_path(relative_path);
        let access = FileAccessMask::new().with_read_control(true).with_write_dacl(true);
        let args = FileCreateArgs::make_open_existing(access);
        let resource = self
            .client
            .create_file(&unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to open for ACL write: {e}")))?;
        let handle = cifs_resource_handle(&resource);

        // 读取目标当前状态，检查是否需要更新
        let query_info = AdditionalInfo::new().with_dacl_security_information(true);
        let target_sd = match handle.query_security_info(query_info).await {
            Ok(sd) => sd,
            Err(e) => {
                let _ = handle.close().await;
                return Err(StorageError::CifsError(format!(
                    "Failed to query target security info: {e}"
                )));
            }
        };

        let source_protected = source_sd.control.dacl_protected();
        let target_protected = target_sd.control.dacl_protected();
        let has_source_explicit = source_sd.dacl.as_ref().is_some_and(|d| !d.ace.is_empty());
        let has_target_explicit = target_sd
            .dacl
            .as_ref()
            .is_some_and(|d| d.ace.iter().any(|ace| !ace.ace_flags.inherited()));

        // 需要更新的情况：保护位不同、源端有显式 ACE 需要写入、或目标端有多余显式 ACE 需要清理
        let needs_update = source_protected != target_protected || has_source_explicit || has_target_explicit;

        trace!(
            "set_security_descriptor {:?}: target current DACL:\n    {}",
            relative_path,
            format_dacl_summary(&target_sd)
        );

        debug!(
            "set_security_descriptor {:?}: source_protected={}, target_protected={}, \
             has_source_explicit={}, has_target_explicit={}, needs_update={}",
            relative_path, source_protected, target_protected, has_source_explicit, has_target_explicit, needs_update
        );

        if needs_update {
            // 合并 DACL：源端显式 ACE + 目标端继承 ACE
            // Windows ACE 顺序：显式拒绝 → 显式允许 → 继承 ACE
            let mut merged_aces: Vec<ACE> = Vec::new();

            // 源端显式 ACE（已经过 filter_explicit_aces 过滤，全部非继承）
            if let Some(ref src_dacl) = source_sd.dacl {
                merged_aces.extend(src_dacl.ace.iter().cloned());
            }

            // 目标端继承 ACE（保留原有继承链）
            if let Some(ref tgt_dacl) = target_sd.dacl {
                merged_aces.extend(tgt_dacl.ace.iter().filter(|ace| ace.ace_flags.inherited()).cloned());
            }

            let source_explicit_count = source_sd.dacl.as_ref().map_or(0, |d| d.ace.len());
            let target_inherited_count = merged_aces.len() - source_explicit_count;

            let mut new_sd = source_sd.clone();
            new_sd.control = new_sd.control.with_dacl_protected(source_protected);
            new_sd.dacl = Some(ACL {
                acl_revision: source_sd
                    .dacl
                    .as_ref()
                    .or(target_sd.dacl.as_ref())
                    .map_or(AclRevision::Nt4, |d| d.acl_revision),
                ace: merged_aces,
            });

            trace!(
                "set_security_descriptor {:?}: writing merged DACL:\n    {}",
                relative_path,
                format_dacl_summary(&new_sd)
            );
            debug!(
                "set_security_descriptor {:?}: writing {} explicit + {} inherited = {} total ACE(s)",
                relative_path,
                source_explicit_count,
                target_inherited_count,
                source_explicit_count + target_inherited_count
            );

            let set_info = AdditionalInfo::new().with_dacl_security_information(true);
            if let Err(e) = handle.set_security_info(new_sd, set_info).await {
                let _ = handle.close().await;
                return Err(StorageError::CifsError(format!("Failed to set security info: {e}")));
            }
        } else {
            debug!(
                "set_security_descriptor {:?}: skipped (no changes needed)",
                relative_path
            );
        }

        let _ = handle.close().await;
        Ok(())
    }

    // ========================================================================
    // walkdir_2（DFS Driver + Reader 池）
    // ========================================================================

    /// 读取单个目录内容，返回排序后的文件和子目录列表
    ///
    /// 由 Reader Worker 调用，通过 SMB2 `FileDirectoryInformation` 查询目录内容。
    pub(crate) async fn read_dir_sorted(
        &self, dir_path: &str, handle: &crate::dir_tree::DirHandle, ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult> {
        use crate::dir_tree::{DirHandle, ReadResult, SubdirEntry};

        let cifs_dir_path = match handle {
            DirHandle::Cifs(p) => p.as_str(),
            _ => {
                return Err(StorageError::OperationError(
                    "DirHandle type mismatch: expected Cifs".into(),
                ));
            }
        };

        let mut files: Vec<Arc<EntryEnum>> = Vec::new();
        let mut subdirs: Vec<SubdirEntry> = Vec::new();
        let mut errors: Vec<String> = Vec::new();

        let unc = self.build_unc_path(Path::new(cifs_dir_path));

        // 打开目录
        let dir_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource = match self.client.create_file(&unc, &dir_args).await {
            Ok(r) => r,
            Err(e) => {
                return Ok(ReadResult {
                    dir_path: dir_path.to_string(),
                    files: Vec::new(),
                    subdirs: Vec::new(),
                    errors: vec![format!("Failed to open directory '{dir_path}': {e}")],
                });
            }
        };

        let Resource::Directory(directory) = resource else {
            return Ok(ReadResult {
                dir_path: dir_path.to_string(),
                files: Vec::new(),
                subdirs: Vec::new(),
                errors: vec![format!("Path '{dir_path}' is not a directory")],
            });
        };

        let dir_arc = Arc::new(directory);

        // 查询目录内容
        let mut stream = match smb::Directory::query::<FileDirectoryInformation>(&dir_arc, "*").await {
            Ok(s) => s,
            Err(e) => {
                return Ok(ReadResult {
                    dir_path: dir_path.to_string(),
                    files: Vec::new(),
                    subdirs: Vec::new(),
                    errors: vec![format!("Failed to query directory '{dir_path}': {e}")],
                });
            }
        };

        while let Some(entry_result) = stream.next().await {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    errors.push(format!("Failed to read entry in '{dir_path}': {e}"));
                    continue;
                }
            };

            let file_name_str = entry.file_name.to_string();
            if file_name_str == "." || file_name_str == ".." {
                continue;
            }

            let relative_path = Self::build_relative_path(dir_path, &file_name_str);
            let extension = file_name_str.rsplit_once('.').map(|(_, ext)| ext.to_string());

            let is_dir = entry.file_attributes.directory();
            let is_symlink = entry.file_attributes.reparse_point();
            let is_readonly = entry.file_attributes.readonly();

            // 过滤逻辑
            let modified_epoch = Some(filetime_to_nanos(entry.last_write_time) / 1_000_000_000);

            let (skip_entry, continue_scan, need_submatch) = if ctx.apply_filter {
                should_skip(
                    ctx.match_expr.as_ref().as_ref(),
                    ctx.exclude_expr.as_ref().as_ref(),
                    Some(&file_name_str),
                    Some(&relative_path),
                    Some(if is_symlink {
                        "symlink"
                    } else if is_dir {
                        "dir"
                    } else {
                        "file"
                    }),
                    modified_epoch,
                    Some(entry.end_of_file as u64),
                    extension.as_deref().or(Some("")),
                )
            } else {
                (false, true, false)
            };

            if skip_entry {
                if is_dir && continue_scan && (ctx.max_depth == 0 || ctx.current_depth + 1 < ctx.max_depth) {
                    let nas = build_smb_nas_entry(
                        file_name_str,
                        relative_path,
                        extension,
                        &entry,
                        true,
                        is_symlink,
                        is_readonly,
                    );
                    subdirs.push(SubdirEntry {
                        entry: Arc::new(EntryEnum::NAS(nas)),
                        visible: false,
                        need_filter: need_submatch,
                    });
                }
                continue;
            }

            let nas = build_smb_nas_entry(
                file_name_str,
                relative_path,
                extension,
                &entry,
                is_dir,
                is_symlink,
                is_readonly,
            );
            let entry_enum = Arc::new(EntryEnum::NAS(nas));

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
        &self, sub_path: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize,
    ) -> Result<crate::WalkDirAsyncIterator2> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        // 计算起始的相对路径（相对于 root）
        let start_path = match sub_path {
            Some(p) if !p.as_os_str().is_empty() => p.to_string_lossy().replace('\\', "/"),
            _ => String::new(),
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

        let root_handle = DirHandle::Cifs(start_path);
        // root_path 传空：CIFS 不需要拼接本地绝对路径，
        // BackendKind 由 root_handle.backend_kind() 自动推导为 Cifs
        let root_path = PathBuf::new();
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

/// 从 SMB `FileDirectoryInformation` 构建 `NASEntry`
fn build_smb_nas_entry(
    name: String, relative_path: String, extension: Option<String>, info: &FileDirectoryInformation, is_dir: bool,
    is_symlink: bool, is_readonly: bool,
) -> NASEntry {
    NASEntry {
        name,
        relative_path: PathBuf::from(relative_path),
        is_dir,
        size: info.end_of_file,
        extension,
        mtime: filetime_to_nanos(info.last_write_time),
        atime: filetime_to_nanos(info.last_access_time),
        ctime: filetime_to_nanos(info.creation_time),
        mode: smb_attributes_to_mode(is_dir, is_readonly),
        hard_links: None,
        is_symlink,
        file_handle: None,
        uid: None,
        gid: None,
        ino: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    }
}

/// 过滤掉继承的 ACE，只保留显式 ACE。
/// `control.dacl_protected` 继承保护位保持不变。
fn filter_explicit_aces(mut sd: SecurityDescriptor) -> SecurityDescriptor {
    if let Some(ref mut dacl) = sd.dacl {
        dacl.ace.retain(|ace| !ace.ace_flags.inherited());
    }
    sd
}

/// 格式化单个 ACE 的摘要信息（用于日志）
fn format_ace_summary(ace: &ACE) -> String {
    let ace_type = ace.ace_type();
    let flags = &ace.ace_flags;
    let inherited = if flags.inherited() { "I" } else { "E" }; // Inherited / Explicit
    let mut inherit_flags = String::new();
    if flags.object_inherit() {
        inherit_flags.push_str("OI|");
    }
    if flags.container_inherit() {
        inherit_flags.push_str("CI|");
    }
    if flags.inherit_only() {
        inherit_flags.push_str("IO|");
    }
    if flags.no_propagate_inherit() {
        inherit_flags.push_str("NP|");
    }
    if !inherit_flags.is_empty() {
        inherit_flags.truncate(inherit_flags.len() - 1); // 去掉末尾 |
    }

    // 提取 SID（AccessAllowed/AccessDenied 都有 sid 字段）
    let sid_str = match &ace.value {
        smb::AceValue::AccessAllowed(a) | smb::AceValue::AccessDenied(a) | smb::AceValue::SystemAudit(a) => {
            format!("{}", a.sid)
        }
        _ => format!("{ace_type:?}"),
    };

    format!("[{inherited}] {ace_type:?} {sid_str} ({inherit_flags})")
}

/// 格式化 DACL 摘要（用于日志）
fn format_dacl_summary(sd: &SecurityDescriptor) -> String {
    let protected = sd.control.dacl_protected();
    match &sd.dacl {
        Some(dacl) => {
            let explicit_count = dacl.ace.iter().filter(|a| !a.ace_flags.inherited()).count();
            let inherited_count = dacl.ace.iter().filter(|a| a.ace_flags.inherited()).count();
            let aces: Vec<String> = dacl.ace.iter().map(format_ace_summary).collect();
            format!(
                "protected={}, total={}, explicit={}, inherited={}\n    {}",
                protected,
                dacl.ace.len(),
                explicit_count,
                inherited_count,
                aces.join("\n    ")
            )
        }
        None => format!("protected={protected}, dacl=None"),
    }
}

/// 获取 `Resource` 枚举的底层 `ResourceHandle`
fn cifs_resource_handle(resource: &Resource) -> &ResourceHandle {
    match resource {
        Resource::File(f) => f.handle(),
        Resource::Directory(d) => d.handle(),
        Resource::Pipe(p) => p.handle(),
    }
}

/// 创建 CIFS 存储实例
pub async fn create_cifs_storage(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let storage = CifsStorage::new(url, block_size).await?;
    Ok(StorageEnum::CIFS(storage))
}

/// 创建 CIFS 目标存储实例，确保 prefix 目录存在
pub async fn create_cifs_storage_ensuring_dir(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let storage = CifsStorage::new(url, block_size).await?;

    // 如果有 root 前缀，确保目录存在
    if !storage.root.is_empty() {
        let root_path = storage.root.clone();
        storage.create_dir_all(Path::new(root_path.as_str())).await?;
    }

    Ok(StorageEnum::CIFS(storage))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_smb_url_basic() {
        let (host, port, share, sub_path, user, pass) = parse_smb_url("smb://admin:password@nas01/shared").unwrap();
        assert_eq!(host, "nas01");
        assert_eq!(port, 445);
        assert_eq!(share, "shared");
        assert_eq!(sub_path, "");
        assert_eq!(user, "admin");
        assert_eq!(pass, "password");
    }

    #[test]
    fn test_parse_smb_url_with_port_and_path() {
        let (host, port, share, sub_path, user, pass) =
            parse_smb_url("smb://user:P%40ss@server:4455/backup/data/2024").unwrap();
        assert_eq!(host, "server");
        assert_eq!(port, 4455);
        assert_eq!(share, "backup");
        assert_eq!(sub_path, "data/2024");
        assert_eq!(user, "user");
        assert_eq!(pass, "P@ss");
    }

    #[test]
    fn test_parse_smb_url_missing_credentials() {
        assert!(parse_smb_url("smb://server/share").is_err());
    }

    #[test]
    fn test_parse_smb_url_missing_share() {
        assert!(parse_smb_url("smb://user:pass@server").is_err());
    }

    #[test]
    fn test_filetime_conversion_roundtrip() {
        let original_ns: i64 = 1_700_000_000_000_000_000; // 约 2023-11-14
        let ft = nanos_to_filetime(original_ns);
        let back = filetime_to_nanos(ft);
        assert_eq!(original_ns, back);
    }

    #[test]
    fn test_smb_attributes_to_mode() {
        assert_eq!(smb_attributes_to_mode(true, false), 0o755);
        assert_eq!(smb_attributes_to_mode(true, true), 0o555);
        assert_eq!(smb_attributes_to_mode(false, false), 0o644);
        assert_eq!(smb_attributes_to_mode(false, true), 0o444);
    }
}
