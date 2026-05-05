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
    ACE, ACL, AclRevision, AdditionalInfo, Client, ClientConfig, CreateDisposition, CreateOptions, FileAccessMask,
    FileAttributes, FileBasicInformation, FileCreateArgs, FileIdExtdDirectoryInformation,
    FileIdFullDirectoryInformation, FileIdInformation, FileInternalInformation, FileStandardInformation, ReadAt,
    Resource, ResourceHandle, SecurityDescriptor, UncPath, WriteAt,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
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

/// 将 SMB `FileTime` (100ns since 1601-01-01) 转换为纳秒时间戳 (ns since Unix epoch)
#[allow(clippy::cast_possible_wrap)]
fn filetime_to_nanos(ft: FileTime) -> i64 {
    // FileTime Deref<Target=u64>，值是 100ns 间隔数
    crate::time_util::smb_filetime_to_nanos(*ft as i64)
}

/// 将纳秒时间戳 (ns since Unix epoch) 转换为 SMB `FileTime`
#[allow(clippy::cast_sign_loss)]
fn nanos_to_filetime(ns: i64) -> FileTime {
    FileTime::from(crate::time_util::nanos_to_smb_filetime(ns) as u64)
}

/// 把 128-bit SMB 文件 ID 编码为 `file_handle`。
///
/// 数据来源：
/// - 目录枚举走 `FileIdExtdDirectoryInformation`（MS-FSCC 2.4.23，class 0x3c）
/// - 单文件查询走 `FileIdInformation`（MS-FSCC 2.4.26）
///
/// 覆盖的后端：
/// - NTFS：低 64 位为 `IndexNumber`，高 64 位 0 → 整体非零稳定
/// - ReFS：完整 128-bit `FileId` 稳定，rename/move 不变
/// - Samba（ext4/xfs/btrfs）：低 64 位为 inode，高 64 位 0
/// - FAT/exFAT/不支持的后端：返回 0 → 退化到 None，让 `JoinStrategy` 走 Path 模式
///
/// 编码：16 字节大端 `Bytes`，可作为 fh3 模式下的 rename 检测键。
fn file_id_to_handle(file_id: u128) -> Option<Bytes> {
    if file_id == 0 {
        None
    } else {
        Some(Bytes::copy_from_slice(&file_id.to_be_bytes()))
    }
}

impl NASEntry {
    /// 从 SMB 通用字段构建 `NASEntry`。
    ///
    /// `lookup`/`get_metadata` 站点通过 `FileBasicInformation` + `FileStandardInformation`
    /// 提供字段，`process_dir` 站点通过 `FileIdFullDirectoryInformation` 提供。两类输入
    /// 解构成同样的标准参数（size、三组 `FileTime`、属性位、可选 `file_id`），因此共用同
    /// 一个构造器。
    ///
    /// `file_id` 在能拿到稳定 128-bit 文件 ID 时填入（NTFS `IndexNumber` 占低 64 位、
    /// `ReFS` 完整 128 位、Samba inode 占低 64 位）；拿不到（如 FAT 后端返回 0）时传 None，
    /// 会落到 Path 模式。
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    pub(crate) fn from_smb_info(
        name: String, relative_path: PathBuf, extension: Option<String>, size: u64, last_write_time: FileTime,
        last_access_time: FileTime, creation_time: FileTime, is_dir: bool, is_symlink: bool, is_readonly: bool,
        file_id: Option<u128>,
    ) -> Self {
        Self {
            name,
            relative_path,
            extension,
            is_dir,
            size,
            mtime: crate::time_util::smb_filetime_to_nanos(*last_write_time as i64),
            atime: crate::time_util::smb_filetime_to_nanos(*last_access_time as i64),
            ctime: crate::time_util::smb_filetime_to_nanos(*creation_time as i64),
            mode: smb_attributes_to_mode(is_dir, is_readonly),
            hard_links: None,
            is_symlink,
            file_handle: file_id.and_then(file_id_to_handle),
            uid: None,
            gid: None,
            ino: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        }
    }
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

/// 目录枚举使用的 SMB info class。
///
/// `FileIdExtdDirectoryInformation`（class 0x3c, 128-bit `file_id`）被新一代 Windows /
/// `ReFS` 支持，但 Samba 4.x 一律返回 `STATUS_INVALID_INFO_CLASS`，必须降级到
/// `FileIdFullDirectoryInformation`（64-bit `file_id`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DirInfoClass {
    Extd128,
    Full64,
}

/// 单文件 file-id 查询使用的 SMB info class。
///
/// `FileIdInformation`（128-bit）覆盖 NTFS / `ReFS`；Samba 不支持时回退到
/// `FileInternalInformation`（64-bit `IndexNumber`）。两者都不返回有效 ID 时记
/// `Unsupported`，让上层 `JoinStrategy` 自动走 Path 模式。
///
/// 首次成功查询时锁定 class；后续同一 storage 上的查询直接走该 class，避免在
/// Samba 后端反复尝试不被支持的 `FileIdInformation`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileIdClass {
    Id128,
    Internal64,
    Unsupported,
}

const STATUS_INVALID_INFO_CLASS: u32 = 0xC000_0003;
const STATUS_OBJECT_NAME_COLLISION: u32 = 0xC000_0035;

/// 判断 smb 错误是否为「对象已存在」（`STATUS_OBJECT_NAME_COLLISION`）。
/// `make_create_new` 命中已存在路径时返回该状态。
fn is_name_collision(err: &smb::Error) -> bool {
    matches!(
        err,
        smb::Error::ReceivedErrorMessage(s, _) | smb::Error::UnexpectedMessageStatus(s)
            if *s == STATUS_OBJECT_NAME_COLLISION
    )
}

/// 判断 smb 错误是否为「Info Class 不支持」（用于探测降级到旧 class）
///
/// 同时匹配 `ReceivedErrorMessage` 与 `UnexpectedMessageStatus`：Samba/Windows
/// 在不同协议路径下两种变体都可能出现（与 `is_not_found` 同样处理）。
fn is_invalid_info_class(err: &smb::Error) -> bool {
    matches!(
        err,
        smb::Error::ReceivedErrorMessage(s, _) | smb::Error::UnexpectedMessageStatus(s)
            if *s == STATUS_INVALID_INFO_CLASS
    )
}

/// 目录枚举条目的协议无关视图
///
/// 把 `FileIdExtdDirectoryInformation`（128-bit）和 `FileIdFullDirectoryInformation`
/// （64-bit）规整成同样字段，让后续处理逻辑无需感知具体 SMB info class。
struct RawDirEntry {
    file_name: String,
    file_attributes: FileAttributes,
    last_write_time: FileTime,
    last_access_time: FileTime,
    creation_time: FileTime,
    end_of_file: u64,
    /// 128-bit file ID，0 表示后端不支持（FAT 等）
    file_id: u128,
}

/// 将一种具体的 SMB 目录信息类规整为 `RawDirEntry`。
///
/// 实现给 `FileIdExtdDirectoryInformation` / `FileIdFullDirectoryInformation`，
/// 让 `collect_dir_entries` 用同一段循环消费两种类型，避免字段抄写漂移。
trait IntoRawDirEntry {
    fn into_raw(self) -> RawDirEntry;
}

impl IntoRawDirEntry for FileIdExtdDirectoryInformation {
    fn into_raw(self) -> RawDirEntry {
        RawDirEntry {
            file_name: self.file_name.to_string(),
            file_attributes: self.file_attributes,
            last_write_time: self.last_write_time,
            last_access_time: self.last_access_time,
            creation_time: self.creation_time,
            end_of_file: self.end_of_file,
            file_id: self.file_id,
        }
    }
}

impl IntoRawDirEntry for FileIdFullDirectoryInformation {
    fn into_raw(self) -> RawDirEntry {
        RawDirEntry {
            file_name: self.file_name.to_string(),
            file_attributes: self.file_attributes,
            last_write_time: self.last_write_time,
            last_access_time: self.last_access_time,
            creation_time: self.creation_time,
            end_of_file: self.end_of_file,
            // 64-bit ID 升宽到 u128（高 64 位 0），供 fh3 模式做 rename 比对。
            file_id: u128::from(self.file_id),
        }
    }
}

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
    /// 目录枚举使用的 SMB info class，首次扫描时通过 `OnceCell::get_or_init`
    /// 探测一次后缓存（参见 [`DirInfoClass`]）。
    dir_info_class: Arc<OnceCell<DirInfoClass>>,
    /// 单文件 file-id 查询使用的 SMB info class（参见 [`FileIdClass`]）。
    ///
    /// 拆成两个字段而非 `Mutex<Option<_>>`：
    /// - `file_id_class`：`OnceCell` 提供 lock-free 读，stat-heavy 热路径每次
    ///   查询零锁开销。
    /// - `file_id_probe_lock`：仅在 cold-start 探测时持锁，串行化并发首调，
    ///   避免每个 first-caller 各自跑一遍 `Id128 → Internal64` 双探测。
    file_id_class: Arc<OnceCell<FileIdClass>>,
    file_id_probe_lock: Arc<Mutex<()>>,
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
        let storage = Self::connect_only(url, block_size).await?;
        storage.check_connectivity().await?;
        info!(
            "Successfully connected to SMB share, root sub-path verified: '{}'",
            storage.root
        );
        Ok(storage)
    }

    /// 连接 share 并构造实例，不做 root sub-path 连通性检查
    ///
    /// 供 dest 路径在 root 可能缺失时使用：构造完成后再调 `ensure_root_exists`。
    async fn connect_only(url: &str, block_size: Option<u64>) -> Result<Self> {
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

        Ok(CifsStorage {
            client: Arc::new(client),
            share_path: Arc::new(share_path),
            root: Arc::new(sub_path),
            config: StorageConfig {
                block_size: effective_block_size,
            },
            dir_info_class: Arc::new(OnceCell::new()),
            file_id_class: Arc::new(OnceCell::new()),
            file_id_probe_lock: Arc::new(Mutex::new(())),
        })
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

    /// 验证存储连通性（只读）
    ///
    /// 仅尝试打开 root（含 sub-path），打开后立即关闭句柄。失败即报错，不创建任何目录。
    /// 目标端首次写入需要自动建立 root sub-path 的场景，由
    /// `create_cifs_storage_ensuring_dir` → `ensure_root_exists` 处理。
    pub async fn check_connectivity(&self) -> Result<()> {
        let root_unc = self.build_unc_path(Path::new(""));
        let args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource = self.client.create_file(&root_unc, &args).await.map_err(|e| {
            StorageError::CifsError(format!("Connectivity check failed: {e}"))
        })?;
        if let Resource::Directory(d) = resource {
            let _ = d.close().await;
        }
        Ok(())
    }

    /// 确保 root sub-path 存在（缺失则按层创建）
    ///
    /// 仅供目标端 dest 路径在首次写入前调用。share 根为空（无 sub-path）时直接返回。
    ///
    /// 注意：直接对 share 根逐层 mkdir，避开 `create_dir_all` 在 `build_unc_path`
    /// 中重复拼接 `self.root` 的问题（否则会创建 `<root>/<root>` 嵌套目录）。
    pub async fn ensure_root_exists(&self) -> Result<()> {
        if self.root.is_empty() {
            return Ok(());
        }
        // 快速路径：root 已存在则直接返回，省掉对每个 component 的两次 RT。
        let root_unc = self.build_unc_path(Path::new(""));
        let open_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        if let Ok(resource) = self.client.create_file(&root_unc, &open_args).await {
            if let Resource::Directory(d) = resource {
                let _ = d.close().await;
            }
            return Ok(());
        }

        debug!("CIFS root sub-path missing, creating root path: {}", self.root);
        let path_norm = self.root.replace('\\', "/");
        let components: Vec<&str> = path_norm.split('/').filter(|s| !s.is_empty()).collect();
        let mut accumulated = String::new();
        for component in components {
            if accumulated.is_empty() {
                accumulated.push_str(component);
            } else {
                accumulated = format!("{accumulated}\\{component}");
            }
            // 直接对 share 根逐层 mkdir，避开 build_unc_path 的 root 前缀拼接（否则
            // 会得到 <root>/<root> 嵌套）。
            let unc = (*self.share_path).clone().with_path(&accumulated);
            self.mkdir_or_open(&unc, &accumulated).await?;
        }
        Ok(())
    }

    /// 在指定 UNC 上创建目录，已存在则视为成功。`accumulated` 仅用于错误描述
    /// （而 `unc` 才是 SMB 端的标准路径；调用方负责传入已加好 `\` 分隔的 UNC）。
    ///
    /// 服务端对已存在路径返回 `STATUS_OBJECT_NAME_COLLISION` (`0xC000_0035`)：
    /// 该状态本身就是"目录已存在"的肯定证据，省掉一次 open-existing 的 RT。
    /// 其他错误才回退到 open-existing 验证（覆盖一些罕见的服务器实现差异）。
    async fn mkdir_or_open(&self, unc: &UncPath, accumulated: &str) -> Result<()> {
        let mk_args = FileCreateArgs::make_create_new(
            FileAttributes::new().with_directory(true),
            CreateOptions::new().with_directory_file(true),
        );
        match self.client.create_file(unc, &mk_args).await {
            Ok(resource) => {
                close_resource(resource).await;
                Ok(())
            }
            Err(ref e) if is_name_collision(e) => Ok(()),
            Err(create_err) => {
                let open_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
                let resource = self.client.create_file(unc, &open_args).await.map_err(|open_err| {
                    StorageError::CifsError(format!(
                        "mkdir '{accumulated}' failed: {create_err}; open also failed: {open_err}"
                    ))
                })?;
                close_resource(resource).await;
                Ok(())
            }
        }
    }

    /// 探测服务器对 `FileIdExtdDirectoryInformation` 的支持，结果永久缓存。
    ///
    /// `OnceCell::get_or_init` 保证并发首次调用只发一次请求：首个 worker 探测,
    /// 其余等待结果。`STATUS_INVALID_INFO_CLASS` 或任何探测失败都降级到
    /// `Full64`，后续不再重试。
    async fn probe_dir_info_class(&self) -> DirInfoClass {
        *self.dir_info_class.get_or_init(|| async { self.run_probe().await }).await
    }

    /// 实际执行一次探测请求；独立函数以便 `get_or_init` 闭包调用。
    async fn run_probe(&self) -> DirInfoClass {
        let root_unc = self.build_unc_path(Path::new(""));
        let open_args = FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let resource = match self.client.create_file(&root_unc, &open_args).await {
            Ok(r) => r,
            Err(e) => {
                warn!("CIFS probe: failed to open root for info-class probe: {e}; defaulting to Full64");
                return DirInfoClass::Full64;
            }
        };
        let Resource::Directory(d) = resource else {
            warn!("CIFS probe: root is not a directory; defaulting to Full64");
            return DirInfoClass::Full64;
        };
        let dir_arc = Arc::new(d);

        let class = match smb::Directory::query::<FileIdExtdDirectoryInformation>(&dir_arc, "*").await {
            Ok(mut s) => match s.next().await {
                None | Some(Ok(_)) => DirInfoClass::Extd128,
                Some(Err(ref e)) if is_invalid_info_class(e) => DirInfoClass::Full64,
                Some(Err(e)) => {
                    warn!("CIFS probe: unexpected stream error: {e}; defaulting to Full64");
                    DirInfoClass::Full64
                }
            },
            Err(ref e) if is_invalid_info_class(e) => DirInfoClass::Full64,
            Err(e) => {
                warn!("CIFS probe: query failed: {e}; defaulting to Full64");
                DirInfoClass::Full64
            }
        };

        if let Ok(d) = Arc::try_unwrap(dir_arc) {
            let _ = d.close().await;
        }

        debug!("CIFS dir info-class probe: {class:?}");
        class
    }

    /// 单文件 file-id 查询（128-bit），首次成功后锁定 info class 避免反复探测。
    ///
    /// 热路径（已锁定）：lock-free `OnceCell::get()` + 单次 `query_with_class`。
    /// 冷路径：取 `file_id_probe_lock`，double-check 后跑 `probe_file_id` 并 `set`。
    async fn query_file_id(&self, handle: &ResourceHandle) -> Option<u128> {
        if let Some(&class) = self.file_id_class.get() {
            return query_with_class(handle, class).await;
        }
        let _guard = self.file_id_probe_lock.lock().await;
        if let Some(&class) = self.file_id_class.get() {
            return query_with_class(handle, class).await;
        }
        let (class, value) = probe_file_id(handle).await;
        let _ = self.file_id_class.set(class);
        value
    }

    /// 用已确定的 info class 收集目录全部条目，规整为 `RawDirEntry`。
    ///
    /// 调用方负责：打开目录拿到 `dir_arc`、之后关闭句柄。本函数不持有所有权。
    ///
    /// 错误语义：
    /// - `Err(smb::Error)`：query 起始即失败 → 整个目录视为不可读，调用方按
    ///   `ErrorEvent::Scan` 上报。
    /// - `Ok((entries, errs))`：流中逐条解析，单条 entry 失败不中断，错误描述
    ///   累积到 `errs`；调用方按各自的传播方式转发，保留部分成功结果。
    async fn collect_dir_entries(
        &self, dir_arc: &Arc<smb::Directory>,
    ) -> std::result::Result<(Vec<RawDirEntry>, Vec<String>), smb::Error> {
        match self.probe_dir_info_class().await {
            DirInfoClass::Extd128 => drain_dir::<FileIdExtdDirectoryInformation>(dir_arc).await,
            DirInfoClass::Full64 => drain_dir::<FileIdFullDirectoryInformation>(dir_arc).await,
        }
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

    /// 判定 `smb::Error` 是否表示"对象不存在"语义。
    ///
    /// 对应 NT 状态：
    /// - `STATUS_OBJECT_NAME_NOT_FOUND` (`0xC000_0034`)
    /// - `STATUS_OBJECT_PATH_NOT_FOUND` (`0xC000_003A`)
    fn is_not_found(err: &smb::Error) -> bool {
        const STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
        const STATUS_OBJECT_PATH_NOT_FOUND: u32 = 0xC000_003A;
        match err {
            smb::Error::ReceivedErrorMessage(code, _) | smb::Error::UnexpectedMessageStatus(code) => {
                *code == STATUS_OBJECT_NAME_NOT_FOUND || *code == STATUS_OBJECT_PATH_NOT_FOUND
            }
            _ => false,
        }
    }

    /// 构造覆盖写入语义的 FileCreateArgs（截断已存在或新建）。
    ///
    /// 不能直接使用 `FileCreateArgs::make_overwrite`：其内部固定请求 `generic_all`，
    /// 而 `GENERIC_ALL` 包含 `WRITE_DAC` / `WRITE_OWNER`，Samba 默认不会授予文件 owner，
    /// 导致覆盖已存在文件时返回 `STATUS_ACCESS_DENIED`。
    ///
    /// 截断由 `CreateDisposition::OverwriteIf` 在协议层完成（服务端 reset EOF），
    /// 与 access mask 无关。这里仅请求覆盖实际所需的最小权限。
    fn make_overwrite_args() -> FileCreateArgs {
        FileCreateArgs {
            disposition: CreateDisposition::OverwriteIf,
            attributes: FileAttributes::default(),
            options: CreateOptions::default(),
            desired_access: FileAccessMask::new()
                .with_file_read_data(true)
                .with_file_write_data(true)
                .with_file_read_attributes(true)
                .with_file_write_attributes(true)
                .with_delete(true)
                .with_synchronize(true),
        }
    }

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
        let args = Self::make_overwrite_args();

        let resource = self
            .client
            .create_file(&unc, &args)
            .await
            .map_err(|e| StorageError::CifsError(format!("Failed to create/open file {}: {e}", path.display())))?;

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                path.display()
            )));
        };

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
        let args = Self::make_overwrite_args();

        let resource = self.client.create_file(&unc, &args).await.map_err(|e| {
            StorageError::CifsError(format!("Failed to create/open file {}: {e}", relative_path.display()))
        })?;

        let Resource::File(file) = resource else {
            return Err(StorageError::CifsError(format!(
                "Path {} is not a file",
                relative_path.display()
            )));
        };

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
                current.push_str(component);
            } else {
                current = format!("{current}/{component}");
            }
            let unc = self.build_unc_path(Path::new(&current));
            self.mkdir_or_open(&unc, &current).await?;
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
            // 把 SMB STATUS_OBJECT_PATH_NOT_FOUND / STATUS_OBJECT_NAME_NOT_FOUND
            // 标准化为 StorageError::FileNotFound，便于 orchestrator 的"幂等删除"路径
            // 静默吞掉重复删除（例如父目录已被 delete_dir_all 递归删除）。
            if Self::is_not_found(&e) {
                return StorageError::FileNotFound(relative_path.display().to_string());
            }
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
    ///
    /// 语义 = `rm -rf relative_path`：
    /// - `relative_path = Some(p)`：删除 `p` 下所有内容，**并删除 `p` 本身**
    /// - `relative_path = None`：仅清空 share root 下的内容，不删除 share 根本身
    ///
    /// 进度事件按删除顺序（深度倒序）通过迭代器返回。
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
            // 删除根目录本身（walkdir 只返回其内容，不含根目录自身）
            if let Some(root) = sub_path {
                if let Err(e) = storage.delete_dir(&root).await {
                    error!("Failed to delete root dir {:?}: {:?}", root, e);
                } else {
                    let _ = tx
                        .send(DeleteEvent {
                            relative_path: root,
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
        // FileRenameInformation.file_name 必须是相对于 share 根的路径（反斜杠分隔），
        // 不能是完整 UNC（`\\host\share\...`）。否则 SMB 服务端返回 `Object Name Invalid (0xc0000033)`。
        let to_path_str = to_unc.path().unwrap_or("").to_string();

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

        let handle = match &resource {
            Resource::File(f) => &**f,
            Resource::Directory(d) => &**d,
            Resource::Pipe(_) => {
                close_resource(resource).await;
                return Err(StorageError::CifsError(format!(
                    "Unsupported resource type for {}",
                    relative_path.display()
                )));
            }
        };
        let is_dir = matches!(&resource, Resource::Directory(_));

        // 三次 query 互相独立且都打在同一个 handle 上，并发触发省 2 个 RT。
        let query_result = tokio::try_join!(
            async {
                handle
                    .query_info::<FileBasicInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query basic info: {e}")))
            },
            async {
                handle
                    .query_info::<FileStandardInformation>()
                    .await
                    .map_err(|e| StorageError::CifsError(format!("Failed to query standard info: {e}")))
            },
            async { Ok::<_, StorageError>(self.query_file_id(handle).await) },
        );
        let (basic_info, standard_info, file_id_u128) = match query_result {
            Ok(t) => t,
            Err(e) => {
                close_resource(resource).await;
                return Err(e);
            }
        };
        close_resource(resource).await;

        Ok(build_nas_entry(relative_path, &basic_info, &standard_info, is_dir, file_id_u128))
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
        close_resource(read_resource).await;

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

        // 通过协商出的 info class 收集条目（128-bit Extd 优先，旧服务器降级到 64-bit Full），
        // 统一以 RawDirEntry 视图后续处理；所有路径都得到 file_id 用作 fh3 rename 比对键。
        // query 起始失败 → 上报 ErrorEvent::Scan 并中止本目录；
        // 单条 entry 失败 → 逐条上报 ErrorEvent::Scan 后继续处理剩余 entry，避免单点
        // 失败丢弃整个目录（造成 silent count loss）。
        let (entries, entry_errors) = match self.collect_dir_entries(&dir_arc).await {
            Ok(v) => v,
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
        for reason in entry_errors {
            error!(
                "[Producer {}] readdir entry error in {}: {}",
                producer_id, dir_path, reason
            );
            let _ = tx
                .send(StorageEntryMessage::Error {
                    event: ErrorEvent::Scan,
                    path: PathBuf::from(&dir_path),
                    reason,
                })
                .await;
        }

        for entry in entries {
            let file_name_str = entry.file_name;

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
            let modified_epoch = Some(crate::time_util::nanos_to_secs(filetime_to_nanos(entry.last_write_time)));

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
                    Some(entry.end_of_file),
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
            let storage_entry = EntryEnum::NAS(NASEntry::from_smb_info(
                file_name_str.clone(),
                PathBuf::from(&relative_path),
                extension.clone(),
                entry.end_of_file,
                entry.last_write_time,
                entry.last_access_time,
                entry.creation_time,
                is_dir,
                is_symlink,
                is_readonly,
                Some(entry.file_id),
            ));

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
    /// 由 Reader Worker 调用，通过 SMB2 `FileIdExtdDirectoryInformation` 查询目录内容。
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

        // 通过协商出的 info class 收集条目（128-bit Extd 优先，旧服务器降级到 64-bit Full），
        // 后续逻辑共享同一份 RawDirEntry 视图。
        // query 起始失败 → 整个目录返回单条错误；
        // 单条 entry 失败 → push 到 errors 后继续处理剩余 entry。
        let (entries, entry_errors) = match self.collect_dir_entries(&dir_arc).await {
            Ok(v) => v,
            Err(e) => {
                return Ok(ReadResult {
                    dir_path: dir_path.to_string(),
                    files: Vec::new(),
                    subdirs: Vec::new(),
                    errors: vec![format!("Failed to query directory '{dir_path}': {e}")],
                });
            }
        };
        errors.extend(entry_errors);

        for entry in entries {
            let file_name_str = entry.file_name;
            if file_name_str == "." || file_name_str == ".." {
                continue;
            }

            let relative_path = Self::build_relative_path(dir_path, &file_name_str);
            let extension = file_name_str.rsplit_once('.').map(|(_, ext)| ext.to_string());

            let is_dir = entry.file_attributes.directory();
            let is_symlink = entry.file_attributes.reparse_point();
            let is_readonly = entry.file_attributes.readonly();

            // 过滤逻辑
            let modified_epoch = Some(crate::time_util::nanos_to_secs(filetime_to_nanos(entry.last_write_time)));

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
                    Some(entry.end_of_file),
                    extension.as_deref().or(Some("")),
                )
            } else {
                (false, true, false)
            };

            if skip_entry {
                if is_dir && continue_scan && (ctx.max_depth == 0 || ctx.current_depth + 1 < ctx.max_depth) {
                    let nas = NASEntry::from_smb_info(
                        file_name_str,
                        PathBuf::from(relative_path),
                        extension,
                        entry.end_of_file,
                        entry.last_write_time,
                        entry.last_access_time,
                        entry.creation_time,
                        true,
                        is_symlink,
                        is_readonly,
                        Some(entry.file_id),
                    );
                    subdirs.push(SubdirEntry {
                        entry: Arc::new(EntryEnum::NAS(nas)),
                        visible: false,
                        need_filter: need_submatch,
                    });
                }
                continue;
            }

            let nas = NASEntry::from_smb_info(
                file_name_str,
                PathBuf::from(relative_path),
                extension,
                entry.end_of_file,
                entry.last_write_time,
                entry.last_access_time,
                entry.creation_time,
                is_dir,
                is_symlink,
                is_readonly,
                Some(entry.file_id),
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

/// 已知 class 时直接 query，不再做兜底探测。
async fn query_with_class(handle: &ResourceHandle, class: FileIdClass) -> Option<u128> {
    match class {
        FileIdClass::Id128 => handle
            .query_info::<FileIdInformation>()
            .await
            .ok()
            .and_then(|i| (i.file_id != 0).then_some(i.file_id)),
        FileIdClass::Internal64 => handle
            .query_info::<FileInternalInformation>()
            .await
            .ok()
            .and_then(|i| (i.index_number != 0).then(|| u128::from(i.index_number))),
        FileIdClass::Unsupported => None,
    }
}

/// 首次调用时按 `Id128 → Internal64 → Unsupported` 探测，返回 (锁定的 class, 当次结果)。
async fn probe_file_id(handle: &ResourceHandle) -> (FileIdClass, Option<u128>) {
    if let Ok(info) = handle.query_info::<FileIdInformation>().await
        && info.file_id != 0
    {
        return (FileIdClass::Id128, Some(info.file_id));
    }
    if let Ok(info) = handle.query_info::<FileInternalInformation>().await
        && info.index_number != 0
    {
        return (FileIdClass::Internal64, Some(u128::from(info.index_number)));
    }
    (FileIdClass::Unsupported, None)
}

/// 用 SMB query 出来的 basic/standard info 组装一个 `EntryEnum::NAS`。
fn build_nas_entry(
    relative_path: &Path, basic: &FileBasicInformation, standard: &FileStandardInformation, is_dir: bool,
    file_id: Option<u128>,
) -> EntryEnum {
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

    EntryEnum::NAS(NASEntry::from_smb_info(
        filename,
        relative_path.to_path_buf(),
        extension,
        standard.end_of_file,
        basic.last_write_time,
        basic.last_access_time,
        basic.creation_time,
        is_dir,
        basic.file_attributes.reparse_point(),
        basic.file_attributes.readonly(),
        file_id,
    ))
}

/// 异步关闭一个 `Resource`，吞掉 close 错误（句柄反正即将被丢弃）。
async fn close_resource(resource: Resource) {
    match resource {
        Resource::File(f) => {
            let _ = f.close().await;
        }
        Resource::Directory(d) => {
            let _ = d.close().await;
        }
        Resource::Pipe(_) => {}
    }
}

/// 流式消费一种具体 SMB 目录信息类，规整为 `(Vec<RawDirEntry>, Vec<String>)`。
///
/// 单条 entry 解析失败不中断流；query 起始失败通过 `?` 返回外层 `Err`。
async fn drain_dir<T>(dir_arc: &Arc<smb::Directory>) -> std::result::Result<(Vec<RawDirEntry>, Vec<String>), smb::Error>
where
    T: smb::QueryDirectoryInfoValue + IntoRawDirEntry + Unpin + Send + for<'b> binrw::BinWrite<Args<'b> = ()>,
{
    let mut stream = smb::Directory::query::<T>(dir_arc, "*").await?;
    let mut out = Vec::new();
    let mut errs = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(entry) => out.push(entry.into_raw()),
            Err(e) => errs.push(format!("readdir entry error: {e}")),
        }
    }
    Ok((out, errs))
}

/// 创建 CIFS 存储实例
pub async fn create_cifs_storage(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let storage = CifsStorage::new(url, block_size).await?;
    Ok(StorageEnum::CIFS(storage))
}

/// 创建 CIFS 目标存储实例，root sub-path 不存在时自动创建
///
/// 仅在 dest 路径使用。区别于 `create_cifs_storage`：跳过只读的连通性检查，
/// 改由 `ensure_root_exists` 缺失时按层 mkdir（直接对 share 根，避开
/// `build_unc_path` 的 root 前缀双重拼接）。
pub async fn create_cifs_storage_ensuring_dir(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let storage = CifsStorage::connect_only(url, block_size).await?;
    storage.ensure_root_exists().await?;
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
