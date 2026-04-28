use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use soft_canonicalize::soft_canonicalize;
pub mod async_receiver;
pub use async_receiver::AsyncReceiver;

#[cfg(windows)]
pub mod acl;
pub mod checksum;
pub mod cifs;
pub mod dir_tree;
pub mod error;
pub mod filter;
pub mod local;
pub mod nfs;
pub mod qos;
pub mod s3;
pub mod storage_enum;
pub mod tar_pack;
pub mod third_party;
pub mod time_util;
pub(crate) mod walk_scheduler;

pub use checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
pub use cifs::CifsStorage;
pub use filter::{
    FilterExpression, FilterFieldDef, FilterOperatorDef, dir_matches_date_filter, get_filter_field_definitions,
};
pub use local::LocalStorage;
pub use nfs::{NFSStorage, create_nfs_storage_ensuring_dir};
pub use qos::QosManager;
pub use s3::{MultipartUpload, S3BucketInfo, S3CompletedPart, S3Storage};
pub use storage_enum::{StorageEnum, StorageType, create_storage, create_storage_for_dest, detect_storage_type};
pub use tar_pack::calculate_tar_size;

/// 删除事件，表示一个文件或目录已被删除
#[derive(Debug, Clone)]
pub struct DeleteEvent {
    pub relative_path: PathBuf,
    pub is_dir: bool,
}

/// 删除进度迭代器，通过 channel 接收删除事件
pub type DeleteDirIterator = AsyncReceiver<DeleteEvent>;

pub type Result<T> = std::result::Result<T, error::StorageError>;

/// `walkdir_2` 输出的异步迭代器类型
pub type WalkDirAsyncIterator2 = AsyncReceiver<dir_tree::NdxEvent>;

/// 常量定义
pub const KB: u64 = 1024;
pub const MB: u64 = 1024 * KB;

/// 规范化路径：NFS/S3 路径原样返回，本地路径转换为绝对路径
pub fn canonicalize_path(path: &str) -> std::io::Result<String> {
    match detect_storage_type(path) {
        StorageType::Nfs | StorageType::S3 | StorageType::Cifs => Ok(path.to_string()),
        StorageType::Local => {
            let abs = soft_canonicalize(path)?;
            Ok(abs.to_string_lossy().to_string())
        }
    }
}

/// 将纳秒时间戳转换为YYYY-MM-DD HHMMSS格式的字符串
pub fn datetime_to_string(time: i64) -> String {
    // 创建一个SystemTime对象，然后转换为chrono::DateTime
    let system_time = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(time as u64);
    let datetime: chrono::DateTime<chrono::Utc> = system_time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
}

/// 数据块结构体 - 用于表示文件传输过程中的数据片段
///
/// 该结构体封装了文件数据的一个数据块及其在原始文件中的偏移量，
/// 主要用于在文件复制过程中进行数据分块传输和处理。
#[derive(Debug, Serialize, Deserialize)]
pub struct DataChunk {
    /// 文件中的偏移量，表示该数据块在原文件中的起始位置
    pub offset: u64,
    /// 数据块的实际内容
    pub data: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryEnum {
    NAS(NASEntry),
    S3(S3Entry),
}

impl EntryEnum {
    pub fn get_size(&self) -> u64 {
        match self {
            EntryEnum::NAS(entry) => entry.size,
            EntryEnum::S3(entry) => entry.size,
        }
    }

    pub fn get_relative_path(&self) -> &Path {
        match self {
            EntryEnum::NAS(entry) => entry.relative_path.as_path(),
            EntryEnum::S3(entry) => Path::new(&entry.relative_path),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            EntryEnum::NAS(entry) => &entry.name,
            EntryEnum::S3(entry) => &entry.name,
        }
    }

    pub fn get_is_dir(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => entry.is_dir,
            EntryEnum::S3(entry) => entry.is_dir,
        }
    }

    pub fn get_is_symlink(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => entry.is_symlink,
            EntryEnum::S3(_) => false,
        }
    }

    pub fn get_is_regular_file(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => !entry.is_dir && !entry.is_symlink,
            EntryEnum::S3(entry) => !entry.is_dir,
        }
    }

    pub fn get_mtime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.mtime,
            EntryEnum::S3(entry) => entry.mtime,
        }
    }

    pub fn get_atime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.atime,
            EntryEnum::S3(entry) => entry.mtime,
        }
    }

    pub fn get_ctime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.ctime,
            EntryEnum::S3(entry) => entry.mtime,
        }
    }

    pub fn get_mode(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => Some(entry.mode),
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_extension(&self) -> Option<&str> {
        match self {
            EntryEnum::NAS(entry) => entry.extension.as_deref(),
            EntryEnum::S3(entry) => entry.extension.as_deref(),
        }
    }

    pub fn get_hard_links(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.hard_links,
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_uid(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.uid,
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_gid(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.gid,
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_ino(&self) -> Option<u64> {
        match self {
            EntryEnum::NAS(entry) => entry.ino,
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_file_handle(&self) -> Option<&Bytes> {
        match self {
            EntryEnum::NAS(entry) => entry.file_handle.as_ref(),
            EntryEnum::S3(_) => None,
        }
    }

    pub fn get_version_id(&self) -> Option<&str> {
        match self {
            EntryEnum::NAS(_) => None,
            EntryEnum::S3(entry) => entry.version_id.as_deref(),
        }
    }

    pub fn get_tags(&self) -> Option<&Vec<Tag>> {
        match self {
            EntryEnum::NAS(_) => None,
            EntryEnum::S3(entry) => entry.tags.as_ref(),
        }
    }

    pub fn get_version_count(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(_) => None,
            EntryEnum::S3(entry) => entry.version_count,
        }
    }

    pub fn get_is_latest(&self) -> bool {
        match self {
            EntryEnum::NAS(_) => true,
            EntryEnum::S3(entry) => entry.is_latest,
        }
    }

    pub fn get_is_delete_marker(&self) -> bool {
        match self {
            EntryEnum::NAS(_) => false,
            EntryEnum::S3(entry) => entry.is_delete_marker,
        }
    }

    pub fn set_version_count(&mut self, count: u32) {
        if let EntryEnum::S3(entry) = self {
            entry.version_count = Some(count);
        }
    }
}

/// 失败事件类型，用于区分不同场景下的错误
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorEvent {
    /// 扫描阶段失败（元数据获取、目录遍历、路径处理等）
    Scan,
    /// 文件内容复制失败（含 mkdir、目录结构建立）
    Copy,
    /// ACL/权限复制失败
    CopyAcl,
    /// xattr 复制失败
    CopyXattr,
    /// 删除操作失败
    Delete,
    /// 重命名操作失败
    Rename,
    /// 软链接操作失败（创建/删除 symlink）
    SymlinkOp,
    /// 打包失败
    Pack,
}

impl fmt::Display for ErrorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorEvent::Scan => write!(f, "scan"),
            ErrorEvent::Copy => write!(f, "copy"),
            ErrorEvent::CopyAcl => write!(f, "copy_acl"),
            ErrorEvent::CopyXattr => write!(f, "copy_xattr"),
            ErrorEvent::Delete => write!(f, "delete"),
            ErrorEvent::Rename => write!(f, "rename"),
            ErrorEvent::SymlinkOp => write!(f, "symlink_op"),
            ErrorEvent::Pack => write!(f, "pack"),
        }
    }
}

/// 变更的维度：用于区分内容变更、元数据变更、或两者同时变更
///
/// - `DataOnly`：size 或 mtime 不同（内容变了），属性 mode/uid/gid 未变 → 需 copy_file + set_metadata
/// - `MetadataOnly`：size 和 mtime 相同，但 mode/uid/gid 至少一项不同（chmod/chown）→ 只需 set_metadata，跳过 copy_file
/// - `Both`：内容和属性都变了 → 需 copy_file + set_metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeKind {
    DataOnly,
    MetadataOnly,
    Both,
}

impl ChangeKind {
    /// 对应数据库 state 列的取值，与 `db/src/traits.rs` 中 `Changed` 分支保持一致
    #[must_use]
    pub fn as_state_str(&self) -> &'static str {
        match self {
            ChangeKind::DataOnly => "data_changed",
            ChangeKind::MetadataOnly => "metadata_changed",
            ChangeKind::Both => "both_changed",
        }
    }

    /// 根据两个 entry 的属性差异推断变更类型；无变更时返回 `None`。
    #[must_use]
    pub fn from_entry_diff(from: &EntryEnum, to: &EntryEnum) -> Option<Self> {
        let data_changed = from.get_size() != to.get_size() || from.get_mtime() != to.get_mtime();
        let meta_changed =
            from.get_mode() != to.get_mode() || from.get_uid() != to.get_uid() || from.get_gid() != to.get_gid();
        match (data_changed, meta_changed) {
            (true, true) => Some(Self::Both),
            (true, false) => Some(Self::DataOnly),
            (false, true) => Some(Self::MetadataOnly),
            (false, false) => None,
        }
    }
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_state_str())
    }
}

/// 文件操作消息枚举，用于在扫描/同步过程中传递文件状态变化
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageEntryMessage {
    /// 文件被扫描到
    Scanned(Arc<EntryEnum>),
    /// 新增文件
    New(Arc<EntryEnum>),
    /// 文件已变更（kind 区分 data/metadata/both）
    Changed { entry: Arc<EntryEnum>, kind: ChangeKind },
    /// 文件已删除
    Deleted(Arc<EntryEnum>),
    /// 文件被重命名，(from, to)
    Renamed((Arc<EntryEnum>, Arc<EntryEnum>)),
    /// 文件完整性检查完成
    IntegrityChecked(Arc<EntryEnum>),
    /// 目录需要打包为 tar（仅在 --packaged 模式下由 walkdir 发射）
    Packaged(Arc<EntryEnum>),
    /// 打包完成后的 manifest 数据，由 sync worker 广播，DatabaseConsumer 消费写入数据库
    TarManifest {
        /// `.tar` 文件的 `relative_path`
        tar_path: String,
        /// tar 内包含的所有条目
        entries: Vec<Arc<EntryEnum>>,
    },
    /// 扫描/同步过程中遇到的 per-entry 错误
    Error {
        /// 出错的事件类型
        event: ErrorEvent,
        /// 出错的文件或目录路径
        path: PathBuf,
        /// 错误原因
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NASEntry {
    /// 通用属性
    /// 文件或目录的名称
    pub name: String,
    /// 相对于扫描根目录的相对路径
    pub relative_path: PathBuf,
    /// 文件扩展名（如果有）
    pub extension: Option<String>,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小（字节），目录大小通常为0
    pub size: u64,
    /// 访问时间（纳秒时间戳）
    pub atime: i64,
    /// 创建时间（纳秒时间戳）
    pub ctime: i64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,
    /// 文件权限模式（Unix风格）
    pub mode: u32,

    /// 是否为符号链接（Unix和NFS客户端使用）
    pub is_symlink: bool,
    /// 硬链接数（Unix和NFS客户端使用）
    pub hard_links: Option<u32>,
    /// 用户ID（Unix和NFS客户端使用）
    pub uid: Option<u32>,
    /// 组ID（Unix和NFS客户端使用）
    pub gid: Option<u32>,
    /// inode编号（Unix和NFS客户端使用）
    pub ino: Option<u64>,

    /// 文件句柄（NFS file handle / CIFS file ID 等，用于跨协议唯一标识文件）
    #[serde(skip)]
    pub file_handle: Option<Bytes>,

    /// `NFSv4` ACL（仅 `NFSv4`+ 填充）
    #[serde(skip)]
    pub acl: Option<nfs_rs::Acl>,

    /// `NFSv4` owner 字符串，如 "root@localdomain"
    pub owner: Option<String>,

    /// `NFSv4` `owner_group` 字符串
    pub owner_group: Option<String>,

    /// Extended attributes (xattr) key-value pairs（仅 `NFSv4`+ 扫描时填充）
    #[serde(skip)]
    pub xattrs: Option<Vec<(String, Vec<u8>)>>,
}

impl fmt::Display for NASEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NASEntry {{")?;
        write!(f, "name: {:?}, ", self.name)?;
        write!(f, "relative_path: {}, ", self.relative_path.display())?;
        write!(f, "extension: {:?}, ", self.extension)?;
        write!(f, "is_dir: {}, ", self.is_dir)?;
        write!(f, "size: {}, ", self.size)?;
        write!(f, "atime: {}, ", datetime_to_string(self.atime))?;
        write!(f, "ctime: {}, ", datetime_to_string(self.ctime))?;
        write!(f, "mtime: {}, ", datetime_to_string(self.mtime))?;
        write!(f, "mode: {:o}, ", self.mode)?; // 八进制格式化
        write!(f, "is_symlink: {}, ", self.is_symlink)?;
        write!(f, "hard_links: {:?}, ", self.hard_links)?;
        write!(f, "uid: {:?}, ", self.uid)?;
        write!(f, "gid: {:?}, ", self.gid)?;
        write!(f, "ino: {:?}, ", self.ino)?;
        write!(f, "file_handle: {:?}, ", self.file_handle)?;
        if let Some(ref owner) = self.owner {
            write!(f, "owner: {owner:?}, ")?;
        }
        if let Some(ref owner_group) = self.owner_group {
            write!(f, "owner_group: {owner_group:?}, ")?;
        }
        if let Some(ref acl) = self.acl {
            write!(f, "acl: {} aces, ", acl.aces.len())?;
        }
        if let Some(ref xattrs) = self.xattrs {
            write!(f, "xattrs: {} entries, ", xattrs.len())?;
        }
        write!(f, "}}")
    }
}

/// 文件标签结构，用于存储文件的元数据标签
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// 标签键
    pub key: String,
    /// 标签值
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Entry {
    /// 通用属性
    /// 文件或目录的名称
    pub name: String,
    /// 相对于扫描根目录的相对路径
    pub relative_path: String,
    /// 文件扩展名（如果有）
    pub extension: Option<String>,
    /// 文件大小（字节），目录大小通常为0
    pub size: u64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,

    /// 对象特有属性
    /// 文件标签（S3使用）
    pub tags: Option<Vec<Tag>>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: Option<String>,
    /// 是否为最新版本（S3多版本使用）
    pub is_latest: bool,
    /// 是否为删除标记（S3多版本使用）
    pub is_delete_marker: bool,
    /// 对象版本数量（S3多版本使用）
    pub version_count: Option<u32>,
    /// 是否为目录（S3 prefix）
    pub is_dir: bool,
}

impl fmt::Display for S3Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3Entry {{")?;
        write!(f, "name: {:?}, ", self.name)?;
        write!(f, "relative_path: {:?}, ", self.relative_path)?;
        write!(f, "extension: {:?}, ", self.extension)?;
        write!(f, "size: {}, ", self.size)?;
        write!(f, "mtime: {}, ", datetime_to_string(self.mtime))?;
        write!(f, "tags: {:?}, ", self.tags)?;
        write!(f, "version_id: {:?}, ", self.version_id)?;
        write!(f, "is_latest: {}, ", self.is_latest)?;
        write!(f, "is_delete_marker: {}, ", self.is_delete_marker)?;
        write!(f, "version_count: {:?}, ", self.version_count)?;
        write!(f, "is_dir: {}", self.is_dir)?;
        write!(f, "}}")
    }
}

// 定义异步迭代器类型别名
pub type WalkDirAsyncIterator = AsyncReceiver<StorageEntryMessage>;

/// 计算两个时间点之间的天数差
///
/// # 参数
/// - `now`: 当前时间
/// - `time`: 要比较的时间
///
/// # 返回值
/// - 正数表示time晚于now
/// - 负数表示time早于now
/// - 0表示两个时间相同
pub fn days_between(now: SystemTime, time: SystemTime) -> f64 {
    // 计算两个时间点之间的持续时间
    let duration = if now <= time {
        time.duration_since(now).unwrap_or(Duration::ZERO)
    } else {
        now.duration_since(time).unwrap_or(Duration::ZERO)
    };

    // 将持续时间转换为天数（f64）
    let seconds = duration.as_secs() as f64;
    let nanoseconds = f64::from(duration.subsec_nanos());
    let total_seconds = seconds + nanoseconds / 1_000_000_000.0;

    // 计算天数（一天 = 86400 秒）
    let days = total_seconds / 86400.0;

    // 根据时间顺序调整符号
    if now <= time { days } else { -days }
}
