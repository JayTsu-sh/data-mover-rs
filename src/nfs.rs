// 标准库
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

// 外部crate
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use moka::sync::Cache;
// nfs_rs 错误类型，用于直接匹配 error code
use nfs_rs::NfsError;
use nfs_rs::{ExportEntry, Mount, OPEN_READ, OPEN_WRITE, Time};
use path_clean::PathClean;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use crate::checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
use crate::error::StorageError;
use crate::filter::{FilterExpression, dir_matches_date_filter, should_skip};
use crate::qos::QosManager;
use crate::storage_enum::StorageEnum;
use crate::walk_scheduler::{create_worker_contexts, run_worker_loop};
use crate::{
    DataChunk, DeleteDirIterator, DeleteEvent, EntryEnum, ErrorEvent, MB, NASEntry, Result,
    StorageEntryMessage, WalkDirAsyncIterator,
};

/// 将 `nfs_rs::Time` 转换为纳秒时间戳
fn time_to_i64(time: Time) -> i64 {
    crate::time_util::combine_secs_nanos(i64::from(time.seconds), time.nseconds)
}

/// 将纳秒时间戳转换为 `nfs_rs::Time`
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn i64_to_time(timestamp: i64) -> Time {
    Time {
        seconds: crate::time_util::nanos_to_secs(timestamp) as u32,
        nseconds: crate::time_util::nanos_subsec(timestamp),
    }
}

/// NFSv4 可选富化字段。
///
/// 不同站点对 ACL/owner/xattrs 的填充程度不同：
/// - `lookup` 仅有 attrs 中携带的 ACL/owner，没有 xattrs；
/// - `iterative_walkdir` 同时填充 ACL/owner 与从服务器读到的 xattrs；
/// - `walkdir_2` 跳过的与可见的目录项都不填充。
///
/// 用此结构显式表达每站点的差异，避免在构造器签名中堆 `Option`。
pub(crate) struct NfsEnrich {
    pub acl: Option<nfs_rs::Acl>,
    pub owner: Option<String>,
    pub owner_group: Option<String>,
    pub xattrs: Option<Vec<(String, Vec<u8>)>>,
}

impl Default for NfsEnrich {
    /// 全部不填充。供 `walkdir_2` 系列站点使用。
    fn default() -> Self {
        Self {
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        }
    }
}

impl NfsEnrich {
    /// 从 `attr` 抽取 ACL/owner/owner_group（空字符串视为 None），xattrs 留 None。
    /// 供 `lookup` / `iterative_walkdir` 使用；后者再链 `with_xattrs`。
    pub fn from_attrs(attr: &nfs_rs::Attr) -> Self {
        Self {
            acl: attr.acl.clone(),
            owner: if attr.owner.is_empty() {
                None
            } else {
                Some(attr.owner.clone())
            },
            owner_group: if attr.owner_group.is_empty() {
                None
            } else {
                Some(attr.owner_group.clone())
            },
            xattrs: None,
        }
    }

    pub fn with_xattrs(mut self, xattrs: Option<Vec<(String, Vec<u8>)>>) -> Self {
        self.xattrs = xattrs;
        self
    }
}

impl NASEntry {
    /// 从 NFS `Attr` 构建 `NASEntry`。
    ///
    /// `file_handle` 由调用方提供（lookup 站点用 `obj.fh`，walkdir 用 `entry.handle.clone()`）。
    /// 可选的 ACL/owner/xattrs 通过 [`NfsEnrich`] 显式注入。
    pub(crate) fn from_nfs_attrs(
        name: String,
        relative_path: PathBuf,
        extension: Option<String>,
        attrs: &nfs_rs::Attr,
        file_handle: Bytes,
        enrich: NfsEnrich,
    ) -> Self {
        let is_dir = attrs.type_ == FType3::NF3DIR as u32;
        let is_symlink = attrs.type_ == FType3::NF3LNK as u32;
        Self {
            name,
            relative_path,
            extension,
            is_dir,
            size: attrs.filesize,
            mtime: time_to_i64(attrs.mtime),
            atime: time_to_i64(attrs.atime),
            ctime: time_to_i64(attrs.ctime),
            mode: attrs.file_mode,
            hard_links: Some(attrs.nlink),
            is_symlink,
            file_handle: Some(file_handle),
            uid: Some(attrs.uid),
            gid: Some(attrs.gid),
            ino: Some(attrs.fsid),
            acl: enrich.acl,
            owner: enrich.owner,
            owner_group: enrich.owner_group,
            xattrs: enrich.xattrs,
        }
    }
}

/// 基于目录深度的缓存过期策略。
/// 深度越浅 TTI 越长（高复用），深度越大 TTI 越短（优先淘汰）。
/// cache 容量满时，深层条目因 TTI 短而确定性地先过期被清除。
struct DepthAwareExpiry;

impl DepthAwareExpiry {
    /// 根据路径深度返回对应的 TTI
    fn tti_for_depth(depth: usize) -> Duration {
        match depth {
            0..=2 => Duration::from_secs(7200), // 浅层：2h
            3..=4 => Duration::from_secs(600),  // 中层：10min
            _ => Duration::from_secs(10),       // 深层：10s
        }
    }

    /// 从缓存 key 的路径提取目录深度
    fn path_depth(path: &Path) -> usize {
        path.components()
            .filter(|c| matches!(c, Component::Normal(_)))
            .count()
    }
}

impl moka::Expiry<(PathBuf, Bytes), Bytes> for DepthAwareExpiry {
    fn expire_after_create(
        &self,
        key: &(PathBuf, Bytes),
        _value: &Bytes,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(Self::tti_for_depth(Self::path_depth(&key.0)))
    }

    fn expire_after_read(
        &self,
        key: &(PathBuf, Bytes),
        _value: &Bytes,
        _read_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
        _last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        // 每次读取刷新 TTI（time-to-idle 语义）
        Some(Self::tti_for_depth(Self::path_depth(&key.0)))
    }

    fn expire_after_update(
        &self,
        key: &(PathBuf, Bytes),
        _value: &Bytes,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(Self::tti_for_depth(Self::path_depth(&key.0)))
    }
}

static GLOBAL_CACHE: LazyLock<Cache<(PathBuf, Bytes), Bytes>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(500_000)
        .expire_after(DepthAwareExpiry)
        .build()
});

/// nfs_url → server_id 映射表。
/// 相同 NFS 端点的所有 worker 共享同一 server_id，从而共享 GLOBAL_CACHE 条目，
/// 消除重复 LOOKUP RPC。
static SERVER_ID_REGISTRY: LazyLock<DashMap<String, u64>> = LazyLock::new(DashMap::new);

/// 全局 server ID 计数器，每个不同的 NFS 端点分配一个唯一 ID。
static NEXT_SERVER_ID: AtomicU64 = AtomicU64::new(1);

/// 根据 `nfs_url` 获取或分配 `server_id`。
/// 同一端点的并发调用保证返回相同 ID（DashMap entry API 原子插入）。
fn get_or_assign_server_id(nfs_url: &str) -> u64 {
    if let Some(id) = SERVER_ID_REGISTRY.get(nfs_url) {
        return *id;
    }
    // DashMap::entry() 持有 shard 写锁，保证只有一个 closure 被执行，
    // 计数器仅自增一次。上方 get() 是快速路径（read lock），
    // 在 entry 已存在时避免申请写锁，降低热路径竞争。
    *SERVER_ID_REGISTRY
        .entry(nfs_url.to_string())
        .or_insert_with(|| NEXT_SERVER_ID.fetch_add(1, Ordering::Relaxed))
}

/// NFS STALE handle 重试上限。并发 mkdir/create 时 file handle 可能短暂失效，
/// 单次重试不足以覆盖高并发场景，故允许最多 3 次重试。
const MAX_STALE_RETRIES: u8 = 3;

/// mount 阶段针对 portmapper 端口冲突的总尝试次数（含首次）。
/// Windows 上 nfs-rs 绑定特权端口（<1024）失败后，端口进入 `TIME_WAIT`（默认 240 秒）。
/// nfs-rs 内部已有 200 次端口重试；此处为极端端口耗尽场景的外层兜底。
const MAX_MOUNT_PORT_ATTEMPTS: u32 = 3;

/// mount 内层重试的初始等待时间（毫秒），指数退避：1s、2s
const MOUNT_PORT_RETRY_INITIAL_MS: u64 = 1000;

/// 检测"目标不存在"错误（NFS3ERR_NOENT / NFS4ERR_NOENT / MNT3ERR_NOENT / Io NotFound）。
///
/// 用途：
/// - delete_*：幂等成功（删除已不存在的对象不报错）
/// - lookup_fh：转换为 `DirectoryNotFound`/`FileNotFound` 返回给上层
///
/// 不参与 stale-handle 重试（语义上 NOENT 是终态，不是 cache 失效）。
fn is_nfs_noent(err: &NfsError) -> bool {
    match err {
        NfsError::Nfs3(code) => matches!(code, nfs_rs::Nfs3ErrorCode::NFS3ERR_NOENT),
        NfsError::Nfs4(code) => matches!(code, nfs_rs::Nfs4ErrorCode::NFS4ERR_NOENT),
        NfsError::Mount(code) => matches!(code, nfs_rs::Nfs3MountErrorCode::MNT3ERR_NOENT),
        NfsError::Io(io_err) => io_err.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

/// 检测"陈旧文件句柄"错误（NFS3ERR_STALE / NFS3ERR_BADHANDLE / NFS4ERR_STALE / NFS4ERR_BADHANDLE）。
///
/// 含义：缓存的 file handle 在服务端已失效（如服务端重启、export 重新生成）。
/// 与 NFS4ERR_DELAY 区分：DELAY 是"服务端繁忙，稍后重试"，由 nfs-rs 层
/// （`compound()` 中带退避的重试循环）处理，不在 data-mover 层重复处理。
fn is_stale_handle(err: &NfsError) -> bool {
    match err {
        NfsError::Nfs3(code) => matches!(
            code,
            nfs_rs::Nfs3ErrorCode::NFS3ERR_STALE | nfs_rs::Nfs3ErrorCode::NFS3ERR_BADHANDLE
        ),
        NfsError::Nfs4(code) => matches!(
            code,
            nfs_rs::Nfs4ErrorCode::NFS4ERR_STALE | nfs_rs::Nfs4ErrorCode::NFS4ERR_BADHANDLE
        ),
        _ => false,
    }
}

/// 检测"服务端繁忙"错误（NFS4ERR_DELAY）。
///
/// 含义：服务端暂时繁忙，请稍后重试。nfs-rs 层在 `compound()` 中已对单条 RPC
/// 做了带 jitter 的退避重试。本函数用于 **流式/迭代式操作**（如 `readdirplus`）
/// 的**操作级重试**：当流式遍历中途某条 RPC 在 nfs-rs 重试耗尽后仍返回 DELAY，
/// 重新从头开始整个 readdirplus 比"丢弃失败 entry 继续"安全得多
/// （否则会出现统计计数偏低的 silent data loss）。
///
/// 与 [`is_stale_handle`] 的恢复策略不同：
/// - stale handle: 刷新 root_fh + 清除路径缓存
/// - server busy: 单纯延时再试（nfs-rs 已做，data-mover 在流操作中再试）
fn is_server_busy(err: &NfsError) -> bool {
    matches!(err, NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_DELAY))
}

/// 应用层 NFS4ERR_DELAY 退避：2s/4s/8s 指数延时，最多 3 次（额外 ~14s 容错）。
///
/// nfs-rs 在 `compound()` 中已对单条 RPC 做带 jitter 的退避重试（~75s）。
/// 仍 DELAY 说明服务端持续高负载（如批量并发 create 后期）；
/// 此函数提供应用层的二次延时，由调用方决定 return / continue 的控制流。
async fn backoff_server_busy(op: &str, target: &(dyn std::fmt::Debug + Send + Sync), retries: u8) {
    let sleep_ms = 2000u64 * (1u64 << u64::from(retries));
    warn!(
        "{}: server busy on {:?}, app-level retry {}/{} after {}ms",
        op,
        target,
        retries + 1,
        MAX_STALE_RETRIES,
        sleep_ms
    );
    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
}

/// 检测调用方可通过"清除路径缓存 + 刷新 root_fh + 重试"恢复的错误。
///
/// 包含：
/// - **STALE / BADHANDLE**：文件句柄陈旧（[`is_stale_handle`]），刷新后可恢复。
/// - **NOENT**（[`is_nfs_noent`]）：高并发场景下可能为瞬态——
///   如 worker A 刚 `mkdir` 完成但 worker B 的 path 缓存尚未失效，
///   B 此时 LOOKUP 会拿到 NOENT；清除缓存重试可恢复。
///   若重试耗尽仍 NOENT，调用方应转换为 `FileNotFound`/`DirectoryNotFound`。
///
/// **不包含 NFS4ERR_DELAY**：DELAY 由 nfs-rs 层（`compound()` 退避重试）处理，
/// data-mover 层若再叠加重试只会延长等待，无额外收益。
fn is_retryable_with_invalidation(err: &NfsError) -> bool {
    is_stale_handle(err) || is_nfs_noent(err)
}

/// 清除指定路径的所有前缀缓存条目（从根到完整路径）
fn invalidate_path_cache(components: &[String], root_fh: &Bytes) {
    for i in 0..components.len() {
        let path: PathBuf = components[0..=i].iter().collect();
        GLOBAL_CACHE.invalidate(&(path, root_fh.clone()));
    }
}

/// NFS v3 文件类型枚举
///
/// 该枚举定义了 NFS v3 协议支持的各种文件类型，对应 NFS 协议中的文件类型编码。
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum FType3 {
    /// 普通文件
    #[default]
    NF3REG = 1,
    /// 目录
    NF3DIR = 2,
    /// 符号链接
    NF3LNK = 5,
}

/// NFS 文件句柄结构体
///
/// 该结构体封装了 NFS 文件句柄，包含了 NFS 服务器返回的对象结果和文件路径。
/// 用于在异步操作中传递文件句柄信息，避免重复查找。
#[derive(Debug, Clone)]
pub(crate) struct NFSFileHandle {
    /// NFS 服务器返回的对象结果，包含文件句柄和属性
    pub inner: Arc<nfs_rs::ObjRes>,
    /// 文件的相对路径
    pub path: PathBuf,
}

impl NFSFileHandle {
    /// 创建一个新的 NFS 文件句柄
    ///
    /// # 参数
    /// - `inner`：NFS 服务器返回的对象结果，包含文件句柄和属性
    /// - `path`：文件的相对路径
    ///
    /// # 返回值
    /// - `NFSFileHandle`：新创建的 NFS 文件句柄
    pub(crate) fn new(fh: Bytes, path: PathBuf) -> Self {
        Self {
            inner: Arc::new(nfs_rs::ObjRes { fh, attr: None }),
            path,
        }
    }
}

const DEFAULT_BLOCK_SIZE: u64 = 2 * MB;

/// 从 `relative_path` 中剥离 root 前缀，返回相对于 root 的路径。
/// 使用 `Path::strip_prefix` 按组件比较，跨平台兼容，零字符串分配。
fn strip_root_prefix(root: &str, relative_path: &Path) -> PathBuf {
    if root.is_empty() {
        return relative_path.to_path_buf();
    }
    let root_path = Path::new(root);
    match relative_path.strip_prefix(root_path) {
        Ok(stripped) => stripped.to_path_buf(),
        Err(_) => relative_path.to_path_buf(),
    }
}

/// 安全地拼接两个 NFS 路径部分，避免重复的 '/'
fn join_nfs_paths(base: &str, suffix: &str) -> String {
    if base.is_empty() {
        suffix.to_string()
    } else if suffix.is_empty() {
        base.to_string()
    } else {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            suffix.trim_start_matches('/')
        )
    }
}

/// 构建相对于 root 的路径（使用 '/' 拼接，避免平台差异）
///
/// 在 walkdir 遍历时，`dir_path` 从 root 开始传入。第一层 `dir_path` == root，
/// 递归层 `dir_path` 已经是剥离 root 后的相对路径。此函数负责剥离 root 前缀
/// 并拼接 entry 文件名，返回相对于 root 的路径。
fn build_relative_path_impl(root: &str, dir_path: &str, entry_file_name: &str) -> String {
    if dir_path == root {
        // 第一层：dir_path 恰好等于 root → 直接返回文件名
        entry_file_name.to_string()
    } else if dir_path.is_empty() {
        // root 为空且 dir_path 为空 → 直接返回文件名
        entry_file_name.to_string()
    } else if !root.is_empty() {
        // root 非空：尝试剥离 "root/" 前缀（仅第一层子目录可能触发）
        if let Some(rest) = dir_path.strip_prefix(root) {
            // dir_path 以 root 开头，检查后面是否紧跟 '/'
            if let Some(stripped) = rest.strip_prefix('/') {
                if stripped.is_empty() {
                    entry_file_name.to_string()
                } else {
                    format!("{stripped}/{entry_file_name}")
                }
            } else {
                // root 是 dir_path 的子串但不在目录边界（如 root="ab", dir="abc"）
                format!("{dir_path}/{entry_file_name}")
            }
        } else {
            // dir_path 不包含 root 前缀（递归层的常见路径）
            format!("{dir_path}/{entry_file_name}")
        }
    } else {
        // root 为空，直接拼接
        format!("{dir_path}/{entry_file_name}")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StorageConfig {
    /// 块大小，默认2MB
    pub block_size: u64,
}

/// 该结构体是 NFS v3 存储的核心实现，封装了 NFS 挂载、根文件句柄和根路径等信息。
/// 实现了 `Storage` trait，提供了文件和目录的各种操作方法。
#[derive(Debug, Clone)]
pub struct NFSStorage {
    /// NFS 挂载实例
    mount: Arc<Box<dyn Mount>>,
    /// 根目录文件句柄（共享可变，支持 stale 后刷新）
    root_fh: Arc<std::sync::RwLock<Bytes>>,
    /// 根路径（NFS 路径始终用 '/' 分隔，不依赖平台 `PathBuf`）
    root: Arc<String>,
    /// 存储配置
    pub(crate) config: StorageConfig,
    /// `root_fh` 刷新代数，每次成功刷新 +1，用于合并并发刷新
    refresh_generation: Arc<AtomicU64>,
    /// 序列化 refresh 操作，确保同一时刻只有一个 worker 执行实际刷新 RPC
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    /// NFS 端点的唯一 ID（按 `nfs_url` 分配，同一端点的所有 worker 共享）。
    /// 用于构造 `GLOBAL_CACHE` key 前缀，确保不同服务器的条目互不冲突，
    /// 同时允许同一端点的 worker 复用彼此缓存，消除重复 LOOKUP RPC。
    server_id: u64,
    /// 预计算的 cache key 前缀：`server_id(8B)` + `root_fh`。
    /// 仅在构造和 `refresh_root_fh` 时更新，避免热路径每次分配 `BytesMut`。
    cache_root_fh: Arc<std::sync::RwLock<Bytes>>,
}

/// `RwLock` poison 错误统一入口（写路径均用 ? 传播，不会 panic，此路径理论上不触发）
#[inline]
fn root_fh_lock_err() -> StorageError {
    StorageError::OperationError("root_fh lock poisoned".to_string())
}

/// 构建 cache key 前缀：`server_id(8B)` + `raw_fh`
fn build_cache_root_fh(server_id: u64, raw_fh: &Bytes) -> Bytes {
    let mut buf = bytes::BytesMut::with_capacity(8 + raw_fh.len());
    buf.extend_from_slice(&server_id.to_be_bytes());
    buf.extend_from_slice(raw_fh);
    buf.freeze()
}

impl NFSStorage {
    /// 解析 NFS URL
    ///
    /// 该方法将 NFS URL 解析为标准的 NFS 挂载 URL 和根目录路径。
    /// 支持的 URL 格式：`nfs://server/path:root_dir` 或 `nfs://server/path`
    ///
    /// # 参数
    /// - `url`：NFS URL 字符串
    ///
    /// # 返回值
    /// - `Ok((nfs_url, root_dir))`：解析成功，返回标准 NFS URL 和根目录路径
    /// - `Err(StorageError)`：解析失败，返回错误信息
    #[allow(clippy::similar_names)]
    fn parse_nfs_url(url: &str) -> Result<(String, String)> {
        let parsed_url = url.strip_prefix("nfs://").ok_or_else(|| {
            let msg = format!("Invalid NFS URL format: {url}");
            error!("{msg}");
            StorageError::OperationError(msg)
        })?;

        let (server_part, raw_path_part) = parsed_url.split_once('/').map_or_else(
            || (parsed_url.to_string(), "/".to_string()),
            |(s, p)| (s.to_string(), format!("/{p}")),
        );

        let (path_part, query_part) = raw_path_part
            .split_once('?')
            .unwrap_or((&raw_path_part, ""));

        // root_dir 去掉前后 '/'，使其成为相对路径前缀（空串表示挂载根）
        let (nfs_path, root_dir) = path_part.split_once(':').map_or_else(
            || (path_part.to_string(), String::new()),
            |(p, r)| (p.to_string(), r.trim_matches('/').to_string()),
        );

        // 检查query_part是否包含uid和gid参数
        let has_uid = query_part.contains("uid=");
        let has_gid = query_part.contains("gid=");

        let query_suffix = if query_part.is_empty() {
            // 如果query_part为空，直接添加uid=0&gid=。主要考虑到windows环境下的使用便利性。
            "?uid=0&gid=0".to_string()
        } else {
            // 如果query_part不为空，检查是否需要添加uid和gid
            let mut parts = query_part.split('&').collect::<Vec<&str>>();
            if !has_uid {
                parts.push("uid=0");
            }
            if !has_gid {
                parts.push("gid=0");
            }
            format!("?{}", parts.join("&"))
        };

        let nfs_url = format!("nfs://{server_part}{nfs_path}{query_suffix}");

        Ok((nfs_url, root_dir))
    }

    /// 内部方法：解析 URL、挂载 NFS、构建初始 storage 实例
    /// 返回 `(storage, root_dir)`，此时 `root_fh` 指向 mount 根，尚未解析 `root_dir`
    async fn mount_and_build(url: &str, block_size: Option<u64>) -> Result<(Self, String)> {
        let (nfs_url, root_dir) = Self::parse_nfs_url(url)?;
        info!(
            "Mounting NFS at: {:?} with root dir: {:?}",
            nfs_url, root_dir
        );

        // portmapper 端口冲突重试：Windows 上 nfs-rs 绑定特权端口 (<1024) 时，
        // 先前失败的连接会使端口进入 TIME_WAIT，导致后续 WSAEADDRINUSE (os error 10048)。
        // NfsError::Rpc 包装 portmapper 返回的字符串消息；
        // 等待后重试让 OS 有机会分配不同的可用端口。
        let mount = {
            let mut last_err: Option<nfs_rs::NfsError> = None;
            let mut result = None;
            for attempt in 0..MAX_MOUNT_PORT_ATTEMPTS {
                match nfs_rs::parse_url_and_mount(&nfs_url).await {
                    Ok(m) => {
                        result = Some(m);
                        break;
                    }
                    Err(e) => {
                        let is_portmapper_conflict =
                            matches!(&e, NfsError::Rpc(msg) if msg.contains("portmapper"));
                        if is_portmapper_conflict && attempt + 1 < MAX_MOUNT_PORT_ATTEMPTS {
                            let delay_ms = MOUNT_PORT_RETRY_INITIAL_MS * (1u64 << attempt);
                            warn!(
                                "NFS portmapper 端口冲突 (attempt {}/{}), {}ms 后重试: {}",
                                attempt + 1,
                                MAX_MOUNT_PORT_ATTEMPTS,
                                delay_ms,
                                e
                            );
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            last_err = Some(e);
                        } else {
                            // 非端口冲突错误（或最后一次重试）：直接失败
                            let msg = format!("Failed to mount NFS at {nfs_url}: {e}");
                            error!("{msg}");
                            return Err(StorageError::OperationError(msg));
                        }
                    }
                }
            }
            if let Some(m) = result {
                m
            } else {
                let msg = format!(
                    "Failed to mount NFS at {nfs_url} after {MAX_MOUNT_PORT_ATTEMPTS} retries: {}",
                    last_err.map_or_else(|| "unknown error".to_string(), |e| e.to_string())
                );
                error!("{msg}");
                return Err(StorageError::OperationError(msg));
            }
        };

        let mount_fh = mount.getfh().await;
        // 取 NFS 服务器协商后的 rsize / wsize，将 block_size 对齐到两者的较小值，
        // 确保每次 read/write 调用都走单次 RPC 零拷贝快路径
        let rsize = u64::from(mount.get_max_read_size());
        let wsize = u64::from(mount.get_max_write_size());
        let max_transfer = std::cmp::min(rsize, wsize);
        let effective_block_size = block_size
            .map_or(DEFAULT_BLOCK_SIZE, |size| {
                std::cmp::min(size, DEFAULT_BLOCK_SIZE)
            })
            .min(max_transfer);
        info!(
            "NFS rsize={}, wsize={}, effective block_size={}",
            rsize, wsize, effective_block_size
        );

        let sid = get_or_assign_server_id(&nfs_url);
        let cache_fh = build_cache_root_fh(sid, &mount_fh);
        let storage = NFSStorage {
            root_fh: Arc::new(std::sync::RwLock::new(mount_fh)),
            mount: Arc::new(mount),
            root: Arc::new(String::new()),
            config: StorageConfig {
                block_size: effective_block_size,
            },
            refresh_generation: Arc::new(AtomicU64::new(0)),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            server_id: sid,
            cache_root_fh: Arc::new(std::sync::RwLock::new(cache_fh)),
        };

        Ok((storage, root_dir))
    }

    /// 查询指定主机的 NFS 导出列表
    ///
    /// 该方法封装了 nfs-rs 的 `list_exports` 函数，用于查询指定主机的 NFS 导出列表。
    /// 相当于运行 `showmount -e HOST` 命令，不需要现有的 NFS 挂载。
    ///
    /// # 参数
    /// - `host`：NFS 服务器主机名或 IP 地址，也可以是完整的 `nfs://` URL
    /// # 返回值
    /// - `Ok(Vec<ExportEntry>)`：查询成功，返回导出列表
    /// - `Err(StorageError)`：查询失败，返回错误信息
    pub async fn list_exports(host: &str) -> Result<Vec<ExportEntry>> {
        nfs_rs::list_exports(host)
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to list NFS exports: {e}")))
    }

    pub async fn new(url: &str, block_size: Option<u64>) -> Result<Self> {
        let (mut storage, root_dir) = Self::mount_and_build(url, block_size).await?;

        if !root_dir.is_empty() {
            info!("Looking up file handler for root directory: {:?}", root_dir);
            let path = Path::new(&root_dir);

            let obj_res = storage.lookup_fh(path).await.map_err(|e| {
                let msg = format!("Failed to lookup root directory {root_dir}: {e}");
                error!("{}", msg);
                StorageError::OperationError(msg)
            })?;

            *storage.root_fh.write().map_err(|_| root_fh_lock_err())? = obj_res.fh;
            storage.root = Arc::new(root_dir);
        }

        Ok(storage)
    }

    /// 返回用于 `GLOBAL_CACHE` key 的标识符：`server_id`(8B) + `root_fh`。
    /// 预计算值，clone 仅增加引用计数。
    fn get_root_fh(&self) -> Bytes {
        self.cache_root_fh
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 返回真实的 NFS root file handle（用于 RPC 操作，不含 `server_id` 前缀）
    fn rpc_root_fh(&self) -> Bytes {
        self.root_fh
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 从 NFS mount 协议层重新获取 `root_fh`（绕开缓存，直接 RPC）。
    /// `mount.getfh()` 不依赖 `self.root_fh`，即使 `root_fh` 已 stale 也能成功。
    async fn do_refresh_root_fh(&self) -> Result<()> {
        let mut current_fh = self.mount.getfh().await;

        // 如果有 prefix（self.root 非空），逐级 lookup 得到真正的 root_fh
        if !self.root.is_empty() {
            for component in self.root.split('/').filter(|s| !s.is_empty()) {
                let obj = self
                    .mount
                    .lookup(current_fh, component)
                    .await
                    .map_err(|e| {
                        StorageError::NfsError(format!(
                            "Failed to refresh root fh at '{component}': {e}"
                        ))
                    })?;
                current_fh = obj.fh;
            }
        }

        // 注意：刷新 root_fh 后本实例的 cache_root_fh 前缀随之变化。
        // 与同 server_id 的其他 worker 暂时失去缓存共享，直到它们也完成 FH 刷新。
        // 这是预期行为：root_fh 变化意味着服务器视角已改变，旧缓存条目需重建。
        // 旧条目不会造成错误，仅在 TTI 到期前占用少量内存。
        let cache_fh = build_cache_root_fh(self.server_id, &current_fh);
        *self.root_fh.write().map_err(|_| root_fh_lock_err())? = current_fh;
        *self.cache_root_fh.write().map_err(|_| root_fh_lock_err())? = cache_fh;
        info!("root_fh refreshed successfully");
        Ok(())
    }

    /// 尝试刷新 `root_fh`。如果自 `stale_generation` 以来已有其他 worker 完成刷新，则跳过。
    /// 通过 generation + async mutex 确保同一时刻只有一个 worker 做实际 RPC 刷新，
    /// 其余 worker 等锁后发现 generation 已变，直接复用新 `root_fh`。
    async fn maybe_refresh_root_fh(&self, stale_generation: u64) -> Result<()> {
        let _guard = self.refresh_lock.lock().await;

        let current_gen = self.refresh_generation.load(Ordering::Acquire);
        if current_gen != stale_generation {
            debug!(
                "root_fh already refreshed by another worker (gen {} → {})",
                stale_generation, current_gen
            );
            return Ok(());
        }

        self.do_refresh_root_fh().await?;
        self.refresh_generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn collect_components(path: &Path) -> Result<Vec<String>> {
        let mut components: Vec<String> = Vec::new();

        for component in PathClean::clean(path).components() {
            if let Component::Normal(dirname) = component {
                if let Some(name_str) = dirname.to_str() {
                    components.push(name_str.to_string());
                } else {
                    return Err(StorageError::InvalidPath(
                        "Invalid directory name".to_string(),
                    ));
                }
            }
        }

        Ok(components)
    }

    pub async fn lookup_fh(&self, relative_path: &Path) -> Result<nfs_rs::ObjRes> {
        self.lookup_fh_inner(relative_path, 0).await
    }

    async fn lookup_fh_inner(&self, relative_path: &Path, retries: u8) -> Result<nfs_rs::ObjRes> {
        trace!("Looking up path: {:?}", relative_path);

        let components = Self::collect_components(relative_path)?;
        if components.is_empty() {
            return Ok(nfs_rs::ObjRes {
                fh: self.rpc_root_fh(),
                attr: None,
            });
        }

        // 从根目录开始（RPC 用真实 FH，缓存 key 用含 server_id 的标识符）
        let mut current_fh = self.rpc_root_fh();
        let mut start_index = 0;

        // 从最后一个组件开始匹配缓存，提高缓存查询效率
        // 更深层次的目录路径更容易存在于缓存中
        if !components.is_empty() {
            let mut partial_path: PathBuf = components.iter().collect();
            for i in (0..components.len()).rev() {
                // 构建缓存键
                let cache_key = (partial_path.clone(), self.get_root_fh());

                // 尝试从缓存中获取该路径的文件句柄
                if let Some(fh) = GLOBAL_CACHE.get(&cache_key) {
                    trace!("Global cache result for key={:?}: true", cache_key);
                    current_fh = fh;
                    start_index = i + 1;
                    break;
                }
                trace!("Global cache result for key={:?}: false", cache_key);
                // 移除最后一个组件，准备检查上一级目录
                partial_path.pop();
            }
        }

        // 当前正在处理的路径
        let mut current_path: PathBuf = components[0..start_index].iter().collect();

        // 从找到的缓存点或根目录开始，按顺序创建剩余的目录
        let components_len = components.len();
        for (i, dirname) in components[start_index..].iter().enumerate() {
            // 更新当前路径
            current_path.push(dirname);
            // 构建缓存键
            let cache_key = (current_path.clone(), self.get_root_fh());

            // 使用异步lookup调用
            let mount = self.mount.clone();
            let current_fh_clone = current_fh.clone();
            match mount.lookup(current_fh_clone, dirname).await {
                Ok(obj) => {
                    // 目录已存在，继续使用现有句柄
                    current_fh = obj.fh.clone();

                    let file_type = if let Some(attr) = &obj.attr {
                        attr.type_
                    } else {
                        error!("Missing file attributes for {:?}", obj);
                        continue;
                    };

                    let is_dir = file_type == FType3::NF3DIR as u32;
                    if is_dir {
                        // 将查询结果保存到缓存中
                        trace!(
                            "Inserting into global cache: key={:?}, value_len={}",
                            cache_key,
                            obj.fh.len()
                        );
                        GLOBAL_CACHE.insert(cache_key, obj.fh.clone());
                    }

                    // 如果是最后一个组件，直接返回原始obj对象
                    if i == components_len - start_index - 1 {
                        return Ok(obj);
                    }
                }
                Err(e) => {
                    // Stale handle：缓存的 fh 在 NFS 服务器侧已失效，刷新 root_fh 后从根重试
                    if is_retryable_with_invalidation(&e) && retries < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle detected in lookup_fh for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        invalidate_path_cache(&components, &root_fh);
                        return Box::pin(self.lookup_fh_inner(relative_path, retries + 1)).await;
                    }
                    if is_server_busy(&e) && retries < MAX_STALE_RETRIES {
                        backoff_server_busy("lookup_fh", &relative_path, retries).await;
                        return Box::pin(self.lookup_fh_inner(relative_path, retries + 1)).await;
                    }
                    // NOENT 重试耗尽：路径组件确实不存在，返回 DirectoryNotFound
                    // 这使 delete_file/delete_symlink 的调用方可将其视为幂等成功
                    if is_nfs_noent(&e) {
                        return Err(StorageError::DirectoryNotFound(format!(
                            "Directory '{dirname}' not found: {e}"
                        )));
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to lookup directory {dirname}: {e}"
                    )));
                }
            }
        }

        Ok(nfs_rs::ObjRes {
            fh: current_fh,
            attr: None,
        })
    }

    /// 打开文件，返回文件句柄。
    ///
    /// 对 NFSv3：等同于 lookup（无状态）。
    /// 对 NFSv4.1：发送 OPEN RPC，建立 stateid，支持 share reservation。
    ///
    /// 使用完毕后必须调用 [`close`] 释放资源（v3 为 no-op，v4.1 释放 stateid）。
    pub(crate) async fn open(&self, path: &Path, access: u32) -> Result<NFSFileHandle> {
        let components = Self::collect_components(path)?;
        if components.is_empty() {
            return Err(StorageError::InvalidPath(
                "Cannot open root as file".to_string(),
            ));
        }

        // components 非空已由上方 is_empty() 保证
        let filename = &components[components.len() - 1];

        for attempt in 0..=MAX_STALE_RETRIES {
            // 获取父目录 fh（利用缓存；重试时缓存已被清除，会重新 lookup）
            let parent_fh = if components.len() == 1 {
                self.rpc_root_fh()
            } else {
                let parent_path: PathBuf = components[..components.len() - 1].iter().collect();
                self.lookup_fh(&parent_path).await?.fh
            };

            // 调用 mount.open()：v3 内部走 lookup，v4.1 建立 stateid
            match self.mount.open(parent_fh, filename, access).await {
                Ok(obj) => return Ok(NFSFileHandle::new(obj.fh, path.to_path_buf())),
                Err(e) => {
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in open for {:?}, refreshing root_fh and retrying",
                            path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to open {}: {e}",
                        path.display()
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    /// 关闭文件，释放 share reservation。
    ///
    /// 对 NFSv3：no-op（无状态协议）。
    /// 对 NFSv4.1：发送 CLOSE RPC，释放 stateid。
    pub(crate) async fn close(&self, file: &NFSFileHandle) -> Result<()> {
        self.mount.close(file.inner.fh.clone()).await.map_err(|e| {
            StorageError::NfsError(format!("Failed to close {}: {e}", file.path.display()))
        })
    }

    pub(crate) async fn read_file(&self, path: &Path, size: u64) -> Result<Bytes> {
        let mut handle = self.open(path, OPEN_READ).await?;
        let result = self.read(&mut handle, 0, size as usize).await;
        // best-effort close，不覆盖 read 的错误
        let _ = self.close(&handle).await;
        result
    }

    pub(crate) async fn write_file(
        &self,
        path: &Path,
        data: Bytes,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        let len = data.len() as u32;
        let mut handle = self.create_file(path, uid, gid, mode).await?;
        let result = async {
            // Truncate file to 0 before writing to handle the case where new content is shorter than old content
            // This is critical for incremental sync where Changed files may have different sizes
            self.truncate_file(&handle).await?;
            self.write(&mut handle, 0, data).await?;
            self.commit(&handle, 0, len).await
        }
        .await;
        let _ = self.close(&handle).await;
        result
    }

    /// Truncate a file to 0 bytes by setting size=0 via setattr
    /// This is critical for incremental sync where Changed files may have new content shorter than old content
    pub(crate) async fn truncate_file(&self, file: &NFSFileHandle) -> Result<()> {
        debug!("Truncating file {:?} to 0 bytes", file.path);

        let mut current_fh = file.inner.fh.clone();

        for attempt in 0..=MAX_STALE_RETRIES {
            match self
                .mount
                .setattr(
                    current_fh.clone(),
                    None,    // 不设置 guard_ctime
                    None,    // 不设置 mode
                    None,    // 不设置 uid
                    None,    // 不设置 gid
                    Some(0), // 设置 size=0 来 truncate 文件
                    None,    // 不设置 atime
                    None,    // 不设置 mtime
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in truncate_file for {:?}, re-looking up and retrying",
                            file.path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(&file.path)?;
                        invalidate_path_cache(&components, &root_fh);
                        // 重新 lookup 获取新的文件句柄
                        let fresh_obj = self.lookup_fh(&file.path).await?;
                        current_fh = fresh_obj.fh;
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to truncate file: {}, {e:?}",
                        file.path.display()
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    /// 设置 uid/gid 后 re-lookup 刷新 handle（`set_metadata` 可能因 stale 重试更换了 fh）
    async fn apply_ownership(
        &self,
        handle: &NFSFileHandle,
        path: &Path,
        uid: u32,
        gid: u32,
    ) -> Result<NFSFileHandle> {
        self.set_metadata(handle, None, None, Some(uid), Some(gid), None)
            .await?;
        let fresh = self.lookup_fh(path).await?;
        Ok(NFSFileHandle::new(fresh.fh, path.to_path_buf()))
    }

    pub(crate) async fn create_file(
        &self,
        relative_path: &Path,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<NFSFileHandle> {
        // 提取文件名
        let filename = relative_path
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid destination path".to_string()))?
            .to_string_lossy()
            .to_string();

        // 确保父目录存在（只需一次，重试时只需刷新 fh）
        if let Some(parent) = relative_path.parent() {
            self.create_dir_all(parent).await?;
        }

        for attempt in 0..=MAX_STALE_RETRIES {
            let parent_fh = if let Some(parent) = relative_path.parent() {
                self.lookup_fh(parent).await?.fh
            } else {
                self.rpc_root_fh()
            };

            match self.mount.create(parent_fh, &filename, mode).await {
                Ok(obj) => {
                    let handle = NFSFileHandle::new(obj.fh, relative_path.to_path_buf());
                    if let (Some(uid), Some(gid)) = (uid, gid) {
                        return self.apply_ownership(&handle, relative_path, uid, gid).await;
                    }
                    return Ok(handle);
                }
                Err(e) => {
                    // 文件已存在：fallback 到 OPEN，不 truncate（由调用方决定）
                    if e.is_exist() {
                        trace!("File {:?} already exists, opening for write", relative_path);
                        let handle = self.open(relative_path, OPEN_WRITE).await?;
                        if let (Some(uid), Some(gid)) = (uid, gid) {
                            return self.apply_ownership(&handle, relative_path, uid, gid).await;
                        }
                        return Ok(handle);
                    }
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in create_file for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(relative_path)?;
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    if is_server_busy(&e) && attempt < MAX_STALE_RETRIES {
                        backoff_server_busy("create_file", &relative_path, attempt).await;
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "[create] Failed to create file: {}, {e}",
                        relative_path.display()
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    pub async fn delete_file(&self, relative_path: &Path) -> Result<()> {
        trace!("Removing file {:?}", relative_path);

        // 提取文件名
        let filename = relative_path
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid file path".to_string()))?
            .to_string_lossy()
            .to_string();

        for attempt in 0..=MAX_STALE_RETRIES {
            // 查找文件父目录
            let parent_obj = if let Some(parent) = relative_path.parent() {
                debug!(
                    "Looking up parent path for remove file {:?}: {:?}",
                    relative_path, parent
                );
                self.lookup_fh(parent).await?
            } else {
                trace!("Parent directory of {:?} is root directory", relative_path);
                self.mount.lookup_path("/").await.map_err(|e| {
                    StorageError::NfsError(format!("Failed to lookup root directory: {e}"))
                })?
            };

            // 删除文件 - 使用父目录的file handle
            match self.mount.remove(parent_obj.fh.clone(), &filename).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // 目标已不存在：幂等语义，视为成功
                    if e.is_not_found() {
                        trace!("File {:?} already gone, treating as success", relative_path);
                        return Ok(());
                    }
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in delete_file for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(relative_path)?;
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to remove file: {e}"
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    /// 创建多级目录
    ///
    /// 该方法用于创建多级目录，如果目录已存在则直接使用。
    /// 支持从缓存中查找目录，提高性能。
    ///
    /// # 参数
    /// - `relative_path`：相对路径
    ///
    /// # 返回值
    /// - `Ok(Bytes)`：创建成功，返回最后一个目录的文件句柄
    /// - `Err(StorageError)`：创建失败，返回错误信息
    pub async fn create_dir_all(&self, relative_path: &Path) -> Result<Bytes> {
        self.create_dir_all_inner(relative_path, 0).await
    }

    async fn create_dir_all_inner(&self, relative_path: &Path, retries: u8) -> Result<Bytes> {
        debug!("create_dir_all: {:?}", relative_path);
        // 收集目录组件
        let components = Self::collect_components(relative_path)?;

        // 从根目录开始（RPC 用真实 FH）
        let mut current_fh = self.rpc_root_fh();
        let mut start_index = 0;

        // 从最后一个组件开始匹配缓存，提高缓存查询效率
        // 更深层次的目录路径更容易存在于缓存中
        if !components.is_empty() {
            let mut partial_path: PathBuf = components.iter().collect();
            for i in (0..components.len()).rev() {
                // 构建缓存键
                let cache_key = (partial_path.clone(), self.get_root_fh());
                debug!("create_dir_all: cache_key: {:?}", cache_key);

                // 尝试从缓存中获取该路径的文件句柄
                if let Some(fh) = GLOBAL_CACHE.get(&cache_key) {
                    trace!("Global cache result for key={:?}: true", cache_key);
                    current_fh = fh;
                    start_index = i + 1;
                    break;
                }
                trace!("Global cache result for key={:?}: false", cache_key);
                // 移除最后一个组件，准备检查上一级目录
                partial_path.pop();
            }
        }

        // 当前正在处理的路径
        let mut current_path: PathBuf = components[0..start_index].iter().collect();

        // 从找到的缓存点或根目录开始，按顺序创建剩余的目录
        for dirname in &components[start_index..] {
            debug!("create_dir_all: dirname: {:?}", dirname);
            // 更新当前路径
            current_path.push(dirname);
            // 构建缓存键
            let cache_key = (current_path.clone(), self.get_root_fh());
            debug!("create_dir_all: cache_key: {:?}", cache_key);

            // 先尝试创建目录 - 使用异步mkdir调用
            let mount_clone = self.mount.clone();
            let current_fh_clone = current_fh.clone();
            match mount_clone.mkdir(current_fh_clone, dirname, 0o755).await {
                Ok(obj) => {
                    // 目录创建成功，继续使用新句柄
                    current_fh = obj.fh.clone();
                    // 将创建的目录句柄保存到缓存中
                    trace!(
                        "Inserting into global cache: key={:?}, value_len={}",
                        cache_key,
                        obj.fh.len()
                    );
                    GLOBAL_CACHE.insert(cache_key, obj.fh);
                }
                Err(e) => {
                    debug!("Error: dir {} {}", dirname, e);
                    // 检查错误是否为"目录已存在"
                    if e.is_exist() {
                        // 目录已存在，尝试查找 - 使用异步lookup调用
                        let mount_clone = self.mount.clone();
                        let current_fh_clone = current_fh.clone();
                        match mount_clone.lookup(current_fh_clone, dirname).await {
                            Ok(obj) => {
                                // 找到目录，继续使用现有句柄
                                current_fh = obj.fh.clone();
                                // 将查询结果保存到缓存中
                                trace!(
                                    "Inserting into global cache: key={:?}, value_len={}",
                                    cache_key,
                                    obj.fh.len()
                                );
                                GLOBAL_CACHE.insert(cache_key, obj.fh.clone());
                            }
                            Err(e) => {
                                let err_msg = format!("Failed to lookup directory {dirname}: {e}");
                                if is_retryable_with_invalidation(&e) && retries < MAX_STALE_RETRIES
                                {
                                    debug!(
                                        "Stale handle in create_dir_all lookup for {:?}, refreshing root_fh and retrying",
                                        relative_path
                                    );
                                    let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                                    self.maybe_refresh_root_fh(stale_gen).await?;
                                    let root_fh = self.get_root_fh();
                                    invalidate_path_cache(&components, &root_fh);
                                    return Box::pin(
                                        self.create_dir_all_inner(relative_path, retries + 1),
                                    )
                                    .await;
                                }
                                debug!("Error: {}", err_msg);
                                return Err(StorageError::NfsError(err_msg));
                            }
                        }
                    } else if is_retryable_with_invalidation(&e) && retries < MAX_STALE_RETRIES {
                        // Stale handle：缓存的 fh 在 NFS 服务器侧已失效，刷新 root_fh 后从根重试
                        debug!(
                            "Stale handle detected in create_dir_all for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        invalidate_path_cache(&components, &root_fh);
                        return Box::pin(self.create_dir_all_inner(relative_path, retries + 1))
                            .await;
                    } else {
                        // 其他错误，直接返回
                        return Err(StorageError::NfsError(e.to_string()));
                    }
                }
            }
        }

        Ok(current_fh)
    }

    async fn delete_dir(&self, relative_path: &Path) -> Result<()> {
        // 提取目录名
        let dirname = relative_path
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid directory path".to_string()))?
            .to_string_lossy()
            .to_string();

        // 查找父目录
        let parent_path = relative_path
            .parent()
            .ok_or_else(|| StorageError::InvalidPath("Cannot get parent directory".to_string()))?;

        for attempt in 0..=MAX_STALE_RETRIES {
            debug!(
                "Looking up parent path for remove dir {:?}: {:?}",
                relative_path, parent_path
            );
            let parent_obj = self.lookup_fh(parent_path).await?;

            // 删除目录
            match self.mount.rmdir(parent_obj.fh.clone(), &dirname).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // 目录已不存在：幂等语义，视为成功
                    if e.is_not_found() {
                        trace!(
                            "Directory {:?} already gone, treating as success",
                            relative_path
                        );
                        return Ok(());
                    }
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in delete_dir for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(relative_path)?;
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to remove directory: {e}"
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    pub async fn delete_dir_all(&self, relative_path: Option<&Path>) -> Result<()> {
        let iter = self.delete_dir_all_with_progress(relative_path, 4)?;
        while iter.next().await.is_some() {}
        Ok(())
    }

    pub fn delete_dir_all_with_progress(
        &self,
        relative_path: Option<&Path>,
        concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let (tx, rx) = async_channel::bounded::<DeleteEvent>(1000);
        let concurrency = concurrency.clamp(1, 64);
        let storage = self.clone();

        // 将 relative_path 转为 owned PathBuf 以便 move 进 async 块
        let sub_path = relative_path.map(std::path::Path::to_path_buf);

        tokio::spawn(async move {
            // 1. 复用现有 walkdir 并行遍历（支持子目录）
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

            // 2. 流式消费：边遍历边删文件，目录仅收集路径
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
                    StorageEntryMessage::Error {
                        event,
                        path,
                        reason,
                    } => {
                        error!(
                            "Walkdir error during delete [{}] {:?}: {}",
                            event, path, reason
                        );
                    }
                    _ => {}
                }
            }

            // 等待所有文件删除完成
            for h in file_handles {
                let _ = h.await;
            }

            // 3. 目录按深度降序排序 → 逐个删除（子目录）
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
            // 4. 删除根目录本身（walkdir 只返回其内容，不含根目录自身）
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
            // tx drop → channel 关闭
        });

        Ok(DeleteDirIterator::new(rx))
    }

    pub async fn create_symlink(
        &self,
        relative_path: &Path,
        target_path: &Path,
        atime: i64,
        mtime: i64,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<()> {
        // 提取链接文件名
        let link_filename = relative_path
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid link path".to_string()))?
            .to_string_lossy()
            .to_string();

        let target_path_str = target_path.to_string_lossy().to_string();

        // 确保父目录存在（只需一次，重试时只需刷新 fh）
        if let Some(parent) = relative_path.parent() {
            trace!(
                "Creating parent directory for symlink {:?}: {:?}",
                relative_path, parent
            );
            self.create_dir_all(parent).await?;
        }

        for attempt in 0..=MAX_STALE_RETRIES {
            let parent_fh = if let Some(parent) = relative_path.parent() {
                self.lookup_fh(parent).await?.fh
            } else {
                self.mount
                    .lookup_path("/")
                    .await
                    .map_err(|e| {
                        StorageError::NfsError(format!("Failed to lookup root directory: {e}"))
                    })?
                    .fh
            };

            // 创建符号链接
            match self
                .mount
                .symlink(&target_path_str, parent_fh.clone(), &link_filename)
                .await
            {
                Ok(symlink_obj) => {
                    let handle = NFSFileHandle::new(symlink_obj.fh, relative_path.to_path_buf());
                    return self
                        .set_metadata(&handle, Some(atime), Some(mtime), uid, gid, None)
                        .await;
                }
                Err(e) => {
                    // NFS4ERR_EXIST / NFS3ERR_EXIST：目标同名条目已存在，
                    // 先 REMOVE 再重建（全量同步语义：覆盖已有条目）
                    if e.is_exist() {
                        debug!(
                            "Symlink {:?} already exists, removing and recreating",
                            relative_path
                        );
                        self.mount
                            .remove(parent_fh.clone(), &link_filename)
                            .await
                            .map_err(|rm_err| {
                                StorageError::NfsError(format!(
                                    "Failed to remove existing entry {} before symlink recreation: {rm_err}",
                                    relative_path.display()
                                ))
                            })?;
                        // REMOVE 后直接重试创建
                        match self
                            .mount
                            .symlink(&target_path_str, parent_fh, &link_filename)
                            .await
                        {
                            Ok(symlink_obj) => {
                                let handle =
                                    NFSFileHandle::new(symlink_obj.fh, relative_path.to_path_buf());
                                return self
                                    .set_metadata(&handle, Some(atime), Some(mtime), uid, gid, None)
                                    .await;
                            }
                            Err(e2) => {
                                return Err(StorageError::NfsError(format!(
                                    "Failed to recreate symlink after remove: {e2}"
                                )));
                            }
                        }
                    }
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in create_symlink for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(relative_path)?;
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to create symlink: {e}"
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    pub async fn read_symlink(&self, relative_path: &Path) -> Result<PathBuf> {
        // 查找符号链接文件
        debug!("[read_link] Looking up symlink path: {:?}", relative_path);
        let obj = self.lookup_fh(relative_path).await?;

        // 读取符号链接目标 - 使用异步调用
        let target = self
            .mount
            .readlink(obj.fh.clone())
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to read symlink: {e}")))?;

        // 直接返回符号链接的原始目标路径，不进行路径验证
        Ok(PathBuf::from(target))
    }

    pub(crate) async fn commit(&self, file: &NFSFileHandle, offset: u64, count: u32) -> Result<()> {
        // NFS通常没有单独的sync操作，直接使用FileHandle中存储的storage实例
        self.mount
            .commit(file.inner.fh.clone(), offset, count)
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to commit file: {e}")))?;
        Ok(())
    }

    pub async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        trace!("Rename {:?} to {:?}", from, to);

        // 提取文件名（循环外，不依赖缓存）
        let from_filename = from
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid source path".to_string()))?
            .to_string_lossy()
            .to_string();
        let to_filename = to
            .file_name()
            .ok_or_else(|| StorageError::InvalidPath("Invalid destination path".to_string()))?
            .to_string_lossy()
            .to_string();

        // 确保目标目录存在（只需一次，重试时只需刷新 fh）
        if let Some(parent) = to.parent() {
            self.create_dir_all(parent).await?;
        }

        for attempt in 0..=MAX_STALE_RETRIES {
            // 查找源文件父目录
            let from_parent_obj = if let Some(parent) = from.parent() {
                debug!(
                    "Looking up parent path for rename source {:?}: {:?}",
                    from, parent
                );
                self.lookup_fh(parent).await?
            } else {
                trace!("Source parent directory {:?} is root directory", from);
                self.mount.lookup_path("/").await.map_err(|e| {
                    StorageError::NfsError(format!("Failed to lookup root directory: {e}"))
                })?
            };

            // 查找目标父目录
            let to_parent_obj = if let Some(parent) = to.parent() {
                debug!(
                    "Looking up parent path for rename target {:?}: {:?}",
                    to, parent
                );
                self.lookup_fh(parent).await?
            } else {
                trace!("Target parent directory {:?} is root directory", to);
                self.mount.lookup_path("/").await.map_err(|e| {
                    StorageError::NfsError(format!("Failed to lookup root directory: {e}"))
                })?
            };

            // 重命名文件
            match self
                .mount
                .rename(
                    from_parent_obj.fh.clone(),
                    &from_filename,
                    to_parent_obj.fh.clone(),
                    &to_filename,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in rename from {:?} to {:?}, refreshing root_fh and retrying",
                            from, to
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let from_components = Self::collect_components(from)?;
                        invalidate_path_cache(&from_components, &root_fh);
                        let to_components = Self::collect_components(to)?;
                        invalidate_path_cache(&to_components, &root_fh);
                        continue;
                    }
                    let error_msg = format!(
                        "Failed to rename file from {} to {}: {e}",
                        from.display(),
                        to.display()
                    );
                    error!("{}", error_msg);
                    return Err(StorageError::NfsError(error_msg));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    /// 获取文件元数据
    pub async fn get_metadata(&self, relative_path: &Path) -> Result<EntryEnum> {
        debug!("Looking up file path for metadata {:?}", relative_path);
        // 查找文件
        let obj = self.lookup_fh(relative_path).await?;

        // 获取属性 - 使用异步调用
        let mount_clone = self.mount.clone();
        let obj_fh_clone = obj.fh.clone();
        let attrs = match mount_clone.getattr(obj_fh_clone).await {
            Ok(attr) => attr,
            Err(e) => {
                return Err(StorageError::NfsError(format!(
                    "Failed to get file attributes: {e}"
                )));
            }
        };

        // 提取文件名 - 处理根目录的情况
        let filename = if relative_path.as_os_str().is_empty() || relative_path == Path::new("/") {
            // 根目录使用存储的根目录名或默认名称
            String::from("/")
        } else {
            relative_path
                .file_name()
                .ok_or_else(|| StorageError::InvalidPath("Invalid path".to_string()))?
                .to_string_lossy()
                .to_string()
        };

        // 提取扩展名 - 根目录没有扩展名
        let extension = relative_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(std::string::ToString::to_string);

        let storage_entry = EntryEnum::NAS(NASEntry::from_nfs_attrs(
            filename,
            relative_path.to_path_buf(),
            extension,
            &attrs,
            obj.fh,
            NfsEnrich::from_attrs(&attrs),
        ));

        Ok(storage_entry)
    }

    /// 更新文件或目录的元数据
    /// Update metadata for a file by path (public wrapper around `set_metadata`).
    pub async fn update_metadata(
        &self,
        relative_path: &Path,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        // setattr 在 NFSv4.1 中不需要 open stateid，直接用 lookup 获取 fh 即可，
        // 避免不必要的 OPEN/CLOSE 往返
        let obj = self.lookup_fh(relative_path).await?;
        let handle = NFSFileHandle::new(obj.fh, relative_path.to_path_buf());
        self.set_metadata(&handle, atime, mtime, uid, gid, mode)
            .await
    }

    #[allow(clippy::similar_names)]
    pub(crate) async fn set_metadata(
        &self,
        file: &NFSFileHandle,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        debug!("Setting metadata for {:?}", file.path);

        // 转换纳秒时间戳到Time类型
        let nfs_atime = atime.map(i64_to_time);
        let nfs_mtime = mtime.map(i64_to_time);

        // set_metadata 在 stale 重试时需要 re-lookup 获取新 fh，用 current_fh 跟踪
        let mut current_fh = file.inner.fh.clone();

        for attempt in 0..=MAX_STALE_RETRIES {
            match self
                .mount
                .setattr(
                    current_fh.clone(),
                    None,      // 不设置guard_ctime
                    mode,      // 设置mode
                    uid,       // 设置uid
                    gid,       // 设置gid
                    None,      // 不设置size
                    nfs_atime, // 设置atime
                    nfs_mtime, // 设置mtime
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in set_metadata for {:?}, re-looking up and retrying",
                            file.path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen).await?;
                        let root_fh = self.get_root_fh();
                        let components = Self::collect_components(&file.path)?;
                        invalidate_path_cache(&components, &root_fh);
                        // 重新 lookup 获取新的文件句柄
                        let fresh_obj = self.lookup_fh(&file.path).await?;
                        current_fh = fresh_obj.fh;
                        continue;
                    }
                    return Err(StorageError::NfsError(format!(
                        "Failed to set file metadata: {}, {e:?}",
                        file.path.display()
                    )));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    // ============================================================
    // NFSv4 ACL 操作
    // ============================================================

    /// 查询 NFS 版本
    pub fn version(&self) -> nfs_rs::NFSVersion {
        self.mount.version()
    }

    /// 检测是否支持 ACL（仅 `NFSv4`+）
    pub fn supports_acl(&self) -> bool {
        !matches!(
            self.mount.version(),
            nfs_rs::NFSVersion::NFSv3 | nfs_rs::NFSVersion::Unknown
        )
    }

    /// 检测是否支持 xattr（RFC 8276，需要 `NFSv4`+）。
    /// 与 `supports_acl` 共享相同的版本下限检查；如果服务器声称 `NFSv4` 但不支持
    /// xattr 扩展，`list_xattr` 会返回 Unsupported 错误，`copy_xattr` 会静默跳过。
    pub fn supports_xattr(&self) -> bool {
        self.supports_acl()
    }

    /// 获取文件/目录的 `NFSv4` ACL
    pub async fn get_acl(&self, relative_path: &Path) -> Result<nfs_rs::Acl> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.getacl(obj.fh).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to get ACL for {}: {e}",
                relative_path.display()
            ))
        })
    }

    /// 设置文件/目录的 `NFSv4` ACL
    pub async fn set_acl(&self, relative_path: &Path, acl: &nfs_rs::Acl) -> Result<()> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.setacl(obj.fh, acl).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to set ACL for {}: {e}",
                relative_path.display()
            ))
        })
    }

    /// 查询服务器支持的 ACE 类型
    pub async fn acl_support(&self) -> Result<nfs_rs::AclSupport> {
        self.mount
            .aclsupport(self.rpc_root_fh())
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to query ACL support: {e}")))
    }

    // ============================================================
    // NFSv4 Extended Attributes (xattr)
    // ============================================================

    /// 获取指定 xattr 的值
    pub async fn get_xattr(&self, relative_path: &Path, name: &str) -> Result<Bytes> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.getxattr(obj.fh, name).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to get xattr '{name}' for {}: {e}",
                relative_path.display()
            ))
        })
    }

    /// 设置 xattr 值
    pub async fn set_xattr(&self, relative_path: &Path, name: &str, value: Bytes) -> Result<()> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.setxattr(obj.fh, name, value).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to set xattr '{name}' for {}: {e}",
                relative_path.display()
            ))
        })
    }

    /// 列出所有 xattr 名称
    pub async fn list_xattr(&self, relative_path: &Path) -> Result<Vec<String>> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.listxattr(obj.fh).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to list xattr for {}: {e}",
                relative_path.display()
            ))
        })
    }

    /// 删除指定 xattr
    pub async fn remove_xattr(&self, relative_path: &Path, name: &str) -> Result<()> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.removexattr(obj.fh, name).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to remove xattr '{name}' for {}: {e}",
                relative_path.display()
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn walkdir(
        &self,
        sub_path: Option<&Path>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        packaged: bool,
        package_depth: usize,
    ) -> Result<WalkDirAsyncIterator> {
        // 解析起始目录：sub_path 为 None 时从根开始，否则从子目录开始
        let (start_fh, start_root) = match sub_path {
            Some(p) if !p.as_os_str().is_empty() => {
                let obj = self.lookup_fh(p).await?;
                // Windows 兼容：NFS root 始终用 /
                let sub_path_str = p.to_string_lossy().replace('\\', "/");
                let sub_root = Self::join_paths(&self.root, &sub_path_str);
                (obj.fh, sub_root)
            }
            _ => (self.rpc_root_fh(), (*self.root).clone()),
        };

        // 创建通道
        let (tx, rx) = async_channel::bounded(1000);

        // 创建全局文件计数器
        let total_file_count = Arc::new(AtomicUsize::new(0));

        let max_depth = depth.unwrap_or(0);

        let total_file_count_clone = total_file_count.clone();

        let storage = self.clone();

        // 克隆sender，因为它将在异步任务中使用
        let tx_clone = tx.clone();

        // 创建异步任务来执行目录遍历
        tokio::spawn(async move {
            if let Err(err) = storage
                .iterative_walkdir(
                    &start_root,
                    start_fh,
                    tx_clone.clone(),
                    max_depth,
                    match_expressions.as_ref(),
                    exclude_expressions.as_ref(),
                    concurrency,
                    total_file_count_clone,
                    packaged,
                    package_depth,
                )
                .await
            {
                error!("Error during directory traversal: {}", err);
                let _ = tx_clone
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: std::path::PathBuf::new(),
                        reason: format!("{err}"),
                    })
                    .await;
            }
        });

        Ok(WalkDirAsyncIterator::new(rx))
    }

    /// 迭代式目录遍历函数，使用工作窃取队列实现高效并发
    #[allow(clippy::too_many_arguments)]
    async fn iterative_walkdir(
        &self,
        root_path: &str,
        root_fh: Bytes,
        tx: async_channel::Sender<StorageEntryMessage>,
        max_depth: usize,
        match_expressions: Option<&FilterExpression>,
        exclude_expressions: Option<&FilterExpression>,
        concurrency: usize,
        total_file_count: Arc<AtomicUsize>,
        packaged: bool,
        package_depth: usize,
    ) -> Result<()> {
        let contexts = create_worker_contexts(
            concurrency,
            (root_path.to_string(), root_fh, 0usize, true, None::<usize>),
        )
        .await;
        let match_expr = Arc::new(match_expressions.cloned());
        let exclude_expr = Arc::new(exclude_expressions.cloned());

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
                    |(dir_path, dir_fh, current_depth, skip_filter, package_remaining)| {
                        self_clone.process_dir(
                            ctx.worker_id,
                            dir_path,
                            dir_fh,
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

    /// 处理单个目录，读取条目并过滤，发送符合条件的StorageEntry
    #[allow(clippy::too_many_arguments)]
    async fn process_dir(
        &self,
        producer_id: usize,
        dir_path: String,
        dir_fh: Bytes,
        current_depth: usize,
        tx: &async_channel::Sender<StorageEntryMessage>,
        ctx: &crate::walk_scheduler::WorkerContext<(String, Bytes, usize, bool, Option<usize>)>,
        match_expr: &Arc<Option<FilterExpression>>,
        exclude_expr: &Arc<Option<FilterExpression>>,
        max_depth: usize,
        total_file_count: &Arc<AtomicUsize>,
        skip_filter: bool,
        packaged: bool,
        package_depth: usize,
        package_remaining: Option<usize>,
    ) -> Result<()> {
        // 调用readdirplus获取目录条目流 - 使用异步调用
        // 先把整个目录的 readdirplus 结果完整收集到 Vec，再处理。
        // 这是为了在流中途遇到瞬时错误（NFS4ERR_DELAY 在 nfs-rs 内部重试耗尽，
        // 或 STALE/BADHANDLE 等需 root_fh 刷新的错误）时可以**整体重试 readdirplus**
        // 而不会向下游 channel 重复发送已发条目（流式直发 + 重试 = 重复 entry，
        // 进而导致 ClickHouse 行数 / 统计计数偏离实际）。
        //
        // 内存代价：单目录 readdirplus 全部 entries 缓冲。典型 < 10k entry，
        // 单 entry ~500B，最大 ~5MB/worker，可接受。
        let entries: Vec<_> = {
            let mount_clone = self.mount.clone();
            let mut retries: u8 = 0;
            loop {
                let mut dir_stream = mount_clone.readdirplus(dir_fh.clone()).await;
                let mut buf: Vec<nfs_rs::ReaddirplusEntry> = Vec::new();
                let mut transient_err: Option<NfsError> = None;
                while let Some(entry_result) = dir_stream.next().await {
                    match entry_result {
                        Ok(e) => buf.push(e),
                        Err(e) => {
                            // 流式 readdirplus 中途出错的重试条件（同 read_dir_sorted）
                            if retries < MAX_STALE_RETRIES
                                && (is_retryable_with_invalidation(&e) || is_server_busy(&e))
                            {
                                transient_err = Some(e);
                                break;
                            }
                            // 非可重试错误：上报但不丢弃整个目录（保留部分结果向下游发送）
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
                        }
                    }
                }
                drop(dir_stream);
                if let Some(e) = transient_err {
                    retries += 1;
                    warn!(
                        "[Producer {}] readdirplus on {} hit transient error (retry {}/{}): {}",
                        producer_id, dir_path, retries, MAX_STALE_RETRIES, e
                    );
                    // STALE/BADHANDLE/NOENT 类需要刷新 root_fh；DELAY 类直接重试即可
                    if is_retryable_with_invalidation(&e) {
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        let _ = self.maybe_refresh_root_fh(stale_gen).await;
                        // 注意：这里不刷新 dir_fh 本身（无父路径上下文），依赖 root_fh 刷新
                        // 修复跨 export remount 场景，stale-handle 多次重试可能仍失败时由 errors 报告
                    }
                    // 给服务端一点缓冲时间
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                break buf;
            }
        };

        // 流式处理每个目录条目（已完整收集，无重复风险）
        for entry in entries {
            if entry.file_name == "." || entry.file_name == ".." {
                continue;
            }

            // 构建完整路径（使用 '/' 拼接，避免平台差异）
            let relative_path = self.build_relative_path(&dir_path, &entry.file_name);

            // 提取扩展名（在 file_name 被 move 前提取）
            let extension = entry
                .file_name
                .rsplit_once('.')
                .map(|(_, ext)| ext.to_string());

            // 提取文件名（完整文件名）
            let file_name = &entry.file_name;

            let attrs = if let Some(attr) = entry.attr {
                attr
            } else {
                // Fallback: try standalone GETATTR when inline attrs from READDIRPLUS failed
                warn!(
                    "[Producer {}] Missing inline attrs for {}, trying fallback GETATTR",
                    producer_id, relative_path
                );
                match self.mount.getattr_path(&relative_path).await {
                    Ok(attr) => attr,
                    Err(e) => {
                        error!(
                            "[Producer {}] Fallback GETATTR also failed for {}: {}",
                            producer_id, relative_path, e
                        );
                        let _ = tx
                            .send(StorageEntryMessage::Error {
                                event: ErrorEvent::Scan,
                                path: PathBuf::from(&relative_path),
                                reason: format!("[Producer {producer_id}] Missing file attributes: {relative_path}"),
                            })
                            .await;
                        continue;
                    }
                }
            };

            let file_type = attrs.type_;
            let is_dir = file_type == FType3::NF3DIR as u32;
            let is_symlink = file_type == FType3::NF3LNK as u32;

            // 一次性过滤：基于文件名、路径、文件类型、大小和修改时间
            let (skip_entry, continue_scan, need_submatch) = if skip_filter {
                // 获取修改时间的 epoch seconds
                let modified_epoch = Some(i64::from(attrs.mtime.seconds));

                // root 已为空串或无前导 '/' 前缀，relative_path 直接就是相对路径
                let normalized_path = &relative_path;
                should_skip(
                    match_expr.as_ref().as_ref(),
                    exclude_expr.as_ref().as_ref(),
                    Some(file_name),
                    Some(normalized_path),
                    Some(if is_symlink {
                        "symlink"
                    } else if is_dir {
                        "dir"
                    } else {
                        "file"
                    }),
                    modified_epoch,
                    Some(attrs.filesize),
                    extension.as_deref().or(Some("")),
                )
            } else {
                // skip_filter=false 表示父目录已匹配，子项无需过滤
                // need_submatch=false 确保免过滤传递给所有后代
                (false, true, false)
            };

            // 计算条目的实际深度：目录深度+1
            let entry_depth = current_depth + 1;
            let mut send_packaged = false;

            // package 深度追踪模式：只处理目录，跳过文件和 filter
            if let Some(remaining) = package_remaining {
                if !is_dir {
                    continue;
                }
                if remaining > 1 {
                    ctx.push_task((
                        relative_path.clone(),
                        entry.handle.clone(),
                        current_depth + 1,
                        false,
                        Some(remaining - 1),
                    ))
                    .await;
                    continue;
                }
                send_packaged = true;
            }

            if !send_packaged && skip_entry {
                if continue_scan && is_dir && (current_depth < max_depth || max_depth == 0) {
                    ctx.push_task((
                        relative_path.clone(),
                        entry.handle.clone(),
                        current_depth + 1,
                        need_submatch,
                        None,
                    ))
                    .await;
                }
                continue;
            }

            // 创建StorageEntry
            // Read xattrs if supported (before consuming entry.file_name)
            let xattrs = if self.supports_xattr() {
                let path = Path::new(&relative_path);
                match self.list_xattr(path).await {
                    Ok(names) if !names.is_empty() => {
                        let mut pairs = Vec::with_capacity(names.len());
                        for name in &names {
                            match self.get_xattr(path, name).await {
                                Ok(value) => pairs.push((name.clone(), value.to_vec())),
                                Err(e) => warn!(
                                    "[Producer {}] Failed to read xattr '{}' for {}: {}",
                                    producer_id, name, relative_path, e
                                ),
                            }
                        }
                        if pairs.is_empty() { None } else { Some(pairs) }
                    }
                    Ok(_) => None,
                    Err(e) => {
                        trace!(
                            "[Producer {}] listxattr for {} failed: {}",
                            producer_id, relative_path, e
                        );
                        None
                    }
                }
            } else {
                None
            };

            let storage_entry = EntryEnum::NAS(NASEntry::from_nfs_attrs(
                entry.file_name,
                PathBuf::from(&relative_path),
                extension.clone(),
                &attrs,
                entry.handle.clone(),
                NfsEnrich::from_attrs(&attrs).with_xattrs(xattrs),
            ));

            // packaged 模式：目录匹配 DirDate 条件时决定打包策略
            if !send_packaged
                && packaged
                && is_dir
                && dir_matches_date_filter(match_expr.as_ref().as_ref(), storage_entry.get_name())
            {
                if max_depth > 0 && entry_depth + package_depth > max_depth {
                    continue;
                }
                if package_depth > 0 {
                    ctx.push_task((
                        relative_path.clone(),
                        entry.handle.clone(),
                        current_depth + 1,
                        false,
                        Some(package_depth),
                    ))
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
                    error!(
                        "[Producer {}] Output channel closed, stopping processing",
                        producer_id
                    );
                    break;
                }
                continue;
            }

            // 如果是目录且未达到最大深度，将其添加到任务队列
            if is_dir && (current_depth < max_depth || max_depth == 0) {
                ctx.push_task((
                    relative_path.clone(),
                    entry.handle.clone(),
                    current_depth + 1,
                    need_submatch,
                    None,
                ))
                .await;
            }

            // 检查深度限制：只有当条目深度在允许范围内时才发送
            // 0表示无限深度
            if max_depth == 0 || entry_depth <= max_depth {
                // 更新全局文件计数器
                total_file_count.fetch_add(1, Ordering::Relaxed);

                // 发送StorageEntry到输出通道
                if tx
                    .send(StorageEntryMessage::Scanned(Arc::new(storage_entry)))
                    .await
                    .is_err()
                {
                    error!(
                        "[Producer {}] Output channel closed, stopping processing",
                        producer_id
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn read(&self, file: &mut NFSFileHandle, offset: u64, count: usize) -> Result<Bytes> {
        let mut retry_count = 0;
        let max_retries = 1;
        let mount = self.mount.clone();
        let file_fh = file.inner.fh.clone();

        let mut result = mount
            .read(file_fh, offset, count as u32)
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to read file: {e}")));

        // 检查是否需要刷新文件句柄
        while result.is_err() && retry_count < max_retries {
            if let Err(StorageError::NfsError(msg)) = &result {
                // 检查错误信息是否包含"invalid file handle"
                if msg.contains("invalid file handle") {
                    debug!(
                        "[read] File handle is invalid, refreshing for path: {:?}",
                        file.path
                    );
                    retry_count += 1;

                    // 重新lookup文件获取新的文件句柄
                    debug!("[read] Looking up file path: {:?}", file.path);
                    let obj = self.lookup_fh(&file.path).await?;

                    // 更新文件句柄
                    file.inner = Arc::new(obj);
                    debug!(
                        "[read] Successfully refreshed file handle for path: {:?}",
                        file.path
                    );

                    // 重新尝试读取
                    let mount = self.mount.clone();
                    let file_fh = file.inner.fh.clone();
                    result = mount
                        .read(file_fh, offset, count as u32)
                        .await
                        .map_err(|e| {
                            StorageError::NfsError(format!(
                                "Failed to read file after refresh: {e}"
                            ))
                        });
                } else {
                    // 不是文件句柄无效的错误，直接返回原始错误
                    break;
                }
            } else {
                // 不是NfsError，直接返回
                break;
            }
        }

        result
    }

    async fn write(&self, file: &mut NFSFileHandle, offset: u64, data: Bytes) -> Result<u64> {
        let length = data.len() as u64;
        let chunk_size = self.calculate_chunk_size(length);
        let mut total_written = 0;

        // 如果数据块大小大于chunk_size，需要分拆处理
        if length > chunk_size {
            trace!(
                "[write] Chunk size {} exceeds limit {}, splitting into smaller chunks",
                length, chunk_size
            );

            // 分拆数据块并逐个写入
            let mut current_offset = offset;
            let mut remaining_bytes = length;
            let mut data_index = 0;

            // 获取数据的引用，避免不必要的克隆
            let data_ref = &data;

            while remaining_bytes > 0 {
                trace!("[write] Remaining bytes: {}", remaining_bytes);

                let current_chunk_size = std::cmp::min(remaining_bytes, chunk_size);

                // 直接使用slice操作，避免克隆
                let chunk_data =
                    data_ref.slice(data_index..data_index + current_chunk_size as usize);

                trace!(
                    "[write] Writing chunk of {} bytes to file {:?} at offset {}",
                    current_chunk_size, file.path, current_offset
                );

                // 写入数据到NFS - 使用异步调用
                let written = self
                    .mount
                    .write(file.inner.fh.clone(), current_offset, chunk_data)
                    .await
                    .map_err(|e| StorageError::NfsError(format!("Failed to write file: {e}")))?;

                trace!(
                    "[write] Committing chunk of {} bytes to file {:?} at offset {}",
                    current_chunk_size, file.path, written
                );

                // Commit数据 - 使用异步调用
                self.mount
                    .commit(file.inner.fh.clone(), current_offset, written)
                    .await
                    .map_err(|e| StorageError::NfsError(format!("Failed to commit file: {e}")))?;

                trace!(
                    "[write] Wrote split chunk of {} bytes to file {:?} at offset {}",
                    written, file.path, current_offset
                );

                total_written += u64::from(written);
                current_offset += u64::from(written);
                remaining_bytes -= u64::from(written);
                data_index += written as usize;
            }
        } else {
            // 数据块大小在限制内，直接写入
            // 写入数据到NFS - 使用异步调用
            trace!(
                "[write] Writing {} bytes to file {:?} at offset {}",
                length, file.path, offset
            );
            let written = self
                .mount
                .write(file.inner.fh.clone(), offset, data)
                .await
                .map_err(|e| StorageError::NfsError(format!("Failed to write file: {e}")))?;

            // Commit数据 - 使用异步调用
            self.mount
                .commit(file.inner.fh.clone(), offset, written as u32)
                .await
                .map_err(|e| StorageError::NfsError(format!("Failed to commit file: {e}")))?;
            trace!(
                "[write] Wrote {} bytes to file {:?} at offset {}",
                written, file.path, offset
            );
            total_written = u64::from(written);
        }

        Ok(total_written)
    }

    /// 处理单个文件或目录的复制
    /// 根据文件大小计算合适的块大小并记录大文件日志
    #[inline]
    fn calculate_chunk_size(&self, file_size: u64) -> u64 {
        // 根据文件大小动态调整块大小，优化内存使用
        // chunk size最小为一个字节，最大为2MB
        std::cmp::min(file_size, self.config.block_size).max(1)
    }

    /// 计算相对于 root 的路径
    fn calculate_relative_path(&self, relative_path: &Path) -> PathBuf {
        strip_root_prefix(&self.root, relative_path)
    }

    /// 安全地拼接两个路径部分，避免重复的 '/'
    fn join_paths(base: &str, suffix: &str) -> String {
        join_nfs_paths(base, suffix)
    }

    /// 构建完整路径（使用 '/' 拼接，避免平台差异）
    fn build_relative_path(&self, dir_path: &str, entry_file_name: &str) -> String {
        build_relative_path_impl(&self.root, dir_path, entry_file_name)
    }

    pub(crate) async fn read_data(
        &self,
        tx: mpsc::Sender<DataChunk>,
        relative_path: &Path,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        // 如果文件大小为0，直接返回
        if size == 0 {
            trace!("File {:?} is empty, skipping read", relative_path);
            return Ok(None);
        }

        let chunk_size = self.calculate_chunk_size(size);
        trace!(
            "Starting read_data_task for file {:?}, size: {}, chunk_size: {}",
            relative_path, size, chunk_size
        );

        // 确保相对路径不包含 root 前缀
        let path_to_use = self.calculate_relative_path(relative_path);

        // 通过 open 打开文件（v4.1 建立 stateid）
        let mut handler = match self.open(&path_to_use, OPEN_READ).await {
            Ok(handle) => {
                trace!("Successfully opened source file: {:?}", path_to_use);
                handle
            }
            Err(e) => {
                error!("Failed to open source file: {:?}", e);
                return Err(StorageError::NfsError(format!(
                    "Failed to open source file: {e:?}"
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
                trace!(
                    "QoS acquired {} bytes for file {:?}",
                    chunk_size, relative_path
                );
            }

            let data = match self.read(&mut handler, offset, chunk_size as usize).await {
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
                trace!("Reached end of file {:?}", relative_path);
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

            // 直接发送DataChunk，不使用Box
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
                trace!("Completed reading file {:?}", relative_path);
                break;
            }
        }

        // 关闭文件（v3 no-op，v4.1 释放 stateid）
        let _ = self.close(&handler).await;

        trace!(
            "Finished read_data_task for file {:?}, total bytes processed: {}",
            relative_path, bytes_read
        );

        Ok(hasher)
    }

    pub(crate) async fn write_data(
        &self,
        rx: mpsc::Receiver<DataChunk>,
        relative_path: &Path,
        #[allow(unused)] uid: Option<u32>,
        #[allow(unused)] gid: Option<u32>,
        #[allow(unused)] mode: Option<u32>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        trace!("Starting write_data_task for file {:?}", relative_path);

        let mut reader = rx;

        // 确保相对路径不包含 root 前缀
        let path_to_use = self.calculate_relative_path(relative_path);

        let mut dest_file = self.create_file(&path_to_use, uid, gid, mode).await?;

        trace!("Opened destination file {:?} for writing", relative_path);

        // Truncate file to 0 before writing chunks to handle the case where new content is shorter
        // This is critical for incremental sync where Changed files may have different sizes
        if let Err(e) = self.truncate_file(&dest_file).await {
            error!("Failed to truncate file {:?}: {:?}", relative_path, e);
            let _ = self.close(&dest_file).await;
            return Err(e);
        }

        // 用 async block 包裹写入循环，确保无论成功或失败都能执行 close
        let write_result = async {
            while let Some(chunk) = reader.recv().await {
                let offset = chunk.offset;
                let data = chunk.data;

                let length = data.len() as u64;
                trace!(
                    "Received chunk of {} bytes at offset {} for file {:?}",
                    length, offset, relative_path
                );

                self.write(&mut dest_file, offset, data).await?;
                if let Some(ref c) = bytes_counter {
                    c.fetch_add(length, Ordering::Relaxed);
                }
            }
            Ok(())
        }
        .await;

        // 关闭文件（v3 no-op，v4.1 释放 stateid），无论写入是否成功
        let _ = self.close(&dest_file).await;

        trace!("Finished write_data_task for file {:?}", relative_path);
        write_result
    }

    // ============================================================
    // walkdir_2: 目录分页 + NDX 编号 + 并行预读
    // ============================================================

    /// 读取单个 NFS 目录，返回排序后的 files + subdirs。
    pub(crate) async fn read_dir_sorted(
        &self,
        dir_path: &str,
        handle: &crate::dir_tree::DirHandle,
        ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult> {
        use crate::dir_tree::{DirHandle, ReadResult, SubdirEntry};

        let (fh, nfs_dir_path) = match handle {
            DirHandle::Nfs { fh, path } => (fh.clone(), path.clone()),
            _ => {
                return Err(StorageError::OperationError(
                    "DirHandle type mismatch: expected Nfs".into(),
                ));
            }
        };

        let mut files: Vec<Arc<EntryEnum>> = Vec::new();
        let mut subdirs: Vec<SubdirEntry> = Vec::new();
        let mut errors: Vec<String> = Vec::new();

        // readdirplus（带 stale handle 重试）
        let mut retries: u8 = 0;
        let mut current_fh = fh;
        loop {
            let mount_clone = self.mount.clone();
            let mut dir_stream = mount_clone.readdirplus(current_fh.clone()).await;
            let mut stale = false;

            while let Some(entry_result) = dir_stream.next().await {
                let entry = match entry_result {
                    Ok(e) => e,
                    Err(e) => {
                        let err_msg = e.to_string();
                        // 流式 readdirplus 中途出错的重试条件：
                        //   - is_retryable_with_invalidation: STALE/BADHANDLE/NOENT，刷新 root_fh 后从头重试
                        //   - is_server_busy: NFS4ERR_DELAY 在 nfs-rs 内部重试耗尽后仍未恢复，
                        //                     此时延时后从头 readdirplus 比"丢弃失败 entry"安全
                        //                     （后者导致计数 silent data loss）
                        if retries < MAX_STALE_RETRIES
                            && (is_retryable_with_invalidation(&e) || is_server_busy(&e))
                        {
                            stale = true;
                            break;
                        }
                        errors.push(format!("readdirplus error: {err_msg}"));
                        continue;
                    }
                };

                if entry.file_name == "." || entry.file_name == ".." {
                    continue;
                }

                let Some(attrs) = entry.attr else {
                    errors.push(format!("{}: missing attributes", entry.file_name));
                    continue;
                };

                let is_dir = attrs.type_ == FType3::NF3DIR as u32;
                let is_symlink = attrs.type_ == FType3::NF3LNK as u32;
                let relative_path = self.build_relative_path(&nfs_dir_path, &entry.file_name);
                let extension = entry
                    .file_name
                    .rsplit_once('.')
                    .map(|(_, ext)| ext.to_string());

                // filter（仅当 apply_filter=true 时）
                let (skip_entry, continue_scan, need_submatch) = if ctx.apply_filter {
                    should_skip(
                        ctx.match_expr.as_ref().as_ref(),
                        ctx.exclude_expr.as_ref().as_ref(),
                        Some(&entry.file_name),
                        Some(&relative_path),
                        Some(if is_symlink {
                            "symlink"
                        } else if is_dir {
                            "dir"
                        } else {
                            "file"
                        }),
                        Some(i64::from(attrs.mtime.seconds)),
                        Some(attrs.filesize),
                        extension.as_deref().or(Some("")),
                    )
                } else {
                    (false, true, false)
                };

                if skip_entry {
                    if is_dir
                        && continue_scan
                        && (ctx.max_depth == 0 || ctx.current_depth + 1 < ctx.max_depth)
                    {
                        let nas = NASEntry::from_nfs_attrs(
                            entry.file_name,
                            PathBuf::from(&relative_path),
                            extension,
                            &attrs,
                            entry.handle.clone(),
                            NfsEnrich::default(),
                        );
                        subdirs.push(SubdirEntry {
                            entry: Arc::new(EntryEnum::NAS(nas)),
                            visible: false,
                            need_filter: need_submatch,
                        });
                    }
                    continue;
                }

                let nas = NASEntry::from_nfs_attrs(
                    entry.file_name,
                    PathBuf::from(&relative_path),
                    extension,
                    &attrs,
                    entry.handle.clone(),
                    NfsEnrich::default(),
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

            if stale && retries < MAX_STALE_RETRIES {
                retries += 1;
                let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                self.maybe_refresh_root_fh(stale_gen).await?;
                let root_fh = self.get_root_fh();
                let path_components: Vec<String> = nfs_dir_path
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .map(std::string::ToString::to_string)
                    .collect();
                invalidate_path_cache(&path_components, &root_fh);
                match self.lookup_fh(Path::new(&nfs_dir_path)).await {
                    Ok(obj) => {
                        current_fh = obj.fh;
                        files.clear();
                        subdirs.clear();
                        errors.clear();
                        continue;
                    }
                    Err(e) => {
                        errors.push(format!("stale handle retry failed: {e:?}"));
                        break;
                    }
                }
            }
            break;
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
    pub async fn walkdir_2(
        &self,
        sub_path: Option<&Path>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
    ) -> Result<crate::WalkDirAsyncIterator2> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        // 解析起始路径的文件句柄
        let (start_fh, start_nfs_path) = match sub_path {
            Some(p) if !p.as_os_str().is_empty() => {
                let nfs_path = p.to_string_lossy().replace('\\', "/");
                let obj = self.lookup_fh(Path::new(&nfs_path)).await?;
                (obj.fh, nfs_path)
            }
            _ => (self.rpc_root_fh(), self.root.as_ref().clone()),
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
                    let result = storage
                        .read_dir_sorted(&req.dir_path, &req.handle, &req.ctx)
                        .await;
                    let _ = req.reply.send(result);
                }
            });
        }

        let root_handle = DirHandle::Nfs {
            fh: start_fh,
            path: start_nfs_path,
        };
        // root_path 用于 extract_dir_handle，NFS 不需要拼绝对路径，传空即可
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

        tokio::spawn(run_dfs_driver(
            req_tx,
            out_tx,
            root_path,
            root_handle,
            base_ctx,
        ));

        Ok(crate::AsyncReceiver::new(out_rx))
    }
}

/// 创建 NFS 存储实例
///
/// 该函数用于创建 NFS 存储实例，并包装为 `StorageEnum`。
///
/// # 参数
/// - `url`：NFS URL 字符串
/// - `block_size`：可选的块大小，默认值为 2MB
///
/// # 返回值
/// - `Ok(StorageEnum)`：创建成功，返回 `StorageEnum::NFS` 实例
/// - `Err(StorageError)`：创建失败，返回错误信息
pub async fn create_nfs_storage(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let nfs_storage = NFSStorage::new(url, block_size).await?;
    Ok(StorageEnum::NFS(nfs_storage))
}

/// 创建 NFS 目标存储实例，如果 prefix 目录不存在则自动创建
pub async fn create_nfs_storage_ensuring_dir(
    url: &str,
    block_size: Option<u64>,
) -> Result<StorageEnum> {
    let (mut storage, root_dir) = NFSStorage::mount_and_build(url, block_size).await?;

    if !root_dir.is_empty() {
        let path = Path::new(&root_dir);
        if let Ok(obj_res) = storage.lookup_fh(path).await {
            *storage.root_fh.write().map_err(|_| root_fh_lock_err())? = obj_res.fh;
            storage.root = Arc::new(root_dir);
        } else {
            // prefix 目录不存在，创建之
            info!("NFS prefix '{}' does not exist, creating it", root_dir);
            let fh = storage.create_dir_all(path).await.map_err(|e| {
                StorageError::OperationError(format!(
                    "Failed to create NFS prefix '{root_dir}': {e}"
                ))
            })?;
            *storage.root_fh.write().map_err(|_| root_fh_lock_err())? = fh;
            storage.root = Arc::new(root_dir);
        }
    }

    Ok(StorageEnum::NFS(storage))
}

impl Drop for NFSStorage {
    fn drop(&mut self) {
        // 只在最后一个持有者 drop 时才发 UMNT。
        // NFSStorage 通过 Arc<Box<dyn Mount>> 共享连接，提前 umount 会摧毁其他 clone 正在使用的连接。
        // strong_count == 1 表示当前 self 是唯一持有者（clone() 前的最后一个引用），
        // clone 后 count 会变为 2 但 spawn 的 task 立即持有 → mount 不会被真正释放。
        if Arc::strong_count(&self.mount) == 1 {
            let mount = self.mount.clone();
            // UMNT 是 advisory（RFC 1813），fire-and-forget 即可。
            // try_current() 在 runtime 已关闭时返回 Err，静默跳过。
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = mount.umount().await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nfs_url() {
        // 测试标准格式：nfs://server/path:root_dir
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path:root_dir").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "root_dir");

        // 测试没有根目录的格式：nfs://server/path
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "");

        // 测试带有查询参数的格式
        let (nfs_url, root_dir) =
            NFSStorage::parse_nfs_url("nfs://server/path:root_dir?foo=bar").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?foo=bar&uid=0&gid=0");
        assert_eq!(root_dir, "root_dir");

        // 测试带有 uid 参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path?uid=1000").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?uid=1000&gid=0");
        assert_eq!(root_dir, "");

        // 测试带有 gid 参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path?gid=1000").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?gid=1000&uid=0");
        assert_eq!(root_dir, "");

        // 测试带有 uid 和 gid 参数的格式
        let (nfs_url, root_dir) =
            NFSStorage::parse_nfs_url("nfs://server/path?uid=1000&gid=1000").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?uid=1000&gid=1000");
        assert_eq!(root_dir, "");

        // 测试没有路径的格式：nfs://server
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server").unwrap();
        assert_eq!(nfs_url, "nfs://server/?uid=0&gid=0");
        assert_eq!(root_dir, "");

        // 测试带前导斜杠的 root_dir
        let (nfs_url, root_dir) =
            NFSStorage::parse_nfs_url("nfs://server/path:/prefix/dir").unwrap();
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "prefix/dir");

        // 测试无效格式（非 nfs:// 开头）
        let result = NFSStorage::parse_nfs_url("http://server/path");
        assert!(result.is_err());
    }

    // --- strip_root_prefix 测试 ---

    #[test]
    fn test_strip_root_prefix_empty_root() {
        // root 为空时直接返回原路径
        let result = strip_root_prefix("", Path::new("some/file.txt"));
        assert_eq!(result, PathBuf::from("some/file.txt"));
    }

    #[test]
    fn test_strip_root_prefix_with_root_prefix() {
        // 路径包含 root 前缀时正确剥离
        let result = strip_root_prefix("prefix/dir", Path::new("prefix/dir/file.txt"));
        assert_eq!(result, PathBuf::from("file.txt"));
    }

    #[test]
    fn test_strip_root_prefix_nested() {
        // 多层嵌套路径
        let result = strip_root_prefix("data", Path::new("data/sub/deep/file.txt"));
        assert_eq!(result, PathBuf::from("sub/deep/file.txt"));
    }

    #[test]
    fn test_strip_root_prefix_without_root_prefix() {
        // 路径不含 root 前缀时直接返回（最常见场景）
        let result = strip_root_prefix("prefix", Path::new("other/file.txt"));
        assert_eq!(result, PathBuf::from("other/file.txt"));
    }

    #[test]
    fn test_strip_root_prefix_partial_match() {
        // root="ab", path="abc/d" → 不应误匹配（Path::strip_prefix 按组件比较）
        let result = strip_root_prefix("ab", Path::new("abc/d"));
        assert_eq!(result, PathBuf::from("abc/d"));
    }

    #[test]
    fn test_strip_root_prefix_exact_root() {
        // 路径恰好等于 root
        let result = strip_root_prefix("prefix", Path::new("prefix"));
        assert_eq!(result, PathBuf::from(""));
    }

    // --- build_relative_path_impl 测试 ---

    #[test]
    fn test_build_relative_path_at_root() {
        // dir_path == root → 返回纯文件名
        let result = build_relative_path_impl("data/prefix", "data/prefix", "file.txt");
        assert_eq!(result, "file.txt");
    }

    #[test]
    fn test_build_relative_path_under_root() {
        // dir_path 在 root 下 → 剥离前缀后拼接文件名
        let result = build_relative_path_impl("data", "data/subdir", "file.txt");
        assert_eq!(result, "subdir/file.txt");
    }

    #[test]
    fn test_build_relative_path_deep_under_root() {
        // 深层嵌套
        let result = build_relative_path_impl("data", "data/a/b", "file.txt");
        assert_eq!(result, "a/b/file.txt");
    }

    #[test]
    fn test_build_relative_path_empty_root_with_dir() {
        // root="" 且 dir_path 非空 → 直接拼接
        let result = build_relative_path_impl("", "subdir", "file.txt");
        assert_eq!(result, "subdir/file.txt");
    }

    #[test]
    fn test_build_relative_path_empty_root_empty_dir() {
        // root="" 且 dir_path="" → 返回纯文件名
        let result = build_relative_path_impl("", "", "file.txt");
        assert_eq!(result, "file.txt");
    }

    #[test]
    fn test_build_relative_path_no_root_match() {
        // dir_path 不匹配 root（递归层常见路径）
        let result = build_relative_path_impl("data", "subdir", "file.txt");
        assert_eq!(result, "subdir/file.txt");
    }

    #[test]
    fn test_build_relative_path_partial_root_match() {
        // root="ab", dir="abc" → 不应误剥离（目录边界检查）
        let result = build_relative_path_impl("ab", "abc", "file.txt");
        assert_eq!(result, "abc/file.txt");
    }

    #[test]
    fn test_build_relative_path_root_with_trailing_slash_in_dir() {
        // dir_path = "root/" 的边界情况（strip_prefix root 后剩 "/"）
        let result = build_relative_path_impl("data", "data/", "file.txt");
        assert_eq!(result, "file.txt");
    }

    // --- join_nfs_paths 测试 ---

    #[test]
    fn test_join_nfs_paths_basic() {
        assert_eq!(join_nfs_paths("base", "suffix"), "base/suffix");
    }

    #[test]
    fn test_join_nfs_paths_empty_base() {
        assert_eq!(join_nfs_paths("", "suffix"), "suffix");
    }

    #[test]
    fn test_join_nfs_paths_empty_suffix() {
        assert_eq!(join_nfs_paths("base", ""), "base");
    }

    #[test]
    fn test_join_nfs_paths_both_empty() {
        assert_eq!(join_nfs_paths("", ""), "");
    }

    #[test]
    fn test_join_nfs_paths_trailing_leading_slashes() {
        // 避免重复 '/'
        assert_eq!(join_nfs_paths("base/", "/suffix"), "base/suffix");
    }

    #[test]
    fn test_join_nfs_paths_multiple_slashes() {
        assert_eq!(join_nfs_paths("base///", "///suffix"), "base/suffix");
    }

    // --- is_stale_handle 测试（仅 STALE/BADHANDLE，不再包含 NOENT）---

    #[test]
    fn test_is_stale_handle_nfs3err_stale() {
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_STALE);
        assert!(is_stale_handle(&err));
    }

    #[test]
    fn test_is_stale_handle_nfs3err_badhandle() {
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_BADHANDLE);
        assert!(is_stale_handle(&err));
    }

    #[test]
    fn test_is_stale_handle_nfs4err_stale() {
        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_STALE);
        assert!(is_stale_handle(&err));
    }

    #[test]
    fn test_is_stale_handle_nfs4err_badhandle() {
        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_BADHANDLE);
        assert!(is_stale_handle(&err));
    }

    #[test]
    fn test_is_stale_handle_excludes_noent() {
        // NOENT 不属于 stale handle（语义上是终态"对象不存在"，不是"句柄陈旧"）
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_NOENT);
        assert!(!is_stale_handle(&err));
        assert!(is_nfs_noent(&err));

        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(!is_stale_handle(&err));
        assert!(is_nfs_noent(&err));

        let err = NfsError::Mount(nfs_rs::Nfs3MountErrorCode::MNT3ERR_NOENT);
        assert!(!is_stale_handle(&err));
        assert!(is_nfs_noent(&err));
    }

    #[test]
    fn test_is_stale_handle_excludes_delay() {
        // NFS4ERR_DELAY 由 nfs-rs 层（compound 重试循环）处理，不在 data-mover 层重试
        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_DELAY);
        assert!(!is_stale_handle(&err));
        assert!(!is_nfs_noent(&err));
    }

    #[test]
    fn test_is_stale_handle_unrelated_errors() {
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_PERM);
        assert!(!is_stale_handle(&err));

        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_EXIST);
        assert!(!is_stale_handle(&err));

        let err = NfsError::Rpc("connection refused".to_string());
        assert!(!is_stale_handle(&err));
    }

    #[test]
    fn test_is_nfs_noent_io_notfound() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file or directory");
        let err = NfsError::Io(io_err);
        assert!(is_nfs_noent(&err));
        assert!(!is_stale_handle(&err));
    }

    #[test]
    fn test_is_nfs_noent_io_other() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err = NfsError::Io(io_err);
        assert!(!is_nfs_noent(&err));
        assert!(!is_stale_handle(&err));
    }

    // --- is_retryable_with_invalidation 测试（STALE/BADHANDLE/NOENT 都参与重试）---

    #[test]
    fn test_is_retryable_with_invalidation_stale() {
        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_STALE);
        assert!(is_retryable_with_invalidation(&err));
    }

    #[test]
    fn test_is_retryable_with_invalidation_noent_concurrency_race() {
        // 高并发场景：worker A 刚 mkdir 完，worker B lookup cache 尚未失效，
        // B 拿到 NOENT。清除缓存 + 重试可恢复。
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_NOENT);
        assert!(is_retryable_with_invalidation(&err));

        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(is_retryable_with_invalidation(&err));
    }

    #[test]
    fn test_is_retryable_with_invalidation_excludes_delay() {
        // DELAY 由 nfs-rs 层处理，不在 data-mover 层重复重试
        let err = NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_DELAY);
        assert!(!is_retryable_with_invalidation(&err));
    }

    #[test]
    fn test_is_retryable_with_invalidation_excludes_unrelated() {
        let err = NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_PERM);
        assert!(!is_retryable_with_invalidation(&err));

        let err = NfsError::Rpc("connection refused".to_string());
        assert!(!is_retryable_with_invalidation(&err));
    }
}
