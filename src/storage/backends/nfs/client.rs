// 标准库
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

// 外部crate
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use moka::sync::Cache;
// nfs_rs 错误类型，用于直接匹配 error code
use nfs_rs::NfsError;
use nfs_rs::{
    Attr, DirectoryCookie, ExportEntry, Mount, NFSVersion, OPEN_READ, OPEN_WRITE, Time,
    WriteOutcome,
};
use path_clean::PathClean;
use tracing::{debug, error, info, trace, warn};
use url::form_urlencoded;

use crate::error::StorageError;
use crate::storage::backends::nfs::common::{
    NfsFactsError, NfsFactsProvider, NfsFailureCode, NfsInstanceFacts, NfsRetryAction,
    NfsRetryPolicy,
};
use crate::storage::backends::nfs::namespace::{
    NfsListingPages, NfsNamespaceObservation, NfsNamespaceProtocol, NfsReadEnd, NfsReadFailure,
    inline_ownership,
};
use crate::storage::backends::nfs::source::{
    NfsProtocolFailure, NfsReadCursor, NfsSourceObservation, NfsSourceProtocol,
};
use crate::storage::backends::nfs::staged::{NfsStageFile, NfsStagedProtocol};
use crate::transfer_concurrency::{
    TransferBackend, TransferConcurrency, resolve_transfer_concurrency,
};
use crate::{MB, Result};

mod owner;
mod role_create;
mod role_delete;
mod role_listing;
mod role_metadata;
mod role_namespace;
mod role_rename;
mod role_symlink;

pub(crate) use owner::{owner_id, owners_are_strings};

/// 将 `nfs_rs::Time` 转换为纳秒时间戳
fn time_to_i64(time: Time) -> i64 {
    crate::time_util::combine_secs_nanos(i64::from(time.seconds), time.nseconds)
}

/// 将纳秒时间戳转换为 `nfs_rs::Time`
fn i64_to_time(timestamp: i64) -> Result<Time> {
    let seconds = u32::try_from(crate::time_util::nanos_to_secs(timestamp)).map_err(|_| {
        StorageError::OperationError(format!(
            "NFS timestamp is outside the supported 1970..=2106 range: {timestamp}ns"
        ))
    })?;
    Ok(Time {
        seconds,
        nseconds: crate::time_util::nanos_subsec(timestamp),
    })
}

/// 基于目录深度的缓存过期策略。
/// 深度越浅 TTI 越长（高复用），深度越大 TTI 越短（优先淘汰）。
/// cache 容量满时，深层条目因 TTI 短而确定性地先过期被清除。
struct DepthAwareExpiry;

impl DepthAwareExpiry {
    /// 根据路径深度返回对应的 TTI
    fn tti_for_depth(depth: usize) -> Duration {
        match depth {
            0..=2 => Duration::from_hours(2), // 浅层：2h
            3..=4 => Duration::from_mins(10), // 中层：10min
            _ => Duration::from_secs(10),     // 深层：10s
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

#[cfg(test)]
thread_local! {
    /// Handles this thread put into [`GLOBAL_CACHE`], so a test can tell that a code path running
    /// on it cached none.
    static CACHE_WRITES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

static GLOBAL_CACHE: LazyLock<Cache<(PathBuf, Bytes), Bytes>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(500_000)
        .expire_after(DepthAwareExpiry)
        .support_invalidation_closures()
        .build()
});

/// Forgets the cached handles of `prefix` and of every directory below it on one server. After a
/// directory is removed or renamed away, those handles still resolve — into the moved directory,
/// or into nothing — and a directory recreated at the same path would never be looked up again.
/// Keys use the `collect_components` form (`NFSStorage::cache_path`); `Path::starts_with`
/// compares whole components, so `a/bc` is not below `a/b`.
fn invalidate_subtree_cache(prefix: PathBuf, root_fh: Bytes) {
    let registered = GLOBAL_CACHE
        .invalidate_entries_if(move |key, _| key.1 == root_fh && key.0.starts_with(&prefix));
    if registered.is_err() {
        // Only possible if the cache was built without closure support; forgetting everything is
        // always safe, just slower.
        GLOBAL_CACHE.invalidate_all();
    }
}

/// `nfs_url` → `server_id` 映射表。
/// 相同 NFS 端点的所有 worker 共享同一 `server_id，从而共享` `GLOBAL_CACHE` 条目，
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

/// `检测"目标不存在"错误（NFS3ERR_NOENT` / `NFS4ERR_NOENT` / `MNT3ERR_NOENT` / Io `NotFound`）。
///
/// 用途：
/// - `delete_file`：转换为 `FileNotFound`，由调用方决定是否按幂等成功处理
/// - `delete_dir`：幂等成功（删除已不存在的目录不报错）
/// - `lookup_fh：转换为` `DirectoryNotFound`/`FileNotFound` 返回给上层
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

/// `检测"陈旧文件句柄"错误（NFS3ERR_STALE` / `NFS3ERR_BADHANDLE` / `NFS4ERR_STALE` / `NFS4ERR_BADHANDLE`）。
///
/// 含义：缓存的 file handle 在服务端已失效（如服务端重启、export 重新生成）。
/// 与 `NFS4ERR_DELAY` 区分：DELAY 是"服务端繁忙，稍后重试"，由 nfs-rs 层
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

/// `检测"服务端繁忙"错误（NFS4ERR_DELAY`）。
///
/// 含义：服务端暂时繁忙，请稍后重试。nfs-rs 层在 `compound()` 中已对单条 RPC
/// 做了带 jitter 的退避重试。本函数用于 **流式/迭代式操作**（如 `readdirplus`）
/// 的**操作级重试**：当流式遍历中途某条 RPC 在 nfs-rs 重试耗尽后仍返回 DELAY，
/// 重新从头开始整个 readdirplus 比"丢弃失败 entry 继续"安全得多
/// （否则会出现统计计数偏低的 silent data loss）。
///
/// 与 [`is_stale_handle`] 的恢复策略不同：
/// - stale handle: 刷新 `root_fh` + 清除路径缓存
/// - server busy: 单纯延时再试（nfs-rs 已做，data-mover 在流操作中再试）
fn is_server_busy(err: &NfsError) -> bool {
    matches!(err, NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_DELAY))
}

/// 应用层 `NFS4ERR_DELAY` 退避：2s/4s/8s 指数延时，最多 3 次（额外 ~14s 容错）。
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

/// The NFS URL parameter nfs-rs sizes directory replies with. data-mover neither sets it nor
/// forwards it (P7 C7d, user decision 2026-09-27): nfs-rs ≥ 0.8.6 sizes READDIR / READDIRPLUS
/// replies from the server's published read limit, and a URL may not override that.
const READDIR_BUFFER_PARAMETER: &str = "readdir-buffer";

/// Refuses an NFS URL query that sets `readdir-buffer` (a `ConfigError`: the URL parsed, the
/// parameter is not one data-mover accepts), rather than forwarding it to nfs-rs. Names are
/// percent-decoded as nfs-rs decodes them (`Url::query_pairs`), so an encoded spelling cannot
/// slip through; a fragment (`#…`), which nfs-rs never reads, is not looked at.
fn reject_readdir_buffer(query: &str) -> Result<()> {
    let query = query.split_once('#').map_or(query, |(query, _)| query);
    let names_it =
        form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == READDIR_BUFFER_PARAMETER);
    if names_it {
        return Err(StorageError::ConfigError(format!(
            "NFS URL parameter {READDIR_BUFFER_PARAMETER} is not accepted: directory replies are sized from the server's published read limit"
        )));
    }
    Ok(())
}

/// Why [`NFSStorage::lookup_fh`] failed, before the legacy string wrapping: a failure before any
/// LOOKUP answered (bad path, root refresh), or the error of the LOOKUP of `dirname` once the
/// stale / busy retries were spent.
enum LookupFailure {
    Storage(StorageError),
    Component { dirname: String, error: NfsError },
}

impl From<StorageError> for LookupFailure {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// 检测调用方可通过"清除路径缓存 + 刷新 `root_fh` + 重试"恢复的错误。
///
/// 包含：
/// - **STALE / BADHANDLE**：文件句柄陈旧（[`is_stale_handle`]），刷新后可恢复。
/// - **NOENT**（[`is_nfs_noent`]）：高并发场景下可能为瞬态——
///   如 worker A 刚 `mkdir` 完成但 worker B 的 path 缓存尚未失效，
///   B 此时 LOOKUP 会拿到 NOENT；清除缓存重试可恢复。
///   若重试耗尽仍 NOENT，调用方应转换为 `FileNotFound`/`DirectoryNotFound`。
///
/// **不包含 `NFS4ERR_DELAY`**：DELAY 由 nfs-rs 层（`compound()` 退避重试）处理，
/// data-mover 层若再叠加重试只会延长等待，无额外收益。
fn is_retryable_with_invalidation(err: &NfsError) -> bool {
    let code = match err {
        NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_BADHANDLE)
        | NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_BADHANDLE) => NfsFailureCode::BadHandle,
        error if is_stale_handle(error) => NfsFailureCode::StaleHandle,
        error if is_nfs_noent(error) => NfsFailureCode::ConcurrentLookupMiss,
        _ => return false,
    };
    NfsRetryPolicy::new(MAX_STALE_RETRIES).action(code, 0) == NfsRetryAction::InvalidateAndRetry
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

struct NfsRoleReadCursor {
    storage: NFSStorage,
    handle: Option<NFSFileHandle>,
}

impl Drop for NfsRoleReadCursor {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let storage = self.storage.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = storage.close(&handle).await;
            });
        }
    }
}

#[async_trait]
impl NfsReadCursor for NfsRoleReadCursor {
    async fn read_at(
        &self,
        offset: u64,
        count: usize,
    ) -> std::result::Result<Bytes, NfsProtocolFailure> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(NfsProtocolFailure::protocol)?;
        self.storage.role_read_exact(handle, offset, count).await
    }
}

#[async_trait]
impl NfsSourceProtocol for NFSStorage {
    fn read_concurrency(&self) -> usize {
        self.config.transfer_concurrency.read()
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::try_from(self.config.read_chunk_bytes).unwrap_or(usize::MAX)
    }

    async fn describe(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<NfsSourceObservation, NfsProtocolFailure> {
        let (file_handle, attrs) = self.role_lookup_with_attrs(path).await?;
        let kind = role_kind(&attrs);
        Ok(NfsSourceObservation {
            kind,
            timestamps: Some(attrs_timestamps(&attrs)),
            ownership: Some(inline_ownership(
                owner_id(&attrs.owner, attrs.uid, self.owners_are_strings()),
                owner_id(&attrs.owner_group, attrs.gid, self.owners_are_strings()),
                attrs.file_mode,
            )),
            mode: Some(attrs.file_mode),
            size: (kind == crate::model::EntryKind::File).then_some(attrs.filesize),
            content_version: [
                attrs.mtime.seconds,
                attrs.mtime.nseconds,
                attrs.ctime.seconds,
                attrs.ctime.nseconds,
            ]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>()
            .into(),
            file_handle,
        })
    }

    async fn open(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(Box<dyn NfsReadCursor>, Bytes), NfsProtocolFailure> {
        let handle = self
            .open(&PathBuf::from(path.as_str()), nfs_rs::OPEN_READ)
            .await
            .map_err(classify_role_error)?;
        let identity = handle.inner.fh.clone();
        Ok((
            Box::new(NfsRoleReadCursor {
                storage: self.clone(),
                handle: Some(handle),
            }),
            identity,
        ))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn classify_role_error(error: StorageError) -> NfsProtocolFailure {
    let (class, transience) = match error {
        StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_) => (
            crate::model::FailureClass::NotFound,
            crate::model::Transience::Permanent,
        ),
        StorageError::PermissionDenied(_) => (
            crate::model::FailureClass::PermissionDenied,
            crate::model::Transience::Permanent,
        ),
        StorageError::InvalidPath(_) => (
            crate::model::FailureClass::InvalidInput,
            crate::model::Transience::Permanent,
        ),
        StorageError::Cancelled => (
            crate::model::FailureClass::Cancelled,
            crate::model::Transience::Transient,
        ),
        StorageError::IoError(_) => (
            crate::model::FailureClass::Connectivity,
            crate::model::Transience::Unknown,
        ),
        _ => (
            crate::model::FailureClass::Protocol,
            crate::model::Transience::Unknown,
        ),
    };
    NfsProtocolFailure::new(class, transience)
}

fn role_kind(attrs: &Attr) -> crate::model::EntryKind {
    if attrs.type_ == FType3::NF3LNK as u32 {
        crate::model::EntryKind::Symlink
    } else if attrs.type_ == FType3::NF3DIR as u32 {
        crate::model::EntryKind::Directory
    } else {
        crate::model::EntryKind::File
    }
}

use crate::storage::backends::nfs::metadata::timestamp as nfs_timestamp;

/// Builds the neutral timestamp record from one NFS attribute set.
///
/// `created` carries the POSIX change time: `NFSv3` has no birth time, and this is the mapping
/// the NFS metadata role already uses, so both roles report the same three values.
fn attrs_timestamps(attrs: &Attr) -> crate::model::TimestampMetadata {
    crate::model::TimestampMetadata {
        accessed: nfs_timestamp(time_to_i64(attrs.atime)),
        modified: nfs_timestamp(time_to_i64(attrs.mtime)),
        created: nfs_timestamp(time_to_i64(attrs.ctime)),
    }
}

#[async_trait]
impl NfsNamespaceProtocol for NFSStorage {
    async fn create_symlink(
        &self,
        request: &crate::storage::CreateSymlinkRequest,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.role_create_symlink(request).await
    }
    async fn stat(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<NfsNamespaceObservation, NfsProtocolFailure> {
        let (file_handle, attrs) = self.role_lookup_with_attrs(path).await?;
        let kind = role_kind(&attrs);
        Ok(NfsNamespaceObservation {
            path: path.clone(),
            kind,
            size: (kind == crate::model::EntryKind::File).then_some(attrs.filesize),
            file_handle,
            timestamps: Some(attrs_timestamps(&attrs)),
            mode: Some(attrs.file_mode),
            ownership: Some(inline_ownership(
                owner_id(&attrs.owner, attrs.uid, self.owners_are_strings()),
                owner_id(&attrs.owner_group, attrs.gid, self.owners_are_strings()),
                attrs.file_mode,
            )),
        })
    }

    async fn directory_handle(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<Bytes, NfsProtocolFailure> {
        self.role_directory_handle(path)
            .await
            .map_err(classify_role_error)
    }

    async fn read_directory(
        &self,
        path: &crate::model::StoragePath,
        handle: Bytes,
        from: DirectoryCookie,
        pages: &mut dyn NfsListingPages,
    ) -> std::result::Result<NfsReadEnd, NfsReadFailure> {
        self.read_role_directory(path, handle, from, pages).await
    }

    async fn forget_directory(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.forget_role_directory(path)
            .await
            .map_err(classify_role_error)
    }

    async fn read_link(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<Bytes, NfsProtocolFailure> {
        let object = self.role_lookup_fh(Path::new(path.as_str())).await?;
        let target = self
            .mount
            .readlink(object.fh)
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error)?;
        Ok(Bytes::from(target))
    }

    /// Creates one level, with stale-parent recovery in `src/storage/backends/nfs/client/role_namespace.rs`.
    async fn create_directory(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.role_create_directory(path).await
    }

    async fn cleanup_transfer_artifacts(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.role_cleanup_artifacts(Path::new(path.as_str()), false)
            .await
    }

    async fn delete(
        &self,
        path: &crate::model::StoragePath,
        kind: crate::model::EntryKind,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        let native = PathBuf::from(path.as_str());
        if kind == crate::model::EntryKind::Directory {
            self.role_cleanup_artifacts(&native, true).await?;
            self.role_delete_directory(&native).await
        } else {
            self.delete_file(&native).await.map_err(classify_role_error)
        }
    }

    async fn rename(
        &self,
        from: &crate::model::StoragePath,
        to: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.role_rename(from, to).await
    }
}

struct NfsRoleStageFile {
    storage: NFSStorage,
    handle: NFSFileHandle,
    deferred_gate: tokio::sync::RwLock<()>,
    deferred_writes: tokio::sync::Mutex<Vec<NfsDeferredWrite>>,
    closed: std::sync::atomic::AtomicBool,
}

struct NfsDeferredWrite {
    offset: u64,
    accepted_bytes: usize,
    retry_data: Option<Bytes>,
    outcomes: Vec<WriteOutcome>,
}

impl NfsDeferredWrite {
    fn from_outcomes(offset: u64, data: Bytes, outcomes: Vec<WriteOutcome>) -> Self {
        let accepted_bytes = data.len();
        let retry_data = outcomes
            .iter()
            .any(|outcome| outcome.committed != nfs_rs::WriteCommitted::FileSync)
            .then_some(data);
        Self {
            offset,
            accepted_bytes,
            retry_data,
            outcomes,
        }
    }

    const fn accepted_bytes(&self) -> usize {
        self.accepted_bytes
    }

    fn commit_pressure(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.committed != nfs_rs::WriteCommitted::FileSync)
            .count()
    }
}

struct NfsUnstableWriteFailure {
    accepted: Option<NfsDeferredWrite>,
    /// Boxed to keep the `Result` small: every accepted write would otherwise carry the size of
    /// a full protocol error.
    error: Box<NfsError>,
}

impl NfsRoleStageFile {
    fn new(storage: NFSStorage, handle: NFSFileHandle) -> Self {
        Self {
            storage,
            handle,
            deferred_gate: tokio::sync::RwLock::new(()),
            deferred_writes: tokio::sync::Mutex::new(Vec::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    async fn checkpoint_deferred_writes(&self) -> nfs_rs::Result<()> {
        let _gate = self.deferred_gate.write().await;
        let mut writes = self.deferred_writes.lock().await;
        if writes.is_empty() {
            return Ok(());
        }

        for attempt in 0..3 {
            let start = writes.iter().map(|write| write.offset).min().unwrap_or(0);
            let end = writes
                .iter()
                .filter_map(|write| write.offset.checked_add(write.accepted_bytes() as u64))
                .max()
                .unwrap_or(start);
            let (commit_offset, commit_count) =
                u32::try_from(end.saturating_sub(start)).map_or((0, 0), |count| (start, count));
            let outcomes = writes
                .iter()
                .flat_map(|write| write.outcomes.iter().cloned())
                .collect::<Vec<_>>();

            match self
                .storage
                .mount
                .commit_write_batch(
                    self.handle.inner.fh.clone(),
                    commit_offset,
                    commit_count,
                    &outcomes,
                )
                .await
            {
                Ok(()) => {
                    writes.clear();
                    return Ok(());
                }
                Err(error)
                    if attempt < 2
                        && error.operation_outcome().is_some_and(|outcome| {
                            outcome.context().operation == "write_verifier"
                        }) =>
                {
                    let retry_ranges = writes
                        .iter()
                        .filter_map(|write| {
                            write.retry_data.clone().map(|data| (write.offset, data))
                        })
                        .collect::<Vec<_>>();
                    if retry_ranges.is_empty() {
                        return Err(error);
                    }
                    let mut retried = Vec::with_capacity(retry_ranges.len());
                    for (offset, data) in retry_ranges {
                        match self
                            .storage
                            .role_write_unstable(&self.handle, offset, data)
                            .await
                        {
                            Ok(write) => retried.push(write),
                            Err(failure) => {
                                if let Some(accepted) = failure.accepted {
                                    retried.push(accepted);
                                }
                                *writes = retried;
                                return Err(*failure.error);
                            }
                        }
                    }
                    *writes = retried;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the final commit attempt always returns")
    }

    async fn retain_unstable_write(
        &self,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<(u64, usize), NfsProtocolFailure> {
        let _gate = self.deferred_gate.read().await;
        match self
            .storage
            .role_write_unstable(&self.handle, offset, data)
            .await
        {
            Ok(write) => {
                let written = write.accepted_bytes() as u64;
                let mut writes = self.deferred_writes.lock().await;
                writes.push(write);
                let commit_pressure = writes
                    .iter()
                    .map(NfsDeferredWrite::commit_pressure)
                    .sum::<usize>();
                Ok((written, commit_pressure))
            }
            Err(failure) => {
                if let Some(accepted) = failure.accepted {
                    self.deferred_writes.lock().await.push(accepted);
                }
                Err(crate::storage::backends::nfs::protocol::classify_error(
                    *failure.error,
                ))
            }
        }
    }
}

impl Drop for NfsRoleStageFile {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let handle = self.handle.clone();
        let storage = self.storage.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = storage.close(&handle).await;
            });
        }
    }
}

#[async_trait]
impl NfsStageFile for NfsRoleStageFile {
    async fn read_at(
        &self,
        offset: u64,
        count: usize,
    ) -> std::result::Result<Bytes, NfsProtocolFailure> {
        self.storage
            .role_read_exact(&self.handle, offset, count)
            .await
    }

    async fn write_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        self.storage
            .role_write_once(&self.handle, offset, data)
            .await
    }

    async fn write_uncommitted_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        self.storage
            .role_write_unstable(&self.handle, offset, data)
            .await
            .map(|write| write.accepted_bytes() as u64)
            .map_err(|failure| {
                crate::storage::backends::nfs::protocol::classify_error(*failure.error)
            })
    }

    async fn write_deferred_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        let (written, commit_pressure) = self.retain_unstable_write(offset, data).await?;
        if commit_pressure >= self.storage.config.transfer_concurrency.write() {
            self.checkpoint_deferred_writes()
                .await
                .map_err(crate::storage::backends::nfs::protocol::classify_error)?;
        }
        Ok(written)
    }

    async fn write_until_checkpoint_at(
        &self,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        self.retain_unstable_write(offset, data)
            .await
            .map(|(written, _)| written)
    }

    async fn checkpoint(&self) -> std::result::Result<(), NfsProtocolFailure> {
        self.checkpoint_deferred_writes()
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error)
    }

    async fn set_len(&self, size: u64) -> std::result::Result<(), NfsProtocolFailure> {
        self.storage
            .mount
            .setattr(
                self.handle.inner.fh.clone(),
                None,
                None,
                None,
                None,
                Some(size),
                None,
                None,
            )
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error)
    }

    async fn close(&self) -> std::result::Result<(), NfsProtocolFailure> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let checkpoint = self
            .checkpoint_deferred_writes()
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error);
        let close = self.storage.role_close(&self.handle).await;
        checkpoint.and(close)
    }

    async fn close_uncommitted(&self) -> std::result::Result<(), NfsProtocolFailure> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _gate = self.deferred_gate.write().await;
        self.deferred_writes.lock().await.clear();
        self.storage.role_close(&self.handle).await
    }
}

#[async_trait]
impl NfsStagedProtocol for NFSStorage {
    fn supports_acl(&self) -> bool {
        Self::supports_acl(self)
    }

    fn supports_xattrs(&self) -> bool {
        Self::supports_xattr(self)
    }

    fn read_inflight(&self) -> usize {
        self.config.transfer_concurrency.read()
    }

    fn write_inflight(&self) -> usize {
        self.config.transfer_concurrency.write()
    }

    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::try_from(self.config.read_chunk_bytes).unwrap_or(usize::MAX)
    }

    fn maximum_write_chunk_bytes(&self) -> usize {
        usize::try_from(self.config.write_chunk_bytes).unwrap_or(usize::MAX)
    }

    async fn create_empty(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        let handle = self.create_empty_open(path).await?;
        if let Err(primary) = handle.close().await {
            let cleanup_failure = self
                .delete_file(native)
                .await
                .err()
                .map(classify_role_error);
            return Err(cleanup_failure.unwrap_or(primary));
        }
        Ok(())
    }

    async fn create_empty_open(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        let Some(handle) = self.role_create_file_exclusive(native, Some(0o600)).await? else {
            return Err(NfsProtocolFailure::new(
                crate::model::FailureClass::Conflict,
                crate::model::Transience::Permanent,
            ));
        };
        Ok(Box::new(NfsRoleStageFile::new(self.clone(), handle)))
    }

    async fn open_read(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
        let handle = self
            .open(Path::new(path.as_str()), OPEN_READ)
            .await
            .map_err(classify_role_error)?;
        Ok(Box::new(NfsRoleStageFile::new(self.clone(), handle)))
    }

    async fn open_write(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
        let handle = self
            .open(Path::new(path.as_str()), OPEN_WRITE)
            .await
            .map_err(classify_role_error)?;
        Ok(Box::new(NfsRoleStageFile::new(self.clone(), handle)))
    }

    async fn size(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        self.role_lookup_with_attrs(path)
            .await
            .map(|(_, attrs)| attrs.filesize)
    }

    async fn stat(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(bool, u64), NfsProtocolFailure> {
        // nfs-rs reports both versions' types in the v3 numbering (NF4REG is also 1).
        self.role_lookup_with_attrs(path)
            .await
            .map(|(_, attrs)| (attrs.type_ == FType3::NF3REG as u32, attrs.filesize))
    }

    async fn rename(
        &self,
        from: &crate::model::StoragePath,
        to: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.role_rename(from, to).await
    }

    async fn delete(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.delete_file(Path::new(path.as_str()))
            .await
            .map_err(classify_role_error)
    }
}

const DEFAULT_BLOCK_SIZE: u64 = MB; // 1MB

/// 单文件读取的同时在飞请求数（inflight read pipeline 深度）。
///
/// NFS READ 多命中 server 端 cache，单请求延迟低且稳定。以实测链路
/// （RTT 0.33ms + server ~1ms ≈ 1.3ms，64KB chunk）计：串行 ≈ 49MB/s，
/// 4 深度理论 ≈ 196MB/s，已超过 1GbE 链路上限。与 cifs/s3 的深度对齐。
/// v3 无并发上限；v4.1 受 session slot（64）约束，远未触及。
/// 写默认 8：`FILE_SYNC` 的持久化延迟和抖动通常高于读，使用更深窗口。
const DEFAULT_TRANSFER_CONCURRENCY: TransferConcurrency = TransferConcurrency::defaults(4, 8);

/// A zero limit is unspecified. Preserve the existing default/client cap in each direction.
fn negotiated_chunk_sizes(client: Option<u64>, rsize: u64, wsize: u64) -> (u64, u64) {
    let client_ceiling = client
        .filter(|&size| size > 0)
        .unwrap_or(DEFAULT_BLOCK_SIZE)
        .min(DEFAULT_BLOCK_SIZE);
    let ceiling = |server| {
        if server > 0 {
            client_ceiling.min(server)
        } else {
            client_ceiling
        }
    };
    (ceiling(rsize), ceiling(wsize))
}

#[derive(Clone, Debug)]
pub(crate) struct StorageConfig {
    /// Read and write ceilings are negotiated independently.
    pub read_chunk_bytes: u64,
    pub write_chunk_bytes: u64,
    pub transfer_concurrency: TransferConcurrency,
}

/// 该结构体是 NFS v3 存储的核心实现，封装了 NFS 挂载、根文件句柄和根路径等信息。
/// 实现了 `Storage` trait，提供了文件和目录的各种操作方法。
#[derive(Debug, Clone)]
pub(crate) struct NFSStorage {
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
    /// Optimized role-only lookup. nfs-rs already folds GETATTR into LOOKUP on every supported
    /// dialect, so reuse those attributes instead of paying a second metadata RPC. Directory-cache
    /// hits can lack attributes and deliberately fall back to one GETATTR.
    async fn role_lookup_with_attrs(
        &self,
        path: &crate::model::StoragePath,
    ) -> std::result::Result<(Bytes, Attr), NfsProtocolFailure> {
        let object = self.role_lookup_fh(Path::new(path.as_str())).await?;
        let file_handle = object.fh;
        let attrs = if let Some(attrs) = object.attr {
            attrs
        } else {
            self.mount
                .getattr(file_handle.clone())
                .await
                .map_err(crate::storage::backends::nfs::protocol::classify_error)?
        };
        Ok((file_handle, attrs))
    }

    /// Optimized role-only read primitive.
    ///
    /// The normal case is one negotiated READ RPC and returns the crate-owned `Bytes` without a
    /// copy. A legal short read is completed before returning because the role stream reserves
    /// non-overlapping ranges and requires every issued range to be exact.
    async fn role_read_exact(
        &self,
        file: &NFSFileHandle,
        offset: u64,
        count: usize,
    ) -> std::result::Result<Bytes, NfsProtocolFailure> {
        if count == 0 {
            return Ok(Bytes::new());
        }
        let mut current = offset;
        let mut remaining = count;
        let mut aggregate: Option<BytesMut> = None;

        while remaining != 0 {
            let requested = u32::try_from(remaining).map_err(|_| NfsProtocolFailure::protocol())?;
            let bytes = self
                .mount
                .read(file.inner.fh.clone(), current, requested)
                .await
                .map_err(crate::storage::backends::nfs::protocol::classify_error)?;
            if bytes.len() > remaining {
                return Err(NfsProtocolFailure::protocol());
            }
            if aggregate.is_none() && bytes.len() == count {
                return Ok(bytes);
            }
            if bytes.is_empty() {
                break;
            }
            current = current
                .checked_add(bytes.len() as u64)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            remaining -= bytes.len();
            aggregate
                .get_or_insert_with(|| BytesMut::with_capacity(count))
                .extend_from_slice(&bytes);
        }

        Ok(aggregate.map_or_else(Bytes::new, BytesMut::freeze))
    }

    /// Optimized role-only WRITE primitive. The staged adapter has already negotiated and split
    /// its chunks, so this issues exactly one RPC and retains nfs-rs' structured outcome.
    async fn role_write_once(
        &self,
        file: &NFSFileHandle,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<u64, NfsProtocolFailure> {
        nfs_rs::write_all(
            self.mount.as_ref().as_ref(),
            file.inner.fh.clone(),
            offset,
            data,
        )
        .await
        .map_err(crate::storage::backends::nfs::protocol::classify_error)
    }

    async fn role_write_unstable(
        &self,
        file: &NFSFileHandle,
        offset: u64,
        data: Bytes,
    ) -> std::result::Result<NfsDeferredWrite, NfsUnstableWriteFailure> {
        let mut accepted = 0usize;
        let mut outcomes = Vec::new();
        while accepted < data.len() {
            let outcome = match self
                .mount
                .write(
                    file.inner.fh.clone(),
                    offset + accepted as u64,
                    data.slice(accepted..),
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let accepted_write = (accepted != 0).then(|| {
                        NfsDeferredWrite::from_outcomes(offset, data.slice(..accepted), outcomes)
                    });
                    return Err(NfsUnstableWriteFailure {
                        accepted: accepted_write,
                        error: Box::new(error),
                    });
                }
            };
            let count = outcome.count as usize;
            if count == 0 || count > data.len() - accepted {
                return Err(NfsUnstableWriteFailure {
                    accepted: (accepted != 0).then(|| {
                        NfsDeferredWrite::from_outcomes(offset, data.slice(..accepted), outcomes)
                    }),
                    error: Box::new(NfsError::Rpc(
                        "server returned an invalid write count".to_owned(),
                    )),
                });
            }
            accepted += count;
            outcomes.push(outcome);
        }
        Ok(NfsDeferredWrite::from_outcomes(offset, data, outcomes))
    }

    /// Optimized role-only CLOSE preserves uncertain session-control outcomes from nfs-rs.
    async fn role_close(
        &self,
        file: &NFSFileHandle,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        self.mount
            .close(file.inner.fh.clone())
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error)
    }

    pub(crate) fn instance_facts(&self) -> Result<NfsInstanceFacts> {
        let dialect = crate::storage::backends::nfs::protocol::dialect(self.mount.version())
            .map_err(|_| {
                StorageError::NfsError(
                    "NFS server did not negotiate a supported exact dialect".to_owned(),
                )
            })?;
        let negotiated = self.mount.capabilities();
        NfsInstanceFacts {
            dialect,
            max_read_size: self.mount.get_max_read_size(),
            max_write_size: self.mount.get_max_write_size(),
            acl: negotiated.acl,
            xattrs: negotiated.named_attributes,
            stable_writes: true,
        }
        .validate()
        .map_err(|_| StorageError::NfsError("invalid negotiated NFS facts".to_owned()))
    }
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
    fn parse_nfs_url(url: &str) -> Result<(String, String)> {
        let without_scheme = url.strip_prefix("nfs://").ok_or_else(|| {
            let msg = format!("Invalid NFS URL format: {url}");
            error!("{msg}");
            StorageError::OperationError(msg)
        })?;

        let (server_part, raw_path_part) = without_scheme.split_once('/').map_or_else(
            || (without_scheme.to_string(), "/".to_string()),
            |(s, p)| (s.to_string(), format!("/{p}")),
        );

        let (path_part, query_part) = raw_path_part
            .split_once('?')
            .unwrap_or((&raw_path_part, ""));
        reject_readdir_buffer(query_part)?;

        // root_dir 去掉前后 '/'，使其成为相对路径前缀（空串表示挂载根）
        let (nfs_path, root_dir) = path_part.split_once(':').map_or_else(
            || (path_part.to_string(), String::new()),
            |(p, r)| (p.to_string(), r.trim_matches('/').to_string()),
        );

        // 检查query_part是否包含uid和gid参数
        let query_has_user = query_part.contains("uid=");
        let query_has_group = query_part.contains("gid=");

        let query_suffix = if query_part.is_empty() {
            // 如果query_part为空，直接添加uid=0&gid=。主要考虑到windows环境下的使用便利性。
            "?uid=0&gid=0".to_string()
        } else {
            // 如果query_part不为空，检查是否需要添加uid和gid
            let mut parts = query_part.split('&').collect::<Vec<&str>>();
            if !query_has_user {
                parts.push("uid=0");
            }
            if !query_has_group {
                parts.push("gid=0");
            }
            format!("?{}", parts.join("&"))
        };

        let nfs_url = format!("nfs://{server_part}{nfs_path}{query_suffix}");

        Ok((nfs_url, root_dir))
    }

    /// 内部方法：解析 URL、挂载 NFS、构建初始 storage 实例
    /// 返回 `(storage, root_dir)`，此时 `root_fh` 指向 mount 根，尚未解析 `root_dir`
    async fn mount_and_build(
        url: &str,
        block_size: Option<u64>,
        concurrency: Option<TransferConcurrency>,
    ) -> Result<(Self, String)> {
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
        let rsize = u64::from(mount.get_max_read_size());
        let wsize = u64::from(mount.get_max_write_size());
        let (read_chunk_bytes, write_chunk_bytes) =
            negotiated_chunk_sizes(block_size, rsize, wsize);
        info!(
            "NFS rsize={}, wsize={}, effective read_chunk_bytes={}, write_chunk_bytes={}",
            rsize, wsize, read_chunk_bytes, write_chunk_bytes
        );

        let sid = get_or_assign_server_id(&nfs_url);
        let cache_fh = build_cache_root_fh(sid, &mount_fh);
        let storage = NFSStorage {
            root_fh: Arc::new(std::sync::RwLock::new(mount_fh)),
            mount: Arc::new(mount),
            root: Arc::new(String::new()),
            config: StorageConfig {
                read_chunk_bytes,
                write_chunk_bytes,
                transfer_concurrency: resolve_transfer_concurrency(
                    TransferBackend::Nfs,
                    DEFAULT_TRANSFER_CONCURRENCY,
                    concurrency,
                )?,
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn list_exports(host: &str) -> Result<Vec<ExportEntry>> {
        nfs_rs::list_exports(host)
            .await
            .map_err(|e| StorageError::NfsError(format!("Failed to list NFS exports: {e}")))
    }

    /// 将解析出的 `root_dir` 装载到 storage：lookup root fh 后写入 `root_fh` + `root`。
    ///
    /// - `create_if_missing = true`（目标端）：prefix 不存在（NOENT）时自动逐级创建；
    /// - `create_if_missing = false`（源端）：prefix 不存在直接报 `DirectoryNotFound`。
    ///
    /// 网络 / 权限等非 NOENT 错误一律如实上抛，不能误判为"目录不存在"。
    async fn attach_root(mut self, root_dir: String, create_if_missing: bool) -> Result<Self> {
        if root_dir.is_empty() {
            return Ok(self);
        }
        let path = Path::new(&root_dir);
        let fh = match self.lookup_fh(path).await {
            Ok(obj_res) => {
                // prefix 存在但不是目录时 root_fh 会指向文件，后续所有操作以怪异方式失败
                if let Some(attr) = &obj_res.attr
                    && attr.type_ != FType3::NF3DIR as u32
                {
                    return Err(StorageError::OperationError(format!(
                        "NFS prefix '{root_dir}' exists but is not a directory"
                    )));
                }
                obj_res.fh
            }
            Err(StorageError::DirectoryNotFound(_)) if create_if_missing => {
                info!("NFS prefix '{}' does not exist, creating it", root_dir);
                let components = Self::collect_components(path)?;
                self.role_create_all(&components).await.map_err(|e| {
                    StorageError::OperationError(format!(
                        "Failed to create NFS prefix '{root_dir}': {:?} ({:?})",
                        e.class, e.status
                    ))
                })?;
                self.lookup_fh(path).await?.fh
            }
            Err(e) => {
                error!("Failed to lookup NFS root directory {root_dir}: {e}");
                return Err(e);
            }
        };
        *self.root_fh.write().map_err(|_| root_fh_lock_err())? = fh;
        self.root = Arc::new(root_dir);
        Ok(self)
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

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn lookup_fh(&self, relative_path: &Path) -> Result<nfs_rs::ObjRes> {
        self.lookup_fh_inner(relative_path, 0)
            .await
            .map_err(|failure| match failure {
                LookupFailure::Storage(error) => error,
                // NOENT once the retries are spent: the component really is absent.
                // Delete callers may treat it as idempotent success.
                LookupFailure::Component { dirname, error } if is_nfs_noent(&error) => {
                    StorageError::DirectoryNotFound(format!(
                        "Directory '{dirname}' not found: {error}"
                    ))
                }
                LookupFailure::Component { dirname, error } => {
                    StorageError::NfsError(format!("Failed to lookup directory {dirname}: {error}"))
                }
            })
    }

    /// [`Self::lookup_fh`] keeping the error of the LOOKUP that failed, so the role can classify
    /// it itself (`role_lookup_fh`, `src/storage/backends/nfs/client/role_create.rs`) instead of from the legacy string
    /// wrapping.
    async fn lookup_fh_inner(
        &self,
        relative_path: &Path,
        retries: u8,
    ) -> std::result::Result<nfs_rs::ObjRes, LookupFailure> {
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
                        Self::cache_directory(cache_key, &obj.fh);
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
                    return Err(LookupFailure::Component {
                        dirname: dirname.clone(),
                        error: e,
                    });
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

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
                    // 目标已不存在：保留可观察的 FileNotFound 分类。
                    // 需要幂等删除的调用方应显式将 FileNotFound 视为成功。
                    if e.is_not_found() {
                        trace!(
                            "File {:?} already gone, returning FileNotFound",
                            relative_path
                        );
                        return Err(StorageError::FileNotFound(
                            relative_path.display().to_string(),
                        ));
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

    fn deepest_cached_directory(&self, components: &[String]) -> (Bytes, usize) {
        let mut current_fh = self.rpc_root_fh();
        if components.is_empty() {
            return (current_fh, 0);
        }
        let mut partial_path: PathBuf = components.iter().collect();
        for index in (0..components.len()).rev() {
            let cache_key = (partial_path.clone(), self.get_root_fh());
            debug!("create_dir_all: cache_key: {:?}", cache_key);
            if let Some(fh) = GLOBAL_CACHE.get(&cache_key) {
                trace!("Global cache result for key={:?}: true", cache_key);
                current_fh = fh;
                return (current_fh, index + 1);
            }
            trace!("Global cache result for key={:?}: false", cache_key);
            partial_path.pop();
        }
        (current_fh, 0)
    }

    /// The path form `lookup_fh` builds its cache keys from. A key built from a raw `join` is
    /// written but never read: `join` uses the platform separator, while the lookup side runs
    /// every path through `collect_components`.
    fn cache_path(path: &Path) -> Result<PathBuf> {
        Ok(Self::collect_components(path)?.iter().collect())
    }

    /// [`invalidate_subtree_cache`] for `path` on this server.
    fn forget_subtree(&self, path: &Path) {
        // A path whose handles could never have been cached has nothing to forget.
        if let Ok(prefix) = Self::cache_path(path) {
            invalidate_subtree_cache(prefix, self.get_root_fh());
        }
    }

    /// Caches a copy of `file_handle`, never the handle itself: nfs-rs decodes handles without
    /// copying, so the `Bytes` it hands out is a slice of the whole RPC reply (a LOOKUP or MKDIR
    /// reply, or a readdirplus page), and caching the slice would keep that buffer alive for as
    /// long as the entry lives — hundreds of bytes to 8 KiB per cached directory.
    fn cache_directory(cache_key: (PathBuf, Bytes), file_handle: &Bytes) {
        trace!(
            "Inserting into global cache: key={:?}, value_len={}",
            cache_key,
            file_handle.len()
        );
        #[cfg(test)]
        CACHE_WRITES.with(|writes| writes.set(writes.get() + 1));
        GLOBAL_CACHE.insert(cache_key, Bytes::copy_from_slice(file_handle));
    }

    /// SETATTR with the stale-handle retry. The outer error is our own step failing (re-lookup,
    /// time conversion); the inner one is the server's answer to the SETATTR itself, kept as
    /// nfs-rs gave it so a caller can classify it and name its status.
    pub(crate) async fn setattr_retrying(
        &self,
        file: &NFSFileHandle,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<std::result::Result<(), NfsError>> {
        debug!("Setting metadata for {:?}", file.path);

        // 转换纳秒时间戳到Time类型
        let access_time_value = atime.map(i64_to_time).transpose()?;
        let modification_time_value = mtime.map(i64_to_time).transpose()?;

        // set_metadata 在 stale 重试时需要 re-lookup 获取新 fh，用 current_fh 跟踪
        let mut current_fh = file.inner.fh.clone();

        for attempt in 0..=MAX_STALE_RETRIES {
            match self
                .mount
                .setattr(
                    current_fh.clone(),
                    None,                    // 不设置guard_ctime
                    mode,                    // 设置mode
                    uid,                     // 设置uid
                    gid,                     // 设置gid
                    None,                    // 不设置size
                    access_time_value,       // 设置atime
                    modification_time_value, // 设置mtime
                )
                .await
            {
                Ok(()) => return Ok(Ok(())),
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
                    return Ok(Err(e));
                }
            }
        }
        unreachable!("retry loop always returns")
    }

    // ============================================================
    // NFSv4 ACL 操作
    // ============================================================

    /// 查询 NFS 版本
    #[must_use]
    pub fn version(&self) -> NFSVersion {
        self.mount.version()
    }

    /// Whether this mount's owners arrive as strings (`NFSv4`) rather than numbers (`NFSv3`); see
    /// [`owner_id`].
    pub(crate) fn owners_are_strings(&self) -> bool {
        owners_are_strings(self.mount.version())
    }

    /// 返回当前已协商实例报告的 ACL 能力。
    #[must_use]
    pub fn supports_acl(&self) -> bool {
        self.mount.capabilities().acl
    }

    /// 返回当前已协商实例报告的 named-attribute 能力。
    #[must_use]
    pub fn supports_xattr(&self) -> bool {
        self.mount.capabilities().named_attributes
    }

    // ============================================================
    // NFSv4 Extended Attributes (xattr)
    // ============================================================

    /// 获取指定 xattr 的值
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn list_xattr(&self, relative_path: &Path) -> Result<Vec<String>> {
        let obj = self.lookup_fh(relative_path).await?;
        self.mount.listxattr(obj.fh).await.map_err(|e| {
            StorageError::NfsError(format!(
                "Failed to list xattr for {}: {e}",
                relative_path.display()
            ))
        })
    }
}

impl NfsFactsProvider for NFSStorage {
    fn instance_facts(&self) -> std::result::Result<NfsInstanceFacts, NfsFactsError> {
        NFSStorage::instance_facts(self).map_err(|_| NfsFactsError)
    }
}

/// Builds the `ArchitectureReady` NFS role handle without routing through the legacy enum.
///
/// # Errors
/// Returns an error when mount, root attachment, fact validation, or role construction fails.
pub(crate) async fn create_nfs_role_storage(
    url: &str,
    block_size: Option<u64>,
    ensure_dir: bool,
    concurrency: Option<TransferConcurrency>,
) -> Result<crate::storage::Storage> {
    // The canonical endpoint (ADR-0006), derived before mounting so a bad URL fails fast.
    let identity = crate::storage::endpoint::nfs(url)
        .map_err(|error| StorageError::UrlParseError(error.to_string()))?;
    let (storage, root_dir) = NFSStorage::mount_and_build(url, block_size, concurrency).await?;
    let storage = storage.attach_root(root_dir, ensure_dir).await?;
    crate::storage::backends::nfs::connect(Arc::new(storage), identity)
        .map_err(|error| StorageError::ConfigError(error.to_string()))
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

/// Lists exports advertised by the NFS server.
///
/// # Errors
/// Returns a protocol error when the server cannot be queried.
pub async fn list_exports(host: &str) -> Result<Vec<ExportEntry>> {
    NFSStorage::list_exports(host).await
}

impl NFSStorage {
    async fn role_delete_directory(
        &self,
        relative_path: &Path,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        // 提取目录名
        let dirname = relative_path
            .file_name()
            .ok_or_else(NfsProtocolFailure::protocol)?
            .to_string_lossy()
            .to_string();

        // 查找父目录
        let parent_path = relative_path
            .parent()
            .ok_or_else(NfsProtocolFailure::protocol)?;

        for attempt in 0..=MAX_STALE_RETRIES {
            debug!(
                "Looking up parent path for remove dir {:?}: {:?}",
                relative_path, parent_path
            );
            let parent_obj = self.role_lookup_fh(parent_path).await?;

            // 删除目录
            match self.mount.rmdir(parent_obj.fh.clone(), &dirname).await {
                Ok(()) => {
                    self.forget_subtree(relative_path);
                    return Ok(());
                }
                Err(e) => {
                    // 目录已不存在：幂等语义，视为成功
                    if e.is_not_found() {
                        trace!(
                            "Directory {:?} already gone, treating as success",
                            relative_path
                        );
                        self.forget_subtree(relative_path);
                        return Ok(());
                    }
                    if is_retryable_with_invalidation(&e) && attempt < MAX_STALE_RETRIES {
                        debug!(
                            "Stale handle in delete_dir for {:?}, refreshing root_fh and retrying",
                            relative_path
                        );
                        let stale_gen = self.refresh_generation.load(Ordering::Acquire);
                        self.maybe_refresh_root_fh(stale_gen)
                            .await
                            .map_err(classify_role_error)?;
                        let root_fh = self.get_root_fh();
                        let components =
                            Self::collect_components(relative_path).map_err(classify_role_error)?;
                        invalidate_path_cache(&components, &root_fh);
                        continue;
                    }
                    return Err(crate::storage::backends::nfs::protocol::classify_error(e));
                }
            }
        }
        unreachable!("retry loop always returns")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;

    /// A cached handle owns its bytes: the reply it was decoded from can be freed.
    #[test]
    fn a_cached_handle_does_not_share_the_reply_buffer() {
        let reply = Bytes::from(vec![7_u8; 512]);
        let handle = reply.slice(100..132);
        let key = (
            PathBuf::from("pinned"),
            Bytes::from(uuid::Uuid::new_v4().as_bytes().to_vec()),
        );
        NFSStorage::cache_directory(key.clone(), &handle);
        let cached = GLOBAL_CACHE.get(&key).assert_value("the handle is cached");
        assert_eq!(cached, handle);
        let reply_range = reply.as_ptr_range();
        assert!(!reply_range.contains(&cached.as_ptr()));
        GLOBAL_CACHE.invalidate(&key);
    }

    /// A handle cached under a key `lookup_fh` never asks for is written and never read, which
    /// looks exactly like a cache that does not help — no failure, just the LOOKUP it was meant
    /// to remove. The raw `join` is the shape that gets this wrong.
    /// Removing or renaming a directory forgets its cached handle and every handle below it on
    /// the same server, and nothing else.
    #[test]
    fn a_removed_or_renamed_directory_forgets_its_cached_subtree() {
        let server = Bytes::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        let other_server = Bytes::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        let key = |path: &str, root: &Bytes| (PathBuf::from(path), root.clone());
        let entries = [
            key("a", &server),
            key("a/b", &server),
            key("a/b/c", &server),
            key("a/bc", &server),
            key("a/b", &other_server),
        ];
        for entry in &entries {
            GLOBAL_CACHE.insert(entry.clone(), Bytes::from_static(b"handle"));
        }
        invalidate_subtree_cache(PathBuf::from("a/b"), server.clone());
        let cached = |entry: &(PathBuf, Bytes)| GLOBAL_CACHE.get(entry).is_some();
        assert!(!cached(&key("a/b", &server)));
        assert!(!cached(&key("a/b/c", &server)));
        assert!(cached(&key("a", &server)));
        assert!(cached(&key("a/bc", &server)));
        assert!(cached(&key("a/b", &other_server)));
    }

    #[test]
    fn a_backfilled_cache_key_is_the_one_lookup_will_ask_for() {
        for (parent, name, expected) in [
            ("", "c", "c"),
            ("a", "c", "a/c"),
            ("a/b", "c", "a/b/c"),
            ("./a", "c", "a/c"),
        ] {
            let child = Path::new(parent).join(name);
            let key = NFSStorage::cache_path(&child).assert_value("cache path");
            let asked_for: PathBuf = NFSStorage::collect_components(&child)
                .assert_value("lookup components")
                .iter()
                .collect();
            assert_eq!(key, asked_for, "backfilled key for {child:?}");
            assert_eq!(key, PathBuf::from(expected));
        }
        // The case that proves the normalisation is load-bearing rather than decorative.
        let unnormalised = Path::new("./a").join("c");
        assert_ne!(unnormalised, PathBuf::from("a/c"));
    }

    #[test]
    fn negotiated_read_and_write_limits_are_independent() {
        let small = DEFAULT_BLOCK_SIZE / 4;
        assert_eq!(
            negotiated_chunk_sizes(None, small, DEFAULT_BLOCK_SIZE),
            (small, DEFAULT_BLOCK_SIZE)
        );
        assert_eq!(
            negotiated_chunk_sizes(None, DEFAULT_BLOCK_SIZE, small),
            (DEFAULT_BLOCK_SIZE, small)
        );
        assert_eq!(
            negotiated_chunk_sizes(Some(small / 2), small, DEFAULT_BLOCK_SIZE),
            (small / 2, small / 2)
        );
        assert_eq!(
            negotiated_chunk_sizes(Some(0), 0, small),
            (DEFAULT_BLOCK_SIZE, small)
        );
        assert_eq!(
            negotiated_chunk_sizes(None, small, 0),
            (small, DEFAULT_BLOCK_SIZE)
        );
        assert_eq!(
            negotiated_chunk_sizes(
                Some(2 * DEFAULT_BLOCK_SIZE),
                2 * DEFAULT_BLOCK_SIZE,
                2 * DEFAULT_BLOCK_SIZE
            ),
            (DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE)
        );
    }

    #[test]
    fn deferred_write_retains_payload_only_when_server_commit_is_required() {
        for (committed, expects_retry) in [
            (nfs_rs::WriteCommitted::FileSync, false),
            (nfs_rs::WriteCommitted::DataSync, true),
            (nfs_rs::WriteCommitted::Unstable, true),
        ] {
            let write = NfsDeferredWrite::from_outcomes(
                7,
                Bytes::from_static(b"payload"),
                vec![WriteOutcome::new(7, committed, Some([1; 8]))],
            );
            assert_eq!(write.accepted_bytes(), 7);
            assert_eq!(write.retry_data.is_some(), expects_retry);
            assert_eq!(write.commit_pressure(), usize::from(expects_retry));
        }
    }

    #[test]
    fn test_parse_nfs_url() {
        // 测试标准格式：nfs://server/path:root_dir
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path:root_dir")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "root_dir");

        // 测试没有根目录的格式：nfs://server/path
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "");

        // 测试带有查询参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path:root_dir?foo=bar")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?foo=bar&uid=0&gid=0");
        assert_eq!(root_dir, "root_dir");

        // 测试带有 uid 参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path?uid=1000")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?uid=1000&gid=0");
        assert_eq!(root_dir, "");

        // 测试带有 gid 参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path?gid=1000")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?gid=1000&uid=0");
        assert_eq!(root_dir, "");

        // 测试带有 uid 和 gid 参数的格式
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path?uid=1000&gid=1000")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?uid=1000&gid=1000");
        assert_eq!(root_dir, "");

        // 测试没有路径的格式：nfs://server
        let (nfs_url, root_dir) =
            NFSStorage::parse_nfs_url("nfs://server").assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/?uid=0&gid=0");
        assert_eq!(root_dir, "");

        // readdir-buffer 不接受也不转发（P7 C7d）：回复大小按服务器公布的上限定
        for url in [
            "nfs://server/path?readdir-buffer=8192,8192",
            "nfs://server/path:root?uid=0&readdir-buffer=65536",
            "nfs://server/path?readdir-buffer",
            "nfs://server/path?readdir%2Dbuffer=4096,4096",
            "nfs://server/path?%72eaddir-buffer=4096",
        ] {
            assert!(
                matches!(
                    NFSStorage::parse_nfs_url(url),
                    Err(StorageError::ConfigError(message)) if message.contains("readdir-buffer")
                ),
                "{url}"
            );
        }
        // 只认（解码后的）参数名本身；片段 nfs-rs 不读
        assert!(NFSStorage::parse_nfs_url("nfs://server/path?x-readdir-buffer=1").is_ok());
        assert!(NFSStorage::parse_nfs_url("nfs://server/path?uid=0#x&readdir-buffer=1").is_ok());

        // 测试带前导斜杠的 root_dir
        let (nfs_url, root_dir) = NFSStorage::parse_nfs_url("nfs://server/path:/prefix/dir")
            .assert_value("test value should be present");
        assert_eq!(nfs_url, "nfs://server/path?uid=0&gid=0");
        assert_eq!(root_dir, "prefix/dir");

        // 测试无效格式（非 nfs:// 开头）
        let result = NFSStorage::parse_nfs_url("http://server/path");
        assert!(result.is_err());
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
