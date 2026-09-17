# Error Taxonomy

## `StorageError` 29 个变体 (src/error.rs)

| # | 变体 | 含义 / 来源 |
|---|---|---|
| 1 | `IoError(std::io::Error)` | std::io / tokio::io 错误透传 |
| 2 | `ConfigError(String)` | 配置非法 (URL 字段缺失等) |
| 3 | `UnsupportedType(String)` | 不支持的存储类型 |
| 4 | `OperationError(String)` | 通用操作错误 (尽量避免，应映射到具体变体) |
| 5 | `InvalidPath(String)` | 路径格式非法 |
| 6 | `InvalidFilterExpression(String)` | filter DSL 表达式语法错 |
| 7 | `MismatchedParentheses(String)` | filter DSL 括号不匹配 |
| 8 | `InvalidToken(String)` | filter DSL 非法 token |
| 9 | `UnexpectedEndOfToken(String)` | filter DSL 表达式提前结束 |
| 10 | `ChecksumError(String)` | blake3 校验失败 |
| 11 | `Cancelled` | **不是错误**，是 CancellationToken 信号 |
| 12 | `S3Error(String)` | S3 SDK 错误 (除已映射的 404 等) |
| 13 | `NfsError(String)` | NFS 协议错误 (除已映射的 EACCES 等) |
| 14 | `FileNotFound(String)` | S3 404 / NFS ENOENT (file) / Local |
| 15 | `DirectoryNotFound(String)` | NFS ENOENT (dir) / S3 NoSuchBucket |
| 16 | `PermissionDenied(String)` | NFS EACCES/EPERM / Win ACL deny |
| 17 | `MismatchedType` | StorageEnum 与 file handle type 不匹配 |
| 18 | `TaskJoinError(String)` | tokio JoinError 透传 |
| 19 | `UrlParseError(String)` | URL 解析失败 (统一入口) |
| 20 | `SerializationError(String)` | serde / binrw 错误 |
| 21 | `InsufficientSpace(String)` | NFS ENOSPC / S3 quota |
| 22 | `FileLockError(String)` | 文件锁冲突 |
| 23 | `WinAceError(String)` | Windows ACL/SDDL 错误 |
| 24 | `CifsError(String)` | SMB/CIFS 协议错误 (除已映射的) |
| 25 | `HdfsOperation(HdfsOperationError)` | HDFS 结构化操作错误 (带 operation / kind / retryable) |
| 26 | `ReadError(String)` | 从源存储读取文件数据失败 |
| 27 | `WriteError(String)` | 向目标存储写入文件数据失败 |
| 28 | `MismatchData(Vec<MismatchDataField>)` | 完整性比对：文件内容或条目类型不匹配 |
| 29 | `MismatchMeta(Vec<MismatchMetaField>)` | 完整性比对：POSIX 元数据不匹配 |

> 表格顺序跟 `src/error.rs` 的声明顺序一致；`HdfsOperation` 在源文件里排第一，这里按补录时间
> 放在末尾。2026-09-17 复核：`grep -c '#\[error' src/error.rs` = 29，此前表与 R3 的 verify
> 命令都停在 24，25-29 是期间新增但没同步进来的。

## Retry Taxonomy (commit `7eb3046`)

调用方 (例如 sync 引擎) 根据 `StorageError` 决定是否重试：

| 错误 | 决策 | 备注 |
|---|---|---|
| `Cancelled` | **不是错误** | 上游可重入队，不算失败 |
| `IoError` | 视具体 io kind 决定 | tokio io 错误 kind 看具体 |
| `S3Error` | delay_backoff (除非 4xx) | 5xx / 网络错误重试 |
| `NfsError(EACCES/EPERM)` | **deny_list** | 直接失败，不重试 |
| `NfsError(EAGAIN/ECONNRESET)` | **delay_backoff** | 指数退避 |
| `CifsError` | 视具体 NTSTATUS 决定 | 大多 deny_list |
| `FileNotFound` | deny_list | 不重试 |
| `DirectoryNotFound` | deny_list | 不重试 |
| `PermissionDenied` | deny_list | 不重试 |
| `ChecksumError` | retry once 然后 deny_list | 第一次可能传输错，第二次大概率源已损坏 |
| `InsufficientSpace` | deny_list | 不重试 |
| `TaskJoinError` | deny_list | 内部 panic，重试无意义 |
| `UrlParseError` | deny_list | 配置问题 |
| 其他 | deny_list (默认保守) | 修改默认决策需要 PR 说明 |

## 映射规则

### S3

| 来源 | 映射到 |
|---|---|
| `NoSuchKey` | `FileNotFound(key)` |
| `NoSuchBucket` | `DirectoryNotFound(bucket)` |
| 其他 GetObject 错误 | `S3Error(...)` |
| HeadObject 404 | `FileNotFound(key)` |
| 网络错误 | `S3Error(...)` (走 delay_backoff) |

### NFS

| 来源 errno | 映射到 |
|---|---|
| `EACCES` | `PermissionDenied(...)` |
| `EPERM` | `PermissionDenied(...)` |
| `ENOENT` (file) | `FileNotFound(...)` |
| `ENOENT` (dir) | `DirectoryNotFound(...)` |
| `ENOSPC` | `InsufficientSpace(...)` |
| 其他 | `NfsError(...)` |

### CIFS

CIFS 只有 role-based 实现，不经过 `StorageError`；映射表在
`src/storage/backends/cifs/source.rs::classify_status`，产出 `FailureClass` + `Transience`：

| NTSTATUS | FailureClass | Transience |
|---|---|---|
| `STATUS_OBJECT_NAME_NOT_FOUND` / `STATUS_OBJECT_PATH_NOT_FOUND` | `NotFound` | Permanent |
| `STATUS_ACCESS_DENIED` | `PermissionDenied` | Permanent |
| `STATUS_WRONG_PASSWORD` / `STATUS_LOGON_FAILURE` / `STATUS_USER_ACCOUNT_LOCKED_OUT` | `Authentication` | Permanent |
| `STATUS_OBJECT_NAME_COLLISION` (mkdir 已存在) / `STATUS_DIRECTORY_NOT_EMPTY` (删非空目录) | `Conflict` | Permanent |
| `STATUS_SHARING_VIOLATION` / `STATUS_DELETE_PENDING` | `Conflict` | Transient |
| `STATUS_DISK_FULL` | `Capacity` | Permanent |
| `STATUS_INVALID_PARAMETER` / `STATUS_OBJECT_NAME_INVALID` | `InvalidInput` | Permanent |
| `STATUS_NOT_IMPLEMENTED` / `STATUS_NOT_SUPPORTED` / `STATUS_DEVICE_FEATURE_NOT_SUPPORTED` | `Unsupported` | Permanent |
| `STATUS_CANCELLED` | `Cancelled` | Transient |
| `STATUS_IO_TIMEOUT` / `STATUS_NETWORK_NAME_DELETED` / `STATUS_NETWORK_SESSION_EXPIRED` | `Connectivity` (session failure) | Transient |
| `STATUS_NOT_A_DIRECTORY` (0xC0000103，目录 open 命中文件) | `Conflict` | Permanent |
| `STATUS_CANNOT_DELETE` (0xC0000121) | `PermissionDenied` | **Unknown** |
| 其他 NTSTATUS / 非状态类 smb 错误 | `Protocol` | Unknown |

最后两个状态不在固定的 smb-rs `Status` 枚举里，`classify_status` 先按裸 `u32` 匹配，再走
`Status::try_from`。`STATUS_CANNOT_DELETE` 的 transience 是 `Unknown` 而不是 `Permanent`：
只读属性是永久的，但被映射/占用的文件会自己恢复，服务端不区分两者，写死 `Permanent` 会让
后者永远不重试。

非状态类 smb-rs 错误：`NotFound` → NotFound；`MissingPermissions` → PermissionDenied；
`InvalidArgument` → InvalidInput；`UnsupportedOperation` → Unsupported；`Cancelled` → Cancelled；
`ConnectionStopped` / `SessionInvalidated` / `RuntimeTerminated` / `TransportError` → session 级 Connectivity。
legacy 的"`OBJECT_NAME_COLLISION` 当成功"已随 #150 删除：role 返回 `Conflict`，由调用方决定是否幂等。

### Local

| std::io::ErrorKind | 映射到 |
|---|---|
| `NotFound` | `FileNotFound(...)` 或 `DirectoryNotFound(...)` (按上下文) |
| `PermissionDenied` | `PermissionDenied(...)` |
| 其他 | `IoError(...)` |

## 改 error 时

1. **不要随便加新变体**。当前 24 个覆盖 95% 场景。
2. 加变体必须 PR 说明：为什么不能用现有变体表示。
3. 改映射规则 (例如把 `S3 5xx` 改 retry → deny_list) 必须同步：
   - 本 doc 表格。
   - [storage-{nfs,s3,cifs,local}.md](.) 对应段。
   - 单测在 `src/{nfs,s3,cifs,local}.rs` 内。
4. 调 `reviewer` agent 复查映射一致性。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| 把 `Cancelled` 当错误处理 | 是信号不是错，上游应识别 |
| `OperationError(String)` 滥用 | 应映射到具体变体，OperationError 是兜底 |
| S3 404 重试 | 已修，必须 → FileNotFound |
| NFS EACCES 重试 | 已修，必须 deny_list |
| 错误信息中泄漏密码 / ak/sk | URL 必须先过 `url_redact.rs` |
