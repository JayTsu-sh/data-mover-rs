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

role-based S3（`src/s3/role_protocol.rs` → `FailureClass`，ADR-0006 C6）：

| 来源 | 映射到 |
|---|---|
| 404 / `NoSuchKey` / `NoSuchUpload` / `NoSuchVersion` | 条目 `NotFound` / Permanent |
| `BadDigest`（收到的 body 与 `Content-MD5` 不符，传输中损坏；ADR-0006 C14a） | 条目 `Corruption` / **Transient** —— 重发即可（`S3ProtocolFailure::corrupted_upload`） |
| `InvalidDigest`（`Content-MD5` 头本身格式错，是我们的请求错） | 条目 `InvalidInput` / Permanent —— 重发同样失败 |
| `InvalidPart` / `InvalidPartOrder`（Complete 的分段表与服务端持有的分段不符或乱序；ADR-0006 C15a） | 条目 `Conflict` / Permanent —— 同一分段表重发不会变 |
| 412 `PreconditionFailed`（`copy-source-if-match` / `If-Match` 不符）、409；416 `InvalidRange`（`UploadPartCopy` / 读取的范围源里已没有：源缩短或被替换；ADR-0006 C18） | 条目 `Conflict` / Permanent —— 源变了，重发不会变 |
| `EntityTooSmall`（非最后一段小于 5 MiB 最小分段；C15a） | 条目 `Corruption` / Permanent —— 分段本身错了 |
| **带 versionId 的** HEAD / GET 返回 405（该版本是删除标记）；`Current` 在版本化桶上也会钉成 versionId，所以普通读也属此类 | 条目 `NotFound` / Permanent |
| **带 versionId 的** GET 返回 400 且错误码 `InvalidArgument`（versionId 格式错） | 条目 `InvalidInput` / Permanent |
| 其余 400（`ExpiredToken`、region 错……；HEAD 没有错误码，其 400 一律在此） | 仍按通用映射（会话级）—— 这些关乎整个会话 |
| 不带 versionId 的 405 | 仍按通用映射（会话 `Protocol` / Unknown）—— DXN / SG 对不支持的操作也可能回 405 |
| **按 versionId 的** DeleteObject 被 Object Lock 拒绝：AWS 403 `AccessDenied`，MinIO 400 `InvalidRequest`（"Object is WORM protected"，RELEASE.2023-03-20 实测），或 `ObjectLocked`（ADR-0006 C17，`role_protocol/versions.rs`） | 条目 `PermissionDenied` / Permanent —— 只关乎这个版本；删指针版本时降级为告警 + 删除标记（`upload_pointer::delete`），不算传输失败 |
| **不带** versionId 的 DeleteObject 也被 Object Lock 拒绝（Ceph RGW / DXN：当前版本有 legal hold 时 403 `AccessDenied`，2026-09-25 实测；AWS / MinIO 加删除标记） | 条目 `PermissionDenied` / Permanent（映射不变）；删指针时（先按版本被拒之后）降级为告警、指针留在原处（`upload_pointer::Removal::Held`），不算传输失败 |
| Complete 之后删自己的指针失败（任何类别） | 映射不变；`publish_final_upload` 只告警、照常报已发布（对象已提交，重试会多一个版本；遗留指针下次 prepare 清） |
| 节流：429、503，或错误码 `SlowDown` / `ServiceUnavailable` / `Throttling*` / `TooManyRequests*` / `RequestLimitExceeded` / `RequestThrottled*` / `BandwidthLimitExceeded`（不论状态码；P7 C3） | 会话 **`Throttled`** / Transient（此前是 `Connectivity`）。范围保持会话级，传输侧行为不变（引擎不按类别分支；`definite_refusal` 不含它，仍走 reconcile）。**列举**在生产者内按页退避（`src/storage/throttle.rs`：首个约 125–250 ms、翻倍、单次 ≤15 s、单个列举累计 ≤约 2 分钟，带抖动，取消即停），预算耗尽才是**该列举**的条目 `Throttled` / Transient（目录 `Unreadable`）；遍历把任何 `Throttled` 会话失败收窄为那个列举 / 那个子项的失败，**节流不结束遍历**（P7 评审现存问题 1） |
| 其余 5xx（500 / 502 / 504 …） | 会话 `Connectivity` / Transient（不变） |
| DXN 重试一个仍在完成中的 Complete：500 `InternalError`（"This multipart completion is already in progress"） | 通用 5xx：会话 `Connectivity` / Transient —— 不是提交前的拒绝，`settle_completion` 查 `ListParts` |
| DXN 完成后的 `ListParts`：404 `NoSuchKey`（MinIO 回 `NoSuchUpload`） | 条目 `NotFound` / Permanent（同上第一行）—— 与 `NoSuchUpload` 一样进入按版本结算（C17） |
| 按 versionId 删除一个不存在的版本 | 成功（S3 / MinIO 回 204），不是错误 |
| 按 versionId 的 DeleteObject 回 405（存储不支持按版本删） | 条目 `Unsupported` / Permanent —— **不是**带版本读的「删除标记」`NotFound`（清理会把它当成已删）；删指针时降级为普通删除 + 告警 |
| describe 之后对象被替换（`read` / native bind / `observe_bound`） | 条目 `Conflict` / Permanent |
| 请求 `Id(v)` 而 HEAD 回来的不是版本 v（存储忽略 `?versionId=`） | 条目 `Unsupported` / Permanent |
| 一个目录的列举翻页：续页 token / marker 绕回来过、连续空页超限（`paging.rs`，C22） | **该目录**的条目 `Protocol` / Permanent（报在目录上，块 `Failed`，遍历继续兄弟；不结束会话 —— 之前 token 绕圈是会话失败，P7 评审现存问题 3） |
| 一个目录的条目超过 500 万（`paging.rs`） | 该目录的条目 `Capacity` / Permanent（同上） |

引擎（ADR-0006 C7）：同一进程里另一个传输正在写同一目的端文件（按（目的端点, 最终路径）的租约）→ Prepare 阶段
条目 `Conflict` / **Transient**，调用方可稍后重排。

### NFS

| 来源 errno | 映射到 |
|---|---|
| `EACCES` | `PermissionDenied(...)` |
| `EPERM` | `PermissionDenied(...)` |
| `ENOENT` (file) | `FileNotFound(...)` |
| `ENOENT` (dir) | `DirectoryNotFound(...)` |
| `ENOSPC` | `InsufficientSpace(...)` |
| 其他 | `NfsError(...)` |

上表是 legacy `StorageError` 路径。role 路径（`src/storage/backends/nfs/protocol.rs::classify_error`）直接从
nfs-rs 错误产出 `FailureClass` + `Transience`，并记下服务端状态名（诊断如
`NFS role failed: NFS4ERR_PERM (permission denied)`）。**先看 nfs-rs 的 `OperationOutcome` 包装**：
被包装的错误按恢复动作分类，里面的状态只用来命名（例如 `DoNotRetry` 包着 `NFS4ERR_PERM` 是
`Protocol` / Permanent，不是 `PermissionDenied`）。

| nfs-rs 错误 | FailureClass | Transience |
|---|---|---|
| `OperationOutcome` Retry / Remount | `Connectivity` | Transient |
| `OperationOutcome` Reopen | `Protocol` | Transient |
| `OperationOutcome` VerifyThenResume | `Protocol` | Unknown |
| `OperationOutcome` DoNotRetry | `Protocol` | Permanent |
| `Unsupported` | `Unsupported` | Permanent |
| NOENT 类（`is_not_found`） | `NotFound` | Permanent |
| `NFS3ERR_ACCES` / `NFS3ERR_PERM` / `NFS4ERR_ACCESS` / `NFS4ERR_PERM` | `PermissionDenied` | Permanent |
| `NFS3ERR_NOSPC` / `NFS4ERR_NOSPC` | `Capacity` | Permanent |
| `InvalidInput` | `InvalidInput` | Permanent |
| `Io` PermissionDenied / NotFound | `PermissionDenied` / `NotFound` | Permanent |
| `Io` ConnectionReset / TimedOut / WouldBlock | `Connectivity` | Transient |
| 其他 `Io` / `Rpc` | `Connectivity` | Unknown |
| `NFS4ERR_DELAY` / `NFS3ERR_JUKEBOX`（服务端忙。`NFS4ERR_DELAY` 由 nfs-rs 与列举的 `list_role_entries` 先退避；`NFS3ERR_JUKEBOX` 两者都还不重试，立即浮出——待补） | **`Throttled`** | Transient |
| 其他服务端状态（如 `NFS4ERR_BADOWNER`、`NFS4ERR_STALE`） | `Protocol` | Unknown |

`Connectivity` 经 `role_failure` 升为会话失败（metadata 阶段报 `SessionLost`，后面的族不再应用）；
经 `entry_scoped` 的调用方（可选元数据的 Required 读取）不升级。

元数据角色的 SETATTR（mode / owner / 时间）也走这张表（K3b，`setattr_role`）；此前经 legacy 字符串包装，
被拒一律是 `Protocol` / Unknown、没有状态名、也从不升为会话失败。SETATTR 之前的 LOOKUP 与 stale 重试里的
re-lookup 仍走 legacy `classify_role_error`，失败时没有状态。

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
`classify` 产出的 diagnostic 是 `CIFS {Operation} failed: {smb 错误 Display}` (去掉 NUL)：
smb-rs 的 Display 只含 NTSTATUS / 传输原因 / UNC 路径，不含凭据。之前是固定文案
"CIFS entry operation failed"，`Protocol` 兜底一旦命中就无从定位 (2026-09-18 的中途丢流
问题就是靠这行文本才找到的)。

### Local

| std::io::ErrorKind | 映射到 |
|---|---|
| `NotFound` | `FileNotFound(...)` 或 `DirectoryNotFound(...)` (按上下文) |
| `PermissionDenied` | `PermissionDenied(...)` |
| 其他 | `IoError(...)` |

### 列举：整个目录失败 vs 单个子项失败（role-based `Namespace` 的 `List`，P7 §8）

契约：只有**关乎目录本身**的错误让整个列举失败；某个子项描述不出来时报成该子项的条目失败，兄弟照常交付，
backend 把它放进 `ListingPage::failures`（`ChildFailure`），遍历把它报成 `Failure{scope: Child}`，计入块的 `counts.failed_children`，
目录的 `outcome` 仍是 `Complete`（P7 C2）；整个列举失败才是 `Failure{scope: Listing}` + `outcome: Unreadable`。
Local 早已如此；CIFS / NFS / HDFS 于 P7 评审发现的现存问题 2 修复；S3 的 size 缺失 / 为负、键越出前缀于 P7 C3 修复
（此前是**会话**失败）。

**不是失败的两种**（P7 C3）：
- **消失**：列举的子项失败 class 为 `NotFound` 且路径不是目录本身（唯一谓词 `listing::is_vanished`；
  `ChildFailure::vanished` 构造）= 列举返回了、描述时已不存在（Local `readdir` 后 `lstat` 得 ENOENT）。遍历计入
  `vanished`、不发项。观察时（元数据角色、`read_link`）的 `NotFound` 先 `stat` 确认子项本身不在才算消失，子项还在则
  是 `Conflict` / Transient 的子项失败（xattr 在列举与读取之间被删之类不能让活文件“消失”）。父列出后自己列举时
  `NotFound` 的子目录是空的 `Complete` 块（`DirectoryListed.vanished`）+ `vanished_listings`。**遍历根** `NotFound`
  仍是列举失败。`delete_tree` / `ndx_walk` 把消失的子项当作已不存在（不报、不标记目录不完整）。
- **节流**：见上面 S3 表的节流行；超预算只是该列举的失败。

**无名子项**：名字拼不出路径的子项，`ChildFailure { name: None, raw_name: Some(原始字节) }`，失败的路径是**目录**，
诊断是原因 + `raw name "<转义字节>"`，`EntryFailureIdentity` 由后端 + 目录 + 原始名派生（`ChildFailure::unnamed`）。
消费方规则：这样的失败让该目录已知的直接子项都不参与删除（`ScanFailure` 文档）。

| 情况 | 范围 | FailureClass / Transience |
|---|---|---|
| 目录打不开 / 读不了、READDIRPLUS 流出错、HDFS `list_status` 失败、遍历根不存在 | 整个列举（报在目录上，块 `Unreadable`；`Connectivity` 照常升会话） | 按上面各 backend 的映射 |
| 已交出若干子项之后读目录失败（Local `getdents` 出错，P7 C5：此前记成目录上的子项失败，且 rustix 出错后不再给条目，列举会被读成“完整”） | 整个列举，先交出已读到的子项，块 `Interrupted { delivered }`；一项都没交出则 `Unreadable` | 按 Local 映射（`EIO` 等 → `Protocol` / Unknown） |
| 列举返回的子项在描述 / 观察时已不存在 | 不是失败：`vanished` | `NotFound`（只用于分类） |
| Local 名字不是 UTF-8 / 拼出的路径超长 | 无名子项（`raw_name` = Unix 原始字节，Windows UTF-16LE） | `Unsupported` / Permanent |
| NFS / CIFS 名字含 U+FFFD（nfs-rs、smb-rs 有损解码，原名已丢，按它拼出的路径指不到原条目；真含 U+FFFD 的名字无法区分，一并报） | 无名子项（NFS 无原始字节；CIFS `raw_name` = 解码后的 UTF-8） | `Unsupported` / Permanent |
| NFS READDIRPLUS 某项没带属性（v3 `post_op_attr` 可省；v4 属性解不出也是这样）或没带文件句柄 | 子项（报在子项路径；名字拼不出 `StoragePath` 时报在目录上） | `Protocol` / Unknown |
| CIFS 子项的身份字节超出模型上限（造不出 `SourceIdentity`） | 子项 | `Protocol` / Permanent |
| HDFS 子项的 status 转换不了（时间超出 `i64` 纳秒等）、路径拼不出 `StoragePath` | 子项；不是本目录直接子项的（越出根、嵌套）或拼不出的报在目录路径上 | `Protocol` / Permanent |
| S3 对象 size 缺失 / 为负（`role_protocol/listing.rs` 逐项解码为 `S3RejectedEntry`） | 子项（报在对象路径）；版本列举里这个 key 的全部版本都不列 | `Protocol` / Permanent |
| S3 键 / 公共前缀越出存储根，或越出所列目录（服务端无视 prefix） | 无名子项（`raw_name` = 服务端给的键） | `Protocol` / Permanent |

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
