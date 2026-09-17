# CIFS / SMB Backend

## 底层依赖与边界

- CIFS **只有** role-based 实现：`src/storage/backends/cifs/`，经
  `storage::connect_backend(BackendConfig::Cifs(CifsBackendConfig { .. }))` 构造。
  `src/cifs.rs` 只剩 `create_cifs_role_storage` 这一个 factory bridge (解析 inflight 环境变量后
  调 `backends::cifs::connect`)。
- 历史 `CifsStorage` / `StorageEnum::CIFS` / `create_cifs_storage` 及其 `smb://` URL 解析已随
  #150 删除；`create_storage("smb://...")` 返回 `UnsupportedType`。`StorageType::Cifs` 仅保留给
  `detect_storage_type` 识别 scheme。
- backend 使用 smb-rs domain facade：`Client → Session → Share → File / Directory`。
- data-mover 不得重新依赖 smb-rs 的 connection、runtime、wire create/query/set 类型或协议 handle。
- `smb_domain::protocol` 只允许用于 lossless ACL codec 等明确的协议值边界，普通 I/O 不使用。
- 依赖只有一份：`smb-domain = { package = "smb", git = JayTsu-sh/smb-rs, rev = 9f68e92... }`
  (smb-rs main 顶端 = PR #70 + #71 + #72 + #73。#70/#71 给出 metadata-timestamps、目录 rename、
  `GuestPolicy`，并删掉约 2400 行从未接线的 lease-slot 缓存 / multichannel 残留 / 未用协议
  helper (`runtime/port.rs` 的 `Legacy*` 别名改为 `Protocol*`)；#72 把 `QUERY_DIRECTORY` 本来
  就返回的四个时间戳与只读 / reparse 属性暴露到 `DirectoryEntry`，不发新请求也不换 info
  class)；#73 让协商始终直接发 SMB2 NEGOTIATE。不得改成浮动 branch。`[patch.crates-io] smb` 与
  历史 API 提交 `c3ecf00` 已删除。

## 连接配置

没有 URL。`CifsBackendConfig` 字段：`server` / `share` / `username` / `password` /
`root: Option<String>` (share 内子路径) / `identity: BackendIdentity` / `signing_policy`。
凭据只有 NTLM (`smb_domain::Credentials::ntlm`)；facade 没有 multichannel 开关。
协商**始终**直接发 SMB2 NEGOTIATE，不再需要 legacy 的 `smb2_only` 开关 (smb-rs #73)。

**匿名 / guest 访问**：`guest_policy: CifsGuestPolicy` (默认 `Deny`)。服务端把未知用户或空/错密码映射到
guest 账号时 (ONTAP `vserver cifs options -guest-unix-user`，Samba `map to guest`)，会话没有 session
key、不能签名 (MS-SMB2 3.2.5.3.1)，`Deny` 下客户端在最终 SessionSetup 应答处拒绝。设为 `AllowUnsigned`
后接受未签名 guest 会话 (smb-rs `GuestPolicy`)。`username` 为空 + `AllowUnsigned` 时 factory 发送占位
身份 `anonymous` (NTLM 层拒绝空身份，真正的 null session 不可用)，由服务端做 guest 映射。
安全含义：guest 会话没有消息完整性；且 ONTAP 的 guest 映射是 SVM 级的——错密码也会被映射成 guest。

## 与 legacy 路径的能力差异

legacy `CifsStorage` 中以下能力在 role-based backend 里**没有**对应物 (删除时的盘点)：

- `Namespace` 实现 `Stat` / `List` / `CreateDirectory` / `Delete` (文件或空目录) / `Rename`
  (文件和目录，replace 语义与 NFS/HDFS 一致；目录 rename 走 smb-rs `Directory::rename_replace`，
  SMB2 `FileRenameInformation` 对目录句柄同样有效)。`ReadLink` 返回 typed `Unsupported`：
  facade 不暴露 reparse point，且 FAS2750 实测见下文"真实环境证据"。
  已存在目录再 `CreateDirectory` → `Conflict` (与 Local 的 AlreadyExists 一致)，不再像 legacy
  `mkdir_or_open` 那样吞掉 COLLISION。
- 递归删除带进度 (legacy `delete_dir_all_with_progress`) 现在是 backend 无关的
  `storage::delete_tree`：走 `Namespace` 角色，先并发删非目录条目，再按深度由深到浅删目录。
  `Delete` 返回 `NotFound` 记成功 (SMB 在 CLOSE 时才真正移除，close 应答丢失会让条目已经消失)；
  有后代删不掉的目录不再下发 `Delete`，只报一次 `Conflict`；取消时等待在途删除完成，不 abort。
- 根子路径自动创建 (legacy `ensure_root_exists`) 由 `CifsBackendConfig.ensure_dir` 控制
  (与 NFS / HDFS 的 `ensure_dir` 对齐)。`true` 时连接阶段补齐缺失层级：根存在只花一次 open，
  缺失才逐级探测 + `create_new`，COLLISION 视为竞争成功，某层是文件则连接失败。
  `false` 不探测根，行为与之前一致。
- 遍历由通用 `traversal::StorageTraversalSource` 驱动；filter DSL 剪枝与 `max_depth` 经
  `TraversalRequest.filter` / `.max_depth` 注入 (适配器 `crate::DslTraversalFilter`)。
  packaged/NDX 分页、work-stealing 仍归上层编排。
- 无 128-bit file id / info-class 探测；source identity 是
  `len + written + changed` (目录 len 记 0)，`describe` / `open` / `metadata` / `list` 四条路径
  共用 `protocol::identity_bytes` 同一套字节，互相可比
  (矩阵 `hardlink_topology: unsupported`，路径级 identity 足够)。
- 目录列举带回 `FILE_DIRECTORY_INFORMATION` 里已有的四个时间戳，挂在
  `SourceDescriptor::inline_timestamps` 上；`ObservationPlan` 只要时间戳时，遍历直接用它，
  不再对每个条目多发一次 `Metadata::observe` (N+1 → 1)。
- 无 `check_connectivity` / `probe_server_time`；`Metadata` 只支持 `Timestamps` 与 `Acl`，
  numeric uid/gid/mode 标为 `Unsupported`：SMB 不暴露 POSIX mode，FAS2750 unix 卷上的 mode 由
  服务端 name-mapping + umask 决定 (session 里 `mapped_unix_user=lisauser`)，客户端无法观察/应用。
- 整文件 `read_file` / `write_file`、`set_file_len`、`.part` 续传、tar 直写按设计删除，
  由 staged `Checkpointed` / `AtomicReplace` 替代。
- 默认并发 8/8 (legacy 为 4/4)。

## 关键代码点

### 资源句柄管理

所有 domain resource 在成功、失败和取消路径都必须显式 close。通用 `Resource` 走
`protocol::close_resource`；流式 File cursor 在 EOF 或终止边界关闭。

S99 教训：早期 `get_metadata` 在 error path 漏 close → 句柄泄漏 → 长 session 句柄耗尽。

```rust
// 反例
let h = open(...).await?;
let info = h.query_info().await?;  // ← 这里 ? 直接返回，h 没 close
h.close().await?;

// 正确
let h = open(...).await?;
let result = async { h.query_info().await }.await;
close_resource(&h).await;  // 总是 close
result
```

### Staged destination 与恢复

- 与 NFS 共用 `Checkpointed` / `AtomicReplace` 策略，CIFS 不支持 `Direct`。
- stage 位于最终文件的父目录，采用共享的 `.data-mover-<target-hash>-<uuid>.stage` 命名。
- `Checkpointed` 默认间隔 64 MiB；文件大于间隔且源端多块时延迟建立恢复记录。
  每个检查点等待所有已发出写入完成，FLUSH 数据，写入并 FLUSH checkpoint 临时文件，
  原子替换 `.stage.checkpoint`，再注册恢复信息。结束时 FLUSH 数据。
- `AtomicReplace` 不创建 checkpoint，也不主动 FLUSH；关闭 stage 后原子 rename 发布。
- recovery identity 是 opaque envelope；恢复时先把 stage 原子 rename 到 claim-token 派生路径，
  验证 checkpoint 的 binding、路径、校验和，只使用记录的连续前缀，不用 EOF 推断进度。
- checkpoint 名称不随 claim 改变；发布/丢弃时仅清理自己创建的 checkpoint。
- publish 使用 smb-rs `File::rename_replace`。响应丢失时沿用既有 publication reconciliation。
- SMB FLUSH 不等于 POSIX 目录 fsync；不额外宣称目录元数据持久化保证。

### Source read 与 QoS

- 每条 source stream 只持有一个 File handle，默认最多 8 路 positioned reads，按 offset 有序输出。
- CIFS 工厂解析已有 inflight 环境变量：`DATA_MOVER_CIFS_READ_INFLIGHT` /
  `DATA_MOVER_CIFS_WRITE_INFLIGHT`，其次全局方向配置，最后 `DATA_MOVER_INFLIGHT`。
  支持 1..=24，默认读写各 8；读还受 request.read_inflight 和 runtime 三项预算限制，写独立限流。
- 每次读分配 payload 前取得 runtime 的 chunk、byte、operation 准入；取消/丢弃流会关闭句柄。
- 每次请求不超过 smb-rs 协商的 `maximum_read_chunk`，最后一块精确收缩；短读是 corruption。
- source QoS 在真实 READ 前准入并只记录源端带宽/IOPS，目标 WRITE 不计入。
- describe 后到 open 之间必须重新构造 source identity；变化时在首个 READ 前 fast-fail。

### Metadata observation

- timestamps 来自 inline metadata，精度为 SMB FILETIME 的 100ns。
- 普通 `transfer()` 自动复制 mtime（Checkpointed / AtomicReplace 均支持），不自动复制
  atime、creation time 或 ChangeTime。跨协议纳秒时间向下对齐到 100ns，报告精度损失。
- 元数据观察通过同次查询的 size/written/changed 绑定已描述的 source identity；源版本改变则拒绝。
- 先完成数据写入并关闭写句柄、执行可选读回校验，再在 stage 上应用 mtime，最后 rename。
  Checkpointed 在元数据应用后再次 FLUSH；AtomicReplace 不增加 FLUSH。
- 文件和目录可显式调用 `Metadata::apply(Timestamps)`；目录不创建 stage、不自动递归修正时间。
  底层属性句柄拒绝最终 reparse point，避免误改软链接所指向对象；不宣称支持软链接元数据复制。
- 设置时间只经 smb-rs 公共 `open_metadata` / `set_metadata` API，未指定字段保持不变。
  数字 uid/gid/mode 不伪装成 Windows SID；自动跨协议复制会报告所有权未保留。
- ACL 需要额外 storage call，因此 `Omit`/`InlineOnly` 不调用服务器；只有
  `BestEffort`/`Required` 才 query security descriptor。
- ACL 语义与 Windows `src/acl.rs::copy_acl` 及 legacy `CifsStorage` 一致 (`metadata.rs`
  `explicit_dacl` / `merge_dacl`)：observe 只保留**显式** (非 `INHERITED_ACE`) ACE + `SE_DACL_PROTECTED`
  位；apply 先读目标 SD，源显式 ACE 在前、目标继承 ACE 在后合并 (canonical 顺序)，保护位取源端；
  源端 `SE_DACL_PROTECTED` 时不保留目标继承 ACE (protected 就是停止继承)；源端 NULL DACL 不应用；
  两端都无显式 ACE 且保护位相同时跳过 `SET_INFO`。与 `copy_acl` 的差异：源无显式 ACE 时
  这里会清掉目标端多余的显式 ACE (严格镜像源端)，`copy_acl` 则直接跳过。编码仍是 self-relative `SecurityDescriptor` 字节
  (`AclEncoding::WindowsSecurityDescriptor`)，与 `acl::get_acl_bytes` 的格式相同。
- CIFS xattr、tags、numeric ownership 当前按 typed not-applicable/unsupported 体现，
  不通过 storage enum 做协议配对分支。

### FileTime ↔ Unix nanos

- SMB FileTime = 100ns ticks since 1601-01-01 UTC。
- 转换在 `time_util.rs`，不要散写。

### 来自 legacy 路径的协议经验 (代码已删，经验保留)

- 写文件用 `CreateDisposition::OverwriteIf`：早期 Create + 追加触发 Samba `STATUS_ACCESS_DENIED`
  (commit `4051`)。现在由 smb-domain `File` open options 封装。
- Rename 必须用 share-relative 路径，不是 UNC 全路径 (commit `4052`)。smb-domain `rename_replace` 已封装。
- mkdir 时 `STATUS_OBJECT_NAME_COLLISION` 应视为成功 (commit `4061`)。
- 目录列举优先 `FileIdExtdDirectory` (128-bit id)，`FileIdBothDirectory` 只有 64-bit (commit `b1b9db1`)。

## 真实环境证据 (FAS2750 / ONTAP 9.19.1，2026-09-16，2026-09-17 复测)

来源：`tests/cifs_capability_probe.rs` (`[probe]` 行)、`tests/cifs_namespace_contract.rs`、
`tests/cifs_policy_contract.rs` 与 ONTAP REST。share `ontap_lisaauto_cifs` (SVM `lizy`，AD 域
CIFS 服务器 `LIZYAD`，卷 security style **unix**，LIF 10.128.61.200 / .201 分属两个节点)。

| 项 | 实测 | 决定 |
|---|---|---|
| 匿名 / guest (legacy `anon`) | 默认配置下：空身份被 NTLM 层拒绝，实名空/错密码 `STATUS_WRONG_PASSWORD`。设置 `guest-unix-user=pcuser` 并建 share `dm_anon_share` (Everyone full_control) 后：未知用户 + 空密码被接受为 guest，`CifsGuestPolicy::AllowUnsigned` 下 Namespace 契约全绿 (mkdir/list/rename/delete)；空用户名经占位身份同样通过；`Deny` 下按预期拒绝 (未签名会话)。AD 内置 `guest` 账号返回 `OutcomeUnknown` (账号禁用状态在 smb-rs 里未细分) | **已实现** (`guest_policy`)。真正的 null session (空身份) 仍不可用 |
| SMB1 多协议探测 (legacy `smb2_only`) | 协商结果是 SMB 3.1.1 (`protocol=smb3`, `ntlmv2`)，但**方言只反映协商结果，不代表没先发过 SMB1 帧**。实际上 `ConnectionConfig::smb2_only_negotiate` 是 `#[derive(Default)]` 上的 `bool`，默认 `false`，facade 此前每次建连都先发 legacy SMB1 多协议 NEGOTIATE | **已修** (smb-rs #73)：facade 固定 `smb2_only_negotiate = true`，始终直接 SMB2，且不提供开关。旧记录写成「不实现」是误判 —— 不是没实现，是默认行为从 legacy 的「跳过」悄悄变成了「发送」 |
| Multichannel | 服务端 `multichannel=false`；dual LIF 靠两个地址 | **不实现** (需要时先改 smb-rs facade) |
| 签名 | 服务端不强制 (`smb_signing=false`)；`WhenRequired` / `Required` 都能连 | 保持 `CifsSigningPolicy` |
| 目录 rename | role `Rename` 对目录 → `Completed`，往返成功 (smb-rs `Directory::rename_replace`) | **已实现** |
| Namespace `Stat/CreateDirectory/Delete/Rename` | 契约全绿；已存在目录再 mkdir → `Conflict`；rename 替换已存在文件成功 | **已实现** |
| 符号链接 | NFS 建的 UNIX symlink 在 SMB 列举里始终是 0 字节普通文件，不带 reparse 标记。share `symlink-properties` 为空 (本 share 默认)：`open` → `STATUS_ACCESS_DENIED`，role Stat → `PermissionDenied` (ONTAP 文档化行为)；临时设为 `enable`：服务端跟随，悬空链接呈现为 len=0 的 File，Stat → File。两种配置下客户端都无法识别它是链接；smb-rs 也无 `FSCTL_GET_REPARSE_POINT` | **不实现**。`ReadLink` 保持 typed `Unsupported`；遍历遇到时按 entry failure (PermissionDenied) 隔离，不中断 |
| ACL | query 正常 (2.9 KB SD 含 DACL)；显式/继承合并路径下 policy contract 通过 | **已实现** (见 Metadata observation) |
| uid/gid/mode | facade `ResourceMetadata` 只有 4 个时间 + len；服务端 unix 卷由 name-mapping 决定 mode | **不实现**，矩阵改 `unsupported` |
| 时钟 / 精度 | **偏的是本机，不是服务器**：ONTAP 同步 `ntp1.aliyun.com`，直接查同一 NTP 源本机慢 1.75-1.9 s，REST 查两节点也是本机慢 1.24 s+；探测报的 +550 ms (09-16) / +1624 ms (09-17) 是 WSL2 时钟漂移量。WSL2 内没有 NTP 客户端 (`timedatectl` NTP service: n/a，clocksource `tsc`)，宿主 PTP 源 (`ptp0`/hyperv) 无人用于校准，所以偏差随开机时长累积。written 时间戳 100 ns 对齐 | 无需 `probe_server_time` —— 需要担心的从来不是服务端。本机时钟只被 filter DSL 的相对时间表达式用到 (天级，2 s 误差无感)；所有影响正确性的比较都是服务端对服务端。探测那行的措辞 `server clock skew vs local` 量的其实是本机漂移 |
| 根目录列举 | 7-8 项 7-60 ms | — |
| 列举元数据密度 | `DirectoryEntry` 自 smb-rs #72 起带四个时间戳 + 只读 / reparse 属性；handle 侧 `ResourceMetadata` 仍只有四个时间 + len | **已实现**：`SourceDescriptor::inline_timestamps`，遍历只要时间戳时不再每条目多发一次 `Metadata::observe` |
| 递归删除 | 11 个条目 (6 文件 + 5 目录，四层深) 一次删净：文件并发、目录由深到浅、根最后，零失败，事后列举无残留 | **已实现** (`storage::delete_tree`) |
| 根子路径自动创建 | 一次创建三层 `a/nested/deep`；对已存在父级再建子目录复用不报错 | **已实现** (`ensure_dir`) |
| filter / max_depth 剪枝 | `max_depth=1` 只列举一层；`path == "sub/**"` 只收子目录内容 (sub 本身 PartialMatch 隐藏但下潜)；`exclude name == ...` 在两层同名文件上都生效 | **已实现** (`TraversalRequest.filter` / `.max_depth`) |
| 跨端完整性比对 | 同一对象完全一致；跨目录副本报出 Size + Modified + Content | **已实现** (`integrity::compare`) |

复现：`.claude/skills/e2e-cifs` (`CIFS_REAL_*`)。新能力的真机链路用
`examples/storage_role_operations` 的 `seed → traverse → compare → delete-tree`。
注意 `CIFS_POLICY_TEST_BYTES` 里的 64 MiB **等于**默认 checkpoint 间隔，日志会是
`SkippedBelowCheckpointThreshold`；要真正压到恢复路径需给一个更大的值 (实测 192 MiB 起
`recovery=Checkpointed`)。symlink probe 需要 `CIFS_PROBE_NFS_URL`
(`nfs://<lif>/<vol>:/?uid=0&gid=0&noresvport=true`)；WSL2 NAT 会改写源端口，ONTAP
`mount_root_only=true` 时会 `AUTH_TOOWEAK`，测试期间需临时关闭并事后恢复。

## 升级 smb-rs 依赖

`smb-domain` 固定的是 JayTsu-sh/smb-rs **main 上的一个提交**，不是 branch。升级 = 改一处 rev，
但验证必须完整，因为 smb-rs 的协议内部 (credits、签名、recovery) 出问题只会在真实服务器和
大文件上暴露 (2026-09-16 一次 64 MiB checkpoint 用例的间歇失败就是这样发现的)。

1. **确认来源**：目标提交必须已在 smb-rs `main` 上 (`git merge-base --is-ancestor <rev> origin/main`)，
   且 smb-rs 自己的 CI ("Format, lint, and test") 对该提交是绿的。不固定分支顶端以外的 PR 分支；
   如果确实要先用未合并分支验证，Cargo.toml 里写完整 40 位 rev，并在 PR 描述里说明，合并后再切回 main 提交。
2. **看变更面**：`git log --oneline <old>..<new>` + `git diff --stat`。凡是碰到
   `session/` `connection/` `runtime/wire.rs` `runtime/port.rs` `crypto/` `domain/` `facade/` 的提交，
   都按"协议变更"对待，走第 4 步的完整矩阵；只改 docs / tests 的提交可以只做第 3 步。
3. **本地门禁**：改 `Cargo.toml` 的 `rev`，`cargo fetch`，确认 `Cargo.lock` 里 `smb-*` 只剩新 rev；
   `python3 .claude/skills/quality-clippy/scripts/run.py` (即 `cargo clippy --all-targets -- -D warnings ...`)；
   `cargo test --lib`；`python3 tests/validate_architecture_dependencies.py .`。
4. **真实环境**：`.claude/skills/e2e-cifs` 全套 (需要 `CIFS_REAL_*`)：
   - `cifs_namespace_contract`：Stat / List / CreateDirectory / Rename (文件+目录) / Delete；
   - `cifs_policy_contract`：Checkpointed / AtomicReplace，含 64 MiB 多块 checkpoint、双 LIF、
     读回校验、目录 mtime；**至少跑 2 次**——协议层的回归常表现为间歇失败；
   - `cifs_namespace_contract` 以 `CIFS_REAL_GUEST_POLICY=allow-unsigned` + 空用户名跑匿名 share；
   - `cifs_capability_probe`：对照"真实环境证据"表，`[probe]` 行有变化时更新该表。
   失败时先用 `git bisect`/切回旧 rev 重跑同一用例区分"smb-rs 回归"与"服务端瞬时状态"。
5. **同步记录**：更新本文件"底层依赖与边界"里的 rev 与内容摘要、`CLAUDE.md` "强制约束" 的 rev、
   `.claude/docs/codebase.md` 依赖表；smb-rs 公开 API 有增删时同步 `连接配置` / `能力差异` 段和
   `docs/architecture/backend-capability-matrix.yaml`。
6. **提交**：单独一个 `chore(deps): smb-domain 固定到 smb-rs main <rev>` commit，body 写清 smb-rs
   提交范围、跑过的矩阵和结果；不要和功能改动混在一起，出问题能单独 revert。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| Samba `STATUS_ACCESS_DENIED` on write | smb-domain open options 用 OverwriteIf 语义 |
| 目录 len 在列举与句柄查询上不一致 | 列举报 0，目录句柄报索引分配量；identity 对目录一律记 0 |
| `STATUS_NOT_A_DIRECTORY` / `STATUS_CANNOT_DELETE` 不在 smb-rs `Status` 枚举里 | `classify_status` 先按裸 u32 匹配，映射到 `Conflict` / `PermissionDenied` |
| Rename 用 UNC 路径失败 | 改 share-relative (smb-domain 已封装) |
| 服务器要求签名 | 始终遵守；`CifsSigningPolicy::Required` 可强制 |
| 未知用户/空密码报 "Message not signed ... signing is required" | 服务端做了 guest 映射，会话无法签名；需要 `CifsGuestPolicy::AllowUnsigned` (注意无完整性保护) |
| 长 session 句柄耗尽 | 检查所有 close 路径走 `close_resource` |
| 符号链接 | facade 不暴露 reparse point；`ReadLink` 返回 typed `Unsupported` (实测见"真实环境证据") |
| 目录 rename 目标已存在 | NTFS 语义下不能替换非空目录，服务器返回 COLLISION/ACCESS_DENIED → `Conflict`/`PermissionDenied` |

## 测试

- `examples/cifs_mount_comparison.rs` — role-based 传输入口，`--transport client` 走 CIFS backend，
  读 `CIFS_REAL_SERVER` / `CIFS_REAL_SECOND_SERVER` / `CIFS_REAL_SHARE` / `CIFS_REAL_USER` / `CIFS_REAL_PASS`。
- `tests/cifs_policy_contract.rs` — 同一组 CIFS_REAL_* 环境变量下运行的真实双 LIF 策略测试 (`#[ignore]`)。
- 无外部依赖的单测：`src/storage/backends/cifs/*_tests.rs` (in-memory protocol)。
- skill：`.claude/skills/e2e-cifs/` (需要 `.env` 填 CIFS_REAL_*)。

## 改 CIFS 时

1. 读本 doc + `src/storage/backends/cifs/` (protocol / source / staged / metadata / namespace)。
2. 调 `backend-specialist` agent 传 `cifs`。
3. 改完跑 `make e2e-cifs` (需测试服务器)，否则至少 `make clippy && make test`。
4. CIFS 不在 `StorageEnum` 里；新增对外能力走 role (`Namespace` / `Metadata` / ...) 而不是 enum 分派。

## 签名策略

`CifsBackendConfig.signing_policy` 使用公开的 `CifsSigningPolicy`：

- `WhenRequired`（枚举默认值）：对端允许时省略普通消息签名及未签名响应的验签。
- `Required`：要求签名或加密，保留强制完整性保护的选择。

策略通过 smb-rs 公共 Client 构造入口传入，由依赖协商和执行，backend 不访问 wire/runtime。
服务端要求签名时始终遵守；SMB 3.1.1 TREE_CONNECT、认证/绑定及加密完整性保护保留。
无加密且省略签名时，普通流量不具备 SMB 消息完整性保护。
基准例子提供 `--signing required|when-required`，默认使用协商策略。
