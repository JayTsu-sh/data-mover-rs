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
- 依赖只有一份：`smb-domain = { package = "smb", git = JayTsu-sh/smb-rs, rev = 91e6cfb... }`
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

legacy `CifsStorage` 的能力在 role-based backend 里的去向 —— 有对应物的说清语义变化，没有的
说清为什么以及代价。2026-09-17 按删除前的 `git show b0c149b^:src/cifs.rs` 逐方法复核过一遍。

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
- 遍历有两条线并存。**条目流**由通用 `traversal::StorageTraversalSource` 驱动，filter DSL
  剪枝与 `max_depth` 经 `TraversalRequest.filter` / `.max_depth` 注入 (适配器
  `crate::DslTraversalFilter`)；除 `Entry` / `EntryFailure` 外还产出目录完成事件
  (`DirectoryListed` / `SubtreeComplete`，见 architecture.md §6)，消费方 match 时必须带通配臂
  —— `TraversalItem` 是 `#[non_exhaustive]`。**NDX 分页**由 crate 根的 `ndx_walk` 驱动 (legacy `walkdir_2`
  的中立替身)：走 `Namespace::List` 取数，复用 `dir_tree::run_dfs_driver` 的 DFS 栈 / 预读
  窗口 / NDX 与 gap 编号，产出 `NdxEvent`。两条线的 filter 适配不同 —— 前者是
  `TraversalDecision`，后者是 legacy `should_skip` 三元组 (`ReadContext.apply_filter` /
  `SubdirEntry.visible` / `need_filter`)。这是有意并存：其余四个 backend 的 walkdir_2 也还在
  legacy 那条线上。work-stealing 仍归上层编排。
- **source identity 用 64 位 SMB file id，`StableWithinBackend`** —— 与 NFS 的 file handle
  同一角色，也是 terrasync `clickhouse.rs` 里 `file_handle` 列 / `JoinStrategy::FileHandle`
  的输入 (2026-09-18 起，smb-rs PR #74)。消费端经 `ObservedEntry::source_identity()` →
  `SourceIdentity::stable_bytes()` 取字节 (只在 `StableWithinBackend` 时是 `Some`)。编码 16 字节
  大端、高 64 位补零：NTFS / Samba 上 (高位本来是 0) 与 legacy `file_id_to_handle` 逐字节一致；
  legacy 优先用 `FileIdExtd` 存完整 128 位，高位非零的服务器 (ReFS) 上两者不一致，base 表要
  重建。四条路径来源：
  - `list`：`FileIdFullDirectoryInformation` (64 位) → `FileIdExtdDirectoryInformation`
    (128 位取低 64) → 窄 class 的阶梯，`STATUS_INVALID_INFO_CLASS` 出现在流的**第一个元素**上，
    smb-rs 靠读首元素判定并降级。宽 class 随 QUERY_DIRECTORY 一起回来，**零额外往返**。
  - `describe` / `open` / `metadata`：CREATE 响应的 `QFid` create context (smb-rs 每次 open
    都附带)，同样零额外往返。
  统一 64 位而不是 128：QFid 只给 64 位，ReFS 上 Extd 的 128 位与之不一致，统一后同一对象
  的列举与打开在所有文件系统上一致 (这是 rename 检测对身份的一致性要求，见 smb-rs PR #74)。
  零值报 `None` → 退回路径级身份 (FAT 一类后端)。
  **两个来源相互独立，连接期探测一次钉死会话模式** (`protocol::probe_identity_mode`)：完整
  列举根目录一次，用根自己的 `.` 记录判断列举路径 (所以空根也能判)，再打开根本身判断打开
  路径；服务器不给 `.` 记录时退到首个子项，根打不开时退到前 3 个子项 (首个条目常是
  `$RECYCLE.BIN` 或别人的目录，单个 ACCESS_DENIED 不该关掉整个会话的 rename 检测)。只答其一的
  服务器上 List 与 Stat 会对每个条目给出不同身份 → 每个传输 `Conflict`，所以任一缺失就在
  **两条路径上都**关掉 file id，退回路径级身份。探测是能力检测，**永不让连接失败**：任一步出错
  (根不可列举的只写目录、`ensure_dir=false` 且根尚不存在、根与候选全被 ACL 拒绝) 或列举里
  无可判断的记录 → warn + 关掉 file id。关掉是保守值："接受宽 class 但不答 QFid"的服务器上
  默认开会分裂身份。之前"空根 → 关掉"让匿名 share 契约 (空 share 上先连接后建目录) 退回
  路径级身份，`.` 记录消除了这个环境依赖。列举**必须读到流尾**而不是取到首项就丢流，原因见
  "已知陷阱"的中途丢流一条。
  **变更检测与身份分离，但 `expected_source` 只比身份**：身份跨 rename **和跨编辑**都稳定
  (`source_tests::a_file_id_makes_the_identity_rename_stable`)，这正是 rename 检测要的性质；
  代价是 describe→open 之间源被原地编辑时 CIFS 不再报 `Conflict`
  (`source_tests::with_a_file_id_an_edited_source_still_opens_because_identity_is_the_file_id`)。
  NFS / Local 用 fh / dev+ino 做身份，本来就是这个语义；CIFS 此前靠 `len+written+changed`
  才有那道检查。`content_version` (= `identity_bytes`) 现在只进恢复绑定 (`engine.rs:1107`)；
  把它接进 `ReadRequest::expected_source` / `observe_bound` 是跨后端的模型工作，未做。
  **仍未覆盖**：ONTAP share 经 junction 跨卷时两个卷的 file id 理论上可碰撞；QFid 同时回
  `volume_id`，`ResourceMetadata::volume_id()` 已暴露但列举侧 (`FileIdFull`) 没有，所以身份
  暂不掺 volume id。要验证请在含 junction 的 share 上比对 `volume_id`。
- **describe 从 4 个往返降到 2 个，open (读游标) 从 3 个降到 1 个**：`CREATE + 2×QUERY_INFO
  (+ CLOSE)` → `CREATE (+ CLOSE)`。CREATE 响应本来就带四个时间戳、EndOfFile、FileAttributes、
  reparse 标志和 QFid，smb-rs 现在把它快照在 `ResourceHandle::opened()`，`opened_metadata()`
  同步读取。快照是 open 时刻的，写后不刷新 —— staged `size()` 每次重新 open 所以仍然新鲜；
  需要写后新值的路径用 `metadata()` (保留两次查询)。`Metadata::observe` 的 inline 记录
  (`CifsInlineMetadata.readonly`) 因此在 metadata 路径也有值了；但 `Namespace::Stat` 走的是
  `describe` → `CifsSourceFacts`，没有 readonly 字段，`inline_mode` 仍只在 list 路径有。
- **升级影响 (一次性，必读)**：CIFS 身份的 `identity_key` 变了 (strength tag 与字节都不同)，
  `content_version` 从 `None` 变为 `Some`。
  (1) 恢复记录按 recovery binding 的哈希做 key (`engine.rs` `recovery_store::open_existing(binding)`)，
  而 binding 同时含 `identity_key` 与 `content_version` (`engine.rs:1104-1110`)。CIFS 作源时两者
  都变 → 新 binding **查不到**旧记录 → `recover` 根本不会被调用，直接走全新 `prepare`。真实后果
  不是报错，而是升级前在途的 `Checkpointed` 传输在目标端留下的 stage 文件和 recovery-store 记录
  **静默孤儿化**。升级后 `discard_prior_recovery` 同样用新 binding 查，也找不到 —— 所以唯一有效
  的做法是**升级前**排空或 `discard` 所有以 CIFS 为源的在途传输；升级后残留只能手工清理目标端
  stage 与 recovery-store。(2) 旧 `EntrySnapshot` 的 key 全部不再匹配，升级后第一次增量会把所有
  CIFS 条目视为新增，一次性全量。
- 目录列举带回 `FILE_DIRECTORY_INFORMATION` 里已有的四个时间戳，挂在
  `SourceDescriptor::inline_timestamps` 上；`ObservationPlan` 只要时间戳时，遍历直接用它，
  不再对每个条目多发一次 `Metadata::observe` (N+1 → 1)。
- 无 `check_connectivity`：连接阶段 `connect_share` 已经做了认证 + tree connect，丢的只是
  会话建立之后的健康探测。`probe_server_time` 则**不是回归** —— legacy `CifsStorage` 从来没有
  这个方法 (旧 `src/cifs.rs` 零命中)，它是 D7 的要求项；真实环境证据那节也已论证不需要它。
- `Metadata` 只支持 `Timestamps` 与 `Acl`，numeric uid/gid/mode 标为 `Unsupported`：SMB 不暴露
  POSIX mode，FAS2750 unix 卷上的 mode 由服务端 name-mapping + umask 决定 (session 里
  `mapped_unix_user=lisauser`)，客户端无法观察/应用。
- **`FILE_ATTRIBUTE_READONLY` → 近似 mode，只在列举路径**。`QUERY_DIRECTORY` 记录带只读位，
  `namespace.rs list` 把它经 `smb_attributes_to_mode` (dir → 0o755/0o555，file → 0o644/0o444，
  照搬 legacy) 挂到中立的 `SourceDescriptor::inline_mode` 上，`ndx_walk` 再填进
  `NASEntry.mode`。stat 路径显式留 `None` 而不是伪造：`smb_domain::ResourceMetadata`
  (domain/mod.rs:845) 只有四个时间戳和长度，没有任何属性位，填 `false` 会让 stat 谎报"不是只读"。
  这个值是**展示级**的，不进 `MetadataObservations.ownership_mode` (那仍是 `NotApplicable`)：
  塞进去会让 transfer engine 以为可以 apply，目标端会被写错权限。apply 侧不做 (legacy 也没做)。
  **注意两条遍历线报的 mode 不一样**：`ndx_walk` 的 `NASEntry.mode` 有这个近似值，而
  `traversal::StorageTraversalSource` 产出的 `ObservedEntry` 根本不带 mode
  (`ownership_mode` 是 `NotApplicable`)。同一个条目、两条线、两种答案 —— 这是
  `inline_mode` 只服务于 NDX 输出的直接后果。
  根治要给 smb-rs 的 `RuntimeMetadata` / `ResourceMetadata` 加属性位 —— 底层 stat 本来就在查
  `FileBasicInformation`，数据已在响应里、零额外往返，适合和 file id 合成同一个上游 PR。
- **列举里的 reparse point 一律当普通文件**。`protocol.rs list` 明确不把它映射成 `Symlink`
  (facade 读不了 link target，`Symlink` 会把遍历送进不支持的 `ReadLink`)，所以
  `NASEntry.is_symlink` 恒 `false`，filter 表达式里的 `type == "symlink"` 在 CIFS 上恒不匹配。
  FAS2750 实测见下文"真实环境证据"的符号链接行：unix 卷上的 symlink 在 SMB 列举里本来就不带
  reparse 标记，客户端无从识别，所以这条在实测环境里不构成额外损失。
- 整文件 `read_file` / `write_file`、`set_file_len`、`.part` 续传、tar 直写按设计删除，
  由 staged `Checkpointed` / `AtomicReplace` 替代。
- 递归建目录 (legacy `create_dir_all`) 现在是 backend 无关的 `storage::create_directory_all`：
  逐层 `CreateDirectory`，`Ok` 与 `Conflict` 都算"这层已存在"，只在叶子层冲突时多花一次 `Stat`
  确认不是文件。legacy 的 `DirExistsCache` 不补 —— 跨调用缓存是调用方的会话状态。
- `update_metadata` 的 2-RT compound (legacy `evict_lease` + `compound_set_basic_info`) 没有
  对应物，现在是 open + set + close 三个往返，facade 不暴露 wire compound
  (`domain/batch.rs` 的 `Batch::execute` 是顺序 await，不是 SMB2 复合请求)。**正确性没问题**：
  当年那个 compound 的动机是 deferred-close 句柄的 sticky LastWriteTime 会盖掉 SetInfo，而
  `require_confirmed_close` + staged `write` 先确认关闭再 `apply_metadata` 已经把成因结构性消除。
  剩下的纯粹是每文件 1 个往返的差距。
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
| 匿名 / guest (legacy `anon`) | 2026-09-16 实测：设 `guest-unix-user=pcuser` 并建 share `dm_anon_share` (Everyone full_control) 后，未知用户 + 空密码被接受为 guest，`AllowUnsigned` 下 Namespace 契约全绿；`Deny` 下按预期拒绝。**2026-09-21 复测失败，原因在服务端而非代码**：SVM `lizy` 发现的域控数为 **0** (`vserver cifs domain discovered-servers` 空)，而 guest 映射发生在"确认该用户在域里无效"之后 —— DC 联系不上就走不到那一步，任何未知用户的 SESSION_SETUP 等约 2 秒后被回以 `0xC0000466 STATUS_SERVER_UNAVAILABLE` (字面义即"域控不可用")。其余测试不受影响，因为测试账号 `LIZYAD\lisauser` 是**本地**用户 (RID 1008)，本地校验不经过 DC。(该状态码此前未建模，表现为 `OutcomeUnknown` + 连接被杀；已由 smb-rs PR #81 修正，probe 行随之从 `rejected (Operation outcome is unknown ...)` 变为 `rejected (Unexpected message status: Server Unavailable (0xc0000466).)`) | **已实现** (`guest_policy`)。DC 恢复后这一档应自动可用 |
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
| file id (2026-09-18，smb-rs #74 → main `10de18c`；连接期 `probe_identity_mode` 判定两路都有 → 会话启用) | `[identity] strength=StableWithinBackend (List) / StableWithinBackend (Stat)`：ONTAP 接受 `FileIdFullDirectoryInformation` 宽 class，**且回答 QFid create context**；`real_share_namespace_verbs_roundtrip` 里 rename 前后 `identity_key` 相等；policy 契约 (64 MiB Checkpointed / AtomicReplace) 在 2-RT describe/open 上通过 | **已实现**。rename 检测不再退回 Path 模式 |
| NDX 分页遍历 (2026-09-18) | `storage_role_operations --backend cifs --root <scratch> ndx-walk --entries`：`seed --path sub` 后 2 页 DFS，NDX `0(sub) → gap 1 → 2,3,4 → gap -1`，每个条目 (含目录) `identity=StableWithinBackend`，时间戳为真值；`--max-depth 1` 只出根页且 `sub` 作为条目发出；`delete-tree --delete-root` 3 文件 + 1 目录零失败 | **已实现** (`storage::ndx_walk` + `create_directory_all` 建父目录) |

复现：`.claude/skills/e2e-cifs` (`CIFS_REAL_*`)。新能力的真机链路用
`examples/storage_role_operations` 的 `seed → traverse → compare → delete-tree`。
注意 `CIFS_POLICY_TEST_BYTES` 里的 64 MiB **等于**默认 checkpoint 间隔，日志会是
`SkippedBelowCheckpointThreshold`；要真正压到恢复路径需给一个更大的值 (实测 192 MiB 起
`recovery=Checkpointed`)。symlink probe 需要 `CIFS_PROBE_NFS_URL`
(`nfs://<lif>/<vol>:/?uid=0&gid=0&noresvport=true`)；WSL2 NAT 会改写源端口，ONTAP
`mount_root_only=true` 时会 `AUTH_TOOWEAK`，测试期间需临时关闭并事后恢复。

## 升级 smb-rs 依赖

> 2026-09-23：`ced48ff` → main 顶端 **`91e6cfb`**（#82 `c81c75a` runtime / CMAC 签名批处理 / crypto
> executor / session setup / dialects / domain security，#83 预备阶段取消修复）。协议层变更，第 4 步完整
> 矩阵在 `91e6cfb` 上跑了两轮：policy contract ×2、namespace ×2、probe ×2，全过；匿名 share 一档未跑
> （`.env` 不设 `CIFS_REAL_GUEST_POLICY`，且 SVM 域控发现问题见下方证据表）。smb-rs 自己的 CI 对该提交
> 为绿。data-mover 代码无需改动。`Cargo.lock` 随 sspi 0.21.3 更新了一串 crypto 预发布依赖
> （ed25519 / rsa / p256… / signature 3.0.0）并把 uuid 升到 1.26.1（semver 范围内）。
> **行为变化**：domain `query_security` / `set_security` 只保留 account 类 ACE（域账户 S-1-5-21-…、
> POSIX 映射 S-1-22-1/2-x、Everyone、SYSTEM），其余（BUILTIN\Administrators、CREATOR OWNER…）在读与写
> 两侧都被丢弃 —— smb-rs 有意为之，用户确认 CIFS→CIFS 只复制 account ACE，不记为损失。
>
> 2026-09-18 (三)：smb-rs **PR #78** (issue #77) 合入 main，顶端 `79d50c0`：连接被 drop (未
> `close()`) 后恢复任务不再对已失效的 `Weak` 按退避重试三次并误报 `AttemptsExhausted`；
> bootstrap 报 `Closed` 即结束恢复。真机验证过恢复本身可用：generation 退出后 reconnect、会话
> 重认证、tree 恢复毫秒级完成，同一 `Share` 继续可用。变更面 `connection.rs`、
> `runtime/recovery_driver.rs` → 协议层，第 4 步完整矩阵在 `79d50c0` 上跑过。
>
> 2026-09-18 (二)：smb-rs **PR #76** (issue #75，中途丢流 / 未建模 NTSTATUS 致命退出、恢复等待
> 无界) 合入 main，顶端 `8b10f35`。变更面 `runtime/engine.rs`、`runtime/recovery_driver.rs`、
> `resource/directory.rs`、`smb-msg/header.rs` → 协议层，第 4 步完整矩阵在 `8b10f35` 上跑过
> (namespace ×2、policy ×2、匿名 share ×1、probe ×1、8 轮 seed→ndx-walk→delete-tree)。
>
> 2026-09-18 (一)：file id / CREATE 快照经 smb-rs **PR #74** 以 merge commit 合入 main，
> 顶端 `10de18c` (树与分支 head `7585e30` 相同)。第 3、4 步在 `7585e30` 上跑过
> (namespace ×3、policy ×3、probe ×1)，重钉后又在 `10de18c` 上各跑一次。

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
   - `cifs_namespace_contract` 以 `CIFS_REAL_GUEST_POLICY=allow-unsigned` + 空用户名跑匿名 share
     —— **先确认 SVM 能发现域控** (`vserver cifs domain discovered-servers` 非空)，否则这一档必然
     报 `0xC0000466`，那是域控不可用而不是升级回归 (2026-09-21 实测，见上方证据表)。
     `skill` 的 `.env` 默认不设 `CIFS_REAL_GUEST_POLICY`，所以 `run.py` 本来就不跑它；
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
| **中途丢弃 `entries()` 流** (取到首项就 return) | 曾让紧随其后的第一个操作报 `Invalid state: only the Connection dependency may wait in this recovery stage`，或整个进程挂住。真机复现 + generation 退出事件定位到根因是 **`Wire("invalid-status")`**：消费端取空缓冲的瞬间 fetch_loop 就发下一条 QUERY_DIRECTORY，drop 流触发 CANCEL，服务器在 CLOSE 之后用 `STATUS_FILE_CLOSED` (0xC0000128) 回它，这个状态不在 smb-msg `Status` 枚举里，`Header::status()` 失败被 engine 判为致命 → generation 退出 → 恢复期。**已修 (smb-rs #76，钉 `8b10f35`)**：未建模 NTSTATUS 不再致命，draining 操作的迟到响应不受状态合约约束；流 Drop 先 cancel 再唤醒、fetch_loop 醒来先查 token；恢复等待无 deadline 时用 `total_timeout` 兜底，`recover()` 所有失败分支都释放等待者；generation 退出 / 恢复起止有 tracing 事件。data-mover 的 `list` / `probe_listing` 仍读到流尾再 close —— 少一次无人接收的往返，不依赖上游行为。**仍开着的**：恢复期内 Session/Tree 依赖的操作是立刻报 `DependencyNotConnection` (到我们这里是 `Protocol`/Unknown)，而不是等毫秒级的恢复完成，与 smb-rs 自己的不变量文档相悖，见 [smb-rs #79](https://github.com/JayTsu-sh/smb-rs/issues/79)；在此之前上游把它当可重试处理 |

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
