# Local Backend

## 底层依赖

- **`cap_std::fs`** 能力目录句柄 + `std::fs`；阻塞操作交给受控执行器。
- Windows ACL 位于 `src/storage/backends/local/windows_acl.rs`；Unix ACL 经文件描述符读写 POSIX xattr。
- 软链接支持 (`symlink_metadata` vs `metadata`)。

## URL 形式

任何 **不带 scheme 的本地路径** 都视为 Local：

```
/abs/path
./rel/path
~/expanded
C:\Windows\path  (Windows)
```

库调用方使用 `BackendConfig::Local`；示例中的路径 / URL 解析位于 `examples/support/endpoint.rs`。

## 关键行为

### 递归删除

`storage::delete_tree` 通过 `Namespace` 角色编排删除，先删除叶子，再按深度删除目录；进度和失败由流中的结构化事件报告。

### Unix 元数据

完整提取：
- `uid` / `gid` / `mode` (`MetadataExt::mode()`)。
- `dev + ino`（运行期对象校验和传输恢复绑定，不保证跨重新挂载稳定）。
- `mtime` / `atime` / `ctime` (走 time_util)。

### 跨扫描改名证据

Unix Local 为已有 metadata 中包含可表示创建时间的条目提供独立的 `RenameEvidence`：
`ino_u64_be || btime_ns_i64_be`，固定 16 字节内联保存。列举、Stat、直接观察和读取描述符
复用同一编码，不逐文件追加 metadata 系统调用。创建时间缺失或溢出时没有候选；合法零值和负值保留。
连接时缓存根目录设备号，子项属于其他设备（嵌套挂载）时不生成候选。Windows 不提供此方案。

证据质量为启发式，默认只提供候选。调用方若明确接受启发式匹配，须通过
`LocalBackendConfig.rename_context` 提供稳定的数据集域与 `AllowHeuristic` 策略；更换共享或介质时
必须轮换域或 epoch。该上下文通过 Namespace 暴露，并参与续扫指纹；持久化扫描基线的消费方也须
单独保存上下文。硬链接和其他重复候选必须整体按歧义回退，改名后内容变化仍须独立判断。

本地挂载 CIFS/NFS 同样走 Local 角色。不得因挂载类型或 `serverino` 自动信任候选：CIFS 同一共享
覆盖多个服务端文件系统时可能发生 inode 冲突，创建时间也可修改；NFS 挂载没有 btime 时回退路径匹配。
`SourceIdentity` 及其恢复和 Direct 同文件保护继续使用原来的 `dev + ino`，不使用改名候选替代。

### 作为目的端设 owner

非 root 且没有 `CAP_CHOWN` 时，只能把文件留给自己（uid 是自己、gid 是所在组）。`LocalStagedDestination` 连接时
读一次（`staged/owner_privilege.rs`），`may_set_owner` 逐文件回答；不能设的只拷 mode（去 set-id），报
`OwnerAndGroupNotPermitted`。详见 [metadata-negotiation.md](metadata-negotiation.md)「目的端没有 chown 特权」。

### Windows 元数据

通过 `windows_acl.rs` + `windows-sys` (`#[cfg(windows)]`)：
- 元数据角色使用 `AclMetadata::WindowsSecurityDescriptor`。
- 观察和暂存写入在能力目录内打开句柄，再读取 / 设置安全描述符。
- `storage::windows_acl` 保留独立的 ACL / SDDL 工具入口。

### Symlink

- `symlink_metadata` 不跟随 symlink，`metadata` 跟随。
- role traversal 暴露 `EntryKind::Symlink`。

### `block_size` (可选)

- 创建大文件时可选用于 pre-allocate (`fallocate` Linux)。
- 不在 URL 中暴露，是内部 API 参数。

### Namespace 角色 (role-based，`src/storage/backends/local/namespace.rs`)

`connect_backend(BackendConfig::Local)` 出借的 `LocalNamespace` 与 `LocalObservationAdapter`
共用同一个 `cap_std::Dir` (连接时 canonicalize 一次)。

**不跟随 symlink，任何一段都不**：路径从根能力开始逐段用
`cap_primitives::fs::open_dir_nofollow` 打开 (直接依赖 `cap-primitives`，与 `cap-std` 同版本)，
某一段是链接就失败 (`InvalidInput`：unix 上是 `ENOTDIR`，Windows 上是
`ERROR_STOPPED_ON_SYMLINK`，都显式映射)；最后一段在父目录句柄里操作。这同时堵住了 `List` 看到目录
之后、被换成链接再 `Delete` 的竞态。`cap-std` 的 `Dir` 方法只保证最后一段 (或都不保证)，
不要改回 `root.read_dir(path)` / `root.remove_file(path)` 这类整路径调用 —— reviewer 在
P1 复现过：`delete_tree` 的根是链接时会删光链接目标里的文件。

| 动词 | 行为 |
|---|---|
| `List` | **逐页流式** (P7 C5，`namespace/listing.rs`)：打开目录后 `entries()`，每页一次 `spawn_blocking`，把打开的 `ReadDir` 带进带出，读至多 `page_hint` 个子项 (描述成功 + 子项失败；隐藏的 artifact 不占名额) 就交出；两页之间只占一个目录 fd、不占 blocking 线程。每页之前与等待交出时看取消令牌，取消时先关 fd 再报 `Cancelled` 信号；消费方 drop 流后下一次交出失败即退出、关 fd，**不读到底**。读目录本身出错 (`getdents`) 结束列举：先交出已读到的子项，再报列举失败 (遍历记 `Interrupted { delivered }`，一项未交出则 `Unreadable`；`NotConnected` / `BrokenPipe` 仍升会话)；不可续读 (rustix 出错后不再给条目)。下钻不带提示，子目录按名字从根逐段重新打开。逐子项 `DirEntry::metadata()` (fstatat，不跟随)：描述符的身份、backend fact、content version、时间戳与观察角色同源；unix 上 `inline_mode = mode & 0o7777`，另带 `inline_ownership`（uid / gid / 完整 mode，与观察角色同源的 lstat；`observe_listed` 据此回答只要 ownership / timestamps 的计划，遍历不再每条目再 lstat 一次，P7 C0）。过滤 `.data-mover-*` 传输临时文件（计入页的 `internal_hidden`）。子项路径按契约拼成“请求的目录 + `/` + 名字”，不做规范化（`./d` 列出 `./d/x`，P7 C1）。子项拼不成 `StoragePath` (非 UTF-8、超长) → 无名子项失败 (`ChildFailure::unnamed`，带原始字节)；读目录到 lstat 之间消失的子项报 `ChildFailure::vanished` (遍历计入 `vanished`、不发项，P7 Q12)。`delete_tree` 整目录收集：读目录中途出错时整个列举失败 (此前记成目录上的子项失败、已读到的子项照常处理) |
| `Stat` | 父目录句柄里 `symlink_metadata`；根用 `dir_metadata` |
| `ReadLink` | `read_link_contents` (原样返回链接文本；`read_link` 会把绝对目标当越界拒绝) |
| `CreateDirectory` | 只建一级；已存在 → `Conflict`，父缺失 → `NotFound`；之后同步父目录 |
| `Delete` | 父目录句柄里先不跟随判类型：目录 `remove_dir` (只删空目录，非空 → `Conflict`)，文件/链接 `remove_file`；Windows 上目录链接回退 `remove_dir`。**从不递归**。不做目录同步 —— `delete_tree` 否则每个条目付一次 fsync |
| `Rename` | 两端父目录各自不跟随打开后 `rename`，同名文件直接替换；之后同步目标 (与源，若不同) 父目录 |

- 根目录 (`""` 或只由 `.` 组成) 只允许 `List` / `Stat`；`..`、绝对路径、`.data-mover-*` 组件
  一律 `InvalidInput`。
- `NotConnected` / `BrokenPipe` (网络挂载断开) → session 失败，其余 → entry 失败；
  `DirectoryNotEmpty` → `Conflict`，`NotADirectory` → `InvalidInput`，共享的 `classify_io` 不变。
- `CreateDirectory` / `Rename` 成功后目录同步失败，照 §10 报为该动词失败；调用方若重试
  `Rename` 会得到 `NotFound` (已经移走了)。
- `Delete` 目录时若只剩被隐藏的 `.data-mover-*`（经 `List` 看是空的），先把它们删掉（artifact 目录
  连内容，不跟随符号链接）再删目录；只要还有可见子项就照旧 `Conflict`，旁边的 artifact 一个不动
  (ADR-0006 C3b)。代价：同一目录里并发写入的 stage 会被一起删 —— 调用方契约本就是一个 key 一个写者，
  且调用方正在删这个目录。
- Windows：`Rename` 到已存在的目录会被拒 (报 `PermissionDenied`，不重映射)；只读文件 `Delete`
  失败；`cap-primitives` 打开目录不带 `FILE_SHARE_DELETE`，某个动词持有目录句柄期间，另一个
  任务对该目录的 rename/delete 会遇到共享冲突 (`Protocol`)。`delete_tree` 按深度倒序删目录，
  不会撞上，但遍历与 `Rename` 并发时可能；Windows 运行时语义需要 Windows 主机验证。
- 测试接缝：`#[cfg(test)] LocalNamespace::fail_list_call(n)` 让第 n 次 `List` 报
  `PermissionDenied`，替代原 `LocalTraversalSource` 的 `EnumerationProbe`。这是 backend 内部
  接缝，不是网络 mock (T3 不适用)。

### 目的端恢复（ADR-0006 C8，`src/storage/backends/local/staged/at_destination.rs`）

续传状态全在最终文件旁，运行 data-mover 的机器上什么都不存（C21 起所有后端都如此，本地恢复存储已删除）。
- 名字只由最终文件名决定：`.data-mover-<d>.stage` / `.pointer`（+ 固定 `.pointer.tmp`）/ `.claim`，
  `<d>` = `artifacts::final_name_digest`。列举会隐藏它们。
- 指针即检查点：`DMDPTR01{binding, 传输标识, 持久前缀, "DMLSTG03"}`，只在 stage `sync_data` 之后写；
  续传时把 stage 截到这个前缀。无前缀或无标记的指针按损坏清理。
- claim：prepare 先取（flock，锁后比 dev/ino），被占 → `Conflict` / 瞬时；stage 活着就一直持有；删除时持锁删名字。
  flock 只在同一内核内有效：NFS / drvfs（`/mnt/c`）上的 Local 路径只靠调用方契约与进程内租约。
- artifact 名字处只接受普通文件：symlink / 目录 → `Conflict`，目标不动。
- 真机矩阵：`DEST=<目录> bash .claude/skills/_shared/resume_matrix.sh`，应见 `prepare=Resumed`、
  `reused_bytes > 0`、BLAKE3 相等、成功后无 `.data-mover-*`。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| Windows 路径 `\` vs Unix `/` | URL parse 阶段统一为 `/` |
| Windows ACL 没有等价 mode | 走 Windows security descriptor 表示 |
| 跨设备 rename (EXDEV) | 应走 copy + delete fallback |
| 软链接循环 | 遍历不下钻符号链接 |

## 测试

- `tests/lab/tools/storage_role_operations/` — Local 遍历、创建与删除。
- `examples/storage_namespace.rs` — 优化目录结构。
- `tests/test_copy_file_cancel.rs` — 角色传输的预取消和传输中取消契约。
- `tests/test_storage_type.rs` — 公开后端配置契约。
- `tests/local_namespace_contract.rs` — 公开 API 走 Local namespace：`create_directory_all` / `delete_tree` (含目录链接不删目标) / `StorageTraversalSource`。
- skill：`.claude/skills/e2e-local/` — **无外部依赖，CI 跑这个**。

## 改 Local 时

1. 读本 doc + `src/storage/backends/local/` + 必要时 `src/storage/backends/local/windows_acl.rs`。
2. Windows 相关改动必须 cfg 包裹。
3. 保留能力目录约束和阻塞执行器边界。
4. 调 `backend-specialist` agent 传 `local`。
5. 验证：`make e2e-local` (任何机器都能跑) + `make test`。
