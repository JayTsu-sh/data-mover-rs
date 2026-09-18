# Local Backend

## 底层依赖

- **`std::fs`** + `tokio::fs` (异步) + `rayon` (并行删除)。
- ACL 走 `src/acl.rs` (Unix/Windows 抽象)。
- 软链接支持 (`symlink_metadata` vs `metadata`)。

## URL 形式

任何 **不带 scheme 的本地路径** 都视为 Local：

```
/abs/path
./rel/path
~/expanded
C:\Windows\path  (Windows)
```

`detect_storage_type` 在 `lib.rs` 实现 7 路判别 (Unix/Win/NFS/S3 basic/https/http/hcp/relative/empty)，详见 `tests/test_storage_type.rs`。

## 关键行为

### Rayon 并行删除

`delete_dir_all_with_progress` 用 rayon 并行遍历删除。

- 适合大目录 (>10K 文件) 加速。
- **进度回调必须线程安全** (内部 Atomic 计数)。
- 与 tokio 共存：rayon spawn 在 `spawn_blocking` 包装的线程池里。

### Unix 元数据

完整提取：
- `uid` / `gid` / `mode` (`MetadataExt::mode()`)。
- `ino` (inode number，作为 join key)。
- `mtime` / `atime` / `ctime` (走 time_util)。

### Windows 元数据

通过 `acl.rs` + `windows` crate (`#[cfg(windows)]`)：
- ACL → SDDL 字符串。
- File attributes (Hidden / System / ReadOnly)。
- WinAceError → 走 `StorageError::WinAceError`。

### Symlink

- `symlink_metadata` 不跟随 symlink，`metadata` 跟随。
- copy_file 默认跟随，可配置。
- walkdir 暴露 `is_symlink` 字段。

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
| `List` | 打开目录后 `entries()` + 逐子项 `DirEntry::metadata()` (不跟随)。描述符的身份、backend fact、content version、时间戳与观察角色同源；unix 上 `inline_mode = mode & 0o7777`。过滤 `.data-mover-*` 传输临时文件。子项拼不成 `StoragePath` (非 UTF-8、超长) → 带独立身份的逐项失败 (`NamespaceResult::Listing`)；读目录到 stat 之间消失的子项直接跳过 |
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
- 被隐藏的 `.data-mover-*` 孤儿临时文件会让所在目录删不掉：`delete_tree` 报 `Conflict`，
  但列举里看不到是哪个子项。
- Windows：`Rename` 到已存在的目录会被拒 (报 `PermissionDenied`，不重映射)；只读文件 `Delete`
  失败；`cap-primitives` 打开目录不带 `FILE_SHARE_DELETE`，某个动词持有目录句柄期间，另一个
  任务对该目录的 rename/delete 会遇到共享冲突 (`Protocol`)。`delete_tree` 按深度倒序删目录，
  不会撞上，但遍历与 `Rename` 并发时可能；本机无 Windows 工具链，Windows 编译靠 GitHub CI 的 `windows-latest` job 兜底。
- 测试接缝：`#[cfg(test)] LocalNamespace::fail_list_call(n)` 让第 n 次 `List` 报
  `PermissionDenied`，替代原 `LocalTraversalSource` 的 `EnumerationProbe`。这是 backend 内部
  接缝，不是网络 mock (T3 不适用)。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| Rayon 阻塞 tokio runtime | 在 `spawn_blocking` 内 spawn rayon |
| Windows 路径 `\` vs Unix `/` | URL parse 阶段统一为 `/` |
| Windows ACL 没有等价 mode | 走 `acl.rs` 的 SDDL 表示 |
| 跨设备 rename (EXDEV) | 应走 copy + delete fallback |
| 软链接循环 | walkdir 检测 inode 重复 |

## 测试

- `examples/local_walkdir.rs` — 递归 + md5。
- `examples/local_walkdir_2.rs` — walkdir_2 API 演示。
- `examples/local_opt_dir.rs` — 优化目录结构。
- `tests/test_copy_file_cancel.rs` — 取消语义 (用 /tmp 路径，无外部依赖)。
- `tests/test_storage_type.rs` — URL 判别 (纯单元测试)。
- `tests/local_namespace_contract.rs` — 公开 API 走 Local namespace：`create_directory_all` / `delete_tree` (含目录链接不删目标) / `ndx_walk` / `StorageTraversalSource`。
- skill：`.claude/skills/e2e-local/` — **无外部依赖，CI 跑这个**。

## 改 Local 时

1. 读本 doc + `src/local.rs` + 必要时 `src/acl.rs`。
2. Windows 相关改动必须 cfg 包裹。
3. Rayon 调用必须在 spawn_blocking 内。
4. 调 `backend-specialist` agent 传 `local`。
5. 验证：`make e2e-local` (任何机器都能跑) + `make test`。
