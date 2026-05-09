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
- skill：`.claude/skills/e2e-local/` — **无外部依赖，CI 跑这个**。

## 改 Local 时

1. 读本 doc + `src/local.rs` + 必要时 `src/acl.rs`。
2. Windows 相关改动必须 cfg 包裹。
3. Rayon 调用必须在 spawn_blocking 内。
4. 调 `backend-specialist` agent 传 `local`。
5. 验证：`make e2e-local` (任何机器都能跑) + `make test`。
