# NFS Backend

## 底层依赖

- **crate**：`nfs-rs` (0.2.0，crates.io)。
- 支持 NFSv3 和 NFSv4 (自动协商 / 升级)。
- 属性缓存用 `moka` (0.12.15)，减少冗余 GETATTR RPC。

## URL 形式

```
nfs://host[:port]/export[/sub/path][?uid=N&gid=N]
```

示例：

```
nfs://nas01/data
nfs://nas01:2049/exports/home?uid=1000&gid=1000
nfs://nas01/exports/home:/prefix?uid=1000&gid=1000
```

`:/prefix` 语法分隔 export 和 mount 后的子路径。

## URL 参数

| 参数 | 默认 | 含义 |
|---|---|---|
| `uid` | 1000 | 操作时使用的 Unix uid (RPC AUTH_SYS) |
| `gid` | 1000 | 操作时使用的 Unix gid |

## Retry Taxonomy (核心)

NFS 错误必须按 commit `7eb3046` 的分类映射：

| errno | 分类 | 行为 |
|---|---|---|
| `EACCES` | `deny_list` | 不重试，直接返回 `PermissionDenied` |
| `EPERM` | `deny_list` | 不重试，直接返回 `PermissionDenied` |
| `EAGAIN` | `delay_backoff` | 应用层指数退避重试 |
| `ECONNRESET` | `delay_backoff` | 应用层指数退避重试 |
| `ETIMEDOUT` | `delay_backoff` | 同上 |
| `ENOENT` | (映射) | 返回 `FileNotFound` 或 `DirectoryNotFound` |
| `ENOSPC` | (映射) | 返回 `InsufficientSpace` |

**修改 retry 决策必须同步更新本表 + [error-taxonomy.md](error-taxonomy.md)**。

## NfsEnrich

- 结构在 `lookup` vs `walkdir_2` 中行为不同 (按站点配置)。
- `lookup` — 单条目的 ACL/owner/xattrs 完整 enrich。
- `walkdir_2` — 批量条目可能裁剪 enrich 字段以提速。
- 改这里前先看现有逻辑的两个分支。

## Mount 保活

- NFS 需要 mount handle 持续活着，不能断开重连随便用。
- `NFSStorage` 内部持有 mount，drop 时自动 unmount。
- 长时空闲的 mount 可能被服务器踢，需要 keepalive。

## moka 缓存

- 缓存 NFS attr (mode/mtime/size 等) 减少 GETATTR。
- TTL 在 NFSStorage 构造时配置。
- **改 attr 后必须 invalidate**，否则下次 GETATTR 会读到 stale。

## 公开导出

- `create_nfs_storage_ensuring_dir` (commit `b11ce9d`) — 创建 NFSStorage 同时确保目标目录存在。在 lib.rs 顶层 `pub use`。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| EACCES 被当成可重试 | 已修，必须 deny_list |
| GETATTR 读到 stale | 改 attr 后调用 invalidate |
| mount 被踢 | 长 session 加 keepalive |
| uid/gid 默认 1000 不匹配服务器 | URL 参数显式指定 |
| v3 vs v4 差异 (例如 `setattr` 字段) | 走 `nfs-rs` 的协议无关接口 |

## 测试

- `examples/nfs_walkdir.rs` — 遍历 export。
- `examples/nfs_export.rs` — 查询 export 元信息。
- `examples/nfs_opt_dir.rs` — 创建优化目录结构。
- skill：`.claude/skills/e2e-nfs/` (需要 `.env`)。

## 改 NFS 时

1. 读本 doc + [error-taxonomy.md](error-taxonomy.md) 的 NFS 段。
2. 改 retry 决策必须更新两个 doc 的映射表。
3. 调 `backend-specialist` agent 传 `nfs`。
4. 改完跑 `make e2e-nfs` (需测试服务器)。
