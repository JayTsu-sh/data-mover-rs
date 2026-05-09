---
name: e2e-nfs
description: 验证 NFS backend — 跑 examples/nfs_walkdir + nfs_export + nfs_opt_dir。重点验证 retry taxonomy (EACCES/EPERM → deny_list, EAGAIN/ECONNRESET → delay_backoff, commit 7eb3046)。
---

# e2e-nfs

**触发关键词**：验证 NFS / 测 v3 / 测 v4 / 测 retry 分类 / e2e nfs。

## 输入 (.env)

```
NFS_HOST=nas01
NFS_PORT=2049
NFS_EXPORT=/data
NFS_UID=1000
NFS_GID=1000
NFS_DENY_DIR=/data/no-access     # 用于测 EACCES (服务器侧设置 0700 root-only)
```

## 步骤

1. cargo build --example {nfs_walkdir, nfs_export, nfs_opt_dir}
2. nfs_walkdir 遍历 export — 应正常返回。
3. nfs_export 查询 export 信息 — 应返回 export 列表。
4. nfs_opt_dir 创建优化目录结构 — 应成功 (含已存在目录)。
5. **retry taxonomy 验证**：访问 NFS_DENY_DIR (设为 0700 root-only) — 应直接 PermissionDenied，**不重试**。

## 成功判据

- 1-4 退出码 = 0
- 5 错误必须是 `PermissionDenied`，且日志中**不应有** retry/backoff 字样

## 失败如何排查

- ENOENT mount → 检查 export 是否启用，`/etc/exports` 配置
- EACCES 走了 retry → backend 错误映射回归，看 `src/nfs.rs` errno → StorageError 映射
- moka 缓存 stale → 改 attr 后没 invalidate，看 nfs.rs 的 cache invalidation
- v3 vs v4 协商失败 → nfs-rs crate 行为，可能要降级 v3
