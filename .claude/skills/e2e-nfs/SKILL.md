---
name: e2e-nfs
description: 验证 NFS backend — 跑 examples/nfs_walkdir + nfs_export + nfs_opt_dir。可选验证拒绝访问的目录报权限错误；「不重试」由单测覆盖。
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
# 可选：export 下一个 NFS_UID 读不了的目录（相对 export，服务器侧 0700 属 root）
NFS_DENY_DIR=no-access
# 可选：原样追加到 URL 查询串；含 `&` 的值要加单引号（metadata_matrix.sh 用 bash source 读 .env）
NFS_OPTIONS='version=4.1&noresvport=true'
# 可选：只遍历 export 下这个子目录（默认整个 export）
NFS_WALK_PATH=
```

注释只能单独成行：`env_loader.py` 不剥行尾 `# ...`。

nfs-rs 默认 NFSv3；服务器开了 `mount_rootonly` 又经 NAT（WSL2）访问时 v3 MOUNT 被拒（AUTH_TOOWEAK），用
`NFS_OPTIONS=version=4.1&noresvport=true`。`noresvport=true` 也让 `nfs_export` 的 MOUNT 查询不去绑特权端口
（非 root 绑不了，报 `Permission denied (os error 13)`）。FAS2750 的现成配置记在仓库外的实验室环境文件里。

**写入范围**：只有 `nfs_opt_dir` 写，它只在 URL 根下自建 `dm-e2e-opt-dir-<纳秒>`（已存在则拒绝），建
`dir1/dir2/dir3`，再经以该目录为根的第二个连接建一次（新根句柄、句柄缓存为空，每级都真的对已存在目录发
`mkdir`），最后只删这个目录，要求每个删除事件都成功、且含这个目录本身。`nfs_walkdir` / `nfs_export` 只读。

## 步骤

1. cargo build --example {nfs_walkdir, nfs_export, nfs_opt_dir}
2. nfs_walkdir `--url` 遍历 export（或 `NFS_WALK_PATH`）— 应正常返回。
3. nfs_export 以同一 URL（带选项）查询 export 列表 — 应返回 export 列表。
4. nfs_opt_dir 在自己的临时目录里建再删嵌套目录 — 应成功 (含已存在目录)。
5. **拒绝访问**（可选，需 `NFS_DENY_DIR`）：遍历该目录 — 应报权限错误并以非 0 退出。`NFS_UID` 必须真的
   读不了它：`NFS_UID=0` 且服务器不 squash root 时这一步会失败。「权限错误不重试」由 `src/nfs.rs` 的
   `test_is_retryable_with_invalidation_excludes_unrelated` 单测覆盖，本步不观察重试日志。

6. **ACL / xattr 拷贝策略矩阵**（可选，需 `NFS_META_V41_EXPORT`）：
   `bash .claude/skills/e2e-nfs/scripts/metadata_matrix.sh`。用 `examples/nfs_metadata_copy.rs`
   验证 `CopiedMetadataRequest`（要拷哪些功能）：NFSv3 源要 ACL + xattr → 拷贝成功、两族记
   `Unsupported`（源端读不了）；NFSv4.1 同端要 ACL → 源端打标记的 ACL 原样到达（raw GETACL 回读比对，
   不要 ACL 作阴性对照必须失败）；要 xattr 而该导出没协商 named attributes → 成功、记 `Unsupported`；
   设了 `CIFS_REAL_*` 时再验 NFSv4 → CIFS 要 ACL → 成功、记 `Unsupported`（编码不同，不做转换）。
   语义见 `.claude/docs/metadata-negotiation.md`。

## 成功判据

- 1-4 退出码 = 0（`nfs_walkdir` 遍历中有任何错误即非 0）
- 5 退出码 = 1，输出含权限错误（`NFS3ERR_ACCES` / `NFS4ERR_ACCESS` / permission）
- 6 末行 `RESULT pass=N fail=0`，退出码 0

## 失败如何排查

- ENOENT mount → 检查 export 是否启用，`/etc/exports` 配置
- EACCES 走了 retry → backend 错误映射回归，看 `src/nfs.rs` errno → StorageError 映射
- moka 缓存 stale → 改 attr 后没 invalidate，看 nfs.rs 的 cache invalidation
- v3 vs v4 协商失败 → nfs-rs crate 行为，可能要降级 v3
