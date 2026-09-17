---
name: e2e-cifs
description: 验证 role-based CIFS / SMB backend — 编译 examples/cifs_mount_comparison 并在真实 share 上跑 tests/cifs_policy_contract (Checkpointed / AtomicReplace)。需要真 SMB 服务器和 .env (CIFS_REAL_*)。
---

# e2e-cifs

**触发关键词**：验证 CIFS / 测 SMB / e2e cifs / CIFS policy contract。

CIFS 只有 role-based 实现 (`src/storage/backends/cifs/`)，不在 `StorageEnum` 里，
没有 `smb://` URL；legacy `cifs_walkdir` / `cifs_copy` examples 已随 #150 删除。

## 输入 (.env)

```
CIFS_REAL_SERVER=nas01
CIFS_REAL_SECOND_SERVER=nas01-lif2      # 可选，缺省 = CIFS_REAL_SERVER
CIFS_REAL_SHARE=test-share
CIFS_REAL_USER=admin
CIFS_REAL_PASS=password
CIFS_POLICY_TEST_BYTES=4096,67108864    # 可选，逗号分隔文件大小
CIFS_PROBE_NFS_URL=nfs://nas01/share:/?uid=0&gid=0&noresvport=true   # 可选，probe 用它建 symlink fixture
CIFS_REAL_GUEST_POLICY=allow-unsigned   # 可选，对匿名/guest share 跑契约 (配合未知用户 + 空密码)
```

匿名 share 用法：`CIFS_REAL_SHARE=dm_anon_share CIFS_REAL_USER= CIFS_REAL_PASS= CIFS_REAL_GUEST_POLICY=allow-unsigned`
(FAS2750 上需 `vserver cifs options -guest-unix-user pcuser`，share ACL 给 Everyone)。

`DATA_MOVER_RECOVERY_DIR` 未设置时 runner 自动创建临时目录。

## 步骤

1. `cargo build --release --example cifs_mount_comparison`
2. `cargo test --release --test cifs_policy_contract -- --ignored --nocapture`
   - Checkpointed：checkpoint 落盘、claim、按记录前缀恢复
   - AtomicReplace：stage 关闭后原子 rename 发布
   - 目录 / 文件 mtime 复制、ACL best-effort
3. `cargo test --release --test cifs_namespace_contract -- --ignored --nocapture`
   - Namespace role：CreateDirectory / Stat / List / Rename (replace 语义) / Delete，
     以及 directory rename 与 ReadLink 的 typed `Unsupported`
4. `cargo test --release --test cifs_capability_probe -- --ignored --nocapture`
   - 只打印 `[probe]` 证据：匿名/guest 登录、签名策略、根目录列举、时钟偏差、ACL 查询

## 成功判据

- example 编译通过
- policy + namespace contract 全部通过 (退出码 0)，测试自行清理 fixture
- probe 退出码 0，`[probe]` 行作为能力取舍的证据记录到 `.claude/docs/storage-cifs.md`

## 失败如何排查

- 认证失败 → 只支持 NTLM；检查 CIFS_REAL_USER / CIFS_REAL_PASS
- 服务器要求签名而失败 → `CifsSigningPolicy::Required`，见 `.claude/docs/storage-cifs.md` "签名策略"
- 句柄泄漏 (长跑后 hang) → `protocol.rs` 的 `close_resource` 检查
- 恢复失败 → 检查 `DATA_MOVER_RECOVERY_DIR` 可写且两次运行一致
