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
**跑之前先确认 SVM 能发现域控**：`vserver cifs domain discovered-servers` 为空时，任何未知用户的
登录都会等约 2 秒后拿到 `0xC0000466 STATUS_SERVER_UNAVAILABLE` —— guest 映射发生在"确认用户在域里
无效"之后，DC 不可用就走不到那一步。2026-09-21 的 FAS2750 正处于这个状态，而其余测试照常通过，
因为测试账号是本地用户、不经过 DC。这一档也不在 `run.py` 的默认步骤里 (`.env` 不设
`CIFS_REAL_GUEST_POLICY`)。详见 `.claude/docs/storage-cifs.md` 的"真实环境证据"表。

恢复状态在目的端（ADR-0006 C11；C21 删了本地恢复存储，运行处什么都不记）：每个用例成功后断言最终文件旁没有
`.data-mover-*` 残留。

## 步骤

1. `cargo build --release --example cifs_mount_comparison`
2. `cargo test --release --test cifs_policy_contract -- --ignored --nocapture`
   - Checkpointed：指针在目的端、按记录前缀续传、成功后无 artifact 残留
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

## 打开协议日志

三个 `cifs_*` 测试都调用 `tests/common/init_tracing()`，按 `RUST_LOG` 开关，不设时零开销：

```bash
RUST_LOG=smb=debug cargo test --release --test cifs_namespace_contract -- --ignored --nocapture
RUST_LOG=smb=trace ...   # 加上原始帧，定位未知 NTSTATUS 时需要
```

真机故障往往是间歇的，重跑未必复现；日志是唯一的现场记录。

## 失败如何排查

- 认证失败 → 只支持 NTLM；检查 CIFS_REAL_USER / CIFS_REAL_PASS
- 服务器要求签名而失败 → `CifsSigningPolicy::Required`，见 `.claude/docs/storage-cifs.md` "签名策略"
- 句柄泄漏 (长跑后 hang) → `protocol.rs` 的 `close_resource` 检查
- 续传不生效 → 看最终文件旁 `.data-mover-<d>.pointer` 是否在、`prepare` 是否报 Restarted 及原因；
  跑 `DEST=smb: bash .claude/skills/_shared/resume_matrix.sh`
