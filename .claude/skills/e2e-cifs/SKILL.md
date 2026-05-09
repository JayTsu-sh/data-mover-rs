---
name: e2e-cifs
description: 验证 CIFS / SMB backend — 跑 examples/cifs_copy + cifs_walkdir，矩阵 smb2_only=[true,false] × anon=[true,false]。需要真 SMB 服务器和 .env。
---

# e2e-cifs

**触发关键词**：验证 CIFS / 测 SMB / 测 SMB2 / 测匿名 SMB / e2e cifs。

## 输入 (.env)

```
CIFS_HOST=nas01
CIFS_PORT=445
CIFS_SHARE=test-share
CIFS_USER=admin
CIFS_PASS=password
CIFS_ANON_SHARE=public          # 匿名 share (可选)
```

## 步骤

1. cargo build --example cifs_walkdir
2. cargo build --example cifs_copy
3. 矩阵：
   - `smb2_only=true, anon=false` (默认) — cifs_walkdir
   - `smb2_only=false, anon=false` — cifs_walkdir (兼容老 NAS)
   - `smb2_only=true, anon=true` (CIFS_ANON_SHARE 设了才跑)
4. cifs_copy 拷一个小文件，blake3 校验。
5. 验证 STATUS_OBJECT_NAME_COLLISION 当成功 (mkdir 已存在目录)。

## 成功判据

- 所有矩阵单元退出码 = 0
- copy 后 blake3 一致
- mkdir 已存在目录无报错

## 失败如何排查

- STATUS_ACCESS_DENIED on write → `.claude/docs/storage-cifs.md` "CreateDisposition" 段 (commit 4051)
- 协商失败 → 试 smb2_only=false (老设备) 或 smb2_only=true (新 NAS 不接受 SMB1 探测)
- 匿名失败 → 检查 anon=true + 空密码，看服务器 share 是否真允许匿名
- 句柄泄漏 (长跑后 hang) → `close_resource` helper 检查
