# CIFS / SMB Backend

## 底层依赖

- **crate**：`smb` (0.11，crates.io)。**不是 patched git**。
- 升级 smb crate 是真实风险 — API 不稳定，绑定 0.11。
- 协议解析用 `binrw` 0.15 (FileTime / 目录条目 SerializeBounded)。

## URL 形式

```
smb://[user[:password]@]host[:port]/share[/sub/path][?param=value&...]
```

示例：

```
smb://admin:secret@nas01/shared
smb://admin:secret@nas01:445/shared/data
smb://admin:secret@nas01/shared?smb2_only=false
smb://guest:@nas01/public                 # 匿名 (空密码)
```

路径中的反斜杠 `\` 必须 percent-encode 为 `%5C`。

## URL 参数

| 参数 | 类型 | 默认 | 含义 | 来源 |
|---|---|---|---|---|
| `smb2_only` | bool | `true` | `true`：直接 SMB2 NegotiateRequest，跳过 SMB1 多协议探测帧 (快)。`false`：先 SMB1 探测再升级到 SMB2/3 (兼容老设备 / 防火墙)。 | commit `af0e017` |
| `anon` | bool | (推断) | `true`：匿名访问 (空密码 + 无签名)。配合空 password 或 `guest:`。 | commit `9b332aa` |
| `file_id` | (内部) | 自动 | 128-bit `FileIdExtdDirectory`，NTFS inode 编码为 `fh3` join key。 | commit `b1b9db1` |

## 关键代码点

### 资源句柄管理

**所有 SMB 资源句柄走 `close_resource` helper**，不要裸 `.close()`。

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

### CreateDisposition

- **写文件用 `CreateDisposition::OverwriteIf`** (commit `4051`)。
- 早期用 Create + 追加，触发 Samba 服务器的 `STATUS_ACCESS_DENIED`。
- OverwriteIf = 不存在则建，存在则截断。最稳。

### Rename

- **必须用 share-relative 路径**，不是 UNC 全路径 (commit `4052`)。
- `FileRenameInformation` 字段填 `\sub\path\target.txt` (相对 share 根)，不是 `\\server\share\sub\path\target.txt`。

### Mkdir

- `mkdir_or_open` helper：`STATUS_OBJECT_NAME_COLLISION` 当作成功 (节省 1 RT 每个已存在目录)。
- 详见 commit `4061`。

### File ID

- 用 `FileIdExtdDirectory` info class (128-bit)，不是旧的 `FileIdBothDirectory` (64-bit)。
- 通过 `info-class probe` 在连接时探测服务器是否支持 (commit `b1b9db1`)。
- 探测结果缓存在 `CifsStorage::file_id_class` (`OnceCell` + `Mutex<()>` 双层，热路径 lock-free)。

### FileTime ↔ Unix nanos

- SMB FileTime = 100ns ticks since 1601-01-01 UTC。
- 转换在 `time_util.rs`，不要散写。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| Samba `STATUS_ACCESS_DENIED` on write | 用 `CreateDisposition::OverwriteIf` |
| Rename 用 UNC 路径失败 | 改 share-relative |
| 老 NAS 不接受直接 SMB2 协商 | 用户加 `?smb2_only=false` |
| 匿名 share 不能签名 | 加 `?anon=true` 或空密码 |
| 长 session 句柄耗尽 | 检查所有 close 路径走 `close_resource` |
| `FileIdBothDirectory` 不可用 | 已切到 `FileIdExtdDirectory` |
| `mkdir` 已存在报错 | `mkdir_or_open` 把 `OBJECT_NAME_COLLISION` 当成功 |

## 测试

- `examples/cifs_copy.rs` — clap CLI，src + dst 两个 SMB URL。
- `examples/cifs_walkdir.rs` — 遍历单 share。
- 无 integration test 在 `tests/` (网络后端)。
- skill：`.claude/skills/e2e-cifs/` (需要 `.env` 填测试服务器)。

## 改 CIFS 时

1. 读本 doc + 本 backend 当前的 `src/cifs.rs`。
2. 调 `backend-specialist` agent 传 `cifs`。
3. 改完跑 `make e2e-cifs` (需测试服务器)，否则至少 `make clippy && make test`。
4. 如果改的是公开操作 → 走 [storage-enum-dispatch.md](storage-enum-dispatch.md) 五处同步。
