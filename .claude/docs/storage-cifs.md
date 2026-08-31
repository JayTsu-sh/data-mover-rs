# CIFS / SMB Backend

## 底层依赖与边界

- Architecture-ready backend 使用 smb-rs domain facade：`Client → Session → Share → File / Directory`。
- data-mover 不得重新依赖 smb-rs 的 connection、runtime、wire create/query/set 类型或协议 handle。
- `smb::protocol` 只允许用于 lossless ACL codec 等明确的协议值边界，普通 I/O 不使用。
- `src/cifs.rs` 是待 #150 删除的历史公共路径；新功能只进入
  `src/storage/backends/cifs/`。迁移期间的双版本依赖不是兼容承诺。
- 经项目授权，新 domain facade 固定到 smb-rs `main` 已验证提交
  `44cad000cf596a473b20f890506c9522fa665fd0`；历史路径固定旧提交
  `c3ecf0007a5477cfd954d4e6dda65c7abd765e71`。两者都不得改成浮动 branch，
  旧提交及此迁移例外随 #150 删除。

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

所有 domain resource 在成功、失败和取消路径都必须显式 close。通用 `Resource` 走
`protocol::close_resource`；流式 File cursor 在 EOF 或终止边界关闭。

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

### Staged destination 与恢复

- 未发布文件位于 backend root 下的 `.data-mover-staging/`，FinalDestination 在 publish 前不变。
- 每个完整写流执行 `flush`；只有重新观察到的连续文件长度才是 durable prefix。
- recovery identity 是 opaque envelope；恢复时先把 stage 原子 rename 到 claim-token 派生路径，
  再重新观察 durable prefix，不能信任调用方进度。
- publish 使用 smb-rs `File::rename_replace`。响应丢失时必须重新观察 stage/final 并比较
  size + BLAKE3，不能盲目重试 rename。
- `VerifyOrSkip` 仅在既有 FinalDestination 内容等价时保留目标并删除 stage。

### Source read 与 QoS

- 每条 source stream 只持有一个 File handle，顺序发出 positioned reads。
- 每次请求不超过 smb-rs 协商的 `maximum_read_chunk`，最后一块精确收缩；短读是 corruption。
- source QoS 在真实 READ 前准入并只记录源端带宽/IOPS，目标 WRITE 不计入。
- describe 后到 open 之间必须重新构造 source identity；变化时在首个 READ 前 fast-fail。

### Metadata observation

- timestamps 来自 inline metadata。
- ACL 需要额外 storage call，因此 `Omit`/`InlineOnly` 不调用服务器；只有
  `BestEffort`/`Required` 才 query security descriptor。
- CIFS xattr、tags、numeric ownership 当前按 typed not-applicable/unsupported 体现，
  不通过 storage enum 做协议配对分支。

### 历史 CreateDisposition

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
