# CIFS / SMB Backend

## 底层依赖与边界

- Architecture-ready backend 使用 smb-rs domain facade：`Client → Session → Share → File / Directory`。
- data-mover 不得重新依赖 smb-rs 的 connection、runtime、wire create/query/set 类型或协议 handle。
- `smb::protocol` 只允许用于 lossless ACL codec 等明确的协议值边界，普通 I/O 不使用。
- `src/cifs.rs` 是待 #150 删除的历史公共路径；新功能只进入
  `src/storage/backends/cifs/`。迁移期间的双版本依赖不是兼容承诺。
- 经项目授权，新 domain facade 固定到 JayTsu-sh/smb-rs `feat/metadata-timestamps` 已验证提交
  `18ed91d65c3c5bac22fde1f552d46e0771f11fac`；历史路径固定旧提交
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

- 与 NFS 共用 `Checkpointed` / `AtomicReplace` 策略，CIFS 不支持 `Direct`。
- stage 位于最终文件的父目录，采用共享的 `.data-mover-<target-hash>-<uuid>.stage` 命名。
- `Checkpointed` 默认间隔 64 MiB；文件大于间隔且源端多块时延迟建立恢复记录。
  每个检查点等待所有已发出写入完成，FLUSH 数据，写入并 FLUSH checkpoint 临时文件，
  原子替换 `.stage.checkpoint`，再注册恢复信息。结束时 FLUSH 数据。
- `AtomicReplace` 不创建 checkpoint，也不主动 FLUSH；关闭 stage 后原子 rename 发布。
- recovery identity 是 opaque envelope；恢复时先把 stage 原子 rename 到 claim-token 派生路径，
  验证 checkpoint 的 binding、路径、校验和，只使用记录的连续前缀，不用 EOF 推断进度。
- checkpoint 名称不随 claim 改变；发布/丢弃时仅清理自己创建的 checkpoint。
- publish 使用 smb-rs `File::rename_replace`。响应丢失时沿用既有 publication reconciliation。
- SMB FLUSH 不等于 POSIX 目录 fsync；不额外宣称目录元数据持久化保证。

### Source read 与 QoS

- 每条 source stream 只持有一个 File handle，默认最多 8 路 positioned reads，按 offset 有序输出。
- CIFS 工厂解析已有 inflight 环境变量：`DATA_MOVER_CIFS_READ_INFLIGHT` /
  `DATA_MOVER_CIFS_WRITE_INFLIGHT`，其次全局方向配置，最后 `DATA_MOVER_INFLIGHT`。
  支持 1..=24，默认读写各 8；读还受 request.read_inflight 和 runtime 三项预算限制，写独立限流。
- 每次读分配 payload 前取得 runtime 的 chunk、byte、operation 准入；取消/丢弃流会关闭句柄。
- 每次请求不超过 smb-rs 协商的 `maximum_read_chunk`，最后一块精确收缩；短读是 corruption。
- source QoS 在真实 READ 前准入并只记录源端带宽/IOPS，目标 WRITE 不计入。
- describe 后到 open 之间必须重新构造 source identity；变化时在首个 READ 前 fast-fail。

### Metadata observation

- timestamps 来自 inline metadata，精度为 SMB FILETIME 的 100ns。
- 普通 `transfer()` 自动复制 mtime（Checkpointed / AtomicReplace 均支持），不自动复制
  atime、creation time 或 ChangeTime。跨协议纳秒时间向下对齐到 100ns，报告精度损失。
- 元数据观察通过同次查询的 size/written/changed 绑定已描述的 source identity；源版本改变则拒绝。
- 先完成数据写入并关闭写句柄、执行可选读回校验，再在 stage 上应用 mtime，最后 rename。
  Checkpointed 在元数据应用后再次 FLUSH；AtomicReplace 不增加 FLUSH。
- 文件和目录可显式调用 `Metadata::apply(Timestamps)`；目录不创建 stage、不自动递归修正时间。
  底层属性句柄拒绝最终 reparse point，避免误改软链接所指向对象；不宣称支持软链接元数据复制。
- 设置时间只经 smb-rs 公共 `open_metadata` / `set_metadata` API，未指定字段保持不变。
  数字 uid/gid/mode 不伪装成 Windows SID；自动跨协议复制会报告所有权未保留。
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
- `tests/cifs_policy_contract.rs` — 显式配置 CIFS_REAL_* 环境变量后运行的真实双 LIF 策略测试。
- skill：`.claude/skills/e2e-cifs/` (需要 `.env` 填测试服务器)。

## 改 CIFS 时

1. 读本 doc + 本 backend 当前的 `src/cifs.rs`。
2. 调 `backend-specialist` agent 传 `cifs`。
3. 改完跑 `make e2e-cifs` (需测试服务器)，否则至少 `make clippy && make test`。
4. 如果改的是公开操作 → 走 [storage-enum-dispatch.md](storage-enum-dispatch.md) 五处同步。

## 签名策略

`CifsBackendConfig.signing_policy` 使用公开的 `CifsSigningPolicy`：

- `WhenRequired`（枚举默认值）：对端允许时省略普通消息签名及未签名响应的验签。
- `Required`：要求签名或加密，保留强制完整性保护的选择。

策略通过 smb-rs 公共 Client 构造入口传入，由依赖协商和执行，backend 不访问 wire/runtime。
服务端要求签名时始终遵守；SMB 3.1.1 TREE_CONNECT、认证/绑定及加密完整性保护保留。
无加密且省略签名时，普通流量不具备 SMB 消息完整性保护。
基准例子提供 `--signing required|when-required`，默认使用协商策略。
历史 `StorageEnum` 路径保持原实现；此配置只作用于新的 backend 工厂。
