# NFS Backend

## 底层依赖

- **crate**：`nfs-rs` (0.2.0，crates.io)。
- 支持 NFSv3 和 NFSv4 (自动协商 / 升级)。
- 属性缓存用 `moka` (0.12.15)，减少冗余 GETATTR RPC。

## 读写块大小

挂载后分别计算读写上限：读侧取客户端/默认上限与 rsize 的最小值，写侧取客户端/默认上限与 wsize 的最小值，两者不相互限制。客户端/默认上限继续保持现有 1 MiB 封顶规则，零值视为未指定。
优化角色接口与 StorageEnum 写流水线均使用写侧上限；StorageEnum.block_size() 对 NFS 返回读侧的块大小提示。大于写上限的输入 Bytes 块仍零拷贝拆分并进入并发写队列。

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

- `create_nfs_storage(url, block_size, ensure_dir)` — 统一工厂。`ensure_dir = true` 时
  prefix 目录不存在（NOENT）自动创建；其他 lookup 错误（网络/权限）如实上抛。
  root 解析逻辑收敛在 `NFSStorage::attach_root`。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| EACCES 被当成可重试 | 已修，必须 deny_list |
| GETATTR 读到 stale | 改 attr 后调用 invalidate |
| mount 被踢 | 长 session 加 keepalive |
| uid/gid 默认 1000 不匹配服务器 | URL 参数显式指定 |
| v3 vs v4 差异 (例如 `setattr` 字段) | 走 `nfs-rs` 的协议无关接口 |

### NFSv4 owner / owner_group 是字符串，nfs-rs 会把认不出的名字变成 65534

NFSv3 的 uid/gid 是数字；NFSv4.0/4.1 线上是 UTF-8 字符串（`owner` / `owner_group`）。ONTAP 在 SVM
能把 id 映射成名字时发 `name@domain`，否则发数字串 —— FAS2750 实测：uid 0 → `root@localdomain`，
1000 / 4242 / 65533（SVM 无对应用户）→ `"1000"` 等，v4.0 与 v4.1 一致。nfs-rs 0.8.4
`parse_numeric_owner` 只认数字串、`N@domain` 与 `root`，其余名字**不报错地**给 65534。接了
LDAP/NIS 的 SVM 会让普通用户都走名字形式，拷过去就全是 nobody。

我们这边的处理（`src/nfs/owner.rs` `owner_id`，用在 role 的 stat 与 `NASEntry`）：按**协商到的版本**
判断 —— v3 的数值原样可信；v4 只有非空的数字串、`N@domain`、`root` / `root@…` 才信 nfs-rs 的数值，
其余（包括服务端没发这个 RECOMMENDED 属性时 nfs-rs 留下的空串 + 0）给 `None`。之后：
- 拷贝走 `observe_copy_bound` 的「只有 mode」路径：mode 照拷，owner/group 记为损失
  `OwnerAndGroupUnmapped`（与 HDFS 那种「源端本来就没有数字 owner」的 `OwnerAndGroupDropped` 分开），
  文件不失败（用户规则：做不到的跳过并记原因）。目的端文件的 owner/group 于是是**执行拷贝的那个身份**
  （以 root 跑就是 root:root 建出来的，只是不再 chown）。只有一半（仅 group 或仅 owner）映射不了时
  两个都丢 —— 模型里没有只改一半的 mutation，列为后续。
- 其余观测：`InlineOnly` / `BestEffort` 下 ownership 为 `MetadataObservation::Unsupported`，不再
  `unwrap_or_default()` 成 0（root）；`Required` 下整条观测以 `FailureClass::Unsupported` 失败（拷贝路径
  不走这里，它走上面的 `observe_copy_bound`）。
- **set-id 位跟着 owner 走**（`model::without_unowned_set_id`）：owner 不带过去时文件归执行拷贝的身份所有，
  以 root 跑就是 root，所以 uid 未知去掉 setuid，gid 未知且不是目录去掉 setgid（目录的 setgid 只是
  让子项继承组，不授予权限）。拷贝路径（只拷文件，也覆盖 HDFS 的 mode-only）、legacy `NASEntry`
  （所有 legacy 消费方都会应用这个 mode 且不 chown）与 tar 头三处用同一条规则。
- tar 打包（`src/tar_pack.rs` `header_ownership`）：缺的 uid/gid 记为 65534，并去掉 setuid/setgid 位
  —— 以前 `unwrap_or(0)` 会把它写成 root。
- legacy 路径收到 `None` 的行为有三种，均不会写成 0（mode 已按上一条去掉 set-id 位）：Local 与 NFS 的 `write_file` / `create_file`
  两个都不 chown；NFS 的 `set_entry_metadata` 与 `create_symlink` 分别传 uid、gid，只设已知的那个。
- **目的端的镜像问题**（未处理，列为后续）：nfs-rs `encode_setattr` 总是发数字串；ONTAP 关掉
  `-v4-numeric-ids` 或用 Kerberos 时会回 `NFS4ERR_BADOWNER`，我们现在把它归成 Protocol + Unknown，
  报错不够精确，且需要真机验证。

上游：nfs-rs 0.8.4 `src/nfs4/attrs.rs:654`（`parse_numeric_owner`）应暴露「映射不了」而不是伪造 nobody。

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
