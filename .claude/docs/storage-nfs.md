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

**role 失败的诊断带服务端状态**（`storage/backends/nfs/protocol.rs::classify_error`）：经过
`classify_error` 的失败，服务端回了 NFSv3/v4 状态时，`NfsProtocolFailure.status` 记下 RFC 名字和含义，
诊断是 `NFS role failed: NFS4ERR_BADOWNER (bad owner)` / `NFS session failed: NFS4ERR_BADSESSION (...)`；
nfs-rs 包在 `OperationOutcome` 里的状态也会取出来。传输 / 客户端侧失败没有状态，诊断仍是
`NFS role failed`。分类（class / transience）不受影响。
元数据角色的 SETATTR（mode / owner / 时间）经 `NFSStorage::setattr_retrying` 保留服务端对 SETATTR 的
回答，也走 `classify_error`（分类见 [error-taxonomy.md](error-taxonomy.md)）。
**缺口**：仍走 legacy `StorageError` 再 `classify_role_error` 的路径（SETATTR 之前的 LOOKUP / re-lookup、
xattr、namespace、staged 的多数操作）在转成字符串时丢了 nfs-rs 错误，没有状态。

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

## 传输 artifact 与列举 (ADR-0006 C3b)

- legacy `walkdir` / `walkdir_2` 与 role namespace `List` 都隐藏 `.data-mover-*`（相对根的任一段，先于过滤
  与 GETATTR）；role namespace 拒绝寻址它们。
- role `List` 自 P7 C7 起**逐页流式**（`storage/backends/nfs/listing.rs` 的生产者 + `src/nfs/role_listing.rs` 的
  READDIRPLUS 读取）：nfs-rs 的 `readdirplus` 流逐条取，按 `page_hint` 攒页交出，一次最多多读一个回复；
  消费方 drop 或取消 → 下一条之前停，**不放弃在途的 READDIRPLUS**（NFSv4.1 被取消的请求会把 session slot 围起来）。
  - **下钻提示**：每个子目录带一份**拷贝**出来的文件句柄（`DescentHint`，从不引用回复缓冲），遍历下钻时带回来，
    子目录直接用它发 READDIRPLUS，不再每目录 LOOKUP。没有提示（遍历根、`ndx_walk` / `delete_tree` 这类不带提示的
    调用方）或提示失效（`STALE` / `BADHANDLE` / `FHEXPIRED`）才按路径 `lookup_fh`（它照旧读写 `GLOBAL_CACHE`）。
  - **列举不再写 `GLOBAL_CACHE`**：C7 之前每列一个目录就把所有子目录句柄写进去，m1-source 整树上缓存常驻约
    13.5 万项 / 92 MB、另有过期碎片约 99 MB（P7 §12.7 内存根因）。变更路径（`create_dir_all`、`lookup_fh`、
    rmdir / rename 的失效）照旧用它。
  - **重试**：首页交出之前可从目录开头重读（不会重复交出）—— 句柄失效 → 解析路径（提示失效时一次，之后忘掉缓存再解析
    至多 3 次）；`DELAY` / `JUKEBOX` / `GRACE`（`GRACE` C7 起分类为 `Throttled`）→ 列举的节流预算内退避；nfs-rs 认证可安全重发的传输失败 → 250 ms / 1 s / 2 s。
    错误直接经 `classify_error`，保留服务端状态名。
  - **首页之后按 cookie 续读（P7 C7c，nfs-rs 0.8.5 `Mount::readdirplus_page`）**：读者一页一页地从列举持有的
    `DirectoryCookie`（cookie + verifier）读，整个回复取完且目录未完才把 `next` 报给列举（`reached`）。首页之后的失败按上面
    同样的分类 / 预算重试，从最后一个完整取完的回复之后续读；句柄解析后变了则从目录开头读（cookie 在别的句柄上无意义）。
    - 去重：列举记住已取子项 `(fileid, 名字)` 的带密钥 64 位摘要（`listing_resume.rs`），读者看每一项之前先问
      `first_sight`（在 `.snapshot` 的 LOOKUP 之前），已取的跳过；只有 push / hide 之后才算取过（看到一半读取失败的那项续读后
      还会再看）。不用名字本身：nfs-rs 把非 UTF-8 名字有损解码，两个不同名字可能成同一串。上限 2^18 项（每列举约 4.5 MiB）；
      超过就不再记，首页后的失败照 C7 结局（`Interrupted`）。
    - cookie 循环：同一次读取里 `next` 重复 → `NfsReadEnd::Repeated` → 该列举 `Protocol` / Permanent（先交出已读到的）。
      只在一次读取内判：不认 cookie、续读时从头重发整个目录的服务器（去重吸收）能读完。nfs-rs 自己拒绝“不前进的页”（`Xdr`，
      `Protocol` / Unknown）。
    - cookie 被拒（`is_bad_directory_cookie`：`NFS3ERR_BAD_COOKIE` / `NFS4ERR_BAD_COOKIE` / `NFS4ERR_NOT_SAME`，
      `NfsRestart::FromStart`），或续读的 cookie 被答以空的目录末尾（ONTAP 与 DXN-2 答不认识的 cookie 就是这样；目录真在
      回复边界结束时也会这样，代价是多读一遍）→ 从目录开头读、跳过已取的，至多 3 次。
    - 真机事实（nfs-rs 0.8.5 CHANGELOG）：ONTAP（v3 / v4.0 / v4.1）拒绝乱写的 verifier（`BAD_COOKIE`）；DXN-2（v3 / v4.0）
      接受乱写的 cookie 与 verifier —— 目录在两次读取之间变了时，续读可能静默漏掉或重复（重复由去重吸收；漏掉的属于
      P7 Q12“变化中的目录”）。
    - 每取到新子项，重试预算续满（节流预算只重置加倍，总量仍是该列举的）。重试用尽的结局与之前首页后相同：已读到的先交出，
      遍历记 `Interrupted`；可安全重发的传输失败只让该列举失败。每次首页后的续读打一条 `info!`（相对路径、原因、已交出页数）。
    - 真机（C7c）：ONTAP v3 / v4.0 / v4.1、DXN-2 v3 / v4.0 上 5 万文件目录与 base 逐行相同；`ss -K` 掐断连接时只有 v4.1 把
      `Retry` 交给列举（续读），v3 / v4.0 由 nfs-rs 自己重连重发；注入式故障（首页后截断回复再报 `ConnectionReset`）下五种都
      读完、0 重复。见 P7 设计 §14 C7c。
- role `List`（`drain_readdirplus` → `role_listed_entry` / `role_observation`）里某项描述不出来 ——
  READDIRPLUS 没带属性（v3 `post_op_attr` 可省；v4 的 fattr4 解不出时 nfs-rs 也给 None）、没带文件句柄（v3
  `post_op_fh3` FALSE、v4 属性里没有 `FATTR4_FILEHANDLE`）——
  报成该子项自己的 `Protocol` / Unknown 条目失败，兄弟照常，列举 `Partial`（之前整个目录失败；P7 评审现存问题 2）。
  只有 LOOKUP 目录、READDIRPLUS 流本身出错才让整个列举失败。唯一的例外是下一条的 `.snapshot`。
- **ONTAP 的 `.snapshot`（P7 C7b，用户 2026-09-26 定，`role_listing.rs` 的 `snapshot_directory`）**：ONTAP 9.19.1 的
  NFSv3 READDIRPLUS 在卷根列出 `.snapshot`，**带句柄、不带属性**；NFSv4 READDIR 不列它。真机实测：它与卷根 **fsid 相同**
  （只有其中每个快照的 fsid 不同，v3 `0x<n>80231f3a`），从任何目录 LOOKUP 都是同一个对象（fileid = 卷根 + 3，mode 0777，
  nlink = 2 + 快照数），名字被保留（任何目录里 MKDIR `.snapshot` 回 `EXIST`）；快照里的子项 v3 READDIRPLUS 也不带属性。
  规则（**只限 NFSv3**，只看名字恰为 `.snapshot` 且 `post_op_attr` 缺失的项）：
  - 在被列目录里 LOOKUP `.snapshot` 一次（回复没带属性再对返回的句柄 GETATTR）；
  - 是目录 → **隐藏**：不列出、不算失败、不下钻；计入页的 `internal_hidden`（与 `.data-mover-*` 同一计数，不在 `listed`
    里，`listed == emitted + filtered + failed + vanished` 照旧成立），并打一条 `info!`（只含导出内的相对路径）；
  - 不是目录 → 用查到的属性与句柄（拷贝）当普通子项；
  - 查失败 → 该子项失败，分类与状态取自 LOOKUP / GETATTR（`NOENT` 即 `NotFound` = 消失）；会话（`Connectivity`）、
    服务端忙（`Throttled`）、LOOKUP 用的目录句柄失效（`STALE` / `BADHANDLE`）则按 READDIRPLUS 失败结束
    这次读取（首页前从头重读 / 退避 / 重发，首页后 `Interrupted`）。
  - 带属性的 `.snapshot`（例如 DXN 上真建的目录）、其他名字（含 CIFS 拼法 `~snapshot`，NFS 上两种版本都 `NOENT`）、
    缺句柄但有属性的项、NFSv4 一律照旧。
  - 结果：ONTAP 同一导出 v3 与 v4.x 列出同样的条目、零失败；每次列卷根多一次 LOOKUP（和一条 `info!`）。
  - **残余风险**：某台服务器对一个**真实的**、名叫 `.snapshot` 的目录也不给属性（NFSv3 允许任何项省略；例如 Linux knfsd
    对拼不出句柄的挂载点），它会被隐藏、不迁移 —— 只留 `info!` 与 `internal_hidden` 计数。ONTAP 上不会发生（名字保留），
    DXN 给真目录带属性。
  - legacy `walkdir` / `walkdir_2` **不变**（P7 收尾删除）：它用 GETATTR 补属性，把 `.snapshot` 当普通目录下钻（有快照时
    会列出每个快照的整棵树）。
  - `delete_tree`：卷根列举不再带一个 `.snapshot` 子项失败，所以对**非 backend 根**的 junction 卷根 `delete_root` 会发出
    RMDIR，由 ONTAP 拒绝（之前因子项失败不发）。RMDIR 不递归，不会删掉什么。
- role `List` 的描述符带 `inline_timestamps` + `inline_ownership`（P7 C0）：同一份 READDIRPLUS 属性，owner / group
  经同一个 `owner_id` 映射（映射不了 → `Unmapped`，与 GETATTR 路径一样报 `Unsupported`，`Required` 时同样拒绝）。
  v4 READDIR 与 GETATTR 请求同一个 `standard_getattr_bitmap`，v3 READDIRPLUS 带完整 fattr3，所以
  `NfsMetadataAdapter::observe_listed` 对只要 ownership / timestamps（ACL / xattr 为 Omit / InlineOnly 或 mount
  不支持）的计划直接用列举属性作答，遍历不再每条目一次 GETATTR；两条路径共用 `observations()`，
  `metadata_tests.rs::a_listed_child_is_observed_exactly_as_a_getattr_would` 逐计划比对。
- legacy `delete_dir_all(_with_progress)` 内部走 `walk_listing(.., hide_artifacts=false)`：不这样的话
  含 artifact 的目录 `rmdir` 会失败，而事件只记日志、`error: None`，静默删不干净。role `Delete(dir)`
  走的就是它，所以 NFS 的 `delete_tree` 不需要额外清扫。

## Namespace 角色的建目录（`src/nfs/role_namespace.rs`，2026-09-27）

- `Namespace::create_directory` **只建一层**（与 CIFS / HDFS 一致；trait 契约允许已存在的目录答成功，挡路的非目录被
  删掉重建则是 NFS 独有的迁移语义，见下）；递归由 backend 无关的
  `storage::create_directory_all` 逐层驱动，所以它"被拒但已是目录的层算已存在"的规则也管到 NFS 的中间层。
  此前它转发给 legacy 递归 `NFSStorage::create_dir_all`，中间层在一次调用里建完，规则够不到；legacy 调用方
  （`StorageEnum` / 写路径建父目录）仍用 legacy 递归版，**行为不变**。
- 一层：句柄缓存里已有该目录 → 成功、零 RPC（与 legacy 最深缓存层一样；外部删掉了缓存过的目录时，下一层 MKDIR
  会 `STALE` → 忘掉缓存再解析父目录 → `NotFound` / **Transient**（报在下一层；缓存已清，调用方重试会重建；legacy
  是同一次调用里整条路径从根重来）。一开始就解析不到的父目录照旧 `NotFound` / Permanent）。
  否则 LOOKUP 父目录（经缓存）后 MKDIR：
  - `EXIST` → LOOKUP 该名字（无属性则 GETATTR）：目录 → 成功；**非目录（文件 / 符号链接）→ 删掉再 MKDIR**
    —— legacy 的 rsync 式迁移语义，保留，`warn!` 记录；REMOVE 的 `NOENT` 当作兄弟任务已删。
  - `STALE` / `BADHANDLE` / `NOENT`（并发改名 / 删除）→ 刷新根句柄、忘掉路径缓存、重试。
  - 其余直接 `classify_error`（保留状态名）：`ACCES` / `PERM` / `ROFS` → 条目 `PermissionDenied` / Permanent。
  - 至多 `MAX_STALE_RETRIES`（3）次重试，用尽报最后的原因（一直被非目录挡住 → `Conflict` / Unknown，最后那个
    非目录不删 —— 没有重试可用来建目录时不做破坏性的一步）。
  - 残余：MKDIR 前解析父目录的 LOOKUP 仍经 legacy `lookup_fh` + `classify_role_error`（`ACCES` 等是 `Protocol` /
    Unknown，断连不升会话）；v3 MKDIR 回复可以不带句柄（ONTAP / DXN 都带），nfs-rs 此时报 `Rpc` → 会话失败。
- 真机（`tests/nfs_mkdir_contract.rs`，e2e-nfs 步骤 4b）：uid 4242 在 `dm-mkdir-*/ro`（0555）下的 `ro/w`（0777）里
  `create_directory_all("…/ro/w/new")` 成功，`"…/ro/new"` → `PermissionDenied` / Permanent 报在 `…/ro/new`。
  已存在目录、父目录不可写时：**DXN-2 v3 / v4.0 回 `ACCES`**（先查权限；旧代码在这里整条失败，报在第一层、
  `Protocol` / Unknown），**ONTAP v3 / v4.0 / v4.1 回 `EXIST`**（先查重名；旧代码能过已存在的层，但新层被拒是
  `Protocol` / Unknown）。ONTAP 的 `.snapshot`（任何目录都能到，只读）：MKDIR `.snapshot` 本身回 `EXIST`，
  其下新建回 `NFS3ERR_ROFS` / `NFS4ERR_ROFS` → `PermissionDenied`。没有真正的只读导出（不新建导出）。

## 目的端恢复（ADR-0006 C10，`src/storage/backends/nfs/at_destination.rs`）

续传状态全在最终文件旁，运行 data-mover 的机器上什么都不存（C21 起所有后端都如此，本地恢复存储已删除）。
- 名字只由最终文件名决定：`.data-mover-<d>.stage` / `.pointer`（+ 固定 `.pointer.tmp`）。**没有 claim 文件**：
  NFS 没有所有主机都遵守的锁。
- 指针 = `DMDPTR01{binding, 传输标识, 持久前缀, "DMNSTG01" ‖ 16 字节 nonce}`，只在句柄 `checkpoint()`（COMMIT /
  FILE_SYNC）证明前缀之后写；写法：删临时名 → 独占创建 → `write_at`（nfs-rs 返回前已 COMMIT）→ close → rename 覆盖。
  rename 回复丢失 → 读回比较字节。
- **nonce 围栏**：每次 prepare 取新 nonce；续传先用自己的 nonce 重写指针（接管）再截断 stage；之后每次重写指针、
  发布 rename 前、清理前都读回指针，nonce 不同（或写过后指针没了）→ `Conflict`（永久），不碰那些名字。围栏是检查
  不是锁；非 recoverable 的 stage 在第一个检查点之前没有指针可比 —— 都靠调用方契约兜底，读回校验抓混写的字节。
- 旧路径已在 C10d 删除：随机名 stage、`<stage>.checkpoint`（`DMNCKP01`）、`DMNRCV03` 恢复身份与 `.claim-` 改名
  恢复；store 时代的 `StagedDestination::prepare` / `recovery_identity` / `recover` / `prepare_ephemeral` /
  `handoff_recovery` 已在 C21 从 trait 删除。stage 只能经 `prepare_at_destination` 准备，`validate`
  只接受 at_destination 的确定名 stage。单测在 `staged_tests.rs` 用 `prepare_stage` / `prepare_ephemeral_stage` /
  `resume_stage` 辅助函数（Restart / 非 recoverable / Discover）。
- 续传遇 `PermissionDenied`（元数据已施加）→ 经元数据角色设回 0600 再打开。
- `rmdir` / `rename` 成功后 `GLOBAL_CACHE` 会忘掉该路径及其下所有目录句柄（C10a），否则改名后重建的目录会被
  解析到旧目录里。
- 真机：`examples/nfs3_contract.rs --dialect nfs41`（`validate_recovery` 覆盖续传与接管），`resume_matrix.sh`
  `DEST=nfs://…`。ONTAP NFSv4.1 已验（2026-09-25）；NFSv3 真机待定（WSL 下 ONTAP v3 MOUNT 被拒，m1-target 的 v3
  导出路径未知）。

## 测试

- `examples/nfs_walkdir.rs` — 遍历 export。
- `examples/nfs_export.rs` — 查询 export 元信息。
- `examples/nfs_opt_dir.rs` — 创建优化目录结构。
- `tests/nfs_mkdir_contract.rs`（`#[ignore]`，`NFS_REAL_URL`）— 不可写的已存在层上 `create_directory_all`，
  可选 `NFS_REAL_RO_PATH` 只读文件系统。
- skill：`.claude/skills/e2e-nfs/` (需要 `.env`)。

## 改 NFS 时

1. 读本 doc + [error-taxonomy.md](error-taxonomy.md) 的 NFS 段。
2. 改 retry 决策必须更新两个 doc 的映射表。
3. 调 `backend-specialist` agent 传 `nfs`。
4. 改完跑 `make e2e-nfs` (需测试服务器)。
