# Architecture

## 数据流总览

```
URL string
    │
    ▼
detect_storage_type()   ── lib.rs ──  按 scheme 判别 4 backend
    │
    ▼
create_storage(url, block_size, ensure_dir)
    │
    ├──→ Local(LocalStorage)
    ├──→ NFS(NFSStorage)
    ├──→ S3(S3Storage)
    └──→ HDFS(HDFSStorage)

(CIFS 不在 StorageEnum 里：只有 role-based `storage::connect_backend` 路径)
    │
    ▼
StorageEnum 提供 30+ 操作 (walkdir, walkdir_2, copy_file, copy_file_with_cancel,
    delete_dir_all, get_metadata, mkdir, write_file, read_file, rename, ...)
    │
    ▼  每个操作内部 match self { ... } 分派到具体 backend
    │
    ▼
Backend 实现 (nfs.rs / s3.rs / local.rs / hdfs.rs)
    │
    ▼
底层 crate (nfs-rs / aws-sdk-s3 / std::fs+rayon / hdfs-native)
```

### role-based 侧的中立组合层

`storage::connect_backend` 出来的 `Storage` 借出四个角色 (`ReadSource` /
`StagedDestination` / `Namespace` / `Metadata`)。凡是"树形状"的操作都写成拿 `&Storage`、
借角色的中立 helper，不再逐 backend 实现：

```
Storage (roles)
    │
    ├──→ traversal::StorageTraversalSource   Namespace::List + Metadata → TraversalItem 流 (深度优先块顺序 + 列举预读)
    ├──→ ndx_walk (crate 根)                 Namespace::List → dir_tree::run_dfs_driver → NdxEvent
    ├──→ storage::delete_tree                Namespace::{List,Delete} → DeleteTreeItem 流
    ├──→ storage::create_directory_all       Namespace::{CreateDirectory,Stat} 逐层创建
    └──→ integrity::compare                  ReadSource + Metadata
```

`ndx_walk` 是 legacy `walkdir_2` 的中立替身：DFS 栈、预读窗口、NDX / gap 编号仍归
`src/dir_tree.rs` 的 `run_dfs_driver`(那部分本来就后端无关)，这里只负责列举取数、按名排序、
以及把 `SourceDescriptor` 反拼成 `NdxEvent` 载荷要的 `EntryEnum::NAS`。

`StorageTraversalSource` 的调度 (`src/traversal/storage.rs` + `storage/{cursor,observe}.rs`)：
准入游标按"块"走深度优先 —— 一个目录的子项按列举顺序连续输出，再依次下钻各子目录；序号只由
游标分配，重排缓冲按序号输出，所以列举/观察乱序完成不影响顺序。列举预读额度
`min(max_inflight_operations, 64)` 只计**在途**，在途 + 已列出待消费合计另限 2 倍额度 (否则早期预读的
浅层兄弟会占住额度，饿死深层)；与观察窗口分开；游标马上需要的列举不计额度 (防饿死)。
deferred filter 已决定下钻的 `Pending` 槽位也会预读。列举结果**到达时**就对每个子项做
filter 决定 (deferred 的除外)，所以已预读、游标还没走到的目录，其子目录也能继续预读；候选按
输出顺序深度优先展开 (`cursor.rs` 的 `Search`)。
取消或结束时在途的列举和观察都 detach 而非 abort (CIFS 句柄)。

**目录完成事件**。游标在两处排入 marker 槽位：块被完全准入时排 `DirectoryListed`，帧弹出前排
`SubtreeComplete`。于是一个被列举的目录 D 的输出是：D 的块 → `DirectoryListed(D)` → 各子目录
递归同样的序列 → `SubtreeComplete(D)`；跑完的遍历以根的 `SubtreeComplete` 收尾。**只有真被
列举过的目录**才有这两项 —— `max_depth` 边界目录发了 Entry 却没列举，两项都没有；filter 隐藏
但仍下钻的目录两项都有、却没有自己的 Entry。取消时 `DirectoryListed` 可以没有配对的
`SubtreeComplete`，只有 `TraversalOutcome::Completed` 保证配对。

marker **占准入窗口**的名额，不绕过它，所以两个 marker 都不会让 `admitted()` 越界。
(唯一的例外是既有的 `queue_failure`：`take_top_listing` 为列举自带的 N 个不可描述子项、以及
列举失败本身排队失败项时不查窗口，`admitted()` 可短暂到 `max_inflight + N`。它们同步落盘、
下一次 flush 就排空，且 N 受该次列举已在内存里的结果限制。这是 P2 之前就有的行为，P3 没有
扩大它。) 重入安全靠位置：`DirectoryListed`
由 `Frame.closed` 一次性标志守护 (`next_after_block` 每槽位调一次)；`SubtreeComplete` 的窗口
检查必须在 `next_after_block` 返回 `Next::Pop` 之后、`stack.pop()` 之前 —— 放到调用之前会让
`Descend` 分支已消费的槽位随提前返回一起丢掉，表现为整棵子目录静默消失。

判定与汇总在**输出侧** (`storage/output.rs`) 而不是游标：槽位离开重排缓冲时比它早的槽位全部
已经过去，所以在 `DirectoryListed` 处整个块 (含 deferred 决定) 已落定，在 `SubtreeComplete`
处其下所有块也已落定，计数是精确的。`DirectoryListing` 四个状态互斥，同时成立取最强者
`Failed > Partial > Filtered > Complete`。账目分两条路进来：立即决定隐藏的子项**根本不分配
槽位** (给它分配会让一个排除百万文件的 filter 反过来被准入窗口节流)，记在 `BlockFacts`；
deferred 决定随 `Settled::Child.descent` 走。两条路的 `max_depth` 与 filter 判定顺序统一为
先判深度，否则同一个目录会因 filter 是否 deferred 而一会儿报 Pruned 一会儿报 Truncated。

**覆盖面**：

1. S3 **不出借 `Namespace` 角色**，上面四个 helper 在 S3 上于 preflight 处返回
   `CapabilityUnavailable`。其余四个 backend (Local / NFS / CIFS / HDFS) 都出借。
   Local 的 namespace 在 `src/storage/backends/local/namespace.rs`，与观察角色共用同一个
   `cap_std::Dir` 沙箱；路径逐段用 `open_dir_nofollow` 解析，**任何一段**是 symlink 都拒绝，
   最后一段在父目录句柄里操作 (描述/删除的是链接本身)。
2. `ndx_walk` 还要求列举自带修改时间 (`SourceDescriptor::inline_timestamps`)，缺了就把
   该目录报成错误而不是产出 epoch —— 填 epoch 会让增量同步认为所有条目都变了。目前
   CIFS (`QUERY_DIRECTORY`)、NFS (`readdirplus`)、HDFS (listing) 和 Local (unix 逐子项
   `fstatat`，Windows 直接取目录读回的属性) 都挂了，四个都可用。
3. 列举里个别子项无法描述 (例如文件名拼不成 `StoragePath`) 时，backend 返回
   `NamespaceResult::Listing { entries, failures }`，三个消费方用 `into_listing()` 统一
   处理：失败逐项上报，兄弟条目照常。

**关键点**：没有 `Storage` trait，没有 `dyn Storage`，没有 vtable。这是有意的 — 4 个 backend 协议差异极大，trait 抽象会塞 30+ 默认方法和大量 `Self`-bound 限制，不如 enum + match 直接。

代价：**新增/修改一个 StorageEnum 操作 = 五处同步**。详见 [storage-enum-dispatch.md](storage-enum-dispatch.md)。

## Pipeline 与背压

`storage_enum.rs` 内置两个容量常数 (背压关键)：

```rust
const COPY_PIPELINE_CAPACITY: usize = 2;   // 拷贝流水线
const TAR_PIPELINE_CAPACITY: usize = 16;   // tar 打包流水线
```

为什么这两个值：
- **COPY=2** — 拷贝是 IO 密集，背压收紧避免远端 backend 把 buffer 撑爆。
- **TAR=16** — tar 打包是 CPU + IO 混合，需要更深 buffer 让上游 walk 不阻塞。

调这两个值前先想清楚：调大 → 内存占用上升；调小 → 远端慢时上游 walk 卡顿。

## 并发模型：work-stealing

所有 backend 的 walkdir 共享 `walk_scheduler.rs` 的 `WorkerContext<T>`：

- 每 worker 持有一个 LIFO 自栈 + 邻居 FIFO 引用。
- `pop_task()` — 先弹自己栈 (LIFO，保留局部性)，再 FIFO 窃邻居最旧任务 (减少冲突)。
- `push_task()` — 递增 `active_tasks` + `notify.notify_waiters()`。
- `is_done()` — `active_tasks == 0 && active_producers == 0`。

`AsyncReceiver<T>` 是 mpsc 接收端的薄 wrapper，单方法 `next() -> Option<T>`，给 walkdir/walkdir_2/delete 流用。

详见 [walk-scheduler.md](walk-scheduler.md)。

## 异步运行时

- **tokio (full)** — 任何异步代码默认 tokio。
- 不混用 async-std / smol。
- `tokio::spawn` 用于 worker 任务；`tokio::task::spawn_blocking` 用于 std::fs 等阻塞调用。

## 错误模型

- `StorageError` 24 变体 (thiserror)。详见 [error-taxonomy.md](error-taxonomy.md)。
- **`Cancelled` 不是错误**，是 CancellationToken 信号。上游可重入队，不算失败。
- Retry 分类 (commit `7eb3046` "split NFS retry taxonomy")：
  - `EACCES` / `EPERM` → `deny_list`：直接 `PermissionDenied`，不重试。
  - `EAGAIN` / `ECONNRESET` → `delay_backoff`：指数退避重试。
  - S3 404 → `FileNotFound`：不重试 (commit `7eb3046`)。

## 时间统一

`time_util.rs` 是所有时间转换的中心：

- CIFS `FileTime`：100ns ticks since 1601-01-01 UTC → Unix nanos。
- NFS `Time { secs, nsecs }` → Unix nanos。
- S3 `DateTime` (毫秒) → Unix nanos。
- Local `SystemTime` → Unix nanos。

**禁止散写时间转换**。新加 backend 必须扩 `time_util.rs`。

## 资源句柄

CIFS / NFS / S3 都有需要显式释放的句柄。**统一走 `close_resource` helper** (`storage/backends/cifs/protocol.rs` 已有)。

S99 教训：裸 `.close()` 在 error path 漏释放 → 资源泄漏。helper 用 RAII-style 包装确保不漏。

## ACL / xattr

- Unix：uid/gid/mode/ino 走 `local.rs` + `nfs.rs` 直接读。
- Windows：通过 `acl.rs` + windows crate (cfg)。
- NFS：`NfsEnrich` 结构在 lookup vs walkdir_2 行为有差异 (按站点配置)。

## 日志脱敏

`url_redact.rs` 提供 URL 日志脱敏函数 — 隐藏 access_key / secret_key / 密码。

**所有打印 URL 的日志必须先过 redact**。grep `info!.*url` / `error!.*url` 时复查。

## 模块边界 (谁可以依赖谁)

```
lib.rs                  ← 公开 API 出口
  │
  ├── storage_enum.rs   ← dispatch 层
  │     │
  │     ├── hdfs.rs ─── hdfs-native crate
  │     ├── nfs.rs  ─── nfs-rs crate + moka cache
  │     ├── s3.rs   ─── aws-sdk-s3 + hyper-rustls
  │     └── local.rs ── std::fs + rayon + acl.rs
  │
  ├── filter.rs         ← walkdir 流水线在 enumerate 后调
  ├── walk_scheduler.rs ← 4 backend walkdir 共享底座
  ├── async_receiver.rs ← mpsc wrapper
  ├── dir_tree.rs       ← walkdir_2 事件类型
  ├── tar_pack.rs       ← copy_dir 用
  ├── checksum.rs       ← copy_file 完整性校验
  ├── qos.rs            ← copy 流速率限制
  ├── time_util.rs      ← 所有时间转换中心
  ├── url_redact.rs     ← 所有 URL 日志中心
  ├── error.rs          ← 全库错误
  └── acl.rs            ← Unix/Win ACL 抽象 (local.rs 用)
```

**禁止反向依赖**：backend 模块不可 `use crate::storage_enum::StorageEnum`。
