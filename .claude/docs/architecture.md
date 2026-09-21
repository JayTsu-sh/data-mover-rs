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

## 遍历的三套处理逻辑

> **现状**：legacy work-stealing 与 admission cursor 并存，P6 删前者。
> **终局**：只剩 admission cursor，两种模式 (`Admission` / `NameBytes`)。
> 本节标了 **[计划]** 的内容属于 P7 (流式列举) / P8 (排序与槽位可溢写)，尚未实现。

### 一、legacy work-stealing (现状，P6 删)

任务 `T` = **一个待列举的目录**，不含任何条目：Local `(PathBuf, depth, need_submatch,
package_remaining)`，NFS `(String, Bytes, depth, bool)`。

worker 主循环 (`run_worker_loop`)：

```
loop {
    task = pop_task()
        ├─ 自栈 pop_back()        LIFO，保 cache 局部性
        └─ 空 → 依次锁 N-1 个邻居 pop_front()   FIFO，偷最旧 = 最浅 = 期望最大的子树
    if task:
        active_producers += 1;  process_dir(task);  active_producers -= 1; active_tasks -= 1
    else if active_tasks == 0 && active_producers == 0:
        break          // 两个都要查：任务空但 producer 还活着就可能再 push
    else:
        timeout(100μs, notify.notified())    // 丢唤醒的兜底，见下
}
```

每目录处理 (`local.rs` 的 `process_dir`)：

```
for entry in read_dir(dir):
    metadata = stat(entry)
    if filter 判定跳过: continue
    channel.send(EntryEnum::NAS(...))        // async_channel::bounded(1000) → 有背压
    if is_dir && depth < max_depth:
        push_task((full_path, depth+1, ...)) // 无界 VecDeque → 无背压
```

特征：条目边读边发**从不累积**；目录任务**无界累积**；顺序完全由调度决定、不可复现；
**目录 / 子树边界在概念上不存在** —— 任务一拆边界信息当场销毁，这正是消费者拿不到权威
"这个目录的子集已完整"信号的根因。

### 二、admission cursor —— `Admission` 模式

驱动循环 (`storage.rs` 的 `drive`)：

```
loop {
    if cancelled: return
    step = cursor.advance(...)        // 尽可能多地准入
    cursor.start_listings(...)        // 起"游标马上需要的" + 预取
    state.output.flush()              // 按序号严格释放到 mpsc
    match step {
        Done       && 无在途观察 → return
        WindowFull && 无在途观察 → 继续    // flush 刚腾空窗口，必然能前进
        _ → select! { 取消 | 一个列举完成 | 一个观察完成 }
    }
}
```

结束时 `tasks.detach_all(); listings.detach_all()` —— **不 abort**：CIFS 列举在两个 await
之间持有目录句柄，drop 掉 future 就泄漏。

`advance()` 单步：

```
loop {
    frame = stack.last() else return Done

    ① 还没拿到列举 → take_top_listing()
         到了   → 装上；列举级 failures 先按序发出
         失败   → facts.listing_failed = true，装一个空块   ← 失败与成功共用同一条收尾路径
         没到   → return WaitListing

    ② 块里还有子项 → 窗口满? WindowFull
                    admit(child)   // 分配序号、spawn 观察、记 slots、更新 facts

    ③ 块空且 !closed → 窗口满? WindowFull
                      queue_block_end()   // DirectoryListed(D) 在这里拿到序号
                      closed = true

    ④ 消费一个 slot (next_after_block)：
         Pop     → 窗口满? WindowFull      ← 检查必须在返回 Pop 之后、stack.pop() 之前
                   queue_subtree_end(); stack.pop()
         Descend → stack.push(Frame::new(work))
         Skip    → {}
         Wait    → return WaitObservation
}
```

**序号只在 cursor 里按这个顺序分配**；列举与观察可以任意次序完成，时间只决定*何时*能走下一步，
不决定*谁拿到哪个序号*。重排缓冲严格按序号释放 —— 这是确定性的全部来源。

预取 (`start_listings`)：栈顶需要的那个**一定起**，哪怕超预算 (否则远处的预取会饿死当前阻塞点)；
其余按输出顺序 (栈顶往下、深的优先) 选候选，在途 ≤ P = `min(max_inflight, 64)`，
在途 + 已完成 ≤ 2P；扫描上限 4P；已到达的候选被**展开**成它自己的子目录，所以预取能穿透多层。

**[计划] P7 把块变成流式**：`Frame.children` 从全量 `VecDeque<Child>` 变成打开的游标 + 一页；
`Listing.arrived` 从全量 `Prepared` 变成游标句柄 + 首页；filter 决定从"到达时对全部子项做一遍"
变成"逐页做"；预取的含义从"把整份 listing 取回来扣住"变成"开着游标、一页在途、不排到就不继续读"，
背压交给协议层。代价是栈上每帧持有一个打开的游标横跨整棵子树：NFS / S3 是无状态 cookie (几乎免费)，
**CIFS 是真实句柄，ONTAP 会超时回收** —— 这是终局里最脆的一环，需要游标失效后的重建策略。

### 三、admission cursor —— `NameBytes` 模式

驱动循环、序号分配、窗口、marker 顺序**全部不变**，只有"块从哪来"不同。

1. **当前块必须物化** —— 流式与排序在单目录内互斥，要排就得看完整块。这是物理约束，不是实现取舍。
2. **排序在列举任务里做**，不在驱动任务上：`prepare` 跑在驱动任务上，千万条的 O(n log n) 会让
   驱动长时间不到 await 点，取消响应变钝。
3. **`sort_unstable_by`**，键是最后一段的 UTF-8 字节 (`entry_name(&x.path)`)。不是口味问题：
   稳定排序要为 `Vec<SourceDescriptor>` 额外分配 n/2 个元素，千万条就是 +GB 级峰值。
   代价是同名子项相对次序未指定 (同目录同名是 backend 缺陷)。
4. **[计划] P8 超阈值外部归并排序**：分批排序写 run，k 路归并回读；驱动侧拿到的是"一个有序块"，
   无论背后是 `Vec` 还是归并迭代器。临时文件清理挂在取消路径上。
5. **下钻序自动有序**：`descend` / `slots` 按排序后的顺序建立 (`prepare` 按 `entries` 顺序 →
   `admit` 同序 `push_back` → `next_after_block` 队首取)，所以排一次，块内序与下钻序同时有序。
6. **事件**：`DirectoryListed.child_order = NameBytes`；列举级 failures 排在块首、**在序之外**
   (它们可能没有合法名字)；观察失败的 `EntryFailure` 有真路径，留在自己的排序位上；
   marker 多重集与每个 `SubtreeSummary` 的八个计数在两种模式下**必须完全相同**。
7. **消费者必须逐段比较**：整条流是有序 DFS 前序，等价于"把分隔符视作小于所有其他字节"。
   `a/`(含 `a/x`) 与 `a.txt` 给出 `a, a/x, a.txt`，而整路径字节序给 `a, a.txt, a/x`
   (`.`=0x2E < `/`=0x2F)。拿整条相对路径按字节比会错位，且编译通过、小样本全对 —— 错的后果是
   判成"目标端有而源端无"→ **删数据**。

### 四、单目录海量子目录：低内存模式

`Frame.slots` 与 `Prepared.descend` 各为一个将要下钻的子目录留一条记录。1000 万子目录 =
1000 万条记录，仍是 GB 级。根因是**物理的**：任何要回到某目录 1000 万个子目录的遍历，都必须
记住 1000 万个返回点，否则就得重新列举 (而目录可能已变，一致性也没了)。legacy 的无界任务队列
是同一件事。缓解分三层，**逐目录动态触发**，因为只有流着才知道这个目录有多少子目录：

**第 0 层 (恒定生效，[计划] 随 P9)** —— 槽位只存**名字**，父路径在帧里，下钻时现拼。
1000 万 × ~20 B ≈ 200 MB，对比存全路径的 ~1.2 GB。不需要切换模式。

**第 1 层 (默认逃生门，[计划] 复用 P8 的溢写)** —— 单帧槽位累计字节越过阈值 (如 32 MiB) 时，
把槽位列表溢写到与排序缓冲同一套临时文件机制。槽位按序追加、按序读回，是最理想的磁盘访问模式。
**保住全部契约**：块序不变、marker 顺序不变、预取照常。一旦某帧溢写，其生命周期内保持溢写，
不回摆 (避免抖动)。

**第 2 层 (无盘兜底)** —— 该帧切到**即时下钻**：流式时遇到子目录立刻进去，不记槽位。内存归零，
但三项代价：

- 放弃该目录的预取 (退化为串行)；
- 父游标必须横跨整棵子树保持打开 —— **CIFS 上恰恰在最需要它的场景下最脆**；
- **事件时序退化**：子目录的 `SubtreeComplete` 会早于父目录的 `DirectoryListed`。G1 (每个被列举
  目录一个完成事件、在其直接子项之后) 仍成立，但"`DirectoryListed(D)` 先于任何子目录事件"
  不再成立。

第 2 层必须**如实汇报** —— 沿用 `DirectoryListed.child_order` 开的先例 (逐目录、正交于完整性的
属性)。它改变事件时序，依赖早通知的消费者必须能看出来，否则会静默失效，与 P3 那条
"`Filtered` 误报成 `Complete` = 目标端删数据的假许可"同族。

**`NameBytes` 下只有第 0 / 1 层。** 排序本来就要物化整块，第 2 层在语义上不成立 —— 没看完整块
就不知道谁排第一。所以有序模式下海量子目录只能靠溢写；无盘时该目录**必须拒绝而不是静默降级**。

### 五、三者对比

| | legacy work-stealing | 终局 `Admission` | 终局 `NameBytes` |
|---|---|---|---|
| 输出顺序 | 不确定 | 确定 (backend 列举序) | 有序 DFS 前序 |
| 子树完成语义 | **结构上做不到** | 有，精确计数 | 同左 |
| 当前块 | 流式 | 流式 | 必须物化 |
| 内存 | 待处理队列**无界** | 配置决定 | 配置决定 + 排序缓冲 (超阈值溢写) |
| 背压 | 只在条目通道 | 贯通到协议翻页 | 同左 |
| 不规则树负载均衡 | **更好** (窃最浅) | 无窃取 | 无窃取 |
| 头阻塞 | **没有** | 有 | 有 |

一句话：**`Admission` 用「输出的头阻塞」换「输出的确定性 + 子树完成语义」，work-stealing 反之。**
两者并非竞争架构 —— work-stealing 是**调度策略**，admission 是**输出契约**，前者可以成为后者
预取窗口内部的实现细节。P6 删 legacy 前值得先评估"窃最浅"这个启发式是否值得吸收。

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

> 这是 **legacy** `StorageEnum::walkdir` 的调度，P6 删。role-based 遍历用 admission cursor，
> 两者的完整处理逻辑与对比见上面 [遍历的三套处理逻辑](#遍历的三套处理逻辑)。

所有 backend 的 walkdir 共享 `walk_scheduler.rs` 的 `WorkerContext<T>`：

- 每 worker 持有一个 LIFO 自栈 + 邻居 FIFO 引用。
- `pop_task()` — 先弹自己栈 (LIFO，保留局部性)，再 FIFO 窃邻居最旧任务 (减少冲突)。
- `push_task()` — 递增 `active_tasks` + `notify.notify_waiters()`。
- `is_done()` — `active_tasks == 0 && active_producers == 0`。

**已知缺陷 (2026-09-21)**：`notify_waiters()` **不存 permit**，对当时没在等的 worker 就是丢
唤醒。`wait_for_task()` 里那个 100μs 超时就是为此兜底 (注释写着"防止通知丢失")，代价是空闲时
每 worker 每秒醒 1 万次、每次依次锁 N-1 个 tokio `Mutex`。正解是 `notify_one()` (一次 push 一个
任务、唤醒一个 worker，1:1)，然后超时可以去掉。同一个成因刚在 smb-rs 上被证实并修复
(PR #80，目录列举的分页握手丢唤醒)。因为这套要在 P6 删除，是否现在修由维护者决定。

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
