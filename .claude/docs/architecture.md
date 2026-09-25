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
7. **消费者比较的键是 `(父目录, 名字)`，不是整条路径。** 块不交错 —— 一个目录的全部子项都排在
   任何孙子之前，目录本身按排序后的深度优先访问。所以 `a/`(含 `a/x`) 与 `a.txt` 给出
   `a, a.txt, …, a/x`：两者都是根的子项，都排在 `a` 之下的任何东西之前。**这不是"有序 DFS
   前序"** —— 真正的前序要求 `a, a/x, a.txt`，那需要即时下钻，而块序恰恰不允许。拿整条路径
   逐段比较会把 `a/x` 排到第二位、静默错位，读出来就是"目标端有而源端无"→ **删数据**；而它在
   单个目录内与正确规则恰好一致，所以小样本全对。
   (2026-09-21 更正：本节初稿把它写成"有序 DFS 前序"并给了 `a, a/x, a.txt` 的例子，是错的 ——
   真机跑 `examples/storage_role_operations traverse --order name-bytes` 才发现。)

**真机证据 (FAS2750，2026-09-21)**：同一棵树 (目录 `Zoo`/`a`/`B`/`bar` + `a/a1`、`a/a2`) 同时
播到 CIFS `ontap_lisaauto_cifs` 与 NFS `ontap_lisaauto_nfs` (v4.1)，`--order name-bytes` 两侧
**逐行相同**；`--order admission` 两侧不同 (CIFS `Zoo, a, B, bar` / NFS `B, Zoo, a, bar`)。
字节序 `B`(0x42) < `Zoo`(0x5A) < `a`(0x61) < `bar`(0x62)，而大小写不敏感排序会给
`a, B, bar, Zoo` —— 所以这同时证明字节序压过了 NTFS/ONTAP 的 `$UpCase` collation。

> **陷阱**：ONTAP 的 NFS READDIR 在这棵树上**恰好就返回字节序**。只测 NFS 会得出"根本不用排序"
> 的结论，而同一棵树在 CIFS 上并非如此。不要用单个 backend 的原生顺序推断是否需要排序。

### 四、单目录海量子目录：低内存模式

`Frame.slots` 与 `Prepared.descend` 各为一个将要下钻的子目录留一条记录。根因是**物理的**：
任何要回到某目录 1000 万个子目录的遍历，都必须记住 1000 万个返回点，否则就得重新列举
(而目录可能已变，一致性也没了)。legacy 的无界任务队列是同一件事。

关键在于槽位**只增不减**：`next_after_block` 要等 `closed == true` (块完全准入) 才开始消费，
而 `closed` 要等流读尽。所以流的整个过程里槽位没有任何出口。

缓解是**一个恒定的基线 + 两个互斥的逃生门**，**逐目录动态触发** (只有流着才知道这个目录有多少
子目录)。它们不是架构分层，也不叠加：基线总是开着，逃生门由同一个全局预算触发、对**同一帧**
二选一 (不同帧可以各走各的)。编号唯一的含义是**代价递增**。

#### 基线：名字 arena (恒定生效，不需要触发，[计划] 随 P9)

槽位不是 `VecDeque<Slot>`，而是一段**连续的、长度前缀的字节缓冲**，从头顺序消费 —— 因为槽位
本来就只按 FIFO 用一次，不需要随机访问，也就不需要 per-slot 结构体和偏移表。父路径在帧里，
下钻时现拼。

| 1000 万子目录 | 驻留 |
|---|---|
| `VecDeque<Slot>` 存全路径 | ~1.5 GB |
| 名字 arena | **~210 MB** |

约 7 倍。**基线把逃生门的触发门槛抬得很高** —— 几百 MB 在多数部署里是可以接受的，于是下面两层
根本不会触发，契约、预取、句柄生命周期全部不受影响。

#### 逃生门 A：arena 溢写 (首选，[计划] 复用 P8 的溢写)

arena 本来就是顺序追加、顺序消费的字节流，溢写它是**平凡的**：同一段缓冲写到文件再顺序读回，
不需要排序、不需要索引。

- **保住全部契约**：块序不变、marker 顺序不变、预取照常。
- **句柄立刻释放**：一口气把列举读完、arena 落盘、关掉目录句柄。
- 一旦某帧溢写，其生命周期内保持溢写，不回摆 (避免抖动)。

**"要盘"指的是临时溢写空间，不是持久化。** 生命周期 = 该目录遍历期间，帧结束即删，崩溃直接
丢弃 (整场遍历本来也没了)，**不需要 fsync**。它也不是恢复状态：续传需要的一切都在目的端
(ADR-0006；本地恢复存储与 `DATA_MOVER_RECOVERY_DIR` 已在 C21 删除)，运行处不能有要跨进程重启
活下来的东西，所以溢写文件**不该**被写成可以指望的状态。

"无盘"有三种真实来源，其中两种不是物理限制：只读根文件系统的容器；**只有 tmpfs** (溢写到
tmpfs 就是写进内存，一点都不省 —— 实现要么识别、要么写进文档，否则"开了溢写怎么还 OOM"
会查很久)；**策略禁止** —— 溢写把**路径名**落到本地盘，而路径名往往就是敏感信息 (客户的目录
结构、人名、项目代号)。由此的实现要求：文件 `0600` / 目录 `0700`，取消与正常结束**都要确定
删除**，且必须是**显式配置的 opt-in**，不能默默找个 temp dir 就写。与 R8 同类，只是对象从
凭据换成了路径。

#### 逃生门 B：槽位窗口降为 K (无盘兜底)

不是"即时下钻"—— 那是 K=1 的退化情形。真正的参数是**记多少**：

| | 槽位窗口 | 内存 | 列举并发 |
|---|---|---|---|
| 默认 | 无界 | 随子目录数增长 | 满 |
| 逃生门 B | **K 常数** | **O(K)** | **K 路** |
| (即时下钻) | K = 1 | 0 | 无 |

**K 取 `P = min(max_inflight, 64)`** —— 列举预取在途本来就不超过 P，窗口开得比 P 大换不来
额外并发。所以逃生门 B **仍是 K 路并发**，不是串行。

**切换的完整过程**：

触发在 `admit()` push 槽位时记全局累计 (**必须是全局预算**：逐帧阈值不封顶总量，100 个帧
各差一点就是 100×阈值)：

```
global_slot_bytes += name_len + 常数
if global_slot_bytes > BUDGET:
    候选 = 槽位字节最多的帧
    if 候选.closed → 只能走 A (溢写)      ← 已无流可停，换策略救不了
    else           → 可走 A 或 B
```

切换瞬间做四件事：

```
1. frame.mode = Windowed { k: K }
2. 停止读该目录的流            ← 游标 park，不再新增槽位
3. 立刻开始消费已累积的槽位     ← 不等 closed 就下钻，这是时序退化的来源
4. 在途的预取一个都不取消       ← 已付过的 RTT 不浪费；P2 的教训是 detach 不 abort
```

切换**不释放**已占用的内存，只止住增长：槽位从预算值单调下降，降到 K 以下就补读流，
最终在 K 附近振荡。内存峰值停在预算上 —— 这正是预算该给的保证。

**不回摆**：时序已经退化，回到严格块序也挽不回已发出的顺序，反而让同一目录行为反复横跳。

四条不能破坏的不变式：序号不回溯 (切换只改"下一个给谁")；已 spawn 的观察不受影响；
`closed` 一次性标志不变、`DirectoryListed(D)` 仍只发一次且仍在 D 全部直接子项之后；
在途预取不取消。

**代价**：

- **事件时序退化**：子目录的 `SubtreeComplete` 会早于父目录的 `DirectoryListed`。G1 (每个被
  列举目录一个完成事件、在其直接子项之后) 与 G2 (祖先先被列举) 仍成立，失去的是
  "`DirectoryListed(D)` 先于任何子目录事件" —— 即从**早通知**退化成**迟通知**。
- **父游标横跨整棵子树保持打开**，见下。

必须**如实汇报**，沿用 `DirectoryListed.child_order` 开的先例。但有个躲不掉的局限：
`DirectoryListed(D)` 在最后才到，而退化在中间就发生了，所以这是**事后确认不是事前预警**。
消费者真正该做的是**不假定**父的 `Listed` 早于子事件 —— 契约写死，字段只作确认。

#### 逃生门 B 的句柄问题

**读完流才能放掉目录句柄，但读完流就必须记下全部槽位。** 逃生门 B 选择了不读完，于是该目录的
列举游标要一直开到整棵子树遍历结束 —— 1000 万子目录可能是数小时。

这个问题的严重性**完全取决于协议的"位置"模型**：

| backend | park 期间要持有什么 | 逃生门 B 可用性 |
|---|---|---|
| NFS | READDIR cookie (**无状态**，几十字节) | 可用，句柄问题不存在 |
| S3 | continuation token (**无状态**) | 可用 |
| Local | 一个 fd，无超时 | 可用，只受 fd 配额 |
| **CIFS** | **CREATE 出来的真实目录句柄** | **有问题** |

CIFS 上三条路，都不便宜：

1. **保活** —— 在被 park 的句柄上定期做不推进游标的操作 (QUERY_INFO)，防空闲回收。
   防不住 session 失效 / 服务器重启 / 网络中断。**"QUERY_INFO 不推进枚举状态"以及 ONTAP 的
   空闲回收阈值都必须在 FAS2750 实测，不得假设。**
2. **断开重连 + 按位置续** —— `SMB2_INDEX_SPECIFIED` + `FileIndex` 理论上可 O(1) 续，
   **ONTAP 是否支持必须实测**。不支持就只能重扫跳过，每次补窗口 O(N)，总代价 O(N²/K)：
   N=1000 万、K=64 时是 1.5×10¹² 次服务端条目遍历，**不可用**。
3. **在 CIFS 上不提供逃生门 B** —— 无盘且 arena 放不下时**拒绝该目录并返回 typed 失败**。

**推荐第 3 条。** 理由：逃生门 B 在 CIFS 上大概率半路失败，而**半路失败比一开始拒绝糟糕得多** ——
那时已经遍历了几小时。注意由此得到的对称性：**逃生门 B 只在句柄不成问题的 backend 上可用**
(NFS / S3 / Local)，而 CIFS 恰好既是句柄最脆的、也是最该靠基线 + 逃生门 A 的。

#### 取舍总表

| | 内存 | 契约 | 句柄 | 需要 |
|---|---|---|---|---|
| 基线 arena | ~7× 改善 | 全保住 | 读完即释放 | — |
| 逃生门 A 溢写 | 常数 | 全保住 | **读完即释放** | 本地磁盘 |
| 逃生门 B 窗口 K | 常数 | **时序退化** | **持有整棵子树** | — (CIFS 上不提供) |

优先级是 **基线 → A → B**，且 A 与 B 不是等价备选：**A 唯一的代价是要盘，换来契约与句柄
双全**；B 省掉磁盘，代价是时序退化**加上**句柄长持有。

**`NameBytes` 下只有基线与逃生门 A。** 排序本来就要物化整块，逃生门 B 在语义上不成立 —— 没看完整块
就不知道谁排第一。所以有序模式下海量子目录只能靠溢写；无盘时该目录**必须拒绝而不是静默降级**
(静默降级 = 无序却声称有序 = 目标端删数据)。


### 五、三者对比

| | legacy work-stealing | 终局 `Admission` | 终局 `NameBytes` |
|---|---|---|---|
| 输出顺序 | 不确定 | 确定 (backend 列举序) | 块序 + 块内/下钻按名字节序 |
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
