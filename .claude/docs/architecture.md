# Architecture

> 本文保留旧实现的历史背景。当前操作接口和依赖规则以
> [Storage and Transfer Architecture](../../docs/architecture/storage-transfer-architecture.md) 为准；
> `StorageEnum` 和旧管线已删除，历史代码片段不可作为实现模板。

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
StorageEnum 提供 传输/元数据操作 (copy_file, copy_file_with_cancel,
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
    ├──→ traversal::StorageTraversalSource   Namespace::List + Metadata → TraversalItem 流 (默认 Unordered：worker 栈 + 窃最浅；Blocks：深度优先块顺序 + 列举预读)
    ├──→ storage::delete_tree                Namespace::{List,Delete} → DeleteTreeItem 流
    ├──→ storage::create_directory_all       Namespace::{CreateDirectory,Stat} 逐层创建
    └──→ integrity::compare                  ReadSource + Metadata
```

`StorageTraversalSource` 有两种调度，由 `TraversalRequest.order` 选 (P7 §3)：**`Unordered` (默认，P7 C10)**
见下文「遍历的两种调度」第一节之后的 Unordered 小节 (`storage/unordered.rs` + `unordered/{pool,worker,page_io}.rs`)，续扫检查点见「一·六」；
**`Blocks`** 的调度 (`src/traversal/storage/{blocks,cursor,stream,observe}.rs`，公共件在 `src/traversal/storage.rs`)：
准入游标按"块"走深度优先 (`TraversalOrder::Blocks`) —— 一个目录的子项按列举顺序逐页连续输出，
列举结束后按子目录名字节序依次下钻；序号只由游标分配，重排缓冲按序号输出，所以列举/观察乱序完成
不影响顺序。列举预读额度 `min(max_inflight_operations, 64)` 只计**等页**的列举，游标未到的已起列举
(等页 + 停在首页后的) 合计另限 2 倍额度 (否则早期预读的浅层兄弟会占住额度，饿死深层)；与观察窗口分开；
游标马上需要的列举不计额度 (防饿死)。
deferred filter 已决定下钻的 `Pending` 槽位也会预读。列举结果**在列举自己的任务里**就对每个子项做
filter 决定 (deferred 的除外)，所以已预读、游标还没走到的目录，其子目录也能继续预读；候选按
输出顺序深度优先展开 (`cursor.rs` 的 `Search`)。驱动每轮先 `try_recv` 收下所有已到的页与结束，
不只是游标正等的那个 —— 否则已预读目录的子目录不会被展开预读。
**按页观察 (P7 C0)**：不需要 I/O 的子项 (非 symlink，且列举已带齐观察计划要的元数据) 也在列举任务里
组装成 `ObservedEntry`，驱动一块只被唤醒一次、准入时直接落槽；filter 隐藏的子项在那里只计数、不进块。
需要 `modified` 的 filter (C0b)：子项在列举任务里已组装好时，就用这条 entry 当场决定 (与 deferred 读到的值
是同一个)，只有要 I/O 或观察失败的子项才 deferred；deferred 的下钻槽位在 prepare 时就按块内顺序放进
`slots`，用列举任务发的 ticket (`Shared.tickets`) 作键，而不是准入时的输出序号 —— 否则即时与 deferred
混在一块时下钻顺序会乱。
需要 I/O 的 (symlink 目标、列举没带的元数据) 每块一个观察任务 (`observe.rs` 的 `Observations`)，
游标准入时把子项喂给它，任务内 `FuturesUnordered` 并发，结果逐个回送，总量受准入窗口约束 —— 不再每条目
一个任务。重排缓冲 (`output.rs`) 对恰好是下一个序号的槽位走就绪队列，乱序到达的才进 `BTreeMap`。
单条组装与端到端每条目成本见 `storage/tests/bench.rs` (ignored，`--release --ignored --nocapture` 手跑)。
身份的分配 (P7 C0 第四项)：`SourceIdentity` 的稳定字节 ≤ 46 字节时内联 (`StableBytes`，ONTAP 的 32 字节 NFS 句柄、
CIFS file id、Local dev+inode 放得下；更长的 NFS 句柄与 S3 路径 + ETag 仍在堆上)，省掉每条目一次分配；`BackendIdentity` 改 `Arc` 共享在本机交替实测里
反而更慢 (单线程描述 + 组装 380 → 444 ns，四线程 170 → 184 ns，端到端无差别)，没有采用 —— 共享引用计数
是每条目一次原子读改写，glibc tcache 下复制二十来字节的字符串并不比它贵。
取消或结束时在途的列举和观察都 detach 而非 abort (CIFS 句柄)；观察任务在块的 feed 被丢弃后跑完手上的
才退出。

**事件与计数 (P7 C2)**。`TraversalItem` 是 `Entry` / `Failure(ScanFailure)` / `DirectoryListed` /
`Checkpoint` (`Resumable::On` 时发)。`ScanFailure.scope` 分 `Child` (这一个子项：列举描述不出，或观察失败) 与
`Listing` (这个目录的列举不完整；`delivered` = 失败前已发出的子项数，C4 之前整目录收齐才发，恒为 0)。
游标在块被完全准入时排一个 marker 槽位，输出侧把它变成 `DirectoryListed { path, outcome, counts }`：
一个被列举的目录 D 的输出是 D 的块 → `DirectoryListed(D)` → 各子目录递归同样的序列 (空目录也发)。
**只有真被列举过的目录**才有它 —— `max_depth` 边界目录发了 Entry 却没列举；filter 隐藏但仍下钻的目录
有它、却没有自己的 Entry。列举失败的目录：块里是 `Failure{scope: Listing}`，然后
`DirectoryListed.outcome = Unreadable` (有已发出子项时 `Interrupted`)；子项失败不改 outcome，只进
`counts.failed_children`。消费方的删除安全规则写在 `ScanFailure` 的文档上 (P7 §4.1)：只有 `Completed`
才能做删除检测；`Listing` 失败的目录整棵旧子树不参与删除；`Child` 失败的条目本身不参与。
`SubtreeComplete` / `SubtreeSummary` / `DirectoryListing` / `ChildOrder` 已删 (Blocks 下可由
名字序 DFS + `DirectoryListed` 推出)。

计数 (`src/traversal/counts.rs`)：每块 `BlockCounts`，全场 `ScanTotals` (在 `TraversalCompletion.totals`，
只有 `Completed` 才有)。不变式 `listed == emitted + filtered + failed_children + vanished` (每块与全场都成立，
`is_balanced()`)；`internal_hidden` (`.data-mover-*`；NFSv3 上 ONTAP 的 `.snapshot`，P7 C7b) 不在 `listed` 里；pruned / truncated 与之正交；
`files + directories + symlinks + other == emitted`。带历史的 key 在分段前只计一次；`bytes` 是当前版本字节，
`versions` / `delete_markers` 数历史记录，`all_versions_bytes` 数所有对象版本的字节。无历史时后者等于 `bytes`。
当前快照与消费方契约见 [ADR-0008](../../docs/adr/0008-streaming-traversal-and-object-history.md)。

**错误模型 (P7 C3，§8)**。三层：子项失败 (`Failure{scope: Child}`，流继续)、列举失败 (`Failure{scope: Listing}`，
该目录不完整、兄弟继续)、会话终止 (`TraversalTerminalFailure::Session`，现为 `non_exhaustive`)。两种**不是失败**：
- **消失** (`NotFound`，Q12)：列举返回了、描述或观察时已不存在的子项 (Local `readdir` 后 `lstat`、观察时的
  元数据 / `read_link`；列举里是 `ChildFailure` 且 class 为 `NotFound`，`ChildFailure::is_vanished`) 计入
  `vanished`，不发任何项、不进 `failed_children`；deferred 的目录消失了就不下钻。观察时的 `NotFound` 要先
  `stat` 确认子项本身不在了才算消失 (子读取可能是别的东西 NotFound，如 xattr 在列举与读取之间被删)；子项还在
  则是该子项的 `Conflict` / Transient 失败。父目录列出后、自己的列举开始前已不存在的**子目录**：块 `Complete`
  且为空、`DirectoryListed.vanished = true`、不是列举失败，计入 `ScanTotals.vanished_listings` (它已作为父块的
  子项计过——emitted，或列举时就定了下钻、观察时才发现消失——不再进子项方程)。**遍历根不存在仍是列举失败**。
  判定“消失的子项”只用一个谓词 `listing::is_vanished`：class `NotFound` 且路径不是目录本身。
- **节流** (`FailureClass::Throttled` / Transient，Q11)：S3 429 / 503 / `SlowDown` 等、NFS `DELAY` / `JUKEBOX`。
  S3 列举在生产者内按页退避重试 (`src/storage/throttle.rs`，单个列举累计约 2 分钟、抖动、随取消停止)，预算耗尽
  才是**该列举**的失败；遍历把任何 `Throttled` 会话失败收窄为那个列举 / 那个子项的失败
  (`throttling_scoped_to`)，节流永远不结束遍历。
拼不出路径的子项 (非 UTF-8、库有损解码出 U+FFFD、S3 键越出前缀) 是**无名**子项失败：路径是目录，
`ChildFailure.raw_name` 与诊断里带原始字节，`EntryFailureIdentity` 由目录 + 原始名派生；不再用
`@local-unix-hex:` 之类的替身路径。`TraversalRequest` 为 `non_exhaustive`，用 `TraversalRequest::new(root)`
加 setter 构造。

marker **占准入窗口**的名额，不绕过它，所以不会让 `admitted()` 越界。
(唯一的例外是既有的 `queue_failure`：`take_top_listing` 为列举自带的 N 个不可描述子项、以及
列举失败本身排队失败项时不查窗口，`admitted()` 可短暂到 `max_inflight + N`。它们同步落盘、
下一次 flush 就排空，且 N 受该次列举已在内存里的结果限制。) 重入安全靠位置：`DirectoryListed`
由 `Frame.closed` 一次性标志守护 (`next_after_block` 每槽位调一次)。

计数在**输出侧** (`storage/output.rs`) 而不是游标：槽位离开重排缓冲时比它早的槽位全部已经过去，
所以在 `DirectoryListed` 处整个块 (含 deferred 决定) 已落定，计数是精确的；全场总数是各块之和。
账目分两条路进来：立即决定隐藏的子项**根本不分配槽位** (给它分配会让一个排除百万文件的 filter
反过来被准入窗口节流)，与 `listed` / `internal_hidden` 一起记在 `BlockFacts`；deferred 决定随
`Settled::Child` (`ChildItem::Filtered` / `ChildItem::Vanished`) 与 `.descent` 走。两条路的 `max_depth` 与 filter 判定顺序统一为
先判深度，否则同一个目录会因 filter 是否 deferred 而一会儿报 Pruned 一会儿报 Truncated。
两条路计数相同由 `storage/tests/counts.rs` 钉住。

**覆盖面**：

1. 五个 backend 都出借 `Namespace`（P7 C1 起是类型化动词 `stat` / `list` / `read_link` /
   `create_directory` / `delete` / `rename` / `capabilities`；`list(ListRequest) -> ListingStream` 是唯一
   列举入口，页里带名字，`src/storage/listing.rs`）。S3 只有 `stat` / `list`（ADR-0006 C22，按 `<path>/` 前缀
   delimiter 列举；连接时判定为版本化的桶上 `list` 每个 key 一个子项、带整段历史 `ObjectHistory`，P7 C11b 起；
   `list_versions` / `TraversalVersions` 已删），变更动词 `Unsupported`；
   `delete_tree` / `create_directory_all` 经 `Storage::mutable_namespace`（看
   `Namespace::capabilities().mutations`）在任何 I/O 之前返回 `CapabilityUnavailable`。
   Local 的 namespace 在 `src/storage/backends/local/namespace.rs`，与观察角色共用同一个
   `cap_std::Dir` 沙箱；路径逐段用 `open_dir_nofollow` 解析，**任何一段**是 symlink 都拒绝，
   最后一段在父目录句柄里操作 (描述/删除的是链接本身)。
2. 列举里个别子项无法描述 (例如文件名拼不成 `StoragePath`、NFS READDIRPLUS 某项没带属性或
   句柄、HDFS 某项 status 转换不了) 时，backend 把它放进 `ListingPage::failures`（`ChildFailure`），
   收集型消费方经 `collect_listing` + `ListingPage::into_descriptors` 统一处理：失败逐项上报，
   兄弟条目照常。Local / CIFS / NFS / HDFS 都只让关乎目录本身的错误 (打不开、读目录失败) 整个失败；
   S3 的名字拼不出同样逐项报，但单个对象的 size / key 异常仍是会话失败 (待修)。分类见
   [error-taxonomy.md](error-taxonomy.md)「列举」。

**关键点**：没有 `Storage` trait，没有 `dyn Storage`，没有 vtable。这是有意的 — 4 个 backend 协议差异极大，trait 抽象会塞 30+ 默认方法和大量 `Self`-bound 限制，不如 enum + match 直接。

代价：**新增/修改一个 StorageEnum 操作 = 五处同步**。详见 [storage-enum-dispatch.md](storage-enum-dispatch.md)。

## 遍历的两种调度

`TraversalOrder::Unordered` 默认无全局顺序，`Blocks` 提供逐目录边界；都逐页消费列举。
本节标 **[计划]** 的低内存方案属于 P8/P9，尚未实现。

### 一·五、`Unordered` 调度器 (P7 C10，默认)

worker 栈与窃取调度，统一使用 role 列举 / 观察 / 计数 / 错误模型：

- **K 个 worker** = `min(max_inflight_operations, 64)`；namespace 的 `parked_listing_limit()` 有值时 (CIFS 8)
  再取 min —— worker 等消费方时手里的列举一直开着，所以每个打开的列举都可能停车，K 就是打开句柄数的上界。
- **栈** (`unordered/pool.rs`)：每个 worker 一个 `VecDeque<Arc<DirectoryWork>>` + `active` (正在列的目录)，
  `parking_lot::Mutex`。自己的栈 LIFO 弹最新 (深度优先，待处理集合小)；空了按各槽位发布的 `front_depth`
  (无锁读) 挑**最浅**的受害者，按下标序同时锁两个槽位，出受害者栈头与登记为自己的 `active` 在同一临界区
  —— 任何时刻每个未完成目录都在某个槽位里 (栈上或 active)，C11 的前沿快照按下标序锁全部槽位即得一致视图，
  不需要暂停 worker。窃取后受害者还有剩余且有空闲 worker → 再唤醒一个 (级联)。
- **终止**：全局 `outstanding` = 已压栈未完成的目录数。子目录先 `fetch_add` 再入栈，父目录在列举结束、
  全部子目录入栈之后才 `fetch_sub`；减到 0 的那个 worker 取消 `halt` (请求令牌的子令牌)，所有等待都选它。
  空闲 worker 睡在 `Notify` 上，登记 `idle` 与压栈两侧各一个 SeqCst fence (Dekker)，压栈只在有空闲 worker 时
  `notify_one`，忙时每目录零唤醒。会话失败 / worker panic / 取消同样取消 `halt`。每个 worker 等的是自己的
  `halt.child_token()` (也传给列举请求)，64 个 worker 与它们的列举生产者不挤同一个等待表。
- **一个目录** (`unordered/worker.rs`)：worker 直接读 `ListingStream` (没有中间的列举任务)，每页先
  `now_or_never` 偷看 (单页目录的结束随页一起到)；页用与 Blocks 相同的 `stream::prepare` 准备 (扫描根自己的
  对象守卫、消失、即时决定、无 I/O 观察、下钻槽位)；要 I/O 的子项每页一个 detach 任务
  (`unordered/page_io.rs`，`FuturesUnordered`，全局 `max_inflight_operations` 个观察许可，不 abort)；按列举序
  组成一批 (页里描述不出的在前)，**整批交给会话之后**才把子目录压栈 —— 所以目录的 Entry 一定先于它下面的
  任何项。列举失败：非根、`NotFound`、一个都没返回 → `vanished_listings`；否则 `Failure{scope: Listing,
  delivered}` 跟在已交出的子项之后。`halt` 触发后结束的列举不报失败 (R10)。
- **没有** 序号、重排缓冲、`DirectoryListed`、每目录计数；每个 worker 本地累加 `BlockCounts` + 列举计数，
  结束时相加进 `ScanTotals`。与 Blocks 同一棵树：条目集合、失败集合、`ScanTotals` 逐字段相同
  (`storage/tests/unordered.rs` 的 `same_as_blocks` 在多种 fake 上钉住)。
- **一个 key 的各段在一起** (P7 C11b，Q8)：版本化桶上一个 key 一项；历史超过 10 万条时 `tally_child` 把它切成多段放进
  同一批，会话通道 `send_whole` 整批一条消息，别的 worker 插不进一批中间（C10 的 `trailing_run` / carry 已删）。
- **会话通道按批** (`traversal/channel.rs`)：`mpsc::unbounded` 传 `Vec<TraversalItem>` + 按条计的信号量
  (`max_buffered_items` 个许可，批用完才还)。Blocks 的 flush 也是一次一批 (`send`，超过上限按上限切，单一发送方
  顺序不变)；Unordered 用 `send_whole` (超过上限就等整个上限空出来再整批发)。

`traversal::pair_directory_blocks`（`traversal/pairing.rs`，P7 Q4）按原始名字哈希配对两侧已到 `DirectoryListed` 的块，
显式映射两侧目录到逻辑目录，借用观察与历史段，保留同名不同类型和重复冲突。不完整块、过滤、失败或名字语义不精确时
不推断缺失；调用方仍须等双方会话 `Completed`、排除祖先失败子树并对旧记录应用相同过滤 / 深度，才可用于删除判断。

### 一·六、续扫检查点 (P7 C11，两种调度都有，默认关)

`TraversalRequest::resumable(Resumable::On { every })` 时流里约每 `every` 一个 `TraversalItem::Checkpoint`，
取消 / 会话失败时最后一项也是检查点 (此时取消不丢已产出的项，会话排空到检查点)；`resume_from(Checkpoint)`
续扫，指纹 (storage、root、order、filter 键、`max_depth`、连接是否列历史) 不符在任何 I/O 之前拒绝
(`TraversalTerminalFailure::Resume`)。检查点记的是**还要列的目录**，不是见过的条目：Unordered 取 `Pool::snapshot`
的前沿；Blocks 取序号 ≤ `Output::delivered` 的最新位置链，续扫时重列链上祖先 (不发出) 定出剩下的子目录。
不求精确：前沿里的目录整块重列 (有重复、不丢)。编解码 `src/traversal/checkpoint.rs` (`DMTC` v2，golden 钉住；旧版本拒绝)，
调度侧 `src/traversal/storage/resume.rs` + `resume/{positions,relist,blocks}.rs`；设计 P7 §9.3。

### 二、admission cursor —— `Blocks` 模式 (P7 C4 起逐页)

每个列举一个 detach 的任务 (`storage/stream.rs`)：**先等 credit** → `stream.next()` 取一页 →
在任务里 prepare (即时 filter 决定、无 I/O 观察、下钻槽位) → 不等待地偷看下一项 → 经共享 channel
交给驱动。credit 初始 1，游标每取走一页加 1。所以遍历对一个列举最多持有：游标正在准入的一页 +
交出未取的 1 页 (已 prepare) + 偷看取下的至多 1 页 (原始)，**与目录多大无关**；backend 流自己另有
预读 (from_producer：channel 1 页 + 生产者手里 1 页)。偷看到的结束 (`End` / 失败) 不需要 credit：
生产者正常返回不再占 channel 槽发 `End` 消息，而是先置 `finished` 标志再丢掉最后一个 sender，
所以单页小目录的页一收到、结束就可见，页与结束一起到，驱动一次唤醒 (偷看没看到的结束要等游标取走前一页)。任务中途 panic 由 `Reporter` 的 Drop 报 `Internal`。
**Local 自 C5 起逐页读目录**（每页一次 `spawn_blocking`，`ReadDir` 带进带出）；**HDFS 自 C8 起逐页列举**（`list_status_iter` 按页取，列举前不再 `get_file_info`，取消 / drop 立即放弃进行中的请求；中途可重试失败从最后读到的名字之后续读）；**S3 当前对象自 C6 起逐页列举**（`ListObjectsV2` 每个服务端页一到就按 `page_hint` 交出，不假设 key 有序：公共前缀用集合去重、续页只用不透明 token、节流 / 瞬时失败用同一 token 重发；版本化桶按目录收齐，C11b 起每个 key 一项历史）；**NFS 自 C7 起逐页列举**（READDIRPLUS 的回复逐条取、按 `page_hint` 攒页交出，一次只多读一个回复；取消 / drop 在下一个请求之前停，不放弃在途请求；首页前后都保留缓冲、从最后完整回复的 cookie 续读并去重（C7c，首页前由 `c0ff649` 补齐）；新接收条目补满预算；去重超过上限后已发布页则不再续，未发布可丢缓冲有界从头重试且不再补预算；重试用尽或不可重试才结束列举）；**CIFS 自 C9 起逐页列举**（生产者持有目录句柄，`QUERY_DIRECTORY` 记录逐条取、按 `page_hint` 攒页交出；停车时句柄一直开着，所以预取受 `parked_listing_limit` = 8 约束；取消 / drop 在取下一条之前停、让在途记录到达，然后关句柄，最后一页在关句柄之后交出；首页之前句柄失效（smb-rs 重连后的 `InvalidState` / `StaleObject`、`STATUS_FILE_CLOSED` / `STATUS_INVALID_HANDLE`）重开目录从头读，至多 3 次；之后失败 → `Interrupted`）。

驱动循环 (`storage/blocks.rs` 的 `drive`)：

```
loop {
    if cancelled: return
    while arrivals.try_recv() → cursor.listed(id, arrival)   // 页 / End / Failed / Terminal
    step = cursor.advance(...)        // 尽可能多地准入
    cursor.start_listings(...)        // 起"游标马上需要的" + 预取
    state.output.flush()              // 按序号严格释放到 mpsc
    match step {
        Done       && 无在途观察 → return
        WindowFull && 无在途观察 → 继续    // flush 刚腾空窗口，必然能前进
        _ → select! { 取消 | 一个列举的下一页或结束 | 一个观察完成 }
    }
}
```

结束时观察任务 detach，丢掉 arrivals 接收端 —— **不 abort**：列举任务发现通道关闭后 drop 流，
backend 生产者走自己的收尾路径关句柄 (CIFS 目录句柄)。

`advance()` 单步：

```
loop {
    frame = stack.last() else return Done
    ⓪ closed → 消费一个 slot (next_after_block)：
         Pop → stack.pop()；Descend → stack.push(Frame::new(work))；Skip；Wait → WaitObservation
    ① 还没拿到列举 → take_top_listing()；未起 → return WaitListing (start_listings 会起它)
    ② 当前页还有子项 → 窗口满? WindowFull；admit(child)
    ③ 列举还有已到的页 → 取下一页 (credit +1)，该页描述不出的子项先按序排 Failure{Child}
    ④ 列举未结束 → return WaitListing
    ⑤ 列举已结束、块全部准入 → 窗口满? WindowFull
         Failed 且 NotFound 且非根且一个都没返回 → vanished，空块 Complete
         Failed 其他 → Failure{scope: Listing, delivered = 本块 listed}  (0 → Unreadable，>0 → Interrupted)
         queue_block_end()   // DirectoryListed(D) 在这里拿到序号
         closed = true；释放当前页队列的 room
}
```

**下钻排序**：槽位按到页顺序累积 (`Descend(work)` 或 deferred 的 `Pending{ticket, path, hint}`)，
列举结束 (`End` 或失败) 时按子目录名 (最后一段) 的原始 UTF-8 字节排序一次 (`sort_unstable_by`，不为千万子目录再分配半长缓冲；
同名只可能是列举重复返回同一目录，谁先都列同一路径)，O(d log d)，只排子目录。
中途失败的列举已返回的子目录照常下钻。块内子项**不排序**。

**下钻提示** (§12.7)：列举可给目录子项带一个 backend 私有、拷贝出来的 `DescentHint`
(`ListedChild::with_descent_hint`)，经 `Descend` 槽位的 `DirectoryWork.hint` / `Pending` 槽位的
`hint` 带到该子目录自己的 `ListRequest.descent_hint`。C4 建通道；**C7 起 NFS 填它**：子目录的文件句柄拷贝一份
(绝不是 READDIRPLUS 回复的切片)，子目录的列举直接拿它发 READDIRPLUS，不再每目录 LOOKUP，也不再把句柄写进
`GLOBAL_CACHE`；没有提示 (遍历根) 或提示失效 (`STALE` / `BADHANDLE` / `FHEXPIRED`) 才按路径 `lookup_fh`。
内存从"整棵树的目录数 × 约 625 B 的缓存项"变成"待处理目录数 × 句柄长度"。Local / HDFS / S3 不需要提示，不填。

**序号只在 cursor 里按这个顺序分配**；列举与观察可以任意次序完成，时间只决定*何时*能走下一步，
不决定*谁拿到哪个序号*。重排缓冲严格按序号释放 —— 这是确定性的全部来源。

预取 (`start_listings`)：栈顶需要的那个**一定起**，哪怕超预算 (否则远处的预取会饿死当前阻塞点)；
其余按输出顺序 (栈顶往下、深的优先) 选候选，等页的列举 ≤ P = `min(max_inflight, 64)`，
游标未到的已起列举 (等页的 + 停在首页之后的) ≤ 2P；扫描上限 4P；已起的候选被**展开**成它已返回的
子目录，所以预取能穿透多层 (块未关时按列举序展开，只影响先读谁，不影响输出)。

预取的含义是"首页到齐"：预取的列举交出首页 (外加至多一页原始的偷看页) 后停住，游标到了才继续读。
**祖先帧的列举在下钻前已读尽**，栈上没有横跨子树的打开游标；只有"已预取、游标还没走到"的目录会停在
半路 (≤ 2P 个)。backend 逐页之后 (C5–C9)：NFS / S3 / HDFS 停车什么都不占，Local 占一个 fd，
**CIFS 占一个真实句柄**：`Namespace::parked_listing_limit()` 对 CIFS 回答 8 (Q10)，游标对"已起、未读完"
的预取列举 (等页的 + 停车的) 数不超过它，游标正要的那个不计、总是起 —— 所以任何时刻打开的 CIFS
列举句柄 ≤ 8 + 1，且不会因上限卡住遍历 (C9)。HDFS (C8) 服务端什么都不占；客户端 hdfs-native 迭代器里
至多留一批 ≤ 1000 个 status。S3 多版本 (C11b 前) 生产者整目录收齐。
驻留探针 (`storage/residency.rs`) 有 `pages_buffered`、`parked_listings`。

### 三、`NameBytes` 兄弟排序 (P7 C4 已删除)

P4 的 `NameBytes` 把整块按名字排序后发出，块必须物化 (千万条约 4.5 GB)、首项要等整个目录列完。
P7 定为**只排下钻**：块内保持列举序，两侧逐目录对比时消费方按名字哈希匹配一个块 (内存 O(单目录))，
并且要等两侧都到了该目录的 `DirectoryListed` 才能判定"一侧缺失"(design §3.1)。仍然成立的：

- **消费者的键是 `(父目录, 名字)`，不是整条路径。** 块不交错 —— 一个目录的全部子项都在任何孙子之前，
  所以 `a/`(含 `a/x`) 与 `a.txt` 都在 `a/x` 之前。这不是深度优先前序；拿整条路径逐段比较会静默错位，
  读出来就是"目标端有而源端无"→ **删数据**。
- **下钻序是原始字节序，不是任何服务端 collation**。2026-09-21 FAS2750 实测：同一棵树 (`Zoo`/`a`/`B`/`bar`)
  在 CIFS 与 NFSv4.1 上原生列举序不同 (CIFS `Zoo, a, B, bar` / NFS `B, Zoo, a, bar`)，字节序
  `B < Zoo < a < bar` 在两侧一致。
- **陷阱**：ONTAP 的 NFS READDIR 在这棵树上恰好返回字节序，只测 NFS 会以为"不用排"。

### 四、单目录海量子目录：低内存模式

`Listing.slots` (cursor.rs) 为每个将要下钻的子目录留一条记录。根因是**物理的**：
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

- (P7 前的规划稿；`SubtreeComplete` / `child_order` 已在 P7 C2 删除。)
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

**`Blocks` 下逃生门 B 同样不成立**：下钻按名字排序，没看完整个列举就不知道先下哪个子目录。
所以海量子目录只能靠基线与溢写；无盘时该目录**必须拒绝而不是静默降级**。(`Unordered` 没有这个限制：
子目录一发现就入栈，栈上只有 `DirectoryWork`，与 legacy 的任务队列同量级。)


### 两种调度的取舍

| | `Unordered` (默认) | `Blocks` |
|---|---|---|
| 输出 | 目录 Entry 先于子树；跨目录无全局顺序 | 块内列举序；按子目录名字节序下钻 |
| 完成 | 会话 outcome + Listing failures | 另有每目录 DirectoryListed 与 BlockCounts |
| 计数 | ScanTotals | 相同 ScanTotals + 块计数 |
| 调度 | worker 栈 + 窃最浅，无头阻塞 | admission cursor，有头阻塞 |
| 背压 | 贯通协议翻页 | 贯通协议翻页 |

历史性能比较见 P7 §12.8，旧 scanner 的删除范围见 [迁移清单](../../docs/design/p7-legacy-cleanup.md)。

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

## 并发模型

遍历调度入口与取消约束见 [walk-scheduler.md](walk-scheduler.md)。删除进度由
`DeleteDirIterator` 接收，克隆共享事件，`next(&self)` 在通道关闭时返回 None。

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
- NFS：lookup 直接保留属性中的 ACL / owner / owner_group；列举观察按 ObservationPlan 决定。

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
  ├── filter.rs         ← DslTraversalFilter 适配到通用遍历
  ├── traversal/        ← Unordered / Blocks 调度与续扫
  ├── delete_progress.rs ← 删除事件接收器
  ├── tar_pack.rs       ← copy_dir 用
  ├── checksum.rs       ← copy_file 完整性校验
  ├── qos.rs            ← copy 流速率限制
  ├── time_util.rs      ← 所有时间转换中心
  ├── url_redact.rs     ← 所有 URL 日志中心
  ├── error.rs          ← 全库错误
  └── acl.rs            ← Unix/Win ACL 抽象 (local.rs 用)
```

**禁止反向依赖**：backend 模块不可 `use crate::storage_enum::StorageEnum`。
