# P7 流式列举：设计评审稿

> 状态：**待评审**（2026-09-25）。拍板后再写成 ADR 并实现。
> **以文末「定型版」为准**：它与前文冲突之处覆盖前文，前文的问题清单由定型版的 16 个问题取代。
> 完整 HTML 版在 `root@10.131.9.10:/root/p7-streaming-listing-design.html`。
> 来源：architect 两轮方案（主方案 + 针对需求 R1–R5 的增补），行号基于 `6c6ac22`。

## 背景

今天 5 个 `Namespace` 实现都先把整个目录收成 `Vec` 才交给遍历（`src/storage/roles.rs:485-499`，
`NamespaceResult::Entries`），遍历再整份转成 `Prepared`（`src/traversal/storage/cursor.rs:66-80`）。后果：

- 每个在途或已到达的列举都是 O(整个目录)，最多 2P = 128 份；1000 万条的平铺目录约 4.5 GB。
- S3 只能靠 500 万条上限自保（`src/storage/backends/s3/paging.rs:22-29`）。
- 目录全部列完才出第一条。
- 在 5337 万条的 NFS 树上，role-based 的 CPU 是 legacy 的 3 倍：
  legacy 998 s / 240 s CPU / 15 MB，role-based 1145 s / 724 s CPU / 178 MB（去掉 LOOKUP 后墙钟 1053 s）。

## 用户需求

| 编号 | 需求 |
|---|---|
| R1 | 无序流式列举，开销接近 legacy `walkdir` |
| R2 | 按目录给出排好序的子项，供 terrasync 双进程做源端 / 目的端差值比较 |
| R3 | 多版本对象的所有版本放进一个 item（用户原提议按 last-modified 排序） |
| R4 | 统计扫描总数、过滤掉的数量、过滤后的数量 |
| R5 | 各类扫描异常的合理设计：单条失败、目录失败、存储连接异常等 |

## 现有实现（含 legacy）

| 实现 | 方式 | 内存 | 顺序 | 完成 / 失败信息 |
|---|---|---|---|---|
| legacy `walkdir`（work-stealing，`src/walk_scheduler.rs`） | 每读到一页就逐条送进 channel | 很小（15 MB / 5337 万条） | 无序 | 无；失败页只记日志（NFS 在处理第一条前可从头重列，之后发 `ErrorEvent::Scan` 并返回 Ok，`src/nfs.rs:3389-3433`） |
| legacy `walkdir_2` / `ndx_walk` / `dir_tree` | 按目录出页，页内按名字排序（`src/nfs.rs` `finish_read_dir`） | 每目录整块 | 目录内有序 | 以页为单位 |
| role-based `StorageTraversalSource` | 深度优先 + 有界预读，每目录整块 | O(最宽目录) × 在途数 | Admission（无序）或 `NameBytes`（名字字节序） | `DirectoryListed` / `SubtreeComplete` + `is_exhaustive()`，失败有类型 |

已经修掉的 NFS 问题：每目录多一次 LOOKUP（`8a1348b`），无条件排序（`5e9cc39`）。
仍在的问题：每个子项一次跨线程交接。`admit` 给每个子项都 `tasks.spawn`（`src/traversal/storage.rs:422-425`），
哪怕观察计划根本不需要 I/O，每条约 1.9 次 futex，legacy 是 0.3 次。

## 方案

### 1. 流式到每一层

**Namespace API**（`src/storage/roles.rs:472-555`，只列数据结构）：

```
ListRequest   { path, versions: Current|All, cancel: CancellationToken, page_hint }
ListingPage   { entries: Vec<SourceDescriptor>, failures: Vec<EntryOperationFailure>, internal_hidden: u64 }
ListingStream  'static + Send 的页流，以 Ok-EOF 或一个 Err 结束
Namespace::list_stream(&self, ListRequest) -> ListingStream
```

- 默认实现把旧的 `execute(List)` 包成单页，所以约 17 个测试替身、`ndx_walk`、`delete_tree` 不用改。
- **机制 M2（推荐）**：每个列举一个生产者任务，外加容量为 1 页的 channel（辅助函数 `ListingStream::from_producer`）。
  - 背压：生产者阻塞在发送上，不会多发 READDIRPLUS / QUERY_DIRECTORY / ListObjects 请求。
  - **丢弃流就等于关闭（写进契约）**：接收端被丢弃后发送失败，生产者走 `close_resource` 退出，构造上保证不泄漏。
  - 解码和建 descriptor 留在工作线程上，不压在驱动任务上（node1 上 CPU 是瓶颈）。
  - 顺带解决 P5 遗留：取消令牌随 `ListRequest` 进入 S3 分页；`listings_are_abortable()` 可以删掉。

**各后端**：

| 后端 | 位置模型 | 实现 | 中途能否续读 |
|---|---|---|---|
| NFS | cookie + verifier（nfs-rs 0.8.4 私有） | 生产者持有 `Arc<mount>`，逐条读 `readdirplus`，每 N 条组一页 | 否，除非上游公开按 cookie 读一页的接口 |
| CIFS | 真实目录句柄 | 生产者持有 `Directory`，每个出口都 `close_directory` | 否 |
| S3 | continuation token / 版本 marker（无状态） | 一个请求就是一页；按 key 单调的水位去重（替换 O(N) 的 `HashSet`，`listing.rs:79-81`、`paging.rs:36`） | 是 |
| HDFS | startAfter（无状态） | 用 `list_status_iter` 替代 `list_status`（`src/hdfs/namespace.rs:284`） | 是 |
| Local | fd | 每页一次 `spawn_blocking`，带着 `ReadDir` 进出，挂起时不占阻塞线程 | 不需要 |

### 2. 遍历契约不变

- **块序不变**：父目录的流读完之前不下钻子目录。否则块会交错，按 `(父目录, 名字)` 做归并比较的一方会把"还没出现"读成"对侧没有"，进而删错数据。
- **可以提前的只有列举 I/O**：栈顶目录还在翻页时，已到达页里的子目录可以预取孙目录。输出顺序不变。
- G1（每个目录的 `DirectoryListed` 在它全部直接子项之后）、G2（祖先先列举）不变。G3 多一个"中断"状态（见第 4 节）。
- **内存上界**：驻留子项 ≤ (2P + 1) × 2 × 页。P = 64、页 1024 时最坏约 60 MB（现在是 128 份整目录）。
  下钻槽位（`Frame.slots`）仍随当前块的子目录数增长，要到 P8 / P9 才有上界；文档不能写"内存有界"。
- 绝大多数目录一页就到 EOF，行为与今天完全一样。

### 3. 排序与双进程比较（R2）

**R2 合理**：两边各自按名字有序给出，按目录归并比较，内存只与当前目录相关。
比较应当由两个 `NameBytes` 顺序的递归遍历按块归并完成，这个能力已经存在。
不建议让 terrasync 自己逐个目录调排序接口，那样它得自己重做预取、上界和完整性判断。
有序的单目录列举可以另外作为 `Namespace` 的便利接口提供，共用同一个排序器和比较器。

排序选项：

- **(a) 推荐**：Admission 模式流式；`NameBytes` 仍在列举任务里整块读完再排。上界由 P8 的溢写补上。
- (b) 利用后端原生顺序流式输出排序结果。**有坑**：
  - S3 的 key 序不等于名字序。例：key 序是 `a`、`a!`、`a.txt`、`a/`，名字序应是 文件 `a`、目录 `a`、`a!`、`a.txt`。
    修正需要扣住一个窗口，最坏无上界（`log/` 旁边有一千万个 `log.N`）。
  - HDFS 可能按**有符号**字节比较名字（未核实），非 ASCII 名字会排在 ASCII 前。
- (c) 外部归并排序加溢写，就是 P8。
- (K) 把 `NameBytes` 的键改成"目录按 `名字 + /` 比较"（即 S3 的 key 序）。这样 S3 天然有序、O(1) 重排；
  代价是破坏 P4 的键（FAS2750 证据要重跑），文件 `a` 和目录 `a` 不再相邻。

**比较的等值问题（必须先定）**：字节序是正确的**顺序**，但不是大小写不敏感或会做 Unicode 归一的目的端上的正确**等值**。
例：源端 `README`，CIFS 目的端 `readme`。字节序下两者不相邻，归并会报"源端独有 `README`"和"目的端独有 `readme`"，
删除后者会删掉刚拷过去的前者。选项：(i) 两边都按目的端的折叠规则排序；(ii) 源端逐块检测折叠后的冲突并报类型化冲突；
(iii) 对大小写不敏感的目的端禁止"按差值删除"。至少要有 (ii)。比较器（今天私有的 `sibling_key`，`cursor.rs:528`）应公开，
避免 terrasync 自己实现后产生偏差。

### 4. 过滤、失败位置、中途失败

- 立即决定与 `max_depth` 改为逐页做，语义不变。
- **顺带修**：列举已带 `modified` 时，过滤也还在推迟决定（`storage.rs:364-382`），DSL 主线上几乎所有子项都无谓延迟。
  改为有内联 `modified` 就立即决定；推迟路径取的也是同一个内联值，结果不变。
- 目录级的逐子项失败今天排在块首；流式下出现在失败发生的那一页的位置（Admission 模式）。
- **列到一半失败**（已输出 N 条后第 k 页失败）：新增 `DirectoryListing::Interrupted { delivered }` 和
  `SubtreeSummary.interrupted_listings`，纳入 `is_exhaustive()`。优先级：Failed > Interrupted > Partial > Filtered > Complete。
- **中途重试不重复**：S3 / HDFS 用同一个 token / startAfter 重发，精确不重复；NFS 只在第一页发出前允许从头重列，
  之后失败即 Interrupted（这是行为变化：今天读到第 90 万条遇到陈旧句柄会静默重列）；CIFS 无法续读，即 Interrupted。
  **绝不"重列后跳过 N 条"**：目录可能已变，会静默丢或重，读起来就是删除。

### 5. 扫描异常（R5）

| 层级 | 例子 | 处理 | 映射 |
|---|---|---|---|
| 单个条目 | 缺属性、单个子项 stat / 权限失败、非法名字、列举后被删 | 输出单条失败项，目录 Partial，遍历继续 | `Entry(EntryOperationFailure{Traverse/Observe, NotFound/PermissionDenied/Unsupported/Protocol, Permanent})` |
| 单个目录 | 无权限、扫描中被删、页中途失败、token 循环、跨页重复 | 首条前失败 → Failed；之后 → Interrupted；兄弟目录继续；瞬时错误在生产者内有限退避（D3 允许的后端内短重试，需文档化） | Entry 级失败 |
| 连接 / 会话 | 连接重置、服务端宕机、Kerberos / SMB 会话过期、S3 凭据过期 | 今天直接终止遍历。透明重连应放在后端会话层（nfs-rs Remount、smb-rs 重连、SDK 凭据刷新），不在 P7；P7 保证终止前已交付的 `SubtreeComplete` 足以让调用方知道哪些子树已完成 | `Session(BackendSessionFailure{Connectivity/Authentication})` |
| 限流 | S3 SlowDown / 503、NFS JUKEBOX / DELAY | 列举页在生产者内有限退避；耗尽才让该目录 Interrupted，而不是结束会话；跨列举连续 N 次失败才升级为会话失败 | — |
| 资源上限 | 单目录超上限、单 key 版本过多 | 流式模式去掉目录条目上限；`NameBytes` 在 P8 前保留物化上限并报 `Capacity`；可选的"失败超阈值即终止"策略需你决定 | `Capacity` |
| 取消 | 用户取消 | 信号而非错误（R10）；生产者退出并关闭 | `Cancelled` |

**现状里的三个问题（无论 P7 做不做都该修）**：
1. **一次 S3 SlowDown 就可能结束整个遍历**：限流被归为会话失败（`src/s3/role_protocol.rs:518-523`），且没有配置重试策略。
2. **CIFS 和 NFS 因一个坏子项让整个目录列举失败**：`collect::<Result<Vec>>()?`（`cifs/namespace.rs:92`、`nfs/namespace.rs:83-85`，
   NFS 缺属性时 `nfs.rs:4657-4659`），违反"逐子项失败逐子项报"的契约。
3. **token 循环被当成会话失败**（`paging.rs:69-71`），它只是一个目录的问题，应为 Entry / Protocol / Permanent。

不需要新增 `StorageError` 变体，全部映射到现有 `FailureClass` / `Transience`。

### 6. 计数（R4）

`DirectoryListed` 与 `SubtreeSummary` 新增字段（两者都是 `non_exhaustive`，新增不破坏）：

| 字段 | 含义 |
|---|---|
| `scanned_entries` | 存储返回、遍历看到的每个子项（含描述失败的） |
| `filtered_entries` | 被过滤隐藏的条目（今天已算出 `BlockFacts.hidden`，只是没暴露，`output.rs:41,73`） |
| `child_failures` | 逐子项失败（与会把列举失败也计入的 `entry_failures` 区分） |
| `internal_entries` | `.data-mover-*` 内部文件（由 `ListingPage.internal_hidden` 报告；今天各后端在遍历看到前就丢了） |
| `emitted_bytes` | 输出文件的总字节数 |
| `files` / `directories` / `symlinks` / `other` | 按类型计数 |

- **不变式**（测试和 debug 构建断言）：`scanned = observed + filtered + child_failures`；internal 不计入 scanned；
  pruned / truncated 与之正交。
- 总数取根目录的 `SubtreeComplete`。代价：驱动任务上的普通 u64，无原子操作、无逐条分配，无序模式同样如此。

### 7. 性能与无序模式（R1）

**先于 P7 单独做、单独量（Phase A）**：

- **A1**：观察不需要 I/O 时（非符号链接、观察计划可用内联信息满足），在列举任务里按页组装 `ObservedEntry` 并直接落定，
  不再每条 spawn。交接从每条一次变成每页一次。验收：`d019/d000` 上每条 futex 从 1.9 降到 0.5 以下。
- **A2**：有内联 `modified` 就立即决定（见第 4 节）。
- **A3**：HDFS 去掉每个目录列举前的 `get_metadata`（`src/hdfs/namespace.rs:275`），与旧的 NFS LOOKUP 同类。

**无序模式**：
- (i) **推荐**：保持块序，先量 A1 + 按页批处理之后的数据。墙钟差距已只有约 5%，这棵树上的瓶颈不是块序。
- (ii) 新增 `TraversalOrder::Unordered`：块可以交错，跨目录不再排序号；`DirectoryListed(D)` 在 D 最后一页之后；
  `SubtreeComplete` 用未完成后代计数。G1、G2 仍成立。必须是新变体，因为 Admission 的文档承诺了同目录连续。

### 8. 多版本对象（R3）

- **按 last-modified 排序不可取**：C22 已证明服务端列举顺序才是权威顺序。时间可能跨节点偏差、同毫秒并列、缺失，
  最新版本不一定时间最大（`history.rs:1-12`）。
- **一个 item 装全部版本的问题**：
  - 单 key 版本数无上界，而 item channel 只按条数有界、不按字节，会打破内存上界；
  - 每个版本已有自己的 `ObservedEntry`（身份键、v6 快照、绑定到该版本的元数据、`SourceVersion::Id`），
    打包后某个版本观察失败就只能丢掉整个 item 或产生残缺 item，失去逐条失败的粒度；
  - 它能换来的只是：`NameBytes` 归并键可以不带版本序号。
- **推荐**：保持每个版本一个 item（今天已保证同一 key 连续、由旧到新），在 `EntryVersion` 上加 `{ position, count }` 作为
  明确的"该 key 结束"信号（不依赖 `is_latest`，最新版本观察失败时它会丢失）；每 key 版本数设上限，超了只让该 key
  以逐子项 `Capacity` 失败。

## 提交计划（概要）

1. **Phase A（性能，不改契约）**：A1、A2、A3，各自一个 commit，在 node1 上单独测量。
2. **Phase B（计数，只增字段）**：新计数字段、各后端报告 `internal_hidden`。
3. **Phase C（流式）**：`ListingStream` 类型与默认适配 → 游标按页消费（默认单页，行为逐项不变）→ Interrupted 语义 →
   Local → S3 → NFS → HDFS → CIFS → 文档。
4. 不涉及 `StorageEnum`，五处同步规则不适用；legacy walker 在 P6 前保持不变。

## 主要风险

- **高：块序回退 = 静默删数据**。流读完前关块或下钻。缓解：注入验证（提前关块必须让测试变红）。
- **高：CIFS 句柄生命周期**。流式和挂起的预取让 ONTAP 上的目录句柄开得更久、更多，漏关即 S99 重演。
  缓解：生产者持有句柄、每个出口都关、挂起数上限、FAS2750 压测并用 REST 查打开文件数回到 0。
- **高：中途重试产生重复或丢失**。缓解：第一页之后绝不重列。
- **高：大小写不敏感目的端的比较等值**（第 3 节）。缓解：D6 在"按差值删除"上线前定下来，FAS2750 上做大小写混合夹具。
- 中：S3 各家存储跨页顺序或前缀重复；nfs-rs 若要公开 cookie 接口需升级依赖；限流重分类可能掩盖端点宕机。
- 低：小目录的每页开销；Local 每页 `spawn_blocking` 的开销。

## 验证

- 每一步 `make ci`（均带 timeout）。
- 注入验证（每条必须变红）：提前关块、首条后重列、丢弃生产者不关句柄、漏报 `internal_hidden`、把 Interrupted 判成 Complete。
- 真机：
  - node1 + m1-source 5337 万条，c=64：Phase A 后、P7 后各与 legacy 交替 4 轮，条目集逐条相同、`exhaustive=true`、
    计数不变式成立，记录墙钟 / CPU / RSS / 每条 futex。
  - NFS 平铺 100 万文件目录（`/test-chark/data-mover-p7-<ts>/`，用完删除）：首条时间与 RSS。
  - FAS2750 CIFS：5 万条目录在 c=1/4/32 下逐行一致；挂起句柄压测；大小写混合树与 NFS 的 NameBytes 输出一致。
  - MinIO：100 万对象平铺前缀的 RSS 与首条时间；版本数超过一页的 key。

## 补充（architect 第三轮，2026-09-25）

第三轮细化出 27 个问题（完整版在会话 scratchpad 的 `p7-streaming-listing-design.html`），与上面 D1–D14 基本对应。
新增的要点：

- **178 MB 常驻内存至今没解释清楚**：m1-source 每个目录只有约 35 项，帧里持有的东西应只有几 MB。建议 Phase A 之前先在
  node1 上用驻留探针跑一遍，并对照 `MALLOC_ARENA_MAX=2`（怀疑 glibc 多 arena）。
- **排序键改为 git tree 序**（目录按 `名字 + /` 比较，即 D5 的 (K)）：S3 原生键序因此天然有序、可真正流式；代价是推翻
  P4 已推送的定义、FAS2750 证据要重跑。
- **HDFS 与 Local 的列举同样会因一个坏子项整目录失败**：HDFS 也有 `collect::<Result<Vec<_>>>()?`，与 CIFS / NFS 同类。
- **多版本的顺序**：真正需要你定的是由旧到新（现状，latest 最后）还是由新到旧（与服务端列举顺序一致、无需扣留）。
- **双进程比较的拓扑**：两个遍历并行（双活）归并，还是两进程逐目录列举后比较 —— 建议前者（见第 3 节）。
- **会话失败后的续传**：调用方凭已收到的 `SubtreeComplete` 跳过已完成子树重扫，是否需要库提供更显式的续传点。

## 需要你拍板的问题

| # | 问题 | 推荐 |
|---|---|---|
| D1 | 无序模式：先量再说，还是现在就加 `TraversalOrder::Unordered` | 先做 Phase A 再量 |
| D2 | 流式机制 M2（生产者 + 1 页 channel）；顺带把 `list_versions` / `listings_are_abortable` 并入 `ListRequest`（破坏性） | M2，并入 |
| D3 | 列到一半失败：新增 `Interrupted`，还是复用 `Failed` | 新增 |
| D4 | Admission 模式下目录级失败项的位置：块内任意位置，还是块尾 | 块内任意位置（`NameBytes` 仍在块首） |
| D5 | `NameBytes` 的键：保留 P4 的键，还是改为 key 序（目录按 `名字 + /`）让 S3 原生有序 | 保留；P8 前再决定 |
| D6 | 对大小写不敏感 / 做 Unicode 归一的目的端，比较等值怎么定：折叠排序 / 源端冲突检测 / 禁止按差值删除 | 至少源端冲突检测 |
| D7 | CIFS 预取后挂起的句柄：上限（如 8）、读到 EOF、还是大目录不预取 | 上限 8，先在 FAS2750 实测 |
| D8 | NFS 中途续读：给 nfs-rs 加公开的按 cookie 读页接口（要升级依赖），还是接受"首页之后失败即中断" | 先接受，上游另开 issue |
| D9 | 多版本：每版本一个 item + `position/count`，还是一个 item 装全部 | 每版本一个 item + `position/count` |
| D10 | 计数字段的集合与命名；会话级完成信息是否也带 `scanned_entries` | 按第 6 节 |
| D11 | S3 列举中的限流：退避后让该目录 Interrupted 而不是结束会话；连续多少次失败升级为会话失败 | 是；阈值待定 |
| D12 | `TraversalRequest` 是否加"失败超阈值即终止"选项（新公开 API） | 待定 |
| D13 | 页大小：内部常量 1024，还是公开成请求字段 | 内部常量 |
| D14 | S3 的 500 万上限：流式模式去掉，`NameBytes` 在 P8 前保留为物化上限 | 是 |

## 定型版（2026-09-25，覆盖前文；完整见 HTML D1–D14）

**两种扫描方式**：

```
enum TraversalOrder {
    Unordered,  // 无序 DFS：列到一页就发一页，不同目录的项可交错；默认
    Blocks,     // 块序 DFS：一个目录的直接子项连续成块，块内按列举顺序不排序；
                //           块读完后按子目录名的原始 UTF-8 字节序深度优先下钻
}
```

- 遍历里不再整块读进内存：文件一律随页流出，块序只需记住子目录（今天本来就有这份记录）。
- 删除兄弟排序：P4 的 `sort_by_name` / `sibling_key`、`ChildOrder`、`DirectoryListed.child_order`；物化上限、S3 原生有序、
  HDFS 有符号字节序、git tree 排序键这些问题随之消失。P8 只剩「子目录返回点写盘 / 名字 arena」。
- 两进程比较：两侧都用 `Blocks`，下钻顺序一致，按目录同步推进；块内文件由消费方按名字哈希匹配（内存与单目录大小成正比）。

**事件模型**：

```
enum TraversalItem {
    Entry(Box<ObservedEntry>),
    Failure(ScanFailure),             // 两种模式都有；scope: Child（只是这个子项）| Listing（这个目录没列完，带 delivered）
    DirectoryListed(DirectoryListed), // 只有 Blocks 发：每个被列举的目录（含空目录）在最后一个子项之后
}
struct DirectoryListed { path, outcome: Complete | Interrupted | Unreadable, counts }
TraversalCompletion { totals }        // 两种模式都有全局总数
```

- `SubtreeComplete` 两种模式都删；`Unordered` 不发任何目录事件，只用全局原子计数（结构近似 legacy）。
- **删除安全规则（写进契约）**：只有会话以 `Completed` 结束才做删除检测；出现 `scope == Listing` 失败的目录整棵旧子树
  不参与删除；`scope == Child` 失败的条目本身不参与删除；用了 filter / `max_depth` 时旧代条目按同样条件判定。
- 计数：`listed = emitted + filtered + failed + vanished`；列举后才消失的条目计入 `vanished`，不发失败项。

**断点续扫（流内检查点，两种模式，D15 更正 D14）**：遍历按间隔（如每 10 万项或 10 秒）在流里插入 `Checkpoint` 项
（带版本号的不透明字节），约定其之前的所有项已覆盖它记下的进度；调用方把检查点与之前的项在同一事务里提交，续扫时传
`TraversalRequest.resume_from(checkpoint)`；检查点之后的项会重发一次（调用方按路径 upsert）；取消与会话失败同一套逻辑
（取消即暂停）。D14「失败时返回游标」有漏洞：通道里已发出但调用方未存下的项会丢失，续扫点必须与调用方已提交的内容对齐。
- Blocks：检查点记深度优先位置（路径 + 每层下一个要进的子目录名，几百字节）；续扫重列各级父目录但不重发，没列完的目录整个重列。
- Unordered：检查点记前沿（已发现未列完的目录及其深度 / filter 状态；5337 万条的树约 6400 条路径、约 300 KB）；
  取检查点时短暂停止启动新列举；正在列的目录整个重列并重发，已扫完的子目录可能再扫一遍；前沿大小与待处理目录数成正比
  （单目录 1000 万子目录会让前沿变 1000 万条，待 P8 / P9）；删除检测要合并前后两次运行的 Listing 失败，且最后一次以
  `Completed` 结束。
- 性能（推算，未实测）：检查点做成请求选项 `checkpoint: Option<CheckpointPolicy>`，默认关，关时零开销；开启时每个**目录**
  登记 / 注销各一次（5337 万条的树约 670 万次，约 1 s CPU，占总 CPU < 0.5%），每 10 s 复制一次前沿（约 1 ms）；不需要暂停
  所有 worker（弹栈与登记在同一把 worker 锁内、偷任务先登记后拿走，最坏某目录被记两次、续扫多列一次）。只有单目录海量
  子目录时检查点变贵（1000 万子目录约 0.5–1 GB），用自适应间隔，或停发检查点并在流里明确告知「此后不能续扫」。

**错误**：新增 `FailureClass::Throttled`（单个列举累计退避约 2 分钟后报 `scope == Listing`）；CIFS / NFS / HDFS 单个子项失败
只报该项；列举中被修改的目录可能重复或漏项，写进契约由消费方按（父目录, 名字）去重（S3 在遍历内去重）。

**多版本**：每版本一项，服务端顺序（最旧在前），同一 key 连续；库里提供按 key 分组的适配器；单 key 上限 10 万，超过只让该 key
报 `Capacity`。

**精确统计（D16，覆盖 D15 的「重发靠 upsert 去重」）**：「刀前永远算数，刀后续扫时作废重来」。
1. 遍历在流里插 `Checkpoint(k)`（下一刀），保证续扫恰好重发刀后全部内容：刀前的一个不重发，刀后的一个不漏。
   - Blocks 只在 `DirectoryListed` 之后下刀，检查点记深度优先位置。
   - Unordered 检查点记「未开始列的目录」和「列到一半的目录在刀前已列出的名字」（含被过滤的与 `.data-mover-*`，计数也不重复），
     续扫重列并跳过这些名字；S3 / HDFS 用 `StartAfter` / `startAfter` 续读，不存名单；刀上正列着千万级目录时推迟下刀。
2. 消费方给两刀之间写入的行打 `epoch = k`；收到 `Checkpoint(k)` 先等其前所有批写库成功再存检查点（不需要事务；两步之间
   崩溃就回到更早一刀，同样精确）。
3. 续扫时 `DELETE ... WHERE epoch >= k`（最多一个检查点间隔的行），内存统计恢复到刀 k 的值，遍历从刀 k 继续。
4. 结束时交叉校验：遍历报告的发出数必须等于临时表去重后行数，不等就不提交 generation。
- 计数分工：只有遍历看得见的计数（列出、被过滤、`.data-mover-*`、消失、列举失败）由 data-mover 按刀精确维护，放在
  `TraversalCompletion`；分布统计（按类型 / 大小 / 时间区间等）留在 terrasync，最终用 SQL 在临时表上聚合；时间分桶基准在
  扫描开始时固定并写入检查点。
- 「精确」指精确等于本次扫描实际看到的内容，不是存储某一瞬间的快照。检查点默认关，开启时推算开销 < 1%。
- terrasync 现状（10.131.9.10，`8edd400`，只读查看）：`FullStats` 只在内存、重发即重复计数；临时表是普通 `MergeTree` 不去重；
  ClickHouse 无跨语句事务；批量写库并发、批间无屏障；`TimeRange` 按「现在」分桶，续扫后同一文件会落进不同的桶。

### 待拍板（建议）

| # | 问题 | 建议 |
|---|---|---|
| 1 | 默认扫描方式 | `Unordered` |
| 2 | `SubtreeComplete` 两种模式都删 | 删 |
| 3 | 计数放置：Blocks 的 `DirectoryListed` 带本目录计数，两种模式 `TraversalCompletion` 带全局总数 | 同意 |
| 4 | data-mover 是否提供逐目录配对辅助（两侧都发 `DirectoryListed` 后才判定一侧缺失） | 提供 |
| 5 | 采用「下一刀 + 刀后回滚」的精确续扫模型（两种模式都支持，检查点默认关、terrasync 扫描时打开） | 同意 |
| 5a | 分布统计的最终来源：结束时用 SQL 聚合，还是内存累加器随检查点保存、续扫时恢复 | SQL 聚合 |
| 5b | 分工：遍历看得见的计数由 data-mover 精确维护，分布统计（分桶边界、扩展名大小写等业务口径）留在 terrasync | 同意 |
| 5c | terrasync 需改三处（临时表加 `epoch` 列并改 `ReplacingMergeTree` 兜底、下刀前等所有写库完成、时间分桶改用扫描开始时间），只写进设计、不动 terrasync 代码 | 是 |
| 6 | Blocks 下钻按原始 UTF-8 字节，不做大小写折叠与 Unicode 规范化 | 是 |
| 7 | 大小写 / 规范化不敏感的目标：公开名字语义，折叠后相同、字节不同时报冲突不删除 | 是 |
| 8 | 多版本形状与单 key 上限 | 每版本一项 + 分组适配器，10 万 |
| 9 | CIFS 预取同时打开的目录句柄上限（先在 FAS2750 测 ONTAP 空闲句柄回收） | 8 |
| 10 | 节流：`FailureClass::Throttled`，单列举累计退避约 2 分钟后报 Listing 失败 | 是 |
| 11 | 列举后消失的条目只计 `vanished`，不发失败项 | 是 |
| 12 | 修改中的目录可能重复 / 漏项，写进契约由消费方去重 | 是 |
| 13 | 改 nfs-rs 上游公开 READDIRPLUS 分页接口，NFS 可从断点续列 | 做，单独 `chore(deps)` |
| 14 | 先做 C0（去掉每条目 spawn）并在 node1 补测基线（`MALLOC_ARENA_MAX=2`、驻留探针） | 是 |
| 15 | `Unordered` 验收门槛（node1、5337 万条、c=64、与 legacy 交替四轮） | CPU ≤ 1.3× legacy、RSS ≤ 50 MB、耗时 ≤ legacy + 5% |
| 16 | 写 ADR-0008（两种扫描方式 + 事件模型） | 写 |
