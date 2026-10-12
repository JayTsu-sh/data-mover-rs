# 扫描（遍历）P7 设计：流式列举、两种扫描方式、事件、计数、错误与续扫

> **状态：P7 实现与验收已完成**（2026-10-12）。C11b / C12、legacy 清理、C7c 首页前续读与 Q4 配对辅助均已完成；CI、可用环境实测、MinIO 十万版本与扫描续跑通过。DXN-2、StorageGRID 的环境限制保留在 §14.3；最终 CI、NFS 与修复后的 HDFS 验收见 §14.4。第 15 节保留 2026-09-25 的决定记录，ADR-0008 已完成。
>
> - 本文件**取代**此前的评审稿（Q1–Q30 问题清单、“待拍板”）。评审稿保留在 git 历史里，最后一个评审稿 commit 是 `fef8f2a`。
> - 权威 HTML 副本在 `root@10.131.9.10:/root/p7-design/p7-streaming-listing-design.html`（本文件据其 sha256 前缀 `4e16e7c4` 的版本转写）；
>   讨论过程稿在同目录 `p7-streaming-listing-design-draft.html`。两者不一致时以 HTML 为准，并同步本文件。
> - 原稿元信息：分支 `implement/storage-transfer-architecture` · 基线 `6c6ac22` · terrasync-rs 对照 `8edd400` · 2026-09-25 ·
>   只有设计，没有实现。本版按讨论中的全部结论重新组织；DXN / StorageGRID 版本顺序实测已完成（10.2.1）；性能目标见 12.6。
> - 与原稿相比，本文件只多了三处：各处对已修问题的状态标注、文末“设计过程中发现的现存问题”（沿用评审稿编号，更新了状态）、
>   文末“收尾：清理扫描相关旧代码”。另外 11.3 / 11.4、12.5 / 12.6 按编号排序（原稿里是倒过来的），原稿对用户的称呼“你”改为“用户”。

## 目录

1. 目标与结论摘要
2. 现状与实测
3. 两种扫描方式（3.2 扫描入口与参数）
4. 事件模型（4.1 消费方的删除安全规则）
5. 流式列举：Namespace 接口与各 backend（5.4 接口逐项说明）
6. 调度与内存
7. 计数
8. 错误模型
9. 断点续扫（9.1 接口与检查点时机，9.3 C11 的实现）
10. 多版本对象：每个 key 一项（10.2 版本顺序实测）
11. 与 terrasync 写库的配合（ClickHouse）
12. 性能（12.5 按页并发、12.6 性能目标与 O1–O6、12.8 C10 的性能门实测）
13. 破坏性变更清单
14. 实施计划（PLAN / CHANGE / CREATE / RISK / VERIFY）
15. 决定记录（全部采纳）
16. 附录：否决的方案与理由

附：设计过程中发现的现存问题（状态）；收尾：清理扫描相关旧代码

## 1. 目标与结论摘要

**目标**：扫描时列举一页一页地流进遍历，不再把整个目录读进内存；提供两种扫描方式，其中无序方式的成本接近 legacy `walkdir`，同时保留 legacy 没有的东西：typed 失败、完整性判定（删除检测的前提）、准确的计数，以及可选的断点续扫。

**原则**（用户定的）：不考虑兼容性，怎么合理怎么设计，现有结构需要改就改。

### 已经定下的

| # | 决定 | 一句话理由 |
|---|---|---|
| 1 | 扫描只有两种方式：**Unordered**（无序 DFS）与 **Blocks**（块序 DFS：一个目录的直接子项连续成块，块内按列举顺序、不排序；块读完后按子目录名字节序深度优先下钻） | 列举场景只有这两种；Blocks 的内存和 CPU 与今天的 `Admission` 基本相同，只多一次子目录排序 |
| 2 | 不做兄弟排序（删除 P4 的 `NameBytes` 兄弟排序与 `ChildOrder`） | 全排要整块物化 + O(N log N)；两进程对比的文件匹配由消费方按名字做 |
| 3 | `DirectoryListed` 只在 Blocks 里发；Unordered 不发任何正常路径的目录事件 | 块边界只在 Blocks 有用；Unordered 省掉计数树和每目录两项 |
| 4 | Namespace 只有一个流式列举入口 `list(ListRequest) -> ListingStream`，逐页返回，页里只带名字 | 两种方式共用；页形状为 P9（路径不重复存前缀）预留 |
| 5 | 断点续扫是扫描入参（`resumable` / `resume_from`），默认关，由上层决定 | data-mover 不决定策略；关闭时零开销 |
| 6 | 续扫**不求精确**：从最近一个已落库的检查点重来，重扫部分靠库去重；唯一硬性要求是“进度不领先于已落库的数据” | 重复扫描代价小；真正的问题是 ClickHouse 批没写下去而进度前进了 |
| 7 | terrasync 的临时表不用改 | 每批一张新临时表、比较后并入 `ReplacingMergeTree` 的 base，跨批重复天然被吸收 |
| 8 | S3 列举**不假设有序** | 有真实的无序实现：**ONTAP S3 的 NAS 桶**按目录遍历序返回（NetApp KB CONTAP-675913），AWS directory bucket 也不保证有序。“ONTAP 配重删 / 压缩就无序”是传闻，实测不成立（Q22，2026-09-26） |
| 10 | 检查点默认间隔 **1 分钟**（Q14） | 用户定的 |
| 13 | `should_scan` 只放需要额外接口调用的项（`Acl / Xattrs / Tags / SymlinkTarget`），是一个集合、没有档位；列举顺带的信息一律收下（Q31） | 用户定的 |
| 11 | `rename` 保留在 Namespace（Q23） | 用户定的 |
| 12 | terrasync 写库的改动在 terrasync 里做，本设计只列要求（Q16） | 用户确认 |
| 9 | **没有 `versions` 参数**：多版本桶上每个历史版本和删除标记都要列出来；一个对象的全部历史（含删除标记）是**每个 key 一项**，不是每个版本一项 | 用户定的；历史是对象的属性，不是另一种扫描方式 |

### 决定记录与仍待处理的事项

全部问题的最终决定见 第 15 节。目前没有待用户决定的问题。

## 2. 现状与实测

> 本节是 **P7 实施前的历史基线**；其中“现状 / 今天 / 未修”和旧接口、性能数字描述当时的代码，
> 不代表当前实现。保留这些记录用于对照；当前完成情况与验收边界见 §14.3 / §14.4。

### 2.1 今天的遍历怎么工作

- **一种调度**：准入游标（`src/traversal/storage/cursor.rs`），块序 DFS。一个目录的全部直接子项连续输出（`cursor.rs:1-18`）；块完全准入后才下钻（`cursor.rs:297-303`）；
  下钻顺序 = 子目录在块中出现的顺序（`cursor.rs:483-497, 375-393`）。`Admission` 用列举顺序，`NameBytes` 把块排序（`cursor.rs:507-534`）。
- **每个目录整块读进内存**：所有 backend 的 `List` 都返回整个目录的 `Vec<SourceDescriptor>`（`src/storage/roles.rs:474-521`）。
  在飞列举 ≤ P = min(max_inflight, 64)，已到未取 ≤ 2P（`cursor.rs:228-237`），每份都是整个目录，大小没有上限。
- **每个子项一个任务**：`admit()` 对每个子项 `tasks.spawn`（`src/traversal/storage.rs:422`）。默认观察计划下这个任务不做任何 I/O（`observe.rs:17-28, 95-97`），
  只是把描述转成 `ObservedEntry`，然后 `join_next` 再唤醒驱动。
- **事件**：每个被列举目录发 `DirectoryListed` 与 `SubtreeComplete`，在输出侧按序汇总（`output.rs`）。

### 2.2 实测（node1，m1-source NFS，53,368,420 条 / 3,368,421 目录，c=64）

|  | wall | CPU (user+sys) | RSS | futex / 条 |
|---|---|---|---|---|
| legacy `walkdir` | 998 s | 240 s | 15 MB | 0.30 |
| 角色版（`8a1348b` 之前） | 1145 s | 724 s（3.0×） | 178 MB（11.9×） | 1.9 |
| 角色版（`8a1348b` 之后） | 1053 s | 未重测 | 未重测 | 未重测 |

- 已修：每目录多一次 LOOKUP（`8a1348b`，从 READDIRPLUS 回填目录句柄，`nfs.rs:4661-4669`）；无条件排序（`5e9cc39`）。
- 未修：每条目一次跨线程交接（上面的 spawn），这是 CPU 差距的主因。
- **178 MB 没解释清楚**：驻留探针在小树上说帧只持有 O(最宽单目录 + 2P)，这棵树每目录才 35 项。首要怀疑 glibc 多 arena（每条目的任务在 32 个线程间分配 / 释放）。

### 2.3 各 backend 今天的列举

| backend | 今天 | 位置模型（跨页） | 值得注意的问题 |
|---|---|---|---|
| Local | `spawn_blocking` 读完整目录（`local/namespace.rs:140-153, 257-297`） | fd，无超时 | 列举级错误与子项错误都记在目录路径上（`:271-275`），分不清 |
| NFS | 收完整个 `readdirplus` 流；出错整目录从头重试（`nfs.rs:4610-4691`） | cookie + verifier，无状态；但 nfs-rs 0.8.4 不公开页游标（`mount.rs:28` `pub(crate)`；0.8.5 起公开 `Mount::readdirplus_page`，C7c 已用上） | 一个子项缺属性 → 整目录失败（`nfs.rs:4657-4659`）；一个子项描述失败 → 整目录失败（`backends/nfs/namespace.rs:75-86`） |
| CIFS | open → `entries("*").try_collect()` → close（`cifs/protocol.rs:230-246`）；**C9 `936e20b` 起逐页流式** | **真实目录句柄**（CREATE 出来的） | 一个子项失败 → 整目录失败（`cifs/namespace.rs:92`） |
| S3 | 收齐所有页再建块，500 万上限（`s3/listing.rs:55-130`，`paging.rs:24-29`） | continuation token，无状态 | 对象去重用 O(N) HashSet（`listing.rs:79-81`）；token 重复 → 会话失败（`paging.rs:83-86`）；429/503/SlowDown → 会话失败（`src/s3/role_protocol.rs:518-523`） |
| HDFS | `get_metadata` + `list_status` 全量（`src/hdfs/namespace.rs:271-288`） | startAfter，无状态 | 每目录多一次 RPC（`:275`）；一个子项失败 → 整目录失败（`backends/hdfs/namespace.rs:44-52`） |

## 3. 两种扫描方式

```
enum TraversalOrder {
    Unordered,   // 无序 DFS
    Blocks,      // 块序 DFS：块内列举序；块读完后按子目录名字节序深度优先下钻
}
```

|  | Unordered | Blocks |
|---|---|---|
| 输出 | 页到即发；不同目录的项可以交错 | 一个目录的直接子项连续成块；块内是 backend 列举序 |
| 下钻 | 子目录一发现就入 worker 栈 | 块读完后，按子目录名的原始 UTF-8 字节排序（O(d log d)，只排子目录），依次深度优先 |
| 目录事件 | 无 | 每个被列举目录一个 `DirectoryListed`，在块的最后一项之后，**空目录也发** |
| 确定性 | 无 | 下钻顺序跨 backend 一致；块内顺序取决于 backend |
| 完整性 | 会话 `Completed` + 失败清单 | 同左，另有每目录 `DirectoryListed.outcome` |
| 续扫检查点 | 前沿 + 正在列的目录 | 块边界上的 DFS 位置 |
| 典型用途 | 扫描写库、拷贝 | 需要逐目录边界、两侧按目录同步推进的对比 |

### 3.1 为什么 Blocks 下钻排序、块内不排序

块序本来就要在块读完之前记住这个目录的全部子目录（槽位要等块关闭才开始消费）。所以只排下钻顺序几乎不花钱：内存与今天的 `Admission` 相同，多一次 O(d log d)。换来的是“各 backend 访问目录的顺序一致”，
两侧按目录同步推进时漂移可界。块内全排则要整块物化（1000 万文件约 4.5 GB）并且首项要等整个目录列完。

代价：块内文件无序，两进程逐文件对比时，消费方按名字做哈希匹配（内存 O(单目录)），并且要等两侧都到了该目录的 `DirectoryListed` 才能判定“一侧缺失”。

**Q4 已实现**（`46a8a4c`）：`traversal::pair_directory_blocks` 接收两侧各一个以 `DirectoryListed` 关闭的块，
通过 `BlockPairingOptions` 显式映射两侧存储目录到同一个逻辑目录。辅助函数按原始名字哈希配对，不要求块内排序、不做 I/O；
结果借用原始观察，保留同名不同类型、目录自己的尾斜杠对象及每个 key 的全部历史分段。重复的同名同类型条目标为冲突，
不覆盖；历史段必须从 0 连续到末段，路径、边界和计数不一致则拒绝输入。

`OnlyLeft` / `OnlyRight` 只表示这两个完整且名字语义精确的块中的缺失；不完整列举、过滤、未命名子项失败和不确定 / 非精确
名字语义阻止块内缺失判断，具名子项失败阻止该名字的判断。`Matched` 仅表示双方观察到了对应类型，不表示内容相同。
删除前仍须等两场会话都 `Completed`、排除祖先列举失败的子树，并对旧记录应用相同过滤与深度；被剪枝或截断的子目录不能
证明其后代缺失。调用方负责对断点重放去重，以及按目标端真实规则处理大小写 / 规范化冲突。本轮验收见 §14.4。


### 3.2 扫描入口与参数（对照 legacy `walkdir`）

**入口**：`StorageTraversalSource::new(&storage)?.traverse(request) -> TraversalSession`（`src/traversal/storage.rs:56, 114`；
trait 在 `src/traversal/mod.rs:515`）。会话逐项给出 `TraversalItem`，结束后给出结局（`Completed(TraversalCompletion)` / `Cancelled` / 会话失败）。
两种扫描方式都是这一个入口。

```
let request = TraversalRequest::new(root)
    .order(TraversalOrder::Unordered)                         // 或 Blocks
    .filter(Arc::new(DslTraversalFilter::parse(match_expr, exclude_expr)?))   // 与 legacy 同一套 DSL
    .max_depth(NonZeroUsize::new(3))                          // None = 不限
    .should_scan([Extra::Acl, Extra::Tags])                   // 只放需要额外调用的，见下
    .max_inflight_operations(64)
    .max_buffered_items(1024)
    .resumable(Resumable::Off)
    .cancel(token);
let session = StorageTraversalSource::new(&storage)?.traverse(request);
```

#### legacy `WalkOptions`（`src/storage_options.rs:13-21`）→ 新请求

| legacy | 新 | 说明 |
|---|---|---|
| `depth` | `max_depth` | 根的子项是深度 1；深度为 d 的目录只在 d < max_depth 时被列举 |
| `match_expressions` / `exclude_expressions` | `filter: Arc<dyn TraversalFilter>` | DSL 适配器 `DslTraversalFilter`（`src/filter_traversal.rs`）包的就是 legacy 的 `should_skip` 三元组，映射为 `TraversalDecision { emit, descend, filter_children }`。DSL 能引用的字段（名字、路径、类型、扩展名、大小、修改时间）都来自列举本身，不需要额外调用（S3 前缀没有修改时间） |
| `concurrency` | `max_inflight_operations` | 同时进行的观察数；列举预取另有上限 min(它, 64) |
| `include_tags` | `should_scan(… Extra::Tags …)` | 见下表 |
| NFS 的 ACL / owner / xattr 充实（legacy `NfsEnrich`，`src/nfs.rs:97-150`） | ACL / xattr → `should_scan`；owner / mode → 列举顺带，无参数 | 见下表 |
| `packaged` / `package_depth` | 不在扫描里 | tar 打包是消费方的事；打包深度用 `max_depth` 表达 |

#### should_scan：只放**需要额外接口调用**才能拿到的东西

> **原则（用户定的）**：列举本身已经返回的信息（大小、类型、时间、uid / gid / mode、身份）**一律收下**，不需要参数，也不会为它多发任何请求；`should_scan` 里只列“每条要额外调用才能拿到的”。

```
.should_scan([Extra::Acl, Extra::Tags])        // 一个集合，没有档位

enum Extra { Acl, Xattrs, Tags, SymlinkTarget }  // 以后可加：S3 用户元数据、HDFS ACL / xattr
```

- 取代今天的 `ObservationPlan` 在扫描里的用法：`Omit` = 不在集合里；`InlineOnly` 不再需要（列举有的总会收下）；时间与 ownership **不再是可选族**，
  在 `ObservedEntry` 里是普通的 `Option` 字段，列举没给就是 `None`。
- **只有一档（Q31 已定，不要 BestEffort）**：要了就必须拿到。
  - backend 做不到的项：在任何 I/O 之前拒绝整个扫描（看 `NamespaceCapabilities` / 元数据能力），不会扫到一半才发现。
  - 对某条目没有意义的项（S3 前缀的 tags、符号链接的 ACL / xattr、目录的符号链接目标）：记为不适用，不算失败。
  - 单条取失败：先在有界退避内重试瞬时错误；仍失败 → 这一条发 `Failure{scope: Child}`，不发 `Entry`，计入 `failed_children`。按删除安全规则（4.1 第 3 条），这一条不参与删除。
- 传输侧拷贝元数据时自己的观察（`copied_metadata_observation_plan`）不受影响，那是拷贝前按需取的，不属于扫描。

#### 各 backend：列举顺带收下的（无参数）

|  | Local | NFS | CIFS | S3 | HDFS |
|---|---|---|---|---|---|
| 时间 | 有（Unix 列举时逐项 fstatat；Windows 从目录读回的属性） | 有（READDIRPLUS） | 有（QUERY_DIRECTORY） | 对象有 LastModified；前缀没有 | 有 |
| uid / gid / mode | 有 | 有（READDIRPLUS；**今天没保留，要补**） | 没有（只有从只读属性推出的展示用 mode） | 没有 | mode 有；owner 是名字不是 uid |

#### 各 backend：`should_scan` 里可以要的额外项（每条至少一次往返）

| 额外项 | Local | NFS | CIFS | S3 | HDFS |
|---|---|---|---|---|---|
| `Acl` | 开 fd 读 POSIX access / default ACL | GETACL（挂载协商了 ACL 才可用） | CREATE + 查询安全描述符 + CLOSE（最贵） | 不支持 | 今天不支持（有 getAclStatus，可补） |
| `Xattrs` | listxattr / getxattr | NFSv4.2 xattr（服务端支持才有） | 不支持 | 不支持（`x-amz-meta-*` 是否算作这一项待定） | 今天不支持（有 getXAttrs，可补） |
| `Tags` | — | — | — | GetObjectTagging，每个对象（多版本时每个版本）一次 | — |
| `SymlinkTarget` | readlink | READLINK | 不支持 | — | — |

默认是空集合：扫描不做任何额外调用，每条的成本只剩列举本身。

#### 这一原则顺带修掉的两个问题

1. **NFS 的 ownership 被当成额外调用**：今天只要计划里要了任何可选族，遍历就调元数据角色（`observe.rs:17-28`），NFS 的元数据角色先对路径发一次 GETATTR（`nfs/metadata.rs:231-239`），
   而 READDIRPLUS 早已返回了 uid / gid / mode，只是列举描述里没保留（`nfs/namespace.rs:195-212`）。改为列举直接带上。
2. **symlink 目标今天是无条件额外读取**：每个链接都 `read_link`（`observe.rs:127-136`），不在任何计划里。改为 `Extra::SymlinkTarget`；没要时链接条目的目标为 `None`（Q33）。

## 4. 事件模型

```
#[non_exhaustive]
enum TraversalItem {
    Entry(Box<ObservedEntry>),
    Failure(ScanFailure),               // 两种方式都有
    DirectoryListed(DirectoryListed),   // 只有 Blocks
    Checkpoint(Checkpoint),             // 只在 resumable = On 时
}

struct ScanFailure {
    path: StoragePath,
    scope: FailureScope,                // Child：这一个子项；Listing：这个目录的列举不完整
    failure: EntryOperationFailure,     // class / transience / diagnostic
    delivered: Option<u64>,             // scope == Listing：失败前已发出的子项数（0 = 一页都没读到）
}

struct DirectoryListed {                // Blocks
    path: StoragePath,
    outcome: ListingOutcome,            // Complete | Interrupted | Unreadable
    counts: BlockCounts,                // 见第 7 节
}

struct TraversalCompletion { totals: ScanTotals }   // 见第 7 节
```

- **删除**：`SubtreeComplete`、`SubtreeSummary`、`DirectoryListing`（带优先级的互斥枚举）、`ChildOrder`、
  `DirectoryListed.child_order / pruned_children / truncated_children`（后两者进 `counts`）。
  `SubtreeComplete` 在 Blocks 里可以由“名字序 DFS + `DirectoryListed`”推出（Q2）。
- **`scope` 必须有**：今天列举级失败和“列举里某个子项描述不出来”都是 `EntryFailure`，路径都可能是目录本身，消费方分不清“目录没列完”和“目录里有个坏子项”。
- 列举级失败不再单独占一个 `EntryFailure` 项；在 Blocks 里它同时体现在 `DirectoryListed.outcome`。

### 4.1 消费方的删除安全规则（写进契约）

1. 只有会话结局是 `Completed` 才能做删除检测。
2. 任何 `scope == Listing` 的目录，**其整棵旧子树**都不参与删除：列举中断后，下面的子目录可能根本没被看到。
3. `scope == Child` 的条目本身不参与删除。
4. 有 filter 或 `max_depth` 时，旧代条目也要用同一个 filter / 深度判定，被排除或截断的不参与删除（这些信息来自请求本身）。
5. 大小写或 Unicode 规范化不敏感的目标上，“折叠后相等、字节不等”的一对名字报冲突，不删除（Q6）。
6. **路径逐字节比较，绝不去掉末尾的 `/`**（F5）：S3 的目录对象 `p/d/`、文件 `p/d`、目录 `p/d` 是三个不同的条目（身份键、
   快照、`TransferIdentity` 都不同）。规则 4 判定目录对象时用它自己的名字与深度：name = `d`（最后一个非空段），
   深度 = `p/d` 子项的深度。把 `p/d/` 规范化成 `p/d` 的消费方会把两者合并，删除检测可能删错（§11 对 terrasync 的要求）。

## 5. 流式列举：Namespace 接口与各 backend

### 5.1 接口

```
trait Namespace {                       // 类型化动词，删除 NamespaceRequest / NamespaceResult / execute
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure>;
    fn list(&self, request: ListRequest) -> ListingStream;          // 唯一的列举入口
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure>;
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure>;
    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure>;
    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure>;
    fn capabilities(&self) -> NamespaceCapabilities;  // histories / mutations / symlinks / 名字语义（大小写、规范化）
}
struct ListRequest  { path: StoragePath, cancel: CancellationToken, page_hint: NonZeroUsize }
struct ListingPage  { children: Vec<ListedChild>,  // 名字 + kind + size + inline 时间/mode + 身份 + backend fact
                      failures: Vec<ChildFailure>,  // 按名字；拼不出名字时带原始字节
                      internal_hidden: u32 }        // .data-mover-*，只计数
type ListingStream = Pin<Box<dyn Stream<Item = Result<ListingPage, StorageRoleFailure>> + Send + 'static>>;
```

- **契约**：任何时刻 drop 流都不得泄漏服务端资源。
- **机制**：每个列举一个生产者 + 容量 1 的 channel（`ListingStream::from_producer`，新文件 `src/storage/listing.rs`）。channel 满即停止翻页（背压到协议层，预读一页）。
  C10 起生产者先在消费方自己的任务里推进，交出第一项后仍未结束（或流被 drop）才转成 detach 的任务：单页目录不另起任务、没有任务间交接（§12.6 O1）。
  接收端消失 → 生产者的 `send` 失败 → 走自己的 `close_resource` 路径退出。CIFS 句柄在结构上不会泄漏，与 P2 “detach 不 abort” 一致。
  今天的 `listings_are_abortable()`（`roles.rs:552`）因此删除。
- **页里只带名字**：目录路径只在请求里出现一次，不再给每个子项拼完整路径（同一个父前缀存 N 遍）。`ObservedEntry` 构造时才拼（直到 P9）。
- 单目录调用方（`delete_tree.rs:263`、`ndx_walk.rs:211`）用 `collect_listing(stream, limits)`。

### 5.2 各 backend

| backend | 流式做法 | 中途出错 |
|---|---|---|
| Local | 每页一次 `spawn_blocking`，把 `ReadDir` 带进带出；停车时不占 blocking 线程 | readdir 错误基本不是瞬时的 → `Interrupted`（已交付过）/ `Unreadable`（没交付）；已读到的先交付。**已实现** `277f1f8`：20 万文件平铺目录首项 400 ms → 6 ms、RSS 217 → 22 MB |
| NFS | 生产者任务逐个读取 READDIRPLUS 回复，按 `page_hint` 攒页 | **首页前后都按 cookie 续读**（C7c `26a27f6`，首页前由 `c0ff649` 补齐）：保留已攒页，从最后完整回复后的 cookie + verifier 继续；句柄变更或 cookie 被拒 / 续读答空末页则从头读，按已接收项 `(fileid, 名字)` 去重。句柄重查、可安全重发的传输失败与从头重读各至多 3 次无进展重试；新接收项才续满预算，节流总时长不重置。去重上限 2^18 项（约 4.5 MiB）：超过后已有页交出则不再续；尚未发布时可丢缓冲有界从头重试，不再开启去重或补预算。cookie 在一次读取内回环 → `Protocol` / Permanent。重试用尽先交出已读子项；安全 Resend 只在已有页交出时收窄为列举失败，之前仍是会话失败。drop / 取消不放弃在途请求。C7b 的缺属性 `.snapshot` 经 LOOKUP 确认为目录后隐藏。历史真机与性能见 §14 C7 / C7c、§12.8；本轮验收见 §14.4 |
| CIFS | 生产者任务持有目录句柄（`CifsDirectory`），逐条取 `QUERY_DIRECTORY` 记录按 `page_hint` 攒页；所有出口都 close，最后一页在关句柄之后交出；停车时句柄开着 → `Namespace::parked_listing_limit()` = 8，Blocks 游标让“已起、未读完”的预取列举不超过它（游标正要的那个总是起） | 首页交出之前句柄失效（smb-rs 重连后旧句柄上的 `InvalidState`、重连时在途的 `StaleObject`、`STATUS_FILE_CLOSED` / `STATUS_INVALID_HANDLE`）→ 重开目录从头读（50 ms / 250 ms / 1 s，至多 3 次，不重复交出）；之后句柄失效无法续 → 已读到的先交出，`Interrupted`（原分类，连接类升会话）。取消 / drop 在取下一条之前停、让在途记录到达；smb-rs 流已预发的下一条 `QUERY_DIRECTORY` 随流 drop 取消（#75/#76）。没有下钻提示（CREATE 按路径一次往返）。**已实现** `936e20b`（接缝 `70f85d1`）：5 万文件平铺目录首项 400 → 34–46 ms、RSS 65 → 21–25 MB（release）；10.3 万条整树 c=1/4/32 与 base 逐行相同（含顺序），RSS 84–118 → 30–44 MB；停车时 ONTAP 上恰 9 个打开的目录句柄（base 0）；与 NFSv3 / v4.1 同一棵树 Blocks 输出逐行相同；停车 30 分钟后照常读完（Q10） |
| S3 | 未开过版本控制的桶：ListObjectsV2，每页转一页；多版本桶：ListObjectVersions，按目录组装每个 key 的历史（见 5.3、第 10 节） | 同一 token 重发（无状态，安全）；节流退避。**已实现** `f2cacdc`（Current 流式；版本化桶按目录物化后每 key 一项、按 `page_hint` 逐页构造子项，C11b）：不假设 key 有序（只用不透明 token，前缀名集合去重，对象只与上一页比对，列完才判 `NotFound`）；节流按 C3 预算、其他瞬时失败同一 token 再发至多 3 次（读到一页即续满），首页前后一样；500 万上限不再限流式 Current 的对象数，只管驻留的前缀名集合与不可解码 key（续页 token 另限 500 万）；物化的历史列举仍按条数计（用户 2026-09-26 定，**已修** `ffd37ca`；5,000,001 个对象的平铺前缀在生产上限下流完）。2 万对象平铺前缀首项 MinIO 3.5 s → 0.4 s、ONTAP 1.2 s → 0.13–0.25 s，RSS 68 → 54 / 66 → 50 MB（debug）；MinIO / ONTAP（含 NAS 桶）/ RustFS / DXN 的多版本与非多版本桶均已跑 |
| HDFS | `list_status_iter`（hdfs-native `client.rs:734`）；去掉列举前的 `get_metadata` | 续读：hdfs-native 不公开 startAfter，可重试失败（I/O、单批 > 5 s）后重开列举、跳过到最后读到的名字之后（按 NameNode 的有符号字节序；只在失败时多读），首页前后都可续、不重复发子项；其他失败或预算用完 → `Interrupted`。**已实现** `9c8185f`；有符号字节序、列举文件回自身、缺目录 `FileNotFound` 已在 2026-09-26 的 C8 真机验收确认（§14）；本轮修复与真机验收见 §14.4 |

### 5.3 S3：不假设有序

| 逻辑 | 做法 | 内存 |
|---|---|---|
| 对象跨页重复 | 遍历里不去重，由库去重吸收；`listed` 计数可能含少量重复（写明） | O(页) |
| 公共前缀跨页重复、不认 delimiter 的 rolled-up 前缀 | 每个列举一个前缀集合，必须去重，否则整棵子树扫两遍 | O(本目录子目录数) |
| 分页循环检测 | continuation token 集合（今天的 `Paging.seen`，`paging.rs:34`）+ 页数上限；循环 → 该列举 `Interrupted(Protocol)`，不再结束整场 | O(页数) |
| 多版本桶：把一个 key 的全部历史组装成一项 | 无序存储上一个 key 的版本可能在任意页，**只有列完整个目录才知道某个 key 的历史完整** → 默认按目录物化（今天 `history.rs` 的 `BTreeMap` 分组本来就不依赖顺序），上限 500 万条版本记录，超限 `Capacity`。见第 10 节与 Q9 | O(目录) |
| “前缀下什么都没有” | 列完仍为空才判定 `NotFound`（今天的 `refuse_absent`） | — |

### 5.4 接口逐项说明

下面每个函数都注明**今天对应的是什么**、**谁在调用**、**各 backend 的行为**。所有函数的错误都是 `StorageRoleFailure`：`Entry(EntryOperationFailure)` 只关乎这一个路径，调用方可以继续；
`Session(BackendSessionFailure)` 表示连接或会话不可用，调用方应当停止。

#### 5.4.1 函数

| 函数 | 做什么 | 今天 / 调用方 | 各 backend |
|---|---|---|---|
| `stat(path) -> SourceDescriptor` | 描述**一个**条目本身，不跟随最后一段的符号链接（链接就描述链接）。不存在 → `Entry(NotFound)` | `execute(Stat)`；`create_dir.rs:137`（建目录遇冲突时确认已有的是不是目录） | Local：逐段不跟随 symlink 解析后 `symlink_metadata`；NFS：LOOKUP + GETATTR；CIFS：CREATE + 查询属性 + 关闭；S3：先 HEAD 对象，没有再看前缀下是否有东西（前缀算目录）；HDFS：`get_file_info` |
| `list(ListRequest) -> ListingStream` | 列一个目录的**直接子项**，一页一页返回（见 5.4.2–5.4.4） | 今天的 `execute(List)` + `list_versions`（后者删除）；遍历（`cursor.rs:576-577`）、`delete_tree.rs:263`、`ndx_walk.rs:211` | 见 5.2 |
| `read_link(path) -> SymlinkTarget` | 读符号链接**本身存的文本**，不解析、不跟随。`SymlinkTarget` = 编码（`UnixBytes` 或 `WindowsWide`）+ 原始字节，不假设是合法路径 | `execute(ReadLink)`；遍历观察 symlink 子项时调用（`observe.rs:127-136`） | Local、NFS 支持；CIFS（smb-rs 不暴露 reparse point，列举里 reparse point 记为 File）、S3、HDFS → `Unsupported` |
| `create_directory(path)` | **只建一层**；已存在 → `Entry(Conflict)`，由调用方决定是否当成功 | `execute(CreateDirectory)`；递归建目录 `create_dir.rs:125` 逐层调用 | Local、NFS、CIFS、HDFS 支持；S3 → `Unsupported`（没有目录） |
| `delete(path)` | 删**一个**文件 / 链接本身 / **空**目录。只剩 `.data-mover-*` 的目录视为空，先清掉 artifact 再删；还有别的东西 → `Conflict`，什么都不删 | `execute(Delete)`；递归删除 `delete_tree.rs:301, 355` | Local、NFS、CIFS、HDFS 支持；S3 → `Unsupported` |
| `rename(from, to)` | 改名一个文件或目录，目标已存在则覆盖（在协议允许的范围内） | `execute(Rename)`；**生产代码里没有调用方**，只有测试（`tests/cifs_namespace_contract.rs`）。传输发布走 `StagedDestination::publish`，不走这里 | Local、NFS、CIFS、HDFS 支持；S3 → `Unsupported`。**保留（Q23 已定）** |
| `capabilities() -> NamespaceCapabilities` | 不做 I/O，回答“这个命名空间能做什么、名字怎么比较”，让调用方**在任何 I/O 之前**拒绝做不到的事 | 取代今天的 `supports_versions()`（改为 `histories`）（`storage.rs:90, 115`）、`mutations_unsupported()`（`handle.rs:190`）、`listings_are_abortable()`（删） | 见 5.4.5 |

#### 5.4.2 `ListRequest`

| 字段 | 含义 | 为什么需要 |
|---|---|---|
| `path: StoragePath` | 要列的目录，相对存储根；空路径 = 根 | — |
| （没有 `versions`） | 已删除。多版本桶上列举**总是**返回每个 key 的完整历史；不保存版本的命名空间没有历史 | 用户定的：历史是对象的属性，不是一种列举模式。今天的 `list_versions`（`roles.rs:534`）与 `TraversalVersions`（`mod.rs:65-86`）一并删除 |
| `cancel: CancellationToken` | 取消后尽快停止翻页 | 今天 Namespace 请求不带令牌，S3 取消后会在后台继续翻页（C22 的遗留）。drop 流也会停，令牌的额外作用是打断**正在等待的那个请求**：S3 / HDFS 是无状态请求，立即放弃；CIFS 要等当前 QUERY_DIRECTORY 返回再关句柄 |
| `page_hint: NonZeroUsize` | 每页最多多少个子项（例如 1024）。backend 可以更少（服务端一页更小），**不能更多** | 它决定遍历里每个打开的列举最多占多少内存。原生页大小各不相同：S3 最多 1000 个 key，NFS 一次回复约 32 KB，CIFS 约 64 KB 缓冲，HDFS 默认 1000 |

#### 5.4.3 `ListingStream` 的语义

- 依次产出 `Ok(ListingPage)`；**流正常结束 = 这个目录列完了（读到 EOF）**。这是遍历判定“列举完整”的唯一依据。
- `Err(Entry(failure))`：这个目录的列举失败，之后流结束。还没出过页 → 遍历记 `Unreadable`；出过页 → `Interrupted { delivered }`。
- `Err(Session(failure))`：会话不可用，遍历终止。
- backend 内部的重试（节流退避、S3 同一 token 重发、NFS 首页前从头重来）对调用方不可见；只有放弃时才产出错误。
- 任何时刻 drop 都不得泄漏服务端资源。
- 不保证任何顺序；同一子项可能在跨页时重复出现（写进契约，Q13）。

#### 5.4.4 `ListingPage`、`ListedChild`、`ChildFailure`

| 字段 | 含义 | 谁用 / 今天的对应 |
|---|---|---|
| **ListingPage** |  |  |
| `children: Vec<ListedChild>` | 这一页里描述成功的子项 | 今天 `NamespaceResult::Entries / Listing.entries`，但那是整个目录 |
| `failures: Vec<ChildFailure>` | 列举**返回了**、却描述不出来的子项：名字拼不成路径（空段、`.` / `..`、非 UTF-8）、缺属性、版本 id 非法、存储根自己的对象有内容…… | 遍历把它们发成 `Failure{scope: Child}`，计入 `failed_children`。今天 CIFS / NFS / HDFS 遇到这种子项会让整个目录失败 |
| `internal_hidden: u32` | 这一页里被隐藏的 `.data-mover-*`（传输过程中的 stage / 指针 artifact）个数；C7b 起也含 NFSv3 列出的、经 LOOKUP 确认是目录的缺属性 `.snapshot`（ONTAP 快照目录） | 它们不是子项，不进 `listed`，单独计入 `internal_hidden`（需求 4）。今天各 backend 直接丢弃、从不计数 |
| **ListedChild**（今天的 `SourceDescriptor`，`src/storage/descriptor.rs`，把完整路径换成名字） |  |  |
| `name` | 最后一段名字。完整路径 = 请求的 `path` + `/` + `name`，**由契约规定**。backend 自己的拼法与之不一致时，必须报 `ChildFailure`，不能交一个拼不回去的名字（这正是 `storage/tests/slots.rs` 钉住的护栏：下钻目标必须逐字等于列举给的拼写）。**名字可以为空**（F5）：空名 = 被列目录自己的对象（S3 的键 `<path>/`），路径因此恰为 `<path>/`；只有 `ListedChild::directory_object` / `ListingPage::push_directory_object` 能产生，只能是 File，存储根上没有（`ListedChild::new` 仍拒空名） | 遍历拼路径、Blocks 下钻排序 |
| `kind` | `File` / `Directory` / `Symlink` / `Special`。CIFS 的 reparse point 记为 `File`（读不了链接目标） | 决定是否下钻、是否 `read_link` |
| `size: Option<u64>` | 文件大小；没有意义时为 `None`（S3 前缀、删除标记） | 统计字节、filter `size` 条件 |
| `source_identity` | 稳定身份 = 哪个后端 + 强度 + 稳定字节。强度：`PathScoped`（只在路径不变时稳定，如 S3 ETag）、`StableWithinBackend`（在这个命名空间里稳定，如 NFS 文件句柄、CIFS file id）、`VersionScoped`（一个不可变版本，如 S3 version id） | 算出 32 字节 `EntryIdentityKey`（blake3）；terrasync 按它关联新旧两代（`OBSERVATION_IDENTITY_JOIN_CLAUSE`），用于识别改名 |
| `content_version: Option<Bytes>` | 内容变化的标记，与身份分开（S3 记 ETag） | 变更检测；native copy 绑定“拷的正是列出来的那一份” |
| `inline_timestamps: Option<TimestampMetadata>` | 列举**顺带**返回的时间（accessed / modified / created）；`None` = 列举没给 | 默认观察计划下不用再调元数据角色；有 `modified` 时 filter 可以即时决定（C0b） |
| `inline_mode: Option<u32>` | 列举顺带能推出的权限位，**仅供展示**（CIFS 由只读属性推出），永远不用于拷贝元数据 | 扫描输出 |
| `backend_fact: Option<Bytes>` | backend 私有的不透明事实（例如 NFS 文件句柄），外部不可解读 | 编进条目快照；只有同 backend 的 native 规划器会读 |
| `history: Option<Box<ObjectHistory>>` | 多版本桶上的对象：这个 key 的全部历史，每条 `VersionRecord` 一个版本或一个删除标记，见第 10 节。其他情况为 `None` | 取代今天的 `version` / `listing.version` / `listing.rank`（每版本一个子项的设计） |
| `listing_only: bool` | 这个子项背后没有东西可以观察（S3 前缀） | 遍历只用列举给的信息，不再问元数据角色 |
| **ChildFailure** |  |  |
| `name: Option<String>` / `raw_name: Option<Bytes>` | 能拼出名字就给名字；拼不出（非 UTF-8 等）给原始字节 | 失败项的路径与诊断 |
| `failure: EntryOperationFailure` | 分类（`FailureClass`）、是否瞬时（`Transience`）、诊断文本 | 发成 `Failure{scope: Child}` |

#### 5.4.5 `NamespaceCapabilities`

| 字段 | 含义 | 谁用 | 各 backend |
|---|---|---|---|
| `histories: bool` | 这个命名空间的对象是否可能带历史（`ListedChild.history`） | 消费方决定是否准备处理历史 | 多版本 S3 桶 true，其余 false |
| `mutations: Result<(), &'static str>` | 能否 `create_directory` / `delete`（/ `rename`）；不能时给出原因 | 递归删除、递归建目录在 I/O 前拒绝（今天 `handle.rs:190`） | S3 不能，其余能 |
| `symlinks: bool` | 能否 `read_link` | 遍历决定 symlink 子项是否需要读目标 | Local、NFS true；其余 false |
| `directory_objects: bool` | 能否存目录对象（键以 `/` 结尾的文件，F5） | 消费方拷贝前用 `transfer::directory_object_action` 判定；引擎在 Preflight 拒绝写到不能存的一端（`Storage::holds_directory_objects`） | S3 true，其余 false |
| `names: NameSemantics { case: Sensitive \| Insensitive \| Unknown, normalization: None \| Nfc \| Nfd \| Unknown }` | 这个命名空间怎么判断“两个名字是同一个” | 消费方的删除安全规则（第 4.1 节第 5 条，Q6）：不敏感的目标上，“折叠后相等、字节不等”报冲突、不删除 | CIFS：连接时查询卷属性里的 `FILE_CASE_SENSITIVE_SEARCH`；NFS：v3 `PATHCONF` / v4 `case_insensitive` 属性；Local：按平台（Linux 敏感，Windows / macOS 默认不敏感）；S3、HDFS：敏感。查不到就是 `Unknown`，按不敏感处理（Q25） |

#### 5.4.6 为什么每个 `ObservedEntry` 都带 `SourceIdentity` 和 blake3 身份键

**两者的分工**：`SourceIdentity` = 后端标识（哪个端点）+ 稳定程度 + 稳定字节（NFS 文件句柄、CIFS file id、S3 ETag 或 version id），是原始事实；`EntryIdentityKey` 是对它加域分隔、
长度前缀后做的 blake3，固定 32 字节（`src/model/observation.rs:111-121`），是它的**可比较、可落库、不泄露内容**的投影。它**不是内容哈希**。

| 用途 | 用的是哪个 | 代码 | 为什么需要 |
|---|---|---|---|
| 1. 拷贝前确认“要拷的还是扫描时看到的那个对象” | `SourceIdentity` | `expert_observation.rs:52`（`observation_matches`）；各 backend：`roles.rs:182`、`s3/source.rs:152-162`、`cifs/source.rs:124`、`hdfs/source.rs:100, 128` | 扫描和拷贝之间文件可能被替换（同路径、不同文件）。身份对不上就报 `Conflict`，不拷一个不是扫描结果的东西 |
| 2. 跨代识别改名 | `SourceIdentity::stable_bytes()`（只在 `StableWithinBackend` 时给）+ 身份键 | terrasync `OBSERVATION_IDENTITY_JOIN_CLAUSE = t.identity_key = b.identity_key`，`identity_key` 列带 bloom filter 索引 | NFS 文件句柄、CIFS file id 改名后不变：新旧两代按它关联，把“删 A + 建 B”识别为“A 改名为 B”，拷贝时不重传数据 |
| 3. 目的端恢复绑定 | 身份键 | `transfer/engine/plan.rs:112-129`（`recovery_binding_for`） | ADR-0006：续传用的 stage 只能被**同一个**源对象续上。路径相同但文件已被替换时，绑定对不上，不会把两个文件的字节拼在一起 |
| 4. 远程传输两半之间的核对 | 身份键 | `expert.rs:125`（源端在 offer 里给出），`expert.rs:576`（目的端核对 evidence） | 两个进程（terrasync 经 QUIC）之间只传 32 字节，不把 backend 私有的句柄字节送过网络 |
| 5. 快照完整性 | 身份键 | `observation_codec.rs:170-182` | 解码时重算并与存的键比较，不一致报 `IdentityMismatch`，发现损坏或拼错端点 |

**为什么要做 blake3，而不直接用原始字节**：

- **定长**：稳定字节长度不一，最长可到 16 MiB（`MAX_MODEL_FIELD_BYTES`）；32 字节适合做数据库列与索引。
- **不泄露**：NFS 文件句柄拿到手就能绕过路径直接访问服务器上的文件；`Debug` 已经把稳定字节打码。落库、上日志、跨网络的只应是不可逆的键。
- **绑定端点**：键里混入了后端标识，同样的句柄字节出现在两个不同的端点上，键也不同，不会误关联。
- **域分隔**：前缀 `data-mover/source-identity/v1` + 长度前缀，保证不同字段组合不会产生相同的输入。

**成本与可以优化的地方**：剖面里 blake3 约占 1%（每条约 100–200 ns，53M 条约 5–10 s CPU）。更贵的其实是**分配**：每条都克隆一份 `BackendIdentity`（内含字符串，
`model/mod.rs:359-362`），再为稳定字节分配一个 `Vec`。建议并入 C0：

1. `BackendIdentity` 改为 `Arc` 共享（一个存储一份），每条只加一次引用计数；
2. 稳定字节用小缓冲内联（NFS 句柄 ≤ 64 字节、CIFS file id 16 字节、ETag 约 34 字节，几乎都放得下）；
3. 身份键保持**在构造时计算**：扫描写库的每一条都要用到它（terrasync 的 `ObservedEntryRecord::capture`），推迟计算省不下什么，反而要处理并发缓存。

#### 5.4.7 稳定程度（`IdentityStrength`）是什么

稳定程度回答的是：**这串稳定字节在什么变化之下保持不变**，也就是它能不能用来认出“还是同一个对象”。它决定消费方可以拿这个身份做什么（`src/model/observation.rs:17-24, 95-107`）。

| 稳定程度 | 含义 | 改名后 | 改内容后 | 同路径删了再建 | 谁产生（代码） |
|---|---|---|---|---|---|
| `StableWithinBackend` | 存储自己给对象的身份，与路径、内容无关 | 不变 | 不变（内容变化另由 `content_version` / size+mtime 判断） | 变 | NFS 文件句柄（`nfs/source.rs:104`）；CIFS file id，服务端给了才有（`cifs/source.rs:240`）；Local Unix 的 dev+inode（`local/observation.rs:700`） |
| `PathScoped` | 只在这条路径上有意义，通常还混入了内容事实 | 变（看起来像删一个、建一个） | 通常变 | 通常变 | HDFS：路径 + size + mtime（`hdfs/source.rs:314-323`）；CIFS 没有 file id 时：路径 + 事实（`:251`）；Local 非 Unix：路径（`local/observation.rs:714`）；S3 当前对象或 `"null"` 版本：ETag（`s3/source.rs:75-89`） |
| `VersionScoped` | 一个不可变版本的身份 | —（S3 没有改名） | 版本本身不会变；新内容是新版本 | 新版本 | S3 有真实 version id 的版本（`s3/source.rs:84`） |

**消费方能做什么**：

- 只有 `StableWithinBackend` 才公开原始字节（`stable_bytes()` 返回 `Some`），可以跨代关联、识别改名。
- 三种都可以用来做“扫描与拷贝之间对象有没有被替换”的核对（第 5.4.6 节用途 1）。
- `PathScoped` 必须和路径一起用；`VersionScoped` 必须和 key 一起用。

**要知道的局限**：Local 的 dev+inode 没有代数（generation），文件删掉后 inode 可能被新文件复用，新文件会被误认成旧文件改名而来。NFS 文件句柄里通常带代数，没有这个问题。
消费方识别改名时仍应核对 size / mtime。

> **发现的问题：S3 的 `PathScoped` 身份里没有路径**。S3 当前对象的稳定字节**只有 ETag**（`s3/source.rs:75-89`），而 HDFS、CIFS、Local 的 `PathScoped` 都把路径放了进去。
> ETag 由内容决定，所以**内容相同、路径不同的对象身份键相同**。空对象的 ETag 全都是 `d41d8cd98f00b204e9800998ecf8427e`，一个桶里所有空对象共用同一个身份键。
>
> terrasync 的 `detect_changed_observations`（`crates/db/src/clickhouse.rs:379-392`）**只按 `identity_key` 做 INNER JOIN**，
> 并把 `relative_path` 不同的配对报成“变更”。结果是内容相同的对象两两交叉配对：N 个空对象产生 N×N 行，全部被当成改名或变更。`stable_bytes()` 的文档说 `PathScoped` 的字节“随路径变化”，
> 这个前提对 S3 不成立。
>
> **修法**（Q28）：(1) data-mover 的契约写明“`PathScoped` 的稳定字节必须包含路径”，S3 改为 路径 + ETag，与 HDFS / CIFS 一致；
> (2) terrasync 只对 `StableWithinBackend` 按身份键关联，其余按 `(relative_path, version_id)` 关联。两边都改。改 S3 身份后，已有代的 S3 条目身份键全部变化，
> 下一次扫描会把它们一次性看成“新增 + 删除”。按“不考虑兼容”的原则可以接受，但要在变更说明里写明，或者在 terrasync 侧做一次重建。
>
> **状态**：data-mover 侧已修（`4fa5aa5`，现存问题 7）；terrasync 侧的关联方式改动仍待在 terrasync 里做。

## 6. 调度与内存

### 6.1 Unordered

- K 个 worker，各自 LIFO 栈（深度优先，压住待处理集合），栈空时窃取别人栈底最浅的目录（抢最大子树）。页直接进条目通道，不经重排缓冲。
- 没有序号、没有目录事件、没有计数树；只有全局原子计数。
- 内存：O(在飞列举 × 页) + O(待处理目录)。LIFO 下待处理 ≈ 深度 × 扇出 × worker，与 legacy 的任务队列同量级。
- 保证：目录的 `Entry` 先于它的任何子项（发现子目录时先发它的 Entry 再入栈）。
- **C10 的实现**（`src/traversal/storage/unordered.rs` + `unordered/{pool,worker,page_io}.rs`）：K = min(c, 64)，namespace 有
  `parked_listing_limit` 时再取 min（CIFS 8：worker 等消费方时列举一直开着，K 即打开句柄的上界）。出栈 / 窃取与登记 `active`
  在同一临界区（窃取按下标序锁两个槽位），任何时刻每个未完成目录都在某槽位里 —— §9.2 的一致性条件，C11 的前沿快照按下标序锁全部
  槽位即可。`outstanding` 子目录入栈前加、父目录全部子目录入栈后减，减到 0 的 worker 取消 halt。页用 Blocks 同一段
  `stream::prepare`（含扫描根守卫，§14.1），一页一批整批交给会话（`channel.rs` 的 `send_whole`）之后才压子目录。每个 key 的
  全部版本连续：页末同一 key 的一段留到下一页批首。与 Blocks 对同一棵树条目、失败、`ScanTotals` 逐字段相同。

### 6.2 Blocks

- 现有准入游标改为逐页消费：`Frame.children` 从整块 `VecDeque` 变为“已到页 + 仍打开的流”；块关闭条件改为“缓冲空且流读尽”；过滤决定逐页做。
- 块关闭时对槽位（子目录）按名字字节排序，然后按序下钻。
- 祖先帧的流在下钻前已读尽，**栈上没有横跨子树的打开游标**。只有“已预取、游标还没走到”的目录会停在半路（≤ 2P 个）：NFS/S3/HDFS 停车什么都不占；Local 占一个 fd；**CIFS 占一个真实句柄**，单设上限（Q10）。
  C9 `936e20b` 的实现：`Namespace::parked_listing_limit()`（默认 `None`，CIFS 8）；游标预取时“已起、未读完”（等页的 + 停车的）
  列举数不超过它，所以停车的必在其中、上界严格，游标正要的那个不计、总是起，上限不会卡住遍历；打开的 CIFS 列举句柄 ≤ 8 + 1
  （FAS2750 实测恰为 9）。代价：CIFS 上预取并发从 2 × min(c, 64) 降到 8；10.3 万条的树上 c=32 wall 未变差（0.81 → 0.67 s）。
- 预取语义从“整份列举到齐”变成“首页到齐”。小目录（多数）一两页就读完，行为与今天相同、句柄立即关闭。
- 内存：O(打开的列举 × 2 页) + O(当前与已预取目录的子目录数)。后者（槽位）P7 压不住，归 P8/P9（名字 arena / 溢写）。

### 6.3 驻留探针

`storage/residency.rs` 加 `pages_buffered`、`parked_listings`、`parked_handles`。验收：平铺 N 文件目录的待准入子项峰值与 N 无关。

## 7. 计数

```
struct ScanTotals {                 // TraversalCompletion 里，两种方式都有
    listed, emitted, filtered, failed_children, vanished, internal_hidden,
    listing_failures, interrupted_listings,
    pruned_dirs, truncated_dirs,
    bytes, files, directories, symlinks, other,
}
struct BlockCounts { /* 同上的每目录版本，只在 Blocks 的 DirectoryListed 里 */ }

不变式：listed == emitted + filtered + failed_children + vanished
       （internal_hidden 不在 listed 里；pruned / truncated 与之正交：子目录可以既 emitted 又没下钻）
```

- 即时与 deferred 两条过滤路径合并计入 `filtered`；deferred 且观察失败的目录计 `failed_children`，仍照常下钻（`storage.rs:528-541`）。
- 列举级失败不是子项，不进不变式；`listed` 只数失败前真正返回的。
- S3 目录对象 `p/d/`（F5）是 `p/d` 块里的 File：计入 `listed` / `emitted`（或 `filtered`）/ `files`，字节计入 `bytes`；
  零字节的文件夹标记也一样（D8）—— console / s3a 建的空文件夹多的桶，files 与入库行数会明显上升。扫描根自己的零字节
  对象不计数，有内容的计 1 个 `failed_children`（D4）。
- Unordered 下每条目只是几次 `u64` 自增，无分配。
- 分布统计（扩展名、大小区间、时间区间、深度、名字长度）是业务口径，放在 terrasync，从库里 SQL 聚合（第 11 节）。
- 续扫后遍历侧计数是近似的（重扫的部分会重复计），Q15。

## 8. 错误模型

三层载体：**子项失败**（`Failure{scope: Child}`，流继续）、**列举失败**（`Failure{scope: Listing}`，该目录不完整，兄弟继续）、
**会话终止**（`TraversalTerminalFailure::Session`，流结束）。取消是 `TraversalOutcome::Cancelled`，不是错误（R10）。

| 类 | 例子 | 行为 | FailureClass / Transience | 今天 |
|---|---|---|---|---|
| 子项 | 名字拼不成路径 / 点段 / 非 UTF-8 | 子项失败，继续 | `Unsupported` / Permanent | Local、S3 逐子项；**CIFS、NFS、HDFS 整目录失败** |
| 子项 | 列举项缺属性（NFS） | 子项失败 | `Protocol` / Unknown | **NFS 整目录失败** |
| 子项 | 单个子项 stat / 权限 / 可选元数据失败 | 子项失败 | 按映射表 | 已如此 |
| 消失 | 列举后、观察前消失；父列出后子目录已不存在 | 计入 `vanished`，不发失败项，不破坏完整性（Q12）。遍历根不存在仍是列举失败 | `NotFound` | 今天是失败项 → 活跃目录上删除检测常被禁用 |
| 列举 | 无权限 / 打不开 | `Unreadable` | `PermissionDenied` / Permanent | 已如此 |
| 列举 | 第 k 页失败（已发出 N 个） | 先重试（首页前可从头，之后只能从位置续）；续不了 → `Interrupted { delivered: N }` | 按原因 | 不存在（整块物化） |
| 列举 | cookie / token 循环、空页不前进 | 该列举 `Interrupted`，不结束整场 | `Protocol` / Permanent（NFS 空页不前进由 nfs-rs 报 `Xdr`，与畸形回复分不开，是 `Protocol` / Unknown） | S3 → 会话失败（`paging.rs:83-86`）；NFS 自 C7c 起检查同一次读取内的 cookie 重复 |
| 会话 | 连接重置、服务器宕、SMB session / Kerberos 票据过期 | 重连属于 backend 会话层；无状态位置在恢复后续页；CIFS 句柄失效 → 该列举 `Interrupted`；恢复不了 → 终止（Resumable 时带最后检查点） | `Connectivity` / Transient；`Authentication` / Permanent | 任何会话失败都结束遍历（`cursor.rs:476-478`） |
| 会话 | S3 凭据过期（`ExpiredToken`） | 终止 | 会话 / Permanent | 已如此 |
| 节流 | S3 429 / 503 / SlowDown；NFS JUKEBOX / DELAY | **退避，不是失败**：生产者内有界、带抖动、看取消令牌；预算耗尽才 `Interrupted`（Q11） | **新增 `FailureClass::Throttled`** / Transient | **S3 一次（SDK 重试后的）SlowDown 就结束整场遍历**（`role_protocol.rs:518-523`） |
| 资源上限 | 多版本桶目录上限、单 key 记录上限、失败数阈值 | `Capacity` / Permanent（列举级或单 key）；可选 `max_child_failures` 快速失败 | `Capacity` | S3 500 万 |
| 取消 | 令牌 | 结局 `Cancelled`；流被 drop，生产者自己关句柄 | — | 已如此 |

**legacy 对照**：legacy NFS 只在处理任何条目之前对可失效句柄 / 服务端忙从头重试，固定 sleep 500 ms（`nfs.rs:3389-3418`）；之后的读失败只 `error!` + 一条 `ErrorEvent::Scan`，
没有“目录不完整”的结构化信号，所以 terrasync 遇任何扫描错误就整场禁用删除检测。

## 9. 断点续扫

### 9.1 接口：扫描入参，由上层决定

```
TraversalRequest::new(root)                 // non_exhaustive + 构造器
    .order(TraversalOrder::Unordered | TraversalOrder::Blocks)
    .resumable(Resumable::Off)              // 默认
    // .resumable(Resumable::On { every: Duration::from_secs(60) })   // 默认 1 分钟
    .resume_from(Option<Checkpoint>)        // None = 从头扫

struct Checkpoint(Bytes);   // 不透明、带版本；绑定 root / order / filter / max_depth 的指纹
```

- **Off**：不跟踪前沿、不取快照、流里没有 `Checkpoint` 项，开销为零。
- **On**：按间隔在流里发 `TraversalItem::Checkpoint`。
- `resume_from` 的指纹与本次请求不一致时，在任何 I/O 之前返回 typed 失败。
- **间隔的含义**：每约 1 分钟在流里**发出**一个检查点，记录发出那一刻的位置；不是“延迟一分钟再保存”。上层收到后等它之前的批写库确认，立刻落库（等的是批量写入的延迟，通常几秒）。崩溃后最多重做约 1 分钟的扫描加上未落库的批。
  不到 1 分钟就结束的扫描不产生检查点。
- **取消时补发一个检查点**：取消时遍历在结束前立即发一个检查点，不等满 1 分钟，所以“取消”就是“暂停”，续扫几乎不重做。会话失败时如果还能发，也补发一个。

### 9.2 语义：不求精确，只要求进度不领先于已落库的数据

> **不变式**：检查点、generation 提交、任务成功，只能建立在 ClickHouse **已确认**的写入之上。满足这一条，从更早的检查点重来、重复扫一段都不会丢数据。

|  | Blocks | Unordered |
|---|---|---|
| 检查点内容 | 块边界上的 DFS 位置（根到当前目录 + 每层下一个要下钻的名字），O(深度) | 前沿（已发现未开始列的目录）+ 正在列的目录，各带深度与 filter 状态；53M 树约 6400 条路径、约 300 KB |
| 续扫时 | 重列祖先（d 次列举，不重发）；没收到 `DirectoryListed` 的块整块重列 | 正在列的目录整块重列，它们已发现的子树可能重扫 |
| 一致性 | 块边界天然一致 | “出栈”与“登记为在列”在同一把 worker 锁下；窃取先登记再出受害者栈。任何时刻每个未完成目录都在集合里，最坏重复登记（多列一次），不会漏；不需要全局暂停 |
| 开销（On） | 可忽略 | 每目录 2 次登记（非每条目），约 < 0.5% CPU；取快照每分钟约 1 ms |

- **退化情况**：一个目录有 1000 万子目录时前沿巨大（约 0.5–1 GB），检查点间隔随前沿大小自适应，或者停止发检查点并在流里明说“此后不可续扫”。
- **可接受的小误差**：检查点与崩溃点之间扫到、又在两次运行之间被删的条目，第一次写下的行会留在库里。数量不超过一个检查点间隔，与“扫描不是快照”同类。
- **删除检测**：只有最后一次运行 `Completed`，且两次运行的列举失败都已并入异常清单时才能做。

### 9.3 C11 的实现（2026-09-27）

- **接口**：`TraversalRequest::resumable(Resumable::Off | On { every })`（默认 Off；`Resumable::on()` 即 60 s），`resume_from(Checkpoint)`。
  请求不能续扫时，会话在任何 I/O 之前以 `TraversalTerminalFailure::Resume(ResumeRefusal)` 结束：`Malformed`（不是本库写的、截断或损坏）、
  `UnsupportedVersion(v)`、`Mismatch`（另一个请求的检查点）、`FilterNotResumable`。`TraversalFilter::resume_key()` 默认 `None`：
  没有键的过滤器不能开检查点也不能续扫（检查点必须知道是不是同一个过滤器）；`DslTraversalFilter` 的键是两棵解析树 `Debug` 形式的 blake3
  （相对时间如 `modified < 7d` 保持相对：隔一天续扫，窗口也随之后移）。`resume_from` 配 Off：从检查点继续，但不再发检查点。
- **取消即暂停（会话契约）**：On 时取消不再丢弃已产出的项：`next_item` 先交完已产出的，最后一项是最终检查点，然后 `None`，
  `finish() == Cancelled`。消费方读到 `None`（或 drop 会话）。会话失败时最后一项同样是检查点，`finish` 返回失败。Off 的取消行为不变。
  例外：Unordered 前沿超过上限时连最终检查点也不发，上一个检查点仍是续扫点。续扫那次的 `ScanTotals` 只计那次交出的（含重复部分），
  不是整棵树（Q15）。
- **指纹**：blake3（derive_key）覆盖格式版本、backend 种类与 `stable_id`（只哈希、不写进检查点）、root、order、`max_depth`、
  过滤器键，以及 namespace 是否列历史（C11b 起取代 `versions`：桶的版本控制在两次运行之间变了 → `Mismatch`）；不含并发、缓冲、
  观察计划、检查点间隔。
- **格式 v1（C11b 起 v2：指纹改绑 histories）**：`DMTC` | 版本 | 顺序 | 指纹 32 字节 | 条数 | 每条（与上一条共享的字节数、其余字节、`child_depth`、标志）|
  前面全部的 blake3 前 16 字节；golden 测试逐字节钉住，任何改动都升版本（C11b 删 `TraversalVersions` 时必须升），旧版本
  `UnsupportedVersion`，不兼容读（同快照的规矩）。解码时校验每条都在 root 之内（按路径分量），Blocks 的条目是从 root 起的一条链。
  上限 2^20 条 / 64 MiB（编码与解码后展开的路径总量都算，防止几十 MB 的伪造输入展开成 TB）：超了这一个检查点不发（`warn!`），
  之前发过的仍然有效。
- **Unordered**：检查点是按下标序锁住全部槽位取的前沿（栈 + `active`），去掉祖先也在其中的目录（重列祖先会再发现它们）。
  到期判定：worker 每页压栈之后、每个目录 finish 之后各看一次，一个 CAS 认领一个周期；取消 / 失败时等全部 worker 停下，
  再取最终前沿。为什么不会领先：不在快照里的目录已经 finish，finish 在它最后一批 `send_whole` 返回之后，所以它的项在检查点之前
  已进会话通道；在快照里的目录续扫时整块重列。续扫：前沿作为种子放进 0 号槽位（浅的在前，窃取者拿最大的子树）。
- **Blocks**：游标每进入一个目录记一条（该目录的块开始的序号，位置）；位置是从 root 到它的链（父指针共享，每条 O(1)）。
  重排缓冲新增 `delivered`：整次 flush 都发出去才前进，被取消截断后不再前进（`next` 在发送之前就前进，不能用它）。
  检查点 = 序号 ≤ `delivered` 的最新一条；输出落后于游标时（慢观察）检查点停在输出之前。周期检查点只在最后交出的一项是
  `DirectoryListed` 时发（不拆块、不拆一个对象的版本段）；取消 / 失败时的最终检查点可以在半个块之后，续扫整块重发。续扫：链上的每个祖先重列一次、
  不发出任何项，只保留名字（原始字节序）大于链上下一环的子目录，要观察才能定的下钻决定当场观察；末环整块重列。
  祖先现在列不出 → 这个目录的 `Listing` 失败单独先发并计数（不进任何块，下一个块的 `outcome` 仍是它自己的）；祖先已不在 →
  计入 `vanished_listings`。续扫的目录事件与完整运行从该目录起的后缀逐项相同。
- **开销（Off）**：没有 `Resume`：Unordered 每页一次 `Option` 判断、Blocks 每轮一次，不读时钟（测试：手动时钟读取次数 0）、
  不记位置。On 的实测见 §12.9：node1 53M 树上与 Off 的差别在轮间噪声之内。
- **大小**（本地实测）：Blocks 每层 13 字节 + 名字，200 层链 3.9 KB；Unordered 每个等待中的目录 13 字节 + 与上一条不同的尾部，
  3000 个子目录的宽目录 42 KB。
- **重复**：前沿里的目录整块重列，所以每次续扫最多把这些目录的子项再发一遍。本地 1237 条的树从任一检查点续扫：Unordered 平均
  重复 17–36 条，Blocks 平均 370–440 条（主要是 1074 项的宽目录整块重列）。
- **已知限制**：续扫以目录为粒度。一个目录只有在一次运行里读完才越过它：如果每次运行都在读完某个大目录之前就被取消，
  续扫永远越不过它（协议位置已在 §16 否决）。

## 10. 多版本对象：每个 key 一项

### 10.1 形状

```
struct ObjectHistory {                  // src/model/history.rs（C11b 实现）
    records: Vec<VersionRecord>,     // 服务端列举顺序反转：最旧在前，最新的一条在最后（本段内）
    order: HistoryOrder,             // Server（取自原始 XML 的位置）| Inferred（存储不守规矩，按时间推断）
    segment: u32,                    // 段号，从 0 开始（Q8：一个 key 超过 MAX_HISTORY_RECORDS = 10 万条时分段）
    complete: bool,                  // 只有这个 key 的最后一段为 true
    largest: Option<u64>,            // 整个 key 最大的一个版本（不只本段），每段都带：R3 按整个 key 判定
}
struct VersionRecord {
    version_id: Option<String>,      // 真实 id；None = "null"（未开版本控制时写入 / 暂停期间写入）
    latest: bool,                    // 列举里的 IsLatest，原样保留
    kind: Object { size, etag, modified } | DeleteMarker { modified },
    identity: SourceIdentity,        // VersionScoped(id)；"null" 为 PathScoped(路径‖ETag)；"null" 标记 PathScoped(路径‖NULL_MARKER)
    metadata: Option<Box<MetadataObservations>>,  // 按观察计划、绑定这一版观察；None = 列举之外什么都没观察；失败只记在这一条上
}
// VersionRecord::source()：拷这一版用的选择器（真实 id → Id(id)；latest 的 "null" → Current；
// 非 latest 的 "null" → Id("null")；删除标记 → None）。ObjectHistory::current()：complete 段上最后一条是对象时返回它。
```

- **一个 key 一项**：`ObservedEntry` 的路径就是这个 key，`history()` 给出全部记录（或其中一段）。目录（前缀）没有历史。
  条目层字段取**当前对象**：size / `ETag`（content version）/ 时间与身份都与 `ListObjectsV2` 给同一对象的逐字节相同
  （`PathScoped(路径‖ETag)`，所以一个桶开不开版本、身份键都不变，expert 半边的核对也照常）；最新一条是删除标记（当前已删）
  时 size 为 `None`、时间是标记的、身份 `PathScoped(路径‖"\0s3-deleted-key\0"‖标记 id 或 "null")`、列举之外无可观察。
  条目层元数据是当前版本的观察；观察失败时时间退回列举给的（C11b 评审）。
- **列出全部**：没有参数；多版本桶上一律列出每个版本和每个删除标记。最新一条是删除标记的 key（“当前已删”）**也出现**，由消费方看 `history.current()`（最新一条是对象时返回它，否则 `None`）判断现状。
- **顺序以服务端为准**，不按 LastModified 排。依据见 10.2 的实测，方案见 10.3。
- **桶怎么判定“多版本”**：连接时 `GetBucketVersioning`。`Enabled` 或 `Suspended`（暂停后旧历史仍在）→ ListObjectVersions；从未开启 → ListObjectsV2，
  `history = None`。查询被拒时一律用 ListObjectVersions（对未开版本的桶它返回每个 key 一条 `"null"`，结果正确，只是慢一些）（Q26）。
  **Q-A（用户 2026-09-27）**：501 / `NotImplemented` / `UnsupportedOperation`（存储没有版本功能）→ ListObjectsV2；瞬时失败（无响应、超时、
  429、5xx、`RequestTimeout` / `SlowDown` 等，SDK 重试之后）→ role connect 失败，`BackendSessionFailure` Connect / `Connectivity` /
  `Transient`（`BackendConnectError::session_failure()` 带给 `connect_backend` 的调用方；诊断里有 HTTP 状态或“no response”），
  不猜 —— 否则同一个未开版本的桶会因一次瞬时失败每项多一条 `"null"`，terrasync 前后两代去重键对不上；构造请求就失败（不是
  瞬时的）按拒绝处理，列举时会以它自己的失败再报。NAS profile 一律 ListObjectsV2（也不因 Unreachable 拒连）。legacy
  `S3Storage::new` 照旧宽容地连接成功。判定在连接时做一次（`S3Namespace` 取 `lists_histories()` 的快照），整个连接不变。

### 10.2 实测：MinIO 的版本顺序（2026-09-25）

**环境**：实验室 MinIO `RELEASE.2023-03-20`（10.131.9.11:9000），临时桶 `data-mover-vorder-<run>`，开启版本控制，结束时按 id 删除全部版本并删桶（两次都返回 204）。
每次写入都记录响应头里的 `x-amz-version-id` 作为**真实写入顺序**，再与 ListObjectVersions **原始 XML** 的元素顺序比较。脚本在本地 scratchpad 的 `vorder/probe.sh`、
`probe_race.sh`，实现时可以原样搬进 `.claude/skills/e2e-s3/scripts/`。

| 场景 | 结果 |
|---|---|
| A 同一 key 快速交替：PUT PUT DEL PUT DEL PUT（间隔约 13 ms） | XML 顺序 = 写入顺序的倒序；`<Version>` 与 `<DeleteMarker>` **按真实顺序交错**；`IsLatest` 在第一条 |
| B 间隔 1.2 s 的 PUT DEL PUT DEL | 同上 |
| C 两个 key 交替写 | 每个 key 的记录在 XML 里**连续**，key 之间有序 |
| D 暂停版本控制后写两次（`null`），再开启并写入、删除 | `null` 版本只保留最后一次（被覆盖），位置正确 |
| 分页 `max-keys=2` | 一个 key 的记录跨越多页，拼起来与单页完全一致 |
| LastModified 精度 | 列举里是**毫秒**（`…:57.875Z`）；`HEAD` 的 `Last-Modified` 只有**秒**（HTTP 日期格式） |
| **E 并发**：同一 key 同时发 12 个 PUT + 4 个 DELETE，3 轮（共 48 次写） | **44 条记录里有 20 个同毫秒并列（13 组）**，并列里既有版本也有删除标记。XML 顺序仍是服务端的存入顺序，LastModified 非增。**用今天的做法（两张表按 LastModified 合并、并列时版本在前）重建出来的顺序与服务端不一致**，第 4 条就错了（服务端是删除标记，合并结果是版本）。`IsLatest` 仍正确 |
| MinIO 的一个偏差 | 当前最新已是删除标记时再 DELETE，MinIO **不新建**删除标记，而是返回已有那个的 id（12 次 DELETE 只产生 8 个标记）。据我所知 AWS 每次都会新建一个。对顺序没有影响，但历史迁移到 MinIO 目标时，连续的删除标记会被合并 |

**结论**：

1. “按存入顺序、最近的在前，版本与删除标记在 XML 里交错”在 MinIO 上**成立**，而且是唯一可靠的顺序来源。
2. LastModified 即使有毫秒精度也**不够**：并发写入时大量并列。今天 `history.rs` 的合并规则在并发场景下会**排错**，这是一个现存 bug，与 P7 无关（已修：`40c1ac9`，现存问题 8）。
3. 顺序在 SDK 解析时就丢了：aws-sdk-s3 把 `<Version>` 和 `<DeleteMarker>` 拆成两个列表（`src/s3/role_protocol/versions.rs:107, 123`）。

#### 10.2.1 三家对照：MinIO / DXN（Ceph RGW Octopus）/ StorageGRID 11.5（2026-09-25，Q30 已批准）

同一组脚本，各用临时桶 `data-mover-vorder-<run>`，跑完按 id 删除全部版本并删桶（全部返回 204）。

| 事实 | MinIO 2023-03-20 | DXN（RGW，3 个网关） | StorageGRID 11.5 |
|---|---|---|---|
| 原始 XML 顺序 = 存入顺序（最新在前），版本与删除标记交错 | 是 | 是 | 是 |
| 同 key 连续、`IsLatest` 在第一条、分页拼接保序 | 是 | 是 | 是 |
| 列举 LastModified 精度 / HEAD 精度 | 毫秒 / 秒 | 毫秒 / 秒 | 毫秒 / 秒 |
| 并发写同一 key：同毫秒并列 | 44 条中 20 条 | 42 条中 17 条 | 48 条中 20 条 |
| 并发写：XML 顺序与 LastModified 是否一致（除并列外） | 一致 | **不一致**：6 处相邻记录时间“倒挂”，含版本对版本（多网关各自打时间） | 一致 |
| 今天 `history.rs` 的 LastModified 合并能否还原服务端顺序 | **不能**（第 4 条起错） | **不能**（第 17 条起错） | **不能**（第 1 条就错） |
| 最新已是删除标记时再 DELETE | 不新建，返回已有标记 | 不新建，返回已有标记 | 每次新建一个（与 AWS 一致） |
| 暂停版本控制期间 PUT 的响应头 `x-amz-version-id` | 返回 `null` | 不返回 | 不返回 |
| **只有 null 版本的 key，重新开启版本控制后写入，再普通 DELETE** | null 版本保留 | **null 版本被删掉**：列举里消失，`GET ?versionId=null` 404（AWS 语义下开启状态的普通 DELETE 只加删除标记） | null 版本保留 |
| 无请求体的 PUT（建桶） | 可以 | 需 LocationConstraint | 必须带 `Content-Length: 0`，否则 411 |

**结论（加强）**：

1. 三家都按“存入顺序、版本与删除标记交错”返回原始 XML，这是**唯一可靠**的顺序来源。
2. LastModified 在三家上都会并列；在 DXN 上甚至与真实顺序矛盾，所以 F2（改为原始 XML 位置）是必须的，不是优化。
3. DXN 的 null 版本丢失是存储自身的缺陷：作为源，它的历史里本来就没有了；作为**目标**，往 DXN 写删除标记会删掉目标上已有的 null 版本。历史迁移到 DXN 时要在报告里说明，
   或者避免在目标上对曾有 null 版本的 key 写删除标记。写进 `.claude/docs/storage-s3.md` 的 DXN 差异。
4. MinIO / DXN 会合并连续的删除标记，StorageGRID 不会；迁移到前两者时计入“目标合并了 N 个删除标记”。
5. Q22（2026-09-26 调研 + 实测）：ONTAP 9.19.1 **对象桶**在各种列举形式、页大小、delimiter、start-after 下都是 UTF-8 字节序，开后台重删 + 压缩、扫描前 / 中 / 后都不变（11,101 个 key、三轮扫描）；有文档的无序是 **NAS 桶**（KB CONTAP-675913，按目录遍历序，`/` 排在 `*` `+` `.` 前）。实验环境：FAS2750 SVM lizy 的桶 data-mover-test / data-mover-test-ver（见 e2e-s3 的 ontap/.env）。**NAS 桶实测（用户建议，2026-09-26）**：在 lizy 上建 NAS 桶 data-mover-nas（FlexVol dm_s3_nas，重删 + 压缩），279 个特意挑选的 key：不带 delimiter 的平铺列举**不是字节序**，而是按路径逐段排序（`/` 最小，整棵 `a/` 子树排在 `a*b` `a+b` `a.b` 前）—— 目录遍历序，页大小 1000 / 50 / 7 下一致、无重复；带 delimiter 逐层列举每层有序、递归走完 279 个不漏；NAS 桶不支持 ListObjectVersions。它作为“无序 S3”的真机环境加入矩阵。

### 10.3 方案

1. **从原始响应体取顺序**：给 ListObjectVersions 加一个读取原始响应体的拦截器（代码里已有拦截器的先例：`src/s3/dxn.rs` 的 `DxnCompatibilityInterceptor`），
   按文档顺序解析 `<Version>` / `<DeleteMarker>`，得到 `(key, versionId, 位置)`。XML 解析器已在依赖树里（`aws-smithy-xml`、`quick-xml`、`roxmltree`），
   不需要新依赖；是否把其中一个提升为直接依赖，实现时确认。SDK 解析出的字段照用，只用原始 XML 补上“位置”。
2. **每个 key 的历史 = 该 key 的记录按服务端位置反转**（最旧在前、最新的一条在最后），**完全不用 LastModified**。
3. **边走边校验**：同一个 key 里 `IsLatest` 必须恰好一条且在最前；同一个 key 的记录必须连续；分页拼接后位置单调。任何一条不成立，说明这个存储不按规矩给顺序（用户遇到过的那种）
   → 该 key 退回“按 LastModified 推断、`IsLatest` 放最后”，并在 `ObjectHistory` 上标 `order: Inferred`（正常为 `order: Server`），让消费方知道这段历史的顺序是推断的。
4. **分页**：位置按“页号 + 页内序号”累计，跨页的 key 自然拼接；有序存储上可以只攥住当前 key，无序存储上按目录物化（第 5.3 节）。
5. **测试**：
   - 单元测试：合成 XML（交错、同毫秒并列、跨页、`IsLatest` 不在最前、同 key 不连续）。第一条就是把 10.3 场景 E 的真实响应存为 fixture：今天的实现在它上面必须失败，新实现必须通过。
   - e2e：把 `probe.sh` / `probe_race.sh` 并入 e2e-s3，在 MinIO 上每次跑。
   - DXN（Ceph RGW Octopus）与 StorageGRID 11.5 上跑同一组：**已完成（10.2.1）**，脚本并入 e2e-s3 后三家都跑。
6. **目的端**：写历史到 MinIO 时，连续的删除标记会被合并（10.3 最后一行）。这属于目标存储的限制，拷贝报告里记为“目标合并了 N 个删除标记”，不算失败。
   重建用 H1 的原语 `transfer::write_delete_marker`（只在 `Enabled` 的桶上、每次调用前查）：结果 `Written` / `Merged`
   （MinIO、DXN 合并连续标记）/ `NullVersionRemoved`（DXN 删 null 版本；RustFS 不给 `d/` 记版本，删除即删数据），都显式报出。

### 10.4 要处理的后果

| 后果 | 处理 |
|---|---|
| 单 key 版本数没有上限（一个频繁覆盖的对象可以有上百万版本），一项可能极大 | 单 key 记录上限 10 万条；超过时**分段**：一个 key 发多项，每项带段号和 `complete: false`，最后一段 `complete: true`（Q8 已定）。**C11b 实现**：分段在输出这一环（`tally_child`），列举、filter、观察、计数都按 key 做一次；各段同一个条目（同一身份键、同样的条目层字段），在同一个 batch 里发出（Unordered `send_whole`，Blocks 单一发送方），所以会话里连续、有序。续扫（C11）：Blocks 取消时 flush 被截断则 `delivered` 不前进，续扫整块重列、各段从第 0 段重发；Unordered 正在列的目录在前沿里、整个重列。契约：**一个 key 的历史收到 `complete=true` 那一段才算完整；续扫后从第 0 段重发** |
| 无序存储上一个 key 的记录可能分散在任意页 | 按目录物化后组装（O(目录)，上限 500 万条记录，Q9）。有序存储本可以只攥住一个 key，但无法事先知道存储是否有序 |
| 一项的观察可能要做很多次 I/O（每个版本单独读 tags / 元数据） | 只在观察计划要求时才做；每条记录的观察结果与失败各自独立，一条失败不让整项失败；一项内的 I/O 受同一个并发窗口约束。**C11b 实现（R2）**：一项内逐条串行（占遍历的同一个观察名额），绑定各自版本（`observe_copy_bound_version`，`VersionRecord::source()` 选版本），失败记在那条记录上（`MetadataObservation::Failed`，`Required` 也一样，NotFound 也不算 vanish），删除标记不观察，会话级失败照常结束遍历、节流按条目限定，取消即停（不交出）|
| 拷贝 | 消费方按 `records` 顺序逐条处理：对象版本用 `record.source`（`SourceVersion::Id`，最新的 `"null"` 为 `Current`）交给 `TransferRequest::with_source_version`；删除标记用 `transfer::write_delete_marker`（H1，只在 `Enabled` 的目标桶上；未开 / 暂停版本的目标只拷最新状态并记原因“目的桶未开版本，历史未重建”，D12）。一个 key 严格串行，重放不去重（D11）；参考 `examples/s3_history_replay.rs` |
| 身份与改名识别 | 项的 `source_identity` 是 key 级的 `PathScoped`；每条记录另有自己的 `VersionScoped` 身份，拷贝前按记录核对 |
| 快照 | **用户 2026-09-27 定：条目快照不考虑版本兼容**。C11b 只保留一种当前格式（版本字节升为 7，带历史的条目带完整 ObjectHistory 段，不带历史的条目同一格式），删除 v4 / v5 / v6 的编码与解码分支；旧快照解码报 `UnsupportedVersion`，调用方按首次扫描处理。守则：① 保留 `DMES` 魔数与版本字节；② **格式任何变化都把版本字节加 1**（哪怕只加一个字段），并有 golden 字节测试（当前格式的编码与存下的字节不符即失败），防止同一版本号下新旧字节混存而被静默读错；③ 这是开发期规则，正式发布前重新决定（至少解码 N-1，或给 terrasync 迁移 / 重扫说明），写进 ADR-0008。目的端恢复指针 / binding 格式（ADR-0006）不在此列：它们关系在途传输能否续传，开发期也可以不兼容，但每次变更的 commit 要写明“升级前排空在途传输” |
| 计数 | `listed` / `emitted` 按 key 计；另加 `versions`（对象版本数）与 `delete_markers`；`bytes` 分为“当前版本字节”和“全部版本字节”。**C11b 实现**：`BlockCounts` / `ScanTotals` 加 `versions`、`delete_markers`、`all_versions_bytes`（没有历史的文件记它自己的大小，所以存储没有历史时它等于 `bytes`）；`bytes` = 当前版本字节（当前已删的 key 为 0）；分段之前对整段历史计一次，listed / emitted / filtered / files 按 key，不按段 |
| 目录对象的历史（R3） | `directory_object_action`：目的端能存目录对象时，任何一版超过单次请求上限就整个 key `Skip(TOO_LARGE)`（按 `ObjectHistory::largest_version`，每段都带整个 key 的最大值，所以各段答案相同 —— 重放中途停下会在目的端历史里留缺口，D11）；目的端是文件系统时看当前版本（没有或 0 字节 → `NothingToDo`，有内容 → `Skip`）；非最后一段 `NothingToDo`（由最后一段决定） |
| 已知限制（C11b） | ① 版本化桶上单目录超过 500 万条记录现在报 `Capacity`（Q9），今天的 Current 流式在版本化桶上没有这个上限 —— 对版本化桶是一次能力倒退，要放开等真实需求。② 只有 `s3:ListBucket`、却被拒 `GetBucketVersioning` **和** `ListBucketVersions` 的主体（常见的最小权限策略）：按 Q26 列历史，每个目录的列举都会权限失败；C11b 之前默认的 Current 遍历对它可用（评审指出，待定是否加回退，见 §14 C11b 行）。③ 物化期间整个目录的原始记录驻留内存，Blocks 预取最多 2 × min(c, 64) 个列举（与之前 All 相同） |
| terrasync 落库 | 把一项展开成每版本一行，沿用今天 base 表的排序键 `(relative_path, version_id, identity_key)`（`clickhouse.rs:569`）与去重（Q27 已定） |

## 11. 与 terrasync 写库的配合（ClickHouse）

统计准不准，主要取决于批量写库。以下基于 terrasync-rs `8edd400`（10.131.9.10）。这些改动在 terrasync 里做，本设计只列要求（Q16）。

### 11.1 现状里对的部分

- 写入用 `async_insert=1` + `wait_for_async_insert=1`（`crates/db/src/clickhouse.rs:326-327`），`end()` 返回时服务端已 flush，**确认过的批不会丢**。
- 增量扫描每批一张新临时表：插入 → 与 base 比较（`NOT IN base`，`clickhouse.rs:200-219`）→ 并入 base（`ReplacingMergeTree`，`:569`）
  → 删表（`app/src/scan.rs:823-850, 889-942`）。续扫重扫的条目、已并入后的重试都落在后面的批里，比较时 base 里已有且未变，不产生事件，并入时被去重。**所以临时表不用改**。

### 11.2 会丢数据或让统计失真的地方（都属于“批没写下去，进度却前进了”）

| 现状 | 后果 | 改法 |
|---|---|---|
| 全量扫描批写重试 3 次（100/200/400 ms，约 0.7 s，`consumer/database.rs:19-21, 52-75`）后**丢弃这一批**（`:395-401`）；`database_consumer_result` 只对增量扫描报错（`:40-48`） | 全量扫描静默丢行，任务仍成功 | 重试耗尽 = 失败；退避按分钟计（能跨过一次服务端重启），期间背压传回遍历 |
| 崩溃时内存里的未满批（每 worker ≤ `batch_size`）与通道缓冲（每消费者 1000）丢失，没有持久进度点 | 只能整场重扫 | 检查点写进状态表，**只在它之前的所有批都确认后才写**；收到检查点先 flush 未满批；并发批按“未确认数归零”判定 |
| 数据库消费者退出后，广播对它的发送失败被静默忽略（`broadcast.rs:36-44`） | 后面的项全没落库，扫描却继续“成功” | 数据库消费者退出 = 整个扫描失败 |
| 统计按广播逐条累加，与写库结果无关（`stats/accumulator.rs:265-272`） | 丢批时统计仍全数计入 | 最终统计用 SQL 在 base（`FINAL`）上聚合；时间分桶用扫描开始时间；内存累加只做进度条 |
| 增量扫描临时表插入不重试（`scan.rs:853-883`） | 一次瞬时错误就不能提交 generation | 重试时**换一张新临时表**（先删再建），不往半成功的表里再插 |
| 全量扫描（`JobType::Scan`）直接写 base，没有 generation 闸门 | 崩溃留下半场数据 | 与增量扫描一样，写完并校验后再生效（Q17） |
| 示例 `batch_size = 100`（`utils/src/app_config.rs:443`） | 53M 条约 53 万次 INSERT | 调到 1 万–10 万 |
| docker 镜像未固定版本（`docker/docker-compose.yml:5`） | 行为随版本漂移 | 固定版本 |

### 11.2a 目录对象（F5）对 terrasync 的要求

- `relative_path` 逐字节保存，**不去掉末尾 `/`**：`p/d/`（目录对象）与 `p/d`（文件或目录）是两行，身份键也不同；删除检测
  按 §4.1 第 6 条比较。消费 F5 之前先查清 terrasync 自己的路径处理（拼接、`Path` 规范化、去重键）。
- 每个条目先过 `transfer::directory_object_action(&entry, &destination_storage)`（传目的端 `Storage`；只传
  `NamespaceCapabilities` 时不知道单次请求上限）。目的端是文件系统：零字节或最新记录是删除标记 → 无操作；有内容 →
  跳过并记 `DIRECTORY_OBJECT_SKIP_REASON`（不算失败）。目的端是 S3（D13）：> 5 GiB 或大小未知的目录对象 → 跳过并记
  `DIRECTORY_OBJECT_TOO_LARGE_REASON`（与引擎 Preflight 拒绝的诊断相同，不算失败），不必等传输失败才知道。不要自己拼
  旁车文件名（任何名字都可能与真实的键冲突）。
- 历史重放（D5 / D11）：按记录旧→新，版本走 `TransferRequest::with_source_version`，删除标记走
  `transfer::write_delete_marker`（H1），先用 `transfer::destination_keeps_history` 判断目的端能否存历史，一个 key 严格串行；重放不去重，重跑前比对目的端历史。

### 11.2b S3 URL 参数化（P1–P5）对 terrasync 的要求

- 配置里的 S3 URL 要迁移：`s3+sg://` → `s3://…?compat=sg`，`s3+dxn://` → `s3://…?compat=dxn`，`s3+hcp://` → `s3://…`（HCP 支持已删除，按标准 S3），
  `s3+http://` → `s3://`；旧 `s3+sg+https://` / `s3+dxn+https://` → `s3+https://…?compat=sg|dxn&tls_verify=false`。旧写法连接时报
  `UrlParseError`，报错给出新写法。
- `s3+https://` 现在**校验证书**：自签 / 私有 CA 端点要么把 CA 放进 `SSL_CERT_FILE` / `SSL_CERT_DIR`，要么 URL 加 `tls_verify=false`
  （每连接告警）。未知参数不再被忽略（改前带桶 URL 的查询串被丢弃），拼 URL 的地方不能再往查询串里塞别的东西。
- 参数不进端点身份，已落库的身份键、恢复绑定不受影响；terrasync 自己若把 URL 文本当键，需要按新写法重新规范化。

### 11.2c 多版本（C11b）对 terrasync 的要求

- **每个 key 一项**：版本化桶上一个 key 是一个条目，带它的历史（`ObservedEntry::history()`）；当前已删的 key 也在
  （`history.current()` 为 `None`）。落库时把一项展开成每版本一行（Q27），沿用 `(relative_path, version_id, identity_key)`。
  按条目发起 `TransferRequest` 的调用方先看 `source_version()`（没有当前对象为 `None`），否则会在已删的 key 上 describe 到
  `NotFound`。
- **快照按 (路径, 段号) 存一份**，不要每个版本行各存一份：一段 10 万条记录的快照约 15 MB。
- **分段**：一个 key 超过 10 万条记录时发多项（同一路径、同一身份键，段号从 0，只有最后一段 `complete=true`）。消费方
  **只有收到 `complete=true` 那一段才把这个 key 的历史当完整**；取消 / 续扫后各段**从第 0 段重发**（已落库的段会再来一次，
  按上面的去重键吸收）。
- **快照格式 v7**（开发期规则：只读当前格式）：所有 backend 的 v4 / v5 / v6 快照都解码为 `UnsupportedVersion`，下一次增量扫描
  按首次扫描处理（全量重扫一次，不只 S3）。C11 的检查点 v1 同样失效（`UnsupportedVersion(1)`），从头扫。
- **连接**：role connect 在 `GetBucketVersioning` 瞬时失败时报 `Connectivity` / `Transient`（`session_failure()`）：重试连接，
  不要当配置错误。
- **计数**：`bytes` 现在是当前版本字节；全部版本的字节是 `all_versions_bytes`，另有 `versions` / `delete_markers`。

### 11.3 可选

- 列举本身的同批重复（S3 跨页重复对象、NFS 变化中的目录在 cookie 边界重复一项；NFS 续读后服务器重发的已由列举按 `(fileid, 名字)` 跳过，C7c）很少见，会多发一次 New；
  要消掉只需在检测 SQL 上加 `LIMIT 1 BY t.relative_path, t.version_id`（代码里已有这种写法，`clickhouse.rs:1216`）。
- 提交前交叉校验：data-mover 报告的 `emitted` 与本轮写入行数对得上才提交。

### 11.4 generation 闸门是什么（Q17）

**generation（代）**：terrasync 的 base 表每一行带一个 `current_state`（0 或 1），表示这一行属于哪一次扫描；状态表（`state_<job>`，`ReplacingMergeTree`，一行）
记着**已提交的是哪一代**。

**增量扫描今天的流程**（`crates/app/src/scan.rs:34-83, 484-716`，`crates/db/src/clickhouse.rs:724-738, 1059-1093`）：

1. `begin_scan_generation`：读出已提交代 c，本轮工作代 w = 1 − c；先把上一轮没提交留下的 w 行改回 c（`rebase_uncommitted_generation`）。
2. 扫描：每批进临时表，与 base 比较得出新增 / 变更，再并入 base，行标记为 w。
3. 任何失败（walk 报错、批写失败、取消、消费者失败）都会把 `IncrementalScanSafety.snapshot_complete` 置为 false。
4. 只有 `snapshot_complete` 仍为 true 时才做删除检测（base 里仍是旧代 c、本轮没刷新到的行 = 已删除），并调用 `commit_scan_generation`：把 w 写进状态表，从此 w 就是“当前”。
5. 否则**不提交**：状态表仍指向 c，本轮的结果不生效，下次 begin 时回滚。

这就是**闸门**：“这次扫描完整、所有写入都成功”之前，它的结果不会成为新的基线，也不会触发删除。

**全量扫描没有这道闸门**：

- `run_full_scan`（`scan.rs:333-417`）不调 begin / commit，数据库消费者把行直接写进 base（当前代 0）。
- 取消时也走到 `“Scan job … completed successfully”` 并返回 `Ok`（`scan.rs:398-417`）。
- 批写失败会被丢弃且不报错（11.2 第 1 行）。
- 下一次运行时，`determine_scan_type` 只看 base 表是否存在（`scan.rs:138-156`），存在就走增量。于是**一次只扫了一半的首次扫描会被当成完整的基线**：没扫到的部分在下一次增量里全被当成“新增”，
  它们的统计也从那一次开始才算对。

**建议**：全量扫描同样 begin / commit。首次扫描没提交时，状态表记“无已提交代”，`determine_scan_type` 据此判为“仍需全量”（开了续扫时从检查点继续）；取消或失败如实返回错误。

## 12. 性能

### 12.1 C0：先修每条目一次的跨线程交接（不依赖 P7）

- 无 I/O 的观察（`needs_metadata_role() == false` 且非 symlink）在列举任务里逐页构造 `ObservedEntry`，驱动收到“已观察的页”，每页一次交接；需要 I/O 的（ACL / xattr / tags、
  symlink、S3 带版本观察）才 spawn。
- 重排缓冲快路径：序号正好是下一个时直接发，不进 `BTreeMap`（`output.rs:139-162`）。
- 身份的分配：`BackendIdentity` 改为 `Arc` 共享，稳定字节内联（见 5.4.6）。
- C0b：列举已带 mtime 时 filter 立即决定（`storage.rs:369-371` 今天只要 `needs_modified()` 就一律 deferred）。值与观察得到的完全相同；
  DSL 主线的 deferred 决定从约 N 降到约 0。

### 12.2 估算（推测，未测）

| 53M 树 | CPU | RSS |
|---|---|---|
| 今天（`8a1348b` 后） | 约 700 s | 约 180 MB |
| C0 后 | 约 330–420 s（futex 1.9 → 约 0.3–0.5 / 条） | 下降，原因待探针与 `MALLOC_ARENA_MAX=2` 查明 |
| Blocks 流式后 | 与 C0 相近（这棵树每目录 35 项，流式对它几乎没有 CPU 收益；对宽目录是数量级的内存收益） | 同上 |
| Unordered | 再省约 5–10%（记账、缓冲、事件），且没有头阻塞 | 与 legacy 同量级 |

### 12.3 Unordered 相对 legacy 仍然存在的成本

- `ObservedEntry` 构造：`SourceIdentity` + blake3 身份键（剖面约 1%）、元数据观察结构、`Box`、路径拼接、backend fact 拷贝。这是契约的一部分（快照、身份），不能省。
- 条目通道每项一次 send：与 legacy 相同。

### 12.4 宽目录（1000 万文件平铺）

|  | 今天 | P7 后 |
|---|---|---|
| 驻留 | 约 4.5 GB（每条 400–500 B），预取最多 2P+1 份同时驻留 | O(页) |
| 首项 | 整个目录列完（S3 1 万页 × 约 50 ms ≈ 8 分钟） | 首页之后 |
| 1000 万个子目录 | 槽位约 1.5 GB | 不变，归 P8/P9 |

### 12.5 组装 `ObservedEntry` 的并发

**结论**：可以并发，而且应该并发，但**并发的粒度必须是“页”，不能是“条”**。今天慢的原因恰恰是按条并发。

| 每条目的成本（估算，未测） | 量级 |
|---|---|
| 组装本身：身份（克隆 `BackendIdentity` 字符串、分配稳定字节）、blake3 身份键、元数据观察结构、路径拼接、`Box` | 约 0.5–1.5 µs |
| 今天为它付出的跨线程交接：每条 spawn 一个任务 + `join_next` 唤醒驱动（每条约 1.9 次 futex） | 约 5–10 µs |
| 实测总账（53M 树） | 角色版约 13.6 µs CPU / 条，legacy 约 4.5 µs / 条 |

交接比工作本身贵好几倍，所以按条并发越多越慢。

#### 按页并发（C0 的做法）

- **组装放在列举的生产者任务里**：每个列举一个任务，页读到后在同一个任务里把整页组装成 `ObservedEntry`，再**整页**交给驱动。每页一次交接，而不是每条一次。
- **并行度来自多个目录同时列举**：
  - Unordered：K 个 worker 各自列举、各自组装，天然 K 路并行，没有单点。
  - Blocks：在飞的预取列举（≤ P 个）各自组装，驱动只负责排序号、发出。
- **条目通道也按页**：内部通道传 `Vec<TraversalItem>`，`TraversalSession::next_item()` 从缓冲的页里逐条取，对外接口不变，通道操作从每条一次降到每页一次。
- **需要额外 I/O 的条目**（`should_scan` 里的 ACL / tags 等）：本来就是 I/O 等待，按条并发是对的，但不必每条 spawn。改为每页一个任务，里面用 `FuturesUnordered` 并发发这一页的请求，
  受 `max_inflight_operations` 约束。

#### 还可以更进一步，但先量再做

- **单个超大目录**（例如千万级平铺目录）在 Blocks 下只有一个生产者，组装是串行的。按约 1 µs / 条算，一个核每秒能组装约 100 万条；NFS 一页约 300 条要一次往返，列举本身更慢。所以通常不是瓶颈。真成为瓶颈时，把一页切成几块、
  分给几个任务组装，按块号还原顺序。
- **降低单条成本**比加并发更划算：`BackendIdentity` 改为 `Arc`、稳定字节内联（5.4.6）、页内共享父路径（P9）。
- **快照编码**（`encode_snapshot`）在消费方（terrasync `ObservedEntryRecord::capture`）里做，由写库的批量 worker 并发，不占遍历的线程。
- **并发过度的风险**：线程越多，分配器的 arena 越多（178 MB 的首要怀疑对象），Blocks 的重排缓冲也越大。以 node1 的实测（CPU、RSS、futex / 条）决定，不凭感觉加。

**验证**：C0 之后在 node1 上用 `strace -c` 看 futex / 条（目标约 0.3，与 legacy 相当），并加一个组装单条耗时的微基准，确认 0.5–1.5 µs 的估算。

### 12.6 性能目标与达不到时的优化路线

> **要求（用户定的）**：Unordered 与 legacy **基本持平**；Blocks 不能差太多；差距大就找优化方案。

| 指标（node1、m1-source 53M 树、c=64、与 legacy 交替四轮取均值） | Unordered | Blocks |
|---|---|---|
| wall | ≤ legacy × 1.05 | ≤ legacy × 1.15 |
| CPU（user + sys） | ≤ legacy × 1.15 | ≤ legacy × 1.4 |
| RSS 峰值 | ≤ 30 MB（legacy 15 MB） | ≤ 100 MB，且不随树规模增长 |
| futex / 条（strace -c） | ≤ 0.4（legacy 0.30） | ≤ 0.5 |

**比较口径**：两边的消费方做同样的事（只计数、不落库），同一棵树、同一台机器、交替运行，防止缓存偏袒一方。

**诚实的预估**：legacy 的 CPU 只有 24%，是 I/O 受限的，wall 持平可以做到。CPU 持平更难：角色版每条多做的“契约内”工作（身份键 blake3、元数据观察结构、快照所需字段）约 1–1.5 µs，53M 条约多 55–80 s，
相对 legacy 的 240 s 是 1.2–1.3 倍。所以要做到 CPU ≤ 1.15×，下面的 O3 大概率需要。

**达不到时，按预期收益从高到低**：

1. **O1 按页组装、按页交接**（C0，12.5）：消掉每条一次的 spawn 和跨线程交接。这是现在 3 倍 CPU 的主因。
2. **O2 减分配**：`BackendIdentity` 用 `Arc`；稳定字节内联小缓冲；页只带名字、页内共享父路径（D2 / P9 方向）；复用页缓冲。
3. **O3 把与契约相关、但扫描本身用不到的计算推迟到用到时**：身份键改为首次调用 `identity_key()` 时计算并缓存；这样只计数或只按路径处理的消费方不付 blake3，写库的消费方在自己的批量 worker 里并行计算。
   这推翻了 5.4.6 “构造时计算”的建议，只在 O1、O2 后仍不达标时采用。
4. **O4 分配器**：先试 `MALLOC_ARENA_MAX=2`（零代码）；仍有问题再评估 mimalloc / jemalloc（新增依赖，需要单独决定）。
5. **O5 Blocks 专用**：序号按页分配而不是按条（一页一个序号区间），重排缓冲按页存；预取窗口按实测调（深链 + 宽树两种形状）。
6. **O6 条目通道**：若 futex 仍高，`TraversalSession` 增加 `next_page()`，让消费方整页取，免去逐条唤醒。

**剖析手段**：node1 装不了 perf，用 `strace -c` 数系统调用；另加一个 feature 门控的内部计数器（每阶段耗时：列举、组装、交接、发出），以及单条组装的微基准。每一步优化都要在 node1 上复测并记录，达标即停。

### 12.7 实测（2026-09-26，node1，200G 链路，c=64，交替各两轮）与内存根因

| 版本 | wall | CPU | RSS | futex / 条 |
|---|---|---|---|---|
| legacy | 973 s | 237 s | 14 MB | 0.28 |
| role（C0 前，40c1ac9） | 1024 s（1.05×） | 685 s（2.90×） | 165 MB | 2.38 |
| role（C0 后，03d5c28） | 1052 s（1.08×） | 357 s（1.51×） | 175 MB | 0.46 |
| C0 + `MALLOC_ARENA_MAX=2` | 1071 s | 462 s | 122 MB | — |

C0 达到了自己的目标（每条 futex 2.38 → 0.46，CPU 约 12.8 → 6.7 µs / 条，legacy 约 4.4 µs）；Blocks 门 wall、futex 过，
**CPU（1.51×）与 RSS（175 MB）未过**。每目录 RPC 与 legacy 相同（约 1 次 sendto）。

**内存根因（诊断构建确认）**：不是遍历，也不是分配器 arena，而是 NFS 的全局“路径 → 文件句柄”缓存
`GLOBAL_CACHE`（`src/nfs.rs`，moka，容量 50 万，闲置过期按距根深度 2 h / 10 min / 10 s）。8a1348b 起
`list_role_entries_once` 把 READDIRPLUS 回来的每个子目录句柄都写进去，3.37M 个目录全部过一遍；legacy 从不写。
整树上缓存稳定在约 13.5 万项、活堆约 92 MB（约 625 B / 目录），另有 3.37M 项创建又过期造成的碎片约 99 MB；
RSS 从不封顶（21 → 192 MB）。子树 `d000`（16.8 万目录）即达 114–124 MB，与并发、arena 数无关。
上限压到 4096 项时 RSS 平在约 43 MB，但多 11% 的字节（补 LOOKUP）、wall +16%、CPU +36%，只证明原因，不是解法。
其余候选（游标栈、预取列举、重排缓冲、条目通道）都有界且与树规模无关。
另：`lookup_fh` 与 `create_dir_all` 把零拷贝解码出的 `obj.fh` 原样存进缓存，每项钉住整个约 250–300 B 的 RPC 回复
（列举路径已 `Bytes::copy_from_slice`），迁移的目的端同样会涨 —— 作为独立修复 **F3** 先做。

**定的修法（A）**：子目录句柄随工作项下传，不经缓存。列举给每个子目录带一个 backend 私有的、拷贝出来的下钻提示
（NFS 是 ≤ 64 B 的文件句柄，绝不引用 READDIRPLUS 页），`ListRequest` 增加可选的同类提示；有提示就直接 READDIRPLUS，
没有或已失效（`NFS3ERR_STALE` / `BADHANDLE`）才回退 `lookup_fh(path)`；列举不再写 `GLOBAL_CACHE`（变更路径仍可用它）。
内存变为“待处理目录数 × 约 64 B”，与 legacy 同形。落在 C7（NFS 侧）与 C4 / C10（调度器携带提示）。
**已实现** `7da78ce`（NFS 侧；C4 的载体 `e86f87b`）：提示 = 8 字节命名空间标签 + 句柄拷贝（≤ 128 B），别的命名空间的提示不读；
失效还包括 `NFS4ERR_STALE` / `FHEXPIRED`；`ndx_walk` / `delete_tree` 暂不带提示（每目录一次 LOOKUP，仍经 `lookup_fh` 缓存）。
node1 整树的 RSS 复测归 Phase 6 的性能门。
预计 Blocks ≤ 100 MB 容易达到；Unordered ≤ 30 MB 还可能要 O2 / O4（启动约 20 MB，其中 12 MB 是二进制映射）。
CPU 的差距（1.51× 对 ≤ 1.4×）按 O2 → O3 在 Phase 6 处理（C10 已处理：O1 的延伸 + O2 即达标，见 §12.8）。原始数据：node1 `/root/dm/p7-base/logs`、`/root/dm/p7-rss/`。


### 12.8 C10 实测：Phase 6 性能门（2026-09-27，node1，200G 链路，c=64，`--quiet`，与 legacy 交替四轮）

二进制：legacy = `/root/dm/p7-base/legacy`（`examples/nfs_walkdir`），role = C10 的 release `storage_role_operations`
（10.131.9.10 上编译 + glibc 2.34 shim，node1 `/root/dm-c10/`）；`--order unordered` / `--order blocks` 同一个二进制。
futex / 条：`strace -f -c` 跑子树 `d019/d000`（126,735 条，先热一遍）。原始数据：node1 `/root/dm-c10/logs/`。

| 顺序 | 轮 | wall 均值 | CPU (user+sys) 均值 | RSS 峰值 | futex / 条 | 条目数 / 失败 |
|---|---|---|---|---|---|---|
| legacy | 4 | 996 s | 254 s（234–266） | 16.5 MB | 0.290 | 53,368,420 / 0 |
| Unordered | 4 | 995 s | 264 s（257–273） | 18.5 MB | 0.296 | 53,368,420 / 0 |
| Blocks | 4 | 1075 s | 296 s（272–309） | 30.5 MB | 0.337 | 53,368,420 / 0 |

比值（对 legacy 均值）：Unordered wall 0.999×、CPU 1.04×；Blocks wall 1.079×、CPU 1.17×。

| 轮次 | legacy wall / CPU | Unordered wall / CPU | Blocks wall / CPU |
|---|---|---|---|
| r1 | 995 / 266 | 992 / 273 | 1027 / 309 |
| r2 | 995 / 234 | 994 / 265 | 1096 / 303 |
| r3 | 996 / 248 | 995 / 257 | 1021 / 299 |
| r4 | 998 / 266 | 998 / 262 | 1156 / 272 |

对照 12.6 的目标：

| 指标 | Unordered（目标） | Blocks（目标） |
|---|---|---|
| wall | **1.00×**（≤ 1.05×） | **1.08×**（≤ 1.15×） |
| CPU | **1.04×**（≤ 1.15×） | **1.17×**（≤ 1.4×） |
| RSS | **18.5 MB**（≤ 30 MB） | **30.5 MB**（≤ 100 MB，且 C7 起不随树规模增长） |
| futex / 条 | **0.296**（≤ 0.4；legacy 0.290） | **0.337**（≤ 0.5） |

全部达标。legacy 自己的 CPU 在四轮间就差 14%（234–266 s），所以单轮比值不可靠，只看交替四轮的均值。

**走到这里用了哪些优化（12.6 的路线）**：
- C10 的第一个原型（O1 = C0 的按页组装之上，没有别的优化）：Unordered wall 1.00×、CPU **1.34×**（318 s 对 237 s），Blocks
  wall 1.09×、CPU 1.34×。10.131.9.10 上经 1GbE 对 m1-source 子树做 perf（`perf record -F 999 -e cpu-clock:u`，legacy 对照，
  用户态 CPU 17.3 s 对 24.8 s）找到两块：
  1. **每个目录一个生产者任务**（O1 的延伸，`511cee5`）：3,368,421 个目录、平均 15.8 条，每个目录一次 spawn、一个 channel、
     生产者 ↔ 消费方几次跨线程唤醒（tokio 的 steal / 经 epoll 的 unpark 在剖面里一大截；legacy 的 worker 直接在自己的任务里发
     READDIRPLUS）。改为生产者先在消费方的任务里推进，交出第一项后仍未结束（宽目录）或流被 drop 时才转成 detach 的任务：
     单页目录不另起任务，宽目录照旧后台预读一页；任何时候不 drop / abort 半途的生产者。
  2. **每条目多出的约 5 次字符串分配**（O2，`0517c68`、`ad2dfec`）：NFS 的 role 列举先造 legacy `NASEntry`（名字、`PathBuf`
     拼接、扩展名）、再 `to_string_lossy` 拷一份路径、`respelled` 前再 clone、`is_internal` 解析 `std::path`，列举契约的名字
     `to_owned`，遍历里再 `child_path` 拼一次。改为直接从 READDIRPLUS 项描述、路径移交不拷贝、名字沿用路径的分配、拼完整路径就地
     拼进名字的分配。
  两项之后第一轮 Unordered CPU 241 s（同场 legacy 271 s，前一场 237 s）、sys 从 76 s 降到 56–62 s（跨线程唤醒少了）。**O3（身份键推迟计算）、O4（分配器）、
  O5、O6 都不需要**：blake3 在剖面里约 0.5 s / 2.7M 条（约 2%），留在构造时计算（5.4.6 的建议不变）。
- Blocks 同样受益（它也用同一个列举生产者与 NFS 描述）：CPU 1.51×（C0）→ 1.17×，RSS 175 MB（C0）→ 30.5 MB（C7 的下钻提示）。

**rebase 到 `d222738`（含 C7c 的 cookie 续读与 `(fileid, 名字)` 去重摘要）之后复测**（2026-09-27，同样的 node1 / m1-source / c=64 / `--quiet`，
交替两轮；二进制 = rebase 后的 C10，node1 `/root/dm-c10/c10-rebased`）：

| 顺序 | wall（两轮） | CPU user+sys（两轮） | RSS 峰值 | futex / 条（d019/d000） | 条目数 / 失败 |
|---|---|---|---|---|---|
| legacy | 996 / 996 s | 246 / 261 s（均 254） | 16.2 MB | 0.292 | 53,368,420 / 0 |
| Unordered | 996 / 996 s（1.00×） | 277 / 235 s（均 256，**1.01×**） | 17.5 MB | 0.306 | 53,368,420 / 0 |
| Blocks | 1108 / 1135 s（**1.13×**） | 319 / 327 s（均 323，**1.27×**） | 26.5 MB | 0.335 | 53,368,420 / 0 |

§12.6 的目标都仍达标（Blocks wall 1.13× 离 1.15× 最近）。单轮 CPU 在 node1 上波动很大（Unordered 235–277 s），为了把 C7c 去重摘要的
代价单独拿出来，在 10.131.9.10 上经 1GbE 对子树 `d019`（2,694,735 条）做了交替四轮 A/B：rebase 前后的两个二进制除 C7c（与同批上游改动）
外相同 —— Unordered CPU 25.40 → 24.94 s，Blocks 30.36 → 30.25 s，RSS 相同；**差别 < 2%，在噪声内**。node1 上 Blocks 两轮比
§12.8 上面四轮的均值高约 9%，A/B 不支持这是 C7c 造成的，归 node1 / NFS 服务器的轮间波动。原始数据：node1 `/root/dm-c10/logs/q*`。

**C10 的真机（遍历 smoke，Unordered c = 8 / 64、Blocks 与 base `10ccb8f` 的 Blocks 比条目集合与计数）**：Local；ONTAP FAS2750
NFSv3（10.131.9.10，root）/ v4.0 / v4.1 整个导出 5399 条（v4.0 那次别人的 `nfs-rs-readdir-page-*` 目录正在被写，比对时排除它）；
DXN-2 NFSv3 / v4.0（播种 1511 条，测后删除）；CIFS FAS2750（播种 18,326 条，c = 1 / 4 / 32，e2e-cifs 全过；停车句柄：10 × 4500 的
子树 stdout 堵住时 Unordered c=4 恒 4、c=32 恒 8，Blocks 恒 8，结束 0）；S3 `traversal_versions.sh`：MinIO 临时版本桶与
data-mover-test、ONTAP 对象桶两种与 NAS 桶、RustFS 临时版本桶与 data-mover-test、DXN 临时版本桶与 data-mover-test、StorageGRID
data-mover-test 全部 pass（Unordered 与 Blocks 条目相同、每 key 版本连续、无目录事件）；StorageGRID 版本桶那次脚本在写入之前的
前缀预检列举就失败（与 C10 无关，未写任何东西）。rebase 到 `d222738` 后复跑：ONTAP NFSv3 / v4.0 / v4.1（5399 条）、DXN-2 v3 / v4.0
（播种 1500 条，测后删除）与 `d222738` 的 Blocks 条目集合、计数逐行相同；e2e-cifs 全过。HDFS（Kerberos 实验集群，runner VM 102，
运行根 `/tmp/data-mover-nightly/c10-20260927103741/`，测后删除）：播种 1524 条（44 个目录，含 12 层深链、1300 文件平铺），Unordered
c = 8 / 64、Blocks c = 8 与 `d222738` 的 Blocks c = 8 / 64 条目集合摘要相同、计数相同、0 失败，Unordered 无目录事件。

### 12.9 C11 实测：续扫检查点的开销与真机续扫（2026-09-27）

**node1 m1-source 整树（53,368,420 条 / 3,368,421 目录，200G 链路，c=64，`--quiet`；On = `--checkpoint-every 60 --checkpoint-out`，
每次写文件前 flush + fsync + rename）**，二进制 = `6f4f7d8`（即 `55f41c2` 在 C7d 合入前基线上的同一提交；10.131.9.10 编译 + glibc 2.34 shim），每种一轮：

| 运行 | wall | CPU（user+sys） | RSS 峰值 | 检查点 | 最大检查点 | 条目 / 失败 |
|---|---|---|---|---|---|---|
| Unordered Off | 996.8 s | 269.6 s | 17.3 MB | 0 | — | 53,368,420 / 0 |
| Unordered On | 996.0 s | 239.1 s | 17.8 MB | 16 | 44 KB（最后一个 21 KB） | 53,368,420 / 0 |
| Blocks On | 1106.1 s | 270.5 s | 25.8 MB | 18 | 160 B | 53,368,420 / 0 |
| Blocks Off | 1068.9 s | 333.5 s | 28.0 MB | 0 | — | 53,368,420 / 0 |

On 不比 Off 慢：单轮 CPU 的轮间波动（§12.8：同一二进制 235–277 s）远大于两者之差，这里 On 的 CPU 甚至更低；wall 由服务器决定。
§9 估计的 < 0.5% 在这个量级上测不出来 —— 只能说开销在噪声之内。Unordered 的检查点约 1000–3000 个等待中的目录（每条 13 字节 +
不同的尾部），与设计的“约 6400 条路径、约 300 KB”同量级而更小（去掉了祖先也在前沿里的目录）；Blocks 是一条 6 层的链，160 字节。

**真机续扫**（`.claude/skills/_shared/traversal_resume.sh`：完整跑一遍，再“取消（之前至少一个周期检查点）→ 从最终检查点续扫”循环到
完成，比对条目集合；两种方式都跑）。每处播种约 660 条（16 个目录 × 20 文件、8 层链、300 文件的宽目录），测后删除：

| 环境 | Unordered | Blocks |
|---|---|---|
| Local（1702 条，含 30 层链、600 文件宽目录、符号链接） | 3 轮，0 丢失，重复 80 | 3 轮，0 丢失，重复 301 |
| Local 6 万文件，SIGKILL（只靠周期检查点，模拟崩溃） | 51 轮，0 丢失，重复 6940 | 54 轮，0 丢失，重复 4410 |
| NFS ONTAP v3（10.131.9.10，root） | 4 轮，0 丢失 | 3 轮，0 丢失；SIGKILL 1 轮 0 丢失 |
| NFS ONTAP v4.1 / v4.0（WSL） | 7–12 / 5 轮，0 丢失 | 3 / 3 轮，0 丢失 |
| NFS DXN-2 v3 | 7–19 轮，0 丢失 | 3 轮，0 丢失 |
| CIFS FAS2750 | 4 轮，0 丢失 | 3 轮，0 丢失 |
| S3 MinIO `data-mover-test` | 8 轮，0 丢失 | 3 轮，0 丢失 |
| S3 ONTAP NAS 桶（`compat=ontap&bucket_type=nas`，无序列举） | 11 轮，0 丢失 | 3 轮，0 丢失 |
| S3 ONTAP 版本桶 `data-mover-test-ver`（`--versions all`，每对象两个版本，1284 条） | 6 轮，0 丢失 | 2 轮，0 丢失 |
| HDFS 实验集群（runner VM 102，240 条，ignored 测试） | 3 轮，0 丢失；从 9 个中途检查点续扫都不丢 | 3 轮，0 丢失，重复 9；同左 |

Unordered 的重复多：它的检查点里是正在列的目录，宽目录（300 文件）每轮都在列、每次续扫整块重列。每轮交出的条目必须多于最大的目录，
否则续扫越不过它（§9.3 已知限制，实测：每轮 150 条时宽目录 300 条的树 50 轮不完成）。C7d 合入之后在 `056a271` 上复跑
ONTAP v3 / v4.1、DXN-2 v3、Local，结果相同。

## 13. 破坏性变更清单

| 项 | 变化 |
|---|---|
| `Namespace`（`roles.rs:474-555`） | 类型化动词；`list(ListRequest) -> ListingStream`；删 `NamespaceRequest`、`NamespaceResult`、`execute`、`list_versions`、`listings_are_abortable`、`mutations_unsupported`（并入 `capabilities`） |
| `TraversalOrder`（`mod.rs:27-63`） | `{Admission, NameBytes}` → `{Unordered, Blocks}`；删兄弟排序 |
| `TraversalItem`（`mod.rs:250-257`） | `{Entry, Failure, DirectoryListed(仅 Blocks), Checkpoint}`；删 `SubtreeComplete` |
| `DirectoryListed` / `DirectoryListing` / `ChildOrder` / `SubtreeSummary`（`mod.rs:262-361`） | 重定型为 `{path, outcome, counts}`；后三个类型删除 |
| `TraversalCompletion`（`mod.rs:380-387`） | `{ totals: ScanTotals }` |
| `TraversalRequest`（`mod.rs:151-166`） | `non_exhaustive` + 构造器；新增 `resumable`、`resume_from`、`max_child_failures` |
| `TraversalTerminalFailure`（`mod.rs:409-412`） | `non_exhaustive`；Resumable 时携带最后检查点 |
| `FailureClass` | 新增 `Throttled` |
| 扫描的观察参数 | `TraversalRequest.observation_plan: ObservationPlan` → `should_scan: 额外项集合`（`Acl / Xattrs / Tags / SymlinkTarget`，无档位）；`ObservedEntry` 的时间与 uid/gid/mode 变为普通 `Option` 字段；symlink 目标不再无条件读取 |
| S3 身份 | `PathScoped` 身份含路径（F1），已有代的 S3 条目身份键一次性全部变化 |
| 多版本（`mod.rs:65-86`，`ObservedEntry::version / source_version`，`storage/descriptor.rs` 的 `ListingFacts`，快照 v6） | 删除 `TraversalVersions` 与 `list_versions`、`EntryVersion`；每版本一项改为每 key 一项（`ObservedEntry::history()` → `ObjectHistory` / `VersionRecord`，超过 10 万条分段），当前已删的 key 也列出；是否列历史由连接时的 `GetBucketVersioning` 决定（从未开过版本的桶不再列 `"null"` 版本，除非拒答；瞬时失败 role connect 失败）；快照只读 v7（所有 backend 的 v4–v6 `UnsupportedVersion`）；检查点 v2（v1 `UnsupportedVersion`）；`bytes` 改为当前版本字节，新增 `versions` / `delete_markers` / `all_versions_bytes`；`BackendConnectError::session_failure()`（C11b） |
| 行为 | 大目录中途的瞬时错误：今天（物化）从头重来会成功，P7 后首页之后不能从头重来 → 可能 `Interrupted`（NFS 需 nfs-rs 页接口才能续） |
| 测试 / 例子 | `storage/tests/order.rs`、`tests/local_namespace_contract.rs` 的兄弟有序断言删改；`examples/storage_role_operations/` 的 `--order`；16 个测试 fake 改用 `ListingStream::from_pages` |
| 目录对象（F5b–F5e） | `NamespaceCapabilities` 新公开字段 `directory_objects`（外部字面量要补）；`ListedChild` 可以是空名（= 目录自己的对象，路径 `<dir>/`）；S3 列举多出 `d/` 文件条目（含零字节文件夹标记，files / 行数上升），以前有内容的 `d/` 是 `Failure{Child}`；`type == "file"` 匹配到它们；写到不能存目录对象的一端在 Preflight 报 `Unsupported`；MinIO 的 `XMinioInvalidObjectName` 从会话 `Protocol` 改为条目 `Unsupported` |
| legacy（F5a） | S3 `walkdir` / `walkdir_2` 对有内容的 `d/` 报 Scan 错误（之前静默跳过）：terrasync 在这类桶上会关闭删除检测 |

## 14. 实施计划

```
PLAN
====
列举按页流入遍历，提供 Unordered / Blocks 两种扫描方式，异常-only 事件模型，准确计数，
typed 错误分层，可选断点续扫。
why: 千万级平铺目录今天整块驻留、首项要等整个目录列完；每条目一次 spawn 让 NFS 整树 CPU 是
     legacy 的 3 倍；terrasync 需要可靠的完整性判定与统计。

CHANGE
======
（与 P7 无关、先做的两个独立修复）
F1  fix(s3)!: PathScoped 身份含路径（路径 + ETag），契约写明（Q28）      —— 已完成：4fa5aa5
    - src/storage/backends/s3/source.rs:75-89；src/model/observation.rs:95-107 的文档
F2  fix(s3): 版本历史顺序取自 ListObjectVersions 原始 XML，不再用 LastModified 合并（Q29） —— 已完成：40c1ac9
    - src/s3/role_protocol/versions.rs:100-130（拦截器取原始响应体，参照 src/s3/dxn.rs）
    - src/storage/backends/s3/history.rs:1-80；fixture：MinIO 并发实测响应
C0  perf(traversal): 页级观察 + 重排缓冲快路径；C0b 有 mtime 时即时决定；
    列举带 inline ownership（NFS / Local），InlineOnly 不再额外 GETATTR；BackendIdentity 用 Arc
    - src/traversal/storage/cursor.rs:550-594   列举任务里构造无 I/O 的 ObservedEntry
    - src/traversal/storage.rs:364-382, 391-427 immediate_decision / admit
    - src/traversal/storage/observe.rs:17-28, 114-162  拆出同步 observe_inline
    - src/traversal/storage/output.rs:139-162   快路径
C1  refactor(storage)!: Namespace 类型化动词 + ListRequest / ListingPage / ListingStream；各 backend 先各产一页
    - src/storage/roles.rs:474-555；src/storage/backends/{local,nfs,cifs,s3,hdfs}/namespace.rs
    - src/storage/delete_tree.rs:263；src/ndx_walk.rs:211；src/traversal/storage/observe.rs:128-136
C2  feat(traversal)!: 事件模型（Entry / Failure{scope} / DirectoryListed / Checkpoint）+ ScanTotals / BlockCounts
    - src/traversal/mod.rs:250-387；src/traversal/storage/output.rs（删子树聚合 :90-107, :224-285）
C3  feat!: 错误模型（Throttled、逐子项失败、vanished、TraversalRequest 构造器、TerminalFailure non_exhaustive）
    - src/model/mod.rs:512-537；src/s3/role_protocol.rs:518-523；各 backend namespace 的 collect 改逐子项
C4  refactor(traversal)!: Blocks 游标逐页消费；块关闭时按名字排下钻；删兄弟排序；TraversalOrder::{Blocks}
    - src/traversal/storage/cursor.rs:66-129, 195-238, 340-393, 467-534
C5  feat(storage): Local 流式            - src/storage/backends/local/namespace.rs:125-153, 257-297
C6  feat(storage): S3 流式（Current；前缀集合；All 物化；节流退避）
    - src/storage/backends/s3/{listing.rs:55-130, paging.rs, namespace.rs:109-146}
F5  目录对象（键以 `/` 结尾的 S3 对象，现存问题 12；用户 2026-09-26 定：统一按文件、全部采纳建议）—— C7 之前，见 §14.1
    —— 已完成：F5a `ec3ee53`、（refactor `8209eb8` 拆出 listing/child.rs）、F5b `e70defb`、F5c `10949a5`、F5d `8958201`、F5e `706e8e9`、F5f `43d0381`
    F5a fix(s3): legacy walkdir / walkdir_2 对有内容的 `d/` 报 Error，不再静默跳过（src/s3.rs 3596 / 3674 / 3925 / 4117 附近）
    F5b feat(storage)!: 列举契约允许“空名子项 = <dir>/”（ListedChild::directory_object、ListingPage::push_directory_object、
        into_parts 根守卫、NamespaceCapabilities.directory_objects、StoragePath::names_directory_object、entry_name 取最后非空段）
    F5c feat(s3)!: Current / All 交出目录自己的对象（任何大小，含零字节与全零历史；storage root / 扫描根：零字节忽略、有内容报 Child 失败）
    F5d feat(s3): 目的端写 `d/`（单 PUT / multipart + 指针在父目录 `p/.data-mover-<digest("d/")>.upload` / native / direct）
    F5e feat(transfer): directory_object_action 辅助（Copy / NothingToDo / Skip(reason)）+ 引擎守卫（目的端无 directory_objects → Preflight Unsupported）
    F5f docs（§4.1 路径逐字节、不去尾 `/`；§5.4.4；§5.4.5；§7；§10.3/10.4；§13；filter-dsl.md；storage-s3.md；ADR-0006）
H1  （已完成，本系列最后一个提交）feat(s3): 目的端重放删除标记（StagedDestination::write_delete_marker + transfer::write_delete_marker；仅 Enabled 桶、每次调用前查
    GetBucketVersioning；结果 Written / Merged / NullVersionRemoved；examples/s3_history_replay.rs）—— F5 之后、C7 之前
F5g fix(s3): 目录对象只用单次请求写（D13）：单 PUT / 单次 CopyObject，> 5 GiB Preflight Unsupported —— 已完成 `2352a6b`
    （前置 refactor `870f385`；五套对象存储版本 / 非版本真机全过）
C7b fix(nfs): v3 READDIRPLUS 列出、却不给属性的 `.snapshot`（ONTAP 卷根）隐藏，与 NFSv4 一致（用户 2026-09-26 定，
    不按 legacy 下钻进快照树）—— 已完成 `0064563`（前置 refactor `02e23b4`）。真机先查清：ONTAP 的 `.snapshot` 与卷根 **fsid 相同**
    （只有其中每个快照的 fsid 不同），READDIRPLUS 给句柄不给属性；名字被保留（任何目录里 MKDIR `.snapshot` 回 EXIST）；
    从任何目录 LOOKUP 都是同一个对象。定的规则（用户 2026-09-26 选方案 2，只限 v3）：名字恰为 `.snapshot` 且缺属性的项
    → 在被列目录里 LOOKUP 一次（回复无属性再 GETATTR）；是目录 → 隐藏（不列、不算失败、不下钻；计入页的 `internal_hidden`、
    一条 `info!`）；不是目录 → 按查到的属性当普通子项；查失败 → 该子项失败（查询自己的分类，`NOENT` 即消失），会话 / 节流 /
    目录句柄失效的失败按 READDIRPLUS 失败结束这次读取。带属性的 `.snapshot`（DXN 上真建的目录）、其他名字、v4 一律不变
C7c feat(storage): NFS 列举首页之后可按 cookie 续读（Q18；nfs-rs 0.8.5 `Mount::readdirplus_page`）—— 已完成：`26a27f6`（前置 `17bb6fb` 升级 nfs-rs）
    - 协调者 2026-09-27 认可与原计划的三处差别：cookie 被拒（或回空末页）→ 带去重从头重读（≤ 3 次）而非 `Interrupted`；
      cookie 回环只在一次读取内检测；`NFS3ERR_NOT_SYNC` 不当坏 cookie。去重摘要（每条目一次 SipHash、每列举 ≤ 2^18 条
      ≈ 4.5 MiB）**已实测**（2026-09-27，C10 rebase 到 `d222738` 之后，§12.8 末）：node1 m1-source 53M 树两轮，§12.6 的目标全部仍达标
      （Unordered CPU 1.01×、RSS 17.5 MB；Blocks CPU 1.27×、RSS 26.5 MB）；同一二进制除 C7c 外不变的 A/B（10.131.9.10 经 1GbE，
      子树 d019 2,694,735 条，交替四轮）CPU 差 < 2%、在噪声内，RSS 不变 —— 不需要改。**后续项已实现** `c0ff649`：首页之前也保留已攒页、cookie 与去重状态；
      新接收条目补满预算，频繁断连不再丢弃已有进展。去重耗尽且尚未发布时，退回不补预算的有界整目录重读；本轮验收见 §14.4
    - 接缝：`read_directory` 带起点 cookie；`NfsListingPages::first_sight(fileid, 名字)`（读者看一项之前问，已取的跳过，
      含 v3 `.snapshot` 的 LOOKUP）、`reached(next)`（整个回复取完、目录未完时报位置；同一次读取里重复 → `NfsReadEnd::Repeated`）；
      `NfsRestart::FromStart`（`is_bad_directory_cookie`：`NFS3ERR_BAD_COOKIE` / `NFS4ERR_BAD_COOKIE` / `NFS4ERR_NOT_SAME`；
      `NFS3ERR_NOT_SYNC` 不是 READDIRPLUS 的状态，nfs-rs 也不算，不收）
    - 读者：一页一页 `readdirplus_page`，页之间看 `stopping()`，不放弃在途请求（v4.1 slot）；EOF 先于 `reached` 判断
    - 生产者：原 `26a27f6` 只在首页后续读；`c0ff649` 起首页前后都按同一分类 / 预算重试，保留已攒页，从最后完整回复后的 cookie 续
      （解析出的句柄变了 → 从头）；`(fileid, 名字)` 带密钥 64 位摘要去重（不用名字：nfs-rs 有损解码，两个名字可能同一串），
      上限 2^18 项（约 4.5 MiB / 列举；超了首页后不再续，首页前仅可丢未发布缓冲有界从头重读，永久关闭去重且不再补预算）；cookie 循环只在一次读取内判（不认 cookie、从头重发整目录的服务器
      靠去重读完，不当循环）；续读的 cookie 被答以空的目录末尾（ONTAP、DXN-2 答不认识的 cookie 的方式）或被拒 → 从头读并去重，
      至多 3 次；每取到新子项续满重试预算（HDFS C8 同一规则）；重试用尽的结局与之前首页后相同（可重发的传输失败只让该列举
      `Interrupted`，其余照原作用域）
    - 与需求原文的差别（理由见提交说明）：cookie 被拒 / 空末页不直接 `Interrupted`，而是从头读 + 去重（结果与续读同样完整）；
      cookie 重复按“一次读取之内”判
    - 真机（2026-09-27，10.131.9.10，release）：ONTAP v3 / v4.0 / v4.1、DXN-2 v3 / v4.0 各一棵 5 万文件平铺目录 + 嵌套小树
      （50,009 / 50,002 条）：与 base（`292a0e8`）逐行相同（含顺序）、0 重复；RSS 17.9–19.2 → 18.0–20.3 MB（+0.1–1.1 MB，
      即 5 万项的摘要集合），耗时不变。故障：`ss -K` 在列举中途三次掐断 NFS 连接 —— v4.1 浮出 `Retry`、三次续读；v3 / v4.0
      （两台服务器）由 nfs-rs 自己重连重发，列举看不到失败；五种都读完、与 base 相同、0 重复。另用不提交的测试构建（环境变量
      开启：每次读取第 60 个回复起每 10 个回复取一半后注入 `ConnectionReset`）：每种 17–31 次首页后续读，被截断的回复从其 cookie
      重发、前一半跳过，五种都读完、与 base 逐行相同（排序后）、0 重复、`interrupted=0`。附带观察：这些服务器上 nfs-rs 的
      READDIRPLUS 回复只有约 40–60 项，1024 项的首页要 20 多个回复；首页之前仍是 C7 的规则（从头、不续满预算），注入过密
      （每 7 个回复）时首页前重发用尽、照旧升会话
P1–P5  S3 URL 参数化（§14.2）—— F5 合入后，可与 C7 并行
       —— 已完成：P1 `2fe52f1`、P2 `71fb411`、P3 `52710c7`（+ refactor `cc65fec` 移出 tls.rs）、
       P4 `db7fbb9`、P5 `4718183`（调用方与文档）；真机见 §14.2 实施记录
N0     NAS 桶实测（curl，§14.2）—— 已完成：`2893508`
N1–N7  `compat=ontap&bucket_type=nas` 的 NAS 桶 profile（§14.2）—— H1、C7 之后
       —— 已完成：N1 `927cc03`、N2 `9a9447d`、N3 `4c8750c`、N4 `9d97837`、N5 `3129902`、N6 `d51af50`、N7 `ce49448`；
       真机见 §14.2 实施记录
F5h fix(s3): 目录对象的单 PUT 改为流式（Content-Length = 源大小、UNSIGNED-PAYLOAD、边传边算 MD5 与回复 ETag 核对），不再整对象缓冲
    在内存里（F5g 现状：流式来源的 `d/` 整个读进内存再 PUT，最坏每个在途传输 5 GiB）。复用 N 阶段的流式单 PUT 原语：该原语先做成
    通用 S3 能力（不绑 NAS profile），F5h 与 NAS profile 共用；五套 S3 真机逐一确认接受 UNSIGNED-PAYLOAD 的流式单 PUT
    —— 已完成 `7260974`（五套 S3 版本 / 非版本桶 `directory_objects.sh` 全过，见 §14.2 实施记录）
S3 收尾（P7 遗留三项，2026-09-27）—— 已完成：
    - `3080f4a` fix(skills): e2e-s3 脚本（traversal_versions / versioning_matrix / directory_objects）的 ListObjectVersions
      不再发空的 version-id-marker（StorageGRID 11.5 回 400，C10 冒烟因此跑不了 data-mover-test-ver）；crate 本身不发，
      补线上守卫测试；StorageGRID 两只桶 + MinIO 的 traversal_versions.sh 全过
    - `a699bd4` refactor(s3): NAS 桶“还是描述时那个对象”的三处判定（unpinned 响应头、断 body 后 HEAD、读完后 HEAD）
      收为 `same_object::object_change`；nas_matrix.sh 全过
    - `4dabfe0` feat(transfer): `directory_object_action` 传目的端 `Storage` 时事先答 `Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON)`
      （与 D13 的 Preflight 拒绝同一规则、同一诊断），见 §14.1“F5 收尾”
C7  feat(storage): NFS 流式（首页前可从头重试）      - src/nfs.rs:4610-4691   —— 已完成：cdcc66d（搬移）、7da78ce
    + 子目录句柄作为下钻提示随工作项下传，列举不再写 GLOBAL_CACHE，无提示 / 失效回退 lookup_fh（§12.7 修法 A）
C7d perf(nfs): READDIR / READDIRPLUS 的回复大小按服务器公布的上限定（用户 2026-09-27 同意）—— 已完成：`9994a55`（chore(deps) nfs-rs 0.8.6）、
    `cc61c26`（URL 不再接受 `readdir-buffer`）
    - 原状：nfs-rs 默认 `readdir-buffer=8192,8192`（dircount / maxcount 各 8 KB），data-mover 从未设置；每个回复只装 23–45 条
    - 做法（用户 2026-09-27 定：只按服务器公布的上限，URL 里不写也不接受 `readdir-buffer`，不另设内存上限；协调者随后定：不加按次传入
      的接口，改 nfs-rs 的默认）：nfs-rs 0.8.6（JayTsu-sh/nfs-rs #124 / #125，tag 触发的发布流程发到 crates.io + PyPI）在 URL 没有
      `readdir-buffer` 时，`maxcount` = 挂载时的读上限（与 READ 同一个：v3 FSINFO `rtmax`，v4.0 `maxread`，v4.1 `maxread` ∧ 会话最大
      回复；不超过 4 MiB 载荷上限；v4.0 不公布 `maxread` 仍 8192），`dircount = maxcount`；页接口与流都用；目录回复的超时按同样大小的
      READ 算；每项句柄是拷贝。data-mover 的 `parse_nfs_url` 拒绝 URL 里的 `readdir-buffer`（`ConfigError`，参数名按 nfs-rs 的方式
      解码后比较）
    - `dircount` 的取法（实测定）：`dircount` 只约束名字 / cookie / fileid，是回复的子集，取 `= maxcount` 就永远不先于 `maxcount`
      截断。实测两种服务器都不让它起作用：maxcount 1 MiB 下 dircount 取 `dtpref`（ONTAP 32 KiB、DXN-2 16 KiB、v4 8 KiB）、
      maxcount/8、maxcount，每回复条数完全相同。不取 `dtpref`：Linux knfsd 报一页，真按它截断的服务器每回复只剩几百个名字
    - 公布值（2026-09-27）：ONTAP 9 v3 rtmax 1 MiB / dtpref 32 KiB，v4.0 maxread 1 MiB，v4.1 会话回复 1 MiB（rsize 1 MiB − 4 KiB）；
      DXN-2 v3 rtmax 1 MiB / dtpref 16 KiB，v4.0 maxread 1 MiB。**ONTAP 目录回复自己封顶 32 KiB**（maxcount ≥ 32K 各档完全一样）
    - nfs-rs 层（单个列举，5 万文件平铺目录，10.131.9.10，两轮）：

      | 服务器 | 每回复条数 8K → 自动 | 列举 wall 8K → 自动 | 首页 8K → 自动 | 峰值 RSS 8K → 自动 |
      |---|---|---|---|---|
      | ONTAP v3 | 41 → 175 | 1.05 → 0.43 s | 1–2 → 2 ms | 6.0 → 6.0 MB |
      | ONTAP v4.0 / v4.1 | 23 → 95 | 2.0–2.1 → 0.95–1.1 s | 1 → 2 ms | 6.1 → 6.4 MB |
      | DXN-2 v3 | 45 → 5,556 | 4.8–5.6 → 2.4–3.6 s | 3–5 → 270–380 ms | 6.1 → 9.4 MB |
      | DXN-2 v4.0 | 39 → 5,000 | 7.8–10.7 → 5.1–14 s（抖动大） | 4–900 → 350 ms–4.6 s（冷缓存） | 6.0 → 10 MB |

      几档固定值（32K / 64K / 256K / 1M）：ONTAP 32K 起即封顶；DXN-2 v3 32K 184 条 3.3–3.5 s，64K 370 条，256K 1,471 条 2.7–2.8 s，
      1M 同自动；v4.0 32K 157 条 5.3–6.9 s，256K 1,250 条 5.3–5.5 s，1M 3.7–4.9 s
    - data-mover 层（`storage_role_operations traverse` Unordered c=64，release，两轮；8K 由不提交的测试钩子经 URL 强制）：

      | 目录 | wall 8K → 自动 | 首个子项 8K → 自动 | 峰值 RSS 8K → 自动 |
      |---|---|---|---|
      | ONTAP v3 5 万平铺 | 1.09–1.10 → 0.50–0.52 s | 35 → 25 ms | 18.5–19.6 → 19.1–19.4 MB |
      | ONTAP v4.0 / v4.1 5 万平铺 | 2.13–2.19 → 1.05–1.13 s | 47–50 → 25–26 ms | 17.1–18.5 → 17.3–18.1 MB |
      | DXN-2 v3 5 万平铺 | 5.2–5.5 → 3.5–3.6 s | 111–133 → 333–388 ms | 18.7 → 31.2–31.5 MB |
      | DXN-2 v4.0 5 万平铺 | 10.2–53.5 → 14.5–47.3 s（服务器抖动） | 0.5–1.4 → 0.4–4.6 s | 17.5–18.7 → 29.9–31.5 MB |
      | DXN-2 v3 宽树 32 × 5,000 | 3.8–4.5 → 3.7–6.3 s | — | 37.5–41.6 → 33.5–41.3 MB |
      | DXN-2 v4.0 宽树 32 × 5,000 | 10.1–14.0 → 8.9–11.6 s | — | 34.5–36.3 → 33.3–38.2 MB |

      条目数、`totals balanced` 各档都相同。
    - node1 m1-source 全树（每目录 35 项：20 个子目录 + 15 个文件，Unordered，c=64，交替四轮，0.8.5 构建 = `92151d2`，
      0.8.6 构建 = `cc61c26` 的树）：53,368,420 条、0 失败，四轮都一样。wall 0.8.5 996.2 / 997.2 s → 0.8.6 995.5 / 996.4 s；CPU（user+sys）
      257.2 / 255.6 → 274.1 / 223.8 s（node1 轮间波动，均值 256.4 → 249.0）；峰值 RSS 17.0 / 18.8 → 17.8 / 17.3 MB。每目录 35 项，8 KiB
      回复在 v3 已能装下（约 45 项），所以不变，与预期一致。原始数据：node1 `/root/dm-c7d/logs/u08{5,6}-r{1,2}.*`
    - §12.6（Unordered RSS ≤ 30 MB）：m1-source 全树**仍达标**（17.3–18.8 MB，wall / CPU 与 0.8.5 相同）。ONTAP 自己封顶 32 KiB，不受影响。服务器照给 1 MiB 时（DXN-2），一个
      5,000 项以上的大目录单独就让峰值 +12 MB（18.7 → 31 MB，超过 30 MB）；32 个宽目录并发时 8K 与自动都在 33–42 MB（宽树的峰值
      主要不是回复缓冲），所以“K 个 worker × 1 MiB”的最坏估计在实测里没有出现叠加。**本轮继续按用户已定结论，不另设内存上限**。
      历史记录中“大目录是否限大小”的疑问仅保留为后续性能观察，不是 P7 尚未实现项，也不表示仍待用户决定
    - 附带（nfs-rs）：v3 READDIR / READDIRPLUS 原来沿 XDR 链表递归解码，1 MiB（约 6,000 项）的回复在 debug 构建里栈溢出（nfs-rs 的
      真机测试撞到），0.8.6 改为逐项解码；8 KiB 时不会发生
    - **首项延迟（用户 2026-09-27 定：维持现状）**：DXN-2 上 1 MiB 回复让大目录首个子项约 0.12 → 0.35 s；不做“首次请求用小尺寸”的变通（需再发 nfs-rs 版本）
    - NFS 收尾（2026-09-27）：`619dd63` role 分类补上 NOTEMPTY / EXIST / ISDIR / NOTDIR（→ Conflict）、FILE_OPEN（→ Conflict / Transient）、
      XDEV（→ Unsupported）、INVAL / NAMETOOLONG / BADNAME（→ InvalidInput）、DQUOT（→ Capacity）（此前 Protocol / Unknown）；`2025194` role rename 自己解析父目录、自己分类 RENAME（被拒 →
      PermissionDenied / Permanent）；`a953ce8` 建缺失父目录失败报在被拒的那一层；`66a9dbe` `Namespace::create_directory` trait 文档写明
      NFS 重建消失的已缓存父目录。真机 nfs_mkdir_contract（含 rename 被拒、rename 进被拒的缺失父目录）ONTAP v3 / v4.0 / v4.1、DXN-2 v3 /
      v4.0 全过，e2e-nfs 一次全过。nfs-rs（未发布）：JayTsu-sh/nfs-rs #127（合并 `84b0828`）—— 回复截断在 XDR 填充里是解码错误不再 panic
      （fastxdr 1.0.2 生成的头，issue #126），v3 / v4.0 无 maxread 的挂载级目录回复大小测试
C8  feat(storage): HDFS 流式 + 去掉列举前 get_metadata  - src/hdfs/namespace.rs:271-288
C9  feat(storage): CIFS 流式（生产者持有句柄；停车句柄上限） - src/storage/backends/cifs/{protocol.rs:230-280, namespace.rs:59-94}
    —— 已完成：70f85d1（接缝 open_directory + CifsDirectory，不改行为）、936e20b
C10 feat(traversal): Unordered 调度器（worker LIFO + 窃最浅 + 全局计数，内部通道按页），设为默认
    —— 已完成：268258c（会话通道按批）、e0fb2c2（refactor：页准备与计数共用）、511cee5（perf：生产者先在消费方任务里推进）、
    0517c68（perf：名字 / 路径分配复用）、ad2dfec（perf(nfs)：直接从 READDIRPLUS 项描述）、6711e58；Phase 6 性能门见 §12.8
C11 feat(traversal): Resumable（Checkpoint 项每约 1 分钟 + 取消时补发、resume_from、指纹），两种方式
    —— 已完成：`8337fad`（refactor：Blocks 驱动移到 storage/blocks.rs）、`51152da`（test：pool 前沿不变式在每个临界区之间检查，
    SlotGuard）、`2ddf553`（refactor：例子拆目录）、`55f41c2`；实现要点 §9.3，node1 开销与真机见 §12.9。故障注入（全部变红）：
    出栈 / 窃取先出栈后登记、快照漏 active、会话取消仍丢弃、最终检查点与取消竞速（两种方式）、指纹漏 root、Off 发检查点、
    Blocks 用 `next` 代替 `delivered`、取最新记录、祖先失败丢失、重列保留 ≥ 下一环、忽略版本字节、最终检查点受周期门控、
    检查点落在块内、解码不设上限
C11b feat(s3)!: 多版本改为每 key 一项（ObjectHistory、分段、快照 v7），删除 TraversalVersions / list_versions
    - 用户 2026-09-27 定（Q-A）：连接时 GetBucketVersioning 瞬时失败（超时 / 5xx / 网络，SDK 重试后）→ role connect 报 Connectivity / Transient，
      不猜；明确拒绝（403）→ 列历史（Q26）；501 / 不支持版本 → ListObjectsV2；快照只留一种格式、不兼容旧版本（用户 2026-09-27）
    —— 已完成，CI、实测与环境限制见 §14.3：`bda95d3`（refactor：版本列举搬到 histories.rs）、`5a20a77`（refactor：tally_child 返回 Tallied）、`f309f7a`（Q26 + Q-A：
    连接时判定，`S3VersioningAtConnect`，瞬时失败 role connect `Connectivity` / `Transient`）、`7ec3a51`（每 key 一项、`ObjectHistory`、
    快照 v7 + golden 字节、删 `TraversalVersions` / `list_versions` / `EntryVersion`、记录级观察 R2、R3、检查点 v2）、`648b132`（分段 Q8）、
    `e2511a2`（计数）、`fdea3ed`（e2e-s3 / 续扫脚本）。要点见 §10.1 / §10.4 / §11.2c。与计划的差别：① `ObjectHistory` 多一个
    `largest`（整个 key 的最大版本，每段都带、进快照），否则分段后各段的 R3 答案可能不同（评审）；② `S3Namespace` 在连接时取一次
    `lists_histories()`（整个连接不变）；③ 构造请求失败（不是瞬时的）按拒绝处理，`Unreachable` 带 HTTP 状态进诊断；
    ④ `BackendConnectError::session_failure()` 把 Q-A 的类型化失败带给 `connect_backend` 的调用方；⑤ 当前版本观察失败时条目时间
    退回列举的；⑥ `Tallied::push_into` 代替迭代器（热路径）。2026-10-12 从 GitHub 的 `wip/p7-c11b` 恢复以上七个实现提交；
    `e2511a2` 已含 count_history 和“当前已删且分段”的回归测试，交接记录中“待补计数修复”已过时。
    2026-10-12 找回原构建目录：501 个 src/tests/examples 与 Cargo 文件逐字节匹配 `fdea3ed`；历史 `ci.log`
    记录 1788 passed / 8 ignored，`fi.log` 记录 17 项故障注入全部变红（JOB_EXIT=0）。这些是历史证据，不代表当前复跑。
    本轮修正：`29975eb`（HTTP 408 与响应丢失）、`aafd463`（历史 InlineOnly 不额外 HEAD，保留列举时间）、
    `757c6c9`（SDK 将 HTTP 2xx 解码失败包装为 ServiceError 时仍按 Q-A 拒连）；当前验收结果统一记在 §14.3。
    保留桶 `data-mover-c11b-1790508092` 不属于测试清理范围（用户要求保留）。
Q4  feat(traversal): Blocks 已关闭目录块的配对辅助 —— 已实现 `46a8a4c`，接口与安全边界见 §3.1；本轮验收见 §14.4
    - src/traversal/pairing.rs、pairing/{input,tests}.rs；src/traversal/mod.rs 导出
C12 docs: docs/architecture/storage-transfer-architecture.md §5/§6；.claude/docs/{architecture,error-taxonomy,storage-*}.md；ADR-0008
    —— 已完成：文档同步当前接口；ADR-0008 记录决策与验收边界，C11b 真机验收见 §14.3。
    （含 C11b 定下的**写入快照格式开发期规则**：一种当前格式、版本字节 + golden 字节测试、任何改动都升版本、旧格式
    `UnsupportedVersion`；正式发布前重新决定 —— 至少解码 N-1，或给 terrasync 迁移 / 重扫说明）
（terrasync 侧，另起：不丢批、分钟级退避、检查点落库顺序、消费者退出即失败、统计以库为准、全量扫描 generation 闸门、
  身份键只对 StableWithinBackend 关联、多版本每项展开成每版本一行）
（可选）chore(deps): nfs-rs 公开 READDIRPLUS 页接口并升级（Q18）—— 已完成：nfs-rs 0.8.5 `17bb6fb`，用法见 C7c

CREATE
======
- src/storage/listing.rs: ListingStream::from_producer / from_pages / collect_listing
- src/traversal/unordered.rs（+ unordered/ 子模块）: Unordered 调度器 —— C10 实际放在 `src/traversal/storage/unordered.rs` +
  `storage/unordered/{pool,worker,page_io}.rs`（要用 storage 模块的私有件：`Shared`、`stream::prepare`、`resolve`）；会话按批的通道是
  新文件 `src/traversal/channel.rs`
- src/traversal/checkpoint.rs: Checkpoint 编解码、指纹
- src/traversal/storage/tests/streaming.rs: 多页 fake（Notify 逐页放行）——逐页准入、块序、驻留与 N 无关、半途失败、首页前重试
- src/traversal/storage/tests/counts.rs: 不变式（即时 / deferred 两遍结果相同）
- src/traversal/unordered_tests.rs: G2、失败清单、计数、检查点前沿不漏 —— C10 实际为 `src/traversal/storage/tests/unordered.rs`
  （与 Blocks 逐项对比、终止、取消、走开）、`storage/unordered/pool/tests.rs`（前沿、窃最浅、唤醒、并发排空）、
  `src/traversal/local_unordered_tests.rs`（真文件系统）；前沿不漏由 `#[cfg(test)]` 下每次出栈 / 窃取的断言保证
- src/storage/backends/s3/streaming_tests.rs: 打乱页内 / 页间顺序、对象与前缀跨页重复、同 key 版本分散在不相邻页、token 循环
- tests/local_streaming_contract.rs: 真文件系统 20 万平铺文件，首项延迟与驻留

RISK
====
- HIGH: CIFS 句柄。生产者任务每个出口都必须 close_resource；ONTAP 停车句柄空闲回收阈值未知 → FAS2750 专测（停车 30 分钟再取）。
- HIGH: 半途失败。首页之后从头重试会重复发出子项 → “首页已交出”闸门 + 注入测试（第 2 页失败）。
- HIGH: Unordered 检查点漏登记 → 续扫后整棵子树丢失。窃取与出栈的登记顺序要有注入测试（故意先出栈后登记 → 必须变红）。
- HIGH（已处理）: S3 列举节流由 C3/C6 按页有界退避并限定列举作用域；未改变传输路径（error-taxonomy 已同步）。
- MEDIUM: 预取视野从“整份到齐”变成“首页到齐”，FAS2750 上复跑 1f13247 那组深链 + 宽树对比 wall。
- MEDIUM: 删 SubtreeComplete / 兄弟排序后，现有 events / order 测试大改；防止“改测试迁就实现”——每个删掉的断言要在新测试里有对应的新契约断言。
- MEDIUM: S3 All 物化上限默认 500 万，超限报 Capacity，不能静默降级。
- LOW: nfs-rs 升级是依赖风险，本计划不依赖它。
- LOW（已处理）: 178 MB 来源已定位到 NFS 列举回填全局句柄缓存，C7 / C10 已修并通过性能门（§12.7 / §12.8）。

VERIFY
======
- timeout 900 make ci；clippy --lib --tests --examples --all-features 零警告
- 故障注入（看失败的形状）：closed 不等流读尽 → 块被截断；首页后从头重试 → 重复项；生产者某出口不 close → 句柄计数不归零；
  Interrupted 不进完整性 → 删除规则误放行；Unordered 先出栈后登记 → 续扫丢子树
- node1 + m1-source 53M 树（c=64，--quiet）：C0 前重测基线（含 MALLOC_ARENA_MAX=2、驻留探针、strace -c futex）、C0 后、
  Blocks 流式后、Unordered 后；与 legacy 交替四轮；条目数必须 53,368,420
- FAS2750：e2e-cifs；cifs_namespace_contract；深链 + 宽树 c=1/4/32；停车句柄 30 分钟；CIFS 与 NFS 的 Blocks 下钻顺序逐行一致
- **NFS 真机（用户 2026-09-26 定）**：ONTAP FAS2750（lizy `/ontap_lisaauto_nfs`）**NFSv3、NFSv4.0、NFSv4.1 三种**（v4.x 从 WSL；v3 因 mount_rootonly + WSL NAT
  只能在 10.131.9.10 上以 root、特权端口跑，二进制从 WSL 拷过去；不改 lizy 设置；C7 时 v3 遍历 5400 条、v4.x 5399 条，多出的是 v3 READDIRPLUS 列出的卷根 `.snapshot`；C7b 后三种都是 5399、0 失败，见 test-environments.md）+ **DXN-2（10.128.136.211）`/nfv3_v4` 下的 `data-mover-test/`，NFSv3 与 NFSv4.0 各一遍**
  （`e2e-nfs/dxn-v3/.env`、`dxn-v40/.env`，只写该子目录）；m1 NFSv3 用于大树 / node1 性能。每个动 NFS 的步骤（C7、C10、C11、收尾清理）都要过
- **S3 真机（用户 2026-09-26 定：本次重构的 S3 实际测试环境）**，每个动 S3 的步骤（C6、F4、C10、C11、C11b、收尾清理）都要在下列每一套上过；
  **ONTAP、RustFS、DXN、StorageGRID 都要在多版本桶与非多版本桶上各验证一遍**（ONTAP：data-mover-test-ver / data-mover-test，另加 NAS 桶；RustFS、DXN：data-mover-test + 临时版本桶）：
  - MinIO（10.131.9.11:9000，`.claude/skills/e2e-s3/.env`，桶 data-mover-test + 临时版本桶）：100 万对象平铺前缀的首项延迟与 RSS；版本化桶（traversal_versions.sh）
  - ONTAP S3（FAS2750 ONTAP 9.19.1，SVM lizy，http://10.128.61.200:80，`ontap/.env`）：对象桶 data-mover-test / data-mover-test-ver，
    卷开后台重删 + 压缩；我们的用户不建桶 → 脚本用已有桶、跑完清空对象（不删桶）。已知：列举字节序；带 delimiter 的 ListObjectVersions
    可能缺 NextVersionIdMarker 丢版本（现存问题 10 / F4，已修 `8d4b225`）；CommonPrefix 计入 max-keys；满页后跟空末页
  - ONTAP S3 **NAS 桶** data-mover-nas（同一端点、同一用户，FlexVol dm_s3_nas 重删 + 压缩）：平铺列举无序（目录遍历序）、无版本 —— “无序 S3”的真机
  - RustFS 1.0.0（VM 105，管理网 http://10.131.9.14:9001 —— 控制台端口也提供 S3 API，`rustfs/.env`，专用用户只能动 data-mover-* 桶，数据可丢弃）：
    ListObjectVersions 按“先 marker 组后 version 组”输出（现存问题 11，历史标 Inferred）；条件 PUT 支持（412）；ListMultipartUploads 支持前缀
  - DXN（Ceph RGW Octopus，`dxn/.env`）：用户 2026-09-26 长期批准建临时桶（`BUCKET_PREFIX=data-mover-dxn`、`S3_LOCATION=magnascale:tos_placement`，用完删光，用户上限 5 个桶）；
    已知差异见 `.claude/docs/storage-s3.md`「DXN」（UploadPartCopy 要 Content-Length: 0、CopyObject 不回版本、3 个网关各自打时间戳）
  - StorageGRID 11.5.0（独立租户 `data-mover-test`，用户 2026-09-26 批准并给出管理凭据）：桶 `data-mover-test` / `data-mover-test-ver`，租户内可建临时桶；CreateBucket 要 `Content-Length: 0`
- 无序 S3：ONTAP NAS 桶（上面）+ 内存 fake 的打乱模式（Q22）
- HDFS 真机（10.131.9.11 runner）：2026-09-27 起用户取消逐次批准，每个动 HDFS 的步骤（C10 起）都直接跑（C8 已于 2026-09-26 通过）
```

### 14.1 F5 目录对象：决定与要点（2026-09-26）

四套对象存储（MinIO、RustFS、ONTAP 对象桶、DXN）都允许键以 `/` 结尾且有内容的对象（PUT 200）；ONTAP NAS 桶把 `d/` 存成文件 `d`。
父列举（带 delimiter）只给 CommonPrefix `<Prefix>`，**没有任何大小属性**（四家 V1 / V2 实测）；内容只在列举 `d` 自己时出现在 Contents 里
（Size / ETag / LastModified），或 HEAD `d/`。GetObjectAttributes 不可依赖（ONTAP NotImplemented，MinIO / DXN 忽略）。

| # | 决定 |
|---|---|
| D1 | **统一按文件**（Blocks 与 Unordered 相同）：条目路径 = 键 `p/d/`，类型 File，由目录 `d` 自己的列举交出（空名子项）；name = `d`；计入 files / bytes；身份 `PathScoped(len‖"p/d/"‖ETag)` 或 `VersionScoped`，不与文件 `p/d`、目录 `p/d` 冲突；快照格式不变 |
| D2 | S3 → 文件系统：目录照建；有内容的 `d/` 逐条跳过并记原因；零字节或最新为删除标记 → 无操作；由 `directory_object_action` 统一判定，引擎仍严格报 Unsupported |
| D3 | legacy 现在修：有内容的报 Error（不交出条目）；代价：terrasync 在这类桶上关闭删除检测 |
| D4 | 存储根 / 扫描根自己的标记：零字节（含全零历史）忽略；任一版本有内容 → `Failure{Child, Unsupported}` |
| D5 | 多版本：只要有记录就交出整段历史（有内容版本、零字节版本、删除标记全保留）；目的 S3（Enabled）按旧→新逐条重放：版本用 `with_source_version`，删除标记用 H1；MinIO / DXN 合并连续标记、DXN 删 null 版本 → 显式报告 |
| D6 | ONTAP NAS 桶作目的端：先实测两种写入顺序；若零字节 `d/` 把目录变成文件，加端点显式选项关掉 `directory_objects`（不用启发式） |
| D7 | DSL：type = file、name = 目录名、path 带尾 `/`；`size > 0` 区分有内容的 |
| D8 | 零字节标记与全零历史也在 S3 → S3 重建（空文件夹得以保留；文件数 / 入库行数上升） |
| D9 | 顺序：F5a → F5b–F5f → H1，都在 C7 之前 |
| D10 | 新原语“目的端写删除标记”（仅 Enabled 桶，每次调用前检查版本状态） |
| D11 | 历史重放由调用方（terrasync）编排，data-mover 提供原语与参考示例；重放不去重，重跑前调用方比对目的端历史 |
| D12 | 目的桶未开版本：只拷最新状态并记原因“目的桶未开版本，历史未重建”，不算失败 |
| D13 | （用户 2026-09-26 补定）目录对象**只用单次请求写**：单 PUT 或单次 CopyObject，不走 multipart / UploadPartCopy；> 5 GiB 在 Preflight、I/O 之前逐条目 `Unsupported`，不回退。一并避开 MinIO 拒绝 multipart `d/` 与 RustFS 把 multipart `d/` 存成 `d`（覆盖同名文件）；无续传，失败从零开始。消费方可事先用 `directory_object_action(&entry, &目的端 Storage)` 得到 `Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON)`（P7 收尾补，见下“F5 收尾”） |

**实施结果（2026-09-26）**：F5a–F5f 与 H1 都已提交（§14）；真机矩阵用含 H1 的树跑。与计划的差别：扫描根自己的对象由遍历在根列举里处理
（`stream::prepare`，Blocks；C10 的 Unordered 必须复用它）；页内自己的对象排最前（键 `d/` 比它下面的键都小，页仍是
S3 键序）；MinIO 版本桶里最新是删除标记的 `d/` 仍被 ListObjectsV2 列成无 ETag 的 0 字节 Contents（HEAD 404）→ 不当子项；
MinIO 对 `d/` 的 CreateMultipartUpload 回 `XMinioInvalidObjectName` → 该条目 `Unsupported`（F5d 的专门映射；F5g 起这种请求
不再发出，映射已删）。`ndx_walk` 仍拒绝 S3，它的 `entry_name` 对 `p/d/` 会给空名：放开 S3 前要一起修。

真机矩阵（`directory_objects.sh`，2026-09-26；多版本 = 临时版本桶或 `data-mover-test-ver`，非多版本 = `data-mover-test`）：

| 存储 | 多版本 | 非多版本 | 差异 |
|---|---|---|---|
| MinIO RELEASE.2023-03-20 | pass | pass | 拒绝对 `d/` 发 multipart（400 `XMinioInvalidObjectName`，单 PUT 可以）→ > 8 MiB 的 `d/` 写不了（typed `Unsupported`；F5g 起只发单次请求，已可写）；删除后的 `d/` 在 V2 列举里留无 ETag 的幻影、父目录仍列前缀；（H1 重放）合并连续删除标记，null 版本探测照常写标记 |
| ONTAP 9.19.1 对象桶 | pass | pass | 一切按 S3；（H1）不合并连续删除标记 |
| DXN（Ceph RGW Octopus） | pass | pass | UploadPartCopy 的 native 拷贝 `d/` 正常；（H1）合并连续删除标记；只有 null 版本的 key 重开版本、写新版本后再普通 DELETE：删掉 null 版本**并**写标记 |
| StorageGRID 11.5 | pass | pass | `s3+sg` 的“native”走流式；ListObjectVersions 拒空 `version-id-marker`；（H1）已删除的 key 再 DELETE 每次新建标记（不合并） |
| RustFS 1.0.0 | pass（脚本按探测到的差异断言） | pass（同左） | **对 `d/` 的 multipart 完成后存成 `d`（去掉 `/`，HEAD `d/` 404）**：我们的读回报 Verify `NotFound`（typed），但对象落在了同名文件 `d` 上 —— 会覆盖真实的文件 `d`（F5g 起不再发 multipart，真实文件不动）；**版本桶里不给 `d/` 记版本**（每次写都是 `null`，DELETE 直接删掉）；native 从 `d/` 源 UploadPartCopy：版本桶里失败（会话 Connectivity，stage 与父目录指针留待续传），非版本桶里完成但同样存成 `d`；（H1）对未记版本的 `d/` 写删除标记不留记录 |
| ONTAP NAS 桶 `data-mover-nas`（D6） | — | 只记录 | 零字节 `d/`：PUT 200 但不留对象（只是目录 `d`），子项前后写都一样；有内容 `d/` 先写：存成**文件** `d`，之后 `d/c` PUT 404；有内容 `d/` 后写（子项已在）：PUT 409 Conflict |

**F5g 实施结果（D13，`2352a6b`）**：S3 目的端对以 `/` 结尾的最终键一律走单次请求 —— 流式（Checkpointed / AtomicReplace）与
`Direct` 一次 `PutObject`，native 一次 `CopyObject`，不看单 PUT 阈值（8 MiB）与 native 单次拷贝上限（64 MiB）；不写指针、不续传，
失败从零开始（`staged/directory_object.rs::single_request_size`，at_destination / direct / native_final 三个 prepare 共用）。
> 5 GiB 或源大小未知：新 `StagedDestination::directory_object_limit()`（默认 `None`，S3 = 5 GiB），引擎在描述源之后、租约与目的端
任何 I/O 之前报 **Preflight**、目的端、条目 `Unsupported` / Permanent（`engine/directory_object.rs::refuse_oversized`，N2 起并入
`refuse_unwritable`；普通入口与
expert 目的端半边都查），不回退；S3 prepare 自己也在 I/O 前同样拒（直接调用角色时为 Prepare）。协议层兜底：role
`begin_multipart` / `complete_multipart` 对拼上前缀后以 `/` 结尾的键不发请求、回条目 `Unsupported`；legacy
`create_multipart_upload` / `multipart_begin` / multipart rename / 可续传路径（接管已开的上传之前）/ `complete_multipart_upload`
回 `UnsupportedType`（legacy 遍历本来不交出这种键，这是给把它交给 legacy 写入的调用方的兜底）。F5d 的 MinIO
`XMinioInvalidObjectName` 专门映射（`begin_failure`）因不可达而删除。prepare 仍按父目录里的指针名清掉 F5g 之前的写入者留下的指针与
上传（`directory_object_artifact` 保留）。前置 refactor `870f385`：`unsupported_at_preflight` 从 `refuse_unholdable` 提出。
代价（需知）：流式目录对象与所有单 PUT 一样先整个缓冲在内存再发，每个进行中的传输最多 5 GiB；native 不缓冲
（**F5h 已消除**：超过单 PUT 阈值的目录对象改为一次流式 PutObject，见 §14 F5h 行）。Checkpointed 的
结果报 `SkippedBelowCheckpointThreshold`（单块时 `SkippedSingleSourceChunk`）。
测试：fake 加 RustFS 旋钮（完成的 multipart `x/` 存成 `x`），目的端旁放真实文件 `dir/obj`：0 B / 5 B / 24 MiB 流式（两种 resume
模式）、Direct 5 B / 24 MiB、native 3 B / 20 MiB（单次拷贝上限调到 5 MiB）都是一次请求、multipart / part / part copy 计数 0、真实
文件不动；> 5 GiB 与大小未知在三个 prepare 都 `Unsupported`、植入的旧指针仍在；F5g 之前留下的指针与上传在 prepare 被清；引擎：12 MiB
`d/` 在 Checkpointed / AtomicReplace / Direct / native 都一次请求，稀疏 5 GiB + 1 的本地源 → `d/` 在 Preflight 拒、旧指针仍在，
expert 目的端同；线上：role 与 legacy 各入口对 `p/d/` 零请求。故障注入 13 项（去掉任一路由、引擎 / expert 守卫、协议 / legacy
各守卫、5 GiB 上限、`directory_object_limit`，或把前缀拼接后的检查退回相对键）各让至少一个测试变红。
真机（2026-09-26，`directory_objects.sh` 全矩阵 10 次：MinIO、RustFS 1.0.0、ONTAP 9.19.1 对象桶、DXN、StorageGRID 11.5 的版本与
非版本桶）全部 `RESULT: pass`，清理后无 `dm-` 残留、无临时桶：12 MiB `mid/` 本地→S3 三种 policy、200 MiB `big/` 流式（切断后
不留对象 / 指针 / 上传、重跑 `Fresh`）与 native（StorageGRID 为流式）都是一次请求（ETag 无 `-N`）、逐字节相等，同名真实文件
`mid` / `big` 在 **RustFS** 与其余各家都不动；MinIO 上 > 8 MiB 的 `d/` 现在写得进去。

**F5 收尾：`directory_object_action` 事先回答“太大”（2026-09-27，`4dabfe0`）**：F5e 的辅助只看 `NamespaceCapabilities`，对 S3
目的端一律 `Copy`，消费方要等 D13 的 Preflight `Unsupported` 才知道 > 5 GiB 的目录对象写不了。现在第二个参数是
`impl Into<DirectoryObjectDestination>`（`holds` = `NamespaceCapabilities::directory_objects`，`limit` =
`StagedDestination::directory_object_limit()`）：传目的端 `Storage` 时两者都有（无 I/O），能存目录对象的目的端上超过上限或
大小未知的目录对象答 `Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON)`，删除标记仍 `Copy`（重放，不写字节），5 GiB 整 `Copy`；
只传 `&NamespaceCapabilities`（旧写法，照常编译）时不知道上限，答案与以前相同；取不到 staged destination 的 `Storage` 也当
没有上限（这样的传输本来就在 Preflight 失败）。辅助与引擎 `refuse_unwritable` 用同一条规则（`over_limit`：有上限且大小未知或
超过）与同一诊断（`DIRECTORY_OBJECT_TOO_LARGE_REASON`），所以列举到的大小与描述源时相同的条目，辅助答 `Copy` 就不会因为大小
在 Preflight 被拒。文件系统目的端、以及不存目录对象却报上限的目的端（NAS 桶）的答案不变。

D6 结论（N1 起按此实现：NAS profile 的 `directory_objects=false`，`d/` 在 Preflight 拒）：零字节 `d/` 不会把目录变成文件（D6 的触发条件不成立）；**有内容的 `d/` 若先于子项写入，NAS 桶静默把它存成文件
`d`**（我们的读回会报 `NotFound`，但文件 `d` 已在、之后子项全 404）。Blocks 下自己的对象在子项之前交出，按遍历顺序拷贝
的消费方正好命中这种顺序。**用户 2026-09-26 定：D13（目录对象只用单次请求写，F5g `2352a6b` 已实现）**，不加端点开关；NAS 桶由 §14.2 的 NAS profile（`directory_objects=false`）处理。

风险要点：消费方若规范化尾 `/`，`p/d/` 与 `p/d` 会合并 → §4.1 写明路径逐字节比较、terrasync 要求同步；空名子项只经专用构造器；
DeleteObject 在非 Enabled 桶上会删数据 → 每次调用前查；指针放父目录是 ADR-0006 新规则（`d` 与 `d/` 的 artifact 不冲突要有测试）。
验收：`.claude/skills/e2e-s3/scripts/directory_objects.sh`（dirobj/、lonely/、emptydir/、全零历史 z/、混合历史 m/、console 空文件夹 e/），
四套对象存储的多版本与非多版本桶 + NAS 桶对照；`examples/s3_history_replay.rs` 把混合历史重放进临时版本桶后逐条比对。

### 14.2 S3 URL 参数化与 ONTAP NAS 桶 profile（2026-09-26，用户全部采纳 16 条建议）

**URL 语法**（一个解析器 `src/s3/url_options.rs`，legacy 与 role 共用）：协议名只剩 `s3://`（http）与 `s3+https://`；查询参数
`compat=sg|dxn|hcp|ontap`、`tls_verify=false`、`bucket_type=nas`，**缺省即默认**（写 `compat=standard` / `bucket_type=object` /
`tls_verify=true` 报 UrlParseError，提示“缺省即默认”）；未知参数名、重复参数、空值、`tls_verify` 用在 `s3://` 上、`bucket_type=nas`
不带 `compat=ontap` 都是 UrlParseError（D4 规则文字相应修订：URL 文本能判定的组合错误都算 UrlParseError）。删除 `s3+http://`、
`s3+sg[+https]://`、`s3+dxn[+https]://`、`s3+hcp://`，报错给出新写法（旧 `+https` 形式的提示里带 `tls_verify=false`）。参数不进端点身份
（TransferIdentity / 恢复绑定不变）。**TLS**：`s3+https://` 默认校验证书（系统根，认 SSL_CERT_FILE / SSL_CERT_DIR），`tls_verify=false` 显式关闭
并每连接告警；自签端点（DXN 8182 等）必须加它；terrasync 配置要迁移。**HCP**：`StorageType::Hcp` 并入 `compat=hcp`；REST 的 scheme / 端口
跟随 S3 URL；role 层 get_tags 改走 HCP REST（无实验环境，只能单测）。**`compat=ontap`**：不改任何线上请求（ONTAP 对象桶的怪癖都已通用处理：
F4、F6、空末页、CommonPrefix 计入 max-keys、不回显 Delimiter），只承载 `bucket_type=nas`、原生拷贝亲和标签 "ontap" 与 403 提示。

**NAS 桶 profile（`compat=ontap&bucket_type=nas`）**，能力开关 `S3BucketCapabilities`（不按厂商名分支）：
- 目的端：≤ 8 MiB 照旧单 PUT；8 MiB–5 GiB **流式单 PUT**（Content-Length = 源大小、UNSIGNED-PAYLOAD、边传边算 MD5 与 PUT 回复 ETag 核对、
  卡住看门狗）；**扣住最后 1 字节到 publish 才提交**（前提：N0 证明 NAS PUT 原子，否则所有流式失败都报“已改变”）；> 5 GiB 在 Preflight、
  目的端 I/O 之前 Unsupported 拒绝，不回退。**无续传**：`resumable_at_destination()=false`，Checkpointed 报 `EffectiveRecovery::UnsupportedByDestination`
  （该枚举加 `#[non_exhaustive]`）、从零开始。**prepare 多一次 HEAD** 记旧状态，结果不确定的失败再 HEAD：未变 / 已变 精确报出。
- 校验：PUT 回复的 MD5 ETag 与自算 MD5 核对 + “ETag 括号式”读回（HEAD 定锚、每段响应 ETag 必须等于锚、BLAKE3 比对）：ETag 变 → Conflict，
  内容不同而 ETag 稳定 → Corruption（报错写明有歧义）；结果记进新的 `TransferOutcome.verification`，不算失败。
- 源端：无 If-Match；每段核对响应 ETag、Content-Range 总长、Last-Modified，**读完再 HEAD 一次**关掉最后一段的窗口；NFS / SMB 写是否改 S3 ETag 待 N0。
- 原生拷贝只限同一 NAS 桶（亲和含桶名），单次 CopyObject ≤ 5 GiB；跨桶自动走流式。无版本、`directory_objects=false`（F5 D6）。
- 连接时仍查 GetBucketVersioning，只告警；参数写在对象桶上照常工作（保证变弱），NAS 桶漏写参数则按今天的失败 + 精确提示（501 / 403 提示加参数）。
- legacy StorageEnum 对 NAS 桶不改行为，只写文档。TLS 测试用本地自签 TLS 服务器（新增 dev-dependency tokio-rustls）+ DXN 8182 真机。

**顺序**：N0 实测立即（curl，a–l：UNSIGNED-PAYLOAD 200 MiB、PUT 中途被杀旧对象是否完好、短体、各处 ETag 是否一致、GET 响应头、5 GiB、零字节 `d/`
两种顺序、NFS 写是否改 ETag、空目录是否为 CommonPrefix、1 GiB CopyObject、名字冲突、PUT 期间 GET）→ P1–P5 在 F5 合入后（可与 C7 并行）→
N1–N7 在 H1、C7 之后。完整方案（file:line）见会话 scratchpad `s3-url-nas-plan.md`（会话结束即失效，要点以本节为准）。

**P1–P5 实施记录（2026-09-26）**：解析器 `src/s3/url_options.rs`（表测 `url_options_tests.rs`），TLS 代码在 `src/s3/tls.rs`。
- `bucket_type=nas` 解析进 `S3UrlOptions`，`S3Storage::new`（legacy 与 role 共用）在任何 I/O 之前报 `UnsupportedType`，N1 去掉这道闸；
  端点身份照常可离线求出。
- 证书校验失败在连接时（GetBucketVersioning 探测）报 `ConfigError`（不重试），写明 `SSL_CERT_FILE` / `tls_verify=false`；只认连接层错误。
- 发现：改前的不校验客户端用 `ClientConfig::builder()`，依赖图同时开了 ring 与 aws-lc-rs，进程没装默认 provider 时一连接就 panic
  （故障注入复现）；现显式用 ring。
- HCP：原生拷贝亲和标签仍是 "standard"（与改前相同）；role 层 get_tags 带 `version=` 读固定版本（未在真机验证）；
  `HCPRestClient::try_new` 改用 `HcpTransport`。
- 真机（2026-09-27，e2e-s3 `run.py`，只列举）：MinIO `s3://`、ONTAP `?compat=ontap`（`data-mover-test` 与 `-ver`）、RustFS、
  DXN `?compat=dxn`（8184）、DXN 8182 `s3+https://…?compat=dxn&tls_verify=false` 全部通过；DXN 8182 不带 `tls_verify=false` 在连接时报
  `ConfigError`（`invalid peer certificate: UnknownIssuer`，带两条出路）；StorageGRID `?compat=sg` 通过；五个旧协议名都报出新写法、
  不含凭据。ONTAP 两个 `compat=ontap` URL 之间 S3→S3 走 Native（写入的对象已清理）。HCP 无实验环境。

**HCP 支持删除（用户 2026-09-27）**：上文与 P1–P5 记录里的 `compat=hcp`、HCP REST 注解读标签（P4）、`s3+hcp://` 的改写提示都作废。
`S3Compatibility::Hcp`、`src/third_party/hcp`（`HCPRestClient`，公开模块 `data_mover::third_party` 整个删除）、role 层 `hcp_tags.rs`、
legacy 的 HCP 标签分支、“HCP 桶一律开版本”的捷径、直接依赖 reqwest 0.13 全部删除（reqwest 0.12 仍经 smb → sspi 引入）；`s3+hcp://` 与 `compat=hcp` 报 UrlParseError，提示按标准 S3
写 `s3://…`。

**N0 实测结论（2026-09-26，ONTAP 9.19.1，`data-mover-nas`，a–l 全部测完并清理）与对上文的修订**：
- **决定 4 成立：NAS PUT 原子**。在 10 MiB / 100 MiB / 150 MiB / 差 1 字节处杀客户端、半关、close、Content-MD5 或签名 sha256 不符（400
  BadDigest，收完 body 才拒）都不改变对象；暂存在 S3 与 NFS 上都不可见，失败后约 25 s 释放空间。扣住最后 1 字节：服务器至少静等 900 s
  （上限未测出），补发后约 0.04 s 提交，回复 ETag = MD5。**实现要求**：放弃时直接关连接；客户端自带看门狗（服务器不会先断，NAT / 代理可能会）。
- **决定 7：NFS 写会改 S3 ETag，但只经由 mtime（纳秒）与 size**。HEAD / GET / 列举 / 拷贝回复的 ETag = f(mtime, size)，与内容、inode、路径无关：
  chmod 不变；改内容后把 mtime 恢复（`rsync -t`、`touch -d`、utimes）则 ETag、Last-Modified、size 全不变；两个不同文件 mtime 与 size 相同则 ETag
  相同。所以“内容不同而 ETag 稳定 → Corruption（有歧义）”是真实会出现的情形，报错文字照上文保留歧义说明。SMB 未测，按同一 inode 属性推断相同。
- **源端逐段读**：替换在读的中途提交时，服务器在提交后 0.3–0.7 s 断开进行中的 GET，已收字节是旧内容的干净前缀、短于 Content-Length（从未混入新内容）。
  **body 短于 Content-Length / Content-Range 按“已改变”（Conflict）判**，不当可重试的断连；Last-Modified 只到秒，变化只能靠 ETag 看出。
  **修订（2026-09-27，协调决定）**：body 断掉（短于 Content-Length / Content-Range、读 body 时连接重置）后先 `HEAD` 一次：ETag（或大小、
  两边都有时的 Last-Modified）与描述时不同 → `Conflict` / Permanent；相同 → 普通断连（会话 `Connectivity` / Transient，可重试）；`HEAD`
  本身失败 → 按它自己的分类（`storage/backends/s3/broken_body.rs`，见下文「N1–N7、F5h 实施记录」之后的跟进）。
- **> 5 GiB 不是服务器限制**：5 GiB + 1 写入并回读正确；服务器不预检大小与剩余空间（Content-Length 5 TiB + 1 也回 100 Continue）。**保留**
  “> 5 GiB 在 Preflight 拒绝”作为设计选择（卷满行为未测，单 PUT 失败即从零开始）；放宽另议。
- chunked 编码 → 501，流式单 PUT 必须带 Content-Length（与上文一致）；UNSIGNED-PAYLOAD 接受（200 MiB、3 次、内容正确）。aws-chunked 签名未测。
- **新增：NAS 目的端名字预检**（N 阶段加进 Preflight 与逐条目 prepare，I/O 之前逐条目 `Unsupported` / Permanent，不回退）：
  - 服务端**静默规范化 key**：`a/../x` 存成 `x` 的上级、`a/./y` → `a/y`、`a//b` → `a/b`（`%2E%2E`、`%2F` 同样处理）→ 含空段、`.`、`..` 段的 key 拒绝，
    否则会写到别的 key 甚至上级目录；
  - 名字段 > 255 字节 → 服务器 500 InternalError（看起来可重试，实际不是）→ 预检拒绝；
  - 非法 UTF-8 → 403 AccessDenied（data-mover 的 key 都是合法 UTF-8，只记录）；
  - 文件 / 目录冲突：已有文件 `a` 时 PUT `a/x` → **404 NoSuchKey**，已有 `a/x` 时 PUT `a` 或带内容的 `a/` → **409 NameCollision** → 这两种回复在 NAS
    profile 下映射为该条目的名字冲突（Permanent），不当“不存在”或可重试；
  - 0 字节 `d/` 建出真目录（HEAD `d/` 404、平铺列举不可见、分隔符列举是 CommonPrefix）；带内容的 `d/` 存成文件 `d`（与 `directory_objects=false`
    一致，F5 D6）。空目录只在带分隔符的列举里以 CommonPrefix 出现。
- 同桶 CopyObject 是克隆（1 GiB 0.01 s，副本互相独立，回复 ETag = 目标 HEAD 的不透明值）：原生拷贝亲和值得保留；> 5 GiB CopyObject 未测，维持单次 ≤ 5 GiB。
- 其余：Range（含 `-100`、多段 → multipart/byteranges）与 416 正常；列举的 LastModified 截到秒（`.000Z`）；256 字节以上的 UTF-8 名字在 NFS 上
  被改名（`:S400001`），两个 key 指向同一对象；`dm_s3_nas` 每小时 :05 打快照，大对象测试须在下个快照前删除。测量脚本不含凭据，见 N0 报告。

**N1–N7、F5h 实施记录（2026-09-27）**（提交：N1 `927cc03`、N2 `9a9447d`、N3 `4c8750c`、N4 `9d97837`、N5 `3129902`、
N6 `d51af50`、N7 `ce49448`、F5h `7260974`；细节见 `.claude/docs/storage-s3.md` URL 段的 N1–N6 与「ONTAP S3 NAS 桶实测行为」）：
- 能力开关 `S3BucketCapabilities`（`storage/backends/s3/capabilities.rs`）：conditional_reads / multipart / resumable / max_object_bytes /
  file_backed_keys / md5_etags / versions / directory_objects / native_same_bucket_only，OBJECT 与 NAS 两行；`S3Protocol::bucket_capabilities()`
  由 `bucket_type` 决定（`compat` 只改请求）。
- 引擎（破坏性）：`StagedDestination::resumable_at_destination()` / `final_refusal(path, size)`（N2）与 `read_back_method(stage)` /
  `upload_digest(stage)`（N4）四个默认方法 —— 不给 `VerificationEvidence` / `PublicationEvidence` 加字段（它们在各 backend 里按字面量构造）；
  `EffectiveRecovery::UnsupportedByDestination` + `#[non_exhaustive]`；`TransferOutcome.verification`（`ReadBackMethod` / `UploadDigest`）。
  名字 / 大小预检与目录对象上限合并在 `refuse_unwritable`（Preflight，目的端任何 I/O 之前），三个 S3 prepare 再查一遍。
- 流式单 PUT（N3，通用）：`S3Protocol::put_object_streaming` + `staged/streaming.rs`；实现中补的规则：SDK 的卡顿保护（stalled stream
  protection）也要对这个请求关掉（否则扣住最后 1 字节几秒就被杀）；本请求强制 `WhenRequired` 校验和（否则 body 变 aws-chunked，NAS 拒）；
  body 在凑满声明长度前被放弃时报错而不是正常结束；扣字节最长 300 s（`HOLD_LIMIT`，低于实测的 900 s）；publish 前请求若已结束（存储因 body
  空闲断连、提前拒绝）报未变；结果不明的发布按 prepare HEAD 结算时隔 2 s 再 HEAD 一次才报未变（`SETTLE_GRACE`，存储已有完整 body，
  回复丢失后仍可能提交）；Direct 等输入结束才送最后 1 字节。
- 与计划的差别：NAS 源端 body 读到一半出错也按 N0 决定报 `Conflict`（普通断连同样不重试，诊断带原始错误）—— 见“待定”（2026-09-27 已收窄为先 `HEAD` 再分类，见「N 系列跟进」）；
  名字冲突映射认错误码（NoSuchKey / NameCollision），无错误码时才看 404 / 409；原生拷贝读回的锚必须等于拷贝回复（设过标签则是之后 HEAD —— 2026-09-27 实测设标签不改 ETag 后去掉了这次 HEAD，见「N 系列跟进」）的 ETag；
  NAS 桶上 `keeps_history` 不发 GetBucketVersioning；Direct 不做 prepare HEAD（它的失败一律报已变）。
- 故障注入：N1 14 项、N2 11 项（1 项防御性、不可达，存活）、N3 14 项（`disable_payload_signing` 一项存活：SDK 对不可重放的 body 本来就发
  UNSIGNED-PAYLOAD，显式关签名是双保险）、N4 9 项、N5 4 项、N6 4 项、F5h 5 项，其余都让至少一个测试变红，改回后逐字节还原。
- 真机（2026-09-27，从 10.131.9.10 跑，它到各端点直连）：
  - `nas_matrix.sh`（`data-mover-nas`）三轮 `RESULT: pass`（42 行）：0 B / 1 KiB / 8 MiB+1 / 200 MiB × 三种 policy × 读回开关，结果报
    `EtagBracketed { ConditionalReadsUnsupported }` / `NotPerformed`、`upload_digest = Matched`、Checkpointed `UnsupportedByDestination`，
    内容逐字节相等；1 GiB 流式单 PUT（读回开，39 s）与同桶原生 1 GiB 一次 CopyObject（25 s，读回开）；5 GiB + 1 与 `a/../x`、`a/./y`、`a//b`、
    256 字节段在 Preflight 拒且未写；有文件 `clash` 时写 `clash/x` → `Conflict`；200 MiB 在 4 s 处取消与 SIGKILL 后旧对象 ETag 不变，重跑
    `Fresh` + `UnsupportedByDestination`；NAS → 本地 200 MiB 相等、读的中途被 PUT 替换 → `Conflict`；同桶原生 1 KiB / 200 MiB 各一次
    CopyObject；NAS ↔ `data-mover-test` 走 Streaming；参数写在对象桶上照常工作（Checkpointed 报 UnsupportedByDestination、括号式读回）；
    NAS 桶漏写参数：8 MiB+1 在 Prepare 报 PermissionDenied，诊断带 `add ?compat=ontap&bucket_type=nas`。跑完 `nas-<run>/` 下 0 个对象 / 目录。
  - 首轮冒烟（同一天较早）8 MiB 以上的流式行曾在 20 s 看门狗处失败（Connectivity / Transient），同一时段小请求也要 1–3 s、TCP 建连 3 s；
    之后 curl 200 MiB 三次均约 1.8 s，冒烟与三轮全矩阵全部通过 —— 判断为当时的网络状况，记录在案（看门狗按设计报可重试）。
  - F5h：`directory_objects.sh` 在 MinIO（临时版本桶 / `data-mover-test`）、ONTAP 对象桶（`data-mover-test-ver` / `data-mover-test`）、RustFS
    （临时版本桶 / `data-mover-test`）、DXN（临时版本桶 / `data-mover-test`）、StorageGRID（`data-mover-test-ver` / `data-mover-test`）十次全部
    `RESULT: pass`：12 MiB `mid/` 三种 policy 都走流式单 PUT（UNSIGNED-PAYLOAD，一次请求、ETag 无 `-N`、相等），200 MiB `big/` 切断后不留对象、
    重跑 `Fresh` —— 五套存储都接受 UNSIGNED-PAYLOAD 的流式单 PUT（明文 HTTP）。
  - 回归（同一棵树，F5h 之后）：`run.py` 在 MinIO、ONTAP 对象桶、ONTAP NAS 桶（`bucket_type=nas`）、RustFS、DXN、StorageGRID 全部
    `all checks passed`；`staged_matrix.sh` 在 MinIO、ONTAP `data-mover-test`、RustFS、DXN、StorageGRID 各 40 行成功（唯一失败行是预期的
    取消），Direct `equal=yes key_uploads=0`、原生 `equal=yes stage_objects=0 key_uploads=0`、取消与 SIGKILL 后 1 指针 + 1 upload、续传后
    `0 / 0`、末行 `left: objects=0 key_uploads=0`；`traversal_versions.sh` 在 MinIO / ONTAP / RustFS / DXN 非版本桶、ONTAP `data-mover-test-ver`、
    NAS 桶（`bucket_type=nas`，`EXPECT_DIR_MARKER=0`）全部 `RESULT: pass`；`nas_matrix.sh` 第四轮 `RESULT: pass`。跑完核对：各存储的桶与改前相同
    （MinIO 另有别人建的 `data-mover-c10-1790460684`，ONTAP `data-mover-test-ver` 下别人的 `dm-1790460935/`，都不是本批写的，未动），我们的前缀无残留。
    `versioning_matrix.sh` 未重跑（版本化桶的写入路径除结算函数的重构外未变，结算在 MD5 ETag 的桶上行为不变）。
- ~~待定（需用户定）：NAS 源端读 body 途中出错一律报 `Conflict` / Permanent~~ —— 2026-09-27 协调决定采用收窄方案，已实现（见下条）。

**N 系列跟进（2026-09-27）**：
- NAS 源端 body 断掉后 `HEAD` 一次再分类（上面「待定」的收窄方案）：变了 → `Conflict` / Permanent；没变 → 会话 `Connectivity` / Transient；
  `HEAD` 失败 → 保留其分类。判定是共用的纯函数 `broken_body_failure`（`storage/backends/s3/broken_body.rs`），真协议
  （`role_protocol/unpinned.rs::read`）与 `MemoryS3` NAS 模式（`BodyCut::{Dropped, Replaced}`）都调它；`get_range` 的三个调用方（源端读、
  括号式读回、`.upload` 指针读 —— NAS 桶不读指针，实际走不到）同样受益。body 断开后 `HEAD` 回 404 报条目 `NotFound`，
  读完后的确认 `HEAD` 回 404 仍报 `Conflict`：都是条目 Permanent，按决定保留 `HEAD` 自己的类别。只多一个请求、只在 body 出错时发。
- NAS 桶上设标签不动 ETag（真机实测，ONTAP 9.19.1，`data-mover-nas`，curl）：1 KiB 对象 `PutObjectTagging` 三轮（每轮隔 2 s、值不同）
  与 `DELETE ?tagging` 后 HEAD 的 `ETag` 与 `Last-Modified` 都不变（标签确实写入，`GET ?tagging` 读到新值）；`CopyObject` 带
  `x-amz-metadata-directive: REPLACE` + `x-amz-meta-foo` + `x-amz-tagging-directive: REPLACE` + `x-amz-tagging`：回复 `ETag` = 之后 HEAD 的
  `ETag`，元数据与标签都存上（HEAD 带 `x-amz-meta-foo`、`x-amz-tagging-count: 1`）；data-mover 发的 REPLACE 不带元数据 / 标签的拷贝同样
  回复 = HEAD；两种拷贝之后再设标签，`ETag` 都不变。对照：自拷贝（`a → a`，REPLACE 换元数据）**会**换新 `ETag` 与 `Last-Modified`
  （内容不变）—— 元数据改写在 NAS 桶上等于一次写，data-mover 不发这种请求。据此去掉 N5 原生拷贝设标签后的那次 HEAD
  （`native_final.rs::send_copy`）：读回直接锚定拷贝回复；若将来某版本设标签改了 ETag，读回报 `Conflict`「replaced after the copy」
  （报错偏严，不会漏报）。探测前缀 `nas-tagprobe-<ts>/` 跑完 0 个对象、目录已删。
- 故障注入：body 分类 12 项（换向、忽略大小 / Last-Modified、HEAD 失败改判 Conflict / 改范围、未变判成条目、诊断丢原因、真协议不发 HEAD、
  真协议恢复旧行为、接受短 body、假桶不存替换、假桶不发 HEAD）与“设标签后又 HEAD”1 项，全部让至少一个测试变红，改回后逐字节还原。
- 真机（2026-09-27，从 10.131.9.10）：`nas_matrix.sh` 一轮 `RESULT: pass`（42 行；「NAS → 本地读的中途被 PUT 替换」仍报 `Conflict`，
  现经 body 断开后的 HEAD 判定；同桶原生 1 KiB / 200 MiB 各一次 CopyObject、读回开）；跑完 `nas-<run>/` 下 0 个对象；MinIO `run.py`
  `all checks passed`。


### 14.3 C11b / C12 与 legacy 清理收尾（2026-10-12）

**收尾已完成**：CI、下表可用环境的实测、MinIO 单 key 十万版本与扫描 cancel / SIGKILL 续跑均通过。
DXN-2 依用户决定作为环境受限项结束；StorageGRID 当前不可达。此阶段执行主机未加载 HDFS 实验配置，
HDFS 未运行；后续已找到 runner 的配置、修复 HA 并通过真机验收（§14.4），HDFS 不再列为环境受限。

最终生产代码验收基准为 `757c6c9`；`17dc58b` 只补充了扫描续跑脚本的整目录重放说明。各真机用例的实际源码修订见下表，
不把较早修订的实测写成最终修订复跑。清理移除了 NDX、`walkdir` / `walkdir_2`、旧 scan 消息与迭代器、HDFS 递归扫描以及
S3 旧版本扫描；调用方统一使用 role traversal。破坏性接口清单、删除提交与可恢复的性能基准见
[legacy 清理记录](p7-legacy-cleanup.md)。NFS 递归删除改用独立的原始目录遍历，包含隐藏传输产物；失败前排空已发出的
文件删除 RPC，首页可有界重试，已处理部分目录不从头重放。

`make ci` 在 `757c6c9` 完整通过：格式、架构边界、Cargo 产物选择回归（2 项）、全特性严格 clippy 零警告、workspace
测试（库测试 1,727 passed / 8 ignored，其余 integration 与 doctest 通过）、examples，以及本地端到端测试（20 + 4 项）。
版本查询定向测试 4/4 通过，用 SDK HTTP 响应回放覆盖正常 Enabled、空配置、
403 拒绝、501 不支持与 HTTP 200 错误 XML 根；最后一种确认为 SDK `ServiceError`，role 连接仍报
`Connectivity` / `Transient`，不会误选历史列举。

本轮修复了真机脚本误用旧二进制的问题：所有受影响的 shell 脚本从 Cargo JSON 的 `compiler-artifact.executable` 取
刚构建的 example 路径（`5a582de`），覆盖非默认 `CARGO_TARGET_DIR`、带空格的路径与配置的目标 triple；找不到产物或
构建失败即停止。回归用放在旧路径的毒化程序防止退回 `target/debug/examples`，并接入本地和托管 CI（`eaa61f6`）。

**S3 实测**（2026-10-12）：

| 环境 | 已通过用例与源码修订 | 边界 |
|---|---|---|
| MinIO | `traversal_versions.sh`、`directory_objects.sh`、十万版本测量：`235537e`；`versioning_matrix.sh`：`eaa61f6`；传输 `resume_matrix.sh`：`757c6c9` | 传输 cancel / SIGKILL 都从新 HOME 成功续传，BLAKE3 一致；扫描 cancel / SIGKILL 也通过，见下文 |
| ONTAP 对象桶 | `traversal_versions.sh`、`directory_objects.sh`：`235537e`；`versioning_matrix.sh`：`eaa61f6`；传输 `resume_matrix.sh`：`757c6c9` | 传输 cancel / SIGKILL 都从新 HOME 成功续传，BLAKE3 一致；扫描续跑另列 |
| ONTAP NAS 桶 | 非版本 `traversal_versions.sh`：`235537e`；`nas_matrix.sh`：`757c6c9` | 全部 21 行大小 / 策略 / 读回组合，以及 1 GiB / native、preflight / conflict、cancel / rerun、源与跨桶路线通过 |
| RustFS | `traversal_versions.sh`：`235537e`；`directory_objects.sh`：`757c6c9` | 临时桶及历史重放桶已删除 |
| DXN | `traversal_versions.sh`：`235537e`；`directory_objects.sh`：`757c6c9` | 后者使用必需的 `S3_LOCATION`；首次漏配在写入前退出，不算用例结果；临时桶及历史重放桶已删除 |
| StorageGRID | 未验证 | 预检在桶 / 前缀操作前报 `No route to host`；用户确认地址不变、当前不可测，未继续重试 |

`versioning_matrix.sh` 两套的中断复制均按预期取消后成功续传，其余场景均 `result=ok`。以上成功用例的临时桶或专用前缀
已验证清理，保留桶 `data-mover-c11b-1790508092` 未触碰。没有用传输续传结果代替扫描续跑结果。

**NAS 协议与 HDFS**：

| 环境 | 结果 | 证据与限制 |
|---|---|---|
| CIFS，`757c6c9` | 通过 | policy 2/2、namespace 2/2、capability 1/1、只读 1/1；生成的专用文件已清理 |
| ONTAP NFSv4.0 / v4.1，`757c6c9` | 通过 | 各用例全部通过；遍历 5,602 项，计数平衡、无失败 / 列举失败；export / option / mkdir 契约通过 |
| ONTAP NFSv3 | 通过 | 旧 profile 的 `noresvport=true` 与服务端 `mount_rootonly=enabled` 冲突；在当前 root Ubuntu 构建机改用 `version=3&noresvport=false` 后，完整 runner 遍历 5,602 项、计数平衡且零失败，export / 嵌套 mkdir-delete / mkdir 契约全过，临时目录已清理；源 LIF 与目标 LIF 各只读 depth-1 遍历均 9 项零失败。无需修改服务或代码 |
| DXN-2 NFSv3 / v4.0 | 环境受限，用户确认按此完成代码验收 | 原生 NFS 与既有 v3 / v4 导出已配置；底层 ThinkFS 故障（全部 MDS down、无 active IOA）阻塞可用性，当前没有 2049 监听 / mountd 注册。未验证本轮 NFS 用例；未改配置、未启停服务、未进行 NFS 写入。用户确认文件系统故障并要求停止该环境全部远端操作，已停止 |
| HDFS | 此阶段未运行 | 此阶段执行主机未加载实验配置，候选 RPC 端口拒绝连接；未进行产品测试写入。后续已找到 VM 102 的 Kerberos / HA 配置并完成环境修复，7 项产品用例与隔离根清理均通过，见 §14.4 |

**扫描检查点续跑**（独立于传输 `resume_matrix.sh`）：

| 环境 / 中断方式 | 结果 | 观察 |
|---|---|---|
| MinIO cancel | 通过 | `Blocks`、十个目录各 300 文件、`CANCEL_AFTER=500`：10 轮完成；完整基准 3,010 项，交付 4,820 项，重复 1,810 项，丢失 / 凭空新增均 0；前缀已清空 |
| ONTAP cancel | 通过 | 实际取消后从检查点续跑通过；专用前缀已清空 |
| ONTAP SIGKILL | 通过 | 二十个目录各 500 文件，`KILL_AFTER=0.15`、`EVERY=0.01`、`Blocks`：4 轮完成；基准 / 交付均 10,020 项，重复 / 丢失 / 凭空新增均 0；10 个检查点、最大 90 bytes，至少一个周期检查点先于中断 |
| MinIO SIGKILL | 通过 | 十万版本测试及清理完成后独立运行：二十个目录各 500 文件，`EVERY=0.01`、`KILL_AFTER=0.30`、`Blocks`、并发 8；实际 SIGKILL 后两轮完成，基准 / 交付均 10,020 项，重复 / 丢失 / 凭空新增均 0；8 个检查点、最大 90 bytes，中断前 5 个周期检查点；10,000 个 fixture key 已删除，前缀确认空 |

平铺 3,000 文件、每轮交付约 500 项就取消的早期用例在 50 轮内没有完成。这与 §9.3 / §12.9 的整目录重放契约一致：
活动目录必须在一轮内列完才能推进，故该 fixture 不能用于证明受支持的续跑进展；改为上述窄目录后通过。另一次临时 helper
把本地日志路径传成扫描 ROOT，在真正扫描前失败，该无效用例不计结果，1,001 个准备对象已删除。
MinIO 的时间控制组也不计为通过：`KILL_AFTER=0.15` 在 50 轮内没有完成，`0.50` / `0.75` 均一轮就完成、没有实际
中断续跑；三个临时前缀都已清理并确认空。最终通过证据是表中的 `0.30` 运行。

ONTAP SIGKILL 用例和较大 MinIO fixture 曾在自动清理时遇到暂时的新连接失败，**原因未明，未证实服务离线**。
后续专用清理工具分别确认精确前缀，删除 ONTAP 剩余 1,915 个 key，并确认两个前缀最终均为空；不把自动清理失败留作残留数据。

**MinIO 单 key 十万版本：通过**。16 个持久连接 worker 发出 100,000 次 PUT，全部成功、得到 100,000 个唯一版本 ID，
无上传错误，准备耗时 6,556.607 s。新旧两个二进制的 Blocks / Unordered 四次遍历均 `Completed`、计数平衡，
failed / listing-failure / interruption / vanished / truncated 均为 0。并发为 8，单次实测如下：

| 二进制 | 顺序 | 峰值 RSS（KB） | wall（s） |
|---|---|---:|---:|
| 新，`235537e` | Blocks | 138,408 | 76.18 |
| 旧，对照 artifact | Blocks | 177,064 | 77.75 |
| 新，`235537e` | Unordered | 137,488 | 76.94 |
| 旧，对照 artifact | Unordered | 183,824 | 78.17 |

完整历史断言通过：`big/k` 恰好一项、`history=0/1`，含 100,000 条记录、100,000 个唯一非 null 版本 ID，
恰好一个 latest、没有该 key 的删除标记，每条记录大小为 1。加上八条辅助 fixture 记录，完整输出共 100,008 条记录，
遍历总数为 100,006 个版本与 2 个删除标记。Current 投影的新旧输出相同：六个条目、四个存活 key
（`big/k`、`mixed/k`、`mixed/plain`、`top`），不输出 record 行。

版本与删除标记显式分批删除均无逐条错误；最终版本列举为空、删桶成功，随后桶 HEAD 为 404，清理完成。此前被 TERM
终止并清理的另一次十万版本准备不算通过；此处记录的是后来完整成功的一轮。

新二进制来自 `235537e57d4adccf99bdb789ea160c2378472547`，SHA-256 为
`cc236f8e33cd3c6208bc4c9d9f1db7f89b31ead2f583560bbb60e3ed7932980c`；旧 artifact 的 SHA-256 为
`1b009291348becff303b3018bae4fa5739282689153c13d3b3d78549a04f2aab`。旧二进制的精确编译标志与构建来源无法证明，
因此这些数值仅表示本轮各一次的 RSS / wall 实测，不构成受控性能基准或普遍性能结论，也不代替 §12 的历史验收。

本轮原始验证汇总：`/tmp/dm-c11b-final-validation.md`；十万版本报告：`/tmp/dm-c11b-final-scale.md`；
CI 日志：`/tmp/dm-c11b-757c6c9-make-ci.log`。
这些是当前验证机上的证据位置，不保证作为仓库文件长期保留。

### 14.4 C7c / Q4 补项与 HDFS 后续验收（2026-10-12）

**最终代码验收基准**：`2486d34`。C7c 首页前续读实现在 `c0ff649`，Q4 闭合目录块配对实现在 `46a8a4c`，
后续 `cf29771` 修正公开类型导入，`2486d34` 让测试复用既有身份工厂；最后两项未改变生产行为。
`cf29771` 的 Q4 定向测试 12/12 通过，包括真实 Local 双会话闭合块配对；NFS 列举定向测试 36/36 通过。
最终 `2486d34` 的 `make ci` exit 0：格式、架构依赖与 Cargo 产物选择回归、全特性严格 clippy、workspace 测试
（库 1,751 passed / 8 ignored，其余 integration / doctest 全过）、examples 和本地端到端（20 + 4 项）全部通过。
日志：`/tmp/dm-p7-2486d34-make-ci.log`。这包含 C7c 新增的 12 项首页前故障测试与 Q4 的 12 项配对测试。

**C7c 实测通过**：使用 `5fd59a8`（父提交 `c0ff649`）的独立 checkout 与 target，未修改源码 / 脚本，
ONTAP v3 / v4.0 / v4.1 的完整 runner 均 exit 0。每套遍历 5,602 项，计数平衡、失败 / 列举失败 / 中断均 0；
export、嵌套 mkdir-delete、权限分类、缓存删除后重建和 `.snapshot` 只读契约均通过，生成的专用目录已清理。
v3 使用 `version=3&noresvport=false`，v4.0 / v4.1 使用各自版本与 `noresvport=true`。原始验证与二进制哈希见
`/tmp/dm-p7-final-nfs-validation.md`；该报告记录的是 `5fd59a8` 的真机验证，不冒充后续 Q4 合入提交的重跑。
DXN-2 仍遵照用户要求停止远端操作，不把其历史通过记录当成本轮验证。

**HDFS 环境已恢复，真机验收通过**：已在 runner VM 102 找到实验 Kerberos / keytab / HA 配置，因此 §14.3 的
“未加载配置”只描述当时执行主机的状态。实际阻碍与用户授权的修复如下：

- 两个 NameNode 自身名字优先解析为 `127.0.1.1`，RPC 绑定 loopback。备份并修复两台 `/etc/hosts` 后，各重启
  NameNode 一次，RPC 恢复到管理地址 `10.131.9.30:9000` / `10.131.9.33:9000`；NN1 ZKFC 按需重启一次。
- 两个 DataNode 上的自身别名也优先解析为 loopback，使 ZooKeeper 的 3888 peer listener 绑错地址、没有 quorum。
  追加授权后，只删除两台的自身 loopback 别名，保留 `10.10.1.31` / `.32` 数据网映射，并补 NN2 管理地址映射；
  三个 ZooKeeper 按 NN1 → DN1 → DN2 逐台重启，恢复 quorum。
- 随后的 QJM Kerberos 错配是 hosts 同 IP 行里短名排在 FQDN 前：实际服务端主体为 `hdfs/FQDN@REALM`，客户端
  按反向解析预期 `hdfs/短名@REALM`。四台 HDFS 节点仅将三条 JournalNode IP 行上的既有 FQDN alias 移到首位，
  没有改 IP 或新增主体。每次修改都保留备份并核验 hash、正向与 canonical 解析；NameNode 随后自然恢复，无需追加重启。

验收前后，两个 NameNode 的 systemd 服务均为 active/running，PID 不变、`NRestarts=39` 不再增长；
这是**进程状态**。认证 HAAdmin 返回的是 nn1 active / nn2 standby，确认只有一个 HA active。
两个管理网 RPC 可达；ZooKeeper 为一 leader、二 follower，各节点 `imok`，peer listener 已绑定正确非 loopback 地址。
没有修改 keytab 或 Hadoop 配置，没有清理 / 重建数据或日志目录，也没有格式化或强制选主。

runner VM 102 使用从 `2486d34` 构建并核对 SHA-256 的两个测试二进制，以 `github-runner`、既有实验配置 / keytab
和独立 `nightly-` 根执行以下七项。每个精确过滤器都实际运行 1 项、0 失败并 exit 0，不把零测试匹配计为通过：

| 测试套件 | 结果 | 覆盖 |
|---|---|---|
| `hdfs_architecture_contract` | 4/4 | role 遍历 / 流式读写 / 校验 / 覆盖；重连后恢复已持久前缀；取消后的扫描续跑；有界乱序 writer 重排 |
| `hdfs_native_contract` | 3/3 | 文件与目录映射；只列一层目录；带深度限制的递归流式扫描 |

第一次将 writer 用例误投 native 二进制时选中 0 项，已排除；改用 architecture 二进制后该项真正通过。
`nightly_lab_cleanup_confined_run_root` 通过，最后一次只读根状态探测返回 `FileNotFound`，确认隔离根不存在。
该探测因“根应存在”的断言而非零退出，是预期的缺失证据，不冒充测试通过；修正 writer 用例后又执行了清理与只读确认。

最终报告：`/tmp/dm-p7-final-validation.md`；逐项原始日志在
`/tmp/dm-p7-hdfs-acceptance-2486d34-nightly-luna-20261012-82f39c1711/`。
architecture 二进制 SHA-256：`c0583253d61c19b50b9da9e2618adc3cdad7a9e8bd5746fd0bb96a6bebfe34b3`；
native 二进制 SHA-256：`67a5af0e4b710ec21279f51c334a51fa09e5585b19276f9b98b761b628915ee4`。
hosts 备份与 apply 记录保留在各实验节点 `/root/dm-p7-*`，未放入仓库。上述临时报告路径不保证长期保留。
HDFS 已解除环境限制；DXN-2 / StorageGRID 仍按 §14.3 保留边界，不为本轮收尾重新访问。

## 15. 决定记录（2026-09-25 全部采纳）

下面每一条都是已定结论。编号沿用讨论时的问题编号，便于对照过程稿。

### 15.1 扫描方式、事件与接口

| 编号 | 决定 | 见 |
|---|---|---|
| Q1 | 默认扫描方式为 `Unordered` | §3 |
| Q2 | 删除 `SubtreeComplete`（两种方式都删）；`DirectoryListed` 只在 Blocks 发，空目录也发 | §4 |
| Q3 | 两种方式都在 `TraversalCompletion` 给总数；Blocks 的 `DirectoryListed` 另带本块计数；Unordered 没有每目录计数 | §7 |
| Q4 | **已实现** `46a8a4c`：`traversal::pair_directory_blocks` 对两侧已关闭的目录块做哈希配对；显式目录映射，保留历史段与类型冲突，失败 / 过滤 / 名字语义不明时不推断缺失。会话完成与祖先失败等删除闸门仍由调用方执行 | §3.1 |
| Q5 | Blocks 下钻键 = 子目录名的原始 UTF-8 字节，不做大小写折叠、不做 Unicode 规范化 | §3 |
| Q6 | 名字语义由 `NamespaceCapabilities.names` 公开；不敏感的目标上“折叠后相等、字节不等”报冲突，不删除 | §4.1、§5.4.5 |
| Q23 | `rename` 保留在 Namespace | §5.4.1 |
| Q24 | `ListedChild.rank` 删除 | §5.4.4 |
| Q25 | 名字语义查不到（`Unknown`）按不敏感处理，更保守 | §5.4.5 |
| Q31 | `should_scan` 是额外项的集合（`Acl / Xattrs / Tags / SymlinkTarget`），没有 BestEffort：要了就必须拿到，单条拿不到发子项失败，backend 做不到则在 I/O 之前拒绝；列举顺带的信息（时间、uid/gid/mode 等）一律收下，并入 C0 修掉 NFS ownership 的额外 GETATTR | §3.2 |
| Q32 | HDFS ACL / xattr、S3 `x-amz-meta-*` 作为后续单独的元数据工作，不放进 P7 | §3.2 |
| Q33 | symlink 目标改为 `Extra::SymlinkTarget`，没要时为 `None` | §3.2 |

### 15.2 多版本与 S3

| 编号 | 决定 | 见 |
|---|---|---|
| Q7 | 一个 key 的历史内部以服务端列举顺序为准（反转成最旧在前、最新在后），不按 LastModified 排序 | §10 |
| Q8 | 单 key 记录上限 10 万条，超过时分段发多项（`complete: false` + 段号） | §10.4 |
| Q9 | 多版本桶按目录物化后组装历史，上限 500 万条记录，超限 `Capacity` | §5.3 |
| Q26 | 用 `GetBucketVersioning` 判定多版本（`Enabled` / `Suspended` → 列全部历史；从未开启 → 普通列举）；查询被拒时一律列全部历史 | §10.1 |
| Q27 | terrasync 落库时把一项展开成每版本一行，沿用现有表结构 | §10.4 |
| Q28 | **独立先修**：S3 `PathScoped` 身份改为 路径 + ETag，契约规定 `PathScoped` 必须含路径；terrasync 只对 `StableWithinBackend` 按身份键关联。**data-mover 侧已修：`4fa5aa5`** | §5.4.7 |
| Q29 | **独立先修**：ListObjectVersions 读原始 XML 取服务端位置，历史顺序完全不用 LastModified；校验不过的 key 标 `order: Inferred`；MinIO 并发实测响应存为回归 fixture。**已修：`40c1ac9`** | §10.2、§10.3 |
| Q12 | 列举可能重复或遗漏子项写进契约，消费方按 (父, 名) 去重；遍历只对子目录（S3 前缀）去重 | §5.3 |

### 15.3 资源、失败与续扫

| 编号 | 决定 | 见 |
|---|---|---|
| Q10 | CIFS 预取停车句柄上限 8（先在 FAS2750 上测 ONTAP 空闲回收）。**实测（C9，2026-09-27，WSL → FAS2750）**：打开的目录句柄停 30 分钟（会话完全空闲，或每 20 s 一个请求）后照常读完；SVM 的 `client_session_timeout` = 900 s，但持有打开文件的会话过了 900 s 仍在（ONTAP 会话表 idle > 15 分钟、open_files = 1）—— 不按空闲回收；没有打开文件的空闲会话约 900 s 后被服务端 RST。C9 构建的停车测试 1800 s：1500 个子项的目录首页后停 1800 s，照常读完（1500 条，26 ms），随后 20 次首页后 drop、每次后会话照常；同一进程里另一条没有打开文件的空闲连接在约 15 分钟时被 RST（即 900 s），smb-rs 重连时有后台任务 panic（`engine.rs:703` “async fn resumed after completion”，上游缺陷，base 的 raw probe 同样出现，与 C9 无关；smb-rs #84，`37d9404` 起已修）。会丢句柄的是连接断开：smb-rs 不重开目录句柄，重连后旧句柄 `InvalidState`（10.131.9.10 网段断连时观察到）。停车句柄的访问权：`37d9404` 起目录只读打开（smb-rs #85），不再持写 / 删除权 | §6.2 |
| Q11 | 新增 `FailureClass::Throttled`；单个列举累计退避约 2 分钟后报 `Interrupted` | §8 |
| Q12（消失） | 列举后消失的条目、父列出后已不存在的子目录计入 `vanished`，不发失败项，不破坏完整性 | §8 |
| Q13 | 同 Q12：重复 / 遗漏写进契约 | §5.4.3 |
| Q14 | 检查点默认每约 1 分钟发出一次；取消时补发一个 | §9.1 |
| Q15 | 遍历侧计数续扫后为近似值；精确的分布统计以库为准 | §7、§11 |
| Q16 | terrasync 写库改动在 terrasync 里做，本设计只列要求 | §11 |
| Q17 | 全量扫描同样走 generation 闸门：首次扫描没提交时下次仍按全量（或从检查点续扫）；取消 / 失败如实报错 | §11.4 |

### 15.4 依赖、顺序与验收

| 编号 | 决定 | 见 |
|---|---|---|
| Q18 | 改 nfs-rs 上游公开 READDIRPLUS 页接口（cookie + verifier），单独的 `chore(deps)` commit —— **已完成**：nfs-rs 0.8.5 `17bb6fb`，列举续读 C7c `26a27f6`（§14） | §5.2 |
| Q19 | 先做 C0，并在 node1 上补测 `8a1348b` 之后的 CPU / RSS 基线（含 `MALLOC_ARENA_MAX=2` 与驻留探针），再做破坏性重构 | §12 |
| Q20 | 验收：Unordered 与 legacy 基本持平（wall ≤ 1.05×、CPU ≤ 1.15×、RSS ≤ 30 MB），Blocks 不能差太多（wall ≤ 1.15×、CPU ≤ 1.4×、RSS ≤ 100 MB）；达不到按 O1–O6 优化 | §12.6 |
| Q21 | 写 ADR-0008：两种扫描方式、事件模型、续扫、每 key 历史 | §14 |

### 15.5 最后两项

| 编号 | 决定 / 结果 | 见 |
|---|---|---|
| Q22 | 无序 S3 的真实例子是 ONTAP **NAS 桶**（KB CONTAP-675913，实测平铺列举为目录遍历序）；ONTAP 对象桶开重删 / 压缩仍有序（实测）。保持“不假设有序”；NAS 桶 data-mover-nas 已进 S3 真机矩阵，另加 fake 打乱模式 | §5.3 |
| Q30 | 已批准并完成：DXN 与 StorageGRID 实测见 10.2.1。新发现 DXN 的 null 版本丢失缺陷与多网关时间倒挂 | §10.2.1 |

**目前没有待用户决定的问题。**

## 16. 附录：否决的方案与理由

| 方案 | 否决理由 |
|---|---|
| 块内按名字全排（P4 的 `NameBytes`） | 整块物化 + O(N log N)，首项要等整个目录；用户选了只排下钻 |
| 按列举顺序下钻（今天的 `Admission`） | 各 backend 访问目录的顺序不一致；排下钻几乎不花钱 |
| 排序键改为 git tree 序，让 S3 原生有序直接流 | S3 不一定有序；兄弟排序本身已不做 |
| Unordered 也发 `DirectoryListed` / `SubtreeComplete`（父指针计数树） | 用户确认 Unordered 不需要；省掉每目录两项与计数树 |
| 多版本“每版本一项 + 同 key 连续”（设计稿原先的建议） | 用户定为每 key 一项：历史是对象的属性；版本数无上限的问题用上限 + 分段解决 |
| 历史内按 LastModified 排序 | 时钟漂移、同毫秒并列、最新的一条不一定时间最大 |
| `versions: Current \| All` 参数 | 用户定为不需要：多版本桶一律列出全部历史 |
| 精确续扫（刀后恰好重发、epoch 列、续扫时 DELETE、有效性谓词、幂等写令牌） | 用户确认续扫不需要精确；重复由库去重吸收，只需保证进度不领先于落库 |
| 临时表改 `ReplacingMergeTree` | 每批一张新临时表 + 并入 base 已吸收跨批重复 |
| 续扫用协议位置（NFS cookie、S3 token、`StartAfter`） | 不能跨会话 / 进程可靠复用；`StartAfter` 依赖顺序（C7c 的 cookie 续读只在一个进程的一次列举之内，不进检查点） |
| M1：每页一个 future、游标在驱动里停车 | CIFS 句柄在 detach 后无法 await close，会泄漏 |
| Namespace 加带默认实现的 `list_pages`，保留 `execute(List)` | 为兼容而设；两条列举路径会漂移 |
| 在遍历里做大小写折叠排序 | 折叠表各家不同（NTFS $UpCase、ONTAP、Samba），贵且易错 |

## 附：设计过程中发现的现存问题（今天的代码，与 P7 是否实施无关）

沿用评审稿的编号（commit message 与 Q28 / Q29 引用的就是这些编号）；问题描述保留当时背景，状态更新至 P7 收尾。

| # | 问题 | 状态 |
|---|---|---|
| 1 | **一次 S3 SlowDown / 503 就可能结束整场遍历**：限流被归为会话失败（`src/s3/role_protocol.rs:518-523`），且没有配置重试策略 | **已修** C3 / C6：`FailureClass::Throttled`，列举生产者按页有界退避；预算用尽只让该列举失败，遍历继续兄弟（§8、Q11） |
| 2 | **CIFS、NFS、HDFS 因一个坏子项让整个目录列举失败**：`collect::<Result<Vec<_>>>()?`（`cifs/namespace.rs:92`、`nfs/namespace.rs:83-85`，NFS 缺属性时 `nfs.rs:4657-4659`，HDFS 同类），违反“逐子项失败逐子项报”的契约 | **已修** `b6aff40`（`fix(storage): 列举里一个描述不出的子项只让它自己失败`）：三者都改为逐子项 `EntryOperationFailure` + `NamespaceResult::Listing`，遍历报 `Partial` |
| 3 | **S3 翻页出现重复 token 被当成会话失败**（`paging.rs`），它只是一个目录的问题 | **已修** `1a68f41`（`fix(s3): 翻页 token 绕圈只让该目录失败`）：改为该目录的 `Protocol` / Permanent 条目失败，块 `Failed`，遍历继续兄弟 |
| 4 | **legacy 版本列举假设服务端按 key 升序**（`src/s3/version_listing.rs`，C20）：靠扣住每页最大的 key 拼一个 key 的完整历史，在无序 S3 上会把一个 key 的历史拆成多组、`version_count` 与 latest 标记出错 | 已随收尾删除；role-based 按目录物化（§5.3、Q9） |
| 5 | **role-based 遍历在 NFS 上常驻 178 MB，来源未查清**：m1-source 每目录约 35 项，帧里的东西应只有几 MB | **已查明并修复**：列举把所有子目录句柄写入 `GLOBAL_CACHE`，缓存驻留和碎片导致增长（§12.7）；C7 下钻提示取代列举回填缓存，C10 性能门通过（§12.8） |
| 6 | **terrasync 写 ClickHouse 的路径在异常时会丢数据**（10.131.9.10，`8edd400`）：全量扫描写失败只重试约 0.7 s 就丢掉整批且任务报成功；数据库消费者退出后广播给它的数据被静默丢弃；统计按广播逐条累加、与是否入库无关；全量扫描直接写 base、崩溃即留下半场数据 | 在 terrasync 里改，本设计只列要求（§11、Q16、Q17） |
| 7 | **S3 当前对象的 `PathScoped` 身份里没有路径**（稳定字节只有 ETag）：内容相同、路径不同的对象共用一个身份键，terrasync 按身份键 `INNER JOIN` 会交叉配对成 N×N 行（§5.4.7、F1 / Q28） | **已修** `4fa5aa5`（`fix(s3)!: PathScoped 身份包含路径`）：S3 当前对象、`"null"` 版本与 `"null"` 删除标记的稳定字节改为 u32 大端路径长度 + 路径 + ETag；契约写进 `IdentityStrength::PathScoped` 文档。破坏性：S3 `PathScoped` 身份键一次性全变。terrasync 侧“只对 `StableWithinBackend` 按身份键关联”仍待在 terrasync 里做 |
| 8 | **版本与删除标记交错时按 LastModified 合并，并发写入时排错**：aws-sdk-s3 把 `<Version>` 与 `<DeleteMarker>` 解析成两个列表，丢了交错顺序（`src/s3/role_protocol/versions.rs:107, 123`）（§10.2、F2 / Q29） | **已修** `40c1ac9`（`fix(s3)!: 版本历史按服务端顺序组装`）：`src/s3/version_order.rs` 拦截原始响应体、按服务端位置倒序组装，校验不过的 key 标 `HistoryOrder::Inferred`；`history.rs` 的核心由 role-based 与 legacy `walkdir` / `walkdir_2` 共用。差别：role-based 在整个目录列举（跨页）上校验同 key 连续；**legacy `walkdir` 只在每批已释放的完整 key 内校验连续**。回归数据 `tests/data/s3/minio-concurrent-versions{,-paged}.xml` |
| 9 | **legacy 版本列举假设 key 升序**：`src/s3/version_listing.rs` 的 `VersionPages::complete_keys` 把每页最大的 key 扣到下一页合并（C20），前提是 ListObjectVersions 按 key 升序返回。在无序存储上（ONTAP NAS 桶，Q22；NAS 桶不支持 ListObjectVersions，所以 ONTAP 上不会触发），同一个 key 的历史会被拆成几段、各自当成一个 key 发出（latest / `version_count` 错）；F2 的连续性校验在 legacy 里也只看每批 | 已随 P7 收尾整体删除；调用方迁到 role-based 的整个目录物化分组。role-based（C22 / C6）按目录物化分组，不受影响 |
| 10 | **ONTAP `ListObjectVersions` 带 `delimiter=/` 会静默丢版本**（2026-09-26 实测，ONTAP 9.19.1）：截断页里有 CommonPrefix、且被收拢的 key 在列举前缀或该 CommonPrefix 之后紧跟多字节字符（如 `p/éb/x`、`p/x/中/y`）时，若页恰好停在后一个 key 的版本中间，服务端给 `NextKeyMarker=<key>` 却**不给** `NextVersionIdMarker`，下一页从该 key 之后开始，剩余版本被跳过（1000 个版本只回 998）。另：页停在 CommonPrefix 上时有时仍给 `NextVersionIdMarker`，下一页重复该前缀；恰好满页后跟一个空的末页；CommonPrefix 计入 max-keys。受影响：legacy `s3.rs` 两处 ListObjectVersions、role `s3/role_protocol/listing.rs` | **已修** `8d4b225`（F4）：截断页没有 `NextVersionIdMarker`、且 `NextKeyMarker` 那个 key 的条目恰是本页文档序的末尾一段时，用最后一条的 versionId 作 version-id-marker；续点不能确定（分组输出、读不到文档序且两种条目都在该 key 上）时不猜（role 该目录 `Protocol` 失败，legacy 先发完整的 key 再报错）；legacy 的 CommonPrefix 跨页去重（role 本来就去重）；空末页容忍。回归数据：ONTAP 实测响应 3 份（`tests/data/s3/ontap-versions-delimiter-*.xml`）；真机 `mkl/z` 1000 个版本 role / legacy 全列出 |
| 11 | **RustFS 1.0.0 的 `ListObjectVersions` 原始 XML 不交错**（2026-09-26 实测，VM 105）：每页先列全部 `<DeleteMarker>`、再列全部 `<Version>`（各组内 key 升序、同 key 新→旧），所以一个 key 的条目在文档里不连续、第一条也不一定是 IsLatest。F2 的逐 key 校验会判不合格 → 该 key 标 `HistoryOrder::Inferred`、按 `LastModified` 排（同毫秒可能排错）。分页（key-marker / version-id-marker，max-keys 1/2/3）不丢不重 | 行为正确（回退如设计）；RustFS 上历史顺序只能推断。可选改进：对“分组输出”的存储（每页 marker 组 + version 组、各自按 key 升序新→旧）把两组当两条有序序列按 key 归并，同 key 内仍需时间决定交错 → 与今天的回退等价，不改。e2e-s3 的 RustFS 行期望 `order=inferred` |
| 12 | **键以 `/` 结尾且有内容的 S3 对象**（“带内容的目录标记”）：MinIO、RustFS、ONTAP 对象桶、DXN 都允许（2026-09-26 实测，PUT 200）；ONTAP NAS 桶去掉末尾 `/` 存成普通文件。role 遍历把它报成该目录上的 `Failure{scope: Child}`（Unsupported），内容不列、不拷；**legacy walkdir（`src/s3.rs:3925`）静默跳过 —— 静默丢数据** | **已修**（F5a `ec3ee53` … F5f `43d0381`，历史重放 H1；§14.1）：role 遍历把它（以及零字节文件夹标记）列成文件条目 `d/`，S3→S3 拷贝，文件系统目的端 typed 跳过 / 拒绝；legacy 报 Scan 错误。MinIO / RustFS 上 > 8 MiB 的写入差异已由 D13 消除（F5g `2352a6b`，只用单次请求写）；遗留：NAS 桶 → N 阶段 profile |

## 收尾：清理扫描相关旧代码

**用户要求（2026-09-25）**：“最后本项目中和扫描相关的都要按照方案进行清理。”

实现与迁移清单：[p7-legacy-cleanup.md](p7-legacy-cleanup.md)。2026-10-12 已分块删除旧 scanner，
保留的行为测试迁到 role 遍历；NFS 原始删除单独保留 artifact 覆盖与 RPC 排空。
当前代码/真机验收仍按 §14 跟踪，删除代码不等于验收已经通过。
历史 node1 二进制与日志路径经 SSH 检查已不存在；§12.8 记录的四次基线仍作为历史证据，
完整删除前源码在 `aafd463`，后续对照从该版本编译。

原定收尾要求（已据此实施代码清理）：

1. **盘点**全部与扫描相关的旧代码，逐项标注“删除”或“迁到新接口”，至少包括：
   - legacy `StorageEnum` 的 `walkdir` / `walkdir_2` 及各 backend（Local / NFS / S3 / HDFS）的实现；
   - `walk_scheduler` / `async_receiver` / `dir_tree`；
   - `ndx_walk`；
   - legacy `version_listing`（现存问题 4）；
   - `NfsEnrich`；
   - 旧的 Namespace 请求类型（`NamespaceRequest` / `NamespaceResult` / `execute` 等，§13）；
   - 使用它们的 examples / tests / skills / docs（含 `CLAUDE.md` 路由表与 `.claude/docs/` 中的相应段落）。
2. **删除前先取 legacy 性能基线**（node1 + m1-source 53M 树，口径同 §12.6），记录在案；此后与 legacy 的对比从 git 历史编译。
3. **逐块删除**：每一块单独一个 commit，commit message 写明“最后一个还包含它的 commit”，便于以后从历史里找回。

---

依据：data-mover-rs `src/traversal/**`、`src/storage/roles.rs`、各 backend 的 namespace / listing、nfs-rs 0.8.4、smb-rs 91e6cfb、
hdfs-native 695fc47、`.claude/docs/{architecture,error-taxonomy}.md`；terrasync-rs 8edd400 的 `crates/db`、
`crates/app/src/{scan.rs,broadcast.rs,consumer/**}`；记忆中的 node1 / FAS2750 实测。性能数字除第 2.2 节外均为推测，未经测量。
