# P7 流式列举：设计评审稿（定型版）

> 状态：**待拍板**（2026-09-25）。用户与 architect 逐轮迭代（D1–D20）后定型；拍板后写成 ADR-0008 再实现。
> 完整设计（16 节：现状实测、两种扫描方式、事件模型、Namespace 接口与各后端、调度与内存、计数、错误模型、断点续扫、
> 多版本、与 terrasync 写库的配合、性能、破坏性变更清单、实施计划 C0–C12、问题清单、否决方案附录）以
> `root@10.131.9.10:/root/p7-streaming-listing-design.html` 为准；逐轮修订的过程稿在同目录
> `p7-streaming-listing-design-draft.html`。本文件只收录结论、现存风险与待拍板问题；本文件的逐轮历史见 git log。

## 目标

扫描时列举一页一页地流进遍历，不再把整个目录读进内存；提供两种扫描方式，其中无序方式的成本接近 legacy `walkdir`，
同时保留 legacy 没有的东西：带类型的失败、完整性判定（删除检测的前提）、准确的计数，以及可选的断点续扫。
原则（用户定）：不考虑兼容性，怎么合理怎么设计，现有结构需要改就改。

## 已经定下的

| # | 决定 | 理由 |
|---|---|---|
| 1 | 扫描只有两种方式：`Unordered`（无序 DFS）与 `Blocks`（块序 DFS：一个目录的直接子项连续成块，块内按列举顺序、不排序；块读完后按子目录名字节序深度优先下钻） | 列举场景只有这两种；`Blocks` 的内存和 CPU 与今天的 `Admission` 基本相同，只多一次子目录排序 |
| 2 | 不做兄弟排序（删除 P4 的 `NameBytes` 兄弟排序与 `ChildOrder`） | 全排要整块物化加 O(N log N)；两进程对比的文件匹配由消费方按名字做 |
| 3 | `DirectoryListed` 只在 `Blocks` 里发；`Unordered` 不发任何正常路径的目录事件 | 块边界只在 `Blocks` 有用；`Unordered` 省掉计数树和每目录两项 |
| 4 | `Namespace` 只有一个流式列举入口 `list(ListRequest) -> ListingStream`，逐页返回，页里只带名字 | 两种方式共用；页形状为 P9（路径不重复存前缀）预留 |
| 5 | 断点续扫是扫描入参（`resumable` / `resume_from`），默认关，由上层决定 | data-mover 不决定策略；关闭时零开销 |
| 6 | 续扫不求精确：从最近一个已落库的检查点重来，重扫部分靠库去重；唯一硬性要求是「进度不领先于已落库的数据」 | 重复扫描代价小；真正的问题是 ClickHouse 批没写下去而进度前进了 |
| 7 | terrasync 的临时表不用改 | 每批一张新临时表、比较后并入 `ReplacingMergeTree` 的 base，跨批重复天然被吸收 |
| 8 | S3 列举不假设有序 | 用户遇到过无序的 S3 实现 |

## 设计过程中发现的现存问题（今天的代码，与 P7 是否实施无关）

1. **一次 S3 SlowDown / 503 就可能结束整场遍历**：限流被归为会话失败（`src/s3/role_protocol.rs:518-523`），且没有配置重试策略。
2. **CIFS、NFS、HDFS 因一个坏子项让整个目录列举失败**：`collect::<Result<Vec<_>>>()?`（`cifs/namespace.rs:92`、
   `nfs/namespace.rs:83-85`，NFS 缺属性时 `nfs.rs:4657-4659`，HDFS 同类），违反「逐子项失败逐子项报」的契约。
   **已修**（`fix(storage): 列举里一个描述不出的子项只让它自己失败`，commit 待提交）：三者都改为逐子项 `EntryOperationFailure`
   + `NamespaceResult::Listing`，遍历报 `Partial`。
3. **S3 翻页出现重复 token 被当成会话失败**（`paging.rs:69-71`），它只是一个目录的问题。
   **已修**（`fix(s3): 翻页 token 绕圈只让该目录失败`，commit 待提交）：改为该目录的 `Protocol` / Permanent 条目失败，块 `Failed`，遍历继续兄弟。
4. **legacy 版本列举假设服务端按 key 升序**（`src/s3/version_listing.rs`，C20）：靠扣住每页最大的 key 拼一个 key 的完整历史，
   在无序 S3 上会把一个 key 的历史拆成多组、`version_count` 与 latest 标记出错。
5. **role-based 遍历在 NFS 上常驻 178 MB，来源未查清**：m1-source 每目录约 35 项，帧里的东西应只有几 MB。待 C0 前用驻留探针
   与 `MALLOC_ARENA_MAX=2` 对照。
6. **terrasync 写 ClickHouse 的路径在异常时会丢数据**（10.131.9.10，`8edd400`，只读查看）：全量扫描写失败只重试约 0.7 s 就丢掉
   整批且任务报成功；数据库消费者退出后广播给它的数据被静默丢弃；统计按广播逐条累加、与是否入库无关；全量扫描直接写 base、
   崩溃即留下半场数据。改法见完整设计第 11 节，按用户指示只写进设计、不改 terrasync 代码。

## 待拍板（每条附建议）

**扫描方式与事件**

| # | 问题 | 建议 |
|---|---|---|
| Q1 | 默认用哪种扫描方式 | `Unordered` |
| Q2 | `SubtreeComplete` 两种方式都删（`Blocks` 里可由名字序 DFS + `DirectoryListed` 推出，续扫用检查点） | 删 |
| Q3 | 计数：两种方式都在 `TraversalCompletion` 给总数；`Blocks` 的 `DirectoryListed` 另带本块计数；`Unordered` 没有每目录计数 | 可以 |
| Q4 | data-mover 是否提供 `Blocks` 两进程逐目录配对的辅助（封装「两侧都到 `DirectoryListed` 才判定缺失」） | 提供，这条规则写错就是删数据 |
| Q5 | `Blocks` 的下钻键用子目录名的原始 UTF-8 字节，不做大小写折叠与 Unicode 规范化 | 是 |
| Q6 | 大小写 / 规范化不敏感的目标：`NamespaceCapabilities` 公开名字语义，消费方遇到「折叠后相等、字节不等」报冲突、不删除 | 是 |

**多版本与 S3**

| # | 问题 | 建议 |
|---|---|---|
| Q7 | 多版本：每版本一项、服务端顺序由旧到新、同 key 连续，另提供分组适配器；不做「每 key 一项、按 LastModified 排序」 | 同意 |
| Q8 | 单 key 版本上限（超限该 key 报 `Capacity`，其余照常） | 10 万 |
| Q9 | S3 `All` 按目录物化（不能假设同 key 版本连续），上限 500 万、超限 `Capacity` | 接受 |

**资源与失败策略**

| # | 问题 | 建议 |
|---|---|---|
| Q10 | CIFS 预取挂起的目录句柄上限 | 8，先在 FAS2750 测 ONTAP 空闲回收 |
| Q11 | 节流：新增 `FailureClass::Throttled`，单个列举累计退避约 2 分钟后报 `Interrupted` | 是 |
| Q12 | 列举后消失的条目、父列出后已不存在的子目录：计入 `vanished`，不发失败项、不破坏完整性 | 是 |
| Q13 | 列举可能重复或遗漏子项（目录正在被修改、S3 跨页重复）：写进契约，由消费方按（父, 名）去重；遍历只对子目录去重 | 是 |

**续扫与统计**

| # | 问题 | 建议 |
|---|---|---|
| Q14 | 检查点默认间隔 | 每 10 万项或 10 秒，先到先发；前沿过大时自适应 |
| Q15 | 遍历侧计数（listed / filtered / internal / vanished / 列举失败）续扫后是近似的（重扫部分重复计） | 可以；精确的分布统计以库为准 |
| Q16 | 完整设计第 11.2 节的 terrasync 写库改动在 terrasync 里做，本设计只列要求、不改那边代码 | 是 |
| Q17 | 全量扫描是否与增量扫描一样加 generation 闸门（写完并校验后再生效） | 要 |

**依赖、顺序与验收**

| # | 问题 | 建议 |
|---|---|---|
| Q18 | 改 nfs-rs 上游，公开 READDIRPLUS 页接口（cookie + verifier），让 NFS 列举中途出错后从断点续读 | 做，单独的 `chore(deps)` commit |
| Q19 | 先做 C0（去掉每条目一次的 spawn）并在 node1 补测 `8a1348b` 之后的 CPU / RSS 基线（含 `MALLOC_ARENA_MAX=2` 与驻留探针），再做破坏性重构 | 是 |
| Q20 | `Unordered` 的验收门槛（node1、5337 万条、c=64、与 legacy 交替四轮） | CPU ≤ 1.3× legacy，RSS ≤ 50 MB，墙钟 ≤ legacy + 5% |
| Q21 | 两种扫描方式 + 事件模型 + 续扫写一篇 ADR | 写，ADR-0008 |
| Q22 | 遇到的无序 S3 是哪一家、哪个版本、实验室里能否访问（能则加进 e2e-s3 矩阵；也决定现存问题 4 的修复排期） | 待告知 |
