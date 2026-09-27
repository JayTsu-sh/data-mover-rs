# P7 C11b 实施计划：S3 多版本改为每个 key 一项

基线：dev `f4f644d`（C11 已合入：8337fad、51152da、2ddf553、55f41c2、f4f644d）。下文行号全部按 f4f644d 核对过。
只有规划，没有改任何仓库文件。

签字前需要知道的行为变化：
- 版本化桶的默认遍历形状变了：所有消费方都会收到带历史的条目，当前已删的 key 也会出现。
- 旧快照（v4/v5/v6）全部读不了，调用方按首次扫描处理。
- 版本化桶上单目录超过 500 万条记录会报 `Capacity`（Q9 的上限）；今天默认的 Current 流式列举在版本化桶上没有这个上限，
  所以对版本化桶是一次能力倒退。

```
PLAN
====
多版本 S3 桶上遍历改为每个 key 一项（ObservedEntry::history() → ObjectHistory），单 key 超过 10 万条记录就分段；
是否列历史由连接时的 GetBucketVersioning 决定（Q26，细则见"已定：Q-A"）；删掉 TraversalVersions、Namespace::list_versions、
EntryVersion；条目快照只保留一种格式 v7，v4/v5/v6 不再解码；计数按 key 计，另加版本数、删除标记数、全部版本字节。
why: 用户定"历史是对象的属性，不是扫描方式"（决定 #9、Q7/Q8/Q9/Q26/Q27）。今天每个版本一项，要靠 Unordered 的
     trailing_run/carry、Blocks 的块连续来保证同一 key 的版本在一起；快照格式有 v4/v5/v6 三套分支。

数据结构（新文件 src/model/history.rs，取代 src/model/entry_version.rs）
  pub const MAX_HISTORY_RECORDS: usize = 100_000;          // Q8
  pub enum HistoryOrder { Server, Inferred }                // 原样迁过来
  pub struct ObjectHistory {
      records: Vec<VersionRecord>,   // 本段内从旧到新；整个 key 的最后一条是最新的
      order: HistoryOrder,
      segment: u32,                  // 段号，从 0 开始
      complete: bool,                // 只有这个 key 的最后一段为 true
  }
  pub struct VersionRecord {
      version_id: Option<String>,    // None = "null"
      latest: bool,                  // 列举里的 IsLatest，原样保留
      kind: RecordKind,              // Object { size, etag, modified } | DeleteMarker { modified }
      identity: SourceIdentity,      // VersionScoped(id) | PathScoped(路径‖ETag) | PathScoped(路径‖NULL_MARKER)
      metadata: Option<Box<MetadataObservations>>,  // None = 列举之外什么都没观察；装箱是为了让每条记录保持在约 100 B
  }
  - 访问方法：records / order / segment / is_complete / latest() / current()。
    current() 只在 complete 段上、且最后一条是对象时返回它，否则 None。
  - VersionRecord::source() 按 EntryVersion::source_version 的规则推出来，不单独存。
  - ObservedEntry.version: Option<Box<EntryVersion>> 换成 history: Option<Box<ObjectHistory>>；
    ListingFacts.version 换成 history（listing_only 保留）。

带历史的条目，条目层字段怎么取（建议；分段后每一段都相同）
  - path = key，kind = File。
  - size = 当前记录的大小；最后一条是删除标记时为 None（filter 的 size 条件照旧放行，filter-dsl.md 那一行的语义不变）。
  - modified = 最后一条记录的时间。
  - source_identity：当前是对象时 = PathScoped(路径‖ETag)，与 ListObjectsV2 给同一个对象的身份逐字节相同。
    这样同一个对象开不开版本、identity_key 都不变，expert 半边的 observation_matches 也能直接对上
    （src/storage/backends/s3/source.rs:209-219）。
  - 最后一条是删除标记时：PathScoped(路径‖"\0s3-deleted-key\0"‖标记 id 或 "null")。
  - content_version = 当前 ETag。
  - metadata = 当前记录的 metadata 拷一份；没有当前记录时只有列举给的信息。

计数（按 key）
  - listed / emitted / filtered / failed / vanished / files 都按 key 计，一个 key 算一次，不管分几段。
  - 新增 versions（对象版本记录数）、delete_markers（删除标记记录数）：只数历史里的记录，存储没有历史时为 0。
  - 新增 all_versions_bytes：所有已存版本的字节；没有历史的文件记它自己的大小，所以存储没有历史时它等于 bytes。
  - bytes 改为"当前版本字节"。
  - 不变式 listed == emitted + filtered + failed_children + vanished 不变。
  - 统计在分段之前对整段历史做一次。

分段：放在输出这一环，不放在列举
  - 列举、filter、观察、计数都只按 key 做一次，拿到的是整段历史（历史本来就按目录物化，不超过 500 万条）。
  - 发出时（tally_child）把超过 MAX_HISTORY_RECORDS 的条目切成 N 项。这 N 项在同一个 batch 里：
    Unordered 走 send_whole；Blocks 只有驱动一个发送方。所以同一个 key 的各段在会话里连续、有序。
  - C10 的 trailing_run / carry 因此整个删掉。

C11 续扫的交互（已按 f4f644d 核对）
  - Blocks：周期检查点只落在块与块之间（src/traversal/storage/resume/blocks.rs:109-133），一个槽位的所有段在同一次 flush 里。
    取消时 flush 可能被截断（src/traversal/storage/output.rs 的 cut），这时 delivered 不前进，最后一个检查点落在这一块之前，
    续扫时整块重列，各段从第 0 段重发。
  - Unordered：检查点在一页之后发。正在列的目录在前沿里，续扫时整个目录重列。
  - 结论：不会跳过任何一段。但消费方可能先看到第 0..k 段，然后被截断（取消排空时）。
    契约要写明："一个 key 的历史只有收到 complete=true 那一段才算完整；续扫后从第 0 段重发。"
  - 指纹：去掉 versions_tag，加入 namespace.capabilities().histories。这样版本控制状态在两次运行之间变了，
    续扫会被拒（Mismatch），而不是前半段是当前对象、后半段是历史。检查点 VERSION 1 → 2。

已定：Q-A（用户 2026-09-27 采纳推荐方案）
  连接时 GetBucketVersioning 的结果怎么决定列举方式：
  - Enabled / Suspended → 列历史（ListObjectVersions）。
  - 从未开启 → ListObjectsV2。
  - 服务端明确拒绝（403 或其他明确的拒绝）→ 列历史（Q26）。
  - 501 / NotImplemented / UnsupportedOperation → ListObjectsV2（这个存储没有版本功能）。
  - 瞬时失败（超时、5xx、网络，SDK 重试之后）→ role connect 报 Connectivity / Transient，不猜。
    原因：如果"一律列历史"，同一个未开版本的桶会因为一次瞬时失败，每项多出一条 "null" 记录，
    terrasync 的去重键 (relative_path, version_id, identity_key) 前后两代不一致，产生重复行。
  - NAS profile（versions=false）→ 一律 ListObjectsV2。
  - legacy S3Storage::new 的宽容行为不变（仍然连接成功），只是把状态记下来，由 role connect 拒绝。

建议（不需要用户决定，默认按这样做）
  R2 一条记录的元数据观察失败，只记在这条记录上（MetadataObservation::Failed），不让整项失败。
     Required 模式也一样（§10.1/§10.4 写得很明确）。每项内部逐条串行观察，占遍历的同一个并发名额；
     删除标记不观察。
  R3 directory_object_action 遇到带历史的条目：
     - 目的端是文件系统：看当前记录（没有或为 0 字节 → NothingToDo；有内容 → Skip）。
     - 目的端能存目录对象：只要任何一条对象记录超过上限，就 Skip(TOO_LARGE) 整个 key。
       因为 D11 规定重放中途一条失败就结束这个 key，留下的历史会缺一段。
  R4 分段只在单元测试里用调小的上限验证，不为真机测试开隐藏的公开参数。
  R5 ADR-0008 在 C12 写；C11b 在 §14 的 C12 行加一项"写入快照格式开发期规则（发布前重定）"。
```

```
CHANGE
======
提交顺序（重构单独提交；破坏性改动带 !；每个提交都要能过 make ci）。行号均为 f4f644d。

── 提交 1 refactor(s3): 版本列举（按目录物化）从 listing.rs 移到 histories.rs（纯搬移，行为不变）
- src/storage/backends/s3/listing.rs:71-158（VersionPages / page_versions / list_all / listing_wide）、
  :386-413（Block::own_history）、:531-549（history_child）、:636-710（Listed / version_descriptor /
  marker_descriptor）→ 移到新文件 histories.rs；Block 需要开放的方法改成 pub(super)
- src/storage/backends/s3/namespace.rs:11：import 改从 histories 引
- src/storage/backends/s3/mod.rs：加 mod histories

── 提交 2 refactor(traversal): tally_child 返回条目序列（为分段做准备，行为不变）
- src/traversal/storage/output.rs:263-300：返回 Option<TraversalItem> 改为可迭代的小序列；
  flush 的 :213 batch.extend 调用处不变
- src/traversal/storage/unordered/worker.rs:181, :237：batch.extend(tally_child(..)) 同步改

── 提交 3 feat(s3)!: 是否列历史由连接时的 GetBucketVersioning 决定（Q26 + Q-A）
- src/s3.rs:574-610：detect_bucket_versioning 返回多态结果
  （Enabled / Suspended / Never / NoVersioning(501) / Refused / Unreachable），不再把所有失败折成 None；
  :147 versioning_is_unsupported 保留
- src/s3.rs:478-483, 744-762：S3Storage 加字段 versioning_at_connect；is_bucket_versioned /
  versioning_suspended 从它推出（legacy 语义不变）；:4774 的测试字面量补上新字段
- src/s3/copy_request.rs:102、src/s3/role_protocol/native.rs:172：结构体字面量补上新字段
- src/storage/backends/s3/protocol.rs:282-285：S3Protocol 加 fn lists_histories(&self) -> bool
  （versions 能力为真，且连接时状态是 Enabled / Suspended / Refused）
- src/s3/role_protocol.rs:168 附近：实现 lists_histories
- src/storage/backends/s3/mod.rs 的 connect：Unreachable 时报 Connectivity / Transient（Q-A）
- src/storage/backends/s3/namespace.rs:157-170：capabilities().histories = protocol.lists_histories()
- src/storage/backends/s3/tests.rs:336、memory_versions.rs:113-160：fake 加连接时状态的开关
  （refused、501、never、suspended、unreachable）
- src/storage/roles/namespace.rs：histories 字段的文档改为"连接时判定的多版本桶"
- src/storage_enum.rs:999 is_bucket_versioned：语义不变（只看 Enabled）

── 提交 4 feat!: 多版本改为每个 key 一项（ObjectHistory），快照只留 v7，删除 TraversalVersions / list_versions
模型
- src/model/entry_version.rs → 删除；新建 src/model/history.rs（结构见 PLAN）
- src/model/mod.rs:13：pub use history::{HistoryOrder, ObjectHistory, RecordKind, VersionRecord, MAX_HISTORY_RECORDS}
- src/model/observation.rs：
  - :291-293 字段改为 history
  - :327, :354 构造器相应改
  - :385-388 with_version 改为 pub(crate) with_history
  - :453-468 删 version()，加 history()；source_version() 对带历史的条目返回 current()?.source()
  - :11 删 use VERSION_WITH_BACKEND_ID
  - :477-480 删 encode_snapshot_v4
  - :486-505 decode_snapshot 的文档去掉 v4 段落，改为"只读当前格式，其他 → UnsupportedVersion"
  - 加 pub(crate) fn into_segments(self, cap) -> Vec<ObservedEntry>（identity_key 不变，只有最后一段 complete）
- src/model/observation_codec.rs：
  - :12-21 只留 const VERSION: u8 = 7，文档写明"格式任何改动都要加 1，并更新 golden 测试"
  - :39-81 encode 去掉 with_backend_id 参数，版本字节固定写 7，写历史段
  - :129-131 只接受 7
  - :147-149 读历史段
  - :187-210 decode_source_identity 删 v4 分支（只走端点指纹）
  - :371-424 删 encode_version / decode_version 和 VERSION_* 标志常量
- 新建 src/model/history_codec.rs：历史段的编解码（不让 codec 超过 800 行）。
  记录数超过 MAX_HISTORY_RECORDS → Malformed；未知标志位 → Malformed

存储 / S3
- src/storage/descriptor.rs:23-31：ListingFacts.version 改为 history: Option<Box<ObjectHistory>>；:6-9 import
- src/storage/listing/child.rs：不改字段（listing 已经带着）
- src/storage/roles/namespace.rs：删 list_versions 默认实现及其文档；删 version_unsupported 的 import
  （src/storage/roles/version.rs 若因此没人用就一起删）
- src/storage/backends/s3/namespace.rs：
  - :1-4 模块文档
  - :116-131 list()：lists_histories() 时走 produce_histories，否则 produce_current
  - :172-188 删 list_versions
- src/storage/backends/s3/histories.rs（提交 1 搬来的）：
  - list_all 改为 produce_histories：整目录读原始记录（受 Q9 的 500 万上限）→ history::key_histories
    → 每页只把 page_hint 个 key 转成 ListedChild（按需逐页转，不一次性全部构造描述），send_split 发出
  - history_child / own_history 每个 key 产出一个 ListedChild：条目层字段按 PLAN 取；
    记录的身份用 source.rs 的 object_identity，删除标记沿用 NULL_MARKER_IDENTITY；
    有一条记录的 id 不合法 → 整个 key 失败（与今天相同）
- src/storage/backends/s3/history.rs：核心排序不改；:1-35 文档去掉"每个版本一项"
- src/storage/listing.rs:40, :576：文档里的"S3 versions until C11b"改掉
- src/storage/listing/testing.rs:52-58：删 list_versions 的辅助函数

遍历
- src/traversal/mod.rs：
  - :55 Unordered 的文档改为"一个 key 的各段有序且连续"
  - :98-120 删 TraversalVersions
  - :223-224 删 versions 字段；:333 new() 里删；:393 删 setter；new() 的文档相应改
- src/traversal/checkpoint.rs：
  - :43 import
  - :168-203 fingerprint 加参数 histories: bool，去掉 :194 的 versions_tag，删 :212-217
  - :151 VERSION 1 → 2；模块文档写明原因
- src/traversal/storage/resume.rs:100 Resume::prepare：从 namespace.capabilities().histories 取值传给 fingerprint
- src/traversal/storage.rs:15、176-209：删 supports_versions、versions_unsupported 和拒绝分支；
  Shared 加 history_segment（默认 MAX_HISTORY_RECORDS，#[cfg(test)] 可调小）；:328/:351 root_object_reported 文档改
- src/traversal/storage/stream.rs:42, 168-171：一律 namespace.list
- src/traversal/storage/stream.rs:297-326 root_object：判断"有内容"要看历史里任何一条对象记录大于 0（D4）
- src/traversal/storage/resume/relist.rs:23, 49-52：一律 namespace.list
- src/traversal/storage/unordered/worker.rs：删 :15-18 文档、:38 import、:55 carry、:114-117 分支、
  :174-186 carry 逻辑、:299-320 结束时的 carry 处理（改为只发失败）、:376-393 trailing_run
- src/traversal/storage/observe.rs：
  - :101-116 metadata_without_io：带历史且计划要的都在列举里 → 每条记录按列举内联组装
  - :120-139 role_observations：带历史 → 逐条记录调 observe_copy_bound_version(路径, record.identity, record.source)，
    删除标记跳过；每次之间检查取消；结果和失败写回各自的记录
  - :148-191 assemble：:184 with_version 改为 with_history；条目层 metadata 取当前记录的
- src/traversal/channel.rs:15-18、src/traversal/storage/output.rs:129、src/traversal/storage/resume/blocks.rs:111、
  src/traversal/storage/unordered.rs:15：文档里"一个对象的全部版本"改为"一个 key 的各段"
- src/traversal/checkpoint/tests.rs:411：versions 那一行改为 histories 翻转 → Mismatch

传输
- src/transfer/engine/expert_observation.rs:13, 21-41：refuse_listed_versions 改为
  "带历史且 current() 是 None（删除标记，或不是最后一段）→ 拒绝"，否则放行
- src/transfer/engine/directory_object.rs:26, 130：按 R3 判定；函数文档 :108-120 同步改
- src/transfer/engine/delete_marker.rs:1-12：文档改为"按 ObjectHistory::records() 从旧到新"

例子（必须在同一个提交里能编译）
- examples/s3_history_replay.rs：
  - :49、54-58 import
  - :108-128 records()：把每一段的 history().records() 按段号拼起来；有 complete=false 却没收到最后一段 → 报错
  - :151-153、200、259-280：改用 record.is_delete_marker() / source() / version_id()，order 取 history.order()
  - :213-218 D12：只处理最后一条
- examples/storage_role_operations/main.rs:6-16、41、91-102、130-131、171：删 --versions；加 --current
  （只打印 current() 的投影，给脚本里原来的 Current 断言用）
- examples/storage_role_operations/traverse.rs：
  - :20, 26 import
  - :149 --sizes 的判断改看 history
  - :213、240 删 versions
  - :309-325 version_of 改为：条目行附加 history=<段号>/<complete> records=<n>[ order=inferred]，
    每条记录单独一行 `Record <path> <段号>.<序号> version=<id|null> latest=<0|1> marker=<0|1> size=<n|->`

测试（同一个提交里迁移）：见 VERIFY 的"测试"

── 提交 5 feat(traversal): 单个 key 超过 10 万条记录时分段发出（Q8）
- src/traversal/storage/output.rs:263 tally_child：先计数，再 into_segments(shared.history_segment)
- Unordered 的 settle（worker.rs:237）走同一个函数
- StorageTraversalSource 加 #[cfg(test)] 的 history_segment(cap)

── 提交 6 feat(traversal): 计数加 versions / delete_markers / all_versions_bytes；bytes 改为当前版本字节（§10.4）
- src/traversal/counts.rs：
  - :17-50、:106-149 加字段
  - :91-103 count_emitted 看历史
  - :151-248 new / merge（解构，漏掉字段编译不过）/ absorb 相应改
  - 模块文档写出口径
- （ScanTotals / BlockCounts 都是 non_exhaustive，加字段不算破坏性改动）

── 提交 7 test(skills): e2e-s3 和续扫脚本改用每个 key 一项的输出
- .claude/skills/e2e-s3/scripts/traversal_versions.sh：
  - :3-44 头注释
  - :249-259：--versions all 去掉（版本化桶自动列历史）；--versions current 改为 --current
  - :282、369 的正则改成条目行加 Record 行
  - 新增断言：gone 和 k 各是一项，gone 的 current 为空，p/z 是一项三条记录
- .claude/skills/e2e-s3/scripts/directory_objects.sh:310, 361, 389, 394, 397, 402, 510：
  --versions current/all 改为 --current / 默认
- .claude/skills/_shared/traversal_resume.sh:10, 23, 26：去掉 EXTRA 的例子；entries() 同时匹配 ^Record 行，
  这样合集比较覆盖到每一条记录
- .claude/skills/e2e-s3/SKILL.md:121：命令行说明同步
- versioning_matrix.sh：不改（它用 curl 列举）

── 提交 8 docs: C11b 收尾
- docs/design/p7-streaming-listing.md：§5.2 S3 行（"All 仍按目录物化"改掉）、§10.1（段号字段）、§10.4、§13 多版本行、
  §14 C11b 行（提交号、Q-A 决定、真机结果）、§14 C12 行加 ADR-0008 的快照规则项
- .claude/docs/storage-s3.md:677-696、733、752（role-based 遍历与版本模式整段）；.claude/docs/architecture.md:139, 230；
  .claude/docs/filter-dsl.md:68（size 和 modified 在带历史条目上的取值）
- CLAUDE.md:33 路由行（去掉 TraversalVersions，加 src/model/history.rs）
- docs/architecture/*、ADR-0006 历史段落：留给 C12，不在这里改

五处同步检查：C11b 不改任何 StorageEnum 操作。只动 role-based 的 Namespace trait，删了带默认实现的 list_versions，
只有 S3 覆盖过它；legacy walkdir / walkdir_2 不受影响。StorageError 变体数仍是 29，不加新变体。
```

```
CREATE
======
- src/model/history.rs：ObjectHistory / VersionRecord / RecordKind / HistoryOrder / MAX_HISTORY_RECORDS；
  不变式与 split(cap)；内嵌 mod tests（从 entry_version.rs 迁来的 source 选择器测试，加分段和 current() 测试）
- src/model/history_codec.rs：v7 历史段编解码
- src/model/snapshot_format_tests.rs：golden 字节测试，以及版本字节 4 / 5 / 6 / 8 → UnsupportedVersion
- tests/data/snapshot/v7-nfs-file.hex、v7-s3-history.hex（对象 + 删除标记 + null + Inferred）、
  v7-s3-history-segment.hex（segment=1、complete=0、有记录级 metadata）
- src/storage/backends/s3/histories.rs：提交 1 搬出来的物化版本列举，提交 4 改为每个 key 一个子项、按需逐页转换
- src/traversal/s3_history_tests.rs：带历史的遍历（两种扫描方式、分段、计数、记录级观察、续扫）；
  s3_tests.rs 已有 685 行，新内容不再往里堆
```

```
RISK
====
- HIGH: 版本化桶上所有消费方的默认输出形状都变了：
    - 当前已删的 key 也会出现（current() 为 None）；
    - 按每项发起 TransferRequest 的调用方会在已删的 key 上 describe 到 NotFound；
    - terrasync 要按 Q27 把一项展开成每个版本一行。
  缓解：提交带 !；§13 写明；例子里给出 --current 投影；terrasync 在拿到含 C11b 的构建之前必须先适配（§11 的要求清单加一条）。
- HIGH: 版本化桶上单目录超过 500 万条记录现在会 Capacity 失败。今天默认的 Current 流式列举在版本化桶上没有这个上限。
  这是 Q9 定的，但对版本化桶是一次能力倒退。
  缓解：失败是 typed 的列举级 Capacity，兄弟目录继续；写进 §10.4 和 storage-s3.md 的已知限制；要放开等真实需求再议。
- HIGH: 内存。每个列举都把整个目录物化，Blocks 预取最多 2 × min(c, 64) 个列举同时在内存里，最坏会到数 GB（今天的 All 同样如此，不是新问题）。
  缓解：原始记录物化后按页才转成 ListedChild；记录里的 metadata 装箱；MinIO 上用 10 万版本的目录量一次 RSS；
  超标再考虑给正在物化的列举单独设上限（后续项）。
- HIGH: 快照不兼容。已存的 v4/v5/v6 全部 UnsupportedVersion，terrasync 按首次扫描处理，会全量重扫。
  缓解：golden 测试钉住当前格式；版本字节规则写进 codec 文档和 C12 的 ADR-0008 项。
  另外 10 万条记录的一段快照约 15 MB：terrasync 不能每个版本行都存一份快照，要按 (路径, 段号) 存一次。写进 §11。
- HIGH: 分段和续扫。取消时消费方可能看到一个 key 的前几段就断了。
  缓解：契约写明"收到 complete=true 那一段才算完整，续扫后从第 0 段重发"；
  故障注入：Blocks 在多块发送途中取消 → 续扫必须补齐每一条记录。
- MEDIUM: Q-A 让 role connect 在 GetBucketVersioning 瞬时失败时失败，而 legacy 连接照旧成功。
  缓解：role 和 legacy 的差别写进 storage-s3.md；错误是 Connectivity / Transient，上游可重试；fake 覆盖 unreachable。
- MEDIUM: 计划要求 tags 等元数据时，每个 key 要做 N 次 I/O。逐条串行，占遍历的同一个并发名额，不会爆发，
  但版本很多时会慢。storage-s3.md 写明。
- MEDIUM: filter 语义。已删的 key 的 modified 是删除标记的时间，所以会命中"最近修改"类条件；
  size 条件对它们照旧放行。写进 filter-dsl.md；不改 DSL，所以 F1–F7 规则不触发。
- MEDIUM: 条目层身份必须和 ListObjectsV2 给的逐字节相等，否则 terrasync 在桶开关版本时会整代重新关联，
  expert 半边的核对也会失败。有专门的测试钉住。
- MEDIUM: 检查点 v2 让已存的 C11 检查点失效（UnsupportedVersion(1)）。C11 刚合入，影响只在开发期。
- LOW: legacy src/s3.rs 多一个字段、detect 改为多态，文件已超 3000 行（backlog），这次只增不到 20 行。
- LOW: Blocks 续扫时要重列祖先目录；在版本化桶上每个祖先都要读完整的 ListObjectVersions，续扫启动会变慢。只影响续扫。
- LOW: RustFS 仍是 Inferred（现存问题 11），DXN 多网关时间倒挂，ONTAP F4 的续点处理都不变，已审查 history.rs 和 histories.rs 的路径。
- LOW: close_resource 和取消语义：S3 不持有句柄；取消仍走 cancelled() → Cancelled，不当错误；记录级观察遇到取消直接停，不算失败。
```

```
VERIFY
======
机器检查（全部套 timeout）
- timeout 900 make ci
- cargo clippy --lib --tests --examples --all-features -- -D warnings
- grep -rnE 'TraversalVersions|EntryVersion|VERSION_WITH_|fn list_versions\(&self, request|supports_versions|trailing_run' src examples tests .claude/skills → 空
  （S3Protocol::list_versions(key) 是另一个单 key 的接口，保留）
- grep -c '#\[error' src/error.rs = 29
- make e2e-local；harness-run skill（按 SKILL 的说明，套 timeout）

测试（新增或迁移）
- 模型：
  - history.rs：空记录被拒；超过上限被拒；current() 各种情形；source() 选择器；split(3) 对 7 条记录切成 3/3/1、段号 0..2、只有最后一段 complete、identity_key 不变
  - snapshot_format_tests：三个 fixture 编码后逐字节相等、解码后相等；版本字节 4 / 5 / 6 / 8 → UnsupportedVersion；记录数超过上限 → Malformed；未知标志位 → Malformed
  - tests/observed_entry_snapshot.rs:121 改为同样的版本字节断言
  - src/model/observation_tests.rs:116-217 的 v4 / v6 测试删掉（它们测的行为被删了）
- S3 列举（namespace_tests.rs:210-360, 590；streaming_tests.rs；directory_object_tests.rs:84, 263, 301；listing_failure_tests.rs:159）：
  - Enabled、Suspended、Refused → 每个 key 一个子项，跨页从旧到新；最后一条是删除标记的 key 也在
  - Never、501、NAS → 只发 ListObjectsV2（请求计数），不带历史
  - Unreachable → role connect 失败，Connectivity / Transient，没有任何列举请求（Q-A）
  - 条目层身份等于同一对象在 ListObjectsV2 下的身份（fake 切换版本状态）
  - 打乱顺序：同一个 key 分散在不相邻的页 → 一项，Inferred
  - Q9：max_entries 调小 → Capacity
  - 目录自己的历史 → 空名子项
- 遍历（新文件 s3_history_tests.rs；s3_tests.rs:32/180/227/247/416/644 迁移；s3_directory_object_tests.rs；
  local_tests.rs 里 a_source_without_versions_refuses_to_list_every_version 改为"Local 永远不带历史"）：
  - 两种扫描方式输出逐字段一致；记录级元数据绑定到对应版本；一条记录失败只记在这条记录上
  - Unordered 8 个 worker 交错的 fake 下，同一个 key 的各段连续
  - 分段 cap=3：计数 listed=1 emitted=1 files=1，versions / delete_markers / all_versions_bytes 正确；
    被 filter 滤掉的 key 不产生任何项、filtered=1
  - Blocks 的 BlockCounts 汇总等于 Unordered 的总数
- C11（src/traversal/checkpoint/tests.rs:411 改）：
  - histories 状态翻转 → Mismatch；v1 检查点 → UnsupportedVersion(1)
  - Blocks 小缓冲下在分块发送途中取消 → 续扫的合集覆盖全部记录
  - Unordered 在物化目录的某一页之后取检查点 → 该目录整个重列
- 传输（s3_expert_listing_tests.rs:57, 161；directory_object_tests.rs:231-276）：
  - 当前是对象的带历史条目可以作 expert 源；已删的 key、不完整的段被拒
  - R3 的三种答案各有测试

故障注入（每一项都必须让至少一个测试变红，改完还原）
  1 列表上不按连接时状态，而用 bucket.versions 判定是否列历史
  2 Refused 映射成不列历史
  3 Unreachable 当成列历史（或当成 Never）继续连接
  4 分段写在列举里（一个 key 多个子项）或按段计 listed
  5 分段的各段分到不同 batch 发出
  6 段号不递增，或每段都标 complete
  7 codec 改字段顺序却不改版本字节
  8 解码仍接受 6
  9 历史改回按 LastModified 排序
  10 指纹里没有 histories
  11 Blocks 取消后 delivered 仍然前进
  12 记录级观察失败让整项失败
  13 记录级观察绑定到当前版本而不是对应版本
  14 丢掉最新一条是删除标记的 key
  15 条目层身份改用 VersionScoped
  16 root_object 只看条目层 size（删除标记最新但有内容的历史会漏报）
  17 expert 接受已删的 key
  18 counts 的 merge 漏掉新字段（解构会让编译失败，这一项靠编译器兜底）

真机矩阵（每一格 RESULT: pass，跑完无 dm- 残留、无临时桶；以 .claude/docs/test-environments.md 为准选从哪里跑）
| 存储 | 版本化桶 | 未版本化桶 | 脚本 |
|---|---|---|---|
| MinIO 10.131.9.11 | 临时桶 | data-mover-test | traversal_versions、directory_objects（含 H1 重放）、traversal_resume（S3，cancel 和 kill 两种模式，版本化桶）、versioning_matrix（SKIP_RESUME=1，传输回归） |
| ONTAP 9.19.1 对象桶 | data-mover-test-ver（S3_VERSIONED_BUCKET） | data-mover-test | 同上（不建桶）；另手工一项：F4 那组键（`p/éb/x`、1000 个版本的 mkl/z）→ 一项 1000 条记录、Server 顺序 |
| ONTAP NAS 桶 | — | data-mover-nas | nas_matrix.sh、traversal_versions（UNVERSIONED）：只走 ListObjectsV2、不带历史，与 base 逐行相同 |
| RustFS 1.0.0（105:9001） | 临时桶 | data-mover-test | traversal_versions EXPECT_ORDER=inferred、directory_objects（d/ 只有 null 一条记录） |
| DXN（Ceph RGW） | 临时桶（BUCKET_PREFIX=data-mover-dxn，带 S3_LOCATION，同时不超过 3 个） | data-mover-test | traversal_versions、directory_objects（H1：合并连续删除标记、删 null 版本） |
| StorageGRID 11.5 | data-mover-test-ver | data-mover-test | traversal_versions、directory_objects（每次新建删除标记） |

另外：
- MinIO 上一个 10 万版本的目录：一项 10 万条记录（正好是上限，不分段），量峰值 RSS，并记入 §14 的 C11b 行
- 可选：本机 20 万文件平铺目录跑 Unordered，CPU 在 tally_child 改动前后相差应在噪声内；超过 2% 再上 node1

人工
- storage_role_operations traverse --current 在版本化桶上的输出 = C11b 之前的 --versions current 输出（排序后逐行比较）
- s3_history_replay 把 m/、z/ 重放进临时版本桶，逐条对比
```
