# Historical legacy scanner evidence

These notes describe the retired scanner implementations and past experiments.
For current APIs see [P7 migration](p7-legacy-cleanup.md). No historical result below is a new validation run.

### legacy 列举隐藏传输 artifact（ADR-0006 C3）

- `walkdir` / `walkdir_2`（含版本化桶的版本与删除标记）跳过相对存储根的任意一段以 `.data-mover-` 开头的
  key 与公共前缀（`storage::artifacts::is_artifact_path`），命中打 `trace!`。存储根本身在 artifact 里时照常
  列举；从根以下的 artifact 内开始遍历（`sub_path = ".data-mover-stage"`）则为空。
- `delete_dir_all_with_progress` **不**过滤：删目录会连其中的 artifact 一起删，并为它们发 `DeleteEvent`
  （路径不曾出现在列举里，按删除数与列举数对账会差出这些）。同目录的 `.upload` 指针（C15b）会随目录一起删；
  C15c 前的 S3 stage 集中在根下 `.data-mover-stage/`，删子目录删不到这些孤儿（C19 起不再写，清理见下节）。
- 代价：用户自己命名为 `.data-mover-*` 的对象从 S3 源端列举中消失；孤儿 `.data-mover-stage/` 只能用 S3
  原生工具看到。**过滤必须保留**：它隐藏的是一切 `.data-mover-*` 名字（`.upload` 指针、别的后端的 stage），不只 temp key。
- 真机（`examples/s3_listing`，2026-09-24）：MinIO / DXN / StorageGRID 非版本化，MinIO / DXN 临时版本化桶，
  artifact 全部隐藏，普通对象、多版本与删除标记行为不变。

### 历史版本迁移（ADR-0006 C20）

把一个对象的全部版本迁到**开了版本**的目的桶，是调用方按下面步骤做的事，不是 data-mover 的一个操作
（全文见 `docs/architecture/storage-transfer-architecture.md`「S3 version history migration」）：

1. **列版本**：legacy `walkdir` / `walkdir_2`，源桶版本状态须是 `Enabled`（`detect_bucket_versioning`；暂停
   版本的桶走 `ListObjectsV2`，只有当前对象、没有 versionId）。每条 `EntryEnum::S3` 带 `get_version_id()` /
   `get_is_latest()` / `get_is_delete_marker()` / `get_version_count()`（`S3Entry` 字段都是 `pub`）。
   - `walkdir`：版本 + 删除标记，同一 key 从旧到新，但多个 worker 共用一个 channel，别的 key 的条目可能插在中间
     （按 `get_relative_path()` 分组）；`version_count` 数版本 + 标记；key 之间顺序不定（HashMap、多 worker）。过滤表达式按版本逐条判断（没有 `modified`）；删除标记按
     `name` / `path` / `extension` 判断、没有 size（`size` 条件对它是 `LazyMatch`，放行）—— D4 之前标记完全不过滤（只涉及历史中间的标记）；D4 列出已删 key 后若仍不过滤，
     被排除的已删 key 会漏出它的 latest 标记。
   - `walkdir_2`：只有版本（没有标记），按名字稳定排序，同一 key 从旧到新；`is_latest` = 最后一个版本，
     但最新条目是删除标记时**一个都不标 latest**（该 key 没有当前对象）；`version_count` 只数版本；过滤含 `modified`。
   - 两者：最新条目是删除标记的 key **也列**（用户决定 D4：每种 S3 遍历都要支持版本化桶，C22 之后的 follow-up；
     之前整个不列，历史拿不到）：`walkdir` 发它的全部版本与标记、从旧到新、标记最后且 `is_latest = true`、
     `is_delete_marker = true`；`walkdir_2` 发它的版本、`is_latest` 全是 `false`（只有标记的 key 不出条目）。只影响走
     `ListObjectVersions` 的桶（版本 `Enabled`）；未开 / 暂停版本的桶走 `ListObjectsV2`，行为不变。
     判断在 `version_listing::versions_with_latest`，测试 `s3::versioned_walk_tests`（离线构造 `S3Storage`，不带标签就不发请求）。
     **对调用方是契约变化**：按 `get_is_latest()` 拷贝的调用方必须同时看 `get_is_delete_marker()`。真机（MinIO VM 102，2026-09-25，临时桶 `data-mover-fu-<ts>`（版本化）与
     `-u`（未开版本），跑完都删，服务器只剩 `a-bucket` / `data-mover-test`）：`h/gone`（两个版本后删除）`walkdir` 列两个
     版本 + `latest=1 marker=1 count=3` 的标记、`walkdir_2` 列两个版本 `latest=0 count=2`；未开版本的桶仍只列当前对象（不带过滤表达式跑，在加标记过滤之前；
     不带过滤时两者输出相同，标记过滤由单测覆盖）。
     `.data-mover-*` 不列（C3）；
     `mtime` = 该版本的 `LastModified`（纳秒，服务端毫秒精度）；`include_tags` 时按 versionId 取标签。
   - **C20 修复**：`ListObjectVersions` 一页最多 1000 条，一页可能停在某个 key 的版本中间（key 升序、每个 key 新→旧）。
     以前按页分组：跨页的 key 分成两组，先发新的一段，`version_count` 按页算，`walkdir_2` 还有两条 `latest`。
     现在 `src/s3/version_listing.rs` 把一页里最大的 key 留到下一页合并（**不能用 `NextKeyMarker`**：MinIO
     回的是 `p/z[minio_cache:v2,return:]` 这种自己的续页标记，只能原样当下一次的 `key-marker`），每个 key 按
     `(LastModified, is_latest)` 稳定排序、同一毫秒内按列举顺序反过来。**Q29 起改为服务端顺序**（见下「版本历史顺序」）：
     并发写入的同一毫秒条目按时间排不出来；校验不过的 key 仍按时间推断并标 `S3Entry.version_order_inferred = true`
     （`EntryEnum::get_version_order_inferred()`，`s3_listing` 打印 ` order=inferred`）。`walkdir` 一页内完整的 key 现按 key
     升序发（以前是 HashMap 序）。
2. **逐个拷**：`TransferRequest::new(..).with_source_version(SourceVersion::Id(version_id))`，**同一 key 从旧到新、
   一次一个、前一个成功后再下一个**。流式与原生 S3→S3 都钉住版本（C6 / C18）。`transfer_resume` 例子的
   `--source-version` 是手工入口。
3. **记结果**：每次成功在最终 key 上**恰好加一个版本**，`TransferOutcome.destination_version` 是它（`None` =
   目的端没报真版本）。

要点：

- 选择器进派生身份，每个版本是独立的传输。同一请求（同一 `Id`）从任何无状态进程重提即续传：`Checkpointed` 且
  > 64 MiB 的对象在最终 key 上留分段上传 + `.upload` 指针，`prepare = Resumed { bytes }`；其余（≤ 64 MiB、
  `AtomicReplace`、`Direct`）从零重来。**中断的版本要先续完再拷下一个**：下一个版本 prepare 看到别的身份的指针，
  abort 上传、删指针（`Restarted { OtherTransfer }`；共用 `with_identity_override` 时是 `BindingChanged`），被清掉的
  版本日后再拷就乱序了。
- 目的端版本顺序 = 各次发布的先后，与源端时间无关。同一 key 并行提交会打乱顺序、互删指针；同进程内
  per-destination guard 在 prepare 拒绝第二个（`Conflict` / Transient，「another transfer in this process is writing
  this destination file」）；跨进程靠调用约定（一个 key 一个写者）。
- 不去重：已完成的版本再跑一次就再加一个版本；带 `final_destination_changed` 的失败可能已经生成了一个版本。
- 删除标记没有内容：`Id(标记)` 的带版本 HEAD 回 405 → 条目 `NotFound`。拷贝从不往最终 key 写删除标记；要在目的端
  重放历史里的删除标记，调用方在相邻两个版本之间调 `transfer::write_delete_marker`（P7 H1）：只在 `GetBucketVersioning`
  此刻为 `Enabled` 的桶上发一次普通 DeleteObject（每次调用前都查；未开 / 暂停版本的桶上它会删数据 → `Unsupported`），
  持有与该文件传输相同的进程内租约，结果按删除前后该键的记录判定：`Written{id}`、`Merged{id}`（MinIO、DXN 在最新
  已是标记时不再加）、`NullVersionRemoved{id, marker}`（DXN 删掉 `"null"` 版本；RustFS 删掉未记版本的目录对象），
  都显式报出。删除之后读不到结果时结局未知（`Transience::Unknown`），重放前先比对目的端历史。调用方先用
  `transfer::destination_keeps_history`（此刻是否 `Enabled`）决定重放整段历史还是只拷最新状态（D12）；编排（旧→新、
  一个 key 严格串行、不去重）归调用方，参考 `examples/s3_history_replay.rs`。
- 目的桶暂停 / 未开版本：每次拷贝覆盖唯一的 `"null"` 版本，只剩最后拷的那个，`destination_version` 为 `None`。
- 不保留：目的端版本是新 versionId、`LastModified` 是写入时间；`transfer` 不带源版本的标签、用户元数据、
  content type、Object Lock retention / legal hold、存储类别、加密设置（S3 目的端 `stores_nothing()`，见
  [metadata-negotiation.md](metadata-negotiation.md)）。标签只能由调用方的元数据计划（`MetadataMutation::Tags`，
  写完后设置）给到目的端版本。
- 真机（MinIO VM 102，2026-09-25，临时桶 `data-mover-c20-<run>`，跑完删除，服务器只剩 `a-bucket` /
  `data-mover-test`）：`h/obj` 写 v1、v2、删除标记、v3 → `walkdir` 按序列出这 4 条（标记 `marker=1`、只有 v3
  `latest=1`、`count=4`），`walkdir_2` 列 v1、v2、v3（`count=3`）；`h/gone`（最新是标记；当时不列，D4 之后列）与
  `h/.data-mover-*.upload` 不列。999 个单版本 key 之后的 `p/z`（3 个版本，第一页停在 z3）：修复前 `walkdir` 发
  z3（`count=1`）、z1、z2（`count=2`），`walkdir_2` 两条 `latest=1`；修复后都是 z1、z2、z3，`count=3`，只有 z3 latest。
  `versioning_matrix.sh`（`data-mover-c17{,-lock}-1790313057`，已删）结果与 C18 相同：按 id 拷 v1、v2（流式与原生）
  各得 2 个版本、新→旧 v2、v1、内容与源相同；按 id 中断续传 `Resumed { 16777216 }`、1 个版本。


### 版本历史顺序（P7 评审现存问题 8 / Q29）

`ListObjectVersions` 把每个 key 的版本与删除标记按**一条**序列新→旧列出（`<Version>` 与 `<DeleteMarker>` 交错），
SDK 却把两种元素解析成两个列表，交错顺序丢了；`LastModified` 只有毫秒，并发写入同一 key 时大量同毫秒，按时间合并
会排错。现在：

- `src/s3/version_order.rs`：`send_in_document_order(request)` 给这一次请求挂拦截器（`read_after_deserialization`，
  非流式响应此时体已在内存；只收 2xx，重试时留最后一次），quick-xml 读根元素下 `<Version>` / `<DeleteMarker>` 的
  序列（实体、字符引用、CDATA 都解），与 SDK 的 `versions()` / `delete_markers()` 逐条对齐 —— 种类、`Key`、`VersionId`
  全对上才给位置（`DocumentOrder`），否则整页没有位置。其余字段仍取 SDK 的解析。每页多一次响应体拷贝（≤ 1000 条，
  约几百 KB）。role 协议（`role_protocol/listing.rs`）与 legacy 两处 `ListObjectVersions`（`walkdir` 的
  `process_versioned_prefix`、`walkdir_2` 的 `read_dir_sorted`）都走它；`role_list_versions`（单 key、只看 `IsLatest`）不用。
- `S3ListedVersion.position` = 页内文档位置；`histories.rs::page_versions` 与 legacy `VersionPages::positioned` 把后页的位置接在
  前页之后（整个目录列举一条序列）。
- `backends/s3/history.rs::key_histories`（两条路径共用）逐 key：**全部有位置、在整个列举里连续（跨页也算；legacy 只在每批已完整的 key 内查，被页末扣留的 key 隔开或释放后再出现的 key 查不出）、恰一条
  `IsLatest` 且排最前** → 按位置倒序（旧→新），`HistoryOrder::Server`，完全不用时间；否则 `HistoryOrder::Inferred`，
  latest 都排最后、同毫秒可能排错，分两种：该 key 有条目**没有位置**（读不到原始顺序，存储本身可能没错）→ 修复前的
  合并（版本、标记各自保持列举顺序倒过来，`LastModified` 只决定两条怎么交错，同毫秒版本在前）；有位置但**校验不过**
  （顺序不对的存储）→ 列举顺序一概不信，按 `LastModified` 稳定排序（各列表列举顺序倒过来只作平手序）。有 key 推断时
  每次列举打一条 `warn`（`history::warn_inferred`），不会悄悄退化。跨页重复列出的同一条（同 key、versionId、种类）只留
  第一次。校验只看得到进来的条目：先被滤掉的（artifact、目录自己的标记、收拢进前缀的深层 key）或没有位置的条目夹在某
  key 中间时发现不了它被拆开 —— 只影响本来就不合规的存储。
- 标记：role-based `ObjectHistory::order()`（快照 v7 历史段 flag bit 1 = `Server`；C11b 之前的 v6 快照不再读），
  例子 `storage_role_operations` 打印 ` order=inferred`；legacy `S3Entry.version_order_inferred`（serde 默认 false）/
  `EntryEnum::get_version_order_inferred()`，例子 `s3_listing` 打印 ` order=inferred`。只在推断时打印，服务端顺序时
  输出与以前逐字节相同（`traversal_versions.sh` 的正则不变）。
- 测试：回归数据 `tests/data/s3/minio-concurrent-versions.xml`（一页）与 `…-paged.xml`（同一列举 `max-keys=2`，22 页，
  key 跨每个页边界），MinIO RELEASE.2023-03-20 真机抓取：同一 key 并发 12 PUT + 4 DELETE × 3 轮，44 条（36 版本、
  8 标记：最新已是标记时 MinIO 再 DELETE 不新建标记），20 条与别的条目同毫秒。`s3::version_order::tests` 用重放
  连接器（`Replay`，按顺序回放响应体）跑真实 SDK + 拦截器：role 列举、22 页跨页、legacy `walkdir_2`（`read_dir_sorted`）
  都等于服务端顺序倒序，同一数据按时间合并（去掉位置）≠ 该顺序且标 `Inferred`；503 重试后取成功那次的体；另有实体 /
  字符引用 / CDATA / 空元素 / 带前缀命名空间 / 不一致 / 截断体。
  `backends::s3::history::tests`（同毫秒交错、时钟偏差、跨页位置、分裂 / latest 不在最前 / 两个 latest / 没有 latest →
  按时间、缺位置 → 两列表合并、重复条目）、`s3::version_listing::tests`（legacy 同样规则 + 跨页扣留 + 被滤掉的条目仍占
  位置）、`namespace_tests::
  a_non_conforming_store_is_ordered_by_time_and_marked_inferred`（`MemoryS3` 新旋钮 `oldest_first_versions`：每个 key
  旧→新列出的不合规存储）。变异：永远不用服务端顺序（退回按时间）→ 13 个测试红（含三个真机数据测试）；去掉校验
  （盲信顺序）→ 4 个红；legacy 位置不跨页累计 → 2 个红。
- 真机（MinIO VM 102，2026-09-25，临时桶 `data-mover-fix-1790348583`，跑完删光，服务器只剩 `a-bucket` /
  `data-mover-test`；临时脚本，扩展自 architect 的 `probe_race.sh`，未入库）：`race`（并发 3 轮，44 条 / 8 标记，11 条同毫秒）、`d/race`
  （前缀下并发 1 轮，15 条 / 3 标记）、`seq`（顺序写 20 次 PUT / DELETE，6 标记）。role `traverse --versions all`（C11b 起为版本化桶上的默认 `traverse`）与
  legacy `walkdir` 三个 key 都**逐条等于服务端原始 XML 倒序**、全部 `Server`；`walkdir_2` 等于其中的版本；`seq` 等于
  写入时记录的 versionId 顺序。修复前的按时间合并在 `race` 第 5 条、`d/race` 第 4 条起就与服务端顺序不同。并发写入
  没有客户端可见的“真实顺序”（客户端完成时间受响应延迟影响，21 对相差 ≥ 50 ms 的完成时间与列举相反，但服务端
  `LastModified` 在这 21 对上都与列举一致），以服务端提交顺序为准。

### ONTAP：带 delimiter 的版本列举缺 `NextVersionIdMarker`（P7 现存问题 10 / F4）

ONTAP 9.19.1 的 `ListObjectVersions`（`delimiter=/`）在截断页里有 CommonPrefix、页又停在某个 key 的版本中间时，给
`NextKeyMarker=<该 key>` 却**不给** `NextVersionIdMarker`；只带 key-marker 的下一页从该 key 之后开始，余下的版本被静默跳过
（`mkl/a`、`mkl/x/中/y` + `mkl/z` 1000 个版本：默认 max-keys 只列出 998 个；max-keys 3 时 7 个版本只列出 1 个）。它还会在
下一页重复上一页末尾的 CommonPrefix，并以一个空的末页结束列举。

- 守卫：`VersionsInOrder::next_version_id_marker`（`src/s3/version_order.rs`）——服务端给了就原样用；没给、页截断、且
  本页条目按**文档序**（F2 拦截器的位置；ONTAP 把 CommonPrefixes 放在条目之后）排成一列时，`NextKeyMarker` 那个 key 的
  条目恰是这一列的**末尾一段** → 用最后一条的 versionId 作 version-id-marker（没有 id 的条目按列举的解码当 `"null"`）。
  这就是 marker 对的语义：每个 key 的条目连成一段的存储（S3 的顺序、ONTAP）上都对，页恰好停在 key 的最后一条时两种续法
  都落到下一个 key；MinIO 的 key-marker 是 token（`k[minio_cache:…]`）、且总给版本 marker，守卫不生效。**不猜**的情况 →
  `UnknownResumePoint`：该 key 的条目不是末尾一段（像 RustFS 那样每页先列全部删除标记再列版本，现存问题 11 —— RustFS 实测
  总给版本 marker，只是防它哪天不给）；读不到文档序且版本与删除标记都在该 key 上（只有一种在该 key 上时，key 升序，
  那一种的最后一条就是末尾）。不猜的后果：role 为该目录的 `Protocol` / Permanent 失败；legacy `walkdir` 先发本页已完整
  的 key，丢下停在其中的那个 key，发 `StorageEntryMessage::Error`（`ErrorEvent::Scan`）结束该前缀；`walkdir_2` 同样先列
  完整的 key，再把原因放进 `errors`。
- 三处都走它：role `role_protocol/listing.rs::version_page`、legacy `process_versioned_prefix`（`walkdir`）与
  `read_dir_sorted`（`walkdir_2`）。`role_list_versions`（单 key、不带 delimiter）不受影响。
- 重复的 CommonPrefix：role All 由 `merge` 去重（本来如此）；legacy `walkdir` 每个列举记已下钻的前缀（否则整棵子树再走
  一遍），`walkdir_2` 记已加入的子目录。空末页：三处本来就容忍（测试覆盖）。
- 回归数据（ONTAP 真机抓取，`data-mover-test-ver` 前缀 `f4-1790409358/`，已删）：`tests/data/s3/ontap-versions-delimiter-small.xml`
  （max-keys 3：`mks/a`、`mks/x/中/y`、`mks/z` × 7，含只带 key-marker 的那一页与空末页）、`…-markers.xml`（max-keys 7：
  带删除标记，首页停在 `z` 的一个删除标记上、前面是它的两个版本 —— 两种条目都在页上，末尾取自文档序）、`…-mixed.xml`
  （max-keys 11：80 个含 `é` / `中` / `.` 的随机名、128 条、18 个子目录，缺 marker 与重复前缀都有）。
  `s3::version_order::ontap_tests` 的 `MarkerReplay` 按请求的 prefix / key-marker / version-id-marker 找回显相同的那一页（未抓的
  请求回空的完整页），经真实 SDK + 拦截器跑 role All、legacy `walkdir_2`、legacy `walkdir`（并数每个子前缀被列了几次）；
  另有 RustFS 形状的合成页（标记组在前）验证三处都不猜、legacy 仍发出完整的 key。变异：去掉守卫 → 6 个全红；去掉“末尾一段”
  检查 → 2 红；去掉 legacy 前缀去重 → 2 红；去掉 role `merge` 去重 → 1 红。
- StorageGRID 11.5 对任何空的 `version-id-marker` 回 400 `InvalidArgument`（“A version-id marker cannot be empty”；空的
  `key-marker` 无妨）。三处列举都不发空的、也不发不带 key-marker 的 version-id-marker：首页不带任何 marker，服务端给的空
  `<NextVersionIdMarker>` 当没给（再走上面的守卫）。`MarkerReplay` 另记原始查询串，
  `no_listing_sends_an_empty_or_unpaired_version_marker` 在三份 ONTAP 数据与一份 StorageGRID 形状的手写两页（首页停在公共前缀、
  空 `NextVersionIdMarker`）上查三处列举；变异：role 发 `Some("")`、legacy `walkdir_2` 总带 version marker、不滤空的
  `NextVersionIdMarker` → 各自变红。e2e-s3 脚本自己的 ListObjectVersions（`traversal_versions.sh` / `versioning_matrix.sh` /
  `directory_objects.sh`）同样只在有 key-marker 时带、且只带非空的 version-id-marker；截断页没有 `NextKeyMarker` 时列举按失败结束
  （不再从首页无限重列）（`3080f4a`）。
- 真机（2026-09-26，ONTAP `data-mover-test-ver`，唯一前缀，跑完删光）：`mkl/z` 1000 个版本 role `traverse --versions all`、
  legacy `walkdir`、`walkdir_2` 都列出 1000 个，四组数据（mkl / mks / mkp / rnd，1187 条）与不带 delimiter 的列举逐条相等、
  无重复，每个目录一次。守卫对别的存储不改结果：`traversal_versions.sh` 在 MinIO、RustFS（`EXPECT_ORDER=inferred`，k / gone
  标 inferred、其余 server，逐行精确）、DXN 的临时版本桶，与 ONTAP `data-mover-test-ver`（已有桶模式）上都 `RESULT: pass`，
  临时桶已删、前缀已清空。


| NDX 分页遍历 (2026-09-18) | `storage_role_operations --backend cifs --root <scratch> ndx-walk --entries`：`seed --path sub` 后 2 页 DFS，NDX `0(sub) → gap 1 → 2,3,4 → gap -1`，每个条目 (含目录) `identity=StableWithinBackend`，时间戳为真值；`--max-depth 1` 只出根页且 `sub` 作为条目发出；`delete-tree --delete-root` 3 文件 + 1 目录零失败 | **已实现** (`storage::ndx_walk` + `create_directory_all` 建父目录) |


| 只读 share 作源 (2026-09-27，share `dm_ro_share`：`lisauser` 在 share ACL 上只有 `read`，目录 777 —— 只读只来自 share ACL；夹具 4 文件 + 3 目录，见 test-environments.md) | 同一组 example (`storage_role_operations traverse` / `ndx-walk`，`transfer_resume` 把每个文件拷到本地) 按三个 pin 各跑一遍：**`91e6cfb`** (dev `3f2bd75`)：根页 2 项正常，`sub` 列举 `PermissionDenied` (`scope=Listing`，NDX `page error sub`)，4 个文件读全部 `Transfer failed on Source … Read failed (PermissionDenied)`，`Access Denied (0xc0000022)`；**`37d9404`** (dev `fde8516`)：遍历 7 项 0 失败、NDX 4 页 0 错，文件读仍全部同样失败；**`2649a0b`** (dev `d4383d0`)：遍历 7 项 0 失败、NDX 4 页 0 错，4 个文件都拷出，本地 BLAKE3 / SHA-256 与写入时一致 (9 MiB 那个 `SkippedBelowCheckpointThreshold`，多块读)。写一律被拒且分类正确：`create-dir` → `PermissionDenied`；上传 (Checkpointed) → `Prepare failed on Destination … (PermissionDenied, Permanent)`、`0xc0000022`、`final_destination_changed=false`；`delete-tree` → 文件 `PermissionDenied`、目录 `Conflict` ("descendants were not deleted")；Namespace 删 / 改名文件与目录都 `PermissionDenied`；之后树不变。`--policy direct` 在 preflight 就被拒 (CIFS 不支持 Direct，与只读无关)。服务端对 CREATE 先查 share ACL 再查重名，所以已存在的目录回 `ACCESS_DENIED` 而不是 `COLLISION`：`d4383d0` 的 `create_directory_all("sub/deeper/x")` 报在已存在的祖先 `sub` 上 (`PermissionDenied`)，"根只读、子目录可写"的 ACL 布局下会误拒；**`c7582a0` 已修** —— 被拒的层 `Stat` 为目录即算已存在，`create_directory_all` `sub/deeper`、`sub/deeper/deepest` 成功且不写，`sub/new` / `sub/deeper/new/x` 仍 `PermissionDenied` 且报在第一个不存在的层 (`sub/new` / `sub/deeper/new`)；旧二进制同一组四个都报在 `sub`；经可写孪生路径 `ontap_lisaauto_cifs/dm-ro-share/` 遍历前后都恰为 7 项夹具 | **已实现**：`tests/cifs_readonly_contract.rs` (e2e-cifs，`CIFS_REAL_RO_SHARE`) 守住 —— 遍历恰为夹具树、逐文件拷出比对 BLAKE3、6 种写被拒为 `PermissionDenied`、`create_directory_all` 过已存在的层 (`c7582a0` 起)；`2649a0b` 上 ×2 通过 (`c7582a0` 加断言后再 ×2 通过)，同一测试拿旧 pin 编译则失败 (`91e6cfb`：遍历 `sub` 列举失败；`37d9404`：读 `a.txt` `PermissionDenied`) |


> (namespace ×2、policy ×2、匿名 share ×1、probe ×1、8 轮 seed→ndx-walk→delete-tree)。
