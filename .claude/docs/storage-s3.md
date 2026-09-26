# S3 Backend

## 底层依赖

- **crate**：`aws-sdk-s3` (1.129.0，crates.io)。
- HTTPS：`hyper-rustls` + `rustls`，**自定义 verifier 允许自签证书**。
- IAM credential 链路：标准 AWS 行为 (env / config file / IMDS)。

## URL 形式

```
s3://[ak[:sk]@]bucket.host[:port]/[prefix]
s3+https://[ak[:sk]@]bucket.host/[prefix]
s3+sg://[ak[:sk]@]bucket.host[:port]/[prefix]
s3+sg+https://[ak[:sk]@]bucket.host/[prefix]
```

示例：

```
s3://AKIAXXX:secretXXX@my-bucket.s3.amazonaws.com/data/
s3+https://AKIAXXX:secretXXX@my-bucket.minio.local/exports
s3://my-bucket.s3.amazonaws.com/data/      # 用 IAM credential
```

scheme：
- `s3://` — HTTP (默认 endpoint port 80)。
- `s3+https://` — HTTPS (默认 port 443，自签证书允许)。
- `s3+sg://` / `s3+sg+https://` — StorageGRID endpoint overlay；仍属于 S3 backend，
  仅在签名前精确删除 `x-id`，不新增 validation profile。
- 所有 profile（含标准 `s3://`）都为 multi-object delete 加 body-matching、SigV4-signed
  `Content-MD5`（`s3/delete_objects_md5.rs`，由 `s3.rs` 的 `configure_compatibility` 统一挂载）：SDK 默认只发
  `x-amz-checksum-crc32`，MinIO `RELEASE.2023-03-20`、Ceph RGW Octopus (DXN)、旧 StorageGRID 都回
  `MissingContentMD5`。

## 关键行为

### 404 → FileNotFound

commit `7eb3046`：S3 GetObject / HeadObject 返回 404 时，**必须映射为 `StorageError::FileNotFound`**，不是 `S3Error` 也不是 retry。

错误处理：

```rust
match aws_sdk_s3::operation::get_object::GetObjectError::from(...) {
    NoSuchKey(_)     => Err(StorageError::FileNotFound(key)),
    NoSuchBucket(_)  => Err(StorageError::DirectoryNotFound(bucket)),
    other            => Err(StorageError::S3Error(other.to_string())),
}
```

### Multipart Upload

- 大文件用 `CreateMultipartUpload` + `UploadPart` * N + `CompleteMultipartUpload`。
- 阈值 / 并发度参考 s3.rs 现有写法。
- **失败必须 abort multipart**，否则 S3 会留部分上传占空间。

### IAM Credential 链路

按优先级：
1. URL 中的 `ak:sk@`。
2. 环境变量 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`。
3. `~/.aws/credentials` 配置文件。
4. EC2 IMDS (实例元数据)。
5. ECS 容器 credential。

### 自定义 TLS Verifier

- `s3+https://` 默认允许自签证书 (MinIO / Ceph / 自建 S3 网关常见)。
- 不暴露给用户配置 — 是安全 trade-off，library 决定。
- 修改前必须 PR 说明。

### 作为拷贝源端的元数据

`S3Metadata` 的拷贝基线（K8）：mtime 取对象自己的 `Last-Modified`（`time_util::http_last_modified`，秒精度），
与 ETag / version 出自同一个 HEAD，`observe_bound` 校验身份仍是被描述的对象（换了 → `Conflict`）。没有
`Last-Modified` 时是 `None`，**不回退到当前时间**（legacy `datatime_to_i64` 会回退，新路径不用它）。代价：
拷贝路径每个对象多一次 HEAD（describe、read 各已有一次；复用扫描期观测需要用户决定，暂不做）。
真机（MinIO）：S3 → Local 的文件 mtime 与 HEAD 的 `Last-Modified` 逐秒相等；改前是拷贝时刻。

对象在 describe 之后被替换（身份判定统一走 `source.rs` 的 `object_identity`：真实 versionId 标识版本
（`VersionScoped`），`"null"` / 空 / 无 versionId 以**路径 + ETag** 标识（`PathScoped`，见下「身份包含路径」））：
`read`、native 的 `bind_source`、`observe_bound` 三处都报 `Conflict` / Permanent（不是可重试的 Protocol / Unknown），
按条目失败。

### 身份包含路径（P7 评审现存问题 7 / Q28，破坏性）

`PathScoped` 的契约（`model::IdentityStrength::PathScoped` 文档）：稳定字节**必须包含路径**。S3 以前只放 ETag，内容
相同、路径不同的对象共用一个身份键（所有空对象都是 `d41d8cd98f00b204e9800998ecf8427e`），按身份键关联两次扫描的消费方
（terrasync 的 `INNER JOIN`）会把它们交叉配对。现在没有真实版本的对象 —— `ListObjectsV2` 列出的当前对象、`"null"`
版本、暂停版本桶的 `"null"` 删除标记 —— 稳定字节 = u32 大端路径长度 + 路径 + ETag（标记是固定标签），与 CIFS 无 file id
时同一写法；构造只有一处（`source.rs` 的 `object_identity` / `path_etag_identity` / `path_scoped_bytes`），describe、
两种列举、read、native 绑定、元数据观察、专家 `observation_matches` 都走它，所以列举到的对象与随后 describe 到的同一
未变对象仍判为相等、被替换的对象仍被拒。无真实版本且无 ETag 的对象照旧拒绝（没有东西守护内容）。`VersionScoped`
（真实 versionId）不变。**破坏性**：S3 所有 `PathScoped` 身份键一次性全变 —— 快照、terrasync 世代（升级后第一次扫描
把这些条目看成删除 + 新增）、无真实版本的 S3 源的恢复绑定（这种源的可续传传输升级后从零开始，升级前排空）。
测试 `backends::s3::tests::versions::the_same_content_at_two_keys_has_two_identities`（describe、列举、`"null"` 标记）；
`storage_role_operations traverse --identity` 打印每条的强度与身份键前 8 字节。

- **列举与 HEAD 精度不同**（MinIO 实测）：`ListObjects` 给毫秒（`04:30:34.882Z`），HEAD 只给秒
  （`04:30:34 GMT`）。拷贝落到目的端的是秒。上层若拿列举（legacy `walkdir` 的毫秒 i64，无精度标记）去比
  目的端文件 mtime，永远不等 → 每次重拷；**比较前按秒截断**，或像 `integrity` 那样按两边较粗精度比。
- 未开版本控制的 bucket 里对象被相同内容覆盖时 ETag 不变、`Last-Modified` 变新，身份校验照样通过，拿到新时间；
  字节相同，可接受。
- 服务端没给 `Last-Modified` 时时间族记 `NotApplicable`（没东西可拷，不报 Applied）；K12 的 mtime 必拷守卫
  要检查 `modified` 真的有值。
- `integrity::compare` 对声明 mtime `NotStored` 的目的端（S3）不比 mtime：那是上传时间，拷贝从不写它。

### 源端版本（ADR-0006 C6）

`S3ReadSource::describe_version` 解析并钉住版本：`Current` 若 HEAD 带真实 versionId（非空、非 `"null"`）就钉成
`Id(v)`，否则保持 `Current`（仅靠 ETag 守护，与改前一致）；`Id(v)` 走 `HEAD ?versionId=v`。之后 read（HEAD +
带 versionId 的 ranged GET）、元数据（`observe_copy_bound_version`：该版本的 `Last-Modified`、按 versionId 取 tags）、
native bind 都用钉住的版本，所以传输中途出现新版本也拷完起始的那个版本（改前是后续 read 报 `Conflict`）。
`content_version` 记 ETag（空 ETag 记 None），所以同一 versionId 内容变了（不应发生，但 fake 与某些兼容实现会）
绑定也变。删除标记（405）/ 不存在的版本 / `NoSuchVersion` → 条目 `NotFound`；这些映射只作用于带 versionId 的请求。
MinIO 2023 / DXN / StorageGRID 11.5 上未开版本桶的对象 HEAD 都不带 `x-amz-version-id`（2026-09-24 实测）。
MinIO 2023 版本化桶实测（临时桶，2026-09-24）：开版本前写入的对象 HEAD 报 `x-amz-version-id: null`；HEAD 删除标记
版本 → 405；不存在的版本 → 404；格式错的 versionId → HEAD 400（无错误码）、GET `InvalidArgument`；当前是删除标记时
HEAD 当前 → 404、HEAD 旧版本 → 200。传输中途上传新版本，钉住的拷贝仍得旧版本字节。

### 作为拷贝目的端的元数据

`S3StagedDestination::copied_metadata_target` 声明什么都不存（mtime `NotStored`、owner/ACL/xattr 都
`Unsupported`）：拷贝不读源端元数据，报告把每个要的族记成 `skipped()`（`DestinationCannotStore`）。对象的
时间是写入时间。见 [metadata-negotiation.md](metadata-negotiation.md)「目的端什么都不存」。
真机：VM 102 的 MinIO（`.claude/skills/e2e-s3/.env`，bucket `data-mover-test`），
`examples/nfs_metadata_copy.rs` 的 `s3:<prefix>` 端点。**URL 里的 AK/SK 原样使用、不做百分号解码**
（e2e-s3 的 `url_builder.py` 会 quote，含特殊字符的 SK 会因此出错）。

### 目的端写入策略基线（C0，改造前，2026-09-24，VM 102 MinIO RELEASE.2023-03-20）

`bash .claude/skills/e2e-s3/scripts/staged_matrix.sh`（驱动 `examples/transfer_resume.rs`，只写/删 `staged-<run>/`）。
C0 时所有策略都是「temp key 分段上传 → CopyObject 到 final → 删 temp」，1 KiB 也不例外（C14b 起 ≤ T 的对象
改为一次 PutObject，见上）。

| 用例 | 结果 / 耗时 |
|---|---|
| 0 B / 1 KiB，任意策略 | 成功，40–110 ms |
| 8 MiB / 8 MiB+1，读回开 | 1.1–2.2 s；读回关 0.7–0.9 s |
| 200 MiB，Checkpointed，读回开 / 关 | 18.3 s / 9.4 s（读回约占一半） |
| 200 MiB，AtomicReplace，读回开 / 关 | 19.6 s / 9.2 s |
| Direct，任意大小 | Preflight 拒绝：S3 不支持 direct（C14c 起支持，见下） |
| 原生 S3→S3 1 KiB / 200 MiB | 0.1 s / 25.3 s（两次服务端全量 copy） |
| Checkpointed 200 MiB 取消后续传 | **续传失败**：Prepare `NotFound`（见下）→ 去掉 claim 后成功 |
| SIGKILL 后续传 | **续传失败**：同上 → 去掉 claim 后成功 |

**续传曾在这台 MinIO 上完全不可用**（已修）：`recover` 第一步是 claim（`PutObject` + `If-None-Match: *`），
MinIO 2023 不支持条件创建，对新 key 也回 404 NoSuchKey → 映射成 `NotFound` → Prepare 失败；取消后上传其实还在。
修复：S3 续传不再写任何标记对象 —— 谁能续传由引擎的恢复记录独占租约（`recovery_store.rs` `open_lease`，
**本机**文件锁，覆盖整个尝试；跨主机或共享恢复目录不受它保护）决定，且同一目的 key 不会被两个传输并发写
（terrasync 保证，用户确认）。直接调用 `recover` 的代码要自己保证独占，`claim_token` 对 S3 无意义。修后真机：取消 / SIGKILL
后续传都成功（200 MiB，读回校验通过），记录随后清除。旧版本在 AWS 上成功 claim 后崩溃留下的 `<temp>.claim`
对象新代码不再认也不再删：不会自动清理；它和 temp 对象都在 `.data-mover-stage/` 下，legacy 列举已不再报告它们（见下）。已知：3 s / 20 MiB/s 中断后只有 1 个分段（8 MiB）可复用，粒度待查。
（以上是 C0 的历史：C15c 起 S3 不再走 temp key 与本地记录，C19 删掉了整条 temp-key 路径，`recover` 对 S3 报
`Unsupported`；遗留对象怎么清见「删除 temp-key 路径（C19）」。）

### 小对象：一次 PutObject 到最终 key（ADR-0006 C14b）

- 阈值 T = `S3BackendConfig.single_put_threshold`（`None` = 8 MiB，范围 [5 MiB, 5 GiB]，越界在 connect
  时、联网前报 S3 的 `BackendConnectError`「invalid configuration」；storage 层按架构守卫不能引用
  `crate::error`，所以不是 `StorageError::ConfigError`）。已知大小 ≤ T（含 0 B）的对象：prepare 不开 upload、不建 temp key；stage
  状态（缓冲、待设 tags、写入事实）放在 `PreparedStage::backend_state`（`staged/single.rs`），不进适配器的
  stage 表 —— 丢弃的 stage 不会滞留缓冲。无续传（`disable_recovery`），运行处也不记任何东西。
- publish = 一次带 Content-MD5 的 `PutObject` 到最终 key。`BadDigest` 等明确拒绝 → 最终对象未变；
  回复丢失 → HEAD 对账（大小相同且 `ETag` = 我们的 MD5 算已发布），否则 `final_destination_changed`。
- 校验在发布**之后**（`verification_point` = `AfterPublish`）：先 HEAD 当前对象（`ETag` / 版本已不是我们的
  → `Conflict`），再按我们的 versionId 读；桶无版本时带 `If-Match: <我们的 ETag>` 读。
- 大于 T、大小未知的对象走 C15b 的最终 key 分段上传（C15c 起）；原生 S3→S3 C18 起也直接写最终 key（见「原生 S3→S3 到最终 key」）。

### `Direct`（ADR-0006 C14c）

- `supports_direct() = true`；`prepare_direct` 拒绝「写回自己的源对象」（`Conflict`），已知大小 ≤ T → single stage，
  否则 multipart stage；都标 `direct`、不续传、`durable_publication = false`（`staged/direct.rs`）。
- `write` 内完成全部写入：≤ T 缓冲后一次 `PutObject`（复用 C14b 的发送与对账）；> T / 大小未知 → 先 abort 最终 key
  上前人留下的 upload（`list_uploads`，失败只告警），**在 `write` 里**才 `CreateMultipartUpload`（写之前就失败的
  传输不会留 upload），每段带 `Content-MD5`（MD5 在 `spawn_blocking` 里算），Complete。各段 `ETag` 都等于本段 MD5
  （SSE-KMS 下不是）且对象 `ETag` 是 `…-N` 形式时，必须等于各段 `ETag` 的复合值（否则条目 `Corruption` / Permanent；
  MinIO 实测一致）。Complete 任何失败（回复丢失、SDK 重试后 `NoSuchUpload`……）→ HEAD，大小与复合 `ETag` 都对才算
  完成。任何失败都在 `write` 里 abort（试两次；引擎不保留失败的 Direct stage，仍 abort 不掉的 upload 留给下一次
  同 key 的 Direct 写或桶的 lifecycle 规则）。
- `publish` 只返回写入事实（带 versionId）；`verification_point` = `AfterPublish`，按版本 / `If-Match` 读最终对象；
  `discard` 只 abort 仍开着的 upload，**从不删最终 key**；元数据经 metadata 角色直接写到最终对象。
- 真机：`PREFIX=data-mover-c14c-<ts> bash .claude/skills/e2e-s3/scripts/staged_matrix.sh`，Direct 行下载比对并数精确
  key 上的 upload（`direct: equal=yes key_uploads=0`）。

### 分段积木（ADR-0006 C15a，行为不变）

- `S3Protocol::upload_part` 每段带 `Content-MD5`（不符 → `BadDigest`，条目 `Corruption` / Transient）；
  `complete_multipart` 返回 `S3WriteFacts`（对象 `ETag` + 规范化后的 versionId）；`list_uploads(key)` 只返回
  **恰好这个 key** 上进行中的 upload id（按前缀列再过滤 `Key == key`：MinIO 只按精确 key 列，AWS / Ceph /
  StorageGRID 按前缀列）。实现在 `src/s3/role_protocol/multipart.rs`。
- `composite_etag(part_etags)`（`protocol.rs`）：`"<各段二进制 MD5 拼接后的 MD5>-<段数>"`；任一段 `ETag`
  不是带引号的 32 位 hex MD5（SSE-KMS 等）→ `None`，不做检查。
- `InvalidPart` / `InvalidPartOrder` → 条目 `Conflict` / Permanent；`EntityTooSmall` → 条目 `Corruption` / Permanent。
- `MemoryS3`：分段校验 MD5、同号分段覆盖、Complete 核对 (号, ETag) 表并按表拼对象、返回复合 ETag 与版本、
  `complete_commits_then_fails` 注入「已提交但回复丢失」、`part_failure` 注入某段失败。

### 最终 key 上的分段上传与 `.upload` 指针（ADR-0006 C15b；C15c 起默认打开）

- 开关 `recovery_at_destination()`（C15b 时真连接恒为 `false`，C15c 起恒为 `true`；C19 删掉字段与测试开关，
  直接返回 `true`；C21 连同本地恢复存储把这个开关从 trait 删掉；`connect_at_destination` 只是把自动间隔换小的测试连接）。自动 checkpoint 间隔 64 MiB
  （`automatic_checkpoint_interval_bytes`）。
- `prepare_at_destination`（`staged/at_destination.rs`）：已知大小 ≤ T → C14b single stage，但先只看指针
  （HEAD，有才 GET；有遗留 → 删指针 + abort 该 key 上所有 upload，报 `Restarted{..}`；不列 upload，省请求）。
  > T 或大小未知 → `discover`，然后在**最终 key** 上分段上传：
  - 指针对象 `.data-mover-<d>.upload`（`sibling_artifact(final, Upload)`，`staged/upload_pointer.rs`）：
    `DMDPTR01`，**无 durable prefix**（`ListParts` 才是持久记录；每个 checkpoint 重写会在版本化桶里各留一个版本），
    扩展 `DMS3UP01 ‖ nonce16 ‖ u64le 分段大小 ‖ u16le 长度 ‖ upload id`。一次带 Content-MD5 的 `PutObject`
    写入（原子，不用临时名；失败但读回逐字节相同算写成），HEAD + 钉住的 ranged GET 读，`DeleteObject` 删。
    `accepts_pointer` 只认这个形状（无前缀、tag 对、id 非空 UTF-8、分段 ∈ [5 MiB, 5 GiB] 且 10000 段装得下）。
  - `observe_stage`：有可接受指针 → `ListParts`（`NoSuchUpload` → 无 stage）取**连续前缀**：第 1..k 段，每段
    恰为指针的分段大小且不超出源大小；只有最后一段可以更短，且必须恰好结束在源大小。**缺段不是错误**（SIGKILL 后
    分段乱序完成会留缺口；旧 temp-key 路径把缺口当永久 `Corruption` 并 abort）：缺口后的分段重传，同号覆盖。
    无指针 → 该 key 上有 upload 即 `Some(0)`（`StageWithoutPointer` 清掉）。`remove_stage` abort key 上所有 upload。
  - Resume → 用新 nonce 重写指针（接管），再 abort key 上**其他** upload；Fresh / Restarted → discovery 已清场，
    `CreateMultipartUpload`；`recoverable` 立即写指针，否则在第一个 deferred checkpoint（本次 write 被服务端确认的
    分段字节数达到间隔时）写，并打开 recovery（C16 起 `Discover` 且已知大小超过间隔的也在 prepare 时写指针，
    recovery 仍在 checkpoint 才开，见「续传粒度」）。
- `write` 从前缀后的段号续传（复用 `parts.rs`，`PartTarget.checkpoint` 钩子），**不 Complete**；
  `verification_point` = `AfterPublish`。
- `publish`：先查栅栏（指针须带我们的 nonce；从未写过则须不存在；否则永久 `Conflict`，最终 key 未变）→
  用全部 (段号, ETag) Complete → 本次 write 发出的段 ETag 都是其 MD5 时核对复合 ETag（不符 → 永久 `Corruption`，
  最终 key 已变；同一 upload 的加密方式一致，所以续传前缀也适用）→ Complete 失败：`ListParts` 仍列得到 → 没完成，
  `final_destination_changed=false`，保留 stage（C15c 改为只有提交前的明确拒绝才算没变，见下节）；`NoSuchUpload` → HEAD 大小 + 复合 ETag 对上算已发布（C17 起改为
  ListObjectVersions 并认领版本，见「版本化桶」），否则 `Conflict` 且最终 key 已变 → 设 tags → 删指针（若是我们的）→ 证据带版本。
- `discard`：查栅栏；仍是我们的 → 先删指针再 abort；被接管 → 什么都不动；从不碰最终 key。原生拷贝遇到这种
  stage → C18 起用 `UploadPartCopy` 填（见「原生 S3→S3 到最终 key」）；它没有本地 recovery identity。
- `MemoryS3` 新增：`complete_failure`（Complete 失败且不提交）、`complete_etag`（Complete 报告指定 ETag）；
  abort 不存在的 upload → `NoSuchUpload`（NotFound），`aborts` 计请求数。

### 目的端恢复打开（ADR-0006 C15c，破坏性）

- `recovery_at_destination()` 对每个 S3 连接恒为 `true`：引擎经进程内租约 + `prepare_at_destination`，
  **不再碰本地 recovery store**，S3 续传不需要 `DATA_MOVER_RECOVERY_DIR`；自动 checkpoint 间隔 64 MiB（D3：
  ≤ 64 MiB 的 Checkpointed 对象从不写指针）。当时还留着仅测试可达的 `with_recovery_at_destination(false)`
  （旧 temp-key 路径 + store），C19 已删。
- **原生 S3→S3**（C15c–C17，C18 已改写）：引擎的 at-destination 原生分支当时改用目的端普通的 `prepare_ephemeral`
  （S3 = temp key，发布时 CopyObject），标 `at_destination`、Fresh、带租约；同 key 上以前流式传输留下的指针 / upload
  不清。C18 起原生拷贝直接写最终 key，并按 discovery 续传或清理这些遗留。
- **指针只给 > 64 MiB**：`recoverable` 的新 upload 只有已知大小超过自动间隔（或大小未知）才在 prepare 时写指针，
  否则 `disable_recovery`（expert 目的端半程对每个超过一个 chunk 的 Checkpointed 对象都要 recoverable；现在
  ≤ 64 MiB 的报 `SkippedBelowCheckpointThreshold`）。
- **Complete 失败从严**：只有服务端在提交前就明确拒绝（`single::definite_refusal` 的 4xx 类：`InvalidPart` /
  `InvalidPartOrder` → `Conflict`，`EntityTooSmall` → `Corruption`，`AccessDenied`、签名 / 凭据……；**`NoSuchUpload` 除外**，
  它也是「重试一个已提交的 Complete」的回答）才报 `final_destination_changed = false` 并保留 stage。其他失败（重置、
  超时、5xx、无法解析）即使 `ListParts` 仍列得到 upload 也报 **已变**：超时的 Complete 可能之后在服务端完成，之后的
  discard 再 abort 得到 `NoSuchUpload` 会被当成功。`NoSuchUpload` 的对账（HEAD 大小 + 复合 ETag）不变。`Direct` 的
  Complete 失败本来就经引擎报已变（`with_stage` 对 direct stage 恒置 changed），不是同一问题，只补了测试。
- **MinIO 的 upload id 两种写法**：`CreateMultipartUpload` 发 base64url(`<deployment id>.<uuid>`)，
  `ListMultipartUploads` 却列裸 `uuid`（RELEASE.2023-03-20 实测），两种写法之后都被接受。续传接管后「abort 该 key
  上**其他** upload」逐字节比较会把自己的 upload 当成别人的 abort 掉（续传的 write 随即 `NoSuchUpload`）——
  `upload_pointer::same_upload` 认这两种写法。`MemoryS3.minio_upload_ids` 模拟它。
- 破坏性：升级时在途的 temp-key 传输（`.data-mover-stage/…` + 本地记录）不再续传，升级前排空（D6）；版本化目的桶里
  每个 > 64 MiB 的对象会留下一个指针版本 + 删除标记（C17 已修：指针按版本删）。
- `MemoryS3`：`part_failure_waits`（失败的分段等更低号分段都存好再失败，切断点确定）、`minio_upload_ids`、按分配缓存
  MD5（每次 ranged 读都核对 ETag，大对象测试原来要几十秒）；`tests::endpoint_of(&protocol)` 每个内存桶一个
  endpoint —— 所有 S3 传输现在都拿引擎的进程内租约（按 endpoint + 路径），并发测试写同一个 key 会 `Conflict`。
- 真机（MinIO，VM 102）：`resume_matrix.sh` 200 MiB、20 MiB/s、`CUT_MS=12000`、`KEEP_STATE=0` —— cancel 与 SIGKILL
  都是本地记录 0、切断后 1 指针 + 1 upload、续传 `Resumed{…}`、BLAKE3 相等、之后 0 / 0。默认 `CUT_MS=6000` 时
  S3 只确认了不到 64 MiB（checkpoint 数的是服务端确认的分段，读端领先最多 4 段），两次都是
  `Restarted{StageWithoutPointer}` 全量重拷 —— S3 要 12 s。

### 续传粒度（ADR-0006 C16）

- 实测（MinIO，200 MiB、20 MiB/s，C15c 之后）：**取消**丢不到一段（12 s 切断时读了 161061888，续传复用
  159383552 = 19 段 —— 在途分段会传完）；**SIGKILL** 丢在途分段（最多 4 段 × 8 MiB + 正在读的一段）：9 s → 复用
  75497472，12 s → 117440512。这两种都是分段本身决定的。
- 修的是第一个 checkpoint 之前被杀：以前指针在服务端确认满 64 MiB 时才写，之前被杀只剩一个没有指针的 upload，下次
  prepare 只能 abort（`StageWithoutPointer`）全量重来 —— 源慢时这个窗口可以是几分钟，正是容器重启的场景。现在
  `ResumeMode::Discover`（引擎为它挂了 deferred checkpoint）且已知大小超过自动间隔的新 upload 在开始时就写指针
  （`pointer_before_checkpoint`），和 `recoverable` 一样。哪些对象有指针不变（D3：> 64 MiB 的 checkpointed 对象，每个
  一次指针 PUT）；stage 的 recovery 仍在第一个 checkpoint 才打开（`PointerCheckpoint` 按 `recovery_enabled` 挂，不再按
  「指针已写」）：之前失败报无可恢复 stage（`has_recoverable_stage() == false`），discard 照旧删指针 + abort；**没被
  discard 的失败**（被杀的写者，或调用方直接 drop 了失败）都留下指针，下次从列出的分段续传。围栏也从一开始就生效。
- 行为变化：第一个 checkpoint 前失败又被 drop 的传输（哪怕一段都没传）会在桶里留下可见的 `.data-mover-*.upload`；桶的
  AbortIncompleteMultipartUpload 生命周期只清 upload，指针要等该 key 下次 prepare 按 `PointerWithoutStage` 清。版本化桶
  里第一个 checkpoint 前失败并被 discard 的传输现在也留一个指针版本 + 删除标记（C17 已修：指针按版本删）。
- 6 s 切断（C15c 时两种都从 0 重来）：cancel 续传 75497472、SIGKILL 续传 41943040，BLAKE3 相等，之后 0 / 0。
  `resume_matrix.sh` 对 S3 不再需要 `CUT_MS=12000`。

### 版本化桶（ADR-0006 C17）

- 协议新增（`src/s3/role_protocol/versions.rs`）：`delete_version(key, id)` = 带 `versionId` 的 DeleteObject，
  彻底删这个版本、不加删除标记；删不存在的版本回 204 算成功（MinIO 实测）；Object Lock 拒绝 → 条目
  `PermissionDenied` / Permanent（AWS 403 `AccessDenied`；MinIO 400 `InvalidRequest`「Object is WORM protected」，实测）；
  405（存储不支持按版本删）→ `Unsupported`，不是带版本读的「删除标记」`NotFound`。`list_versions(key)` =
  ListObjectVersions 以 key 为前缀、只留精确 key，一页里出现别的 key 就停（精确 key 排在它前缀的所有 key 之前）。
  `S3WriteFacts.reported_version` 按响应原样保存版本（含 `"null"`），`version_id` 仍只放真版本。
- **指针按自己的版本删**（`upload_pointer::delete`）：PUT 回的版本；回复丢失但读回逐字节相同 → 用读回 HEAD 的版本
  （字节含本次 prepare 的 nonce，不可能是别人写的）；discovery 清遗留指针 → 删它读到的那个版本（`S3Artifacts`
  记 `found_version`）；续传接管写了新指针后，删被替换的旧指针版本（`delete_replaced`），否则我们的删掉后旧的会重新
  变成当前 —— 删不掉（被锁住等）只告警，并把自己的 `pointer_version` 清成 `None`：发布 / discard 时改发普通删除，一个
  删除标记把两个都盖住。被替换的是 `"null"` 而我们的写入没报真版本（暂停 / 未开版本，有的存储连 `null` 头都不回）→
  那个 `"null"` 已被我们覆盖，不删（否则删掉的是自己的指针，发布时围栏报被接管）。
- **扫尾**（`upload_pointer::sweep`）：SDK 重发一个首发已提交但丢了回复的 PutObject，会留下两个逐字节相同的版本，
  我们只知道后一个；按版本删掉它，前一个就成了当前。所以每次按版本删指针后再读一次当前指针：内容是「陈旧内容」之一
  （删掉的那份、续传替换掉的那份）就按它的版本再删，最多 4 次；别人的指针不动；失败只告警（对象已发布 / 已丢弃，下次
  prepare 会清）。发布 / discard / discovery 清遗留 / 小对象清遗留都走它。暂停版本（PUT 回 `"null"`）→ `?versionId=null`（MinIO 在未开版本的桶上也接受）；响应不带版本
  （未开版本的桶）→ 仍是普通 DeleteObject。
- **Object Lock**：删指针版本被拒（`PermissionDenied`：legal hold / retention，或策略只给 DeleteObject 不给
  DeleteObjectVersion）、存储不支持按版本删（`Unsupported`）、或拒绝它自己报的 `"null"`（`InvalidInput`）→
  `tracing::warn!`，改发普通 DeleteObject 用删除标记盖住它；传输照常成功并报版本，下次 prepare 看不到指针。
  **标记也被拒**（`PermissionDenied`：Ceph RGW / DXN 在当前版本有 legal hold 时连普通 DELETE 也回 403 `AccessDenied`，
  2026-09-25 实测；AWS / MinIO 加标记）→ 再一条告警，指针**留在原处**（`upload_pointer::Removal::Held`），调用方照常
  继续：发布报版本、discard 照样 abort（删指针的其他失败也先 abort 再报错）、小对象 / 原生单拷贝的 `clear_leftover` 直接继续
  （single stage 没有指针栅栏）、discovery 清遗留时把它的字节记下（`S3Artifacts::held_pointer`），新 upload 的
  `FinalUpload.held_pointer` 让栅栏在自己写指针前把「恰好是这份字节的指针」当成没有指针（不是别人接管）—— 否则不写指针的
  upload（原子 / 不可续）发布时会报 `Conflict`，这个 key 以后每次都卡住。之后该 key 每次 prepare 都会看到它
  （`PointerWithoutStage`，多几个请求 + 告警），直到 hold 撤掉后被清掉。桶策略同时拒绝 `DeleteObjectVersion` 与
  `DeleteObject`（同样 403）也落到这里：每次传输都留一个指针版本、各一条告警，越积越多 —— 看到这条告警先查权限。栅栏
  例外只认**那份字节**且只在本 stage 写指针之前：别人盖在上面的指针、或我们写过之后它又变回当前（我们的被别人删了）
  都照旧是接管（`a_held_leftover_excuses_no_take_over`）。不带版本的普通删除（未开版本的桶）失败照旧是失败。
- **发布后删不掉指针只告警**（DXN 修复，2026-09-25）：Complete 成功（或结果不明但已认领）之后，删自己的指针**任何**失败
  （锁、断连、5xx）都只 `tracing::warn!`，传输照常报已发布与版本 —— 对象已提交，报失败会让重试再加一个版本；留下的指针
  （upload 已没了）下次 prepare 按 `PointerWithoutStage` 清。以前是 `changed` 失败：DXN 的锁行因此报
  `Publish failed … Namespace failed at .data-mover-….upload (PermissionDenied, Permanent)`，而最终 key 已写好。
- **最终 key 从不发普通 DELETE**：审计过所有路径，只有 artifact（指针；C19 前还有 temp key）会被普通删除。`MemoryS3` 记录每个
  普通删除，`plain_deletes_of_final_keys()` 在所有版本化测试里断言为空（`the_fake_records_plain_deletes_of_final_keys`
  证明这个断言有效）。
- **Complete 结果不明**（`staged/completion.rs`）：最终 key 分段上传 —— 不是提交前的明确拒绝、`ListParts` 回
  `NoSuchUpload`（调用约定下 = 已提交）→ ListObjectVersions，**最新**条目是版本（不是删除标记）且大小 + 复合 ETag
  对上 → 认领它的版本作 `destination_version`（未开版本的桶列成 `"null"` → 不认领；列不出版本或列表为空 → 退回
  HEAD，不认领；残余风险：任何在完成前 abort 了我们上传的东西 —— 桶的 lifecycle 规则、另一个写者 prepare 时的
  `abort_uploads` —— 也回 `NoSuchUpload`，若最新版本恰是内容相同的旧对象，会认领那个旧版本；另一个写者随后写入的
  相同内容也会被认领 —— 字节仍相同，只是版本未必是我们的 Complete 产生的）；
  否则 `Conflict`、最终 key 已变。C15c 的 `final_destination_changed` 规则不变。`Direct`：先 abort —— 回
  `NoSuchUpload`（已完成）→ 同上认领；abort 成功（我们的没完成）或失败 → 仍按 C14c 只 HEAD 比对，相同的旧对象算写成，
  但**不认领版本**。
- 暂停版本：PUT 回 `x-amz-version-id: null`，MinIO 的 Complete 不回版本头 → `destination_version` 都是 `None`。
- **原生 S3→S3 的 temp key 路径**（历史：C18 起只在开关关掉时可达，C19 已删，下面的 `delete_temp_key` 随之删除）：temp key 按 key 名只属于这个 stage，删除改为 `publication::delete_temp_key`
  —— 列出它的每个条目（版本、删除标记、`"null"` 版本都算）逐个按 id 删（版本化桶里普通删除会把整份对象留在删除标记
  下面；`[null(最新), v1]` 这种先开版本后暂停的情况两个都删）；列表为空 → 什么都不发（不加空删除标记）；列不出
  （告警）/ 按 id 被拒（锁、不支持；`"null"` 删不掉一律）→ 普通删除；版本已不在算成功；其他失败照常报错。`CopyObject` 成功后 HEAD 最终 key 取当前版本作 `destination_version`（对账路径不认领）。每次原生
  发布多一个 LIST + 一个 HEAD（C18 的最终 key 路径没有这两个请求：版本取自 CopyObject / Complete 的回复）。
- 未解决：C17 之前留下的指针版本 + 标记都在标记下面，不清理。单 PUT 丢回复的对账（C14b）仍认领 HEAD 的版本，内容相同
  的旧对象也会被认领（后续可比照分段改成列版本）。
- `MemoryS3` 另有（DXN 修复）：`lock_refuses_delete_markers()`（当前版本被扣时普通删除也回 `PermissionDenied`）、
  `delete_failure`（删某 key 一律失败）、`copy_omits_version`（CopyObject 回复不带版本）、`version_lists`（ListObjectVersions
  计数）；普通删除在被拒之前就记进 `plain_deletes`，`plain_deletes_of_final_keys()` 也看得到被拒的。
- `MemoryS3` 版本化模式（`memory_versions.rs`）：`set_versioning(Enabled | Suspended)`、每 key 有序的版本与标记、
  `delete_version` / `list_versions`、`lock_versions(key)`（按版本删被拒、普通删允许）、`version_deletes()`、
  `keys_with_versions()`、`written_after_lost_completion`（丢回复的 Complete 之后另一个写者覆盖）。
- 测试：`transfer::s3_versioning_tests`（引擎级：各写法报版本、原生拷贝报版本且不留 temp 版本、中断续传只加一个版本、discard 后桶不变、按 id 拷
  v1→v2 按序、按 id 中断续传一个版本且 reused > 0、暂停版本、指针被锁、Complete 结果不明认领版本）、
  `staged::at_destination::tests::versioning`（丢回复的指针 PUT、重复存了两份的指针 PUT（发布与 discovery 都扫掉）、续传
  越过重复指针、暂停版本下续传不删自己的 `"null"` 指针、discard、discovery 按读到的版本删、续传删被替换的版本、
  锁住的指针被标记盖住、续传删不掉被替换的锁住版本仍被标记盖住、Complete 后被覆盖 → Conflict）、`direct::tests::an_unfinished_completion_over_an_identical_object_claims_no_version`、
  `completion::tests`、`role_protocol::versions::tests`（`publication::tests` 随 temp key 在 C19 删除）。
  `MemoryS3` 另有 `put_stored_twice`（SDK 重发已提交的 PUT）、`put_omits_version`（PUT 回复不带版本头）、
  `lock_version(key, id)`。
- 真机（MinIO VM 102，`versioning_matrix.sh` 跑三次，临时桶 `data-mover-c17{,-lock}-1790298684`、`…-1790299956`、
  `…-1790302266`，跑完都已删；第三次在指针扫尾 / temp key `"null"` 修复之后，结果相同）：4 MiB checkpointed、200 MiB checkpointed、Direct 4 / 20 MiB、原生 S3→S3 4 / 200 MiB
  各 1 版本、0 标记、0 artifact，
  `destination_version` = 最新版本；按 id 拷 v1、v2（各 100 MiB，流式）→ 2 个版本、新→旧为 v2、v1，内容与源相同；
  按 id 拷 v1 3 s 取消后续传 `Resumed { 16777216 }`、1 版本、内容为 v1；续传矩阵（200 MiB、20 MiB/s、6 s）cancel
  `Resumed { 75497472 }`、SIGKILL `Resumed { 41943040 }`，BLAKE3 相等，1 版本、0 标记、0 artifact；Object Lock 桶里
  拷贝中给指针版本加 legal hold → `result=ok` 带版本、1 条告警、指针被 1 个标记盖住；暂停版本 4 MiB / 200 MiB /
  Direct 20 MiB 均 `destination_version=null`、1 个 `"null"` 版本、0 标记。

### 原生 S3→S3 到最终 key（ADR-0006 C18）

- 引擎 at-destination 原生分支不再 `prepare_ephemeral`（temp key），改问 native pair 的目的端
  `NativeEndpoint::prepare_native`（crate 内部），请求 `Checkpointed` → `Discover`、其他 → `Restart`，`recoverable = false`；
  S3 在最终 key 上 prepare（`staged/native_final.rs`），报 `Fresh` / `Resumed` / `Restarted` 与流式相同。
  **开关开时原生路径碰不到 temp key**；temp-key 代码（与它每次发布多出的 LIST + HEAD）当时只在
  `with_recovery_at_destination(false)` 的测试里可达，C19 已删。
- 协议新增：`copy_from(source, to)` = `CopyObject`，`x-amz-copy-source-if-match` 钉 ETag、有版本就带 `?versionId=`，
  返回副本的 `ETag` + 版本；`upload_part_copy(source, key, upload_id, n, range)` = `UploadPartCopy`，同样钉住，返回段 ETag。
  源变了 → 412 → `Conflict`；源没了 → `NotFound`。
- **`UploadPartCopy` 带 `Content-Length: 0`**（所有 profile，2026-09-25）：SDK 对没有 body 的请求不发长度（hyper 对空
  body 不加），Ceph RGW（DXN）把 `UploadPartCopy` 当分段上传处理、要求声明长度，缺了回 `411 MissingContentLength` ——
  DXN 上每个 > 64 MiB 的原生拷贝都因此失败。`copy_part` 用 `customize().mutate_request` 加上它（RFC 9110：PUT 应带长度；
  MinIO 实测接受）。`CopyObject` 不加：RGW 另走一条路，缺长度照样接受（DXN 实测）。wire 测试
  `role_protocol::native::tests::copies_pin_the_source_and_copy_no_metadata` 断言两者。legacy `multipart_rename` 的
  `UploadPartCopy` 没改（在 DXN 上大概率同样 411，未验证）。
- **`CopyObject` 回复不带版本**（DXN：版本化桶里也不回 `x-amz-version-id`，2026-09-25 实测）：`send_copy` 在回复
  **没有版本头**（`reported_version` 为 `None`，不是 `"null"`）且连接时桶的版本状态是 `Enabled`
  （`S3Protocol::versioning_enabled`，即 `S3Storage.is_bucket_versioned`）时，按 C17 结算结果不明 Complete 的办法认领：
  ListObjectVersions 该 key，**最新**条目是版本且大小 + 副本回复的 ETag 对上 → 报它的版本；认领不到（列不出、不是最新）
  → 告警、不报版本（拷贝本身成功）。只在这种情况下多一个 LIST：AWS / MinIO 回版本头、暂停版本回 `"null"`、未开版本的桶
  都不多发请求。残余风险同 C17：另一个写者紧接着写入相同内容会被认领（调用约定排除）；存储的列表落后于拷贝时，
  同一源较早拷到这个 key 的相同版本会被认领（字节相同，只是版本可能旧）。
- **≤ 64 MiB**：single stage 记下源（不缓冲字节）；prepare 只看指针（有遗留 → 删指针 + abort key 上的 upload，
  `Restarted{..}`），发布 = 一次 `CopyObject` 到最终 key，再设待设 tags。提交前的明确拒绝 → 最终 key 未变；其他失败 →
  HEAD：大小与**源的** ETag 都对算已拷，否则报已变。MinIO（与 AWS 一样）给分段上传出来的源的副本一个新的普通 MD5 ETag，
  这种源的 CopyObject 丢回复只能报已变。fill 就把这一次 CopyObject 的字节与 1 个请求计入 native 统计（引擎只从 fill
  取），所以发布时被拒也照样报这些数。
- **> 64 MiB**：与流式同一套 discovery + `FinalUpload`（从 `at_destination.rs` 移到 `staged/final_upload.rs`）：分段
  `max(64 MiB, ceil(size/10000))`（≤ 5 GiB），`UploadPartCopy` 并发 `min(6, InflightLimits.operations)`（`NativeEndpoint::copy_into_stage` 带下来），发布时 Complete —— 栅栏、C15c/C17 的失败 / 结果不明规则、
  tags、按版本删指针、`AfterPublish` 读回，全复用流式代码。`Checkpointed` 且已知大小超过间隔 → upload 一开始就写指针
  （C16 规则）；本次 fill 拷完的分段达到间隔时 recovery 打开；`AtomicReplace` 不写指针、从不可恢复。
  **不做复合 ETag 检查**（`md5_etags = false`）：段 ETag 是存储回的，不是本地算的 MD5，证明不了是 MD5（SSE-KMS），
  不符会把已提交的拷贝报成 `Corruption`；内容靠读回校验，复合值仍用于认领结果不明的 Complete（MinIO 实测：各段 ETag
  是段 MD5，Complete 的 ETag 是它们的复合值）。
- **取消**：不再发新段，等在途的段拷完（每个拷完的段下次都算）；**某段失败**：立即失败。失败保留 stage（discard =
  按版本删指针再 abort）。`persisted_bytes = write_offset + 本次拷贝字节`；分段路径的 `native_bytes` 只算本次新拷的。
- **跨路径的分段大小**：续传一律沿用指针里记的分段大小，不管哪条路径写的 —— 原生拷贝以 8 MiB 的 `UploadPartCopy`
  续一个流式 upload（请求多，不占客户端内存）；流式以 64 MiB 分段续一个原生 upload —— 在途分段数按「本对象流式规划的 4 段所占字节」
  折算（`FinalUpload::streamed_inflight`，至少 1），64 MiB 时 1 段在途 + 1 段在填，共 128 MiB（不限时是 5 × 64 = 320 MiB）。
  这些写端分段缓冲**不在** `InflightLimits.bytes` 之内（其 rustdoc 已写明）。不因分段大小重来。
  C15c 的遗留问题（原生拷贝不清同 key 上流式留下的指针 / upload）已解决：原生 prepare 能续就续，其他按决策表清
  （`OtherTransfer` / `BindingChanged` / 原子拷贝 `Requested` …）。
- `EffectiveRecovery` 报流式路径对同一 stage 会报的值（C22 后的 follow-up；C18 最初一律报 `NotApplicableNative`）：
  fill 打开 recovery 后 `Checkpointed`，`Checkpointed` 但分段没到间隔（不写指针）`SkippedBelowCheckpointThreshold`，
  `AtomicReplace` → `Disabled`；只有 `Checkpointed` 的单次 `CopyObject`（≤ 64 MiB，没有恢复状态）报 `NotApplicableNative`
  （fill 在 `NativeStageEvidence::single_operation` 里说明）。续传看 `prepare` / `reused_bytes`。`CopyObject` 带
  `x-amz-metadata-directive: REPLACE` 与 `x-amz-tagging-directive: REPLACE`、不带元数据与 tags：源的用户元数据、内容类型、
  tags 都不带过去，与 `UploadPartCopy`、流式路径一致（目的端元数据不随大小变）；元数据计划要的 tags 之后照常设置。
  MinIO 实测：REPLACE 不带 Content-Type 被接受，副本 `binary/octet-stream`、无 `x-amz-meta-*`、无 tags —— 与 200 MiB
  `UploadPartCopy` 副本和流式上传的对象相同。
- `MemoryS3`（`memory_native.rs`）：`copy_from` / `upload_part_copy` 按 ETag + 版本钉源（变了 → `Conflict`）、
  `native_copies` / `part_copies` 计数、`part_copies_peak`（同时在途的 `UploadPartCopy` 峰值）、`native_failure`（CopyObject 失败不拷）、`copy_commits_then_fails`、
  `part_failure(_waits)` 同样作用于 `UploadPartCopy`、`cancel_after_part_copy(n, token)`（第 n 段拷完后取消）。
  测试连接 `connect_native_at_destination(protocol, identity, interval, (single_max, part))`。
- 测试：`transfer::s3_native_final_tests`（小对象一次 CopyObject、大对象 UploadPartCopy + 一个指针 / 原子无指针、
  切断后续传只重拷第 3–5 段、取消时在途段都保留、原生续流式 upload（8 MiB 段）、流式续原生 upload、清理不能续的遗留、
  发布前取消不动最终 key 且 discard 清干净、并发不超过 operations 上限、源换了内容时不续流式遗留而
  `Restarted{BindingChanged}`）、`role_protocol::native::tests`（请求头：`copy-source-if-match`、`?versionId=`、两个
  REPLACE；416 → `Conflict`）、`transfer::s3_versioning_tests`（原生两种形态各 1 版本、按 id v1→v2 按序 +
  切断续传 1 版本、Complete 结果不明认领版本）、`transfer::s3_native_tests`（CopyObject 明确拒绝未变 / 无回复已变）、
  `staged::native_final_tests`（含 Complete 不按段复合值检查）、`memory_tests`。
- 真机（MinIO VM 102，前缀 `data-mover-c18-<ts>`，跑完 0 对象 0 upload）：4 / 64 / 65 / 200 MiB 原生拷贝 native 请求
  1 / 1 / 2 / 4，`--compare` BLAKE3 相等、无遗留；读回关时 200 MiB 0.31 s、1 GiB（16 段）1.14 s —— 切断要在一秒内：
  1 GiB 300 ms 取消（在途 6 段拷完，402653184）→ 1 指针 + 1 upload → `Resumed { 402653184 }`；500 / 700 ms →
  `Resumed { 805306368 }`；SIGKILL 0.8 s → `Resumed { 402653184 }`（0.6 s：有 upload 与指针但无段，`Resumed { 0 }`；
  0.45 s：还没 prepare，`Fresh`），都相等、无遗留。`staged_matrix.sh` 原生 k1 / m200 `equal=yes stage_objects=0
  key_uploads=0`；`versioning_matrix.sh` 原生 4 / 200 MiB 各 1 版本 0 标记 0 artifact，按 id 原生拷 v1、v2（各 100 MiB）
  两个版本按序、各与源版本相同，临时桶已删。
- 真机 DXN：见「DXN（Ceph RGW Octopus）实测行为」。

### 删除 temp-key 路径（ADR-0006 C19，破坏性）

- C15c 起真连接走不到、C18 起原生拷贝也走不到的 temp-key 路径整体删除：`staged/recovery.rs`（store 时代的
  recovery identity / `recover` / `resumable_parts`）、`staged/publication.rs`（从 temp key `CopyObject` 到最终 key、
  C17 的 `delete_temp_key`）、temp-key 原生拷贝（`staged/native.rs` 只剩「填最终 key 上的 stage」）、`StageState`
  stage 表、`S3StagedDestination::with_recovery_at_destination` 与字段、`S3Protocol::copy_object` / `native_copy`
  （连同 `S3NativeCopyFailure` / `S3NativeCopyResult` / `S3_NATIVE_COPY_SINGLE_MAX` 与 `role_protocol/native.rs` 的
  旧整段 / 分段拷贝）。single stage 的 token 改用最终 key（不再借 temp key 名）。
- `StagedDestination::prepare` / `recovery_identity` / `recover` 当时对 S3 返回 `Unsupported`（C21 已把它们连同
  `prepare_ephemeral` / `handoff_recovery` 从 trait 删除）；不是在目的端 prepare、也不是 `Direct` 的 stage（例如 store
  时代的 temp-key stage）`write` / `publish` / `discard` 等一律 `Conflict`（永久），什么都不碰。`verification_point`
  恒为 `AfterPublish`。破坏性：直接调这些角色方法的调用方要改用 `prepare_at_destination`。
- legacy 列举对 `.data-mover-*` 的过滤（C3）**保留**：它也隐藏 `.upload` 指针与其他后端的 artifact。
- **升级遗留（手工清理）**：C15c 之前在途的传输在存储根下留下的 `.data-mover-stage/<binding hex>/<路径 hash hex>`
  对象（以及旧版本在 AWS 上留下的 `<temp>.claim`）和这些 key 上的未完成 upload，新代码不认、不删、列举也看不到。
  确认没有 C15c 之前的进程还在跑之后，用 S3 原生工具删（`<root>` = 存储 URL 的前缀，没有前缀就去掉 `<root>/`）：
  - 对象：`aws s3 rm --recursive s3://<bucket>/<root>/.data-mover-stage/`（MinIO：`mc rm --recursive --force
    <alias>/<bucket>/<root>/.data-mover-stage/`）。
  - 版本化桶：普通删除只加删除标记，整份对象还在；`aws s3api list-object-versions --bucket <bucket> --prefix
    <root>/.data-mover-stage/` 列出每个版本与删除标记，逐个 `aws s3api delete-object --bucket <bucket> --key <Key>
    --version-id <VersionId>`（MinIO：`mc rm --recursive --versions --force …`）。Object Lock 保护的版本要等保留期过。
  - 未完成的 upload：AWS / Ceph / StorageGRID 按前缀列 —— `aws s3api list-multipart-uploads --bucket <bucket>
    --prefix <root>/.data-mover-stage/`，逐个 `abort-multipart-upload --key <Key> --upload-id <UploadId>`；或给桶配
    `AbortIncompleteMultipartUpload` 生命周期规则。MinIO 2023 只按精确 key 列 upload，按前缀找不到：靠服务端
    `api.stale_uploads_expiry`（默认 24 h）回收。
- 测试：删 `recovery_tests.rs`、`staged/manifest_tests.rs`、`staged/native_tests.rs`、`publication::tests`（只测旧路径；
  缺口 / 连续前缀、续传、discard 失败、原生失败保留 stage 已由 `at_destination_tests`、`native_final_tests`、
  `transfer::s3_*` 覆盖；旧 manifest 的空分段用例移到 `upload_discovery::tests::an_empty_part_counts_only_as_an_empty_source`）与 `role_protocol` 的两个 store 时代实验室 ignored 测试；`role_tests` / `single_tests` /
  `sizing_tests` 改走 `prepare_at_destination`（重连续传、较大分段跨连接续传、对齐上传不补空段），新增
  `store_era_entry_points_are_unsupported`、`a_temp_key_stage_is_refused`。`staged_matrix.sh` 的 `stage_objects=`
  保留，守住「再也不写 temp key」。

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
     `ListObjectVersions` 的桶（版本 `Enabled`，或 HCP）；未开 / 暂停版本的桶走 `ListObjectsV2`，行为不变。
     判断在 `version_listing::versions_with_latest`，测试 `s3::versioned_walk_tests`（离线构造 `S3Storage`，不带标签就不发请求）。
     **对调用方是契约变化**：按 `get_is_latest()` 拷贝的调用方必须同时看 `get_is_delete_marker()`；HCP 永远走
     `ListObjectVersions`，也受影响。真机（MinIO VM 102，2026-09-25，临时桶 `data-mover-fu-<ts>`（版本化）与
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
- 删除标记没有内容：`Id(标记)` 的带版本 HEAD 回 405 → 条目 `NotFound`。data-mover 从不往最终 key 写删除标记；
  要在目的端重放（在相邻两个版本之间对目的 key 发一次普通 DeleteObject）由调用方决定。
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

### role-based 遍历与版本模式（ADR-0006 C22）

- **形态**：S3 出借 `Namespace`（`src/storage/backends/s3/namespace.rs`），由通用的深度优先
  `StorageTraversalSource` 驱动；不做扁平 `TraversalSource` —— 块顺序（目录的全部子项先于任何孙项，
  `TraversalOrder::Blocks`）下，扁平 `ListObjectsV2` 要把根下的全部 key 攒在内存里才能交出根的块。
- **一个目录 = 一个前缀**：`List(p)` = `ListObjectsV2(prefix = "p/", delimiter = "/")`；All 模式
  `list_versions(p)` = `ListObjectVersions` 同参数。**一个目录的所有页收齐再建块**（`listing.rs`），所以跨页的
  key 天然是整的，不需要 C20 的“留下一页最大 key”；续页 token / marker **原样回传**，只拿来判“绕回来了”（本目录
  跟过的所有 token 记在一个集合里，任何一个再出现 → **该目录**的 `Protocol` / Permanent 条目失败、块 `Failed`，遍历继续兄弟，
  防止 A、B、A… 死循环；之前是会话级失败、一个目录就结束整场遍历，P7 评审现存问题 3 修复），从不当 key
  比较或去前缀。代价：每个目录至少一次请求、整个目录在内存里（P7 流式列举之前各后端一样）。
- **翻页上限**（`paging.rs`）：一个目录最多 500 万条（对象 + 版本 + 标记 + 前缀）→ 超了该目录 `Capacity` 失败；连续
  16 页空页却还说有下一页 → 该目录 `Protocol` 失败；都是块 `Failed`，不会悄悄截断。`IsTruncated` 缺失但有 token
  → 继续翻（同 legacy）；没有 VersionId 的条目当 `"null"`；跨页重复的对象 / 版本只留第一次。`Stat` 的前缀探测也按
  同样上限翻过空页，不把“第一页空但有下一页”当成不存在。
- **取消即停**：S3 的列举不持有句柄，生产者用 `PageSink::deliver_abortable`：`ListRequest.cancel` 取消或流被 drop
  时进行中的列举直接丢弃，不在后台继续翻页；CIFS 这类持有目录句柄的列举仍跑完再关句柄（P7 C1 删了
  `listings_are_abortable()`）。
- **不存在的前缀**：非根路径下一条都没列到（没有对象、版本、标记、前缀，也没有自己的 `p/` 标记）→ `NotFound`
  条目失败；遍历根拼错时根的列举是 `Failed`、summary 不 exhaustive，不会被当成“源端为空”。存储根永远存在。
- **子项顺序**：S3 key 序，公共前缀带尾 `/` 比较（`a` < `a!` < `a-0` < `a.txt` < `a/`）；同名的对象 `a` 与目录
  `a` 并存。遍历块内保持这个列举序（P7 C4 删了兄弟排序与 `rank`）：同一 key 的版本连续、从旧到新；
  只有前缀 `a` 被下钻，下钻按子目录名字节序。
- **异常 key**：目录自己的零字节标记对象 `p/` 跳过；有内容的 `p/`（All 模式下它**任何一个旧版本**有内容也算，
  所以 Current 干净的目录在 All 下可能是 `Partial`）、空段 / `.` / `..` 段（`a//`、根下的 `/x`、`./x`、`a/./b`、
  `a/..`）是逐子项失败（报在服务器拼写的路径上，列举 `Partial`），不会被并进别的路径 —— 目的端的路径处理会把
  `a/./b` 折成 `a/b`，两个对象写到同一个文件；`List("a/")` / `"a//b"` / `"a/."` 这类路径 `InvalidInput`。不理 delimiter 的存储
  平铺返回的深层 key 在客户端收拢成一级前缀（去重）。`.data-mover-*` 两种模式都隐藏（key 或任意一段前缀）。
- **Current**：只有当前对象（最新是删除标记的 key 不出现）；身份 `PathScoped(路径, ETag)`（`ListObjectsV2` 不带
  versionId；Q28 之前只有 ETag）；时间 = 列举的 `LastModified`（毫秒精度，HEAD 只有秒），`modified` 过滤器不发 HEAD；目录无时间。
- **All**：每个版本与删除标记各一条，同一 key 连续、从旧到新、latest 最后（`history.rs`）：按**服务端文档顺序**倒过来
  （见下「版本历史顺序」，Q29 之前是按 `LastModified` 交错两条序列，并发写入时排错）；校验不过的 key 按时间推断，
  `EntryVersion::order() == HistoryOrder::Inferred`（例子打印 ` order=inferred`）。删除标记是 `File`、`size` 为 `None`：DSL 的 `size`
  条件对它是 `LazyMatch`（放行），要排除标记按 `version().is_delete_marker()` 在调用方过滤。这是**有意的规则**
  （用户决定 2026-09-25）：`size` 条件不滤删除标记，所以按大小筛的历史迁移仍带上大文件的删除记录 —— 标记不带数据，
  目的端该 key 上没有对象时，重放一个从没拷过的 key 的删除不改任何数据（有对象时重放会把它藏到新标记后面，是否重放
  由调用方判断）；表达式里的其他条件（`name` / `path` …）照常作用于标记。
  `ObservedEntry::version()` = `EntryVersion { id（"null" → None）, latest, 删除标记, order }`；`source_version()`：真版本
  `Id(id)`、latest 的 `"null"` → `Current`、非 latest 的 `"null"` → `Id("null")`、删除标记 → `None`（标记无 size，
  也不 HEAD）。身份 `VersionScoped(id)`（与带版本 describe 一致）。可选元数据按该版本取（`observe_copy_bound_version`，
  标签用它的 versionId）；前缀与删除标记不问元数据角色。快照：带版本的条目 v6，其余仍是 v5 原字节。
- **专家传输（terrasync）**：源端半程用 describe 复核观察；版本化桶上 Current 观察是 `PathScoped(路径, ETag)`、describe 钉住
  `VersionScoped(id)`，`ReadSource::observation_matches` 在 describe 到的路径与版本 ETag 拼出的身份等于列举的身份时认作同一对象，
  对象被替换则仍报 "source differs"；offer / evidence 带观察的身份键（目的端半程比对它），读仍钉在 describe 到的版本。
  All 模式里只有 latest 版本能走专家传输：旧版本报 "older version"（用 `with_source_version` 拷），删除标记报
  "delete marker"。
- **拒绝**：非 S3 源请求 `All` → 会话在任何 I/O 前以 `Session(Unsupported)` 结束、零条目（不会退化成当前对象）。
  S3 的变更动词 `Unsupported`；`delete_tree` / `create_directory_all` / `ndx_walk` 在 I/O 前拒绝 S3（同以前）。
- **例子与真机**：`S3_LISTING_URL=… cargo run --example storage_role_operations -- --backend s3 traverse
  --versions all [--order blocks]`（每条版本打印 `version= latest= marker= size=`）；
  `.claude/skills/e2e-s3/scripts/traversal_versions.sh`（临时桶，见 SKILL 第 8 步）。MinIO VM 102 2026-09-25 通过：
  `k` = 2 B、3 B、标记、4 B，`gone` = 版本 + latest 标记，999 个 key 后的 `p/z` 跨页仍 1、2、3 B，暂停版本写的 `n` =
  `version=null latest=1`，无 artifact；All 1015 条、Current 1008 条；`name-bytes` 并发 1/4/32 输出相同；桶已删。
- legacy `walkdir` / `walkdir_2` 不受 C22 影响；C20 所述「最新为标记的 key 不列」的限制已由 D4 follow-up 去掉（见上
  「历史版本迁移」第 1 步）。

**MinIO 2023 的其他实测行为**：`ListMultipartUploads` 只按**精确 key** 返回（按前缀或整桶都是 0）—— 孤儿上传
无法按前缀发现，只能靠 key 精确查询或服务端 `stale_uploads_expiry` 回收；Content-MD5 不符 → 400 BadDigest
（role 协议把 `BadDigest` 映射成条目 `Corruption` / Transient：传输中损坏，重发即可；`InvalidDigest` —— 摘要头本身
格式错 —— 映射成条目 `InvalidInput` / Permanent；C14a）；
Complete 成功后再 Complete → 404 NoSuchUpload；`If-Match` 对分段 ETag（`…-1`）有效；分段号上限 10000；
单 PUT 上限 5 GiB；版本控制下最后一个 Complete 成为当前版本，`GET ?versionId=` 读到指定版本；
`ListMultipartUploads` 列出的 upload id 是签发的 base64url(`<deployment>.<uuid>`) 里的裸 uuid（C15c）。

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
- `S3ListedVersion.position` = 页内文档位置；`listing.rs::list_all` 与 legacy `VersionPages::positioned` 把后页的位置接在
  前页之后（整个目录列举一条序列）。
- `backends/s3/history.rs::key_histories`（两条路径共用）逐 key：**全部有位置、在整个列举里连续（跨页也算；legacy 只在每批已完整的 key 内查，被页末扣留的 key 隔开或释放后再出现的 key 查不出）、恰一条
  `IsLatest` 且排最前** → 按位置倒序（旧→新），`HistoryOrder::Server`，完全不用时间；否则 `HistoryOrder::Inferred`，
  latest 都排最后、同毫秒可能排错，分两种：该 key 有条目**没有位置**（读不到原始顺序，存储本身可能没错）→ 修复前的
  合并（版本、标记各自保持列举顺序倒过来，`LastModified` 只决定两条怎么交错，同毫秒版本在前）；有位置但**校验不过**
  （顺序不对的存储）→ 列举顺序一概不信，按 `LastModified` 稳定排序（各列表列举顺序倒过来只作平手序）。有 key 推断时
  每次列举打一条 `warn`（`history::warn_inferred`），不会悄悄退化。跨页重复列出的同一条（同 key、versionId、种类）只留
  第一次。校验只看得到进来的条目：先被滤掉的（artifact、目录自己的标记、收拢进前缀的深层 key）或没有位置的条目夹在某
  key 中间时发现不了它被拆开 —— 只影响本来就不合规的存储。
- 标记：role-based `EntryVersion::order()`（快照 v6 flag bit 8 = `Server`；没有这一位的老 v6 快照读成 `Inferred` —— 它们
  确实是按时间合并的；老版本读到这一位报 `Malformed`），
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
  （前缀下并发 1 轮，15 条 / 3 标记）、`seq`（顺序写 20 次 PUT / DELETE，6 标记）。role `traverse --versions all` 与
  legacy `walkdir` 三个 key 都**逐条等于服务端原始 XML 倒序**、全部 `Server`；`walkdir_2` 等于其中的版本；`seq` 等于
  写入时记录的 versionId 顺序。修复前的按时间合并在 `race` 第 5 条、`d/race` 第 4 条起就与服务端顺序不同。并发写入
  没有客户端可见的“真实顺序”（客户端完成时间受响应延迟影响，21 对相差 ≥ 50 ms 的完成时间与列举相反，但服务端
  `LastModified` 在这 21 对上都与列举一致），以服务端提交顺序为准。

### DXN（Ceph RGW Octopus）实测行为（2026-09-25）

`versioning_matrix.sh` 在 DXN 临时桶（`BUCKET_PREFIX=data-mover-dxn`，`S3_LOCATION=magnascale:tos_placement`，见 e2e-s3
SKILL.md）上跑出来的差异，以及各自的处理：

| 行为 | MinIO / AWS | DXN | 处理 |
|---|---|---|---|
| `UploadPartCopy` 不带 `Content-Length` | 接受 | `411 MissingContentLength` | 一律带 `Content-Length: 0`（见 C18 节） |
| `CopyObject` 不带 `Content-Length` | 接受 | 接受 | 不改 |
| 版本化桶里 `CopyObject` 的回复 | 带 `x-amz-version-id` | **不带** | 回复没版本且桶开了版本 → 列版本认领（见 C18 节） |
| legal hold 下删指针版本 | 拒（403 / MinIO 400 WORM） | 403 `AccessDenied` | 告警，改发普通 DELETE |
| legal hold 下普通 DELETE（当前版本被扣） | 加删除标记 | **403 `AccessDenied`** | 告警，指针留在原处，传输照常（见 C17 节） |
| 带 Object Lock 建桶 | 同时开版本 | **不开版本**（`ObjectLockEnabled=Enabled`，版本 off） | 矩阵脚本检测到后自己开版本 |
| 重试一个仍在完成中的 `CompleteMultipartUpload` | MinIO 404 `NoSuchUpload` | 500 `InternalError`「This multipart completion is already in progress」 | 会话 `Connectivity` / Transient：不是提交前的拒绝 → `ListParts`；还列得到 → 报已变 + transient（C15c） |
| 完成后 `ListParts` | MinIO 404 `NoSuchUpload` | 404 `NoSuchKey` | 同为 `NotFound` → 按版本结算（C17）；单测 `dxn_answers_to_a_completion_in_progress_and_after_it` |
| `x-amz-server-side-encryption: aws:kms` 不给 key id | （AWS 用默认 key） | 403 `InvalidAccessKeyId` | 只记录：DXN 上用 SSE-KMS 必须给 `x-amz-server-side-encryption-aws-kms-key-id` |

修复后 DXN 真机（临时桶 `data-mover-dxn-1790336656` 与 `-lock-`，跑完已删，DXN 上只剩 `data-mover-test`）：矩阵全部通过 ——
流式 / Direct / 原生 4 MiB 与 200 MiB 各 1 版本、0 标记、0 artifact、`destination_version` = 最新；按 id 拷 v1、v2（流式与
原生）按序、各与源相同；按 id 3 s 取消后 `Resumed { 8388608 }`、1 版本；续传矩阵 cancel `Resumed { 75497472 }`、SIGKILL
`Resumed { 33554432 }`，BLAKE3 相等；锁行 `result=ok` 带版本、2 条告警、0 标记、1 个 artifact（被扣住的指针版本）；暂停版本
`destination_version=null`。

其他 DXN 限制（UploadPartCopy 源 key 字符、CreateMultipartUpload 丢 tagging、DeleteObjects 要 Content-MD5）见 README
「Known DXN limitations」。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| 404 被当 retry | 已修，必须 → `FileNotFound` |
| Multipart 失败留垃圾 | 必须 abort |
| MinIO 列出的 upload id 与签发的写法不同 | 比较 upload id 用 `upload_pointer::same_upload`，别逐字节比 |
| 自签证书 ECS endpoint 失败 | 用 `s3+https://` (而不是 `https` 显式) |
| Ceph RGW / DXN 对无 body 的 `UploadPartCopy` 回 411 | 显式 `Content-Length: 0`（`role_protocol/native.rs`） |
| DXN 版本化桶的 `CopyObject` 不回版本 | 桶开了版本且回复无版本 → 列版本认领（`native_final::settle_copy_version`） |
| `bucket.host` 解析错误 (path-style vs virtual-hosted) | 检查 endpoint 是否支持 virtual-hosted |
| URL 中的 `:` 在 secret 里被切错 | secret 必须 percent-encode |
| 把 MinIO 的 `NextKeyMarker`（`p/z[minio_cache:v2,return:]`）当 key 用 | 续页 token / marker 只原样回传（C20 / C22） |
| 同一目录里对象 `a` 与前缀 `a/` 同名 | 按路径做键的消费方会撞；块内按 S3 列举序（对象 `a` 在 `a/` 之前，C22） |
| 按 `LastModified` 排同一 key 的版本与删除标记 | 毫秒精度，并发写入同毫秒；用服务端文档顺序（`version_order.rs` + `history.rs`，Q29） |
| `PathScoped` 身份只放内容事实（ETag） | 同内容不同 key 撞身份键；稳定字节必须含路径（Q28） |
| Region 推断 | endpoint host 推断，必要时显式设 `AWS_REGION` |

## 测试

- `examples/s3_walkdir.rs` — bucket 列表。
- skill：`.claude/skills/e2e-s3/` (需要 `.env` 含 endpoint/bucket/ak/sk)。
- skill 内置一个"读不存在的 key 应返回 FileNotFound"的回归测试。
- `DM-STORAGEGRID-REQUEST-CONTRACT`：
  `cargo test s3::storagegrid::tests --locked`，在 capturing Smithy connector seam
  验证只有 StorageGRID 去 `x-id`、所有 profile 的 DeleteObjects 都带签名 MD5；PR 与 release workflow 均独立执行。
- `examples/s3_listing.rs` — walkdir / walkdir_2 / `--sub` / `--delete-dir`，逐条打印路径与 versionId / `latest=` /
  `marker=` / `count=`（真机验遍历、版本与删除范围）；不给 URL 参数时读 `S3_LISTING_URL`（凭据不上命令行）。

## 改 S3 时

1. 读本 doc + 当前 `src/s3.rs`。
2. 改错误映射必须同步 [error-taxonomy.md](error-taxonomy.md)。
3. 改 multipart 阈值 / 并发，跑性能测试 (skill 暂未含)。
4. 调 `backend-specialist` agent 传 `s3`。
5. 验证：`make e2e-s3` (需 .env)，否则至少 `make clippy && make test`。
