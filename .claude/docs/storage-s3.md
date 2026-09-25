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

对象在 describe 之后被替换（身份判定统一走 `source.rs` 的 `object_identity`：真实 versionId 标识版本，
`"null"` / 空 / 无 versionId 以 ETag 标识）：`read`、native 的 `bind_source`、`observe_bound` 三处都报
`Conflict` / Permanent（不是可重试的 Protocol / Unknown），按条目失败。

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

### 小对象：一次 PutObject 到最终 key（ADR-0006 C14b）

- 阈值 T = `S3BackendConfig.single_put_threshold`（`None` = 8 MiB，范围 [5 MiB, 5 GiB]，越界在 connect
  时、联网前报 S3 的 `BackendConnectError`「invalid configuration」；storage 层按架构守卫不能引用
  `crate::error`，所以不是 `StorageError::ConfigError`）。已知大小 ≤ T（含 0 B）的对象：prepare 不开 upload、不建 temp key；stage
  状态（缓冲、待设 tags、写入事实）放在 `PreparedStage::backend_state`（`staged/single.rs`），不进适配器的
  stage 表 —— 丢弃的 stage 不会滞留缓冲。无续传（`disable_recovery`），引擎因此不登记、并清掉本地恢复记录。
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

- 开关 `recovery_at_destination()`（`S3StagedDestination` 字段；C15b 时真连接恒为 `false`，C15c 起恒为 `true`，
  见下节；`connect_at_destination` 现在只是把自动间隔换小的测试连接）。**开关开时**才声明自动 checkpoint 间隔
  64 MiB（`automatic_checkpoint_interval_bytes`）：关着时 store 路径的规划（Checkpointed 从头登记）不变。
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
  ≤ 64 MiB 的 Checkpointed 对象从不写指针）。`with_recovery_at_destination(false)`（仅测试）把旧 temp-key 路径 +
  store 放回来，旧路径 C19 删。
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
  `tracing::warn!`，改发普通 DeleteObject 用删除标记盖住它；传输照常成功并报版本，下次 prepare 看不到指针。标记也发不
  出去才算失败。
- **最终 key 从不发普通 DELETE**：审计过所有路径，只有 artifact（指针、temp key）会被普通删除。`MemoryS3` 记录每个
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
- **原生 S3→S3 的 temp key 路径**（C18 起只在开关关掉时可达，C19 删）：temp key 按 key 名只属于这个 stage，删除改为 `publication::delete_temp_key`
  —— 列出它的每个条目（版本、删除标记、`"null"` 版本都算）逐个按 id 删（版本化桶里普通删除会把整份对象留在删除标记
  下面；`[null(最新), v1]` 这种先开版本后暂停的情况两个都删）；列表为空 → 什么都不发（不加空删除标记）；列不出
  （告警）/ 按 id 被拒（锁、不支持；`"null"` 删不掉一律）→ 普通删除；版本已不在算成功；其他失败照常报错。`CopyObject` 成功后 HEAD 最终 key 取当前版本作 `destination_version`（对账路径不认领）。每次原生
  发布多一个 LIST + 一个 HEAD（C18 的最终 key 路径没有这两个请求：版本取自 CopyObject / Complete 的回复）。
- 未解决：C17 之前留下的指针版本 + 标记都在标记下面，不清理。单 PUT 丢回复的对账（C14b）仍认领 HEAD 的版本，内容相同
  的旧对象也会被认领（后续可比照分段改成列版本）。
- `MemoryS3` 版本化模式（`memory_versions.rs`）：`set_versioning(Enabled | Suspended)`、每 key 有序的版本与标记、
  `delete_version` / `list_versions`、`lock_versions(key)`（按版本删被拒、普通删允许）、`version_deletes()`、
  `keys_with_versions()`、`written_after_lost_completion`（丢回复的 Complete 之后另一个写者覆盖）。
- 测试：`transfer::s3_versioning_tests`（引擎级：各写法报版本、原生拷贝报版本且不留 temp 版本、中断续传只加一个版本、discard 后桶不变、按 id 拷
  v1→v2 按序、按 id 中断续传一个版本且 reused > 0、暂停版本、指针被锁、Complete 结果不明认领版本）、
  `staged::at_destination::tests::versioning`（丢回复的指针 PUT、重复存了两份的指针 PUT（发布与 discovery 都扫掉）、续传
  越过重复指针、暂停版本下续传不删自己的 `"null"` 指针、discard、discovery 按读到的版本删、续传删被替换的版本、
  锁住的指针被标记盖住、续传删不掉被替换的锁住版本仍被标记盖住、Complete 后被覆盖 → Conflict）、`direct::tests::an_unfinished_completion_over_an_identical_object_claims_no_version`、
  `completion::tests`、`role_protocol::versions::tests`、`publication::tests`（temp key 的 `"null"` 版本、空 key 不加标记）。
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
  **开关开时原生路径碰不到 temp key**；temp-key 代码（与它每次发布多出的 LIST + HEAD）只在
  `with_recovery_at_destination(false)` 的测试里可达，C19 删。
- 协议新增：`copy_from(source, to)` = `CopyObject`，`x-amz-copy-source-if-match` 钉 ETag、有版本就带 `?versionId=`，
  返回副本的 `ETag` + 版本；`upload_part_copy(source, key, upload_id, n, range)` = `UploadPartCopy`，同样钉住，返回段 ETag。
  源变了 → 412 → `Conflict`；源没了 → `NotFound`。
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
- `EffectiveRecovery` 仍报 `NotApplicableNative`；续传看 `prepare` / `reused_bytes`。`CopyObject` 带
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

### legacy 列举隐藏传输 artifact（ADR-0006 C3）

- `walkdir` / `walkdir_2`（含版本化桶的版本与删除标记）跳过相对存储根的任意一段以 `.data-mover-` 开头的
  key 与公共前缀（`storage::artifacts::is_artifact_path`），命中打 `trace!`。存储根本身在 artifact 里时照常
  列举；从根以下的 artifact 内开始遍历（`sub_path = ".data-mover-stage"`）则为空。
- `delete_dir_all_with_progress` **不**过滤：删目录会连其中的 artifact 一起删，并为它们发 `DeleteEvent`
  （路径不曾出现在列举里，按删除数与列举数对账会差出这些）。今天 S3 stage 集中在根下 `.data-mover-stage/`，
  删子目录删不到它的孤儿；同目录的 `.upload` 指针（C15b）会随目录一起删。
- 代价：用户自己命名为 `.data-mover-*` 的对象从 S3 源端列举中消失；孤儿 `.data-mover-stage/` 只能用 S3
  原生工具（或 `resume_matrix.sh` 的清理）看到。
- 真机（`examples/s3_listing`，2026-09-24）：MinIO / DXN / StorageGRID 非版本化，MinIO / DXN 临时版本化桶，
  artifact 全部隐藏，普通对象、多版本与删除标记行为不变。

**MinIO 2023 的其他实测行为**：`ListMultipartUploads` 只按**精确 key** 返回（按前缀或整桶都是 0）—— 孤儿上传
无法按前缀发现，只能靠 key 精确查询或服务端 `stale_uploads_expiry` 回收；Content-MD5 不符 → 400 BadDigest
（role 协议把 `BadDigest` 映射成条目 `Corruption` / Transient：传输中损坏，重发即可；`InvalidDigest` —— 摘要头本身
格式错 —— 映射成条目 `InvalidInput` / Permanent；C14a）；
Complete 成功后再 Complete → 404 NoSuchUpload；`If-Match` 对分段 ETag（`…-1`）有效；分段号上限 10000；
单 PUT 上限 5 GiB；版本控制下最后一个 Complete 成为当前版本，`GET ?versionId=` 读到指定版本；
`ListMultipartUploads` 列出的 upload id 是签发的 base64url(`<deployment>.<uuid>`) 里的裸 uuid（C15c）。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| 404 被当 retry | 已修，必须 → `FileNotFound` |
| Multipart 失败留垃圾 | 必须 abort |
| MinIO 列出的 upload id 与签发的写法不同 | 比较 upload id 用 `upload_pointer::same_upload`，别逐字节比 |
| 自签证书 ECS endpoint 失败 | 用 `s3+https://` (而不是 `https` 显式) |
| `bucket.host` 解析错误 (path-style vs virtual-hosted) | 检查 endpoint 是否支持 virtual-hosted |
| URL 中的 `:` 在 secret 里被切错 | secret 必须 percent-encode |
| Region 推断 | endpoint host 推断，必要时显式设 `AWS_REGION` |

## 测试

- `examples/s3_walkdir.rs` — bucket 列表。
- skill：`.claude/skills/e2e-s3/` (需要 `.env` 含 endpoint/bucket/ak/sk)。
- skill 内置一个"读不存在的 key 应返回 FileNotFound"的回归测试。
- `DM-STORAGEGRID-REQUEST-CONTRACT`：
  `cargo test s3::storagegrid::tests --locked`，在 capturing Smithy connector seam
  验证只有 StorageGRID 去 `x-id`、所有 profile 的 DeleteObjects 都带签名 MD5；PR 与 release workflow 均独立执行。
- `examples/s3_listing.rs` — walkdir / walkdir_2 / `--sub` / `--delete-dir`，逐条打印路径（真机验遍历与删除范围）。

## 改 S3 时

1. 读本 doc + 当前 `src/s3.rs`。
2. 改错误映射必须同步 [error-taxonomy.md](error-taxonomy.md)。
3. 改 multipart 阈值 / 并发，跑性能测试 (skill 暂未含)。
4. 调 `backend-specialist` agent 传 `s3`。
5. 验证：`make e2e-s3` (需 .env)，否则至少 `make clippy && make test`。
