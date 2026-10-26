# S3 Backend

## 底层依赖

- **crate**：`aws-sdk-s3` (1.129.0，crates.io)。
- HTTPS：SDK 默认客户端（`rustls` + 系统根证书，认 `SSL_CERT_FILE` / `SSL_CERT_DIR`）；只有 URL 写
  `tls_verify=false` 才换成 `hyper-rustls` + 不校验证书的 verifier（显式 ring provider）。
- IAM credential 链路：标准 AWS 行为 (env / config file / IMDS)。

## URL 形式（P7 §14.2，唯一解析器 `src/storage/backends/s3/client/url_options.rs`）

```
s3://ak:sk@bucket.host[:port][/prefix][?参数]          HTTP
s3+https://ak:sk@bucket.host[:port][/prefix][?参数]    HTTPS，默认校验证书
参数：compat=sg|dxn|ontap   tls_verify=false（只用于 s3+https://）   bucket_type=nas（只与 compat=ontap）
```

示例：

```
s3://AKIAXXX:secretXXX@my-bucket.minio.local:9000/data/
s3+https://AKIAXXX:secretXXX@my-bucket.s3.example.com/exports
s3://AK:SK@data-mover-test.10.131.7.236:10443/p?compat=sg
s3+https://AK:SK@data-mover-test.10.131.7.201:8182/p?compat=dxn&tls_verify=false   # 自签证书
s3://AK:SK@data-mover-test.10.128.61.200/p?compat=ontap
```

- 凭据在**最后一个** `@` 处切开（SK 可含 `/` `+` `=` `?`，不 percent-decode）；查询串从其后第一个 `?` 开始，值按
  form_urlencoded 解码、区分大小写。
- **缺省即默认**：标准 S3、校验证书、对象桶。写出默认值（`compat=standard`、`bucket_type=object`、`tls_verify=true`）、
  未知 / 重复 / 空值参数、`tls_verify` 用在 `s3://` 上、`bucket_type=nas` 不带 `compat=ontap`、无桶端点 URL
  （`list_buckets`）带 `bucket_type` —— 全部 `UrlParseError`（D4：URL 文本能判定的都算）。报错不回显 URL 与凭据。
- **旧协议名已删除**，报错给出新写法：`s3+http://` → `s3://`；`s3+sg://` → `s3://…?compat=sg`；`s3+dxn://` →
  `s3://…?compat=dxn`；`s3+hcp://` → `s3://…`（HCP 支持已删除）；`s3+sg+https://` / `s3+dxn+https://` →
  `s3+https://…?compat=sg|dxn&tls_verify=false`（旧形式本来就不校验证书）。`detect_storage_type` 把任何
  `s3+…://` 判为 S3，所以旧写法走到这条报错而不是被当成本地路径。
- **参数不进端点身份**：`s3://host[:port]/bucket[/prefix]` 与改参数前相同，TransferIdentity / 恢复绑定不变。
- `compat=sg` — StorageGRID：签名前精确删除 `x-id`；不走原生拷贝。
- `compat=dxn` — DXN（Ceph RGW Octopus）：标签不支持；原生拷贝亲和标签 `dxn`。
- **HCP 支持已删除**（用户 2026-09-27）：没有 `compat=hcp`（报错提示按标准 S3 写），不再读 HCP REST 的 `BPM` 注解、
  不再把 HCP 桶一律当作开版本；`src/third_party/hcp` 与直接依赖 reqwest 0.13 一并删除。HCP 命名空间按标准 S3 使用（标签走
  S3 对象标签，版本按 GetBucketVersioning）。
- `compat=ontap` — ONTAP：**不改任何线上请求**（对象桶的怪癖都已通用处理：F4、F6、空末页、CommonPrefix 计入
  max-keys、不回显 Delimiter）；原生拷贝亲和标签 `ontap`（与同端点的标准 URL 之间走流式）；承载 `bucket_type=nas`。
- `bucket_type=nas` — ONTAP NAS 桶 profile（P7 §14.2，N1–N7）。能力开关 `S3BucketCapabilities`
  （`storage/backends/s3/capabilities.rs`，由 `s3/architecture.rs::bucket_capabilities` 按 `bucket_type` 定；`compat` 只改请求、
  不改能力）。N1：连接不再拒绝（桶若回版本状态只告警）；作源端无条件读 —— 不发 `If-Match`，每段响应的 `ETag`、
  `Content-Range` 总长、`Last-Modified` 必须等于描述时的（不等即 `Conflict` / Permanent）；body 读到一半断掉（短于
  Content-Length / Content-Range、读 body 时连接被重置）后 `HEAD` 一次（2026-09-27 协调决定，`broken_body.rs`）：`ETag` / 大小 /
  `Last-Modified` 变了 → `Conflict` / Permanent（被替换，N0：替换提交后服务器切断进行中的 GET），没变 → 会话 `Connectivity` /
  Transient（普通断连，重试），`HEAD` 本身失败 → 按 `HEAD` 自己的分类；读完再 `HEAD` 一次（`role_protocol/unpinned.rs`、
  `source.rs::confirm_unchanged`）。读回（N4）走同一个 `get_range`，规则相同（指针读也走它，但 NAS 桶不续传、
  不读指针，走不到这里）；断开后的 `HEAD` 回 404 是条目 `NotFound`（读完后的确认 `HEAD` 回 404 报 `Conflict`：两者都是条目
  Permanent，有意保留 `HEAD` 自己的类别）；无源端版本、namespace
  `histories=false` / `directory_objects=false`；原生拷贝亲和 `ontap-nas` + 桶名（只与同一 NAS 桶配对）。
  N2：目的端不续传（`resumable_at_destination() = false`、无自动间隔；Checkpointed 报
  `EffectiveRecovery::UnsupportedByDestination`、prepare 按 Restart、不看指针不列 upload）；`final_refusal`（`staged/limits.rs`）
  在 Preflight、目的端任何请求之前拒 > 5 GiB 与含空段 / `.` / `..` 段、段 > 255 字节的 key（条目 `Unsupported` / Permanent），
  S3 的三个 prepare 再查一遍。
  N3：8 MiB–5 GiB 一次**流式单 PUT**（通用原语 `S3Protocol::put_object_streaming`，`role_protocol/streaming_put.rs`：
  `Content-Length` = 源大小、`UNSIGNED-PAYLOAD`、无 `Content-MD5`、强制 `WhenRequired` 校验和（不走 aws-chunked）、不重试、
  关掉 SDK 的超时与卡顿保护；stage 在 `staged/streaming.rs`）。**扣住最后 1 字节到 publish**：write 送出其余字节，
  publish 放出最后 1 字节并等回复 —— 之前任何失败 / 取消 / 丢弃 stage 都中止请求（关连接），最终 key 不变；扣住超过
  300 s（`HOLD_LIMIT`）即放弃，publish 报未变。`Direct` 不扣字节、在 write 里提交。看门狗：通道里的一块在读超时
  （`S3_READ_TIMEOUT`，默认 20 s）内没被取走、或最后 1 字节后回复没来 → `Connectivity` / Transient。边传边算 MD5
  （`spawn_blocking`），回复 `ETag` 是 32 位 hex 且非 KMS / 客户密钥加密时必须等于它，否则 `Corruption` / Transient、
  最终 key 已变。prepare 多一次 HEAD 记下最终 key 原状（`staged/settle.rs`，只在 `ETag` 不是 MD5 的桶上，≤ 8 MiB 的单 PUT
  也用）：结果不明的失败再 HEAD，与原状相同（或仍不存在）报未变，否则已变。NAS 桶上 PUT 回 404 `NoSuchKey`（有文件 `a`
  时写 `a/x`）或 409 `NameCollision`（有 `a/x` 时写 `a`）→ 该条目名字冲突 `Conflict` / Permanent（`put_refusal`）。
  N4：**ETag 括号式读回**（`single.rs::verify_bracketed`，所有 stage 共用）：HEAD 定锚（`ETag` + 大小）、每段响应必须带锚
  `ETag`（替换 → `Conflict` / Permanent）、BLAKE3 与源比 —— 内容或大小不同 → `Corruption` / **Transient**，诊断写明
  “无条件读，写与读之间的替换无法排除”（N0：保留 mtime 的 NFS 改写不改 ETag，真会出现）。不与 PUT 回复比 ETag（NAS 上
  回复是 MD5、HEAD 是不透明值）。结果记进 `TransferOutcome.verification`：`read_back` =
  `EtagBracketed { ConditionalReadsUnsupported }`（对象桶 `Pinned`，读回关 `NotPerformed`），`upload_digest` = `Matched`
  （缓冲 PUT / 分段都带 Content-MD5；流式单 PUT 回复 ETag = 自算 MD5）或 `Unchecked(EtagNotMd5 | NativeCopy | PriorAttempt)`；
  不算失败。
  N5：同一 NAS 桶内的原生拷贝一律**一次 `CopyObject`**（≤ 5 GiB，超过在 Preflight 拒；从不 `UploadPartCopy`，N0：同桶
  CopyObject 是克隆，1 GiB 0.01 s），`x-amz-copy-source-if-match` 照带（NAS 桶认存储的不透明 ETag）；prepare 同样多一次 HEAD，
  回复丢失按原状结算（副本的 ETag 由拷贝时刻的 mtime 派生，不能按源 ETag 认领）；读回的锚必须等于拷贝回复里的 ETag
  （N0：拷贝回复的 ETag = 目标 HEAD 的不透明值；之后设标签不改它，2026-09-27 实测，故设标签后不再 HEAD），
  不等 → `Conflict`「replaced after the copy」。NAS 桶与对象桶之间、两个 NAS 桶之间按 N1 的亲和走流式。
  N6：URL 没写 `bucket_type=nas` 而桶其实是 NAS 桶时照旧失败，但诊断带提示「if this is an ONTAP NAS bucket, add
  ?compat=ontap&bucket_type=nas to the URL」（`role_protocol/hints.rs`）：条件 GET、`CreateMultipartUpload`、`CopyObject` /
  `UploadPartCopy` 回 501（`Unsupported`，任何 compat 都加，写了 `bucket_type=nas` 则不加）；`ListMultipartUploads` /
  `ListParts` 回 403（`PermissionDenied`）只在 `compat=ontap` 下加（别处 403 是普通的策略拒绝）。类别不变。
  **legacy `StorageEnum` 不理会这个 profile**（决定 13，只写文档）：legacy 读不带条件、不核对，写 > 5 MiB 走分段遇 501。
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

URL 必须带 `ak:sk@`（解析器没有凭据就报 `UrlParseError`）；不读环境变量、`~/.aws/credentials`、IMDS。

### TLS

- `s3+https://` **默认校验证书**（P3 起）：系统根证书，认 `SSL_CERT_FILE` / `SSL_CERT_DIR` —— 私有 CA 这样信任。
- 自签端点（DXN 8182、自建 MinIO、ECS 等）：信任其 CA，或 URL 加 `tls_verify=false`（每个连接告警一次）。
- 证书不受信时**连接就失败**：`S3Storage::new` 的 GetBucketVersioning 探测遇证书错误报 `ConfigError`（不重试），写明两条出路；
  `check_connectivity` / `list_buckets` 同样。
- 不校验的客户端显式用 ring provider：依赖图同时打开了 rustls 的 ring 与 aws-lc-rs，`ClientConfig::builder()` 在
  进程没装默认 provider 时 panic（P3 之前的 `s3+https://` 就是这样）。
- 代理：校验证书的连接走 SDK 默认客户端，认 `HTTPS_PROXY` / `NO_PROXY`；`tls_verify=false` 的客户端不认代理（与改前相同）。
- 调试构建里主机没有任何系统根证书时，SDK 的 `debug_assert!` 会在第一个校验连接 panic（release 只告警）：容器镜像要装 CA 包。
- 测试：`tests/s3_tls_verify.rs`（本地 TLS 端点 + `tests/data/tls` 测试 CA；子进程设 `SSL_CERT_FILE`）。
  `tests/data/tls/server-key.pem` 是测试夹具的私钥（只签本地测试证书），秘钥扫描需放行。

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
  （`04:30:34 GMT`）。拷贝落到目的端的是秒。上层若拿列举（旧列举的毫秒 i64，无精度标记）去比
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
`tests/lab/tools/nfs_metadata_copy.rs` 的 `s3:<prefix>` 端点。**URL 里的 AK/SK 原样使用、不做百分号解码**
（e2e-s3 的 `url_builder.py` 会 quote，含特殊字符的 SK 会因此出错）。

### 目的端写入策略基线（C0，改造前，2026-09-24，VM 102 MinIO RELEASE.2023-03-20）

`bash .claude/skills/e2e-s3/scripts/staged_matrix.sh`（驱动 `tests/lab/tools/transfer_resume.rs`，只写/删 `staged-<run>/`）。
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
  StorageGRID 按前缀列）。实现在 `src/storage/backends/s3/client/role_protocol/multipart.rs`。
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

- 协议新增（`src/storage/backends/s3/client/role_protocol/versions.rs`）：`delete_version(key, id)` = 带 `versionId` 的 DeleteObject，
  彻底删这个版本、不加删除标记；删不存在的版本回 204 算成功（MinIO 实测）；Object Lock 拒绝 → 条目
  `PermissionDenied` / Permanent（AWS 403 `AccessDenied`；MinIO 400 `InvalidRequest`「Object is WORM protected」，实测）；
  405（存储不支持按版本删）→ `Unsupported`，不是带版本读的「删除标记」`NotFound`。`list_versions(key)` =
  ListObjectVersions 以 key 为前缀、只留精确 key，一页里出现别的 key 就停（精确 key 排在它前缀的所有 key 之前）。
  `S3WriteFacts.reported_version` 按响应原样保存版本（含 `"null"`），`version_id` 仍只放真版本；例外：连接时桶是
  Suspended 而回复没有版本头 → 记 `"null"`（`S3Storage::write_facts`，`958a3f9`；RustFS 1.0.0 暂停版本时 PUT 不回头）。
- **指针按自己的版本删**（`upload_pointer::delete`）：PUT 回的版本；回复丢失但读回逐字节相同 → 用读回 HEAD 的版本
  （字节含本次 prepare 的 nonce，不可能是别人写的）；discovery 清遗留指针 → 删它读到的那个版本（`S3Artifacts`
  记 `found_version`）；续传接管写了新指针后，删被替换的旧指针版本（`delete_replaced`），否则我们的删掉后旧的会重新
  变成当前 —— 删不掉（被锁住等）只告警，并把自己的 `pointer_version` 清成 `None`：发布 / discard 时改发普通删除，一个
  删除标记把两个都盖住。被替换的是 `"null"` 而我们的写入没报真版本（暂停 / 未开版本，有的存储连 `null` 头都不回）→
  那个 `"null"` 已被我们覆盖，不删（否则删掉的是自己的指针，发布时围栏报被接管）。
- **扫尾**（`upload_pointer::sweep`）：SDK 重发一个首发已提交但丢了回复的 PutObject，会留下两个逐字节相同的版本，
  我们只知道后一个；按版本删掉它，前一个就成了当前。所以每次按版本删指针后再读一次当前指针：内容是「陈旧内容」之一
  （删掉的那份、续传替换掉的那份）就按它的版本再删，最多 4 次；别人的指针不动；失败只告警（对象已发布 / 已丢弃，下次
  prepare 会清）。发布 / discard / discovery 清遗留 / 小对象清遗留都走它。暂停版本（PUT 回 `"null"`，或连接时 Suspended 而回复没版本头）→ `?versionId=null`（MinIO 在未开版本的桶上也接受；
  RustFS 暂停桶里回 204 + `x-amz-delete-marker: false`）；响应不带版本且桶不是 Suspended（未开版本的桶）→ 仍是普通
  DeleteObject。`958a3f9` 之前 RustFS 暂停桶里的指针走了普通 DELETE，按 S3 语义留下一个 `"null"` 删除标记
  （`versioning_matrix.sh` 暂停 200 MiB 行 1 标记 + 1 artifact；MinIO 回 null 头所以没有）。
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
- 暂停版本：PUT 回 `x-amz-version-id: null`（RustFS 不回，见上），MinIO 的 Complete 不回版本头 → `destination_version` 都是 `None`。
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
- **拷贝请求都带 `Content-Length: 0`**（所有 profile）：SDK 对没有 body 的请求不发长度（hyper 对空 body 不加）。Ceph RGW
  （DXN）把 `UploadPartCopy` 当分段上传处理、缺长度回 `411 MissingContentLength`（2026-09-25，`791f30d` 只给它加）；ONTAP
  9.19.1 对**`CopyObject`** 缺长度也回 411（对象桶与 NAS 桶，2026-09-26）—— ≤ 64 MiB 的原生拷贝在 ONTAP 上因此必败。
  `994828c` 起四处拷贝请求一律带（`src/storage/backends/s3/client/copy_request.rs::state_empty_body`，`customize().mutate_request`）：role
  `CopyObject` / `UploadPartCopy`（`role_protocol/native.rs`）与 legacy `CopyObject`（`s3.rs::copy_object`）/ `UploadPartCopy`
  （`multipart_rename.rs`）。curl 实测 MinIO、RustFS、DXN、ONTAP 都接受（RFC 9110：PUT 应带长度；AWS 未复测、
  StorageGRID 未测）。wire 测试 `role_protocol::native::tests::copies_pin_the_source_and_copy_no_metadata`（role 两种）与
  `copy_request::tests::legacy_copies_state_their_empty_body`（legacy 两种）。
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
  （NAS 桶目的端例外：一律 `UnsupportedByDestination`，P7 §14.2 N2）
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

### 列举 artifact 与历史迁移

role namespace 隐藏相对根任意一段以 `.data-mover-` 开头的对象；原始删除接口仍能清理 artifact。
版本化桶每 key 一项，`ObjectHistory` 按记录顺序重放版本与删除标记；单 key 超过记录上限时消费有序分段。
历史重放入口是 `tests/lab/tools/s3_history_replay.rs`。旧 scanner 的历史实验记录已移到
[历史证据](../../docs/design/p7-legacy-history.md)，API 迁移见 [清理清单](../../docs/design/p7-legacy-cleanup.md)。

### role-based 遍历与版本模式（ADR-0006 C22；P7 C11b：版本化桶每个 key 一项）

- **形态**：S3 出借 `Namespace`（`src/storage/backends/s3/namespace.rs`），由通用的深度优先
  `StorageTraversalSource` 驱动；不做扁平 `TraversalSource` —— 块顺序（目录的全部子项先于任何孙项，
  `TraversalOrder::Blocks`）下，扁平 `ListObjectsV2` 要把根下的全部 key 攒在内存里才能交出根的块。
- **一个目录 = 一个前缀**：`List(p)` = `ListObjectsV2(prefix = "p/", delimiter = "/")`；连接时判定为版本化的桶
  （下「历史」）= `ListObjectVersions` 同参数。续页 token / marker **原样回传**，只拿来判“绕回来了”（本目录
  跟过的所有 token 记在一个集合里，任何一个再出现 → **该目录**的 `Protocol` / Permanent 条目失败，遍历继续兄弟，
  防止 A、B、A… 死循环；之前是会话级失败、一个目录就结束整场遍历，P7 评审现存问题 3 修复），从不当 key
  比较或去前缀，也不用 `StartAfter`。
- **Current 流式（P7 C6，`current.rs`）**：每个 `ListObjectsV2` 服务端页一到就描述、按 `ListRequest.page_hint`
  切成若干页交出（首项只等一次请求，驻留 O(页)）。**不假设 key 有序**（ONTAP S3 开了去重 / 压缩时
  `ListObjects` 无序，Q22）：
  - 公共前缀：每个列举一个前缀名集合（列举给的与平铺 key 收拢出的都算），同一前缀在后面的页、乱序、同页多次
    出现都只交出一次（隐藏的 artifact 前缀只计一次、拼不出的前缀只报一次）——否则整棵子树扫两遍。内存 O(本目录子目录数)。
    隐藏的 `.data-mover-*` **对象**、越出目录的 key 若在不相邻的页重复，会再计 / 再报一次（只有前缀记整个列举）。
  - 对象：只与上一页比对（O(页)），吸收“页边界重复一个 key”；隔得更远的重复照样再交出一次 —— 契约允许同一子项
    出现两次（Q12 / Q13），消费方按 (父, 名) 去重，`listed` 计数可能含少量重复。
  - 页内按 key 序；跨页按服务端给页的顺序（有序存储上即 key 序，无序存储上无序）。遍历不依赖这个顺序。
  - 空页（什么都没列到，或只有自己的零字节 `p/` 标记）不交出；列完一页都没交出 → 交一页空页。
  - 翻页上限（下一条）只数**驻留**的：前缀名集合（重复的前缀只记一次）与报过的不可解码 key（续页 token 另有同样的上限）；交出的对象不驻留、不计
    —— 平铺前缀多大都能流完（§12.4 的 1000 万文件平铺目录；用户 2026-09-26 定：上限管内存，不管平铺前缀的大小）。
    上限 / token 回绕 / 连续空页在首页之后触发时，先交出的页照常发出、触发的那一页不交出，遍历记 `Interrupted { delivered }`。
  - **重试**（`paging.rs` 的 `PageRetry`，Current 与历史都用）：请求无状态，同一 token / marker 重发就是同一页，首页前后
    一样：节流（429 / 503 / `SlowDown`…）按 `throttle.rs` 有界退避（每个列举累计约 2 分钟，超了是该列举的 `Throttled` /
    Transient）；其他瞬时失败（5xx 但不含 501、超时、连接断开，已在 SDK 内重试过）再发至多 3 次（250 ms / 1 s / 2 s，读到一页即
    续满），用尽则按原失败的作用域报（会话级的照常升会话）。不从头重列、不重复交出。
- **历史按目录收齐**（`histories.rs` 的 `produce_histories`，§5.3 / Q9）：无序存储上一个 key 的版本可能在任意页，只有列完
  整个目录才知道历史完整；跨页的 key 天然是整的，不需要 C20 的“留下一页最大 key”（那个做法假设 key 升序）。收齐之后每次只把
  `page_hint` 个 key 构造成子项交出（第一页带目录自己的键与失败），不一次性构造整个目录的描述。
- **翻页上限**（`paging.rs`）：一个列举最多**驻留** 500 万条 —— 整个读完的列举（历史的版本 + 标记 + 前缀）按列到的条数计
  （`Paging::advance`）；流式 Current 只数驻留的前缀名集合与报过的不可解码 key（`advance_holding`），另有续页 token 上限
  500 万（每页一个，服务端换着新 token 无尽翻页也会停；每页 1000 个对象时是 50 亿个对象），交出的对象不计，平铺前缀
  多大都能流完（`MemoryS3` 的 `generated` 旋钮：5,000,001 个对象、生产上限，流完）→ 超了该目录 `Capacity` 失败；连续 16 页空页却还说有下一页 → 该目录 `Protocol` 失败；都不会悄悄截断。`IsTruncated` 缺失但有 token
  → 继续翻（同 legacy）；没有 VersionId 的条目当 `"null"`；历史列举里跨页重复的版本只留第一次。`Stat` 的前缀探测也按
  同样上限翻过空页，不把“第一页空但有下一页”当成不存在。
- **取消即停**：S3 的列举不持有句柄：`ListRequest.cancel` 取消或流被 drop 时进行中的请求（连同退避 / 重发的睡眠）
  直接丢弃，不在后台继续翻页（Current 在 `produce_current`、历史在 `produce_histories` 里 `select!`）；
  CIFS 这类持有目录句柄的列举仍跑完再关句柄（P7 C1 删了 `listings_are_abortable()`）。
- **不存在的前缀**：非根路径**列完**仍一条都没列到（没有对象、版本、标记、前缀，也没有自己的 `p/` 标记）→ `NotFound`
  条目失败，之前不交出任何页（前面的空页不算数）；遍历根拼错时根的列举是 `Unreadable`，不会被当成“源端为空”。存储根永远存在。
- **子项顺序**：一起描述的子项（Current 的一个服务端页、历史的整个目录）按 S3 key 序，公共前缀带尾 `/` 比较
  （`a` < `a!` < `a-0` < `a.txt` < `a/`）；同名的对象 `a` 与目录 `a` 并存。遍历块内保持这个列举序（P7 C4 删了兄弟排序与
  `rank`）：一个 key 一项；只有前缀 `a` 被下钻，下钻按子目录名字节序。
- **目录对象**（P7 F5，键以 `/` 结尾，有没有内容都算）：目录 `p` 自己的键 `p/` 由 `p` 的列举作为**空名的文件子项**交出，
  路径恰为 `p/`（name 对 filter 是 `p`、深度同 `p` 的子项、页内排在最前）；Current 只要存在就交出（零字节文件夹标记也是
  零字节文件条目），每个列举只交一次；版本化桶上它是一项、带整段历史（有内容版本、零字节版本、删除标记，旧→新；最新是
  标记也列出）。身份
  `PathScoped(len‖"p/"‖ETag)` / `VersionScoped`，与文件 `p`、目录 `p` 都不冲突。存储根自己的键（URL 前缀本身）：
  零字节 / 全零历史忽略，有内容 → 根上的 `Unsupported` 子项失败；扫描根 `p` 自己的 `p/` 同理（遍历处理）。`stat("p/")`
  只 HEAD，不存在 → `NotFound`。MinIO 2023：版本桶里最新是删除标记的 `p/` 仍被 ListObjectsV2 列成无 ETag、0 字节的
  Contents（HEAD 404）→ 不是子项。
  目的端写 `p/d/`（P7 D13，F5g）：**只用单次请求**，不看单 PUT 阈值与 native 单次拷贝上限 —— 流式 / `Direct` 一次
  `PutObject`（整对象先缓冲在内存），native 一次 `CopyObject`；从不走 multipart / `UploadPartCopy`，不写指针、不续传
  （失败从零开始；Checkpointed 的结果因此报 `SkippedBelowCheckpointThreshold` / `SkippedSingleSourceChunk`；流式的整个缓冲在内存，每个最多 5 GiB）。> 5 GiB 或源大小未知 → 该条目 `Unsupported` / Permanent，在 **Preflight**、目的端任何 I/O 之前
  （`StagedDestination::directory_object_limit` = 5 GiB；S3 prepare 自己也在 I/O 前同样拒），不回退。消费方可事先知道：
  `transfer::directory_object_action(&entry, &目的端 Storage)` 对这种条目答 `Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON)`
  （与 Preflight 的诊断同一常量；只传 `NamespaceCapabilities` 时不知道上限，答 `Copy`）。role 协议与 legacy
  写入对以 `/` 结尾的键从不发 `CreateMultipartUpload`（发出前就拒）。prepare 仍按父目录里的指针名
  （`p/.data-mover-<digest("d/")>.upload`，`artifacts::directory_object_artifact`）清掉 F5g 之前的写入者留下的指针与上传。
  各存储差异见下「目录对象真机」。
- **异常 key**：空段 / `.` / `..` 段（`a//`、根下的 `/x`、`./x`、`a/./b`、
  `a/..`）是逐子项失败（报在服务器拼写的路径上，列举 `Partial`），不会被并进别的路径 —— 目的端的路径处理会把
  `a/./b` 折成 `a/b`，两个对象写到同一个文件；`List("a/")` / `"a//b"` / `"a/."` 这类路径 `InvalidInput`。不理 delimiter 的存储
  平铺返回的深层 key 在客户端收拢成一级前缀（去重）。`.data-mover-*` 两种模式都隐藏（key 或任意一段前缀）。
- **是否列历史**（P7 Q26 / Q-A，C11b）：连接时 `GetBucketVersioning` 的回答（`S3VersioningAtConnect`，
  `src/storage/backends/s3/client/versioning_answer.rs`）：`Enabled` / `Suspended` / 明确拒绝（403 等，未知 Status 同）→ 列历史；从未开启 /
  501·`NotImplemented`·`UnsupportedOperation` → `ListObjectsV2`；无响应 / 超时 / 408 / 429 / 5xx / `RequestTimeout`·`SlowDown` 等
  （SDK 重试之后）→ **role connect 失败** `BackendSessionFailure` Connect / `Connectivity` / `Transient`（诊断带 HTTP 状态；
  `connect_backend` 的 `BackendConnectError::session_failure()` 给出它），不猜；构造请求失败按拒绝处理。NAS profile 一律不列历史、
  也不因无响应拒连。legacy `S3Storage::new` 照旧连接成功（`is_bucket_versioned` / `versioning_suspended` 语义不变）。
  HTTP 2xx 但 SDK 解码失败也算没有回答，即使 SDK 包成 `ServiceError`，仍按上述 `Connectivity` / `Transient` 拒连；
  正常解码的 Enabled 与空配置不受影响（`757c6c9`，SDK 响应回放回归）。
  `S3Namespace` 在连接时取一次 `S3Protocol::lists_histories()`，整个连接不变，`capabilities().histories` 与之相同。
  **与 legacy 的差别**：瞬时失败时 role 连不上而 legacy 连得上；只有 `s3:ListBucket`、被拒 `GetBucketVersioning` 与
  `ListBucketVersions` 的主体在 role 遍历上每个目录都会权限失败（按 Q26 列历史；待定是否加回退）。
- **Current**（未开版本的桶）：只有当前对象；身份 `PathScoped(路径, ETag)`（`ListObjectsV2` 不带 versionId；Q28 之前只有
  ETag）；时间 = 列举的 `LastModified`（毫秒精度，HEAD 只有秒），`modified` 过滤器不发 HEAD；目录无时间。
- **历史**（版本化桶，C11b）：**每个 key 一项**，`ObservedEntry::history()` = `ObjectHistory`：记录旧→新、latest 最后，按
  **服务端文档顺序**倒过来（见下「版本历史顺序」）；校验不过的 key 按时间推断，`order() == HistoryOrder::Inferred`（例子打印
  ` order=inferred`）。当前已删的 key（最新是删除标记）**也列出**，`history.current()` 为 `None`。
  - 条目层字段 = **当前对象**：size、`ETag`、时间、身份 `PathScoped(路径‖ETag)` 与 `ListObjectsV2` 给同一对象的逐字节相同
    （桶开关版本身份键不变；expert 源半边照常核对）；已删的 key：size `None`（DSL `size` 条件照旧 `LazyMatch` 放行，用户
    2026-09-25 定）、时间 = 标记的（`modified` 条件按标记时间判断）、身份 `PathScoped(路径‖"\0s3-deleted-key\0"‖标记 id)`、
    列举之外无可观察。`source_version()` = 当前对象的选择器（真版本 `Id(id)`、latest 的 `"null"` → `Current`），已删为 `None`。
  - `VersionRecord`：`version_id()`（`"null"` → `None`）、`is_latest()`、`kind()`（对象 size / ETag / 时间，或删除标记）、
    `identity()`（`VersionScoped(id)`；`"null"` 为 `PathScoped(路径‖ETag)`，与 `describe_version` 一致）、`source()`（非 latest 的
    `"null"` → `Id("null")`，标记 → `None`）、`metadata()`。
  - 可选元数据（计划要列举之外的）逐条串行观察、绑定各自版本（`observe_copy_bound_version`，标签用它的 versionId），占遍历的
    同一个观察名额；一条失败只记在那条上（`MetadataObservation::Failed`，`Required` 也一样），整项照常交出；删除标记不观察；
    条目层元数据取当前版本的（观察失败时时间退回列举的）。版本很多、计划要 tags 时每个 key 要做 N 次 HEAD / GetObjectTagging。
  - **分段**（Q8）：一个 key 超过 10 万条记录时发多项（段号从 0，只有最后一段 `complete`），连续、有序；只有最后一段有
    `current()`；续扫后从第 0 段重发。计数按 key：`versions` / `delete_markers` / `all_versions_bytes` 另计，`bytes` 为当前版本字节。
  - 快照当前为 v8，保留历史段（`src/model/history_codec.rs`）并增加可选 rename evidence 字段（S3 为 None）；旧格式一律 `UnsupportedVersion`。
- **专家传输（terrasync）**：源端半程用 describe 复核观察；版本化桶上列举的观察是 `PathScoped(路径, ETag)`、describe 钉住
  `VersionScoped(id)`，`ReadSource::observation_matches` 在 describe 到的路径与版本 ETag 拼出的身份等于列举的身份时认作同一对象，
  对象被替换则仍报 "source differs"；offer / evidence 带观察的身份键（目的端半程比对它），读仍钉在 describe 到的版本。
  带历史的条目只有 `current()` 有值时能走专家传输：已删的 key 报 "deleted now"、非最后一段报 "not its last"；旧版本用
  `with_source_version` 拷（`tests/lab/tools/s3_history_replay.rs`）。
- **拒绝**：S3 的变更动词 `Unsupported`；`delete_tree` / `create_directory_all` 在 I/O 前拒绝 S3（同以前）。非 S3
  存储没有历史，条目不带 `history()`。
- **例子与真机**：`S3_LISTING_URL=… cargo run --example storage_role_operations -- --backend s3 traverse [--order blocks]
  [--current]`（版本化桶：条目行 `history=<段>/<complete> records=<n>[ order=inferred]`，每条记录一行
  `Record <key> <段>.<序号> version= latest= marker= size=`；`--current` 只打印当前对象）；
  `.claude/skills/e2e-s3/scripts/traversal_versions.sh`（见 SKILL 第 8 步）。C11b / C12 已完成，真机矩阵、十万版本测量、
  实际扫描 cancel / SIGKILL 续跑与环境限制见设计文档 §14.3。
### 版本历史顺序与 ONTAP 分页

`src/storage/backends/s3/client/version_order.rs` 的 `send_in_document_order` 保留 SDK 原始 XML 中 Version / DeleteMarker
的交错顺序，role 协议据此构造 `HistoryOrder::Server`。顺序无法验证时显式标为 `Inferred`；
`LastModified` 不能恢复同毫秒并发写入的真实顺序。组装规则在 `storage/backends/s3/history.rs`。
`version_order_tests.rs` 用真实 MinIO XML 回放并验证整页、22 页跨页和重试响应顺序。

ONTAP 带 delimiter 的截断页可能只有 `NextKeyMarker`、没有 `NextVersionIdMarker`。
`version_order` 用原始交错序恢复页末 key 的最后一个版本标识；恢复不了就报该目录
`Protocol` / Permanent，禁止跳过该 key 剩余历史。空 version marker 归一化为 None；单 key
不带 delimiter 的版本查询不受影响。`version_order_ontap_tests.rs` 保留真实 ONTAP XML
回放、每版本/前缀一次、未知恢复点和空 marker 请求断言。
历史实验明细见 [历史证据](../../docs/design/p7-legacy-history.md)。

### DXN（Ceph RGW Octopus）实测行为（2026-09-25）

`versioning_matrix.sh` 在 DXN 临时桶（`BUCKET_PREFIX=data-mover-dxn`，`S3_LOCATION=magnascale:tos_placement`，见 e2e-s3
SKILL.md）上跑出来的差异，以及各自的处理：

| 行为 | MinIO / AWS | DXN | 处理 |
|---|---|---|---|
| `UploadPartCopy` 不带 `Content-Length` | 接受 | `411 MissingContentLength` | 一律带 `Content-Length: 0`（见 C18 节） |
| `CopyObject` 不带 `Content-Length` | 接受 | 接受 | 仍一律带（ONTAP 要，`994828c`） |
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

### ONTAP S3 NAS 桶实测行为（2026-09-26，ONTAP 9.19.1，桶 `data-mover-nas`）

NAS 桶（multiprotocol，底下是 FlexVol `dm_s3_nas`）与同一 SVM 的对象桶差别很大。curl（凭据经 0600 `-K` 文件）+
`staged_matrix.sh` 实测。第三列是**不写** `bucket_type=nas` 时 data-mover 的表现，第四列是 NAS profile
（`?compat=ontap&bucket_type=nas`，P7 §14.2 N1–N7）：

| 行为 | 对象桶（`data-mover-test`） | NAS 桶 | 不写参数 | NAS profile |
|---|---|---|---|---|
| PUT 回的 `ETag` | 内容 MD5，与 HEAD 相同 | 内容 MD5（**只在 PUT 回复里**） | — | 流式单 PUT 核对它（`upload_digest`） |
| HEAD / GET / 列举 / CopyObject 回复的 `ETag` | 同上 | **另一个不透明值，不是 MD5**（由 mtime + size 派生，N0） | 读回报 `Verify … (Conflict)`「replaced after publication」（误导） | 身份 `PathScoped(路径 + ETag)`；读回括号式（N4） |
| 条件 GET（`If-Match` / `If-None-Match`，任何值） | 支持 | **501 `NotImplemented`** | 钉住的读失败，`Unsupported` + 补参数提示（N6） | 不发条件，核对响应（N1） |
| 条件 PUT `If-None-Match: *` | — | 支持（412） | — | 不用 |
| `CreateMultipartUpload` | 支持 | **501 `NotImplemented`** | Direct > 8 MiB `Write … (Unsupported)` + 提示 | 从不发；> 8 MiB 流式单 PUT（N3） |
| `ListMultipartUploads` / `ListParts` | 支持 | **403** | Prepare `Observe … (PermissionDenied)`（`compat=ontap` 下带提示） | 从不发（不续传，N2） |
| 单次 PUT > 8 MiB | 支持 | 支持（5 GiB + 1 也收） | 用不上（> 8 MiB 走分段） | 8 MiB–5 GiB 流式单 PUT；> 5 GiB Preflight 拒 |
| 同桶 `CopyObject` | 要 `Content-Length`（411） | 同样要（F6 起带）；`x-amz-copy-source-if-match` 认存储的 ETag；克隆 | 读回开时源端条件 GET 501 | 一次 CopyObject ≤ 5 GiB，读回锚定拷贝回复（N5） |
| 对象桶 ↔ NAS 桶的 `CopyObject` | — | **501**（两个方向） | 同端点同 `compat=ontap` 走原生 → `Unsupported` | 亲和含桶名，走流式（N1） |
| `x-amz-meta-*`、`?tagging` | 支持 | 支持；**`PutObjectTagging` / `DELETE ?tagging` 不改 `ETag` 与 `Last-Modified`**；`CopyObject` REPLACE（带或不带 `x-amz-meta-*` / `x-amz-tagging`）回复 `ETag` = 之后 HEAD 的；自拷贝 REPLACE 换元数据则换新 `ETag` 与 `Last-Modified`（2026-09-27 实测） | — | 原生拷贝设标签后不再 HEAD，读回锚定拷贝回复（N5 跟进） |
| `GetBucketVersioning` / `ListObjectVersions` | 支持 | 501（不支持版本） | 连接时当作未开版本 | 无版本：源端不给版本，遍历全部版本在 I/O 前拒 |
| 删对象后的目录 | — | 目录留下；非空目录 `DELETE <dir>/` 403，空目录 204 | 清理按后序逐个 `DELETE <dir>/` | 同左 |

结论（N7 起）：写 `?compat=ontap&bucket_type=nas` 后 NAS 桶可作源（无序列举、无条件读 + 收尾 HEAD）与目的端（≤ 5 GiB，
不续传，括号式读回，同桶原生拷贝），较弱的保证逐条写进结果（`EffectiveRecovery::UnsupportedByDestination`、
`TransferOutcome.verification`）。不写参数时照旧失败，但 501 / 403 的诊断提示补参数。legacy `StorageEnum` 不理会这个 profile。
真机（2026-09-27，`nas_matrix.sh` 四轮 `RESULT: pass`，行与结果见 P7 设计 §14.2「N1–N7、F5h 实施记录」）。

**补充实测（N0，2026-09-26，原始 socket SigV4 客户端 + curl + 10.131.9.10 上 NFSv3 root 挂载）**，供 NAS profile（P7 §14.2）使用：

| 行为 | NAS 桶 |
|---|---|
| 单 PUT 原子性 | **原子**：中途杀客户端、半关 / close、差 1 字节断开、Content-MD5 / 签名 sha256 不符（400 BadDigest）都不改变旧对象；暂存不可见，约 25 s 释放空间 |
| 扣住最后 1 字节 | 服务器静等 ≥ 900 s（上限未测出），补发后约 0.04 s 提交 |
| `UNSIGNED-PAYLOAD` / chunked | 前者接受；`Transfer-Encoding: chunked` → 501，必须带 Content-Length |
| 单 PUT 大小 | 5 GiB + 1 也接受；不预检大小与剩余空间（100 Continue） |
| 不透明 ETag 的来源 | **只由 mtime（纳秒）与 size 算出**：NFS 写改 mtime → ETag 变；保留 mtime 的改写（`rsync -t` / `touch -d`）ETag 不变；不同文件 mtime + size 相同则 ETag 相同 |
| 替换时进行中的 GET | 提交后 0.3–0.7 s 被断开，已收字节是旧内容的前缀、短于 Content-Length，从不混入新内容 |
| Last-Modified | 精度到秒（列举 `.000Z`）；Range、多段 Range（multipart/byteranges）、416 正常 |
| key 规范化 | **静默改写**：`..`、`.`、`//`、`%2E%2E`、`%2F` 被解析后存到另一个 key；`dir/..` → 409 |
| 名字限制 | 段 > 255 字节 → **500 InternalError**；非法 UTF-8 → 403 AccessDenied；> 255 字节的 UTF-8 名在 NFS 上被改名（`:S400001`），两个 key 指向同一对象 |
| 文件 / 目录冲突 | 有文件 `a` 时 PUT `a/x` → **404 NoSuchKey**；有 `a/x` 时 PUT `a` 或带内容的 `a/` → **409 NameCollision** |
| `d/` | 0 字节建真目录（HEAD 404、仅分隔符列举可见）；带内容存成文件 `d` |
| 同桶 CopyObject | 克隆：1 GiB 0.01 s，副本独立，目标 mtime = 拷贝时刻 |

### 目录对象真机（P7 F5，2026-09-26，`directory_objects.sh`）

| 存储 | 写 `d/`（单 PUT） | 存储自己对 multipart `d/` 的行为（curl 探测；data-mover 自 F5g 起不发） | 版本桶里的 `d/` | 其他 |
|---|---|---|---|---|
| MinIO RELEASE.2023-03-20 | 可以（含内容） | **拒绝**：400 `XMinioInvalidObjectName` | 正常记版本；最新是删除标记时 ListObjectsV2 仍列出无 ETag、0 字节的 `d/`（HEAD 404）→ 列举不当子项，父目录仍列前缀 `d/` | 合并连续删除标记 |
| ONTAP 9.19.1 对象桶 | 可以 | 可以 | 正常 | 不合并删除标记 |
| DXN（Ceph RGW Octopus） | 可以 | 可以 | 正常 | 合并连续删除标记；只有 null 版本的 key 重开版本后写新版本再 DELETE：删掉 null 版本并写标记 |
| StorageGRID 11.5 | 可以 | 可以（`compat=sg` 不走 native） | 正常 | 已删除的 key 再 DELETE 每次新建标记 |
| RustFS 1.0.0 | 可以 | **完成后存成 `d`**（去掉 `/`，HEAD `d/` 404），覆盖同名文件 `d` | **不记版本**：每次写都是 `null`，DELETE 直接删掉（`x-amz-delete-marker: false`） | 从 `d/` **源** 的 native UploadPartCopy（F5d 时测，目的键 `d/`）：版本桶里会话 Connectivity 失败、非版本桶里存成 `d`；目的键不以 `/` 结尾、源 > 64 MiB 的这条路线 D13 不管，未复测 |
| ONTAP NAS 桶 | 零字节：200 但不留对象（只是目录）；有内容且先于子项：存成**文件 `d`**，之后子项 404；有内容且子项已在：409 | NAS 桶本来就只适合作源（见 test-environments） | 不支持版本 | 平铺列举无序 |

写入目录对象的差异（MinIO 拒绝、RustFS 改名覆盖同名文件、NAS 桶先写变文件）：**用户 2026-09-26 定为目录对象只用单次请求写**
（D13，F5g 已实现：单 PUT / 单次 CopyObject，不走 multipart；> 5 GiB 在 Preflight、I/O 之前逐条目 `Unsupported`），见 P7 §14.1。
NAS 桶由 NAS profile（`directory_objects=false`）处理。F5g 真机（2026-09-26，`directory_objects.sh`）：MinIO、RustFS、ONTAP 对象桶、
DXN、StorageGRID 的版本与非版本桶全部 pass —— 12 MiB `mid/` 本地→S3 三种 policy、200 MiB `big/` 流式（切断后不留东西、重跑 `Fresh`）与
native（StorageGRID 为流式）都是一次请求（ETag 无 `-N`），同名真实文件 `mid` / `big` 不动。
F5h（P7 §14 F5h）：超过单 PUT 阈值（默认 8 MiB；阈值未设时也按 8 MiB）的目录对象不再整个缓冲在内存里，改为一次**流式单 PUT**
（N3 的通用原语 `put_object_streaming`：Content-Length、UNSIGNED-PAYLOAD、边传边算 MD5 与回复 ETag 核对、扣住最后 1 字节到
publish、看门狗），任何能存目录对象的桶都是；≤ 阈值照旧缓冲单 PUT 带 Content-MD5；native 照旧一次 CopyObject；prepare 照旧按
父目录里的旧指针清场。差别：SSE-KMS 桶上 > 阈值的 `d/` 报 `upload_digest = Unchecked(EtagNotMd5)`（F5g 的缓冲 PUT 带
Content-MD5，是 `Matched`）；存储若在扣字节期间因 body 空闲断开连接，publish 报未变（请求已结束、body 不完整）。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| 404 被当 retry | 已修，必须 → `FileNotFound` |
| Multipart 失败留垃圾 | 必须 abort |
| MinIO 列出的 upload id 与签发的写法不同 | 比较 upload id 用 `upload_pointer::same_upload`，别逐字节比 |
| 自签 / 私有 CA 端点连接失败（证书不受信） | 把 CA 放进 `SSL_CERT_FILE`，或 URL 加 `?tls_verify=false` |
| 旧协议名 `s3+sg://` / `s3+dxn+https://` 等 | 已删除，改写成 `?compat=` 参数（报错里给出新写法） |
| DXN 对无 body 的 `UploadPartCopy`、ONTAP 对无 body 的 `CopyObject` 回 411 | 所有拷贝请求显式 `Content-Length: 0`（`copy_request.rs`，legacy 与 role） |
| 把 501 `NotImplemented` 当断连重试 | 条目 `Unsupported` / Permanent（`a65ae18`）；ONTAP NAS 桶大量回 501 |
| RustFS 暂停版本桶里 PUT 不回 `x-amz-version-id: null` | 连接时 Suspended → 按 `"null"` 记（`S3Storage::write_facts`），指针按 id 删 |
| ONTAP NAS 桶 PUT 回的 ETag ≠ 之后 HEAD 的 ETag | 存储性质：URL 写 `?compat=ontap&bucket_type=nas`（NAS profile：括号式读回、prepare HEAD 结算），见「ONTAP S3 NAS 桶实测行为」 |
| DXN 版本化桶的 `CopyObject` 不回版本 | 桶开了版本且回复无版本 → 列版本认领（`native_final::settle_copy_version`） |
| `bucket.host` 解析错误 (path-style vs virtual-hosted) | 检查 endpoint 是否支持 virtual-hosted |
| URL 里的凭据被切错 | AK 按第一个 `:`、凭据按最后一个 `@` 切：AK 不能含 `:`，SK 不能含 `@`；**不要** percent-encode（按原样取，编码过的 SK 签名错） |
| 把 MinIO 的 `NextKeyMarker`（`p/z[minio_cache:v2,return:]`）当 key 用 | 续页 token / marker 只原样回传（C20 / C22） |
| 同一目录里对象 `a` 与前缀 `a/` 同名 | 按路径做键的消费方会撞；块内按 S3 列举序（对象 `a` 在 `a/` 之前，C22） |
| 按 `LastModified` 排同一 key 的版本与删除标记 | 毫秒精度，并发写入同毫秒；用服务端文档顺序（`version_order.rs` + `history.rs`，Q29） |
| `PathScoped` 身份只放内容事实（ETag） | 同内容不同 key 撞身份键；稳定字节必须含路径（Q28） |
| Region 推断 | endpoint host 推断，必要时显式设 `AWS_REGION` |

## 测试

- skill：`.claude/skills/e2e-s3/` (需要 `.env` 含 endpoint/bucket/ak/sk)。
- skill 检查不存在的 prefix 返回平衡空列表；HEAD 的 NoSuchKey 映射由 metadata 测试覆盖。
- `DM-STORAGEGRID-REQUEST-CONTRACT`：
  `cargo test --lib storage::backends::s3::client::storagegrid::tests --locked`，在 capturing Smithy connector seam
  验证只有 StorageGRID 去 `x-id`、所有 profile 的 DeleteObjects 都带签名 MD5；PR 与 release workflow 均独立执行。
- `tests/lab/tools/storage_role_operations/` — role 遍历与历史记录输出。

## 改 S3 时

1. 读本 doc + 当前 `src/storage/backends/s3/client.rs`。
2. 改错误映射必须同步 [error-taxonomy.md](error-taxonomy.md)。
3. 改 multipart 阈值 / 并发，跑性能测试 (skill 暂未含)。
4. 调 `backend-specialist` agent 传 `s3`。
5. 验证：`make e2e-s3` (需 .env)，否则至少 `make clippy && make test`。
