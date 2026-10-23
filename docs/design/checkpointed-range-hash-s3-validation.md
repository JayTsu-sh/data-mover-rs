# Checkpointed range-hash：S3 真机验证

日期：2026-10-23（Asia/Shanghai）。最新状态：**DXN 引号兼容性缺陷已由 main 修复，§9 的两种桶“旧→新”和“新→新”共 4 例真机恢复全部通过。其他未覆盖范围不变，不能宣称五套 S3 全部通过。**

阅读顺序：§1–§8 保留初轮旧二进制的历史结果，其中 DXN 的失败结论仅适用于修复前版本；修复后版本、实际源 READ、独立 checksum 和清理结果以 **§9** 为准。

本报告仅记录当前未提交实现的真实执行结果，不以单测或 CI 通过代替真机覆盖。验证期间未修改生产源码、共享服务配置或既有桶的版本设置；未提交或推送。范围和保证边界见[设计](checkpointed-range-hash-recovery.md)、[实验室环境表](../../.claude/docs/test-environments.md)及 [e2e-s3 skill](../../.claude/skills/e2e-s3/SKILL.md)。

## 1. 执行版本、安全范围和证据

- 工作树基线 HEAD：`6aa1633e749c0f5255c82d628f966102bcc39e81`，叠加本轮未提交的最终 feature；不是仅验证该 commit。
- 实际真机工具：`/work/cargo-target/x86_64-unknown-linux-musl/release/examples/transfer_resume`，SHA-256：`5d999398121640d9c00f6bb20a52d77b58b37938b336f269347e51a629e4d6d9`。GNU 示例也成功构建，但下表使用的是上述 musl 工具。
- 唯一远端对象前缀：`resume-hash-20261023-wejz4uyf/`。只访问批准的 `data-mover-test` 和本轮创建的 `data-mover-resume-hash-*` 临时版本桶；不访问 `a-bucket` 或其他团队的数据桶。
- 全程按阶段获取 `/tmp/terrasync-lab-tests.lock` 与 `/tmp/terrasync-lab-performance.lock`。CIFS 接手锁时 S3 立即停止在获取锁处，没有绕过锁；最终两个锁均已释放。
- 仓库内五套 `.env` 均缺失。MinIO、DXN、RustFS 使用本机 0600 实验室清单中可用的凭据，仅在内存与子进程环境传递；未输出凭据。该结果不等于验证缺失 `.env` 中专用 IAM 用户的权限。
- 每次 transfer 均为独立进程、独立空 HOME；运行后检查 HOME 仍为空。恢复不读取任何本地 checkpoint。

证据目录：`/work/resume-hash-s3-validation-wEjz4UYF/`。验证脚本位于 `/tmp/resume-hash-s3-validation-4an24LRm/`，不属于生产代码；日志和快照保留在本机，未加入仓库。主要证据：

| 证据 | 内容 |
| --- | --- |
| [minio.log](/work/resume-hash-s3-validation-wEjz4UYF/minio.log) | 首轮 MinIO 完整记录：7 个满足各自验收条件的用例；发布间隙脚本误报另述 |
| [priority.log](/work/resume-hash-s3-validation-wEjz4UYF/priority.log) | 后续有限复跑：DXN 两种桶的实际失败、RustFS 两个流式恢复通过、脚本限制 |
| [dxn-etag-probe.log](/work/resume-hash-s3-validation-wEjz4UYF/dxn-etag-probe.log) | 5 MiB 独立 multipart 探针，确认 ETag 引号差异 |
| [events.jsonl](/work/resume-hash-s3-validation-wEjz4UYF/events.jsonl) | 所有轮次事件；同名复跑有重复记录，不能简单累计为新增覆盖 |
| `*.snapshot.json` / `*.pointer` | 中断后的 pointer、H、D、冻结 part size 和 `ListParts` 可观察范围 |
| `*.trace.*` / `*.trace-<session>.*` | 流式恢复实际源文件 `pread64`；后续脚本按 session 隔离 |
| `*.http-audit.json` | 原生恢复 HTTP 请求分类，排除 tagging、pointer、`ListParts` 等元数据 GET |
| [final_cleanup_audit.json](/work/resume-hash-s3-validation-wEjz4UYF/final_cleanup_audit.json) | 最后独立核对的对象、版本、精确 key 上传数及临时桶不存在证明 |

## 2. 环境实际覆盖

| Profile | 实际访问 | 非版本 / 版本 | 结果与限制 |
| --- | --- | --- | --- |
| MinIO 2023 | `10.131.9.11:9000`，standard profile | 两种均执行 | 流式取消/SIGKILL、原生故障/SIGKILL恢复、正常历史清理；发布间隙证据见 §5 |
| DXN / Ceph RGW | `10.131.7.201:8184`，`compat=dxn` | 两种均执行 | 初轮因 receipt 指纹不一致失败（§6）；修复后旧→新、新→新共 4 例通过（§9） |
| RustFS 1.0.0 | `10.131.9.14:9001`，standard profile | 两种均执行 | 流式取消恢复通过；原生 relay 的 HEAD 转发错误使原生测试无效，未计覆盖 |
| ONTAP 对象桶 / NAS | 未执行 | 未验证 | 专用 S3 密钥仅在缺失的 `.env` 中；未创建新密钥或借其他身份绕过 |
| StorageGRID | 未执行 | 未验证 | 独立测试租户密钥缺失；清单中的旧密钥属于另一授权写入范围，不用于本轮前缀 |

临时桶只在确认不存在后创建，并检查 CreateBucket 成功。仅对本轮临时桶启用版本；DXN 建桶使用批准的 `magnascale:tos_placement`。MinIO 临时桶带 Object Lock 能力，但本轮**没有完成 legal hold 用例**，不能把建锁桶当作锁下清理验证。

## 3. 数据和计数口径

非零确定性源文件 `payload.bin`：

```text
N = 469,762,381 bytes = 448 MiB + 333
SHA-256 = f4c132149a2c26ba4a3eb00602a967fa1fcc459555be270cec72ba07cf039ee7
```

流式首轮 source block ceiling 为 `3,145,731`，恢复为 `5,242,887` 字节；backend/QoS 可以继续细分实际请求。首轮限速 32 MiB/s，在约 3.5 秒取消或 SIGKILL；恢复使用非限制性 source budget 并开启最终读回。multipart geometry 保持 8 MiB，未随 source block ceiling 改变。

区分三类证据，避免把 H 或文件长度当作 D：

1. `D_pointer`：指针里已记录的范围。
2. `D_parts`：中断后真实 `ListParts` 证明、且被已持久化 H 覆盖的完整 part 范围；允许超过旧 `D_pointer`。恢复 `reused_bytes` 必须等于其总字节数。
3. 源读取：对本地源按文件路径过滤 `strace -ff -yy -e trace=read,pread64` 的成功 `pread64`，计实际返回字节、次数、范围并集以及与 `D_parts` 的交集；同时与有 source budget 的工具计数交叉核对。独立 SHA-256 下载和首轮预读不混入恢复 transfer 的源计数。

原生用例通过本地 HTTP relay 向真实存储转发签名请求，记录源 payload GET 与 `UploadPartCopy` 数量。relay 不是模拟存储，不保存恢复状态；其延迟和响应故障只用于中断。正常元数据 GET 不计 payload；SDK 的 `x-id=GetObject` 和可选 `versionId` 不应被误排除。`ListParts` 返回空缺口以后的部分，在本轮已完成原生用例中是连续前缀；**稀疏岛屿没有完成真机验收**。

指针快照脚本只解析布局并记录 SHA-256，不是独立实现的 BLAKE3/checkpoint 校验器。格式、receipt、H 覆盖校验由被测生产 decoder 执行；损坏记录用例另验证其失败关闭行为。

## 4. 已完成的恢复与独立 checksum

### 4.1 流式：5 个完成用例

下表均满足：新进程 `Resumed`、精确复用 `D_parts`、实际源 READ 与复用范围交集为 **0**、无重复源读取；最终开启读回，随后另用独立下载计算 SHA-256，均等于 §3。每个最终 key 当时均只有一个版本、零删除标记、零 pointer 历史、零未完成上传，检查发生在人工清理整个测试前缀之前。

| 用例 | `D_pointer` 字节 | `D_parts` / reused 字节 | 恢复实际源字节 | 成功源 READ 次数 |
| --- | ---: | ---: | ---: | ---: |
| MinIO 非版本，取消 | 83,886,080 | 92,274,688 | 377,487,693 | 181 |
| MinIO 版本，取消 | 83,886,080 | 92,274,688 | 377,487,693 | 181 |
| MinIO 版本，SIGKILL | 83,886,080 | 92,274,688 | 377,487,693 | 181 |
| RustFS 非版本，取消 | 75,497,472 | 92,274,688 | 377,487,693 | 181 |
| RustFS 版本，取消 | 33,554,432 | 58,720,256 | 411,042,125 | 197 |

例如前三行实际源 READ 并集恰为 `[92,274,688, 469,762,381)`，不是从 `D_pointer` 的 80 MiB 重新读起。RustFS 版本用例并集为 `[58,720,256, 469,762,381)`。这证明本轮可观察层面的恢复避免重读，而非仅有最终 checksum 相同。

源码对应：[S3 part/H 恢复与 receipt 校验](../../src/storage/backends/s3/staged/hash_checkpoint.rs)、[所有合法 part 的几何验证](../../src/storage/backends/s3/staged/upload_discovery.rs)、[冻结 geometry 的写入聚合](../../src/storage/backends/s3/staged/checkpointed_parts.rs)。

### 4.2 原生：3 个完成的故障恢复用例

这些用例的持久化 H 均覆盖整个源文件，而旧指针里的 D 为 0；真实 part 可以通过 H 被采用。原生 geometry 始终为 64 MiB。恢复客户端没有源 payload GET，source budget 计数也为 0；不能据此称服务器内部未读源，服务器仍执行表中的剩余 `UploadPartCopy`。

| 用例 | `D_parts` / reused 字节 | 恢复源 payload GET 字节 / 次数 | 新拷贝字节 | 新 `UploadPartCopy` 次数 |
| --- | ---: | ---: | ---: | ---: |
| MinIO 非版本，relay 故障后恢复 | 402,653,184 | 0 / 0 | 67,109,197 | 2 |
| MinIO 版本，relay 故障后恢复 | 402,653,184 | 0 / 0 | 67,109,197 | 2 |
| MinIO 版本，SIGKILL 后恢复 | 134,217,728 | 0 / 0 | 335,544,653 | 6 |

三行最终均另行完整下载，独立 SHA-256 等于 §3；最终 key 各一个版本、零 marker、零 pointer 历史、零 open upload。

前两行脚本标签含 `native-cancel`，但 20 秒 relay 延迟触发客户端连接/操作超时，首次结果实际为 `Connectivity`，不能称为纯 cancellation-drain 测试。它们证明的是**服务端已落 part、客户端丢失成功回执后，使用先行 H 安全采用尾部**。最后一行确实 SIGKILL；所有重启均为新进程/空 HOME。

源码对应：[原生 copy 的 part 调度、确认与稀疏复用](../../src/storage/backends/s3/staged/native_final.rs)、[原生 hash-only 前置流程](../../src/transfer/engine/native.rs)。

### 4.3 损坏记录：1 个符合预期的失败关闭用例

MinIO 非版本桶，取消后保留 v2 pointer 与 multipart；仅翻转 pointer 外层 checksum 的一个字节。新进程准备失败，实际源 READ 为 **0 字节 / 0 次**，人工清理前 pointer 版本集合与 open upload 未改变。没有把坏 v2 当 legacy orphan 自动清理。

证据：`minio-plain-stream-cancel-corrupt-*`、首轮 `minio.log` 的 `case_pass`；源码对应 [v2 识别和 decoder](../../src/storage/pointer.rs)。此预期失败不计为正向恢复成功。

## 5. 发布间隙：核心行为已观测，但不计完整验收通过

MinIO 版本桶的 relay 拒绝 Published-phase pointer PUT（共 3 次 503），此前 Complete 已成功、upload 已不存在。留下 `ReadyToPublish` pointer，H 和 D 均覆盖 N。新进程使用 `--read-back off`：

- `result=ok`，`reused_bytes=N`，新源 payload GET **0 字节 / 0 次**；无新 part copy、无再次 Complete。
- 对最终 key 的实际 payload GET 为 **114 次 / 939,524,762 字节 = 2N**，带真实 `versionId` pin。一次来自 S3 无 completion receipt 时的 root reconciliation，一次来自通用强制发布后验证。
- 工具输出仍为 `read_back=Disabled` / `read_back_method=NotPerformed`，不能据此判断 reconciliation 没有读回；以原始 HTTP 审计为准。这是本轮发现的观测口径限制。

原始证据：`minio-versioned-native-gap.pointer`、`minio-versioned-native-gap.snapshot.json`、[HTTP 审计](/work/resume-hash-s3-validation-wEjz4UYF/minio-versioned-native-gap.http-audit.json)、对应 first/resume stdout。源码对应：[S3 final reconciliation](../../src/storage/backends/s3/staged/published_recovery.rs)、[通用 recovered-publication 强制验证](../../src/transfer/engine/publication.rs)。

首轮脚本错误地排除了带 `x-id` / `versionId` 的 GET，报告 0 final GET 并在独立 SHA-256 步骤前断言失败；原始记录证明上述两遍完整读取。该前缀随后按 finally 正常清理，故未补到**此用例专属的独立 SHA-256 与清理前版本集合验收**。不把它算入 §4 的 8 个完整正向用例，也不把脚本错误称为生产完整性失败。

## 6. 初轮发现的生产缺陷：DXN 的 ETag 表示差异（修复前）

DXN 的非版本和版本桶均已真实执行取消、新进程恢复。两次观察到：`D_pointer=67,108,864`，`D_parts=H=75,497,472`；恢复在 Prepare 报：

```text
S3 ListParts changed a committed part receipt
Corruption / Permanent; source bytes=0; source operations=0
```

独立 5 MiB、内容为 `x` 的 multipart 探针确认：

```text
UploadPart ETag: "4f08eef4096726bff1125e1b1b734644"  （包含外层双引号）
ListParts  ETag:  4f08eef4096726bff1125e1b1b734644   （不含外层双引号）
exact_equal=false; unquoted_equal=true
```

证据：[探针日志](/work/resume-hash-s3-validation-wEjz4UYF/dxn-etag-probe.log)、`dxn-plain-stream-cancel-resume.stdout`、`dxn-versioned-stream-cancel-resume.stdout` 和相应 pointer 快照。

修复前的 receipt 指纹把 ETag 原字符串长度和字节纳入 digest；恢复验证对 ListParts receipt 集重新计算，因合法引号表示差异误拒绝。探针相同 payload、相同 part 的证据与旧源码共同支持这个根因；没有证据表明源或 part 内容损坏。当前 [receipt 实现](../../src/storage/backends/s3/staged/checkpoint_record.rs)和[恢复验证](../../src/storage/backends/s3/staged/hash_checkpoint.rs)已包含 §9 所述修复，不再代表此处的旧逻辑。

初轮行为是失败关闭，而不是错误接受损坏数据，但当时恢复可用性不达标，不能标绿。问题已第一时间报告；验证 worker 没有修改生产代码。main 随后实现带双引号的规范 receipt 表示并保留严格校验，§9 已完成两种桶复验；不是通过忽略 receipt digest 来放宽校验。

## 7. 未完成范围与验证脚本问题

- MinIO 三次及更多 owner takeover、legal hold 下 strict release 失败、撤销 hold 后清除全部旧 nonce 的 raw-H 版本：**未完成真机用例**；此前生产回归测试不替代本项。
- 真实分页超过 1000 个 pointer 版本、最大 multipart part 数、未知 marker ownership、SSE/KMS、retention、设备/服务器断电：未验证。本轮只验证普通可删除历史不留下 root；不证明物理安全擦除或服务器磁盘不预读。
- 稀疏岛屿、native 纯取消 drain、RustFS 原生 hash-intent/发布间隙、DXN 原生恢复，以及 ONTAP/StorageGRID：没有完整真机验收。
- RustFS 原生尝试中，relay 将响应 headers 转成区分大小写的普通字典，漏取小写 `content-length`，把 HEAD 长度误转发成 0。工具因此走零长度 single-copy，根本没有进入预期 multipart 故障点；不把它算作生产原生路径通过或失败。
- 一次有限复跑复用 strace 标签而读到前轮文件，导致计数重复；此前首轮有效审计和 RustFS 首次标签的有效审计不受影响。后续脚本已改按 session 隔离，但未再启动广泛矩阵。
- HTTP GET 计数器已修正 `x-id` / `versionId` 分类；发布间隙结论使用原始 HTTP 记录重新统计。relay HEAD 修正与 strace session 修正均仅发生在临时验证脚本中，不是生产修复。

## 8. 最终清理和结论

结束前独立复核（[最终清理记录](/work/resume-hash-s3-validation-wEjz4UYF/final-cleanup.log)）：MinIO、DXN、RustFS 的本轮 `data-mover-test/resume-hash-20261023-wejz4uyf/` 均为 **0 当前对象、0 version/marker、0 已使用精确 key 的 multipart upload**。MinIO 的精确-key 上传列举限制已考虑。DXN 小探针也已 abort。三个既有桶仍为 versioning Off。

以下本轮创建的临时版本桶均已清空并删除，最后 HEAD 均返回 404：

```text
data-mover-resume-hash-minio-1792721300
data-mover-resume-hash-minio-1792721605
data-mover-resume-hash-dxn-1792721631
data-mover-resume-hash-rustfs-1792721671
```

没有留下本轮 S3 transfer/relay 子进程或实验锁。所有实际成功且完成独立 checksum 的最终文件已在验收后按本轮前缀安全清理；仅保留本机确定性 fixture 和证据，不是永久生产 hash manifest。

初轮结论：完成 **8 个正向恢复＋独立 SHA-256 用例、1 个损坏记录失败关闭用例**；还观测到 read-back disabled 时发布间隙强制目的端全读和零源重读。当时 DXN 有可复现的 receipt 兼容性缺陷，不能确认整个 S3 profile 矩阵完整通过。后续 DXN 修复及新增 4 例结果见 §9；其他覆盖边界不因该修复而扩大。

## 9. DXN 带双引号 receipt 修复后复验

本节是最新结果，取代 §6 关于 DXN 流式恢复“仍失败”的历史状态。范围严格限制为 DXN 的非版本桶与本轮临时版本桶，没有重跑其他 profile 或更改共享配置。生产代码和回归测试由 main 修改；本 worker 仅只读复核、执行真机验证及更新本报告。

### 9.1 固化二进制与旧记录兼容性

旧 writer 已在构建覆盖前复制并校验：

```text
/work/resume-hash-s3-validation-wEjz4UYF/transfer_resume-before-dxn-quotes
SHA-256 = 5d999398121640d9c00f6bb20a52d77b58b37938b336f269347e51a629e4d6d9
```

修复后的 GNU 示例在 main 发出 ready 后立即复制并校验，后续不受再次构建影响：

```text
/work/resume-hash-dxn-quoted-F09Bdi0H/transfer_resume-fixed-gnu
SHA-256 = 5fdd956391f5dfde773ee438a464dee7a8fff6d3f0119f81bfa603a4a28cc51d
```

main 提供的修复后源码 fingerprint 为 `442e06c909bce2f8342d00060eb1c1819785b9acb66568477096f5d995807d3e`（含 7 项新回归）；本报告用实际二进制 SHA 标识执行版本，不拿源码 fingerprint 替代产物校验。构建后 main 的新增改动仅涉及测试/注释，未变动库生产逻辑。

只读复核确认：新 receipt digest 在相同 v1 domain 下使用带双引号的规范编码，严格验证 opaque-tag body；兼容校验保留 exact-original 和统一 bare 旧记录候选，而不是枚举任意混合引号组合或忽略 digest。协议层原始 ETag、Complete 请求 receipt 没有因此改写。

“旧→新”用例由旧 musl 真正创建并取消 transfer，保留 `DMDPTR02` pointer 中的 `DMS3CP01` v1 receipt 记录，再由新 GNU 独立进程恢复；并未重编码、补写或篡改旧 root。这里的 v1 指 backend receipt record/domain，不是无 hash 的 legacy `DMDPTR01` pointer。两例真实 `ListParts` 均返回 12 个裸 ETag，证明 quote 差异仍存在，并非服务器改行为后才通过。

### 9.2 四条恢复链路的实际结果

沿用 §3 的同一个源 inode、`N=469,762,381` 字节和独立 SHA-256。writer 约 4.5 秒取消，source block ceiling 为 `3,145,731`；恢复为 `5,242,887`，multipart part size 保持 8 MiB。所有阶段分别使用新进程和空 HOME，恢复阶段开启 pinned 读回，之后另外下载最终文件计算 SHA-256。

| 桶 / writer→reader | `D_pointer` 字节 | `D_parts` / reused 字节 | 恢复实际源 READ 字节 | READ 次数 | 与 D 的交集 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 非版本，旧 musl→新 GNU | 92,274,688 | 100,663,296 | 369,099,085 | 177 | 0 |
| 版本，旧 musl→新 GNU | 92,274,688 | 100,663,296 | 369,099,085 | 177 | 0 |
| 非版本，新 GNU→新 GNU | 83,886,080 | 100,663,296 | 369,099,085 | 177 | 0 |
| 版本，新 GNU→新 GNU | 67,108,864 | 83,886,080 | 385,876,301 | 185 | 0 |

前 3 行的实际源 `pread64` 并集恰为 `[100,663,296, 469,762,381)`；最后一行为 `[83,886,080, 469,762,381)`。每行实际读取量均等于 `N - reused_bytes`，范围并集字节数等于总返回字节，无重复读取；与工具 source budget 的字节数和操作数一致。恢复报告均为 `Resumed`、`upload_digest=Matched`，保留原 transfer identity。

四个最终文件的独立下载结果均为：

```text
bytes = 469,762,381
SHA-256 = f4c132149a2c26ba4a3eb00602a967fa1fcc459555be270cec72ba07cf039ee7
```

在人工清理测试前缀之前，四例均核对：**最终 key 只有 1 个版本、0 删除标记、0 pointer 历史、0 未完成 multipart upload**。因此不是靠事后人工删除 root 才宣称 backend release 成功。

### 9.3 证据和最终清理

旧→新证据目录：[followup.log](/work/resume-hash-dxn-quoted-F09Bdi0H/followup.log)、[results.json](/work/resume-hash-dxn-quoted-F09Bdi0H/results.json)、同目录 `*.pointer` / `*.snapshot.json` / `*.parts.json` / `*.trace-<session>.*`。

新→新证据：[fixed-fixed.log](/work/resume-hash-dxn-quoted-F09Bdi0H/fixed-fixed.log)、[fixed-fixed/results.json](/work/resume-hash-dxn-quoted-F09Bdi0H/fixed-fixed/results.json) 及对应 pointer、part receipt、strace 文件。两个执行组使用不同日志目录和 strace session，不混读初轮 trace。

所有写入只位于以下新前缀：

```text
resume-hash-dxn-quoted-20261023-f09bdi0h/
resume-hash-dxn-quoted-20261023-f09bdi0h-fixed/
```

两组都按 finally 在验收后 abort 精确 key 的残留上传、按 version ID 清理本轮前缀，再核对 0 对象、0 version/marker、0 MPU。以下临时版本桶均已删除并再次 HEAD 确认 404：

```text
data-mover-dxn-quotes-1792724330-f09bdi0h
data-mover-dxn-quotes-1792724481-f09bdi0h
```

既有 `data-mover-test` 的版本设置仍为 Off；未触及其他桶。执行持有共享 tests/performance 锁，结束后均释放，无本轮 transfer/strace 子进程。

最新结论：**DXN 本次引号兼容性问题已通过两种桶、旧记录升级恢复与新记录自恢复的 4 例真机验证**。合并初轮覆盖，共有 12 个完成独立 SHA-256 的正向恢复链路和 1 个损坏记录失败关闭用例；仍不是五套 profile 全矩阵认证，§5 的部分发布间隙验收及 §7 的其余缺口保持不变。
