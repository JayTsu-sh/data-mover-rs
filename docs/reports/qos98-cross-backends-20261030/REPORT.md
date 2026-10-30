# QoS 多 backend 复验与 cross backend 验收报告

日期：2026-10-30，Asia/Shanghai。隔离运行：`nightly-qos-cross-20261029192047`。

**主矩阵 943 项判定通过 933 项，10 项未通过；23 条路径中 18 条全部通过。** 补充固定长样本通过 42/42 项。在本轮指定的长样本下补测均通过；主矩阵八项 128 MiB strict 端到端失败和两项 512 MiB 高配额失败仍保留，不能宣称所有文件尺寸和配额组合都通过。 所有失败、A/A 对照和复测均保留，不能用上轮七个 backend 到 Local 的 406 项通过结果替代本次 cross backend 结果。

## 四个场景的结论

| 场景 | 主矩阵结果 | 结论边界 |
|---|---|---|
| 1. 无 QoS 基准 | 23/23 路径成功测量并校验全量内容 | 指定文件、缓存条件和复制策略 |
| 2. 合理软/硬带宽与 IOPS | 138/138 硬包络、138/138 软包络通过；92/92 突发与 92/92 突发后窗口通过 | 源端请求准入语义；不证明物理瞬时峰值 |
| 3. 很高 QoS 持平 | 主矩阵 21/23 通过；两个长样本复核 2/2 通过 | 持平门槛明确取高 QoS ≥ 无 QoS 中位吞吐的 98% |
| 4. 合理 QoS ≥98% | 稳态 138/138 通过；主矩阵端到端 130/138 通过；八个长文件复核 8/8 通过 | 短文件端到端失败仍成立；扩大文件没有修复或抹除短文件失败 |

以上 943 项不是 943 次独立复制：包含同一复制的多个包络/窗口判定，以及导入性能阶段后的判定。最终主矩阵采用 270 个性能样本、115 个有限配额复制样本，导入测量与原文件逐字节核对；补充测量另计。

## 覆盖和真实环境

新增五类 backend（Local、NFSv4.1、CIFS、S3、HDFS）之间全部 **20 个有向、非对角 cross backend 组合**，再加 Local→Local、NFSv3→Local、NFSv4.0→Local，共 **23 条路径**。保留全部七种源配置到 Local 的回归。NFS cross 代表版本为 v4.1；未宣称覆盖每个 NFS 版本之间的完整笛卡尔积。S3→S3 的服务端 native copy 不在客户端源读限速范围内，未纳入这 20 个组合。

真实 runner 为 `10.131.9.11`，Ubuntu/Linux 6.8，24 vCPU、约 32 GiB 内存，本地 ext4。NFSv3/v4.1 与 S3 源在 lab source VM，NFSv4.1 与 S3 目标在独立 target VM；NFSv4.0 使用 DXN，CIFS 使用 ONTAP，HDFS 使用现有 Kerberos HA 集群和 keytab。直接调用各 Rust backend，并非 mock 或由内核挂载替代协议客户端。未改服务端配置、重启服务或清理其他 run。

每条路径先用 16 MiB 做真实复制、完整读回哈希与删除，新 warmup **23/23 通过**。第一次 warmup 因本地目标目录未创建而导致七个本地目标失败，修正目录准备后完整重跑，初次失败也归档。性能数据由随机 1 MiB 块重复完整写入，非稀疏文件；这是逻辑 payload 吞吐，不是物理盘带宽。每次计时前哈希本地 golden 文件；远端缓存来自准备和重复复制，未强制预热所有远端源，也未清空共享服务缓存。

## 复制和计时口径

- 到 Local 的七条回归保留 `default`：默认恢复、提交及复制内读回校验；复制结束后仍独立读取目标并校验全文件 BLAKE3/长度。
- 其余网络目标使用 `cross`：`AtomicReplace`，关闭复制内读回，计时包含真实 payload 传输、协议关闭/提交和原子发布；独立全量目标读回位于计时外，耗时记为 `verification_seconds`。
- **AtomicReplace 在 Local/NFS 上省略最终持久化屏障，不承诺崩溃持久性；其他 backend 仍保留各自协议的 close/commit。cross 结果不能称为默认 durable-copy 性能。** CIFS fixture 不要求受保护 checkpoint ACL。
- 任一复制的独立摘要或长度不匹配即失败，保留目标用于诊断；只有比对 golden 成功后才删除。目标清理与 golden 哈希均不计入吞吐时间。
- QoS 只约束源 payload 读；目标写、目标独立读回、元数据不扣源配额。IOPS 是应用层源 READ / Range GET / HDFS range read，不是物理磁盘 IOPS。无 QoS 完整复制的操作数由有效请求尺寸推算，高 QoS 使用真实预算计数，并检查一致性。

高配额同时启用 **1 TiB/s 与 10^9 IOPS**，保留请求 cap、并发及复制策略，关闭诊断 admission trace。主性能阶段串行运行，每路五组 baseline/high 交替顺序；两个扩展样本为 15 组，两个 arms 同样空闲 1 秒且不计时。比较各自吞吐中位数。有限限速保留完整 admission trace，主矩阵最多四个独立进程，每进程四个 Tokio worker；独立进程拥有独立 QoS group。

有限配额主文件统一 128 MiB。套件额外覆盖 soft=hard 的 strict 压力项；双限用例均为 hard=2×soft。带宽 soft 取 `min(4 或 8 MiB/s, baseline/8)`，另测 strict soft=hard 与 dual hard=2×soft；IOPS soft 取请求数量/16 的整数下界，至少 1，另测 hard=soft 和 hard=2×soft。联合配额使用兼容的带宽/IOPS 组合，源是 S3 时保留其大 GET 计费粒度。基准须超过 hard 的 125%；峰值信用按 `(hard-soft)×peak` 有限积累。准确参数和命令都在原始 JSON 与 [summary.json](summary.json)。

硬上限对 trace 的**全部准入区间**验证 `traffic ≤ hard×Δt + one_actual_request`；软包络额外允许 `(hard-soft)×peak`。没有增加百分比松弛。文件后端约按 10 ms 带宽配额拆读，S3 最大单 GET 8 MiB。末段速率锚定最后最多五秒内的实际 admission 边界，排除左端事件，并要求突发后窗口足够晚，避免低 IOPS 的整包量化伪差。**此证据不等于任意微秒物理窗口绝不超 hard；如果用户要求这种物理瞬时含义，当前应用层实现和本报告都不能据此宣称满足。**

## 主矩阵逐路径结果

有限限速文件均为 128 MiB。表中高 QoS 列为选定性能样本结果；strict 列为包含完整复制收尾的端到端达成率。dual、combined 的所有完整判定见 [checks.json](checks.json)。

| 路径 | 性能文件 MiB | 基准 MiB/s | 高 QoS / 基准 | strict 带宽 / soft | strict IOPS / soft | 四场景主矩阵 |
|---|---|---|---|---|---|---|
| cifs-to-hdfs | 128 | 98.063 | 99.900% | 99.176% | 99.145% | 通过 |
| cifs-to-local | 512 | 100.370 | 100.145% | 99.737% | 99.345% | 通过 |
| cifs-to-nfs41 | 128 | 97.993 | 101.263% | 99.139% | 98.534% | 通过 |
| cifs-to-s3 | 512 | 28.449 | 109.431% | 98.050% | 97.416% | **未通过** |
| hdfs-to-cifs | 128 | 105.479 | 99.615% | 99.904% | 99.778% | 通过 |
| hdfs-to-local | 512 | 311.256 | 101.672% | 99.246% | 99.334% | 通过 |
| hdfs-to-nfs41 | 128 | 286.393 | 99.196% | 98.193% | 99.225% | 通过 |
| hdfs-to-s3 | 128 | 37.781 | 98.855% | 97.931% | 97.272% | **未通过** |
| local-to-cifs | 128 | 106.852 | 100.342% | 99.914% | 99.488% | 通过 |
| local-to-hdfs | 512 | 315.729 | 99.757% | 99.278% | 99.114% | 通过 |
| local-to-local | 512 | 612.086 | 99.049% | 99.683% | 99.352% | 通过 |
| local-to-nfs41 | 128 | 459.506 | 116.663% | 98.982% | 99.064% | 通过 |
| local-to-s3 | 512 | 34.582 | 108.186% | 97.572% | 96.199% | **未通过** |
| nfs3-to-local | 512 | 603.321 | 100.758% | 99.606% | 99.356% | 通过 |
| nfs40-to-local | 128 | 98.863 | 99.962% | 99.717% | 99.143% | 通过 |
| nfs41-to-cifs | 128 | 107.282 | 99.789% | 99.918% | 99.922% | 通过 |
| nfs41-to-hdfs | 128 | 256.477 | 105.413% | 99.391% | 99.220% | 通过 |
| nfs41-to-local | 512 | 572.558 | 102.463% | 99.665% | 99.328% | 通过 |
| nfs41-to-s3 | 512 | 38.016 | 96.123% | 97.910% | 96.234% | **未通过** |
| s3-to-cifs | 128 | 104.686 | 99.985% | 99.383% | 99.390% | 通过 |
| s3-to-hdfs | 512 | 298.491 | 94.822% | 99.511% | 96.080% | **未通过** |
| s3-to-local | 128 | 359.015 | 101.714% | 99.634% | 99.232% | 通过 |
| s3-to-nfs41 | 512 | 514.634 | 100.362% | 99.406% | 99.426% | 通过 |

## 十项未通过判定

末两列仅适用于有限配额：`超出理想服务时间` 为实测总时长减去 bytes/soft 或 operations/soft；`最后准入后尾部` 还包含最后一次源读取本身，不能直接全算成目标提交时间。秒值由原始 trace 重算。

| 路径 | 未通过项 | 达成率 | 超出理想服务时间 秒 | 最后准入后尾部 秒 |
|---|---|---|---|---|
| cifs-to-s3 | iops-strict | 97.416% | 0.4243 | 0.4234 |
| hdfs-to-s3 | bandwidth-strict | 97.931% | 0.5727 | 0.4125 |
| hdfs-to-s3 | iops-strict | 97.272% | 0.4488 | 0.4488 |
| local-to-s3 | bandwidth-strict | 97.572% | 0.7368 | 0.6162 |
| local-to-s3 | iops-strict | 96.199% | 0.6322 | 0.6330 |
| nfs41-to-s3 | bandwidth-strict | 97.910% | 0.5749 | 0.4724 |
| nfs41-to-s3 | 高 QoS / 基准 | 96.123% | — | — |
| nfs41-to-s3 | iops-strict | 96.234% | 0.6311 | 0.6313 |
| s3-to-hdfs | 高 QoS / 基准 | 94.822% | — | — |
| s3-to-hdfs | iops-strict | 96.080% | 0.6528 | 0.6537 |

所有 23 路的 dual/combined 有限配额端到端与稳态门槛均通过：bandwidth-dual 端到端最低 99.737%，iops-dual 最低 107.716%，combined 两资源最低约 103.93%。八项有限配额失败集中在额外的 soft=hard strict 项，不能外推为所有双限 QoS 都失败，也不能据双限通过宣称 strict 通过。

这些失败使主矩阵和对应路径测试脚本保持非零退出，不会输出全绿。五个不全通过路径为 Local→S3、NFSv4.1→S3、CIFS→S3、HDFS→S3、S3→HDFS。

## 根因排查与复核

**一、八项有限配额失败：收尾开销与短文件配额服务时间之比。** 源 admission 稳态均达到 98%，全部 hard/soft 包络通过；失败发生于包含最后源读、S3 分片排空/完成上传、HDFS close 及发布的端到端时间。128 MiB 的大请求 IOPS 用例只有约 16 秒服务时间，额外约半秒便足以低于 98%。不提前发源请求、不省略必要关闭/提交的前提下，源读限速器不能凭空抵扣收尾时间。若理想源服务时间是 L、额外开销 C，整体达成率为 L/(L+C)，达到 98% 需 L≥49C。C 会随文件尺寸、服务端状态及重试变化，并非严格常数。因此测试修正为分别判定稳态和端到端，并补充较长文件；**原短文件的端到端要求依然未满足，不能称为已修复。**

为区分源调度与目标收尾，构建单独诊断二进制，仅记录 HDFS writer.close、S3 EOF 后分片排空和 CompleteMultipartUpload 用时；正式验收二进制不变。诊断数值如下（不参与正式通过计数）：

- `local-to-s3-0`: s3_final_parts_ns=480.683 ms, s3_complete_ns=88.855 ms
- `local-to-s3-1`: s3_final_parts_ns=301.602 ms, s3_complete_ns=94.635 ms
- `nfs41-to-s3-0`: s3_final_parts_ns=422.739 ms, s3_complete_ns=131.896 ms
- `nfs41-to-s3-1`: s3_final_parts_ns=336.560 ms, s3_complete_ns=112.711 ms
- `s3-to-hdfs-0`: hdfs_close_ns=131.538 ms
- `s3-to-hdfs-1`: hdfs_close_ns=147.054 ms

固定依赖 `hdfs-native@695fc47e9f88804e39bba1d30bf128e66da9b8d6` 的 `FileWriter.close` 在 NameNode complete 返回 false 时从 500 ms 开始退避。这是与关闭尾延迟相符的候选机制；当前两次探针的 close 约 132/147 ms，未直接观察到 500 ms；**没有记录 NameNode false/retry，不能确认原失败额外延迟由该重试触发**。原 trace 的约 654 ms 尾部成立，其底层具体调用仍未完全定位。S3 写入则必须等最后分片被接收，再完成 multipart/publish。未通过绕过这些协议步骤、异步提前报成功或免费首包来制造 98%。

**二、两个高 QoS 失败：短样本重复性不足，不能直接归因限速等待。** 先对初始失败的全部六条路径运行完全相同配置的无 QoS A/A 对照；B/A 中位比为 Local→S3 98.453%、Local→HDFS 103.925%、NFSv4.1→S3 107.412%、CIFS→S3 109.442%、S3→NFSv4.1 107.784%、S3→HDFS 94.378%。在没有 QoS 差异时，也超过 2% 容差。因此长样本是必要复核，但不能据此把所有退化都归因环境。

准入专项探针测得 NFSv4.1→S3 在 512 MiB 高配额时 515 次准入、**0 次等待、累计 0.266 ms**；S3→HDFS 为 64 次、**0 次等待、累计 0.090 ms**。这排除了限速等待作为这两次探针的大幅退化原因；尚不足以单独排除整个 SDK/流水线或外部服务波动。512 MiB 五组和进一步 15 组复验仍未通过的记录全部保留，不以单次探针代替性能门槛。

**三、确定并修正的验收工具问题。** 创建遗漏的 Local warmup 目标目录；使用独立 source_kind 而非路由名识别 S3 联合限额；baseline 导入绑定二进制和完整配置 SHA-256，并核对样本数与计时外空闲；复测配置 guard 仅允许扩大性能文件；增加完整远端目标读回、摘要一致后删除，以及环境变量/URL 凭据脱敏；清理 helper 验证 URL 路径中的精确 run ID，并限定删除该 run 子树。

**四、分片优化候选被真实环境否决，已撤回。** 根据 EOF 后约 302～481 ms 分片排空、约 89～132 ms multipart 完成耗时，试验仅对非恢复大文件缩小均匀分片，保留最大内存和容量边界。1 MiB 末片候选在 Local→S3 的 strict IOPS 对照中由 97.461% 降至 96.217%；调整为按常用源读取粒度留约 2 MiB 末片后，四路结果仍退化：

| 路径 | 原版 strict IOPS | 2 MiB 候选 | 处理 |
|---|---|---|---|
| cifs-to-s3 | 97.005% | 96.699% | 撤回 |
| hdfs-to-s3 | 97.394% | 96.829% | 撤回 |
| local-to-s3 | 97.392% | 94.770% | 撤回 |
| nfs41-to-s3 | 96.955% | 96.428% | 撤回 |

分片大小的单元回归可以通过，但这不能证明端到端性能改善。实际额外分片、流水线/服务端行为未因调整末片自动改善；不把尚未证实的更深机制当成定论。两组候选和其单元测试/ADR 改动均已撤回，最终正式二进制恢复为本轮原验收 SHA-256；失败数据、构建/测试日志和拒绝补丁随归档保留。**短文件 strict 端到端失败仍未修复。**

本轮复用了上一轮的调度 deadline、软信用补充、try_lock 快路径、S3 并发与逐次重试计费、实际 IO future 内准入等实现修复。**最终未保留新的生产分片/QoS 算法改动，也未修改服务端配置。** 临时探针改动已逐字节恢复，正常重建二进制与原验收二进制 SHA-256 一致；不把延长测试文件称为生产性能修复。

## 固定长样本补充

在看见结果前固定两条性能复核路径及尺寸：NFSv4.1→S3 2 GiB、S3→HDFS 12 GiB，各五组、交替顺序、两个 arms 同等计时外空闲 1 秒，不启用 admission trace。新文件尺寸用于延长真实传输时间；独立读回仍全量进行。精确计划见归档 `duration-validation/plan.json`。

| 路径 | MiB | 无 QoS MiB/s | 高 QoS MiB/s | 比率 | 基准中位时长 秒 | 结果 |
|---|---|---|---|---|---|---|
| nfs41-to-s3 | 2048 | 36.225 | 36.202 | 99.936% | 56.54 | 通过 |
| s3-to-hdfs | 12288 | 221.902 | 218.520 | 98.476% | 55.38 | 通过 |

八项有限配额复核将文件从 128 MiB 增至 512 MiB，**soft/hard、chunk cap、复制策略与原失败完全相同**；不随文件扩大提高 IOPS 限额。不同路径最多三个进程并行，同一路径的用例串行，且在全部性能测量完成后执行。主矩阵并发最多四路、补测最多三路，因此这不是严格的单变量对照；不能将补测通过单独归因于文件扩大，更不能称为生产代码修复。

| 路径 | 用例 | 原 soft（bytes/s 或 IOPS） | 端到端达成率 | 结果 |
|---|---|---|---|---|
| cifs-to-s3 | iops-strict | 8 | 99.057% | 通过 |
| hdfs-to-s3 | bandwidth-strict | 4952032 | 98.394% | 通过 |
| hdfs-to-s3 | iops-strict | 4 | 98.173% | 通过 |
| local-to-s3 | bandwidth-strict | 4532773 | 98.453% | 通过 |
| local-to-s3 | iops-strict | 4 | 98.886% | 通过 |
| nfs41-to-s3 | bandwidth-strict | 4982843 | 98.880% | 通过 |
| nfs41-to-s3 | iops-strict | 8 | 98.959% | 通过 |
| s3-to-hdfs | iops-strict | 1 | 99.717% | 通过 |

在本轮指定的长样本下补测均通过；主矩阵八项 128 MiB strict 端到端失败和两项 512 MiB 高配额失败仍保留，不能宣称所有文件尺寸和配额组合都通过。 长样本单列于 [duration-checks.json](duration-checks.json)，没有并入/替换主矩阵的十项失败。12 GiB 准备首次超过 300 秒，保留超时记录，延长准备等待上限至 900 秒后继续；未修改 QoS 配置或通过阈值，准备失败不计为性能样本。

## S3 带宽容忍与请求压力专项

根据最新要求，S3 带宽允许一定偏差，优先避免为了贴合带宽曲线拆小 GET/分片、增加存储请求。**具体百分比容差尚未确认，本次保留原 98% 判定，不将原失败改成通过。** 带宽偏差与 payload 请求数、IOPS、hard 准入包络分别报告；普通带宽波动容忍不能自动扩大 IOPS 或免费增加请求。硬包络仍使用实际整包粒度，没有新增百分比余量。

为避免把应用层记账当作实际请求，另构建独立 SDK `before-transmit` 拦截器，记录每次实际 HTTP 尝试（包括重试），按 Range GET、UploadPart、元数据和独立验收读回分类。测试文件 64 MiB，每次全量哈希；未把探针加入正式二进制。九次复制及 **44/44 项专项检查通过**：

| 路径 | 用例 | 实际 MiB/s | payload 请求次数 | 每请求 MiB | 独立读回 GET |
|---|---|---|---|---|---|
| s3-to-local | baseline | 501.510 | 8 | 8 | 计时源 GET |
| s3-to-local | high | 483.708 | 8 | 8 | 计时源 GET |
| s3-to-local | bandwidth-strict | 3.994 | 8 | 8 | 计时源 GET |
| s3-to-local | bandwidth-dual | 5.319 | 8 | 8 | 计时源 GET |
| s3-to-local | bandwidth-low | 1.000 | 8 | 8 | 计时源 GET |
| s3-to-local | iops-strict | 7.973 | 8 | 8 | 计时源 GET |
| local-to-s3 | baseline | 33.908 | 8 | 8 | 32 |
| local-to-s3 | high | 35.586 | 8 | 8 | 32 |
| local-to-s3 | bandwidth-strict | 3.862 | 8 | 8 | 32 |

S3 源端在无 QoS、高 QoS、4 MiB/s strict、4/8 MiB/s dual、1 MiB/s strict 以及 1 IOPS 六种配置中，都只有 **8 次、每次 8 MiB 的 GET**。1 MiB/s 的低带宽配置通过延迟整包请求限速，而非缩小 GET；其请求间隔约八秒，软窗口记录取 16 秒以容纳整包量化，未修改原矩阵窗口或判定。Local→S3 三种配置均为 8 次 8 MiB UploadPart；每次另有 32 次 2 MiB GET 用于独立验收，单独计数，不能混入业务源 IOPS。HEAD、bucket-versioning、multipart 创建/完成等元数据也单独保留。

本专项用于验证请求压力；64 MiB 高 QoS 只有单次、极短测量，S3→Local 高/基准约 96.45%，**没有以专项通过替代场景三的重复性能验收**。当前 QoS 约束源 payload 请求，不能据此宣称目标写、元数据、独立验收或所有 SDK 重试共享同一全局 API 请求上限。请求压力结果仅覆盖本轮观察，不外推无故障下结果为所有重试模式的证明。

同时将 `s3_payload_requests_no_inflation` 加入后续矩阵的 S3 bandwidth-strict、bandwidth-dual、combined 用例。对本轮四条 S3 源路径的历史实测离线补判 **12/12 通过**，见 [额外请求计数判定](request-pressure-existing.json)，它们不追加进历史 943 项；下次相同矩阵会多 12 项。实际 SDK 计数原始日志、专项环境、44 项判定和汇总在归档 `pressure/`。

S3 原 run 与专项 run 的源/目标共四个精确 prefix 均通过对象、历史版本/删除标记、未完成 multipart 清理与空目录审计，当前两个桶未启用 versioning。初次清理错误地把自定义 S3 URL 的 `bucket.host` 当 DNS，已修正清理解析并完成重试，失败日志保留，未重跑或更改九次测量。详见 [S3 清理审计](s3-cleanup.json)。

## 证据、自动化与复现

[完整原始归档](raw-evidence.tar.gz) 包含原始 survey、六条路由的 A/A、512 MiB 和 15 组扩展测量、23 路最终测量/trace、固定长样本、准入/收尾/实际 SDK 请求三种探针、准备及清理日志。导入 baseline 的副本可与其原始来源逐字节核对；不能把副本当成新增独立样本。归档还保留实际运行的 Python 控制脚本、源码快照和验证日志。已扫描并脱敏认证信息，排除私有端点配置、keytab、票据缓存及测试大文件。另保留五个原 fixture 与一个专项 fixture 的 1 MiB 原始数据块，可按 manifest 重复重建本轮实际 fixture，并独立重算全文件 BLAKE3。

- [环境与二进制](environment.json)、[baseline 来源](baseline-origins.json)、[初始公开矩阵参数](matrix.json)。验收二进制 SHA-256：`d7c04f29a868a388e9b3dd9ca8fc470f684ca3f8eab7100b637ead4e5ffa9156`。
- [全部主判定](checks.json)、[长样本判定](duration-checks.json)、[逐测量 CSV](measurements.csv)、[分路径汇总](summary.json)、[全部性能尝试历史](attempt-history.json)。
- [独立证据审计](evidence-audit.json)：重算 943 项主判定、42 项长样本判定与 44 项 SDK 请求压力判定，并补判 12 项原矩阵 S3 请求计数，验证所有摘要、baseline 二进制/配置绑定、归档逐文件 SHA-256，结果与原始判定一致；复现失败不被审计“通过”改写。
- [自动化验证](validation.json)：Rust 2075 通过、9 忽略；Python 16 通过；Clippy `-D warnings`、cargo fmt、架构依赖检查通过。既有 binrw future-incompatibility 提示保留。忽略项需要各自专用环境，不作为本次实测替代。
- [源码清单](source-manifest.json) 标识当前 dirty working tree 及之前 QoS 修复，未提交或推送；[清理结果](cleanup-status.json) 记录隔离数据删除情况；[私有配置清理](private-cleanup.json) 确认本 run 的 9 份私有配置已删除、无残留 fixture；[SHA256SUMS](SHA256SUMS) 校验报告附件；[归档审计](archive-audit.json) 逐项验证原始归档 1793 个文件的 SHA-256 及凭据扫描。

复现入口：

```bash
cargo build --release --locked --features qos-diagnostics \
  --example backend_qos_acceptance --example backend_qos_cleanup
# 按 lab 文档准备隔离 run、私有 sources/destinations 和既有认证环境。
python3 tests/lab/run-cross-backend-qos.py \
  --config /private/endpoints.private.json --root /isolated/local/run \
  --binary /path/backend_qos_acceptance --stage all --repeats 5 --workers 4
# 非零退出表示仍有失败。长样本/诊断的本轮精确控制脚本随原始归档提供。
# 完成后执行标准 cleanup-run.sh，并清理该 run 的 CIFS/NFSv4.0 子树。
```

工具入口为 [cross runner](../../../tests/lab/run-cross-backend-qos.py)、[矩阵生成](../../../tests/lab/cross_qos_matrix.py)、[公共生命周期测量器](../../../tests/lab/tools/backend_qos_acceptance.rs)、[lab 说明](../../../tests/lab/README.md)。结论仅覆盖本轮单文件、指定配额、协议服务与复制策略；不外推所有文件大小、冷缓存、多文件/混合负载、物理设备瞬时峰值或所有 backend 版本组合。
