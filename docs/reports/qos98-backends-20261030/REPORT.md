# 多后端带宽 / IOPS QoS 真实环境验收与优化报告

日期：2026-10-30（Asia/Shanghai）。隔离运行：`nightly-qos98-20261029230541`。

按下述源端请求准入语义，最终验收：**406/406 项通过**，覆盖 5 类 backend、7 个协议配置。门槛为 **98%**；历史 95% 结果不作为本轮通过依据。

## 验收边界与判定方法

实际调用 Rust Local、NFS、CIFS、S3、HDFS backend；源端分别读取真实文件系统或服务，目标统一为 runner 的 Local/ext4。不是内存 mock、内核挂载模拟或完整源目标笛卡尔积。完整复制保留默认提交、持久化和内容校验，计时包含这些开销；随后独立校验目标长度与全文件 BLAKE3。补充测试是串行 64 KiB 源端读取并同步计算 BLAKE3。

QoS 统计的是源端应用层读取：Local read、NFS/CIFS READ、S3 Range GET、HDFS 客户端 range read。元数据请求、目标写入、目标读回校验不扣源端配额。这里的 IOPS 不等于物理磁盘 IOPS；S3 也不等于 HTTP 以外的物理存储操作。完整复制的无 QoS IOPS 按有效请求尺寸推算，高 QoS 使用真实配额操作计数并检查二者请求数相同；串行补充测试直接计数实际源读取。

**硬上限的验证粒度必须明确**：对准入 trace 的全部区间校验 `traffic <= hard × interval + one_actual_request`；软上限再允许有限信用 `(hard-soft) × peak_duration`。末段 5 秒稳态上界为 `soft + one_actual_request / 5s`，所以表中可出现极小的 100% 以上读数；这仍是单请求量化边界。没有给上限增加任何百分比容差。单次 READ/GET 是原子计费单位，最后一个区间左端事件不重复计入速率。文件后端按不超过约 10 ms 的带宽配额拆分读取，S3 保留有界 GET 尺寸，最大 8 MiB。因此本报告证明请求准入层的包络和硬速率间隔，**不宣称任意微秒窗口的网卡/磁盘物理峰值绝不超限**；若“硬 QoS 绝对不能超”要求这种物理瞬时保证，本轮不能据此判为满足。

测试文件由随机 1 MiB 数据块循环完整写入并 fsync，非稀疏文件。源已预热，不清理共享服务器缓存；报告为逻辑 payload 吞吐。性能阶段串行执行，5 组 baseline/high 交替先后顺序，比较各自中位数。高 QoS 同时设置 1 TiB/s 带宽与 10^9 IOPS，保持有效请求尺寸及操作数相同，并关闭只用于诊断的准入 trace。有限限速用例保留完整 trace。较长性能文件与有限限速文件分开，防止为延长计时而改变限速配置。

合理带宽通常为 soft 4 MiB/s、hard 8 MiB/s，另测 soft=hard=4 MiB/s；大请求 IOPS 按文件请求数/16 取整，hard=2×soft，另测相等硬/软值。基准须超过 hard 的 125%。带宽 peak 至少覆盖两个请求；IOPS peak 为 2 秒。双限先空闲补足有限信用，必须观察到突发，随后末尾至少 5 秒稳态降至 soft；稳态窗口还必须晚于两倍 peak。补充用例为 strict 128 IOPS、soft/hard 128/256 IOPS，读取 128 MiB。

四个场景分别对应：无配额 baseline；reasonable strict/dual；高配额 high；合理配额完整操作平均和末段稳态均达到 soft 的 98%。双限允许初始平均高于 soft，但末段不允许长期超出。带宽/IOPS 同时启用时额外验证兼容的联合配额。所有成功复制或读取均验证全量内容。

| 后端 | 有效最大请求 bytes | 限速文件 MiB | 大请求 soft/hard IOPS | 带宽 dual peak 秒 |
|---|---:|---:|---:|---:|
| local | 2097152 | 64 | 2/4 | 1 |
| nfs3 | 1048576 | 64 | 4/8 | 0.5 |
| nfs40 | 1048576 | 128 | 8/16 | 0.5 |
| nfs41 | 1044480 | 64 | 4/8 | 0.5 |
| cifs | 1048576 | 64 | 4/8 | 0.5 |
| s3 | 8388608 | 128 | 1/2 | 4 |
| hdfs | 2097152 | 64 | 2/4 | 1 |

联合限速：文件后端 soft/hard 为 4/8 MiB/s 与 50/100 IOPS；S3 为 8/16 MiB/s 与 1/2 IOPS。联合 peak 均为 2 秒。每个 case 的精确配额、实际请求数、持续时间和稳态值见 measurements.csv，完整命令见原始归档。

## 最终结果：完整复制

| 后端 | 性能文件 MiB | 无 QoS MiB/s | 高 QoS MiB/s | 高 QoS / 基准 | strict 带宽达成率 | strict IOPS / 达成率 |
|---|---:|---:|---:|---:|---:|---:|
| Local / ext4 | 512 | 609.719 | 611.318 | 100.262% | 99.445% | 2 / 99.422% |
| NFSv3 / Linux | 512 | 592.861 | 604.678 | 101.993% | 99.457% | 4 / 99.441% |
| NFSv4.0 / DXN | 128 | 97.445 | 98.060 | 100.631% | 99.688% | 8 / 99.337% |
| NFSv4.1 / Linux | 512 | 567.911 | 570.440 | 100.445% | 99.649% | 4 / 99.637% |
| CIFS / ONTAP | 512 | 100.681 | 100.721 | 100.040% | 99.544% | 4 / 99.534% |
| S3 / RustFS | 128 | 354.724 | 357.497 | 100.782% | 99.727% | 1 / 99.253% |
| HDFS / Kerberos HA | 512 | 314.836 | 323.199 | 102.656% | 99.576% | 2 / 99.509% |

| 后端 | 双限带宽稳态 / soft | 双限 IOPS 稳态 / soft | 联合带宽稳态 / soft | 联合 IOPS 稳态 / soft |
|---|---:|---:|---:|---:|
| Local / ext4 | 100.000% | 99.998% | 100.000% | 100.002% |
| NFSv3 / Linux | 100.000% | 100.000% | 100.000% | 100.002% |
| NFSv4.0 / DXN | 100.000% | 100.000% | 100.000% | 100.000% |
| NFSv4.1 / Linux | 100.000% | 100.000% | 100.001% | 100.003% |
| CIFS / ONTAP | 100.000% | 100.001% | 100.000% | 100.003% |
| S3 / RustFS | 100.000% | 100.000% | 100.000% | 100.000% |
| HDFS / Kerberos HA | 100.000% | 100.000% | 100.000% | 100.002% |

## 最终结果：64 KiB 串行读补充压力

有限限速用例每后端实际读取 2048 次；性能比较使用下表文件尺寸。表中 strict 达成率包含源端读取、协议耗时与全量哈希。

| 后端 | 性能文件 MiB | 基准 IOPS | 高 QoS / 基准 | strict 128 实测 IOPS | strict 达成率 | dual 末段 IOPS |
|---|---:|---:|---:|---:|---:|---:|
| Local / ext4 | 512 | 21748.371 | 98.162% | 127.9746 | 99.980% | 128.0000 |
| NFSv3 / Linux | 512 | 5933.639 | 100.575% | 127.9655 | 99.973% | 128.0000 |
| NFSv4.0 / DXN | 128 | 1022.389 | 105.751% | 127.9837 | 99.987% | 128.0000 |
| NFSv4.1 / Linux | 512 | 3563.530 | 105.025% | 127.9649 | 99.973% | 128.0000 |
| CIFS / ONTAP | 128 | 861.409 | 98.179% | 127.8095 | 99.851% | 128.0001 |
| S3 / RustFS | 128 | 779.742 | 98.699% | 127.9791 | 99.984% | 128.0000 |
| HDFS / Kerberos HA | 512 | 1866.574 | 102.446% | 127.9688 | 99.976% | 128.0000 |

## Local 完整复制补充

另外用 64 MiB 文件复验原先较紧的配额，保留完整复制的持久化、发布与默认校验开销：

| 配额 | 实际平均 | 配额达成率 | 结果 |
|---|---:|---:|---|
| 16 MiB/s，soft=hard | 15.7415 MiB/s | 98.384% | 通过 |
| 128 IOPS，soft=hard，64 KiB READ | 126.9879 IOPS | 99.209% | 通过 |

两项的硬/软包络与稳态 98% 门槛也通过，数据位于归档的 `supplement/`。

最终所有原始判定见 [checks.json](checks.json)，测量见 [measurements.csv](measurements.csv)，分后端汇总见 [summary.json](summary.json)。未通过项如下：

无未通过项（以上述请求准入语义为限）。

## 不达标项与根因、修复

1. **合理配额只能达到旧 95% 门槛，无法稳定达到 98%。** 计时器毫秒级取整、每次在 IO 结束后重新完整等待，以及阻塞池返回延迟会逐次累积。修复为依据实际准入时间计算 deadline，实际 IO 消耗的时间抵扣下一间隔；较长等待用 Tokio，最后不超过 2 ms 用阻塞线程 OS sleep，提前最多 200 µs 醒来并协作让出，直至 deadline。中途等待也补充软信用，防止另一资源限速时少补 token。实际释放后重新锚定硬间隔，禁止迟唤醒后的追赶突发；间隔按纳秒向上取整。最终 Local 完整复制补充用例在 16 MiB/s 与 128 IOPS 下分别达到 98.384% 与 99.209%；原始数据见归档 `supplement/`。

2. **很高 QoS 下仍有明显调度开销。** 无竞争异步 Mutex 仍消耗 Tokio cooperative budget，第 128 次迫使任务让出；已用不绕过 FIFO 等待者的 try_lock 快路径避免。不可变请求量和 strict 最大服务间隔预先缓存；仅当最大的合法请求也已经等待足够时间，才跳过浮点 token/deadline 运算，保留计数、取消和实际释放时间。已有回归证明不会合批放行或让小请求多等。

3. **微秒剩余间隔被异步调度放大。** 一个 4 µs deadline 的首次 poll 会返回 Pending，即使剩余工作极小也发生运行时交接；独立回归修复前失败、修复后通过。对初始剩余不超过 8 µs 的间隔采用有界 CPU 等待，另有真实 wall-clock 上界防止暂停时钟死循环；较长等待仍走原异步路径。代价是每次这种短等待最多占用运行时线程约 8 µs，取消在该边界后立即重新检查；不以提前释放换取吞吐。

4. **S3 高 QoS 退化。** 原代码一旦启用 QoS 就将 Range GET 窗口强制降到 1；另外每 GET 重建/覆盖 SDK 重试配置。恢复与基准一致的并发窗口，在连接时缓存禁用 SDK 自动重试的专用客户端。QoS 路径由应用层逐次重试并独立扣费，普通路径仍使用原客户端。并发回归、真实 wire retry 计数和最终 S3 实测共同验证。

5. **提前拿配额、随后批量发 IO。** 先在生产队列循环里等待配额，再统一 poll 队列，会让“准入 trace 看起来均匀、实际 READ/GET 却集中启动”。S3、NFS、CIFS、HDFS 改为纯函数规划请求大小与偏移，再在 IO future 内、实际协议调用之前取得配额。NFS 2 IOPS 回归在修复前集中发出请求，修复后实际 cursor 调用间隔至少 500 ms；S3 测试同时验证并发窗口不退化和真实 GET 调度间隔，重试也经过配额。

6. **测量伪差与环境准备失败。** 早期固定五秒窗口在 1 IOPS、大 GET 下有整包量化误差；过短 64 MiB 双限用例还将初始突发纳入“稳态”。改为基于实际 admission 边界计算窗口、不重复左端事件，并要求足够长的突发后区间；64 KiB IOPS 用 128 MiB。完整复制约百毫秒的样本曾出现 2%～5% 中位数波动，保留失败记录，另用更长文件及生产成本的无 trace 高配额计时复验；不能将共享环境所有波动都归因代码修复。CIFS 128 MiB 的最终短样本曾为 97.531%，512 MiB、5 组交替复核为 100.040%；这里只接受后者的长样本验收，不声称已证明所有 128 MiB 短操作都无退化。NFSv4.0 准备文件曾返回 Capacity，测试清理本 run 的失败传输残留与旧 fixture 后重建，避免残留、原文件与临时替换文件同时占用容量；未清理其他运行的数据。服务端未提供容量/配额监控，因此只确认 Capacity 返回及清理后恢复，不能进一步断言是全盘、租户还是目录配额耗尽。HDFS 首次认证需要匹配已部署的 HA nameservice 与服务 principal，这是客户端配置修正。CIFS fixture 用 verified AtomicReplace，避免要求源服务器支持受保护 checkpoint ACL；计时的源到 Local 复制策略没有放宽。

对应实现：共享 [QoS scheduler](../../../crates/storage-core/src/runtime/qos.rs)、[等待策略](../../../crates/storage-core/src/runtime/qos/wait.rs)、[S3 source](../../../crates/backend-s3/src/backend/source.rs)、[NFS source](../../../crates/backend-nfs/src/backend/source.rs)、[CIFS pipeline](../../../crates/backend-cifs/src/backend/read_pipeline.rs)、[HDFS source](../../../crates/backend-hdfs/src/backend/source.rs)。

## 证据、复现与限制

测试机器、端点和二进制 SHA-256 见 [environment.json](environment.json)，源码校验和见 [source-manifest.json](source-manifest.json)。性能基准先串行测量；有限配额阶段每批最多 4 个独立进程，汇总速率远低于基准容量，固定每进程 4 个 Tokio worker。不同阶段使用同一个最终二进制，并在导入 baseline 时检查 SHA-256。NFSv4.0 和 S3 的两个阶段共用 128.bin，前一阶段删除后显式重新 seed。

完整原始数据、每次命令、完成事件、准入事件、失败历史及阶段日志保存在 [raw-evidence.tar.gz](raw-evidence.tar.gz)。最终数据位于归档的 `validated-survey/`、`validated-small-survey/`、`validated-full/`、`validated-small/`；CIFS 完整复制以更长样本的 `validated-cifs-long-full/` 为最终验收，NFSv4.0 小 IO 以清理容量残留后重新准备的 `validated-nfs40-small-final/` 为最终验收。原始失败和新测量均保留；`release-*` 是短样本复验失败记录，`stable-*` 是最后一次微秒等待修复前的长样本诊断。更早 `final/` 的 266 项通过记录也保留，不能替代最终二进制验收。归档不含凭据配置、keytab、票据缓存或未脱敏认证调试日志。

最终数据另经 [独立证据复核](evidence-audit.json)：140 个性能计时样本、49 个有限限速样本，另加 2 个 Local 完整复制补充。重新计算全部最终包络与 98% 门槛、核对同尺寸全文件摘要一致性、最终二进制绑定和归档 SHA-256，均通过。

自动化与回归结果见 [validation.json](validation.json)。Rust 2075 通过、9 忽略；Python QoS 校验器 10 通过；Clippy、格式与架构依赖检查通过。忽略项需要各自专用环境；本报告的真实后端验收由独立 release runner 完成。修复前失败和修复后通过的核心回归日志同时归档，覆盖调度预算、微秒等待、NFS 实际发起以及 S3 窗口/配额发起时机。

运行隔离与清理结果见 [cleanup.log](cleanup.log)：已执行标准 prepare-run/cleanup-run 生命周期，并额外处理 DXN NFSv4.0、ONTAP CIFS 的本 run 命名空间。未更改服务配置、重启服务或操作其他 run 的数据。

复现入口（私有配置必须由环境另行提供）：

```bash
cargo build --release --locked --features qos-diagnostics --example backend_qos_acceptance
python3 tests/lab/run-backend-qos.py --config /private/qos.json \
  --root /isolated/local/run --binary /path/backend_qos_acceptance \
  --phase survey --performance-size-mib 512 --output /isolated/local/run/survey
python3 tests/lab/run-backend-qos.py --config /private/qos.json \
  --root /isolated/local/run --binary /path/backend_qos_acceptance \
  --phase final --baseline /isolated/local/run/survey \
  --output /isolated/local/run/final
# 补充小 IO：先 --phase stress --measure-only，再 --phase stress --baseline <survey>
# 同名 source 在上一阶段 cleanup 后，须重新 seed，再运行下一阶段；最后执行 lab cleanup-run.sh
```

配置、环境变量、不同 profile 的性能文件尺寸覆盖、容量受限时删除旧 seed 和清理步骤见 [lab README](../../../tests/lab/README.md)。结论限于本次硬件、预热条件、配额及请求粒度；不外推冷缓存、所有 QoS 参数、物理设备瞬时峰值、S3 server-side copy 或全部源目标组合。
