# data-mover × nfs-rs 0.8.1 真实环境性能与归因报告

日期：2026-09-09（Asia/Shanghai）

## 最终结论

主项目已更新到 crates.io `nfs-rs 0.8.1`。优化前代码没有修改：正式基线独立编译固定
data-mover 提交 `d76ff5cbe1ac439ea71d16a4f158d55f310a1b45` 和 `nfs-rs 0.5.1`。

最初报告的 1 GiB `-11.0%` 不是有效的版本或代码回退结论。基准参数只设置了 transfer
engine 的 `--inflight 8`，却没有设置 NFS 后端环境变量，导致候选实际使用默认读/写并发
`4/8`，而冻结基线显式使用 `8/8`。修正 runner 后，三组都使用相同的 `8/8`：

- 当前优化路径比冻结基线快 3.2%；
- 当前 legacy 路径比冻结基线慢 7.8%；
- 当前优化路径比同为 `nfs-rs 0.8.1` 的 current-legacy 路径耗时低 10.1%。

因此不能把回退归因于 `nfs-rs 0.8.1` 本身。性能损失出现在 data-mover 对新写 API 的
逐块 durable 兼容方式：每个应用块调用一次 `write_all`，同一 mount/file handle 的调用会串行化，
并且每块独立完成 durability。data-mover 的 UNSTABLE WRITE + 批量 `commit_write_batch`
优化消除了这部分损失，并使最终 1 GiB 性能超过旧基线。

## 版本适配语义

`nfs-rs 0.8.1` 删除了 `WriteSession`、`WriteDurability`、`IoOptions` 和旧的稳定
`Mount::write` 语义。当前实现为：

- checkpointed transfer 并发发出 UNSTABLE WRITE，保存 `WriteOutcome` 和对应 payload；
- 默认累计 N=8 个 WRITE 回执后调用一次 `commit_write_batch`；
- 若 N 个回执全部为 `FILE_SYNC`，`commit_write_batch` 直接成功，不发送额外 COMMIT；
- 若 COMMIT 检测到 verifier 变化，最多重写并重提交通常批次 2 次；
- 非 checkpointed 写使用 `nfs_rs::write_all`，保持返回成功即持久化。

## 环境与方法

- 源：`10.128.56.160:/nfsrs_pnfs_b`，Linux NFSv4.1/pNFS，内核挂载 rsize/wsize 1 MiB。
- 目标：`10.128.61.201:/deep_sync_nfs`，Linux NFSv4.1，内核挂载 rsize/wsize 64 KiB。
- 应用块：1 MiB；读/写并发均为 8；每档 3 次；奇偶轮反转执行顺序。
- 计时包含复制、目标完整回读 BLAKE3、元数据处理和发布。
- 每次写入独立目标目录；结束后清理源、目标测试数据。

## 修正后的正式结果

| 大小 | 基线中位耗时 | 优化后中位耗时 | 基线吞吐 | 优化后吞吐 | 优化后变化 | 基线 RSS | 优化后 RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| 4 KiB | 5032.374 ms | 5021.678 ms | 0.001 MiB/s | 0.001 MiB/s | +0.2% | 16.0 MiB | 16.9 MiB |
| 40 MiB | 5922.797 ms | 5890.696 ms | 6.754 MiB/s | 6.790 MiB/s | +0.5% | 43.1 MiB | 63.5 MiB |
| 1 GiB | 27111.938 ms | 26273.676 ms | 37.769 MiB/s | 38.974 MiB/s | +3.2% | 57.3 MiB | 97.0 MiB |

4 KiB 的约 5 秒耗时主要是连接/关闭固定开销。40 MiB 差异很小。1 GiB 的数据面占比
足够高，批量 durability 带来可见收益，但候选 RSS 比旧基线高约 39.7 MiB。

## 三组归因矩阵

| 实现 | data-mover 路径 | nfs-rs | 1 GiB 中位耗时 | 相对冻结基线 |
|---|---|---:|---:|---:|
| baseline | 冻结 legacy | 0.5.1 | 27.112 s | 基准 |
| current-legacy | 当前 legacy，每块 `write_all` | 0.8.1 | 29.213 s | -7.8% |
| optimized | 当前 role，UNSTABLE + batch commit | 0.8.1 | 26.274 s | +3.2% |

严格意义上，baseline 与 current-legacy 的差异同时包含上游破坏性 API 变化和 data-mover 的
兼容迁移，不能只用这两组证明“nfs-rs 内核慢了 7.8%”。但 current-legacy 与 optimized 使用
相同工作树、相同 `nfs-rs 0.8.1`、相同服务器和并发，说明正确的批量集成可以追回约 11.2%
的吞吐，并超过旧版本基线。因此实际瓶颈是集成策略，不是 `0.8.1` 无法达到旧版性能。

## 内存边界

最初候选只在文件末尾 checkpoint。1 GiB 首样本 RSS 达 `1,158,276 KiB`，因为 verifier
恢复要求 payload 一直保留。该实现不可接受。

修复后每 N 个 WRITE 回执形成一个 durability batch，1 GiB RSS 中位数约 97 MiB。
payload 在本批次成功后释放，内存不再随文件大小线性增长，同时保留 verifier 变化重写能力。

## 优化建议

1. **保留低层批量写路径。** checkpointed 大文件不要退回逐块 `write_all`；继续采用 N 个
   `Mount::write` 回执配一个 `commit_write_batch`，并依赖其全 `FILE_SYNC` 零 COMMIT 快路径。
2. **分离并发窗口和 durability batch。** 新增独立 `DATA_MOVER_NFS_COMMIT_BATCH`，避免为了
   增大 COMMIT 批次同时扩大飞行中读写内存。
3. **输出有效配置。** 基准和生产启动日志应打印 engine inflight、backend read/write inflight、
   协商 rsize/wsize 和 commit batch；性能门禁应在运行前断言两侧参数相同。
4. **加入 WRITE/COMMIT 指标。** 记录 `unstable_write_count`、`file_sync_outcome_count`、
   `commit_rpc_required_batches`、`verifier_rewrite_count` 和 durability wait 时间。
5. **把批次状态更新降为 O(1)。** 直接维护 `outcome_count`、`min_offset` 和 `max_end`，避免每次
   WRITE 完成后扫描当前批次。
6. **保持 payload 有界。** 内存预算明确为 `chunk_size × commit_batch` 加读写飞行队列；禁止
   恢复成整文件末尾一次 COMMIT。
7. **拆分性能阶段。** 在端到端门禁之外，分别记录 READ、WRITE、COMMIT、完整回读、元数据、
   rename 和 CLOSE 耗时，避免固定生命周期成本掩盖数据面变化。
8. **扩大统计样本。** 低负载窗口至少运行 7 次交替测试，报告 p50/p95 和标准差，再决定是否
   调整默认 N=8。

## 证据

- 修正后 4 KiB/40 MiB：`benchmarks/nfs-latest-comparison/results/v081-corrected-small-medium.csv`
- 1 GiB 三组归因：`benchmarks/nfs-latest-comparison/results/v081-attribution-1g.csv`
- 无界批次诊断：`benchmarks/nfs-latest-comparison/results/v081-real.csv`
- Harness：`benchmarks/nfs-latest-comparison/src/main.rs`
- Runner：`benchmarks/nfs-latest-comparison/run-real.sh`

以下历史文件的候选 NFS 后端实际仍是默认 `4/8`，不得用于 N=4/8/16 调参结论：
`v081-real-batched.csv`、`v081-window4-1g.csv`、`v081-window16-1g.csv`。
