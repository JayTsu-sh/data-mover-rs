# CIFS 策略验证：FAS2750，2026-09-16

## 环境与方法

- 源 LIF：`10.128.61.200`；目标 LIF：`10.128.61.201`。
- 共享：`ontap_lisaauto_cifs`；只创建独立 UUID 命名的测试文件，不修改 ONTAP 配置。
- 新 CIFS backend 使用 JayTsu-sh/smb-rs `master` 的固定提交
  `d8291b3a3157b026074ab6b0f305dfd99af17f1e`。
- release 构建，Tokio 4 工作线程，单文件任务，源预取和目标写入各最多 8 路。
- 每个用例执行 local → CIFS → CIFS（双 LIF）→ local，均启用读回校验并比较 BLAKE3。
- 主矩阵使用固定内容；补充矩阵使用 BLAKE3 XOF 生成的非均匀内容，以检测错位/重复块。
- **这是功能验证，不是 StorageEnum 性能对比。** 下列为一次运行的端到端观察值，
  包含校验，无预热、多轮统计或缓存控制，不用于推导性能提升幅度。

## 主矩阵：CIFS → CIFS

| 策略 | 文件大小 | 单次耗时（包含读回校验） | 结果 |
|---|---|---:|---|
| Checkpointed | 4 KiB | 0.013 s | Passed |
| Checkpointed | 4 MiB | 0.361 s | Passed |
| Checkpointed | 40 MiB | 3.058 s | Passed |
| Checkpointed | 64 MiB + 1 B | 4.636 s | Passed |
| Checkpointed | 1 GiB | 72.498 s | Passed |
| AtomicReplace | 4 KiB | 0.021 s | Passed |
| AtomicReplace | 4 MiB | 0.372 s | Passed |
| AtomicReplace | 40 MiB | 2.868 s | Passed |
| AtomicReplace | 64 MiB + 1 B | 4.603 s | Passed |
| AtomicReplace | 1 GiB | 72.182 s | Passed |

4 KiB 跳过单源块恢复；4 MiB、40 MiB 跳过阈值以下恢复；64 MiB+1、1 GiB
在 Checkpointed 下建立检查点。AtomicReplace 全部不建立恢复记录。
整个主矩阵的上传、跨 LIF 复制、回传均通过（30 次传输）。

最新代码再以非均匀内容复跑两种策略 × 4 KiB、4 MiB、40 MiB、64 MiB+1，
24 次传输全部通过，总用时 42.99 s。测试后按本轮 UUID/路径哈希核查，远端遗留文件为 0。

## 恢复与失败路径

- 在源 LIF 写入并 FLUSH 前缀及 checkpoint，关闭第一连接，在另一 LIF 重新认领、
  读取记录进度、补写、校验及发布：通过。
- 该实测是跨连接/双 LIF 恢复，**不是断电或 SIGKILL 实测**。
- 单元测试覆盖 checkpoint 后稀疏尾部、恢复认领、写入失败/取消、FLUSH 失败不注册恢复、
  发布完成后的清理失败重试及最终文件保留。
- 预取测试分别约束 chunk、byte、operation 三种预算；写入测试验证并发、等待输入时
  已发出请求仍能前进，以及 FLUSH 前所有写入完成。

## 调试构建观察

单线程 Tokio + debug 构建的 16 MiB CIFS → CIFS 用例曾返回 SMB `OutcomeUnknown`。
相同大小改用 4 线程后两种策略均通过；release 的完整矩阵也通过。
这支持调试构建 CPU/调度延迟触发请求超时的解释，但没有把该解释视为协议抓包结论。
没有放宽生产超时、降低并发，或修改 FAS2750 配置来规避问题。

## 静态与本地验证

- `cargo test --lib`：769 passed，4 ignored。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `python3 tests/validate_architecture_dependencies.py .`：通过。
- `git diff --check`：通过。

## 复现

通过环境变量提供 `CIFS_REAL_SERVER`、`CIFS_REAL_SECOND_SERVER`、`CIFS_REAL_SHARE`、
`CIFS_REAL_USER`、`CIFS_REAL_PASS`，并将 `DATA_MOVER_RECOVERY_DIR` 指向独立临时目录。
不要将密码写入仓库或测试报告。

```sh
CIFS_POLICY_TEST_BYTES=4096,4194304,41943040,67108865,1073741824 cargo test --release --test cifs_policy_contract -- --ignored --nocapture
cargo test --lib real_share_ -- --ignored --nocapture
```

测试自身清理命名范围内的 final/stage/checkpoint 文件。若强制终止整个测试进程，
必须按该轮 UUID 及对应目标路径哈希清理遗留文件，不能批量删除其他任务的 stage。

## 能力边界

CIFS 支持 Checkpointed 和 AtomicReplace；Direct 仍不支持。当前 domain facade
提供 ACL apply，但缺少用于本轮实现的时间戳和 Unix uid/gid 设置接口，不能宣称与
local/NFS 的自动 metadata 复制能力等价。SMB FLUSH 也不代表独立的目录 fsync。
