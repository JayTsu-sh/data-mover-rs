# Nightly Lab #38861411161 HDFS failure investigation

调查对象：GitHub Actions Nightly Lab run [#38861411161](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/38861411161)，提交 `b1f5dcbb66b7f8a40a3ba26cbcde4a83a5839155`。

## 结论

这次失败主要是 HDFS Lab 环境在测试开始时出现瞬态 I/O/连接不可用，脚本没有为 HDFS smoke 阶段提供就绪等待或重试；没有证据表明是 data-mover 产品逻辑回归。

更准确地说，这是“环境故障被脚本放大为确定性失败”：

1. 失败发生在真实 HDFS 合约测试，而不是 workspace 单元测试、编译或 clippy。
2. 7 个连续测试都在连接同一个 HDFS 存储根时失败，并返回完全相同的 `BackendConnectError`；随后 7 个测试又全部通过。
3. 下一次运行 [#38862960216](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/38862960216) 使用仅增加 HDFS smoke 重试的提交 `6f873b6`，HDFS 阶段已通过，说明相同代码路径在稍后重试时可用。

## 一手证据

### 失败阶段和错误形态

GitHub job 日志显示：

- `health-check.sh`、`prepare-run.sh`、workspace tests、examples、clippy 均通过。
- `Rolling hdfs-native contract and real HDFS lab` 于 06:33:24 开始。
- 14 个真实 HDFS 测试中，前 7 个在 06:34:02 到 06:36:54 依次失败；错误均为：

  ```text
  BackendConnectError { kind: Hdfs, message: "HDFS get storage root failed (Io): I/O operation failed", session: None }
  ```

- 后 7 个测试在 06:39:29 到 06:39:39 全部通过。
- 失败测试之后的清理测试 `nightly_lab_cleanup_confined_run_root` 通过。

来源：[run #38861411161 job 日志](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/38861411161/job/116844309877)，以及仓库中的 [tests/hdfs_upstream_contract.py](../../tests/hdfs_upstream_contract.py) 和 [tests/hdfs_native_contract.rs](../../tests/hdfs_native_contract.rs)。

这个时间序列不符合单个测试逻辑错误：同一批测试共享相同的 fixture 连接和 HDFS 根检查，若是固定的路径、权限、Kerberos 或产品逻辑问题，后续测试不会在同一 job 内整体恢复。

### 代码路径确认

真实 smoke 测试通过 [tests/lab/run-hdfs-smoke.sh](../../tests/lab/run-hdfs-smoke.sh) 启动 `hdfs_native_contract` 的 ignored 测试。每个 fixture 在 [tests/hdfs_native_contract.rs](../../tests/hdfs_native_contract.rs) 中调用 `connect_backend`，产品代码随后在 [src/storage/backends/hdfs/client/support.rs](../../src/storage/backends/hdfs/client/support.rs) 的 `create_hdfs_storage` 中对配置根调用 `client.get_file_info(location.root())`。

因此报错点是“连接后获取 HDFS 根目录状态”，不是迁移、读写、元数据转换或路径隔离逻辑。

产品代码对 HDFS 读请求已有有限重试和 5 秒单次超时；但该次运行中的错误最终被映射成非成功的根目录连接错误。旧版 smoke 脚本在一次合约批次失败后直接退出，只执行退出清理，没有给瞬态集群故障第二次机会。

### 环境拓扑风险

Nightly workflow 将基础 HDFS smoke 的 `LAB_HDFS_LOCATION` 配置为显式单 NameNode 地址 `10.131.9.30:9000`；HA 地址只用于后续显式 HA 场景。配置见 [.github/workflows/nightly.yml](../../.github/workflows/nightly.yml)。

这意味着 NameNode 在启动抖动、RPC 短暂不可用或网络瞬态异常期间，基础 smoke 没有备用 endpoint 可切换。当前 `health-check.sh` 主要校验 DNS、SSH、NFS 和 S3，并不执行 HDFS RPC 根目录探测；因此它不能把这种 HDFS 瞬态故障提前阻断。

### 交叉运行验证

此前运行 [#38859131838](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/38859131838) 在同一分支、同一 HDFS 拓扑下通过了 HDFS Native 阶段，失败点是独立完整性脚本的旧断言；这表明 HDFS 端点并非持续不可用。

修复提交 `6f873b6` 只改动 smoke harness：增加 `HDFS_SMOKE_ATTEMPTS`（默认 2）、失败后的隔离根清理和 5 秒等待；没有修改 HDFS 产品实现。对应的新运行 [#38862960216](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/38862960216) 在 06:49:59 开始 HDFS 阶段，并于 06:50:28 成功结束。这个对照实验是判断“瞬态环境故障 + 脚本容错不足”的关键证据。

## 分类判断

| 类别 | 判断 | 依据 |
| --- | --- | --- |
| 环境问题 | **主要原因** | 同一批次前 7 个失败、后 7 个通过；错误统一发生在 HDFS 根目录 RPC；后续重试恢复。 |
| 脚本问题 | **次要原因/放大因素** | 旧 `run-hdfs-smoke.sh` 单次运行失败即退出，且健康检查没有 HDFS RPC readiness probe。 |
| 产品代码问题 | **目前无证据** | 编译、workspace tests、clippy 通过；失败位于连接根目录；相同产品代码在相邻运行及重试后通过。 |

## 已采取的修复与剩余建议

已实施的 harness 修复是合理且最小的：保留最终失败语义，同时对短暂 HDFS 故障重试一次。建议后续仍考虑：

- 在 `health-check.sh` 增加不泄露凭据的 HDFS 根目录 RPC readiness probe；
- 若基础 smoke 必须具备高可用性，使用逻辑 NameService 或显式 failover 配置，而不是单一 NameNode 地址；
- 保留失败时的 NameNode/RPC 诊断信息，便于区分网络拒绝、Kerberos、NameNode 未就绪和 DataNode 故障。

