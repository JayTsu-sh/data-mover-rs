# Windows 原生运行验证（2026-09-15）

## 最终结论：Windows 原生回归已通过

修复提交 `bba1b26d928987df838d2d671d1f5a1e2dcc811e` 在 Windows Server 2025 / Rust 1.95.0 MSVC 下得到 **18 通过、0 失败、18 忽略**。
[修复后完整 CI](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/34920712442)。

修复没有跳过或吞掉同步错误：Windows 目录句柄改用 `FILE_FLAG_BACKUP_SEMANTICS` 和读写权限，然后仍调用 `sync_all()`。
共享实现位于 `src/storage/durability.rs`，覆盖最终发布、checkpoint 替换、stage/claim 清理、新父目录以及恢复注册记录的目录同步。
Unix 保持原来的目录打开与同步行为。Windows 恢复注册记录原先的空目录同步分支也改为真实同步。

原有 AtomicReplace 和 Checkpointed 的各 16 个组合全部完成；新增 257 MiB 文件跨越 Local 256 MiB 持久化 checkpoint 窗口，并验证最终内容与目标目录清理；新增嵌套父目录创建场景也通过。
HDFS 无集群契约 4 项、Local runtime 5 项、覆盖截断 2 项、定位读写 2 项、flush 1 项、并发参数 3 项、公开 API 1 项，共 18 项通过。
18 个忽略项仍为外部 HDFS/S3 集群测试。

Linux 同步验证：747 个库测试通过、4 忽略；5 个新增/既有原生文件系统集成用例通过；全目标 Clippy、格式检查、架构依赖检查通过。
这些结果证明当前原生测试场景可正常完成，不等同于断电故障注入、Windows 网络挂载或 POSIX 元数据支持已验证。

### 根因实验

[独立 Windows 探针](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/34920427662) 的实际结果：

| 打开方式 | 打开目录 | sync_all |
| --- | --- | --- |
| 普通只读 | PermissionDenied，错误码 5 | 未执行 |
| 只读 + FILE_FLAG_BACKUP_SEMANTICS | 成功 | PermissionDenied，错误码 5 |
| 读写 + FILE_FLAG_BACKUP_SEMANTICS | 成功 | 成功 |

因此必须同时使用目录打开标志和可写访问权限。修复后的完整 transfer 测试进一步确认该更改消除了原来的发布后失败。
探针只保留在 `test/windows-probe-20260915` 诊断分支，不进入产品源码；修复位于 `test/windows-native-20260915` 临时验证分支，未合并到主分支。

以下保留修复前两轮的失败记录。


## 环境与范围

- GitHub 托管 `windows-latest`：Windows Server 2025，内核 `10.0.26100.0`。
- Rust 1.95.0，目标 `x86_64-pc-windows-msvc`，原生执行 `.exe`。
- 临时分支：`test/windows-native-20260915`。
- 首轮源码快照：`07fb87ef55371adccb5c14b5001952b27fa1fc7b`。
- 第二轮：`fd3812639abe8e8edb84201ed02d8989aad40717`，仅修改任务为 `--no-fail-fast` 并在失败时保存构建缓存，产品代码相同。
- 快照不包含工作区 `.scratch` 或 benchmarks 原始数据，未修改原工作分支。

工作流：`.github/workflows/windows-runtime.yml`。
新增测试：`tests/local_transfer_runtime.rs`。

Local 场景组合：4 KiB、4 MiB、40 MiB、65 MiB × AtomicReplace/Checkpointed × 开启/关闭读回校验 × 新目标/覆盖较长旧目标。
每个成功场景核对完整内容、长度，并立即删除最终文件、确认目标目录无残留，以验证句柄释放与清理。
另有预取消保持原目标内容的检查，以及已有定位读写、截断、flush、并发参数、公开接口和无集群 HDFS 契约。
测试使用临时目录，不连接真实 NFS/CIFS/HDFS 服务；也不声称验证 Windows POSIX 元数据支持或完整崩溃恢复。

## 首轮结果

[GitHub Actions 首轮日志](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/34918360628)

- 库和全部 examples 的 MSVC 编译通过。
- HDFS 契约：4 通过，18 项需要外部集群的测试按原有定义忽略。
- AtomicReplace：全部 16 个复制/覆盖组合通过。
- 预取消：两种策略均保持已有目标内容，测试通过。
- Checkpointed：首个 4 KiB 场景失败，阶段为 `Publish`，错误类 `PermissionDenied`。
- 失败报告含 `final_destination_changed: true` 和 `committed_cleanup`，即发布已经改变目标，但整个操作不能报告成功。
- 首轮 Cargo 默认遇到失败就停止，后续测试目标未执行；第二轮使用 `--no-fail-fast` 补齐。

## 第二轮完整结果

[GitHub Actions 完整运行日志](https://github.com/JayTsu-sh/data-mover-rs/actions/runs/34919184385)

总计 **15 通过、1 失败、18 忽略**；全部 7 个测试目标均执行。

| 测试目标 | 通过 | 失败 | 忽略 |
| --- | ---: | ---: | ---: |
| hdfs_native_contract | 4 | 0 | 18 |
| local_transfer_runtime | 2 | 1 | 0 |
| test_local_overwrite_truncate | 2 | 0 | 0 |
| test_local_positional_io | 2 | 0 | 0 |
| test_local_write_flush | 1 | 0 | 0 |
| test_transfer_concurrency | 3 | 0 | 0 |
| transfer_public_api | 1 | 0 | 0 |

唯一失败与首轮一致：Checkpointed 在首个 4 KiB、新目标、开启读回校验的组合中，于 Publish 阶段返回 PermissionDenied。
因此 Checkpointed 后续组合尚未运行，不能推断其成功；AtomicReplace 的 16 个组合均完成。
18 个忽略项均为 HDFS 契约中需要真实外部集群的现有测试，没有为了规避此次失败而添加忽略。

构建缓存已在失败后保存，后续修复可以复用。测试工作流和回归用例已留在临时分支，原工作区保留同样文件；未合并到主分支。
本次没有修改产品的 Windows 持久化语义，也没有将失败转为成功。

## 修复前的初步原因定位

`local/staged/publication.rs::finish_publication` 在 rename 后，以 `root.open(".")` 打开只读目录句柄。
非 Unix 的 `directory_sync.rs::sync` 对该句柄执行 `sync_all()`；Rust Windows 实现调用 `FlushFileBuffers`。
Microsoft 明确要求 [FlushFileBuffers 的句柄具有 GENERIC_WRITE 权限](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)。
这条代码路径与首轮“发布已提交后 PermissionDenied”的现象相符，是目前的主要原因判断，尚未通过逐调用诊断或修复后的 Windows 重跑证实。

不能通过忽略目录同步错误来宣称 Checkpointed 的持久化保证成立。这两轮修复前结果不足以宣布 Windows 运行支持通过；修复后结果见文首。
