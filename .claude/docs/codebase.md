# Codebase 概览

## 项目身份

Rust 库，Edition 2024；具体依赖版本和固定 Git 提交以 `Cargo.toml` / `Cargo.lock` 为准。
架构规范见 [storage-transfer-architecture.md](../../docs/architecture/storage-transfer-architecture.md)。

## 当前网络依赖

| 依赖 | 固定来源 |
|---|---|
| `nfs-rs` | crates.io `0.11.1`；Rust / PyPI 同版本正式发布 |
| `smb-domain`（package `smb`） | JayTsu-sh/smb-rs main `02c80bb4f28e4566533e5ff4abc741b59e251ee3`，#91；升级验收见 [CIFS](storage-cifs.md#升级-smb-rs-依赖) |

## 模块与依赖方向

| 目录 | 职责 |
|---|---|
| `src/model/` | 后端无关的数据与失败模型 |
| `src/storage/roles/` | 读取、暂存写入、命名空间、元数据等角色契约 |
| `src/storage/backends/{local,nfs,s3,cifs,hdfs}/` | 协议适配、具体客户端和连接装配 |
| `src/storage/factory.rs` | 向 backend facade 分派类型化连接配置 |
| `src/transfer/` | 基于角色的传输、校验、发布和恢复编排 |
| `src/traversal/` | 遍历、排序和续扫 |
| `src/filter.rs`、`src/filter_traversal.rs` | DSL 与遍历过滤适配 |
| `src/integrity/` | 完整性检查 |
| `src/runtime/` | 预算、阻塞执行和运行时资源边界 |
| `src/error.rs`、`src/time_util.rs`、`src/url_redact.rs` | 共享基础工具 |

`src/{nfs,s3,hdfs,cifs,acl}.rs` 已收拢到对应 backend；旧 `StorageEnum`、
`create_storage` 和旧 entry 模型已移除。跨 backend 操作通过角色契约，不按后端种类分派。
用 `make arch-guard` 检查依赖方向，用 `make audit-large` 获取当前顶层大文件清单。

## 公开入口

- `storage::connect_backend(BackendConfig)` 建立连接并返回 `Storage` 角色集合。
- `storage::connect_scan_backend` 选择扫描连接策略；统一恢复在 `traversal::recoverable_scan`。
- 配置类型和 NFS / S3 发现工具从 `storage` 导出。
- HDFS 客户端构造辅助入口也从 `storage` 导出，协议实现仍在 HDFS backend。
- Windows ACL 工具为 `storage::windows_acl`，角色路径使用能力目录内的文件句柄。

## 远程扫描

- 原生库负责准确报告类型化断连 / 时限和保留修改结果不确定性；NFS 默认在 500 毫秒内短重试，
  无需 URL 参数。`connect_scan_backend` 关闭 S3 SDK / 普通分页重发，并将 HDFS
  默认 failover 次数限制为一次（显式配置优先）。Local 继续普通遍历行为。
- `crates/traversal/src/traversal/scan.rs` 的 `ScanFailureSignal` 包装 namespace / metadata
  读取，记录首个原始网络故障并提供旁路信号；`operation_timeout=None` 沿用原生有界时限，
  显式时限只限制存储操作，不覆盖消费方等待。持有句柄的原生调用在调用方提前结束等待后仍完成清理。
- `recovery.rs` / `recovery/attempt.rs` 的 `recoverable_scan` 负责默认 3 秒防抖窗口与重连，
  同一窗口内连续失败不重新补满预算；原故障位置、读取阶段与参数（列举还绑定原页序）成功后重新进入 Running。重连后只从安全目录
  检查点续扫，活跃目录可能重放，调用方不可把重放误当成新扫描世代。健康前缀重建与消费方背压不消耗恢复预算；失败前缀等待、目标读取、重连及逐次短退避扣同一预算。
  因此 3 秒是累计故障恢复预算，恢复扫描的总墙钟时间可更长；健康前缀仍受独立操作时限约束。
- `ScanRecoveryStatus` 提供 Running / Recovering / Failed 活动；其 `failure_signal()` 只在恢复
  最终失败时发布该窗口最早的原始故障，后续重连时限不覆盖明确断连。取消保持 Cancelled，
  不发布最终网络故障；检查点可见性与取消后的排空策略保持调用方选择。
- 应用负责选择时限 / 恢复窗口以及持久化、CLI / Web 展示；不能把原生库改为全局扫描恢复器。
  行为测试位于 `crates/traversal/src/scan_tests.rs` 与 `recovery_tests.rs`；真实矩阵结果以本次实际日志为准。

## 示例与测试

`examples/README.md` 列出最小公开示例：读取、遍历、复制、命名空间、完整性检查、重命名和 export 发现。
共享 CLI 解析放在 `examples/support/`。性能、故障注入、恢复矩阵和真实环境工具放在
`tests/lab/tools/`；在 `Cargo.toml` 中显式注册 example target，因此原有 `cargo run --example` 命令仍可用。

`tests/` 包含公开契约与集成测试，backend 内部测试覆盖协议适配行为；真实环境脚本位于
`tests/lab/` 与 `.claude/skills/e2e-*/`。运行 `make ci` 检查本地门禁；网络验证需要对应环境。
