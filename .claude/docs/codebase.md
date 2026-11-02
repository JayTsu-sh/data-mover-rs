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
- 扫描与普通操作共用 `storage::connect_backend`；恢复窗口由调用方通过 `traversal::recoverable_traversal` 显式选择。
- 配置类型和 NFS / S3 发现工具从 `storage` 导出。
- HDFS 客户端构造辅助入口也从 `storage` 导出，协议实现仍在 HDFS backend。
- Windows ACL 工具为 `storage::windows_acl`，角色路径使用能力目录内的文件句柄。

## 远程扫描

- 原生库负责准确报告类型化断连 / 时限和保留修改结果不确定性；NFS 默认在 500 毫秒内短重试，
  无需 URL 参数。所有操作共用连接策略，不向 backend 传递扫描身份。HDFS 默认只遍历一轮已配置
  NameNode（显式 XML / override 优先），网络故障与超时不追加 adapter 重试；S3 同样立即报告网络故障，
  安全读和幂等分段保留服务端错误重试，结果不确定的写请求禁止 SDK 自动重放。Local 继续普通遍历行为。
- `crates/traversal/src/traversal/scan.rs` 的 `ScanFailureSignal` 包装 namespace / metadata
  读取，记录首个原始网络故障并提供旁路信号；`operation_timeout=None` 沿用原生有界时限，
  显式时限只限制存储操作，不覆盖消费方等待。持有句柄的原生调用在调用方提前结束等待后仍完成清理。
- `recovery.rs` / `recovery/probe.rs` 的 `recoverable_traversal` 从首次故障识别开始，使用调用方显式提供的墙钟窗口，
  重连、退避和原故障操作验证共用截止时间，消费方背压不能暂停。单个有界 probe 与旧流安全排空并行；
  `scan/probe.rs` 只验证原路径、读取阶段、plan、identity/version 和列举页序，probe 结果不作为扫描条目输出。
  验证成功即进入 Running，随后在旧流与完整 history 排空后，从安全目录检查点正常重建；活跃目录可重放。
  普通 list / traversal 不隐式启用恢复；应用可为扫描明确选择 3 秒。正常重建可以超过所选窗口，
  但 Recovering 状态不能靠健康前缀或背压无限延长。
  原目录 page N 没有跨连接 continuation 时仍须读取前 N 页；无法在窗口内验证时保守失败并保留检查点。
- `TraversalRecoveryStatus` 提供 Running / Recovering / Failed 活动；其 `failure_signal()` 只在恢复
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

HDFS 根验证、stat、listing 和安全读取统一直接返回 Connectivity/Timeout，避免 adapter 网络退避叠加。连接不区分扫描与普通操作；服务错误重试、lease 恢复、单次 RPC 时限按操作语义保留。
