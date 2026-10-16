# Codebase 概览

## 项目身份

Rust 库，Edition 2024；具体依赖版本和固定 Git 提交以 `Cargo.toml` / `Cargo.lock` 为准。
架构规范见 [storage-transfer-architecture.md](../../docs/architecture/storage-transfer-architecture.md)。

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
- 配置类型和 NFS / S3 发现工具从 `storage` 导出。
- HDFS 客户端构造辅助入口也从 `storage` 导出，协议实现仍在 HDFS backend。
- Windows ACL 工具为 `storage::windows_acl`，角色路径使用能力目录内的文件句柄。

## 示例与测试

`examples/README.md` 列出最小公开示例：读取、遍历、复制、命名空间、完整性检查、重命名和 export 发现。
共享 CLI 解析放在 `examples/support/`。性能、故障注入、恢复矩阵和真实环境工具放在
`tests/lab/tools/`；在 `Cargo.toml` 中显式注册 example target，因此原有 `cargo run --example` 命令仍可用。

`tests/` 包含公开契约与集成测试，backend 内部测试覆盖协议适配行为；真实环境脚本位于
`tests/lab/` 与 `.claude/skills/e2e-*/`。运行 `make ci` 检查本地门禁；网络验证需要对应环境。
