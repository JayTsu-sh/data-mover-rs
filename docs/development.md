# 开发规范与场景索引

AI 工作流程、首版格式策略及外部环境异常约束统一见 [AGENTS.md](../AGENTS.md)。根包 data-mover 是 facade，实现归属 crates/；依赖、features、工具链和版本的权威来源为 Cargo.toml、rust-toolchain 配置及 CI。现有 .claude 下的资料和校验脚本作为普通文件使用，Codex 需要按场景显式阅读和运行。

## 编码与架构

- 先理解当前调用链再编辑。通用流程通过 typed roles 保持 backend 无关，新增或修改操作须核对角色接口、能力声明、错误分类及行为测试。
- use 集中在文件顶部，按 std、外部 crate、本 crate 分组；测试模块按现有约定处理。函数内较深路径通过顶部导入缩短。
- 生产代码遵守 workspace 对 unwrap、expect、unsafe 等的 lint；错误使用 thiserror，保留有类型的 source。backend 映射到现有 StorageError 分类，新增分类需说明当前使用场景；Cancelled 保持独立取消语义。
- 关闭 CIFS 的 smb Resource 使用当前 backend/protocol.rs 的 close_resource，遵守该协议的完成和错误处理；其他 backend 按各自资源契约收尾。
- 结构重构保持可观察行为，行为修复单独提交；新增实现按职责拆分，新文件目标不超过 800 行、函数不超过 50 行，既有大文件不继续堆积无关职责。
- 异步、错误和日志沿用 tokio、thiserror、tracing；日志中的 URL 先经现有 url_redact 脱敏。依赖默认来自正式 registry；经授权的 git 依赖固定精确 revision，不能改为浮动 branch。

## 按场景查阅

| 场景 | 先读 |
| --- | --- |
| crate 边界、features、公开入口或新增 backend | [workspace 架构](architecture/crate-workspace.md) |
| storage role 操作、能力与传输 | [传输架构](architecture/storage-transfer-architecture.md)、[能力矩阵](architecture/backend-capability-matrix.yaml)、crates/storage-core/src/storage/roles/ |
| NFS / CIFS / S3 | [.claude/docs/storage-nfs.md](../.claude/docs/storage-nfs.md)、[storage-cifs.md](../.claude/docs/storage-cifs.md)、[storage-s3.md](../.claude/docs/storage-s3.md) 中对应 backend 的章节及 crates/backend-*/ |
| Local namespace 与 ACL | [storage-local.md](../.claude/docs/storage-local.md)、crates/backend-local/ |
| 错误映射、重试与取消 | [错误分类](../.claude/docs/error-taxonomy.md)、本仓库 AGENTS.md 的外部环境约束及当前错误类型 |
| 遍历、过滤和 checkpoint | [调度资料](../.claude/docs/walk-scheduler.md)、[过滤资料](../.claude/docs/filter-dsl.md)、crates/traversal/src/traversal/ 的当前调度与恢复实现 |
| 元数据、ACL 和 xattr | [元数据协商](../.claude/docs/metadata-negotiation.md)、crates/metadata/、crates/acl/ 与对应 backend |
| 校验、QoS、tar 与拷贝 | crates/integrity/、crates/transfer/、crates/storage-core/ 及其当前测试 |
| 后端真实环境、允许操作范围 | [测试环境](../.claude/docs/test-environments.md)、tests/lab/ 和对应校验脚本的说明 |
| commit / PR、命名和日志 | [提交与代码约定](../.claude/docs/conventions.md)；AI 工作流程以 AGENTS.md 为准 |

## 验证

- 工具链、MSRV、edition 与 lint 取当前构建配置；检查命令以 Makefile 和 [.github/workflows/ci.yml](../.github/workflows/ci.yml) 为准。
- 故障预算、错误分类与边界可用确定性测试或受控服务验证；实际 backend 行为仍须提供适用的真实环境证据，不能用 mock 成功替代链路验收。按 AGENTS.md 区分未测、跳过和不支持。
- 修改角色／能力时核对对应行为测试，修改架构守卫时执行其自测；性能、取消、恢复与不确定写入的验证按改动范围执行。
- 测试日志、凭据和机器配置留在仓库外或原有忽略范围。规范在 AGENTS.md 和本页维护，避免另建 AI 专属工作流或重复规则副本。
