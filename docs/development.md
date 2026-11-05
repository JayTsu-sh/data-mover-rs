# 开发规范与场景索引

AI 工作流程、首版格式策略及外部环境异常约束统一见 [AGENTS.md](../AGENTS.md)。首个版本只维护当前接口与数据格式，具体取舍见下方“首个版本”章节。根包 data-mover 是 facade，实现归属 crates/；依赖、features、工具链和版本的权威来源为 Cargo.toml、rust-toolchain 配置及 CI。现有 .claude 下的资料和校验脚本作为普通文件使用，Codex 需要按场景显式阅读和运行。


## 库职责与变更验收

本项目是多 backend 的可复用存储与数据移动库。model／ACL 保存无协议 SDK 依赖的事实与规则，storage-core 定义共享 role 契约，遍历／传输／元数据引擎持有通用策略，backend 实现协议及设备能力，根 facade 提供调用入口。按 [workspace 架构](architecture/crate-workspace.md) 的当前依赖守卫维护分层；新增接口说明实际调用方、错误、顺序和资源契约，避免只转发参数的中间层或无现实变化点的抽象。

| 修改范围 | 设计约束与必须验证的结果 |
| --- | --- |
| crate、feature、SDK 依赖或 facade | 引擎和共享模型保持 backend 无关；feature 只控制编译可用性，运行策略显式配置。执行相关 crate／workspace 测试、架构守卫与独立最小 feature 矩阵，不能用 workspace feature 合并成功证明最小使用方可用 |
| 新增或修改 role／backend 能力 | 同步角色契约、能力声明、适配实现、公开文档及对应测试；unsupported、未认证和环境限制如实表达，不能用空实现或测试跳过伪装支持。按受影响协议、平台和设备能力验证，不对所有 backend 假设相同语义 |
| 遍历、过滤与恢复 | 机制同时服务扫描和拷贝，产品策略由调用方给定；明确 unordered／blocks 的顺序、完成块、分页／游标、取消和恢复契约。确认点与累计统计对齐，重列举不能重复计数；大目录、子目录、过滤／剪枝与故障后的边界应有针对性测试 |
| 拷贝、写入、元数据与发布 | 区分已接收、已持久化、可恢复与最终已发布；扫描 checkpoint 与传输恢复凭据属于不同生命周期。明确部分写、源变更、目标所有权、ACL／xattr 能力、原子性及不确定结果，依协议证据安全重试；最终内容和元数据独立校验，不能只验命令退出码 |
| 并发、资源与性能 | 流式、批量和背压按工作窗口限制资源，避免依赖总目录或文件数量的常驻集合。公开接口交代句柄／缓冲区所有权、取消和 close 行为；优化分别记录正常与故障时的吞吐、延迟、峰值内存和后台任务回收 |

能力矩阵和历史架构资料用于定位契约，版本号、实际能力和历史兼容取舍仍按当前构建、实现与 AGENTS.md／本页“首个版本”规则核验；发现资料过时须随相应变更修正，不能照搬旧结论。

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

## 首个版本：只维护当前接口和数据格式

本项目正在开发首个版本，开发期的 crate 版本号不构成已发布版本的兼容承诺。

- 接口、配置、Checkpoint、快照和缓存格式发生变化时，直接采用当前设计；同步删除旧接口、别名、旧格式读写分支、迁移转换和过时测试、文档。
- 旧开发数据和测试夹具可以清除后重新生成；旧扫描状态重新扫描，旧传输状态从源数据重新开始。清理范围限于已确认归属本项目的历史状态或临时产物，保留用户源文件、已发布的最终文件和无关存储对象。
- 版本号用于识别当前编码并拒绝其他格式；只实现当前格式，不为历史开发版本增加兼容解码器或升级链。
- 保留当前数据的完整性、安全和恢复校验，包括 magic、版本、校验和、长度上限、身份/计划绑定、所有权和权限检查。
- S3 等服务端差异、操作系统支持及当前恢复流程是实际功能，按当前需求维护；不能仅因名称含 `compat`、`legacy` 或 `version` 就删除。
