# Codex 项目指令

本文件是本仓库统一的 AI 开发入口。

## 工作流程

- 修改前检查 git status，保护共享目录中的无关改动；明确行为、crate／role 边界和验证目标，仅提交本次任务文件。
- 理解代码先查当前实现、调用方与测试；存在 .codegraph/ 时遵守下方 CodeGraph 规则，否则用 rg。常规实现选择自主推进，无法从仓库确定且影响正确性的约束再澄清。
- 修改 Rust、backend、遍历、传输、公开接口或 crate 边界前，按 [开发规范](docs/development.md) 阅读对应约束和技术资料；Codex 不依赖 Claude 专属 agent、/evolve 命令或自动加载规则。
- 跨模块改动先核对依赖方向、能力矩阵和当前调用方；复用职责明确的模块，结构重构与行为修复分开提交。外部 I/O 变更必须说明本文件规定的故障处理和验证证据。
- 代码变更执行与影响范围相符的检查，完整运行时改动执行 make ci 及适用的真实环境验证；仅修改指引文档时检查内容、链接及 diff。
- 合入前 review 最终改动；报告实际运行结果、未测环境和剩余风险，不能把预期目标、跳过或能力限制写成通过。

## 首个版本：只维护当前接口和数据格式

本项目正在开发首个版本，开发期的 crate 版本号不构成已发布版本的兼容承诺。

- 接口、配置、Checkpoint、快照和缓存格式发生变化时，直接采用当前设计；同步删除旧接口、别名、旧格式读写分支、迁移转换和过时测试、文档。
- 旧开发数据和测试夹具可以清除后重新生成；旧扫描状态重新扫描，旧传输状态从源数据重新开始。清理范围限于已确认归属本项目的历史状态或临时产物，保留用户源文件、已发布的最终文件和无关存储对象。
- 版本号用于识别当前编码并拒绝其他格式；只实现当前格式，不为历史开发版本增加兼容解码器或升级链。
- 保留当前数据的完整性、安全和恢复校验，包括 magic、版本、校验和、长度上限、身份/计划绑定、所有权和权限检查。
- S3 等服务端差异、操作系统支持及当前恢复流程是实际功能，按当前需求维护；不能仅因名称含 `compat`、`legacy` 或 `version` 就删除。

项目架构、编码规范和验证路由见 [开发规范](docs/development.md)。涉及历史兼容取舍时，以本节为准。

## 外部环境异常：设计、开发和测试约束

涉及外部 I/O、backend、遍历或传输调度的设计、实现和 review，必须分析异常路径。按本层实际操作列出源端／目标端存储及其 DNS、网络、认证和协议服务；ClickHouse、产品 callback 等由使用本库的应用负责，不向本库引入这些业务依赖。

- **设计依据**：在设计或 PR 中记录受影响的「依赖／操作阶段／故障类型／检测时限／恢复预算及负责层／错误结果／一致性保证／验证方式」。评估连接建立、列举、读取、写入、提交、关闭和恢复等实际阶段，不适用项说明原因。
- **识别和上报**：区分连接拒绝、中断、静默丢包／请求超时、认证／权限失败、限流、资源不足及服务端错误。backend 保留底层有类型的原因、操作上下文和写入不确定性，准确映射为 StorageError；取消保持独立语义，不能把未知错误都归为断网。
- **分层和预算**：client 的恢复限于协议安全的短时有界处理；backend 负责错误映射与本操作确有必要的恢复。遍历／传输调度统一执行调用方给定的预算和取消策略，明确重试次数、绝对截止时间与退避；各层共享剩余预算，避免 backend、client 与调度层叠加等待。普通列举接口不隐式携带产品扫描策略，调用它的拷贝等流程逐操作确定策略。
- **扫描恢复**：调用方为扫描指定识别后约 3 秒的恢复窗口时，同一次故障的列举、分页、根目录验证和并发重试共享截止时间；窗口内恢复继续，耗尽后及时返回准确错误。静默丢包的识别耗时另行约束和报告。该策略由通用调度层执行，不在每个 backend 复制，也不自动套用于写入或所有普通操作。
- **一致性与取消**：区分明确未执行、已完成及已发送但结果未知，依据协议证据和幂等性决定重放；连接恢复不等于句柄、游标或事务状态有效。续扫和重列举保持 checkpoint、已确认事实及累计计数一致；重放条目不能重复累加。连接等待、退避、背压和在途操作均须响应取消，并收束后台任务，明确半写入、临时对象和句柄的所有权及清理结果。
- **故障验证**：对受影响 backend／协议和遍历／拷贝路径，注入闪断、持续断连、静默丢包／超时、认证／权限失败、服务端错误，以及确认丢失／未知写结果和故障期间取消。验证分类、检测及恢复／退出耗时、可恢复性、最终数据和精确计数、资源回收与正常性能／内存开销。用确定性测试验证预算和边界，并在受控真实链路验证适用场景；明确未测环境、跳过项和能力限制。

这些是设计和验收约束，不代表所有 backend 已完成对应实现或实测；结论必须附当前代码与验证证据。

<!-- CODEGRAPH_START -->
## CodeGraph

In repositories indexed by CodeGraph (a `.codegraph/` directory exists at the repo root), reach for it BEFORE grep/find or reading files when you need to understand or locate code:

- **MCP tool** (when available): `codegraph_explore` answers most code questions in one call — the relevant symbols' verbatim source plus the call paths between them, including dynamic-dispatch hops grep can't follow. Name a file or symbol in the query to read its current line-numbered source. If it's listed but deferred, load it by name via tool search.
- **Shell** (always works): `codegraph explore "<symbol names or question>"` prints the same output.

If there is no `.codegraph/` directory, skip CodeGraph entirely — indexing is the user's decision.
<!-- CODEGRAPH_END -->
