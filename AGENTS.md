# 项目开发规则

## 首个版本：只维护当前接口和数据格式

本项目正在开发首个版本，开发期的 crate 版本号不构成已发布版本的兼容承诺。

- 接口、配置、Checkpoint、快照和缓存格式发生变化时，直接采用当前设计；同步删除旧接口、别名、旧格式读写分支、迁移转换和过时测试、文档。
- 旧开发数据和测试夹具可以清除后重新生成；旧扫描状态重新扫描，旧传输状态从源数据重新开始。清理范围限于已确认归属本项目的历史状态或临时产物，保留用户源文件、已发布的最终文件和无关存储对象。
- 版本号用于识别当前编码并拒绝其他格式；只实现当前格式，不为历史开发版本增加兼容解码器或升级链。
- 保留当前数据的完整性、安全和恢复校验，包括 magic、版本、校验和、长度上限、身份/计划绑定、所有权和权限检查。
- S3 等服务端差异、操作系统支持及当前恢复流程是实际功能，按当前需求维护；不能仅因名称含 `compat`、`legacy` 或 `version` 就删除。

项目架构、代码规范和验证命令见 [CLAUDE.md](CLAUDE.md)。涉及历史兼容取舍时，以本节为准。

<!-- CODEGRAPH_START -->
## CodeGraph

In repositories indexed by CodeGraph (a `.codegraph/` directory exists at the repo root), reach for it BEFORE grep/find or reading files when you need to understand or locate code:

- **MCP tool** (when available): `codegraph_explore` answers most code questions in one call — the relevant symbols' verbatim source plus the call paths between them, including dynamic-dispatch hops grep can't follow. Name a file or symbol in the query to read its current line-numbered source. If it's listed but deferred, load it by name via tool search.
- **Shell** (always works): `codegraph explore "<symbol names or question>"` prints the same output.

If there is no `.codegraph/` directory, skip CodeGraph entirely — indexing is the user's decision.
<!-- CODEGRAPH_END -->
