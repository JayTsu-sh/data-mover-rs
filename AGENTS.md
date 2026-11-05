# Codex 项目指令

本文件是统一 AI 开发入口；按任务阅读 [开发规范](docs/development.md) 中对应的编码、架构、格式与验证约束。

- 首版只维护当前接口和数据格式，不增加历史兼容层；同步更新调用方和测试。保留当前数据的安全／恢复校验，保护源文件、已发布文件及无关对象；详细边界见开发规范。
- 修改前检查 git status，明确行为、crate／role 边界和验证目标，保护共享目录中的无关工作，仅提交本次任务文件。
- 通用流程通过 typed roles 保持 backend 无关；修改能力、错误分类或公开接口须核对声明、调用方和行为测试，结构重构与行为修复分开提交。
- 涉及外部 I/O、backend、遍历或传输的设计／开发／测试／review，必须读 [故障约束](docs/external-dependency-failures.md)：按实际源／目标存储分析异常，保留有类型原因和未知写结果，恢复预算与取消由相应负责层统一执行；扫描约 3 秒窗口由调用方指定，不把 ClickHouse／产品 callback 引入本库。
- 运行与影响范围相符的检查和真实环境故障验证；完整运行时改动执行 make ci，纯文档检查内容、链接及 diff。合入前 review 最终改动，如实报告未测／跳过及风险。
- 技术资料和现有脚本按开发规范显式读取／运行，不依赖 Claude 专属 agent、命令或自动加载；用当前代码与构建配置核对历史资料。

<!-- CODEGRAPH_START -->
## CodeGraph

仓库存在 .codegraph/ 时，理解或定位代码先用 codegraph_explore（MCP），或在仓库根执行 `codegraph explore "<符号或问题>"`；需要时再用 rg。未索引则正常使用 rg，不自行建索引。
<!-- CODEGRAPH_END -->
