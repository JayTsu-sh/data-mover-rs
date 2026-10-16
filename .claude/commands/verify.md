---
description: 一键自检 — make verify + harness-run；报告失败和环境限制。
---

# /verify

提交或 PR 前执行以下检查，全部通过才算 ready。

1. 运行 `make verify`。具体检查以 Makefile 为准；覆盖格式、架构依赖、编译、测试、示例、本地端到端和大文件审计。
2. 运行 `python3 .claude/skills/harness-run/scripts/runner.py`，完成默认无网络矩阵。
3. 对每个失败报告命令、关键错误和修复后的重跑结果。环境不足应标记 BLOCKED，不能报告通过。
4. 输出各项 PASS / FAIL / BLOCKED、测试数量及日志位置，给出整体结论。

存储能力通过 typed roles、后端能力声明及对应行为测试验证。旧的
`StorageEnum` 分派扫描已随旧 API 删除，不再作为检查入口。

`--include-network` 另跑 `make e2e-network`，需要配置实验环境。
`--quick` 只跑格式、clippy 和测试，并明确尚未完成完整验证。

机械化验证不能代替语义代码评审。
