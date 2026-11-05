# 提交与协作规范

- commit 使用 `<type>(<scope>): <subject>`，scope 可选，type 为 feat、fix、refactor、perf、docs、test、chore 或 ci；中英文描述均可，标识符保持英文，正文解释原因及验证。
- bug 修复与保持语义的结构重构分开提交；修复说明触发条件、复现和预期结果。
- PR 包含背景、最终改动、职责／能力影响、实际验证与未测环境、风险及文档同步；按 [架构 review](architecture-review.md) 给出具体结论。
- 命名沿用 Rust 习惯；公共接口有 rustdoc，复杂内部逻辑说明不变量和原因。编码、错误、日志、测试规范统一见 [开发规范](development.md)，连通性验收见 [故障约束](external-dependency-failures.md)。
- 凭据、原始测试日志、本机配置和临时计划保留仓库外或原有忽略范围，规范不承担会话日志用途。
