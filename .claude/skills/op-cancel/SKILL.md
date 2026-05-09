---
name: op-cancel
description: 测取消语义 — 跑 tests/test_copy_file_cancel，三场景 (token 预取消、mid-transfer、completion)。验证 Cancelled ≠ Error。
---

# op-cancel

**触发关键词**：测取消语义 / 验证 Cancelled / cancel 测试 / op cancel。

## 步骤

1. cargo test --test test_copy_file_cancel
2. 检查日志：取消应返回 `StorageError::Cancelled` 而不是其他错误。
3. 检查残留：取消后目标文件应被清理 (或 partial 状态明确)。

## 成功判据

- 测试退出码 = 0
- 三个场景都覆盖 (pre / mid / post cancel)

## 失败如何排查

- "Cancelled" 被当 Error → backend 没识别 token，看 `src/{backend}.rs::copy_file_with_cancel` 的 token check
- mid-transfer 没响应 cancel → 检查 token poll 频率 (太低则取消滞后)
- 残留垃圾 → 取消路径需要 cleanup (truncate / delete partial)
