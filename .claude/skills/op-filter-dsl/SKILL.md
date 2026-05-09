---
name: op-filter-dsl
description: 测 filter DSL — 跑 cargo test filter::，覆盖 lexer / parser / should_skip 三元组 / glob / dir_matches_date_filter。改 filter.rs 必跑。
---

# op-filter-dsl

**触发关键词**：测 filter DSL / 验证 should_skip / 三元组测试 / 测 lexer / op filter。

## 步骤

1. cargo test filter:: — 跑 filter.rs 内嵌单测。
2. (可选) 跑 fixture 测试覆盖 dir_matches_date_filter 的递归边界。

## 成功判据

- 单测退出码 = 0
- 测试输出包含 "test result: ok" 且 0 failures

## 失败如何排查

- lexer 测试失败 → token 改了没改测试
- 三元组测试失败 → should_skip 行为回归，看 `.claude/docs/filter-dsl.md` 三元组组合表
- glob 测试失败 → `GLOB_MATCH_OPTIONS` 被改 / 散写 MatchOptions 漏了 require_literal_separator
- dir_matches_date_filter 测试失败 → 递归边界逻辑改了，看 `.claude/rules/filter-dsl-rules.md` F5
