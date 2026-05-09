# Filter DSL Rules

针对 `src/filter.rs` (4849 行) 的硬规则。

## F1 · `should_skip` 三元组改动必须先更新 doc

**verify**: 改 `should_skip` 签名或语义的 PR 必须改 `.claude/docs/filter-dsl.md` 的"三元组语义"段。
**why**: 三元组三个分量独立，改一个影响三种使用场景。doc 里的组合表是真相之源。
**how to apply**: PR 描述必须列：
- 新的三元组语义。
- 变化前后的组合表对比。
- 谁会受影响 (walkdir / walkdir_2 / 哪个 backend)。

## F2 · 新增 lexer token 必须加单测

**verify**: `grep -A 2 -nE 'fn tokenize' src/filter.rs` 内部的 token 类型枚举更新时，`mod tests` 必须有对应 token 测试。
**why**: lexer 错 silently fail (或返回 InvalidToken)，没单测可能 long tail 才被发现。
**how to apply**: 加 `Identifier` / `String` / `Number` / `Operator` / `Keyword` 之外的新 token 时同时加 `test_tokenize_<token_name>` 单测。

## F3 · glob 必须用 `GLOB_MATCH_OPTIONS` 常量

**verify**:

```bash
grep -nE 'glob::MatchOptions\s*\{' src/filter.rs | grep -v GLOB_MATCH_OPTIONS
```

应空。

**why**: 散写 MatchOptions 容易漏 `require_literal_separator: true`，导致 `*.rs` 意外匹配 `sub/foo.rs`。
**how to apply**: 所有 glob 调用 `pattern.matches_with(name, GLOB_MATCH_OPTIONS)`。

## F4 · 时间表达式默认 UTC

**verify**: 人工 review。`grep -nE 'Local::now|chrono::Local' src/filter.rs` 应空 (用 `Utc::now`)。
**why**: 用户跨时区使用，filter 表达式语义必须确定。
**how to apply**: 表达式中 `"2024-01-01"` 解析为 `2024-01-01T00:00:00Z`。如果未来允许显式时区，必须扩 DSL 语法 (例如 `"2024-01-01"@+08`) — 不要静默改默认。

## F5 · `dir_matches_date_filter` 递归边界覆盖

**verify**: `mod tests` 内必须有：

- `2024-*` 匹配 `2024-01` 子目录。
- `2024-01-*` 匹配 `2024-01-15` 但不匹配 `2024-02-15`。
- 嵌套：`logs/2024/01/15/*.log` 在 filter `modified > -7d` 下能正确剪枝过期月份目录。

**why**: 这是性能优化的关键 — 错了 → walk 全量目录 → 慢 100×。
**how to apply**: 改这个函数前先读现有单测，加测覆盖新逻辑。

## F6 · DSL 解析错误必须返回精确变体

**verify**: 错误必须是 `InvalidFilterExpression` / `MismatchedParentheses` / `InvalidToken` / `UnexpectedEofToken` 之一，不要 `OperationError`。

**why**: 上游 (CLI / API) 能给用户清晰的错误提示。
**how to apply**: lexer 错 → `InvalidToken` 或 `UnexpectedEofToken`。parser 错 → `MismatchedParentheses` 或 `InvalidFilterExpression`。语义错 (例如类型不匹配) → `InvalidFilterExpression`。

## F7 · 拆分 filter.rs 前调 `architect`

filter.rs 是拆分候选 #1 (4849 行)。拆分前：

1. 调 `architect` agent 出 PLAN。
2. 拆为 `filter/{lexer, parser, eval, glob, time}.rs` 五个子模块。
3. **公开 API 不变** — 外部仍 `crate::filter::Filter` / `crate::filter::should_skip`。
4. 单测分布到子模块，整体测试在 `filter/mod.rs` 入口。
5. PR 标 `refactor(filter): split into submodules` (纯 refactor 不混 fix)。
