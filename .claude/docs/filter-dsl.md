# Filter DSL (filter.rs, 4849 行)

## 这是什么

`filter.rs` 是项目最大的单文件 (4849 行)，**不是简单的黑白名单**，是一个**剪枝决策树**：

> 给定当前条目和当前位置，回答"跳过 / 继续扫这层 / 子项还要不要过滤"。

## 三元组语义 (核心，不能错)

`should_skip()` 返回 `(should_skip, continue_scan, check_children)` 三元组：

| 元 | 含义 | 影响 |
|---|---|---|
| `should_skip: bool` | 当前条目是否跳过 | 单条目级别 |
| `continue_scan: bool` | 是否继续扫这一层 | 同级兄弟节点 |
| `check_children: bool` | 子项是否还需要过滤 | 子目录递归 |

**三个分量独立**，不能互相推导。

例：
- `(true, true, true)` — 跳过当前但继续扫，子项也要过滤。
- `(true, false, false)` — 跳过当前且停止整个分支 (剪枝)。
- `(false, true, false)` — 收当前条目，子项不再过滤 (例如 "目录匹配后子项全收")。
- `(false, false, false)` — 收当前条目，停止扫描，子项不过滤。

**改 `should_skip` 前必须想清三个语义独立性**，并在 PR 描述中说明每种组合的预期行为。

## 表达式形式

```
name matches "*.rs" AND size > 1M
modified > -7d
NOT (path contains "node_modules") AND extension = "ts"
modified > "2024-01-01" AND modified < "2024-06-30"
```

支持的操作：`AND` / `OR` / `NOT` / `()` / `>` / `<` / `=` / `!=` / `matches` / `contains`。

## Lexer

- 在文件前 ~400 行。
- Token 类型：`Identifier`, `String`, `Number`, `Operator`, `LParen`, `RParen`, `Keyword`。
- 错误：`StorageError::InvalidToken` / `MismatchedParentheses` / `InvalidFilterExpression`。

新增 token 必须：
1. 加 lexer 分支。
2. 加 parser 分支。
3. 加单测在 `mod tests` (filter.rs 内)。

## glob 匹配

```rust
const GLOB_MATCH_OPTIONS: glob::MatchOptions = glob::MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: false,
};
```

`require_literal_separator = true` 意味着 `*.rs` 不会匹配 `sub/foo.rs`。这是有意的 — 允许用 `**` 显式跨层。

**所有 glob 调用必须用 `GLOB_MATCH_OPTIONS` 常量**，禁止散写 `MatchOptions { ... }`。

## 时间条件

```rust
enum ModifiedValue {
    RelativeDays(f64),     // 例: -7d, +30d
    AbsoluteEpoch(i64),    // Unix 秒
}
```

- `RelativeDays(-7.0)` = "7 天内修改的"。
- `AbsoluteEpoch(1704067200)` = "2024-01-01 UTC 之后修改的"。

混用：表达式 parse 时统一为 `AbsoluteEpoch` (相对值用当前时间换算)。

## 目录名匹配

`dir_matches_date_filter` 支持递归匹配目录命名模式：

- `2024-01-*` 匹配 `2024-01-01`, `2024-01-15` 等子目录。
- 用于按日期分桶的归档目录加速 (不进不可能匹配的目录)。

修改这个函数前先看现有 4-5 种命名模式的处理。

## 与 walkdir 的关系

```
walkdir 流水线
  │
  ├──> enumerate next entry
  │       │
  │       ▼
  │   filter.should_skip(entry)
  │       │
  │       ├──> (skip=t, scan=f, children=*) → 剪枝整个分支
  │       ├──> (skip=t, scan=t, children=t) → 跳过但继续
  │       ├──> (skip=f, ..., children=t) → 收集 + 子项过滤
  │       └──> (skip=f, ..., children=f) → 收集 + 子项全收
```

walk_scheduler 不感知 filter — filter 是 walkdir 实现层调的，走 enum dispatch 后由各 backend 注入。

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| 散写 glob 选项 | 必须用 `GLOB_MATCH_OPTIONS` |
| 三元组某一元改了忘了对应另两元 | PR 描述必须列三元组组合表 |
| 时间字符串 timezone 默认 | 默认 UTC，文档化 |
| `dir_matches_date_filter` 递归边界 | 加单测覆盖 `2024-*` vs `2024-01-*` 区别 |
| 大表达式 stack overflow | parser 限制嵌套深度 |

## 改 filter.rs 时

1. 调 `filter-expert` agent (本项目专设)。
2. 输出契约：`EXPRESSION-PATH / TUPLE-IMPACT / EDGE-CASE / TEST-NEEDED`。
3. 必须在 PR 描述列三元组组合表。
4. 必须加单测覆盖修改的语义。
5. 跑 `make e2e-local` + `cargo test filter::`。

## 拆分候选

filter.rs 4849 行可拆为：
- `filter/lexer.rs` — token 化 (~400 行)。
- `filter/parser.rs` — AST 构造。
- `filter/eval.rs` — `should_skip` 三元组实现。
- `filter/glob.rs` — `GLOB_MATCH_OPTIONS` + `dir_matches_date_filter`。
- `filter/time.rs` — `ModifiedValue` + 时间表达式。

拆分前调 `architect` agent 出 PLAN，确保不破坏对外 API。
