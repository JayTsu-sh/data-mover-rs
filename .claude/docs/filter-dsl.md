# Filter DSL (filter.rs, 4849 行)

## 这是什么

`filter.rs` 是项目最大的单文件 (4849 行)，**不是简单的黑白名单**，是一个**剪枝决策树**：

> 给定当前条目和当前位置，回答"跳过 / 继续扫这层 / 子项还要不要过滤"。

## 三元组语义 (核心，不能错)

`should_skip()` 返回 `(should_skip, continue_scan, check_children)` 三元组：

| 元 | 含义 | 影响 |
|---|---|---|
| `should_skip: bool` | 当前条目是否跳过 | 单条目级别 |
| `continue_scan: bool` | 该目录是否继续往下扫 | 子目录递归 (仅对目录有意义) |
| `check_children: bool` | 子项是否还需要过滤 | 子目录递归 |

**三个分量独立**，不能互相推导。

例：
- `(true, true, true)` — 跳过当前但继续扫，子项也要过滤。
- `(true, false, false)` — 跳过当前且停止整个分支 (剪枝)。
- `(false, true, false)` — 收当前条目，子项不再过滤 (例如 "目录匹配后子项全收")。
- `(false, false, false)` — 收当前条目，停止扫描，子项不过滤。

**改 `should_skip` 前必须想清三个语义独立性**，并在 PR 描述中说明每种组合的预期行为。

> `continue_scan` 是"要不要把这个目录排进待扫队列"，不是"要不要继续扫同级兄弟"。
> 四个 legacy backend 的用法都是 `if is_dir && continue_scan { subdirs.push(..) }`；
> 默认返回值 `(false, is_dir, true)` 对文件给 `false`，若解释成"同级"则每个文件都会终止本层扫描。

### role-based traversal 的映射

`crate::DslTraversalFilter` (`src/filter_traversal.rs`) 把三元组映射到
`traversal::TraversalDecision`：

| 三元组 | `TraversalDecision` |
|---|---|
| `should_skip` | `emit = !should_skip` |
| `continue_scan` | `descend` |
| `check_children` | `filter_children` |

`filter_children = false` 是**传递性**的：整棵子树都不再调 `should_skip`，连 exclude 也绕过
(legacy `dir_tree.rs` 的 `need_filter` 同语义)。

被 filter 拦下的目录在遍历输出里是**可观测**的，`(emit, descend)` 两个分量各自对应不同的证据：

| `(emit, descend)` | 遍历输出 |
|---|---|
| `(false, true)` 隐藏但下钻 | 没有自己的 `Entry`，但有 `DirectoryListed` + `SubtreeComplete`；父目录的 listing 报 `Filtered` |
| `(true, false)` 发出但不下钻 | 有 `Entry`，没有完成事件；只进父目录的 `DirectoryListed.pruned_children`，**不算** `Filtered` |
| `(false, false)` 两者皆无 | 既无 `Entry` 也无完成事件；父目录同时报 `Filtered` 和 `pruned_children` |

所以 `Filtered` 只意味着"有子项没被输出"，与"有子树没被下钻"是两件事，别把它们合起来读 ——
下游判断"能不能安全删除目的端多余条目"看的是 `SubtreeSummary::is_exhaustive()`，它要求隐藏、
剪枝、截断、失败四者全为零。

`FilterInput` 各字段的取值口径（与 legacy walkdir 对齐）：

| 字段 | 取值 |
|---|---|
| `file_path` | **相对遍历根**，`/` 分隔、无前导 `/`；Local 的 `./` 前缀会被剥掉。不是条目最终发布的 backend 相对路径 |
| `file_name` | 最后一段 |
| `file_type` | `file` / `dir` / `symlink` / `special` |
| `extension` | 与 `Path::extension` 一致：最后一个 `.` 之后，且该 `.` 前必须有内容，所以 `.bashrc` 没有扩展名。永不为 `None` |
| `size` | 目录取 `Some(0)`，否则 `size` 条件会退化成放行的 `LazyMatch` |

**`modified` 缺失时不能先评估**：缺 `modified` 的 `Modified` 条件返回 `LazyMatch`，而
`LazyMatch` 会被组合子吸收 (`Match & Lazy = Match`、`MisMatch | Lazy = MisMatch`)，
结果既不是上界也不是下界 — 可能误剪枝，也可能误收。因此
`FilterExpression::referenced_fields().modified` 为真时，traversal 先做元数据观察再决策
(`TraversalFilter::needs_modified`)，绝不用缺 `modified` 的评估结果。

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

role-based 路径同理：`traversal` 模块不依赖 `crate::filter` (架构依赖校验禁止)，
只认 `TraversalFilter` trait；DSL 适配器在 crate 根的 `src/filter_traversal.rs`。

只有一个遍历实现接这个接缝：`traversal::StorageTraversalSource`，出借 Namespace 角色的
backend (Local / NFS / CIFS / HDFS) 都走它。
可运行入口见 `examples/storage_role_operations.rs`。

## 传输 artifact 不进三元组

所有列举（legacy 与 role-based，全部后端）在调用 `should_skip` **之前**就丢掉相对根任一段以 `.data-mover-`
开头的条目 (ADR-0006 C3b)。放在后面的话，「跳过但继续下探」分支会走进 `.data-mover-stage/` 把里面名字普通的
子项报出来。三元组语义本身不变。

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
