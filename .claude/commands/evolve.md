---
description: 评审 .claude/memory/corrections.jsonl，建议升级到 learned-rules / rules / clippy lint。
---

# /evolve

读 `.claude/memory/corrections.jsonl` 全部条目，对比 `.claude/memory/learned-rules.md` 当前状态，输出建议清单 + 等用户确认 + 应用变更 + 追加 `.claude/memory/evolution-log.md`。

## 步骤

1. **加载**：
   - 读 `.claude/memory/corrections.jsonl` 全部行 (jsonl 格式)。
   - 读 `.claude/memory/learned-rules.md` 当前规则。
   - 读 `.claude/rules/` 目录下所有文件。
   - 读 `Cargo.toml` 的 `[lints]` 段。

2. **聚类**：
   - 把 corrections 按 `applied_to` 文件 + `correction` 关键词聚类。
   - 找重复 ≥ 2 次的模式 → 候选升级。
   - 找单次但有 verify 命令的 → 候选 rule。

3. **判据**：
   - 升级到 `learned-rules.md`：corrections 出现 ≥ 2 次，或单次但用户标 important。
   - 升级到 `.claude/rules/<file>.md`：learned-rules 中累积 ≥ 3 次，且有 verify 命令。
   - 升级到 `Cargo.toml [lints.clippy]`：能用 clippy lint 表达 (例如 unwrap_used / dbg_macro)。
   - 升级到 CI grep：能用 grep 检查的项目特定模式 (例如 "所有 close 必须配 close_resource")。
   - 丢弃：一次性 PR 决策 / 已过时 / 已被代码改动覆盖 (例如代码已重构掉相关 path)。
   - 合并：两条 corrections 描述同一规则的不同表达。

4. **data-mover-rs 特化判据**：
   - 涉及 `StorageEnum` 操作同步 → 优先升级 (高 ROI 防回归)。
   - 涉及 backend 特定陷阱 → 写到 `.claude/docs/storage-<backend>.md`。
   - 涉及 retry 决策 → 写到 `.claude/docs/error-taxonomy.md`。
   - 涉及 filter DSL 三元组 → 写到 `.claude/docs/filter-dsl.md`。
   - 涉及 `close_resource` / 时间转换 / URL redact → 已有 rule，跳过。

5. **输出建议清单**：

```
## 候选升级 (评审 N 条 corrections)

### 升级到 learned-rules.md
- L<n>: <规则一句话>
  - 来源: corrections #<list>
  - verify: <command>

### 升级到 rules/<file>.md
- ...

### 升级到 Cargo.toml lint
- ...

### 升级到 CI grep
- ...

### 丢弃
- corrections #<n>: <reason>

### 合并
- corrections #<a> + #<b>: <merged into ...>
```

6. **等用户确认** (用 AskUserQuestion)：批准 / 部分批准 / 全部跳过。

7. **应用**：
   - 写到对应 `.md` / `Cargo.toml` / `.github/workflows/`。
   - **不删除 corrections.jsonl 任何行** (append-only)。
   - 升级到 rules/ 后，learned-rules.md 中保留为 "graduated" 标记 (不删，留 trace)。

8. **追加 evolution-log.md**：

```
## YYYY-MM-DD HH:MM (reviewer: <claude session id>)
### 评审的 corrections
- <list>
### 决策
- 升级到 learned-rules: <list>
- 升级到 rules: <list>
- 升级到 lint/CI: <list>
- 丢弃: <list with reason>
- 合并: <list>
### 备注
<自由文本>
```

## 不要做

- 不要静默改 `.claude/rules/` — 必须先列建议、再让用户确认。
- 不要删 corrections.jsonl 任何行 — append-only。
- 不要让 `learned-rules.md` 超 50 行 — 超了必须升级或合并旧条目。
- 不要重复升级已经在 rules/ 的规则。

## 例子

```
$ /evolve

读取 12 条 corrections (S97-S103)，当前 learned-rules.md 7 条。

## 候选升级

### 已升级到 rules/ (无操作，trace 检查)
- L1 (close_resource): 已在 storage-driver-conventions.md D2，可从 learned-rules 删除以腾空间
- L2-L7 仍在观察期

### 升级到 CI grep (新建议)
- L4 (NFS EACCES → deny_list): 加 CI grep 检查 src/nfs.rs 是否把 EACCES 走 retry
  verify: `grep -nE 'EACCES.*retry|retry.*EACCES' src/nfs.rs` 应空

### 丢弃 (无)
### 合并 (无)

确认应用？(yes / partial / skip)
```
