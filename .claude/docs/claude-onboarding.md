# Claude 入职手册

> 这是给 *新加入项目的 Claude session* 的元手册：怎么用 .claude/ 里的东西。

## 第一次进入 session

1. 读 `/CLAUDE.md` (项目根的路由表)。
2. 按用户提的场景，从路由表找对应 doc，读 1-2 个。
3. 不要预先把 `.claude/docs/` 全读完 — token 浪费。

## 何时调哪个 agent

| 场景 | Agent | 输出契约 |
|---|---|---|
| 跨多文件改动前 | `architect` | PLAN / CHANGE / CREATE / RISK / VERIFY |
| commit 前自检 | `reviewer` | CRITICAL/HIGH/MEDIUM/LOW + VERDICT |
| 改某 backend (cifs/nfs/s3/local) | `backend-specialist` | BACKEND / DIAGNOSIS / FIX / IMPACT |
| 改 filter.rs | `filter-expert` | EXPRESSION-PATH / TUPLE-IMPACT / EDGE-CASE / TEST-NEEDED |
| 改 StorageEnum 操作 | `dispatch-checker` | OPERATION / TOUCHED / MISSED / VERIFY |

## 何时调哪个 skill

| 场景 | Skill |
|---|---|
| "验证 Local backend" | `e2e-local` (CI 跑这个) |
| "验证 CIFS / 测 SMB2 / 测匿名" | `e2e-cifs` (需 .env) |
| "验证 NFS / 测 retry 分类" | `e2e-nfs` (需 .env) |
| "验证 S3 / 测 multipart / 测 404" | `e2e-s3` (需 .env) |
| "测取消语义" | `op-cancel` |
| "测 filter DSL" | `op-filter-dsl` |
| "查 clippy / 准备 PR" | `quality-clippy` |
| "查覆盖率" | `quality-coverage` |
| "审大文件" | `quality-large-file-audit` |
| "查 StorageEnum 分派完整性" | `quality-dispatch-coverage` |
| "跑全套验证" | `harness-run` |

## 何时调哪个 slash command

- `/route <场景>` — 调试路由表，输出"应该读哪些文件"。
- `/verify` — 一键 `make ci + audit`。
- `/evolve` — 评审 corrections.jsonl，升级到 learned-rules / rules。

## 加新 skill

复制 `e2e-local/` 模板：

```bash
cp -r .claude/skills/e2e-local/ .claude/skills/<new-skill>/
# 编辑 SKILL.md, scripts/run.py, .env.example
# 在 .claude/skills/harness-run/scripts/matrix.yaml 加一行
# 在 Makefile 加 e2e-<new> target
# 在 .claude/docs/claude-onboarding.md 表格加一行 (本文件)
```

SKILL.md 必含字段：
- `name`
- `description` (semantic trigger 字串，决定 Claude 何时调用)
- `inputs` / `constants`
- `steps`
- `success criteria`

## 加新 rule

1. 在 `.claude/memory/corrections.jsonl` 找 ≥ 2 次出现的 correction。
2. 写到 `.claude/memory/learned-rules.md` (50 行硬上限)。
3. 累积 ≥ 3 次或证据足后，提炼到 `.claude/rules/<file>.md`，配 verify 命令。
4. 能机械化检查的，进 `Cargo.toml [lints.clippy]` 或 CI 里加 grep 检查。

## 加新 agent

`.claude/agents/<name>.md` 必含：

```markdown
---
name: <name>
description: <何时调用，写得越具体越好>
tools: [Read, Grep, Glob, Bash]   # 白名单
---

<role 描述>

# 输出契约
<必须严格遵守的输出格式>

# 项目特化指令
<本项目的具体规则>
```

## 加新 doc

判据见 [README.md](README.md) 的"何时新增 doc"段。

总原则：写 *project-specific* 事实，不写 Rust/Linux/网络通用知识。

## 升级链使用

```
session 中遇到 correction
    ↓
我手动追加到 .claude/memory/corrections.jsonl
    ↓
积累几次后，跑 /evolve
    ↓
评审建议 → 升级到 learned-rules.md (草稿) 或直接 rules/*.md
    ↓
能机械化的 → clippy / grep CI
```

每次 session 退出前问自己：**今天有没有学到什么应该写下来的？** 有的话 append corrections.jsonl。

## 不要做

- 不要把 `.claude/docs/` 当 commit log 写 (commit log 用 git log)。
- 不要在 `.claude/rules/` 写没有 verify 命令的"建议"。
- 不要在 CLAUDE.md 堆知识 (堆 doc 里，CLAUDE.md 只路由)。
- 不要把全局 `~/.claude/` 的东西复制到本仓 — 全局是个人偏好，本仓是团队共享。
