# Memory Layer

仓库内可 git 跟踪的"团队学习记录"。与 `/root/.claude/projects/.../memory/` 全局个人 memory **互补**：

| 维度 | 全局 memory (`~/.claude/...`) | 本仓 memory (`.claude/memory/`) |
|---|---|---|
| 范围 | 跨项目个人偏好 | 本项目团队共享 |
| Git 跟踪 | 否 | 是 |
| 谁能读 | 仅当前用户 | 任何 clone 仓库的人 + 任何 Claude session |
| 内容 | "我喜欢中文回复" / "我倾向 X 风格" | "项目曾踩过 close_resource 漏 close 的坑" |

## 三个文件

| 文件 | 用途 | 生命周期 |
|---|---|---|
| `corrections.jsonl` | 每次 session 中纠正 Claude 的事实记录 | append-only |
| `learned-rules.md` | 累积验证后的"草稿规则" | 50 行硬上限，超了 → 升级到 `.claude/rules/*.md` |
| `evolution-log.md` | `/evolve` 每次评审的决策审计 | append-only |

## 升级链

```
session 中遇到 correction
    ↓ Claude 手动 append (或用户 prompt 触发)
corrections.jsonl
    ↓ /evolve 评审
learned-rules.md (草稿，50 行内)
    ↓ 累积 ≥ 3 次或证据足
.claude/rules/<file>.md (可验证规则)
    ↓ 能机械化的
Cargo.toml [lints.clippy] / CI grep 检查
```

每升一级，"下次 session 起点更高一点点"。

## 如何用

### 在 session 里发现新 correction

直接 append 到 `corrections.jsonl`：

```jsonl
{"ts":"2026-05-09T16:32:00Z","session":"S103","context":"改 cifs.rs write_file","correction":"OverwriteIf 必须，否则 Samba ACCESS_DENIED","applied_to":["src/cifs.rs:1820"]}
```

### 评审升级

跑 `/evolve`：
- 读 corrections.jsonl 全部条目
- 找 ≥ 2 次出现的 pattern → 建议升级到 learned-rules.md
- 学到 ≥ 3 次 + 有 verify 命令 → 建议升级到 rules/
- 能 clippy lint → 建议加 `[lints.clippy]`

### 不要

- 不要在本层写**项目无关的通用知识** (Rust 规范等)。那应该在外部教程或 `~/.claude/`。
- 不要在 corrections.jsonl 写一次性的 PR 决策。那进 git log。
- 不要让 learned-rules.md 超 50 行 — 强制升级或删除。

## 种子数据 (S97-S102 教训)

`learned-rules.md` 已植入 4 条历史教训：

1. CIFS 资源句柄走 `close_resource` (S99)
2. CIFS `smb2_only` 默认 true (af0e017)
3. CIFS write 用 `CreateDisposition::OverwriteIf` (Samba 修复)
4. NFS retry taxonomy split (7eb3046)

随 session 演进，这些规则会逐步晋升到 `.claude/rules/` 或 `Cargo.toml`。
