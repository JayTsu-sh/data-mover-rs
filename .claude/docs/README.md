# .claude/docs/ 索引

按场景路由的项目知识层。每个 doc ≤400 行，**只写这个项目特有**的事。

> 入口在 `/CLAUDE.md`。本 README 是 `.claude/docs/` 内部目录。

## 路由表 (镜像 CLAUDE.md)

| 场景 | 文件 |
|---|---|
| 新人/新 session 起手 | [codebase.md](codebase.md) |
| 整体数据流 / 并发模型 | [architecture.md](architecture.md) |
| **新增/修改 `StorageEnum` 操作** | [storage-enum-dispatch.md](storage-enum-dispatch.md) |
| 改 CIFS | [storage-cifs.md](storage-cifs.md) |
| 改 NFS | [storage-nfs.md](storage-nfs.md) |
| 改 S3 | [storage-s3.md](storage-s3.md) |
| 改 Local | [storage-local.md](storage-local.md) |
| 改 filter DSL | [filter-dsl.md](filter-dsl.md) |
| 改 walk 调度 | [walk-scheduler.md](walk-scheduler.md) |
| 改 error / retry | [error-taxonomy.md](error-taxonomy.md) |
| 写 commit / PR | [conventions.md](conventions.md) |
| 加 skill / 升级 rule / 加 agent | [claude-onboarding.md](claude-onboarding.md) |

## 何时新增一个 doc

判据 (满足任一即可)：

1. **某主题在 3 个以上 session 重复被问** → 抽 doc。
2. **某代码区域 >800 行**，且改它需要"先理解 X 才能改 Y"的隐性知识 → 抽 doc。
3. **某协议/外部依赖有 ≥3 个非显式约束** (例如 SMB 的 smb2_only / anon / file_id) → 抽 doc。
4. **某规则需要长说明或图示**，超出 `.claude/rules/` 一行能装的 → 抽 doc。

不抽的判据：

- 单文件单函数能解释清楚 → 写代码注释，不抽 doc。
- "Rust 是什么" / "什么是 async" 这类通用知识 → 不抽，外部教程已经讲。
- 一次性的 PR 决策 → 写 commit message，不抽 doc。

## 何时合并 doc

- 两个 doc 互相 jump 超过 3 次 → 考虑合并。
- 某 doc <50 行 ≥ 1 个月 → 合并到相邻 doc。

## 何时拆 doc

- 单 doc 超 400 行 → 拆子主题。
- 单 doc 路由 >10 个 sub-section → 抽出最大的 section。
