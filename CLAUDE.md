# data-mover-rs

多源数据迁移核心库。4 backend (Local / NFS / S3 / CIFS) + filter DSL + work-stealing 并发遍历。
从 terrasync-rs 拆分独立 (commit `4289a15`)，仅库 (无 binary，无 GUI)。

> 这是 Claude Code 的项目入口。先读这页路由，再按场景按需展开 `.claude/docs/`。
> 不堆知识，只路由。深入内容见各 doc。

## 黄金法则

- **Grep first**。不预设接口形状，看代码再改。
- **Smallest change**。bug fix 与 refactor 分两个 commit。
- **enum-based dispatch 同步规则**：改一个 backend 操作 = `src/storage_enum.rs` + 该 backend.rs + 可能 `src/lib.rs` 导出 + `examples/` + `tests/` 五处同步。漏一处就是潜在编译错或行为分裂。
- **`.unwrap()` / `.expect()` 编译期 deny** (Cargo.toml `[lints.clippy]` 已强制)，仅 `#[cfg(test)]` 和测试 helper 例外。
- **资源句柄走 `close_resource` helper** (cifs.rs 已有)，不要裸 `.close()`。S99 教训。
- **`Cancelled` ≠ `Error`**，是 `CancellationToken` 信号，上游可重入队 (commit `7eb3046` split retry taxonomy)。
- **Backend 错误统一映射到 `StorageError` 24 个变体之一**。新增变体需要 PR 说明强需求。

## 场景路由表

| 场景 | 先读这些 |
|---|---|
| 改 CIFS `smb2_only`/`anon`/`file_id` 行为 | `.claude/docs/storage-cifs.md` + `src/cifs.rs` |
| 改 NFS v3/v4 retry 分类 | `.claude/docs/storage-nfs.md` + `.claude/docs/error-taxonomy.md` + `src/nfs.rs` |
| 改 S3 multipart / 404 / credential | `.claude/docs/storage-s3.md` + `src/s3.rs` |
| 改 Local rayon delete / Win ACL | `.claude/docs/storage-local.md` + `src/local.rs` + `src/acl.rs` |
| **新增/修改 `StorageEnum` 操作** | `.claude/docs/storage-enum-dispatch.md` + `src/storage_enum.rs` (1334 行) |
| 改 filter DSL (lexer / `should_skip` 三元组) | `.claude/docs/filter-dsl.md` + `src/filter.rs` (4849 行) |
| 改 walk 调度 / work-stealing | `.claude/docs/walk-scheduler.md` + `src/walk_scheduler.rs` + `src/async_receiver.rs` |
| 改 error 变体或 retry 映射 | `.claude/docs/error-taxonomy.md` + `src/error.rs` |
| 改时间转换 (FileTime / NFS Time) | `src/time_util.rs` (单文件直读) |
| 改 ACL / xattr | `src/acl.rs` + `.claude/docs/storage-{nfs,local}.md` |
| 改 QoS / 速率限制 | `src/qos.rs` (用 governor crate) |
| 改 checksum / 完整性 | `src/checksum.rs` (用 blake3) |
| 改 tar 打包 | `src/tar_pack.rs` |
| URL 红化 / 日志脱敏 | `src/url_redact.rs` |
| 写 commit / PR | `.claude/docs/conventions.md` |
| 跑某 backend 验证 | `.claude/skills/e2e-{cifs,nfs,s3,local}/SKILL.md` |
| 跑取消语义 / filter DSL 测试 | `.claude/skills/op-{cancel,filter-dsl}/SKILL.md` |
| 跑全套 (无外部环境) | `.claude/skills/harness-run/SKILL.md` 或 `make ci` |
| 大文件拆分候选 (filter 4849 / s3 3350 / nfs 3100 / cifs 2246) | 调 `architect` agent，filter 优先调 `filter-expert` |
| 改 `StorageEnum` 操作时查漏 | 调 `dispatch-checker` agent |
| commit 前自检 | 调 `reviewer` agent |
| 新增 skill / 升级 rule / 加 agent | `.claude/docs/claude-onboarding.md` |

## 强制约束

- **Edition 2024**，MSRV 跟 stable Rust。
- **Cargo.toml `[lints.clippy]`**：`pedantic = warn` + `unwrap_used = deny` + `expect_used = deny` + `dbg_macro / todo / unimplemented = warn`。
- **`[lints.rust]`**：`unsafe_code = deny`。新增 unsafe 必须有 SAFETY 注释 + PR 说明。
- **异步**：tokio (full)。**错误**：thiserror。**日志**：tracing。
- **依赖管理**：全 crates.io，无 git patch (与 terrasync-rs 不同)。升级 `smb` / `nfs-rs` / `aws-sdk-s3` 是真实风险。

## 文件大小现状 (backlog，不是新增红线)

| 文件 | LOC | 状态 |
|---|---|---|
| filter.rs | 4849 | 拆分候选 #1 (DSL lexer 可独立) |
| s3.rs | 3350 | 拆分候选 |
| nfs.rs | 3100 | 拆分候选 |
| cifs.rs | 2246 | 拆分候选 |
| storage_enum.rs | 1334 | dispatch 表，难拆 |
| local.rs | 1134 | 边缘 |

新写的代码遵守 `≤800 行 / 文件`、`≤50 行 / 函数`。已超的不强求当下拆，但**不要再继续堆大**。

## 升级路径 (lessons learned 的去向)

```
correction (用户在 session 里纠正)
    ↓
.claude/memory/corrections.jsonl  (每次 append)
    ↓ /evolve 评审
.claude/memory/learned-rules.md   (50 行硬上限)
    ↓ 提炼
.claude/rules/*.md                (可验证规则)
    ↓ 能机械化检查的
Cargo.toml `[lints.clippy]` 或 CI grep 检查
```

每一步都让"下次 session 起点更高一点点"。

## 进入 session 的标准动作

1. 读 CLAUDE.md (本页) 找路由。
2. 按场景读 1-2 个 `.claude/docs/`。
3. 跨多文件改动前调 `architect` agent。
4. 改某 backend 调 `backend-specialist` agent (传 backend 名)。
5. 改 `StorageEnum` 操作调 `dispatch-checker` agent。
6. 改 filter.rs 调 `filter-expert` agent。
7. commit 前调 `reviewer` agent，跑 `make ci`。
