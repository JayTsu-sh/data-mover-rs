---
description: 一键自检 — make ci + audit-large + audit-dispatch + harness-run (默认无网络)，失败时调 build-error-resolver。
---

# /verify

commit / PR 前的最后一道关。**所有检查必须绿才算 ready**。

## 步骤

1. **`make ci`** — `cargo fmt --check && cargo clippy -- -D warnings && cargo test && cargo build --examples && python3 .claude/skills/e2e-local/scripts/run.py`。
2. **`make audit-large`** — 列 >800 行的 src/*.rs，对比 baseline (filter 4849 / s3 3350 / nfs 3100 / cifs 2246 / storage_enum 1334 / local 1134) 是否新增 / 增长。
3. **`make audit-dispatch`** — 检查 `StorageEnum` 公开方法是否 4 backend 都实现。
4. **`harness-run` (默认无网络)** — 跑 `python3 .claude/skills/harness-run/scripts/runner.py`，包含 e2e-local + op-cancel + op-filter-dsl + quality-*。
5. **如果有失败**：
   - cargo build / clippy 失败 → 调 build-error-resolver agent
   - cargo test 失败 → 看具体测试，调 backend-specialist 或 filter-expert
   - audit-large 警告 (新文件 >800) → 调 architect 出拆分 PLAN
   - audit-dispatch 警告 (有 backend 缺操作) → 调 dispatch-checker

## 输出格式

```
VERIFY (data-mover-rs)

[1/4] make ci ........................ <PASS|FAIL>
      └─ fmt:                          <ok|fail>
      └─ clippy:                       <ok|N warnings>
      └─ test:                         <N passed|fail>
      └─ examples:                     <built|fail>
      └─ e2e-local:                    <ok|fail>

[2/4] audit-large .................... <CLEAN|WARN>
      └─ filter.rs: 4849 (baseline 4849) ✓
      └─ s3.rs: 3350 (baseline 3350) ✓
      └─ ...
      └─ <new file >800 if any>

[3/4] audit-dispatch ................. <CLEAN|WARN>
      └─ <列出未对齐的 StorageEnum 操作>

[4/4] harness-run (no-network) ....... <PASS|FAIL>
      └─ e2e-local:                    <ok>
      └─ op-cancel:                    <ok>
      └─ op-filter-dsl:                <ok>
      └─ quality-clippy:               <ok>
      └─ quality-coverage:             <N% covered>
      └─ quality-large-file-audit:     <ok>
      └─ quality-dispatch-coverage:    <ok>

VERDICT: SHIP IT | NEEDS WORK | BLOCKED
```

## 不要做

- 不要静默吞错 — 失败要明确报告。
- 不要跳过 audit (即使 ci 过) — 大文件 / 分派不齐是慢性病。
- 不要默认跑 e2e-network (除非用户传 `--include-network`)，避免凭据 / 网络依赖问题。
- 不要替代 reviewer agent — `/verify` 是机械化检查，reviewer 是语义 review，两者互补。

## 选项

- `/verify --include-network` — 同时跑 e2e-cifs / e2e-nfs / e2e-s3 (需 .env)
- `/verify --quick` — 只跑 fmt + clippy + test，跳过 e2e 和 audit
