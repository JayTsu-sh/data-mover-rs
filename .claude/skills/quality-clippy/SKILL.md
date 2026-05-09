---
name: quality-clippy
description: 跑 cargo clippy --all-targets，对比 baseline_count.txt — 允许下降，不允许新增。准备 PR 前必跑。最终目标 baseline = 0。
---

# quality-clippy

**触发关键词**：查 clippy / 跑 lint / 准备 PR / quality clippy。

## 策略

项目当前有 ~173 条 clippy warnings/errors (大多 pedantic / doc 格式 / long literal)。强制 `-D warnings` 会让 CI 一直红，不可用。

折衷：**baseline 化** — 总数 ≤ baseline 即 PASS。逐步下降，最终目标 = 0。届时把命令改回 `-D warnings`。

## 步骤

1. cargo clippy --all-targets (不加 -D)
2. 数 warning+error 数 (regex `^(warning|error):`)
3. 对比 `baseline_count.txt`
4. > baseline → FAIL；≤ baseline → PASS；明显下降 (≥ 5) → 提示更新 baseline。

## 锁定下降 (推荐做法)

当一次 PR 修了 N 条 clippy，把 baseline 锁回去防回退：

```bash
# 跑一次拿到当前数量
python3 .claude/skills/quality-clippy/scripts/run.py
# 把数字写回 baseline
echo <new-count> > .claude/skills/quality-clippy/baseline_count.txt
git add .claude/skills/quality-clippy/baseline_count.txt
```

## 终极目标

baseline = 0。届时：
- 修改 `scripts/run.py` 为 `cargo clippy --all-targets -- -D warnings`
- 删除 baseline_count.txt
- Cargo.toml 把 `pedantic = "warn"` 改 `"deny"` (可选，看团队意愿)

## 备注

- `unwrap_used` / `expect_used` 已是 deny — 这两个不进 baseline，新增直接编译失败。
- `dbg_macro` / `todo` / `unimplemented` warn — 算入 baseline 数量。
- pedantic 已 allow 部分 (module_name_repetitions / too_many_lines / cast_*) — 不会贡献 baseline。
