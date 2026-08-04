---
name: quality-clippy
description: 对 lib 执行关键 deny + warning baseline，并额外检查 tests/examples。准备 PR 前必跑。最终目标 baseline = 0。
---

# quality-clippy

**触发关键词**：查 clippy / 跑 lint / 准备 PR / quality clippy。

## 策略

项目当前有 ~173 条 clippy warnings/errors (大多 pedantic / doc 格式 / long literal)。强制 `-D warnings` 会让 CI 一直红，不可用。

折衷：生产库的存量普通 warning 做 **baseline 化**；unwrap/expect/dbg/todo/unimplemented
始终为硬错误。tests/examples 运行 Clippy 并暂时只豁免 unwrap/expect，待其余存量
清零后切换为 `-D warnings`。

baseline 按平台分口径：Windows 多出 `cfg(windows)` 路径的 pedantic 警告，与 Linux 录制的数字不可比。
存在 `baseline_count.<sys.platform>.txt`（如 `baseline_count.win32.txt`）时优先于 `baseline_count.txt`。

## 步骤

1. 对 lib 运行 Clippy，五类关键 lint 使用 `-D`。
2. 统计 lib 的普通 warning+error 数，并对比 `baseline_count.txt`。
3. > baseline → FAIL；≤ baseline → PASS；明显下降 (≥ 5) → 提示更新 baseline。
4. 对 tests/examples 运行 Clippy，局部命令行豁免 unwrap/expect；Clippy 执行失败则 FAIL。

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

- 生产库的 `unwrap_used` / `expect_used` / `dbg_macro` / `todo` / `unimplemented`
  不进 baseline，新增直接编译失败。
- tests/examples 暂时允许 unwrap/expect，但仍运行其他 Clippy 检查。
- pedantic 已 allow 部分 (module_name_repetitions / too_many_lines / cast_*) — 不会贡献 baseline。
