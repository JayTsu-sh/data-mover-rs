# Evolution Log

`/evolve` 每次执行的审计记录。append-only。

格式：

```
## YYYY-MM-DD HH:MM (reviewer: <name>)
### 评审的 corrections
- <ID 或时间戳列表>
### 决策
- 升级到 learned-rules.md: <list>
- 升级到 rules/<file>.md: <list>
- 升级到 Cargo.toml lint / CI grep: <list>
- 丢弃 (一次性 / 已过时): <list with reason>
- 合并 (重复): <list>
### 备注
<可选自由文本>
```

---

## 2026-05-09 16:33 (reviewer: harness-init)
### 评审的 corrections
- S97-S103 全部 12 条种子记录

### 决策
- 升级到 `.claude/rules/storage-driver-conventions.md` D2: L1 (close_resource)
- 写入 `.claude/docs/storage-cifs.md`: L2, L3, L6, L7
- 写入 `.claude/docs/error-taxonomy.md`: L4, L5
- 留在 learned-rules.md 观察: L1-L7 仍保留 (作为 trace)
- 丢弃: 无
- 合并: 无

### 备注
本次是 harness 初始化的种子评审。corrections.jsonl 内容是从 S97-S103 历史
session 提炼的，不是真实当时的 append。后续 session 应在遇到 correction 时
当场 append。

下次 /evolve 评审建议关注：
- L1 已升级到 rules，但仍在 learned-rules.md 留 trace。下一次评审可考虑删除。
- L2/L3/L6/L7 都是 cifs 特定，是否合并出 `.claude/rules/cifs-specific.md` 还是
  保持 docs 化。
- L4/L5 是 retry 决策，是否做成 CI grep 检查 (例如 grep "NoSuchKey" 必须配
  FileNotFound) 值得评估。

## 2026-05-09 16:55 (reviewer: harness-init verification pass)
### 验证发现 (harness 暴露的项目 backlog)
1. **clippy baseline = 173** (`quality-clippy/baseline_count.txt`)
   - 项目积累的 pedantic / doc 格式 / long literal 警告
   - 决策：baseline 化，逐步下降，最终目标 0
   - 不阻塞 CI / verify

2. **dispatch holes = 9** (`quality-dispatch-coverage/baseline_holes.json`)
   - S3 缺 rename/create_symlink/read_symlink/delete_file 等 (语义不适用)
   - check_connectivity 缺 nfs/local
   - set_metadata 缺 cifs/nfs/s3
   - 决策：baseline 化为已知 holes，新增 hole = 回归
   - 后续工作：每个 hole 应该补 `Err(StorageError::UnsupportedType(...))` 实现
     而不是缺失 fn —— 让编译器抓回归而不是脚本

3. **cargo fmt 不通过** (verification 跑 `make ci` 时暴露)
   - 项目 src/ + tests/ 有未格式化代码
   - 决策：不在 harness init 范围内修 (avoid touching business code)
   - 用户应跑 `cargo fmt --all` 把项目格式化一次后才能完整 PASS make ci
   - 等价：在第一个真实 PR 中加 fmt-fix commit

### 验证通过项
- ✓ Layer 0 CLAUDE.md 写完
- ✓ Layer 1 13 个 docs 写完
- ✓ Layer 2 4 条 rules 写完
- ✓ Layer 3 5 个 agents 写完
- ✓ Layer 4 skills 矩阵齐 (e2e + op + quality + harness-run)
- ✓ Layer 5 3 个 commands 写完
- ✓ Layer 6 memory 骨架 + 12 条种子 corrections + 7 条 learned-rules
- ✓ Layer 7 Makefile + CI yaml + .gitignore + .claude/settings.json (hooks)
- ✓ harness-run 默认配置 7/7 skill PASS
- ✓ e2e-local 跑通 (cargo build --examples + 2 个 integration test)
- ✓ quality-large-file-audit PASS (baseline 锁 17 个文件)
- ✓ quality-dispatch-coverage PASS (9 已知 holes baseline)
- ✓ quality-clippy PASS (baseline 173, 等价 ≤ 现状)

### 后续 session 自然演进
- 跑 `cargo fmt --all` 让 make ci 完整 PASS
- 修 dispatch holes 一条条 → 删 baseline_holes.json 对应条目
- 修 clippy → 周期性下调 baseline_count.txt
- session 中遇到 correction → append corrections.jsonl → 后续 /evolve
