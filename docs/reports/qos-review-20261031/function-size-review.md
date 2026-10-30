# Function size review supplement

审查 HEAD：`baac11505b0f0825afdb903d3bcf6f42ebc4d7c1`；固定基准：`4149575e91fe392ee853725e110d6c1be9c6b562`。

**结论：新增 1 项 P3；此前 standards-final-review.md 的“0 个未关闭 finding”应由本补充更正为“0 个安全／正确性阻塞，1 个未关闭规范 P3”。**

未找到 lab CLI、测试函数或测试 helper 对函数长度的既有例外。`CLAUDE.md:80` 要求新代码“≤50 行／函数”；`.claude/docs/conventions.md:93` 明确说明 Clippy `too_many_lines` 当前 allow，须人工 review；`.claude/agents/reviewer.md:40` 也将其列为人工必查。CLAUDE.md 中测试 helper 的例外只修饰 unwrap／expect，不适用于长度。

**P3 — 新增验收 CLI 函数明显超过 50 行，混合参数解析、准备、测量、判定和清理。** 代表位置（物理起止行计数，包含注释和空行）：

| 位置 | 函数 | 行数 |
|---|---|---:|
| `tests/lab/run-backend-qos.py:25` | `main` | 302 |
| `tests/lab/run-cross-backend-qos.py:22` | `main` | 135 |
| `tests/lab/run-local-qos.py:86` | `main` | 136 |
| `tests/lab/tools/backend_qos_acceptance.rs:327` | `main` | 101 |
| `tests/lab/tools/local_qos_acceptance.rs:135` | `main` | 103 |
| `crates/storage-core/src/runtime/qos.rs:264` | `schedule` | 71 |

Rust 的 `schedule` 是已有函数在本次改动中扩展后超限；原有超限代码不应继续堆大的规则同样适用。上表为代表性位置，不是对所有新增测试函数的长度穷举。

实际门禁：`quality-large-file-audit/scripts/run.py` 仅遍历 src 与 crates 的 Rust 文件并计算文件总行数，不计算函数，不覆盖 tests/lab Python，不能替代此条人工审查。我最初将函数长度归入工具检查项而省略，是审查遗漏。

最小合理收尾：另起纯 refactor commit，按现成阶段提取函数，保持计时边界与原执行顺序。Rust 把直接 read 测量提成返回 Measurement／CopyMeasurement 的函数，main 仅准备、分派、校验和输出；Python 按参数配置、单 profile performance、有限配额判定、cleanup 分开，使用一个明确的 run context 承载闭包当前共享状态；schedule 先提取锁获取／最终状态提交，保留持锁跨等待、取消检查和计数的顺序。避免同时改变阈值或调度算法。

拆分后沿用当前 34 项 Python 契约、Rust QoS／cleanup 契约及 imports 等价验证思路检查控制流，确认 timing 起止点未变化。没有新增性能行为便不需重跑数小时真实矩阵。

若本次决定暂缓重构，可将该 P3 如实保留为后续维护项，在合并说明写清“有非阻塞函数长度规范偏差”；这不等于 finding 已修复，也不构成仓库已有豁免。不能以 CI 通过宣称该项已满足。
