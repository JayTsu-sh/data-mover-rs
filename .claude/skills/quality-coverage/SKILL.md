---
name: quality-coverage
description: 跑 cargo llvm-cov 计算覆盖率。当前阈值 30% (启动)，目标 60%。需要 cargo-llvm-cov 安装。
---

# quality-coverage

**触发关键词**：查覆盖率 / 跑 coverage / quality coverage。

## 前置

```
cargo install cargo-llvm-cov
rustup component add llvm-tools-preview
```

## 步骤

1. cargo llvm-cov --workspace --json (json 输出便于解析阈值)
2. 解析覆盖率百分比，对比阈值。
3. (可选) 跑 --html 生成可视化报告到 target/llvm-cov/html。

## 成功判据

- 当前：line coverage ≥ 30%
- 目标：line coverage ≥ 60%

## 备注

- 测试覆盖率受限于"不能 mock 网络 backend"约束，4 个 backend 中只有 Local 能纯单测。
- filter / time_util / error / url_redact / checksum 这些纯逻辑模块覆盖率应高。
- backend 的 walkdir / copy 流复杂，单测覆盖率天然低。
