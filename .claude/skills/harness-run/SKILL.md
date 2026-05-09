---
name: harness-run
description: 跑 data-mover-rs 全套 skill 矩阵。默认无外部环境 (e2e-local + op-* + quality-*)，--include-network 加跑 e2e-cifs/nfs/s3。准备发布前自检。
---

# harness-run

**触发关键词**：跑全套验证 / 准备发布 / harness run / 全套 skill。

## 输入

- `--include-network` (可选): 加跑 e2e-cifs / e2e-nfs / e2e-s3 (需各 skill 的 .env)。
- `--quick` (可选): 只跑 quality-* 和 op-*。

## 矩阵 (matrix.yaml)

无外部环境组：
- e2e-local
- op-cancel
- op-filter-dsl
- quality-clippy
- quality-coverage
- quality-large-file-audit
- quality-dispatch-coverage

外部环境组 (需 --include-network)：
- e2e-cifs
- e2e-nfs
- e2e-s3

## 步骤

1. 顺序跑无外部环境组。
2. 如果 --include-network，再跑外部环境组 (并行可选)。
3. 汇总 markdown 报告到 stdout。

## 成功判据

- 所有跑过的 skill 退出码 = 0。
- coverage skip 不算失败 (cargo-llvm-cov 未安装可跳)。

## 输出格式

```
data-mover-rs harness-run report

| skill                       | status | duration |
|---|---|---|
| e2e-local                   | PASS   | 12.3s    |
| op-cancel                   | PASS   | 4.5s     |
| op-filter-dsl               | PASS   | 8.1s     |
| quality-clippy              | PASS   | 23.4s    |
| quality-coverage            | SKIP   | -        |
| quality-large-file-audit    | PASS   | 0.2s     |
| quality-dispatch-coverage   | PASS   | 0.1s     |

VERDICT: PASS (7/7, 1 skipped)
```
