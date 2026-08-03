---
name: quality-large-file-audit
description: 报告 src/*.rs 相对 baseline 的规模趋势；仅新文件超过 1000 行会阻塞。
---

# quality-large-file-audit

**触发关键词**：审大文件 / 找拆分候选 / quality large file。

## Baseline (2026-08-03)

```
filter.rs        5303
s3.rs            4329
nfs.rs           3853
cifs.rs          3152
storage_enum.rs  2246
local.rs         1423
integrity_check.rs 1372
```

新写的生产代码 ≤1000 行 / 文件。历史文件的增长只产生趋势报告，不作为
CI 硬门禁；`integrity_check.rs` 等内联测试模块也遵循这一规则。

## 步骤

1. wc -l src/*.rs
2. 列 >800 行的文件，标 (NEW / GROWING / SHRINKING / SAME)。
3. 警告 NEW（新文件 >800）和 GROWING（相对 baseline >5%，>10% 标为高增长）。

## 成功判据

- 不允许新文件 >1000 (NEW = FAIL)
- 现有文件的增长只报告 WARN，不阻塞 CI

## 备注

- 拆分候选 #1: filter.rs (调 filter-expert + architect)
- 拆分候选 #2: s3.rs (按 GetObject / PutObject / Multipart / List 拆)
- 拆分候选 #3: nfs.rs (按 v3 / v4 / Mount / Auth 拆)
- 拆分候选 #4: cifs.rs (按 Connection / FileOps / DirOps 拆)
