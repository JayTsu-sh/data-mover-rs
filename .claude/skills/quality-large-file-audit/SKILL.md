---
name: quality-large-file-audit
description: 列出 src/*.rs 中 >800 行的文件，对比 baseline (filter 4849 / s3 3350 / nfs 3100 / cifs 2246)。新文件 >800 或现有文件继续增长是 WARN。
---

# quality-large-file-audit

**触发关键词**：审大文件 / 找拆分候选 / quality large file。

## Baseline (2026-05-09)

```
filter.rs        4849
s3.rs            3350
nfs.rs           3100
cifs.rs          2246
storage_enum.rs  1334
local.rs         1134
```

新写的代码 ≤800 行 / 文件。已超的 6 个是 backlog。

## 步骤

1. wc -l src/*.rs
2. 列 >800 行的文件，标 (NEW / GROWING / SHRINKING / SAME)。
3. 警告 NEW (新文件 >800) 和 GROWING (>baseline+10%)。

## 成功判据

- 不允许新文件 >800 (NEW = FAIL)
- 现有文件增长 ≤ 10% baseline (GROWING ≤ 10% = WARN，> 10% = FAIL)

## 备注

- 拆分候选 #1: filter.rs (调 filter-expert + architect)
- 拆分候选 #2: s3.rs (按 GetObject / PutObject / Multipart / List 拆)
- 拆分候选 #3: nfs.rs (按 v3 / v4 / Mount / Auth 拆)
- 拆分候选 #4: cifs.rs (按 Connection / FileOps / DirOps 拆)
