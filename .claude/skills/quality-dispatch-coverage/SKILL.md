---
name: quality-dispatch-coverage
description: 检查 src/storage_enum.rs 中所有公开方法是否在 4 个 backend (cifs/nfs/s3/local) 都有实现。enum dispatch 没有 trait 完整性强制，必须脚本检查。
---

# quality-dispatch-coverage

**触发关键词**：查 StorageEnum 分派完整性 / dispatch 检查 / 4 backend 一致性。

## 步骤

1. grep src/storage_enum.rs 列所有 `pub (async )?fn <name>` 方法名。
2. 对每个方法名，grep 4 个 backend 文件检查同名 fn 存在。
3. 列缺失。

## 成功判据

- 每个 storage_enum 公开方法在 4 个 backend 都能 grep 到。
- 报告 "OK" / "MISSING in <backend>" 一行一行。

## 备注

- 此 skill 不检查签名一致性 (那需要 rust-analyzer)，只检查"存在"。
- 签名不一致由 cargo check 抓 (在 storage_enum.rs 的 match 中调用时报错)。
- 如果某 backend 不支持某操作，应有 fn 实现且返回 `StorageError::UnsupportedType`，而不是缺失 fn。
