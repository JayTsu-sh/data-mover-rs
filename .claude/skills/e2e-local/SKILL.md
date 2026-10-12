---
name: e2e-local
description: 验证 Local backend — 跑 role-based 两种遍历 + local_opt_dir + tests/test_storage_type + tests/test_copy_file_cancel。无外部依赖，CI 必须跑这个 skill。
---

# e2e-local

**触发关键词**：验证 Local backend / 跑本地测试 / e2e local / 测本地 backend。

## 输入 / 常量

无 .env 需求 — 用 `/tmp/data-mover-skill-local` 作为临时目录。

`_shared/protocol_constants.py::Local` 提供前缀。

## 步骤

1. `timeout 900 cargo build --examples`
2. `timeout 900 cargo test test_storage_type`（URL 判别）
3. `timeout 900 cargo test test_copy_file_cancel`（取消语义）
4. `mkdir -p /tmp/data-mover-skill-local`，然后 `timeout 120 cargo run --example storage_role_operations -- --backend local --root /tmp/data-mover-skill-local seed`
5. `timeout 120 cargo run --example storage_role_operations -- --backend local --root /tmp/data-mover-skill-local traverse --order blocks`，再以 `--order unordered` 跑一次；两次都须 `Completed`、平衡计数且零失败。
6. `timeout 120 cargo run --example local_opt_dir -- /tmp/data-mover-skill-local`
7. 清理本次创建的 `/tmp/data-mover-skill-local`。

## 成功判据

- 所有 cargo run / cargo test 退出码 = 0
- /tmp 目录被正确清理 (脚本结束时不留残留)

## 失败如何排查

- examples 编译失败 → cargo build --examples 单独跑看错
- test_storage_type 失败 → URL 判别逻辑回归，看 `tests/test_storage_type.rs` + `src/lib.rs::detect_storage_type`
- test_copy_file_cancel 失败 → CancellationToken 语义回归，看 `tests/test_copy_file_cancel.rs` + StorageEnum::copy_file_with_cancel
- 遍历失败 → 看 `src/storage/backends/local/namespace/` 与 `src/traversal/`。

## 改 Local 时调用此 skill

修改 `src/local.rs` / `src/acl.rs` / `src/traversal/storage/` 后必跑。
