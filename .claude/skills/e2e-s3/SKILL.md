---
name: e2e-s3
description: 验证 S3 backend — 跑 examples/s3_walkdir，特别验证 404 → FileNotFound (commit 7eb3046) 和 multipart upload。
---

# e2e-s3

**触发关键词**：验证 S3 / 测 multipart / 测 404 映射 / e2e s3。

## 输入 (.env)

```
S3_HOST=s3.amazonaws.com         # 或 minio.local 等
S3_BUCKET=my-bucket
S3_AK=ACCESS_KEY
S3_SK=SECRET_KEY
S3_USE_HTTPS=false               # true → s3+https://
S3_PREFIX=test                   # bucket 内的子路径
```

## 步骤

1. cargo build --example s3_walkdir
2. s3_walkdir 列 bucket — 应返回。
3. **404 验证**：用 `s3_walkdir <bucket>/this-key-does-not-exist-{timestamp}` 触发 NoSuchKey，应映射为 `FileNotFound`，**不重试**。

4. **写入策略矩阵**（role-based 目的端）：`bash .claude/skills/e2e-s3/scripts/staged_matrix.sh` ——
   大小 0 / 1 KiB / 8 MiB / 8 MiB+1 / 20 MiB / 200 MiB × Checkpointed / AtomicReplace / Direct × 读回开关，原生 S3→S3，
   取消与 SIGKILL 后续传。每行打印结果（含 `prepare` / `reused_bytes`）、耗时和遗留：`artifacts=`（前缀下的
   `.data-mover-*` 对象：`.upload` 指针、原生拷贝的 temp key）、`records=`（本地恢复记录，C15c 起 S3 恒为 0）、
   `key_uploads=`（该用例精确 key 上未完成的上传）。续传段另打印 `left at the destination: pointers=… key_uploads=…`
   （切断后期望 1 / 1，续传完成后 0 / 0）。只写/删 `staged-<run>/`
   （`S3_MATRIX_PREFIX=` 可改，必须以 `staged-` 或 `data-mover-` 开头 —— 清理会 abort 本次写过的每个 key 上的上传，再删
   前缀下的一切）。Direct 行（ADR-0006 C14c 起必须成功）另打印 `direct: equal=… key_uploads=…`：下载比对源文件、
   数该精确 key 上未完成的上传，期望 `equal=yes key_uploads=0`。
   MinIO 2023 的 `ListMultipartUploads` 只认精确 key，所以脚本都按精确 key 数上传（按前缀列再过滤 Key）。
   基线与解读见 `.claude/docs/storage-s3.md`「目的端写入策略基线」。

5. **容器重启后续传**：`DEST=s3:data-mover-<ts> CUT_MS=12000 bash .claude/skills/_shared/resume_matrix.sh`。
   ADR-0006 C15c 起 S3 的恢复状态在目的端（最终 key 上的 upload + `.data-mover-<d>.upload` 指针），续传**不需要**
   `DATA_MOVER_RECOVERY_DIR`：期望本地记录 0、切断后 `1 objects + 1 open uploads`、续传 `Resumed{…}`、
   reused + streamed = SIZE、BLAKE3 相等、之后 0 / 0。S3 的 checkpoint 数的是服务端确认的分段（读端最多领先 4 段），
   默认 6 s 切断时还不到 64 MiB，续传会是 `Restarted{StageWithoutPointer}`，所以用 12 s。

## 成功判据

- s3_walkdir 列 bucket 退出码 = 0
- 不存在的 key 返回 FileNotFound，日志中无 retry/backoff

## 失败如何排查

- 401/403 → ak/sk 错或 IAM 权限不够
- 404 走了 retry → S3 错误映射回归，看 `src/s3.rs` 中 GetObject/HeadObject 错误处理
- multipart 失败留垃圾 → 必须 abort，看 s3.rs multipart upload 路径
- 续传的 write 报 `NoSuchUpload`（NotFound）→ 自己的 upload 被当成别人的 abort 了：MinIO 列出的 upload id 与签发的
  写法不同，比较必须走 `upload_pointer::same_upload`
- 续传总是 `Restarted{StageWithoutPointer}` → 切断早于第一个 64 MiB checkpoint（服务端确认的分段），加大 `CUT_MS`
- 自签证书拒绝 → 用 s3+https 不是 https；检查 hyper-rustls verifier 配置
