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
   取消与 SIGKILL 后续传。每行打印结果（含 `prepare` / `reused_bytes` / `native_bytes` / `native_requests`）、耗时和遗留：
   `artifacts=`（前缀下的 `.data-mover-*` 对象：`.upload` 指针；C18 起原生拷贝不再有 temp key）、`records=`（本地恢复记录，C15c 起 S3 恒为 0）、
   `key_uploads=`（该用例精确 key 上未完成的上传）。续传段另打印 `left at the destination: pointers=… key_uploads=…`
   （切断后期望 1 / 1，续传完成后 0 / 0）。只写/删 `staged-<run>/`
   （`S3_MATRIX_PREFIX=` 可改，必须以 `staged-` 或 `data-mover-` 开头 —— 清理会 abort 本次写过的每个 key 上的上传，再删
   前缀下的一切）。Direct 行（ADR-0006 C14c 起必须成功）另打印 `direct: equal=… key_uploads=…`：下载比对源文件、
   数该精确 key 上未完成的上传，期望 `equal=yes key_uploads=0`。原生行（C18 起写最终 key）另打印
   `native: equal=… stage_objects=… key_uploads=…`，期望 `equal=yes stage_objects=0 key_uploads=0`。凭据经
   `curl -K <(…)` 传入，不上命令行。
   MinIO 2023 的 `ListMultipartUploads` 只认精确 key，所以脚本都按精确 key 数上传（按前缀列再过滤 Key）。
   基线与解读见 `.claude/docs/storage-s3.md`「目的端写入策略基线」。

5. **容器重启后续传**：`DEST=s3:data-mover-<ts> bash .claude/skills/_shared/resume_matrix.sh`。
   ADR-0006 C15c 起 S3 的恢复状态在目的端（最终 key 上的 upload + `.data-mover-<d>.upload` 指针），续传**不需要**
   `DATA_MOVER_RECOVERY_DIR`：期望本地记录 0、切断后 `1 objects + 1 open uploads`、续传 `Resumed{…}`、
   reused + streamed = SIZE、BLAKE3 相等、之后 0 / 0。C16 起 > 64 MiB 的 checkpointed upload 一开始就写指针，默认 6 s
   切断（早于第一个 64 MiB checkpoint）也会续传：cancel 与 SIGKILL 都 `Resumed{…}`（取消丢不到一段，SIGKILL 丢在途分段）。

   在版本化桶里跑：`S3_BUCKET_OVERRIDE=<临时桶>` 指向同一服务器上的另一个桶（绝不改 `.env`），每个模式另打印
   `final versions=… markers=… artifact entries=…`（ListObjectVersions：版本化桶期望 1 / 0 / 0；未开版本的桶把对象列成
   `"null"` 版本，也是 1 / 0 / 0）。

6. **版本化桶矩阵**（ADR-0006 C17）：`bash .claude/skills/e2e-s3/scripts/versioning_matrix.sh`。在 `.env` 的服务器上
   **新建两个临时桶** `data-mover-c17-<run>`（开版本）与 `data-mover-c17-lock-<run>`（带 Object Lock），退出时（trap）
   彻底删除：去掉 legal hold、按 id 删每个版本与删除标记、abort 每个上传、删桶，并打印 `cleanup: <桶> deleted`。
   只用可撤销的 legal hold，**从不设 retention**（COMPLIANCE / GOVERNANCE 可能让桶删不掉）；不碰 `.env` 的桶。
   建桶不是 200（例如同名桶已存在，409）就停下，清理只碰本次建出来的桶；`RUN` 必须匹配 `[a-z0-9-]{1,30}`；
   凭据经 `curl -K <(…)` 传入，不上命令行。
   用例：4 MiB checkpointed（单 PUT）、200 MiB checkpointed（指针）、Direct 4 MiB / 20 MiB；`--source-version` 按 id
   拷 v1 再 v2（`--client-shaped` 流式，两版本按序、各与源逐字节相同），再原生按 id 拷一遍（C18：
   `CopyObject` / `UploadPartCopy` 带 `?versionId=`，写到 `hist/ndst`，期望同样按序、各与源相同）；按 id 拷贝 3 s 取消后续传；`resume_matrix.sh`
   cancel / SIGKILL；Object Lock 桶里拷贝进行中给指针版本加 legal hold；暂停版本后三种写法。每行打印 `result` /
   `prepare` / `reused` / `destination_version`，再打印该用例前缀下 `final versions=… delete markers=… artifact entries=…
   version=latest: yes`。期望：每次拷贝 1 个版本、0 标记、0 artifact，`destination_version` 就是最新版本；Object Lock
   行 `result=ok`、1 条告警、1 个标记 + 2 个 artifact 条目（被扣住的指针版本与盖在上面的标记）；暂停版本
   `destination_version=null`。`SKIP_RESUME=1` 跳过续传矩阵。

7. **原生 S3→S3 切断续传**（C18）：MinIO 上服务端拷贝很快（读回关时 1 GiB ≈ 1.1 s），`--bandwidth` 不限原生拷贝，
   所以用 1 GiB 源、读回关、`--cancel-after-ms 300`（或 `timeout -s KILL 0.8`）切断：期望切断后 1 指针 + 1 upload，
   同参数再跑一次 `Resumed { … }`（取消时在途的段都拷完，SIGKILL 丢在途的段），`--compare` 相等，之后 0 / 0。
   源先用本地 → `s3:<prefix>` 上传，再 `--source s3:<prefix> --destination s3:<prefix>`（同 endpoint 才走原生）。

## 成功判据

- s3_walkdir 列 bucket 退出码 = 0
- 不存在的 key 返回 FileNotFound，日志中无 retry/backoff

## 失败如何排查

- 401/403 → ak/sk 错或 IAM 权限不够
- 404 走了 retry → S3 错误映射回归，看 `src/s3.rs` 中 GetObject/HeadObject 错误处理
- multipart 失败留垃圾 → 必须 abort，看 s3.rs multipart upload 路径
- 续传的 write 报 `NoSuchUpload`（NotFound）→ 自己的 upload 被当成别人的 abort 了：MinIO 列出的 upload id 与签发的
  写法不同，比较必须走 `upload_pointer::same_upload`
- 续传是 `Restarted{StageWithoutPointer}` → 开始时没写指针：对象不超过 64 MiB（D3，不续传），或 prepare 不是
  `ResumeMode::Discover`（看 `pointer_before_checkpoint`）
- 版本化桶里留下 `.data-mover-*.upload` 版本或删除标记 → 指针没按版本删：看 `upload_pointer::delete` 拿到的版本
  （写入回的 `reported_version`、读回 HEAD 的版本、discovery 读到的版本）；`MemoryS3` 的
  `plain_deletes_of_final_keys()` 必须一直为空
- 自签证书拒绝 → 用 s3+https 不是 https；检查 hyper-rustls verifier 配置
