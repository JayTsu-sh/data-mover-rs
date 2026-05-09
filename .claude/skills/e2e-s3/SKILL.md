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

## 成功判据

- s3_walkdir 列 bucket 退出码 = 0
- 不存在的 key 返回 FileNotFound，日志中无 retry/backoff

## 失败如何排查

- 401/403 → ak/sk 错或 IAM 权限不够
- 404 走了 retry → S3 错误映射回归，看 `src/s3.rs` 中 GetObject/HeadObject 错误处理
- multipart 失败留垃圾 → 必须 abort，看 s3.rs multipart upload 路径
- 自签证书拒绝 → 用 s3+https 不是 https；检查 hyper-rustls verifier 配置
