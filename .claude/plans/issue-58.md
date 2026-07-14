# issue-58 PR-A'：copy 路径写端字节计数断言 + mismatch 清理（零额外 RPC）

> 上游 issue：JayTsu-sh/terrasync-rs#58（spec v2，维护者性能意见修订版）
> 分支：`fix/issue-58-copy-size-guard`（基于 origin/main bb8b60b）

## 根因与 spec v2 核心

同源失明 bug 类 =「源读截断 → 写入字节数变少 → copy 静默成功」。v2 拍板：
断言数据源用**写端本地计数**（零存储 RPC），不用 get_metadata 读回
（维护者性能反对成立）。分层防线：

| 防线 | 开销 | 门控 |
|---|---|---|
| 写端本地计数断言（`bytes_written == entry.size`） | 零 | 无条件 |
| 目标读回 size（hash 读回顺带取，非独立 GETATTR） | 已含在 hash 读回内 | `enable_integrity_check=true` |
| mismatch 清理（`delete_file` 残留坏文件） | 仅失败路径 | 无条件 |

**性能红线**：全部改动不新增任何存储 RPC（get_metadata/stat 等）。

## 方案

1. **写端计数基础**：`write_pipeline_core` 返回累计写入字节数
   `Result<u64>`（在 settle 的成功分支与 bytes_counter 同点累计）；
   local/nfs/cifs/s3 四后端 `write_data` 改返回 `Result<u64>`（S3 内部本就有
   `written`）。`write_data_resumable` 三处与 `pack_files_to_tar` 调用点丢弃
   计数值适配，行为不变。均为 pub(crate)，不影响 terrasync 公开 API。
2. **单块路径**（storage_enum.rs `copy_file_with_cancel`）：读到内存后、写前
   断言 `data.len() == size`（不等 → Err，尚未写入无需清理）；integrity 读回
   改用 `compute_hash_and_len`（drain 循环顺带数字节，零额外 RPC），
   读回字节数 / hash 任一 mismatch → best-effort `delete_file` 清理 + Err。
3. **多块 pipeline**：write task 汇合后无条件断言 `bytes_written == size`，
   不等 → 清理 + Err；integrity 分支与单块共用 `verify_dest_integrity`
   helper（读回 size 核对 + hash 比对 + mismatch 清理）。
4. **S3 multipart 续传**（`copy_file_resumable_to_s3`）：wrap `on_committed`
   累计本会话确认上传字节数，`CompleteMultipartUpload` **之前**断言
   `== 缺失区间字节总和`，不等则不提交（坏对象根本不落地）。**不 abort**
   upload——沿用既有「失败不 abort，parts 即续传进度」设计，重试自愈；
   （既有 `finalize_resumable_upload` 的 ListParts 全覆盖校验保留为第二道
   server 端防线，非本次新增 RPC）。Complete 之后的 hash mismatch 分支补
   best-effort `delete_file` 清理。
5. **NAS 续传路径**（`copy_file_resumable` NAS 分支）：不动，既有
   tests/test_copy_file_resume.rs 全绿即回归证明。
6. 错误形态：沿用既有 `StorageError::OperationError` + 描述性消息（与现存
   integrity 失败同形态，terrasync 侧零改动）；不新增 enum variant（避免
   破坏下游 match 完备性）。

## 步骤

- [x] 1. 建立本执行计划 → 验证：文件落盘、单独 commit
- [ ] 2. 写端计数基础：`write_pipeline_core`/四后端 `write_data` 返回
      `Result<u64>`，调用点适配 → 验证：`cargo check` 通过，行为零变化
- [ ] 3. `copy_file_with_cancel` 单块/多块：写前断言 + 计数断言 +
      `compute_hash_and_len` + `verify_dest_integrity`/`cleanup_mismatched_dest`
      helper → 验证：`cargo check` + 存量测试全绿
- [ ] 4. `copy_file_resumable_to_s3`：会话字节断言前移 + hash mismatch 清理
      → 验证：`cargo check`（S3 分支无本地环境，逻辑走查 + 断言纯本地）
- [ ] 5. 测试：tests/test_copy_size_guard.rs 截断注入（单块/多块 ×
      integrity on/off → Err 且目标无残留）+ src/storage_enum.rs tests mod
      （`verify_dest_integrity`：hash mismatch、读回 size mismatch → 清理生效）
      → 验证：新用例全绿
- [ ] 6. 回归：`cargo test` 全套 + `cargo clippy --all-targets` 新代码零告警
      → 验证：全绿后在本计划记录结果

## 约束

- 不跑 `cargo fmt`（存量文件未格式化，避免无关 diff）
- 不 push、不动 origin，交付本地就绪分支
- 不新增任何存储 RPC；断言全部基于本地已知量
