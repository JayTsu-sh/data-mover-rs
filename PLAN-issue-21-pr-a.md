# PLAN — issue #21 PR#A（data-mover-rs 侧）

范围：仅 data-mover-rs PR#A（§6 Checklist「data-mover-rs PR#A」半边）。
terrasync-rs PR#B 不在本计划内。

## 步骤

- ✅ step 0: 建执行计划文件，commit（本文件）
- ✅ step 1: fix(local): create_file 加 truncate: bool（write_file/write_data 传
  true，write_data_resumable 传 false）+ T0（10MB→3MB 覆盖回归）
- ⬜ step 2: refactor: 新建 src/write_pipeline.rs（ChunkSink + CommitPolicy +
  write_pipeline_core），NFS/CIFS/Local 三个 write_data/write_data_resumable
  收薄为 sink + wrapper。T1（现有 test_copy_file_resume.rs/test_copy_file_cancel.rs
  全绿）
- ⬜ step 3: feat(storage): storage_enum.rs 新增 StreamHandle(+Serde) /
  resume_prepare / write_chunk_stream / commit_chunk_stream / read_chunk_stream，
  复用现有 write_data_resumable/prepare_resumable_upload/finalize_resumable_upload/
  set_file_len/rename
- ⬜ step 4: refactor: copy_file_resumable(+_to_s3) 重写在三段之上；
  copy_file/pack_files_to_tar 不动。T1 复跑确认仍绿
- ⬜ step 5: 加 T2–T5 单测（write_chunk_stream 顺序写原子提交 / 乱序缺失重复 chunk /
  中途 drop rx 重跑续传 / hash mismatch 不提交）
- ⬜ step 6: 收尾：cargo test --workspace + cargo clippy 全绿，git status 无越界文件，
  删除本计划文件并单独 commit

## 关键设计决策（与 issue #21 两条评论 08:51Z/09:48Z 一致，细节由实现时定）

- resume_prepare 对 NAS：resume=false 或 .part 不存在 → missing=[(0,size)]；
  .part 存在且 len<size → missing=[(len,size)]；len>=size → 视为 stale/已全量完成
  （len==size → missing=[]；len>size → 全量重写 missing=[(0,size)]）。
- resume_prepare 对 S3：直接复用现有 prepare_resumable_upload（ListParts 反推），
  resume 参数对 S3 无意义（S3 自身状态即真值）。
- 融合式 copy_file_resumable（向后兼容旧签名）：NAS 分支沿用调用方 resume.missing_intervals
  原样使用（不重新用 resume_prepare 推断，避免与调用方状态不一致——这是既有行为，不改）；
  S3 分支沿用 resume_prepare 的 ListParts 推断（原逻辑本就是这样，_to_s3 从不使用调用方
  missing_intervals）。
- hash 比对 vs commit 顺序：NAS 侧 hash 检查早于 rename（现状 + T5 要求）；S3 侧维持现状
  finalize_resumable_upload（Complete）早于 hash 检查——S3 的 in-progress multipart
  parts 在 Complete 前不能作为连续对象读取，物理上做不到"先 hash 后 commit"，这是对象存储
  的固有限制，不是引入的新语义。
