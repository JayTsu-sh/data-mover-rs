# 元数据协商（拷贝带什么、谁能否决）

> 场景：要打开或关闭 ACL / xattr 的拷贝、排查「请求了却没带上」、给新 backend 声明能力。
> 代码：`src/transfer/model.rs`（请求）· `src/transfer/engine.rs`（协商）·
> `src/metadata/mod.rs`（规划与应用）· `src/storage/roles.rs`（目的端能力）。

## 必须复制 vs 可配置

这条界线**由类型表达**，不需要额外的标记：

| | 谁决定 | 在哪 |
|---|---|---|
| **基线**：ownership、mode、timestamps(mtime) | backend 声明，调用方**关不掉** | `Metadata::copied_metadata_observation_plan()` |
| **可选**：ACL、xattrs | 调用方开口才带 | `CopiedMetadataRequest`（`TransferRequest::with_copied_metadata`） |

**`CopiedMetadataRequest` 里没有的字段，就是关不掉的。** 加一族到可选集合 = 给这个结构加字段。

atime / ctime 任何情况下都不拷。

## 四档策略

判定点：`src/metadata/mod.rs` 的 `drop_with_losses()` 与 `unavailable()`。

| 情形 | Omit | RequireExact | AllowKnownLoss | BestEffort |
|---|---|---|---|---|
| 能精确写入 | 跳过 | 写 | 写 | 写 |
| 写得进去但有语义损失（精度降级、owner 丢失） | 跳过 | **失败** | 记 `losses` 后继续 | 记 `losses` 后继续 |
| 一端不支持这一族 | 跳过 | **失败** | **失败** | 记 `Unsupported` 后继续 |
| 两端都支持但编码不同（Posix ↔ NfsV4 ↔ WindowsSecurityDescriptor） | 跳过 | **失败** | **失败**（整族丢失是缺席，不是降级） | 记 `Unsupported` 后继续 |
| 源端观测失败（GETACL 报错） | 跳过 | **失败** | **失败** | 记 `Failed` 后继续 |
| 目的端**应用期**拒绝（能力位为真但 SETACL 被拒） | — | **失败** | **失败** | 记 `Failed` 后继续，**传输仍然成功** |
| 应用期**取消**（token 已触发，或 backend 报 `Cancelled`，含服务端的 `STATUS_CANCELLED`） | — | **停止** | **停止** | **停止**，该族保持规划期结果、不记 `Failed` |
| 不适用（symlink 上的 ACL） | 跳过 | 跳过 | 跳过 | 跳过 |

**分野是「降级」与「缺席」**：`AllowKnownLoss` 容忍降级、不容忍缺席；`BestEffort` 两者都容忍。
名字容易让人以为 `AllowKnownLoss` 更宽松 —— 它不是，它要求这一族**必须被带上**。

**选哪一档**：
- 同构 NAS 迁移、希望"能带就带" → `BestEffort`
- 合规场景、ACL 必须逐位一致 → `RequireExact`，并接受在不支持的目的端上确定性失败
- 不确定 → 保持默认 `Omit`

## 否决链（按发生顺序）

1. **调用方策略** —— `Omit` 一票否决，连观测都不发（默认）。
2. **源端观测能力** —— 请求叠加到 backend 的基线 plan 上：
   `RequireExact`/`AllowKnownLoss` → `ObservationMode::Required`，`BestEffort` → `BestEffort`，
   `Omit` → 不动基线。backend 用 `MetadataObservation::{Unsupported, NotApplicable, Failed}` 否决。
3. **目的端写入能力** —— `CopiedMetadataTarget.acl` / `.xattrs`。
4. **规划期交叉** —— `compile_copied_metadata_plan` 按上表裁决。
5. **应用期** —— 服务端可以在能力位说 yes 之后仍然拒绝。只有 `BestEffort` 能吸收这一环。

源端 plan 为 `None`、目的端 target 为 `None`（今天的 S3 目的端）同样算否决：
**只有 `Omit` 与 `BestEffort` 能继续**，其余按"要了却没有路"失败，不静默少拷。

## 各 backend 的能力

| backend | ACL | xattrs | 依据 |
|---|---|---|---|
| NFS | 协商到才有，报 `NfsV4` | 协商到才有 | `mount.capabilities().acl` / `.named_attributes` |
| Local | unix 下 `Posix` | unix 下支持 | 非 unix 整体不参与拷贝 |
| CIFS | `WindowsSecurityDescriptor` | 不支持 | `cifs/metadata.rs` 的 decode 只认这一种 |
| HDFS | 不支持 | 不支持 | 观测侧也一律 `unavailable` |
| S3（目的端） | 不参与 | 不参与 | 未实现 `copied_metadata_target` |

## 两条必须记住的不变式

1. **顺序：mode 先于 ACL。** 写 permission bits 会重算 ACL（POSIX 的 mask 条目；NFSv4 上多数
   服务器包括 ONTAP 由 mode 重建整个 ACL）。`compile_metadata_plan` 的编译顺序就是应用顺序，
   所以 `compile_ownership` 必须排在 `compile_acl` 之前；替换 ownership 的那个 `Mode` mutation
   插在**队首**。由 `acl_is_applied_after_the_mode_that_would_rewrite_it` 与
   `the_mode_that_replaces_ownership_is_applied_before_the_acl_too` 看守。
2. **容忍不能靠重排实现。** `BestEffort` 的族失败后，批量路径重发**所有尚未应用的**、
   去掉被拒那一条：`pending[completed..failed_index] ++ pending[failed_index + 1..]`
   （`MetadataPlan::resume_after`）。不能从 `failed_index + 1` 续 —— backend 可以在动手前整批拒绝
   （Local 的预校验就是，`completed = 0` 而 `failed_index > 0`），那样前面的族会被静默跳过。
   也不是把容忍的族挪到队尾凑成一批 —— 后者会让 `BestEffort` 的 ownership 跑到
   `RequireExact` 的 ACL 后面，重新制造第 1 条要防的静默覆盖。
   `StagedMetadataApplicationFailure` 的三条字段约定（`src/storage/roles.rs`）就是这条的前提。
   由 `src/metadata/stage_tests.rs` 看守。

**取消永远不被容忍。** `BestEffort` 吸收的是「目的端说不」，取消不是。两条路径都按
「token 已触发 **或** 失败类别为 `Cancelled`」判定 —— 被打断的请求可能报成别的类别。

## 代价

开 ACL/xattr 后源端每个文件多一次 GETACL + LISTXATTR/GETXATTR（NFS 的 `get_acl` 还带
`lookup_fh` 与最多 `MAX_STALE_RETRIES` 次重试），目的端 xattrs 是逐属性串行 `set_xattr`。
海量小文件场景吞吐会明显下降，这也是默认 `Omit` 的原因之一。
