# 元数据协商（拷贝带什么、谁能否决）

> 场景：要打开或关闭 ACL / xattr 的拷贝、排查「请求了却没带上」、给新 backend 声明能力。
> 代码：`src/transfer/model.rs`（请求）· `src/transfer/engine/negotiation.rs`（协商）·
> `src/metadata/compile.rs`（规划）· `src/metadata/mod.rs`（应用）· `src/storage/roles.rs`（目的端能力）。

## 用户规则（2026-09-23，决定本页一切）

- **should**（要拷什么）由调用方给 —— terrasync-rs 的命令行 —— 对 data-mover 是用户输入，
  但**用户输入错误由上层处理**：data-mover 从不因「要了做不到」而失败。
- **can**（能不能拷）由两端自己的能力决定：源端能不能读（can scan）、目的端能不能存（can copy，
  依据源端的元数据自动适配，精度降低等损失只记在报告里）。**先看 can，再看 should。**
- **should scan ≡ should copy**：扫描不要的是浪费，要了没扫到的也拷不了 —— 同一个输入。
- 两端都能 → 真的去读、去写；**读或写失败就是失败**：该文件失败，但先把其余族都应用完，再一次性
  列出所有原因。任一端不能 → 不读、不写、报告里记原因，不算失败。
- mtime 必拷（S3 目的端除外，那是目的端的「不能」）。xattr 是一个功能，不按名字挑。

## 必须复制 vs 可配置

这条界线**由类型表达**，不需要额外的标记：

| | 谁决定 | 在哪 |
|---|---|---|
| **基线**：ownership、mode、timestamps(mtime) | backend 声明，调用方**关不掉** | `Metadata::copied_metadata_observation_plan()` |
| **可选功能**：ACL、xattrs | 调用方开口才带 | `CopiedMetadataRequest::with_acl()` / `with_xattrs()` |

**`CopiedMetadataRequest` 里没有的，就是关不掉的。** 加一个可选功能 = 给这个结构加字段。
atime / ctime 任何情况下都不拷。

## 要了一个可选功能之后

判定点：`src/transfer/engine/negotiation.rs` 的 `optional_policy()`（要 → 规划期 `BestEffort`，
不要 → `Omit`），再由 `src/metadata/compile.rs` 的规划器按两端能力裁决。

| 情形 | 不要 | 要 |
|---|---|---|
| 两端都能，精确 | 不读不写 | 拷 |
| 两端都能，有损（精度降低、只保留 mode 等） | 不读不写 | 拷，损失记进报告 |
| 源端读不了（NFSv3 的 ACL、未协商 named attributes 的 xattr） | 不读不写 | 跳过，记 `Unsupported`（`SourceCannotObserve`） |
| 目的端存不了 | 不读不写 | 跳过，记 `Unsupported`（`DestinationCannotStore`） |
| 两端都存但编码不同（Posix ↔ NfsV4 ↔ WindowsSecurityDescriptor） | 不读不写 | 跳过，记 `RequiresExternalMapping`；**不做任何转换** |
| 源端**读失败**（GETACL 报错） | — | **文件失败**（`SourceObservationFailed`，带 class） |
| 目的端**写失败**（能力位为真但 SETACL 被拒） | — | 其余族照常应用，**文件最终失败**并列出所有失败的族 |
| 应用期**取消** / **会话级失败** / **整批失败** | — | **立即停止**并失败 |
| 不适用（symlink 上的 ACL） | 跳过 | 跳过 |

`MetadataPolicy` 的四档（`RequireExact` / `AllowKnownLoss` / `BestEffort` / `Omit`）仍在
`metadata` 模块里，给基线族与直接调用 `compile_metadata_plan` 的代码用；**不再出现在拷贝请求里**。

## 否决链（按发生顺序）

1. **should** —— 不要的功能连观测都不发（默认）。
2. **源端能不能读** —— 要的功能以 `ObservationMode::BestEffort` 叠加到 backend 的基线 plan 上；
   backend 用 `MetadataObservation::{Unsupported, NotApplicable}` 说「读不了」，`Failed` 说「读失败」。
3. **目的端能不能存** —— `CopiedMetadataTarget.acl` / `.xattrs`。
4. **规划期交叉** —— `compile_copied_metadata_plan` 按上表裁决。
5. **应用期** —— 服务端可以在能力位说 yes 之后仍然拒绝：该文件失败（先应用完其余族）。

源端没有元数据角色、目的端没有 target（今天的 S3 目的端）时整段跳过，**包括 mtime** ——
「mtime 必拷、S3 目的端除外」尚未落实，是下一步。

## 各 backend 的能力

| backend | owner/group（源端） | ACL | xattrs | 依据 |
|---|---|---|---|---|
| NFS | v3 数字；v4 字符串，nfs-rs 映射不了的名字走「只有 mode」（`OwnerAndGroupUnmapped`，见 `storage-nfs.md`） | 协商到才有，报 `NfsV4` | nfs-rs 0.8.4 永远报未协商 | `mount.capabilities().acl` / `.named_attributes` |
| Local | unix 下数字 | unix 下 `Posix` | unix 下支持 | 非 unix 整体不参与拷贝 |
| CIFS | 读不到（`NotApplicable`） | `WindowsSecurityDescriptor`（只保留 account 类 ACE） | 不支持 | `cifs/metadata.rs` 的 decode 只认这一种 |
| HDFS | 字符串，走「只有 mode」（`OwnerAndGroupDropped`） | 不支持 | 不支持 | 观测侧也一律 `unavailable` |
| S3（目的端） | 不参与 | 不参与 | 不参与 | 未实现 `copied_metadata_target` |

## 两条必须记住的不变式

1. **顺序：mode 先于 ACL。** 写 permission bits 会重算 ACL（POSIX 的 mask 条目；NFSv4 上多数
   服务器包括 ONTAP 由 mode 重建整个 ACL）。`compile_metadata_plan` 的编译顺序就是应用顺序，
   所以 `compile_ownership` 必须排在 `compile_acl` 之前；替换 ownership 的那个 `Mode` mutation
   插在**队首**。由 `acl_is_applied_after_the_mode_that_would_rewrite_it` 与
   `the_mode_that_replaces_ownership_is_applied_before_the_acl_too` 看守。
2. **续发不能靠重排实现。** 某一族被拒后（记下，继续应用其余族），批量路径重发**所有尚未应用的**、
   去掉被拒那一条：`pending[completed..failed_index] ++ pending[failed_index + 1..]`
   （`MetadataPlan::resume_after`）。不能从 `failed_index + 1` 续 —— backend 可以在动手前整批拒绝
   （Local 的预校验就是，`completed = 0` 而 `failed_index > 0`），那样前面的族会被静默跳过。
   也不是把被拒的族挪到队尾凑成一批 —— 那会让 ownership 跑到 ACL 后面，重新制造第 1 条要防的
   静默覆盖。
   `StagedMetadataApplicationFailure` 的三条字段约定（`src/storage/roles.rs`）就是这条的前提。
   由 `src/metadata/stage_tests.rs` 看守。

**应用期失败的规则**（用户，2026-09-23）：**任何一族写失败都意味着该文件拷贝失败**，但一个文件的
所有族都先应用完，再把失败一次性报出（`MetadataApplicationFailure::failures()`，Display 逐条列出）。
只有三种情况立即停止，因为之后的写要么不可能成功、要么不可信：
- **取消** —— token 已触发，或 backend 报 `Cancelled`（含服务端的 `STATUS_CANCELLED`）；被打断的请求
  可能报成别的类别，所以两者任一即算。该族保持规划期结果，不记 `Failed`。
- **会话级失败**（`StorageRoleFailure::Session`）—— 会话断了，后面每一次写都会跟着失败。
- **整批失败** —— backend 用 `StagedMetadataApplicationFailure::whole_batch(len, completed, error)`
  报告，即 `failed_index == len`：不归属任何一条 mutation。Local 的 stage 打不开、`spawn_blocking`
  丢失、以及 durable **屏障 `sync_all` 失败**走这里。记在**最后一条已应用但未落盘**的族上
  （`completed - 1`；一条都没应用时记第一条），该批次一个都不标 `Applied`。

其余（Entry 级、非取消，`is_refusal`）都是「目的端拒绝这一次写入」：记下，续发剩下的族。

**Local 在拒绝处不跑屏障**（`apply_local_batch`）：任何拒绝都让文件失败、不会发布，续发的批次以屏障
结束、覆盖整个文件；若在拒绝处跑屏障且它失败，整批失败会**盖掉拒绝本身** —— 调用方要处置的原因。
（`7dec6c7` 曾为「被容忍的拒绝落在最后一条」而在拒绝前跑屏障；应用期不再有容忍，这个理由不在了。）

看守：`src/metadata/stage_tests.rs`（续发、二次拒绝、整批失败三形态、会话断、取消）、
`a_failed_metadata_barrier_is_not_absorbed_by_a_best_effort_family` 与
`a_refused_family_fails_the_item_after_the_rest_is_applied`（expert API + 真 Local stage）、
`a_refused_setacl_stays_entry_scoped_and_only_a_lost_connection_does_not`（NFS 的拒绝必须是 Entry 级，
否则会被当成会话失败而提前停止）。

**已知缺口：CIFS。** 它没有批量实现，逐条 `apply_metadata` 在 durable 时 apply 后再 open/flush/close，
flush 失败经 `classify` 成 Entry 级，被当成那一条的拒绝 —— 文件照样失败，但报成「拒绝」而不是
「整批失败」，且不会提前停止。要改得让逐条接口也能表达「屏障失败」。

## 报错

元数据失败的文本由三部分拼成：`TransferFailure` 的标题（阶段、哪一端）+ 具体原因 + 适配器诊断。
- 规划期拒绝：`MetadataPlanError` = 族 + 策略 + `RefusalCause`（源端读不了 / 读失败（带 class）/ 没读 /
  目的端存不了 / 两端编码不同（两个都点名）/ 缺映射器 / 映射失败 / 策略不允许的损失（点名是哪种））。
  `cause().side()` 决定标题里的 Source / Destination。
- 应用期失败：`MetadataApplicationFailure` = 每个失败的族（`failures()`）各自的
  `ApplicationFailureKind`（被拒 / 整批失败 / 会话断 / 取消）+ 存储失败及其诊断，Display 用「; 」逐条列出；
  `family()` / `kind()` / `storage_error()` 指结束应用的那一条（最后一条）。
- Metadata 阶段的 `TransferFailure` 不实现 `source()`：Display 已经完整，链式打印会重复。
- 读源端元数据失败：标题 "observing source metadata failed" + 存储失败及其诊断。

诊断是适配器按契约已脱敏的文本（R8）。服务端拒绝 NFS 的 ACL 读写与 mode / owner / 时间的 SETATTR 时，
诊断带服务端状态名，例如 chown 被拒是 `NFS role failed: NFS4ERR_PERM (permission denied)`；SETATTR 之前的
LOOKUP 失败与 xattr 路径还没有（见 storage-nfs.md「Retry Taxonomy」的缺口）。

## 真机证据（2026-09-23，`examples/nfs_metadata_copy.rs`）

入口：`.claude/skills/e2e-nfs/scripts/metadata_matrix.sh`，9 档全过。环境：m1-source（NFSv3，只读源）、
FAS2750 `ontap_lisaauto_nfs`（ONTAP 9.19.1，NFSv4.1）与同机 CIFS share。

| 路径 | 要什么 | 结果 |
|---|---|---|
| NFSv3 → NFSv4.1 | ACL + xattr | 拷贝成功，两族记 `Unsupported`（源端读不了） |
| NFSv4.1 → NFSv4.1 | ACL | `Applied`，raw GETACL 回读与源**逐条相等且带标记** |
| NFSv4.1 → NFSv4.1 | 不要 ACL（阴性对照） | 目的端没有标记，校验按预期失败 |
| NFSv4.1 → NFSv4.1 | xattr | 拷贝成功，记 `Unsupported`（该导出没协商到 named attributes） |
| NFSv4.1 → CIFS | ACL | 拷贝成功，记 `Unsupported`（NfsV4 与 Windows SD 编码不同，不做转换） |

（2026-09-23 早先按四档策略跑过一版：`RequireExact` / `AllowKnownLoss` 在上面「做不到」的格子里
都是规划期拒绝。按用户规则改成「要了做不到就跳过」后矩阵 9/9。）

**这台 ONTAP 接受 SETACL**，`RequireExact` 的 ACL 真的落地 —— 不是只看报告：源端先加一条
mode 表达不了的 ACE（`--mark-acl`），再从目的端读回比对。**不加标记的比对没有意义**：
mode 是基线族，两端 ACL 都由同一个 mode 重建，`Omit` 也会「相等」。

**不变式 1（mode 先于 ACL）在这台机器上无法证伪。** 把编译顺序临时对调成 ACL 在前，两种标记
（新增主体 `12345`；给 `EVERYONE@` 加 `WRITE_ACL`）都照样到达。前者符合 ONTAP 的
`v4-acl-preserve`（chmod 保留非 `OWNER@/GROUP@/EVERYONE@` 条目）；后者说明随后的 chmod 没有
重建 `EVERYONE@` —— 最可能是目的端 mode 与 ONTAP 从同一份 ACL 推出的 mode 相同，chmod 是
无变化写入。所以这条不变式的依据仍是 POSIX mask 与不保留 ACL 的服务器，真机上只能证明
「按现顺序不丢」，证明不了「反顺序会丢」。

## 代价

开 ACL/xattr 后源端每个文件多一次 GETACL + LISTXATTR/GETXATTR（NFS 的 `get_acl` 还带
`lookup_fh` 与最多 `MAX_STALE_RETRIES` 次重试），目的端 xattrs 是逐属性串行 `set_xattr`。
海量小文件场景吞吐会明显下降，这也是默认 `Omit` 的原因之一。
