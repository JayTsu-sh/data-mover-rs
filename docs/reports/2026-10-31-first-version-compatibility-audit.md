# 首个版本历史兼容性检查

日期：2026-10-31。检查基于本地提交 `35c1671654cd1fed3b582f10e49886b7839897b1`；该提交的文件树与已合入主分支的 `724d3148aff63ae42752f3385c60e97da0480f86` 相同。

## 结论与本次变更

项目确实存在历史开发版本的兼容代码，但不能通过删除所有版本号来清理。`ObservedEntry` 快照虽然编号为 v10，实际上只接受当前版本；扫描 Checkpoint、传输 hash checkpoint、RangeHash 和 HDFS stage token 也没有多版本解码链。

本次把“首个版本只维护当前接口和格式”的规则写入根目录 [AGENTS.md](../../AGENTS.md)，并在 [CLAUDE.md](../../CLAUDE.md) 中引用，取消其开头对“兼容 facade”的描述。下列运行时代码和现行文档问题是本次审查发现的待清理项；本次没有修改其行为、清除开发状态或访问、删除任何 backend 数据。

## 已确认的历史兼容实现

### 1. 目的端指针同时读写两种格式

位置：[pointer.rs](../../crates/storage-core/src/storage/pointer.rs)，第 19–21、29–30、78–84、116、146–154 行。

- 同时识别 `DMDPTR01` 和 `DMDPTR02`。
- `checkpoint: None` 继续写 v1；有 hash checkpoint 时写 v2。注释明确要求保持旧 v1 编码不变。
- v1 解码直接返回无 checkpoint，v2 再读取 checkpoint 内容。
- 测试 `checkpoint_v2_is_bounded_and_does_not_change_v1` 固定了两种格式并存的要求。

建议统一为一个当前格式，明确表达可选 hash evidence，并同步修改各 backend 的恢复、发现、清理和测试夹具。不能直接删掉无 hash 的正常传输或专家路径。旧的、确认归属项目的开发恢复状态可清除并从源重新传输。

关联：[discovery.rs](../../crates/storage-core/src/storage/discovery.rs) 第 109、148、188–193 行的 `legacy_restart`；调用点包括 [at_destination.rs](../../crates/transfer/src/transfer/engine/at_destination.rs) 第 122 行，以及 S3 [at_destination.rs](../../crates/backend-s3/src/backend/staged/at_destination.rs) 第 366 行。该条件同时控制当前非 resumable 任务的恢复资格，应随格式统一重新梳理，保留当前任务语义。

`is_checkpoint_pointer` 对未知 checkpoint 的保护不能随旧格式分支一起直接删掉：其作用是阻止未知状态进入破坏性的旧清理流程。调整后仍需对无法识别、归属不明的状态保持安全拒绝。

### 2. S3 checkpoint 接受旧 ETag 收据摘要

位置：[checkpoint_record.rs](../../crates/backend-s3/src/backend/staged/checkpoint_record.rs)，第 88–120、158–170 行；调用点为 [hash_checkpoint.rs](../../crates/backend-s3/src/backend/staged/hash_checkpoint.rs) 第 87 行。

当前 `receipts()` 只写规范化的 `Quoted` 摘要，但 `receipts_match()` 依次尝试 `Quoted`、`Original`、`Bare` 三种摘要。注释明确说明额外两种用于读取旧 v1 摘要。[etag_receipt_tests.rs](../../crates/backend-s3/src/backend/staged/etag_receipt_tests.rs) 中的 `an_old_bare_checkpoint_remains_readable_when_list_parts_is_quoted` 专门验证这种历史兼容。

建议只保留当前收据摘要规则，删除旧摘要匹配和对应兼容测试，旧开发 checkpoint 重新生成。

必须保留不同 S3 服务返回带引号或不带引号 ETag 的实时归一化及合法性验证。DXN 的 UploadPart 与 ListParts 返回形式不同，这是当前服务端行为适配，不是旧项目数据兼容。

### 3. 元数据 binding 为无 ACL 请求保留 v1 布局

位置：[negotiation.rs](../../crates/transfer/src/transfer/engine/negotiation.rs)，第 72–100、430–457 行。

有 ACL 时使用 `ordinary copied metadata policy v2`；无 ACL 时仍使用 v1 域标签，并保留两字节旧 ACL selector 占位。测试 `automatic_acl_selection_is_distinct_and_invalidates_old_acl_checkpoints` 还要求无 ACL 场景继续匹配 `legacy_binding`。

建议统一当前 binding 输入布局，删除仅为旧布局保留的域分支和占位字段，更新测试。保留 ACL 策略、来源及任务计划的区分和绑定；新旧 binding 不一致时按当前恢复规则重新开始。

### 4. Windows ACL 保留旧方法签名

位置：[inspection.rs](../../crates/backend-local/src/backend/windows_acl/inspection.rs)，第 137–154 行。

`PermissionMapping::get_permission(mask, _ace_flags)` 的注释明确写着“兼容旧方法签名”“主要用于向后兼容”。该方法忽略 flags，并把返回项固定构造为 `AceType::Allow`。在 `src`、`crates`、`tests`、`examples` 的 Rust 源码中检索 `get_permission(`，仅找到此定义。

建议移除旧包装，使用当前权限解析入口和结构化 ACE 表达；更新相关文档与测试，无需为首版前的外部调用方保留旧签名。

### 5. Cargo 测试脚本翻译旧模块路径

位置：[run_cargo_test.py](../../tests/run_cargo_test.py)，第 14、23–40 行。

脚本把旧 `storage::backends::<backend>::...` 和 `model::acl::...` 等过滤器翻译为拆 crate 后的 package 和模块路径。当前 CI 仍在使用旧入口，见 [ci.yml](../../.github/workflows/ci.yml) 第 105、109–110 行和 [release-validation.yml](../../.github/workflows/release-validation.yml) 第 52 行。对应兼容测试为 [test_cargo_test_selection.py](../../tests/test_cargo_test_selection.py) 中的 `test_legacy_filters_select_the_owning_workspace_package`。

建议先将所有调用方改成当前 package 与过滤器，再删除旧路径改写。例如显式传 `--package data-mover-backend-s3 backend::client::storagegrid::tests`。不能只删转换器而不改 CI；应保留脚本的零用例、全部 ignored 拒绝机制。

## 接口精简候选，不直接判为兼容分支

[observation.rs](../../crates/model/src/observation.rs) 第 645–656 行同时提供 `encode_snapshot() -> EntrySnapshot` 和 `try_encode_snapshot() -> Result<...>`。前者是后者的 panic 包装，后者检查历史记录分段上限。可以统一为一个返回 `Result` 的当前接口，并更新调用方；不过仅凭两个入口共存，尚不能证明其唯一用途是兼容旧 API，也可能是有意提供便利方法。

根包 `data-mover` 的 facade 和 re-export 是当前统一消费入口，有实际架构用途。应取消仅为旧导入路径保留的重复出口和兼容承诺，而非因为注释含 `Compatibility exports` 就删除整个 facade。

## Checkpoint、快照版本号的实际行为

| 对象 | 当前格式 | 解码行为与判断 |
| --- | --- | --- |
| `ObservedEntry` 快照 | `DMES`，v10 | [observation_codec.rs](../../crates/model/src/observation_codec.rs):188–189 只接受 `VERSION`，其他版本报 `UnsupportedVersion`；无旧版解码器 |
| 扫描 Checkpoint | `DMTC`，v1 | [checkpoint.rs](../../crates/traversal/src/traversal/checkpoint.rs):295–298 只接受当前版本；无迁移链 |
| 传输 hash Checkpoint | `DMHCP002` | [checkpoint.rs](../../crates/storage-core/src/storage/checkpoint.rs):80 只接受当前 magic；没有 `DMHCP001` 分支 |
| RangeHash | `DMRHASH1`，v1 | [codec.rs](../../crates/storage-core/src/storage/range_hash/codec.rs):132–133 同时核对当前 magic 与版本 |
| HDFS stage token | `hdfs-stage-v2` | [staged.rs](../../crates/backend-hdfs/src/backend/staged.rs):50 只接受当前前缀；无 v1 回退 |

版本编号大于 1 只能说明格式曾经变化，不能证明仍在兼容旧版。保留版本号与 golden fixture 有助于发现布局改变，拒绝误读；无需为“第一版”把所有编号重置成 1。格式今后变化时更新当前编码、测试夹具，并让旧开发状态重新生成即可。

校验和、长度限制、backend/源身份、计划和元数据 binding、路径所有权、安全清理、崩溃恢复均属于当前功能，不应作为兼容负担删除。

## 过时文档和承诺

- [crate-workspace.md](../architecture/crate-workspace.md) 第 24 行仍将根包职责写为 `legacy import paths`，第 100 行说明旧测试过滤器，第 103 行起使用“Recovery and compatibility”，第 150 行要求 compatibility fixtures。应依据首版规则改为当前入口、格式和完整性契约，相关代码清理时同步修订。
- [ADR 0006](../adr/0006-destination-resident-recovery.md) 第 208 行将磁盘格式描述为“formats a newer binary must still read”。它记录了旧设计；该兼容承诺已由根 AGENTS.md 的首版规则取代，后续更新应明确这一点，不能再据此增加旧版解码器。
- [.claude/docs/storage-s3.md](../../.claude/docs/storage-s3.md) 第 85、756 行和 [.claude/docs/storage-nfs.md](../../.claude/docs/storage-nfs.md) 第 242–243 行仍描述 `StorageEnum` 的现行行为。在 `src` 和 `crates` 中已找不到该 Rust 类型。应移除这些过时用法说明，避免继续指导开发者使用已不存在的 API。
- 明确归档的历史设计、实验报告及原始测试结果可作为证据保留，不代表继续支持其旧接口或格式；当前操作文档不能把归档内容当成现行能力。

## 清理顺序及边界

1. 移除无调用的旧签名；更新测试门禁调用方后删除旧过滤器改写；同步整理现行文档。
2. 统一元数据 binding 与 S3 收据摘要，删除历史匹配测试，更新当前格式夹具。
3. 协调 storage-core、transfer 和五个 backend 统一目的端指针，覆盖普通/专家传输、无 hash/有 hash、恢复、发布和安全清理场景。
4. 按当前接口设计决定是否统一快照编码入口；继续保留当前格式拒绝、完整性及恢复校验。

清除对象仅限确认归属本项目的开发 checkpoint、扫描状态、临时产物和夹具。用户源文件、已发布的目的文件、S3 业务版本历史及其他对象不属于可直接清除的“旧开发数据”。本次审查不需要清除任何真实数据。

## 后续执行：清理 v1–v9 快照

用户在本次审查后明确要求丢弃此前九个快照版本，已在工作区执行：

- 删除 `tests/data/snapshot/` 和 `crates/model/tests/data/snapshot/` 中全部 14 个 v8/v9 夹具文件；保留四个 v10 golden 夹具。
- 删除 `observation_tests.rs` 中内嵌的 v4 编码、其专用解码辅助函数与拒绝测试，以及 facade 的 v8 专用夹具拒绝测试。未发现其余 v1–v9 专用夹具或运行时旧版编解码器。
- 使用当前格式样本检查全部 255 个非当前版本号均被拒绝；无需存储旧格式样本即可覆盖 v1–v9 和未来未知版本。
- 更新 ADR-0008、ADR-0006 当前格式说明、架构文档及 P7 的现行快照规则，取消快照 N-1 兼容计划。历史实验报告的版本记录保留为事实记录。

当前快照编码仍为 v10，编码布局未改；没有重置版本编号、清除 Git 历史或访问真实 backend 数据。指针双格式、S3 旧摘要、旧 binding 和旧接口等其他审查项不在此次快照清理范围内。

清理后的验证：Rust 1.95.0 下，`cargo test --locked -p data-mover-model --lib -- --test-threads=1` 为 45 项通过；`cargo test --locked -p data-mover --no-default-features --test observed_entry_snapshot -- --test-threads=1` 为 7 项通过，合计 52 项、0 失败。当前格式测试对四个 v10 样本逐一检查全部 255 个非当前版本号，公开接口另验证一份当前观察的全部非当前版本号。`rustfmt --check`、`git diff --check`、现存夹具版本及链接检查均通过。

## 初次审查验证（快照清理前）

初次审查阶段仅修改指导文档和检查报告；运行时结论来自当前编码器、解码器、调用点和测试源码交叉检查。后续快照清理及其验证记录见上一节。

- `RUSTUP_TOOLCHAIN=1.95.0 CARGO_TARGET_DIR=/work/cargo-target-ci-1.95 cargo test --locked -p data-mover-model --lib snapshot_format_tests -- --test-threads=1`：4 项通过，0 失败，包括当前格式 golden 编码、拒绝其他版本、历史记录分段和损坏编码拒绝。
- 初次审查阶段三个 Markdown 文件的本地链接检查通过，无缺失目标；`git diff --check` 通过。
- 初次审查阶段未改动 Rust 或测试门禁代码，未重跑全量 CI 或真实 backend 性能验收；上述 4 项只验证快照格式，不代表已清除其余兼容实现。

## 代码审查与合入前验证

固定审查基线为 `724d3148aff63ae42752f3385c60e97da0480f86`。规范与需求由两个独立审查任务并行检查，另按仓库 reviewer 清单逐行核对。

### Standards

PASS：没有遗留的规范违规或需上报的代码异味；提交前 reviewer 结论为 SHIP IT，CRITICAL/HIGH/MEDIUM/LOW 均为 0。生产 Rust 仅更新 codec 文档；版本检查、字段解码、身份及完整性校验实现没有改动。修改后的两个测试函数分别为 17 行、47 行，未超过仓库上限。

### Spec

PASS：首版规则、兼容性检查与 v1–v9 快照清理符合用户要求。发现并修复 1 项 P3 报告准确性问题：将清理前“未改动 Rust”的验证明确标为初次审查阶段；修复后剩余 0 项。其他指针、S3 收据和 binding 兼容项仍作为单独待办，没有混入本次清理。

### 完整验证

- `RUSTUP_TOOLCHAIN=1.95.0 CARGO_TARGET_DIR=/work/cargo-target-ci-1.95 make ci` 完整退出码为 0；格式、架构守卫、Python harness contracts、feature matrix、工作区全目标 Clippy、工作区测试、示例构建和 Local e2e 全部通过。
- 其中 `cargo test --workspace --no-fail-fast` 合计 2194 项通过、0 失败、45 项忽略（含 doctest）。较清理前减少的两项为已删除的 v4/v8 专用拒绝测试；当前格式和全部 255 个非当前版本号的拒绝由现有测试覆盖。
- 夹具清单核对通过：恰好删除 14 个 v8/v9 文件，4 个 v10 文件与固定基线逐字节一致；模型及测试中没有旧夹具引用。
- 未执行真实网络 backend 性能复测；本次未改变 backend、QoS 或生产编解码行为，已有性能报告的结论保持其原有验收范围。
