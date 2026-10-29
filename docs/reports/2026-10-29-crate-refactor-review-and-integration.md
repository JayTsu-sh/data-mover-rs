# Crate 重构：主分支整合、review 与验收

日期：2026-10-29（Asia/Shanghai）。PR：[GitHub #171](https://github.com/JayTsu-sh/data-mover-rs/pull/171)。

代码基准为 GitHub main `056d71860a9b01a1203f8425dc5bfa37f837afef`；被审查、构建和真实环境执行的最终生产代码为 `313ebca33f13d73f08a2b159a01d4945ec52e733`。
后续验收报告提交仅更新文档。此前未提交工作树的验证见[历史报告](2026-10-29-crate-refactor-live-validation.md)，不用于替代本次最终代码验收。

## Standards

独立只读审查通过，**未解决问题 0 项**。依照仓库约定复查依赖方向、生产/测试边界、API 文档、恢复状态、凭据处理以及变更范围；工具可强制检查的项目交由 CI 验证。

复查中修正了两个全功能集成测试的 `required-features`、filter 测试的 owner 映射，避免测试迁移后出现空选；Clippy 和普通测试门禁覆盖整个 workspace，`make ci` 增加 feature isolation 检查。

## Spec

独立只读审查通过，**未解决问题 0 项**。功能引擎、协议适配器与 ACL 的职责划分、默认 facade、feature 隔离、恢复能力和扩展边界符合 [crate 方案](../architecture/crate-workspace.md)。

审查及完整测试发现的问题均已修正：

- 删除新公开的 unchecked path 构造入口；跨 crate 元数据组装保留大小、重复 family 以及 v10 retained failures 校验。
- 整合主分支的 ACL 自动投影、遗漏报告、恢复绑定、源版本权限查询和目录 marker 逻辑；保留发布版 NFS 0.9.1。外部依赖的名称、版本与来源集合和基准 main 完全一致。
- History attachment 检查 endpoint 归属、marker 互斥及实际最大版本大小；record 构造限制 ETag 大小。完整版本历史仍可作为扫描中间状态，输出时分段；`try_encode_snapshot` 对未分段的超长历史返回错误，旧序列化入口明确检查此前置条件。回归覆盖 100,001 条历史记录的分段 roundtrip。
- 更新移动后的 doctest crate 路径及快照版本断言，保留主分支新测试对 named principal/version override 的约束。
- HDFS lab 的 HA nameservice URL 构造和失败退出码分别在独立修复提交中处理。

## 本地自动化验收

全部通过：

- `cargo test --workspace --all-features --locked --no-fail-fast`：**2183 passed、0 failed、45 ignored**（包含 doctest；ignored 项不冒充真机验收）。
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`。
- `cargo build --examples --locked`；`cargo fmt --all -- --check`；`git diff --check`。
- 13 组 feature 配置的隔离构建与依赖图检查，以及 minimal/all-targets、禁用 connector、snapshot、外部 adapter 合约。
- Python unittest **113 项**；`make verify` 完整通过，覆盖 architecture guard 及其自测、harness、feature matrix、workspace Clippy/tests、examples、e2e-local、大文件审计。

## 最终代码的真实环境验收

证据目录：`/work/tmp/data-mover-review-real-20261029-005453/`。`artifacts.json` 记录本地构建产物的 SHA-256；传输至 HDFS/Kerberos 节点后核对 10 个产物再执行，远端不借用旧 checkout 的可执行文件。

| 范围 | 结果 |
| --- | --- |
| 跨 backend 拷贝 | Local / Linux NFSv3 / Linux NFSv4.1 / RustFS S3 / Kerberos HDFS 的 25 个有向组合，小文件与 `48 MiB + 137 B` 各一轮，独立大小和摘要验证通过；包含 multipart rename 的精确非空单测 |
| 跨 backend 断点续传 | 同样 25 个有向组合，`96 MiB + 123 B`；新进程恢复，逐项检查正的部分复用、最终大小和摘要 |
| 完整性 | 正向矩阵、path/observed 输入和目标缺失、类型/mtime/内容/大小变化反例通过 |
| HDFS | Kerberos HA nameservice 的 15 项真实合约及 4 项架构合约通过，包含运行根清理 |
| S3 | MinIO、RustFS、ONTAP、DXN 各 40 次成功传输：36 个尺寸/写策略/read-back 组合、2 次 native、合作取消与 SIGKILL 后各一次恢复；取消是预期失败且留有可恢复状态。每组 12 次 Direct、2 次 Native 的独立内容比较通过；恢复复用 184,549,376–201,326,592 B；临时对象/上传归零 |
| StorageGRID | 遍历通过；0 B / 1 KiB / `20 MiB + 1 B` × 三种写策略，9 次写入及 9 次独立内容比较通过；通过独立客户端清理并确认 0 对象/上传 |
| ONTAP NFSv3/v4.1 和 Local | 200 MiB 合作取消、SIGKILL 后均正向恢复，身份一致、复用字节为正、最终 BLAKE3 相同，空 HOME，无远端恢复文件残留 |
| ACL | Linux POSIX ACL ↔ ONTAP NFSv4 ACL 四向拷贝；命名主体、mask、优先级、组权限组合，path/observed 比较及修改目标 ACL 后的差异检测通过；同模型应用为 Applied，跨模型按主分支策略明确报告 AppliedPartialAcl / IdentityResolutionUnverified |
| SMB | ONTAP 6 项真实合约通过；protected checkpoint DACL 的不支持行为复现，安全拒绝且清理成功，不计为正向续传 |
| 断点续扫 | 14 个用例，每个基准 972 条目：Local/NFS/SMB/S3 的 unordered/blocks 合作取消 8 项；Local/NFS/S3 的 unordered/blocks SIGKILL 6 项；每项 2–8 轮完成，最终 `lost=0`、`invented=0` |

`asserted-matrices.json`、`scan-assertions.json`、`acl-four-results.json` 及 `remote-evidence/` 保存逐项结果。对观察式脚本另外解析结果和断言，未仅凭脚本退出码宣称成功。

测试准备阶段的错误保留在日志中，未计为功能失败或成功：扫描夹具最初缺少 Local/NFS 根目录，创建本轮隔离目录后重跑通过；ACL 旧验收脚本仍期望所有跨模型应用为 Applied，更新为主分支已经规定的 partial 报告后通过。StorageGRID 测试工具没有实现 S3 artifact 清理，使用独立 S3 客户端清理本轮前缀并确认无残留。

## 兼容性与验证边界

版本统一为 0.4，默认构建继续提供完整功能及现有 facade 路径。`--no-default-features` 现在真正省略 SDK；所有 connector 配置/身份类型仍可解析，未编译 connector 返回 typed error。子 crate 暂不独立发布。快照 v10 和恢复格式与本次 main 基准相同，未引入服务端数据迁移。

ONTAP UNIX security style 共享不保留受保护的 checkpoint DACL，当前程序按既有合约拒绝 hash checkpoint；未放宽权限保护或更改共享配置。之前独立 Samba 的正向恢复仅作为历史补充证据，本次不重复计入。

HA nameservice 连接和恢复通过不等同于服务端故障切换/停机注入通过；本次没有执行服务端故障注入，也没有重跑历史报告中的 S3 全版本分页场景及 DXN-2 NFSv4.0。测试使用唯一临时目录、前缀和 DXN 临时桶；未创建/轮换服务身份或修改服务端配置。
