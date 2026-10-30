# QoS 代码审查与 GitHub 主分支整合（2026-10-31）

PR：[GitHub #172](https://github.com/JayTsu-sh/data-mover-rs/pull/172)。审查基准 GitHub main `4149575e91fe392ee853725e110d6c1be9c6b562`，最终代码提交 `baac11505b0f0825afdb903d3bcf6f42ebc4d7c1`；后续审查附件提交仅更新报告。合入状态和最新 GitHub checks 以 PR 为准。

代码审查结论：**可合入，0 个安全／正确性阻塞，保留 1 项非阻塞 P3 维护问题。** 这不是四场景性能全部通过的结论。全部生产和工具变更纳入审查，历史失败、候选撤回及原始证据不覆盖。

## Standards

独立 Standards 轴检查架构、取消、资源/凭据、范围及文档。深层限定路径的 P3 已用顶部 import/时钟别名修正；feature 和 S3 transport 文档 P3 已补全。与已测代码比较的 4 个 Rust 文件经 import/别名规范化后等价，见 [等价核对](architect-import-equivalence.json)。

补充核查更正了此前报告的“0 未关闭”：新 CLI 入口及 `SourceQosBudget::schedule` 超过仓库约定的 50 行。**当前仍有 1 项 P3，未关闭。** 仓库没有 lab/helper 长函数例外，Clippy/file audit 也未强制这一条。本次保留为明确维护债务，避免为风格扩大已验证热路径的重构；后续应以独立纯 refactor 提取准备、测量、判定、清理阶段，并验证计时边界和调度顺序。此决定不是已有规范豁免，也不表示该项已修复。

原始审查、复审及更正分别见 [初审](standards-review.md)、[第一次复审](standards-rereview.md)、[代码复审](standards-final-review.md)、[函数长度补充](function-size-review.md)。最终 Standards：0 个阻塞，最严重未关闭项为 P3 长函数。

## Spec

独立 Spec 轴发现并修复：

- **P1 清理越界**：原 guard 只匹配原始路径中的 run ID，`..`、编码父路径或 Local symlink 可重定向清理。现在拒绝父组件和 NUL；Local 根在连接前 canonicalize，实际路径必须保持原 run 根及下级路径，拒绝 run 根本身和下级 symlink；验证后的 canonical endpoint 才传给连接器。
- 复审又发现未知 `://` scheme 被 guard 当远端、连接器却回退 Local。现显式限定支持的 scheme，未知或大小写不匹配的 scheme 均拒绝；slash 出现在 S3 凭据时按实际解析器的最后 `@` 规则取路径，不能把凭据/query 中的 run ID 当作根。
- **P2 验收假绿**：未知、重复或空 profile 选集原本会 0/0 成功。现在在生成夹具或调用后端前拒绝，最终判定同时要求 checks 非空；新合同已接入 CI。

[Spec 初审](spec-review.md)、[复审](spec-rereview.md)、[最终复审](spec-final-review.md)。最终 Spec：0 个未关闭代码阻塞；高 QoS 的性能目标仍未完成实测验收，不能写为已解决。

## 最终本地验证

Rust 工具链 **1.95.0**，与仓库 CI/MSRV 一致：

- `make verify` 通过：format、架构 guard/自测、harness contracts、14 组 feature 隔离配置、全目标 Clippy、workspace tests、examples、e2e-local、文件大小审计。
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` 通过，包含 opt-in diagnostics 示例。
- `cargo test --workspace --all-features --locked --no-fail-fast`：**2196 passed，0 failed，45 ignored**，包含 doctest。默认 workspace 同为 2196/0/45；ignored 未计为真实服务测试。
- Python 全部 **146 passed**，其中 QoS **34 passed**；Rust cleanup scope **3 passed**。
- 真实 cleanup 可执行程序 **11 项通过**：父路径、编码父路径、run/子目录 symlink、未知/大写 scheme、含 `://` 的 Local symlink 反例均被拒绝；正常 run 子树删除成功，根和兄弟目录 sentinel 保留。只使用可丢弃本地目录，未联系外部 backend。[结果及二进制 SHA](cleanup-safety-e2e.json)、[可重跑脚本](cleanup-safety-e2e.py)。

开始时的默认 stable 验证在早期 feature 检查中主动停止，随后用 1.95.0 完整重跑，前一日志单独保存，不算最终通过证据。所有 changed 源代码、工具和规范文档通过 diff whitespace 检查；历史 CSV 的 CRLF 和原始日志尾部空行保留原字节，未为了 diff 检查重写取证数据。

## 性能证据与边界

此前保留版的持续验收为 9 条重点跨 backend 路径加 Local，**50 用例、415 判定通过**；短文件端到端失败和尾部原样保留。高 QoS 9 路有 **5 个原始比值低于 98%、8 路 A/A 精度不足**，仍待宿主机/实际存储池独占窗口复验。详见[完整性能报告](../qos-remediation-20261030/REPORT.md)。本轮没有把这些性能结果标成四场景全绿，也没有重跑整套真实 backend 性能矩阵。

性能数据绑定此前 `9b56c318` 构建与其归档源码。本轮审查后生产 Rust 只做 import/别名整理；其他新修复属于 lab 范围/选择器与 CI 合同。预取候选仍撤回。当前源码指纹见 [source manifest](source-manifest.json)，不会修改历史 manifest 伪装为同一个构建。

## 整合门禁与证据

GitHub PR 的 Linux 必需 CI 以及 Windows compile/native checks 全部成功后才合并，不使用管理员绕过或失败 checks 强制合入。报告提交不改变上述已验证代码；以 PR 当前 head 的检查为准。

[机器验证摘要](validation.json)、[完整审查/测试日志归档](raw-review-evidence.tar.gz)、[归档逐成员指纹](archive-manifest.json)、[附件 SHA256](SHA256SUMS)。审查源文件指纹共 654 项。归档不包含私有端点配置、可执行文件或大 payload。
