# Storage 变更架构复核

## 范围与方法

- 复核 PR #169 相对 `origin/main` `16043c710eb6230ef556037bd723efba04abe31e` 的完整代码变更，
  包括正式 crates.io `nfs-rs` 0.9.1 更新及本轮架构整改。
- 分别检查 Standards 与 Spec，再按实际职责、接口深度和维护时的改动集中程度决定整改。
- 依据 `CONTEXT.md`、storage-transfer architecture、metadata negotiation 与公开 README 契约。
- 最终差异中的 filter、Local 后端源码没有增量；它们已在基线中，本轮没有重复调整。

## Standards

### 1. 比较实现分散：已收拢

原 `integrity/comparison.rs` 负责比较编排，却从父 `mod.rs` 导入只有自己调用的
`compare_facts`、`compare_modified`、`compare_content`。修改一种比较行为需要在两个文件间跳转。

三个实现移入现有 `comparison.rs`；父模块保留公共输入/报告类型、Endpoint 和共用时间语义。
不增加文件，不改变公开的 `integrity::compare` 路径或比较结果。

### 2. 子项名字规则重复：已收拢

`storage/listing.rs` 与 `storage/listing/child.rs` 分别重复检查尾斜杠是否只用于单段 File key。
现在二者调用现有 `listing/names.rs` 的同一 predicate，各自保留错误类型与错误归属。
普通子项构造规则和路径拼写没有放宽。

### 3. ACL 选择器：保留现有公开接口

`IntegrityPlan` 与 `CopiedMetadataRequest` 使用布尔选择器表达可选、严格、等效 ACL 请求。
可以考虑在后续公开接口演进中引入一个强度类型；本轮保持它们与检查点绑定的兼容性，
没有为两个不同操作再增加一层公开抽象。实际 ACL codec、原生比较与跨模型证明仍集中于
`model::acl`，策略规划仍由 `metadata` 负责。

## Spec

### 1. 历史版本权限绕过 Metadata：已修复

原 `integrity/permissions.rs` 对 Current 调用 `Metadata::permissions`，对 Id 却自行检查能力、
观察所有权并构造数字身份。这重复了角色实现，也使独立的 named-permissions override 只对
Current 生效。

现在所有权限事实均通过 `Metadata::permissions(descriptor)`：

- Current 保留 inline 优先和原 bound 观察回退。
- Id 只读取 `observe_copy_bound_version` 绑定的版本，保留 mode-only 事实。
- NFS/HDFS 不支持 Id 时明确拒绝，不能退回当前文件。
- named-principal backend 保留自己的 override；integrity 不再根据 numeric ownership capability
  阻止它提供权限事实。

整个 Metadata trait 与 MetadataMutation 内聚到 `storage/roles/metadata.rs`，并经原路径 reexport。
已超长的 `roles.rs` 随之缩小，公开接口和调用路径保持兼容。行为修复与结构调整分开提交。

### 2. S3 marker 存在两套解释：已收拢

原 S3 projection 已经判断 marker/content/history，通用 traversal 又按大小、尾斜杠和 history
过滤一次，并另外解释根对象。规则变化需要同步两层。

现在 S3 `traversal_listing` 独占这些语义：

- 父 listing 已表示的 own object 不在子 listing 重复发出。
- 根对象只有当前无 history 的零字节 marker 可省略；content 或任何 history 都保持原有
  `Unsupported` 子项诊断。
- 根诊断跨页去重属于单次 projection，不再占用通用 traversal 的共享 AtomicBool。

通用 traversal 只观察、过滤、计数与调度后端投影出的子项。原始 namespace listing、删除语义、
续扫语义版本没有改动。原 generic 根分类测试迁至真实 S3 adapter；原计数和失败断言保留，
另验证重复服务器页仍只产生一次根诊断。

## 保留的职责划分

| Module | 拥有的职责 |
|---|---|
| `model::acl` | 原生 payload codec、原生比较、受限跨模型访问证明 |
| `metadata` | 从观察和目标能力编译策略、损失与 mutation |
| `storage::Metadata` | 版本绑定的元数据与权限事实、后端应用 |
| NFS adapter / 独立 `nfs-rs` | 模型到协议转换、写后读回与恢复核验 / RPC 实现 |
| S3 traversal projection | 对象、marker、history 和根对象的逻辑投影 |
| `integrity` / `traversal` | 比较事实 / 消费逻辑子项及调度 |

`staged_acl` 与 marker 观察私有模块承载恢复和观察不变式，有实际职责；不为减少文件数强行合并。
协议源码仍在独立 nfs-rs 仓库，PR 文件差异和新增提交历史均不包含 vendor。

## 验证

- 权限回归先捕获 current 降级、NFS/HDFS Id 降级及 named override 被绕过，再确认修复通过。
- 定向权限测试 24 项、integrity 38 项、NFS metadata 33 项、HDFS metadata 2 项通过。
- S3/traversal 244 项通过、5 项按原设置忽略；覆盖 root、marker、history、snapshot、resume、
  过滤、深度与计数，原有断言未因重构放宽。
- Rust 1.95 完整 `make ci` 通过：库测试 2005 项通过、0 失败、9 项按原设置忽略；Clippy、
  架构守卫、workspace 集成测试、doctests、examples 和本地 E2E 通过。
- 发布依赖的四向实机复制/严格比对和具体 ACL 规划反例全部通过，见
  [ACL 验收报告](2026-10-28-nfs-acl-four-way-validation.md)。
