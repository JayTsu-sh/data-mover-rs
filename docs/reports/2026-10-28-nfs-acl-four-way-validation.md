# NFS ACL 四组合实现与真机验收

日期：2026-10-28。实施依据：[双向映射研究](../research/nfs-acl-bidirectional-mapping.md)。

## 结果

已实现 NFSv3 原生 POSIX ACL、NFSv4 原生 ACL 保留、双向受限映射，以及严格完整性比对。
**四种组合均已通过真实复制、内容比对和严格 ACL 比对；跨模型的通过范围为普通文件当前 rwx 访问。**
同模型保留原生配置，跨模型不承诺还原 mask 潜在位、未来 chmod/chown、继承或任意 NFSv4 管理权限。

| 组合 | 实际路径 | 最终成功复制 | 验收内容 |
| --- | --- | ---: | --- |
| POSIX → POSIX | Linux nfsd `.12` NFSv3 → Linux nfsd `.13` NFSv3 | 6 | named、mask 潜在位、零权限优先级、组包含链、不可跨模型的分拆组与零 mask 配置均原生保留 |
| POSIX → NFSv4 | Linux `.12` NFSv3 → ONTAP NFSv4.1 | 4 | named、masked、precedence、chain；独立语义证明及目标原生读回 |
| NFSv4 → POSIX | ONTAP NFSv4.1 → Linux `.13` NFSv3 | 5 | 上述四例及目标篡改后的恢复重拷；修复零 mask 权限放宽 |
| NFSv4 → NFSv4 | ONTAP NFSv4.1 → 同导出中的另一隔离目录 | 5 | 上述四例及含 EVERYONE WRITE_ACL 的原生配置，完整有序 ACE 保留 |

最终矩阵共 **20 次成功复制、4 次预期拒绝、23 次内容及 ACL 比对**；23 次中 22 次匹配，
另一次是刻意篡改目标 named user 权限后的预期不匹配。所有被接受的严格 ACL 检查均无 ignored。
另有 2 次 observed 输入检查，已计入 23 次比对。

## 环境与保留路径

| 端点 | 导出 / 协议 | 客户端配置 |
| --- | --- | --- |
| `10.131.9.12`，Linux nfsd | `/srv/nfs/v3`，NFSv3 + NFSACL 100227 v3 | uid=0、gid=0、noresvport=true |
| `10.131.9.13`，Linux nfsd | `/srv/nfs/v3`，NFSv3 + NFSACL 100227 v3 | 同上 |
| `10.128.61.200`，ONTAP | `/ontap_lisaauto_nfs`，NFSv4.1 | 同上 |

最终测试根（保留）：

```text
data-mover-test/integrity-plan/acl-four-20261028-final-2000/
```

`.12` 的 `posix/` 保存六种源文件；`.13` 的 `pp/`、`4p/` 保存 POSIX 目标；ONTAP 的
`p4/`、`44/`、`complex-src/`、`complex-dst/` 保存 NFSv4 源和目标。所有复制使用相同相对文件名，
以 endpoint root 区分目标。四次规划期拒绝没有发布其目标文件。

初次验证根 `acl-four-20261028-implementation-1900/` 也保留，作为零 mask 缺陷的原始证据。
原生协议专测根在两台 Linux 上均为 `acl-four-20261028-native-protocol-1828/`。
仅创建和更改隔离测试数据；未修改服务器配置、身份数据库或导出策略。临时客户端挂载均已卸载。

## 实现接口

```rust
// 原生配置必须保存，不能因能力不足而跳过。
CopiedMetadataRequest::default().with_required_acl();
IntegrityPlan::default().with_required_acl();

// 调用方明确确认数字身份与实际组解析一致，允许限定的当前访问等价。
CopiedMetadataRequest::default().with_acl_mapping();
IntegrityPlan::default().with_acl_equivalence();
```

旧 `IntegrityPlan { acl, xattrs }` 字面量需补充 `..IntegrityPlan::default()`；对新增
`RequireEquivalent` / `Equivalent` / `AclMappingRejected` 等枚举变体的穷尽匹配也需更新。

普通 `with_acl()` 仍按原有 BestEffort 规则报告不支持/编码不匹配。严格选择不会把 required ACL
缺失、不可比或失败变成成功。严格复制同时要求能够保留 numeric owner/group。

`MetadataApplicationReport::mappings()` 区分 `Exact` 与 `Equivalent`；
`IntegrityReport::acl_scope()` 区分 `NativeConfiguration { encoding }` 与 `CurrentRegularFileAccess`。
ACL 本身比较失败/未验证时不提供成功范围。跨模型证明通过后才由 ACL 授权证明覆盖基本 mode 差异，
owner/group 与 suid/sgid/sticky 等特殊位继续独立比较。

NFSv3 能力来自实际扩展探测，不由版本号推断。NFS ACL SET 后完整读回，核实 access/default、
原始 permissions/mask 或完整有序 ACE。已发布恢复路径只读核验最终文件的 ACL。
恢复核验还会检查计划中 numeric owner/group 与特殊 mode 位，防止 OWNER@ 的实际主体在发布后改变；
基本 rwx mode 差异仍由 ACL 契约处理。NFSv4 原生范围是 FATTR4_ACL，不包括独立 dacl/sacl 的 ACL 级控制字段。

## 正例与实际访问

四种共同可接受的源 ACL：

| 文件 | 关键配置 |
| --- | --- |
| `named.bin` | named user/group 为 r，mask r，other 无权限 |
| `masked.bin` | named user/group 原始 rwx，mask r；跨模型只承诺当前有效权限 |
| `precedence.bin` | named user/group 为零、other r，源 mask r；不能错误落入 other |
| `chain.bin` | 两个 named group 分别 r 与 rw；包含链允许复合读写 |

在最终复制结果上，另起非特权进程，以 named user、两个不同 primary group、other 四类身份，
对每个共同例执行 read、write、O_RDWR 和 execute access 检查。所有进程清空客户端补充组并移除能力。
源 POSIX、P/P、P/4、4/P、4/4 五处，共 **16 组主体/文件比较、320 个操作结果一致**。
`precedence.bin` 的 named user/group 均不能读取，other 保持可读；chain 的 rw 组能够 O_RDWR。

这组真机结果验证所列身份与操作。它不证明任意服务器的身份/补充组解析相同；Linux nfsd 的
manage-gids 可改变客户端补充组。全部七种非空 rwx 请求、owner/named/group/other 优先级及组组合，
由独立 POSIX/NFSv4 求值器的类枚举和单元测试另行覆盖，其中包含全部 512 种基本 mode 往返。

## 反例

| 反例 | 最终结果 |
| --- | --- |
| 两个组分别 r 与 w，要求跨模型转换 | 规划期拒绝，不满足组包含链；P/P 原生复制仍成功 |
| 扩展 POSIX ACL mask=0、other 有权限 | 跨模型规划期拒绝；P/P 原生配置仍保留 |
| NFSv4 EVERYONE WRITE_ACL 等额外管理权限 → POSIX | 规划期拒绝；4/4 原生复制和读回成功 |
| 仅选原生严格策略却要求 POSIX → NFSv4 | 规划期拒绝，需要显式映射策略 |
| 复制成功后将 POSIX 目标的 named user r 改为零 | 内容相同，严格 ACL 比对仍明确不匹配，ignored 为空；重拷恢复后匹配 |

畸形 payload、缺失/unsupported/failed 观察、非数字身份、ACE 顺序变化、部分 append/overwrite、
AUDIT/ALARM、继承和目录跨模型等反例还由公共模型、后端和公开 compare() 测试覆盖。
发布后恢复另有五项回归：owner/group 改变、特殊 mode 位改变、必需身份不可观察必须失败；
匹配身份及合法基本 mode 投影仍通过，无 ACL 的调用不增加检查，全部只读。该缺陷先出现三个失败回归，
修复后 NFS 后端完整 235 项测试通过。

## 实测发现并修复的零 mask 缺陷

第一轮严格比对通过后，额外的实际访问探针发现：`precedence.bin` 反向映射把有效权限并集写为
mask=0。ACL 文本中的命名条目仍为零，但 Linux 的 group mode 为零会绕过 ACL 检查，转入 mode
权限判断，令命名用户错误获得 other 的 read。

最小本机复现只改变 mask：相同 named user/group 零权限、other r，mask=0 时 named user 能读，
mask=r 或 rwx 时不能读。参考实现的对应分支见
[Linux v6.12 `acl_permission_check`](https://github.com/torvalds/linux/blob/v6.12/fs/namei.c#L325-L365)。
这里是依据代码与实验作出的因果判断，不声称两台服务器运行的源码版本恰为 v6.12。

修复后，反向映射在相关条目均为零且 other 有权限时生成非零 mask=7；条目权限仍为零，
不会增加实际授权，同时使 Linux 执行 ACL 检查。跨模型源若已有上述零 mask 歧义配置则拒绝。
数学 POSIX 求值器与同模型原生保留规则没有被改写。

新增两个针对该缺陷的回归先失败，修复后通过；公共模块共 19 项测试通过。
最终新根中重新执行全部复制及 320 个真实操作结果，全部符合预期。

## 原生 NFSv3 协议独立验证

两台 Linux nfsd 各 6 项断言通过：raw rwx 被 mask r 屏蔽仍完整保留、目录 access/default 设置、
default 清空、恢复，以及新建客户端后的文件与目录 ACL 持久性。另以只读 Linux 内核 NFSv3
挂载独立 getfacl，确认 named raw rwx、有效 r、mask 和目录 default 均一致。
目录测试验证原生协议配置保留，不代表整棵目录树的继承时序已实现或已验收。

可选 ONTAP NFSv3 检查在挂载阶段失败，未执行 POSIX ACL unsupported 断言。
该项没有计作通过；NFSv3 扩展不可用及失败响应由公共客户端 TCP 测试覆盖。

## 证据与重现

- [结构化结果](2026-10-28-nfs-acl-four-way-results.json)：最终所有命令/退出码/输出、实际访问结果、初次缺陷与最小复现。
- [原始日志](2026-10-28-nfs-acl-four-way-validation.log)：最终复制矩阵、访问矩阵、原生协议测试及内核独立读回。
- 实际调用入口：`tests/lab/tools/nfs_metadata_copy.rs` 的 `--acl-required` / `--acl-mapping`；
  `tests/lab/tools/integrity_input_probe.rs` 的 `--acl-required` / `--acl-equivalent`。
- 例如 POSIX → NFSv4，两个 URL 使用上表服务器、最终根 `posix/` 与 `p4/`：

```sh
cargo run --example nfs_metadata_copy -- \
  --source "$SOURCE_URL" --destination "$DESTINATION_URL" \
  --source-path named.bin --destination-path named.bin \
  --acl-mapping --expect copied --expect-acl applied
cargo run --example integrity_input_probe -- \
  --source "$SOURCE_URL" --destination "$DESTINATION_URL" \
  --key named.bin --input path --expected match --acl --acl-equivalent --expect-acl equal
```

## 实现阶段本地门禁

最终 `make ci` 通过，见 [完整 CI 日志](2026-10-28-nfs-acl-four-way-ci.log)：

- cargo fmt、架构依赖守卫、测试工具契约检查通过。
- Clippy 全目标零警告。
- 库测试 **1995 passed、0 failed、9 ignored**；workspace 集成测试与 doctests 通过。
- 全部 examples 构建、本地端到端验证通过。
- vendor/nfs-rs 独立库测试 **595 passed、0 failed**。
- NFS 后端单独验证 **235 passed、0 failed**；公共 ACL 模块 **19 passed、0 failed**。
- `git diff --check` 通过。未提交修改。

库中的 ignored 项为原有显式跳过测试，不计入通过数量；本文四组合真机测试另行执行并记录上述结果。

## 拆分协议补丁前的集成验证

本节记录拆分前基于 `nfs-rs` **0.8.8** 加 NFSACL 补丁的验证，保留 main 的 URL 解码等上游修复。
该协议补丁随后移到 nfs-rs 仓库；data-mover 等正式版本发布后更新 crates.io 依赖。
下列结果是拆分前的历史证据，不代表当前发布依赖已经包含这些新接口。
两路审查后统一模型 ACL codec，并修复观察时间精度误判和可选 NFSACL 探测的重试参数 panic。

- 完整 `make ci` 通过：Clippy 全目标零警告，库测试 **2000 passed、0 failed、9 ignored**，
  workspace 集成测试、doctests、examples 和本地端到端检查通过。
- vendor 独立库测试 **618 passed、0 failed、0 ignored**。
- 使用本轮构建的工具，在上文保留根的 `named.bin` 上再次完成 POSIX→POSIX、
  POSIX→NFSv4、NFSv4→POSIX、NFSv4→NFSv4 四次复制及四次严格比对；
  其中两个跨模型比对使用观察输入，全部 `differences=[]`、`ignored=[]`。
- 原生跨模型请求、带额外 NFSv4 管理权限的跨模型请求两次均按预期拒绝。

本轮复核复用了已保留的测试文件；上述四次复制与两次拒绝是原矩阵之后的额外检查。
见 [本轮原始日志](2026-10-28-nfs-acl-main-integration.log) 和
[本轮结构化结果](2026-10-28-nfs-acl-main-integration-results.json)。
