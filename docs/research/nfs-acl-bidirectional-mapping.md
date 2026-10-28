# NFS POSIX ACL / NFSv4 ACL 四种复制组合与完整性校验

> 实施后状态：本文件保留实施前的研究与当时证据范围。2026-10-28 已实现原生 NFSACL v3、双向受限映射和严格校验；当前验收结果及零 mask 修复见 [四组合实现报告](../reports/2026-10-28-nfs-acl-four-way-validation.md)。

调研日期：2026-10-28。范围：研究 POSIX → POSIX、NFSv4 → NFSv4、POSIX → NFSv4、NFSv4 → POSIX 四种 NFS ACL 复制组合；不修改项目源代码、不新增远端实验。本文复用 [Linux → ONTAP 真机报告](../reports/2026-10-28-linux-nfs-acl-validation.md) 和 [前期可比较性调研](nfs3-posix-nfs4-acl-comparison.md)。**本轮没有运行新的同模型端到端用例，也没有进行 NFSv4 → POSIX 的新真机验证。** 已有证据与建议能力分别标注。

## 1. 四种复制组合的回答

**四种组合都有可实现的支持范围。同模型应优先直接保留原生配置；跨模型的两个方向应实现为有准入条件的部分转换器。** 两类路径均需要完整观察和目标读回，不能让有损投影后的相同替代源、目标权限相等。

| 源 ACL → 目标 ACL | 推荐复制路径 | 严格校验的主要契约 | 当前项目与证据范围 |
| --- | --- | --- | --- |
| POSIX → POSIX | 直接复制 typed access/default ACL、原始条目权限和 mask | 同模型配置相等；身份和环境条件另外决定访问语义 | 本地 POSIX 保存已有单测证据；纯 NFSv3 URL 的 POSIX ACL 协议读写尚缺，不宣称已有 NFS3 → NFS3 完整复制 |
| NFSv4 → NFSv4 | 直接复制所选属性的完整有序 ACE；若选择 dacl/sacl，还需其 ACL 级 flags | 所选原生属性配置相等；目标改写时不能静默降级 | 当前 FATTR4_ACL 同编码链路部分可用，已有 ONTAP 历史正反例；不涵盖任意服务器或完整 dacl/sacl 状态 |
| POSIX → NFSv4 | 经过准入的模型转换 | 范围内独立语义证明 + 目标符合预期 | 已有三种文件的有限访问正例，转换借助 Linux nfsd；项目转换器待实现 |
| NFSv4 → POSIX | 经过准入的反向转换 | 范围内独立语义证明 + 目标符合预期 | 本文提出保守子集；尚无本轮反向端到端实测 |

**组权限包含链、普通文件/rwx、无继承这些限制属于本文首期跨模型转换方案，不适用于同模型原生配置复制。** 同模型目录 ACL 配置可以在读写能力完整的路径保留；整棵目录复制的继承时序和未来创建行为需要分别验证。

当前仓库尚未提供四种组合的完整统一能力。后续实现至少需要完整 POSIX ACL 读写、类型化映射、身份解析、可表示性判定和写后读回比较。已完成的三个跨模型文件正例证明存在有价值的输入子集；它们不足以证明通用转换正确。实际实验借助同一 Linux 源的 NFSv4 视图取得 nfsd 投影，并不是本项目已有转换器。[已有真机方法与限制](../reports/2026-10-28-linux-nfs-acl-validation.md)

RFC 允许服务器把 NFSv4 ACL 映射到能力较弱的本地模型，遇到不能精确表达的权限时可能收紧或拒绝。因此 SETATTR 成功不能作为无损转换证书。[RFC 7530 §6.2.1、§6.2.1.3](https://www.rfc-editor.org/rfc/rfc7530.html#section-6.2.1)

## 2. 必须区分的三个校验结论

| 契约 | 证明的事情 | 是否足够回答“权限相等” |
| --- | --- | --- |
| 原生配置相等 | POSIX 按 tag/qualifier/原始权限比较 access/default 与 mask；NFSv4 比完整有序 ACE 和所选属性的控制信息 | 同模型配置保真；身份或服务器行为不同仍可能导致访问差异 |
| 指定范围内语义相等 | 在声明的身份、组成员、操作和服务器解释规则下，两端授权决定一致 | 可以，但必须显示范围，不能写成完整 ACL 无损 |
| 目标符合已接受映射 | 实际目标原生 ACL 等于映射计划中的预期目标 ACL | 证明写入结果符合计划；单独成立不证明源、目标语义相等 |

例如，NFSv4 源只允许 append，映射策略将其收紧为 POSIX 不可写，目标也确实不可写：第三项可以成功，第二项失败。不能把第三项的成功显示为严格 ACL 保真成功。

同模型可以先实现 `NativeConfigVerified(attribute_scope)`：源、目标观察完整，目标原生配置与源（或显式身份重命名后的计划）一致，必需字段无 ignored。若还宣称实际访问相同，则必须额外满足身份、组成员和服务器解释条件。字节传输格式相同与 typed 配置相等也应区分，不能把 RPC XDR、xattr、工具文本直接混为一种表示。

建议新的强制校验契约同时满足以下条件，名称仅作设计示意：

```text
StrictMappedVerified(scope) =
    完整源观察 && 完整目标观察
    && 身份与服务器解释条件已满足
    && 独立可表示性证明(source, planned_target, scope)
    && 原生目标读回符合预期(planned_target, actual_target)
    && scope 内必需的检查没有 ignored / unknown
```

`RequireExact` 不能悄悄改成“丢弃 mask 潜在位之后相同”。若现有语义要求保留原始配置，跨模型不可满足就拒绝；新的限定语义校验应有显式策略和结果范围。允许损失的策略可以报告“映射符合预期，已收紧哪些权限”，同时保留“语义不等/未验证”。

这里的“独立”指判定器直接解释源模型和目标模型，而不让两端都通过同一个有损 mapper 再比较。`F(source) == F(destination)` 可能掩盖两侧被删除的信息；即使往返转换恢复原始表示，也不能单凭这一点证明中间模型采用相同授权规则。目标比较可复用计划中已生成的目标值；**源到计划目标的可表示性证明必须另外成立。**

因此，“复制后 ACL 校验要通过”的可实现承诺是：只接受约定范围内可等价表达的输入，目标真实写入并完整读回后，按该范围的规则通过；一旦目标改写 ACL、出现未接受损失或观察不完整，复制后的强制校验就应失败。不是让每一次 SETACL 成功都自动通过。

## 3. Linux 已有双向实现能提供什么

本节核查固定版本 Linux v6.12 的 [`fs/nfsd/nfs4acl.c`](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs4acl.c)，作为算法参照，不声称与实验 VM 的内核版本逐字相同。

| 路径 / 函数 | 已有行为 | 不能推导出的结论 |
| --- | --- | --- |
| `nfsd4_get_nfs4_acl` → `_posix_to_nfsv4_one` | 将 POSIX access/default ACL 投影为有序 NFSv4 ACE；应用 mask，用 DENY 保持 owner/named 优先级，组 ALLOW 先于组 DENY | 不能证明任意组合请求、跨服务器身份或目录创建行为相同 |
| `mask_from_posix` | 文件 w 映射为 WRITE_DATA 与 APPEND_DATA；目录 w 还加入 DELETE_CHILD；补充固定的元数据权限 | 这些附加权限不是 POSIX ACL 条目原本具有的独立配置能力 |
| `nfs4_acl_nfsv4_to_posix` → `process_one_v4_ace` → `posix_state_to_acl` | 根据 ACE 顺序合并 ALLOW/DENY，生成 access/default ACL | 接受并生成 POSIX ACL，不代表精确逆变换 |
| `low_mode_from_nfs4` | 保守转换；文件仅在 WRITE_DATA、APPEND_DATA 都允许时得到 w，目录还要求 DELETE_CHILD | partial-write / append-only 不可凭转换成功判等 |

反向实现只接受 ALLOW/DENY 和有限标志；其注释明确选择更严格结果。目录处理中，仅 FILE_INHERIT 或仅 DIRECTORY_INHERIT 也会进入同一个 default ACL 状态，代码注释承认扩大为两种继承。这是服务器兼容转换行为，不能作为本项目的精确可表示性门槛。[Linux v6.12 反向实现及 `NFS4_SUPPORTED_FLAGS`](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs4acl.c)

### mask 的不可逆信息

POSIX `user:alice:rwx, mask::r--` 与 `user:alice:r--, mask::r--` 当前有效权限相同，原始配置不同。只保留有效权限的映射无法区分二者；放宽 mask 后的行为也不同。若用户要求来回恢复原始条目，必须另存原始 ACL 和映射元数据，并明确备份数据不属于目标实际执行的原生 ACL。双向“能复制”与原始配置的可逆往返是不同承诺。[Linux `summarize_posix_acl`、`_posix_to_nfsv4_one`](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs4acl.c)

## 4. 跨模型首期共同子集及两个方向的准入

以下为依据协议和 Linux 检查规则提出的**保守工程子集**，不是 RFC 对所有服务器的承诺，也不是声称已经证明所有可转换 ACL 都必须长成这种形式。

### 4.1 共同范围

首期只承诺普通文件当前 read/write/execute 授权，包含所有七种非空 rwx 组合请求；NFSv4 的 write 在此范围中始终表示 WRITE_DATA 与 APPEND_DATA 一起。范围不包含目录创建、未来 chmod/chown/组成员变化、删除和 ACL 管理操作。特权主体、root squash、LSM、只读导出等属于服务器环境条件，不能由 ACL 文本独立证明。[RFC 8881 §6.3.1](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.3.1)

共同前提：完整输入；明确 owner/group；命名用户和组具有无歧义、无主体合并的身份映射；两端实际采用相同组成员关系；无继承标志和审计语义；附加权限符合已经选择并验证的固定服务器 profile。拒绝无法识别的权限位、主体种类和标志。

### 4.2 POSIX → NFSv4

1. 校验 POSIX ACL 结构：owner/group/other、需要时存在 mask、主体唯一合法、access/default 状态完整。文件出现 default 数据视为异常，不静默删除。
2. 保留 owner 和 named user 优先级；组匹配失败时禁止落入 other。生成规范有序 ALLOW/DENY 布局，不能把 `other` 直接替换为无条件 EVERYONE ALLOW。
3. 应用 mask 得到各组有效权限，执行 §4.4 的多组准入。
4. 承诺仅为当前访问语义；若要求原始 masked 位和后续 mask 修改行为完全保真，拒绝有效权限投影或使用另行声明的备份方案。
5. 独立检查源求值和候选目标求值，并对实际目标读回执行同编码比较。

POSIX 的 owner/named/group/other 优先级、mask 和复合请求检查，应直接按 Linux [`posix_acl_permission`](https://github.com/torvalds/linux/blob/v6.12/fs/posix_acl.c) 实现独立判定器。该判定器不应调用转换器代替原生语义。

### 4.3 NFSv4 → POSIX：精确准入条件

首期采用“只接受已识别的规范 POSIX 兼容模板，再证明限定语义”的方式，避免把任意有序 ACE 的逆向综合作为首期范围。

1. 只接受普通文件有效的 ALLOW/DENY；拒绝 AUDIT/ALARM、未知位、继承标志、未知特殊主体和非固定元数据权限。此子集不承诺保留 dacl/sacl 的额外控制信息。
2. READ_DATA 和 EXECUTE 可单独决定；对每个相关主体/组条件，WRITE_DATA 与 APPEND_DATA 的最终授权必须一致。模板还要求相应 write 权限按固定成对规则出现；partial append/overwrite 不可省略或并成 w 后宣称精确。
3. OWNER、命名用户、组、EVERYONE 的有序布局必须属于已审核的 POSIX 优先级模板；任意 DENY 列表先返回不可表示。以后可用独立符号判定扩大范围，不能靠 ACE 排序获得准入。
4. 反推 POSIX access ACL 的有效 rwx，并以确定规则选择 mask，例如 named 和 group 有效位的并集；不宣称恢复曾经存在的原始 mask 或潜在位。执行 §4.4 的多组准入。
5. **独立判定器直接比较原始 NFSv4 ACL 与候选 POSIX ACL**，覆盖 owner、所有命名用户、其余用户、相关组条件和复合请求。只有无反例且在资源上限内完成证明才准入。模板和多组规则可以提供有界证明；可选通用求解器超时/超限须返回未验证。
6. 完整写入并读回 POSIX access/default 状态、owner/group、mode；目标中遗留的额外 ACL 条目也属于差异。

NFSv4 按顺序处理尚未决定的权限位，EVERYONE 包含 owner 和 group，AUDIT/ALARM 另外具有事件语义。这些是以上拒绝条件的依据。[RFC 8881 §6.2.1、§6.2.1.1、§6.2.1.5](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1)

### 4.4 多组复合请求的有界充分条件

Linux POSIX 对组的检查是：**至少一个匹配组条目覆盖整个请求，并通过 mask**；它不是先合并各组的 rwx 再检查。组匹配但不足时也不会回退 other。[Linux `posix_acl_permission`](https://github.com/torvalds/linux/blob/v6.12/fs/posix_acl.c)

对于规范模板中的组分支，NFSv4 可以从多个匹配组 ALLOW 累计权限。设匹配组的 mask 后有效位为 `P_i`，其并集为 `U`：

```text
POSIX 允许 q  ⇔ 存在 i，q ⊆ P_i
规范 NFSv4 允许 q ⇔ q ⊆ U
```

**工程推导：**若 owning group 和所有 named group 的有效 rwx 集合两两可按包含关系比较，则任意非空匹配组子集都有最大项，`U` 正好等于该项，上述两个条件对全部七种 q 相同。`r ⊆ rw` 可接受；`r` 与 `w` 互不包含，拒绝。零权限组也要保留其“已匹配”含义，不可删除成回退 other。

这是规范模板下的保守充分条件，不是任意 NFSv4 ACL 的完整可表示性定理，也不证明管理权限或服务器身份解析。检查最多八种不同 rwx mask 即可，无需枚举 `2^组数`。若未来允许受限的实际组组合，可以放宽为“每个允许组合都存在一项覆盖该组合并集”，但须给出稳定身份和成员关系证据。

本次还对上述抽象组分支进行有限穷举：八种 rwx mask 的 256 个族、所有非空成员组合和七种请求，共 44,135 次检查；52 个包含链族全部相等，其余 204 个族均有反例。这仅验证该抽象推导，不是转换器测试，也不覆盖 owner/named 优先级、身份、附加权限、继承或真实服务器访问。临时结果保存在 `/tmp/dm-acl-mask-model.json`，不作为永久真机证据。

已有反例正是两组分别只读和只写：Linux 源本地 `read=true, write=true, read_write=EACCES`，ONTAP 映射 ACL 的对应客户端测试全部成功。两项实验环境分别说明，不能冒充已统一服务器组数据库后的全域证明。[真机报告 §8](../reports/2026-10-28-linux-nfs-acl-validation.md)

## 5. 同模型复制：直接保留配置

### 5.1 POSIX → POSIX

推荐路径是完整读取 POSIX access ACL 和目录 default ACL，直接构造目标 POSIX ACL，不经过 NFSv4。严格 typed 配置相等的单位是 `(tag, qualifier, 原始 permissions)`；mask 自身也是完整条目，不先折算成 effective 权限。libacl 的 `acl_cmp` 就以条目 tag、qualifier、permissions 的双向对应判断相等，不要求条目展示顺序相同。[libacl `acl_cmp(3)`](https://man7.org/linux/man-pages/man3/acl_cmp.3.html)

必须保留：

- owner/group/other 条目，全部 named user/group 的原始 rwx，以及原始 mask；包括当前被 mask 屏蔽的权限位和零权限 named 条目。
- 目录 default ACL 的完整独立内容；明确“有 default”“已确认没有 default”“无法观察”的区别。目标旧 default 需要显式替换或清除。
- owner/group 身份与 named qualifier 的关系。若两端数值命名空间不同，使用明确且不合并主体的映射，结果须报告为身份重命名后的配置相等。
- 由 ACL 派生的 mode 和 suid/sgid/sticky 等独立元数据。顺序与权限足够性需要纳入已有元数据应用路径。

例如 `user:alice:rwx, mask::r--` 原样复制到 POSIX 目标是可表达的；改写成 `user:alice:r--` 即便当前能读的结果相同，也不满足原始配置保真。含 r-only 和 w-only 两个组的 POSIX ACL 同样可以直接保留，两端按同一种组请求算法检查，**无需通过 §4.4 的跨模型包含链门槛**。后续放宽 mask 的行为可在相同模型/身份/实现条件下保留；未来谁可以更改 ACL、组成员如何变化仍是另外的环境条件。

不要在恢复时无意重算 mask。`setfacl` 的默认工具行为可能重算 mask，除非明确给出 mask 或使用不重算的选项；协议/库实现应直接保留输入条目，而不是依赖会变更它们的命令默认值。[`setfacl(1)` 的 `--no-mask` / `--mask`](https://man7.org/linux/man-pages/man1/setfacl.1.html#OPTIONS)

NFSACL v3 的 access/default 是独立数据，必须请求实际条目，不能只请求 count。Linux GETACL 在没有扩展 access ACL 时可能由 mode 生成基本 ACL；因此可承诺 typed 基本配置保真，但不能据此推断底层 xattr 是否真实存在。NFSACL 的 Solaris 线表示还会给基本 ACL 合成 mask，并携带 owner/group 的 ID，条目顺序也可以不同；这些由协议解析恢复为 typed POSIX 配置，**不是删除扩展 ACL 的原始 mask**。[Linux `nfs3acl.c`](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs3acl.c)、[`nfsacl.c` 线表示说明与编码器](https://github.com/torvalds/linux/blob/v6.12/fs/nfs_common/nfsacl.c)

这一路径不必限制为普通文件：POSIX access ACL 可以设置在文件或目录，default ACL 可以设置在目录；无 default 的目标也需要确认清除结果。现有树复制能否正确调用这些接口、处理目录最终权限和创建顺序，是待验收的实现能力，不由模型可表达性自动推出。[libacl `acl_set_file(3)`](https://man7.org/linux/man-pages/man3/acl_set_file.3.html)

### 5.2 NFSv4 → NFSv4

推荐直接复制选定 NFSv4 属性，保留每条 ACE 的顺序、type、所有有效 flag 位、完整 access_mask 和 who。ALLOW/DENY 不能排序成集合，也不能只剩 rwx。append 与 overwrite、DELETE/DELETE_CHILD、WRITE_ACL/WRITE_OWNER、继承标志和审计类型，在目标确实支持相应特性的原生路径上均有保留机会，不必先落入 POSIX 子集。[RFC 7530 §6.2.1](https://www.rfc-editor.org/rfc/rfc7530.html#section-6.2.1)

需要先区分两种属性范围：

| 所选范围 | 需要观察和恢复的配置 | 不能宣称的内容 |
| --- | --- | --- |
| FATTR4_ACL / `acl` | 完整有序 ACE；允许的 ACE type 由服务器支持情况决定 | 读回这些 ACE 不能证明保存了 dacl/sacl 的 ACL 级 flags |
| NFSv4.1 `dacl` + `sacl` | 各自的有序 ACE 和 ACL4_AUTO_INHERIT / PROTECTED / DEFAULTED 等合法控制字段 | 不能将只复制 `acl` 的链路标成此范围的完全复制 |

只读取 `acl` 无法知道 dacl/sacl 的控制字段；通过 `acl` 设置时，服务器还 SHOULD 将两者 PROTECTED 置真。因此“ACE 列表相同”不能代表完整自动继承配置相同。针对 dacl/sacl 的要求需要单独观察和应用这些属性，并处理协议要求的设置组合；不能把它们混入 `acl` 的 ACE flags。[RFC 8881 §6.4.3.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.4.3.2)

`aclsupport` 仅用四个位声明 ALLOW、DENY、AUDIT、ALARM 类型支持，**没有声明所有权限位、继承模式、主体解析和底层存储都能无损实现**。支持 NFSv4 或支持 ALLOW/DENY 不足以完成能力准入；还要看所选属性可读写性、具体 ACL 特征和实际读回。不能存储某类型时服务器必须拒绝，能存但不能执行时协议也建议拒绝。[RFC 8881 §6.2.1.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1.2)

即便是 4 → 4，目标也可能是 Linux nfsd + POSIX 底层；它会执行 §3 的保守转换。所以“同线协议、同编码”不意味着任意 NFSv4 ACL 都能原样落地。目标拒绝或重写为更严格 ACL 时，原生配置精确模式应报告失败；用户另选损失策略后，可以报告目标符合被接受的变化，但不能继续报告原始配置保真。源也是 POSIX 底层时，NFSv4 观察本身已经可能丢失潜在 masked 位；4 → 4 只保留源所暴露的 NFSv4 属性，不恢复其隐藏底层配置。[Linux nfsd 双向投影实现](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs4acl.c)

同模型 NFSv4 配置也不应套用跨模型组包含链：双方确实执行同一 NFSv4 模型时，多个组累计满足请求是需要保留的原本行为。对同模型语义等价的判定，继续确认主体解析、环境和服务器 profile。

### 5.3 未知位、身份和目录的共同处理

**未知字段不能直接删掉。** 对同模型原样复制，只有无损 codec 能完整保存字段、目标确实支持该值、所选协议属性允许该值时，才可将其作为 opaque 配置传递并精确读回比较。实现不理解字段的授权意义时，只能报告保存了该配置，不能证明其语义等价。协议明确保留且要求清零的位不因“opaque”获得合法性；codec 会丢位或目标支持未明时，严格模式返回不能完成/未验证。跨模型转换则仍按 §4 拒绝无法解释的位。

同模型字符串或数值相等也可能对应不同主体，`manage-gids` 仍可能令两端实际组成员不同。原生配置保真结果可以独立报告；需要“实际访问相同”的调用方仍须核实完整身份命名空间和有效组列表，不能把它们从同编码推导出来。[RFC 8881 §5.9](https://www.rfc-editor.org/rfc/rfc8881.html#section-5.9)、[`rpc.mountd --manage-gids`](https://man7.org/linux/man-pages/man8/rpc.mountd.8.html)

同模型可以保存目录 default/inherit 配置，无需等待跨模型目录映射成熟。但树复制要规定父 ACL 何时生效、子对象何时创建、每个现存子对象何时设置自身 ACL，以及自动继承是否会再次改变它们。最终应逐对象完整读回；父配置相同并不能单独证明所有子对象已经相同。未来创建行为还取决于 mode/ACL 创建参数、服务器策略和身份。[POSIX 创建规则](https://man7.org/linux/man-pages/man5/acl.5.html#OBJECT_CREATION_AND_DEFAULT_ACLs)、[NFSv4 创建与继承规则](https://www.rfc-editor.org/rfc/rfc7530.html#section-6.4.3)

## 6. 跨模型支持与拒绝矩阵

表中“可支持”均表示建议实现后的契约，不表示当前项目已支持。

| 输入 / 目标要求 | POSIX → NFSv4 | NFSv4 → POSIX | 可给出的校验结果 |
| --- | --- | --- | --- |
| 完整普通文件基本 ACL；固定 profile、身份对齐 | 可支持 | 规范模板可支持 | 限定当前访问语义相等 + 目标符合计划 |
| named user/group、mask；组有效位满足包含链 | 可支持有效权限 | 规范模板可支持有效权限 | 同上，必须覆盖复合请求 |
| named rwx 被 mask 压成 r，还要求恢复原始 rwx | 有效权限可映射，原始配置不保真 | 无法凭 ACL 恢复隐藏配置 | 不得报告原始配置无损 |
| 多组分别提供 r 与 w | 拒绝首期严格语义模式 | 拒绝首期严格语义模式 | 已知组合请求差异 / 不满足子集 |
| append-only 或 overwrite-only | POSIX 本身无这种独立配置 | 无法精确映射为 POSIX w | 不可表示；显式收紧只能是有损结果 |
| 任意顺序的复杂 DENY | 正向模板可生成必要 DENY | 非模板拒绝，后续求解器可扩大范围 | 接受 ALLOW/DENY 类型不等于证明可表示 |
| 额外 DELETE、WRITE_OWNER、非 owner WRITE_ACL、named attributes 等 | POSIX 条目无独立对应，不能任意生成 | 固定 profile 外拒绝 | rwx 相同不能给完整 ACL 相等 |
| AUDIT/ALARM，sacl 事件行为 | POSIX 无对应 | 拒绝 | 不能静默丢弃后判完整相等 |
| 目录 access/default 与文件/目录/仅继承/不传播标志 | 可研究显式投影 | 极受限 default 结构可研究 | 首期不承诺继承语义相等 |
| 一侧 unsupported / 仅 mode 外观 / 不完整 ACL | 拒绝强制映射 | 拒绝强制映射 | 未验证，不能算强制 ACL 通过 |
| named 身份无法解析、多个主体合并、组集合不明 | 拒绝强制映射 | 拒绝强制映射 | 身份条件未满足 |

DELETE 还可能与父目录 DELETE_CHILD 共同决定删除，不能只投影文件自己的 rwx；WRITE_ACL 和 WRITE_OWNER 分别控制 ACL/mode 与 owner/group 修改，超出 POSIX rwx 条目的表达范围。[RFC 8881 §6.2.1.3.1–§6.2.1.3.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1.3.1)

## 7. 跨模型目录和身份为什么需要单独设限

### 默认 ACL 不等于完整 NFSv4 继承策略

POSIX default ACL 是子对象初始化模板，最终 access ACL 还受创建 mode 限制；不能从父目录投影相同推出子对象权限相同。[Linux ACL 文档：对象创建和 default ACL](https://man7.org/linux/man-pages/man5/acl.5.html#OBJECT_CREATION_AND_DEFAULT_ACLs)

NFSv4 能分别表达文件和目录继承、inherit-only、no-propagate，以及新属性中的自动继承控制。CREATE/OPEN 是否携带 mode/ACL 也影响最终结果。因此，即便仅研究 FI|DI|IO 对应的 default 子集，也需要固定创建流程和服务器行为，验证文件/子目录、不同 mode 和多代继承。[RFC 7530 §6.4.3](https://www.rfc-editor.org/rfc/rfc7530.html#section-6.4.3)、[RFC 8881 §6.4.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.4.3)

已有 parent ACL 在协议允许归一化后相同，但 mode 0600 和 0666 的新文件出现权限差异；0644 仅在所测样本一致。故当前环境不能将 default ACL 纳入完整等价支持范围。复制已有子对象时逐个设置 access ACL，可以验证这些现存对象；仍不能宣称保留未来创建行为。[真机目录反例](../reports/2026-10-28-linux-nfs-acl-validation.md)

### 身份映射必须覆盖 ACL 内所有主体

NFSv4 使用字符串身份，其本地解析不由协议固定；owner/group 映射成功并不代表 named ACE 映射成功。[RFC 8881 §5.9](https://www.rfc-editor.org/rfc/rfc8881.html#section-5.9)

工程上需要逐个映射 UID/GID、named user/group 和特殊主体，验证域、类型、映射唯一性以及目标的实际组成员关系。不能用字符串相等、数字相等或客户端声明的补充组证明跨服务器身份条件成立。

已有 Linux 源开启 `manage-gids=y`。该功能会以服务器查询得到的组列表替换客户端列表，主组不受同样替换；所以清空/指定客户端补充组仍不保证双端有效组相同。[`rpc.mountd --manage-gids` 官方手册](https://man7.org/linux/man-pages/man8/rpc.mountd.8.html)、[本次实证](../reports/2026-10-28-linux-nfs-acl-validation.md)

## 8. 目标读回如何比较才不会制造通过

建议将计划中的目标 ACL 按**目标原生模型**编码，写入后重新读取实际目标：NFSv4 比有序 ACE，POSIX 比规范化 typed access/default 条目。允许的归一化必须列入版本化契约；不能任意排序 NFSv4 ACE、删除 DENY、忽略未知权限或借反向投影清除差异。

一个已有、范围明确的归一化是：特殊主体上的 `ACE4_IDENTIFIER_GROUP` 求值时忽略。Linux 的 `A::GROUP@` 与 ONTAP 的 `A:g:GROUP@` 因此可以按协议规则归一化；普通 named group 的 g 标志不能这样忽略。[RFC 8881 §6.2.1.5](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1.5)、[真机原生 ACL 差异](../reports/2026-10-28-linux-nfs-acl-validation.md)

如果 POSIX 写入后服务器将三条基本 ACL 折叠为 mode，应先成功确认 access ACL 确实不存在，再按该模型规则重建基本表示。`EOPNOTSUPP`、读取失败和 ACL 确实不存在必须区分。目标上旧 default ACL 也不能被“没有计划写入”当成已清除。

owner/group、mode 与 ACL 的修改顺序应纳入计划；按声明的服务器 profile 完成 mode/owner 后设置 ACL，并读回最终状态。ACL 之后再次 chmod 可能重写 ACL，不能用它修正数值 mode 来掩盖映射问题。[RFC 7530 §6.4.1](https://www.rfc-editor.org/rfc/rfc7530.html#section-6.4.1)

POSIX extended ACL 的 mode group 位对应 mask，而 NFSv4 的派生 mode 有自己的计算规则；语义相等时，原始 group mode 数字也不必相等。因此限定语义模式应分别报告原始 mode 差异和该模式下的解释结果，保留 owner/group、suid/sgid/sticky 等独立检查。若策略要求原始 mode 精确保留，不能偷偷放宽；满足不了就报告不满足该策略。[Linux ACL 与 mode 对应关系](https://man7.org/linux/man-pages/man5/acl.5.html#CORRESPONDENCE_BETWEEN_ACL_ENTRIES_AND_FILE_PERMISSION_BITS)、[RFC 8881 §6.3.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.3.2)

## 9. 仓库实现缺口与建议顺序

以下定位来自本轮对当前工作区的只读核查；所有改动均为后续建议。同编码路径已有部分基础，应在新增跨模型功能前先建立完整的原生复制与强制读回契约。

| 层 | 当前实现 | 四组合支持需要补齐的内容 |
| --- | --- | --- |
| NFS 协议客户端 | [`Mount::getacl/setacl`](../../vendor/nfs-rs/src/mount.rs) 返回 NFSv4 `Acl`；默认实现明确拒绝 v3，v3 没有 POSIX ACL 覆盖实现 | 为 POSIX access/default 增加独立类型和 NFSACL v3 GETACL/SETACL；按实际服务能力探测，不能只根据 NFSv3 版本启用 |
| NFS 元数据后端 | [`acl.rs`](../../src/storage/backends/nfs/acl.rs) 仅编码 NfsV4；[`metadata.rs`](../../src/storage/backends/nfs/metadata.rs)、[`staged.rs`](../../src/storage/backends/nfs/staged.rs) 的 ACL 写入/目标声明沿用该模型 | 同时支持 Posix 和 NfsV4 原生编解码，正确声明每个连接可读/可写的模型；保留 Unsupported 与已确认无扩展 ACL 的区别 |
| 数据模型 | [`AclMetadata`](../../src/model/metadata_observation.rs) 保存 encoding、原生 access/default bytes；未提供语义转换器 | 保留原始数据，增加经过验证的 typed 解析和内部语义表示；RPC XDR 数据不能直接当作本地 POSIX xattr 字节 |
| 本地 POSIX 路径 | [`observation.rs`](../../src/storage/backends/local/observation.rs)、[`staged.rs`](../../src/storage/backends/local/staged.rs) 已读取/写入 access/default xattr | 可作为原生 POSIX 数据来源和验收参照；不替代纯 NFSv3 URL 客户端的协议实现 |
| 元数据规划 | [`compile_acl`](../../src/metadata/compile.rs) 直接接受同编码，异编码为 RequiresExternalMapping；[`PrincipalMapper`](../../src/metadata/mod.rs) 只映射 owner/group | 保留同编码直接复制，再增加跨模型两个方向的部分转换；需要身份重命名时覆盖 ACL 内全部 named 主体；已具备的应用顺序仍保持 ownership/mode 在 ACL 之前 |
| 普通复制请求 | [`CopiedMetadataRequest`](../../src/transfer/model.rs) 的 acl 是 bool；[`optional_policy`](../../src/transfer/engine/negotiation.rs) 将其转为 BestEffort | 分别表达“要求原生配置保真并验证”和“要求映射语义等价并验证”；不能把现有可跳过语义当成强制保真 |
| 完整性比较 | [`compare_acl_values`](../../src/integrity/permissions.rs) 对同编码比较原生 payload，异编码 ignored；[`IntegrityReport::matches`](../../src/integrity/mod.rs) 仅看 differences 是否为空 | 同模型增加明确的 typed/有序字段和属性范围校验；跨模型增加带方向、范围与身份条件的比较。强制模式必须确认 ACL 确实已比较并相等，ignored 不能计为通过 |
| 权限与恢复绑定 | mode 当前直接数值比较；[`checkpoint_binding`](../../src/metadata/checkpoint_binding.rs) 和复制协商绑定已规划 mutation | 在显式语义模式下正确解释 ACL 与 mode；将映射版本、范围、身份条件及目标计划纳入可验证恢复上下文，防止重试时换用不同规则 |

NFSACL v3 是 RPC 程序 **100227**，GETACL/SETACL 分别为过程 **1/2**，access/default 有独立 mask 位。Linux 服务端分别读写这两种 ACL，并可能在没有扩展 access ACL 时返回 mode 构成的基本 ACL。这与 NFSv4 属性 FATTR4_ACL 不同。[Linux NFSACL 定义](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/nfsacl.h)、[NFSACL v3 服务端实现](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs3acl.c)

| 优先阶段 | 交付范围 | 独立完成条件 |
| --- | --- | --- |
| 1 | 加固已有 4 → 4 FATTR4_ACL 原生复制、强制读回和失败报告 | 有序 ACE 全字段保留；目标改写、遗漏或未观察时不能算严格通过；不扩大成完整 dacl/sacl 承诺 |
| 2 | 增加 NFSACL v3 typed access/default 读写，完成 P → P | mask 潜在位、零权限 named、目录 default、旧 ACL 清除均保真；真实 NFS3 → NFS3 读回验证 |
| 3 | 逐个实现 P → 4、4 → P 受限转换 | §4 的独立准入、身份条件和目标读回同时成立；两个方向各有正例及必须拒绝的反例 |
| 4 | 按需求增加同模型 dacl/sacl、树复制时序验证及跨模型目录 profile | 所选属性完整性与目录创建/自动继承行为分别验收；不由文件用例推导 |

贯穿各阶段的计划需要包含：模型与属性范围、对象类型、完整源 ACL、owner/group、全部主体映射、profile/codec 版本、预期目标和已知损失。同模型计划直接保留原生配置，跨模型才计算候选投影并提供独立准入结果。编译阶段拒绝不满足策略的输入；应用阶段报告真实写入；integrity 阶段要求实际读回符合计划，不能以 `matches=true` 加 `Acl ignored` 满足强制 ACL 契约。

当前策略中的 `RequiresExternalMapping`、`RequireExact`、`AllowKnownLoss`、`BestEffort` 等应保留清晰区别。特别是简单 `--acl` 或布尔请求对应的 best-effort 行为，不能自动升级为已经完成上述强制证明。

当前项目的 NFSv4 ACL codec 只保存 FATTR4_ACL 的有序 ACE，不涵盖通用 NFSv4.1 dacl/sacl 的 ACL 级控制标志。即使 vendored 客户端另有这些接口，同模型现有承诺也应明确为所选 `acl` 属性；跨模型首期另限定为 §4 的普通文件 access 子集。不能把两者统一描述为所有 v4.1 安全状态，也不应因此排除同模型目录 `acl` 配置复制。

### 9.1 复制后的验证与独立 compare

- **复制后的验证**有复制计划：同模型保留完整配置及已接受的身份重命名，跨模型先证明源与计划目标符合限定语义；两者都在写入后完整读回目标并与计划比较。若契约还承诺实际访问相同，再核对所需身份和服务器解释条件。应用报告中的 Applied 本身不足以替代读回。
- **单独调用 compare**可能没有复制计划：必须在同一个显式契约下完整读取两端。同模型比较 typed POSIX 配置或所选 NFSv4 属性的有序字段，跨模型使用各自模型的独立语义判定器。不能依赖一份不存在的历史转换记录，也不能只看编码不同就跳过后仍宣称 ACL 一致。
- 两种入口都应支持现有路径和 ObservedEntry 选择方式，并核对对象/观测是否仍有效。无法确定身份、观察发生变化或解析不完整时，不制造相等结论。
- 当前自动检查的 owner/group 与特殊权限位继续独立检查。仅在显式 ACL 语义契约下处理 mode 表示差异；默认原生比较和 RequireExact 的精度不能被隐式降低。其他类型、内容、时间等检查也不因 ACL 映射成功而自动通过。

## 10. 最小验收矩阵

下表是后续实现验收建议，**不是本轮已执行测试**。已有历史用例只为其明确覆盖范围提供证据。

### 10.1 同模型原生配置

| 验收案例 | 预期 |
| --- | --- |
| P → P：named 原始 rwx、mask 仅 r、零权限 named 条目 | 保留原始条目和 mask；仅有效权限相同不能通过配置校验 |
| P → P：owning/named 组分别 r 与 w | 可直接复制，复合请求拒绝行为仍按 POSIX；不施加跨模型包含链门槛 |
| P → P：目录 access/default 内容不同；源无 default 而目标有 | 分别保留，源无 default 时目标旧 default 被清除并读回确认 |
| P → P：NFSACL 基本条目合成 mask/顺序/owner 字段差异 | 以已定义 typed 规范解析比较；不能顺带删掉扩展 ACL 原始 mask |
| P → P：真实 NFS3 URL 源与目标 | 使用新协议实现独立端到端验证；本地 xattr/snapshot 单测不代替该用例 |
| 4 → 4：交错 ALLOW/DENY、append-only、DELETE/WRITE_ACL/WRITE_OWNER、合法继承 flags | 目标完整支持时按原生配置保留；不要求变成 POSIX 兼容模板 |
| 4 → 4：target 将 GROUP@ 的 g 改写 | 原始表示模式报告差异；只有显式协议归一化模式才可忽略该特殊主体标志 |
| 4 → 4：目标为 POSIX backing 的 nfsd，不能精确存储给定 ACE | 拒绝或读回发现改写；严格原生模式失败，不能凭 SETACL 成功通过 |
| 4 → 4：`acl` ACE 相同而 dacl/sacl flags 不同 | `acl` 范围只报告该范围；完整 dacl/sacl 契约须发现差异 |
| 同模型未知字段、codec 丢位或 target support 不明 | 不删除；无法完成无损合法传递时严格模式未通过，语义保持未验证 |
| 同模型目录树，创建期间继承与最终显式 ACL 不同 | 按计划逐对象恢复与最终读回；另测未来创建，不能只比较父 ACL |
| 同模型 ID/字符串相同但服务端组解析不同 | 可分别报告配置相等与环境条件不满足；不能宣称访问相同 |

2026-09-23 的 [历史真机记录](../../.claude/docs/metadata-negotiation.md) 记录了同一 ONTAP 不同路径间 NFSv4.1 → NFSv4.1 复制：ACL Applied、原生 GETACL 逐 ACE 相同，以及带标记 ACL 的复制/omit 反例。对应 [metadata_matrix.sh](../../.claude/skills/e2e-nfs/scripts/metadata_matrix.sh) 使用命名主体标记和 EVERYONE WRITE_ACL 标记；[nfs_metadata_copy.rs](../../tests/lab/tools/nfs_metadata_copy.rs) 的 `verify_acl` 独立 GETACL，要求目标保留标记、完整列表与源相等、应用报告为 Applied。这是该链路的已有证据，未在本轮重跑，也不代表任意服务器特性均已覆盖。

本地 POSIX 的 [`required_acl_captures_access_and_directory_default_losslessly`](../../src/storage/backends/local/observation/tests.rs) 检查 access/default 原始数据观察和快照往返，证明本地保存基础；它不是 NFS3 → NFS3 复制的端到端测试。10 月 VM103/104 的设置/重挂实测同样只证明源协议确实支持扩展 ACL，不能替代尚未完成的项目原生 P → P 复制验证。

### 10.2 跨模型限定语义

| 验收案例 | 预期 |
| --- | --- |
| 两方向 basic / named / owner 优先级 / named 比 other 严格 | 子集内通过；目标读回一致；独立判定无反例 |
| mask 隐藏 rwx；正反向往返 | 当前权限可相同，明确不能恢复隐藏原始配置 |
| 组 r 与 rw；组 r 与 w | 前者可接受；后者严格模式拒绝，包含 O_RDWR / 复合请求反例 |
| append-only / overwrite-only | 拒绝精确反向；即使收紧后目标符合预期，也不能给语义相等 |
| DELETE、named WRITE_ACL/WRITE_OWNER、AUDIT/ALARM、未知位 | 固定 profile 外拒绝，不能通过投影消失 |
| target 将 GROUP@ 的 g 位改写 | 仅允许特殊主体的规定归一化 |
| 目标权限缺失/多余、ACE 顺序改变、默认 ACL 未清除 | 实际读回差异必须失败 |
| 同一有损 mapper 在两侧删掉相同字段 | 独立源语义检查仍须发现损失，禁止假通过 |
| unsupported、观察失败、身份冲突、求解超限 | 强制校验未通过，保留具体未验证原因 |
| parent 相同；0600/0644/0666 新建文件及子目录 | 首期拒绝继承等价声明；将来 profile 必须覆盖创建行为 |

已有三个文件用例的有限访问正例、目录反例和 manage-gids 证据继续有效；它们支持上述受限实现方向，不能替代新转换器的验证。[实测范围与原始证据入口](../reports/2026-10-28-linux-nfs-acl-validation.md)

## 11. 算法复用和协议进展

Linux `nfs4acl.c` 文件头提供三条款 BSD 风格许可，适合作为可追溯的算法参照；不能把相邻内核代码都视作同一许可。建议依据协议、模型和独立测试实现 Rust 转换器，原生内核实现不是可直接调用的 Rust 用户态转换库；如需移植具体代码，再逐文件核对所用版本、依赖和版权声明。[Linux v6.12 文件头](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs4acl.c)

截至调研日期，IETF 工作组的 [`draft-ietf-nfsv4-posix-acls-02`](https://datatracker.ietf.org/doc/draft-ietf-nfsv4-posix-acls/02/) 提议在 NFSv4.2 增加直接承载 POSIX ACL 的可选属性，以避免模型转换损失；它仍是 Internet-Draft，不能作为当前 NFSv4.1 或现有服务器已经具备该能力的依据。若将来两端能原生提供同一种 ACL 模型，应优先考虑该能力，而不是增加不必要的模型投影。
