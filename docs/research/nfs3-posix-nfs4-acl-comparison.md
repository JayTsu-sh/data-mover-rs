# NFSv3 POSIX ACL 与 NFSv4.1 ACL 的可比较性

调研日期：2026-10-28。问题：从 NFSv3 读取的 POSIX ACL，复制到 ONTAP NFSv4.1 后，能否判为 ACL 相等，进而放开 integrity 的跨编码比较限制。

后续已找到 Linux nfsd 源，完成真正 POSIX ACL 的设置、映射和访问验证：三个受限文件用例得到正例，目录继承及多组条件仍有差异。详见 [Linux NFSv3 → ONTAP NFSv4.1 真机报告](../reports/2026-10-28-linux-nfs-acl-validation.md)。下文保留最初设备样本与理论分析，不把后续正例扩大为通用相等。

## 结论

**可以对有明确范围、完整输入和已验证映射的子集做语义比较；不能普遍认为 POSIX ACL 与 NFSv4 ACL 相等。当前 ONTAP 真机样本不足以放开限制。**

这里有两个独立条件：先证明两端取得了完整 ACL，再证明其权限语义相同。NFSv3 未返回 ACL 属于观察缺失，不能通过转换 mode 补成“完整 ACL 相等”。当前实验恰好遇到这一情况，详见 §4。

| “相等”的含义 | 能否成立 | 需要的证据 |
| --- | --- | --- |
| 原生 ACL 表示相同 | 跨 POSIX/NFSv4 编码不直接成立 | 同模型、完整原生条目比较；这是当前实现的范围 |
| 当前对象的指定权限语义相同 | 部分场景可以 | 明确操作集合、身份映射、ACL 解释规则，并比较所有相关主体/组组合 |
| 完整 ACL 行为相同 | 条件更强，不能从 rwx 相同推出 | 还要覆盖额外权限、目录继承及承诺范围内的后续变化 |
| 只知道 mode 相同，一侧 ACL unsupported | 不能判定 ACL 相等 | 应保持未验证，或者补齐原生 ACL 观察 |

下文“可以放开”的条件是工程建议，并非协议对任意服务器的保证。

## 1. 协议版本不等于 ACL 模型

NFSv3 标准属性包含 mode、uid、gid，没有完整 ACL 字段；ACL 扩展能力要另行确认。不能仅因连接使用 NFSv3，就认为取得了 POSIX ACL。[RFC 1813 §2.5、§3.3.1](https://www.rfc-editor.org/rfc/rfc1813.html)

ONTAP 官方的 UNIX security style 支持项是 NFSv3 mode bits 和 NFSv4.x ACLs。通过 v3 显示的 `user::/group::/other::` 三条基本权限，不足以证明底层没有 NFSv4 named ACE。[ONTAP security styles](https://docs.netapp.com/us-en/ontap/nfs-admin/security-styles-their-effects-concept.html)

更直接地，官方 TR-4067 第 36 页明确说明 ONTAP 不支持 POSIX ACL；它列出的 NFSv3 细粒度权限途径包括 NTFS 权限及身份映射，或经 v4 管理挂载设置的 NFSv4 ACL。这与本次 v3 `setfacl` 不支持的结果一致。[NetApp TR-4067，第 36 页](https://www.netapp.com/media/10720-tr-4067.pdf#page=36)

因此需要区分：

- **完整 POSIX ACL**：源确实支持并成功读回 access/default ACL。
- **mode 的 POSIX 外观**：工具只显示基本三条，不包含底层扩展 ACL。
- **源原生 ACL 未知**：协议或实现不支持读取，不能将其当成空 ACL。

## 2. 为什么需要语义比较

### POSIX 的优先级和 mask

Linux 实现优先检查 owner、命名用户、匹配的组，最后才可能使用 other；命名用户和组的权限受 mask 限制，owner/other 不受该 mask 限制。匹配组但权限不足时不会回退到 other。目录的 default ACL 以及创建 mode 也参与子对象权限生成。[Linux `posix_acl_permission`、`posix_acl_create_masq`、`posix_acl_create`](https://github.com/torvalds/linux/blob/master/fs/posix_acl.c)

### NFSv4 的顺序和权限集合

NFSv4 按 ACE 顺序处理尚未决定的请求权限位。`EVERYONE@` 包含 owner 和所属组，并不等于 POSIX other。其权限还区分 write/append、删除、修改 ACL 等操作；继承也有面向文件、目录和 inherit-only 等标志。[RFC 8881 §6.2.1](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1)

由此推导，以下捷径都不能证明完整 ACL 相等：仅比较 rwx、把 other 直接换成 EVERYONE、将 ACE 排序后比较、忽略 mask 或默认/继承信息。

### 实现存在映射，但映射成功不等于无损等价

Linux nfsd 的 `_posix_to_nfsv4_one` 会应用 mask、插入 DENY、安排组 ALLOW/DENY 顺序，并补充部分管理权限；反向转换的代码注释明确选择更严格的结果。这证明跨模型转换有实际实现，也说明转换不能简化成逐条改名。[Linux nfsd `nfs4acl.c`](https://github.com/torvalds/linux/blob/master/fs/nfsd/nfs4acl.c)

ONTAP 对 chmod 的处理也受 `v4-acl-preserve` 影响：当前文档默认保留并调整 ACL，配置也可使其删除 ACL。不能用某次 chmod 后的结果推导所有设备上的后续行为。[ONTAP ACL modification](https://docs.netapp.com/us-en/ontap/nfs-admin/enable-disable-modification-nfsv4-acls-task.html)

## 3. 三个反例

这些是根据上述规则推导的例子，不是本轮新增真机结果。

### 命名用户被 other 扩权

合法 POSIX access ACL 中，若 `user:alice:---`、`mask::r--`、`other::r--`，Alice 仍不可读。若转换时遗漏对 Alice 的限制，只加入允许 EVERYONE 读取的 ACE，Alice 就会获得读权限。owner/group 的基本三条相同不能发现此错误。

### 单权限相同不代表组合请求相同

考虑同属 g1、g2 的普通用户：g1 只读，g2 只写，mask 为 rw。Linux `posix_acl_permission` 要求某一个匹配组条目覆盖整个请求，因而复合 rw 请求失败；两个分别允许 read、write/append 的 NFSv4 组 ACE 可以累计满足请求。只分别测试单个 read 和 write 会漏掉这个差别。

这一例子直接来自 Linux 的请求检查和 RFC ACE 求值规则；它不声称所有服务器或所有系统调用都采取相同请求拆分方式。即便使用 Linux nfsd 的映射，也不能未经验证就承诺所有操作组合等价。[Linux 检查入口](https://github.com/torvalds/linux/blob/master/fs/posix_acl.c#L359)、[NFSv4 求值规则](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1)

### 当前目录访问相同，子对象权限不同

两个目录可拥有相同的当前访问权限，但默认 ACL 或 NFSv4 继承标志不同。它们随后创建的文件或子目录会得到不同权限。因此当前访问相同与继承行为相同需要分别验证。

## 4. 2026-10-28 真机证据

本节复用本会话已完成的实验；本次调研没有重新修改远端配置。日志归档见 [NFSv3 → NFSv4.1 ACL 实测](../reports/2026-10-28-nfs3-nfs41-acl-evidence.log)。

| 实验 | 已观察到的结果 | 能证明什么 |
| --- | --- | --- |
| DXN 两套设备通过 v3 设置命名 POSIX ACL | `setfacl` 均返回 `Operation not supported` | 当前环境未能建立扩展 POSIX ACL 正例；不泛化到所有型号/配置 |
| 用户补充的 `10.131.5.221:/m1-source`，强制 NFSv3 | 挂载成功；access/default ACL 的读写均返回 `EOPNOTSUPP`；重挂结果相同；RPC 100227 v3 未注册 | 当前导出也无法提供扩展 POSIX ACL 夹具，未进入跨模型复制验证 |
| ONTAP v3 设置 POSIX ACL | 同样返回 `Operation not supported` | 未取得用户期望的“扩展 POSIX 源 ACL”夹具 |
| ONTAP 基本 0640 文件由 v3 复制到 v4.1 | 内容、uid/gid/mode 相同；另用 v4.1 读取源与目标，原生三条 ACE 相同 | 该基本权限样本的原生 ACL 确实一致 |
| 在同一源文件经 v4.1 加入 `A::42421:r`，再经 v3 复制 | 目标丢失 named ACE，内容和 mode 仍相同 | mode 相同不能证明原生 ACL 相同 |
| 标记源文件的 v3 POSIX ACL 查询 | `getxattr(system.posix_acl_access)` 返回 `EOPNOTSUPP`，getfacl 仍显示基本三条 | 三条输出并非完整 native ACL 的证明 |

标记用例的当前 compare 输出：

```text
matches=true
ignored=[Acl { source: None, destination: Some(NfsV4) }]
```

这表示其余已执行检查相符，ACL 没有获得相等结论。原生 ACL 差异是用 v4.1 独立读回确认的；没有以 UID 42421 实际登录执行访问测试，不将这一步描述成已测得的有效访问结果。

远端夹具按用户要求保留在：

```text
10.128.61.200:/ontap_lisaauto_nfs/
  data-mover-test/integrity-plan/ontap3-to-ontap41-20261028/
    source/access.bin
    destination/access.bin
```

新增 NFSv3 源测试目录也已保留：
`10.131.5.221:/m1-source/data-mover-test/integrity-plan/posix-v3-221-20261028-1158/`。
其中 `access.bin` 的 mode 为 0640，另有 `inheritance/` 目录；命名和默认 ACL 均未写入成功。

## 5. 对 integrity 限制的建议

当前 [`compare_acl_values`](../../src/integrity/permissions.rs) 只在两端都取得 ACL 且编码相同时比较 `AclMetadata`；编码不同或观察缺失则记录 ignored。同编码比较保留 payload 与顺序，并非有效权限求解器。NFS [`acl::encode`](../../src/storage/backends/nfs/acl.rs) 保存 NFSv4 ACE；[`AclMetadata`](../../src/model/metadata_observation.rs) 目前没有跨模型映射契约。

**建议允许将来的跨模型语义比较，但保留以下准入条件，而不是直接删除 encoding 检查：**

1. 两端均成功取得完整 ACL。Unsupported、未知、mode-only 投影不得补成空 ACL；显式确认不存在 POSIX 扩展 ACL 时，才可结合 mode 构造基本权限输入。
2. 明确身份命名空间和 owner/group 关系；确认双方的 UID/GID、NFSv4 principal 与组成员含义一致。仅字符串或数字相同不能自动证明跨服务器身份相同。[RFC 8881 §5.9](https://www.rfc-editor.org/rfc/rfc8881.html#section-5.9)
3. 声明比较范围。首期可以只研究普通文件当前 read/write/execute 访问；这必须与“完整 ACL 相等”区分。目录默认 ACL、继承和额外 NFSv4 权限未建模时，不能给完整相等结论。
4. 使用双方实际求值规则证明所承诺范围内的等价；覆盖命名用户优先级、mask、多组和组合请求。转换成功、转换后 mode 一致或少数账号访问成功均不够。
5. 无法覆盖的输入返回不可比较；已找到范围内的权限差异返回不等；仅在证据完整且证明成立时返回范围内相等。严格 ACL 校验的调用方还必须检查 ACL 是否 ignored。

最容易验证的起点是：完整观察、身份已对齐、无命名主体、无继承的普通文件，并验证 NFSv4 ACL 的确是相应基本权限表示。即使这个子集通过，也只能按声明的权限范围放开。

对于当前 ONTAP v3 → v4.1 路径，优先方案是另用 v4.1 对源进行原生 ACL 观察，再与目标原生 ACL 比较。这可以检查实际复制结果，但需要可靠绑定源对象身份，且仍不能使 v3 复制自动保留其无法读取的 ACL。

## 6. 放开前需要补齐的验证

| 场景 | 应验证的结果 |
| --- | --- |
| 真正支持 POSIX ACL 的源 → ONTAP v4.1 | 成功创建并读回命名用户/组、mask；读取目标完整 ACE，验证正例 |
| mask 截断、命名用户比 other 更严格 | 无扩权，且按完整优先级比较 |
| 同时属于多个组，权限分散 | 单项与组合请求均符合声明的等价范围 |
| NFSv4 write 与 append 分离，额外 DENY/管理权限 | 发现差异或明确不可比较，不静默忽略 |
| 默认 ACL、文件/目录继承、创建 mode | 当前权限与子对象生成行为分别验证 |
| 查询 unsupported、身份无法解析、mode-only 投影 | 保留不可比较，不出现 ACL 相等 |

最初调研未取得跨模型扩展 ACL 正例；后续 Linux nfsd 验证已补齐三个文件用例的限定访问正例，具体映射路径及反例见文首链接。项目尚未实现转换器，也未因此放开比较限制。
