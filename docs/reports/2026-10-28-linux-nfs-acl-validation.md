# Linux NFSv3 POSIX ACL → ONTAP NFSv4.1 ACL 真机验证

日期：2026-10-28。PVE：`10.131.9.20`（用户确认的地址）。

## 结论

**已找到真正支持 POSIX ACL 的 NFSv3 源，并取得受限文件访问的相等正例；完整 ACL 仍不能通用判等。**

- VM 103、104 均运行 Linux nfsd，提供 NFSv3 和 ACL 扩展服务。
- 在 VM 103 上建立 POSIX ACL，经 Linux nfsd 的 NFSv4 视图映射后写入 ONTAP，三个文件用例的所测身份访问结果一致，重新挂载后仍一致。
- 目录继承和跨服务器组成员解析出现差异。不能从父目录 ACL 表示相同推出创建子对象后的权限相同。
- 当前项目 NFSv3 后端没有取得源 ACL；实际复制跳过 ACL，丢失命名用户/组权限。跨编码 compare 仍报告 ACL ignored。

本次独立映射使用源服务器额外提供的 NFSv4.1 视图，不是项目已实现 POSIX→NFSv4 转换的证明。验证没有修改比较逻辑、服务器配置或用户/组数据库。

## 1. PVE 清单与源共享

| VM | 名称 / 管理 IP | 已确认能力 | 导出 |
| --- | --- | --- | --- |
| 103 | ubuntu-24e / `10.131.9.12` | Linux nfsd，`+3 +4 +4.1 -4.2`；RPC `100227 v3`（nfs_acl） | `/srv/nfs/v3`、`/srv/nfs/v4` |
| 104 | ubuntu-24f / `10.131.9.13` | 同上；扩展 POSIX ACL 设置、重挂读取和默认继承通过 | `/srv/nfs/v3`、`/srv/nfs/v4` |

对 guest agent 可用的 VM 检查内核 nfsd 状态和 `exportfs -v`；对具有已知 IP 的运行 VM 查询 RPC。VM 510–514 仅网络查询，没有执行 guest exec。VM 605 无 guest agent，未取得 IP，未完成检查。因此上述为已确认清单，不宣称覆盖无地址 VM。

源导出的访问范围包含本客户端所在的 `10.131.0.0/20`，配置为 rw/no_root_squash。未改变导出策略或启动任何服务。

实际源：

```text
nfs://10.131.9.12/srv/nfs/v3?version=3
```

目标：

```text
nfs://10.128.61.200/ontap_lisaauto_nfs?version=4.1
```

## 2. POSIX ACL 支持的实证

两台 Linux VM 均在独立目录成功完成：

1. 通过 NFSv3 设置命名用户 `42421`、命名组 `42422` 的 access ACL。
2. 为目录设置包含这些主体的 default ACL。
3. 创建继承 ACL 的子文件。
4. 卸载后重新以 NFSv3 挂载，读回上述命名条目及 default ACL。

这次 `getfacl` 明确包含命名用户、命名组及 mask，区别于此前仅显示 mode 三条的设备。

## 3. 先验证当前项目的实际行为

源 `access.bin` 具有命名用户/组的只读权限。使用本项目 `nfs_metadata_copy --acl` 从 NFSv3 复制到 ONTAP NFSv4.1，结果：

```text
OwnershipMode: Applied
Acl: Unsupported
ACL skipped: the source cannot read it
copied 33 bytes
```

目标原生 ACL 只有 owner/group/everyone，丢失了命名条目。以普通数值身份测试，UID 42421 和 GID 42422 成员在源上可读、目标上不可读。内容和 mode 相同不能掩盖这个差别。

对应 compare：

```text
matches=true bytes=33 differences=[]
ignored=[Acl { source: None, destination: Some(NfsV4) }]
```

因此有两个独立问题：服务器支持 POSIX ACL，不代表当前项目的 NFSv3 观察路径已支持；补上观察后，仍需跨模型映射及比较。

## 4. 独立映射方法和测试范围

为检验模型的可比性，使用下列路径：

```text
源 NFSv3 setfacl 写入 POSIX ACL
    → 同服务器、同导出、同文件的 NFSv4.1 nfs4_getfacl
    → Linux nfsd 生成的 NFSv4 ACL
    → ONTAP nfs4_setfacl
    → 读回目标原生 ACL，并测试实际访问
```

文件内容从 v3 源读取，owner/group/mode 先复制，ACL 最后设置。没有在 ACL 设置后再 chmod。目录则复制 parent ACL 后，在两端分别以相同 mode 创建新文件，以观察继承效果。

访问测试在本客户端通过 `setpriv` 使用数值身份，清空或显式指定补充组并丢弃 capability。测试为只打开文件的 `O_RDONLY`、`O_WRONLY`、`O_RDWR`，不写入内容；另记录 `os.access(X_OK)`，未实际执行文件。`true` 表示成功，数字 `13` 表示 EACCES。

主要主体为 UID 42421、GID 42422 的成员、other，以及具有补充组 42424 的主体。没有把 root 的访问成功当成 ACL 正例。该有限矩阵不覆盖全部身份、管理权限或后续 ACL 修改行为。

## 5. 普通文件正例

| 源 POSIX 用例 | 所测访问结果 | 说明 |
| --- | --- | --- |
| 命名用户/组只读，other 无权限 | 两端一致；重挂后仍一致 | 命名主体可读不可写，other 不可读写 |
| 命名条目 rwx，mask 仅 r | 两端一致；重挂后仍一致 | 映射保留当前有效只读权限 |
| 命名用户/组无权限，other 只读 | 两端一致；重挂后仍一致 | Linux nfsd 插入 DENY，保留 POSIX 优先级 |

每个用例检查四个主体，每个主体记录三种 open 及 X_OK；三个用例均 4/4 主体结果相同。**这里的“相同”仅指上述已测访问。**

mask 用例尤其需要区分当前有效权限和原始配置：POSIX 保存的 named rwx 被 mask 限成 r；投影后的 NFSv4 ACL 只体现有效权限。未来放宽 POSIX mask 时，源端潜在权限会恢复，不能据当前结果宣称两者全部后续行为等价。

## 6. 原生表示的差异：特殊主体上的 group 标志

Linux 输出 `A::GROUP@:rtcy`，ONTAP 读回为 `A:g:GROUP@:rtcy`。四个文件映射用例和 parent 目录均有该差别；保持 ACE 顺序、仅忽略特殊主体上的 `g` 后，这些原生列表相同。

RFC 规定特殊主体的 `ACE4_IDENTIFIER_GROUP` 标志在求值时忽略。因此此差别本身不代表有效权限不同，不能通过任意排序或删除其他标志来类推归一化。[RFC 8881 §6.2.1.5](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1.5)

当前 comparator 的真实行为也已验证：

```text
本地后端读取 v3 挂载（取得 Posix） vs ONTAP：
matches=true bytes=36 differences=[]
ignored=[Acl { source: Some(Posix), destination: Some(NfsV4) }]

源改用 NFSv4.1 观察，同一文件 vs ONTAP：
matches=false bytes=36 differences=[Acl { encoding: NfsV4 }] ignored=[]
```

第二项说明当前同编码比较仍是原生 payload 相等判断，尚未做上述协议允许的标志归一化。

## 7. 目录继承反例

源目录 default ACL 包含：命名用户 r-x、命名组 r--、mask r-x。将 Linux nfsd 的目录 NFSv4 映射写入 ONTAP 后，parent 原生 ACL 在 §6 的归一化下相同。

随后在两端分别创建同名新文件，umask 为 0，不再设置子文件 ACL：

| 创建 mode | Linux 源 POSIX 结果 | ONTAP 目标实际访问 | 判断 |
| --- | --- | --- | --- |
| 0600 | default ACL 的 named 条目保留，但 mask 为 ---，命名用户/组不可读 | 命名用户/组可读 | 不同，重挂后仍存在 |
| 0644 | named 有效只读，other 只读 | 所测 open 结果一致 | 仅本样本已测访问一致 |
| 0666 | default ACL 不授予组/other 写，源仍不可写 | 所测命名用户、组成员及 other 可写 | 不同，重挂后仍存在 |

此结果是在当前 ONTAP 配置下观察到的，未调整其 ACL 继承或 chmod 参数，不推广为所有 NFSv4 服务器的固定行为。它已足够否定“parent ACL 投影相同即可保证子对象权限相同”。

## 8. 多组和身份解析：分开验证两个因素

初次远端测试发现，客户端同时带 GID 42422 和补充组 42424 时，源只按前者获得读权限，目标可读写。核查 VM 103 得到 `/etc/nfs.conf` 中 **`manage-gids=y`**；重新使用独立 UID 测试，差异仍存在。

源端组缓存中也看到该未知 UID 的补充组列表为空。因此该远端差异包含服务器组解析因素，不能单独归因于 POSIX/NFSv4 ACL 算法，也不能把客户端声明的组列表当成两端实际使用的相同组集合。

为独立检查底层 POSIX 行为，又通过 VM 103 guest agent 启动一次无 capability 的普通身份进程，直接打开同一个源文件；不创建账号、不修改组配置。GID 42422 只读、补充组 42424 只写时：

```text
Linux 源文件本地 POSIX 检查，uid=42603 gid=42422 groups=[42424]：
read=true write=true read_write=13

ONTAP 映射 ACL，客户端同时带这两个组的测试：
read=true write=true read_write=true
```

这与 Linux POSIX 检查要求某一个匹配组覆盖复合请求、NFSv4 允许 ACE 累积满足权限位的模型差别一致。两项是分别进行的访问实验，不冒充已建立统一服务器身份数据库的跨服务器全量等价证明。[Linux POSIX ACL 检查](https://github.com/torvalds/linux/blob/master/fs/posix_acl.c)、[NFSv4 ACE 求值](https://www.rfc-editor.org/rfc/rfc8881.html#section-6.2.1)

## 9. 对实现方向的影响

本次证据支持继续实现**有范围的语义比较**，不支持直接删掉 `encoding` 限制：

1. 先补 NFSv3 POSIX ACL 的完整观察；当前项目连接上的 ACL 能力不足，不能用真实服务器支持代替。
2. 区分原生表示相等和当前指定访问语义相等。即便同属 NfsV4，也可能需要协议允许的特殊标志归一化。
3. 对文件子集明确声明比较操作和身份条件；不能仅看 rwx，或把组合请求/目录继承留在隐含范围内。
4. 对默认 ACL、继承、未解析身份及未覆盖的权限，保留不可比较。当前 best-effort 的 matches=true 必须结合 ignored 理解。

代码尚未因此更改。完整理论条件见 [可比较性调研](../research/nfs3-posix-nfs4-acl-comparison.md)。

## 10. 证据与保留夹具

- [原始执行日志](2026-10-28-linux-nfs-acl-validation.log)：POSIX 设置/重挂、项目复制、手工映射、实际访问及复测。
- [结构化结果](2026-10-28-linux-nfs-acl-results.json)：VM RPC 清单、ACL 列表、逐主体结果及新挂载复测。

保留目录：

```text
VM103 10.131.9.12:/srv/nfs/v3/
  data-mover-test/integrity-plan/linux3-to-ontap41-20261028/
    access.bin, cases/, inheritance/

VM104 10.131.9.13:/srv/nfs/v3/
  data-mover-test/integrity-plan/posix-v3-104-20261028/

ONTAP 10.128.61.200:/ontap_lisaauto_nfs/
  data-mover-test/integrity-plan/linux3-to-ontap41-20261028/
    project/, mapped/
```

所有本轮临时客户端挂载已卸载。PVE 和 VM 检查未修改服务配置，凭据未写入上述证据。

## 11. 独立映射的逐条 ACL 与访问矩阵

本节逐字保留结构化结果中的 ACL 文本；不排序、不折叠 ACE。访问结果列依次为只读 open / 只写 open / 读写 open。

### 11.1 访问身份

| 名称 | UID | 主 GID | 客户端补充组 |
| --- | --- | --- | --- |
| 命名用户 | 42421 | 42420 | [] |
| 命名组成员 | 42425 | 42422 | [] |
| other | 42426 | 42420 | [] |
| 含补充组的主体 | 42425 | 42422 | [42424] |

所有上述文件样本的 X_OK 检查均为 false。该检查没有实际执行文件，也不代表 WRITE_ACL、WRITE_OWNER、DELETE 等额外权限已经验证。

### 11.2 named：命名用户/组只读

源 POSIX ACL：

```text
user::rw-
user:42421:r--
group::r--
group:42422:r--
mask::r--
other::---
```

Linux nfsd 的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A::GROUP@:rtcy
A:g:42422:rtcy
A::EVERYONE@:tcy
```

ONTAP 写入后原生读回：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A:g:GROUP@:rtcy
A:g:42422:rtcy
A::EVERYONE@:tcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 | 结果 |
| --- | --- | --- | --- |
| 命名用户 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 命名组成员 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| other | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 含补充组的主体 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |

### 11.3 masked：mask 截断 named rwx

源 POSIX ACL：

```text
user::rw-
user:42421:rwx	#effective:r--
group::---
group:42422:rwx	#effective:r--
mask::r--
other::---
```

Linux nfsd 的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A::GROUP@:tcy
A:g:42422:rtcy
A::EVERYONE@:tcy
```

ONTAP 写入后原生读回：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A:g:GROUP@:tcy
A:g:42422:rtcy
A::EVERYONE@:tcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 | 结果 |
| --- | --- | --- | --- |
| 命名用户 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 命名组成员 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| other | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 含补充组的主体 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |

### 11.4 precedence：命名主体无权限、other 可读

源 POSIX ACL：

```text
user::rw-
user:42421:---
group::---
group:42422:---
mask::r--
other::r--
```

Linux nfsd 的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
D::42421:r
A::42421:tcy
A::GROUP@:tcy
A:g:42422:tcy
D::GROUP@:r
D:g:42422:r
A::EVERYONE@:rtcy
```

ONTAP 写入后原生读回：

```text
A::OWNER@:rwatTcCy
D::42421:r
A::42421:tcy
A:g:GROUP@:tcy
A:g:42422:tcy
D:g:GROUP@:r
D:g:42422:r
A::EVERYONE@:rtcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 | 结果 |
| --- | --- | --- | --- |
| 命名用户 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 命名组成员 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| other | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 含补充组的主体 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |

### 11.5 overlapping：权限分散在多个组

源 POSIX ACL：

```text
user::rw-
group::---
group:42422:r--
group:42424:-w-
mask::rw-
other::---
```

Linux nfsd 的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::GROUP@:tcy
A:g:42422:rtcy
A:g:42424:watcy
A::EVERYONE@:tcy
```

ONTAP 写入后原生读回：

```text
A::OWNER@:rwatTcCy
A:g:GROUP@:tcy
A:g:42422:rtcy
A:g:42424:watcy
A::EVERYONE@:tcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 | 结果 |
| --- | --- | --- | --- |
| 命名用户 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 命名组成员 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| other | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 所测访问一致 |
| 含补充组的主体 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 允许 / 允许 | 有差异 |

本项远端差异含 `manage-gids=y` 的身份解析影响。底层 POSIX 组合请求的独立检查见 §8，二者不可混为同一个实验。

### 11.6 目录 access/default ACL 及 parent 映射

源 POSIX：

```text
user::rwx
group::r-x
other::r-x
default:user::rwx
default:user:42421:r-x
default:group::r-x
default:group:42422:r--
default:mask::r-x
default:other::r-x
```

Linux nfsd 投影：

```text
A::OWNER@:rwaDxtTcCy
A::GROUP@:rxtcy
A::EVERYONE@:rxtcy
A:fdi:OWNER@:rwaDxtTcCy
A:fdi:42421:rxtcy
A:fdi:GROUP@:rxtcy
A:fdig:42422:rtcy
D:fdig:42422:x
A:fdi:EVERYONE@:rxtcy
```

ONTAP parent 读回：

```text
A::OWNER@:rwaDxtTcCy
A:g:GROUP@:rxtcy
A::EVERYONE@:rxtcy
A:fdi:OWNER@:rwaDxtTcCy
A:fdi:42421:rxtcy
A:fdig:GROUP@:rxtcy
A:fdig:42422:rtcy
D:fdig:42422:x
A:fdi:EVERYONE@:rxtcy
```

#### 创建 mode 0o600 的子文件

源 POSIX：

```text
user::rw-
user:42421:r-x	#effective:---
group::r-x	#effective:---
group:42422:r--	#effective:---
mask::---
other::---
```

源子文件的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::42421:tcy
A::GROUP@:tcy
A:g:42422:tcy
A::EVERYONE@:tcy
```

目标子文件原生读回：

```text
A::OWNER@:rwatTnNcCoy
A::42421:rxtcy
A:g:GROUP@:tcy
A:g:42422:rtcy
D:g:42422:x
A::EVERYONE@:tcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 |
| --- | --- | --- |
| 命名用户 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |
| 命名组成员 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |
| other | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） |
| 含补充组的主体 | 拒绝（EACCES） / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |

#### 创建 mode 0o644 的子文件

源 POSIX：

```text
user::rw-
user:42421:r-x	#effective:r--
group::r-x	#effective:r--
group:42422:r--
mask::r--
other::r--
```

源子文件的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A::GROUP@:rtcy
A:g:42422:rtcy
A::EVERYONE@:rtcy
```

目标子文件原生读回：

```text
A::OWNER@:rwatTnNcCoy
A::42421:rxtcy
A:g:GROUP@:rtncy
A:g:42422:rtcy
D:g:42422:x
A::EVERYONE@:rtncy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 |
| --- | --- | --- |
| 命名用户 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |
| 命名组成员 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |
| other | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |
| 含补充组的主体 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 拒绝（EACCES） / 拒绝（EACCES） |

#### 创建 mode 0o666 的子文件

源 POSIX：

```text
user::rw-
user:42421:r-x	#effective:r--
group::r-x	#effective:r--
group:42422:r--
mask::r--
other::r--
```

源子文件的 NFSv4 投影：

```text
A::OWNER@:rwatTcCy
A::42421:rtcy
A::GROUP@:rtcy
A:g:42422:rtcy
A::EVERYONE@:rtcy
```

目标子文件原生读回：

```text
A::OWNER@:rwatTnNcCoy
A::42421:rxtcy
A:g:GROUP@:rwatTnNcy
A:g:42422:rtcy
D:g:42422:x
A::EVERYONE@:rwatTnNcy
```

| 身份 | 源：读 / 写 / 读写 | 目标：读 / 写 / 读写 |
| --- | --- | --- |
| 命名用户 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 允许 / 允许 |
| 命名组成员 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 允许 / 允许 |
| other | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 允许 / 允许 |
| 含补充组的主体 | 允许 / 拒绝（EACCES） / 拒绝（EACCES） | 允许 / 允许 / 允许 |

## 12. 独立映射的操作顺序

下列为本次操作的等价命令说明。`M3`、`M4`、`T4` 分别是源 v3、同一源导出的 v4.1、目标 ONTAP v4.1 客户端挂载点；变量不代表新增服务端导出。实际执行与清理过程见原始日志。

```bash
mount -t nfs -o vers=3,proto=tcp,mountproto=tcp,acl,nolock,nosharecache,soft,timeo=20,retrans=1,actimeo=0 \
  10.131.9.12:/srv/nfs/v3 "$M3"
mount -t nfs -o vers=4.1,proto=tcp,nosharecache,soft,timeo=20,retrans=1,actimeo=0 \
  10.131.9.12:/srv/nfs/v3 "$M4"
mount -t nfs -o vers=4.1,proto=tcp,nosharecache,soft,timeo=20,retrans=1,actimeo=0 \
  10.128.61.200:/ontap_lisaauto_nfs "$T4"
```

对每个普通文件：

1. 在 `M3` 下独立路径创建文件，用 `setfacl --set` 写入 §11 对应的 POSIX ACL。
2. 从 `M3` 读取内容复制到 `T4`，先复制 owner/group/mode 和文件时间。
3. 对同一个源文件的 `M4` 路径执行 `nfs4_getfacl`，保留 ACE 顺序，去掉显示用的注释与空行。
4. 将所得列表经 stdin 输入 `nfs4_setfacl -S -`，设置到目标文件；之后不执行 chmod。
5. 分别 `getfacl`、`nfs4_getfacl` 保存源和目标读回；用无 capability 的 `setpriv` 进程执行三种 open。
6. 卸载、重新挂载，重做三个文件正例及 0600/0666 继承反例的访问检查。

目录实验只映射 parent ACL。随后分别以 `os.open(O_CREAT|O_EXCL, mode)` 在两端创建子文件，umask=0；不补设子 ACL。这保证观察到的是各服务器当前配置的真实继承结果。

普通文件数据没有通过访问探针改写；`O_WRONLY` 和 `O_RDWR` 只用于验证能否打开。完整 ACL 还涉及未覆盖的操作，因此本报告不将有限访问矩阵称为全量等价证明。
