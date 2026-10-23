# ONTAP UNIX security style 下的 SMB share ACL

2026-10-23；官方文档研究及当前 ONTAP 的只读交叉验证。侧研究员负责规范来源，
主协调者执行 §6、§7 的 REST/SMB 观察；没有创建夹具、远端写入或代码/配置修改。
凭据从批准的仓库外文件在内存/管道传递，未打印或记录。

## 1. 先区分三个不同对象

| 对象 | 管什么 | UNIX security style 下的含义 |
|---|---|---|
| SMB share-level ACL | 谁能经这个 share 做哪些操作 | 仍然存在，独立于 volume/qtree security style |
| 文件/目录原生权限 | 具体对象允许哪些访问 | UNIX mode bits **或 NFSv4 ACL** |
| SMB 查询呈现的文件 DACL | 向 SMB 客户端表示文件安全信息 | 可以是 UNIX 权限的构造/合成表示，不等于存储了 NTFS ACL |

NetApp 明确说明 share ACL 不依赖底层 security style；它只限制访问，不能超越文件级
权限授予额外权限。因此“UNIX style 没有 share ACL”与“看到文件 DACL 就是 NTFS
effective style”都不成立。
来源：[Manage share-level ACL](https://docs.netapp.com/us-en/ontap-restapi-991/manage_share-level_acl.html)、
[UNIX-style 文件安全信息](https://docs.netapp.com/us-en/ontap/smb-admin/display-file-security-unix-style-volumes-task.html)。

## 2. Share ACL 实际长什么样

官方 schema 中，每条 share ACE 包含 principal、类型和 share 权限：

- `type` 可为 `windows`、`unix_user`、`unix_group`。
- `user_or_group` 是名称；`sid` 表示 SID，`unix_id` 表示相应 UNIX UID/GID。
- `permission` 可为 `no_access`、`read`、`change`、`full_control`。

这些是 **share 层**的数据，不是文件 owner/group/other 的 rwx 三元组，也不是每文件的
NFSv4 ACE 列表。不能把 share 的 `full_control` 当作文件 mode 777 或 WRITE_DAC 保证。
这里只阅读 schema；没有调用其写入接口。
来源：[NetApp share ACL schema](https://docs.netapp.com/us-en/ontap-restapi/post-protocols-cifs-shares--acls.html)。

## 3. 文件“合成 ACL”又是什么

开启 UNIX 权限的 NT ACL 呈现时，SMB 查询可以看到以下占位 SID：

| SID 模式 | 含义 |
|---|---|
| `S-1-5-21-2038298172-1297133386-11111-<uid>` | UNIX owner UID |
| `S-1-5-21-2038298172-1297133386-22222-<gid>` | UNIX group GID |
| `S-1-5-21-2038298172-1297133386-33333` | UNIX other |
| 当前访问用户的额外 ACE | 该用户的有效权限表示，**不是文件实际应用的一条权限** |

这几类条目是文件查询的表示，不是 share ACL 的替代品。部分常见 access mask 能与 rwx
对应；特殊 Windows 权限并不能完整映射到 mode bits，显示出的 Full Control 也不能逐项
推导全部特殊操作能力。
来源：[NetApp 官方 KB：非原生协议的权限转换与 SID 表](https://kb.netapp.com/on-prem/ontap/da/NAS/NAS-KBs/How_does_ONTAP_generate_permissions_for_NFS_and_CIFS_clients_when_the_volume_security_style_is_not_native_to_the_protocol)。

官方还解释：应用将原文件安全属性复制到临时文件以保留 UNIX 权限；ONTAP 不会因此
把该构造 ACL 存成 NTFS ACL。该保证不能被换成“任意过滤/删改后的 ACL 仍等权”。
来源：[Preserving UNIX permissions](https://docs.netapp.com/us-en/ontap/smb-admin/how-ontap-preserves-unix-permissions-concept.html)。

## 4. 实际访问判定：share 允许只是必要条件

对普通 SMB 访问，NetApp 给出的检查顺序是：

1. SMB share 权限；
2. 若配置了 Storage-Level Access Guard（SLAG），检查该层；
3. 文件/目录原生权限：NTFS ACL、NFSv4 ACL 或 UNIX mode bits。

在不考虑特殊绕行权限的普通授权模型中，可将 share 与 file 的效果概括为请求操作
必须同时获准；存在 SLAG 时还必须通过 SLAG。这里的“交集”是授权条件，不是直接将
不同层的数字 mask 做位运算。SLAG 可用于 UNIX-style 卷，并且普通 SMB/NFS 文件安全
查询看不到它。
来源：[NetApp SLAG 与访问检查顺序](https://docs.netapp.com/us-en/ontap/smb-admin/secure-file-access-storage-level-access-guard-concept.html)。

**示例，不是真机字段**：share=read、文件 mode=777，不能仅据 mode 获得经 share 写入；
share=full_control、文件拒绝某次读请求，也不能由 share 越权授予该次文件访问。
经另一 share/NFS export 的访问须按其入口和映射重新判断，不能用一个只读 share 证明
文件对所有客户端都只读。Windows/UNIX 名称映射及组身份也参与 UNIX/mixed 数据检查。
来源：[Share ACL 限制原则](https://docs.netapp.com/us-en/ontap-restapi-991/manage_share-level_acl.html)、
[NetApp 关于映射用户/组参与访问检查的说明](https://docs.netapp.com/us-en/ontap/nas-audit/create-security-trace-filters-task.html)。

## 5. NFSv4 ACL 与正向证明的限制

官方明确 UNIX-style 对象可使用 mode bits 或 NFSv4 ACL；其管理面文件安全输出在只有
mode bits 时 ACL 字段为空，有 NFSv4 安全信息时才显示 ACL。
来源：[UNIX-style 文件安全信息](https://docs.netapp.com/us-en/ontap/smb-admin/display-file-security-unix-style-volumes-task.html)。

由此应保守推导：仅知道 `effective_style=unix`、UID/GID 和 mode，或只见 SMB 的三类
合成条目，不能自动认定已获取完整 NFSv4 ACL。官方上述合成 ACL 说明并未建立任意
NFSv4 named ACE、deny 与继承信息的无损投影证明；也不能把 mode 0600 当通用充分条件。
同样，单个当前用户的有效访问结果，不代表全部用户的可访问集合。

对 checkpoint 保密性的结论是：应分别记录 share 入口限制、原生文件权限模型和完整
观察依据，再证明 pointer 不比 payload 扩权。不能把“share ACL 看起来足够严格”或
“合成 DACL mask 相同”单独当作完整正向授权证明。已有真机字段见 §6；SLAG 状态未获提供。

## 6. 当前真机结果（主协调者提供，非本侧新增查询）

主协调者以已批准凭据完成只读 REST 查询，凭据通过 stdin pipe 载入且未打印；本节不含凭据。

- `ontap_lisaauto_cifs`：path=`/ontap_lisaauto_cifs`；ACE 为 Everyone/windows/change（`S-1-1-0`）、lisauser/windows/full_control（`S-1-5-21-3753256468-586717423-4274650760-1008`）、xuyh/windows/full_control（同 SID 前缀、RID `1017`）。
- `dm_ro_share`：path=`/ontap_lisaauto_cifs/dm-ro-share`；唯一 ACE 为 lisauser/windows/read。
- 同一路径 `dm-ro-share/a.txt`：inode=`2168`，`effective_style=unix`、`security_style=unix`，uid=`2`、gid=`0`、mode=`777`（`rwxrwxrwx`）；REST 无 `acls` 字段。
- 既有 SMB 同文件观察：owner 为上表 UID 2 SID、group 为 GID 0 SID；DACL 四条为 owner/group/other/lisauser，mask 全为 `0x1f01ff`，protected=false。

因此 share ACL 不是 UNIX mode；文件 777 不能绕过只读 share，share full_control 也不能绕过原生文件检查。模式与合成 ACL 的一致性仍不是完整权限证明。
此前 checkpoint 的失败点是文件 DACL protected 位回读不满足契约；不是 share 权限拒绝，不能通过放宽 share ACL 修复。上述查询没有写入文件或配置。

## 7. 观察方法及有效权限字段的交叉验证

Share 配置通过 `GET /api/protocols/cifs/shares?svm.name=lizy&name=...&fields=name,path,acls,svm`
查询；文件通过 `GET /api/protocols/file-security/permissions/{svm.uuid}/{encoded-path}?lookup_names=false&fields=*`
查询。CIFS service 返回服务器名 `LIZYAD`。这些是当前配置，不是默认值；本轮没有查询 SLAG。

主协调者另取得两个实验室公共锁，以要求签名的 SMB 3.1.1 会话，经 `dm_ro_share/a.txt`
成功读取 1 byte；仅记录读取长度，未输出内容、未发写请求。句柄、会话和锁随后释放。
因此真实会话确实能经只读 share 读取；本轮没有发送写请求来重测写拒绝。

REST effective-permissions 使用 `LIZYAD\\lisauser` 查询时，主 share 返回完整 share
permissions，但 `dm_ro_share` 返回 `share_permissions=["none"]`；后者与实际可读
结果不符，不能据该字段断言当前用户不可读。原因本轮未诊断，不把它归因为 ACL
配置错误。访问限制的说明依据实际 share ACE、官方权限语义和 SMB 观察，未用该
矛盾字段替代实际会话行为。

最后，当前样本的 UID/GID/mode 与四条合成 ACE 不能推广为整个 UNIX-style share
所有文件都具有相同权限。合成 DACL 的 protected=false 也不表示 share ACL 未生效。
