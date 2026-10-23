# ONTAP：SMB 安全信息可观察性（有界侧记）

2026-10-23。范围仅 SMB wire、固定 SDK、既有真机证据；CLI/REST 综合由主协调者补充。
侧研究员做了下文的一次只读 SMB 查询，主协调者做了只读 REST 交叉验证；
无远端写入、生产修改、构建或新夹具。既有凭据仅在内存/管道加载，不打印、不存入文档。

## 结论

**可从 ONTAP 的合成 ACL 解析出已知 UNIX 身份表示，但还不能据此直接签发通用权限证明。**
最小可行方向是 SDK 增加显式 owner/group 查询和不丢 ACE 的安全观察 interface，
再由 backend-local 证据门槛决定是否接受 UNIX 路径；不是删除 protected 检查。
没有找到可直接使用的 ONTAP `FSCTL_SECURITY_STYLE`；这属于本次未找到，不是证明其不存在。

## 1. QUERY_INFO：wire 有能力，固定 SDK 尚未暴露完整观察

SMB2 QUERY_INFO 的 `InfoType=Security(3)`、`FileInfoClass=0`，
`AdditionalInformation=0x7` 可同时请求 OWNER(1)、GROUP(2)、DACL(4)。
这些返回的是安全描述符字段，不附带 UNIX/NTFS effective-style 标签。
来源：[Microsoft MS-SMB2 §2.2.37](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d623b2f7-a5cd-4639-8cc9-71fa7d9f9ba9)。

固定依赖为 `2649a0bbd615c23d64faca0053e969288694e034`，本轮直接读其 Cargo checkout：

| 层 | 已有能力 | 缺口 |
|---|---|---|
| wire `AdditionalInfo` | owner/group/DACL flags 都有 | 不是服务器 capability 探测结果 |
| `SecurityDescriptor` | 可解码 `owner_sid`、`group_sid`、DACL | 有字段不代表查询已请求它们 |
| domain `SecuritySelection` | 只有 `dacl: bool` | owner/group 无请求入口 |
| domain 查询/设置 | 自动过滤 trustee | 不是 lossless security snapshot |
| `ResourceMetadata` | file/volume ID、时间、长度、属性 | 无 UNIX uid/gid/mode、ACL 类型 |

源码：[flags](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb-msg/src/info/common.rs#L16)、
[descriptor](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb-dtyp/src/security/security_descriptor.rs#L39)、
[selection/filter](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb/src/domain/security.rs#L28)、
[query flags](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb/src/runtime/port.rs#L1110)。

data-mover 的 [get_acl](../../src/storage/backends/cifs/protocol.rs) 走该 domain interface，
仅请求 DACL。因此不能把今天的 `get_acl()` 改名当作完整 owner/group/权限证明。

## 2. 合成 SID：已有官方映射，不必猜 UID/GID

NetApp 官方 KB 给出如下映射；这里使用完整前缀，而不是将任意 account RID 当 UNIX ID：

| SID | 官方含义 | 旧样本解析 |
|---|---|---|
| `S-1-5-21-2038298172-1297133386-11111-<uid>` | UNIXPermUid | 尾部 `2` → UID 2 |
| `S-1-5-21-2038298172-1297133386-22222-<gid>` | UNIXPermGid | 尾部 `0` → GID 0 |
| `S-1-5-21-2038298172-1297133386-33333` | UNIXPerm other | 无用户/组 ID 后缀 |

KB 明确：额外的当前访问用户 ACE 反映其有效权限，不是文件实际应用的权限。
部分常见 access mask 可翻译成 rwx，其他特殊权限不能用 mode bits 完整表达。
来源：[NetApp 官方 KB：非原生协议的权限表示](https://kb.netapp.com/on-prem/ontap/da/NAS/NAS-KBs/How_does_ONTAP_generate_permissions_for_NFS_and_CIFS_clients_when_the_volume_security_style_is_not_native_to_the_protocol)。

**已证实的 SDK 缺口**：other SID 的 subauthority 数量为 4；固定 SDK 的 account-domain
允许规则要求至少 5，且它不匹配其他允许分支，所以会被丢弃。uid/gid 两条则保留。
SDK 同样可能过滤非 account trustee 的 deny；安全判断不能沿用元数据复制的有损视图。
上述数量判断来自固定源码与已保存的 `capability-probe.jsonl`，不是新的远端实验。

## 3. FSCTL、文件系统名称和 POSIX 扩展

- 本次官方资料检索没有找到直接返回 per-file ONTAP security style 的标准/厂商 FSCTL。
  固定 SDK 的 [FsctlCodes](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb-msg/src/ioctl/fsctl.rs#L20)
  也没有该类型；不能据此捏造请求码或断言 ONTAP 没有其他私有 interface。
- `FileFsAttributeInformation` / 报告为 NTFS 不是充分证据：ONTAP 可以把 UNIX effective
  security 呈现为 NTFS ACL；另一种呈现则为 FAT。卷级名称不等于当前文件实际权限模型。
- 官方存在 UNIX extensions 功能，默认关闭；该描述未给出足以接入本项目的 wire
  dialect/查询结构。不能把它自动等同现代 SMB3 POSIX extensions，更不能因协商 3.1.1
  就假定能查询 UNIX metadata。固定 domain/wire 源码检索未找到可用的 POSIX uid/gid/mode
  查询链路。本轮没有查询或更改设备选项。
  来源：[NetApp SMB server options](https://docs.netapp.com/us-en/ontap/smb-admin/server-options-reference.html)。

## 4. 已知与未证明的界线

已知：旧样本可解释为 UID 2/GID 0 的 UNIX 合成表示；原始读回能看到 other；
显式保护设置后 DACL 字节未变而 protected=false。详见
[既有 CIFS 验证报告](checkpointed-range-hash-cifs-validation.md)，远端夹具当时已删除。

未证明：其他文件/身份的 owner/group 是否同样可解释、任意文件是否真正使用 mode bits、
是否另有 NFSv4 ACL、同一 share 内所有路径是否同模型、所有身份映射与权限类是否稳定。
`owner=2/group=0` 是此样本的数值，不是通用规则；mode 0600 也不能单独证明 NFSv4 ACL
环境的完整有效权限。标准 SD 没有提供上述缺项的自动证书。

## 5. 最小后续工作建议（本轮未实施）

1. SDK 增加 opt-in、未过滤的 owner+group+DACL snapshot；保留旧 metadata interface 行为。
2. CIFS 内部解析严格匹配的 ONTAP synthetic SID，区分 owner/group/other 与会话有效 ACE；
   遇到缺项、重复、未知 mask/deny/模型歧义就返回 Unknown，不静默折算权限。
3. 将该观察作为 UNIX proof 的输入，不是 proof 本身；每文件 actual style、底层 ACL 模型
   与跨协议约束由主协调者的 CLI/REST 研究补足证据门槛。
4. 后续获批测试应对比不同只读身份查询、owner/group 字段、other 的无损保留，并覆盖
   mixed/junction/NFSv4 ACL 和来源不完整的拒绝；本轮不创建测试对象，也不实施权限变更。

## 6. 收尾：已授权的单次只读 wire 观察

现有 `ontap_lisaauto_cifs/dm-ro-share/a.txt`（38 B），共享锁均以 nonblocking 方式取得；
仅 READ_CONTROL 打开，发送 QUERY_INFO Security `0x7`，不读 payload、不发 SET_INFO。
新会话协商 SMB 3.1.1 且要求签名；查询返回：

- owner_sid：`S-1-5-21-2038298172-1297133386-11111-2`。
- group_sid：`S-1-5-21-2038298172-1297133386-22222-0`。
- DACL 原始四条：上述 owner、group、固定 other、当前访问用户；mask 全为 `0x1f01ff`。
- protected=false；DACL BLAKE3 `2e31d5b2ceb4b04f90ca1aaa2bbad32fb7048a5eb6bce4a929aca91cf99639ae`。

与主协调者报告的同路径 REST `effective_style=unix, uid=2, gid=0, mode=777` 一致；
REST 未在本侧重复查询。此处证明本样本的身份/权限表示可观察，不证明完整有效授权，
也不证明 NFSv4 ACL 缺失、任意路径模型相同，或 filtered-DACL 相等即可安全放行。

双向过滤已核对：[Resource::query_security 第93行](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb/src/domain/security.rs#L93)
和 [Resource::set_security 第108行](https://github.com/JayTsu-sh/smb-rs/blob/2649a0bbd615c23d64faca0053e969288694e034/crates/smb/src/domain/security.rs#L108)
均调用 `retain_supported_aces`。因此“查询和设置都过滤”不是推测。

补充范围判断：[SHARE_INFO_502](https://learn.microsoft.com/en-us/windows/win32/api/lmshare/ns-lmshare-share_info_502)
的 descriptor 属于 share，不是 per-file effective style；SRVSVC 502 不能填补该缺口。
现代 SMB3 POSIX GUID/FilePosixInformation 的 **ONTAP 实际支持仍未验证**，不据此修改 SDK。
若服务器证据成立，可按用户授权补 SDK；本轮未实施。

## 7. 主协调者：管理接口的官方能力与真机交叉验证

这部分不能冒充普通 SMB 连接的 capability。它提供独立的实际权限参照。

官方 REST（权限查询自 ONTAP 9.9 引入）：

```text
GET /api/protocols/file-security/permissions/{svm.uuid}/{encoded-svm-path}?fields=*
```

返回 `security_style`、`effective_style`、`user_id`、`group_id`、`mode_bits`、
`text_mode_bits` 及适用的 ACL 信息。`lookup_names=false` 自 9.17 引入，不能直接用于
所有支持该 endpoint 的旧版本。不要把返回的数字 `775` 当作十进制 POSIX bit mask；
本次同时记录文本权限用于核对。
来源：[ONTAP REST 权限查询](https://docs.netapp.com/us-en/ontap-restapi/get-protocols-file-security-permissions-.html)。

CLI 对应：

```text
vserver security file-directory show -vserver lizy -path /ontap_lisaauto_cifs/dm-ro-share/a.txt
```

该命令支持查看 effective-style、UNIX 身份和 mode；UNIX 文件可能由 mode 或 NFSv4 ACL
控制，不能将缺少 native NTFS protected flag 本身解释为没有权限保护。
来源：[ONTAP CLI](https://docs.netapp.com/us-en/ontap-cli/vserver-security-file-directory-show.html)、
[UNIX 文件安全说明](https://docs.netapp.com/us-en/ontap/smb-admin/display-file-security-unix-style-volumes-task.html)。

2026-10-23 对现有 ONTAP 9.19.1 管理地址执行只读 GET，没有新建或修改文件：

| 对象（SVM lizy） | inode | security_style / effective_style | UID / GID | mode / 文本 |
| --- | --- | --- | --- | --- |
| `/ontap_lisaauto_cifs` | 64 | unix / unix | 0 / 0 | 775 / rwxrwxr-x |
| `/ontap_lisaauto_cifs/dm-ro-share/a.txt`（常驻只读夹具） | 2168 | unix / unix | 2 / 0 | 777 / rwxrwxrwx |

两次 `fields=*` 返回均没有 `acls` 字段；这与 mode-only 文件的预期一致，但不推广成
所有 UNIX-style 文件都没有 NFSv4 ACL。share 查询确认 namespace path 为
`/ontap_lisaauto_cifs`。SVM UUID 为 `ad1b23af-e0f1-11f0-af40-d039ea10a68f`。

另一个官方只读查询可计算指定用户的有效权限，自 ONTAP 9.8 引入：

```text
GET /api/protocols/file-security/effective-permissions/{svm.uuid}/{path}?user=...&type=windows&share.name=...
```

主协调者查询 `user=lisauser,type=windows,share.name=dm_ro_share,path=/a.txt`，得到
文件层多项许可、share 层 `none`。未限定域的 Windows 名字可能不能代表实际登录
身份，且 share 与文件权限是两层不同结果；本次不将该结果解释为已证明真实 SMB
session 的最终可读/不可读。生产 verifier 也不能仅凭一个身份的正向访问结果
证明其他所有主体都无法读取。
来源：[REST 有效权限](https://docs.netapp.com/us-en/ontap-restapi/get-protocols-file-security-effective-permissions-.html)、
[CLI 有效权限与身份/路径约定](https://docs.netapp.com/us-en/ontap-cli/vserver-security-file-directory-show-effective-permissions.html)。

综合：服务端确实能提供有意义的 UNIX 权限投影，SDK 的有损过滤及 owner/group
selection 是可修的客户端缺口。优先补齐 SMB 无损观察，而非把管理凭据加入普通
transfer；但不能把“能解析投影”扩大为“完整 native NFSv4 ACL 与所有有效权限已获证明”。
