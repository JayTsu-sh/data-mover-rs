# Local 文件身份与跨扫描改名识别调研

调研日期：2026-10-26。范围：MR !56 的 `ino || btime` 在当前角色 API 架构中的适用性；仅设计建议，未修改实现。MR 证据来自本机已下载的最终差异，未重新调用 GitLab。

后续实施：本调研保留方案阶段的分析；实际实现和挂载验证结果见
[验证报告](../reports/2026-10-26-local-rename-evidence.md)。本期只实现 `Heuristic`，未实现持久句柄 provider。

## 推荐结论

**保留现有 `SourceIdentity(dev, ino)` 的运行期检查和恢复绑定；另设有明确质量、命名空间和版本的 `RenameEvidence`，其中 `ino + btime` 是启发式候选。** 不能把 MR 的 16 字节直接替换当前 `SourceIdentity`，也不能把创建时间宣传成 inode 世代号。

本结论是依据下述事实和当前代码约束作出的设计判断。首期只实施使用现有 metadata 的轻量候选；`Persistent` 是预留扩展方向，有可靠持久句柄的提供者可以后续接入，不在本期实现，不把额外系统调用加到所有文件的默认扫描路径。

## 已核实的事实

| 问题 | 一手资料支持的结论 | 对方案的影响 |
| --- | --- | --- |
| CIFS inode 跨重挂是否稳定 | 内核文档仅称 `serverino` 的 inode *may be persistent*；一个 share 若覆盖多个服务端文件系统，inode 不保证全局唯一；`noserverino` 的客户端 inode 会随卸载或重启变化。[CIFS 使用文档](https://docs.kernel.org/admin-guide/cifs/usage.html) | 不能检测到 CIFS 或 `serverino` 就自动保证身份稳定；多服务端文件系统必须拆分可信域，无法拆分则不给自动改名依据。 |
| btime 是不是世代号 | SMB `FileBasicInformation` 明确同时用于查询和设置，包含 `CreationTime`。[MS-FSCC](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/16023025-8a78-492f-8b96-c873b042ac50) | 创建时间可修改，变化会造成漏识别；复用或人为保留会削弱区别新旧对象的能力。 |
| 新文件是否必有新创建时间 | Microsoft 文档说明 NTFS 有 file tunneling，替换旧文件时可能保留旧创建时间。[CreationTime 官方文档](https://learn.microsoft.com/en-us/dotnet/api/system.io.filesysteminfo.creationtime?view=net-9.0) | MR 中“新文件必有新的 btime”的普适断言不成立。该事实本身不能证明完整 `ino + btime` 必然冲突：部分 NTFS 文件 ID 还包含序列信息。 |
| 缺失 btime 能否用 0 补 | `statx` 的请求位和返回位不同；字段不支持时返回 mask 会清位，字段可能填 dummy 值。[statx 手册](https://www.man7.org/linux/man-pages/man2/statx.2.html) | 缺失必须保留为缺失。不能变成 0 后仍宣告与正常候选同等可靠。合法 epoch 时间与缺失也应区分。 |
| 持久句柄有什么更强语义 | Linux exportable filesystem 的句柄机制旨在跨 rename、truncate、服务器重启稳定；默认编码包含 inode 和 generation。[内核导出机制](https://docs.kernel.org/filesystems/nfs/exporting.html) | 在具体文件系统确认支持世代区分时，持久句柄优于 btime。应保留 opaque bytes，不自行解析成自造统一 inode 格式。 |
| `name_to_handle_at` 能否普适替代 | 手册允许其结果用来比较不同时间、路径的对象，但某些文件系统不支持；mount ID 不能当持久文件系统 ID，句柄也可能因文件系统特定原因 stale。获取句柄和按句柄打开是不同操作，`CAP_DAC_READ_SEARCH` 要求属于后者。[接口手册](https://www.man7.org/linux/man-pages/man2/open_by_handle_at.2.html) | 可选能力，不能作为默认无条件方案；不能因为不需打开就要求额外权限。句柄需配稳定文件系统域。 |
| CIFS 返回句柄就一定带世代吗 | Linux v6.12 CIFS export 代码明确 generation 为 0，并使用 `generic_encode_ino32_fh`，支持还受 `CONFIG_CIFS_NFSD_EXPORT` 限制。[内核 CIFS export 源码](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/smb/client/export.c) | `name_to_handle_at` 调用成功本身不能升级为可信持久身份，尤其不能泛化到 CIFS。 |
| NFS 本地挂载的句柄 | Linux v6.12 NFS export 编码嵌入服务端 handle。[内核 NFS export 源码](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/nfs/export.c)；NFSv4.1 明确区分 persistent 与 volatile handle，后者可因 rename 或迁移过期。[RFC 8881 §4.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-4.2) | NFS provider 需验证句柄类别和服务器语义，不把“网络句柄”统一视为永久标识。 |
| `f_fsid` 能否直接替换 `dev` | 手册说明不同系统对 `f_fsid` 的解释不同，可能使用设备号变体，部分系统会对非特权用户返回 0。[statfs 手册](https://www.man7.org/linux/man-pages/man2/statfs.2.html) | 没有足够依据把任意 `f_fsid` 当跨重挂 UUID。块设备文件系统可按具体实现验证 UUID；网络共享需要协议或配置的可信域。 |

## 当前代码的约束

`src/storage/backends/local/observation.rs::source_identity` 把 `dev + ino` 小端编码到临时 `Vec`，之后 `SourceIdentity::new` 又复制到 46 字节内联区。`src/model/observation.rs` 明确把 `StableWithinBackend` 的 `stable_bytes()` 暴露为跨扫描 join key。因此当前 API 的“运行期对象检查”和“持久改名证据”已有语义混用，不能用改一个 helper 的方式扩散。

主代理核实该身份还用于 `ReadRequest.expected_source`、打开后 metadata 对比、`observe_bound`、Direct 源/目标硬链接别名保护以及恢复绑定。删除 `dev` 或按 btime 是否可用将身份降为路径，会改变这些行为。应给新用途独立入口，并明确新代码不再将 Local 的旧 `stable_bytes()` 当成跨重挂改名承诺。

本项目锁定的 `cap-primitives 4.0.3` 本地源码已核实：`src/rustix/fs/stat_unchecked.rs` 在 Linux 优先请求 `BASIC_STATS | BTIME`，在不支持的环境回退；`metadata_ext.rs::from_rustix_statx` 仅在返回 mask 带 `BTIME` 时提供 `created`。因而已有 `Metadata::created()` 可复用，无需为轻量候选再调用 `statx`。这些是锁定版本源码事实，而非对所有平台/版本的承诺。[上游源码入口](https://github.com/bytecodealliance/cap-std/tree/main/cap-primitives/src/rustix/fs)

## 建议接口与匹配规则

1. 运行期继续使用 `SourceIdentity(dev, ino)`；明确它也不等于永久对象 UUID，跨断开连接和重新挂载可能失效。不要因改名需求改变既有传输恢复格式。
2. 观察结果新增可选的 `RenameEvidence`：方案版本、`Heuristic`/`Persistent` 质量、固定长度 token 或 opaque handle。命名空间以扫描/后端为单位保存，避免每条重复存字符串；候选只能在同一可信数据集域、同一方案版本下比较。
3. 轻量 Local 候选取 `ino_be64 || btime_ns_i64_be`，共 16 字节。btime 必须真实存在，并 checked 转成距 Unix 纪元的有符号 64 位纳秒；正常时间范围与 MR 编码一致，epoch 前按负数准确编码。缺失、溢出、转换错误时返回 `None`，禁止饱和或默认 0；真实的 0 是有效时间。方案名称和版本明确包含有符号时间编码语义。
4. 默认安全策略：启发式只产生候选，不作为自动破坏性改名、删除、跳过内容校验的唯一依据。生产部署若明确接受其风险，可在经过验证的单一 CIFS export、稳定 serverino、可靠创建时间和可信域约束下显式启用启发式改名；配置应记录在扫描指纹中。
5. 强证据只来自经验证的文件系统/协议 provider。成功拿到 opaque handle 不是充分条件，还应确认句柄跨 rename/重挂的语义及世代区分，且具备稳定域。找不到强证据的环境按路径同步，保持可用，只损失改名优化。
6. 候选匹配必须一对一且同类型；同一候选对应多个路径（硬链接、冲突）时保守回退，不能按遍历顺序随意配对。改名后内容或元数据可能已变，改名识别不能替代内容版本判断。内容相同只能证明数据可复用，不能证明同一个对象。
7. 数据集域不能仅用挂载路径：路径可重新挂到另一份数据。可用经验证的文件系统 UUID/服务端共享身份，或显式配置的数据集标识与更换介质时轮换的 epoch。自动识别不到就关闭跨重挂匹配；不要用 mount ID 或任意 `f_fsid` 冒充持久域。

以上是设计推论，不是官方协议的额外保证。保持 `SourceIdentity` 可以避免主动更改 transfer recovery binding 本身；若把 `RenameEvidence` 保存进快照，必须遵循当前 `observation_codec.rs` 只接受当前版本的策略，升级快照版本并拒绝旧版本，重新扫描和重建扫描基线，不增加旧版本兼容链。下游应迁移到新入口，旧 Local rename join 用法不再推荐。

## 内存与性能要求

- 16 字节候选直接保存在固定数组或模型内联区；提供内部固定长度/直接填充构造入口和借用 accessor，避免 `Vec`、`concat`、`Bytes::copy_from_slice` 以及拼接字符串。
- 准确目标是零临时堆分配、无整段中转复制；整数编码写入、模型移动和哈希读取并不是可承诺消除的全部机器级复制。不要为消除 16 字节 memcpy 增加 unsafe 或复杂共享所有权。
- 命名空间、能力和策略按后端或文件系统缓存；每文件复用已有 metadata。强 handle provider 作为可选增强，其额外系统调用和可能的网络取元数据开销应单独基准测试。
- 若后端私有 fact 已携带同一 metadata 信息，优先在已有 fact/观察构造路径直接编码，按接口实际所有权选择借用或移动；避免把整个 fact 克隆为 `Bytes` 再解析。

## 备选方案对比

| 方案 | 正确性与成本 | 判断 |
| --- | --- | --- |
| 全局将 SourceIdentity 改为 ino+btime | 触及运行期检查、别名保护和恢复；btime 仍不权威 | 不推荐 |
| 全面改用 name_to_handle_at | 支持性和持久性依文件系统；CIFS 未解决世代；逐文件额外调用 | 后续专用 provider，首期不选 |
| 保留运行期身份，独立启发式 RenameEvidence | 候选可复用已有 metadata；不干扰恢复；明确策略边界 | 推荐 |

## 验证矩阵

| 场景 | 预期行为 |
| --- | --- |
| 普通 rename、跨目录 rename | 可信域与方案一致时得到同一候选；仅启用对应策略的消费者使用 |
| rename 后修改内容 | 仍可识别候选对应，内容独立标记 Changed，不能跳过更新 |
| 硬链接多路径、候选冲突 | 无唯一配对时按路径回退，不按遍历顺序猜测 |
| inode 复用 | 不同 btime 区分；不能以此声称已覆盖所有服务器的复用情形 |
| btime 缺失、合法 0、负时间、越界 | 分别为 None、有效候选、正确有符号编码、None |
| 手动修改 SMB 创建时间、NTFS tunneling | 证明启发式局限；保守策略仍正确同步 |
| serverino/noserverino、同 share 多文件系统 | 前者需部署验证；后两者不自动开启跨重挂匹配 |
| remount、同路径换数据集 | 前者仅经确认同可信域继续匹配；后者域或 epoch 必须改变 |
| 旧快照、未知方案 | 旧快照明确拒绝并重扫、重建基线；当前格式中未知候选方案不参与匹配；不主动更改 transfer recovery identity hash |
| 源替换、Direct 硬链接别名 | 原有运行期检查结果不变 |
