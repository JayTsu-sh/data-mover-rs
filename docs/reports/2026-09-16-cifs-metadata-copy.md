# CIFS 自动 mtime 复制

## 行为

普通 `transfer()` 在 CIFS 为源或目标时接入 baseline metadata planning：自动复制 mtime，不复制访问时间、创建时间或 ChangeTime。Checkpointed 与 AtomicReplace 均支持，不依赖读回校验是否开启。源 mtime 与同次 metadata 查询得到的 size/written/changed 版本观察绑定；版本不一致时返回 Conflict。

跨 Local/NFS 到 CIFS 时，数字 uid/gid/mode 不映射为 Windows SID，报告未保留的所有权语义。CIFS 作为源时明确标记数字所有权 NotApplicable，不伪造 uid/gid，也不把未支持的数字所有权误判为漏读。现有 DACL 保持显式观察与应用，不自动复制。

时间使用 SMB FILETIME 的 100ns 精度；更细的源时间由 planner 向下对齐，并记录 TimestampPrecisionReduced。Snapshot codec 为新增精度使用独立标记 4，保留已有 Nanoseconds 标记 3。

## 顺序和权限

1. 完成 stage 写入和写句柄关闭。
2. 完成可选的读回校验。
3. 打开只需要 READ_ATTRIBUTES/WRITE_ATTRIBUTES 的句柄，设置 mtime，关闭。
4. Checkpointed 重新打开 stage 执行 FLUSH 并关闭；AtomicReplace 不增加主动 FLUSH。
5. rename 发布。元数据设置/同步失败时不进入发布。

目录可显式调用 `Metadata::apply(Timestamps)`，没有目录 stage，也不增加递归目录完成调度。属性设置拒绝最终 reparse point，避免误改软链接指向的对象；本轮不宣称支持软链接元数据复制。父路径仍遵循 SMB 服务端的路径解析语义。

底层通用 API 支持可选 accessed/created，但 data-mover 自动复制只传 modified。未指定时间以协议零值表示保持不变，文件属性同样不变。SMB FLUSH 不等价于 POSIX 目录 fsync。

## 依赖

JayTsu-sh/smb-rs `feat/metadata-timestamps`，固定提交 `89cba6984142409b1705692b7781a8f9aa3049bc`。新增公共 `MetadataUpdate`、`MetadataOpenOptions`、`Share::open_metadata`、`Resource/File/Directory::set_metadata`。data-mover 不导入 wire/runtime 类型。

## 协议依据及其他元数据范围

- [MS-FSCC FileBasicInformation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/16023025-8a78-492f-8b96-c873b042ac50)：时间与 DOS 属性可设置；零时间表示不修改对应字段。
- [MS-SMB2 security SET_INFO](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/7d7f9b83-e1a7-4467-a289-69882d57b0e8)：owner/group SID、DACL、SACL 需要对应权限，SACL 需要 ACCESS_SYSTEM_SECURITY。
- DOS 属性、owner/group SID、SACL、EA、ADS 尚未接入本轮自动复制。压缩/稀疏/加密也不等同于简单复制属性位。

## 验证

- `cargo test --lib`：774 passed，4 ignored。
- `cargo clippy --all-targets --all-features -- -D warnings`、`cargo fmt --all --check`、架构 dependency guard、`git diff --check` 均通过。
- FAS2750：源 LIF 10.128.61.200、目标 LIF 10.128.61.201，共享 ontap_lisaauto_cifs。两个真实测试通过，共 24 次文件传输及一个目录案例。
- 文件大小 4 KiB、2 MiB+1、64 MiB+1；Checkpointed / AtomicReplace；Local→CIFS、CIFS→CIFS、CIFS→Local。CIFS→CIFS 同时覆盖 read-back disabled/enabled，第二次覆盖同名目标。
- 每次文件传输返回后重新观察 mtime，均保留 `1700000001123456700` Unix ns。大于 64 MiB 的 Checkpointed 样本实际启用了恢复记录。目录显式设置 mtime 后，访问时间与创建时间保持不变。
- mock 回归覆盖 source version 冲突、取消、mtime-only 字段、负时间/溢出、100ns 精度损失、metadata 后按策略 FLUSH、FLUSH 失败时关闭句柄且不发布。
- smb-rs：147 lib、6 domain API、1 metadata API 通过；all-targets/all-features Clippy 和架构 guard 通过。
- 原始真实测试日志：[cifs-metadata-real-20260916.log](cifs-metadata-real-20260916.log)。测试使用隔离 UUID 名称并清理，不修改共享或账号配置。

本轮未重新做性能基准；此前无自动 mtime 的性能报告不能视为本次代码的性能结果。
