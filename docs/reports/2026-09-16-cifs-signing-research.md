# CIFS signing 与 mount 性能差异调查

日期：2026-09-16。范围：FAS2750 上的 SMB 3.1.1、Linux CIFS mount，以及 data-mover 固定的 smb-rs `d8291b3a3157b026074ab6b0f305dfd99af17f1e`。只调查，没有修改服务端安全配置或生产代码。

## 结论

Linux mount 能在这个共享上使用不带签名的普通 READ/WRITE，是因为服务端没有强制签名，客户端默认策略也没有强制；这不是 Local backend 绕过 SMB。直接 CIFS backend 的依赖则主动要求普通认证会话的消息经过签名或加密。协议允许双方都不要求签名时使用未签名的业务消息，但当前 smb-domain 公共 API 没有相应开关。

本次正式性能矩阵已经给 mount 设置 `sec=ntlmsspi`，双方都签名，因此约 2.7 倍差距不能解释成“mount 不签名、CIFS 签名”。源页缓存命中减少了整个 SMB 读取路径，直接客户端还有显著的软件 AES-CMAC CPU 成本。以下把现场证据与代码推断分开。

## 现场证据

| 场景 | 实际观察 |
|---|---|
| 初始默认 mount | 捕获 WRITE unsigned |
| mount 改成 `sec=ntlmsspi` | 捕获 WRITE signed |
| 直接 CIFS backend | 捕获 READ/WRITE signed |
| FAS 协商 | SMB 3.1.1，SecurityMode `0x1`，没有 REQUIRED 位 |
| 直接客户端协商上限 | read/write 均为 1,048,576 字节 |

证据：[初始捕获摘要](../../benchmarks/cifs-mount-comparison/security-probe.jsonl)、[签名对齐摘要](../../benchmarks/cifs-mount-comparison/security-aligned.jsonl)、[协商摘要](../../benchmarks/cifs-mount-comparison/security-negotiated.jsonl)。mount 小文件探针的源页缓存已命中，因此没有捕获源 READ；不能将“未捕获 READ”当成“READ 未签名”。

正式矩阵的 40 MiB AtomicReplace 中位数：源缓存命中时 mount 432.736 ms、客户端 1199.250 ms；驱逐客户端源页缓存后 mount 927.432 ms、客户端 1232.550 ms。预热在计时之外，服务端缓存没有清空。这两组分别回答“重复读取已缓存源文件”和“需要经 SMB 重新读取源内容”的问题，不能混用。[完整报告](../../benchmarks/cifs-mount-comparison/report.md)

## Linux 为什么允许不签名

SMB2/3 的 SIGNING_ENABLED 和 SIGNING_REQUIRED 是不同概念。若会话要求签名，服务端必须拒绝未签名的普通请求；若不要求，可以继续处理。普通未签名请求对应的服务端响应也会跳过签名处理。[MS-SMB2 请求验签](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/85df1680-2ee7-4d25-a916-a982371ddc75)、[响应签名](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d594481c-f6d5-4de5-8842-9099063d41e7)

Linux `sec=ntlmssp` 选择 NTLMSSP 认证；`sec=ntlmsspi` 还将 `ctx->sign` 设为 true。Linux 构造普通 SMB2 请求时，只有连接的 `server->sign` 为真且不采用 SMB 加密时才设置 signed 标志。因此“认证成功”和“每个数据消息都签名”不是同一件事。[Linux v6.16 挂载选项解析](https://github.com/torvalds/linux/blob/v6.16/fs/smb/client/fs_context.c#L217-L232)、[SMB2 请求头构造](https://github.com/torvalds/linux/blob/v6.16/fs/smb/client/smb2pdu.c#L151-L152)

这里 v6.16 源码用于说明主线机制，不声称它与现场内核构建逐字一致。现场实际策略以捕获的 signed 标志为准。挂载工具手册也明确指出 `ntlmsspi` 强制签名，而服务端要求或全局客户端策略也会启用签名。[mount.cifs 手册](https://man7.org/linux/man-pages/man8/mount.cifs.8.html)

## 当前 CIFS backend 为什么签名，能否关闭

代码路径如下，链接固定到本次依赖提交：

1. data-mover [factory.rs](../../src/storage/factory.rs) 调用 `smb_domain::Client::new()`，使用 NTLM 凭据连接共享，没有签名配置参数。
2. [facade/mod.rs:55](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/facade/mod.rs#L55) 构造 DomainClient；[domain/mod.rs:395](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/domain/mod.rs#L395) 使用默认 RuntimeClient。
3. [session/state.rs:432](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/session/state.rs#L432) 的 Ready 状态 `allow_unsigned()` 只对 guest/null 会话返回 true。普通用户名密码认证不满足此条件，和 FAS 是否设置 REQUIRED 无关。
4. [session/channel.rs:105](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/session/channel.rs#L105) 为未加密且不允许 unsigned 的请求选择 `SignWithChannel`；同文件约 285 行拒绝相应会话的未签名、未加密响应。
5. [connection/config.rs](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/connection/config.rs) 只有 `allow_unsigned_guest_access`，没有普通认证会话的签名策略。该配置模块本身也是 crate-private；不能拿内部 wire 的 `Protection::None` 当成公共产品功能。

因此：**协议上可以，当前 API 不可以。** 要提供该能力，需要先在 smb-rs 公共 facade 增加签名策略并传递到协商、会话状态、发送和接收校验。建议保留默认签名，另提供“仅在对端不要求时允许未签名”的显式策略；若采用严格 Disabled 策略，应在服务端要求签名时失败，不能继续无签名发送。会话绑定、认证握手、重连和多通道有独立的协议约束，也不能统一用“清除 signed 位”处理。这些是后续设计建议，本轮没有实现。

关闭签名会取消 SMB 消息级完整性保护；NTLM 身份认证仍存在。SMB 加密是另一个选择：它包含完整性保护，代码选择 Encrypt 时不再额外选择 SignWithChannel，但这不代表“不做密码学计算”。[当前 wire protection 分支](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/wire.rs#L453)

## inflight 为什么不能完全补偿缓存

缓存命中让复制计时阶段免去源内容的网络 READ、响应验签和 SMB 解码。inflight 只能重叠仍然需要执行的工作：理想流水线趋近读、写、CPU 等瓶颈阶段的最大耗时；不能让这些工作凭空消失。八个在途请求也不等于八个并行密码学执行线程。

本次 40 MiB、冷源缓存、AtomicReplace 的直接客户端用户态 CPU 采样共 434 个：约 93.55% 位于软件 AES fixslice，另约 3.92% 位于 CMAC update。测试 VM 的 CPU flags 没有 AES。这说明本次用户态计算热点主要是软件签名处理，不能将 97.47% 用户态 CPU 样本直接解释成 97.47% 端到端耗时。[perf 数据](../../benchmarks/cifs-mount-comparison/profile-client.txt)、[CPU 信息](../../benchmarks/cifs-mount-comparison/cpu.txt)

实现依据：SMB3 无显式签名算法选择时默认 AES-CMAC；SMB3.1.1 有 signing capabilities 响应时使用服务端选择的算法。[dialects.rs:130](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/dialects.rs#L130)、[dialects.rs:195](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/dialects.rs#L195)。CMAC 实现调用 `Cmac<Aes128>`，perf 支持本次实际经过该路径；现有捕获摘要未记录 signing capabilities，不能确定是显式协商还是默认回退。[crypto/signing.rs:134](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/crypto/signing.rs#L134)

同一连接的 runtime owner 循环 inline await 请求准备与响应处理；发送路径同步计算消息签名，接收路径同步验签，没有给每个在途请求分派独立密码学 worker。因此 inflight 可以隐藏网络等待，但当前结构不能把同一连接上的所有签名计算并行铺满多个 CPU。源连接与目标连接仍可并行；这不等于整个复制只能使用一个核心。[engine.rs:703](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/engine.rs#L703)、[发送变换:1025](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/engine.rs#L1025)、[接收变换:1484](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/engine.rs#L1484)、[同步验签](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/wire.rs#L811)

据此，优先验证 VM 暴露 AES 指令后的签名性能，再考虑受控的密码学并行化或签名策略对照实验，比仅增加 inflight 更有针对性。Linux mount 自身的 kernel crypto 消耗不在这个用户态 profile 中，尚不能量化两个 AES 实现的效率差距，也不能保证开启 AES 后就完全追平源缓存命中的 mount。

## 补充：FAS2750 上发送签名与接收验签是否都可选

需要区分“协商成普通数据消息无需签名的会话”和“收到签名响应后不校验它”。对于前者，本次 FAS2750 的 `0x1` 协商模式和默认 mount 的 unsigned WRITE 成功记录说明具备可选签名的实际条件；普通请求不签名时，服务端按协议也跳过对应响应的签名，于是客户端没有该响应的 AES-CMAC 验签计算。本次探针没有实际捕获默认 mount 的 unsigned READ，不能声称该 READ 路径已经实测通过。未经修改的 smb-rs 也尚不能完成这种模式的端到端测试。[服务端响应签名规则](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d594481c-f6d5-4de5-8842-9099063d41e7)

当前微软客户端验签规范 §3.2.5.1.3 要求：若 `Session.SigningRequired` 为真，必须验签，失败必须丢弃；已经成功解密、MessageId 全 1、STATUS_PENDING 属于列出的跳过情况。该版本文字不能解读为“即使 SigningRequired 为假，也一律必须验证任何带 signed 位的响应”。因此不能把可选会话的协议要求与当前 smb-rs 更严格的实现混为一谈；也不能把服务端不要求签名推广成任意会话都能设置 `verify=false`。[客户端验签规范](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/36172e53-ac81-48fb-b2e3-caa3761b9157)

当前 smb-rs 接收路径有两层：

| 层 | 实际行为 | 对可选签名实现的影响 |
|---|---|---|
| `runtime/wire.rs::verify_plain_incoming`，约774–827行 | 普通带 signed 位的消息执行真实验签；未签名普通消息跳过此处计算；另外处理加密、pending、通知、session invalidated 恢复信号、binding 特例 | 仅跳过这一函数无法让正常匿名以外会话接受未签名响应 |
| `session/channel.rs::_verify_incoming`，约251–294行 | 检查会话身份、必须加密条件；普通认证会话拒绝既未签名也未加密的响应 | 必须让有效会话策略控制是否允许 unsigned |
| `tree.rs`，约506行 | 共享要求加密时拒绝未加密响应 | 允许 unsigned 不能绕过共享加密要求 |

源码：[wire 接收验签](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/runtime/wire.rs#L774)、[session 接收检查](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/session/channel.rs#L251)、[tree 加密检查](https://github.com/JayTsu-sh/smb-rs/blob/d8291b3a3157b026074ab6b0f305dfd99af17f1e/crates/smb/src/tree.rs#L506)。内部 `skip_security_validation` 是认证建立阶段工具，注释限定用途，不是公共 READ/WRITE 验签开关。

设计上建议用统一的有效会话保护策略决定普通请求发送签名和响应接受规则，而不是两个互相矛盾的 `sign=false` / `verify=false` 独立开关。对可选模式里仍然带签名的响应继续验证，是可保留的实现策略；获得主要吞吐收益的路径是双方普通数据消息都不签名，不是忽略已经收到的签名。SMB 3.1.1 TREE_CONNECT、认证和绑定等控制请求有特殊规则，不能跟随普通数据路径一概取消保护；共享或会话加密也仍需遵守。[TREE_CONNECT 规则](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/652e0c14-5014-4470-999d-b174d7b2da87)
