# Checkpointed range-hash：CIFS 真机验证

日期：2026-10-23，Asia/Shanghai。对象：当前工作树的最终实现，不是旧版协议测试。

## 结论

**本环境验证了安全拒绝，未验证通过 CIFS v2 hash 续传。** 已批准的 ONTAP
`ontap_lisaauto_cifs` share 是 UNIX security style：服务器接受设置 DACL，但不保留
`SE_DACL_PROTECTED`。当前实现到第一个 64 MiB checkpoint 时按契约返回
`Unsupported / Permanent`，不降级写出无法保护的 hash pointer。受限目录与未经 ACL
修改的新目录均复现；fresh-process 重试也复现。

同一服务器/账号/构建的 AtomicReplace 对照成功传完 192 MiB + 123 B，独立客户端完整
读回的 BLAKE3 和 SHA-256 均与本地源一致。这是非 hash 路径的传输对照，**不是 v2
恢复成功的替代证据**。后续正向验收需要另行批准、确实能保留 protected DACL 的 SMB
测试目录或 share；本轮没有改变卷、share、服务或全局 ACL。

## 环境、构建与隔离

- 环境来源：[真机环境目录](../../.claude/docs/test-environments.md)、
  [e2e-cifs skill](../../.claude/skills/e2e-cifs/SKILL.md)、
  [CIFS backend 说明](../../.claude/docs/storage-cifs.md)。只操作现有批准 share。
- research 方法：以当前源码、真实 SMB API 应答、实际进程输出为一手证据；不以
  in-memory 单测或既往真机记录冒充本次成功。
- 观察客户端：临时 venv 中的 `smbprotocol 1.17.0`，SMB **3.1.1**，
  `require_signing=true`。独立摘要使用 Python `blake3 1.0.11` 和 `hashlib.sha256`。
  观察客户端签名结果不等于宣称 transfer example 也强制签名；后者保持现有默认策略。
- 主协调者构建的二进制：
  `/work/cargo-target/x86_64-unknown-linux-musl/release/examples/transfer_resume`。
  SHA-256：`5d999398121640d9c00f6bb20a52d77b58b37938b336f269347e51a629e4d6d9`。
  本次没有启动 Cargo，也没有修改生产源码或提交代码。工作树基线 HEAD 为
  `6aa1633e749c0f5255c82d628f966102bcc39e81`，实现包含当时未提交的共同集成改动。
- 唯一远端写入/删除根：`resume-hash-cifs-20261023-hTeYcJ/`。
  本地证据根：`/tmp/resume-hash-cifs-20261023-hTeYcJ/`。无本地恢复记录复用；每个传输
  子进程使用单独的空 HOME，已结束进程的 HOME 均保持空目录。
- 每次远端操作持有 `/tmp/terrasync-lab-tests.lock` 与
  `/tmp/terrasync-lab-performance.lock`；锁被其他运行持有时不绕过。命令均设置 timeout。
  凭据从仓库外批准的 0600 文件在内存加载，不进入文档或子进程命令行。
- 文件大小 **201,326,715 B = 192 MiB + 123 B**，严格大于 64 MiB checkpoint 间隔；
  源为固定 inode 的本地确定性文件，mode 0600。首轮源带宽 20 MiB/s；不是阈值以下用例。

## 1. 独立 DACL 能力证据

先在唯一运行根上收紧权限，再在其下创建空的 `acl-probe.bin`。独立客户端发送
`SMB2 SET_INFO(Security)`：`additional_information=0x80000004`
（DACL + PROTECTED_DACL），descriptor 同样带 `DACL_PROTECTED`，随后重新打开并
QUERY_INFO 读回，不依赖 data-mover 的 ACL 判断。

| 观察 | 结果 |
|---|---|
| `READ_CONTROL` 查询 | 成功 |
| `WRITE_DAC` / SET_INFO | 成功，不是权限不足 |
| 设置前后 DACL 序列化字节 | 完全相同 |
| 请求 protected 位 | true |
| 重新查询 protected 位 | **false** |
| probe 数据长度 | 0 B |

受限 DACL 中 group/other 投影 ACE 的 mask 均为 0；其 DACL BLAKE3 为
`0e5fd583f53ae14e3c35fee69e902be6b42fc91e6c9aaa92fab89eac79125ceb`。
未修改 ACL 的新目录读回 DACL BLAKE3 为
`46a2578a5f440c0aa37540584d9e5d7f258b5791bf05b8ac658e4fa23d8f25e0`，
protected 位同样为 false，普通 Checkpointed 传输同样被拒绝。

原始摘要证据：`capability-probe.jsonl`、`preflight.jsonl`、
`default-refusal-clean-parent.jsonl`。一次清理连接曾超时；随后重试成功并删除运行根后，
才重新创建未经 ACL 修改的目录做对照，不能把之前仍存在的受限目录称作默认权限对照。

## 2. 当前普通 Checkpointed 路径与新进程重试

请求使用普通 `transfer()` 路径、`--policy checkpointed --read-back off`。初次请求安排
6 s 后取消，但保护失败先于取消发生，因此该行**不算合作取消成功**。

```text
CIFS Write failed: Unsupported operation:
server did not preserve the protected checkpoint DACL
```

| 用例 | 结果 | 库报告 source bytes / ops | 独立 pread64 观察 |
|---|---|---:|---|
| 受限目录首轮，6 s 取消计划 | 提前 Unsupported | 67,108,864 / 321 | 首轮未跟踪 |
| 同路径、同 identity、全新进程/HOME | Unsupported | 67,108,864 / 323 | 323 次，67,738,012 B，`[0, 67738012)` |
| 删除并重建默认 ACL 目录后首轮 | Unsupported | 67,318,580 / 324 | 324 次，67,947,728 B，`[0, 67947728)` |

各次都报告 `final_destination_changed=false`、`recoverable_stage=false`，最终文件不存在。
原始 SMB 列举只见 **67,108,864 B stage**，没有 `.pointer` 或 `.pointer.tmp`。
不存在已提交的 D/H，所以新进程从 0 读取，不可将 stage 文件长度冒认为可复用 D。
strace 针对实际源文件路径筛选 `pread64` 并记录 offset、返回长度；库的 streamed bytes
与失败时已发出的预读并不相同，表中保留两个口径，不宣称 QoS bytes 是全部物理读取。

安全契约与实测对应：[checkpoint_acl::copy](../../src/storage/backends/cifs/checkpoint_acl.rs)
同时核验 DACL 与 protected 位；
[write_record](../../src/storage/backends/cifs/at_destination.rs) 在写 pointer 内容之前调用
`protect_checkpoint`，失败路径删除 temporary。本轮证实无 hash pointer 残留；并未声称用
事后列举证明从未创建过空 temporary。

证据：`cancel.jsonl`、`fresh-refusal.jsonl`、`cancel-fresh-process.strace`、
`default-refusal-clean-parent.jsonl`、`default-first.strace`。同 identity 重试为
`6551b8326279e82526c058369306f098dc65eaef478696715a53493fc600137a`。

## 3. 客户端中断边界

最后已经启动的有界客户端中断脚本得到以下结果；均未能越过首个 v2 checkpoint。

| 用例 | 中断时观察 | fresh-process 结果 |
|---|---|---|
| SIGKILL，进程启动后约 3.04 s | 已写 **18,035,512 B stage**，无 pointer；Python 子进程返回 `-9`，不是合作取消 | 同 identity，首个 checkpoint 返回同一 Unsupported |
| 合作取消，传输计时 1,000 ms | `Cancelled / Transient`，`final_destination_changed=false`；**17,616,080 B stage**，无 pointer | 同 identity，首个 checkpoint 返回同一 Unsupported |

两次 fresh-process 的实际源 `pread64`：

| 前一次中断 | 库 source bytes / ops | 实际 pread64 次数/字节与合并范围 |
|---|---:|---|
| SIGKILL | 67,108,864 / 322 | 322 次，67,528,296 B，`[0, 67528296)` |
| 合作取消 | 67,108,864 / 323 | 323 次，67,738,012 B，`[0, 67738012)` |

没有 pointer 时从 0 重新读取是安全结果，不是源免重读能力通过。以上所有中断与重试
的 HOME 均保持空。原始证据在 `finish.jsonl`、`kill-fresh-process.strace` 和
`early-cancel-fresh-process.strace`。

同一已启动的收尾脚本还观察到：目标存在 67 B 旧 final 时，checkpoint 保护失败后旧
final 仍逐字节相同，BLAKE3 为
`5db3acac2fbfec10b45b67c649d9007a1d5e5cce89ca8cb1cc3373d4da0042c9`。
该检查是拒绝时的旧 final 保留，不是 hash 发布后恢复。

不能在首次 v2 pointer 已提交后做正向 SIGKILL 恢复：该环境在提交前就拒绝保护。

## 4. AtomicReplace 与独立完整性对照

相同 endpoint 和二进制，`--policy atomic --read-back off`，201,326,715 B：
`result=ok`、`prepare=Fresh`、`recovery=Disabled`、`reused_bytes=0`，库报告源读取
201,326,715 B / 97 ops。独立观察客户端另开 SMB 文件，完整读取发布后的 final，并与
本地源分别计算两个摘要，长度及摘要全部相等：

```text
BLAKE3  7c4dd848976a118f2642298b52bc00aa8331462c0fe213fa929496be0b5959d6
SHA256  3355e297d818a2458e5bb5be511110dc57c8f7c0907535fc2fc3fb588013130a
```

这排除了“账号根本不能写”“数据路径不通”“只有 read-back 开启才成功”等替代解释。
它不证明 hash checkpoint、恢复后的 source range 排除或 v2 root release。
证据：`controls.jsonl`，独立摘要行 `independent_hash`。

## 5. 清理与验收边界

**远端已清理完成**：收尾脚本删除唯一运行根内的三个 64 MiB stage 和 67 B sentinel，
再删除目录；之前的 AtomicReplace 对照 final 已在同一根的前一次清理中删除。没有
删除其他运行根或常驻夹具。清理后另起观察进程/新 SMB 会话，直接 stat 同一路径收到
`SMBOSError, errno=2`，确认目录不存在。证据：`finish.jsonl` 的 `cleanup` 与
`final-cleanup-verification.jsonl` 的 `fresh_session_cleanup_verified`。

本次持有的两把实验锁均已释放（其他验证仍可能重新持锁）。临时本地源文件及 seed
副本共八个、约 1.5 GiB，均为可重建的生成数据，验证结束后删除；
保留临时脚本、JSON 输出和 strace 供本轮汇总审阅。没有把凭据写入它们或提交。

本地复现入口为 `/tmp/resume-hash-cifs-20261023-hTeYcJ/validate.py`，使用上文已记录 SHA
的二进制。主要 action 为 `preflight`、`fresh-refusal`、`default-refusal`、
`capability-probe`、`controls`、`finish`、`verify-absent`；脚本仅在本轮唯一运行根内
创建/检查/清理文件。初始正向 v2 断言在收到 Unsupported 后终止；后续明确按负向能力
检查记录，不将其改称正向恢复成功。临时脚本的退出码规范化修正不涉及生产源码。

尚未得到真机正向证据的项目：实际 `DMDPTR02` 编解码/私有 pointer DACL、保存的 H/D
恢复、源读取与 recovered D 严格不相交、checkpoint 后 cancel/SIGKILL 的 fresh-process
续传、v2 final 独立摘要及 root release、rename 发布间隙恢复。CIFS sparse island 与权限
收紧流程也未由本次试验覆盖。不存在这些成功证据，不能将本轮标记为“CIFS hash
方案完整性通过”。本轮能确认的是当前安全拒绝行为与 AtomicReplace 对照完整性；
不涉及服务器掉电、介质持久性或新增跨主机分布式锁保证。
