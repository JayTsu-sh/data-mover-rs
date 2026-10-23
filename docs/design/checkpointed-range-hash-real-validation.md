# Checkpointed range hash：真机验证记录

日期：2026-10-23。对应 [完整设计](checkpointed-range-hash-recovery.md)；
验收边界见 [独立检查表](checkpointed-range-hash-validation-checklist.md)。
本文只把本次实际运行的场景标为通过，不用数学测试、mock 或历史 nightly 替代真机证据。
按 research 工作流，由各后端独立研究/验证结果汇总成此记录，区别于此前仅完成本地门禁的实现交付。
初次验证轮没有修改生产库；仅补充测试工具的独立块大小参数、离线 NFS 审计工具/测试和验证文档。
后续用户确认的 DXN 引号兼容修复另记于本文末尾，不与初次构建混用证据。

**最新补充：DXN 引号兼容缺陷已修复，两种桶的旧→新、新→新四例真机恢复通过；见本文末尾。**
其余 backend/profile 的覆盖缺口不因此解除，仍不能宣称全量验收通过。

**初次结论：核心 hash/恢复性质已在已测真实路径得到证明，但全量验收未通过。**
DXN S3 已复现 receipt ETag 引号差异导致的恢复误拒绝；当前 ONTAP CIFS 位置不能保留
protected DACL，较大 Checkpointed 传输在首次 hash checkpoint 提交时安全拒绝。
此外，AFX 源文件本次 placement 是退化 DS=MDS，尚无当前生产多 DS 条带路径证明。
这些不是本地 CI 通过可以豁免的缺口；不据此宣布整个方案完整可用。

## 代码与证据身份

- HEAD：`6aa1633e749c0f5255c82d628f966102bcc39e81`，包含尚未提交的 range-hash 实现。
- 实际参与构建的源码集合（含未跟踪的实现文件）指纹：
  `f58c9e08bc69bdf47a4007728f2c0eee6478deb019763a114234fe1b3decd697`。
  生成方式：`git ls-files -c -o --exclude-standard -- src Cargo.toml Cargo.lock examples/support/endpoint.rs tests/lab/tools/transfer_resume.rs | LC_ALL=C sort | xargs sha256sum | sha256sum`。
  文档和后续独立离线审计工具不在此集合内。
- AFX 最初构建的 musl `transfer_resume.initial` SHA-256：
  `21376752f4214df72f08699ace0d6b9fdb04ea75f655293071ea706cb5a6fc72`。
- 仅增加测试工具独立 source/destination block-size 参数后的 musl 构建 SHA-256：
  `5d999398121640d9c00f6bb20a52d77b58b37938b336f269347e51a629e4d6d9`。
  库实现未因此改变。参数通过已有 connector 传递，不覆盖服务端协议上限，
  也不表示 CIFS/HDFS 已支持该参数。
- 本地私有证据：`/tmp/data-mover-hash-validation-evidence.puqx1E/`。
  三份大型 pcap 实际保存在私有目录 `/work/data-mover-hash-validation-pcaps-20261023-puqx1E/`，
  原路径保留符号链接，避免占满根文件系统。
  AFX 私有运行日志、二进制、抓包和恢复记录副本：
  `10.128.56.17:/tmp/data-mover-hash-20261023.uQqo3e/`。
  抓包和 hash 状态可能包含数据片段，不入仓库，不作为公开附件。

## AFX pNFS：生产 transfer 路径

执行机为用户指定的 `10.128.56.17`。现有内核挂载 `/mnt/dir1` 对应
`192.168.9.101:/pnfs_test2`，协议 v4.1，内核 rsize/wsize 为 65,536。
生产测试走用户态 NFS connector，不通过内核复制。
写入仅限唯一根 `resume-hash-20261023-uQqo3e/`。
整个测试持有共享 `terrasync-lab-tests.lock` 和 `terrasync-lab-performance.lock`；
没有修改挂载、服务、网络、快照或设备配置。
[授权范围](../../.claude/docs/test-environments.md)。

源文件为非零确定性数据，大小 **402,653,517 字节（384 MiB + 333）**。
通过 AtomicReplace 播种且启用读回校验。后续所有成功的 Checkpointed 传输也启用内置读回。

| 场景 | 实际保存/复用的 D | 恢复源 payload 读取 | 结果 |
| --- | --- | --- | --- |
| 32 MiB/s admission，6,500 ms 正常取消后新进程恢复 | `[0, 67108864)`，generation 1024 | 335,544,653 字节 / 5,121 次，64 KiB 请求 | `Resumed`，Pinned 读回通过，12,355 ms |
| 64 MiB/s admission，7 s `SIGKILL` 后新进程恢复 | `[0, 134217728)`，generation 2048 | 268,435,789 字节 / 8,192 次；源块改为 32,771，目标块改为 49,157 字节 | `Resumed`，Pinned 读回通过，15,284 ms |
| 不启用 QoS，源块 32,771、目标块 49,157 字节并发完整传输 | 新传输 | 抓包 12,287 次，402,653,517 字节 | `Fresh`，Pinned 读回通过，3,835 ms；66 次线级完成顺序逆转，非多 DS 证明 |
| 修改已完成 stage 的一个字节再恢复 | `[0, 67108864)`，没有重传该源前缀 | 335,544,653 字节 / 6,826 次；源块 49,157 | 在 Verify 报 `Corruption/Permanent`，未发布 final |
| 检查点生成后修改同大小源文件的一个字节 | 原恢复记录保留 | **0 字节 / 0 次** | Prepare 报 `Unsupported/Permanent`：恢复 hash 状态与本次绑定不兼容；未发布 final |

前两行 `source_streamed_bytes = 文件大小 - |D|`，请求数等于剩余长度按新源块取整。
初始取消/kill 使用 initial 二进制；第一行恢复仍使用 initial，其余恢复与并发/负向场景使用 options 构建。
SIGKILL 时 stage 长度为 **201,850,880 字节**，超过持久化的 128 MiB；
恢复没有把未承诺尾部当作 D。残留零字节 pointer.tmp 也没有被当作有效恢复根。
两份保存根均为外层 `DMDPTR02`、内层 `DMHCP002`、Receiving，H 编码为 106 字节；
源大小、generation、D 直接从保存记录解析，且相同记录被生产恢复路径成功接受。
[格式](../../src/storage/checkpoint.rs)、[发现校验](../../src/storage/discovery.rs)。

stage 篡改的位置为 12,345；原源字节为 178，修改为 0，确实改变了内容。
恢复仍不读取其已完成源前缀，最后却能拒绝损坏目标，因此此场景没有把目标自身的 hash
偷换为预期源 hash。源篡改场景还比对了失败前后 pointer SHA-256，相同，证明未静默清空后重传。
这是普通 mtime/身份绑定检测，不是对抗保留 mtime 的修改者，也不是不可变快照证明。

在源篡改前，独立经内核挂载读取源与三个成功 final，四个 SHA-256 均为：

`1d8c7ae0bacc7e2938e72ed8711fd6f514894ec8be69f32d263f519b1f3c8bcf`

该独立验收读取在 transfer/抓包审计区间之外，不计为恢复流程的源读取。
生产恢复使用源 H 重建 BLAKE3，并将其交给目的端 Verify；SHA-256 是额外独立核对，
不是替代该生产算法。[最终比较](../../src/transfer/engine/publication.rs)。

### 线级读取审计与清理

三个成功 transfer 分别保存完整 NFS/2049 抓包，捕获前启动全新连接。
tcpdump 报捕获包数 104,069 / 165,122 / 170,526，均为 **0 kernel drops**。
仅凭此或 aggregate 计数不能宣称源 READ 与 D 无交集：离线工具区分源与目标读回的
filehandle，并检查 CURRENT_FH、请求/回复关联与 pNFS layout。
`cancel-read-audit.json` / `kill-read-audit.json` 的源 READ/返回区间并集分别是
`[67108864,402653517)` / `[134217728,402653517)`，没有重复、未回答或失败的源 READ，
与保存 D 的交集均为空。完整并发运行的请求和返回并集均为 `[0,402653517)`，
实际观察 66 次完成顺序相对发起顺序逆转。
SIGKILL 后改变 chunk 的恢复也观察到 67 次线级完成顺序逆转。

**重要限制：这三次生产传输的源 bulk READ 都使用 MDS filehandle，发往 `192.168.9.101`。**
即使观察到 LAYOUTGET 和目标端 DS-handle 读回，也不能据此宣称源端完成了跨多个 DS 的条带读取。
因此本轮已证明真实 NFS 乱序完成下的 hash 与恢复区间排除，尚未补齐当前生产 transfer
在多 DS pNFS 条带数据路径上的证据。历史 raw-probe 的 DS 成功不能替代这项缺口。
源码与实际 GETDEVICEINFO 应答能解释该回退：源文件设备公布的首选 DS 是 MDS 自己
（`192.168.9.101:2049`），固定 nfs-rs 0.8.6 对这种 degenerate placement 走 MDS。
不能把它误诊为 engine 绕过 pNFS。还需要非退化源 placement 的生产路径验证。
完整 filehandle 选择、layout/device 帧号、复现命令见
[NFS 线级审计](checkpointed-range-hash-nfs-wire-validation.md)。
离线审计工具的 12 项独立测试通过，并已纳入 `make harness-contract`；
它们验证解析器，不替代真实抓包。

三个成功目标均独立检查无 `.data-mover-*` 残留；损坏 stage 的失败未留下 final。
源绑定失败保留的自有 stage/pointer 在证据复制后显式删除，随后通过测试工具删除唯一运行根，
内核路径确认根已不存在；本地 384 MiB 播种文件也删除。
删除仅涉及本次生成的测试数据，不影响他人数据；日志、抓包和小型 checkpoint 副本保留。
退出锁定 shell 后重新 `flock -n` 确认两个共享锁已释放。

## 其他已完成真机/本机验证

| Backend/profile | 本次结果 | 证据范围 |
| --- | --- | --- |
| Linux Local / XFS | SIGKILL、新进程复用 256 MiB，通过；损坏 pointer、同大小源变更均提前拒绝 | `strace` 直接证明源 pread 区间为 `[268435456,805306368)`，与 D 无交集；独立 BLAKE3 一致；不是 Windows 或断电认证 |
| ONTAP NFSv3 | 取消恢复、改变源 chunk、Pinned 读回和独立 BLAKE3 均通过 | 源 201,326,925 字节，D 为 67,115,008；恢复恰读 134,211,917 / 2,731 次；aggregate 证据，不是逐 READ 区间证明 |
| ONTAP NFSv4.0 | 同上，通过 | 实际 D 与计数同 v3；不是逐 READ 区间或非 1024 对齐切点的独立证明 |
| ONTAP CIFS / UNIX security style | **安全拒绝，不是正向 hash 恢复通过** | 服务端不保留 protected DACL 位，首次 v2 hash pointer 提交前报 Unsupported；AtomicReplace 和独立摘要对照成功；未放宽私密性要求 |
| Kerberos HDFS / replicated append | 取消、SIGKILL、H-only intent、lease tail 恢复四项通过 | syscall 证明均不重读实际复用前缀；最强场景在 D 之外采用 8,386,560 字节、仍严格处于保存 H 内；独立 BLAKE3 和清理通过 |
| MinIO 非版本/版本 | 3 项流式、3 项原生恢复及独立 SHA-256 通过；另有损坏根失败关闭 | 流式 syscall 验证源 READ 不碰采用的 parts；原生 H 完整保存后恢复 0 源 payload GET；成功后 root 历史和 MPU 均清理 |
| RustFS 非版本/版本 | 两项流式恢复和独立 SHA-256 通过 | ListParts 可采用 pointer 的 D 之外、但已被 H 覆盖的 parts；原生用例因 relay 脚本问题无效，未计通过 |
| DXN S3 非版本/版本 | 初次发现真实缺陷；后续修复四例通过 | 原字节指纹曾因引号差异误报 Corruption；后续统一双引号编码，旧→新与新→新均恢复成功，见末尾 |
| ONTAP S3、StorageGRID | **未测，凭据受限** | 本机缺少对应授权专用密钥；没有借其他身份或新建凭据绕过范围 |

详见 [Local 实测](checkpointed-range-hash-local-validation.md) 和
[NFSv3/v4.0 实测](checkpointed-range-hash-validation-checklist.md#this-sidecars-real-ontap-nfsv3--v40-results)。
当前 CIFS 环境限制及原始 SMB 应答见 [CIFS 实测](checkpointed-range-hash-cifs-validation.md)。
需要另行获准、实际保留 protected DACL 的测试位置，才能补齐 CIFS 正向 hash 恢复证据；
没有为测试而修改共享卷 security style。
HDFS 的 H/D 与 lease-recovery 数值、四份 syscall trace 和清理证明见
[HDFS 真机实测](checkpointed-range-hash-hdfs-validation.md)。
8 个完整 S3 正向恢复用例、损坏 pointer 阴性用例、发布间隙观察及 DXN 探针见
[S3 真机实测](checkpointed-range-hash-s3-validation.md)。
MinIO 的额外发布间隙试验确认 `read-back off` 时仍发生最终目标强制验证、且 0 源 payload GET；
但用例专属的独立 checksum 验收因脚本误分类中止，不能计为完整通过。

DXN 后续修复应对 opaque ETag 规定允许的引号规范化，保留真正 receipt 值不同的拒绝测试；
不能通过不再验证 receipt 来掩盖兼容性缺陷。本轮只报告，不擅自改生产逻辑。
这些场景使用独立固定的 GNU 二进制，SHA-256
`bb7e314842bd894bfd154cc54010e551cac1a1cd0a7d4a4e20377d3cb3bf475a`。

本轮首次和纳入离线审计自测后的最终 `make ci` 均以退出码 0 完成，最终日志
`/tmp/data-mover-hash-validation-ci-final-20261023.log`：
1,826 项 library tests 通过、9 项忽略、0 失败；其他配置的 integration tests、
61 项 Python harness tests（28+21+12）、fmt、严格 clippy、architecture guard、examples build 和 Local e2e 均通过。
测试工具新增独立块大小参数的两个 example tests 也通过。
这不是完整外部 backend nightly 或真实长时内存门禁的替代证据。

## 验证范围的判定规则

以下不能由上述成功结果推导：

- 文件系统或服务端断电、NFS write-verifier 变化、SMB 服务器重启、HDFS DataNode 掉电。
  本轮只允许客户端故障，不擅自扰动共享设备。
- 源文件不可变性；文件系统身份/mtime 检查弱于 snapshot 或 immutable version。
- 每一种 backend/profile 的 sparse D、Ready/Published commit gap、权限失败、Object Lock、
  长时间内存门禁和完整 ReleaseReady 矩阵。需要逐项实测，不能以另一个 backend 的通过填补。
- 非 Checkpointed 两种模式的线级零新增 I/O；这仍由现有明确的操作计数回归测试约束，
  本轮 AtomicReplace 播种成功不是完整模式隔离证明。

远端本轮测试数据、临时 S3 桶、MPU、runner payload 和 Local 大型夹具均已在验收/证据保留后清理，
各后端报告给出了精确边界；仅保留受限的本机日志、抓包、小型 checkpoint 和部分可再生验证 fixture。
未改共享设备配置；没有提交、推送或触发 GitHub nightly。

## 后续：DXN ETag 双引号兼容修复

用户确认后，仅修改 S3 checkpoint receipt 编码/校验：规范值使用恰好一对双引号，
保留大小写和原始协议回执，不扩大至通用 transfer 或其他策略。旧 quoted 指纹字节相同；
恢复还受限兼容 exact-raw 与 uniformly-bare legacy 指纹，不能确定性还原的旧 mixed 表示仍拒绝。
畸形引号、控制字符、weak-validator 与真正变更继续失败关闭。
迁移细节见 [设计 §14.5](checkpointed-range-hash-recovery.md#145-dxn-receipt-引号兼容修复)。

7 项 focused regression 在旧实现先失败、修复后通过；完整 `make ci` 退出 0：
1,833 项 library tests 通过、9 项忽略、0 失败，61 项 Python harness tests、严格 clippy、
fmt、architecture guard、workspace integrations、examples 与 Local e2e 均通过。
最终日志：`/tmp/data-mover-dxn-etag-ci-final-20261023.log`。
此候选 source/tool 指纹为 `442e06c909bce2f8342d00060eb1c1819785b9acb66568477096f5d995807d3e`，
真机复测使用固定 GNU executable，SHA-256
`5fdd956391f5dfde773ee438a464dee7a8fff6d3f0119f81bfa603a4a28cc51d`。
真机复测的独立结果见 [S3 follow-up 记录](checkpointed-range-hash-s3-validation.md)，
未将修复前其他 backend 的通过迁移到本候选；CIFS 与多 DS 等原覆盖边界不因此解除。

DXN 非版本/版本桶各完成旧 musl→新 GNU、新 GNU→新 GNU 两种取消恢复，共四例通过。
源大小均为 469,762,381 字节；前三例复用 100,663,296、实际源读 369,099,085 字节，
最后一例复用 83,886,080、实际源读 385,876,301 字节。
syscall 审计证明每例源 READ 与复用 D 无交集、无重复，Pinned 读回与独立 SHA-256 均一致。
成功后各目标仅一个版本，零 pointer 历史、marker、MPU；验收后两个唯一前缀已清理，
两个临时版本桶删除并 HEAD 确认 404，共享锁和子进程均释放。
这证明当前 DXN 引号问题及既有 quoted receipt 记录升级恢复已修复，不代表新增其他 S3 profile 认证。
