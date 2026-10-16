# Backend 传输默认配置：环境证据与补测方案

日期：2026-10-15。范围：Local、NFS、CIFS、S3、HDFS 的独立读写配置。
前半部分是实施前的源码与历史证据核对；实施后的新测量与环境限制见末节。历史结果不等同于新 pipeline 的默认值结论。

## 设计与决策边界

backend 持有读/写并发、读/写块大小和协议约束；source 对外按字节顺序输出，内部允许并发读取。
中间以 bounded channel 背压，容量 4 只作为旧 `copy_file` 的起点，后续比较 2/4/8。
destination 用 `Bytes` 分片拆分或聚合，避免应用层复制 payload；native 请求并发由 native backend 管理。
普通 `transfer` 调用者无需提供 `chunks / bytes / operations`。

默认值应是“某类环境下经验证的保守起点”。不能仅靠协议名称找到跨硬件、RTT、服务端、持久化策略的全局最优值。
先保留当前行为作为对照，完成统一 channel 和分片 body 改造后再决定是否调整；已有旧 pipeline 的吞吐拐点也需复测。
调研结果固化为 backend 的内置默认值；连接时再按协商的 rsize/wsize、SMB credits/请求上限等能力收紧。
是否需要针对不同环境提供配置覆盖，由补测结果决定；不在每次 transfer 时运行自动压测，也不按服务器 IP 硬编码调优值。

## 改造前的实际配置：以执行路径为准

下表记录本次实现之前的审计基线。当前实现已移除请求中的 `InflightLimits`，
NFS 读窗口来自 backend 配置；S3 已接入有序并发 Range GET（初始读 4，实测后调为 2，带 QoS 的读取仍逐段推进），
multipart 写默认 4，支持 backend 环境覆盖。分片 body 取代连续聚合缓冲，native 窗口由 backend 自管。
这些是实现变化，不是新的真实环境最优值结论；以下历史证据与补测矩阵继续作为定参依据。

| Backend | 当前代码实际行为 | 候选决策与注意事项 |
| --- | --- | --- |
| Local | factory 要求显式提供读/写并发；读并发再取请求限制的较小值；read ceiling 2 MiB，write task ceiling 8 MiB | README 的 4/8 可作待验证起点，但不能称所有 factory 调用的隐式默认；8 MiB 是写上限，不会自动聚合小块 |
| NFS | client 配置默认 4/8；写使用 backend 的 write depth；普通 source 的读窗口直接取 `ReadRequest.read_inflight`；读写块分别取 client ceiling 与协商 rsize/wsize 的较小值，client 默认 ceiling 1 MiB | 先保留配置起点 4/8，修复读配置归属后比较非对称窗口；不能把 client read=4 当现有 source 的必然有效值 |
| CIFS | 连接时解析独立读写配置，默认 8/8；source 仍受请求上限约束；读写块按服务器协商上限收紧 | 保留 8/8；本仓库 FAS 环境协商为 1 MiB，但这不是所有服务器固定值 |
| S3 streaming | source 默认 read chunk ceiling 8 MiB，普通 Range GET 串行推进；新 multipart part 基准 8 MiB、最多 4 parts 在途，另有正在组装的 part；single PUT 默认阈值 8 MiB | README 的 4/5 不能当当前 role 路径有效并发；恢复有序并发读、分片 body 后重测，不直接照搬 4/5 |
| S3 native | 默认 ≤64 MiB 用单次 CopyObject；更大对象以至少 64 MiB 的 part 复制，内部并发 ceiling 6，仍受传入 operations 限制 | 6 是实现选择，不是协议硬上限；独立比较 2/4/6/8，禁止混入 streaming 结果 |
| HDFS | 实际 client 默认 read=4/write=1；source 取请求与 client read 的较小值；读写 chunk ceiling 2 MiB，写通过有序 session 逐块提交 | trait 的 read 默认 8 被真实 client 覆盖；先保留 4/1，读窗口比较 2/4/8/16，底层 packet pipeline 与文件级并发分别计量 |

代码依据：
[Local factory](../../src/storage/factory.rs)、[Local source](../../src/storage/backends/local/source.rs)、[Local writer](../../src/storage/backends/local/staged.rs)、
[NFS client](../../src/storage/backends/nfs/client.rs)、[NFS source](../../src/storage/backends/nfs/source.rs)、[NFS writer](../../src/storage/backends/nfs/staged.rs)、
[CIFS connect](../../src/storage/backends/cifs/connection.rs)、[CIFS source](../../src/storage/backends/cifs/source.rs)、
[S3 source](../../src/storage/backends/s3/source.rs)、[S3 sizing](../../src/storage/backends/s3/staged.rs)、[S3 parts](../../src/storage/backends/s3/staged/parts.rs)、
[S3 native](../../src/storage/backends/s3/staged/native_final.rs)、[HDFS client defaults](../../src/storage/backends/hdfs/client/support.rs)、
[HDFS real protocol](../../src/storage/backends/hdfs/client/role_protocol.rs)、[HDFS protocol](../../src/storage/backends/hdfs/protocol.rs)。
恢复中的 S3 multipart 必须保留已记录 part size，可能为 64 MiB；不能只按新建上传 8 MiB 估算内存。

## 已有测量能支持什么

### Local

保存完整原始数据的 2026-09-14 policy 比较环境为 Linux 6.8.0-124、24 vCPU、约 32 GiB RAM、XFS，源块 2 MiB、写任务上限 8 MiB、读写窗口 8。
1 GiB 新目标、不读回校验时，AtomicReplace 中位 348.867 ms，而 Checkpointed 为 873.056 ms。
这证明持久化策略显著改变端到端结果，不能拿这种 policy 差异作为块大小或读写并发选择依据；它也没有比较 4/8 与 8/8。
证据：[environment](../../benchmarks/local-policy-comparison/results-20260914/environment.json)、
[summary](../../benchmarks/local-policy-comparison/results-20260914/summary.csv)、[samples](../../benchmarks/local-policy-comparison/results-20260914/samples.csv)。

历史独立写基准涉及 ext4/XFS/tmpfs 和多种 chunk/depth，但原始数据只在 `/tmp`，当前文档不能提供长期原始数据链；只作为候选来源，不据此宣布某个写 chunk 最优。
[旧写基准](local-write-buffer-benchmark.md) 和 [旧读上限调研](local-io-buffer-limits.md) 还保留历史 5 MiB 结论；现行 8 MiB 以上述源码及 [ADR-0002](../adr/0002-auto-and-quick-policy.md) 为准。
Local backend 可能指本地磁盘、tmpfs 或网络挂载，报告必须记录实际 filesystem/mount，不能混为一种设备环境。

### NFS

2026-09-14 FAS2750 单文件窗口实验保存了 180 个正式样本，应用块 1,044,480 B，读写对称为 8/16/24；关闭计时内读回校验，计时外校验，Checkpointed 间隔 64 MiB。
40 MiB client 从 8 升到 24，Checkpointed 耗时 +34.8%、AtomicReplace +44.1%；1 GiB 某些 Checkpointed 组合改善，但 AtomicReplace 没有普遍改善。
因此该环境支持“不要统一提高窗口”，不能推出独立 read=8/write=16 最优。
证据：[environment](../../benchmarks/nfs-inflight-comparison/results-20260914/environment.json)、
[summary](../../benchmarks/nfs-inflight-comparison/results-20260914/summary.csv)、[samples](../../benchmarks/nfs-inflight-comparison/results-20260914/samples.csv)、
[口径与分析](../../benchmarks/nfs-inflight-comparison/results-20260914/report.md)。挂载 Local 路径与用户态 NFS 路径必须分别统计。

[nfs-rs 版本调研](nfs-rs-latest-real-performance.md) 已指出若干旧 window4/window16 文件实际 backend 仍为 4/8，不可据文件名调参。
它还记录过整文件末尾一次 COMMIT 导致 1 GiB 传输 RSS 约 1.1 GiB，改成有界 durability batch 后约 97 MiB；
NFS 内存边界必须包括保留用于 verifier 恢复的 payload，不能仅计算 read/channel/write 在途窗口。
这是历史问题证据，不代表当前统一采用 commit batch=8；当前 checkpoint 路径的保留与释放节奏应以 client/staged writer 和 ADR-0002 后续更新为准，补测须实际记录高水位。
该记录使用另一组服务器及包含连接/读回校验的计时口径，不能直接与 FAS 上述毫秒数比较。

### CIFS

2026-09-16 FAS2750、SMB3.1.1、WhenRequired、普通 READ/WRITE 无签名的 40 MiB warm-source 单文件实验，协商读写上限 1 MiB、请求字节预算 32 MiB。
读写同时从 8 调到 16：Checkpointed 中位 522.071→489.293 ms，AtomicReplace 481.965→484.038 ms；各组 7 个正式样本且区间重叠。
这支持保持 8/8 作为该场景起点，不支持全局提高到 16，也不能证明非对称窗口的优劣。
证据：[environment](../../benchmarks/cifs-depth-40m/results/environment.json)、[samples](../../benchmarks/cifs-depth-40m/results/samples.csv)、
[报告](../../benchmarks/cifs-depth-40m/report.md)。warm 指内核挂载源页驻留；用户态 CIFS 仍需网络 READ。
签名/加密、credit、服务器类型和 source 缓存必须分别记录；不要将无签名环境的默认值外推为签名必需环境最优值。

### S3

本轮没有找到可同时追溯当前 role 行为、独立读写窗口和 CPU/RSS 的完整 S3 默认值矩阵。
README 的“4～8 吞吐拐点”只能作为待验证线索，不能升级为当前实现实测结论。
[现有矩阵脚本](../../tests/lab/run-inflight-benchmark.sh) 通过环境设置 depth，但
[当前 example limits](../../examples/support/copy.rs) 固定 `8, chunk*8, 8`；环境变量设为 16 不保证 source 实际并发 16。
历史数据应按保存的 commit/source archive/binary 判定，不能用当前 runner 推定旧二进制行为。
Standard S3、StorageGRID、DXN、ONTAP object bucket、ONTAP NAS bucket 应分组；NAS 不支持 multipart 时须单独测 streamed PUT。
TLS、checksum/MD5、single PUT、multipart、UploadPartCopy 也须分开，native 结果不能证明本地 payload pipeline 的调优收益。

### HDFS

2026-09-15 HA/Kerberos、两个 DataNode 的 zero-copy packet 比较使用 2 MiB 应用块和 8 个有序 source reads，关闭读回，连接建立排除计时。
1 GiB Direct 为 208.83 MiB/s、AtomicReplace 204.20、Checkpointed 199.15；后者含 64 MiB hsync/recovery 边界。
这说明该实现和环境下的可达吞吐，不证明真实默认 read=4 应改为 8；未形成独立窗口 sweep，也缺少该矩阵的 CPU/峰值 RSS 联合结果。
证据：[summary](../../benchmarks/hdfs-native-comparison/results/20260915-zero-copy/summary.csv)、
[raw](../../benchmarks/hdfs-native-comparison/results/20260915-zero-copy/raw.csv)、
[确认样本](../../benchmarks/hdfs-native-comparison/results/20260915-zero-copy/one-gib-confirmation.csv)、
[环境与口径](../../benchmarks/hdfs-native-comparison/results/20260915-zero-copy/REPORT.md)。
zero-copy 优化前后未交错运行，集群状态变化仍混在差异里；不能把全部变化归因于消除复制。

## 下一轮最小补测矩阵

先固定 channel=4 和一种持久化/校验策略，分别扫 read 与 write，再确认少量组合；最后比较 channel=2/4/8，避免一开始做所有参数的笛卡尔积。

| Backend/环境 | 独立候选 | 必须额外控制 |
| --- | --- | --- |
| Local：实际目标 SSD/HDD、ext4/XFS，tmpfs 只作 CPU 上界 | read 1/2/4/8；write 1/2/4/8；read chunk 256 KiB/1/2 MiB；write 上限 1/2/4/8 MiB | 源冷/热、同盘/跨盘、blocking pool；8 MiB ceiling 不等于每次真的写 8 MiB |
| NFS：FAS 与 Linux nfsd，v3/v4.1，目标生产 RTT | read 2/4/8；write 4/8/16；按协商上限测 chunk，小块 64/256 KiB 作对照 | session slots、pNFS、WRITE commitment、COMMIT batch 与 payload 保留；24 仅在大文件证据支持后扩展 |
| CIFS：FAS 与目标 Samba/Windows 环境 | read 4/8/16 × 分阶段 write 4/8/16；chunk 256 KiB/协商上限 | credits、签名/加密、FLUSH、源缓存；文件并发另设轴 |
| S3：每种实际 endpoint/profile 分组 | 有序 read 1/2/4/8；multipart write 1/2/4/5/8；part 8/16/32 MiB；native 2/4/6/8 | TLS、part 数量下限、body retry、MD5、single PUT/NAS/恢复上传与新上传 |
| HDFS：目标 HA/Kerberos 集群 | read 2/4/8/16；chunk 512 KiB/1/2 MiB；有序写保留，单独测底层 packet 配置 | replication、block placement、DataNode encryption、hsync、NameNode 固定成本 |

所有环境先覆盖 4 KiB、4 MiB、40 MiB、1 GiB 及非整块尾部；文件级并发至少分别记录 1 和代表生产值，不能混作每文件读写深度。
S3 native 的 8 路候选需在隔离实验实现中允许超过当前内部 ceiling=6，并断言实际有效值；仅传入 operations=8 仍然只会得到最多 6 路。
精确记录 backend 解析值、请求上限、实际 chunk/part、观测的峰值在途读/写请求、channel 高水位、读写等待、重排序缓冲、durability/retry 保留量。
同时保存 commit、lockfile、binary hash、脱敏环境、原始样本、缓存状态、校验/metadata/持久化口径；仅记录环境变量不算 effective config 证据。

选择默认值时共同评估吞吐、CPU 秒/GiB、峰值 RSS、p50/p95 和取消收尾时间。
可以预先约定“吞吐距最好有效组合不超过 5% 时，优先 CPU/内存更低者”作为工程准则；5% 是待团队采用的阈值，不是已有实验结论。
先做少量试测，再对候选交错轮换至少 7 次；保留全部失败与波动，不凭单次峰值选择默认。

改造验收须另测慢 writer、首块延迟导致读乱序完成、错误/取消、跨 part 边界和恢复：source 输出仍有序，所有内部队列有界，分片不复制 payload。
内存统计覆盖 source、channel、writer、NFS COMMIT/retry 和 SDK buffer；共享 `Bytes` 小切片可能保留大 allocation，不能只累计 slice 长度。
完成这些检查与同口径 old/new 比较后，再把实际环境支持的值固化到 backend 默认配置与文档。

## 用户指定环境与可执行基准入口

2026-10-15 用户指定测试机 `10.131.5.223` 及下表存储。本节只核对仓库记录，未连接远端；可达性、运行负载和现行凭据均未验证。
主要依据为 [.claude 环境总表](../../.claude/docs/test-environments.md)，其中更新后的状态应优先于根目录历史 `lab-environments.md`。

| 环境 | 数据面与隔离范围 | profile 位置与待确认项 |
| --- | --- | --- |
| FAS2750 | source LIF `10.128.61.200`、target LIF `10.128.61.201`；NFS `/ontap_lisaauto_nfs`；SMB share `ontap_lisaauto_cifs`；只建独立测试子目录 | `.claude/skills/e2e-nfs/.env`、`e2e-cifs/.env`；NFSv3 须保留端口，核对 `noresvport=false`；两 LIF 不等于两台独立设备，必须隔离路径 |
| DXN-1 S3 | `http://10.131.7.201:8184`，HTTPS `:8182` 自签；`compat=dxn`；测试桶 `data-mover-test` 下独立前缀，原桶不得启版本 | `.claude/skills/e2e-s3/dxn/.env`；临时桶另受已有范围和 `LocationConstraint=magnascale:tos_placement` 约束 |
| DXN-2 NAS | NFS `10.128.136.211:/nfv3_v4:/data-mover-test`，仅此子树；历史支持 v3/v4.0 | `.claude/skills/e2e-nfs/dxn-v3/.env`、`dxn-v40/.env`；总表记录 2026-10-12 ThinkFS 故障和当时停止远端操作，先核实恢复；没有获确认的 SMB 测试 share |
| openfs | `10.131.5.221:/m1-source`、`:/m1-target`；node1 对 source 的历史 200G 入口 `192.168.131.214:/m1-source` | 新建唯一测试子目录，不改既有 53M 树；target 的直连 export、可写目录边界待核实；SMB share 名为 `m1-source/m1-target`，但历史 guest 登录失败，未确认有效认证 profile |
| StorageGRID | **明文 HTTP** `http://10.131.7.236:10443`；`compat=sg`；新版总表指定专属租户下 `data-mover-test`、`data-mover-test-ver` | `.claude/skills/e2e-s3/storagegrid/.env`；旧记录曾允许 `storage-bucket1/data-mover-test/`，新版明确将旧桶列为他人数据、不碰，应按新版专属测试桶隔离 |

本工作区上述真实 `.env` 均不存在；只有通用 `.env.example` 模板，不能当作已配好认证。
需要的键名为 NFS 的 `NFS_HOST/NFS_PORT/NFS_EXPORT/NFS_UID/NFS_GID/NFS_OPTIONS`，
CIFS 的 `CIFS_REAL_SERVER/CIFS_REAL_SECOND_SERVER/CIFS_REAL_SHARE/CIFS_REAL_USER/CIFS_REAL_PASS`，
S3 的 `S3_HOST/S3_BUCKET/S3_AK/S3_SK/S3_COMPAT/S3_USE_HTTPS/S3_TLS_VERIFY/S3_PREFIX`；此处不记录值。
旧环境总表含明文认证信息，执行时只在受控进程内加载，禁止直接回显原文或将带认证的 URL 放入命令参数、报告。

既有入口（以下是调用形式，不代表已运行）：

- `cargo build --release --locked --example nfs_mount_comparison --example cifs_mount_comparison --example local_copy_comparison --example transfer_resume`：本地生成待部署产物；目标机兼容性另验。
- `timeout <秒> ./nfs_mount_comparison --transport client --policy checkpointed --source <隔离源URL> --destination <隔离目标URL> --path fixture.bin --concurrency 1`；用 `DATA_MOVER_NFS_READ_INFLIGHT` 与 `DATA_MOVER_NFS_WRITE_INFLIGHT` 独立扫参。
- `timeout <秒> ./cifs_mount_comparison --transport client --signing when-required --policy checkpointed --source <隔离源目录> --destination <隔离目标目录> --path fixture.bin --concurrency 1`；认证由环境传入，两个方向单独扫参。
- S3 先用 `S3_ENV=<profile路径> timeout <秒> bash .claude/skills/e2e-s3/scripts/staged_matrix.sh` 做契约验收；这是带播种和清理的功能矩阵，不是独立读写最优值基准。`transfer_resume` 可复用其 profile 与隔离前缀，正式吞吐 runner 需明确 streaming/native 路径并保存 CPU/RSS。
- `benchmarks/nfs-inflight-comparison/run.py --binary <路径> --output <新目录>` 和 CIFS 历史 runner 含硬编码旧 export、挂载点或输出目录，不能原样用于新环境；先参数化并审计清理边界。`tests/lab/run-inflight-benchmark.sh <run-id> <csv>` 同样绑定 PVE lab，不会自动使用这五个环境。

测试机历史为 RHEL9/glibc 2.34/32 核，无 gcc、不能访问 GitHub；当前总表建议在本地构建再部署，
旧 [node1 构建记录](../../claude/memory/m1-source-nfs-lab.md) 留有 WSL 构建加 `p7-base/glibc234-shim.c` 的方式和远端 `/root/dm/p7-base/{legacy,role,role-c0,run.sh,BUILD-INFO}`。
shim 不在当前仓库，旧二进制也不能替代本轮源码；需先只读检查远端工具链、动态符号与可复用构建资料，再选择兼容 glibc 2.34 的构建环境。
更早 vendor 离线构建配方依赖 gcc，不能假定当前仍可执行。SSH 认证由用户既有配置提供，本仓库未发现 node1 专用 SSH key 路径。
各条命令都包 timeout；目录/前缀带 `data-mover-<时间戳>-<唯一ID>`，清理仅作用于本次精确根并记录未清理项；不修改共享设备全局配置或清全局缓存。

### 2026-10-15 DXN 实时只读核验

用户随后明确授权使用设备管理认证查找资源，必要时创建资源。本子调查只执行 SSH 管理查询、HTTP GET/HEAD、TLS 证书元数据及 RPC/export 查询，未创建用户、key、bucket、export、目录，也未运行传输负载。

DXN-1 `10.131.7.201`：管理 SSH 成功，主机名 `node11`，管理身份实际 uid=0，可用 `/bin/radosgw-admin`、`/bin/ceph`；`ceph health` 返回 `HEALTH_OK`。
`radosgw-admin user list`、`user info --uid=data-mover-test` 确认专用用户仍存在、未暂停、max_buckets=5、持有一对现成 S3 key。
`bucket list --uid=data-mover-test` 仅列出 `data-mover-test`；`bucket stats --bucket=data-mover-test` 确认 owner 正是该用户、tenant 为空、placement 为 `tos_placement/`，
main、multipart、multimeta 都为 0 对象/0 字节。HTTP `:8184` 正常返回 S3 XML，服务标识为 `MagnascaleTosServer`。
因此现有资源适合继续以唯一前缀隔离，无需新建用户或 key；versioning 状态仍须通过签名 GetBucketVersioning 单独核实，不能从 stats 的缺失字段推断。
现成 key 已按主线程要求在受限进程内提取到临时 `dxn-s3.env`（0600，父目录0700），此报告不记录密钥或完整临时认证路径。
管理 help 明确支持 `user create`、`key create`、`bucket list/stats`；若以后需新建隔离身份，可用 `radosgw-admin user create --uid=<唯一测试用户> --display-name=<描述> --max-buckets=<上限>`，
其生成的认证输出直接保存到受限文件，随后以 S3 API CreateBucket 并指定 `LocationConstraint=magnascale:tos_placement`；本次未执行这些创建操作。
zonegroup 实测名为 `magnascale`，placement target 为 `tos_placement`，默认 placement 为 `/`，保留显式 LocationConstraint。

DXN-2 `10.128.136.211`：RPC 实测 NFSv3/v4 在 2049、mountd 在 2050、nfs_acl 在 2052，`showmount -e` **只返回 `/caohui *`**，旧 `/nfv3_v4` 记录已不适用。
这表明当前有 NFS 服务，不能继续引用“无2049”的历史故障作为现状；但 `/caohui` 的所有权、可写范围、底层文件系统健康均未确认，未尝试写入。
HTTPS GET 根页标识 `Lenovo MagnaScale Manager`；HEAD 根页返回 gunicorn 500，不能据此判断整个数据面健康。
公开 TLS 证书 CN 为 `*.controller.dsm`，issuer 为 LenovoNetapp；此通用证书只能确认产品线索，不能绑定到当前物理设备。
管理 SSH 被原有 known_hosts 拦截：收到的 ED25519 指纹为 `SHA256:mkhSNEzeJ08xVnXdku4Szv7+HD8HTkvqOFkAYCF1W3o`，与旧 ECDSA 记录冲突。
本次没有绕过校验或向身份未确认的 HTTP 登录接口提交密码；须先由可信管理来源核对新指纹，才能进一步确认设备身份、NAS 创建命令、SMB share 与认证来源。
现阶段 DXN-2 的具体创建方法及可用 SMB 身份仍为真实阻塞，不凭旧 export 或管理 SSH 用户推定数据面权限。

### 认证资料补查与主线程资源准备进展

主线程已在 StorageGRID 使用用户提供的 S3 认证新建独立专用桶 `data-mover-pipeline-20261015-i3dfpj`，没有使用或修改旧 `storage-bucket1`。
DXN-1 主线程已使用前述现有专用桶准备 benchmark；准备资源不代表已经得到性能测量结论。

FAS SMB：在既有本地验证工作树 `/tmp/dm-codex-validation/.claude/skills/e2e-cifs/.env` 找到已登记的真实 profile，
私下校验其 endpoint 属于已知 FAS LIF、share 为专用 `ontap_lisaauto_cifs`、所需认证字段完整非空，安全提取为临时 `fas-cifs.env`（0600）。
本子任务没有连接 SMB 测试认证，也没有修改用户或密码；是否仍有效由后续隔离预检确认，报告不记录身份或密码值。
因此“当前工作区缺少真实 `.env`”仍属实，但不必据此立即在设备上创建新身份。

openfs SMB：环境总表、历史记录以及已定位的既有 skill profile 中没有找到经验证可用的数据面认证；
文档只有匿名登录失败记录，不能把设备管理 SSH 认证直接视为 SMB 认证。本子任务未生成 `openfs-cifs.env`，未猜测或试探密码。
如继续安排 SMB 性能测试，需先由可信管理接口定位现有可用 SMB 身份，或按用户授权创建仅覆盖本轮测试目录的专用身份和 share。

## 2026-10-15 实现后的真实测量与定参

实现已移除普通、observed、expert 和 tar 请求里的 `InflightLimits`；读取通过 backend 的有序 stream 接入容量 4 的 channel。
backend 各自限制读取/写入窗口，S3 multipart 和 single PUT 聚合共享 `Bytes` 分片，HTTP body 的重试重新播放分片，不先合并 payload。
S3 native 的 6 路窗口仍独立存在，本轮 streaming 数据不能用来调整 native 默认值。

所有正式测量在用户指定的 node1 执行：Xeon Gold 6526Y、32 逻辑 CPU、376 GiB RAM、RHEL9/glibc 2.34；Local 根为 XFS。
FAS/DXN/StorageGRID 使用 1 GbE 主接口；openfs 使用已验证的 `192.168.131.214` 200 GbE 直连，不与 `10.131.5.221` 的 1 GbE 结果混合。
用户态 NFSv3 和 SMB 没有换成内核挂载来计时，没有改设备全局设置或清全局缓存。
测量固定单文件并发、warm source、同一隔离目标的重复覆盖；不代表冷盘、批量小文件、多个并行 transfer 或 WAN 的最优值。

40 MiB+137 B 扫描独立读/写窗口；128 MiB+137 B 复核主要候选。两种策略分别计量，每组先预热一次，再交替正反顺序正式运行 7 次。
1 GiB+137 B 另做长传输/RSS检查：Local/openfs 为每组 3 次，两个 S3 为每组 1 次，不能把后者当稳定的吞吐排名。
所有路径强制 client-shaped streaming，不使用 CopyObject/UploadPartCopy 近路。
耗时仅包含 transfer；连接及单独的内容/适用元数据读回不在计时内。CPU/RSS 是整个进程的 `/usr/bin/time` 值，含连接与 metadata，正式样本不做外部读回。
4 KiB 和 4 MiB+137 B 的两个 S3 单 PUT 路径另做写入/读回验收，覆盖跨 Local 2 MiB 片段的 single PUT，不混入吞吐统计。

### 本轮采用的基线

| Backend | 读/写 | 决策 |
| --- | --- | --- |
| Local 示例入口 | **4/2**，原为 4/8 | warm-XFS 大文件 CPU/RSS 显著下降；typed factory 仍显式配置窗口，不谎称所有 Local 实例具有隐式默认 |
| NFS | **4/8**，保留 | FAS、DXN-2 与 openfs 的拐点不同；4/8 保持跨设备折中，不按 IP 写特例 |
| CIFS | **8/8**，保留 | FAS 128 MiB Checkpointed 下 read=8 优于 4，DXN-2 guest 则相反；不强行统一最优，没有提高到 16 的一致证据 |
| S3 streaming | **2/4**，原初始基线 4/4 | DXN 和 StorageGRID 的 128 MiB 均提高吞吐并减少 CPU/RSS；保留环境覆盖 |
| HDFS | **4/1**，保留 | 本轮用户未指定 HDFS 集群，未用历史结果冒充新实测 |

以下为 Checkpointed 中位数，CPU 是进程 CPU 秒，RSS 是每次进程峰值的中位数。

| 环境 / 大小 | 配置变化 | MiB/s | CPU 秒 | RSS MiB |
| --- | --- | --- | --- | --- |
| Local / 128 MiB | 4/8 → 4/2 | 1422.8 → 1486.1 | 0.49 → 0.23 | 63.0 → 46.6 |
| Local / 1 GiB，n=3 | 4/8 → 4/2 | 1810.0 → 1885.9 | 4.16 → 1.79 | 75.6 → 47.4 |
| DXN S3 / 128 MiB | 4/4 → 2/4 | 65.85 → 70.70 | 0.71 → 0.59 | 140.5 → 113.3 |
| StorageGRID / 128 MiB | 4/4 → 2/4 | 73.39 → 80.84 | 0.60 → 0.54 | 133.8 → 94.9 |

S3 2/4 不是每个大小吞吐都更高：1 GiB 单样本中 DXN 4/4 快约 2%，StorageGRID 4/4 快约 6.5%；
后者 2/4 的 RSS 约 160.5 MiB、4/4 约 208.2 MiB。该结果支持资源/吞吐折中，不支持宣称单一全局最优。
以大对象为主的部署可以覆盖 read=4；不依据单次 1 GiB 差异做对象大小自适应规则。
openfs 在 128 MiB 时 write=16 有收益，但 1 GiB 的 4/8 与 4/16 中位吞吐约 1696/1695 MiB/s，仍保留 4/8。

原始数据：[逐组合汇总](backend-pipeline-20261015/summary.csv)、[环境与 binary/fixture hash](backend-pipeline-20261015/environment.json)；
同目录每个 `*-40m.csv`、`*-128m.csv`、`*-1g*.csv` 保留预热和正式样本。
`sample_p95_ms` 只是小样本的最近秩统计量，n=7 时接近本组最大值，不是生产总体 p95。
复现入口为 [benchmark](../../tests/lab/tools/backend_pipeline_benchmark.rs)、[窗口扫描](../../tests/lab/run-backend-window-sweep.sh)、
[汇总脚本](../../tests/lab/summarize-backend-window-sweep.py)。可用 `WINDOW_CANDIDATES='2:4 4:4'`、`ROUNDS`、`SWEEP_POLICIES` 缩小复核范围。

node1 的 glibc 低于本机构建环境；本轮 release binary 链接仅用于 lab 的 [两个 C23 wrapper](../../tests/lab/glibc234-shim.c)，
用 `readelf --version-info` 验证最高导入 GLIBC_2.34。wrapper 恢复旧整数解析语义，不作为生产构建或可移植性修复提交到库。

### 实时资源与异常记录

FAS 使用既有 NFS `/ontap_lisaauto_nfs` 和 SMB `ontap_lisaauto_cifs`，仅创建唯一测试子目录。
openfs 当前没有 `/m1-target` export，因此在 `/m1-source` 下用两个独立子目录，没有创建新 export 或遍历业务树。
DXN S3 复用 `data-mover-test` 中唯一前缀；StorageGRID 新建独立桶 `data-mover-pipeline-20261015-i3dfpj`，不读写旧 `storage-bucket1` 中的对象。
测试机工作目录 `/root/dm-pipeline-20261015.i3DFPj` 保存 binary、fixture、原始逐次日志，认证另用受限文件且不进仓库/报告。

DXN-2 的新主机指纹由用户确认后，通过单独 known_hosts 固定，未删改全局记录。
当前设备为 MagnaScale 5.0.0、2026-10-13 重装后的 node211；Ceph HEALTH_OK、TFS rank/IOA 健康，NFS/SMB 服务 active。
NFS `/caohui` 为 no_root_squash、RW，`NFS_Commit=FALSE` 原配置未改；其策略耗时和别的设备分组，不据协议成功声称断电持久化已验证。
首个 40 MiB Local→NFS 播种在 OwnershipMode 阶段被拒绝，未发布目标；随后 4 KiB 两次、同一 40 MiB 命令两次及完整窗口扫描均通过。
初始失败见 [记录](backend-pipeline-20261015/dxn2-preflight.txt)，不计作成功样本、不据此改 uid/mode 逻辑，也没有发额外 SETATTR 探测。

openfs 首次跨 Local/NFS 读回还发现 14 ns 的 mtime 量化差异。后续 NAS fixture 使用整秒 mtime 控制输入；
这不证明纳秒时间戳精确保真，未放宽库内 timestamp 比较，也未把该次完整 metadata 验证标成通过。

DXN-2 的精确管理凭据只尝试正式登录一次，未取得可用 DSM JWT，未猜测/重置其他账号。
运行中的用户创建 API 会将 request.data 写 INFO 日志，不能为自动创建测试身份而把密码送入该日志。
已有 `lisa-auth-test` 允许 nobody 定向写入，产品目录 rights=777，匿名 tree-connect 已通过；优先使用该测试共享的唯一随机数据子目录，
通过 example/lab 专用 `CIFS_GUEST_POLICY=allow_unsigned` 显式选择，不改服务器 guest 配置，也不改变库的 GuestPolicy::Deny 默认值。

DXN-2 SMB 随后已通过 4 KiB、40 MiB、128 MiB 播种及内容/适用元数据读回，完成两种策略的 40 MiB 全窗口扫描和 128 MiB 4/8、8/8 复核。
128 MiB Checkpointed 的 4/8、8/8 分别约 43.80、40.60 MiB/s，FAS 同大小则 8/8 更好，因此只把 4/8 列为 DXN-2 guest 单环境候选；
不按 IP 特判库默认值，也不把 unsigned guest 的 CPU 成本外推到必须签名/加密的环境。
openfs SMB 在显式 guest 预检中返回 `STATUS_LOGON_FAILURE (0xc000006d)`；没有尝试猜用其他设备密码，仍缺经验证的数据面身份/管理入口。

本轮合计 **929 个正式样本、139 个预热样本**，分组和每组 n 见 summary.csv；另有不计入吞吐统计的功能预检与失败记录。
最终 `cargo test --workspace`（library 1661 passed、9 ignored；integration/doc 均通过）、全目标 Clippy `-D warnings`、新增 benchmark 3 个测试、
架构 guard 及现有 Python runner contracts 均通过。未声称 ignored 的所有远端功能矩阵或 HDFS 集群测试已执行。

临时认证副本与诊断 helper 源码已从本机/测试机清理，未删除或修改原有 credential store、账号、key 或全局 known_hosts。
**当前保留**隔离 fixture、各自唯一目录/对象前缀、新建的 StorageGRID 专用桶和测试机 artifact 目录供后续复测，不是已清空状态。
保留范围仅为本轮 namespace token 对应的随机测试数据；未读写业务对象或更改共享设备全局配置。
