# Checkpointed 乱序树形 hash 与范围恢复设计

状态：设计已确认，生产实现与验证结果见 §14；2026-10-22。

目标：中断续传不重读 checkpoint 已覆盖的源端范围；最终一致性比较直接复用源端计算的完整 BLAKE3，只读回目的端。没有成功后长期保存 hash、结果数据库或永久 sidecar 的需求。

## 1. 结论

采用一个 backend 无关的 `RangeHash` module：按文件逻辑偏移接收数据，维护标准 BLAKE3 子树和完成范围；由各目的端 adapter 证明哪些数据可恢复。读块、hash 子树、写块、checkpoint 间隔和 pNFS 条带分别独立。

不采用“给顺序 Hasher 加一个全文件位图”，也不改成自定义 Merkle digest。完成范围可以使用窗口位图/区间压缩；最终摘要必须与 `blake3::hash(file)` 完全一致。

实施前先核实定位读写 interface，再扩展持久化恢复。调研基线中，`read_positioned` / `write_positioned` 只在 adapter 上存在，普通 producer 调用顺序 `read`，续传校验仍从零读取源文件。本轮新路径的实际启用范围见 §1.1 与 §14。

本设计的完整依据分为：

- [各 backend 源码和协议调研](checkpointed-range-hash-backend-research.md)。
- [BLAKE3 格式、边界规则和隔离实验](checkpointed-range-hash-crypto-research.md)。
- 本文 §3 的 pNFS 本轮诊断及既有抓包复核。

调研阶段没有修改生产代码、Cargo 依赖、服务端配置或性能门限；实施阶段不得放宽既有门禁。

### 1.1 模式隔离（实施硬约束）

| 模式/场景 | Payload 交付顺序 | Hash 恢复、位图、intent、临时发布状态 |
| --- | --- | --- |
| Checkpointed 且实际符合恢复门槛 | 源支持定位读取时允许乱序；目的端不支持定位写入时有界重排 | 启用 |
| Checkpointed 小文件、单源块、不支持恢复的目的端 | 保持原有顺序路径和原有校验 | 不为本功能新增记录/请求 |
| AtomicReplace | 保持原有顺序交付；底层并发预读/条带并发不变 | 不启用 |
| Direct | 保持原有顺序交付；底层并发预读不变 | 不启用 |

纯数学 hash helper 可内部复用，但本次不能通过共享 helper 改变另外两种模式的路由、读写顺序、持久化、元数据请求数或校验语义。普通 `transfer` 是本次新路径的启用入口；expert 两半 interface 保持既有显式流契约，不能由内部改动悄悄切换为乱序。

## 2. 基线与保证范围

data-mover 源码基线为 `6aa1633e749c0f5255c82d628f966102bcc39e81`，已由 PR #159 合入 `main` 的 `d646973`；两者源码 tree 相同。生产依赖为 registry `nfs-rs 0.8.6`、`blake3 1.8.5`。

本机另一个 `../nfs-rs` checkout 的 HEAD 为 `bf3807fa70a66a3632b5a5a0527e622ec7301e8e`，但有大量用户未提交修改，包括 pNFS 实现。仅只读调查，没有 checkout、同步索引、修改或合入这些内容。引用其当前源码的结论不自动适用于生产的 0.8.6。

承诺是：在源内容绑定未变、目的端恢复证据有效的前提下，新 checkpoint 中可恢复的逻辑字节范围不会被新的源端 READ 覆盖。它不承诺服务器内部磁盘/条带不会预读相邻字节，也不把尚未提交 checkpoint 的预读数据称为已完成工作。

Local/NFS/CIFS/HDFS 的 file id、长度和时间戳变化检测不等于不可变快照。最终匹配证明目的端等于传输观察到并保留 hash 的源字节；需要时间点一致性时，使用不可变版本/快照或适当的外部写入约束。检测到源版本变化必须失败或明确重启，不能把旧 hash 和新源内容拼接。

## 3. pNFS 真实环境调查

### 3.1 环境与安全范围

用户指定 AFX 测试机 `10.128.56.17`，本轮登录确认 Linux `5.14.0-427.13.1.el9_4.x86_64`，机器空闲，共享测试锁无持有者。已有 `/mnt/dir1` 挂载为 `192.168.9.101:/pnfs_test2`，协商 `rsize=wsize=65536`。

没有修改或卸载这个挂载。诊断用 Rust 用户态客户端访问同一导出，只创建本轮唯一 `release-hash-research-20261022-8OKLEc*.bin`，持有 `/tmp/terrasync-lab-tests.lock` 和 `/tmp/terrasync-lab-performance.lock`，并限制运行时间。四个文件均已删除，客户端 umount 成功，锁已释放。

仓库旧文档还记录了 `client1/.232` 和 `192.168.100.201:/fg01`。本轮 `.232` SSH 密钥被拒；旧 `.201:/fg01` 从本机返回 early EOF、从新测试机连接超时，未把它当作本轮已验证环境。

`nfs-rs/tests/lab/prepare-run.sh` 已尝试，但执行机缺少另一套共享 CI 环境的专用 SSH 私钥，在首个主机认证失败；未在那些主机创建数据。清理脚本也已执行，遇到相同认证限制。因此下面是独立隔离诊断，不是完整 lab/release gate。

凭据没有写入文档或原始证据。原始日志、pcap 和临时程序不加入 Git；仅记录摘要和可复核的标识。

### 3.2 本轮协商和真实 READ

使用 registry `nfs-rs = 0.8.6` 构建隔离程序，不替换 data-mover 依赖。

| 环境 | 用户态协商 READ / WRITE | 本轮验证 |
| --- | ---: | --- |
| `.17` → `192.168.9.101:/pnfs_test2` | 65,536 / 65,536 bytes | 挂载、独立 DS 数据路径、混合分块、乱序返回、全内容校验和清理 |
| 本机 → `10.128.56.160:/nfsrs_pnfs_b` | 1,044,480 / 1,044,480 bytes | 只读挂载/协商、正常 umount；没有负载测试 |
| 本机 → `10.128.56.161:/nfsrs_pnfs_b` | 1,044,480 / 1,044,480 bytes | 同上 |

三者均报告 `pnfs=true`，但能力标志本身不能证明数据真的经过独立 DS。

AFX 每轮新建 `16 MiB + 123 = 16,777,339` 字节非零确定性数据。写入遵守协商上限；读取轮流使用 **32,771 / 49,157 / 65,536** 字节，8 个并发请求，处理短读续读；分别使用顺序交付和完成顺序交付，每轮每种方式 342 个逻辑范围。逐字节校验使用绝对文件偏移生成独立期望值。

| Trial | 实际数据路径 | 顺序交付秒 | 完成顺序交付秒 | 完成顺序中 index 不等于交付位置的次数 |
| --- | --- | ---: | ---: | ---: |
| 初始轮 | MDS `.101`，退化布局 | 0.056861 | 0.048659 | 38 / 342 |
| b | DS `192.168.9.103` | 0.069350 | 0.067044 | 169 / 342 |
| c | DS `192.168.9.102` | 0.065248 | 0.063522 | 181 / 342 |
| d | DS `192.168.9.104` | 0.063512 | 0.063493 | 170 / 342 |

所有顺序交付轮的相同计数为 0。三轮非退化抓包中，各 DS 约有 17 MB 出站写流量和 33 MB 入站读流量，MDS 仅约 11 KB 控制流量；这些是含协议开销的网络字节，不是精确文件字节。四轮抓包内核丢包均为 0。

此诊断证明：生产依赖可在真实 DS 路径上对非 hash 对齐的逻辑范围并发读取，完成顺序确实可变。文件小、缓存未清除、轮次少且计时包含内容检查，不能据此宣布吞吐提升，也不证明同一文件同时跨多个 DS。

### 3.3 本轮复核的新机器既有 20 GiB 证据

另只读复核 `.17:/tmp/nfs-rs-pnfs-repeat.17QexE/` 中已有的 `probe-controlled` / `probe-fast` 日志和后者压缩抓包；没有重新运行或修改这些历史实验。

| 既有实验 | 顺序 A 秒 | 并行 4 秒 | 顺序 B 秒 | 日志中的全内容校验 |
| --- | ---: | ---: | ---: | --- |
| controlled | 43.814269 | 21.857005 | 43.874271 | 通过 |
| fast | 43.524465 | 20.399211 | 42.962749 | 通过 |

按 `workload.log` 的相位时间窗口，解压 fast 抓包，统计来自 TCP/2049、原始包长度大于 8192 的入站包：

| 相位 | `.101` bulk 包 | `.102` bulk 包 | `.103` bulk 包 | `.104` bulk 包 | 同一 1 ms 桶出现多个 endpoint |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential_a | 172608 | 137562 | 206049 | 171736 | 0 |
| parallel_4 | 181184 | 145785 | 218120 | 181326 | 12966 |
| sequential_b | 173103 | 137778 | 206913 | 171612 | 0 |

这比只看 TCP 连接数更有力：既有并行阶段存在多个 endpoint 的细时间粒度 bulk 流量重叠，而顺序阶段没有。它仍不是逐请求延迟分布、物理节点身份或生产 hash 方案的 A/B benchmark。统计按日志时间窗口和网络包过滤，未逐 filehandle 归属所有历史包。一个 IP endpoint 不等于一台物理控制器，1 ms 时间桶也不是微秒级同时执行证明。

复核文件 SHA-256：`full.pcap.zst = daad691740fb58b3b84e2935b6345027f299f806a564bd6d0f6af6ea29b40c9c`；既有实验 binary SHA-256 为 `fc04dc5965db1522704150d02a83b84f9f69c8417f1d735819b0c8199075283f`。

### 3.4 两层顺序等待必须分开

1. **nfs-rs 内部**：调查的较新 dirty checkout 中，`pnfs_read_layout` 在 `src/nfs41/pnfs_io.rs:874–899` 使用 `try_join_all`，再按条带顺序拼接；一条慢 stripe 会拖住整个逻辑 READ。逻辑偏移和 dense/sparse DS 偏移的区别见 [RFC 8881 §13.4](https://datatracker.ietf.org/doc/html/rfc8881#section-13.4)。
2. **data-mover 外部**：NFS adapter 的顺序读使用 `FuturesOrdered`，定位读使用 `FuturesUnordered`；普通 transfer 又消费顺序字节流。

第一阶段移除第二层不必要的等待，不承诺已移除第一层。真正把单个 DS stripe 提前交付需要 nfs-rs 新的流式 interface，另行评审；不能简单把底层 `try_join_all` 换为无序集合而丢掉逻辑位置、短读和错误语义。pNFS layout、DS 选择、刷新及 fallback 继续留在 NFS adapter/依赖内，不泄漏进通用 hash module。

## 4. Backend 适配决策

下表是代码调查结论，不是本轮对所有设备做了实测。完整源码/协议链接在 backend 调研文档。

| Backend | 当前分块/执行事实 | 新恢复形态与限制 |
| --- | --- | --- |
| Local | 默认读 2 MiB、写上限 8 MiB；小读块不强制拼成大写块；定位读写已存在 | 可恢复离散持久化范围；必须停止对已记录离散范围做破坏性截断；sync 数据后提交元数据 |
| NFS v3 / v4.0 / v4.1 | R/W 独立协商，各受 adapter 1 MiB cap；默认深度 4/8；定位读写已存在 | 区间覆盖与 WRITE/COMMIT/verifier 证据结合；不能把 WRITE 成功或文件长度当作 durable bitmap |
| CIFS | 单次读写上限来自协商/credits/facade，默认深度 8/8；支持定位读写 | FLUSH 后才能确认恢复范围；现有 facade 缺少截断不等于旧尾部可复用 |
| HDFS | 读写 role chunk 为 2 MiB；128 MiB HDFS block 是另一层布局；目的端顺序 append | 只恢复连续 append 前缀；乱序源数据须有界重排；lease recovery 的额外尾部需 hash intent 覆盖 |
| S3 object profiles | GET 默认 8 MiB；stream part 默认至少 8 MiB并随文件大小调整；恢复沿用原 upload 的 part 大小 | 以冻结的 part geometry 和 ListParts 证明离散完整 part；输入可按任意源块拼接，但只在完整 part 级复用 |
| S3 NAS | 无 multipart；有界 single PUT，最后一字节留到 publish；当前不支持恢复 | 不宣称位图能让中断 PUT 续传；保持显式 UnsupportedByDestination |

HDFS 额外核对了固定依赖 `hdfs-native@695fc47`：replicated `hsync` 发 sync packet 并等待 ACK，再做 NameNode fsync；block 关闭的 `set_last_packet` **总是设置 sync_block**，跨 block 时先关闭旧 block。不能把 Java 文档中一般性的 close 不强制 sync 警告，直接当成这个 Rust 依赖的实现事实。Erasure-coded 文件的 `hsync` 当前仍不支持；本轮没有进行断电/副本故障认证。

固定源码：[file.rs](https://github.com/kimahriman/hdfs-native/blob/695fc47e9f88804e39bba1d30bf128e66da9b8d6/rust/src/file.rs)、[block_writer.rs](https://github.com/kimahriman/hdfs-native/blob/695fc47e9f88804e39bba1d30bf128e66da9b8d6/rust/src/hdfs/block_writer.rs)、[connection.rs](https://github.com/kimahriman/hdfs-native/blob/695fc47e9f88804e39bba1d30bf128e66da9b8d6/rust/src/hdfs/connection.rs)。

## 5. 统一坐标，不统一 chunk size

所有 module 使用绝对逻辑区间 `[offset, offset + length)`。不使用 READ 序号、WRITE 序号、DS offset 或 multipart part number 作为通用 hash 坐标。

```text
source adapter: PositionedChunk(offset, Bytes)
    ├─ RangeHash.observe(offset, bytes) → source hash coverage H
    └─ destination adapter: split / aggregate / reorder → backend evidence D
                         ↓
              checkpoint recovery coverage R = D ∩ H
```

三种适配独立执行：

- 一个大 READ 跨多个 hash 子树/WRITE：共享 `Bytes` 切片，分别交付。
- 多个小 READ 覆盖一个 hash 叶子：仅拼接尚未完整的叶子碎片；不为了 hash 拼接整个 MiB 大块。
- S3 part 跨多个源块/hash 子树：由 S3 adapter 聚合 body，hash 仍按原始逻辑偏移计算。

不取读/写/hash/stripe 大小的最小公倍数，不补零对齐，不为 hash 增加源端 READ。文件末尾必须用真实长度。传输重试、更换协商 rsize/wsize 或并发窗口，不改变持久化逻辑坐标；既有 S3 upload 的 part 大小则必须保持不变。

恢复调度先合并相邻的缺失区间，再按源端实际上限发请求；不能为合并请求而跨过已完成范围，也不能按每个 1 KiB hash 叶子发一个 READ。Hash 拆分/合并沿用现有 QoS 计费边界，不把纯 CPU 分块计成后端 I/O；新增恢复元数据请求单独统计并按原有元数据限流语义处理，不能靠绕过限流换取 pNFS 并发。

## 6. Hash 与完成范围分别记录

### 6.1 三种状态

- `H`：已持久化的源端 hash 覆盖，可包含 **hash-only intent**；这些数据不一定已经写到目的端。
- `D`：目的端按本协议恢复条件证明可复用的数据范围。不是“发过 WRITE”，也不是文件大小。
- `R = D ∩ H`：本轮续传可以跳过源 READ 和目的 WRITE 的范围。

普通 checkpoint 将 `R` 与相应源端证据一致提交。对于 HDFS recovered tail、S3 已上传 part 等可以独立于 pointer 被发现的数据，必须额外使用 §7 的写前 hash intent，避免 D 已前进、H 却丢失。

位图绝不把 H 直接当作 D。H 中尚未写入目的端的范围，恢复时仍需读源数据来补写；已有 hash 覆盖不能凭空重建 payload。这些重复读取不属于重读已经完成的数据。

### 6.2 BLAKE3 的稳定表示

使用标准 BLAKE3 的 1024-byte 叶子坐标和合法二叉子树，不把 1024 bytes 强制变成网络 RPC 大小。完整子树保存 `(start_chunk, height, CV[32])`；H 本身尚未覆盖完整叶子的地方，保存该叶子内精确 span 和源字节碎片。

只合并合法 sibling；设置正确绝对 chunk counter。不得拼接普通 finalize digest 冒充父节点，也不得 dump Rust `Hasher` 内存。crate 无稳定 Hasher 导出/导入 interface；使用自己的版本化 codec 和公开 `hazmat` 计算能力。

必须保留最终 root 的两个子节点/可还原 frontier，不能提前压成一个不可逆的 whole-file non-root CV。空文件、≤1024 字节文件、恰好 1024 字节、短尾叶、整 power-of-two 文件分别处理。结果与官方向量、参考实现和普通 `blake3::hash` 三方比对。

### 6.3 任意恢复切点

例如目的端恢复到 `5 MiB + 17`：

- 如果 H 也只到这个位置，保存 `[0,4 MiB)`、`[4 MiB,5 MiB)` 子树，加上最后 17 个源字节碎片。
- 如果写前 intent 已使 H 覆盖到 8 MiB，可以保留跨过该 D 切点的完整 H 子树；不能也不需要把这个 CV 逆向拆开。恢复时从 D 切点读取缺失 payload，已落在 H 内的字节不再重复计入 hash；新的 H 外字节才补入树。

因此必须区分 H 与 R，不能简单要求每个已算好的子树都恰好等于一个 durable WRITE。若采用仅保存 R 的较简模式，提交前必须保留足够的孩子节点/边界碎片以投影到 R，不能在丢掉原材料后尝试拆 CV。

两种模式都不把切点向下取整后重读已完成前缀。边界碎片属于敏感文件内容，继承 staging 的访问权限和保密要求。

### 6.4 位图不是全文件常驻数组

`CoverageMap` module 内部使用压缩连续前缀/区间，加活动窗口位图与精确 partial-leaf spans；磁盘编码可按页切换 ranges/bitmap，必须标记 schema、粒度和起始逻辑偏移。

调度位图可以派生，恢复权威是通过校验的 generation 和 backend 证据。禁止两个独立完成表不一致时任意选择较大的一个。重叠响应、同 range 冲突内容、重复回调和短读，均不能重复累计覆盖。

## 7. 持久化位置与提交协议

### 7.1 位置与格式

全部状态仍留在目的端，Local/NFS/CIFS/HDFS 使用同目录恢复文件，S3 使用同 prefix 的恢复对象。沿用保留 artifact 命名体系，不在运行容器、本地 HOME 或业务数据库创建恢复依赖。

现有 `DMDPTR01` extension 最大 4 KiB。此次实施选择有界 inline `DMDPTR02`：保留 backend extension 的 4 KiB 上限，另用独立长度字段保存最多 **1 MiB** 的 hash/范围 envelope。旧指针的编码逐字节保持不变。不引入外置 manifest/pages 的第二种恢复真相。

概念字段仍按以下职责划分（inline 时合并在同一原子 pointer 中，不是两个文件）：

```text
root: version, transfer/source binding, stage incarnation, owner nonce,
      generation, committed manifest ref, optional pending hash-intent ref
manifest: algorithm=standard BLAKE3, source length/version, H nodes/fragments,
          committed/re-observed D facts, range encoding, backend opaque evidence,
          transient publication phase, referenced bytes length/digest
```

H-only、D-confirmed、最终 source root 必须有不同类型标签；不能把 source CV 当成 destination verification。记录包含坐标、长度、stage/upload 身份、generation 和完整性校验，解码前限制计数/长度/递归深度并检查溢出。校验和仅抗意外损坏，不是恶意存储的认证。

范围采用规范化、相邻合并的绝对字节区间；“位图”表示逻辑覆盖，不强制分配全文件 bit array。活动窗口有限，连续 H 使用 dyadic frontier，D 的洞有界；S3 最多 10,000 个 part 的 opaque receipts 以确定性摘要绑定，不能把所有 ETag 无界堆进 4 KiB extension。

在释放一批可恢复 payload 之前，对编码后的 H、既有 D 以及在途写入可能新增的范围预留检查总长度。超限返回明确的 checkpoint 错误并保留上一代，不截断证据、重读已完成范围或静默退回 legacy。外置分页若将来确有需要，须单独设计、认证及版本升级，本次不暗示已经支持。

### 7.2 常规 checkpoint

在稳定 owner 和冻结的候选范围下：数据持久化 barrier → 新 hash/range manifest 持久化 → 原子更新 root 并保证其持久性 → 报告 checkpoint → 回收不再引用的旧记录。

在 root 提交前不报告这些范围已经完成。root 引用的 stage、manifest、pages 必须先可恢复；已有记录不能被下一代原地破坏。Local 的 rename 还需要目录 sync；其他协议按各自能力完成，不能把“调用成功”统一等价成 POSIX fsync。

### 7.3 HDFS/S3 的额外写前 intent

单靠上面的顺序不能覆盖“数据已经被 HDFS lease recovery / S3 ListParts 看见，但后续 hash checkpoint 尚未成功”的窗口。

对这些会被目的端独立采用的数据，先计算一个有界批次的 hash，持久化并通过 root 锚定 `pending hash-intent`，**收到持久化成功后才允许发送该批可恢复写入/part**。intent 是 H，不是完成位图。不能仅写一个不可发现的 orphan blob，再假定恢复能找到它。

崩溃恢复重新查询 D，与已锚定的 H 求交。目的端没有的数据仍缺失；存在且符合当前恢复持久化模型的数据可复用。对超出 H 的意外尾部/part 必须明确报告无法满足 hash 恢复保证，不得用目的端当前内容的 hash 伪造源端期望值。

S3 的 ListParts/part size/number/upload id 是恢复证据；ETag 是 opaque receipt，不当作 BLAKE3。已记录 ETag 不匹配时不能直接保留旧 bit。HDFS 仍要求连续 append，不因为有离散 H 就宣称目的端能填洞。

如果 H 持久化范围比 D 大，允许保留完整 H CV，不做不可逆的 CV 反切。只有在确实要丢弃 H 的某部分或源版本失效时，才需要回到较细材料；不能为了降低元数据而静默破坏既有恢复表示。

### 7.4 请求数和 owner 约束

写前 intent 有额外元数据 I/O 和延迟，是更强恢复承诺的成本，不能写成“免费”。按有界窗口/多个 part 批量提交，小状态 inline 时合并为一次元数据 PUT；需要外置 manifest 时计入 manifest + root 请求。S3 可从 ListParts 重建 D，不强制每个 part 再写一次位图。

记录每 GiB 元数据 PUT/GET/DELETE、checkpoint 字节、等待时间和传输总请求；不能为追求细粒度 bitmap 按每次 READ 写 S3 元数据，也不靠放低带宽/IOPS门限使测试通过。batch 大小受现有 payload admission 约束，不新增一个无界的“预 hash 整窗口”缓存。

现有 NFS/S3 等 nonce 检查不是完整分布式锁。新方案仍要求同一目的 key 的唯一 writer/外部排他；只有 backend 实际支持时才使用条件替换或等价 fencing。不能为不支持 CAS 的兼容端点编造强原子 owner 保证。

## 8. 最终比较、发布与清理

持久化临时状态机为 `Receiving → ReadyToPublish → PublishedNeedsVerification → Release`。已有发布前校验目的端可在 ReadyToPublish 内完成校验；S3 等发布后校验保留 PublishedNeedsVerification 记录。

完整 H 覆盖 `[0,L)` 后计算标准 source root；要求 D 最终也完整。最终比较使用这个 root，独立读取目的端并比较长度和全部 32 字节摘要，不重新调用当前会双端读取的 `integrity::compare(Content)`。

发布前保存临时的完整 source root、stage/final 身份和必要的提交信息。S3 Complete 后 upload/parts 消失，必须切换为绑定最终对象/version 的发布后验证状态；不能沿用基线在 publish 内删除全部恢复证据再做 read-back 的顺序。

`ReadyToPublish` 只能在本次 staged metadata 全部应用后提交。还要绑定请求的 ACL/xattr 选择和实际编译出的元数据写入计划（包含 tags、ACL 字节和 principal 值，不能 hash 已脱敏的 Debug 字符串）。恢复最终文件时若这些条件变化则失败并保留记录，不在 final 上补做 staged metadata。此前的应用报告没有持久化，不能伪造恢复报告。

S3 tags 在 Complete 之后单独应用，Ready 记录只能证明已绑定的待发布计划。
恢复 final 时必须只读核验实际 tags 与该计划一致，并用最终对象身份和 owner fence
包围核验；tags 缺失、不完整或读取失败则明确失败并保留 root，禁止补写 tags 或报告成功清理。

中断发生在 rename/Complete 之后时，用临时 source root 和实际发布结果协调恢复，而不是因 stage 消失就重新读源。丢失提交回复时必须处理 ambiguous commit；没有版本/条件能力时不能自动采用被其他 writer 换过的 final 对象。

校验和发布均完成后，先移除可发现 root，再释放其 owned manifest/pages/intent，做到可重试清理。无永久成功记录。清理后、调用方收到成功前崩溃，无法保证该次调用 exactly-once 成功通知；不扩展本需求去解决这一窗口。

新 v2 hash root 若被 S3 Object Lock 等机制保护而不能释放，返回带已发布状态和待清理 authority 的错误，不能把残留永久 hash 记录称为已清理成功；其他模式和 legacy 路径原有 warning 语义保持不变。Unix Local/NFS/HDFS 的新记录使用私有权限；CIFS 在写入 raw leaf fragments 前保护并核验 pointer DACL，必要的 `READ_CONTROL`/`WRITE_DAC` 不可用时明确拒绝。Windows Local 的发布间隙恢复在不能可靠 pin 身份时失败关闭。

Native S3 路由保持现有 storage affinity，不重新引入请求级 shaping 开关。Native 不携带客户端 payload：完整承诺需要单独的可恢复 source-hash phase，并把其 H 保留到 native copy 和最终校验结束。仅服务端 copy receipt 不能生成 BLAKE3。第一批 streaming 实现完成前，不宣称 native 已有同等无重读保证。

## 9. Module / interface 划分

建议内部 seam，不新增调用方 hash 分块或位图配置：

| Module/interface | 负责 | 不负责 |
| --- | --- | --- |
| `RangeHash` | 逻辑 offset 数据观察、合法 CV 合并、H 覆盖、fragment、版本化数学表示、标准 root | NFS DS、S3 part、网络请求、数据持久化证明 |
| 覆盖区间 helper（`checkpoint`） | 规范化范围、逻辑完成位图、缺失范围调度、重复/越界检测 | 把稀疏文件长度推断为完成 |
| `StagedDestination` 恢复 interface | backend D 证据、恢复几何、manifest/root 持久化、临时发布阶段和释放 | 复制各自独立的 hash 实现 |
| transfer engine | 源绑定、read/write/hash admission、H/D 协调、恢复调度、verify/publish/release 生命周期 | 判断 backend kind 后拼协议细节 |

此次保留公开 `CheckpointObservation.durable_prefix` 的兼容含义；内部 `PreparedStage` 同时持有规范化离散 D 与完整 H。`write_offset` 仅是连续前缀，不能再用它代表全部已完成范围。backend hooks 表达实际 stage 是否支持恢复、是否要求写前 intent，以及持久化/释放操作；S3 的冻结 part geometry 与 receipts 留在 adapter 内。这些是目的端能力事实，不新增公开 TransferRequest 策略。

当前 `WriteProgressObserver` 是通知而非恢复控制，不可用它实现持久化提交。需要内部可等待的 checkpoint handshake，使 hash 材料在 writer 决定 barrier/恢复范围前保持可用，并把元数据错误真正传播给传输。

## 10. 内存与调度

不允许无界乱序：只在最低缺失位置前方一个受字节 admission 限制的活动窗口内推进。HDFS/S3 ordered-body path 在同一预算中进行有界重排；能定位写的目的端不保留无谓的大段 payload 等前缀。

内存账户覆盖源预取、channel backing allocation、writer retained/replay buffers、S3 part body、未决 hash fragments/CV、编码缓冲和元数据缓存。`Bytes::slice` 仍可能钉住原始大 allocation，不能只按切片长度计数。

完整前缀的 CV 可压缩至 `O(log L)`；带 k 个洞的状态不保证 O(log L)。例如活动窗口 32 MiB、每个 1 KiB 叶子暂存 48-byte record，最坏叶子元数据约 1.5 MiB，位图约 4 KiB；这只是容量估算，不是已验证默认参数。无限散布叶子可使元数据线性增长，必须停止继续 admission，不能悄悄丢掉唯一 hash 证据。

保持当前 HDFS 132/150/204 MiB RSS预算、10%增长门限和固定测试 worker profile。prototype 实验并未证明长时间 streaming 的内存上限；实现阶段必须重新跑完整门禁，不以设计估算代替测量。

## 11. 已完成验证与尚未验证

### 11.1 已完成

- §3 本轮四个真实文件的混合分块读取、内容检查、网络路径和清理；三轮通过独立 DS。
- 生产依赖对两套 NetApp endpoint 的只读协商检查。
- 新 AFX 测试机既有 20 GiB 日志和抓包复核；不是本轮性能 A/B。
- 隔离 BLAKE3 实验：**16,440** 次稀疏恢复模拟、**35** 个官方默认 hash 向量，与普通 crate 和官方 reference implementation 比对。
- **42** 次跨进程、版本化序列化恢复；含 `5 MiB + 17` 切点，读块 **32,771 / 65,536 / 98,304** 字节，所有恢复源请求均不覆盖已存前缀。主调查也复跑了 resume binary。

大输入例为 `7 MiB + 333`，三种分块都只读取剩余 `2,097,468` 字节。checkpoint 229 bytes，包含两个子树 CV 和 17 个边界字节，完整标准 digest 一致。实验是文件序列化和新的进程恢复，不是 fsync/kill 故障证明；初始输入缓冲也不是生产有界 streaming 的实现。

### 11.2 实施前后的验证矩阵

| 类别 | 必须验证 |
| --- | --- |
| Hash | 空/短/整叶/power-of-two、随机乱序、错误 counter/merge/root、固定官方向量、H跨D切点、partial leaf |
| 分块 | 64 KiB、1020 KiB、2 MiB、8 MiB、奇数长度，读/写/barrier三者任意不对齐；恢复前后协商大小变化 |
| 恢复 | root/intent/data/manifest各点SIGKILL；fresh process；源端READ范围与R交集严格为空；H-only不误记为完成 |
| Local/NFS/CIFS | 洞、短写、verifier变化、FLUSH失败、已记录离散范围不被截断；owner竞态与旧generation保留 |
| HDFS | 2 MiB写、64 MiB checkpoint、跨128MiB block、部分checksum chunk、lease恢复尾部、HA；EC Unsupported真实报告 |
| S3 | 稀疏已完成parts、ListParts分页、旧ETag/part替换、原partsize恢复、同part重试、SSE/compat差异、元数据请求数 |
| 发布后 | S3 Complete回复丢失、rename后崩溃、readback中断、final被替换、恢复后仅读目的端完成比较和清理 |
| pNFS | 现有.17真实DS路径、多个逻辑range无序完成；多DS同文件的并行抓包单独记录；慢DS用可控模拟，不改共享设备网络 |
| 资源 | 长文件、长洞、慢consumer、hash intent阻塞、RSS与growth、请求成本、GC代数/暂存空间上限 |

调研阶段没有运行全 backend 新功能真机矩阵；实施阶段的已完成验证与尚未通过的 release gates 单独记录在 §14。没有为研究扩大权限、重启服务或改动共享设备设置。

## 12. 实施顺序与不采用的方案

1. **Hash module 与 codec**：先把隔离正确性覆盖迁入仓库正式测试；实现 H 与 D 分离、root特殊情况、边界碎片、容量和格式约束。
2. **Streaming + Local/NFS/CIFS**：启用已有 positioned interface，接 checkpoint handshake / generations / 发布后释放；验证异构分块，不一次性更换所有后端。
3. **HDFS + S3**：实现可发现的写前 hash-intent、HDFS连续append、S3离散part恢复和请求预算；完成崩溃及现有内存门禁。
4. **Native 与发布窗口**：接可恢复 hash phase 和 transient published verification，保留既有 native affinity；验证不重复源hash扫描。
5. **pNFS性能验收**：在相同binary/workload/数据模式下做 ordered vs positioned-hash 的多轮 A/B；分别测DS等待、hashCPU、内存、checkpoint请求，不把较新dirty依赖效果归因于本方案。

旧 `DMDPTR01` 没有源 hash，无法凭 prefix/ETag 补出它。上线前优先 drain旧任务；遇旧记录明确区分 legacy恢复/受控重启与新无重读保证，不能静默标记为已支持。未知新schema、坏manifest、失效源绑定必须 fail closed，不能自动采用不受信任的更大范围。

不采用：按 backend 的chunk序号存hash、统一各端chunk大小、XOR/拼普通digest、自定义hash替代标准BLAKE3、全文件常驻叶子表、源重读后假称零重读、用目的端hash冒充源证据、按每次READ更新S3对象、为hash自动关闭native、永久成功manifest。

## 13. 复核入口

- 当前源码：[producer](../../src/transfer/engine/producer.rs#L96)、[payload](../../src/transfer/engine/payload.rs#L26)、[NFS positioned read](../../src/storage/backends/nfs/source.rs#L144)、[pointer codec](../../src/storage/pointer.rs#L23)、[verification](../../src/transfer/engine/publication.rs#L101)、[standalone integrity compare](../../src/integrity/mod.rs#L254)。
- 协议和实现原始依据：[RFC 8881](https://datatracker.ietf.org/doc/html/rfc8881)、[BLAKE3 1.8.5 hazmat](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs)、其余 backend 协议链接见调研附文。
- 本轮隔离 pNFS 源码 `/tmp/data-mover-pnfs-research.8OKLEc/src/bin/io_probe.rs` SHA-256：`836fc72051a89e2e576c62eec552e0721286dbb2adafa108fd8a4611b1ab5cf1`；musl binary SHA-256：`c6b00bb79dd918491e04c618e56617e22c52000fb4b7ed629d62937b63c3e9f6`。
- `.17` 本轮日志/pcap：`/tmp/release-hash-research-20261022-8OKLEc{,-b,-c,-d}/`；本机已复制小型pcap供复核。保留这些诊断证据，不保留测试数据文件。
- 既有20GiB复核原始数据仍在用户原目录，未覆盖/删除；本轮摘要不把这些机器本地证据提交为版本化原始数据。

复现必须使用唯一release前缀、共享锁、超时和无条件清理，不能直接重用本文文件名覆盖已有记录。临时实验程序不是交付的生产功能，后续实现应把关键覆盖迁入正式测试。

## 14. 本轮实施与验证

### 14.1 生产实现

- `storage::range_hash`：标准 BLAKE3 dyadic frontier、partial leaves、独立可恢复 codec；保留 root 特殊情况，不序列化 `Hasher` 内存。
- `storage::checkpoint` / `PreparedStage`：H、已确认写入和 durable D 分离；v2 pointer、generation、范围归一化、元数据计划绑定、发布阶段和共享提交 gate。
- `transfer::engine::checkpointed`：按 D 补洞，按缺失 H 更新 hash；支持定位读完成顺序和顺序 fallback。Local/NFS/CIFS 不等整个窗口才交付；HDFS/S3 先锚定有界 intent，再释放可恢复 payload。源/写/metadata 事务取消时等待必要的收尾，不把未提交状态报为 checkpoint。
- Local/NFS/CIFS 保留已提交离散范围，使用真正的数据 barrier；HDFS 维持连续 append，校验 lease-recovered tail；S3 使用冻结的 multipart geometry、完整 part 和 opaque receipt 摘要，覆盖 native / streamed 互相续传。
- 最终摘要重建后只读回目的端。发布后的恢复校验和临时状态释放不再依赖重读源文件；旧 v1 明确区分，不能宣称同等保证。
- AtomicReplace、Direct、expert ordered transport 和不符合门槛的新传输不启用新恢复路径；改变源读块/恢复门槛不使已存在的 v2 记录被当作 v1 丢弃。

### 14.2 `.17` 上的生产路径验证

使用本轮生产库构建的静态 `transfer_resume`，不是隔离的 hash prototype。源和目的端均为用户态 NFS v4.1，经现有 pNFS 导出；保留已有 kernel mount 不动。持有两个 lab 锁，仅写唯一目录 `data-mover-range-hash-20261022-UL1jr0/`。

文件长度 `402,653,517 = 384 MiB + 333`，非零确定性数据。目的端 checkpoint 为 64 MiB；源 READ 协商上限为 65,536 bytes。用 source QoS 使中断落在传输阶段，最终读回校验均开启。

| 场景 | 恢复前持久化 D | 新进程源 READ 字节 | READ 次数 | 最终读回与清理 |
| --- | ---: | ---: | ---: | --- |
| 正常取消后恢复 | 134,217,728 | 268,435,789 | 4,097 | 通过；无 pointer/stage 残留 |
| SIGKILL 后恢复 | 134,217,728 | 268,435,789 | 4,097 | 通过；无 pointer/stage 残留 |

两次均严格等于 `文件长度 - D`，不是为最终源 hash 再扫描一遍。SIGKILL 时 stage 长度为 201,326,592，但只采用 pointer 证明的 134,217,728；没有把可见尾部误作完成位图。另一次较早取消发生在首个 checkpoint 前，明确报告不可恢复，不把未提交数据列入保证。

独立测试校验中，源与两个最终文件的 SHA-256 均为 `1d8c7ae0bacc7e2938e72ed8711fd6f514894ec8be69f32d263f519b1f3c8bcf`。这是额外的验收读，与上述 transfer 内部 READ 计数分开。两次恢复进程峰值 RSS 为 86,188 / 88,036 KiB；小样本、带 QoS、非 HDFS，不替代长时间内存增长门禁或吞吐 A/B。

真机本轮源/目的 payload 和目录已删除，锁已释放；日志、366-byte checkpoint 副本和诊断 binary 留在 `.17:/tmp/data-mover-range-hash-20261022.UL1jr0/`，本机小型证据副本在 `/tmp/data-mover-range-hash-evidence.eXUpw3/`。所测 binary SHA-256：`985bfec99075eb6a0f6b208df28251d1b7119028596934ec6ae26007ba4ce680`。该记录标识本轮 NFS 集成构建，不把之后仅在本地验证的其他 adapter 修订当作同一真机测试。

### 14.3 验证边界

最终 `make ci` **退出 0**：格式检查、架构依赖守卫、49 项 Python harness 检查、全部 targets 严格 Clippy、workspace 测试、examples 构建与 `e2e-local` runner 均通过。库测试 **1,826 通过、9 忽略、0 失败**；集成测试按各自配置执行，需要外部环境的 ignored 项没有冒充已认证。还额外完成 Local fixture 的 blocks / unordered 遍历（均 3 项、计数平衡、零失败）和 namespace smoke test，临时 fixture 已清理。完整本机 CI 日志：`/tmp/data-mover-range-hash-ci-20261022.log`。

真实 CIFS、HDFS HA/Kerberos/EC、S3 各 profile 与版本桶的完整新增功能矩阵，以及既有 HDFS RSS/growth/nightly gates，尚不能因本轮 NFS 验证或 harness 自测而标记为通过。没有放宽内存、IOPS、带宽或 request 门限，也没有自动提交、推送或触发 GitHub nightly。

### 14.4 后续真机完整性验证（2026-10-23）

后续在当前集成代码上补跑的结果见 [真机验证总表](checkpointed-range-hash-real-validation.md)。
Local syscall、AFX NFS wire audit、ONTAP NFSv3/v4.0、Kerberos HDFS append/lease tail、
MinIO/RustFS 流式和 MinIO 原生恢复已有正向证据，包含实际源读取区间排除和独立最终摘要。
它们不等于全量矩阵通过：DXN 实测发现 receipt ETag 引号差异导致恢复误拒绝；
当前 CIFS UNIX-style share 不保留 protected DACL，首次 hash checkpoint 安全拒绝；
AFX 当前源 placement 为 DS=MDS，未覆盖生产多 DS 条带源数据路径。
这些问题/缺口应按总表继续处理，不能用本地 CI 或历史 §14.2 代替。此轮验证没有改生产代码。

### 14.5 DXN receipt 引号兼容修复

按后续确认，S3 checkpoint receipt 指纹统一使用恰好一对双引号的 ETag 表示，
长度字段也采用该表示的长度。只规范化 receipt 编码，不改 `UploadState.parts` 的协议原值，
不修改 source identity、条件请求或非 hash-checkpoint 路径。opaque 值保持大小写敏感；
不平衡/重复引号、空值、控制字符与 weak-validator 形式拒绝，不能复用忽略大小写的旧比较器。

`DMS3CP01` 结构和 digest domain 保持不变，旧版全带引号指纹字节完全相同。
恢复依次允许规范带引号、原样 legacy、全不带引号 legacy 三种确定性编码，均先校验 ETag 语法。
旧版任意 mixed 编码在 ListParts 改变表示后可能无法还原，仍失败关闭，
不枚举每个 part 的引号组合，不静默重传。新 mixed native/streamed receipts 总是写成统一带引号编码。
已发布恢复继续原样保留 `recovered_receipts`，不能对已消失的 MPU 从空 parts 重算。

新增 7 项 focused 回归覆盖实际 receipt 校验调用点，先观察旧实现失败，再验证修复转绿，
包括旧 quoted/bare/mixed 检查点、重复恢复、opaque 真变更拒绝、畸形值拒绝和发布后证据保留。
真机 follow-up 结果另见 [S3 验证报告](checkpointed-range-hash-s3-validation.md)。
DXN 非版本/版本桶的旧→新、新→新共四例已通过，实际源 READ 不覆盖复用 D，
独立 SHA-256 相同，成功后的 pointer/history/MPU 清理及两个临时桶删除均核对完成。
完整 `make ci` 退出 0：库测试 1,833 通过、9 忽略、0 失败；其余门禁均通过。
