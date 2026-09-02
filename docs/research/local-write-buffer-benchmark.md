# Local 目标端最大写入 chunk 独立基准调研

- 调研日期：2026-09-01
- 仓库基线：`76fdb81738df32a85284a974e1fea7570590a2b9`，并检查当前工作区改动
- 范围：Linux buffered positional I/O；当前 role-based Local staged destination
- 状态：源码、一手资料、256 MiB 全矩阵与 1 GiB focused confirmation 已完成

## 研究问题

本调研只回答 Local **目标端**应暴露多大的工程写入上限。它不从 Local 源端的
2 MiB 读取上限推导答案，也不把 Linux syscall 上限或 Tokio 默认 buffer 当成性能
结论。

需要分别回答三个问题：

1. API/capability：一个 `write_at` 最多能接收多少字节？
2. performance policy：在哪个 chunk 后吞吐不再稳定增长？
3. runtime policy：在吞吐接近时，哪个 chunk 能更好地约束 inflight 内存、blocking
   task 占用时间、取消和 fast-fail 延迟？

## 已确认的分层事实

| 层次 | 已确认事实 | 对工程上限的含义 |
|---|---|---|
| Linux `pwrite` | 单次最多实际传输 `0x7ffff000` B，且成功也允许 short write | 约 2 GiB 是 syscall 硬上限，不是推荐 chunk |
| Rust `FileExt::write_at` | 返回实际写入长度；`write_all_at` 通过重复调用补齐 | backend 必须处理 short write |
| Tokio 1.52.1 `fs::File` | `AsyncRead`/`AsyncWrite` 默认 `max_buf_size` 是 2 MiB，可修改 | 2 MiB 是 Tokio 默认，不是 Local 文件系统能力 |
| 当前 role-based Local | 已转换为 `std::fs::File`，直接在 `spawn_blocking` 中调用 `write_at` | 当前路径绕过 Tokio 2 MiB buffer |
| legacy Local | 2 MiB `DEFAULT_BLOCK_SIZE` 主要决定源端 read chunk；写 pipeline 对 Local 不二次切分 | 不能用历史 read chunk 证明 write ceiling |

### Linux 与 Rust std

Linux `pwrite()` 从指定 offset 最多写入 `count` 字节，不改变共享文件游标；成功返回值
可能小于 `count`。[Linux `pread(2)`/`pwrite(2)`](https://man7.org/linux/man-pages/man2/pread.2.html)

Linux `write()` 及同类调用单次最多传输 `0x7ffff000` 字节；这个限制在 32 位和 64 位
Linux 上相同。[Linux `write(2)` NOTES](https://man7.org/linux/man-pages/man2/write.2.html#NOTES)
内核将它定义为 `MAX_RW_COUNT = INT_MAX & PAGE_MASK`，并在进入具体文件系统前裁剪
用户传入的 count。[Linux `include/linux/fs.h`](https://github.com/torvalds/linux/blob/abdf623ddb75b24659018d3952d8f61937306ae5/include/linux/fs.h#L2441-L2445)、
[`fs/read_write.c`](https://github.com/torvalds/linux/blob/abdf623ddb75b24659018d3952d8f61937306ae5/fs/read_write.c#L560-L570)

Rust Unix `FileExt::write_at` 的契约是返回实际写入长度；`write_all_at` 才承诺重复调用直至
写满或失败。[Rust `FileExt` 文档](https://doc.rust-lang.org/stable/std/os/unix/fs/trait.FileExt.html)
Linux 上 std 最终调用 `pwrite64`，先将 slice 限到 `ssize_t::MAX`；内核更低的
`MAX_RW_COUNT` 仍是最终单 syscall 边界。[Rust Unix fd 实现](https://github.com/rust-lang/rust/blob/70222712809cd5cc1718ed8995914a1cbacb6b92/library/std/src/sys/fd/unix.rs#L418-L428)

因此，64 MiB 或更大的 buffer 在 API 上可以合法传入，但这既不表示一次 syscall 一定写满，
也不表示它是适合 data-mover 的工程 chunk。

### Tokio 的 2 MiB 与当前实现无关

本仓库 `Cargo.lock` 锁定 Tokio 1.52.1。该版本将 `DEFAULT_MAX_BUF_SIZE` 定义为 2 MiB，
`File::from_std` 用它初始化文件；`poll_write` 最多复制 `max_buf_size` 字节到内部 buffer
后派发 blocking write。[Tokio `blocking.rs`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/io/blocking.rs#L20-L28)、
[`File::from_std`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L282-L291)、
[`File::poll_write`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L727-L780)

Tokio 同时公开 `set_max_buf_size`，官方示例明确演示将其改成 8 MiB；所以 2 MiB 只是默认
调度/复制粒度。[Tokio `File::set_max_buf_size`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L563-L592)

当前 role-based [`write_piece`](../../src/storage/backends/local/staged.rs) 使用
`std::os::unix::fs::FileExt::write_at`，不经过 Tokio `fs::File::poll_write`。因此不能把
Tokio 2 MiB 默认值描述成当前写端 capability。

### blocking task 对上限的约束

Tokio 说明：已经开始执行的 `spawn_blocking` task 不能 abort；blocking thread 达到上限时，
后续 task 会排队。[Tokio 1.52.1 `spawn_blocking`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/task/blocking.rs#L91-L127)

当前一个上游 `Bytes` 对应一个 blocking task。因此 chunk 越大，至少带来：

- `write_concurrency × chunk` 级别的 payload 在途内存；
- 已开始 task 的最坏不可取消工作量增加；
- 某个写失败被观察到之前，其他 task 可能写入更多数据；
- 单个大 piece 的 short-write 补写循环占用同一 blocking worker 更久。

所以即使大 chunk 吞吐略高，也需要把取消延迟、fast-fail 和内存一起纳入工程决策。

## 仓库实现与历史

### 当前 role-based Local staged destination

[`consume_input`](../../src/storage/backends/local/staged.rs) 为每个非空 `Bytes` 分配连续
offset：不超过 5 MiB 时整块启动一次 `write_piece`，超过 5 MiB 时用零拷贝 slice 拆为多个
最多 5 MiB 的 piece。`write_concurrency` 限制同时在途的 piece/task 数。

[`write_piece`](../../src/storage/backends/local/staged.rs) 在一个 `spawn_blocking` closure 中对
完整 piece 首先发起一次 `write_at`；只有 short write 时才用剩余 slice 和更新后的 offset
继续写；返回 0 映射为 `WriteZero`，`Interrupted` 在同一 offset 重试。所有 piece 完成后，
staged destination 执行一次 `set_len(issued)` 和 `sync_data`，再原子保存 checkpoint。因此
当前行为是：

```text
one upstream Bytes <= 5 MiB
  -> one blocking task
  -> one full-size write_at in the normal path
  -> retry only the unwritten suffix on short write

one upstream Bytes > 5 MiB
  -> zero-copy slices <= 5 MiB
  -> one bounded blocking task per slice
```

这个路径透明接收不同协议产生的 chunk，同时把 Local 单次写任务、不可取消 blocking 工作量
和正常 syscall 大小约束在 5 MiB；边界、尾块、short write 与 `Interrupted` 均有测试覆盖。

当前 [`StagedDestination`](../../src/storage/roles.rs) 只有 `write(stage, ByteStream)`，没有
目标端 capability/协商字段。若最终选择显式 ceiling，近期最小变更是在 Local backend
内部拆分大 piece；若 transfer engine 需要在源端提前避免产生过大的 piece，则需另行设计
destination capabilities seam，不能把 Local 私有常量上升成所有协议的共同假设。

### legacy Local 的 2 MiB 不是写端上限

legacy [`DEFAULT_BLOCK_SIZE`](../../src/local.rs) 是 2 MiB，`LocalStorage::new` 会将配置值收紧
到该值。它直接决定 Local **读取**产生的 `DataChunk` 大小，所以 Local→Local 常见写入也
会被动呈现为 2 MiB piece。

但 legacy `write_pipeline_core` 对 Local 传入 `sub_chunk_size=None`，明确表示 Local sink
整块接收上游数据；只有 NFS 按协商 `wsize` 二次切分。提交
[`a705a222`](https://github.com/JayTsu-sh/data-mover-rs/commit/a705a222f6a26ab3e1551df4cd8082a58700eee2)
之前的提交 [`c57e0a5`](https://github.com/JayTsu-sh/data-mover-rs/commit/c57e0a5a4f87867ed7933d912f3871034a5a3634)
明确修复了来自 S3 的 5 MiB 和 CIFS 的 8 MiB chunk 写入 Local 时被 Tokio 单次 2 MiB
推进上限截短的问题：改用 `write_all_buf` 循环写满。随后 `a705a222` 把实现换成
positional `write_at` 并保留写满/补 short write 的语义，没有添加 2 MiB 写上限。

因此历史只证明 Local 写端长期能够消费大于 2 MiB 的跨协议输入，不证明大于 2 MiB 更快，
也不证明写端必须与 read ceiling 相等。

## 独立基准方法

基准必须直接测目标端，避免把源端读取、hash、stream channel 或 publish rename 混入
`max_write_chunk_bytes` 判断。

### 固定条件

- 单个目标文件只打开一次，使用与生产一致的共享 `Arc<std::fs::File>`；
- 使用 positional `FileExt::write_at`，正常路径一次提交完整 piece，short write 才补后缀；
- 不预先 `fallocate`，因为当前生产 staged path 也不预分配；
- 每个样本写新文件，结束时执行一次 `sync_all`；
- 每个组合至少三轮，报告中位数及波动范围；
- 内容预先生成或重复使用，计时区间不包含 payload 构造；
- 每轮结束删除自己的隔离测试文件，不能复用已增长文件；
- 至少覆盖 ext4 与 XFS；tmpfs 只用于观察内存/调度上限，不能代替磁盘结论。

当前机器：Linux `6.8.0-124-generic`，4 KiB page；`/tmp` 是 `/dev/sda1` ext4，`/work`
是 `/dev/sdc` XFS。两者 block queue 当前 `max_sectors_kb=1280`、
`max_hw_sectors_kb=32767`，且 write cache 为 write-back。这些 block-layer 值可以导致一个
用户 `pwrite` 被内核拆成多个底层请求，不应直接作为用户空间 chunk ceiling。
[Linux queue sysfs ABI](https://docs.kernel.org/admin-guide/abi-stable.html#abi-file-stable-sysfs-block)

### 参数矩阵

至少测：

- 文件大小：40 MiB、1 GiB；4 KiB 只用于固定开销基线；
- chunk：4 KiB、64 KiB、256 KiB、512 KiB、1 MiB、2 MiB、4 MiB、8 MiB、16 MiB；
- write concurrency：1、4、8；生产默认值必须明确标出；
- 文件系统：ext4、XFS；条件允许时补 tmpfs。

如果完整笛卡尔积成本过高，先用 concurrency=1 和生产默认 concurrency 扫所有 chunk，
再对 256 KiB、1 MiB、2 MiB、4 MiB 扫全部 concurrency。

### 完整长期评估应分开记录的指标

| 指标 | 目的 |
|---|---|
| issue/write completion throughput | 观察 page-cache 接收和 syscall/task 开销 |
| `sync_all` latency | 观察把 dirty pages 推到 durable boundary 的成本 |
| write + sync total throughput | 对齐 staged destination 的实际完成语义 |
| CPU time / CPU utilization | 排除仅以更多 CPU 换取微小吞吐 |
| peak RSS | 验证 `concurrency × chunk` 内存代价 |
| `pwrite64` call count | 验证正常 piece 是否一次 syscall，识别 short write |
| short-write count | 检查大 buffer 是否引入补写 |
| p50/p95 task latency | 评估 fast-fail 被观察的时间粒度 |
| cancellation drain latency | 评估不可 abort 的 blocking task 尾延迟 |

不能只报告 page-cache write throughput：`write_at` 成功通常不等于 durable，生产路径最终还会
`sync_all`。同时也不能只报告 total，因为一次 `sync_all` 的设备波动可能掩盖 chunk 对 task /
syscall 开销的影响；两段必须同时报告。

本次独立基准实际记录了 submit、`sync_all`、total、write call、short write 和 VmHWM；尚未
实现 CPU、单 task latency 与 cancellation drain latency。后面三项应作为长期性能回归基准的
补充，不把它们伪装成本次已测数据。

## 决策规则

最终建议值应按以下顺序确定，而不是简单选择单轮最快点：

1. 排除在任一主测文件系统上吞吐明显退化或波动显著的 chunk；
2. 找到生产 concurrency 下 write+sync 吞吐的平台区；
3. 在平台区内选择最小的 chunk，除非更大值在 ext4、XFS 的 1 GiB 样本都呈现可重复、
   有实质意义的收益；
4. 确认 `write_concurrency × max_write_chunk_bytes` 能纳入 runtime 总 inflight 字节预算；
5. 确认 cancellation/fast-fail 尾延迟没有越过项目允许边界；
6. 把选择定义为 backend capability ceiling，实际 piece 仍可按 runtime 内存预算、上游
   piece 和剩余长度缩小；QoS 仍只施加在源端，不新增目标端 QoS；
7. 无论上限为何，Local 目标端都必须在 backend 内拆分更大的跨协议上游 piece，并保留
   short-write 补后缀循环。

## 实测结果

基准实现为 [`local_write_benchmark.rs`](../../examples/local_write_benchmark.rs)，矩阵 runner 为
[`run-local-write-benchmark.sh`](../../tests/lab/run-local-write-benchmark.sh)。每个样本创建一个
新文件；为排除源端分配/复制成本，所有并发 task clone 同一个 immutable `Bytes` payload。
基准使用与生产相同的 `spawn_blocking + write_at` short-write 循环和 positional inflight，
全部 piece 完成后执行一次 `sync_all`，随后删除隔离文件。

第一轮全矩阵：每个样本 256 MiB，ext4/XFS/tmpfs，chunk 为 64 KiB、256 KiB、1/2/4/8 MiB，
inflight 为 1/2/4/8，每个组合三轮。下面是 inflight=4 的中位数：

| FS | chunk | submit MiB/s | write + sync MiB/s |
|---|---:|---:|---:|
| ext4 | 64 KiB | 1717 | 872 |
| ext4 | 256 KiB | 1835 | 844 |
| ext4 | 1 MiB | 1954 | 919 |
| ext4 | **2 MiB** | **2050** | **948** |
| ext4 | 4 MiB | 2003 | 906 |
| ext4 | 8 MiB | 1911 | 855 |
| XFS | 64 KiB | 3927 | 1318 |
| XFS | 256 KiB | **4950** | **1476** |
| XFS | 1 MiB | 4820 | 961 |
| XFS | **2 MiB** | 4408 | 1413 |
| XFS | 4 MiB | 4365 | 1421 |
| XFS | 8 MiB | 4042 | 995 |
| tmpfs | 64 KiB | 2246 | 2246 |
| tmpfs | 256 KiB | **2961** | **2959** |
| tmpfs | 1 MiB | 2566 | 2565 |
| tmpfs | **2 MiB** | 2748 | 2747 |
| tmpfs | 4 MiB | 2592 | 2587 |
| tmpfs | 8 MiB | 2814 | 2812 |

第二轮 focused confirmation：每个样本 1 GiB，只测 ext4/XFS，chunk 为 1/2/4/8 MiB，
inflight 为 4/8，每个组合三轮：

| FS | chunk | submit MiB/s (inflight 4) | total MiB/s (4) | total MiB/s (8) |
|---|---:|---:|---:|---:|
| ext4 | 1 MiB | 1893 | **1034** | **985** |
| ext4 | **2 MiB** | **1947** | 944 | 723 |
| ext4 | 4 MiB | 1811 | 921 | 973 |
| ext4 | 8 MiB | 1750 | 645 | 750 |
| XFS | 1 MiB | **4757** | 836 | 880 |
| XFS | **2 MiB** | 3257 | **1496** | **1450** |
| XFS | 4 MiB | 4494 | 814 | 872 |
| XFS | 8 MiB | 4219 | 947 | 1499 |

全部 264 个正式样本（216 个全矩阵、48 个 confirmation）中，`write_calls` 都等于逻辑
piece 数，`short_writes=0`。这证明候选范围内正常 piece 对应一次 `pwrite64`，但不取消生产
代码处理 short write/EINTR 的责任。

`sync_all` 是主要波动源，尤其 XFS 和 ext4 的个别轮次，因此不能把某个 total 单点当作
普适设备结论。不过两组数据给出稳定的工程边界：

- 64/256 KiB 会显著增加 syscall 和 blocking task 数；
- 1 MiB 已进入吞吐平台，但 XFS 1 GiB durable total 明显低于 2 MiB；
- 2 MiB 在 256 MiB ext4 上是 submit 与 total 最佳点，在 1 GiB XFS 上是稳定最佳点；
- 4/8 MiB 没有跨 ext4/XFS、跨文件大小的可重复收益，ext4 8 MiB 反而明显退化；
- 本 microbenchmark 的 peak RSS 随复用 payload 增大：1 MiB 约 4.7 MiB、2 MiB 约
  5.8 MiB、4 MiB 约 7.8 MiB、8 MiB 约 12 MiB。由于并发 task clone 的是同一个
  `Bytes`，这些数值只反映单 payload/chunk 增量，**不是** production distinct upstream
  `Bytes` 的真实峰值；生产内存上界仍必须按 `write_inflight × write_chunk` 加 channel /
  source 持有量预算推导。更大 piece 还增加不可 abort 的 blocking 工作量。

原始结果在 `/tmp/local-write-benchmark.csv` 和
`/tmp/local-write-benchmark-confirm.csv`。它们是当前机器的临时 lab 数据，不应被当作所有
硬件上的绝对性能保证；长期可复现证据是仓库中的 benchmark 与 runner。

### S3 5 MiB piece：直接写还是拆分

为直接回答跨协议输入问题，又以 1 GiB 文件对 2 MiB 与 5 MiB piece 做了 focused 对比。
第一组覆盖 ext4/XFS、inflight 1/4/8、三轮；第二组将 chunk 顺序反转，并在 inflight 4 下
增加五轮，降低固定测试顺序和后台 writeback 的偏差。36 个第一组样本和 20 个反序样本
仍然全部满足一次 piece 对应一次 `write_at`，`short_writes=0`。

生产常用 inflight 4 合并两种顺序后的中位数：

| FS | piece | submit MiB/s | write + sync MiB/s | calls / GiB |
|---|---:|---:|---:|---:|
| ext4 | 2 MiB | 1965 | 964 | 512 |
| ext4 | 5 MiB | 1930 | 890 | 205 |
| XFS | 2 MiB | 4340 | 1007 | 512 |
| XFS | 5 MiB | 4642 | 1481 | 205 |

5 MiB 把 syscall/task 数减少约 60%，但没有形成跨文件系统的一致性能优势：相对 2 MiB，
ext4 submit/total 分别约为 -1.8%/-7.6%，XFS 则约为 +7.0%/+47%。durable total 的差异仍
主要由高波动的 `sync_all` 决定，不能据此把 XFS 当前机器上的优势声明成通用 Local 事实。
5 MiB microbenchmark 的复用 payload VmHWM 约 8.6 MiB，2 MiB 约 5.7 MiB；生产 distinct
buffers 的差距会由 inflight 进一步放大。

在明确采用“带宽差别不大时优先减少目标端写 ops”的选择标准后，5 MiB 更符合工程目标：
它在本组数据中将每 GiB 的应用层写调用从 512 次降为 205 次，submit 吞吐在 ext4 上只下降
约 1.8%，在 XFS 上则更高。因此 S3 的 5 MiB piece 到达 Local 后直接提交一次；CIFS 等产生
的更大 piece 仅在超过 5 MiB 时由 Local backend 拆分。原始 focused 数据位于
`/tmp/local-write-s3-5m-vs-2m.csv` 和 `/tmp/local-write-s3-5m-vs-2m-reverse.csv`。

## 结论与修改建议

Local 目标端的工程 capability ceiling 建议明确为：

```text
LOCAL_MAX_WRITE_CHUNK_BYTES = 5 MiB
```

选择 5 MiB 是独立写基准和“吞吐接近时减少写 ops”准则共同得出的结果，不是为了与读端
对称，也不是因为 Tokio 或 Linux 强制。输入 chunk 不超过 5 MiB 时直接签发一次 positional
write；超过 5 MiB 时取剩余输入与 5 MiB ceiling 的较小者，最后一段允许不足 5 MiB。
Local 写端不新增 QoS，也不把源端 QoS grant 当成自身 capability；源 QoS 只在源端 shaping，
写端只消费已经到达的 stream。

已落地的实现规则：

1. 在 Local staged backend 定义独立的 `LOCAL_MAX_WRITE_CHUNK_BYTES`，不要复用 read 常量；
2. `consume_input` 对大于 5 MiB 的跨协议 `Bytes` 做零拷贝 slice/split，再分别签发 positional
   write；不超过 5 MiB 的 piece 只发起一次 `write_at`；
3. `write_concurrency` 继续限制在途 piece 数，同时 runtime 必须计入
   `write_concurrency × 5 MiB` 的目标端在途字节；
4. 保留 short-write 补后缀循环，并补上 `ErrorKind::Interrupted` 重试；
5. 增加大于 ceiling 的拆分、最后不足 5 MiB、short write、EINTR、offset 顺序、inflight、
   cancellation/fast-fail 和最终 data-durability/checkpoint 测试；
6. Local 写端仍需透明接受 S3 5 MiB、CIFS 8 MiB 等更大的输入；“接受”表示在 backend
   内拆分，不是把 unsupported 上升到 storage enum/transfer 层。
