# Local 写入最大 chunk size 复核

> 后续决策（2026-09-10）：Local 内部写任务上限已调整为 8 MiB。本文保留形成原始
> 5 MiB 建议时的证据；现行架构由 ADR-0002 及架构文档定义。64 MiB Auto 检查点间隔
> 已与写任务上限解耦。

- 调研日期：2026-09-10
- 范围：Linux buffered positional I/O、当前 Local staged destination、XFS/ext4
- 结论口径：区分 syscall 硬上限、Local destination capability ceiling 和一次传输的实际 write piece

## 结论

当前 `LOCAL_MAX_WRITE_CHUNK_BYTES = 5 MiB` 适合作为 **Local 目标端 capability
ceiling**，暂时不应改成 2 MiB 或继续增大到 8 MiB。它表示目标端最多把一个连续
`Bytes` piece 作为一个 blocking positional-write task 提交，不表示 Local→Local 每次都应写
5 MiB。

当前应采用以下三层数值：

| 层次 | 数值 | 含义 |
|---|---:|---|
| Linux 单 syscall 上限 | `0x7ffff000` = 2,147,479,552 B | 正确性硬边界，不是性能参数 |
| Local destination ceiling | **5 MiB** | 一个目标端 write task / 正常路径一个 `pwrite64` 的最大输入 |
| Local→Local 实际 piece | **最大 2 MiB** | Local source 每次最多产生 2 MiB，目标端不会把多次读取聚合成 5 MiB |

因此，对“Local 写最大能有多大”的回答是约 2 GiB；对“本项目 Local 写上限应该设多少”的
回答是 **保留 5 MiB ceiling**；对“Local→Local 实际应该按多大写”的回答是 **维持当前
2 MiB source piece 直接写入**。这三个问题不能用同一个数值回答。

5 MiB 的理由是兼容较大的上游连续 piece 并减少应用层 write ops，同时限制不可取消的
blocking task 大小。它不是 Linux、XFS、页大小或块设备要求的边界。已有跨 ext4/XFS 基准
没有证明 5 MiB 是所有机器的吞吐最优点，但也没有证据支持把 capability 收紧为 2 MiB；
收紧只会把已经连续的 5 MiB 输入拆成三次写，并不会改善 Local→Local，因为后者本来就是
2 MiB piece。

## Linux 和 Rust 的真实限制

Linux `pwrite()` 在指定 offset 写入，且成功时允许返回少于请求长度。Linux 对 `write()` 和
同类 syscall 的单次传输硬上限为 `0x7ffff000` 字节；内核将其定义为
`MAX_RW_COUNT = INT_MAX & PAGE_MASK`，进入具体文件系统前会裁剪 count。
[Linux `pread(2)`/`pwrite(2)`](https://man7.org/linux/man-pages/man2/pread.2.html)、
[`write(2)` NOTES](https://man7.org/linux/man-pages/man2/write.2.html#NOTES)、
[Linux v6.8 `include/linux/fs.h`](https://github.com/torvalds/linux/blob/v6.8/include/linux/fs.h)、
[`fs/read_write.c`](https://github.com/torvalds/linux/blob/v6.8/fs/read_write.c)

Rust `FileExt::write_at` 只返回实际写入长度；`write_all_at` 才循环到全部完成或遇到错误。
当前 Local 自己的 `write_all_at` 还显式重试 `Interrupted`、补写 short-write 后缀，并防止
`WriteZero`，所以 5 MiB piece 在正确性上不依赖“一次 syscall 必然写满”。
[Rust `FileExt`](https://doc.rust-lang.org/stable/std/os/unix/fs/trait.FileExt.html)、
[`staged.rs`](../../src/storage/backends/local/staged.rs)

本机 page size 为 4096 B，因此 `INT_MAX & PAGE_MASK` 正好等于 `0x7ffff000`。约 2 GiB 的
syscall ceiling 不能用作工程 chunk。一个接近该值的 `spawn_blocking` task 在开始
运行后无法通过 abort 停止，会显著延长取消和 fast-fail 的观察时间；blocking thread 饱和时，
后续 task 还会排队。[Tokio `spawn_blocking`](https://docs.rs/tokio/1.52.1/tokio/task/fn.spawn_blocking.html)

## page cache、XFS 与块设备请求

当前文件没有以 `O_DIRECT` 打开，因而使用 buffered I/O。Linux iomap 文档说明 buffered
write 先更新 page cache 中的 folio 并把范围标脏，脏缓存之后由 writeback 写入存储，也可以
由 `fsync` 一类操作强制落盘。XFS buffered write 走 `iomap_file_buffered_write` 路径；用户的
一个大 `pwrite64` 会跨多个 folio/extent 迭代，不等于一个磁盘请求。
[Linux iomap buffered-I/O 文档](https://github.com/torvalds/linux/blob/v6.8/Documentation/filesystems/iomap/operations.rst)、
[Linux v6.8 XFS file path](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_file.c)、
[Linux v6.8 iomap buffered I/O](https://github.com/torvalds/linux/blob/v6.8/fs/iomap/buffered-io.c)

块层的 `max_sectors_kb`、`max_hw_sectors_kb` 限制的是请求队列，不是 buffered userspace
buffer 的 capability；内核可以把用户写入拆成多个底层请求。
[Linux block queue sysfs ABI](https://docs.kernel.org/admin-guide/abi-stable.html#abi-file-stable-sysfs-block)

`fdatasync` 等待文件数据以及后续读取所需的元数据进入持久介质。它等待的是调用时仍未完成
的脏页回写、I/O 和必要元数据，而不是简单按“此前调用过多少次 `pwrite64`”收费。因此增大
write piece 可以减少 syscall/task 数，却不能保证 Auto 的最终同步更短；后台 writeback
时机和设备状态可能掩盖或反转差异。[Linux `fsync(2)`/`fdatasync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)

## 当前代码中的实际数据流

Local source 的 `maximum_read_chunk_bytes()` 返回 2 MiB。transfer engine 再取该值与 inflight
字节预算的较小者，因此 Local→Local 的 stream piece 最大是 2 MiB。
[`source.rs`](../../src/storage/backends/local/source.rs)、[`engine.rs`](../../src/transfer/engine.rs)

Local destination 对每个上游 `Bytes` 执行以下规则：

```text
upstream piece <= 5 MiB
    -> 保持完整 piece
    -> 一个 spawn_blocking task
    -> 正常情况下一个 pwrite64

upstream piece > 5 MiB
    -> Bytes::slice 切成 <= 5 MiB 的 view
    -> 每个 view 一个有界 write task
    -> write_concurrency 控制同时在途 task 数
```

`Bytes::slice` 创建共享底层存储的 view，不复制 payload；当前实现因此能拆分 CIFS 等更大的
piece 而不做数据复制。[bytes `Bytes::slice`](https://docs.rs/bytes/1.11.1/bytes/struct.Bytes.html#method.slice)、
[`consume_input_chunks`](../../src/storage/backends/local/staged.rs)

反过来，把多个 2 MiB Local read piece 合成一个连续的 5 MiB `write_at(&[u8])` 不能只靠
`Bytes::slice` 完成：多个 allocation/view 不是一个连续 slice。当前实现没有聚合，它直接把
每个 2 MiB piece 写出。若未来希望无拷贝聚合，应另行设计 vectored positional I/O，并单独
评估 iovec 数量、partial-write 推进、checkpoint 连续前缀和并发调度；不能通过把 ceiling
从 5 MiB 改成更大自动获得这种行为。

## 已有基准的证据

仓库已有独立目标端基准覆盖 264 个正式样本：256 MiB 全矩阵测试了 ext4/XFS/tmpfs、
64 KiB 到 8 MiB、inflight 1/2/4/8；1 GiB focused confirmation 测了 ext4/XFS、1/2/4/8 MiB、
inflight 4/8。所有样本的 syscall 数都等于逻辑 piece 数，`short_writes=0`。这说明在该环境
和候选范围内正常 piece 通常由一次 `pwrite64` 接收，但不构成取消 short-write 处理的依据。
[完整方法和原始结果说明](local-write-buffer-benchmark.md)

生产常用 inflight 4 下，2 MiB 与 5 MiB 的正序、反序 1 GiB focused 结果中位数为：

| 文件系统 | piece | submit MiB/s | write + sync MiB/s | write calls/GiB |
|---|---:|---:|---:|---:|
| ext4 | 2 MiB | 1965 | 964 | 512 |
| ext4 | 5 MiB | 1930 | 890 | 205 |
| XFS | 2 MiB | 4340 | 1007 | 512 |
| XFS | 5 MiB | 4642 | 1481 | 205 |

5 MiB 将应用层 write call/task 数减少约 60%。它在 ext4 上没有吞吐优势，在该 XFS 环境上
更快；`sync_all` 波动很大，所以不能把 XFS 数值外推成所有 Local 设备的规律。更完整矩阵还
显示 4/8 MiB 没有跨文件系统、文件大小的可重复收益，ext4 的 8 MiB 明显退化。这支持
“5 MiB 是兼顾跨协议 piece 和 task 粒度的 ceiling”，不支持“越大越快”。

## Auto 与 Quick 的影响

Auto 和 Quick 使用完全相同的 write-piece 切分及 inflight 路径；两者的差别发生在持久化与
恢复阶段：Auto 会执行周期 checkpoint、最终数据同步和发布后的目录同步，Quick 跳过这些
Local durability barriers。因而：

- Quick 更直接受 submit throughput、syscall 数和 task 调度影响；
- Auto 的总耗时可能由 checkpoint/final `sync_data` 主导，write ceiling 只能间接改变脏页
  形成和回写重叠，不能消除同步成本；
- Local→Local 在两种策略下都仍是最大 2 MiB 实际 write piece，修改 5 MiB ceiling 对当前
  Local→Local 基准不会产生变化，除非同时实现目标端聚合或改变源端 chunk。

当前 `automatic_checkpoint_interval_bytes()` 返回
`10 * LOCAL_MAX_WRITE_CHUNK_BYTES`，即 50 MiB。这把恢复策略语义和写性能调优常量耦合在
一起：若未来根据新硬件把 ceiling 改为 4 MiB 或 8 MiB，checkpoint interval 会无意变为
40 MiB 或 80 MiB。建议保持本轮 5 MiB 不变，并在后续修改中把 50 MiB checkpoint interval
定义为独立常量；之后调整 write ceiling 才不会改变 Auto 的恢复门槛。

## 建议

1. 保留 `LOCAL_MAX_WRITE_CHUNK_BYTES = 5 MiB`，明确命名和文档语义是 destination
   capability ceiling，而不是固定写块。
2. Local→Local 继续直接写最大 2 MiB 的源 piece，不为凑到 5 MiB 等待或复制数据。
3. 对来自 S3 的连续 5 MiB `Bytes` 整块提交；超过 5 MiB 的输入继续用零拷贝 slice 拆分。
4. 保留 `write_all_at` 的 EINTR、short-write 和 `WriteZero` 处理。5 MiB 不是一次写满保证。
5. 不将 ceiling 提高到 8 MiB：现有数据没有跨文件系统收益，且会增大一个不可取消 task 的
   最坏工作量。若以后引入 io_uring、direct I/O、vectored aggregation 或不同存储设备，重新
   测量后再选择相应 profile。
6. 将 Auto 的 50 MiB checkpoint interval 与 5 MiB ceiling 解耦；这是语义隔离，不要求改变
   当前运行结果。

这个结论适用于当前 Linux buffered-I/O 实现。Windows 的 `seek_write`、网络文件系统客户端、
`O_DIRECT` 和 io_uring 有不同的对齐、队列与取消模型，需要分别协商和测试。
