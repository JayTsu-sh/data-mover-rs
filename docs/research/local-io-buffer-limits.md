# Local buffered positional I/O 的 buffer 上限：1 MiB 还是 2 MiB

- 调研日期：2026-09-01
- 范围：Linux 上的普通文件、buffered I/O、Rust `std::os::unix::fs::FileExt::{read_at, write_at}`；不涵盖 `O_DIRECT`、Windows 或网络文件系统
- 仓库基线：`76fdb81738df32a85284a974e1fea7570590a2b9`，并检查了当前工作区中的 Local backend 改动

## 结论

| 层次 | 数值 | 含义 |
|---|---:|---|
| Linux syscall 硬上限 | `0x7ffff000` = 2,147,479,552 B | 一次 `pread/pwrite` 最多实际传输这么多；不是性能建议值 |
| Tokio 1.52.1 `fs::File` 默认内部上限 | **2 MiB** | 仅约束 Tokio `AsyncRead`/`AsyncWrite` 路径；可以显式修改 |
| 历史/现存 legacy Local 源端上限 | **2 MiB** | 自 terrasync-rs 初始实现起，默认值和用户可配置值的硬上限都是 2 MiB |
| role-based Local 源端原上限 | **1 MiB** | 2026-08-29 新增新架构实现时单独硬编码，与 legacy 行为发生分叉 |
| 当前 role-based Local 源端上限 | **2 MiB** | 根据本调研结论恢复为显式 backend capability ceiling |
| 建议的 Local `max_read_chunk_bytes` | **2 MiB** | 恢复 Local 已有的协议事实；实际 chunk 仍须与调用方请求、inflight 字节预算和 QoS 取最小值 |

重新核对后的答案是：**如果问题是 data-mover 的 Local `max read chunk` 工程协商上限，2 MiB 更符合项目既有设计。** 1 MiB 不是 Linux、Rust 或 Local 文件系统的限制，而是新 role-based backend 引入时新增的保守常量。现有实测也不能证明 2 MiB 是所有设备上的吞吐最优点，因此应把 2 MiB 定义为“backend 可接受的最大值”，而不是要求每次读取都固定使用 2 MiB。

写端已通过独立基准决策为 5 MiB ceiling。历史 2 MiB 常量只直接控制 Local 源端读 chunk；目标端对不超过 5 MiB 的上游 `Bytes` 整块提交，对更大输入在 backend 内拆分，并处理 short write 和 `EINTR`。读写上限相互独立，详见[独立写入基准](local-write-buffer-benchmark.md)。

## 第一手来源

### Linux syscall

`pread()`/`pwrite()` 分别从指定 offset 最多读写 `count` 字节，不改变共享文件游标，并且成功返回也允许少于请求长度。[Linux `pread(2)`/`pwrite(2)`](https://man7.org/linux/man-pages/man2/pread.2.html)

Linux `read()`、`write()` 及同类调用一次最多传输 `0x7ffff000` 字节；read 与 write 的官方 man-pages 均给出相同数值。[`read(2)`](https://man7.org/linux/man-pages/man2/read.2.html#NOTES)、[`write(2)`](https://man7.org/linux/man-pages/man2/write.2.html#NOTES)

内核把该值定义为 `MAX_RW_COUNT = INT_MAX & PAGE_MASK`，并在进入文件系统实现前把 read/write 的 `count` 截到这个值。[Linux `include/linux/fs.h`](https://github.com/torvalds/linux/blob/abdf623ddb75b24659018d3952d8f61937306ae5/include/linux/fs.h#L2441-L2445)、[`fs/read_write.c`](https://github.com/torvalds/linux/blob/abdf623ddb75b24659018d3952d8f61937306ae5/fs/read_write.c#L560-L570)

这意味着传入大于 `0x7ffff000` 的 Rust slice 不会让一次 syscall 传输更多数据，只会形成合法的短读/短写，调用者仍需循环。

### Rust `FileExt`

Rust `FileExt::read_at` 明确允许 short read；`write_at` 返回实际写入长度。其 `read_exact_at`/`write_all_at` convenience methods 也是通过重复调用完成整个 buffer。[Rust `FileExt` 文档](https://doc.rust-lang.org/stable/std/os/unix/fs/trait.FileExt.html)

Linux 上 Rust std 最终调用 `pread64`/`pwrite64`。std 先把 slice 长度限制到 `ssize_t::MAX`，但 Linux 内核更低的 `MAX_RW_COUNT` 仍是最终单 syscall 上限。[Rust Unix fd 实现：read_at](https://github.com/rust-lang/rust/blob/70222712809cd5cc1718ed8995914a1cbacb6b92/library/std/src/sys/fd/unix.rs#L187-L197)、[write_at](https://github.com/rust-lang/rust/blob/70222712809cd5cc1718ed8995914a1cbacb6b92/library/std/src/sys/fd/unix.rs#L418-L428)

当前读取实现先用 `vec![0; length]` 分配完整 chunk。Rust `Vec` 的理论 allocation 上限为 `isize::MAX` 字节，但实际可分配内存、Linux syscall 上限和 data-mover 的并发内存预算都更严格。[Rust `Vec` 文档](https://doc.rust-lang.org/stable/std/vec/index.html)

### Tokio 的 2 MiB 来源

本仓库 `Cargo.lock` 锁定 Tokio 1.52.1。该版本将 `DEFAULT_MAX_BUF_SIZE` 定义为 **2 MiB**，`tokio::fs::File::from_std` 用它初始化 `max_buf_size`，`poll_read` 再取调用方剩余 buffer 与该值的较小者。[Tokio 1.52.1 `blocking.rs`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/io/blocking.rs#L20-L28)、[`File::from_std`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L282-L291)、[`File::poll_read`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L596-L622)

Tokio 同时公开 `set_max_buf_size`，所以 2 MiB 仍是 Tokio 的默认调度/复制粒度，不是操作系统硬上限。[Tokio `File::set_max_buf_size`](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/fs/file.rs#L563-L592)

这一限制只适用于 Tokio 文件的 `AsyncRead`/`AsyncWrite` 实现。legacy Local 在提交 [`a705a222`](https://github.com/JayTsu-sh/data-mover-rs/commit/a705a222f6a26ab3e1551df4cd8082a58700eee2) 后，以及当前 role-based Local，都把句柄转换为 `std::fs::File` 并直接调用 `FileExt::read_at`；它们已经绕过 Tokio 的 2 MiB 内部 buffer。因此今天选择 2 MiB 应是 data-mover 的显式协商策略，而不是依赖 Tokio 的隐式限制。

### ext4 与 block device

ext4 的 block 通常为 4 KiB，官方文档讨论的是 allocation granularity、文件系统大小和最大文件大小，并没有把一个 buffered `pread/pwrite` buffer 限制为一个 block。[ext4 block 文档](https://github.com/torvalds/linux/blob/abdf623ddb75b24659018d3952d8f61937306ae5/Documentation/filesystems/ext4/blocks.rst#L3-L15)

本机 `/sys/block/{sda,sdc}/queue/max_sectors_kb` 都是 1280 KiB，`max_hw_sectors_kb` 都是 32767 KiB；这些是 block layer/硬件请求边界，内核可以把一个更大的 buffered 用户 I/O 拆成多个底层请求，不能直接拿来作为用户 buffer 上限。[Linux queue sysfs 文档](https://docs.kernel.org/admin-guide/abi-stable.html#abi-file-stable-sysfs-block)

## 当前仓库事实

### 2 MiB 的项目历史

terrasync-rs 的初始提交 [`06978e42`](https://github.com/JayTsu-sh/terrasync-rs/commit/06978e42a74ecd393cfdd73f34b454bbe3824e80) 已定义 `DEFAULT_BLOCK_SIZE = 2 * MB`，构造器还用 `min(size, DEFAULT_BLOCK_SIZE)` 将用户请求收紧到 2 MiB。读取时 `calculate_chunk_size` 同样注明最大为 2 MiB。[初始 `local.rs`](https://github.com/JayTsu-sh/terrasync-rs/blob/06978e42a74ecd393cfdd73f34b454bbe3824e80/crates/storage_v2/src/local.rs#L138-L159)

存储代码从 terrasync-rs 抽取到本仓库的提交 [`4289a15c`](https://github.com/JayTsu-sh/data-mover-rs/commit/4289a15cf09e5f773c61dc09f7f89e87a4ea5a8e) 保留了相同常量；当前 [`src/local.rs`](../../src/local.rs) 仍是 2 MiB，测试也明确把 Local chunk shape 标为 2 MiB（[`test_local_positional_io.rs`](../../tests/test_local_positional_io.rs)）。2026-08-11 的 positional-I/O 优化只把 Tokio 共享游标改为 `std::fs::FileExt::read_at`，没有修改 2 MiB 上限。

2026-08-29 提交 [`86371cf1`](https://github.com/JayTsu-sh/data-mover-rs/commit/86371cf1bd58553491e7aa93e6696a6f2ab19f63) 新建 role-based Local backend 时，另行写入 `MAX_READ_CHUNK = 1 MiB`，并在架构文档中记录新 range stream 的 1 MiB cap；该提交未包含 1 MiB 对 2 MiB 的性能对比或迁移理由。

调研时 [`source.rs`](../../src/storage/backends/local/source.rs) 的 1 MiB 是“新架构当时的实现事实”，不是历史 Local capability。根据本调研结论，role-based Local 已恢复 2 MiB ceiling。它每次发起的 range 取剩余长度、调用方 `maximum_chunk_bytes` 与 2 MiB 的较小者。`read_file_range` 为该 range 一次性分配 `Vec<u8>`，首先对完整 range 发起一次 `read_at`；只有发生 short read 时才继续读取缺失后缀。

写入端在 [`staged.rs`](../../src/storage/backends/local/staged.rs) 中没有自己的最大 chunk 常量。每个上游 `Bytes` 被整体移交给一个 blocking task，task 循环调用 `write_at` 直到全部写完，因此能优雅处理 short write。`write_concurrency` 只限制同时在途的 piece 数量，不限制每个 piece 的字节数。

由此得到当前工作区的有效行为：

- Local 源端的每个输出 piece 最大 2 MiB；调用方 ceiling、inflight 字节预算和 QoS 还可能把它缩小。
- Local 目标端当前可接受任意上游 `Bytes`；它的工程写入上限不从读端数值推导，等待独立基准决定。
- caller 请求大于 2 MiB 时 Local 收紧为 2 MiB；caller 请求更小时严格尊重更小值。
- 若来自其他协议的源端发出更大 piece，Local 写端会接受它，但在途内存和不可取消时间也会随之增大；因此仍需显式的 write capability 上限。
- Local 性能脚本和 comparison example 默认请求 2 MiB；修正后 legacy 与 role-based backend 可以在相同实际 read chunk 下比较。

## 当前机器 probe

环境：Linux `6.8.0-124-generic`；`/tmp` 为 `/dev/sda1` ext4，`/work` 为 `/dev/sdc` XFS，另测 `/dev/shm` tmpfs；页大小 4 KiB。probe 使用 buffered `pread/pwrite`，每个样本处理 512 MiB，写侧结束后 `fdatasync`，每个 buffer size 做三轮并取中位数。

| buffer | ext4 read MiB/s | ext4 write+sync | XFS read | XFS write+sync | tmpfs read | tmpfs write+sync |
|---:|---:|---:|---:|---:|---:|---:|
| 4 KiB | 1688 | 642 | 1926 | 635 | 4009 | 1590 |
| 64 KiB | 6541 | 738 | 7324 | 1059 | 23442 | 2769 |
| 256 KiB | **7606** | 808 | **7876** | 1007 | 29728 | **2928** |
| 1 MiB | 6641 | 827 | 7465 | 1017 | **30063** | 2720 |
| 2 MiB | 6650 | 785 | 6568 | 970 | 19576 | 2807 |
| 4 MiB | 6401 | 834 | 6252 | **1109** | 19180 | 2841 |
| 8 MiB | 6206 | 764 | 6578 | 1047 | 19138 | 2807 |
| 16 MiB | 5596 | 734 | 6116 | 1002 | 17328 | 2592 |
| 32 MiB | 4758 | 769 | 4911 | 956 | 10446 | 2364 |
| 64 MiB | 4619 | **883** | 4593 | 957 | 7639 | 2304 |

64 MiB 的单次 `pread/pwrite` 在三个环境都成功，直接证明 1 MiB 不是 API 或文件系统硬上限。与此同时，更大 buffer 没有带来稳定收益：ext4/XFS 的 buffered read 峰值在 256 KiB，tmpfs 在 1 MiB；写吞吐对 chunk 相对不敏感。

局限：这是当前机器的短时、单进程、顺序 buffered I/O probe，read 数值明显包含 page-cache 影响；没有控制 NUMA、CPU frequency、后台 I/O、冷缓存或不同设备队列。它适合排除“1 MiB/2 MiB 是硬上限”并筛选工程候选值，不足以宣称某个 buffer 是所有 Local 环境的全局最优值。原始临时结果位于 `/tmp/local-io-{ext4,xfs,tmpfs}{,-2,-3}.csv`，不会作为长期测试证据保留。

这组数据也没有证明 2 MiB 比 1 MiB 更快：ext4 两者基本相同，XFS/tmpfs 的 2 MiB 样本更低。它不否定把 2 MiB 暴露为协商**上限**，但说明运行时应允许内存预算、QoS 或未来的环境调优把实际 chunk 降到 1 MiB 或更小。

## 协商建议

1. Local capability 的 `max_read_chunk_bytes` 设为 **2 MiB**，与 terrasync-rs 初始实现、legacy Local 和现有 Local 测试语义对齐；不要把 `0x7ffff000`、Tokio 默认值本身或磁盘 `max_sectors_kb` 当作证明。
2. 最终读取 chunk 取 Local 2 MiB ceiling、调用方请求、transfer 内存窗口和 QoS grant 的最小值。返回的 short read 仍应作为正常情况循环完成。
3. `max_write_chunk_bytes` 独立定义为 **5 MiB**；不超过 ceiling 的输入整块提交，超过 ceiling 的输入由目标端拆分，并循环处理 short write 与 `EINTR`。
4. 内存预算至少按 `read_concurrency × read_chunk` 加 `write_concurrency × write_chunk` 和 transfer channel 持有量估算；不能只限制 task 数而不限制每个 `Bytes`。
5. `spawn_blocking` 已开始执行的任务不能被 abort，且 blocking pool 饱和后任务会排队。因此大到数十 MiB/数 GiB 的单 task 会直接削弱取消和 fast-fail 语义。[Tokio 1.52.1 `spawn_blocking` 文档/源码](https://github.com/tokio-rs/tokio/blob/905c146aeda741ea2202f942a7c3a606dda13da5/tokio/src/task/blocking.rs#L91-L127)
6. 在 Local→Local 的 4 KiB、40 MiB、1 GiB 基准中至少比较 256 KiB、1 MiB、2 MiB、4 MiB，并同时记录吞吐、CPU、峰值 RSS、取消完成时间和 short-I/O 正确性。基准必须记录 backend 最终协商出的实际 chunk，不能只记录调用方请求值。
