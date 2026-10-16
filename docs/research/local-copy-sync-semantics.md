# Local copy：长度设置、同步与发布的省略条件

日期：2026-09-10。研究范围：4 MiB 新建 stage、XFS、Linux 6.8；仅研究，不修改生产实现。内核依据是上游 v6.8，不等同于已核对运行环境 Ubuntu 6.8.0-124 的全部补丁。

本地阶段数据见 [4 MiB 阶段报告](../../benchmarks/local-copy-comparison/results/4m-phase-breakdown-20260910/report.md)：去数据同步版的 `set_len` 平均 0.716 ms，发布目录同步平均 1.636 ms，rename 约 0.021 ms。它们是调用边界实测，不能直接预测删除后的净收益。

## 最优先：有条件省略末尾 set_len

Rust `File::set_len` 调整文件长度；扩展部分填零，缩短删除尾部。Linux `ftruncate` 也没有承诺同长度调用无成本。[Rust File::set_len](https://doc.rust-lang.org/std/fs/struct.File.html#method.set_len)、[Linux truncate(2)](https://man7.org/linux/man-pages/man2/truncate.2.html)

上游 Linux v6.8 的 `xfs_setattr_size` 对非零同长度没有通用提前返回：当 `newsize > ip->i_disk_size && oldsize != ip->i_disk_size` 时，执行 `filemap_write_and_wait_range(i_disk_size, newsize - 1)`，之后进入 inode 日志事务。因此，内存 EOF 已到达最终长度、磁盘 EOF 尚未跟上时，同长度 truncate 也可能触发写回等待。这是源码支持的可能机制；没有本机内核栈/tracepoint 证据，不能断言此次 0.716 ms 全部来自该分支。[Linux v6.8 xfs_iops.c：xfs_setattr_size](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_iops.c#L738-L918)

工程推论：以下条件由既有状态保证时，新建 stage 可以省略最终 `set_len`，不必额外执行一次 `stat`：

- stage 由本次排他创建，初始长度为零，传输期间由本次操作独占修改。
- 全部目标写入已成功完成，且准确覆盖 `[0, final_length)`，没有越界写入或仅依赖总字节数的重复区间计数。
- 空文件保持初始零长度；稀疏复制若省略尾部零区间，则另行保留长度设置。

恢复 stage 可能存在检查点之后的旧尾部，因此默认保留截断；只有恢复入口已完成长度归一化且后续维持上述不变量时才可同样省略。文件长度正确不代表内容已持久化，省略 `set_len` 不应连带删除数据同步。

## 数据同步与目录同步不能互相替代

`fdatasync` 保证数据及读取数据所必需的元数据（包括文件长度）同步；`fsync` 还涵盖额外文件元数据。文件同步不保证包含该文件的目录项已经落盘，持久化文件名需要显式同步目录。[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

XFS v6.8 的常规文件 fsync 首先执行 `file_write_and_wait_range`；目录 fsync 仅调用 `xfs_log_force_inode`。因此不能将较慢的目录 fsync 解释成“目录 fsync 等价于同步文件数据”。观测到等待位置变化，可能涉及先前 truncate 的写回和日志顺序，需进一步验证。[Linux v6.8 xfs_file.c](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_file.c#L66-L160)

| 成功返回的承诺 | 可以省略 | 必须保留的条件 |
|---|---|---|
| 数据及最终名字持久化 | 已证明冗余的 `set_len`；同目录的重复目录同步 | 写完后数据同步，发布后目录同步；若承诺额外属性持久化，要同步相关属性 |
| 数据持久化但不保证名字恢复 | 最终目录同步 | 明确这是较弱契约；仍同步数据 |
| 普通运行下复制完成、允许崩溃丢失 | 数据和目录同步 | 仍检查写入结果、完成区间、取消及发布结果；不能生成超出持久数据进度的可靠恢复承诺 |

表中是从同步语义推导出的接口选择，不是建议默认降低现有契约。重复目录同步的合并必须覆盖本次发布之前的修改；不能使用本次发布之前就已开始、覆盖范围不确定的同步轮次。

## Stage 和 rename

Linux rename 提供运行中的原子替换，已打开旧文件的描述符继续有效；这些可见性语义不等于断电持久化。[Linux rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)

工程建议：继续保留 stage + rename。一个 chunk 或一次 write 不能证明直接覆盖也具备原子性；写入可能部分成功，取消也可能发生在写入期间。只有明确接受目标暴露部分内容、覆盖失败破坏旧文件的接口，才可选择直接写最终路径。目前 rename 约 0.021 ms，不是最优先节省项。

同一实际父目录中的 stage → final，发布后同步一次该目录即可覆盖两次目录项变化；若父目录在操作中被移动，或兼容旧的异目录 stage，仍需保留实际受影响目录的处理。缓存目录句柄不能悄悄改变最终目标路径的解析语义。

## 建议验证顺序

1. 新建 stage 的尾部长度不变量成立时省略 `set_len`；分别测正式同步模式和去数据同步诊断模式，观察等待是否转移。
2. 保存创建 stage 时的文件句柄，减少重开文件、路径解析及调度；继续保留最终发布路径重新解析和取消检查。
3. 作为隔离诊断，增加同时移除数据与目录同步的对照，量化框架剩余开销；不能将该结果称为同持久化语义优化。
4. 如果还需要解释内核等待，跟踪 truncate 写回、XFS log force 与块层完成；在此之前不分配精确内核成本占比。

本档 4 MiB 不触发 Auto 检查点；删除恢复逻辑不会解释这一档回退。多源块仍保留 inflight。跳过可选读回校验与跳过必要写入/发布错误检查是不同的决定。
