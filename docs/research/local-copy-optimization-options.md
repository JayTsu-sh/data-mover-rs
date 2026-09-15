# Local → Local：哪些步骤可以优化或省略

日期：2026-09-10。以 4 MiB、文件并发 1、读写 inflight 各 8 为主要证据。结论不是修改生产持久化约定的授权；本轮只研究并执行隔离实验，生产实现未改。

## 结论及优先级

最值得做的是：**在已证明长度正确的新建 stage 上省略重复 set_len；复用创建时的文件句柄；缓存已校验的内部路径信息。** 保留多块 inflight、原子发布、必要的身份/字节数/错误/取消检查。保持当前持久化约定时，数据同步与目录同步不能直接省略。

| 步骤 | 处理建议 | 条件与理由 | 优先级 |
|---|---|---|---|
| 新建 stage 末尾 set_len | 有条件省略 | 本次 create_new、初始空文件、同一受控 stage、所有连续范围写入成功且无越界，长度自然正确；利用已有状态，不新增 fstat | P1，已有诊断 A/B 收益 |
| 恢复 stage 末尾截断 | 默认保留 | 可能存在检查点之后的旧尾部；write_offset=0 也不能单独证明是新文件 | 必要 |
| stage 创建后再次 open | 优化为复用 FD | prepare 已返回打开的文件，ephemeral 分支当前只缓存目录而丢弃 FD，write 又重开；用每次 stage 的私有状态保存 FD | P2，尚未测净收益 |
| stage token / 相对路径 / 文件名反复解析 | 缓存已验证结果 | 与 FD 一起放在 stage 私有状态；恢复/外部 token 必须先验证；不建立跨任务全局路径缓存 | P2，微秒级候选 |
| 源 metadata + open 后身份比对 | 保留语义，可研究合并获取 | describe 与 read 之间可能替换源；不能直接删除第二次身份绑定。未来由同一次 open/fstat 建立并持有读会话需明确生命周期和替换语义 | P3，接口改动较大 |
| 最终长度收尾的单独阻塞任务 | 条件合并/省略 | 无 set_len、无数据同步、无其他操作的诊断分支可省空任务；正式版仍需执行数据同步，可以研究同一任务串行完成后续工作 | P3；原排队/返回约 0.039 ms，不是主因 |
| 原子 rename / stage | 保留 | 实测 rename 约 0.021 ms；直接写最终路径会暴露部分内容并可能破坏旧目标，一块数据也不等于原子写 | 必要 |
| 数据 fdatasync | 正式模式保留 | 需要保证数据及必要长度元数据持久化；目录同步不替代它。关闭读回校验不代表关闭持久化 | 必要 |
| 最终父目录 fsync | 正式模式保留 | 保证发布后的目录项持久化；异步完成的独立模式才可选择省略，且不能承诺同样恢复保证 | 必要 |
| 同一个实际目录的第二次 fsync | 已省略 | 现有代码已比较 dev/ino；移动父目录和旧布局异目录 stage 仍需处理两个实际目录 | 已优化，无新收益 |
| 重新解析最终父目录 / inode 判断 | 保留 | 父目录可能在复制过程中移动，缓存旧 FD 不能替代最终路径语义；已有对应回归测试 | 必要 |
| payload 读回和内容哈希 | 用户关闭时省略，当前已做 | 固定覆盖发布不读取既有目标；关闭验证时不可伪造已校验证据 | 已优化 |
| 4 MiB Auto 检查点、claim、全局注册 | 当前不执行 | 低于自动阈值；删除相关空判定几乎无价值。大文件仍保留分批检查点及其持久化顺序 | 已优化 |
| 每块字节计数、写错误、取消、顺序约束 | 保留最小集合 | 连续完整写入是省略 set_len 的证明基础；错误与最终提交前取消不能为省几个分支而删掉 | 必要 |
| 两块读取、两块写入 / inflight | 保留 | 本轮实际 payload 读写未出现主要回退；不为 4 MiB 特例改变独立协商的块大小，或复制聚合成连续缓冲区 | 保留 |
| 旧版属性设置 | 仅无属性要求时可省 | 旧版会应用源权限、属主、时间，新版示例未请求；不能把删除它视为同功能优化 | 按调用契约 |

“可省略”指在条件已经由状态或已有流程证明时省略；不建议为了每个优化新增逐块 stat、剩余耗时估计、预算协商。仅在 prepare/recover 边界标记 stage 来源，热路径复用已有字节/范围证据。

## 新增隔离 A/B：省去 set_len 是否只是转移等待

基底是此前**去数据同步版**。变体仅去掉 sync_written_file 内的 set_len，保留原阻塞任务结构、stage、rename 和目录同步。这个补丁直接跳过该调用，仅用于本次新建且完整写入的 4 MiB 场景，不能直接应用到全部恢复/错误路径。

同轮轮换三组各 40 次，另各一次预热；源缓存预热，目标新建且预先同步，传输内读回关闭，计时外完整 SHA-256 校验、精确目标文件集合检查、同步输出后清理。120 个计时样本及 3 个预热全部通过内容检查。跟踪另跑 12 次，与性能统计分开。

| 实现 | P50 ms | P95 ms | 平均 ms |
|---|---:|---:|---:|
| StorageEnum | 3.239 | 3.666 | 3.299 |
| 去数据同步版 | 5.370 | 5.857 | 5.504 |
| 再省去末尾 set_len | 3.548 | 4.310 | 3.667 |

去掉 set_len 后中位耗时改善 **33.9%**，相对同轮 StorageEnum 仍慢 **9.6%**。这个实验支持该调用是有价值的优化对象，但只证明当前去同步模式，不证明正式同步模式能获得相同比例。

一个独立 trace 样本中，基底目录 fsync 1.720 ms，省 set_len 后为 0.599 ms。说明移除 set_len 不仅消除其直接耗时，还可能改变后续同步依赖。不能将原来的 0.716 ms 直接视作可节省量，也不能将这里的净改善推广到其他文件系统或正式同步路径。

原始证据：[样本和补丁目录](../../benchmarks/local-copy-comparison/results/4m-optimization-research-20260910/)，其中 samples/result.json、summary.json、environment.json、omit-final-setlen.patch、run.sh、trace/ 分别记录测量、汇总、二进制哈希、变体、运行命令和系统调用。生产源码及共享正式版二进制保持原样。

## set_len 为什么可能昂贵

Linux v6.8 的 XFS 对非零同长度 truncate 没有一般性的提前返回；磁盘记录长度落后于内存长度时，可能执行写回等待，之后进入 inode 日志事务。它不是简单的内存赋值。[上游 xfs_setattr_size](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_iops.c#L738-L918)

这是与现象一致的源码解释；运行环境内核为 Ubuntu 6.8.0-124，未审计所有发行版补丁，也未抓到本机内核栈，故不声称本次每个等待均已定位到该分支。完整同步契约和来源见 [同步语义研究](local-copy-sync-semantics.md)。目录 fsync 与文件 fsync 在 XFS 中走不同路径，前者不能代替负载数据同步。[上游 XFS fsync 实现](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_file.c#L66-L160)、[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

## 实施时必须明确的省略条件

- “新建”是本次排他 create_new 的来源标记，不是 write_offset==0；该标记放在 Local 的类型化 backend_state 中，恢复路径重新建立自己的状态。
- 必须保证写入同一 stage，拥有排他修改约束。复用 FD 应维护 FD 与后续发布名字的一致性，不能把取消/失败后的失效句柄跨尝试复用。
- 全部写任务已结束、每个位置写返回完整成功、写入范围连续覆盖 [0,N)、无区间超过 N。现有 issued 单调推进、write_all_at、drain 和最终长度计数可作为基础，而不是再遍历文件。
- 新建空文件自然保持零长度；若将来引入跳过尾部零区间的稀疏写，必须保留显式长度扩展。
- 错误/取消路径、恢复旧尾部、同一 stage 的重复写入需要独立处理，不能套用“第一次新建完整成功”结论。
- 保留数据同步顺序：完整写入 → 数据持久化 → 原子发布 → 目录持久化；恢复记录不能领先于已持久数据。

## 建议的实现顺序与验证

1. 先实现局限于 Local、明确新建完整成功路径的 set_len 省略，保留恢复/失败路径及正式数据同步。测试新建空文件、4 MiB、乱序写完成、部分写错误、恢复带尾部、取消不发布。分别复测正式版和去同步诊断版；此时才量化正式收益。
2. 再做 stage FD 与解析结果复用，确保 prepare/write/verify/publish/discard 的状态和生命周期一致，覆盖失败清理、恢复重开、父目录移动和取消。单独测净收益，避免把多个改变混在一起。
3. 暂不优化规划分支、rename 或增加一套特殊 4 MiB 复制通道。以单独阶段计时确认剩余瓶颈后再决定。

## 本地代码与约定依据

- [prepare_mode 与 stage 生命周期](../../src/storage/backends/local/staged.rs)：create_new 返回 FD，但 ephemeral backend_state 只保留目录；open_stage_file_for 再打开文件；consume_input_chunks 保持单调位置写；sync_written_file 执行 set_len + sync_data。
- [恢复](../../src/storage/backends/local/staged/recovery.rs)：重建 stage 并恢复 checkpoint 偏移，不在入口统一截断旧尾部。
- [源端绑定](../../src/storage/backends/local/source.rs)：describe 和 open_bound_file 的身份比对。
- [发布与目录同步](../../src/storage/backends/local/staged/publication.rs)、[同步轮次](../../src/storage/backends/local/staged/directory_sync.rs)：提交前取消、当前最终父目录、同 inode 省去第二次同步。
- [执行与恢复 ADR](../adr/0001-local-transfer-execution-and-recovery.md)：读回可选、持久化和原子发布必须保留、多块 inflight、只规划一次。
- [上一轮阶段计时](../../benchmarks/local-copy-comparison/results/4m-phase-breakdown-20260910/report.md)：本报告的成本优先级依据。
