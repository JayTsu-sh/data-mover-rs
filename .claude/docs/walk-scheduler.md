# Walk Scheduler (work-stealing)

> 本页主要讲 **legacy** `StorageEnum::walkdir` 的 work-stealing 调度。role-based 遍历
> (`traversal::StorageTraversalSource`) 有两种：默认的 `Unordered` (P7 C10，同样是 worker 栈 + 窃最浅，
> 与 legacy 的差别见文末「role 的 Unordered 调度器」) 与 `Blocks` (深度优先块顺序 + 列举预读，
> `src/traversal/storage/cursor.rs`)。两者见 `.claude/docs/architecture.md`「遍历的三套处理逻辑」。

## 文件

- `src/walk_scheduler.rs` (147 行) — `WorkerContext<T>`。
- `src/async_receiver.rs` (31 行) — mpsc 接收端 wrapper。

## 数据结构

```rust
struct WorkerContext<T> {
    own_stack:  LIFO<T>,     // 自栈 (LIFO 局部性)
    peer_queues: Vec<&FIFO<T>>,  // 邻居 FIFO 引用
    active_tasks:    AtomicUsize,
    active_producers: AtomicUsize,
    notify: Notify,
}
```

## 核心方法

### `pop_task()`

按优先级：

1. **自栈 LIFO 弹出** — 保留 cache locality (刚 push 的子任务最可能 hot)。
2. 自栈空 → **窃邻居 FIFO 最旧任务** — 窃最旧减少与邻居的 head 冲突。
3. 邻居都空 → wait on `notify`。

### `push_task(task)`

1. push 到自栈。
2. `active_tasks.fetch_add(1)`.
3. `notify.notify_waiters()` 唤醒等待的 worker。

### `is_done()`

```rust
fn is_done(&self) -> bool {
    self.active_tasks.load() == 0
        && self.active_producers.load() == 0
}
```

**两个计数都得是 0** — `active_tasks=0` 意味着没有待处理任务，但如果 producer 还活着可能还会 push，所以也要等 producer 退出。

## AsyncReceiver

```rust
pub struct AsyncReceiver<T> {
    inner: tokio::sync::mpsc::Receiver<T>,
}

impl<T> AsyncReceiver<T> {
    pub async fn next(&mut self) -> Option<T> { ... }
}
```

薄 wrapper — 给 `DeleteDirIterator` / `WalkDirAsyncIterator2` 用，避免外部直接持有 tokio 类型。

## 谁在用

所有 backend 的 walkdir 实现走这套：

- `nfs.rs` walkdir / walkdir_2
- `s3.rs` walkdir (S3 list 本身分页，walk 是平的，但仍走 scheduler 统一并发)
- `local.rs` walkdir / walkdir_2

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| `is_done` 早退 | 必须同时检 active_tasks + active_producers |
| 窃任务被取走两次 | FIFO 用原子 take，head 推进 |
| 长尾任务卡死 | 自栈 LIFO 让最深的子任务先做完，避免 leaf 任务积压 |
| Notify 丢失 wake | **会丢**：`push_task` 用的 `notify_waiters()` 不存 permit，对当时没在等的 worker 就是丢。存 permit 的是 `notify_one()`。现在靠 `wait_for_task()` 的 100μs 超时兜底 (空闲时每 worker 每秒醒 1 万次)。正解是改 `notify_one()` 并去掉超时。同一成因见 smb-rs PR #80 |
| 跨 worker 数据共享 | 任务自包含，不要全局可变状态 |

## 调参

- worker 数量 — 默认 `num_cpus`，可配置。
- 队列容量 — 看任务粒度 (粗粒度 → 小队列；细粒度 → 大队列)。

## 改 walk_scheduler 时

1. 改并发原语必须跑压测。
2. 用 `loom` (未引入) 验证 atomic 顺序 — 当前依赖代码 review。
3. 改完跑 `make test` (含 `tests/test_copy_file_cancel.rs` 间接覆盖 walk)。
4. 改完压测 4 个 backend 的 walkdir 性能。

## role 的 Unordered 调度器 (P7 C10) 与 legacy 的差别

`src/traversal/storage/unordered/pool.rs`。形状相同 (每 worker 一个 LIFO 栈、空了窃别人栈底最浅的)，
下面几处是有意改的：

| | legacy `WorkerContext` | `Unordered` 的 `Pool` |
|---|---|---|
| 终止 | `active_tasks` + `active_producers` 两个计数都为 0 | 一个 `outstanding`：子目录入栈前 `fetch_add`，父目录全部子目录入栈后才 `fetch_sub`；减到 0 的 worker 取消 `halt` |
| 唤醒 | 每次 push `notify_waiters()` (不存 permit，会丢) + 100 µs 超时兜底 | 只在有空闲 worker 时 `notify_one()`；空闲登记与压栈两侧 SeqCst fence，不会丢；窃取后受害者有剩余则级联再唤醒一个；没有超时轮询 |
| 窃取 | 依次锁邻居，偷 FIFO 头 | 读各槽位发布的 `front_depth` 挑最浅的，只锁它与自己 (按下标序)，出栈与登记 `active` 同一临界区 |
| 前沿 | 无 | 每个未完成目录任何时刻都在某槽位的栈上或 `active` 里 (C11 检查点的前提；测试构建下每把槽位锁都经 `pool/checked.rs` 的 `SlotGuard`，线程放开最后一把时锁全部槽位核对，出栈 / 窃取拆成两个临界区必然失败) |
| 输出 | 每条目一次 channel send | 每页一批 (`traversal/channel.rs`)，按条计的背压；目录的 Entry 所在批交出后才压它的子目录 |
| 列举中的资源 | — | worker 等消费方时列举一直开着：K ≤ `parked_listing_limit` (CIFS 8) |

故障注入 (P7 C10，全部变红)：`fetch_sub` 提前、窃取先出栈后登记、自己出栈后才登记、K 不受停车上限约束、
先压栈后交批 等，见设计 §14 C10 行。

## 续扫检查点 (P7 C11)

`TraversalRequest::resumable(Resumable::On { every })` 时 (`src/traversal/checkpoint.rs` 编解码与指纹，
`src/traversal/storage/resume.rs` + `resume/{positions,relist,blocks}.rs`)：

- **Unordered**：worker 每页压栈后、每个目录 finish 后看一次是否到期 (一个 CAS 认领一个周期)，到期就
  `Pool::snapshot` —— 按下标序锁住全部槽位拷出栈 + `active`，不暂停 worker —— 去掉祖先也在前沿里的目录后编码发出。
  取消 / 失败时 join 全部 worker 后取最终前沿，作为会话最后一项。续扫时前沿作为种子放进 0 号槽位。
- **Blocks**：游标每进入一个目录记 (块开始的序号, 位置链)；检查点取序号 ≤ `Output::delivered` 的最新一条。
  续扫先把链上祖先重列一遍 (不发出，只留名字大于下一环的子目录)，再照常驱动。
- Off 时 `Shared::resume` 为 `None`：不读时钟、不记位置、不取快照，每页 / 每轮一次 `Option` 判断。
- 设计与不变式见 P7 设计 §9.3。
