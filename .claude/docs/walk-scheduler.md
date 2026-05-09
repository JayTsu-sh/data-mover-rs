# Walk Scheduler (work-stealing)

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

- `cifs.rs` walkdir / walkdir_2
- `nfs.rs` walkdir / walkdir_2
- `s3.rs` walkdir (S3 list 本身分页，walk 是平的，但仍走 scheduler 统一并发)
- `local.rs` walkdir / walkdir_2

## 已知陷阱

| 陷阱 | 应对 |
|---|---|
| `is_done` 早退 | 必须同时检 active_tasks + active_producers |
| 窃任务被取走两次 | FIFO 用原子 take，head 推进 |
| 长尾任务卡死 | 自栈 LIFO 让最深的子任务先做完，避免 leaf 任务积压 |
| Notify 丢失 wake | tokio Notify 有 permit 机制，不会丢 |
| 跨 worker 数据共享 | 任务自包含，不要全局可变状态 |

## 调参

- worker 数量 — 默认 `num_cpus`，可配置。
- 队列容量 — 看任务粒度 (粗粒度 → 小队列；细粒度 → 大队列)。

## 改 walk_scheduler 时

1. 改并发原语必须跑压测。
2. 用 `loom` (未引入) 验证 atomic 顺序 — 当前依赖代码 review。
3. 改完跑 `make test` (含 `tests/test_copy_file_cancel.rs` 间接覆盖 walk)。
4. 改完压测 4 个 backend 的 walkdir 性能。
