// 标准库
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// 外部 crate
use tokio::sync::{Mutex, Notify};
use tracing::{error, info};

/// 工作窃取调度器的 worker 上下文
///
/// 封装了并发目录遍历中每个 worker 所需的调度基础设施：
/// - 自有任务栈（LIFO）和邻居栈（用于 FIFO 窃取）
/// - 活跃生产者 / 活跃任务计数器
/// - 异步通知机制（替代轮询 sleep）
///
/// `T` 为任务类型，各后端不同：
/// - Local: `(PathBuf, usize, bool)`
/// - NFS:   `(String, Bytes, usize, bool)`
/// - S3:    `(String, usize, bool)`
pub(crate) struct WorkerContext<T: Send> {
    pub worker_id: usize,
    my_stack: Arc<Mutex<VecDeque<T>>>,
    neighbor_stacks: Vec<Arc<Mutex<VecDeque<T>>>>,
    active_producers: Arc<AtomicUsize>,
    active_tasks: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl<T: Send> WorkerContext<T> {
    /// LIFO 弹出自己栈的任务，失败则 FIFO 窃取邻居最旧（最浅）的任务
    ///
    /// 注意：必须先将自己栈的弹出结果存入变量让 `MutexGuard` 立即释放，
    /// 否则在窃取邻居任务时会同时持有自己栈的锁，可能导致死锁。
    pub async fn pop_task(&self) -> Option<T> {
        let own_task = self.my_stack.lock().await.pop_back();
        if let Some(task) = own_task {
            return Some(task);
        }
        for neighbor in &self.neighbor_stacks {
            if let Some(task) = neighbor.lock().await.pop_front() {
                return Some(task);
            }
        }
        None
    }

    /// 将新任务推入自己的栈（LIFO），并递增活跃任务计数器、通知等待中的 worker
    pub async fn push_task(&self, task: T) {
        self.my_stack.lock().await.push_back(task);
        self.active_tasks.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// 标记当前 worker 开始处理任务
    pub fn begin_processing(&self) {
        self.active_producers.fetch_add(1, Ordering::Release);
    }

    /// 标记当前 worker 完成任务处理
    pub fn end_processing(&self) {
        self.active_producers.fetch_sub(1, Ordering::Release);
        self.active_tasks.fetch_sub(1, Ordering::Release);
    }

    /// 检查是否所有任务都已完成（无活跃任务且无活跃生产者）
    pub fn is_done(&self) -> bool {
        self.active_tasks.load(Ordering::Acquire) == 0
            && self.active_producers.load(Ordering::Acquire) == 0
    }

    /// 等待新任务通知，超时 100μs 后返回（避免忙等，同时防止通知丢失）
    pub async fn wait_for_task(&self) {
        tokio::time::timeout(Duration::from_micros(100), self.notify.notified())
            .await
            .ok();
    }
}

/// 创建 N 个 WorkerContext，初始任务放入 worker 0 的栈
///
/// concurrency 会被 clamp 到 [1, 64] 范围
pub(crate) async fn create_worker_contexts<T: Send>(
    concurrency: usize,
    initial_task: T,
) -> Vec<WorkerContext<T>> {
    let concurrency = concurrency.clamp(1, 64);

    let stacks: Vec<Arc<Mutex<VecDeque<T>>>> = (0..concurrency)
        .map(|_| Arc::new(Mutex::new(VecDeque::new())))
        .collect();

    // 初始任务放入 worker 0 的栈
    stacks[0].lock().await.push_back(initial_task);

    let active_producers = Arc::new(AtomicUsize::new(0));
    let active_tasks = Arc::new(AtomicUsize::new(1)); // 初始任务计数为 1
    let notify = Arc::new(Notify::new());

    (0..concurrency)
        .map(|i| {
            let my_stack = Arc::clone(&stacks[i]);
            let neighbor_stacks: Vec<Arc<Mutex<VecDeque<T>>>> = stacks
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, s)| Arc::clone(s))
                .collect();

            WorkerContext {
                worker_id: i,
                my_stack,
                neighbor_stacks,
                active_producers: Arc::clone(&active_producers),
                active_tasks: Arc::clone(&active_tasks),
                notify: Arc::clone(&notify),
            }
        })
        .collect()
}

/// 运行工作窃取循环
///
/// 每获取一个任务调用 `process_fn`，若返回 Err 则记录错误日志但不中断循环。
/// `task_display` 用于错误日志中显示任务信息。
pub(crate) async fn run_worker_loop<T, F, Fut, D>(
    ctx: &WorkerContext<T>,
    mut process_fn: F,
    task_display: D,
) where
    T: Send,
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = crate::Result<()>>,
    D: Fn(&T) -> String,
{
    loop {
        if let Some(task) = ctx.pop_task().await {
            ctx.begin_processing();

            let task_info = task_display(&task);
            if let Err(e) = process_fn(task).await {
                error!(
                    "[Worker {}] Failed to process {}: {:?}",
                    ctx.worker_id, task_info, e
                );
            }

            ctx.end_processing();
        } else if ctx.is_done() {
            info!("[Worker {}] All tasks completed, exiting", ctx.worker_id);
            break;
        } else {
            ctx.wait_for_task().await;
        }
    }
}
