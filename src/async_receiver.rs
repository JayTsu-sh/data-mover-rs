/// 通用异步通道接收器封装。
///
/// 基于 [`async_channel::Receiver<T>`]，天然支持多 worker 并发 clone，
/// 无需外部 Mutex，各 worker 独立等待，消除锁竞争。
///
/// 通道关闭（所有发送端 drop）时 [`next`](AsyncReceiver::next) 返回 `None`，
/// 可作为流式迭代的自然终止信号。
///
/// # Thread Safety
///
/// `AsyncReceiver<T>` 可在线程间 Send + Sync（与 `async_channel::Receiver` 一致）。
pub struct AsyncReceiver<T> {
    rx: async_channel::Receiver<T>,
}

impl<T> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

impl<T> AsyncReceiver<T> {
    pub fn new(rx: async_channel::Receiver<T>) -> Self {
        Self { rx }
    }

    /// 异步获取下一个元素，通道关闭时返回 None
    pub async fn next(&self) -> Option<T> {
        self.rx.recv().await.ok()
    }
}
