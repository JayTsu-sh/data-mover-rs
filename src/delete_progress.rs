//! Shared asynchronous deletion progress.

use crate::DeleteEvent;

/// Cloneable deletion progress receiver. Clones share events; `next(&self)` is Send + Sync.
pub struct DeleteDirIterator {
    rx: async_channel::Receiver<DeleteEvent>,
}

impl Clone for DeleteDirIterator {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

impl DeleteDirIterator {
    #[must_use]
    pub fn new(rx: async_channel::Receiver<DeleteEvent>) -> Self {
        Self { rx }
    }

    /// 异步获取下一个元素，通道关闭时返回 None
    pub async fn next(&self) -> Option<DeleteEvent> {
        self.rx.recv().await.ok()
    }
}
