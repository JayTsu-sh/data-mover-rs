use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::CommitCallback;

/// 跨协议写入管道共享的进度上报参数。
#[derive(Clone)]
pub(crate) struct WriteProgress {
    pub bytes_counter: Option<Arc<AtomicU64>>,
    pub on_committed: CommitCallback,
}
