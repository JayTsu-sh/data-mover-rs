//! NFS/CIFS/Local 写入管道消重（issue #21 PR#A）。
//!
//! `ChunkSink` 吸收三种后端的写原语与落盘屏障差异（Local `sync_all` /
//! NFS FILE_SYNC no-op / CIFS `flush`），`write_pipeline_core` 统一 inflight
//! 并发调度、分块派发、错误处理与进度上报时序。三种后端各自的
//! `write_data`/`write_data_resumable` 收薄为「开文件（截断/不截断）→ 构造
//! sink → 调用 core → close」。
//!
//! **S3 不接入此管道**——S3 的两个变体（singlepart/multipart vs part 粒度续传）
//! 没有共享的写入原语（无随机写、无 flush 屏障、offset 必须 part 对齐），
//! 套用统一 sink 只是假统一；且 S3 字节级续传刚落地，风险最高，维持独立实现
//! （见 `s3.rs`）。
//!
//! ## Local 并入 core（inflight=1）的等价性说明
//!
//! Local 历史实现是严格顺序写：收到一个 chunk → await 写完 → 再收下一个。
//! 并入 `write_pipeline_core` 后用 `inflight=1` 表达同样的约束，但需要注意
//! 调度细节：本模块把「派发新写 → 若已达 inflight 上限则等一个写完」的检查放在
//! **派发之后**（而非 NFS/CIFS 原实现的「派发前先等一个」），这样 `inflight=1`
//! 时每个 chunk 的写会在**下一个 chunk 被接收之前**就完整 await 完成，与
//! Local 原有的严格顺序（无提前一个 chunk 的 pipeline 滞后）逐字节等价；
//! 对 `inflight>1`（NFS/CIFS）而言，稳态并发窗口仍是 `inflight`，仅初始/收尾
//! 阶段的逐出时机相差不超过一个槽位，不影响正确性与稳态吞吐。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;

use crate::error::StorageError;
use crate::{CommitCallback, DataChunk, Result};

/// 写入原语：吸收 NFS/CIFS/Local 的落盘/并发差异，供 `write_pipeline_core` 统一调度。
///
/// 实现方需保证：`write_at` 返回 `Ok(n)` 时 `n == data.len()`（写满，短写已在实现
/// 内部映射为 `Err`）；`flush` 是同步落盘屏障（返回后数据确认落盘）。
#[async_trait]
pub(crate) trait ChunkSink: Sync {
    /// 在 `offset` 处写入 `data`，成功要求写满整个 `data`（短写已映射为 `Err`）。
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<u64>;

    /// 落盘屏障：Local `sync_all` / CIFS `flush` / NFS no-op（FILE_SYNC 写即落盘）。
    async fn flush(&self) -> Result<()>;
}

/// 落盘提交策略：控制 `on_committed` 触发的时机与粒度。
pub(crate) enum CommitPolicy {
    /// 不上报进度（`write_data` 全量写：无续传状态需要维护）。
    None,
    /// 每个 chunk 写完立即上报（NFS：FILE_SYNC 写返回即落盘，无需额外屏障）。
    PerChunk(CommitCallback),
    /// 每累计 `every` 个已写 chunk 做一次 `flush` 落盘屏障，屏障之后才批量上报
    /// 这批 chunk（Local `sync_all` / CIFS `flush`：写本身不保证落盘，需要显式
    /// 屏障，保证进度记录不超前于真实落盘数据）。
    Barrier { every: usize, cb: CommitCallback },
}

type WriteFut<'a> = Pin<Box<dyn Future<Output = (u64, Result<u64>)> + Send + 'a>>;

/// 处理一个已完成的写结果：累加 `bytes_counter`、按 `commit` 策略记录/上报进度。
/// 返回 `true` 当且仅当本次调用令 `first_error` 从 `None` 变为 `Some`（首次出错）。
async fn settle<S: ChunkSink>(
    done: (u64, Result<u64>),
    sink: &S,
    commit: &CommitPolicy,
    first_error: &mut Option<StorageError>,
    pending: &mut Vec<(u64, u64)>,
    bytes_counter: &Option<Arc<AtomicU64>>,
) -> bool {
    let was_ok = first_error.is_none();
    let (offset, res) = done;
    match res {
        Ok(written) if was_ok => {
            if let Some(c) = bytes_counter {
                c.fetch_add(written, Ordering::Relaxed);
            }
            match commit {
                CommitPolicy::None => {}
                CommitPolicy::PerChunk(cb) => cb(offset, written),
                CommitPolicy::Barrier { .. } => pending.push((offset, written)),
            }
        }
        Ok(_) => {} // 出错之后的"侥幸成功" chunk 不计入有效进度
        Err(e) if was_ok => *first_error = Some(e),
        Err(_) => {} // 已有 first_error，丢弃后续错误
    }

    // Barrier 策略：累计达到阈值时做一次落盘屏障，屏障后批量上报。
    if was_ok
        && first_error.is_none()
        && let CommitPolicy::Barrier { every, cb } = commit
        && pending.len() >= *every
    {
        match sink.flush().await {
            Ok(()) => {
                for (o, l) in pending.drain(..) {
                    cb(o, l);
                }
            }
            Err(e) => *first_error = Some(e),
        }
    }

    was_ok && first_error.is_some()
}

/// 统一的分块写入管道。
///
/// - `sub_chunk_size`：`Some(n)` 时把超过 `n` 字节的 `DataChunk` 按 `n` 切分后
///   分别派发（如 NFS 受协商 wsize 限制）；`None` 时整块派发（CIFS/Local）。
/// - `inflight`：同时在飞的写请求数上限（`1` = 严格串行，见模块文档的等价性说明）。
/// - 出错处理：记录首个错误、关闭 `rx`（协作取消上游 read 任务，其后续
///   `tx.send().await` 会收到 `SendError`）、丢弃已缓冲 chunk 剩余部分、
///   drain 完剩余 inflight 后返回该错误——与重构前 NFS/CIFS 手写版本一致。
pub(crate) async fn write_pipeline_core<S: ChunkSink>(
    mut rx: mpsc::Receiver<DataChunk>,
    sink: &S,
    sub_chunk_size: Option<u64>,
    inflight: usize,
    commit: CommitPolicy,
    bytes_counter: Option<Arc<AtomicU64>>,
) -> Result<()> {
    let inflight = inflight.max(1);
    let mut inflight_set: FuturesUnordered<WriteFut<'_>> = FuturesUnordered::new();
    let mut first_error: Option<StorageError> = None;
    let mut pending: Vec<(u64, u64)> = Vec::new();

    'recv: while let Some(chunk) = rx.recv().await {
        if first_error.is_some() {
            continue;
        }
        let DataChunk { offset, data } = chunk;

        let pieces: Vec<(u64, Bytes)> = match sub_chunk_size {
            #[allow(clippy::cast_possible_truncation)]
            Some(n) if data.len() as u64 > n => {
                let n = n as usize;
                let total = data.len();
                let mut v = Vec::with_capacity(total.div_ceil(n));
                let mut idx = 0usize;
                while idx < total {
                    let end = (idx + n).min(total);
                    v.push((offset + idx as u64, data.slice(idx..end)));
                    idx = end;
                }
                v
            }
            _ => vec![(offset, data)],
        };

        for (sub_offset, sub_data) in pieces {
            if first_error.is_some() {
                continue 'recv;
            }
            let fut: WriteFut<'_> = Box::pin(async move {
                let res = sink.write_at(sub_offset, sub_data).await;
                (sub_offset, res)
            });
            inflight_set.push(fut);

            while inflight_set.len() >= inflight {
                let Some(done) = inflight_set.next().await else {
                    break;
                };
                if settle(
                    done,
                    sink,
                    &commit,
                    &mut first_error,
                    &mut pending,
                    &bytes_counter,
                )
                .await
                {
                    rx.close();
                }
            }
        }
    }

    // Drain 剩余 inflight。
    while let Some(done) = inflight_set.next().await {
        settle(
            done,
            sink,
            &commit,
            &mut first_error,
            &mut pending,
            &bytes_counter,
        )
        .await;
    }

    // 收尾屏障：无错误时无条件做最后一次 flush（即便 pending 为空也执行，
    // 与重构前 Local/CIFS 手写版本一致），随后批量上报剩余 pending。
    if first_error.is_none()
        && let CommitPolicy::Barrier { cb, .. } = &commit
    {
        match sink.flush().await {
            Ok(()) => {
                for (o, l) in pending.drain(..) {
                    cb(o, l);
                }
            }
            Err(e) => first_error = Some(e),
        }
    }

    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
