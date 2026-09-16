//! Ordered or completion-ordered SMB reads with admission before allocating payloads.
use super::source::{ReadState, classify, entry_failure};
use crate::model::{FailureClass, Operation};
use crate::storage::{ByteStream, PositionedByteStream, PositionedChunk, StorageRoleFailure};
use futures::{
    StreamExt as _,
    future::Either,
    stream::{FuturesOrdered, FuturesUnordered},
};

pub(super) fn stream(state: ReadState) -> ByteStream {
    Box::pin(build(state, false).map(|item| item.map(|chunk| chunk.data)))
}

pub(super) fn positioned_stream(state: ReadState) -> PositionedByteStream {
    build(state, true)
}

fn build(state: ReadState, unordered: bool) -> PositionedByteStream {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let cancel = state.request.cancel.clone();
    let path = state.request.path.clone();
    tokio::spawn(async move {
        let result = run(&state, &tx, unordered).await;
        let close = state.cursor.close().await;
        let result = result
            .and_then(|()| close.map_err(|e| classify(&state.request.path, Operation::Read, &e)));
        if let Err(error) = result {
            let _ = tx.send(Err(error)).await;
        }
    });
    Box::pin(futures::stream::unfold(
        (rx, cancel, path, false),
        |(mut rx, cancel, path, ended)| async move {
            if ended {
                return None;
            }
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    rx.close();
                    while rx.recv().await.is_some() {}
                    Some(Err(entry_failure(&path, Operation::Read, FailureClass::Cancelled)))
                }
                item = rx.recv() => item,
            };
            result.map(|item| {
                let ended = item.is_err();
                (item, (rx, cancel, path, ended))
            })
        },
    ))
}

async fn run(
    state: &ReadState,
    tx: &tokio::sync::mpsc::Sender<Result<PositionedChunk, StorageRoleFailure>>,
    unordered: bool,
) -> Result<(), StorageRoleFailure> {
    let request = &state.request;
    let failed = |class| entry_failure(&request.path, Operation::Read, class);
    let maximum = request
        .maximum_chunk_bytes
        .min(state.cursor.maximum_read_chunk() as usize);
    if maximum == 0 {
        return Err(failed(FailureClass::Protocol));
    }
    let depth = request.read_inflight;
    let mut next = state.range.start;
    let mut reads = if unordered {
        Either::Right(FuturesUnordered::new())
    } else {
        Either::Left(FuturesOrdered::new())
    };
    loop {
        while match &reads {
            Either::Left(queue) => queue.len(),
            Either::Right(queue) => queue.len(),
        } < depth
            && next < state.range.end
        {
            let count = usize::try_from((state.range.end - next).min(maximum as u64))
                .map_err(|_| failed(FailureClass::InvalidInput))?;
            let admission = if let Some(budget) = &request.read_budget {
                let reserved = tokio::select! {
                    () = tx.closed() => return Ok(()),
                    result = budget.reserve(count, match &reads { Either::Left(queue) => queue.is_empty(), Either::Right(queue) => queue.is_empty() }) => result,
                }
                .map_err(|_| failed(FailureClass::Cancelled))?;
                let Some(admission) = reserved else { break };
                Some(admission)
            } else {
                None
            };
            let offset = next;
            // QoS can reduce a read; obtain the grant before assigning the next offset.
            let count = if let Some(qos) = &request.source_qos {
                usize::try_from(
                    tokio::select! {
                        () = tx.closed() => return Ok(()),
                        result = qos.admit_read(count as u64, &request.cancel) => result,
                    }
                    .map_err(|_| failed(FailureClass::Cancelled))?,
                )
                .map_err(|_| failed(FailureClass::InvalidInput))?
            } else {
                count
            };
            let wire_count =
                u32::try_from(count).map_err(|_| failed(FailureClass::InvalidInput))?;
            let future = async move {
                let result = tokio::select! {
                    biased;
                    () = request.cancel.cancelled() => Err(failed(FailureClass::Cancelled)),
                    result = state.cursor.read_at(offset, wire_count) => result.map_err(|e| classify(&request.path, Operation::Read, &e)),
                };
                (offset, count, result, admission)
            };
            match &mut reads {
                Either::Left(queue) => queue.push_back(future),
                Either::Right(queue) => queue.push(future),
            }
            next += count as u64;
        }
        let value = tokio::select! {
            biased;
            () = tx.closed() => return Ok(()),
            () = request.cancel.cancelled() => return Err(failed(FailureClass::Cancelled)),
            value = reads.next() => value,
        };
        let Some((offset, count, result, admission)) = value else {
            return Ok(());
        };
        let bytes = result?;
        if bytes.len() != count {
            return Err(failed(FailureClass::Corruption));
        }
        if let Some(qos) = &request.source_qos {
            qos.record_read_bytes(bytes.len() as u64);
        }
        if let (Some(budget), Some(admission)) = (&request.read_budget, admission) {
            budget.ready(offset, admission);
        }
        tokio::select! {
            () = request.cancel.cancelled() => return Err(failed(FailureClass::Cancelled)),
            result = tx.send(Ok(PositionedChunk { offset, data: bytes })) => if result.is_err() { return Ok(()) },
        }
    }
}
