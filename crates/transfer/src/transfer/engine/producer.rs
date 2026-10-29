//! An ordered source stream connected to a writer by a bounded channel.
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::failure::source_failure;
use super::{TransferFailure, TransferPhase, TransferSide};
use crate::model::{FailureClass, SourceIdentity, SourceVersion, StoragePath};
use crate::runtime::inflight::ReadRange;
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceQosBudget, StorageRoleFailure};

/// Decouples independently bounded backend read/write windows. This is a queue of
/// backend-sized chunks, not a per-transfer concurrency or total-memory setting.
const COPY_CHANNEL_CAPACITY: usize = 4;

type Payload = Result<Bytes, StorageRoleFailure>;
pub(super) type PayloadSender = mpsc::Sender<Payload>;

pub(super) struct PayloadReceiver {
    receiver: mpsc::Receiver<Payload>,
    cancel: CancellationToken,
    path: StoragePath,
    terminal: bool,
}

impl PayloadReceiver {
    pub(super) async fn next(&mut self) -> Option<Payload> {
        if self.terminal {
            return None;
        }
        let item = tokio::select! {
            biased;
            () = self.cancel.cancelled() => Some(Err(source_failure(&self.path, FailureClass::Cancelled))),
            item = self.receiver.recv() => item,
        };
        self.terminal = item.as_ref().is_none_or(Result::is_err);
        item
    }

    pub(super) fn into_stream(self) -> ByteStream {
        Box::pin(futures::stream::unfold(self, |mut receiver| async move {
            receiver.next().await.map(|item| (item, receiver))
        }))
    }
}

pub(super) fn payload_channel(
    cancel: CancellationToken,
    path: StoragePath,
) -> (PayloadSender, PayloadReceiver) {
    let (sender, receiver) = mpsc::channel(COPY_CHANNEL_CAPACITY);
    (
        sender,
        PayloadReceiver {
            receiver,
            cancel,
            path,
            terminal: false,
        },
    )
}

pub(super) struct ProducerRequest {
    pub(super) source: Arc<dyn ReadSource>,
    pub(super) path: StoragePath,
    pub(super) source_identity: SourceIdentity,
    pub(super) version: SourceVersion,
    pub(super) cancel: CancellationToken,
    pub(super) sender: PayloadSender,
    pub(super) size: u64,
    pub(super) write_start: u64,
    pub(super) source_qos: Option<SourceQosBudget>,
    pub(super) hash_content: bool,
}

pub(super) async fn produce(request: ProducerRequest) -> Result<Option<[u8; 32]>, TransferFailure> {
    let result = produce_inner(&request).await;
    if let Err(failure) = &result {
        let role = failure
            .role
            .as_deref()
            .cloned()
            .unwrap_or_else(|| source_failure(&request.path, FailureClass::Corruption));
        tokio::select! {
            biased;
            () = request.cancel.cancelled() => {},
            _ = request.sender.send(Err(role)) => {},
        }
    }
    result
}

async fn produce_inner(request: &ProducerRequest) -> Result<Option<[u8; 32]>, TransferFailure> {
    let role = |error| TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, error);
    let cancelled = || role(source_failure(&request.path, FailureClass::Cancelled));
    let mut hasher = request.hash_content.then(blake3::Hasher::new);
    let read_start = if request.hash_content {
        0
    } else {
        request.write_start
    };
    let maximum = request.source.maximum_read_chunk_bytes().max(1);
    let mut stream = tokio::select! {
        biased;
        () = request.cancel.cancelled() => return Err(cancelled()),
        () = request.sender.closed() => return Err(cancelled()),
        stream = request.source.read(ReadRequest {
            path: request.path.clone(),
            range: Some(read_start..request.size),
            expected_source: Some(request.source_identity.clone()),
            maximum_chunk_bytes: maximum,
            read_inflight: request.source.read_concurrency().max(1),
            read_budget: None,
            cancel: request.cancel.clone(),
            source_qos: request.source_qos.clone(),
            version: request.version.clone(),
        }) => stream.map_err(role)?,
    };
    let mut offset = read_start;
    loop {
        let item = tokio::select! {
            biased;
            () = request.cancel.cancelled() => return Err(cancelled()),
            () = request.sender.closed() => return Err(cancelled()),
            item = stream.next() => item,
        };
        let Some(bytes) = item else { break };
        let bytes = bytes.map_err(role)?;
        let next = offset
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= request.size);
        let Some(next) = next.filter(|_| !bytes.is_empty() && bytes.len() <= maximum) else {
            return Err(role(source_failure(
                &request.path,
                FailureClass::Corruption,
            )));
        };
        if let Some(hasher) = &mut hasher {
            hasher.update(&bytes);
        }
        if next > request.write_start {
            let skip = usize::try_from(request.write_start.saturating_sub(offset))
                .map_err(|_| role(source_failure(&request.path, FailureClass::Corruption)))?;
            tokio::select! {
                biased;
                () = request.cancel.cancelled() => return Err(cancelled()),
                sent = request.sender.send(Ok(bytes.slice(skip..))) => sent.map_err(|_| cancelled())?,
            }
        }
        offset = next;
    }
    if request.cancel.is_cancelled() {
        return Err(cancelled());
    }
    if offset != request.size {
        return Err(role(source_failure(
            &request.path,
            FailureClass::Corruption,
        )));
    }
    Ok(hasher.map(|hasher| *hasher.finalize().as_bytes()))
}

pub(super) async fn read_exact_range(
    source: &dyn ReadSource,
    path: &StoragePath,
    source_identity: &SourceIdentity,
    version: &SourceVersion,
    cancel: &tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    range: ReadRange,
) -> Result<Bytes, StorageRoleFailure> {
    let mut stream = source
        .read(ReadRequest {
            path: path.clone(),
            range: Some(range.offset..range.offset + range.length as u64),
            expected_source: Some(source_identity.clone()),
            maximum_chunk_bytes: range.length,
            read_inflight: 1,
            read_budget: None,
            cancel: cancel.clone(),
            source_qos,
            version: version.clone(),
        })
        .await?;
    let mut output = BytesMut::with_capacity(range.length);
    while let Some(item) = stream.next().await {
        let item = item?;
        let remaining = range.length - output.len();
        if item.len() > remaining {
            return Err(source_failure(path, FailureClass::Corruption));
        }
        output.extend_from_slice(&item);
    }
    if output.len() != range.length {
        return Err(source_failure(path, FailureClass::Corruption));
    }
    Ok(output.freeze())
}
